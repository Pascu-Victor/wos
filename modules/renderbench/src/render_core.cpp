#include "render_core.hpp"

#include <sys/stat.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): POSIX nanosleep is declared here.
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <ios>
#include <iosfwd>
#include <limits>
#include <mandelbench/lodepng.hpp>
#include <memory>
#include <numbers>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tracebench {
namespace {

constexpr float PI = std::numbers::pi_v<float>;
constexpr float EPSILON = 0.0005F;
constexpr int MAX_TRANSPARENT_HOPS = 16;
constexpr float DEBUG_BLACK_TEXTURE_LUMINANCE = 0.04F;
constexpr float DEBUG_TRIANGLE_EDGE_DISTANCE = 0.008F;
constexpr float DEBUG_NORMAL_DIVERGENCE_DOT = 0.35F;
constexpr float DEBUG_GRAZING_VIEW_DOT = 0.08F;
constexpr float DEBUG_DARK_BASE_LUMINANCE = 0.08F;
constexpr float DEBUG_DARK_RESULT_LUMINANCE = 0.025F;
constexpr float MIN_GEOMETRIC_OUTGOING_DOT = 0.05F;
constexpr int PREVIEW_MAX_DIMENSION = 1024;

struct Vec3 {
    float x = 0.0F;
    float y = 0.0F;
    float z = 0.0F;

    auto operator+(const Vec3& other) const -> Vec3 { return {x + other.x, y + other.y, z + other.z}; }
    auto operator-(const Vec3& other) const -> Vec3 { return {x - other.x, y - other.y, z - other.z}; }
    auto operator*(float value) const -> Vec3 { return {x * value, y * value, z * value}; }
    auto operator/(float value) const -> Vec3 { return {x / value, y / value, z / value}; }
    auto operator*(const Vec3& other) const -> Vec3 { return {x * other.x, y * other.y, z * other.z}; }
    auto operator+=(const Vec3& other) -> Vec3& {
        x += other.x;
        y += other.y;
        z += other.z;
        return *this;
    }
};

struct Vec2 {
    float x = 0.0F;
    float y = 0.0F;

    auto operator+(const Vec2& other) const -> Vec2 { return {.x = x + other.x, .y = y + other.y}; }
    auto operator-(const Vec2& other) const -> Vec2 { return {.x = x - other.x, .y = y - other.y}; }
    auto operator*(float value) const -> Vec2 { return {.x = x * value, .y = y * value}; }
};

auto dot(const Vec3& a, const Vec3& b) -> float { return (a.x * b.x) + (a.y * b.y) + (a.z * b.z); }

auto cross(const Vec3& a, const Vec3& b) -> Vec3 {
    return {
        .x = (a.y * b.z) - (a.z * b.y),
        .y = (a.z * b.x) - (a.x * b.z),
        .z = (a.x * b.y) - (a.y * b.x),
    };
}

auto length(const Vec3& value) -> float { return std::sqrt(std::max(dot(value, value), 0.0F)); }

auto normalize(const Vec3& value) -> Vec3 {
    float const LEN = length(value);
    if (LEN <= 0.0F) {
        return {};
    }
    return value / LEN;
}

auto reflect(const Vec3& direction, const Vec3& normal) -> Vec3 { return direction - (normal * (2.0F * dot(direction, normal))); }

auto luminance(const Vec3& value) -> float { return (0.2126F * value.x) + (0.7152F * value.y) + (0.0722F * value.z); }

auto face_forward(const Vec3& normal, const Vec3& direction) -> Vec3 { return dot(normal, direction) < 0.0F ? normal : normal * -1.0F; }

struct Ray {
    Vec3 origin;
    Vec3 direction;
};

enum class AlphaMode : uint8_t {
    OPAQUE,
    MASK,
    BLEND,
};

struct Material {
    Vec3 base_color{.x = 0.8F, .y = 0.8F, .z = 0.8F};
    Vec3 emissive{};
    float base_alpha = 1.0F;
    float metallic = 0.0F;
    float roughness = 0.7F;
    AlphaMode alpha_mode = AlphaMode::OPAQUE;
    float alpha_cutoff = 0.5F;
    int base_color_texture = -1;
    int metallic_roughness_texture = -1;
    int normal_texture = -1;
    int emissive_texture = -1;
    int occlusion_texture = -1;
};

struct Triangle {
    Vec3 a;
    Vec3 b;
    Vec3 c;
    Vec3 normal;
    Vec3 n0;
    Vec3 n1;
    Vec3 n2;
    Vec2 uv0;
    Vec2 uv1;
    Vec2 uv2;
    Vec3 tangent;
    Vec3 bitangent;
    int material = 0;
};

struct Texture {
    int width = 0;
    int height = 0;
    std::vector<unsigned char> rgba;

    auto valid() const -> bool {
        return width > 0 && height > 0 && rgba.size() >= static_cast<size_t>(width) * static_cast<size_t>(height) * 4U;
    }

    auto sample(Vec2 uv, bool alpha_weighted_rgb = false) const -> std::array<float, 4> {
        if (!valid()) {
            return {1.0F, 1.0F, 1.0F, 1.0F};
        }

        float const U = uv.x - std::floor(uv.x);
        float const V = uv.y - std::floor(uv.y);
        float const PX = (U * static_cast<float>(width)) - 0.5F;
        float const PY = (V * static_cast<float>(height)) - 0.5F;
        auto wrap_index = [](int value, int limit) -> int {
            int const WRAPPED = value % limit;
            return WRAPPED < 0 ? WRAPPED + limit : WRAPPED;
        };
        int const X_BASE = static_cast<int>(std::floor(PX));
        int const Y_BASE = static_cast<int>(std::floor(PY));
        int const X0 = wrap_index(X_BASE, width);
        int const Y0 = wrap_index(Y_BASE, height);
        int const X1 = wrap_index(X_BASE + 1, width);
        int const Y1 = wrap_index(Y_BASE + 1, height);
        float const TX = PX - static_cast<float>(X_BASE);
        float const TY = PY - static_cast<float>(Y_BASE);

        auto load = [&](int x, int y) -> std::array<float, 4> {
            size_t const PIXEL = (static_cast<size_t>(y) * static_cast<size_t>(width)) + static_cast<size_t>(x);
            size_t const OFFSET = PIXEL * 4U;
            return {
                static_cast<float>(rgba[OFFSET + 0U]) / 255.0F,
                static_cast<float>(rgba[OFFSET + 1U]) / 255.0F,
                static_cast<float>(rgba[OFFSET + 2U]) / 255.0F,
                static_cast<float>(rgba[OFFSET + 3U]) / 255.0F,
            };
        };
        auto const A = load(X0, Y0);
        auto const B = load(X1, Y0);
        auto const C = load(X0, Y1);
        auto const D = load(X1, Y1);
        std::array<float, 4> const WEIGHT{
            (1.0F - TX) * (1.0F - TY),
            TX * (1.0F - TY),
            (1.0F - TX) * TY,
            TX * TY,
        };
        std::array<float, 4> out{};
        std::array<std::array<float, 4>, 4> const TAP{A, B, C, D};
        if (alpha_weighted_rgb) {
            float alpha = 0.0F;
            for (size_t tap = 0; tap < TAP.size(); ++tap) {
                alpha += TAP[tap][3] * WEIGHT[tap];
            }
            out[3] = alpha;
            if (alpha > 1.0e-6F) {
                for (size_t channel = 0; channel < 3U; ++channel) {
                    float premul = 0.0F;
                    for (size_t tap = 0; tap < TAP.size(); ++tap) {
                        premul += TAP[tap][channel] * TAP[tap][3] * WEIGHT[tap];
                    }
                    out[channel] = premul / alpha;
                }
            }
            return out;
        }
        for (size_t channel = 0; channel < out.size(); ++channel) {
            for (size_t tap = 0; tap < TAP.size(); ++tap) {
                out[channel] += TAP[tap][channel] * WEIGHT[tap];
            }
        }
        return out;
    }
};

struct Mat4 {
    std::array<float, 16> m{};

    static auto identity() -> Mat4 {
        Mat4 out;
        out.m[0] = 1.0F;
        out.m[5] = 1.0F;
        out.m[10] = 1.0F;
        out.m[15] = 1.0F;
        return out;
    }
};

auto mat4_mul(const Mat4& lhs, const Mat4& rhs) -> Mat4 {
    Mat4 out;
    for (size_t column = 0; column < 4U; ++column) {
        for (size_t row = 0; row < 4U; ++row) {
            float value = 0.0F;
            for (size_t k = 0; k < 4U; ++k) {
                value += lhs.m[(k * 4U) + row] * rhs.m[(column * 4U) + k];
            }
            out.m[(column * 4U) + row] = value;
        }
    }
    return out;
}

auto mat4_translate(Vec3 value) -> Mat4 {
    Mat4 out = Mat4::identity();
    out.m[12] = value.x;
    out.m[13] = value.y;
    out.m[14] = value.z;
    return out;
}

auto mat4_scale(Vec3 value) -> Mat4 {
    Mat4 out = Mat4::identity();
    out.m[0] = value.x;
    out.m[5] = value.y;
    out.m[10] = value.z;
    return out;
}

auto mat4_quaternion(std::array<float, 4> q) -> Mat4 {
    Vec3 const V{.x = q[0], .y = q[1], .z = q[2]};
    float const QLEN = std::sqrt(dot(V, V) + (q[3] * q[3]));
    if (QLEN <= 0.0F) {
        return Mat4::identity();
    }
    float const X = q[0] / QLEN;
    float const Y = q[1] / QLEN;
    float const Z = q[2] / QLEN;
    float const W = q[3] / QLEN;

    Mat4 out = Mat4::identity();
    out.m[0] = 1.0F - (2.0F * Y * Y) - (2.0F * Z * Z);
    out.m[1] = (2.0F * X * Y) + (2.0F * W * Z);
    out.m[2] = (2.0F * X * Z) - (2.0F * W * Y);
    out.m[4] = (2.0F * X * Y) - (2.0F * W * Z);
    out.m[5] = 1.0F - (2.0F * X * X) - (2.0F * Z * Z);
    out.m[6] = (2.0F * Y * Z) + (2.0F * W * X);
    out.m[8] = (2.0F * X * Z) + (2.0F * W * Y);
    out.m[9] = (2.0F * Y * Z) - (2.0F * W * X);
    out.m[10] = 1.0F - (2.0F * X * X) - (2.0F * Y * Y);
    return out;
}

auto transform_point(const Mat4& transform, Vec3 value) -> Vec3 {
    float const X = (transform.m[0] * value.x) + (transform.m[4] * value.y) + (transform.m[8] * value.z) + transform.m[12];
    float const Y = (transform.m[1] * value.x) + (transform.m[5] * value.y) + (transform.m[9] * value.z) + transform.m[13];
    float const Z = (transform.m[2] * value.x) + (transform.m[6] * value.y) + (transform.m[10] * value.z) + transform.m[14];
    float const W = (transform.m[3] * value.x) + (transform.m[7] * value.y) + (transform.m[11] * value.z) + transform.m[15];
    if (std::fabs(W) > 1.0e-8F) {
        return {.x = X / W, .y = Y / W, .z = Z / W};
    }
    return {.x = X, .y = Y, .z = Z};
}

auto transform_vector(const Mat4& transform, Vec3 value) -> Vec3 {
    return {
        .x = (transform.m[0] * value.x) + (transform.m[4] * value.y) + (transform.m[8] * value.z),
        .y = (transform.m[1] * value.x) + (transform.m[5] * value.y) + (transform.m[9] * value.z),
        .z = (transform.m[2] * value.x) + (transform.m[6] * value.y) + (transform.m[10] * value.z),
    };
}

struct Aabb {
    Vec3 min{.x = std::numeric_limits<float>::max(), .y = std::numeric_limits<float>::max(), .z = std::numeric_limits<float>::max()};
    Vec3 max{.x = -std::numeric_limits<float>::max(), .y = -std::numeric_limits<float>::max(), .z = -std::numeric_limits<float>::max()};

    void include(const Vec3& p) {
        min.x = std::min(min.x, p.x);
        min.y = std::min(min.y, p.y);
        min.z = std::min(min.z, p.z);
        max.x = std::max(max.x, p.x);
        max.y = std::max(max.y, p.y);
        max.z = std::max(max.z, p.z);
    }

    void include(const Aabb& box) {
        include(box.min);
        include(box.max);
    }
};

struct BvhNode {
    Aabb bounds;
    int left = -1;
    int right = -1;
    int start = 0;
    int count = 0;
};

struct Camera {
    Vec3 origin{.x = 0.0F, .y = 1.0F, .z = 4.8F};
    Vec3 forward{.x = 0.0F, .y = -0.05F, .z = -1.0F};
    Vec3 right{.x = 1.0F, .y = 0.0F, .z = 0.0F};
    Vec3 up{.x = 0.0F, .y = 1.0F, .z = 0.0F};
    float vfov_degrees = 45.0F;
};

struct Rng {
    uint64_t state = 0x9e3779b97f4a7c15ULL;

    auto next_u32() -> uint32_t {
        uint64_t old_state = state;
        state = (old_state * 6364136223846793005ULL) + 1442695040888963407ULL;
        auto const XSHIFTED = static_cast<uint32_t>(((old_state >> 18U) ^ old_state) >> 27U);
        auto const ROT = static_cast<uint32_t>(old_state >> 59U);
        return (XSHIFTED >> ROT) | (XSHIFTED << ((0U - ROT) & 31U));
    }

    auto uniform() -> float { return static_cast<float>(next_u32() >> 8U) * (1.0F / 16777216.0F); }
};

auto triangle_bounds(const Triangle& tri) -> Aabb {
    Aabb box;
    box.include(tri.a);
    box.include(tri.b);
    box.include(tri.c);
    return box;
}

auto triangle_centroid(const Triangle& tri) -> Vec3 { return (tri.a + tri.b + tri.c) / 3.0F; }

auto axis_value(const Vec3& value, int axis) -> float {
    if (axis == 0) {
        return value.x;
    }
    if (axis == 1) {
        return value.y;
    }
    return value.z;
}

auto intersect_aabb(const Aabb& box, const Ray& ray, float max_t) -> bool {
    double tmin = EPSILON;
    double tmax = max_t;
    std::array<double, 3> const ORIGIN{ray.origin.x, ray.origin.y, ray.origin.z};
    std::array<double, 3> const DIR{ray.direction.x, ray.direction.y, ray.direction.z};
    std::array<double, 3> const BMIN{box.min.x, box.min.y, box.min.z};
    std::array<double, 3> const BMAX{box.max.x, box.max.y, box.max.z};
    for (size_t axis = 0; axis < DIR.size(); ++axis) {
        if (std::fabs(DIR.at(axis)) < 1.0e-12) {
            if (ORIGIN.at(axis) < BMIN.at(axis) || ORIGIN.at(axis) > BMAX.at(axis)) {
                return false;
            }
            continue;
        }
        double const INV_D = 1.0 / DIR.at(axis);
        double t0 = (BMIN.at(axis) - ORIGIN.at(axis)) * INV_D;
        double t1 = (BMAX.at(axis) - ORIGIN.at(axis)) * INV_D;
        if (INV_D < 0.0) {
            std::swap(t0, t1);
        }
        tmin = std::max(tmin, t0);
        tmax = std::min(tmax, t1);
        if (tmax < tmin) {
            return false;
        }
    }
    return true;
}

auto intersect_triangle(const Triangle& tri, const Ray& ray, float& t, float& u, float& v) -> bool {
    double const E1X = static_cast<double>(tri.b.x) - static_cast<double>(tri.a.x);
    double const E1Y = static_cast<double>(tri.b.y) - static_cast<double>(tri.a.y);
    double const E1Z = static_cast<double>(tri.b.z) - static_cast<double>(tri.a.z);
    double const E2X = static_cast<double>(tri.c.x) - static_cast<double>(tri.a.x);
    double const E2Y = static_cast<double>(tri.c.y) - static_cast<double>(tri.a.y);
    double const E2Z = static_cast<double>(tri.c.z) - static_cast<double>(tri.a.z);
    double const DX = ray.direction.x;
    double const DY = ray.direction.y;
    double const DZ = ray.direction.z;
    double const PX = (DY * E2Z) - (DZ * E2Y);
    double const PY = (DZ * E2X) - (DX * E2Z);
    double const PZ = (DX * E2Y) - (DY * E2X);
    double const DET = (E1X * PX) + (E1Y * PY) + (E1Z * PZ);
    if (std::fabs(DET) < 1.0e-12) {
        return false;
    }
    double const INV_DET = 1.0 / DET;
    double const TX = static_cast<double>(ray.origin.x) - static_cast<double>(tri.a.x);
    double const TY = static_cast<double>(ray.origin.y) - static_cast<double>(tri.a.y);
    double const TZ = static_cast<double>(ray.origin.z) - static_cast<double>(tri.a.z);
    double const U = ((TX * PX) + (TY * PY) + (TZ * PZ)) * INV_DET;
    if (U < 0.0 || U > 1.0) {
        return false;
    }
    double const QX = (TY * E1Z) - (TZ * E1Y);
    double const QY = (TZ * E1X) - (TX * E1Z);
    double const QZ = (TX * E1Y) - (TY * E1X);
    double const V = ((DX * QX) + (DY * QY) + (DZ * QZ)) * INV_DET;
    if (V < 0.0 || U + V > 1.0) {
        return false;
    }
    double const T_HIT = ((E2X * QX) + (E2Y * QY) + (E2Z * QZ)) * INV_DET;
    if (T_HIT <= static_cast<double>(EPSILON) || T_HIT > static_cast<double>(std::numeric_limits<float>::max())) {
        return false;
    }
    t = static_cast<float>(T_HIT);
    u = static_cast<float>(U);
    v = static_cast<float>(V);
    return true;
}

auto fallback_basis(const Vec3& normal) -> std::array<Vec3, 2> {
    Vec3 const TANGENT = normalize(cross(std::fabs(normal.x) > 0.5F ? Vec3{0.0F, 1.0F, 0.0F} : Vec3{1.0F, 0.0F, 0.0F}, normal));
    return {TANGENT, normalize(cross(normal, TANGENT))};
}

auto triangle_basis(Vec3 a, Vec3 b, Vec3 c, Vec2 uv0, Vec2 uv1, Vec2 uv2, Vec3 normal) -> std::array<Vec3, 2> {
    Vec3 const E1 = b - a;
    Vec3 const E2 = c - a;
    Vec2 const DUV1 = uv1 - uv0;
    Vec2 const DUV2 = uv2 - uv0;
    float const DENOM = (DUV1.x * DUV2.y) - (DUV2.x * DUV1.y);
    if (std::fabs(DENOM) <= 1.0e-8F) {
        return fallback_basis(normal);
    }

    float const INV = 1.0F / DENOM;
    Vec3 tangent = normalize((E1 * DUV2.y - E2 * DUV1.y) * INV);
    if (length(tangent) <= 0.0F) {
        return fallback_basis(normal);
    }
    tangent = normalize(tangent - (normal * dot(tangent, normal)));
    Vec3 bitangent = normalize(cross(normal, tangent));
    if (length(bitangent) <= 0.0F) {
        return fallback_basis(normal);
    }
    return {tangent, bitangent};
}

auto make_tri(Vec3 a, Vec3 b, Vec3 c, int material) -> Triangle {
    Vec3 const NORMAL = normalize(cross(b - a, c - a));
    auto const BASIS = fallback_basis(NORMAL);
    return {.a = a,
            .b = b,
            .c = c,
            .normal = NORMAL,
            .n0 = NORMAL,
            .n1 = NORMAL,
            .n2 = NORMAL,
            .uv0 = {},
            .uv1 = {},
            .uv2 = {},
            .tangent = BASIS[0],
            .bitangent = BASIS[1],
            .material = material};
}

void add_quad(std::vector<Triangle>& triangles, Vec3 a, Vec3 b, Vec3 c, Vec3 d, int material) {
    triangles.push_back(make_tri(a, b, c, material));
    triangles.push_back(make_tri(a, c, d, material));
}

void add_box(std::vector<Triangle>& triangles, Vec3 min, Vec3 max, int material) {
    Vec3 const P000{.x = min.x, .y = min.y, .z = min.z};
    Vec3 const P001{.x = min.x, .y = min.y, .z = max.z};
    Vec3 const P010{.x = min.x, .y = max.y, .z = min.z};
    Vec3 const P011{.x = min.x, .y = max.y, .z = max.z};
    Vec3 const P100{.x = max.x, .y = min.y, .z = min.z};
    Vec3 const P101{.x = max.x, .y = min.y, .z = max.z};
    Vec3 const P110{.x = max.x, .y = max.y, .z = min.z};
    Vec3 const P111{.x = max.x, .y = max.y, .z = max.z};
    add_quad(triangles, P000, P100, P110, P010, material);
    add_quad(triangles, P101, P001, P011, P111, material);
    add_quad(triangles, P001, P000, P010, P011, material);
    add_quad(triangles, P100, P101, P111, P110, material);
    add_quad(triangles, P010, P110, P111, P011, material);
    add_quad(triangles, P001, P101, P100, P000, material);
}

auto read_file_bytes(const std::string& path) -> std::vector<uint8_t> {
    if (path.empty()) {
        return {};
    }

    std::ifstream file(path, std::ios::binary);
    if (!file) {
        return {};
    }

    std::array<char, 64 * 1024> buffer{};
    std::vector<uint8_t> bytes;
    while (true) {
        file.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        auto const GOT = file.gcount();
        if (GOT > 0) {
            auto const* BEGIN = reinterpret_cast<const uint8_t*>(buffer.data());
            bytes.insert(bytes.end(), BEGIN, BEGIN + GOT);
        }
        if (file.bad()) {
            return {};
        }
        if (file.eof()) {
            break;
        }
        if (!file && GOT == 0) {
            return {};
        }
    }
    return bytes;
}

auto read_u32_le(std::span<const uint8_t> bytes, size_t offset) -> uint32_t {
    if (offset + 4 > bytes.size()) {
        return 0;
    }
    return static_cast<uint32_t>(bytes[offset]) | (static_cast<uint32_t>(bytes[offset + 1]) << 8U) |
           (static_cast<uint32_t>(bytes[offset + 2]) << 16U) | (static_cast<uint32_t>(bytes[offset + 3]) << 24U);
}

struct JsonValue {
    enum class Type : uint8_t {
        JSON_NULL,
        JSON_BOOL,
        JSON_NUMBER,
        JSON_STRING,
        JSON_ARRAY,
        JSON_OBJECT,
    };

    Type type = Type::JSON_NULL;
    bool boolean = false;
    double number = 0.0;
    std::string string;
    std::vector<JsonValue> array;
    std::vector<std::string> object_keys;
    std::vector<JsonValue> object_values;
};

struct JsonParser {
    explicit JsonParser(std::string_view input) : text(input) {}

    auto parse() -> std::optional<JsonValue> {
        skip_ws();
        auto value = parse_value();
        skip_ws();
        if (!value.has_value() || pos != text.size()) {
            return std::nullopt;
        }
        return value;
    }

   private:
    std::string_view text;
    size_t pos = 0;

    void skip_ws() {
        while (pos < text.size() && std::isspace(static_cast<unsigned char>(text[pos])) != 0) {
            ++pos;
        }
    }

    auto consume(char expected) -> bool {
        skip_ws();
        if (pos >= text.size() || text[pos] != expected) {
            return false;
        }
        ++pos;
        return true;
    }

    auto match(std::string_view token) -> bool {
        if (pos + token.size() > text.size() || text.substr(pos, token.size()) != token) {
            return false;
        }
        pos += token.size();
        return true;
    }

    auto parse_string() -> std::optional<std::string> {
        if (!consume('"')) {
            return std::nullopt;
        }
        std::string out;
        while (pos < text.size()) {
            char const CH = text[pos++];
            if (CH == '"') {
                return out;
            }
            if (CH != '\\') {
                out.push_back(CH);
                continue;
            }
            if (pos >= text.size()) {
                return std::nullopt;
            }
            char const ESC = text[pos++];
            switch (ESC) {
                case '"':
                case '\\':
                case '/':
                    out.push_back(ESC);
                    break;
                case 'b':
                    out.push_back('\b');
                    break;
                case 'f':
                    out.push_back('\f');
                    break;
                case 'n':
                    out.push_back('\n');
                    break;
                case 'r':
                    out.push_back('\r');
                    break;
                case 't':
                    out.push_back('\t');
                    break;
                case 'u':
                    if (pos + 4U > text.size()) {
                        return std::nullopt;
                    }
                    pos += 4U;
                    out.push_back('?');
                    break;
                default:
                    return std::nullopt;
            }
        }
        return std::nullopt;
    }

    auto parse_number() -> std::optional<JsonValue> {
        size_t const START = pos;
        if (pos < text.size() && text[pos] == '-') {
            ++pos;
        }
        while (pos < text.size() && std::isdigit(static_cast<unsigned char>(text[pos])) != 0) {
            ++pos;
        }
        if (pos < text.size() && text[pos] == '.') {
            ++pos;
            while (pos < text.size() && std::isdigit(static_cast<unsigned char>(text[pos])) != 0) {
                ++pos;
            }
        }
        if (pos < text.size() && (text[pos] == 'e' || text[pos] == 'E')) {
            ++pos;
            if (pos < text.size() && (text[pos] == '+' || text[pos] == '-')) {
                ++pos;
            }
            while (pos < text.size() && std::isdigit(static_cast<unsigned char>(text[pos])) != 0) {
                ++pos;
            }
        }
        if (START == pos) {
            return std::nullopt;
        }
        std::string const TOKEN{text.substr(START, pos - START)};
        char* end = nullptr;
        double const VALUE = std::strtod(TOKEN.c_str(), &end);
        if (end == TOKEN.c_str() || !std::isfinite(VALUE)) {
            return std::nullopt;
        }
        JsonValue out;
        out.type = JsonValue::Type::JSON_NUMBER;
        out.number = VALUE;
        return out;
    }

    auto parse_array() -> std::optional<JsonValue> {
        if (!consume('[')) {
            return std::nullopt;
        }
        JsonValue out;
        out.type = JsonValue::Type::JSON_ARRAY;
        skip_ws();
        if (consume(']')) {
            return out;
        }
        while (true) {
            auto item = parse_value();
            if (!item.has_value()) {
                return std::nullopt;
            }
            out.array.push_back(std::move(*item));
            skip_ws();
            if (consume(']')) {
                return out;
            }
            if (!consume(',')) {
                return std::nullopt;
            }
        }
    }

    auto parse_object() -> std::optional<JsonValue> {
        if (!consume('{')) {
            return std::nullopt;
        }
        JsonValue out;
        out.type = JsonValue::Type::JSON_OBJECT;
        skip_ws();
        if (consume('}')) {
            return out;
        }
        while (true) {
            auto key = parse_string();
            if (!key.has_value() || !consume(':')) {
                return std::nullopt;
            }
            auto value = parse_value();
            if (!value.has_value()) {
                return std::nullopt;
            }
            out.object_keys.push_back(std::move(*key));
            out.object_values.push_back(std::move(*value));
            skip_ws();
            if (consume('}')) {
                return out;
            }
            if (!consume(',')) {
                return std::nullopt;
            }
        }
    }

    auto parse_value() -> std::optional<JsonValue> {
        skip_ws();
        if (pos >= text.size()) {
            return std::nullopt;
        }
        if (text[pos] == '"') {
            auto parsed = parse_string();
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            JsonValue out;
            out.type = JsonValue::Type::JSON_STRING;
            out.string = std::move(*parsed);
            return out;
        }
        if (text[pos] == '{') {
            return parse_object();
        }
        if (text[pos] == '[') {
            return parse_array();
        }
        if (match("true")) {
            JsonValue out;
            out.type = JsonValue::Type::JSON_BOOL;
            out.boolean = true;
            return out;
        }
        if (match("false")) {
            JsonValue out;
            out.type = JsonValue::Type::JSON_BOOL;
            return out;
        }
        if (match("null")) {
            return JsonValue{};
        }
        return parse_number();
    }
};

auto json_member(const JsonValue& value, std::string_view key) -> const JsonValue* {
    if (value.type != JsonValue::Type::JSON_OBJECT) {
        return nullptr;
    }
    for (size_t i = 0; i < value.object_keys.size() && i < value.object_values.size(); ++i) {
        const auto& member_key = value.object_keys[i];
        if (member_key.size() == key.size() && std::equal(member_key.begin(), member_key.end(), key.begin())) {
            return &value.object_values[i];
        }
    }
    return nullptr;
}

auto json_int_value(const JsonValue* value, int fallback) -> int {
    if (value == nullptr || value->type != JsonValue::Type::JSON_NUMBER) {
        return fallback;
    }
    return static_cast<int>(value->number);
}

auto json_size_value(const JsonValue* value, size_t fallback) -> size_t {
    int const AS_INT = json_int_value(value, -1);
    if (AS_INT < 0) {
        return fallback;
    }
    return static_cast<size_t>(AS_INT);
}

auto json_float_value(const JsonValue* value, float fallback) -> float {
    if (value == nullptr || value->type != JsonValue::Type::JSON_NUMBER) {
        return fallback;
    }
    return static_cast<float>(value->number);
}

auto json_bool_value(const JsonValue* value, bool fallback) -> bool {
    if (value == nullptr || value->type != JsonValue::Type::JSON_BOOL) {
        return fallback;
    }
    return value->boolean;
}

auto json_string_value(const JsonValue* value) -> std::string {
    if (value == nullptr || value->type != JsonValue::Type::JSON_STRING) {
        return {};
    }
    return value->string;
}

auto json_array_float(const JsonValue* value, size_t index, float fallback) -> float {
    if (value == nullptr || value->type != JsonValue::Type::JSON_ARRAY || index >= value->array.size()) {
        return fallback;
    }
    return json_float_value(&value->array[index], fallback);
}

auto json_vec3_value(const JsonValue* value, Vec3 fallback) -> Vec3 {
    return {
        json_array_float(value, 0U, fallback.x),
        json_array_float(value, 1U, fallback.y),
        json_array_float(value, 2U, fallback.z),
    };
}

auto json_int_array(const JsonValue* value) -> std::vector<int> {
    std::vector<int> out;
    if (value == nullptr || value->type != JsonValue::Type::JSON_ARRAY) {
        return out;
    }
    out.reserve(value->array.size());
    for (const auto& item : value->array) {
        out.push_back(json_int_value(&item, -1));
    }
    return out;
}

auto json_member_int(const JsonValue& value, std::string_view key, int fallback) -> int {
    return json_int_value(json_member(value, key), fallback);
}

auto json_member_size(const JsonValue& value, std::string_view key, size_t fallback) -> size_t {
    return json_size_value(json_member(value, key), fallback);
}

auto json_member_float(const JsonValue& value, std::string_view key, float fallback) -> float {
    return json_float_value(json_member(value, key), fallback);
}

auto json_member_string(const JsonValue& value, std::string_view key) -> std::string { return json_string_value(json_member(value, key)); }

auto texture_index_from(const JsonValue& value, std::string_view key) -> int {
    const JsonValue* texture = json_member(value, key);
    if (texture == nullptr || texture->type != JsonValue::Type::JSON_OBJECT) {
        return -1;
    }
    return json_member_int(*texture, "index", -1);
}

struct GlbChunks {
    std::string json;
    std::span<const uint8_t> bin;
};

auto read_glb_chunks(std::span<const uint8_t> bytes) -> std::optional<GlbChunks> {
    if (bytes.size() < 12U || read_u32_le(bytes, 0U) != 0x46546C67U || read_u32_le(bytes, 4U) != 2U) {
        return std::nullopt;
    }
    size_t const FILE_LEN = std::min<size_t>(read_u32_le(bytes, 8U), bytes.size());
    GlbChunks chunks;
    size_t cursor = 12U;
    while (cursor + 8U <= FILE_LEN) {
        uint32_t const CHUNK_LEN = read_u32_le(bytes, cursor);
        uint32_t const CHUNK_TYPE = read_u32_le(bytes, cursor + 4U);
        cursor += 8U;
        if (cursor + CHUNK_LEN > FILE_LEN) {
            return std::nullopt;
        }
        if (CHUNK_TYPE == 0x4E4F534AU) {
            chunks.json.assign(reinterpret_cast<const char*>(bytes.data() + cursor), CHUNK_LEN);
        } else if (CHUNK_TYPE == 0x004E4942U) {
            chunks.bin = bytes.subspan(cursor, CHUNK_LEN);
        }
        cursor += CHUNK_LEN;
    }
    if (chunks.json.empty()) {
        return std::nullopt;
    }
    return chunks;
}

struct GltfBufferView {
    int buffer = 0;
    size_t byte_offset = 0;
    size_t byte_length = 0;
    size_t byte_stride = 0;
};

struct GltfAccessor {
    int buffer_view = -1;
    size_t byte_offset = 0;
    int component_type = 0;
    size_t count = 0;
    std::string type;
    bool normalized = false;
};

struct GltfImage {
    int buffer_view = -1;
    std::string mime_type;
};

struct GltfTexture {
    int source = -1;
};

struct GltfPrimitive {
    int position = -1;
    int normal = -1;
    int texcoord = -1;
    int indices = -1;
    int material = -1;
    int mode = 4;
};

struct GltfMesh {
    std::vector<GltfPrimitive> primitives;
};

struct GltfNode {
    int mesh = -1;
    int camera = -1;
    std::vector<int> children;
    Mat4 local = Mat4::identity();
};

struct GltfSceneInfo {
    std::vector<int> nodes;
    int skybox_image = -1;
    float skybox_intensity = 0.8F;
};

struct GltfCamera {
    float yfov_degrees = 45.0F;
};

struct GltfDocument {
    std::vector<GltfBufferView> buffer_views;
    std::vector<GltfAccessor> accessors;
    std::vector<GltfImage> images;
    std::vector<GltfTexture> textures;
    std::vector<Material> materials;
    std::vector<GltfMesh> meshes;
    std::vector<GltfNode> nodes;
    std::vector<GltfSceneInfo> scenes;
    std::vector<GltfCamera> cameras;
    int scene = 0;
};

auto image_for_texture(const GltfDocument& doc, int texture_index) -> int {
    if (texture_index < 0 || static_cast<size_t>(texture_index) >= doc.textures.size()) {
        return -1;
    }
    return doc.textures[static_cast<size_t>(texture_index)].source;
}

auto parse_node_transform(const JsonValue& node) -> Mat4 {
    const JsonValue* matrix = json_member(node, "matrix");
    if (matrix != nullptr && matrix->type == JsonValue::Type::JSON_ARRAY && matrix->array.size() >= 16U) {
        Mat4 out;
        for (size_t i = 0; i < out.m.size(); ++i) {
            out.m[i] = json_float_value(&matrix->array[i], i % 5U == 0U ? 1.0F : 0.0F);
        }
        return out;
    }

    Vec3 const TRANSLATION = json_vec3_value(json_member(node, "translation"), {});
    Vec3 const SCALE = json_vec3_value(json_member(node, "scale"), {1.0F, 1.0F, 1.0F});
    std::array<float, 4> rotation{
        json_array_float(json_member(node, "rotation"), 0U, 0.0F),
        json_array_float(json_member(node, "rotation"), 1U, 0.0F),
        json_array_float(json_member(node, "rotation"), 2U, 0.0F),
        json_array_float(json_member(node, "rotation"), 3U, 1.0F),
    };
    return mat4_mul(mat4_translate(TRANSLATION), mat4_mul(mat4_quaternion(rotation), mat4_scale(SCALE)));
}

void parse_skybox(const JsonValue& scene, GltfSceneInfo& out) {
    const JsonValue* extras = json_member(scene, "extras");
    const JsonValue* tracebench = extras != nullptr ? json_member(*extras, "tracebench") : nullptr;
    const JsonValue* skybox = tracebench != nullptr ? json_member(*tracebench, "skybox") : nullptr;
    if (skybox == nullptr || skybox->type != JsonValue::Type::JSON_OBJECT) {
        return;
    }
    std::string const TYPE = json_member_string(*skybox, "type");
    if (!TYPE.empty() && TYPE != "equirectangular") {
        return;
    }
    out.skybox_image = json_member_int(*skybox, "image", -1);
    out.skybox_intensity = std::max(0.0F, json_member_float(*skybox, "intensity", out.skybox_intensity));
}

auto parse_gltf_document(const JsonValue& root) -> GltfDocument {
    GltfDocument doc;
    doc.scene = json_member_int(root, "scene", 0);

    if (const JsonValue* views = json_member(root, "bufferViews"); views != nullptr && views->type == JsonValue::Type::JSON_ARRAY) {
        doc.buffer_views.reserve(views->array.size());
        for (const auto& item : views->array) {
            GltfBufferView view;
            view.buffer = json_member_int(item, "buffer", 0);
            view.byte_offset = json_member_size(item, "byteOffset", 0U);
            view.byte_length = json_member_size(item, "byteLength", 0U);
            view.byte_stride = json_member_size(item, "byteStride", 0U);
            doc.buffer_views.push_back(view);
        }
    }

    if (const JsonValue* accessors = json_member(root, "accessors");
        accessors != nullptr && accessors->type == JsonValue::Type::JSON_ARRAY) {
        doc.accessors.reserve(accessors->array.size());
        for (const auto& item : accessors->array) {
            GltfAccessor accessor;
            accessor.buffer_view = json_member_int(item, "bufferView", -1);
            accessor.byte_offset = json_member_size(item, "byteOffset", 0U);
            accessor.component_type = json_member_int(item, "componentType", 0);
            accessor.count = json_member_size(item, "count", 0U);
            accessor.type = json_member_string(item, "type");
            accessor.normalized = json_bool_value(json_member(item, "normalized"), false);
            doc.accessors.push_back(std::move(accessor));
        }
    }

    if (const JsonValue* images = json_member(root, "images"); images != nullptr && images->type == JsonValue::Type::JSON_ARRAY) {
        doc.images.reserve(images->array.size());
        for (const auto& item : images->array) {
            GltfImage image;
            image.buffer_view = json_member_int(item, "bufferView", -1);
            image.mime_type = json_member_string(item, "mimeType");
            doc.images.push_back(std::move(image));
        }
    }

    if (const JsonValue* textures = json_member(root, "textures"); textures != nullptr && textures->type == JsonValue::Type::JSON_ARRAY) {
        doc.textures.reserve(textures->array.size());
        for (const auto& item : textures->array) {
            doc.textures.push_back({.source = json_member_int(item, "source", -1)});
        }
    }

    if (const JsonValue* materials = json_member(root, "materials");
        materials != nullptr && materials->type == JsonValue::Type::JSON_ARRAY) {
        doc.materials.reserve(materials->array.size());
        for (const auto& item : materials->array) {
            Material material;
            if (const JsonValue* pbr = json_member(item, "pbrMetallicRoughness");
                pbr != nullptr && pbr->type == JsonValue::Type::JSON_OBJECT) {
                const JsonValue* base_color_factor = json_member(*pbr, "baseColorFactor");
                material.base_color = json_vec3_value(base_color_factor, material.base_color);
                material.base_alpha = std::clamp(json_array_float(base_color_factor, 3U, material.base_alpha), 0.0F, 1.0F);
                material.metallic = std::clamp(json_member_float(*pbr, "metallicFactor", material.metallic), 0.0F, 1.0F);
                material.roughness = std::clamp(json_member_float(*pbr, "roughnessFactor", material.roughness), 0.03F, 1.0F);
                material.base_color_texture = image_for_texture(doc, texture_index_from(*pbr, "baseColorTexture"));
                material.metallic_roughness_texture = image_for_texture(doc, texture_index_from(*pbr, "metallicRoughnessTexture"));
            }
            std::string const ALPHA_MODE = json_member_string(item, "alphaMode");
            if (ALPHA_MODE == "MASK") {
                material.alpha_mode = AlphaMode::MASK;
            } else if (ALPHA_MODE == "BLEND") {
                material.alpha_mode = AlphaMode::BLEND;
            }
            material.alpha_cutoff = std::clamp(json_member_float(item, "alphaCutoff", material.alpha_cutoff), 0.0F, 1.0F);
            material.normal_texture = image_for_texture(doc, texture_index_from(item, "normalTexture"));
            material.emissive = json_vec3_value(json_member(item, "emissiveFactor"), material.emissive);
            material.emissive_texture = image_for_texture(doc, texture_index_from(item, "emissiveTexture"));
            material.occlusion_texture = image_for_texture(doc, texture_index_from(item, "occlusionTexture"));
            doc.materials.push_back(material);
        }
    }

    if (const JsonValue* meshes = json_member(root, "meshes"); meshes != nullptr && meshes->type == JsonValue::Type::JSON_ARRAY) {
        doc.meshes.reserve(meshes->array.size());
        for (const auto& mesh_json : meshes->array) {
            GltfMesh mesh;
            if (const JsonValue* primitives = json_member(mesh_json, "primitives");
                primitives != nullptr && primitives->type == JsonValue::Type::JSON_ARRAY) {
                mesh.primitives.reserve(primitives->array.size());
                for (const auto& primitive_json : primitives->array) {
                    GltfPrimitive primitive;
                    primitive.indices = json_member_int(primitive_json, "indices", -1);
                    primitive.material = json_member_int(primitive_json, "material", -1);
                    primitive.mode = json_member_int(primitive_json, "mode", 4);
                    if (const JsonValue* attrs = json_member(primitive_json, "attributes"); attrs != nullptr) {
                        primitive.position = json_member_int(*attrs, "POSITION", -1);
                        primitive.normal = json_member_int(*attrs, "NORMAL", -1);
                        primitive.texcoord = json_member_int(*attrs, "TEXCOORD_0", -1);
                    }
                    mesh.primitives.push_back(primitive);
                }
            }
            doc.meshes.push_back(std::move(mesh));
        }
    }

    if (const JsonValue* cameras = json_member(root, "cameras"); cameras != nullptr && cameras->type == JsonValue::Type::JSON_ARRAY) {
        doc.cameras.reserve(cameras->array.size());
        for (const auto& camera_json : cameras->array) {
            GltfCamera camera;
            if (const JsonValue* perspective = json_member(camera_json, "perspective"); perspective != nullptr) {
                camera.yfov_degrees = json_member_float(*perspective, "yfov", camera.yfov_degrees * PI / 180.0F) * 180.0F / PI;
            }
            doc.cameras.push_back(camera);
        }
    }

    if (const JsonValue* nodes = json_member(root, "nodes"); nodes != nullptr && nodes->type == JsonValue::Type::JSON_ARRAY) {
        doc.nodes.reserve(nodes->array.size());
        for (const auto& node_json : nodes->array) {
            GltfNode node;
            node.mesh = json_member_int(node_json, "mesh", -1);
            node.camera = json_member_int(node_json, "camera", -1);
            node.children = json_int_array(json_member(node_json, "children"));
            node.local = parse_node_transform(node_json);
            doc.nodes.push_back(std::move(node));
        }
    }

    if (const JsonValue* scenes = json_member(root, "scenes"); scenes != nullptr && scenes->type == JsonValue::Type::JSON_ARRAY) {
        doc.scenes.reserve(scenes->array.size());
        for (const auto& scene_json : scenes->array) {
            GltfSceneInfo scene;
            scene.nodes = json_int_array(json_member(scene_json, "nodes"));
            parse_skybox(scene_json, scene);
            doc.scenes.push_back(std::move(scene));
        }
    }

    return doc;
}

auto component_size(int component_type) -> size_t {
    switch (component_type) {
        case 5120:
        case 5121:
            return 1U;
        case 5122:
        case 5123:
            return 2U;
        case 5125:
        case 5126:
            return 4U;
        default:
            return 0U;
    }
}

auto accessor_component_count(std::string_view type) -> size_t {
    if (type == "SCALAR") {
        return 1U;
    }
    if (type == "VEC2") {
        return 2U;
    }
    if (type == "VEC3") {
        return 3U;
    }
    if (type == "VEC4") {
        return 4U;
    }
    if (type == "MAT4") {
        return 16U;
    }
    return 0U;
}

auto buffer_view_bytes(const GltfDocument& doc, std::span<const uint8_t> bin, int view_index) -> std::optional<std::span<const uint8_t>> {
    if (view_index < 0 || static_cast<size_t>(view_index) >= doc.buffer_views.size()) {
        return std::nullopt;
    }
    const auto& view = doc.buffer_views[static_cast<size_t>(view_index)];
    if (view.buffer != 0 || view.byte_offset + view.byte_length > bin.size()) {
        return std::nullopt;
    }
    return bin.subspan(view.byte_offset, view.byte_length);
}

auto accessor_element_bytes(const GltfDocument& doc, std::span<const uint8_t> bin, int accessor_index, size_t element)
    -> std::optional<std::span<const uint8_t>> {
    if (accessor_index < 0 || static_cast<size_t>(accessor_index) >= doc.accessors.size()) {
        return std::nullopt;
    }
    const auto& accessor = doc.accessors[static_cast<size_t>(accessor_index)];
    if (element >= accessor.count || accessor.buffer_view < 0 || static_cast<size_t>(accessor.buffer_view) >= doc.buffer_views.size()) {
        return std::nullopt;
    }
    const auto& view = doc.buffer_views[static_cast<size_t>(accessor.buffer_view)];
    size_t const COMPONENT_SIZE = component_size(accessor.component_type);
    size_t const COMPONENTS = accessor_component_count(accessor.type);
    if (COMPONENT_SIZE == 0U || COMPONENTS == 0U || view.buffer != 0) {
        return std::nullopt;
    }
    size_t const ELEMENT_SIZE = COMPONENT_SIZE * COMPONENTS;
    size_t const STRIDE = view.byte_stride != 0U ? view.byte_stride : ELEMENT_SIZE;
    size_t const OFFSET = view.byte_offset + accessor.byte_offset + (element * STRIDE);
    size_t const VIEW_END = view.byte_offset + view.byte_length;
    if (OFFSET + ELEMENT_SIZE > bin.size() || OFFSET + ELEMENT_SIZE > VIEW_END) {
        return std::nullopt;
    }
    return bin.subspan(OFFSET, ELEMENT_SIZE);
}

auto read_u16_le(std::span<const uint8_t> bytes, size_t offset) -> uint16_t {
    if (offset + 2U > bytes.size()) {
        return 0U;
    }
    return static_cast<uint16_t>(bytes[offset]) | static_cast<uint16_t>(static_cast<uint16_t>(bytes[offset + 1U]) << 8U);
}

auto read_i16_le(std::span<const uint8_t> bytes, size_t offset) -> int16_t { return static_cast<int16_t>(read_u16_le(bytes, offset)); }

auto read_f32_le(std::span<const uint8_t> bytes, size_t offset) -> float {
    float out = 0.0F;
    if (offset + sizeof(out) <= bytes.size()) {
        uint32_t const RAW = read_u32_le(bytes, offset);
        std::memcpy(&out, &RAW, sizeof(out));
    }
    return out;
}

auto read_component_float(std::span<const uint8_t> bytes, int component_type, size_t component, bool normalized) -> float {
    size_t const OFFSET = component * component_size(component_type);
    switch (component_type) {
        case 5120: {
            int8_t const VALUE = static_cast<int8_t>(bytes[OFFSET]);
            return normalized ? std::max(-1.0F, static_cast<float>(VALUE) / 127.0F) : static_cast<float>(VALUE);
        }
        case 5121: {
            uint8_t const VALUE = bytes[OFFSET];
            return normalized ? static_cast<float>(VALUE) / 255.0F : static_cast<float>(VALUE);
        }
        case 5122: {
            int16_t const VALUE = read_i16_le(bytes, OFFSET);
            return normalized ? std::max(-1.0F, static_cast<float>(VALUE) / 32767.0F) : static_cast<float>(VALUE);
        }
        case 5123: {
            uint16_t const VALUE = read_u16_le(bytes, OFFSET);
            return normalized ? static_cast<float>(VALUE) / 65535.0F : static_cast<float>(VALUE);
        }
        case 5125: {
            uint32_t const VALUE = read_u32_le(bytes, OFFSET);
            return normalized ? static_cast<float>(static_cast<double>(VALUE) / 4294967295.0) : static_cast<float>(VALUE);
        }
        case 5126:
            return read_f32_le(bytes, OFFSET);
        default:
            return 0.0F;
    }
}

auto read_accessor_vec3(const GltfDocument& doc, std::span<const uint8_t> bin, int accessor_index, size_t element) -> std::optional<Vec3> {
    auto bytes = accessor_element_bytes(doc, bin, accessor_index, element);
    if (!bytes.has_value() || accessor_component_count(doc.accessors[static_cast<size_t>(accessor_index)].type) < 3U) {
        return std::nullopt;
    }
    const auto& accessor = doc.accessors[static_cast<size_t>(accessor_index)];
    return Vec3{
        read_component_float(*bytes, accessor.component_type, 0U, accessor.normalized),
        read_component_float(*bytes, accessor.component_type, 1U, accessor.normalized),
        read_component_float(*bytes, accessor.component_type, 2U, accessor.normalized),
    };
}

auto read_accessor_vec2(const GltfDocument& doc, std::span<const uint8_t> bin, int accessor_index, size_t element) -> std::optional<Vec2> {
    auto bytes = accessor_element_bytes(doc, bin, accessor_index, element);
    if (!bytes.has_value() || accessor_component_count(doc.accessors[static_cast<size_t>(accessor_index)].type) < 2U) {
        return std::nullopt;
    }
    const auto& accessor = doc.accessors[static_cast<size_t>(accessor_index)];
    return Vec2{
        read_component_float(*bytes, accessor.component_type, 0U, accessor.normalized),
        read_component_float(*bytes, accessor.component_type, 1U, accessor.normalized),
    };
}

auto read_accessor_index(const GltfDocument& doc, std::span<const uint8_t> bin, int accessor_index, size_t element)
    -> std::optional<uint32_t> {
    auto bytes = accessor_element_bytes(doc, bin, accessor_index, element);
    if (!bytes.has_value()) {
        return std::nullopt;
    }
    int const COMPONENT_TYPE = doc.accessors[static_cast<size_t>(accessor_index)].component_type;
    switch (COMPONENT_TYPE) {
        case 5121:
            return static_cast<uint32_t>((*bytes)[0]);
        case 5123:
            return static_cast<uint32_t>(read_u16_le(*bytes, 0U));
        case 5125:
            return read_u32_le(*bytes, 0U);
        default:
            return std::nullopt;
    }
}

auto atomic_write_text(const std::string& path, const std::string& text) -> bool {
    std::string const TMP = path + ".tmp";
    FILE* file = std::fopen(TMP.c_str(), "wb");
    if (file == nullptr) {
        return false;
    }
    bool ok = std::fwrite(text.data(), 1, text.size(), file) == text.size();
    ok = std::fclose(file) == 0 && ok;
    if (!ok) {
        return false;
    }
    return std::rename(TMP.c_str(), path.c_str()) == 0;
}

auto ensure_dir(const std::string& path) -> bool {
    if (path.empty()) {
        return false;
    }
    std::string current;
    for (char ch : path) {
        current.push_back(ch);
        if (ch != '/') {
            continue;
        }
        if (current.size() > 1 && ::mkdir(current.c_str(), 0755) != 0 && errno != EEXIST) {
            return false;
        }
    }
    return ::mkdir(path.c_str(), 0755) == 0 || errno == EEXIST;
}

auto tonemap(Vec3 value) -> Vec3 {
    value = {.x = value.x / (1.0F + value.x), .y = value.y / (1.0F + value.y), .z = value.z / (1.0F + value.z)};
    return {.x = std::pow(std::max(value.x, 0.0F), 1.0F / 2.2F),
            .y = std::pow(std::max(value.y, 0.0F), 1.0F / 2.2F),
            .z = std::pow(std::max(value.z, 0.0F), 1.0F / 2.2F)};
}

auto to_rgba8(FilmView film) -> std::vector<unsigned char> {
    std::vector<unsigned char> rgba(static_cast<size_t>(film.width) * static_cast<size_t>(film.height) * 4U);
    for (int y = 0; y < film.height; ++y) {
        for (int x = 0; x < film.width; ++x) {
            size_t const PIXEL = (static_cast<size_t>(y) * static_cast<size_t>(film.width)) + static_cast<size_t>(x);
            size_t const RGB = PIXEL * 3U;
            Vec3 const MAPPED = tonemap({.x = film.rgb[RGB], .y = film.rgb[RGB + 1], .z = film.rgb[RGB + 2]});
            rgba[(PIXEL * 4U) + 0U] = static_cast<unsigned char>(std::clamp(MAPPED.x * 255.0F, 0.0F, 255.0F));
            rgba[(PIXEL * 4U) + 1U] = static_cast<unsigned char>(std::clamp(MAPPED.y * 255.0F, 0.0F, 255.0F));
            rgba[(PIXEL * 4U) + 2U] = static_cast<unsigned char>(std::clamp(MAPPED.z * 255.0F, 0.0F, 255.0F));
            rgba[(PIXEL * 4U) + 3U] = 255;
        }
    }
    return rgba;
}

auto write_png_atomic(const std::string& path, FilmView film) -> bool {
    std::string const TMP = path + ".tmp";
    auto rgba = to_rgba8(film);
    unsigned const ERROR = lodepng::encode(TMP, rgba, static_cast<unsigned>(film.width), static_cast<unsigned>(film.height));
    if (ERROR != 0) {
        return false;
    }
    return std::rename(TMP.c_str(), path.c_str()) == 0;
}

auto downsample_preview(FilmView film, int target_width, int target_height) -> std::vector<float> {
    std::vector<float> storage(static_cast<size_t>(target_width) * static_cast<size_t>(target_height) * 3U);
    for (int y = 0; y < target_height; ++y) {
        int const SOURCE_Y = std::min(film.height - 1, static_cast<int>((static_cast<int64_t>(y) * film.height) / target_height));
        for (int x = 0; x < target_width; ++x) {
            int const SOURCE_X = std::min(film.width - 1, static_cast<int>((static_cast<int64_t>(x) * film.width) / target_width));
            size_t const SOURCE = ((static_cast<size_t>(SOURCE_Y) * static_cast<size_t>(film.width)) + static_cast<size_t>(SOURCE_X)) * 3U;
            size_t const TARGET = ((static_cast<size_t>(y) * static_cast<size_t>(target_width)) + static_cast<size_t>(x)) * 3U;
            storage[TARGET] = film.rgb[SOURCE];
            storage[TARGET + 1U] = film.rgb[SOURCE + 1U];
            storage[TARGET + 2U] = film.rgb[SOURCE + 2U];
        }
    }
    return storage;
}

auto write_preview_png_atomic(const std::string& path, FilmView film) -> bool {
    int const MAX_DIMENSION = std::max(film.width, film.height);
    if (MAX_DIMENSION <= PREVIEW_MAX_DIMENSION) {
        return write_png_atomic(path, film);
    }

    int const PREVIEW_WIDTH = std::max(1, static_cast<int>((static_cast<int64_t>(film.width) * PREVIEW_MAX_DIMENSION) / MAX_DIMENSION));
    int const PREVIEW_HEIGHT = std::max(1, static_cast<int>((static_cast<int64_t>(film.height) * PREVIEW_MAX_DIMENSION) / MAX_DIMENSION));
    auto preview_storage = downsample_preview(film, PREVIEW_WIDTH, PREVIEW_HEIGHT);
    FilmView preview{
        .width = PREVIEW_WIDTH,
        .height = PREVIEW_HEIGHT,
        .rgb = std::span<float>(preview_storage.data(), preview_storage.size()),
    };
    return write_png_atomic(path, preview);
}

}  // namespace

auto final_image_pixel_count(FilmView film, uint64_t& pixels) -> bool {
    if (film.width <= 0 || film.height <= 0) {
        return false;
    }
    auto const WIDTH = static_cast<uint64_t>(film.width);
    auto const HEIGHT = static_cast<uint64_t>(film.height);
    if (HEIGHT != 0 && WIDTH > std::numeric_limits<uint64_t>::max() / HEIGHT) {
        return false;
    }
    pixels = WIDTH * HEIGHT;
    return true;
}

auto film_storage_is_complete(FilmView film) -> bool {
    uint64_t pixels = 0;
    if (!final_image_pixel_count(film, pixels)) {
        return false;
    }
    if (pixels > std::numeric_limits<uint64_t>::max() / 3U) {
        return false;
    }
    uint64_t const REQUIRED_FLOATS = pixels * 3U;
    return REQUIRED_FLOATS <= static_cast<uint64_t>(film.rgb.size());
}

auto should_write_final_image(const Options& options, FilmView film) -> bool {
    if (!options.write_final_image) {
        return false;
    }
    uint64_t pixels = 0;
    if (!final_image_pixel_count(film, pixels)) {
        return false;
    }
    return options.final_image_max_pixels == 0 || pixels <= options.final_image_max_pixels;
}

struct Scene {
    std::vector<Material> materials;
    std::vector<Texture> textures;
    std::vector<Triangle> triangles;
    std::vector<int> triangle_indices;
    std::vector<BvhNode> bvh;
    Camera camera;
    float sky_intensity = 0.8F;
    int skybox_texture = -1;
    bool glb_valid = false;
    int embedded_png_textures = 0;

    auto build_node(int start, int end) -> int {
        int const NODE_INDEX = static_cast<int>(bvh.size());
        bvh.push_back({});
        Aabb bounds;
        for (int i = start; i < end; ++i) {
            bounds.include(triangle_bounds(triangles[static_cast<size_t>(triangle_indices[static_cast<size_t>(i)])]));
        }
        bvh[static_cast<size_t>(NODE_INDEX)].bounds = bounds;

        int const COUNT = end - start;
        if (COUNT <= 4) {
            auto& node = bvh[static_cast<size_t>(NODE_INDEX)];
            node.start = start;
            node.count = COUNT;
            return NODE_INDEX;
        }

        Aabb centroid_bounds;
        for (int i = start; i < end; ++i) {
            centroid_bounds.include(triangle_centroid(triangles[static_cast<size_t>(triangle_indices[static_cast<size_t>(i)])]));
        }
        Vec3 const EXTENT = centroid_bounds.max - centroid_bounds.min;
        int axis = 0;
        if (EXTENT.y > EXTENT.x && EXTENT.y >= EXTENT.z) {
            axis = 1;
        } else if (EXTENT.z > EXTENT.x) {
            axis = 2;
        }

        int const MID = start + (COUNT / 2);
        auto begin = triangle_indices.begin() + start;
        auto mid = triangle_indices.begin() + MID;
        auto end_it = triangle_indices.begin() + end;
        std::nth_element(begin, mid, end_it, [&](int lhs, int rhs) {
            return axis_value(triangle_centroid(triangles[static_cast<size_t>(lhs)]), axis) <
                   axis_value(triangle_centroid(triangles[static_cast<size_t>(rhs)]), axis);
        });
        int const LEFT = build_node(start, MID);
        int const RIGHT = build_node(MID, end);
        bvh[static_cast<size_t>(NODE_INDEX)].left = LEFT;
        bvh[static_cast<size_t>(NODE_INDEX)].right = RIGHT;
        return NODE_INDEX;
    }

    void build_bvh() {
        triangle_indices.resize(triangles.size());
        for (size_t i = 0; i < triangle_indices.size(); ++i) {
            triangle_indices[i] = static_cast<int>(i);
        }
        bvh.clear();
        if (!triangles.empty()) {
            (void)build_node(0, static_cast<int>(triangles.size()));
        }
    }
};

void decode_embedded_textures(Scene& scene, const GltfDocument& doc, std::span<const uint8_t> bin) {
    scene.textures.resize(doc.images.size());
    for (size_t i = 0; i < doc.images.size(); ++i) {
        const auto& image = doc.images[i];
        if (image.mime_type != "image/png") {
            continue;
        }
        auto bytes = buffer_view_bytes(doc, bin, image.buffer_view);
        if (!bytes.has_value()) {
            continue;
        }
        Texture texture;
        unsigned width = 0;
        unsigned height = 0;
        if (lodepng::decode(texture.rgba, width, height, bytes->data(), bytes->size()) != 0 || width == 0U || height == 0U) {
            continue;
        }
        texture.width = static_cast<int>(width);
        texture.height = static_cast<int>(height);
        scene.textures[i] = std::move(texture);
        ++scene.embedded_png_textures;
    }
}

void set_camera_basis(Scene& scene, Vec3 origin, Vec3 forward, Vec3 right, Vec3 up, float vfov_degrees) {
    forward = normalize(forward);
    right = normalize(right);
    up = normalize(up);
    if (length(forward) <= 0.0F || length(right) <= 0.0F || length(up) <= 0.0F) {
        return;
    }
    scene.camera.origin = origin;
    scene.camera.forward = forward;
    scene.camera.right = right;
    scene.camera.up = up;
    scene.camera.vfov_degrees = std::clamp(vfov_degrees, 10.0F, 120.0F);
}

void frame_camera(Scene& scene) {
    if (scene.triangles.empty()) {
        return;
    }
    Aabb bounds;
    for (const auto& tri : scene.triangles) {
        bounds.include(triangle_bounds(tri));
    }
    Vec3 const CENTER = (bounds.min + bounds.max) * 0.5F;
    float const RADIUS = std::max(0.5F, length(bounds.max - bounds.min) * 0.55F);
    Vec3 const ORIGIN = CENTER + Vec3{.x = 0.0F, .y = RADIUS * 0.35F, .z = RADIUS * 2.4F};
    Vec3 forward = normalize(CENTER - ORIGIN);
    Vec3 right = normalize(cross(forward, {.x = 0.0F, .y = 1.0F, .z = 0.0F}));
    if (length(right) <= 0.0F) {
        right = normalize(cross(forward, {.x = 1.0F, .y = 0.0F, .z = 0.0F}));
    }
    Vec3 const UP = normalize(cross(right, forward));
    set_camera_basis(scene, ORIGIN, forward, right, UP, scene.camera.vfov_degrees);
}

auto primitive_material_index(const Scene& scene, int material_index) -> int {
    if (material_index < 0 || static_cast<size_t>(material_index) >= scene.materials.size()) {
        return 0;
    }
    return material_index;
}

void append_primitive(Scene& scene, const GltfDocument& doc, std::span<const uint8_t> bin, const GltfPrimitive& primitive,
                      const Mat4& transform) {
    if (primitive.mode != 4 || primitive.position < 0 || static_cast<size_t>(primitive.position) >= doc.accessors.size()) {
        return;
    }

    const auto& position_accessor = doc.accessors[static_cast<size_t>(primitive.position)];
    size_t element_count = position_accessor.count;
    if (primitive.indices >= 0 && static_cast<size_t>(primitive.indices) < doc.accessors.size()) {
        element_count = doc.accessors[static_cast<size_t>(primitive.indices)].count;
    }
    if (element_count < 3U) {
        return;
    }

    int const MATERIAL = primitive_material_index(scene, primitive.material);
    auto vertex_index = [&](size_t element) -> std::optional<size_t> {
        if (primitive.indices < 0) {
            return element;
        }
        auto index = read_accessor_index(doc, bin, primitive.indices, element);
        if (!index.has_value()) {
            return std::nullopt;
        }
        return static_cast<size_t>(*index);
    };

    auto read_uv = [&](size_t index) -> Vec2 {
        if (primitive.texcoord < 0) {
            return {};
        }
        return read_accessor_vec2(doc, bin, primitive.texcoord, index).value_or(Vec2{});
    };

    auto read_normal = [&](size_t index, Vec3 fallback) -> Vec3 {
        if (primitive.normal < 0) {
            return fallback;
        }
        auto normal = read_accessor_vec3(doc, bin, primitive.normal, index);
        if (!normal.has_value()) {
            return fallback;
        }
        Vec3 const WORLD = normalize(transform_vector(transform, *normal));
        return length(WORLD) > 0.0F ? WORLD : fallback;
    };

    for (size_t element = 0; element + 2U < element_count; element += 3U) {
        auto ia = vertex_index(element + 0U);
        auto ib = vertex_index(element + 1U);
        auto ic = vertex_index(element + 2U);
        if (!ia.has_value() || !ib.has_value() || !ic.has_value()) {
            continue;
        }
        auto pa_local = read_accessor_vec3(doc, bin, primitive.position, *ia);
        auto pb_local = read_accessor_vec3(doc, bin, primitive.position, *ib);
        auto pc_local = read_accessor_vec3(doc, bin, primitive.position, *ic);
        if (!pa_local.has_value() || !pb_local.has_value() || !pc_local.has_value()) {
            continue;
        }

        Vec3 const PA = transform_point(transform, *pa_local);
        Vec3 const PB = transform_point(transform, *pb_local);
        Vec3 const PC = transform_point(transform, *pc_local);
        Vec3 const GEOMETRIC = normalize(cross(PB - PA, PC - PA));
        if (length(GEOMETRIC) <= 0.0F) {
            continue;
        }
        Vec2 const UVA = read_uv(*ia);
        Vec2 const UVB = read_uv(*ib);
        Vec2 const UVC = read_uv(*ic);
        auto const BASIS = triangle_basis(PA, PB, PC, UVA, UVB, UVC, GEOMETRIC);

        scene.triangles.push_back({
            .a = PA,
            .b = PB,
            .c = PC,
            .normal = GEOMETRIC,
            .n0 = read_normal(*ia, GEOMETRIC),
            .n1 = read_normal(*ib, GEOMETRIC),
            .n2 = read_normal(*ic, GEOMETRIC),
            .uv0 = UVA,
            .uv1 = UVB,
            .uv2 = UVC,
            .tangent = BASIS[0],
            .bitangent = BASIS[1],
            .material = MATERIAL,
        });
    }
}

void append_mesh(Scene& scene, const GltfDocument& doc, std::span<const uint8_t> bin, int mesh_index, const Mat4& transform) {
    if (mesh_index < 0 || static_cast<size_t>(mesh_index) >= doc.meshes.size()) {
        return;
    }
    for (const auto& primitive : doc.meshes[static_cast<size_t>(mesh_index)].primitives) {
        append_primitive(scene, doc, bin, primitive, transform);
    }
}

void append_node(Scene& scene, const GltfDocument& doc, std::span<const uint8_t> bin, int node_index, const Mat4& parent, bool& camera_set,
                 int depth) {
    if (node_index < 0 || static_cast<size_t>(node_index) >= doc.nodes.size() || depth > 64) {
        return;
    }

    const auto& node = doc.nodes[static_cast<size_t>(node_index)];
    Mat4 const TRANSFORM = mat4_mul(parent, node.local);
    if (!camera_set && node.camera >= 0 && static_cast<size_t>(node.camera) < doc.cameras.size()) {
        set_camera_basis(scene, transform_point(TRANSFORM, {}), transform_vector(TRANSFORM, {0.0F, 0.0F, -1.0F}),
                         transform_vector(TRANSFORM, {.x = 1.0F, .y = 0.0F, .z = 0.0F}), transform_vector(TRANSFORM, {0.0F, 1.0F, 0.0F}),
                         doc.cameras[static_cast<size_t>(node.camera)].yfov_degrees);
        camera_set = true;
    }
    append_mesh(scene, doc, bin, node.mesh, TRANSFORM);
    for (int child : node.children) {
        append_node(scene, doc, bin, child, TRANSFORM, camera_set, depth + 1);
    }
}

void append_scene_geometry(Scene& scene, const GltfDocument& doc, std::span<const uint8_t> bin, bool& camera_set) {
    int const ACTIVE = doc.scene >= 0 && static_cast<size_t>(doc.scene) < doc.scenes.size() ? doc.scene : 0;
    if (!doc.scenes.empty()) {
        const auto& active_scene = doc.scenes[static_cast<size_t>(ACTIVE)];
        for (int node : active_scene.nodes) {
            append_node(scene, doc, bin, node, Mat4::identity(), camera_set, 0);
        }
    }

    if (!scene.triangles.empty()) {
        return;
    }

    Mat4 const IDENTITY = Mat4::identity();
    for (size_t mesh = 0; mesh < doc.meshes.size(); ++mesh) {
        append_mesh(scene, doc, bin, static_cast<int>(mesh), IDENTITY);
    }
}

auto load_glb_scene(std::span<const uint8_t> bytes) -> std::optional<Scene> {
    auto chunks = read_glb_chunks(bytes);
    if (!chunks.has_value()) {
        return std::nullopt;
    }
    JsonParser parser(chunks->json);
    auto root = parser.parse();
    if (!root.has_value()) {
        return std::nullopt;
    }

    GltfDocument doc = parse_gltf_document(*root);
    Scene scene;
    scene.glb_valid = true;
    decode_embedded_textures(scene, doc, chunks->bin);
    scene.materials = doc.materials;
    if (scene.materials.empty()) {
        scene.materials.push_back({});
    }

    int const ACTIVE = doc.scene >= 0 && static_cast<size_t>(doc.scene) < doc.scenes.size() ? doc.scene : 0;
    if (!doc.scenes.empty()) {
        const auto& active_scene = doc.scenes[static_cast<size_t>(ACTIVE)];
        scene.sky_intensity = active_scene.skybox_intensity;
        if (active_scene.skybox_image >= 0 && static_cast<size_t>(active_scene.skybox_image) < scene.textures.size()) {
            scene.skybox_texture = active_scene.skybox_image;
        }
    }

    bool camera_set = false;
    append_scene_geometry(scene, doc, chunks->bin, camera_set);
    if (scene.triangles.empty()) {
        return std::nullopt;
    }
    if (!camera_set) {
        frame_camera(scene);
    }
    scene.build_bvh();
    return scene;
}

struct Hit {
    float t = std::numeric_limits<float>::max();
    Vec3 position;
    Vec3 normal;
    Vec3 geometric_normal;
    Vec2 uv;
    float u = 0.0F;
    float v = 0.0F;
    float w = 0.0F;
    Vec3 tangent;
    Vec3 bitangent;
    int material = 0;
    bool hit = false;
};

auto intersect_scene(const Scene& scene, const Ray& ray) -> Hit {
    Hit closest;
    if (scene.bvh.empty()) {
        return closest;
    }

    std::array<int, 64> stack{};
    int stack_size = 0;
    stack[static_cast<size_t>(stack_size++)] = 0;
    while (stack_size > 0) {
        int const NODE_INDEX = stack[static_cast<size_t>(--stack_size)];
        const auto& node = scene.bvh[static_cast<size_t>(NODE_INDEX)];
        if (!intersect_aabb(node.bounds, ray, closest.t)) {
            continue;
        }
        if (node.count > 0) {
            for (int i = 0; i < node.count; ++i) {
                int const TRI_INDEX = scene.triangle_indices[static_cast<size_t>(node.start) + i];
                const auto& tri = scene.triangles[static_cast<size_t>(TRI_INDEX)];
                float t = 0.0F;
                float u = 0.0F;
                float v = 0.0F;
                if (intersect_triangle(tri, ray, t, u, v) && t < closest.t) {
                    float const W = 1.0F - u - v;
                    Vec3 const GEOMETRIC_NORMAL = face_forward(tri.normal, ray.direction);
                    Vec3 normal = normalize((tri.n0 * W) + (tri.n1 * u) + (tri.n2 * v));
                    if (length(normal) <= 0.0F) {
                        normal = tri.normal;
                    }
                    if (dot(normal, GEOMETRIC_NORMAL) < 0.0F) {
                        normal = normal * -1.0F;
                    }
                    closest.t = t;
                    closest.position = ray.origin + (ray.direction * t);
                    closest.normal = normal;
                    closest.geometric_normal = GEOMETRIC_NORMAL;
                    closest.uv = (tri.uv0 * W) + (tri.uv1 * u) + (tri.uv2 * v);
                    closest.u = u;
                    closest.v = v;
                    closest.w = W;
                    closest.tangent = normalize(tri.tangent - (closest.normal * dot(tri.tangent, closest.normal)));
                    if (length(closest.tangent) <= 0.0F) {
                        closest.tangent = fallback_basis(closest.normal)[0];
                    }
                    closest.bitangent = normalize(cross(closest.normal, closest.tangent));
                    closest.material = tri.material;
                    closest.hit = true;
                }
            }
        } else {
            if (node.left >= 0 && stack_size < static_cast<int>(stack.size())) {
                stack[static_cast<size_t>(stack_size++)] = node.left;
            }
            if (node.right >= 0 && stack_size < static_cast<int>(stack.size())) {
                stack[static_cast<size_t>(stack_size++)] = node.right;
            }
        }
    }
    return closest;
}

auto cosine_hemisphere(const Vec3& normal, Rng& rng) -> Vec3 {
    float const R1 = rng.uniform();
    float const R2 = rng.uniform();
    float const PHI = 2.0F * PI * R1;
    float const R = std::sqrt(R2);
    Vec3 const LOCAL{.x = std::cos(PHI) * R, .y = std::sin(PHI) * R, .z = std::sqrt(std::max(0.0F, 1.0F - R2))};
    Vec3 const TANGENT =
        normalize(cross(std::fabs(normal.x) > 0.5F ? Vec3{.x = 0.0F, .y = 1.0F, .z = 0.0F} : Vec3{1.0F, 0.0F, 0.0F}, normal));
    Vec3 const BITANGENT = cross(normal, TANGENT);
    return normalize((TANGENT * LOCAL.x) + (BITANGENT * LOCAL.y) + (normal * LOCAL.z));
}

auto clamp_to_geometric_hemisphere(const Vec3& direction, const Vec3& geometric_normal) -> Vec3 {
    float const D = dot(direction, geometric_normal);
    if (D >= MIN_GEOMETRIC_OUTGOING_DOT) {
        return direction;
    }

    Vec3 const TANGENT = direction - (geometric_normal * D);
    if (length(TANGENT) <= 1.0e-6F) {
        return geometric_normal;
    }
    return normalize(TANGENT + (geometric_normal * MIN_GEOMETRIC_OUTGOING_DOT));
}

auto spawn_surface_ray(const Hit& hit, const Vec3& direction) -> Ray {
    Vec3 const OUT_DIR = clamp_to_geometric_hemisphere(direction, hit.geometric_normal);
    return {.origin = hit.position + (hit.geometric_normal * EPSILON) + (OUT_DIR * EPSILON), .direction = OUT_DIR};
}

auto sample_scene_texture(const Scene& scene, int texture_index, Vec2 uv, bool alpha_weighted_rgb = false) -> std::array<float, 4> {
    if (texture_index < 0 || static_cast<size_t>(texture_index) >= scene.textures.size()) {
        return {1.0F, 1.0F, 1.0F, 1.0F};
    }
    return scene.textures[static_cast<size_t>(texture_index)].sample(uv, alpha_weighted_rgb);
}

struct ShadedMaterial {
    Vec3 base_color;
    Vec3 emissive;
    float alpha = 1.0F;
    float metallic = 0.0F;
    float roughness = 0.7F;
    float base_texture_luminance = 1.0F;
    float base_texture_alpha = 1.0F;
    bool sampled_base_texture = false;
};

auto shade_material(const Scene& scene, const Material& material, Vec2 uv) -> ShadedMaterial {
    ShadedMaterial shaded;
    shaded.base_color = material.base_color;
    shaded.emissive = material.emissive;
    shaded.alpha = material.base_alpha;
    shaded.metallic = material.metallic;
    shaded.roughness = material.roughness;

    bool const ALPHA_AWARE = material.alpha_mode != AlphaMode::OPAQUE;
    auto const BASE = sample_scene_texture(scene, material.base_color_texture, uv, ALPHA_AWARE);
    Vec3 const BASE_RGB{.x = BASE[0], .y = BASE[1], .z = BASE[2]};
    shaded.sampled_base_texture = material.base_color_texture >= 0;
    shaded.base_texture_luminance = luminance(BASE_RGB);
    shaded.base_texture_alpha = BASE[3];
    shaded.base_color = shaded.base_color * BASE_RGB;
    shaded.alpha = std::clamp(shaded.alpha * BASE[3], 0.0F, 1.0F);

    auto const MR = sample_scene_texture(scene, material.metallic_roughness_texture, uv);
    shaded.roughness = std::clamp(shaded.roughness * MR[1], 0.03F, 1.0F);
    shaded.metallic = std::clamp(shaded.metallic * MR[2], 0.0F, 1.0F);

    auto const EMISSIVE = sample_scene_texture(scene, material.emissive_texture, uv);
    shaded.emissive = shaded.emissive * Vec3{.x = EMISSIVE[0], .y = EMISSIVE[1], .z = EMISSIVE[2]};

    auto const OCCLUSION = sample_scene_texture(scene, material.occlusion_texture, uv);
    shaded.base_color = shaded.base_color * std::clamp(OCCLUSION[0], 0.2F, 1.0F);
    return shaded;
}

auto skip_alpha_hit(const Material& material, const ShadedMaterial& shaded, Rng& rng) -> bool {
    if (material.alpha_mode == AlphaMode::OPAQUE) {
        return false;
    }
    if (material.alpha_mode == AlphaMode::MASK) {
        return shaded.alpha < material.alpha_cutoff;
    }
    return rng.uniform() >= shaded.alpha;
}

auto debug_color_for_hit(const Material& material, const Hit& hit, const ShadedMaterial& shaded) -> std::optional<Vec3> {
    if (shaded.sampled_base_texture && shaded.base_texture_luminance <= DEBUG_BLACK_TEXTURE_LUMINANCE) {
        return Vec3{.x = 18.0F, .y = 0.0F, .z = 18.0F};
    }

    float const EDGE_DISTANCE = std::min({hit.u, hit.v, hit.w});
    if (EDGE_DISTANCE <= DEBUG_TRIANGLE_EDGE_DISTANCE) {
        return Vec3{.x = 18.0F, .y = 18.0F, .z = 0.0F};
    }

    if (shaded.alpha < 0.98F) {
        if (material.alpha_mode == AlphaMode::OPAQUE) {
            return Vec3{.x = 0.0F, .y = 18.0F, .z = 0.0F};
        }
        return Vec3{.x = 0.0F, .y = 18.0F, .z = 18.0F};
    }

    return std::nullopt;
}

auto shade_normal(const Scene& scene, const Material& material, const Hit& hit) -> Vec3 {
    if (material.normal_texture < 0) {
        return hit.normal;
    }
    auto const SAMPLE = sample_scene_texture(scene, material.normal_texture, hit.uv);
    Vec3 const TANGENT_SPACE{
        (SAMPLE[0] * 2.0F) - 1.0F,
        (SAMPLE[1] * 2.0F) - 1.0F,
        (SAMPLE[2] * 2.0F) - 1.0F,
    };
    Vec3 normal = normalize((hit.tangent * TANGENT_SPACE.x) + (hit.bitangent * TANGENT_SPACE.y) + (hit.normal * TANGENT_SPACE.z));
    if (length(normal) <= 0.0F) {
        return hit.normal;
    }
    return dot(normal, hit.geometric_normal) >= 0.0F ? normal : normal * -1.0F;
}

auto environment(const Scene& scene, const Vec3& dir) -> Vec3 {
    if (scene.skybox_texture >= 0 && static_cast<size_t>(scene.skybox_texture) < scene.textures.size() &&
        scene.textures[static_cast<size_t>(scene.skybox_texture)].valid()) {
        Vec3 const D = normalize(dir);
        float const U = (std::atan2(D.z, D.x) / (2.0F * PI)) + 0.5F;
        float const V = std::acos(std::clamp(D.y, -1.0F, 1.0F)) / PI;
        auto const SKY = scene.textures[static_cast<size_t>(scene.skybox_texture)].sample({U, V});
        return Vec3{SKY[0], SKY[1], SKY[2]} * scene.sky_intensity;
    }
    float const T = 0.5F * (dir.y + 1.0F);
    Vec3 const HORIZON{0.65F, 0.72F, 0.80F};
    Vec3 const ZENITH{0.10F, 0.16F, 0.26F};
    return (HORIZON * (1.0F - T) + ZENITH * T) * scene.sky_intensity;
}

auto trace_ray(const Scene& scene, Ray ray, const Options& options, Rng& rng) -> Vec3 {
    Vec3 radiance{};
    Vec3 throughput{1.0F, 1.0F, 1.0F};
    std::optional<Vec3> dark_primary_debug_color;
    std::optional<Vec3> grazing_primary_debug_color;
    int transparent_hops = 0;
    for (int depth = 0; depth < options.max_depth;) {
        Hit const hit = intersect_scene(scene, ray);
        if (!hit.hit) {
            radiance += throughput * environment(scene, ray.direction);
            break;
        }

        const auto& material = scene.materials[static_cast<size_t>(hit.material)];
        ShadedMaterial const SHADED = shade_material(scene, material, hit.uv);
        if (skip_alpha_hit(material, SHADED, rng)) {
            if (options.debug_edge_colors && depth == 0) {
                return {.x = 0.0F, .y = 18.0F, .z = 18.0F};
            }
            if (++transparent_hops > MAX_TRANSPARENT_HOPS) {
                radiance += throughput * environment(scene, ray.direction);
                break;
            }
            ray = {.origin = hit.position + (ray.direction * EPSILON), .direction = ray.direction};
            continue;
        }
        transparent_hops = 0;

        if (options.debug_edge_colors && depth == 0) {
            if (auto debug_color = debug_color_for_hit(material, hit, SHADED); debug_color.has_value()) {
                return *debug_color;
            }
        }

        Vec3 const SURFACE_NORMAL = shade_normal(scene, material, hit);
        if (options.debug_edge_colors && depth == 0) {
            if (dot(SURFACE_NORMAL, hit.normal) <= DEBUG_NORMAL_DIVERGENCE_DOT) {
                return Vec3{.x = 18.0F, .y = 4.0F, .z = 0.0F};
            }
            if (std::fabs(dot(hit.normal, ray.direction)) <= DEBUG_GRAZING_VIEW_DOT) {
                grazing_primary_debug_color = Vec3{.x = 0.0F, .y = 0.0F, .z = 18.0F};
            }
            if (luminance(SHADED.base_color) > DEBUG_DARK_BASE_LUMINANCE) {
                dark_primary_debug_color = Vec3{.x = 18.0F, .y = 8.0F, .z = 0.0F};
            }
        }

        if (luminance(SHADED.emissive) > 0.0F) {
            radiance += throughput * SHADED.emissive;
            break;
        }

        float const SPECULAR_PROB = std::clamp(0.06F + (SHADED.metallic * 0.55F) + ((1.0F - SHADED.roughness) * 0.18F), 0.05F, 0.9F);
        Vec3 next_dir;
        if (rng.uniform() < SPECULAR_PROB) {
            Vec3 const REFLECTED = reflect(ray.direction, SURFACE_NORMAL);
            Vec3 const GLOSS = cosine_hemisphere(REFLECTED, rng);
            next_dir = normalize((REFLECTED * (1.0F - SHADED.roughness)) + (GLOSS * SHADED.roughness));
            throughput = throughput *
                         ((SHADED.base_color * SHADED.metallic) + (Vec3{.x = 0.04F, .y = 0.04F, .z = 0.04F} * (1.0F - SHADED.metallic)));
        } else {
            next_dir = cosine_hemisphere(SURFACE_NORMAL, rng);
            throughput = throughput * SHADED.base_color;
        }

        ray = spawn_surface_ray(hit, next_dir);
        if (depth >= 3) {
            float const P = std::clamp(luminance(throughput), 0.1F, 0.95F);
            if (rng.uniform() > P) {
                break;
            }
            throughput = throughput / P;
        }
        ++depth;
    }
    if (options.debug_edge_colors && dark_primary_debug_color.has_value() && luminance(radiance) <= DEBUG_DARK_RESULT_LUMINANCE) {
        return *dark_primary_debug_color;
    }
    if (options.debug_edge_colors && grazing_primary_debug_color.has_value()) {
        return *grazing_primary_debug_color;
    }
    return radiance;
}

auto default_scene() -> Scene {
    Scene scene;
    scene.materials = {
        {.base_color = {.x = 0.76F, .y = 0.74F, .z = 0.68F}, .roughness = 0.85F},
        {.base_color = {.x = 0.85F, .y = 0.18F, .z = 0.14F}, .roughness = 0.7F},
        {.base_color = {.x = 0.20F, .y = 0.55F, .z = 0.78F}, .roughness = 0.62F},
        {.base_color = {.x = 0.75F, .y = 0.68F, .z = 0.52F}, .metallic = 0.6F, .roughness = 0.24F},
        {.base_color = {.x = 1.0F, .y = 0.92F, .z = 0.78F}, .emissive = {8.0F, 7.1F, 5.6F}, .roughness = 0.2F},
    };
    add_quad(scene.triangles, {.x = -2.6F, .y = 0.0F, .z = -2.6F}, {.x = 2.6F, .y = 0.0F, .z = -2.6F}, {.x = 2.6F, .y = 0.0F, .z = 2.6F},
             {.x = -2.6F, .y = 0.0F, .z = 2.6F}, 0);
    add_quad(scene.triangles, {.x = -2.6F, .y = 0.0F, .z = -2.6F}, {.x = -2.6F, .y = 2.6F, .z = -2.6F}, {.x = 2.6F, .y = 2.6F, .z = -2.6F},
             {.x = 2.6F, .y = 0.0F, .z = -2.6F}, 0);
    add_quad(scene.triangles, {.x = -2.6F, .y = 2.6F, .z = -2.6F}, {.x = -2.6F, .y = 2.6F, .z = 2.6F}, {.x = 2.6F, .y = 2.6F, .z = 2.6F},
             {.x = 2.6F, .y = 2.6F, .z = -2.6F}, 0);
    add_quad(scene.triangles, {.x = -2.6F, .y = 0.0F, .z = 2.6F}, {.x = -2.6F, .y = 2.6F, .z = 2.6F}, {.x = -2.6F, .y = 2.6F, .z = -2.6F},
             {.x = -2.6F, .y = 0.0F, .z = -2.6F}, 1);
    add_quad(scene.triangles, {.x = 2.6F, .y = 0.0F, .z = -2.6F}, {.x = 2.6F, .y = 2.6F, .z = -2.6F}, {.x = 2.6F, .y = 2.6F, .z = 2.6F},
             {.x = 2.6F, .y = 0.0F, .z = 2.6F}, 2);
    add_quad(scene.triangles, {.x = -0.55F, .y = 2.58F, .z = -1.25F}, {.x = 0.55F, .y = 2.58F, .z = -1.25F},
             {.x = 0.55F, .y = 2.58F, .z = -0.25F}, {.x = -0.55F, .y = 2.58F, .z = -0.25F}, 4);
    add_box(scene.triangles, {.x = -1.2F, .y = 0.0F, .z = -1.4F}, {.x = -0.25F, .y = 1.1F, .z = -0.45F}, 3);
    add_box(scene.triangles, {.x = 0.45F, .y = 0.0F, .z = -1.8F}, {.x = 1.25F, .y = 1.65F, .z = -0.95F}, 0);
    scene.camera.forward = normalize(scene.camera.forward);
    scene.camera.right = normalize(cross(scene.camera.forward, {.x = 0.0F, .y = 1.0F, .z = 0.0F}));
    scene.camera.up = normalize(cross(scene.camera.right, scene.camera.forward));
    scene.build_bvh();
    return scene;
}

}  // namespace tracebench

namespace tracebench {

void FilmView::set_pixel(int x, int y, float r, float g, float b) const {
    size_t const PIXEL = (static_cast<size_t>(y) * static_cast<size_t>(width)) + static_cast<size_t>(x);
    size_t const OFFSET = PIXEL * 3U;
    rgb[OFFSET] = r;
    rgb[OFFSET + 1U] = g;
    rgb[OFFSET + 2U] = b;
}

auto backend_name(Backend backend) -> const char* { return backend == Backend::Ipc ? "ipc" : "mpi"; }

auto placement_name(Placement placement) -> const char* {
    return placement == Placement::NodeThreads ? "node-threads" : "process-per-core";
}

auto debug_render_mode_name(const Options& options) -> const char* {
    return options.debug_constant_tile_us > 0 ? "constant-tile" : "pathtraced";
}

namespace {

auto is_option_token(const char* value) -> bool { return value != nullptr && std::string_view(value).starts_with("--"); }

auto require_option_value(int argc, char* const* argv, int& index, std::string_view name) -> const char* {
    if (index + 1 >= argc || is_option_token(argv[index + 1])) {
        std::fprintf(stderr, "renderbench: missing value for %.*s\n", static_cast<int>(name.size()), name.data());
        return nullptr;
    }
    return argv[++index];
}

auto parse_int_option(std::string_view name, const char* value, int minimum, int& out) -> bool {
    if (value == nullptr || value[0] == '\0') {
        std::fprintf(stderr, "renderbench: invalid value for %.*s\n", static_cast<int>(name.size()), name.data());
        return false;
    }

    errno = 0;
    char* end = nullptr;
    long const PARSED = std::strtol(value, &end, 10);
    if (end == value || *end != '\0' || errno == ERANGE || PARSED > std::numeric_limits<int>::max()) {
        std::fprintf(stderr, "renderbench: invalid value for %.*s: '%s'\n", static_cast<int>(name.size()), name.data(), value);
        return false;
    }
    if (PARSED < minimum) {
        std::fprintf(stderr, "renderbench: %.*s must be >= %d\n", static_cast<int>(name.size()), name.data(), minimum);
        return false;
    }

    out = static_cast<int>(PARSED);
    return true;
}

}  // namespace

void print_usage(const char* argv0) {
    std::fprintf(stderr,
                 "usage: %s --scene scene.glb --backend ipc|mpi --placement node-threads|process-per-core "
                 "--width N --height N --spp N --max-depth N --tile-size N --output-root PATH [--threads N] "
                 "[--debug-edge-colors] [--debug-constant-tile-us N] [--debug-node-thread-batch-size N] "
                 "[--coordinator-reserve-cpus N] [--node-worker-reserve-cpus N] [--coordinator-skip-local-worker] "
                 "[--disable-worker-output-queue|--enable-worker-output-queue] "
                 "[--disable-single-thread-worker-queue|--enable-single-thread-worker-queue] "
                 "[--enable-process-persistent-workers|--disable-process-persistent-workers] "
                 "[--disable-final-image|--enable-final-image] [--final-image-max-pixels N] [--live]\n",
                 argv0 != nullptr ? argv0 : "renderbench");
}

auto parse_options(int argc, char* const* argv, Backend default_backend, Options& options) -> ParseStatus {
    options = {};
    options.backend = default_backend;
    for (int i = 1; i < argc; ++i) {
        std::string_view const ARG = argv[i];
        auto parse_required_int = [&](int minimum, int& out) -> bool {
            const char* const VALUE = require_option_value(argc, argv, i, ARG);
            return VALUE != nullptr && parse_int_option(ARG, VALUE, minimum, out);
        };
        if (ARG == "--scene") {
            const char* const VALUE = require_option_value(argc, argv, i, ARG);
            if (VALUE == nullptr) {
                return ParseStatus::Error;
            }
            options.scene_path = VALUE;
        } else if (ARG == "--backend") {
            const char* const VALUE = require_option_value(argc, argv, i, ARG);
            if (VALUE == nullptr) {
                return ParseStatus::Error;
            }
            std::string_view const VALUE_VIEW = VALUE;
            if (VALUE_VIEW == "mpi") {
                options.backend = Backend::Mpi;
            } else if (VALUE_VIEW == "ipc") {
                options.backend = Backend::Ipc;
            } else {
                std::fprintf(stderr, "renderbench: invalid --backend '%s' (expected ipc or mpi)\n", VALUE);
                return ParseStatus::Error;
            }
        } else if (ARG == "--placement") {
            const char* const VALUE = require_option_value(argc, argv, i, ARG);
            if (VALUE == nullptr) {
                return ParseStatus::Error;
            }
            std::string_view const VALUE_VIEW = VALUE;
            if (VALUE_VIEW == "process-per-core") {
                options.placement = Placement::ProcessPerCore;
            } else if (VALUE_VIEW == "node-threads") {
                options.placement = Placement::NodeThreads;
            } else {
                std::fprintf(stderr, "renderbench: invalid --placement '%s' (expected node-threads or process-per-core)\n", VALUE);
                return ParseStatus::Error;
            }
        } else if (ARG == "--width") {
            if (!parse_required_int(1, options.width)) {
                return ParseStatus::Error;
            }
        } else if (ARG == "--height") {
            if (!parse_required_int(1, options.height)) {
                return ParseStatus::Error;
            }
        } else if (ARG == "--spp") {
            if (!parse_required_int(1, options.spp)) {
                return ParseStatus::Error;
            }
        } else if (ARG == "--max-depth") {
            if (!parse_required_int(1, options.max_depth)) {
                return ParseStatus::Error;
            }
        } else if (ARG == "--tile-size") {
            if (!parse_required_int(4, options.tile_size)) {
                return ParseStatus::Error;
            }
            options.tile_size_explicit = true;
        } else if (ARG == "--output-root") {
            const char* const VALUE = require_option_value(argc, argv, i, ARG);
            if (VALUE == nullptr) {
                return ParseStatus::Error;
            }
            options.output_root = VALUE;
        } else if (ARG == "--threads") {
            if (!parse_required_int(0, options.threads)) {
                return ParseStatus::Error;
            }
        } else if (ARG == "--run-id") {
            const char* const VALUE = require_option_value(argc, argv, i, ARG);
            if (VALUE == nullptr) {
                return ParseStatus::Error;
            }
            options.run_id = VALUE;
        } else if (ARG == "--debug-edge-colors") {
            options.debug_edge_colors = true;
        } else if (ARG == "--debug-constant-tile-us") {
            if (!parse_required_int(0, options.debug_constant_tile_us)) {
                return ParseStatus::Error;
            }
        } else if (ARG == "--debug-node-thread-batch-size") {
            if (!parse_required_int(0, options.debug_node_thread_batch_size)) {
                return ParseStatus::Error;
            }
        } else if (ARG == "--coordinator-reserve-cpus") {
            if (!parse_required_int(0, options.coordinator_reserve_cpus)) {
                return ParseStatus::Error;
            }
        } else if (ARG == "--node-worker-reserve-cpus") {
            if (!parse_required_int(0, options.node_worker_reserve_cpus)) {
                return ParseStatus::Error;
            }
        } else if (ARG == "--coordinator-skip-local-worker") {
            options.coordinator_skip_local_worker = true;
        } else if (ARG == "--disable-worker-output-queue") {
            options.disable_worker_output_queue = true;
        } else if (ARG == "--enable-worker-output-queue") {
            options.disable_worker_output_queue = false;
        } else if (ARG == "--disable-single-thread-worker-queue") {
            options.disable_single_thread_worker_queue = true;
        } else if (ARG == "--enable-single-thread-worker-queue") {
            options.disable_single_thread_worker_queue = false;
        } else if (ARG == "--enable-process-persistent-workers") {
            options.process_persistent_workers = true;
        } else if (ARG == "--disable-process-persistent-workers") {
            options.process_persistent_workers = false;
        } else if (ARG == "--disable-final-image") {
            options.write_final_image = false;
        } else if (ARG == "--enable-final-image") {
            options.write_final_image = true;
        } else if (ARG == "--final-image-max-pixels") {
            int parsed = 0;
            if (!parse_required_int(0, parsed)) {
                return ParseStatus::Error;
            }
            options.final_image_max_pixels = static_cast<uint64_t>(parsed);
        } else if (ARG == "--live") {
            options.live_preview = true;
            options.preview_update_interval_seconds = LIVE_PREVIEW_UPDATE_INTERVAL_SECONDS;
        } else if (ARG == "--tracebench-worker" || ARG == "--worker-command-stream") {
            continue;
        } else if (ARG == "--worker-id") {
            int ignored = 0;
            if (!parse_required_int(0, ignored)) {
                return ParseStatus::Error;
            }
        } else if (ARG == "--worker-first-slot") {
            int ignored = 0;
            if (!parse_required_int(0, ignored)) {
                return ParseStatus::Error;
            }
        } else if (ARG == "--worker-batch-start") {
            int ignored = 0;
            if (!parse_required_int(0, ignored)) {
                return ParseStatus::Error;
            }
        } else if (ARG == "--worker-batch-count") {
            int ignored = 0;
            if (!parse_required_int(1, ignored)) {
                return ParseStatus::Error;
            }
        } else if (ARG == "--worker-count" || ARG == "--worker-threads" || ARG == "--worker-slots" || ARG == "--worker-total-slots") {
            int ignored = 0;
            if (!parse_required_int(1, ignored)) {
                return ParseStatus::Error;
            }
        } else if (ARG == "--help" || ARG == "-h") {
            print_usage(argv[0]);
            return ParseStatus::Help;
        } else if (ARG.starts_with("--")) {
            std::fprintf(stderr, "renderbench: unknown option %.*s\n", static_cast<int>(ARG.size()), ARG.data());
            return ParseStatus::Error;
        }
    }
    if (options.run_id.empty()) {
        options.run_id = make_run_id();
    }
    return ParseStatus::Ok;
}

static auto spread_morton_bits(uint32_t value) -> uint64_t {
    uint64_t bits = value;
    bits = (bits | (bits << 16U)) & 0x0000FFFF0000FFFFULL;
    bits = (bits | (bits << 8U)) & 0x00FF00FF00FF00FFULL;
    bits = (bits | (bits << 4U)) & 0x0F0F0F0F0F0F0F0FULL;
    bits = (bits | (bits << 2U)) & 0x3333333333333333ULL;
    bits = (bits | (bits << 1U)) & 0x5555555555555555ULL;
    return bits;
}

static auto morton_tile_key(int tile_x, int tile_y) -> uint64_t {
    return spread_morton_bits(static_cast<uint32_t>(tile_x)) | (spread_morton_bits(static_cast<uint32_t>(tile_y)) << 1U);
}

auto make_tiles(int width, int height, int tile_size) -> std::vector<Tile> {
    struct OrderedTile {
        Tile tile;
        uint64_t order_key = 0;
        int grid_y = 0;
        int grid_x = 0;
    };

    std::vector<OrderedTile> ordered_tiles;
    int index = 0;
    int grid_y = 0;
    for (int y = 0; y < height; y += tile_size, ++grid_y) {
        int grid_x = 0;
        for (int x = 0; x < width; x += tile_size, ++grid_x) {
            ordered_tiles.push_back({
                .tile = {.x0 = x, .y0 = y, .x1 = std::min(x + tile_size, width), .y1 = std::min(y + tile_size, height), .index = index++},
                .order_key = morton_tile_key(grid_x, grid_y),
                .grid_y = grid_y,
                .grid_x = grid_x,
            });
        }
    }

    std::ranges::sort(ordered_tiles, [](const OrderedTile& left, const OrderedTile& right) {
        if (left.order_key != right.order_key) {
            return left.order_key < right.order_key;
        }
        if (left.grid_y != right.grid_y) {
            return left.grid_y < right.grid_y;
        }
        return left.grid_x < right.grid_x;
    });

    std::vector<Tile> tiles;
    tiles.reserve(ordered_tiles.size());
    for (const auto& tile : ordered_tiles) {
        tiles.push_back(tile.tile);
    }
    return tiles;
}

auto make_film_storage(int width, int height) -> std::vector<float> {
    return std::vector<float>(static_cast<size_t>(width) * static_cast<size_t>(height) * 3U, 0.0F);
}

auto load_scene(const std::string& path) -> std::shared_ptr<Scene> {
    if (path.empty()) {
        return std::make_shared<Scene>(default_scene());
    }

    constexpr int MAX_ATTEMPTS = 12;
    constexpr useconds_t RETRY_DELAY_US = 100000;
    size_t last_size = 0;
    bool saw_bytes = false;

    for (int attempt = 0; attempt < MAX_ATTEMPTS; ++attempt) {
        auto bytes = read_file_bytes(path);
        last_size = bytes.size();
        saw_bytes = saw_bytes || !bytes.empty();
        if (!bytes.empty()) {
            auto glb_scene = load_glb_scene(bytes);
            if (glb_scene.has_value()) {
                return std::make_shared<Scene>(std::move(*glb_scene));
            }
        }
        if (attempt + 1 < MAX_ATTEMPTS) {
            ::usleep(RETRY_DELAY_US);
        }
    }

    if (!saw_bytes) {
        std::fprintf(stderr, "renderbench: unable to read scene '%s'\n", path.c_str());
    } else {
        std::fprintf(stderr, "renderbench: invalid or unsupported GLB scene '%s' after retries (last_read=%zu bytes)\n", path.c_str(),
                     last_size);
    }
    return nullptr;
}

namespace {

auto tile_payload_float_count(const Tile& tile) -> size_t {
    int const WIDTH = std::max(0, tile.x1 - tile.x0);
    int const HEIGHT = std::max(0, tile.y1 - tile.y0);
    return static_cast<size_t>(WIDTH) * static_cast<size_t>(HEIGHT) * 3U;
}

template <typename StorePixel>
void render_tile_pixels(const Scene& scene, const Options& options, const Tile& tile, uint64_t seed, StorePixel store_pixel) {
    float const ASPECT = static_cast<float>(options.width) / static_cast<float>(options.height);
    float const SCALE = std::tan((scene.camera.vfov_degrees * PI / 180.0F) * 0.5F);
    for (int y = tile.y0; y < tile.y1; ++y) {
        for (int x = tile.x0; x < tile.x1; ++x) {
            Rng rng{seed ^ (static_cast<uint64_t>(x) * 0x9e3779b185ebca87ULL) ^ (static_cast<uint64_t>(y) * 0xc2b2ae3d27d4eb4fULL)};
            Vec3 color{};
            for (int sample = 0; sample < options.spp; ++sample) {
                float const U =
                    (((static_cast<float>(x) + rng.uniform()) / static_cast<float>(options.width)) - 0.5F) * 2.0F * ASPECT * SCALE;
                float const V = (0.5F - ((static_cast<float>(y) + rng.uniform()) / static_cast<float>(options.height))) * 2.0F * SCALE;
                Ray ray{.origin = scene.camera.origin,
                        .direction = normalize(scene.camera.forward + (scene.camera.right * U) + (scene.camera.up * V))};
                color += trace_ray(scene, ray, options, rng);
            }
            color = color / static_cast<float>(options.spp);
            store_pixel(x, y, color);
        }
    }
}

void sleep_debug_constant_tile(int delay_us) {
    if (delay_us <= 0) {
        return;
    }

    timespec remaining{
        .tv_sec = delay_us / 1'000'000,
        .tv_nsec = static_cast<long>(delay_us % 1'000'000) * 1000L,
    };
    while (::nanosleep(&remaining, &remaining) != 0 && errno == EINTR) {
    }
}

auto debug_constant_tile_color(const Tile& tile) -> Vec3 {
    uint32_t hash = static_cast<uint32_t>(tile.index) * 0x9E3779B1U;
    hash ^= hash >> 16U;
    return {
        .x = 0.15F + (static_cast<float>((hash >> 0U) & 0x7FU) / 255.0F),
        .y = 0.18F + (static_cast<float>((hash >> 8U) & 0x7FU) / 255.0F),
        .z = 0.20F + (static_cast<float>((hash >> 16U) & 0x7FU) / 255.0F),
    };
}

template <typename StorePixel>
void render_debug_constant_tile_pixels(const Options& options, const Tile& tile, StorePixel store_pixel) {
    sleep_debug_constant_tile(options.debug_constant_tile_us);
    Vec3 const COLOR = debug_constant_tile_color(tile);
    for (int y = tile.y0; y < tile.y1; ++y) {
        for (int x = tile.x0; x < tile.x1; ++x) {
            store_pixel(x, y, COLOR);
        }
    }
}

}  // namespace

void render_tile(const Scene& scene, FilmView film, const Options& options, const Tile& tile, uint64_t seed) {
    auto store_pixel = [&](int x, int y, Vec3 color) { film.set_pixel(x, y, color.x, color.y, color.z); };
    if (options.debug_constant_tile_us > 0) {
        render_debug_constant_tile_pixels(options, tile, store_pixel);
    } else {
        render_tile_pixels(scene, options, tile, seed, store_pixel);
    }
}

auto render_tile_payload(const Scene& scene, std::span<float> payload, const Options& options, const Tile& tile, uint64_t seed) -> bool {
    size_t const REQUIRED_FLOATS = tile_payload_float_count(tile);
    if (payload.size() < REQUIRED_FLOATS) {
        return false;
    }

    auto* out = payload.data();
    auto const* const OUT_END = out + REQUIRED_FLOATS;
    auto store_pixel = [&](int, int, Vec3 color) {
        *out++ = color.x;
        *out++ = color.y;
        *out++ = color.z;
    };
    if (options.debug_constant_tile_us > 0) {
        render_debug_constant_tile_pixels(options, tile, store_pixel);
    } else {
        render_tile_pixels(scene, options, tile, seed, store_pixel);
    }
    return out == OUT_END;
}

auto monotonic_seconds() -> double {
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0.0;
    }
    return static_cast<double>(ts.tv_sec) + (static_cast<double>(ts.tv_nsec) / 1000000000.0);
}

auto make_run_id() -> std::string {
    time_t const NOW = std::time(nullptr);
    return "run-" + std::to_string(static_cast<long long>(NOW));
}

auto run_dir(const Options& options) -> std::string { return options.output_root + "/" + options.run_id; }

auto ensure_output_tree(const Options& options) -> bool { return ensure_dir(options.output_root) && ensure_dir(run_dir(options)); }

auto write_status(const Options& options, const Progress& progress) -> bool {
    std::ostringstream out;
    out << "{\n"
        << R"(  "run_id": ")" << options.run_id << "\",\n"
        << R"(  "backend": ")" << backend_name(options.backend) << "\",\n"
        << R"(  "placement": ")" << placement_name(options.placement) << "\",\n"
        << "  \"width\": " << options.width << ",\n"
        << "  \"height\": " << options.height << ",\n"
        << "  \"spp\": " << options.spp << ",\n"
        << "  \"max_depth\": " << options.max_depth << ",\n"
        << "  \"tile_size\": " << options.tile_size << ",\n"
        << "  \"debug_edge_colors\": " << (options.debug_edge_colors ? "true" : "false") << ",\n"
        << R"(  "debug_render_mode": ")" << debug_render_mode_name(options) << "\",\n"
        << "  \"debug_constant_tile_us\": " << options.debug_constant_tile_us << ",\n"
        << "  \"debug_node_thread_batch_size\": " << options.debug_node_thread_batch_size << ",\n"
        << "  \"coordinator_reserve_cpus\": " << options.coordinator_reserve_cpus << ",\n"
        << "  \"node_worker_reserve_cpus\": " << options.node_worker_reserve_cpus << ",\n"
        << "  \"coordinator_skip_local_worker\": " << (options.coordinator_skip_local_worker ? "true" : "false") << ",\n"
        << "  \"worker_output_queue_disabled\": " << (options.disable_worker_output_queue ? "true" : "false") << ",\n"
        << "  \"single_thread_worker_queue_disabled\": " << (options.disable_single_thread_worker_queue ? "true" : "false") << ",\n"
        << "  \"final_image_enabled\": " << (options.write_final_image ? "true" : "false") << ",\n"
        << "  \"final_image_max_pixels\": " << options.final_image_max_pixels << ",\n"
        << "  \"live_preview\": " << (options.live_preview ? "true" : "false") << ",\n"
        << "  \"preview_update_interval_seconds\": " << options.preview_update_interval_seconds << ",\n"
        << "  \"tiles_done\": " << progress.tiles_done << ",\n"
        << "  \"total_tiles\": " << progress.total_tiles << ",\n"
        << "  \"samples_done\": " << progress.samples_done << ",\n"
        << "  \"total_samples\": " << progress.total_samples << ",\n"
        << "  \"elapsed_seconds\": " << progress.elapsed_seconds << ",\n"
        << "  \"done\": " << (progress.done ? "true" : "false") << "\n"
        << "}\n";
    bool ok = atomic_write_text(run_dir(options) + "/status.json", out.str());
    std::ostringstream latest;
    latest << "{ \"run_id\": \"" << options.run_id << "\" }\n";
    ok = atomic_write_text(options.output_root + "/latest.json", latest.str()) && ok;
    return ok;
}

auto write_metrics(const Options& options, const Progress& progress, double rays_per_second) -> bool {
    std::ostringstream out;
    out << "{\n"
        << "  \"run_id\": \"" << options.run_id << "\",\n"
        << "  \"backend\": \"" << backend_name(options.backend) << "\",\n"
        << "  \"placement\": \"" << placement_name(options.placement) << "\",\n"
        << "  \"debug_edge_colors\": " << (options.debug_edge_colors ? "true" : "false") << ",\n"
        << "  \"debug_render_mode\": \"" << debug_render_mode_name(options) << "\",\n"
        << "  \"debug_constant_tile_us\": " << options.debug_constant_tile_us << ",\n"
        << "  \"debug_node_thread_batch_size\": " << options.debug_node_thread_batch_size << ",\n"
        << "  \"coordinator_reserve_cpus\": " << options.coordinator_reserve_cpus << ",\n"
        << "  \"node_worker_reserve_cpus\": " << options.node_worker_reserve_cpus << ",\n"
        << "  \"coordinator_skip_local_worker\": " << (options.coordinator_skip_local_worker ? "true" : "false") << ",\n"
        << "  \"worker_output_queue_disabled\": " << (options.disable_worker_output_queue ? "true" : "false") << ",\n"
        << "  \"single_thread_worker_queue_disabled\": " << (options.disable_single_thread_worker_queue ? "true" : "false") << ",\n"
        << "  \"final_image_enabled\": " << (options.write_final_image ? "true" : "false") << ",\n"
        << "  \"final_image_max_pixels\": " << options.final_image_max_pixels << ",\n"
        << "  \"live_preview\": " << (options.live_preview ? "true" : "false") << ",\n"
        << "  \"preview_update_interval_seconds\": " << options.preview_update_interval_seconds << ",\n"
        << "  \"elapsed_seconds\": " << progress.elapsed_seconds << ",\n"
        << "  \"primary_samples\": " << progress.total_samples << ",\n"
        << "  \"rays_per_second_estimate\": " << rays_per_second << "\n"
        << "}\n";
    return atomic_write_text(run_dir(options) + "/metrics.json", out.str());
}

auto write_preview_png(const Options& options, FilmView film) -> bool {
    return write_preview_png_atomic(run_dir(options) + "/preview.png", film);
}

auto write_final_png(const Options& options, FilmView film) -> bool {
    if (!should_write_final_image(options, film)) {
        uint64_t pixels = 0;
        if (final_image_pixel_count(film, pixels)) {
            std::fprintf(stderr, "renderbench: skipping final PNG (%llu pixels, max=%llu); preview.png remains available\n",
                         static_cast<unsigned long long>(pixels), static_cast<unsigned long long>(options.final_image_max_pixels));
        }
        return false;
    }
    return write_png_atomic(run_dir(options) + "/frame_000.png", film);
}

}  // namespace tracebench
