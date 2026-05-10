#include "util.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <format>
#include <iterator>
#include <memory>
#include <print>
#include <span>
#include <string>
#include <vector>

#include "config.hpp"
#include "lodepng.hpp"

namespace {

void free_lodepng_buffer(unsigned char* ptr) {
    // LodePNG allocates this C buffer and documents free() as the release API.
    std::free(ptr);  // NOLINT(cppcoreguidelines-no-malloc)
}

}  // namespace

unsigned char color2byte(float v) {
    float const C = std::clamp(v * 255.0F, 0.0F, 255.0F);
    return static_cast<unsigned char>(C);
}

void hsv2rgb(float h, float s, float v, unsigned char* rgb) {
    if (rgb == nullptr) {
        return;
    }

    int i = 0;
    float f = NAN;
    float p = NAN;
    float q = NAN;
    float t = NAN;
    float r = NAN;
    float g = NAN;
    float b = NAN;

    if (s == 0) {
        rgb[0] = color2byte(v);
        rgb[1] = color2byte(v);
        rgb[2] = color2byte(v);
        return;
    }

    h /= 60;
    i = static_cast<int>(std::floor(h));
    f = h - static_cast<float>(i);
    p = v * (1 - s);
    q = v * (1 - (s * f));
    t = v * (1 - (s * (1 - f)));

    switch (i) {
        case 0:
            r = v;
            g = t;
            b = p;
            break;
        case 1:
            r = q;
            g = v;
            b = p;
            break;
        case 2:
            r = p;
            g = v;
            b = t;
            break;
        case 3:
            r = p;
            g = q;
            b = v;
            break;
        case 4:
            r = t;
            g = p;
            b = v;
            break;
        default:
            r = v;
            g = p;
            b = q;
            break;
    }

    rgb[0] = color2byte(r);
    rgb[1] = color2byte(g);
    rgb[2] = color2byte(b);
}

void init_colormap(int len, unsigned char* map) {
    if (len <= 0 || map == nullptr) {
        return;
    }

    for (int i = 0; i < len - 1; i++) {
        hsv2rgb(static_cast<float>(i) / 4.0F, 1.0F, static_cast<float>(i) / (static_cast<float>(i) + 8.0F),
                &map[static_cast<size_t>(i) * 3]);
    }

    map[(3 * (len - 1)) + 0] = 0;
    map[(3 * (len - 1)) + 1] = 0;
    map[(3 * (len - 1)) + 2] = 0;
}

void set_pixel(unsigned char* image, int width, int x, int y, const unsigned char* c) {
    image[(4 * width * y) + (4 * x) + 0] = c[0];
    image[(4 * width * y) + (4 * x) + 1] = c[1];
    image[(4 * width * y) + (4 * x) + 2] = c[2];
    image[(4 * width * y) + (4 * x) + 3] = 255;
}

void save_image(const char* filename, const unsigned char* image, unsigned width, unsigned height) {
    unsigned error = 0;
    unsigned char* png_raw = nullptr;
    size_t pngsize = 0;
    LodePNGState state;

    lodepng_state_init(&state);

    error = lodepng_encode(&png_raw, &pngsize, image, width, height, &state);
    const std::unique_ptr<unsigned char, decltype(&free_lodepng_buffer)> PNG{png_raw, free_lodepng_buffer};
    if (error == 0U) {
        unsigned save_error = lodepng_save_file(PNG.get(), pngsize, filename);
        if (save_error != 0) {
            std::println(stderr, "ERROR: {}: {}", save_error, lodepng_error_text(save_error));
        }
    }
    if (error != 0U) {
        std::println(stderr, "ERROR: {}: {}", error, lodepng_error_text(error));
    }
    lodepng_state_cleanup(&state);
}

void description(const char* name, int width, int height, int max_iteration, int threads, char* desc) {
    constexpr size_t DESCRIPTION_CAPACITY = 100;
    if (desc == nullptr) {
        return;
    }
    if (strcmp("cpu", name) == 0) {
        std::format_to_n(desc, DESCRIPTION_CAPACITY, "width={} height={} iterations={} threads={}", width, height, max_iteration, threads);
    } else {
        std::format_to_n(desc, DESCRIPTION_CAPACITY, "{}", "-");
    }
}

void progress(const char* name, int width, int height, int max_iteration, int threads, int repeat, int r, double time) {
    if (!MANDELBENCH_DEBUG_ENABLED) {
        return;
    }

    std::array<char, 100> desc{};
    description(name, width, height, max_iteration, threads, desc.data());
    std::println(stderr, "name={} {} repeat={}/{} duration={:.2f}", name, desc.data(), r + 1, repeat, time);
}

void report(const char* name, int width, int height, int max_iteration, int threads, int repeat, const std::vector<double>& times) {
    if (repeat <= 0 || times.empty()) {
        std::println(stderr, "name={} repeat={} no samples", name, repeat);
        return;
    }

    double avg = NAN;
    double stdev = NAN;
    double min = NAN;
    double max = NAN;
    double median = NAN;
    double total = NAN;
    std::array<char, 100> desc{};

    description(name, width, height, max_iteration, threads, desc.data());
    size_t const SAMPLE_COUNT = std::min(static_cast<size_t>(repeat), times.size());
    std::span<const double> const SAMPLES(times.data(), SAMPLE_COUNT);

    total = 0;
    min = SAMPLES.front();
    max = SAMPLES.front();
    for (double const TIME : SAMPLES) {
        total += TIME;
        min = std::min(min, TIME);
        max = std::max(max, TIME);
    }
    avg = total / static_cast<double>(SAMPLE_COUNT);

    stdev = 0;
    for (double const TIME : SAMPLES) {
        stdev += (TIME - avg) * (TIME - avg);
    }
    stdev = std::sqrt(stdev / static_cast<double>(SAMPLE_COUNT));

    std::vector<double> sorted_times(SAMPLES.begin(), SAMPLES.end());
    std::ranges::sort(sorted_times);
    auto const MEDIAN_POS = static_cast<ptrdiff_t>(SAMPLE_COUNT / 2);
    auto const MEDIAN_IT = std::next(sorted_times.begin(), MEDIAN_POS);
    if ((SAMPLE_COUNT & 1U) != 0U) {
        median = *MEDIAN_IT;
    } else {
        median = (*std::prev(MEDIAN_IT) + *MEDIAN_IT) / 2.0;
    }

    std::string rep = std::format("name={} {} repeat={} total_compute={:.2f} min={:.2f} max={:.2f} median={:.2f} avg={:.2f} stdev={:.2f}",
                                  name, desc.data(), SAMPLE_COUNT, total, min, max, median, avg, stdev);

    const std::unique_ptr<FILE, decltype(&std::fclose)> REPORT_FILE{std::fopen(REPORT, "a"), std::fclose};
    if (REPORT_FILE == nullptr) {
        std::println(stderr, "ERROR: could not open report file '{}'", REPORT);
        return;
    }
    std::fputs(rep.c_str(), REPORT_FILE.get());
    std::println(stderr, "{}", rep);
}
