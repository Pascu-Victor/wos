#pragma once
#include <cstdint>
#include <cstring>
#include <defines/defines.hpp>
namespace ker::mod::gfx::fb {
class FbFont {
   public:
    void load_font();
    char name[256]{};
    uint8_t height;  // max 64
    uint8_t width;   // max 64
    uint64_t data[256][64]{};

    FbFont(const char* name, uint8_t height, uint8_t width, const uint64_t data[256][64]);
    FbFont();
    FbFont(const FbFont& other);
    auto operator=(const FbFont& other) -> FbFont&;

    [[nodiscard]] auto get_height() const -> uint8_t;

    [[nodiscard]] auto get_width() const -> uint8_t;

    [[nodiscard]] auto get_data(char c) const -> const uint64_t*;
};
}  // namespace ker::mod::gfx::fb
