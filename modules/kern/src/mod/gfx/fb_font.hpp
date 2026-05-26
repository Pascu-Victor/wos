#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <defines/defines.hpp>
namespace ker::mod::gfx::fb {
class FbFont {
   public:
    static constexpr std::size_t NAME_SIZE = 256;
    static constexpr std::size_t GLYPH_COUNT = 256;
    static constexpr std::size_t GLYPH_HEIGHT_MAX = 64;

    using Name = std::array<char, NAME_SIZE>;
    using GlyphData = std::array<uint64_t, GLYPH_HEIGHT_MAX>;
    using FontData = std::array<GlyphData, GLYPH_COUNT>;

    void load_font();
    Name name{};
    uint8_t height;  // max 64
    uint8_t width;   // max 64
    FontData data{};

    FbFont(const char* name, uint8_t height, uint8_t width, const FontData* data);
    FbFont();
    FbFont(const FbFont& other);
    auto operator=(const FbFont& other) -> FbFont&;

    [[nodiscard]] auto get_height() const -> uint8_t;

    [[nodiscard]] auto get_width() const -> uint8_t;

    [[nodiscard]] auto get_data(char c) const -> const uint64_t*;
};
}  // namespace ker::mod::gfx::fb
