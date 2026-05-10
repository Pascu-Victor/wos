#pragma once
#include <extern/limine.h>

#include <cmath>
#include <cstdint>
#include <cstring>
#include <mod/gfx/fb_font.hpp>
#include <util/hcf.hpp>

namespace ker::mod::gfx::fb {
constexpr bool WOS_HAS_GFX_FB = false;

enum class OffsetMode : uint8_t { OFFSET_PIXEL, OFFSET_CHAR };

enum class TermColors : uint32_t {
    BLACK = 0x00000000,
    RED = 0x00AA0000,
    ORANGE = 0x00FFAA00,
    YELLOW = 0x00AAAA00,
    GREEN = 0x0000AA00,
    BLUE = 0x000000AA,
    MAGENTA = 0x00AA00AA,
    CYAN = 0x0000AAAA,
    WHITE = 0x00AAAAAA,
    BRIGHT_BLACK = 0x00555555,
    BRIGHT_RED = 0x00FF5555,
    BRIGHT_ORANGE = 0x00FFAF19,
    BRIGHT_GREEN = 0x0055FF55,
    BRIGHT_YELLOW = 0x00FFFF55,
    BRIGHT_BLUE = 0x005555FF,
    BRIGHT_MAGENTA = 0x00FF55FF,
    BRIGHT_CYAN = 0x0055FFFF,
    BRIGHT_WHITE = 0x00FFFFFF
};

enum class FillMode : uint8_t { FILL, NO_FILL };

const static uint32_t TERM_BG_COLOR = static_cast<uint32_t>(TermColors::BLACK);
const static uint32_t TERM_FG_COLOR = static_cast<uint32_t>(TermColors::WHITE);

void init();

inline void write_pixel(uint16_t x, uint16_t y, uint32_t color);

void clear(uint32_t color = static_cast<uint32_t>(TermColors::BLACK));

void draw_rect(uint16_t x, uint16_t y, uint16_t w, uint16_t h, uint32_t color = static_cast<uint32_t>(TermColors::WHITE),
               FillMode fill = FillMode::NO_FILL);

void draw_char(uint16_t x, uint16_t y, char c, uint32_t color = static_cast<uint32_t>(TermColors::WHITE),
               uint32_t bg_color = static_cast<uint32_t>(TermColors::BLACK), OffsetMode mode = OffsetMode::OFFSET_CHAR);

// returns number of extra lines drawn (due to newlines)
auto draw_string(uint16_t x, uint16_t y, const char* str, uint32_t color = static_cast<uint32_t>(TermColors::WHITE),
                 uint32_t bg_color = static_cast<uint32_t>(TermColors::BLACK), OffsetMode mode = OffsetMode::OFFSET_CHAR) -> uint64_t;

void draw_line(uint16_t x1, uint16_t y1, uint16_t x2, uint16_t y2, uint32_t color = static_cast<uint32_t>(TermColors::WHITE));

void draw_circle(uint16_t x, uint16_t y, uint16_t radius, uint32_t color = static_cast<uint32_t>(TermColors::WHITE),
                 FillMode fill = FillMode::NO_FILL);

// viewport width in current font characters
auto viewport_width_chars() -> uint64_t;

// viewport height in current font characters
auto viewport_height_chars() -> uint64_t;

// viewport width in pixels
auto viewport_width() -> uint64_t;

// viewport height in pixels
auto viewport_height() -> uint64_t;

// scrolls the viewport up by one line
void scroll();

void map_framebuffer();

// TODO: int  setFont(const FbFont* font);
}  // namespace ker::mod::gfx::fb
