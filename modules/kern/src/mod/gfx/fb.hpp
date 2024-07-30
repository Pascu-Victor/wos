#pragma once
#include <limine.h>

#include <std/hcf.hpp>
#include <util/math.hpp>
#include <util/mem.hpp>
#include <mod/gfx/fb_font.hpp>

namespace ker::mod::gfx {
    namespace fb {
        enum class OffsetMode {
            OFFSET_PIXEL,
            OFFSET_CHAR
        };

        enum TermColors: uint32_t {
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

        enum class FillMode {
            FILL,
            NO_FILL
        };

        const static uint32_t TERM_BG_COLOR = static_cast<uint32_t>(TermColors::BLACK);
        const static uint32_t TERM_FG_COLOR = static_cast<uint32_t>(TermColors::WHITE);

        void init(void);

        inline void writePixel(uint16_t x, uint16_t y, uint32_t color);

        void clear(uint32_t color = static_cast<uint32_t>(TermColors::BLACK));

        void drawRect(
            uint16_t x,
            uint16_t y,
            uint16_t w,
            uint16_t h,
            uint32_t color = static_cast<uint32_t>(TermColors::WHITE),
            FillMode fill = FillMode::NO_FILL
        );

        void drawChar(
            uint16_t x,
            uint16_t y,
            char c,
            uint32_t color = static_cast<uint32_t>(TermColors::WHITE),
            uint32_t bg_color = static_cast<uint32_t>(TermColors::BLACK),
            OffsetMode mode = OffsetMode::OFFSET_CHAR
        );

        void drawString(
            uint16_t x,
            uint16_t y,
            const char *str,
            uint32_t color = static_cast<uint32_t>(TermColors::WHITE),
            uint32_t bg_color = static_cast<uint32_t>(TermColors::BLACK),
            OffsetMode mode = OffsetMode::OFFSET_CHAR
        );

        void drawLine(
            uint16_t x1,
            uint16_t y1,
            uint16_t x2,
            uint16_t y2,
            uint32_t color = static_cast<uint32_t>(TermColors::WHITE)
        );

        void drawCircle(
            uint16_t x,
            uint16_t y,
            uint16_t radius,
            uint32_t color = static_cast<uint32_t>(TermColors::WHITE),
            FillMode fill = FillMode::NO_FILL
        );

        //viewport width in current font characters
        int viewportWidthChars(void);

        //viewport height in current font characters
        int viewportHeightChars(void);

        //viewport width in pixels
        int viewportWidth(void);

        //viewport height in pixels
        int viewportHeight(void);

        //scrolls the viewport up by one line
        void scroll(void);

        //TODO: int  setFont(const FbFont* font);
    }
}
