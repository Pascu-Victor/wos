#pragma once
#include <limine.h>

#include <util/funcs.hpp>
#include <util/math.hpp>
#include <util/mem.hpp>
#include <mod/gfx/fb_font.hpp>

namespace gfx {
    namespace fb {
        enum class OffsetMode {
            OFFSET_PIXEL,
            OFFSET_CHAR
        };

        namespace TermColors {
            static const uint32_t BLACK = 0x00000000;
            static const uint32_t RED = 0x00AA0000;
            static const uint32_t ORANGE = 0x00FFAA00;
            static const uint32_t YELLOW = 0x00AAAA00;
            static const uint32_t GREEN = 0x0000AA00;
            static const uint32_t BLUE = 0x000000AA;
            static const uint32_t MAGENTA = 0x00AA00AA;
            static const uint32_t CYAN = 0x0000AAAA;
            static const uint32_t WHITE = 0x00AAAAAA;
            static const uint32_t BRIGHT_BLACK = 0x00555555;
            static const uint32_t BRIGHT_RED = 0x00FF5555;
            static const uint32_t BRIGHT_ORANGE = 0x00FFAF19;
            static const uint32_t BRIGHT_GREEN = 0x0055FF55;
            static const uint32_t BRIGHT_YELLOW = 0x00FFFF55;
            static const uint32_t BRIGHT_BLUE = 0x005555FF;
            static const uint32_t BRIGHT_MAGENTA = 0x00FF55FF;
            static const uint32_t BRIGHT_CYAN = 0x0055FFFF;
            static const uint32_t BRIGHT_WHITE = 0x00FFFFFF;
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

        //TODO: int  setFont(const FbFont* font);
    }
}
