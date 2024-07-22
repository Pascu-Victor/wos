#include "fb.hpp"


__attribute__((used, section(".requests")))
static volatile limine_framebuffer_request framebufferRequest = {
    .id = LIMINE_FRAMEBUFFER_REQUEST,
    .revision = 0,
    .response = nullptr,
};

namespace gfx {
    namespace fb {
        FbFont __currentFont;
        limine_framebuffer *__framebuffer;

        
        void init(void) {
            if (framebufferRequest.response == nullptr
            || framebufferRequest.response->framebuffer_count < 1) {
                hcf();
            }

            __framebuffer = framebufferRequest.response->framebuffers[0];

            // manual constructor calling since we can't use new for now
            std::strcpy(__currentFont.name, "default");
            __currentFont.height = 16;
            __currentFont.width = 16;
            __currentFont.loadFont();

            clear(TERM_BG_COLOR);
        }

        inline void writePixel(uint16_t x, uint16_t y, uint32_t color) {
            uint32_t *fb = (uint32_t*)__framebuffer->address;
            fb[x + y * __framebuffer->width] = color;
        }

        void clear(uint32_t color) {
            uint32_t *fb = (uint32_t*)__framebuffer->address;
            for (size_t i = 0; i < __framebuffer->width * __framebuffer->height; i++) {
                fb[i] = color;
            }
        }

        void drawRect(
            uint16_t x,
            uint16_t y,
            uint16_t w,
            uint16_t h,
            uint32_t color,
            FillMode fill
        ) {
            if(fill == FillMode::FILL) {
                for (size_t i = 0; i < h; i++) {
                    for (size_t j = 0; j < w; j++) {
                        writePixel(x + j, y + i, color);
                    }
                }
            } else {
                for (size_t i = 0; i < w; i++) {
                    writePixel(x + i, y, color);
                    writePixel(x + i, y + h, color);
                }
                for (size_t i = 0; i < h; i++) {
                    writePixel(x, y + i, color);
                    writePixel(x + w, y + i, color);
                }
            }
        }

        void drawChar(
            uint16_t x,
            uint16_t y,
            char c,
            uint32_t color,
            uint32_t bg_color,
            OffsetMode mode
        ) {
            if(mode == OffsetMode::OFFSET_CHAR) {
                x *= __currentFont.width;
                y *= __currentFont.height;
            }
            const uint64_t *data = __currentFont.getData(c);
            for (size_t i = 0; i < __currentFont.height; i++) {
                for (size_t j = 0; j < __currentFont.width; j++) {
                    if (data[i] & (1ULL << j)) {
                        writePixel(x + __currentFont.width - j, y + i, color);
                    }
                    else {
                        writePixel(x + __currentFont.width - j, y + i, bg_color);
                    }
                }
            }
        }

        void drawString(
            uint16_t x,
            uint16_t y,
            const char *str,
            uint32_t color,
            uint32_t bg_color,
            OffsetMode mode
        ) {
            for (size_t i = 0; str[i] != '\0'; i++) {
                if(str[i] == '\n') {
                    if (mode == OffsetMode::OFFSET_CHAR)
                    {
                        y++;
                    } else {
                        y += __currentFont.height;
                    }
                    
                    x = 0;
                    continue;
                }
                if (mode == OffsetMode::OFFSET_CHAR) {
                    drawChar((x + i) * __currentFont.width, y * __currentFont.height, str[i], color, bg_color, OffsetMode::OFFSET_PIXEL);
                } else {
                    drawChar(x + i * __currentFont.width, y, str[i], color, bg_color, OffsetMode::OFFSET_PIXEL);
                }
            }
        }

        void drawLine(
            uint16_t x1,
            uint16_t y1,
            uint16_t x2,
            uint16_t y2,
            uint32_t color
        ) {
            int dx = x2 - x1;
            int dy = y2 - y1;
            int dx1 = 0, dy1 = 0, dx2 = 0, dy2 = 0;
            if (dx < 0) {
                dx1 = -1;
            } else if (dx > 0) {
                dx1 = 1;
            }
            if (dy < 0) {
                dy1 = -1;
            } else if (dy > 0) {
                dy1 = 1;
            }
            if (dx < 0) {
                dx2 = -1;
            } else if (dx > 0) {
                dx2 = 1;
            }
            int longest = abs(dx);
            int shortest = abs(dy);
            if (!(longest > shortest)) {
                longest = abs(dy);
                shortest = abs(dx);
                if (dy < 0) {
                    dy2 = -1;
                } else if (dy > 0) {
                    dy2 = 1;
                }
                dx2 = 0;
            }
            int numerator = longest >> 1;
            for (int i = 0; i <= longest; i++) {
                writePixel(x1, y1, color);
                numerator += shortest;
                if (!(numerator < longest)) {
                    numerator -= longest;
                    x1 += dx1;
                    y1 += dy1;
                } else {
                    x1 += dx2;
                    y1 += dy2;
                }
            }
        }

        void drawCircle(
            uint16_t x,
            uint16_t y,
            uint16_t radius,
            uint32_t color,
            FillMode fill
        ) {
            int f = 1 - radius;
            int ddF_x = 1;
            int ddF_y = -2 * radius;
            int x1 = 0;
            int y1 = radius;

            if(fill == FillMode::FILL) {
                drawLine(x, y - radius, x, y + radius, color);
            } else {
                writePixel(x, y + radius, color);
                writePixel(x, y - radius, color);
                writePixel(x + radius, y, color);
                writePixel(x - radius, y, color);
            }

            while (x1 < y1) {
                if (f >= 0) {
                    y1--;
                    ddF_y += 2;
                    f += ddF_y;
                }
                x1++;
                ddF_x += 2;
                f += ddF_x;
                if(fill == FillMode::FILL) {
                    drawLine(x - x1, y + y1, x + x1, y + y1, color);
                    drawLine(x - x1, y - y1, x + x1, y - y1, color);
                    drawLine(x - y1, y + x1, x + y1, y + x1, color);
                    drawLine(x - y1, y - x1, x + y1, y - x1, color);
                } else {
                    writePixel(x + x1, y + y1, color);
                    writePixel(x - x1, y + y1, color);
                    writePixel(x + x1, y - y1, color);
                    writePixel(x - x1, y - y1, color);
                    writePixel(x + y1, y + x1, color);
                    writePixel(x - y1, y + x1, color);
                    writePixel(x + y1, y - x1, color);
                    writePixel(x - y1, y - x1, color);
                }
            }

        }

        // int set_font(const FbFont* font) {
        //     if(font->getWidth() > 64) {
        //         return -1;
        //     }
        //     __currentFont = *font;
        //     return 0;
        // }
    }
}