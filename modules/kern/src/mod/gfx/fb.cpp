#include "fb.hpp"

#include <algorithm>
#include <cmath>
#include <platform/mm/virt.hpp>
__attribute__((used, section(".requests"))) static volatile limine_framebuffer_request framebufferRequest = {
    .id = LIMINE_FRAMEBUFFER_REQUEST,
    .revision = 0,
    .response = nullptr,
};

namespace ker::mod::gfx::fb {
namespace {
FbFont __currentFont;
limine_framebuffer* __framebuffer;
[[maybe_unused]]
uint32_t backBuffer[3840 * 2160];
}  // namespace

void init(void) {
    if constexpr (WOS_HAS_GFX_FB) {
        if (framebufferRequest.response == nullptr || framebufferRequest.response->framebuffer_count < 1) {
            hcf();
        }

        __framebuffer = framebufferRequest.response->framebuffers[0];

        __framebuffer->width = std::min<uint64_t>(__framebuffer->width, (uint64_t)3840);
        __framebuffer->height = std::min<uint64_t>(__framebuffer->height, (uint64_t)2160);

        // manual constructor calling since we can't use new for now
        std::strcpy(__currentFont.name, "default");
        __currentFont.height = 16;
        __currentFont.width = 16;
        __currentFont.loadFont();

        clear(TERM_BG_COLOR);
    } else {
        mod::io::serial::write(
            "Tried to init disabled subsystem: GFX::FB\n"
            "recompile with this system enabled or do not call this init functon\n"
            "HALTING");
        hcf();
    }
}

inline void swapBuffers(void) {
    if constexpr (WOS_HAS_GFX_FB) {
        uint32_t* fb = (uint32_t*)__framebuffer->address;
        for (size_t i = 0; i < __framebuffer->width * __framebuffer->height; i++) {
            fb[i] = backBuffer[i];
        }
    } else {
        hcf();
    }
}

inline void writePixel(uint16_t x, uint16_t y, uint32_t color) {
    if constexpr (WOS_HAS_GFX_FB) {
        backBuffer[x + y * __framebuffer->width] = color;
    } else {
        hcf();
    }
}

void clear(uint32_t color) {
    if constexpr (WOS_HAS_GFX_FB) {
        for (size_t i = 0; i < __framebuffer->width * __framebuffer->height; i++) {
            backBuffer[i] = color;
        }
        swapBuffers();
    } else {
        hcf();
    }
}

void drawRect(uint16_t x, uint16_t y, uint16_t w, uint16_t h, uint32_t color, FillMode fill) {
    if constexpr (WOS_HAS_GFX_FB) {
        if (fill == FillMode::FILL) {
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
        swapBuffers();
    } else {
        hcf();
    }
}

inline void drawCharNoSwap(uint16_t x, uint16_t y, char c, uint32_t color, uint32_t bg_color, OffsetMode mode) {
    if constexpr (WOS_HAS_GFX_FB) {
        if (mode == OffsetMode::OFFSET_CHAR) {
            x *= __currentFont.width;
            y *= __currentFont.height;
        }
        const uint64_t* data = __currentFont.getData(c);
        for (size_t i = 0; i < __currentFont.height; i++) {
            for (size_t j = 0; j < __currentFont.width; j++) {
                if (data[i] & (1ULL << j)) {
                    writePixel(x + __currentFont.width - j, y + i, color);
                } else {
                    writePixel(x + __currentFont.width - j, y + i, bg_color);
                }
            }
        }
    } else {
        hcf();
    }
}

void drawChar(uint16_t x, uint16_t y, char c, uint32_t color, uint32_t bg_color, OffsetMode mode) {
    if constexpr (WOS_HAS_GFX_FB) {
        drawCharNoSwap(x, y, c, color, bg_color, mode);
        swapBuffers();
    } else {
        hcf();
    }
}

uint64_t drawString(uint16_t x, uint16_t y, const char* str, uint32_t color, uint32_t bg_color, OffsetMode mode) {
    if constexpr (WOS_HAS_GFX_FB) {
        uint64_t lines = 0;
        for (size_t i = 0; str[i] != '\0'; i++) {
            if (str[i] == '\n') {
                if (mode == OffsetMode::OFFSET_CHAR) {
                    y++;
                } else {
                    y += __currentFont.height;
                }
                lines++;
                x = 0;
                continue;
            }
            if (mode == OffsetMode::OFFSET_CHAR) {
                drawCharNoSwap((x + i) * __currentFont.width, y * __currentFont.height, str[i], color, bg_color, OffsetMode::OFFSET_PIXEL);
            } else {
                drawCharNoSwap(x + i * __currentFont.width, y, str[i], color, bg_color, OffsetMode::OFFSET_PIXEL);
            }
        }
        swapBuffers();
        return lines;
    } else {
        hcf();
    }
}

void drawLineNoSwap(uint16_t x1, uint16_t y1, uint16_t x2, uint16_t y2, uint32_t color) {
    if constexpr (WOS_HAS_GFX_FB) {
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
        int longest = std::abs(dx);
        int shortest = std::abs(dy);
        if (!(longest > shortest)) {
            longest = std::abs(dy);
            shortest = std::abs(dx);
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
    } else {
        hcf();
    }
}

void drawLine(uint16_t x1, uint16_t y1, uint16_t x2, uint16_t y2, uint32_t color) {
    if constexpr (WOS_HAS_GFX_FB) {
        drawLineNoSwap(x1, y1, x2, y2, color);
        swapBuffers();
    } else {
        hcf();
    }
}

void drawCircle(uint16_t x, uint16_t y, uint16_t radius, uint32_t color, FillMode fill) {
    if constexpr (WOS_HAS_GFX_FB) {
        int f = 1 - radius;
        int ddF_x = 1;
        int ddF_y = -2 * radius;
        int x1 = 0;
        int y1 = radius;

        if (fill == FillMode::FILL) {
            drawLineNoSwap(x, y - radius, x, y + radius, color);
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
            if (fill == FillMode::FILL) {
                drawLineNoSwap(x - x1, y + y1, x + x1, y + y1, color);
                drawLineNoSwap(x - x1, y - y1, x + x1, y - y1, color);
                drawLineNoSwap(x - y1, y + x1, x + y1, y + x1, color);
                drawLineNoSwap(x - y1, y - x1, x + y1, y - x1, color);
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
        swapBuffers();
    } else {
        hcf();
    }
}

uint64_t viewportWidth(void) {
    if (WOS_HAS_GFX_FB) {
        return __framebuffer->width;
    } else {
        hcf();
    }
}

uint64_t viewportHeight(void) {
    if constexpr (WOS_HAS_GFX_FB) {
        return __framebuffer->height;
    } else {
        hcf();
    }
}

uint64_t viewportWidthChars(void) {
    if constexpr (WOS_HAS_GFX_FB) {
        return __framebuffer->width / __currentFont.width;
    } else {
        hcf();
    }
}

uint64_t viewportHeightChars(void) {
    if constexpr (WOS_HAS_GFX_FB) {
        return __framebuffer->height / __currentFont.height;
    } else {
        hcf();
    }
}

void scroll() {
    if constexpr (WOS_HAS_GFX_FB) {
        for (size_t i = 0; i < (__framebuffer->height - __currentFont.height) * __framebuffer->width; i++) {
            backBuffer[i] = backBuffer[i + __framebuffer->width * __currentFont.height];
        }
        for (size_t i = (__framebuffer->height - __currentFont.height) * __framebuffer->width;
             i < __framebuffer->width * __framebuffer->height; i++) {
            backBuffer[i] = TERM_BG_COLOR;
        }
    } else {
        hcf();
    }
}

// int set_font(const FbFont* font) {
//     if(font->getWidth() > 64) {
//         return -1;
//     }
//     __currentFont = *font;
//     return 0;
// }

void mapFramebuffer(void) {
    if constexpr (WOS_HAS_GFX_FB) {
        auto fbPhys = (uint64_t)mm::addr::getPhysPointer((mm::addr::paddr_t)(__framebuffer->address));

        ker::mod::io::serial::write("Mapping framebuffer\n");
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("Width: ");
        ker::mod::io::serial::write(__framebuffer->width);
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("Height: ");
        ker::mod::io::serial::write(__framebuffer->height);
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("Start physical address: ");
        ker::mod::io::serial::writeHex(fbPhys);
        uint64_t framebufferSize = __framebuffer->width * __framebuffer->height * __framebuffer->bpp / 8;
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("Theoretical end physical address: ");
        ker::mod::io::serial::writeHex((uint64_t)fbPhys + framebufferSize);
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("Framebuffer size: ");
        ker::mod::io::serial::writeHex(framebufferSize);
        ker::mod::io::serial::write("\n");
    } else {
        hcf();
    }
}

}  // namespace ker::mod::gfx::fb
