#include "fb.hpp"

#include <extern/limine.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "mod/gfx/fb_font.hpp"
#include "mod/io/serial/serial.hpp"
#include "platform/mm/addr.hpp"
#include "util/hcf.hpp"

namespace ker::mod::gfx::fb {
namespace {
constexpr uint64_t MAX_FRAMEBUFFER_WIDTH = 3840;
constexpr uint64_t MAX_FRAMEBUFFER_HEIGHT = 2160;
constexpr std::size_t BACK_BUFFER_PIXELS = MAX_FRAMEBUFFER_WIDTH * MAX_FRAMEBUFFER_HEIGHT;
using BackBuffer = std::array<uint32_t, BACK_BUFFER_PIXELS>;

// Limine discovers requests by scanning the .requests section; keep the section
// and volatile storage semantics while using anonymous-namespace linkage.
__attribute__((used, section(".requests"))) volatile limine_framebuffer_request framebuffer_request = {
    .id = LIMINE_FRAMEBUFFER_REQUEST_ID,
    .revision = 0,
    .response = nullptr,
};

FbFont current_font;
limine_framebuffer* framebuffer;
[[maybe_unused]]
BackBuffer back_buffer{};
static_assert(sizeof(BackBuffer) == BACK_BUFFER_PIXELS * sizeof(uint32_t));

[[maybe_unused]]
auto back_buffer_at(std::size_t index) -> uint32_t& {
    if (index >= back_buffer.size()) {
        hcf();
    }
    // Bounds are checked above; operator[] avoids hosted-library exception paths.
    return back_buffer[index];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
}
}  // namespace

void init() {
    if constexpr (WOS_HAS_GFX_FB) {
        if (framebuffer_request.response == nullptr || framebuffer_request.response->framebuffer_count < 1) {
            hcf();
        }

        framebuffer = framebuffer_request.response->framebuffers[0];

        framebuffer->width = std::min<uint64_t>(framebuffer->width, MAX_FRAMEBUFFER_WIDTH);
        framebuffer->height = std::min<uint64_t>(framebuffer->height, MAX_FRAMEBUFFER_HEIGHT);

        // manual constructor calling since we can't use new for now
        std::strncpy(current_font.name.data(), "default", current_font.name.size());
        constexpr uint8_t DEFAULT_FONT_WIDTH = 16;
        constexpr uint8_t DEFAULT_FONT_HEIGHT = 16;
        current_font.height = DEFAULT_FONT_HEIGHT;
        current_font.width = DEFAULT_FONT_WIDTH;
        current_font.load_font();

        clear(TERM_BG_COLOR);
    } else {
        mod::io::serial::write(
            "Tried to init disabled subsystem: GFX::FB\n"
            "recompile with this system enabled or do not call this init functon\n"
            "HALTING");
        hcf();
    }
}

namespace {
[[maybe_unused]]
inline void swap_buffers() {
    if constexpr (WOS_HAS_GFX_FB) {
        auto* fb = static_cast<uint32_t*>(framebuffer->address);
        for (size_t i = 0; i < framebuffer->width * framebuffer->height; i++) {
            fb[i] = back_buffer_at(i);
        }
    } else {
        hcf();
    }
}
}  // namespace

inline void write_pixel(uint16_t x, uint16_t y, uint32_t color) {
    if constexpr (WOS_HAS_GFX_FB) {
        back_buffer_at(static_cast<std::size_t>(x + (y * framebuffer->width))) = color;
    } else {
        hcf();
    }
}

void clear(uint32_t color) {
    if constexpr (WOS_HAS_GFX_FB) {
        for (size_t i = 0; i < framebuffer->width * framebuffer->height; i++) {
            back_buffer_at(i) = color;
        }
        swap_buffers();
    } else {
        hcf();
    }
}

void draw_rect(uint16_t x, uint16_t y, uint16_t w, uint16_t h, uint32_t color, FillMode fill) {
    if constexpr (WOS_HAS_GFX_FB) {
        if (fill == FillMode::FILL) {
            for (size_t i = 0; i < h; i++) {
                for (size_t j = 0; j < w; j++) {
                    write_pixel(x + j, y + i, color);
                }
            }
        } else {
            for (size_t i = 0; i < w; i++) {
                write_pixel(x + i, y, color);
                write_pixel(x + i, y + h, color);
            }
            for (size_t i = 0; i < h; i++) {
                write_pixel(x, y + i, color);
                write_pixel(x + w, y + i, color);
            }
        }
        swap_buffers();
    } else {
        hcf();
    }
}

namespace {
[[maybe_unused]]
inline void draw_char_no_swap(uint16_t x, uint16_t y, char c, uint32_t color, uint32_t bg_color, OffsetMode mode) {
    if constexpr (WOS_HAS_GFX_FB) {
        if (mode == OffsetMode::OFFSET_CHAR) {
            x *= current_font.width;
            y *= current_font.height;
        }
        const uint64_t* data = current_font.get_data(c);
        for (size_t i = 0; i < current_font.height; i++) {
            for (size_t j = 0; j < current_font.width; j++) {
                if ((data[i] & (1ULL << j)) != 0U) {
                    write_pixel(x + current_font.width - j, y + i, color);
                } else {
                    write_pixel(x + current_font.width - j, y + i, bg_color);
                }
            }
        }
    } else {
        hcf();
    }
}
}  // namespace

void draw_char(uint16_t x, uint16_t y, char c, uint32_t color, uint32_t bg_color, OffsetMode mode) {
    if constexpr (WOS_HAS_GFX_FB) {
        draw_char_no_swap(x, y, c, color, bg_color, mode);
        swap_buffers();
    } else {
        hcf();
    }
}

uint64_t draw_string(uint16_t x, uint16_t y, const char* str, uint32_t color, uint32_t bg_color, OffsetMode mode) {
    if constexpr (WOS_HAS_GFX_FB) {
        uint64_t lines = 0;
        for (size_t i = 0; str[i] != '\0'; i++) {
            if (str[i] == '\n') {
                if (mode == OffsetMode::OFFSET_CHAR) {
                    y++;
                } else {
                    y += current_font.height;
                }
                lines++;
                x = 0;
                continue;
            }
            if (mode == OffsetMode::OFFSET_CHAR) {
                draw_char_no_swap((x + i) * current_font.width, y * current_font.height, str[i], color, bg_color, OffsetMode::OFFSET_PIXEL);
            } else {
                draw_char_no_swap(x + (i * current_font.width), y, str[i], color, bg_color, OffsetMode::OFFSET_PIXEL);
            }
        }
        swap_buffers();
        return lines;
    } else {
        hcf();
    }
}

namespace {
[[maybe_unused]]
void draw_line_no_swap(uint16_t x1, uint16_t y1, uint16_t x2, uint16_t y2, uint32_t color) {
    if constexpr (WOS_HAS_GFX_FB) {
        int const DX = x2 - x1;
        int const DY = y2 - y1;
        int dx1 = 0;
        int dy1 = 0;
        int dx2 = 0;
        int dy2 = 0;
        if (DX < 0) {
            dx1 = -1;
        } else if (DX > 0) {
            dx1 = 1;
        }
        if (DY < 0) {
            dy1 = -1;
        } else if (DY > 0) {
            dy1 = 1;
        }
        if (DX < 0) {
            dx2 = -1;
        } else if (DX > 0) {
            dx2 = 1;
        }
        int longest = std::abs(DX);
        int shortest = std::abs(DY);
        if (!(longest > shortest)) {
            longest = std::abs(DY);
            shortest = std::abs(DX);
            if (DY < 0) {
                dy2 = -1;
            } else if (DY > 0) {
                dy2 = 1;
            }
            dx2 = 0;
        }
        int numerator = longest >> 1;
        for (int i = 0; i <= longest; i++) {
            write_pixel(x1, y1, color);
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
}  // namespace

void draw_line(uint16_t x1, uint16_t y1, uint16_t x2, uint16_t y2, uint32_t color) {
    if constexpr (WOS_HAS_GFX_FB) {
        draw_line_no_swap(x1, y1, x2, y2, color);
        swap_buffers();
    } else {
        hcf();
    }
}

void draw_circle(uint16_t x, uint16_t y, uint16_t radius, uint32_t color, FillMode fill) {
    if constexpr (WOS_HAS_GFX_FB) {
        int f = 1 - radius;
        int dd_f_x = 1;
        int dd_f_y = -2 * radius;
        int x1 = 0;
        int y1 = radius;

        if (fill == FillMode::FILL) {
            draw_line_no_swap(x, y - radius, x, y + radius, color);
        } else {
            write_pixel(x, y + radius, color);
            write_pixel(x, y - radius, color);
            write_pixel(x + radius, y, color);
            write_pixel(x - radius, y, color);
        }

        while (x1 < y1) {
            if (f >= 0) {
                y1--;
                dd_f_y += 2;
                f += dd_f_y;
            }
            x1++;
            dd_f_x += 2;
            f += dd_f_x;
            if (fill == FillMode::FILL) {
                draw_line_no_swap(x - x1, y + y1, x + x1, y + y1, color);
                draw_line_no_swap(x - x1, y - y1, x + x1, y - y1, color);
                draw_line_no_swap(x - y1, y + x1, x + y1, y + x1, color);
                draw_line_no_swap(x - y1, y - x1, x + y1, y - x1, color);
            } else {
                write_pixel(x + x1, y + y1, color);
                write_pixel(x - x1, y + y1, color);
                write_pixel(x + x1, y - y1, color);
                write_pixel(x - x1, y - y1, color);
                write_pixel(x + y1, y + x1, color);
                write_pixel(x - y1, y + x1, color);
                write_pixel(x + y1, y - x1, color);
                write_pixel(x - y1, y - x1, color);
            }
        }
        swap_buffers();
    } else {
        hcf();
    }
}

uint64_t viewport_width() {
    if (WOS_HAS_GFX_FB) {
        return framebuffer->width;
    }
    hcf();
}

uint64_t viewport_height() {
    if constexpr (WOS_HAS_GFX_FB) {
        return framebuffer->height;
    } else {
        hcf();
    }
}

uint64_t viewport_width_chars() {
    if constexpr (WOS_HAS_GFX_FB) {
        return framebuffer->width / current_font.width;
    } else {
        hcf();
    }
}

uint64_t viewport_height_chars() {
    if constexpr (WOS_HAS_GFX_FB) {
        return framebuffer->height / current_font.height;
    } else {
        hcf();
    }
}

void scroll() {
    if constexpr (WOS_HAS_GFX_FB) {
        for (size_t i = 0; i < (framebuffer->height - current_font.height) * framebuffer->width; i++) {
            back_buffer_at(i) = back_buffer_at(i + (framebuffer->width * current_font.height));
        }
        for (size_t i = (framebuffer->height - current_font.height) * framebuffer->width; i < framebuffer->width * framebuffer->height;
             i++) {
            back_buffer_at(i) = TERM_BG_COLOR;
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

void map_framebuffer() {
    if constexpr (WOS_HAS_GFX_FB) {
        auto const FB_PHYS =
            reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<mm::addr::paddr_t>(framebuffer->address)));

        ker::mod::io::serial::write("Mapping framebuffer\n");
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("Width: ");
        ker::mod::io::serial::write(framebuffer->width);
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("Height: ");
        ker::mod::io::serial::write(framebuffer->height);
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("Start physical address: ");
        ker::mod::io::serial::write_hex(FB_PHYS);
        uint64_t const FRAMEBUFFER_SIZE = framebuffer->width * framebuffer->height * framebuffer->bpp / 8;
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("Theoretical end physical address: ");
        ker::mod::io::serial::write_hex(FB_PHYS + FRAMEBUFFER_SIZE);
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("Framebuffer size: ");
        ker::mod::io::serial::write_hex(FRAMEBUFFER_SIZE);
        ker::mod::io::serial::write("\n");
    } else {
        hcf();
    }
}

}  // namespace ker::mod::gfx::fb
