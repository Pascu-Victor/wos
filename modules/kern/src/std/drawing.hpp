#pragma once

#include <defines/defines.hpp>
#include <std/math.hpp>

namespace std::gfx {

    struct ColorRGBA {
        uint8_t r;
        uint8_t g;
        uint8_t b;
        uint8_t a;

        ColorRGBA();
        ColorRGBA(uint8_t r, uint8_t g, uint8_t b, uint8_t a);
        ColorRGBA(uint32_t colorPacked);
        ColorRGBA(const ColorRGBA& color);
        ColorRGBA operator=(const ColorRGBA& color);

        uint32_t toPacked();
    };

    union Pixel {
        uint32_t value;
        ColorRGBA color;
    };

    struct ColorHSVA {
        uint8_t h;
        uint8_t s;
        uint8_t v;
        uint8_t a;
    };

    ColorHSVA rgbaToHsva(ColorRGBA color);

    ColorHSVA shiftHue(ColorHSVA color, int8_t shift);

    ColorRGBA hsvaToRgba(ColorHSVA color);
}