#include "drawing.hpp"

namespace std::gfx {

ColorRGBA::ColorRGBA() {
    r = 0;
    g = 0;
    b = 0;
    a = 0;
}

ColorRGBA::ColorRGBA(uint8_t r, uint8_t g, uint8_t b, uint8_t a) {
    this->r = r;
    this->g = g;
    this->b = b;
    this->a = a;
}

ColorRGBA::ColorRGBA(uint32_t colorPacked) {
    this->a = (colorPacked >> 24) & 0xFF;
    this->r = (colorPacked >> 16) & 0xFF;
    this->g = (colorPacked >> 8) & 0xFF;
    this->b = colorPacked & 0xFF;
}

ColorRGBA::ColorRGBA(const ColorRGBA& color) {
    this->r = color.r;
    this->g = color.g;
    this->b = color.b;
    this->a = color.a;
}

ColorRGBA ColorRGBA::operator=(const ColorRGBA& color) {
    this->r = color.r;
    this->g = color.g;
    this->b = color.b;
    this->a = color.a;
    return *this;
}

uint32_t ColorRGBA::toPacked() { return (a << 24) | (r << 16) | (g << 8) | b; }

ColorHSVA rgbaToHsva(ColorRGBA color) {
    uint8_t min, max, delta;
    ColorHSVA hsv;
    hsv.a = color.a;

    min = math::min(color.r, math::min(color.g, color.b));
    max = math::max(color.r, math::max(color.g, color.b));

    delta = max - min;

    hsv.v = max;
    if (max == 0) {
        hsv.s = 0;
    } else {
        hsv.s = 255 * delta / max;
    }

    if (max == min) {
        hsv.h = 0;
    } else {
        if (max == color.r) {
            hsv.h = 43 * (color.g - color.b) / delta;
        } else if (max == color.g) {
            hsv.h = 85 + 43 * (color.b - color.r) / delta;
        } else {
            hsv.h = 171 + 43 * (color.r - color.g) / delta;
        }
    }
    return hsv;
}

ColorHSVA shiftHue(ColorHSVA color, int8_t shift) {
    color.h += shift;
    if (color.h > 255) {
        color.h -= 255;
    } else if (color.h < 0) {
        color.h += 255;
    }
    return color;
}

ColorRGBA hsvaToRgba(ColorHSVA color) {
    uint8_t region, remainder, p, q, t;
    ColorRGBA ARGB;
    ARGB.a = color.a;

    if (color.s == 0) {
        ARGB.r = color.v;
        ARGB.g = color.v;
        ARGB.b = color.v;
        return ARGB;
    }

    region = color.h / 43;
    remainder = (color.h - (region * 43)) * 6;

    p = (color.v * (255 - color.s)) >> 8;
    q = (color.v * (255 - ((color.s * remainder) >> 8))) >> 8;
    t = (color.v * (255 - ((color.s * (255 - remainder)) >> 8))) >> 8;

    switch (region) {
        case 0:
            ARGB.r = color.v;
            ARGB.g = t;
            ARGB.b = p;
            break;
        case 1:
            ARGB.r = q;
            ARGB.g = color.v;
            ARGB.b = p;
            break;
        case 2:
            ARGB.r = p;
            ARGB.g = color.v;
            ARGB.b = t;
            break;
        case 3:
            ARGB.r = p;
            ARGB.g = q;
            ARGB.b = color.v;
            break;
        case 4:
            ARGB.r = t;
            ARGB.g = p;
            ARGB.b = color.v;
            break;
        default:
            ARGB.r = color.v;
            ARGB.g = p;
            ARGB.b = q;
            break;
    }
    return ARGB;
}

}  // namespace std::gfx
