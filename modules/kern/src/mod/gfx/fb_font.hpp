#pragma once
#include <stdint.h>
#include <cstddef>

#include <util/mem.hpp>
#include <util/string.hpp>

class FbFont {
public:
    void loadFont();
    char name[256];
    uint8_t height; // max 64
    uint8_t width; // max 64
    uint64_t data[256][64];
public:
    FbFont(const char* name, uint8_t height, uint8_t width, const uint64_t data[256][64]);
    FbFont();
    FbFont(const FbFont& other);
    FbFont& operator=(const FbFont& other);

    uint8_t getHeight() const;

    uint8_t getWidth() const;

    const uint64_t* getData(char c) const;
};