#pragma once
#include <util/mem.hpp>
#include <util/math.hpp>

namespace std {
    size_t strlen(const char* str);

    char *strcpy(char *dest, const char *src);

    void reverse(char s[]);

    void itoa(int n, char s[], int base=10);

    void u64toa(uint64_t n, char s[], int base=10);

    //TODO: char* strdup(const char* str);
}