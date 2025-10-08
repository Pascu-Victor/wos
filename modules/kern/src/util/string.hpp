#pragma once
#include <stdarg.h>

#include <util/mem.hpp>

namespace _std {
// size_t strlen(const char *str);

// char *strcpy(char *dest, const char *src);

// char *strncpy(char *dest, const char *src, size_t n);

// void reverse(char s[]);

int itoa(int n, char s[], int base = 10);

// int u64toa(uint64_t n, char s[], int base = 10);

int u64toh(uint64_t n, char s[]);

// char *snprintf(char *str, size_t size, const char *format, ...);

// char *strcat(char *dest, const char *src);

// size_t strlcat(char *dest, const char *src, size_t size);

// int strncmp(const char *s1, const char *s2, size_t n);

// char *vsnprintf(char *str, size_t size, const char *format, va_list args);
template <typename T>
char *vsnprintf(char *str, T size, const char *format, va_list args) {
    static_assert(std::is_same_v<T, size_t>, "size must be of type size_t");
    size_t i = 0;
    size_t j = 0;
    char buf[64] = {0};  // no number is bigger than 64 digits in base 10

    while (format[i] != '\0') {
        if (format[i] == '%') {
            i++;
            switch (format[i]) {
                case 'd': {
                    int n = va_arg(args, int);
                    int len = itoa(n, buf);
                    strncpy(str + j, buf, size - j);
                    j += len;
                    break;
                }
                case 'x': {
                    uint64_t n = va_arg(args, uint64_t);
                    int len = u64toh(n, buf);
                    strncpy(str + j, buf, size - j);
                    j += len;
                    break;
                }
                case 's': {
                    const char *s = va_arg(args, const char *);
                    strncpy(str + j, s, size - j);
                    j += strlen(s);
                    break;
                }
                case 'c': {
                    char c = va_arg(args, int);
                    str[j++] = c;
                    break;
                }
                case 'b': {
                    int n = va_arg(args, int);
                    int len = itoa(n, buf, 2);
                    strncpy(str + j, buf, size - j);
                    j += len;
                    break;
                }
                case 'h': {
                    // padded hex byte
                    uint64_t n = va_arg(args, uint64_t);
                    int len = u64toh(n, buf);
                    if (len == 1) {
                        str[j++] = '0';
                    }
                    strncpy(str + j, buf, size - j);
                    j += len;
                    break;
                }
                default:
                    str[j++] = format[i];
                    break;
            }
        } else {
            str[j++] = format[i];
        }
        i++;
    }

    str[j] = '\0';

    return str;
}

}  // namespace _std

//
namespace std {
using _std::itoa;
// using _std::reverse;
// using _std::snprintf;
// using _std::strcat;
// using _std::strcpy;
// using _std::strlcat;
// using _std::strlen;
// using _std::strncmp;
// using _std::strncpy;
// using _std::u64toa;
using _std::u64toh;
using _std::vsnprintf;
};  // namespace std
