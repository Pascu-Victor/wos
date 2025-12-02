#pragma once
#include <cstdarg>
#include <cstring>
#include <util/mem.hpp>

namespace _std {

auto itoa(int n, char s[], int base = 10) -> int;

auto u64toh(uint64_t n, char s[]) -> int;

template <typename T>
auto vsnprintf(char* str, T size, const char* format, va_list args) -> char* {
    static_assert(std::is_same_v<T, size_t>, "size must be of type size_t");
    size_t i = 0;
    size_t j = 0;
    char buf[64] = {0};  // no number is bigger than 64 digits in base 10

    while (format[i] != '\0') {
        if (format[i] == '%') {
            i++;
            switch (format[i]) {
                case '.':
                    i++;
                    if (format[i] == '*') {
                        i++;
                        if (format[i] == 's') {
                            // %.*s - precision-limited string
                            int precision = va_arg(args, int);
                            const char* s = va_arg(args, const char*);
                            int len = static_cast<int>(strlen(s));
                            int copy_len = (precision < len) ? precision : len;
                            if (j + copy_len < size) {
                                strncpy(str + j, s, copy_len);
                                j += copy_len;
                            }
                            break;
                        }
                    }
                    // Unsupported format, treat literally
                    str[j++] = '%';
                    str[j++] = '.';
                    str[j++] = format[i];
                    break;
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
                    const char* s = va_arg(args, const char*);
                    size_t src_len = strlen(s);
                    size_t space_left = (j < size) ? (size - j - 1) : 0;
                    size_t to_copy = (src_len < space_left) ? src_len : space_left;
                    if (to_copy > 0) {
                        strncpy(str + j, s, to_copy);
                        j += to_copy;
                    }
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
                case 'p': {
                    uint64_t n = va_arg(args, uint64_t);
                    str[j++] = '0';
                    str[j++] = 'x';
                    int len = u64toh(n, buf);
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
