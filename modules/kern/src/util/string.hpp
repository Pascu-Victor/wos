#pragma once
#include <array>
#include <cstdarg>
#include <cstring>
#include <span>
#include <util/mem.hpp>

namespace _std {

auto itoa(int n, std::span<char> s, int base = 10) -> int;

auto u64toh(uint64_t n, std::span<char> s) -> int;

template <typename T>
auto vsnprintf(char* str, T size, const char* format, va_list args) -> char* {
    static_assert(std::is_same_v<T, size_t>, "size must be of type size_t");
    size_t i = 0;
    size_t j = 0;
    std::array<char, 64> buf = {};  // no number is bigger than 64 digits in base 10

    while (format[i] != '\0') {
        if (format[i] == '%') {
            i++;

            // Parse width specifier
            int width = 0;
            bool width_from_arg = false;
            char pad_char = ' ';

            // Check for zero-padding
            if (format[i] == '0') {
                pad_char = '0';
                i++;
            }

            // Check for width from argument (*) or numeric width
            if (format[i] == '*') {
                width_from_arg = true;
                i++;
            } else {
                while (format[i] >= '0' && format[i] <= '9') {
                    width = (width * 10) + (format[i] - '0');
                    i++;
                }
            }

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
                    if (width_from_arg) {
                        width = va_arg(args, int);
                    }
                    int n = va_arg(args, int);
                    int len = itoa(n, std::span(buf.data(), buf.size()));

                    // Apply padding
                    int pad_len = width - len;
                    if (pad_len > 0 && j + pad_len < size) {
                        for (int k = 0; k < pad_len; k++) {
                            str[j++] = pad_char;
                        }
                    }

                    strncpy(str + j, buf.data(), size - j);
                    j += len;
                    break;
                }
                case 'l': {
                    i++;
                    if (format[i] == 'u') {
                        // %lu — unsigned long (64-bit on x86_64)
                        if (width_from_arg) {
                            width = va_arg(args, int);
                        }
                        uint64_t n = va_arg(args, uint64_t);

                        int len = 0;
                        if (n == 0) {
                            buf[len++] = '0';
                        } else {
                            std::array<char, 64> temp;
                            int temp_len = 0;
                            while (n > 0) {
                                temp[temp_len++] = static_cast<char>('0' + (n % 10));
                                n /= 10;
                            }
                            for (int k = 0; k < temp_len; k++) {
                                buf[k] = temp[temp_len - 1 - k];
                            }
                            len = temp_len;
                        }
                        buf[len] = '\0';

                        int pad_len = width - len;
                        if (pad_len > 0 && j + pad_len < size) {
                            for (int k = 0; k < pad_len; k++) {
                                str[j++] = pad_char;
                            }
                        }

                        strncpy(str + j, buf.data(), size - j);
                        j += len;
                    } else if (format[i] == 'x') {
                        // %lx — unsigned long hex (64-bit on x86_64)
                        if (width_from_arg) {
                            width = va_arg(args, int);
                        }
                        uint64_t n = va_arg(args, uint64_t);
                        int len = u64toh(n, std::span(buf.data(), buf.size()));

                        int pad_len = width - len;
                        if (pad_len > 0 && j + pad_len < size) {
                            for (int k = 0; k < pad_len; k++) {
                                str[j++] = pad_char;
                            }
                        }

                        strncpy(str + j, buf.data(), size - j);
                        j += len;
                    } else if (format[i] == 'd') {
                        // %ld — signed long (64-bit on x86_64)
                        if (width_from_arg) {
                            width = va_arg(args, int);
                        }
                        int64_t sn = va_arg(args, int64_t);
                        bool negative = sn < 0;
                        uint64_t n = negative ? static_cast<uint64_t>(-sn) : static_cast<uint64_t>(sn);

                        int len = 0;
                        if (n == 0) {
                            buf[len++] = '0';
                        } else {
                            std::array<char, 64> temp;
                            int temp_len = 0;
                            while (n > 0) {
                                temp[temp_len++] = static_cast<char>('0' + (n % 10));
                                n /= 10;
                            }
                            for (int k = 0; k < temp_len; k++) {
                                buf[k] = temp[temp_len - 1 - k];
                            }
                            len = temp_len;
                        }
                        if (negative) {
                            // Shift right and prepend '-'
                            for (int k = len; k > 0; k--) buf[k] = buf[k - 1];
                            buf[0] = '-';
                            len++;
                        }
                        buf[len] = '\0';

                        int pad_len = width - len;
                        if (pad_len > 0 && j + pad_len < size) {
                            for (int k = 0; k < pad_len; k++) {
                                str[j++] = pad_char;
                            }
                        }

                        strncpy(str + j, buf.data(), size - j);
                        j += len;
                    } else if (format[i] == 'l') {
                        i++;
                        if (format[i] == 'u') {
                            if (width_from_arg) {
                                width = va_arg(args, int);
                            }
                            uint64_t n = va_arg(args, uint64_t);

                            // Convert unsigned to decimal string
                            int len = 0;
                            if (n == 0) {
                                buf[len++] = '0';
                            } else {
                                std::array<char, 64> temp;
                                int temp_len = 0;
                                while (n > 0) {
                                    temp[temp_len++] = static_cast<char>('0' + (n % 10));
                                    n /= 10;
                                }
                                // Reverse into buf
                                for (int k = 0; k < temp_len; k++) {
                                    buf[k] = temp[temp_len - 1 - k];
                                }
                                len = temp_len;
                            }
                            buf[len] = '\0';

                            // Apply padding
                            int pad_len = width - len;
                            if (pad_len > 0 && j + pad_len < size) {
                                for (int k = 0; k < pad_len; k++) {
                                    str[j++] = pad_char;
                                }
                            }

                            strncpy(str + j, buf.data(), size - j);
                            j += len;
                        } else if (format[i] == 'x') {
                            if (width_from_arg) {
                                width = va_arg(args, int);
                            }
                            uint64_t n = va_arg(args, uint64_t);
                            int len = u64toh(n, std::span(buf.data(), buf.size()));

                            // Apply padding
                            int pad_len = width - len;
                            if (pad_len > 0 && j + pad_len < size) {
                                for (int k = 0; k < pad_len; k++) {
                                    str[j++] = pad_char;
                                }
                            }

                            strncpy(str + j, buf.data(), size - j);
                            j += len;
                        }
                    }
                    break;
                }
                case 'x': {
                    if (width_from_arg) {
                        width = va_arg(args, int);
                    }
                    unsigned int n = va_arg(args, unsigned int);
                    int len = u64toh(n, std::span(buf.data(), buf.size()));

                    // Apply padding
                    int pad_len = width - len;
                    if (pad_len > 0 && j + pad_len < size) {
                        for (int k = 0; k < pad_len; k++) {
                            str[j++] = pad_char;
                        }
                    }

                    strncpy(str + j, buf.data(), size - j);
                    j += len;
                    break;
                }
                case 'z': {
                    // Length modifier for size_t
                    i++;
                    if (format[i] == 'u') {
                        if (width_from_arg) {
                            width = va_arg(args, int);
                        }
                        size_t n = va_arg(args, size_t);

                        // Convert unsigned to decimal string
                        int len = 0;
                        if (n == 0) {
                            buf[len++] = '0';
                        } else {
                            std::array<char, 64> temp;
                            int temp_len = 0;
                            while (n > 0) {
                                temp[temp_len++] = static_cast<char>('0' + (n % 10));
                                n /= 10;
                            }
                            // Reverse into buf
                            for (int k = 0; k < temp_len; k++) {
                                buf[k] = temp[temp_len - 1 - k];
                            }
                            len = temp_len;
                        }
                        buf[len] = '\0';

                        // Apply padding
                        int pad_len = width - len;
                        if (pad_len > 0 && j + pad_len < size) {
                            for (int k = 0; k < pad_len; k++) {
                                str[j++] = pad_char;
                            }
                        }

                        strncpy(str + j, buf.data(), size - j);
                        j += len;
                    } else {
                        // Unsupported z modifier
                        str[j++] = '%';
                        str[j++] = 'z';
                        str[j++] = format[i];
                    }
                    break;
                }
                case 'u': {
                    if (width_from_arg) {
                        width = va_arg(args, int);
                    }
                    unsigned int n = va_arg(args, unsigned int);

                    // Convert unsigned to decimal string
                    int len = 0;
                    if (n == 0) {
                        buf[len++] = '0';
                    } else {
                        std::array<char, 64> temp;
                        int temp_len = 0;
                        while (n > 0) {
                            temp[temp_len++] = static_cast<char>('0' + (n % 10));
                            n /= 10;
                        }
                        // Reverse into buf
                        for (int k = 0; k < temp_len; k++) {
                            buf[k] = temp[temp_len - 1 - k];
                        }
                        len = temp_len;
                    }
                    buf[len] = '\0';

                    // Apply padding
                    int pad_len = width - len;
                    if (pad_len > 0 && j + pad_len < size) {
                        for (int k = 0; k < pad_len; k++) {
                            str[j++] = pad_char;
                        }
                    }

                    strncpy(str + j, buf.data(), size - j);
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
                    int len = itoa(n, std::span(buf.data(), buf.size()), 2);
                    strncpy(str + j, buf.data(), size - j);
                    j += len;
                    break;
                }
                case 'p': {
                    uint64_t n = va_arg(args, uint64_t);
                    str[j++] = '0';
                    str[j++] = 'x';
                    int len = u64toh(n, std::span(buf.data(), buf.size()));
                    strncpy(str + j, buf.data(), size - j);
                    j += len;
                    break;
                }
                case 'h': {
                    // padded hex byte
                    uint8_t n = va_arg(args, int);
                    int len = u64toh(n, std::span(buf.data(), buf.size()));
                    if (len == 1) {
                        str[j++] = '0';
                    }
                    strncpy(str + j, buf.data(), size - j);
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
// using std_ext::reverse;
// using std_ext::snprintf;
// using std_ext::strcat;
// using std_ext::strcpy;
// using std_ext::strlcat;
// using std_ext::strlen;
// using std_ext::strncmp;
// using std_ext::strncpy;
// using std_ext::u64toa;
using _std::u64toh;
using _std::vsnprintf;
};  // namespace std
