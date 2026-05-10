#pragma once
#include <array>
#include <cstdarg>
#include <cstring>
#include <span>
#include <utility>
// NOLINTNEXTLINE
namespace _std {

auto itoa(int n, std::span<char> s, int base = 10) -> int;

auto u64toh(uint64_t n, std::span<char> s) -> int;

template <typename T>
auto vsnprintf(char* str, T size, const char* format, va_list args) -> int {
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
                case '.': {
                    i++;
                    int precision = 0;
                    if (format[i] == '*') {
                        i++;
                        if (format[i] == 's') {
                            // %.*s - precision from arg
                            precision = va_arg(args, int);
                            const char* s = va_arg(args, const char*);
                            if (s == nullptr) {
                                break;
                            }
                            // count up to precision chars without calling strlen
                            size_t slen = 0;
                            while (std::cmp_less(slen, precision) && s[slen] != '\0') {
                                slen++;
                            }
                            size_t const COPY_LEN = (j + slen + 1 < size) ? slen : (size > j + 1 ? size - j - 1 : 0);
                            if (COPY_LEN > 0) {
                                strncpy(str + j, s, COPY_LEN);
                                j += COPY_LEN;
                            }
                            break;
                        }
                    } else if (format[i] >= '0' && format[i] <= '9') {
                        // parse numeric precision  e.g. %.11s
                        while (format[i] >= '0' && format[i] <= '9') {
                            precision = (precision * 10) + (format[i] - '0');
                            i++;
                        }
                        if (format[i] == 's') {
                            // %.Ns - fixed numeric precision
                            const char* s = va_arg(args, const char*);
                            if (s == nullptr) {
                                break;
                            }
                            size_t slen = 0;
                            while (std::cmp_less(slen, precision) && s[slen] != '\0') {
                                slen++;
                            }
                            size_t const COPY_LEN = (j + slen + 1 < size) ? slen : (size > j + 1 ? size - j - 1 : 0);
                            if (COPY_LEN > 0) {
                                strncpy(str + j, s, COPY_LEN);
                                j += COPY_LEN;
                            }
                            break;
                        }
                    }
                    // Unsupported format, treat literally
                    str[j++] = '%';
                    str[j++] = '.';
                    // don't re-print format[i] — it was already consumed by the numeric-precision loop
                    // Step back so the outer i++ lands on the unrecognised char
                    i--;
                    break;
                }
                case 'd': {
                    if (width_from_arg) {
                        width = va_arg(args, int);
                    }
                    int const N = va_arg(args, int);
                    int const LEN = itoa(N, std::span(buf.data(), buf.size()));

                    // Apply padding
                    int const PAD_LEN = width - LEN;
                    if (PAD_LEN > 0 && j + PAD_LEN < size) {
                        for (int k = 0; k < PAD_LEN; k++) {
                            str[j++] = pad_char;
                        }
                    }

                    strncpy(str + j, buf.data(), size - j);
                    j += LEN;
                    break;
                }
                case 'l': {
                    i++;
                    if (format[i] == 'u') {
                        // %lu - unsigned long (64-bit on x86_64)
                        if (width_from_arg) {
                            width = va_arg(args, int);
                        }
                        uint64_t n = va_arg(args, uint64_t);

                        int len = 0;
                        if (n == 0) {
                            buf[len++] = '0';
                        } else {
                            std::array<char, 64> temp{};
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

                        int const PAD_LEN = width - len;
                        if (PAD_LEN > 0 && j + PAD_LEN < size) {
                            for (int k = 0; k < PAD_LEN; k++) {
                                str[j++] = pad_char;
                            }
                        }

                        strncpy(str + j, buf.data(), size - j);
                        j += len;
                    } else if (format[i] == 'x') {
                        // %lx - unsigned long hex (64-bit on x86_64)
                        if (width_from_arg) {
                            width = va_arg(args, int);
                        }
                        uint64_t const N = va_arg(args, uint64_t);
                        int const LEN = u64toh(N, std::span(buf.data(), buf.size()));

                        int const PAD_LEN = width - LEN;
                        if (PAD_LEN > 0 && j + PAD_LEN < size) {
                            for (int k = 0; k < PAD_LEN; k++) {
                                str[j++] = pad_char;
                            }
                        }

                        strncpy(str + j, buf.data(), size - j);
                        j += LEN;
                    } else if (format[i] == 'd') {
                        // %ld - signed long (64-bit on x86_64)
                        if (width_from_arg) {
                            width = va_arg(args, int);
                        }
                        int64_t const SN = va_arg(args, int64_t);
                        bool const NEGATIVE = SN < 0;
                        uint64_t n = NEGATIVE ? static_cast<uint64_t>(-SN) : static_cast<uint64_t>(SN);

                        int len = 0;
                        if (n == 0) {
                            buf[len++] = '0';
                        } else {
                            std::array<char, 64> temp{};
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
                        if (NEGATIVE) {
                            // Shift right and prepend '-'
                            for (int k = len; k > 0; k--) {
                                buf[k] = buf[k - 1];
                            }
                            buf[0] = '-';
                            len++;
                        }
                        buf[len] = '\0';

                        int const PAD_LEN = width - len;
                        if (PAD_LEN > 0 && j + PAD_LEN < size) {
                            for (int k = 0; k < PAD_LEN; k++) {
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
                                std::array<char, 64> temp{};
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
                            int const PAD_LEN = width - len;
                            if (PAD_LEN > 0 && j + PAD_LEN < size) {
                                for (int k = 0; k < PAD_LEN; k++) {
                                    str[j++] = pad_char;
                                }
                            }

                            strncpy(str + j, buf.data(), size - j);
                            j += len;
                        } else if (format[i] == 'x') {
                            if (width_from_arg) {
                                width = va_arg(args, int);
                            }
                            uint64_t const N = va_arg(args, uint64_t);
                            int const LEN = u64toh(N, std::span(buf.data(), buf.size()));

                            // Apply padding
                            int const PAD_LEN = width - LEN;
                            if (PAD_LEN > 0 && j + PAD_LEN < size) {
                                for (int k = 0; k < PAD_LEN; k++) {
                                    str[j++] = pad_char;
                                }
                            }

                            strncpy(str + j, buf.data(), size - j);
                            j += LEN;
                        }
                    }
                    break;
                }
                case 'x': {
                    if (width_from_arg) {
                        width = va_arg(args, int);
                    }
                    unsigned int const N = va_arg(args, unsigned int);
                    int const LEN = u64toh(N, std::span(buf.data(), buf.size()));

                    // Apply padding
                    int const PAD_LEN = width - LEN;
                    if (PAD_LEN > 0 && j + PAD_LEN < size) {
                        for (int k = 0; k < PAD_LEN; k++) {
                            str[j++] = pad_char;
                        }
                    }

                    strncpy(str + j, buf.data(), size - j);
                    j += LEN;
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
                            std::array<char, 64> temp{};
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
                        int const PAD_LEN = width - len;
                        if (PAD_LEN > 0 && j + PAD_LEN < size) {
                            for (int k = 0; k < PAD_LEN; k++) {
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
                        std::array<char, 64> temp{};
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
                    int const PAD_LEN = width - len;
                    if (PAD_LEN > 0 && j + PAD_LEN < size) {
                        for (int k = 0; k < PAD_LEN; k++) {
                            str[j++] = pad_char;
                        }
                    }

                    strncpy(str + j, buf.data(), size - j);
                    j += len;
                    break;
                }
                case 's': {
                    const char* s = va_arg(args, const char*);
                    if (s == nullptr) {
                        // Print "(null)" like standard printf
                        const char* null_str = "(null)";
                        size_t const NULL_LEN = 6;
                        size_t const SPACE_LEFT = (j < size) ? (size - j - 1) : 0;
                        size_t const TO_COPY = (NULL_LEN < SPACE_LEFT) ? NULL_LEN : SPACE_LEFT;
                        if (TO_COPY > 0) {
                            strncpy(str + j, null_str, TO_COPY);
                            j += TO_COPY;
                        }
                        break;
                    }
                    size_t const SRC_LEN = strlen(s);
                    size_t const SPACE_LEFT = (j < size) ? (size - j - 1) : 0;
                    size_t const TO_COPY = (SRC_LEN < SPACE_LEFT) ? SRC_LEN : SPACE_LEFT;
                    if (TO_COPY > 0) {
                        strncpy(str + j, s, TO_COPY);
                        j += TO_COPY;
                    }
                    break;
                }
                case 'c': {
                    char const C = va_arg(args, int);
                    str[j++] = C;
                    break;
                }
                case 'b': {
                    int const N = va_arg(args, int);
                    int const LEN = itoa(N, std::span(buf.data(), buf.size()), 2);
                    strncpy(str + j, buf.data(), size - j);
                    j += LEN;
                    break;
                }
                case 'p': {
                    uint64_t const N = va_arg(args, uint64_t);
                    str[j++] = '0';
                    str[j++] = 'x';
                    int const LEN = u64toh(N, std::span(buf.data(), buf.size()));
                    strncpy(str + j, buf.data(), size - j);
                    j += LEN;
                    break;
                }
                case 'h': {
                    // padded hex byte
                    uint8_t const N = va_arg(args, int);
                    int const LEN = u64toh(N, std::span(buf.data(), buf.size()));
                    if (LEN == 1) {
                        str[j++] = '0';
                    }
                    strncpy(str + j, buf.data(), size - j);
                    j += LEN;
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

    return static_cast<int>(j);
}

}  // namespace _std

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
