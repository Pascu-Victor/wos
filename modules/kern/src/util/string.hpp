#pragma once
#include <array>
#include <cstdarg>
#include <cstdint>
#include <cstring>
#include <span>
#include <type_traits>
#include <utility>
// NOLINTNEXTLINE
namespace ker::util::string {

auto itoa(int n, std::span<char> s, int base = 10) -> int;

auto u64toh(uint64_t n, std::span<char> s) -> int;

auto u64toa(uint64_t n, std::span<char> s, int base = 10) -> int;

[[nodiscard]] constexpr auto bounded_copy_len(size_t write_pos, size_t source_len, size_t buffer_size) -> size_t {
    if (write_pos + source_len + 1 < buffer_size) {
        return source_len;
    }
    if (buffer_size > write_pos + 1) {
        return buffer_size - write_pos - 1;
    }
    return 0;
}

template <size_t N>
auto u64_decimal_to_buffer(std::array<char, N>& buf, uint64_t n) -> int {
    size_t len = 0;
    if (n == 0) {
        buf.at(len++) = '0';
    } else {
        std::array<char, N> temp{};
        size_t temp_len = 0;
        while (n > 0) {
            temp.at(temp_len++) = static_cast<char>('0' + (n % 10));
            n /= 10;
        }
        for (size_t k = 0; k < temp_len; k++) {
            buf.at(k) = temp.at(temp_len - 1 - k);
        }
        len = temp_len;
    }
    buf.at(len) = '\0';
    return static_cast<int>(len);
}

template <size_t N>
auto i64_decimal_to_buffer(std::array<char, N>& buf, int64_t value) -> int {
    bool const NEGATIVE = value < 0;
    uint64_t const MAGNITUDE = NEGATIVE ? (~static_cast<uint64_t>(value) + 1U) : static_cast<uint64_t>(value);
    int len = u64_decimal_to_buffer(buf, MAGNITUDE);
    if (NEGATIVE) {
        for (int k = len; k > 0; k--) {
            buf.at(static_cast<size_t>(k)) = buf.at(static_cast<size_t>(k - 1));
        }
        buf.at(0) = '-';
        len++;
        buf.at(static_cast<size_t>(len)) = '\0';
    }
    return len;
}

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
                            size_t const COPY_LEN = bounded_copy_len(j, slen, size);
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
                            size_t const COPY_LEN = bounded_copy_len(j, slen, size);
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
                        uint64_t const N = va_arg(args, uint64_t);

                        int const LEN = u64_decimal_to_buffer(buf, N);

                        int const PAD_LEN = width - LEN;
                        if (PAD_LEN > 0 && j + PAD_LEN < size) {
                            for (int k = 0; k < PAD_LEN; k++) {
                                str[j++] = pad_char;
                            }
                        }

                        strncpy(str + j, buf.data(), size - j);
                        j += LEN;
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

                        int const LEN = i64_decimal_to_buffer(buf, SN);

                        int const PAD_LEN = width - LEN;
                        if (PAD_LEN > 0 && j + PAD_LEN < size) {
                            for (int k = 0; k < PAD_LEN; k++) {
                                str[j++] = pad_char;
                            }
                        }

                        strncpy(str + j, buf.data(), size - j);
                        j += LEN;
                    } else if (format[i] == 'l') {
                        i++;
                        if (format[i] == 'u') {
                            if (width_from_arg) {
                                width = va_arg(args, int);
                            }
                            uint64_t const N = va_arg(args, uint64_t);

                            // Convert unsigned to decimal string
                            int const LEN = u64_decimal_to_buffer(buf, N);

                            // Apply padding
                            int const PAD_LEN = width - LEN;
                            if (PAD_LEN > 0 && j + PAD_LEN < size) {
                                for (int k = 0; k < PAD_LEN; k++) {
                                    str[j++] = pad_char;
                                }
                            }

                            strncpy(str + j, buf.data(), size - j);
                            j += LEN;
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
                        size_t const N = va_arg(args, size_t);

                        // Convert unsigned to decimal string
                        int const LEN = u64_decimal_to_buffer(buf, static_cast<uint64_t>(N));

                        // Apply padding
                        int const PAD_LEN = width - LEN;
                        if (PAD_LEN > 0 && j + PAD_LEN < size) {
                            for (int k = 0; k < PAD_LEN; k++) {
                                str[j++] = pad_char;
                            }
                        }

                        strncpy(str + j, buf.data(), size - j);
                        j += LEN;
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
                    unsigned int const N = va_arg(args, unsigned int);

                    // Convert unsigned to decimal string
                    int const LEN = u64_decimal_to_buffer(buf, static_cast<uint64_t>(N));

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

}  // namespace ker::util::string
