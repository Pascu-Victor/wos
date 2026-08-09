#pragma once
#include <array>
#include <cstdarg>
#include <cstdint>
#include <cstring>
#include <limits>
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

namespace detail {

class BoundedFormatWriter {
   public:
    BoundedFormatWriter(char* output, size_t capacity) : output_(output), capacity_(capacity) {}

    void append(char value) {
        size_t const STORED = stored_size();
        if (STORED < writable_capacity()) {
            output_[STORED] = value;
        }
        advance(1);
    }

    void append(const char* source, size_t length) {
        if (source == nullptr || length == 0) {
            return;
        }

        size_t const STORED = stored_size();
        size_t const WRITABLE = writable_capacity();
        size_t const COPY_LEN = STORED < WRITABLE ? ((length < WRITABLE - STORED) ? length : WRITABLE - STORED) : 0;
        if (COPY_LEN != 0) {
            std::memcpy(output_ + STORED, source, COPY_LEN);
        }
        advance(length);
    }

    void append_repeat(char value, size_t count) {
        size_t const STORED = stored_size();
        size_t const WRITABLE = writable_capacity();
        size_t const COPY_LEN = STORED < WRITABLE ? ((count < WRITABLE - STORED) ? count : WRITABLE - STORED) : 0;
        for (size_t i = 0; i < COPY_LEN; ++i) {
            output_[STORED + i] = value;
        }
        advance(count);
    }

    void terminate() {
        if (output_ != nullptr && capacity_ != 0) {
            output_[stored_size()] = '\0';
        }
    }

    [[nodiscard]] auto result() const -> int {
        if (length_overflow_ || logical_size_ > static_cast<size_t>(std::numeric_limits<int>::max())) {
            return -1;
        }
        return static_cast<int>(logical_size_);
    }

   private:
    [[nodiscard]] auto writable_capacity() const -> size_t { return output_ != nullptr && capacity_ != 0 ? capacity_ - 1 : 0; }

    [[nodiscard]] auto stored_size() const -> size_t {
        size_t const WRITABLE = writable_capacity();
        return logical_size_ < WRITABLE ? logical_size_ : WRITABLE;
    }

    void advance(size_t count) {
        if (count > std::numeric_limits<size_t>::max() - logical_size_) {
            logical_size_ = std::numeric_limits<size_t>::max();
            length_overflow_ = true;
            return;
        }
        logical_size_ += count;
    }

    char* output_{};
    size_t capacity_{};
    size_t logical_size_{};
    bool length_overflow_{};
};

template <size_t N>
auto u64_base_to_buffer(std::array<char, N>& buf, uint64_t value, uint32_t base) -> int {
    constexpr char DIGITS[] = "0123456789abcdef";
    if (base < 2 || base > 16 || N < 2) {
        return 0;
    }

    size_t len = 0;
    if (value == 0) {
        buf.at(len++) = '0';
    } else {
        std::array<char, N> reverse{};
        while (value != 0 && len + 1 < N) {
            reverse.at(len++) = DIGITS[value % base];
            value /= base;
        }
        for (size_t i = 0; i < len; ++i) {
            buf.at(i) = reverse.at(len - 1 - i);
        }
    }
    buf.at(len) = '\0';
    return static_cast<int>(len);
}

}  // namespace detail

template <typename T>
auto vsnprintf(char* str, T size, const char* format, va_list args) -> int {
    static_assert(std::is_same_v<T, size_t>, "size must be of type size_t");
    detail::BoundedFormatWriter writer(str, size);
    if (format == nullptr || (str == nullptr && size != 0)) {
        writer.terminate();
        return -1;
    }

    size_t i = 0;
    std::array<char, 64> buf = {};  // no number is bigger than 64 digits in base 10

    auto append_padded = [&writer](const char* value, size_t length, int width, char pad_char) {
        if (width > 0 && static_cast<size_t>(width) > length) {
            writer.append_repeat(pad_char, static_cast<size_t>(width) - length);
        }
        writer.append(value, length);
    };

    while (format[i] != '\0') {
        if (format[i] != '%') {
            writer.append(format[i++]);
            continue;
        }

        ++i;
        if (format[i] == '\0') {
            writer.append('%');
            break;
        }

        int width = 0;
        bool width_from_arg = false;
        bool width_overflow = false;
        char pad_char = ' ';
        if (format[i] == '0') {
            pad_char = '0';
            ++i;
        }
        if (format[i] == '*') {
            width_from_arg = true;
            ++i;
        } else {
            while (format[i] >= '0' && format[i] <= '9') {
                int const DIGIT = format[i] - '0';
                if (width > (std::numeric_limits<int>::max() - DIGIT) / 10) {
                    width_overflow = true;
                } else if (!width_overflow) {
                    width = (width * 10) + DIGIT;
                }
                ++i;
            }
        }
        if (width_overflow) {
            writer.terminate();
            return -1;
        }
        if (width_from_arg) {
            width = va_arg(args, int);
        }

        if (format[i] == '.') {
            ++i;
            int precision = 0;
            bool precision_from_arg = false;
            bool precision_overflow = false;
            if (format[i] == '*') {
                precision_from_arg = true;
                ++i;
            } else {
                while (format[i] >= '0' && format[i] <= '9') {
                    int const DIGIT = format[i] - '0';
                    if (precision > (std::numeric_limits<int>::max() - DIGIT) / 10) {
                        precision_overflow = true;
                    } else if (!precision_overflow) {
                        precision = (precision * 10) + DIGIT;
                    }
                    ++i;
                }
            }
            if (precision_overflow) {
                writer.terminate();
                return -1;
            }
            if (precision_from_arg) {
                precision = va_arg(args, int);
            }
            if (format[i] == 's') {
                const char* value = va_arg(args, const char*);
                if (value == nullptr) {
                    value = "(null)";
                }
                size_t length = 0;
                size_t const LIMIT = precision < 0 ? std::numeric_limits<size_t>::max() : static_cast<size_t>(precision);
                while (length < LIMIT && value[length] != '\0') {
                    ++length;
                }
                append_padded(value, length, width, pad_char);
                ++i;
                continue;
            }
            writer.append('%');
            writer.append('.');
            if (format[i] != '\0') {
                writer.append(format[i++]);
            }
            continue;
        }

        switch (format[i]) {
            case 'd': {
                int const VALUE = va_arg(args, int);
                int const LENGTH = i64_decimal_to_buffer(buf, VALUE);
                append_padded(buf.data(), static_cast<size_t>(LENGTH), width, pad_char);
                break;
            }
            case 'u': {
                unsigned int const VALUE = va_arg(args, unsigned int);
                int const LENGTH = u64_decimal_to_buffer(buf, VALUE);
                append_padded(buf.data(), static_cast<size_t>(LENGTH), width, pad_char);
                break;
            }
            case 'x': {
                unsigned int const VALUE = va_arg(args, unsigned int);
                int const LENGTH = detail::u64_base_to_buffer(buf, VALUE, 16);
                append_padded(buf.data(), static_cast<size_t>(LENGTH), width, pad_char);
                break;
            }
            case 'l': {
                ++i;
                bool const LONG_LONG = format[i] == 'l';
                if (LONG_LONG) {
                    ++i;
                }
                int length = 0;
                if (format[i] == 'u') {
                    uint64_t const VALUE = LONG_LONG ? va_arg(args, unsigned long long) : va_arg(args, unsigned long);
                    length = u64_decimal_to_buffer(buf, VALUE);
                } else if (format[i] == 'x') {
                    uint64_t const VALUE = LONG_LONG ? va_arg(args, unsigned long long) : va_arg(args, unsigned long);
                    length = detail::u64_base_to_buffer(buf, VALUE, 16);
                } else if (format[i] == 'd') {
                    int64_t const VALUE = LONG_LONG ? va_arg(args, long long) : va_arg(args, long);
                    length = i64_decimal_to_buffer(buf, VALUE);
                } else {
                    writer.append('%');
                    writer.append('l');
                    if (LONG_LONG) {
                        writer.append('l');
                    }
                    if (format[i] != '\0') {
                        writer.append(format[i]);
                    }
                    break;
                }
                append_padded(buf.data(), static_cast<size_t>(length), width, pad_char);
                break;
            }
            case 'z': {
                ++i;
                if (format[i] == 'u') {
                    size_t const VALUE = va_arg(args, size_t);
                    int const LENGTH = u64_decimal_to_buffer(buf, static_cast<uint64_t>(VALUE));
                    append_padded(buf.data(), static_cast<size_t>(LENGTH), width, pad_char);
                } else {
                    writer.append('%');
                    writer.append('z');
                    if (format[i] != '\0') {
                        writer.append(format[i]);
                    }
                }
                break;
            }
            case 's': {
                const char* value = va_arg(args, const char*);
                if (value == nullptr) {
                    value = "(null)";
                }
                size_t const LENGTH = std::strlen(value);
                append_padded(value, LENGTH, width, pad_char);
                break;
            }
            case 'c': {
                char const VALUE = static_cast<char>(va_arg(args, int));
                append_padded(&VALUE, 1, width, pad_char);
                break;
            }
            case 'b': {
                unsigned int const VALUE = va_arg(args, unsigned int);
                int const LENGTH = detail::u64_base_to_buffer(buf, VALUE, 2);
                append_padded(buf.data(), static_cast<size_t>(LENGTH), width, pad_char);
                break;
            }
            case 'p': {
                auto* const VALUE = va_arg(args, void*);
                writer.append('0');
                writer.append('x');
                int const LENGTH = detail::u64_base_to_buffer(buf, reinterpret_cast<uintptr_t>(VALUE), 16);
                writer.append(buf.data(), static_cast<size_t>(LENGTH));
                break;
            }
            case 'h': {
                uint8_t const VALUE = static_cast<uint8_t>(va_arg(args, int));
                int const LENGTH = detail::u64_base_to_buffer(buf, VALUE, 16);
                if (LENGTH == 1) {
                    writer.append('0');
                }
                writer.append(buf.data(), static_cast<size_t>(LENGTH));
                break;
            }
            default:
                writer.append(format[i]);
                break;
        }
        if (format[i] != '\0') {
            ++i;
        }
    }

    writer.terminate();
    return writer.result();
}

}  // namespace ker::util::string
