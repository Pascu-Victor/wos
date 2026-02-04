#include "string.hpp"

#include <stdarg.h>

namespace _std {
__attribute__((no_builtin("strlen"))) inline auto strlen(const char* str) -> size_t {
    size_t len = 0;
    while (str[len] != '\0') {
        len++;
    }
    return len;
}

__attribute__((no_builtin("strcpy"))) char* strcpy(char* dest, const char* src) {
    size_t i = 0;
    for (; src[i] != '\0'; i++) {
        dest[i] = src[i];
    }
    dest[i] = '\0';

    return dest;
}

__attribute__((no_builtin("strncpy"))) char* strncpy(char* dest, const char* src, size_t n) {
    size_t i = 0;
    for (; src[i] != '\0' && i < n; i++) {
        dest[i] = src[i];
    }
    dest[i] = '\0';

    return dest;
}

__attribute__((no_builtin("strlen"))) void reverse(char s[]) {
    int c, i, j;

    for (i = 0, j = strlen(s) - 1; i < j; i++, j--) {
        c = s[i];
        s[i] = s[j];
        s[j] = c;
    }
}
auto itoa(int n, std::span<char> s, int base) -> int {
    int i = 0;
    bool is_negative = false;

    if (n == 0) {
        s[i++] = '0';
        s[i] = '\0';
        return 1;
    }

    if (n < 0 && base == 10) {
        is_negative = true;
        n = -n;
    }

    while (n != 0) {
        int rem = n % base;
        s[i++] = (rem > 9) ? static_cast<char>((rem - 10) + 'a') : static_cast<char>(rem + '0');
        n = n / base;
    }

    if (is_negative) {
        s[i++] = '-';
    }

    s[i] = '\0';

    reverse(s.data());
    return i;
}

auto u64toa(uint64_t n, std::span<char> s, int base = 10) -> int {
    int i = 0;

    if (n == 0) {
        s[i++] = '0';
        s[i] = '\0';
        return 1;
    }

    while (n != 0) {
        int rem = static_cast<int>(n % base);
        s[i++] = (rem > 9) ? static_cast<char>((rem - 10) + 'a') : static_cast<char>(rem + '0');
        n = n / base;
    }

    s[i] = '\0';

    reverse(s.data());

    return i;
}

auto u64toh(uint64_t n, std::span<char> s) -> int {
    int i = 0;

    if (n == 0) {
        s[i++] = '0';
        s[i] = '\0';
        return 1;
    }

    while (n != 0) {
        int rem = static_cast<int>(n % 16);
        s[i++] = (rem > 9) ? static_cast<char>((rem - 10) + 'a') : static_cast<char>(rem + '0');
        n = n / 16;
    }

    s[i] = '\0';

    reverse(s.data());

    return i;
}

int u64toh(uint64_t n, char s[]) {
    int i = 0;

    if (n == 0) {
        s[i++] = '0';
        s[i] = '\0';
        return 1;
    }

    while (n != 0) {
        int rem = n % 16;
        s[i++] = (rem > 9) ? (rem - 10) + 'a' : rem + '0';
        n = n / 16;
    }

    s[i] = '\0';

    reverse(s);

    return i;
}

namespace std = _std;

char* snprintf(char* str, size_t size, const char* format, ...) {
    va_list args;
    va_start(args, format);
    _std::vsnprintf(str, size, format, args);
    va_end(args);
    return str;
}

char* strcat(char* dest, const char* src) {
    size_t dest_len = _std::strlen(dest);
    size_t i;

    for (i = 0; src[i] != '\0'; i++) {
        dest[dest_len + i] = src[i];
    }

    dest[dest_len + i] = '\0';

    return dest;
}

size_t strlcat(char* dest, const char* src, size_t size) {
    size_t dest_len = _std::strlen(dest);
    size_t src_len = _std::strlen(src);
    size_t i;

    if (size <= dest_len) {
        return size + src_len;
    }

    for (i = 0; i < size - dest_len - 1 && src[i] != '\0'; i++) {
        dest[dest_len + i] = src[i];
    }

    dest[dest_len + i] = '\0';

    return dest_len + src_len;
}

int strcmp(const char* str1, const char* str2) {
    while (*str1 != '\0' && *str1 == *str2) {
        str1++;
        str2++;
    }
    return static_cast<unsigned char>(*str1) - static_cast<unsigned char>(*str2);
}

int strncmp(const char* str1, const char* str2, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (str1[i] != str2[i]) {
            return str1[i] - str2[i];
        }
    }
    return 0;
}

char* strdup(const char* str) {
    size_t len = _std::strlen(str);
    char* new_str = new char[len + 1];
    strncpy(new_str, str, len);
    return new_str;
}
}  // namespace _std

// Expose functions as extern "C"
extern "C" {
__attribute__((no_builtin("strlen"))) size_t strlen(const char* str) { return _std::strlen(str); }

__attribute__((no_builtin("strcpy"))) char* strcpy(char* dest, const char* src) { return _std::strcpy(dest, src); }

__attribute__((no_builtin("strncpy"))) char* strncpy(char* dest, const char* src, size_t n) { return _std::strncpy(dest, src, n); }

__attribute__((no_builtin("strlen"))) void reverse(char s[]) { _std::reverse(s); }

int itoa(int n, std::span<char> s, int base) { return _std::itoa(n, s, base); }

int u64toa(uint64_t n, std::span<char> s, int base) { return _std::u64toa(n, s, base); }

int u64toh(uint64_t n, std::span<char> s) { return _std::u64toh(n, s); }

char* snprintf(char* str, size_t size, const char* format, ...) {
    va_list args;
    va_start(args, format);
    char* result = _std::snprintf(str, size, format, args);
    va_end(args);
    return result;
}

char* strcat(char* dest, const char* src) { return _std::strcat(dest, src); }

size_t strlcat(char* dest, const char* src, size_t size) { return _std::strlcat(dest, src, size); }

int strcmp(const char* str1, const char* str2) { return _std::strcmp(str1, str2); }

int strncmp(const char* str1, const char* str2, size_t n) { return _std::strncmp(str1, str2, n); }

char* strdup(const char* str) { return _std::strdup(str); }

size_t strnlen(const char* s, size_t n) {
    size_t len = 0;
    while (len < n && s[len]) ++len;
    return len;
}
}
