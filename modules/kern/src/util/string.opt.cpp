#include "string.hpp"

#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <span>

// This translation unit provides the kernel's freestanding C string/stdio ABI
// symbols, so raw char buffers, va_list forwarding, and exact libc parameter
// names are intentional at this boundary.
// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access, cppcoreguidelines-pro-bounds-array-to-pointer-decay,
// clang-analyzer-security.insecureAPI.strcpy, readability-inconsistent-declaration-parameter-name,
// readability-inconsistent-declaration-parameter-name)
namespace ker::util::string {
namespace {
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

__attribute__((no_builtin("strlen"))) void reverse(char* s) {
    size_t j = strlen(s);
    if (j == 0) {
        return;
    }
    --j;

    for (size_t i = 0; i < j; i++, j--) {
        char const C = s[i];
        s[i] = s[j];
        s[j] = C;
    }
}
}  // namespace
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
        int const REM = n % base;
        s[i++] = (REM > 9) ? static_cast<char>((REM - 10) + 'a') : static_cast<char>(REM + '0');
        n = n / base;
    }

    if (is_negative) {
        s[i++] = '-';
    }

    s[i] = '\0';

    reverse(s.data());
    return i;
}

auto u64toa(uint64_t n, std::span<char> s, int base) -> int {
    int i = 0;

    if (n == 0) {
        s[i++] = '0';
        s[i] = '\0';
        return 1;
    }

    while (n != 0) {
        int const REM = static_cast<int>(n % base);
        s[i++] = (REM > 9) ? static_cast<char>((REM - 10) + 'a') : static_cast<char>(REM + '0');
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
        int const REM = static_cast<int>(n % 16);
        s[i++] = (REM > 9) ? static_cast<char>((REM - 10) + 'a') : static_cast<char>(REM + '0');
        n = n / 16;
    }

    s[i] = '\0';

    reverse(s.data());

    return i;
}
namespace {
auto strcat(char* dest, const char* src) -> char* {
    size_t const DEST_LEN = ker::util::string::strlen(dest);
    size_t i = 0;

    for (i = 0; src[i] != '\0'; i++) {
        dest[DEST_LEN + i] = src[i];
    }

    dest[DEST_LEN + i] = '\0';

    return dest;
}

auto strlcat(char* dest, const char* src, size_t size) -> size_t {
    size_t const DEST_LEN = ker::util::string::strlen(dest);
    size_t const SRC_LEN = ker::util::string::strlen(src);
    size_t i = 0;

    if (size <= DEST_LEN) {
        return size + SRC_LEN;
    }

    for (i = 0; i < size - DEST_LEN - 1 && src[i] != '\0'; i++) {
        dest[DEST_LEN + i] = src[i];
    }

    dest[DEST_LEN + i] = '\0';

    return DEST_LEN + SRC_LEN;
}

auto strcmp(const char* str1, const char* str2) -> int {
    while (*str1 != '\0' && *str1 == *str2) {
        str1++;
        str2++;
    }
    return static_cast<unsigned char>(*str1) - static_cast<unsigned char>(*str2);
}

auto strncmp(const char* str1, const char* str2, size_t n) -> int {
    for (size_t i = 0; i < n; i++) {
        if (str1[i] != str2[i]) {
            return str1[i] - str2[i];
        }
    }
    return 0;
}

auto strdup(const char* str) -> char* {
    size_t const LEN = ker::util::string::strlen(str);
    char* new_str = new char[LEN + 1];
    strncpy(new_str, str, LEN);
    return new_str;
}
__attribute__((no_builtin("strstr"))) auto strstr(char* haystack, const char* needle) -> char* {
    size_t const HAYSTACK_LEN = ker::util::string::strlen(haystack);
    size_t const NEEDLE_LEN = ker::util::string::strlen(needle);

    if (NEEDLE_LEN == 0) {
        return haystack;
    }
    if (HAYSTACK_LEN < NEEDLE_LEN) {
        return nullptr;
    }

    for (size_t i = 0; i <= HAYSTACK_LEN - NEEDLE_LEN; i++) {
        if (ker::util::string::strncmp(haystack + i, needle, NEEDLE_LEN) == 0) {
            return haystack + i;
        }
    }

    return nullptr;
}
}  // namespace
}  // namespace ker::util::string

// Expose functions as extern "C"
extern "C" {
__attribute__((no_builtin("strlen"))) size_t strlen(const char* str) { return ker::util::string::strlen(str); }

__attribute__((no_builtin("strcpy"))) char* strcpy(char* dest, const char* src) { return ker::util::string::strcpy(dest, src); }

__attribute__((no_builtin("strncpy"))) char* strncpy(char* dest, const char* src, size_t n) {
    return ker::util::string::strncpy(dest, src, n);
}

__attribute__((no_builtin("strlen"))) void reverse(char* s) { ker::util::string::reverse(s); }

__attribute__((no_builtin("strstr"))) char* strstr(const char* haystack, const char* needle) {
    return ker::util::string::strstr(const_cast<char*>(haystack), needle);
}

auto itoa(int n, std::span<char> s, int base) -> int { return ker::util::string::itoa(n, s, base); }

auto u64toa(uint64_t n, std::span<char> s, int base) -> int { return ker::util::string::u64toa(n, s, base); }

auto u64toh(uint64_t n, std::span<char> s) -> int { return ker::util::string::u64toh(n, s); }

auto snprintf(char* str, size_t size, const char* format, ...) -> int {
    va_list args;
    va_start(args, format);
    int const RET = ker::util::string::vsnprintf(str, size, format, args);
    va_end(args);
    return RET;
}

auto vsnprintf(char* str, size_t size, const char* format, va_list args) -> int {
    return ker::util::string::vsnprintf(str, size, format, args);
}

auto strcat(char* dest, const char* src) -> char* { return ker::util::string::strcat(dest, src); }

auto strlcat(char* dest, const char* src, size_t size) -> size_t { return ker::util::string::strlcat(dest, src, size); }

auto strcmp(const char* str1, const char* str2) -> int { return ker::util::string::strcmp(str1, str2); }

auto strncmp(const char* str1, const char* str2, size_t n) -> int { return ker::util::string::strncmp(str1, str2, n); }

auto strdup(const char* str) -> char* { return ker::util::string::strdup(str); }

auto strnlen(const char* s, size_t n) -> size_t {
    size_t len = 0;
    while (len < n && (s[len] != 0)) {
        ++len;
    }
    return len;
}

auto memchr(const void* ptr, int value, size_t num) -> void* {
    const auto* p = static_cast<const unsigned char*>(ptr);
    for (size_t i = 0; i < num; i++) {
        if (p[i] == static_cast<unsigned char>(value)) {
            // NOLINTNEXTLINE
            return const_cast<unsigned char*>(p + i);
        }
    }
    return nullptr;
}
}
// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access, cppcoreguidelines-pro-bounds-array-to-pointer-decay,
// clang-analyzer-security.insecureAPI.strcpy, readability-inconsistent-declaration-parameter-name,
// readability-inconsistent-declaration-parameter-name)
