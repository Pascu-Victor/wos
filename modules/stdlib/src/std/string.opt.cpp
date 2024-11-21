#include "string.hpp"

namespace std {
size_t strlen(const char *str) {
    size_t len = 0;
    while (str[len]) {
        len++;
    }
    return len;
}

char *strcpy(char *dest, const char *src) {
    size_t i = 0;
    for (; src[i] != '\0'; i++) {
        dest[i] = src[i];
    }
    dest[i] = '\0';

    return dest;
}

char *strncpy(char *dest, const char *src, size_t n) {
    size_t i = 0;
    for (; src[i] != '\0' && i < n; i++) {
        dest[i] = src[i];
    }
    dest[i] = '\0';

    return dest;
}

void reverse(char s[]) {
    int c, i, j;

    for (i = 0, j = strlen(s) - 1; i < j; i++, j--) {
        c = s[i];
        s[i] = s[j];
        s[j] = c;
    }
}

int itoa(int n, char s[], int base) {
    int i = 0;
    bool isNegative = false;

    if (n == 0) {
        s[i++] = '0';
        s[i] = '\0';
        return 1;
    }

    if (n < 0 && base == 10) {
        isNegative = true;
        n = -n;
    }

    while (n != 0) {
        int rem = n % base;
        s[i++] = (rem > 9) ? (rem - 10) + 'a' : rem + '0';
        n = n / base;
    }

    if (isNegative) {
        s[i++] = '-';
    }

    s[i] = '\0';

    reverse(s);
    return i;
}

int u64toa(uint64_t n, char s[], int base) {
    int i = 0;

    if (n == 0) {
        s[i++] = '0';
        s[i] = '\0';
        return 1;
    }

    while (n != 0) {
        int rem = n % base;
        s[i++] = (rem > 9) ? (rem - 10) + 'a' : rem + '0';
        n = n / base;
    }

    s[i] = '\0';

    reverse(s);

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

char *snprintf(char *str, size_t size, const char *format, ...) {
    va_list args;
    va_start(args, format);
    vsnprintf(str, size, format, args);
    va_end(args);
    return str;
}

char *strcat(char *dest, const char *src) {
    size_t dest_len = strlen(dest);
    size_t i;

    for (i = 0; src[i] != '\0'; i++) {
        dest[dest_len + i] = src[i];
    }

    dest[dest_len + i] = '\0';

    return dest;
}

size_t strlcat(char *dest, const char *src, size_t size) {
    size_t dest_len = strlen(dest);
    size_t src_len = strlen(src);
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

int strncmp(const char *str1, const char *str2, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (str1[i] != str2[i]) {
            return str1[i] - str2[i];
        }
    }
    return 0;
}

char *strdup(const char *str) {
    size_t len = strlen(str);
    char *new_str = new char[len + 1];
    strncpy(new_str, str, len);
    return new_str;
}
}  // namespace std
