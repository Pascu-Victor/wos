#include "string.hpp"

namespace std {
    size_t strlen(const char* str) {
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

    // char* strdup(const char* str) {
    //     size_t len = strlen(str);
    //     char* new_str = new char[len + 1];
    //     strcpy(new_str, str);
    //     return new_str;
    // }    
}