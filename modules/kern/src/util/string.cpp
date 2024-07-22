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

    // char* strdup(const char* str) {
    //     size_t len = strlen(str);
    //     char* new_str = new char[len + 1];
    //     strcpy(new_str, str);
    //     return new_str;
    // }    
}