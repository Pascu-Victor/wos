#pragma once
#include <std/math.hpp>
#include <std/mem.hpp>

namespace std {
size_t strlen(const char *str);

char *strcpy(char *dest, const char *src);

char *strncpy(char *dest, const char *src, size_t n);

void reverse(char s[]);

int itoa(int n, char s[], int base = 10);

int u64toa(uint64_t n, char s[], int base = 10);

// TODO: char* strdup(const char* str);
}  // namespace std
