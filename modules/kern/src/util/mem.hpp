#pragma once
#include <cstdint>

#include <mod/kmem/kmem.hpp>
#include <util/except.hpp>

namespace std {

    void *memcpy(void *dest, const void *src, size_t n);
    void *memset(void *s, int c, size_t n);
    void *memmove(void *dest, const void *src, size_t n);
    int memcmp(const void *s1, const void *s2, size_t n);
    char *strcpy(char *dest, const char *src);
    void *malloc(size_t size);
}

//TODO: void* operator new(std::size_t sz);
//TODO: void* operator new[](std::size_t sz);
//TODO: void operator delete(void* ptr) noexcept;
//TODO: void operator delete(void* ptr, std::size_t size) noexcept;
//TODO: void operator delete[](void* ptr) noexcept;
//TODO: void operator delete[](void* ptr, std::size_t size) noexcept;