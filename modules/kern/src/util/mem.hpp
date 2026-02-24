#pragma once
#include <atomic>
#include <cstdint>
#include <defines/defines.hpp>
#include <platform/sys/spinlock.hpp>

extern "C" {
void* memcpy(void* dest, const void* src, size_t n);
void* memset(void* s, int c, size_t n);
void* memmove(void* dest, const void* src, size_t n);
int memcmp(const void* s1, const void* s2, size_t n);
char* strcpy(char* dest, const char* src);

int __cxa_atexit(void (*func)(void*), void* arg, void* dso_handle);

void run_atexit_handlers();
}

namespace std {
template <typename T = void*, typename... Ts>
void* multimemcpy(void* dest, T* src = nullptr, Ts*... rest) {
    size_t size = sizeof(T);
    if (src == nullptr) {
        return dest;
    }
    memcpy(dest, &src, size);
    return multimemcpy((char*)dest + size, rest...);
}

}  // namespace std
