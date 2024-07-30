#pragma once

#include "slab.hpp"

namespace ker::mod::mm::dyn::kmalloc
{
    static const uint64_t KMALLOC_NOGROW = 1;
    
    void init();
    void* kmalloc(uint64_t size, uint64_t flags = 0);
    void kfree(void* ptr, size_t size);

    slab::SlabCache* createCache(uint64_t objectSize, const char* name, uint64_t align);
    void* cacheAlloc(slab::SlabCache* cache, uint64_t flags);
    void cacheFree(slab::SlabCache* cache, void* ptr);
    void cacheDestroy(slab::SlabCache* cache);

    template <typename T>
    inline static T* kmalloc(void)
    {
        return (T*)kmalloc(sizeof(T));
    }

    template <typename T>
    inline static void kfree(T* ptr)
    {
        kfree(ptr, sizeof(T));
    }
}

void* operator new(std::size_t sz);
void* operator new[](std::size_t sz);
// void operator delete(void* ptr) noexcept;
void operator delete(void* ptr, std::size_t size) noexcept;
// void operator delete[](void* ptr) noexcept;
void operator delete[](void* ptr, std::size_t size) noexcept;