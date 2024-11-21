#include "kmalloc.hpp"

namespace ker::mod::mm::dyn::kmalloc {
// static const uint64_t KEMEM_LIST_SIZE = 6;
// min size is 8 bytes
// max size is 4096 bytes
// static slab::SlabCache* kmemList[KEMEM_LIST_SIZE];
static tlsf_t tlsf = nullptr;
static ker::mod::sys::Spinlock *kmallocLock;

void init() {
    // for (uint64_t i = 0; i < KEMEM_LIST_SIZE; i++) {
    //     kmemList[i] = createCache(cacheSize, "kmalloc", 0);
    // }
    tlsf = tlsf_create_with_pool(phys::pageAlloc(1024 * 1024 * 256), 1024 * 1024 * 256);
    kmallocLock = (ker::mod::sys::Spinlock *)tlsf_malloc(tlsf, sizeof(ker::mod::sys::Spinlock));
}

void *malloc(uint64_t size) {
    kmallocLock->lock();
    void *ptr = tlsf_malloc(tlsf, size);
    kmallocLock->unlock();
    return ptr;
}

void *realloc(void *ptr, int sz) {
    kmallocLock->lock();
    void *newPtr = tlsf_realloc(tlsf, ptr, sz);
    kmallocLock->unlock();
    return newPtr;
}

void *calloc(int sz) {
    kmallocLock->lock();
    void *ptr = tlsf_malloc(tlsf, sz);
    kmallocLock->unlock();
    if (ptr) [[likely]]
        memset(ptr, 0, sz);
    return ptr;
}

void free(void *ptr) {
    kmallocLock->lock();
    tlsf_free(tlsf, ptr);
    kmallocLock->unlock();
}

}  // namespace ker::mod::mm::dyn::kmalloc

void *operator new(size_t sz) {
    void *ptr = ker::mod::mm::dyn::kmalloc::malloc(sz + sizeof(uint64_t));
    *((uint64_t *)ptr) = sz;
    return (void *)((uint64_t)ptr + sizeof(uint64_t));
}

void *operator new[](size_t sz) {
    void *ptr = ker::mod::mm::dyn::kmalloc::malloc(sz);
    return (void *)((uint64_t)ptr);
}

// void operator delete(void* ptr) noexcept {
//     ker::mod::mm::dyn::kmalloc::kfree(ptr);
// }

void operator delete(void *ptr, size_t size) noexcept {
    (void)size;
    ker::mod::mm::dyn::kmalloc::free(ptr);
}

// void operator delete[](void* ptr) noexcept {
//     ker::mod::mm::dyn::kmalloc::kfree(ptr);
// }

void operator delete[](void *ptr, size_t size) noexcept {
    (void)size;
    ker::mod::mm::dyn::kmalloc::free(ptr);
}

void operator delete(void *ptr) noexcept { ker::mod::mm::dyn::kmalloc::free(ptr); }

void operator delete[](void *ptr) noexcept { ker::mod::mm::dyn::kmalloc::free(ptr); }
