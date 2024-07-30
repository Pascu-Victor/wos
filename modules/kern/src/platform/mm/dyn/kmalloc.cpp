#include "kmalloc.hpp"

namespace ker::mod::mm::dyn::kmalloc
{   
    static const uint64_t KEMEM_LIST_SIZE = 10;
    // min size is 8 bytes
    // max size is 4096 bytes
    static slab::SlabCache* kmemList[KEMEM_LIST_SIZE];

    void init()
    {
        size_t cacheSize = 8;
        for (uint64_t i = 0; i < KEMEM_LIST_SIZE; i++)
        {
           kmemList[i] = createCache(cacheSize, "kmalloc", 0);
        }
    }

    void* kmalloc(uint64_t size, uint64_t flags)
    {
        for (size_t i = KEMEM_LIST_SIZE - 1; i !=0; i--)
        {
            if (size <= kmemList[i]->objectSize)
            {
                return cacheAlloc(kmemList[i], flags);
            }
        }
        
        //Should never reach here panic!
        hcf();
        return nullptr;
    }

    void kfree(void* ptr, size_t size)
    {
        for (size_t i = KEMEM_LIST_SIZE - 1; i !=0; i--)
        {
            if (size <= kmemList[i]->objectSize)
            {
                cacheFree(kmemList[i], ptr);
                return;
            }
        }
        
        //Should never reach here panic!
        hcf();
    }

    slab::SlabCache* newCache(uint64_t objectSize, const char* name, uint64_t align) {
        if(objectSize % align != 0) {
            objectSize = paging::align(objectSize, align);
        }

        slab::SlabCache* cache = phys::pageAlloc<slab::SlabCache>();
        if (!cache) {
            return nullptr;
        }

        cache->objectSize = objectSize;
        cache->name = name;
        cache->size = paging::PAGE_SIZE - sizeof(slab::SlabCache);
        cache->slabs = nullptr;

        cacheGrow(cache, 1);

        return cache;
    }

    slab::SlabCache* createCache(uint64_t objectSize, const char* name, uint64_t align)
    {
        if(!align){
            align = 8;
        }
        if(!objectSize){
            return nullptr;
        }

        return newCache(objectSize, name, align);
    }

    void* cacheAlloc(slab::SlabCache* cache, uint64_t flags)
    {
        if(!cache){
            return nullptr;
        }

        if(!cache->slabs){
            //invalid Panic!
            hcf();
            return nullptr;
        }

        for(slab::Slab* slab = cache->slabs; slab != nullptr; slab = slab->next){
            if(!slab->freelist){
                continue;
            }

            void* mem = slab->freelist->mem;

            slab->freelist = slab->freelist->next;
            slab->refs++;

            return mem;
        }

        if(flags & KMALLOC_NOGROW){
            return nullptr;
        }

        bool success = cacheGrow(cache, 1);

        if(!success){
            //could not grow the cache Panic!
            hcf();
            return nullptr;
        }

        return cacheAlloc(cache, flags);
    }

    void cacheFree(slab::SlabCache* cache, void* ptr)
    {
        if(!cache || !ptr){
            return;
        }

        for(slab::Slab* slab = cache->slabs; slab != nullptr; slab = slab->next){
            if(slab->refs == 0){
                continue;
            }

            slab->refs--;
            *((void **) ptr) = slab->freelist->mem;
            slab->freelist->mem = ptr;
            return;
        }

        //could not find the slab to free the memory Panic!
        hcf();
    }

    void cacheDestroy(slab::SlabCache* cache)
    {
        if(!cache){
            return;
        }

        for(slab::Slab* slab = cache->slabs; slab != nullptr;){
            if(slab->refs != 0){
                //Panic!
                hcf();
            }
            for(slab::FreeSlab* freeSlab = slab->freelist; freeSlab != nullptr;){
                slab::FreeSlab* next = freeSlab->next;
                phys::pageFree(freeSlab->mem);
                freeSlab = next;
            }
        }

        memset(cache, 0, sizeof(slab::SlabCache));
        phys::pageFree(cache);
    }

}