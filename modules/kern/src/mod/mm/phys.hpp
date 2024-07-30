#pragma once

#include <limine.h>

#include <defines/defines.hpp>
#include <mod/mm/paging.hpp>
#include <mod/io/serial/serial.hpp>
#include <mod/mm/addr.hpp>
#include <mod/sys/spinlock.hpp>
#include <std/hcf.hpp>

namespace ker::mod::mm::phys {
    void init(limine_memmap_response *memmapResponse);
    void* pageAlloc(uint64_t size = paging::PAGE_SIZE);
    void pageFree(void* page);

    template <typename T>
    inline static T* pageAlloc(void)
    {
        return (T*)pageAlloc(sizeof(T));
    }

    template <typename T>
    inline static void pageFree(T* page)
    {
        pageFree((void*)page);
    }
}