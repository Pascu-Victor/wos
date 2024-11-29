#pragma once
#include <defines/defines.hpp>

void mini_malloc_init();
void* mini_malloc(size_t size);
void mini_free(void* address);
