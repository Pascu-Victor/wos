#pragma once
#include <abi/callnums.hpp>
#include <defines/defines.hpp>
#include <std/type_traits.hpp>

namespace ker::abi {
uint64_t syscall_impl(callnums callnum);

uint64_t syscall_impl(callnums callnum, const void* a1);

uint64_t syscall_impl(callnums callnum, const void* a1, const void* a2);

uint64_t syscall_impl(callnums callnum, const void* a1, const void* a2, const void* a3);

uint64_t syscall_impl(callnums callnum, const void* a1, const void* a2, const void* a3, const void* a4);

uint64_t syscall_impl(callnums callnum, const void* a1, const void* a2, const void* a3, const void* a4, const void* a5);

uint64_t syscall_impl(callnums callnum, const void* a1, const void* a2, const void* a3, const void* a4, const void* a5, const void* a6);

uint64_t syscall(callnums callnum, uint64_t* a1 = nullptr, uint64_t* a2 = nullptr, uint64_t* a3 = nullptr, uint64_t* a4 = nullptr,
                 uint64_t* a5 = nullptr, uint64_t* a6 = nullptr);
}  // namespace ker::abi
