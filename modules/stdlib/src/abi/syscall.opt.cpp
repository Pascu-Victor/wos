#include "syscall.hpp"

namespace ker::abi {
uint64_t syscall_impl(callnums callnum) {
    uint64_t ret;
    asm volatile("syscall" : "=a"(ret) : "a"(static_cast<uint64_t>(callnum)) : "memory");
    return ret;
}

uint64_t syscall_impl(callnums callnum, uint64_t* a1) {
    uint64_t ret;
    asm volatile("syscall" : "=a"(ret) : "a"(static_cast<uint64_t>(callnum)), "D"(a1) : "memory");
    return ret;
}

uint64_t syscall_impl(callnums callnum, uint64_t* a1, uint64_t* a2) {
    uint64_t ret;
    asm volatile("syscall" : "=a"(ret) : "a"(static_cast<uint64_t>(callnum)), "D"(a1), "S"(a2) : "memory");
    return ret;
}

uint64_t syscall_impl(callnums callnum, uint64_t* a1, uint64_t* a2, uint64_t* a3) {
    uint64_t ret;
    asm volatile("syscall" : "=a"(ret) : "a"(static_cast<uint64_t>(callnum)), "D"(a1), "S"(a2), "d"(a3) : "memory");
    return ret;
}

uint64_t syscall_impl(callnums callnum, uint64_t* a1, uint64_t* a2, uint64_t* a3, uint64_t* a4) {
    uint64_t ret;
    asm volatile("syscall" : "=a"(ret) : "a"(static_cast<uint64_t>(callnum)), "D"(a1), "S"(a2), "d"(a3), "r"(a4) : "memory");
    return ret;
}

uint64_t syscall_impl(callnums callnum, uint64_t* a1, uint64_t* a2, uint64_t* a3, uint64_t* a4, uint64_t* a5) {
    uint64_t ret;
    asm volatile("syscall" : "=a"(ret) : "a"(static_cast<uint64_t>(callnum)), "D"(a1), "S"(a2), "d"(a3), "r"(a4), "r"(a5) : "memory");
    return ret;
}

uint64_t syscall_impl(callnums callnum, uint64_t* a1, uint64_t* a2, uint64_t* a3, uint64_t* a4, uint64_t* a5, uint64_t* a6) {
    uint64_t ret;
    asm volatile("syscall"
                 : "=a"(ret)
                 : "a"(static_cast<uint64_t>(callnum)), "D"(a1), "S"(a2), "d"(a3), "r"(a4), "r"(a5), "r"(a6)
                 : "memory");
    return ret;
}

uint64_t syscall(callnums callnum, uint64_t* a1, uint64_t* a2, uint64_t* a3, uint64_t* a4, uint64_t* a5, uint64_t* a6) {
    return syscall_impl(callnum, a1, a2, a3, a4, a5, a6);
}

}  // namespace ker::abi
