#include <abi/callnums/process.h>

#include <abi/callnums.hpp>
#include <abi/resource.hpp>
#include <cstdint>

namespace {
using ker::abi::process::procmgmt_ops;
namespace resource = ker::abi::resource;
auto process_call(procmgmt_ops op, uint64_t argument, void* value) -> uint64_t {
    auto result = static_cast<uint64_t>(ker::abi::callnums::PROCESS);
    register uint64_t version asm("r8") = resource::VERSION;
    asm volatile("syscall"
                 : "+a"(result)
                 : "D"(static_cast<uint64_t>(op)), "S"(argument), "d"(value), "r"(version)
                 : "rcx", "r11", "memory");
    return result;
}
[[noreturn]] void finish(uint64_t status) {
    static_cast<void>(process_call(procmgmt_ops::EXIT, status, nullptr));
    __builtin_unreachable();
}
}  // namespace

extern "C" [[noreturn, gnu::used]] void stack_probe(uint64_t* initial_rsp) {
    constexpr uint64_t STACK = static_cast<uint64_t>(resource::Id::STACK);
    constexpr uint64_t BOUND = 64 * 1024;
    uint64_t const TOP = (reinterpret_cast<uint64_t>(initial_rsp) + 4095) & ~uint64_t{4095};
    resource::Limit limit{};
    if (process_call(procmgmt_ops::GETRLIMIT, STACK, &limit) != 0 || limit.soft != BOUND) {
        finish(1);
    }
    auto* boundary = reinterpret_cast<volatile uint8_t*>(TOP - BOUND);
    *boundary = 0x5a;
    auto** argv = reinterpret_cast<char**>(initial_rsp + 1);
    if (initial_rsp[0] > 1 && argv[1][0] == 'd') {
        // First byte below the admitted depth must fault with SIGSEGV.
        boundary[-1] = 1;
        finish(2);
    }
    limit.soft = 0;
    if (process_call(procmgmt_ops::SETRLIMIT, STACK, &limit) != 0 || *boundary != 0x5a) {
        finish(3);
    }
    *boundary = 0x6b;  // Existing backing survives lowering, including writes.
    limit.soft = 2 * BOUND;
    if (process_call(procmgmt_ops::SETRLIMIT, STACK, &limit) != 0) {
        finish(4);
    }
    *reinterpret_cast<volatile uint8_t*>(TOP - 2 * BOUND) = 0x7c;
    resource::Usage usage{};
    if (process_call(procmgmt_ops::RESOURCE_USAGE, STACK, &usage) != 0 || usage.current != 2 * BOUND || usage.peak < usage.current) {
        finish(5);
    }
    finish(0);
}

extern "C" [[gnu::naked, noreturn]] void _start() { asm volatile("mov %rsp, %rdi\n\tand $-16, %rsp\n\tcall stack_probe\n\tud2"); }
