#include <abi/callnums/process.h>

#include <abi/callnums.hpp>
#include <abi/resource.hpp>
#include <cstdint>

namespace {
auto process_call(ker::abi::process::procmgmt_ops op, uint64_t resource, uint64_t pointer) -> uint64_t {
    auto result = static_cast<uint64_t>(ker::abi::callnums::PROCESS);
    register uint64_t version asm("r8") = ker::abi::resource::VERSION;
    asm volatile("syscall"
                 : "+a"(result)
                 : "D"(static_cast<uint64_t>(op)), "S"(resource), "d"(pointer), "r"(version)
                 : "rcx", "r11", "memory");
    return result;
}
}  // namespace

// No libc, interpreter, TLS, or startup descriptor opens. This tests the
// kernel exec contract even when the inherited descriptor limit is zero.
extern "C" [[noreturn]] void _start() {
    ker::abi::resource::Limit limit{};
    auto result = process_call(ker::abi::process::procmgmt_ops::GETRLIMIT, static_cast<uint64_t>(ker::abi::resource::Id::NOFILE),
                               reinterpret_cast<uint64_t>(&limit));
    uint64_t const STATUS = result == 0 && limit.soft == 0 && limit.hard == 256 ? 0 : 1;
    static_cast<void>(process_call(ker::abi::process::procmgmt_ops::EXIT, STATUS, 0));
    __builtin_unreachable();
}
