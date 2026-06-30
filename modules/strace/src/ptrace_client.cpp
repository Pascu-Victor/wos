#include "ptrace_client.hpp"

#include <sys/process.h>

#include <array>
#include <cstring>

namespace wos::strace {
namespace {

constexpr size_t MAX_STRING_LEN = 256;

}  // namespace

auto ptrace_call(ker::abi::ptrace::request request, uint64_t pid, uint64_t addr, uint64_t data) -> int64_t {
    return ker::process::ptrace(static_cast<uint64_t>(request), pid, addr, data);
}

auto read_mem_partial(uint64_t pid, uint64_t addr, void* buffer, size_t size) -> size_t {
    ker::abi::ptrace::MemIo io{
        .address = addr,
        .buffer = buffer,
        .size = size,
        .transferred = 0,
    };
    (void)ptrace_call(ker::abi::ptrace::request::READ_MEM, pid, 0, reinterpret_cast<uint64_t>(&io));
    return io.transferred <= size ? io.transferred : size;
}

auto syscall_wait(uint64_t pid, ker::abi::ptrace::StopInfo& info) -> bool {
    std::memset(&info, 0, sizeof(info));
    return ptrace_call(ker::abi::ptrace::request::SYSCALL_WAIT, pid, 0, reinterpret_cast<uint64_t>(&info)) >= 0;
}

auto read_remote_info(uint64_t pid, ker::abi::ptrace::RemoteInfo& info) -> bool {
    std::memset(&info, 0, sizeof(info));
    return ptrace_call(ker::abi::ptrace::request::GET_REMOTE_INFO, pid, 0, reinterpret_cast<uint64_t>(&info)) >= 0;
}

auto read_c_string(uint64_t pid, uint64_t addr) -> std::string {
    if (addr == 0) {
        return "NULL";
    }

    std::array<char, MAX_STRING_LEN> buffer{};
    size_t used = 0;
    while (used < buffer.size()) {
        size_t const GOT = read_mem_partial(pid, addr + used, buffer.data() + used, buffer.size() - used);
        if (GOT == 0) {
            break;
        }

        const auto* const NUL = static_cast<const char*>(std::memchr(buffer.data() + used, '\0', GOT));
        if (NUL != nullptr) {
            return {buffer.data(), static_cast<size_t>(NUL - buffer.data())};
        }
        used += GOT;
    }
    std::string out{buffer.data(), used};
    out += "...";
    return out;
}

}  // namespace wos::strace
