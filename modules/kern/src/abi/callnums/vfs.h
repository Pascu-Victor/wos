#pragma once
#include <cstdint>

namespace ker::abi::vfs {
enum class ops : uint64_t {
    open,
    read,
    write,
    close,
    lseek,
    isatty,
};

}  // namespace ker::abi::vfs
