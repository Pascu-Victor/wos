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
    read_dir_entries,
};

}  // namespace ker::abi::vfs
