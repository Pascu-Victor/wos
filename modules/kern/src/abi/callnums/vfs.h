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
    mount,
    mkdir,
    readlink,
    symlink,
    sendfile,
    stat,
    fstat,
};

}  // namespace ker::abi::vfs
