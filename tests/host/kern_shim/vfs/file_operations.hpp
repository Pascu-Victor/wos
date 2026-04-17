#pragma once
// Host shim for vfs/file_operations.hpp — minimal stub.

#include <cstddef>
#include <cstdint>

namespace ker::vfs {

struct File;

constexpr size_t DIRENT_NAME_MAX = 256;

struct FileOperations {
    void* placeholder = nullptr;
};

}  // namespace ker::vfs
