#pragma once

#include <cstddef>
#include <cstdint>

namespace ker::vfs {
// WOS mlibc uses signed 64-bit off_t and pid_t. This is the public FCNTL
// snapshot, independently of the narrower authenticated WKI diagnostic PID.
struct VfsFlockAbi {
    int16_t l_type = 0;
    int16_t l_whence = 0;
    int64_t l_start = 0;
    int64_t l_len = 0;
    int64_t l_pid = 0;
};
static_assert(sizeof(VfsFlockAbi) == 32);
static_assert(offsetof(VfsFlockAbi, l_type) == 0);
static_assert(offsetof(VfsFlockAbi, l_whence) == 2);
static_assert(offsetof(VfsFlockAbi, l_start) == 8);
static_assert(offsetof(VfsFlockAbi, l_len) == 16);
static_assert(offsetof(VfsFlockAbi, l_pid) == 24);
}  // namespace ker::vfs
