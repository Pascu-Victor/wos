#pragma once
#include <vfs/file.hpp>
namespace ker::vfs {
inline constexpr int O_LOCAL = 0x10000;
inline constexpr int O_NO_CACHE = 0x20000;
inline auto vfs_open_file(const char*, int, int) -> File* { return nullptr; }
inline void vfs_close_file(File*) {}
}  // namespace ker::vfs
