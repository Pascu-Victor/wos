#pragma once
#include <cerrno>
#include <cstddef>
#include <vfs/file.hpp>
namespace ker::mod::mm::swap {
struct SwapExtent;
}
namespace ker::vfs::xfs {
inline auto xfs_collect_swap_extents(File*, ker::mod::mm::swap::SwapExtent**, size_t*) -> int { return -EOPNOTSUPP; }
}  // namespace ker::vfs::xfs
