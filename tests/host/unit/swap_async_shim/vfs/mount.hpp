#pragma once
#include <dev/block_device.hpp>
namespace ker::vfs {
inline auto mounted_block_device_overlaps(ker::dev::BlockDevice*) -> bool { return false; }
}  // namespace ker::vfs
