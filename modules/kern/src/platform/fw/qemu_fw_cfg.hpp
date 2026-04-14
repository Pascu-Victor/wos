#pragma once

#include <cstddef>
#include <cstdint>

namespace ker::platform::fw {

// Read a QEMU fw_cfg named file into buf.  Returns bytes read, or -1 if not found.
// Maximum name length: 55 chars (QEMU limit).  buf must be at least buf_size bytes.
auto fw_cfg_read_file(const char* name, void* buf, size_t buf_size) -> int;

}  // namespace ker::platform::fw
