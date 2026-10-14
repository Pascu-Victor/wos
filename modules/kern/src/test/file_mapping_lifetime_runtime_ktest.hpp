#pragma once

namespace ker::test {

// Posts ordinary scheduled diagnostic work after PID1/providers exist. This
// does not register a pre-scheduler KTEST or execute any retirement inline.
[[nodiscard]] auto start_file_mapping_runtime_selftest() -> bool;

}  // namespace ker::test
