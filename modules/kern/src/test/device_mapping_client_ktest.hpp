#pragma once

namespace ker::test {
// Called by an ordinary scheduled selftest worker; no early-boot shortcut.
[[nodiscard]] auto run_device_mapping_client_selftest() -> bool;
}  // namespace ker::test
