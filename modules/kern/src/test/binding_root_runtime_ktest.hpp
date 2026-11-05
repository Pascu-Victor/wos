#pragma once

namespace ker::test {
// Real scheduled fixture: permanent executor, registered private roots and
// typed index snapshots. No device mapping provider is enabled.
[[nodiscard]] auto start_binding_root_runtime_selftest() -> bool;
}  // namespace ker::test
