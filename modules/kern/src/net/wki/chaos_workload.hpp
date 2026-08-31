#pragma once

#include <cstddef>
#include <cstdint>

namespace ker::net::wki {

constexpr size_t WKI_CHAOS_WORKLOAD_COMMAND_MAX = 256;
constexpr size_t WKI_CHAOS_WORKLOAD_RESULT_MAX = 2048;
constexpr size_t WKI_CHAOS_WORKLOAD_SCRATCH_BYTES = size_t{1} * 1024 * 1024;

// This live-scenario bridge is inert unless both `wki.chaos` and
// `wki.chaos.workload` were present on the kernel command line. Initialization
// conditionally registers the fixed RAM-only block resource used by B1.
void wki_chaos_workload_init(bool workload_boot_allowed);
void wki_chaos_workload_shutdown();

// True only while both boot gates remain active and the dedicated scratch
// resource was registered successfully.
auto wki_chaos_workload_allowed() -> bool;

// Strict bounded procfs command surface. The input is copied before parsing
// and no caller-owned pointer is retained. Returns zero or a negative errno.
auto wki_chaos_workload_configure(const char* command, size_t len) -> int;

// Copy the current single-row schema-v1 result into caller-owned storage.
auto wki_chaos_workload_snapshot(char* out, size_t capacity) -> size_t;

// Receiver-side TASK_SUBMIT hook. This is called only from the task-context
// submit worker before exec construction and never from RX/NAPI. It is a
// no-op unless the boot-gated hold is active, and cannot wait past the
// caller's existing absolute operation deadline.
auto wki_chaos_workload_compute_publish_wait(uint64_t absolute_deadline_us) -> int;

#ifdef WOS_SELFTEST
auto wki_chaos_workload_selftest_parser_and_bounds() -> bool;
#endif

}  // namespace ker::net::wki
