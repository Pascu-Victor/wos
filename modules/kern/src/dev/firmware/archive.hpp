#pragma once

#include <dev/firmware/types.hpp>

namespace ker::dev::firmware {

// Canonical firmware-relative name, including room for its terminating NUL.
// The view is length-bounded; this function never searches beyond it.
[[nodiscard]] auto canonical_name(std::string_view name) -> bool;

// No allocation, locking, callbacks, VFS access or hashing. Source storage is
// accessible and immutable for the call and every subsequently retained view.
// Output is private scratch until success; every failure returns count zero.
// MAX_RECORDS includes the trailer. Duplicate detection rescans prior validated
// headers/names, with bounded quadratic work and constant auxiliary storage.
[[nodiscard]] auto scan_archive(std::span<const uint8_t> source, std::span<ArchiveEntry> private_output) -> ArchiveResult;

}  // namespace ker::dev::firmware
