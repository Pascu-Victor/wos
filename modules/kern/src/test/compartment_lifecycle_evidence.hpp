#pragma once

#ifdef WOS_SELFTEST
#include <cstdint>
#include <test/compartment_lifecycle_runtime.hpp>
#include <test/lifecycle_wire60.hpp>

namespace ker::test::lifecycle60::evidence {
struct FileIdentity {
    uint64_t device{}, inode{}, bytes{};
    auto operator==(const FileIdentity&) const -> bool = default;
};

// Every operation requires the execution-owned backend permission. The
// caller must not also hold ACK/census, IRQ, publication or registry guards.
// Temporary native File references finish before success or failure returns.
auto read_manifest(const EvidenceLease&, criterion9::wire::Manifest& output) noexcept -> int;
auto check_driver_image(const EvidenceLease&, const criterion9::wire::ManifestEntry&) noexcept -> int;
auto check_source_fd(const EvidenceLease&, int fd, const criterion9::wire::ManifestEntry&, FileIdentity& output) noexcept -> int;
// Immutable value-only observations after actual driver admission. Same
// permanent controller only; these own no File/Task/namespace references.
auto driver_wire_header() noexcept -> const criterion9::wire::Record*;
auto driver_wire_fd_ledger() noexcept -> const criterion9::wire::FdLedger*;
}  // namespace ker::test::lifecycle60::evidence
#endif
