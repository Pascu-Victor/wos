#pragma once

#ifdef WOS_SELFTEST
namespace ker::test::lifecycle60 {
// Same permanent controller, no live evidence/census lease, ACK or owner guard.
// Immutable scalar snapshots only; never dereferences any subject/FD identity.
void publish_census_report() noexcept;
}  // namespace ker::test::lifecycle60
#endif
