#pragma once
#ifdef WOS_SELFTEST
#include <cstdint>
#include <test/anonymous_usercopy_wire.hpp>
namespace ker::test::anonymous_usercopy {
// Boot-only enable; no reset, Task session or public policy authority.
void enable_boot() noexcept;
auto dispatch(uint64_t request_address, uint64_t bytes, uint64_t unused3, uint64_t unused4) -> uint64_t;
}  // namespace ker::test::anonymous_usercopy
#endif
