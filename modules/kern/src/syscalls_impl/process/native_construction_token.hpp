#pragma once

#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::syscall::process {
// Compare-only invocation identity. This owns no Task reference and must never
// be converted back into authority by dereferencing actor. Cookies are checked
// and never reused by the diagnostic controller; an empty token is ordinary.
struct NativeConstructionToken {
#ifdef WOS_SELFTEST
    ker::mod::sched::task::Task* actor{};
    uint64_t cookie{};
#endif
};

#ifdef WOS_SELFTEST
enum class NativeConstructionFault : uint8_t;
// Root's scheduled controller supplies these definitions. Begin runs at public
// CREATE before construction locks, including an IF-off syscall entry; any
// bounded parking requires an explicit safe-context policy in that controller.
auto exec_selftest_begin_native_construction_attempt(ker::mod::sched::task::Task& actor) noexcept -> NativeConstructionToken;
// No allocation, blocking, Task dereference or backend work. This may run under
// publication/owner guards and must reject every foreign/stale/empty token.
auto exec_selftest_consume_scoped_native_construction_fault(NativeConstructionToken token, NativeConstructionFault fault) noexcept -> bool;
#endif
}  // namespace ker::syscall::process
