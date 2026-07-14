#include <atomic>
#include <cstdint>
#include <net/wki/dev_server.hpp>
#include <test/ktest.hpp>
#include <type_traits>
#include <utility>

KTEST(WkiDevServerBinding, LifecycleFlagsAreAtomic) {
    using Binding = ker::net::wki::DevServerBinding;

    constexpr bool REFS_ATOMIC = std::is_same_v<decltype(std::declval<Binding&>().refs), std::atomic<uint32_t>>;
    constexpr bool RETIRING_ATOMIC = std::is_same_v<decltype(std::declval<Binding&>().retiring), std::atomic<bool>>;

    KEXPECT_TRUE(REFS_ATOMIC);
    KEXPECT_TRUE(RETIRING_ATOMIC);
}

KTEST(WkiDevServerBinding, MovePreservesLifecycleFlags) { KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_binding_lifecycle_flags()); }

KTEST(WkiDevServerBinding, MoveTransfersBlockWriterLeaseExactlyOnce) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_block_writer_lease_transfer());
}

KTEST(WkiDevServerBinding, RetirementOwnershipAndWriterReservationPersistUntilErase) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_retirement_ownership_guards());
}

KTEST(WkiDevServerDetach, AdmissionIsExactIdempotentAndBlocksReplacement) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_detach_admission_lifecycle());
}

KTEST(WkiDevServerAttachAckFailure, DefersExactBlockVfsAndNetCleanupOutsideRx) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_attach_ack_failure_defers_cleanup());
}
