#include <cstring>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <syscalls_impl/shm/shm.hpp>
#include <test/ktest.hpp>

namespace ker::syscall::shm {
namespace visible_mm = ker::mod::mm;
namespace visible_paging = ker::mod::mm::paging;

auto visible_leaf_raw(visible_paging::PageTable* root, uint64_t address) -> uint64_t {
    auto* table = root;
    if (table == nullptr) return 0;
    for (unsigned level = 4; level > 1; --level) {
        auto const& entry = table->entries.at((address >> (12U + (level - 1U) * 9U)) & 0x1FFU);
        if (entry.present == 0 || (level < 4 && entry.pagesize != 0)) return 0;
        table = reinterpret_cast<visible_paging::PageTable*>(visible_mm::addr::get_virt_pointer(entry.frame << visible_paging::PAGE_SHIFT));
    }
    uint64_t raw = 0;
    auto const& entry = table->entries.at((address >> visible_paging::PAGE_SHIFT) & 0x1FFU);
    std::memcpy(&raw, &entry, sizeof(raw));
    return raw;
}
}  // namespace ker::syscall::shm

KTEST(SysvVisiblePid, HostProcessAndThreadPreserveCanonicalMetadata) { KEXPECT_TRUE(ker::syscall::shm::shm_selftest_visible_pid_host()); }
KTEST(SysvVisiblePid, PrivateCreatorAndExplicitOperatorsUseLocalProcessNames) {
    KEXPECT_TRUE(ker::syscall::shm::shm_selftest_visible_pid_private());
}
KTEST(SysvVisiblePid, HistoricalCreatorSurvivesTaskRetirement) {
    KEXPECT_TRUE(ker::syscall::shm::shm_selftest_visible_pid_historical_creator());
}
KTEST(SysvVisiblePid, ForeignPidScopesAndCollidingLocalNamesProjectZero) {
    KEXPECT_TRUE(ker::syscall::shm::shm_selftest_visible_pid_foreign_scopes());
}
KTEST(SysvVisiblePid, PidScopeReleaseAndReuseCannotRebindHistory) {
    KEXPECT_TRUE(ker::syscall::shm::shm_selftest_visible_pid_scope_reuse());
}
KTEST(SysvVisiblePid, ForkCleanupAndIpcRetirementPreservePhysicalAttachments) {
    KEXPECT_TRUE(ker::syscall::shm::shm_selftest_visible_pid_fork_retirement());
}
KTEST(SysvVisiblePid, DenialClosureAndStatOutputGuardsRemainUnchanged) {
    KEXPECT_TRUE(ker::syscall::shm::shm_selftest_visible_pid_denial_guards());
}
