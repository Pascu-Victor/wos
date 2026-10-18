namespace ker::mod::mm::file_mapping {
class FileMappingBinding;
}
// Explicit Host endpoints; the resolver, CAS, publisher, splitter, pin commit,
// registry, preparation owners and NUMA factory below are actual source.
namespace {
unsigned publication_depth{}, cow_depth{}, pin_depth{}, root_readers{};
unsigned actual_flushes{}, tracked_pages{};
bool binding_matches{true};
bool fork_mode{}, fork_eager{};
ker::mod::mm::paging::PageTable* fork_child{};
unsigned fork_exclusives{}, fork_reader_attempts{}, fork_cow_entries{}, fork_swap_clones{};
std::vector<std::string> fork_events;
ker::mod::mm::huge::PreparedSpan* span_at_flush{};
ker::mod::mm::huge::SplitPlanStorage* plan_at_flush{};
}  // namespace
namespace ker::mod::mm::addr {
auto get_virt_pointer(vaddr_t p) -> vaddr_t* {
    if (host_table_address(p)) return reinterpret_cast<vaddr_t*>(p);
    for (auto const& b : backend().blocks)
        if (b.storage && p >= b.physical && p - b.physical < uint64_t{b.pages} * PAGE)
            return reinterpret_cast<vaddr_t*>(reinterpret_cast<uint64_t>(b.storage.get()) + p - b.physical);
    ADD_FAILURE() << "unexpected Host address endpoint " << p;
    return nullptr;
}
}  // namespace ker::mod::mm::addr
#include <platform/sched/task.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/syscall_preparation.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
namespace ker::syscall::vmem {
extern std::atomic<const SharedVmemPublicationGuard*> g_shared_vmem_publication_owner;
}
namespace ker::mod::sched {
auto preempt_count() -> uint32_t { return backend().preempt_depth; }
void kern_yield() {
    host_range::check_yield_boundary();
    ++host_range::yields;
    if (host_range::on_yield) host_range::on_yield();
}
}  // namespace ker::mod::sched
ker::mod::sys::Mutex g_shared_vmem_publication_lock;
namespace host_range {
inline bool concurrent = false;
inline std::atomic<unsigned> active{}, records{}, clears{};
}  // namespace host_range
namespace ker::mod::sys {
void Mutex::record_owner(uint64_t) {
    if (host_range::concurrent) {
        EXPECT_EQ(host_range::active.fetch_add(1), 0U);
        ++host_range::records;
        return;
    }
    EXPECT_EQ(cow_depth + pin_depth, 0U);
    ++publication_depth;
    ++host_range::tries;
}
void Mutex::clear_owner() {
    if (host_range::expect_fork_wait) {
        EXPECT_EQ(root_readers + cow_depth + pin_depth + backend().preempt_depth, 0U);
        if (host_range::current) EXPECT_EQ(host_range::current->usercopy_pagemap_accesses.load(), 0U);
    }
    if (host_range::concurrent) {
        EXPECT_EQ(host_range::active.fetch_sub(1), 1U);
        ++host_range::clears;
        return;
    }
    EXPECT_EQ(cow_depth + pin_depth, 0U);
    EXPECT_EQ(publication_depth, 1U);
    --publication_depth;
    ++host_range::unlocks;
}
void Mutex::lock() {
    ++host_range::blocking;
    if (host_range::expect_fork_wait) {
        EXPECT_TRUE(host_range::interrupts);
        EXPECT_EQ(root_readers + cow_depth + pin_depth + backend().preempt_depth, 0U);
        if (host_range::current) EXPECT_EQ(host_range::current->usercopy_pagemap_accesses.load(), 0U);
    }
    if (host_range::on_blocking) host_range::on_blocking();
    if (host_range::expect_fork_wait) EXPECT_EQ(publication_depth, 0U);
    ASSERT_TRUE(try_lock());
}
}  // namespace ker::mod::sys
namespace host_range {
void check_preparation_allocation() {
    if (!expect_fork_wait) return;
    ++allocation_checks;
    check_yield_boundary();
}
void check_yield_boundary() {
    EXPECT_EQ(publication_depth + cow_depth + pin_depth + root_readers + backend().preempt_depth, 0U);
    if (current) EXPECT_EQ(current->usercopy_pagemap_accesses.load(), 0U);
    EXPECT_TRUE(interrupts);
}
}  // namespace host_range
namespace ker::mod::mm::virt {
using addr::paddr_t;
using addr::vaddr_t;
using paging::PageTable;
using paging::PageTableEntry;
constexpr paddr_t PADDR_INVALID = static_cast<paddr_t>(-1);
constexpr uint64_t PTE_FRAME_MASK = 0x000ffffffffff000ULL;
constexpr size_t USER_PAGEMAP_ACCESS_GATE_NONE = SIZE_MAX;
auto& managed_huge_registry = ::huge_core;
struct HostRootControl {
    uint64_t serial{}, ps_geometry_epoch{1};
    uint64_t ps_admission_suspensions{};  // Actual308 counter declaration; finite root lookup remains a Host port.
};
HostRootControl host_root_control;
HostRootControl host_child_control{23, 1};
PageTable* kernel_pagemap{};
auto find_user_root_control_locked(PageTable* root) -> HostRootControl* {
    auto& control = host_root_control;
    if (fork_mode && root == fork_child) return &host_child_control;
    if (root != runtime_root || !huge_root_live) return nullptr;
    control.serial = runtime_serial;
    return &control;
}
struct HostLock {
    unsigned& depth;
    auto lock_irqsave() -> uint64_t {
        EXPECT_EQ(depth, 0U);
        ++depth;
        return 0;
    }
    void unlock_irqrestore(uint64_t) {
        EXPECT_EQ(depth, 1U);
        --depth;
    }
};
HostLock cow_pte_lock{cow_depth}, user_mapping_pin_lock{pin_depth};
auto acquire_user_pagemap_access(PageTable* root, bool, size_t& index) -> bool {
    if (host_range::expect_fork_wait) {
        ++host_range::range_readers;
        auto* owner = ker::syscall::vmem::g_shared_vmem_publication_owner.load(std::memory_order_acquire);
        EXPECT_NE(owner, nullptr);
        if (owner) EXPECT_TRUE(owner->acquired());
        EXPECT_EQ(publication_depth, 1U);
        EXPECT_EQ(backend().preempt_depth, 1U);
        EXPECT_TRUE(host_range::interrupts);
        if (host_range::current) EXPECT_EQ(host_range::current->usercopy_pagemap_accesses.load(), 1U);
    }
    EXPECT_EQ(cow_depth + pin_depth, 0U);
    if (fork_mode && host_range::exclusive) {
        ++fork_reader_attempts;
        ADD_FAILURE() << "nested reader under the original exclusive";
        return false;
    }
    if ((root != runtime_root && !(fork_mode && root == fork_child)) || host_range::reader_busy || host_range::exclusive) return false;
    ++root_readers;
    index = 0;
    return true;
}
void release_user_pagemap_access(size_t index) {
    if (index != USER_PAGEMAP_ACCESS_GATE_NONE) {
        EXPECT_GT(root_readers, 0U);
        --root_readers;
    }
}
void flush_pagemap_after_update(PageTable* root, uint64_t va, bool full) {
    EXPECT_EQ(root, runtime_root);
    EXPECT_EQ(va, 0U);
    EXPECT_TRUE(full);
    EXPECT_EQ(cow_depth + pin_depth, 0U);
    EXPECT_EQ(publication_depth, 1U);
    EXPECT_TRUE(root_readers > 0 || host_range::exclusive);
    huge::Snapshot s{};
    huge_core.snapshot(s);
    if (!fork_mode) EXPECT_GT(s.charged_pages, 0U);
    if (fork_mode) fork_events.push_back("ACK");
    if (span_at_flush) {
        EXPECT_TRUE(span_at_flush->empty());
        EXPECT_GT(s.mapped_pages, 0U);
    }
    if (plan_at_flush && !(fork_mode && backend().preempt_depth != 0)) {
        EXPECT_EQ(s.splitting_pages, uint64_t{plan_at_flush->descriptor_count} * huge::LEAF_PAGES);
        EXPECT_GE(s.table_pages, plan_at_flush->table_count);
        for (uint16_t i = 0; i < plan_at_flush->descriptor_count; ++i) EXPECT_EQ(plan_at_flush->tables[i], nullptr);
    }
    ++actual_flushes;
}
void owned_frame_track_fresh_normal_mapping(PageTable* root, uint64_t, uint64_t, uint64_t) {
    if (fork_mode && root == fork_child) {
        EXPECT_TRUE(host_range::exclusive);
        EXPECT_EQ(backend().preempt_depth, 1U);
        return;
    }
    EXPECT_GT(actual_flushes, 0U);
    huge::Snapshot snapshot{};
    huge_core.snapshot(snapshot);
    EXPECT_GE(snapshot.splitting_pages, huge::LEAF_PAGES);
    ++tracked_pages;
}
struct HostBindingIndex {
    auto matches_pin_locked(PageTable*, uint64_t, uint64_t, file_mapping::FileMappingBinding* binding) -> bool {
        return binding_matches && binding == nullptr;
    }
    auto root_empty_locked(PageTable*) -> bool { return true; }
} file_mapping_index;
// Actual PageTable/PageTableEntry and UserPagePin declarations are extracted.

auto begin_user_pagemap_exclusive(PageTable* root) -> size_t {
    EXPECT_EQ(root, runtime_root);
    EXPECT_EQ(root_readers, 0U);
    EXPECT_FALSE(host_range::exclusive);
    host_range::exclusive = true;
    if (fork_mode) {
        ++fork_exclusives;
        fork_events.push_back("exclusive");
    }
    return 0;
}
void end_user_pagemap_exclusive(size_t index) {
    EXPECT_EQ(index, 0U);
    EXPECT_TRUE(host_range::exclusive);
    host_range::exclusive = false;
}
}  // namespace ker::mod::mm::virt
