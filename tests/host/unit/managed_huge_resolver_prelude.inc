// Explicit Host endpoints; the resolver, CAS, publisher, splitter, pin commit,
// registry, preparation owners and NUMA factory below are actual source.
namespace {
unsigned publication_depth{}, cow_depth{}, pin_depth{}, root_readers{};
unsigned actual_flushes{}, tracked_pages{};
bool binding_matches{true};
ker::mod::mm::huge::PreparedSpan* span_at_flush{};
ker::mod::mm::huge::SplitPlanStorage* plan_at_flush{};
}
namespace ker::mod::mm::addr {
auto get_virt_pointer(vaddr_t p) -> vaddr_t* {
    if (host_table_address(p)) return reinterpret_cast<vaddr_t*>(p);
    for (auto const& b : backend().blocks)
        if (b.storage && p >= b.physical && p - b.physical < uint64_t{b.pages} * PAGE)
            return reinterpret_cast<vaddr_t*>(reinterpret_cast<uint64_t>(b.storage.get()) + p - b.physical);
    ADD_FAILURE() << "unexpected Host address endpoint " << p;
    return nullptr;
}
}
namespace ker::syscall::vmem {
class SharedVmemPublicationGuard {
   public:
    SharedVmemPublicationGuard() {
        EXPECT_EQ(cow_depth + pin_depth, 0U);
        ++publication_depth;
    }
    ~SharedVmemPublicationGuard() {
        EXPECT_EQ(cow_depth + pin_depth, 0U);
        --publication_depth;
    }
};
}
namespace ker::mod::mm::virt {
using addr::paddr_t;
using addr::vaddr_t;
using paging::PageTable;
using paging::PageTableEntry;
constexpr uint64_t PTE_FRAME_MASK = 0x000ffffffffff000ULL;
constexpr size_t USER_PAGEMAP_ACCESS_GATE_NONE = SIZE_MAX;
auto& managed_huge_registry = ::huge_core;
struct HostRootControl {
    uint64_t serial{};
};
auto find_user_root_control_locked(PageTable* root) -> HostRootControl* {
    static HostRootControl control;
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
    EXPECT_EQ(cow_depth + pin_depth, 0U);
    if (root != runtime_root) return false;
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
    EXPECT_GT(root_readers, 0U);
    huge::Snapshot s{};
    huge_core.snapshot(s);
    EXPECT_GT(s.charged_pages, 0U);
    if (span_at_flush) {
        EXPECT_TRUE(span_at_flush->empty());
        EXPECT_GT(s.mapped_pages, 0U);
    }
    if (plan_at_flush) {
        EXPECT_EQ(s.splitting_pages, uint64_t{plan_at_flush->descriptor_count} * huge::LEAF_PAGES);
        EXPECT_GE(s.table_pages, plan_at_flush->table_count);
        for (uint16_t i = 0; i < plan_at_flush->descriptor_count; ++i) EXPECT_EQ(plan_at_flush->tables[i], nullptr);
    }
    ++actual_flushes;
}
void owned_frame_track_fresh_normal_mapping(PageTable*, uint64_t, uint64_t, uint64_t) {
    EXPECT_GT(actual_flushes, 0U);
    ++tracked_pages;
}
struct HostBindingIndex {
    auto matches_pin_locked(PageTable*, uint64_t, uint64_t, file_mapping::FileMappingBinding* binding) -> bool {
        return binding_matches && binding == nullptr;
    }
} file_mapping_index;
// Actual PageTable/PageTableEntry and UserPagePin declarations are extracted.
