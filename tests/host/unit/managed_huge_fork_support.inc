// Only physical/ref/root/index/TLB/scheduler endpoints are modeled. The original
// exclusive class, actual survey/split, coordinator and full copier are extracted.
// These maintained pre-device vectors have no Object/grant/family provider.
// A prepared device hook is an explicit empty endpoint; nonempty use aborts.
namespace ker::mod::mm::device_vm {
class PreparedFork {
   public:
    auto prepare_current(sched::task::Task&, const sys::SyscallPreparationScope&) -> int { return 0; }
    auto matches_current(sched::task::Task&, const ker::syscall::vmem::SharedVmemPublicationGuard&) -> bool { return true; }
    auto empty() const noexcept -> bool { return true; }
    auto attach_before_copy(sched::task::Task&, sched::task::Task&, paging::PageTable*,
                            const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept -> int { std::abort(); }
};
}
namespace {
size_t fork_fail_table_at = SIZE_MAX, fork_table_attempts{};
unsigned fork_source_wait_samples{};
void before_fork_preempt() {
    if (host_range::expect_fork_wait && root_readers != 0 && host_range::on_range_preempt) host_range::on_range_preempt();
    if (!fork_mode) return;
    // New actual advisory sampling enters PreemptGuard before its short Task
    // lease. Keep old COW-entry checks unchanged whenever PUB is retained.
    if (!host_range::exclusive && publication_depth == 0 && host_range::interrupts) {
        EXPECT_EQ(root_readers + cow_depth + pin_depth + backend().preempt_depth, 0U);
        if (host_range::current) EXPECT_EQ(host_range::current->usercopy_pagemap_accesses.load(), 0U);
        ++fork_source_wait_samples;
        return;
    }
    huge::Snapshot snapshot{};
    huge_core.snapshot(snapshot);
    EXPECT_TRUE(host_range::exclusive);
    EXPECT_EQ(root_readers, 0U);
    EXPECT_EQ(cow_depth + pin_depth, 0U);
    EXPECT_EQ(snapshot.mapped_pages + snapshot.splitting_pages, 0U);
    ++fork_cow_entries;
    fork_events.push_back("COW");
}
}  // namespace
namespace ker::mod::mm::phys {
auto page_ref_try_inc(void* pointer, PageLookupHint*) -> bool {
    uint32_t page{};
    auto* block = backend().containing(reinterpret_cast<uint64_t>(pointer), page);
    if (block == nullptr || block->refs[page] == 0) return false;
    ++block->refs[page];
    return true;
}
auto page_ref_get(void* pointer, PageLookupHint*) -> uint32_t { return page_ref_get(pointer); }
void page_free(void* pointer) {
    auto found = std::find(table_live.begin(), table_live.end(), pointer);
    if (found != table_live.end()) {
        std::free(*found);
        table_live.erase(found);
    } else {
        (void)page_ref_dec(pointer);
    }
}
}  // namespace ker::mod::mm::phys
namespace ker::mod::mm::virt {
auto pte_raw(const PageTableEntry& entry) -> uint64_t { return std::bit_cast<uint64_t>(entry); }
auto pte_from_raw(uint64_t raw) -> PageTableEntry { return std::bit_cast<PageTableEntry>(raw); }
auto entry_at(PageTable* root, size_t index) -> PageTableEntry& { return root->entries[index]; }
auto alloc_zeroed_page_table() -> PageTable* {
    EXPECT_TRUE(host_range::exclusive);
    EXPECT_EQ(backend().preempt_depth, 1U);
    if (fork_table_attempts++ == fork_fail_table_at) return nullptr;
    auto* table = static_cast<PageTable*>(std::aligned_alloc(PAGE, PAGE));
    if (!table) std::abort();
    std::memset(table, 0, PAGE);
    table_live.push_back(table);
    return table;
}
auto clone_anonymous_swap_mappings(PageTable*, PageTable*) -> bool {
    EXPECT_TRUE(host_range::exclusive);
    EXPECT_EQ(backend().preempt_depth, 1U);
    ++fork_swap_clones;
    return true;
}
auto is_current_pagemap(PageTable* root) -> bool { return root == runtime_root; }
void selftest_require_mutable_pagemap_root(PageTable* root, const char*) {
    EXPECT_TRUE(root == runtime_root || (fork_mode && root == fork_child));
}
auto fork_eager_copy_enabled() -> bool { return fork_eager; }
void owned_frame_untrack_mapping(PageTable*, uint64_t, uint64_t) {}
auto is_watched_mmap_vaddr(uint64_t) -> bool { return false; }
struct log {
    template <class... Args>
    static void warn(const char*, Args...) {}
};
}  // namespace ker::mod::mm::virt

// Existing 54 positive vectors supply no new swap plan. Actual prepared swap
// bodies have separate ownership/refusal coverage; this endpoint must stay idle.
namespace ker::mod::mm::virt::detail {
struct PreparedSwapForkAccess {
    static auto preflight(PreparedAnonymousSwapFork&, PageTable*, PageTable*) -> int { std::abort(); }
    static auto commit(PreparedAnonymousSwapFork&, PageTable*, PageTable*) -> int { std::abort(); }
};
auto prepare_anonymous_swap_fork(sched::task::Task&, sched::task::Task&, const sys::SyscallPreparationScope&, PreparedAnonymousSwapFork&)
    -> int {
    std::abort();
}
void reset_anonymous_swap_fork(PreparedAnonymousSwapFork&) noexcept { std::abort(); }
}  // namespace ker::mod::mm::virt::detail
