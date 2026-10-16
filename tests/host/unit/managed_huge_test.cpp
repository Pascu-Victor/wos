#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <platform/acpi/numa_boot.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/managed_huge.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/reclaim.hpp>
#include <platform/sched/scheduler.hpp>
#include <string>
#include <utility>
#include <vector>

// Compile the real numa_alloc.cpp and its real phys.hpp declarations. Only the
// Host physical/topology/address/ref functions below are seams. Their ledger
// checks factory dispatch and ownership, not native buddy/reclaim/TLB behavior.
namespace {
namespace mm = ker::mod::mm;
namespace numa = mm::numa;
namespace topology = ker::mod::acpi::numa;
constexpr uint64_t PAGE = mm::paging::PAGE_SIZE;
constexpr uint64_t LARGE = 512 * PAGE;
void check_huge_capacity_before_free();
void before_fork_preempt();
// Explicit Host table/root address endpoints for the actual private resolver.
mm::paging::PageTable* runtime_root{};
uint64_t runtime_serial{1};
bool host_table_address(uint64_t);
bool huge_check_free{};
constexpr auto OWNER = mm::PhysicalPageOwner::USER_PRIVATE_MAPPING;
using Initialization = mm::phys::PageInitialization;
using Reclaim = numa::ReclaimMode;

enum class Dispatch { ZERO, OVERWRITE, RECLAIM_ZERO, RECLAIM_OVERWRITE, RANGE };
struct Call {
    Dispatch dispatch{};
    mm::PhysicalPageOwner owner{};
    uint64_t first{};
    uint64_t end{};
    uint64_t alignment{};
    uint32_t pages{};
    uint32_t retries{};
    Initialization initialization{};
    std::string name{};
};
struct FreeStorage {
    void operator()(void* pointer) const { std::free(pointer); }
};
struct Block {
    std::unique_ptr<void, FreeStorage> storage{};
    uint64_t physical{};
    uint32_t pages{};
    std::vector<uint32_t> refs{};
};
struct Backend {
    topology::Topology topology{};
    bool published{true};
    bool fail_all{};
    bool return_unmanaged_uma{};
    std::vector<uint64_t> failed_ranges{};
    std::vector<Call> calls{};
    std::vector<Block> blocks{};
    std::vector<void*> decremented{};

    // CPU/preemption primitives are Host seams; the production builder and
    // authoritative inline scheduler::PreemptGuard are compiled unchanged.
    uint64_t current_cpu{};
    uint32_t preempt_depth{};
    uint32_t expected_cpu_depth{1};
    uint32_t preempt_disables{};
    uint32_t preempt_enables{};
    uint32_t cpu_reads{};
    const numa::AllocationContext* output_during_guard{};
    numa::AllocationContext output_before_guard{};
    numa::Policy* caller_policy_to_change{};
    bool migrate_after_guard{};
    uint64_t next_cpu{};

    void observe_context(numa::AllocationContext& output, uint32_t incoming_depth = 0) {
        preempt_depth = incoming_depth;
        expected_cpu_depth = incoming_depth + 1;
        preempt_disables = preempt_enables = cpu_reads = 0;
        output_during_guard = &output;
        output_before_guard = output;
        caller_policy_to_change = nullptr;
        migrate_after_guard = false;
    }

    void domains(uint16_t nodes) {
        topology = {};
        topology.node_count = topology.cpu_count = nodes;
        for (uint16_t node = 0; node < nodes; ++node) {
            topology.cpu_nodes[node] = node;
            topology.nodes[node].cpus.set(node);
            for (uint16_t other = 0; other < nodes; ++other) topology.distances[node][other] = node == other ? 10 : 20;
        }
        topology.memory_range_count = nodes;
        for (uint16_t node = 0; node < nodes; ++node) {
            topology.memory[node] = {0x10000000ULL + node * 0x10000000ULL, 64 * LARGE, node, 0};
            topology.nodes[node].usable_bytes = 64 * LARGE;
        }
    }
    auto allocate(uint64_t physical, uint32_t pages, uint64_t alignment, Initialization initialization) -> void* {
        if (fail_all || std::find(failed_ranges.begin(), failed_ranges.end(), physical) != failed_ranges.end()) return nullptr;
        uint64_t const BYTES = static_cast<uint64_t>(pages) * PAGE;
        uint64_t const STORAGE_BYTES = (BYTES + alignment - 1) & ~(alignment - 1);
        void* storage = std::aligned_alloc(alignment, STORAGE_BYTES);
        if (storage == nullptr) std::abort();
        std::memset(storage, initialization == Initialization::ZERO ? 0 : 0xa5, STORAGE_BYTES);
        blocks.push_back({std::unique_ptr<void, FreeStorage>{storage}, physical, pages, std::vector<uint32_t>(pages, 1)});
        return storage;
    }
    auto plain(Dispatch dispatch, mm::PhysicalPageOwner owner, uint64_t bytes, std::string_view name, uint32_t retries,
               Initialization initialization) -> void* {
        EXPECT_EQ(bytes, PAGE);
        calls.push_back({dispatch, owner, 0, 0, PAGE, 1, retries, initialization, std::string{name}});
        return allocate(return_unmanaged_uma ? 0x70000000ULL : topology.memory[0].base, 1, PAGE, initialization);
    }
    auto range(mm::PhysicalPageOwner owner, uint64_t first, uint64_t end, uint32_t pages, uint64_t alignment, Initialization initialization)
        -> void* {
        calls.push_back({Dispatch::RANGE, owner, first, end, alignment, pages, 0, initialization, {}});
        EXPECT_GE(end - first, static_cast<uint64_t>(pages) * PAGE);
        EXPECT_EQ(first % alignment, 0U);
        return allocate(first, pages, alignment, initialization);
    }
    auto containing(uint64_t pointer, uint32_t& page) -> Block* {
        for (auto& block : blocks) {
            if (block.storage == nullptr) continue;
            auto const FIRST = reinterpret_cast<uint64_t>(block.storage.get());
            if (pointer < FIRST || pointer - FIRST >= static_cast<uint64_t>(block.pages) * PAGE) continue;
            page = static_cast<uint32_t>((pointer - FIRST) / PAGE);
            return &block;
        }
        return nullptr;
    }
    auto live_refs() const -> uint64_t {
        uint64_t result = 0;
        for (auto const& block : blocks)
            for (auto ref : block.refs) result += ref;
        return result;
    }
};
Backend* active_backend{};
auto backend() -> Backend& {
    if (active_backend == nullptr) std::abort();
    return *active_backend;
}

class NumaAllocation : public ::testing::Test {
   protected:
    Backend model{};
    void SetUp() override {
        ASSERT_EQ(active_backend, nullptr);
        active_backend = &model;
        model.domains(1);
    }
    void TearDown() override {
        EXPECT_EQ(model.live_refs(), 0U);
        EXPECT_EQ(model.preempt_depth, 0U);
        EXPECT_EQ(model.preempt_disables, model.preempt_enables);
        active_backend = nullptr;
    }
    static auto context(numa::Mode mode = numa::Mode::LOCAL, numa::NodeMask mask = 0, numa::NodeId local = 0, uint64_t index = 0)
        -> numa::AllocationContext {
        return {{mask, mode}, local, index};
    }
    static void expect_empty(const numa::AllocationResult& result, numa::AllocationError error) {
        EXPECT_TRUE(result.span.empty());
        EXPECT_EQ(result.span.data(), nullptr);
        EXPECT_EQ(result.span.pages(), 0U);
        EXPECT_EQ(result.error, error);
        EXPECT_EQ(result.actual, numa::INVALID_NODE);
        EXPECT_FALSE(result.used_fallback());
    }
};
}  // namespace

namespace ker::mod::acpi::numa {
auto boot_topology() noexcept -> const Topology* { return backend().published ? &backend().topology : nullptr; }
}  // namespace ker::mod::acpi::numa

namespace ker::mod::cpu {
auto current_cpu() -> uint64_t {
    auto& state = backend();
    ++state.cpu_reads;
    EXPECT_EQ(state.preempt_depth, state.expected_cpu_depth);
    if (state.caller_policy_to_change != nullptr) *state.caller_policy_to_change = numa::Policy{};
    return state.current_cpu;
}
}  // namespace ker::mod::cpu

namespace ker::mod::sched {
void preempt_disable() {
    before_fork_preempt();
    auto& state = backend();
    ++state.preempt_disables;
    ++state.preempt_depth;
}
void preempt_enable() {
    auto& state = backend();
    EXPECT_GT(state.preempt_depth, 0U);
    if (state.output_during_guard != nullptr) {
        EXPECT_EQ(state.output_during_guard->policy, state.output_before_guard.policy);
        EXPECT_EQ(state.output_during_guard->local_node, state.output_before_guard.local_node);
        EXPECT_EQ(state.output_during_guard->virtual_page_index, state.output_before_guard.virtual_page_index);
    }
    --state.preempt_depth;
    ++state.preempt_enables;
    if (state.migrate_after_guard) state.current_cpu = state.next_cpu;
}
}  // namespace ker::mod::sched

namespace ker::mod::mm::addr {
auto get_phys_pointer(paddr_t pointer) -> paddr_t* {
    uint32_t page = 0;
    auto* block = backend().containing(pointer, page);
    if (block == nullptr && host_table_address(pointer)) return reinterpret_cast<paddr_t*>(pointer);
    if (block == nullptr) {
        ADD_FAILURE() << "address conversion lacks an owned Host allocation";
        return nullptr;
    }
    auto const OFFSET = pointer - reinterpret_cast<uint64_t>(block->storage.get());
    return reinterpret_cast<paddr_t*>(block->physical + OFFSET);
}
}  // namespace ker::mod::mm::addr

// C25 only allocates512-page data spans. A notification here would be an
// actual factory dispatch regression, not evidence of any reclaim progress.
namespace ker::mod::mm::reclaim {
auto notify_order0_node_failure(uint16_t) noexcept -> NodeReclaimStatus {
    ADD_FAILURE() << "managed-huge factory unexpectedly notified order0 pressure";
    return NodeReclaimStatus::NONE;
}
}  // namespace ker::mod::mm::reclaim

namespace ker::mod::mm::phys {
auto page_alloc_may_fail(PhysicalPageOwner owner, uint64_t bytes, std::string_view name) -> void* {
    return backend().plain(Dispatch::ZERO, owner, bytes, name, 0, PageInitialization::ZERO);
}
auto page_alloc_full_overwrite_page_may_fail(PhysicalPageOwner owner, std::string_view name) -> void* {
    return backend().plain(Dispatch::OVERWRITE, owner, paging::PAGE_SIZE, name, 0, PageInitialization::FULL_OVERWRITE);
}
auto page_alloc_with_reclaim_may_fail(PhysicalPageOwner owner, uint64_t bytes, std::string_view name, uint32_t retries) -> void* {
    return backend().plain(Dispatch::RECLAIM_ZERO, owner, bytes, name, retries, PageInitialization::ZERO);
}
auto page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner owner, std::string_view name, uint32_t retries) -> void* {
    return backend().plain(Dispatch::RECLAIM_OVERWRITE, owner, paging::PAGE_SIZE, name, retries, PageInitialization::FULL_OVERWRITE);
}
auto page_alloc_order0_span_in_range_may_fail(PhysicalPageOwner owner, uint64_t first, uint64_t end, uint32_t pages, uint64_t alignment,
                                              PageInitialization initialization) -> void* {
    return backend().range(owner, first, end, pages, alignment, initialization);
}
auto page_ref_dec(void* pointer) -> uint32_t {
    uint32_t page = 0;
    auto* block = backend().containing(reinterpret_cast<uint64_t>(pointer), page);
    if (block == nullptr || block->refs[page] == 0 || reinterpret_cast<uint64_t>(pointer) % paging::PAGE_SIZE != 0) {
        ADD_FAILURE() << "invalid or duplicate Host page_ref_dec";
        return 0;
    }
    if (huge_check_free) check_huge_capacity_before_free();
    backend().decremented.push_back(pointer);
    uint32_t const LEFT = --block->refs[page];
    if (std::all_of(block->refs.begin(), block->refs.end(), [](uint32_t ref) { return ref == 0; })) block->storage.reset();
    return LEFT;
}
}  // namespace ker::mod::mm::phys

namespace {
namespace huge = mm::huge;
huge::RegistryCore huge_core{};
constexpr huge::RootKey ROOT{0xffff800010000000ULL, 1};
bool huge_root_live{true}, fail_plan_heap{}, invalid_kind{}, heap_without_reservation{};
size_t table_attempt{}, fail_table_at{SIZE_MAX}, table_freed{};
std::vector<mm::paging::PageTable*> table_live;
bool host_table_address(uint64_t address) {
    if (runtime_root != nullptr && address == reinterpret_cast<uint64_t>(runtime_root)) return true;
    return std::any_of(table_live.begin(), table_live.end(), [=](auto* table) { return address == reinterpret_cast<uint64_t>(table); });
}
void check_huge_capacity_before_free() {
    huge::Snapshot snapshot{};
    huge_core.snapshot(snapshot);
    EXPECT_GE(snapshot.charged_pages, huge::LEAF_PAGES);
}
auto caps(uint64_t pages = 1ULL << 22, uint16_t nodes = 2) -> huge::CapsInput {
    huge::CapsInput in{};
    in.node_count = nodes;
    for (uint16_t i = 0; i < nodes; ++i) in.usable[i] = pages;
    return in;
}
auto raw(const huge::LeafKey& key) -> uint64_t { return key.pa_2m | 0x87 | (uint64_t{1} << 63); }
}  // namespace
// Test-only heap failure at the genuine nothrow new expression; ordinary new
// still provides ASan's allocation/deallocation tracking on successful calls.
void* operator new(std::size_t bytes, const std::nothrow_t&) noexcept {
    if (bytes == sizeof(huge::SplitPlanStorage)) {
        huge::Snapshot s{};
        huge_core.snapshot(s);
        if (s.plans == 0) heap_without_reservation = true;
    }
    if (fail_plan_heap && bytes == sizeof(huge::SplitPlanStorage)) {
        fail_plan_heap = false;
        return nullptr;
    }
    return ::operator new(bytes);
}
namespace ker::mod::mm::huge {
struct RegistryCoreTestAccess {
    static void exhaust_next(RegistryCore& c) { c.issuer_ = UINT64_MAX - 1; }
};
namespace detail {
auto root_key(paging::PageTable* root, RootKey& out) noexcept -> Error {
    if (out.address_bits || out.incarnation) return Error::Invalid;
    if (!huge_root_live) return Error::Stale;
    if (root == runtime_root && runtime_root != nullptr) {
        out = {reinterpret_cast<uint64_t>(root), runtime_serial};
        return Error::None;
    }
    if (reinterpret_cast<uint64_t>(root) != ROOT.address_bits) return Error::Stale;
    out = ROOT;
    return Error::None;
}
auto root_matches(RootKey root) noexcept -> bool {
    return huge_root_live &&
           (RegistryCore::same_root(root, ROOT) ||
            (runtime_root != nullptr && RegistryCore::same_root(root, {reinterpret_cast<uint64_t>(runtime_root), runtime_serial})));
}
auto initialize_caps(const CapsInput& in) noexcept -> Error { return huge_core.initialize(in); }
auto reserve_leaf(RootKey root, uint64_t va, uint16_t node, LeafHandle& out) noexcept -> Error {
    if (!root_matches(root)) return Error::Stale;
    return huge_core.reserve_leaf(root, va, node, out);
}
auto cancel_leaf(LeafHandle h) noexcept -> Error { return huge_core.cancel_preparing(h); }
auto reserve_plan(uint16_t n, PlanHandle& h) noexcept -> Error { return huge_core.reserve_table_plan(n, h); }
auto release_plan(PlanHandle h) noexcept -> Error { return huge_core.release_table_plan(h); }
void snapshot(Snapshot& out) noexcept { huge_core.snapshot(out); }
auto allocate_table() noexcept -> paging::PageTable* {
    huge::Snapshot s{};
    huge_core.snapshot(s);
    EXPECT_GT(s.table_pages, 0U);
    EXPECT_GT(s.plans, 0U);
    if (table_attempt++ == fail_table_at) return nullptr;
    auto* p = static_cast<paging::PageTable*>(std::aligned_alloc(PAGE, PAGE));
    if (p != nullptr) {
        std::memset(p, 0, PAGE);
        table_live.push_back(p);
    }
    return p;
}
void release_table(paging::PageTable* p) noexcept {
    if (p == nullptr) return;
    huge::Snapshot s{};
    huge_core.snapshot(s);
    EXPECT_GT(s.table_pages, 0U);
    EXPECT_GT(s.plans, 0U);
    auto it = std::find(table_live.begin(), table_live.end(), p);
    ASSERT_NE(it, table_live.end());
    table_live.erase(it);
    ++table_freed;
    std::free(p);
}
}  // namespace detail
}  // namespace ker::mod::mm::huge
namespace ker::mod::mm::phys {
auto page_kind_get(void* p) -> PageKind {
    return host_table_address(reinterpret_cast<uint64_t>(p)) || invalid_kind ? PageKind::PAGE_TABLE : PageKind::NORMAL;
}
auto page_ref_get(void* p) -> uint32_t {
    uint32_t page{};
    auto* b = backend().containing(reinterpret_cast<uint64_t>(p), page);
    return b ? b->refs[page] : host_table_address(reinterpret_cast<uint64_t>(p)) ? 1 : 0;
}
auto node_counters_ready() noexcept -> bool { return backend().published; }
auto snapshot_node_counters(NodeCounterSnapshot& out, NodeCounterWorkspace&) noexcept -> NodeCounterError {
    out = {};
    out.complete = true;
    out.node_count = backend().topology.node_count;
    for (uint16_t i = 0; i < out.node_count; ++i) {
        out.nodes[i].usable = backend().topology.nodes[i].usable_bytes / PAGE;
        out.global.usable += out.nodes[i].usable;
    }
    return NodeCounterError::NONE;
}
}  // namespace ker::mod::mm::phys
namespace {
TEST(HugeCore, DefaultHasNoAuthorityAndInvalidInitializationIsWhole) {
    huge::RegistryCore c{};
    huge::Snapshot s{};
    c.snapshot(s);
    EXPECT_FALSE(s.ready);
    EXPECT_EQ(s.charged_pages, 0U);
    EXPECT_FALSE(c.root_has_published_owners(ROOT));
    huge::LeafHandle h{};
    EXPECT_EQ(c.reserve_leaf(ROOT, LARGE, 0, h), huge::Error::NotReady);
    auto old = c;
    auto bad = caps();
    bad.node_count = 65;
    EXPECT_EQ(c.initialize(bad), huge::Error::Invalid);
    EXPECT_EQ(std::memcmp(&old, &c, sizeof(c)), 0);
    bad = caps();
    bad.usable[0] = UINT64_MAX;
    bad.usable[1] = 1;
    EXPECT_EQ(c.initialize(bad), huge::Error::Invalid);
    EXPECT_EQ(std::memcmp(&old, &c, sizeof(c)), 0);
    bad = caps();
    bad.node_count = 1;
    EXPECT_EQ(c.initialize(bad), huge::Error::Invalid);
    EXPECT_EQ(std::memcmp(&old, &c, sizeof(c)), 0);
    EXPECT_EQ(c.initialize(caps()), huge::Error::None);
    EXPECT_EQ(c.initialize(caps()), huge::Error::Conflict);
}
TEST(HugeCore, FractionRoundingMemorylessAndHighNode) {
    huge::RegistryCore c{};
    auto input = caps(0, 64);
    input.usable[63] = 16 * 512 + 8191;
    ASSERT_EQ(c.initialize(input), huge::Error::None);
    huge::Snapshot s{};
    c.snapshot(s);
    EXPECT_EQ(s.caps.node_pages[63], 512U);
    EXPECT_EQ(s.caps.node_pages[0], 0U);
    EXPECT_EQ(s.caps.global_pages, 512U);
    huge::LeafHandle h{};
    EXPECT_EQ(c.reserve_leaf(ROOT, LARGE, 0, h), huge::Error::Capacity);
    ASSERT_EQ(c.reserve_leaf(ROOT, LARGE, 63, h), huge::Error::None);
    EXPECT_EQ(c.cancel_preparing(h), huge::Error::None);
}
TEST(HugeCore, RootGlobalAndNodeCapsIncludePreparing) {
    huge::RegistryCore c{};
    ASSERT_EQ(c.initialize(caps()), huge::Error::None);
    std::array<huge::LeafHandle, 128> hs{};
    for (size_t i = 0; i < 64; ++i) ASSERT_EQ(c.reserve_leaf(ROOT, (i + 1) * LARGE, 0, hs[i]), huge::Error::None);
    huge::LeafHandle extra{77, 0};
    auto before = extra;
    EXPECT_EQ(c.reserve_leaf(ROOT, 100 * LARGE, 0, extra), huge::Error::Capacity);
    EXPECT_EQ(extra.slot, before.slot);
    EXPECT_EQ(extra.generation, 0U);
    huge::RootKey other{ROOT.address_bits + PAGE, 2};
    for (size_t i = 64; i < 128; ++i) ASSERT_EQ(c.reserve_leaf(other, (i - 63) * LARGE, 1, hs[i]), huge::Error::None);
    EXPECT_EQ(c.reserve_leaf({other.address_bits + PAGE, 3}, LARGE, 1, extra), huge::Error::Capacity);
    for (auto h : hs) EXPECT_EQ(c.cancel_preparing(h), huge::Error::None);
    huge::Snapshot s{};
    c.snapshot(s);
    EXPECT_EQ(s.charged_pages, 0U);
}
TEST(HugeCore, StaleHandlesRootsAndMalformedInputNeverReleaseAnotherOwner) {
    huge::RegistryCore c{};
    ASSERT_EQ(c.initialize(caps()), huge::Error::None);
    huge::LeafHandle a{};
    ASSERT_EQ(c.reserve_leaf(ROOT, LARGE, 0, a), huge::Error::None);
    huge::LeafHandle b{};
    EXPECT_EQ(c.reserve_leaf(ROOT, LARGE, 1, b), huge::Error::Conflict);
    EXPECT_FALSE(c.root_has_published_owners(ROOT));
    EXPECT_EQ(c.cancel_preparing(a), huge::Error::None);
    ASSERT_EQ(c.reserve_leaf(ROOT, LARGE, 0, b), huge::Error::None);
    EXPECT_NE(a.generation, b.generation);
    EXPECT_EQ(c.cancel_preparing(a), huge::Error::Stale);
    EXPECT_EQ(c.publish_mapped(b, {{ROOT.address_bits, 2}, LARGE, LARGE}, 0x87 | LARGE), huge::Error::Invalid);
    EXPECT_EQ(c.cancel_preparing(b), huge::Error::None);
    for (auto va : {uint64_t{0}, LARGE + PAGE, uint64_t{1} << 47}) {
        a = {};
        EXPECT_EQ(c.reserve_leaf(ROOT, va, 0, a), huge::Error::Invalid);
    }
}
TEST(HugeCore, MappedSplitAndRetiredChargeEndsOnlyAtExplicitCompletion) {
    huge::RegistryCore c{};
    ASSERT_EQ(c.initialize(caps()), huge::Error::None);
    huge::LeafHandle h{};
    huge::LeafKey key{ROOT, LARGE, 2 * LARGE};
    ASSERT_EQ(c.reserve_leaf(ROOT, LARGE, 0, h), huge::Error::None);
    ASSERT_EQ(c.publish_mapped(h, key, raw(key)), huge::Error::None);
    EXPECT_TRUE(c.root_has_published_owners(ROOT));
    EXPECT_TRUE(c.match_mapped(h, key, raw(key) | 0x60));
    EXPECT_FALSE(c.match_mapped(h, key, raw(key) ^ 2));
    EXPECT_EQ(c.cancel_preparing(h), huge::Error::Stale);
    ASSERT_EQ(c.begin_split(h, key), huge::Error::None);
    huge::Snapshot s{};
    c.snapshot(s);
    EXPECT_EQ(s.splitting_pages, 512U);
    ASSERT_EQ(c.complete_split_after_ack(h), huge::Error::None);
    EXPECT_EQ(c.complete_split_after_ack(h), huge::Error::Stale);
    h = {};
    ASSERT_EQ(c.reserve_leaf(ROOT, LARGE, 0, h), huge::Error::None);
    ASSERT_EQ(c.publish_mapped(h, key, raw(key)), huge::Error::None);
    ASSERT_EQ(c.detach_mapped(h, key), huge::Error::None);
    EXPECT_EQ(c.complete_retired_after_ack(h), huge::Error::Conflict);
    EXPECT_EQ(c.advance_retired_handoff(h, 1, 64), huge::Error::Invalid);
    for (uint16_t i = 0; i < 512; i += 64) ASSERT_EQ(c.advance_retired_handoff(h, i, 64), huge::Error::None);
    EXPECT_EQ(c.advance_retired_handoff(h, 512, 1), huge::Error::Invalid);
    c.snapshot(s);
    EXPECT_EQ(s.retired_pages, 512U);
    ASSERT_EQ(c.complete_retired_after_ack(h), huge::Error::None);
    c.snapshot(s);
    EXPECT_EQ(s.charged_pages, 0U);
}
TEST(HugeCore, PlanCapacityIsReservedInFullAndGenerationsDoNotWrap) {
    huge::RegistryCore c{};
    ASSERT_EQ(c.initialize(caps()), huge::Error::None);
    huge::PlanHandle a{}, b{}, extra{};
    EXPECT_EQ(c.reserve_table_plan(0, a), huge::Error::Invalid);
    EXPECT_EQ(c.reserve_table_plan(65, a), huge::Error::Invalid);
    ASSERT_EQ(c.reserve_table_plan(64, a), huge::Error::None);
    ASSERT_EQ(c.reserve_table_plan(64, b), huge::Error::None);
    EXPECT_EQ(c.reserve_table_plan(1, extra), huge::Error::Capacity);
    EXPECT_EQ(c.release_table_plan(a), huge::Error::None);
    EXPECT_EQ(c.release_table_plan(a), huge::Error::Stale);
    EXPECT_EQ(c.release_table_plan(b), huge::Error::None);
    huge::RegistryCoreTestAccess::exhaust_next(c);
    a = {};
    ASSERT_EQ(c.reserve_table_plan(1, a), huge::Error::None);
    EXPECT_EQ(a.generation, UINT64_MAX);
    huge::LeafHandle h{};
    EXPECT_EQ(c.reserve_leaf(ROOT, LARGE, 0, h), huge::Error::GenerationExhausted);
    EXPECT_EQ(c.release_table_plan(a), huge::Error::None);
}
TEST(HugeCore, TenThousandOwnedStateCyclesCloseWithoutRetainedCharge) {
    huge::RegistryCore c{};
    ASSERT_EQ(c.initialize(caps()), huge::Error::None);
    for (size_t i = 0; i < 10000; ++i) {
        huge::LeafHandle h{};
        huge::PlanHandle p{};
        ASSERT_EQ(c.reserve_leaf(ROOT, LARGE, 0, h), huge::Error::None);
        ASSERT_EQ(c.reserve_table_plan(1, p), huge::Error::None);
        ASSERT_EQ(c.cancel_preparing(h), huge::Error::None);
        ASSERT_EQ(c.release_table_plan(p), huge::Error::None);
    }
    huge::Snapshot s{};
    c.snapshot(s);
    EXPECT_EQ(s.charged_pages, 0U);
    EXPECT_EQ(s.table_pages, 0U);
    EXPECT_EQ(s.plans, 0U);
}
class HugeOwnership : public NumaAllocation {
   protected:
    void SetUp() override {
        NumaAllocation::SetUp();
        model.domains(2);
        huge_core = {};
        heap_without_reservation = false;
        huge_root_live = true;
        huge_check_free = true;
        invalid_kind = false;
        fail_plan_heap = false;
        table_attempt = table_freed = 0;
        fail_table_at = SIZE_MAX;
        table_live.reserve(128);
        ASSERT_EQ(huge::initialize_after_topology(), huge::Error::None);
    }
    void TearDown() override {
        huge::Snapshot s{};
        huge::snapshot(s);
        EXPECT_EQ(s.charged_pages, 0U);
        EXPECT_EQ(s.table_pages, 0U);
        EXPECT_EQ(s.plans, 0U);
        EXPECT_TRUE(table_live.empty());
        EXPECT_FALSE(heap_without_reservation);
        huge_check_free = false;
        NumaAllocation::TearDown();
    }
};
TEST_F(HugeOwnership, RealFactoryZeroFrameOwnershipMoveAndResetBeforeCredit) {
    huge::PreparedSpan a;
    ASSERT_EQ(a.prepare(ROOT, LARGE, context(numa::Mode::BIND, 1)), huge::Error::None);
    ASSERT_NE(a.data(), nullptr);
    EXPECT_EQ(model.live_refs(), 512U);
    auto* bytes = static_cast<uint8_t*>(a.data());
    for (size_t i = 0; i < LARGE; ++i) ASSERT_EQ(bytes[i], 0);
    huge::PreparedSpan b(std::move(a));
    EXPECT_TRUE(a.empty());
    EXPECT_FALSE(b.empty());
    b = std::move(b);
    b.reset();
    b.reset();
    EXPECT_EQ(model.live_refs(), 0U);
}
TEST_F(HugeOwnership, PreferredFailureFallsBackOnlyAfterActualChargeAndStrictDoesNotEscape) {
    model.failed_ranges = {model.topology.memory[0].base};
    huge::PreparedSpan p;
    EXPECT_EQ(p.prepare(ROOT, LARGE, context(numa::Mode::BIND, 1)), huge::Error::Exhausted);
    EXPECT_TRUE(p.empty());
    ASSERT_EQ(p.prepare(ROOT, LARGE, context(numa::Mode::PREFERRED, 1)), huge::Error::None);
    EXPECT_EQ(p.requested(), 0);
    EXPECT_EQ(p.actual(), 1);
}
TEST_F(HugeOwnership, InvalidSpanAndStaleRootFailureReleaseActualOwners) {
    huge::PreparedSpan p;
    huge_root_live = false;
    EXPECT_EQ(p.prepare(ROOT, LARGE, context()), huge::Error::Stale);
    EXPECT_TRUE(model.calls.empty());
    huge_root_live = true;
    invalid_kind = true;
    EXPECT_EQ(p.prepare(ROOT, LARGE, context(numa::Mode::BIND, 1)), huge::Error::Invalid);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_TRUE(p.empty());
}
TEST_F(HugeOwnership, TableFailureEveryPrefixAndHeapFailureReturnBeforeCapacityCredits) {
    for (size_t fail : {size_t{0}, size_t{1}, size_t{2}, size_t{3}, size_t{31}, size_t{63}}) {
        table_attempt = 0;
        table_freed = 0;
        fail_table_at = fail;
        huge::PreparedTables p;
        EXPECT_EQ(p.prepare(64), huge::Error::Exhausted);
        EXPECT_TRUE(p.empty());
        EXPECT_EQ(table_freed, fail);
        EXPECT_TRUE(table_live.empty());
    }
    huge::PreparedTables p;
    fail_plan_heap = true;
    EXPECT_EQ(p.prepare(4), huge::Error::Exhausted);
    EXPECT_TRUE(p.empty());
    EXPECT_FALSE(fail_plan_heap);
}
TEST_F(HugeOwnership, PreparedTablesMovesKeepGlobal128LimitAndPrivateZeroedTables) {
    huge::PreparedTables a, b, c;
    ASSERT_EQ(a.prepare(64), huge::Error::None);
    ASSERT_EQ(b.prepare(64), huge::Error::None);
    EXPECT_EQ(c.prepare(1), huge::Error::Capacity);
    EXPECT_EQ(a.count(), 64);
    EXPECT_EQ(a.table(64), nullptr);
    for (uint16_t i = 0; i < 64; ++i) {
        auto* bytes = reinterpret_cast<uint8_t*>(a.table(i));
        for (size_t j = 0; j < PAGE; ++j) ASSERT_EQ(bytes[j], 0);
    }
    c = std::move(a);
    EXPECT_TRUE(a.empty());
    EXPECT_EQ(c.count(), 64);
    c.reset();
    ASSERT_EQ(a.prepare(1), huge::Error::None);
}
TEST(HugeCore, ReusedRootAddressNewSerialCannotConsumeOldPendingOrPublishedRows) {
    huge::RegistryCore c{};
    ASSERT_EQ(c.initialize(caps()), huge::Error::None);
    huge::LeafHandle old{}, current{};
    huge::RootKey newer{ROOT.address_bits, ROOT.incarnation + 1};
    ASSERT_EQ(c.reserve_leaf(ROOT, LARGE, 0, old), huge::Error::None);
    ASSERT_EQ(c.reserve_leaf(newer, LARGE, 0, current), huge::Error::None);
    huge::LeafKey key{newer, LARGE, LARGE};
    ASSERT_EQ(c.publish_mapped(current, key, raw(key)), huge::Error::None);
    EXPECT_FALSE(c.root_has_published_owners(ROOT));
    EXPECT_TRUE(c.root_has_published_owners(newer));
    EXPECT_EQ(c.cancel_preparing(old), huge::Error::None);
    EXPECT_TRUE(c.match_mapped(current, key, raw(key)));
    EXPECT_EQ(c.detach_mapped(old, key), huge::Error::Stale);
    ASSERT_EQ(c.begin_split(current, key), huge::Error::None);
    EXPECT_EQ(c.complete_split_after_ack(current), huge::Error::None);
}
TEST(HugeCore, UnsupportedWordsAndBadPhysicalGeometryLeavePendingOwnerUnchanged) {
    huge::RegistryCore c{};
    ASSERT_EQ(c.initialize(caps()), huge::Error::None);
    huge::LeafHandle h{};
    ASSERT_EQ(c.reserve_leaf(ROOT, LARGE, 0, h), huge::Error::None);
    huge::LeafKey key{ROOT, LARGE, LARGE};
    auto before = c;
    for (uint64_t bad :
         {raw(key) | (1ULL << 12), raw(key) | (1ULL << 8), raw(key) | (1ULL << 9), raw(key) & ~1ULL, raw(key) & ~(1ULL << 7)}) {
        EXPECT_EQ(c.publish_mapped(h, key, bad), huge::Error::Invalid);
        EXPECT_EQ(std::memcmp(&before, &c, sizeof(c)), 0);
    }
    auto unaligned = key;
    unaligned.pa_2m += PAGE;
    EXPECT_EQ(c.publish_mapped(h, unaligned, raw(unaligned)), huge::Error::Invalid);
    EXPECT_EQ(std::memcmp(&before, &c, sizeof(c)), 0);
    EXPECT_EQ(c.cancel_preparing(h), huge::Error::None);
}

}  // namespace
