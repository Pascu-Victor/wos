#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <platform/acpi/numa_boot.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/phys.hpp>
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
            topology.memory[node] = {0x10000000ULL + node * 0x10000000ULL, 2 * LARGE, node, 0};
            topology.nodes[node].usable_bytes = 2 * LARGE;
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
    if (block == nullptr) {
        ADD_FAILURE() << "address conversion lacks an owned Host allocation";
        return nullptr;
    }
    auto const OFFSET = pointer - reinterpret_cast<uint64_t>(block->storage.get());
    return reinterpret_cast<paddr_t*>(block->physical + OFFSET);
}
}  // namespace ker::mod::mm::addr

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
    backend().decremented.push_back(pointer);
    uint32_t const LEFT = --block->refs[page];
    if (std::all_of(block->refs.begin(), block->refs.end(), [](uint32_t ref) { return ref == 0; })) block->storage.reset();
    return LEFT;
}
}  // namespace ker::mod::mm::phys

namespace {
TEST_F(NumaAllocation, InvalidArgumentsRefuseBeforeTopologyAndAnyAllocation) {
    model.published = false;
    for (uint32_t pages : {0U, 2U, 511U, 513U})
        expect_empty(numa::try_allocate_pages(context(), pages, PAGE, OWNER), numa::AllocationError::INVALID_ARGUMENT);
    for (uint64_t alignment : {uint64_t{0}, PAGE / 2, 3 * PAGE})
        expect_empty(numa::try_allocate_pages(context(), 1, alignment, OWNER), numa::AllocationError::INVALID_ARGUMENT);
    for (auto owner : {mm::PhysicalPageOwner::INVALID, mm::PhysicalPageOwner::COUNT})
        expect_empty(numa::try_allocate_pages(context(), 1, PAGE, owner), numa::AllocationError::INVALID_ARGUMENT);
    expect_empty(numa::try_allocate_pages(context(), 1, PAGE, OWNER, static_cast<Initialization>(255)),
                 numa::AllocationError::INVALID_ARGUMENT);
    expect_empty(numa::try_allocate_pages(context(), 1, PAGE, OWNER, Initialization::ZERO, static_cast<Reclaim>(255)),
                 numa::AllocationError::INVALID_ARGUMENT);
    expect_empty(numa::try_allocate_pages(context(), 1, PAGE, OWNER), numa::AllocationError::NO_TOPOLOGY);
    EXPECT_TRUE(model.calls.empty());
}

TEST_F(NumaAllocation, InvalidPolicyAndLocalNodeNeverReachBackend) {
    model.domains(3);
    for (auto request : {context(numa::Mode::LOCAL, 1), context(numa::Mode::BIND, 0), context(numa::Mode::BIND, 8),
                         context(numa::Mode::PREFERRED, 3), context(static_cast<numa::Mode>(255))})
        expect_empty(numa::try_allocate_pages(request, 1, PAGE, OWNER), numa::AllocationError::INVALID_POLICY);
    expect_empty(numa::try_allocate_pages(context(numa::Mode::LOCAL, 0, numa::INVALID_NODE), 1, PAGE, OWNER),
                 numa::AllocationError::INVALID_LOCAL_NODE);
    EXPECT_TRUE(model.calls.empty());
}

TEST_F(NumaAllocation, ExactUmaDispatchPreservesInitializationReclaimAndNames) {
    for (auto reclaim : {Reclaim::NEVER, Reclaim::LEGACY_UMA}) {
        for (auto initialization : {Initialization::ZERO, Initialization::FULL_OVERWRITE}) {
            auto result = numa::try_allocate_pages(context(), 1, PAGE, OWNER, initialization, reclaim, "actual-site");
            ASSERT_EQ(result.error, numa::AllocationError::NONE);
            ASSERT_NE(result.span.data(), nullptr);
            EXPECT_EQ(result.requested, 0);
            EXPECT_EQ(result.actual, 0);
            EXPECT_FALSE(result.used_fallback());
            EXPECT_EQ(result.span.pages(), 1U);
            auto const EXPECTED = reclaim == Reclaim::NEVER
                                      ? (initialization == Initialization::ZERO ? Dispatch::ZERO : Dispatch::OVERWRITE)
                                      : (initialization == Initialization::ZERO ? Dispatch::RECLAIM_ZERO : Dispatch::RECLAIM_OVERWRITE);
            auto const& call = model.calls.back();
            EXPECT_EQ(call.dispatch, EXPECTED);
            EXPECT_EQ(call.owner, OWNER);
            EXPECT_EQ(call.name, "actual-site");
            EXPECT_EQ(call.retries, reclaim == Reclaim::NEVER ? 0U : mm::phys::PAGE_ALLOC_RECLAIM_RETRY_DEFAULT);
            auto const* bytes = static_cast<const uint8_t*>(result.span.data());
            EXPECT_TRUE(std::all_of(bytes, bytes + PAGE, [initialization](uint8_t value) {
                return value == (initialization == Initialization::ZERO ? 0 : 0xa5);
            }));
        }
    }
    auto result = numa::try_allocate_pages(context(), 1, PAGE, OWNER);
    EXPECT_EQ(model.calls.back().name, "numa-uma");
    EXPECT_EQ(model.calls.size(), 5U);
    EXPECT_EQ(model.decremented.size(), 4U);
}

TEST_F(NumaAllocation, UmaExhaustionIsHonestAndOutputAdapterPreservesEmptyOwnership) {
    model.fail_all = true;
    auto request = context();
    numa::AllocationResult output;
    numa::try_allocate_pages_into(output, request, 1, PAGE, OWNER, Initialization::FULL_OVERWRITE, Reclaim::LEGACY_UMA, "failed-site");
    expect_empty(output, numa::AllocationError::EXHAUSTED);
    EXPECT_EQ(output.requested, 0);
    EXPECT_EQ(model.calls.size(), 1U);
    EXPECT_EQ(model.calls[0].dispatch, Dispatch::RECLAIM_OVERWRITE);
    EXPECT_EQ(model.calls[0].name, "failed-site");
    EXPECT_EQ(request.policy, numa::Policy{});
    EXPECT_EQ(request.local_node, 0);
    EXPECT_EQ(request.virtual_page_index, 0U);
    EXPECT_TRUE(model.decremented.empty());
}

TEST_F(NumaAllocation, UmaHigherAlignmentAndLargeSpanUseRangeWithoutLegacyReclaim) {
    auto aligned = numa::try_allocate_pages(context(), 1, 2 * PAGE, OWNER, Initialization::FULL_OVERWRITE, Reclaim::LEGACY_UMA);
    ASSERT_EQ(aligned.error, numa::AllocationError::NONE);
    EXPECT_EQ(reinterpret_cast<uint64_t>(aligned.span.data()) % (2 * PAGE), 0U);
    EXPECT_EQ(model.calls.back().dispatch, Dispatch::RANGE);
    EXPECT_EQ(model.calls.back().alignment, 2 * PAGE);
    auto large = numa::try_allocate_pages(context(), 512, LARGE, OWNER, Initialization::ZERO, Reclaim::LEGACY_UMA);
    ASSERT_EQ(large.error, numa::AllocationError::NONE);
    EXPECT_EQ(large.span.pages(), 512U);
    EXPECT_EQ(model.calls.back().dispatch, Dispatch::RANGE);
    EXPECT_EQ(model.calls.back().pages, 512U);
    EXPECT_EQ(model.calls.back().initialization, Initialization::ZERO);
    auto const FIRST = reinterpret_cast<uint64_t>(large.span.data());
    large.span.reset();
    ASSERT_EQ(model.decremented.size(), 512U);
    for (size_t i = 0; i < 512; ++i) EXPECT_EQ(model.decremented[i], reinterpret_cast<void*>(FIRST + i * PAGE));
    EXPECT_TRUE(large.span.empty());
    EXPECT_EQ(large.span.pages(), 0U);
}

TEST_F(NumaAllocation, UnmanagedUmaFrameIsReleasedBeforeInvalidPlacementReturns) {
    model.return_unmanaged_uma = true;
    numa::AllocationResult output;
    numa::try_allocate_pages_into(output, context(), 1, PAGE, OWNER, Initialization::FULL_OVERWRITE, Reclaim::LEGACY_UMA);
    expect_empty(output, numa::AllocationError::INVALID_PLACEMENT);
    EXPECT_EQ(output.requested, 0);
    ASSERT_EQ(model.calls.size(), 1U);
    EXPECT_EQ(model.calls[0].dispatch, Dispatch::RECLAIM_OVERWRITE);
    ASSERT_EQ(model.blocks.size(), 1U);
    EXPECT_EQ(model.blocks[0].physical, 0x70000000ULL);
    EXPECT_EQ(model.blocks[0].storage, nullptr);
    EXPECT_EQ(model.decremented.size(), 1U);
    EXPECT_EQ(model.live_refs(), 0U);
    output.span.reset();
    EXPECT_EQ(model.decremented.size(), 1U);
}

TEST_F(NumaAllocation, BindCannotEscapeMaskEvenWithLegacyReclaimAndOtherFreeNodes) {
    model.domains(3);
    auto const FIRST = model.topology.memory[1].base;
    model.failed_ranges = {FIRST};
    auto result = numa::try_allocate_pages(context(numa::Mode::BIND, 2), 1, PAGE, OWNER, Initialization::ZERO, Reclaim::LEGACY_UMA);
    expect_empty(result, numa::AllocationError::EXHAUSTED);
    EXPECT_EQ(result.requested, 1);
    ASSERT_EQ(model.calls.size(), 1U);
    EXPECT_EQ(model.calls[0].dispatch, Dispatch::RANGE);
    EXPECT_EQ(model.calls[0].first, FIRST);
    EXPECT_TRUE(model.decremented.empty());
}

TEST_F(NumaAllocation, PreferredFallbackReportsRequestedAndActualSeparately) {
    model.domains(3);
    model.failed_ranges = {model.topology.memory[2].base};
    auto result =
        numa::try_allocate_pages(context(numa::Mode::PREFERRED, 4), 1, PAGE, OWNER, Initialization::FULL_OVERWRITE, Reclaim::LEGACY_UMA);
    ASSERT_EQ(result.error, numa::AllocationError::NONE);
    EXPECT_EQ(result.requested, 2);
    EXPECT_EQ(result.actual, 0);
    EXPECT_TRUE(result.used_fallback());
    ASSERT_EQ(model.calls.size(), 2U);
    EXPECT_EQ(model.calls[0].first, model.topology.memory[2].base);
    EXPECT_EQ(model.calls[1].first, model.topology.memory[0].base);
    EXPECT_EQ(model.calls[1].initialization, Initialization::FULL_OVERWRITE);
}

TEST_F(NumaAllocation, InterleaveRotatesWithinSelectedMaskAndFallbackStaysThere) {
    model.domains(3);
    model.failed_ranges = {model.topology.memory[2].base};
    auto result = numa::try_allocate_pages(context(numa::Mode::INTERLEAVE, 5, 1, 1), 1, PAGE, OWNER);
    ASSERT_EQ(result.error, numa::AllocationError::NONE);
    EXPECT_EQ(result.requested, 2);
    EXPECT_EQ(result.actual, 0);
    ASSERT_EQ(model.calls.size(), 2U);
    EXPECT_EQ(model.calls[0].first, model.topology.memory[2].base);
    EXPECT_EQ(model.calls[1].first, model.topology.memory[0].base);
    model.calls.clear();
    auto next = numa::try_allocate_pages(context(numa::Mode::INTERLEAVE, 5, 1, 2), 1, PAGE, OWNER);
    ASSERT_EQ(next.error, numa::AllocationError::NONE);
    EXPECT_EQ(next.requested, 0);
    EXPECT_EQ(next.actual, 0);
    ASSERT_EQ(model.calls.size(), 1U);
    EXPECT_EQ(model.calls[0].first, model.topology.memory[0].base);
}

TEST_F(NumaAllocation, MultipleRangesAreTriedForSelectedNodeWithoutChangingContext) {
    model.domains(3);
    model.topology.memory_range_count = 4;
    model.topology.memory[3] = {0x50000000, 2 * LARGE, 1, 0};
    model.topology.nodes[1].usable_bytes += 2 * LARGE;
    model.failed_ranges = {model.topology.memory[1].base};
    auto request = context(numa::Mode::BIND, 2, 0, 17);
    numa::AllocationResult output;
    numa::try_allocate_pages_into(output, request, 1, PAGE, OWNER, Initialization::ZERO, Reclaim::LEGACY_UMA, "range-site");
    ASSERT_EQ(output.error, numa::AllocationError::NONE);
    EXPECT_EQ(output.requested, 1);
    EXPECT_EQ(output.actual, 1);
    ASSERT_EQ(model.calls.size(), 2U);
    EXPECT_EQ(model.calls[0].first, model.topology.memory[1].base);
    EXPECT_EQ(model.calls[1].first, model.topology.memory[3].base);
    EXPECT_EQ(request.policy, (numa::Policy{2, numa::Mode::BIND}));
    EXPECT_EQ(request.local_node, 0);
    EXPECT_EQ(request.virtual_page_index, 17U);
}

TEST_F(NumaAllocation, PhysicalLookupUsesManagedRangesWithExclusiveEnds) {
    model.domains(3);
    for (uint16_t node = 0; node < 3; ++node) {
        auto const& range = model.topology.memory[node];
        EXPECT_EQ(numa::physical_node(range.base), node);
        EXPECT_EQ(numa::physical_node(range.base + range.length - 1), node);
        EXPECT_EQ(numa::physical_node(range.base + range.length), numa::INVALID_NODE);
    }
    EXPECT_EQ(numa::physical_node(0), numa::INVALID_NODE);
    model.published = false;
    EXPECT_EQ(numa::physical_node(0x10000000), numa::INVALID_NODE);
    EXPECT_TRUE(model.calls.empty());
}

TEST_F(NumaAllocation, OwnedSpanMoveAssignmentDropsOldOwnerAndPreservesPayload) {
    auto first = numa::try_allocate_pages(context(), 1, PAGE, OWNER, Initialization::FULL_OVERWRITE);
    auto second = numa::try_allocate_pages(context(), 1, PAGE, OWNER, Initialization::FULL_OVERWRITE);
    ASSERT_EQ(first.error, numa::AllocationError::NONE);
    ASSERT_EQ(second.error, numa::AllocationError::NONE);
    void* const OLD = first.span.data();
    void* const TRANSFERRED = second.span.data();
    static_cast<uint8_t*>(TRANSFERRED)[71] = 0x37;
    numa::OwnedSpan moved{std::move(second.span)};
    EXPECT_TRUE(second.span.empty());
    EXPECT_EQ(second.span.pages(), 0U);
    EXPECT_TRUE(model.decremented.empty());
    first.span = std::move(moved);
    EXPECT_TRUE(moved.empty());
    EXPECT_EQ(first.span.data(), TRANSFERRED);
    EXPECT_EQ(static_cast<uint8_t*>(first.span.data())[71], 0x37);
    ASSERT_EQ(model.decremented.size(), 1U);
    EXPECT_EQ(model.decremented[0], OLD);
    auto& alias = first.span;
    first.span = std::move(alias);
    EXPECT_EQ(first.span.data(), TRANSFERRED);
    EXPECT_EQ(model.decremented.size(), 1U);
    first.span.reset();
    first.span.reset();
    ASSERT_EQ(model.decremented.size(), 2U);
    EXPECT_EQ(model.decremented[1], TRANSFERRED);
}

TEST_F(NumaAllocation, ReleaseTransfersExactlyOneReferencePerPageToCaller) {
    void* released = nullptr;
    {
        auto result = numa::try_allocate_pages(context(), 512, LARGE, OWNER, Initialization::FULL_OVERWRITE);
        ASSERT_EQ(result.error, numa::AllocationError::NONE);
        released = result.span.release();
        EXPECT_TRUE(result.span.empty());
        EXPECT_EQ(result.span.pages(), 0U);
    }
    EXPECT_TRUE(model.decremented.empty());
    EXPECT_EQ(model.live_refs(), 512U);
    for (uint32_t page = 0; page < 512; ++page)
        EXPECT_EQ(mm::phys::page_ref_dec(static_cast<std::byte*>(released) + static_cast<uint64_t>(page) * PAGE), 0U);
    EXPECT_EQ(model.live_refs(), 0U);
}

// Copy the complete initialized representation so early failures cannot silently
// clear/reset a pre-existing context. Context has scalar/trivial value members.
auto context_representation(const numa::AllocationContext& value) -> std::array<std::byte, sizeof(numa::AllocationContext)> {
    std::array<std::byte, sizeof(numa::AllocationContext)> bytes{};
    std::memcpy(bytes.data(), &value, bytes.size());
    return bytes;
}
auto sentinel_context() -> numa::AllocationContext {
    numa::AllocationContext result;
    std::memset(&result, 0x5a, sizeof(result));
    result.policy = {3, numa::Mode::INTERLEAVE};
    result.local_node = 23;
    result.virtual_page_index = 0x123456789abcdef0ULL;
    return result;
}

TEST_F(NumaAllocation, ContextRejectsMissingAndMalformedTopologyWithoutCPUOrGuard) {
    auto output = sentinel_context();
    auto const BEFORE = context_representation(output);
    for (unsigned scenario = 0; scenario < 6; ++scenario) {
        SCOPED_TRACE(scenario);
        model.domains(2);
        model.published = scenario != 0;
        if (scenario == 1) model.topology.node_count = 0;
        if (scenario == 2) model.topology.node_count = numa::MAX_NODES + 1;
        if (scenario == 3) model.topology.cpu_count = 0;
        if (scenario == 4) model.topology.cpu_count = static_cast<uint16_t>(model.topology.cpu_nodes.size() + 1);
        if (scenario == 5) model.topology.cpu_count = UINT16_MAX;
        model.observe_context(output, 3);
        EXPECT_EQ(numa::capture_allocation_context({}, output), numa::AllocationError::NO_TOPOLOGY);
        EXPECT_EQ(context_representation(output), BEFORE);
        EXPECT_EQ(model.preempt_depth, 3U);
        EXPECT_EQ(model.preempt_disables, 0U);
        EXPECT_EQ(model.preempt_enables, 0U);
        EXPECT_EQ(model.cpu_reads, 0U);
        EXPECT_TRUE(model.calls.empty());
    }
    model.preempt_depth = 0;
}

TEST_F(NumaAllocation, ContextRejectsInvalidCopiedPolicyBeforeCPUOrGuard) {
    model.domains(3);
    auto output = sentinel_context();
    auto const BEFORE = context_representation(output);
    for (auto policy :
         {numa::Policy{1, numa::Mode::LOCAL}, numa::Policy{0, numa::Mode::BIND}, numa::Policy{8, numa::Mode::BIND},
          numa::Policy{3, numa::Mode::PREFERRED}, numa::Policy{0, numa::Mode::INTERLEAVE}, numa::Policy{0, static_cast<numa::Mode>(255)}}) {
        SCOPED_TRACE(static_cast<unsigned>(policy.mode));
        model.observe_context(output, 2);
        EXPECT_EQ(numa::capture_allocation_context(policy, output), numa::AllocationError::INVALID_POLICY);
        EXPECT_EQ(context_representation(output), BEFORE);
        EXPECT_EQ(model.preempt_depth, 2U);
        EXPECT_EQ(model.preempt_disables, 0U);
        EXPECT_EQ(model.preempt_enables, 0U);
        EXPECT_EQ(model.cpu_reads, 0U);
        EXPECT_TRUE(model.calls.empty());
    }
    model.preempt_depth = 0;
}

TEST_F(NumaAllocation, ContextCPUAndMappedNodeFailuresRestoreExactNestedDepth) {
    for (uint32_t incoming_depth : {0U, 4U}) {
        for (unsigned scenario = 0; scenario < 5; ++scenario) {
            SCOPED_TRACE(incoming_depth);
            SCOPED_TRACE(scenario);
            model.domains(2);
            auto output = sentinel_context();
            auto const BEFORE = context_representation(output);
            model.current_cpu = scenario == 0   ? model.topology.cpu_count
                                : scenario == 1 ? model.topology.cpu_nodes.size()
                                : scenario == 2 ? UINT64_MAX
                                                : 1;
            if (scenario == 3) model.topology.cpu_nodes[1] = numa::INVALID_NODE;
            if (scenario == 4) model.topology.cpu_nodes[1] = model.topology.node_count;
            model.observe_context(output, incoming_depth);
            EXPECT_EQ(numa::capture_allocation_context({}, output), numa::AllocationError::INVALID_LOCAL_NODE);
            EXPECT_EQ(context_representation(output), BEFORE);
            EXPECT_EQ(model.preempt_depth, incoming_depth);
            EXPECT_EQ(model.preempt_disables, 1U);
            EXPECT_EQ(model.preempt_enables, 1U);
            EXPECT_EQ(model.cpu_reads, 1U);
            EXPECT_TRUE(model.calls.empty());
        }
    }
    model.preempt_depth = 0;
}

TEST_F(NumaAllocation, ContextCapturesMappedCPUAndCommitsOnlyAfterGuardRestoration) {
    model.domains(3);
    model.topology.cpu_nodes[2] = 1;
    for (uint32_t incoming_depth : {0U, 5U}) {
        auto output = sentinel_context();
        model.current_cpu = 2;
        model.observe_context(output, incoming_depth);
        model.migrate_after_guard = true;
        model.next_cpu = 0;
        EXPECT_EQ(numa::capture_allocation_context({}, output), numa::AllocationError::NONE);
        EXPECT_EQ(output.policy, numa::Policy{});
        EXPECT_EQ(output.local_node, 1);
        EXPECT_EQ(output.virtual_page_index, 0U);
        EXPECT_EQ(model.current_cpu, 0U);
        EXPECT_EQ(model.preempt_depth, incoming_depth);
        EXPECT_EQ(model.preempt_disables, 1U);
        EXPECT_EQ(model.preempt_enables, 1U);
        EXPECT_EQ(model.cpu_reads, 1U);
        EXPECT_TRUE(model.calls.empty());
    }
    model.preempt_depth = 0;
}

TEST_F(NumaAllocation, ContextPreservesEveryValidPolicyModeAndCopiedValue) {
    model.domains(3);
    model.current_cpu = 2;
    for (auto policy : {numa::Policy{}, numa::Policy{2, numa::Mode::PREFERRED}, numa::Policy{5, numa::Mode::BIND},
                        numa::Policy{5, numa::Mode::INTERLEAVE}}) {
        auto const COPIED = policy;
        auto output = sentinel_context();
        model.observe_context(output);
        model.caller_policy_to_change = &policy;
        EXPECT_EQ(numa::capture_allocation_context(policy, output), numa::AllocationError::NONE);
        EXPECT_EQ(output.policy, COPIED);
        EXPECT_EQ(policy, numa::Policy{});
        EXPECT_EQ(output.local_node, 2);
        EXPECT_EQ(output.virtual_page_index, 0U);
        EXPECT_EQ(model.preempt_depth, 0U);
        EXPECT_EQ(model.preempt_disables, 1U);
        EXPECT_EQ(model.preempt_enables, 1U);
        EXPECT_EQ(model.cpu_reads, 1U);
        EXPECT_TRUE(model.calls.empty());
    }
}

TEST_F(NumaAllocation, ContextAcceptsLastCPUArrayEntryAndNode63WithoutSubstitution) {
    model.domains(64);
    model.topology.cpu_count = static_cast<uint16_t>(model.topology.cpu_nodes.size());
    model.topology.cpu_nodes.back() = 63;
    model.current_cpu = model.topology.cpu_nodes.size() - 1;
    auto output = sentinel_context();
    model.observe_context(output);
    auto const POLICY = numa::Policy{uint64_t{1} << 63, numa::Mode::BIND};
    EXPECT_EQ(numa::capture_allocation_context(POLICY, output), numa::AllocationError::NONE);
    EXPECT_EQ(output.policy, POLICY);
    EXPECT_EQ(output.local_node, 63);
    EXPECT_EQ(output.virtual_page_index, 0U);
    EXPECT_EQ(model.cpu_reads, 1U);
    EXPECT_EQ(model.preempt_depth, 0U);
    EXPECT_TRUE(model.calls.empty());
}

TEST_F(NumaAllocation, ContextRetainsMemorylessLocalForRealFactoryFallbackOrStrictExhaustion) {
    model.domains(2);
    model.topology.memory_range_count = 1;
    model.topology.nodes[1].usable_bytes = 0;
    model.current_cpu = 1;
    numa::AllocationContext local;
    model.observe_context(local);
    ASSERT_EQ(numa::capture_allocation_context({}, local), numa::AllocationError::NONE);
    EXPECT_EQ(local.local_node, 1);
    EXPECT_EQ(local.virtual_page_index, 0U);
    EXPECT_TRUE(model.calls.empty());
    auto fallback = numa::try_allocate_pages(local, 1, PAGE, OWNER);
    ASSERT_EQ(fallback.error, numa::AllocationError::NONE);
    EXPECT_EQ(fallback.requested, 1);
    EXPECT_EQ(fallback.actual, 0);
    EXPECT_TRUE(fallback.used_fallback());
    ASSERT_EQ(model.calls.size(), 1U);
    EXPECT_EQ(model.calls[0].dispatch, Dispatch::RANGE);
    EXPECT_EQ(model.calls[0].first, model.topology.memory[0].base);

    numa::AllocationContext bound;
    model.observe_context(bound);
    ASSERT_EQ(numa::capture_allocation_context({2, numa::Mode::BIND}, bound), numa::AllocationError::NONE);
    EXPECT_EQ(bound.local_node, 1);
    auto failed = numa::try_allocate_pages(bound, 1, PAGE, OWNER);
    expect_empty(failed, numa::AllocationError::EXHAUSTED);
    EXPECT_EQ(failed.requested, 1);
    EXPECT_EQ(model.calls.size(), 1U);
    EXPECT_EQ(model.cpu_reads, 1U);
    EXPECT_EQ(model.preempt_depth, 0U);
}
}  // namespace
