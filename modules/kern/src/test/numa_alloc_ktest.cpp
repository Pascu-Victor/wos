#include <array>
#include <cstdint>
#include <cstring>
#include <platform/acpi/numa_boot.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <test/ktest.hpp>
#include <type_traits>
#include <utility>

namespace numa = ker::mod::mm::numa;
namespace topology = ker::mod::acpi::numa;
namespace phys = ker::mod::mm::phys;
namespace mm = ker::mod::mm;

namespace {
using log = ker::mod::dbg::logger<"numa-test">;
constexpr uint64_t PAGE = mm::paging::PAGE_SIZE;
constexpr uint64_t LARGE = 512 * PAGE;
constexpr size_t OWNER = static_cast<size_t>(mm::PhysicalPageOwner::SELFTEST);

static_assert(!std::is_copy_constructible_v<numa::OwnedSpan>);
static_assert(!std::is_copy_assignable_v<numa::OwnedSpan>);
static_assert(std::is_nothrow_move_constructible_v<numa::OwnedSpan>);
static_assert(std::is_nothrow_move_assignable_v<numa::OwnedSpan>);
static_assert(!std::is_constructible_v<numa::OwnedSpan, void*, uint32_t>);

auto physical_address(const void* pointer) -> uint64_t {
    return reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(pointer)));
}

auto snapshot() -> phys::PhysicalBalanceSnapshot {
    phys::PhysicalBalanceSnapshot result{};
    phys::get_physical_balance_snapshot(result);
    return result;
}

void expect_identity(const phys::PhysicalBalanceSnapshot& current) {
    KEXPECT_EQ(current.identity_mismatch_pages, 0U);
    KEXPECT_EQ(current.untracked_unreclaimable_pages, 0U);
    KEXPECT_EQ(current.managed_pages, current.identity_pages);
}

void expect_closed(const phys::PhysicalBalanceSnapshot& before, const phys::PhysicalBalanceSnapshot& after) {
    expect_identity(after);
    KEXPECT_EQ(after.owners[OWNER].pages, before.owners[OWNER].pages);
    KEXPECT_EQ(after.owners[OWNER].objects, before.owners[OWNER].objects);
    // Other CPUs may allocate/free unrelated owners. Preserve raw global-free
    // samples rather than claiming an unowned global quiescent interval.
    log::info("allocator closure free_before=%llu free_after=%llu selftest_pages=%llu selftest_objects=%llu",
              static_cast<unsigned long long>(before.free_pages), static_cast<unsigned long long>(after.free_pages),
              static_cast<unsigned long long>(after.owners[OWNER].pages), static_cast<unsigned long long>(after.owners[OWNER].objects));
}

auto zeroed(const numa::OwnedSpan& span) -> bool {
    auto const* bytes = static_cast<const uint8_t*>(span.data());
    for (uint64_t i = 0; i < static_cast<uint64_t>(span.pages()) * PAGE; ++i)
        if (bytes[i] != 0) return false;
    return true;
}

auto memory_nodes(const topology::Topology& machine) -> numa::NodeMask {
    numa::NodeMask mask = 0;
    for (numa::NodeId node = 0; node < machine.node_count; ++node)
        if (machine.nodes[node].usable_bytes != 0) mask |= numa::NodeMask{1} << node;
    return mask;
}

void expect_span(const numa::AllocationResult& result, uint32_t pages, uint64_t alignment) {
    KEXPECT_EQ(result.error, numa::AllocationError::NONE);
    KEXPECT_EQ(result.span.pages(), pages);
    auto const ADDRESS = physical_address(result.span.data());
    KEXPECT_EQ(ADDRESS % alignment, 0U);
    KEXPECT_EQ(numa::physical_node(ADDRESS), result.actual);
    for (uint32_t i = 0; i < pages; ++i) {
        auto* const FRAME = static_cast<uint8_t*>(result.span.data()) + static_cast<uint64_t>(i) * PAGE;
        KEXPECT_EQ(phys::page_ref_get(FRAME), 1U);
        KEXPECT_EQ(phys::page_kind_get(FRAME), mm::PageKind::NORMAL);
        KEXPECT_EQ(numa::physical_node(physical_address(FRAME)), result.actual);
        KEXPECT_EQ(numa::physical_node(physical_address(FRAME) + PAGE - 1), result.actual);
    }
}
}  // namespace

KTEST(NumaAlloc, InvalidArgumentsNeverReachUmaShortcut) {
    KREQUIRE_NE(topology::boot_topology(), nullptr);
    auto const BEFORE = snapshot();
    expect_identity(BEFORE);
    numa::AllocationContext const CONTEXT{{}, 0, 0};
    for (uint32_t pages : {0U, 2U, 511U, 513U}) {
        auto result = numa::try_allocate_pages(CONTEXT, pages, PAGE, mm::PhysicalPageOwner::SELFTEST);
        KEXPECT_EQ(result.error, numa::AllocationError::INVALID_ARGUMENT);
        KEXPECT_TRUE(result.span.empty());
    }
    for (uint64_t alignment : {uint64_t{0}, PAGE - 1, PAGE + 1}) {
        auto result = numa::try_allocate_pages(CONTEXT, 1, alignment, mm::PhysicalPageOwner::SELFTEST);
        KEXPECT_EQ(result.error, numa::AllocationError::INVALID_ARGUMENT);
        KEXPECT_TRUE(result.span.empty());
    }
    for (auto owner : {mm::PhysicalPageOwner::INVALID, mm::PhysicalPageOwner::COUNT}) {
        auto result = numa::try_allocate_pages(CONTEXT, 1, PAGE, owner);
        KEXPECT_EQ(result.error, numa::AllocationError::INVALID_ARGUMENT);
        KEXPECT_TRUE(result.span.empty());
    }
    auto bad_initialization =
        numa::try_allocate_pages(CONTEXT, 1, PAGE, mm::PhysicalPageOwner::SELFTEST, static_cast<phys::PageInitialization>(255));
    KEXPECT_EQ(bad_initialization.error, numa::AllocationError::INVALID_ARGUMENT);
    KEXPECT_TRUE(bad_initialization.span.empty());
    auto bad_policy = numa::try_allocate_pages({{1, numa::Mode::LOCAL}, 0, 0}, 1, PAGE, mm::PhysicalPageOwner::SELFTEST);
    KEXPECT_EQ(bad_policy.error, numa::AllocationError::INVALID_POLICY);
    KEXPECT_TRUE(bad_policy.span.empty());
    auto bad_local = numa::try_allocate_pages({{}, numa::INVALID_NODE, 0}, 1, PAGE, mm::PhysicalPageOwner::SELFTEST);
    KEXPECT_EQ(bad_local.error, numa::AllocationError::INVALID_LOCAL_NODE);
    KEXPECT_TRUE(bad_local.span.empty());
    KEXPECT_EQ(numa::physical_node(UINT64_MAX), numa::INVALID_NODE);
    expect_closed(BEFORE, snapshot());
}

KTEST(NumaAlloc, ZeroedFrameIsConfinedToEverySelectedMemoryNode) {
    auto const* machine = topology::boot_topology();
    KREQUIRE_NE(machine, nullptr);
    auto const BEFORE = snapshot();
    expect_identity(BEFORE);
    for (numa::NodeId node = 0; node < machine->node_count; ++node) {
        if (machine->nodes[node].usable_bytes == 0) continue;
        auto result =
            numa::try_allocate_pages({{numa::NodeMask{1} << node, numa::Mode::BIND}, 0, 0}, 1, PAGE, mm::PhysicalPageOwner::SELFTEST);
        KREQUIRE_TRUE(!result.span.empty());
        expect_span(result, 1, PAGE);
        KEXPECT_EQ(result.requested, node);
        KEXPECT_EQ(result.actual, node);
        KEXPECT_FALSE(result.used_fallback());
        KEXPECT_TRUE(zeroed(result.span));
        std::memset(result.span.data(), 0xa5, PAGE);
    }
    expect_closed(BEFORE, snapshot());
}

KTEST(NumaAlloc, Aligned512FramesAreIndependentlyOwnedAndFullyZeroed) {
    auto const* machine = topology::boot_topology();
    KREQUIRE_NE(machine, nullptr);
    auto const BEFORE = snapshot();
    expect_identity(BEFORE);
    {
        numa::AllocationResult result;
        for (numa::NodeId node = 0; node < machine->node_count && result.span.empty(); ++node) {
            if (machine->nodes[node].usable_bytes < LARGE) continue;
            result = numa::try_allocate_pages({{numa::NodeMask{1} << node, numa::Mode::BIND}, 0, 0}, 512, LARGE,
                                              mm::PhysicalPageOwner::SELFTEST);
        }
        KREQUIRE_TRUE(!result.span.empty());
        expect_span(result, 512, LARGE);
        KEXPECT_EQ(result.actual, result.requested);
        KEXPECT_TRUE(zeroed(result.span));
        auto const LIVE = snapshot();
        expect_identity(LIVE);
        KEXPECT_EQ(LIVE.owners[OWNER].pages, BEFORE.owners[OWNER].pages + 512);
        KEXPECT_EQ(LIVE.owners[OWNER].objects, BEFORE.owners[OWNER].objects + 512);
        std::memset(result.span.data(), 0x5a, LARGE);
    }
    expect_closed(BEFORE, snapshot());
}

KTEST(NumaAlloc, MoveAssignmentResetAndExplicitReleaseOwnExactlyOneReference) {
    KREQUIRE_NE(topology::boot_topology(), nullptr);
    auto const BEFORE = snapshot();
    expect_identity(BEFORE);
    {
        auto first =
            numa::try_allocate_pages({{}, 0, 0}, 1, PAGE, mm::PhysicalPageOwner::SELFTEST, phys::PageInitialization::FULL_OVERWRITE);
        auto second = numa::try_allocate_pages({{}, 0, 1}, 1, PAGE, mm::PhysicalPageOwner::SELFTEST);
        KREQUIRE_TRUE(!first.span.empty());
        KREQUIRE_TRUE(!second.span.empty());
        std::memset(first.span.data(), 0x36, PAGE);  // Fulfil FULL_OVERWRITE before any read/publication.
        void* const FIRST = first.span.data();
        numa::OwnedSpan moved(std::move(first.span));
        KEXPECT_TRUE(first.span.empty());
        KEXPECT_EQ(first.span.pages(), 0U);
        KEXPECT_EQ(moved.data(), FIRST);
        second.span = std::move(moved);  // Retire the previous destination owner.
        KEXPECT_TRUE(moved.empty());
        KEXPECT_EQ(moved.pages(), 0U);
        KEXPECT_EQ(second.span.data(), FIRST);
        auto* alias = &second.span;
        second.span = std::move(*alias);  // Self move preserves the live owner.
        KEXPECT_EQ(second.span.data(), FIRST);
        auto const LIVE = snapshot();
        KEXPECT_EQ(LIVE.owners[OWNER].pages, BEFORE.owners[OWNER].pages + 1);
        KEXPECT_EQ(LIVE.owners[OWNER].objects, BEFORE.owners[OWNER].objects + 1);
        void* const TRANSFERRED = second.span.release();
        KEXPECT_EQ(TRANSFERRED, FIRST);
        KEXPECT_TRUE(second.span.empty());
        KEXPECT_EQ(second.span.pages(), 0U);
        KEXPECT_EQ(phys::page_ref_get(TRANSFERRED), 1U);
        if (TRANSFERRED != nullptr) phys::page_ref_dec(TRANSFERRED);
        second.span.reset();
        second.span.reset();
    }
    expect_closed(BEFORE, snapshot());
}

KTEST(NumaAlloc, FailedPreparationReturnsItsEarlierOwnedPrefix) {
    auto const* machine = topology::boot_topology();
    KREQUIRE_NE(machine, nullptr);
    // No managed x86 RAM interval can contain a reusable 2^63-aligned address.
    for (size_t i = 0; i < machine->memory_range_count; ++i) {
        KREQUIRE_TRUE(machine->memory[i].base != 0);
        KREQUIRE_TRUE(machine->memory[i].base + machine->memory[i].length < (uint64_t{1} << 63));
    }
    auto const BEFORE = snapshot();
    expect_identity(BEFORE);
    {
        auto prefix = numa::try_allocate_pages({{}, 0, 0}, 1, PAGE, mm::PhysicalPageOwner::SELFTEST);
        KREQUIRE_TRUE(!prefix.span.empty());
        auto failure = numa::try_allocate_pages({{}, 0, 1}, 1, uint64_t{1} << 63, mm::PhysicalPageOwner::SELFTEST);
        KEXPECT_EQ(failure.error, numa::AllocationError::EXHAUSTED);
        KEXPECT_TRUE(failure.span.empty());
        KEXPECT_EQ(failure.requested, 0U);
        KEXPECT_EQ(failure.actual, numa::INVALID_NODE);
        KEXPECT_FALSE(failure.used_fallback());
    }
    expect_closed(BEFORE, snapshot());
}

KTEST(NumaAlloc, MemorylessPreferredFallbackAndBindFailureRemainExplicit) {
    auto const* machine = topology::boot_topology();
    KREQUIRE_NE(machine, nullptr);
    bool exercised = false;
    for (numa::NodeId node = 0; node < machine->node_count; ++node) {
        if (machine->nodes[node].usable_bytes != 0) continue;
        exercised = true;
        auto const BEFORE = snapshot();
        {
            numa::NodeMask const MASK = numa::NodeMask{1} << node;
            auto strict = numa::try_allocate_pages({{MASK, numa::Mode::BIND}, 0, 0}, 1, PAGE, mm::PhysicalPageOwner::SELFTEST);
            KEXPECT_EQ(strict.error, numa::AllocationError::EXHAUSTED);
            KEXPECT_TRUE(strict.span.empty());
            KEXPECT_EQ(strict.requested, node);
            auto preferred = numa::try_allocate_pages({{MASK, numa::Mode::PREFERRED}, 0, 0}, 1, PAGE, mm::PhysicalPageOwner::SELFTEST);
            KREQUIRE_TRUE(!preferred.span.empty());
            expect_span(preferred, 1, PAGE);
            KEXPECT_EQ(preferred.requested, node);
            KEXPECT_NE(preferred.actual, node);
            KEXPECT_TRUE(preferred.used_fallback());
        }
        expect_closed(BEFORE, snapshot());
    }
    if (!exercised) log::info("memoryless-node runtime branch unexercised: topology has no zero-RAM node");
}

KTEST(NumaAlloc, ThousandAllocationReleaseCyclesReturnOwnerBaseline) {
    auto const* machine = topology::boot_topology();
    KREQUIRE_NE(machine, nullptr);
    auto const MASK = memory_nodes(*machine);
    KREQUIRE_NE(MASK, 0U);
    auto const BEFORE = snapshot();
    expect_identity(BEFORE);
    for (uint64_t index = 0; index < 1000; ++index) {
        numa::Mode const MODE = index % 2 == 0 ? numa::Mode::BIND : numa::Mode::INTERLEAVE;
        auto result = numa::try_allocate_pages({{MASK, MODE}, 0, index}, 1, PAGE, mm::PhysicalPageOwner::SELFTEST);
        KREQUIRE_TRUE(!result.span.empty());
        expect_span(result, 1, PAGE);
        KEXPECT_NE(MASK & (numa::NodeMask{1} << result.actual), 0U);
        KEXPECT_EQ(result.actual, result.requested);
        KEXPECT_FALSE(result.used_fallback());
        std::memset(result.span.data(), static_cast<int>(index & 0xff), PAGE);
    }
    expect_closed(BEFORE, snapshot());
    // This is a physical allocator loop, not map/fork/pin/unmap qualification.
}
