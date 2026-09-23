#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <dev/dma/manager.hpp>
#include <functional>
#include <mutex>
#include <vector>

namespace dma = ker::dev::dma;
namespace detail = dma::detail;
namespace ker::dev::pci {
struct PCIDevice {};
}  // namespace ker::dev::pci
namespace {
ker::dev::pci::PCIDevice first{}, second{};
struct Backend {
    std::mutex mutex;
    static inline thread_local bool locked = false;
    std::vector<std::byte*> allocations;
    std::vector<int> events;
    uint64_t physical = 0x11000;
    bool fail_alloc = false;
    bool fail_map = false;
    bool uncertain_map = false;
    bool fail_unmap = false;
    bool bad_iova = false;
    bool allow_buffer = true;
    bool allow_allocation = true;
    bool allow_device = true;
    uint64_t unmapped_address = 0, unmapped_physical = 0;
    size_t unmapped_bytes = 0;
    dma::Direction unmapped_direction = dma::Direction::to_device;
    size_t frees = 0;
    std::function<void()> copy_hook;
    std::function<void()> unmap_hook;
    ~Backend() {
        for (auto* p : allocations) std::free(p);
    }
    auto lock() -> uint64_t {
        mutex.lock();
        EXPECT_FALSE(locked);
        locked = true;
        return 0;
    }
    void unlock(uint64_t) {
        EXPECT_TRUE(locked);
        locked = false;
        mutex.unlock();
    }
    auto admit(const ker::dev::pci::PCIDevice* dev) -> bool { return allow_device && (dev == &first || dev == &second); }
    auto allocation_context() -> bool { return allow_allocation; }
    auto kernel_buffer(std::span<std::byte>) -> bool { return allow_buffer; }
    auto allocate(size_t bytes) -> detail::Storage {
        EXPECT_FALSE(locked);
        if (fail_alloc) return {};
        auto* p = static_cast<std::byte*>(std::aligned_alloc(detail::PAGE, bytes));
        if (!p) return {};
        std::memset(p, 0, bytes);
        allocations.push_back(p);
        events.push_back(1);
        return {p, physical};
    }
    void free(detail::Storage storage, size_t) {
        EXPECT_FALSE(locked);
        ++frees;
        events.push_back(6);
        auto it = std::find(allocations.begin(), allocations.end(), storage.cpu);
        ASSERT_NE(it, allocations.end());
        std::free(*it);
        allocations.erase(it);
    }
    auto map(const ker::dev::pci::PCIDevice*, uint64_t address, size_t bytes, size_t, uint64_t mask, dma::Direction) -> detail::Mapping {
        EXPECT_FALSE(locked);
        events.push_back(2);
        if (fail_map || uncertain_map) return {dma::Error::mapping_failed, address, uncertain_map};
        if (!detail::extent_fits(address, bytes, mask)) return {.error = dma::Error::address_limit};
        return {.address = bad_iova ? address + 1 : address};
    }
    auto unmap(const ker::dev::pci::PCIDevice*, uint64_t address, uint64_t original_physical, size_t bytes, dma::Direction direction)
        -> bool {
        unmapped_address = address;
        unmapped_physical = original_physical;
        unmapped_bytes = bytes;
        unmapped_direction = direction;
        EXPECT_FALSE(locked);
        events.push_back(5);
        if (unmap_hook) unmap_hook();
        return !fail_unmap;
    }
    void copy(std::byte* dest, const std::byte* source, size_t size) {
        EXPECT_FALSE(locked);
        if (copy_hook) copy_hook();
        std::memcpy(dest, source, size);
    }
    void for_device(std::byte*, size_t, dma::Direction) {
        EXPECT_FALSE(locked);
        events.push_back(3);
    }
    void for_cpu(std::byte*, size_t, dma::Direction) {
        EXPECT_FALSE(locked);
        events.push_back(4);
    }
};
using Manager = detail::Manager<Backend, 2>;
}  // namespace

TEST(DmaManager, ChecksPageExtentsMasksAlignmentAndOverflow) {
    for (size_t bytes : {size_t{0}, SIZE_MAX, detail::MAX_BACKING + 1}) EXPECT_FALSE(detail::layout(bytes, 4096, UINT64_MAX));
    for (size_t alignment : {size_t{0}, size_t{3}, SIZE_MAX, detail::MAX_BACKING * 2})
        EXPECT_FALSE(detail::layout(1, alignment, UINT64_MAX));
    for (uint64_t mask : {uint64_t{0}, uint64_t{4094}, uint64_t{0xefff}}) EXPECT_FALSE(detail::layout(1, 4096, mask));
    EXPECT_TRUE(detail::layout(1, 1, 4095));
    EXPECT_TRUE(detail::layout(detail::MAX_BACKING, 4096, UINT64_MAX));
    EXPECT_FALSE(detail::layout(detail::MAX_BACKING, 8192, UINT64_MAX));
    EXPECT_TRUE(detail::extent_fits(UINT64_MAX - 4095, 4096, UINT64_MAX));
    EXPECT_FALSE(detail::extent_fits(UINT64_MAX - 4094, 4096, UINT64_MAX));
    EXPECT_FALSE(detail::extent_fits(4096, 1, 4095));
}

TEST(DmaManager, CoherentAddressPublicationRequiresExactRetirementAndInvalidationBeforeFree) {
    Backend b;
    Manager m{b};
    auto a = m.allocate_coherent(&first, 5000, 65536, UINT32_MAX);
    ASSERT_EQ(a.error, dma::Error::none);
    ASSERT_NE(a.allocation.cpu, nullptr);
    EXPECT_EQ(m.stats().backing_pages, 32U);
    EXPECT_TRUE(std::all_of(a.allocation.cpu, a.allocation.cpu + 5000, [](std::byte v) { return v == std::byte{0}; }));
    auto tx = m.prepare(a.allocation.handle);
    ASSERT_EQ(tx.error, dma::Error::none);
    EXPECT_EQ(tx.submission.address, 0x20000U);
    EXPECT_EQ(m.release(a.allocation.handle), dma::Error::busy);
    EXPECT_EQ(m.complete(tx.submission, 0, dma::Retirement::command_completed), dma::Error::invalid_completion);
    EXPECT_EQ(m.complete(tx.submission, 0, dma::Retirement::device_quiesced), dma::Error::none);
    EXPECT_EQ(m.release(a.allocation.handle), dma::Error::none);
    EXPECT_EQ(b.events, (std::vector<int>{1, 2, 3, 4, 5, 6}));
    EXPECT_EQ(m.stats().regions, 0U);
    EXPECT_EQ(m.stats().backing_pages, 0U);
}

TEST(DmaManager, EveryPrepublicationFailureUnwindsWithoutLeakingCapacity) {
    Backend b;
    Manager m{b};
    EXPECT_EQ(m.allocate_coherent(nullptr, 1, 4096, UINT64_MAX).error, dma::Error::unavailable_device);
    b.fail_alloc = true;
    EXPECT_EQ(m.allocate_coherent(&first, 1, 4096, UINT64_MAX).error, dma::Error::allocation_failed);
    b.fail_alloc = false;
    b.fail_map = true;
    EXPECT_EQ(m.allocate_coherent(&first, 1, 4096, UINT64_MAX).error, dma::Error::mapping_failed);
    b.fail_map = false;
    EXPECT_EQ(m.allocate_coherent(&first, 1, 4096, 0xffff).error, dma::Error::address_limit);
    b.physical = UINT64_MAX - 100;
    EXPECT_EQ(m.allocate_coherent(&first, 1, 4096, UINT64_MAX).error, dma::Error::address_limit);
    EXPECT_EQ(m.stats().regions, 0U);
    EXPECT_EQ(b.frees, 3U);
}

TEST(DmaManager, StreamingOwnsExclusiveBouncePagesAndCopiesOnlyCompletedReceiveBytes) {
    for (auto dir : {dma::Direction::to_device, dma::Direction::from_device, dma::Direction::bidirectional}) {
        Backend b;
        Manager m{b};
        std::array<std::byte, 9> buffer{};
        buffer.fill(std::byte{0x11});
        auto a = m.map_streaming(&first, buffer, dir, UINT64_MAX);
        ASSERT_EQ(a.error, dma::Error::none);
        EXPECT_EQ(a.allocation.cpu, nullptr);
        auto tx = m.prepare(a.allocation.handle);
        ASSERT_EQ(tx.error, dma::Error::none);
        auto* bounce = b.allocations.front();
        EXPECT_EQ(bounce[0], dir == dma::Direction::from_device ? std::byte{0} : std::byte{0x11});
        std::memset(bounce, 0x22, 9);
        EXPECT_EQ(m.complete(tx.submission, 10, dma::Retirement::command_completed), dma::Error::invalid_completion);
        EXPECT_EQ(m.complete(tx.submission, 4, dma::Retirement::command_completed), dma::Error::none);
        for (size_t i = 0; i < 9; ++i) EXPECT_EQ(buffer[i], i < 4 && dir != dma::Direction::to_device ? std::byte{0x22} : std::byte{0x11});
        EXPECT_EQ(m.release(a.allocation.handle), dma::Error::none);
    }
}

TEST(DmaManager, StaleCrossDeviceAndCrossRegistryTokensCannotRetireCurrentDma) {
    Backend b, other;
    Manager m{b}, n{other};
    auto a = m.allocate_coherent(&first, 4096, 4096, UINT64_MAX).allocation;
    auto t1 = m.prepare(a.handle).submission;
    auto wrong = t1;
    wrong.handle.device = &second;
    EXPECT_EQ(m.complete(wrong, 0, dma::Retirement::device_quiesced), dma::Error::invalid_handle);
    EXPECT_EQ(n.release(a.handle), dma::Error::invalid_handle);
    EXPECT_EQ(m.complete(t1, 0, dma::Retirement::device_quiesced), dma::Error::none);
    auto t2 = m.prepare(a.handle).submission;
    EXPECT_NE(t2.sequence, t1.sequence);
    EXPECT_EQ(m.complete(t1, 0, dma::Retirement::device_quiesced), dma::Error::invalid_completion);
    EXPECT_EQ(m.complete(t2, 0, dma::Retirement::device_quiesced), dma::Error::none);
    EXPECT_EQ(m.release(a.handle), dma::Error::none);
    auto next = m.allocate_coherent(&first, 4096, 4096, UINT64_MAX).allocation;
    EXPECT_EQ(next.handle.slot, a.handle.slot);
    EXPECT_NE(next.handle.generation, a.handle.generation);
    EXPECT_EQ(m.release(a.handle), dma::Error::invalid_handle);
    EXPECT_EQ(m.release(next.handle), dma::Error::none);
}

TEST(DmaManager, CapacityAndGenerationExhaustionNeverWrapOrAllocateUntrackedStorage) {
    Backend b;
    detail::Manager<Backend, 1, 2> m{b};
    auto a = m.allocate_coherent(&first, 1, 4096, UINT64_MAX).allocation;
    EXPECT_EQ(m.allocate_coherent(&first, 1, 4096, UINT64_MAX).error, dma::Error::exhausted);
    for (int i = 0; i < 2; ++i) {
        auto tx = m.prepare(a.handle);
        ASSERT_EQ(tx.error, dma::Error::none);
        EXPECT_EQ(m.complete(tx.submission, 0, dma::Retirement::device_quiesced), dma::Error::none);
    }
    EXPECT_EQ(m.prepare(a.handle).error, dma::Error::exhausted);
    EXPECT_EQ(m.release(a.handle), dma::Error::none);
    auto next = m.allocate_coherent(&first, 1, 4096, UINT64_MAX).allocation;
    EXPECT_EQ(m.release(next.handle), dma::Error::none);
    EXPECT_EQ(m.allocate_coherent(&first, 1, 4096, UINT64_MAX).error, dma::Error::exhausted);
    EXPECT_EQ(b.frees, 2U);
}

TEST(DmaManager, UncertainMapAndFailedInvalidationKeepAllBackingPinned) {
    for (bool map_failure : {false, true}) {
        Backend b;
        Manager m{b};
        b.uncertain_map = map_failure;
        auto a = m.allocate_coherent(&first, 4096, 4096, UINT64_MAX);
        if (map_failure)
            EXPECT_EQ(a.error, dma::Error::quarantined);
        else {
            ASSERT_EQ(a.error, dma::Error::none);
            b.fail_unmap = true;
        }
        EXPECT_EQ(m.release(a.allocation.handle), dma::Error::quarantined);
        EXPECT_EQ(m.prepare(a.allocation.handle).error, dma::Error::quarantined);
        EXPECT_EQ(m.stats().backing_pages, 1U);
        EXPECT_EQ(m.stats().quarantined, 1U);
        EXPECT_EQ(b.frees, 0U);
    }
}

TEST(DmaManager, InvalidBackendAddressIsUnmappedBeforeFreeOrQuarantined) {
    for (bool fail : {false, true}) {
        Backend b;
        Manager m{b};
        b.bad_iova = true;
        b.fail_unmap = fail;
        auto a = m.allocate_coherent(&first, 4096, 4096, UINT64_MAX);
        EXPECT_EQ(a.error, fail ? dma::Error::quarantined : dma::Error::address_limit);
        EXPECT_EQ(b.frees, fail ? 0U : 1U);
        EXPECT_EQ(b.events, (fail ? std::vector<int>{1, 2, 5} : std::vector<int>{1, 2, 5, 6}));
    }
}

TEST(DmaManager, CopiesAndInvalidationRunUnlockedWhileBusyRecordsRejectRacingRelease) {
    Backend b;
    Manager m{b};
    std::array<std::byte, 16> buffer{};
    auto a = m.map_streaming(&first, buffer, dma::Direction::bidirectional, UINT64_MAX).allocation;
    b.copy_hook = [&] {
        EXPECT_EQ(m.release(a.handle), dma::Error::busy);
        EXPECT_EQ(m.prepare(a.handle).error, dma::Error::busy);
        EXPECT_EQ(m.quarantine(a.handle), dma::Error::busy);
    };
    auto tx = m.prepare(a.handle);
    ASSERT_EQ(tx.error, dma::Error::none);
    EXPECT_EQ(m.complete(tx.submission, 16, dma::Retirement::command_completed), dma::Error::none);
    b.unmap_hook = [&] {
        EXPECT_EQ(m.release(a.handle), dma::Error::busy);
        EXPECT_EQ(m.prepare(a.handle).error, dma::Error::busy);
    };
    EXPECT_EQ(m.release(a.handle), dma::Error::none);
}

TEST(DmaManager, TimeoutQuarantineNeverCopiesBackOrAutomaticallyFrees) {
    Backend b;
    {
        Manager m{b};
        std::array<std::byte, 4> buffer{};
        b.allow_buffer = false;
        EXPECT_EQ(m.map_streaming(&first, buffer, dma::Direction::from_device, UINT64_MAX).error, dma::Error::invalid_argument);
        b.allow_buffer = true;
        auto a = m.map_streaming(&first, buffer, dma::Direction::from_device, UINT64_MAX).allocation;
        auto tx = m.prepare(a.handle).submission;
        EXPECT_EQ(m.quarantine(a.handle), dma::Error::quarantined);
        EXPECT_EQ(m.complete(tx, 4, dma::Retirement::command_completed), dma::Error::quarantined);
        EXPECT_EQ(m.release(a.handle), dma::Error::quarantined);
    }
    EXPECT_EQ(b.frees, 0U);  // Registry destruction is not evidence of hardware retirement.
}

TEST(DmaManager, AtomicContextCanReusePreparedStorageButCannotAllocateOrRelease) {
    Backend b;
    Manager m{b};
    std::array<std::byte, 8> buffer{};
    auto a = m.map_streaming(&first, buffer, dma::Direction::to_device, UINT64_MAX).allocation;
    b.allow_allocation = false;
    EXPECT_EQ(m.allocate_coherent(&first, 1, 4096, UINT64_MAX).error, dma::Error::invalid_context);
    EXPECT_EQ(m.release(a.handle), dma::Error::invalid_context);
    auto tx = m.prepare(a.handle);
    ASSERT_EQ(tx.error, dma::Error::none);
    EXPECT_EQ(m.complete(tx.submission, 0, dma::Retirement::never_submitted), dma::Error::none);
    b.allow_allocation = true;
    EXPECT_EQ(m.release(a.handle), dma::Error::none);
}

TEST(DmaManager, StreamingPoolRebindsWithoutAllocationAndConsumesBorrowedLifetime) {
    Backend b;
    Manager m{b};
    auto pool = m.allocate_streaming(&first, 64, dma::Direction::bidirectional, UINT64_MAX);
    ASSERT_EQ(pool.error, dma::Error::none);
    ASSERT_NE(pool.allocation.cpu, nullptr);
    auto h = pool.allocation.handle;
    EXPECT_EQ(m.prepare(h).error, dma::Error::invalid_argument);
    std::array<std::byte, 32> first_buffer{}, next_buffer{};
    first_buffer.fill(std::byte{0x31});
    b.allow_allocation = false;
    ASSERT_EQ(m.bind_streaming(h, first_buffer, dma::Direction::to_device), dma::Error::none);
    auto first_tx = m.prepare(h);
    ASSERT_EQ(first_tx.error, dma::Error::none);
    EXPECT_EQ(pool.allocation.cpu[0], std::byte{0x31});
    EXPECT_EQ(m.bind_streaming(h, next_buffer, dma::Direction::from_device), dma::Error::busy);
    EXPECT_EQ(m.complete(first_tx.submission, first_buffer.size(), dma::Retirement::command_completed), dma::Error::none);
    EXPECT_EQ(m.prepare(h).error, dma::Error::invalid_argument);
    ASSERT_EQ(m.bind_streaming(h, next_buffer, dma::Direction::from_device), dma::Error::none);
    auto next_tx = m.prepare(h);
    ASSERT_EQ(next_tx.error, dma::Error::none);
    std::memset(pool.allocation.cpu, 0x42, next_buffer.size());
    EXPECT_EQ(m.complete(first_tx.submission, 0, dma::Retirement::command_completed), dma::Error::invalid_completion);
    EXPECT_EQ(m.complete(next_tx.submission, 7, dma::Retirement::command_completed), dma::Error::none);
    EXPECT_EQ(next_buffer[6], std::byte{0x42});
    EXPECT_EQ(next_buffer[7], std::byte{0});
    EXPECT_EQ(first_buffer[0], std::byte{0x31});
    EXPECT_EQ(b.allocations.size(), 1U);
    b.allow_allocation = true;
    EXPECT_EQ(m.release(h), dma::Error::none);
    EXPECT_EQ(m.stats().regions, 0U);
}

TEST(DmaManager, StreamingBindingsEnforceCapacityDirectionAndNonoverlappingCopies) {
    Backend b;
    Manager m{b};
    auto pool = m.allocate_streaming(&first, 64, dma::Direction::to_device, UINT64_MAX).allocation;
    std::array<std::byte, 65> buffer{};
    EXPECT_EQ(m.bind_streaming(pool.handle, buffer, dma::Direction::to_device), dma::Error::invalid_argument);
    EXPECT_EQ(m.bind_streaming(pool.handle, std::span{buffer}.first(1), dma::Direction::from_device), dma::Error::invalid_argument);
    EXPECT_EQ(m.bind_streaming(pool.handle, {pool.cpu + 1, 32}, dma::Direction::to_device), dma::Error::invalid_argument);
    EXPECT_EQ(m.bind_streaming(pool.handle, {reinterpret_cast<std::byte*>(UINTPTR_MAX - 3), 8}, dma::Direction::to_device),
              dma::Error::invalid_argument);
    ASSERT_EQ(m.bind_streaming(pool.handle, {pool.cpu, 64}, dma::Direction::to_device), dma::Error::none);
    b.copy_hook = [] { FAIL() << "exact CPU view must avoid overlapping memcpy"; };
    auto tx = m.prepare(pool.handle);
    ASSERT_EQ(tx.error, dma::Error::none);
    EXPECT_EQ(m.complete(tx.submission, 0, dma::Retirement::never_submitted), dma::Error::none);
    EXPECT_EQ(m.prepare(pool.handle).error, dma::Error::invalid_argument);
    EXPECT_EQ(m.release(pool.handle), dma::Error::none);
}

TEST(DmaManager, PerDeviceAccountingIncludesLiveAndQuarantinedOwnership) {
    Backend b;
    Manager m{b};
    auto a = m.allocate_coherent(&first, 4096, 4096, UINT64_MAX).allocation;
    auto c = m.allocate_streaming(&second, 8192, dma::Direction::from_device, UINT64_MAX).allocation;
    ASSERT_NE(a.handle.registry, nullptr);
    ASSERT_NE(c.handle.registry, nullptr);
    EXPECT_EQ(m.stats().regions, 2U);
    EXPECT_EQ(m.stats(&first).backing_pages, 1U);
    EXPECT_EQ(m.stats(&second).backing_pages, 2U);
    EXPECT_EQ(m.stats(&second).streaming, 1U);
    EXPECT_EQ(m.quarantine(c.handle), dma::Error::quarantined);
    EXPECT_EQ(m.stats(&first).quarantined, 0U);
    EXPECT_EQ(m.stats(&second).quarantined, 1U);
    EXPECT_EQ(m.release(a.handle), dma::Error::none);
    EXPECT_EQ(m.stats(&first).regions, 0U);
    EXPECT_EQ(m.stats(&second).regions, 1U);
    ker::dev::pci::PCIDevice unknown{};
    EXPECT_EQ(m.stats(&unknown).regions, 0U);
}

TEST(DmaManager, ReleaseUsesOriginalPhysicalExtentAndMappingPermissionAfterRebind) {
    Backend b;
    Manager m{b};
    auto pool = m.allocate_streaming(&first, 5000, dma::Direction::bidirectional, UINT64_MAX);
    ASSERT_EQ(pool.error, dma::Error::none);
    std::array<std::byte, 12> buffer{};
    ASSERT_EQ(m.bind_streaming(pool.allocation.handle, buffer, dma::Direction::from_device), dma::Error::none);
    auto submission = m.prepare(pool.allocation.handle);
    ASSERT_EQ(submission.error, dma::Error::none);
    ASSERT_EQ(m.complete(submission.submission, 0, dma::Retirement::never_submitted), dma::Error::none);
    ASSERT_EQ(m.release(pool.allocation.handle), dma::Error::none);
    EXPECT_EQ(b.unmapped_address, b.physical);
    EXPECT_EQ(b.unmapped_physical, b.physical);
    EXPECT_EQ(b.unmapped_bytes, 8192U);
    EXPECT_EQ(b.unmapped_direction, dma::Direction::bidirectional);
    b.bad_iova = true;
    EXPECT_EQ(m.allocate_coherent(&first, 1, 65536, UINT64_MAX).error, dma::Error::address_limit);
    EXPECT_EQ(b.unmapped_address, 0x20001U);
    EXPECT_EQ(b.unmapped_physical, 0x20000U);
    EXPECT_EQ(b.unmapped_bytes, 4096U);
    EXPECT_EQ(b.unmapped_direction, dma::Direction::bidirectional);
}

TEST(DmaManager, ClosedUnitRefusesNewSubmissionsFromPreviouslyAllocatedPools) {
    Backend b;
    Manager m{b};
    auto allocation = m.allocate_coherent(&first, 4096, 4096, UINT64_MAX);
    ASSERT_EQ(allocation.error, dma::Error::none);
    b.allow_device = false;
    EXPECT_EQ(m.prepare(allocation.allocation.handle).error, dma::Error::unavailable_device);
    EXPECT_EQ(m.stats().device_owned, 0U);
    b.fail_unmap = true;
    EXPECT_EQ(m.release(allocation.allocation.handle), dma::Error::quarantined);
    EXPECT_EQ(b.frees, 0U);
    EXPECT_EQ(m.stats().quarantined, 1U);
}

TEST(DmaManager, FaultLookupRetainsGenerationAndDistinguishesPendingReleaseFromFree) {
    Backend b;
    Manager m{b};
    auto allocation = m.allocate_coherent(&first, 5000, 65536, UINT64_MAX);
    ASSERT_EQ(allocation.error, dma::Error::none);
    auto info = m.mapping_info(&first, 0x21fff);
    EXPECT_EQ(info.handle.generation, allocation.allocation.handle.generation);
    EXPECT_EQ(info.physical, 0x20000U);
    EXPECT_EQ(info.address, 0x20000U);
    EXPECT_EQ(info.bytes, 8192U);
    EXPECT_FALSE(info.release_pending);
    EXPECT_EQ(m.mapping_info(&second, 0x20000).handle.registry, nullptr);
    EXPECT_EQ(m.mapping_info(&first, 0x22000).handle.registry, nullptr);
    b.allow_allocation = false;
    EXPECT_EQ(m.mapping_info(&first, 0x20000).handle.registry, nullptr);
    b.allow_allocation = true;
    b.unmap_hook = [&] {
        auto pending = m.mapping_info(&first, 0x20000);
        EXPECT_EQ(pending.handle.generation, info.handle.generation);
        EXPECT_TRUE(pending.release_pending);
    };
    EXPECT_EQ(m.release(allocation.allocation.handle), dma::Error::none);
    EXPECT_EQ(m.mapping_info(&first, 0x20000).handle.registry, nullptr);
    auto next = m.allocate_coherent(&first, 5000, 65536, UINT64_MAX);
    ASSERT_EQ(next.error, dma::Error::none);
    EXPECT_NE(next.allocation.handle.generation, info.handle.generation);
    EXPECT_EQ(m.quarantine(next.allocation.handle), dma::Error::quarantined);
    EXPECT_TRUE(m.mapping_info(&first, 0x20000).quarantined);
}
