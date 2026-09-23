#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <dev/dma/manager.hpp>
#include <dev/virtio/virtio.hpp>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace dma = ker::dev::dma;
namespace virtio = ker::dev::virtio;
namespace ker::dev::pci {
struct PCIDevice {};
}  // namespace ker::dev::pci
namespace {
ker::dev::pci::PCIDevice device;
struct Backend {
    struct Backing {
        dma::detail::Storage storage;
        size_t bytes;
    };
    std::mutex mutex;
    bool locked = false, allocation_allowed = true;
    size_t allocation_calls = 0, fail_after = SIZE_MAX, copies = 0;
    uint64_t next_address = 0x100000;
    std::vector<Backing> backing;
    std::function<void()> copy_observer;
    ~Backend() {
        for (auto const& b : backing) std::free(b.storage.cpu);
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
    auto allocation_context() -> bool { return allocation_allowed; }
    auto admit(const ker::dev::pci::PCIDevice* p) -> bool { return p == &device; }
    auto kernel_buffer(std::span<std::byte>) -> bool { return true; }
    auto allocate(size_t bytes) -> dma::detail::Storage {
        EXPECT_FALSE(locked);
        if (allocation_calls++ >= fail_after) return {};
        auto* cpu = static_cast<std::byte*>(std::aligned_alloc(4096, bytes));
        if (cpu == nullptr) return {};
        std::memset(cpu, 0, bytes);
        dma::detail::Storage storage{cpu, next_address};
        next_address += bytes;
        backing.push_back({storage, bytes});
        return storage;
    }
    void free(dma::detail::Storage storage, size_t bytes) {
        EXPECT_FALSE(locked);
        auto it = std::find_if(backing.begin(), backing.end(), [&](auto const& b) { return b.storage.cpu == storage.cpu; });
        ASSERT_NE(it, backing.end());
        EXPECT_EQ(it->bytes, bytes);
        std::free(storage.cpu);
        backing.erase(it);
    }
    auto map(const ker::dev::pci::PCIDevice*, uint64_t address, size_t bytes, size_t, uint64_t mask, dma::Direction)
        -> dma::detail::Mapping {
        EXPECT_FALSE(locked);
        return dma::detail::extent_fits(address, bytes, mask) ? dma::detail::Mapping{.address = address}
                                                              : dma::detail::Mapping{.error = dma::Error::address_limit};
    }
    auto unmap(const ker::dev::pci::PCIDevice*, uint64_t, uint64_t, size_t, dma::Direction) -> bool {
        EXPECT_FALSE(locked);
        return true;
    }
    void copy(std::byte* to, const std::byte* from, size_t bytes) {
        EXPECT_FALSE(locked);
        ++copies;
        if (copy_observer) copy_observer();
        std::memcpy(to, from, bytes);
    }
    void for_device(std::byte*, size_t, dma::Direction) { EXPECT_FALSE(locked); }
    void for_cpu(std::byte*, size_t, dma::Direction) { EXPECT_FALSE(locked); }
    auto cpu(uint64_t address) -> std::byte* {
        for (auto const& b : backing)
            if (address >= b.storage.physical && address - b.storage.physical < b.bytes)
                return b.storage.cpu + (address - b.storage.physical);
        return nullptr;
    }
};
using Manager = dma::detail::Manager<Backend, 1024>;
std::unique_ptr<Backend> backend;
std::unique_ptr<Manager> manager;
std::vector<ker::net::PacketBuffer*> consumed;
}  // namespace
namespace ker::dev::dma {
auto allocate_coherent(const pci::PCIDevice* p, size_t b, size_t a, uint64_t m) -> AllocationResult {
    return manager->allocate_coherent(p, b, a, m);
}
auto allocate_streaming(const pci::PCIDevice* p, size_t b, Direction d, uint64_t m) -> AllocationResult {
    return manager->allocate_streaming(p, b, d, m);
}
auto bind_streaming(Handle h, std::span<std::byte> b, Direction d) -> Error { return manager->bind_streaming(h, b, d); }
auto prepare(Handle h) -> SubmissionResult { return manager->prepare(h); }
auto complete(Submission s, size_t b, Retirement p) -> Error { return manager->complete(s, b, p); }
auto release(Handle h) -> Error { return manager->release(h); }
auto quarantine(Handle h) -> Error { return manager->quarantine(h); }
}  // namespace ker::dev::dma
namespace ker::net {
void pkt_free(PacketBuffer* p) {
    EXPECT_EQ(std::count(consumed.begin(), consumed.end(), p), 0);
    consumed.push_back(p);
}
}  // namespace ker::net
namespace {
class VirtqueueDma : public testing::Test {
   protected:
    virtio::Virtqueue* q = nullptr;
    std::array<ker::net::PacketBuffer, 4> packets{};
    void SetUp() override {
        backend = std::make_unique<Backend>();
        manager = std::make_unique<Manager>(*backend);
        consumed.clear();
    }
    void TearDown() override {
        backend->allocation_allowed = true;
        backend->copy_observer = {};
        if (q != nullptr && !virtio::virtq_retire(q, true)) {
            // The emulated device is gone. Kernel quarantine is intentionally
            // permanent; only the host fixture disposes of its stopped world.
            delete q;
        }
        manager.reset();
        backend.reset();
    }
    void make(virtio::QueueKind kind, uint16_t size = 4) {
        q = virtio::virtq_alloc(&device, size, kind);
        ASSERT_NE(q, nullptr);
        ASSERT_TRUE(q->ring.expose());
    }
    auto submit(size_t packet, size_t bytes = 64) -> int {
        auto& p = packets[packet];
        p.data = p.storage.data();
        p.len = bytes;
        return virtio::virtq_add_buf(q, {reinterpret_cast<std::byte*>(p.data), bytes}, &p);
    }
    void used(uint16_t index, uint32_t id, uint32_t length) {
        auto& e = q->used->ring[index % q->size];
        e.id = id;
        e.len = length;
        q->used->idx = index + 1;
    }
};
TEST_F(VirtqueueDma, ReceiveCopiesOnlyValidatedBytesBeforeDescriptorReuse) {
    make(virtio::QueueKind::receive);
    auto& pkt = packets[0];
    pkt.storage.fill(0x55);
    pkt.lifetime_ctx = &device;
    size_t const ALLOCATIONS = backend->allocation_calls;
    backend->allocation_allowed = false;
    int const id = submit(0);
    ASSERT_GE(id, 0);
    auto* bounce = backend->cpu(q->desc[id].addr);
    ASSERT_NE(bounce, nullptr);
    EXPECT_NE(bounce, reinterpret_cast<std::byte*>(pkt.data));
    std::memset(bounce, 0x9a, 64);
    backend->copy_observer = [&] {
        EXPECT_EQ(q->num_free, 3);
        EXPECT_EQ(q->pkt_map[id], &pkt);
        EXPECT_NE(q->packet_dma[id].submission.sequence, 0U);
    };
    used(0, id, 17);
    uint32_t length = UINT32_MAX;
    EXPECT_EQ(virtio::virtq_get_buf(q, &length), id);
    EXPECT_EQ(length, 17U);
    EXPECT_EQ(q->num_free, 4);
    EXPECT_EQ(q->packet_dma[id].submission.sequence, 0U);
    EXPECT_TRUE(std::all_of(pkt.storage.begin(), pkt.storage.begin() + 17, [](auto b) { return b == 0x9a; }));
    EXPECT_TRUE(std::all_of(pkt.storage.begin() + 17, pkt.storage.end(), [](auto b) { return b == 0x55; }));
    EXPECT_EQ(pkt.lifetime_ctx, &device);
    EXPECT_EQ(backend->allocation_calls, ALLOCATIONS);
    EXPECT_EQ(backend->copies, 1U);
}
TEST_F(VirtqueueDma, TransmitCopiesBeforePublicationAndNeverCopiesBack) {
    make(virtio::QueueKind::transmit);
    packets[0].storage.fill(0x42);
    backend->copy_observer = [&] {
        EXPECT_EQ(q->avail->idx, 0);
        EXPECT_EQ(q->num_free, 4);
    };
    int const id = submit(0);
    ASSERT_GE(id, 0);
    auto* bounce = backend->cpu(q->desc[id].addr);
    ASSERT_NE(bounce, nullptr);
    EXPECT_EQ(bounce[63], std::byte{0x42});
    packets[0].storage[0] = 0x91;
    EXPECT_EQ(bounce[0], std::byte{0x42});
    std::memset(bounce, 0xcc, 64);
    backend->copy_observer = {};
    used(0, id, UINT32_MAX);  // legacy TX length is not a receive extent
    uint32_t length = 123;
    EXPECT_EQ(virtio::virtq_get_buf(q, &length), id);
    EXPECT_EQ(length, 0U);
    EXPECT_EQ(packets[0].storage[0], 0x91);
    EXPECT_EQ(backend->copies, 1U);
}
TEST_F(VirtqueueDma, BadIdLengthAndPendingCountStopWithoutCopyOrReuse) {
    for (int fault = 0; fault < 3; ++fault) {
        make(virtio::QueueKind::receive);
        int const id = submit(0);
        ASSERT_GE(id, 0);
        used(0, fault == 0 ? 999 : id, fault == 1 ? 65 : 32);
        if (fault == 2) q->used->idx = 2;
        uint32_t length = 1234;
        EXPECT_EQ(virtio::virtq_get_buf(q, &length), virtio::VIRTQ_BAD_BUF);
        EXPECT_TRUE(q->failed.load());
        EXPECT_EQ(q->num_free, 3);
        EXPECT_EQ(q->last_used_idx, 0);
        EXPECT_EQ(length, 1234U);
        EXPECT_EQ(backend->copies, 0U);
        EXPECT_EQ(q->pkt_map[id], &packets[0]);
        EXPECT_EQ(virtio::virtq_get_buf(q, nullptr), virtio::VIRTQ_NO_BUF);
        EXPECT_FALSE(virtio::virtq_has_pending(q));
        EXPECT_LT(submit(1), 0);
        EXPECT_TRUE(virtio::virtq_retire(q, true));
        EXPECT_EQ(manager->stats().regions, 0U);
        consumed.clear();
    }
}
TEST_F(VirtqueueDma, DuplicateUsedEntryCannotRetireAnotherOutstandingPacket) {
    make(virtio::QueueKind::receive);
    int const first = submit(0), second = submit(1);
    ASSERT_GE(second, 0);
    used(0, first, 0);
    EXPECT_EQ(virtio::virtq_get_buf(q, nullptr), first);
    q->pkt_map[first] = nullptr;
    ker::net::pkt_free(&packets[0]);
    used(1, first, 0);
    EXPECT_EQ(virtio::virtq_get_buf(q, nullptr), virtio::VIRTQ_BAD_BUF);
    EXPECT_EQ(q->pkt_map[second], &packets[1]);
    EXPECT_NE(q->packet_dma[second].submission.sequence, 0U);
    EXPECT_EQ(consumed.size(), 1U);
}
TEST_F(VirtqueueDma, RingIndicesWrapWhileSubmissionGenerationAdvances) {
    make(virtio::QueueKind::receive);
    q->avail->idx = UINT16_MAX;
    q->last_used_idx = UINT16_MAX;
    q->used->idx = UINT16_MAX;
    int const id = submit(0);
    ASSERT_GE(id, 0);
    auto const SEQUENCE = q->packet_dma[id].submission.sequence;
    EXPECT_EQ(q->avail->idx, 0);
    used(UINT16_MAX, id, 0);
    EXPECT_EQ(virtio::virtq_get_buf(q, nullptr), id);
    EXPECT_EQ(q->last_used_idx, 0);
    q->pkt_map[id] = nullptr;
    EXPECT_EQ(submit(0), id);
    EXPECT_GT(q->packet_dma[id].submission.sequence, SEQUENCE);
}
TEST_F(VirtqueueDma, EveryPartialProbeAllocationFailureUnwindsExactly) {
    for (size_t failure = 0; failure < 5; ++failure) {
        backend->allocation_calls = 0;
        backend->fail_after = failure;
        q = virtio::virtq_alloc(&device, 4, virtio::QueueKind::receive);
        EXPECT_EQ(q, nullptr);
        EXPECT_EQ(manager->stats().regions, 0U);
        EXPECT_TRUE(backend->backing.empty());
    }
}
TEST_F(VirtqueueDma, UnprovenResetPinsRingAndAllSlotsButDetachesBorrowedPackets) {
    make(virtio::QueueKind::receive);
    ASSERT_GE(submit(0), 0);
    auto const BEFORE = manager->stats();
    EXPECT_FALSE(virtio::virtq_retire(q, false));
    ASSERT_NE(q, nullptr);
    auto const AFTER = manager->stats();
    EXPECT_EQ(AFTER.regions, BEFORE.regions);
    EXPECT_EQ(AFTER.backing_pages, BEFORE.backing_pages);
    EXPECT_EQ(AFTER.quarantined, BEFORE.regions);
    EXPECT_EQ(consumed, (std::vector<ker::net::PacketBuffer*>{&packets[0]}));
    EXPECT_EQ(q->pkt_map[0], nullptr);
}
TEST_F(VirtqueueDma, MetadataNeverEntersBindingAndControlQueuesHaveNoPacketSlots) {
    make(virtio::QueueKind::receive);
    auto& p = packets[0];
    EXPECT_LT(virtio::virtq_add_buf(q, {reinterpret_cast<std::byte*>(&p.pool_chunk), 1}, &p), 0);
    EXPECT_LT(virtio::virtq_add_buf(q, {reinterpret_cast<std::byte*>(p.storage.data()), p.storage.size() + 1}, &p), 0);
    EXPECT_EQ(q->avail->idx, 0);
    EXPECT_EQ(q->num_free, 4);
    EXPECT_TRUE(virtio::virtq_retire(q, true));
    make(virtio::QueueKind::control);
    EXPECT_EQ(manager->stats().regions, 1U);
    EXPECT_EQ(manager->stats().streaming, 0U);
    EXPECT_LT(submit(0), 0);
    EXPECT_LT(sizeof(dma::detail::Manager<Backend, dma::REGISTRY_CAPACITY>), 2U * 1024 * 1024);
}
}  // namespace
