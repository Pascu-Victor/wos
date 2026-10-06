#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/block_device.hpp>
#include <dev/block_request.hpp>
#include <memory>
#include <new>
#include <platform/mm/paging.hpp>
#include <platform/mm/swap.hpp>
#include <platform/sys/spinlock.hpp>
#include <test/ktest.hpp>

namespace {
namespace swap = ker::mod::mm::swap;
using namespace ker::dev;
constexpr size_t PAGE_BYTES = ker::mod::mm::paging::PAGE_SIZE;
using Page = std::array<uint8_t, PAGE_BYTES>;

// Permanent provider storage is required by the production queue registry.
// Test commands retain SG pointers until this controller is explicitly stepped.
struct DelayedSwapDevice {
    BlockDevice device{};
    ker::mod::sys::Spinlock lock;
    std::array<uint8_t, PAGE_BYTES * 2> media{};
    BlockRequest* pending{};
    BlockRequestToken token{};

    static auto read(BlockDevice* device, uint64_t lba, size_t count, void* buffer) -> int {
        auto& self = *static_cast<DelayedSwapDevice*>(device->private_data);
        std::memcpy(buffer, self.media.data() + lba * device->block_size, count * device->block_size);
        return 0;
    }
    static auto write(BlockDevice* device, uint64_t lba, size_t count, const void* buffer) -> int {
        auto& self = *static_cast<DelayedSwapDevice*>(device->private_data);
        std::memcpy(self.media.data() + lba * device->block_size, buffer, count * device->block_size);
        return 0;
    }
    static auto submit(BlockDevice* device, BlockRequest& request) -> int {
        auto& self = *static_cast<DelayedSwapDevice*>(device->private_data);
        uint64_t const FLAGS = self.lock.lock_irqsave();
        int result = -EAGAIN;
        if (self.pending == nullptr) {
            self.pending = &request;
            self.token = request.token;
            result = 0;
        }
        self.lock.unlock_irqrestore(FLAGS);
        return result;
    }
    static void service(BlockDevice*) {}
    static void cancel(BlockDevice*, BlockRequestToken, int) {
        // Cancellation does not establish quiescence; only retire() does.
    }
    auto retire(BlockRequestToken expected, int status = 0) -> bool {
        uint64_t const FLAGS = lock.lock_irqsave();
        auto* request = pending != nullptr && token == expected ? pending : nullptr;
        size_t bytes = 0;
        if (request != nullptr) {
            bytes = status == 0 ? request->description.bytes : 0;
            if (status == 0 && request->description.op != BlockRequestOperation::FLUSH) {
                size_t offset = request->description.lba * device.block_size;
                for (size_t i = 0; i < request->segment_count; ++i) {
                    auto const& segment = request->segments[i];
                    if (request->description.op == BlockRequestOperation::WRITE) {
                        std::memcpy(media.data() + offset, segment.data, segment.size);
                    } else {
                        std::memcpy(segment.data, media.data() + offset, segment.size);
                    }
                    offset += segment.size;
                }
            }
            pending = nullptr;
        }
        lock.unlock_irqrestore(FLAGS);
        return request != nullptr && block_request_complete(&device, expected, status, bytes);
    }
};
DelayedSwapDevice storage;

struct TestPages {
    Page source{};
    Page destination{};
};

auto all_zero(const Page& page) -> bool {
    for (uint8_t byte : page) {
        if (byte != 0) return false;
    }
    return true;
}
}  // namespace

KTEST(SwapAsync, DelayedIoPinsGenerationAndAreaUntilFinish) {
    constexpr const char* NAME = "ktest-swap-async-delayed";
    auto pages = std::unique_ptr<TestPages>(new (std::nothrow) TestPages);
    KREQUIRE_NE(pages.get(), nullptr);
    pages->source.fill(0x6D);
    storage.device.block_size = 512;
    storage.device.total_blocks = storage.media.size() / 512;
    storage.device.private_data = &storage;
    storage.device.read_blocks = DelayedSwapDevice::read;
    storage.device.write_blocks = DelayedSwapDevice::write;
    std::memcpy(storage.device.name.data(), NAME, std::strlen(NAME) + 1);
    static const BlockAsyncOps OPS{DelayedSwapDevice::submit, DelayedSwapDevice::service, DelayedSwapDevice::cancel};
    // Deliberately smaller than the anonymous page-in window: one page/slot.
    KREQUIRE_EQ(block_queue_configure(&storage.device, {1, PAGE_BYTES, 32, 1, PAGE_BYTES}, &OPS), 0);
    swap::SwapExtent const EXTENT{&storage.device, 0, 2};
    KREQUIRE_EQ(swap::activate_extents(NAME, &EXTENT, 1), 0);
    swap::SwapSlot first{};
    KREQUIRE_EQ(swap::allocate_slot(&first), 0);

    swap::SlotIo write;
    KREQUIRE_EQ(swap::submit_slot_io(write, first, pages->source.data(), true), 0);
    KEXPECT_EQ(swap::free_slot(first), -EBUSY);
    KEXPECT_EQ(swap::swapoff_path(NAME), -EBUSY);
    block_queue_service(&storage.device);
    KEXPECT_FALSE(write.request.completed.load(std::memory_order_acquire));
    KEXPECT_TRUE(storage.retire(write.request.token));
    block_queue_service(&storage.device);
    KEXPECT_EQ(swap::free_slot(first), -EBUSY);  // Transport done, slot publication not done.
    KREQUIRE_EQ(swap::finish_slot_io(write), 0);

    // No-ownership backpressure must preserve the already stored generation
    // and checksum. The unrelated retained owner uses the whole page budget.
    BlockRequest blocker;
    BlockSegment const SEGMENT{pages->destination.data(), PAGE_BYTES};
    KREQUIRE_EQ(
        block_request_submit(&storage.device, blocker, BlockRequestOperation::READ, PAGE_BYTES / 512, PAGE_BYTES / 512, {&SEGMENT, 1}), 0);
    swap::SlotIo rejected;
    KEXPECT_EQ(swap::submit_slot_io(rejected, first, pages->source.data(), true), -EAGAIN);
    KEXPECT_FALSE(rejected.active);
    KEXPECT_EQ(rejected.request.queue, nullptr);
    KEXPECT_EQ(block_request_cancel(blocker), 0);
    block_queue_service(&storage.device);
    static_cast<void>(storage.retire(blocker.token, -ECANCELED));
    KEXPECT_EQ(block_request_wait(blocker), -ECANCELED);
    KEXPECT_EQ(block_request_release(blocker), 0);

    swap::SlotIo read;
    KREQUIRE_EQ(swap::submit_slot_io(read, first, pages->destination.data(), false), 0);
    block_queue_service(&storage.device);
    KEXPECT_TRUE(storage.retire(read.request.token));
    KREQUIRE_EQ(swap::finish_slot_io(read), 0);
    KEXPECT_TRUE(std::memcmp(pages->source.data(), pages->destination.data(), PAGE_BYTES) == 0);

    storage.media[73] ^= 0x80;
    pages->destination.fill(0xCC);
    KREQUIRE_EQ(swap::submit_slot_io(read, first, pages->destination.data(), false), 0);
    block_queue_service(&storage.device);
    KEXPECT_TRUE(storage.retire(read.request.token));
    KEXPECT_EQ(swap::finish_slot_io(read), -EILSEQ);
    KEXPECT_TRUE(all_zero(pages->destination));
    storage.media[73] ^= 0x80;

    pages->destination.fill(0xCC);
    KREQUIRE_EQ(swap::submit_slot_io(read, first, pages->destination.data(), false), 0);
    block_queue_service(&storage.device);
    BlockRequestToken const OLD = read.request.token;
    KREQUIRE_EQ(block_queue_begin_reset(&storage.device), 0);
    KEXPECT_EQ(block_queue_finish_reset(&storage.device), -EBUSY);
    KEXPECT_EQ(swap::free_slot(first), -EBUSY);
    KEXPECT_TRUE(storage.retire(OLD));
    block_queue_service(&storage.device);
    KREQUIRE_EQ(block_queue_finish_reset(&storage.device), 0);
    KEXPECT_EQ(swap::finish_slot_io(read), -EIO);
    KEXPECT_TRUE(all_zero(pages->destination));
    KEXPECT_FALSE(block_request_complete(&storage.device, OLD, 0, PAGE_BYTES));
    KREQUIRE_EQ(swap::free_slot(first), 0);
    swap::SwapSlot reused{};
    KREQUIRE_EQ(swap::allocate_slot(&reused), 0);
    KEXPECT_EQ(reused.index, first.index);
    KEXPECT_NE(reused.generation, first.generation);
    KEXPECT_EQ(swap::submit_slot_io(read, first, pages->destination.data(), false), -ESTALE);
    KEXPECT_EQ(swap::free_slot(reused), 0);
    KEXPECT_EQ(swap::swapoff_path(NAME), 0);
    block_queue_close(&storage.device);
    KEXPECT_EQ(block_queue_drain(&storage.device), 0);
}
