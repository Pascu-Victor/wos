#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <platform/mm/paging.hpp>
#include <platform/mm/swap.hpp>
#include <test/fault_block_device.hpp>
#include <test/ktest.hpp>
#include <vfs/fs/tmpfs.hpp>

namespace {

constexpr size_t PAGE_SIZE = ker::mod::mm::paging::PAGE_SIZE;
constexpr uint64_t SWAP_PAGES = 8;

using Page = std::array<uint8_t, PAGE_SIZE>;

struct DrainConsumerContext {
    ker::mod::mm::swap::SwapSlot slot{};
    Page output{};
    int injected_error{};
    size_t calls{};
};

DrainConsumerContext drain_consumer_context;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto migrate_test_slot(void* opaque, uint32_t area_id) -> int {
    auto* context = static_cast<DrainConsumerContext*>(opaque);
    if (context == nullptr || context->slot.area != area_id) {
        return 0;
    }
    context->calls++;
    if (context->injected_error < 0) {
        return context->injected_error;
    }
    int const READ_RET = ker::mod::mm::swap::read_slot(context->slot, context->output.data());
    if (READ_RET < 0) {
        return READ_RET;
    }
    return ker::mod::mm::swap::free_slot(context->slot);
}

auto new_fault_swap() -> std::unique_ptr<ker::test::FaultBlockDevice> {
    return std::unique_ptr<ker::test::FaultBlockDevice>(new (std::nothrow) ker::test::FaultBlockDevice(PAGE_SIZE, SWAP_PAGES));
}

auto activate_fault_swap(ker::test::FaultBlockDevice& storage, const char* name) -> int {
    ker::mod::mm::swap::SwapExtent const EXTENT{.device = storage.device(), .start_block = 0, .page_count = SWAP_PAGES};
    return ker::mod::mm::swap::activate_extents(name, &EXTENT, 1);
}

auto page_is_zero(const Page& page) -> bool {
    for (uint8_t byte : page) {
        if (byte != 0) {
            return false;
        }
    }
    return true;
}

}  // namespace

KTEST(Swap, GenerationRejectsAbaAndCorruptionFailsClosed) {
    auto storage = new_fault_swap();
    KREQUIRE_NE(storage.get(), nullptr);
    KREQUIRE_TRUE(storage->valid());
    constexpr const char* NAME = "ktest-swap-generation";
    KREQUIRE_EQ(activate_fault_swap(*storage, NAME), 0);

    ker::mod::mm::swap::SwapSlot first{};
    KREQUIRE_EQ(ker::mod::mm::swap::allocate_slot(&first), 0);
    KEXPECT_TRUE(ker::mod::mm::swap::slot_valid(first));

    Page source{};
    Page output{};
    source.fill(0x6D);
    KREQUIRE_EQ(ker::mod::mm::swap::write_slot(first, source.data()), 0);
    KEXPECT_EQ(ker::mod::mm::swap::read_slot(first, output.data()), 0);
    KEXPECT_TRUE(std::memcmp(source.data(), output.data(), PAGE_SIZE) == 0);

    storage->clear_history();
    KREQUIRE_TRUE(storage->corrupt_read(1, 73, 0x80));
    output.fill(0xCC);
    KEXPECT_EQ(ker::mod::mm::swap::read_slot(first, output.data()), -EILSEQ);
    KEXPECT_TRUE(page_is_zero(output));

    KREQUIRE_EQ(ker::mod::mm::swap::free_slot(first), 0);
    KEXPECT_EQ(ker::mod::mm::swap::free_slot(first), -EINVAL);

    ker::mod::mm::swap::SwapSlot reused{};
    KREQUIRE_EQ(ker::mod::mm::swap::allocate_slot(&reused), 0);
    KEXPECT_EQ(reused.area, first.area);
    KEXPECT_EQ(reused.index, first.index);
    KEXPECT_NE(reused.generation, first.generation);
    KEXPECT_EQ(ker::mod::mm::swap::read_slot(first, output.data()), -ESTALE);
    KEXPECT_EQ(ker::mod::mm::swap::free_slot(first), -ESTALE);
    KEXPECT_EQ(ker::mod::mm::swap::swapoff_path(NAME), -EBUSY);

    storage->set_unavailable(-ENODEV);
    KEXPECT_EQ(ker::mod::mm::swap::write_slot(reused, source.data()), -ENODEV);
    storage->clear_unavailable();
    KEXPECT_EQ(ker::mod::mm::swap::read_slot(reused, output.data()), -ENODATA);
    KREQUIRE_EQ(ker::mod::mm::swap::write_slot(reused, source.data()), 0);

    KEXPECT_EQ(ker::mod::mm::swap::free_slot(reused), 0);
    KEXPECT_EQ(ker::mod::mm::swap::swapoff_path(NAME), 0);
}

KTEST(Swap, PartialReadFailureIsDeterministicAndNeverExposed) {
    auto storage = new_fault_swap();
    KREQUIRE_NE(storage.get(), nullptr);
    KREQUIRE_TRUE(storage->valid());
    constexpr const char* NAME = "ktest-swap-partial-read";
    KREQUIRE_EQ(activate_fault_swap(*storage, NAME), 0);

    ker::mod::mm::swap::SwapSlot slot{};
    KREQUIRE_EQ(ker::mod::mm::swap::allocate_slot(&slot), 0);
    Page source{};
    Page output{};
    source.fill(0xA7);
    KREQUIRE_EQ(ker::mod::mm::swap::write_slot(slot, source.data()), 0);

    storage->clear_history();
    KREQUIRE_TRUE(storage->fail_read_prefix(1, 19, -EIO));
    output.fill(0xCC);
    KEXPECT_EQ(ker::mod::mm::swap::read_slot(slot, output.data()), -EIO);
    KEXPECT_TRUE(page_is_zero(output));
    KREQUIRE_EQ(storage->event_count(), static_cast<size_t>(1));
    KEXPECT_EQ(storage->event(0)->operation, ker::test::FaultBlockOperation::READ);
    KEXPECT_EQ(storage->event(0)->transferred_bytes, static_cast<size_t>(19));
    KEXPECT_EQ(storage->event(0)->result, -EIO);

    KEXPECT_EQ(ker::mod::mm::swap::free_slot(slot), 0);
    KEXPECT_EQ(ker::mod::mm::swap::swapoff_path(NAME), 0);
}

KTEST(Swap, SwapoffDrainsOutsideLockAndRestoresActiveOnFailure) {
    auto storage = new_fault_swap();
    KREQUIRE_NE(storage.get(), nullptr);
    KREQUIRE_TRUE(storage->valid());
    constexpr const char* NAME = "ktest-swap-draining";
    KREQUIRE_EQ(activate_fault_swap(*storage, NAME), 0);

    Page source{};
    source.fill(0xD4);
    drain_consumer_context = {};
    KREQUIRE_EQ(ker::mod::mm::swap::allocate_slot(&drain_consumer_context.slot), 0);
    KREQUIRE_EQ(ker::mod::mm::swap::write_slot(drain_consumer_context.slot, source.data()), 0);
    ker::mod::mm::swap::SwapConsumer const CONSUMER{
        .name = "ktest-drain",
        .context = &drain_consumer_context,
        .migrate_area = migrate_test_slot,
    };
    KREQUIRE_TRUE(ker::mod::mm::swap::register_consumer(CONSUMER));

    drain_consumer_context.injected_error = -EIO;
    KEXPECT_EQ(ker::mod::mm::swap::swapoff_path(NAME), -EIO);
    KEXPECT_EQ(drain_consumer_context.calls, static_cast<size_t>(1));

    drain_consumer_context.injected_error = 0;
    KEXPECT_EQ(ker::mod::mm::swap::swapoff_path(NAME), 0);
    KEXPECT_EQ(drain_consumer_context.calls, static_cast<size_t>(2));
    KEXPECT_TRUE(std::memcmp(source.data(), drain_consumer_context.output.data(), PAGE_SIZE) == 0);
}

KTEST(Swap, TmpfsSwapoffFaultIsTransactionalThenPagesIn) {
    ker::vfs::tmpfs::register_tmpfs();
    auto storage = new_fault_swap();
    KREQUIRE_NE(storage.get(), nullptr);
    KREQUIRE_TRUE(storage->valid());
    constexpr const char* NAME = "ktest-swap-tmpfs-drain";
    KREQUIRE_EQ(activate_fault_swap(*storage, NAME), 0);

    Page source{};
    source.fill(0x39);
    ker::mod::mm::swap::SwapSlot slot{};
    KREQUIRE_EQ(ker::mod::mm::swap::allocate_slot(&slot), 0);
    KREQUIRE_EQ(ker::mod::mm::swap::write_slot(slot, source.data()), 0);

    auto* root = ker::vfs::tmpfs::create_root_node();
    KREQUIRE_NE(root, nullptr);
    int mount_error = 0;
    auto* mount = ker::vfs::tmpfs::create_mount_context(root, "size=1M", false, &mount_error);
    KREQUIRE_NE(mount, nullptr);
    KREQUIRE_EQ(mount_error, 0);
    auto* node = ker::vfs::tmpfs::tmpfs_create_file(root, "swapoff-page");
    KREQUIRE_NE(node, nullptr);
    node->pages = new (std::nothrow) ker::vfs::tmpfs::TmpPage[1];
    KREQUIRE_NE(node->pages, nullptr);
    node->page_count = 1;
    node->charged_pages = 1;
    node->size = PAGE_SIZE;
    node->pages[0].state = ker::vfs::tmpfs::TmpPageState::SWAPPED;
    node->pages[0].swap_slot = slot;

    storage->set_unavailable(-ENODEV);
    KEXPECT_EQ(ker::mod::mm::swap::swapoff_path(NAME), -ENODEV);
    KEXPECT_EQ(node->pages[0].state, ker::vfs::tmpfs::TmpPageState::SWAPPED);
    KEXPECT_TRUE(ker::mod::mm::swap::slot_valid(node->pages[0].swap_slot));

    storage->clear_unavailable();
    KEXPECT_EQ(ker::mod::mm::swap::swapoff_path(NAME), 0);
    KEXPECT_EQ(node->pages[0].state, ker::vfs::tmpfs::TmpPageState::RESIDENT);
    KEXPECT_FALSE(ker::mod::mm::swap::slot_valid(node->pages[0].swap_slot));
    KEXPECT_TRUE(node->pages[0].data != nullptr && std::memcmp(node->pages[0].data, source.data(), PAGE_SIZE) == 0);

    ker::vfs::tmpfs::destroy_mount_context(mount);
}
