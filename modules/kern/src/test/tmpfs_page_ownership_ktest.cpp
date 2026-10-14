#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <new>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/swap.hpp>
#include <platform/security/credentials.hpp>
#include <platform/sys/mutex.hpp>
#include <test/fault_block_device.hpp>
#include <test/ktest.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/tmpfs.hpp>

namespace {
namespace mm = ker::mod::mm;
namespace phys = mm::phys;
namespace swap = mm::swap;
namespace tmpfs = ker::vfs::tmpfs;
namespace vfs = ker::vfs;
namespace security = ker::mod::security;
using MutexGuard = ker::mod::sys::MutexGuard;

constexpr size_t PAGE = mm::paging::PAGE_SIZE;
constexpr uint64_t SWAP_PAGES = 8;
constexpr const char* SWAP_NAME = "ktest-tmpfs-owned-page-reclaim";
constexpr std::array<uint8_t, 16> CONTENT{0x31, 0x5A, 0xC7, 0x04, 0x8F, 0x22, 0x71, 0xD9, 0xB3, 0x19, 0xE4, 0x65, 0x0B, 0xF2, 0x98, 0x46};

auto swap_stats() -> swap::SwapStats {
    swap::SwapStats out{};
    swap::get_stats(&out);
    return out;
}

auto same_swap_stats(const swap::SwapStats& left, const swap::SwapStats& right) -> bool {
    return left.total_bytes == right.total_bytes && left.free_bytes == right.free_bytes && left.used_bytes == right.used_bytes &&
           left.cached_bytes == right.cached_bytes && left.active_areas == right.active_areas;
}

// Exact owner totals, not total allocator population: KASAN and unrelated
// permanent kernel tables can grow while an actual fresh object is touched.
struct PageBalance {
    phys::PhysicalBalanceSnapshot before{};
    PageBalance() { phys::get_physical_balance_snapshot(before); }
    ~PageBalance() {
        phys::PhysicalBalanceSnapshot after{};
        phys::get_physical_balance_snapshot(after);
        for (auto owner : {mm::PhysicalPageOwner::TMPFS_DATA, mm::PhysicalPageOwner::SELFTEST}) {
            size_t const i = static_cast<size_t>(owner);
            KEXPECT_EQ(after.owners.at(i).pages, before.owners.at(i).pages);
            KEXPECT_EQ(after.owners.at(i).objects, before.owners.at(i).objects);
        }
    }
};

struct FrameHold {
    void* page{};
    FrameHold() = default;
    FrameHold(const FrameHold&) = delete;
    auto operator=(const FrameHold&) -> FrameHold& = delete;
    ~FrameHold() { static_cast<void>(reset()); }
    auto acquire(tmpfs::TmpNode& node) -> bool {
        if (page != nullptr) return false;
        MutexGuard guard(node.io_lock);
        if (node.page_count == 0 || node.pages[0].state != tmpfs::TmpPageState::RESIDENT || node.pages[0].data == nullptr) return false;
        if (!phys::page_ref_try_inc(node.pages[0].data)) return false;
        page = node.pages[0].data;
        return true;
    }
    auto reset() -> uint32_t {
        void* const retired = page;
        page = nullptr;
        return retired != nullptr ? phys::page_ref_dec(retired) : 0;
    }
};

struct NodeSnapshot {
    size_t size{}, descriptors{}, charged{}, resident{};
    tmpfs::TmpPageState state = tmpfs::TmpPageState::HOLE;
    void* frame{};
    swap::SwapSlot slot{};
};

auto snapshot(tmpfs::TmpNode& node) -> NodeSnapshot {
    MutexGuard guard(node.io_lock);
    NodeSnapshot out{.size = node.size, .descriptors = node.page_count, .charged = node.charged_pages, .resident = node.resident_pages};
    if (node.page_count != 0) {
        out.state = node.pages[0].state;
        out.frame = node.pages[0].data;
        out.slot = node.pages[0].swap_slot;
    }
    return out;
}

// No FD table or global mount is used. A real backend File keeps the canonical
// node open until it closes; the private mount outlives that final close.
struct TmpFile {
    tmpfs::TmpNode* root{};
    tmpfs::TmpfsMount* mount{};
    vfs::File* file{};
    tmpfs::TmpNode* node{};
    TmpFile() = default;
    TmpFile(const TmpFile&) = delete;
    auto operator=(const TmpFile&) -> TmpFile& = delete;
    ~TmpFile() { reset(); }
    auto init() -> bool {
        root = tmpfs::create_root_node();
        if (root == nullptr) return false;
        int error = 0;
        mount = tmpfs::create_mount_context(root, "size=4K", false, &error);
        if (mount == nullptr || error != 0) return false;
        file = tmpfs::tmpfs_open_path(security::Snapshot::bootstrap_root(), root, "owned-page", vfs::O_CREAT | 2, 0600, &error);
        if (file == nullptr || error != 0) return false;
        file->fops = tmpfs::get_tmpfs_fops();
        node = static_cast<tmpfs::TmpNode*>(file->private_data);
        return node != nullptr;
    }
    void close_file() {
        auto* const retired = file;
        file = nullptr;
        if (retired != nullptr) vfs::vfs_put_file(retired);
    }
    void reset() {
        close_file();
        node = nullptr;
        tmpfs::tmpfs_lock_tree();
        if (mount != nullptr) {
            auto* const retired = mount;
            mount = nullptr;
            root = nullptr;
            tmpfs::destroy_mount_context(retired);
        } else if (root != nullptr) {
            auto* const retired = root;
            root = nullptr;
            tmpfs::tmpfs_free_node(retired);
        }
        tmpfs::tmpfs_unlock_tree();
    }
    auto used_bytes() const -> size_t {
        MutexGuard guard(mount->accounting_lock);
        return mount->used_bytes;
    }
    auto truncate(size_t size) -> int { return file->fops->vfs_truncate(file, static_cast<off_t>(size)); }
    auto add_alias() -> tmpfs::TmpNode* {
        tmpfs::tmpfs_lock_tree();
        auto* const alias = tmpfs::tmpfs_create_hardlink(root, "owned-alias", node);
        tmpfs::tmpfs_unlock_tree();
        return alias;
    }
    auto unlink() -> int {
        tmpfs::TmpfsLookupHandle lookup;
        int const acquired = tmpfs::tmpfs_acquire_lookup(security::Snapshot::bootstrap_root(), root, "owned-page", &lookup);
        if (acquired != 0) return acquired;
        return tmpfs::tmpfs_unlink_lookup(security::Snapshot::bootstrap_root(), lookup);
    }
};

struct SlotHold {
    swap::SwapSlot slot{};
    SlotHold() = default;
    SlotHold(const SlotHold&) = delete;
    auto operator=(const SlotHold&) -> SlotHold& = delete;
    ~SlotHold() { KEXPECT_EQ(reset(), 0); }
    auto acquire() -> int { return swap::slot_valid(slot) ? -EBUSY : swap::allocate_slot(&slot); }
    auto reset() -> int {
        if (!swap::slot_valid(slot)) return 0;
        int const result = swap::free_slot(slot);
        if (result == 0) slot = swap::invalid_slot();
        return result;
    }
};

struct FaultSwap {
    swap::SwapStats before{swap_stats()};
    void* storage{};
    ker::test::FaultBlockDevice* device{};
    bool active{};
    FaultSwap() = default;
    FaultSwap(const FaultSwap&) = delete;
    auto operator=(const FaultSwap&) -> FaultSwap& = delete;
    ~FaultSwap() { static_cast<void>(reset()); }
    auto init() -> bool {
        // The allocator chooses the first active area. Refuse foreign swap
        // admission rather than send this test's I/O to another device.
        if (before.active_areas != 0 || !same_swap_stats(before, swap_stats())) return false;
        storage = phys::page_alloc_may_fail(mm::PhysicalPageOwner::SELFTEST, sizeof(ker::test::FaultBlockDevice), "tmpfs-fault-swap");
        if (storage == nullptr) return false;
        device = ::new (storage) ker::test::FaultBlockDevice(PAGE, SWAP_PAGES);
        if (!device->valid()) return false;
        swap::SwapExtent const extent{.device = device->device(), .start_block = 0, .page_count = SWAP_PAGES};
        int const result = swap::activate_extents(SWAP_NAME, &extent, 1);
        active = result == 0;
        return active;
    }
    auto reset() -> bool {
        if (active) {
            device->clear_fault();
            device->clear_unavailable();
            int const result = swap::swapoff_path(SWAP_NAME);
            KEXPECT_EQ(result, 0);
            // An unexpectedly live area must keep its physical device alive.
            // Preserve bounded backing on refusal; never leave dangling I/O.
            if (result != 0) return false;
            active = false;
            KEXPECT_TRUE(same_swap_stats(before, swap_stats()));
        }
        if (device != nullptr) {
            device->~FaultBlockDevice();
            device = nullptr;
        }
        phys::page_free(storage);
        storage = nullptr;
        return true;
    }
};
}  // namespace

KTEST(TmpfsPageOwnership, SparseTailShrinkKeepsRetainedCharge) {
    PageBalance balance;
    FrameHold observer;
    TmpFile owner;
    KREQUIRE_TRUE(owner.init());
    KREQUIRE_EQ(tmpfs::tmpfs_write(owner.file, CONTENT.data(), CONTENT.size(), 0), static_cast<ssize_t>(CONTENT.size()));
    KREQUIRE_TRUE(observer.acquire(*owner.node));
    KREQUIRE_EQ(owner.truncate(3 * PAGE), 0);
    KREQUIRE_EQ(snapshot(*owner.node).descriptors, 3U);
    {
        MutexGuard guard(owner.node->io_lock);
        KEXPECT_EQ(owner.node->pages[1].state, tmpfs::TmpPageState::HOLE);
        KEXPECT_EQ(owner.node->pages[2].state, tmpfs::TmpPageState::HOLE);
    }
    KEXPECT_EQ(owner.used_bytes(), PAGE);
    KREQUIRE_EQ(owner.truncate(PAGE), 0);
    auto const retained = snapshot(*owner.node);
    KEXPECT_EQ(retained.descriptors, 1U);
    KEXPECT_EQ(retained.charged, 1U);
    KEXPECT_EQ(retained.resident, 1U);
    KEXPECT_EQ(retained.frame, observer.page);
    KEXPECT_EQ(owner.used_bytes(), PAGE);
    KEXPECT_EQ(phys::page_ref_get(observer.page), 2U);
    uint8_t byte = 0xD1;
    KEXPECT_EQ(tmpfs::tmpfs_write(owner.file, &byte, 1, 2 * PAGE), static_cast<ssize_t>(-ENOSPC));
    KEXPECT_EQ(tmpfs::tmpfs_get_size(owner.file), PAGE);
    KEXPECT_EQ(owner.used_bytes(), PAGE);
    KEXPECT_EQ(snapshot(*owner.node).charged, 1U);
    KEXPECT_TRUE(std::memcmp(observer.page, CONTENT.data(), CONTENT.size()) == 0);
}

KTEST(TmpfsPageOwnership, InodeRetirementDropsOnlyItsFrameReference) {
    PageBalance balance;
    for (bool truncate_first : {false, true}) {
        FrameHold observer;
        TmpFile owner;
        KREQUIRE_TRUE(owner.init());
        KREQUIRE_EQ(tmpfs::tmpfs_write(owner.file, CONTENT.data(), CONTENT.size(), 0), static_cast<ssize_t>(CONTENT.size()));
        KREQUIRE_TRUE(observer.acquire(*owner.node));
        KEXPECT_EQ(phys::page_ref_get(observer.page), 2U);
        if (truncate_first) {
            KREQUIRE_EQ(owner.truncate(0), 0);
            KEXPECT_EQ(snapshot(*owner.node).descriptors, 0U);
            KEXPECT_EQ(snapshot(*owner.node).charged, 0U);
            KEXPECT_EQ(owner.used_bytes(), 0U);
        }
        owner.reset();
        KEXPECT_EQ(phys::page_ref_get(observer.page), 1U);
        KEXPECT_EQ(phys::page_kind_get(observer.page), mm::PageKind::NORMAL);
        KEXPECT_TRUE(std::memcmp(observer.page, CONTENT.data(), CONTENT.size()) == 0);
        KEXPECT_EQ(observer.reset(), 0U);
        // No allocator metadata is queried after that final decrement.
    }
}

KTEST(TmpfsPageOwnership, ReferencedReclaimRefusesBeforeSwapThenRetries) {
    PageBalance balance;
    FaultSwap swap_owner;
    KREQUIRE_TRUE(swap_owner.init());
    FrameHold observer;
    TmpFile owner;
    KREQUIRE_TRUE(owner.init());
    KREQUIRE_EQ(tmpfs::tmpfs_write(owner.file, CONTENT.data(), CONTENT.size(), 0), static_cast<ssize_t>(CONTENT.size()));
    KREQUIRE_TRUE(observer.acquire(*owner.node));
    auto* const alias = owner.add_alias();
    KREQUIRE_NE(alias, nullptr);
    SlotHold probe;
    KREQUIRE_EQ(probe.acquire(), 0);
    auto const first_slot = probe.slot;
    KREQUIRE_EQ(probe.reset(), 0);
    auto const before = swap_stats();
    swap_owner.device->clear_history();
    KEXPECT_EQ(tmpfs::selftest_tmpfs_evict_page(alias, 0), -EINVAL);
    KEXPECT_EQ(tmpfs::selftest_tmpfs_evict_page(owner.node, 0), -EAGAIN);
    auto const retained = snapshot(*owner.node);
    KREQUIRE_EQ(retained.state, tmpfs::TmpPageState::RESIDENT);
    KEXPECT_EQ(retained.frame, observer.page);
    KEXPECT_EQ(retained.charged, 1U);
    KEXPECT_EQ(retained.resident, 1U);
    KEXPECT_EQ(owner.used_bytes(), PAGE);
    KEXPECT_EQ(phys::page_ref_get(observer.page), 2U);
    KEXPECT_TRUE(same_swap_stats(before, swap_stats()));
    KEXPECT_EQ(swap_owner.device->event_count(), 0U);
    KREQUIRE_EQ(probe.acquire(), 0);
    KEXPECT_EQ(probe.slot.area, first_slot.area);
    KEXPECT_EQ(probe.slot.index, first_slot.index);
    KEXPECT_EQ(probe.slot.generation, first_slot.generation + 1);
    KREQUIRE_EQ(probe.reset(), 0);
    void* const frame = observer.page;
    KREQUIRE_EQ(observer.reset(), 1U);

    KREQUIRE_TRUE(swap_owner.device->fail_operation(ker::test::FaultBlockOperation::WRITE, 1, -EIO));
    KREQUIRE_EQ(tmpfs::selftest_tmpfs_evict_page(owner.node, 0), -EIO);
    auto const failed = snapshot(*owner.node);
    KREQUIRE_EQ(failed.state, tmpfs::TmpPageState::RESIDENT);
    KREQUIRE_EQ(failed.frame, frame);
    KEXPECT_EQ(failed.charged, 1U);
    KEXPECT_EQ(failed.resident, 1U);
    KEXPECT_EQ(owner.used_bytes(), PAGE);
    KEXPECT_TRUE(same_swap_stats(before, swap_stats()));
    FrameHold rollback_observer;
    KREQUIRE_TRUE(rollback_observer.acquire(*owner.node));
    KEXPECT_EQ(phys::page_ref_get(rollback_observer.page), 2U);
    KEXPECT_TRUE(std::memcmp(rollback_observer.page, CONTENT.data(), CONTENT.size()) == 0);
    KREQUIRE_EQ(rollback_observer.reset(), 1U);
    swap_owner.device->clear_fault();
    swap_owner.device->clear_history();
    KREQUIRE_EQ(tmpfs::selftest_tmpfs_evict_page(owner.node, 0), 0);
    auto const evicted = snapshot(*owner.node);
    KEXPECT_EQ(evicted.state, tmpfs::TmpPageState::SWAPPED);
    KEXPECT_NULL(evicted.frame);
    KEXPECT_TRUE(swap::slot_valid(evicted.slot));
    KEXPECT_EQ(evicted.slot.area, first_slot.area);
    KEXPECT_EQ(evicted.charged, 1U);
    KEXPECT_EQ(evicted.resident, 0U);
    KEXPECT_EQ(owner.used_bytes(), PAGE);
    KEXPECT_EQ(swap_stats().used_bytes, PAGE);
    KEXPECT_EQ(swap_stats().free_bytes + PAGE, before.free_bytes);
    KEXPECT_EQ(swap_stats().total_bytes, before.total_bytes);
    KEXPECT_EQ(swap_stats().active_areas, before.active_areas);
    KEXPECT_EQ(swap_owner.device->operation_count(ker::test::FaultBlockOperation::WRITE), 1U);
    KREQUIRE_TRUE(swap_owner.device->fail_read_prefix(1, 7, -EIO));
    std::array<uint8_t, CONTENT.size()> output{};
    output.fill(0xCC);
    KEXPECT_EQ(tmpfs::tmpfs_read(owner.file, output.data(), output.size(), 0), static_cast<ssize_t>(-EIO));
    KEXPECT_EQ(snapshot(*owner.node).state, tmpfs::TmpPageState::SWAPPED);
    KEXPECT_EQ(snapshot(*owner.node).slot.generation, evicted.slot.generation);
    KEXPECT_EQ(snapshot(*owner.node).charged, 1U);
    KEXPECT_EQ(owner.used_bytes(), PAGE);
    KEXPECT_EQ(swap_stats().used_bytes, PAGE);
    for (uint8_t byte : output) KEXPECT_EQ(byte, 0xCCU);
    swap_owner.device->clear_fault();
    KREQUIRE_EQ(tmpfs::tmpfs_read(owner.file, output.data(), output.size(), 0), static_cast<ssize_t>(output.size()));
    KEXPECT_TRUE(output == CONTENT);
    auto const paged_in = snapshot(*owner.node);
    KEXPECT_EQ(paged_in.state, tmpfs::TmpPageState::RESIDENT);
    KEXPECT_FALSE(swap::slot_valid(paged_in.slot));
    KEXPECT_EQ(paged_in.charged, 1U);
    KEXPECT_EQ(paged_in.resident, 1U);
    KEXPECT_EQ(owner.used_bytes(), PAGE);
    KEXPECT_TRUE(same_swap_stats(before, swap_stats()));
    // Frame may have been recycled by successful eviction; never probe it.
    owner.reset();
    KEXPECT_TRUE(swap_owner.reset());
}

KTEST(TmpfsPageOwnership, UnlinkedNodeFinalRetirementEndsMutexLifetime) {
    PageBalance balance;
    // Real final-close and NodeRef paths run under the diagnostic kernel's
    // KASAN. Explicit before-delete mutex ordering is also checked by the
    // actual-body Host regression; there is no after-free mutex probe here.
    for (unsigned i = 0; i < 8; ++i) {
        TmpFile owner;
        KREQUIRE_TRUE(owner.init());
        KREQUIRE_EQ(tmpfs::tmpfs_write(owner.file, CONTENT.data(), CONTENT.size(), 0), static_cast<ssize_t>(CONTENT.size()));
        auto node = tmpfs::tmpfs_acquire_node(owner.node);
        KREQUIRE_TRUE(node);
        KREQUIRE_EQ(owner.unlink(), 0);
        owner.close_file();
        KEXPECT_EQ(node.get()->open_count.load(), 0U);
        KEXPECT_EQ(node.get()->link_count.load(), 0U);
        KEXPECT_EQ(node.get()->lookup_ref_count.load(), 1U);
        KEXPECT_EQ(snapshot(*node.get()).resident, 1U);
        owner.node = nullptr;
        node.reset();
        KEXPECT_EQ(owner.used_bytes(), 0U);
        owner.reset();
    }
}
