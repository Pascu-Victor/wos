// Private mapping permission, cache ownership and COW commit regressions.
#ifdef WOS_SELFTEST

#include <abi/callnums/vmem.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/swap.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/usercopy.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <test/fault_block_device.hpp>
#include <test/ktest.hpp>
#include <vfs/file.hpp>
#include <vfs/vfs.hpp>

namespace {
namespace addr = ker::mod::mm::addr;
namespace mm = ker::mod::mm;
namespace paging = ker::mod::mm::paging;
namespace phys = ker::mod::mm::phys;
namespace task = ker::mod::sched::task;
namespace usercopy = ker::mod::sys::usercopy;
namespace virt = ker::mod::mm::virt;
namespace swap = ker::mod::mm::swap;
namespace vfs = ker::vfs;
namespace vmem = ker::syscall::vmem;
namespace vmabi = ker::abi::vmem;

constexpr uint64_t VA = 0x5B000000ULL;
constexpr uint8_t ORIGINAL = 0x39;
constexpr uint8_t REPLACEMENT = 0xA7;
constexpr size_t PAGE = paging::PAGE_SIZE;
using Bytes = std::array<uint8_t, PAGE>;

auto physical(const void* page) -> uint64_t { return reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(page))); }
auto frame(paging::PageTable* root) -> uint8_t* {
    uint64_t const p = virt::translate(root, VA);
    return p == virt::PADDR_INVALID ? nullptr : reinterpret_cast<uint8_t*>(addr::get_virt_pointer(p));
}
auto full_frame(const void* page, uint8_t fill, uint8_t first) -> bool {
    if (page == nullptr) return false;
    auto const* bytes = static_cast<const uint8_t*>(page);
    if (bytes[0] != first) return false;
    for (size_t i = 1; i < PAGE; ++i) {
        if (bytes[i] != fill) return false;
    }
    return true;
}
auto full_read(task::Task& subject, uint8_t fill, uint8_t first) -> bool {
    std::unique_ptr<Bytes> bytes(new (std::nothrow) Bytes{});
    return bytes != nullptr && usercopy::copy_from_task(subject, VA, bytes->data(), bytes->size()) &&
           full_frame(bytes->data(), fill, first);
}

struct OwnedTask {
    std::unique_ptr<task::Task> subject{new (std::nothrow) task::Task{}};
    paging::PageTable* root{};
    OwnedTask() = default;
    OwnedTask(const OwnedTask&) = delete;
    ~OwnedTask() { reset(); }
    auto init() -> bool {
        if (subject == nullptr) return false;
        root = virt::create_pagemap();
        if (root == nullptr) return false;
        subject->type = task::TaskType::PROCESS;
        subject->pagemap = root;
        return true;
    }
    void reset() {
        if (root == nullptr) return;
        KEXPECT_EQ(subject->usercopy_pagemap_accesses.load(std::memory_order_acquire), 0U);
        KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(root), 0U);
        KEXPECT_EQ(subject->ref_count.load(std::memory_order_acquire), 1U);
        KEXPECT_EQ(subject->detach_pagemap_after_usercopy_quiescence(), root);
        task::release_lazy_vmem_ranges(*subject);
        vmem::release_file_mmap_ranges_for_pagemap(root);
        virt::destroy_user_space(root, 0, "private-promotion-ktest", "owned-fixture");
        virt::release_pagemap(root);
        root = nullptr;
    }
    auto map(uint64_t flags) -> uint8_t* {
        auto* page = static_cast<uint8_t*>(phys::page_alloc(mm::PhysicalPageOwner::USER_PRIVATE_MAPPING, PAGE, "promotion-fixture"));
        if (page == nullptr) return nullptr;
        std::memset(page, ORIGINAL, PAGE);
        virt::map_page(root, VA, physical(page), flags);  // Allocation ref -> PTE.
        return page;
    }
};

// A real heap File and immutable view, with a synthetic bounded read backend.
// This qualifies cache/read/PTE/ref mechanics, not a native filesystem grant.
// Declare before every Task that retains its File so Tasks unwind first.
struct OwnedFile {
    unsigned reads{};
    unsigned closes{};
    vfs::FileOperations operations{};
    vfs::File* file{};
    void* cached{};
    bool evicted{};
    OwnedFile() = default;
    OwnedFile(const OwnedFile&) = delete;
    ~OwnedFile() {
        if (file == nullptr) return;
        if (cached != nullptr && !evicted) {
            KEXPECT_TRUE(vmem::selftest_evict_file_mmap_cache_exact(*file, 0, cached));
        }
        release_observer();
        KEXPECT_EQ(file->pos, 29);
        KEXPECT_EQ(file->open_flags, 0);
        KEXPECT_EQ(file->refcount.load(), 1);
        vfs::vfs_put_file(file);
        KEXPECT_EQ(closes, 1U);
    }
    auto init() -> bool {
        operations.vfs_read = [](vfs::File* f, void* output, size_t count, size_t offset) -> ssize_t {
            auto& owner = *static_cast<OwnedFile*>(f->private_data);
            ++owner.reads;
            if (offset >= PAGE) return 0;
            size_t const n = std::min(count, PAGE - offset);
            std::memset(output, ORIGINAL, n);
            return static_cast<ssize_t>(n);
        };
        operations.vfs_close = [](vfs::File* f) -> int {
            ++static_cast<OwnedFile*>(f->private_data)->closes;
            return 0;
        };
        file = new (std::nothrow) vfs::File{};
        if (file == nullptr) return false;
        file->fops = &operations;
        file->private_data = this;
        file->fs_type = vfs::FSType::SOCKET;
        file->pos = 29;
        file->open_flags = 0;
        file->refcount.store(1);
        file->exec_read_view.reset(new (std::nothrow) vfs::ExecReadView{});
        if (file->exec_read_view == nullptr) return false;
        uint64_t const identity = vfs::ExecContentState::allocate_identity();
        if (identity == 0) return false;
        file->exec_read_view->content_id = identity;
        auto& stat = file->exec_read_view->metadata;
        stat.st_dev = 0x50524F4D;
        stat.st_ino = identity;
        stat.st_mode = vfs::S_IFREG | 0400;
        stat.st_size = PAGE;
        return true;
    }
    auto admit_cold_cache() -> bool {
        vmem::SelftestFileCacheObservation observation{};
        if (!vmem::selftest_observe_file_mmap_cache(*file, 0, observation)) return false;
        if (observation.retained_frame != nullptr) {
            phys::page_ref_dec(observation.retained_frame);
            return false;  // Every owned File has a fresh, non-reused identity.
        }
        return observation.vacant_way;
    }
    auto observe_cache() -> bool {
        vmem::SelftestFileCacheObservation observation{};
        if (!vmem::selftest_observe_file_mmap_cache(*file, 0, observation)) return false;
        if (cached == nullptr) {
            cached = observation.retained_frame;
        } else if (observation.retained_frame != nullptr) {
            bool const same = cached == observation.retained_frame;
            phys::page_ref_dec(observation.retained_frame);
            if (!same) return false;
        }
        return cached != nullptr && observation.retained_frame != nullptr;
    }
    void release_observer() {
        if (cached == nullptr) return;
        void* const page = cached;
        cached = nullptr;
        phys::page_ref_dec(page);
        KEXPECT_EQ(phys::page_ref_get(page), 0U);  // Tasks/cache retired first.
    }
    auto lazy(OwnedTask& owner) -> bool {
        if (cached == nullptr && !admit_cold_cache()) return false;
        auto const& st = file->exec_read_view->metadata;
        task::LazyVmemRange range{.start = VA,
                                  .end = VA + PAGE,
                                  .prot = vmabi::PROT_READ,
                                  .flags = vmabi::MAP_PRIVATE,
                                  .kind = task::LazyVmemKind::FILE_BACKED,
                                  .file = file,
                                  .file_offset = 0,
                                  .file_dev = st.st_dev,
                                  .file_ino = st.st_ino,
                                  .file_size = static_cast<uint64_t>(st.st_size),
                                  .file_mtime_sec = st.st_mtim.tv_sec,
                                  .file_mtime_nsec = st.st_mtim.tv_nsec,
                                  .file_ctime_sec = st.st_ctim.tv_sec,
                                  .file_ctime_nsec = st.st_ctim.tv_nsec};
        vfs::vfs_retain_file(file);
        if (!owner.subject->lazy_vmem_ranges.push_back(range)) {
            vfs::vfs_put_file(file);
            return false;
        }
        paging::PageFault fault{};
        bool const mapped = vmem::materialize_lazy_file_page(owner.subject.get(), range, VA, fault);
        // Recover a cache ref even if insertion succeeded but install refused.
        bool const observed = observe_cache();
        return mapped && observed && frame(owner.root) == cached;
    }
    auto eager(OwnedTask& owner) -> bool {
        if (cached == nullptr && !admit_cold_cache()) return false;
        bool const mapped = vmem::selftest_map_cached_private_file_page(*owner.subject, *file, VA, 0);
        bool const observed = observe_cache();
        return mapped && observed && frame(owner.root) == cached;
    }
    auto evict() -> bool {
        if (cached == nullptr || evicted) return false;
        evicted = vmem::selftest_evict_file_mmap_cache_exact(*file, 0, cached);
        return evicted;
    }
};

struct OwnerBalance {
    phys::PhysicalBalanceSnapshot before{};
    OwnerBalance() { phys::get_physical_balance_snapshot(before); }
    ~OwnerBalance() {
        phys::PhysicalBalanceSnapshot after{};
        phys::get_physical_balance_snapshot(after);
        for (auto owner : {mm::PhysicalPageOwner::USER_PRIVATE_MAPPING, mm::PhysicalPageOwner::USER_FILE_CACHE,
                           mm::PhysicalPageOwner::USER_FILE_MAPPING}) {
            size_t const i = static_cast<size_t>(owner);
            KEXPECT_EQ(after.owners.at(i).pages, before.owners.at(i).pages);
            KEXPECT_EQ(after.owners.at(i).objects, before.owners.at(i).objects);
        }
    }
};

auto clone(OwnedTask& parent, OwnedTask& child) -> bool {
    // Child root is genuinely empty. No scratch/user page before COW copy.
    vmem::SharedVmemPublicationGuard guard;
    return task::clone_lazy_vmem_ranges(*child.subject, *parent.subject) && virt::deep_copy_user_pagemap_cow(parent.root, child.root);
}
auto write(OwnedTask& owner) -> bool { return usercopy::copy_to_task(*owner.subject, VA, &REPLACEMENT, 1); }
}  // namespace

KTEST(PrivateWritePromotion, CachedAliasesRequireAuthorizedWrite) {
    OwnerBalance balance;
    for (bool lazy : {false, true}) {
        OwnedFile file;
        OwnedTask writer;
        OwnedTask peer;
        KREQUIRE_TRUE(file.init());
        KREQUIRE_TRUE(writer.init());
        KREQUIRE_TRUE(peer.init());
        KREQUIRE_TRUE(lazy ? file.lazy(writer) : file.eager(writer));
        KREQUIRE_TRUE(lazy ? file.lazy(peer) : file.eager(peer));
        auto* cached = frame(peer.root);
        KREQUIRE_NE(cached, nullptr);
        KEXPECT_EQ(frame(writer.root), cached);
        KEXPECT_EQ(phys::page_ref_get(cached), 4U);  // Cache + observer + two PTEs.
        KEXPECT_EQ(file.reads, 1U);
        KEXPECT_FALSE(write(writer));
        KEXPECT_TRUE(full_read(*peer.subject, ORIGINAL, ORIGINAL));
        virt::unify_page_flags(writer.root, VA, paging::page_types::USER);
        KEXPECT_FALSE(virt::user_page_writable_now(writer.root, VA));
        KREQUIRE_TRUE(write(writer));
        KEXPECT_NE(frame(writer.root), cached);
        KEXPECT_EQ(phys::page_ref_get(cached), 3U);
        KEXPECT_TRUE(full_read(*writer.subject, ORIGINAL, REPLACEMENT));
        KEXPECT_TRUE(full_frame(cached, ORIGINAL, ORIGINAL));
        KEXPECT_TRUE(full_read(*peer.subject, ORIGINAL, ORIGINAL));
        virt::unify_page_flags(writer.root, VA, paging::page_types::USER_READONLY);
        KEXPECT_FALSE(write(writer));
        virt::unify_page_flags(writer.root, VA, paging::PAGE_PRESENT | paging::PAGE_NX);
        KEXPECT_FALSE(write(writer));
        KEXPECT_TRUE(full_frame(frame(writer.root), ORIGINAL, REPLACEMENT));
        virt::unify_page_flags(writer.root, VA, paging::page_types::USER);
        KREQUIRE_TRUE(write(writer));
        KEXPECT_TRUE(full_frame(cached, ORIGINAL, ORIGINAL));
    }
}

KTEST(PrivateWritePromotion, ReadonlyForkAndExactEvictionKeepPeers) {
    OwnerBalance balance;
    OwnedFile file;
    OwnedFile unrelated;
    OwnedTask parent;
    OwnedTask child;
    OwnedTask other;
    KREQUIRE_TRUE(file.init());
    KREQUIRE_TRUE(unrelated.init());
    KREQUIRE_TRUE(parent.init());
    KREQUIRE_TRUE(child.init());
    KREQUIRE_TRUE(other.init());
    KREQUIRE_TRUE(file.eager(parent));
    KREQUIRE_TRUE(unrelated.eager(other));
    KREQUIRE_TRUE(clone(parent, child));
    auto* cached = frame(parent.root);
    auto* untouched = frame(other.root);
    KEXPECT_EQ(frame(child.root), cached);
    KEXPECT_EQ(phys::page_ref_get(cached), 4U);
    KEXPECT_FALSE(vmem::selftest_evict_file_mmap_cache_exact(*file.file, PAGE, cached));
    KEXPECT_FALSE(vmem::selftest_evict_file_mmap_cache_exact(*file.file, 0, untouched));
    KEXPECT_EQ(phys::page_ref_get(cached), 4U);
    KREQUIRE_TRUE(file.evict());
    KEXPECT_EQ(phys::page_ref_get(cached), 3U);
    KEXPECT_EQ(phys::page_ref_get(untouched), 3U);
    virt::unify_page_flags(child.root, VA, paging::page_types::USER);
    KREQUIRE_TRUE(write(child));
    KEXPECT_NE(frame(child.root), cached);
    KEXPECT_EQ(frame(parent.root), cached);
    KEXPECT_EQ(phys::page_ref_get(cached), 2U);
    KEXPECT_TRUE(full_read(*parent.subject, ORIGINAL, ORIGINAL));
    KEXPECT_TRUE(full_read(*child.subject, ORIGINAL, REPLACEMENT));
    KEXPECT_TRUE(full_read(*other.subject, ORIGINAL, ORIGINAL));
    parent.reset();
    KEXPECT_EQ(phys::page_ref_get(cached), 1U);  // Retained observer only.
    file.release_observer();
    KEXPECT_EQ(phys::page_ref_get(cached), 0U);
}

KTEST(PrivateWritePromotion, ExclusiveFreshAndSharedRestriction) {
    OwnerBalance balance;
    OwnedTask exclusive;
    OwnedTask restricted;
    OwnedTask peer;
    KREQUIRE_TRUE(exclusive.init());
    KREQUIRE_TRUE(restricted.init());
    KREQUIRE_TRUE(peer.init());
    auto* fresh = exclusive.map(paging::page_types::USER_READONLY);
    auto* shared = restricted.map(paging::page_types::USER | paging::PAGE_SHARED | paging::PAGE_WRITE_RESTRICTED);
    KREQUIRE_NE(fresh, nullptr);
    KREQUIRE_NE(shared, nullptr);
    KEXPECT_FALSE(write(exclusive));
    virt::unify_page_flags(exclusive.root, VA, paging::page_types::USER);
    KEXPECT_TRUE(virt::user_page_writable_now(exclusive.root, VA));
    KREQUIRE_TRUE(write(exclusive));
    KEXPECT_EQ(frame(exclusive.root), fresh);
    KEXPECT_EQ(phys::page_ref_get(fresh), 1U);
    KREQUIRE_TRUE(clone(restricted, peer));
    for (uint64_t flags : {uint64_t{paging::page_types::USER_READONLY}, uint64_t{paging::PAGE_PRESENT | paging::PAGE_NX},
                           uint64_t{paging::page_types::USER}}) {
        virt::unify_page_flags(peer.root, VA, flags);
        KEXPECT_TRUE(virt::is_page_write_restricted(peer.root, VA));
        KEXPECT_FALSE(write(peer));
        KEXPECT_EQ(frame(peer.root), shared);
        KEXPECT_TRUE(full_frame(shared, ORIGINAL, ORIGINAL));
        KEXPECT_EQ(phys::page_ref_get(shared), 2U);
    }
    {
        OwnedTask readonly_shared;
        OwnedTask shared_peer;
        KREQUIRE_TRUE(readonly_shared.init());
        KREQUIRE_TRUE(shared_peer.init());
        auto* shared_frame = readonly_shared.map(paging::page_types::USER_READONLY | paging::PAGE_SHARED);
        KREQUIRE_NE(shared_frame, nullptr);
        task::LazyVmemRange const conflicting{
            .start = VA, .end = VA + PAGE, .prot = vmabi::PROT_READ | vmabi::PROT_WRITE, .flags = vmabi::MAP_SHARED | vmabi::MAP_ANONYMOUS};
        KREQUIRE_TRUE(readonly_shared.subject->lazy_vmem_ranges.push_back(conflicting));
        KREQUIRE_TRUE(clone(readonly_shared, shared_peer));
        // Metadata alone cannot synthesize private COW on a shared leaf.
        KEXPECT_FALSE(write(readonly_shared));
        KEXPECT_EQ(frame(readonly_shared.root), shared_frame);
        KEXPECT_TRUE(full_frame(shared_frame, ORIGINAL, ORIGINAL));
        // Explicitly granting WRITE keeps an unrestricted shared leaf shared.
        virt::unify_page_flags(readonly_shared.root, VA, paging::page_types::USER);
        KREQUIRE_TRUE(write(readonly_shared));
        KEXPECT_EQ(frame(readonly_shared.root), shared_frame);
        KEXPECT_EQ(frame(shared_peer.root), shared_frame);
        KEXPECT_EQ(phys::page_ref_get(shared_frame), 2U);
        KEXPECT_TRUE(full_read(*shared_peer.subject, ORIGINAL, REPLACEMENT));
    }
}

KTEST(PrivateWritePromotion, CopyCommitDowngradeAndScopedOOM) {
    OwnerBalance balance;
    for (auto action : {virt::SelftestCowAction::FAIL_DESTINATION_ALLOCATION, virt::SelftestCowAction::DOWNGRADE_READ_BEFORE_COMMIT,
                        virt::SelftestCowAction::DOWNGRADE_NONE_BEFORE_COMMIT}) {
        OwnedTask parent;
        OwnedTask child;
        KREQUIRE_TRUE(parent.init());
        KREQUIRE_TRUE(child.init());
        auto* original = parent.map(paging::page_types::USER);
        KREQUIRE_NE(original, nullptr);
        KREQUIRE_TRUE(clone(parent, child));
        virt::SelftestCowObservation refused{};
        KEXPECT_FALSE(virt::selftest_copy_to_task_cow_action(*parent.subject, VA, &REPLACEMENT, 0, action, refused));
        KEXPECT_FALSE(refused.admitted);
        KEXPECT_EQ(refused.consumed, 0U);
        KEXPECT_NULL(refused.destination);
        KEXPECT_TRUE(refused.disarmed);
        KEXPECT_EQ(phys::page_ref_get(original), 2U);
        virt::SelftestCowObservation observation{};
        KEXPECT_FALSE(virt::selftest_copy_to_task_cow_action(*parent.subject, VA, &REPLACEMENT, 1, action, observation));
        KEXPECT_TRUE(observation.admitted);
        KEXPECT_EQ(observation.consumed, 1U);
        KEXPECT_TRUE(observation.action_outside_spinlocks);
        KEXPECT_TRUE(observation.disarmed);
        if (action == virt::SelftestCowAction::FAIL_DESTINATION_ALLOCATION) {
            KEXPECT_NULL(observation.destination);
        } else {
            KREQUIRE_NE(observation.destination, nullptr);
            KEXPECT_EQ(phys::page_ref_get(observation.destination), 0U);
            KEXPECT_FALSE(write(parent));
        }
        KEXPECT_EQ(frame(parent.root), original);
        KEXPECT_EQ(frame(child.root), original);
        KEXPECT_EQ(phys::page_ref_get(original), 2U);
        KEXPECT_TRUE(full_frame(original, ORIGINAL, ORIGINAL));
        KEXPECT_EQ(parent.subject->ref_count.load(), 1U);
        KEXPECT_EQ(parent.subject->usercopy_pagemap_accesses.load(), 0U);
        virt::unify_page_flags(parent.root, VA, paging::page_types::USER);
        KREQUIRE_TRUE(write(parent));
        KEXPECT_NE(frame(parent.root), original);
        KEXPECT_EQ(phys::page_ref_get(original), 1U);
        KEXPECT_TRUE(full_read(*parent.subject, ORIGINAL, REPLACEMENT));
        KEXPECT_TRUE(full_read(*child.subject, ORIGINAL, ORIGINAL));
    }
}

KTEST(PrivateWritePromotion, AnonymousPermissionMustBeCurrent) {
    OwnerBalance balance;
    for (bool admitted : {false, true}) {
        OwnedTask owner;
        KREQUIRE_TRUE(owner.init());
        auto* original = owner.map(paging::page_types::USER_READONLY);
        KREQUIRE_NE(original, nullptr);
        task::LazyVmemRange range{.start = VA,
                                  .end = VA + PAGE,
                                  .prot = vmabi::PROT_READ | vmabi::PROT_WRITE,
                                  .flags = vmabi::MAP_PRIVATE | vmabi::MAP_ANONYMOUS};
        KREQUIRE_TRUE(owner.subject->lazy_vmem_ranges.push_back(range));
        if (!admitted) {
            uint64_t const irq = owner.subject->lazy_vmem_lock.lock_irqsave();
            owner.subject->lazy_vmem_ranges.at(0).prot = vmabi::PROT_READ;
            owner.subject->lazy_vmem_lock.unlock_irqrestore(irq);
            virt::unify_page_flags(owner.root, VA, paging::page_types::USER_READONLY);
        }
        KEXPECT_EQ(write(owner), admitted);
        KEXPECT_EQ(frame(owner.root), original);  // Exclusive frame promotes.
        KEXPECT_EQ(phys::page_ref_get(original), 1U);
        KEXPECT_TRUE(full_frame(original, ORIGINAL, admitted ? REPLACEMENT : ORIGINAL));
    }
    // This sequential test qualifies actual current permission, not the stale
    // snapshot interleaving. No callback/park under cow/lazy/pin is proposed.
}

struct SwapStorage {
    std::unique_ptr<ker::test::FaultBlockDevice> device{new (std::nothrow) ker::test::FaultBlockDevice(PAGE, 16)};
    swap::SwapStats before{};
    const char* name{};
    SwapStorage() { swap::get_stats(&before); }
    ~SwapStorage() {
        if (name != nullptr) KEXPECT_EQ(swap::swapoff_path(name), 0);
        swap::SwapStats after{};
        swap::get_stats(&after);
        KEXPECT_EQ(after.total_bytes, before.total_bytes);
        KEXPECT_EQ(after.free_bytes, before.free_bytes);
        KEXPECT_EQ(after.used_bytes, before.used_bytes);
        KEXPECT_EQ(after.cached_bytes, before.cached_bytes);
        KEXPECT_EQ(after.active_areas, before.active_areas);
    }
    auto init(const char* label) -> bool {
        if (device == nullptr || !device->valid()) return false;
        swap::SwapExtent extent{.device = device->device(), .start_block = 0, .page_count = 16};
        if (swap::activate_extents(label, &extent, 1) != 0) return false;
        name = label;
        return true;
    }
};

KTEST(PrivateWritePromotion, SuccessfulSwapPreservesReadAndNone) {
    OwnerBalance balance;
    for (bool none : {false, true}) {
        SwapStorage storage;
        OwnedTask parent;
        OwnedTask child;
        KREQUIRE_TRUE(storage.init(none ? "ktest-promotion-none" : "ktest-promotion-read"));
        KREQUIRE_TRUE(parent.init());
        KREQUIRE_TRUE(child.init());
        KREQUIRE_NE(parent.map(paging::page_types::USER), nullptr);
        KREQUIRE_TRUE(virt::selftest_anonymous_swap_pageout(parent.root, VA));
        KREQUIRE_TRUE(clone(parent, child));
        uint64_t const flags = none ? (paging::PAGE_PRESENT | paging::PAGE_NX) : paging::page_types::USER_READONLY;
        KREQUIRE_TRUE(virt::protect_anonymous_swap_page(parent.root, VA, flags));
        KREQUIRE_TRUE(virt::protect_anonymous_swap_page(child.root, VA, flags));
        KREQUIRE_TRUE(virt::selftest_anonymous_swap_pagein(parent.root, VA));
        KREQUIRE_TRUE(virt::selftest_anonymous_swap_pagein(child.root, VA));
        auto* p = frame(parent.root);
        auto* c = frame(child.root);
        KREQUIRE_NE(p, nullptr);
        KREQUIRE_NE(c, nullptr);
        KEXPECT_NE(p, c);
        KEXPECT_EQ(phys::page_ref_get(p), 1U);
        KEXPECT_EQ(phys::page_ref_get(c), 1U);
        KEXPECT_FALSE(write(parent));
        KEXPECT_FALSE(write(child));
        KEXPECT_TRUE(full_frame(p, ORIGINAL, ORIGINAL));
        KEXPECT_TRUE(full_frame(c, ORIGINAL, ORIGINAL));
        virt::unify_page_flags(child.root, VA, paging::page_types::USER);
        KREQUIRE_TRUE(write(child));
        KEXPECT_EQ(frame(child.root), c);  // Independent pagein destination.
        KEXPECT_TRUE(full_frame(p, ORIGINAL, ORIGINAL));
        KEXPECT_TRUE(full_read(*child.subject, ORIGINAL, REPLACEMENT));
    }
}
#endif
