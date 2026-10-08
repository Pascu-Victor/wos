#include "shm.hpp"

#include <abi/callnums/shm.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>
#ifdef WOS_SELFTEST
#include <memory>
#include <platform/sched/resource.hpp>
#endif
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/ipc_namespace.hpp>
#include <platform/sched/resource_as.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sched/threading.hpp>
#include <platform/security/authorization.hpp>
#include <platform/sys/spinlock.hpp>
#include <platform/sys/usercopy.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>

namespace ker::syscall::shm {
namespace {
using log = ker::mod::dbg::logger<"shm">;
using ker::mod::sched::ipc::NamespaceRef;

constexpr uint64_t USER_SPACE_START = 0x0000000000400000ULL;
constexpr uint64_t USER_SPACE_END = 0x00007FFFFFFFFFFFULL;
constexpr uint64_t SHM_SEARCH_START = 0x0000200000000000ULL;
constexpr size_t MAX_SHM_SEGMENTS = 64;
constexpr size_t MAX_SHM_ATTACHMENTS = 256;

struct ShmSegment {
    bool active = false;
    int id = -1;
    int key = ker::abi::shm::IPC_PRIVATE;
    void* backing = nullptr;
    uint64_t size = 0;
    uint64_t page_count = 0;
    ker::mod::security::IpcOwnership ownership{};
    uint64_t creator_pid = 0;
    uint64_t owner_pid = 0;
    uint64_t last_pid = 0;
    uint64_t attach_count = 0;
    bool marked_destroy = false;
    NamespaceRef ipc_namespace{};
};

struct ShmAttachment {
    bool active = false;
    bool publishing = false;
    uint64_t pid = 0;
    int shmid = -1;
    uint64_t addr = 0;
    uint64_t size = 0;
};

ker::mod::sys::Spinlock g_lock;
std::array<ShmSegment, MAX_SHM_SEGMENTS> g_segments{};
std::array<ShmAttachment, MAX_SHM_ATTACHMENTS> g_attachments{};
uint64_t g_next_id = 1;

auto current_task() -> ker::mod::sched::task::Task* { return ker::mod::sched::get_current_task(); }

auto to_errno(int error) -> uint64_t { return static_cast<uint64_t>(-error); }

auto align_shm_size(uint64_t size, uint64_t& aligned) -> bool {
    constexpr uint64_t PAGE_MASK = ker::mod::mm::paging::PAGE_SIZE - 1;
    aligned = 0;
    if (size == 0 || size > UINT64_MAX - PAGE_MASK) {
        return false;
    }
    aligned = page_align_up(size);
    return aligned != 0 && aligned <= USER_SPACE_END - USER_SPACE_START;
}

auto process_pid_for_task(const ker::mod::sched::task::Task* task) -> uint64_t {
    if (task == nullptr) {
        return 0;
    }
    return ker::mod::sched::task::process_pid(*task);
}

auto backing_page(const ShmSegment& segment, uint64_t index) -> void* {
    auto base = reinterpret_cast<uint64_t>(segment.backing);
    return reinterpret_cast<void*>(base + (index * ker::mod::mm::paging::PAGE_SIZE));
}

auto backing_paddr(const ShmSegment& segment, uint64_t index) -> uint64_t {
    auto* page = backing_page(segment, index);
    return reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
}

// Internal attachment ownership uses exact, globally nonreused IDs. This
// function must not be used to admit a new userspace name/ID capability.
auto find_retained_segment_by_id(int id) -> ShmSegment* {
    auto* it = std::ranges::find_if(g_segments, [id](const ShmSegment& segment) -> bool { return segment.active && segment.id == id; });
    return it != g_segments.end() ? &*it : nullptr;
}

auto find_visible_segment_by_id(int id, const NamespaceRef& ipc_namespace) -> ShmSegment* {
    auto* segment = find_retained_segment_by_id(id);
    return segment != nullptr && segment->ipc_namespace == ipc_namespace ? segment : nullptr;
}

auto find_segment_by_key(int key, const NamespaceRef& ipc_namespace) -> ShmSegment* {
    auto* it = std::ranges::find_if(g_segments, [key, &ipc_namespace](const ShmSegment& segment) -> bool {
        return segment.active && segment.key == key && segment.ipc_namespace == ipc_namespace;
    });
    return it != g_segments.end() ? &*it : nullptr;
}

auto find_free_segment() -> ShmSegment* {
    auto* it = std::ranges::find_if(g_segments, [](const ShmSegment& segment) -> bool { return !segment.active; });
    return it != g_segments.end() ? &*it : nullptr;
}

auto find_free_attachment() -> ShmAttachment* {
    auto* it = std::ranges::find_if(g_attachments,
                                    [](const ShmAttachment& attachment) -> bool { return !attachment.active && !attachment.publishing; });
    return it != g_attachments.end() ? &*it : nullptr;
}

auto find_attachment(uint64_t pid, uint64_t addr) -> ShmAttachment* {
    auto* it = std::ranges::find_if(g_attachments, [pid, addr](const ShmAttachment& attachment) -> bool {
        return attachment.active && attachment.pid == pid && attachment.addr == addr;
    });
    return it != g_attachments.end() ? &*it : nullptr;
}

struct SharedLayoutOverlapQuery {
    ker::mod::mm::paging::PageTable* pagemap = nullptr;
    uint64_t start = 0;
    uint64_t size = 0;
};

auto task_has_overlapping_initial_layout(ker::mod::sched::task::Task* candidate, void* opaque) -> bool {
    auto* query = static_cast<SharedLayoutOverlapQuery*>(opaque);
    return candidate != nullptr && query != nullptr && candidate->pagemap == query->pagemap &&
           ker::mod::sched::threading::range_overlaps_initial_layout(candidate->thread, query->start, query->size);
}

// The caller holds SharedVmemPublicationGuard, so a matching Task cannot
// publish a new pagemap/thread pair while the scheduler registry is queried.
auto range_overlaps_shared_initial_layout(ker::mod::sched::task::Task* task, uint64_t addr, uint64_t size) -> bool {
    if (task == nullptr || task->pagemap == nullptr || size == 0) {
        return true;
    }
    if (ker::mod::sched::threading::range_overlaps_initial_layout(task->thread, addr, size)) {
        return true;
    }

    SharedLayoutOverlapQuery query{.pagemap = task->pagemap, .start = addr, .size = size};
    auto* owner = ker::mod::sched::find_active_task_lifetime_ref_if(task_has_overlapping_initial_layout, &query);
    if (owner == nullptr) {
        return false;
    }
    owner->release();
    return true;
}

auto range_overlaps_lazy_vmem(ker::mod::sched::task::Task* task, uint64_t addr, uint64_t size) -> bool {
    if (task == nullptr || size == 0 || addr > UINT64_MAX - size) {
        return true;
    }

    uint64_t const END = addr + size;
    bool overlaps = false;
    uint64_t const FLAGS = task->lazy_vmem_lock.lock_irqsave();
    for (const auto& range : task->lazy_vmem_ranges) {
        if (addr < range.end && range.start < END) {
            overlaps = true;
            break;
        }
    }
    task->lazy_vmem_lock.unlock_irqrestore(FLAGS);
    return overlaps;
}

auto range_is_free(ker::mod::sched::task::Task* task, uint64_t addr, uint64_t size) -> bool {
    if (task == nullptr || task->pagemap == nullptr || size == 0 || addr > USER_SPACE_END - size ||
        range_overlaps_shared_initial_layout(task, addr, size) || range_overlaps_lazy_vmem(task, addr, size)) {
        return false;
    }

    uint64_t const END = addr + size;
    for (uint64_t current = addr; current < END; current += ker::mod::mm::paging::PAGE_SIZE) {
        if (ker::mod::mm::virt::is_page_mapped_or_reserved(task->pagemap, current)) {
            return false;
        }
    }
    return true;
}

auto find_free_range(ker::mod::sched::task::Task* task, uint64_t size, uint64_t hint) -> uint64_t {
    if (task == nullptr || task->pagemap == nullptr || size == 0 || size > USER_SPACE_END - USER_SPACE_START) {
        return 0;
    }

    if (hint >= USER_SPACE_START && hint <= USER_SPACE_END - size && range_is_free(task, hint, size)) {
        return hint;
    }

    uint64_t current = SHM_SEARCH_START;
    while (current <= USER_SPACE_END - size) {
        if (range_is_free(task, current, size)) {
            return current;
        }

        current += ker::mod::mm::paging::PAGE_SIZE;
    }

    return 0;
}

void release_segment(ShmSegment& segment) {
    if (!segment.active || segment.backing == nullptr) {
        segment = {};
        return;
    }

    for (uint64_t i = 0; i < segment.page_count; ++i) {
        ker::mod::mm::phys::page_ref_dec(backing_page(segment, i));
    }

    segment = {};
}

void maybe_release_destroyed_segment(ShmSegment& segment) {
    if (segment.active && segment.marked_destroy && segment.attach_count == 0) {
        release_segment(segment);
    }
}

void abandon_attachment_publication(ShmAttachment* attachment) {
    if (attachment == nullptr) {
        return;
    }

    uint64_t const FLAGS = g_lock.lock_irqsave();
    if (!attachment->publishing) {
        g_lock.unlock_irqrestore(FLAGS);
        return;
    }

    auto* segment = find_retained_segment_by_id(attachment->shmid);
    *attachment = {};
    if (segment != nullptr) {
        if (segment->attach_count > 0) {
            segment->attach_count--;
        }
        maybe_release_destroyed_segment(*segment);
    }
    g_lock.unlock_irqrestore(FLAGS);
}

void rollback_attachment_pages(ker::mod::sched::task::Task* task, const ShmSegment* segment, uint64_t addr, uint64_t mapped_pages) {
    if (task == nullptr || task->pagemap == nullptr || segment == nullptr) {
        return;
    }

    for (uint64_t i = 0; i < mapped_pages; ++i) {
        uint64_t const PAGE_ADDR = addr + (i * ker::mod::mm::paging::PAGE_SIZE);
        if (ker::mod::sys::usercopy::mapped_physical_address(*task, PAGE_ADDR) == backing_paddr(*segment, i)) {
            ker::mod::mm::virt::unmap_page(task->pagemap, PAGE_ADDR);
        }
    }
}

void detach_attachments_for_pid_locked(uint64_t pid) {
    for (auto& attachment : g_attachments) {
        if (!attachment.active || attachment.pid != pid) {
            continue;
        }

        auto* segment = find_retained_segment_by_id(attachment.shmid);
        attachment = {};
        if (segment != nullptr) {
            if (segment->attach_count > 0) {
                segment->attach_count--;
            }
            maybe_release_destroyed_segment(*segment);
        }
    }
}

auto create_segment(int key, uint64_t size, int shmflg, ker::mod::sched::task::Task* task, const ker::mod::security::Snapshot& credentials,
                    const NamespaceRef& ipc_namespace) -> uint64_t {
    uint64_t aligned_size = 0;
    if (!align_shm_size(size, aligned_size)) {
        return to_errno(EINVAL);
    }
    size = aligned_size;
    auto* slot = find_free_segment();
    if (slot == nullptr) {
        return to_errno(ENOSPC);
    }
    if (g_next_id > INT32_MAX) return to_errno(ENOSPC);
    NamespaceRef scope;
    int const RETAIN_ERROR = ipc_namespace.retain(scope);
    if (RETAIN_ERROR != 0) return static_cast<uint64_t>(RETAIN_ERROR);

    void* backing = ker::mod::mm::phys::page_alloc(ker::mod::mm::PhysicalPageOwner::USER_SHARED_MEMORY, size, "sysv-shm");
    if (backing == nullptr) {
        log::warn("out of physical memory for %llu-byte shared segment", static_cast<unsigned long long>(size));
        return to_errno(ENOMEM);
    }

    std::memset(backing, 0, size);
    if (!ker::mod::mm::phys::page_split_to_order0(backing)) {
        ker::mod::mm::phys::page_free(backing);
        return to_errno(ENOMEM);
    }

    int const ID = static_cast<int>(g_next_id++);
    uint64_t const PROCESS_PID = process_pid_for_task(task);
    *slot = {
        .active = true,
        .id = ID,
        .key = key,
        .backing = backing,
        .size = size,
        .page_count = size / ker::mod::mm::paging::PAGE_SIZE,
        .ownership = {.uid = credentials.user().effective,
                      .gid = credentials.group().effective,
                      .creator_uid = credentials.user().effective,
                      .creator_gid = credentials.group().effective,
                      .mode = static_cast<uint32_t>(shmflg) & 0777U},
        .creator_pid = PROCESS_PID,
        .owner_pid = PROCESS_PID,
        .last_pid = PROCESS_PID,
        .attach_count = 0,
        .marked_destroy = false,
        .ipc_namespace = std::move(scope),
    };

    return static_cast<uint64_t>(ID);
}

auto shmget_impl(ker::mod::sched::task::Task* task, int key, uint64_t size, int shmflg) -> uint64_t {
    auto const* ipc_namespace = task != nullptr ? task->ipc_namespace() : nullptr;
    if (ipc_namespace == nullptr) {
        return to_errno(ESRCH);
    }

    auto const CREDENTIALS = task->credentials();
    uint64_t aligned_size = 0;
    if (size != 0) {
        if (!align_shm_size(size, aligned_size)) {
            return to_errno(EINVAL);
        }
        size = aligned_size;
    }

    uint64_t const FLAGS = g_lock.lock_irqsave();
    if (ipc_namespace->closed()) {
        g_lock.unlock_irqrestore(FLAGS);
        return to_errno(EIDRM);
    }

    if (key != ker::abi::shm::IPC_PRIVATE) {
        if (auto* existing = find_segment_by_key(key, *ipc_namespace); existing != nullptr) {
            if ((shmflg & ker::abi::shm::IPC_CREAT) != 0 && (shmflg & ker::abi::shm::IPC_EXCL) != 0) {
                g_lock.unlock_irqrestore(FLAGS);
                return to_errno(EEXIST);
            }
            if (size != 0 && size > existing->size) {
                g_lock.unlock_irqrestore(FLAGS);
                return to_errno(EINVAL);
            }
            uint32_t const REQUESTED =
                (static_cast<uint32_t>(shmflg) | (static_cast<uint32_t>(shmflg) >> 3U) | (static_cast<uint32_t>(shmflg) >> 6U)) & 6U;
            if (!ker::mod::security::may_access_ipc(CREDENTIALS, existing->ownership, REQUESTED)) {
                g_lock.unlock_irqrestore(FLAGS);
                return to_errno(EACCES);
            }
            int const ID = existing->id;
            g_lock.unlock_irqrestore(FLAGS);
            return static_cast<uint64_t>(ID);
        }

        if ((shmflg & ker::abi::shm::IPC_CREAT) == 0) {
            g_lock.unlock_irqrestore(FLAGS);
            return to_errno(ENOENT);
        }
    }

    uint64_t const RESULT = create_segment(key, size, shmflg, task, CREDENTIALS, *ipc_namespace);
    g_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

auto shmat_impl(ker::mod::sched::task::Task* task, int shmid, uint64_t shmaddr, int shmflg) -> uint64_t {
    auto const* ipc_namespace = task != nullptr ? task->ipc_namespace() : nullptr;
    if (ipc_namespace == nullptr || task->pagemap == nullptr) {
        return to_errno(ESRCH);
    }

    // This sleepable publication mutex must precede g_lock. Page-table
    // construction may reclaim/yield, so g_lock is released before mapping.
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    auto const CREDENTIALS = task->credentials();
    ker::mod::sched::resource::AddressSpaceObservation observation(task);
    uint64_t const FLAGS = g_lock.lock_irqsave();
    if (ipc_namespace->closed()) {
        g_lock.unlock_irqrestore(FLAGS);
        return to_errno(EIDRM);
    }
    auto* segment = find_visible_segment_by_id(shmid, *ipc_namespace);
    if (segment == nullptr || segment->marked_destroy) {
        g_lock.unlock_irqrestore(FLAGS);
        return to_errno(EINVAL);
    }

    uint32_t const REQUIRED = (shmflg & ker::abi::shm::SHM_RDONLY) != 0 ? 4U : 6U;
    if (!ker::mod::security::may_access_ipc(CREDENTIALS, segment->ownership, REQUIRED)) {
        g_lock.unlock_irqrestore(FLAGS);
        return to_errno(EACCES);
    }

    auto* attachment = find_free_attachment();
    if (attachment == nullptr) {
        g_lock.unlock_irqrestore(FLAGS);
        return to_errno(ENOSPC);
    }

    uint64_t const SIZE = segment->size;
    uint64_t const PAGE_COUNT = segment->page_count;
    uint64_t const PROCESS_PID = process_pid_for_task(task);
    *attachment = {
        .active = false,
        .publishing = true,
        .pid = PROCESS_PID,
        .shmid = segment->id,
        .addr = 0,
        .size = SIZE,
    };
    // Pin the segment while g_lock is dropped for address selection and
    // page-table publication. A failed publication decrements this count.
    segment->attach_count++;
    g_lock.unlock_irqrestore(FLAGS);

    uint64_t const ADDR = shmaddr != 0 ? shmaddr : find_free_range(task, SIZE, 0);
    if (ADDR == 0 || ADDR % ker::mod::mm::paging::PAGE_SIZE != 0 || ADDR < USER_SPACE_START || ADDR > USER_SPACE_END - SIZE ||
        !range_is_free(task, ADDR, SIZE)) {
        abandon_attachment_publication(attachment);
        return to_errno(EINVAL);
    }

    if (!ker::mod::sched::resource::admit_address_space(task, task->pagemap, SIZE)) {
        abandon_attachment_publication(attachment);
        return to_errno(ENOMEM);
    }

    uint64_t page_flags = ker::mod::mm::paging::PAGE_PRESENT | ker::mod::mm::paging::PAGE_USER | ker::mod::mm::paging::PAGE_SHARED;
    if ((shmflg & ker::abi::shm::SHM_RDONLY) == 0) {
        page_flags |= ker::mod::mm::paging::PAGE_WRITE;
    } else {
        page_flags |= ker::mod::mm::paging::PAGE_WRITE_RESTRICTED;
    }

    uint64_t mapped_pages = 0;
    for (uint64_t i = 0; i < PAGE_COUNT; ++i) {
        uint64_t const PAGE_ADDR = ADDR + (i * ker::mod::mm::paging::PAGE_SIZE);
        // A direct publisher that does not yet participate in the shared-VM
        // mutex must not turn this transaction into a replacement mapping.
        if (ker::mod::mm::virt::is_page_mapped_or_reserved(task->pagemap, PAGE_ADDR)) {
            rollback_attachment_pages(task, segment, ADDR, mapped_pages);
            abandon_attachment_publication(attachment);
            return to_errno(EINVAL);
        }
        auto* page = backing_page(*segment, i);
        ker::mod::mm::phys::page_ref_inc(page);
        ker::mod::mm::virt::map_page(task->pagemap, PAGE_ADDR, backing_paddr(*segment, i), page_flags);
        mapped_pages++;
    }

    uint64_t const COMMIT_FLAGS = g_lock.lock_irqsave();
    attachment->addr = ADDR;
    attachment->publishing = false;
    attachment->active = true;
    segment->last_pid = PROCESS_PID;
    g_lock.unlock_irqrestore(COMMIT_FLAGS);
    return ADDR;
}

void detach_attachment_locked(ShmAttachment& attachment, ker::mod::sched::task::Task* task) {
    auto* segment = find_retained_segment_by_id(attachment.shmid);
    if (segment != nullptr) {
        if (segment->attach_count > 0) {
            segment->attach_count--;
        }
        segment->last_pid = task != nullptr ? process_pid_for_task(task) : attachment.pid;
        maybe_release_destroyed_segment(*segment);
    }

    attachment = {};
}

auto shmdt_impl(uint64_t shmaddr) -> uint64_t {
    auto* task = current_task();
    if (task == nullptr) {
        return to_errno(ESRCH);
    }

    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    ker::mod::sched::resource::AddressSpaceObservation observation(task);
    uint64_t const FLAGS = g_lock.lock_irqsave();
    auto* attachment = find_attachment(process_pid_for_task(task), shmaddr);
    if (attachment == nullptr) {
        g_lock.unlock_irqrestore(FLAGS);
        return to_errno(EINVAL);
    }

    ShmAttachment const DETACHED = *attachment;
    auto* segment = find_retained_segment_by_id(DETACHED.shmid);
    g_lock.unlock_irqrestore(FLAGS);

    for (uint64_t i = 0; segment != nullptr && i < segment->page_count; ++i) {
        uint64_t const ADDR = DETACHED.addr + (i * ker::mod::mm::paging::PAGE_SIZE);
        if (ker::mod::sys::usercopy::mapped_physical_address(*task, ADDR) == backing_paddr(*segment, i)) {
            ker::mod::mm::virt::unmap_page(task->pagemap, ADDR);
        }
    }

    uint64_t const DETACH_FLAGS = g_lock.lock_irqsave();
    attachment = find_attachment(process_pid_for_task(task), shmaddr);
    if (attachment != nullptr) {
        detach_attachment_locked(*attachment, task);
    }
    g_lock.unlock_irqrestore(DETACH_FLAGS);
    return 0;
}

void fill_stat(const ShmSegment& segment, ker::abi::shm::ShmidDs& out) {
    out = {};
    out.shm_perm.key = segment.key;
    out.shm_perm.uid = segment.ownership.uid;
    out.shm_perm.gid = segment.ownership.gid;
    out.shm_perm.cuid = segment.ownership.creator_uid;
    out.shm_perm.cgid = segment.ownership.creator_gid;
    out.shm_perm.mode = segment.ownership.mode;
    out.shm_perm.seq = segment.id;
    out.shm_segsz = segment.size;
    out.shm_atime = 0;
    out.shm_dtime = 0;
    out.shm_ctime = 0;
    out.shm_cpid = static_cast<int64_t>(segment.creator_pid);
    out.shm_lpid = static_cast<int64_t>(segment.last_pid);
    out.shm_nattch = segment.attach_count;
}

auto shmctl_impl(ker::mod::sched::task::Task* task, int shmid, int cmd, uint64_t buf_addr) -> uint64_t {
    auto const* ipc_namespace = task != nullptr ? task->ipc_namespace() : nullptr;
    if (ipc_namespace == nullptr) {
        return to_errno(ESRCH);
    }
    auto const CREDENTIALS = task->credentials();
    uint64_t const FLAGS = g_lock.lock_irqsave();
    if (ipc_namespace->closed()) {
        g_lock.unlock_irqrestore(FLAGS);
        return to_errno(EIDRM);
    }
    auto* segment = find_visible_segment_by_id(shmid, *ipc_namespace);
    if (segment == nullptr) {
        g_lock.unlock_irqrestore(FLAGS);
        return to_errno(EINVAL);
    }

    switch (cmd) {
        case ker::abi::shm::IPC_RMID:
            if (!ker::mod::security::may_control_ipc(CREDENTIALS, segment->ownership)) {
                g_lock.unlock_irqrestore(FLAGS);
                return to_errno(EPERM);
            }
            segment->marked_destroy = true;
            maybe_release_destroyed_segment(*segment);
            g_lock.unlock_irqrestore(FLAGS);
            return 0;
        case ker::abi::shm::IPC_STAT: {
            if (!ker::mod::security::may_access_ipc(CREDENTIALS, segment->ownership, 4)) {
                g_lock.unlock_irqrestore(FLAGS);
                return to_errno(EACCES);
            }
            ker::abi::shm::ShmidDs stat{};
            fill_stat(*segment, stat);
            g_lock.unlock_irqrestore(FLAGS);
            if (buf_addr == 0) {
                return to_errno(EFAULT);
            }
            return ker::mod::sys::usercopy::copy_value_to_task(*task, buf_addr, stat) ? 0 : to_errno(EFAULT);
        }
        default:
            g_lock.unlock_irqrestore(FLAGS);
            return to_errno(EINVAL);
    }
}

}  // namespace

auto sys_shm(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t /*a4*/) -> uint64_t {
    switch (static_cast<ker::abi::shm::ops>(op)) {
        case ker::abi::shm::ops::GET:
            return shmget_impl(current_task(), static_cast<int>(a1), a2, static_cast<int>(a3));
        case ker::abi::shm::ops::ATTACH:
            return shmat_impl(current_task(), static_cast<int>(a1), a2, static_cast<int>(a3));
        case ker::abi::shm::ops::DETACH:
            return shmdt_impl(a1);
        case ker::abi::shm::ops::CTL:
            return shmctl_impl(current_task(), static_cast<int>(a1), static_cast<int>(a2), a3);
        default:
            return to_errno(EINVAL);
    }
}

auto shm_clone_for_fork_locked(ker::mod::sched::task::Task* parent, ker::mod::sched::task::Task* child) -> bool {
    if (parent == nullptr || child == nullptr) {
        return true;
    }

    uint64_t const FLAGS = g_lock.lock_irqsave();
    for (const auto& parent_attachment : g_attachments) {
        if (!parent_attachment.active || parent_attachment.pid != process_pid_for_task(parent)) {
            continue;
        }

        auto* segment = find_retained_segment_by_id(parent_attachment.shmid);
        auto* child_attachment = find_free_attachment();
        if (segment == nullptr || child_attachment == nullptr) {
            detach_attachments_for_pid_locked(child->pid);
            g_lock.unlock_irqrestore(FLAGS);
            return false;
        }

        *child_attachment = parent_attachment;
        child_attachment->pid = child->pid;
        segment->attach_count++;
    }
    g_lock.unlock_irqrestore(FLAGS);
    return true;
}

void shm_cleanup_for_task(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->is_thread) {
        return;
    }

    uint64_t const PID = process_pid_for_task(task);
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    uint64_t const FLAGS = g_lock.lock_irqsave();
    detach_attachments_for_pid_locked(PID);
    g_lock.unlock_irqrestore(FLAGS);
}

void shm_retire_namespace(const NamespaceRef& ipc_namespace) {
    if (ipc_namespace.is_host()) return;
    uint64_t const FLAGS = g_lock.lock_irqsave();
    ipc_namespace.close();
    for (auto& segment : g_segments) {
        if (segment.active && segment.ipc_namespace == ipc_namespace) {
            segment.marked_destroy = true;
            maybe_release_destroyed_segment(segment);
        }
    }
    g_lock.unlock_irqrestore(FLAGS);
}

#ifdef WOS_SELFTEST
namespace {
struct NamespaceFixture {
    NamespaceRef first_scope, second_scope;
    std::unique_ptr<ker::mod::sched::task::Task> host = std::make_unique<ker::mod::sched::task::Task>();
    std::unique_ptr<ker::mod::sched::task::Task> first = std::make_unique<ker::mod::sched::task::Task>();
    std::unique_ptr<ker::mod::sched::task::Task> second = std::make_unique<ker::mod::sched::task::Task>();
    std::unique_ptr<ker::mod::sched::task::Task> child = std::make_unique<ker::mod::sched::task::Task>();
    int host_id = -1;

    auto prepare() -> bool {
        for (auto* task : {host.get(), first.get(), second.get(), child.get()}) task->pid = ker::mod::sched::task::get_next_pid();
        return ker::mod::sched::ipc::create_namespace(first_scope) == 0 && ker::mod::sched::ipc::create_namespace(second_scope) == 0 &&
               first->initialize_ipc_namespace(first_scope) && second->initialize_ipc_namespace(second_scope);
    }

    ~NamespaceFixture() {
        shm_cleanup_for_task(child.get());
        shm_cleanup_for_task(first.get());
        shm_cleanup_for_task(second.get());
        shm_retire_namespace(first_scope);
        shm_retire_namespace(second_scope);
        if (host_id >= 0) static_cast<void>(shmctl_impl(host.get(), host_id, ker::abi::shm::IPC_RMID, 0));
    }
};

auto segment_count() -> size_t {
    uint64_t const FLAGS = g_lock.lock_irqsave();
    size_t const COUNT = std::ranges::count_if(g_segments, [](auto const& segment) { return segment.active; });
    g_lock.unlock_irqrestore(FLAGS);
    return COUNT;
}

auto unused_fixture_key() -> int {
    // Do not select or remove a preexisting host object, even on a failed test.
    uint64_t const FLAGS = g_lock.lock_irqsave();
    int key = -1;
    while (std::ranges::any_of(g_segments, [key](auto const& segment) { return segment.active && segment.key == key; })) --key;
    g_lock.unlock_irqrestore(FLAGS);
    return key;
}
}  // namespace

auto shm_selftest_namespace_isolation() -> bool {
    auto const NAMESPACES = ker::mod::sched::ipc::live_namespace_count();
    auto const SEGMENTS = segment_count();
    bool ok = true;
    {
        NamespaceFixture fixture;
        if (!fixture.prepare()) return false;
        auto const KEY = unused_fixture_key();
        constexpr auto FLAGS = ker::abi::shm::IPC_CREAT | ker::abi::shm::IPC_EXCL | 0600;
        auto const HOST_ID = static_cast<int64_t>(shmget_impl(fixture.host.get(), KEY, 4096, FLAGS));
        if (HOST_ID <= 0) return false;
        fixture.host_id = static_cast<int>(HOST_ID);
        auto const FIRST_ID = static_cast<int64_t>(shmget_impl(fixture.first.get(), KEY, 4096, FLAGS));
        auto const SECOND_ID = static_cast<int64_t>(shmget_impl(fixture.second.get(), KEY, 4096, FLAGS));
        if (FIRST_ID <= 0 || SECOND_ID <= 0) return false;
        // BSP selftests can run before a schedulable current Task exists. An
        // actual kernel pagemap is sufficient for foreign attach rejection:
        // no PTE mutation is allowed to reach this borrowed root.
        auto* pagemap = ker::mod::mm::virt::get_kernel_pagemap();
        if (pagemap == nullptr) return false;
        if (current_task() == nullptr || current_task()->pagemap == nullptr) {
            log::info("IPC namespace fixture uses kernel map during early boot selftests");
        }
        ok &= FIRST_ID != SECOND_ID && HOST_ID != FIRST_ID && HOST_ID != SECOND_ID;
        for (auto* subject : {fixture.host.get(), fixture.first.get(), fixture.second.get()}) {
            auto const OWN_ID = subject == fixture.host.get() ? HOST_ID : subject == fixture.first.get() ? FIRST_ID : SECOND_ID;
            ok &= shmget_impl(subject, KEY, 0, 0) == static_cast<uint64_t>(OWN_ID);
            ok &= shmget_impl(subject, KEY, 4096, FLAGS) == to_errno(EEXIST);
            for (auto id : {HOST_ID, FIRST_ID, SECOND_ID}) {
                // Own STAT reaches the unchanged null-output check; foreign
                // STAT/control must reject before usercopy or permissions.
                ok &= shmctl_impl(subject, static_cast<int>(id), ker::abi::shm::IPC_STAT, 0) == to_errno(id == OWN_ID ? EFAULT : EINVAL);
                if (id != OWN_ID) {
                    ok &= shmctl_impl(subject, static_cast<int>(id), ker::abi::shm::IPC_RMID, 0) == to_errno(EINVAL);
                    subject->pagemap = pagemap;
                    ok &= shmat_impl(subject, static_cast<int>(id), 0, 0) == to_errno(EINVAL);
                    subject->pagemap = nullptr;
                }
            }
        }
        ok &= ker::mod::sched::resource::inherit(*fixture.child, *fixture.first);
        ok &= shmget_impl(fixture.child.get(), KEY, 0, 0) == static_cast<uint64_t>(FIRST_ID);
        ok &= shmctl_impl(fixture.child.get(), static_cast<int>(SECOND_ID), ker::abi::shm::IPC_RMID, 0) == to_errno(EINVAL);
    }
    return ok && segment_count() == SEGMENTS && ker::mod::sched::ipc::live_namespace_count() == NAMESPACES;
}

auto shm_selftest_namespace_retirement() -> bool {
    auto const NAMESPACES = ker::mod::sched::ipc::live_namespace_count();
    auto const SEGMENTS = segment_count();
    bool ok = true;
    {
        NamespaceFixture fixture;
        if (!fixture.prepare() || !fixture.child->initialize_ipc_namespace(fixture.second_scope)) return false;
        auto const ID = static_cast<int64_t>(shmget_impl(fixture.first.get(), ker::abi::shm::IPC_PRIVATE, 4096, 0600));
        if (ID <= 0) return false;
        {
            ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
            uint64_t const FLAGS = g_lock.lock_irqsave();
            auto* segment = find_retained_segment_by_id(static_cast<int>(ID));
            auto* attachment = find_free_attachment();
            if (segment == nullptr || attachment == nullptr) {
                g_lock.unlock_irqrestore(FLAGS);
                return false;
            }
            // Metadata fixture supplies an explicit inherited mapping capability.
            // It does not claim to exercise user PTEs or shared futex execution.
            *attachment = {
                .active = true, .pid = fixture.first->pid, .shmid = static_cast<int>(ID), .addr = SHM_SEARCH_START, .size = 4096};
            segment->attach_count++;
            g_lock.unlock_irqrestore(FLAGS);
            ok &= shm_clone_for_fork_locked(fixture.first.get(), fixture.child.get());
        }
        shm_retire_namespace(fixture.first_scope);
        ok &= fixture.first_scope.closed() && !fixture.second_scope.closed();
        ok &= shmget_impl(fixture.first.get(), 1, 4096, ker::abi::shm::IPC_CREAT) == to_errno(EIDRM);
        ok &= shmctl_impl(fixture.first.get(), static_cast<int>(ID), ker::abi::shm::IPC_STAT, 0) == to_errno(EIDRM);
        ok &= shmctl_impl(fixture.child.get(), static_cast<int>(ID), ker::abi::shm::IPC_RMID, 0) == to_errno(EINVAL);
        shm_cleanup_for_task(fixture.first.get());
        {
            uint64_t const FLAGS = g_lock.lock_irqsave();
            auto* segment = find_retained_segment_by_id(static_cast<int>(ID));
            ok &= segment != nullptr && segment->marked_destroy && segment->attach_count == 1;
            g_lock.unlock_irqrestore(FLAGS);
        }
        shm_cleanup_for_task(fixture.child.get());
        ok &= segment_count() == SEGMENTS;
        NamespaceRef host;
        shm_retire_namespace(host);
        ok &= !host.closed();
    }
    return ok && segment_count() == SEGMENTS && ker::mod::sched::ipc::live_namespace_count() == NAMESPACES;
}
#endif

}  // namespace ker::syscall::shm
