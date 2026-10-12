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
#include <new>
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
    // Historical value-only names: physical attachment ownership stays separate.
    uint64_t creator_pid_scope_generation = 0;
    uint64_t visible_creator_pid = 0;
    uint64_t visible_last_pid = 0;
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

// Caller already owns the live actor; a bound thread retains its immutable
// canonical metadata owner. No retain/drop, registry join, lock or allocation.
struct ShmPidIdentity {
    uint64_t generation = 0;
    uint64_t process_name = 0;
    bool valid = false;
};

auto pid_identity_for_task(const ker::mod::sched::task::Task* task) -> ShmPidIdentity {
    auto const* scope = task != nullptr ? task->pid_namespace() : nullptr;
    if (scope == nullptr) return {};
    uint64_t const NAME = scope->is_host() ? process_pid_for_task(task) : ker::mod::sched::task::namespace_process_pid(*task);
    return {.generation = scope->generation(), .process_name = NAME, .valid = NAME != 0};
}

auto visible_pid(const ShmSegment& segment, const ShmPidIdentity& reader, uint64_t name) -> uint64_t {
    return reader.valid && reader.generation == segment.creator_pid_scope_generation ? name : 0;
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
    auto const PID_IDENTITY = pid_identity_for_task(task);
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
        .creator_pid_scope_generation = PID_IDENTITY.generation,
        .visible_creator_pid = PID_IDENTITY.process_name,
        .visible_last_pid = PID_IDENTITY.process_name,
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
    auto const PID_IDENTITY = pid_identity_for_task(task);
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
    segment->visible_last_pid = visible_pid(*segment, PID_IDENTITY, PID_IDENTITY.process_name);
    g_lock.unlock_irqrestore(COMMIT_FLAGS);
    return ADDR;
}

void detach_attachment_locked(ShmAttachment& attachment, ker::mod::sched::task::Task* task) {
    auto const PID_IDENTITY = pid_identity_for_task(task);
    auto* segment = find_retained_segment_by_id(attachment.shmid);
    if (segment != nullptr) {
        if (segment->attach_count > 0) {
            segment->attach_count--;
        }
        segment->last_pid = task != nullptr ? process_pid_for_task(task) : attachment.pid;
        segment->visible_last_pid = visible_pid(*segment, PID_IDENTITY, PID_IDENTITY.process_name);
        maybe_release_destroyed_segment(*segment);
    }

    attachment = {};
}

auto shmdt_impl(ker::mod::sched::task::Task* task, uint64_t shmaddr) -> uint64_t {
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

void fill_stat(const ShmSegment& segment, const ShmPidIdentity& reader, ker::abi::shm::ShmidDs& out) {
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
    out.shm_cpid = static_cast<int64_t>(visible_pid(segment, reader, segment.visible_creator_pid));
    out.shm_lpid = static_cast<int64_t>(visible_pid(segment, reader, segment.visible_last_pid));
    out.shm_nattch = segment.attach_count;
}

auto shmctl_impl(ker::mod::sched::task::Task* task, int shmid, int cmd, uint64_t buf_addr) -> uint64_t {
    auto const* ipc_namespace = task != nullptr ? task->ipc_namespace() : nullptr;
    if (ipc_namespace == nullptr) {
        return to_errno(ESRCH);
    }
    auto const CREDENTIALS = task->credentials();
    auto const PID_IDENTITY = pid_identity_for_task(task);
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
            fill_stat(*segment, PID_IDENTITY, stat);
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
            return shmdt_impl(current_task(), a1);
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

// Native-only page-table observer is defined in the native test translation unit.
auto visible_leaf_raw(ker::mod::mm::paging::PageTable* root, uint64_t address) -> uint64_t;

namespace {
namespace visible_task = ker::mod::sched::task;
namespace visible_pid_scope = ker::mod::sched::pid_ns;
namespace visible_ipc = ker::mod::sched::ipc;
namespace visible_resource = ker::mod::sched::resource;
namespace visible_mm = ker::mod::mm;
namespace visible_phys = ker::mod::mm::phys;
namespace visible_virt = ker::mod::mm::virt;
namespace visible_paging = ker::mod::mm::paging;
namespace visible_copy = ker::mod::sys::usercopy;
namespace visible_security = ker::mod::security;
namespace visible_abi = ker::abi::shm;

constexpr uint64_t VISIBLE_PAGE = visible_paging::PAGE_SIZE;
constexpr uint64_t VISIBLE_OUTPUT = 0x49000000ULL;
constexpr uint64_t VISIBLE_ATTACHMENT = 0x4B000000ULL;
constexpr uint64_t VISIBLE_STAT = VISIBLE_OUTPUT + 128;
constexpr uint8_t VISIBLE_GUARD = 0xA7;

auto visible_physical(const void* page) -> uint64_t {
    return reinterpret_cast<uint64_t>(visible_mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
}

// These quiescent BSP tests count actual objects, not seeded table entries or
// globally free pages (the page-table allocator may retain its warm pool).
struct VisiblePidBaseline {
    size_t segments = segment_count();
    size_t active_attachments{};
    size_t publishing_attachments{};
    size_t pid_scopes = visible_pid_scope::live_namespace_count();
    size_t ipc_scopes = visible_ipc::live_namespace_count();
    visible_phys::PhysicalOwnerSnapshot shared{};

    VisiblePidBaseline() {
        uint64_t const FLAGS = g_lock.lock_irqsave();
        for (auto const& attachment : g_attachments) {
            active_attachments += attachment.active ? 1 : 0;
            publishing_attachments += attachment.publishing ? 1 : 0;
        }
        g_lock.unlock_irqrestore(FLAGS);
        visible_phys::PhysicalBalanceSnapshot balance{};
        visible_phys::get_physical_balance_snapshot(balance);
        shared = balance.owners[static_cast<size_t>(visible_mm::PhysicalPageOwner::USER_SHARED_MEMORY)];
    }

    auto equals(const VisiblePidBaseline& other) const -> bool {
        return segments == other.segments && active_attachments == other.active_attachments &&
               publishing_attachments == other.publishing_attachments && pid_scopes == other.pid_scopes && ipc_scopes == other.ipc_scopes &&
               shared.pages == other.shared.pages && shared.objects == other.shared.objects;
    }
};

// Own the mutable Task and optionally its private user root. A bound thread
// borrows only another fixture member's root; it dies before that member.
struct VisiblePidOwnedTask {
    std::unique_ptr<visible_task::Task> subject{new (std::nothrow) visible_task::Task{}};
    visible_paging::PageTable* root{};
    bool owns_root{};
    bool& cleanup_ok;

    explicit VisiblePidOwnedTask(bool& clean) : cleanup_ok(clean) {}
    VisiblePidOwnedTask(const VisiblePidOwnedTask&) = delete;
    auto operator=(const VisiblePidOwnedTask&) -> VisiblePidOwnedTask& = delete;
    ~VisiblePidOwnedTask() { clear(); }

    auto init(bool private_root = true, bool output_page = true, uint32_t uid = 100) -> bool {
        if (subject == nullptr) return false;
        subject->pid = visible_task::get_next_pid();
        subject->type = visible_task::TaskType::PROCESS;
        visible_security::Snapshot credentials;
        if (visible_security::Snapshot::restore_fields(
                {.user = {uid, uid, uid}, .group = {uid, uid, uid}, .authority_ceiling = 0, .no_new_privileges = true}, credentials) != 0 ||
            visible_resource::initialize_credentials(*subject, credentials) != 0)
            return false;
        if (!private_root) return true;
        root = visible_virt::create_pagemap_may_fail();
        if (root == nullptr) return false;
        owns_root = true;
        subject->pagemap = root;
        return !output_page || map(VISIBLE_OUTPUT, VISIBLE_GUARD) != nullptr;
    }

    auto map(uint64_t address, uint8_t fill) -> uint8_t* {
        if (root == nullptr || visible_virt::is_page_mapped_or_reserved(root, address)) return nullptr;
        auto* page = static_cast<uint8_t*>(
            visible_phys::page_alloc(visible_mm::PhysicalPageOwner::SELFTEST, VISIBLE_PAGE, "sysv-visible-pid-ktest"));
        if (page == nullptr) return nullptr;
        std::memset(page, fill, VISIBLE_PAGE);
        // Allocation's single reference transfers into this actual PTE.
        visible_virt::map_page(root, address, visible_physical(page), visible_paging::page_types::USER);
        return page;
    }

    void clear() {
        if (subject == nullptr) return;
        // Metadata is detached first; root teardown later releases PTE refs.
        shm_cleanup_for_task(subject.get());
        cleanup_ok &= subject->usercopy_pagemap_accesses.load(std::memory_order_acquire) == 0;
        cleanup_ok &= subject->ref_count.load(std::memory_order_acquire) == 1;
        if (root != nullptr) {
            cleanup_ok &= subject->detach_pagemap_after_usercopy_quiescence() == root;
            if (owns_root) {
                visible_virt::destroy_user_space(root, subject->pid, "sysv-visible-pid-ktest", "private-fixture");
                visible_virt::release_pagemap(root, subject->pid);
            }
        }
        root = nullptr;
        owns_root = false;
        subject.reset();
    }
};

struct VisiblePidFixture {
    bool& cleanup_ok;
    visible_pid_scope::NamespaceRef first_pid, second_pid;
    visible_ipc::NamespaceRef first_ipc, second_ipc;
    std::array<std::unique_ptr<VisiblePidOwnedTask>, 4> actors{};
    std::unique_ptr<visible_task::Task> host_cleanup{new (std::nothrow) visible_task::Task{}};
    std::array<int, 4> host_ids{-1, -1, -1, -1};
    size_t host_id_count{};

    explicit VisiblePidFixture(bool& clean) : cleanup_ok(clean) {}
    ~VisiblePidFixture() {
        // Reverse creation order also releases every bound canonical owner.
        for (size_t index = actors.size(); index != 0; --index) actors[index - 1].reset();
        if (host_cleanup != nullptr) {
            for (size_t index = 0; index < host_id_count; ++index)
                cleanup_ok &= shmctl_impl(host_cleanup.get(), host_ids[index], visible_abi::IPC_RMID, 0) == 0;
        }
        shm_retire_namespace(first_ipc);
        shm_retire_namespace(second_ipc);
    }

    auto scopes(bool second = false) -> bool {
        return host_cleanup != nullptr && visible_pid_scope::create_namespace(first_pid) == 0 &&
               visible_ipc::create_namespace(first_ipc) == 0 &&
               (!second || (visible_pid_scope::create_namespace(second_pid) == 0 && visible_ipc::create_namespace(second_ipc) == 0));
    }

    auto make(size_t index, bool root = true, bool output = true, uint32_t uid = 100) -> VisiblePidOwnedTask* {
        if (index >= actors.size() || actors[index] != nullptr) return nullptr;
        actors[index].reset(new (std::nothrow) VisiblePidOwnedTask(cleanup_ok));
        return actors[index] != nullptr && actors[index]->init(root, output, uid) ? actors[index].get() : nullptr;
    }

    auto private_actor(size_t index, bool second = false, uint32_t uid = 100) -> VisiblePidOwnedTask* {
        auto* actor = make(index, true, true, uid);
        if (actor == nullptr || !actor->subject->initialize_pid_namespace(second ? second_pid : first_pid) ||
            !actor->subject->initialize_ipc_namespace(second ? second_ipc : first_ipc))
            return nullptr;
        return actor;
    }

    auto bound_thread(size_t index, VisiblePidOwnedTask& parent) -> VisiblePidOwnedTask* {
        auto* actor = make(index, false);
        if (actor == nullptr) return nullptr;
        actor->subject->is_thread = true;
        actor->subject->owner_pid = parent.subject->pid;
        if (!visible_resource::bind_thread_owner(*actor->subject, *parent.subject)) return nullptr;
        actor->root = parent.root;
        actor->subject->pagemap = parent.root;
        return actor;
    }

    auto create(VisiblePidOwnedTask& actor, int mode = 0600, int key = visible_abi::IPC_PRIVATE) -> int {
        int64_t const RESULT = static_cast<int64_t>(shmget_impl(actor.subject.get(), key, VISIBLE_PAGE, mode | visible_abi::IPC_CREAT));
        if (RESULT <= 0) return -1;
        if (actor.subject->ipc_namespace()->is_host()) {
            if (host_id_count == host_ids.size()) {
                cleanup_ok &= shmctl_impl(actor.subject.get(), static_cast<int>(RESULT), visible_abi::IPC_RMID, 0) == 0;
                return -1;
            }
            host_ids[host_id_count++] = static_cast<int>(RESULT);
        }
        return static_cast<int>(RESULT);
    }
};

template <class Case>
auto run_visible_pid_case(Case&& run) -> bool {
    VisiblePidBaseline const BEFORE;
    bool cleanup_ok = true;
    bool ok = false;
    {
        VisiblePidFixture fixture(cleanup_ok);
        ok = run(fixture);
    }
    VisiblePidBaseline const AFTER;
    return ok && cleanup_ok && BEFORE.equals(AFTER);
}

// Observe private metadata under its real lock, without changing table slots.
struct VisiblePidSegmentState {
    bool found{};
    uint64_t creator{}, owner{}, last{}, generation{}, visible_creator{}, visible_last{}, attachments{};
    void* backing{};
    bool marked{};
};

auto visible_segment_state(int id) -> VisiblePidSegmentState {
    VisiblePidSegmentState out;
    uint64_t const FLAGS = g_lock.lock_irqsave();
    if (auto const* segment = find_retained_segment_by_id(id); segment != nullptr) {
        out = {.found = true,
               .creator = segment->creator_pid,
               .owner = segment->owner_pid,
               .last = segment->last_pid,
               .generation = segment->creator_pid_scope_generation,
               .visible_creator = segment->visible_creator_pid,
               .visible_last = segment->visible_last_pid,
               .attachments = segment->attach_count,
               .backing = segment->backing,
               .marked = segment->marked_destroy};
    }
    g_lock.unlock_irqrestore(FLAGS);
    return out;
}

auto visible_attachment_owned(const visible_task::Task& actor, int id, uint64_t address) -> bool {
    uint64_t const FLAGS = g_lock.lock_irqsave();
    auto const* attachment = find_attachment(visible_task::process_pid(actor), address);
    bool const OWNED = attachment != nullptr && attachment->active && !attachment->publishing && attachment->shmid == id &&
                       attachment->pid == visible_task::process_pid(actor) && attachment->size == VISIBLE_PAGE;
    g_lock.unlock_irqrestore(FLAGS);
    return OWNED;
}

auto visible_expected_stat(const VisiblePidOwnedTask& reader, int id, uint64_t creator, uint64_t last, uint64_t attachments,
                           int mode = 0600, int key = visible_abi::IPC_PRIVATE) -> visible_abi::ShmidDs {
    visible_abi::ShmidDs expected;
    // Initialize padding too; successful STAT's complete120-byte output is
    // compared with this independently specified old ABI image and guards.
    std::memset(&expected, 0, sizeof(expected));
    expected.shm_perm.key = key;
    expected.shm_perm.uid = 100;
    expected.shm_perm.gid = 100;
    expected.shm_perm.cuid = 100;
    expected.shm_perm.cgid = 100;
    expected.shm_perm.mode = static_cast<uint32_t>(mode);
    expected.shm_perm.seq = id;
    expected.shm_segsz = VISIBLE_PAGE;
    expected.shm_cpid = static_cast<int64_t>(creator);
    expected.shm_lpid = static_cast<int64_t>(last);
    expected.shm_nattch = attachments;
    static_cast<void>(reader);
    return expected;
}

auto visible_fill_output(VisiblePidOwnedTask& actor) -> bool {
    std::array<uint8_t, sizeof(visible_abi::ShmidDs) + 32> bytes{};
    bytes.fill(VISIBLE_GUARD);
    return visible_copy::copy_to_task(*actor.subject, VISIBLE_STAT - 16, bytes.data(), bytes.size());
}

auto visible_stat_matches(VisiblePidOwnedTask& actor, int id, uint64_t creator, uint64_t last, uint64_t attachments, int mode = 0600,
                          int key = visible_abi::IPC_PRIVATE) -> bool {
    uint32_t const TASK_REFS = actor.subject->ref_count.load(std::memory_order_acquire);
    uint32_t const ROOT_REFS = visible_phys::page_ref_get(actor.root);
    uint32_t const ROOT_READERS = visible_virt::selftest_user_pagemap_reader_count(actor.root);
    if (!visible_fill_output(actor) || shmctl_impl(actor.subject.get(), id, visible_abi::IPC_STAT, VISIBLE_STAT) != 0) return false;
    std::array<uint8_t, sizeof(visible_abi::ShmidDs) + 32> bytes{};
    if (!visible_copy::copy_from_task(*actor.subject, VISIBLE_STAT - 16, bytes.data(), bytes.size())) return false;
    auto const EXPECTED = visible_expected_stat(actor, id, creator, last, attachments, mode, key);
    bool ok = std::memcmp(bytes.data() + 16, &EXPECTED, sizeof(EXPECTED)) == 0;
    for (size_t index = 0; index < 16; ++index) ok &= bytes[index] == VISIBLE_GUARD && bytes[bytes.size() - 1 - index] == VISIBLE_GUARD;
    return ok && actor.subject->usercopy_pagemap_accesses.load(std::memory_order_acquire) == 0 &&
           actor.subject->ref_count.load(std::memory_order_acquire) == TASK_REFS && visible_phys::page_ref_get(actor.root) == ROOT_REFS &&
           visible_virt::selftest_user_pagemap_reader_count(actor.root) == ROOT_READERS;
}

auto visible_denied_stat(VisiblePidOwnedTask& actor, int id, int error) -> bool {
    if (!visible_fill_output(actor) || shmctl_impl(actor.subject.get(), id, visible_abi::IPC_STAT, VISIBLE_STAT) != to_errno(error))
        return false;
    std::array<uint8_t, sizeof(visible_abi::ShmidDs) + 32> bytes{};
    if (!visible_copy::copy_from_task(*actor.subject, VISIBLE_STAT - 16, bytes.data(), bytes.size())) return false;
    return std::ranges::all_of(bytes, [](uint8_t byte) { return byte == VISIBLE_GUARD; });
}

auto visible_attach(VisiblePidOwnedTask& actor, int id, uint64_t address = VISIBLE_ATTACHMENT) -> bool {
    auto const BEFORE = visible_segment_state(id);
    if (!BEFORE.found || shmat_impl(actor.subject.get(), id, address, 0) != address) return false;
    return visible_attachment_owned(*actor.subject, id, address) &&
           visible_copy::mapped_physical_address(*actor.subject, address) == visible_physical(BEFORE.backing) &&
           visible_phys::page_ref_get(BEFORE.backing) == BEFORE.attachments + 2;
}

auto visible_detach(VisiblePidOwnedTask& actor, uint64_t address = VISIBLE_ATTACHMENT) -> bool {
    return shmdt_impl(actor.subject.get(), address) == 0 &&
           visible_copy::mapped_physical_address(*actor.subject, address) == visible_virt::PADDR_INVALID;
}
}  // namespace

auto shm_selftest_visible_pid_host() -> bool {
    for (bool private_ipc : {false, true}) {
        if (!run_visible_pid_case([private_ipc](VisiblePidFixture& fixture) {
                if (!fixture.scopes()) return false;
                auto* process = fixture.make(0);
                if (process == nullptr || (private_ipc && !process->subject->initialize_ipc_namespace(fixture.first_ipc))) return false;
                auto* thread = fixture.bound_thread(1, *process);
                if (thread == nullptr || thread->subject->pid == process->subject->pid ||
                    thread->subject->credential_owner() != process->subject.get() ||
                    process->subject->ref_count.load(std::memory_order_acquire) != 2)
                    return false;
                int const ID = fixture.create(*process);
                if (ID < 0 || !visible_stat_matches(*process, ID, process->subject->pid, process->subject->pid, 0) ||
                    !visible_attach(*thread, ID) || !visible_stat_matches(*thread, ID, process->subject->pid, process->subject->pid, 1))
                    return false;
                auto const STATE = visible_segment_state(ID);
                if (STATE.generation != 0 || STATE.creator != process->subject->pid || STATE.owner != process->subject->pid ||
                    STATE.last != process->subject->pid || !visible_detach(*thread) ||
                    !visible_stat_matches(*process, ID, process->subject->pid, process->subject->pid, 0))
                    return false;
                fixture.actors[1].reset();
                return process->subject->ref_count.load(std::memory_order_acquire) == 1;
            }))
            return false;
    }
    return true;
}

auto shm_selftest_visible_pid_private() -> bool {
    return run_visible_pid_case([](VisiblePidFixture& fixture) {
        if (!fixture.scopes()) return false;
        auto* creator = fixture.private_actor(0);
        auto* child = fixture.make(1);
        if (creator == nullptr || child == nullptr || !visible_resource::inherit(*child->subject, *creator->subject)) return false;
        auto* thread = fixture.bound_thread(2, *creator);
        if (thread == nullptr || creator->subject->namespace_tid() != 1 || child->subject->namespace_tid() != 2 ||
            thread->subject->namespace_tid() != 3 || visible_task::namespace_process_pid(*thread->subject) != 1)
            return false;
        int const ID = fixture.create(*creator);
        if (ID < 0 || !visible_stat_matches(*creator, ID, 1, 1, 0) || !visible_attach(*child, ID) ||
            !visible_stat_matches(*creator, ID, 1, 2, 1))
            return false;
        auto const CHILD = visible_segment_state(ID);
        if (CHILD.last != child->subject->pid || !visible_detach(*child) || !visible_stat_matches(*creator, ID, 1, 2, 0) ||
            !visible_attach(*thread, ID) || !visible_stat_matches(*child, ID, 1, 1, 1))
            return false;
        auto const THREAD = visible_segment_state(ID);
        return THREAD.creator == creator->subject->pid && THREAD.owner == creator->subject->pid && THREAD.last == creator->subject->pid &&
               THREAD.generation == fixture.first_pid.generation() && visible_detach(*thread) &&
               visible_stat_matches(*creator, ID, 1, 1, 0);
    });
}

auto shm_selftest_visible_pid_historical_creator() -> bool {
    return run_visible_pid_case([](VisiblePidFixture& fixture) {
        if (!fixture.scopes()) return false;
        auto* creator = fixture.private_actor(0);
        auto* reader = fixture.make(1);
        if (creator == nullptr || reader == nullptr || !visible_resource::inherit(*reader->subject, *creator->subject)) return false;
        int const ID = fixture.create(*creator);
        if (ID < 0 || !visible_attach(*creator, ID) || !visible_detach(*creator)) return false;
        uint64_t const CREATOR_PHYSICAL = creator->subject->pid;
        uint64_t const GENERATION = fixture.first_pid.generation();
        fixture.actors[0].reset();
        auto const STATE = visible_segment_state(ID);
        // The actual creator Task and root are gone; only value history and
        // the independently inherited reader/IPC references remain.
        return STATE.found && STATE.creator == CREATOR_PHYSICAL && STATE.generation == GENERATION && STATE.visible_creator == 1 &&
               STATE.visible_last == 1 && visible_stat_matches(*reader, ID, 1, 1, 0);
    });
}

auto shm_selftest_visible_pid_foreign_scopes() -> bool {
    for (bool private_ipc : {false, true}) {
        if (!run_visible_pid_case([private_ipc](VisiblePidFixture& fixture) {
                if (!fixture.scopes(true)) return false;
                auto* creator = fixture.make(0);
                auto* foreign = fixture.make(1);
                if (creator == nullptr || foreign == nullptr || !creator->subject->initialize_pid_namespace(fixture.first_pid) ||
                    !foreign->subject->initialize_pid_namespace(fixture.second_pid) ||
                    (private_ipc && (!creator->subject->initialize_ipc_namespace(fixture.first_ipc) ||
                                     !foreign->subject->initialize_ipc_namespace(fixture.first_ipc))) ||
                    creator->subject->namespace_tid() != 1 || foreign->subject->namespace_tid() != 1 ||
                    fixture.first_pid.generation() == fixture.second_pid.generation())
                    return false;
                int const ID = fixture.create(*creator);
                if (ID < 0 || !visible_stat_matches(*foreign, ID, 0, 0, 0) || !visible_attach(*foreign, ID) ||
                    !visible_stat_matches(*creator, ID, 1, 0, 1) || !visible_stat_matches(*foreign, ID, 0, 0, 1) ||
                    !visible_detach(*foreign) || !visible_stat_matches(*creator, ID, 1, 0, 0))
                    return false;
                auto const FOREIGN = visible_segment_state(ID);
                if (FOREIGN.last != foreign->subject->pid || FOREIGN.visible_last != 0 || !visible_attach(*creator, ID) ||
                    !visible_detach(*creator) || !visible_stat_matches(*creator, ID, 1, 1, 0))
                    return false;
                // PID closure alone does not revoke independently open IPC identity.
                fixture.first_pid.close();
                return !fixture.first_ipc.closed() && visible_stat_matches(*creator, ID, 1, 1, 0) &&
                       visible_stat_matches(*foreign, ID, 0, 0, 0);
            }))
            return false;
    }
    return true;
}

auto shm_selftest_visible_pid_scope_reuse() -> bool {
    return run_visible_pid_case([](VisiblePidFixture& fixture) {
        size_t const PID_BASELINE = visible_pid_scope::live_namespace_count();
        if (!fixture.scopes()) return false;
        auto* creator = fixture.private_actor(0);
        auto* reader = fixture.make(1);
        if (creator == nullptr || reader == nullptr || !visible_resource::inherit(*reader->subject, *creator->subject)) return false;
        auto* thread = fixture.bound_thread(2, *creator);
        int const ID = fixture.create(*creator);
        if (thread == nullptr || ID < 0 || !visible_stat_matches(*reader, ID, 1, 1, 0)) return false;
        uint64_t const OLD_GENERATION = fixture.first_pid.generation();
        for (size_t index = fixture.actors.size(); index != 0; --index) fixture.actors[index - 1].reset();
        fixture.first_pid.reset();
        // Only the IPC scope and value-only segment remain. Every old Task,
        // bound owner and PID reference has actually been destroyed.
        if (visible_pid_scope::live_namespace_count() != PID_BASELINE || !visible_segment_state(ID).found ||
            visible_pid_scope::create_namespace(fixture.first_pid) != 0)
            return false;
        auto* fresh = fixture.private_actor(0);
        return fresh != nullptr && fresh->subject->namespace_tid() == 1 && fixture.first_pid.generation() != OLD_GENERATION &&
               visible_stat_matches(*fresh, ID, 0, 0, 0) && visible_segment_state(ID).generation == OLD_GENERATION;
    });
}

auto shm_selftest_visible_pid_fork_retirement() -> bool {
    return run_visible_pid_case([](VisiblePidFixture& fixture) {
        if (!fixture.scopes(true)) return false;
        auto* parent = fixture.private_actor(0);
        auto* child = fixture.make(1, true, false);
        auto* foreign_parent = fixture.private_actor(2, true);
        auto* retained = fixture.make(3, true, false);
        if (parent == nullptr || child == nullptr || foreign_parent == nullptr || retained == nullptr ||
            !visible_resource::inherit(*child->subject, *parent->subject) ||
            !visible_resource::inherit(*retained->subject, *foreign_parent->subject))
            return false;
        int const ID = fixture.create(*parent);
        if (ID < 0 || !visible_attach(*parent, ID)) return false;
        auto const ORIGINAL = visible_segment_state(ID);
        {
            ker::syscall::vmem::SharedVmemPublicationGuard publication;
            if (!visible_virt::deep_copy_user_pagemap_cow(parent->root, child->root) ||
                !shm_clone_for_fork_locked(parent->subject.get(), child->subject.get()) ||
                !visible_virt::deep_copy_user_pagemap_cow(parent->root, retained->root) ||
                !shm_clone_for_fork_locked(parent->subject.get(), retained->subject.get()))
                return false;
        }
        auto const CLONED = visible_segment_state(ID);
        uint64_t const PRIVATE_PHYSICAL = visible_copy::mapped_physical_address(*parent->subject, VISIBLE_OUTPUT);
        uint64_t const SHARED_BITS = visible_paging::PAGE_SHARED | visible_paging::PAGE_WRITE;
        for (auto* actor : {parent, child, retained}) {
            uint64_t const SHARED_LEAF = visible_leaf_raw(actor->root, VISIBLE_ATTACHMENT);
            uint64_t const PRIVATE_LEAF = visible_leaf_raw(actor->root, VISIBLE_OUTPUT);
            if ((SHARED_LEAF & SHARED_BITS) != SHARED_BITS || (SHARED_LEAF & visible_paging::PAGE_COW) != 0 ||
                (PRIVATE_LEAF & visible_paging::PAGE_COW) == 0 || (PRIVATE_LEAF & visible_paging::PAGE_WRITE) != 0 ||
                visible_copy::mapped_physical_address(*actor->subject, VISIBLE_OUTPUT) != PRIVATE_PHYSICAL)
                return false;
        }
        if (CLONED.attachments != 3 || CLONED.last != ORIGINAL.last || CLONED.visible_last != ORIGINAL.visible_last ||
            visible_phys::page_ref_get(ORIGINAL.backing) != 4 ||
            visible_copy::mapped_physical_address(*child->subject, VISIBLE_ATTACHMENT, true) != visible_physical(ORIGINAL.backing) ||
            visible_copy::mapped_physical_address(*retained->subject, VISIBLE_ATTACHMENT, true) != visible_physical(ORIGINAL.backing) ||
            !visible_attachment_owned(*child->subject, ID, VISIBLE_ATTACHMENT) ||
            !visible_attachment_owned(*retained->subject, ID, VISIBLE_ATTACHMENT) || !visible_stat_matches(*child, ID, 1, 1, 3))
            return false;
        // STAT copyout genuinely resolves the child's private COW scratch,
        // while the SysV PTE remains the exact shared backing in all roots.
        if (visible_copy::mapped_physical_address(*child->subject, VISIBLE_OUTPUT) == PRIVATE_PHYSICAL ||
            visible_copy::mapped_physical_address(*parent->subject, VISIBLE_OUTPUT) != PRIVATE_PHYSICAL ||
            visible_copy::mapped_physical_address(*child->subject, VISIBLE_ATTACHMENT) != visible_physical(ORIGINAL.backing))
            return false;
        // Ordinary fork COW keeps the shared mapping shared. Verify actual
        // cross-root writable bytes, not merely coincident page translations.
        uint64_t const MARKER = 0xB42C001122334455ULL;
        uint64_t observed = 0;
        if (!visible_copy::copy_value_to_task(*parent->subject, VISIBLE_ATTACHMENT, MARKER) ||
            !visible_copy::copy_value_from_task(*child->subject, VISIBLE_ATTACHMENT, observed) || observed != MARKER)
            return false;
        fixture.actors[1].reset();
        fixture.actors[0].reset();
        auto const LAST = visible_segment_state(ID);
        if (LAST.attachments != 1 || LAST.last != ORIGINAL.last || LAST.visible_last != ORIGINAL.visible_last ||
            visible_phys::page_ref_get(ORIGINAL.backing) != 2)
            return false;
        shm_retire_namespace(fixture.first_ipc);
        auto const RETIRED = visible_segment_state(ID);
        if (!fixture.first_ipc.closed() || !RETIRED.marked || RETIRED.attachments != 1 ||
            shmctl_impl(retained->subject.get(), ID, visible_abi::IPC_STAT, VISIBLE_STAT) != to_errno(EINVAL) ||
            !visible_detach(*retained) || visible_segment_state(ID).found || visible_phys::page_ref_get(ORIGINAL.backing) != 0)
            return false;
        return true;
    });
}

auto shm_selftest_visible_pid_denial_guards() -> bool {
    return run_visible_pid_case([](VisiblePidFixture& fixture) {
        if (!fixture.scopes(true)) return false;
        auto* owner = fixture.private_actor(0);
        auto* denied = fixture.make(1, true, true, 200);
        auto* foreign = fixture.private_actor(2, true);
        auto* unbound = fixture.make(3);
        if (owner == nullptr || denied == nullptr || foreign == nullptr || unbound == nullptr ||
            !visible_resource::inherit(*denied->subject, *owner->subject) || owner->subject->credentials().authority_ceiling() != 0 ||
            denied->subject->credentials().authority_ceiling() != 0)
            return false;
        unbound->subject->is_thread = true;
        if (pid_identity_for_task(nullptr).valid || pid_identity_for_task(unbound->subject.get()).valid ||
            !visible_denied_stat(*unbound, 1, ESRCH) ||
            shmget_impl(nullptr, visible_abi::IPC_PRIVATE, VISIBLE_PAGE, 0600) != to_errno(ESRCH) ||
            shmat_impl(nullptr, 1, VISIBLE_ATTACHMENT, 0) != to_errno(ESRCH) ||
            shmdt_impl(nullptr, VISIBLE_ATTACHMENT) != to_errno(ESRCH) ||
            shmctl_impl(nullptr, 1, visible_abi::IPC_STAT, 0) != to_errno(ESRCH))
            return false;
        int const KEY = unused_fixture_key();
        int const ID = fixture.create(*owner, 0600, KEY);
        if (ID < 0 || !visible_denied_stat(*foreign, ID, EINVAL) || !visible_denied_stat(*denied, ID, EACCES) ||
            shmat_impl(foreign->subject.get(), ID, VISIBLE_ATTACHMENT, 0) != to_errno(EINVAL) ||
            shmat_impl(denied->subject.get(), ID, VISIBLE_ATTACHMENT, 0) != to_errno(EACCES) ||
            shmctl_impl(denied->subject.get(), ID, visible_abi::IPC_RMID, 0) != to_errno(EPERM) ||
            shmget_impl(denied->subject.get(), KEY, 0, 0400) != to_errno(EACCES) || !visible_stat_matches(*owner, ID, 1, 1, 0, 0600, KEY))
            return false;
        // An unmapped suffix may leave the existing successfully copied prefix;
        // this test deliberately does not assert all-or-nothing EFAULT.
        uint64_t const PARTIAL = VISIBLE_OUTPUT + VISIBLE_PAGE - 64;
        if (shmctl_impl(owner->subject.get(), ID, visible_abi::IPC_STAT, PARTIAL) != to_errno(EFAULT)) return false;
        std::array<uint8_t, 64> prefix{};
        auto const EXPECTED = visible_expected_stat(*owner, ID, 1, 1, 0, 0600, KEY);
        if (!visible_copy::copy_from_task(*owner->subject, PARTIAL, prefix.data(), prefix.size()) ||
            std::memcmp(prefix.data(), &EXPECTED, prefix.size()) != 0)
            return false;
        // Replacing a retained attachment's PTE must not let DETACH unmap the
        // new, unrelated frame. The old segment mapping ref was already dropped.
        if (!visible_attach(*owner, ID)) return false;
        auto const ATTACHED = visible_segment_state(ID);
        visible_virt::unmap_page(owner->root, VISIBLE_ATTACHMENT);
        auto* replacement = owner->map(VISIBLE_ATTACHMENT, 0xD2);
        if (replacement == nullptr || shmdt_impl(owner->subject.get(), VISIBLE_ATTACHMENT) != 0 ||
            visible_copy::mapped_physical_address(*owner->subject, VISIBLE_ATTACHMENT) != visible_physical(replacement) ||
            visible_segment_state(ID).attachments != 0 || visible_phys::page_ref_get(ATTACHED.backing) != 1)
            return false;
        visible_virt::unmap_page(owner->root, VISIBLE_ATTACHMENT);
        if (!visible_attach(*owner, ID)) return false;
        shm_retire_namespace(fixture.first_ipc);
        if (!visible_denied_stat(*owner, ID, EIDRM) ||
            shmget_impl(owner->subject.get(), visible_abi::IPC_PRIVATE, VISIBLE_PAGE, 0600) != to_errno(EIDRM) ||
            shmat_impl(owner->subject.get(), ID, VISIBLE_ATTACHMENT + VISIBLE_PAGE, 0) != to_errno(EIDRM) ||
            shmctl_impl(owner->subject.get(), ID, visible_abi::IPC_RMID, 0) != to_errno(EIDRM) || !visible_detach(*owner))
            return false;
        return !visible_segment_state(ID).found;
    });
}
#endif

}  // namespace ker::syscall::shm
