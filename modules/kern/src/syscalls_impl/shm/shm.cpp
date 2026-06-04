#include "shm.hpp"

#include <abi/callnums/shm.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::syscall::shm {
namespace {
using log = ker::mod::dbg::logger<"shm">;

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
    uint32_t mode = 0;
    uint64_t creator_pid = 0;
    uint64_t owner_pid = 0;
    uint64_t last_pid = 0;
    uint64_t attach_count = 0;
    bool marked_destroy = false;
};

struct ShmAttachment {
    bool active = false;
    uint64_t pid = 0;
    int shmid = -1;
    uint64_t addr = 0;
    uint64_t size = 0;
};

ker::mod::sys::Spinlock g_lock;
std::array<ShmSegment, MAX_SHM_SEGMENTS> g_segments{};
std::array<ShmAttachment, MAX_SHM_ATTACHMENTS> g_attachments{};
int g_next_id = 1;

auto current_task() -> ker::mod::sched::task::Task* { return ker::mod::sched::get_current_task(); }

auto to_errno(int error) -> uint64_t { return static_cast<uint64_t>(-error); }

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

auto find_segment_by_id(int id) -> ShmSegment* {
    auto* it = std::ranges::find_if(g_segments, [id](const ShmSegment& segment) -> bool { return segment.active && segment.id == id; });
    return it != g_segments.end() ? &*it : nullptr;
}

auto find_segment_by_key(int key) -> ShmSegment* {
    auto* it = std::ranges::find_if(g_segments, [key](const ShmSegment& segment) -> bool { return segment.active && segment.key == key; });
    return it != g_segments.end() ? &*it : nullptr;
}

auto find_free_segment() -> ShmSegment* {
    auto* it = std::ranges::find_if(g_segments, [](const ShmSegment& segment) -> bool { return !segment.active; });
    return it != g_segments.end() ? &*it : nullptr;
}

auto find_free_attachment() -> ShmAttachment* {
    auto* it = std::ranges::find_if(g_attachments, [](const ShmAttachment& attachment) -> bool { return !attachment.active; });
    return it != g_attachments.end() ? &*it : nullptr;
}

auto find_attachment(uint64_t pid, uint64_t addr) -> ShmAttachment* {
    auto* it = std::ranges::find_if(g_attachments, [pid, addr](const ShmAttachment& attachment) -> bool {
        return attachment.active && attachment.pid == pid && attachment.addr == addr;
    });
    return it != g_attachments.end() ? &*it : nullptr;
}

auto range_is_free(ker::mod::sched::task::Task* task, uint64_t addr, uint64_t size) -> bool {
    for (uint64_t current = addr; current < addr + size; current += ker::mod::mm::paging::PAGE_SIZE) {
        if (ker::mod::mm::virt::is_page_mapped(task->pagemap, current)) {
            return false;
        }
    }
    return true;
}

auto find_free_range(ker::mod::sched::task::Task* task, uint64_t size, uint64_t hint) -> uint64_t {
    if (task == nullptr || task->pagemap == nullptr || size == 0) {
        return 0;
    }

    size = page_align_up(size);

    if (hint >= USER_SPACE_START && hint + size <= USER_SPACE_END && range_is_free(task, hint, size)) {
        return hint;
    }

    uint64_t current = SHM_SEARCH_START;
    while (current + size <= USER_SPACE_END) {
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

auto create_segment(int key, uint64_t size, int shmflg, ker::mod::sched::task::Task* task) -> uint64_t {
    if (size == 0) {
        return to_errno(EINVAL);
    }

    size = page_align_up(size);
    auto* slot = find_free_segment();
    if (slot == nullptr) {
        return to_errno(ENOSPC);
    }

    void* backing = ker::mod::mm::phys::page_alloc(size, "sysv-shm");
    if (backing == nullptr) {
        log::warn("out of physical memory for %llu-byte shared segment", static_cast<unsigned long long>(size));
        return to_errno(ENOMEM);
    }

    std::memset(backing, 0, size);
    if (!ker::mod::mm::phys::page_split_to_order0(backing)) {
        ker::mod::mm::phys::page_free(backing);
        return to_errno(ENOMEM);
    }

    int const ID = g_next_id++;
    uint64_t const PROCESS_PID = process_pid_for_task(task);
    *slot = {
        .active = true,
        .id = ID,
        .key = key,
        .backing = backing,
        .size = size,
        .page_count = size / ker::mod::mm::paging::PAGE_SIZE,
        .mode = static_cast<uint32_t>(shmflg & static_cast<int>(std::filesystem::perms::mask)),
        .creator_pid = PROCESS_PID,
        .owner_pid = PROCESS_PID,
        .last_pid = PROCESS_PID,
        .attach_count = 0,
        .marked_destroy = false,
    };

    return static_cast<uint64_t>(ID);
}

auto shmget_impl(int key, uint64_t size, int shmflg) -> uint64_t {
    auto* task = current_task();
    if (task == nullptr) {
        return to_errno(ESRCH);
    }

    uint64_t const FLAGS = g_lock.lock_irqsave();

    if (key != ker::abi::shm::IPC_PRIVATE) {
        if (auto* existing = find_segment_by_key(key); existing != nullptr) {
            if ((shmflg & ker::abi::shm::IPC_CREAT) != 0 && (shmflg & ker::abi::shm::IPC_EXCL) != 0) {
                g_lock.unlock_irqrestore(FLAGS);
                return to_errno(EEXIST);
            }
            if (size != 0 && page_align_up(size) > existing->size) {
                g_lock.unlock_irqrestore(FLAGS);
                return to_errno(EINVAL);
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

    uint64_t const RESULT = create_segment(key, size, shmflg, task);
    g_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

auto shmat_impl(int shmid, uint64_t shmaddr, int shmflg) -> uint64_t {
    auto* task = current_task();
    if (task == nullptr || task->pagemap == nullptr) {
        return to_errno(ESRCH);
    }

    uint64_t const FLAGS = g_lock.lock_irqsave();
    auto* segment = find_segment_by_id(shmid);
    if (segment == nullptr || segment->marked_destroy) {
        g_lock.unlock_irqrestore(FLAGS);
        return to_errno(EINVAL);
    }

    uint64_t const SIZE = segment->size;
    uint64_t const ADDR = shmaddr != 0 ? shmaddr : find_free_range(task, SIZE, 0);
    if (ADDR == 0 || ADDR % ker::mod::mm::paging::PAGE_SIZE != 0 || ADDR < USER_SPACE_START || ADDR + SIZE > USER_SPACE_END ||
        !range_is_free(task, ADDR, SIZE)) {
        g_lock.unlock_irqrestore(FLAGS);
        return to_errno(EINVAL);
    }

    auto* attachment = find_free_attachment();
    if (attachment == nullptr) {
        g_lock.unlock_irqrestore(FLAGS);
        return to_errno(ENOSPC);
    }

    uint64_t page_flags = ker::mod::mm::paging::PAGE_PRESENT | ker::mod::mm::paging::PAGE_USER | ker::mod::mm::paging::PAGE_SHARED;
    if ((shmflg & ker::abi::shm::SHM_RDONLY) == 0) {
        page_flags |= ker::mod::mm::paging::PAGE_WRITE;
    }

    for (uint64_t i = 0; i < segment->page_count; ++i) {
        auto* page = backing_page(*segment, i);
        ker::mod::mm::phys::page_ref_inc(page);
        ker::mod::mm::virt::map_page(task->pagemap, ADDR + (i * ker::mod::mm::paging::PAGE_SIZE), backing_paddr(*segment, i), page_flags);
    }

    *attachment = {
        .active = true,
        .pid = process_pid_for_task(task),
        .shmid = segment->id,
        .addr = ADDR,
        .size = SIZE,
    };
    segment->attach_count++;
    segment->last_pid = process_pid_for_task(task);

    g_lock.unlock_irqrestore(FLAGS);
    return ADDR;
}

void detach_attachment(ShmAttachment& attachment, ker::mod::sched::task::Task* task, bool unmap_pages) {
    auto* segment = find_segment_by_id(attachment.shmid);
    if (unmap_pages && task != nullptr && task->pagemap != nullptr) {
        for (uint64_t addr = attachment.addr; addr < attachment.addr + attachment.size; addr += ker::mod::mm::paging::PAGE_SIZE) {
            if (ker::mod::mm::virt::is_page_mapped(task->pagemap, addr)) {
                ker::mod::mm::virt::unmap_page(task->pagemap, addr);
            }
        }
    }

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

    uint64_t const FLAGS = g_lock.lock_irqsave();
    auto* attachment = find_attachment(process_pid_for_task(task), shmaddr);
    if (attachment == nullptr) {
        g_lock.unlock_irqrestore(FLAGS);
        return to_errno(EINVAL);
    }

    detach_attachment(*attachment, task, true);
    g_lock.unlock_irqrestore(FLAGS);
    return 0;
}

void fill_stat(const ShmSegment& segment, ker::abi::shm::ShmidDs& out) {
    out = {};
    out.shm_perm.key = segment.key;
    out.shm_perm.uid = 0;
    out.shm_perm.gid = 0;
    out.shm_perm.cuid = 0;
    out.shm_perm.cgid = 0;
    out.shm_perm.mode = segment.mode;
    out.shm_perm.seq = segment.id;
    out.shm_segsz = segment.size;
    out.shm_atime = 0;
    out.shm_dtime = 0;
    out.shm_ctime = 0;
    out.shm_cpid = static_cast<int32_t>(segment.creator_pid);
    out.shm_lpid = static_cast<int32_t>(segment.last_pid);
    out.shm_nattch = segment.attach_count;
}

auto shmctl_impl(int shmid, int cmd, uint64_t buf_addr) -> uint64_t {
    uint64_t const FLAGS = g_lock.lock_irqsave();
    auto* segment = find_segment_by_id(shmid);
    if (segment == nullptr) {
        g_lock.unlock_irqrestore(FLAGS);
        return to_errno(EINVAL);
    }

    switch (cmd) {
        case ker::abi::shm::IPC_RMID:
            segment->marked_destroy = true;
            maybe_release_destroyed_segment(*segment);
            g_lock.unlock_irqrestore(FLAGS);
            return 0;
        case ker::abi::shm::IPC_STAT: {
            if (buf_addr == 0) {
                g_lock.unlock_irqrestore(FLAGS);
                return to_errno(EFAULT);
            }
            ker::abi::shm::ShmidDs stat{};
            fill_stat(*segment, stat);
            g_lock.unlock_irqrestore(FLAGS);
            auto* user_buf = reinterpret_cast<ker::abi::shm::ShmidDs*>(buf_addr);
            *user_buf = stat;
            return 0;
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
            return shmget_impl(static_cast<int>(a1), a2, static_cast<int>(a3));
        case ker::abi::shm::ops::ATTACH:
            return shmat_impl(static_cast<int>(a1), a2, static_cast<int>(a3));
        case ker::abi::shm::ops::DETACH:
            return shmdt_impl(a1);
        case ker::abi::shm::ops::CTL:
            return shmctl_impl(static_cast<int>(a1), static_cast<int>(a2), a3);
        default:
            return to_errno(EINVAL);
    }
}

auto shm_clone_for_fork(ker::mod::sched::task::Task* parent, ker::mod::sched::task::Task* child) -> bool {
    if (parent == nullptr || child == nullptr) {
        return true;
    }

    uint64_t const FLAGS = g_lock.lock_irqsave();
    for (const auto& parent_attachment : g_attachments) {
        if (!parent_attachment.active || parent_attachment.pid != process_pid_for_task(parent)) {
            continue;
        }

        auto* segment = find_segment_by_id(parent_attachment.shmid);
        auto* child_attachment = find_free_attachment();
        if (segment == nullptr || child_attachment == nullptr) {
            g_lock.unlock_irqrestore(FLAGS);
            shm_cleanup_for_task(child);
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
    uint64_t const FLAGS = g_lock.lock_irqsave();
    for (auto& attachment : g_attachments) {
        if (attachment.active && attachment.pid == PID) {
            detach_attachment(attachment, task, false);
        }
    }
    g_lock.unlock_irqrestore(FLAGS);
}

}  // namespace ker::syscall::shm
