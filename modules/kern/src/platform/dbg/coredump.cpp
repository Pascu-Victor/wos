#include "coredump.hpp"

#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <net/wki/remote_compute.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <vfs/vfs.hpp>

namespace ker::mod::dbg::coredump {
namespace {
using log = ker::mod::dbg::logger<"cdmp">;

constexpr uint64_t COREDUMP_MAGIC = 0x504d55444f43534fULL;  // "WOSCODMP" little-endian-ish identifier
constexpr uint32_t COREDUMP_VERSION = 3;
constexpr size_t MAX_WALK_WARNINGS_PER_LEVEL = 8;
constexpr bool CAPTURE_USER_MEMORY_SNAPSHOT = false;

// x86-64 canonical address check: bits[63:47] must all be the same.
// For kernel HHDM addresses (bit 47 set) the upper 17 bits are all 1s.
constexpr int CANONICAL_SIGN_BIT = 47;
constexpr uint64_t CANONICAL_KERNEL_UPPER = 0x1ffffULL;

// Keep this in sync with tmpfs' internal flag.
constexpr int O_CREAT = 0100;  // octal = 64 decimal = 0x40 hex
constexpr uint32_t RING_SIZE = 4;

auto page_table_entry_at(ker::mod::mm::paging::PageTable* table, size_t index) -> ker::mod::mm::paging::PageTableEntry& {
    return table->entries.at(index);
}
[[maybe_unused]]
auto page_table_entry_at(const ker::mod::mm::paging::PageTable* table, size_t index) -> const ker::mod::mm::paging::PageTableEntry& {
    return table->entries.at(index);
}

bool is_ram(uint64_t phys) {
    for (auto* zone = ker::mod::mm::phys::get_zones(); zone != nullptr; zone = zone->next) {
        // zone->start is virtual (HHDM)
        // zone->len is length in bytes
        auto const ZONE_START_PHYS =
            reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(static_cast<ker::mod::mm::addr::vaddr_t>(zone->start)));
        if (phys >= ZONE_START_PHYS && phys < ZONE_START_PHYS + zone->len) {
            return true;
        }
    }
    return false;
}

struct CoreDumpHeader {
    uint64_t magic;
    uint32_t version;
    uint32_t header_size;

    uint64_t timestamp_quantums;
    uint64_t pid;
    uint64_t cpu;

    uint64_t int_num;
    uint64_t err_code;
    uint64_t cr2;
    uint64_t cr3;

    // Trap/exception frame at the time of fault.
    ker::mod::gates::InterruptFrame trap_frame;
    ker::mod::cpu::GPRegs trap_regs;

    // Saved user context frame/regs (if available).
    ker::mod::gates::InterruptFrame saved_frame;
    ker::mod::cpu::GPRegs saved_regs;

    uint64_t task_entry;
    uint64_t task_pagemap;

    uint64_t elf_header_addr;
    uint64_t program_header_addr;

    uint64_t segment_count;
    uint64_t segment_table_offset;

    uint64_t elf_size;
    uint64_t elf_offset;

    // Version 2 restart/snapshot metadata. The dump now contains all present
    // user pages, not just the faulting stack window.
    uint64_t segment_entry_size;
    uint64_t page_size;
    uint64_t snapshot_flags;

    uint64_t interp_base;
    uint64_t program_header_count;
    uint64_t program_header_ent_size;

    uint64_t thread_fs_base;
    uint64_t thread_gs_base;
    uint64_t thread_stack_base;
    uint64_t threadstack_size;
    uint64_t thread_tls_base;
    uint64_t threadtls_size;
    uint64_t thread_safe_stack;

    // File ABI payload: fixed raw arrays are intentional.
    // NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    char exe_path[ker::mod::sched::task::Task::EXE_PATH_MAX];
    char cwd[ker::mod::sched::task::Task::CWD_MAX];
    char root[ker::mod::sched::task::Task::CWD_MAX];
    char wait_channel[64];
    char wki_target_hostname[ker::mod::sched::task::Task::WKI_TARGET_HOSTNAME_MAX];
    char wki_submitter_hostname[ker::mod::sched::task::Task::WKI_TARGET_HOSTNAME_MAX];
    // NOLINTEND(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

    // Version 3 richer Task snapshot. Keep this scalar-only and append-only:
    // the request is captured from the exception path without allocation.
    uint64_t task_ptr;
    uint64_t thread_ptr;
    uint64_t parent_pid;
    uint64_t owner_pid;
    uint64_t wki_remote_pid;
    uint64_t session_id;
    uint64_t pgid;
    uint64_t task_type;
    uint64_t task_state;
    uint64_t sched_queue;
    uint64_t current_cpu;
    uint64_t domain_id;
    uint64_t domain_mask;
    uint64_t wki_target_flags;
    uint64_t wki_proxy_task_id;
    uint64_t elf_buffer_addr;
    uint64_t captured_elf_buffer_size;
    uint64_t elf_buffer_shared;
    uint64_t start_time_us;
    uint64_t user_time_us;
    uint64_t system_time_us;
    uint64_t vruntime;
    uint64_t vdeadline;
    uint64_t wake_at_us;
    uint64_t wait_channel_addr;
    uint64_t waiting_for_pid;
    uint64_t wait_status_user_addr;
    uint64_t wait_rusage_user_addr;
    uint64_t sig_pending;
    uint64_t sig_mask;
    uint64_t ptrace_tracer_pid;
    uint64_t uid;
    uint64_t gid;
    uint64_t euid;
    uint64_t egid;
    uint64_t task_flags;
} __attribute__((packed));

constexpr uint64_t TASK_FLAG_IS_THREAD = 1ULL << 0;
constexpr uint64_t TASK_FLAG_IS_ELF_BUFFER_SHARED = 1ULL << 1;
constexpr uint64_t TASK_FLAG_HAS_RUN = 1ULL << 2;
constexpr uint64_t TASK_FLAG_HAS_EXITED = 1ULL << 3;
constexpr uint64_t TASK_FLAG_WAITED_ON = 1ULL << 4;
constexpr uint64_t TASK_FLAG_DEFERRED_TASK_SWITCH = 1ULL << 5;
constexpr uint64_t TASK_FLAG_YIELD_SWITCH = 1ULL << 6;
constexpr uint64_t TASK_FLAG_VOLUNTARY_BLOCK = 1ULL << 7;
constexpr uint64_t TASK_FLAG_PREEMPT_PENDING = 1ULL << 8;
constexpr uint64_t TASK_FLAG_IN_SIGNAL_HANDLER = 1ULL << 9;
constexpr uint64_t TASK_FLAG_DO_SIGRETURN = 1ULL << 10;
constexpr uint64_t TASK_FLAG_WANTS_BLOCK = 1ULL << 11;
constexpr uint64_t TASK_FLAG_JUST_WOKE = 1ULL << 12;
constexpr uint64_t TASK_FLAG_CPU_PINNED = 1ULL << 13;
constexpr uint64_t TASK_FLAG_DOMAIN_HARD = 1ULL << 14;
constexpr uint64_t TASK_FLAG_WKI_PREFER_INLINE = 1ULL << 15;
constexpr uint64_t TASK_FLAG_WKI_SKIP_LEGACY_PLACEMENT = 1ULL << 16;

enum class SegmentType : uint8_t {
    STACK_PAGE = 1,
    FAULT_PAGE = 2,
    MEMORY_PAGE = 3,
};

struct CoreDumpSegment {
    uint64_t vaddr;
    uint64_t size;
    uint64_t file_offset;
    uint32_t type;
    uint32_t present;
    uint64_t pte_flags;
    uint64_t phys_addr;
} __attribute__((packed));

auto u64_to_dec(char* out, size_t out_size, uint64_t v) -> size_t {
    if (out_size == 0) {
        return 0;
    }
    std::array<char, 32> tmp{};
    size_t n = 0;
    while (true) {
        tmp.at(n++) = static_cast<char>('0' + (v % 10));
        v /= 10;
        if (v == 0 || n >= tmp.size()) {
            break;
        }
    }

    size_t w = 0;
    while (w + 1 < out_size && n > 0) {
        out[w++] = tmp.at(--n);
    }
    out[w] = '\0';
    return w;
}

void copy_cstr(char* out, size_t out_size, const char* src) {
    if (out_size == 0) {
        return;
    }

    size_t i = 0;
    for (; i + 1 < out_size && src[i] != '\0'; ++i) {
        out[i] = src[i];
    }
    out[i] = '\0';
}

auto sanitize_name(const char* in, char* out, size_t out_size) -> void {
    if (out_size == 0) {
        return;
    }
    if (in == nullptr || in[0] == '\0') {
        copy_cstr(out, out_size, "unknown");
        return;
    }

    size_t w = 0;
    for (size_t r = 0; in[r] != '\0' && w + 1 < out_size; ++r) {
        char const C = in[r];
        bool const OK = (C >= 'a' && C <= 'z') || (C >= 'A' && C <= 'Z') || (C >= '0' && C <= '9') || (C == '_');
        out[w++] = OK ? C : '_';
    }
    out[w] = '\0';
}

void append_cstr(char* out, size_t out_size, size_t& pos, const char* src) {
    for (size_t i = 0; src[i] != '\0' && pos + 1 < out_size; ++i) {
        out[pos++] = src[i];
    }
    if (out_size != 0) {
        out[pos] = '\0';
    }
}

auto write_all(int fd, const void* buf, size_t count) -> bool {
    const auto* p = static_cast<const uint8_t*>(buf);
    size_t remaining = count;
    while (remaining > 0) {
        size_t wrote = 0;
        ssize_t const RC = ker::vfs::vfs_write(fd, p, remaining, &wrote);
        if (RC < 0 || wrote == 0) {
            return false;
        }
        p += wrote;
        remaining -= wrote;
    }
    return true;
}

auto pte_raw(const ker::mod::mm::paging::PageTableEntry& e) -> uint64_t {
    uint64_t val = 0;
    std::memcpy(&val, &e, sizeof(val));
    return val;
}

struct UserPageWalkStats {
    uint64_t invalid_pml3_frames = 0;
    uint64_t invalid_pml2_frames = 0;
    uint64_t invalid_pml1_frames = 0;
    uint64_t skipped_data_pages = 0;
};

auto phys_to_hhdm_checked(uint64_t phys_addr) -> void* {
    const uint64_t PAGE_BASE = phys_addr & ~(ker::mod::mm::paging::PAGE_SIZE - 1);
    if (!is_ram(PAGE_BASE)) {
        return nullptr;
    }

    const uint64_t VIRT_RAW = PAGE_BASE + ker::mod::mm::addr::get_hhdm_offset();
    if ((VIRT_RAW >> CANONICAL_SIGN_BIT) != CANONICAL_KERNEL_UPPER) {
        return nullptr;
    }

    if (ker::mod::mm::phys::page_ref_get(reinterpret_cast<void*>(VIRT_RAW)) == 0) {
        return nullptr;
    }

    return reinterpret_cast<void*>(VIRT_RAW);
}

template <typename Fn>
void for_each_user_page(ker::mod::mm::paging::PageTable* pagemap, uint64_t pid, const char* name, UserPageWalkStats* stats, Fn fn) {
    if (pagemap == nullptr) {
        return;
    }

    constexpr uint64_t PAGE = ker::mod::mm::paging::PAGE_SIZE;
    constexpr size_t USER_PML4_ENTRIES = 256;

    for (size_t i4 = 0; i4 < USER_PML4_ENTRIES; ++i4) {
        const auto& pml4e = page_table_entry_at(pagemap, i4);
        if (!pml4e.present || !pml4e.user) {
            continue;
        }

        const uint64_t PML3_PHYS = static_cast<uint64_t>(pml4e.frame) << ker::mod::mm::paging::PAGE_SHIFT;
        auto* pml3 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(phys_to_hhdm_checked(PML3_PHYS));
        if (pml3 == nullptr) {
            if (stats != nullptr && stats->invalid_pml3_frames++ < MAX_WALK_WARNINGS_PER_LEVEL) {
                log::warn("pid=%lu name=%s invalid PML3 frame=0x%llx under PML4[%zu]", pid, name != nullptr ? name : "?",
                          static_cast<unsigned long long>(PML3_PHYS), i4);
            }
            continue;
        }
        for (size_t i3 = 0; i3 < 512; ++i3) {
            const auto& pml3e = page_table_entry_at(pml3, i3);
            if (!pml3e.present || !pml3e.user) {
                continue;
            }

            const uint64_t PML2_PHYS = static_cast<uint64_t>(pml3e.frame) << ker::mod::mm::paging::PAGE_SHIFT;
            auto* pml2 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(phys_to_hhdm_checked(PML2_PHYS));
            if (pml2 == nullptr) {
                if (stats != nullptr && stats->invalid_pml2_frames++ < MAX_WALK_WARNINGS_PER_LEVEL) {
                    log::warn("pid=%lu name=%s invalid PML2 frame=0x%llx under PML4[%zu]/PML3[%zu]", pid, name != nullptr ? name : "?",
                              static_cast<unsigned long long>(PML2_PHYS), i4, i3);
                }
                continue;
            }
            for (size_t i2 = 0; i2 < 512; ++i2) {
                const auto& pml2e = page_table_entry_at(pml2, i2);
                if (!pml2e.present || !pml2e.user) {
                    continue;
                }

                uint64_t const BASE = (i4 << 39) | (i3 << 30) | (i2 << 21);
                if (pml2e.pagesize) {
                    uint64_t const RAW = pte_raw(pml2e);
                    uint64_t const PHYS_BASE = pml2e.frame << ker::mod::mm::paging::PAGE_SHIFT;
                    for (size_t part = 0; part < 512; ++part) {
                        uint64_t const VADDR = BASE + (part * PAGE);
                        uint64_t const PHYS = PHYS_BASE + (part * PAGE);
                        if (phys_to_hhdm_checked(PHYS) != nullptr) {
                            fn(VADDR, PHYS, RAW);
                        } else if (stats != nullptr) {
                            ++stats->skipped_data_pages;
                        }
                    }
                    continue;
                }

                const uint64_t PML1_PHYS = static_cast<uint64_t>(pml2e.frame) << ker::mod::mm::paging::PAGE_SHIFT;
                auto* pml1 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(phys_to_hhdm_checked(PML1_PHYS));
                if (pml1 == nullptr) {
                    if (stats != nullptr && stats->invalid_pml1_frames++ < MAX_WALK_WARNINGS_PER_LEVEL) {
                        log::warn("pid=%lu name=%s invalid PML1 frame=0x%llx under PML4[%zu]/PML3[%zu]/PML2[%zu]", pid,
                                  name != nullptr ? name : "?", static_cast<unsigned long long>(PML1_PHYS), i4, i3, i2);
                    }
                    continue;
                }
                for (size_t i1 = 0; i1 < 512; ++i1) {
                    const auto& pte = page_table_entry_at(pml1, i1);
                    if (!pte.present || !pte.user) {
                        continue;
                    }

                    uint64_t const VADDR = BASE | (i1 << 12);
                    uint64_t const PHYS = pte.frame << ker::mod::mm::paging::PAGE_SHIFT;
                    if (phys_to_hhdm_checked(PHYS) != nullptr) {
                        fn(VADDR, PHYS, pte_raw(pte));
                    } else if (stats != nullptr) {
                        ++stats->skipped_data_pages;
                    }
                }
            }
        }
    }
}

// Snapshot of all data needed to write a coredump, captured at exception time.
// No heap allocation, no locks. Physical page reads happen via HHDM in the coredump task.
struct CoreDumpRequest {
    ker::mod::cpu::GPRegs gpr;
    ker::mod::gates::InterruptFrame frame;
    ker::mod::gates::InterruptFrame saved_frame;
    ker::mod::cpu::GPRegs saved_regs;
    uint64_t cr2;
    uint64_t cr3;
    uint64_t cpu_id;
    uint64_t pid;
    uint64_t entry;
    uint64_t elf_header_addr;
    uint64_t program_header_addr;
    uint64_t interp_base;
    uint64_t program_header_count;
    uint64_t program_header_ent_size;
    uint64_t thread_fs_base;
    uint64_t thread_gs_base;
    uint64_t thread_stack_base;
    uint64_t thread_stack_size;
    uint64_t thread_tls_base;
    uint64_t thread_tls_size;
    uint64_t thread_safe_stack;
    uint64_t task_ptr_value;
    uint64_t thread_ptr_value;
    uint64_t parent_pid;
    uint64_t owner_pid;
    uint64_t wki_remote_pid;
    uint64_t session_id;
    uint64_t pgid;
    uint64_t task_type;
    uint64_t task_state;
    uint64_t sched_queue;
    uint64_t current_cpu;
    uint64_t domain_id;
    uint64_t domain_mask;
    uint64_t wki_target_flags;
    uint64_t wki_proxy_task_id;
    uint64_t elf_buffer_addr;
    uint64_t captured_elf_buffer_size;
    uint64_t elf_buffer_shared_value;
    uint64_t task_pagemap_value;
    uint64_t start_time_us;
    uint64_t user_time_us;
    uint64_t system_time_us;
    uint64_t vruntime;
    uint64_t vdeadline;
    uint64_t wake_at_us;
    uint64_t wait_channel_addr;
    uint64_t waiting_for_pid;
    uint64_t wait_status_user_addr;
    uint64_t wait_rusage_user_addr;
    uint64_t sig_pending;
    uint64_t sig_mask;
    uint64_t ptrace_tracer_pid;
    uint64_t uid;
    uint64_t gid;
    uint64_t euid;
    uint64_t egid;
    uint64_t task_flags;
    uint8_t* elf_buffer;  // stolen from task->elf_buffer; retained after write to avoid allocator re-entry during crash handling
    size_t elf_buffer_size;
    bool elf_buffer_shared;
    ker::mod::mm::paging::PageTable* pagemap;
    uint64_t user_rsp;
    uint64_t timestamp;
    ker::mod::sched::task::Task* task_ptr;  // holds a refcount to block GC; released when done
    std::array<char, 48> name{};
    std::array<char, ker::mod::sched::task::Task::EXE_PATH_MAX> exe_path{};
    std::array<char, ker::mod::sched::task::Task::CWD_MAX> cwd{};
    std::array<char, ker::mod::sched::task::Task::CWD_MAX> root{};
    std::array<char, 64> wait_channel{};
    std::array<char, ker::mod::sched::task::Task::WKI_TARGET_HOSTNAME_MAX> wki_target_hostname{};
    std::array<char, ker::mod::sched::task::Task::WKI_TARGET_HOSTNAME_MAX> wki_submitter_hostname{};
};

struct CoreDumpSlot {
    CoreDumpRequest req{};
    std::atomic<bool> ready{false};
};

std::array<CoreDumpSlot, RING_SIZE> g_ring{};
std::atomic<uint32_t> g_write_seq{0};
uint32_t g_read_seq{0};  // only touched by coredump task
ker::mod::sched::task::Task* g_coredump_task{nullptr};

void perform_coredump(const CoreDumpRequest& req);

[[noreturn]] void coredump_task_fn() {
    while (true) {
        uint32_t const IDX = g_read_seq % RING_SIZE;

        if (g_ring.at(IDX).ready.load(std::memory_order_acquire)) {
            CoreDumpRequest& req = g_ring.at(IDX).req;

            // Switch to kernel pagemap: VFS I/O and HHDM page reads require it.
            // (The coredump task is a DAEMON so it already uses the kernel pagemap,
            // but this makes the invariant explicit and handles any edge cases.)
            ker::mod::mm::virt::switch_to_kernel_pagemap();

            perform_coredump(req);

            // Do not delete a private ELF buffer here. Coredump handling runs on
            // the crash path and private ELF buffers are often medium allocations;
            // freeing those here can re-enter fragile allocator state immediately
            // before GC reclaims the crashed task. Shared cache buffers only need
            // their cache reference released.
            if (req.elf_buffer != nullptr) {
                if (req.elf_buffer_shared) {
                    ker::net::wki::wki_remote_compute_release_elf_buffer(req.elf_buffer);
                }
                req.elf_buffer = nullptr;
            }

            // Release the refcount acquired at exception time.
            // GC was blocked from reclaiming the task (and its pagemap) while rc > 1.
            if (req.task_ptr != nullptr) {
                req.task_ptr->release();
                req.task_ptr = nullptr;
            }

            g_ring.at(IDX).ready.store(false, std::memory_order_release);
            g_read_seq++;
        } else {
            ker::mod::sched::kern_block();
        }
    }
}

void perform_coredump(const CoreDumpRequest& req) {
    std::array<char, 48> prog{};
    sanitize_name(req.name.data(), prog.data(), prog.size());

    std::array<char, 32> ts{};
    u64_to_dec(ts.data(), ts.size(), req.timestamp);

    std::array<char, 160> path{};
    constexpr const char* SUFFIX = "_coredump.bin";
    size_t pos = 0;
    // Keep crash-path dumps off the persistent rootfs for now. Writing large
    // coredumps through XFS/buffer-cache is currently triggering allocator
    // corruption, which obscures the original userspace mapping bug.
    constexpr const char* PREFIX = "/tmp/";
    append_cstr(path.data(), path.size(), pos, PREFIX);
    append_cstr(path.data(), path.size(), pos, prog.data());
    if (pos + 1 < path.size()) {
        path.at(pos++) = '_';
    }
    append_cstr(path.data(), path.size(), pos, ts.data());
    append_cstr(path.data(), path.size(), pos, SUFFIX);
    path.at(pos) = '\0';

    int const FD = ker::vfs::vfs_open(path.data(), O_CREAT, 0);
    if (FD < 0) {
        log::error("failed to open %s", path.data());
        return;
    }

    constexpr uint64_t PAGE = ker::mod::mm::paging::PAGE_SIZE;
    uint64_t stack_page = req.user_rsp & ~(PAGE - 1);
    uint64_t fault_page = req.cr2 & ~(PAGE - 1);

    UserPageWalkStats walk_stats{};
    uint64_t seg_capacity = 0;
    for_each_user_page(req.pagemap, req.pid, req.name.data(), &walk_stats,
                       [&](uint64_t /*vaddr*/, uint64_t /*phys*/, uint64_t /*pte*/) -> void { ++seg_capacity; });

    CoreDumpSegment* segs = nullptr;
    uint64_t const SEG_TABLE_BYTES = seg_capacity * sizeof(CoreDumpSegment);
    uint64_t const SEG_TABLE_ALLOC_BYTES = (SEG_TABLE_BYTES + PAGE - 1) & ~(PAGE - 1);
    if (seg_capacity != 0) {
        segs = static_cast<CoreDumpSegment*>(ker::mod::mm::phys::page_alloc(SEG_TABLE_ALLOC_BYTES, "coredump-segments"));
        if (segs == nullptr) {
            ker::vfs::vfs_close(FD);
            log::error("failed to allocate segment table for %s", path.data());
            return;
        }
        std::fill_n(segs, seg_capacity, CoreDumpSegment{});
    }

    uint64_t next_offset = sizeof(CoreDumpHeader) + (seg_capacity * sizeof(CoreDumpSegment));
    uint64_t seg_index = 0;
    uint64_t dropped_segments = 0;
    if (segs != nullptr) {
        for_each_user_page(req.pagemap, req.pid, req.name.data(), &walk_stats, [&](uint64_t vaddr, uint64_t phys, uint64_t pte_flags) {
            if (seg_index >= seg_capacity) {
                ++dropped_segments;
                return;
            }

            CoreDumpSegment s{};
            s.vaddr = vaddr;
            s.size = PAGE;
            s.file_offset = next_offset;
            s.present = 1;
            s.pte_flags = pte_flags;
            s.phys_addr = phys;
            s.type = static_cast<uint32_t>(SegmentType::MEMORY_PAGE);
            const bool IS_THREAD_STACK =
                req.thread_stack_base != 0 && vaddr >= req.thread_stack_base && vaddr < req.thread_stack_base + req.thread_stack_size;
            const bool CONTAINS_CURRENT_STACK = vaddr <= stack_page && stack_page < vaddr + PAGE;
            if (vaddr == fault_page) {
                s.type = static_cast<uint32_t>(SegmentType::FAULT_PAGE);
            } else if (IS_THREAD_STACK || CONTAINS_CURRENT_STACK) {
                s.type = static_cast<uint32_t>(SegmentType::STACK_PAGE);
            }
            segs[seg_index++] = s;
            next_offset += PAGE;
        });
    }

    CoreDumpHeader hdr{};
    hdr.magic = COREDUMP_MAGIC;
    hdr.version = COREDUMP_VERSION;
    hdr.header_size = sizeof(CoreDumpHeader);
    hdr.timestamp_quantums = req.timestamp;
    hdr.pid = req.pid;
    hdr.cpu = req.cpu_id;
    hdr.int_num = req.frame.int_num;
    hdr.err_code = req.frame.err_code;
    hdr.cr2 = req.cr2;
    hdr.cr3 = req.cr3;
    hdr.trap_frame = req.frame;
    hdr.trap_regs = req.gpr;
    hdr.saved_frame = req.saved_frame;
    hdr.saved_regs = req.saved_regs;
    hdr.task_entry = req.entry;
    hdr.task_pagemap = req.task_pagemap_value;
    hdr.elf_header_addr = req.elf_header_addr;
    hdr.program_header_addr = req.program_header_addr;
    hdr.segment_count = seg_capacity;
    hdr.segment_table_offset = sizeof(CoreDumpHeader);
    hdr.elf_size = req.elf_buffer != nullptr ? req.elf_buffer_size : 0;
    hdr.elf_offset = next_offset;
    hdr.segment_entry_size = sizeof(CoreDumpSegment);
    hdr.page_size = PAGE;
    hdr.snapshot_flags = 1;  // Full present-user-page snapshot.
    hdr.interp_base = req.interp_base;
    hdr.program_header_count = req.program_header_count;
    hdr.program_header_ent_size = req.program_header_ent_size;
    hdr.thread_fs_base = req.thread_fs_base;
    hdr.thread_gs_base = req.thread_gs_base;
    hdr.thread_stack_base = req.thread_stack_base;
    hdr.threadstack_size = req.thread_stack_size;
    hdr.thread_tls_base = req.thread_tls_base;
    hdr.threadtls_size = req.thread_tls_size;
    hdr.thread_safe_stack = req.thread_safe_stack;
    std::copy_n(req.exe_path.begin(), req.exe_path.size(), std::begin(hdr.exe_path));
    std::copy_n(req.cwd.begin(), req.cwd.size(), std::begin(hdr.cwd));
    std::copy_n(req.root.begin(), req.root.size(), std::begin(hdr.root));
    std::copy_n(req.wait_channel.begin(), req.wait_channel.size(), std::begin(hdr.wait_channel));
    std::copy_n(req.wki_target_hostname.begin(), req.wki_target_hostname.size(), std::begin(hdr.wki_target_hostname));
    std::copy_n(req.wki_submitter_hostname.begin(), req.wki_submitter_hostname.size(), std::begin(hdr.wki_submitter_hostname));
    hdr.task_ptr = req.task_ptr_value;
    hdr.thread_ptr = req.thread_ptr_value;
    hdr.parent_pid = req.parent_pid;
    hdr.owner_pid = req.owner_pid;
    hdr.wki_remote_pid = req.wki_remote_pid;
    hdr.session_id = req.session_id;
    hdr.pgid = req.pgid;
    hdr.task_type = req.task_type;
    hdr.task_state = req.task_state;
    hdr.sched_queue = req.sched_queue;
    hdr.current_cpu = req.current_cpu;
    hdr.domain_id = req.domain_id;
    hdr.domain_mask = req.domain_mask;
    hdr.wki_target_flags = req.wki_target_flags;
    hdr.wki_proxy_task_id = req.wki_proxy_task_id;
    hdr.elf_buffer_addr = req.elf_buffer_addr;
    hdr.captured_elf_buffer_size = req.captured_elf_buffer_size;
    hdr.elf_buffer_shared = req.elf_buffer_shared_value;
    hdr.start_time_us = req.start_time_us;
    hdr.user_time_us = req.user_time_us;
    hdr.system_time_us = req.system_time_us;
    hdr.vruntime = req.vruntime;
    hdr.vdeadline = req.vdeadline;
    hdr.wake_at_us = req.wake_at_us;
    hdr.wait_channel_addr = req.wait_channel_addr;
    hdr.waiting_for_pid = req.waiting_for_pid;
    hdr.wait_status_user_addr = req.wait_status_user_addr;
    hdr.wait_rusage_user_addr = req.wait_rusage_user_addr;
    hdr.sig_pending = req.sig_pending;
    hdr.sig_mask = req.sig_mask;
    hdr.ptrace_tracer_pid = req.ptrace_tracer_pid;
    hdr.uid = req.uid;
    hdr.gid = req.gid;
    hdr.euid = req.euid;
    hdr.egid = req.egid;
    hdr.task_flags = req.task_flags;

    bool ok = write_all(FD, &hdr, sizeof(hdr));
    ok = ok && (seg_capacity == 0 || write_all(FD, segs, seg_capacity * sizeof(CoreDumpSegment)));

    for (uint64_t i = 0; ok && i < seg_capacity; ++i) {
        if (segs[i].present == 0) {
            continue;
        }
        void const* page_ptr = phys_to_hhdm_checked(segs[i].phys_addr);
        if (page_ptr == nullptr) {
            ok = false;
            continue;
        }
        ok = write_all(FD, page_ptr, PAGE);
    }

    if (ok && req.elf_buffer != nullptr && req.elf_buffer_size != 0) {
        const uint8_t* elf = req.elf_buffer;
        size_t remaining = req.elf_buffer_size;
        while (ok && remaining > 0) {
            size_t const CHUNK = remaining > 4096 ? 4096 : remaining;
            ok = write_all(FD, elf, CHUNK);
            elf += CHUNK;
            remaining -= CHUNK;
        }
    }

    ker::vfs::vfs_close(FD);
    // Intentionally retain the emergency segment table. It is allocated only
    // when a process is already crashing, and freeing multi-page buffers from
    // this path has caused allocator metadata failures before GC finishes.

    if (ok) {
        log::info("wrote %s", path.data());
        if (dropped_segments != 0) {
            log::warn("skipped %lu pages because pagemap grew while dumping %s", static_cast<unsigned long>(dropped_segments), path.data());
        }
        if (walk_stats.invalid_pml3_frames != 0 || walk_stats.invalid_pml2_frames != 0 || walk_stats.invalid_pml1_frames != 0 ||
            walk_stats.skipped_data_pages != 0) {
            log::warn("pid=%lu name=%s pagemap=%p dump completed with invalidPml3=%lu invalidPml2=%lu invalidPml1=%lu skippedPages=%lu",
                      req.pid, req.name.data(), static_cast<void*>(req.pagemap), walk_stats.invalid_pml3_frames,
                      walk_stats.invalid_pml2_frames, walk_stats.invalid_pml1_frames, walk_stats.skipped_data_pages);
        }
    } else {
        log::error("failed while writing %s", path.data());
        if (walk_stats.invalid_pml3_frames != 0 || walk_stats.invalid_pml2_frames != 0 || walk_stats.invalid_pml1_frames != 0 ||
            walk_stats.skipped_data_pages != 0) {
            log::warn("pid=%lu name=%s pagemap=%p dump failed with invalidPml3=%lu invalidPml2=%lu invalidPml1=%lu skippedPages=%lu",
                      req.pid, req.name.data(), static_cast<void*>(req.pagemap), walk_stats.invalid_pml3_frames,
                      walk_stats.invalid_pml2_frames, walk_stats.invalid_pml1_frames, walk_stats.skipped_data_pages);
        }
    }
}

}  // namespace

void init() {
    g_coredump_task = ker::mod::sched::task::Task::create_kernel_thread("coredump", coredump_task_fn);
    if (g_coredump_task == nullptr) {
        log::error("failed to create coredump task");
        return;
    }
    ker::mod::sched::post_task_balanced(g_coredump_task);
    log::info("task started");
}

void try_write_for_task(ker::mod::sched::task::Task* task, const ker::mod::cpu::GPRegs& gpr, const ker::mod::gates::InterruptFrame& frame,
                        uint64_t cr2, uint64_t cr3, uint64_t cpu_id) {
    if (task == nullptr || g_coredump_task == nullptr || task->dumpable == 0) {
        return;
    }

    // Claim a ring slot. Wrap around; drop if the consumer hasn't cleared this slot yet.
    uint32_t const SEQ = g_write_seq.fetch_add(1, std::memory_order_relaxed);
    uint32_t const IDX = SEQ % RING_SIZE;
    CoreDumpSlot& slot = g_ring.at(IDX);

    if (slot.ready.load(std::memory_order_acquire)) {
        log::warn("ring full, dropping coredump for PID %lx", static_cast<unsigned long>(task->pid));
        return;
    }

    // Take a refcount on the task. This prevents GC from reclaiming the Task
    // metadata while the coredump task writes the request and any stolen ELF
    // buffer. It does not pin the pagemap; async memory snapshots stay disabled
    // until page-table ownership is explicit. try_acquire fails if task is
    // already EXITING/DEAD.
    if (!task->try_acquire()) {
        return;
    }

    CoreDumpRequest& req = slot.req;
    req.gpr = gpr;
    req.frame = frame;
    req.saved_frame = task->context.frame;
    req.saved_regs = task->context.regs;
    req.cr2 = cr2;
    req.cr3 = cr3;
    req.cpu_id = cpu_id;
    req.pid = task->pid;
    req.entry = task->entry;
    req.elf_header_addr = task->elf_header_addr;
    req.program_header_addr = task->program_header_addr;
    req.interp_base = task->interp_base;
    req.program_header_count = task->program_header_count;
    req.program_header_ent_size = task->program_header_ent_size;
    req.thread_fs_base = 0;
    req.thread_gs_base = 0;
    req.thread_stack_base = 0;
    req.thread_stack_size = 0;
    req.thread_tls_base = 0;
    req.thread_tls_size = 0;
    req.thread_safe_stack = 0;
    if (task->thread != nullptr) {
        req.thread_fs_base = task->thread->fsbase;
        req.thread_gs_base = task->thread->gsbase;
        req.thread_stack_base = task->thread->stack_base_virt;
        req.thread_stack_size = task->thread->stack_size;
        req.thread_tls_base = task->thread->tls_base_virt;
        req.thread_tls_size = task->thread->tls_size;
        req.thread_safe_stack = task->thread->safestack_ptr_value;
    }
    req.task_ptr_value = reinterpret_cast<uint64_t>(task);
    req.thread_ptr_value = reinterpret_cast<uint64_t>(task->thread);
    req.parent_pid = task->parent_pid;
    req.owner_pid = task->owner_pid;
    req.wki_remote_pid = task->wki_remote_pid;
    req.session_id = task->session_id;
    req.pgid = task->pgid;
    req.task_type = static_cast<uint64_t>(task->type);
    req.task_state = static_cast<uint64_t>(task->state.load(std::memory_order_acquire));
    req.sched_queue = static_cast<uint64_t>(task->sched_queue);
    req.current_cpu = task->cpu;
    req.domain_id = task->domain_id;
    req.domain_mask = task->domain_mask;
    req.wki_target_flags = task->wki_target_flags;
    req.wki_proxy_task_id = task->wki_proxy_task_id;
    req.elf_buffer_addr = reinterpret_cast<uint64_t>(task->elf_buffer);
    req.captured_elf_buffer_size = task->elf_buffer_size;
    req.elf_buffer_shared_value = task->is_elf_buffer_shared ? 1 : 0;
    req.task_pagemap_value = reinterpret_cast<uint64_t>(task->pagemap);
    req.start_time_us = task->start_time_us;
    req.user_time_us = task->user_time_us;
    req.system_time_us = task->system_time_us;
    req.vruntime = static_cast<uint64_t>(task->vruntime);
    req.vdeadline = static_cast<uint64_t>(task->vdeadline);
    req.wake_at_us = task->wake_at_us;
    req.wait_channel_addr = reinterpret_cast<uint64_t>(task->wait_channel);
    req.waiting_for_pid = task->waiting_for_pid;
    req.wait_status_user_addr = task->wait_status_user_addr;
    req.wait_rusage_user_addr = task->wait_rusage_user_addr;
    req.sig_pending = task->sig_pending;
    req.sig_mask = task->sig_mask;
    req.ptrace_tracer_pid = task->ptrace_tracer_pid;
    req.uid = task->uid;
    req.gid = task->gid;
    req.euid = task->euid;
    req.egid = task->egid;
    req.task_flags = 0;
    req.task_flags |= task->is_thread ? TASK_FLAG_IS_THREAD : 0;
    req.task_flags |= task->is_elf_buffer_shared ? TASK_FLAG_IS_ELF_BUFFER_SHARED : 0;
    req.task_flags |= task->has_run ? TASK_FLAG_HAS_RUN : 0;
    req.task_flags |= task->has_exited ? TASK_FLAG_HAS_EXITED : 0;
    req.task_flags |= ker::mod::sched::task::task_waited_on(*task) ? TASK_FLAG_WAITED_ON : 0;
    req.task_flags |= task->deferred_task_switch ? TASK_FLAG_DEFERRED_TASK_SWITCH : 0;
    req.task_flags |= task->yield_switch ? TASK_FLAG_YIELD_SWITCH : 0;
    req.task_flags |= task->is_voluntary_blocked() ? TASK_FLAG_VOLUNTARY_BLOCK : 0;
    req.task_flags |= task->preempt_pending ? TASK_FLAG_PREEMPT_PENDING : 0;
    req.task_flags |= task->in_signal_handler ? TASK_FLAG_IN_SIGNAL_HANDLER : 0;
    req.task_flags |= task->do_sigreturn ? TASK_FLAG_DO_SIGRETURN : 0;
    req.task_flags |= task->wants_block ? TASK_FLAG_WANTS_BLOCK : 0;
    req.task_flags |= task->just_woke ? TASK_FLAG_JUST_WOKE : 0;
    req.task_flags |= task->cpu_pinned ? TASK_FLAG_CPU_PINNED : 0;
    req.task_flags |= task->domain_hard ? TASK_FLAG_DOMAIN_HARD : 0;
    req.task_flags |= task->wki_prefer_inline ? TASK_FLAG_WKI_PREFER_INLINE : 0;
    req.task_flags |= task->wki_skip_legacy_placement ? TASK_FLAG_WKI_SKIP_LEGACY_PLACEMENT : 0;
    // A Task ref keeps the Task object alive, but process exit is still allowed
    // to destroy and release task->pagemap immediately. Until coredump owns or
    // pins page-table pages separately, keep async dumps to metadata + ELF bytes.
    req.pagemap = CAPTURE_USER_MEMORY_SNAPSHOT ? task->pagemap : nullptr;
    req.user_rsp = frame.rsp;
    req.timestamp = ker::mod::time::get_ticks();
    req.task_ptr = task;
    sanitize_name(task->name, req.name.data(), req.name.size());
    std::copy_n(task->exe_path.begin(), req.exe_path.size(), req.exe_path.begin());
    std::copy_n(task->cwd.begin(), req.cwd.size(), req.cwd.begin());
    std::copy_n(task->root.begin(), req.root.size(), req.root.begin());
    copy_cstr(req.wait_channel.data(), req.wait_channel.size(), task->wait_channel != nullptr ? task->wait_channel : "");
    std::copy_n(task->wki_target_hostname.begin(), req.wki_target_hostname.size(), req.wki_target_hostname.begin());
    std::copy_n(task->wki_submitter_hostname.begin(), req.wki_submitter_hostname.size(), req.wki_submitter_hostname.begin());

    // Steal elf_buffer so exit() won't free it before the coredump task writes it.
    // Private buffers are intentionally retained after writing; see coredump_task_fn().
    req.elf_buffer = task->elf_buffer;
    req.elf_buffer_size = task->elf_buffer_size;
    req.elf_buffer_shared = task->is_elf_buffer_shared;
    task->elf_buffer = nullptr;
    task->elf_buffer_size = 0;
    task->is_elf_buffer_shared = false;

    // Publish the slot and wake the coredump task. Safe from CPL=3 exception context:
    // userspace-fault handlers cannot be holding any kernel spinlock.
    slot.ready.store(true, std::memory_order_release);
    ker::mod::sched::kern_wake(g_coredump_task);
}

}  // namespace ker::mod::dbg::coredump
