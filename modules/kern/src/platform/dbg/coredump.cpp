#include "coredump.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
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

constexpr uint64_t COREDUMP_MAGIC = 0x504d55444f43534full;  // "WOSCODMP" little-endian-ish identifier
constexpr uint32_t COREDUMP_VERSION = 2;
constexpr size_t MAX_WALK_WARNINGS_PER_LEVEL = 8;

// x86-64 canonical address check: bits[63:47] must all be the same.
// For kernel HHDM addresses (bit 47 set) the upper 17 bits are all 1s.
static constexpr int CANONICAL_SIGN_BIT = 47;
static constexpr uint64_t CANONICAL_KERNEL_UPPER = 0x1ffffULL;

// Keep this in sync with tmpfs' internal flag.
constexpr int O_CREAT = 0100;  // octal = 64 decimal = 0x40 hex

bool is_ram(uint64_t phys) {
    for (auto* zone = ker::mod::mm::phys::getZones(); zone != nullptr; zone = zone->next) {
        // zone->start is virtual (HHDM)
        // zone->len is length in bytes
        uint64_t zoneStartPhys = (uint64_t)ker::mod::mm::addr::get_phys_pointer((ker::mod::mm::addr::vaddr_t)zone->start);
        if (phys >= zoneStartPhys && phys < zoneStartPhys + zone->len) {
            return true;
        }
    }
    return false;
}

struct CoreDumpHeader {
    uint64_t magic;
    uint32_t version;
    uint32_t headerSize;

    uint64_t timestampQuantums;
    uint64_t pid;
    uint64_t cpu;

    uint64_t intNum;
    uint64_t errCode;
    uint64_t cr2;
    uint64_t cr3;

    // Trap/exception frame at the time of fault.
    ker::mod::gates::interruptFrame trapFrame;
    ker::mod::cpu::GPRegs trapRegs;

    // Saved user context frame/regs (if available).
    ker::mod::gates::interruptFrame savedFrame;
    ker::mod::cpu::GPRegs savedRegs;

    uint64_t taskEntry;
    uint64_t taskPagemap;

    uint64_t elfHeaderAddr;
    uint64_t programHeaderAddr;

    uint64_t segmentCount;
    uint64_t segmentTableOffset;

    uint64_t elfSize;
    uint64_t elfOffset;

    // Version 2 restart/snapshot metadata. The dump now contains all present
    // user pages, not just the faulting stack window.
    uint64_t segmentEntrySize;
    uint64_t pageSize;
    uint64_t snapshotFlags;

    uint64_t interpBase;
    uint64_t programHeaderCount;
    uint64_t programHeaderEntSize;

    uint64_t threadFsBase;
    uint64_t threadGsBase;
    uint64_t threadStackBase;
    uint64_t threadStackSize;
    uint64_t threadTlsBase;
    uint64_t threadTlsSize;
    uint64_t threadSafeStack;

    char exePath[ker::mod::sched::task::Task::EXE_PATH_MAX];
    char cwd[ker::mod::sched::task::Task::CWD_MAX];
    char root[ker::mod::sched::task::Task::CWD_MAX];
} __attribute__((packed));

enum class SegmentType : uint32_t {
    StackPage = 1,
    FaultPage = 2,
    MemoryPage = 3,
};

struct CoreDumpSegment {
    uint64_t vaddr;
    uint64_t size;
    uint64_t fileOffset;
    uint32_t type;
    uint32_t present;
    uint64_t pteFlags;
    uint64_t physAddr;
} __attribute__((packed));

auto u64_to_dec(char* out, size_t outSize, uint64_t v) -> size_t {
    if (outSize == 0) {
        return 0;
    }
    char tmp[32];
    size_t n = 0;
    do {
        tmp[n++] = static_cast<char>('0' + (v % 10));
        v /= 10;
    } while (v != 0 && n < sizeof(tmp));

    size_t w = 0;
    while (w + 1 < outSize && n > 0) {
        out[w++] = tmp[--n];
    }
    out[w] = '\0';
    return w;
}

auto sanitize_name(const char* in, char* out, size_t outSize) -> void {
    if (outSize == 0) {
        return;
    }
    if (in == nullptr || in[0] == '\0') {
        const char fallback[] = "unknown";
        size_t i = 0;
        for (; i + 1 < outSize && fallback[i] != '\0'; ++i) {
            out[i] = fallback[i];
        }
        out[i] = '\0';
        return;
    }

    size_t w = 0;
    for (size_t r = 0; in[r] != '\0' && w + 1 < outSize; ++r) {
        char c = in[r];
        bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (c == '_');
        out[w++] = ok ? c : '_';
    }
    out[w] = '\0';
}

auto write_all(int fd, const void* buf, size_t count) -> bool {
    const uint8_t* p = static_cast<const uint8_t*>(buf);
    size_t remaining = count;
    while (remaining > 0) {
        size_t wrote = 0;
        ssize_t rc = ker::vfs::vfs_write(fd, p, remaining, &wrote);
        if (rc < 0 || wrote == 0) {
            return false;
        }
        p += wrote;
        remaining -= wrote;
    }
    return true;
}

auto pte_raw(const ker::mod::mm::paging::PageTableEntry& e) -> uint64_t {
    uint64_t val = 0;
    __builtin_memcpy(&val, &e, sizeof(val));
    return val;
}

struct UserPageWalkStats {
    uint64_t invalidPml3Frames = 0;
    uint64_t invalidPml2Frames = 0;
    uint64_t invalidPml1Frames = 0;
    uint64_t skippedDataPages = 0;
};

auto phys_to_hhdm_checked(uint64_t phys_addr) -> void* {
    const uint64_t page_base = phys_addr & ~(ker::mod::mm::paging::PAGE_SIZE - 1);
    if (!is_ram(page_base)) {
        return nullptr;
    }

    const uint64_t virt_raw = page_base + ker::mod::mm::addr::get_hhdm_offset();
    if ((virt_raw >> CANONICAL_SIGN_BIT) != CANONICAL_KERNEL_UPPER) {
        return nullptr;
    }

    if (ker::mod::mm::phys::pageRefGet(reinterpret_cast<void*>(virt_raw)) == 0) {
        return nullptr;
    }

    return reinterpret_cast<void*>(virt_raw);
}

template <typename Fn>
void for_each_user_page(ker::mod::mm::paging::PageTable* pagemap, uint64_t pid, const char* name, UserPageWalkStats* stats, Fn&& fn) {
    if (pagemap == nullptr) {
        return;
    }

    constexpr uint64_t PAGE = ker::mod::mm::paging::PAGE_SIZE;
    constexpr size_t USER_PML4_ENTRIES = 256;

    for (size_t i4 = 0; i4 < USER_PML4_ENTRIES; ++i4) {
        const auto& pml4e = pagemap->entries[i4];
        if (!pml4e.present || !pml4e.user) {
            continue;
        }

        const uint64_t pml3_phys = static_cast<uint64_t>(pml4e.frame) << ker::mod::mm::paging::PAGE_SHIFT;
        auto* pml3 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(phys_to_hhdm_checked(pml3_phys));
        if (pml3 == nullptr) {
            if (stats != nullptr && stats->invalidPml3Frames++ < MAX_WALK_WARNINGS_PER_LEVEL) {
                log::warn("coredump: pid=%lu name=%s invalid PML3 frame=0x%llx under PML4[%zu]", pid, name != nullptr ? name : "?",
                          (unsigned long long)pml3_phys, i4);
            }
            continue;
        }
        for (size_t i3 = 0; i3 < 512; ++i3) {
            const auto& pml3e = pml3->entries[i3];
            if (!pml3e.present || !pml3e.user) {
                continue;
            }

            const uint64_t pml2_phys = static_cast<uint64_t>(pml3e.frame) << ker::mod::mm::paging::PAGE_SHIFT;
            auto* pml2 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(phys_to_hhdm_checked(pml2_phys));
            if (pml2 == nullptr) {
                if (stats != nullptr && stats->invalidPml2Frames++ < MAX_WALK_WARNINGS_PER_LEVEL) {
                    log::warn("coredump: pid=%lu name=%s invalid PML2 frame=0x%llx under PML4[%zu]/PML3[%zu]", pid,
                              name != nullptr ? name : "?", (unsigned long long)pml2_phys, i4, i3);
                }
                continue;
            }
            for (size_t i2 = 0; i2 < 512; ++i2) {
                const auto& pml2e = pml2->entries[i2];
                if (!pml2e.present || !pml2e.user) {
                    continue;
                }

                uint64_t base = (i4 << 39) | (i3 << 30) | (i2 << 21);
                if (pml2e.pagesize) {
                    uint64_t raw = pte_raw(pml2e);
                    uint64_t physBase = pml2e.frame << ker::mod::mm::paging::PAGE_SHIFT;
                    for (size_t part = 0; part < 512; ++part) {
                        uint64_t vaddr = base + part * PAGE;
                        uint64_t phys = physBase + part * PAGE;
                        if (phys_to_hhdm_checked(phys) != nullptr) {
                            fn(vaddr, phys, raw);
                        } else if (stats != nullptr) {
                            ++stats->skippedDataPages;
                        }
                    }
                    continue;
                }

                const uint64_t pml1_phys = static_cast<uint64_t>(pml2e.frame) << ker::mod::mm::paging::PAGE_SHIFT;
                auto* pml1 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(phys_to_hhdm_checked(pml1_phys));
                if (pml1 == nullptr) {
                    if (stats != nullptr && stats->invalidPml1Frames++ < MAX_WALK_WARNINGS_PER_LEVEL) {
                        log::warn("coredump: pid=%lu name=%s invalid PML1 frame=0x%llx under PML4[%zu]/PML3[%zu]/PML2[%zu]", pid,
                                  name != nullptr ? name : "?", (unsigned long long)pml1_phys, i4, i3, i2);
                    }
                    continue;
                }
                for (size_t i1 = 0; i1 < 512; ++i1) {
                    const auto& pte = pml1->entries[i1];
                    if (!pte.present || !pte.user) {
                        continue;
                    }

                    uint64_t vaddr = base | (i1 << 12);
                    uint64_t phys = pte.frame << ker::mod::mm::paging::PAGE_SHIFT;
                    if (phys_to_hhdm_checked(phys) != nullptr) {
                        fn(vaddr, phys, pte_raw(pte));
                    } else if (stats != nullptr) {
                        ++stats->skippedDataPages;
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
    ker::mod::gates::interruptFrame frame;
    ker::mod::gates::interruptFrame savedFrame;
    ker::mod::cpu::GPRegs savedRegs;
    uint64_t cr2;
    uint64_t cr3;
    uint64_t cpuId;
    uint64_t pid;
    uint64_t entry;
    uint64_t elfHeaderAddr;
    uint64_t programHeaderAddr;
    uint64_t interpBase;
    uint64_t programHeaderCount;
    uint64_t programHeaderEntSize;
    uint64_t threadFsBase;
    uint64_t threadGsBase;
    uint64_t threadStackBase;
    uint64_t threadStackSize;
    uint64_t threadTlsBase;
    uint64_t threadTlsSize;
    uint64_t threadSafeStack;
    uint8_t* elfBuffer;      // stolen from task->elfBuffer; retained after write to avoid allocator re-entry during crash handling
    size_t elfBufferSize;
    bool elfBufferShared;
    ker::mod::mm::paging::PageTable* pagemap;
    uint64_t userRsp;
    uint64_t timestamp;
    ker::mod::sched::task::Task* taskPtr;  // holds a refcount to block GC; released when done
    char name[48];
    char exePath[ker::mod::sched::task::Task::EXE_PATH_MAX];
    char cwd[ker::mod::sched::task::Task::CWD_MAX];
    char root[ker::mod::sched::task::Task::CWD_MAX];
};

constexpr uint32_t RING_SIZE = 4;

struct CoreDumpSlot {
    CoreDumpRequest req;
    std::atomic<bool> ready{false};
};

static CoreDumpSlot g_ring[RING_SIZE];                                  // NOLINT
static std::atomic<uint32_t> g_write_seq{0};                           // NOLINT
static uint32_t g_read_seq{0};                                         // NOLINT only touched by coredump task
static ker::mod::sched::task::Task* g_coredump_task{nullptr};          // NOLINT

static void perform_coredump(const CoreDumpRequest& req);

[[noreturn]] static void coredump_task_fn() {
    while (true) {
        uint32_t idx = g_read_seq % RING_SIZE;

        if (g_ring[idx].ready.load(std::memory_order_acquire)) {
            CoreDumpRequest& req = g_ring[idx].req;

            // Switch to kernel pagemap: VFS I/O and HHDM page reads require it.
            // (The coredump task is a DAEMON so it already uses the kernel pagemap,
            // but this makes the invariant explicit and handles any edge cases.)
            ker::mod::mm::virt::switchToKernelPagemap();

            perform_coredump(req);

            // Do not delete a private ELF buffer here. Coredump handling runs on
            // the crash path and private ELF buffers are often medium allocations;
            // freeing those here can re-enter fragile allocator state immediately
            // before GC reclaims the crashed task. Shared cache buffers only need
            // their cache reference released.
            if (req.elfBuffer != nullptr) {
                if (req.elfBufferShared) {
                    ker::net::wki::wki_remote_compute_release_elf_buffer(req.elfBuffer);
                }
                req.elfBuffer = nullptr;
            }

            // Release the refcount acquired at exception time.
            // GC was blocked from reclaiming the task (and its pagemap) while rc > 1.
            if (req.taskPtr != nullptr) {
                req.taskPtr->release();
                req.taskPtr = nullptr;
            }

            g_ring[idx].ready.store(false, std::memory_order_release);
            g_read_seq++;
        } else {
            ker::mod::sched::kern_block();
        }
    }
}

static void perform_coredump(const CoreDumpRequest& req) {
    char prog[48];
    sanitize_name(req.name, prog, sizeof(prog));

    char ts[32];
    u64_to_dec(ts, sizeof(ts), req.timestamp);

    char path[160];
    const char suffix[] = "_coredump.bin";
    size_t pos = 0;
    // Keep crash-path dumps off the persistent rootfs for now. Writing large
    // coredumps through XFS/buffer-cache is currently triggering allocator
    // corruption, which obscures the original userspace mapping bug.
    const char prefix[] = "/tmp/";
    for (size_t i = 0; prefix[i] != '\0' && pos + 1 < sizeof(path); ++i) {
        path[pos++] = prefix[i];
    }
    for (size_t i = 0; prog[i] != '\0' && pos + 1 < sizeof(path); ++i) {
        path[pos++] = prog[i];
    }
    if (pos + 1 < sizeof(path)) {
        path[pos++] = '_';
    }
    for (size_t i = 0; ts[i] != '\0' && pos + 1 < sizeof(path); ++i) {
        path[pos++] = ts[i];
    }
    for (size_t i = 0; suffix[i] != '\0' && pos + 1 < sizeof(path); ++i) {
        path[pos++] = suffix[i];
    }
    path[pos] = '\0';

    int fd = ker::vfs::vfs_open(path, O_CREAT, 0);
    if (fd < 0) {
        ker::mod::dbg::log("coredump: failed to open %s", path);
        return;
    }

    constexpr uint64_t PAGE = ker::mod::mm::paging::PAGE_SIZE;
    uint64_t stackPage = req.userRsp & ~(PAGE - 1);
    uint64_t faultPage = req.cr2 & ~(PAGE - 1);

    UserPageWalkStats walkStats{};
    uint64_t segCapacity = 0;
    for_each_user_page(req.pagemap, req.pid, req.name, &walkStats,
                       [&](uint64_t /*vaddr*/, uint64_t /*phys*/, uint64_t /*pte*/) { ++segCapacity; });

    CoreDumpSegment* segs = nullptr;
    uint64_t segTableBytes = segCapacity * sizeof(CoreDumpSegment);
    uint64_t segTableAllocBytes = (segTableBytes + PAGE - 1) & ~(PAGE - 1);
    if (segCapacity != 0) {
        segs = static_cast<CoreDumpSegment*>(ker::mod::mm::phys::pageAlloc(segTableAllocBytes, "coredump-segments"));
        if (segs == nullptr) {
            ker::vfs::vfs_close(fd);
            ker::mod::dbg::log("coredump: failed to allocate segment table for %s", path);
            return;
        }
        std::memset(segs, 0, segTableBytes);
    }

    uint64_t nextOffset = sizeof(CoreDumpHeader) + segCapacity * sizeof(CoreDumpSegment);
    uint64_t segIndex = 0;
    uint64_t droppedSegments = 0;
    if (segs != nullptr) {
        for_each_user_page(req.pagemap, req.pid, req.name, &walkStats, [&](uint64_t vaddr, uint64_t phys, uint64_t pteFlags) {
            if (segIndex >= segCapacity) {
                ++droppedSegments;
                return;
            }

            CoreDumpSegment s{};
            s.vaddr = vaddr;
            s.size = PAGE;
            s.fileOffset = nextOffset;
            s.present = 1;
            s.pteFlags = pteFlags;
            s.physAddr = phys;
            s.type = static_cast<uint32_t>(SegmentType::MemoryPage);
            if (vaddr == faultPage) {
                s.type = static_cast<uint32_t>(SegmentType::FaultPage);
            } else if (req.threadStackBase != 0 && vaddr >= req.threadStackBase && vaddr < req.threadStackBase + req.threadStackSize) {
                s.type = static_cast<uint32_t>(SegmentType::StackPage);
            } else if (vaddr <= stackPage && stackPage < vaddr + PAGE) {
                s.type = static_cast<uint32_t>(SegmentType::StackPage);
            }
            segs[segIndex++] = s;
            nextOffset += PAGE;
        });
    }

    CoreDumpHeader hdr{};
    hdr.magic = COREDUMP_MAGIC;
    hdr.version = COREDUMP_VERSION;
    hdr.headerSize = sizeof(CoreDumpHeader);
    hdr.timestampQuantums = req.timestamp;
    hdr.pid = req.pid;
    hdr.cpu = req.cpuId;
    hdr.intNum = req.frame.intNum;
    hdr.errCode = req.frame.errCode;
    hdr.cr2 = req.cr2;
    hdr.cr3 = req.cr3;
    hdr.trapFrame = req.frame;
    hdr.trapRegs = req.gpr;
    hdr.savedFrame = req.savedFrame;
    hdr.savedRegs = req.savedRegs;
    hdr.taskEntry = req.entry;
    hdr.taskPagemap = reinterpret_cast<uint64_t>(req.pagemap);
    hdr.elfHeaderAddr = req.elfHeaderAddr;
    hdr.programHeaderAddr = req.programHeaderAddr;
    hdr.segmentCount = segCapacity;
    hdr.segmentTableOffset = sizeof(CoreDumpHeader);
    hdr.elfSize = req.elfBuffer != nullptr ? req.elfBufferSize : 0;
    hdr.elfOffset = nextOffset;
    hdr.segmentEntrySize = sizeof(CoreDumpSegment);
    hdr.pageSize = PAGE;
    hdr.snapshotFlags = 1;  // Full present-user-page snapshot.
    hdr.interpBase = req.interpBase;
    hdr.programHeaderCount = req.programHeaderCount;
    hdr.programHeaderEntSize = req.programHeaderEntSize;
    hdr.threadFsBase = req.threadFsBase;
    hdr.threadGsBase = req.threadGsBase;
    hdr.threadStackBase = req.threadStackBase;
    hdr.threadStackSize = req.threadStackSize;
    hdr.threadTlsBase = req.threadTlsBase;
    hdr.threadTlsSize = req.threadTlsSize;
    hdr.threadSafeStack = req.threadSafeStack;
    std::memcpy(hdr.exePath, req.exePath, sizeof(hdr.exePath));
    std::memcpy(hdr.cwd, req.cwd, sizeof(hdr.cwd));
    std::memcpy(hdr.root, req.root, sizeof(hdr.root));

    bool ok = write_all(fd, &hdr, sizeof(hdr));
    ok = ok && (segCapacity == 0 || write_all(fd, segs, segCapacity * sizeof(CoreDumpSegment)));

    for (uint64_t i = 0; ok && i < segCapacity; ++i) {
        if (segs[i].present == 0) {
            continue;
        }
        void* pagePtr = phys_to_hhdm_checked(segs[i].physAddr);
        if (pagePtr == nullptr) {
            ok = false;
            continue;
        }
        ok = write_all(fd, pagePtr, PAGE);
    }

    if (ok && req.elfBuffer != nullptr && req.elfBufferSize != 0) {
        const uint8_t* elf = req.elfBuffer;
        size_t remaining = req.elfBufferSize;
        while (ok && remaining > 0) {
            size_t chunk = remaining > 4096 ? 4096 : remaining;
            ok = write_all(fd, elf, chunk);
            elf += chunk;
            remaining -= chunk;
        }
    }

    ker::vfs::vfs_close(fd);
    // Intentionally retain the emergency segment table. It is allocated only
    // when a process is already crashing, and freeing multi-page buffers from
    // this path has caused allocator metadata failures before GC finishes.

    if (ok) {
        ker::mod::dbg::log("coredump: wrote %s", path);
        if (droppedSegments != 0) {
            ker::mod::dbg::log("coredump: skipped %d pages because pagemap grew while dumping %s", droppedSegments, path);
        }
        if (walkStats.invalidPml3Frames != 0 || walkStats.invalidPml2Frames != 0 || walkStats.invalidPml1Frames != 0 ||
            walkStats.skippedDataPages != 0) {
            log::warn("coredump: pid=%lu name=%s pagemap=%p dump completed with invalidPml3=%lu invalidPml2=%lu invalidPml1=%lu skippedPages=%lu",
                      req.pid, req.name, static_cast<void*>(req.pagemap), walkStats.invalidPml3Frames, walkStats.invalidPml2Frames,
                      walkStats.invalidPml1Frames, walkStats.skippedDataPages);
        }
    } else {
        ker::mod::dbg::log("coredump: failed while writing %s", path);
        if (walkStats.invalidPml3Frames != 0 || walkStats.invalidPml2Frames != 0 || walkStats.invalidPml1Frames != 0 ||
            walkStats.skippedDataPages != 0) {
            log::warn("coredump: pid=%lu name=%s pagemap=%p dump failed with invalidPml3=%lu invalidPml2=%lu invalidPml1=%lu skippedPages=%lu",
                      req.pid, req.name, static_cast<void*>(req.pagemap), walkStats.invalidPml3Frames, walkStats.invalidPml2Frames,
                      walkStats.invalidPml1Frames, walkStats.skippedDataPages);
        }
    }
}

}  // namespace

void init() {
    g_coredump_task = ker::mod::sched::task::Task::createKernelThread("coredump", coredump_task_fn);
    if (g_coredump_task == nullptr) {
        ker::mod::dbg::log("coredump: failed to create coredump task");
        return;
    }
    ker::mod::sched::post_task_balanced(g_coredump_task);
    ker::mod::dbg::log("coredump: task started");
}

void tryWriteForTask(ker::mod::sched::task::Task* task, const ker::mod::cpu::GPRegs& gpr, const ker::mod::gates::interruptFrame& frame,
                     uint64_t cr2, uint64_t cr3, uint64_t cpuId) {
    // Crash-path coredumps are temporarily disabled. They are currently
    // provoking secondary allocator / filesystem failures that make the real
    // userspace mapping bug much harder to isolate.
    (void)task;
    (void)gpr;
    (void)frame;
    (void)cr2;
    (void)cr3;
    (void)cpuId;
    return;

    if (task == nullptr || g_coredump_task == nullptr) {
        return;
    }

    // Claim a ring slot. Wrap around; drop if the consumer hasn't cleared this slot yet.
    uint32_t seq = g_write_seq.fetch_add(1, std::memory_order_relaxed);
    uint32_t idx = seq % RING_SIZE;
    CoreDumpSlot& slot = g_ring[idx];

    if (slot.ready.load(std::memory_order_acquire)) {
        ker::mod::dbg::log("coredump: ring full, dropping coredump for PID %x", task->pid);
        return;
    }

    // Take a refcount on the task. This prevents GC from reclaiming it (and its pagemap
    // and user pages) while the coredump task reads them via HHDM. The coredump task
    // calls task->release() when done. tryAcquire fails if task is already EXITING/DEAD.
    if (!task->tryAcquire()) {
        return;
    }

    CoreDumpRequest& req = slot.req;
    req.gpr = gpr;
    req.frame = frame;
    req.savedFrame = task->context.frame;
    req.savedRegs = task->context.regs;
    req.cr2 = cr2;
    req.cr3 = cr3;
    req.cpuId = cpuId;
    req.pid = task->pid;
    req.entry = task->entry;
    req.elfHeaderAddr = task->elfHeaderAddr;
    req.programHeaderAddr = task->programHeaderAddr;
    req.interpBase = task->interpBase;
    req.programHeaderCount = task->programHeaderCount;
    req.programHeaderEntSize = task->programHeaderEntSize;
    req.threadFsBase = 0;
    req.threadGsBase = 0;
    req.threadStackBase = 0;
    req.threadStackSize = 0;
    req.threadTlsBase = 0;
    req.threadTlsSize = 0;
    req.threadSafeStack = 0;
    if (task->thread != nullptr) {
        req.threadFsBase = task->thread->fsbase;
        req.threadGsBase = task->thread->gsbase;
        req.threadStackBase = task->thread->gsbase;
        req.threadStackSize = task->thread->stackSize;
        req.threadTlsBase = task->thread->tlsBaseVirt;
        req.threadTlsSize = task->thread->tlsSize;
        req.threadSafeStack = task->thread->safestackPtrValue;
    }
    req.pagemap = task->pagemap;
    req.userRsp = frame.rsp;
    req.timestamp = ker::mod::time::getTicks();
    req.taskPtr = task;
    sanitize_name(task->name, req.name, sizeof(req.name));
    std::memcpy(req.exePath, task->exe_path, sizeof(req.exePath));
    std::memcpy(req.cwd, task->cwd, sizeof(req.cwd));
    std::memcpy(req.root, task->root, sizeof(req.root));

    // Steal elfBuffer so exit() won't free it before the coredump task writes it.
    // Private buffers are intentionally retained after writing; see coredump_task_fn().
    req.elfBuffer = task->elfBuffer;
    req.elfBufferSize = task->elfBufferSize;
    req.elfBufferShared = task->isElfBufferShared;
    task->elfBuffer = nullptr;
    task->elfBufferSize = 0;
    task->isElfBufferShared = false;

    // Publish the slot and wake the coredump task. Safe from CPL=3 exception context:
    // userspace-fault handlers cannot be holding any kernel spinlock.
    slot.ready.store(true, std::memory_order_release);
    ker::mod::sched::kern_wake(g_coredump_task);
}

}  // namespace ker::mod::dbg::coredump
