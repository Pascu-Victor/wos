#include "coredump.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
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

constexpr uint64_t COREDUMP_MAGIC = 0x504d55444f43534full;  // "WOSCODMP" little-endian-ish identifier
constexpr uint32_t COREDUMP_VERSION = 1;

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
} __attribute__((packed));

enum class SegmentType : uint32_t {
    StackPage = 1,
    FaultPage = 2,
};

struct CoreDumpSegment {
    uint64_t vaddr;
    uint64_t size;
    uint64_t fileOffset;
    uint32_t type;
    uint32_t present;
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
    uint8_t* elfBuffer;      // stolen from task->elfBuffer; coredump task frees after write
    size_t elfBufferSize;
    ker::mod::mm::paging::PageTable* pagemap;
    uint64_t userRsp;
    uint64_t timestamp;
    ker::mod::sched::task::Task* taskPtr;  // holds a refcount to block GC; released when done
    char name[48];
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

            // Free the ELF buffer stolen from the crashing task.
            if (req.elfBuffer != nullptr) {
                if (!ker::net::wki::wki_remote_compute_release_elf_buffer(req.elfBuffer)) {
                    delete[] req.elfBuffer;
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
    const char prefix[] = "/root/";
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

    constexpr uint64_t PAGE = 4096;
    constexpr uint64_t MAX_STACK_PAGES = 4;
    CoreDumpSegment segs[MAX_STACK_PAGES + 1] = {};
    uint64_t segCount = 0;

    uint64_t dataOffset = sizeof(CoreDumpHeader) + sizeof(segs);
    uint64_t nextOffset = dataOffset;

    auto add_page = [&](uint64_t vaddrPage, SegmentType type) {
        CoreDumpSegment s{};
        s.vaddr = vaddrPage;
        s.size = PAGE;
        s.type = static_cast<uint32_t>(type);

        uint64_t phys = 0;
        if (req.pagemap != nullptr) {
            phys = ker::mod::mm::virt::translate(req.pagemap, vaddrPage);
        }

        if (phys != ker::mod::mm::virt::PADDR_INVALID && is_ram(phys)) {
            s.present = 1;
            s.fileOffset = nextOffset;
            nextOffset += PAGE;
        } else {
            s.present = 0;
            s.fileOffset = 0;
        }

        segs[segCount++] = s;
    };

    uint64_t stackPage = req.userRsp & ~(PAGE - 1);
    for (uint64_t i = 0; i < MAX_STACK_PAGES; ++i) {
        add_page(stackPage - i * PAGE, SegmentType::StackPage);
    }

    uint64_t faultPage = req.cr2 & ~(PAGE - 1);
    add_page(faultPage, SegmentType::FaultPage);

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
    hdr.segmentCount = segCount;
    hdr.segmentTableOffset = sizeof(CoreDumpHeader);
    hdr.elfSize = req.elfBuffer != nullptr ? req.elfBufferSize : 0;
    hdr.elfOffset = nextOffset;

    bool ok = write_all(fd, &hdr, sizeof(hdr));
    ok = ok && write_all(fd, &segs[0], sizeof(segs));

    for (uint64_t i = 0; ok && i < segCount; ++i) {
        if (segs[i].present == 0) {
            continue;
        }
        uint64_t phys = ker::mod::mm::virt::translate(req.pagemap, segs[i].vaddr);
        if (phys == ker::mod::mm::virt::PADDR_INVALID || !is_ram(phys)) {
            continue;
        }
        void* pagePtr = ker::mod::mm::addr::get_virt_pointer(phys);
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

    if (ok) {
        ker::mod::dbg::log("coredump: wrote %s", path);
    } else {
        ker::mod::dbg::log("coredump: failed while writing %s", path);
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
    req.pagemap = task->pagemap;
    req.userRsp = task->context.frame.rsp;
    req.timestamp = ker::mod::time::getTicks();
    req.taskPtr = task;
    sanitize_name(task->name, req.name, sizeof(req.name));

    // Steal elfBuffer so exit() won't free it before the coredump task writes it.
    // The coredump task owns it from this point and frees it after writing.
    req.elfBuffer = task->elfBuffer;
    req.elfBufferSize = task->elfBufferSize;
    task->elfBuffer = nullptr;
    task->elfBufferSize = 0;

    // Publish the slot and wake the coredump task. Safe from CPL=3 exception context:
    // userspace-fault handlers cannot be holding any kernel spinlock.
    slot.ready.store(true, std::memory_order_release);
    ker::mod::sched::kern_wake(g_coredump_task);
}

}  // namespace ker::mod::dbg::coredump
