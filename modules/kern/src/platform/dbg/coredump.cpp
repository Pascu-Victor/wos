#include "coredump.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/task.hpp>
#include <vfs/vfs.hpp>

namespace ker::mod::dbg::coredump {
namespace {

constexpr uint64_t COREDUMP_MAGIC = 0x504d55444f43534full;  // "WOSCODMP" little-endian-ish identifier
constexpr uint32_t COREDUMP_VERSION = 1;

// Keep this in sync with tmpfs' internal flag.
constexpr int O_CREAT = 0x100;

bool is_ram(uint64_t phys) {
    for (auto* zone = ker::mod::mm::phys::getZones(); zone != nullptr; zone = zone->next) {
        // zone->start is virtual (HHDM)
        // zone->len is length in bytes
        uint64_t zoneStartPhys = (uint64_t)ker::mod::mm::addr::getPhysPointer((ker::mod::mm::addr::vaddr_t)zone->start);
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

}  // namespace

void tryWriteForTask(ker::mod::sched::task::Task* task, const ker::mod::cpu::GPRegs& gpr, const ker::mod::gates::interruptFrame& frame,
                     uint64_t cr2, uint64_t cr3, uint64_t cpuId) {
    if (task == nullptr) {
        return;
    }

    // CRITICAL: Enter epoch critical section to prevent GC from freeing the task
    // or its resources (pagemap, thread, etc.) while we're writing the coredump.
    // This ensures the task pointer and all its fields remain valid for the duration.
    ker::mod::sched::EpochGuard epochGuard;

    // The fault may have happened while running on a userspace pagemap.
    // Some kernel allocations (e.g. FAT tables, VFS metadata) are not guaranteed
    // to be mapped there, so switch to the kernel pagemap for the duration.
    uint64_t savedCr3 = rdcr3();
    ker::mod::mm::virt::switchToKernelPagemap();

    uint64_t timestamp = ker::mod::time::getTicks();

    char prog[48];
    sanitize_name(task->name, prog, sizeof(prog));

    char ts[32];
    u64_to_dec(ts, sizeof(ts), timestamp);

    char path[160];
    // Format: /mnt/disk/[PROGRAM]_[TIMESTAMP]_coredump.bin
    const char suffix[] = "_coredump.bin";
    size_t pos = 0;
    const char prefix[] = "/mnt/disk/";
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
        wrcr3(savedCr3);
        return;
    }

    // Decide which user pages to snapshot (best-effort).
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
        if (task->pagemap != nullptr) {
            phys = ker::mod::mm::virt::translate(task->pagemap, vaddrPage);
        }

        if (phys != 0 && is_ram(phys)) {
            s.present = 1;
            s.fileOffset = nextOffset;
            nextOffset += PAGE;
        } else {
            s.present = 0;
            s.fileOffset = 0;
        }

        segs[segCount++] = s;
    };

    // Stack pages around faulting user RSP (even if the trap was kernel-mode, this is still useful).
    uint64_t userRsp = task->context.frame.rsp;
    uint64_t stackPage = userRsp & ~(PAGE - 1);
    for (uint64_t i = 0; i < MAX_STACK_PAGES; ++i) {
        add_page(stackPage - i * PAGE, SegmentType::StackPage);
    }

    // Faulting address page (CR2).
    uint64_t faultPage = cr2 & ~(PAGE - 1);
    add_page(faultPage, SegmentType::FaultPage);

    // Build header.
    CoreDumpHeader hdr{};
    hdr.magic = COREDUMP_MAGIC;
    hdr.version = COREDUMP_VERSION;
    hdr.headerSize = sizeof(CoreDumpHeader);
    hdr.timestampQuantums = timestamp;
    hdr.pid = task->pid;
    hdr.cpu = cpuId;
    hdr.intNum = frame.intNum;
    hdr.errCode = frame.errCode;
    hdr.cr2 = cr2;
    hdr.cr3 = cr3;
    hdr.trapFrame = frame;
    hdr.trapRegs = gpr;
    hdr.savedFrame = task->context.frame;
    hdr.savedRegs = task->context.regs;
    hdr.taskEntry = task->entry;
    hdr.taskPagemap = reinterpret_cast<uint64_t>(task->pagemap);
    hdr.elfHeaderAddr = task->elfHeaderAddr;
    hdr.programHeaderAddr = task->programHeaderAddr;
    hdr.segmentCount = segCount;
    hdr.segmentTableOffset = sizeof(CoreDumpHeader);

    hdr.elfSize = task->elfBuffer != nullptr ? task->elfBufferSize : 0;
    hdr.elfOffset = nextOffset;

    // Write header + full segment table array (fixed size) for simplicity.
    bool ok = write_all(fd, &hdr, sizeof(hdr));
    ok = ok && write_all(fd, &segs[0], sizeof(segs));

    // Write segment contents.
    for (uint64_t i = 0; ok && i < segCount; ++i) {
        if (segs[i].present == 0) {
            continue;
        }
        uint64_t phys = ker::mod::mm::virt::translate(task->pagemap, segs[i].vaddr);
        if (phys == 0 || !is_ram(phys)) {
            continue;
        }
        void* pagePtr = ker::mod::mm::addr::getVirtPointer(phys);
        ok = write_all(fd, pagePtr, PAGE);
    }

    // Write ELF image bytes if available.
    if (ok && task->elfBuffer != nullptr && task->elfBufferSize != 0) {
        const uint8_t* elf = task->elfBuffer;
        size_t remaining = task->elfBufferSize;
        while (ok && remaining > 0) {
            size_t chunk = remaining > 4096 ? 4096 : remaining;
            ok = write_all(fd, elf, chunk);
            elf += chunk;
            remaining -= chunk;
        }
    }

    ker::vfs::vfs_close(fd);

    // Restore original address space before returning.
    wrcr3(savedCr3);

    if (ok) {
        ker::mod::dbg::log("coredump: wrote %s", path);
    } else {
        ker::mod::dbg::log("coredump: failed while writing %s", path);
    }
}

}  // namespace ker::mod::dbg::coredump
