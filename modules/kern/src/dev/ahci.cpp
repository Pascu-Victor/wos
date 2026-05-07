/**
 * AHCI Driver - Complete rewrite following OSDev Wiki specification
 * Reference: https://wiki.osdev.org/AHCI
 */

#include "ahci.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <net/wki/remotable.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/sched/scheduler.hpp>

#include "block_device.hpp"
#include "pci.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sys/spinlock.hpp"
#ifdef AHCI_BENCH
#include "platform/tsc/tsc.hpp"
#endif

namespace ker::dev::ahci {

// AHCI Constants
constexpr uint32_t HBA_PxCMD_ST = 0x0001;
constexpr uint32_t HBA_PxCMD_FRE = 0x0010;
constexpr uint32_t HBA_PxCMD_FR = 0x4000;
constexpr uint32_t HBA_PxCMD_CR = 0x8000;
constexpr uint32_t HBA_PxIS_TFES = 0x40000000;
constexpr uint8_t ATA_DEV_BUSY = 0x80;
constexpr uint8_t ATA_DEV_DRQ = 0x08;
constexpr uint8_t ATA_CMD_READ_DMA_EX = 0x25;
constexpr uint8_t ATA_CMD_WRITE_DMA_EX = 0x35;
constexpr uint8_t ATA_CMD_FLUSH_CACHE_EXT = 0xEA;
constexpr uint8_t ATA_CMD_IDENTIFY = 0xEC;

constexpr uint32_t SATA_SIG_ATA = 0x00000101;
constexpr uint32_t SATA_SIG_ATAPI = 0xEB140101;
constexpr uint32_t SATA_SIG_SEMB = 0xC33C0101;
constexpr uint32_t SATA_SIG_PM = 0x96690101;

constexpr uint8_t HBA_PORT_IPM_ACTIVE = 1;
constexpr uint8_t HBA_PORT_DET_PRESENT = 3;

// Bit masks

constexpr uint32_t HBA_GHC_AE = 0x80000000;  // AHCI Enable
constexpr uint32_t HBA_GHC_IE = 0x00000002;  // Interrupt Enable

enum : uint8_t { AHCI_DEV_NULL = 0, AHCI_DEV_SATA = 1, AHCI_DEV_SEMB = 2, AHCI_DEV_PM = 3, AHCI_DEV_SATAPI = 4 };

// Global state
namespace {
// MMIO read/write helpers for clairity and to ensure volatile semantics
inline auto mmio_read32(volatile uint32_t& reg) -> uint32_t { return reg; }
inline void mmio_write32(volatile uint32_t& reg, uint32_t val) { reg = val; }
inline void mmio_or32(volatile uint32_t& reg, uint32_t bits) { reg |= bits; }
inline void mmio_and32(volatile uint32_t& reg, uint32_t mask) { reg &= mask; }

// WKI remotable ops for AHCI block devices
auto remotable_can_remote() -> bool { return true; }
auto remotable_can_share() -> bool { return true; }
auto remotable_can_passthrough() -> bool { return false; }
auto remotable_on_attach(uint16_t node_id) -> int {
    ker::mod::dbg::log("[AHCI] remote attach from 0x%04x", node_id);
    return 0;
}
void remotable_on_detach(uint16_t node_id) { ker::mod::dbg::log("[AHCI] remote detach from 0x%04x", node_id); }
void remotable_on_fault(uint16_t node_id) { ker::mod::dbg::log("[AHCI] remote fault for 0x%04x", node_id); }
const ker::net::wki::RemotableOps s_remotable_ops = {
    .can_remote = remotable_can_remote,
    .can_share = remotable_can_share,
    .can_passthrough = remotable_can_passthrough,
    .on_remote_attach = remotable_on_attach,
    .on_remote_detach = remotable_on_detach,
    .on_remote_fault = remotable_on_fault,
};

volatile HBA_MEM* hba_mem = nullptr;
constexpr size_t MAX_PORTS = 32;
AHCIDevice devices[MAX_PORTS];  // NOLINT
size_t device_count = 0;

// Store virtual addresses for command structures
struct PortMemory {
    HBA_CMD_HEADER* clb_virt;
    uint8_t* fb_virt;
    uint8_t* ctb_virt[32];  // Command tables NOLINT
};

PortMemory port_memory[MAX_PORTS];  // NOLINT

// Per-port spinlocks to protect concurrent disk I/O operations
// This prevents race conditions when multiple CPUs try to access the same disk simultaneously
ker::mod::sys::Spinlock port_locks[MAX_PORTS];  // NOLINT

// Software-level tracking of which command slots are in use
// This prevents the race where we release the lock but hardware hasn't updated ci yet
std::atomic<uint32_t> port_slots_in_use[MAX_PORTS] = {};  // NOLINT

// Stop command engine on a port
void stop_cmd(volatile HBA_PORT* port) {
    // Clear ST (bit0)
    mmio_and32(port->cmd, ~HBA_PxCMD_ST);

    // Clear FRE (bit4)
    mmio_and32(port->cmd, ~HBA_PxCMD_FRE);

    // Wait until FR (bit14), CR (bit15) are cleared
    while (true) {
        if ((mmio_read32(port->cmd) & HBA_PxCMD_FR) != 0U) {
            continue;
        }
        if ((mmio_read32(port->cmd) & HBA_PxCMD_CR) != 0U) {
            continue;
        }
        break;
    }
}

// Start command engine on a port
void start_cmd(volatile HBA_PORT* port) {
    // Wait until CR (bit15) is cleared
    uint32_t timeout = 0;
    constexpr size_t MAX_TIMEOUT = 1000000;
    while ((mmio_read32(port->cmd) & HBA_PxCMD_CR) != 0U) {
        if (timeout++ == MAX_TIMEOUT) {
            ahci_log("start_cmd: timeout waiting for CR to clear\n");
            break;
        }
    }

    // Set FRE (bit4) and ST (bit0)
    mmio_or32(port->cmd, HBA_PxCMD_FRE);
    mmio_or32(port->cmd, HBA_PxCMD_ST);
}

// Check device type on a port
auto check_type(volatile HBA_PORT* port) -> int {
    uint32_t ssts = mmio_read32(port->ssts);

    uint8_t ipm = (ssts >> 8) & 0x0F;
    uint8_t det = ssts & 0x0F;

    if (det != HBA_PORT_DET_PRESENT)  // Check drive status
        return AHCI_DEV_NULL;
    if (ipm != HBA_PORT_IPM_ACTIVE) {
        return AHCI_DEV_NULL;
    }

    switch (mmio_read32(port->sig)) {
        case SATA_SIG_ATAPI:
            return AHCI_DEV_SATAPI;
        case SATA_SIG_SEMB:
            return AHCI_DEV_SEMB;
        case SATA_SIG_PM:
            return AHCI_DEV_PM;
        default:
            return AHCI_DEV_SATA;
    }
}

// Rebase port memory structures
void port_rebase(volatile HBA_PORT* port, size_t portno) {
    stop_cmd(port);  // Stop command engine

    // Get kernel page table for physical address translation
    auto* kernel_pt = ker::mod::mm::virt::getKernelPagemap();

    // Command list offset: 1K*portno
    // Command list entry size = 32 bytes
    // Command list entry maxim count = 32
    // Command list maxium size = 32*32 = 1K per port
    auto* clb_virt = new (std::nothrow) uint8_t[1024];
    if (clb_virt == nullptr) {
        ker::mod::dbg::log("ahci: failed to allocate CLB kernel allocation");
        hcf();
    }
    std::memset(clb_virt, 0, 1024);
    uint64_t clb_phys = ker::mod::mm::virt::translate(kernel_pt, reinterpret_cast<uint64_t>(clb_virt));
    if (clb_phys == ker::mod::mm::virt::PADDR_INVALID) {
        ker::mod::dbg::log("ahci: failed to translate CLB kernel allocation");
        hcf();
    }
    mmio_write32(port->clb, static_cast<uint32_t>(clb_phys & UINT32_MAX));
    mmio_write32(port->clbu, static_cast<uint32_t>((clb_phys >> 32) & UINT32_MAX));

    // Store virtual address for later use
    port_memory[portno].clb_virt = reinterpret_cast<HBA_CMD_HEADER*>(clb_virt);

    // FIS offset: 32K+256*portno
    // FIS entry size = 256 bytes per port
    auto* fb_virt = new (std::nothrow) uint8_t[256];
    if (fb_virt == nullptr) {
        ker::mod::dbg::log("ahci: failed to allocate FIS buffer kernel allocation");
        hcf();
    }
    std::memset(fb_virt, 0, 256);
    uint64_t fb_phys = ker::mod::mm::virt::translate(kernel_pt, reinterpret_cast<uint64_t>(fb_virt));
    if (fb_phys == ker::mod::mm::virt::PADDR_INVALID) {
        ker::mod::dbg::log("ahci: failed to translate FIS buffer kernel allocation");
        hcf();
    }
    mmio_write32(port->fb, static_cast<uint32_t>(fb_phys & UINT32_MAX));
    mmio_write32(port->fbu, static_cast<uint32_t>((fb_phys >> 32) & UINT32_MAX));

    // Store virtual address for later use
    port_memory[portno].fb_virt = fb_virt;

    // Command table offset: 40K + 8K*portno
    // Command table size = 256*32 = 8K per port
    auto* cmdheader = reinterpret_cast<HBA_CMD_HEADER*>(clb_virt);
    for (int i = 0; i < 32; i++) {
        // Command table: 128B fixed header + 8192 × 16B PRDT entries = 131200 bytes.
        // 8192 entries × 4KB/page = 32MB per command (ATA sector count is 16-bit = 32MB max).
        constexpr size_t CTB_SIZE = 128 + (8192 * sizeof(HBA_PRDT_ENTRY));
        cmdheader[i].prdtl = 8192;
        auto* ctb_virt = new (std::nothrow) uint8_t[CTB_SIZE];
        if (ctb_virt == nullptr) {
            ker::mod::dbg::log("ahci: failed to allocate CTB kernel allocation");
            hcf();
        }
        std::memset(ctb_virt, 0, CTB_SIZE);
        uint64_t ctb_phys = ker::mod::mm::virt::translate(kernel_pt, reinterpret_cast<uint64_t>(ctb_virt));
        if (ctb_phys == ker::mod::mm::virt::PADDR_INVALID) {
            ker::mod::dbg::log("ahci: failed to translate CTB kernel allocation");
            hcf();
        }
        cmdheader[i].ctba = static_cast<uint32_t>(ctb_phys & UINT32_MAX);
        cmdheader[i].ctbau = static_cast<uint32_t>((ctb_phys >> 32) & UINT32_MAX);

        // Store virtual address for later use
        port_memory[portno].ctb_virt[i] = ctb_virt;
    }

    start_cmd(port);  // Start command engine
}

// Find a free command slot (must be called with port lock held)
auto find_cmdslot(volatile HBA_PORT* port, int portno) -> uint32_t {
    // Check both hardware registers AND software tracking for busy slots
    // The software tracking handles the race where we've issued a command but hardware hasn't updated ci yet
    uint32_t hw_slots = (mmio_read32(port->sact) | mmio_read32(port->ci));
    uint32_t sw_slots =
        (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) ? port_slots_in_use[portno].load(std::memory_order_acquire) : 0;
    uint32_t slots = hw_slots | sw_slots;
    uint32_t cmdslots = (mmio_read32(hba_mem->cap) & 0x1F00) >> 8;  // Number of command slots
    for (uint32_t i = 0; i < cmdslots; i++) {
        if ((slots & 1) == 0) {
            return i;
        }
        slots >>= 1;
    }
    ahci_log("Cannot find free command list entry\n");
    return -1;
}

// Generic function to read/write sectors from/to disk
auto read_write_disk(volatile HBA_PORT* port, int portno, uint32_t startl, uint32_t starth, uint32_t count, uint8_t* buf, bool write_op)
    -> bool {
    uint32_t slot = UINT32_MAX;

    // Acquire per-port lock only for the critical section: finding a slot and issuing the command
    // We must NOT hold the lock during the busy-wait for completion, as that would cause livelock
    if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
        port_locks[portno].lock();
    }

    mmio_write32(port->is, static_cast<uint32_t>(-1));  // Clear pending interrupt bits
    slot = find_cmdslot(port, portno);
    if (slot == UINT32_MAX) {
        if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
            port_locks[portno].unlock();
        }
        return false;
    }

    // Mark this slot as in-use in software BEFORE releasing the lock
    // This prevents other CPUs from grabbing the same slot before hardware updates ci
    if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
        port_slots_in_use[portno].fetch_or(1U << slot, std::memory_order_release);
    }

    // Choose the right page table for buffer virtual->physical translation.
    // Kernel-space buffers (HHDM addresses) must always use the kernel pagemap
    // because kernel threads may have a stale or unrelated task pagemap.
    // Userspace buffers (below HHDM) need the current task's pagemap.
    ker::mod::mm::paging::PageTable* active_pt = nullptr;
    {
        uint64_t hhdm = ker::mod::mm::addr::get_hhdm_offset();
        bool is_kernel_buf = (reinterpret_cast<uint64_t>(buf) >= hhdm);

        if (is_kernel_buf) {
            active_pt = ker::mod::mm::virt::getKernelPagemap();
        } else {
            ker::mod::sched::task::Task* cur_task = nullptr;
            if (ker::mod::sched::has_run_queues()) {
                cur_task = ker::mod::sched::get_current_task();
            }
            if (cur_task != nullptr && cur_task->pagemap != nullptr) {
                active_pt = cur_task->pagemap;
            } else {
                active_pt = ker::mod::mm::virt::getKernelPagemap();
            }
        }
    }

    // Get command header using stored virtual address
    HBA_CMD_HEADER* cmdheader = &port_memory[portno].clb_virt[slot];

    // Each PRDT entry must stay within a single mapped page span. The old code
    // assumed page-aligned buffers and advanced in fixed 4 KiB steps, which
    // breaks for callers like remote VFS that DMA into resp_buf + header.
    constexpr uint32_t SECTOR_SIZE = 512;
    constexpr size_t MAX_PRDT = 8192;

    uint64_t total_bytes = static_cast<uint64_t>(count) * SECTOR_SIZE;
    uint64_t buf_virt = reinterpret_cast<uint64_t>(buf);
    uint64_t first_page_off = buf_virt & (ker::mod::mm::paging::PAGE_SIZE - 1);
    size_t prdt_entries =
        static_cast<size_t>((first_page_off + total_bytes + ker::mod::mm::paging::PAGE_SIZE - 1) / ker::mod::mm::paging::PAGE_SIZE);
    prdt_entries = std::min(prdt_entries, MAX_PRDT);

    auto* cmdtbl = reinterpret_cast<HBA_CMD_TBL*>(port_memory[portno].ctb_virt[slot]);
    std::memset(cmdtbl, 0, sizeof(HBA_CMD_TBL) + ((prdt_entries - 1) * sizeof(HBA_PRDT_ENTRY)));

    uint64_t remaining_bytes = total_bytes;
    size_t actual_prdt_entries = 0;
    while (remaining_bytes > 0 && actual_prdt_entries < prdt_entries) {
        uint64_t phys = ker::mod::mm::virt::translate(active_pt, buf_virt);
        if (phys == ker::mod::mm::virt::PADDR_INVALID) {
            if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
                port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
                port_locks[portno].unlock();
            }
            return false;
        }

        uint64_t page_off = buf_virt & (ker::mod::mm::paging::PAGE_SIZE - 1);
        uint64_t bytes_this_entry = std::min<uint64_t>(remaining_bytes, ker::mod::mm::paging::PAGE_SIZE - page_off);

        cmdtbl->prdt_entry[actual_prdt_entries].dba = static_cast<uint32_t>(phys & UINT32_MAX);
        cmdtbl->prdt_entry[actual_prdt_entries].dbau = static_cast<uint32_t>((phys >> 32) & UINT32_MAX);
        cmdtbl->prdt_entry[actual_prdt_entries].dbc = static_cast<uint32_t>(bytes_this_entry - 1);
        cmdtbl->prdt_entry[actual_prdt_entries].i = 1;

        buf_virt += bytes_this_entry;
        remaining_bytes -= bytes_this_entry;
        actual_prdt_entries++;
    }

    if (remaining_bytes != 0) {
        if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
            port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
            port_locks[portno].unlock();
        }
        return false;
    }

    cmdheader->cfl = sizeof(FIS_REG_H2D) / sizeof(uint32_t);
    cmdheader->w = write_op ? 1 : 0;
    cmdheader->prdtl = static_cast<uint16_t>(actual_prdt_entries);

    // Setup command FIS
    auto* cmdfis = reinterpret_cast<FIS_REG_H2D*>(&cmdtbl->cfis);
    cmdfis->fis_type = FIS_TYPE_REG_H2D;
    cmdfis->c = 1;  // Command
    cmdfis->command = write_op ? ATA_CMD_WRITE_DMA_EX : ATA_CMD_READ_DMA_EX;

    cmdfis->lba0 = static_cast<uint8_t>(startl);
    cmdfis->lba1 = static_cast<uint8_t>(startl >> 8);
    cmdfis->lba2 = static_cast<uint8_t>(startl >> 16);
    cmdfis->device = 1 << 6;  // LBA mode

    cmdfis->lba3 = static_cast<uint8_t>(startl >> 24);
    cmdfis->lba4 = static_cast<uint8_t>(starth);
    cmdfis->lba5 = static_cast<uint8_t>(starth >> 8);

    cmdfis->countl = count & 0xFF;
    cmdfis->counth = (count >> 8) & 0xFF;

    // Wait for port to be ready (still holding lock - this should be quick)
    int spin = 0;
    constexpr int MAX_SPIN = 1000000;
    while (((mmio_read32(port->tfd) & (ATA_DEV_BUSY | ATA_DEV_DRQ)) != 0U) && spin < MAX_SPIN) {
        spin++;
    }
    if (spin == MAX_SPIN) {
        ahci_log("Port is hung\n");
        if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
            port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
            port_locks[portno].unlock();
        }
        return false;
    }

    // Issue command
#ifdef AHCI_BENCH
    uint64_t t_issue = ker::mod::tsc::getNs();
#endif
    mmio_write32(port->ci, 1 << slot);

    // Release lock after issuing command - the slot is now ours and we just need to wait
    // Other concurrent I/O operations can use different command slots
    if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
        port_locks[portno].unlock();
    }

    // Wait for completion (without holding lock - this can take a long time)
    constexpr uint64_t COMPLETION_TIMEOUT = 50000000ULL;  // ~50M iterations
    uint64_t wait_iter = 0;
    while (true) {
        uint32_t ci_val = mmio_read32(port->ci);
        if ((ci_val & (1 << slot)) == 0) {
            break;
        }
        uint32_t is_val = mmio_read32(port->is);
        if ((is_val & HBA_PxIS_TFES) != 0U) {  // Task file error
            if (write_op) {
                ahci_log("Write disk error\n");
            } else {
                ahci_log("Read disk error\n");
            }
            // Clear the software slot tracking on error
            if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
                port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
            }
            return false;
        }
        if (++wait_iter >= COMPLETION_TIMEOUT) {
            uint32_t tfd_val = mmio_read32(port->tfd);
            uint32_t serr_val = mmio_read32(port->serr);
            uint32_t cmd_val = mmio_read32(port->cmd);
            uint32_t ssts_val = mmio_read32(port->ssts);
            ker::mod::dbg::log("AHCI TIMEOUT: slot=%u ci=0x%x is=0x%x tfd=0x%x serr=0x%x cmd=0x%x ssts=0x%x %s lba=%u:%u cnt=%u", slot,
                               ci_val, is_val, tfd_val, serr_val, cmd_val, ssts_val, write_op ? "WRITE" : "READ", starth, startl, count);
            if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
                port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
            }
            return false;
        }
        // Yield to other threads/allow interrupts during long waits
        asm volatile("pause");
    }

    // Check again
    if ((mmio_read32(port->is) & HBA_PxIS_TFES) != 0U) {
        if (write_op) {
            ahci_log("Write disk error\n");
        } else {
            ahci_log("Read disk error\n");
        }
        // Clear the software slot tracking on error
        if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
            port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
        }
        return false;
    }

#ifdef AHCI_BENCH
    {
        uint64_t t_done = ker::mod::tsc::getNs();
        static std::atomic<uint64_t> s_cmd_count{0};
        static std::atomic<uint64_t> s_total_ns{0};
        static std::atomic<uint64_t> s_total_sectors{0};
        uint64_t n = s_cmd_count.fetch_add(1, std::memory_order_relaxed);
        s_total_ns.fetch_add(t_done - t_issue, std::memory_order_relaxed);
        s_total_sectors.fetch_add(count, std::memory_order_relaxed);
        if ((n & 63) == 63) {
            uint64_t total_ns = s_total_ns.exchange(0, std::memory_order_relaxed);
            uint64_t total_sec = s_total_sectors.exchange(0, std::memory_order_relaxed);
            uint64_t avg_us = total_ns / (64ULL * 1000ULL);
            uint64_t mbps = total_sec == 0 ? 0 : (total_sec * 512 * 1000) / (total_ns == 0 ? 1 : total_ns);
            ker::mod::dbg::log("[AHCI bench] cmd#%lu: avg_latency=%luus throughput~%luMB/s (last 64 cmds, %lu sectors)\n", (unsigned long)n,
                               (unsigned long)avg_us, (unsigned long)mbps, (unsigned long)total_sec);
        }
    }
#endif

    // Clear the software slot tracking on success
    if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
        port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
    }

    return true;
}

// Max sectors per AHCI command: 512 PRDT entries × 8 sectors/page = 4096 sectors (2 MB)
// 8192 PRDT entries × 8 sectors/page = 65536, capped at 65535 (ATA 16-bit sector count)
constexpr uint32_t AHCI_MAX_SECTORS_PER_CMD = 65535;

// Read sectors from disk - splits large requests into per-command chunks
auto read_disk(volatile HBA_PORT* port, int portno, uint32_t startl, uint32_t starth, uint32_t count, uint8_t* buf) -> bool {
    uint32_t done = 0;
    while (done < count) {
        uint32_t chunk = count - done;
        chunk = std::min(chunk, AHCI_MAX_SECTORS_PER_CMD);
        uint64_t lba = (static_cast<uint64_t>(starth) << 32) | startl;
        lba += done;
        if (!read_write_disk(port, portno, static_cast<uint32_t>(lba & 0xFFFFFFFFU), static_cast<uint32_t>(lba >> 32), chunk,
                             buf + (done * 512UL), false)) {
            return false;
        }
        done += chunk;
    }
    return true;
}

// Write sectors to disk - splits large requests into per-command chunks
auto write_disk(volatile HBA_PORT* port, int portno, uint32_t startl, uint32_t starth, uint32_t count, uint8_t* buf) -> bool {
    uint32_t done = 0;
    while (done < count) {
        uint32_t chunk = count - done;
        chunk = std::min(chunk, AHCI_MAX_SECTORS_PER_CMD);
        uint64_t lba = (static_cast<uint64_t>(starth) << 32) | startl;
        lba += done;
        if (!read_write_disk(port, portno, static_cast<uint32_t>(lba & 0xFFFFFFFFU), static_cast<uint32_t>(lba >> 32), chunk,
                             buf + (done * 512UL), true)) {
            return false;
        }
        done += chunk;
    }
    return true;
}

// Flush volatile write cache to non-volatile storage
auto flush_disk(volatile HBA_PORT* port, int portno) -> bool {
    uint32_t slot = UINT32_MAX;

    if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
        port_locks[portno].lock();
    }

    mmio_write32(port->is, static_cast<uint32_t>(-1));
    slot = find_cmdslot(port, portno);
    if (slot == UINT32_MAX) {
        if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
            port_locks[portno].unlock();
        }
        return false;
    }

    if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
        port_slots_in_use[portno].fetch_or(1U << slot, std::memory_order_release);
    }

    HBA_CMD_HEADER* cmdheader = &port_memory[portno].clb_virt[slot];
    cmdheader->cfl = sizeof(FIS_REG_H2D) / sizeof(uint32_t);
    cmdheader->w = 0;
    cmdheader->prdtl = 0;  // No data transfer

    auto* cmdtbl = reinterpret_cast<HBA_CMD_TBL*>(port_memory[portno].ctb_virt[slot]);
    std::memset(cmdtbl, 0, sizeof(HBA_CMD_TBL));

    auto* cmdfis = reinterpret_cast<FIS_REG_H2D*>(&cmdtbl->cfis);
    cmdfis->fis_type = FIS_TYPE_REG_H2D;
    cmdfis->c = 1;
    cmdfis->command = ATA_CMD_FLUSH_CACHE_EXT;
    cmdfis->device = 1 << 6;  // LBA mode

    // Wait for port to be ready
    int spin = 0;
    constexpr int MAX_SPIN = 1000000;
    while (((mmio_read32(port->tfd) & (ATA_DEV_BUSY | ATA_DEV_DRQ)) != 0U) && spin < MAX_SPIN) {
        spin++;
    }
    if (spin == MAX_SPIN) {
        ahci_log("flush_disk: Port is hung\n");
        if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
            port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
            port_locks[portno].unlock();
        }
        return false;
    }

    mmio_write32(port->ci, 1 << slot);

    if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
        port_locks[portno].unlock();
    }

    // Wait for completion
    while (true) {
        if ((mmio_read32(port->ci) & (1 << slot)) == 0) {
            break;
        }
        if ((mmio_read32(port->is) & HBA_PxIS_TFES) != 0U) {
            ahci_log("flush_disk: error\n");
            if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
                port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
            }
            return false;
        }
        asm volatile("pause");
    }

    if ((mmio_read32(port->is) & HBA_PxIS_TFES) != 0U) {
        ahci_log("flush_disk: error\n");
        if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
            port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
        }
        return false;
    }

    if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
        port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
    }

    return true;
}

// Wrapper for block device flush
auto ahci_flush_blocks(BlockDevice* bdev) -> int {
    if (bdev == nullptr || bdev->private_data == nullptr) {
        return -1;
    }

    auto* dev = static_cast<AHCIDevice*>(bdev->private_data);
    if (dev->port_num >= MAX_PORTS) {
        return -1;
    }

    volatile HBA_PORT* port = &hba_mem->ports[dev->port_num];
    return flush_disk(port, dev->port_num) ? 0 : -1;
}

// Wrapper for block device read
auto ahci_read_blocks(BlockDevice* bdev, uint64_t block, size_t count, void* buffer) -> int {
    if (bdev == nullptr || bdev->private_data == nullptr || buffer == nullptr) {
        return -1;
    }

    auto* dev = static_cast<AHCIDevice*>(bdev->private_data);
    if (dev->port_num >= MAX_PORTS) {
        return -1;
    }

    volatile HBA_PORT* port = &hba_mem->ports[dev->port_num];

    auto startl = static_cast<uint32_t>(block & UINT32_MAX);
    auto starth = static_cast<uint32_t>((block >> 32) & UINT32_MAX);

    bool result = read_disk(port, dev->port_num, startl, starth, static_cast<uint32_t>(count), static_cast<uint8_t*>(buffer));
    return result ? 0 : -1;
}

// Wrapper for block device write
auto ahci_write_blocks(BlockDevice* bdev, uint64_t block, size_t count, const void* buffer) -> int {
    if (bdev == nullptr || bdev->private_data == nullptr || buffer == nullptr) {
        return -1;
    }

    auto* dev = static_cast<AHCIDevice*>(bdev->private_data);
    if (dev->port_num >= MAX_PORTS) {
        return -1;
    }

    volatile HBA_PORT* port = &hba_mem->ports[dev->port_num];

    auto startl = static_cast<uint32_t>(block & UINT32_MAX);
    auto starth = static_cast<uint32_t>((block >> 32) & UINT32_MAX);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    bool result =
        write_disk(port, dev->port_num, startl, starth, static_cast<uint32_t>(count), static_cast<uint8_t*>(const_cast<void*>(buffer)));
    return result ? 0 : -1;
}

// Send ATA IDENTIFY command and return the actual total number of LBA sectors.
// Returns 0 on failure, in which case the caller should fall back to a default.
auto identify_disk(volatile HBA_PORT* port, int portno) -> uint64_t {
    uint32_t slot = UINT32_MAX;

    if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
        port_locks[portno].lock();
    }

    mmio_write32(port->is, static_cast<uint32_t>(-1));
    slot = find_cmdslot(port, portno);
    if (slot == UINT32_MAX) {
        if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
            port_locks[portno].unlock();
        }
        return 0;
    }

    if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
        port_slots_in_use[portno].fetch_or(1U << slot, std::memory_order_release);
    }

    // Allocate a 512-byte buffer for the IDENTIFY response
    auto* id_buf = new (std::nothrow) uint8_t[512];
    if (id_buf == nullptr) {
        if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
            port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
            port_locks[portno].unlock();
        }
        return 0;
    }
    std::memset(id_buf, 0, 512);

    auto* kernel_pt = ker::mod::mm::virt::getKernelPagemap();
    uint64_t buf_phys = ker::mod::mm::virt::translate(kernel_pt, reinterpret_cast<uint64_t>(id_buf));
    if (buf_phys == ker::mod::mm::virt::PADDR_INVALID) {
        ker::mod::dbg::log("ahci: failed to translate identify buffer");
        hcf();
    }

    HBA_CMD_HEADER* cmdheader = &port_memory[portno].clb_virt[slot];
    cmdheader->cfl = sizeof(FIS_REG_H2D) / sizeof(uint32_t);
    cmdheader->w = 0;
    cmdheader->prdtl = 1;

    auto* cmdtbl = reinterpret_cast<HBA_CMD_TBL*>(port_memory[portno].ctb_virt[slot]);
    std::memset(cmdtbl, 0, sizeof(HBA_CMD_TBL));

    // Single PRDT entry for 512 bytes
    cmdtbl->prdt_entry[0].dba = static_cast<uint32_t>(buf_phys & UINT32_MAX);
    cmdtbl->prdt_entry[0].dbau = static_cast<uint32_t>((buf_phys >> 32) & UINT32_MAX);
    cmdtbl->prdt_entry[0].dbc = 512 - 1;  // 512 bytes
    cmdtbl->prdt_entry[0].i = 1;

    auto* cmdfis = reinterpret_cast<FIS_REG_H2D*>(&cmdtbl->cfis);
    cmdfis->fis_type = FIS_TYPE_REG_H2D;
    cmdfis->c = 1;
    cmdfis->command = ATA_CMD_IDENTIFY;
    cmdfis->device = 0;

    // Wait for port to be ready
    int spin = 0;
    constexpr int MAX_SPIN = 1000000;
    while (((mmio_read32(port->tfd) & (ATA_DEV_BUSY | ATA_DEV_DRQ)) != 0U) && spin < MAX_SPIN) {
        spin++;
    }
    if (spin == MAX_SPIN) {
        ahci_log("identify_disk: port is hung\n");
        delete[] id_buf;
        if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
            port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
            port_locks[portno].unlock();
        }
        return 0;
    }

    mmio_write32(port->ci, 1 << slot);

    if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
        port_locks[portno].unlock();
    }

    // Wait for completion
    while (true) {
        if ((mmio_read32(port->ci) & (1 << slot)) == 0) {
            break;
        }
        if ((mmio_read32(port->is) & HBA_PxIS_TFES) != 0U) {
            ahci_log("identify_disk: error\n");
            delete[] id_buf;
            if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
                port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
            }
            return 0;
        }
        asm volatile("pause");
    }

    if ((mmio_read32(port->is) & HBA_PxIS_TFES) != 0U) {
        ahci_log("identify_disk: error after completion\n");
        delete[] id_buf;
        if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
            port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
        }
        return 0;
    }

    if (portno >= 0 && static_cast<size_t>(portno) < MAX_PORTS) {
        port_slots_in_use[portno].fetch_and(~(1U << slot), std::memory_order_release);
    }

    // Parse IDENTIFY response (array of 256 uint16_t words)
    auto* id_words = reinterpret_cast<uint16_t*>(id_buf);

    uint64_t total = 0;

    // Check if 48-bit LBA is supported (word 83, bit 10)
    if ((id_words[83] & (1 << 10)) != 0) {
        // Words 100-103: 48-bit total user addressable LBA sectors
        total = static_cast<uint64_t>(id_words[100]) | (static_cast<uint64_t>(id_words[101]) << 16) |
                (static_cast<uint64_t>(id_words[102]) << 32) | (static_cast<uint64_t>(id_words[103]) << 48);
    }

    // Fall back to 28-bit LBA (words 60-61) if 48-bit returned 0
    if (total == 0) {
        total = static_cast<uint64_t>(id_words[60]) | (static_cast<uint64_t>(id_words[61]) << 16);
    }

    delete[] id_buf;

    ahci_log("identify_disk: total sectors = 0x");
    ahci_log_hex(total);
    ahci_log("\n");

    return total;
}

// Probe all ports for devices
void probe_port(volatile HBA_MEM* abar) {
    uint32_t port_implemented = mmio_read32(abar->pi);
    int i = 0;
    while (i < 32) {
        if ((port_implemented & 1) != 0U) {
            int dt = check_type(&abar->ports[i]);
            if (dt == AHCI_DEV_SATA) {
                ahci_log("SATA drive found at port ");
                ahci_log_hex(i);
                ahci_log("\n");

                // Register device
                if (device_count < MAX_PORTS) {
                    AHCIDevice* dev = &devices[device_count];
                    dev->port_num = i;

                    // Issue ATA IDENTIFY to get real disk capacity
                    uint64_t sectors = identify_disk(&abar->ports[i], i);
                    dev->total_sectors = (sectors > 0) ? sectors : 131072;  // Fallback 64MB

                    // Set up block device
                    dev->bdev.major = 8;
                    dev->bdev.minor = device_count;

                    // Generate device name (sda, sdb, etc.)
                    dev->bdev.name[0] = 's';
                    dev->bdev.name[1] = 'd';
                    dev->bdev.name[2] = static_cast<char>('a' + device_count);
                    dev->bdev.name[3] = '\0';

                    dev->bdev.block_size = 512;
                    dev->bdev.total_blocks = dev->total_sectors;
                    dev->bdev.read_blocks = ahci_read_blocks;
                    dev->bdev.write_blocks = ahci_write_blocks;
                    dev->bdev.flush = ahci_flush_blocks;
                    dev->bdev.private_data = dev;
                    dev->bdev.remotable = &s_remotable_ops;

                    // Register with block device manager
                    block_device_register(&dev->bdev);
                    device_count++;
                }
            } else if (dt == AHCI_DEV_SATAPI) {
                ahci_log("SATAPI drive found at port ");
                ahci_log_hex(i);
                ahci_log("\n");
            } else if (dt != AHCI_DEV_NULL) {
                ahci_log("Other device found at port ");
                ahci_log_hex(i);
                ahci_log("\n");
            }
        }

        port_implemented >>= 1;
        i++;
    }
}

}  // namespace

// Main AHCI initialization
auto ahci_init() -> int {
    if (hba_mem == nullptr) {
        ahci_log("ahci_init: HBA memory not set\n");
        return -1;
    }

    ahci_log("ahci_init: Initializing AHCI driver\n");

    // Enable AHCI mode and interrupts
    mmio_or32(hba_mem->ghc, HBA_GHC_AE);
    mmio_or32(hba_mem->ghc, HBA_GHC_IE);

    ahci_log("ahci_init: GHC = 0x");
    ahci_log_hex(mmio_read32(hba_mem->ghc));
    ahci_log("\n");

    // Probe and initialize ports
    uint32_t port_implemented = mmio_read32(hba_mem->pi);
    for (size_t i = 0; i < MAX_PORTS; i++) {
        if (((port_implemented & (1 << i)) != 0U)) {
            ahci_log("ahci_init: Rebasing port ");
            ahci_log_hex(i);
            ahci_log("\n");
            port_rebase(&hba_mem->ports[i], i);
        }
    }

    // Probe for devices
    probe_port(hba_mem);

    return 0;
}

// Set AHCI base address
auto ahci_set_base(volatile uint32_t* base) -> void {
    hba_mem = reinterpret_cast<volatile HBA_MEM*>(base);
    ahci_log("ahci_set_base: AHCI base = 0x");
    ahci_log_hex(reinterpret_cast<uint64_t>(base));
    ahci_log("\n");
}

extern "C" char __kernel_end[];  // NOLINT

// Initializes AHCI controller by discovering and setting up the AHCI device
auto ahci_controller_init() -> void {
    ker::mod::dbg::log("Initializing AHCI controller");

    // Discover AHCI controller via PCI
    ker::dev::pci::PCIDevice* ahci_dev = ker::dev::pci::pci_find_ahci_controller();
    if (ahci_dev != nullptr) {
        ker::mod::dbg::log("AHCI controller found, setting up...");
        // Extract MMIO base from BAR5
        uint32_t bar5 = ahci_dev->bar[5];  // NOLINT
        // BAR5 for AHCI is a memory-mapped I/O base address
        if (bar5 != 0 && bar5 != UINT32_MAX) {
            // Place MMIO after the kernel image (linker-provided end + 2MB guard gap, page-aligned)
            const auto KERNEL_END = reinterpret_cast<uint64_t>(__kernel_end);
            constexpr uint64_t MMIO_GUARD_GAP = 0x200000;  // 2 MB
            const uint64_t ahci_kernel_vaddr = (KERNEL_END + MMIO_GUARD_GAP + 0xFFF) & ~0xFFFULL;
            const uint64_t ahci_size = 0x2000;  // 2 pages

            ker::mod::dbg::log("Mapping AHCI MMIO from physical 0x%x to virtual 0x%x", bar5, ahci_kernel_vaddr);

            // Map the MMIO region to kernel space
            for (uint64_t offset = 0; offset < ahci_size; offset += mod::mm::paging::PAGE_SIZE) {
                ker::mod::mm::virt::mapToKernelPageTable(ahci_kernel_vaddr + offset, bar5 + offset, ker::mod::mm::paging::pageTypes::MMIO);
            }

            volatile uint32_t* ahci_base = (volatile uint32_t*)ahci_kernel_vaddr;
            ahci_set_base(ahci_base);

            // Now initialize AHCI
            ahci_init();
        } else {
            ker::mod::dbg::log("Invalid AHCI BAR5 address: 0x%x", bar5);
        }
    } else {
        ker::mod::dbg::log("No AHCI controller found on PCI");
    }
}

}  // namespace ker::dev::ahci
