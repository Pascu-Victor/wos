/**
 * AHCI Driver - Complete rewrite following OSDev Wiki specification
 * Reference: https://wiki.osdev.org/AHCI
 */

#include "ahci.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/paging.hpp>

#include "block_device.hpp"
#include "pci.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/virt.hpp"

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

// Stop command engine on a port
void stop_cmd(volatile HBA_PORT* port) {
    // Clear ST (bit0)
    port->cmd &= ~HBA_PxCMD_ST;

    // Clear FRE (bit4)
    port->cmd &= ~HBA_PxCMD_FRE;

    // Wait until FR (bit14), CR (bit15) are cleared
    while (true) {
        if ((port->cmd & HBA_PxCMD_FR) != 0U) {
            continue;
        }
        if ((port->cmd & HBA_PxCMD_CR) != 0U) {
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
    while ((port->cmd & HBA_PxCMD_CR) != 0U) {
        if (timeout++ == MAX_TIMEOUT) {
            ahci_log("start_cmd: timeout waiting for CR to clear\n");
            break;
        }
    }

    // Set FRE (bit4) and ST (bit0)
    port->cmd |= HBA_PxCMD_FRE;
    port->cmd |= HBA_PxCMD_ST;
}

// Check device type on a port
auto check_type(volatile HBA_PORT* port) -> int {
    uint32_t ssts = port->ssts;

    uint8_t ipm = (ssts >> 8) & 0x0F;
    uint8_t det = ssts & 0x0F;

    if (det != HBA_PORT_DET_PRESENT)  // Check drive status
        return AHCI_DEV_NULL;
    if (ipm != HBA_PORT_IPM_ACTIVE) {
        return AHCI_DEV_NULL;
    }

    switch (port->sig) {
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
    auto* kernel_pt =
        (ker::mod::mm::paging::PageTable*)ker::mod::mm::addr::getVirtPointer((uint64_t)ker::mod::mm::virt::getKernelPageTable());

    // Command list offset: 1K*portno
    // Command list entry size = 32 bytes
    // Command list entry maxim count = 32
    // Command list maxium size = 32*32 = 1K per port
    auto* clb_virt = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(1024));
    std::memset(clb_virt, 0, 1024);
    uint64_t clb_phys = ker::mod::mm::virt::translate(kernel_pt, reinterpret_cast<uint64_t>(clb_virt));
    port->clb = static_cast<uint32_t>(clb_phys & UINT32_MAX);
    port->clbu = static_cast<uint32_t>((clb_phys >> 32) & UINT32_MAX);

    // Store virtual address for later use
    port_memory[portno].clb_virt = reinterpret_cast<HBA_CMD_HEADER*>(clb_virt);

    // FIS offset: 32K+256*portno
    // FIS entry size = 256 bytes per port
    auto* fb_virt = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(256));
    std::memset(fb_virt, 0, 256);
    uint64_t fb_phys = ker::mod::mm::virt::translate(kernel_pt, reinterpret_cast<uint64_t>(fb_virt));
    port->fb = static_cast<uint32_t>(fb_phys & UINT32_MAX);
    port->fbu = static_cast<uint32_t>((fb_phys >> 32) & UINT32_MAX);

    // Store virtual address for later use
    port_memory[portno].fb_virt = fb_virt;

    // Command table offset: 40K + 8K*portno
    // Command table size = 256*32 = 8K per port
    auto* cmdheader = reinterpret_cast<HBA_CMD_HEADER*>(clb_virt);
    for (int i = 0; i < 32; i++) {
        cmdheader[i].prdtl = 8;  // 8 prdt entries per command table
                                 // 256 bytes per command table, 64+16+48+16*8
        // Command table offset: 40K + 8K*portno + cmdheader_index*256
        auto* ctb_virt = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(256));
        std::memset(ctb_virt, 0, 256);
        uint64_t ctb_phys = ker::mod::mm::virt::translate(kernel_pt, reinterpret_cast<uint64_t>(ctb_virt));
        cmdheader[i].ctba = static_cast<uint32_t>(ctb_phys & UINT32_MAX);
        cmdheader[i].ctbau = static_cast<uint32_t>((ctb_phys >> 32) & UINT32_MAX);

        // Store virtual address for later use
        port_memory[portno].ctb_virt[i] = ctb_virt;
    }

    start_cmd(port);  // Start command engine
}

// Find a free command slot
auto find_cmdslot(volatile HBA_PORT* port) -> uint32_t {
    // If not set in SACT and CI, the slot is free
    uint32_t slots = (port->sact | port->ci);
    uint32_t cmdslots = (hba_mem->cap & 0x1F00) >> 8;  // Number of command slots
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
    port->is = static_cast<uint32_t>(-1);  // Clear pending interrupt bits
    uint32_t slot = find_cmdslot(port);
    if (slot == UINT32_MAX) {
        return false;
    }

    // Get kernel page table for physical address translation
    auto* kernel_pt =
        (ker::mod::mm::paging::PageTable*)ker::mod::mm::addr::getVirtPointer((uint64_t)ker::mod::mm::virt::getKernelPageTable());

    // Get command header using stored virtual address
    HBA_CMD_HEADER* cmdheader = &port_memory[portno].clb_virt[slot];

    cmdheader->cfl = sizeof(FIS_REG_H2D) / sizeof(uint32_t);         // Command FIS size
    cmdheader->w = write_op ? 1 : 0;                                 // Write flag
    cmdheader->prdtl = static_cast<uint16_t>((count - 1) >> 4) + 1;  // PRDT entries count

    // Get command table using stored virtual address
    auto* cmdtbl = reinterpret_cast<HBA_CMD_TBL*>(port_memory[portno].ctb_virt[slot]);
    std::memset(cmdtbl, 0, sizeof(HBA_CMD_TBL) + ((cmdheader->prdtl - 1) * sizeof(HBA_PRDT_ENTRY)));

    // Get physical address of buffer
    uint64_t buf_phys = ker::mod::mm::virt::translate(kernel_pt, reinterpret_cast<uint64_t>(buf));

    // 8K bytes (16 sectors) per PRDT
    uint32_t remaining_sectors = count;
    size_t i = 0;
    for (; i < cmdheader->prdtl - 1; i++) {
        cmdtbl->prdt_entry[i].dba = static_cast<uint32_t>(buf_phys & UINT32_MAX);
        cmdtbl->prdt_entry[i].dbau = static_cast<uint32_t>((buf_phys >> 32) & UINT32_MAX);
        cmdtbl->prdt_entry[i].dbc = (8 * 1024) - 1;  // 8K bytes
        cmdtbl->prdt_entry[i].i = 1;
        buf_phys += 4 * 1024 * 2;  // 4K words = 8K bytes
        remaining_sectors -= 16;   // 16 sectors
    }

    // Last entry
    cmdtbl->prdt_entry[i].dba = static_cast<uint32_t>(buf_phys & UINT32_MAX);
    cmdtbl->prdt_entry[i].dbau = static_cast<uint32_t>((buf_phys >> 32) & UINT32_MAX);
    cmdtbl->prdt_entry[i].dbc = (remaining_sectors << 9) - 1;  // 512 bytes per sector
    cmdtbl->prdt_entry[i].i = 1;

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

    // Wait for port to be ready
    int spin = 0;
    constexpr int MAX_SPIN = 1000000;
    while (((port->tfd & (ATA_DEV_BUSY | ATA_DEV_DRQ)) != 0U) && spin < MAX_SPIN) {
        spin++;
    }
    if (spin == MAX_SPIN) {
        ahci_log("Port is hung\n");
        return false;
    }

    // Issue command
    port->ci = 1 << slot;

    // Wait for completion
    while (true) {
        if ((port->ci & (1 << slot)) == 0) {
            break;
        }
        if ((port->is & HBA_PxIS_TFES) != 0U) {  // Task file error
            if (write_op) {
                ahci_log("Write disk error\n");
            } else {
                ahci_log("Read disk error\n");
            }
            return false;
        }
    }

    // Check again
    if ((port->is & HBA_PxIS_TFES) != 0U) {
        if (write_op) {
            ahci_log("Write disk error\n");
        } else {
            ahci_log("Read disk error\n");
        }
        return false;
    }

    return true;
}

// Read sectors from disk
auto read_disk(volatile HBA_PORT* port, int portno, uint32_t startl, uint32_t starth, uint32_t count, uint8_t* buf) -> bool {
    return read_write_disk(port, portno, startl, starth, count, buf, false);
}

// Write sectors to disk
auto write_disk(volatile HBA_PORT* port, int portno, uint32_t startl, uint32_t starth, uint32_t count, uint8_t* buf) -> bool {
    return read_write_disk(port, portno, startl, starth, count, buf, true);
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

// Probe all ports for devices
void probe_port(volatile HBA_MEM* abar) {
    uint32_t port_implemented = abar->pi;
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
                    dev->total_sectors = 131072;  // Default 64MB

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
                    dev->bdev.flush = nullptr;
                    dev->bdev.private_data = dev;

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
    hba_mem->ghc |= HBA_GHC_AE;
    hba_mem->ghc |= HBA_GHC_IE;

    ahci_log("ahci_init: GHC = 0x");
    ahci_log_hex(hba_mem->ghc);
    ahci_log("\n");

    // Probe and initialize ports
    uint32_t port_implemented = hba_mem->pi;
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
            const uint64_t ahci_kernel_vaddr = 0xffffffff80500000ULL;
            const uint64_t ahci_size = 0x2000;  // 2 pages

            ker::mod::dbg::log("Mapping AHCI MMIO from physical 0x%x to virtual 0x%x", bar5, ahci_kernel_vaddr);

            // Map the MMIO region to kernel space
            for (uint64_t offset = 0; offset < ahci_size; offset += mod::mm::paging::PAGE_SIZE) {
                ker::mod::mm::virt::mapToKernelPageTable(ahci_kernel_vaddr + offset, bar5 + offset,
                                                         ker::mod::mm::paging::pageTypes::KERNEL);
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
