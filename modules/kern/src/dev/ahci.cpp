/**
 * AHCI Driver - Complete rewrite following OSDev Wiki specification
 * Reference: https://wiki.osdev.org/AHCI
 */

#include "ahci.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/ahci_dma.hpp>
#include <dev/ahci_lifecycle.hpp>
#ifdef WOS_ASYNC_BLOCK_IO
#include <dev/ahci_async.hpp>
#include <dev/block_request.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/sys/spinlock.hpp>
#endif
#include <dev/dma/dma.hpp>
#include <net/wki/remotable.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/usercopy.hpp>
#include <span>
#include <utility>

#include "block_device.hpp"
#include "pci.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/task.hpp"
#include "platform/sys/mutex.hpp"
#include "util/hcf.hpp"
#ifdef AHCI_BENCH
#include "platform/tsc/tsc.hpp"
#endif

namespace ker::dev::ahci {

using log = ker::mod::dbg::logger<"ahci">;

// AHCI Constants
constexpr uint32_t HBA_PX_CMD_ST = 0x0001;
constexpr uint32_t HBA_PX_CMD_FRE = 0x0010;
constexpr uint32_t HBA_PX_CMD_CR = 0x8000;
constexpr uint8_t ATA_DEV_BUSY = 0x80;
constexpr uint8_t ATA_DEV_DRQ = 0x08;
constexpr uint8_t ATA_CMD_READ_DMA_EX = 0x25;
constexpr uint8_t ATA_CMD_WRITE_DMA_EX = 0x35;
constexpr uint8_t ATA_CMD_FLUSH_CACHE_EXT = 0xEA;
constexpr uint8_t ATA_CMD_IDENTIFY = 0xEC;

constexpr uint32_t SATA_SIG_ATAPI = 0xEB140101;
constexpr uint32_t SATA_SIG_SEMB = 0xC33C0101;
constexpr uint32_t SATA_SIG_PM = 0x96690101;

constexpr uint8_t HBA_PORT_IPM_ACTIVE = 1;
constexpr uint8_t HBA_PORT_DET_PRESENT = 3;

// Bit masks

constexpr uint32_t HBA_GHC_AE = 0x80000000;  // AHCI Enable
constexpr uint32_t HBA_GHC_IE = 0x00000002;  // Interrupt Enable
constexpr uint64_t AHCI_WAIT_YIELD_INTERVAL = 4096;

// Global state
namespace {
enum class AhciDeviceType : uint8_t { NONE = 0, SATA = 1, SEMB = 2, PM = 3, SATAPI = 4 };

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
    log::trace("remote attach from 0x%04x", node_id);
    return 0;
}
void remotable_on_detach(uint16_t node_id) { log::trace("remote detach from 0x%04x", node_id); }
void remotable_on_fault(uint16_t node_id) { log::trace("remote fault for 0x%04x", node_id); }
const ker::net::wki::RemotableOps S_REMOTABLE_OPS = {
    .can_remote = remotable_can_remote,
    .can_share = remotable_can_share,
    .can_passthrough = remotable_can_passthrough,
    .on_remote_attach = remotable_on_attach,
    .on_remote_detach = remotable_on_detach,
    .on_remote_fault = remotable_on_fault,
};

volatile HbaMem* hba_mem = nullptr;
constexpr size_t MAX_PORTS = HBA_MAX_PORTS;
constexpr size_t COMMAND_SLOT_COUNT = 32;
constexpr size_t CLB_SIZE = 1024;
constexpr size_t FIS_RECEIVE_SIZE = 256;
constexpr uint32_t SECTOR_SIZE = 512;
constexpr size_t COMMAND_TABLE_PRDT_ENTRY_COUNT = 128;
constexpr uint32_t AHCI_MAX_SECTORS_PER_CMD = 1024;
constexpr size_t TRANSFER_CAPACITY = AHCI_MAX_SECTORS_PER_CMD * SECTOR_SIZE;
constexpr uint64_t ENGINE_TIMEOUT_NS = 1000000000ULL;
constexpr uint64_t COMMAND_TIMEOUT_NS = 30000000000ULL;
constexpr uint64_t MAX_WAIT_ITERATIONS = 100000000ULL;
pci::PCIDevice* controller = nullptr;
uint64_t dma_mask = UINT32_MAX;
std::atomic<bool> controller_failed{false};
lifecycle::Admission admission;
mod::sys::Mutex lifecycle_lock;
bool shutdown_attempted = false;  // lifecycle_lock; terminal, never reopens old DMA addresses.
int shutdown_result = 0;
#ifdef WOS_SELFTEST
size_t probe_allocation_budget = SIZE_MAX;
SelftestRetirementFault retirement_fault = SelftestRetirementFault::none;
#endif
std::array<AHCIDevice, MAX_PORTS> devices{};
size_t device_count = 0;

struct CoherentRegion {
    dma::Allocation allocation{};
    dma::Submission submission{};
};
struct PortMemory {
    HbaCmdHeader* clb_virt = nullptr;
    uint8_t* fb_virt = nullptr;
    std::array<uint8_t*, COMMAND_SLOT_COUNT> ctb_virt{};
    CoherentRegion clb{}, fis{};
    std::array<CoherentRegion, COMMAND_SLOT_COUNT> tables{};
    dma::Allocation transfer{};
#ifdef WOS_ASYNC_BLOCK_IO
    std::array<dma::Allocation, async_rules::MAX_SLOTS - 1> extra_transfers{};
    std::array<dma::Submission, async_rules::MAX_SLOTS> async_submissions{};
    std::array<BlockRequest*, async_rules::MAX_SLOTS> requests{};
    async_rules::Port async{};
    uint32_t pending_issue_mask{};  // Slot guard; retained preparing slots, no hardware doorbell yet.
    bool health_check_due = true;   // Slot guard; every reserve attempt obligates the following full health snapshot.
    mod::sys::Spinlock slot_guard{};
    std::atomic<uint32_t> interrupt_status{};
    BlockDevice* provider = nullptr;
    bool native_enabled = false;
    bool ncq_supported = false;
    bool flush_supported = false;
    std::array<uint16_t, 10> identify_serial{};
    std::atomic<bool> queue_reset_pending{false};
    AhciAsyncSnapshot stats{};
#ifdef WOS_SELFTEST
    SelftestAsyncFault injected_fault = SelftestAsyncFault::none;
#endif
#endif
    bool prepared = false;
    bool failed = false;  // Serialized by this port's command mutex after probe.
};

std::array<PortMemory, MAX_PORTS> port_memory{};

// Per-port locks serialize non-NCQ ATA commands. READ_DMA_EXT and
// WRITE_DMA_EXT are ordinary DMA commands; do not issue multiple commands to a
// SATA port at once unless this driver grows NCQ support.
std::array<ker::mod::sys::Mutex, MAX_PORTS> port_locks{};

// Software-level tracking of which command slots are in use
// This prevents the race where we release the lock but hardware hasn't updated ci yet
std::array<std::atomic<uint32_t>, MAX_PORTS> port_slots_in_use{};
auto execute_command_locked(volatile HbaPort* port, int portno, uint8_t command, uint64_t lba, uint32_t sectors,
                            std::span<std::byte> buffer, bool write_op) -> bool;
#ifdef WOS_ASYNC_BLOCK_IO
uint8_t native_irq_vector = 0;
constexpr uint32_t NATIVE_IRQ_MASK = dma_rules::ERROR_INTERRUPTS | (1U << 6) | (1U << 22) | (1U << 3) | 1U;
void ahci_async_service(BlockDevice* bdev);
auto ahci_async_poll_active(BlockDevice* bdev) -> bool;
auto ahci_async_submit(BlockDevice* bdev, BlockRequest& request) -> int;
void ahci_async_cancel(BlockDevice* bdev, BlockRequestToken token, int reason);
auto native_ops = BlockAsyncOps{ahci_async_submit, ahci_async_service, ahci_async_cancel, ahci_async_poll_active};
auto slot_transfer(PortMemory& memory, size_t index) -> dma::Allocation& {
    return index == 0 ? memory.transfer : memory.extra_transfers[index - 1];
}
#endif

constexpr auto command_table_size(size_t prdt_entries) -> size_t { return sizeof(HbaCmdTbl) + (prdt_entries * sizeof(HbaPrdtEntry)); }

auto command_table_prdt_entries(HbaCmdTbl* table) -> HbaPrdtEntry* {
    auto* bytes = reinterpret_cast<uint8_t*>(table);
    return reinterpret_cast<HbaPrdtEntry*>(bytes + sizeof(HbaCmdTbl));
}

auto valid_port(int portno) -> bool { return portno >= 0 && std::cmp_less(portno, MAX_PORTS); }
auto port_index(int portno) -> size_t { return static_cast<size_t>(portno); }

void clear_port_errors(volatile HbaPort* port) {
    mmio_write32(port->is, UINT32_MAX);
    mmio_write32(port->serr, UINT32_MAX);
}

void reset_command_header(HbaCmdHeader* cmdheader) {
    cmdheader->a = 0;
    cmdheader->p = 0;
    cmdheader->r = 0;
    cmdheader->b = 0;
    cmdheader->c = 0;
    cmdheader->pmp = 0;
    cmdheader->prdbc = 0;
}

void log_command_failure(volatile HbaPort* port, int portno, uint32_t slot, bool write_op, const char* reason, uint32_t starth,
                         uint32_t startl, uint32_t count) {
    log::warn("%s: port=%d slot=%u op=%s lba=%u:%u count=%u ci=0x%x is=0x%x tfd=0x%x serr=0x%x sact=0x%x cmd=0x%x ssts=0x%x", reason,
              portno, slot, write_op ? "WRITE" : "READ", starth, startl, count, mmio_read32(port->ci), mmio_read32(port->is),
              mmio_read32(port->tfd), mmio_read32(port->serr), mmio_read32(port->sact), mmio_read32(port->cmd), mmio_read32(port->ssts));
}

void wait_for_completion_relax(uint64_t iteration) {
    if ((iteration & (AHCI_WAIT_YIELD_INTERVAL - 1)) == 0 && ker::mod::mm::phys::can_wait_for_reclaim()) {
        ker::mod::sched::kern_yield();
        return;
    }
    asm volatile("pause");
}

template <typename Predicate>
auto wait_until(Predicate ready, uint64_t timeout_ns) -> bool {
    uint64_t const BEGIN = mod::time::get_monotonic_ns();
    for (uint64_t i = 0; i < MAX_WAIT_ITERATIONS; ++i) {
        if (ready()) return true;
        if (mod::time::get_monotonic_ns() - BEGIN >= timeout_ns) return false;
        wait_for_completion_relax(i);
    }
    return false;
}

auto stop_cmd(volatile HbaPort* port) -> bool {
    struct Port {
        volatile HbaPort* registers;
        void clear_command(uint32_t mask) { mmio_and32(registers->cmd, ~mask); }
        auto wait_clear(uint32_t mask) -> bool {
            return wait_until([&] { return (mmio_read32(registers->cmd) & mask) == 0; }, ENGINE_TIMEOUT_NS);
        }
    } io{port};
    return dma_rules::stop(io);
}

auto start_cmd(volatile HbaPort* port) -> bool {
    if ((mmio_read32(port->cmd) & (HBA_PX_CMD_ST | HBA_PX_CMD_CR)) != 0) return false;
    mmio_or32(port->cmd, HBA_PX_CMD_FRE);
    if (!wait_until([&] { return (mmio_read32(port->tfd) & (ATA_DEV_BUSY | ATA_DEV_DRQ)) == 0; }, ENGINE_TIMEOUT_NS)) return false;
    if ((mmio_read32(port->ssts) & 0xfU) != HBA_PORT_DET_PRESENT) return false;
    mmio_or32(port->cmd, HBA_PX_CMD_ST);
    return (mmio_read32(port->cmd) & HBA_PX_CMD_ST) != 0;
}

void quarantine_port(size_t index) {
    auto& memory = port_memory[index];
    memory.failed = true;
    auto pin = [](dma::Handle handle) {
        if (handle.registry != nullptr) (void)dma::quarantine(handle);
    };
    pin(memory.clb.allocation.handle);
    pin(memory.fis.allocation.handle);
    for (auto const& table : memory.tables) pin(table.allocation.handle);
    pin(memory.transfer.handle);
#ifdef WOS_ASYNC_BLOCK_IO
    for (auto const& pool : memory.extra_transfers) pin(pool.handle);
#endif
}

void fail_stop(size_t index) {
    controller_failed.store(true, std::memory_order_release);
    bool const DISABLED = pci::pci_set_bus_master(controller, false);
    quarantine_port(index);
    log::error("port %zu failed to stop; BME disabled=%u, backing quarantined", index, static_cast<unsigned>(DISABLED));
}

// The port mutex excludes completion readers; this polled driver has no IRQ
// handler and keeps both GHC.IE and PxIE masked. A timeout is not retirement.
auto recover_port_after_error(volatile HbaPort* port, size_t index, dma::Submission transfer) -> bool {
    mmio_write32(port->ie, 0);
    if (!stop_cmd(port)) {
        fail_stop(index);
        return false;
    }
    if (transfer.sequence != 0 && dma::complete(transfer, 0, dma::Retirement::device_quiesced) != dma::Error::none) {
        quarantine_port(index);
        return false;
    }
    clear_port_errors(port);
    // Clearing ST resets CI/SACT. Never restart a device still requiring a
    // SATA reset; leave it offline with its controller-owned rings retained.
    if (controller_failed.load(std::memory_order_acquire) || mmio_read32(port->ci) != 0 || mmio_read32(port->sact) != 0 ||
        (mmio_read32(port->tfd) & (ATA_DEV_BUSY | ATA_DEV_DRQ)) != 0 || !start_cmd(port)) {
        if (!stop_cmd(port)) fail_stop(index);
        port_memory[index].failed = true;
        return false;
    }
    return true;
}

// Check device type on a port
auto check_type(volatile HbaPort* port) -> AhciDeviceType {
    uint32_t const SSTS = mmio_read32(port->ssts);

    uint8_t const IPM = (SSTS >> 8) & 0x0F;
    uint8_t const DET = SSTS & 0x0F;

    if (DET != HBA_PORT_DET_PRESENT) {  // Check drive status
        return AhciDeviceType::NONE;
    }
    if (IPM != HBA_PORT_IPM_ACTIVE) {
        return AhciDeviceType::NONE;
    }

    switch (mmio_read32(port->sig)) {
        case SATA_SIG_ATAPI:
            return AhciDeviceType::SATAPI;
        case SATA_SIG_SEMB:
            return AhciDeviceType::SEMB;
        case SATA_SIG_PM:
            return AhciDeviceType::PM;
        default:
            return AhciDeviceType::SATA;
    }
}

auto allocate_region(CoherentRegion& region, size_t bytes) -> bool {
#ifdef WOS_SELFTEST
    if (probe_allocation_budget == 0) return false;
    if (probe_allocation_budget != SIZE_MAX) --probe_allocation_budget;
#endif
    auto result = dma::allocate_coherent(controller, bytes, 4096, dma_mask);
    region.allocation = result.allocation;
    if (result.error != dma::Error::none) return false;
    auto submission = dma::prepare(region.allocation.handle);
    region.submission = submission.submission;
    return submission.error == dma::Error::none;
}

// Probe-only: all engines are stopped and BME is disabled. Nothing here can
// expose a buffer to hardware before every port has been prepared successfully.
auto port_prepare(size_t portno) -> bool {
    auto& memory = port_memory[portno];
    if (!allocate_region(memory.clb, CLB_SIZE) || !allocate_region(memory.fis, FIS_RECEIVE_SIZE)) return false;
    memory.clb_virt = reinterpret_cast<HbaCmdHeader*>(memory.clb.allocation.cpu);
    memory.fb_virt = reinterpret_cast<uint8_t*>(memory.fis.allocation.cpu);
    for (size_t i = 0; i < COMMAND_SLOT_COUNT; ++i) {
        if (!allocate_region(memory.tables[i], command_table_size(COMMAND_TABLE_PRDT_ENTRY_COUNT))) return false;
        memory.ctb_virt[i] = reinterpret_cast<uint8_t*>(memory.tables[i].allocation.cpu);
        auto const ADDRESS = memory.tables[i].submission.address;
        memory.clb_virt[i].ctba = static_cast<uint32_t>(ADDRESS);
        memory.clb_virt[i].ctbau = static_cast<uint32_t>(ADDRESS >> 32);
    }
    auto pool = dma::allocate_streaming(controller, TRANSFER_CAPACITY, dma::Direction::bidirectional, dma_mask);
    memory.transfer = pool.allocation;
    if (pool.error != dma::Error::none) return false;
#ifdef WOS_ASYNC_BLOCK_IO
    for (auto& extra : memory.extra_transfers) {
        auto result = dma::allocate_streaming(controller, TRANSFER_CAPACITY, dma::Direction::bidirectional, dma_mask);
        extra = result.allocation;
        if (result.error != dma::Error::none) return false;
    }
#endif
    memory.prepared = true;
    return true;
}

// All engines stopped, BME disabled, and either prepublication or every port
// mutex held with command admission closed. No IRQ handler can retain a view.
auto release_quiesced_regions() -> bool {
    bool released = true;
    auto release_region = [&](CoherentRegion& region) {
        if (region.allocation.handle.registry == nullptr) return;
        if (region.submission.sequence != 0 && dma::complete(region.submission, 0, dma::Retirement::device_quiesced) != dma::Error::none) {
            released = false;
            return;
        }
        region.submission = {};
        if (dma::release(region.allocation.handle) != dma::Error::none)
            released = false;
        else
            region = {};
    };
    for (auto& memory : port_memory) {
        release_region(memory.clb);
        release_region(memory.fis);
        for (auto& table : memory.tables) release_region(table);
        if (memory.transfer.handle.registry != nullptr) {
            if (dma::release(memory.transfer.handle) != dma::Error::none)
                released = false;
            else
                memory.transfer = {};
        }
#ifdef WOS_ASYNC_BLOCK_IO
        for (auto& pool : memory.extra_transfers) {
            if (pool.handle.registry == nullptr) continue;
            if (dma::release(pool.handle) != dma::Error::none)
                released = false;
            else
                pool = {};
        }
#endif
        memory.clb_virt = nullptr;
        memory.fb_virt = nullptr;
        memory.ctb_virt.fill(nullptr);
        memory.failed = true;
        memory.prepared = false;
    }
    return released;
}

void port_install(volatile HbaPort* port, size_t portno) {
    auto const& memory = port_memory[portno];
    mmio_write32(port->clb, static_cast<uint32_t>(memory.clb.submission.address));
    mmio_write32(port->clbu, static_cast<uint32_t>(memory.clb.submission.address >> 32));
    mmio_write32(port->fb, static_cast<uint32_t>(memory.fis.submission.address));
    mmio_write32(port->fbu, static_cast<uint32_t>(memory.fis.submission.address >> 32));
    mmio_write32(port->ie, 0);
    clear_port_errors(port);
}

// Find a free command slot (must be called with port lock held)
auto find_cmdslot(volatile HbaPort* port, int portno) -> uint32_t {
    // Check both hardware registers AND software tracking for busy slots
    // The software tracking handles the race where we've issued a command but hardware hasn't updated ci yet
    uint32_t const HW_SLOTS = (mmio_read32(port->sact) | mmio_read32(port->ci));
    uint32_t const SW_SLOTS = valid_port(portno) ? port_slots_in_use.at(port_index(portno)).load(std::memory_order_acquire) : 0;
    uint32_t slots = HW_SLOTS | SW_SLOTS;
    uint32_t const CMDSLOTS = ((mmio_read32(hba_mem->cap) & 0x1F00) >> 8) + 1;  // Number of command slots
    for (uint32_t i = 0; i < CMDSLOTS; i++) {
        if ((slots & 1) == 0) {
            return i;
        }
        slots >>= 1;
    }
    ahci_log("Cannot find free command list entry\n");
    return UINT32_MAX;
}

#ifdef WOS_ASYNC_BLOCK_IO
struct NativeCompletion {
    BlockRequestToken token{};
    int status{};
    size_t bytes{};
};

auto native_memory(BlockDevice* bdev) -> PortMemory* {
    if (bdev == nullptr || bdev->private_data == nullptr) return nullptr;
    auto* dev = static_cast<AHCIDevice*>(bdev->private_data);
    if (dev->port_num >= MAX_PORTS) return nullptr;
    auto& memory = port_memory[dev->port_num];
    return memory.provider == bdev && memory.native_enabled ? &memory : nullptr;
}

// SG is retained kernel storage; DMA sees only the driver's exclusive pool.
auto native_validate(const BlockRequest& request, const AHCIDevice& device) -> int {
    auto const& io = request.description;
    if (io.op == BlockRequestOperation::FLUSH)
        return io.lba == 0 && io.count == 0 && io.bytes == 0 && request.segment_count == 0 ? 0 : -EINVAL;
    if ((io.op != BlockRequestOperation::READ && io.op != BlockRequestOperation::WRITE) || io.count == 0 ||
        io.count > AHCI_MAX_SECTORS_PER_CMD || io.lba >= (uint64_t{1} << 48) || io.count > (uint64_t{1} << 48) - io.lba ||
        io.lba > device.total_sectors || io.count > device.total_sectors - io.lba || io.bytes != io.count * SECTOR_SIZE ||
        request.segment_count == 0 || request.segment_count > request.segments.size())
        return -EINVAL;
    size_t total = 0;
    for (size_t i = 0; i < request.segment_count; ++i) {
        auto const& sg = request.segments[i];
        auto const ADDRESS = reinterpret_cast<uint64_t>(sg.data);
        if (ADDRESS < mod::mm::addr::get_hhdm_offset() || sg.size == 0 || sg.size > io.bytes - total || sg.size - 1 > UINT64_MAX - ADDRESS)
            return -EINVAL;
        total += sg.size;
    }
    return total == io.bytes ? 0 : -EINVAL;
}

void native_copy(BlockRequest& request, std::byte* pool, bool to_pool) {
    size_t offset = 0;
    for (size_t i = 0; i < request.segment_count; ++i) {
        auto const& sg = request.segments[i];
        if (to_pool)
            std::memcpy(pool + offset, sg.data, sg.size);
        else
            std::memcpy(sg.data, pool + offset, sg.size);
        offset += sg.size;
    }
}

void native_build(PortMemory& memory, size_t index, const BlockRequest& request, dma::Submission transfer, bool ncq) {
    auto* header = &memory.clb_virt[index];
    auto* table = reinterpret_cast<HbaCmdTbl*>(memory.ctb_virt[index]);
    size_t const BYTES = request.description.bytes;
    size_t const PRDS = (BYTES + 4095) / 4096;
    std::memset(table, 0, command_table_size(PRDS));
    reset_command_header(header);
    header->cfl = sizeof(FisRegH2D) / sizeof(uint32_t);
    header->w = request.description.op == BlockRequestOperation::WRITE;
    header->prdtl = static_cast<uint16_t>(PRDS);
    header->prdbc = 0;
    auto* prds = command_table_prdt_entries(table);
    for (size_t i = 0; i < PRDS; ++i) {
        uint64_t const ADDRESS = transfer.address + i * 4096;
        prds[i].dba = static_cast<uint32_t>(ADDRESS);
        prds[i].dbau = static_cast<uint32_t>(ADDRESS >> 32);
        prds[i].dbc = static_cast<uint32_t>(std::min(size_t{4096}, BYTES - i * 4096) - 1);
    }
    auto* fis = reinterpret_cast<FisRegH2D*>(table->cfis);
    fis->fis_type = FIS_TYPE_REG_H2D;
    fis->c = 1;
    bool const WRITE = request.description.op == BlockRequestOperation::WRITE;
    fis->command = BYTES == 0 ? ATA_CMD_FLUSH_CACHE_EXT
                   : ncq      ? (WRITE ? 0x61 : 0x60)
                              : (WRITE ? ATA_CMD_WRITE_DMA_EX : ATA_CMD_READ_DMA_EX);
    fis->device = 1U << 6;
    auto const LBA = request.description.lba;
    fis->lba0 = static_cast<uint8_t>(LBA);
    fis->lba1 = static_cast<uint8_t>(LBA >> 8);
    fis->lba2 = static_cast<uint8_t>(LBA >> 16);
    fis->lba3 = static_cast<uint8_t>(LBA >> 24);
    fis->lba4 = static_cast<uint8_t>(LBA >> 32);
    fis->lba5 = static_cast<uint8_t>(LBA >> 40);
    if (ncq) {
        fis->featurel = static_cast<uint8_t>(request.description.count);
        fis->featureh = static_cast<uint8_t>(request.description.count >> 8);
        fis->countl = static_cast<uint8_t>(index << 3);
    } else {
        fis->countl = static_cast<uint8_t>(request.description.count);
        fis->counth = static_cast<uint8_t>(request.description.count >> 8);
    }
}

// Called with the port mutex and short slot guard held. Count actual MMIO
// active bits separately from retained software ownership, without allocation.
void native_observe_hardware(PortMemory& memory, uint32_t ci, uint32_t sact) {
    size_t active = 0;
    uint32_t const BITS = ci | sact;
    for (size_t i = 0; i < memory.ctb_virt.size(); ++i) {
        if ((BITS & (uint32_t{1} << i)) != 0) ++active;
    }
    memory.stats.current_ci = ci;
    memory.stats.current_sact = sact;
    memory.stats.hardware_active = active;
    memory.stats.hardware_high_water = std::max(memory.stats.hardware_high_water, active);
}

auto ahci_async_submit(BlockDevice* bdev, BlockRequest& request) -> int {
    auto* memory_ptr = native_memory(bdev);
    if (memory_ptr == nullptr) return -ENODEV;
    auto* device = static_cast<AHCIDevice*>(bdev->private_data);
    auto& memory = *memory_ptr;
    size_t const PORT = device->port_num;
    int const VALID = native_validate(request, *device);
    if (VALID != 0) return VALID;
    if (request.description.op == BlockRequestOperation::FLUSH && !memory.flush_supported) return -EOPNOTSUPP;
    if (!port_locks[PORT].try_lock()) return -EAGAIN;
    auto unlock = [&] { port_locks[PORT].unlock(); };
    auto* port = &hba_mem->ports[PORT];
    if (!admission.allows_command() || memory.failed || controller_failed.load(std::memory_order_acquire)) {
        unlock();
        return -ENODEV;
    }
    if (memory.queue_reset_pending.load(std::memory_order_acquire)) {
        unlock();
        return -EAGAIN;
    }
    bool const NCQ = memory.ncq_supported && request.description.op != BlockRequestOperation::FLUSH;
    uint64_t flags = memory.slot_guard.lock_irqsave();
    memory.health_check_due = true;
    size_t const INDEX = async_rules::reserve(memory.async, request.token, request.description.bytes, request.deadline_us, NCQ);
    uint64_t const GENERATION = INDEX < async_rules::MAX_SLOTS ? memory.async.slots[INDEX].generation : 0;
    memory.slot_guard.unlock_irqrestore(flags);
    if (INDEX == async_rules::MAX_SLOTS) {
        bool const OFFLINE = memory.async.offline;
        unlock();
        return OFFLINE ? -EOVERFLOW : -EAGAIN;
    }
    auto& pool = slot_transfer(memory, INDEX);
    dma::Submission submission{};
    int status = 0;
    if (request.description.bytes != 0) {
        if (request.description.op == BlockRequestOperation::WRITE) native_copy(request, pool.cpu, true);
        auto const DIRECTION =
            request.description.op == BlockRequestOperation::WRITE ? dma::Direction::to_device : dma::Direction::from_device;
        if (dma::bind_streaming(pool.handle, {pool.cpu, request.description.bytes}, DIRECTION) != dma::Error::none)
            status = -EIO;
        else {
            auto const PREPARED = dma::prepare(pool.handle);
            submission = PREPARED.submission;
            if (PREPARED.error != dma::Error::none) status = -EIO;
        }
    }
    // No wait here: the task service retries transient taskfile backpressure.
    if (status == 0 && (mmio_read32(port->tfd) & (ATA_DEV_BUSY | ATA_DEV_DRQ)) != 0) status = -EAGAIN;
    if (status == 0) native_build(memory, INDEX, request, submission, NCQ);
    flags = memory.slot_guard.lock_irqsave();
    if (status == 0 && (!admission.allows_command() || memory.async.frozen || memory.async.offline ||
                        memory.async.slots[INDEX].phase != async_rules::Phase::preparing ||
                        memory.async.slots[INDEX].generation != GENERATION || memory.async.slots[INDEX].cancel_reason != 0))
        status = -ECANCELED;
    if (status == 0) {
        memory.requests[INDEX] = &request;
        memory.async_submissions[INDEX] = submission;
        memory.pending_issue_mask |= uint32_t{1} << INDEX;
        port_slots_in_use[PORT].fetch_or(uint32_t{1} << INDEX, std::memory_order_release);
        block_model_detail::increment(memory.stats.submitted);
        memory.stats.high_water = std::max(memory.stats.high_water, async_rules::owned(memory.async));
    }
    memory.slot_guard.unlock_irqrestore(flags);
    if (status != 0) {
        if (submission.sequence != 0 && dma::complete(submission, 0, dma::Retirement::never_submitted) != dma::Error::none) {
            quarantine_port(PORT);
            status = -EIO;
        }
        flags = memory.slot_guard.lock_irqsave();
        (void)async_rules::release(memory.async, INDEX, GENERATION);
        memory.slot_guard.unlock_irqrestore(flags);
    }
    unlock();
    // The runtime service owner always calls the provider again after its
    // issue loop, publishing every staged slot before releasing service_lock.
    return status;
}

void ahci_async_cancel(BlockDevice* bdev, BlockRequestToken token, int reason) {
    auto* memory = native_memory(bdev);
    if (memory == nullptr) return;
    uint64_t const FLAGS = memory->slot_guard.lock_irqsave();
    bool const CANCELED = async_rules::cancel(memory->async, token, reason);
    memory->slot_guard.unlock_irqrestore(FLAGS);
    if (CANCELED) block_queue_wake(bdev);
}

auto native_port_reset(volatile HbaPort* port) -> bool {
    // Rings are retained and every data submission has already retired. FIS
    // reception permits the post-COMRESET D2H FIS to clear taskfile busy.
    mmio_or32(port->cmd, HBA_PX_CMD_FRE);
    uint32_t const CONTROL = mmio_read32(port->sctl) & ~0xfU;
    mmio_write32(port->sctl, CONTROL | 1U);
    uint64_t const START = mod::time::get_us();
    if (!wait_until([&] { return mod::time::get_us() - START >= 1'000; }, ENGINE_TIMEOUT_NS)) return false;
    mmio_write32(port->sctl, CONTROL);
    if (!wait_until(
            [&] {
                return (mmio_read32(port->ssts) & 0xfU) == HBA_PORT_DET_PRESENT &&
                       (mmio_read32(port->tfd) & (ATA_DEV_BUSY | ATA_DEV_DRQ)) == 0;
            },
            ENGINE_TIMEOUT_NS))
        return false;
    clear_port_errors(port);
    return mmio_read32(port->ci) == 0 && mmio_read32(port->sact) == 0 && start_cmd(port);
}

auto native_revalidate(volatile HbaPort* port, size_t index) -> bool {
    auto& memory = port_memory[index];
    auto const* device = static_cast<AHCIDevice*>(memory.provider->private_data);
    std::array<uint16_t, 256> words{};
    // COMRESET's signature D2H can contain DF/diagnostic status (QEMU uses
    // 0x130). Qualify it with an exclusive real command before native admission;
    // never suppress fatal status checks for a subsequently issued request.
    if (!execute_command_locked(port, static_cast<int>(index), ATA_CMD_IDENTIFY, 0, 0, std::as_writable_bytes(std::span{words}), false))
        return false;
    uint64_t total = uint64_t{words[100]} | (uint64_t{words[101]} << 16) | (uint64_t{words[102]} << 32) | (uint64_t{words[103]} << 48);
    if ((words[83] & (1U << 10)) == 0 || total == 0) total = uint64_t{words[60]} | (uint64_t{words[61]} << 16);
    return total == device->total_sectors && words[75] == memory.stats.identify_queue_depth &&
           words[76] == memory.stats.identify_sata_caps && words[83] == memory.stats.identify_command_sets &&
           std::equal(memory.identify_serial.begin(), memory.identify_serial.end(), words.begin() + 10) &&
           (mmio_read32(port->tfd) & 0x21U) == 0 && mmio_read32(port->ci) == 0 && mmio_read32(port->sact) == 0;
}

auto ahci_async_poll_active(BlockDevice* bdev) -> bool {
    auto* memory_ptr = native_memory(bdev);
    if (memory_ptr == nullptr) return false;
    auto& memory = *memory_ptr;
    size_t const PORT = static_cast<AHCIDevice*>(bdev->private_data)->port_num;
    if (!port_locks[PORT].try_lock()) return false;
    struct Unlock {
        mod::sys::Mutex& lock;
        ~Unlock() { lock.unlock(); }
    } const UNLOCK{port_locks[PORT]};
    uint64_t const FLAGS = memory.slot_guard.lock_irqsave();
    struct Restore {
        mod::sys::Spinlock& lock;
        uint64_t flags;
        ~Restore() { lock.unlock_irqrestore(flags); }
    } const RESTORE{memory.slot_guard, FLAGS};
    if (memory.pending_issue_mask != 0 || memory.health_check_due || memory.async.frozen || memory.async.offline || memory.failed ||
        memory.queue_reset_pending.load(std::memory_order_acquire) || !admission.allows_command() ||
        controller_failed.load(std::memory_order_acquire) || memory.interrupt_status.load(std::memory_order_acquire) != 0)
        return false;
#ifdef WOS_SELFTEST
    if (memory.injected_fault != SelftestAsyncFault::none) return false;
#endif
    uint32_t owned_mask = 0;
    size_t owned_count = 0;
    for (size_t i = 0; i < memory.async.slots.size(); ++i) {
        auto const& slot = memory.async.slots[i];
        if (slot.phase == async_rules::Phase::free) continue;
        auto const* request = memory.requests[i];
        auto const& submission = memory.async_submissions[i];
        if (slot.phase != async_rules::Phase::active || !slot.ncq || slot.cancel_reason != 0 || slot.generation == 0 || slot.epoch == 0 ||
            slot.epoch != memory.async.epoch || request == nullptr || request->token != slot.token || slot.bytes == 0 ||
            request->description.bytes != slot.bytes || submission.size != slot.bytes || submission.sequence == 0)
            return false;
        owned_mask |= uint32_t{1} << i;
        ++owned_count;
    }
    // Preserve sole-command full qualification and every staged submission.
    if (owned_count < 2) return false;
    auto* port = &hba_mem->ports[PORT];
    uint32_t const CI = mmio_read32(port->ci);
    uint32_t const SACT = mmio_read32(port->sact);
    uint32_t const STATUS = mmio_read32(port->is);
    uint64_t const NOW = mod::time::get_us();
    if ((CI | SACT) != owned_mask || STATUS != 0 || memory.interrupt_status.load(std::memory_order_acquire) != 0 ||
        controller_failed.load(std::memory_order_acquire) || !admission.allows_command())
        return false;
    for (auto const& slot : memory.async.slots) {
        if (slot.phase != async_rules::Phase::free && slot.deadline_us != 0 && NOW >= slot.deadline_us) return false;
    }
    // No status acknowledgement, completion claim, DMA retirement or copy.
    // A TFD-only fault remains retained until the next mandatory full service.
    return true;
}

void ahci_async_service(BlockDevice* bdev) {
    auto* memory_ptr = native_memory(bdev);
    if (memory_ptr == nullptr) return;
    auto& memory = *memory_ptr;
    size_t const PORT = static_cast<AHCIDevice*>(bdev->private_data)->port_num;
    if (!port_locks[PORT].try_lock()) return;
    auto* port = &hba_mem->ports[PORT];
    if (memory.failed) {
        uint64_t const FLAGS = memory.slot_guard.lock_irqsave();
        bool const EMPTY = async_rules::owned(memory.async) == 0;
        memory.slot_guard.unlock_irqrestore(FLAGS);
        if (EMPTY) {
            port_locks[PORT].unlock();
            if (memory.queue_reset_pending.load(std::memory_order_acquire) && block_queue_finish_reset(bdev, false) == 0)
                memory.queue_reset_pending.store(false, std::memory_order_release);
            return;
        }
    }
    std::array<NativeCompletion, async_rules::MAX_SLOTS> completions{};
    std::array<async_rules::Slot, async_rules::MAX_SLOTS> qualified{};
    size_t count = 0;
    uint64_t flags = memory.slot_guard.lock_irqsave();
    if (async_rules::owned(memory.async) == 0 && memory.pending_issue_mask == 0 && !memory.health_check_due && !memory.async.frozen &&
        !memory.failed && !memory.queue_reset_pending.load(std::memory_order_acquire) &&
        !controller_failed.load(std::memory_order_acquire) && memory.interrupt_status.load(std::memory_order_acquire) == 0) {
        memory.slot_guard.unlock_irqrestore(flags);
        port_locks[PORT].unlock();
        return;
    }
    // Active bits precede error reads. A hardware fault that clears a command
    // after this snapshot cannot make an earlier active command look complete.
    // The same guard excludes IRQ publication/acknowledgment until every
    // eligible completion has been claimed from this qualified snapshot.
    uint32_t const CI = mmio_read32(port->ci);
    uint32_t const SACT = mmio_read32(port->sact);
    uint32_t const OBSERVED = memory.interrupt_status.exchange(0, std::memory_order_acq_rel) | mmio_read32(port->is);
    uint32_t const TFD = mmio_read32(port->tfd);
    mmio_write32(port->is, OBSERVED);
    memory.health_check_due = false;
    native_observe_hardware(memory, CI, SACT);
    if ((OBSERVED & (dma_rules::ERROR_INTERRUPTS | (1U << 6) | (1U << 22))) != 0 || (TFD & 0x21U) != 0 ||
        controller_failed.load(std::memory_order_acquire))
        async_rules::freeze(memory.async);
    if (async_rules::timeout(memory.async, mod::time::get_us())) block_model_detail::increment(memory.stats.timeouts);
#ifdef WOS_SELFTEST
    SelftestAsyncFault fault = SelftestAsyncFault::none;
    bool issued = false;
    for (auto const& slot : memory.async.slots) issued = issued || slot.phase == async_rules::Phase::active;
    if (issued) {
        fault = memory.injected_fault;
        memory.injected_fault = SelftestAsyncFault::none;
        if (fault != SelftestAsyncFault::none) {
            async_rules::freeze(memory.async, fault == SelftestAsyncFault::timeout ? -ETIMEDOUT : -EIO);
            if (fault == SelftestAsyncFault::timeout) block_model_detail::increment(memory.stats.timeouts);
        }
    }
#endif
    if (memory.pending_issue_mask != 0 &&
        (!admission.allows_command() || memory.failed || controller_failed.load(std::memory_order_acquire)))
        async_rules::freeze(memory.async, -ESHUTDOWN);
    if (!memory.async.frozen) {
        for (size_t i = 0; i < qualified.size(); ++i) {
            auto const SLOT = memory.async.slots[i];
            if (async_rules::claim(memory.async, i, SLOT.generation, SLOT.epoch, CI, SACT)) qualified[i] = SLOT;
        }
        // Root queue service prepares its bounded issue loop before calling
        // us again. Preparing slots cannot claim completion from clear CI/SACT;
        // publish them together so preparation cannot serialize hardware I/O.
        uint32_t const PENDING = memory.pending_issue_mask;
        if (PENDING != 0 && (TFD & (ATA_DEV_BUSY | ATA_DEV_DRQ)) == 0) {
            uint32_t ncq = 0;
            size_t completion_index = async_rules::MAX_SLOTS;
            bool const SOLE = (PENDING & (PENDING - 1)) == 0 && async_rules::owned(memory.async) == 1;
            for (size_t i = 0; i < memory.async.slots.size(); ++i) {
                uint32_t const BIT = uint32_t{1} << i;
                if ((PENDING & BIT) == 0) continue;
                auto const SLOT = memory.async.slots[i];
                if (memory.requests[i] == nullptr || !async_rules::publish(memory.async, i, SLOT.generation)) {
                    async_rules::freeze(memory.async);
                    break;
                }
                if (SLOT.ncq) ncq |= BIT;
                bool const FLUSH = !SLOT.ncq && SLOT.bytes == 0 && memory.requests[i]->description.op == BlockRequestOperation::FLUSH &&
                                   memory.requests[i]->description.bytes == 0 && memory.async_submissions[i].sequence == 0;
                bool const DATA = memory.requests[i]->token == SLOT.token &&
                                  (memory.requests[i]->description.op == BlockRequestOperation::READ ||
                                   memory.requests[i]->description.op == BlockRequestOperation::WRITE) &&
                                  SLOT.bytes != 0 && memory.requests[i]->description.bytes == SLOT.bytes &&
                                  memory.async_submissions[i].size == SLOT.bytes && memory.async_submissions[i].sequence != 0;
                if (SOLE && (FLUSH || DATA)) completion_index = i;
            }
            if (!admission.allows_command() || memory.failed || controller_failed.load(std::memory_order_acquire))
                async_rules::freeze(memory.async, -ESHUTDOWN);
            if (!memory.async.frozen) {
                asm volatile("mfence" ::: "memory");
                if (ncq != 0) mmio_write32(port->sact, ncq);
                mmio_write32(port->ci, PENDING);
                // NCQ occupancy lives in SACT after queue acceptance clears
                // CI. Sample it before another MMIO exit can retire the batch.
                uint32_t const PUBLISHED_SACT = mmio_read32(port->sact);
                uint32_t const PUBLISHED_CI = mmio_read32(port->ci);
                memory.pending_issue_mask &= ~PENDING;
                native_observe_hardware(memory, PUBLISHED_CI, PUBLISHED_SACT);
                if (completion_index != async_rules::MAX_SLOTS) {
                    // A sole command may finish during or after the doorbell
                    // readback. Qualify one fresh full active-before-error
                    // snapshot under the same IRQ guard before claiming it.
                    auto const SLOT = memory.async.slots[completion_index];
                    uint32_t const FRESH_CI = mmio_read32(port->ci);
                    uint32_t const FRESH_SACT = mmio_read32(port->sact);
                    uint32_t const FRESH_STATUS = memory.interrupt_status.exchange(0, std::memory_order_acq_rel) | mmio_read32(port->is);
                    uint32_t const FRESH_TFD = mmio_read32(port->tfd);
                    mmio_write32(port->is, FRESH_STATUS);
                    native_observe_hardware(memory, FRESH_CI, FRESH_SACT);
                    if ((FRESH_STATUS & (dma_rules::ERROR_INTERRUPTS | (1U << 6) | (1U << 22))) != 0 || (FRESH_TFD & 0x21U) != 0 ||
                        controller_failed.load(std::memory_order_acquire))
                        async_rules::freeze(memory.async);
                    if (async_rules::timeout(memory.async, mod::time::get_us())) block_model_detail::increment(memory.stats.timeouts);
#ifdef WOS_SELFTEST
                    fault = memory.injected_fault;
                    memory.injected_fault = SelftestAsyncFault::none;
                    if (fault != SelftestAsyncFault::none) {
                        async_rules::freeze(memory.async, fault == SelftestAsyncFault::timeout ? -ETIMEDOUT : -EIO);
                        if (fault == SelftestAsyncFault::timeout) block_model_detail::increment(memory.stats.timeouts);
                    }
#endif
                    if (!admission.allows_command() || memory.failed || memory.queue_reset_pending.load(std::memory_order_acquire) ||
                        controller_failed.load(std::memory_order_acquire))
                        async_rules::freeze(memory.async, -ESHUTDOWN);
                    if (!memory.async.frozen && FRESH_CI == 0 && FRESH_SACT == 0 &&
                        async_rules::claim(memory.async, completion_index, SLOT.generation, SLOT.epoch, FRESH_CI, FRESH_SACT))
                        qualified[completion_index] = SLOT;
                }
            }
        }
    }
    bool const FROZEN = memory.async.frozen;
    int const RESET_REASON = memory.async.reset_reason;
    memory.slot_guard.unlock_irqrestore(flags);
    if (FROZEN) {
        if (!memory.queue_reset_pending.load(std::memory_order_acquire)) {
            (void)block_queue_begin_reset(bdev, RESET_REASON == 0 ? -EIO : RESET_REASON);
            memory.queue_reset_pending.store(true, std::memory_order_release);
        }
        mmio_write32(port->ie, 0);
        bool stopped = stop_cmd(port);
#ifdef WOS_SELFTEST
        if (fault == SelftestAsyncFault::stop) stopped = false;
#endif
        bool retired = stopped;
        for (size_t i = 0; i < async_rules::MAX_SLOTS; ++i) {
            flags = memory.slot_guard.lock_irqsave();
            auto const SLOT = memory.async.slots[i];
            bool const STAGED = (memory.pending_issue_mask & (uint32_t{1} << i)) != 0;
            memory.slot_guard.unlock_irqrestore(flags);
            if (SLOT.phase == async_rules::Phase::free) continue;
            auto const SUBMISSION = memory.async_submissions[i];
            if ((STAGED || stopped) && SUBMISSION.sequence != 0 &&
                dma::complete(SUBMISSION, 0, STAGED ? dma::Retirement::never_submitted : dma::Retirement::device_quiesced) !=
                    dma::Error::none) {
                (void)dma::quarantine(slot_transfer(memory, i).handle);
                retired = false;
            }
            completions[count++] = {SLOT.token, SLOT.cancel_reason != 0 ? SLOT.cancel_reason : -EIO, 0};
            memory.requests[i] = nullptr;
            memory.async_submissions[i] = {};
            flags = memory.slot_guard.lock_irqsave();
            (void)async_rules::release(memory.async, i, SLOT.generation);
            memory.pending_issue_mask &= ~(uint32_t{1} << i);
            memory.slot_guard.unlock_irqrestore(flags);
        }
        if (!stopped) {
            fail_stop(PORT);
            block_model_detail::increment(memory.stats.quarantines);
        }
        port_slots_in_use[PORT].store(0, std::memory_order_release);
        // Join captured reset-era IRQ status after old command/DMA ownership
        // retires. A delayed IRQ now samples only the current hardware latch.
        flags = memory.slot_guard.lock_irqsave();
        memory.interrupt_status.store(0, std::memory_order_release);
        clear_port_errors(port);
        memory.slot_guard.unlock_irqrestore(flags);
        bool reset = retired && admission.allows_command() && !controller_failed.load(std::memory_order_acquire) &&
                     native_port_reset(port) && native_revalidate(port, PORT);
#ifdef WOS_SELFTEST
        if (fault == SelftestAsyncFault::reset) reset = false;
#endif
        flags = memory.slot_guard.lock_irqsave();
        // Qualify the final epoch against errors arriving after IDENTIFY as
        // well as IRQ-published errors; clearing a latch never proves success.
        uint32_t const FINAL_CI = mmio_read32(port->ci);
        uint32_t const FINAL_SACT = mmio_read32(port->sact);
        uint32_t const FINAL_STATUS = memory.interrupt_status.exchange(0, std::memory_order_acq_rel) | mmio_read32(port->is);
        uint32_t const FINAL_TFD = mmio_read32(port->tfd);
        if (reset && ((FINAL_STATUS & (dma_rules::ERROR_INTERRUPTS | (1U << 6) | (1U << 22))) != 0 || (FINAL_TFD & 0x21U) != 0 ||
                      FINAL_CI != 0 || FINAL_SACT != 0))
            reset = false;
        mmio_write32(port->is, FINAL_STATUS);
        if (!reset) memory.failed = true;
        (void)async_rules::finish_reset(memory.async, reset);
        block_model_detail::increment(memory.stats.resets);
        if (reset && native_irq_vector != 0) mmio_write32(port->ie, NATIVE_IRQ_MASK);
        memory.slot_guard.unlock_irqrestore(flags);
        if (!reset) {
            if (stopped && !stop_cmd(port)) fail_stop(PORT);
        }
        log::info("port %zu reset qualified=%u tfd=0x%x ci=0x%x sact=0x%x", PORT, static_cast<unsigned>(reset), mmio_read32(port->tfd),
                  mmio_read32(port->ci), mmio_read32(port->sact));
    } else {
        for (size_t i = 0; i < async_rules::MAX_SLOTS; ++i) {
            auto const SLOT = qualified[i];
            if (SLOT.phase == async_rules::Phase::free) continue;
            int status = 0;
            // AHCI 5.4.1: PRDBC is not valid for NCQ commands.
            if (!SLOT.ncq && memory.clb_virt[i].prdbc != SLOT.bytes) status = -EIO;
            auto const SUBMISSION = memory.async_submissions[i];
            if (SUBMISSION.sequence != 0 &&
                dma::complete(SUBMISSION, status == 0 ? SLOT.bytes : 0, dma::Retirement::command_completed) != dma::Error::none) {
                (void)dma::quarantine(slot_transfer(memory, i).handle);
                status = -EIO;
                memory.failed = true;
            }
            if (status == 0 && memory.requests[i]->description.op == BlockRequestOperation::READ)
                native_copy(*memory.requests[i], slot_transfer(memory, i).cpu, false);
            completions[count++] = {SLOT.token, status, status == 0 ? SLOT.bytes : 0};
            memory.requests[i] = nullptr;
            memory.async_submissions[i] = {};
            flags = memory.slot_guard.lock_irqsave();
            (void)async_rules::release(memory.async, i, SLOT.generation);
            memory.slot_guard.unlock_irqrestore(flags);
            port_slots_in_use[PORT].fetch_and(~(uint32_t{1} << i), std::memory_order_release);
        }
    }
    for (size_t i = 0; i < count; ++i) block_model_detail::increment(memory.stats.completed);
    bool const RESET_PENDING = memory.queue_reset_pending.load(std::memory_order_acquire);
    bool const REOPEN = !memory.failed && !controller_failed.load(std::memory_order_acquire) && admission.allows_command();
    port_locks[PORT].unlock();
    for (size_t i = 0; i < count; ++i)
        (void)block_request_complete_from_service(bdev, completions[i].token, completions[i].status, completions[i].bytes);
    if (RESET_PENDING && block_queue_finish_reset(bdev, REOPEN) == 0) memory.queue_reset_pending.store(false, std::memory_order_release);
}

void native_irq(uint8_t, void*) {
    uint32_t const PENDING = mmio_read32(hba_mem->is) & mmio_read32(hba_mem->pi);
    for (size_t i = 0; i < MAX_PORTS; ++i) {
        if ((PENDING & (uint32_t{1} << i)) == 0) continue;
        auto& memory = port_memory[i];
        uint64_t const FLAGS = memory.slot_guard.lock_irqsave();
        uint32_t const STATUS = mmio_read32(hba_mem->ports[i].is);
        memory.interrupt_status.fetch_or(STATUS, std::memory_order_release);
        mmio_write32(hba_mem->ports[i].is, STATUS);
        memory.slot_guard.unlock_irqrestore(FLAGS);
        if (memory.provider != nullptr) block_queue_wake(memory.provider);
    }
    mmio_write32(hba_mem->is, PENDING);
}

void native_interrupts_init() {
    uint8_t const VECTOR = mod::gates::allocate_vector();
    if (VECTOR == 0) return;
    if (mod::gates::request_irq(VECTOR, native_irq, nullptr, "ahci") != 0) {
        mod::gates::free_irq(VECTOR);
        return;
    }
    if (pci::pci_enable_msi(controller, VECTOR) != 0) {
        mod::gates::free_irq(VECTOR);
        return;  // Polling service remains fully functional.
    }
    native_irq_vector = VECTOR;
    for (size_t i = 0; i < MAX_PORTS; ++i) {
        if (!port_memory[i].native_enabled) continue;
        clear_port_errors(&hba_mem->ports[i]);
        mmio_write32(hba_mem->ports[i].ie, NATIVE_IRQ_MASK);
    }
    mmio_write32(hba_mem->is, UINT32_MAX);
    mmio_or32(hba_mem->ghc, HBA_GHC_IE);
}

auto native_interrupts_stop() -> bool {
    mmio_and32(hba_mem->ghc, ~HBA_GHC_IE);
    bool masked = (mmio_read32(hba_mem->ghc) & HBA_GHC_IE) == 0;
    uint32_t const PORTS = mmio_read32(hba_mem->pi);
    for (size_t i = 0; i < MAX_PORTS; ++i) {
        if ((PORTS & (uint32_t{1} << i)) == 0) continue;
        mmio_write32(hba_mem->ports[i].ie, 0);
        if (mmio_read32(hba_mem->ports[i].ie) != 0) masked = false;
    }
    if (native_irq_vector != 0 && masked) {
        uint8_t const CAP = pci::pci_find_capability(controller, pci::PCI_CAP_ID_MSI);
        if (CAP != 0) {
            auto const CONTROL = pci::pci_config_read16(controller->bus, controller->slot, controller->function, CAP + 2);
            pci::pci_config_write16(controller->bus, controller->slot, controller->function, CAP + 2, CONTROL & ~uint16_t{1});
        }
        mod::gates::free_irq(native_irq_vector);
        native_irq_vector = 0;
    }
    return masked;
}
#endif

// Caller holds this port mutex. Teardown alone bypasses closed admission for its final flush.
auto execute_command_locked(volatile HbaPort* port, int portno, uint8_t command, uint64_t lba, uint32_t sectors,
                            std::span<std::byte> buffer, bool write_op) -> bool {
    if (!valid_port(portno) || buffer.size() > TRANSFER_CAPACITY) return false;
    size_t const INDEX = port_index(portno);
    auto& memory = port_memory[INDEX];
    if (!memory.prepared || memory.failed || controller_failed.load(std::memory_order_acquire)) return false;
    auto& port_slots = port_slots_in_use[INDEX];
    clear_port_errors(port);
    uint32_t const SLOT = find_cmdslot(port, portno);
    if (SLOT == UINT32_MAX) return false;
    dma::Submission transfer{};
    auto* task = static_cast<mod::sched::task::Task*>(nullptr);
    bool const USER_BUFFER = !buffer.empty() && reinterpret_cast<uint64_t>(buffer.data()) < mod::mm::addr::get_hhdm_offset();
    auto target = buffer;
    if (USER_BUFFER) {
        if (!mod::sched::can_query_current_task()) return false;
        task = mod::sched::get_current_task();
        if (task == nullptr) return false;
        target = {memory.transfer.cpu, buffer.size()};
        // No-fault copies preserve the previous requirement of present user
        // pages and avoid recursive disk faults while holding this port lock.
        if (write_op &&
            !mod::sys::usercopy::copy_from_task_mapped(*task, reinterpret_cast<uint64_t>(buffer.data()), target.data(), target.size()))
            return false;
    }
    if (!buffer.empty()) {
        auto const DIRECTION = write_op ? dma::Direction::to_device : dma::Direction::from_device;
        if (dma::bind_streaming(memory.transfer.handle, target, DIRECTION) != dma::Error::none) return false;
        auto prepared = dma::prepare(memory.transfer.handle);
        if (prepared.error != dma::Error::none) return false;
        transfer = prepared.submission;
    }
    auto* header = &memory.clb_virt[SLOT];
    size_t const PRD_COUNT = (buffer.size() + 4095) / 4096;
    auto* table = reinterpret_cast<HbaCmdTbl*>(memory.ctb_virt[SLOT]);
    std::memset(table, 0, command_table_size(PRD_COUNT));
    auto* prdt = command_table_prdt_entries(table);
    size_t offset = 0;
    for (size_t i = 0; i < PRD_COUNT; ++i) {
        size_t const BYTES = std::min(size_t{4096}, buffer.size() - offset);
        uint64_t const ADDRESS = transfer.address + offset;
        prdt[i].dba = static_cast<uint32_t>(ADDRESS);
        prdt[i].dbau = static_cast<uint32_t>(ADDRESS >> 32);
        prdt[i].dbc = static_cast<uint32_t>(BYTES - 1);
        prdt[i].i = 0;  // Polled completion; no descriptor interrupt reader.
        offset += BYTES;
    }
    reset_command_header(header);
    header->cfl = sizeof(FisRegH2D) / sizeof(uint32_t);
    header->w = write_op ? 1 : 0;
    header->prdtl = static_cast<uint16_t>(PRD_COUNT);
    auto* fis = reinterpret_cast<FisRegH2D*>(&table->cfis);
    fis->fis_type = FIS_TYPE_REG_H2D;
    fis->c = 1;
    fis->command = command;
    fis->device = command == ATA_CMD_IDENTIFY ? 0 : 1 << 6;
    fis->lba0 = static_cast<uint8_t>(lba);
    fis->lba1 = static_cast<uint8_t>(lba >> 8);
    fis->lba2 = static_cast<uint8_t>(lba >> 16);
    fis->lba3 = static_cast<uint8_t>(lba >> 24);
    fis->lba4 = static_cast<uint8_t>(lba >> 32);
    fis->lba5 = static_cast<uint8_t>(lba >> 40);
    fis->countl = static_cast<uint8_t>(sectors);
    fis->counth = static_cast<uint8_t>(sectors >> 8);
    if (!wait_until([&] { return (mmio_read32(port->tfd) & (ATA_DEV_BUSY | ATA_DEV_DRQ)) == 0; }, ENGINE_TIMEOUT_NS)) {
        if (transfer.sequence != 0 && dma::complete(transfer, 0, dma::Retirement::never_submitted) != dma::Error::none) {
            if (!stop_cmd(port)) fail_stop(INDEX);
            quarantine_port(INDEX);
            return false;
        }
        (void)recover_port_after_error(port, INDEX, {});
        return false;
    }
    port_slots.fetch_or(1U << SLOT, std::memory_order_release);
    asm volatile("mfence" ::: "memory");  // Publish coherent command table/header before the doorbell.
#ifdef AHCI_BENCH
    uint64_t const t_issue = ker::mod::tsc::get_ns();
#endif
    mmio_write32(port->ci, 1U << SLOT);
    // These are non-NCQ commands, so the device must not see another command on this port
    // until this mutex-protected command and its DMA completion have retired.
    dma_rules::Completion result = dma_rules::Completion::pending;
    bool const FINISHED = wait_until(
        [&] {
            if (controller_failed.load(std::memory_order_acquire)) {
                result = dma_rules::Completion::error;
                return true;
            }
            result =
                dma_rules::completion(mmio_read32(port->ci), mmio_read32(port->sact), mmio_read32(port->is), mmio_read32(port->tfd), SLOT);
            return result != dma_rules::Completion::pending;
        },
        COMMAND_TIMEOUT_NS);
    asm volatile("mfence" ::: "memory");
    if (!FINISHED || result != dma_rules::Completion::success || header->prdbc != buffer.size()) {
        log_command_failure(port, portno, SLOT, write_op, "managed command failed or timed out", static_cast<uint32_t>(lba >> 32),
                            static_cast<uint32_t>(lba), sectors);
        (void)recover_port_after_error(port, INDEX, transfer);
        port_slots.fetch_and(~(1U << SLOT), std::memory_order_release);
        return false;
    }
#ifdef AHCI_BENCH
    if (sectors != 0) {
        uint64_t t_done = ker::mod::tsc::get_ns();
        static std::atomic<uint64_t> s_cmd_count{0};
        static std::atomic<uint64_t> s_total_ns{0};
        static std::atomic<uint64_t> s_total_sectors{0};
        uint64_t n = s_cmd_count.fetch_add(1, std::memory_order_relaxed);
        s_total_ns.fetch_add(t_done - t_issue, std::memory_order_relaxed);
        s_total_sectors.fetch_add(sectors, std::memory_order_relaxed);
        if ((n & 63) == 63) {
            uint64_t total_ns = s_total_ns.exchange(0, std::memory_order_relaxed);
            uint64_t total_sec = s_total_sectors.exchange(0, std::memory_order_relaxed);
            uint64_t avg_us = total_ns / (64ULL * 1000ULL);
            uint64_t mbps = total_sec == 0 ? 0 : (total_sec * SECTOR_SIZE * 1000) / (total_ns == 0 ? 1 : total_ns);
            log::debug("bench cmd#%lu: avg_latency=%luus throughput~%luMB/s (last 64 cmds, %lu sectors)", static_cast<unsigned long>(n),
                       static_cast<unsigned long>(avg_us), static_cast<unsigned long>(mbps), static_cast<unsigned long>(total_sec));
        }
    }
#endif
    bool success = true;
    if (transfer.sequence != 0) success = dma::complete(transfer, buffer.size(), dma::Retirement::command_completed) == dma::Error::none;
    port_slots.fetch_and(~(1U << SLOT), std::memory_order_release);
    if (!success) {
        if (!stop_cmd(port)) fail_stop(INDEX);
        quarantine_port(INDEX);
        return false;
    }
    if (USER_BUFFER && !write_op)
        return mod::sys::usercopy::copy_to_task_mapped(*task, reinterpret_cast<uint64_t>(buffer.data()), target.data(), target.size());
    return true;
}

// Serialized non-NCQ command path shared by data, IDENTIFY and durable flush.
auto execute_command(volatile HbaPort* port, int portno, uint8_t command, uint64_t lba, uint32_t sectors, std::span<std::byte> buffer,
                     bool write_op) -> bool {
    if (!valid_port(portno)) return false;
    auto& port_lock = port_locks[port_index(portno)];
    mod::sys::MutexGuard const GUARD(port_lock);
    if (!admission.allows_command()) return false;
    return execute_command_locked(port, portno, command, lba, sectors, buffer, write_op);
}

auto read_write_disk(volatile HbaPort* port, int portno, uint32_t startl, uint32_t starth, uint32_t count, const uint8_t* buf,
                     bool write_op) -> bool {
    if (count == 0 || count > AHCI_MAX_SECTORS_PER_CMD || buf == nullptr) return false;
    auto* cpu = reinterpret_cast<std::byte*>(const_cast<uint8_t*>(buf));
    return execute_command(port, portno, write_op ? ATA_CMD_WRITE_DMA_EX : ATA_CMD_READ_DMA_EX, (uint64_t{starth} << 32) | startl, count,
                           {cpu, size_t{count} * SECTOR_SIZE}, write_op);
}

// Read sectors from disk - splits large requests into per-command chunks
auto read_disk(volatile HbaPort* port, int portno, uint32_t startl, uint32_t starth, uint32_t count, uint8_t* buf) -> bool {
    uint32_t done = 0;
    while (done < count) {
        uint32_t chunk = count - done;
        chunk = std::min(chunk, AHCI_MAX_SECTORS_PER_CMD);
        uint64_t lba = (static_cast<uint64_t>(starth) << 32) | startl;
        lba += done;
        auto* chunk_buf = buf + (static_cast<size_t>(done) * SECTOR_SIZE);
        if (!read_write_disk(port, portno, static_cast<uint32_t>(lba & 0xFFFFFFFFU), static_cast<uint32_t>(lba >> 32), chunk, chunk_buf,
                             false)) {
            return false;
        }
        done += chunk;
    }
    return true;
}

// Write sectors to disk - splits large requests into per-command chunks
auto write_disk(volatile HbaPort* port, int portno, uint32_t startl, uint32_t starth, uint32_t count, uint8_t* buf) -> bool {
    uint32_t done = 0;
    while (done < count) {
        uint32_t chunk = count - done;
        chunk = std::min(chunk, AHCI_MAX_SECTORS_PER_CMD);
        uint64_t lba = (static_cast<uint64_t>(starth) << 32) | startl;
        lba += done;
        auto* chunk_buf = buf + (static_cast<size_t>(done) * SECTOR_SIZE);
        if (!read_write_disk(port, portno, static_cast<uint32_t>(lba & 0xFFFFFFFFU), static_cast<uint32_t>(lba >> 32), chunk, chunk_buf,
                             true)) {
            return false;
        }
        done += chunk;
    }
    return true;
}

// Only a successfully completed ATA flush establishes durability.
auto flush_disk(volatile HbaPort* port, int portno) -> bool {
    return execute_command(port, portno, ATA_CMD_FLUSH_CACHE_EXT, 0, 0, {}, false);
}

#ifdef WOS_ASYNC_BLOCK_IO
// Legacy providers report an unavailable or closed command path as I/O failure.
auto native_legacy_sync(BlockDevice* bdev, BlockRequestOperation operation, uint64_t block, size_t count, void* buffer) -> int {
    int const RESULT = block_request_sync(bdev, operation, block, count, buffer);
    return RESULT == -ENODEV || RESULT == -ESHUTDOWN ? -EIO : RESULT;
}
#endif

// Wrapper for block device flush
auto ahci_flush_blocks(BlockDevice* bdev) -> int {
    if (bdev == nullptr || bdev->private_data == nullptr) {
        return -EINVAL;
    }

    auto* dev = static_cast<AHCIDevice*>(bdev->private_data);
    if (dev->port_num >= MAX_PORTS) {
        return -EINVAL;
    }
#ifdef WOS_ASYNC_BLOCK_IO
    if (native_memory(bdev) != nullptr) return native_legacy_sync(bdev, BlockRequestOperation::FLUSH, 0, 0, nullptr);
#endif

    volatile HbaPort* port = &hba_mem->ports[dev->port_num];
    return flush_disk(port, dev->port_num) ? 0 : -EIO;
}

// Wrapper for block device read
auto ahci_read_blocks(BlockDevice* bdev, uint64_t block, size_t count, void* buffer) -> int {
    if (bdev == nullptr || bdev->private_data == nullptr || buffer == nullptr) {
        return -EINVAL;
    }

    auto* dev = static_cast<AHCIDevice*>(bdev->private_data);
    if (dev->port_num >= MAX_PORTS) {
        return -EINVAL;
    }
#ifdef WOS_ASYNC_BLOCK_IO
    if (native_memory(bdev) != nullptr) return native_legacy_sync(bdev, BlockRequestOperation::READ, block, count, buffer);
#endif

    volatile HbaPort* port = &hba_mem->ports[dev->port_num];

    auto startl = static_cast<uint32_t>(block & UINT32_MAX);
    auto starth = static_cast<uint32_t>((block >> 32) & UINT32_MAX);

    bool const RESULT = read_disk(port, dev->port_num, startl, starth, static_cast<uint32_t>(count), static_cast<uint8_t*>(buffer));
    return RESULT ? 0 : -EIO;
}

// Wrapper for block device write
auto ahci_write_blocks(BlockDevice* bdev, uint64_t block, size_t count, const void* buffer) -> int {
    if (bdev == nullptr || bdev->private_data == nullptr || buffer == nullptr) {
        return -EINVAL;
    }

    auto* dev = static_cast<AHCIDevice*>(bdev->private_data);
    if (dev->port_num >= MAX_PORTS) {
        return -EINVAL;
    }
#ifdef WOS_ASYNC_BLOCK_IO
    if (native_memory(bdev) != nullptr)
        return native_legacy_sync(bdev, BlockRequestOperation::WRITE, block, count, const_cast<void*>(buffer));
#endif

    volatile HbaPort* port = &hba_mem->ports[dev->port_num];

    auto startl = static_cast<uint32_t>(block & UINT32_MAX);
    auto starth = static_cast<uint32_t>((block >> 32) & UINT32_MAX);

    auto* write_buffer = static_cast<uint8_t*>(const_cast<void*>(buffer));  // NOLINT(cppcoreguidelines-pro-type-const-cast)
    bool const RESULT = write_disk(port, dev->port_num, startl, starth, static_cast<uint32_t>(count), write_buffer);
    return RESULT ? 0 : -EIO;
}

auto identify_disk(volatile HbaPort* port, int portno) -> uint64_t {
    std::array<uint16_t, 256> words{};
    auto buffer = std::as_writable_bytes(std::span{words});
    if (!execute_command(port, portno, ATA_CMD_IDENTIFY, 0, 0, buffer, false)) return 0;
    uint64_t total = 0;
    if ((words[83] & (1U << 10)) != 0)
        total = uint64_t{words[100]} | (uint64_t{words[101]} << 16) | (uint64_t{words[102]} << 32) | (uint64_t{words[103]} << 48);
    if (total == 0) total = uint64_t{words[60]} | (uint64_t{words[61]} << 16);
#ifdef WOS_ASYNC_BLOCK_IO
    auto& memory = port_memory[port_index(portno)];
    bool const LBA48 = (words[83] & 0xc000U) == 0x4000U && (words[83] & (1U << 10)) != 0;
    memory.flush_supported = (words[83] & 0xc000U) == 0x4000U && (words[83] & (1U << 13)) != 0;
    memory.ncq_supported =
        LBA48 && words[76] != UINT16_MAX && (words[76] & (1U << 8)) != 0 && (mmio_read32(hba_mem->cap) & (1U << 30)) != 0;
    size_t const HBA_DEPTH = ((mmio_read32(hba_mem->cap) >> 8) & 0x1fU) + 1;
    size_t const DEVICE_DEPTH = (words[75] & 0x1fU) + 1;
    memory.async.depth = memory.ncq_supported ? std::min({async_rules::MAX_SLOTS, HBA_DEPTH, DEVICE_DEPTH}) : 1;
    memory.stats.depth = memory.async.depth;
    memory.stats.ncq = memory.ncq_supported;
    memory.stats.port = static_cast<uint8_t>(portno);
    memory.stats.hba_cap = mmio_read32(hba_mem->cap);
    memory.stats.identify_queue_depth = words[75];
    memory.stats.identify_sata_caps = words[76];
    memory.stats.identify_command_sets = words[83];
    std::copy_n(words.begin() + 10, memory.identify_serial.size(), memory.identify_serial.begin());
    log::info("port %d IDENTIFY depth=%zu ncq=%u words75=0x%x words76=0x%x words83=0x%x", portno, memory.async.depth,
              static_cast<unsigned>(memory.ncq_supported), words[75], words[76], words[83]);
#endif
    return total;
}

// Probe all ports for devices
void probe_port(volatile HbaMem* abar) {
    uint32_t port_implemented = mmio_read32(abar->pi);
    size_t i = 0;
    while (i < MAX_PORTS) {
        if ((port_implemented & 1) != 0U) {
            AhciDeviceType const DT = check_type(&abar->ports[i]);
            if (DT == AhciDeviceType::SATA && port_memory[i].prepared && !port_memory[i].failed) {
                ahci_log("SATA drive found at port ");
                ahci_log_hex(i);
                ahci_log("\n");

                // Register device
                if (device_count < MAX_PORTS) {
                    AHCIDevice* dev = &devices.at(device_count);
                    dev->port_num = static_cast<uint8_t>(i);

                    // Issue ATA IDENTIFY to get real disk capacity
                    uint64_t const SECTORS = identify_disk(&abar->ports[i], static_cast<int>(i));
                    if (SECTORS == 0) {
                        port_implemented >>= 1;
                        ++i;
                        continue;  // Never publish invented capacity after a failed IDENTIFY.
                    }
                    dev->total_sectors = SECTORS;

                    // Set up block device
                    dev->bdev.major = 8;
                    dev->bdev.minor = static_cast<unsigned>(device_count);

                    // Generate device name (sda, sdb, etc.)
                    dev->bdev.name.at(0) = 's';
                    dev->bdev.name.at(1) = 'd';
                    dev->bdev.name.at(2) = static_cast<char>('a' + static_cast<int>(device_count));
                    dev->bdev.name.at(3) = '\0';

                    dev->bdev.block_size = SECTOR_SIZE;
                    dev->bdev.total_blocks = dev->total_sectors;
                    dev->bdev.read_blocks = ahci_read_blocks;
                    dev->bdev.write_blocks = ahci_write_blocks;
                    dev->bdev.flush = ahci_flush_blocks;
                    dev->bdev.private_data = dev;
                    dev->bdev.remotable = &S_REMOTABLE_OPS;

                    // Register with block device manager
                    block_device_register(&dev->bdev);
#ifdef WOS_ASYNC_BLOCK_IO
                    auto& memory = port_memory[i];
                    BlockQueueLimits limits{};
                    limits.depth = memory.async.depth;
                    limits.max_transfer_bytes = TRANSFER_CAPACITY;
                    limits.max_segments = BLOCK_MODEL_MAX_SG;
                    limits.max_requests = BLOCK_MODEL_MAX_REQUESTS;
                    limits.max_inflight_bytes = 8 * 1024 * 1024;
                    memory.provider = &dev->bdev;
                    memory.native_enabled = block_queue_configure(&dev->bdev, limits, &native_ops) == 0;
#endif
                    device_count++;
                }
            } else if (DT == AhciDeviceType::SATAPI) {
                ahci_log("SATAPI drive found at port ");
                ahci_log_hex(i);
                ahci_log("\n");
            } else if (DT != AhciDeviceType::NONE) {
                ahci_log("Other device found at port ");
                ahci_log_hex(i);
                ahci_log("\n");
            }
        }

        port_implemented >>= 1;
        i++;
    }
}

struct RetirementBackend {
    size_t locked_ports = 0;
    bool bme_disabled = false;
    bool interrupts_stopped = true;
    ~RetirementBackend() {
        while (locked_ports != 0) port_locks[--locked_ports].unlock();
    }
    void close_admission() { admission.close(); }
    auto drain_readers() -> bool {
        // A command has a 30-second completion deadline plus bounded recovery.
        constexpr uint64_t DRAIN_TIMEOUT_NS = COMMAND_TIMEOUT_NS + 4 * ENGINE_TIMEOUT_NS;
        while (locked_ports < MAX_PORTS) {
            if (!wait_until([&] { return port_locks[locked_ports].try_lock(); }, DRAIN_TIMEOUT_NS)) return false;
            ++locked_ports;
        }
#ifdef WOS_ASYNC_BLOCK_IO
        // No admitted request/CPU reader remains; mask and drain IRQ readers
        // before the final polled flush so IRQ acknowledgement cannot hide an error.
        interrupts_stopped = native_interrupts_stop();
#endif
        return true;
    }
    auto flush() -> bool {
#ifdef WOS_SELFTEST
        if (retirement_fault == SelftestRetirementFault::flush) return false;
#endif
        bool durable = true;
        for (size_t i = 0; i < device_count; ++i) {
            auto const PORT = devices[i].port_num;
            if (!execute_command_locked(&hba_mem->ports[PORT], PORT, ATA_CMD_FLUSH_CACHE_EXT, 0, 0, {}, false)) durable = false;
        }
        return durable;
    }
    auto stop_engines() -> bool {
        mmio_and32(hba_mem->ghc, ~HBA_GHC_IE);
        bool stopped = interrupts_stopped && (mmio_read32(hba_mem->ghc) & HBA_GHC_IE) == 0;
        uint32_t const PORTS = mmio_read32(hba_mem->pi);
        for (size_t i = 0; i < MAX_PORTS; ++i) {
            if ((PORTS & (1U << i)) == 0) continue;
            auto* port = &hba_mem->ports[i];
            mmio_write32(port->ie, 0);
            if (mmio_read32(port->ie) != 0) stopped = false;
#ifdef WOS_SELFTEST
            // Deliberately withhold one engine's stop proof. This exercises
            // retained live backing, not an assertion of a real hardware hang.
            if (retirement_fault == SelftestRetirementFault::stop && device_count != 0 && i == devices[0].port_num) {
                stopped = false;
                continue;
            }
#endif
            if (!stop_cmd(port)) stopped = false;
        }
        return stopped;
    }
    auto disable_bus_master() -> bool {
        controller_failed.store(true, std::memory_order_release);
        bme_disabled = pci::pci_set_bus_master(controller, false);
        return bme_disabled;
    }
    void quarantine() {
        // All port mutexes were acquired: no CPU completion reader can race
        // this transition or keep a borrowed streaming buffer alive afterward.
        for (size_t i = 0; i < MAX_PORTS; ++i) quarantine_port(i);
    }
    auto release() -> bool {
        uint32_t const PORTS = mmio_read32(hba_mem->pi);
        for (size_t i = 0; i < MAX_PORTS; ++i) {
            if ((PORTS & (1U << i)) == 0) continue;
            auto* port = &hba_mem->ports[i];
            clear_port_errors(port);
            mmio_write32(port->clb, 0);
            mmio_write32(port->clbu, 0);
            mmio_write32(port->fb, 0);
            mmio_write32(port->fbu, 0);
        }
        mmio_write32(hba_mem->is, UINT32_MAX);
        asm volatile("mfence" ::: "memory");
        return release_quiesced_regions();
    }
};

}  // namespace

auto ahci_async_snapshot(BlockDevice* bdev, AhciAsyncSnapshot& snapshot) -> bool {
#ifdef WOS_ASYNC_BLOCK_IO
    auto* memory = native_memory(bdev);
    if (memory == nullptr) return false;
    size_t const PORT = static_cast<AHCIDevice*>(bdev->private_data)->port_num;
    if (!port_locks[PORT].try_lock()) return false;
    uint64_t const FLAGS = memory->slot_guard.lock_irqsave();
    auto* port = &hba_mem->ports[PORT];
    native_observe_hardware(*memory, mmio_read32(port->ci), mmio_read32(port->sact));
    snapshot = memory->stats;
    snapshot.active = async_rules::owned(memory->async);
    snapshot.offline = memory->failed || memory->async.offline;
    memory->slot_guard.unlock_irqrestore(FLAGS);
    port_locks[PORT].unlock();
    return true;
#else
    (void)bdev;
    (void)snapshot;
    return false;
#endif
}

#ifdef WOS_SELFTEST
auto selftest_inject_async_fault(BlockDevice* bdev, SelftestAsyncFault fault) -> bool {
#ifdef WOS_ASYNC_BLOCK_IO
    auto* memory = native_memory(bdev);
    if (memory == nullptr) return false;
    uint64_t const FLAGS = memory->slot_guard.lock_irqsave();
    memory->injected_fault = fault;
    memory->slot_guard.unlock_irqrestore(FLAGS);
    block_queue_wake(bdev);
    return true;
#else
    (void)bdev;
    (void)fault;
    return false;
#endif
}
void selftest_limit_probe_allocations(size_t count) {
    if (controller == nullptr) probe_allocation_budget = count;
}
void selftest_set_retirement_fault(SelftestRetirementFault fault) {
    if (controller == nullptr) retirement_fault = fault;
}
auto selftest_retirement_fault() -> SelftestRetirementFault { return retirement_fault; }
#endif

auto ahci_shutdown() -> int {
    if (mod::sched::can_query_current_task() && !mod::mm::phys::can_wait_for_reclaim()) return -EWOULDBLOCK;
    mod::sys::MutexGuard const LIFECYCLE(lifecycle_lock);
    if (shutdown_attempted) return shutdown_result;
    shutdown_attempted = true;
    admission.close();
    if (controller == nullptr || hba_mem == nullptr) return 0;
#ifdef WOS_ASYNC_BLOCK_IO
    bool queues_drained = true;
    for (size_t i = 0; i < device_count; ++i) {
        auto* bdev = &devices[i].bdev;
        if (native_memory(bdev) == nullptr) continue;
        block_queue_close(bdev);
        if (block_queue_drain(bdev) != 0) queues_drained = false;
    }
    if (!queues_drained) {
        // Keep request and DMA ownership for a future safe service; retirement
        // cannot free rings while a provider still owns a request.
        controller_failed.store(true, std::memory_order_release);
        (void)pci::pci_set_bus_master(controller, false);
        shutdown_result = -EIO;
        return shutdown_result;
    }
#endif
    auto const BEFORE = dma::stats(controller);
    RetirementBackend backend;
    auto const RESULT = lifecycle::retire(backend);
    auto const AFTER = dma::stats(controller);
    shutdown_result = RESULT == lifecycle::Result::released ? 0 : -EIO;
    log::info("DMA retirement result=%u bme_disabled=%u regions=%zu->%zu pages=%zu->%zu quarantined=%zu", static_cast<unsigned>(RESULT),
              static_cast<unsigned>(backend.bme_disabled), BEFORE.regions, AFTER.regions, BEFORE.backing_pages, AFTER.backing_pages,
              AFTER.quarantined);
    return shutdown_result;
}

// Probe runs before publication of block providers, with no command readers.
static auto ahci_init() -> int {
    if (hba_mem == nullptr || controller == nullptr) return -1;
    mmio_or32(hba_mem->ghc, HBA_GHC_AE);
    mmio_and32(hba_mem->ghc, ~HBA_GHC_IE);  // Polled driver: never admit an IRQ reader.
    if ((mmio_read32(hba_mem->ghc) & (HBA_GHC_AE | HBA_GHC_IE)) != HBA_GHC_AE) {
        (void)pci::pci_set_bus_master(controller, false);
        return -1;
    }
    uint32_t const PORTS = mmio_read32(hba_mem->pi);
    for (size_t i = 0; i < MAX_PORTS; ++i) {
        if ((PORTS & (1U << i)) == 0) continue;
        mmio_write32(hba_mem->ports[i].ie, 0);
        if (mmio_read32(hba_mem->ports[i].ie) != 0 || !stop_cmd(&hba_mem->ports[i])) {
            controller_failed.store(true, std::memory_order_release);
            (void)pci::pci_set_bus_master(controller, false);
            log::error("inherited port %zu failed to stop; AHCI admission refused", i);
            return -1;
        }
    }
    if (!pci::pci_set_bus_master(controller, false)) return -1;
    dma_mask = (mmio_read32(hba_mem->cap) & (1U << 31)) != 0 ? UINT64_MAX : UINT32_MAX;
    auto const BEFORE = dma::stats();
    for (size_t i = 0; i < MAX_PORTS; ++i) {
        if ((PORTS & (1U << i)) != 0 && !port_prepare(i)) {
            bool const RELEASED = release_quiesced_regions();
            auto const AFTER = dma::stats();
            log::error("probe DMA unwind complete=%u regions=%zu->%zu pages=%zu->%zu", static_cast<unsigned>(RELEASED), BEFORE.regions,
                       AFTER.regions, BEFORE.backing_pages, AFTER.backing_pages);
            return -1;
        }
    }
    for (size_t i = 0; i < MAX_PORTS; ++i)
        if ((PORTS & (1U << i)) != 0) port_install(&hba_mem->ports[i], i);
    asm volatile("mfence" ::: "memory");
    if (!pci::pci_set_bus_master(controller, true)) {
        (void)release_quiesced_regions();
        return -1;
    }
    for (size_t i = 0; i < MAX_PORTS; ++i) {
        if ((PORTS & (1U << i)) != 0 && check_type(&hba_mem->ports[i]) == AhciDeviceType::SATA && !start_cmd(&hba_mem->ports[i])) {
            if (!stop_cmd(&hba_mem->ports[i])) {
                fail_stop(i);
                return -1;
            }
            port_memory[i].failed = true;
        }
    }
    auto const AFTER = dma::stats();
    log::info("managed DMA ready: regions=%zu pages=%zu mask=0x%llx; passthrough, no isolation", AFTER.regions - BEFORE.regions,
              AFTER.backing_pages - BEFORE.backing_pages, static_cast<unsigned long long>(dma_mask));
    admission.open_for_probe();
    probe_port(hba_mem);
#ifdef WOS_ASYNC_BLOCK_IO
    native_interrupts_init();
#endif
    return 0;
}

// Set AHCI base address
static auto ahci_set_base(volatile uint32_t* base) -> void {
    hba_mem = reinterpret_cast<volatile HbaMem*>(base);
    ahci_log("ahci_set_base: AHCI base = 0x");
    ahci_log_hex(reinterpret_cast<uint64_t>(base));
    ahci_log("\n");
}

// Initializes AHCI controller by discovering and setting up the AHCI device
auto ahci_controller_init() -> void {
    mod::sys::MutexGuard const LIFECYCLE(lifecycle_lock);
    if (controller != nullptr || shutdown_attempted) return;
    log::info("initializing controller");

    // Discover AHCI controller via PCI
    auto* ahci_dev = ker::dev::pci::pci_find_ahci_controller();
    if (ahci_dev != nullptr) {
        log::info("controller found, setting up");
        // Extract MMIO base from BAR5
        uint32_t const BAR5 = ahci_dev->bar.at(5);
        uint64_t const PHYSICAL = BAR5 & ~uint32_t{0xf};
        uint64_t const HHDM = mod::mm::addr::get_hhdm_offset();
        constexpr uint64_t AHCI_SIZE = 0x2000;  // 2 pages
        // Share a checked high-half alias with every task's kernel mappings;
        // randomized image-end placement can wrap into a private user half.
        if ((BAR5 & 1U) == 0 && BAR5 != UINT32_MAX && PHYSICAL != 0 && (PHYSICAL & (mod::mm::paging::PAGE_SIZE - 1)) == 0 &&
            HHDM >= 0xffff800000000000ULL && (HHDM & (mod::mm::paging::PAGE_SIZE - 1)) == 0 && PHYSICAL <= UINT64_MAX - (AHCI_SIZE - 1) &&
            HHDM <= UINT64_MAX - PHYSICAL && AHCI_SIZE - 1 <= UINT64_MAX - HHDM - PHYSICAL) {
            uint64_t const AHCI_KERNEL_VADDR = HHDM + PHYSICAL;
            // MMIO access precedes engine stop; BME admission moves into
            // ahci_init after every required managed ring/buffer mapping.
            pci::pci_enable_memory_space(ahci_dev);
            controller = ahci_dev;
            // PCIe Device Control: disable Relaxed Ordering and No Snoop for
            // the coherent, ordered DMA backend. Probe is serialized at boot.
            uint8_t const PCIE = pci::pci_find_capability(controller, 0x10);
            if (PCIE != 0) {
                if (PCIE > 0xf4) {
                    (void)pci::pci_set_bus_master(controller, false);
                    return;
                }
                auto const OFFSET = static_cast<uint8_t>(PCIE + 8);
                uint16_t const CONTROL = pci::pci_config_read16(controller->bus, controller->slot, controller->function, OFFSET);
                pci::pci_config_write16(controller->bus, controller->slot, controller->function, OFFSET,
                                        static_cast<uint16_t>(CONTROL & ~0x810U));
                if ((pci::pci_config_read16(controller->bus, controller->slot, controller->function, OFFSET) & 0x810U) != 0) {
                    (void)pci::pci_set_bus_master(controller, false);
                    log::error("cannot establish coherent ordered PCIe DMA");
                    return;
                }
            }
            log::info("mapping MMIO from physical 0x%lx to virtual 0x%lx", PHYSICAL, AHCI_KERNEL_VADDR);

            // Map the MMIO region to kernel space
            for (uint64_t offset = 0; offset < AHCI_SIZE; offset += mod::mm::paging::PAGE_SIZE) {
                ker::mod::mm::virt::map_to_kernel_page_table(AHCI_KERNEL_VADDR + offset, PHYSICAL + offset,
                                                             ker::mod::mm::virt::kernel_mmio_page_flags());
            }

            auto* ahci_base = reinterpret_cast<volatile uint32_t*>(AHCI_KERNEL_VADDR);
            ahci_set_base(ahci_base);

            // Now initialize AHCI
            ahci_init();
        } else {
            (void)pci::pci_set_bus_master(ahci_dev, false);
            log::error("invalid MMIO BAR5/HHDM range: bar=0x%x hhdm=0x%lx", BAR5, HHDM);
        }
    } else {
        log::info("no controller found on PCI");
    }
}

}  // namespace ker::dev::ahci
