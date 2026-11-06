#pragma once

// Types/effect declarations precede complete actual source and are order dependent.
// clang-format off
#include "cpu_maintenance_ports.inc"
#include "pci_cold_fixture.hpp"
#include "e1000_mmio_consumer_ports.inc"
#include <platform/mm/mmio_mapping_platform.hpp>
#include <platform/mm/mmio_mapping_private.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/asm/cpu_safe_msr.hpp>
#include <dev/iommu/domains.hpp>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <string>
#include <sys/mman.h>
// clang-format on

namespace mmio_writer_host {
void map_page(ker::mod::mm::paging::PageTable*, uint64_t, uint64_t, uint64_t);
}

namespace mmio_host {
using Table = ker::mod::mm::paging::PageTable;
inline constexpr uint64_t FRAME = 0x000ffffffffff000ULL;
inline constexpr uint64_t BANK = 0xfffffc8000000000ULL;
inline std::recursive_mutex pin;
inline thread_local unsigned pin_depth{};
inline std::map<uint64_t, Table*> tables;
inline std::map<const Table*, uint64_t> physical;
inline Table* root{};
inline uint64_t next_table{0x1000000}, reloads{}, allocs{}, releases{}, register_reads{}, register_writes{};
inline size_t table_budget{SIZE_MAX};
inline bool building{true}, addressable{true}, nx{true}, layout{true};
inline uint64_t table_alias_flags{3ULL | (1ULL << 63)};
inline std::string command_line{"--typed-pci-mmio"};
inline std::map<void*, size_t> dma_backing;
inline size_t dma_allocs{}, dma_frees{};
inline uint64_t setup_allocs{};
inline std::vector<ker::mod::sys::Spinlock*> lock_stack;
inline ker::mod::sys::Spinlock* config_lock{};

auto allocate_table() -> Table* {
    if (table_budget == 0) return nullptr;
    if (table_budget != SIZE_MAX) --table_budget;
    void* backing = mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (backing == MAP_FAILED) return nullptr;
    auto* table = new (backing) Table{};
    auto const pa = next_table;
    next_table += 4096;
    tables.emplace(pa, table);
    physical.emplace(table, pa);
    ++allocs;
    return table;
}
auto raw(const Table* table, size_t slot) -> uint64_t {
    uint64_t result;
    std::memcpy(&result, reinterpret_cast<const uint8_t*>(table) + slot * 8, 8);
    return result;
}
auto translate(uint64_t address) -> uint64_t {
    auto* table = root;
    for (unsigned level = 4; level != 0; --level) {
        assert(table != nullptr);
        auto const entry = raw(table, (address >> (12 + 9 * (level - 1))) & 511);
        if ((entry & 1) == 0) return UINT64_MAX;
        if (level == 1 || (entry & 128) != 0) {
            auto const bytes = uint64_t{1} << (12 + 9 * (level - 1));
            return (entry & FRAME & ~(bytes - 1)) + (address & (bytes - 1));
        }
        auto found = tables.find(entry & FRAME);
        if (found == tables.end()) return UINT64_MAX;
        table = found->second;
    }
    return UINT64_MAX;
}
}  // namespace mmio_host

namespace ker::dev::pci::port_io {
void note_config_lock() {
    assert(!mmio_host::lock_stack.empty());
    if (mmio_host::config_lock == nullptr) {
        mmio_host::config_lock = mmio_host::lock_stack.back();
        assert(pci_cold_host::config_depth == 0);
        ++pci_cold_host::config_depth;
    }
    assert(mmio_host::lock_stack.back() == mmio_host::config_lock && pci_cold_host::config_depth == 1);
}
auto inl(uint16_t port) -> uint32_t {
    note_config_lock();
    return pci_cold_host::inl(port);
}
void outl(uint16_t port, uint32_t value) {
    note_config_lock();
    pci_cold_host::outl(port, value);
}
void outw(uint16_t port, uint16_t value) {
    note_config_lock();
    pci_cold_host::outw(port, value);
}
void outb(uint16_t port, uint8_t value) {
    note_config_lock();
    pci_cold_host::outb(port, value);
}
}  // namespace ker::dev::pci::port_io
namespace ker::mod::acpi {
auto find_table_bounded(std::string_view signature) -> bounded::Result { return pci_cold_host::acpi_table(signature); }
}  // namespace ker::mod::acpi
namespace ker::dev::iommu {
auto discovery() -> const Discovery* { return &pci_cold_host::iommu_inventory; }
}  // namespace ker::dev::iommu

namespace ker::mod::sys {
auto Spinlock::lock_irqsave() -> uint64_t {
    auto const flags = runtime_ports::flags;
    runtime_ports::flags &= ~runtime_ports::IF;
    mutex_.lock();
    mmio_host::lock_stack.push_back(this);
    if (this == mmio_host::config_lock) ++pci_cold_host::config_depth;
    return flags;
}
void Spinlock::unlock_irqrestore(uint64_t flags) {
    assert(!mmio_host::lock_stack.empty() && mmio_host::lock_stack.back() == this);
    if (this == mmio_host::config_lock) {
        assert(pci_cold_host::config_depth == 1);
        --pci_cold_host::config_depth;
    }
    mmio_host::lock_stack.pop_back();
    mutex_.unlock();
    runtime_ports::flags = flags;
}
}  // namespace ker::mod::sys

namespace ker::mod::cpu {
void cpuid(CpuidContext* value) {
    auto const& cpu = runtime_ports::current();
    auto const found = cpu.cpuid.find({static_cast<uint32_t>(value->function), 0});
    assert(found != cpu.cpuid.end());
    value->eax = static_cast<int>(found->second.eax);
    value->ebx = static_cast<int>(found->second.ebx);
    value->ecx = static_cast<int>(found->second.ecx);
    value->edx = static_cast<int>(found->second.edx);
}
auto try_read_msr(uint32_t index, uint64_t& output) -> bool {
    if (index == 0x1b) {
        output = 0xfee00d00;
        return true;
    }
    auto const found = runtime_ports::current().msr.find(index);
    if (found == runtime_ports::current().msr.end()) return false;
    output = found->second;
    return true;
}
}  // namespace ker::mod::cpu
void cpu_get_msr(uint32_t index, uint64_t* output) {
    assert(output != nullptr);
    auto const found = runtime_ports::current().msr.find(index);
    assert(found != runtime_ports::current().msr.end());
    *output = found->second;
}
namespace ker::mod::smt {
auto get_core_count() -> uint64_t { return runtime_ports::configured.load(); }
auto get_apic_id_for_cpu(uint64_t cpu) -> uint32_t {
    assert(cpu < runtime_ports::configured.load());
    return runtime_ports::cpus[cpu].apic;
}
}  // namespace ker::mod::smt
namespace ker::dev::iommu {
auto bus_master_allowed(const pci::PCIDevice*) -> bool { return true; }
auto domain_stats() -> DomainStats { return {}; }
auto domain_info(const pci::PCIDevice*) -> DomainInfo { return {.mode = DomainMode::software_passthrough}; }
auto map(const pci::PCIDevice*, uint64_t, size_t, vtd::Access) -> vtd::TableResult { std::abort(); }
auto unmap(const pci::PCIDevice*, uint64_t, uint64_t, size_t, vtd::Access) -> bool { std::abort(); }
}  // namespace ker::dev::iommu
namespace ker::mod::mm {
auto boot_memory_map() -> const limine_memmap_response* { return &pci_cold_host::memory_map; }
namespace addr {
auto get_hhdm_offset() -> uint64_t { return 0xffff800000000000ULL; }
// Explicit DMA-TU-only finite address port; host malloc pointers cannot inhabit
// the native upper canonical kernel half. No native arithmetic is qualified.
auto dma_host_hhdm_offset() -> uint64_t { return 0; }
auto get_virt_pointer(vaddr_t physical) -> vaddr_t* {
    auto* value = pci_cold_host::ecam_pointer(physical);
    assert(value != nullptr);
    return reinterpret_cast<vaddr_t*>(value);
}
}  // namespace addr
namespace phys {
auto page_alloc_may_fail(PhysicalPageOwner owner, uint64_t bytes, std::string_view name) -> void* {
    assert(mmio_host::pin_depth == 0 && pci_cold_host::config_depth == 0);
    assert(owner == PhysicalPageOwner::DMA_BUFFER && name == "dma_buffer" && bytes % 4096 == 0);
    auto* result = std::aligned_alloc(4096, bytes);
    if (result != nullptr) {
        std::memset(result, 0, bytes);
        mmio_host::dma_backing.emplace(result, bytes);
        ++mmio_host::dma_allocs;
    }
    return result;
}
void page_free(void* pointer) {
    assert(mmio_host::pin_depth == 0 && pci_cold_host::config_depth == 0 && mmio_host::dma_backing.erase(pointer) == 1);
    ++mmio_host::dma_frees;
    std::free(pointer);
}
}  // namespace phys
namespace virt {
auto kernel_mmio_page_flags() -> uint64_t { return 0x800000000000001bULL; }
void map_range_to_kernel_page_table(Range range, uint64_t flags) {
    assert(pci_cold_host::config_depth == 0 && range.start < range.end);
    assert(mmio::detail::record_generic_mapping(mmio_host::root, range.start + addr::get_hhdm_offset(), range.start,
                                                range.end - range.start, flags));
}
}  // namespace virt
}  // namespace ker::mod::mm

namespace ker::mod::mm::mmio::detail::platform {
auto lock_pin() noexcept -> uint64_t {
    auto const flags = runtime_ports::flags;
    runtime_ports::flags &= ~runtime_ports::IF;
    assert(pci_cold_host::config_depth == 0 && mmio_host::pin_depth == 0);
    mmio_host::pin.lock();
    ++mmio_host::pin_depth;
    return flags;
}
void unlock_pin(uint64_t flags) noexcept {
    assert(mmio_host::pin_depth == 1);
    --mmio_host::pin_depth;
    mmio_host::pin.unlock();
    runtime_ports::flags = flags;
}
auto boot_root_building() noexcept -> bool { return mmio_host::building; }
auto pre_task_boot_context() noexcept -> bool { return !controller_host::scheduled; }
auto command_line() noexcept -> const char* { return mmio_host::command_line.c_str(); }
auto boot_memory_map() noexcept -> const limine_memmap_response* { return &pci_cold_host::memory_map; }
auto kernel_root() noexcept -> paging::PageTable* { return mmio_host::root; }
auto hhdm_offset() noexcept -> uint64_t { return 0xffff800000000000ULL; }
auto nx_ready() noexcept -> bool { return mmio_host::nx; }
auto arena_layout_safe(uint64_t start, uint64_t bytes) noexcept -> bool {
    return mmio_host::layout && start == mmio_host::BANK && bytes == 1ULL << 39;
}
auto table_physical(const paging::PageTable* table, uint64_t& output) noexcept -> bool {
    auto const found = mmio_host::physical.find(table);
    if (found != mmio_host::physical.end()) {
        output = found->second;
        return true;
    }
    // Mirror native arithmetic rather than accidentally supplying a stronger
    // host-only root whitelist. Intrinsic BANK-pointer rejection must happen
    // in the actual Runtime before treating a device VA as an HHDM table.
    auto const address = reinterpret_cast<uint64_t>(table);
    if (address < 0xffff800000000000ULL || (address & 4095U) != 0) return false;
    auto const physical = address - 0xffff800000000000ULL;
    if (physical >= (1ULL << 52)) return false;
    output = physical;
    return true;
}
auto table_virtual(uint64_t physical) noexcept -> paging::PageTable* {
    auto const found = mmio_host::tables.find(physical);
    return found == mmio_host::tables.end() ? nullptr : found->second;
}
auto table_frame_valid(uint64_t physical) noexcept -> bool { return mmio_host::tables.contains(physical); }
auto allocate_zeroed_table() noexcept -> paging::PageTable* {
    assert(mmio_host::pin_depth == 0 && pci_cold_host::config_depth == 0);
    return mmio_host::allocate_table();
}
void release_unpublished_table(paging::PageTable* table) noexcept {
    auto const found = mmio_host::physical.find(table);
    assert(found != mmio_host::physical.end());
    mmio_host::tables.erase(found->second);
    mmio_host::physical.erase(found);
    ++mmio_host::releases;
    assert(munmap(table, 4096) == 0);
}
auto ensure_table_addressable(paging::PageTable* table) noexcept -> bool {
    if (!mmio_host::addressable) return false;
    auto const physical = mmio_host::physical.at(table);
    // Execute the unchanged actual generic writer while this fresh RAM frame
    // is still ordinary. The real registry subsequently checks the real
    // supervisor/RW/NX/selector0 HHDM leaf before registering it private.
    mmio_writer_host::map_page(mmio_host::root, physical + 0xffff800000000000ULL, physical, mmio_host::table_alias_flags);
    return true;
}
void reload_current_root() noexcept { ++mmio_host::reloads; }
auto read_register32(uint64_t virtual_address) noexcept -> uint32_t {
    ++mmio_host::register_reads;
    auto const physical = mmio_host::translate(virtual_address);
    assert(physical >= pci_cold_host::BAR0 && physical - pci_cold_host::BAR0 < pci_cold_host::BAR_BYTES);
    return e1000_host::read_register(physical - pci_cold_host::BAR0);
}
void write_register32(uint64_t virtual_address, uint32_t value) noexcept {
    ++mmio_host::register_writes;
    auto const physical = mmio_host::translate(virtual_address);
    assert(physical >= pci_cold_host::BAR0 && physical - pci_cold_host::BAR0 < pci_cold_host::BAR_BYTES);
    e1000_host::write_register(physical - pci_cold_host::BAR0, value);
}
}  // namespace ker::mod::mm::mmio::detail::platform
