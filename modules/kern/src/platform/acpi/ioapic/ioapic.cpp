#include "ioapic.hpp"

#include <cstdint>
#include <platform/acpi/madt/madt.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>

#include "platform/mm/paging.hpp"

namespace ker::mod::ioapic {

namespace {
volatile uint32_t* ioapic_base = nullptr;
uint32_t gsi_base = 0;
uint32_t max_redirection_entries = 0;

// cannot inline these are MIMO
[[gnu::noinline]] void ioapic_write(uint32_t reg, uint32_t value) {
    ioapic_base[0] = reg;    // IOREGSEL
    ioapic_base[4] = value;  // IOWIN (offset 0x10 in bytes = index 4 in uint32_t)
}
// cannot inline these are MIMO
[[gnu::noinline]] auto ioapic_read(uint32_t reg) -> uint32_t {
    ioapic_base[0] = reg;   // IOREGSEL
    return ioapic_base[4];  // IOWIN
}

void write_redirection(uint8_t index, uint64_t value) {
    uint32_t reg_lo = IOAPIC_REG_REDTBL_BASE + (index * 2);
    uint32_t reg_hi = reg_lo + 1;
    ioapic_write(reg_lo, static_cast<uint32_t>(value & 0xFFFFFFFF));
    ioapic_write(reg_hi, static_cast<uint32_t>(value >> 32));
}

auto read_redirection(uint8_t index) -> uint64_t {
    uint32_t reg_lo = IOAPIC_REG_REDTBL_BASE + (index * 2);
    uint32_t reg_hi = reg_lo + 1;
    uint64_t lo = ioapic_read(reg_lo);
    uint64_t hi = ioapic_read(reg_hi);
    return lo | (hi << 32);
}
}  // namespace

void init() {
    const auto& apic_info = acpi::madt::get_apic_info();

    if (apic_info.usable_ioapics == 0) {
        dbg::log("IOAPIC: No IO APICs found in MADT");
        return;
    }

    // Use the first IO APIC
    uint32_t phys_addr = apic_info.ioapics[0].io_apic_addr;
    gsi_base = apic_info.ioapics[0].global_sys_int_base;

    // Map the IO APIC MMIO page into kernel page table.
    // MMIO regions are not in the Limine memory map, so the HHDM has no
    // page table entry for them after the kernel switches to its own page tables.
    auto virt_addr = reinterpret_cast<uint64_t>(mm::addr::get_virt_pointer(phys_addr));
    mm::virt::map_to_kernel_page_table(virt_addr, phys_addr, mm::paging::page_types::MMIO);

    ioapic_base = reinterpret_cast<volatile uint32_t*>(virt_addr);

    // Read version register to get max redirection entries
    uint32_t ver = ioapic_read(IOAPIC_REG_VER);
    max_redirection_entries = ((ver >> 16) & 0xFF) + 1;

    dbg::log("IOAPIC: addr=0x%x gsi_base=%d max_entries=%d", phys_addr, gsi_base, max_redirection_entries);

    // Mask all entries initially
    for (uint32_t i = 0; i < max_redirection_entries; i++) {
        write_redirection(i, IOAPIC_REDIR_MASK);
    }

    // Apply interrupt source overrides from MADT
    // These remap ISA IRQs (e.g., IRQ 0->GSI 2 for timer)
    for (uint32_t i = 0; i < apic_info.usable_ioapic_isos; i++) {
        const auto& iso = apic_info.ioapic_isos[i];
        dbg::log("IOAPIC: ISO: bus=%d source(IRQ)=%d -> GSI %d flags=0x%x", iso.bus, iso.source, iso.global_sys_int, iso.flags);
    }
}

void route_irq(uint8_t gsi, uint8_t vector, uint32_t dest_apic_id) {
    if (ioapic_base == nullptr) {
        return;
    }

    uint8_t index = gsi - gsi_base;
    if (index >= max_redirection_entries) {
        return;
    }

    // Build redirection entry:
    // - Edge-triggered, active-high (default for PCI/MSI-like devices)
    // - Physical destination mode
    // - Fixed delivery mode
    uint64_t entry = vector;
    entry |= (static_cast<uint64_t>(dest_apic_id) << IOAPIC_REDIR_DEST_SHIFT);

    // Check if there's an interrupt source override for this GSI
    const auto& apic_info = acpi::madt::get_apic_info();
    for (uint32_t i = 0; i < apic_info.usable_ioapic_isos; i++) {
        const auto& iso = apic_info.ioapic_isos[i];
        if (iso.global_sys_int == gsi) {
            // Apply polarity override
            uint8_t polarity = iso.flags & 0x3;
            if (polarity == 0x3) {  // active low
                entry |= IOAPIC_REDIR_ACTIVE_LOW;
            }
            // Apply trigger mode override
            uint8_t trigger = (iso.flags >> 2) & 0x3;
            if (trigger == 0x3) {  // level triggered
                entry |= IOAPIC_REDIR_LEVEL;
            }
            break;
        }
    }

    write_redirection(index, entry);
    dbg::log("IOAPIC: Routed GSI %d -> vector %d (dest APIC %d)", gsi, vector, dest_apic_id);
}

void mask_irq(uint8_t gsi) {
    if (ioapic_base == nullptr) {
        return;
    }

    uint8_t index = gsi - gsi_base;
    if (index >= max_redirection_entries) {
        return;
    }

    uint64_t entry = read_redirection(index);
    entry |= IOAPIC_REDIR_MASK;
    write_redirection(index, entry);
}

void unmask_irq(uint8_t gsi) {
    if (ioapic_base == nullptr) {
        return;
    }

    uint8_t index = gsi - gsi_base;
    if (index >= max_redirection_entries) {
        return;
    }

    uint64_t entry = read_redirection(index);
    entry &= ~IOAPIC_REDIR_MASK;
    write_redirection(index, entry);
}

}  // namespace ker::mod::ioapic
