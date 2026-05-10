#include "madt.hpp"

#include <cstdint>
#include <platform/dbg/dbg.hpp>

#include "platform/pic/pic.hpp"

namespace ker::mod::acpi::madt {
static ApicInfo apic_device;

static void enumerate_devices(MultiApicTable* madt_ptr) {
    constexpr const char* APIC_NAMES[6] = {
        "LAPIC", "I/O APIC", "I/O APIC Interrupt source override", "I/O APIC NMI source", "I/O APIC NMI", "LAPIC Address override",
    };
    auto total_table_len = madt_ptr->sdt.length;
    uint64_t current_offset = 0x2CULL;
    auto* current_apic = (APICRecord*)((uint64_t)madt_ptr + current_offset);
    while (current_offset < total_table_len) {
        dbg::log("APIC Type: %s", APIC_NAMES[current_apic->type]);
        switch (current_apic->type) {
            case MADT_TYPE_LAPIC: {
                auto const* lapic = reinterpret_cast<LAPIC*>(current_apic);
                dbg::log("LAPIC Processor ID: %d", lapic->acpi_processor_id);
                dbg::log("LAPIC ID: %d", lapic->apic_id);
                dbg::log("LAPIC Flags: %d", lapic->flags);
                apic_device.lapics[apic_device.usable_lapics++] = *lapic;
            } break;
            case MADT_TYPE_IOAPIC: {
                auto const* io_apic = reinterpret_cast<IOApic*>(current_apic);
                dbg::log("IOAPIC ID: %d", io_apic->io_apic_id);
                dbg::log("IOAPIC Addr: %x", io_apic->io_apic_addr);
                dbg::log("Global Sys Int Base: %d", io_apic->global_sys_int_base);
                apic_device.ioapics[apic_device.usable_ioapics++] = *io_apic;
            } break;
            case MADT_TYPE_IOAPIC_INT_SRC_OVERRIDE: {
                auto const* io_apic_int_src_override = reinterpret_cast<LAPICIntSrcOverride*>(current_apic);
                dbg::log("IOAPIC Int Src Override Bus: %d", io_apic_int_src_override->bus);
                dbg::log("IOAPIC Int Src Override Source: %d", io_apic_int_src_override->source);
                dbg::log("IOAPIC Int Src Override Global Sys Int: %d", io_apic_int_src_override->global_sys_int);
                dbg::log("IOAPIC Int Src Override Flags: %d", io_apic_int_src_override->flags);
                apic_device.ioapic_isos[apic_device.usable_ioapic_isos++] = *io_apic_int_src_override;
            } break;
            case MADT_TYPE_LAPIC_NMI: {
                auto const* lapic_nmi = reinterpret_cast<LAPICNMI*>(current_apic);
                dbg::log("LAPIC NMI Processor ID: %d", lapic_nmi->acpi_processor_id);
                dbg::log("LAPIC NMI Flags: %d", lapic_nmi->flags);
                dbg::log("LAPIC NMI LINT: %d", lapic_nmi->lint);
                apic_device.lapic_nmis[apic_device.usable_lapic_nmis++] = *lapic_nmi;
            } break;
            case MADT_TYPE_LAPIC_ADDR_OVERRIDE: {
                auto const* lapic = reinterpret_cast<LAPIC*>(current_apic);
                dbg::log("LAPIC Processor ID: %d", lapic->acpi_processor_id);
                dbg::log("LAPIC ID: %d", lapic->apic_id);
                dbg::log("LAPIC Flags: %d", lapic->flags);
                apic_device.lapics[apic_device.usable_lapics++] = *lapic;
            } break;
            case MADT_TYPE_LAPIC_X2APIC: {
                auto const* x2apic = reinterpret_cast<X2APIC*>(current_apic);
                dbg::log("X2APIC ID: %d", x2apic->x2apic_id);
                dbg::log("X2APIC Flags: %d", x2apic->flags);
                dbg::log("X2APIC Processor UID: %d", x2apic->acpi_processor_uid);
                apic_device.x2apics[apic_device.usable_x2_apics++] = *x2apic;
            } break;
            default:
                dbg::log("Unknown APIC type: %d", current_apic->type);
                break;
        }
        current_offset += current_apic->length;
        current_apic = (APICRecord*)((uint64_t)madt_ptr + current_offset);
    }
}

ApicInfo parse_madt(void* madt_base_ptr) {
    auto* madt_ptr = reinterpret_cast<MultiApicTable*>(static_cast<uint64_t*>(madt_base_ptr));
    uint8_t oem_string[7] = {0};
    for (int i = 0; i < 6; i++) {
        oem_string[i] = madt_ptr->sdt.oem_id[i];
    }
    oem_string[6] = 0;

    uint8_t table_string[9] = {0};
    for (int i = 0; i < 8; i++) {
        table_string[i] = madt_ptr->sdt.oem_table_id[i];
    }
    table_string[8] = 0;

    apic_device.lapic_addr = madt_ptr->local_apic_addr;

    pic::disable();
    enumerate_devices(madt_ptr);

    dbg::log("MADT OEM ID: %s", oem_string);
    dbg::log("MADT OEM Table ID: %s", table_string);
    return apic_device;
}

auto get_apic_info() -> const ApicInfo& { return apic_device; }
}  // namespace ker::mod::acpi::madt
