#include "madt.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <platform/dbg/dbg.hpp>

#include "platform/pic/pic.hpp"

namespace ker::mod::acpi::madt {

using log = ker::mod::dbg::logger<"madt">;

namespace {

ApicInfo apic_device;

auto apic_type_name(uint8_t type) -> const char* {
    switch (type) {
        case MADT_TYPE_LAPIC:
            return "LAPIC";
        case MADT_TYPE_IOAPIC:
            return "I/O APIC";
        case MADT_TYPE_IOAPIC_INT_SRC_OVERRIDE:
            return "I/O APIC Interrupt source override";
        case MADT_TYPE_IOAPIC_NMI:
            return "I/O APIC NMI source";
        case MADT_TYPE_LAPIC_NMI:
            return "I/O APIC NMI";
        case MADT_TYPE_LAPIC_ADDR_OVERRIDE:
            return "LAPIC Address override";
        case MADT_TYPE_LAPIC_X2APIC:
            return "X2APIC";
        default:
            return "Unknown";
    }
}

auto apic_record_at(MultiApicTable* madt_ptr, uint64_t offset) -> APICRecord* {
    return reinterpret_cast<APICRecord*>(reinterpret_cast<uint64_t>(madt_ptr) + offset);
}

void enumerate_devices(MultiApicTable* madt_ptr) {
    auto total_table_len = madt_ptr->sdt.length;
    uint64_t current_offset = 0x2CULL;
    auto* current_apic = apic_record_at(madt_ptr, current_offset);
    while (current_offset < total_table_len) {
        log::debug("APIC Type: %s", apic_type_name(current_apic->type));
        switch (current_apic->type) {
            case MADT_TYPE_LAPIC: {
                auto const* lapic = reinterpret_cast<LAPIC*>(current_apic);
                log::debug("LAPIC Processor ID: %d", lapic->acpi_processor_id);
                log::debug("LAPIC ID: %d", lapic->apic_id);
                log::debug("LAPIC Flags: %d", lapic->flags);
                apic_device.lapics[apic_device.usable_lapics++] = *lapic;
            } break;
            case MADT_TYPE_IOAPIC: {
                auto const* io_apic = reinterpret_cast<IOApic*>(current_apic);
                log::debug("IOAPIC ID: %d", io_apic->io_apic_id);
                log::debug("IOAPIC Addr: %x", io_apic->io_apic_addr);
                log::debug("Global Sys Int Base: %d", io_apic->global_sys_int_base);
                apic_device.ioapics[apic_device.usable_ioapics++] = *io_apic;
            } break;
            case MADT_TYPE_IOAPIC_INT_SRC_OVERRIDE: {
                auto const* io_apic_int_src_override = reinterpret_cast<LAPICIntSrcOverride*>(current_apic);
                log::debug("IOAPIC Int Src Override Bus: %d", io_apic_int_src_override->bus);
                log::debug("IOAPIC Int Src Override Source: %d", io_apic_int_src_override->source);
                log::debug("IOAPIC Int Src Override Global Sys Int: %d", io_apic_int_src_override->global_sys_int);
                log::debug("IOAPIC Int Src Override Flags: %d", io_apic_int_src_override->flags);
                apic_device.ioapic_isos[apic_device.usable_ioapic_isos++] = *io_apic_int_src_override;
            } break;
            case MADT_TYPE_LAPIC_NMI: {
                auto const* lapic_nmi = reinterpret_cast<LAPICNMI*>(current_apic);
                log::debug("LAPIC NMI Processor ID: %d", lapic_nmi->acpi_processor_id);
                log::debug("LAPIC NMI Flags: %d", lapic_nmi->flags);
                log::debug("LAPIC NMI LINT: %d", lapic_nmi->lint);
                apic_device.lapic_nmis[apic_device.usable_lapic_nmis++] = *lapic_nmi;
            } break;
            case MADT_TYPE_LAPIC_ADDR_OVERRIDE: {
                auto const* lapic = reinterpret_cast<LAPIC*>(current_apic);
                log::debug("LAPIC Processor ID: %d", lapic->acpi_processor_id);
                log::debug("LAPIC ID: %d", lapic->apic_id);
                log::debug("LAPIC Flags: %d", lapic->flags);
                apic_device.lapics[apic_device.usable_lapics++] = *lapic;
            } break;
            case MADT_TYPE_LAPIC_X2APIC: {
                auto const* x2apic = reinterpret_cast<X2APIC*>(current_apic);
                log::debug("X2APIC ID: %d", x2apic->x2apic_id);
                log::debug("X2APIC Flags: %d", x2apic->flags);
                log::debug("X2APIC Processor UID: %d", x2apic->acpi_processor_uid);
                apic_device.x2apics[apic_device.usable_x2_apics++] = *x2apic;
            } break;
            default:
                log::warn("Unknown APIC type: %d", current_apic->type);
                break;
        }
        current_offset += current_apic->length;
        current_apic = apic_record_at(madt_ptr, current_offset);
    }
}

}  // namespace

ApicInfo parse_madt(void* madt_base_ptr) {
    auto* madt_ptr = static_cast<MultiApicTable*>(madt_base_ptr);
    std::array<char, 7> oem_string{};
    for (size_t i = 0; i < 6; i++) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access): bounded ACPI OEM byte copy.
        oem_string[i] = madt_ptr->sdt.oem_id[i];
    }

    std::array<char, 9> table_string{};
    for (size_t i = 0; i < 8; i++) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access): bounded ACPI OEM byte copy.
        table_string[i] = madt_ptr->sdt.oem_table_id[i];
    }

    apic_device.lapic_addr = madt_ptr->local_apic_addr;

    pic::disable();
    enumerate_devices(madt_ptr);

    log::info("OEM ID: %s", oem_string.data());
    log::info("OEM Table ID: %s", table_string.data());
    return apic_device;
}

auto get_apic_info() -> const ApicInfo& { return apic_device; }
}  // namespace ker::mod::acpi::madt
