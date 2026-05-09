#pragma once

#include <defines/defines.hpp>
#include <platform/acpi/acpi.hpp>
#include <platform/acpi/tables/sdt.hpp>
#include <platform/pic/pic.hpp>

namespace ker::mod::acpi::madt {
const uint64_t MADT_TYPE_LAPIC = 0;
const uint64_t MADT_TYPE_IOAPIC = 1;
const uint64_t MADT_TYPE_IOAPIC_INT_SRC_OVERRIDE = 2;
const uint64_t MADT_TYPE_IOAPIC_NMI = 3;
const uint64_t MADT_TYPE_LAPIC_NMI = 4;
const uint64_t MADT_TYPE_LAPIC_ADDR_OVERRIDE = 5;
const uint64_t MADT_TYPE_LAPIC_X2APIC = 9;

struct IOApic {
    uint8_t type;
    uint8_t length;
    uint8_t io_apic_id;
    uint8_t reserved;
    uint32_t io_apic_addr;
    uint32_t global_sys_int_base;
} __attribute__((packed));

struct LAPIC {
    uint8_t type;
    uint8_t length;
    uint8_t acpi_processor_id;
    uint8_t apic_id;
    uint32_t flags;
} __attribute__((packed));

struct LAPICNMI {
    uint8_t type;
    uint8_t length;
    uint8_t acpi_processor_id;
    uint16_t flags;
    uint8_t lint;
} __attribute__((packed));

struct LAPICIntSrcOverride {
    uint8_t type;
    uint8_t length;
    uint8_t bus;
    uint8_t source;
    uint32_t global_sys_int;
    uint16_t flags;
} __attribute__((packed));

struct X2APIC {
    uint8_t type;
    uint8_t length;
    uint16_t reserved;
    uint32_t x2apic_id;
    uint32_t flags;
    uint32_t acpi_processor_uid;
} __attribute__((packed));

struct APICRecord {
    uint8_t type;
    uint8_t length;
} __attribute__((packed));

struct MultiApicTable {
    Sdt sdt;
    uint32_t local_apic_addr;
    uint32_t local_apic_flags;
} __attribute__((packed));
struct ApicInfo {
    uint64_t lapic_addr;
    uint32_t usable_ioapics;
    IOApic ioapics[512];
    uint32_t usable_lapics;
    LAPIC lapics[512];
    uint32_t usable_lapic_nmis;
    LAPICNMI lapic_nmis[512];
    uint32_t usable_ioapic_isos;
    LAPICIntSrcOverride ioapic_isos[512];
    uint32_t usable_x2_apics;
    X2APIC x2apics[512];
} __attribute__((packed));

ApicInfo parse_madt(void* madt);
auto get_apic_info() -> const ApicInfo&;
}  // namespace ker::mod::acpi::madt
