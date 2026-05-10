#pragma once

#include <cstddef>
#include <defines/defines.hpp>
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
constexpr size_t MADT_PARSED_ENTRY_CAPACITY = 512;

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

static_assert(sizeof(IOApic) == 12);
static_assert(sizeof(LAPIC) == 8);
static_assert(sizeof(LAPICNMI) == 6);
static_assert(sizeof(LAPICIntSrcOverride) == 10);
static_assert(sizeof(X2APIC) == 16);
static_assert(sizeof(APICRecord) == 2);
static_assert(offsetof(MultiApicTable, local_apic_addr) == 36);
static_assert(offsetof(MultiApicTable, local_apic_flags) == 40);
static_assert(sizeof(MultiApicTable) == 44);

struct ApicInfo {
    uint64_t lapic_addr;
    uint32_t usable_ioapics;
    // NOLINTNEXTLINE(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): Parsed MADT storage.
    IOApic ioapics[MADT_PARSED_ENTRY_CAPACITY];
    uint32_t usable_lapics;
    // NOLINTNEXTLINE(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): Parsed MADT storage.
    LAPIC lapics[MADT_PARSED_ENTRY_CAPACITY];
    uint32_t usable_lapic_nmis;
    // NOLINTNEXTLINE(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): Parsed MADT storage.
    LAPICNMI lapic_nmis[MADT_PARSED_ENTRY_CAPACITY];
    uint32_t usable_ioapic_isos;
    // NOLINTNEXTLINE(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): Parsed MADT storage.
    LAPICIntSrcOverride ioapic_isos[MADT_PARSED_ENTRY_CAPACITY];
    uint32_t usable_x2_apics;
    // NOLINTNEXTLINE(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): Parsed MADT storage.
    X2APIC x2apics[MADT_PARSED_ENTRY_CAPACITY];
} __attribute__((packed));

ApicInfo parse_madt(void* madt);
auto get_apic_info() -> const ApicInfo&;
}  // namespace ker::mod::acpi::madt
