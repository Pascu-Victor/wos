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
    uint8_t ioApicId;
    uint8_t reserved;
    uint32_t ioApicAddr;
    uint32_t globalSysIntBase;
} __attribute__((packed));

struct LAPIC {
    uint8_t type;
    uint8_t length;
    uint8_t acpiProcessorId;
    uint8_t apicId;
    uint32_t flags;
} __attribute__((packed));

struct LAPICNMI {
    uint8_t type;
    uint8_t length;
    uint8_t acpiProcessorId;
    uint16_t flags;
    uint8_t lint;
} __attribute__((packed));

struct LAPICIntSrcOverride {
    uint8_t type;
    uint8_t length;
    uint8_t bus;
    uint8_t source;
    uint32_t globalSysInt;
    uint16_t flags;
} __attribute__((packed));

struct X2APIC {
    uint8_t type;
    uint8_t length;
    uint16_t reserved;
    uint32_t x2apicId;
    uint32_t flags;
    uint32_t acpiProcessorUid;
} __attribute__((packed));

struct APICRecord {
    uint8_t type;
    uint8_t length;
} __attribute__((packed));

struct MultiApicTable {
    Sdt sdt;
    uint32_t localApicAddr;
    uint32_t localApicFlags;
} __attribute__((packed));
struct ApicInfo {
    uint64_t lapicAddr;
    uint32_t usableIOAPICs;
    IOApic ioapics[512];
    uint32_t usableLAPICs;
    LAPIC lapics[512];
    uint32_t usableLAPICNMIs;
    LAPICNMI lapicNMIs[512];
    uint32_t usableIOAPICISOs;
    LAPICIntSrcOverride ioapicISOs[512];
    uint32_t usableX2APICs;
    X2APIC x2apics[512];
} __attribute__((packed));

ApicInfo parseMadt(void* madt);
auto getApicInfo() -> const ApicInfo&;
}  // namespace ker::mod::acpi::madt
