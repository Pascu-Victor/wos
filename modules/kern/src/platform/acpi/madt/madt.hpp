#pragma once

#include <defines/defines.hpp>
#include <platform/acpi/acpi.hpp>
#include <platform/acpi/tables/sdt.hpp>
#include <platform/pic/pic.hpp>

namespace ker::mod::acpi::madt {
const uint64_t LAPIC_ADDR_OVERRIDE = 5;
const uint64_t IOAPIC = 1;

struct madt {
    Sdt sdt;
    uint32_t localApicAddr;
    uint32_t localApicFlags;
} __attribute__((packed));

struct IOApic {
    uint8_t type;
    uint8_t length;
    uint8_t ioApicId;
    uint8_t reserved;
    uint32_t ioApicAddr;
    uint32_t globalSysIntBase;
} __attribute__((packed));

struct ApicInfo {
    uint64_t lapicAddr;
    uint32_t usableIOApics;
    struct IOApic* ioapics[16];
} __attribute__((packed));

ApicInfo parseMadt(void* madt);
}  // namespace ker::mod::acpi::madt
