#pragma once

#include <cstddef>
#include <defines/defines.hpp>
#include <mod/io/port/port.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/addr.hpp>
#include <util/hcf.hpp>

namespace ker::mod::acpi::rsdp {
struct Rsdp {
    char signature[8];  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): Firmware table bytes.
    uint8_t checksum;
    char oem_id[6];  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): Firmware table bytes.
    uint8_t revision;
    uint32_t rsdt_addr;
    uint32_t length;
    uint64_t xsdt_addr;
    uint8_t extended_checksum;
    uint8_t reserved[3];  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): Firmware reserved bytes.
} __attribute__((packed));

static_assert(offsetof(Rsdp, rsdt_addr) == 16);
static_assert(offsetof(Rsdp, length) == 20);
static_assert(offsetof(Rsdp, xsdt_addr) == 24);
static_assert(offsetof(Rsdp, reserved) == 33);
static_assert(sizeof(Rsdp) == 36);

Rsdp get();
bool use_xsdt();
void init(uint64_t rsdp_addr);
}  // namespace ker::mod::acpi::rsdp
