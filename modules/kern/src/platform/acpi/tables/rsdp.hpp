#pragma once

#include <defines/defines.hpp>
#include <mod/io/port/port.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/addr.hpp>
#include <util/hcf.hpp>

namespace ker::mod::acpi::rsdp {
struct Rsdp {
    char signature[8];
    uint8_t checksum;
    char oem_id[6];
    uint8_t revision;
    uint32_t rsdt_addr;
    uint32_t length;
    uint64_t xsdt_addr;
    uint8_t extended_checksum;
    uint8_t reserved[3];
} __attribute__((packed));

Rsdp get();
bool use_xsdt();
void init(uint64_t rsdp_addr);
}  // namespace ker::mod::acpi::rsdp
