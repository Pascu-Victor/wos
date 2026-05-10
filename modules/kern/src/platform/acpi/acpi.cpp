#include "acpi.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "extern/limine.h"
#include "platform/acpi/tables/rdst.hpp"
#include "platform/acpi/tables/rsdp.hpp"
#include "platform/acpi/tables/sdt.hpp"
#include "platform/mm/addr.hpp"
#include "util/hcf.hpp"

__attribute__((used, section(".requests"))) static volatile limine_rsdp_request rsdp_request = {
    .id = LIMINE_RSDP_REQUEST_ID,
    .revision = 0,
    .response = nullptr,
};

namespace ker::mod::acpi {
void init() { ker::mod::acpi::rsdp::init((uint64_t)rsdp_request.response->address); }

static bool validate_checksum(Sdt* sdt) {
    uint8_t sum = 0;
    for (size_t i = 0; i < sdt->length; i++) {
        sum += (reinterpret_cast<uint8_t*>(sdt))[i];
    }
    return sum == 0;
}

ACPIResult parse_acpi_tables(const char* ident) {
    rsdp::Rsdp const RSDP = rsdp::get();

    Rsdt const* rsdt = reinterpret_cast<Rsdt*>(mm::addr::get_virt_pointer(RSDP.xsdt_addr));
    Xsdt const* xsdt = nullptr;
    Sdt header{};

    if (rsdp::use_xsdt()) {
        xsdt = reinterpret_cast<Xsdt*>(mm::addr::get_virt_pointer(RSDP.xsdt_addr));
        header = xsdt->header;
    } else {
        rsdt = reinterpret_cast<Rsdt*>(mm::addr::get_virt_pointer(static_cast<uint64_t>(RSDP.rsdt_addr)));
        header = rsdt->header;
    }

    size_t const ENTRIES = (header.length - sizeof(Sdt)) / (rsdp::use_xsdt() ? sizeof(uint64_t) : sizeof(uint32_t));
    if (xsdt == nullptr) {
        hcf();  // no xsdt entries???
    }
    for (size_t i = 0; i < ENTRIES; i++) {
        Sdt* sdt =
            reinterpret_cast<Sdt*>(mm::addr::get_virt_pointer((rsdp::use_xsdt() ? xsdt->next[i] : static_cast<uint64_t>(rsdt->next[i]))));
        if (memcmp(sdt->signature, ident, 4) == 0 && validate_checksum(sdt)) {
            ACPIResult result{};
            result.success = true;
            result.data = sdt;
            return result;
        }
    }

    ACPIResult result{};
    result.success = false;
    result.data = nullptr;
    return result;
}

}  // namespace ker::mod::acpi
