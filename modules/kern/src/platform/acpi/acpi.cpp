#include "acpi.hpp"

#include <cstddef>
#include <cstdint>

#include "extern/limine.h"
#include "platform/acpi/tables/rdst.hpp"
#include "platform/acpi/tables/rsdp.hpp"
#include "platform/acpi/tables/sdt.hpp"
#include "platform/mm/addr.hpp"

namespace {

__attribute__((used, section(".requests"))) volatile limine_rsdp_request rsdp_request = {
    // NOLINT
    .id = LIMINE_RSDP_REQUEST_ID,
    .revision = 0,
    .response = nullptr,
};

}  // namespace

namespace ker::mod::acpi {

namespace {

auto validate_checksum(const Sdt* sdt) -> bool {
    uint8_t sum = 0;
    for (size_t i = 0; i < sdt->length; i++) {
        sum += reinterpret_cast<const uint8_t*>(sdt)[i];
    }
    return sum == 0;
}

auto signature_matches(const Sdt& sdt, const char* ident) -> bool {
    for (size_t i = 0; i < 4; ++i) {
        if (sdt.signature[i] != ident[i]) {
            return false;
        }
    }
    return true;
}

}  // namespace

void init() { ker::mod::acpi::rsdp::init(reinterpret_cast<uint64_t>(rsdp_request.response->address)); }

ACPIResult parse_acpi_tables(const char* ident) {
    rsdp::Rsdp const RSDP = rsdp::get();

    Rsdt const* rsdt = nullptr;
    Xsdt const* xsdt = nullptr;
    Sdt header{};
    bool const USE_XSDT = rsdp::use_xsdt();

    if (USE_XSDT) {
        xsdt = reinterpret_cast<Xsdt*>(mm::addr::get_virt_pointer(RSDP.xsdt_addr));
        header = xsdt->header;
    } else {
        rsdt = reinterpret_cast<Rsdt*>(mm::addr::get_virt_pointer(static_cast<uint64_t>(RSDP.rsdt_addr)));
        header = rsdt->header;
    }

    size_t const ENTRIES = (header.length - sizeof(Sdt)) / (USE_XSDT ? sizeof(uint64_t) : sizeof(uint32_t));
    for (size_t i = 0; i < ENTRIES; i++) {
        auto const TABLE_PHYS = USE_XSDT ? xsdt->next[i] : static_cast<uint64_t>(rsdt->next[i]);
        auto* sdt = reinterpret_cast<Sdt*>(mm::addr::get_virt_pointer(TABLE_PHYS));
        if (signature_matches(*sdt, ident) && validate_checksum(sdt)) {
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
