#include "rsdp.hpp"

#include <cstddef>
#include <cstdint>

#include "mod/io/serial/serial.hpp"
#include "util/hcf.hpp"

namespace ker::mod::acpi::rsdp {
static bool has_xsdt = false;
static Rsdp rsdp;

static void validate_checksum(const Rsdp* rsdp) {
    uint8_t sum = 0;
    auto* ptr = (uint8_t*)rsdp;

    for (size_t i = 0; i < 20; i++) {
        sum += *ptr++;
    }

    if ((sum & 0xFF) != 0) {
        io::serial::write("ACPI: RSDP checksum failed\n");
        hcf();
    }
}

auto use_xsdt() -> bool { return has_xsdt; }

void init(uint64_t rsdp_addr) {
    rsdp = *(Rsdp*)rsdp_addr;
    validate_checksum(&rsdp);
    rsdp.oem_id[5] = 0;
    // TODO: log stuff

    if (rsdp.revision >= 2) {
        has_xsdt = true;
    }
}

auto get() -> Rsdp { return rsdp; }
}  // namespace ker::mod::acpi::rsdp
