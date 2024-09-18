#include "rsdp.hpp"

namespace ker::mod::acpi::rsdp {
static bool hasXsdt = false;
static Rsdp rsdp;

void validateChecksum(const Rsdp *rsdp) {
    uint8_t sum = 0;
    uint8_t *ptr = (uint8_t *)rsdp;

    for (size_t i = 0; i < 20; i++) {
        sum += *ptr++;
    }

    if ((sum & 0xFF) != 0) {
        io::serial::write("ACPI: RSDP checksum failed\n");
        hcf();
    }
}

bool useXsdt(void) { return hasXsdt; }

void init(uint64_t rsdpAddr) {
    validateChecksum(&rsdp);
    rsdp = *(Rsdp *)rsdpAddr;
    rsdp.oem_id[5] = 0;
    // TODO: log stuff

    if (rsdp.revision >= 2) {
        hasXsdt = true;
    }
}

Rsdp get(void) { return rsdp; }
}  // namespace ker::mod::acpi::rsdp
