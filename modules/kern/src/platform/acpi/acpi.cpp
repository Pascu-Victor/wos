#include "acpi.hpp"

__attribute__((used, section(".requests")))
static volatile limine_rsdp_request rsdpRequest = {
    .id = LIMINE_RSDP_REQUEST,
    .revision = 0,
    .response = nullptr,
};

namespace ker::mod::acpi {
    void init() {
        ker::mod::acpi::rsdp::init((uint64_t)rsdpRequest.response->address);
    }

    bool validateChecksum(Sdt *sdt) {
        uint8_t sum = 0;
        for(size_t i = 0; i < sdt->length; i++) {
            sum += ((uint8_t*)sdt)[i];
        }
        return sum == 0;
    }

    ACPIResult parseAcpiTables(const char* ident) {
        rsdp::Rsdp rsdp = rsdp::get();
        
        Rsdt *rsdt = (Rsdt*)mm::addr::getVirtPointer(rsdp.xsdt_addr);
        Xsdt *xsdt = nullptr;
        Sdt header;

        if(rsdp::useXsdt()) {
            xsdt = (Xsdt*)mm::addr::getVirtPointer(rsdp.xsdt_addr);
            header = xsdt->header;
        } else {
            rsdt = (Rsdt*)mm::addr::getVirtPointer((uint64_t)rsdp.rsdt_addr);
            header = rsdt->header;
        }

        size_t entries = (header.length - sizeof(Sdt)) / (rsdp::useXsdt() ? sizeof(uint64_t) : sizeof(uint32_t));
        if(xsdt == nullptr) {
            hcf(); // no xsdt entries???
        }
        for(size_t i = 0; i < entries; i++) {
            Sdt *sdt =(Sdt*) mm::addr::getVirtPointer((rsdp::useXsdt() ? xsdt->next[i] : (uint64_t)rsdt->next[i]));
            if(memcmp(sdt->signature, ident, 4) == 0 && validateChecksum(sdt)) {
                ACPIResult result;
                result.success = true;
                result.data = sdt;
                return result;
            }
        }

        ACPIResult result;
        result.success = false;
        result.data = nullptr;
        return result;
    }

}