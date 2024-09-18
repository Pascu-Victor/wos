#include "madt.hpp"

namespace ker::mod::acpi::madt {
static ApicInfo apicDevice;

void enumerateDevices(madt **madt) {
    (void)madt;
    // TODO: log devices to console
}

ApicInfo parseMadt(void *madtBasePtr) {
    madt *madtPtr = (madt *)((uint64_t *)madtBasePtr);
    uint8_t oemString[7] = {0};
    for (int i = 0; i < 6; i++) {
        oemString[i] = madtPtr->sdt.oem_id[i];
    }
    oemString[6] = 0;

    uint8_t tableString[9] = {0};
    for (int i = 0; i < 8; i++) {
        tableString[i] = madtPtr->sdt.oem_table_id[i];
    }
    tableString[8] = 0;

    apicDevice.lapicAddr = madtPtr->localApicAddr;

    pic::disable();
    return apicDevice;
}
}  // namespace ker::mod::acpi::madt
