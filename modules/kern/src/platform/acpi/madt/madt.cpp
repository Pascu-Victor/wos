#include "madt.hpp"

#include <platform/dbg/dbg.hpp>

namespace ker::mod::acpi::madt {
static ApicInfo apicDevice;

void enumerateDevices(MultiApicTable *madtPtr) {
    constexpr const char *apicNames[6] = {
        "LAPIC", "I/O APIC", "I/O APIC Interrupt source override", "I/O APIC NMI source", "I/O APIC NMI", "LAPIC Address override",
    };
    auto totalTableLen = madtPtr->sdt.length;
    uint64_t currentOffset = 0x2CULL;
    APICRecord *currentApic = (APICRecord *)((uint64_t)madtPtr + currentOffset);
    while (currentOffset < totalTableLen) {
        dbg::log("APIC Type: %s", apicNames[currentApic->type]);
        switch (currentApic->type) {
            case MADT_TYPE_LAPIC: {
                LAPIC *lapic = (LAPIC *)currentApic;
                dbg::log("LAPIC Processor ID: %d", lapic->acpiProcessorId);
                dbg::log("LAPIC ID: %d", lapic->apicId);
                dbg::log("LAPIC Flags: %d", lapic->flags);
                apicDevice.lapics[apicDevice.usableLAPICs++] = *lapic;
            } break;
            case MADT_TYPE_IOAPIC: {
                IOApic *ioApic = (IOApic *)currentApic;
                dbg::log("IOAPIC ID: %d", ioApic->ioApicId);
                dbg::log("IOAPIC Addr: %x", ioApic->ioApicAddr);
                dbg::log("Global Sys Int Base: %d", ioApic->globalSysIntBase);
                apicDevice.ioapics[apicDevice.usableIOAPICs++] = *ioApic;
            } break;
            case MADT_TYPE_IOAPIC_INT_SRC_OVERRIDE: {
                LAPICIntSrcOverride *ioApicIntSrcOverride = (LAPICIntSrcOverride *)currentApic;
                dbg::log("IOAPIC Int Src Override Bus: %d", ioApicIntSrcOverride->bus);
                dbg::log("IOAPIC Int Src Override Source: %d", ioApicIntSrcOverride->source);
                dbg::log("IOAPIC Int Src Override Global Sys Int: %d", ioApicIntSrcOverride->globalSysInt);
                dbg::log("IOAPIC Int Src Override Flags: %d", ioApicIntSrcOverride->flags);
                apicDevice.ioapicISOs[apicDevice.usableIOAPICISOs++] = *ioApicIntSrcOverride;
            } break;
            case MADT_TYPE_LAPIC_NMI: {
                LAPICNMI *lapicNMI = (LAPICNMI *)currentApic;
                dbg::log("LAPIC NMI Processor ID: %d", lapicNMI->acpiProcessorId);
                dbg::log("LAPIC NMI Flags: %d", lapicNMI->flags);
                dbg::log("LAPIC NMI LINT: %d", lapicNMI->lint);
                apicDevice.lapicNMIs[apicDevice.usableLAPICNMIs++] = *lapicNMI;
            } break;
            case MADT_TYPE_LAPIC_ADDR_OVERRIDE: {
                LAPIC *lapic = (LAPIC *)currentApic;
                dbg::log("LAPIC Processor ID: %d", lapic->acpiProcessorId);
                dbg::log("LAPIC ID: %d", lapic->apicId);
                dbg::log("LAPIC Flags: %d", lapic->flags);
                apicDevice.lapics[apicDevice.usableLAPICs++] = *lapic;
            } break;
            case MADT_TYPE_LAPIC_X2APIC: {
                X2APIC *x2apic = (X2APIC *)currentApic;
                dbg::log("X2APIC ID: %d", x2apic->x2apicId);
                dbg::log("X2APIC Flags: %d", x2apic->flags);
                dbg::log("X2APIC Processor UID: %d", x2apic->acpiProcessorUid);
                apicDevice.x2apics[apicDevice.usableX2APICs++] = *x2apic;
            } break;
            default:
                dbg::log("Unknown APIC type: %d", currentApic->type);
                break;
        }
        currentOffset += currentApic->length;
        currentApic = (APICRecord *)((uint64_t)madtPtr + currentOffset);
    }
}

ApicInfo parseMadt(void *madtBasePtr) {
    MultiApicTable *madtPtr = (MultiApicTable *)((uint64_t *)madtBasePtr);
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
    enumerateDevices(madtPtr);

    dbg::log("MADT OEM ID: %s", oemString);
    dbg::log("MADT OEM Table ID: %s", tableString);
    return apicDevice;
}

auto getApicInfo() -> const ApicInfo& { return apicDevice; }
}  // namespace ker::mod::acpi::madt
