#pragma once

#include <defines/defines.hpp>
#include <mod/io/port/port.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/acpi/acpi.hpp>
#include <platform/acpi/madt/madt.hpp>
#include <platform/asm/msr.hpp>
#include <platform/mm/addr.hpp>

namespace ker::mod::apic {
static uint64_t apic_base = 0x0;

enum class APICRegisters : uint16_t {
    ID = 0x20,
    VER = 0x30,
    TASK_PRIORITY = 0x80,
    ARB_PRIORITY = 0x90,
    PROC_PRIORITY = 0xA0,
    EOI = 0xB0,
    REMOTE_READ = 0xC0,
    LOGICAL_DEST = 0xD0,
    DEST_FORMAT = 0xE0,
    SPURIOUS = 0xF0,
    ISR_START = 0x100,  // 0x100 - 0x170
    TRIG_MODE = 0x180,  // 0x180 - 0x1F0
    IRQ = 0x200,        // 0x200 - 0x270
    ERROR_STAT = 0x280,
    LVT_CMCI = 0x2F0,
    ICR0 = 0x300,
    ICR1 = 0x310,
    LVT_TIMER = 0x320,
    LVT_THERMAL = 0x330,
    LVT_PERFMON = 0x340,
    LVT_LINT0 = 0x350,
    LVT_LINT1 = 0x360,
    LVT_ERROR = 0x370,
    TMR_INIT_CNT = 0x380,
    TMR_CURR_CNT = 0x390,
    TMR_DIV_CFG = 0x3E0,
};

enum class X2APICMSRs : uint16_t {
    ID = 0x802,
    VER = 0x803,
    TASK_PRIORITY = 0x808,
    PROC_PRIORITY = 0x80A,
    EOI = 0x80B,
    LOGICAL_DEST = 0x80D,
    SPURIOUS_INT_VEC = 0x80F,
    IN_SERVICE_REGISTER = 0x810,         // 0x810 - 0x817
    TRIGGER_MODE_REGISTER = 0x818,       // 0x818 - 0x81F
    INTERRUPT_REQUEST_REGISTER = 0x820,  // 0x820 - 0x827
    ERROR_STATUS_REGISTER = 0x828,
    LVT_CMCI = 0x82F,
    ICR = 0x830,  // interrupt command register
    LVT_TIMER = 0x832,
    LVT_THERMAL = 0x833,
    LVT_PERFMON = 0x834,
    LVT_LINT0 = 0x835,
    LVT_LINT1 = 0x836,
    LVT_ERROR = 0x837,
    TIMER_INIT_COUNT = 0x838,
    TIMER_CURRENT_COUNT = 0x839,
    TIMER_DIVIDE_CONFIG = 0x83E,
    SELF_IPI = 0x83F,

};

const uint32_t IPI_BROADCAST_ID = 0xFFFFFFFF;

enum class IPIDeliveryMode : uint8_t {
    FIXED = 0,
    LOWEST_PRIORITY = 1,
    SMI = 2,
    NMI = 4,
    INIT = 5,
    STARTUP = 6,
    EXTINT = 7,
};

enum class IPIDestinationMode : uint8_t {
    PHYSICAL = 0,
    LOGICAL = 1,
};

enum class IPILevel : uint8_t {
    DEASSERT = 0,
    ASSERT = 1,
};

enum class IPIDestinationShorthand : uint8_t {
    NONE = 0,
    SELF = 1,
    ALL_INCLUDING_SELF = 2,
    ALL_EXCLUDING_SELF = 3,
};

enum class IPITriggerMode : uint8_t {
    EDGE = 0,
    LEVEL = 1,
};

union IPIConfig {
    struct {
        uint8_t vector;
        IPIDeliveryMode delivery_mode : 3;
        IPIDestinationMode destination_mode : 1;
        uint8_t reserved1 : 2;
        IPILevel level : 1;
        IPITriggerMode trigger_mode : 1;
        uint8_t reserved2 : 2;
        IPIDestinationShorthand destination_shorthand : 2;
        uint32_t reserved3 : 12;
    } __attribute__((packed));
    uint32_t packed_value;
};

void write_reg(uint32_t reg, uint64_t value);

uint32_t read_reg(uint32_t reg);

void eoi();

void send_ipi(IPIConfig message_type, uint32_t destination);

void reset_apic_counter();

uint64_t get_ticks();

uint32_t calibrate_timer(uint64_t us);

void one_shot_timer(uint64_t ticks);

void init();
void init_apic_mp();

uint32_t get_apic_id();

// void startInterrupts();
}  // namespace ker::mod::apic
