#pragma once

#include <defines/defines.hpp>
#include <mod/io/port/port.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/acpi/acpi.hpp>
#include <platform/acpi/madt/madt.hpp>
#include <platform/asm/msr.hpp>
#include <platform/mm/addr.hpp>

namespace ker::mod::apic {
static uint64_t LAPIC_BASE = 0x0;
static const uint64_t LAPIC_ID = 0x20;
static const uint64_t LAPIC_EOI = 0xb0;
static const uint64_t LAPIC_SPURIOUS = 0xf0;
static const uint64_t LAPIC_LVT_TIMER = 0x320;
static const uint64_t LAPIC_TIMER_MASK = 0x10000;
static const uint64_t LAPIC_TIMER_PERIODIC = 0x20000;
static const uint64_t LAPIC_TIMER_INIT_COUNT = 0x380;
static const uint64_t LAPIC_TIMER_CURRENT_COUNT = 0x390;
static const uint64_t LAPIC_TIMER_DIV = 0x3e0;

void write(uint32_t offset, uint32_t value);
uint32_t read(uint32_t offset);

void enable(void);
void eoi(void);

uint32_t calibrateTimer(uint64_t us);
void setTimeout(uint64_t numTicks);
int cpuid(void);
void setCpuId(uint32_t id);

void oneShotTimer(uint64_t ticks);

void init(void);
}  // namespace ker::mod::apic

namespace ker::mod::apic2 {
static uint64_t APIC_BASE = 0x0;

enum class APICRegisters {
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

enum class X2APICMSRs {
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

enum class ICR0MessageType {
    SINGLE = (0 << 18),
    SELF = (1 << 18),
    BROADCAST = (2 << 18),
    BROADCAST_EXCLUSIVE = (3 << 18),
};

enum class APICQueries : uint32_t {
    ICR0_DELIVERY_STATUS = (1 << 12),
};

static void writeReg(uint32_t reg, uint32_t value);

static uint32_t readReg(uint32_t reg);

static void eoi();

static void sendIpi(uint32_t lapicId, uint32_t vector, ICR0MessageType messageType);

static void resetApicCounter();

static uint64_t getTicks();

static void calibrateTimer(uint64_t us);

void oneShotTimer(uint64_t ticks);

void initForCpu(uint64_t cpuNo);

void startInterrupts();
}  // namespace ker::mod::apic2
