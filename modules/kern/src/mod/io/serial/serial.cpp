#include "serial.hpp"

#include <atomic>
#include <platform/asm/cpu.hpp>

namespace ker::mod::io {
namespace serial {
bool isInit = false;

// Reentrant spinlock: tracks owner CPU and recursion depth
// Use UINT64_MAX-1 for early boot (before per-CPU is set up)
static constexpr uint64_t NO_OWNER = UINT64_MAX;
static constexpr uint64_t EARLY_BOOT_CPU = UINT64_MAX - 1;
static std::atomic<uint64_t> lockOwner{NO_OWNER};
static std::atomic<uint64_t> lockDepth{0};
static std::atomic<bool> cpuIdAvailable{false};
static std::atomic<bool> inPanicMode{false};

void markCpuIdAvailable() { cpuIdAvailable.store(true, std::memory_order_release); }

void enterPanicMode() {
    // Once in panic mode, all lock operations become no-ops.
    // This prevents deadlocks when CPU ID detection is unreliable during panic.
    inPanicMode.store(true, std::memory_order_release);
}

static uint64_t getCurrentCpuId() {
    if (cpuIdAvailable.load(std::memory_order_acquire)) {
        return cpu::currentCpu();
    }
    // During early boot, treat as single CPU with special ID
    return EARLY_BOOT_CPU;
}

void acquireLock() {
    // In panic mode, skip all locking to avoid deadlocks from unreliable CPU ID
    if (inPanicMode.load(std::memory_order_acquire)) {
        return;
    }

    uint64_t cpu = getCurrentCpuId();

    // If we already own the lock, just increment depth
    if (lockOwner.load(std::memory_order_relaxed) == cpu) {
        lockDepth.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    // Spin until we can acquire
    uint64_t expected = NO_OWNER;
    while (!lockOwner.compare_exchange_weak(expected, cpu, std::memory_order_acquire, std::memory_order_relaxed)) {
        expected = NO_OWNER;
        asm volatile("pause");
    }
    lockDepth.store(1, std::memory_order_relaxed);
}

void releaseLock() {
    // In panic mode, skip all locking
    if (inPanicMode.load(std::memory_order_acquire)) {
        return;
    }

    uint64_t depth = lockDepth.fetch_sub(1, std::memory_order_relaxed);
    if (depth == 1) {
        // Last release - actually unlock
        lockOwner.store(NO_OWNER, std::memory_order_release);
    }
}

// Internal unlocked character write
static void writeCharUnlocked(char c) {
    while ((inb(0x3F8 + 5) & 0x20) == 0);
    outb(0x3F8, c);
}

void init(void) {
    if (isInit) {
        return;
    }
    outb(0x3F8 + 1, 0x00);  // Disable all interrupts
    outb(0x3F8 + 3, 0x80);  // Enable DLAB (set baud rate divisor)
    outb(0x3F8 + 0, 0x02);  // Set divisor to 2 (lo byte) 38400 baud
    outb(0x3F8 + 1, 0x00);  //                  (hi byte)
    outb(0x3F8 + 3, 0x03);  // 8 bits, no parity, one stop bit
    outb(0x3F8 + 2, 0xC7);  // Enable FIFO, clear them, with 14-byte threshold
    outb(0x3F8 + 4, 0x0B);  // IRQs enabled, RTS/DSR set
    isInit = true;
}

void write(const char* str) {
    acquireLock();
    for (size_t i = 0; str[i] != '\0'; i++) {
        while ((inb(0x3F8 + 5) & 0x20) == 0);
        outb(0x3F8, str[i]);
    }
    releaseLock();
}

void write(const char* str, uint64_t len) {
    acquireLock();
    for (size_t i = 0; i < len; i++) {
        while ((inb(0x3F8 + 5) & 0x20) == 0);
        outb(0x3F8, str[i]);
    }
    releaseLock();
}

void write(const char c) {
    acquireLock();
    while ((inb(0x3F8 + 5) & 0x20) == 0);
    outb(0x3F8, c);
    releaseLock();
}

// Unlocked write variants - caller must hold lock
void writeUnlocked(const char* str) {
    for (size_t i = 0; str[i] != '\0'; i++) {
        writeCharUnlocked(str[i]);
    }
}

void writeUnlocked(const char* str, uint64_t len) {
    for (size_t i = 0; i < len; i++) {
        writeCharUnlocked(str[i]);
    }
}

void writeUnlocked(const char c) { writeCharUnlocked(c); }

void writeUnlocked(uint64_t num) {
    char str[21];
    str[20] = '\0';
    // simple u64 -> decimal string converter
    int pos = 20;
    if (num == 0) {
        str[--pos] = '0';
    } else {
        while (num > 0 && pos > 0) {
            str[--pos] = '0' + (num % 10);
            num /= 10;
        }
    }
    // shift to start
    int len = 20 - pos;
    for (int i = 0; i < len; ++i) str[i] = str[pos + i];
    str[len] = '\0';
    writeUnlocked(str);
}

void write(uint64_t num) {
    acquireLock();
    writeUnlocked(num);
    releaseLock();
}

void writeHexUnlocked(uint64_t num) {
    char str[17];
    str[16] = '\0';
    const char* hex = "0123456789abcdef";
    for (int i = 0; i < 16; ++i) {
        str[15 - i] = hex[num & 0xF];
        num >>= 4;
    }
    // trim leading zeros
    int start = 0;
    while (start < 15 && str[start] == '0') ++start;
    int len = 16 - start;
    for (int i = 0; i < len; ++i) str[i] = str[start + i];
    str[len] = '\0';
    writeUnlocked(str);
}

void writeHex(uint64_t num) {
    acquireLock();
    writeHexUnlocked(num);
    releaseLock();
}

void writeBinUnlocked(uint64_t num) {
    char str[65];
    str[64] = '\0';
    for (uint64_t i = 64; i > 0; i--) {
        str[64 - i] = (num & (1ULL << (i - 1))) ? '1' : '0';
    }
    writeUnlocked(str);
}

void writeBin(uint64_t num) {
    acquireLock();
    writeBinUnlocked(num);
    releaseLock();
}
}  // namespace serial
}  // namespace ker::mod::io
