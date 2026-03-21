#include "serial.hpp"

#include <atomic>
#include <platform/asm/cpu.hpp>

namespace ker::mod::io::serial {
bool isInit = false;

// Reentrant spinlock: tracks owner CPU and recursion depth
// Use UINT64_MAX-1 for early boot (before per-CPU is set up)
static constexpr uint64_t NO_OWNER = UINT64_MAX;
static std::atomic<uint64_t> lock_owner{NO_OWNER};
static std::atomic<uint64_t> lock_depth{0};
static std::atomic<bool> cpu_id_available{false};
static std::atomic<bool> in_panic_mode{false};

void markCpuIdAvailable() { cpu_id_available.store(true, std::memory_order_release); }

void enterPanicMode() {
    // Once in panic mode, all lock operations become no-ops.
    // This prevents deadlocks when CPU ID detection is unreliable during panic.
    in_panic_mode.store(true, std::memory_order_release);
}

void acquireLock() {
    // In panic mode, skip all locking to avoid deadlocks from unreliable CPU ID
    if (in_panic_mode.load(std::memory_order_acquire)) {
        return;
    }

    uint64_t cpu = cpu_id_available.load(std::memory_order_acquire)
                       ? cpu::currentCpu()
                       : 0;

    // If we already own the lock, just increment depth
    if (lock_owner.load(std::memory_order_relaxed) == cpu) {
        lock_depth.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    // Spin until we can acquire
    uint64_t expected = NO_OWNER;
    while (!lock_owner.compare_exchange_weak(expected, cpu, std::memory_order_acquire, std::memory_order_relaxed)) {
        expected = NO_OWNER;
        asm volatile("pause");
    }
    lock_depth.store(1, std::memory_order_relaxed);
}

void releaseLock() {
    // In panic mode, skip all locking
    if (in_panic_mode.load(std::memory_order_acquire)) {
        return;
    }

    uint64_t depth = lock_depth.fetch_sub(1, std::memory_order_relaxed);
    if (depth == 1) {
        // Last release - actually unlock
        lock_owner.store(NO_OWNER, std::memory_order_release);
    }
}

// Internal unlocked character write
static void write_char_unlocked(char c) {
    while ((inb(0x3F8 + 5) & 0x20) == 0);
    outb(0x3F8, c);
}

// Per-CPU line buffers: each CPU accumulates characters here and only takes
// the serial lock when flushing a complete line or on overflow. This prevents
// characters from different CPUs interleaving within a single output line.
static constexpr size_t MAX_CPUS = 256;
static constexpr size_t BUF_DATA_SIZE = 256 - sizeof(size_t); // 248 bytes, struct = 256 = 4 cache lines

struct alignas(64) CpuSerialBuf {
    size_t len = 0;
    char data[BUF_DATA_SIZE] = {};
};
static_assert(sizeof(CpuSerialBuf) == 256, "CpuSerialBuf must be 256 bytes");

static CpuSerialBuf cpu_bufs[MAX_CPUS];

static size_t get_buf_idx() {
    if (!cpu_id_available.load(std::memory_order_acquire)) {
        return 0; // during early boot all CPUs share buffer 0 (they run serially at that point)
    }
    return cpu::currentCpu() % MAX_CPUS;
}

static void flush_buf(size_t idx) {
    CpuSerialBuf& buf = cpu_bufs[idx];
    if (buf.len == 0) { return; }
    acquireLock();
    for (size_t i = 0; i < buf.len; i++) {
        write_char_unlocked(buf.data[i]);
    }
    releaseLock();
    buf.len = 0;
}

static void buf_char(char c) {
    size_t idx = get_buf_idx();
    CpuSerialBuf& buf = cpu_bufs[idx];
    buf.data[buf.len++] = c;
    if (c == '\n' || buf.len >= BUF_DATA_SIZE) {
        flush_buf(idx);
    }
}

void init() {
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
    for (size_t i = 0; str[i] != '\0'; i++) { buf_char(str[i]); }
}

void write(const char* str, uint64_t len) {
    for (size_t i = 0; i < len; i++) { buf_char(str[i]); }
}

void write(const char c) {
    buf_char(c);
}

void write(uint64_t num) {
    char str[21];
    str[20] = '\0';
    int pos = 20;
    if (num == 0) {
        str[--pos] = '0';
    } else {
        while (num > 0 && pos > 0) {
            str[--pos] = static_cast<char>('0' + (num % 10));
            num /= 10;
        }
    }
    write(str + pos);
}

void writeHex(uint64_t num) {
    char str[17];
    str[16] = '\0';
    const char* hex = "0123456789abcdef";
    for (int i = 0; i < 16; ++i) {
        str[15 - i] = hex[num & 0xF];
        num >>= 4;
    }
    int start = 0;
    while (start < 15 && str[start] == '0') { ++start; }
    write(str + start);
}

void writeBin(uint64_t num) {
    char str[65];
    str[64] = '\0';
    for (uint64_t i = 64; i > 0; i--) {
        str[64 - i] = ((num & (1ULL << (i - 1))) != 0U) ? '1' : '0';
    }
    write(str);
}

// Unlocked write variants - caller must hold lock
void writeUnlocked(const char* str) {
    for (size_t i = 0; str[i] != '\0'; i++) {
        write_char_unlocked(str[i]);
    }
}

void writeUnlocked(const char* str, uint64_t len) {
    for (size_t i = 0; i < len; i++) {
        write_char_unlocked(str[i]);
    }
}

void writeUnlocked(const char c) { write_char_unlocked(c); }

void writeUnlocked(uint64_t num) {
    char str[21];
    str[20] = '\0';
    int pos = 20;
    if (num == 0) {
        str[--pos] = '0';
    } else {
        while (num > 0 && pos > 0) {
            str[--pos] = static_cast<char>('0' + (num % 10));
            num /= 10;
        }
    }
    int len = 20 - pos;
    for (int i = 0; i < len; ++i) { str[i] = str[pos + i]; }
    str[len] = '\0';
    writeUnlocked(str);
}

void writeHexUnlocked(uint64_t num) {
    char str[17];
    str[16] = '\0';
    const char* hex = "0123456789abcdef";
    for (int i = 0; i < 16; ++i) {
        str[15 - i] = hex[num & 0xF];
        num >>= 4;
    }
    int start = 0;
    while (start < 15 && str[start] == '0') { ++start; }
    int len = 16 - start;
    for (int i = 0; i < len; ++i) { str[i] = str[start + i]; }
    str[len] = '\0';
    writeUnlocked(str);
}

void writeBinUnlocked(uint64_t num) {
    char str[65];
    str[64] = '\0';
    for (uint64_t i = 64; i > 0; i--) {
        str[64 - i] = ((num & (1ULL << (i - 1))) != 0U) ? '1' : '0';
    }
    writeUnlocked(str);
}
}  // namespace ker::mod::io::serial
