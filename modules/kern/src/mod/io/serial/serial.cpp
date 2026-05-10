#include "serial.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/asm/cpu.hpp>

#include "mod/io/port/port.hpp"

namespace ker::mod::io::serial {

namespace {
constexpr uint16_t DATA_PORT = 0x3F8;
constexpr uint16_t STATUS_PORT = DATA_PORT + 5;
constexpr uint8_t STATUS_TX_READY = 0x20;
constexpr uint8_t OP_DISABLE_INTERRUPTS = 0x00;
bool is_init = false;
// Reentrant spinlock: tracks owner CPU and recursion depth
// Use UINT64_MAX for "no owner"
constexpr uint64_t NO_OWNER = UINT64_MAX;
std::atomic<uint64_t> lock_owner{NO_OWNER};
std::atomic<uint64_t> lock_depth{0};
std::atomic<bool> cpu_id_available{false};

// Set to true once any CPU enters panic mode.
std::atomic<bool> in_panic_mode{false};
// CPU index of the first CPU to call enterPanicMode().
constexpr uint64_t NO_PANIC_OWNER = UINT64_MAX;
std::atomic<uint64_t> panic_owner_cpu{NO_PANIC_OWNER};
}  // namespace

void mark_cpu_id_available() { cpu_id_available.store(true, std::memory_order_release); }

// Returns true if this CPU is the first to enter panic mode (becomes the panic owner).
// Returns false for every subsequent caller.
bool enter_panic_mode() {
    in_panic_mode.store(true, std::memory_order_release);
    uint64_t expected = NO_PANIC_OWNER;
    constexpr uint64_t ADDR_MASK = 0xFFFFU;
    auto fallback = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&expected) & ADDR_MASK);
    uint64_t const CPU_ID = cpu_id_available.load(std::memory_order_acquire) ? cpu::current_cpu() : fallback;
    return panic_owner_cpu.compare_exchange_strong(expected, CPU_ID, std::memory_order_acq_rel, std::memory_order_acquire);
}

bool is_panic_mode() { return in_panic_mode.load(std::memory_order_acquire); }

bool is_panic_owner() {
    if (!in_panic_mode.load(std::memory_order_acquire)) {
        return false;
    }
    uint64_t const OWNER = panic_owner_cpu.load(std::memory_order_acquire);
    if (OWNER == NO_PANIC_OWNER) {
        return false;
    }
    if (!cpu_id_available.load(std::memory_order_acquire)) {
        // Very early boot, single CPU — assume this CPU is the owner.
        return true;
    }
    return cpu::current_cpu() == OWNER;
}

void acquire_lock() {
    // In panic mode all locking is a no-op: the panic owner holds the normal
    // lock before calling enterPanicMode(), so other CPUs must not spin on it.
    if (in_panic_mode.load(std::memory_order_acquire)) {
        return;
    }

    uint64_t const CPU = cpu_id_available.load(std::memory_order_acquire) ? cpu::current_cpu() : 0;

    // If we already own the lock, just increment depth (reentrant)
    if (lock_owner.load(std::memory_order_relaxed) == CPU) {
        lock_depth.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    // Spin until we can acquire
    uint64_t expected = NO_OWNER;
    while (!lock_owner.compare_exchange_weak(expected, CPU, std::memory_order_acquire, std::memory_order_relaxed)) {
        expected = NO_OWNER;
        asm volatile("pause");
    }
    lock_depth.store(1, std::memory_order_relaxed);
}

void release_lock() {
    // In panic mode all locking is a no-op.
    if (in_panic_mode.load(std::memory_order_acquire)) {
        return;
    }

    uint64_t const DEPTH = lock_depth.fetch_sub(1, std::memory_order_relaxed);
    if (DEPTH == 1) {
        lock_owner.store(NO_OWNER, std::memory_order_release);
    }
}

namespace {
// Internal unlocked character write
void write_char_unlocked(char c) {
    while ((inb(STATUS_PORT) & STATUS_TX_READY) == 0) {
        ;
    }
    outb(DATA_PORT, c);
}

// Per-CPU line buffers: each CPU accumulates characters here and only takes
// the serial lock when flushing a complete line or on overflow. This prevents
// characters from different CPUs interleaving within a single output line.
constexpr size_t MAX_CPUS = 256;
constexpr size_t BUF_DATA_SIZE = 256 - sizeof(size_t);  // 248 bytes, struct = 256 = 4 cache lines
constexpr size_t SERIAL_BUF_ALIGNMENT = 256;            // Align to cache line size to avoid false sharing

struct alignas(SERIAL_BUF_ALIGNMENT) CpuSerialBuf {
    size_t len = 0;
    std::array<char, BUF_DATA_SIZE> data{};
};
static_assert(sizeof(CpuSerialBuf) == SERIAL_BUF_ALIGNMENT, "CpuSerialBuf must be 256 bytes");

std::array<CpuSerialBuf, MAX_CPUS> cpu_bufs{};

auto get_buf_idx() -> size_t {
    if (!cpu_id_available.load(std::memory_order_acquire)) {
        return 0;  // during early boot all CPUs share buffer 0 (they run serially at that point)
    }
    return cpu::current_cpu() % MAX_CPUS;
}

void flush_buf(size_t idx) {
    CpuSerialBuf& buf = cpu_bufs[idx];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    if (buf.len == 0) {
        return;
    }
    acquire_lock();
    for (size_t i = 0; i < buf.len; i++) {
        write_char_unlocked(buf.data[i]);  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    }
    release_lock();
    buf.len = 0;
}

void buf_char(char c) {
    size_t const IDX = get_buf_idx();
    CpuSerialBuf& buf = cpu_bufs[IDX];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    buf.data[buf.len++] = c;            // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    if (c == '\n' || buf.len >= buf.data.size()) {
        flush_buf(IDX);
    }
}
}  // namespace

void init() {
    if (is_init) {
        return;
    }
    constexpr uint8_t OP_ENABLE_DLAB = 0x80;
    constexpr uint8_t DIVISOR_LO = 0x03;  // 38400 baud
    constexpr uint8_t DIVISOR_HI = 0x00;
    constexpr uint8_t LINE_CTRL_8N1 = 0x03;
    constexpr uint8_t FIFO_CTRL_ENABLE = 0xC7;
    constexpr uint8_t MODEM_CTRL = 0x0B;

    outb(DATA_PORT + 1, OP_DISABLE_INTERRUPTS);
    outb(DATA_PORT + 3, OP_ENABLE_DLAB);
    outb(DATA_PORT + 0, DIVISOR_LO);
    outb(DATA_PORT + 1, DIVISOR_HI);
    outb(DATA_PORT + 3, LINE_CTRL_8N1);
    outb(DATA_PORT + 2, FIFO_CTRL_ENABLE);
    outb(DATA_PORT + 4, MODEM_CTRL);
    is_init = true;
}

void write(const char* str) {
    for (size_t i = 0; str[i] != '\0'; i++) {
        buf_char(str[i]);
    }
}

void write(const char* str, uint64_t len) {
    for (size_t i = 0; i < len; i++) {
        buf_char(str[i]);
    }
}

void write(const char C) { buf_char(C); }

// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access): fixed-size numeric formatting buffers.
void write(uint64_t num) {
    constexpr size_t BUF_SIZE = 21;  // Max uint64_t is 20 digits + null terminator
    std::array<char, BUF_SIZE> str{};
    constexpr uint64_t BASE = 10;
    str[BUF_SIZE - 1] = '\0';
    size_t pos = BUF_SIZE - 1;
    if (num == 0) {
        str[--pos] = '0';
    } else {
        while (num > 0 && pos > 0) {
            str[--pos] = static_cast<char>('0' + (num % BASE));
            num /= BASE;
        }
    }
    write(str.data() + pos);
}

void write_hex(uint64_t num) {
    constexpr size_t BUF_SIZE = 17;  // 16 hex digits for uint64_t + null terminator
    std::array<char, BUF_SIZE> str{};
    str[BUF_SIZE - 1] = '\0';
    constexpr std::array<char, 16> HEX_DIGITS{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    for (size_t i = 0; i < BUF_SIZE - 1; ++i) {
        constexpr uint64_t NIBBLE_MASK = 0xFU;
        str[(BUF_SIZE - 2 - i)] = HEX_DIGITS[static_cast<size_t>(num & NIBBLE_MASK)];
        num >>= 4;
    }
    size_t start = 0;
    while (start < BUF_SIZE - 1 && str[start] == '0') {
        ++start;
    }
    write(str.data() + start);
}

void write_bin(uint64_t num) {
    constexpr size_t BUF_SIZE = 65;  // 64 binary digits for uint64_t + null terminator
    std::array<char, BUF_SIZE> str{};
    str[BUF_SIZE - 1] = '\0';
    for (uint64_t i = BUF_SIZE - 1; i > 0; i--) {
        str[static_cast<size_t>(BUF_SIZE - 2 - (i - 1))] = ((num & (1ULL << (i - 1))) != 0U) ? '1' : '0';
    }
    write(str.data());
}

// Unlocked write variants - caller must hold lock
void write_unlocked(const char* str) {
    for (size_t i = 0; str[i] != '\0'; i++) {
        write_char_unlocked(str[i]);
    }
}

void write_unlocked(const char* str, uint64_t len) {
    for (size_t i = 0; i < len; i++) {
        write_char_unlocked(str[i]);
    }
}

void write_unlocked(const char C) { write_char_unlocked(C); }

void write_unlocked(uint64_t num) {
    constexpr size_t BUF_SIZE = 21;  // Max uint64_t is 20 digits + null terminator
    std::array<char, BUF_SIZE> str{};
    constexpr uint64_t BASE = 10;
    str[BUF_SIZE - 1] = '\0';
    size_t pos = BUF_SIZE - 1;
    if (num == 0) {
        str[--pos] = '0';
    } else {
        while (num > 0 && pos > 0) {
            str[--pos] = static_cast<char>('0' + (num % BASE));
            num /= BASE;
        }
    }
    size_t const LEN = BUF_SIZE - 1 - pos;
    for (size_t i = 0; i < LEN; ++i) {
        str[i] = str[pos + i];
    }
    str[LEN] = '\0';
    write_unlocked(str.data());
}

void write_hex_unlocked(uint64_t num) {
    constexpr size_t BUF_SIZE = 17;  // 16 hex digits for uint64_t + null terminator
    std::array<char, BUF_SIZE> str{};
    str[BUF_SIZE - 1] = '\0';
    constexpr std::array<char, 16> HEX_DIGITS{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    constexpr uint64_t NIBBLE_MASK = 0xFU;
    for (size_t i = 0; i < BUF_SIZE - 1; ++i) {
        str[(BUF_SIZE - 2 - i)] = HEX_DIGITS[static_cast<size_t>(num & NIBBLE_MASK)];
        num >>= 4;
    }
    size_t start = 0;
    while (start < BUF_SIZE - 1 && str[start] == '0') {
        ++start;
    }
    size_t const LEN = BUF_SIZE - 1 - start;
    for (size_t i = 0; i < LEN; ++i) {
        str[i] = str[start + i];
    }
    str[LEN] = '\0';
    write_unlocked(str.data());
}

void write_bin_unlocked(uint64_t num) {
    constexpr size_t BUF_SIZE = 65;  // 64 binary digits for uint64_t + null terminator
    std::array<char, BUF_SIZE> str{};
    str[BUF_SIZE - 1] = '\0';
    for (uint64_t i = BUF_SIZE - 1; i > 0; i--) {
        str[static_cast<size_t>(BUF_SIZE - 2 - (i - 1))] = ((num & (1ULL << (i - 1))) != 0U) ? '1' : '0';
    }
    write_unlocked(str.data());
}
// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
}  // namespace ker::mod::io::serial
