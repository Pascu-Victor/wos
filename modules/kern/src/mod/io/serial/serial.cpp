#include "serial.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/asm/cpu.hpp>
#include <platform/mm/tlb_shootdown.hpp>
#include <platform/sched/scheduler.hpp>
#include <span>

#include "mod/io/port/port.hpp"

namespace ker::mod::io::serial {

namespace {
constexpr uint16_t DATA_PORT = 0x3F8;
constexpr uint16_t STATUS_PORT = DATA_PORT + 5;
constexpr uint8_t STATUS_TX_READY = 0x20;
constexpr uint8_t STATUS_TX_EMPTY = 0x40;
constexpr uint8_t FIFO_READY_MASK = 0xC0;
constexpr size_t TX_FIFO_SIZE = 16;
constexpr uint8_t OP_DISABLE_INTERRUPTS = 0x00;
bool is_init = false;
std::atomic<bool> tx_fifo_available{false};
// Reentrant serial lock: track the owning task when scheduler context exists,
// otherwise fall back to a tagged CPU id during very early boot.
constexpr uintptr_t NO_OWNER = 0;
constexpr uintptr_t FALLBACK_CPU_OWNER_TAG = 1;
std::atomic<uintptr_t> lock_owner{NO_OWNER};
std::atomic<uint64_t> lock_depth{0};
ker::mod::sched::task::Task* lock_preempt_owner = nullptr;
std::atomic<bool> cpu_id_available{false};

// Set to true once any CPU enters panic mode.
std::atomic<bool> in_panic_mode{false};
// CPU index of the first CPU to call enterPanicMode().
constexpr uint64_t NO_PANIC_OWNER = UINT64_MAX;
std::atomic<uint64_t> panic_owner_cpu{NO_PANIC_OWNER};

auto current_lock_owner() -> uintptr_t {
    if (ker::mod::sched::can_query_current_task()) {
        if (auto* task = ker::mod::sched::get_current_task(); task != nullptr) {
            return reinterpret_cast<uintptr_t>(task);
        }
    }

    uint64_t const CPU = cpu_id_available.load(std::memory_order_acquire) ? cpu::current_cpu() : 0;
    return (static_cast<uintptr_t>(CPU) << 1U) | FALLBACK_CPU_OWNER_TAG;
}

auto is_task_owner(uintptr_t owner) -> bool { return owner != NO_OWNER && (owner & FALLBACK_CPU_OWNER_TAG) == 0; }
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

    uintptr_t const OWNER = current_lock_owner();

    // If we already own the lock, just increment depth (reentrant).
    if (lock_owner.load(std::memory_order_relaxed) == OWNER) {
        lock_depth.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    // Prevent the owning task from being preempted while another CPU spins on
    // the serial lock. Early boot fallback owners do not have scheduler state.
    auto* const PREEMPT_OWNER =
        is_task_owner(OWNER) ? ker::mod::sched::preempt_disable_token_at(reinterpret_cast<uint64_t>(__builtin_return_address(0))) : nullptr;

    // Spin until we can acquire. If an interrupt on this CPU re-entered the
    // lock after we disabled preemption but before the CAS below, treat that as
    // a same-owner recursive acquisition instead of taking the lock twice.
    uintptr_t expected = NO_OWNER;
    uint32_t service_spins = 0;
    while (!lock_owner.compare_exchange_weak(expected, OWNER, std::memory_order_acquire, std::memory_order_relaxed)) {
        if (expected == OWNER) {
            if (PREEMPT_OWNER != nullptr) {
                ker::mod::sched::preempt_enable_token_at(PREEMPT_OWNER, reinterpret_cast<uint64_t>(__builtin_return_address(0)));
            }
            lock_depth.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        expected = NO_OWNER;
        ++service_spins;
        if ((service_spins & 0xFFU) == 0U && !ker::mod::sched::interrupts_enabled()) {
            ker::mod::mm::virt::service_pending_tlb_shootdowns();
        }
        asm volatile("pause" ::: "memory");
    }
    lock_preempt_owner = PREEMPT_OWNER;
    lock_depth.store(1, std::memory_order_relaxed);
}

void release_lock() {
    // In panic mode all locking is a no-op.
    if (in_panic_mode.load(std::memory_order_acquire)) {
        return;
    }

    uint64_t const DEPTH = lock_depth.fetch_sub(1, std::memory_order_relaxed);
    if (DEPTH == 1) {
        auto* const PREEMPT_OWNER = lock_preempt_owner;
        lock_preempt_owner = nullptr;
        lock_owner.store(NO_OWNER, std::memory_order_release);
        if (PREEMPT_OWNER != nullptr) {
            ker::mod::sched::preempt_enable_token_at(PREEMPT_OWNER, reinterpret_cast<uint64_t>(__builtin_return_address(0)));
        }
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

auto disable_tx_interrupts() -> uint64_t {
    // Written by inline asm output constraints.
    // NOLINTNEXTLINE(misc-const-correctness)
    uint64_t flags = 0;
    asm volatile("pushfq; pop %0; cli" : "=r"(flags)::"memory");
    return flags;
}

void restore_tx_interrupts(uint64_t flags) {
    if ((flags & cpu::GATE_IF_MASK) != 0) {
        asm volatile("sti" ::: "memory");
    }
}

void write_fifo_bytes(const char* bytes, size_t count) {
    // Some KVM string-I/O paths emulate OUTSB one byte at a time. Ordinary
    // OUTB avoids that emulation while retaining one ready check per burst.
    for (char const VALUE : std::span<const char>(bytes, count)) {
        outb(DATA_PORT, static_cast<uint8_t>(VALUE));
    }
}

void write_bytes_unlocked(const char* bytes, uint64_t len) {
    if (len == 0) {
        return;
    }
    std::span<const char> remaining(bytes, static_cast<size_t>(len));
    if (!tx_fifo_available.load(std::memory_order_acquire) || is_panic_mode()) {
        for (char const VALUE : remaining) {
            write_char_unlocked(VALUE);
        }
        return;
    }

    while (!remaining.empty()) {
        // The serial lock is reentrant for interrupt logging on its owner.
        // Mask IRQs across the empty check and burst so a nested write cannot
        // consume the FIFO capacity that this burst is about to use.
        uint64_t const FLAGS = disable_tx_interrupts();
        if ((inb(STATUS_PORT) & STATUS_TX_READY) == 0) {
            restore_tx_interrupts(FLAGS);
            asm volatile("pause" ::: "memory");
            continue;
        }
        size_t const COUNT = std::min(remaining.size(), TX_FIFO_SIZE);
        write_fifo_bytes(remaining.data(), COUNT);
        remaining = remaining.subspan(COUNT);
        restore_tx_interrupts(FLAGS);
    }
    // Do not leave a final FIFO burst queued beyond the write call. As with
    // character writes, only a character in the shift register may remain.
    while ((inb(STATUS_PORT) & STATUS_TX_READY) == 0) {
        asm volatile("pause" ::: "memory");
    }
}
}  // namespace

void init() {
    if (is_init) {
        return;
    }
    constexpr uint8_t OP_ENABLE_DLAB = 0x80;
    constexpr uint8_t DIVISOR_LO = 0x01;  // 115200 baud
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
    tx_fifo_available.store((inb(DATA_PORT + 2) & FIFO_READY_MASK) == FIFO_READY_MASK, std::memory_order_release);
    is_init = true;
}

void write(const char* str) {
    ScopedLock const LOCK;
    write_unlocked(str);
}

void write(const char* str, uint64_t len) {
    ScopedLock const LOCK;
    write_unlocked(str, len);
}

void write(const char C) {
    ScopedLock const LOCK;
    write_char_unlocked(C);
}

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
    ScopedLock const LOCK;
    write_unlocked(str.data() + pos);
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
    ScopedLock const LOCK;
    write_unlocked(str.data() + start);
}

void write_bin(uint64_t num) {
    constexpr size_t BUF_SIZE = 65;  // 64 binary digits for uint64_t + null terminator
    std::array<char, BUF_SIZE> str{};
    str[BUF_SIZE - 1] = '\0';
    for (uint64_t i = BUF_SIZE - 1; i > 0; i--) {
        str[static_cast<size_t>(BUF_SIZE - 2 - (i - 1))] = ((num & (1ULL << (i - 1))) != 0U) ? '1' : '0';
    }
    ScopedLock const LOCK;
    write_unlocked(str.data());
}

// Unlocked write variants - caller must hold lock
void write_unlocked(const char* str) { write_bytes_unlocked(str, std::strlen(str)); }

void write_unlocked(const char* str, uint64_t len) { write_bytes_unlocked(str, len); }

void write_unlocked(const char C) { write_char_unlocked(C); }

void flush_unlocked() {
    if (!tx_fifo_available.load(std::memory_order_acquire) || is_panic_mode()) {
        return;
    }
    while ((inb(STATUS_PORT) & STATUS_TX_EMPTY) == 0) {
        asm volatile("pause" ::: "memory");
    }
}

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
