#include "spinlock.hpp"

#include <atomic>
#include <cstdint>
#include <platform/sched/scheduler.hpp>

#if SPINLOCK_DEBUG
// Number of pause iterations before emitting a warning.  Each pause is ~100
// cycles on modern x86, so 100M iterations ≈ ~10ms at 3.4GHz.
static constexpr uint64_t SPINLOCK_DEBUG_THRESHOLD = 100'000'000;
#endif

namespace ker::mod::sys {

namespace {

#if SPINLOCK_DEBUG
void print_hex(uint64_t val) {
    std::array<char, 17> hex{};
    for (int i = 15; i >= 0; --i) {
        uint8_t nibble = (val >> (i * 4)) & 0xF;
        hex[static_cast<size_t>(15 - i)] = nibble < 10 ? static_cast<char>('0' + nibble) : static_cast<char>('a' + nibble - 10);
    }
    hex[16] = '\0';
    io::serial::writeUnlocked(hex.data());
}

void print_uint(uint32_t val) {
    std::array<char, 11> buf{};
    int pos = 10;
    buf[pos] = '\0';
    if (val == 0) {
        buf[--pos] = '0';
    } else {
        while (val > 0) {
            buf[--pos] = static_cast<char>('0' + (val % 10));
            val /= 10;
        }
    }
    io::serial::writeUnlocked(buf.data() + pos);
}

// Walk the RBP chain from the given frame pointer and store return addresses.
// Stops at null/non-canonical frames or when depth is reached.
void walk_stack(void** fp, void** out, int depth) {
    for (int i = 0; i < depth; i++) {
        out[i] = nullptr;
        // Sanity-check: must be a kernel address
        if (fp == nullptr || reinterpret_cast<uint64_t>(fp) < 0xffff000000000000ULL) {
            break;
        }
        out[i] = *(fp + 1);  // return address is at [rbp+8]
        auto* next = reinterpret_cast<void**>(*fp);
        if (next <= fp) {
            break;  // stack grows down; must go to higher addresses
        }
        fp = next;
    }
}

void print_stack(void** frames, int depth, const char* label) {
    io::serial::writeUnlocked(label);
    io::serial::writeUnlocked("\n");
    for (int i = 0; i < depth; i++) {
        if (frames[i] == nullptr) {
            break;
        }
        io::serial::writeUnlocked("  #");
        print_uint(static_cast<uint32_t>(i));
        io::serial::writeUnlocked(" 0x");
        print_hex(reinterpret_cast<uint64_t>(frames[i]));
        io::serial::writeUnlocked("\n");
    }
}

#endif

void lock_ticket(Spinlock* lock) {
    // Take a ticket - this is our position in the FIFO queue.
    uint32_t const MY_TICKET = lock->next_ticket.fetch_add(1, std::memory_order_relaxed);

#if SPINLOCK_DEBUG
    uint64_t spins = 0;
    bool warned = false;
#endif

    // Spin until the lock is "serving" our ticket.
    while (lock->now_serving.load(std::memory_order_acquire) != MY_TICKET) {
        asm volatile("pause");
#if SPINLOCK_DEBUG
        if (!warned && ++spins >= SPINLOCK_DEBUG_THRESHOLD) {
            warned = true;

            io::serial::writeUnlocked("!!! SPINLOCK STUCK: waiter=0x");
            print_hex(reinterpret_cast<uint64_t>(__builtin_return_address(0)));
            io::serial::writeUnlocked(" owner=0x");
            print_hex(reinterpret_cast<uint64_t>(lock->owner_caller));
            io::serial::writeUnlocked("\n");

            // Waiter stack trace (current execution context)
            std::array<void*, Spinlock::SPINLOCK_STACK_DEPTH> waiter_frames{};
            auto* fp = reinterpret_cast<void**>(__builtin_frame_address(0));
            walk_stack(fp, waiter_frames.data(), Spinlock::SPINLOCK_STACK_DEPTH);
            print_stack(waiter_frames.data(), Spinlock::SPINLOCK_STACK_DEPTH, "== WAITER STACK ==");

            // Owner stack trace (saved at acquisition time)
            print_stack(lock->owner_stack.data(), Spinlock::SPINLOCK_STACK_DEPTH, "== OWNER STACK ==");
        }
#endif
    }

#if SPINLOCK_DEBUG
    // Record who acquired the lock.
    lock->owner_caller = __builtin_return_address(0);
    auto* fp = reinterpret_cast<void**>(__builtin_frame_address(0));
    walk_stack(fp, lock->owner_stack.data(), Spinlock::SPINLOCK_STACK_DEPTH);
#endif
}

auto try_lock_ticket(Spinlock* lock) -> bool {
    // Only succeed if no one else is waiting or holding the lock.
    uint32_t current = lock->now_serving.load(std::memory_order_relaxed);
    bool const ACQUIRED =
        lock->next_ticket.compare_exchange_strong(current, current + 1, std::memory_order_acquire, std::memory_order_relaxed);
#if SPINLOCK_DEBUG
    if (ACQUIRED) {
        lock->owner_caller = __builtin_return_address(0);
        auto* fp = reinterpret_cast<void**>(__builtin_frame_address(0));
        walk_stack(fp, lock->owner_stack.data(), Spinlock::SPINLOCK_STACK_DEPTH);
    }
#endif
    return ACQUIRED;
}

void unlock_ticket(Spinlock* lock) {
#if SPINLOCK_DEBUG
    lock->owner_caller = nullptr;
    lock->owner_stack.fill(nullptr);
#endif
    // Advance now_serving so the next ticket holder stops spinning.
    lock->now_serving.fetch_add(1, std::memory_order_release);
}

}  // namespace

void Spinlock::lock() {
    sched::preempt_disable_at(reinterpret_cast<uint64_t>(__builtin_return_address(0)));
    lock_ticket(this);
}

auto Spinlock::try_lock() -> bool {
    sched::preempt_disable_at(reinterpret_cast<uint64_t>(__builtin_return_address(0)));
    if (!try_lock_ticket(this)) {
        sched::preempt_enable_at(reinterpret_cast<uint64_t>(__builtin_return_address(0)));
        return false;
    }
    return true;
}

void Spinlock::unlock() {
    unlock_ticket(this);
    sched::preempt_enable_at(reinterpret_cast<uint64_t>(__builtin_return_address(0)));
}

auto Spinlock::lock_irqsave() -> uint64_t {
    // NOLINTNEXTLINE(misc-const-correctness)
    uint64_t flags = 0;
    asm volatile("pushfq; popq %0" : "=r"(flags));
    asm volatile("cli");
    sched::preempt_disable_at(reinterpret_cast<uint64_t>(__builtin_return_address(0)));
    lock_ticket(this);
    return flags;
}

void Spinlock::unlock_irqrestore(uint64_t flags) {
    unlock_ticket(this);
    if ((flags & 0x200) != 0) {
        asm volatile("sti");
    }
    sched::preempt_enable_at(reinterpret_cast<uint64_t>(__builtin_return_address(0)));
}

}  // namespace ker::mod::sys
