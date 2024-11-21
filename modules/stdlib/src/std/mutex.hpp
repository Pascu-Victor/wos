#pragma once
#include <abi/interfaces/futex.int.hpp>
#include <abi/syscall.hpp>
#include <platform/sys/spinlock.hpp>
#include <std/atomic.hpp>

namespace std {
struct adopt_lock_t {
    explicit adopt_lock_t() = default;
};

constexpr adopt_lock_t adopt_lock{};

template <typename T>
class lock_guard {
   public:
    typedef T mutex_type;

    [[__nodiscard__]]
    explicit lock_guard(mutex_type& __m)
        : _M_device(__m) {
        _M_device.lock();
    }

    [[__nodiscard__]]
    lock_guard(mutex_type& __m, adopt_lock_t) noexcept
        : _M_device(__m) {}  // calling thread owns mutex

    ~lock_guard() { _M_device.unlock(); }

    lock_guard(const lock_guard&) = delete;
    lock_guard& operator=(const lock_guard&) = delete;

   private:
    mutex_type& _M_device;
};

class mutex {
   public:
    mutex() : m_futex(0) {}

    ~mutex() = default;

    void lock() {
        uint64_t expected = 0;
        // Try to acquire the lock by setting m_futex to 1
        if (!m_futex.compare_exchange_strong(expected, 1, std::memory_order_acquire)) {
            // If the lock is already held, wait for it to be released
            while (true) {
                if (expected == 2 || m_futex.compare_exchange_strong(expected, 2, std::memory_order_acquire)) {
                    ker::abi::syscall(ker::abi::callnums::futex, (uint64_t*)&m_futex,
                                      (uint64_t*)ker::abi::inter::futex::futex_ops::futex_wait, (uint64_t*)2, nullptr, nullptr, 0);
                    expected = 0;
                }
                if (m_futex.compare_exchange_strong(expected, 2, std::memory_order_acquire)) {
                    break;
                }
            }
        }
    }

    void unlock() {
        if (m_futex.fetch_sub(1, std::memory_order_release) != 1) {
            m_futex.store(0, std::memory_order_release);
            ker::abi::syscall(ker::abi::callnums::futex, (uint64_t*)&m_futex, (uint64_t*)ker::abi::inter::futex::futex_ops::futex_wake,
                              (uint64_t*)1, nullptr, nullptr, 0);
        }
    }

    bool try_lock() {
        uint64_t expected = 0;
        return m_futex.compare_exchange_strong(expected, 1, std::memory_order_acquire);
    }

    mutex(const mutex&) = delete;
    mutex& operator=(const mutex&) = delete;

   private:
    std::atomic<uint64_t> m_futex;
};

}  // namespace std
