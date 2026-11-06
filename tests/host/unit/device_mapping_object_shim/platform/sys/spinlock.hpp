#pragma once
#include <atomic>
#include <object_pages.hpp>

namespace ker::mod::sys {
struct Spinlock {
    void lock() {
        object_fixture::check(owner.load(std::memory_order_relaxed) != std::this_thread::get_id());
        mutex.lock();
        owner.store(std::this_thread::get_id(), std::memory_order_relaxed);
        ++object_fixture::lock_depth;
    }
    void unlock() {
        object_fixture::check(owner.load(std::memory_order_relaxed) == std::this_thread::get_id() && object_fixture::lock_depth != 0);
        --object_fixture::lock_depth;
        owner.store({}, std::memory_order_relaxed);
        mutex.unlock();
    }
    auto lock_irqsave() -> uint64_t {
        auto const previous = object_fixture::irq_depth++;
        lock();
        return previous;
    }
    void unlock_irqrestore(uint64_t previous) {
        unlock();
        object_fixture::irq_depth = static_cast<unsigned>(previous);
    }
    std::mutex mutex;
    std::atomic<std::thread::id> owner;
};
}  // namespace ker::mod::sys
