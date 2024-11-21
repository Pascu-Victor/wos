#pragma once

namespace std {

enum memory_order {
    memory_order_relaxed,
    memory_order_consume,
    memory_order_acquire,
    memory_order_release,
    memory_order_acq_rel,
    memory_order_seq_cst
};

template <typename T>
class atomic {
   public:
    atomic() : value(0) {}
    atomic(T val) : value(val) {}

    T load(memory_order order = memory_order::memory_order_seq_cst) const { return __atomic_load_n(&value, order); }

    void store(T val, memory_order order = memory_order::memory_order_seq_cst) { __atomic_store_n(&value, val, order); }

    T exchange(T val, memory_order order = memory_order::memory_order_seq_cst) { return __atomic_exchange_n(&value, val, order); }

    bool compare_exchange_strong(T& expected, T desired, memory_order order = memory_order::memory_order_seq_cst) {
        return __atomic_compare_exchange_n(&value, &expected, desired, false, order, order);
    }

    T fetch_add(T arg, memory_order order = memory_order::memory_order_seq_cst) { return __atomic_fetch_add(&value, arg, order); }

    T fetch_sub(T arg, memory_order order = memory_order::memory_order_seq_cst) { return __atomic_fetch_sub(&value, arg, order); }

    atomic(const atomic&) = delete;
    atomic& operator=(const atomic&) = delete;

   private:
    volatile T value;
};

}  // namespace std
