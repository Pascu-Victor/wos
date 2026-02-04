#pragma once
#include <limine.h>

#include <atomic>
#include <cstdint>
#include <defines/defines.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/boot/handover.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/interrupt.hpp>
#include <platform/mm/mm.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <type_traits>

namespace ker::mod::smt {

using CpuGotoAddr = void (*)(struct limine_smp_info*);

struct CpuInfo {
    uint32_t processor_id;
    uint32_t lapic_id;
    CpuGotoAddr* goto_address = nullptr;
    uint64_t* stack_pointer_ref = nullptr;
    sched::task::Task* currentTask = nullptr;
    // Used by panic/OOM to indicate this CPU has entered the halted state for diagnostics
    std::atomic<bool> isHaltedForOom{false};
};

auto getCoreCount() -> uint64_t;
auto getCpu(uint64_t number) -> CpuInfo&;

// Get logical CPU index from APIC ID (reliable, doesn't depend on GS)
auto getCpuIndexFromApicId(uint32_t apicId) -> uint64_t;

// Get the kernel PerCpu structure for a given CPU index
// Used to restore GS_BASE when entering idle loop (no task context)
cpu::PerCpu* getKernelPerCpu(uint64_t cpuIndex);

void start_smt(boot::HandoverModules& modules, uint64_t kernel_rsp);

void init();

auto thisCpuInfo() -> const CpuInfo&;

template <typename T>
class PerCpuVar {
    static_assert(std::is_default_constructible_v<T> || std::is_arithmetic_v<T>,
                  "T must have a default constructor or be a primitive type");

   private:
    T* _data;

   public:
    PerCpuVar(T defaultValue = T()) : _data(new T[getCoreCount()]) {
        for (uint64_t i = 0; i < getCoreCount(); ++i) {
            _data[i] = defaultValue;
        }
    }

    T& get() { return _data[cpu::currentCpu()]; }

    auto operator->() -> T* { return &_data[cpu::currentCpu()]; }

    void set(const T& data) { _data[cpu::currentCpu()] = data; }

    auto operator=(const T& data) -> PerCpuVar& {
        _data[cpu::currentCpu()] = data;
        return *this;
    }
};

template <typename T>
class PerCpuCrossAccess {
   private:
    T* _data;

    // Per-element spinlocks for fine-grained locking
    std::atomic<bool>* _locks;

    void lockCpu(uint64_t cpu) {
        while (_locks[cpu].exchange(true, std::memory_order_acquire)) {
            // Spin with pause hint
            asm volatile("pause");
        }
    }

    void unlockCpu(uint64_t cpu) { _locks[cpu].store(false, std::memory_order_release); }

   public:
    PerCpuCrossAccess() : _data(new T[getCoreCount()]), _locks(new std::atomic<bool>[getCoreCount()]) {
        for (uint64_t i = 0; i < getCoreCount(); ++i) {
            new (&_data[i]) T();  // Default construct each element (supports non-copyable types like std::atomic)
            _locks[i].store(false, std::memory_order_relaxed);
        }
    }

    // Access current CPU's data (no locking needed - only this CPU accesses it)
    // WARNING: Use thisCpuLocked() if other CPUs might be modifying via withLock!
    auto thisCpu() -> T* { return &_data[cpu::currentCpu()]; }

    // Locked access to current CPU's data - use when other CPUs might modify via withLock
    template <typename Func>
    auto thisCpuLocked(Func&& func) -> decltype(func(std::declval<T*>())) {
        uint64_t cpu = cpu::currentCpu();
        lockCpu(cpu);
        auto result = func(&_data[cpu]);
        unlockCpu(cpu);
        return result;
    }

    // Locked access to current CPU's data without return value
    template <typename Func>
    void thisCpuLockedVoid(Func&& func) {
        uint64_t cpu = cpu::currentCpu();
        lockCpu(cpu);
        func(&_data[cpu]);
        unlockCpu(cpu);
    }

    // Access another CPU's data with locking
    auto thatCpu(uint64_t cpu) -> T* { return &_data[cpu]; }

    // Locked access to another CPU's data - use when modifying cross-CPU
    template <typename Func>
    auto withLock(uint64_t cpu, Func&& func) -> decltype(func(std::declval<T*>())) {
        lockCpu(cpu);
        auto result = func(&_data[cpu]);
        unlockCpu(cpu);
        return result;
    }

    // Locked access without return value
    template <typename Func>
    void withLockVoid(uint64_t cpu, Func&& func) {
        lockCpu(cpu);
        func(&_data[cpu]);
        unlockCpu(cpu);
    }

    void setThisCpu(const T& data) { _data[cpu::currentCpu()] = data; }

    void setThatCpu(const T& data, uint64_t cpu) {
        lockCpu(cpu);
        _data[cpu] = data;
        unlockCpu(cpu);
    }
};

auto getCpuNode(uint64_t cpuNo) -> uint64_t;

inline void startCpuTask(uint64_t cpuNo, CpuGotoAddr task, mm::Stack<4096> stack) {
    auto cpu = getCpuNode(cpuNo);
    auto& cpuData = getCpu(cpu);

    // push function arguments to the stack of the target CPU
    *stack.sp = reinterpret_cast<uint64_t>(task);
    stack.sp++;

    __atomic_store_n(&cpuData.stack_pointer_ref, stack.sp, __ATOMIC_SEQ_CST);
    __atomic_store_n(cpuData.goto_address, task, __ATOMIC_SEQ_CST);
}

// Overload accepting a CpuGotoAddr-compatible function (takes limine_smp_info*).
constexpr void execOnAllCpus(CpuGotoAddr func) {
    auto* initStacks = new mm::Stack<4096>[getCoreCount()];
    for (uint64_t i = 0; i < getCoreCount(); i++) {
        if (i == cpu::currentCpu()) {
            continue;
        }
        startCpuTask(i, func, initStacks[i]);
    }
}

template <typename... FuncArgs>
constexpr void execOnAllCpus(void (*func)(FuncArgs...), FuncArgs... data) {
    auto* initStacks = new mm::Stack<4096>[getCoreCount()];
    for (uint64_t i = 0; i < getCoreCount(); i++) {
        if (i == cpu::currentCpu()) {
            continue;
        }
        startCpuTask(i, reinterpret_cast<CpuGotoAddr>(func), initStacks[i], data...);
    }
}

auto cpuCount() -> uint64_t;

auto setTcb(void* tcb) -> uint64_t;

// Halt all other CPUs immediately by scheduling a HLT loop on them.
// This is safe to call from panic/OOM paths where we want other cores
// quiesced to avoid further concurrent access to global state.
void haltOtherCores();

// C-linkage wrapper used by external C code to request halting other CPUs.
extern "C" void ker_smt_halt_other_cpus(void);

}  // namespace ker::mod::smt
