#pragma once
#include <limine.h>

#include <defines/defines.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/boot/handover.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/interrupt.hpp>
#include <platform/mm/mm.hpp>
#include <platform/sched/scheduler.hpp>
#include <std/borrows.hpp>
#include <std/cstdassert.hpp>
#include <std/mem.hpp>
#include <std/string.hpp>
#include <std/type_traits.hpp>

namespace ker::mod::smt {
constexpr static uint64_t SMT_MAX_CPUS = 256;

typedef void (*CpuGotoAddr)(struct limine_smp_info*);

struct CpuInfo {
    uint32_t processor_id;
    uint32_t lapic_id;
    CpuGotoAddr* goto_address = nullptr;
    uint64_t** stack_pointer_ref = nullptr;
};

uint64_t getCoreCount(void);
const CpuInfo& getCpu(uint64_t number);

__attribute__((noreturn)) void init(boot::HandoverModules& modules);

const CpuInfo& thisCpuInfo();

template <typename T>
class PerCpuVar {
    static_assert(std::is_default_constructible<T>::value || std::is_arithmetic<T>::value,
                  "T must have a default constructor or be a primitive type");

   private:
    T _data[SMT_MAX_CPUS];

   public:
    PerCpuVar(T defaultValue = T()) {
        for (uint64_t i = 0; i < SMT_MAX_CPUS; i++) {
            _data[i] = defaultValue;
        }
    }

    T& get() { return _data[thisCpuInfo().processor_id]; }

    T* operator->() { return &_data[thisCpuInfo().processor_id]; }

    void set(const T& data) { _data[thisCpuInfo().processor_id] = data; }

    T& operator=(const T& data) {
        _data[thisCpuInfo().processor_id] = data;
        return _data[thisCpuInfo().processor_id];
    }
};

template <typename T>
class PerCpuCrossAccess {
   private:
    std::Borrowable<T> _data[SMT_MAX_CPUS];

   public:
    PerCpuCrossAccess(T defaultValue = T()) {
        for (uint64_t i = 0; i < SMT_MAX_CPUS; i++) {
            _data[i] = defaultValue;
        }
    }

    std::Borrowable<T>::BorrowedRef thisCpu() { return _data[thisCpuInfo().processor_id].borrow(); }

    std::Borrowable<T>::BorrowedRef thatCpu(uint64_t cpu) { return _data[cpu].borrow(); }

    void setThisCpu(const T& data) { _data[thisCpuInfo().processor_id] = data; }

    void setThatCpu(const T& data, uint64_t cpu) { _data[cpu] = data; }
};

template <size_t StackSize = 4096, typename... FuncArgs>
void startCpuTask(uint64_t cpuNo, CpuGotoAddr task, mm::Stack<StackSize> stack, FuncArgs... data) {
    uint64_t getCpuNode(uint64_t cpuNo);

    auto cpu = getCpuNode(cpuNo);
    auto cpuData = getCpu(cpu);

    // push function arguments to the stack of the target CPU
    static_assert(sizeof...(data) < stack.size, "Stack size is too small for the data provided");
    *stack.sp = reinterpret_cast<uint64_t>(task);
    stack.sp++;
    std::multimemcpy(stack.sp, data...);
    stack.sp += sizeof...(data);

    __atomic_store_n(cpuData.stack_pointer_ref, stack.sp, __ATOMIC_SEQ_CST);
    __atomic_store_n(cpuData.goto_address, reinterpret_cast<CpuGotoAddr>(task), __ATOMIC_SEQ_CST);
}

template <typename... FuncArgs>
constexpr void execOnAllCpus(void (*func)(FuncArgs...), FuncArgs... data) {
    mm::Stack<4096>* initStacks;
    initStacks = new mm::Stack<4096>[getCoreCount()];
    for (uint64_t i = 0; i < getCoreCount(); i++) {
        if (i == thisCpuInfo().processor_id) {
            continue;
        }
        startCpuTask(i, reinterpret_cast<CpuGotoAddr>(func), initStacks[i], data...);
    }
}

}  // namespace ker::mod::smt
