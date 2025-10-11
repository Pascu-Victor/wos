#pragma once
#include <limine.h>

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

typedef void (*CpuGotoAddr)(struct limine_smp_info*);

struct CpuInfo {
    uint32_t processor_id;
    uint32_t lapic_id;
    CpuGotoAddr* goto_address = nullptr;
    uint64_t* stack_pointer_ref = nullptr;
    sched::task::Task* currentTask = nullptr;
};

uint64_t getCoreCount(void);
const CpuInfo getCpu(uint64_t number);

__attribute__((noreturn)) void startSMT(boot::HandoverModules& modules, uint64_t kernelRsp);

void init();

const CpuInfo thisCpuInfo();

template <typename T>
class PerCpuVar {
    static_assert(std::is_default_constructible<T>::value || std::is_arithmetic<T>::value,
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

    T* operator->() { return &_data[cpu::currentCpu()]; }

    void set(const T& data) { _data[cpu::currentCpu()] = data; }

    T& operator=(const T& data) {
        _data[cpu::currentCpu()] = data;
        return _data[cpu::currentCpu()];
    }
};

template <typename T>
class PerCpuCrossAccess {
   private:
    T* _data;

   public:
    PerCpuCrossAccess(T defaultValue = T()) {
        _data = new T[getCoreCount()];
        for (uint64_t i = 0; i < getCoreCount(); ++i) _data[i] = defaultValue;
    }

    T* thisCpu() { return &_data[cpu::currentCpu()]; }

    T* thatCpu(uint64_t cpu) { return &_data[cpu]; }

    void setThisCpu(const T& data) { _data[cpu::currentCpu()] = data; }

    void setThatCpu(const T& data, uint64_t cpu) { _data[cpu] = data; }
};

inline void startCpuTask(uint64_t cpuNo, CpuGotoAddr task, mm::Stack<4096> stack) {
    uint64_t getCpuNode(uint64_t cpuNo);

    auto cpu = getCpuNode(cpuNo);
    auto cpuData = getCpu(cpu);

    // push function arguments to the stack of the target CPU
    *stack.sp = reinterpret_cast<uint64_t>(task);
    stack.sp++;

    __atomic_store_n(&cpuData.stack_pointer_ref, stack.sp, __ATOMIC_SEQ_CST);
    __atomic_store_n(cpuData.goto_address, reinterpret_cast<CpuGotoAddr>(task), __ATOMIC_SEQ_CST);
}

template <typename... FuncArgs>
constexpr void execOnAllCpus(void (*func)(FuncArgs...), FuncArgs... data) {
    mm::Stack<4096>* initStacks;
    initStacks = new mm::Stack<4096>[getCoreCount()];
    for (uint64_t i = 0; i < getCoreCount(); i++) {
        if (i == cpu::currentCpu()) {
            continue;
        }
        startCpuTask(i, reinterpret_cast<CpuGotoAddr>(func), initStacks[i], data...);
    }
}

uint64_t cpuCount();

uint64_t setTcb(void* tcb);

}  // namespace ker::mod::smt
