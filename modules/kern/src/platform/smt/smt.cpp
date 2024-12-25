#include "smt.hpp"

__attribute__((used, section(".requests"))) const static volatile limine_smp_request smp_request = {
    .id = LIMINE_SMP_REQUEST,
    .revision = 0,
    .response = nullptr,
    .flags = 1,
};
namespace ker::mod::smt {
static PerCpuCrossAccess<CpuInfo>* cpuData;
uint32_t flags;
uint32_t bsp_lapic_id;
uint64_t cpu_count;

uint64_t getCoreCount(void) { return cpu_count; }

const CpuInfo getCpu(uint64_t number) { return cpuData->thatCpu(number).copy(); }

const CpuInfo thisCpuInfo() { return cpuData->thisCpu().copy(); }

// Future NUMA support here
uint64_t getCpuNode(uint64_t cpuNo) { return cpuNo; }

void cpuParamInit() {
    apic::initApicMP();
    uint64_t cpuNo = apic::getApicId();

    cpuSetMSR(IA32_KERNEL_GS_BASE, (uint64_t)cpuData->thatCpu(cpuNo)->stack_pointer_ref - KERNEL_STACK_SIZE);

    cpu::setCurrentCpuid(cpuNo);
    desc::idt::idtInit();
    sched::percpuInit();
    auto idleTask = new sched::task::Task("idle", 0, 0, sched::task::TaskType::IDLE);
    sched::postTask(idleTask);
    sched::startScheduler();
}

void runHandoverTasks(boot::HandoverModules& modStruct, uint64_t kernelRsp) {
    sched::percpuInit();
    for (uint64_t i = 0; i < modStruct.count; i++) {
        auto newTask = new sched::task::Task(modStruct.modules[i].name, (uint64_t)modStruct.modules[i].entry, kernelRsp,
                                             sched::task::TaskType::PROCESS);
        sched::postTask(newTask);
    }
    dbg::log("Posted task for main thread");
    // for (uint64_t i = 1; i < smt::cpu_count; i++) {
    //     auto newCpuTask = new sched::task::Task(modStruct.modules[0].name, (uint64_t)modStruct.modules[0].entry, kernelRsp,
    //                                             sched::task::TaskType::PROCESS);
    //     sched::postTaskForCpu(i, newCpuTask);
    // }
    sched::startScheduler();
}

void init() {
    assert(smp_request.response != nullptr);
    flags = smp_request.response->flags;
    bsp_lapic_id = smp_request.response->bsp_lapic_id;
    cpu_count = smp_request.response->cpu_count;
}

// init per cpu data
__attribute__((noreturn)) void startSMT(boot::HandoverModules& modules, uint64_t kernelRsp) {
    assert(smp_request.response != nullptr);
    cpuSetMSR(IA32_KERNEL_GS_BASE, kernelRsp - KERNEL_STACK_SIZE);
    cpu::setCurrentCpuid(0);

    cpuData = new PerCpuCrossAccess<CpuInfo>();

    for (uint64_t i = 0; i < getCoreCount(); i++) {
        cpuData->thatCpu(i)->processor_id = smp_request.response->cpus[i]->processor_id;
        cpuData->thatCpu(i)->lapic_id = smp_request.response->cpus[i]->lapic_id;
        cpuData->thatCpu(i)->goto_address = &smp_request.response->cpus[i]->goto_address;
        cpuData->thatCpu(i)->stack_pointer_ref = (uint64_t*)(smp_request.response->cpus[i]->extra_argument);
    }
    for (uint64_t i = 1; i < smp_request.response->cpu_count; i++) {
        auto stack = mm::Stack();
        cpuData->thatCpu(i)->stack_pointer_ref = stack.sp;
        startCpuTask(i, reinterpret_cast<CpuGotoAddr>(cpuParamInit), stack);
    }
    runHandoverTasks(modules, kernelRsp);
    hcf();
}

uint64_t cpuCount() { return cpu_count; }

}  // namespace ker::mod::smt
