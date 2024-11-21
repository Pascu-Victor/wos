#include "smt.hpp"

__attribute__((used, section(".requests"))) const static volatile limine_smp_request smp_request = {
    .id = LIMINE_SMP_REQUEST,
    .revision = 0,
    .response = nullptr,
    .flags = 1,
};
namespace ker::mod::smt {
CpuInfo _wOS_CPUDATA[SMT_MAX_CPUS];
uint64_t revision;
uint32_t flags;
uint32_t bsp_lapic_id;
uint64_t cpu_count;

uint64_t getCoreCount(void) { return cpu_count; }

const CpuInfo& getCpu(uint64_t number) { return _wOS_CPUDATA[number]; }

const CpuInfo& thisCpuInfo() { return _wOS_CPUDATA[cpu::currentCpu()]; }

// Future NUMA support here
uint64_t getCpuNode(uint64_t cpuNo) { return cpuNo; }

void park() {
    while (true) {
        asm volatile("hlt");
    }
}

void cpuParamInit(uint64_t* stack) {
    (void)stack;
    // ker::mod::desc::gdt::initDescriptors((uint64_t)stack + sizeof((uint64_t)stack));
    apic::init();
    desc::idt::idtInit();

    // auto firstTask = new sched::task::Task("park", (uint64_t)&park, sched::task::TaskType::DAEMON);
    sched::percpuInit();
    // sched::postTask(firstTask);
    park();
    sched::startScheduler();
}

void runHandoverTasks(boot::HandoverModules& modStruct) {
    sched::percpuInit();
    for (uint64_t i = 0; i < modStruct.count; i++) {
        auto newTask =
            new sched::task::Task(modStruct.modules[i].name, (uint64_t)modStruct.modules[i].entry, sched::task::TaskType::PROCESS);
        sched::postTask(newTask);
    }
    sched::startScheduler();
}

// init per cpu data
__attribute__((noreturn)) void init(boot::HandoverModules& modules) {
    revision = smp_request.revision;
    assert(smp_request.response != nullptr);
    flags = smp_request.response->flags;
    bsp_lapic_id = smp_request.response->bsp_lapic_id;
    cpu_count = smp_request.response->cpu_count;

    if (smp_request.response->cpu_count > SMT_MAX_CPUS) {
        [[unlikely]] dbg::log("SMT: CPU count exceeds maximum supported CPUs");
        hcf();
    }

    for (uint64_t i = 0; i < smp_request.response->cpu_count; i++) {
        _wOS_CPUDATA[i].processor_id = smp_request.response->cpus[i]->processor_id;
        _wOS_CPUDATA[i].lapic_id = smp_request.response->cpus[i]->lapic_id;
        _wOS_CPUDATA[i].goto_address = &smp_request.response->cpus[i]->goto_address;
        _wOS_CPUDATA[i].stack_pointer_ref = (uint64_t**)&(smp_request.response->cpus[i]->extra_argument);
    }

    for (uint64_t i = 0; i < smp_request.response->cpu_count; i++) {
        if (i == cpu::currentCpu()) {
            continue;
        }
        auto stack = mm::Stack();
        _wOS_CPUDATA[i].stack_pointer_ref = &stack.sp;
        startCpuTask(i, reinterpret_cast<CpuGotoAddr>(cpuParamInit), stack, stack.sp);
    }

    runHandoverTasks(modules);
    hcf();
}

}  // namespace ker::mod::smt
