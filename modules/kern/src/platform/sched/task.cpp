#include "task.hpp"

#include <platform/loader/elf_loader.hpp>
#include <platform/mm/virt.hpp>

namespace ker::mod::sched::task {
Task::Task(const char* name, uint64_t elfStart, uint64_t kernelRsp, TaskType type) {
    this->name = name;
    // this->entry = entry;
    // this->regs.ip = entry;
    this->pagemap = mm::virt::createPagemap();
    this->context.frame.rsp =
        (uint64_t)mm::addr::getPhysPointer((mm::addr::vaddr_t)mm::phys::pageAlloc(mm::paging::PAGE_SIZE)) + mm::paging::PAGE_SIZE;
    this->context.regs = cpu::GPRegs();
    this->type = type;
    this->cpu = cpu::currentCpu();
    this->context.syscallKernelStack = kernelRsp;
    this->context.syscallUserStack = (uint64_t)(new uint8_t[USER_STACK_SIZE]) + USER_STACK_SIZE;

    mm::virt::copyKernelMappings(this);
    mm::virt::mapPage(this->pagemap, (uint64_t)this->context.frame.rsp - mm::paging::PAGE_SIZE,
                      (uint64_t)this->context.frame.rsp - mm::paging::PAGE_SIZE, mm::paging::pageTypes::USER);

    uint64_t elfEntry = loader::elf::loadElf((loader::elf::ElfFile*)elfStart, this->pagemap);
    this->entry = elfEntry;
    this->context.frame.rip = elfEntry;
}

Task::Task(const Task& task) {
    this->name = task.name;
    this->entry = task.entry;
    this->context = task.context;
    this->pagemap = task.pagemap;
    this->type = task.type;
    this->cpu = task.cpu;
    this->thread = task.thread;
}

void Task::loadContext(cpu::GPRegs* gpr) { this->context.regs = *gpr; }

void Task::saveContext(cpu::GPRegs* gpr) {
    cpuSetMSR(IA32_GS_BASE, (uint64_t)this->context.syscallUserStack);
    cpuSetMSR(IA32_KERNEL_GS_BASE, (uint64_t)this->context.syscallKernelStack);
    *gpr = context.regs;
}
}  // namespace ker::mod::sched::task
