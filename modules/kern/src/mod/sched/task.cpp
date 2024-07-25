#include "task.hpp"
#include <mod/mm/virt.hpp>

namespace ker::mod::sched::task
{
    Task::Task(const char* name, mm::paging::PageTable* pagemap, uint64_t entry, TaskType type, uint64_t stack, sys::context_switch::TaskRegisters regs)
        : name(name), pagemap(pagemap), entry(entry), type(type), stack(stack), regs(regs)
    {
    }

    Task::Task(const char* name, uint64_t entry)
    {
        this->name = name;
        this->entry = entry;
        this->regs.ip = entry;
        this->stack = 0;
        this->regs.regs = cpu::GPRegs();
        this->pagemap = mm::virt::createPagemap(); 
    }
}