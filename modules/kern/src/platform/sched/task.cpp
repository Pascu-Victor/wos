#include "task.hpp"

#include <platform/mm/virt.hpp>

namespace ker::mod::sched::task {
Task::Task(const char* name, uint64_t entry) {
    this->name = name;
    this->entry = entry;
    this->regs.ip = entry;
    this->stack = 0;
    this->regs.regs = cpu::GPRegs();
    this->pagemap = mm::virt::createPagemap();
}
}  // namespace ker::mod::sched::task
