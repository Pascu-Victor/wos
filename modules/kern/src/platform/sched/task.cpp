#include "task.hpp"

#include <platform/loader/elf_loader.hpp>
#include <platform/mm/virt.hpp>

namespace ker::mod::sched::task {
Task::Task(const char* name, uint64_t elf_start, TaskType type) {
    this->name = name;
    // this->entry = entry;
    // this->regs.ip = entry;
    this->pagemap = mm::virt::createPagemap();
    this->regs.rsp =
        (uint64_t)mm::addr::getPhysPointer((mm::addr::vaddr_t)mm::phys::pageAlloc(mm::paging::PAGE_SIZE)) + mm::paging::PAGE_SIZE;
    this->regs.regs = cpu::GPRegs();
    this->type = type;
    this->cpu = cpu::currentCpu();
    mm::virt::mapPage(this->pagemap, (uint64_t)this->regs.rsp - mm::paging::PAGE_SIZE, (uint64_t)this->regs.rsp - mm::paging::PAGE_SIZE,
                      mm::paging::pageTypes::USER);

    uint64_t elfEntry = loader::elf::loadElf((loader::elf::ElfFile*)elf_start, this->pagemap);
    this->entry = elfEntry;
    this->regs.ip = elfEntry;
}
}  // namespace ker::mod::sched::task
