#pragma once

#include <cstdint>

namespace ker::mod::cpu {
struct GPRegs;
}

namespace ker::mod::gates {
struct interruptFrame;
}

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::dbg::coredump {

// Best-effort: writes a core dump for the given task to /mnt/disk (persistent FAT32 image).
// File name format: [PROGRAM_NAME]_[TIMESTAMP_IN_QUANTUMS]_coredump.bin
void tryWriteForTask(ker::mod::sched::task::Task* task, const ker::mod::cpu::GPRegs& gpr, const ker::mod::gates::interruptFrame& frame,
                     uint64_t cr2, uint64_t cr3, uint64_t cpuId);

}  // namespace ker::mod::dbg::coredump
