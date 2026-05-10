#pragma once

#include <cstdint>

namespace ker::mod::cpu {
struct GPRegs;
}

namespace ker::mod::gates {
struct InterruptFrame;
}

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::dbg::coredump {

// Create the long-lived coredump kernel task. Call once after the scheduler is up.
void init();

// Best-effort: snapshot the crash state and hand off to the coredump task.
// Returns immediately; VFS I/O runs on the coredump task's own stack.
// File name format: [PROGRAM_NAME]_[TIMESTAMP_IN_QUANTUMS]_coredump.bin
void try_write_for_task(ker::mod::sched::task::Task* task, const ker::mod::cpu::GPRegs& gpr, const ker::mod::gates::InterruptFrame& frame,
                        uint64_t cr2, uint64_t cr3, uint64_t cpu_id);

}  // namespace ker::mod::dbg::coredump
