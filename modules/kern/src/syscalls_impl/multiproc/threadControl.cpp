#include "threadInfo.hpp"

#include <platform/sched/scheduler.hpp>

namespace ker::syscall::multiproc {
uint64_t threadControl(abi::multiproc::threadControlOps op, void *arg1) {
    switch (op) {
        case abi::multiproc::threadControlOps::setTCB: {
            void *tcb = arg1;
            return mod::smt::setTcb(tcb);
        }

        case abi::multiproc::threadControlOps::yield: {
            (void)arg1;
            auto *task = mod::sched::getCurrentTask();
            if (task != nullptr) {
                task->yieldSwitch = true;
                task->deferredTaskSwitch = true;
            }
            return 0;
        }

        default:
            mod::dbg::error("Invalid op in syscall thread control");
            return -1;
    }
}
}  // namespace ker::syscall::multiproc
