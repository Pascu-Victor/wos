#include "threadInfo.hpp"

namespace ker::syscall::multiproc {
uint64_t threadControl(abi::multiproc::threadControlOps op, void *arg1) {
    switch (op) {
        case abi::multiproc::threadControlOps::setTCB: {
            void *tcb = arg1;
            return mod::smt::setTcb(tcb);
        }

        default:
            mod::dbg::error("Invalid op in syscall thread control");
            return -1;
    }
}
}  // namespace ker::syscall::multiproc
