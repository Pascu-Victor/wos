#include "threadInfo.hpp"

namespace ker::syscall::multiproc {
uint64_t threadInfo(ker::abi::inter::multiproc::threadInfoOps op) {
    switch (op) {
        case abi::inter::multiproc::threadInfoOps::currentThreadId:
            return mod::apic::getApicId();

        case abi::inter::multiproc::threadInfoOps::nativeThreadCount:
            return mod::smt::cpuCount();

        default:
            mod::dbg::error("Invalid op in syscall threadInfo");
            return -1;
    }
}
}  // namespace ker::syscall::multiproc
