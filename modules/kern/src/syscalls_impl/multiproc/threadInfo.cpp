#include "threadInfo.hpp"

namespace ker::syscall::multiproc {
uint64_t threadInfo(abi::multiproc::threadInfoOps op) {
    switch (op) {
        case abi::multiproc::threadInfoOps::currentThreadId:
            return mod::apic::getApicId();

        case abi::multiproc::threadInfoOps::nativeThreadCount:
            return mod::smt::cpuCount();

        default:
            mod::dbg::error("Invalid op in syscall thread info");
            return -1;
    }
}
}  // namespace ker::syscall::multiproc
