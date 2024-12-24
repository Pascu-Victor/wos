#include "multiproc.hpp"

namespace ker::abi::multiproc {
uint64_t currentThreadId() { return ker::abi::syscall(callnums::multiproc, (uint64_t)inter::multiproc::threadInfoOps::currentThreadId); }
uint64_t nativeThreadCount() {
    return ker::abi::syscall(callnums::multiproc, (uint64_t)inter::multiproc::threadInfoOps::nativeThreadCount);
}
}  // namespace ker::abi::multiproc
