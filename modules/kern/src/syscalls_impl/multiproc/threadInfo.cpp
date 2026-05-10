#include "threadInfo.hpp"

#include <cstdint>
#include <platform/asm/cpu.hpp>

#include "abi/callnums/multiproc.h"
#include "platform/acpi/apic/apic.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/smt/smt.hpp"

namespace ker::syscall::multiproc {
auto thread_info(abi::multiproc::threadInfoOps op) -> uint64_t {
    switch (op) {
        case abi::multiproc::threadInfoOps::CURRENT_THREAD_ID:
            return mod::apic::get_apic_id();

        case abi::multiproc::threadInfoOps::NATIVE_THREAD_COUNT:
            return mod::smt::cpu_count();

        case abi::multiproc::threadInfoOps::CURRENT_CPU:
            return mod::cpu::current_cpu();

        default:
            mod::dbg::error("Invalid op in syscall thread info");
            return -1;
    }
}
}  // namespace ker::syscall::multiproc
