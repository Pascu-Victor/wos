#pragma once
#include <abi/callnums/multiproc.h>

#include <abi/callnums.hpp>
#include <defines/defines.hpp>
#include <platform/acpi/apic/apic.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/smt/smt.hpp>

namespace ker::syscall::multiproc {
uint64_t threadInfo(ker::abi::multiproc::threadInfoOps op);
}  // namespace ker::syscall::multiproc
