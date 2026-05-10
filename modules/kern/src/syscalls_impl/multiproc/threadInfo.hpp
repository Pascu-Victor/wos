#pragma once
#include <abi/callnums/multiproc.h>

#include <abi/callnums.hpp>
#include <cstdint>
#include <defines/defines.hpp>
#include <platform/acpi/apic/apic.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/smt/smt.hpp>

namespace ker::syscall::multiproc {
auto thread_info(ker::abi::multiproc::threadInfoOps op) -> uint64_t;
}  // namespace ker::syscall::multiproc
