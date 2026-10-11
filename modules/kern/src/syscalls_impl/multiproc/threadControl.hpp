#pragma once
#include <abi/callnums/multiproc.h>

#include <abi/callnums.hpp>
#include <defines/defines.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/smt/smt.hpp>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::syscall::multiproc {
// Caller retains the exact subject; CPU-domain/physical selectors require Host scope.
auto host_thread_control_for_subject(ker::mod::sched::task::Task* subject, ker::abi::multiproc::threadControlOps op, uint64_t arg1,
                                     uint64_t arg2, uint64_t arg3) -> uint64_t;
[[noreturn]] void wos_thread_exit_current();
auto thread_control(ker::abi::multiproc::threadControlOps op, uint64_t arg1, uint64_t arg2, uint64_t arg3) -> uint64_t;
}  // namespace ker::syscall::multiproc
