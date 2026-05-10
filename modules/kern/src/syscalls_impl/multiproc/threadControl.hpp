#pragma once
#include <abi/callnums/multiproc.h>

#include <abi/callnums.hpp>
#include <defines/defines.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/smt/smt.hpp>

namespace ker::syscall::multiproc {
auto thread_control(ker::abi::multiproc::threadControlOps op, void* arg1, void* arg2, void* arg3) -> uint64_t;
}  // namespace ker::syscall::multiproc
