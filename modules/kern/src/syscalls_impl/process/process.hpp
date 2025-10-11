#pragma once
#include <abi/callnums/process.h>
#include <sys/types.h>

#include <abi/callnums.hpp>
#include <abi/syscall.hpp>
#include <cstdint>
#include <defines/defines.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/smt/smt.hpp>
#include <syscalls_impl/log/log.hpp>
#include <syscalls_impl/log/sys_log.hpp>
#include <syscalls_impl/multiproc/multiproc.hpp>
#include <syscalls_impl/process/process.hpp>
#include <syscalls_impl/syscalls.hpp>
#include <util/mem.hpp>

namespace ker::syscall::process {
auto process(abi::process::procmgmt_ops op, u_int64_t a1, u_int64_t a2, u_int64_t a3, u_int64_t a4) -> u_int64_t;
}
