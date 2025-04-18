#pragma once
#include <abi/callnums/process.h>

#include <abi/callnums.hpp>
#include <abi/syscall.hpp>
#include <defines/defines.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/smt/smt.hpp>
#include <std/cstdint.hpp>
#include <std/mem.hpp>
#include <syscalls_impl/log/log.hpp>
#include <syscalls_impl/log/sys_log.hpp>
#include <syscalls_impl/multiproc/multiproc.hpp>
#include <syscalls_impl/process/process.hpp>
#include <syscalls_impl/syscalls.hpp>

namespace ker::syscall::process {}
