#pragma once

#include <abi/callnums.hpp>
#include <abi/interfaces/multiproc.int.hpp>
#include <abi/syscall.hpp>
#include <defines/defines.hpp>
#include <std/type_traits.hpp>

namespace ker::abi::multiproc {
uint64_t currentThreadId();
uint64_t nativeThreadCount();
}  // namespace ker::abi::multiproc
