#pragma once

// Host shim: replaces kernel defines.

#include <cstring>
#include <cstdint>
#include <cstddef>

#ifndef __always_inline
#define __always_inline __inline __attribute__((__always_inline__))
#endif

#include <defines/datatypes.hpp>
#include <defines/stack_defines.hpp>
