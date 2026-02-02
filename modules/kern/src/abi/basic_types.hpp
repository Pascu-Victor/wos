#pragma once
// Minimal fixed-width types for kernel ABI headers.

// Avoid pulling in system <cstdint> or libc++ headers here; define only the
// smallest set of integer typedefs used by ABI headers.
typedef unsigned long long uint64_t;
typedef long long int64_t;
typedef unsigned int uint32_t;
typedef int int32_t;
