/* Host shim for linux/types.h
 *
 * Provides the standard Linux integer types but OMITS the Be16/Be32/Be64
 * typedefs that conflict with the WOS kernel's struct-based endian wrappers
 * defined in net/endian.hpp.
 */
#ifndef _LINUX_TYPES_H
#define _LINUX_TYPES_H

#include <asm/types.h>

#ifndef __ASSEMBLY__

#include <linux/posix_types.h>

#ifdef __SIZEOF_INT128__
typedef __signed__ __int128 __s128 __attribute__((aligned(16)));
typedef unsigned __int128 __u128 __attribute__((aligned(16)));
#endif

/* Intentionally omitted: Be16, Be32, Be64, __le16, __le32, __le64
 * These conflict with WOS kernel's struct __be{16,32,64} in net/endian.hpp. */

typedef __u16 __sum16;
typedef __u32 __wsum;

#define __aligned_u64 __u64 __attribute__((aligned(8)))
#define __aligned_s64 __s64 __attribute__((aligned(8)))

typedef unsigned __poll_t;

#endif /* __ASSEMBLY__ */
#endif /* _LINUX_TYPES_H */
