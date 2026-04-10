---
applyTo: "toolchain/src/mlibc/**"
---

# mlibc for WOS - Architecture & Conventions

## Sysdep Tag System

mlibc uses a tag-based sysdep dispatch system. To add a new syscall-backed function:

1. **Tag struct** is declared in `options/internal/include/mlibc/sysdep-tags.hpp` (e.g. `struct Writev {};`)
2. **Signature** is declared in `options/internal/include/mlibc/sysdep-signatures.hpp` using `SYSDEP_FUNC(Tag, args...)`
3. **WOS registration**: Add the tag to `WosSysdepTags` in `sysdeps/wos/include/mlibc/sysdeps.hpp` (multi-inheritance list)
4. **Implementation**: Add `Sysdeps<Tag>::operator()(args...)` in `sysdeps/wos/generic/sysdeps.cpp`

The `mlibc::IsImplemented<Tag>` trait is `true` only when the tag is in `WosSysdepTags`. POSIX fallback code uses `if constexpr (mlibc::IsImplemented<Tag>)` to choose between the sysdep and a generic fallback.

## Key Files

- `sysdeps/wos/include/mlibc/sysdeps.hpp` — WosSysdepTags (list of all implemented sysdep tags)
- `sysdeps/wos/generic/sysdeps.cpp` — All `Sysdeps<Tag>::operator()` implementations
- `sysdeps/wos/include/generic/syscall.h` — Raw `syscall()` wrapper (inline asm, x86-64 `syscall` instruction)
- `sysdeps/wos/include/sys/callnums.h` — Syscall numbers (enum `callnums`: vfs, net, threading, etc.)
- `options/wos/generic/vfs.cpp` — Old-style free-function sysdeps (`sys_read`, `sys_write`, etc.) - legacy, prefer tag system

## Syscall ABI

WOS uses a subsystem + operation pattern (NOT Linux-style flat numbers):

- RAX = `ker::abi::callnums::XXX` (subsystem: vfs, net, threading, process, etc.)
- RDI = operation enum (e.g. `ker::abi::vfs::ops::read`)
- RSI, RDX, RCX, R8, R9 = arguments
- Return value in RAX, negative = `-errno`

Higher-level wrappers exist: `ker::abi::vfs::read()`, `ker::abi::vfs::write()`, etc. Prefer these over raw `syscall()`.

## Error Convention

Sysdep implementations return `0` on success or a positive errno value on failure. The calling code in POSIX options sets `errno` and returns `-1`. Example pattern:

```cpp
int Sysdeps<Write>::operator()(int fd, const void *buf, size_t count, ssize_t *bytes_written) {
    ssize_t r = ker::abi::vfs::write(fd, buf, count);
    if (r < 0)
        return (int)(-r);
    if (bytes_written)
        *bytes_written = r;
    return 0;
}
```

## Signal Safety

Fallback implementations in `options/posix/generic/` may not be async-signal-safe (heap allocations, logging). When signal safety matters, implement the proper sysdep via the tag system instead of relying on fallbacks.

## Build

mlibc is built as part of the WOS build via the "Build WOS" task. Source is under `toolchain/src/mlibc/`.
