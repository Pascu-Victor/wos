---
applyTo: "toolchain/src/mlibc/**"
---

# mlibc for WOS - Architecture and Conventions

These rules apply when the local branch contains `toolchain/src/mlibc/**`.

Reviewed: 2026-06-01.

Local `.gitmodules` currently contains `toolchain/src/mlibc`, `toolchain/src/busybox`, and `toolchain/src/dropbear` submodules, and no `modules/stdlib/musl` submodule. Before editing libc code, still verify the local workspace because this area changes quickly.

## Sysdep tag system

mlibc uses a tag-based sysdep dispatch system. To add a new syscall-backed function:

1. Declare the tag struct in `options/internal/include/mlibc/sysdep-tags.hpp`.
2. Declare the signature in `options/internal/include/mlibc/sysdep-signatures.hpp` using `SYSDEP_FUNC(Tag, args...)`.
3. Add the tag to `WosSysdepTags` in `sysdeps/wos/include/mlibc/sysdeps.hpp`.
4. Implement `Sysdeps<Tag>::operator()(args...)` in `sysdeps/wos/generic/sysdeps.cpp`.

The `mlibc::IsImplemented<Tag>` trait is `true` only when the tag is in `WosSysdepTags`. POSIX fallback code uses `if constexpr (mlibc::IsImplemented<Tag>)` to choose between the WOS sysdep and a generic fallback.

## Key files

- `options/internal/include/mlibc/sysdep-tags.hpp`
- `options/internal/include/mlibc/sysdep-signatures.hpp`
- `sysdeps/wos/include/mlibc/sysdeps.hpp`
- `sysdeps/wos/generic/sysdeps.cpp`
- `sysdeps/wos/include/generic/syscall.h`
- `sysdeps/wos/include/sys/callnums.h`
- `options/wos/generic/vfs.cpp` for legacy free-function sysdeps; prefer the tag system.

## WOS syscall ABI

WOS uses a subsystem + operation pattern, not Linux-style flat syscall numbers.

- `RAX` = `ker::abi::callnums::XXX` subsystem.
- The generic syscall wrapper in `sysdeps/wos/include/sys/syscall.h` passes `a1..a6` in `RDI`, `RSI`, `RDX`, `R8`, `R9`, and `R10`.
- For most WOS subsystems, `a1`/`RDI` is the operation enum and the remaining registers hold operation arguments.
- The local kernel syscall handler in `modules/kern/src/platform/sys/syscall.cpp` copies those same registers into `A1..A6`; verify both sides before editing.
- Return value in `RAX`; negative means `-errno` at the raw syscall boundary.

Prefer higher-level wrappers such as `ker::abi::vfs::read()` and `ker::abi::vfs::write()` over raw `syscall()` when available.

## mlibc error convention

Sysdep implementations return `0` on success or a positive errno value on failure. POSIX calling code sets `errno` and returns `-1`.

Example:

```cpp
int Sysdeps<Write>::operator()(int fd, const void *buf, size_t count, ssize_t *bytes_written) {
    ssize_t r = ker::abi::vfs::write(fd, buf, count);
    if (r < 0)
        return static_cast<int>(-r);
    if (bytes_written)
        *bytes_written = r;
    return 0;
}
```

## Compatibility policy

When extending `mlibc` or other libc-facing userspace compatibility layers, prefer broadly glibc-compatible headers, constants, ABI surface, and behavior over one-off WOS-specific hacks or app-local workarounds.

If a temporary workaround is unavoidable:

- Scope it narrowly.
- Comment why it exists.
- Keep the path open for compatible libc behavior later.

## Signal safety

Fallback implementations in `options/posix/generic/` may not be async-signal-safe. When signal safety matters, implement the proper WOS sysdep through the tag system instead of relying on fallbacks.

## Build

mlibc, when present locally, is built as part of the WOS build through the `Build WOS` task. Do not assume public `main` has this tree.

The current module build uses `toolchain/sysroot` as the target sysroot. WOS utility modules include headers from the copied sysroot and WOS ABI headers; when changing libc-facing headers, check both `toolchain/src/mlibc/sysdeps/wos/include/**` and `modules/kern/src/abi/**` for drift.
