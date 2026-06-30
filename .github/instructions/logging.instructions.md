---
applyTo: "modules/kern/src/**"
---

# WOS Logging and Journal Conventions

Apply these to kernel code.

Reviewed: 2026-06-01.

## Default rule

Kernel logging must use the journal-backed logging API.

New module code should define a typed logger, for example:

```cpp
using log = ker::mod::dbg::logger<"wki">;
```

Use:

```cpp
log::trace(...);
log::debug(...);
log::info(...);
log::warn(...);
log::error(...);
```

instead of manually prefixing messages with tags such as `[WKI]`.

## Compatibility rule

Existing `ker::mod::dbg::log()` call sites may remain only as compatibility during migration. New code should use a module logger when the typed logger API is available in the local tree.

If the local branch still has old-style logging in a subsystem, do not perform a broad logging migration unless requested. For touched lines, prefer moving new or changed routine logging toward the typed logger convention.

## Serial output rule

Do not write routine logs directly to serial.

Direct serial writes are allowed only in:

- Panic paths.
- Very early boot before the journal is initialized.
- The serial driver itself.
- Emergency dump paths where allocation or scheduler interaction is unsafe.

## Disabled/ad hoc debug logs

If a subsystem has disabled or commented-out debug logging, migrate it to `trace` or `debug` journal calls instead of leaving ad hoc serial/debug code.

## Critical-path logging caution

Before adding logging to scheduler, syscall, interrupt, panic, allocator, NAPI/RX, or WKI spin-wait paths, check whether logging can allocate, take locks, or re-enter code that is unsafe in that context.

## Userspace journal tools

The `modules/journal` target builds the WOS journal reader/daemon binary and `libjournal.so`. The rootfs installs that binary as `/usr/bin/journalctl` and `/usr/sbin/journald`.

When changing kernel journal records or `/dev/journal` behavior:

- Treat `ker::mod::dbg::journal::JournalRecord` as a userspace ABI; preserve magic/version/header/size expectations unless updating kernel, mlibc copied headers, and `modules/journal` together.
- Verify filtering, follow, daemon persistence, and poll/read behavior against `modules/journal/src/main.cpp`.
- Do not assume systemd `journalctl` option compatibility; this is the WOS journal utility.
