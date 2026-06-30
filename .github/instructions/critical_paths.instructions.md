---
applyTo: "**"
---

# WOS Critical Path Checklist

Critical-path code must be changed conservatively. A small mistake can become a scheduler bug, wire incompatibility, deadlock, filesystem recursion, or user/kernel ABI break.

## Always classify the context

For each changed function, identify whether it can run in any of these contexts:

- Interrupt handler.
- Panic/emergency path.
- Early boot before the journal is initialized.
- Scheduler hook or task-state transition.
- Syscall handler or syscall implementation.
- Network RX/NAPI/netpoll path.
- WKI spin-wait or ACK-wait path.
- VFS path resolution/open/read/write/readdir path.
- Procfs diagnostic path consumed by WOS utilities.
- Kernel perf event recording path.
- Ptrace stop/register/memory path.
- Device proxy/server operation path.
- Userspace libc wrapper/sysdep path.

Then answer:

- Can it allocate?
- Can it block?
- Can it spin-wait?
- Can it log?
- Can it take locks?
- Which locks may already be held?
- Can it call into VFS, scheduler, network, or WKI again?

## Syscall ABI path

Local entrypoint:

- `modules/kern/src/platform/sys/syscall.cpp`

Current dispatch pattern reviewed on 2026-06-01:

- `RAX` selects `ker::abi::callnums` subsystem.
- `RDI`, `RSI`, `RDX`, `R8`, `R9`, `R10` are copied into `a1` through `a6`.
- Dispatch routes to subsystem handlers such as VFS, net, futex, threading, time, vmem, process, and logging.
- Ptrace syscall-stop hooks can observe and modify syscall entry/exit state for WOS `strace`.

Before changing syscall behavior:

- Check kernel ABI headers and userspace wrapper/sysdep code together.
- Preserve argument ordering.
- Preserve errno sign convention at each layer.
- Validate user pointers where applicable.
- Avoid logging or serial output in hot syscall paths unless explicitly diagnostic and safe.
- Check whether the libc-facing layer expects Linux-like behavior or WOS-specific behavior.

## Scheduler and task-state paths

Read first:

- `modules/kern/src/platform/sched/**`
- `modules/kern/src/net/wki/remote_compute.*` when WKI remote placement is involved.

Before changing scheduler code:

- Identify which task object is the subject and which task is currently running.
- Do not assume `get_current_task()` refers to the task being submitted or manipulated.
- Check atomic ordering on task state and exit flags.
- Check dead-list and epoch-based reclamation implications.
- Do not add blocking waits while scheduler locks are held.

WKI-specific known hazard:

- Do not use `make_absolute()` from scheduler hooks or remote compute if it resolves against `get_current_task()` instead of the submitted task. Resolve relative paths using the relevant `Task` object's `cwd`.

## Network RX / NAPI / WKI ACK-wait paths

Read first:

- `modules/kern/src/net/netpoll.hpp`
- `modules/kern/src/net/wki/wki.cpp`
- Relevant WKI transport file.

Before changing RX or NAPI-adjacent code:

- Determine whether the code runs inside poll/RX context.
- Do not call operations that spin-wait for an ACK from a context that must receive that ACK.
- Prefer deferring ACK-waiting or mount/zone work to a timer/thread/pending queue.
- Check PacketBuffer ownership and whether transport takes ownership.
- Verify that inline polling/yield helpers are used intentionally.

Known WKI hazard:

- `wki_remote_vfs_mount()` and `wki_zone_create()` must not be called directly from NAPI poll context because ACK processing can be blocked by the current context.

## WKI wire and channel reliability

Read first:

- `modules/kern/src/net/wki/wire.hpp`
- `modules/kern/src/net/wki/wki.hpp`
- `modules/kern/src/net/wki/wki.cpp`
- `modules/kern/src/net/wki/channel.*`

Before changing wire/channel code:

- Preserve `__attribute__((packed))` wire layout unless intentionally changing protocol ABI.
- Preserve `static_assert` sizes and update all peers/handlers together.
- Check `WKI_HEADER_SIZE`, `WKI_ETH_MAX_PAYLOAD`, channel IDs, message types, seq/ack arithmetic, retransmit queues, credits, and TTL.
- Check ownership and lifetime for retransmit and reorder buffers.
- Check direct-peer checksum bypass versus routed checksums.

## VFS and remote VFS

Read first:

- `modules/kern/src/vfs/**`
- `modules/kern/src/net/wki/remote_vfs.*`
- WKI instructions for VFS path construction.

Before changing path behavior:

- Identify whether path resolution is local, remote, symlink, mount, or server-side export resolution.
- Protect against recursive remote mount traversal.
- Do not allow server-side exported path resolution to cross into another `FSType::REMOTE` mount.
- Ensure remote `/wki/` directory listings do not expose self-referencing loops.
- Check cache invalidation for read-ahead, write-behind, and directory cache behavior.

## Device proxy/server and fencing

Read first:

- `modules/kern/src/net/wki/dev_proxy.*`
- `modules/kern/src/net/wki/dev_server.*`
- `modules/kern/src/net/wki/remotable.*`
- `modules/kern/src/net/wki/blk_ring.hpp`

Before changing lifecycle behavior:

- Check attach/detach protocol.
- Check proxy locks and pending operation state.
- Check timeout behavior.
- On fence, proxy block devices are suspended first, not immediately destroyed. Reconnect may resume operations; timeout performs hard teardown and unmount.

## Logging and serial output

Use journal-backed logging for routine kernel logs. Direct serial output is limited to panic, very early boot before journal initialization, serial driver internals, and emergency dump paths where normal logging is unsafe.

## WOS utility ABI/procfs surfaces

Read first:

- `modules/kern/src/vfs/fs/procfs.cpp`
- `modules/kern/src/platform/dbg/journal.*`
- `modules/kern/src/platform/debug/ptrace.*`
- `modules/kern/src/platform/perf/perf_events.*`
- Matching userspace utility under `modules/`

Before changing utility-facing kernel diagnostics:

- Treat `JournalRecord`, ptrace request numbers/structs, perf event text formats, and `/proc/memacc` key/value rows as compatibility surfaces for WOS tools.
- Do not assume Linux `/proc`, systemd journal, upstream `strace`, or Linux `perf` behavior; the local WOS utility source defines expected behavior.
- Preserve nonblocking/poll semantics for device/procfs readers when utilities follow or poll data.
- Ask the user to run the relevant WOS utility when behavior requires runtime confirmation.

## Patch contract template

```text
Problem:
Expected files to change:
Files read:
Entrypoints:
Callers:
Callees:
Locks held:
Can allocate/block/spin/log:
ABI/wire/syscall impact:
Lifetime/ownership impact:
Build/test plan:
Runtime/debug data needed from user:
Rollback plan:
```
