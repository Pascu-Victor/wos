# Init supervisor control ABI

WOS exposes the init supervisor control plane through four append-only process
operation selectors. The ABI is declared in `abi/init_control.hpp` in the
kernel and installed for userspace as `<sys/init_control.h>`.

## Operations

| Process operation | Caller | Result |
| --- | --- | --- |
| `INIT_CONTROL_SUBMIT` | Effective UID 0 | Enqueue one `Request` |
| `INIT_CONTROL_RECEIVE` | Process ID 1 | Receive the oldest request |
| `INIT_STATUS_PUBLISH` | Process ID 1 | Atomically replace the status snapshot |
| `INIT_STATUS_READ` | Any process | Read the latest complete snapshot |

All operations are nonblocking. An empty mailbox, a full mailbox, or a status
read before PID 1 publishes the first snapshot returns `-EAGAIN`. A failed PID
1 request copyout leaves the request queued. Mutating requests are authenticated
from kernel task credentials; the kernel overwrites `sender_pid` and
`sender_euid`, so requesters cannot forge them.

Version 1 uses exact fixed sizes and requires unused flags and reserved fields
to be zero. Names are nonempty, NUL-terminated, and limited to ASCII letters,
digits, `.`, `_`, `-`, and `@`. The mailbox holds 16 requests and the status
snapshot holds 16 services. Each service includes up to four ordered transition
records with a monotonic timestamp, previous state, next state, and reason. The
kernel validates their enums and timestamp order, canonicalizes name padding,
and clears unused history records and service slots before publishing data to
readers. The complete version 1 snapshot is fixed at 4,160 bytes.

PID 1 should drain requests with `init_control_receive()` until it returns
`-EAGAIN` on every supervisor tick. It publishes a zero-initialized
`StatusSnapshot` with `sequence == 0`; the kernel assigns a monotonically
increasing nonzero sequence when committing the snapshot.

## Command line

`servicectl status [SERVICE]` reads the public snapshot. `servicectl start
SERVICE`, `stop SERVICE`, and `restart SERVICE` submit authenticated requests.
Targeted `status SERVICE` output includes the service's bounded transition
history.
Only the kernel enforces mutation authorization; the command is not a security
boundary by itself.
