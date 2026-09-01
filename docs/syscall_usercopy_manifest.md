# Syscall usercopy contract manifest

This is the closed-world inventory for the syscall/usercopy seam. The local ABI
headers and syscall implementations remain authoritative; the host test
`tests/host/unit/syscall_usercopy_manifest_test.py` requires every current ABI
operation to have exactly one row here.

`Access` describes memory in a userspace address space: `in`, `out`, or
`in/out` means copied through `platform/sys/usercopy`; `range` means mapped user
pages are intentionally operated on through pinned mappings; `address` means a
value is retained or used as a mapping/deferred execution address but is not
dereferenced by that syscall; `none` means the operation has no userspace
memory operand. A zero-size range performs no copy unless a row says otherwise.
All arithmetic used to form a range, array size, or nested address must be
checked before access.

The common rule is **No usercopy while locked** by a subsystem spinlock. Input
records and strings are snapshotted before subsystem work. Output ranges that
must reject an initially bad address before an irreversible effect are
preflighted first. A concurrent remap can still make a later commit return
`-EFAULT`, but stable frame pins ensure that it cannot turn into a kernel fault
or access a recycled frame. Backends may never report more bytes than their
kernel bounce-buffer capacity; an over-report is `-EOVERFLOW`.

## Top-level operations without a subordinate operation enum

| ABI family | Operation | Access | Maximum / bound | Nullable | Partial progress | Irreversible-effect ordering |
| --- | --- | --- | --- | --- | --- | --- |
| `TOP` | `VMEM_MAP` | `address` | page-aligned checked `size` within user space; `hint` is an address value | hint yes; size no | none | validates mapping arguments before publishing an anonymous or file mapping |
| `TOP` | `DEBUG` | `none` | n/a | n/a | none | changes local interrupt state only |
| `TOP` | `PERSONALITY` | `none` | n/a | n/a | none | scalar task state only |

`VMEM_MAP and VMEM address arguments` are not ordinary syscall buffers. They
identify mappings or execution addresses. They still receive canonical-range,
alignment, overflow, and ownership validation before page-table mutation.

## Logging

| ABI family | Operation | Access | Maximum / bound | Nullable | Partial progress | Irreversible-effect ordering |
| --- | --- | --- | --- | --- | --- | --- |
| `SYS_LOG` | `LOG` | `in` | message prefix up to `MAX_SYSLOG_COPY` (4096), with strict NUL scan when length is zero | no | a mapped prefix may be logged and marked truncated; zero copied bytes returns `-EFAULT` | copies before journal/framebuffer emission |
| `SYS_LOG` | `LOG_LINE` | `in` | message prefix up to `MAX_SYSLOG_COPY` (4096), with strict NUL scan when length is zero | no | a mapped prefix may be logged and marked truncated; zero copied bytes returns `-EFAULT` | copies before journal/framebuffer emission |
| `SYS_LOG` | `LOG_EX` | `in` | message up to 4096 plus optional module C string up to `JOURNAL_MODULE_MAX` | module yes; message no | message prefix may be emitted as truncated; module faults are atomic | snapshots message and module before journal emission |
| `SYS_LOG` | `LOG_BLOCK_BEGIN` | `none` | n/a | n/a | none | acquires logical log-block ownership only |
| `SYS_LOG` | `LOG_BLOCK_END` | `none` | n/a | n/a | none | releases logical log-block ownership only |

## Futex

| ABI family | Operation | Access | Maximum / bound | Nullable | Partial progress | Irreversible-effect ordering |
| --- | --- | --- | --- | --- | --- | --- |
| `FUTEX` | `FUTEX_WAIT` | `in` | one aligned 32-bit word plus optional fixed timeout record | timeout yes; word no | none | pins and checks the word before queue publication; timeout is snapshotted first |
| `FUTEX` | `FUTEX_WAKE` | `in` | one aligned 32-bit word/address key | no | wake count only | validates and pins the futex address before waking waiters |

## Threading

| ABI family | Operation | Access | Maximum / bound | Nullable | Partial progress | Irreversible-effect ordering |
| --- | --- | --- | --- | --- | --- | --- |
| `THREAD_INFO` | `CURRENT_THREAD_ID` | `none` | n/a | n/a | none | scalar query |
| `THREAD_INFO` | `NATIVE_THREAD_COUNT` | `none` | n/a | n/a | none | scalar query |
| `THREAD_INFO` | `CURRENT_CPU` | `none` | n/a | n/a | none | scalar query |
| `THREAD_CONTROL` | `SET_TCB` | `out` | TCB self pointer plus signal-mask-cache prefix through offset `0x4c` | no | cache-valid protocol leaves partial concurrent-fault writes invalid | preflights and initializes through usercopy before the address-only FS-base install |
| `THREAD_CONTROL` | `YIELD` | `none` | n/a | n/a | none | scheduler state only |
| `THREAD_CONTROL` | `THREAD_CREATE` | `in/out` | fixed 16-byte prepared-stack input plus TCB signal-cache prefix/TID output; entry addresses are values | TCB/stack no | none | snapshots stack and preflights TCB before publication; writes TID before posting and rolls it back if posting fails |
| `THREAD_CONTROL` | `THREAD_EXIT` | `none` | n/a | n/a | none | exits current thread |
| `THREAD_CONTROL` | `SET_AFFINITY` | `none` | n/a | n/a | none | scalar task state only |
| `THREAD_CONTROL` | `GET_AFFINITY` | `none` | n/a | n/a | none | scalar return value |
| `THREAD_CONTROL` | `CREATE_DOMAIN` | `in` | fixed 42-byte request | no | none | snapshots request before domain creation |
| `THREAD_CONTROL` | `SET_DOMAIN` | `none` | n/a | n/a | none | scalar task/domain state only |
| `THREAD_CONTROL` | `QUERY_DOMAIN` | `out` | fixed 264-byte response | no | none | builds a kernel response before one copyout |

## Process management

| ABI family | Operation | Access | Maximum / bound | Nullable | Partial progress | Irreversible-effect ordering |
| --- | --- | --- | --- | --- | --- | --- |
| `PROCESS` | `EXIT` | `none` | n/a | n/a | none | terminates the process |
| `PROCESS` | `EXEC` | `in` | path C string up to 511 chars; pointer vectors and all strings total at most 2 MiB | argv/envp yes; path no | none | deep snapshot completes before child/image creation |
| `PROCESS` | `WAITPID` | `out` | optional 32-bit status plus optional fixed rusage record | both yes | none | all requested outputs are preflighted before a child can be reaped |
| `PROCESS` | `GETPID` | `none` | n/a | n/a | none | scalar query |
| `PROCESS` | `GETPPID` | `none` | n/a | n/a | none | scalar query |
| `PROCESS` | `FORK` | `none` | n/a | n/a | none | register-frame input is already on the kernel syscall stack |
| `PROCESS` | `SIGACTION` | `in/out` | optional fixed `KernelSigaction` input and output | both yes | none | snapshots/preflights before changing the handler table |
| `PROCESS` | `SIGPROCMASK` | `in/out` | optional 64-bit input and optional 64-bit old-mask output | both yes | none | input and output commit precede mask mutation |
| `PROCESS` | `KILL` | `none` | n/a | n/a | none | scalar target and signal only |
| `PROCESS` | `SIGRETURN` | `in` | one fixed signal frame at the saved userspace stack pointer | no | none | copies/validates the whole frame before restoring registers |
| `PROCESS` | `GETUID` | `none` | n/a | n/a | none | scalar query |
| `PROCESS` | `GETEUID` | `none` | n/a | n/a | none | scalar query |
| `PROCESS` | `GETGID` | `none` | n/a | n/a | none | scalar query |
| `PROCESS` | `GETEGID` | `none` | n/a | n/a | none | scalar query |
| `PROCESS` | `SETUID` | `none` | n/a | n/a | none | scalar credential mutation |
| `PROCESS` | `SETGID` | `none` | n/a | n/a | none | scalar credential mutation |
| `PROCESS` | `SETEUID` | `none` | n/a | n/a | none | scalar credential mutation |
| `PROCESS` | `SETEGID` | `none` | n/a | n/a | none | scalar credential mutation |
| `PROCESS` | `GETUMASK` | `none` | n/a | n/a | none | scalar query |
| `PROCESS` | `SETUMASK` | `none` | n/a | n/a | none | scalar task mutation |
| `PROCESS` | `SETSID` | `none` | n/a | n/a | none | scalar process-group mutation |
| `PROCESS` | `GETSID` | `none` | n/a | n/a | none | scalar query |
| `PROCESS` | `SETPGID` | `none` | n/a | n/a | none | scalar process-group mutation |
| `PROCESS` | `GETPGID` | `none` | n/a | n/a | none | scalar query |
| `PROCESS` | `EXECVE` | `in` | path C string up to 511 chars; pointer vectors and all strings total at most 2 MiB | argv/envp yes; path no | none | deep snapshot and image preparation complete before replacing the old image |
| `PROCESS` | `GETHOSTNAME` | `out` | actual hostname length plus NUL, bounded by caller capacity and `HOSTNAME_MAX` | no | none | validates capacity before copying only the complete NUL-terminated name; untouched capacity need not be mapped |
| `PROCESS` | `SETHOSTNAME` | `in` | explicit length in `1..HOSTNAME_MAX-1` | no | none | snapshots bytes before changing global hostname state |
| `PROCESS` | `SETPRIORITY` | `none` | n/a | n/a | none | scalar scheduler mutation |
| `PROCESS` | `SETWKITARGET` | `in` | explicit hostname length bounded by task WKI target storage | yes when clearing or selecting local/balanced | none | snapshots hostname before task placement-state mutation |
| `PROCESS` | `GETWKITARGET` | `out` | optional NUL hostname within caller capacity plus optional 32-bit flags | both yes | none | preflights every requested output before either copyout |
| `PROCESS` | `PTRACE` | `in/out` | request-specific fixed descriptors and bounded nested buffers; see PTRACE table | request-specific | request-specific transferred/count fields | request-specific preflight precedes tracee mutation or resume |
| `PROCESS` | `GETGROUPS` | `out` | current group count times 32-bit gid; size capped at `0x7fffffff` | yes only for size-zero query | none | complete kernel snapshot copied once |
| `PROCESS` | `SETGROUPS` | `in` | at most `SUPPLEMENTARY_GROUPS_MAX` 32-bit gids | yes only for zero groups | none | full snapshot precedes credential mutation |
| `PROCESS` | `SIGSUSPEND` | `in` | one 64-bit signal mask | no | none | snapshots mask before blocking/state mutation |
| `PROCESS` | `UNAME` | `out` | one fixed `KernelUtsname` | no | none | constructs zero-initialized record before copyout |
| `PROCESS` | `CLONE_VM_PROC` | `in/out` | fixed `CloneVmArgs` plus checked nested parent/child TID fields | nested outputs flag-dependent | none | snapshots/preflights all fields before child publication; failure destroys unpublished child |
| `PROCESS` | `PRCTL` | `in/out` | command-specific scalar, 32-bit output, or 16-byte task name | command-specific | none | pointer commands copy before task-state mutation |
| `PROCESS` | `ARCH_PRCTL` | `out` | `ARCH_GET_FS/ARCH_GET_GS` copy one 64-bit value; SET operations treat arg as an address value | GET no; SET address may be zero | none | GET is one copyout; SET installs a deferred base without dereferencing it |
| `PROCESS` | `SIGALTSTACK` | `in/out` | optional fixed `KernelStackT` input/output; nested stack range checked for overflow and user-space bounds | both records yes | none | old state copies out before mutation; nested stack backing is touched only during signal delivery |
| `PROCESS` | `GETRESUID` | `out` | three 32-bit uid outputs | no | none | preflights all three before copying any |
| `PROCESS` | `GETRESGID` | `out` | three 32-bit gid outputs | no | none | preflights all three before copying any |
| `PROCESS` | `SIGPENDING` | `out` | fixed 128-byte signal-set record | no | none | one complete copyout |
| `PROCESS` | `GETPRIORITY` | `none` | n/a | n/a | none | scalar query |
| `PROCESS` | `SPAWN` | `in` | EXEC deep snapshot plus fixed options, at most 32 action records, and bounded action paths | options yes; path no | none | deep snapshot finishes before process allocation/publication |
| `PROCESS` | `INIT_CONTROL_SUBMIT` | `in` | one fixed 128-byte versioned request | no | none | copies, validates, canonicalizes, and authenticates the request before enqueueing it under the mailbox lock |
| `PROCESS` | `INIT_CONTROL_RECEIVE` | `out` | one fixed 128-byte versioned request | no | destination prefix may be written on fault; the request remains queued | snapshots the oldest request under lock, copies out without the lock, and dequeues only after complete copyout |
| `PROCESS` | `INIT_STATUS_PUBLISH` | `in` | one fixed 4160-byte versioned snapshot with at most 16 services and four transitions per service | no | none | copies, validates, and sanitizes the complete snapshot before atomically replacing status under lock |
| `PROCESS` | `INIT_STATUS_READ` | `out` | one fixed 4160-byte versioned snapshot | no | destination prefix may be written on fault; broker state is unchanged | snapshots complete status under lock, then performs one copyout without the lock |

Command details: `PR_SET_NAME` consumes exactly 16 bytes into a kernel buffer;
`PR_GET_NAME` produces exactly 16 bytes. `PR_GET_PDEATHSIG` produces one
32-bit value. Scalar PRCTL commands have no pointer operand. The
`ARCH_GET_FS/ARCH_GET_GS` forms copy one value out; ARCH SET forms retain an
address but do not dereference it. The **SIGALTSTACK nested address** is range
checked when installed, then the signal-frame path materializes/pins and copies
through usercopy; an unmapped deferred range becomes a delivery fault rather
than a kernel fault.

## Time

| ABI family | Operation | Access | Maximum / bound | Nullable | Partial progress | Irreversible-effect ordering |
| --- | --- | --- | --- | --- | --- | --- |
| `TIME` | `GETTIMEOFDAY` | `out` | one fixed timeval | no | none | computes kernel value before one copyout |
| `TIME` | `CLOCK_GETTIME` | `out` | one fixed timespec | no | none | computes kernel value before one copyout |
| `TIME` | `NANOSLEEP` | `in/out` | one required timespec plus optional remaining timespec | remaining yes; request no | none | snapshots request and initializes requested output before scheduling sleep |
| `TIME` | `TIMES` | `out` | optional fixed tms plus optional clock value | both yes | none | preflights all requested outputs before either copyout |
| `TIME` | `SETITIMER` | `in` | one fixed itimerval | no | none | snapshots and validates before timer mutation |
| `TIME` | `GETITIMER` | `out` | one fixed itimerval | no | none | builds record before one copyout |

## VFS

| ABI family | Operation | Access | Maximum / bound | Nullable | Partial progress | Irversible-effect ordering |
| --- | --- | --- | --- | --- | --- | --- |
| `VFS` | `OPEN` | `in` | path C string up to 511 chars | no | none | path snapshot precedes open/create |
| `VFS` | `READ` | `out` | explicit length, copied in bounded bounce chunks; optional size_t result | buffer yes only for zero length; result yes | returns bytes already copied | descriptor checks precede null-buffer error; result output is preflighted before read |
| `VFS` | `WRITE` | `in` | explicit length, copied in bounded bounce chunks; optional size_t result | buffer yes only for zero length; result yes | returns bytes already written | descriptor checks precede null-buffer error; each input chunk is copied before write |
| `VFS` | `CLOSE` | `none` | n/a | n/a | none | closes descriptor |
| `VFS` | `LSEEK` | `out` | optional one `off_t` | yes | none | output is preflighted before changing file position |
| `VFS` | `ISATTY` | `none` | n/a | n/a | none | scalar query |
| `VFS` | `READ_DIR_ENTRIES` | `out` | caller maximum in chunks up to 16 KiB | yes only where backend zero/invalid semantics apply | returns complete bytes already copied | each chunk range is preflighted before advancing directory state |
| `VFS` | `MOUNT` | `in` | four path/data C strings, each up to 511 chars | source and data yes; target/type no | none | all strings snapshot before mount mutation |
| `VFS` | `MKDIR` | `in` | path C string up to 511 chars | no | none | snapshot before create |
| `VFS` | `READLINK` | `in/out` | path up to 511; output up to min(caller size, 512) | output yes only for zero size; path no | none | path snapshot and output preflight precede backend read |
| `VFS` | `SYMLINK` | `in` | two C strings up to 511 chars each | no | none | both snapshots precede create |
| `VFS` | `SENDFILE` | `in/out` | optional fixed `off_t`; transfer count is scalar | offset yes | transfer byte count | offset preflight/snapshot precedes transfer; copyback follows successful transfer |
| `VFS` | `STAT` | `in/out` | path up to 511 plus one fixed zero-initialized Stat | no | none | path snapshot precedes lookup; copyout only after success |
| `VFS` | `FSTAT` | `out` | one fixed zero-initialized Stat | no | none | descriptor lookup precedes copyout |
| `VFS` | `UMOUNT` | `in` | path C string up to 511 chars | no | none | snapshot before unmount |
| `VFS` | `DUP` | `none` | n/a | n/a | none | descriptor mutation only |
| `VFS` | `DUP2` | `none` | n/a | n/a | none | descriptor mutation only |
| `VFS` | `GETCWD` | `out` | NUL string within min(caller size, 512) | no | none | kernel path built and length checked before copyout |
| `VFS` | `CHDIR` | `in` | path C string up to 511 chars | no | none | snapshot before cwd mutation |
| `VFS` | `ACCESS` | `in` | path C string up to 511 chars | no | none | snapshot before lookup |
| `VFS` | `UNLINK` | `in` | path C string up to 511 chars | no | none | snapshot before unlink |
| `VFS` | `RMDIR` | `in` | path C string up to 511 chars | no | none | snapshot before removal |
| `VFS` | `RENAME` | `in` | two C strings up to 511 chars each | no | none | both snapshots precede rename |
| `VFS` | `CHMOD` | `in` | path C string up to 511 chars | no | none | snapshot before metadata mutation |
| `VFS` | `TRUNCATE` | `none` | n/a | n/a | none | fd and scalar length only |
| `VFS` | `PIPE` | `out` | exactly two integers | no | none | output preflight precedes creation; both FDs close if copyout fails |
| `VFS` | `PREAD` | `out` | explicit length through bounded bounce chunks | buffer yes only for zero length | returns bytes already copied | descriptor checks precede null-buffer error; offset is scalar and unchanged |
| `VFS` | `PWRITE` | `in` | explicit length through bounded bounce chunks | buffer yes only for zero length | returns bytes already written | descriptor checks precede null-buffer error; each chunk snapshots before write |
| `VFS` | `FCNTL` | `in/out` | command-specific scalar or fixed 32-byte flock record | flock no for record commands | none | record input and GET output are copied only at syscall boundary |
| `VFS` | `FCHMOD` | `none` | n/a | n/a | none | fd and scalar mode only |
| `VFS` | `CHOWN` | `in` | path C string up to 511 chars | no | none | snapshot before metadata mutation |
| `VFS` | `FCHOWN` | `none` | n/a | n/a | none | fd and scalar IDs only |
| `VFS` | `FACCESSAT` | `in` | path C string up to 511 chars | no | none | snapshot before lookup |
| `VFS` | `UNLINKAT` | `in` | path C string up to 511 chars | no | none | snapshot before unlink |
| `VFS` | `RENAMEAT` | `in` | two C strings up to 511 chars each | no | none | both snapshots precede rename |
| `VFS` | `EPOLL_CREATE` | `none` | n/a | n/a | none | descriptor creation only |
| `VFS` | `EPOLL_CTL` | `in` | optional fixed EpollEvent | yes where control operation permits | none | event snapshot precedes interest-list mutation |
| `VFS` | `EPOLL_PWAIT` | `out` | at most `EPOLL_MAX_INTEREST` EpollEvent records | no for positive maxevents | none | full output capacity preflight precedes wait/dequeue; backend count is bounded before copyout |
| `VFS` | `IOCTL` | `in/out` | closed PTY command table: int, int64, Winsize, KTermios, or no argument | only no-argument commands | none | bounce/preflight precedes backend ioctl; copyout follows success |
| `VFS` | `FSYNC` | `none` | n/a | n/a | none | fd-only sync |
| `VFS` | `LINK` | `in` | two C strings up to 511 chars each | no | none | both snapshots precede link creation |
| `VFS` | `WKI_RULE_ADD` | `in` | prefix C string up to 511 chars | no | none | snapshot before rule mutation |
| `VFS` | `WKI_RULE_GET` | `out` | prefix within caller capacity and 512-byte kernel bound plus one 32-bit route | no | none | both outputs preflight before backend query/copyout |
| `VFS` | `WKI_RULE_CLEAR` | `none` | n/a | n/a | none | rule-table mutation only |
| `VFS` | `PIVOT_ROOT` | `in` | two C strings up to 511 chars each | no | none | both snapshots precede namespace mutation |
| `VFS` | `WKI_RULE_GET_DEFAULT` | `out` | prefix within caller capacity and 512-byte kernel bound plus one 32-bit route | no | none | both outputs preflight before backend query/copyout |
| `VFS` | `STATVFS` | `in/out` | path up to 511 plus one fixed zero-initialized Statvfs | no | none | snapshot/lookup precede copyout |
| `VFS` | `FSTATVFS` | `out` | one fixed zero-initialized Statvfs | no | none | descriptor query precedes copyout |
| `VFS` | `LSTAT` | `in/out` | path up to 511 plus one fixed zero-initialized Stat | no | none | snapshot/lookup precede copyout |
| `VFS` | `SYNC` | `none` | n/a | n/a | none | global sync only |
| `VFS` | `REALPATH` | `in/out` | path up to 511; output NUL string within min(caller size, 512) | output yes only for zero size; path no | none | snapshot and capacity preflight precede lookup/copyout |
| `VFS` | `OPENAT` | `in` | path C string up to 511 chars | no | none | snapshot before open/create |
| `VFS` | `STATAT` | `in/out` | path up to 511 plus one fixed zero-initialized Stat | no | none | snapshot/lookup precede copyout |
| `VFS` | `UTIMENSAT` | `in` | path up to 511 plus optional array of exactly two Timespec records | times yes; path no | none | both records copy in one checked range before timestamp mutation |
| `VFS` | `MKDIRAT` | `in` | path C string up to 511 chars | no | none | snapshot before create |
| `VFS` | `READLINKAT` | `in/out` | path up to 511; output up to min(caller size, 512) | output yes only for zero size; path no | none | snapshot/preflight precede backend read |
| `VFS` | `LINKAT` | `in` | two C strings up to 511 chars each | no | none | both snapshots precede link creation |
| `VFS` | `SYMLINKAT` | `in` | two C strings up to 511 chars each | no | none | both snapshots precede create |
| `VFS` | `FCHMODAT` | `in` | path C string up to 511 chars | no | none | snapshot before metadata mutation |
| `VFS` | `FCHDIR` | `none` | n/a | n/a | none | fd-only cwd mutation |
| `VFS` | `FCHOWNAT` | `in` | path C string up to 511 chars | no | none | snapshot before metadata mutation |
| `VFS` | `FSTAT_CLOSE` | `out` | one fixed Stat plus one fixed integer result | no | none | both outputs preflight before the descriptor is irreversibly closed |
| `VFS` | `METADATA_BATCH` | `in/out` | header, at most 64 entry records, at most two 511-char paths per item, and 64 result records | second path operation-dependent | per-item status records; syscall copy is atomic | all input paths and the complete result range are snapshotted/preflighted before batch execution |
| `VFS` | `SETXATTR` | `in` | path up to 511 chars, name up to 255 chars, and value up to 65536 bytes | value yes only for zero size; path/name no | none | complete path/name/value snapshots and flag validation precede mutation |
| `VFS` | `LSETXATTR` | `in` | path up to 511 chars, name up to 255 chars, and value up to 65536 bytes | value yes only for zero size; path/name no | none | complete path/name/value snapshots and flag validation precede no-follow mutation |
| `VFS` | `FSETXATTR` | `in` | name up to 255 chars and value up to 65536 bytes | value yes only for zero size; name no | none | complete name/value snapshots and flag validation precede retained-fd mutation |
| `VFS` | `GETXATTR` | `in/out` | path up to 511 chars, name up to 255 chars, and output up to 65536 bytes | output yes only for zero size; path/name no | none | full output capacity preflight and input snapshots precede lookup; one complete copyout follows success |
| `VFS` | `LGETXATTR` | `in/out` | path up to 511 chars, name up to 255 chars, and output up to 65536 bytes | output yes only for zero size; path/name no | none | full output capacity preflight and input snapshots precede no-follow lookup; one complete copyout follows success |
| `VFS` | `FGETXATTR` | `in/out` | name up to 255 chars and output up to 65536 bytes | output yes only for zero size; name no | none | full output capacity preflight and name snapshot precede retained-fd lookup; one complete copyout follows success |
| `VFS` | `LISTXATTR` | `in/out` | path up to 511 chars and packed output up to 65536 bytes | output yes only for zero size; path no | none | full output capacity preflight and path snapshot precede lookup; packed list copies only after complete success |
| `VFS` | `LLISTXATTR` | `in/out` | path up to 511 chars and packed output up to 65536 bytes | output yes only for zero size; path no | none | full output capacity preflight and path snapshot precede no-follow lookup; packed list copies only after complete success |
| `VFS` | `FLISTXATTR` | `out` | packed output up to 65536 bytes | yes only for zero size | none | full output capacity preflight precedes retained-fd lookup; packed list copies only after complete success |
| `VFS` | `REMOVEXATTR` | `in` | path up to 511 chars and name up to 255 chars | no | none | complete path/name snapshots precede mutation |
| `VFS` | `LREMOVEXATTR` | `in` | path up to 511 chars and name up to 255 chars | no | none | complete path/name snapshots precede no-follow mutation |
| `VFS` | `FREMOVEXATTR` | `in` | name up to 255 chars | no | none | complete name snapshot precedes retained-fd mutation |

FCNTL command split: `F_GETLK/F_OFD_GETLK` snapshot one 32-byte flock,
preflight that same record, run VFS on the kernel record, then copy it back.
`F_SETLK/F_SETLKW/F_OFD_SETLK/F_OFD_SETLKW` snapshot the record before lock
state can change and do not copy it back. Scalar commands and the private flock
command do not treat `arg` as a pointer.

The VFS ioctl allowlist is closed. `TIOCGPTN`, `TIOCGWINSZ`, `TIOCGPGRP`,
`TCGETS` are output commands; `TIOCSPTLCK`, `TIOCSWINSZ`, `TIOCSPGRP`,
`TCSETS`, `TCSETSW`, `TCSETSF` are input commands; `TIOCSCTTY`, `TIOCNOTTY`,
and `TCFLSH` carry no pointer. Unknown commands return `-ENOTTY` rather than
forwarding a raw userspace address into a device backend.

## Networking

| ABI family | Operation | Access | Maximum / bound | Nullable | Partial progress | Irreversible-effect ordering |
| --- | --- | --- | --- | --- | --- | --- |
| `NET` | `SOCKET` | `none` | n/a | n/a | none | creates descriptor from scalar arguments |
| `NET` | `BIND` | `in` | sockaddr length at most 128 bytes; family parser requires its exact minimum | yes only for zero length | none | address snapshot precedes bind |
| `NET` | `LISTEN` | `none` | n/a | n/a | none | scalar socket mutation |
| `NET` | `ACCEPT` | `in/out` | size_t capacity plus sockaddr prefix at most 28 bytes | address pair yes; length required when address requested | none | address outputs preflight before accept; accepted socket is destroyed on copy failure |
| `NET` | `CONNECT` | `in` | sockaddr length at most 128 bytes; family parser requires its exact minimum | yes only for zero length | none | snapshot precedes connect |
| `NET` | `SEND` | `in` | explicit length in chunks up to 256 KiB | yes only for zero length | returns bytes already sent | each chunk snapshots before protocol call |
| `NET` | `RECV` | `out` | explicit length in chunks up to 256 KiB | yes only for zero length | returns bytes already copied | each chunk preflights before protocol receive; copied bytes are committed monotonically |
| `NET` | `CLOSE` | `none` | n/a | n/a | none | closes descriptor |
| `NET` | `SENDTO` | `in` | explicit data length plus optional domain sockaddr at most 28 bytes | address yes; data only for zero length | returns bytes already sent | address snapshot and each data chunk precede send |
| `NET` | `RECVFROM` | `out` | explicit data length plus optional domain sockaddr at most 28 bytes | address yes; data only for zero length | returns bytes already copied | output ranges preflight before receive; backend address over-report is `-EOVERFLOW` |
| `NET` | `SENDTO_EX` | `in` | fixed 32-byte descriptor, explicit data length, and optional sockaddr at most 128 bytes | address yes only for zero address length; data only for zero length | returns bytes already sent | descriptor/address snapshots and data chunk snapshot precede send |
| `NET` | `RECVFROM_EX` | `in/out` | fixed 32-byte descriptor, explicit data length, optional sockaddr capacity at most 128 bytes, and optional 8-byte result length | address/result pair yes when no address requested; data only for zero length | returns bytes already copied | every output range is preflighted before receive; full sockaddr length is reported separately from the copied prefix |
| `NET` | `SETSOCKOPT` | `in` | option length at most 65520 bytes | yes only for zero length | none | full option snapshot precedes protocol mutation |
| `NET` | `GETSOCKOPT` | `in/out` | size_t capacity plus option bytes at most 65520 | option yes only for zero capacity; length no | none | full output capacity preflight precedes backend; over-report is `-EOVERFLOW` |
| `NET` | `SHUTDOWN` | `none` | n/a | n/a | none | scalar socket mutation |
| `NET` | `GETPEERNAME` | `in/out` | size_t capacity plus sockaddr prefix at most 28 bytes | no | none | capacity/output preflight precedes backend query |
| `NET` | `GETSOCKNAME` | `in/out` | size_t capacity plus sockaddr prefix at most 28 bytes | no | none | capacity/output preflight precedes backend query |
| `NET` | `SELECT` | `in/out` | three optional 128-byte fd sets plus optional fixed timeval; nfds at most 1024 | all four pointers yes | none | snapshots and preflights sets before wait; ready sets copy back after success |
| `NET` | `POLL` | `in/out` | at most `FD_TABLE_SIZE` fixed pollfd records | yes only for zero nfds | none | complete array snapshot/preflight precedes wait; one complete copyback follows |
| `NET` | `IOCTL_NET` | `in/out` | closed command request of 40-byte ifreq or 48-byte route record | no | none | input snapshot and output preflight precede net-device/route mutation |
| `NET` | `SET_DEV_CPU_AFFINITY` | `in` | fixed 24-byte request | no | none | snapshot precedes queue-affinity changes |
| `NET` | `NETCTL_IF_LIST` | `in/out` | size_t capacity plus at most `MAX_NET_DEVICES` 52-byte records | record array yes for count query; capacity no | none | capacity/output preflight precedes complete list copyout |
| `NET` | `NETCTL_ADDR_LIST` | `in/out` | size_t capacity plus at most `2*MAX_NET_DEVICES*MAX_ADDRS_PER_IF` 76-byte IPv4/IPv6 records | record array yes for count query; capacity no | none | capacity/output preflight precedes complete list copyout |
| `NET` | `NETCTL_ADDR_SET` | `in` | fixed 48-byte request | no | none | snapshot/validation precede address mutation |
| `NET` | `NETCTL_ADDR_DEL` | `in` | fixed 48-byte request | no | none | snapshot/validation precede address mutation |
| `NET` | `NETCTL_LINK_SET` | `in` | fixed 68-byte request | no | none | snapshot/validation precede link mutation |
| `NET` | `NETCTL_ADDR_SET_V2` | `in` | fixed versioned 64-byte request | no | none | complete snapshot and reserved/flag/scope/lifetime validation precede IPv6 address mutation |
| `NET` | `NETCTL_ROUTE_LIST` | `in/out` | size_t capacity plus at most 64 fixed 64-byte IPv6 route records | record array yes for count query; capacity no | none | capacity/output preflight precedes complete bounded route snapshot copyout |
| `NET` | `NETCTL_ROUTE_SET` | `in` | fixed versioned 64-byte request | no | none | complete snapshot and reserved/flag/prefix/scope/gateway validation precede route publication |
| `NET` | `NETCTL_ROUTE_DEL` | `in` | fixed versioned 64-byte request | no | none | complete snapshot and reserved/flag/prefix/scope/gateway validation precede route removal |

## Virtual memory

| ABI family | Operation | Access | Maximum / bound | Nullable | Partial progress | Irreversible-effect ordering |
| --- | --- | --- | --- | --- | --- | --- |
| `VMEM` | `ANON_ALLOCATE` | `address` | checked page-aligned size within user space; hint is an address value | hint yes; size no | none | validates/reserves before mapping publication |
| `VMEM` | `ANON_FREE` | `range` | checked page-aligned user range | no | none | mapping mutation is serialized against stable pins and shared-task publication |
| `VMEM` | `PROTECT` | `range` | checked page-aligned user range | no | none | validates whole range before protection/page-table mutation |
| `VMEM` | `MREMAP` | `range` | checked aligned old/new sizes within user space | no | none | pins source/destination pages; old mapping is freed only after complete copy, new mapping rolls back on failure |
| `VMEM` | `MSYNC` | `range` | checked page-aligned user range, snapshotted one pinned page at a time | no | clean or nonresident pages are skipped; invalidation after a successful pin returns `-EFAULT` | dirty bytes are copied into a page-sized kernel buffer, then the pin is released before backend sync |
| `VMEM` | `SWAPON` | `in` | strict path C string up to 511 chars | no | none | snapshot precedes swap-state mutation |
| `VMEM` | `SWAPOFF` | `in` | strict path C string up to 511 chars | no | none | snapshot precedes swap-state mutation |

## Shared memory

| ABI family | Operation | Access | Maximum / bound | Nullable | Partial progress | Irreversible-effect ordering |
| --- | --- | --- | --- | --- | --- | --- |
| `SHM` | `GET` | `none` | n/a | n/a | none | scalar key/size/flags; size alignment overflow is rejected before allocation |
| `SHM` | `ATTACH` | `address` | checked segment-sized user range; requested address is a placement value | requested address yes | none | reserves/maps only after overflow and collision checks |
| `SHM` | `DETACH` | `address` | one previously attached base address | no | none | address lookup precedes detach |
| `SHM` | `CTL` | `out` | one fixed zero-initialized ShmidDs for `IPC_STAT`; other commands have no pointer | required for IPC_STAT | none | IPC_STAT snapshots under lock and copies after unlock; IPC_RMID has no usercopy |

## Power

| ABI family | Operation | Access | Maximum / bound | Nullable | Partial progress | Irreversible-effect ordering |
| --- | --- | --- | --- | --- | --- | --- |
| `POWER` | `REBOOT` | `none` | n/a | n/a | none | scalar power command |
| `POWER` | `GET_STATE` | `none` | n/a | n/a | none | scalar query |
| `POWER` | `PREPARE` | `none` | n/a | n/a | none | scalar shutdown-state mutation |

## Ptrace request surface

These rows refine `PROCESS.PTRACE`. Target-address ranges use the target
pagemap and stable frame pins; descriptor and buffer addresses use the tracer
pagemap. Tracer ABI layout and register ordering are unchanged.

| ABI family | Operation | Access | Maximum / bound | Nullable | Partial progress | Irreversible-effect ordering |
| --- | --- | --- | --- | --- | --- | --- |
| `PTRACE` | `TRACEME` | `none` | n/a | n/a | none | trace relationship mutation only |
| `PTRACE` | `PEEKDATA` | `out` | pinned 8-byte target range plus one 8-byte tracer output | no | none | target word snapshots before tracer copyout |
| `PTRACE` | `POKEDATA` | `range` | pinned 8-byte target range, including two-page boundary | no | none | every touched page pins before either byte is written |
| `PTRACE` | `CONT` | `none` | n/a | n/a | none | resumes tracee |
| `PTRACE` | `KILL` | `none` | n/a | n/a | none | signals tracee |
| `PTRACE` | `SINGLESTEP` | `none` | n/a | n/a | none | mutates saved kernel context and resumes tracee |
| `PTRACE` | `GETREGSET` | `in/out` | fixed RegsetIo plus bounded nested GPR or XSAVE output | no | descriptor size may report required/produced bytes | descriptor and nested output preflight precede copyout |
| `PTRACE` | `SETREGSET` | `in` | fixed RegsetIo plus bounded nested GPR or XSAVE input | no | none | complete nested snapshot precedes saved-register mutation |
| `PTRACE` | `ATTACH` | `none` | n/a | n/a | none | trace relationship mutation only |
| `PTRACE` | `DETACH` | `none` | n/a | n/a | none | detaches/resumes tracee |
| `PTRACE` | `SYSCALL` | `none` | n/a | n/a | none | resumes tracee in syscall-stop mode |
| `PTRACE` | `SETOPTIONS` | `none` | n/a | n/a | none | scalar trace options only |
| `PTRACE` | `GETEVENTMSG` | `out` | one fixed Event | no | none | builds kernel event before copyout |
| `PTRACE` | `SEIZE` | `none` | n/a | n/a | none | trace relationship mutation only |
| `PTRACE` | `INTERRUPT` | `none` | n/a | n/a | none | stops tracee |
| `PTRACE` | `LIST_THREADS` | `in/out` | fixed ThreadList plus capacity-bounded TID array no larger than active task count | descriptor no; array yes for count query | descriptor count reports total | descriptor/nested output preflight precede copyout |
| `PTRACE` | `READ_MEM` | `in/out` | fixed MemIo plus checked target range and tracer output of requested size | no | descriptor `transferred` records copied prefix | target pages and tracer chunks are bounded; descriptor preflight precedes transfer |
| `PTRACE` | `WRITE_MEM` | `in/out` | fixed MemIo plus checked tracer input and target range of requested size | no | descriptor `transferred` records written prefix | input chunks snapshot before pinned target writes; descriptor preflight precedes transfer |
| `PTRACE` | `GET_MAPS` | `none` | n/a | n/a | none | currently returns `-ENOSYS` |
| `PTRACE` | `GET_IMAGES` | `in/out` | fixed ImageList plus at most two fixed ImageRecord outputs | descriptor no; array yes for count query | descriptor count reports total | output preflight precedes record copyout |
| `PTRACE` | `GET_REMOTE_INFO` | `out` | one fixed 104-byte RemoteInfo | no | none | builds zero-initialized record before copyout |
| `PTRACE` | `SET_HW_BREAK` | `in` | one fixed HwBreak | no | none | snapshot/validation precede debug-register mutation |
| `PTRACE` | `DEL_HW_BREAK` | `in` | one fixed HwBreak | no | none | snapshot/validation precede debug-register mutation |
| `PTRACE` | `SYSCALL_WAIT` | `out` | one fixed 224-byte StopInfo | no | none | output preflight precedes wait/stop consumption |

## Error and concurrency rules

- Null, kernel-half, noncanonical, wrapped, unmapped, and permission-invalid
  non-empty ranges fail with `-EFAULT` unless the operation's established ABI
  uses `-EINVAL` for a mapping-range argument.
- Cross-page fixed records are all-or-fault. Streaming I/O and the explicitly
  documented log/ptrace transfers return only committed prefix progress.
- Read-only output mappings fail before a preflighted operation begins. COW and
  lazy pages are materialized only through the checked writable path.
- Mapping replacement, detach, leaf mutation, COW commit, and stable frame-pin
  acquisition share the pagemap/user-mapping serialization contract. Physical
  frame references use a live-only increment so a page cannot be recycled
  during a copy. Successful HHDM copyout commits the matching user leaf's
  dirty bit atomically, preserving shared-file `msync` visibility.
- Usercopy never follows nested pointers after releasing the snapshot that
  contained them: exec/spawn, clone, metadata batch, ptrace descriptors, socket
  length records, and command-dependent records snapshot and validate each
  layer explicitly.

## Verification layers

The host manifest and raw-access audits close the operation inventory and the
syntactic/transitive syscall boundary. The isolated MM KTEST cases exercise
stable pins, null and forbidden ranges, cross-page partial progress, read-only
outputs, lazy materialization, COW isolation, and pagemap-exclusive mutation.

The opt-in command `testprog usercopy-negative` is the user-run runtime evidence
collector. It checks null, user-limit/noncanonical, kernel-range, wrapped, and
partially mapped addresses against every pointer-bearing syscall family,
including the PTRACE sub-surface. It separately checks read-only outputs, lazy
copyout, COW parent isolation, and a shared-pagemap `mmap`/`munmap` race. The
probe accepts only success or `-EFAULT` during the race, does not mutate VFS or
network state, and removes its private SHM segment before returning.

## Completion evidence

The contract was completed on 2026-08-09 with the following evidence:

- Normal `Build WOS` completed successfully.
- The host suite passed all 143 tests, including the manifest, raw-access,
  usercopy, VFS, network, process, time, VM, and ABI source checks.
- A user-run isolated `bin/wos-ktest` pass exercised the MM/usercopy negative
  and concurrency cases under the diagnostic profile.
- User-run local and multi-node `testprog usercopy-negative` passes each
  completed 4,058 checks with zero failures. The multi-node invocation used
  `remotely testprog usercopy-negative`, covering the real WKI placement path.

Future changes to a pointer-bearing syscall, usercopy, VM mapping lifetime,
VFS I/O, socket I/O, or a deferred completion path must rerun the focused host
checks and Normal `Build WOS`. Changes affecting mapping concurrency or
non-current tasks must also rerun `bin/wos-ktest`, local
`testprog usercopy-negative`, and the multi-node `remotely` invocation. Retain
the participating serial logs; do not infer cross-node ordering when global
timestamps are incomplete.
