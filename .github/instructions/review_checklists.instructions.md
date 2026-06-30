---
applyTo: "**"
---

# WOS Review Checklists

Use these checklists for code review and self-review.

## General correctness review

- Does the change solve the stated problem with minimum surface area?
- Are unrelated changes avoided?
- Are source comments still truthful?
- Are error paths handled?
- Are allocation failures handled?
- Are object lifetimes and ownership clear?
- Are locks acquired and released in all paths?
- Is the build/test plan adequate?

## Kernel review

- Is the code valid without exceptions and RTTI?
- Is it safe in its execution context?
- Does it allocate or block in an unsafe path?
- Does it log safely?
- Are atomics and memory ordering appropriate?
- Are user pointers validated where applicable?
- Are return values and errno conventions consistent?

## Syscall review

- Are syscall numbers/op enums unchanged unless intended?
- Is register argument mapping correct?
- Is raw syscall negative errno converted correctly at wrappers?
- Are userspace/libc wrappers updated with kernel changes?
- Are ABI structs and constants compatible?
- Does ptrace syscall-stop behavior remain correct for WOS `strace`?

## Scheduler/task review

- Is the subject task distinct from current task when needed?
- Are task state transitions atomic and ordered correctly?
- Are dead-list/epoch/GC paths preserved?
- Are scheduler locks held across only safe operations?
- Does WKI remote placement avoid recursive remote submission?

## VFS review

- Does path resolution respect mount boundaries?
- Are symlinks handled intentionally?
- Are remote mounts protected from recursion?
- Are file descriptor lifetimes and `FileOperations` pointers valid?
- Are caches invalidated on writes/closes/readdir changes?

## WKI review

- Are packed wire structs and size assertions preserved?
- Are payload lengths validated before casts/copies?
- Is the correct channel used?
- Are credits, seq/ack, retransmit, and ACK-pending state preserved?
- Are peer states and fencing paths handled?
- Are RX/NAPI contexts free of ACK-waiting operations?
- Are spin-wait loops able to make progress?
- Are remote VFS paths bounded and recursion-safe?
- Are proxy/server lifecycle and reconnect/teardown semantics preserved?

## Logging review

- Does new routine kernel logging use the journal-backed logger?
- Are direct serial writes limited to allowed paths?
- Were hot-path logs added only when safe?
- Are disabled debug logs converted to trace/debug instead of ad hoc serial output?
- Is `JournalRecord` ABI compatibility preserved when touching journal code?

## WOS userspace utility review

- Did you read the local utility source instead of assuming GNU/Linux/systemd/procps/upstream behavior from the command name?
- Are rootfs aliases in `configs/rootfs/aliases.tsv` still accurate?
- If kernel-facing, are procfs/device/syscall/ptrace/perf/journal formats still compatible with the matching utility?
- Do option usage/help text and implementation still agree?
- Is runtime validation explicitly user-run when it requires WOS or a cluster?

## libc/mlibc review

- Is the local libc tree verified first?
- Does the change use mlibc tag sysdeps when applicable?
- Are errno conventions correct?
- Does behavior aim for broad compatibility rather than app-local hacks?
- Are signal-safety requirements met?
