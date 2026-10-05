# Perf capture completeness

`perf record` and `perf run` save a `CAPTURE_STATUS` section in `perf.data`.
Structured conversion and `perf data-export` also emit its JSON envelope as
`perf.capture_receipt`. Older snapshots without the section remain readable.

The receipt assesses the event stream admitted by the requested filter. It
records the filter string and the effective kernel mask, CPU count, session,
control version, events written/consumed/overwritten/pending, events and bytes
saved, and read/write/control/limit failures. `semantic_span_completeness` is
`not_assessed`: a lossless stream can still contain an unfinished operation,
or exclude an operation because of its filter or capture start/stop boundaries.
Events appear in CPU order in each procfs snapshot, not global timestamp order.

`event_stream_complete=true` requires a witnessed enabled start, the same
session/mask/CPU count at the disabled finish, exactly the recorder's one stop
control operation, no overwritten or pending events, every consumed event saved,
and no I/O or capture-limit failure. Another reader's consumption, control
changes, an unavailable status interface, and loss all prevent this claim.
Incomplete captures remain useful evidence; `record`/`run` report incompleteness
and return failure while preserving the file when it can be written.

The read-only `/proc/kperfstatus` line is versioned:

```text
perf_capture version=1 session=... control_version=... enabled=0 mask=... cpus=... written=... consumed=... overwritten=... pending=...
```

It uses the same kernel-diagnostic read authorization as `/proc/kperf`.
`/proc/kperfctl` still reads exactly `enabled\n` or `disabled\n`; event rows,
the 48-byte binary event, syscalls, and the structured container version are
unchanged. Always-on scheduler counters survive recording resets. Capture
counters reset with each new session.

Procfs consumes an event only after its complete text line fits. Rejected events
remain queued. The final drain repeats snapshots after recording stops, with
bounds of 1,024 reads and 6 MiB of saved event text. An oversized queued line,
read/write failure, or limit is explicit incomplete evidence. Events consumed
before an output failure cannot be recovered; the saved count and failure flag
prevent a false completeness claim. The global recording control and destructive
stream still support one intended recorder at a time; receipts detect interference.

Exec trace ownership
--------------------

An `execve` attempt begins after its task-owned request is admitted, before
faulting argument snapshotting. Snapshot copying has an `arg_copy` span; each
recursive loader also records its immutable argument inspection. Every shebang
depth keeps its own `execve` correlation, capped by the existing recursion limit.
The enclosing attempts end deepest first after successful publication and
synchronous cleanup, or with the exact failing errno. An unfinished substage
ends with that failure; fatal request/image recovery reports `-ECANCELED`.
Failures before request admission do not create an attempt span.

Native trace state moves with the tracked prepared image after recursive loader
frames unwind. A remote worker's trace moves with the existing claimed request;
optional local fallback or refusal returns it before the worker's result ACK.
Successful remote completion keeps trace ownership beyond the early worker ACK
and request destruction. Events retain the preparing actor's full subject PID
as a value even when completion executes on the canonical owner. Recording a
new stage while its scope/operation is disabled uses no timing reads or trace
correlations; inactive stages cannot acquire an END after a later enable.

These rules do not change capture receipts: semantic span completeness remains
`not_assessed`. To qualify a lifecycle, check its subject/op/correlation pairs and
exact outcomes in addition to the loss/control receipt. Capture boundaries and
filters can still omit an attempt's beginning or end.

Native image return
-------------------

`first_run` remains the task's initial scheduling event, including the child
returning from fork before it calls exec. It does not identify entry into a new
exec image. A successful captured native exec instead retains its root attempt's
correlation, subject PID, recording session and initial RIP through synchronous
cleanup. Failed exec preserves the previous image's marker; forked tasks do not
inherit a pending marker.

At the final kernel return to that exact RIP, two `local_proc` POINTs share the
root `execve` correlation: `exec_image_owner` identifies the canonical execution
PID and `exec_user_return` identifies the preparing subject PID. These can differ
for a worker exec. Both PIDs retain their full 64-bit width. `aux` identifies the
return tail: 1 normal SYSRET, 2 signal IRET, 3 scheduler IRET, or 4 synthetic
initial/owner entry. A signal-handler redirect leaves the marker pending until
a later return to the staged image's initial RIP; the marker consumes once.

The timestamp describes the kernel handoff before userspace executes. Lazy
entry-page faults, loader work, libc startup and the command body occur after
this boundary and still need their own measurements. It is not a timestamp of
the first userspace instruction. Spawn/remote construction without a captured
native exec attempt does not invent a local correlation or this marker.

Exec stages and deferred image POINTs retain their original capture session.
The ring and summary admission checks reject a different session under their
existing locks, preventing reset's reused correlation numbers from attributing
an old image or endpoint to a new capture. Missing endpoints caused by capture
boundaries/filtering still require semantic analysis; the event-stream receipt
continues to report semantic span completeness as `not_assessed`.

Fork child association
----------------------

A successful captured parent `fork` span emits a `fork_child` POINT after child
publication succeeds and before its END. The POINT carries the child's full
64-bit PID and the parent's exact FORK correlation; BEGIN/END retain the calling
parent task's full PID. `aux` carries the child's CPU. The legacy `fork` child
POINT keeps its independent correlation and CPU meaning. An error span emits no
child link. Child scheduling, exec or exit can precede the parent-side POINT;
the link identifies the attempt and child, rather than the first child run.

Fork BEGIN, both child POINTs and END/summary bind the original recording
session. Disabled or excluded fork instrumentation skips clock/correlation
work. Reset, filter changes and capture boundaries can omit the link or END,
which must be reported as incomplete semantic evidence rather than inferred.

Userspace startup and body
--------------------------

A captured native root exec can advertise version 1 of the optional startup
trace capability in three private auxv pairs: `0x57505301` (version),
`0x57505302` (full 64-bit capture session), and `0x57505303` (root exec
correlation). Session and correlation zero are valid. Absent, partial,
duplicate or unsupported capability data emits no userspace trace syscalls.
Old kernels omit the capability; old libc ignores the additional auxv pairs.
The six extra stack words preserve existing stack alignment and fit the
native inline auxv storage. Spawn and remote construction do not advertise a
native root correlation they do not have.

`local_proc.userspace_stage` (operation 19) POINTs carry the canonical execution
PID and root exec correlation. `aux` identifies the milestone:

| aux | Milestone | Boundary |
| --- | --- | --- |
| 1 | IMAGE_ENTRY | Early interpreter entry before self-relocation, or static CRT entry |
| 2 | RTLD_READY | After linking, TLS and constructors, before returning to executable entry |
| 3 | CRT_ENTRY | Executable CRT entry before main setup |
| 4 | MAIN_BEGIN | Immediately before invoking main, after libc startup |
| 5 | MAIN_END | Immediately after main returns, before exit |

Dynamic startup orders these points 1, 2, 3, 4, 5; static startup orders them
1, 3, 2, 4, 5. A main function that execs or exits directly produces no
MAIN_END. Measure its body through the separately recorded next exec or exit
boundary instead of inventing a return. The initial image point's timestamp
is kernel receipt of its numeric marker syscall after entry-page faults and
the assembly stub's auxv scan. It is not the exact first user instruction.
The gap from `exec_user_return` retains that residual and marker overhead.

The private marker uses logging subsystem 0, appended operation 5, with
numeric phase/session/correlation/version/reserved arguments and no user
pointers. It bypasses logging gates and copying. Kernel ownership, completed
image handoff, exact session/correlation, ordering prerequisites and a
once-per-image stage mask guard admission; existing perf locks reject capture
resets. The user-supplied milestone is diagnostic, not identity attestation.
Early assembly preserves registers, flags and stack and needs no calls,
GOT/PLT, TLS, allocation or loader relocations. Later libc markers use the
WOS `PerfTrace` sysdep. Existing syscall operations, WKI wire, 48-byte event
layout, procfs, JSON and perf.data formats are unchanged. Semantic completeness
still requires checking actual image milestones and capture boundaries.

Profiling parent observer stages
--------------------------------

`perf run --observer-stages --filter=local_proc,switch,wake,sleep -- /bin/true`
adds a version 1 `run_observer` object to the existing capture receipt. Without
this option, the command makes no observer clock reads and emits no observer
object. Options after `--` are passed to the command.

The object identifies the profiling parent's full 64-bit PID, the direct
child's full 64-bit PID and the capture session (zero is valid). Bind those
PIDs to the exact `fork_child` association before comparing child EXIT and
parent WAIT events. The observer does not invent a kernel correlation.

`spans` contains tuples `[phase, begin_ns, end_ns, result]`. A boundary sample
ends one phase and starts the next, giving contiguous, nonoverlapping wall
intervals. `result` is the signed decimal wait result for `wait_probe`, or
null. `parent_setup`, `data_open`, `timebase_write` and `initial_scan` cover
work before the first wait probe. Later phases are `wait_probe`, `loop_control`,
`group_scan`, `sleep` and `ring_drain`; completion phases are `final_reap`,
`capture_stop`, `final_stats`, `final_report` and `final_drain`.

Within the initial and subsequent group scans, `proc_stats` brackets the
existing enumeration and collection of main-process stats, `proc_images`
brackets each tracked image-map read, and `proc_cmdline` brackets each newly
tracked command-line read. These detailed intervals replace portions of
`initial_scan` or `group_scan`; those names cover the remaining bookkeeping.
All intervals remain contiguous and nonoverlapping. Reading the snapshots
still precedes reaping, retaining the existing opportunity to collect final
CPU and image metadata. A process that exits before its stat read can be
absent from the scan; no image or command-line interval is invented for it. Repeated
group scans can exhaust the same 32-span bound sooner; no parser limit is
raised to conceal this clipping.

Samples use the kernel's boot monotonic clock, the same origin as perf event
timestamps. Quality is `syscall_sample`: intervals include observer overhead
and scheduling delays. They measure wall time, not CPU time. The interval
between fork's parent return and its first sample, and metadata writing after
the final drain sample, lie outside the measured window. For an exit-to-wait
gap, intersect each interval with those exact kernel endpoints and report any
uncovered residual. Do not assign the entire gap to the existing 10 ms sleep.

The trace reserves space before fork and retains at most 32 spans. Once full,
it counts `dropped_spans` and skips further observer clock reads while the
command and capture continue normally. Missing or backward samples set
`clock_failed`; invalid transitions set `invalid_state`. `complete` requires
a finished, continuous, unclipped observer window and a complete event stream.
Observer incompleteness does not itself make the event stream incomplete or
change the command's result. Existing receipt/parser limits, JSONL record
counts, perf.data sections and container version remain unchanged.
