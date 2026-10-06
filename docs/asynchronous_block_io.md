# Asynchronous block I/O

`WOS_ASYNC_BLOCK_IO` gates native queue registration and worker startup. The
legacy synchronous callbacks remain available. The authoritative interfaces
are `modules/kern/src/dev/block_request.hpp` and `block_request_model.hpp`;
the implementation is described below. Local qualification receipts live in
`build/goal30-evidence/` and are kept separate from committed source.

Fresh builds default to `WOS_ASYNC_BLOCK_IO=ON` after the qualification below.
`-DWOS_ASYNC_BLOCK_IO=OFF` selects the tested synchronous compatibility profile.
Existing CMake caches retain their explicit option values. The optional wait
diagnostics remain OFF for normal builds and performance qualification.

Each physical disk has one permanent queue shared by its partition aliases.
The registry holds at most 64 queues; retired queues are not reused. Each
queue bounds admission to 64 retained requests and 32 scatter segments per
request. Current AHCI and WKI registrations use an 8 MiB byte budget. The
global transfer ceiling is 16 MiB; provider limits can be smaller. Overlapping
transfers preserve submission order whenever either transfer writes. A flush
joins every earlier transfer and blocks every later transfer. Cancellation is
an intention until the provider retires transport access; caller descriptors
and kernel buffers remain live through callback return and explicit release.
Task service gathers completions under one bounded queue guard, then runs
callbacks outside it and publishes each result after callback retirement.
Each serialized service pass calls the provider before admission and retires
callbacks afterward. Every submission attempt requires another provider call,
including rejected attempts. A healthy native pass with no attempt can omit
that second call after checking lifecycle, generation and cancellation under
the queue guard. Reset, close, cancellation and caller-driven service retain
it, followed by the final callback scan. Retained staged requests continue
through later timed worker service or caller service; their polling cadence
can differ.
A separate task-service completion entry can omit the worker wake while the
matching retained task owns a provider call followed by that mandatory gather.
The gather clears this scope before callbacks run. Generic IRQ and external
completions always wake the worker; callbacks and foreign waiters retain their
existing wake behavior.
Callbacks cannot wait on their own queue
or destroy their descriptor. Synchronous calls stage resident user buffers in
the caller's address space. Ordinary VFS syscalls materialize and pin cold
buffers through the existing software usercopy path; direct block callers
must supply resident buffers. Boot and finalizer waits explicitly service the
queue, so completion does not depend on a worker being present.

AHCI negotiates NCQ using HBA CAP and IDENTIFY words 75, 76 and 83, with at most
eight slots and a separate retained 512 KiB DMA bounce pool for each slot.
Non-NCQ and flush commands remain exclusive. Submission prepares and retains
DMA while its slot stays preparing. Bounded service publishes the prepared
batch with one PxSACT mask followed by one PxCI mask; clear hardware bits
cannot complete a slot before that publication. Staged cancellation, timeout
or admission closure retires unissued DMA with `never_submitted` proof.
IRQ handlers only publish/acknowledge status and
wake deferred service. Task service retires DMA and read copying before
claiming completion outside port locks. Uncertain commands fail together.
IRQ and service share a bounded slot guard for status publication and
completion qualification. Service reads active bits before error latches and
claims eligible completions before releasing that guard, so IRQ acknowledgment
cannot hide a fatal error between the two observations. Reset publication
rechecks hardware and deferred faults after IDENTIFY.
For a sole data-free flush or a sole newly published READ or WRITE, service
takes one fresh full hardware and error snapshot after the doorbell readback,
including when that readback still shows activity. READ and WRITE eligibility
requires matching request/slot identity and positive request, slot and
DMA-submission bytes with a live submission sequence. Multiple owners retain
the ordinary path. Fresh active bits prevent completion; readback alone never
establishes success. Existing DMA retirement checks still apply, including
exact PRDBC bytes for non-NCQ commands; NCQ does not use PRDBC as completion
evidence. A successful READ copies into its retained SG only after DMA
retirement. Cancellation can still override final status after hardware
qualification. A completed WRITE requires its explicit FLUSH for durability.
Hardware tag reuse follows proven engine stop, COMRESET and an exclusive
IDENTIFY that verifies unchanged geometry, capabilities and serial identity.
Reset-signature task-file status is qualified by that actual command; fatal
status checks remain active for every admitted native operation. Unproven stop
quarantines driver backing. Software generations never substitute for hardware
retirement, and uncertain writes are never retried as successful writes.

Buffer-cache demand reads retain one stack descriptor for a single slice
and use bounded eight-context pipelines for larger spans and dirty writeback. Writeback retains snapshots, buffer references and dirty epochs
through completion; its join token remains published through final cleanup.
Writeback checks for captured dirty work under the cache lock before allocating
a run descriptor, then rechecks selection after allocation. A sole prepared
run uses callback-free admission followed immediately by wait and release;
multiple runs retain worker wakes for progress during later refills. Foreign
writeback joins, dirty generations and durability barriers remain unchanged.
Run descriptors try one non-reclaiming full-overwrite physical metadata
allocation and construct every member before publication. Fragmentation uses
the existing fallible heap allocation. Cleanup frees each backing through its
allocator after request, snapshot, buffer-reference and registry joins.
XFS retains its existing mapping and log/durability ordering. Swap retains
slot generation, page and area ownership through request retirement and CRC
validation. WKI uses the serial authenticated RPC adapter at depth one; fencing
closes/resets queue admission and joins RPC/buffer ownership before replacing
the binding. This changes no WKI wire structure.

Deferred user FILE faults retain the task, pagemap, file and range identity
until task-context materialization and PTE publication finish. Admission uses
a fixed 64-entry pool. Kernel-mode hardware FILE faults cannot enter this
exception parking path and still terminate through the kernel fault handler;
ordinary software-prefault usercopy remains supported. This does not establish
safety for raw kernel user-pointer dereferences or a concurrent remap of
user-supplied TLS between initial prefault and first dispatch.

Block, FILE and workqueue workers retire publication references and shutdown
holds before calling the scheduler's explicit kernel-thread exit helper.
Qualified PROCESS syscall waits use the existing syscall-safe park even when
syscall entry has masked interrupts. A native queue with depth greater than
one may first admit up to 32 extra service passes within a fixed 128 microsecond
window. Service mutex contention immediately takes the existing wait path.
This window limits when passes start; an existing callback or recovery pass
can take longer. An unfinished request uses a 16 microsecond retry deadline
after this window or service mutex contention. This requests a recheck;
scheduler contention can delay actual dispatch. Each extra pass rechecks task safety, request generation,
deadline, cancellation and queue admission. Pending signals use the existing one-shot
yield while retained I/O retires. Boot, panic, preemption-disabled and unsafe
deferred-wait contexts retain their nonparking fallback.

An optional provider hook applies only to those extra passes. AHCI may retain
a batch of at least two healthy active NCQ commands after checking CI, SACT,
IS, all slot identities and current deadlines. That path makes no state change,
completion claim, DMA retirement, copy or status acknowledgement. An observed
completion, event or invalid condition falls back to the full fresh service
path. Mandatory PRE service and every required POST service remain full. After
full PRE and its first completion gather, a healthy mandatory pass may return
under the existing selection guard when finite submitted/completed equality
proves no unfinished request. Retained terminal owners may remain charged.
Extra polls, saturation, lifecycle changes and required POST retain the full
tail and second gather. The empty check is instantaneous; new admission plus
cancellation after unlocking progresses through the existing wakes and joins.
A TFD-only fault
stays retained until the next mandatory full pass, and the poll admission
budget does not promise a strict wall-time error detection bound. The hook
saves a TFD read and an IS acknowledgement on a retained active batch; its
performance benefit requires fresh qualification.

Request selection counts issued owners and checks hardware depth in one
guarded scan. With asynchronous I/O enabled, that scan can also select the
oldest queued read when every unfinished owner is a read. Any queued, issued
or completing write or flush retains the existing conflict selector.
Completing owners keep their ordering authority without consuming an issued
hardware slot. The combined scan changes no callback or provider-service
boundaries. Saturated queues do extra eligibility checks; its performance
tradeoff requires qualification.

`WOS_BLOCK_WAIT_DIAGNOSTICS` is disabled by default. When enabled,
`/proc/blockq` adds bounded cumulative wait-exit counters, published when a
wait returns. They distinguish completion, context, contention, eligibility,
clock regression, time budget, deadline and pass-limit observations. They
include background callers and do not establish distinct scheduler admissions
or exclusive elapsed cost. Counter saturation prevents exact count inference;
an elapsed sample after the pass limit requires a monotonic clock. Instrumented
timings are excluded from performance qualification.

Ordinary asynchronous submission wakes the service worker. Callback-free
batch callers can use `block_request_submit_for_wait()` and immediately
service or wait for every accepted request before returning or blocking on
other work. This avoids an admission wake while preserving completion, IRQ,
cancellation and reset wakes. AHCI publishes accepted staged slots in the
service pass that follows the issue loop.

Native XFS reads can copy a complete cached range directly into a stable
kernel destination before allocating another buffer. Incomplete coverage
falls back to the referenced read, which overwrites any partial cache copy.
After a sequential read on the same open file, qualified syscall contexts can
aggregate up to 4 MiB synchronously for aligned demands between 64 KiB and
512 KiB, bounded by the current written extent,
allocation group, full-block EOF and provider limits. Only the predicted next
offset survives the call; mappings and size are resolved under the inode I/O
lock each time. Supplemental failure retires all requests before retrying the
demanded range. The kernel address check selects this path under the existing
stable-buffer contract; it does not validate arbitrary pointers.
Direct cache copies remain limited to 512 KiB. Existing large-demand batching
keeps its 2 MiB limit. The enabled cache data allocation ceiling is 4 MiB so an
aggregated buffer can later be dirtied and written through a joined snapshot;
dirty-run aggregation and the disabled allocation ceiling remain 2 MiB.
Enabled, page-aligned power-of-two buffers above 64 KiB try one full-overwrite
buddy allocation. Fragmentation retains the existing vmap fallback. This
attempt does not enter direct reclaim or expand the requested size.

Enabled XFS transaction before-images can use exact page backing for
page-aligned power-of-two sizes from 4 KiB through 64 KiB. One fallible,
non-reclaiming allocation precedes the original heap fallback. Capture fully
initializes the private copy before publishing it; commit, cancellation and
error cleanup free it through its recorded allocator. Canonical alias copies,
journal holds and rollback ordering remain unchanged. The disabled profile
retains the original heap allocation.

## Test navigation

All targets are registered in `tests/host/CMakeLists.txt` and listed by
`bin/wos-test list`. `buffer_cache_async_test.cpp` is a second translation unit
of `buffer_cache_test`, so source inventory checks accept a source registered
in that combined executable while still rejecting missing and stale sources.

| Target | Executed code and scope |
| --- | --- |
| `block_request_model_test` | Production model: overlap/flush ordering, exact-once claims, cancellation, terminal owner budgets, generations, partition/SG arithmetic. |
| `block_read_arbiter_test` | Actual request-model header in both profiles: independent selector oracle, hardware depth, slot reuse, mixed operations, completing barriers, cancellation/reset and bounded scan work. |
| `block_request_fault_test` | Fault device: delayed/reordered delivery, torn writes, failed/unsupported flush, uncertain timeout and reset ownership. |
| `block_request_runtime_test` | Actual queue runtime with scheduler/backend seams, callbacks, waits, backpressure, partitions and reset races. |
| `block_wait_diagnostics_test` | Full production wait and polling constants in both probe profiles: independently expected retry deadline, reason precedence, finite polling, completion races, reference/lock balance and saturation. |
| `block_worker_runtime_test` | Actual queue and reclaim runtime with real host threads: failed worker creation/post, temporary wake references, shutdown/admission races, exact owner retirement, provider/callback NoIo scopes, explicit clean exit and finalizer sync after worker exit; IF-off qualified syscall waits resume a service mutex owner while preserving internal return-yield markers; sticky signals use the existing one-shot yield until buffers retire, and unsafe wait contexts retain CPU pause. |
| `block_wait_poll_predicate_test` | Actual polling predicates: task context, signal/shutdown, request identity, generation, cancellation, deadline, reset/admission and caller-driven exclusions. |
| `xfs_read_compiled_test`, `xfs_read_runtime_test`, `xfs_read_disabled_runtime_test` | Complete extracted XFS read with the actual buffer cache: dirty/retired aliases, no-allocation cache hits, partial-copy fallback, bounded sequential reads, fresh mapping/EOF authority, failed supplemental ownership retirement, 4 MiB dirty snapshot writeback and disabled-profile compatibility. |
| `block_request_model_fuzz_smoke` | Bounded CTest smoke run of the production state/arithmetic model; longer campaigns use `bin/wos-test fuzz block_request_model_fuzz`. |
| `ahci_async_test` | Production slot rules: eight tags, out-of-order retirement, exclusive flush, stale generations, timeout/reset and offline/quarantine admission. |
| `ahci_async_integration_test` | Verbatim native and polled driver functions with real layouts and instrumented DMA/MMIO: eight prepared requests/one doorbell, staged versus issued retirement, CI versus SACT, observed depth, IRQ/status races, read copy, cancellation, errors, COMRESET/IDENTIFY qualification, changed-device rejection and quarantine. |
| `ahci_controller_init_integration_test` | Complete controller-init body and production MMIO flag helper: masked BAR, checked high-half placement, both page endpoints, invalid-input BME closure, PCI coherence and lifecycle ordering. |
| `buffer_cache_test` | Actual cache implementation including async demand reads/writeback, redirty protection, backpressure, errors and cleanup joins. |
| `buffer_cache_allocation_runtime_test` | Native allocation and release bodies in both profiles: exact buddy sizing, one non-reclaiming attempt, vmap fallback, fragmentation, failure and balanced backing ownership. |
| `xfs_undo_allocation_runtime_test` | Actual transaction capture, rollback and cleanup with allocator seams: exact backing sizes, full initialization, heap fallback, allocation errors, canonical aliases, journal/reference balance and allocator-specific release in both profiles. |
| `swap_async_test` | Actual swap/request code: retained slot/page ownership, CRC/error handling and slot retirement. |
| `workqueue_runtime_test` | Actual worker queue: retained task launch context, active callback join, close rejection, destruction and explicit clean exit after task-context and shutdown-hold retirement. |
| `reclaim_runtime_test` | Actual reclaim coordinator: task-bound nested NoIo scopes, clean reclaim progress without recursive I/O, boot/switch ownership and unsafe-context rejection. |
| `lazy_file_fault_runtime_test` | Actual deferred file-fault pool, admission/worker/handler branch, materialization/PTE commit and scheduler clean-exit helper with physical-memory and File lifetime seams; checks retained task/pagemap/File ownership, failed startup/cancellation/commit cleanup and status-zero exit after publication/ref/guard retirement. |
| `xfs_log_clean_runtime_test` | Actual XFS log clean-marker, recovery and record-building code: monotonic LSN progression across clean mounts, untouched metadata verification, CRC qualification and failed/unsupported flush handling. |
| `blockbench_io_test` | Real Linux file I/O using the benchmark source: QD1/QD8 exact patterns, corrupt/short reads, strict arguments and 100 write+fsync latency samples. |
| `vfs_metadata_batch_runtime_test` | Actual batch function and frozen metadata benchmark decisions: unsupported local missing-final fallback, complete create/rename phases, retained remote routing/credentials, errors and ambiguous mutations. |
| `blockbench_mmap_io_test` | Actual functional test source: closed-FD private FILE mappings, cold read/write syscall buffers, short/interrupted I/O, exact dissimilar patterns, cleanup and error injection. |
| `block_device_source_test`, `buffer_cache_source_test`, `tmpfs_swap_source_test`, XFS/WKI/shutdown source suites | Structural guards supplement executed runtime tests; they do not qualify hardware, durability or performance. |

The gated `AhciAsync` KTEST group requires an actual native disk. Its read
batch retains eight heap-owned 512 KiB read buffers, distinguishes retained
software depth from hardware depth and requires
observed PxCI/PxSACT depth greater than one for an NCQ disk with negotiated
depth greater than one. Taskfile and timeout fixtures fail requests before
read-copy publication and check reset recovery. Queued cancellation followed
by exclusive flush checks no DMA admission for the cancelled request.
Scope cleanup cancels, waits and releases accepted requests even when a fatal
assertion returns early. Host quarantine tests intentionally retain driver
backing; they do not release live DMA merely to balance counters.

## Compilation and qualification

Linux host compilation does not establish WOS ABI compatibility. Build
`testprog` through Normal Build WOS and the current target mlibc sysroot; the
benchmark uses ordinary `pread`, `write`, `fsync`, pthread and monotonic clock
interfaces so the same binary can exercise baseline and candidate kernels.
Its host test deliberately substitutes `O_NO_CACHE=0` and therefore cannot
establish uncached WOS throughput or WOS flush durability.

`/proc/blockq` provides queue requests/bytes, reset/backpressure and latency
counters plus native AHCI receipts. AHCI `high_water` counts retained software
slots. `hardware_high_water` counts actual sampled PxCI/PxSACT active bits;
`current_ci`, `current_sact`, HBA CAP and IDENTIFY words retain the qualification
inputs. Negotiated depth alone does not prove concurrent hardware use.

The shared `scripts/cluster/node_setup.py` accepts `vm.disk_cache`; private
durability acceptance uses a flush-honoring profile such as `writeback`,
`none`, `writethrough` or `directsync`, not the default `unsafe`. Keep normal
and KTEST artifacts isolated. Authorized launches use
`bin/wos-cluster --no-setup` and `bin/wos-ktest --no-setup` without root network
setup, and must leave unrelated VMs intact.

The frozen acceptance protocol requires five alternating matched baseline and
candidate pairs: QD8 median throughput at least 115% of baseline, QD1 and
metadata throughput at least 95%, and nearest-rank p95 over the pooled 500
4 KiB write+fsync samples per role at most 110% (100 samples per run).
Keep every sample and exact source/kernel
receipts. Require exact error, durability and ownership checks, zero active
native slots after drain, and zero unexpected quarantine. Current pass/fail
evidence is recorded with each private run. The seventeenth full campaign
passed every fixed gate; all earlier failed or invalid campaigns remain
retained.

The qualified normal kernel is `2503f9dfbd4f4fd2`; the matching isolated fast
diagnostic kernel is `e2ce6c98b00578ac`. The control `ddb9db13cff3a073` retains
the old block implementation with identical prerequisite metadata, clean-log
and AHCI initialization fixes. The source default changes only after matching
recovery qualification and preserves the measured normal kernel bytes.

| Current qualification | Result |
| --- | --- |
| Actual-source host checks | All 35 selected storage, worker, reclaim, FILE, XFS, benchmark and source checks passed; independent model/fuzz and semantic mutation controls also passed. |
| Build profiles | Normal Build WOS, isolated fast diagnostic, explicit OFF and restored ON builds passed. Restored ON was byte-identical to the qualified normal kernel. |
| Isolated fast KTEST | 71,740 assertions passed, zero failed; all five native AHCI cases passed. Explicit final device retirement released 252 DMA regions and 6,348 pages, with bus mastering disabled and no quarantine. |
| One-CPU FILE qualification | 256 cold closed-FD private pages passed exact checksum and isolation checks; cold pwrite/pread used dissimilar patterns. Independent read-only verification confirmed the original 1 MiB prefix remained intact. Ordinary worker shutdown preceded final DMA retirement. |
| Abrupt XFS guest reset | Acknowledged 4 KiB fsync survived the host-requested reset. Two distinct boots, actual dirty-log recovery, exact guest and Linux read-only data checks, balanced queues and normal worker/DMA retirement passed. |
| WKI block recovery | Paused memory proved a unique retained issued 4 KiB WRITE and pending authenticated RPC before owner reset. The old operation failed, a distinct owner boot passed exact read/write/flush/verify/restore/detach, and full TX/RX partition fenced the peer and retired all bindings, proxies, queues and DMA. |
| Fixed five-pair performance campaign | All ten runs passed data, metadata, cold fsync and ownership checks. Every candidate sampled actual hardware depth eight. QD8 median throughput ratio was 1.17635, QD1 0.995996, create 1.15547 and rename 1.02397. Pooled 500-sample fsync p95 was 3,928,943 ns versus 3,727,522 ns, ratio 1.05404 within the 1.10 ceiling. |

The first matching WKI attempt timed out during its preparatory READ, before
the WRITE/reset barrier. Its failed receipt remains retained and its cause is
unresolved. A separate instrumented run used the same frozen kernel, guest
script, 100 ms RPC deadline, fault rule and reset/fencing requirements, adding
only passive observation of the private multicast traffic. That run completed
the required recovery proof. The packet capture confirms successful READ
responses in that run; it does not explain the earlier timeout. WKI retains
its depth-one authenticated RPC adapter and makes no remote throughput claim.

| Goal criterion | Evidence surface |
| --- | --- |
| 1–2: request states and exactly-once publication | Production model/runtime, generation and callback ownership checks, independent state oracle, mutation controls and bounded fuzzing. |
| 3: bounds, backpressure and execution context | Admission/fault/worker tests, fixed queue/request/byte budgets and allocation-free IRQ claims. |
| 4: ordering and durability | Overlap/flush models, failed/unsupported flush matrices, XFS log tests and abrupt-reset recovery. |
| 5: SG, partitions and device limits | Model arithmetic, actual sync bridge and AHCI DMA/MMIO integration tests. |
| 6: AHCI concurrency and reset | Actual hardware depth eight in every candidate, matching native KTEST and reset/quarantine controls. |
| 7: synchronous compatibility and progress | OFF build and actual worker, reclaim, workqueue, swap and FILE ownership/progress tests. |
| 8: consumer migration and joins | Cache/XFS/swap tests plus matching FILE and WKI reset/fence/retirement proofs. |
| 9: bounded diagnostics | Additive `/proc/blockq` queue, byte, latency, backpressure and AHCI hardware observations; optional wait counters compile out when disabled. |
| 10: fault, performance and retirement | All fixed campaign gates, matching KTEST/local/remote recovery, exact cold bytes and balanced request/DMA retirement. |

The first functional qualification used the frozen normal kernel `fb26f3f0`
and matching fast diagnostic kernel `0515e1ef`. These are historical receipts;
subsequent performance fixes require matching qualification before enablement:

| Check | Result |
| --- | --- |
| Focused final-source host CTest | 15 targets passed, including actual request/worker, AHCI, cache, swap, FILE, XFS and mmap code. |
| Normal Build WOS, isolated fast diagnostic build, OFF kernel build | Passed. |
| Isolated fast KTEST | 71,740 assertions passed, zero failed; all five native AHCI cases passed and terminal device retirement released every DMA mapping. |
| One-CPU FILE qualification | 256 cold closed-FD private pages, exact checksum and isolation, cold pwrite/pread with dissimilar patterns, successful cleanup and independent Linux read-only verification. |
| Abrupt XFS guest reset | Acknowledged 4 KiB fsync survived reset; the second boot reported a dirty log and completed recovery, exact data passed in WOS and independent Linux verification, and normal shutdown retired all DMA. |
| WKI block recovery | Paused memory inspection proved a retained issued write and pending RPC before owner reset; the old operation failed, a new authenticated owner incarnation passed exact I/O, and final fencing retired bindings, proxy queues and DMA. |
| First five-pair performance campaign | Failed; all ten runs retained. Local metadata cleanup aborted on both roles. Raw read/fsync diagnostics also showed regressions, and one candidate reached hardware depth seven. No performance acceptance or default enablement. |
| Second five-pair performance campaign | All ten runs completed with exact reads, metadata checks, cold Linux fsync verification and balanced request/DMA retirement. Performance acceptance failed: QD1, QD8 and create throughput ratios were 0.917, 0.907 and 0.846; fsync p95 was 1.389 times baseline. One candidate reached actual hardware depth six. All samples and failures remain retained. |
| Third five-pair performance campaign | All ten runs completed with the same functional and retirement checks. Performance acceptance failed: QD1, QD8 and create ratios were 0.916, 1.016 and 0.913; fsync p95 was 1.305 times baseline. Three candidates missed hardware depth eight. All samples remain retained. |
| Fourth five-pair performance campaign | All ten runs completed and every candidate reached actual hardware depth eight. QD1 improved by 20%, but QD8 improved by 4% against the required 15%, create ratio was 0.948 against 0.95, and fsync p95 was 1.254 times baseline against the 1.10 ceiling. Acceptance failed; all samples remain retained. |
| Fifth five-pair performance campaign | All ten runs completed and every candidate reached actual hardware depth eight. QD8 improved by 19%; QD1 and metadata gates passed. Fsync p95 was 1.110 times baseline against the 1.100 ceiling, so acceptance failed. All samples remain retained. |
| Sixth five-pair performance campaign | All ten runs completed and every candidate reached actual hardware depth eight. QD8 improved by 17%; QD1 and metadata gates passed. Fsync p95 was 1.152 times baseline against the 1.100 ceiling, so acceptance failed. All samples remain retained. |
| Seventh five-pair performance campaign | All ten runs completed and every candidate reached actual hardware depth eight. QD1 and metadata gates passed. QD8 improved by 12% against the required 15%, and fsync p95 was 1.175 times baseline against the 1.100 ceiling. Acceptance failed; all samples remain retained. |
| Eighth five-pair performance campaign | All ten runs completed and every candidate reached actual hardware depth eight. Metadata gates passed. QD1 ratio was 0.916 against 0.95, QD8 improved by 12% against 15%, and fsync p95 was 1.187 times baseline against 1.10. Acceptance failed; all samples remain retained. |
| Ninth five-pair campaign | All ten ran; five candidates and four controls passed correctness and retirement checks, with hardware depth eight in every candidate. The remaining control panicked during init before the benchmark because a common AHCI MMIO address calculation wrapped. The comparator rejected the full campaign. Available-sample metrics are diagnostic only; all ten receipts and the crash remain retained. |
| Tenth five-pair performance campaign | All ten runs completed; every candidate reached actual hardware depth eight, with exact reads, metadata, cold fsync checks and balanced request/DMA retirement. QD8 improved by 22% and metadata gates passed. QD1 ratio was 0.878 against 0.95, and fsync p95 was 1.113 times baseline against 1.10. Acceptance failed; all samples remain retained. |
| Eleventh five-pair performance campaign | All ten runs completed with exact data and balanced request/DMA retirement; every candidate reached hardware depth eight. QD1 improved by 56%, QD8 by 39%, and rename passed. Create ratio was 0.934 against 0.95, and fsync p95 was 1.126 times baseline against 1.10. Acceptance failed; all samples remain retained. |
| Twelfth five-pair performance campaign | All ten runs passed data, metadata, cold fsync and request/DMA retirement checks; every candidate reached hardware depth eight. QD1 improved by 73%, metadata passed, and fsync p95 was 1.069 times baseline within the 1.10 ceiling. QD8 improved by 9.5% against the required 15%, so acceptance failed; all samples remain retained. |
| Thirteenth five-pair performance campaign | All ten runs passed correctness, cold fsync and request/DMA retirement checks; every candidate reached hardware depth eight. QD1 improved by 143%, QD8 by 26%, rename passed, and fsync p95 was 1.093 times baseline within the 1.10 ceiling. Create ratio was 0.927 against 0.95, so acceptance failed; all samples remain retained. |
| Fourteenth five-pair performance campaign | All ten runs passed correctness, cold fsync and request/DMA retirement checks; every candidate reached hardware depth eight. QD1 improved by 21%, create by 18%, rename passed, and fsync p95 was 0.967 times baseline within the 1.10 ceiling. QD8 improved by 13.2% against the required 15%, so acceptance failed; all samples remain retained. |
| Fifteenth five-pair performance campaign | All ten runs passed correctness, cold fsync and request/DMA retirement checks; every candidate reached hardware depth eight. QD1 improved by 5.3%, QD8 by 20.4%, create by 25.3%, and rename passed. Fsync p95 was 1.11026 times baseline against the 1.10 ceiling, so acceptance failed; all samples remain retained. |
| Sixteenth five-pair performance campaign | All ten runs passed correctness, cold fsync and request/DMA retirement checks; every candidate reached hardware depth eight. QD1 improved by 8.8%, QD8 by 17.1%, create by 18.9%, and rename by 4.5%. Fsync p95 was 1.12205 times baseline against the 1.10 ceiling, so acceptance failed; all samples remain retained. |
| Seventeenth five-pair performance campaign | All ten runs and every fixed gate passed. Every candidate reached actual hardware depth eight. QD8 improved by 17.6%, QD1 ratio was 0.996, create improved by 15.5%, rename by 2.4%, and fsync p95 was 1.05404 times baseline within the 1.10 ceiling. Matching current KTEST, FILE, abrupt XFS and instrumented WKI recovery also passed before default enablement. |

A common AHCI initialization fix now maps the two controller pages at a
checked high-half direct-map address derived from the masked physical BAR.
It rejects invalid alignment, canonicality, and endpoint overflow before
mapping or controller publication. This removes the unchecked placement after
the relocated kernel image that caused the ninth campaign's control panic.
The new control and candidate use identical initialization bodies; existing
async service, DMA retirement, and flush paths remain unchanged. Host tests
reproduce the old address wrap and validate both fixed bodies. The tenth campaign completed with the fixed kernels, without the prior
control boot panic. That campaign's performance gates failed. The current
seventeenth campaign and matching recovery checks above completed qualification.

The second campaign used normal candidate `505e1985` and old-source control
`711d853e`. Both include the same local metadata-batch capability fallback
and clean-log cycle prerequisites. The control otherwise retains the old
block implementation. That historical candidate remains unqualified; current
source is qualified by the matching checks above.

The reset proof preserves QEMU and host writeback state. It does not establish
physical power-loss behavior or replace the separate torn-write/fault matrices.
Diagnostic AHCI tracing observed two active hardware slots; eight QEMU
dispatches alone do not satisfy the performance campaign's depth-eight gate.

## Committed baseline audit limitations

The unchanged HEAD inventory inputs expose existing global manifest
failures: `kcov_runtime_test.cpp` has an assert-based `main` and no registered
GTest target, while eight credential KTEST wrappers call shared assertion
helpers that the direct-body-only KTEST manifest rejects. Fifteen existing
Python runtime tests also lack the globally required `source` label in HEAD;
the new Goal 30 Python targets include it. Goal 30 does not
weaken either assertion policy or change those production fixtures. Report
global manifest failures alongside the independently checked new registrations
and passing storage suites.
