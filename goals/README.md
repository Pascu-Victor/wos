# WOS long-running goal portfolio

These files are candidate contracts for improving WOS. Each one is deliberately
large enough to occupy a primary integration agent and several bounded
subagents for roughly one to two continuous weeks. That horizon is a planning
signal, not a completion condition: a goal is complete only when its stated
evidence exists and all required checks pass.

The contracts follow OpenAI's [six-part Goal pattern](https://developers.openai.com/cookbook/examples/codex/using_goals_in_codex):

1. outcome;
2. verification surface;
3. constraints;
4. boundaries;
5. iteration policy; and
6. blocked stop condition.

Every file contains a copy-pastable `/goal` command and the expanded execution
contract behind it. These files do not activate Goal Mode by themselves.
OpenAI's shorter [Follow a goal guide](https://learn.chatgpt.com/use-cases/follow-goals)
describes the same core contract: one objective and stopping condition, inputs
to read first, concrete evidence commands/artifacts, checkpointed progress, and
an explicit pause/block rule.

## Portfolio

| ID  | Goal                                                                                 | Primary surfaces                                            | Avoid concurrent ownership with               | Status      |
| --- | ------------------------------------------------------------------------------------ | ----------------------------------------------------------- | --------------------------------------------- | ----------- |
| 01  | [Fault-safe syscall user access](01-syscall-usercopy-safety.md)                      | syscall ABI, usercopy, VFS, sockets                         | 10, 11 when changing the same ABI wrappers    | Done        |
| 02  | [Unified memory-pressure coordinator](02-unified-memory-reclaim.md)                  | allocator, caches, swap, reclaim                            | other allocator/reclaim rewrites              | Done        |
| 03  | [Event-driven child lifecycle](03-process-child-events.md)                           | waitpid, exit, ptrace, scheduler                            | 04 or 16 in shared Task/deferred-switch code  | Done        |
| 04  | [Executable scheduler transition model](04-scheduler-transition-model.md)            | EEVDF transitions, migration, handoff                       | 03, 16, and other scheduler restructures      | Done        |
| 05  | [Asynchronous xHCI hotplug lifecycle](05-xhci-hotplug-lifecycle.md)                  | IRQ, USB DMA, CDC netdevice teardown                        | other USB/netdevice lifetime rewrites         | Done        |
| 06  | [Production IPv6 dual stack](06-ipv6-dual-stack.md)                                  | TCP/UDP, NDP, routing, netd                                 | broad socket or network-stack rewrites        | Done        |
| 07  | [Mount-scoped crash-consistent XFS journal](07-xfs-crash-consistency.md)             | XFS log, buffer cache, block I/O                            | 10 when changing XFS transactions             | Done        |
| 08  | [Deterministic WKI chaos and recovery](08-wki-chaos-recovery.md)                     | WKI transport, peers, every remote service                  | 09 and broad WKI lifecycle changes            | Done        |
| 09  | [Authenticated WKI peer sessions](09-wki-authenticated-sessions.md)                  | WKI wire protocol, admission, authorization                 | 08 until transport hooks are coordinated      | Done        |
| 10  | [End-to-end extended attributes](10-end-to-end-xattrs.md)                            | libc, syscall/VFS, XFS, remote VFS                          | 01, 07, 11 on shared surfaces                 | Done        |
| 11  | [libc and syscall-ABI conformance gate](11-libc-syscall-conformance.md)              | mlibc, syscall ABI, target test runner                      | 01, 10, or 13 on shared wrappers/ABI mirrors  | Done        |
| 12  | [Reproducible WOSDBG incident bundles](12-wosdbg-incident-bundles.md)                | KTEST/cluster capture, WOSDBG                               | 13 on shared ingestion schemas                | Done        |
| 13  | [Versioned structured telemetry](13-structured-telemetry.md)                         | journal, perf, strace, WOSDBG                               | 11 or 12 on shared ABI/ingestion formats      | Done        |
| 14  | [Declarative bounded init supervisor](14-declarative-init-supervisor.md)             | PID 1, service lifecycle, shutdown                          | other init/service ownership rewrites         | Done        |
| 15  | [Randomized and hardened userspace layouts](15-userspace-address-space-hardening.md) | ELF/exec, VM, TLS, ptrace/debugging                         | 01, 04, 11 on loader/VM/ABI surfaces          | Done        |
| 16  | [PREEMPT_NOBLOCK default kernel preemption](16-preempt-noblock-kernel-preemption.md) | timer preemption, kernel frames, migration, return assembly | 03 or 04 on shared scheduler/Task transitions | Done        |
| 17  | [Fault-safe anonymous memory swap](17-anonymous-memory-swap.md)                      | VM faults, PTEs, COW, reclaim, swap                         | 15 and allocator/VM rewrites                  | Done        |
| 18  | [Stable transactional VFS pathwalk](18-stable-vfs-pathwalk.md)                      | pathwalk, mounts, namespace mutation, remote VFS             | 26 and broad VFS/remote-VFS changes           | Done        |
| 19  | [Immutable credentials and least privilege](19-immutable-credentials-least-privilege.md) | credentials, authorization, init, WKI identity            | 22 on Task/ServiceSpec; 18 on VFS policy      | Not started |
| 20  | [IOMMU-backed DMA ownership](20-iommu-dma-ownership.md)                              | ACPI/PCI, DMA, AHCI, xHCI, NICs                              | 25 on ivshmem; 30 on AHCI/DMA                 | Not started |
| 21  | [Modern kernel self-protection](21-kernel-self-protection.md)                        | linker/boot, W^X, KASLR, SMEP/SMAP, stacks                  | 16 and entry/return or VM rewrites            | Not started |
| 22  | [Kernel-enforced resource governance](22-kernel-resource-governance.md)              | rlimits, Task, VM/VFS, init, remote compute                  | 19 on Task/ServiceSpec; 28 on job quotas      | Not started |
| 23  | [Reproducible offline builds](23-reproducible-offline-builds.md)                      | toolchain, CMake, sysroots, images, SBOM                     | broad build/image/cluster script changes      | Done        |
| 24  | [Semantic regression and mutation testing](24-semantic-regression-platform.md)       | host tests, models, KTEST, faults, fuzzing                   | broad structural refactors in tested paths    | Not started |
| 25  | [Secure session-bound WKI RDMA](25-secure-wki-rdma.md)                               | WKI auth, RoCE, ivshmem, zones, fast paths                   | 20 on ivshmem; 26 on remote VFS               | Not started |
| 26  | [Owner-authoritative remote VFS coherency](26-remote-vfs-coherency-locking.md)        | remote VFS, caches, append, advisory locks                   | 18 and 25 on shared VFS/wire surfaces         | Not started |
| 27  | [Loop-free bounded WKI routing](27-wki-routing-convergence.md)                        | WKI LSA/LSDB, RX, SPF, route publication                     | other peer/routing/wire protocol rewrites     | Not started |
| 28  | [Durable reconnectable remote jobs](28-durable-remote-jobs.md)                        | remote compute, streams, reconnect, result retention         | 19 identity; 22 quotas; compute rewrites      | Not started |
| 29  | [Production TCP congestion and loss recovery](29-tcp-congestion-loss-recovery.md)    | TCP sender, ACK/SACK, timers, impairment tests               | 31 on shared timer paths; broad TCP rewrites  | Not started |
| 30  | [Asynchronous multi-queue block I/O](30-asynchronous-block-io.md)                    | block API, AHCI, buffer cache, XFS, swap, proxy              | 20 on DMA/AHCI; 25 on remote block            | Not started |
| 31  | [High-resolution timekeeping and timers](31-high-resolution-timekeeping.md)           | TSC/RTC/NTP, timer queues, signals, libc                     | 16 scheduler timers; 29 TCP timers            | Not started |
| 32  | [Live multi-node WOSDBG control plane](32-live-wosdbg-control-plane.md)               | WOSDBG, QMP/gdb, debugserver, incident capture               | 21 symbolization; debugserver rewrites        | Done        |

## Status authority

The portfolio `Status` column is the canonical goal-selection filter. Do not
select, reload, or rescan a row marked `Done`; its completion record and current
source remain available only for later regression work. Completed goals are
currently 01 through 18 and 32. Select only a row whose status is `Not started`
(or a future explicit in-progress state). Goals 19 through 31 are the unfinished
portfolio produced from local-source reconnaissance on 2026-09-03.

## How to use a goal

1. Skip every portfolio row marked `Done`, then re-read the current local source
   and path-specific instructions for the selected unfinished goal. The evidence
   for goals 01–16 was collected beginning on 2026-08-08; goals 17–32 were
   investigated on 2026-09-03. Any of it may become stale in this fast-moving
   repository.
2. Use a dedicated thread and preferably a dedicated worktree for one goal.
3. Paste that file's `/goal` block. Do not turn the week estimate into a token or
   time budget; completion remains evidence-based.
4. Give subagents bounded reconnaissance, test, or non-overlapping implementation
   workstreams. The primary agent owns the impact map, critical-path patch
   contract, integration, and final verification.
5. Before editing critical code, write the repository-required patch contract.
   Re-run it whenever the design or file set materially changes.
6. Keep an iteration ledger: hypothesis, patch, focused evidence, regressions,
   next action. Never broaden into unrelated cleanup merely to keep working.
7. WOS runtime debugging can be run rootlessly by appending `--no-setup` to
   `wos-ktest` or `wos-cluster`. Physical-hardware checks remain user-run.

## Portfolio rules

- Goals are individually valid; the numeric order is not a mandatory roadmap.
- Overlap warnings identify ownership conflicts, not hard dependencies. If two
  goals touch the same critical path, serialize them or explicitly merge their
  patch contracts.
- Existing ABI and wire formats stay compatible unless a goal explicitly
  defines version negotiation, migration, and negative compatibility tests.
- A partial implementation, elapsed time, exhausted context, or passing build is
  never sufficient on its own. The completion contract in the selected file is
  authoritative.
- If the local tree invalidates a premise, update the contract before acting.
  Do not implement a stale plan because it is written here.

## Reconnaissance note

The current `scripts/test/ktest_setup.py` defaults to
`configs/node_ktest.json`, while some repository instructions still name
`configs/node.json`. Goal executors must follow the local script and reconcile
that documentation discrepancy when their work touches KTEST configuration.

This directory is intentionally ignored by the root `.gitignore`; it is local
planning material unless a later task explicitly chooses to publish it.
