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
| 25  | [Secure session-bound WKI RDMA](25-secure-wki-rdma.md)                               | WKI auth, RoCE, ivshmem, zones, fast paths                   | 20 on ivshmem; 26 on remote VFS               | Complete    |
| 26  | [Owner-authoritative remote VFS coherency](26-remote-vfs-coherency-locking.md)        | remote VFS, caches, append, advisory locks                   | 18 and 25 on shared VFS/wire surfaces         | Complete |
| 28  | [Durable reconnectable remote jobs](28-durable-remote-jobs.md)                        | remote compute, streams, reconnect, result retention         | 19 identity; 22 quotas; compute rewrites      | In progress |
| 29  | [Production TCP congestion and loss recovery](29-tcp-congestion-loss-recovery.md)    | TCP sender, ACK/SACK, timers, impairment tests               | 31 on shared timer paths; broad TCP rewrites  | Complete    |
| 30  | [Asynchronous multi-queue block I/O](30-asynchronous-block-io.md)                    | block API, AHCI, buffer cache, XFS, swap, proxy              | 20 on DMA/AHCI; 25 on remote block            | Complete    |
| 33 | [Native WOS hardware virtualization](33-native-hypervisor.md) | Intel VMX/EPT, AMD SVM/NPT, common VM ABI | 36, 38, 40 on VM/MM/Task state | Not started |
| 34 | [Linux guest virtual machine monitor](34-linux-guest-virtual-machine-monitor.md) | libwosvirt, per-VM VMM, vmmd/vmctl, Linux boot/devices | 33 until VM ABI stabilizes; 35 on VMM control | Not started |
| 35 | [WKI distributed virtualization](35-wki-distributed-virtualization.md) | VM identity/control, executor placement, remote devices | 25, 28, 36–38 on WKI and service state | Not started |
| 36 | [VM checkpoints and live migration](36-wki-vm-checkpoint-migration.md) | dirty tracking, device quiescence, state transfer, ownership | 33, 35, 38, 40 on VM/MM/wire state | Not started |
| 37 | [Replicated VM volumes](37-wki-replicated-vm-volumes.md) | durable quorum log, block leases, snapshots, recovery | 20, 25, 30 on block transport; 35–38 on WKI | Not started |
| 38 | [Disaggregated guest memory](38-wki-disaggregated-guest-memory.md) | remote RAM providers, EPT/NPT cache, page ownership | 25 on transport; 33, 36, 40 on MM/EPT/NPT | Not started |
| 39 | [Isolated service compartments](39-isolated-service-compartments.md) | mount/PID/IPC namespaces, lifecycle, authority, launcher | 19 on credentials; 26 on VFS policy | In progress |
| 40 | [NUMA placement and safe large pages](40-numa-memory-locality.md) | SRAT/SLIT, CPU domains, page policy, compound lifetime | 33, 36, 38 on VM/MM/Task state | Not started |
| 41 | [Physical GPU platform](41-physical-gpu-platform.md) | PCI resources/reset, firmware, device mmap, AMD-Vi, IRQ remap | 20 on DMA/IOMMU; 40 on VM/page lifetime | Not started |
| 42 | [DRM-compatible GPU kernel core](42-drm-gpu-kernel-core.md) | DRM uAPI, GEM/GPUVM, dma-buf, syncobj, scheduler, KMS | 21, 22, 41 on MM/security/device state | Not started |
| 43 | [Mesa and windowed graphics platform](43-mesa-graphics-platform.md) | libdrm, Mesa, Vulkan/EGL/GL, LLVM/Rust, Wayland/WSI | 11, 23, 42; vendor userland integration | Not started |
| 44 | [AMD amdgpu/RADV/RadeonSI graphics](44-amd-amdgpu-mesa-vulkan.md) | AMD firmware/IP, VRAM/GPUVM/rings, display, Vulkan/GL | 41–43; 45 on shared amdgpu state | Not started |
| 45 | [AMD ROCm HSA/HIP compute](45-amd-rocm-hsa-hip.md) | amdkfd, HSA queues/memory, ROCr, HIP/LLVM, rocBLAS | 41, 44 on IOMMU/MM/amdgpu/reset | Not started |
| 46 | [NVIDIA Nouveau/NVK/Zink graphics](46-nvidia-nouveau-nvk.md) | Nouveau/NVKM, GSP, VRAM/GPUVM, NVK/NAK/Zink, display | 41–43; 47 on exclusive GPU ownership | Not started |
| 47 | [NVIDIA open KMD and CUDA runtime](47-nvidia-cuda-driver-stack.md) | NVIDIA RM/UVM OS layer, proprietary libcuda ABI, CUDA | 41; 46 on exclusive GPU ownership | Not started |

Goal 25 completes the authenticated opt-in implementation and its documented
software-QEMU, production-source model and isolated-KTEST security scope. The
default service mask remains zero. Combined Ethernet failures, multicore
availability observations and physical hardware qualification still constrain
production rollout; see [the qualification record](../docs/wki_secure_rdma.md).

## New platform goals: 33–40

Goal 28 is paused and incomplete at its tested wire-codec checkpoint
`70bc3fe7`. At the user's request, current work selects Goal 39. Its native
isolation, launcher and runtime criteria remain outstanding; the local progress
ledger records source reconnaissance and qualification. The user cleared the previous goal-manager record on 2026-10-08,
and Goal 39 is now the active Goal Mode objective. Private native IPC and mount
ownership, Task native lookup/consumption and subject mount/unmount/repeatable
pivot, retained File listings and subject procfs mount observations are qualified
at the kernel API checkpoint. Native PID owner/local-name inheritance, exact
requester registry lookup and GC pin withdrawal now pass actual kernel tests.
Durable local SID/PGID inheritance, scoped session/group adapters and exact PTY
foreground names also pass native production API tests, including destroyed
leaders and sibling aliases (78,761 assertions, zero failures, 1,284 cases).
Canonical self PID/TID/PPID, local creation returns and initial/replacement/
pthread/COW TCB adapters now pass native production API tests, including explicit
shared-clone physical-alias refusal (78,906 assertions, zero failures,1,289 cases).
Public wait now preserves local and event-time group names, exact scope, public
wakes, claims/reaping and Host supervision (79,109 assertions, zero failures,
1,295 cases; all 43 compartment cases). Private signal PID/TID/group selectors,
exact admission and closure/route interleavings now pass native production tests
(79,602 assertions, zero failures, 1,302 cases; all 50 compartment cases).
Disposable test images were audited and pruned; Goal39 remains active.
Public compartment admission, remaining procfs/positive PID/ptrace, namespace-
init lifecycle, complete WKI/network enforcement and full service/runtime
criteria remain incomplete. Goal39 remains active with all ten criteria required.

The [local virtualization manual corpus](../docs/virtualization-manuals.md)
provides pinned source downloads, Docling PDF text and offline search. Check its
manifest for missing sources before treating a reference as available.

Goals 33–40 were investigated on 2026-09-08. Read the
[reconnaissance, source map, dependencies and stale-guidance findings](RECONNAISSANCE-33-40.md)
for their architectural basis. Each new contract includes its own copyable
`/goal` prompt, acceptance criteria, scope limits, checkpoints, subagent roles,
validation requirements and blocked stop rule.

The distributed hypervisor program is **33 → 34 → 35**, followed by migration
(36), replicated disk integration (37), and remote guest RAM (38). Storage work
for 37 can start independently; its final guest acceptance needs 35. Goal 39
adds native service compartments; goal 40 adds NUMA placement and safe large
pages. Dependencies are explicit in the individual files; these are separate
large outcomes, not a single unbounded “improve WOS” run.

**Both Intel VMX/EPT and AMD SVM/NPT are mandatory support targets**, using one
common userspace API and machine model. Read the
[virtualization architecture and userspace contract](VIRTUALIZATION-ARCHITECTURE.md)
for `libwosvirt`, per-VM `wos-vmm`, `vmmd`, `vmctl`, guest boot, run/exit handling,
virtual devices and the WKI bridge. Vendor hardware validation is required for
both; implementing one backend does not complete the goals. Same-vendor live
migration is required for each backend; cross-vendor live migration remains
explicitly rejected until separately implemented and validated.

The platform includes
remote execution and devices, migration, storage resilience and RAM pooling;
splitting one guest's vCPUs concurrently across physical hosts requires a
further memory-consistency design and is not promised by these contracts.
Each goal targets approximately 168–336 hours of continuous primary-agent work
with bounded subagents, without treating time spent as proof of completion.

## Physical GPU program: 41–47

Goals 41–47 were investigated on 2026-09-23. Read the shared
[physical GPU architecture and evaluation](GPU-ARCHITECTURE.md) before selecting
one. These are multi-quarter program contracts, not ordinary package ports; their
individual effort estimates supersede the portfolio's usual one-to-two-week
planning size while retaining the same evidence-based completion rule.

The supported device is a complete physical PCI function owned by WOS, either on
bare metal or assigned whole to a WOS guest by an external Linux KVM/VFIO host.
No virtio-gpu, virgl, Venus, mediated device, SR-IOV VF or host API forwarding can
satisfy physical-driver acceptance. WOS consuming a passed-through GPU does not
depend on goals 33–34. Conversely, making WOS pass a GPU onward to its own guests
is a future device-assignment goal and remains outside both programs.

The common path is **41 → 42 → 43**. AMD graphics is goal 44 and ROCm/KFD/HIP is
goal 45. NVIDIA open graphics is goal 46. Genuine CUDA is goal 47 and uses
NVIDIA's open RM/UVM kernel source plus exactly matching, lawfully obtained
proprietary `libcuda` and firmware; Mesa/NVK does not implement CUDA. Goals 46
and 47 are alternative owners of one NVIDIA GPU and must not bind it concurrently.

Every vendor claim names exact PCI IDs, firmware/VBIOS hashes, driver revisions,
IOMMU/BAR configuration and execution environment. A Mesa build, software
renderer, mocked ioctl, emulated GPU or single triangle is only a milestone.
Physical runtime/debug evidence is mandatory and remains user-run under the
current repository instructions.

## Status authority

The portfolio lists unfinished goals only. Its `Status` column is the
canonical selection filter. Completed and superseded contracts were removed
on 2026-10-05; their historical summaries below are not active objectives.
Select a row marked `Not started` or `In progress` and verify its premises
against the current source before starting work.

## How to use a goal

1. Select an unfinished portfolio row, then re-read the current local source
   and path-specific instructions. Goals 17–32 were investigated on 2026-09-03;
   the later platform programs have their own reconnaissance dates. Planning
   evidence may become stale in this fast-moving repository.
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
7. Follow the user's current runtime authorization. In this session the user
   explicitly permits launching owned WOS instances with `bin/wos-cluster
   --no-setup` and isolated selftests with `bin/wos-ktest --no-setup`. Preserve
   unrelated VMs, use the shared launch guard, and retain KTEST isolation.
   Hardware scenarios outside that authorization still require user-run
   fixtures and supplied logs.

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

## Retired work

Goals 01–24, 27, 31 and 32 were marked Done in the previous portfolio.
The WOSDBG UI revision and event-driven SSH broker work also have completed
records. Their contracts and chronological ledgers were removed during cleanup.
Durable subsystem documentation, regression tests and referenced security audits
remain in the repository; the audits now live under `docs/security/audits/`.

The broad stability continuation and old goals 48–50 were replaced at the
user's request by a focused launch-overhead objective. That replacement is
complete. Serial FIFO batching retained synchronous journal completion and
reduced the rejection-log median from 1.250 to 1.060 ms in the final private
comparison. The balanced placement median of eight group medians fell from
3.900 to 3.763 ms. Direct, locally and homeward timings increased slightly;
no general speedup or matched Linux ratio is claimed. Positive trusted compute,
physical hardware and broader topology qualification remain outside that result.

See [runtime-linker startup milestones](../docs/rtld_startup.md),
[wait accounting](../docs/kernel_wait_accounting.md),
[WKI retry ownership](../docs/wki_bootstrap_replay.md) and
[perf capture](../docs/perf_capture.md) for the retained implementation contracts.
The compact historical completion receipt remains local at
`build/goal48-launch-performance-result-v87/goal-result-proof.json`.
Superseded handoffs, experiment builds and private VM archives were pruned on
2026-10-05. The cleanup inventory is `build/worktree-cleanup-20261005.json`.
The retained receipt summarizes prior validation and is not a current-source
audit.

This directory is ignored by default, although some existing files are tracked.
Untracked contracts remain local planning material.
