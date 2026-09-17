# Goal 35: WKI Distributed VM Control and Remote Devices

## Copy-pastable goal command

```text
/goal Make a WOS cluster one distributed virtualization platform: create, query, run, stop and console-attach to VMs on another WOS node, with Intel and AMD executors, VM-scoped remote block/network resources and authenticated idempotent lifecycle control over WKI through vmctl, per-node vmmd and a typed kernel/userspace service bridge. Complete every acceptance criterion in goals/35-wki-distributed-virtualization.md, including multi-node fault traces, resource accounting, mixed-capability rejection, Normal Build WOS and executable protocol tests. Preserve single-executor ownership, existing WKI authentication, bounded nonblocking RX and current service compatibility. Use only the listed VMM/WKI/control surfaces, integrate bounded subagents by checkpoint, and follow the evidence and blocked rules in the file.
```

## Outcome and scale

From WOS node A, manage a Linux VM executing on node B whose virtual disk or
network backend can reside on node C. WOS nodes provide execution and resources
through WKI, with a single coherent VM identity and control API.
**Target: one to two continuous weeks of integration work with bounded subagents.**

**Hard prerequisites:** the accepted native backend and Linux VMM from goals
33–34. Secure RDMA goal 25, durable jobs goal 28, and replicated storage goal 37
are not prerequisites: authenticated message transport and explicit owner-loss
semantics must suffice. Reuse any completed compatible infrastructure without
silently coupling VM lifetime to one-shot remote process submission.

## Baseline and impact map

Read `modules/kern/src/net/wki/{wki.hpp,wire.hpp,wki.cpp}`,
`auth.hpp`, `auth_policy.hpp`, `auth_policy.cpp`,
`remote_compute.hpp`, `remote_compute.cpp`, `peer.cpp`,
`resource_contract.hpp`, `remotable.hpp`, `dev_server.hpp`,
`dev_proxy.hpp`, `remote_net.hpp`, and `zone.hpp`.
Read their relevant implementations, `modules/kern/src/dev/block_device.hpp`,
`platform/sched/resource.hpp`, and the VMM/management modules before editing.

The 2026-09-08 source has authenticated WKI v3, resource incarnations,
channel-generation lifetimes, remote device exports, process submission, and
bounded deferred workers. `wki_init()` does not enable raw RoCE: it explicitly
identifies its authentication bypass. None of these supplies a VM protocol.

Flow: local controller → authenticated VM operation → fixed RX admission →
task-context VM worker → capability/permission/quota checks → executor VM →
remote resource attachment → device completions → console/status streams.
CPU/RAM remain on B in this goal; C owns resource backends. B may use Intel
VMX/EPT or AMD SVM/NPT independently of A/C's vendor. Implement the distributed
portion of the [userspace architecture contract](VIRTUALIZATION-ARCHITECTURE.md).

## Userspace service and resource path

`vmctl` sends a versioned operation to its local `vmmd`. The local manager uses
a narrow WOS service endpoint to send an authenticated VM request over WKI.
On B, kernel RX validates and admits a bounded envelope; a task-context bridge
queues it for B's `vmmd`, which checks VM ownership and executor capacity before
starting or controlling the corresponding `wos-vmm` process. Kernel WKI code
must not become the userspace machine emulator or depend on a raw-WKI socket
that the repository does not provide.

Implement pollable request/completion queues with authenticated peer identity,
VM owner scope, operation ID, channel/session generation and opaque reply token.
The token authorizes one admitted request; arbitrary caller-supplied source IDs
or permission masks are rejected. Queue-full, daemon restart, canceled operation,
stale reply and session rotation have explicit transport and application states.
Transport ACK is not VM acceptance or an operation commit.

Guest disk/network workers use scoped backing adapters. Authorized WKI block
proxies expose positional I/O/flush semantics; remote network adapters bind the
new guest Ethernet endpoint to an allowed remote resource. Reuse attachment
incarnations and retirement rather than exposing kernel pointers or unbounded
zone mappings to userspace. Guest instructions and ordinary exits execute on B;
WKI carries control, console and selected resource I/O rather than every VM exit.

Keep the node daemon supervised separately from each VMM. Losing the CLI or
control session does not terminate the running process; daemon restart reconciles
kernel/VMM VM identity and request state before accepting mutations. Remote
launch is its own VM protocol, not a disguised one-shot process submission.

## Checkpoints and parallel ownership

1. Specify VM identity, owner/executor incarnation, operation IDs, authorization,
   protocol negotiation, state transitions and bounded request/result retention.
   Primary owns wire/header changes; a subagent implements the state model.
2. A VMM subagent implements executor lifecycle; a device subagent implements
   remote backend adapters; the primary integrates authenticated admission.
3. Rotate a subagent to CLI/console/status and another to chaos fixtures;
   integrate reconnect, duplicate handling, cancellation and cleanup.
4. Execute user-run three-node scenarios and independent lock/lifetime review.

## Acceptance criteria

1. Node A can list capable executors and create/start/query/stop/destroy a VM on
   B with console attach/reattach. Status reports VM ID, authenticated owner,
   executor incarnation, resource assignments and terminal/failure reason.
2. Capability admission checks CPU model, backend ABI/machine version, RAM,
   vCPU quota and requested device permissions before running a guest.
   Mixed-capability peers reject explicitly without changing existing services.
3. Three-node evidence shows guest disk I/O routed B→C and guest network I/O
   through a separately authorized backend. Guests cannot claim arbitrary host
   devices, WKI transport NICs, or another VM's resource attachments.
4. Duplicate/lost/reordered create, start, stop, destroy and response messages
   produce one accepted mutation per operation ID. Retention expiry yields an
   explicit unknown result rather than silently replaying a non-idempotent action.
5. Controller disconnect does not implicitly kill or duplicate an accepted VM;
   authenticated reattach recovers status. Executor reboot yields a new
   incarnation and a visible lost/stopped state, never an unproven automatic restart.
6. Device-peer fencing resolves every in-flight operation with a defined
   completion/error policy and bounded cleanup. Guest-visible reset/error
   semantics are documented; no indefinite uninterruptible vCPU/worker waits.
7. Console credits and resource quotas bound memory per VM, peer, and controller;
   one noisy VM cannot consume all WKI control admission. Prove control progress
   during saturated console and block traffic.
8. Run 100 lifecycle cycles plus every protocol transition fault point and a
   2-hour three-node guest I/O workload. VM/FD/channel/binding/page counts return
   to baseline; unrelated remote VFS and compute regression scenarios still pass.

9. The userspace service bridge and installed CLI/manager/VMM path complete
   a remote workflow end to end. Test queue exhaustion, forged origin, permission
   escalation, reply-token reuse, daemon death and stale-session completion.
10. Repeat lifecycle and guest I/O acceptance with Intel and AMD executors, and
    include mixed-vendor controller/resource-node scenarios. Report executor
    backend/feature requirements in discovery and failure diagnostics; controller
    vendor does not determine admissible guest execution.

## Constraints, compatibility, and rollback

WKI peer/channel/proxy locks protect only bounded admission and identity checks;
VM creation, VFS access, ACK waits and teardown occur in task context.
Use explicit negotiated VM resource/operation definitions with layout assertions
and authorization updates on every receiver. Do not reuse an unknown permission
as a privileged default. Authentication does not imply encryption: document the
existing trusted-cluster confidentiality boundary.

No VM migration, distributed coherent CPU cache, automatic failover, new generic
orchestrator/database, or insecure RDMA enablement. Gate the new service;
rollback rejects new VM requests, drains exact VM incarnations, and leaves
ordinary WKI resource services operational.

## Execution, evidence, and stop rules

Read `AGENTS.md`, the subsystem index, applicable kernel/WKI/critical-path/build
instructions, and the source entrypoints below before changing code. Source
wins over this dated plan. Make an impact map and the full repository patch
contract, including locks, unsafe contexts, ownership, ABI, validation, and
rollback, before each materially different critical patch.

Use a primary integration agent and up to three active subagents with bounded,
non-overlapping assignments; rotate assignments between checkpoints. The primary
owns shared headers, integration, and acceptance. Keep
`goals/progress/35/ledger.md` and `goals/progress/35/evidence.md`:
checkpoint, hypothesis, change, command, result,
artifact/build ID, remaining failure, next action. Preserve existing user edits.

Run Normal `Build WOS`; if the task is unavailable, mirror the current
`.vscode/tasks.json` command/environment. Register executable host tests with
the existing CMake test infrastructure and run their generated CTest entries.
Use `scripts/dev/format_repo.sh --check <changed-code-paths>`.
Prepare isolated diagnostics with `bin/wos-ktest --build-only`, retaining
`build-ktest/` and `ktest-data/` isolation. Read
`docs/kernel_debug_flags.md` before changing diagnostic flags. Never combine
report-mode KUBSan with the UBSan trap profile.

Kernel/cluster runtime execution and debugging are user-run under the current
instructions. Supply exact fixture-specific commands, expected observations,
and log locations early; analyze returned evidence while continuing independent
implementation and host validation. Runtime requirements in this file are mandatory,
not replaced by model tests, source-string tests, a successful build, or a demo
script that has not run.

Finish only when **all** acceptance criteria in this file have supporting evidence.
The one-to-two-week horizon is a scope estimate, not an enforced delay, token
budget, autonomy guarantee, or alternative finish line. Do not pad work or
silently shrink the contract. If a premise changes, record the discrepancy and
reconcile the contract with the actual tree before dependent edits. If an
indispensable blocker persists for three consecutive goal turns and no useful
authorized work remains, report the attempts, evidence, exact missing input,
and restart step, and mark the active goal blocked. Missing prerequisites or
hardware are never completion. Budget and pause controls remain user/system
controlled.
