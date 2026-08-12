# Goal: Make xHCI Events, Hotplug, and Teardown Lifecycle-Safe

## Planning metadata

- **Expected scale:** 1–2 continuous weeks of agent time with subagents active. Completion depends on lifecycle evidence, not elapsed time.
- **Primary integration agent:** owns the controller/device state machine, IRQ-to-worker boundary, DMA ownership, and final hotplug contract.
- **Three bounded subagent workstreams:** (1) xHCI command/transfer request correlation and fake-event tests; (2) port enumeration, disconnect, endpoint/slot teardown, and failure unwind; (3) synchronized IRQ/netdevice/CDC/WKI lifetime handling and runtime stress fixtures.
- **Critical surfaces:** interrupt context, xHCI MMIO and rings, DMA memory, endpoint/slot state, PCI MSI/vector ownership, USB callbacks, netdevice/NAPI readers, and WKI bindings.
- **Completion rule:** repeated connect/disconnect under traffic must be evidenced without stale DMA or object references. An asynchronous worker alone, a successful boot probe, or time spent does not complete the goal.

## Copy-pastable goal command

```text
/goal Outcome: xHCI IRQ handling only acknowledges and publishes bounded events; task-context workers own enumeration and disconnect, commands/transfers have individually correlated completions and cancellation, and teardown drains I/O, stops/drops endpoints, disables slots, detaches CDC/netdevice/WKI users, then safely releases or recycles DMA-backed storage. | Verification surface: Normal Build WOS, focused host interrupt/netdevice/USB source tests, isolated KTEST fake-controller lifecycle cases, and user-run QEMU USB-network hotplug plus optional real-hardware evidence. | Constraints: Never allocate, block, spin for completion, or run class attach/detach in IRQ context; preserve xHCI W1C/cycle/ERDP/DCI/context rules, DMA ownership, network packet ownership, and existing boot-time enumeration behavior. | Boundaries: Implement robust xHCI plus CDC Ethernet lifecycle and the minimal synchronized IRQ/netdevice primitives it requires; do not build a general USB stack, add unrelated class drivers, redesign networking/WKI protocols, or clean up unrelated device code. | Iteration policy: Build and test the request/event engine first, move connect work out of IRQ, add complete failure unwind, then disconnect and reader-drain semantics; fault-inject every stage before enabling storage reuse. | Blocked stop condition: Stop and mark blocked only after the same indispensable QEMU or hardware runtime blocker repeats for three consecutive goal turns, fake-controller/source alternatives are exhausted, and the exact user-run hotplug command and requested logs/traces are documented. | Live execution/debugging: I am able to run and debug WOS live by appending --no-setup to the wos-ktest and wos-cluster scripts to not need root access.
```

## Local source evidence

`modules/kern/src/dev/usb/xhci.cpp::process_event` handles `TRB_PORT_STATUS_CHG` and calls `enumerate_device` directly for a SuperSpeed connect. `enumerate_device` allocates controller-owned pages and invokes `enable_slot`, `address_device`, control transfers, and endpoint configuration. Those helpers spin on completion state populated by `process_event`, so the port IRQ path can wait for work serviced by the same event path.

Commands and transfers share controller-wide `volatile` fields `XhciController::cmd_done`, `cmd_result`, and `cmd_slot_id` in `xhci.hpp`. `send_command` serializes commands with `cmd_lock`, but control and bulk transfers use the same completion fields without independent request identity. The port handler has connect/reset handling but no disconnect branch. `enumerate_device` marks `UsbDevice::active` early and has multiple allocation/descriptor/command failure exits without a complete rollback and Disable Slot sequence.

`modules/kern/src/dev/usb/cdc_ether.cpp::cdc_detach` explicitly notes that Stop/Drop Endpoint or Disable Slot is required before rings can be freed, and that netdevice lifetime synchronization is required before static CDC storage can be recycled. `modules/kern/src/net/netdevice.hpp::netdev_unregister` delegates reader-drain safety to its caller. `modules/kern/src/platform/interrupt/gates.cpp::free_irq` only clears raw handler/data pointers and does not synchronize an in-flight handler. There is no xHCI KTEST; current USB checks are primarily source-level.

## Measurable completion contract

1. xHCI hard IRQ work is bounded to status acknowledgement, event-ring draining/publication, ERDP update, and worker wakeup. It cannot enumerate, detach, allocate, block, or spin for command completion.
2. Commands and transfers use separate request records correlated by command/transfer TRB identity, with atomic completion publication, result/residual status, timeout, cancellation, and ownership rules.
3. Per-port/device states cover disconnected, resetting, enumerating, configured, disconnecting, failed, and reusable states with serialized transitions.
4. Every enumeration failure unwinds allocated contexts/rings, DCBAA publication, enabled slot, class-driver state, and vector/worker references in reverse order.
5. Disconnect prevents new I/O, cancels/drains requests, issues Stop/Drop Endpoint and Disable Slot as appropriate, detaches the class driver, drains netdevice/WKI readers, and only then frees or reconstructs storage.
6. IRQ unregistration proves no handler retains freed `private_data`; netdevice unregister/reuse proves no RX/TX/NAPI/WKI reader remains.
7. Fake-controller KTESTs cover completion correlation, event wrap/cycle bits, connect/reset, disconnect during enumeration and transfer, timeout, duplicate events, and every failure stage.
8. Repeated QEMU attach/detach/re-attach under TX/RX and WKI publication does not exhaust four static CDC slots, reuse stale state, leak tracked USB pages, or access retired objects.

## Invariants, locks, unsafe contexts, and ABI

Hard IRQ context cannot allocate, block, yield, take sleeping locks, wait for an event it must service, or invoke class/network teardown. Event publication must use atomics or an IRQ-safe lock with explicit memory ordering. Command-ring, endpoint, port-state, worker, IRQ, netdevice registry, NAPI, and WKI binding lock order must be documented. No DMA ring/context page is released while the controller can fetch it; no packet is freed twice when cancellation races completion.

Preserve xHCI cycle-bit and link-TRB rules, `PORTSC` write-one-to-clear handling, ERDP/EHB semantics, endpoint DCI mapping, DCBAA/context alignment, packed USB descriptor layouts, MSI/vector reservation, and current network ownership conventions. No userspace ABI or WKI wire-format change is intended.

## Boundaries

In scope are xHCI event/request machinery, port/device/endpoint lifecycle, CDC attach/detach, minimal synchronized `free_irq` and netdevice retention/unregister primitives, focused diagnostics, and tests. Out of scope are USB hubs beyond what current root ports require, isochronous scheduling, new USB classes, a general driver framework, network protocol changes, and unrelated PCI/interrupt cleanup.

## Parallel workstreams

Subagent 1 owns request objects, ring correlation, atomics, and fake completion tests. Subagent 2 owns port workers, enumeration rollback, disconnect commands, and endpoint/slot ownership. Subagent 3 owns IRQ synchronization, CDC/netdevice/WKI reader drain, and hotplug stress fixtures. The primary agent controls shared `XhciController`/`UsbDevice` state and integrates teardown in strict ownership order.

## Verification plan

- Run the Normal `Build WOS` task.
- Run focused host tests for interrupt registration, netdevice/table lifetime, packet ownership, WKI remotable/device publication, and new USB source contracts.
- Run isolated `bin/wos-ktest` with fake-controller event, request, timeout, rollback, and detach interleavings.
- Run QEMU `usb-net` connect/disconnect cycles under traffic and WKI publication with `bin/wos-ktest --no-setup`; use `bin/wos-cluster --no-setup` when a multi-node live run is needed. Optional real-xHCI hardware evidence remains user-run.

## Iteration policy

Make request completion independently testable, defer port work, make enumeration transactional, then enable disconnect/reuse after synchronous reader drain exists. Keep reuse disabled until every failure path has a test. Rootless QEMU evidence is collected directly with `--no-setup`; missing optional real-hardware evidence is reported without claiming it.

## Blocked stop condition

Apply the three-turn blocked threshold from the `/goal` command exactly.

## Rollback and staging

Stage request correlation, worker deferral, enumeration unwind, disconnect, and storage reuse separately. An intermediate state may quarantine DMA storage. If synchronized reuse fails, roll back only reuse while retaining IRQ deferral and request correlation; never free storage still visible to the controller or readers.

## Completion record — 2026-08-12

- **Status:** Completed.
- **Live execution note:** I am able to run and debug WOS live by appending
  `--no-setup` to the `wos-ktest` and `wos-cluster` scripts to not need root
  access.
- **Source result:** The “Local source evidence” section above is now
  historical. Hard IRQ work is bounded; request completion is correlated by
  TRB identity; port enumeration/disconnect runs in a task worker; failure and
  disconnect paths stop/drop endpoints and disable the slot before releasing
  DMA storage; CDC, netdevice, route, ARP, devfs, and WKI readers drain before
  reusable storage is reconstructed.
- **Verification result:** Normal `Build WOS` and diagnostic
  `bin/wos-ktest --build-only` passed. The complete rootless diagnostic suite
  passed 6,032 tests with zero failures, including request/event correlation,
  wrap/cycle, rollback, disconnect/cancellation, CDC reuse, IRQ drain,
  netdevice generations, ARP retirement, and ARP transmit-at-preemption-depth
  zero. Focused xHCI, net-table lifetime, UDP binding, and cluster fixture host
  tests passed; formatting and `git diff --check` passed.
- **Live result:** A rootless KTEST VM selected QEMU's CDC configuration,
  published `eth2` as a remotable netdevice, and completed two bound DNS
  request/response exchanges over the USB NIC. The final tree completed nine
  configure/detach lifecycles in one boot; after every final detach `eth2` was
  absent and the packet pool returned exactly to its 1,536-buffer free
  baseline. Earlier iteration passes added 35 normal-dwell, 16 ten-millisecond,
  and 16 zero-dwell cycles. Final serial/QEMU logs contained no panic,
  sanitizer failure, preemption-disabled block, xHCI timeout, or quarantine.
- **Remaining optional evidence:** No physical xHCI controller was exercised;
  real-hardware evidence remains optional under this goal's verification
  surface.
- **Residual observation:** Interface bring-up accounted one transient CDC TX
  drop while later USB transfers and both DNS exchanges succeeded. The driver
  intentionally refuses a second simultaneous transfer on one endpoint rather
  than blocking in an unsafe context; sustained-throughput queueing remains
  outside this lifecycle goal.
