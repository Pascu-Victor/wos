---
applyTo: "modules/kern/src/net/wki/**"
---

# WKI Research Cache

Generated from public `Pascu-Victor/wos` `main` plus user-supplied local instructions. Local unpublished work may differ. Verify before editing.

Reviewed against local WKI source: 2026-06-01.

## Source of truth files

- `wki.hpp`: constants, core types, global state, public API declarations.
- `wire.hpp`: protocol constants, message types, channel IDs, packed wire payloads.
- `wki.cpp`: global state definition, time/yield helpers, CRC, init/shutdown, transport registry, peer/channel management, send paths, timer/RX integration.
- `peer.*`: peer handshake, heartbeat, fencing, peer state transitions.
- `channel.*`: channel reliability, ACK/retransmit/reorder behavior.
- `routing.*`: route/LSA behavior.
- `zone.*`: shared-memory/RDMA zone behavior.
- `remote_vfs.*`: VFS export, remote mount/proxy file operations, remote VFS DEV_OP handlers.
- `remote_compute.*`: scheduler remote placement, task submit/wait/cancel, load reports, remote execution.
- `remote_ipc.*` and `remote_ipc_socket.cpp`: cross-node IPC/file-descriptor proxying for pipes, sockets, PTYs, futex/eventfd/epoll style resources, plus IPC perf snapshots.
- `dev_proxy.*` / `dev_server.*`: consumer/owner sides of remote devices.
- `remote_net.*`: remote NIC behavior.
- `transport_*.*`: concrete transports.

## Core state summary

`WkiState` contains:

- local node identity and MAC.
- initialized flag.
- fixed peer table and peer count.
- peer hash index.
- transport linked list and transport count.
- capability fields.
- routing/LSA sequence state.

`WkiPeer` contains:

- node identity, MAC, transport pointers, state, heartbeat info.
- routing info such as next hop and hop count.
- credits/capabilities.
- per-peer channel pointer array.
- per-peer lock.

`WkiChannel` contains:

- peer/channel identity.
- priority and active flag.
- tx/rx seq/ack state.
- credit state.
- retransmit queue.
- reorder buffer.
- duplicate ACK tracking.
- statistics.
- inline retransmit storage.
- channel lock.

## Public API map from `wki.hpp`

Core:

- `wki_init()`
- `wki_shutdown()`

Transport:

- `wki_transport_register()`
- `wki_transport_unregister()`

Peer management:

- `wki_peer_find()`
- `wki_peer_list_by_zone()`
- `wki_peer_alloc()`
- `wki_peer_count()`

Sending:

- `wki_send()`
- `wki_send_raw()`

RX dispatch:

- `wki_rx()`

Channel management:

- `wki_channel_get()`
- `wki_channel_alloc()`
- `wki_channel_close()`
- `wki_channels_close_for_peer()`

Timer/time/progress:

- `wki_timer_tick()`
- `wki_now_us()`
- `wki_spin_yield()`
- `wki_spin_yield_channel()`

CRC:

- `wki_crc32()`
- `wki_crc32_continue()`

Internal RX handlers include HELLO, HELLO_ACK, HEARTBEAT, HEARTBEAT_ACK, LSA, and FENCE_NOTIFY handlers.

Remote IPC public entrypoints from `remote_ipc.hpp` include:

- `wki_ipc_subsystem_init()`
- `wki_ipc_get_perf_snapshot()`
- `wki_ipc_export_task_fds()`
- `wki_ipc_cleanup_exported_fds()`
- `wki_ipc_find_pipe_affinity_node()`
- `wki_ipc_attach_task_fds()`
- `wki_ipc_doorbell_rx()`
- `wki_ipc_handle_dev_op_req()` / `wki_ipc_handle_dev_op_resp()`
- Socket proxy helpers such as `wki_ipc_socket_shutdown()`, `wki_ipc_socket_getpeername()`, `wki_ipc_socket_getsockopt()`, and `wki_ipc_socket_setsockopt()`.

## Protocol/channel map

Well-known channels:

- `WKI_CHAN_CONTROL = 0`
- `WKI_CHAN_ZONE_MGMT = 1`
- `WKI_CHAN_EVENT_BUS = 2`
- `WKI_CHAN_RESOURCE = 3`
- Dynamic channels start at `WKI_CHAN_DYNAMIC_BASE = 16`
- Reserved dynamic channels start at `WKI_CHAN_DYNAMIC_RESERVED_BASE = 240`
- `WKI_CHAN_IPC_DATA = 240`

Message classes observed in `wire.hpp`:

- Control plane: HELLO, HELLO_ACK, HEARTBEAT, HEARTBEAT_ACK, LSA, FENCE_NOTIFY, reconcile/resource advert/withdraw.
- Zone management: create/destroy/read/write/notify.
- Event bus: subscribe, unsubscribe, publish, ack.
- Resource operations: attach/detach, device ops, IRQ forward, channel open/close.
- Compute: task submit/accept/reject/complete/cancel and load reports.
- Remote IPC rides resource/device operations and task-submit IPC fd maps. `ResourceType` includes IPC resource kinds such as pipe, eventfd, PTY, futex, epoll, and socket.

## Remote VFS observed behavior

Local `remote_vfs.cpp` reviewed on 2026-06-01 includes:

- Server-side VFS exports.
- Remote FD table.
- Consumer-side proxy state.
- Directory cache with a 5-second stale interval.
- Read-ahead cache and write-behind flush behavior.
- DEV_OP request/response flow through WKI resource/device operations.
- Spin-wait send-and-wait helper using `wki_spin_yield_channel()`.

Before modifying:

- Check proxy lock discipline.
- Check cache invalidation on close/write/readdir.
- Check timeout constant and retry behavior.
- Check whether local branch has recursive remote mount protection helpers.

## Remote compute observed behavior

Local `remote_compute.cpp` reviewed on 2026-06-01 includes:

- Scheduler remote-placement hook registration.
- Guard against re-remote-placing `wki-remote` tasks.
- Local load measurement from scheduler run queues.
- Inline task submit over `WKI_CHAN_RESOURCE`.
- Spin-wait for accept/complete using `wki_spin_yield_channel()`.
- Load report sending to connected peers.
- Cleanup on peer fencing.
- Remote task completion monitoring.
- ELF validation/loading and output capture.
- VFS_REF and RESOURCE_REF support in receiver paths, with queued processing for modes that can wait on VFS progress.

Before modifying:

- Check subject task versus current task.
- Check task lifetime after remote submission.
- Check output capture lifetime and file descriptor ownership.
- Check binary size against `WKI_ETH_MAX_PAYLOAD` for inline submit.
- Check rejection/status behavior.

## Remote IPC observed behavior

Local `remote_ipc.cpp` / `remote_ipc_socket.cpp` includes:

- Exporting task file descriptors before remote submission via `WkiIpcFdEntry` records appended to `TASK_SUBMIT`.
- Consumer-side proxy `FileOperations` for pipe/socket style remote descriptors.
- Server/home-side `WkiIpcExport` records holding file references for exported descriptors.
- `s_ipc_lock` protecting export/proxy lists, pending control waits, poll waiters, and cleanup paths.
- Proxy refcounting and local ring buffers for incoming pipe/socket data.
- Deferred DEV_OP worker behavior for operations that should not be handled inline.
- Poll/epoll forwarding and readiness wakeups.
- IPC perf accounting through `WkiPerfScope::REMOTE_IPC` and `/proc/kipcstat`.

Before modifying:

- Check export file reference ownership and release paths.
- Check proxy refcount, close, and blocked-reader wakeups.
- Check whether code can run in RX, syscall, poll, close, or worker context.
- Preserve `WkiIpcFdEntry` and shared-region layout size assertions.
- Verify socket proxy behavior against net syscalls that detect proxy socket files.

## Questions to answer during WKI reconnaissance

- Which message type or channel is involved?
- Is the path control, resource, event, zone, or compute?
- Is the peer direct, routed, fenced, reconnecting, or unknown?
- Does the code run in RX/NAPI, timer, syscall, scheduler, or normal thread context?
- Which locks are held?
- Does the operation wait for an ACK or response?
- Can progress happen while waiting?
- Are payload lengths validated before casts/copies?
- Are path strings bounded and null-terminated where needed?
- Are remote mounts or symlinks involved?
- Are IPC exports/proxies holding file refs correctly?
- Are poll waiters and blocked readers woken on close/error paths?
- Are atomics or movable container entries involved?
