---
applyTo: "modules/kern/src/net/wki/**"
---

# WKI Subsystem Conventions

WKI code is one of the highest-risk areas of WOS. It combines networking, wire protocol, peer state, routing, reliable channels, remote VFS, remote compute, eventing, remote devices, and fencing.

## Namespace and organization

- All WKI code lives in `ker::net::wki`.
- Internal RX handlers live in `ker::net::wki::detail`.
- Anonymous namespaces in `.cpp` files hold file-local storage such as `g_*` globals and helpers.
- The global state singleton is `extern WkiState g_wki;`, declared in `wki.hpp` and defined in `wki.cpp`.

## Function naming

- Public API functions: `wki_*()`.
- Subsystem-specific functions: `wki_<subsys>_*()`.
- RX handlers: `detail::handle_*()`.
- Timer callbacks: `wki_*_timer_tick()` or `wki_timer_thread()`.

## Read first for WKI tasks

Always read:

- `modules/kern/src/net/wki/wki.hpp`
- `modules/kern/src/net/wki/wire.hpp`
- `modules/kern/src/net/wki/wki.cpp`
- The specific subsystem file being touched.

For remote VFS:

- `remote_vfs.hpp`
- `remote_vfs.cpp`
- VFS core/mount/path code.

For remote compute:

- `remote_compute.hpp`
- `remote_compute.cpp`
- Scheduler/task code.

For device proxy/server:

- `dev_proxy.hpp`, `dev_proxy.cpp`
- `dev_server.hpp`, `dev_server.cpp`
- `remotable.hpp`, `remotable.cpp`
- `blk_ring.hpp`

## Current local file map

Reviewed: 2026-06-01.

Observed WKI files include:

- Core: `wki.hpp`, `wki.cpp`, `wire.hpp`
- Peer/channel/routing: `peer.*`, `channel.*`, `routing.*`
- Shared memory/zones: `zone.*`
- Events: `event.*`
- Remote device/resource: `dev_proxy.*`, `dev_server.*`, `remotable.*`, `blk_ring.hpp`, `irq_fwd.*`
- Remote files/network/compute/IPC: `remote_vfs.*`, `remote_net.*`, `remote_compute.*`, `remote_ipc.*`, `remote_ipc_socket.cpp`
- Transports: `transport_eth.*`, `transport_roce.*`, `transport_ivshmem.*`

Verify this list against the local branch before relying on it.

## Key constants and concepts

Use named constants instead of magic numbers.

- Wire: `WKI_HEADER_SIZE`, `WKI_ETH_MAX_PAYLOAD`, `WKI_MAX_FRAME_SIZE`
- Peers: `WKI_MAX_PEERS`, `WKI_NODE_INVALID`, `WKI_NODE_BROADCAST`
- Channels: `WKI_CHAN_CONTROL`, `WKI_CHAN_ZONE_MGMT`, `WKI_CHAN_EVENT_BUS`, `WKI_CHAN_RESOURCE`, `WKI_CHAN_DYNAMIC_BASE`, `WKI_CHAN_DYNAMIC_RESERVED_BASE`, `WKI_CHAN_IPC_DATA`
- Peer/channel capacities: `WKI_MAX_TRANSPORTS`, `WKI_MAX_CHANNELS`
- IPC/data credits: `WKI_CREDITS_IPC_DATA`, `WKI_REORDER_IPC_DATA` when touching remote IPC paths.
- Heartbeat/RTO/credit constants from `wki.hpp`
- VFS: `VFS_EXPORT_PATH_LEN`, `VFS_RDMA_BULK_SIZE` when present locally
- Timeouts: `WKI_OP_TIMEOUT_US`, `WKI_DEV_PROXY_TIMEOUT_US`, `WKI_TASK_SUBMIT_TIMEOUT_US` when present locally

Local values reviewed on 2026-06-01 include:

- `WKI_MAX_PEERS = 256`
- `WKI_MAX_TRANSPORTS = 8`
- `WKI_MAX_CHANNELS = 256`
- `WKI_HEADER_SIZE = 32`
- `WKI_ETH_MAX_PAYLOAD = 8954`
- `WKI_NODE_INVALID = 0x0000`
- `WKI_NODE_BROADCAST = 0xFFFF`
- well-known channels `0..3`, dynamic channels starting at `16`, reserved dynamic channels starting at `240`, and `WKI_CHAN_IPC_DATA = 240`

## Wire protocol rules

- All wire structs use `__attribute__((packed))`.
- Fixed-size wire structs must have `static_assert` size checks.
- Do not reorder fields in wire structs unless intentionally changing protocol ABI.
- Check all handlers before adding or changing `MsgType` values.
- Check payload-length validation before reading variable-length data.
- Preserve sequence number arithmetic helpers in `wire.hpp`.
- Preserve channel ID semantics.

## Locking

Known locks include:

- Per-peer: `WkiPeer::lock`
- Per-channel: `WkiChannel::lock`
- Global peer table: `WkiState::peer_lock`
- Transport registry: `WkiState::transport_lock`
- Channel pool lock in `wki.cpp`
- `remote_compute.cpp`: file-local compute lock if present in local branch
- Per-proxy: `ProxyVfsState::lock`, `ProxyNetState::lock`, `ProxyBlockState::lock`

Before taking a lock, check whether RX/NAPI, scheduler, syscall, or timer context could already hold another relevant lock.

## Preferred buffer style

Prefer `std::array` over C-style arrays for local buffers. Use `.data()` and `.size()`.

C-style arrays may remain where required by ABI, packed wire overlays, compiler constraints, or existing carefully audited code. Do not perform broad style-only rewrites.

## WKI initialization

Local `wki_init()` initializes the global node state and then initializes WKI subsystems. Observed order includes routing, zone, RoCE transport, remotable devices, device server, device proxy, event bus, IRQ forwarding, remote VFS, remote NIC, remote compute, and the IPC proxy subsystem.

If adding a subsystem, check dependency order. Do not assume a later subsystem is available during earlier initialization.

## Transport and send path notes

Local send path concepts:

- `wki_send_raw()` is used for raw/unreliable messages such as HELLO/HEARTBEAT.
- `wki_send()` sends reliable messages through a `WkiChannel` with credits, seq/ack, retransmit tracking, and transport resolution.
- Direct single-hop peers may bypass WKI CRC when Ethernet FCS is considered sufficient; routed paths may use CRC.
- Transport ownership of `PacketBuffer` differs by TX function; verify before changing ownership or error paths.

## Async/spin-wait pattern

Use the local branch's established async wait pattern. Existing local instructions mention:

- `WkiWaitEntry` on the kernel stack.
- `wki_wait_for_op()` / `wki_wake_op()`.

The local branch also contains targeted spin-yield helpers such as `wki_spin_yield()` and `wki_spin_yield_channel()` for spin-wait loops that need to drive WKI progress. Verify the current wait pattern before editing.

## Remote VFS path construction

Remote VFS paths follow:

```text
/wki/<hostname>/<local_path_without_leading_slash>
```

Rules:

- During init, the local hostname/self path may be symlinked to `/wki/<local_hostname>` in local-branch code.
- On peer connect, remote VFS mounts may be created under `/wki/<peer_hostname>`.
- When building VFS_REF paths, strip the leading `/` from the absolute local path, then prepend `/wki/<hostname>/`.
- Server-side path resolution must not follow into another `FSType::REMOTE` mount.
- Readdir on remote `/wki/` directories must filter self-referencing entries.

## Critical WKI gotchas

1. Do not use `make_absolute()` from scheduler hooks or remote compute if it resolves through `get_current_task()`. That can resolve against the running task, not the task being submitted. Resolve relative paths manually with the submitted task's `cwd`.

2. Do not call ACK-waiting operations such as `wki_remote_vfs_mount()` or `wki_zone_create()` from NAPI poll/RX context. The ACK may require the same context to progress. Defer to a timer/thread/pending queue.

3. Structs containing `std::atomic<bool>` need explicit move constructors if stored in containers that move elements. Use relaxed load/store for move-only state copies when that is the intended semantics.

4. Server-side path resolution must protect against recursive remote mount traversal using the local branch's protection helper, such as `path_crosses_remote_mount()` when present.

5. Proxy block devices are suspended, not immediately destroyed, on fence. Reconnect may resume operations during the grace period; after timeout, hard teardown and unmount are expected.

6. Do not broaden WKI logging in hot RX/TX/spin-wait paths without checking allocation and lock behavior.

7. Remote IPC paths (`remote_ipc.*`, `remote_ipc_socket.cpp`) are WKI resource/data-channel users for cross-node pipe, socket, PTY, futex, eventfd, and epoll forwarding. Check `s_ipc_lock`, exported file references, proxy refcounts, poll waiters, and deferred DEV_OP worker behavior before changing them.

## Logging in WKI

New WKI code should use the journal-backed typed logger convention when available:

```cpp
using log = ker::mod::dbg::logger<"wki">;
```

Existing old-style `ker::mod::dbg::log("[WKI] ...")` may remain during migration. Do not perform a wholesale logging migration unless requested.
