---
applyTo: "modules/kern/src/net/wki/**"
---

# WKI Subsystem Conventions

## Namespace & Code Organization

- All code lives in `ker::net::wki`. Internal RX handlers go in `ker::net::wki::detail`.
- Anonymous namespaces in `.cpp` files hold storage (`g_*` globals) and helper functions.
- Global state singleton: `extern WkiState g_wki;` (declared in `wki.hpp`).

## Function Naming

- Public API: `wki_*()` (e.g., `wki_init()`, `wki_send()`, `wki_peer_find()`)
- Subsystem-specific: `wki_<subsys>_*()` (e.g., `wki_remote_vfs_mount()`, `wki_dev_proxy_attach_block()`)
- RX handlers: `detail::handle_*()` (e.g., `detail::handle_hello()`, `detail::handle_task_submit()`)
- Timer callbacks: `wki_*_timer_tick()`, `wki_timer_thread()`

## Key Constants (use these, not magic numbers)

- Path limits: `Task::CWD_MAX` (256), `Task::EXE_PATH_MAX` (256), `WKI_HOSTNAME_MAX` (64)
- Wire: `WKI_HEADER_SIZE` (32), `WKI_ETH_MAX_PAYLOAD` (8954)
- VFS: `VFS_EXPORT_PATH_LEN` (256), `VFS_RDMA_BULK_SIZE` (2MB)
- Timeouts: `WKI_OP_TIMEOUT_US` (5s), `WKI_DEV_PROXY_TIMEOUT_US` (100ms)
- Peer: `WKI_MAX_PEERS` (256), `WKI_NODE_INVALID` (0x0000), `WKI_NODE_BROADCAST` (0xFFFF)

## Locking

- Per-peer: `WkiPeer::lock` (`Spinlock`)
- Per-channel: `WkiChannel::lock` (`Spinlock`)
- Global peer table: `WkiState::peer_lock`
- `remote_compute.cpp`: `s_compute_lock` (file-local `Spinlock`)
- Per-proxy: `ProxyVfsState::lock`, `ProxyNetState::lock`, `ProxyBlockState::lock`

## Prefer `std::array` over C-style arrays for local buffers

Use `std::array<char, N>` with `.data()` and `.size()` instead of raw `char buf[N]`.

## VFS Path Construction

Remote VFS paths follow the pattern `/wki/<hostname>/<local_path_without_leading_slash>`.

- During init: `vfs_symlink("/", "/wki/<local_hostname>")` creates a self-symlink
- On peer connect: `wki_remote_vfs_mount()` creates `FSType::REMOTE` mounts under `/wki/<peer_hostname>`
- When building VFS_REF paths: strip leading `/` from the absolute local path, then prepend `/wki/<hostname>/`

## Critical Gotchas

1. **Cannot use `make_absolute()` from scheduler hooks or remote compute**: It calls `get_current_task()` which returns the _running_ task, not the task being submitted. Resolve relative paths manually using `task->cwd`.

2. **NAPI re-entrance deadlocks**: `wki_remote_vfs_mount()` and `wki_zone_create()` spin-wait for ACKs. If called from NAPI poll context, the ACK can never arrive. Always defer these to the timer tick via pending queues.

3. **`std::atomic<bool>` deletes move constructors**: Structs containing atomics (`SubmittedTask`, `DevServerBinding`, `ProxyBlockState`) need explicit move constructors using `std::memory_order_relaxed` load/store.

4. **Recursive VFS protection**: Server-side path resolution must not follow into other `FSType::REMOTE` mounts (`path_crosses_remote_mount()`). Readdir on remote `/wki/` dirs filters out self-referencing entries.

5. **Fence lifecycle**: Proxy block devices are suspended (not destroyed) on fence, with a 30s grace period. If the peer reconnects, ops resume. After timeout, hard teardown + unmount.

## Wire Protocol

- All wire structs use `__attribute__((packed))` with `static_assert` on size.
- Channels: `WKI_CHAN_CONTROL` (0), `WKI_CHAN_ZONE_MGMT` (1), `WKI_CHAN_EVENT_BUS` (2), `WKI_CHAN_RESOURCE` (3), dynamic channels start at `WKI_CHAN_DYNAMIC_BASE` (16+).
- Async wait pattern: `WkiWaitEntry` on kernel stack + `wki_wait_for_op()` / `wki_wake_op()`.
