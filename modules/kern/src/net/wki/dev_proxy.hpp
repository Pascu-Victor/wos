#pragma once

#include <cstdint>
#include <dev/block_device.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::wki {

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

constexpr uint64_t WKI_DEV_PROXY_TIMEOUT_US = 100000;       // 100 ms per-operation timeout
constexpr uint64_t WKI_DEV_PROXY_FENCE_WAIT_US = 30000000;  // 30 s — max time to wait for fence lift before teardown
constexpr uint64_t WKI_DEV_PROXY_FENCE_POLL_US = 50000;     // 50 ms — poll interval while waiting for fence lift

// ─────────────────────────────────────────────────────────────────────────────
// ProxyBlockState — one per remote block device attachment (consumer side)
// ─────────────────────────────────────────────────────────────────────────────

struct ProxyBlockState {
    bool active = false;
    bool fenced = false;         // peer is fenced — ops should block and wait for reconnection
    uint64_t fence_time_us = 0;  // timestamp when fenced (for timeout-based teardown)
    uint16_t owner_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    uint32_t resource_id = 0;
    uint16_t max_op_size = 0;

    // Synchronous blocking for DEV_OP_RESP
    volatile bool op_pending = false;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
    int16_t op_status = 0;
    void* op_resp_buf = nullptr;
    uint16_t op_resp_len = 0;
    uint16_t op_resp_max = 0;

    // Attach handshake (DEV_ATTACH_ACK)
    volatile bool attach_pending = false;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
    uint8_t attach_status = 0;
    uint16_t attach_channel = 0;
    uint16_t attach_max_op_size = 0;

    // Registered block device (callers use this transparently)
    ker::dev::BlockDevice bdev;

    ker::mod::sys::Spinlock lock;
};

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

// Initialize the device proxy subsystem. Called from wki_init().
void wki_dev_proxy_init();

// Attach to a remote block device. Sends DEV_ATTACH_REQ and blocks until ACK.
// On success, registers a proxy BlockDevice and returns a pointer to it.
// On failure, returns nullptr.
auto wki_dev_proxy_attach_block(uint16_t owner_node, uint32_t resource_id, const char* local_name) -> ker::dev::BlockDevice*;

// Detach a proxy block device. Sends DEV_DETACH to the owner.
void wki_dev_proxy_detach_block(ker::dev::BlockDevice* proxy_bdev);

// Suspend all proxies for a fenced peer — block device stays registered but
// I/O operations will block until the fence is lifted or a timeout expires.
// Called from wki_peer_fence().
void wki_dev_proxy_suspend_for_peer(uint16_t node_id);

// Resume suspended proxies after a peer reconnects.  Re-attaches the proxy
// channel so that blocked I/O can complete.  Called from handle_hello()
// on the FENCED → RECONNECTING → CONNECTED path.
void wki_dev_proxy_resume_for_peer(uint16_t node_id);

// Hard-detach all proxies for a peer (final teardown after fence timeout).
// Unregisters block devices and unmounts dependent filesystems.
void wki_dev_proxy_detach_all_for_peer(uint16_t node_id);

// Periodic check: tear down proxies that have been fenced longer than
// WKI_DEV_PROXY_FENCE_WAIT_US.  Called from wki_peer_timer_tick().
void wki_dev_proxy_fence_timeout_tick(uint64_t now_us);

// ─────────────────────────────────────────────────────────────────────────────
// Internal — RX message handlers (called from wki.cpp dispatch)
// ─────────────────────────────────────────────────────────────────────────────

namespace detail {

void handle_dev_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_dev_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
