#include "dev_proxy.hpp"

#include <array>
#include <cstring>
#include <deque>
#include <dev/block_device.hpp>
#include <memory>
#include <net/wki/remotable.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Storage
// -----------------------------------------------------------------------------

namespace {
// unique_ptr indirection: ProxyBlockState contains Spinlock (std::atomic<bool>) which has
// deleted move-assignment. std::deque::erase needs to shift elements, so we store via
// unique_ptr so the deque only moves pointers.
std::deque<std::unique_ptr<ProxyBlockState>> g_proxies;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_dev_proxy_initialized = false;                    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto find_proxy_by_bdev(ker::dev::BlockDevice* bdev) -> ProxyBlockState* {
    for (auto& p : g_proxies) {
        if (p->active && &p->bdev == bdev) {
            return p.get();
        }
    }
    return nullptr;
}

auto find_proxy_by_channel(uint16_t owner_node, uint16_t channel_id) -> ProxyBlockState* {
    for (auto& p : g_proxies) {
        if (p->active && p->owner_node == owner_node && p->assigned_channel == channel_id) {
            return p.get();
        }
    }
    return nullptr;
}

auto find_proxy_by_attach(uint16_t owner_node) -> ProxyBlockState* {
    for (auto& p : g_proxies) {
        if (p->attach_pending && p->owner_node == owner_node) {
            return p.get();
        }
    }
    return nullptr;
}

// -----------------------------------------------------------------------------
// Remote block device function pointers
// -----------------------------------------------------------------------------

auto remote_block_read(ker::dev::BlockDevice* dev, uint64_t block, size_t count, void* buffer) -> int {
    auto* state = find_proxy_by_bdev(dev);
    if (state == nullptr || !state->active) {
        return -1;
    }

    auto* dest = static_cast<uint8_t*>(buffer);
    uint64_t lba = block;
    auto remaining = static_cast<uint32_t>(count);

    // Calculate max blocks per chunk based on response payload capacity
    auto max_resp_data = static_cast<uint32_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload));
    uint32_t blocks_per_chunk = max_resp_data / static_cast<uint32_t>(dev->block_size);
    if (blocks_per_chunk == 0) {
        return -1;
    }

    while (remaining > 0) {
        uint32_t chunk = (remaining > blocks_per_chunk) ? blocks_per_chunk : remaining;
        auto chunk_bytes = static_cast<uint16_t>(chunk * static_cast<uint32_t>(dev->block_size));

        // Build request: DevOpReqPayload + {lba:u64, count:u32}
        constexpr uint16_t REQ_DATA_LEN = 12;
        std::array<uint8_t, sizeof(DevOpReqPayload) + REQ_DATA_LEN> req_buf = {};

        auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
        req->op_id = OP_BLOCK_READ;
        req->data_len = REQ_DATA_LEN;

        auto* req_data = req_buf.data() + sizeof(DevOpReqPayload);
        memcpy(req_data, &lba, sizeof(uint64_t));
        memcpy(req_data + sizeof(uint64_t), &chunk, sizeof(uint32_t));

        // Set up blocking state
        state->lock.lock();
        state->op_pending = true;
        state->op_status = 0;
        state->op_resp_buf = dest;
        state->op_resp_max = chunk_bytes;
        state->op_resp_len = 0;
        state->lock.unlock();

        // Send request
        int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf.data(),
                                static_cast<uint16_t>(sizeof(DevOpReqPayload) + REQ_DATA_LEN));
        if (send_ret != WKI_OK) {
            state->op_pending = false;
            return -1;
        }

        // Spin-wait for response
        uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
        while (state->op_pending) {
            if (wki_now_us() >= deadline) {
                state->op_pending = false;
                return -1;
            }
            asm volatile("pause" ::: "memory");
        }

        if (state->op_status != 0) {
            return static_cast<int>(state->op_status);
        }

        dest += chunk_bytes;
        lba += chunk;
        remaining -= chunk;
    }

    return 0;
}

auto remote_block_write(ker::dev::BlockDevice* dev, uint64_t block, size_t count, const void* buffer) -> int {
    auto* state = find_proxy_by_bdev(dev);
    if (state == nullptr || !state->active) {
        return -1;
    }

    const auto* src = static_cast<const uint8_t*>(buffer);
    uint64_t lba = block;
    auto remaining = static_cast<uint32_t>(count);

    // Calculate max blocks per chunk based on request payload capacity
    // Request: DevOpReqPayload(4) + lba(8) + count(4) + data
    constexpr uint32_t WRITE_HDR_OVERHEAD = sizeof(DevOpReqPayload) + 12;
    auto max_req_data = static_cast<uint32_t>(WKI_ETH_MAX_PAYLOAD - WRITE_HDR_OVERHEAD);
    uint32_t blocks_per_chunk = max_req_data / static_cast<uint32_t>(dev->block_size);
    if (blocks_per_chunk == 0) {
        return -1;
    }

    while (remaining > 0) {
        uint32_t chunk = (remaining > blocks_per_chunk) ? blocks_per_chunk : remaining;
        auto chunk_bytes = (chunk * static_cast<uint32_t>(dev->block_size));

        // Build request: DevOpReqPayload + {lba:u64, count:u32} + data
        auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + 12 + chunk_bytes);
        auto* req_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(req_total));
        if (req_buf == nullptr) {
            return -1;
        }

        auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf);
        req->op_id = OP_BLOCK_WRITE;
        req->data_len = static_cast<uint16_t>(12 + chunk_bytes);

        auto* req_data = req_buf + sizeof(DevOpReqPayload);
        memcpy(req_data, &lba, sizeof(uint64_t));
        memcpy(req_data + sizeof(uint64_t), &chunk, sizeof(uint32_t));
        memcpy(req_data + 12, src, chunk_bytes);

        // Set up blocking state
        state->lock.lock();
        state->op_pending = true;
        state->op_status = 0;
        state->op_resp_buf = nullptr;
        state->op_resp_max = 0;
        state->op_resp_len = 0;
        state->lock.unlock();

        // Send request
        int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf, req_total);
        ker::mod::mm::dyn::kmalloc::free(req_buf);

        if (send_ret != WKI_OK) {
            state->op_pending = false;
            return -1;
        }

        // Spin-wait for response
        uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
        while (state->op_pending) {
            if (wki_now_us() >= deadline) {
                state->op_pending = false;
                return -1;
            }
            asm volatile("pause" ::: "memory");
        }

        if (state->op_status != 0) {
            return static_cast<int>(state->op_status);
        }

        src += chunk_bytes;
        lba += chunk;
        remaining -= chunk;
    }

    return 0;
}

auto remote_block_flush(ker::dev::BlockDevice* dev) -> int {
    auto* state = find_proxy_by_bdev(dev);
    if (state == nullptr || !state->active) {
        return -1;
    }

    // Build request: DevOpReqPayload with no data
    DevOpReqPayload req = {};
    req.op_id = OP_BLOCK_FLUSH;
    req.data_len = 0;

    // Set up blocking state
    state->lock.lock();
    state->op_pending = true;
    state->op_status = 0;
    state->op_resp_buf = nullptr;
    state->op_resp_max = 0;
    state->op_resp_len = 0;
    state->lock.unlock();

    int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, &req, sizeof(req));
    if (send_ret != WKI_OK) {
        state->op_pending = false;
        return -1;
    }

    // Spin-wait for response
    uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
    while (state->op_pending) {
        if (wki_now_us() >= deadline) {
            state->op_pending = false;
            return -1;
        }
        asm volatile("pause" ::: "memory");
    }

    return static_cast<int>(state->op_status);
}

}  // namespace

// -----------------------------------------------------------------------------
// Init
// -----------------------------------------------------------------------------

void wki_dev_proxy_init() {
    if (g_dev_proxy_initialized) {
        return;
    }
    g_dev_proxy_initialized = true;
    ker::mod::dbg::log("[WKI] Dev proxy subsystem initialized");
}

// -----------------------------------------------------------------------------
// Attach / Detach
// -----------------------------------------------------------------------------

auto wki_dev_proxy_attach_block(uint16_t owner_node, uint32_t resource_id, const char* local_name) -> ker::dev::BlockDevice* {
    // Allocate a new proxy state via unique_ptr (deque stores pointers, not objects)
    g_proxies.push_back(std::make_unique<ProxyBlockState>());
    auto* state = g_proxies.back().get();

    state->owner_node = owner_node;
    state->resource_id = resource_id;

    // Send DEV_ATTACH_REQ
    DevAttachReqPayload attach_req = {};
    attach_req.target_node = owner_node;
    attach_req.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
    attach_req.resource_id = resource_id;
    attach_req.attach_mode = static_cast<uint8_t>(AttachMode::PROXY);
    attach_req.requested_channel = 0;  // auto-assign

    state->attach_pending = true;
    state->attach_status = 0;
    state->attach_channel = 0;

    int send_ret = wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ, &attach_req, sizeof(attach_req));
    if (send_ret != WKI_OK) {
        g_proxies.pop_back();
        return nullptr;
    }

    // Spin-wait for attach ACK
    uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
    while (state->attach_pending) {
        if (wki_now_us() >= deadline) {
            state->attach_pending = false;
            g_proxies.pop_back();
            ker::mod::dbg::log("[WKI] Dev proxy attach timeout: node=0x%04x res_id=%u", owner_node, resource_id);
            return nullptr;
        }
        asm volatile("pause" ::: "memory");
    }

    // Check attach result
    if (state->attach_status != static_cast<uint8_t>(DevAttachStatus::OK)) {
        ker::mod::dbg::log("[WKI] Dev proxy attach rejected: node=0x%04x res_id=%u status=%u", owner_node, resource_id,
                           state->attach_status);
        g_proxies.pop_back();
        return nullptr;
    }

    state->assigned_channel = state->attach_channel;
    state->max_op_size = state->attach_max_op_size;

    // Query block device info via OP_BLOCK_INFO
    DevOpReqPayload info_req = {};
    info_req.op_id = OP_BLOCK_INFO;
    info_req.data_len = 0;

    // Use op_pending for the info response
    std::array<uint8_t, 16> info_buf = {};
    state->lock.lock();
    state->op_pending = true;
    state->op_status = 0;
    state->op_resp_buf = static_cast<void*>(info_buf.data());
    state->op_resp_max = info_buf.size();
    state->op_resp_len = 0;
    state->lock.unlock();

    send_ret = wki_send(owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, &info_req, sizeof(info_req));
    if (send_ret != WKI_OK) {
        // Clean up: send detach and remove
        DevDetachPayload det = {};
        det.target_node = owner_node;
        det.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
        det.resource_id = resource_id;
        wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));
        g_proxies.pop_back();
        return nullptr;
    }

    // Spin-wait for info response
    deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
    while (state->op_pending) {
        if (wki_now_us() >= deadline) {
            state->op_pending = false;
            g_proxies.pop_back();
            return nullptr;
        }
        asm volatile("pause" ::: "memory");
    }

    if (state->op_status != 0 || state->op_resp_len < 16) {
        g_proxies.pop_back();
        return nullptr;
    }

    // Parse block info: {block_size:u64, total_blocks:u64}
    uint64_t block_size = 0;
    uint64_t total_blocks = 0;
    memcpy(&block_size, static_cast<const void*>(info_buf.data()), sizeof(uint64_t));
    memcpy(&total_blocks, static_cast<const void*>(info_buf.data() + sizeof(uint64_t)), sizeof(uint64_t));

    // Populate the proxy BlockDevice
    state->bdev.major = 0;
    state->bdev.minor = 0;
    if (local_name != nullptr) {
        size_t name_len = strlen(local_name);
        if (name_len >= ker::dev::BLOCK_NAME_SIZE) {
            name_len = ker::dev::BLOCK_NAME_SIZE - 1;
        }
        memcpy(state->bdev.name.data(), local_name, name_len);
        state->bdev.name[name_len] = '\0';
    }
    state->bdev.block_size = static_cast<size_t>(block_size);
    state->bdev.total_blocks = total_blocks;
    state->bdev.read_blocks = remote_block_read;
    state->bdev.write_blocks = remote_block_write;
    state->bdev.flush = remote_block_flush;
    state->bdev.private_data = state;
    state->active = true;

    // Check for naming collision before registering
    if (ker::dev::block_device_find_by_name(state->bdev.name.data()) != nullptr) {
        ker::mod::dbg::log("[WKI] Dev proxy name collision: %s already registered", state->bdev.name.data());
        // Send detach and clean up
        DevDetachPayload det = {};
        det.target_node = owner_node;
        det.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
        det.resource_id = resource_id;
        wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));
        g_proxies.pop_back();
        return nullptr;
    }

    // Register in the block device subsystem
    ker::dev::block_device_register(&state->bdev);

    ker::mod::dbg::log("[WKI] Dev proxy attached: %s node=0x%04x res_id=%u ch=%u bs=%u tb=%llu", state->bdev.name.data(), owner_node,
                       resource_id, state->assigned_channel, static_cast<unsigned>(block_size), total_blocks);

    return &state->bdev;
}

void wki_dev_proxy_detach_block(ker::dev::BlockDevice* proxy_bdev) {
    auto* state = find_proxy_by_bdev(proxy_bdev);
    if (state == nullptr) {
        return;
    }

    // Unregister from block device subsystem
    ker::dev::block_device_unregister(&state->bdev);

    // Send DEV_DETACH to owner
    DevDetachPayload det = {};
    det.target_node = state->owner_node;
    det.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
    det.resource_id = state->resource_id;
    wki_send(state->owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));

    // Close the dynamic channel
    WkiChannel* ch = wki_channel_get(state->owner_node, state->assigned_channel);
    if (ch != nullptr) {
        wki_channel_close(ch);
    }

    ker::mod::dbg::log("[WKI] Dev proxy detached: %s", state->bdev.name.data());

    state->active = false;

    // Remove inactive entries
    for (auto it = g_proxies.begin(); it != g_proxies.end();) {
        if (!(*it)->active) {
            it = g_proxies.erase(it);
        } else {
            ++it;
        }
    }
}

// -----------------------------------------------------------------------------
// Fencing cleanup
// -----------------------------------------------------------------------------

void wki_dev_proxy_detach_all_for_peer(uint16_t node_id) {
    for (auto& p : g_proxies) {
        if (!p->active || p->owner_node != node_id) {
            continue;
        }

        // Fail any pending operation
        if (p->op_pending) {
            p->op_status = -1;
            p->op_pending = false;
        }

        // Close the dynamic channel
        WkiChannel* ch = wki_channel_get(p->owner_node, p->assigned_channel);
        if (ch != nullptr) {
            wki_channel_close(ch);
        }

        // Unregister from block device subsystem
        ker::dev::block_device_unregister(&p->bdev);

        ker::mod::dbg::log("[WKI] Dev proxy fenced: %s node=0x%04x", p->bdev.name.data(), node_id);

        p->active = false;
    }

    // Remove inactive entries
    for (auto it = g_proxies.begin(); it != g_proxies.end();) {
        if (!(*it)->active) {
            it = g_proxies.erase(it);
        } else {
            ++it;
        }
    }
}

// -----------------------------------------------------------------------------
// RX handlers
// -----------------------------------------------------------------------------

namespace detail {

void handle_dev_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevAttachAckPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const DevAttachAckPayload*>(payload);

    // Find the pending attach for this owner node
    ProxyBlockState* state = find_proxy_by_attach(hdr->src_node);
    if (state == nullptr) {
        return;
    }

    state->attach_status = ack->status;
    state->attach_channel = ack->assigned_channel;
    state->attach_max_op_size = ack->max_op_size;

    // Release the spin-wait
    asm volatile("" ::: "memory");
    state->attach_pending = false;
}

void handle_dev_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevOpRespPayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const DevOpRespPayload*>(payload);
    const uint8_t* resp_data = payload + sizeof(DevOpRespPayload);
    uint16_t resp_data_len = resp->data_len;

    // Verify data fits
    if (sizeof(DevOpRespPayload) + resp_data_len > payload_len) {
        return;
    }

    // Find proxy by (src_node, channel_id)
    ProxyBlockState* state = find_proxy_by_channel(hdr->src_node, hdr->channel_id);
    if (state == nullptr || !state->op_pending) {
        return;
    }

    state->lock.lock();
    state->op_status = resp->status;

    // Copy response data if there's a destination buffer
    if (resp_data_len > 0 && state->op_resp_buf != nullptr) {
        uint16_t copy_len = (resp_data_len > state->op_resp_max) ? state->op_resp_max : resp_data_len;
        memcpy(state->op_resp_buf, resp_data, copy_len);
        state->op_resp_len = copy_len;
    } else {
        state->op_resp_len = 0;
    }

    state->lock.unlock();

    // Release the spin-wait
    asm volatile("" ::: "memory");
    state->op_pending = false;
}

}  // namespace detail

}  // namespace ker::net::wki
