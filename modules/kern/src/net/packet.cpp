#include "packet.hpp"

#include <cstring>
#include <net/netdevice.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net {

namespace {
PacketBuffer* pool = nullptr;
size_t pool_capacity = 0;
PacketBuffer* free_list = nullptr;
ker::mod::sys::Spinlock pool_lock;
bool initialized = false;

void add_buffers_to_pool(size_t count) {
    // Allocate new buffers
    auto* new_buffers = static_cast<PacketBuffer*>(ker::mod::mm::dyn::kmalloc::calloc(count, sizeof(PacketBuffer)));
    if (new_buffers == nullptr) {
        ker::mod::dbg::log("net: Failed to allocate %zu packet buffers", count);
        return;
    }

    pool_lock.lock();
    // Link new buffers into free list
    for (size_t i = 0; i < count; i++) {
        new_buffers[i].next = free_list;
        free_list = &new_buffers[i];
    }
    pool_capacity += count;
    pool_lock.unlock();

    ker::mod::dbg::log("net: Added %zu packet buffers (total: %zu)", count, pool_capacity);
}
}  // namespace

void pkt_pool_init() {
    if (initialized) {
        return;
    }

    // Start with minimum pool size
    add_buffers_to_pool(PKT_POOL_MIN_SIZE);
    initialized = true;
}

void pkt_pool_expand_for_nics() {
    // Calculate required size: 1024 buffers per NIC, minimum 1024 total
    size_t nic_count = netdev_count();
    size_t required = nic_count * PKT_POOL_PER_NIC;
    if (required < PKT_POOL_MIN_SIZE) {
        required = PKT_POOL_MIN_SIZE;
    }

    // Add more buffers if needed
    if (required > pool_capacity) {
        size_t to_add = required - pool_capacity;
        add_buffers_to_pool(to_add);
    }
}

auto pkt_pool_size() -> size_t { return pool_capacity; }

auto pkt_alloc() -> PacketBuffer* {
    // With NAPI, packet allocation is only called from worker thread context
    pool_lock.lock();
    if (free_list == nullptr) {
        pool_lock.unlock();
        return nullptr;
    }

    auto* pkt = free_list;
    free_list = pkt->next;
    pool_lock.unlock();

    // Initialize the packet buffer
    pkt->data = pkt->storage.data() + PKT_HEADROOM;
    pkt->len = 0;
    pkt->next = nullptr;
    pkt->dev = nullptr;
    pkt->protocol = 0;

    return pkt;
}

void pkt_free(PacketBuffer* pkt) {
    if (pkt == nullptr) {
        return;
    }

    // With NAPI, packet freeing is only called from worker thread context
    pool_lock.lock();
    pkt->next = free_list;
    free_list = pkt;
    pool_lock.unlock();
}

}  // namespace ker::net
