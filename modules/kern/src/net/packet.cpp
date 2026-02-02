#include "packet.hpp"

#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net {

namespace {
PacketBuffer pool[PKT_POOL_SIZE];
PacketBuffer* free_list = nullptr;
ker::mod::sys::Spinlock pool_lock;
bool initialized = false;
}  // namespace

void pkt_pool_init() {
    if (initialized) {
        return;
    }

    free_list = nullptr;
    for (size_t i = 0; i < PKT_POOL_SIZE; i++) {
        pool[i].next = free_list;
        free_list = &pool[i];
    }

    initialized = true;
#ifdef DEBUG_NET
    ker::mod::dbg::log("net: PacketBuffer pool initialized (%d buffers, %d bytes each)", PKT_POOL_SIZE, PKT_BUF_SIZE);
#endif
}

auto pkt_alloc() -> PacketBuffer* {
    pool_lock.lock();
    if (free_list == nullptr) {
        pool_lock.unlock();
        return nullptr;
    }

    auto* pkt = free_list;
    free_list = pkt->next;
    pool_lock.unlock();

    // Initialize the packet buffer
    pkt->data = pkt->storage + PKT_HEADROOM;
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

    pool_lock.lock();
    pkt->next = free_list;
    free_list = pkt;
    pool_lock.unlock();
}

}  // namespace ker::net
