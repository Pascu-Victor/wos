#include "net.hpp"

#include <net/loopback.hpp>
#include <net/packet.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::net {

void init() {
#ifdef DEBUG_NET
    ker::mod::dbg::log("net: Initializing networking subsystem");
#endif

    // Initialize packet buffer pool
    pkt_pool_init();

    // Initialize loopback device
    loopback_init();

#ifdef DEBUG_NET
    ker::mod::dbg::log("net: Networking subsystem ready");
#endif
}

}  // namespace ker::net
