#include "net.hpp"

#include <net/loopback.hpp>
#include <net/packet.hpp>
#include <net/route.hpp>
#include <net/route6.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::net {

using log = ker::mod::dbg::logger<"net">;

void init() {
#ifdef DEBUG_NET
    log::debug("Initializing networking subsystem");
#endif

    // Initialize packet buffer pool
    pkt_pool_init();

    // Route registries must be empty before loopback publishes its connected
    // IPv4 and IPv6 routes.
    route_init();
    route6_init();

    // Initialize loopback device
    loopback_init();

#ifdef DEBUG_NET
    log::debug("Networking subsystem ready");
#endif
}

}  // namespace ker::net
