#pragma once
// Host shim for net/netdevice.hpp — minimal NetDevice stub.

#include <cstddef>
#include <cstdint>
#include <net/packet.hpp>

namespace ker::net {

struct NetDevice {
    const char* name = nullptr;
    uint16_t mtu = 1500;
};

}  // namespace ker::net

// WKI forward declarations expected by netdevice.hpp consumers
namespace ker::net::wki {
struct WkiTransport;
}
