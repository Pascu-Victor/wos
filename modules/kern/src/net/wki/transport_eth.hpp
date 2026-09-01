#pragma once

#include <cstdint>
#include <net/address.hpp>
#include <net/netdevice.hpp>
#include <net/packet.hpp>

namespace ker::net::wki {

struct WkiHeader;
struct WkiTransport;

// Claim a config-assigned WKI NIC before peer discovery can snapshot local
// resources. Claiming does not register a transport or send any traffic.
void wki_eth_transport_claim(net::NetDevice* netdev);

// Initialize the Ethernet WKI transport on the given NIC.
// Called from main.cpp during boot after NIC drivers are probed.
void wki_eth_transport_init(net::NetDevice* netdev);

// RX entry point - called from ethernet.cpp's eth_rx() dispatch
void wki_eth_rx(net::NetDevice* dev, net::PacketBuffer* pkt);

// Neighbor MAC table management (used by peer.cpp during HELLO handshake)
void wki_eth_neighbor_add(uint16_t node_id, const proto::MacAddress& mac);
void wki_eth_neighbor_remove(uint16_t node_id);
auto eth_neighbor_find_mac(uint16_t node_id, proto::MacAddress& mac_out) -> bool;

// Resolve the source MAC of a concrete Ethernet transport. Directed HELLOs
// use this so multi-NIC peers advertise the MAC reachable on that link.
auto wki_eth_transport_source_mac(const WkiTransport* transport, proto::MacAddress& mac_out) -> bool;

// Return the underlying NetDevice used by the WKI Ethernet transport.
// Used by wki_spin_yield() to drive inline NAPI polling.
auto wki_eth_get_netdev() -> net::NetDevice*;

// Returns true when this node recently failed to enqueue WKI Ethernet traffic.
// Heartbeat fencing treats this as an unsafe local observation window: if our
// own control/bulk frames may not be leaving the NIC, a missing heartbeat is not
// strong enough evidence to tear down a peer.
auto wki_eth_recent_tx_pressure(uint64_t now_us) -> bool;

// Make an injected transport failure participate in the same local-observation
// safety window as a concrete Ethernet enqueue failure.
void wki_eth_note_injected_tx_failure(WkiTransport* transport, const void* data, uint16_t len);

// Apply direct-peer ingress identity only after the chaos boundary elects to
// deliver a frame. This keeps injected drops from refreshing peer contact.
void wki_eth_note_rx_contact(WkiTransport* transport, const WkiHeader* header, const proto::MacAddress& src_mac);

}  // namespace ker::net::wki
