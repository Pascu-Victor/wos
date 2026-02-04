#pragma once

#include <cstdint>
#include <net/netdevice.hpp>
#include <net/packet.hpp>

namespace ker::net::wki {

// Initialize the Ethernet WKI transport on the given NIC.
// Called from main.cpp during boot after NIC drivers are probed.
void wki_eth_transport_init(net::NetDevice* netdev);

// RX entry point — called from ethernet.cpp's eth_rx() dispatch
void wki_eth_rx(net::NetDevice* dev, net::PacketBuffer* pkt);

// Neighbor MAC table management (used by peer.cpp during HELLO handshake)
void wki_eth_neighbor_add(uint16_t node_id, const std::array<uint8_t, 6>& mac);
void wki_eth_neighbor_remove(uint16_t node_id);
auto eth_neighbor_find_mac(uint16_t node_id, std::array<uint8_t, 6>& mac_out) -> bool;

}  // namespace ker::net::wki
