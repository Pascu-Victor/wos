#!/usr/bin/env python3
"""Audit permanent raw RoCE closure and concrete secure ingress boundaries."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ROCE = ROOT / "modules/kern/src/net/wki/transport_roce.cpp"
ETH = ROOT / "modules/kern/src/net/wki/transport_eth.cpp"


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:int|void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
    if match is None:
        raise AssertionError(f"missing function {name}")
    depth, pos = 1, match.end()
    while pos < len(source) and depth:
        depth += (source[pos] == "{") - (source[pos] == "}")
        pos += 1
    assert depth == 0
    return source[match.end():pos - 1]


def ordered(body: str, *tokens: str) -> None:
    pos = 0
    for token in tokens:
        found = body.find(token, pos)
        assert found >= 0, f"missing ordered token {token}"
        pos = found + len(token)


def main() -> None:
    roce, eth = ROCE.read_text(), ETH.read_text()
    rejects = {
        "wki_roce_transport_init": "", "wki_roce_transport_get": "return nullptr;",
        "wki_roce_region_reset_received": "return false;",
        "wki_roce_region_prepare_tagged_write": "return false;",
        "wki_roce_region_tagged_write_complete": "return false;",
        "wki_roce_region_finish_tagged_write": "",
        "wki_roce_region_wait_received": "return false;",
        "wki_roce_region_wait_tagged_write": "return false;",
        "wki_roce_rdma_write_tagged": "return -1;",
        "roce_rdma_register_region": "return -1;",
        "roce_rdma_unregister_region": "return -1;",
    }
    for name, expected in rejects.items():
        assert function_body(roce, name).strip() == expected, f"{name} raw authority/effects remain"
    for forbidden in ("s_region_buckets", "s_roce_transport", "region_write(", "region_copy_read_chunk", "wki_zone_find(",
                      "wki_ipc_doorbell_rx(", "while (", "new ", "wki_transport_register("):
        assert forbidden not in roce, f"legacy RoCE effect remains: {forbidden}"
    rx = function_body(roce, "roce_rx")
    ordered(rx, "if (pkt == nullptr) return;", "wki_eth_transport_find(dev)",
            "transport->secure_kind == rdma_protocol::Transport::ROCE", "dev->mtu >= rdma_protocol::RECORD_BYTES + proto::ETH_HLEN",
            "pkt->len >= rdma_protocol::HEADER_BYTES", "pkt->len <= rdma_protocol::RECORD_BYTES",
            "std::memcpy(&magic", "magic == rdma_protocol::MAGIC", "WkiRxMetadata const METADATA",
            "wki_chaos_secure_rx_ingress(", "net::pkt_free(pkt)")
    assert rx.count("pkt_free(") == 1 and "get_or_create" not in rx
    lookup = function_body(eth, "wki_eth_transport_find")
    ordered(lookup, "g_wki.transport_lock.lock()", "visited < WKI_MAX_TRANSPORTS", "transport->tx != eth_wki_tx",
            "transport->registration_generation == 0", "transport->chaos_unregistering.load", "priv->netdev == netdev",
            "g_wki.transport_lock.unlock()")
    assert "register(" not in lookup and "get_or_create" not in lookup
    tx = function_body(eth, "eth_secure_tx")
    ordered(tx, "!eth_secure_record(data, len)", "!eth_secure_capable(priv->netdev)",
            "wki_eth_transport_find(priv->netdev) != self", "peer->state != PeerState::CONNECTED", "!peer->auth_session.active",
            "peer->transport != self", "peer->mac.is_multicast()", "PacketBuffer* pkt = pkt_alloc_tx()",
            "std::memcpy(pkt->data, data, len)", "pkt->dev = priv->netdev", "proto::eth_tx(priv->netdev, pkt, DST_MAC, WKI_ETHERTYPE_ROCE)")
    assert "pkt_alloc()" not in tx and "wki_eth_get_netdev()" not in tx and "eth_neighbor_find_mac" not in tx
    assert "rdma_capable = true" not in eth
    print("WKI secure adapter/legacy RoCE closure source invariants hold")


if __name__ == "__main__":
    main()
