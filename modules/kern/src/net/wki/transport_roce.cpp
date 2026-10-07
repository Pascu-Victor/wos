#include "transport_roce.hpp"

#include <array>
#include <cstdint>
#include <cstring>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/proto/ethernet.hpp>
#include <net/wki/chaos.hpp>
#include <net/wki/rdma_protocol.hpp>
#include <net/wki/secure_rdma.hpp>
#include <net/wki/transport_eth.hpp>
#include <net/wki/wki.hpp>

namespace ker::net::wki {

// Legacy raw-memory operations have no registry, allocation or wait path.
// Existing callers remain link-compatible but can never create authority.
void wki_roce_transport_init() {}
auto wki_roce_transport_get() -> WkiTransport* { return nullptr; }
auto wki_roce_region_reset_received(uint32_t /*rkey*/) -> bool { return false; }
auto wki_roce_region_prepare_tagged_write(uint32_t /*rkey*/, uint16_t /*cookie*/) -> bool { return false; }
auto wki_roce_region_tagged_write_complete(uint32_t /*rkey*/, uint16_t /*cookie*/, uint32_t /*len*/) -> bool { return false; }
void wki_roce_region_finish_tagged_write(uint32_t /*rkey*/, uint16_t /*cookie*/) {}
auto wki_roce_region_wait_received(uint32_t /*rkey*/, uint32_t /*len*/, uint64_t /*timeout_us*/) -> bool { return false; }
auto wki_roce_region_wait_tagged_write(uint32_t /*rkey*/, uint16_t /*cookie*/, uint32_t /*len*/, uint64_t /*timeout_us*/) -> bool {
    return false;
}
auto wki_roce_rdma_write_tagged(uint16_t /*neighbor_id*/, uint32_t /*rkey*/, uint64_t /*remote_offset*/, const void* /*local_buf*/,
                                uint32_t /*len*/, uint16_t /*cookie*/) -> int {
    return -1;
}

void roce_rx(net::NetDevice* dev, net::PacketBuffer* pkt) {
    if (pkt == nullptr) return;
    // Unknown/raw ingress never creates an overlay or a secondary NIC provider.
    auto* transport = wki_eth_transport_find(dev);
    if (transport != nullptr && transport->secure_kind == rdma_protocol::Transport::ROCE && transport->secure_tx != nullptr &&
        dev->mtu >= rdma_protocol::RECORD_BYTES + proto::ETH_HLEN && pkt->data != nullptr && pkt->len >= rdma_protocol::HEADER_BYTES &&
        pkt->len <= rdma_protocol::RECORD_BYTES) {
        uint32_t magic = 0;
        std::memcpy(&magic, pkt->data, sizeof(magic));
        if (magic == rdma_protocol::MAGIC) {
            WkiRxMetadata const METADATA{.has_src_mac = true, .src_mac = pkt->src_mac};
            wki_chaos_secure_rx_ingress(transport, pkt->data, static_cast<uint16_t>(pkt->len), &METADATA);
        }
    }
    // Common ingress copied the bounded record before returning, even on reject.
    net::pkt_free(pkt);
}

#ifdef WOS_SELFTEST
namespace {
auto roce_rdma_register_region(WkiTransport* /*self*/, uint64_t /*phys_addr*/, uint32_t /*size*/, uint32_t* /*rkey*/) -> int { return -1; }
auto roce_rdma_unregister_region(WkiTransport* /*self*/, uint32_t /*rkey*/, uint32_t /*size*/) -> int { return -1; }
}  // namespace

auto wki_roce_selftest_uninitialized_receive_preserves_region() -> bool {
    std::array<uint8_t, 4> const ORIGINAL{1, 2, 3, 4};
    auto backing = ORIGINAL;
    uint32_t rkey = UINT32_MAX;
    // Registration cannot borrow backing or overwrite the caller's result.
    bool valid = roce_rdma_register_region(nullptr, reinterpret_cast<uint64_t>(backing.data()), backing.size(), &rkey) == -1 &&
                 rkey == UINT32_MAX && roce_rdma_unregister_region(nullptr, rkey, backing.size()) == -1;
    unsigned releases = 0;
    // Four former raw opcodes, including forged doorbell/read, are plain rejects.
    for (uint8_t opcode : {uint8_t{1}, uint8_t{2}, uint8_t{3}, uint8_t{4}}) {
        auto* pkt = net::pkt_alloc();
        if (pkt == nullptr) return false;
        std::array<uint8_t, 28> raw{};
        raw[0] = opcode;
        raw[1] = 1;
        std::memcpy(pkt->data, raw.data(), raw.size());
        pkt->len = raw.size();
        pkt->lifetime_ctx = &releases;
        pkt->lifetime_release = [](void* context) { ++*static_cast<unsigned*>(context); };
        roce_rx(nullptr, pkt);
    }
    valid = valid && releases == 4 && backing == ORIGINAL && wki_roce_transport_get() == nullptr && !wki_roce_region_reset_received(rkey) &&
            !wki_roce_region_prepare_tagged_write(rkey, 1) && !wki_roce_region_tagged_write_complete(rkey, 1, 0) &&
            !wki_roce_region_wait_received(rkey, 0, UINT64_MAX) && !wki_roce_region_wait_tagged_write(rkey, 1, 0, UINT64_MAX) &&
            wki_roce_rdma_write_tagged(2, rkey, 0, backing.data(), backing.size(), 1) == -1;
    wki_roce_region_finish_tagged_write(rkey, 1);
    wki_roce_transport_init();
    return valid && backing == ORIGINAL && wki_roce_transport_get() == nullptr;
}
#endif

}  // namespace ker::net::wki
