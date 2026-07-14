#include <cstdint>
#include <limits>
#include <net/wki/channel.hpp>
#include <net/wki/wki.hpp>
#include <test/ktest.hpp>

KTEST(WkiChannel, ResetClearsPostFenceReliabilityState) {
    ker::net::wki::WkiChannel ch{};
    ch.channel_id = ker::net::wki::WKI_CHAN_IPC_DATA;
    ch.peer_node_id = 0x1234;
    ch.priority = ker::net::wki::PriorityClass::THROUGHPUT;
    ch.active = true;
    ch.generation = 7;
    ch.tx_seq = 41;
    ch.tx_ack = 17;
    ch.rx_seq = 99;
    ch.rx_dispatch_seq = 97;
    ch.rx_ack_pending = 98;
    ch.ack_pending = true;
    ch.ack_pending_since_us = std::numeric_limits<uint64_t>::max();
    ch.tx_credits = 1;
    ch.rx_credits = 2;
    ch.rto_us = ker::net::wki::WKI_MAX_RTO_US;
    ch.srtt_us = 123;
    ch.rttvar_us = 456;
    ch.retransmit_deadline = std::numeric_limits<uint64_t>::max();
    ch.last_dup_ack = 96;
    ch.dup_ack_count = ker::net::wki::WKI_FAST_RETRANSMIT_THRESH;
    ch.bytes_sent = 111;
    ch.bytes_received = 222;
    ch.retransmits = 3;
    ch.perf_last_stall_report_us = 444;
    ch.perf_last_stall_status = 555;

    auto* rt = ker::net::wki::wki_retransmit_entry_alloc(4);
    KREQUIRE_NE(rt, nullptr);
    rt->len = 4;
    rt->seq = 41;
    ch.retransmit_head = rt;
    ch.retransmit_tail = rt;
    ch.retransmit_count = 1;

    auto* ro = new ker::net::wki::WkiReorderEntry{};
    ro->data = new uint8_t[2]{};
    ro->len = 2;
    ro->seq = 100;
    ch.reorder_head = ro;
    ch.reorder_count = 1;

    ker::net::wki::wki_channel_reset(&ch);

    KEXPECT_TRUE(ch.active);
    KEXPECT_EQ(ch.channel_id, ker::net::wki::WKI_CHAN_IPC_DATA);
    KEXPECT_EQ(ch.peer_node_id, 0x1234);
    KEXPECT_EQ(ch.priority, ker::net::wki::PriorityClass::THROUGHPUT);
    KEXPECT_EQ(ch.generation, 7U);
    KEXPECT_EQ(ch.tx_seq, 0U);
    KEXPECT_EQ(ch.tx_ack, 0U);
    KEXPECT_EQ(ch.rx_seq, 0U);
    KEXPECT_EQ(ch.rx_dispatch_seq, 0U);
    KEXPECT_EQ(ch.rx_ack_pending, ker::net::wki::WKI_ACK_NONE);
    KEXPECT_FALSE(ch.ack_pending);
    KEXPECT_EQ(ch.ack_pending_since_us, 0U);
    KEXPECT_EQ(ch.tx_credits, ker::net::wki::WKI_CREDITS_IPC_DATA);
    KEXPECT_EQ(ch.rx_credits, ker::net::wki::WKI_CREDITS_IPC_DATA);
    KEXPECT_EQ(ch.retransmit_head, nullptr);
    KEXPECT_EQ(ch.retransmit_tail, nullptr);
    KEXPECT_EQ(ch.retransmit_count, 0U);
    KEXPECT_EQ(ch.reorder_head, nullptr);
    KEXPECT_EQ(ch.reorder_count, 0U);
    KEXPECT_EQ(ch.last_dup_ack, 0U);
    KEXPECT_EQ(ch.dup_ack_count, 0U);
    KEXPECT_EQ(ch.rto_us, ker::net::wki::WKI_INITIAL_RTO_US);
    KEXPECT_EQ(ch.srtt_us, 0U);
    KEXPECT_EQ(ch.rttvar_us, 0U);
    KEXPECT_EQ(ch.retransmit_deadline, 0U);
    KEXPECT_EQ(ch.bytes_sent, 0U);
    KEXPECT_EQ(ch.bytes_received, 0U);
    KEXPECT_EQ(ch.retransmits, 0U);
    KEXPECT_EQ(ch.perf_last_stall_report_us, 0U);
    KEXPECT_EQ(ch.perf_last_stall_status, 0U);
    KEXPECT_FALSE(ch.tx_rt_entry_in_use);
}

KTEST(WkiChannel, ResetKeepsInlineRetransmitStorageOwnedByChannel) {
    ker::net::wki::WkiChannel ch{};
    ch.channel_id = ker::net::wki::WKI_CHAN_CONTROL;
    ch.peer_node_id = 0x5678;
    ch.active = true;
    ch.tx_rt_entry_in_use = true;
    ch.tx_rt_entry.data = ch.tx_rt_buf.data();
    ch.tx_rt_entry.len = 16;
    ch.tx_rt_entry.seq = 7;
    ch.retransmit_head = &ch.tx_rt_entry;
    ch.retransmit_tail = &ch.tx_rt_entry;
    ch.retransmit_count = 1;

    ker::net::wki::wki_channel_reset(&ch);

    KEXPECT_TRUE(ch.active);
    KEXPECT_EQ(ch.channel_id, ker::net::wki::WKI_CHAN_CONTROL);
    KEXPECT_EQ(ch.peer_node_id, 0x5678);
    KEXPECT_EQ(ch.retransmit_head, nullptr);
    KEXPECT_EQ(ch.retransmit_tail, nullptr);
    KEXPECT_EQ(ch.retransmit_count, 0U);
    KEXPECT_FALSE(ch.tx_rt_entry_in_use);
    KEXPECT_EQ(ch.tx_rt_entry.data, nullptr);
}

KTEST(WkiChannel, CloseRetiresChannelAndClearsQueuedState) {
    ker::net::wki::WkiChannel ch{};
    ch.channel_id = ker::net::wki::WKI_CHAN_RESOURCE;
    ch.peer_node_id = 0x2468;
    ch.active = true;
    ch.tx_seq = 12;
    ch.tx_ack = 3;
    ch.rx_seq = 9;
    ch.rx_ack_pending = 8;
    ch.ack_pending = true;
    ch.ack_pending_since_us = std::numeric_limits<uint64_t>::max();
    ch.tx_credits = 1;
    ch.rx_credits = 2;
    ch.retransmit_deadline = std::numeric_limits<uint64_t>::max();

    auto* rt = ker::net::wki::wki_retransmit_entry_alloc(4);
    KREQUIRE_NE(rt, nullptr);
    rt->len = 4;
    rt->seq = 12;
    ch.retransmit_head = rt;
    ch.retransmit_tail = rt;
    ch.retransmit_count = 1;

    auto* ro = new ker::net::wki::WkiReorderEntry{};
    ro->data = new uint8_t[8]{};
    ro->len = 8;
    ro->seq = 10;
    ch.reorder_head = ro;
    ch.reorder_count = 1;

    ker::net::wki::wki_channel_close(&ch);

    KEXPECT_FALSE(ch.active);
    KEXPECT_EQ(ch.peer_node_id, 0x2468);
    KEXPECT_EQ(ch.channel_id, ker::net::wki::WKI_CHAN_RESOURCE);
    KEXPECT_EQ(ch.tx_seq, 0U);
    KEXPECT_EQ(ch.tx_ack, 0U);
    KEXPECT_EQ(ch.rx_seq, 0U);
    KEXPECT_EQ(ch.rx_ack_pending, ker::net::wki::WKI_ACK_NONE);
    KEXPECT_FALSE(ch.ack_pending);
    KEXPECT_EQ(ch.ack_pending_since_us, 0U);
    KEXPECT_EQ(ch.tx_credits, ker::net::wki::WKI_CREDITS_RESOURCE);
    KEXPECT_EQ(ch.rx_credits, ker::net::wki::WKI_CREDITS_RESOURCE);
    KEXPECT_EQ(ch.retransmit_head, nullptr);
    KEXPECT_EQ(ch.retransmit_tail, nullptr);
    KEXPECT_EQ(ch.retransmit_count, 0U);
    KEXPECT_EQ(ch.retransmit_deadline, 0U);
    KEXPECT_EQ(ch.reorder_head, nullptr);
    KEXPECT_EQ(ch.reorder_count, 0U);
}

KTEST(WkiChannel, AckNextMustNotAdvancePastTransmittedSeq) {
    ker::net::wki::WkiChannel ch{};
    ch.tx_seq = 1;
    ch.tx_ack = 0;

    KEXPECT_EQ(ch.rx_ack_pending, ker::net::wki::WKI_ACK_NONE);
    KEXPECT_EQ(ker::net::wki::WKI_ACK_NONE + 1U, 0U);
    KEXPECT_TRUE(ker::net::wki::wki_channel_ack_next_within_sent_window(&ch, 0));
    KEXPECT_TRUE(ker::net::wki::wki_channel_ack_next_within_sent_window(&ch, 1));
    KEXPECT_FALSE(ker::net::wki::wki_channel_ack_next_within_sent_window(&ch, 2));
    KEXPECT_FALSE(ker::net::wki::wki_channel_ack_next_within_sent_window(nullptr, 1));
}

KTEST(WkiChannel, InlineRetransmitStorageRequiresCapacityAndIdleSlot) {
    ker::net::wki::WkiChannel ch{};

    KEXPECT_TRUE(ker::net::wki::wki_channel_has_inline_retransmit_storage(&ch, ker::net::wki::WkiChannel::WKI_RT_INLINE_SIZE));
    KEXPECT_FALSE(ker::net::wki::wki_channel_has_inline_retransmit_storage(&ch, ker::net::wki::WkiChannel::WKI_RT_INLINE_SIZE + 1));

    ch.tx_rt_entry_in_use = true;
    KEXPECT_FALSE(ker::net::wki::wki_channel_has_inline_retransmit_storage(&ch, 1));
    KEXPECT_FALSE(ker::net::wki::wki_channel_has_inline_retransmit_storage(nullptr, 1));
}

KTEST(WkiChannel, HeapRetransmitEntryOwnsContiguousExactFrameStorage) {
    auto* entry = ker::net::wki::wki_retransmit_entry_alloc(ker::net::wki::WKI_MAX_FRAME_SIZE);
    KREQUIRE_NE(entry, nullptr);
    KEXPECT_EQ(entry->data, reinterpret_cast<uint8_t*>(entry + 1));

    entry->data[0] = 0x12;
    entry->data[ker::net::wki::WKI_MAX_FRAME_SIZE - 1] = 0x34;
    KEXPECT_EQ(entry->data[0], 0x12);
    KEXPECT_EQ(entry->data[ker::net::wki::WKI_MAX_FRAME_SIZE - 1], 0x34);

    ker::net::wki::wki_retransmit_entry_release(nullptr, entry);
    KEXPECT_EQ(ker::net::wki::wki_retransmit_entry_alloc(ker::net::wki::WKI_MAX_FRAME_SIZE + 1), nullptr);
}
