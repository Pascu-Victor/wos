#!/usr/bin/env python3
"""Run current TCP output, ACK-retirement/SACK, and timer bodies on the host.

The protocol bodies are copied verbatim from the current source at test time;
the real TcpCB, sender model and PacketBuffer layouts are included. Only clock,
packet pool, IP emission, socket wake/reference, and hash lifetime are seams.
This validates deterministic transitions and ownership, not NIC delivery.
"""

import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "modules/kern/src/net/proto"


def function(source, name):
    match = re.search(rf"^(?:auto|void|bool) {name}\(", source, re.M)
    assert match, name
    end = source.index("{", match.start()) + 1
    depth = 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[match.start():end]


def braced(source, marker):
    start = source.index(marker)
    end = source.index("{", start) + 1
    depth = 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[start:end]


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <new>
#include <unordered_set>
#include <utility>
#include <vector>
#include <net/endian.hpp>
#include <net/proto/tcp.hpp>
#include <platform/dbg/dbg.hpp>
namespace ker::mod::sched::task { struct Task {}; }
namespace ker::mod::sched { void kern_wake(task::Task*) {} }
namespace fixture {
using namespace ker::net;
using namespace ker::net::proto;
std::unordered_set<PacketBuffer*> pool;
std::vector<std::vector<uint8_t>> emissions;
int allocation_budget = -1, transmit_result = 0;
bool fail_metadata = false;
unsigned wakes = 0, socket_refs = 0, removed = 0;
std::function<void()> emission_hook;
TcpCB* raced = nullptr;
unsigned race_locks = 0;
enum class Race { None, Ack, Close, Generation, Window } race;
}
void* operator new(size_t size, const std::nothrow_t&) noexcept {
    assert(tcp_test_lock_depth == 0);
    if (fixture::fail_metadata) return nullptr;
    return ::operator new(size);
}
void operator delete(void* ptr, const std::nothrow_t&) noexcept { ::operator delete(ptr); }
namespace ker::net {
auto fixture_allocate_packet() -> PacketBuffer* {
    if (fixture::allocation_budget == 0) return nullptr;
    if (fixture::allocation_budget > 0) --fixture::allocation_budget;
    auto* pkt = new PacketBuffer{};
    pkt->data = pkt->storage.data() + PKT_HEADROOM;
    assert(fixture::pool.insert(pkt).second);
    return pkt;
}
auto pkt_alloc_tx() -> PacketBuffer* {
    assert(tcp_test_lock_depth == 0);
    return fixture_allocate_packet();
}
auto pkt_alloc() -> PacketBuffer* {
    // Production pkt_alloc uses only the existing packet pool (no heap grow).
    return fixture_allocate_packet();
}
void pkt_free(PacketBuffer* pkt) {
    if (pkt == nullptr) return;
    assert(fixture::pool.erase(pkt) == 1);
    delete pkt;
}
bool socket_try_acquire(Socket*) { ++fixture::socket_refs; return true; }
void socket_release(Socket*) { assert(fixture::socket_refs); --fixture::socket_refs; }
void socket_wake_waiters(Socket*) { assert(tcp_test_lock_depth == 0); ++fixture::wakes; }
}
namespace ker::net::proto {
volatile uint64_t tcp_ms_counter = 100;
auto tcp_now_ms() -> uint64_t { return tcp_ms_counter; }
void tcp_cb_acquire(TcpCB* cb) { assert(cb->refcnt.load() > 0); cb->refcnt.fetch_add(1); }
void tcp_cb_release(TcpCB* cb) { assert(cb->refcnt.fetch_sub(1) > 1); }
void tcp_free_cb(TcpCB*) { assert(tcp_test_lock_depth == 0); ++fixture::removed; }
auto tcp_transmit_prebuilt(PacketBuffer* pkt, const SocketEndpoint&, const SocketEndpoint&, uint32_t) -> int {
    assert(tcp_test_lock_depth == 0);
    if (fixture::emission_hook) {
        auto hook = std::move(fixture::emission_hook);
        fixture::emission_hook = {};
        hook();
    }
    fixture::emissions.emplace_back(pkt->data, pkt->data + pkt->len);
    pkt_free(pkt);
    return fixture::transmit_result;
}
constexpr size_t TCP_SYN_OPTIONS_LEN = 12;
constexpr size_t TCP_ACK_SACK_OPTIONS_LEN = 12;
'''


CASES = r'''
} // namespace ker::net::proto
using namespace ker::net;
using namespace ker::net::proto;
using namespace fixture;
void initialize(TcpCB& cb, uint32_t sequence = 1000) {
    assert(pool.empty() && tcp_test_lock_depth == 0);
    emissions.clear(); allocation_budget = -1; transmit_result = 0;
    fail_metadata = false; emission_hook = {}; tcp_ms_counter = 100;
    wakes = socket_refs = removed = 0;
    cb.state = TcpState::ESTABLISHED; cb.sender_enabled = true;
    cb.snd_una = cb.snd_nxt = sequence; cb.snd_wnd = 65535;
    cb.snd_mss = 100; cb.rcv_nxt = 9000; cb.rcv_wnd = 8192;
    cb.local = SocketEndpoint::ipv4(IPv4Address{0x0a1d0001}, 10000);
    cb.remote = SocketEndpoint::ipv4(IPv4Address{0x0a1d0002}, 10001);
    tcp_sender_reset(cb.sender, cb.snd_mss, sequence);
}
void finish(TcpCB& cb) {
    tcp_test_before_lock = nullptr;
    cb.lock.lock();
    clear_retransmits_locked(&cb);
    tcp_timer_disarm(&cb);
    cb.lock.unlock();
    assert(cb.refcnt.load() == 1 && !cb.on_timer_list);
    assert(pool.empty() && socket_refs == 0 && tcp_test_lock_depth == 0);
}
void send(TcpCB& cb, size_t length = 100) {
    std::array<uint8_t, 1000> bytes{};
    for (size_t i = 0; i < bytes.size(); ++i) bytes[i] = uint8_t(i);
    assert(tcp_send_segment(&cb, TCP_ACK | TCP_PSH, bytes.data(), length));
}
const TcpHeader& emitted(size_t index = 0) {
    return *reinterpret_cast<const TcpHeader*>(emissions.at(index).data());
}
void output_contract() {
    for (int family : {4, 6}) {
        TcpCB cb; initialize(cb);
        if (family == 6) {
            cb.local = SocketEndpoint::ipv6(IPv6Address::from_bytes({0x20,1,0xd,0xb8,0,0,0,0,0,0,0,0,0,0,0,1}), 10000);
            cb.remote = SocketEndpoint::ipv6(IPv6Address::from_bytes({0x20,1,0xd,0xb8,0,0,0,0,0,0,0,0,0,0,0,2}), 10001);
        }
        cb.sender.cwnd = 200;
        send(cb); send(cb);
        assert(cb.snd_nxt == 1200 && cb.retransmit_entries == 2 && pool.size() == 2);
        auto before = emissions.size();
        uint8_t byte = 42;
        assert(!tcp_send_segment(&cb, TCP_ACK, &byte, 1));
        assert(emissions.size() == before && cb.snd_nxt == 1200 && pool.size() == 2);
        cb.lock.lock(); retire_acked_retransmits_locked(&cb, 1200, 200, true); cb.snd_una = 1200; cb.lock.unlock();
        assert(cb.retransmit_entries == 0 && pool.empty() && cb.srtt_ms == 100);
        finish(cb);
    }
    for (int budget : {0, 1}) {
        TcpCB cb; initialize(cb); allocation_budget = budget;
        uint8_t byte = 42;
        assert(!tcp_send_segment(&cb, TCP_ACK, &byte, 1));
        assert(cb.snd_nxt == 1000 && cb.retransmit_entries == 0 && pool.empty());
        finish(cb);
    }
    TcpCB cb; initialize(cb); fail_metadata = true;
    uint8_t byte = 42;
    assert(!tcp_send_segment(&cb, TCP_ACK, &byte, 1));
    assert(cb.snd_nxt == 1000 && pool.empty());
    fail_metadata = false; transmit_result = -EIO;
    Socket sock; cb.socket = &sock;
    assert(!tcp_send_segment(&cb, TCP_ACK, &byte, 1));
    assert(cb.snd_nxt == 1000 && cb.retransmit_entries == 0 && pool.empty());
    assert(sock.pending_error.load() == EIO);
    finish(cb);
}
void concurrent_ack_after_output_failure() {
    TcpCB cb; initialize(cb); transmit_result = -EIO;
    emission_hook = [&] {
        cb.lock.lock(); cb.snd_una = cb.snd_nxt;
        retire_acked_retransmits_locked(&cb, cb.snd_una, 200, true);
        cb.lock.unlock();
    };
    uint8_t byte = 42;
    assert(tcp_send_segment(&cb, TCP_ACK, &byte, 1));
    assert(cb.snd_nxt == 1001 && cb.snd_una == 1001 && pool.empty());
    finish(cb);
}
void karn_partial_and_wrap() {
    TcpCB cb; initialize(cb, UINT32_MAX - 149);
    send(cb); send(cb);
    cb.retransmit_head->retries = 1;
    cb.lock.lock(); retire_acked_retransmits_locked(&cb, cb.snd_nxt, 500, true); cb.lock.unlock();
    assert(cb.srtt_ms == 0 && cb.rto_ms == TCP_RTO_INITIAL_MS && pool.empty());
    finish(cb);
    TcpCB partial; initialize(partial); send(partial);
    partial.lock.lock(); retire_acked_retransmits_locked(&partial, 1050, 200, true); partial.snd_una = 1050; partial.lock.unlock();
    assert(partial.retransmit_entries == 1 && partial.srtt_ms == 0);
    auto* spare = pkt_alloc_tx();
    partial.lock.lock();
    assert(tcp_build_retransmit_locked(&partial, partial.retransmit_head, 1050, 1100, spare));
    partial.lock.unlock();
    auto* header = reinterpret_cast<TcpHeader*>(spare->data);
    assert(ntohl(header->seq) == 1050 && spare->len == sizeof(TcpHeader) + 50);
    for (size_t i=0; i<50; ++i) assert(spare->data[sizeof(TcpHeader)+i] == uint8_t(50+i));
    assert(partial.snd_nxt == 1100 && partial.retransmit_head->retries == 1);
    pkt_free(spare); finish(partial);
}
void sack_parser() {
    TcpCB cb; initialize(cb); send(cb); send(cb); send(cb); cb.sack_permitted = true;
    alignas(TcpHeader) std::array<uint8_t, 32> bytes{};
    auto* header = reinterpret_cast<TcpHeader*>(bytes.data()); header->data_offset = 8 << 4;
    bytes[20] = TCP_OPTION_NOP; bytes[21] = TCP_OPTION_NOP; bytes[22] = TCP_OPTION_SACK; bytes[23] = 10;
    uint32_t left = htonl(1100), right = htonl(1300);
    std::memcpy(bytes.data()+24, &left, 4); std::memcpy(bytes.data()+28, &right, 4);
    cb.lock.lock(); parse_sender_sack_locked(&cb, header); cb.lock.unlock();
    assert(cb.sender.sack.count == 1 && cb.sender.sack.ranges[0].left == 1100);
    assert(cb.retransmit_entries == 3 && cb.snd_una == 1000);
    bytes[23] = 255;
    cb.lock.lock(); parse_sender_sack_locked(&cb, header); cb.lock.unlock();
    assert(cb.sender.sack.count == 1); finish(cb);
}
void timeout_contract() {
    TcpCB cb; initialize(cb); send(cb); send(cb); send(cb); emissions.clear();
    auto* entry = cb.retransmit_head;
    auto cwnd = cb.sender.cwnd;
    auto sent = entry->send_time_ms;
    auto deadline = cb.retransmit_deadline;
    allocation_budget = 0; tcp_timer_tick(deadline);
    assert(emissions.empty() && entry->retries == 0 && entry->rto_retries == 0);
    assert(entry->send_time_ms == sent && cb.rto_ms == 1000 && cb.sender.cwnd == cwnd);
    assert(cb.retransmit_deadline == deadline + 10);
    allocation_budget = -1; tcp_timer_tick(cb.retransmit_deadline);
    assert(emissions.size() == 1 && ntohl(emitted().seq) == 1000);
    assert(entry->retries == 1 && entry->rto_retries == 1 && cb.rto_ms == 2000);
    assert(cb.sender.cwnd == 100 && cb.snd_nxt == 1300 && cb.retransmit_entries == 3 && pool.size() == 3);
    finish(cb);
}
void race_hook(const void* lock) {
    if (lock != &raced->lock || ++race_locks != 2) return;
    tcp_test_before_lock = nullptr;
    if (race == Race::Ack) raced->snd_una = raced->snd_nxt;
    if (race == Race::Close) raced->state = TcpState::CLOSED;
    if (race == Race::Generation) ++raced->output_generation;
    if (race == Race::Window) { raced->snd_wnd = 4096; raced->persist_needed = false; }
}
void deferred_cancellation() {
    for (Race type : {Race::Ack, Race::Close, Race::Generation}) {
        TcpCB cb; initialize(cb, UINT32_MAX - 99); send(cb); assert(cb.snd_nxt == 0); emissions.clear();
        raced = &cb; race = type; race_locks = 0; tcp_test_before_lock = race_hook;
        tcp_timer_tick(cb.retransmit_deadline);
        assert(emissions.empty()); finish(cb);
    }
}
void persist_contract() {
    TcpCB cb; initialize(cb); send(cb); cb.snd_wnd = 0; emissions.clear();
    cb.lock.lock(); tcp_timer_arm(&cb); cb.lock.unlock();
    auto cwnd = cb.sender.cwnd;
    tcp_timer_tick(cb.persist_deadline);
    assert(emissions.size() == 1 && emissions[0].size() == sizeof(TcpHeader) + 1);
    assert(ntohl(emitted().seq) == cb.snd_una && cb.snd_nxt == 1100);
    assert(cb.persist_probes == 1 && cb.persist_interval == 2000 && cb.sender.cwnd == cwnd && cb.rto_ms == 1000);
    for (unsigned i=0; i<8; ++i) tcp_timer_tick(cb.persist_deadline);
    assert(cb.persist_interval == TCP_PERSIST_MAX_MS && cb.sender.timeouts == 0);
    emissions.clear(); raced = &cb; race=Race::Window; race_locks=0; tcp_test_before_lock=race_hook;
    tcp_timer_tick(cb.persist_deadline);
    assert(emissions.empty()); finish(cb);
}
void persist_first_byte_and_fin() {
    TcpCB cb; initialize(cb); cb.snd_wnd=0;
    uint8_t value=42;
    assert(!tcp_send_segment(&cb, TCP_ACK, &value, 1));
    assert(tcp_send_segment(&cb, TCP_ACK, &value, 1, true));
    assert(cb.snd_nxt==1001 && cb.retransmit_entries==1 && cb.persist_needed);
    assert(!tcp_send_segment(&cb, TCP_ACK, &value, 1, true));
    finish(cb);
    TcpCB fin; initialize(fin); send(fin);
    emission_hook = [&] { assert(fin.state==TcpState::FIN_WAIT_1); fin.state=TcpState::FIN_WAIT_2; };
    assert(tcp_send_segment(&fin, TCP_FIN|TCP_ACK, nullptr, 0));
    assert(fin.state==TcpState::FIN_WAIT_2 && fin.snd_nxt==1101 && fin.retransmit_entries==2);
    finish(fin);
    TcpCB failed; initialize(failed); transmit_result=-EIO;
    assert(!tcp_send_segment(&failed, TCP_FIN|TCP_ACK, nullptr, 0));
    assert(failed.state==TcpState::ESTABLISHED && failed.snd_nxt==1000 && failed.retransmit_entries==0);
    finish(failed);
}
void sack_hole_and_pmtu_timeout() {
    TcpCB cb; initialize(cb); send(cb); emissions.clear();
    assert(tcp_sack_add(cb.sender.sack,cb.snd_una,cb.snd_nxt,1025,1100));
    cb.fast_retransmit_pending=true;
    cb.lock.lock(); tcp_timer_arm(&cb); cb.lock.unlock();
    tcp_timer_tick(tcp_now_ms());
    assert(emissions.size()==1 && emissions[0].size()==sizeof(TcpHeader)+25);
    assert(ntohl(emitted().seq)==1000 && cb.snd_nxt==1100 && cb.retransmit_entries==1);
    finish(cb);
    TcpCB smaller; initialize(smaller); send(smaller); emissions.clear(); smaller.snd_mss=25;
    tcp_timer_tick(smaller.retransmit_deadline);
    assert(emissions.size()==1 && emissions[0].size()==sizeof(TcpHeader)+25 && smaller.snd_nxt==1100);
    finish(smaller);
    TcpCB gap; initialize(gap); Socket socket; gap.socket=&socket; send(gap); gap.snd_una=999;
    tcp_timer_tick(gap.retransmit_deadline);
    assert(gap.state==TcpState::CLOSED && socket.pending_error.load()==ETIMEDOUT && pool.empty());
    finish(gap);
}
void graceful_close_preserves_data() {
    for (TcpState state : {TcpState::ESTABLISHED,TcpState::CLOSE_WAIT}) {
        TcpCB cb; initialize(cb); Socket socket;
        socket.proto_data=&cb; cb.socket=&socket; cb.state=state;
        tcp_cb_acquire(&cb); // Additional owning Socket reference; fixture keeps its own.
        send(cb); send(cb);
        auto first=cb.retransmit_head;
        tcp_close_op(&socket);
        assert(cb.socket==nullptr && socket.proto_data==nullptr);
        assert(cb.retransmit_head==first && cb.retransmit_entries==3 && cb.snd_nxt==1201);
        assert(cb.state==(state==TcpState::ESTABLISHED ? TcpState::FIN_WAIT_1 : TcpState::LAST_ACK));
        assert(pool.size()==3);
        cb.lock.lock(); retire_acked_retransmits_locked(&cb,1201,200,true); cb.snd_una=1201; cb.lock.unlock();
        assert(pool.empty()); finish(cb);
    }
}
void terminal_timeout_wake() {
    TcpCB cb; initialize(cb); Socket sock; cb.socket=&sock; send(cb);
    cb.retransmit_head->rto_retries = 8;
    tcp_timer_tick(cb.retransmit_deadline);
    assert(cb.state == TcpState::CLOSED && sock.pending_error.load()==ETIMEDOUT);
    assert(wakes==1 && socket_refs==0 && cb.retransmit_entries==0 && pool.empty());
    finish(cb);
}
void recv_window_update_contract() {
    {
        TcpCB cb; initialize(cb); Socket sock; cb.socket=&sock;
        sock.rcvbuf.capacity=8192; cb.rcv_wnd=0;
        maybe_send_recv_window_update(&cb,&sock);
        assert(cb.rcv_wnd==8192 && emissions.size()==1);
        assert(ntohs(emitted().window)==8192 && ntohl(emitted().ack)==9000);
        assert(!cb.ack_pending && cb.refcnt.load()==1 && pool.empty());
        finish(cb);
    }
    {
        TcpCB cb; initialize(cb); Socket sock; cb.socket=&sock;
        sock.rcvbuf.capacity=8192; cb.rcv_wnd=0; allocation_budget=0;
        maybe_send_recv_window_update(&cb,&sock);
        assert(emissions.empty() && cb.ack_pending && cb.on_timer_list && pool.empty());
        allocation_budget=-1; tcp_timer_tick(cb.timer_deadline.load());
        assert(emissions.size()==1 && ntohs(emitted().window)==8192 && !cb.ack_pending);
        finish(cb);
    }
    {
        TcpCB cb; initialize(cb); Socket sock; cb.socket=&sock; sock.rcvbuf.capacity=8192;
        maybe_send_recv_window_update(&cb,&sock);
        assert(emissions.empty() && !cb.ack_pending && !cb.on_timer_list && pool.empty());
        assert(cb.refcnt.load()==1);
        finish(cb);
    }
    {
        TcpCB cb; initialize(cb); Socket sock; cb.socket=&sock;
        sock.rcvbuf.capacity=8192; cb.rcv_wnd=0; cb.state=TcpState::CLOSED;
        maybe_send_recv_window_update(&cb,&sock);
        assert(emissions.empty() && !cb.ack_pending && pool.empty() && cb.refcnt.load()==1);
        finish(cb);
    }
    for (Race type : {Race::Close,Race::Generation}) {
        TcpCB cb; initialize(cb); Socket sock; cb.socket=&sock;
        sock.rcvbuf.capacity=8192; cb.rcv_wnd=0;
        raced=&cb; race=type; race_locks=0; tcp_test_before_lock=race_hook;
        maybe_send_recv_window_update(&cb,&sock);
        assert(emissions.empty() && pool.empty() && cb.refcnt.load()==1);
        finish(cb);
    }
    {
        TcpCB cb; initialize(cb); Socket sock; cb.socket=&sock;
        sock.rcvbuf.capacity=8192; cb.rcv_wnd=0; transmit_result=-EIO;
        maybe_send_recv_window_update(&cb,&sock);
        assert(emissions.size()==1 && cb.ack_pending && cb.on_timer_list && pool.empty());
        finish(cb);
    }
}
void closing_ack_contract() {
    for (TcpState state : {TcpState::CLOSING,TcpState::LAST_ACK}) {
        TcpCB cb; initialize(cb); Socket sock; cb.socket=&sock;
        send(cb); send(cb); assert(tcp_send_segment(&cb,TCP_FIN|TCP_ACK,nullptr,0));
        cb.state=state;
        fixture_process_fin_ack(&cb,TCP_ACK,1100);
        assert(cb.snd_una==1100 && cb.retransmit_entries==2 && cb.retransmit_head->seq==1100);
        fixture_process_fin_ack(&cb,TCP_ACK,1200);
        assert(cb.snd_una==1200 && cb.retransmit_entries==1 && cb.retransmit_head->seq==1200);
        emissions.clear(); tcp_timer_tick(cb.retransmit_deadline);
        assert(emissions.size()==1 && ntohl(emitted().seq)==1200 && (emitted().flags&TCP_FIN));
        assert(emissions[0].size()==sizeof(TcpHeader));
        fixture_process_fin_ack(&cb,TCP_ACK,1201);
        assert(cb.state==(state==TcpState::CLOSING ? TcpState::TIME_WAIT : TcpState::CLOSED));
        assert(pool.empty() && cb.retransmit_entries==0 && cb.socket==&sock);
        finish(cb);
    }
    for (TcpState state : {TcpState::CLOSING,TcpState::LAST_ACK}) {
        TcpCB cb; initialize(cb); send(cb); cb.state=state;
        cb.fast_retransmit_pending=true; cb.persist_needed=true; cb.persist_deadline=1234;
        fixture_process_fin_ack(&cb,TCP_RST,1000);
        assert(cb.state==TcpState::CLOSED && cb.retransmit_entries==0 && !cb.fast_retransmit_pending);
        assert(!cb.persist_needed && cb.persist_deadline==0 && cb.retransmit_deadline==0 && pool.empty());
        finish(cb);
    }
    TcpCB cb; initialize(cb); Socket sock; cb.socket=&sock; send(cb); cb.state=TcpState::CLOSE_WAIT;
    emission_hook=[&] { assert(cb.state==TcpState::LAST_ACK); fixture_process_fin_ack(&cb,TCP_ACK,cb.snd_nxt); };
    assert(tcp_send_segment(&cb,TCP_FIN|TCP_ACK,nullptr,0));
    assert(cb.state==TcpState::CLOSED && cb.socket==&sock && pool.empty()); finish(cb);
}
void mss_path_cap_contract() {
    auto v4=SocketEndpoint::ipv4(IPv4Address::from_host_order(0x0a000001),1);
    auto v6=SocketEndpoint::ipv6(IPv6Address::from_bytes({0x20,1,0xd,0xb8,0,0,0,0,0,0,0,0,0,0,0,1}),1);
    assert(tcp_send_mss_for_path(v4,UINT16_MAX,1500)==1460);
    assert(tcp_send_mss_for_path(v6,UINT16_MAX,1500)==1440);
    assert(tcp_send_mss_for_path(v4,UINT16_MAX,UINT32_MAX)==10072);
    assert(tcp_send_mss_for_path(v6,UINT16_MAX,UINT32_MAX)==10052);
    assert(tcp_send_mss_for_path(v4,UINT16_MAX,1)==1 && tcp_send_mss_for_path(v6,0,1280)==1);
    assert(tcp_send_mss_for_path(v6,UINT16_MAX,1280)==1220);
}
void sack_duplicate_window_contract() {
    auto acknowledgement = [](std::array<uint8_t,32>& bytes, uint32_t ack, uint16_t window,
                              uint32_t left, uint32_t right, uint8_t option_length=10) -> TcpHeader* {
        bytes.fill(0);
        auto* hdr=reinterpret_cast<TcpHeader*>(bytes.data());
        hdr->flags=TCP_ACK; hdr->data_offset=8<<4;
        hdr->seq=htonl(9000); hdr->ack=htonl(ack); hdr->window=htons(window);
        bytes[20]=TCP_OPTION_NOP; bytes[21]=TCP_OPTION_NOP;
        bytes[22]=TCP_OPTION_SACK; bytes[23]=option_length;
        left=htonl(left); right=htonl(right);
        std::memcpy(bytes.data()+24,&left,4); std::memcpy(bytes.data()+28,&right,4);
        return hdr;
    };
    {
        TcpCB cb; initialize(cb); cb.sack_permitted=true; cb.snd_wnd=8192;
        cb.snd_wl1=9000; cb.snd_wl2=cb.snd_una;
        for(unsigned i=0;i<5;++i) send(cb);
        alignas(TcpHeader) std::array<uint8_t,32> bytes{};
        for(unsigned i=1;i<=3;++i) {
            auto* hdr=acknowledgement(bytes,1000,uint16_t(8192-i*100),1100,1100+i*100);
            fixture_process_stream_ack(&cb,hdr);
            assert(cb.sender.duplicate_acks==i && cb.snd_una==1000);
            assert(cb.snd_wnd==8192-i*100 && cb.sender.sack.count==1);
        }
        assert(cb.sender.in_recovery && cb.fast_retransmit_pending && cb.fast_retransmit_seq==1000);
        assert(cb.sender.fast_retransmits==1 && cb.sender.sack.accepted_blocks==3);
        emissions.clear(); tcp_timer_tick(tcp_now_ms());
        assert(emissions.size()==1 && ntohl(emitted().seq)==1000);
        assert(emissions[0].size()==sizeof(TcpHeader)+100);
        assert(cb.snd_nxt==1500 && cb.retransmit_entries==5 && cb.retransmit_packets==1);
        finish(cb);
    }
    {
        TcpCB cb; initialize(cb); cb.sack_permitted=true; cb.snd_wnd=8192;
        cb.snd_wl1=9000; cb.snd_wl2=cb.snd_una;
        for(unsigned i=0;i<5;++i) send(cb);
        alignas(TcpHeader) std::array<uint8_t,32> bytes{};
        fixture_process_stream_ack(&cb,acknowledgement(bytes,1000,8092,1100,1200));
        assert(cb.sender.duplicate_acks==1);
        // Repeated hints with a new window are pure window updates.
        fixture_process_stream_ack(&cb,acknowledgement(bytes,1000,7992,1100,1200));
        assert(cb.sender.duplicate_acks==0);
        for(uint8_t length : {uint8_t(255),uint8_t(9),uint8_t(0)}) {
            fixture_process_stream_ack(&cb,acknowledgement(bytes,1000,uint16_t(cb.snd_wnd-1),1200,1300,length));
            assert(cb.sender.duplicate_acks==0 && cb.sender.sack.count==1);
        }
        for(auto range : {TcpSackRange{1200,1501},TcpSackRange{1300,1200},TcpSackRange{1200,1200}}) {
            fixture_process_stream_ack(&cb,acknowledgement(bytes,1000,uint16_t(cb.snd_wnd-1),range.left,range.right));
            assert(cb.sender.duplicate_acks==0 && cb.sender.sack.count==1);
        }
        // An invalid cumulative ACK cannot admit otherwise valid new SACK.
        fixture_process_stream_ack(&cb,acknowledgement(bytes,1501,7000,1200,1300));
        assert(cb.sender.duplicate_acks==0 && cb.sender.sack.accepted_blocks==1);
        auto* window_only=acknowledgement(bytes,1000,6000,1200,1300);
        window_only->data_offset=5<<4;
        fixture_process_stream_ack(&cb,window_only);
        assert(cb.sender.duplicate_acks==0 && !cb.fast_retransmit_pending && !cb.sender.in_recovery);
        finish(cb);
    }
    for(bool enabled : {false,true}) {
        TcpCB cb; initialize(cb); cb.sender_enabled=enabled; cb.sack_permitted=!enabled; cb.snd_wnd=8192;
        cb.snd_wl1=9000; cb.snd_wl2=cb.snd_una; send(cb); send(cb); send(cb); send(cb);
        alignas(TcpHeader) std::array<uint8_t,32> bytes{};
        for(unsigned i=1;i<=3;++i) {
            fixture_process_stream_ack(&cb,acknowledgement(bytes,1000,uint16_t(8192-i*100),1100,1100+i*100));
        }
        assert(cb.sender.sack.count==0 && cb.sender.duplicate_acks==0 && !cb.fast_retransmit_pending);
        finish(cb);
    }
}
void receive_ack_generation_contract() {
    for (bool change_generation : {false,true}) {
        TcpCB cb; initialize(cb); cb.state=TcpState::CLOSING; cb.snd_nxt=cb.snd_una+1; transmit_result=-EIO;
        if (change_generation) emission_hook=[&] { ++cb.output_generation; cb.state=TcpState::SYN_SENT; };
        fixture_process_fin_ack(&cb,TCP_ACK|TCP_FIN,cb.snd_una);
        assert(emissions.size()==1 && pool.empty());
        assert(cb.ack_pending==!change_generation && cb.on_timer_list==!change_generation);
        finish(cb);
    }
    TcpCB cb; initialize(cb); cb.state=TcpState::CLOSING; cb.snd_nxt=cb.snd_una+1;
    raced=&cb; race=Race::Generation; race_locks=0; tcp_test_before_lock=race_hook;
    fixture_process_fin_ack(&cb,TCP_ACK|TCP_FIN,cb.snd_una);
    assert(emissions.empty() && pool.empty() && !cb.ack_pending && !cb.on_timer_list);
    finish(cb);
}
void handshake_timeout_epoch_contract() {
    for (int family : {4,6}) {
        TcpCB cb; initialize(cb); Socket sock; cb.socket=&sock; cb.state=TcpState::SYN_SENT;
        if (family==6) cb.remote=SocketEndpoint::ipv6(IPv6Address::from_bytes({0x20,1,0xd,0xb8,0,0,0,0,0,0,0,0,0,0,0,2}),10001);
        cb.rcv_mss=family==4 ? 1460 : 1220;
        // tcp_connect reserves the SYN sequence byte before output.
        ++cb.snd_nxt;
        assert(tcp_send_segment(&cb,TCP_SYN,nullptr,0));
        tcp_timer_tick(cb.retransmit_deadline); assert(cb.rto_backed_off);
        alignas(TcpHeader) std::array<uint8_t,24> bytes{};
        auto* hdr=reinterpret_cast<TcpHeader*>(bytes.data());
        hdr->data_offset=6<<4; hdr->flags=TCP_SYN|TCP_ACK; hdr->seq=htonl(9000); hdr->ack=htonl(cb.snd_nxt); hdr->window=htons(65535);
        bytes[20]=TCP_OPTION_MSS; bytes[21]=4; bytes[22]=255; bytes[23]=255;
        fixture_process_syn_ack(&cb,hdr,1500);
        assert(cb.state==TcpState::ESTABLISHED && sock.state==SocketState::CONNECTED);
        assert(cb.snd_mss==cb.rcv_mss && cb.sender.mss==cb.rcv_mss && cb.sender.cwnd==cb.sender.mss);
        assert(!cb.rto_backed_off && cb.retransmit_entries==0 && pool.empty() && socket_refs==0);
        cb.sender.cwnd=8*cb.sender.mss; cb.sender.ssthresh=UINT32_MAX;
        send(cb); send(cb); emissions.clear(); tcp_timer_tick(cb.retransmit_deadline);
        assert(emissions.size()==1 && cb.rto_backed_off && cb.sender.timeouts==1);
        assert(cb.sender.cwnd==cb.sender.mss && cb.sender.ssthresh==2*cb.sender.mss);
        finish(cb);
    }
}
void zero_window_ack_contract() {
    for (int family : {4,6}) {
        for (bool queued : {false,true}) {
            TcpCB cb; initialize(cb);
            if (family==6) {
                cb.local=SocketEndpoint::ipv6(IPv6Address::from_bytes({0x20,1,0xd,0xb8,0,0,0,0,0,0,0,0,0,0,0,1}),10000);
                cb.remote=SocketEndpoint::ipv6(IPv6Address::from_bytes({0x20,1,0xd,0xb8,0,0,0,0,0,0,0,0,0,0,0,2}),10001);
            }
            cb.snd_wl1=9000; cb.snd_wl2=cb.snd_una;
            if (queued) send(cb);
            auto sequence=cb.snd_nxt;
            TcpHeader hdr{}; hdr.flags=TCP_ACK; hdr.data_offset=5<<4;
            hdr.seq=htonl(9001); hdr.ack=htonl(sequence); hdr.window=0;
            fixture_process_stream_ack(&cb,&hdr);
            assert(cb.snd_una==sequence && cb.retransmit_entries==0 && pool.empty());
            assert(cb.snd_wnd==0 && cb.persist_needed && cb.persist_deadline!=0 && cb.on_timer_list);
            uint8_t byte=42;
            assert(!tcp_send_segment(&cb,TCP_ACK,&byte,1));
            assert(cb.persist_deadline!=0 && cb.snd_nxt==sequence && pool.empty());
            auto cwnd=cb.sender.cwnd;
            emissions.clear(); tcp_timer_tick(cb.persist_deadline);
            assert(emissions.size()==1 && emissions[0].size()==sizeof(TcpHeader));
            assert(ntohl(emitted().seq)==sequence-1 && cb.snd_nxt==sequence && cb.snd_una==sequence);
            assert(cb.persist_probes==1 && cb.sender.timeouts==0 && cb.sender.cwnd==cwnd);
            auto old_deadline=cb.persist_deadline;
            hdr.seq=htonl(9002); hdr.window=htons(4096);
            fixture_process_stream_ack(&cb,&hdr);
            assert(!cb.persist_needed && cb.persist_deadline==0);
            emissions.clear(); tcp_timer_tick(old_deadline);
            assert(emissions.empty() && cb.persist_probes==1);
            finish(cb);
        }
    }
    for (bool invalid_ack : {false,true}) {
        TcpCB cb; initialize(cb); cb.snd_wl1=9000; cb.snd_wl2=cb.snd_una;
        TcpHeader hdr{}; hdr.flags=TCP_ACK; hdr.data_offset=5<<4; hdr.window=0;
        hdr.seq=htonl(invalid_ack?9001:8999); hdr.ack=htonl(cb.snd_una+(invalid_ack?1:0));
        fixture_process_stream_ack(&cb,&hdr);
        assert(cb.snd_wnd==65535 && !cb.persist_needed && cb.persist_deadline==0 && !cb.on_timer_list);
        finish(cb);
    }
}
void handshake_zero_window_contract() {
    for (int family : {4,6}) {
        TcpCB cb; initialize(cb); Socket sock; cb.socket=&sock; cb.state=TcpState::SYN_SENT; cb.rcv_mss=1460;
        if (family==6) {
            cb.local=SocketEndpoint::ipv6(IPv6Address::from_bytes({0x20,1,0xd,0xb8,0,0,0,0,0,0,0,0,0,0,0,1}),10000);
            cb.remote=SocketEndpoint::ipv6(IPv6Address::from_bytes({0x20,1,0xd,0xb8,0,0,0,0,0,0,0,0,0,0,0,2}),10001);
        }
        ++cb.snd_nxt; assert(tcp_send_segment(&cb,TCP_SYN,nullptr,0));
        TcpHeader hdr{}; hdr.flags=TCP_SYN|TCP_ACK; hdr.data_offset=5<<4;
        hdr.seq=htonl(9000); hdr.ack=htonl(cb.snd_nxt); hdr.window=0;
        fixture_process_syn_ack(&cb,&hdr,1500);
        assert(cb.state==TcpState::ESTABLISHED && cb.snd_wnd==0 && cb.retransmit_entries==0);
        assert(cb.persist_needed && cb.persist_deadline!=0 && cb.on_timer_list && pool.empty());
        auto sequence=cb.snd_nxt; emissions.clear(); tcp_timer_tick(cb.persist_deadline);
        assert(emissions.size()==1 && ntohl(emitted().seq)==sequence-1 && cb.persist_probes==1);
        assert(cb.snd_nxt==sequence && cb.sender.timeouts==0);
        finish(cb);
    }
}
void half_close_probe_reply_contract() {
    for (auto state : {TcpState::ESTABLISHED,TcpState::CLOSE_WAIT,TcpState::FIN_WAIT_1,TcpState::FIN_WAIT_2}) {
        for (bool spare : {false,true}) {
            TcpCB cb; initialize(cb); cb.state=state; cb.rcv_wnd=4096;
            TcpHeader hdr{}; hdr.flags=TCP_ACK; hdr.data_offset=5<<4;
            hdr.seq=htonl(cb.rcv_nxt-1); hdr.ack=htonl(cb.snd_una); hdr.window=htons(4096);
            fixture_probe_reply(&cb,&hdr,spare);
            if (!spare) {
                assert(emissions.empty() && cb.ack_pending && cb.on_timer_list);
                tcp_timer_tick(cb.timer_deadline.load());
            }
            assert(emissions.size()==1 && (emitted().flags&TCP_ACK)!=0);
            assert(ntohl(emitted().ack)==cb.rcv_nxt && ntohs(emitted().window)==4096);
            assert(!cb.ack_pending && pool.empty() && cb.sender.timeouts==0);
            finish(cb);
        }
    }
    for (auto state : {TcpState::CLOSED,TcpState::LISTEN,TcpState::SYN_SENT,TcpState::SYN_RECEIVED,
                       TcpState::CLOSING,TcpState::LAST_ACK,TcpState::TIME_WAIT}) {
        TcpCB cb; initialize(cb); cb.state=state;
        TcpHeader hdr{}; hdr.flags=TCP_ACK; hdr.data_offset=5<<4; hdr.seq=htonl(cb.rcv_nxt-1);
        fixture_probe_reply(&cb,&hdr,true);
        assert(emissions.empty() && !cb.ack_pending && pool.empty()); finish(cb);
    }
    for (unsigned rejected : {0,1,2,3,4,5}) {
        TcpCB cb; initialize(cb);
        TcpHeader hdr{}; hdr.flags=TCP_ACK; hdr.data_offset=5<<4; hdr.seq=htonl(cb.rcv_nxt-1);
        size_t payload=0;
        if (rejected<3) hdr.flags|=rejected==0?TCP_SYN:(rejected==1?TCP_FIN:TCP_RST);
        if (rejected==3) payload=1;
        if (rejected==4) hdr.seq=htonl(cb.rcv_nxt);
        if (rejected==5) hdr.seq=htonl(cb.rcv_nxt+1);
        fixture_probe_reply(&cb,&hdr,true,payload);
        assert(emissions.empty() && !cb.ack_pending && pool.empty()); finish(cb);
    }
    TcpCB wrap; initialize(wrap); wrap.rcv_nxt=0;
    TcpHeader hdr{}; hdr.flags=TCP_ACK; hdr.data_offset=5<<4; hdr.seq=htonl(UINT32_MAX);
    fixture_probe_reply(&wrap,&hdr,true);
    assert(emissions.size()==1 && ntohl(emitted().ack)==0); finish(wrap);
}
int main() {
    recv_window_update_contract();
    output_contract(); concurrent_ack_after_output_failure(); karn_partial_and_wrap(); sack_parser();
    timeout_contract(); deferred_cancellation(); persist_contract(); terminal_timeout_wake();
    persist_first_byte_and_fin(); sack_hole_and_pmtu_timeout(); graceful_close_preserves_data();
    closing_ack_contract(); mss_path_cap_contract();
    sack_duplicate_window_contract();
    receive_ack_generation_contract();
    handshake_timeout_epoch_contract();
    zero_window_ack_contract(); handshake_zero_window_contract();
    half_close_probe_reply_contract();
    std::puts("TCP actual-source output/ACK/timer ownership and impairment contracts passed");
}
'''


def harness():
    output = (SOURCE / "tcp_output.cpp").read_text()
    input_source = (SOURCE / "tcp_input.cpp").read_text()
    timer = (SOURCE / "tcp_timer.cpp").read_text()
    core = (SOURCE / "tcp.cpp").read_text()
    code = PRELUDE
    start = output.index("struct TcpOutputRef {")
    end = output.index("\n};", start) + len("\n};")
    code += "\n" + output[start:end]
    for name in ("write_sack_option_locked", "tcp_fill_control_locked", "tcp_build_control_locked", "tcp_send_segment",
                 "tcp_build_ack", "tcp_send_ack", "tcp_build_keepalive_probe", "tcp_build_ack_locked", "tcp_build_keepalive_locked",
                 "tcp_build_retransmit_locked", "tcp_build_probe_locked"):
        code += "\n" + function(output, name)
    for name in ("tcp_header_len", "read_be32_unaligned", "update_rto_from_sample_locked", "retire_acked_retransmits_locked",
                 "parse_sender_sack_locked", "drain_retransmit_queue", "read_be16_unaligned", "parse_syn_options", "wake_socket"):
        code += "\n" + function(input_source, name)
    code += "\n} // namespace ker::net::proto\n"
    code += timer[timer.index("namespace ker::net::proto {"):timer.index("[[noreturn]] void tcp_timer_thread()")]
    code += r'''
struct FixtureBinding { TcpCB* cb=nullptr; SocketEndpoint local{}; };
std::array<FixtureBinding, MAX_TCP_BINDINGS> tcp_bindings{};
ker::mod::sys::Spinlock tcp_bind_lock;
template<class... T> void tcp_diag_log_local_close(T...) {}
void tcp_remove_listener(TcpCB*) {}
void tcp_drain_accept_queue(Socket*) {}
'''
    code += "\n" + function(core, "tcp_close_op")
    code += "\n" + function(core, "maybe_send_recv_window_update")
    code += "\n" + braced(input_source, "struct TcpInputPacketRef {") + ";\n"
    prefix = input_source[input_source.index("    PacketBuffer* deferred_ack = nullptr;", input_source.index("void tcp_process_segment(")):
                          input_source.index("    cb_lock_flags = cb->lock.lock_irqsave();", input_source.index("void tcp_process_segment("))]
    code += r'''
void fixture_process_stream_ack(TcpCB* cb, const TcpHeader* hdr, size_t payload_len=0) {
    tcp_cb_acquire(cb);
    uint8_t flags=hdr->flags;
    uint32_t const SEG_SEQ=ntohl(hdr->seq);
    uint32_t seg_ack=ntohl(hdr->ack);
    uint16_t seg_wnd=ntohs(hdr->window);
    TcpInputPacketRef ack_spare{};
'''
    code += prefix
    code += "\ncb_lock_flags=cb->lock.lock_irqsave();\nprocess_stream_ack();\ncb->lock.unlock_irqrestore(cb_lock_flags);\n"
    code += "tcp_cb_release(cb);\n}\n"
    code += r'''
void fixture_probe_reply(TcpCB* cb, const TcpHeader* hdr, bool spare, size_t payload_len=0) {
    tcp_cb_acquire(cb);
    uint8_t flags=hdr->flags;
    uint32_t const SEG_SEQ=ntohl(hdr->seq);
    uint32_t seg_ack=ntohl(hdr->ack);
    uint16_t seg_wnd=ntohs(hdr->window);
    TcpInputPacketRef ack_spare{spare?pkt_alloc():nullptr};
'''
    code += prefix
    code += "\ncb_lock_flags=cb->lock.lock_irqsave();\n"
    code += input_source[input_source.index("    bool const PROBE_REPLY_STATE ="):
                         input_source.index("    Socket* socket_to_wake = deferred_wake", input_source.index("    bool const PROBE_REPLY_STATE ="))]
    code += "tcp_timer_arm(cb);\ncb->lock.unlock_irqrestore(cb_lock_flags);\n"
    code += "if(deferred_ack) emit_deferred_ack();\nelse if(defer_ack_pending) retry_deferred_ack();\ntcp_cb_release(cb);\n}\n"
    code += r'''
void fixture_process_fin_ack(TcpCB* cb, uint8_t flags, uint32_t acknowledgement) {
    tcp_cb_acquire(cb);
    TcpHeader header{};
    header.flags=flags; header.data_offset=5<<4;
    const TcpHeader* hdr=&header;
    uint32_t const SEG_SEQ=cb->snd_wl1+1;
    uint32_t seg_ack=acknowledgement;
    uint16_t seg_wnd=4096;
    size_t payload_len=0;
    TcpInputPacketRef ack_spare{(flags&TCP_FIN)!=0 ? pkt_alloc() : nullptr};
'''
    code += prefix
    code += "\ncb_lock_flags=cb->lock.lock_irqsave();\nswitch(cb->state) {\n"
    code += braced(input_source, "case TcpState::CLOSING: {")
    code += braced(input_source, "case TcpState::LAST_ACK: {")
    code += "\ndefault: assert(false); }\ncb->lock.unlock_irqrestore(cb_lock_flags);\n"
    code += "if(deferred_ack) emit_deferred_ack();\nif(defer_ack_pending) retry_deferred_ack();\ntcp_cb_release(cb);\n}\n"
    code += r'''
void fixture_process_syn_ack(TcpCB* cb, const TcpHeader* hdr, uint32_t ingress_mtu) {
    tcp_cb_acquire(cb);
    uint8_t flags=hdr->flags;
    uint32_t const SEG_SEQ=ntohl(hdr->seq);
    uint32_t seg_ack=ntohl(hdr->ack);
    uint16_t seg_wnd=ntohs(hdr->window);
    size_t payload_len=0;
    TcpInputPacketRef ack_spare{pkt_alloc()};
'''
    code += prefix
    code += "\ncb_lock_flags=cb->lock.lock_irqsave();\nswitch(cb->state) {\n"
    code += braced(input_source,"case TcpState::SYN_SENT: {")
    code += "\ndefault: assert(false); }\ncb->lock.unlock_irqrestore(cb_lock_flags);\ntcp_cb_release(cb);\n}\n"
    return code + CASES


def main():
    compiler = shutil.which("/usr/bin/clang++") or shutil.which("clang++")
    assert compiler, "native Clang is required"
    # Workspace temporary storage also works when /tmp has a per-user quota.
    with tempfile.TemporaryDirectory(prefix="tcp-integration-", dir=ROOT / "build") as temporary:
        directory = Path(temporary)
        source = directory / "harness.cpp"
        source.write_text(harness())
        executable = directory / "harness"
        env = dict(os.environ, TMPDIR=str(directory), ASAN_OPTIONS="detect_leaks=0")
        command = [compiler, "-std=c++23", "-g", "-O1", "-fsanitize=address,undefined", "-fno-sanitize=vptr",
                   "-I" + str(Path(__file__).with_name("tcp_integration_shim")), "-I" + str(ROOT / "tests/host/kern_shim"),
                   "-I" + str(ROOT / "modules/kern/src"), "-I" + str(ROOT / "modules"), str(source), "-o", str(executable)]
        subprocess.run(command, env=env, check=True, timeout=60)
        subprocess.run([str(executable)], env=env, check=True, timeout=20)


if __name__ == "__main__":
    main()
