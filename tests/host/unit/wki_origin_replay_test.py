#!/usr/bin/env python3
"""Compile real origin replay, encoding and admission bodies with host doubles.

Locks, peer storage, scheduling and reliable TX are deterministic doubles. This
checks worker claims and lifecycle interleavings, not kernel concurrency or MACs.
--source/--header permit the saved pre-patch version to demonstrate the missing
directed delivery (its absent worker is a no-op; proposed fields are inert).
"""

import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def function(source: str, name: str) -> str:
    match = re.search(rf"^(?:void|auto) {name}\([^;]*?\{{", source, re.M)
    assert match, f"missing {name}"
    pos, depth = match.end(), 1
    while depth:
        depth += (source[pos] == "{") - (source[pos] == "}")
        pos += 1
    return source[match.start():pos]


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <span>
#include <vector>
#include <net/wki/routing_lease.hpp>
using namespace ker::net::wki;
constexpr size_t WKI_MAX_PEERS = 256, WKI_MAX_NEIGHBORS_PER_LSA = 32;
constexpr size_t ORIGIN_REPLAY_BUDGET = 8;
constexpr uint16_t WKI_NODE_INVALID = 0, WKI_CHAN_CONTROL = 0;
constexpr uint16_t WKI_CAP_ROUTING_LEASE = 0x80;
constexpr size_t WKI_AUTH_NONCE_SIZE = 32;
constexpr int WKI_OK = 0, WKI_ERR_NO_ROUTE = -2, WKI_ERR_PEER_FENCED = -3;
enum class MsgType { LSA };
enum class PeerState { CONNECTED, FENCED };
struct Lock {
    bool held = false;
    void lock() { assert(!held); held = true; }
    void unlock() { assert(held); held = false; }
} s_routing_lock;
struct Peer {
    uint16_t node_id = 0, capabilities = 0;
    PeerState state = PeerState::CONNECTED;
    struct { Lock replay_lock; bool active = true; uint32_t generation = 2; } auth_session;
    uint32_t remote_boot_epoch = 10;
    std::array<uint8_t,32> remote_auth_nonce{};
    bool busy = false;
    unsigned readers = 0;
};
struct OriginPeer {
    uint16_t node = 0, capabilities = 0;
    uint32_t generation = 0;
    routing_lease::Identity identity{};
};
struct { uint16_t my_node_id = 1, capabilities = 0; } g_wki;
std::array<Peer,WKI_MAX_PEERS> peers;
Peer* wki_peer_find(uint16_t node) {
    assert(!s_routing_lock.held);
    for (auto& peer : peers) if (peer.node_id == node) return &peer;
    return nullptr;
}
bool wki_peer_lifecycle_rx_try_acquire(Peer* peer) {
    assert(!s_routing_lock.held);
    if (peer->busy) return false;
    ++peer->readers; return true;
}
void after_release(Peer*);
void wki_peer_lifecycle_rx_release(Peer* peer) {
    assert(!s_routing_lock.held && peer->readers != 0);
    --peer->readers; after_release(peer);
}
bool leases_enabled() { return (g_wki.capabilities & WKI_CAP_ROUTING_LEASE) != 0; }
std::atomic<bool> s_own_pending{false}, s_replay_pending{false};
uint64_t s_revision = 1, s_published_revision = 1, s_invalidation_revision = 0;
size_t s_origin_replay_index = 0;
unsigned notifications = 0;
void notify_routing_worker() { ++notifications; }
void wki_lsa_generate_and_flood() { s_own_pending = true; notify_routing_worker(); }
void wki_lsa_replay_to_peer(uint16_t);
int wki_send(uint16_t, uint16_t, MsgType, const uint8_t*, uint16_t);
'''

CASES = r'''
struct Sent { uint16_t node; std::vector<uint8_t> bytes; };
std::vector<Sent> sent;
int send_result = WKI_OK;
bool request_during_send = false, new_session_after_release = false;
unsigned expected_generation = 2;
void after_release(Peer* peer) {
    if (!new_session_after_release) return;
    new_session_after_release = false;
    assert(peer->readers == 0);
    peer->auth_session.generation = 3;
    s_routing_lock.lock();
    lsdb_find(peer->node_id)->authenticated_generation = 3;
    s_routing_lock.unlock();
    expected_generation = 3;
    wki_lsa_replay_to_peer(peer->node_id);
}
int wki_send(uint16_t node, uint16_t channel, MsgType type, const uint8_t* data, uint16_t length) {
    assert(!s_routing_lock.held && channel == 0 && type == MsgType::LSA);
    auto* peer = wki_peer_find(node);
    assert(peer && peer->readers != 0 && !peer->busy && peer->state == PeerState::CONNECTED);
    assert(peer->auth_session.generation == expected_generation);
    auto const* payload = reinterpret_cast<LsaPayload const*>(data);
    assert(payload->origin_node == 1 && payload->lsa_seq == 4);
    assert(payload->num_neighbors == (lsdb_find(1)->lease.withdrawn ? 0 : 1));
    if (length != lsa_total_size(payload)) {
        assert(length == lsa_total_size(payload) + sizeof(LsaLeaseSuffix));
        auto const* suffix = reinterpret_cast<LsaLeaseSuffix const*>(data + lsa_total_size(payload));
        assert(suffix->token == 0 && suffix->boot_epoch == 10 && suffix->age_ms == 0);
        assert(suffix->flags == (lsdb_find(1)->lease.withdrawn ? WKI_LSA_LEASE_WITHDRAWN : 0));
    }
    sent.push_back({node, {data, data + length}});
    if (request_during_send) { request_during_send = false; wki_lsa_replay_to_peer(node); }
    return send_result;
}
void reset(size_t targets = 1) {
    assert(!s_routing_lock.held);
    s_lsdb = {}; peers = {}; sent.clear(); notifications = 0;
    s_origin_replay_index = 0; send_result = WKI_OK;
    request_during_send = new_session_after_release = false; expected_generation = 2;
    s_own_pending = s_replay_pending = false;
    s_revision = s_published_revision = 1;
    g_wki.capabilities = 0;
    auto& own = s_lsdb[0]; own.origin_node = 1; own.valid = true;
    own.lsa_seq = 4; own.num_neighbors = 1; own.neighbors[0] = {2,1,1500};
    own.advertised_identity.boot_epoch = 10; own.advertised_identity.nonce[0] = 9;
    for (size_t i = 0; i < targets; ++i) {
        auto& entry = s_lsdb[i+1]; entry.origin_node = static_cast<uint16_t>(i+2);
        entry.authenticated_generation = 2;
        entry.authenticated_identity = own.advertised_identity;
        auto& peer = peers[i]; peer.node_id = entry.origin_node; peer.remote_auth_nonce[0] = 9;
    }
}
void request(uint16_t node = 2) {
    size_t const before = sent.size(); wki_lsa_replay_to_peer(node);
    assert(sent.size() == before); // RX only queues; no TX or peer lookup under routing lock.
    s_own_pending = s_replay_pending = false; // existing own generation/database sweep completed.
}
void no_leaks() { for (auto const& peer : peers) assert(peer.readers == 0); }
int main() {
    // Requested node 4 is routed: delivery must not rely on direct fanout.
    reset(3); request(4); drain_origin_replays(100);
    assert(sent.size() == 1 && sent[0].node == 4); no_leaks();
    drain_origin_replays(200); assert(sent.size() == 1 && !routing_work_pending());

    reset(); request(); request(); request();
    assert(routing_work_pending()); drain_origin_replays(100);
    assert(sent.size() == 1 && !routing_work_pending()); no_leaks();

    reset(255); for (uint16_t node = 2; node <= 256; ++node) request(node);
    drain_origin_replays(100); assert(sent.size() == ORIGIN_REPLAY_BUDGET);
    assert(routing_work_pending());
    for (int round = 0; round < 31; ++round) drain_origin_replays(200);
    assert(sent.size() == 255 && !routing_work_pending());
    std::array<bool,257> seen{};
    for (auto const& item : sent) { assert(!seen[item.node]); seen[item.node] = true; }
    no_leaks();

    reset(); request(0); request(0xffff); request(1); request(300);
    assert(!routing_work_pending()); drain_origin_replays(100); assert(sent.empty());
    reset(); s_lsdb[1].fenced = true; request(); drain_origin_replays(100); assert(sent.empty());
    reset(); s_lsdb[1].authenticated_generation = 0; request(); drain_origin_replays(100); assert(sent.empty());

    reset(); request(); s_own_pending = true;
    drain_origin_replays(100); assert(sent.empty() && s_lsdb[1].origin_replay_pending);
    s_own_pending = false; drain_origin_replays(200); assert(sent.size() == 1);
    reset(); request(); s_lsdb[0].valid = false;
    drain_origin_replays(100); assert(sent.empty() && s_lsdb[1].origin_replay_pending && !routing_work_pending());
    s_lsdb[0].valid = true; drain_origin_replays(200); assert(sent.size() == 1);
    reset(); request(); s_lsdb[0].origin_node = 0;
    drain_origin_replays(100); assert(sent.empty() && !routing_work_pending());

    reset(); request(); send_result = WKI_ERR_NO_ROUTE;
    drain_origin_replays(100); assert(sent.size() == 1 && s_lsdb[1].origin_replay_pending);
    assert(!routing_work_pending()); send_result = WKI_OK;
    drain_origin_replays(200); assert(sent.size() == 2 && !s_lsdb[1].origin_replay_pending); no_leaks();
    reset(); request(); peers[0].busy = true;
    drain_origin_replays(100); assert(sent.empty() && s_lsdb[1].origin_replay_pending && !routing_work_pending());
    peers[0].busy = false; drain_origin_replays(200); assert(sent.size() == 1); no_leaks();

    reset(); request(); peers[0].auth_session.generation = 3;
    drain_origin_replays(100); assert(sent.empty() && !s_lsdb[1].origin_replay_pending); no_leaks();
    reset(); request(); peers[0].auth_session.generation = s_lsdb[1].authenticated_generation = 3;
    drain_origin_replays(100); assert(sent.empty() && !s_lsdb[1].origin_replay_pending); no_leaks();
    reset(); request(); peers[0].state = PeerState::FENCED;
    drain_origin_replays(100); assert(sent.empty() && !s_lsdb[1].origin_replay_pending); no_leaks();
    reset(); request(); s_lsdb[1].fenced = true;
    drain_origin_replays(100); assert(sent.empty() && !s_lsdb[1].origin_replay_pending); no_leaks();
    reset(); request(); peers[0].node_id = 0;
    drain_origin_replays(100); assert(sent.empty() && !s_lsdb[1].origin_replay_pending); no_leaks();

    for (int result : {WKI_OK, WKI_ERR_NO_ROUTE}) {
        reset(); request(); request_during_send = true; send_result = result;
        drain_origin_replays(100); assert(sent.size() == 1 && routing_work_pending());
        assert(s_lsdb[1].origin_replay_pending && !s_lsdb[1].origin_replay_retry);
        s_own_pending = false; s_replay_pending = false; send_result = WKI_OK;
        drain_origin_replays(200); assert(sent.size() == 2); no_leaks();
    }
    reset(); request(); send_result = WKI_ERR_NO_ROUTE; new_session_after_release = true;
    drain_origin_replays(100); assert(sent.size() == 1 && s_lsdb[1].origin_replay_generation == 3);
    assert(s_lsdb[1].origin_replay_pending && !s_lsdb[1].origin_replay_retry);
    s_own_pending = false; s_replay_pending = false; send_result = WKI_OK;
    drain_origin_replays(200); assert(sent.size() == 2); no_leaks();

    for (bool local : {false,true}) for (bool remote : {false,true}) {
        reset(); g_wki.capabilities = local ? WKI_CAP_ROUTING_LEASE : 0;
        peers[0].capabilities = s_lsdb[1].authenticated_capabilities = remote ? WKI_CAP_ROUTING_LEASE : 0;
        request(); drain_origin_replays(100); assert(sent.size() == 1);
        assert(sent[0].bytes.size() == sizeof(LsaPayload) + sizeof(LsaNeighborEntry) +
               (local && remote ? sizeof(LsaLeaseSuffix) : 0)); no_leaks();
    }
    reset(); g_wki.capabilities = peers[0].capabilities = s_lsdb[1].authenticated_capabilities = WKI_CAP_ROUTING_LEASE;
    s_lsdb[0].lease.withdrawn = true; s_lsdb[0].num_neighbors = 0;
    request(); drain_origin_replays(100); assert(sent.size() == 1); no_leaks();
    std::puts("PASS actual directed origin replay: coalescing, budget, generation pin, racing claims, retries and wire profiles");
}
'''


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=ROOT / "modules/kern/src/net/wki/routing.cpp")
    parser.add_argument("--header", type=Path, default=ROOT / "modules/kern/src/net/wki/routing.hpp")
    args = parser.parse_args()
    source = args.source.read_text()
    header = args.header.read_text()
    wire = (ROOT / "modules/kern/src/net/wki/wire.hpp").read_text()
    layouts = wire[wire.index("struct LsaNeighborEntry {"):wire.index("struct LsaLeaseRequest {")]
    entry = header[header.index("struct LsdbEntry {"):header.index("\n};", header.index("struct LsdbEntry {")) + 3]
    if "origin_replay_pending" not in entry:
        entry = entry[:-3] + "\nuint32_t origin_replay_generation = 0;\nbool origin_replay_pending = false, origin_replay_retry = false;\n};"
    bodies = "\n".join(function(source, name) for name in ("lsdb_find", "rx_origin", "confirmed_rx_origin", "encode_lsa"))
    if "void drain_origin_replays(" in source:
        bodies += "\n" + function(source, "send_origin_replay") + "\n" + function(source, "drain_origin_replays")
    else:
        bodies += "\nvoid drain_origin_replays(uint64_t) {}\n"
    bodies += "\n" + function(source, "wki_lsa_replay_to_peer") + "\n" + function(source, "routing_work_pending")
    compiler = os.environ.get("HOST_CXX") or shutil.which("g++", path=os.defpath)
    assert compiler, "host C++ compiler required"
    with tempfile.TemporaryDirectory(prefix="wos-wki-origin-") as directory:
        harness, binary = Path(directory) / "origin.cpp", Path(directory) / "origin"
        harness.write_text(PRELUDE + layouts + "\nconstexpr size_t LSA_BUFFER_SIZE = sizeof(LsaPayload) + 32*sizeof(LsaNeighborEntry) + sizeof(LsaLeaseSuffix);\n" + entry + "\nstd::array<LsdbEntry,WKI_MAX_PEERS> s_lsdb;\n" + bodies + CASES)
        subprocess.run([compiler, "-std=c++20", "-Wall", "-Wextra", "-Werror", "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-fno-pie", "-no-pie", "-I", str(ROOT / "modules/kern/src"), str(harness), "-o", str(binary)], check=True)
        # LeakSanitizer cannot inspect threads in the ptrace sandbox. Address
        # and undefined-behavior instrumentation remain active for the bodies.
        subprocess.run([str(binary)], check=True, timeout=15, env=os.environ | {"ASAN_OPTIONS": "detect_leaks=0"})


if __name__ == "__main__":
    main()
