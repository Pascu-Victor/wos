#!/usr/bin/env python3
"""Exercise the actual routing flood sweep with deterministic send failures.

The transport and lock are stubbed; cursor, budget, retry and revision handling
are compiled directly from routing.cpp, including concurrent RX replacement.
"""

import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def flood_function(source: str) -> str:
    match = re.search(r"^void drain_floods\(\) \{", source, re.M)
    assert match, "missing drain_floods"
    start = source.index("{", match.start())
    end, depth = start + 1, 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[match.start():end]


PRELUDE = r'''
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
constexpr size_t WKI_MAX_PEERS = 4, LSA_BUFFER_SIZE = 128;
constexpr uint16_t WKI_NODE_INVALID = 0, WKI_CHAN_CONTROL = 0;
constexpr uint16_t WKI_CAP_ROUTING_LEASE = 0x80;
constexpr int WKI_OK = 0;
enum class MsgType { LSA };
struct LsdbEntry {
    uint16_t origin_node = 99, num_neighbors = 0, flood_cursor = 0, flood_exclude = 0;
    uint16_t hint_num_neighbors = 0;
    uint32_t lsa_seq = 7, hint_sequence = 0, rdma_zone_bitmap = 0, hint_rdma_zone_bitmap = 0;
    uint64_t revision = 1, received_time_us = 0, hint_received_time_us = 0;
    std::array<int,4> neighbors{}, hint_neighbors{};
    int advertised_identity = 0, hint_identity = 0;
    bool valid = false, hint_valid = false, flood_pending = false, flood_retry = false;
    struct { bool withdrawn = false; } lease;
};
struct DirectPeer { uint16_t node = 0, capabilities = 0; };
struct Lock {
    bool held = false;
    void lock() { assert(!held); held = true; }
    void unlock() { assert(held); held = false; }
} s_routing_lock;
std::array<LsdbEntry,WKI_MAX_PEERS> s_lsdb;
std::array<DirectPeer,WKI_MAX_PEERS> s_direct;
size_t s_flood_index = 0, FLOOD_BUDGET = WKI_MAX_PEERS;
struct { uint16_t my_node_id = 99; } g_wki;
struct { unsigned flood_retries = 0; } s_diag;
unsigned sends = 0, encoded_seq = 0;
std::array<unsigned,32> destinations{}, sequences{};
int failures = 0;
uint16_t fail_node = 11;
bool replace_during_send = false;
bool leases_enabled() { return false; }
uint64_t wki_now_us() { return 100; }
size_t encode_lsa(const LsdbEntry& entry, std::array<uint8_t,LSA_BUFFER_SIZE>&, bool, int, uint64_t) {
    assert(!s_routing_lock.held); encoded_seq = entry.lsa_seq; return 12;
}
int wki_send(uint16_t node, uint16_t channel, MsgType type, const uint8_t*, size_t len) {
    assert(!s_routing_lock.held && channel == 0 && type == MsgType::LSA && len == 12);
    assert(sends < destinations.size());
    destinations[sends] = node; sequences[sends++] = encoded_seq;
    if (replace_during_send) {
        replace_during_send = false;
        auto& e = s_lsdb[0]; ++e.revision; e.lsa_seq = 8;
        e.flood_cursor = 0; e.flood_pending = true; e.flood_retry = false;
    }
    if (node == fail_node && failures != 0) { if (failures > 0) --failures; return -1; }
    return WKI_OK;
}
void reset() {
    s_lsdb = {}; s_direct = {}; s_flood_index = 0; FLOOD_BUDGET = 4;
    s_diag = {}; sends = 0; failures = 0; replace_during_send = false; fail_node = 11;
    s_lsdb[0].valid = s_lsdb[0].flood_pending = true;
    s_direct[0].node = 10; s_direct[1].node = 11; s_direct[2].node = 12;
}
'''

CASES = r'''
int main() {
    reset(); failures = 1;
    drain_floods();
    assert(sends == 3 && s_diag.flood_retries == 1);
    assert(s_lsdb[0].flood_pending && s_lsdb[0].flood_retry && s_lsdb[0].flood_cursor == 0);
    drain_floods();
    assert(sends == 6 && !s_lsdb[0].flood_pending && !s_lsdb[0].flood_retry);
    drain_floods(); assert(sends == 6);

    reset(); FLOOD_BUDGET = 2; fail_node = 10; failures = 1;
    drain_floods();
    assert(sends == 2 && s_lsdb[0].flood_cursor == 2 && s_lsdb[0].flood_retry);
    drain_floods();
    assert(sends == 3 && s_lsdb[0].flood_cursor == 0 && s_lsdb[0].flood_pending && s_lsdb[0].flood_retry);
    drain_floods();
    assert(sends == 5 && s_lsdb[0].flood_cursor == 2 && !s_lsdb[0].flood_retry);
    drain_floods();
    assert(sends == 6 && !s_lsdb[0].flood_pending && !s_lsdb[0].flood_retry);
    drain_floods(); assert(sends == 6);

    reset(); failures = -1;
    drain_floods(); drain_floods();
    assert(sends == 6 && s_diag.flood_retries == 2 && s_lsdb[0].flood_pending);
    failures = 0; drain_floods();
    assert(sends == 9 && !s_lsdb[0].flood_pending);

    reset(); replace_during_send = true;
    drain_floods();
    assert(sends == 3 && s_lsdb[0].lsa_seq == 8 && s_lsdb[0].flood_pending);
    assert(sequences[0] == 7 && sequences[2] == 7);
    drain_floods();
    assert(sends == 6 && sequences[3] == 8 && sequences[5] == 8 && !s_lsdb[0].flood_pending);

    reset(); s_lsdb[0].origin_node = 12; s_lsdb[0].flood_exclude = 11;
    drain_floods();
    assert(sends == 1 && destinations[0] == 10 && !s_lsdb[0].flood_pending);
    reset(); s_lsdb[0].hint_valid = true; s_lsdb[0].origin_node = 12;
    s_lsdb[0].hint_sequence = 42; s_lsdb[0].hint_received_time_us = 1;
    drain_floods(); assert(sends == 2 && sequences[0] == 42 && sequences[1] == 42);
    std::puts("PASS flood failure recovery, bounded sweeps, revision replacement and exclusions");
}
'''


def main() -> None:
    source = (ROOT / "modules/kern/src/net/wki/routing.cpp").read_text()
    compiler = os.environ.get("HOST_CXX") or shutil.which("c++", path=os.defpath)
    assert compiler, "host C++ compiler required"
    with tempfile.TemporaryDirectory(prefix="wos-wki-flood-") as directory:
        harness = Path(directory) / "flood.cpp"
        binary = Path(directory) / "flood"
        harness.write_text(PRELUDE + flood_function(source) + CASES)
        subprocess.run([compiler, "-std=c++20", "-Wall", "-Wextra", "-Werror", str(harness), "-o", str(binary)], check=True)
        subprocess.run([str(binary)], check=True, timeout=10)


if __name__ == "__main__":
    main()
