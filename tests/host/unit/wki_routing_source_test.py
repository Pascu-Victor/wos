#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
ROUTING_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "routing.cpp"
ROUTING_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "routing.hpp"
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:const\s*)?(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def test_routing_lookup_returns_locked_snapshot() -> None:
    header = ROUTING_HPP.read_text()
    source = ROUTING_CPP.read_text()

    require_tokens(
        header,
        ["auto wki_routing_lookup(uint16_t dst_node, RoutingEntry* out, bool discovery = false) -> bool;"],
        "routing snapshot declaration",
    )
    if "auto wki_routing_lookup(uint16_t dst_node) -> const RoutingEntry*" in header + source:
        fail("routing lookup must not return a pointer into the mutable routing table")

    body = function_body(source, "wki_routing_lookup")
    require_order(
        body,
        [
            "if (out == nullptr)",
            "return false",
            "s_routing_lock.lock()",
            "s_published_revision < s_invalidation_revision",
            "for (auto const& route : s_tables.at(s_published_table))",
            "if (route.valid && route.dst_node == dst_node && NOW < route.lease_expires_us)",
            "*out = route",
            "s_routing_lock.unlock()",
            "return true",
            "s_routing_lock.unlock()",
            "return false",
        ],
        "routing lookup snapshot locking",
    )
    if "return &route" in body:
        fail("routing lookup must copy route snapshots rather than return route addresses")


def test_wki_send_paths_consume_route_snapshots() -> None:
    source = WKI_CPP.read_text()
    for obsolete in ("find_transport_for_peer", "resolve_next_hop"):
        if obsolete in source:
            fail(f"split route resolver {obsolete} can mix publication generations")
    resolve = function_body(source, "ResolvedRoute::resolve")
    if "peer->next_hop" in resolve:
        fail("withdrawn routes must not fall back to cached peer next hops")
    if resolve.count("wki_routing_lookup(dst_node, &route, allow_discovery)") != 1:
        fail("a routed transmission must use one copied SPF row")
    require_order(
        resolve,
        [
            "wki_routing_lookup(dst_node, &route, allow_discovery)",
            "wki_peer_find(route.next_hop)",
            "admit_hop(hop)",
            "hop->state == PeerState::CONNECTED",
            "hop->transport == route.transport",
            "hop->remote_boot_epoch == route.next_hop_boot_epoch",
            "hop->auth_session.generation == route.next_hop_auth_generation",
            "g_wki.peer_lock.unlock()",
            "g_wki.transport_lock.lock()",
            "transport == route.transport",
            "transport->chaos_in_flight.fetch_add(1",
            "g_wki.transport_lock.unlock()",
        ],
        "route identity validation and transport lifetime admission",
    )
    require_tokens(source, ["leased_transport->chaos_in_flight.fetch_sub(1", "wki_peer_lifecycle_rx_release(leased_hop)"], "route lease release")
    require_tokens(source, ["wki_peer_lifecycle_rx_release(leased_origin)"], "service origin lease release")
    require_order(resolve, ["wki_peer_find(dst_node)", "(!allow_discovery || retain_origin) && !admit_origin(peer)",
                            "wki_routing_lookup(dst_node, &route, allow_discovery)"],
                  "service origin is pinned before its route generation is captured")
    require_order(function_body(source, "admit_origin"),
                  ["peer == nullptr", "return false", "wki_peer_lifecycle_rx_try_acquire(peer)",
                   "leased_origin = peer", "wki_peer_lifecycle_owned_by_current_task(peer)", "admitted_origin = peer"],
                  "origin gate is nonblocking and borrows only the completed current writer")
    require_order(function_body(source, "admit_hop"),
                  ["peer == admitted_origin", "return true", "wki_peer_lifecycle_rx_try_acquire(peer)"],
                  "direct origin and next hop share one lifecycle admission")
    require_order(function_body(source, "admit_hop"),
                  ["wki_peer_lifecycle_rx_try_acquire(peer)", "leased_hop = peer",
                   "wki_peer_lifecycle_owned_by_current_task(peer)"],
                  "TX retains a shared peer gate or borrows its exact task-owned writer")
    require_order(resolve, ["peer->is_direct && !REQUIRE_LEASE", "admit_hop(peer)", "peer->lock.lock()"],
                  "direct contacts also require lifecycle admission before capture")
    if "wki_peer_lifecycle_acquire(" in resolve:
        fail("routed RX/TX admission must never wait for a lifecycle writer")
    for name in ("wki_send_raw", "wki_send_impl", "wki_send_authenticated_header", "wki_rx", "send_routed_frame"):
        body = function_body(source, name)
        submit = "route.send_pkt(pkt)" if name == "wki_send_impl" else "route.send("
        require_order(body, ["ResolvedRoute route", "route.resolve(", submit], name)
    for name in ("wki_timer_tick", "wki_timer_tick_single", "wki_rx"):
        require_tokens(function_body(source, name), ["send_routed_frame("], f"{name} retransmit routing")
    require_order(
        function_body(source, "wki_send_raw"),
        ["std::array<WkiTransport*, WKI_MAX_TRANSPORTS>", "g_wki.transport_lock.lock()",
         "t->chaos_in_flight.fetch_add(1", "g_wki.transport_lock.unlock()",
         "wki_transport_send(t, WKI_NODE_BROADCAST", "t->chaos_in_flight.fetch_sub(1"],
        "broadcast leases transports and releases registry lock before TX",
    )


def test_route_discovery_cannot_bypass_service_lease_expiry() -> None:
    source = WKI_CPP.read_text()
    discovery = function_body(source, "routing_discovery_message")
    allowed = set(re.findall(r"case MsgType::([A-Z_]+):", discovery))
    if allowed != {"HELLO", "HELLO_ACK", "HELLO_CONFIRM", "LSA", "LSA_ACK"}:
        fail("expired discovery routes must be restricted to identity/topology recovery")
    resolve = function_body(source, "ResolvedRoute::resolve")
    require_order(resolve, ["WKI_CAP_ROUTING_LEASE", "!allow_discovery", "!REQUIRE_LEASE"],
                  "direct peers must also satisfy the opt-in service lease")
    require_order(function_body(source, "send_routed_frame"),
                  ["len < WKI_HEADER_SIZE", "std::memcpy(&header, frame", "static_cast<MsgType>(header.msg_type)",
                   "routing_discovery_message(TYPE)"],
                  "retransmits use their retained message type for discovery admission")
    init = function_body(source, "wki_init")
    require_order(init, ['cmdline_has_token(ker::init::get_kernel_cmdline(), "wki.routing_lease")',
                        "g_wki.capabilities |= WKI_CAP_ROUTING_LEASE"], "routing leases remain explicitly opt-in")
    require_tokens(function_body(source, "wki_dispatch_reliable_msg"),
                   ["case MsgType::LSA_ACK:", "detail::handle_lsa_ack(hdr, payload, payload_len)"],
                   "bounded lease challenge RX dispatch")


def test_resolved_tx_rechecks_deadline_and_preserves_packet_ownership() -> None:
    source = WKI_CPP.read_text()
    require_tokens(function_body(source, "tx_deadline_live"),
                   ["route.lease_expires_us == UINT64_MAX", "wki_now_us() < route.lease_expires_us"],
                   "captured finite leases expire at the exact deadline")
    require_order(function_body(source, "tx_deadline_live"),
                  ["route.publication_generation != 0",
                   "wki_routing_snapshot_current(route.publication_generation, route.lease_expires_us)",
                   "route.lease_expires_us == UINT64_MAX"],
                  "published routes recheck revocation barrier and deadline at final TX admission")
    require_order(function_body(source, "send"),
                  ["!tx_deadline_live()", "return WKI_ERR_NO_ROUTE", "wki_transport_send(transport(), next_hop()"],
                  "raw resolved transport submission must recheck the retained deadline")
    require_order(function_body(source, "send_pkt"),
                  ["!tx_deadline_live()", "net::pkt_free(pkt)", "return WKI_ERR_NO_ROUTE",
                   "wki_transport_send_pkt(transport(), next_hop(), pkt)"],
                  "deadline rejection consumes the packet exactly like transport rejection")
    require_order(function_body(source, "wki_send_impl"),
                  ["memcpy(rt_data, frame, frame_len)", "route.send_pkt(pkt)", "INITIAL_TX_FAILED = RET < 0",
                   "if (INITIAL_TX_FAILED)", "rt_entry->retries = 1", "ch->tx_seq++"],
                  "expired initial attempt remains owned by the reliable retransmit stream")
    rx = function_body(source, "wki_rx")
    for start, end, install in (
        ("case MsgType::HELLO: {", "case MsgType::HELLO_ACK: {", "wki_auth_install_peer_session"),
        ("case MsgType::HELLO_ACK: {", "case MsgType::HELLO_CONFIRM: {", "wki_auth_install_peer_session"),
        ("case MsgType::HELLO_CONFIRM: {", "case MsgType::HEARTBEAT:", "wki_auth_confirm_peer_session"),
    ):
        branch = rx[rx.index(start):rx.index(end)]
        require_order(branch, ["PeerLifecycleTryLease", install, "wki_routing_peer_session_changed(peer->node_id)"],
                      "completed origin sessions invalidate routing before writer scope ends")


def test_rx_defers_spf_and_flooding() -> None:
    routing = ROUTING_CPP.read_text()
    for name in ("handle_lsa", "handle_lsa_ack", "wki_routing_recompute", "wki_lsa_generate_and_flood", "wki_lsa_replay_to_peer", "wki_routing_timer_tick"):
        body = function_body(routing, name)
        body = re.sub(r"//[^\n]*|/\*.*?\*/", "", body, flags=re.S)
        for forbidden in ("routing_model::compute(", "compute_and_publish(", "wki_send(", "new ", "kern_sleep", "kern_yield"):
            if forbidden in body:
                fail(f"RX-safe {name} must not execute {forbidden}")
    require_tokens(
        function_body(routing, "handle_lsa"),
        ["lsa->num_neighbors > WKI_MAX_NEIGHBORS_PER_LSA", "payload_len != lsa_total_size(lsa)",
         "routing_model::sequence_newer", "entry->flood_pending = true", "entry->revision = ++s_revision"],
        "bounded LSA admission",
    )
    require_tokens(routing, ["FLOOD_BUDGET", "attempts < FLOOD_BUDGET", "snapshot = s_lsdb.at(INDEX)"], "bounded task-context flood")
    for name, work in (("compute_and_publish", "compute_table("), ("drain_floods", "wki_send("),
                       ("drain_lease_requests", "wki_send("), ("drain_lease_responses", "wki_send(")):
        body = function_body(routing, name)
        prefix = body[:body.index(work)]
        if prefix.rfind("s_routing_lock.lock()") > prefix.rfind("s_routing_lock.unlock()"):
            fail(f"{name} must release the routing lock before {work}")


def test_publication_discards_obsolete_results() -> None:
    routing = ROUTING_CPP.read_text()
    require_order(
        function_body(routing, "compute_and_publish"),
        ["REVISION = s_revision", "s_routing_lock.unlock()", "snapshot_direct_peers()", "s_work_lsdb.at(i) = s_lsdb.at(i)",
         "compute_table(output, false", "compute_table(s_discovery_tables.at(OUTPUT), true", "s_routing_lock.lock()",
         "REVISION != s_revision", "++s_diag.obsolete", "s_published_table = OUTPUT", "s_published_revision = REVISION"],
        "complete route publication with revision rejection",
    )
    require_order(function_body(routing, "compute_table"),
                  ["routing_model::compute(", "s_path_expiry.at(best) = std::min",
                   "route.publication_generation = revision", "route.lease_expires_us = s_path_expiry.at(i)"],
                  "route expiration propagates through every predecessor in the path")
    require_order(
        function_body(routing, "wki_routing_invalidate_node"),
        ["s_routing_lock.lock()", "entry->valid = false", "s_invalidation_revision = s_revision", "s_routing_lock.unlock()"],
        "withdrawal suppresses prior publications immediately",
    )
    require_order(
        function_body(routing, "generate_own_lsa"),
        ["s_own_pending.exchange(false", "snapshot_direct_peers()"],
        "own-LSA request consumed before collecting peer state",
    )
    require_order(
        function_body(routing, "snapshot_direct_peers"),
        ["PREVIOUS.transport != out.transport", "if (changed)", "++s_revision", "s_own_pending.store(true"],
        "transport changes independently invalidate published next-hop identity",
    )


def test_new_session_replays_current_lsa_after_key_install() -> None:
    header = ROUTING_HPP.read_text()
    routing = ROUTING_CPP.read_text()
    wki = WKI_CPP.read_text()

    require_tokens(
        header,
        ["void wki_lsa_replay_to_peer(uint16_t peer_node);"],
        "authenticated LSA replay declaration",
    )
    replay = function_body(routing, "wki_lsa_replay_to_peer")
    require_order(
        replay,
        [
            "s_replay_pending.store(true",
            "wki_lsa_generate_and_flood()",
        ],
        "session replay requests bounded task-context database replay",
    )
    require_tokens(
        function_body(routing, "snapshot_direct_peers"),
        ["peer.state == PeerState::CONNECTED", "peer.is_direct", "peer.auth_session.active"],
        "deferred replay admits only authenticated connected direct peers",
    )

    rx = function_body(wki, "wki_rx")
    if rx.count("wki_lsa_replay_to_peer(peer->node_id);") != 3:
        fail("current HELLO, HELLO_ACK, and confirmed responder sessions must replay the current LSA")
    require_order(
        rx,
        [
            "wki_auth_install_peer_session(peer",
            "if (installed)",
            "wki_peer_send_hello_confirm(peer)",
            "wki_lsa_replay_to_peer(peer->node_id)",
            "wki_resource_advertise_to_peer(peer->node_id)",
        ],
        "HELLO_ACK must install and confirm keys before topology or resource replay",
    )
    require_order(
        rx,
        [
            "AUTH_RESULT == WkiAuthFrameResult::PENDING_CONFIRMATION",
            "wki_auth_confirm_peer_session(peer, verified_trailer)",
            "detail::handle_hello(transport, hdr, payload, PAYLOAD_LEN, true)",
            "wki_lsa_replay_to_peer(peer->node_id)",
            "wki_resource_advertise_to_peer(peer->node_id)",
        ],
        "the responder must promote a confirmed session before topology or resource replay",
    )


def test_origin_leases_are_authenticated_and_do_not_promote_hints() -> None:
    routing = ROUTING_CPP.read_text()
    admission = function_body(routing, "handle_lsa")
    require_order(admission,
                  ["confirmed_rx_origin(hdr->src_node)", "hdr->src_node != lsa->origin_node", "s_routing_lock.lock()",
                   "entry->fenced", "routing_lease::accept_response"],
                  "only an authenticated origin may answer its pending lease challenge")
    require_tokens(admission, ["entry->hint_identity", "entry->hint_neighbors", "entry->lease.extended_required",
                               "SAME_PAYLOAD", "suffix->token != 0"],
                   "discovery hints remain distinct and same-sequence replies require exact payload equality")
    if "lease.extended_required = true" in admission:
        fail("a relayed hint must not establish the origin's authenticated capability")
    require_order(function_body(routing, "snapshot_direct_peers"),
                  ["peer.auth_session.active", "origin.identity.nonce = peer.remote_auth_nonce",
                   "g_wki.peer_lock.unlock()", "entry->authenticated_generation != origin.generation",
                   "origin.identity = entry->authenticated_identity", "routing_lease::bind_origin"],
                  "origin binding uses completed session identity outside the peer lock")
    if routing.count("observe_authenticated_identity(") != 2:
        fail("only the completed-session hook may establish authenticated origin identity")
    require_tokens(function_body(routing, "confirmed_rx_origin"),
                   ["!entry->fenced", "entry->authenticated_generation == result.generation",
                    "result.identity = entry->authenticated_identity"],
                   "RX identity comes from the completed session record, not a staged HELLO nonce")
    require_tokens(function_body(routing, "topology_eligible"),
                   ["routing_lease::usable(entry.lease, now)", "routing_lease::legacy_allowed(entry.lease)"],
                   "service topology excludes expired or downgraded origins")
    require_order(function_body(routing, "wki_routing_peer_session_changed"),
                  ["rx_origin(node_id)", "s_routing_lock.lock()", "observe_authenticated_identity",
                   "entry->lease.session_generation != CURRENT.generation", "routing_lease::fence",
                   "s_invalidation_revision = s_revision"],
                  "session changes synchronously suppress old origin leases")
    require_tokens(function_body(routing, "drain_lease_requests"),
                   ["attempts < LEASE_BUDGET", "routing_lease::pending", "routing_lease::begin_request"],
                   "receiver challenges coalesce and have a fixed per-iteration budget")
    require_order(function_body(routing, "wki_routing_snapshot_current"),
                  ["s_routing_lock.lock()", "publication_generation >= s_invalidation_revision",
                   "wki_now_us() < lease_expires_us", "s_routing_lock.unlock()"],
                  "submission rejects expired or synchronously invalidated copied snapshots")
    require_tokens(function_body(routing, "wki_routing_peer_session_changed"),
                   ["bool const NEW_SESSION", "if (NEW_SESSION)", "entry->fenced = false"],
                   "idempotent old-session HELLO cannot undo explicit withdrawal")


def test_legacy_fences_keep_bounded_discovery_without_service_revival() -> None:
    routing = ROUTING_CPP.read_text()
    eligibility = function_body(routing, "topology_eligible")
    require_order(eligibility, ["if (discovery)", "WKI_ROUTING_DISCOVERY_US", "if (!leases_enabled())"],
                  "capability-off discovery must not inherit service tombstone rejection")
    admission = function_body(routing, "handle_lsa")
    require_order(admission,
                  ["if (entry != nullptr && !DOWNGRADE", "bool const NEW_HINT", "bool const REFRESH_HINT",
                   "bool const REFRESH_DUE", "if (NEW_HINT)", "entry->flood_pending = true",
                   "entry->hint_received_time_us = START", "if (NEW_HINT || REFRESH_DUE)",
                   "entry->hint_revision_time_us = START",
                   "else if (entry->fenced)"],
                  "all-mode hints and duplicate-only refresh precede the terminal service barrier")
    require_tokens(admission,
                   ["entry->discovery_expired", "DISCOVERY_REFRESH_INTERVAL_US", "notify = true"],
                   "exact legacy replays refresh discovery without scheduling SPF for every packet")
    compute = function_body(routing, "compute_table")
    require_tokens(" ".join(compute.split()), ["if (discovery && entry.hint_valid", "bool const BLOCKED = discovery",
                             "s_path_expiry.fill(discovery ? DISCOVERY_DEADLINE : UINT64_MAX)",
                             "if ((leases_enabled() || discovery) && NODE != g_wki.my_node_id)"],
                   "legacy discovery uses current neighbor leaves and bounded full-path deadlines")
    maintenance = function_body(routing, "maintain_leases")
    if "if (!leases_enabled())" in maintenance:
        fail("legacy discovery expiry must run even with capability disabled")
    require_tokens(maintenance, ["WKI_ROUTING_DISCOVERY_US", "entry.discovery_expired = true"],
                   "aged hints trigger discovery withdrawal in both modes")


def test_flood_worker_budget_covers_maximum_peer_fanout() -> None:
    routing = ROUTING_CPP.read_text()
    require_tokens(routing, ["constexpr size_t FLOOD_BUDGET = WKI_MAX_PEERS;",
                             "constexpr uint64_t WORKER_ACTIVE_INTERVAL_US = 20'000;",
                             "constexpr uint64_t WORKER_IDLE_INTERVAL_US = 1'000'000;"],
                   "maximum flood sweep must fit nominally within the ten-second refresh")
    worker = function_body(routing, "routing_worker")
    require_tokens(worker, ["routing_work_pending() ? WORKER_ACTIVE_INTERVAL_US : WORKER_IDLE_INTERVAL_US"],
                   "routing worker must retain the bounded active cadence without polling it while idle")
    pending = function_body(routing, "routing_work_pending")
    require_tokens(pending, ["entry.flood_cursor != 0 || !entry.flood_retry",
                             "entry.lease_response_token != 0 && !entry.lease_response_retry"],
                   "failed sends must use the idle retry deadline instead of keeping the worker hot")
    require_tokens(function_body(routing, "drain_floods"),
                   ["attempts < FLOOD_BUDGET", "++attempts", "wki_send", "entry.flood_retry = retry"],
                   "worker flood transmissions have an explicit fixed attempt budget")
    require_tokens(function_body(routing, "drain_lease_responses"),
                   ["entry.lease_response_retry = false", "entry.lease_response_retry = true"],
                   "lease response failures use bounded retry pacing")
    # Conservative 256 origins x 256 peers: 256 iterations * 20ms = 5.12s.
    # This excludes variable compute, scheduler and transport time by design.
    if 256 * 256 * 20_000 // 256 >= 10_000_000:
        fail("nominal capacity flood sweep exceeds the origin refresh period")


def test_completed_origin_replay_is_worker_owned_and_generation_pinned() -> None:
    source = ROUTING_CPP.read_text()
    producer = function_body(source, "wki_lsa_replay_to_peer")
    require_order(producer, ["s_routing_lock.lock()", "lsdb_find(peer_node)",
                            "!entry->fenced", "entry->authenticated_generation != 0",
                            "entry->origin_replay_generation = entry->authenticated_generation",
                            "entry->origin_replay_pending = true", "s_routing_lock.unlock()",
                            "s_replay_pending.store", "wki_lsa_generate_and_flood()"],
                  "completed-session replay keeps generation and database fanout")
    if "wki_send(" in producer or "lsdb_alloc(" in producer:
        fail("origin replay enqueue must use existing bounded state without inline TX")
    send = function_body(source, "send_origin_replay")
    require_order(send, ["wki_peer_lifecycle_rx_try_acquire(peer)", "confirmed_rx_origin(node)",
                        "peer->state == PeerState::CONNECTED", "CURRENT.generation == generation",
                        "encode_lsa(", "wki_send(", "wki_peer_lifecycle_rx_release(peer)"],
                  "directed origin replay pins its completed generation through TX")
    worker = function_body(source, "routing_worker")
    require_order(worker, ["generate_own_lsa(NOW)", "compute_and_publish()", "drain_origin_replays(NOW)"],
                  "origin replay uses current own topology and published discovery admission")
    require_tokens(function_body(source, "routing_work_pending"),
                   ["entry.origin_replay_pending && !entry.origin_replay_retry"],
                   "failed origin sends retry at the idle deadline")


def main() -> None:
    test_routing_lookup_returns_locked_snapshot()
    test_wki_send_paths_consume_route_snapshots()
    test_route_discovery_cannot_bypass_service_lease_expiry()
    test_resolved_tx_rechecks_deadline_and_preserves_packet_ownership()
    test_rx_defers_spf_and_flooding()
    test_publication_discards_obsolete_results()
    test_new_session_replays_current_lsa_after_key_install()
    test_origin_leases_are_authenticated_and_do_not_promote_hints()
    test_legacy_fences_keep_bounded_discovery_without_service_revival()
    test_flood_worker_budget_covers_maximum_peer_fanout()
    test_completed_origin_replay_is_worker_owned_and_generation_pinned()
    print("WKI routing source invariants hold")


if __name__ == "__main__":
    main()
