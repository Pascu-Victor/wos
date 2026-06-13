#include "event.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <deque>
#include <net/wki/peer.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/perf/perf_events.hpp>

#include "platform/sys/spinlock.hpp"

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Storage
// -----------------------------------------------------------------------------

namespace {

std::deque<WkiEventSubscription> g_subscriptions;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<WkiEventHandler> g_local_handlers;      // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_event_initialized = false;                  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto perf_current_pid() -> uint64_t { return 0; }

auto perf_current_cpu() -> uint32_t { return static_cast<uint32_t>(ker::mod::cpu::get_current_cpu_id_safe()); }

void perf_record_event_point(ker::mod::perf::WkiPerfEventOp op, uint16_t peer, int32_t status, uint32_t aux, uint32_t correlation,
                             uint64_t callsite) {
    if (!ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }

    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::EVENT_BUS,
                                     static_cast<uint8_t>(op), ker::mod::perf::WkiPerfPhase::POINT, peer, WKI_CHAN_EVENT_BUS, correlation,
                                     status, aux, callsite);
    uint32_t const LATENCY_US = (op == ker::mod::perf::WkiPerfEventOp::REPLAY) ? aux : 0U;
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::EVENT_BUS, static_cast<uint8_t>(op), peer, WKI_CHAN_EVENT_BUS, status,
                                       LATENCY_US, op == ker::mod::perf::WkiPerfEventOp::REPLAY,
                                       op == ker::mod::perf::WkiPerfEventOp::RETRY ? 1U : 0U, 0);
}

bool event_matches(uint16_t sub_class, uint16_t sub_id, uint16_t pub_class, uint16_t pub_id) {
    return wki_event_filter_matches(sub_class, sub_id, pub_class, pub_id);
}

// -----------------------------------------------------------------------------
// D1: Pending reliable events - awaiting ACK from remote subscribers
// -----------------------------------------------------------------------------

constexpr uint8_t RELIABLE_MAX_RETRIES = 5;
constexpr uint64_t RELIABLE_RETRY_US = 50000;  // 50ms

struct PendingReliableEvent {
    uint16_t subscriber_node = WKI_NODE_INVALID;
    uint16_t event_class = 0;
    uint16_t event_id = 0;
    uint16_t origin_node = 0;
    uint64_t send_time_us = 0;
    uint32_t correlation = 0;
    uint8_t retries = 0;
    uint16_t payload_len = 0;
    std::array<uint8_t, sizeof(EventPublishPayload) + WKI_EVENT_DATA_MAX> payload = {};
};

std::deque<PendingReliableEvent> g_pending_reliable;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

ker::mod::sys::Spinlock s_event_lock;

enum class EventSendKind : uint8_t {
    PUBLISH,
    REPLAY,
    RETRY,
};

struct EventSendWork {
    uint16_t peer_node = WKI_NODE_INVALID;
    uint16_t payload_len = 0;
    uint32_t correlation = 0;
    uint32_t aux = 0;
    uint64_t callsite = 0;
    EventSendKind kind = EventSendKind::PUBLISH;
    bool reliable = false;
    std::array<uint8_t, sizeof(EventPublishPayload) + WKI_EVENT_DATA_MAX> payload = {};
};

struct EventHandlerWork {
    EventHandlerFn handler = nullptr;
    uint16_t origin_node = WKI_NODE_INVALID;
    uint16_t event_class = 0;
    uint16_t event_id = 0;
    const void* data = nullptr;
    uint16_t data_len = 0;
};

void queue_event_publish_send(std::deque<EventSendWork>& sends, uint16_t peer_node, const void* payload, uint16_t payload_len,
                              EventSendKind kind, bool reliable, uint32_t correlation, uint32_t aux, uint64_t callsite) {
    EventSendWork work;
    work.peer_node = peer_node;
    work.payload_len = payload_len;
    work.correlation = correlation;
    work.aux = aux;
    work.callsite = callsite;
    work.kind = kind;
    work.reliable = reliable;
    if (payload != nullptr && payload_len > 0) {
        memcpy(work.payload.data(), payload, payload_len);
    }
    sends.push_back(work);
}

void queue_matching_handlers_locked(std::deque<EventHandlerWork>& handlers, uint16_t origin_node, uint16_t event_class, uint16_t event_id,
                                    const void* data, uint16_t data_len) {
    for (const auto& h : g_local_handlers) {
        if (!h.active || h.handler == nullptr) {
            continue;
        }
        if (!event_matches(h.event_class, h.event_id, event_class, event_id)) {
            continue;
        }

        handlers.push_back(EventHandlerWork{
            .handler = h.handler,
            .origin_node = origin_node,
            .event_class = event_class,
            .event_id = event_id,
            .data = data,
            .data_len = data_len,
        });
    }
}

void dispatch_event_sends(const std::deque<EventSendWork>& sends) {
    for (auto const& work : sends) {
        if (work.peer_node == WKI_NODE_INVALID || work.payload_len == 0) {
            continue;
        }

        constexpr auto PUBLISH_OP = static_cast<uint8_t>(ker::mod::perf::WkiPerfEventOp::PUBLISH);
        if (work.kind == EventSendKind::PUBLISH && work.reliable &&
            ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::EVENT_BUS, PUBLISH_OP)) {
            ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::EVENT_BUS, PUBLISH_OP,
                                             ker::mod::perf::WkiPerfPhase::BEGIN, work.peer_node, WKI_CHAN_EVENT_BUS, work.correlation, 0,
                                             work.payload_len, work.callsite);
        }

        int const STATUS = wki_send(work.peer_node, WKI_CHAN_EVENT_BUS, MsgType::EVENT_PUBLISH, work.payload.data(), work.payload_len);

        if (work.kind == EventSendKind::RETRY) {
            perf_record_event_point(ker::mod::perf::WkiPerfEventOp::RETRY, work.peer_node, STATUS, work.aux, work.correlation,
                                    work.callsite);
        } else if (work.kind == EventSendKind::PUBLISH && !work.reliable) {
            perf_record_event_point(ker::mod::perf::WkiPerfEventOp::PUBLISH, work.peer_node, STATUS, work.payload_len, work.correlation,
                                    work.callsite);
        }
    }
}

void dispatch_event_handlers(const std::deque<EventHandlerWork>& handlers) {
    for (auto const& work : handlers) {
        if (work.handler != nullptr) {
            work.handler(work.origin_node, work.event_class, work.event_id, work.data, work.data_len);
        }
    }
}

// -----------------------------------------------------------------------------
// D2: Event log ring buffer - replay matching events to new subscribers
// -----------------------------------------------------------------------------

constexpr size_t EVENT_LOG_MAX = 128;

struct EventLogEntry {
    uint16_t event_class = 0;
    uint16_t event_id = 0;
    uint16_t origin_node = 0;
    uint16_t data_len = 0;
    uint64_t timestamp_us = 0;
    std::array<uint8_t, WKI_EVENT_DATA_MAX> data = {};
};

std::array<EventLogEntry, EVENT_LOG_MAX> g_event_log;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_event_log_head = 0;                         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_event_log_count = 0;                        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void event_log_push(uint16_t event_class, uint16_t event_id, uint16_t origin_node, const void* data, uint16_t data_len) {
    auto& entry = g_event_log.at(g_event_log_head);
    entry.event_class = event_class;
    entry.event_id = event_id;
    entry.origin_node = origin_node;
    entry.timestamp_us = wki_now_us();
    entry.data_len = std::min<uint16_t>(data_len, static_cast<uint16_t>(entry.data.size()));
    if (data != nullptr && entry.data_len > 0) {
        memcpy(entry.data.data(), data, entry.data_len);
    }

    g_event_log_head = (g_event_log_head + 1) % EVENT_LOG_MAX;
    if (g_event_log_count < EVENT_LOG_MAX) {
        g_event_log_count++;
    }
}

auto event_log_replay_to_locked(std::deque<EventSendWork>& sends, uint16_t subscriber_node, uint16_t sub_class, uint16_t sub_id)
    -> uint64_t {
    if (g_event_log_count == 0) {
        return 0;
    }

    uint64_t const REPLAY_START_US = wki_now_us();

    // Determine the start index in the ring buffer
    uint32_t start = 0;
    if (g_event_log_count >= EVENT_LOG_MAX) {
        start = g_event_log_head;  // oldest entry
    }

    for (uint32_t i = 0; i < g_event_log_count; i++) {
        uint32_t const IDX = (start + i) % EVENT_LOG_MAX;
        const auto& entry = g_event_log.at(IDX);

        if (!event_matches(sub_class, sub_id, entry.event_class, entry.event_id)) {
            continue;
        }

        auto total_len = static_cast<uint16_t>(sizeof(EventPublishPayload) + entry.data_len);
        std::array<uint8_t, sizeof(EventPublishPayload) + WKI_EVENT_DATA_MAX> buf = {};

        auto* pub = reinterpret_cast<EventPublishPayload*>(buf.data());
        pub->event_class = entry.event_class;
        pub->event_id = entry.event_id;
        pub->origin_node = entry.origin_node;
        pub->data_len = entry.data_len;

        if (entry.data_len > 0) {
            memcpy(buf.data() + sizeof(EventPublishPayload), entry.data.data(), entry.data_len);
        }

        queue_event_publish_send(sends, subscriber_node, buf.data(), total_len, EventSendKind::REPLAY, false, 0, 0, WOS_PERF_CALLSITE());
    }

    return REPLAY_START_US;
}

}  // namespace

// -----------------------------------------------------------------------------
// Init
// -----------------------------------------------------------------------------

void wki_event_init() {
    if (g_event_initialized) {
        return;
    }
    g_event_initialized = true;
    ker::mod::dbg::log("[WKI] Event bus subsystem initialized");
}

// -----------------------------------------------------------------------------
// Subscribe / Unsubscribe - outgoing requests to a remote node
// -----------------------------------------------------------------------------

void wki_event_subscribe(uint16_t peer_node, uint16_t event_class, uint16_t event_id, uint8_t delivery_mode) {
    if (!g_event_initialized) {
        return;
    }

    EventSubscribePayload sub = {};
    sub.event_class = event_class;
    sub.event_id = event_id;
    sub.delivery_mode = delivery_mode;

    wki_send(peer_node, WKI_CHAN_EVENT_BUS, MsgType::EVENT_SUBSCRIBE, &sub, sizeof(sub));
    perf_record_event_point(ker::mod::perf::WkiPerfEventOp::SUBSCRIBE, peer_node, 0, delivery_mode,
                            ker::mod::perf::next_wki_trace_correlation(), WOS_PERF_CALLSITE());
}

void wki_event_unsubscribe(uint16_t peer_node, uint16_t event_class, uint16_t event_id) {
    if (!g_event_initialized) {
        return;
    }

    EventSubscribePayload unsub = {};
    unsub.event_class = event_class;
    unsub.event_id = event_id;

    wki_send(peer_node, WKI_CHAN_EVENT_BUS, MsgType::EVENT_UNSUBSCRIBE, &unsub, sizeof(unsub));
    perf_record_event_point(ker::mod::perf::WkiPerfEventOp::UNSUBSCRIBE, peer_node, 0, 0, ker::mod::perf::next_wki_trace_correlation(),
                            WOS_PERF_CALLSITE());
}

// -----------------------------------------------------------------------------
// Publish - send event to matching remote subscribers + invoke local handlers
// -----------------------------------------------------------------------------

void wki_event_publish(uint16_t event_class, uint16_t event_id, const void* data, uint16_t data_len) {
    if (!g_event_initialized) {
        return;
    }

    // Build the publish payload: EventPublishPayload + data.
    // Clamp before forming total_len so large uint16_t inputs cannot wrap.
    WkiEventPayloadSize const SIZE = wki_event_payload_size(data_len);
    data_len = SIZE.data_len;
    uint16_t const TOTAL_LEN = SIZE.total_len;
    std::array<uint8_t, sizeof(EventPublishPayload) + WKI_EVENT_DATA_MAX> buf = {};

    auto* pub = reinterpret_cast<EventPublishPayload*>(buf.data());
    pub->event_class = event_class;
    pub->event_id = event_id;
    pub->origin_node = g_wki.my_node_id;
    pub->data_len = data_len;

    if (data != nullptr && data_len > 0) {
        memcpy(buf.data() + sizeof(EventPublishPayload), data, data_len);
    }

    bool notify_timer = false;
    std::deque<EventSendWork> sends;
    std::deque<EventHandlerWork> handlers;

    s_event_lock.lock();

    // D2: Store in event log ring buffer for future replay
    event_log_push(event_class, event_id, g_wki.my_node_id, data, data_len);

    // Send to all matching remote subscribers
    for (const auto& sub : g_subscriptions) {
        if (!sub.active) {
            continue;
        }
        if (!event_matches(sub.event_class, sub.event_id, event_class, event_id)) {
            continue;
        }

        // D1: For RELIABLE subscribers, stash a pending entry for ACK tracking
        uint32_t const CORRELATION = ker::mod::perf::next_wki_trace_correlation();
        if (sub.delivery_mode == EVENT_DELIVERY_RELIABLE) {
            PendingReliableEvent pending;
            pending.subscriber_node = sub.subscriber_node;
            pending.event_class = event_class;
            pending.event_id = event_id;
            pending.origin_node = g_wki.my_node_id;
            pending.send_time_us = wki_now_us();
            pending.correlation = CORRELATION;
            pending.retries = 0;
            pending.payload_len = TOTAL_LEN;
            memcpy(pending.payload.data(), buf.data(), TOTAL_LEN);

            g_pending_reliable.push_back(pending);
            notify_timer = true;
        }

        queue_event_publish_send(sends, sub.subscriber_node, buf.data(), TOTAL_LEN, EventSendKind::PUBLISH,
                                 sub.delivery_mode == EVENT_DELIVERY_RELIABLE, CORRELATION, TOTAL_LEN, WOS_PERF_CALLSITE());
    }

    queue_matching_handlers_locked(handlers, g_wki.my_node_id, event_class, event_id, data, data_len);

    s_event_lock.unlock();

    dispatch_event_sends(sends);
    dispatch_event_handlers(handlers);

    if (notify_timer) {
        wki_timer_notify();
    }
}

// -----------------------------------------------------------------------------
// Local handler registration
// -----------------------------------------------------------------------------

void wki_event_register_handler(uint16_t event_class, uint16_t event_id, EventHandlerFn handler) {
    if (handler == nullptr) {
        return;
    }

    WkiEventHandler h;
    h.active = true;
    h.event_class = event_class;
    h.event_id = event_id;
    h.handler = handler;

    s_event_lock.lock();
    g_local_handlers.push_back(h);
    s_event_lock.unlock();
}

void wki_event_unregister_handler(EventHandlerFn handler) {
    s_event_lock.lock();
    std::erase_if(g_local_handlers, [handler](const WkiEventHandler& h) { return h.handler == handler; });
    s_event_lock.unlock();
}

// -----------------------------------------------------------------------------
// D1: Timer tick - retransmit reliable events that haven't been ACKed
// -----------------------------------------------------------------------------

void wki_event_timer_tick(uint64_t now_us) {
    if (!g_event_initialized) {
        return;
    }

    std::deque<EventSendWork> retry_sends;
    s_event_lock.lock();

    if (g_pending_reliable.empty()) {
        s_event_lock.unlock();
        return;
    }

    bool any_removed = false;

    for (auto& pending : g_pending_reliable) {
        if (now_us < wki_future_deadline_us(pending.send_time_us, RELIABLE_RETRY_US)) {
            continue;
        }

        if (pending.retries >= RELIABLE_MAX_RETRIES) {
            // Give up - mark for removal
            pending.subscriber_node = WKI_NODE_INVALID;
            any_removed = true;
            continue;
        }

        queue_event_publish_send(retry_sends, pending.subscriber_node, pending.payload.data(), pending.payload_len, EventSendKind::RETRY,
                                 true, pending.correlation, pending.retries + 1U, WOS_PERF_CALLSITE());
        pending.send_time_us = now_us;
        pending.retries++;
    }

    if (any_removed) {
        std::erase_if(g_pending_reliable, [](const PendingReliableEvent& p) { return p.subscriber_node == WKI_NODE_INVALID; });
    }

    s_event_lock.unlock();
    dispatch_event_sends(retry_sends);
}

auto wki_event_next_timer_deadline_us(uint64_t now_us) -> uint64_t {
    if (!g_event_initialized) {
        return UINT64_MAX;
    }

    uint64_t next_deadline = UINT64_MAX;

    s_event_lock.lock();
    for (auto const& pending : g_pending_reliable) {
        if (pending.subscriber_node == WKI_NODE_INVALID) {
            continue;
        }

        uint64_t const FIRE_AT = wki_future_deadline_us(pending.send_time_us, RELIABLE_RETRY_US);
        next_deadline = std::min(next_deadline, wki_next_or_immediate_deadline_us(FIRE_AT, now_us));
    }
    s_event_lock.unlock();

    return next_deadline;
}

// -----------------------------------------------------------------------------
// Fencing cleanup - remove all subscriptions and pending events for a fenced peer
// -----------------------------------------------------------------------------

void wki_event_cleanup_for_peer(uint16_t node_id) {
    s_event_lock.lock();
    std::erase_if(g_subscriptions, [node_id](const WkiEventSubscription& sub) { return sub.subscriber_node == node_id; });
    std::erase_if(g_pending_reliable, [node_id](const PendingReliableEvent& p) { return p.subscriber_node == node_id; });
    s_event_lock.unlock();
}

// -----------------------------------------------------------------------------
// RX handlers
// -----------------------------------------------------------------------------

namespace detail {

void handle_event_subscribe(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(EventSubscribePayload)) {
        return;
    }

    const auto* sub = reinterpret_cast<const EventSubscribePayload*>(payload);
    std::deque<EventSendWork> replay_sends;
    uint64_t replay_start_us = 0;

    s_event_lock.lock();

    // Check if this subscription already exists (upsert)
    for (auto& existing : g_subscriptions) {
        if (existing.active && existing.subscriber_node == hdr->src_node && existing.event_class == sub->event_class &&
            existing.event_id == sub->event_id) {
            // Update delivery mode
            existing.delivery_mode = sub->delivery_mode;
            s_event_lock.unlock();
            return;
        }
    }

    // Add new subscription
    WkiEventSubscription entry;
    entry.active = true;
    entry.subscriber_node = hdr->src_node;
    entry.event_class = sub->event_class;
    entry.event_id = sub->event_id;
    entry.delivery_mode = sub->delivery_mode;

    g_subscriptions.push_back(entry);

    // D2: Replay matching events from log to the new subscriber
    replay_start_us = event_log_replay_to_locked(replay_sends, hdr->src_node, sub->event_class, sub->event_id);

    s_event_lock.unlock();

    dispatch_event_sends(replay_sends);
    if (replay_start_us != 0) {
        perf_record_event_point(ker::mod::perf::WkiPerfEventOp::REPLAY, hdr->src_node, 0,
                                static_cast<uint32_t>(wki_now_us() - replay_start_us), 0, WOS_PERF_CALLSITE());
    }

    perf_record_event_point(ker::mod::perf::WkiPerfEventOp::SUBSCRIBE, hdr->src_node, 0, sub->delivery_mode,
                            ker::mod::perf::next_wki_trace_correlation(), WOS_PERF_CALLSITE());

    ker::mod::dbg::log("[WKI] Event subscription: node=0x%04x class=0x%04x id=0x%04x mode=%s", hdr->src_node, sub->event_class,
                       sub->event_id, (sub->delivery_mode == EVENT_DELIVERY_RELIABLE) ? "reliable" : "best-effort");
}

void handle_event_unsubscribe(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(EventSubscribePayload)) {
        return;
    }

    const auto* unsub = reinterpret_cast<const EventSubscribePayload*>(payload);

    s_event_lock.lock();
    std::erase_if(g_subscriptions, [&](const WkiEventSubscription& s) {
        return s.subscriber_node == hdr->src_node && s.event_class == unsub->event_class && s.event_id == unsub->event_id;
    });
    s_event_lock.unlock();

    perf_record_event_point(ker::mod::perf::WkiPerfEventOp::UNSUBSCRIBE, hdr->src_node, 0, 0, ker::mod::perf::next_wki_trace_correlation(),
                            WOS_PERF_CALLSITE());

    ker::mod::dbg::log("[WKI] Event unsubscription: node=0x%04x class=0x%04x id=0x%04x", hdr->src_node, unsub->event_class,
                       unsub->event_id);
}

void handle_event_publish(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(EventPublishPayload)) {
        return;
    }

    const auto* pub = reinterpret_cast<const EventPublishPayload*>(payload);

    // Validate data_len
    if (sizeof(EventPublishPayload) + pub->data_len > payload_len) {
        return;
    }
    if (pub->data_len > WKI_EVENT_DATA_MAX) {
        return;
    }

    const void* event_data = nullptr;
    if (pub->data_len > 0) {
        event_data = payload + sizeof(EventPublishPayload);
    }

    std::deque<EventHandlerWork> handlers;
    s_event_lock.lock();

    queue_matching_handlers_locked(handlers, pub->origin_node, pub->event_class, pub->event_id, event_data, pub->data_len);

    s_event_lock.unlock();
    dispatch_event_handlers(handlers);

    // D1: Always send an ACK back to the publisher. We don't track our own
    // outgoing subscriptions locally, so we can't check delivery mode here.
    // Sending an unconditional ACK is cheap; the sender ignores ACKs for
    // best-effort subscriptions (no matching PendingReliableEvent).
    EventAckPayload ack = {};
    ack.event_class = pub->event_class;
    ack.event_id = pub->event_id;
    ack.origin_node = pub->origin_node;
    ack.reserved = 0;

    wki_send(hdr->src_node, WKI_CHAN_EVENT_BUS, MsgType::EVENT_ACK, &ack, sizeof(ack));
}

void handle_event_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(EventAckPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const EventAckPayload*>(payload);

    uint32_t correlation = 0;
    uint32_t latency_us = 0;

    // D1: Remove the matching pending reliable event for this subscriber
    s_event_lock.lock();
    auto const MATCH = std::ranges::find_if(g_pending_reliable, [&](const PendingReliableEvent& p) {
        return wki_event_ack_matches(p.subscriber_node, hdr->src_node, p.event_class, p.event_id, p.origin_node, *ack);
    });
    if (MATCH != g_pending_reliable.end()) {
        correlation = MATCH->correlation;
        latency_us = static_cast<uint32_t>(wki_now_us() - MATCH->send_time_us);
        g_pending_reliable.erase(MATCH);
    }
    s_event_lock.unlock();

    if (wki_event_ack_should_record_perf(correlation, ker::mod::perf::is_wki_recording_enabled())) {
        ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::EVENT_BUS,
                                         static_cast<uint8_t>(ker::mod::perf::WkiPerfEventOp::ACK), ker::mod::perf::WkiPerfPhase::END,
                                         hdr->src_node, WKI_CHAN_EVENT_BUS, correlation, 0, latency_us, WOS_PERF_CALLSITE());
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::EVENT_BUS,
                                           static_cast<uint8_t>(ker::mod::perf::WkiPerfEventOp::ACK), hdr->src_node, WKI_CHAN_EVENT_BUS, 0,
                                           latency_us, true, 0, 0);
    }
}

}  // namespace detail

#ifdef WOS_SELFTEST
auto wki_event_selftest_ack_removes_single_matching_pending() -> bool {
    wki_event_init();

    constexpr uint16_t SUBSCRIBER = 0x1234;
    constexpr uint16_t EVENT_CLASS = EVENT_CLASS_CUSTOM;
    constexpr uint16_t EVENT_ID = 0x0042;
    constexpr uint16_t ORIGIN = 0x5678;

    EventPublishPayload publish = {};
    publish.event_class = EVENT_CLASS;
    publish.event_id = EVENT_ID;
    publish.origin_node = ORIGIN;
    publish.data_len = 0;

    PendingReliableEvent first;
    first.subscriber_node = SUBSCRIBER;
    first.event_class = EVENT_CLASS;
    first.event_id = EVENT_ID;
    first.origin_node = ORIGIN;
    first.send_time_us = wki_now_us();
    first.correlation = 1;
    first.payload_len = sizeof(publish);
    memcpy(first.payload.data(), &publish, sizeof(publish));

    PendingReliableEvent second = first;
    second.correlation = 2;

    s_event_lock.lock();
    g_pending_reliable.clear();
    g_pending_reliable.push_back(first);
    g_pending_reliable.push_back(second);
    s_event_lock.unlock();

    WkiHeader hdr = {};
    hdr.src_node = SUBSCRIBER;

    EventAckPayload ack = {};
    ack.event_class = EVENT_CLASS;
    ack.event_id = EVENT_ID;
    ack.origin_node = ORIGIN;
    detail::handle_event_ack(&hdr, reinterpret_cast<const uint8_t*>(&ack), sizeof(ack));

    s_event_lock.lock();
    size_t remaining_matches = 0;
    bool second_still_pending = false;
    for (auto const& pending : g_pending_reliable) {
        if (wki_event_ack_matches(pending.subscriber_node, SUBSCRIBER, pending.event_class, pending.event_id, pending.origin_node, ack)) {
            remaining_matches++;
            second_still_pending = second_still_pending || pending.correlation == second.correlation;
        }
    }
    g_pending_reliable.clear();
    s_event_lock.unlock();

    return remaining_matches == 1 && second_still_pending;
}
#endif

}  // namespace ker::net::wki
