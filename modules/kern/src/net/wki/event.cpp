#include "event.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <deque>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>

namespace ker::net::wki {

// ─────────────────────────────────────────────────────────────────────────────
// Storage
// ─────────────────────────────────────────────────────────────────────────────

namespace {

std::deque<WkiEventSubscription> g_subscriptions;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<WkiEventHandler> g_local_handlers;      // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_event_initialized = false;                  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

bool event_matches(uint16_t sub_class, uint16_t sub_id, uint16_t pub_class, uint16_t pub_id) {
    if (sub_class != EVENT_WILDCARD && sub_class != pub_class) {
        return false;
    }
    if (sub_id != EVENT_WILDCARD && sub_id != pub_id) {
        return false;
    }
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// D1: Pending reliable events — awaiting ACK from remote subscribers
// ─────────────────────────────────────────────────────────────────────────────

constexpr uint8_t RELIABLE_MAX_RETRIES = 5;
constexpr uint64_t RELIABLE_RETRY_US = 50000;  // 50ms

struct PendingReliableEvent {
    uint16_t subscriber_node = WKI_NODE_INVALID;
    uint16_t event_class = 0;
    uint16_t event_id = 0;
    uint16_t origin_node = 0;
    uint64_t send_time_us = 0;
    uint8_t retries = 0;
    uint16_t payload_len = 0;
    std::array<uint8_t, sizeof(EventPublishPayload) + 256> payload = {};
};

std::deque<PendingReliableEvent> g_pending_reliable;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// ─────────────────────────────────────────────────────────────────────────────
// D2: Event log ring buffer — replay matching events to new subscribers
// ─────────────────────────────────────────────────────────────────────────────

constexpr size_t EVENT_LOG_MAX = 128;

struct EventLogEntry {
    uint16_t event_class = 0;
    uint16_t event_id = 0;
    uint16_t origin_node = 0;
    uint16_t data_len = 0;
    uint64_t timestamp_us = 0;
    std::array<uint8_t, 256> data = {};
};

std::array<EventLogEntry, EVENT_LOG_MAX> g_event_log;         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_event_log_head = 0;                                // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_event_log_count = 0;                               // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void event_log_push(uint16_t event_class, uint16_t event_id, uint16_t origin_node, const void* data, uint16_t data_len) {
    auto& entry = g_event_log[g_event_log_head];
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

void event_log_replay_to(uint16_t subscriber_node, uint16_t sub_class, uint16_t sub_id) {
    if (g_event_log_count == 0) {
        return;
    }

    // Determine the start index in the ring buffer
    uint32_t start = 0;
    if (g_event_log_count >= EVENT_LOG_MAX) {
        start = g_event_log_head;  // oldest entry
    }

    for (uint32_t i = 0; i < g_event_log_count; i++) {
        uint32_t idx = (start + i) % EVENT_LOG_MAX;
        const auto& entry = g_event_log[idx];

        if (!event_matches(sub_class, sub_id, entry.event_class, entry.event_id)) {
            continue;
        }

        // Build publish payload and send to subscriber
        auto total_len = static_cast<uint16_t>(sizeof(EventPublishPayload) + entry.data_len);
        std::array<uint8_t, sizeof(EventPublishPayload) + 256> buf = {};

        auto* pub = reinterpret_cast<EventPublishPayload*>(buf.data());
        pub->event_class = entry.event_class;
        pub->event_id = entry.event_id;
        pub->origin_node = entry.origin_node;
        pub->data_len = entry.data_len;

        if (entry.data_len > 0) {
            memcpy(buf.data() + sizeof(EventPublishPayload), entry.data.data(), entry.data_len);
        }

        wki_send(subscriber_node, WKI_CHAN_EVENT_BUS, MsgType::EVENT_PUBLISH, buf.data(), total_len);
    }
}

}  // namespace

// ─────────────────────────────────────────────────────────────────────────────
// Init
// ─────────────────────────────────────────────────────────────────────────────

void wki_event_init() {
    if (g_event_initialized) {
        return;
    }
    g_event_initialized = true;
    ker::mod::dbg::log("[WKI] Event bus subsystem initialized");
}

// ─────────────────────────────────────────────────────────────────────────────
// Subscribe / Unsubscribe — outgoing requests to a remote node
// ─────────────────────────────────────────────────────────────────────────────

void wki_event_subscribe(uint16_t peer_node, uint16_t event_class, uint16_t event_id, uint8_t delivery_mode) {
    if (!g_event_initialized) {
        return;
    }

    EventSubscribePayload sub = {};
    sub.event_class = event_class;
    sub.event_id = event_id;
    sub.delivery_mode = delivery_mode;

    wki_send(peer_node, WKI_CHAN_EVENT_BUS, MsgType::EVENT_SUBSCRIBE, &sub, sizeof(sub));
}

void wki_event_unsubscribe(uint16_t peer_node, uint16_t event_class, uint16_t event_id) {
    if (!g_event_initialized) {
        return;
    }

    EventSubscribePayload unsub = {};
    unsub.event_class = event_class;
    unsub.event_id = event_id;

    wki_send(peer_node, WKI_CHAN_EVENT_BUS, MsgType::EVENT_UNSUBSCRIBE, &unsub, sizeof(unsub));
}

// ─────────────────────────────────────────────────────────────────────────────
// Publish — send event to matching remote subscribers + invoke local handlers
// ─────────────────────────────────────────────────────────────────────────────

void wki_event_publish(uint16_t event_class, uint16_t event_id, const void* data, uint16_t data_len) {
    if (!g_event_initialized) {
        return;
    }

    // Build the publish payload: EventPublishPayload + data
    auto total_len = static_cast<uint16_t>(sizeof(EventPublishPayload) + data_len);
    std::array<uint8_t, sizeof(EventPublishPayload) + 256> buf = {};

    // Clamp data to fit in stack buffer
    if (total_len > sizeof(buf)) {
        data_len = static_cast<uint16_t>(sizeof(buf) - sizeof(EventPublishPayload));
        total_len = static_cast<uint16_t>(sizeof(buf));
    }

    auto* pub = reinterpret_cast<EventPublishPayload*>(buf.data());
    pub->event_class = event_class;
    pub->event_id = event_id;
    pub->origin_node = g_wki.my_node_id;
    pub->data_len = data_len;

    if (data != nullptr && data_len > 0) {
        memcpy(buf.data() + sizeof(EventPublishPayload), data, data_len);
    }

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

        wki_send(sub.subscriber_node, WKI_CHAN_EVENT_BUS, MsgType::EVENT_PUBLISH, buf.data(), total_len);

        // D1: For RELIABLE subscribers, stash a pending entry for ACK tracking
        if (sub.delivery_mode == EVENT_DELIVERY_RELIABLE) {
            PendingReliableEvent pending;
            pending.subscriber_node = sub.subscriber_node;
            pending.event_class = event_class;
            pending.event_id = event_id;
            pending.origin_node = g_wki.my_node_id;
            pending.send_time_us = wki_now_us();
            pending.retries = 0;
            pending.payload_len = total_len;
            memcpy(pending.payload.data(), buf.data(), total_len);

            g_pending_reliable.push_back(pending);
        }
    }

    // Invoke matching local handlers
    for (const auto& h : g_local_handlers) {
        if (!h.active || h.handler == nullptr) {
            continue;
        }
        if (!event_matches(h.event_class, h.event_id, event_class, event_id)) {
            continue;
        }

        h.handler(g_wki.my_node_id, event_class, event_id, data, data_len);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Local handler registration
// ─────────────────────────────────────────────────────────────────────────────

void wki_event_register_handler(uint16_t event_class, uint16_t event_id, EventHandlerFn handler) {
    if (handler == nullptr) {
        return;
    }

    WkiEventHandler h;
    h.active = true;
    h.event_class = event_class;
    h.event_id = event_id;
    h.handler = handler;

    g_local_handlers.push_back(h);
}

void wki_event_unregister_handler(EventHandlerFn handler) {
    std::erase_if(g_local_handlers, [handler](const WkiEventHandler& h) { return h.handler == handler; });
}

// ─────────────────────────────────────────────────────────────────────────────
// D1: Timer tick — retransmit reliable events that haven't been ACKed
// ─────────────────────────────────────────────────────────────────────────────

void wki_event_timer_tick(uint64_t now_us) {
    if (!g_event_initialized || g_pending_reliable.empty()) {
        return;
    }

    bool any_removed = false;

    for (auto& pending : g_pending_reliable) {
        if (now_us - pending.send_time_us < RELIABLE_RETRY_US) {
            continue;
        }

        if (pending.retries >= RELIABLE_MAX_RETRIES) {
            // Give up — mark for removal
            pending.subscriber_node = WKI_NODE_INVALID;
            any_removed = true;
            continue;
        }

        // Retransmit
        wki_send(pending.subscriber_node, WKI_CHAN_EVENT_BUS, MsgType::EVENT_PUBLISH, pending.payload.data(),
                 pending.payload_len);
        pending.send_time_us = now_us;
        pending.retries++;
    }

    if (any_removed) {
        std::erase_if(g_pending_reliable,
                       [](const PendingReliableEvent& p) { return p.subscriber_node == WKI_NODE_INVALID; });
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Fencing cleanup — remove all subscriptions and pending events for a fenced peer
// ─────────────────────────────────────────────────────────────────────────────

void wki_event_cleanup_for_peer(uint16_t node_id) {
    std::erase_if(g_subscriptions, [node_id](const WkiEventSubscription& sub) { return sub.subscriber_node == node_id; });
    std::erase_if(g_pending_reliable,
                   [node_id](const PendingReliableEvent& p) { return p.subscriber_node == node_id; });
}

// ─────────────────────────────────────────────────────────────────────────────
// RX handlers
// ─────────────────────────────────────────────────────────────────────────────

namespace detail {

void handle_event_subscribe(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(EventSubscribePayload)) {
        return;
    }

    const auto* sub = reinterpret_cast<const EventSubscribePayload*>(payload);

    // Check if this subscription already exists (upsert)
    for (auto& existing : g_subscriptions) {
        if (existing.active && existing.subscriber_node == hdr->src_node && existing.event_class == sub->event_class &&
            existing.event_id == sub->event_id) {
            // Update delivery mode
            existing.delivery_mode = sub->delivery_mode;
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

    ker::mod::dbg::log("[WKI] Event subscription: node=0x%04x class=0x%04x id=0x%04x mode=%s", hdr->src_node,
                       sub->event_class, sub->event_id,
                       (sub->delivery_mode == EVENT_DELIVERY_RELIABLE) ? "reliable" : "best-effort");

    // D2: Replay matching events from log to the new subscriber
    event_log_replay_to(hdr->src_node, sub->event_class, sub->event_id);
}

void handle_event_unsubscribe(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(EventSubscribePayload)) {
        return;
    }

    const auto* unsub = reinterpret_cast<const EventSubscribePayload*>(payload);

    std::erase_if(g_subscriptions, [&](const WkiEventSubscription& s) {
        return s.subscriber_node == hdr->src_node && s.event_class == unsub->event_class && s.event_id == unsub->event_id;
    });

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

    const void* event_data = nullptr;
    if (pub->data_len > 0) {
        event_data = payload + sizeof(EventPublishPayload);
    }

    // Invoke matching local handlers
    for (const auto& h : g_local_handlers) {
        if (!h.active || h.handler == nullptr) {
            continue;
        }
        if (!event_matches(h.event_class, h.event_id, pub->event_class, pub->event_id)) {
            continue;
        }

        h.handler(pub->origin_node, pub->event_class, pub->event_id, event_data, pub->data_len);
    }

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

    // D1: Remove the matching pending reliable event for this subscriber
    std::erase_if(g_pending_reliable, [&](const PendingReliableEvent& p) {
        return p.subscriber_node == hdr->src_node && p.event_class == ack->event_class && p.event_id == ack->event_id &&
               p.origin_node == ack->origin_node;
    });
}

}  // namespace detail

}  // namespace ker::net::wki
