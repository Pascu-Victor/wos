#pragma once

#include <cstdint>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Well-known Event IDs
// -----------------------------------------------------------------------------

// SYSTEM class (EVENT_CLASS_SYSTEM = 0x0001)
constexpr uint16_t EVENT_SYSTEM_NODE_JOIN = 0x0001;
constexpr uint16_t EVENT_SYSTEM_NODE_LEAVE = 0x0002;
constexpr uint16_t EVENT_SYSTEM_FENCING = 0x0003;

// DEVICE class (EVENT_CLASS_DEVICE = 0x0004)
constexpr uint16_t EVENT_DEVICE_HOTPLUG = 0x0001;
constexpr uint16_t EVENT_DEVICE_REMOVE = 0x0002;

// STORAGE class (EVENT_CLASS_STORAGE = 0x0005)
constexpr uint16_t EVENT_STORAGE_MOUNT = 0x0001;
constexpr uint16_t EVENT_STORAGE_UNMOUNT = 0x0002;

// ZONE class (EVENT_CLASS_ZONE = 0x0006)
constexpr uint16_t EVENT_ZONE_CREATED = 0x0001;
constexpr uint16_t EVENT_ZONE_DESTROYED = 0x0002;

// Wildcard matching
constexpr uint16_t EVENT_WILDCARD = 0xFFFF;

// -----------------------------------------------------------------------------
// Subscription — tracks which remote nodes want events from us
// -----------------------------------------------------------------------------

struct WkiEventSubscription {
    bool active = false;
    uint16_t subscriber_node = WKI_NODE_INVALID;
    uint16_t event_class = 0;
    uint16_t event_id = 0;
    uint8_t delivery_mode = EVENT_DELIVERY_BEST_EFFORT;
};

// -----------------------------------------------------------------------------
// Local handler — kernel subsystem callbacks for incoming events
// -----------------------------------------------------------------------------

using EventHandlerFn = void (*)(uint16_t origin_node, uint16_t event_class, uint16_t event_id, const void* data, uint16_t data_len);

struct WkiEventHandler {
    bool active = false;
    uint16_t event_class = 0;
    uint16_t event_id = 0;
    EventHandlerFn handler = nullptr;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the event bus subsystem. Called from wki_init().
void wki_event_init();

// Send a subscription request to a remote node (ask it to publish events to us).
void wki_event_subscribe(uint16_t peer_node, uint16_t event_class, uint16_t event_id, uint8_t delivery_mode = EVENT_DELIVERY_BEST_EFFORT);

// Send an unsubscribe request to a remote node.
void wki_event_unsubscribe(uint16_t peer_node, uint16_t event_class, uint16_t event_id);

// Publish an event to all remote subscribers and invoke local handlers.
void wki_event_publish(uint16_t event_class, uint16_t event_id, const void* data, uint16_t data_len);

// Register a local handler for incoming events matching (class, id).
// Use EVENT_WILDCARD for wildcard matching.
void wki_event_register_handler(uint16_t event_class, uint16_t event_id, EventHandlerFn handler);

// Unregister a local handler by function pointer.
void wki_event_unregister_handler(EventHandlerFn handler);

// Remove all subscriptions and pending state for a fenced peer.
void wki_event_cleanup_for_peer(uint16_t node_id);

// D1: Timer tick for reliable event retransmission. Called from wki_peer_timer_tick().
void wki_event_timer_tick(uint64_t now_us);

// -----------------------------------------------------------------------------
// Internal — RX message handlers (called from wki.cpp dispatch)
// -----------------------------------------------------------------------------

namespace detail {

void handle_event_subscribe(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_event_unsubscribe(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_event_publish(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_event_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
