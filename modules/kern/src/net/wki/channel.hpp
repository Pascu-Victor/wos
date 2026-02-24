#pragma once

#include <net/wki/wki.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Channel reliability helpers
//
// The core reliability engine (seq/ack, retransmit queue, credits, reorder
// buffer) is integrated into wki.cpp's wki_send() and wki_rx() paths.
// This header exposes additional channel-level utilities.
// -----------------------------------------------------------------------------

// Send a standalone ACK on a channel (no payload, just ACK piggybacked)
void wki_channel_send_ack(WkiChannel* ch);

// Update RTT estimate from a new sample (Jacobson/Karels)
void wki_channel_update_rtt(WkiChannel* ch, uint32_t sample_us);

// Retransmit the oldest unACKed message on a channel
auto wki_channel_retransmit(WkiChannel* ch) -> int;

// Reset a channel to initial state (used during reconnection)
void wki_channel_reset(WkiChannel* ch);

// Get default credit count for a well-known channel
auto wki_channel_default_credits(uint16_t channel_id) -> uint16_t;

}  // namespace ker::net::wki
