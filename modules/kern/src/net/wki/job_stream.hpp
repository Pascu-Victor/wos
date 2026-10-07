#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace ker::net::wki::jobs {

// Private values, not wire layouts. Every operation requires the owning stream
// lock; authentication and retained job/packet ownership belong to the adapter.
enum class StreamId : uint8_t { STDOUT = 1, STDERR = 2 };
enum class StreamStatus : uint8_t {
    OK,
    READY,
    BLOCKED,
    CLOSED,
    LIMIT,
    STALE,
    INVALID,
    GAP,
    REPLAY,
    BUSY,
    EMPTY,
    END,
    OUT_OF_ORDER,
    CONFLICT
};
struct StreamIo {
    StreamStatus status{StreamStatus::INVALID};
    size_t bytes{};
    bool end{};
};
struct StreamCredit {
    uint64_t attachment{};
    uint64_t revision{};
    uint64_t consumed{};
    uint64_t window{};
    constexpr auto operator==(const StreamCredit&) const -> bool = default;
};
struct StreamFrame {
    uint64_t attachment{};
    uint64_t generation{};
    uint64_t offset{};
    size_t bytes{};
    bool end{};
    constexpr auto operator==(const StreamFrame&) const -> bool = default;
};
struct StreamPrepared {
    StreamStatus status{StreamStatus::INVALID};
    StreamFrame frame{};
};

// Producer bytes remain until application consumption is acknowledged. Each
// prepared frame has a private payload copy owned by the caller through reliable
// TX acceptance/retry; transport RX acknowledgement alone must not free bytes.
template <size_t Capacity>
class SendStream {
    static_assert(Capacity != 0);

   public:
    constexpr explicit SendStream(uint64_t initial_offset = 0, uint64_t first_frame_generation = 1)
        : retained_(initial_offset),
          produced_(initial_offset),
          exposed_(initial_offset),
          cursor_(initial_offset),
          next_frame_generation_(first_frame_generation) {}
    SendStream(const SendStream&) = delete;
    auto operator=(const SendStream&) -> SendStream& = delete;
    SendStream(SendStream&&) = delete;
    auto operator=(SendStream&&) -> SendStream& = delete;

    [[nodiscard]] constexpr auto retained() const -> uint64_t { return retained_; }
    [[nodiscard]] constexpr auto produced() const -> uint64_t { return produced_; }
    [[nodiscard]] constexpr auto buffered() const -> size_t { return static_cast<size_t>(produced_ - retained_); }
    [[nodiscard]] constexpr auto closed() const -> bool { return closed_; }

    constexpr auto append(std::span<const uint8_t> bytes) -> StreamIo {
        if (closed_) return {StreamStatus::CLOSED};
        auto const COUNT = std::min<uint64_t>(std::min(bytes.size(), Capacity - buffered()), UINT64_MAX - produced_);
        for (size_t index = 0; index < COUNT; ++index) data_[(produced_ + index) % Capacity] = bytes[index];
        produced_ += COUNT;
        auto const STATUS = COUNT == bytes.size()     ? StreamStatus::OK
                            : produced_ == UINT64_MAX ? StreamStatus::LIMIT
                                                      : StreamStatus::BLOCKED;
        return {STATUS, static_cast<size_t>(COUNT)};
    }
    constexpr void close() { closed_ = true; }

    // Preflight both stdout/stderr before changing the job attachment. Tokens
    // are nonzero, never reused, and supplied by the job's authorized controller.
    [[nodiscard]] constexpr auto check_attach(uint64_t attachment, uint64_t consumed, uint64_t window) const -> StreamStatus {
        if (attachment == 0 || window > Capacity || window > UINT64_MAX - consumed) return StreamStatus::INVALID;
        if (attachment <= last_attachment_) return StreamStatus::STALE;
        if (consumed < retained_) return StreamStatus::GAP;
        if (consumed > exposed_) return StreamStatus::INVALID;
        return StreamStatus::OK;
    }
    constexpr auto attach(uint64_t attachment, uint64_t consumed, uint64_t window) -> StreamStatus {
        auto const STATUS = check_attach(attachment, consumed, window);
        if (STATUS != StreamStatus::OK) return STATUS;
        retained_ = consumed;
        cursor_ = consumed;
        last_attachment_ = attachment;
        credit_ = {attachment, 0, consumed, window};
        attached_ = true;
        end_queued_ = false;
        pending_ = {};
        return StreamStatus::OK;
    }
    constexpr auto detach(uint64_t attachment) -> bool {
        if (!current(attachment)) return false;
        attached_ = false;
        pending_ = {};
        return true;
    }

    // Credits are absolute windows with revisions, never additive grants.
    // Old revisions cannot undo consumption or restore already spent credits.
    constexpr auto credit(const StreamCredit& credit) -> StreamStatus {
        if (!current(credit.attachment)) return StreamStatus::STALE;
        if (credit.revision == 0) return StreamStatus::INVALID;
        if (credit.revision < credit_.revision) return StreamStatus::STALE;
        if (credit.revision == credit_.revision) return credit == credit_ ? StreamStatus::REPLAY : StreamStatus::CONFLICT;
        if (credit.consumed < retained_ || credit.consumed > cursor_ || credit.window > Capacity ||
            credit.window > UINT64_MAX - credit.consumed)
            return StreamStatus::INVALID;
        retained_ = credit.consumed;
        credit_ = credit;
        return StreamStatus::OK;
    }

    // The caller must already own a bounded packet buffer and a job holder.
    // A TX refusal leaves this reservation/payload owned for retry. Only finish
    // after reliable TX takes responsibility, or detach and replay on reattach.
    constexpr auto prepare(uint64_t attachment, std::span<uint8_t> payload) -> StreamPrepared {
        if (!current(attachment)) return {StreamStatus::STALE};
        if (pending_.generation != 0) return {StreamStatus::BUSY};
        if (end_queued_) return {StreamStatus::END};
        if (cursor_ == produced_ && !closed_) return {StreamStatus::EMPTY};
        if (cursor_ < produced_ && cursor_ >= credit_.consumed + credit_.window) return {StreamStatus::BLOCKED};
        auto const AVAILABLE =
            std::min(produced_ - cursor_, credit_.consumed + credit_.window > cursor_ ? credit_.consumed + credit_.window - cursor_ : 0);
        auto const COUNT = static_cast<size_t>(std::min<uint64_t>(AVAILABLE, payload.size()));
        if (COUNT == 0 && cursor_ != produced_) return {StreamStatus::INVALID};
        if (next_frame_generation_ == 0) return {StreamStatus::LIMIT};
        for (size_t index = 0; index < COUNT; ++index) payload[index] = data_[(cursor_ + index) % Capacity];
        auto const GENERATION = next_frame_generation_;
        next_frame_generation_ = GENERATION == UINT64_MAX ? 0 : GENERATION + 1;
        pending_ = {attachment, GENERATION, cursor_, COUNT, closed_ && cursor_ + COUNT == produced_};
        cursor_ += COUNT;
        exposed_ = std::max(exposed_, cursor_);
        return {StreamStatus::READY, pending_};
    }
    constexpr auto finish(const StreamFrame& frame) -> bool {
        if (!current(frame.attachment) || frame.generation == 0 || frame != pending_) return false;
        end_queued_ = frame.end;
        pending_ = {};
        return true;
    }

   private:
    [[nodiscard]] constexpr auto current(uint64_t attachment) const -> bool { return attached_ && attachment == last_attachment_; }
    std::array<uint8_t, Capacity> data_{};
    uint64_t retained_{};
    uint64_t produced_{};
    uint64_t exposed_{};
    uint64_t cursor_{};
    uint64_t last_attachment_{};
    uint64_t next_frame_generation_{};
    StreamCredit credit_{};
    StreamFrame pending_{};
    bool attached_{};
    bool closed_{};
    bool end_queued_{};
};

// Consumer storage and byte delivery are separate from transport acceptance.
// Reattachment discards unconsumed local storage and requests replay from the
// last consumed offset; it never advances that application receipt implicitly.
template <size_t Capacity>
class ReceiveStream {
    static_assert(Capacity != 0);

   public:
    constexpr explicit ReceiveStream(uint64_t initial_offset = 0) : consumed_(initial_offset), received_(initial_offset) {}
    ReceiveStream(const ReceiveStream&) = delete;
    auto operator=(const ReceiveStream&) -> ReceiveStream& = delete;
    ReceiveStream(ReceiveStream&&) = delete;
    auto operator=(ReceiveStream&&) -> ReceiveStream& = delete;

    [[nodiscard]] constexpr auto consumed() const -> uint64_t { return consumed_; }
    [[nodiscard]] constexpr auto buffered() const -> size_t { return static_cast<size_t>(received_ - consumed_); }
    [[nodiscard]] constexpr auto window() const -> uint64_t { return std::min<uint64_t>(Capacity, UINT64_MAX - consumed_); }
    [[nodiscard]] constexpr auto ended() const -> bool { return closed_ && received_ == consumed_; }
    [[nodiscard]] constexpr auto advertised_credit() const -> StreamCredit { return credit_; }

    constexpr auto attach(uint64_t attachment) -> StreamStatus { return attach(attachment, window()); }
    constexpr auto attach(uint64_t attachment, uint64_t window) -> StreamStatus {
        if (attachment == 0 || window > Capacity || window > UINT64_MAX - consumed_) return StreamStatus::INVALID;
        if (attachment <= last_attachment_) return StreamStatus::STALE;
        last_attachment_ = attachment;
        received_ = consumed_;
        granted_end_ = consumed_ + window;
        credit_ = {attachment, 0, consumed_, window};
        closed_ = false;
        return StreamStatus::OK;
    }

    // Reading creates space but does not itself grant wire credits. Keep the
    // old grant's high-water mark when shrinking: already authorized in-flight
    // frames may still arrive. Physical capacity remains a separate bound.
    constexpr auto grant(uint64_t revision, uint64_t window) -> StreamStatus {
        if (last_attachment_ == 0 || revision == 0) return StreamStatus::INVALID;
        if (revision < credit_.revision) return StreamStatus::STALE;
        StreamCredit const NEXT{last_attachment_, revision, consumed_, window};
        if (revision == credit_.revision) return NEXT == credit_ ? StreamStatus::REPLAY : StreamStatus::CONFLICT;
        if (window > Capacity || window > UINT64_MAX - consumed_) return StreamStatus::INVALID;
        credit_ = NEXT;
        granted_end_ = std::max(granted_end_, consumed_ + window);
        return StreamStatus::OK;
    }

    constexpr auto receive(uint64_t attachment, uint64_t offset, std::span<const uint8_t> bytes, bool end) -> StreamStatus {
        if (attachment == 0 || attachment != last_attachment_) return StreamStatus::STALE;
        if (bytes.size() > UINT64_MAX - offset || (bytes.empty() && !end)) return StreamStatus::INVALID;
        auto const END = offset + bytes.size();
        if (offset > received_ || (offset < received_ && END > received_)) return StreamStatus::OUT_OF_ORDER;
        if (end && END < received_) return StreamStatus::CONFLICT;
        if (offset < received_) {
            for (auto index = std::max(offset, consumed_); index < END; ++index)
                if (data_[index % Capacity] != bytes[static_cast<size_t>(index - offset)]) return StreamStatus::CONFLICT;
            if (end) closed_ = true;
            return StreamStatus::REPLAY;
        }
        if (closed_) return bytes.empty() && end ? StreamStatus::REPLAY : StreamStatus::CLOSED;
        if (END > granted_end_ || bytes.size() > Capacity - buffered()) return StreamStatus::BLOCKED;
        for (size_t index = 0; index < bytes.size(); ++index) data_[(offset + index) % Capacity] = bytes[index];
        received_ = END;
        closed_ = end;
        return StreamStatus::OK;
    }
    constexpr auto read(std::span<uint8_t> bytes) -> StreamIo {
        auto const COUNT = std::min(bytes.size(), buffered());
        for (size_t index = 0; index < COUNT; ++index) bytes[index] = data_[(consumed_ + index) % Capacity];
        consumed_ += COUNT;
        return {StreamStatus::OK, COUNT, ended()};
    }

   private:
    std::array<uint8_t, Capacity> data_{};
    uint64_t consumed_{};
    uint64_t received_{};
    uint64_t last_attachment_{};
    uint64_t granted_end_{};
    StreamCredit credit_{};
    bool closed_{};
};

}  // namespace ker::net::wki::jobs
