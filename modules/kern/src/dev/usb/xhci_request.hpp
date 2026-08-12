#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <dev/usb/xhci.hpp>
#include <limits>
#include <span>

namespace ker::dev::usb::xhci_request {

constexpr uint64_t TRB_POINTER_MASK = ~uint64_t{0xFU};
constexpr uint32_t EVENT_DATA_FLAG = 1U << 2U;
constexpr uint32_t EVENT_RESIDUAL_MASK = 0x00FFFFFFU;
constexpr uint32_t EVENT_DCI_SHIFT = 16;
constexpr uint32_t EVENT_DCI_MASK = 0x1FU << EVENT_DCI_SHIFT;
constexpr uint32_t EVENT_SLOT_SHIFT = 24;
constexpr uint32_t EVENT_SLOT_MASK = 0xFFU << EVENT_SLOT_SHIFT;
constexpr int32_t DEFAULT_FAILED_RESULT = -1;
constexpr int32_t DEFAULT_PENDING_RESULT = std::numeric_limits<int32_t>::min();

enum class RequestKind : uint8_t {
    COMMAND,
    TRANSFER,
};

// RESERVED, COMPLETING, TIMING_OUT, and RELEASING are single-writer
// transition states. Result fields are valid after an acquire load observes
// COMPLETED, TIMED_OUT, or RETIRED.
enum class RequestState : uint8_t {
    FREE,
    RESERVED,
    SUBMITTED,
    COMPLETING,
    COMPLETED,
    TIMING_OUT,
    TIMED_OUT,
    RETIRED,
    RELEASING,
};

static_assert(std::atomic<RequestState>::is_always_lock_free, "xHCI request state must remain IRQ-safe");
static_assert(std::atomic<uint64_t>::is_always_lock_free, "xHCI request identities must remain IRQ-safe");
static_assert(std::atomic<uint32_t>::is_always_lock_free, "xHCI request results must remain IRQ-safe");

struct RequestHandle {
    static constexpr size_t INVALID_INDEX = std::numeric_limits<size_t>::max();

    size_t index = INVALID_INDEX;
    uint32_t generation = 0;

    [[nodiscard]] constexpr auto valid() const -> bool { return index != INVALID_INDEX && generation != 0; }
    [[nodiscard]] constexpr auto operator==(const RequestHandle&) const -> bool = default;
};

struct RequestSpec {
    RequestKind kind = RequestKind::COMMAND;
    uint8_t slot_id = 0;
    uint8_t dci = 0;
    std::span<const uint64_t> trb_phys{};
    uint32_t requested_length = 0;
    uint64_t deadline_us = 0;
    bool event_data_valid = false;
    uint64_t event_data = 0;
};

struct RequestSnapshot {
    RequestState state = RequestState::FREE;
    RequestKind kind = RequestKind::COMMAND;
    uint32_t generation = 0;
    uint8_t slot_id = 0;
    uint8_t dci = 0;
    uint32_t requested_length = 0;
    uint64_t deadline_us = 0;
    int32_t result = DEFAULT_PENDING_RESULT;
    uint32_t completion_code = 0;
    uint32_t residual = 0;
    uint8_t event_slot_id = 0;
    uint8_t event_dci = 0;
    uint64_t event_parameter = 0;
};

struct DecodedCompletionEvent {
    bool valid = false;
    RequestKind kind = RequestKind::COMMAND;
    bool uses_event_data = false;
    uint64_t identity = 0;
    uint32_t completion_code = 0;
    uint32_t residual = 0;
    uint8_t slot_id = 0;
    uint8_t dci = 0;
};

[[nodiscard]] inline auto decode_completion_event(const Trb& event) -> DecodedCompletionEvent {
    DecodedCompletionEvent decoded{};
    uint32_t const TYPE = event.control & TRB_TYPE_MASK;
    if (TYPE == TRB_CMD_COMPLETION) {
        decoded.valid = true;
        decoded.kind = RequestKind::COMMAND;
        decoded.identity = event.param & TRB_POINTER_MASK;
        decoded.completion_code = event.status >> 24U;
        decoded.slot_id = static_cast<uint8_t>((event.control & EVENT_SLOT_MASK) >> EVENT_SLOT_SHIFT);
        return decoded;
    }
    if (TYPE != TRB_TRANSFER_EVENT) {
        return decoded;
    }

    decoded.valid = true;
    decoded.kind = RequestKind::TRANSFER;
    decoded.uses_event_data = (event.control & EVENT_DATA_FLAG) != 0;
    decoded.identity = decoded.uses_event_data ? event.param : (event.param & TRB_POINTER_MASK);
    decoded.completion_code = event.status >> 24U;
    decoded.residual = event.status & EVENT_RESIDUAL_MASK;
    decoded.slot_id = static_cast<uint8_t>((event.control & EVENT_SLOT_MASK) >> EVENT_SLOT_SHIFT);
    decoded.dci = static_cast<uint8_t>((event.control & EVENT_DCI_MASK) >> EVENT_DCI_SHIFT);
    return decoded;
}

[[nodiscard]] constexpr auto default_event_result(const DecodedCompletionEvent& event) -> int32_t {
    if (event.completion_code == TRB_CC_SUCCESS || (event.kind == RequestKind::TRANSFER && event.completion_code == TRB_CC_SHORT_PKT)) {
        return 0;
    }
    return DEFAULT_FAILED_RESULT;
}

enum class CompletionDisposition : uint8_t {
    COMPLETED,
    DUPLICATE,
    UNKNOWN,
    LATE_AFTER_TIMEOUT,
    AMBIGUOUS,
    UNSUPPORTED_EVENT,
};

struct CompletionResult {
    CompletionDisposition disposition = CompletionDisposition::UNKNOWN;
    RequestHandle request{};
};

enum class TimeoutDisposition : uint8_t {
    TIMED_OUT,
    NOT_ARMED,
    NOT_DUE,
    COMPLETION_WON,
    ALREADY_TIMED_OUT,
    STALE_HANDLE,
};

struct TimeoutScanResult {
    size_t examined = 0;
    size_t timed_out = 0;
};

using TimeoutSink = void (*)(void* context, RequestHandle request);

template <size_t Capacity, size_t MaxRequestTrbs = 4>
class FixedRequestTable {
    static_assert(Capacity > 0, "xHCI request table must not be empty");
    static_assert(MaxRequestTrbs > 0, "xHCI request identity list must not be empty");
    static_assert(MaxRequestTrbs <= std::numeric_limits<uint8_t>::max(), "xHCI request TRB count must fit its record field");

   public:
    [[nodiscard]] auto reserve(const RequestSpec& spec) -> RequestHandle {
        if (!valid_spec(spec)) {
            return {};
        }

        for (size_t index = 0; index < Capacity; ++index) {
            auto& record = records_.at(index);
            RequestState expected = RequestState::FREE;
            if (!record.state.compare_exchange_strong(expected, RequestState::RESERVED, std::memory_order_acq_rel,
                                                      std::memory_order_acquire)) {
                continue;
            }

            uint32_t generation = record.generation.load(std::memory_order_relaxed) + 1U;
            if (generation == 0) {
                generation = 1;
            }
            record.generation.store(generation, std::memory_order_relaxed);
            record.kind.store(spec.kind, std::memory_order_relaxed);
            record.slot_id.store(spec.slot_id, std::memory_order_relaxed);
            record.dci.store(spec.dci, std::memory_order_relaxed);
            record.requested_length.store(spec.requested_length, std::memory_order_relaxed);
            record.deadline_us.store(spec.deadline_us, std::memory_order_relaxed);
            record.event_data_valid.store(spec.event_data_valid, std::memory_order_relaxed);
            record.event_data.store(spec.event_data, std::memory_order_relaxed);
            record.trb_count.store(static_cast<uint8_t>(spec.trb_phys.size()), std::memory_order_relaxed);
            for (size_t trb_index = 0; trb_index < MaxRequestTrbs; ++trb_index) {
                uint64_t const PHYS = trb_index < spec.trb_phys.size() ? spec.trb_phys[trb_index] : 0;
                record.trb_phys.at(trb_index).store(PHYS, std::memory_order_relaxed);
            }
            reset_result(record);
            record.state.store(RequestState::SUBMITTED, std::memory_order_release);
            return {.index = index, .generation = generation};
        }
        return {};
    }

    [[nodiscard]] auto complete_event(const Trb& event) -> CompletionResult {
        DecodedCompletionEvent const DECODED = decode_completion_event(event);
        return complete_decoded_event(DECODED, default_event_result(DECODED), event.param);
    }

    [[nodiscard]] auto complete_event(const Trb& event, int32_t result) -> CompletionResult {
        return complete_decoded_event(decode_completion_event(event), result, event.param);
    }

    [[nodiscard]] auto try_timeout(RequestHandle handle, uint64_t now_us, int32_t timeout_result) -> TimeoutDisposition {
        auto* record = find_record(handle);
        if (record == nullptr) {
            return TimeoutDisposition::STALE_HANDLE;
        }

        uint64_t const DEADLINE_US = record->deadline_us.load(std::memory_order_acquire);
        if (DEADLINE_US == 0) {
            return TimeoutDisposition::NOT_ARMED;
        }
        if (now_us < DEADLINE_US) {
            return TimeoutDisposition::NOT_DUE;
        }

        return transition_to_timed_out(*record, timeout_result);
    }

    // Task-context cancellation does not depend on the request deadline. It
    // retains the same DMA quarantine as a deadline timeout: the caller must
    // establish a controller-specific retirement barrier before release().
    [[nodiscard]] auto cancel(RequestHandle handle, int32_t cancel_result) -> TimeoutDisposition {
        auto* record = find_record(handle);
        if (record == nullptr) {
            return TimeoutDisposition::STALE_HANDLE;
        }
        return transition_to_timed_out(*record, cancel_result);
    }

    // Scans exactly Capacity fixed records. The optional sink runs only after
    // the timed-out result has been release-published.
    [[nodiscard]] auto expire_due(uint64_t now_us, int32_t timeout_result, TimeoutSink sink = nullptr, void* context = nullptr)
        -> TimeoutScanResult {
        TimeoutScanResult scan{};
        for (size_t index = 0; index < Capacity; ++index) {
            ++scan.examined;
            auto& record = records_.at(index);
            if (record.state.load(std::memory_order_acquire) != RequestState::SUBMITTED) {
                continue;
            }
            RequestHandle const HANDLE = {.index = index, .generation = record.generation.load(std::memory_order_acquire)};
            if (try_timeout(HANDLE, now_us, timeout_result) != TimeoutDisposition::TIMED_OUT) {
                continue;
            }
            ++scan.timed_out;
            if (sink != nullptr) {
                sink(context, HANDLE);
            }
        }
        return scan;
    }

    // Timeout alone does not return DMA ownership. The integration layer must
    // call this only after an endpoint stop/dequeue, command-ring recovery, an
    // exact late event, or another controller-specific retirement barrier.
    [[nodiscard]] auto mark_timed_out_retired(RequestHandle handle) -> bool {
        auto* record = find_record(handle);
        if (record == nullptr) {
            return false;
        }
        RequestState expected = RequestState::TIMED_OUT;
        return record->state.compare_exchange_strong(expected, RequestState::RETIRED, std::memory_order_acq_rel, std::memory_order_acquire);
    }

    // Completed requests may be released immediately. Timed-out requests must
    // first pass through RETIRED, which prevents accidental DMA reuse.
    [[nodiscard]] auto release(RequestHandle handle) -> bool {
        auto* record = find_record(handle);
        if (record == nullptr) {
            return false;
        }

        RequestState state = record->state.load(std::memory_order_acquire);
        while (state == RequestState::COMPLETED || state == RequestState::RETIRED) {
            if (record->state.compare_exchange_weak(state, RequestState::RELEASING, std::memory_order_acq_rel, std::memory_order_acquire)) {
                clear_record(*record);
                record->state.store(RequestState::FREE, std::memory_order_release);
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] auto snapshot(RequestHandle handle, RequestSnapshot& out) const -> bool {
        auto const* record = find_record(handle);
        if (record == nullptr) {
            return false;
        }

        RequestState const STATE = record->state.load(std::memory_order_acquire);
        if (STATE == RequestState::FREE || STATE == RequestState::RESERVED || STATE == RequestState::COMPLETING ||
            STATE == RequestState::TIMING_OUT || STATE == RequestState::RELEASING) {
            return false;
        }

        out.state = STATE;
        out.kind = record->kind.load(std::memory_order_relaxed);
        out.generation = record->generation.load(std::memory_order_relaxed);
        out.slot_id = record->slot_id.load(std::memory_order_relaxed);
        out.dci = record->dci.load(std::memory_order_relaxed);
        out.requested_length = record->requested_length.load(std::memory_order_relaxed);
        out.deadline_us = record->deadline_us.load(std::memory_order_relaxed);
        out.result = record->result.load(std::memory_order_relaxed);
        out.completion_code = record->completion_code.load(std::memory_order_relaxed);
        out.residual = record->residual.load(std::memory_order_relaxed);
        out.event_slot_id = record->event_slot_id.load(std::memory_order_relaxed);
        out.event_dci = record->event_dci.load(std::memory_order_relaxed);
        out.event_parameter = record->event_parameter.load(std::memory_order_relaxed);

        std::atomic_thread_fence(std::memory_order_acquire);
        return record->generation.load(std::memory_order_relaxed) == handle.generation &&
               record->state.load(std::memory_order_relaxed) == STATE;
    }

    [[nodiscard]] static constexpr auto capacity() -> size_t { return Capacity; }

   private:
    struct RequestRecord {
        std::atomic<RequestState> state{RequestState::FREE};
        std::atomic<RequestKind> kind{RequestKind::COMMAND};
        std::atomic<uint32_t> generation{0};
        std::atomic<uint8_t> slot_id{0};
        std::atomic<uint8_t> dci{0};
        std::atomic<uint8_t> trb_count{0};
        std::array<std::atomic<uint64_t>, MaxRequestTrbs> trb_phys{};
        std::atomic<bool> event_data_valid{false};
        std::atomic<uint64_t> event_data{0};
        std::atomic<uint32_t> requested_length{0};
        std::atomic<uint64_t> deadline_us{0};
        std::atomic<int32_t> result{DEFAULT_PENDING_RESULT};
        std::atomic<uint32_t> completion_code{0};
        std::atomic<uint32_t> residual{0};
        std::atomic<uint8_t> event_slot_id{0};
        std::atomic<uint8_t> event_dci{0};
        std::atomic<uint64_t> event_parameter{0};
    };

    [[nodiscard]] static auto transition_to_timed_out(RequestRecord& record, int32_t timeout_result) -> TimeoutDisposition {
        RequestState expected = RequestState::SUBMITTED;
        if (!record.state.compare_exchange_strong(expected, RequestState::TIMING_OUT, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
            return timeout_disposition_for_state(expected);
        }

        record.result.store(timeout_result, std::memory_order_relaxed);
        record.completion_code.store(0, std::memory_order_relaxed);
        record.residual.store(0, std::memory_order_relaxed);
        record.event_slot_id.store(0, std::memory_order_relaxed);
        record.event_dci.store(0, std::memory_order_relaxed);
        record.event_parameter.store(0, std::memory_order_relaxed);
        record.state.store(RequestState::TIMED_OUT, std::memory_order_release);
        return TimeoutDisposition::TIMED_OUT;
    }

    [[nodiscard]] static auto valid_spec(const RequestSpec& spec) -> bool {
        if (spec.trb_phys.empty() || spec.trb_phys.size() > MaxRequestTrbs) {
            return false;
        }
        if (spec.kind == RequestKind::COMMAND && (spec.trb_phys.size() != 1 || spec.event_data_valid)) {
            return false;
        }
        if (spec.kind == RequestKind::TRANSFER && (spec.slot_id == 0 || spec.dci == 0 || spec.dci > 31)) {
            return false;
        }
        for (size_t i = 0; i < spec.trb_phys.size(); ++i) {
            if (spec.trb_phys[i] == 0 || (spec.trb_phys[i] & ~TRB_POINTER_MASK) != 0) {
                return false;
            }
            for (size_t j = 0; j < i; ++j) {
                if (spec.trb_phys[i] == spec.trb_phys[j]) {
                    return false;
                }
            }
        }
        return true;
    }

    static void reset_result(RequestRecord& record) {
        record.result.store(DEFAULT_PENDING_RESULT, std::memory_order_relaxed);
        record.completion_code.store(0, std::memory_order_relaxed);
        record.residual.store(0, std::memory_order_relaxed);
        record.event_slot_id.store(0, std::memory_order_relaxed);
        record.event_dci.store(0, std::memory_order_relaxed);
        record.event_parameter.store(0, std::memory_order_relaxed);
    }

    static void clear_record(RequestRecord& record) {
        record.slot_id.store(0, std::memory_order_relaxed);
        record.dci.store(0, std::memory_order_relaxed);
        record.trb_count.store(0, std::memory_order_relaxed);
        for (auto& trb_phys : record.trb_phys) {
            trb_phys.store(0, std::memory_order_relaxed);
        }
        record.event_data_valid.store(false, std::memory_order_relaxed);
        record.event_data.store(0, std::memory_order_relaxed);
        record.requested_length.store(0, std::memory_order_relaxed);
        record.deadline_us.store(0, std::memory_order_relaxed);
        reset_result(record);
    }

    [[nodiscard]] auto find_record(RequestHandle handle) -> RequestRecord* {
        if (!handle.valid() || handle.index >= Capacity) {
            return nullptr;
        }
        auto& record = records_.at(handle.index);
        if (record.generation.load(std::memory_order_acquire) != handle.generation) {
            return nullptr;
        }
        return &record;
    }

    [[nodiscard]] auto find_record(RequestHandle handle) const -> const RequestRecord* {
        if (!handle.valid() || handle.index >= Capacity) {
            return nullptr;
        }
        auto const& record = records_.at(handle.index);
        if (record.generation.load(std::memory_order_acquire) != handle.generation) {
            return nullptr;
        }
        return &record;
    }

    [[nodiscard]] static auto timeout_disposition_for_state(RequestState state) -> TimeoutDisposition {
        switch (state) {
            case RequestState::COMPLETING:
            case RequestState::COMPLETED:
                return TimeoutDisposition::COMPLETION_WON;
            case RequestState::TIMING_OUT:
            case RequestState::TIMED_OUT:
            case RequestState::RETIRED:
                return TimeoutDisposition::ALREADY_TIMED_OUT;
            case RequestState::FREE:
            case RequestState::RESERVED:
            case RequestState::RELEASING:
            case RequestState::SUBMITTED:
            default:
                return TimeoutDisposition::STALE_HANDLE;
        }
    }

    [[nodiscard]] static auto record_matches_event(const RequestRecord& record, const DecodedCompletionEvent& event) -> bool {
        if (record.kind.load(std::memory_order_acquire) != event.kind) {
            return false;
        }
        if (event.kind == RequestKind::TRANSFER) {
            if (record.slot_id.load(std::memory_order_acquire) != event.slot_id ||
                record.dci.load(std::memory_order_acquire) != event.dci) {
                return false;
            }
            if (event.uses_event_data) {
                return record.event_data_valid.load(std::memory_order_acquire) &&
                       record.event_data.load(std::memory_order_acquire) == event.identity;
            }
        } else if (event.uses_event_data) {
            return false;
        }

        size_t const COUNT = record.trb_count.load(std::memory_order_acquire);
        for (size_t index = 0; index < COUNT && index < MaxRequestTrbs; ++index) {
            if (record.trb_phys.at(index).load(std::memory_order_acquire) == event.identity) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] auto complete_decoded_event(const DecodedCompletionEvent& event, int32_t result, uint64_t raw_parameter)
        -> CompletionResult {
        if (!event.valid) {
            return {.disposition = CompletionDisposition::UNSUPPORTED_EVENT};
        }

        size_t match_count = 0;
        size_t match_index = 0;
        RequestState match_state = RequestState::FREE;
        uint32_t match_generation = 0;
        for (size_t index = 0; index < Capacity; ++index) {
            auto& record = records_.at(index);
            RequestState const STATE = record.state.load(std::memory_order_acquire);
            if (STATE == RequestState::FREE || STATE == RequestState::RESERVED || STATE == RequestState::RELEASING ||
                !record_matches_event(record, event)) {
                continue;
            }
            ++match_count;
            match_index = index;
            match_state = STATE;
            match_generation = record.generation.load(std::memory_order_acquire);
        }

        if (match_count == 0) {
            return {.disposition = CompletionDisposition::UNKNOWN};
        }
        if (match_count != 1) {
            return {.disposition = CompletionDisposition::AMBIGUOUS};
        }

        RequestHandle const HANDLE = {.index = match_index, .generation = match_generation};
        auto& record = records_.at(match_index);
        if (match_state != RequestState::SUBMITTED) {
            return {.disposition = completion_disposition_for_state(match_state), .request = HANDLE};
        }

        RequestState expected = RequestState::SUBMITTED;
        if (!record.state.compare_exchange_strong(expected, RequestState::COMPLETING, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
            return {.disposition = completion_disposition_for_state(expected), .request = HANDLE};
        }

        record.result.store(result, std::memory_order_relaxed);
        record.completion_code.store(event.completion_code, std::memory_order_relaxed);
        record.residual.store(event.residual, std::memory_order_relaxed);
        record.event_slot_id.store(event.slot_id, std::memory_order_relaxed);
        record.event_dci.store(event.dci, std::memory_order_relaxed);
        record.event_parameter.store(raw_parameter, std::memory_order_relaxed);
        record.state.store(RequestState::COMPLETED, std::memory_order_release);
        return {.disposition = CompletionDisposition::COMPLETED, .request = HANDLE};
    }

    [[nodiscard]] static auto completion_disposition_for_state(RequestState state) -> CompletionDisposition {
        switch (state) {
            case RequestState::TIMING_OUT:
            case RequestState::TIMED_OUT:
            case RequestState::RETIRED:
                return CompletionDisposition::LATE_AFTER_TIMEOUT;
            case RequestState::COMPLETING:
            case RequestState::COMPLETED:
                return CompletionDisposition::DUPLICATE;
            case RequestState::FREE:
            case RequestState::RESERVED:
            case RequestState::RELEASING:
            case RequestState::SUBMITTED:
            default:
                return CompletionDisposition::UNKNOWN;
        }
    }

    std::array<RequestRecord, Capacity> records_{};
};

struct EventRingCursor {
    size_t dequeue = 0;
    bool cycle = true;
};

struct EventRingDrainResult {
    bool valid = false;
    size_t consumed = 0;
    bool more_ready = false;
    bool budget_exhausted = false;
};

using EventSink = void (*)(void* context, const Trb& event);

[[nodiscard]] inline auto event_ring_entry_ready(const Trb& event, bool expected_cycle) -> bool {
    return ((event.control & TRB_CYCLE) != 0) == expected_cycle;
}

// Consumes at most min(budget, ring_size) entries, so even a concurrently
// refilled ring cannot make one IRQ invocation unbounded. MMIO acknowledgement
// and ERDP publication deliberately remain the caller's responsibility.
[[nodiscard]] inline auto consume_event_ring(const Trb* ring, size_t ring_size, EventRingCursor& cursor, size_t budget,
                                             EventSink sink = nullptr, void* context = nullptr) -> EventRingDrainResult {
    EventRingDrainResult result{};
    if (ring == nullptr || ring_size == 0 || cursor.dequeue >= ring_size) {
        return result;
    }
    result.valid = true;

    size_t const LIMIT = budget < ring_size ? budget : ring_size;
    while (result.consumed < LIMIT) {
        auto const& entry = ring[cursor.dequeue];
        uint32_t const FIRST_CONTROL = entry.control;
        if (((FIRST_CONTROL & TRB_CYCLE) != 0) != cursor.cycle) {
            break;
        }

        // The controller publishes an event by writing its cycle bit last.
        // Pair that observation with an acquire fence before copying payload.
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
        Trb const EVENT = {.param = entry.param, .status = entry.status, .control = entry.control};
        if (!event_ring_entry_ready(EVENT, cursor.cycle)) {
            break;
        }

        if (sink != nullptr) {
            sink(context, EVENT);
        }
        ++result.consumed;
        ++cursor.dequeue;
        if (cursor.dequeue == ring_size) {
            cursor.dequeue = 0;
            cursor.cycle = !cursor.cycle;
        }
    }

    result.more_ready = event_ring_entry_ready(ring[cursor.dequeue], cursor.cycle);
    result.budget_exhausted = LIMIT != 0 && result.consumed == LIMIT && result.more_ready;
    return result;
}

}  // namespace ker::dev::usb::xhci_request
