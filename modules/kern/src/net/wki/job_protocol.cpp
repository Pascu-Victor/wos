#include "job_protocol.hpp"

#include <cstring>
#include <net/wki/auth_policy.hpp>

namespace ker::net::wki::jobs::protocol {
namespace {
namespace credential = ker::mod::security::credential_record;

template <typename T>
auto read(std::span<const uint8_t> bytes, T& value) -> bool {
    if (bytes.size() < sizeof(T)) return false;
    std::memcpy(&value, bytes.data(), sizeof(T));
    return true;
}

auto header_valid(const Header& header, Kind kind, size_t size) -> Decode {
    if (header.magic != MAGIC || header.version != VERSION) return Decode::VERSION;
    if (size > WKI_ETH_MAX_PAYLOAD || header.size != size) return Decode::LENGTH;
    if (header.reserved != 0 || header.flags != 0) return Decode::FLAGS;
    return header.kind == static_cast<uint16_t>(kind) ? Decode::OK : Decode::FIELD;
}

auto key_value(const KeyWire& wire, Key& out) -> bool {
    Key value{{{wire.node, wire.uid}, wire.submitter_incarnation}, wire.serial, wire.receiver_incarnation, wire.attempt};
    if (wire.reserved != 0 || wire.reserved2 != 0 || !value.valid()) return false;
    out = value;
    return true;
}

auto operation_valid(uint16_t operation) -> bool {
    return operation >= static_cast<uint16_t>(Operation::INFO) && operation <= static_cast<uint16_t>(Operation::SIGNAL_ACK);
}

auto window_valid(uint64_t consumed, uint64_t window) -> bool { return window <= MAX_STREAM_CAPACITY && window <= UINT64_MAX - consumed; }

auto request_valid(const Request& fields, std::span<const uint8_t> body, RequestView& out) -> Decode {
    auto const HEADER = header_valid(fields.prefix.header, Kind::REQUEST, sizeof(fields) + body.size());
    if (HEADER != Decode::OK) return HEADER;
    if (fields.prefix.correlation == 0 || fields.reserved != 0 || fields.reserved2 != 0 || !operation_valid(fields.operation))
        return Decode::FIELD;
    RequestView next{};
    next.operation = static_cast<Operation>(fields.operation);
    next.correlation = fields.prefix.correlation;
    next.service = fields.prefix.service;
    if (next.operation == Operation::INFO) {
        if (fields.prefix.key != KeyWire{} || next.service != 0) return Decode::FIELD;
    } else if (next.service == 0 || !key_value(fields.prefix.key, next.key)) {
        return Decode::FIELD;
    }
    if (credential::decode(fields.credentials, next.credentials) != 0) return Decode::CREDENTIAL;
    switch (next.operation) {
        case Operation::INFO:
            if (body.size() != sizeof(InfoQuery)) return Decode::LENGTH;
            if (!read(body, next.info) || next.info.client_incarnation == 0 || next.info.reserved != 0 ||
                (next.info.required_features & ~FEATURES) != 0)
                return Decode::FIELD;
            break;
        case Operation::SUBMIT: {
            Submit submit{};
            if (!read(body, submit)) return Decode::SHORT;
            if (submit.version != VERSION) return Decode::VERSION;
            if (submit.reserved != 0 || submit.size == 0 || submit.size > MAX_PLAN) return Decode::FIELD;
            if (body.size() - sizeof(submit) != submit.size) return Decode::LENGTH;
            next.plan = body.subspan(sizeof(submit));
            break;
        }
        case Operation::ATTACH:
            if (body.size() != sizeof(Attach)) return Decode::LENGTH;
            if (!read(body, next.attach) || !window_valid(next.attach.stdout_consumed, next.attach.stdout_window) ||
                !window_valid(next.attach.stderr_consumed, next.attach.stderr_window))
                return Decode::FIELD;
            break;
        case Operation::DETACH: {
            AttachmentId id{};
            if (body.size() != sizeof(id)) return Decode::LENGTH;
            if (!read(body, id) || id.attachment == 0) return Decode::FIELD;
            next.attachment = id.attachment;
            break;
        }
        case Operation::SIGNAL: {
            Signal signal{};
            if (body.size() != sizeof(signal)) return Decode::LENGTH;
            if (!read(body, signal) || signal.serial == 0 || signal.signal == 0 || signal.signal > MAX_SIGNAL || signal.reserved != 0)
                return Decode::FIELD;
            next.control_serial = signal.serial;
            next.signal = signal.signal;
            break;
        }
        case Operation::SIGNAL_STATUS:
        case Operation::SIGNAL_ACK: {
            ControlId id{};
            if (body.size() != sizeof(id)) return Decode::LENGTH;
            if (!read(body, id) || id.serial == 0) return Decode::FIELD;
            next.control_serial = id.serial;
            break;
        }
        case Operation::COMMIT:
        case Operation::STATUS:
        case Operation::CANCEL:
            if (!body.empty()) return Decode::LENGTH;
            break;
    }
    out = next;
    return Decode::OK;
}

auto info_valid(const Info& info) -> bool {
    if (info.client_incarnation == 0 || (info.available_features & ~FEATURES) != 0 ||
        (info.granted_features & ~info.available_features) != 0)
        return false;
    if (info.available_features == 0)
        return info.granted_features == 0 && info.max_plan == 0 && info.stream_capacity == 0 && info.records == 0 && info.scopes == 0 &&
               info.signal_receipts == 0 && info.max_signal == 0 && info.byte_limit == 0 && info.proposal_ttl_us == 0 &&
               info.result_ttl_us == 0;
    if ((info.available_features & CORE) != CORE || (info.granted_features != 0 && (info.granted_features & CORE) != CORE) ||
        info.max_plan == 0 || info.max_plan > MAX_PLAN || info.stream_capacity == 0 || info.stream_capacity > MAX_STREAM_CAPACITY ||
        info.records == 0 || info.records > MAX_RECORDS || info.scopes == 0 || info.scopes > MAX_SCOPES || info.byte_limit == 0 ||
        info.byte_limit > MAX_RESERVED_BYTES || info.proposal_ttl_us == 0 || info.result_ttl_us == 0)
        return false;
    if ((info.available_features & SIGNALS) == 0) return info.signal_receipts == 0 && info.max_signal == 0;
    return info.signal_receipts != 0 && info.signal_receipts <= MAX_SIGNAL_RECEIPTS && info.max_signal != 0 &&
           info.max_signal <= MAX_SIGNAL;
}

auto response_valid(const Response& fields, std::span<const uint8_t> body, ResponseView& out) -> Decode {
    auto const HEADER = header_valid(fields.prefix.header, Kind::RESPONSE, sizeof(fields) + body.size());
    if (HEADER != Decode::OK) return HEADER;
    if (fields.prefix.correlation == 0 || !operation_valid(fields.operation) || fields.state > static_cast<uint8_t>(State::LOST) ||
        fields.reason > static_cast<uint16_t>(Reason::CONTROL_EXPIRED) ||
        fields.control_state > static_cast<uint16_t>(ControlState::DONE) || (fields.flags & ~(NATIVE_EXIT | DETACHED)) != 0 ||
        fields.status > 0 || fields.stdout_retained > fields.stdout_produced || fields.stderr_retained > fields.stderr_produced)
        return Decode::FIELD;
    ResponseView next{};
    next.fields = fields;
    auto const OPERATION = static_cast<Operation>(fields.operation);
    auto const STATE = static_cast<State>(fields.state);
    auto const REASON = static_cast<Reason>(fields.reason);
    bool const HAS_EXIT = (fields.flags & NATIVE_EXIT) != 0;
    if (!HAS_EXIT && fields.exit_status != 0) return Decode::FIELD;
    if (OPERATION == Operation::INFO) {
        if (REASON != Reason::OK && REASON != Reason::UNSUPPORTED && REASON != Reason::DENIED && REASON != Reason::INVALID &&
            REASON != Reason::QUOTA)
            return Decode::FIELD;
        if (body.size() != sizeof(Info)) return Decode::LENGTH;
        if (fields.prefix.key != KeyWire{} || STATE != State::UNKNOWN || fields.flags != 0 || fields.pid != 0 || fields.attachment != 0 ||
            fields.stdout_retained != 0 || fields.stdout_produced != 0 || fields.stderr_retained != 0 || fields.stderr_produced != 0 ||
            fields.control_serial != 0 || fields.control_state != 0 || !read(body, next.info) || !info_valid(next.info))
            return Decode::FIELD;
        if (next.info.available_features != 0 && (fields.prefix.service == 0 || fields.receiver_incarnation == 0)) return Decode::FIELD;
        if (REASON == Reason::OK && (next.info.granted_features & CORE) != CORE) return Decode::FIELD;
    } else {
        if (!body.empty()) return Decode::LENGTH;
        if (fields.prefix.service == 0 || !key_value(fields.prefix.key, next.key) || fields.receiver_incarnation == 0) return Decode::FIELD;
        if (STATE == State::LOST) {
            if (REASON != Reason::LOST || fields.receiver_incarnation == next.key.receiver_incarnation) return Decode::FIELD;
        } else if (fields.receiver_incarnation != next.key.receiver_incarnation) {
            return Decode::FIELD;
        }
        if ((STATE == State::UNKNOWN && REASON == Reason::OK) || (STATE == State::EXPIRED) != (REASON == Reason::EXPIRED) ||
            (STATE == State::LOST) != (REASON == Reason::LOST) || (REASON == Reason::UNKNOWN && STATE != State::UNKNOWN) ||
            (REASON == Reason::CANCELED && STATE != State::CANCELED))
            return Decode::FIELD;
        if (STATE == State::UNKNOWN || STATE == State::LOST || STATE == State::EXPIRED) {
            if (fields.pid != 0 || fields.attachment != 0 || fields.flags != 0 || fields.stdout_retained != 0 ||
                fields.stdout_produced != 0 || fields.stderr_retained != 0 || fields.stderr_produced != 0 || fields.control_state != 0)
                return Decode::FIELD;
        } else if (STATE == State::RUNNING) {
            if (fields.pid == 0 || HAS_EXIT) return Decode::FIELD;
        } else if (STATE == State::TERMINAL || STATE == State::CANCELED) {
            if (HAS_EXIT != (fields.pid != 0)) return Decode::FIELD;
        } else if (fields.pid != 0 || HAS_EXIT || fields.stdout_produced != 0 || fields.stderr_produced != 0) {
            return Decode::FIELD;
        }
        bool const CONTROL_OPERATION =
            OPERATION == Operation::SIGNAL || OPERATION == Operation::SIGNAL_STATUS || OPERATION == Operation::SIGNAL_ACK;
        if (!CONTROL_OPERATION && (fields.control_serial != 0 || fields.control_state != 0)) return Decode::FIELD;
        if (CONTROL_OPERATION && fields.control_serial == 0) return Decode::FIELD;
        if (REASON == Reason::CONTROL_PENDING &&
            (!CONTROL_OPERATION || (fields.control_state != static_cast<uint16_t>(ControlState::PROPOSED) &&
                                    fields.control_state != static_cast<uint16_t>(ControlState::CLAIMED))))
            return Decode::FIELD;
        if (REASON == Reason::CONTROL_EXPIRED && (!CONTROL_OPERATION || fields.control_state != 0)) return Decode::FIELD;
    }
    switch (REASON) {
        case Reason::UNSUPPORTED:
        case Reason::DENIED:
        case Reason::INVALID:
        case Reason::CONFLICT:
        case Reason::QUOTA:
        case Reason::BAD_STATE:
        case Reason::OUTPUT_GAP:
        case Reason::LAUNCH_FAILED:
            if (fields.status == 0) return Decode::FIELD;
            break;
        case Reason::UNKNOWN:
        case Reason::LOST:
        case Reason::EXPIRED:
        case Reason::CANCELED:
        case Reason::CONTROL_PENDING:
        case Reason::CONTROL_EXPIRED:
            if (fields.status != 0) return Decode::FIELD;
            break;
        case Reason::OK:
            // DONE may carry a checked negative native signal result.
            if (fields.status != 0 && fields.control_state != static_cast<uint16_t>(ControlState::DONE)) return Decode::FIELD;
            break;
    }
    out = next;
    return Decode::OK;
}

auto output_valid(const Output& fields, std::span<const uint8_t> body, OutputView& out) -> Decode {
    auto const HEADER = header_valid(fields.prefix.header, Kind::OUTPUT, sizeof(fields) + body.size());
    if (HEADER != Decode::OK) return HEADER;
    OutputView next{};
    if (fields.prefix.correlation != 0 || fields.prefix.service == 0 || !key_value(fields.prefix.key, next.key) || fields.attachment == 0 ||
        fields.stream < 1 || fields.stream > 2 || fields.reserved != 0 || (fields.flags & ~OUTPUT_END) != 0 ||
        fields.size > UINT64_MAX - fields.offset || (fields.size == 0 && (fields.flags & OUTPUT_END) == 0))
        return Decode::FIELD;
    if (body.size() != fields.size) return Decode::LENGTH;
    next.fields = fields;
    next.bytes = body;
    out = next;
    return Decode::OK;
}

auto credit_valid(const Credit& fields, CreditView& out) -> Decode {
    auto const HEADER = header_valid(fields.request.prefix.header, Kind::CREDIT, sizeof(fields));
    if (HEADER != Decode::OK) return HEADER;
    CreditView next{};
    if (fields.request.prefix.correlation != 0 || fields.request.prefix.service == 0 || !key_value(fields.request.prefix.key, next.key) ||
        fields.request.operation != 0 || fields.request.reserved != 0 || fields.request.reserved2 != 0 || fields.attachment == 0 ||
        fields.revision == 0 || fields.stream < 1 || fields.stream > 2 || !window_valid(fields.consumed, fields.window))
        return Decode::FIELD;
    for (auto byte : fields.reserved) {
        if (byte != 0) return Decode::FLAGS;
    }
    if (credential::decode(fields.request.credentials, next.credentials) != 0) return Decode::CREDENTIAL;
    next.fields = fields;
    out = next;
    return Decode::OK;
}

template <typename T, typename View, typename Validator>
auto encode(T fields, Kind kind, std::span<const uint8_t> body, std::span<uint8_t> out, size_t& written, Validator validate) -> Decode {
    if (body.size() > WKI_ETH_MAX_PAYLOAD - sizeof(T)) return Decode::LENGTH;
    auto const SIZE = sizeof(T) + body.size();
    fields.prefix.header.kind = static_cast<uint16_t>(kind);
    fields.prefix.header.size = static_cast<uint16_t>(SIZE);
    View view{};
    auto const RESULT = validate(fields, body, view);
    if (RESULT != Decode::OK) return RESULT;
    if (out.size() < SIZE) return Decode::SHORT;
    // Body first: it may alias either the future header or payload in out.
    if (!body.empty()) std::memmove(out.data() + sizeof(T), body.data(), body.size());
    std::memcpy(out.data(), &fields, sizeof(T));
    written = SIZE;
    return Decode::OK;
}
}  // namespace

auto key_wire(const Key& key) -> KeyWire {
    return {.node = key.scope.owner.node,
            .uid = key.scope.owner.uid,
            .submitter_incarnation = key.scope.submitter_incarnation,
            .serial = key.serial,
            .receiver_incarnation = key.receiver_incarnation,
            .attempt = key.attempt};
}

auto decode_request(std::span<const uint8_t> bytes, RequestView& out) -> Decode {
    Request fields{};
    if (!read(bytes, fields)) return Decode::SHORT;
    return request_valid(fields, bytes.subspan(sizeof(fields)), out);
}
auto decode_response(std::span<const uint8_t> bytes, ResponseView& out) -> Decode {
    Response fields{};
    if (!read(bytes, fields)) return Decode::SHORT;
    return response_valid(fields, bytes.subspan(sizeof(fields)), out);
}
auto decode_output(std::span<const uint8_t> bytes, OutputView& out) -> Decode {
    Output fields{};
    if (!read(bytes, fields)) return Decode::SHORT;
    return output_valid(fields, bytes.subspan(sizeof(fields)), out);
}
auto decode_credit(std::span<const uint8_t> bytes, CreditView& out) -> Decode {
    Credit fields{};
    if (!read(bytes, fields)) return Decode::SHORT;
    if (bytes.size() != sizeof(fields)) return Decode::LENGTH;
    return credit_valid(fields, out);
}

auto encode_request(Request fields, std::span<const uint8_t> body, std::span<uint8_t> out, size_t& written) -> Decode {
    return encode<Request, RequestView>(fields, Kind::REQUEST, body, out, written, request_valid);
}
auto encode_response(Response fields, std::span<const uint8_t> body, std::span<uint8_t> out, size_t& written) -> Decode {
    return encode<Response, ResponseView>(fields, Kind::RESPONSE, body, out, written, response_valid);
}
auto encode_output(Output fields, std::span<const uint8_t> body, std::span<uint8_t> out, size_t& written) -> Decode {
    return encode<Output, OutputView>(fields, Kind::OUTPUT, body, out, written, output_valid);
}
auto encode_credit(Credit fields, std::span<uint8_t> out, size_t& written) -> Decode {
    fields.request.prefix.header.kind = static_cast<uint16_t>(Kind::CREDIT);
    fields.request.prefix.header.size = sizeof(fields);
    CreditView view{};
    auto const RESULT = credit_valid(fields, view);
    if (RESULT != Decode::OK) return RESULT;
    if (out.size() < sizeof(fields)) return Decode::SHORT;
    std::memcpy(out.data(), &fields, sizeof(fields));
    written = sizeof(fields);
    return Decode::OK;
}

auto response_matches(const RequestView& request, const ResponseView& response) -> bool {
    if (request.correlation != response.fields.prefix.correlation ||
        static_cast<uint16_t>(request.operation) != response.fields.operation || request.key != response.key ||
        request.control_serial != response.fields.control_serial)
        return false;
    if (request.operation != Operation::INFO) return request.service == response.fields.prefix.service;
    return request.info.client_incarnation == response.info.client_incarnation &&
           (response.fields.reason != static_cast<uint16_t>(Reason::OK) ||
            (response.info.granted_features & request.info.required_features) == request.info.required_features);
}

auto map_owner(uint16_t authenticated_node, uint64_t policy, const Key& key, const ker::mod::security::Snapshot& supplied,
               ker::mod::security::Snapshot& mapped) -> int {
    if (!key.valid() || key.scope.owner.node != authenticated_node) return -EACCES;
    ker::mod::security::Snapshot next{};
    int const RESULT = auth_policy::map_compute_identity(policy, supplied, next);
    if (RESULT != 0) return RESULT;
    if (next.user().effective != key.scope.owner.uid) return -EACCES;
    mapped = next;
    return 0;
}
}  // namespace ker::net::wki::jobs::protocol
