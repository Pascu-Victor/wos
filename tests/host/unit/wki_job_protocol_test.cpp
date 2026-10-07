#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <net/wki/auth.hpp>
#include <net/wki/job_protocol.hpp>
#include <random>
#include <vector>

namespace {
namespace jobs = ker::net::wki::jobs;
namespace p = jobs::protocol;
namespace security = ker::mod::security;
namespace credential = security::credential_record;
using namespace ker::net::wki;
constexpr jobs::Key KEY{{{7, 1000}, 0x1122334455667788}, 23, 0x8877665544332211, 1};
constexpr uint64_t SERVICE = 987;
constexpr uint64_t POLICY = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);

auto user() -> security::Snapshot {
    security::Snapshot value;
    std::array<uint32_t, 2> groups{100, 200};
    EXPECT_EQ(security::Snapshot::restore_fields({.user = {1001, 1000, 1002},
                                                  .group = {20, 21, 22},
                                                  .groups = groups,
                                                  .authority_ceiling = security::ALL_AUTHORITY,
                                                  .no_new_privileges = true,
                                                  .dumpable = true},
                                                 value),
              0);
    return value;
}

template <typename T>
auto bytes_of(const T& value) -> std::span<const uint8_t> {
    return {reinterpret_cast<const uint8_t*>(&value), sizeof(value)};
}
template <typename T>
auto raw(const T& fields, std::span<const uint8_t> body = {}) -> std::vector<uint8_t> {
    std::vector<uint8_t> bytes(sizeof(fields) + body.size());
    std::memcpy(bytes.data(), &fields, sizeof(fields));
    if (!body.empty()) std::memcpy(bytes.data() + sizeof(fields), body.data(), body.size());
    return bytes;
}

auto request(p::Operation operation = p::Operation::STATUS) -> p::Request {
    p::Request fields{};
    fields.prefix.correlation = 123;
    fields.prefix.service = SERVICE;
    fields.prefix.key = p::key_wire(KEY);
    fields.operation = static_cast<uint16_t>(operation);
    fields.credentials = credential::encode(user());
    return fields;
}
auto response() -> p::Response {
    p::Response fields{};
    fields.prefix = request().prefix;
    fields.operation = static_cast<uint16_t>(p::Operation::STATUS);
    fields.state = static_cast<uint8_t>(p::State::RUNNING);
    fields.pid = 55;
    fields.receiver_incarnation = KEY.receiver_incarnation;
    return fields;
}
auto output() -> p::Output {
    p::Output fields{};
    fields.prefix = request().prefix;
    fields.prefix.correlation = 0;
    fields.attachment = 9;
    fields.offset = 42;
    fields.size = 3;
    fields.stream = 1;
    return fields;
}
auto credit() -> p::Credit {
    p::Credit fields{};
    fields.request = request();
    fields.request.prefix.correlation = 0;
    fields.request.operation = 0;
    fields.attachment = 9;
    fields.revision = 1;
    fields.consumed = 42;
    fields.window = 64;
    fields.stream = 2;
    return fields;
}
auto info() -> p::Info {
    return {.client_incarnation = 17,
            .available_features = p::FEATURES,
            .granted_features = p::FEATURES,
            .max_plan = p::MAX_PLAN,
            .stream_capacity = 4096,
            .records = 16,
            .scopes = 64,
            .signal_receipts = 8,
            .max_signal = 64,
            .byte_limit = 1024 * 1024,
            .proposal_ttl_us = 1000,
            .result_ttl_us = 2000};
}

template <typename Fields, typename Encoder>
auto frame(Fields fields, std::span<const uint8_t> body, Encoder encode) -> std::vector<uint8_t> {
    std::vector<uint8_t> bytes(WKI_ETH_MAX_PAYLOAD);
    size_t written = 0;
    EXPECT_EQ(encode(fields, body, bytes, written), p::Decode::OK);
    bytes.resize(written);
    return bytes;
}
auto credit_frame(p::Credit fields) -> std::vector<uint8_t> {
    std::vector<uint8_t> bytes(sizeof(fields));
    size_t written = 0;
    EXPECT_EQ(p::encode_credit(fields, bytes, written), p::Decode::OK);
    EXPECT_EQ(written, sizeof(fields));
    return bytes;
}

template <typename View, typename Decoder>
void reject_without_assignment(std::span<const uint8_t> bytes, Decoder decode) {
    View view{};
    view.key = KEY;
    view.key.serial = UINT64_MAX;
    EXPECT_NE(decode(bytes, view), p::Decode::OK);
    EXPECT_EQ(view.key.serial, UINT64_MAX);
}
}  // namespace

TEST(WkiJobProtocol, GoldenHeaderIdentityAndCanonicalCredential) {
    auto bytes = frame(request(), {}, p::encode_request);
    std::array<uint8_t, 16> const HEADER{0x4A, 0x4F, 0x42, 0x31, 1, 0, 1, 0, 160, 0, 0, 0, 0, 0, 0, 0};
    EXPECT_EQ(std::vector<uint8_t>(bytes.begin(), bytes.begin() + 16), std::vector<uint8_t>(HEADER.begin(), HEADER.end()));
    std::array<uint8_t, 40> const KEY_BYTES{7,    0,    0,    0,    0xE8, 3, 0, 0, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33,
                                            0x22, 0x11, 23,   0,    0,    0, 0, 0, 0,    0,    0x11, 0x22, 0x33, 0x44,
                                            0x55, 0x66, 0x77, 0x88, 1,    0, 0, 0, 0,    0,    0,    0};
    EXPECT_EQ(std::vector<uint8_t>(bytes.begin() + 32, bytes.begin() + 72), std::vector<uint8_t>(KEY_BYTES.begin(), KEY_BYTES.end()));
    EXPECT_EQ(bytes[72], 4);
    EXPECT_EQ(bytes[80], 1);
    EXPECT_EQ(bytes[82], 80);
    p::RequestView view{};
    ASSERT_EQ(p::decode_request(bytes, view), p::Decode::OK);
    EXPECT_EQ(view.key, KEY);
    EXPECT_EQ(view.credentials, user());
}

TEST(WkiJobProtocol, EveryOperationHasOneExactBoundedBody) {
    std::array<uint8_t, 11> plan{};
    p::Submit submit{.size = plan.size()};
    auto plan_body = raw(submit, plan);
    p::Attach attach{10, 20, 30, 40};
    p::AttachmentId detach{9};
    p::Signal signal{7, 15, 0};
    p::ControlId control{7};
    for (uint16_t op = 2; op <= 10; ++op) {
        auto fields = request(static_cast<p::Operation>(op));
        std::span<const uint8_t> body{};
        if (op == 2) body = plan_body;
        if (op == 5) body = bytes_of(attach);
        if (op == 6) body = bytes_of(detach);
        if (op == 8) body = bytes_of(signal);
        if (op >= 9) body = bytes_of(control);
        auto bytes = frame(fields, body, p::encode_request);
        p::RequestView view{};
        ASSERT_EQ(p::decode_request(bytes, view), p::Decode::OK) << op;
        EXPECT_EQ(view.operation, static_cast<p::Operation>(op));
        if (op == 2) EXPECT_EQ(view.plan.size(), plan.size());
        if (op == 5) EXPECT_EQ(+view.attach.stderr_window, 40U);
        if (op == 6) EXPECT_EQ(view.attachment, 9U);
        if (op >= 8) EXPECT_EQ(view.control_serial, 7U);
        // Reframe the longer body to test operation validation independently of
        // the outer declared length.
        fields.prefix.header.kind = 1;
        fields.prefix.header.size = bytes.size() + 1;
        bytes.push_back(0);
        std::memcpy(bytes.data(), &fields, sizeof(fields));
        reject_without_assignment<p::RequestView>(bytes, p::decode_request);
    }
}

TEST(WkiJobProtocol, NegotiationBindsClientNonceCapabilitiesAndLimits) {
    auto query = request(p::Operation::INFO);
    query.prefix.key = {};
    query.prefix.service = 0;
    p::InfoQuery body{17, p::FEATURES, 0};
    auto query_bytes = frame(query, bytes_of(body), p::encode_request);
    p::RequestView decoded{};
    ASSERT_EQ(p::decode_request(query_bytes, decoded), p::Decode::OK);
    auto reply = response();
    reply.prefix.key = {};
    reply.operation = 1;
    reply.state = 0;
    reply.pid = 0;
    auto limits = info();
    auto bytes = frame(reply, bytes_of(limits), p::encode_response);
    p::ResponseView result{};
    ASSERT_EQ(p::decode_response(bytes, result), p::Decode::OK);
    EXPECT_TRUE(p::response_matches(decoded, result));
    result.info.client_incarnation++;
    EXPECT_FALSE(p::response_matches(decoded, result));
    result.info.client_incarnation--;
    result.info.granted_features = p::CORE;
    EXPECT_FALSE(p::response_matches(decoded, result));
    std::array<uint8_t, 512> storage{};
    for (auto bad : {p::Info{.client_incarnation = 17, .available_features = p::CONTROL},
                     p::Info{.client_incarnation = 17, .available_features = p::FEATURES | 16}}) {
        size_t written = 91;
        EXPECT_NE(p::encode_response(reply, bytes_of(bad), storage, written), p::Decode::OK);
        EXPECT_EQ(written, 91U);
    }
    limits.stream_capacity = p::MAX_STREAM_CAPACITY + 1;
    size_t written = 0;
    EXPECT_EQ(p::encode_response(reply, bytes_of(limits), storage, written), p::Decode::FIELD);
    reply.reason = static_cast<uint16_t>(p::Reason::UNSUPPORTED);
    reply.status = -ENOTSUP;
    reply.prefix.service = 0;
    reply.receiver_incarnation = 0;
    limits = {.client_incarnation = 17};
    bytes = frame(reply, bytes_of(limits), p::encode_response);
    ASSERT_EQ(p::decode_response(bytes, result), p::Decode::OK);
    EXPECT_TRUE(p::response_matches(decoded, result));
}

TEST(WkiJobProtocol, EveryTruncatedFrameAndTrailingByteIsRejected) {
    std::array<uint8_t, 3> payload{10, 20, 30};
    auto check = []<typename View>(std::vector<uint8_t> bytes, auto decoder) {
        for (size_t size = 0; size < bytes.size(); ++size)
            reject_without_assignment<View>(std::span<const uint8_t>(bytes).first(size), decoder);
        bytes.push_back(0);
        reject_without_assignment<View>(bytes, decoder);
    };
    check.operator()<p::RequestView>(frame(request(), {}, p::encode_request), p::decode_request);
    check.operator()<p::ResponseView>(frame(response(), {}, p::encode_response), p::decode_response);
    check.operator()<p::OutputView>(frame(output(), payload, p::encode_output), p::decode_output);
    check.operator()<p::CreditView>(credit_frame(credit()), p::decode_credit);
}

TEST(WkiJobProtocol, ReservedHeaderIdentityAndCredentialFieldsFailClosed) {
    auto bytes = frame(request(), {}, p::encode_request);
    // Reserved and invalid fields, including unused supplementary-group slots.
    for (size_t offset : {0U, 4U, 6U, 8U, 10U, 12U, 34U, 68U, 72U, 74U, 76U, 80U, 82U, 86U, 87U, 120U}) {
        auto corrupt = bytes;
        corrupt[offset] ^= 0x80;
        reject_without_assignment<p::RequestView>(corrupt, p::decode_request);
    }
    for (size_t offset : {16U, 24U, 32U, 40U, 48U, 56U, 64U}) {
        auto corrupt = bytes;
        size_t width = offset == 32 ? 2 : offset == 64 ? 4 : 8;
        std::fill_n(corrupt.begin() + offset, width, 0);
        reject_without_assignment<p::RequestView>(corrupt, p::decode_request);
    }
    auto corrupt = bytes;
    corrupt[84] = 8;  // Unknown credential flag.
    reject_without_assignment<p::RequestView>(corrupt, p::decode_request);
    corrupt = bytes;
    corrupt[85] = 9;  // Too many supplementary groups.
    reject_without_assignment<p::RequestView>(corrupt, p::decode_request);
}

TEST(WkiJobProtocol, PlanFramingRequiresVersionSizeAndNativeValidationLater) {
    std::vector<uint8_t> plan(p::MAX_PLAN, 0xFF);
    auto body = raw(p::Submit{.size = p::MAX_PLAN}, plan);
    auto bytes = frame(request(p::Operation::SUBMIT), body, p::encode_request);
    p::RequestView view{};
    ASSERT_EQ(p::decode_request(bytes, view), p::Decode::OK);
    EXPECT_EQ(view.plan.size(), p::MAX_PLAN);
    EXPECT_EQ(view.plan.data(), bytes.data() + sizeof(p::Request) + sizeof(p::Submit));
    // Deliberately nonsensical process bytes are only framed, not admitted.
    for (size_t offset : {160U, 162U, 164U}) {
        auto corrupt = bytes;
        corrupt[offset] ^= 1;
        reject_without_assignment<p::RequestView>(corrupt, p::decode_request);
    }
    std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> storage{};
    body = raw(p::Submit{.size = 0});
    size_t written = 44;
    EXPECT_EQ(p::encode_request(request(p::Operation::SUBMIT), body, storage, written), p::Decode::FIELD);
    plan.push_back(0);
    body = raw(p::Submit{.size = static_cast<uint32_t>(plan.size())}, plan);
    EXPECT_EQ(p::encode_request(request(p::Operation::SUBMIT), body, storage, written), p::Decode::FIELD);
    EXPECT_EQ(written, 44U);
}

TEST(WkiJobProtocol, OffsetWindowsSignalsAndAttachmentTokensAreBounded) {
    std::array<uint8_t, 512> storage{};
    size_t written = 99;
    for (auto attach : {p::Attach{UINT64_MAX, 1, 0, 0}, p::Attach{0, 0, 0, p::MAX_STREAM_CAPACITY + 1}})
        EXPECT_EQ(p::encode_request(request(p::Operation::ATTACH), bytes_of(attach), storage, written), p::Decode::FIELD);
    for (auto signal : {p::Signal{0, 15, 0}, p::Signal{1, 0, 0}, p::Signal{1, 65, 0}, p::Signal{1, 1, 1}})
        EXPECT_EQ(p::encode_request(request(p::Operation::SIGNAL), bytes_of(signal), storage, written), p::Decode::FIELD);
    p::AttachmentId detached{};
    p::ControlId control{};
    EXPECT_EQ(p::encode_request(request(p::Operation::DETACH), bytes_of(detached), storage, written), p::Decode::FIELD);
    EXPECT_EQ(p::encode_request(request(p::Operation::SIGNAL_ACK), bytes_of(control), storage, written), p::Decode::FIELD);
    EXPECT_EQ(written, 99U);
}

TEST(WkiJobProtocol, LostExpiredAndUnknownAreEvidenceWithoutNativeState) {
    for (auto state : {p::State::UNKNOWN, p::State::EXPIRED, p::State::LOST}) {
        auto fields = response();
        fields.pid = 0;
        fields.state = static_cast<uint8_t>(state);
        fields.reason = static_cast<uint16_t>(state == p::State::UNKNOWN ? p::Reason::UNKNOWN
                                              : state == p::State::LOST  ? p::Reason::LOST
                                                                         : p::Reason::EXPIRED);
        if (state == p::State::LOST) fields.receiver_incarnation++;
        auto bytes = frame(fields, {}, p::encode_response);
        p::ResponseView view{};
        ASSERT_EQ(p::decode_response(bytes, view), p::Decode::OK);
        fields.prefix.header.kind = 2;
        fields.prefix.header.size = sizeof(fields);
        auto bad = fields;
        bad.pid = 1;
        reject_without_assignment<p::ResponseView>(raw(bad), p::decode_response);
        bad = fields;
        bad.stdout_produced = 1;
        reject_without_assignment<p::ResponseView>(raw(bad), p::decode_response);
        bad = fields;
        bad.receiver_incarnation = state == p::State::LOST ? KEY.receiver_incarnation : KEY.receiver_incarnation + 1;
        reject_without_assignment<p::ResponseView>(raw(bad), p::decode_response);
    }
}

TEST(WkiJobProtocol, NativeExitAndSignalReceiptsCannotInventCompletion) {
    auto fields = response();
    fields.state = static_cast<uint8_t>(p::State::TERMINAL);
    fields.flags = p::NATIVE_EXIT | p::DETACHED;
    fields.exit_status = 7 << 8;
    auto bytes = frame(fields, {}, p::encode_response);
    p::ResponseView view{};
    ASSERT_EQ(p::decode_response(bytes, view), p::Decode::OK);
    std::array<uint8_t, 256> storage{};
    size_t written = 99;
    fields.flags = 0;
    EXPECT_EQ(p::encode_response(fields, {}, storage, written), p::Decode::FIELD);
    fields = response();
    fields.operation = static_cast<uint16_t>(p::Operation::SIGNAL_STATUS);
    fields.control_serial = 3;
    fields.control_state = static_cast<uint16_t>(p::ControlState::CLAIMED);
    fields.reason = static_cast<uint16_t>(p::Reason::CONTROL_PENDING);
    bytes = frame(fields, {}, p::encode_response);
    ASSERT_EQ(p::decode_response(bytes, view), p::Decode::OK);
    fields.status = -ESRCH;
    EXPECT_EQ(p::encode_response(fields, {}, storage, written), p::Decode::FIELD);
    fields.reason = 0;
    fields.control_state = static_cast<uint16_t>(p::ControlState::DONE);
    bytes = frame(fields, {}, p::encode_response);
    ASSERT_EQ(p::decode_response(bytes, view), p::Decode::OK);
    EXPECT_EQ(+view.fields.status, -ESRCH);
    fields.operation = static_cast<uint16_t>(p::Operation::STATUS);
    EXPECT_EQ(p::encode_response(fields, {}, storage, written), p::Decode::FIELD);
}

TEST(WkiJobProtocol, ResponseCorrelationChecksEveryJobIdentityComponent) {
    p::RequestView request_view{};
    p::ResponseView response_view{};
    ASSERT_EQ(p::decode_request(frame(request(), {}, p::encode_request), request_view), p::Decode::OK);
    ASSERT_EQ(p::decode_response(frame(response(), {}, p::encode_response), response_view), p::Decode::OK);
    EXPECT_TRUE(p::response_matches(request_view, response_view));
    auto original = response_view;
    response_view.fields.prefix.service++;
    EXPECT_FALSE(p::response_matches(request_view, response_view));
    response_view = original;
    response_view.fields.prefix.correlation++;
    EXPECT_FALSE(p::response_matches(request_view, response_view));
    response_view = original;
    response_view.fields.operation++;
    EXPECT_FALSE(p::response_matches(request_view, response_view));
    for (int component = 0; component < 6; ++component) {
        response_view = original;
        switch (component) {
            case 0:
                response_view.key.scope.owner.node++;
                break;
            case 1:
                response_view.key.scope.owner.uid++;
                break;
            case 2:
                response_view.key.scope.submitter_incarnation++;
                break;
            case 3:
                response_view.key.serial++;
                break;
            case 4:
                response_view.key.receiver_incarnation++;
                break;
            case 5:
                response_view.key.attempt++;
                break;
        }
        EXPECT_FALSE(p::response_matches(request_view, response_view));
    }
}

TEST(WkiJobProtocol, StreamFramesHaveExactPayloadAndExplicitOffsetBoundEof) {
    std::array<uint8_t, 3> payload{10, 20, 30};
    auto fields = output();
    auto bytes = frame(fields, payload, p::encode_output);
    p::OutputView view{};
    ASSERT_EQ(p::decode_output(bytes, view), p::Decode::OK);
    EXPECT_EQ(view.bytes.data(), bytes.data() + sizeof(fields));
    EXPECT_EQ(std::vector<uint8_t>(view.bytes.begin(), view.bytes.end()), std::vector<uint8_t>(payload.begin(), payload.end()));
    fields.size = 0;
    fields.flags = p::OUTPUT_END;
    fields.offset = UINT64_MAX;
    bytes = frame(fields, {}, p::encode_output);
    ASSERT_EQ(p::decode_output(bytes, view), p::Decode::OK);
    std::array<uint8_t, 512> storage{};
    size_t written = 99;
    fields.flags = 0;
    EXPECT_EQ(p::encode_output(fields, {}, storage, written), p::Decode::FIELD);
    fields = output();
    fields.offset = UINT64_MAX - 2;
    EXPECT_EQ(p::encode_output(fields, payload, storage, written), p::Decode::FIELD);
    for (int field = 0; field < 5; ++field) {
        fields = output();
        if (field == 0) fields.stream = 3;
        if (field == 1) fields.attachment = 0;
        if (field == 2) fields.prefix.correlation = 1;
        if (field == 3) fields.reserved = 1;
        if (field == 4) fields.flags = 2;
        EXPECT_EQ(p::encode_output(fields, payload, storage, written), p::Decode::FIELD);
    }
    EXPECT_EQ(written, 99U);
}

TEST(WkiJobProtocol, CreditsCarryFullCredentialsAndNonAdditiveRevision) {
    auto bytes = credit_frame(credit());
    p::CreditView view{};
    ASSERT_EQ(p::decode_credit(bytes, view), p::Decode::OK);
    EXPECT_EQ(view.credentials, user());
    EXPECT_EQ(+view.fields.consumed, 42U);
    for (size_t offset : {16U, 72U, 74U, 76U, 160U, 168U, 192U, 193U, 194U, 195U, 196U, 197U, 198U, 199U}) {
        auto corrupt = bytes;
        if (offset == 160 || offset == 168)
            std::fill_n(corrupt.begin() + offset, 8, 0);
        else
            corrupt[offset] = 3;
        reject_without_assignment<p::CreditView>(corrupt, p::decode_credit);
    }
    auto bad = credit();
    bad.consumed = UINT64_MAX;
    size_t written = 99;
    std::array<uint8_t, 200> storage{};
    EXPECT_EQ(p::encode_credit(bad, storage, written), p::Decode::FIELD);
    bad = credit();
    bad.window = p::MAX_STREAM_CAPACITY + 1;
    EXPECT_EQ(p::encode_credit(bad, storage, written), p::Decode::FIELD);
    EXPECT_EQ(written, 99U);
}

TEST(WkiJobProtocol, EncodersPreflightAndSupportOverlappingBody) {
    std::array<uint8_t, 256> storage{};
    storage.fill(0xAA);
    auto before = storage;
    size_t written = 99;
    auto bad = request();
    bad.prefix.header.flags = 1;
    EXPECT_EQ(p::encode_request(bad, {}, storage, written), p::Decode::FLAGS);
    EXPECT_EQ(storage, before);
    EXPECT_EQ(p::encode_request(request(), {}, std::span(storage).first(159), written), p::Decode::SHORT);
    EXPECT_EQ(storage, before);
    EXPECT_EQ(p::encode_response(response(), {}, std::span(storage).first(151), written), p::Decode::SHORT);
    EXPECT_EQ(p::encode_credit(credit(), std::span(storage).first(199), written), p::Decode::SHORT);
    std::array<uint8_t, 3> payload{10, 20, 30};
    EXPECT_EQ(p::encode_output(output(), payload, std::span(storage).first(98), written), p::Decode::SHORT);
    EXPECT_EQ(storage, before);
    EXPECT_EQ(written, 99U);
    for (size_t offset : {0U, 95U, 96U, 97U, 130U}) {
        std::copy(payload.begin(), payload.end(), storage.begin() + offset);
        ASSERT_EQ(p::encode_output(output(), std::span<const uint8_t>(storage).subspan(offset, 3), storage, written), p::Decode::OK);
        p::OutputView view{};
        ASSERT_EQ(p::decode_output(std::span(storage).first(written), view), p::Decode::OK);
        EXPECT_TRUE(std::equal(payload.begin(), payload.end(), view.bytes.begin(), view.bytes.end()));
    }
    std::vector<uint8_t> oversized(WKI_ETH_MAX_PAYLOAD);
    before = storage;
    written = 99;
    EXPECT_EQ(p::encode_request(request(), oversized, storage, written), p::Decode::LENGTH);
    EXPECT_EQ(storage, before);
    EXPECT_EQ(written, 99U);
}

TEST(WkiJobProtocol, OwnerMappingUsesAuthenticatedNodeEffectiveUidAndPeerPolicy) {
    auto mapped = security::Snapshot::bootstrap_root();
    auto supplied = user();
    for (auto policy : {uint64_t{0}, wki_policy_bits(WkiPolicy::COMPUTE), POLICY | (uint64_t{1} << 63)}) {
        auto before = mapped;
        EXPECT_EQ(p::map_owner(7, policy, KEY, supplied, mapped), -EACCES);
        EXPECT_EQ(mapped, before);
    }
    auto before = mapped;
    EXPECT_EQ(p::map_owner(8, POLICY, KEY, supplied, mapped), -EACCES);
    EXPECT_EQ(mapped, before);
    auto wrong = KEY;
    wrong.scope.owner.uid = supplied.user().real;
    EXPECT_EQ(p::map_owner(7, POLICY, wrong, supplied, mapped), -EACCES);
    EXPECT_EQ(mapped, before);
    EXPECT_EQ(p::map_owner(7, POLICY, KEY, supplied, mapped), 0);
    EXPECT_EQ(mapped, supplied);
    wrong = KEY;
    wrong.attempt = 0;
    before = mapped;
    EXPECT_EQ(p::map_owner(7, POLICY, wrong, supplied, mapped), -EACCES);
    EXPECT_EQ(mapped, before);
}

TEST(WkiJobProtocol, RootRequiresSeparatelyProvisionedTrust) {
    auto root = security::Snapshot::bootstrap_root();
    auto root_key = KEY;
    root_key.scope.owner.uid = 0;
    auto mapped = user();
    auto before = mapped;
    EXPECT_EQ(p::map_owner(7, POLICY, root_key, root, mapped), -EACCES);
    EXPECT_EQ(mapped, before);
    EXPECT_EQ(p::map_owner(7, POLICY | wki_policy_bits(WkiPolicy::IDENTITY_ROOT), root_key, root, mapped), 0);
    EXPECT_EQ(mapped, root);
}

TEST(WkiJobProtocol, SignalResponseMustEchoItsImmutableControlIdentity) {
    auto fields = request(p::Operation::SIGNAL);
    p::Signal body{77, 15, 0};
    p::RequestView query{};
    ASSERT_EQ(p::decode_request(frame(fields, bytes_of(body), p::encode_request), query), p::Decode::OK);
    auto reply = response();
    reply.operation = static_cast<uint16_t>(p::Operation::SIGNAL);
    reply.control_serial = 77;
    reply.control_state = static_cast<uint16_t>(p::ControlState::DONE);
    p::ResponseView result{};
    ASSERT_EQ(p::decode_response(frame(reply, {}, p::encode_response), result), p::Decode::OK);
    EXPECT_TRUE(p::response_matches(query, result));
    result.fields.control_serial++;
    EXPECT_FALSE(p::response_matches(query, result));
}

TEST(WkiJobProtocol, SealedServicePrivilegesCannotBypassPeerTrust) {
    auto fields = security::SnapshotFields{.user = {1000, 1000, 1000},
                                           .group = {20, 20, 20},
                                           .authority_ceiling = security::ALL_AUTHORITY,
                                           .service_privileges = 1,
                                           .no_new_privileges = true,
                                           .service_privileges_locked = true};
    security::Snapshot supplied{};
    ASSERT_EQ(security::Snapshot::restore_fields(fields, supplied), 0);
    auto mapped = user();
    auto before = mapped;
    EXPECT_EQ(p::map_owner(7, POLICY, KEY, supplied, mapped), -EACCES);
    EXPECT_EQ(mapped, before);
    EXPECT_EQ(p::map_owner(7, POLICY | wki_policy_bits(WkiPolicy::IDENTITY_ROOT), KEY, supplied, mapped), 0);
    EXPECT_EQ(mapped, supplied);
    auto wire = request();
    wire.credentials = credential::encode(supplied);
    p::RequestView decoded{};
    ASSERT_EQ(p::decode_request(frame(wire, {}, p::encode_request), decoded), p::Decode::OK);
    EXPECT_EQ(decoded.credentials, supplied);
}

TEST(WkiJobProtocol, InvalidResponseEnumsFlagsRangesAndResultsLeaveOutputUnchanged) {
    auto good = response();
    good.prefix.header.kind = 2;
    good.prefix.header.size = sizeof(good);
    for (int field = 0; field < 11; ++field) {
        auto bad = good;
        switch (field) {
            case 0:
                bad.state = 10;
                break;
            case 1:
                bad.reason = 15;
                break;
            case 2:
                bad.control_state = 4;
                break;
            case 3:
                bad.flags = 4;
                break;
            case 4:
                bad.status = 1;
                break;
            case 5:
                bad.stdout_retained = 1;
                break;
            case 6:
                bad.stderr_retained = 1;
                break;
            case 7:
                bad.exit_status = 1;
                break;
            case 8:
                bad.pid = 0;
                break;
            case 9:
                bad.flags = p::NATIVE_EXIT;
                break;
            case 10:
                bad.control_serial = 1;
                break;
        }
        reject_without_assignment<p::ResponseView>(raw(bad), p::decode_response);
    }
}

TEST(WkiJobProtocol, MutatedAndRandomInputsStayBoundedAndSuccessfulDecodesReencodeExactly) {
    std::mt19937 random(0x28404);
    std::array<uint8_t, 3> payload{1, 2, 3};
    std::array frames{frame(request(), {}, p::encode_request), frame(response(), {}, p::encode_response),
                      frame(output(), payload, p::encode_output), credit_frame(credit())};
    for (size_t run = 0; run < 5000; ++run) {
        size_t which = run % frames.size();
        auto bytes = frames[which];
        if (run % 3 == 0) {
            bytes.resize(random() % 1000);
            for (auto& byte : bytes) byte = static_cast<uint8_t>(random());
        } else {
            bytes[random() % bytes.size()] ^= static_cast<uint8_t>(random());
        }
        std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> encoded{};
        size_t written = 0;
        p::Decode result{};
        if (which == 0) {
            p::RequestView view{};
            result = p::decode_request(bytes, view);
            if (result == p::Decode::OK) {
                p::Request fields{};
                std::memcpy(&fields, bytes.data(), sizeof(fields));
                EXPECT_EQ(p::encode_request(fields, std::span<const uint8_t>(bytes).subspan(sizeof(fields)), encoded, written),
                          p::Decode::OK);
            }
        } else if (which == 1) {
            p::ResponseView view{};
            result = p::decode_response(bytes, view);
            if (result == p::Decode::OK) EXPECT_EQ(p::encode_response(view.fields, {}, encoded, written), p::Decode::OK);
        } else if (which == 2) {
            p::OutputView view{};
            result = p::decode_output(bytes, view);
            if (result == p::Decode::OK) EXPECT_EQ(p::encode_output(view.fields, view.bytes, encoded, written), p::Decode::OK);
        } else {
            p::CreditView view{};
            result = p::decode_credit(bytes, view);
            if (result == p::Decode::OK) EXPECT_EQ(p::encode_credit(view.fields, encoded, written), p::Decode::OK);
        }
        if (result == p::Decode::OK) {
            EXPECT_EQ(written, bytes.size());
            EXPECT_TRUE(std::equal(bytes.begin(), bytes.end(), encoded.begin()));
        }
    }
}
