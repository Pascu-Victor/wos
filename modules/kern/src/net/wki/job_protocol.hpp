#pragma once

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <net/wki/job_state.hpp>
#include <net/wki/wire.hpp>
#include <platform/security/credential_record.hpp>
#include <span>

namespace ker::net::wki::jobs::protocol {

// Unadvertised schema until the native service is implemented. Discovery uses
// a versioned COMPUTE resource, never a reused HELLO capability bit. The native
// adapter must admit the authenticated peer and exact RESOURCE channel first.
inline constexpr uint32_t RESOURCE_ID = 0x4A4F4201;
inline constexpr char RESOURCE_NAME[] = "process-durable-jobs-v1";
inline constexpr uint32_t MAGIC = 0x31424F4A;  // JOB1, little endian.
inline constexpr uint16_t VERSION = 1;
inline constexpr uint32_t CONTROL = 1;
inline constexpr uint32_t STREAMS = 2;
inline constexpr uint32_t RETENTION = 4;
inline constexpr uint32_t SIGNALS = 8;
inline constexpr uint32_t CORE = CONTROL | STREAMS | RETENTION;
inline constexpr uint32_t FEATURES = CORE | SIGNALS;
inline constexpr size_t MAX_PLAN = 8192;
inline constexpr uint64_t MAX_STREAM_CAPACITY = 1024 * 1024;
inline constexpr uint32_t MAX_RECORDS = 256;
inline constexpr uint32_t MAX_SCOPES = 4096;
inline constexpr uint32_t MAX_SIGNAL_RECEIPTS = 64;
inline constexpr uint32_t MAX_SIGNAL = 64;
inline constexpr uint64_t MAX_RESERVED_BYTES = 64 * 1024 * 1024;
inline constexpr uint8_t OUTPUT_END = 1;
inline constexpr uint8_t NATIVE_EXIT = 1;
inline constexpr uint8_t DETACHED = 2;

// WOS targets x86-64; existing WKI packed integer fields are little endian.
static_assert(std::endian::native == std::endian::little);

enum class Kind : uint16_t { REQUEST = 1, RESPONSE = 2, OUTPUT = 3, CREDIT = 4 };
enum class Operation : uint16_t {
    INFO = 1,
    SUBMIT = 2,
    COMMIT = 3,
    STATUS = 4,
    ATTACH = 5,
    DETACH = 6,
    CANCEL = 7,
    SIGNAL = 8,
    SIGNAL_STATUS = 9,
    SIGNAL_ACK = 10
};
enum class State : uint8_t {
    UNKNOWN = 0,
    PROPOSED = 1,
    ACCEPTED = 2,
    COMMITTED = 3,
    STARTING = 4,
    RUNNING = 5,
    TERMINAL = 6,
    CANCELED = 7,
    EXPIRED = 8,
    LOST = 9
};
enum class Reason : uint16_t {
    OK = 0,
    UNSUPPORTED = 1,
    DENIED = 2,
    INVALID = 3,
    CONFLICT = 4,
    QUOTA = 5,
    UNKNOWN = 6,
    LOST = 7,
    EXPIRED = 8,
    BAD_STATE = 9,
    OUTPUT_GAP = 10,
    LAUNCH_FAILED = 11,
    CANCELED = 12,
    CONTROL_PENDING = 13,
    CONTROL_EXPIRED = 14
};
enum class ControlState : uint16_t { NONE = 0, PROPOSED = 1, CLAIMED = 2, DONE = 3 };
enum class Decode : uint8_t { OK, SHORT, LENGTH, VERSION, FLAGS, FIELD, CREDENTIAL };

struct Header {
    uint32_t magic{MAGIC};
    uint16_t version{VERSION};
    uint16_t kind{};
    uint16_t size{};
    uint16_t reserved{};
    uint32_t flags{};
} __attribute__((packed));
static_assert(sizeof(Header) == 16);

struct KeyWire {
    uint16_t node{};
    uint16_t reserved{};
    uint32_t uid{};
    uint64_t submitter_incarnation{};
    uint64_t serial{};
    uint64_t receiver_incarnation{};
    uint32_t attempt{};
    uint32_t reserved2{};
    constexpr auto operator==(const KeyWire&) const -> bool = default;
} __attribute__((packed));
static_assert(sizeof(KeyWire) == 40);
static_assert(offsetof(KeyWire, receiver_incarnation) == 24);

struct Prefix {
    Header header{};
    uint64_t correlation{};
    uint64_t service{};
    KeyWire key{};
} __attribute__((packed));
static_assert(sizeof(Prefix) == 72);

struct Request {
    Prefix prefix{};
    uint16_t operation{};
    uint16_t reserved{};
    uint32_t reserved2{};
    ker::mod::security::credential_record::Record credentials{};
} __attribute__((packed));
static_assert(sizeof(Request) == 160);
static_assert(offsetof(Request, credentials) == 80);

struct InfoQuery {
    uint64_t client_incarnation{};
    uint32_t required_features{};
    uint32_t reserved{};
} __attribute__((packed));
static_assert(sizeof(InfoQuery) == 16);

struct Submit {
    uint16_t version{VERSION};
    uint16_t reserved{};
    uint32_t size{};
    // Opaque immutable plan follows. Native semantic validation is mandatory
    // before acceptance; framing success alone grants no execution.
} __attribute__((packed));
static_assert(sizeof(Submit) == 8);

struct Attach {
    uint64_t stdout_consumed{};
    uint64_t stdout_window{};
    uint64_t stderr_consumed{};
    uint64_t stderr_window{};
} __attribute__((packed));
static_assert(sizeof(Attach) == 32);

struct Signal {
    uint64_t serial{};
    uint32_t signal{};
    uint32_t reserved{};
} __attribute__((packed));
static_assert(sizeof(Signal) == 16);

struct ControlId {
    uint64_t serial{};
} __attribute__((packed));
static_assert(sizeof(ControlId) == 8);

struct AttachmentId {
    uint64_t attachment{};
} __attribute__((packed));
static_assert(sizeof(AttachmentId) == 8);

struct Response {
    Prefix prefix{};
    uint16_t operation{};
    uint8_t state{};
    uint8_t flags{};
    uint16_t reason{};
    uint16_t control_state{};
    int32_t status{};
    int32_t exit_status{};
    uint64_t pid{};  // Diagnostic value only, never control authority.
    uint64_t attachment{};
    uint64_t stdout_retained{};
    uint64_t stdout_produced{};
    uint64_t stderr_retained{};
    uint64_t stderr_produced{};
    uint64_t receiver_incarnation{};
    uint64_t control_serial{};
} __attribute__((packed));
static_assert(sizeof(Response) == 152);
static_assert(offsetof(Response, receiver_incarnation) == 136);

struct Info {
    uint64_t client_incarnation{};
    uint32_t available_features{};
    uint32_t granted_features{};
    uint32_t max_plan{};
    uint32_t stream_capacity{};
    uint32_t records{};
    uint32_t scopes{};
    uint32_t signal_receipts{};
    uint32_t max_signal{};
    uint64_t byte_limit{};
    uint64_t proposal_ttl_us{};
    uint64_t result_ttl_us{};
} __attribute__((packed));
static_assert(sizeof(Info) == 64);

struct Output {
    Prefix prefix{};
    uint64_t attachment{};
    uint64_t offset{};
    uint32_t size{};
    uint8_t stream{};  // 1 stdout, 2 stderr.
    uint8_t flags{};
    uint16_t reserved{};
} __attribute__((packed));
static_assert(sizeof(Output) == 96);

struct Credit {
    Request request{};  // operation is zero for asynchronous credits.
    uint64_t attachment{};
    uint64_t revision{};
    uint64_t consumed{};
    uint64_t window{};
    uint8_t stream{};
    std::array<uint8_t, 7> reserved{};
} __attribute__((packed));
static_assert(sizeof(Credit) == 200);
static_assert(sizeof(Request) + sizeof(Submit) + MAX_PLAN <= WKI_ETH_MAX_PAYLOAD);

// Decoded values are not wire layouts or authentication proof. Borrowed spans
// must remain within their retained input owner; failed decode assigns nothing.
struct RequestView {
    Key key{};
    uint64_t correlation{};
    uint64_t service{};
    Operation operation{Operation::INFO};
    ker::mod::security::Snapshot credentials{};
    InfoQuery info{};
    Attach attach{};
    uint64_t attachment{};
    uint64_t control_serial{};
    uint32_t signal{};
    std::span<const uint8_t> plan{};
};
struct ResponseView {
    Key key{};
    Response fields{};
    Info info{};
};
struct OutputView {
    Key key{};
    Output fields{};
    std::span<const uint8_t> bytes{};
};
struct CreditView {
    Key key{};
    Credit fields{};
    ker::mod::security::Snapshot credentials{};
};

[[nodiscard]] auto key_wire(const Key& key) -> KeyWire;
[[nodiscard]] auto decode_request(std::span<const uint8_t> bytes, RequestView& out) -> Decode;
[[nodiscard]] auto decode_response(std::span<const uint8_t> bytes, ResponseView& out) -> Decode;
[[nodiscard]] auto decode_output(std::span<const uint8_t> bytes, OutputView& out) -> Decode;
[[nodiscard]] auto decode_credit(std::span<const uint8_t> bytes, CreditView& out) -> Decode;

// Header kind/size are supplied by the typed encoder. Other malformed values
// and short output leave output/written unchanged. Body may overlap output.
[[nodiscard]] auto encode_request(Request fields, std::span<const uint8_t> body, std::span<uint8_t> out, size_t& written) -> Decode;
[[nodiscard]] auto encode_response(Response fields, std::span<const uint8_t> body, std::span<uint8_t> out, size_t& written) -> Decode;
[[nodiscard]] auto encode_output(Output fields, std::span<const uint8_t> body, std::span<uint8_t> out, size_t& written) -> Decode;
[[nodiscard]] auto encode_credit(Credit fields, std::span<uint8_t> out, size_t& written) -> Decode;

[[nodiscard]] auto response_matches(const RequestView& request, const ResponseView& response) -> bool;

// Caller supplies current authenticated peer facts while retaining lifecycle
// and exact request-channel admission. This maps full credentials and owner;
// it does not verify MACs, peer liveness, service grants or channel generations.
[[nodiscard]] auto map_owner(uint16_t authenticated_node, uint64_t policy, const Key& key, const ker::mod::security::Snapshot& supplied,
                             ker::mod::security::Snapshot& mapped) -> int;

}  // namespace ker::net::wki::jobs::protocol
