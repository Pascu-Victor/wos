#include "live_protocol.h"

#include <qglobal.h>

#include <QElapsedTimer>
#include <QFileInfo>
#include <QHostAddress>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonParseError>
#include <QLocalSocket>
#include <QMutex>
#include <QMutexLocker>
#include <QSet>
#include <QTcpSocket>
#include <QXmlStreamReader>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <functional>
#include <limits>
#include <utility>

namespace wosdbg::live {
namespace {

constexpr int K_CANCEL_POLL_MS = 50;
constexpr qint64 K_MAX_SAFE_JSON_INTEGER = 9007199254740991LL;

[[noreturn]] void fail(ProtocolErrorCode code, const QString& message) { throw ProtocolError(code, message); }

auto bounded_wait_ms(const OperationContext& context) -> int {
    context.throw_if_stopped();
    return std::min(context.remaining_ms(), K_CANCEL_POLL_MS);
}

template <typename Socket, typename WaitFunction>
void wait_for_socket(Socket& socket, const OperationContext& context, WaitFunction wait, const QString& operation) {
    for (;;) {
        const int WAIT_MS = bounded_wait_ms(context);
        if ((socket.*wait)(WAIT_MS)) {
            return;
        }
        context.throw_if_stopped();
        if (socket.state() == Socket::UnconnectedState) {
            fail(ProtocolErrorCode::Disconnected, QString("%1: %2").arg(operation, socket.errorString()));
        }
    }
}

auto hex_value(char value) -> int {
    if (value >= '0' && value <= '9') {
        return value - '0';
    }
    if (value >= 'a' && value <= 'f') {
        return value - 'a' + 10;
    }
    if (value >= 'A' && value <= 'F') {
        return value - 'A' + 10;
    }
    return -1;
}

auto rsp_checksum(const QByteArray& payload) -> uint8_t {
    uint8_t result = 0;
    for (const char value : payload) {
        result = static_cast<uint8_t>(result + static_cast<uint8_t>(value));
    }
    return result;
}

auto encode_hex_byte(uint8_t value) -> QByteArray {
    static constexpr char DIGITS[] = "0123456789abcdef";
    QByteArray result(2, Qt::Uninitialized);
    result[0] = DIGITS[value >> 4U];
    result[1] = DIGITS[value & 0xfU];
    return result;
}

auto is_rsp_error(const QByteArray& payload) -> bool {
    if (payload.size() < 3 || payload.front() != 'E') {
        return false;
    }
    return std::ranges::all_of(payload.sliced(1), [](char value) { return hex_value(value) >= 0; });
}

auto decode_rsp_payload(const QByteArray& wire, qsizetype max_decoded) -> QByteArray {
    QByteArray decoded;
    decoded.reserve(std::min(wire.size(), max_decoded));
    for (qsizetype index = 0; index < wire.size(); ++index) {
        const char value = wire[index];
        if (value == '}') {
            if (++index >= wire.size()) {
                fail(ProtocolErrorCode::MalformedMessage, "truncated GDB RSP escape");
            }
            if (decoded.size() >= max_decoded) {
                fail(ProtocolErrorCode::MessageTooLarge, "decoded GDB RSP packet exceeds its configured bound");
            }
            decoded.push_back(static_cast<char>(wire[index] ^ 0x20));
            continue;
        }
        if (value == '*') {
            if (decoded.isEmpty() || ++index >= wire.size()) {
                fail(ProtocolErrorCode::MalformedMessage, "invalid GDB RSP run-length encoding");
            }
            const int REPEAT = static_cast<unsigned char>(wire[index]) - 29;
            if (REPEAT < 3 || decoded.size() > max_decoded - REPEAT) {
                fail(REPEAT < 3 ? ProtocolErrorCode::MalformedMessage : ProtocolErrorCode::MessageTooLarge,
                     "invalid or oversized GDB RSP run-length encoding");
            }
            decoded.append(REPEAT, decoded.back());
            continue;
        }
        if (decoded.size() >= max_decoded) {
            fail(ProtocolErrorCode::MessageTooLarge, "decoded GDB RSP packet exceeds its configured bound");
        }
        decoded.push_back(value);
    }
    return decoded;
}

class StrictJsonScanner final {
   public:
    StrictJsonScanner(QByteArrayView bytes, int max_depth) : bytes(bytes), max_depth(max_depth) {}

    void scan() {
        skip_space();
        scan_value(0);
        skip_space();
        if (position != bytes.size()) {
            fail(ProtocolErrorCode::MalformedMessage, "unexpected data after QMP JSON value");
        }
    }

   private:
    void skip_space() {
        while (position < bytes.size()) {
            const char VALUE = bytes[position];
            if (VALUE != ' ' && VALUE != '\t' && VALUE != '\r' && VALUE != '\n') {
                return;
            }
            ++position;
        }
    }

    auto consume(char expected) -> bool {
        if (position < bytes.size() && bytes[position] == expected) {
            ++position;
            return true;
        }
        return false;
    }

    void require(char expected, const QString& message) {
        if (!consume(expected)) {
            fail(ProtocolErrorCode::MalformedMessage, message);
        }
    }

    auto scan_string() -> QString {
        const qsizetype START = position;
        require('"', "QMP JSON string is missing its opening quote");
        bool escaped = false;
        while (position < bytes.size()) {
            const unsigned char VALUE = static_cast<unsigned char>(bytes[position++]);
            if (escaped) {
                if (VALUE == 'u') {
                    for (int digit = 0; digit < 4; ++digit) {
                        if (position >= bytes.size() || hex_value(bytes[position++]) < 0) {
                            fail(ProtocolErrorCode::MalformedMessage, "invalid QMP JSON unicode escape");
                        }
                    }
                } else if (std::strchr("\"\\/bfnrt", VALUE) == nullptr) {
                    fail(ProtocolErrorCode::MalformedMessage, "invalid QMP JSON string escape");
                }
                escaped = false;
                continue;
            }
            if (VALUE == '\\') {
                escaped = true;
                continue;
            }
            if (VALUE == '"') {
                const QByteArray ENCODED(bytes.sliced(START, position - START).toByteArray());
                QJsonParseError error;
                const QJsonDocument DOCUMENT = QJsonDocument::fromJson("[" + ENCODED + "]", &error);
                if (error.error != QJsonParseError::NoError || !DOCUMENT.isArray() || DOCUMENT.array().size() != 1 ||
                    !DOCUMENT.array().at(0).isString()) {
                    fail(ProtocolErrorCode::MalformedMessage, "invalid UTF-8 or escape in QMP JSON string");
                }
                return DOCUMENT.array().at(0).toString();
            }
            if (VALUE < 0x20U) {
                fail(ProtocolErrorCode::MalformedMessage, "control byte in QMP JSON string");
            }
        }
        fail(ProtocolErrorCode::MalformedMessage, "unterminated QMP JSON string");
    }

    void scan_number() {
        if (consume('-') && position >= bytes.size()) {
            fail(ProtocolErrorCode::MalformedMessage, "truncated QMP JSON number");
        }
        if (consume('0')) {
            if (position < bytes.size() && bytes[position] >= '0' && bytes[position] <= '9') {
                fail(ProtocolErrorCode::MalformedMessage, "QMP JSON number has a leading zero");
            }
        } else {
            const qsizetype INTEGER_START = position;
            while (position < bytes.size() && bytes[position] >= '0' && bytes[position] <= '9') {
                ++position;
            }
            if (position == INTEGER_START) {
                fail(ProtocolErrorCode::MalformedMessage, "invalid QMP JSON number");
            }
        }
        if (consume('.')) {
            const qsizetype FRACTION_START = position;
            while (position < bytes.size() && bytes[position] >= '0' && bytes[position] <= '9') {
                ++position;
            }
            if (position == FRACTION_START) {
                fail(ProtocolErrorCode::MalformedMessage, "invalid QMP JSON fraction");
            }
        }
        if (position < bytes.size() && (bytes[position] == 'e' || bytes[position] == 'E')) {
            ++position;
            if (position < bytes.size() && (bytes[position] == '+' || bytes[position] == '-')) {
                ++position;
            }
            const qsizetype EXPONENT_START = position;
            while (position < bytes.size() && bytes[position] >= '0' && bytes[position] <= '9') {
                ++position;
            }
            if (position == EXPONENT_START) {
                fail(ProtocolErrorCode::MalformedMessage, "invalid QMP JSON exponent");
            }
        }
    }

    void scan_literal(QByteArrayView literal) {
        if (position > bytes.size() || bytes.size() - position < literal.size() || bytes.sliced(position, literal.size()) != literal) {
            fail(ProtocolErrorCode::MalformedMessage, "invalid QMP JSON literal");
        }
        position += literal.size();
    }

    void scan_array(int depth) {
        require('[', "invalid QMP JSON array");
        skip_space();
        if (consume(']')) {
            return;
        }
        for (;;) {
            scan_value(depth + 1);
            skip_space();
            if (consume(']')) {
                return;
            }
            require(',', "QMP JSON array is missing a comma");
            skip_space();
        }
    }

    void scan_object(int depth) {
        require('{', "invalid QMP JSON object");
        skip_space();
        if (consume('}')) {
            return;
        }
        QSet<QString> keys;
        for (;;) {
            if (position >= bytes.size() || bytes[position] != '"') {
                fail(ProtocolErrorCode::MalformedMessage, "QMP JSON object member name must be a string");
            }
            const QString KEY = scan_string();
            if (keys.contains(KEY)) {
                fail(ProtocolErrorCode::DuplicateJsonMember, QString("duplicate QMP JSON member %1").arg(KEY));
            }
            keys.insert(KEY);
            skip_space();
            require(':', "QMP JSON object member is missing a colon");
            skip_space();
            scan_value(depth + 1);
            skip_space();
            if (consume('}')) {
                return;
            }
            require(',', "QMP JSON object is missing a comma");
            skip_space();
        }
    }

    void scan_value(int depth) {
        if (depth > max_depth) {
            fail(ProtocolErrorCode::LimitExceeded, "QMP JSON nesting exceeds its configured bound");
        }
        skip_space();
        if (position >= bytes.size()) {
            fail(ProtocolErrorCode::MalformedMessage, "truncated QMP JSON value");
        }
        switch (bytes[position]) {
            case '{':
                scan_object(depth);
                return;
            case '[':
                scan_array(depth);
                return;
            case '"':
                (void)scan_string();
                return;
            case 't':
                scan_literal("true");
                return;
            case 'f':
                scan_literal("false");
                return;
            case 'n':
                scan_literal("null");
                return;
            default:
                scan_number();
                return;
        }
    }

    QByteArrayView bytes;
    qsizetype position = 0;
    int max_depth;
};

auto strict_json_object(const QByteArray& bytes, int max_depth) -> QJsonObject {
    StrictJsonScanner(bytes, max_depth).scan();
    QJsonParseError error;
    const QJsonDocument DOCUMENT = QJsonDocument::fromJson(bytes, &error);
    if (error.error != QJsonParseError::NoError || !DOCUMENT.isObject()) {
        fail(ProtocolErrorCode::MalformedMessage, QString("invalid QMP JSON object: %1").arg(error.errorString()));
    }
    return DOCUMENT.object();
}

auto json_id_matches(const QJsonValue& value, qint64 expected) -> bool {
    if (!value.isDouble()) {
        return false;
    }
    const double NUMBER = value.toDouble();
    // Equality to the finite integer also rejects NaN and infinities without
    // relying on floating-point classification under -ffast-math.
    return NUMBER == static_cast<double>(expected);
}

auto decode_hex(const QByteArray& encoded, qsizetype expected_bytes, bool allow_unavailable, bool* available = nullptr) -> QByteArray {
    if (expected_bytes < 0 || expected_bytes > std::numeric_limits<qsizetype>::max() / 2 || encoded.size() != expected_bytes * 2) {
        fail(ProtocolErrorCode::MalformedMessage, "GDB RSP hex payload has an unexpected length");
    }
    QByteArray result(expected_bytes, '\0');
    bool all_available = true;
    for (qsizetype index = 0; index < expected_bytes; ++index) {
        const char HIGH_CHAR = encoded[index * 2];
        const char LOW_CHAR = encoded[index * 2 + 1];
        if (allow_unavailable && (HIGH_CHAR == 'x' || HIGH_CHAR == 'X') && (LOW_CHAR == 'x' || LOW_CHAR == 'X')) {
            all_available = false;
            continue;
        }
        const int HIGH = hex_value(HIGH_CHAR);
        const int LOW = hex_value(LOW_CHAR);
        if (HIGH < 0 || LOW < 0) {
            fail(ProtocolErrorCode::MalformedMessage, "GDB RSP payload contains invalid hexadecimal data");
        }
        result[index] = static_cast<char>((HIGH << 4U) | LOW);
    }
    if (available != nullptr) {
        *available = all_available;
    }
    return result;
}

auto little_endian_u64(const QByteArray& bytes, bool available) -> std::optional<uint64_t> {
    if (!available || bytes.size() > static_cast<qsizetype>(sizeof(uint64_t))) {
        return std::nullopt;
    }
    uint64_t value = 0;
    for (qsizetype index = 0; index < bytes.size(); ++index) {
        value |= static_cast<uint64_t>(static_cast<unsigned char>(bytes[index])) << (index * 8U);
    }
    return value;
}

auto parse_hex_u64(const QJsonValue& value, const QString& field) -> uint64_t {
    if (!value.isString()) {
        fail(ProtocolErrorCode::MalformedMessage, QString("image catalog field %1 must be a hexadecimal string").arg(field));
    }
    QString text = value.toString();
    if (text.startsWith("0x", Qt::CaseInsensitive)) {
        text.remove(0, 2);
    }
    if (text.isEmpty() || text.size() > 16 || !std::ranges::all_of(text, [](QChar digit) { return hex_value(digit.toLatin1()) >= 0; })) {
        fail(ProtocolErrorCode::MalformedMessage, QString("image catalog field %1 has invalid hexadecimal syntax").arg(field));
    }
    bool ok = false;
    const qulonglong RESULT = text.toULongLong(&ok, 16);
    if (!ok) {
        fail(ProtocolErrorCode::MalformedMessage, QString("image catalog field %1 overflows uint64").arg(field));
    }
    return RESULT;
}

auto parse_hex_bytes(const QJsonValue& value, const QString& field, qsizetype max_bytes) -> QByteArray {
    if (!value.isString()) {
        fail(ProtocolErrorCode::MalformedMessage, QString("image catalog field %1 must be a hexadecimal string").arg(field));
    }
    const QByteArray ENCODED = value.toString().toLatin1();
    if ((ENCODED.size() % 2) != 0 || ENCODED.size() / 2 > max_bytes) {
        fail(ProtocolErrorCode::MalformedMessage, QString("image catalog field %1 has an invalid length").arg(field));
    }
    return decode_hex(ENCODED, ENCODED.size() / 2, false);
}

auto parse_json_i64(const QJsonValue& value, const QString& field) -> qint64 {
    if (!value.isDouble()) {
        fail(ProtocolErrorCode::MalformedMessage, QString("image catalog field %1 must be an integer").arg(field));
    }
    const double NUMBER = value.toDouble();
    // Ordered range comparisons reject NaN and infinities.  Once the value is
    // inside the safe JSON integer range the cast is defined, and a round-trip
    // comparison rejects fractional values.
    if (!(NUMBER >= static_cast<double>(-K_MAX_SAFE_JSON_INTEGER) && NUMBER <= static_cast<double>(K_MAX_SAFE_JSON_INTEGER))) {
        fail(ProtocolErrorCode::MalformedMessage, QString("image catalog field %1 is not an exact JSON integer").arg(field));
    }
    const qint64 INTEGER = static_cast<qint64>(NUMBER);
    if (static_cast<double>(INTEGER) != NUMBER) {
        fail(ProtocolErrorCode::MalformedMessage, QString("image catalog field %1 is not an exact JSON integer").arg(field));
    }
    return INTEGER;
}

}  // namespace

ProtocolError::ProtocolError(ProtocolErrorCode code, const QString& message)
    : std::runtime_error(message.toStdString()), error_code(code), error_message(message) {}

auto OperationContext::with_timeout(int timeout_ms, std::stop_token cancellation) -> OperationContext {
    if (timeout_ms <= 0) {
        throw std::invalid_argument("live protocol timeout must be positive");
    }
    return OperationContext{.deadline = QDeadlineTimer(timeout_ms, Qt::PreciseTimer), .cancellation = cancellation};
}

auto OperationContext::remaining_ms() const -> int {
    const qint64 REMAINING = deadline.remainingTime();
    if (REMAINING < 0) {
        return std::numeric_limits<int>::max();
    }
    return static_cast<int>(std::min<qint64>(REMAINING, std::numeric_limits<int>::max()));
}

void OperationContext::throw_if_stopped() const {
    if (cancellation.stop_requested()) {
        fail(ProtocolErrorCode::Cancelled, "live protocol operation cancelled");
    }
    if (deadline.hasExpired()) {
        fail(ProtocolErrorCode::Timeout, "live protocol operation timed out");
    }
}

class Transcript::Impl final {
   public:
    explicit Impl(TranscriptLimits limits) : limits(limits) {
        if (limits.max_records <= 0 || limits.max_total_bytes <= 0 || limits.max_record_bytes <= 0) {
            throw std::invalid_argument("transcript bounds must be positive");
        }
        clock.start();
    }

    TranscriptLimits limits;
    mutable QMutex mutex;
    QElapsedTimer clock;
    QVector<TranscriptRecord> records;
    qsizetype retained_bytes = 0;
    uint64_t next_sequence = 1;
    uint64_t discarded_records = 0;
    uint64_t discarded_bytes = 0;
};

Transcript::Transcript(TranscriptLimits limits) : impl(std::make_unique<Impl>(limits)) {}

Transcript::~Transcript() = default;

void Transcript::append(const QString& protocol, TranscriptDirection direction, const QString& kind, const QByteArray& payload) {
    QMutexLocker lock(&impl->mutex);
    if (impl->records.size() >= impl->limits.max_records || impl->retained_bytes >= impl->limits.max_total_bytes) {
        ++impl->discarded_records;
        impl->discarded_bytes += static_cast<uint64_t>(payload.size());
        return;
    }
    const qsizetype AVAILABLE = impl->limits.max_total_bytes - impl->retained_bytes;
    const qsizetype RETAIN = std::min({payload.size(), impl->limits.max_record_bytes, AVAILABLE});
    const bool TRUNCATED = RETAIN != payload.size();
    impl->records.push_back(TranscriptRecord{.sequence = impl->next_sequence++,
                                             .monotonic_nanoseconds = impl->clock.nsecsElapsed(),
                                             .protocol = protocol,
                                             .direction = direction,
                                             .kind = kind,
                                             .payload = payload.left(RETAIN),
                                             .truncated = TRUNCATED});
    impl->retained_bytes += RETAIN;
    if (TRUNCATED) {
        impl->discarded_bytes += static_cast<uint64_t>(payload.size() - RETAIN);
    }
}

auto Transcript::records() const -> QVector<TranscriptRecord> {
    QMutexLocker lock(&impl->mutex);
    return impl->records;
}

auto Transcript::discarded_records() const -> uint64_t {
    QMutexLocker lock(&impl->mutex);
    return impl->discarded_records;
}

auto Transcript::discarded_bytes() const -> uint64_t {
    QMutexLocker lock(&impl->mutex);
    return impl->discarded_bytes;
}

class QmpAdapter::Impl final {
   public:
    enum class Command { Capabilities, QueryStatus, Stop, Resume };

    Impl(QString socket_path, QmpLimits limits, std::shared_ptr<Transcript> transcript)
        : path(std::move(socket_path)), limits(limits), transcript(std::move(transcript)) {
        if (path.isEmpty() || limits.max_message_bytes < 256 || limits.max_pending_events <= 0 || limits.max_unmatched_messages <= 0 ||
            limits.max_json_depth <= 0) {
            throw std::invalid_argument("invalid QMP endpoint or bounds");
        }
    }

    void record(TranscriptDirection direction, const QString& kind, const QByteArray& payload = {}) const {
        if (transcript) {
            transcript->append("qmp", direction, kind, payload);
        }
    }

    void write_all(const QByteArray& bytes, const OperationContext& context) {
        qsizetype offset = 0;
        while (offset < bytes.size()) {
            context.throw_if_stopped();
            const qint64 WRITTEN = socket.write(bytes.constData() + offset, bytes.size() - offset);
            if (WRITTEN < 0) {
                fail(ProtocolErrorCode::Disconnected, QString("QMP write failed: %1").arg(socket.errorString()));
            }
            offset += static_cast<qsizetype>(WRITTEN);
            if (WRITTEN == 0) {
                wait_for_socket(socket, context, &QLocalSocket::waitForBytesWritten, "QMP write failed");
            }
        }
        while (socket.bytesToWrite() > 0) {
            wait_for_socket(socket, context, &QLocalSocket::waitForBytesWritten, "QMP write failed");
        }
    }

    auto read_message(const OperationContext& context) -> QJsonObject {
        for (;;) {
            const qsizetype NEWLINE = input.indexOf('\n');
            if (NEWLINE >= 0) {
                QByteArray line = input.left(NEWLINE);
                input.remove(0, NEWLINE + 1);
                if (line.endsWith('\r')) {
                    line.chop(1);
                }
                if (line.isEmpty()) {
                    continue;
                }
                if (line.size() > limits.max_message_bytes) {
                    fail(ProtocolErrorCode::MessageTooLarge, "QMP message exceeds its configured bound");
                }
                record(TranscriptDirection::Received, "message", line);
                return strict_json_object(line, limits.max_json_depth);
            }
            if (input.size() > limits.max_message_bytes) {
                fail(ProtocolErrorCode::MessageTooLarge, "QMP message exceeds its configured bound");
            }
            if (socket.bytesAvailable() == 0) {
                wait_for_socket(socket, context, &QLocalSocket::waitForReadyRead, "QMP read failed");
            }
            const QByteArray CHUNK = socket.read(limits.max_message_bytes + 1 - input.size());
            if (CHUNK.isEmpty() && socket.state() == QLocalSocket::UnconnectedState) {
                fail(ProtocolErrorCode::Disconnected, "QMP connection closed");
            }
            input += CHUNK;
        }
    }

    void queue_event(const QJsonObject& event) {
        if (events.size() >= limits.max_pending_events) {
            events.pop_front();
            ++discarded_events;
        }
        events.push_back(event);
    }

    auto execute(Command command, const OperationContext& context) -> QJsonObject {
        if (socket.state() != QLocalSocket::ConnectedState) {
            fail(ProtocolErrorCode::Disconnected, "QMP connection is not open");
        }
        QString command_name;
        switch (command) {
            case Command::Capabilities:
                command_name = "qmp_capabilities";
                break;
            case Command::QueryStatus:
                command_name = "query-status";
                break;
            case Command::Stop:
                command_name = "stop";
                break;
            case Command::Resume:
                command_name = "cont";
                break;
        }
        if (next_id <= 0 || next_id > K_MAX_SAFE_JSON_INTEGER) {
            fail(ProtocolErrorCode::LimitExceeded, "QMP request identifier space exhausted");
        }
        const qint64 REQUEST_ID = next_id++;
        const QByteArray REQUEST = QJsonDocument(QJsonObject{{"execute", command_name}, {"id", REQUEST_ID}}).toJson(QJsonDocument::Compact);
        if (REQUEST.size() > limits.max_message_bytes) {
            fail(ProtocolErrorCode::MessageTooLarge, "QMP request exceeds its configured bound");
        }
        record(TranscriptDirection::Sent, command_name, REQUEST);
        write_all(REQUEST + "\r\n", context);

        qsizetype unmatched = 0;
        for (;;) {
            const QJsonObject RESPONSE = read_message(context);
            if (json_id_matches(RESPONSE.value("id"), REQUEST_ID)) {
                const bool HAS_RETURN = RESPONSE.contains("return");
                const bool HAS_ERROR = RESPONSE.contains("error");
                if (HAS_RETURN == HAS_ERROR) {
                    fail(ProtocolErrorCode::UnexpectedResponse, QString("QMP %1 response has an invalid envelope").arg(command_name));
                }
                if (HAS_ERROR) {
                    if (!RESPONSE.value("error").isObject()) {
                        fail(ProtocolErrorCode::UnexpectedResponse, QString("QMP %1 returned a malformed error").arg(command_name));
                    }
                    const QByteArray ERROR = QJsonDocument(RESPONSE.value("error").toObject()).toJson(QJsonDocument::Compact);
                    fail(ProtocolErrorCode::RemoteError, QString("QMP %1 failed: %2").arg(command_name, QString::fromUtf8(ERROR)));
                }
                if (!RESPONSE.value("return").isObject()) {
                    fail(ProtocolErrorCode::UnexpectedResponse, QString("QMP %1 returned a non-object result").arg(command_name));
                }
                return RESPONSE.value("return").toObject();
            }
            if (RESPONSE.value("event").isString()) {
                queue_event(RESPONSE);
            } else if (++unmatched > limits.max_unmatched_messages) {
                fail(ProtocolErrorCode::LimitExceeded, "too many unmatched QMP messages");
            }
        }
    }

    QString path;
    QmpLimits limits;
    std::shared_ptr<Transcript> transcript;
    QLocalSocket socket;
    QByteArray input;
    QVector<QJsonObject> events;
    uint64_t discarded_events = 0;
    qint64 next_id = 1;
};

QmpAdapter::QmpAdapter(QString socket_path, QmpLimits limits, std::shared_ptr<Transcript> transcript)
    : impl(std::make_unique<Impl>(std::move(socket_path), limits, std::move(transcript))) {}

QmpAdapter::~QmpAdapter() = default;
QmpAdapter::QmpAdapter(QmpAdapter&&) noexcept = default;
auto QmpAdapter::operator=(QmpAdapter&&) noexcept -> QmpAdapter& = default;

void QmpAdapter::connect(const OperationContext& context) {
    close();
    const QFileInfo ENDPOINT(impl->path);
    if (ENDPOINT.isSymLink()) {
        fail(ProtocolErrorCode::InvalidEndpoint, "QMP endpoint must not be a symbolic link");
    }
    impl->socket.connectToServer(impl->path, QIODevice::ReadWrite);
    while (impl->socket.state() != QLocalSocket::ConnectedState) {
        wait_for_socket(impl->socket, context, &QLocalSocket::waitForConnected, "QMP connect failed");
    }
    impl->record(TranscriptDirection::State, "connected", impl->path.toUtf8());
    try {
        const QJsonObject GREETING = impl->read_message(context);
        if (!GREETING.value("QMP").isObject()) {
            fail(ProtocolErrorCode::UnexpectedResponse, "invalid QMP greeting");
        }
        (void)impl->execute(Impl::Command::Capabilities, context);
    } catch (...) {
        close();
        throw;
    }
}

void QmpAdapter::close() {
    if (!impl) {
        return;
    }
    if (impl->socket.state() != QLocalSocket::UnconnectedState) {
        impl->socket.abort();
        impl->record(TranscriptDirection::State, "closed");
    }
    impl->input.clear();
}

auto QmpAdapter::is_connected() const -> bool { return impl && impl->socket.state() == QLocalSocket::ConnectedState; }
auto QmpAdapter::socket_path() const -> QString { return impl->path; }

auto QmpAdapter::query_status(const OperationContext& context) -> QmpStatus {
    const QJsonObject STATUS = impl->execute(Impl::Command::QueryStatus, context);
    if (!STATUS.value("status").isString() || !STATUS.value("running").isBool()) {
        fail(ProtocolErrorCode::UnexpectedResponse, "QMP query-status result is missing status/running");
    }
    return QmpStatus{.status = STATUS.value("status").toString(),
                     .running = STATUS.value("running").toBool(),
                     .singlestep = STATUS.value("singlestep").toBool(false)};
}

auto QmpAdapter::take_events() -> QmpEventBatch {
    QmpEventBatch result{.events = std::move(impl->events), .discarded = impl->discarded_events};
    impl->events.clear();
    impl->discarded_events = 0;
    return result;
}

void QmpAdapter::stop(const OperationContext& context) { (void)impl->execute(Impl::Command::Stop, context); }
void QmpAdapter::resume(const OperationContext& context) { (void)impl->execute(Impl::Command::Resume, context); }

QemuPauseLease::QemuPauseLease(std::shared_ptr<QmpAdapter> qmp, QmpStatus before, int cleanup_timeout_ms)
    : qmp(std::move(qmp)), before(std::move(before)), cleanup_timeout_ms(cleanup_timeout_ms) {}

auto QemuPauseLease::acquire(std::shared_ptr<QmpAdapter> qmp, const OperationContext& context, int cleanup_timeout_ms) -> QemuPauseLease {
    if (!qmp || cleanup_timeout_ms <= 0) {
        throw std::invalid_argument("QEMU pause lease requires an adapter and positive cleanup timeout");
    }
    QmpStatus before = qmp->query_status(context);
    QemuPauseLease lease(std::move(qmp), before, cleanup_timeout_ms);
    if (!before.running) {
        return lease;
    }

    // Once stop is attempted after observing a running target, cleanup must try
    // cont even when the command reply is lost: the target may have transitioned.
    lease.owns_transition = true;
    lease.qmp->stop(context);
    const QmpStatus AFTER = lease.qmp->query_status(context);
    if (AFTER.running) {
        fail(ProtocolErrorCode::UnexpectedResponse, "QEMU remained running after an acknowledged QMP stop");
    }
    return lease;
}

QemuPauseLease::~QemuPauseLease() { cleanup_noexcept(); }

QemuPauseLease::QemuPauseLease(QemuPauseLease&& other) noexcept
    : qmp(std::move(other.qmp)),
      before(std::move(other.before)),
      owns_transition(std::exchange(other.owns_transition, false)),
      cleanup_timeout_ms(other.cleanup_timeout_ms) {}

auto QemuPauseLease::operator=(QemuPauseLease&& other) noexcept -> QemuPauseLease& {
    if (this != &other) {
        cleanup_noexcept();
        qmp = std::move(other.qmp);
        before = std::move(other.before);
        owns_transition = std::exchange(other.owns_transition, false);
        cleanup_timeout_ms = other.cleanup_timeout_ms;
    }
    return *this;
}

void QemuPauseLease::release(const OperationContext& context) {
    if (!owns_transition) {
        return;
    }
    if (!qmp->is_connected()) {
        qmp->connect(context);
    }
    qmp->resume(context);
    owns_transition = false;
}

void QemuPauseLease::cleanup_noexcept() noexcept {
    if (!owns_transition || !qmp) {
        return;
    }
    try {
        release(OperationContext::with_timeout(cleanup_timeout_ms));
    } catch (...) {
        // Destructors cannot report recovery failure. The explicit release path
        // remains available to callers that must retain structured evidence.
    }
}

class GdbRspAdapter::Impl final {
   public:
    struct Frame {
        char marker = '$';
        QByteArray decoded;
    };

    Impl(QString address, quint16 port, RspLimits limits, std::shared_ptr<Transcript> transcript)
        : address(std::move(address)), port(port), limits(limits), transcript(std::move(transcript)) {
        const QHostAddress HOST(this->address);
        if (HOST.isNull() || !HOST.isLoopback() || port == 0 || limits.max_packet_bytes < 256 ||
            limits.max_decoded_packet_bytes < limits.max_packet_bytes || limits.max_target_description_bytes <= 0 ||
            limits.max_image_catalog_bytes <= 0 || limits.max_memory_read_bytes <= 0 || limits.max_async_packets <= 0 ||
            limits.max_features <= 0 || limits.max_images <= 0 || limits.max_retries < 0) {
            throw std::invalid_argument("invalid loopback GDB RSP endpoint or bounds");
        }
        capabilities.packet_size = limits.max_packet_bytes;
    }

    void record(TranscriptDirection direction, const QString& kind, const QByteArray& payload = {}) const {
        if (transcript) {
            transcript->append("gdb-rsp", direction, kind, payload);
        }
    }

    void write_all(const QByteArray& bytes, const OperationContext& context) {
        qsizetype offset = 0;
        while (offset < bytes.size()) {
            context.throw_if_stopped();
            const qint64 WRITTEN = socket.write(bytes.constData() + offset, bytes.size() - offset);
            if (WRITTEN < 0) {
                fail(ProtocolErrorCode::Disconnected, QString("GDB RSP write failed: %1").arg(socket.errorString()));
            }
            offset += static_cast<qsizetype>(WRITTEN);
            if (WRITTEN == 0) {
                wait_for_socket(socket, context, &QTcpSocket::waitForBytesWritten, "GDB RSP write failed");
            }
        }
        while (socket.bytesToWrite() > 0) {
            wait_for_socket(socket, context, &QTcpSocket::waitForBytesWritten, "GDB RSP write failed");
        }
    }

    auto read_byte(const OperationContext& context) -> char {
        while (socket.bytesAvailable() == 0) {
            wait_for_socket(socket, context, &QTcpSocket::waitForReadyRead, "GDB RSP read failed");
        }
        char value = 0;
        if (!socket.getChar(&value)) {
            fail(ProtocolErrorCode::Disconnected, "GDB RSP connection closed");
        }
        return value;
    }

    auto read_ack(const OperationContext& context) -> char {
        for (qsizetype ignored = 0; ignored <= limits.max_async_packets; ++ignored) {
            const char VALUE = read_byte(context);
            if (VALUE == '+' || VALUE == '-') {
                return VALUE;
            }
            if (VALUE == '$' || VALUE == '%') {
                fail(ProtocolErrorCode::UnexpectedResponse, "GDB RSP packet arrived before request acknowledgement");
            }
        }
        fail(ProtocolErrorCode::LimitExceeded, "too much noise before GDB RSP acknowledgement");
    }

    auto read_frame_after_marker(char marker, const OperationContext& context) -> Frame {
        QByteArray wire;
        while (true) {
            const char VALUE = read_byte(context);
            if (VALUE == '#') {
                break;
            }
            if (wire.size() >= limits.max_packet_bytes) {
                fail(ProtocolErrorCode::MessageTooLarge, "GDB RSP packet exceeds its configured bound");
            }
            wire.push_back(VALUE);
        }
        const int HIGH = hex_value(read_byte(context));
        const int LOW = hex_value(read_byte(context));
        if (HIGH < 0 || LOW < 0) {
            fail(ProtocolErrorCode::MalformedMessage, "GDB RSP packet has an invalid checksum field");
        }
        if (rsp_checksum(wire) != static_cast<uint8_t>((HIGH << 4U) | LOW)) {
            fail(ProtocolErrorCode::MalformedMessage, "GDB RSP packet checksum mismatch");
        }
        QByteArray decoded = decode_rsp_payload(wire, limits.max_decoded_packet_bytes);
        record(TranscriptDirection::Received, marker == '%' ? "notification" : "response", decoded);
        return Frame{.marker = marker, .decoded = std::move(decoded)};
    }

    auto read_frame(const OperationContext& context) -> Frame {
        for (qsizetype ignored = 0; ignored <= limits.max_async_packets; ++ignored) {
            const char MARKER = read_byte(context);
            if (MARKER == '$' || MARKER == '%') {
                return read_frame_after_marker(MARKER, context);
            }
            if (MARKER == '+' || MARKER == '-') {
                continue;
            }
        }
        fail(ProtocolErrorCode::LimitExceeded, "too much noise before GDB RSP packet");
    }

    static auto is_console_output(const QByteArray& payload) -> bool {
        return payload.size() > 1 && payload.front() == 'O' && ((payload.size() - 1) % 2) == 0 &&
               std::ranges::all_of(payload.sliced(1), [](char value) { return hex_value(value) >= 0; });
    }

    auto request(const QByteArray& payload, const OperationContext& context) -> QByteArray {
        if (socket.state() != QAbstractSocket::ConnectedState) {
            fail(ProtocolErrorCode::Disconnected, "GDB RSP connection is not open");
        }
        if (payload.size() > capabilities.packet_size || payload.size() > limits.max_packet_bytes) {
            fail(ProtocolErrorCode::MessageTooLarge, "GDB RSP request exceeds negotiated PacketSize");
        }
        QByteArray frame;
        frame.reserve(payload.size() + 4);
        frame += '$';
        frame += payload;
        frame += '#';
        frame += encode_hex_byte(rsp_checksum(payload));

        int retransmits = 0;
        for (;;) {
            record(TranscriptDirection::Sent, "request", payload);
            write_all(frame, context);
            if (capabilities.no_ack_mode || read_ack(context) == '+') {
                break;
            }
            if (++retransmits > limits.max_retries) {
                fail(ProtocolErrorCode::RemoteError, "GDB RSP request was repeatedly rejected");
            }
        }

        qsizetype async_packets = 0;
        for (;;) {
            Frame response;
            try {
                response = read_frame(context);
            } catch (const ProtocolError& error) {
                if (error.code() != ProtocolErrorCode::MalformedMessage || capabilities.no_ack_mode ||
                    retransmits++ >= limits.max_retries) {
                    throw;
                }
                write_all("-", context);
                continue;
            }
            if (!capabilities.no_ack_mode && response.marker == '$') {
                write_all("+", context);
            }
            if (response.marker == '%' || is_console_output(response.decoded)) {
                if (++async_packets > limits.max_async_packets) {
                    fail(ProtocolErrorCode::LimitExceeded, "too many asynchronous GDB RSP packets");
                }
                continue;
            }
            return response.decoded;
        }
    }

    void negotiate(const OperationContext& context) {
        const QByteArray RESPONSE = request("qSupported:qXfer:features:read+;QStartNoAckMode+", context);
        const QList<QByteArray> FEATURES = RESPONSE.split(';');
        QSet<QByteArray> names;
        for (const QByteArray& feature : FEATURES) {
            if (feature.isEmpty()) {
                continue;
            }
            const qsizetype EQUALS = feature.indexOf('=');
            QByteArray name = EQUALS >= 0 ? feature.left(EQUALS) : feature;
            if (name.endsWith('+') || name.endsWith('-')) {
                name.chop(1);
            }
            if (name.isEmpty() || names.contains(name)) {
                fail(ProtocolErrorCode::MalformedMessage, "GDB RSP qSupported contains an empty or duplicate feature");
            }
            names.insert(name);
            capabilities.advertised.push_back(QString::fromLatin1(feature));
            if (name == "PacketSize") {
                bool ok = false;
                const qulonglong SIZE = feature.sliced(EQUALS + 1).toULongLong(&ok, 16);
                if (!ok || SIZE < 256) {
                    fail(ProtocolErrorCode::MalformedMessage, "GDB RSP advertised an invalid PacketSize");
                }
                capabilities.packet_size = static_cast<qsizetype>(std::min<qulonglong>(SIZE, limits.max_packet_bytes));
            } else if (feature == "qXfer:features:read+") {
                capabilities.qxfer_features_read = true;
            } else if (feature == "qXfer:wos-images:read+") {
                capabilities.qxfer_wos_images_read = true;
            }
        }
        if (!capabilities.qxfer_features_read) {
            fail(ProtocolErrorCode::Unsupported, "GDB RSP target does not support qXfer:features:read");
        }
        if (names.contains("QStartNoAckMode")) {
            const auto FEATURE = std::ranges::find_if(FEATURES, [](const QByteArray& value) { return value == "QStartNoAckMode+"; });
            if (FEATURE != FEATURES.end()) {
                if (request("QStartNoAckMode", context) != "OK") {
                    fail(ProtocolErrorCode::UnexpectedResponse, "GDB RSP rejected QStartNoAckMode");
                }
                capabilities.no_ack_mode = true;
                record(TranscriptDirection::State, "no-ack-mode");
            }
        }
    }

    auto fetch_xfer(const QByteArray& object, const QString& annex, qsizetype max_bytes, const OperationContext& context,
                    qsizetype* shared_total_bytes = nullptr) -> QByteArray {
        if (annex.size() > 128 || !std::ranges::all_of(annex, [](QChar value) {
                return value.isLetterOrNumber() || value == '.' || value == '_' || value == '-';
            })) {
            fail(ProtocolErrorCode::MalformedMessage, "unsafe GDB RSP qXfer annex");
        }
        QByteArray result;
        const qsizetype CHUNK_SIZE = std::min<qsizetype>(4096, std::max<qsizetype>(1, capabilities.packet_size - 1));
        for (qsizetype chunks = 0;; ++chunks) {
            if (chunks > max_bytes / CHUNK_SIZE + 1) {
                fail(ProtocolErrorCode::LimitExceeded, "too many GDB RSP qXfer chunks");
            }
            const QByteArray COMMAND = QByteArray("qXfer:") + object + ":read:" + annex.toLatin1() + ':' +
                                       QByteArray::number(result.size(), 16) + ',' + QByteArray::number(CHUNK_SIZE, 16);
            const QByteArray RESPONSE = request(COMMAND, context);
            if (is_rsp_error(RESPONSE)) {
                fail(ProtocolErrorCode::RemoteError, QString("GDB RSP qXfer read failed: %1").arg(QString::fromLatin1(RESPONSE)));
            }
            if (RESPONSE.isEmpty() || (RESPONSE.front() != 'm' && RESPONSE.front() != 'l')) {
                fail(ProtocolErrorCode::MalformedMessage, "invalid GDB RSP qXfer chunk marker");
            }
            const QByteArray CHUNK = RESPONSE.sliced(1);
            if (RESPONSE.front() == 'm' && CHUNK.isEmpty()) {
                fail(ProtocolErrorCode::MalformedMessage, "non-final GDB RSP qXfer chunk made no progress");
            }
            if (result.size() > max_bytes - CHUNK.size() ||
                (shared_total_bytes != nullptr && *shared_total_bytes > max_bytes - CHUNK.size())) {
                fail(ProtocolErrorCode::MessageTooLarge, "GDB RSP qXfer object exceeds its configured bound");
            }
            result += CHUNK;
            if (shared_total_bytes != nullptr) {
                *shared_total_bytes += CHUNK.size();
            }
            if (RESPONSE.front() == 'l') {
                return result;
            }
        }
    }

    auto load_layout(const OperationContext& context) -> QVector<RegisterDescriptor> {
        if (layout_loaded) {
            return layout;
        }
        QVector<RegisterDescriptor> parsed;
        QSet<QString> visited;
        QSet<uint32_t> regnums;
        qsizetype total_bytes = 0;
        uint32_t next_regnum = 0;

        std::function<void(const QString&)> parse_annex = [&](const QString& annex) {
            if (visited.contains(annex)) {
                return;
            }
            if (visited.size() >= limits.max_features) {
                fail(ProtocolErrorCode::LimitExceeded, "too many GDB RSP target-description feature files");
            }
            visited.insert(annex);
            const QByteArray XML = fetch_xfer("features", annex, limits.max_target_description_bytes, context, &total_bytes);
            if (annex == "target.xml") {
                root_description = XML;
            }
            QXmlStreamReader reader(XML);
            // QEMU's generated target.xml uses xi:include without declaring
            // the xi namespace. GDB accepts that established wire format, so
            // parse names lexically while retaining all of the normal XML
            // well-formedness and bounded-recursion checks.
            reader.setNamespaceProcessing(false);
            while (!reader.atEnd()) {
                reader.readNext();
                if (!reader.isStartElement()) {
                    continue;
                }
                if (reader.name() == u"include" || reader.name().endsWith(u":include")) {
                    parse_annex(reader.attributes().value("href").toString());
                    continue;
                }
                if (reader.name() != u"reg") {
                    continue;
                }
                const auto ATTRIBUTES = reader.attributes();
                bool bits_ok = false;
                const uint32_t BITS = ATTRIBUTES.value("bitsize").toUInt(&bits_ok, 10);
                bool regnum_ok = false;
                const QString REGNUM_TEXT = ATTRIBUTES.value("regnum").toString();
                uint32_t regnum = REGNUM_TEXT.isEmpty() ? next_regnum : REGNUM_TEXT.toUInt(&regnum_ok, 0);
                if (REGNUM_TEXT.isEmpty()) {
                    regnum_ok = true;
                }
                const QString NAME = ATTRIBUTES.value("name").toString();
                if (!bits_ok || BITS == 0 || (BITS % 8) != 0 || !regnum_ok || NAME.isEmpty() || regnums.contains(regnum)) {
                    fail(ProtocolErrorCode::MalformedMessage, "invalid or duplicate register in GDB RSP target description");
                }
                regnums.insert(regnum);
                parsed.push_back(RegisterDescriptor{.name = NAME,
                                                    .type = ATTRIBUTES.value("type").toString(),
                                                    .group = ATTRIBUTES.value("group").toString(),
                                                    .regnum = regnum,
                                                    .bitsize = BITS});
                if (regnum == std::numeric_limits<uint32_t>::max()) {
                    fail(ProtocolErrorCode::LimitExceeded, "GDB RSP register number overflow");
                }
                next_regnum = regnum + 1;
                if (parsed.size() > 4096) {
                    fail(ProtocolErrorCode::LimitExceeded, "too many GDB RSP registers");
                }
            }
            if (reader.hasError()) {
                fail(ProtocolErrorCode::MalformedMessage, QString("invalid GDB RSP target-description XML: %1").arg(reader.errorString()));
            }
        };
        parse_annex("target.xml");
        if (parsed.isEmpty()) {
            fail(ProtocolErrorCode::MalformedMessage, "GDB RSP target description contains no registers");
        }
        std::ranges::sort(parsed, {}, &RegisterDescriptor::regnum);
        layout = std::move(parsed);
        layout_loaded = true;
        return layout;
    }

    QString address;
    quint16 port;
    RspLimits limits;
    std::shared_ptr<Transcript> transcript;
    QTcpSocket socket;
    RspCapabilities capabilities;
    QByteArray root_description;
    QVector<RegisterDescriptor> layout;
    bool layout_loaded = false;
};

GdbRspAdapter::GdbRspAdapter(QString loopback_address, quint16 port, RspLimits limits, std::shared_ptr<Transcript> transcript)
    : impl(std::make_unique<Impl>(std::move(loopback_address), port, limits, std::move(transcript))) {}

GdbRspAdapter::~GdbRspAdapter() = default;
GdbRspAdapter::GdbRspAdapter(GdbRspAdapter&&) noexcept = default;
auto GdbRspAdapter::operator=(GdbRspAdapter&&) noexcept -> GdbRspAdapter& = default;

void GdbRspAdapter::connect(const OperationContext& context) {
    close();
    const QHostAddress HOST(impl->address);
    impl->socket.connectToHost(HOST, impl->port, QIODevice::ReadWrite);
    while (impl->socket.state() != QAbstractSocket::ConnectedState) {
        wait_for_socket(impl->socket, context, &QTcpSocket::waitForConnected, "GDB RSP connect failed");
    }
    impl->record(TranscriptDirection::State, "connected", QString("%1:%2").arg(impl->address).arg(impl->port).toUtf8());
    try {
        impl->negotiate(context);
    } catch (...) {
        close();
        throw;
    }
}

void GdbRspAdapter::close() {
    if (!impl) {
        return;
    }
    if (impl->socket.state() != QAbstractSocket::UnconnectedState) {
        impl->socket.abort();
        impl->record(TranscriptDirection::State, "closed");
    }
}

auto GdbRspAdapter::is_connected() const -> bool { return impl && impl->socket.state() == QAbstractSocket::ConnectedState; }
auto GdbRspAdapter::capabilities() const -> RspCapabilities { return impl->capabilities; }

auto GdbRspAdapter::stop_reason(const OperationContext& context) -> QByteArray {
    const QByteArray RESPONSE = impl->request("?", context);
    if (RESPONSE.isEmpty() || (RESPONSE.front() != 'S' && RESPONSE.front() != 'T' && RESPONSE.front() != 'W' && RESPONSE.front() != 'X')) {
        fail(ProtocolErrorCode::UnexpectedResponse, "GDB RSP returned an invalid stop reason");
    }
    return RESPONSE;
}

auto GdbRspAdapter::target_description(const OperationContext& context) -> QByteArray {
    (void)impl->load_layout(context);
    return impl->root_description;
}

auto GdbRspAdapter::register_layout(const OperationContext& context) -> QVector<RegisterDescriptor> { return impl->load_layout(context); }

auto GdbRspAdapter::read_registers(const OperationContext& context) -> QVector<RegisterValue> {
    const QVector<RegisterDescriptor> LAYOUT = impl->load_layout(context);
    qsizetype total_bytes = 0;
    for (const auto& descriptor : LAYOUT) {
        const qsizetype BYTES = descriptor.bitsize / 8;
        if (total_bytes > impl->limits.max_decoded_packet_bytes - BYTES) {
            fail(ProtocolErrorCode::LimitExceeded, "GDB RSP register file exceeds its configured bound");
        }
        total_bytes += BYTES;
    }
    const QByteArray RESPONSE = impl->request("g", context);
    if (is_rsp_error(RESPONSE)) {
        fail(ProtocolErrorCode::RemoteError, QString("GDB RSP register read failed: %1").arg(QString::fromLatin1(RESPONSE)));
    }
    if (total_bytes > std::numeric_limits<qsizetype>::max() / 2) {
        fail(ProtocolErrorCode::LimitExceeded, "GDB RSP register file size overflows");
    }
    if (RESPONSE.size() != total_bytes * 2) {
        // QEMU can advertise registers in target.xml that it omits from the
        // legacy aggregate `g` packet. Its numbered `p` reads remain aligned
        // with the XML layout, so use that bounded typed path instead.
        if (LAYOUT.size() > 512) {
            fail(ProtocolErrorCode::LimitExceeded, "GDB RSP per-register fallback exceeds its request bound");
        }
        QVector<RegisterValue> values;
        values.reserve(LAYOUT.size());
        for (const auto& descriptor : LAYOUT) {
            const qsizetype BYTES = descriptor.bitsize / 8;
            const QByteArray ITEM = impl->request("p" + QByteArray::number(descriptor.regnum, 16), context);
            if (is_rsp_error(ITEM)) {
                values.push_back(
                    RegisterValue{.descriptor = descriptor, .bytes = {}, .available = false, .little_endian_u64 = std::nullopt});
                continue;
            }
            bool available = false;
            QByteArray bytes = decode_hex(ITEM, BYTES, true, &available);
            values.push_back(RegisterValue{.descriptor = descriptor,
                                           .bytes = bytes,
                                           .available = available,
                                           .little_endian_u64 = little_endian_u64(bytes, available)});
        }
        return values;
    }
    QVector<RegisterValue> values;
    values.reserve(LAYOUT.size());
    qsizetype offset = 0;
    for (const auto& descriptor : LAYOUT) {
        const qsizetype BYTES = descriptor.bitsize / 8;
        bool available = false;
        QByteArray bytes = decode_hex(RESPONSE.sliced(offset * 2, BYTES * 2), BYTES, true, &available);
        values.push_back(RegisterValue{
            .descriptor = descriptor, .bytes = bytes, .available = available, .little_endian_u64 = little_endian_u64(bytes, available)});
        offset += BYTES;
    }
    return values;
}

auto GdbRspAdapter::read_register(uint32_t regnum, const OperationContext& context) -> RegisterValue {
    const QVector<RegisterDescriptor> LAYOUT = impl->load_layout(context);
    const auto FOUND = std::ranges::find(LAYOUT, regnum, &RegisterDescriptor::regnum);
    if (FOUND == LAYOUT.end()) {
        fail(ProtocolErrorCode::UnexpectedResponse, "requested GDB RSP register is absent from target description");
    }
    const QByteArray RESPONSE = impl->request("p" + QByteArray::number(regnum, 16), context);
    if (is_rsp_error(RESPONSE)) {
        fail(ProtocolErrorCode::RemoteError, QString("GDB RSP register read failed: %1").arg(QString::fromLatin1(RESPONSE)));
    }
    const qsizetype BYTES = FOUND->bitsize / 8;
    bool available = false;
    QByteArray bytes = decode_hex(RESPONSE, BYTES, true, &available);
    return RegisterValue{
        .descriptor = *FOUND, .bytes = bytes, .available = available, .little_endian_u64 = little_endian_u64(bytes, available)};
}

auto GdbRspAdapter::read_memory(uint64_t address, qsizetype length, const OperationContext& context) -> QByteArray {
    if (length <= 0 || length > impl->limits.max_memory_read_bytes ||
        static_cast<uint64_t>(length - 1) > std::numeric_limits<uint64_t>::max() - address) {
        fail(ProtocolErrorCode::LimitExceeded, "GDB RSP memory read is empty, oversized, or overflows its address range");
    }
    const QByteArray COMMAND = "m" + QByteArray::number(address, 16) + ',' + QByteArray::number(length, 16);
    const QByteArray RESPONSE = impl->request(COMMAND, context);
    if (is_rsp_error(RESPONSE)) {
        fail(ProtocolErrorCode::RemoteError, QString("GDB RSP memory read failed: %1").arg(QString::fromLatin1(RESPONSE)));
    }
    return decode_hex(RESPONSE, length, false);
}

auto GdbRspAdapter::image_catalog(const OperationContext& context) -> ImageCatalog {
    if (!impl->capabilities.qxfer_wos_images_read) {
        fail(ProtocolErrorCode::Unsupported, "GDB RSP target does not support qXfer:wos-images:read");
    }
    const QByteArray JSON = impl->fetch_xfer("wos-images", {}, impl->limits.max_image_catalog_bytes, context);
    const QJsonObject ROOT = strict_json_object(JSON, 32);
    if (ROOT.value("format").toString() != "wos-image-catalog" || parse_json_i64(ROOT.value("version"), "version") != 1 ||
        !ROOT.value("source").isString() || !ROOT.value("snapshotStatus").isString() || !ROOT.value("images").isArray()) {
        fail(ProtocolErrorCode::MalformedMessage, "invalid WOS image catalog envelope");
    }
    if (ROOT.value("source").toString().size() > 256 || ROOT.value("snapshotStatus").toString().size() > 256) {
        fail(ProtocolErrorCode::LimitExceeded, "WOS image catalog status text exceeds its configured bound");
    }
    const qint64 RECORD_SIZE = parse_json_i64(ROOT.value("recordSize"), "recordSize");
    const qint64 STATUS_CODE = parse_json_i64(ROOT.value("snapshotStatusCode"), "snapshotStatusCode");
    const qint64 COUNT = parse_json_i64(ROOT.value("count"), "count");
    const QJsonArray IMAGES = ROOT.value("images").toArray();
    if (RECORD_SIZE <= 0 || COUNT < 0 || COUNT != IMAGES.size() || COUNT > impl->limits.max_images) {
        fail(ProtocolErrorCode::LimitExceeded, "WOS image catalog count or record size is invalid");
    }

    ImageCatalog catalog{.source = ROOT.value("source").toString(),
                         .snapshot_status = ROOT.value("snapshotStatus").toString(),
                         .snapshot_status_code = STATUS_CODE,
                         .record_size = RECORD_SIZE,
                         .images = {}};
    catalog.images.reserve(IMAGES.size());
    for (qsizetype index = 0; index < IMAGES.size(); ++index) {
        if (!IMAGES[index].isObject()) {
            fail(ProtocolErrorCode::MalformedMessage, QString("WOS image catalog entry %1 is not an object").arg(index));
        }
        const QJsonObject IMAGE = IMAGES[index].toObject();
        const QString PREFIX = QString("images[%1].").arg(index);
        QByteArray path = parse_hex_bytes(IMAGE.value("pathHex"), PREFIX + "pathHex", 4096);
        const qint64 BUILD_ID_SIZE = parse_json_i64(IMAGE.value("buildIdSize"), PREFIX + "buildIdSize");
        QByteArray build_id = parse_hex_bytes(IMAGE.value("buildId"), PREFIX + "buildId", 64);
        if (path.contains('\0') || BUILD_ID_SIZE < 0 || BUILD_ID_SIZE != build_id.size()) {
            fail(ProtocolErrorCode::MalformedMessage, QString("WOS image catalog entry %1 has an invalid path or build ID").arg(index));
        }
        ImageCatalogEntry entry{.path_bytes = std::move(path),
                                .load_base = parse_hex_u64(IMAGE.value("loadBase"), PREFIX + "loadBase"),
                                .image_start = parse_hex_u64(IMAGE.value("imageStart"), PREFIX + "imageStart"),
                                .image_end = parse_hex_u64(IMAGE.value("imageEnd"), PREFIX + "imageEnd"),
                                .text_address = parse_hex_u64(IMAGE.value("textAddress"), PREFIX + "textAddress"),
                                .text_size = parse_hex_u64(IMAGE.value("textSize"), PREFIX + "textSize"),
                                .entry = parse_hex_u64(IMAGE.value("entry"), PREFIX + "entry"),
                                .dynamic_address = parse_hex_u64(IMAGE.value("dynamicAddress"), PREFIX + "dynamicAddress"),
                                .flags = parse_hex_u64(IMAGE.value("flags"), PREFIX + "flags"),
                                .build_id = std::move(build_id)};
        if (entry.image_end < entry.image_start || entry.text_size > std::numeric_limits<uint64_t>::max() - entry.text_address) {
            fail(ProtocolErrorCode::MalformedMessage, QString("WOS image catalog entry %1 has an invalid address range").arg(index));
        }
        catalog.images.push_back(std::move(entry));
    }
    return catalog;
}

}  // namespace wosdbg::live
