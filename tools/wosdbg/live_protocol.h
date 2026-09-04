#pragma once

#include <QByteArray>
#include <QDeadlineTimer>
#include <QJsonObject>
#include <QString>
#include <QStringList>
#include <QVector>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <stop_token>

namespace wosdbg::live {

enum class ProtocolErrorCode {
    Cancelled,
    Timeout,
    Disconnected,
    InvalidEndpoint,
    MessageTooLarge,
    MalformedMessage,
    DuplicateJsonMember,
    UnexpectedResponse,
    RemoteError,
    LimitExceeded,
    Unsupported,
};

class ProtocolError final : public std::runtime_error {
   public:
    ProtocolError(ProtocolErrorCode code, const QString& message);

    [[nodiscard]] auto code() const noexcept -> ProtocolErrorCode { return error_code; }
    [[nodiscard]] auto message() const -> QString { return error_message; }

   private:
    ProtocolErrorCode error_code;
    QString error_message;
};

struct OperationContext {
    QDeadlineTimer deadline;
    std::stop_token cancellation;

    [[nodiscard]] static auto with_timeout(int timeout_ms, std::stop_token cancellation = {}) -> OperationContext;
    [[nodiscard]] auto remaining_ms() const -> int;
    void throw_if_stopped() const;
};

enum class TranscriptDirection { Sent, Received, State };

struct TranscriptRecord {
    uint64_t sequence = 0;
    qint64 monotonic_nanoseconds = 0;
    QString protocol;
    TranscriptDirection direction = TranscriptDirection::State;
    QString kind;
    QByteArray payload;
    bool truncated = false;
};

struct TranscriptLimits {
    qsizetype max_records = 4096;
    qsizetype max_total_bytes = 1024 * 1024;
    qsizetype max_record_bytes = 64 * 1024;
};

class Transcript final {
   public:
    explicit Transcript(TranscriptLimits limits = {});
    ~Transcript();

    Transcript(const Transcript&) = delete;
    auto operator=(const Transcript&) -> Transcript& = delete;

    void append(const QString& protocol, TranscriptDirection direction, const QString& kind, const QByteArray& payload = {});
    [[nodiscard]] auto records() const -> QVector<TranscriptRecord>;
    [[nodiscard]] auto discarded_records() const -> uint64_t;
    [[nodiscard]] auto discarded_bytes() const -> uint64_t;

   private:
    class Impl;
    std::unique_ptr<Impl> impl;
};

struct QmpLimits {
    qsizetype max_message_bytes = 1024 * 1024;
    qsizetype max_pending_events = 256;
    qsizetype max_unmatched_messages = 256;
    int max_json_depth = 64;
};

struct QmpEventBatch {
    QVector<QJsonObject> events;
    uint64_t discarded = 0;
};

struct QmpStatus {
    QString status;
    bool running = false;
    bool singlestep = false;
};

class QmpAdapter final {
   public:
    QmpAdapter(QString socket_path, QmpLimits limits = {}, std::shared_ptr<Transcript> transcript = {});
    ~QmpAdapter();

    QmpAdapter(const QmpAdapter&) = delete;
    auto operator=(const QmpAdapter&) -> QmpAdapter& = delete;
    QmpAdapter(QmpAdapter&&) noexcept;
    auto operator=(QmpAdapter&&) noexcept -> QmpAdapter&;

    void connect(const OperationContext& context);
    void close();
    [[nodiscard]] auto is_connected() const -> bool;
    [[nodiscard]] auto socket_path() const -> QString;

    [[nodiscard]] auto query_status(const OperationContext& context) -> QmpStatus;
    [[nodiscard]] auto take_events() -> QmpEventBatch;

   private:
    friend class QemuPauseLease;
    class Impl;
    std::unique_ptr<Impl> impl;

    void stop(const OperationContext& context);
    void resume(const OperationContext& context);
};

class QemuPauseLease final {
   public:
    static auto acquire(std::shared_ptr<QmpAdapter> qmp, const OperationContext& context, int cleanup_timeout_ms = 2000) -> QemuPauseLease;
    ~QemuPauseLease();

    QemuPauseLease(const QemuPauseLease&) = delete;
    auto operator=(const QemuPauseLease&) -> QemuPauseLease& = delete;
    QemuPauseLease(QemuPauseLease&& other) noexcept;
    auto operator=(QemuPauseLease&& other) noexcept -> QemuPauseLease&;

    [[nodiscard]] auto owns_pause() const -> bool { return owns_transition; }
    [[nodiscard]] auto initial_status() const -> QmpStatus { return before; }
    void release(const OperationContext& context);

   private:
    QemuPauseLease(std::shared_ptr<QmpAdapter> qmp, QmpStatus before, int cleanup_timeout_ms);
    void cleanup_noexcept() noexcept;

    std::shared_ptr<QmpAdapter> qmp;
    QmpStatus before;
    bool owns_transition = false;
    int cleanup_timeout_ms = 2000;
};

struct RspLimits {
    qsizetype max_packet_bytes = 64 * 1024;
    qsizetype max_decoded_packet_bytes = 1024 * 1024;
    qsizetype max_target_description_bytes = 256 * 1024;
    qsizetype max_image_catalog_bytes = 256 * 1024;
    qsizetype max_memory_read_bytes = 4096;
    qsizetype max_async_packets = 256;
    int max_features = 32;
    int max_images = 1024;
    int max_retries = 3;
};

struct RspCapabilities {
    qsizetype packet_size = 0;
    bool no_ack_mode = false;
    bool qxfer_features_read = false;
    bool qxfer_wos_images_read = false;
    QStringList advertised;
};

struct RegisterDescriptor {
    QString name;
    QString type;
    QString group;
    uint32_t regnum = 0;
    uint32_t bitsize = 0;
};

struct RegisterValue {
    RegisterDescriptor descriptor;
    QByteArray bytes;
    bool available = false;
    std::optional<uint64_t> little_endian_u64;
};

struct ImageCatalogEntry {
    QByteArray path_bytes;
    uint64_t load_base = 0;
    uint64_t image_start = 0;
    uint64_t image_end = 0;
    uint64_t text_address = 0;
    uint64_t text_size = 0;
    uint64_t entry = 0;
    uint64_t dynamic_address = 0;
    uint64_t flags = 0;
    QByteArray build_id;
};

struct ImageCatalog {
    QString source;
    QString snapshot_status;
    qint64 snapshot_status_code = 0;
    qint64 record_size = 0;
    QVector<ImageCatalogEntry> images;
};

class GdbRspAdapter final {
   public:
    GdbRspAdapter(QString loopback_address, quint16 port, RspLimits limits = {}, std::shared_ptr<Transcript> transcript = {});
    ~GdbRspAdapter();

    GdbRspAdapter(const GdbRspAdapter&) = delete;
    auto operator=(const GdbRspAdapter&) -> GdbRspAdapter& = delete;
    GdbRspAdapter(GdbRspAdapter&&) noexcept;
    auto operator=(GdbRspAdapter&&) noexcept -> GdbRspAdapter&;

    void connect(const OperationContext& context);
    void close();
    [[nodiscard]] auto is_connected() const -> bool;
    [[nodiscard]] auto capabilities() const -> RspCapabilities;

    [[nodiscard]] auto stop_reason(const OperationContext& context) -> QByteArray;
    [[nodiscard]] auto target_description(const OperationContext& context) -> QByteArray;
    [[nodiscard]] auto register_layout(const OperationContext& context) -> QVector<RegisterDescriptor>;
    [[nodiscard]] auto read_registers(const OperationContext& context) -> QVector<RegisterValue>;
    [[nodiscard]] auto read_register(uint32_t regnum, const OperationContext& context) -> RegisterValue;
    [[nodiscard]] auto read_memory(uint64_t address, qsizetype length, const OperationContext& context) -> QByteArray;
    [[nodiscard]] auto image_catalog(const OperationContext& context) -> ImageCatalog;

   private:
    class Impl;
    std::unique_ptr<Impl> impl;
};

}  // namespace wosdbg::live
