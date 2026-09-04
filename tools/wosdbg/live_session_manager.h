#pragma once

#include <QJsonObject>
#include <QString>
#include <QStringList>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <stop_token>

#include "config.h"

namespace wosdbg::live {

// Stable machine-readable failures returned by every frontend.  Messages are
// diagnostic only; callers should branch on live_session_error_code().
enum class LiveSessionErrorCode {
    Disabled,
    InvalidArgument,
    InvalidDescriptor,
    UnsafePath,
    EndpointRejected,
    StaleProcess,
    DuplicateTarget,
    TargetNotFound,
    TargetBusy,
    SessionNotFound,
    LeaseExpired,
    LeaseOwnerMismatch,
    LeaseTokenMismatch,
    LimitExceeded,
    SensitiveConfirmationRequired,
    BuildIdUnavailable,
    SymbolsQuarantined,
    ProtocolFailure,
    ShuttingDown,
};

[[nodiscard]] auto live_session_error_code(LiveSessionErrorCode code) -> QString;

class LiveSessionError final : public std::runtime_error {
   public:
    LiveSessionError(LiveSessionErrorCode code, const QString& message);

    [[nodiscard]] auto code() const noexcept -> LiveSessionErrorCode { return error_code; }
    [[nodiscard]] auto code_string() const -> QString { return live_session_error_code(error_code); }
    [[nodiscard]] auto message() const -> QString { return error_message; }

   private:
    LiveSessionErrorCode error_code;
    QString error_message;
};

// Owns all live transports and pause leases.  Methods are synchronous and
// internally serialized because protocol adapters are intentionally
// single-owner/single-thread.  No API accepts raw protocol commands or exposes
// register/memory mutation.
class LiveSessionManager final {
   public:
    explicit LiveSessionManager(LiveSettings settings, QStringList allowed_roots = {});
    ~LiveSessionManager();

    LiveSessionManager(const LiveSessionManager&) = delete;
    auto operator=(const LiveSessionManager&) -> LiveSessionManager& = delete;

    // Reload the configured targets and bounded version-1 runtime descriptors.
    [[nodiscard]] auto discover_targets() -> QJsonObject;
    [[nodiscard]] auto list_targets() const -> QJsonObject;

    // target_ids is acquired atomically: QEMU pause leases are obtained before
    // any RSP reads, and partial acquisition is unwound in reverse order.
    [[nodiscard]] auto open_session(const QStringList& target_ids, const QString& owner, const QString& audit_id, int ttl_ms = 0,
                                    std::stop_token cancellation = {}) -> QJsonObject;
    [[nodiscard]] auto list_sessions(const QString& owner = {}) const -> QJsonObject;
    [[nodiscard]] auto session_status(const QString& session_id, const QString& lease_token, const QString& owner) const -> QJsonObject;
    [[nodiscard]] auto renew(const QString& session_id, const QString& lease_token, const QString& owner, int ttl_ms = 0) -> QJsonObject;
    [[nodiscard]] auto close(const QString& session_id, const QString& lease_token, const QString& owner) -> QJsonObject;
    [[nodiscard]] auto close_owner(const QString& owner) -> QJsonObject;
    [[nodiscard]] auto sweep_expired() -> QJsonObject;
    void shutdown();

    [[nodiscard]] auto read_registers(const QString& session_id, const QString& lease_token, const QString& owner, const QString& target_id,
                                      std::stop_token cancellation = {}) -> QJsonObject;
    [[nodiscard]] auto read_memory(const QString& session_id, const QString& lease_token, const QString& owner, const QString& target_id,
                                   uint64_t address, int length, bool confirm_sensitive, std::stop_token cancellation = {}) -> QJsonObject;
    [[nodiscard]] auto frame_pointer_backtrace(const QString& session_id, const QString& lease_token, const QString& owner,
                                               const QString& target_id, int max_frames, std::stop_token cancellation = {}) -> QJsonObject;
    [[nodiscard]] auto resolve_address(const QString& session_id, const QString& lease_token, const QString& owner,
                                       const QString& target_id, uint64_t address) -> QJsonObject;
    [[nodiscard]] auto inspect_pte(const QString& session_id, const QString& lease_token, const QString& owner, const QString& target_id,
                                   uint64_t address, std::stop_token cancellation = {}) -> QJsonObject;

    // Payloads are redacted by default.  Including protocol payloads requires
    // an explicit confirmation and is still capped by maxTranscriptBytes.
    [[nodiscard]] auto transcript(const QString& session_id, const QString& lease_token, const QString& owner,
                                  bool include_sensitive_payloads, bool confirm_sensitive) const -> QJsonObject;

    // Internal capture metadata. Paths are already canonicalized and
    // allowlisted; the shared service passes them only to the incident writer
    // and never returns them directly from a frontend tool.
    [[nodiscard]] auto capture_sources(const QString& session_id, const QString& lease_token, const QString& owner) const -> QJsonObject;

   private:
    class Impl;
    std::unique_ptr<Impl> impl;
};

}  // namespace wosdbg::live
