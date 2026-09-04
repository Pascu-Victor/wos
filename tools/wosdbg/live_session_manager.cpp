#include "live_session_manager.h"

#include <elf.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <QCryptographicHash>
#include <QDir>
#include <QFile>
#include <QFileInfo>
#include <QHostAddress>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonParseError>
#include <QMutex>
#include <QMutexLocker>
#include <QRandomGenerator>
#include <QRegularExpression>
#include <QSet>
#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstring>
#include <limits>
#include <map>
#include <optional>
#include <utility>
#include <vector>

#include "elf_symbol_resolver.h"
#include "live_protocol.h"

namespace wosdbg::live {
namespace {

constexpr qint64 MAX_DESCRIPTOR_BYTES = 1024 * 1024;
constexpr qint64 MAX_SYMBOL_BYTES = 512LL * 1024 * 1024;
constexpr int MAX_DESCRIPTOR_DEPTH = 32;
constexpr int MAX_DESCRIPTOR_MEMBERS = 4096;
constexpr int MAX_LOG_PATHS = 32;
constexpr int MAX_OWNER_BYTES = 160;
constexpr int MAX_BACKTRACE_FRAMES = 128;
constexpr uint64_t MAX_FRAME_POINTER_STEP = 16ULL * 1024 * 1024;

struct ElfBuildIdNote {
    uint64_t runtime_address = 0;
    QByteArray bytes;
};

auto local_elf_build_id_note(const QString& path) -> std::optional<ElfBuildIdNote> {
    QFile file(path);
    if (!file.open(QIODevice::ReadOnly)) {
        return std::nullopt;
    }
    const QByteArray header_bytes = file.read(sizeof(Elf64_Ehdr));
    Elf64_Ehdr header{};
    if (header_bytes.size() != sizeof(header)) {
        return std::nullopt;
    }
    std::memcpy(&header, header_bytes.constData(), sizeof(header));
    if (std::memcmp(header.e_ident, ELFMAG, SELFMAG) != 0 || header.e_ident[EI_CLASS] != ELFCLASS64 ||
        header.e_ident[EI_DATA] != ELFDATA2LSB || header.e_phentsize != sizeof(Elf64_Phdr) || header.e_phnum == 0 || header.e_phnum > 256) {
        return std::nullopt;
    }
    if (!file.seek(static_cast<qint64>(header.e_phoff))) {
        return std::nullopt;
    }
    const qsizetype program_size = static_cast<qsizetype>(header.e_phnum) * static_cast<qsizetype>(sizeof(Elf64_Phdr));
    const QByteArray program_bytes = file.read(program_size);
    if (program_bytes.size() != program_size) {
        return std::nullopt;
    }
    for (uint16_t index = 0; index < header.e_phnum; ++index) {
        Elf64_Phdr program{};
        std::memcpy(&program, program_bytes.constData() + static_cast<qsizetype>(index) * sizeof(program), sizeof(program));
        if (program.p_type != PT_NOTE || program.p_filesz == 0 || program.p_filesz > 64 * 1024 ||
            program.p_offset > static_cast<uint64_t>(file.size()) ||
            program.p_filesz > static_cast<uint64_t>(file.size()) - program.p_offset || !file.seek(static_cast<qint64>(program.p_offset))) {
            continue;
        }
        const QByteArray notes = file.read(static_cast<qint64>(program.p_filesz));
        qsizetype offset = 0;
        while (offset <= notes.size() - static_cast<qsizetype>(sizeof(Elf64_Nhdr))) {
            Elf64_Nhdr note{};
            std::memcpy(&note, notes.constData() + offset, sizeof(note));
            offset += sizeof(note);
            const uint64_t name_padded = (static_cast<uint64_t>(note.n_namesz) + 3U) & ~3ULL;
            const uint64_t descriptor_padded = (static_cast<uint64_t>(note.n_descsz) + 3U) & ~3ULL;
            if (name_padded > static_cast<uint64_t>(notes.size() - offset) ||
                descriptor_padded > static_cast<uint64_t>(notes.size() - offset) - name_padded) {
                break;
            }
            const QByteArray name = notes.sliced(offset, note.n_namesz);
            const qsizetype descriptor_offset = offset + static_cast<qsizetype>(name_padded);
            if (note.n_type == NT_GNU_BUILD_ID && name.startsWith("GNU") && note.n_descsz > 0 && note.n_descsz <= 64 &&
                program.p_vaddr <= std::numeric_limits<uint64_t>::max() - static_cast<uint64_t>(descriptor_offset)) {
                return ElfBuildIdNote{.runtime_address = program.p_vaddr + static_cast<uint64_t>(descriptor_offset),
                                      .bytes = notes.sliced(descriptor_offset, note.n_descsz)};
            }
            offset = descriptor_offset + static_cast<qsizetype>(descriptor_padded);
        }
    }
    return std::nullopt;
}

[[noreturn]] void fail(LiveSessionErrorCode code, const QString& message) { throw LiveSessionError(code, message); }

auto success(QJsonObject result) -> QJsonObject {
    result["ok"] = true;
    result["format"] = "wosdbg-live";
    result["version"] = 1;
    return result;
}

auto sanitized_text(QString value, int max_length = 160) -> QString {
    value.replace(QChar('\0'), QChar::ReplacementCharacter);
    value.replace('\r', ' ');
    value.replace('\n', ' ');
    value.replace('\t', ' ');
    return value.left(max_length);
}

auto direction_name(TranscriptDirection direction) -> QString {
    switch (direction) {
        case TranscriptDirection::Sent:
            return "sent";
        case TranscriptDirection::Received:
            return "received";
        case TranscriptDirection::State:
            return "state";
    }
    return "state";
}

auto random_token() -> QString {
    QByteArray bytes(32, Qt::Uninitialized);
    for (qsizetype offset = 0; offset < bytes.size(); offset += 4) {
        const quint32 value = QRandomGenerator::system()->generate();
        const qsizetype count = std::min<qsizetype>(4, bytes.size() - offset);
        std::memcpy(bytes.data() + offset, &value, static_cast<size_t>(count));
    }
    return QString::fromLatin1(bytes.toHex());
}

auto tokens_equal(const QString& left, const QString& right) -> bool {
    const QByteArray a = QCryptographicHash::hash(left.toUtf8(), QCryptographicHash::Sha256);
    const QByteArray b = QCryptographicHash::hash(right.toUtf8(), QCryptographicHash::Sha256);
    unsigned char different = static_cast<unsigned char>(a.size() ^ b.size());
    const qsizetype count = std::min(a.size(), b.size());
    for (qsizetype index = 0; index < count; ++index) {
        different |= static_cast<unsigned char>(a[index] ^ b[index]);
    }
    return different == 0;
}

auto is_below(const QString& path, const QString& root) -> bool { return path == root || path.startsWith(root + QDir::separator()); }

auto has_parent_component(const QString& path) -> bool {
    return path.split('/', Qt::KeepEmptyParts).contains("..") || path.split('\\', Qt::KeepEmptyParts).contains("..");
}

auto canonical_existing_parent(QString path) -> QString {
    QFileInfo info(path);
    while (!info.exists()) {
        const QString parent = info.dir().absolutePath();
        if (parent == info.absoluteFilePath()) {
            return {};
        }
        info.setFile(parent);
    }
    return info.canonicalFilePath();
}

auto checked_path(const QString& input, const QStringList& allowed_roots, bool require_regular, bool allow_socket) -> QString {
    if (input.isEmpty()) {
        return {};
    }
    if (!QFileInfo(input).isAbsolute() || has_parent_component(input)) {
        fail(LiveSessionErrorCode::UnsafePath, "live path must be absolute and cannot contain parent traversal");
    }
    const QString cleaned = QDir::cleanPath(input);
    struct stat metadata{};
    const QByteArray encoded = QFile::encodeName(cleaned);
    const bool exists = ::lstat(encoded.constData(), &metadata) == 0;
    if (exists && S_ISLNK(metadata.st_mode)) {
        fail(LiveSessionErrorCode::UnsafePath, "symbolic links are not accepted for live paths");
    }
    if (require_regular && (!exists || !S_ISREG(metadata.st_mode))) {
        fail(LiveSessionErrorCode::UnsafePath, "live evidence path is not a regular file");
    }
    if (exists && !require_regular && !S_ISSOCK(metadata.st_mode) && !S_ISREG(metadata.st_mode)) {
        fail(LiveSessionErrorCode::UnsafePath, "live endpoint path has an unsafe file type");
    }
    if (exists && allow_socket && !S_ISSOCK(metadata.st_mode)) {
        fail(LiveSessionErrorCode::UnsafePath, "QMP endpoint is not a Unix socket");
    }

    const QString comparison = exists ? QFileInfo(cleaned).canonicalFilePath() : canonical_existing_parent(cleaned);
    if (comparison.isEmpty() || std::ranges::none_of(allowed_roots, [&](const QString& root) { return is_below(comparison, root); })) {
        fail(LiveSessionErrorCode::UnsafePath, "live path is outside configured allowed roots");
    }
    return cleaned;
}

auto read_regular_bounded(const QString& path, const QStringList& allowed_roots) -> QByteArray {
    const QString checked = checked_path(path, allowed_roots, true, false);
    int flags = O_RDONLY;
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    const QByteArray encoded = QFile::encodeName(checked);
    const int descriptor = ::open(encoded.constData(), flags);
    if (descriptor < 0) {
        fail(LiveSessionErrorCode::InvalidDescriptor, "runtime descriptor cannot be opened");
    }
    QByteArray bytes;
    struct stat before{};
    struct stat after{};
    const auto close_descriptor = [&]() { ::close(descriptor); };
    if (::fstat(descriptor, &before) != 0 || !S_ISREG(before.st_mode) || before.st_size < 0 || before.st_size > MAX_DESCRIPTOR_BYTES) {
        close_descriptor();
        fail(LiveSessionErrorCode::InvalidDescriptor, "runtime descriptor exceeds its regular-file bound");
    }
    bytes.resize(static_cast<qsizetype>(before.st_size));
    qsizetype offset = 0;
    while (offset < bytes.size()) {
        const ssize_t result = ::read(descriptor, bytes.data() + offset, static_cast<size_t>(bytes.size() - offset));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            close_descriptor();
            fail(LiveSessionErrorCode::InvalidDescriptor, "runtime descriptor changed or became unreadable");
        }
        offset += result;
    }
    if (::fstat(descriptor, &after) != 0) {
        close_descriptor();
        fail(LiveSessionErrorCode::InvalidDescriptor, "runtime descriptor metadata became unavailable");
    }
    close_descriptor();
    if (before.st_dev != after.st_dev || before.st_ino != after.st_ino || before.st_size != after.st_size ||
        before.st_mtim.tv_sec != after.st_mtim.tv_sec || before.st_mtim.tv_nsec != after.st_mtim.tv_nsec) {
        fail(LiveSessionErrorCode::InvalidDescriptor, "runtime descriptor changed while it was read");
    }
    return bytes;
}

void check_json_bounds(const QJsonValue& value, int depth, int& members) {
    if (depth > MAX_DESCRIPTOR_DEPTH || ++members > MAX_DESCRIPTOR_MEMBERS) {
        fail(LiveSessionErrorCode::InvalidDescriptor, "runtime descriptor exceeds structural bounds");
    }
    if (value.isArray()) {
        for (const QJsonValue& child : value.toArray()) {
            check_json_bounds(child, depth + 1, members);
        }
    } else if (value.isObject()) {
        const QJsonObject object = value.toObject();
        members += object.size();
        if (members > MAX_DESCRIPTOR_MEMBERS) {
            fail(LiveSessionErrorCode::InvalidDescriptor, "runtime descriptor has too many members");
        }
        for (const QJsonValue& child : object) {
            check_json_bounds(child, depth + 1, members);
        }
    }
}

auto process_start_ticks(qint64 pid) -> std::optional<uint64_t> {
#if defined(Q_OS_LINUX)
    QFile stat_file(QString("/proc/%1/stat").arg(pid));
    if (!stat_file.open(QIODevice::ReadOnly)) {
        return std::nullopt;
    }
    const QByteArray bytes = stat_file.read(16 * 1024);
    const qsizetype closing = bytes.lastIndexOf(')');
    if (closing < 0) {
        return std::nullopt;
    }
    const QList<QByteArray> fields = bytes.sliced(closing + 1).simplified().split(' ');
    if (fields.size() <= 19) {
        return std::nullopt;
    }
    bool ok = false;
    const uint64_t ticks = fields[19].toULongLong(&ok);
    return ok ? std::optional<uint64_t>(ticks) : std::nullopt;
#else
    Q_UNUSED(pid);
    return std::nullopt;
#endif
}

auto current_boot_id() -> QString {
#if defined(Q_OS_LINUX)
    QFile boot_id("/proc/sys/kernel/random/boot_id");
    if (boot_id.open(QIODevice::ReadOnly)) {
        return QString::fromLatin1(boot_id.read(128)).trimmed().toLower();
    }
#endif
    return {};
}

void verify_process_identity(qint64 pid, const QString& expected_ticks) {
    if (pid <= 0) {
        fail(LiveSessionErrorCode::InvalidDescriptor, "runtime target has an invalid PID");
    }
    if (::kill(static_cast<pid_t>(pid), 0) != 0 && errno == ESRCH) {
        fail(LiveSessionErrorCode::StaleProcess, "runtime target process no longer exists");
    }
    bool ok = false;
    const uint64_t expected = expected_ticks.toULongLong(&ok);
    if (!ok || expected == 0) {
        fail(LiveSessionErrorCode::InvalidDescriptor, "runtime target has an invalid process start identity");
    }
    const auto actual = process_start_ticks(pid);
    if (actual && *actual != expected) {
        fail(LiveSessionErrorCode::StaleProcess, "runtime target PID was reused");
    }
}

auto as_node_id(const QJsonValue& value) -> QString {
    if (value.isString()) {
        return value.toString().trimmed();
    }
    if (value.isDouble()) {
        const double number = value.toDouble();
        if (number >= 0 && number <= 9007199254740991.0 && number == std::floor(number)) {
            return QString::number(static_cast<qulonglong>(number));
        }
    }
    return {};
}

auto hex_u64(uint64_t value) -> QString { return QString("0x%1").arg(value, 16, 16, QLatin1Char('0')); }

auto little_u64(const QByteArray& bytes) -> std::optional<uint64_t> {
    if (bytes.size() < 8) {
        return std::nullopt;
    }
    uint64_t value = 0;
    for (int index = 0; index < 8; ++index) {
        value |= static_cast<uint64_t>(static_cast<unsigned char>(bytes[index])) << (index * 8U);
    }
    return value;
}

}  // namespace

auto live_session_error_code(LiveSessionErrorCode code) -> QString {
    switch (code) {
        case LiveSessionErrorCode::Disabled:
            return "disabled";
        case LiveSessionErrorCode::InvalidArgument:
            return "invalid_argument";
        case LiveSessionErrorCode::InvalidDescriptor:
            return "invalid_descriptor";
        case LiveSessionErrorCode::UnsafePath:
            return "unsafe_path";
        case LiveSessionErrorCode::EndpointRejected:
            return "endpoint_rejected";
        case LiveSessionErrorCode::StaleProcess:
            return "stale_process";
        case LiveSessionErrorCode::DuplicateTarget:
            return "duplicate_target";
        case LiveSessionErrorCode::TargetNotFound:
            return "target_not_found";
        case LiveSessionErrorCode::TargetBusy:
            return "target_busy";
        case LiveSessionErrorCode::SessionNotFound:
            return "session_not_found";
        case LiveSessionErrorCode::LeaseExpired:
            return "lease_expired";
        case LiveSessionErrorCode::LeaseOwnerMismatch:
            return "lease_owner_mismatch";
        case LiveSessionErrorCode::LeaseTokenMismatch:
            return "lease_token_mismatch";
        case LiveSessionErrorCode::LimitExceeded:
            return "limit_exceeded";
        case LiveSessionErrorCode::SensitiveConfirmationRequired:
            return "sensitive_confirmation_required";
        case LiveSessionErrorCode::BuildIdUnavailable:
            return "build_id_unavailable";
        case LiveSessionErrorCode::SymbolsQuarantined:
            return "symbols_quarantined";
        case LiveSessionErrorCode::ProtocolFailure:
            return "protocol_failure";
        case LiveSessionErrorCode::ShuttingDown:
            return "shutting_down";
    }
    return "protocol_failure";
}

LiveSessionError::LiveSessionError(LiveSessionErrorCode code, const QString& message)
    : std::runtime_error(message.toStdString()), error_code(code), error_message(message) {}

class LiveSessionManager::Impl final {
   public:
    struct Target {
        LiveTargetSettings settings;
        QString source;
        qint64 pid = 0;
        QString process_start_ticks;
        QString local_build_id;
        QString local_symbol_status;
    };

    struct Connection {
        Target target;
        std::shared_ptr<QmpAdapter> qmp;
        std::optional<QemuPauseLease> pause;
        std::unique_ptr<GdbRspAdapter> rsp;
        ImageCatalog images;
        QString remote_build_id;
        QString quarantine_reason;
        uint64_t symbol_load_base = 0;
        std::unique_ptr<wosdbg::SymbolTable> symbols;
        std::unique_ptr<wosdbg::SectionMap> sections;
        QString paused_unix_ns;
    };

    struct Session {
        QString id;
        QString token;
        QString owner;
        QString audit_id;
        std::chrono::steady_clock::time_point expires;
        QString opened_unix_ns;
        std::shared_ptr<Transcript> transcript;
        std::vector<std::unique_ptr<Connection>> connections;
    };

    Impl(LiveSettings value, QStringList roots) : settings(std::move(value)) {
        if (roots.isEmpty()) {
            roots.append(QDir::currentPath());
        }
        for (const QString& root : roots) {
            const QString canonical = QFileInfo(root).canonicalFilePath();
            if (!canonical.isEmpty() && !allowed_roots.contains(canonical)) {
                allowed_roots.append(canonical);
            }
        }
        if (allowed_roots.isEmpty()) {
            fail(LiveSessionErrorCode::UnsafePath, "live debugging has no valid allowed root");
        }
    }

    auto operation() const -> OperationContext { return OperationContext::with_timeout(settings.operation_timeout_ms); }

    auto checked_ttl(int requested) const -> int {
        const int ttl = requested == 0 ? settings.lease_ms : requested;
        if (ttl < 1000 || ttl > settings.lease_ms) {
            fail(LiveSessionErrorCode::InvalidArgument, "lease TTL is outside the configured bound");
        }
        return ttl;
    }

    void require_enabled() const {
        if (!settings.enabled) {
            fail(LiveSessionErrorCode::Disabled, "live debugging is disabled by configuration");
        }
        if (shutting_down) {
            fail(LiveSessionErrorCode::ShuttingDown, "live session manager is shutting down");
        }
    }

    void validate_owner(const QString& owner) const {
        if (owner.isEmpty() || owner.toUtf8().size() > MAX_OWNER_BYTES) {
            fail(LiveSessionErrorCode::InvalidArgument, "lease owner is empty or too long");
        }
    }

    void validate_endpoint(const LiveTargetSettings& target) const {
        const QHostAddress address(target.host);
        if (address.isNull() || !address.isLoopback() || !settings.allowed_hosts.contains(target.host) || target.port == 0) {
            fail(LiveSessionErrorCode::EndpointRejected, "live target endpoint is not an allowlisted loopback address");
        }
        if (target.transport != "qemu" && target.transport != "debugserver") {
            fail(LiveSessionErrorCode::EndpointRejected, "live target transport is unsupported");
        }
        if (target.transport == "qemu" && target.qmp_socket.isEmpty()) {
            fail(LiveSessionErrorCode::EndpointRejected, "QEMU live target has no QMP endpoint");
        }
    }

    auto validate_target(Target target, bool runtime) const -> Target {
        static const QRegularExpression safe_id("^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$");
        if (!safe_id.match(target.settings.id).hasMatch() || target.settings.node_id.isEmpty()) {
            fail(LiveSessionErrorCode::InvalidDescriptor, "live target has an invalid target or node ID");
        }
        validate_endpoint(target.settings);
        if (!target.settings.qmp_socket.isEmpty()) {
            target.settings.qmp_socket = checked_path(target.settings.qmp_socket, allowed_roots, false, true);
        }
        if (!target.settings.symbol_path.isEmpty()) {
            target.settings.symbol_path = checked_path(target.settings.symbol_path, allowed_roots, true, false);
            if (QFileInfo(target.settings.symbol_path).size() > MAX_SYMBOL_BYTES) {
                fail(LiveSessionErrorCode::LimitExceeded, "live symbol image exceeds its file-size bound");
            }
            target.local_build_id = wosdbg::elf_build_id_from_file(target.settings.symbol_path).trimmed().toLower();
            target.local_symbol_status = target.local_build_id.isEmpty() ? "unavailable" : "verified-local";
        } else {
            target.local_symbol_status = "not-configured";
        }
        if (!target.settings.expected_build_id.isEmpty()) {
            const QString expected = target.settings.expected_build_id.trimmed().toLower();
            if (expected.size() < 2 || expected.size() > 128 || (expected.size() % 2) != 0 ||
                std::ranges::any_of(expected, [](QChar c) { return !c.isDigit() && (c < 'a' || c > 'f'); })) {
                fail(LiveSessionErrorCode::InvalidDescriptor, "live target expected build ID is malformed");
            }
            target.settings.expected_build_id = expected;
            if (!target.local_build_id.isEmpty() && target.local_build_id != expected) {
                target.local_symbol_status = "quarantined-local-build-id-mismatch";
            }
        }
        if (target.settings.log_paths.size() > MAX_LOG_PATHS) {
            fail(LiveSessionErrorCode::LimitExceeded, "live target has too many log paths");
        }
        QSet<QString> logs;
        for (QString& path : target.settings.log_paths) {
            path = checked_path(path, allowed_roots, false, false);
            if (logs.contains(path)) {
                fail(LiveSessionErrorCode::InvalidDescriptor, "live target repeats a log path");
            }
            logs.insert(path);
        }
        if (runtime) {
            verify_process_identity(target.pid, target.process_start_ticks);
        }
        return target;
    }

    auto parse_runtime_descriptor(const QString& path) const -> std::vector<Target> {
        const QByteArray bytes = read_regular_bounded(path, allowed_roots);
        QJsonParseError parse_error;
        const QJsonDocument document = QJsonDocument::fromJson(bytes, &parse_error);
        if (parse_error.error != QJsonParseError::NoError || !document.isObject()) {
            fail(LiveSessionErrorCode::InvalidDescriptor, "runtime descriptor is not valid JSON object data");
        }
        int members = 0;
        check_json_bounds(document.object(), 0, members);
        const QJsonObject root = document.object();
        if (root.value("format").toString() != "wosdbg-live" || root.value("version").toInt(-1) != 1 ||
            root.value("state").toString() != "running" || !root.value("targets").isArray()) {
            fail(LiveSessionErrorCode::InvalidDescriptor, "runtime descriptor format, version, state, or targets is invalid");
        }
        const QString descriptor_boot_id = root.value("bootId").toString().trimmed().toLower();
        const QString host_boot_id = current_boot_id();
        if (descriptor_boot_id.isEmpty() || (!host_boot_id.isEmpty() && descriptor_boot_id != host_boot_id)) {
            fail(LiveSessionErrorCode::StaleProcess, "runtime descriptor belongs to another host boot");
        }
        const QString launch_nonce = root.value("launchNonce").toString();
        if (launch_nonce.isEmpty() || launch_nonce.toUtf8().size() > 256) {
            fail(LiveSessionErrorCode::InvalidDescriptor, "runtime descriptor has an invalid launch nonce");
        }
        const QJsonArray target_values = root.value("targets").toArray();
        if (target_values.size() > settings.max_targets) {
            fail(LiveSessionErrorCode::LimitExceeded, "runtime descriptor exceeds the target bound");
        }
        const QJsonArray topology_nodes = root.value("topology").toObject().value("nodes").toArray();
        if (topology_nodes.isEmpty() || topology_nodes.size() > settings.max_targets) {
            fail(LiveSessionErrorCode::LimitExceeded, "runtime descriptor topology is empty or oversized");
        }
        QSet<QString> declared_nodes;
        for (const QJsonValue& value : topology_nodes) {
            if (!value.isObject()) {
                fail(LiveSessionErrorCode::InvalidDescriptor, "runtime topology node is not an object");
            }
            const QString node_id = as_node_id(value.toObject().value("nodeId"));
            if (node_id.isEmpty() || declared_nodes.contains(node_id)) {
                fail(LiveSessionErrorCode::DuplicateTarget, "runtime topology repeats or omits a node ID");
            }
            declared_nodes.insert(node_id);
        }
        std::vector<Target> parsed;
        parsed.reserve(static_cast<size_t>(target_values.size()));
        QSet<QString> ids;
        QSet<QString> nodes;
        for (const QJsonValue& value : target_values) {
            if (!value.isObject()) {
                fail(LiveSessionErrorCode::InvalidDescriptor, "runtime descriptor target is not an object");
            }
            const QJsonObject object = value.toObject();
            const QString id = object.value("id").toString().trimmed();
            const QString node_id = as_node_id(object.value("nodeId"));
            if (ids.contains(id) || nodes.contains(node_id) || !declared_nodes.contains(node_id)) {
                fail(LiveSessionErrorCode::DuplicateTarget, "runtime descriptor repeats a target or node ID");
            }
            ids.insert(id);
            nodes.insert(node_id);
            // Non-debug nodes are retained in topology descriptors but are not
            // connectable live targets.  Do not infer or probe an endpoint.
            if (!object.value("debug").toBool(false) || object.value("port").isNull()) {
                continue;
            }
            Target target;
            target.source = QFileInfo(path).fileName();
            target.settings.id = id;
            target.settings.node_id = node_id;
            target.settings.transport = object.value("transport").toString().trimmed().toLower();
            target.settings.host = object.value("host").toString().trimmed();
            const int port = object.value("port").toInt();
            target.settings.port = port > 0 && port <= 65535 ? static_cast<quint16>(port) : 0;
            target.settings.qmp_socket = object.value("qmpSocket").toString();
            target.settings.symbol_path = object.value("symbolPath").toString();
            target.settings.expected_build_id = object.value("expectedBuildId").toString();
            const QJsonArray log_paths = object.value("logPaths").toArray();
            for (const QJsonValue& log_path : log_paths) {
                if (!log_path.isString()) {
                    fail(LiveSessionErrorCode::InvalidDescriptor, "runtime target log path is not a string");
                }
                target.settings.log_paths.append(log_path.toString());
            }
            target.pid = object.value("pid").toInteger();
            target.process_start_ticks = object.value("processStartTicks").toString();
            parsed.push_back(validate_target(std::move(target), true));
        }
        return parsed;
    }

    void add_target(std::map<QString, Target>& result, QSet<QString>& nodes, Target target) const {
        if (result.contains(target.settings.id) || nodes.contains(target.settings.node_id)) {
            fail(LiveSessionErrorCode::DuplicateTarget, "target discovery found a duplicate target or node ID");
        }
        nodes.insert(target.settings.node_id);
        result.emplace(target.settings.id, std::move(target));
        if (std::cmp_greater(result.size(), settings.max_targets)) {
            fail(LiveSessionErrorCode::LimitExceeded, "target discovery exceeds the configured bound");
        }
    }

    auto discover_locked() -> QJsonObject {
        require_enabled();
        std::map<QString, Target> discovered;
        QSet<QString> nodes;
        for (const LiveTargetSettings& configured : settings.targets) {
            Target target;
            target.settings = configured;
            target.source = "configuration";
            add_target(discovered, nodes, validate_target(std::move(target), false));
        }
        for (const QString& descriptor : settings.runtime_descriptors) {
            for (Target& target : parse_runtime_descriptor(descriptor)) {
                add_target(discovered, nodes, std::move(target));
            }
        }
        targets = std::move(discovered);
        return targets_json();
    }

    auto targets_json() const -> QJsonObject {
        QJsonArray entries;
        for (const auto& [id, target] : targets) {
            QJsonArray logs;
            for (const QString& path : target.settings.log_paths) {
                logs.append(QFileInfo(path).fileName());
            }
            entries.append(QJsonObject{{"id", id},
                                       {"nodeId", target.settings.node_id},
                                       {"transport", target.settings.transport},
                                       {"host", target.settings.host},
                                       {"port", static_cast<int>(target.settings.port)},
                                       {"source", target.source},
                                       {"symbolFile", QFileInfo(target.settings.symbol_path).fileName()},
                                       {"localBuildId", target.local_build_id},
                                       {"symbolStatus", target.local_symbol_status},
                                       {"logFiles", logs}});
        }
        return success(QJsonObject{
            {"enabled", settings.enabled}, {"targetCount", entries.size()}, {"maxTargets", settings.max_targets}, {"targets", entries}});
    }

    auto find_connection(Session& session, const QString& target_id) const -> Connection& {
        const auto found =
            std::ranges::find_if(session.connections, [&](const auto& connection) { return connection->target.settings.id == target_id; });
        if (found == session.connections.end()) {
            fail(LiveSessionErrorCode::TargetNotFound, "target is not part of the live session");
        }
        return **found;
    }

    auto checked_session(const QString& session_id, const QString& token, const QString& owner) -> Session& {
        const auto found = sessions.find(session_id.toStdString());
        if (found == sessions.end()) {
            fail(LiveSessionErrorCode::SessionNotFound, "live session does not exist");
        }
        Session& session = *found->second;
        if (session.owner != owner) {
            fail(LiveSessionErrorCode::LeaseOwnerMismatch, "live session owner does not match");
        }
        if (!tokens_equal(session.token, token)) {
            fail(LiveSessionErrorCode::LeaseTokenMismatch, "live session lease token does not match");
        }
        if (std::chrono::steady_clock::now() >= session.expires) {
            close_connections_reverse(session);
            sessions.erase(found);
            fail(LiveSessionErrorCode::LeaseExpired, "live session lease expired");
        }
        return session;
    }

    auto status_json(const Session& session) const -> QJsonObject {
        const auto remaining = std::max<int64_t>(
            0, std::chrono::duration_cast<std::chrono::milliseconds>(session.expires - std::chrono::steady_clock::now()).count());
        QJsonArray target_entries;
        for (const auto& connection : session.connections) {
            target_entries.append(QJsonObject{{"id", connection->target.settings.id},
                                              {"nodeId", connection->target.settings.node_id},
                                              {"transport", connection->target.settings.transport},
                                              {"pauseMode", connection->target.settings.transport == "qemu" ? "qmp-lease" : "external"},
                                              {"ownsPause", connection->pause && connection->pause->owns_pause()},
                                              {"pausedUnixNs", connection->paused_unix_ns},
                                              {"localBuildId", connection->target.local_build_id},
                                              {"remoteBuildId", connection->remote_build_id},
                                              {"symbolsQuarantined", !connection->quarantine_reason.isEmpty()},
                                              {"quarantineReason", connection->quarantine_reason}});
        }
        return success(QJsonObject{
            {"sessionId", session.id},
            {"leaseOwner", session.owner},
            {"auditId", session.audit_id},
            {"openedUnixNs", session.opened_unix_ns},
            {"leaseMsRemaining", static_cast<qint64>(remaining)},
            {"readOnly", true},
            {"capabilities", QJsonArray{"registers", "memory-read", "backtrace", "symbols", "snapshot"}},
            {"limits", QJsonObject{{"maxMemoryBytes", settings.max_memory_bytes},
                                   {"maxTranscriptBytes", settings.max_transcript_bytes},
                                   {"operationTimeoutMs", settings.operation_timeout_ms},
                                   {"leaseMs", settings.lease_ms}}},
            {"clock",
             QJsonObject{{"domain", "host-realtime"}, {"quality", "host-sequential-snapshots"}, {"globalGuestOrderAvailable", false}}},
            {"targetCount", target_entries.size()},
            {"targets", target_entries}});
    }

    void verify_remote_images(Connection& connection, const OperationContext& context) const {
        try {
            connection.images = connection.rsp->image_catalog(context);
        } catch (const ProtocolError& error) {
            if (error.code() == ProtocolErrorCode::Unsupported) {
                if (connection.target.settings.transport != "qemu" || connection.target.local_build_id.isEmpty()) {
                    connection.quarantine_reason = "remote-image-catalog-unavailable";
                    return;
                }
                const auto note = local_elf_build_id_note(connection.target.settings.symbol_path);
                if (!note) {
                    connection.quarantine_reason = "local-build-id-note-unavailable";
                    return;
                }
                const QByteArray remote = connection.rsp->read_memory(note->runtime_address, note->bytes.size(), context);
                if (remote != note->bytes || QString::fromLatin1(remote.toHex()).toLower() != connection.target.local_build_id) {
                    connection.quarantine_reason = "remote-build-id-mismatch";
                    return;
                }
                connection.remote_build_id = connection.target.local_build_id;
                connection.symbols = wosdbg::load_symbols_from_file(connection.target.settings.symbol_path);
                connection.sections = wosdbg::load_sections_from_file(connection.target.settings.symbol_path);
                if (!connection.symbols && !connection.sections) {
                    connection.quarantine_reason = "local-symbols-unavailable";
                }
                return;
            }
            throw;
        }
        QString expected = connection.target.settings.expected_build_id;
        if (expected.isEmpty()) {
            expected = connection.target.local_build_id;
        }
        if (expected.isEmpty()) {
            connection.quarantine_reason = "local-build-id-unavailable";
            return;
        }
        const auto image = std::ranges::find_if(connection.images.images, [&](const ImageCatalogEntry& entry) {
            return QString::fromLatin1(entry.build_id.toHex()).toLower() == expected;
        });
        if (image == connection.images.images.end()) {
            connection.quarantine_reason = "remote-build-id-mismatch";
            return;
        }
        connection.remote_build_id = QString::fromLatin1(image->build_id.toHex()).toLower();
        connection.symbol_load_base = image->load_base;
        if (connection.target.local_build_id != expected || connection.target.local_symbol_status.startsWith("quarantined")) {
            connection.quarantine_reason = "local-build-id-mismatch";
            return;
        }
        connection.symbols = wosdbg::load_symbols_from_file(connection.target.settings.symbol_path, image->load_base);
        connection.sections = wosdbg::load_sections_from_file(connection.target.settings.symbol_path, image->load_base);
        if (!connection.symbols && !connection.sections) {
            connection.quarantine_reason = "local-symbols-unavailable";
        }
    }

    void close_connections_reverse(Session& session) noexcept {
        for (auto iterator = session.connections.rbegin(); iterator != session.connections.rend(); ++iterator) {
            Connection& connection = **iterator;
            if (connection.rsp) {
                connection.rsp->close();
            }
            if (connection.pause) {
                try {
                    connection.pause->release(OperationContext::with_timeout(std::min(settings.operation_timeout_ms, 2000)));
                } catch (...) {
                }
                connection.pause.reset();
            }
            if (connection.qmp) {
                connection.qmp->close();
            }
        }
        session.connections.clear();
    }

    auto sweep_expired_locked() -> QJsonArray {
        QJsonArray closed;
        const auto now = std::chrono::steady_clock::now();
        for (auto iterator = sessions.begin(); iterator != sessions.end();) {
            if (iterator->second->expires > now) {
                ++iterator;
                continue;
            }
            closed.append(iterator->second->id);
            close_connections_reverse(*iterator->second);
            iterator = sessions.erase(iterator);
        }
        return closed;
    }

    auto resolved(Connection& connection, uint64_t address) const -> QString {
        if (!connection.quarantine_reason.isEmpty() || (!connection.symbols && !connection.sections)) {
            return {};
        }
        std::vector<wosdbg::SymbolTable*> symbols;
        std::vector<wosdbg::SectionMap*> sections;
        if (connection.symbols) {
            symbols.push_back(connection.symbols.get());
        }
        if (connection.sections) {
            sections.push_back(connection.sections.get());
        }
        const auto result = wosdbg::resolve_address(address, symbols, sections);
        return result ? QString::fromStdString(*result) : QString{};
    }

    LiveSettings settings;
    QStringList allowed_roots;
    std::map<QString, Target> targets;
    mutable std::map<std::string, std::unique_ptr<Session>> sessions;
    mutable QMutex mutex;
    bool shutting_down = false;
};

LiveSessionManager::LiveSessionManager(LiveSettings settings, QStringList allowed_roots)
    : impl(std::make_unique<Impl>(std::move(settings), std::move(allowed_roots))) {}

LiveSessionManager::~LiveSessionManager() { shutdown(); }

auto LiveSessionManager::discover_targets() -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    return impl->discover_locked();
}

auto LiveSessionManager::list_targets() const -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    impl->require_enabled();
    return impl->targets_json();
}

auto LiveSessionManager::open_session(const QStringList& target_ids, const QString& owner, const QString& audit_id, int ttl_ms,
                                      std::stop_token cancellation) -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    impl->require_enabled();
    impl->validate_owner(owner);
    if (audit_id.isEmpty() || audit_id.toUtf8().size() > 160) {
        fail(LiveSessionErrorCode::InvalidArgument, "live session auditId is empty or too long");
    }
    const int ttl = impl->checked_ttl(ttl_ms);
    if (target_ids.isEmpty() || target_ids.size() > impl->settings.max_targets) {
        fail(LiveSessionErrorCode::InvalidArgument, "live session target list is empty or oversized");
    }
    impl->sweep_expired_locked();
    if (std::cmp_greater_equal(impl->sessions.size(), impl->settings.max_sessions)) {
        fail(LiveSessionErrorCode::LimitExceeded, "live session limit reached");
    }
    if (impl->targets.empty()) {
        impl->discover_locked();
    }
    QStringList requested = target_ids;
    requested.removeDuplicates();
    if (requested.size() != target_ids.size()) {
        fail(LiveSessionErrorCode::DuplicateTarget, "live session repeats a target ID");
    }
    std::sort(requested.begin(), requested.end());
    for (const QString& id : requested) {
        if (!impl->targets.contains(id)) {
            fail(LiveSessionErrorCode::TargetNotFound, "live target does not exist");
        }
        for (const auto& [unused, active] : impl->sessions) {
            Q_UNUSED(unused);
            if (std::ranges::any_of(active->connections, [&](const auto& connection) { return connection->target.settings.id == id; })) {
                fail(LiveSessionErrorCode::TargetBusy, "live target already has an active pause lease");
            }
        }
    }

    auto session = std::make_unique<Impl::Session>();
    session->id = random_token();
    session->token = random_token();
    session->owner = owner;
    session->audit_id = audit_id;
    session->expires = std::chrono::steady_clock::now() + std::chrono::milliseconds(ttl);
    session->opened_unix_ns =
        QString::number(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    session->transcript = std::make_shared<Transcript>(
        TranscriptLimits{.max_records = 4096, .max_total_bytes = impl->settings.max_transcript_bytes, .max_record_bytes = 64 * 1024});
    try {
        // Phase one: acquire every QMP pause in deterministic target order.
        for (const QString& id : requested) {
            auto connection = std::make_unique<Impl::Connection>();
            connection->target = impl->targets.at(id);
            if (connection->target.settings.transport == "qemu") {
                connection->qmp = std::make_shared<QmpAdapter>(connection->target.settings.qmp_socket, QmpLimits{}, session->transcript);
                const OperationContext context = OperationContext::with_timeout(impl->settings.operation_timeout_ms, cancellation);
                connection->qmp->connect(context);
                connection->pause.emplace(QemuPauseLease::acquire(connection->qmp, context));
                connection->paused_unix_ns = QString::number(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
            }
            session->connections.push_back(std::move(connection));
        }
        // Phase two: no RSP read occurs until all QEMU targets are paused.
        for (auto& connection : session->connections) {
            connection->rsp =
                std::make_unique<GdbRspAdapter>(connection->target.settings.host, connection->target.settings.port,
                                                RspLimits{.max_memory_read_bytes = impl->settings.max_memory_bytes}, session->transcript);
            const OperationContext context = OperationContext::with_timeout(impl->settings.operation_timeout_ms, cancellation);
            connection->rsp->connect(context);
            impl->verify_remote_images(*connection, context);
        }
    } catch (const ProtocolError& error) {
        impl->close_connections_reverse(*session);
        fail(LiveSessionErrorCode::ProtocolFailure, QString("live protocol operation failed (%1)").arg(static_cast<int>(error.code())));
    } catch (...) {
        impl->close_connections_reverse(*session);
        throw;
    }
    const QString token = session->token;
    QJsonObject result = impl->status_json(*session);
    result["leaseToken"] = token;
    impl->sessions.emplace(session->id.toStdString(), std::move(session));
    return result;
}

auto LiveSessionManager::list_sessions(const QString& owner) const -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    impl->require_enabled();
    impl->sweep_expired_locked();
    QJsonArray entries;
    for (const auto& [unused, session] : impl->sessions) {
        Q_UNUSED(unused);
        if (owner.isEmpty() || session->owner == owner) {
            entries.append(impl->status_json(*session));
        }
    }
    return success(QJsonObject{{"sessionCount", entries.size()}, {"maxSessions", impl->settings.max_sessions}, {"sessions", entries}});
}

auto LiveSessionManager::session_status(const QString& session_id, const QString& lease_token, const QString& owner) const -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    impl->require_enabled();
    return impl->status_json(impl->checked_session(session_id, lease_token, owner));
}

auto LiveSessionManager::renew(const QString& session_id, const QString& lease_token, const QString& owner, int ttl_ms) -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    impl->require_enabled();
    Impl::Session& session = impl->checked_session(session_id, lease_token, owner);
    session.expires = std::chrono::steady_clock::now() + std::chrono::milliseconds(impl->checked_ttl(ttl_ms));
    return impl->status_json(session);
}

auto LiveSessionManager::close(const QString& session_id, const QString& lease_token, const QString& owner) -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    impl->require_enabled();
    Impl::Session& session = impl->checked_session(session_id, lease_token, owner);
    impl->close_connections_reverse(session);
    impl->sessions.erase(session_id.toStdString());
    return success(QJsonObject{{"closed", true}, {"sessionId", session_id}});
}

auto LiveSessionManager::close_owner(const QString& owner) -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    if (!impl->settings.enabled) {
        return success(QJsonObject{{"closedCount", 0}, {"sessionIds", QJsonArray{}}});
    }
    impl->require_enabled();
    impl->validate_owner(owner);
    QJsonArray closed;
    for (auto iterator = impl->sessions.begin(); iterator != impl->sessions.end();) {
        if (iterator->second->owner != owner) {
            ++iterator;
            continue;
        }
        closed.append(iterator->second->id);
        impl->close_connections_reverse(*iterator->second);
        iterator = impl->sessions.erase(iterator);
    }
    return success(QJsonObject{{"closedCount", closed.size()}, {"sessionIds", closed}});
}

auto LiveSessionManager::sweep_expired() -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    if (!impl->settings.enabled) {
        return success(QJsonObject{{"closedCount", 0}, {"sessionIds", QJsonArray{}}});
    }
    impl->require_enabled();
    const QJsonArray closed = impl->sweep_expired_locked();
    return success(QJsonObject{{"closedCount", closed.size()}, {"sessionIds", closed}});
}

void LiveSessionManager::shutdown() {
    if (!impl) {
        return;
    }
    QMutexLocker lock(&impl->mutex);
    if (impl->shutting_down) {
        return;
    }
    impl->shutting_down = true;
    for (auto& [unused, session] : impl->sessions) {
        Q_UNUSED(unused);
        impl->close_connections_reverse(*session);
    }
    impl->sessions.clear();
}

auto LiveSessionManager::read_registers(const QString& session_id, const QString& lease_token, const QString& owner,
                                        const QString& target_id, std::stop_token cancellation) -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    impl->require_enabled();
    Impl::Connection& connection = impl->find_connection(impl->checked_session(session_id, lease_token, owner), target_id);
    try {
        const QVector<RegisterValue> values =
            connection.rsp->read_registers(OperationContext::with_timeout(impl->settings.operation_timeout_ms, cancellation));
        QJsonArray registers;
        for (const RegisterValue& value : values) {
            QJsonObject entry{{"name", value.descriptor.name},
                              {"regnum", static_cast<qint64>(value.descriptor.regnum)},
                              {"bitsize", static_cast<qint64>(value.descriptor.bitsize)},
                              {"available", value.available}};
            if (value.available) {
                entry["bytesHex"] = QString::fromLatin1(value.bytes.toHex());
                if (value.little_endian_u64) {
                    entry["value"] = hex_u64(*value.little_endian_u64);
                }
            }
            registers.append(entry);
        }
        return success(QJsonObject{{"targetId", target_id}, {"registerCount", registers.size()}, {"registers", registers}});
    } catch (const ProtocolError& error) {
        fail(LiveSessionErrorCode::ProtocolFailure,
             QString("register read failed (%1): %2").arg(static_cast<int>(error.code())).arg(error.message()));
    }
}

auto LiveSessionManager::read_memory(const QString& session_id, const QString& lease_token, const QString& owner, const QString& target_id,
                                     uint64_t address, int length, bool confirm_sensitive, std::stop_token cancellation) -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    impl->require_enabled();
    if (!confirm_sensitive) {
        fail(LiveSessionErrorCode::SensitiveConfirmationRequired, "memory reads require explicit sensitive-payload confirmation");
    }
    if (length <= 0 || length > impl->settings.max_memory_bytes || address > std::numeric_limits<uint64_t>::max() - length) {
        fail(LiveSessionErrorCode::LimitExceeded, "memory read exceeds the configured bound");
    }
    Impl::Connection& connection = impl->find_connection(impl->checked_session(session_id, lease_token, owner), target_id);
    try {
        const QByteArray data =
            connection.rsp->read_memory(address, length, OperationContext::with_timeout(impl->settings.operation_timeout_ms, cancellation));
        return success(QJsonObject{{"targetId", target_id},
                                   {"address", hex_u64(address)},
                                   {"size", data.size()},
                                   {"encoding", "base64"},
                                   {"data", QString::fromLatin1(data.toBase64())},
                                   {"sha256", QString::fromLatin1(QCryptographicHash::hash(data, QCryptographicHash::Sha256).toHex())},
                                   {"sensitive", true}});
    } catch (const ProtocolError& error) {
        fail(LiveSessionErrorCode::ProtocolFailure, QString("memory read failed (%1)").arg(static_cast<int>(error.code())));
    }
}

auto LiveSessionManager::frame_pointer_backtrace(const QString& session_id, const QString& lease_token, const QString& owner,
                                                 const QString& target_id, int max_frames, std::stop_token cancellation) -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    impl->require_enabled();
    if (max_frames <= 0 || max_frames > MAX_BACKTRACE_FRAMES || max_frames * 16 > impl->settings.max_memory_bytes) {
        fail(LiveSessionErrorCode::LimitExceeded, "backtrace exceeds the configured frame or memory bound");
    }
    Impl::Connection& connection = impl->find_connection(impl->checked_session(session_id, lease_token, owner), target_id);
    try {
        const OperationContext context = OperationContext::with_timeout(impl->settings.operation_timeout_ms, cancellation);
        const QVector<RegisterValue> registers = connection.rsp->read_registers(context);
        std::optional<uint64_t> rbp;
        std::optional<uint64_t> rip;
        for (const RegisterValue& value : registers) {
            const QString name = value.descriptor.name.toLower();
            if (name == "rbp") {
                rbp = value.little_endian_u64;
            } else if (name == "rip") {
                rip = value.little_endian_u64;
            }
        }
        if (!rbp || !rip) {
            fail(LiveSessionErrorCode::ProtocolFailure, "target register layout lacks RIP or RBP");
        }
        QJsonArray frames;
        frames.append(QJsonObject{{"index", 0}, {"address", hex_u64(*rip)}, {"symbol", impl->resolved(connection, *rip)}});
        uint64_t frame_pointer = *rbp;
        for (int index = 1; index < max_frames && frame_pointer != 0; ++index) {
            if ((frame_pointer & 7U) != 0 || frame_pointer > std::numeric_limits<uint64_t>::max() - 16) {
                break;
            }
            const QByteArray frame = connection.rsp->read_memory(frame_pointer, 16, context);
            const auto next = little_u64(frame.first(8));
            const auto return_address = little_u64(frame.sliced(8, 8));
            if (!next || !return_address || *return_address == 0) {
                break;
            }
            frames.append(QJsonObject{{"index", index},
                                      {"framePointer", hex_u64(frame_pointer)},
                                      {"address", hex_u64(*return_address)},
                                      {"symbol", impl->resolved(connection, *return_address)}});
            if (*next <= frame_pointer || *next - frame_pointer > MAX_FRAME_POINTER_STEP) {
                break;
            }
            frame_pointer = *next;
        }
        return success(QJsonObject{{"targetId", target_id},
                                   {"frameCount", frames.size()},
                                   {"symbolsVerified", connection.quarantine_reason.isEmpty()},
                                   {"frames", frames}});
    } catch (const ProtocolError& error) {
        fail(LiveSessionErrorCode::ProtocolFailure, QString("backtrace read failed (%1)").arg(static_cast<int>(error.code())));
    }
}

auto LiveSessionManager::resolve_address(const QString& session_id, const QString& lease_token, const QString& owner,
                                         const QString& target_id, uint64_t address) -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    impl->require_enabled();
    Impl::Connection& connection = impl->find_connection(impl->checked_session(session_id, lease_token, owner), target_id);
    if (!connection.quarantine_reason.isEmpty() || (!connection.symbols && !connection.sections)) {
        fail(LiveSessionErrorCode::SymbolsQuarantined, QString("symbols are unavailable: %1").arg(connection.quarantine_reason));
    }
    return success(
        QJsonObject{{"targetId", target_id},
                    {"address", hex_u64(address)},
                    {"objectAddress", hex_u64(address >= connection.symbol_load_base ? address - connection.symbol_load_base : address)},
                    {"symbol", impl->resolved(connection, address)},
                    {"verified", true}});
}

auto LiveSessionManager::inspect_pte(const QString& session_id, const QString& lease_token, const QString& owner, const QString& target_id,
                                     uint64_t address, std::stop_token cancellation) -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    impl->require_enabled();
    Impl::Connection& connection = impl->find_connection(impl->checked_session(session_id, lease_token, owner), target_id);
    if (!connection.quarantine_reason.isEmpty() || !connection.symbols) {
        fail(LiveSessionErrorCode::SymbolsQuarantined, "verified kernel symbols are required for live PTE inspection");
    }
    try {
        const OperationContext context = OperationContext::with_timeout(impl->settings.operation_timeout_ms, cancellation);
        const QVector<RegisterValue> registers = connection.rsp->read_registers(context);
        std::optional<uint64_t> cr3;
        for (const RegisterValue& value : registers) {
            if (value.descriptor.name.compare("cr3", Qt::CaseInsensitive) == 0) {
                cr3 = value.little_endian_u64;
                break;
            }
        }
        if (!cr3) {
            fail(LiveSessionErrorCode::ProtocolFailure, "target register layout lacks CR3");
        }
        std::optional<uint64_t> hhdm_symbol;
        for (const wosdbg::SymbolEntry& symbol : connection.symbols->entries()) {
            const QString name = QString::fromStdString(symbol.name);
            const bool DEMANGLED_HHDM = name.contains("mm::addr::") && name.endsWith("hhdm_offset");
            const bool ITANIUM_HHDM = name.contains("3mod2mm4addr") && name.contains("hhdm_offset");
            if (DEMANGLED_HHDM || ITANIUM_HHDM) {
                hhdm_symbol = symbol.addr;
                break;
            }
        }
        if (!hhdm_symbol) {
            fail(LiveSessionErrorCode::ProtocolFailure, "verified symbols do not expose the WOS HHDM offset");
        }
        const auto hhdm = little_u64(connection.rsp->read_memory(*hhdm_symbol, 8, context));
        if (!hhdm || *hhdm == 0) {
            fail(LiveSessionErrorCode::ProtocolFailure, "live HHDM offset is unavailable");
        }
        constexpr uint64_t address_mask = 0x000ffffffffff000ULL;
        uint64_t table_phys = *cr3 & address_mask;
        const std::array<unsigned, 4> shifts = {39, 30, 21, 12};
        QJsonArray entries;
        bool present = true;
        QString page_size = "4K";
        uint64_t physical = 0;
        for (size_t level = 0; level < shifts.size(); ++level) {
            const uint64_t index = (address >> shifts[level]) & 0x1ffULL;
            if (table_phys > std::numeric_limits<uint64_t>::max() - *hhdm ||
                *hhdm + table_phys > std::numeric_limits<uint64_t>::max() - index * 8) {
                fail(LiveSessionErrorCode::LimitExceeded, "page-table address overflow");
            }
            const uint64_t entry_address = *hhdm + table_phys + index * 8;
            const auto entry = little_u64(connection.rsp->read_memory(entry_address, 8, context));
            if (!entry) {
                fail(LiveSessionErrorCode::ProtocolFailure, "short page-table read");
            }
            entries.append(QJsonObject{{"level", static_cast<int>(4 - level)},
                                       {"index", static_cast<qint64>(index)},
                                       {"entryAddress", hex_u64(entry_address)},
                                       {"value", hex_u64(*entry)},
                                       {"present", (*entry & 1U) != 0},
                                       {"writable", (*entry & 2U) != 0},
                                       {"user", (*entry & 4U) != 0},
                                       {"noExecute", (*entry & (1ULL << 63)) != 0}});
            if ((*entry & 1U) == 0) {
                present = false;
                break;
            }
            const bool huge = level == 1 || level == 2 ? (*entry & (1U << 7)) != 0 : false;
            if (huge) {
                const unsigned offset_bits = level == 1 ? 30 : 21;
                const uint64_t page_mask = ~((1ULL << offset_bits) - 1);
                physical = (*entry & address_mask & page_mask) | (address & ~page_mask);
                page_size = level == 1 ? "1G" : "2M";
                break;
            }
            table_phys = *entry & address_mask;
            if (level == shifts.size() - 1) {
                physical = table_phys | (address & 0xfffULL);
            }
        }
        return success(QJsonObject{{"targetId", target_id},
                                   {"address", hex_u64(address)},
                                   {"cr3", hex_u64(*cr3)},
                                   {"hhdmOffset", hex_u64(*hhdm)},
                                   {"present", present},
                                   {"pageSize", page_size},
                                   {"physicalAddress", present ? hex_u64(physical) : QString()},
                                   {"entries", entries},
                                   {"verifiedSymbols", true}});
    } catch (const ProtocolError& error) {
        fail(LiveSessionErrorCode::ProtocolFailure, QString("live PTE read failed (%1)").arg(static_cast<int>(error.code())));
    }
}

auto LiveSessionManager::transcript(const QString& session_id, const QString& lease_token, const QString& owner,
                                    bool include_sensitive_payloads, bool confirm_sensitive) const -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    impl->require_enabled();
    if (include_sensitive_payloads && !confirm_sensitive) {
        fail(LiveSessionErrorCode::SensitiveConfirmationRequired,
             "transcript payload export requires explicit sensitive-payload confirmation");
    }
    const Impl::Session& session = impl->checked_session(session_id, lease_token, owner);
    QJsonArray entries;
    for (const TranscriptRecord& record : session.transcript->records()) {
        QJsonObject entry{
            {"sequence", static_cast<qint64>(record.sequence)},
            {"protocol", sanitized_text(record.protocol, 32)},
            {"direction", direction_name(record.direction)},
            {"kind", sanitized_text(record.kind, 64)},
            {"payloadBytes", record.payload.size()},
            {"payloadSha256", QString::fromLatin1(QCryptographicHash::hash(record.payload, QCryptographicHash::Sha256).toHex())},
            {"payloadRedacted", !include_sensitive_payloads},
            {"truncated", record.truncated}};
        if (include_sensitive_payloads) {
            entry["payloadEncoding"] = "base64";
            entry["payload"] = QString::fromLatin1(record.payload.toBase64());
        }
        entries.append(entry);
    }
    QJsonObject body{{"format", "wosdbg-live-transcript"},
                     {"version", 1},
                     {"records", entries},
                     {"discardedRecords", static_cast<qint64>(session.transcript->discarded_records())},
                     {"discardedBytes", static_cast<qint64>(session.transcript->discarded_bytes())},
                     {"containsSensitivePayloads", include_sensitive_payloads}};
    const QByteArray canonical = QJsonDocument(body).toJson(QJsonDocument::Compact);
    if (canonical.size() > impl->settings.max_transcript_bytes) {
        fail(LiveSessionErrorCode::LimitExceeded, "serialized transcript exceeds the configured bound");
    }
    return success(QJsonObject{
        {"transcript", body},
        {"digest",
         QString("sha256:%1").arg(QString::fromLatin1(QCryptographicHash::hash(canonical, QCryptographicHash::Sha256).toHex()))}});
}

auto LiveSessionManager::capture_sources(const QString& session_id, const QString& lease_token, const QString& owner) const -> QJsonObject {
    QMutexLocker lock(&impl->mutex);
    impl->require_enabled();
    const Impl::Session& session = impl->checked_session(session_id, lease_token, owner);
    QJsonArray targets;
    for (const auto& connection : session.connections) {
        QJsonArray logs;
        for (const QString& path : connection->target.settings.log_paths) {
            logs.append(path);
        }
        targets.append(QJsonObject{{"id", connection->target.settings.id},
                                   {"nodeId", connection->target.settings.node_id},
                                   {"symbolPath", connection->target.settings.symbol_path},
                                   {"localBuildId", connection->target.local_build_id},
                                   {"remoteBuildId", connection->remote_build_id},
                                   {"logPaths", logs}});
    }
    return success(QJsonObject{{"targets", targets}});
}

}  // namespace wosdbg::live
