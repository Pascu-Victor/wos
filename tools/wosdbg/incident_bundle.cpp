#include "incident_bundle.h"

#include <archive.h>
#include <archive_entry.h>

#include <QByteArrayView>
#include <QCryptographicHash>
#include <QDir>
#include <QFile>
#include <QFileInfo>
#include <QHash>
#include <QJsonDocument>
#include <QJsonParseError>
#include <QRegularExpression>
#include <QSet>
#include <QtGlobal>
#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <limits>
#include <optional>
#include <utility>

#ifdef Q_OS_UNIX
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace wosdbg {
namespace {

constexpr qsizetype COPY_BUFFER_SIZE = 64 * 1024;

struct SnapshotFile {
    QString path;
    QString absolute_path;
    uint64_t size = 0;
    QString sha256;
};

struct LoadContext {
    IncidentBundle& bundle;
    const IncidentLimits& limits;
    QHash<QString, SnapshotFile> files;
    QSet<QString> seen_paths;
    QSet<QString> file_paths;
    size_t entry_count = 0;
    uint64_t total_bytes = 0;
    uint64_t archive_bytes = 0;
};

void add_issue(LoadContext& context, QString code, QString message, QString path = {}, bool fatal = true, QJsonObject details = {}) {
    context.bundle.issues.push_back(IncidentIssue{
        .code = std::move(code), .message = std::move(message), .path = std::move(path), .fatal = fatal, .details = std::move(details)});
}

auto has_fatal_issue(const IncidentBundle& bundle) -> bool {
    return std::ranges::any_of(bundle.issues, [](const IncidentIssue& issue) { return issue.fatal; });
}

auto checked_add(uint64_t left, uint64_t right, uint64_t* result) -> bool {
    if (right > std::numeric_limits<uint64_t>::max() - left) {
        return false;
    }
    *result = left + right;
    return true;
}

auto checked_mul(uint64_t left, uint64_t right, uint64_t* result) -> bool {
    if (left != 0 && right > std::numeric_limits<uint64_t>::max() / left) {
        return false;
    }
    *result = left * right;
    return true;
}

auto normalize_member_path(const QString& raw_path, bool directory, const IncidentLimits& limits, QString* normalized, QString* reason)
    -> bool {
    QString path = raw_path;
    if (directory) {
        while (path.endsWith('/')) {
            path.chop(1);
        }
    }
    if (path.isEmpty()) {
        *reason = "empty path";
        return false;
    }
    if (path.contains(QChar::Null) || path.contains(QChar::ReplacementCharacter)) {
        *reason = "path contains NUL or invalid UTF-8";
        return false;
    }
    if (path.startsWith('/') || QDir::isAbsolutePath(path)) {
        *reason = "absolute path";
        return false;
    }
    if (path.contains('\\')) {
        *reason = "backslash is not permitted in bundle paths";
        return false;
    }
    const QByteArray UTF8 = path.toUtf8();
    if (static_cast<size_t>(UTF8.size()) > limits.max_path_length) {
        *reason = QString("path exceeds %1 UTF-8 bytes").arg(limits.max_path_length);
        return false;
    }
    const QStringList PARTS = path.split('/', Qt::KeepEmptyParts);
    if (PARTS.size() > static_cast<qsizetype>(limits.max_path_depth)) {
        *reason = QString("path exceeds depth %1").arg(limits.max_path_depth);
        return false;
    }
    for (const auto& part : PARTS) {
        if (part.isEmpty() || part == "." || part == "..") {
            *reason = "path contains empty, dot, or traversal component";
            return false;
        }
        for (const QChar ch : part) {
            if (ch.unicode() < 0x20 || ch.unicode() == 0x7f) {
                *reason = "path contains a control character";
                return false;
            }
        }
    }
    if (QDir::cleanPath(path) != path) {
        *reason = "path is not in normalized POSIX form";
        return false;
    }
    *normalized = path;
    return true;
}

auto register_entry(LoadContext& context, const QString& path, bool directory) -> bool {
    ++context.entry_count;
    if (context.entry_count > context.limits.max_members) {
        add_issue(context, "member_count_exceeded", QString("bundle contains more than %1 entries").arg(context.limits.max_members), path,
                  true, QJsonObject{{"limit", QString::number(context.limits.max_members)}});
        return false;
    }
    if (context.seen_paths.contains(path)) {
        add_issue(context, "duplicate_member", "bundle contains a duplicate normalized member path", path);
        return false;
    }

    QString prefix;
    const QStringList PARTS = path.split('/');
    for (qsizetype i = 0; i + 1 < PARTS.size(); ++i) {
        prefix = prefix.isEmpty() ? PARTS[i] : prefix + '/' + PARTS[i];
        if (context.file_paths.contains(prefix)) {
            add_issue(context, "member_path_conflict", "member descends from a path already declared as a regular file", path, true,
                      QJsonObject{{"conflict", prefix}});
            return false;
        }
    }
    if (!directory) {
        const QString DESCENDANT_PREFIX = path + '/';
        for (const auto& existing : std::as_const(context.seen_paths)) {
            if (existing.startsWith(DESCENDANT_PREFIX)) {
                add_issue(context, "member_path_conflict", "regular file conflicts with an existing descendant member", path, true,
                          QJsonObject{{"conflict", existing}});
                return false;
            }
        }
    }

    context.seen_paths.insert(path);
    if (!directory) {
        context.file_paths.insert(path);
    }
    return true;
}

auto prepare_output_file(LoadContext& context, const QString& member_path, QFile* output) -> bool {
    const QString ABSOLUTE = QDir(context.bundle.root_path).absoluteFilePath(member_path);
    const QString PARENT = QFileInfo(ABSOLUTE).absolutePath();
    if (!QDir().mkpath(PARENT)) {
        add_issue(context, "snapshot_write_failed", "could not create snapshot parent directory", member_path);
        return false;
    }
    output->setFileName(ABSOLUTE);
    if (!output->open(QIODevice::WriteOnly | QIODevice::NewOnly)) {
        add_issue(context, "snapshot_write_failed", QString("could not create snapshot member: %1").arg(output->errorString()),
                  member_path);
        return false;
    }
    return true;
}

auto account_bytes(LoadContext& context, const QString& path, uint64_t member_bytes, uint64_t next_chunk) -> bool {
    uint64_t next_member = 0;
    uint64_t next_total = 0;
    if (!checked_add(member_bytes, next_chunk, &next_member) || next_member > context.limits.max_member_bytes) {
        add_issue(context, "member_too_large", QString("member exceeds %1-byte limit").arg(context.limits.max_member_bytes), path, true,
                  QJsonObject{{"limit", QString::number(context.limits.max_member_bytes)}});
        return false;
    }
    if (!checked_add(context.total_bytes, next_chunk, &next_total) || next_total > context.limits.max_total_bytes) {
        add_issue(context, "expanded_size_exceeded", QString("expanded bundle exceeds %1-byte limit").arg(context.limits.max_total_bytes),
                  path, true, QJsonObject{{"limit", QString::number(context.limits.max_total_bytes)}});
        return false;
    }
    if (context.archive_bytes > 0 && context.limits.max_expansion_ratio > 0) {
        uint64_t ratio_limit = 0;
        if (!checked_mul(context.archive_bytes, context.limits.max_expansion_ratio, &ratio_limit)) {
            ratio_limit = std::numeric_limits<uint64_t>::max();
        }
        if (next_total > ratio_limit) {
            add_issue(context, "archive_expansion_ratio_exceeded",
                      QString("archive expansion exceeds configured %1:1 ratio").arg(context.limits.max_expansion_ratio), path, true,
                      QJsonObject{{"archiveBytes", QString::number(context.archive_bytes)},
                                  {"expandedBytes", QString::number(next_total)},
                                  {"ratioLimit", QString::number(context.limits.max_expansion_ratio)}});
            return false;
        }
    }
    context.total_bytes = next_total;
    return true;
}

void remember_snapshot_file(LoadContext& context, const QString& path, uint64_t size, const QCryptographicHash& hash) {
    context.files.insert(path, SnapshotFile{.path = path,
                                            .absolute_path = QDir(context.bundle.root_path).absoluteFilePath(path),
                                            .size = size,
                                            .sha256 = QString::fromLatin1(hash.result().toHex())});
}

#ifdef Q_OS_UNIX

class ScopedFd {
   public:
    explicit ScopedFd(int fd = -1) : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }
    ScopedFd(const ScopedFd&) = delete;
    auto operator=(const ScopedFd&) -> ScopedFd& = delete;
    ScopedFd(ScopedFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    auto operator=(ScopedFd&& other) noexcept -> ScopedFd& {
        if (this != &other) {
            if (fd_ >= 0) {
                ::close(fd_);
            }
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }
    [[nodiscard]] int get() const { return fd_; }
    [[nodiscard]] bool valid() const { return fd_ >= 0; }

   private:
    int fd_;
};

auto stat_is_sparse(const struct stat& info) -> bool {
#ifdef Q_OS_LINUX
    if (info.st_size <= 0) {
        return false;
    }
    const auto ALLOCATED = static_cast<uint64_t>(info.st_blocks) * 512ULL;
    return ALLOCATED < static_cast<uint64_t>(info.st_size);
#else
    Q_UNUSED(info);
    return false;
#endif
}

auto stat_changed(const struct stat& before, const struct stat& after) -> bool {
    if (before.st_dev != after.st_dev || before.st_ino != after.st_ino || before.st_size != after.st_size) {
        return true;
    }
#ifdef Q_OS_LINUX
    return before.st_mtim.tv_sec != after.st_mtim.tv_sec || before.st_mtim.tv_nsec != after.st_mtim.tv_nsec ||
           before.st_ctim.tv_sec != after.st_ctim.tv_sec || before.st_ctim.tv_nsec != after.st_ctim.tv_nsec;
#else
    return before.st_mtime != after.st_mtime || before.st_ctime != after.st_ctime;
#endif
}

auto copy_directory_file(LoadContext& context, int directory_fd, const QByteArray& name, const QString& path, const struct stat& expected)
    -> bool {
    if (expected.st_nlink > 1) {
        add_issue(context, "directory_hardlink_rejected", "directory member has multiple hard links", path);
        return false;
    }
    if (stat_is_sparse(expected)) {
        add_issue(context, "sparse_member_rejected", "sparse directory member is not permitted", path);
        return false;
    }
    if (expected.st_size < 0 || static_cast<uint64_t>(expected.st_size) > context.limits.max_member_bytes) {
        add_issue(context, "member_too_large", QString("directory member exceeds %1-byte limit").arg(context.limits.max_member_bytes),
                  path);
        return false;
    }
    uint64_t projected_total = 0;
    if (!checked_add(context.total_bytes, static_cast<uint64_t>(expected.st_size), &projected_total) ||
        projected_total > context.limits.max_total_bytes) {
        add_issue(context, "expanded_size_exceeded",
                  QString("directory snapshot exceeds %1-byte limit").arg(context.limits.max_total_bytes), path);
        return false;
    }

    ScopedFd input(::openat(directory_fd, name.constData(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (!input.valid()) {
        add_issue(context, "directory_member_open_failed", QString("openat failed: %1").arg(QString::fromLocal8Bit(std::strerror(errno))),
                  path);
        return false;
    }
    struct stat opened{};
    if (::fstat(input.get(), &opened) != 0 || !S_ISREG(opened.st_mode) || opened.st_dev != expected.st_dev ||
        opened.st_ino != expected.st_ino || stat_changed(expected, opened)) {
        add_issue(context, "directory_member_changed", "directory member changed while being opened", path);
        return false;
    }

    QFile output;
    if (!prepare_output_file(context, path, &output)) {
        return false;
    }
    QCryptographicHash hash(QCryptographicHash::Sha256);
    uint64_t copied = 0;
    char buffer[COPY_BUFFER_SIZE];
    while (true) {
        const ssize_t READ = ::read(input.get(), buffer, sizeof(buffer));
        if (READ == 0) {
            break;
        }
        if (READ < 0) {
            if (errno == EINTR) {
                continue;
            }
            output.close();
            QFile::remove(output.fileName());
            add_issue(context, "directory_member_read_failed", QString("read failed: %1").arg(QString::fromLocal8Bit(std::strerror(errno))),
                      path);
            return false;
        }
        const auto CHUNK = static_cast<uint64_t>(READ);
        uint64_t next_copied = 0;
        if (!checked_add(copied, CHUNK, &next_copied) || next_copied > context.limits.max_member_bytes) {
            output.close();
            QFile::remove(output.fileName());
            add_issue(context, "member_too_large", "directory member grew beyond configured limit", path);
            return false;
        }
        if (output.write(buffer, READ) != READ) {
            output.close();
            QFile::remove(output.fileName());
            add_issue(context, "snapshot_write_failed", QString("snapshot write failed: %1").arg(output.errorString()), path);
            return false;
        }
        hash.addData(QByteArrayView(buffer, READ));
        copied = next_copied;
    }
    if (!output.flush()) {
        const QString ERROR = output.errorString();
        output.close();
        QFile::remove(output.fileName());
        add_issue(context, "snapshot_write_failed", QString("snapshot flush failed: %1").arg(ERROR), path);
        return false;
    }
    output.close();
    struct stat after{};
    if (::fstat(input.get(), &after) != 0 || stat_changed(opened, after) || copied != static_cast<uint64_t>(opened.st_size)) {
        QFile::remove(output.fileName());
        add_issue(context, "directory_member_changed", "directory member changed while it was snapshotted", path);
        return false;
    }
    context.total_bytes = projected_total;
    remember_snapshot_file(context, path, copied, hash);
    return true;
}

auto snapshot_directory_at(LoadContext& context, int directory_fd, const QString& relative_prefix) -> bool {
    const int ITERATOR_FD = ::dup(directory_fd);
    if (ITERATOR_FD < 0) {
        add_issue(context, "directory_read_failed", "could not duplicate directory descriptor", relative_prefix);
        return false;
    }
    DIR* directory = ::fdopendir(ITERATOR_FD);
    if (directory == nullptr) {
        ::close(ITERATOR_FD);
        add_issue(context, "directory_read_failed", "could not enumerate directory", relative_prefix);
        return false;
    }
    std::vector<QByteArray> names;
    errno = 0;
    while (dirent* entry = ::readdir(directory)) {
        QByteArray name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        names.push_back(std::move(name));
    }
    const int READ_ERRNO = errno;
    ::closedir(directory);
    if (READ_ERRNO != 0) {
        add_issue(context, "directory_read_failed",
                  QString("directory enumeration failed: %1").arg(QString::fromLocal8Bit(std::strerror(READ_ERRNO))), relative_prefix);
        return false;
    }
    std::ranges::sort(names);

    for (const auto& name : names) {
        const QString COMPONENT = QString::fromUtf8(name);
        const QString RAW_PATH = relative_prefix.isEmpty() ? COMPONENT : relative_prefix + '/' + COMPONENT;
        struct stat info{};
        if (::fstatat(directory_fd, name.constData(), &info, AT_SYMLINK_NOFOLLOW) != 0) {
            add_issue(context, "directory_member_stat_failed",
                      QString("fstatat failed: %1").arg(QString::fromLocal8Bit(std::strerror(errno))), RAW_PATH);
            return false;
        }
        const bool IS_DIRECTORY = S_ISDIR(info.st_mode);
        QString path;
        QString reason;
        if (!normalize_member_path(RAW_PATH, IS_DIRECTORY, context.limits, &path, &reason)) {
            add_issue(context, "unsafe_member_path", reason, RAW_PATH);
            return false;
        }
        if (!register_entry(context, path, IS_DIRECTORY)) {
            return false;
        }
        if (S_ISLNK(info.st_mode)) {
            add_issue(context, "symlink_member_rejected", "symbolic links are not permitted", path);
            return false;
        }
        if (IS_DIRECTORY) {
            ScopedFd child(::openat(directory_fd, name.constData(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
            if (!child.valid()) {
                add_issue(context, "directory_member_open_failed",
                          QString("could not open child directory: %1").arg(QString::fromLocal8Bit(std::strerror(errno))), path);
                return false;
            }
            if (!QDir().mkpath(QDir(context.bundle.root_path).absoluteFilePath(path)) ||
                !snapshot_directory_at(context, child.get(), path)) {
                return false;
            }
        } else if (S_ISREG(info.st_mode)) {
            if (!copy_directory_file(context, directory_fd, name, path, info)) {
                return false;
            }
        } else {
            add_issue(context, "unsafe_member_type", "only regular files and directories are permitted", path, true,
                      QJsonObject{{"mode", QString::number(static_cast<qulonglong>(info.st_mode), 8)}});
            return false;
        }
    }
    return true;
}

auto snapshot_directory(LoadContext& context, const QString& source_path) -> bool {
    const QByteArray ENCODED = QFile::encodeName(source_path);
    ScopedFd root(::open(ENCODED.constData(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (!root.valid()) {
        add_issue(context, "directory_open_failed",
                  QString("could not securely open incident directory: %1").arg(QString::fromLocal8Bit(std::strerror(errno))));
        return false;
    }
    return snapshot_directory_at(context, root.get(), {});
}

#else

auto snapshot_directory(LoadContext& context, const QString&) -> bool {
    add_issue(context, "directory_snapshot_unsupported", "secure directory snapshots require openat/O_NOFOLLOW support on this host");
    return false;
}

#endif

class ArchiveHandle {
   public:
    ArchiveHandle() : value(archive_read_new()) {}
    ~ArchiveHandle() {
        if (value != nullptr) {
            archive_read_free(value);
        }
    }
    ArchiveHandle(const ArchiveHandle&) = delete;
    auto operator=(const ArchiveHandle&) -> ArchiveHandle& = delete;
    archive* value = nullptr;
};

auto archive_error_text(archive* reader) -> QString {
    const char* text = archive_error_string(reader);
    return text == nullptr ? QString("unknown libarchive error") : QString::fromLocal8Bit(text);
}

auto supported_archive_format(archive* reader) -> bool {
    const int BASE = archive_format(reader) & ARCHIVE_FORMAT_BASE_MASK;
    return BASE == ARCHIVE_FORMAT_TAR || BASE == ARCHIVE_FORMAT_ZIP;
}

auto snapshot_archive(LoadContext& context, const QString& source_path) -> bool {
    ArchiveHandle reader;
    if (reader.value == nullptr) {
        add_issue(context, "archive_open_failed", "could not allocate libarchive reader");
        return false;
    }
    archive_read_support_filter_all(reader.value);
    archive_read_support_format_tar(reader.value);
    archive_read_support_format_zip(reader.value);

#ifdef Q_OS_UNIX
    const QByteArray ENCODED = QFile::encodeName(source_path);
    ScopedFd source(::open(ENCODED.constData(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (!source.valid()) {
        add_issue(context, "archive_open_failed",
                  QString("could not securely open archive: %1").arg(QString::fromLocal8Bit(std::strerror(errno))));
        return false;
    }
    struct stat source_info{};
    if (::fstat(source.get(), &source_info) != 0 || !S_ISREG(source_info.st_mode) || source_info.st_size < 0) {
        add_issue(context, "archive_source_invalid", "incident archive is not a readable regular file");
        return false;
    }
    if (static_cast<uint64_t>(source_info.st_size) > context.limits.max_archive_bytes) {
        add_issue(context, "archive_too_large", QString("archive exceeds %1-byte source limit").arg(context.limits.max_archive_bytes),
                  source_path);
        return false;
    }
    context.archive_bytes = static_cast<uint64_t>(source_info.st_size);
    if (archive_read_open_fd(reader.value, source.get(), COPY_BUFFER_SIZE) != ARCHIVE_OK) {
#else
    const QFileInfo SOURCE_INFO(source_path);
    if (SOURCE_INFO.size() < 0 || static_cast<uint64_t>(SOURCE_INFO.size()) > context.limits.max_archive_bytes) {
        add_issue(context, "archive_too_large", QString("archive exceeds %1-byte source limit").arg(context.limits.max_archive_bytes),
                  source_path);
        return false;
    }
    context.archive_bytes = static_cast<uint64_t>(SOURCE_INFO.size());
    if (archive_read_open_filename(reader.value, QFile::encodeName(source_path).constData(), COPY_BUFFER_SIZE) != ARCHIVE_OK) {
#endif
        add_issue(context, "archive_open_failed", archive_error_text(reader.value));
        return false;
    }

    bool format_checked = false;
    while (true) {
        archive_entry* entry = nullptr;
        const int STATUS = archive_read_next_header(reader.value, &entry);
        if (STATUS == ARCHIVE_EOF) {
            break;
        }
        if (STATUS != ARCHIVE_OK) {
            add_issue(context, "archive_read_failed", archive_error_text(reader.value));
            return false;
        }
        if (!format_checked) {
            format_checked = true;
            if (!supported_archive_format(reader.value)) {
                add_issue(context, "archive_format_unsupported", "only tar and zip incident archives are supported");
                return false;
            }
        }

        const char* PATH_TEXT = archive_entry_pathname_utf8(entry);
        if (PATH_TEXT == nullptr) {
            PATH_TEXT = archive_entry_pathname(entry);
        }
        const QString RAW_PATH = PATH_TEXT == nullptr ? QString() : QString::fromUtf8(PATH_TEXT);
        const mode_t FILE_TYPE = archive_entry_filetype(entry);
        const bool IS_DIRECTORY = FILE_TYPE == AE_IFDIR;
        QString path;
        QString reason;
        if (!normalize_member_path(RAW_PATH, IS_DIRECTORY, context.limits, &path, &reason)) {
            add_issue(context, "unsafe_member_path", reason, RAW_PATH);
            return false;
        }
        if (!register_entry(context, path, IS_DIRECTORY)) {
            return false;
        }
        if (archive_entry_symlink(entry) != nullptr) {
            add_issue(context, "symlink_member_rejected", "archive symbolic links are not permitted", path);
            return false;
        }
        if (archive_entry_hardlink(entry) != nullptr) {
            add_issue(context, "hardlink_member_rejected", "archive hard links are not permitted", path);
            return false;
        }
        if (archive_entry_sparse_count(entry) > 0) {
            add_issue(context, "sparse_member_rejected", "archive sparse entries are not permitted", path);
            return false;
        }
        if (archive_entry_is_encrypted(entry) != 0) {
            add_issue(context, "encrypted_member_rejected", "encrypted archive members are not permitted", path);
            return false;
        }
        if (!IS_DIRECTORY && FILE_TYPE != AE_IFREG) {
            add_issue(context, "unsafe_member_type", "only regular files and directories are permitted", path, true,
                      QJsonObject{{"fileType", QString::number(static_cast<unsigned int>(FILE_TYPE), 8)}});
            return false;
        }
        if (IS_DIRECTORY) {
            if (archive_entry_size(entry) != 0) {
                add_issue(context, "unsafe_directory_member", "archive directory entries must not carry a data payload", path);
                return false;
            }
            if (!QDir().mkpath(QDir(context.bundle.root_path).absoluteFilePath(path))) {
                add_issue(context, "snapshot_write_failed", "could not create snapshot directory", path);
                return false;
            }
            archive_read_data_skip(reader.value);
            continue;
        }

        const la_int64_t DECLARED_SIZE = archive_entry_size(entry);
        if (DECLARED_SIZE >= 0 && static_cast<uint64_t>(DECLARED_SIZE) > context.limits.max_member_bytes) {
            add_issue(context, "member_too_large",
                      QString("archive member declares %1 bytes; limit is %2").arg(DECLARED_SIZE).arg(context.limits.max_member_bytes),
                      path);
            return false;
        }
        QFile output;
        if (!prepare_output_file(context, path, &output)) {
            return false;
        }
        QCryptographicHash hash(QCryptographicHash::Sha256);
        uint64_t copied = 0;
        char buffer[COPY_BUFFER_SIZE];
        while (true) {
            const la_ssize_t READ = archive_read_data(reader.value, buffer, sizeof(buffer));
            if (READ == 0) {
                break;
            }
            if (READ < 0) {
                output.close();
                QFile::remove(output.fileName());
                add_issue(context, "archive_read_failed", archive_error_text(reader.value), path);
                return false;
            }
            const auto CHUNK = static_cast<uint64_t>(READ);
            if (!account_bytes(context, path, copied, CHUNK)) {
                output.close();
                QFile::remove(output.fileName());
                return false;
            }
            if (output.write(buffer, READ) != READ) {
                output.close();
                QFile::remove(output.fileName());
                add_issue(context, "snapshot_write_failed", QString("snapshot write failed: %1").arg(output.errorString()), path);
                return false;
            }
            hash.addData(QByteArrayView(buffer, READ));
            copied += CHUNK;
        }
        if (!output.flush()) {
            const QString ERROR = output.errorString();
            output.close();
            QFile::remove(output.fileName());
            add_issue(context, "snapshot_write_failed", QString("snapshot flush failed: %1").arg(ERROR), path);
            return false;
        }
        output.close();
        if (DECLARED_SIZE >= 0 && copied != static_cast<uint64_t>(DECLARED_SIZE)) {
            QFile::remove(output.fileName());
            add_issue(context, "archive_member_truncated",
                      QString("archive declared %1 bytes but yielded %2").arg(DECLARED_SIZE).arg(copied), path);
            return false;
        }
        remember_snapshot_file(context, path, copied, hash);
    }
    if (!format_checked) {
        add_issue(context, "archive_empty", "incident archive contains no entries");
        return false;
    }
#ifdef Q_OS_UNIX
    struct stat source_after{};
    if (::fstat(source.get(), &source_after) != 0 || stat_changed(source_info, source_after)) {
        add_issue(context, "archive_source_changed", "incident archive changed while it was snapshotted", source_path);
        return false;
    }
#endif
    return true;
}

auto json_uint64(const QJsonValue& value, uint64_t* output) -> bool {
    if (!value.isDouble()) {
        return false;
    }
    const double NUMBER = value.toDouble();
    // QJsonDocument rejects NaN and infinities; retain only the integer and
    // JSON-safe-range checks here so Release's -ffast-math remains warning-free.
    if (NUMBER < 0 || std::floor(NUMBER) != NUMBER || NUMBER > 9007199254740991.0) {
        return false;
    }
    *output = static_cast<uint64_t>(NUMBER);
    return true;
}

auto valid_sha256(const QString& value) -> bool {
    static const QRegularExpression SHA256_RE("^[0-9a-f]{64}$");
    return SHA256_RE.match(value).hasMatch();
}

auto canonical_json_value(const QJsonValue& value) -> QJsonValue {
    if (value.isArray()) {
        QJsonArray result;
        const QJsonArray input = value.toArray();
        for (const auto& child : input) {
            result.append(canonical_json_value(child));
        }
        return result;
    }
    if (value.isObject()) {
        const QJsonObject input = value.toObject();
        QStringList keys = input.keys();
        std::ranges::sort(keys);
        QJsonObject result;
        for (const auto& key : keys) {
            result.insert(key, canonical_json_value(input[key]));
        }
        return result;
    }
    return value;
}

auto expected_incident_id(QJsonObject manifest) -> QString {
    manifest.remove("incidentId");
    manifest.remove("createdUtc");
    const QJsonObject CANONICAL = canonical_json_value(manifest).toObject();
    const QByteArray PAYLOAD = QJsonDocument(CANONICAL).toJson(QJsonDocument::Compact);
    return "sha256:" + QString::fromLatin1(QCryptographicHash::hash(PAYLOAD, QCryptographicHash::Sha256).toHex());
}

auto member_node_id(const QJsonObject& object, QString* node_id) -> bool {
    QJsonValue value = object["nodeId"];
    if (value.isUndefined()) {
        value = object["node"];
    }
    if (value.isUndefined() || value.isNull()) {
        node_id->clear();
        return true;
    }
    if (value.isString()) {
        *node_id = value.toString();
        return !node_id->isEmpty();
    }
    uint64_t numeric = 0;
    if (json_uint64(value, &numeric)) {
        *node_id = QString::number(numeric);
        return true;
    }
    return false;
}

void validate_manifest(LoadContext& context) {
    const auto MANIFEST_IT = context.files.constFind("manifest.json");
    if (MANIFEST_IT == context.files.cend()) {
        add_issue(context, "manifest_missing", "bundle root does not contain manifest.json", "manifest.json");
        return;
    }
    if (MANIFEST_IT->size > context.limits.max_manifest_bytes) {
        add_issue(context, "manifest_too_large",
                  QString("manifest is %1 bytes; limit is %2").arg(MANIFEST_IT->size).arg(context.limits.max_manifest_bytes),
                  "manifest.json");
        return;
    }

    QFile file(MANIFEST_IT->absolute_path);
    if (!file.open(QIODevice::ReadOnly)) {
        add_issue(context, "manifest_read_failed", QString("could not read manifest: %1").arg(file.errorString()), "manifest.json");
        return;
    }
    const QByteArray DATA = file.read(static_cast<qint64>(context.limits.max_manifest_bytes + 1));
    if (static_cast<uint64_t>(DATA.size()) != MANIFEST_IT->size) {
        add_issue(context, "manifest_read_failed", "manifest snapshot size changed unexpectedly", "manifest.json");
        return;
    }
    QJsonParseError error;
    const QJsonDocument DOCUMENT = QJsonDocument::fromJson(DATA, &error);
    if (error.error != QJsonParseError::NoError) {
        add_issue(context, "manifest_invalid_json", error.errorString(), "manifest.json", true,
                  QJsonObject{{"offset", QString::number(error.offset)}});
        return;
    }
    if (!DOCUMENT.isObject()) {
        add_issue(context, "manifest_not_object", "manifest JSON root must be an object", "manifest.json");
        return;
    }
    context.bundle.manifest = DOCUMENT.object();
    const QString FORMAT = context.bundle.manifest["format"].toString();
    if (FORMAT != "wosincident") {
        add_issue(context, "manifest_format_unsupported", "manifest format must be 'wosincident'", "manifest.json", true,
                  QJsonObject{{"format", FORMAT}});
    }
    uint64_t version = 0;
    if (!json_uint64(context.bundle.manifest["version"], &version)) {
        add_issue(context, "manifest_version_invalid", "manifest version must be a nonnegative integer", "manifest.json");
    } else if (version != 1) {
        add_issue(context, "manifest_version_unsupported", "only wosincident manifest version 1 is supported", "manifest.json", true,
                  QJsonObject{{"version", QString::number(version)}});
    }
    const QString INCIDENT_ID = context.bundle.manifest["incidentId"].toString();
    if (INCIDENT_ID.isEmpty()) {
        add_issue(context, "incident_id_missing", "manifest incidentId must be a non-empty string", "manifest.json");
    } else {
        const QString EXPECTED_ID = expected_incident_id(context.bundle.manifest);
        if (INCIDENT_ID != EXPECTED_ID) {
            add_issue(context, "incident_id_mismatch", "manifest incidentId does not match its canonical evidence identity",
                      "manifest.json", true, QJsonObject{{"expected", EXPECTED_ID}, {"actual", INCIDENT_ID}});
        }
    }

    if (!context.bundle.manifest["members"].isArray()) {
        add_issue(context, "manifest_members_invalid", "manifest members must be an array", "manifest.json");
        return;
    }
    const QJsonArray MEMBER_VALUES = context.bundle.manifest["members"].toArray();
    if (static_cast<size_t>(MEMBER_VALUES.size()) > context.limits.max_members) {
        add_issue(context, "manifest_member_count_exceeded",
                  QString("manifest declares more than %1 members").arg(context.limits.max_members), "manifest.json");
        return;
    }

    QSet<QString> declared_paths;
    QSet<QString> declared_ids;
    QStringList original_order;
    for (qsizetype index = 0; index < MEMBER_VALUES.size(); ++index) {
        const QString LOCATION = QString("members[%1]").arg(index);
        if (!MEMBER_VALUES[index].isObject()) {
            add_issue(context, "manifest_member_invalid", "manifest member must be an object", LOCATION);
            continue;
        }
        const QJsonObject OBJECT = MEMBER_VALUES[index].toObject();
        QString path;
        QString reason;
        if (!OBJECT["path"].isString() || !normalize_member_path(OBJECT["path"].toString(), false, context.limits, &path, &reason)) {
            add_issue(context, "manifest_member_path_invalid", reason.isEmpty() ? "member path must be a string" : reason, LOCATION);
            continue;
        }
        if (path == "manifest.json") {
            add_issue(context, "manifest_member_reserved_path", "manifest.json cannot declare itself as an evidence member", path);
            continue;
        }
        if (declared_paths.contains(path)) {
            add_issue(context, "manifest_member_duplicate", "manifest declares the same member path more than once", path);
            continue;
        }
        declared_paths.insert(path);
        original_order << path;

        IncidentMember member;
        member.path = path;
        member.id = OBJECT["id"].toString(QString("artifact:%1").arg(path));
        member.kind = OBJECT["kind"].toString();
        member.sha256 = OBJECT["sha256"].toString();
        if (!member_node_id(OBJECT, &member.node_id)) {
            add_issue(context, "manifest_member_node_id_invalid", "member nodeId must be a non-empty string or JSON-safe integer", path);
        }
        member.build_id = OBJECT["buildId"].toString();
        member.binary = OBJECT["binary"].toString();
        member.source_name = OBJECT["sourceName"].toString();
        member.state = OBJECT["state"].toString("complete");
        member.metadata = OBJECT;
        if (member.id.isEmpty() || declared_ids.contains(member.id)) {
            add_issue(context, "manifest_member_id_invalid", "member id must be non-empty and unique", path);
        } else {
            declared_ids.insert(member.id);
        }
        if (member.kind.isEmpty()) {
            add_issue(context, "manifest_member_kind_invalid", "member kind must be a non-empty string", path);
        }
        if (!json_uint64(OBJECT["size"], &member.size)) {
            add_issue(context, "manifest_member_size_invalid", "member size must be a nonnegative JSON-safe integer", path);
        } else if (member.size > context.limits.max_member_bytes) {
            add_issue(context, "member_too_large", "manifest member size exceeds configured limit", path);
        }
        if (!OBJECT["required"].isBool()) {
            add_issue(context, "manifest_member_required_invalid", "member required must be a boolean", path);
        } else {
            member.required = OBJECT["required"].toBool();
        }
        if (!OBJECT["truncated"].isBool()) {
            add_issue(context, "manifest_member_truncated_invalid", "member truncated must be a boolean", path);
        } else {
            member.truncated = OBJECT["truncated"].toBool();
        }
        const bool INTENTIONALLY_MISSING = member.state == "missing" || member.state == "error";
        if (!INTENTIONALLY_MISSING && !valid_sha256(member.sha256)) {
            add_issue(context, "manifest_member_sha256_invalid", "materialized member sha256 must be 64 lowercase hex digits", path);
        } else if (INTENTIONALLY_MISSING && !member.sha256.isEmpty()) {
            add_issue(context, "manifest_member_sha256_invalid", "missing/error member sha256 must be null or empty", path);
        }
        if (!member.binary.isEmpty()) {
            QString binary;
            if (!normalize_member_path(member.binary, false, context.limits, &binary, &reason)) {
                add_issue(context, "manifest_binary_path_invalid", reason, path);
            } else {
                member.binary = binary;
            }
        }

        const auto SNAPSHOT = context.files.constFind(path);
        if (SNAPSHOT == context.files.cend()) {
            add_issue(context, member.required ? "required_member_missing" : "optional_member_missing",
                      member.required ? "required manifest member is absent" : "optional manifest member is absent", path, member.required);
        } else if (INTENTIONALLY_MISSING) {
            add_issue(context, "member_state_conflict", "member is materialized despite missing/error state", path);
        } else {
            member.absolute_path = SNAPSHOT->absolute_path;
            if (member.size != SNAPSHOT->size) {
                add_issue(context, "member_size_mismatch", "snapshot size does not match manifest", path, true,
                          QJsonObject{{"manifest", QString::number(member.size)}, {"actual", QString::number(SNAPSHOT->size)}});
            }
            if (valid_sha256(member.sha256) && member.sha256 != SNAPSHOT->sha256) {
                add_issue(context, "member_checksum_mismatch", "snapshot SHA-256 does not match manifest", path, true,
                          QJsonObject{{"manifest", member.sha256}, {"actual", SNAPSHOT->sha256}});
            }
        }
        context.bundle.members.push_back(std::move(member));
    }

    QStringList sorted_order = original_order;
    std::ranges::sort(sorted_order);
    if (original_order != sorted_order) {
        add_issue(context, "manifest_members_unsorted", "manifest member array is not lexicographically sorted by path", "manifest.json",
                  false);
    }
    std::ranges::sort(context.bundle.members,
                      [](const IncidentMember& left, const IncidentMember& right) { return left.path < right.path; });

    QStringList actual_paths = context.files.keys();
    std::ranges::sort(actual_paths);
    for (const auto& actual : actual_paths) {
        if (actual != "manifest.json" && !declared_paths.contains(actual)) {
            add_issue(context, "undeclared_member", "snapshot contains a regular file absent from manifest members", actual);
        }
    }
    for (const auto& member : context.bundle.members) {
        if (!member.binary.isEmpty()) {
            const auto* binary = context.bundle.find_member(member.binary);
            if (binary == nullptr || binary->absolute_path.isEmpty()) {
                add_issue(context, "binary_member_missing", "referenced matching binary is not materialized", member.path, false,
                          QJsonObject{{"binary", member.binary}});
            }
        }
    }
}

}  // namespace

bool IncidentBundle::valid() const {
    return !manifest.isEmpty() && !std::ranges::any_of(issues, [](const IncidentIssue& issue) { return issue.fatal; });
}

QString IncidentBundle::incident_id() const { return manifest["incidentId"].toString(); }

const IncidentMember* IncidentBundle::find_member(const QString& path) const {
    const auto it = std::ranges::lower_bound(members, path, {}, &IncidentMember::path);
    return it != members.end() && it->path == path ? &*it : nullptr;
}

std::unique_ptr<IncidentBundle> load_incident_bundle(const QString& source_path, const IncidentLimits& limits) {
    auto bundle = std::make_unique<IncidentBundle>();
    const QFileInfo INPUT_SOURCE(source_path);
    if (INPUT_SOURCE.isSymLink()) {
        bundle->source_path = INPUT_SOURCE.absoluteFilePath();
        bundle->issues.push_back(IncidentIssue{.code = "source_symlink_rejected",
                                               .message = "top-level incident source may not be a symbolic link",
                                               .path = source_path,
                                               .fatal = true,
                                               .details = {}});
        return bundle;
    }
    bundle->source_path = QFileInfo(source_path).canonicalFilePath();
    if (bundle->source_path.isEmpty()) {
        bundle->source_path = QFileInfo(source_path).absoluteFilePath();
    }
    bundle->storage = std::make_shared<QTemporaryDir>(QDir::temp().absoluteFilePath("wosdbg-incident-XXXXXX"));
    if (!bundle->storage->isValid()) {
        bundle->issues.push_back(IncidentIssue{.code = "snapshot_create_failed",
                                               .message = "could not create private incident snapshot directory",
                                               .path = source_path,
                                               .fatal = true,
                                               .details = {}});
        return bundle;
    }
    bundle->root_path = bundle->storage->path();
    LoadContext context{.bundle = *bundle,
                        .limits = limits,
                        .files = {},
                        .seen_paths = {},
                        .file_paths = {},
                        .entry_count = 0,
                        .total_bytes = 0,
                        .archive_bytes = 0};

    const QFileInfo SOURCE(bundle->source_path);
    if (!SOURCE.exists()) {
        add_issue(context, "source_not_found", "incident source does not exist", source_path);
        return bundle;
    }
    bool snapshotted = false;
    if (SOURCE.isDir()) {
        bundle->archive = false;
        snapshotted = snapshot_directory(context, bundle->source_path);
    } else if (SOURCE.isFile()) {
        bundle->archive = true;
        snapshotted = snapshot_archive(context, bundle->source_path);
    } else {
        add_issue(context, "source_type_rejected", "incident source must be a regular archive file or directory", source_path);
        return bundle;
    }

    if (snapshotted && !has_fatal_issue(*bundle)) {
        validate_manifest(context);
    }
    return bundle;
}

QJsonObject incident_issue_to_json(const IncidentIssue& issue) {
    QJsonObject object{{"code", issue.code}, {"message", issue.message}, {"path", issue.path}, {"fatal", issue.fatal}};
    if (!issue.details.isEmpty()) {
        object["details"] = issue.details;
    }
    return object;
}

QJsonObject incident_member_to_json(const IncidentMember& member, bool include_absolute_path) {
    QJsonObject object{{"id", member.id},
                       {"path", member.path},
                       {"kind", member.kind},
                       {"size", QString::number(member.size)},
                       {"sha256", member.sha256},
                       {"required", member.required},
                       {"truncated", member.truncated},
                       {"nodeId", member.node_id},
                       {"buildId", member.build_id},
                       {"binary", member.binary},
                       {"sourceName", member.source_name},
                       {"state", member.state},
                       {"available", !member.absolute_path.isEmpty()}};
    if (include_absolute_path) {
        object["absolutePath"] = member.absolute_path;
    }
    return object;
}

QJsonArray incident_issues_to_json(const IncidentBundle& bundle) {
    QJsonArray issues;
    for (const auto& issue : bundle.issues) {
        issues.append(incident_issue_to_json(issue));
    }
    return issues;
}

QJsonObject incident_inventory_to_json(const IncidentBundle& bundle, size_t start, size_t count, bool include_absolute_paths) {
    const size_t BEGIN = std::min(start, bundle.members.size());
    const size_t SAFE_COUNT = std::min<size_t>(count, 10000);
    const size_t END = std::min(bundle.members.size(), BEGIN + std::min(SAFE_COUNT, bundle.members.size() - BEGIN));
    QJsonArray members;
    for (size_t index = BEGIN; index < END; ++index) {
        members.append(incident_member_to_json(bundle.members[index], include_absolute_paths));
    }
    return QJsonObject{{"ok", bundle.valid()},
                       {"incidentId", bundle.incident_id()},
                       {"sourcePath", bundle.source_path},
                       {"archive", bundle.archive},
                       {"start", QString::number(BEGIN)},
                       {"count", QString::number(members.size())},
                       {"total", QString::number(bundle.members.size())},
                       {"members", members},
                       {"issues", incident_issues_to_json(bundle)}};
}

}  // namespace wosdbg
