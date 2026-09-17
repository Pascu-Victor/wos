#include "service_state.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>

namespace wos::init {
namespace {
class File {
   public:
    File() = default;
    explicit File(int value) : value_(value) {}
    ~File() {
        if (value_ >= 0) ::close(value_);
    }
    File(const File&) = delete;
    auto operator=(const File&) -> File& = delete;
    auto get() const -> int { return value_; }
    void reset(int value) {
        if (value_ >= 0) ::close(value_);
        value_ = value;
    }

   private:
    int value_{-1};
};
constexpr int DIRECTORY_FLAGS = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
auto metadata(int fd, bool directory, PrivateStateInode& out) -> int {
    struct stat state{};
    if (::fstat(fd, &state) != 0) return errno;
    out = {fd, state.st_uid, state.st_gid, state.st_mode, static_cast<uint64_t>(state.st_nlink), directory};
    return 0;
}
}  // namespace

static auto prepare_flat_private_state_at(int directory, uint32_t uid, uint32_t gid, const std::array<const char*, 2>& names,
                                          bool allow_legacy_directory) -> int {
    std::array<PrivateStateInode, 3> entries{};
    PrivateStateInode parent{};
    int error = metadata(directory, true, parent);
    if (error != 0) return error;
    // Old root-owned key directories were 0755. Validate the entire retained
    // namespace before tightening that mode or transferring any ownership.
    bool const LEGACY_DIRECTORY = allow_legacy_directory && parent.uid == 0 && parent.gid == 0 && parent.mode == 0040755;
    if (LEGACY_DIRECTORY) parent.mode = 0040700;
    // Validate the directory before enumerating its private contents.
    error = validate_private_state_owners(std::span(&parent, 1), uid, gid);
    if (error != 0) return error;
    int const DUPLICATE = ::dup(directory);
    if (DUPLICATE < 0) return errno;
    DIR* stream = ::fdopendir(DUPLICATE);
    if (stream == nullptr) {
        error = errno;
        ::close(DUPLICATE);
        return error;
    }
    std::array<File, 2> files{};
    std::array<bool, 2> seen{};
    size_t count = 0, scanned = 0;
    for (;;) {
        errno = 0;
        auto* item = ::readdir(stream);
        if (item == nullptr) {
            error = errno;
            break;
        }
        if (++scanned > 4) {
            error = EPERM;
            break;
        }
        if (std::strcmp(item->d_name, ".") == 0 || std::strcmp(item->d_name, "..") == 0) continue;
        size_t index = 2;
        if (std::strcmp(item->d_name, names[0]) == 0) index = 0;
        if (std::strcmp(item->d_name, names[1]) == 0) index = 1;
        if (index == 2 || seen[index]) {
            error = EPERM;
            break;
        }
        seen[index] = true;
        struct stat staged{};
        if (::fstatat(directory, item->d_name, &staged, AT_SYMLINK_NOFOLLOW) != 0) {
            error = errno;
            break;
        }
        std::array<PrivateStateInode, 2> preflight{
            {{0, staged.st_uid, staged.st_gid, staged.st_mode, static_cast<uint64_t>(staged.st_nlink), false}, parent}};
        error = validate_private_state_owners(preflight, uid, gid);
        if (error != 0) break;
        files[index].reset(::openat(directory, item->d_name, O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
        if (files[index].get() < 0) {
            error = errno;
            break;
        }
        error = metadata(files[index].get(), false, entries[count]);
        if (error != 0) break;
        ++count;
    }
    if (::closedir(stream) != 0 && error == 0) error = errno;
    if (error != 0) return error;
    entries[count++] = parent;
    if (LEGACY_DIRECTORY && ::fchmod(directory, 0700) != 0) return errno;
    error = change_private_state_owners(std::span(entries.data(), count), uid, gid,
                                        [](int fd, uint32_t owner, uint32_t group) { return ::fchown(fd, owner, group) == 0 ? 0 : errno; });
    if (error != 0 && LEGACY_DIRECTORY && ::fchmod(directory, 0755) != 0) return EIO;
    return error;
}

auto prepare_journal_state_at(int directory, uint32_t uid, uint32_t gid) -> int {
    return prepare_flat_private_state_at(directory, uid, gid, {"wos.journal", "wos.journal.1"}, false);
}

auto prepare_keygen_state_at(int directory, uint32_t uid, uint32_t gid) -> int {
    return prepare_flat_private_state_at(directory, uid, gid, {"dropbear_rsa_host_key", "dropbear_rsa_host_key.pub"}, true);
}

auto prepare_private_keygen_state(uint32_t uid, uint32_t gid) -> int {
    File parent(::open("/", DIRECTORY_FLAGS));
    if (parent.get() < 0) return errno;
    for (const char* name : {"etc", "dropbear"}) {
        struct stat state{};
        if (::fstat(parent.get(), &state) != 0) return errno;
        if (!S_ISDIR(state.st_mode) || state.st_uid != 0 || state.st_gid != 0 || (state.st_mode & 0022) != 0) return EPERM;
        int const CHILD = ::openat(parent.get(), name, DIRECTORY_FLAGS);
        if (CHILD < 0) return errno;
        parent.reset(CHILD);
    }
    return prepare_keygen_state_at(parent.get(), uid, gid);
}

auto prepare_netd_state_at(int directory, uint32_t uid, uint32_t gid, std::span<const char> seed) -> int {
    if (uid == 0 || uid == UINT32_MAX || gid == 0 || gid == UINT32_MAX || seed.empty() || seed.size() > 4096) return EINVAL;
    struct stat existing{};
    if (::fstatat(directory, "netd", &existing, AT_SYMLINK_NOFOLLOW) == 0) return EEXIST;
    if (errno != ENOENT) return errno;
    constexpr const char* STAGING = ".netd-init";
    if (::mkdirat(directory, STAGING, 0700) != 0) return errno;
    File staged(::openat(directory, STAGING, DIRECTORY_FLAGS));
    int error = staged.get() < 0 ? errno : 0;
    bool file_created = false;
    if (error == 0) {
        File file(::openat(staged.get(), "resolv.conf", O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600));
        if (file.get() < 0)
            error = errno;
        else {
            file_created = true;
            size_t written = 0;
            while (written < seed.size()) {
                ssize_t const COUNT = ::write(file.get(), seed.data() + written, seed.size() - written);
                if (COUNT < 0 && errno == EINTR) continue;
                if (COUNT <= 0) {
                    error = COUNT < 0 ? errno : EIO;
                    break;
                }
                written += static_cast<size_t>(COUNT);
            }
            if (error == 0 && ::fchown(file.get(), uid, gid) != 0) error = errno;
            if (error == 0 && ::fchmod(file.get(), 0644) != 0) error = errno;
        }
        if (error == 0 && ::fchmod(staged.get(), 0755) != 0) error = errno;
        if (error == 0 && ::fchown(staged.get(), uid, gid) != 0) error = errno;
        if (error == 0 && ::renameat(directory, STAGING, directory, "netd") != 0) error = errno;
    }
    if (error != 0) {
        bool cleaned = !file_created || ::unlinkat(staged.get(), "resolv.conf", 0) == 0;
        if (::unlinkat(directory, STAGING, AT_REMOVEDIR) != 0) cleaned = false;
        if (!cleaned) return EIO;
    }
    return error;
}

auto prepare_netd_runtime_state(uint32_t uid, uint32_t gid) -> int {
    auto trusted_directory = [](int fd) {
        struct stat state{};
        return ::fstat(fd, &state) == 0 && S_ISDIR(state.st_mode) && state.st_uid == 0 && state.st_gid == 0 && (state.st_mode & 0022) == 0;
    };
    File root(::open("/", DIRECTORY_FLAGS));
    if (root.get() < 0) return errno;
    if (!trusted_directory(root.get())) return EPERM;
    File run(::openat(root.get(), "run", DIRECTORY_FLAGS));
    if (run.get() < 0) return errno;
    File etc(::openat(root.get(), "etc", DIRECTORY_FLAGS));
    if (etc.get() < 0) return errno;
    if (!trusted_directory(run.get()) || !trusted_directory(etc.get())) return EPERM;
    File seed(::openat(etc.get(), "netd-resolv.conf", O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
    if (seed.get() < 0) return errno;
    struct stat state{};
    if (::fstat(seed.get(), &state) != 0) return errno;
    if (state.st_uid != 0 || state.st_gid != 0 || state.st_mode != (S_IFREG | 0644) || state.st_nlink != 1) return EPERM;
    std::array<char, 4097> bytes{};
    size_t count = 0;
    while (count < bytes.size()) {
        ssize_t const READ = ::read(seed.get(), bytes.data() + count, bytes.size() - count);
        if (READ < 0 && errno == EINTR) continue;
        if (READ < 0) return errno;
        if (READ == 0) break;
        count += static_cast<size_t>(READ);
    }
    return prepare_netd_state_at(run.get(), uid, gid, std::span(bytes.data(), count));
}

auto prepare_private_journal_state(uint32_t uid, uint32_t gid) -> int {
    File parent(::open("/", DIRECTORY_FLAGS));
    if (parent.get() < 0) return errno;
    for (const char* name : {"var", "log", "journal"}) {
        struct stat state{};
        if (::fstat(parent.get(), &state) != 0) return errno;
        if (!S_ISDIR(state.st_mode) || state.st_uid != 0 || (state.st_mode & 0022) != 0) return EPERM;
        int const CHILD = ::openat(parent.get(), name, DIRECTORY_FLAGS);
        if (CHILD < 0) return errno;
        parent.reset(CHILD);
    }
    return prepare_journal_state_at(parent.get(), uid, gid);
}
}  // namespace wos::init
