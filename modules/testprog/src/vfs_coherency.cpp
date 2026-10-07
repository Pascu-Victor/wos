#include "vfs_coherency.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vfs/flock_abi.hpp>

namespace {
static_assert(sizeof(struct flock) == sizeof(ker::vfs::VfsFlockAbi));
static_assert(offsetof(struct flock, l_type) == offsetof(ker::vfs::VfsFlockAbi, l_type));
static_assert(offsetof(struct flock, l_whence) == offsetof(ker::vfs::VfsFlockAbi, l_whence));
static_assert(offsetof(struct flock, l_start) == offsetof(ker::vfs::VfsFlockAbi, l_start));
static_assert(offsetof(struct flock, l_len) == offsetof(ker::vfs::VfsFlockAbi, l_len));
static_assert(offsetof(struct flock, l_pid) == offsetof(ker::vfs::VfsFlockAbi, l_pid));
#ifdef __WOS__
static_assert(std::is_same_v<decltype(flock::l_pid), int64_t>);
#endif
constexpr unsigned PARTICIPANTS = 3;
constexpr unsigned RECORDS = 64;
constexpr uint64_t RECORD_MAGIC = 0x574F535646533236;
volatile sig_atomic_t interrupted = 0;
void interrupt_wait(int) { interrupted = 1; }

class Descriptor {
   public:
    explicit Descriptor(int fd = -1) : fd_(fd) {}
    Descriptor(const Descriptor&) = delete;
    auto operator=(const Descriptor&) -> Descriptor& = delete;
    ~Descriptor() {
        if (fd_ >= 0) static_cast<void>(close(fd_));
    }
    auto get() const -> int { return fd_; }
    auto finish() -> bool { return fd_ >= 0 && close(std::exchange(fd_, -1)) == 0; }

   private:
    int fd_;
};

struct Record {
    uint64_t magic;
    uint64_t run;
    uint32_t role;
    uint32_t sequence;
    std::array<uint8_t, 56> payload;
};
static_assert(sizeof(Record) == 80);

auto make_record(uint64_t run, unsigned role, unsigned sequence) -> Record {
    Record result{RECORD_MAGIC, run, role, sequence, {}};
    for (size_t i = 0; i < result.payload.size(); ++i) result.payload.at(i) = static_cast<uint8_t>((role * 73 + sequence * 19 + i) & 255);
    return result;
}

auto seconds() -> int64_t {
    timespec value{};
    return clock_gettime(CLOCK_MONOTONIC, &value) == 0 ? static_cast<int64_t>(value.tv_sec) : -1;
}

class Campaign {
   public:
    Campaign(const char* root, unsigned role, uint64_t run, unsigned timeout)
        : root_(root), role_(role), run_(run), deadline_(seconds() + timeout) {}

    auto execute() -> bool {
        if (seconds() < 0) return failure("monotonic-clock");
        if (role_ == 0 && !initialize()) return false;
        if (!wait("initialized", 0) || !append() || !cache() || !locks() || !lifecycle() || !exec_lifecycle() || !wait_lifecycle() ||
            !mark("complete"))
            return false;
        if (role_ == 0 && !wait_all("complete")) return false;
        std::printf("VFS-COHERENCY-PASS role=%u run=%llu records=%u\n", role_, static_cast<unsigned long long>(run_), RECORDS);
        return true;
    }

   private:
    auto failure(const char* stage) const -> bool {
        std::printf("VFS-COHERENCY-FAIL role=%u run=%llu stage=%s errno=%d\n", role_, static_cast<unsigned long long>(run_), stage, errno);
        std::fflush(stdout);
        return false;
    }
    auto path(const char* leaf, std::array<char, 512>& output) const -> bool {
        int const LENGTH = std::snprintf(output.data(), output.size(), "%s/%s", root_, leaf);
        if (LENGTH < 0 || static_cast<size_t>(LENGTH) >= output.size()) {
            errno = ENAMETOOLONG;
            return false;
        }
        return true;
    }
    auto open_file(const char* leaf, int flags) const -> int {
        std::array<char, 512> name{};
        return path(leaf, name) ? open(name.data(), flags, 0600) : -1;
    }
    auto create(const char* leaf, std::string_view bytes = {}) -> bool {
        Descriptor file(open_file(leaf, O_CREAT | O_EXCL | O_RDWR));
        if (file.get() < 0 || (!bytes.empty() && write(file.get(), bytes.data(), bytes.size()) != static_cast<ssize_t>(bytes.size())) ||
            !file.finish())
            return failure(leaf);
        return true;
    }
    auto marker(const char* stage, unsigned role, std::array<char, 96>& name) const -> bool {
        int const LENGTH =
            std::snprintf(name.data(), name.size(), "barriers/%s-%u-%llu", stage, role, static_cast<unsigned long long>(run_));
        return LENGTH > 0 && static_cast<size_t>(LENGTH) < name.size();
    }
    auto mark(const char* stage) -> bool {
        std::array<char, 96> name{};
        if (!marker(stage, role_, name)) return failure("marker-length");
        Descriptor file(open_file(name.data(), O_CREAT | O_EXCL | O_WRONLY));
        if (file.get() < 0 || write(file.get(), &run_, sizeof(run_)) != sizeof(run_) || !file.finish()) return failure(stage);
        return true;
    }
    auto wait(const char* stage, unsigned role) -> bool {
        std::array<char, 96> name{};
        if (!marker(stage, role, name)) return failure("marker-length");
        for (;;) {
            Descriptor file(open_file(name.data(), O_RDONLY));
            if (file.get() >= 0) {
                uint64_t observed = 0;
                if (pread(file.get(), &observed, sizeof(observed), 0) == sizeof(observed) && observed == run_) return true;
            }
            int64_t const NOW = seconds();
            if (NOW < 0 || NOW >= deadline_) {
                errno = ETIMEDOUT;
                return failure(stage);
            }
            timespec const PAUSE{0, 20'000'000};
            static_cast<void>(nanosleep(&PAUSE, nullptr));
        }
    }
    auto wait_all(const char* stage) -> bool {
        for (unsigned role = 0; role < PARTICIPANTS; ++role)
            if (!wait(stage, role)) return false;
        return true;
    }
    auto initialize() -> bool {
        // Refuse an existing root: every campaign owns a fresh namespace.
        if (mkdir(root_, 0700) != 0) return failure("fresh-root");
        // Coordination changes a separate directory, so the complete root
        // enumeration below is stable after the intended namespace mutation.
        std::array<char, 512> barriers{};
        if (!path("barriers", barriers) || mkdir(barriers.data(), 0700) != 0) return failure("barrier-directory");
        for (const char* leaf : {"append", "posix", "ofd", "flock", "close"})
            if (!create(leaf)) return false;
        for (unsigned role = 0; role < PARTICIPANTS; ++role) {
            std::array<char, 32> leaf{};
            std::snprintf(leaf.data(), leaf.size(), "lifecycle-%u", role);
            if (!create(leaf.data())) return false;
        }
        return create("original", "seed") && create("removed", "seed") && create("directory-old") && mark("initialized");
    }
    auto append() -> bool {
        Descriptor writer(open_file("append", O_WRONLY | O_APPEND));
        if (writer.get() < 0 || !mark("append-ready") || !wait_all("append-ready")) return failure("append-open-barrier");
        for (unsigned sequence = 0; sequence < RECORDS; ++sequence) {
            Record const RECORD = make_record(run_, role_, sequence);
            // One syscall is one append record. A short result is a failure,
            // never a positioned tail repair that could conceal interleaving.
            if (write(writer.get(), &RECORD, sizeof(RECORD)) != sizeof(RECORD)) return failure("atomic-append");
        }
        if (!writer.finish() || !mark("append-done") || !wait_all("append-done")) return failure("append-commit");
        Descriptor reader(open_file("append", O_RDONLY));
        struct stat metadata{};
        if (reader.get() < 0 || fstat(reader.get(), &metadata) != 0 || metadata.st_size != PARTICIPANTS * RECORDS * sizeof(Record))
            return failure("append-exact-size");
        std::array<std::array<bool, RECORDS>, PARTICIPANTS> seen{};
        for (unsigned index = 0; index < PARTICIPANTS * RECORDS; ++index) {
            Record observed{};
            if (pread(reader.get(), &observed, sizeof(observed), static_cast<off_t>(index * sizeof(Record))) != sizeof(observed) ||
                observed.magic != RECORD_MAGIC || observed.run != run_ || observed.role >= PARTICIPANTS || observed.sequence >= RECORDS)
                return failure("append-record-framing");
            Record const EXPECTED = make_record(run_, observed.role, observed.sequence);
            if (observed.payload != EXPECTED.payload || seen.at(observed.role).at(observed.sequence))
                return failure("append-record-uniqueness");
            seen.at(observed.role).at(observed.sequence) = true;
        }
        for (auto const& role : seen)
            for (bool present : role)
                if (!present) return failure("append-missing-record");
        std::printf("VFS-COHERENCY-CASE role=%u case=append records=%u\n", role_, PARTICIPANTS * RECORDS);
        return mark("append-verified") && wait_all("append-verified");
    }
    auto bytes(int fd, std::string_view expected) -> bool {
        std::array<char, 32> observed{};
        return expected.size() < observed.size() &&
               pread(fd, observed.data(), observed.size(), 0) == static_cast<ssize_t>(expected.size()) &&
               std::memcmp(observed.data(), expected.data(), expected.size()) == 0;
    }
    auto directory(DIR* stream, bool changed) -> bool {
        if (stream == nullptr) return false;
        bool old = false, fresh = false, added = false;
        errno = 0;
        while (auto* entry = readdir(stream)) {
            old |= std::strcmp(entry->d_name, "directory-old") == 0;
            fresh |= std::strcmp(entry->d_name, "directory-new") == 0;
            added |= std::strcmp(entry->d_name, "directory-added") == 0;
        }
        return errno == 0 && old == !changed && fresh == changed && added == changed;
    }
    auto directory(bool changed) -> bool {
        std::unique_ptr<DIR, decltype(&closedir)> stream(opendir(root_), closedir);
        bool const FRESH = directory(stream.get(), changed);
        return stream && closedir(stream.release()) == 0 && FRESH;
    }
    auto cache() -> bool {
        Descriptor original(open_file("original", O_RDONLY)), removed(open_file("removed", O_RDONLY));
        std::unique_ptr<DIR, decltype(&closedir)> retained(opendir(root_), closedir);
        struct stat before{};
        if (original.get() < 0 || removed.get() < 0 || !bytes(original.get(), "seed") || !bytes(removed.get(), "seed") ||
            fstat(original.get(), &before) != 0 || before.st_size != 4 || !directory(retained.get(), false) || !directory(false) ||
            !mark("cache-armed") || !wait_all("cache-armed"))
            return failure("cache-fill");
        if (role_ == 0) {
            Descriptor writer(open_file("original", O_RDWR)), removed_writer(open_file("removed", O_RDWR));
            if (writer.get() < 0 || removed_writer.get() < 0 || pwrite(writer.get(), "committed", 9, 0) != 9 ||
                pwrite(removed_writer.get(), "unlinked", 8, 0) != 8 || ftruncate(writer.get(), 9) != 0 || fchmod(writer.get(), 0640) != 0)
                return failure("cache-native-commit");
            std::array<char, 512> from{}, to{};
            if (!path("original", from) || !path("moved", to) || rename(from.data(), to.data()) != 0 ||
                !create("original", "replacement") || !path("removed", from) || unlink(from.data()) != 0 || !path("directory-old", from) ||
                !path("directory-new", to) || rename(from.data(), to.data()) != 0 || !create("directory-added") || !writer.finish() ||
                !removed_writer.finish() || !mark("cache-committed"))
                return failure("cache-namespace-commit");
        }
        if (!wait("cache-committed", 0)) return false;
#ifdef __WOS__
        if (role_ != 0) {
            errno = 0;
            if (readdir(retained.get()) != nullptr || errno != EAGAIN) return failure("directory-changed-eof-retry");
        }
#endif
        errno = 0;
        rewinddir(retained.get());
        if (errno != 0 || !directory(retained.get(), true) || closedir(retained.release()) != 0)
            return failure("directory-retained-eof-rewind");
        std::printf("VFS-COHERENCY-CASE role=%u case=directory-retained-eof-rewind\n", role_);
        struct stat after{};
        Descriptor replacement(open_file("original", O_RDONLY));
        bool const ORIGINAL_BYTES = bytes(original.get(), "committed");
        bool const REMOVED_BYTES = bytes(removed.get(), "unlinked");
        int const STAT_RESULT = fstat(original.get(), &after);
        bool const REPLACEMENT_BYTES = replacement.get() >= 0 && bytes(replacement.get(), "replacement");
        bool const DIRECTORY_FRESH = directory(true);
        if (!ORIGINAL_BYTES || !REMOVED_BYTES || STAT_RESULT != 0 || after.st_ino != before.st_ino || after.st_size != 9 ||
            (after.st_mode & 07777) != 0640 || !REPLACEMENT_BYTES || !DIRECTORY_FRESH) {
            std::printf(
                "VFS-COHERENCY-CACHE role=%u original=%u removed=%u stat=%d inode_before=%llu inode_after=%llu size=%lld mode=%o "
                "replacement=%u directory=%u\n",
                role_, ORIGINAL_BYTES ? 1U : 0U, REMOVED_BYTES ? 1U : 0U, STAT_RESULT, static_cast<unsigned long long>(before.st_ino),
                static_cast<unsigned long long>(after.st_ino), static_cast<long long>(after.st_size),
                static_cast<unsigned>(after.st_mode & 07777), REPLACEMENT_BYTES ? 1U : 0U, DIRECTORY_FRESH ? 1U : 0U);
            return failure("cache-fresh-retained-inode");
        }
        std::printf("VFS-COHERENCY-CASE role=%u case=cache-rename-unlink-stat-directory\n", role_);
        return mark("cache-verified") && wait_all("cache-verified");
    }
    auto command(int fd, unsigned family, bool release, bool blocking, off_t start = 0) -> int {
        if (family == 2) return flock(fd, release ? LOCK_UN : LOCK_EX | (blocking ? 0 : LOCK_NB));
        struct flock range{};
        range.l_type = release ? F_UNLCK : F_WRLCK;
        range.l_whence = SEEK_SET;
        range.l_start = start;
        range.l_len = 16;
        return fcntl(fd, family == 0 ? (blocking ? F_SETLKW : F_SETLK) : (blocking ? F_OFD_SETLKW : F_OFD_SETLK), &range);
    }
    auto locks() -> bool {
        constexpr std::array<const char*, 3> FAMILIES{"posix", "ofd", "flock"};
        for (unsigned family = 0; family < FAMILIES.size(); ++family) {
            const char* leaf = FAMILIES.at(family);
            Descriptor file(open_file(leaf, O_RDWR));
            std::array<char, 96> held{}, checked{}, done{};
            std::snprintf(held.data(), held.size(), "%s-held", leaf);
            std::snprintf(checked.data(), checked.size(), "%s-conflict", leaf);
            std::snprintf(done.data(), done.size(), "%s-done", leaf);
            if (file.get() < 0) return failure("lock-open");
            if (family == 1) {
                for (int cmd : {F_OFD_GETLK, F_OFD_SETLK, F_OFD_SETLKW}) {
                    struct flock invalid{};
                    invalid.l_type = F_WRLCK;
                    invalid.l_whence = SEEK_SET;
                    invalid.l_len = 16;
#ifdef __WOS__
                    invalid.l_pid = static_cast<pid_t>(uint64_t{1} << 32);
#else
                    invalid.l_pid = 1;
#endif
                    errno = 0;
                    if (fcntl(file.get(), cmd, &invalid) != -1 || errno != EINVAL) return failure("ofd-invalid-pid");
                }
            }
            if (role_ == 0) {
                if (command(file.get(), family, false, false) != 0 || !mark(held.data())) return failure("owner-acquire");
                for (unsigned peer = 1; peer < PARTICIPANTS; ++peer)
                    if (!wait(checked.data(), peer)) return false;
                if (command(file.get(), family, true, false) != 0) return failure("owner-unlock");
            } else {
                if (!wait(held.data(), 0)) return false;
                errno = 0;
                if (command(file.get(), family, false, false) != -1 || (errno != EAGAIN && errno != EACCES))
                    return failure("remote-conflict");
                if (family != 2) {
                    struct flock query{};
                    query.l_type = F_WRLCK;
                    query.l_whence = SEEK_SET;
                    query.l_len = 16;
                    errno = 0;
                    int const RESULT = fcntl(file.get(), family == 0 ? F_GETLK : F_OFD_GETLK, &query);
                    std::printf(
                        "VFS-COHERENCY-QUERY role=%u family=%s result=%d errno=%d type=%d whence=%d start=%lld length=%lld pid=%lld\n",
                        role_, leaf, RESULT, errno, query.l_type, query.l_whence, static_cast<long long>(query.l_start),
                        static_cast<long long>(query.l_len), static_cast<long long>(query.l_pid));
                    if (RESULT != 0 || query.l_type != F_WRLCK || query.l_start != 0 || query.l_len != 16 ||
                        (family == 0 ? query.l_pid <= 0 : query.l_pid != -1))
                        return failure("remote-query-owner");
                }
                if (!mark(checked.data()) || command(file.get(), family, false, true) != 0 || command(file.get(), family, true, false) != 0)
                    return failure("blocking-grant-unlock");
            }
            if (!file.finish() || !mark(done.data()) || !wait_all(done.data())) return failure("lock-close-barrier");
            std::printf("VFS-COHERENCY-CASE role=%u case=%s-query-conflict-wait\n", role_, leaf);
        }
        // An independent descriptor close releases this process's POSIX lock.
        // OFD query has different ownership and can observe that POSIX range.
        Descriptor holder(open_file("close", O_RDWR)), independent(open_file("close", O_RDWR));
        off_t const START = static_cast<off_t>(role_ * 32);
        struct flock query{};
        query.l_type = F_WRLCK;
        query.l_whence = SEEK_SET;
        query.l_start = START;
        query.l_len = 16;
        if (holder.get() < 0 || independent.get() < 0 || command(holder.get(), 0, false, false, START) != 0 ||
            fcntl(independent.get(), F_OFD_GETLK, &query) != 0 || query.l_type != F_WRLCK || !independent.finish())
            return failure("any-descriptor-close-setup");
        query.l_type = F_WRLCK;
        query.l_pid = 0;
        if (fcntl(holder.get(), F_OFD_GETLK, &query) != 0 || query.l_type != F_UNLCK) return failure("any-descriptor-close-release");
        return holder.finish();
    }
    auto query(int fd, short type, pid_t pid, off_t start = 0) -> bool {
        struct flock value{};
        value.l_type = F_WRLCK;
        value.l_whence = SEEK_SET;
        value.l_start = start;
        value.l_len = 16;
        return fcntl(fd, F_OFD_GETLK, &value) == 0 && value.l_type == type &&
               (type == F_UNLCK || (value.l_start == start && value.l_len == 16 && value.l_pid == pid));
    }
    auto join(pid_t child, int signal = 0) -> bool {
        int status = 0;
        pid_t observed = -1;
        do {
            observed = waitpid(child, &status, 0);
        } while (observed == -1 && errno == EINTR);
        if (observed == child &&
            (signal == 0 ? WIFEXITED(status) && WEXITSTATUS(status) == 0 : WIFSIGNALED(status) && WTERMSIG(status) == signal))
            return true;
        std::printf("VFS-COHERENCY-CHILD role=%u pid=%lld observed=%lld status=%d\n", role_, static_cast<long long>(child),
                    static_cast<long long>(observed), status);
        return false;
    }
    auto lifecycle() -> bool {
        std::array<char, 32> leaf{};
        std::snprintf(leaf.data(), leaf.size(), "lifecycle-%u", role_);
        pid_t const PARENT = getpid();
        {
            Descriptor holder(open_file(leaf.data(), O_RDWR)), observer(open_file(leaf.data(), O_RDWR));
            if (holder.get() < 0 || observer.get() < 0 || command(holder.get(), 0, false, false) != 0) return failure("fork-posix-parent");
            pid_t const CHILD = fork();
            if (CHILD < 0) return failure("fork-posix");
            if (CHILD == 0) {
                struct flock value{};
                value.l_type = F_WRLCK;
                value.l_whence = SEEK_SET;
                value.l_len = 16;
                if (fcntl(holder.get(), F_GETLK, &value) != 0 || value.l_type != F_WRLCK || value.l_pid != PARENT) _exit(11);
                errno = 0;
                if (command(holder.get(), 0, false, false) != -1 || (errno != EAGAIN && errno != EACCES)) _exit(12);
                if (!holder.finish()) _exit(13);
                _exit(0);
            }
            if (!join(CHILD) || !query(observer.get(), F_WRLCK, PARENT) || !observer.finish() || !query(holder.get(), F_UNLCK, 0) ||
                !holder.finish())
                return failure("fork-posix-parent-survives-child-close");
        }
        std::printf("VFS-COHERENCY-CASE role=%u case=fork-posix-scope\n", role_);
        {
            Descriptor holder(open_file(leaf.data(), O_RDWR)), observer(open_file(leaf.data(), O_RDWR));
            if (holder.get() < 0 || observer.get() < 0 || command(holder.get(), 1, false, false) != 0) return failure("fork-ofd-parent");
            pid_t const CHILD = fork();
            if (CHILD < 0) return failure("fork-ofd");
            if (CHILD == 0) {
                if (command(holder.get(), 1, false, false) != 0 || !holder.finish()) _exit(21);
                _exit(0);
            }
            if (!join(CHILD) || !query(observer.get(), F_WRLCK, -1)) return failure("fork-ofd-child-close");
            Descriptor alias(dup(holder.get()));
            if (alias.get() < 0 || !holder.finish() || !query(observer.get(), F_WRLCK, -1) || !alias.finish() ||
                !query(observer.get(), F_UNLCK, 0) || !observer.finish())
                return failure("ofd-last-shared-close");
        }
        std::printf("VFS-COHERENCY-CASE role=%u case=fork-ofd-last-close\n", role_);
        {
            Descriptor observer(open_file(leaf.data(), O_RDWR));
            std::array<int, 2> ready{}, release{};
            if (observer.get() < 0 || pipe(ready.data()) != 0) return failure("exit-ready-pipe");
            Descriptor ready_read(ready.at(0)), ready_write(ready.at(1));
            if (pipe(release.data()) != 0) return failure("exit-release-pipe");
            Descriptor release_read(release.at(0)), release_write(release.at(1));
            pid_t const CHILD = fork();
            if (CHILD < 0) return failure("exit-fork");
            if (CHILD == 0) {
                if (!observer.finish() || !ready_read.finish() || !release_write.finish()) _exit(31);
                Descriptor holder(open_file(leaf.data(), O_RDWR));
                if (holder.get() < 0 || command(holder.get(), 0, false, false) != 0) _exit(32);
                char value = 'L';
                if (write(ready_write.get(), &value, 1) != 1 || read(release_read.get(), &value, 1) != 1 || value != 'X') _exit(33);
                // No explicit close/unlock: kernel process exit owns retirement.
                _exit(0);
            }
            if (!ready_write.finish() || !release_read.finish()) return failure("exit-parent-pipes");
            char value = 0;
            if (read(ready_read.get(), &value, 1) != 1 || value != 'L' || !query(observer.get(), F_WRLCK, CHILD))
                return failure("exit-child-lock-visible");
            value = 'X';
            if (write(release_write.get(), &value, 1) != 1 || !join(CHILD) || !query(observer.get(), F_UNLCK, 0) ||
                command(observer.get(), 0, false, false) != 0 || command(observer.get(), 0, true, false) != 0 || !observer.finish())
                return failure("exit-lock-release-before-waitpid");
        }
        std::printf("VFS-COHERENCY-CASE role=%u case=exit-lock-retirement\n", role_);
        return mark("lifecycle-verified") && wait_all("lifecycle-verified");
    }
    auto exec_lifecycle() -> bool {
        std::array<char, 32> leaf{};
        std::snprintf(leaf.data(), leaf.size(), "lifecycle-%u", role_);
        Descriptor observer(open_file(leaf.data(), O_RDWR));
        std::array<int, 2> ready{}, release{};
        if (observer.get() < 0 || pipe(ready.data()) != 0) return failure("exec-ready-pipe");
        Descriptor ready_read(ready.at(0)), ready_write(ready.at(1));
        if (pipe(release.data()) != 0) return failure("exec-release-pipe");
        Descriptor release_read(release.at(0)), release_write(release.at(1));
        pid_t const CHILD = fork();
        if (CHILD < 0) return failure("exec-fork");
        if (CHILD == 0) {
            if (!observer.finish() || !ready_read.finish() || !release_write.finish()) _exit(41);
            Descriptor holder(open_file(leaf.data(), O_RDWR | O_CLOEXEC));
            if (holder.get() < 0 || command(holder.get(), 0, false, false) != 0 || command(holder.get(), 1, false, false, 32) != 0)
                _exit(42);
            char value = 'L';
            if (write(ready_write.get(), &value, 1) != 1 || read(release_read.get(), &value, 1) != 1 || value != 'C') _exit(43);
            std::array<std::array<char, 24>, 3> arguments{};
            std::snprintf(arguments.at(0).data(), arguments.at(0).size(), "%d", holder.get());
            std::snprintf(arguments.at(1).data(), arguments.at(1).size(), "%d", ready_write.get());
            std::snprintf(arguments.at(2).data(), arguments.at(2).size(), "%d", release_read.get());
            execl("/proc/self/exe", "testprog", "vfs-coherency-exec-probe", arguments.at(0).data(), arguments.at(1).data(),
                  arguments.at(2).data(), static_cast<char*>(nullptr));
            _exit(44);
        }
        if (!ready_write.finish() || !release_read.finish()) return failure("exec-parent-pipes");
        char value = 0;
        if (read(ready_read.get(), &value, 1) != 1 || value != 'L' || !query(observer.get(), F_WRLCK, CHILD) ||
            !query(observer.get(), F_WRLCK, -1, 32))
            return failure("exec-child-locks-visible");
        value = 'C';
        if (write(release_write.get(), &value, 1) != 1 || read(ready_read.get(), &value, 1) != 1 || value != 'E' ||
            !query(observer.get(), F_UNLCK, 0) || !query(observer.get(), F_UNLCK, 0, 32))
            return failure("exec-cloexec-lock-release");
        // The successor is alive and waiting on the release pipe during both
        // queries, so normal process exit cannot explain this retirement.
        value = 'X';
        if (write(release_write.get(), &value, 1) != 1 || !join(CHILD) || !observer.finish()) return failure("exec-successor-exit");
        std::printf("VFS-COHERENCY-CASE role=%u case=exec-cloexec-lock-retirement\n", role_);
        return mark("exec-verified") && wait_all("exec-verified");
    }
    auto observe_wait_child(pid_t child) -> bool {
        std::array<char, 64> name{};
        std::snprintf(name.data(), name.size(), "/proc/%lld/stat", static_cast<long long>(child));
        char previous = 0;
        for (;;) {
            Descriptor file(open(name.data(), O_RDONLY));
            std::array<char, 1024> text{};
            if (file.get() < 0) return failure("wait-child-stat-open");
            ssize_t const LENGTH = read(file.get(), text.data(), text.size() - 1);
            if (LENGTH <= 0) return failure("wait-child-stat-read");
            const char* end = std::strrchr(text.data(), ')');
            if (end != nullptr && end[1] == ' ' && end[2] != previous) {
                previous = end[2];
                std::printf("VFS-COHERENCY-WAIT role=%u child=%lld state=%c\n", role_, static_cast<long long>(child), previous);
                std::fflush(stdout);
            }
#ifdef __WOS__
            // Short RPC polling can produce a runnable sample. The owner's
            // exact zero-to-two pending gate proves both native acquisitions.
            if (end != nullptr && end[1] == ' ' && (end[2] == 'R' || end[2] == 'S' || end[2] == 'D')) return true;
#else
            if (end != nullptr && end[1] == ' ' && (end[2] == 'S' || end[2] == 'D')) return true;
#endif
            if (seconds() < 0 || seconds() >= deadline_) return failure("wait-child-not-sleeping");
            timespec const PAUSE{0, 20'000'000};
            static_cast<void>(nanosleep(&PAUSE, nullptr));
        }
    }
    auto remote_pending(unsigned expected) -> bool {
#ifdef __WOS__
        // The native counter covers remote controller waits only. Requiring
        // a zero baseline before either child starts, then exactly two pending
        // receipts, establishes both independent remote acquisitions.
        for (;;) {
            FILE* stream = std::fopen("/proc/wki/netdiag", "r");
            if (stream == nullptr) return failure("wait-owner-diagnostic-open");
            std::array<char, 4096> line{};
            unsigned rows = 0, ends = 0;
            uint64_t pending = 0;
            bool valid = true;
            while (std::fgets(line.data(), static_cast<int>(line.size()), stream) != nullptr) {
                std::string_view const TEXT(line.data());
                if (TEXT.starts_with("wki_vfs_advisory ")) {
                    ++rows;
                    const char* token = std::strstr(line.data(), " pending=");
                    char* end = nullptr;
                    errno = 0;
                    if (!TEXT.starts_with("wki_vfs_advisory version=1 ") || token == nullptr) {
                        valid = false;
                    } else {
                        pending = std::strtoull(token + 9, &end, 10);
                        valid &= errno == 0 && end != token + 9 && (*end == ' ' || *end == '\n');
                    }
                }
                ends += TEXT == "wki_netdiag_end complete=1\n" ? 1U : 0U;
            }
            valid &= std::ferror(stream) == 0;
            valid &= std::fclose(stream) == 0;
            if (!valid || rows != 1 || ends != 1) return failure("wait-owner-diagnostic-incomplete");
            if (pending == expected) {
                std::printf("VFS-COHERENCY-PENDING role=%u remote=%llu expected=%u\n", role_, static_cast<unsigned long long>(pending),
                            expected);
                return true;
            }
            if (expected != 0 && pending > expected) return failure("wait-owner-unexpected-pending");
            if (seconds() < 0 || seconds() >= deadline_) return failure("wait-owner-pending-timeout");
            timespec const PAUSE{0, 20'000'000};
            static_cast<void>(nanosleep(&PAUSE, nullptr));
        }
#else
        static_cast<void>(expected);
        return true;
#endif
    }
    auto wait_lifecycle() -> bool {
        constexpr std::array<const char*, 3> FAMILIES{"posix", "ofd", "flock"};
        std::array<char, 32> leaf{};
        std::snprintf(leaf.data(), leaf.size(), "lifecycle-%u", role_);
        for (bool fatal : {false, true}) {
            for (unsigned family = 0; family < FAMILIES.size(); ++family) {
#ifdef __WOS__
                // The owner's legacy native lock loop yields while runnable.
                // Coordinate the two remote waiters here; the host
                // baseline still exercises native signal/kill on all roles.
                bool const COORDINATOR = role_ == 0;
#else
                bool const COORDINATOR = false;
#endif
                Descriptor holder(open_file(leaf.data(), O_RDWR)), observer(open_file(leaf.data(), O_RDWR));
                std::array<int, 2> ready{};
                if (holder.get() < 0 || observer.get() < 0 || command(holder.get(), family, false, false) != 0 || pipe(ready.data()) != 0)
                    return failure("wait-parent-setup");
                Descriptor ready_read(ready.at(0)), ready_write(ready.at(1));
                std::array<char, 96> start{}, armed{}, pending{}, done{};
                const char* mode = fatal ? "kill" : "signal";
                std::snprintf(start.data(), start.size(), "%s-%s-start", mode, FAMILIES.at(family));
                std::snprintf(armed.data(), armed.size(), "%s-%s-ready", mode, FAMILIES.at(family));
                std::snprintf(pending.data(), pending.size(), "%s-%s-pending", mode, FAMILIES.at(family));
                std::snprintf(done.data(), done.size(), "%s-%s-done", mode, FAMILIES.at(family));
                if ((role_ == 0 && (!remote_pending(0) || !mark(start.data()))) || !wait(start.data(), 0))
                    return failure("wait-empty-baseline");
                pid_t const CHILD = COORDINATOR ? 0 : fork();
                if (CHILD < 0) return failure("wait-fork");
                if (!COORDINATOR && CHILD == 0) {
                    std::printf("VFS-COHERENCY-WAIT role=%u child=%lld family=%s stage=setup\n", role_, static_cast<long long>(getpid()),
                                FAMILIES.at(family));
                    std::fflush(stdout);
                    if (!holder.finish() || !observer.finish() || !ready_read.finish()) _exit(51);
                    struct sigaction action{};
                    action.sa_handler = interrupt_wait;
                    if (!fatal && (sigemptyset(&action.sa_mask) != 0 || sigaction(SIGUSR1, &action, nullptr) != 0)) _exit(52);
                    interrupted = 0;
                    Descriptor waiter(open_file(leaf.data(), O_RDWR));
                    char const VALUE = 'W';
                    if (waiter.get() < 0 || write(ready_write.get(), &VALUE, 1) != 1) _exit(53);
                    std::printf("VFS-COHERENCY-WAIT role=%u child=%lld family=%s stage=blocking\n", role_, static_cast<long long>(getpid()),
                                FAMILIES.at(family));
                    std::fflush(stdout);
                    errno = 0;
                    int const RESULT = command(waiter.get(), family, false, true);
                    if (fatal || RESULT != -1 || errno != EINTR || interrupted != 1) _exit(54);
                    _exit(0);
                }
                std::printf("VFS-COHERENCY-WAIT role=%u child=%lld family=%s stage=parent-ready\n", role_, static_cast<long long>(CHILD),
                            FAMILIES.at(family));
                std::fflush(stdout);
                char value = 0;
                if (!ready_write.finish() ||
                    (!COORDINATOR && (read(ready_read.get(), &value, 1) != 1 || value != 'W' || !observe_wait_child(CHILD))))
                    return failure("wait-child-readiness");
                if (!mark(armed.data()) || !wait_all(armed.data()) || (role_ == 0 && (!remote_pending(2) || !mark(pending.data()))) ||
                    !wait(pending.data(), 0))
                    return failure("wait-confirmed-barrier");
                if (!COORDINATOR && (kill(CHILD, fatal ? SIGKILL : SIGUSR1) != 0 || !join(CHILD, fatal ? SIGKILL : 0)))
                    return failure("wait-signal-join");
                if (family != 2) {
                    if (!query(observer.get(), F_WRLCK, family == 0 ? getpid() : -1)) return failure("wait-parent-lock-survives");
                } else {
                    errno = 0;
                    if (command(observer.get(), family, false, false) != -1 || errno != EAGAIN)
                        return failure("wait-parent-flock-survives");
                }
                if (command(holder.get(), family, true, false) != 0 || command(observer.get(), family, false, false) != 0 ||
                    command(observer.get(), family, true, false) != 0 || !holder.finish() || !observer.finish() || !mark(done.data()) ||
                    !wait_all(done.data()))
                    return failure("wait-successor-cleanup");
                std::printf("VFS-COHERENCY-CASE role=%u case=%s-%s-%s\n", role_, mode, FAMILIES.at(family),
                            COORDINATOR ? "remote-coordinator" : "blocked-retirement");
            }
        }
        return true;
    }
    const char* root_;
    unsigned role_;
    uint64_t run_;
    int64_t deadline_;
};

auto number(const char* input, uint64_t& output) -> bool {
    if (input == nullptr || input[0] < '0' || input[0] > '9') return false;
    char* end = nullptr;
    errno = 0;
    output = std::strtoull(input, &end, 10);
    return errno == 0 && end != input && *end == '\0';
}
}  // namespace

auto run_vfs_coherency(int argc, char** argv) -> int {
    uint64_t role = 0, run = 0, timeout = 120;
    if ((argc != 3 && argc != 4) || argv[0][0] != '/' || !number(argv[1], role) || role >= PARTICIPANTS || !number(argv[2], run) ||
        run == 0 || (argc == 4 && (!number(argv[3], timeout) || timeout == 0 || timeout > 600))) {
        std::printf("usage: testprog vfs-coherency ABSOLUTE_ROOT ROLE(0..2) RUN_ID [TIMEOUT_SECONDS(1..600)]\n");
        return 2;
    }
    Campaign campaign(argv[0], static_cast<unsigned>(role), run, static_cast<unsigned>(timeout));
    return campaign.execute() ? 0 : 1;
}

auto run_vfs_coherency_exec_probe(int argc, char** argv) -> int {
    std::array<uint64_t, 3> descriptors{};
    if (argc != 3) return 61;
    for (size_t index = 0; index < descriptors.size(); ++index)
        if (!number(argv[index], descriptors.at(index)) || descriptors.at(index) > INT_MAX) return 62;
    errno = 0;
    if (fcntl(static_cast<int>(descriptors.at(0)), F_GETFD) != -1 || errno != EBADF) return 63;
    char value = 'E';
    if (write(static_cast<int>(descriptors.at(1)), &value, 1) != 1 || read(static_cast<int>(descriptors.at(2)), &value, 1) != 1 ||
        value != 'X')
        return 64;
    return 0;
}
