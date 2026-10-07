#include "vfs_coherency.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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
        if (!wait("initialized", 0) || !append() || !cache() || !locks() || !lifecycle() || !mark("complete")) return false;
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
        int const LENGTH = std::snprintf(name.data(), name.size(), "%s-%u-%llu", stage, role, static_cast<unsigned long long>(run_));
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
    auto directory(bool changed) -> bool {
        DIR* stream = opendir(root_);
        if (stream == nullptr) return false;
        bool old = false, fresh = false;
        errno = 0;
        while (auto* entry = readdir(stream)) {
            old |= std::strcmp(entry->d_name, "directory-old") == 0;
            fresh |= std::strcmp(entry->d_name, "directory-new") == 0;
        }
        int const ERROR = errno;
        bool const CLOSED = closedir(stream) == 0;
        return ERROR == 0 && CLOSED && old == !changed && fresh == changed;
    }
    auto cache() -> bool {
        Descriptor original(open_file("original", O_RDONLY)), removed(open_file("removed", O_RDONLY));
        struct stat before{};
        if (original.get() < 0 || removed.get() < 0 || !bytes(original.get(), "seed") || !bytes(removed.get(), "seed") ||
            fstat(original.get(), &before) != 0 || before.st_size != 4 || !directory(false) || !mark("cache-armed") ||
            !wait_all("cache-armed"))
            return failure("cache-fill");
        if (role_ == 0) {
            Descriptor writer(open_file("original", O_RDWR)), removed_writer(open_file("removed", O_RDWR));
            if (writer.get() < 0 || removed_writer.get() < 0 || pwrite(writer.get(), "committed", 9, 0) != 9 ||
                pwrite(removed_writer.get(), "unlinked", 8, 0) != 8 || ftruncate(writer.get(), 9) != 0 || fchmod(writer.get(), 0640) != 0)
                return failure("cache-native-commit");
            std::array<char, 512> from{}, to{};
            if (!path("original", from) || !path("moved", to) || rename(from.data(), to.data()) != 0 ||
                !create("original", "replacement") || !path("removed", from) || unlink(from.data()) != 0 || !path("directory-old", from) ||
                !path("directory-new", to) || rename(from.data(), to.data()) != 0 || !writer.finish() || !removed_writer.finish() ||
                !mark("cache-committed"))
                return failure("cache-namespace-commit");
        }
        if (!wait("cache-committed", 0)) return false;
        struct stat after{};
        Descriptor replacement(open_file("original", O_RDONLY));
        if (!bytes(original.get(), "committed") || !bytes(removed.get(), "unlinked") || fstat(original.get(), &after) != 0 ||
            after.st_ino != before.st_ino || after.st_size != 9 || (after.st_mode & 07777) != 0640 || replacement.get() < 0 ||
            !bytes(replacement.get(), "replacement") || !directory(true))
            return failure("cache-fresh-retained-inode");
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
    auto query(int fd, short type, pid_t pid) -> bool {
        struct flock value{};
        value.l_type = F_WRLCK;
        value.l_whence = SEEK_SET;
        value.l_len = 16;
        return fcntl(fd, F_OFD_GETLK, &value) == 0 && value.l_type == type &&
               (type == F_UNLCK || (value.l_start == 0 && value.l_len == 16 && value.l_pid == pid));
    }
    auto join(pid_t child) -> bool {
        int status = 0;
        pid_t observed = -1;
        do {
            observed = waitpid(child, &status, 0);
        } while (observed == -1 && errno == EINTR);
        if (observed == child && WIFEXITED(status) && WEXITSTATUS(status) == 0) return true;
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
