#include "xattr_matrix.hpp"

#include <fcntl.h>
#include <pthread.h>
#include <sys/xattr.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <print>
#include <string>
#include <vector>

namespace {

constexpr size_t LARGE_VALUE_SIZE = 64 * 1024;
constexpr size_t TREE_ATTRIBUTE_COUNT = 96;

struct Matrix {
    int failures{};
    int checks{};

    void expect(bool condition, const char* label) {
        checks++;
        if (condition) {
            return;
        }
        failures++;
        std::println(stderr, "xattr-matrix: FAIL {} errno={}", label, errno);
    }
};

struct RaceContext {
    int fd{-1};
    std::atomic<bool> failed{false};
};

void* replace_race_worker(void* private_data) {
    auto* context = static_cast<RaceContext*>(private_data);
    constexpr char VALUE[] = "replace";
    for (size_t iteration = 0; iteration < 64; ++iteration) {
        if (fsetxattr(context->fd, "user.race", VALUE, sizeof(VALUE) - 1, XATTR_REPLACE) != 0 && errno != ENODATA) {
            context->failed.store(true, std::memory_order_release);
            break;
        }
    }
    return nullptr;
}

void* remove_create_race_worker(void* private_data) {
    auto* context = static_cast<RaceContext*>(private_data);
    constexpr char VALUE[] = "create";
    for (size_t iteration = 0; iteration < 64; ++iteration) {
        if (fremovexattr(context->fd, "user.race") != 0 && errno != ENODATA) {
            context->failed.store(true, std::memory_order_release);
            break;
        }
        if (fsetxattr(context->fd, "user.race", VALUE, sizeof(VALUE) - 1, XATTR_CREATE) != 0 && errno != EEXIST) {
            context->failed.store(true, std::memory_order_release);
            break;
        }
    }
    return nullptr;
}

auto list_contains(const std::vector<char>& packed, const char* name) -> bool {
    size_t offset = 0;
    while (offset < packed.size()) {
        size_t const REMAINING = packed.size() - offset;
        size_t const LEN = strnlen(packed.data() + offset, REMAINING);
        if (LEN == REMAINING) {
            return false;
        }
        if (std::strcmp(packed.data() + offset, name) == 0) {
            return true;
        }
        offset += LEN + 1;
    }
    return false;
}

auto expect_value(Matrix& matrix, const char* path, const char* name, const void* expected, size_t expected_size, bool nofollow) -> bool {
    errno = 0;
    ssize_t const REQUIRED = nofollow ? lgetxattr(path, name, nullptr, 0) : getxattr(path, name, nullptr, 0);
    matrix.expect(REQUIRED == static_cast<ssize_t>(expected_size), "size query");
    if (REQUIRED != static_cast<ssize_t>(expected_size)) {
        return false;
    }

    std::vector<uint8_t> value(expected_size == 0 ? 1 : expected_size);
    errno = 0;
    ssize_t const RECEIVED =
        nofollow ? lgetxattr(path, name, value.data(), expected_size) : getxattr(path, name, value.data(), expected_size);
    matrix.expect(RECEIVED == REQUIRED, "value length");
    bool const EQUAL = expected_size == 0 || std::memcmp(value.data(), expected, expected_size) == 0;
    matrix.expect(EQUAL, "value bytes");
    return RECEIVED == REQUIRED && EQUAL;
}

void cleanup_created_paths(const std::string& path, const std::string& link_path, bool link_created) {
    if (link_created) {
        static_cast<void>(unlink(link_path.c_str()));
    }
    static_cast<void>(unlink(path.c_str()));
}

}  // namespace

auto run_xattr_matrix(int argc, char** argv) -> int {
    if (argc == 4 && argv != nullptr && argv[1] != nullptr && argv[2] != nullptr && argv[3] != nullptr &&
        std::strcmp(argv[1], "--expect-set-error") == 0) {
        int expected_errno = 0;
        if (std::sscanf(argv[2], "%d", &expected_errno) != 1 || expected_errno <= 0) {
            std::println(stderr, "xattr-matrix: invalid expected errno {}", argv[2]);
            return 2;
        }
        constexpr char PROBE_VALUE[] = "probe";
        errno = 0;
        int const RESULT = setxattr(argv[3], "user.wos-error-probe", PROBE_VALUE, sizeof(PROBE_VALUE) - 1, 0);
        int const RESULT_ERRNO = errno;
        if (RESULT == 0) {
            static_cast<void>(removexattr(argv[3], "user.wos-error-probe"));
        }
        std::println("xattr-error-probe: result={} errno={} expected={} path={}", RESULT, RESULT_ERRNO, expected_errno, argv[3]);
        return RESULT == -1 && RESULT_ERRNO == expected_errno ? 0 : 1;
    }
    if (argc != 2 || argv == nullptr || argv[1] == nullptr) {
        std::println(stderr, "usage: testprog xattr-matrix <file-path> | xattr-matrix --expect-set-error <errno> <existing-path>");
        return 2;
    }

    std::string const PATH = argv[1];
    std::string const LINK_PATH = PATH + ".xattr-link-" + std::to_string(getpid());
    size_t const LAST_SEPARATOR = PATH.find_last_of('/');
    std::string const LINK_TARGET = LAST_SEPARATOR == std::string::npos ? PATH : PATH.substr(LAST_SEPARATOR + 1);

    int const FD = open(PATH.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (FD < 0) {
        std::println(stderr, "xattr-matrix: cannot create {} errno={}", PATH, errno);
        return 1;
    }

    Matrix matrix{};
    constexpr char INITIAL[] = "abc";
    constexpr char REPLACED[] = "replacement";

    errno = 0;
    matrix.expect(setxattr(PATH.c_str(), "user.basic", INITIAL, sizeof(INITIAL) - 1, XATTR_CREATE) == 0, "path create");
    expect_value(matrix, PATH.c_str(), "user.basic", INITIAL, sizeof(INITIAL) - 1, false);

    errno = 0;
    matrix.expect(setxattr(PATH.c_str(), "user.basic", INITIAL, sizeof(INITIAL) - 1, XATTR_CREATE) == -1 && errno == EEXIST,
                  "create rejects existing");
    errno = 0;
    matrix.expect(setxattr(PATH.c_str(), "user.missing", INITIAL, sizeof(INITIAL) - 1, XATTR_REPLACE) == -1 && errno == ENODATA,
                  "replace rejects missing");
    matrix.expect(setxattr(PATH.c_str(), "user.basic", REPLACED, sizeof(REPLACED) - 1, XATTR_REPLACE) == 0, "replace existing");
    expect_value(matrix, PATH.c_str(), "user.basic", REPLACED, sizeof(REPLACED) - 1, false);

    std::array<char, 2> short_value{};
    errno = 0;
    matrix.expect(getxattr(PATH.c_str(), "user.basic", short_value.data(), short_value.size()) == -1 && errno == ERANGE,
                  "short get buffer");

    matrix.expect(fsetxattr(FD, "user.empty", nullptr, 0, XATTR_CREATE) == 0, "fd empty create");
    errno = 0;
    matrix.expect(fgetxattr(FD, "user.empty", nullptr, 0) == 0, "fd empty size query");

    std::vector<uint8_t> large(LARGE_VALUE_SIZE);
    for (size_t i = 0; i < large.size(); ++i) {
        large.at(i) = static_cast<uint8_t>((i * 131U) ^ (i >> 3U));
    }
    matrix.expect(fsetxattr(FD, "user.large", large.data(), large.size(), XATTR_CREATE) == 0, "64KiB fd create");
    std::vector<uint8_t> large_read(large.size());
    errno = 0;
    ssize_t const LARGE_READ = fgetxattr(FD, "user.large", large_read.data(), large_read.size());
    matrix.expect(LARGE_READ == static_cast<ssize_t>(large.size()) && large_read == large, "64KiB fd round trip");

    for (size_t index = 0; index < TREE_ATTRIBUTE_COUNT; ++index) {
        std::array<char, 32> name{};
        std::array<uint8_t, 192> value{};
        std::snprintf(name.data(), name.size(), "user.tree.%03zu", index);
        value.fill(static_cast<uint8_t>(index));
        if (fsetxattr(FD, name.data(), value.data(), value.size(), XATTR_CREATE) != 0) {
            matrix.expect(false, "DA-tree growth");
            break;
        }
    }

    errno = 0;
    ssize_t const LIST_SIZE = flistxattr(FD, nullptr, 0);
    matrix.expect(LIST_SIZE > 0, "fd list size query");
    if (LIST_SIZE > 0) {
        std::vector<char> list(static_cast<size_t>(LIST_SIZE));
        ssize_t const LIST_READ = flistxattr(FD, list.data(), list.size());
        matrix.expect(LIST_READ == LIST_SIZE, "fd packed list length");
        matrix.expect(list_contains(list, "user.basic"), "list contains basic");
        matrix.expect(list_contains(list, "user.large"), "list contains large");
        matrix.expect(!list_contains(list, "xfs.parent"), "list hides internal namespace");
    }

    for (size_t index = 0; index < TREE_ATTRIBUTE_COUNT; ++index) {
        std::array<char, 32> name{};
        std::snprintf(name.data(), name.size(), "user.tree.%03zu", index);
        if (fremovexattr(FD, name.data()) != 0) {
            matrix.expect(false, "DA-tree shrink");
            break;
        }
    }
    errno = 0;
    matrix.expect(fgetxattr(FD, "user.tree.095", nullptr, 0) == -1 && errno == ENODATA, "DA-tree entries removed");

    matrix.expect(fremovexattr(FD, "user.large") == 0, "remote value remove");
    errno = 0;
    matrix.expect(fgetxattr(FD, "user.large", nullptr, 0) == -1 && errno == ENODATA, "remote value blocks retired");

    constexpr char RACE_INITIAL[] = "initial";
    matrix.expect(fsetxattr(FD, "user.race", RACE_INITIAL, sizeof(RACE_INITIAL) - 1, XATTR_CREATE) == 0, "race seed");
    RaceContext race{.fd = FD};
    pthread_t replace_thread{};
    pthread_t remove_thread{};
    int const REPLACE_STARTED = pthread_create(&replace_thread, nullptr, replace_race_worker, &race);
    int const REMOVE_STARTED = pthread_create(&remove_thread, nullptr, remove_create_race_worker, &race);
    matrix.expect(REPLACE_STARTED == 0 && REMOVE_STARTED == 0, "race workers start");
    if (REPLACE_STARTED == 0) {
        matrix.expect(pthread_join(replace_thread, nullptr) == 0, "replace worker join");
    }
    if (REMOVE_STARTED == 0) {
        matrix.expect(pthread_join(remove_thread, nullptr) == 0, "remove worker join");
    }
    matrix.expect(!race.failed.load(std::memory_order_acquire), "concurrent replace/remove errno contract");
    constexpr char RACE_FINAL[] = "final";
    matrix.expect(fsetxattr(FD, "user.race", RACE_FINAL, sizeof(RACE_FINAL) - 1, 0) == 0, "race final write");
    expect_value(matrix, PATH.c_str(), "user.race", RACE_FINAL, sizeof(RACE_FINAL) - 1, false);

    // Keep the stored target relative to the containing directory.  This is
    // required for the same matrix to exercise a WKI-exported directory: an
    // absolute /wki/... target has meaning only on the client and would turn
    // server-side final-component resolution into a recursive remote lookup.
    bool const LINK_CREATED = symlink(LINK_TARGET.c_str(), LINK_PATH.c_str()) == 0;
    matrix.expect(LINK_CREATED, "create symlink");
    constexpr char FOLLOWED[] = "followed";
    matrix.expect(setxattr(LINK_PATH.c_str(), "user.follow", FOLLOWED, sizeof(FOLLOWED) - 1, XATTR_CREATE) == 0,
                  "path follows final symlink");
    expect_value(matrix, PATH.c_str(), "user.follow", FOLLOWED, sizeof(FOLLOWED) - 1, false);
    errno = 0;
    matrix.expect(lgetxattr(LINK_PATH.c_str(), "user.follow", nullptr, 0) == -1 && errno == ENODATA, "lpath does not follow final symlink");
    errno = 0;
    matrix.expect(lsetxattr(LINK_PATH.c_str(), "user.link", FOLLOWED, sizeof(FOLLOWED) - 1, XATTR_CREATE) == -1 && errno == EPERM,
                  "user namespace rejects symlink mutation");

    errno = 0;
    matrix.expect(setxattr(PATH.c_str(), "xfs.parent", INITIAL, sizeof(INITIAL) - 1, 0) == -1 && errno == EOPNOTSUPP,
                  "internal namespace denied");

    matrix.expect(fremovexattr(FD, "user.empty") == 0, "fd remove");
    errno = 0;
    matrix.expect(fgetxattr(FD, "user.empty", nullptr, 0) == -1 && errno == ENODATA, "removed attr missing");

    int const CLOSE_RET = close(FD);
    matrix.expect(CLOSE_RET == 0, "close test file");
    cleanup_created_paths(PATH, LINK_PATH, LINK_CREATED);

    std::println("xattr-matrix: checks={} failures={} path={}", matrix.checks, matrix.failures, PATH);
    return matrix.failures == 0 ? 0 : 1;
}
