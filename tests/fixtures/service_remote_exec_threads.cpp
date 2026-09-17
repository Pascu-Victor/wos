#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <signal.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
struct Event {
    char kind{};
    int error{};
    int exit_status{};
    pid_t pid{};
    uid_t real{}, effective{}, saved{};
    std::array<char, 64> host{};
};
std::array<char, 1024> control{};
unsigned ready;
bool worker_exec;
unsigned replacement_status;
bool fallback_mode, reject_mode, expect_rejection;
bool receiver_reject;
bool retired_owner;
pthread_t owner_thread;
char* image;
char** arguments;

auto now_ms() -> uint64_t {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value)) _exit(90);
    return static_cast<uint64_t>(value.tv_sec) * 1000 + value.tv_nsec / 1000000;
}

auto path(const char* directory, const char* leaf) -> std::array<char, 1200> {
    std::array<char, 1200> result{};
    int const SIZE = std::snprintf(result.data(), result.size(), "%s/%s", directory, leaf);
    if (SIZE < 0 || static_cast<size_t>(SIZE) >= result.size()) _exit(91);
    return result;
}

auto write_record(const char* directory, const char* leaf, const void* data, size_t size) -> bool {
    auto const NAME = path(directory, leaf);
    int const FD = open(NAME.data(), O_WRONLY | O_CREAT | O_EXCL, 0666);
    if (FD < 0) return false;
    bool const OK = write(FD, data, size) == static_cast<ssize_t>(size);
    return close(FD) == 0 && OK;
}

auto read_record(const char* directory, const char* leaf, void* data, size_t size, unsigned timeout_ms) -> bool {
    auto const NAME = path(directory, leaf);
    auto const DEADLINE = now_ms() + timeout_ms;
    do {
        int const FD = open(NAME.data(), O_RDONLY);
        if (FD >= 0) {
            bool const OK = read(FD, data, size) == static_cast<ssize_t>(size);
            close(FD);
            if (OK) return true;
        }
        usleep(1000);
    } while (now_ms() < DEADLINE);
    return false;
}

void event(char kind, int error = 0, int exit_status = 0) {
    Event value{.kind = kind, .error = error, .exit_status = exit_status, .pid = getpid()};
    if (getresuid(&value.real, &value.effective, &value.saved) || gethostname(value.host.data(), value.host.size())) _exit(91);
    if (!write_record(control.data(), kind == 'O' ? "old" : "replacement", &value, sizeof(value))) _exit(92);
}

[[noreturn]] void replace() {
    execv(image, arguments);
    event('F', errno, replacement_status);
    if (expect_rejection) {
        char done{};
        if (!read_record(control.data(), "finish", &done, 1, 30000) || done != 'D') _exit(93);
        if (!write_record(control.data(), "finished", &done, 1)) _exit(93);
        _exit(replacement_status);
    }
    _exit(93);
}

[[noreturn]] void old_actor() {
    __atomic_store_n(&ready, 1, __ATOMIC_RELEASE);
    char release{};
    if (!read_record(control.data(), "release", &release, 1, 30000) || release != 'R') _exit(94);
    event('O');
    // A baseline failure must not leave an old actor spinning indefinitely.
    // Its thread exit follows the witness; the replacement remains alive.
    pthread_exit(nullptr);
}

void* worker(void*) {
    if (!worker_exec) old_actor();
    if (retired_owner && pthread_join(owner_thread, nullptr)) _exit(95);
    auto const DEADLINE = now_ms() + 10000;
    while (!__atomic_load_n(&ready, __ATOMIC_ACQUIRE)) {
        if (now_ms() >= DEADLINE) _exit(95);
        usleep(1000);
    }
    replace();
}

void* old_worker(void*) { old_actor(); }

auto reap(pid_t child, int& status, unsigned timeout_ms) -> bool {
    auto const DEADLINE = now_ms() + timeout_ms;
    do {
        auto const RESULT = waitpid(child, &status, WNOHANG);
        if (RESULT == child) return true;
        if (RESULT < 0 && errno != EINTR) return false;
        usleep(1000);
    } while (now_ms() < DEADLINE);
    return false;
}

auto payload(int argc, char** argv) -> int {
    if (argc != 4) return 2;
    std::snprintf(control.data(), control.size(), "%s", argv[2]);
    event('E', 0, std::atoi(argv[3]));
    char done{};
    if (!read_record(control.data(), "finish", &done, 1, 30000) || done != 'D') return 3;
    return write_record(control.data(), "finished", &done, 1) ? std::atoi(argv[3]) : 4;
}

auto run_case(bool remote, bool selected_worker, uid_t uid, char* local_image, char* remote_image, char* peer, char* owner) -> int {
    ready = 0;
    worker_exec = selected_worker;
    bool const REMOTE_IMAGE = remote && !fallback_mode && !reject_mode;
    expect_rejection = remote && (reject_mode || receiver_reject);
    image = REMOTE_IMAGE ? remote_image : local_image;
    std::snprintf(control.data(), control.size(), "%s-control-%d-%u-%d", local_image, remote, uid, selected_worker);
    if (mkdir(control.data(), 0777) || chmod(control.data(), 0777)) return 2;
    std::array<char, 1200> receiver_control{};
    if (REMOTE_IMAGE)
        std::snprintf(receiver_control.data(), receiver_control.size(), "/wki/%s%s", owner, control.data());
    else
        std::snprintf(receiver_control.data(), receiver_control.size(), "%s", control.data());
    char flag[] = "--payload";
    std::array<char, 16> exit_text{};
    std::snprintf(exit_text.data(), exit_text.size(), "%u", replacement_status);
    char* args[] = {image, flag, receiver_control.data(), exit_text.data(), nullptr};
    arguments = args;
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        if (setgroups(0, nullptr) || setgid(uid) || setuid(uid)) _exit(96);
        const char* target = fallback_mode || reject_mode ? "goal19-missing-process-fixture" : peer;
        auto const PLACEMENT =
            remote ? ker::process::setwkitarget(target, std::strlen(target), fallback_mode ? 0 : ker::process::WKI_TARGET_FLAG_STRICT)
                   : ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL);
        if (PLACEMENT != 0) _exit(97);
        owner_thread = pthread_self();
        pthread_t thread;
        if (pthread_create(&thread, nullptr, worker, nullptr)) _exit(98);
        if (worker_exec && retired_owner) {
            pthread_t observer;
            if (pthread_create(&observer, nullptr, old_worker, nullptr)) _exit(98);
            pthread_exit(nullptr);
        }
        if (worker_exec) old_actor();
        auto const DEADLINE = now_ms() + 10000;
        while (!__atomic_load_n(&ready, __ATOMIC_ACQUIRE)) {
            if (now_ms() >= DEADLINE) _exit(99);
            usleep(1000);
        }
        replace();
    }
    if (CHILD < 0) {
        rmdir(control.data());
        return 2;
    }
    Event replacement{}, old{};
    bool const REPLACED = read_record(control.data(), "replacement", &replacement, sizeof(replacement), 15000) &&
                          (expect_rejection ? replacement.kind == 'F' && replacement.error == (receiver_reject ? EIO : EHOSTUNREACH)
                                            : replacement.kind == 'E');
    bool const IDENTITY = replacement.real == uid && replacement.effective == uid && replacement.saved == uid;
    bool const HOST = std::strcmp(replacement.host.data(), REMOTE_IMAGE && !expect_rejection ? peer : owner) == 0;
    char wake = 'R';
    bool const RELEASED = write_record(control.data(), "release", &wake, 1);
    bool const OLD_SURVIVED = REPLACED && RELEASED && read_record(control.data(), "old", &old, sizeof(old), 1000) && old.kind == 'O';
    char done = 'D';
    bool const FINISHED =
        write_record(control.data(), "finish", &done, 1) && read_record(control.data(), "finished", &done, 1, 30000) && done == 'D';
    int status = 0;
    bool normal_reap = reap(CHILD, status, 5000);
    bool reaped = normal_reap;
    if (!reaped) {
        kill(CHILD, SIGKILL);
        reaped = reap(CHILD, status, 5000);
    }
    bool cleaned = reaped && FINISHED;
    if (cleaned) {
        for (const char* leaf : {"replacement", "old", "release", "finish", "finished"}) {
            auto const NAME = path(control.data(), leaf);
            if (unlink(NAME.data()) && errno != ENOENT) cleaned = false;
        }
        if (rmdir(control.data())) cleaned = false;
    }
    bool const PASS = REPLACED && IDENTITY && HOST && replacement.exit_status == static_cast<int>(replacement_status) && RELEASED &&
                      OLD_SURVIVED == expect_rejection && FINISHED && normal_reap && WIFEXITED(status) &&
                      WEXITSTATUS(status) == replacement_status && cleaned;
    std::printf(
        "%s placement=%s executor=%s uid=%u source=%ld receiver=%ld host=%s identity=%d replacement=%d old_survived=%d "
        "old_pid=%ld old_host=%s old_uid=%u/%u/%u exec_error=%d normal_reap=%d status=%d payload_exit=%d expected_exit=%u exit_ok=%d "
        "cleaned=%d "
        "reaped=%d\n",
        PASS ? "PASS" : "FAIL",
        remote ? (fallback_mode     ? "fallback"
                  : receiver_reject ? "receiver-rejected"
                  : reject_mode     ? "rejected"
                                    : "remote")
               : "local",
        selected_worker ? (retired_owner ? "retired-owner-worker" : "worker") : "main", uid, static_cast<long>(CHILD),
        static_cast<long>(replacement.pid), replacement.host.data(), IDENTITY, REPLACED, OLD_SURVIVED, static_cast<long>(old.pid),
        old.host.data(), old.real, old.effective, old.saved, replacement.error, normal_reap, status, replacement.exit_status,
        replacement_status, WIFEXITED(status) && WEXITSTATUS(status) == replacement_status, cleaned, reaped);
    return reaped && cleaned ? (PASS ? 0 : 1) : 2;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);
    signal(SIGPIPE, SIG_IGN);
    if (argc > 1 && std::strcmp(argv[1], "--payload") == 0) return payload(argc, argv);
    if (argc < 4) return 2;
    for (int index = 4; index < argc; ++index) {
        if (std::strcmp(argv[index], "--receiver-reject") == 0) {
            receiver_reject = true;
            continue;
        }
        if (std::strcmp(argv[index], "--retired-owner") == 0) {
            retired_owner = true;
            continue;
        }
        if (std::strcmp(argv[index], "--fallback") == 0) {
            fallback_mode = true;
            continue;
        }
        if (std::strcmp(argv[index], "--reject") == 0) {
            reject_mode = true;
            continue;
        }
        if (std::strcmp(argv[index], "--exit-status") || ++index == argc) return 2;
        char* end = nullptr;
        auto const STATUS = std::strtoul(argv[index], &end, 10);
        if (argv[index][0] == '\0' || *end != '\0' || STATUS > 255) return 2;
        replacement_status = static_cast<unsigned>(STATUS);
    }
    if (static_cast<unsigned>(fallback_mode) + reject_mode + receiver_reject > 1) return 2;
    unsigned failed = 0;
    for (bool remote : {false, true}) {
        for (uid_t uid : {0U, 100U}) {
            for (bool selected_worker : {false, true}) {
                int const RESULT = run_case(remote, selected_worker, uid, argv[0], argv[3], argv[1], argv[2]);
                if (RESULT == 2) return 2;
                failed += RESULT != 0;
            }
        }
    }
    std::printf("remote exec thread ownership: %u/8 cases passed\n", 8 - failed);
    return failed ? 1 : 0;
}
