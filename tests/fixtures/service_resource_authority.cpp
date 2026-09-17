// Ordinary service-management and process-local resource authority checks.
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
namespace service = ker::abi::service_resource;

int drop(unsigned mode) {
    if (mode == 1 && (setgid(1000) || setuid(1000))) return 90;
    if (mode == 2 && ker::process::prctl(0x574f5302, 0, 0, 0, 0)) return 90;
    if (mode >= 3) {
        unsigned const id = 100 + mode - 3;
        unsigned const grants = mode == 4 ? 1 : mode == 5 ? 2 : 0;
        if (ker::process::prctl(38, 1, 0, 0, 0) || ker::process::prctl(0x574f5303, grants, 0, 0, 0) || setgid(id) || setuid(id)) return 90;
    }
    return 0;
}

int profile(unsigned mode, int report) {
    rlimit initial{524288, 1048576};
    if (setrlimit(RLIMIT_CORE, &initial) || drop(mode)) return 90;
    unsigned failures = 0, checks = 0;
    auto check = [&](const char* operation, int64_t result, int64_t expected, bool preserved) {
        bool const ok = result == expected && preserved;
        dprintf(report, "profile=%u op=%s result=%lld expected=%lld preserved=%d\n", mode, operation, static_cast<long long>(result),
                static_cast<long long>(expected), preserved);
        ++checks;
        if (!ok) ++failures;
    };
    service::Configuration config{};
    config.size = sizeof(config);
    config.version = service::VERSION;
    config.slot = service::MAX_SERVICES - 1;
    memcpy(config.name, "goal19-resource", 16);
    auto const before_config = config;
    auto result = ker::process::service_resource_configure(&config);
    check("configure", result, -EPERM, !memcmp(&config, &before_config, sizeof(config)));
    check("configure-null", ker::process::service_resource_configure(nullptr), -EPERM, true);
    check("select-clear", ker::process::service_resource_select(service::NO_SELECTION), -EPERM, true);
    check("select-slot", ker::process::service_resource_select(0), -EPERM, true);

    service::Snapshot snapshot;
    memset(&snapshot, 0xa5, sizeof(snapshot));
    auto const sentinel = snapshot;
    result = ker::process::service_resource_read(0, &snapshot);
    bool const read_state = mode == 0 ? snapshot.size == sizeof(snapshot) && snapshot.version == service::VERSION && snapshot.slot == 0 &&
                                            snapshot.name[0] != '\0'
                                      : !memcmp(&snapshot, &sentinel, sizeof(snapshot));
    check("read-slot", result, mode == 0 ? 0 : -EACCES, read_state);
    snapshot = sentinel;
    result = ker::process::service_resource_read(service::MAX_SERVICES, &snapshot);
    check("read-invalid", result, mode == 0 ? -EINVAL : -EACCES, !memcmp(&snapshot, &sentinel, sizeof(snapshot)));

    rlimit raised{524288, 2097152}, observed{};
    errno = 0;
    int status = setrlimit(RLIMIT_CORE, &raised);
    int error = errno;
    bool observed_ok = getrlimit(RLIMIT_CORE, &observed) == 0;
    rlimit const expected = mode == 0 ? raised : initial;
    check("raise-hard", status == 0 ? 0 : -error, mode == 0 ? 0 : -EPERM,
          observed_ok && observed.rlim_cur == expected.rlim_cur && observed.rlim_max == expected.rlim_max);
    rlimit soft{expected.rlim_max, expected.rlim_max};
    errno = 0;
    status = setrlimit(RLIMIT_CORE, &soft);
    error = errno;
    observed_ok = getrlimit(RLIMIT_CORE, &observed) == 0;
    check("raise-soft", status == 0 ? 0 : -error, 0,
          observed_ok && observed.rlim_cur == soft.rlim_cur && observed.rlim_max == soft.rlim_max);
    rlimit invalid{soft.rlim_max + 1, soft.rlim_max};
    errno = 0;
    status = setrlimit(RLIMIT_CORE, &invalid);
    error = errno;
    observed_ok = getrlimit(RLIMIT_CORE, &observed) == 0;
    check("invalid-limit", status == 0 ? 0 : -error, -EINVAL,
          observed_ok && observed.rlim_cur == soft.rlim_cur && observed.rlim_max == soft.rlim_max);
    return failures || checks != 9 ? 91 : 0;
}

struct ProfileRequest {
    unsigned mode;
    int report;
};

void* run_profile(void* argument) {
    auto const& request = *static_cast<ProfileRequest*>(argument);
    return reinterpret_cast<void*>(static_cast<uintptr_t>(profile(request.mode, request.report)));
}
}  // namespace

int main(int argc, char** argv) {
    char node[64]{};
    bool const worker = argc == 4 && strcmp(argv[3], "--worker") == 0;
    if ((argc != 3 && !worker) || geteuid() != 0 || gethostname(node, sizeof(node)) || strcmp(node, argv[2])) return 2;
    int const report = open(argv[1], O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (report < 0) return 3;
    unsigned failures = 0;
    dprintf(report, "node=%s execution=%s\n", node, worker ? "worker" : "main");
    for (unsigned mode = 0; mode < 6; ++mode) {
        pid_t child = fork();
        if (child == 0) {
            alarm(10);
            if (!worker) _exit(profile(mode, report));
            ProfileRequest request{mode, report};
            pthread_t thread;
            void* result = nullptr;
            if (pthread_create(&thread, nullptr, run_profile, &request) || pthread_join(thread, &result)) _exit(92);
            _exit(static_cast<int>(reinterpret_cast<uintptr_t>(result)));
        }
        int status = -1;
        if (child < 0 || waitpid(child, &status, 0) != child || status != 0) ++failures;
        dprintf(report, "wait profile=%u status=%d\n", mode, status);
    }
    dprintf(report, "%s: resources profiles=6 checks=54 failures=%u\n", failures ? "FAIL" : "PASS", failures);
    int const synced = fsync(report);
    close(report);
    return failures || synced ? 1 : 0;
}
