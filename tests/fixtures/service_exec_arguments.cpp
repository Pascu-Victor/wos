// Disposable-VM coverage for immutable exec arguments and recursive shebangs.
#include <errno.h>
#include <grp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>

namespace {
constexpr unsigned ARGUMENTS = 128;
constexpr unsigned WIDTH = 240;
void make_argument(char* out, unsigned index) {
    for (unsigned i = 0; i < WIDTH - 1; ++i) out[i] = static_cast<char>('a' + (index + i) % 26);
    out[WIDTH - 1] = 0;
}
int payload(int argc, char** argv, unsigned marker) {
    unsigned depth = static_cast<unsigned>(atoi(argv[marker + 1]));
    unsigned profile = static_cast<unsigned>(atoi(argv[marker + 2]));
    unsigned environment = static_cast<unsigned>(atoi(argv[marker + 3]));
    if (marker != 1 + 2 * depth || argc != static_cast<int>(marker + 4 + ARGUMENTS) || getuid() != (profile ? 1000U : 0U)) return 10;
    for (unsigned i = 0; i < depth; ++i) {
        char option[32], script[512];
        snprintf(option, sizeof(option), "arg%u", i);
        snprintf(script, sizeof(script), "%s.s%u", argv[0], i + 1);
        if (strcmp(argv[1 + 2 * i], option) || strcmp(argv[2 + 2 * i], script)) return 11;
    }
    char expected[WIDTH];
    for (unsigned i = 0; i < ARGUMENTS; ++i) {
        make_argument(expected, i);
        if (strcmp(argv[marker + 4 + i], expected)) return 12;
    }
    const char* value = getenv("GOAL19_EXEC_ARGS");
    const char* empty = getenv("GOAL19_EMPTY");
    if (environment ? (!value || strcmp(value, "preserved") || !empty || *empty) : (value || empty)) return 13;
    return 0;
}
int child_case(const char* binary, unsigned depth, unsigned profile, unsigned environment) {
    if (profile && (setgroups(0, nullptr) || setgid(2001) || setuid(1000))) return 20;
    char path[512], depth_text[16], profile_text[16], environment_text[16];
    if (depth && depth != 6)
        snprintf(path, sizeof(path), "%s.s%u", binary, depth);
    else
        snprintf(path, sizeof(path), "%s", binary);
    snprintf(depth_text, sizeof(depth_text), "%u", depth);
    snprintf(profile_text, sizeof(profile_text), "%u", profile);
    snprintf(environment_text, sizeof(environment_text), "%u", environment);
    char marker[] = "--probe";
    char strings[ARGUMENTS][WIDTH];
    char* args[ARGUMENTS + 6] = {path, marker, depth_text, profile_text, environment_text};
    for (unsigned i = 0; i < ARGUMENTS; ++i) {
        make_argument(strings[i], i);
        args[i + 5] = strings[i];
    }
    args[ARGUMENTS + 5] = nullptr;
    char value[] = "GOAL19_EXEC_ARGS=preserved", empty[] = "GOAL19_EMPTY=";
    char* env[] = {value, empty, nullptr};
    if (depth == 6) args[37] = reinterpret_cast<char*>(1);
    int result = execve(path, args, environment ? env : nullptr);
    int error = errno;
    if (((depth == 5 && error == ELOOP) || (depth == 6 && error == EFAULT)) && result == -1 && getuid() == (profile ? 1000U : 0U)) return 0;
    printf("FAIL exec result=%d errno=%d depth=%u\n", result, error, depth);
    return 21;
}
}  // namespace
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    for (int i = 1; i + 3 < argc; ++i) {
        if (!strcmp(argv[i], "--probe")) return payload(argc, argv, static_cast<unsigned>(i));
    }
    if (argc != 1) return 2;
    unsigned failures = 0;
    for (unsigned profile = 0; profile < 2; ++profile) {
        for (unsigned environment = 0; environment < 2; ++environment) {
            for (unsigned depth : {0U, 1U, 4U, 5U, 6U}) {
                pid_t child = fork();
                if (child == 0) _exit(child_case(argv[0], depth, profile, environment));
                int status = 0;
                bool reaped = false;
                for (unsigned i = 0; child > 0 && i < 10000; ++i) {
                    pid_t result = waitpid(child, &status, WNOHANG);
                    if (result == child) {
                        reaped = true;
                        break;
                    }
                    if (result < 0 && errno != EINTR) break;
                    usleep(1000);
                }
                if (child > 0 && !reaped) {
                    kill(child, SIGKILL);
                    waitpid(child, &status, 0);
                }
                bool ok = reaped && WIFEXITED(status) && WEXITSTATUS(status) == 0;
                printf("%s profile=%u env=%u depth=%u reaped=%d status=%d\n", ok ? "PASS" : "FAIL", profile, environment, depth, reaped,
                       status);
                failures += !ok;
            }
        }
    }
    return failures ? 1 : 0;
}
