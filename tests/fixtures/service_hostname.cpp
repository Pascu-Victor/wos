// Functional hostname authorization/publication checks for a disposable WOS VM.
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
constexpr unsigned ROUNDS = 12000;
constexpr unsigned long HOSTNAME = 1UL << 13;
constexpr int DROP = 0x574f5302;
constexpr int GRANTS = 0x574f5303;
const char* names[] = {"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "b",
                       "ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"};
unsigned go, failures;
bool known(const char* name) {
    for (const auto* expected : names)
        if (!strcmp(name, expected)) return true;
    return false;
}
void fail() { __atomic_fetch_add(&failures, 1, __ATOMIC_RELAXED); }
void* writer(void* arg) {
    const auto index = reinterpret_cast<unsigned long>(arg);
    while (!__atomic_load_n(&go, __ATOMIC_ACQUIRE)) usleep(100);
    for (unsigned i = 0; i < ROUNDS; ++i) {
        const auto* name = names[(i + index) % 3];
        if (sethostname(name, strlen(name))) fail();
    }
    return nullptr;
}
void* reader(void*) {
    while (!__atomic_load_n(&go, __ATOMIC_ACQUIRE)) usleep(100);
    for (unsigned i = 0; i < ROUNDS; ++i) {
        char name[64]{};
        utsname info{};
        if (gethostname(name, sizeof(name)) || !known(name)) fail();
        if (uname(&info) || !known(info.nodename)) fail();
    }
    return nullptr;
}
int profile(unsigned mode) {
    if (mode == 0 && setuid(1000)) return 90;
    if (mode == 1 && ker::process::prctl(DROP, 0, 0, 0, 0)) return 90;
    if (mode == 2 && (ker::process::prctl(38, 1, 0, 0, 0) || ker::process::prctl(GRANTS, 2, 0, 0, 0) || setuid(102))) return 90;
    if (mode == 3 && ker::process::prctl(DROP, HOSTNAME, 0, 0, 0)) return 90;
    char before[64]{}, after[64]{};
    if (gethostname(before, sizeof(before))) return 91;
    errno = 0;
    const int result = sethostname("permitted-name", 14);
    const int error = errno;
    if (gethostname(after, sizeof(after))) return 91;
    if (mode == 3) return result == 0 && !strcmp(after, "permitted-name") ? 0 : 92;
    if (result != -1 || error != EPERM || strcmp(before, after)) return 93;
    // Permission refusal precedes user-pointer validation.
    errno = 0;
    if (sethostname(nullptr, 1) != -1 || errno != EPERM) return 94;
    return 0;
}
}  // namespace

int main() {
    char original[64]{};
    if (geteuid() != 0 || gethostname(original, sizeof(original))) return 90;
    for (unsigned mode = 0; mode < 4; ++mode) {
        const pid_t child = fork();
        if (child == 0) _exit(profile(mode));
        int status{};
        if (child < 0 || waitpid(child, &status, 0) != child || status != 0) fail();
        printf("profile=%u status=%d\n", mode, status);
    }
    if (sethostname(names[0], strlen(names[0]))) fail();
    pthread_t threads[4]{};
    unsigned created = 0;
    for (; created < 4; ++created) {
        if (pthread_create(&threads[created], nullptr, created < 2 ? writer : reader,
                           reinterpret_cast<void*>(static_cast<unsigned long>(created)))) {
            fail();
            break;
        }
    }
    __atomic_store_n(&go, 1, __ATOMIC_RELEASE);
    for (unsigned i = 0; i < created; ++i)
        if (pthread_join(threads[i], nullptr)) fail();
    if (sethostname(original, strlen(original))) fail();
    char restored[64]{};
    if (gethostname(restored, sizeof(restored)) || strcmp(restored, original)) fail();
    printf("hostname checks: failures=%u writes=%u reads=%u restored=%s\n", failures, 2 * ROUNDS, 4 * ROUNDS, restored);
    return failures ? 1 : 0;
}
