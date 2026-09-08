#include "resource_limits.hpp"

#include <abi/callnums/process.h>
#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <spawn.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/resource_policy.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/times.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <abi/callnums.hpp>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
struct Checks {
    unsigned failed = 0;
    unsigned total = 0;
    void expect(bool condition, const char* name) {
        ++total;
        if (!condition) {
            ++failed;
            std::fprintf(stderr, "resource-limits: FAIL %s errno=%d\n", name, errno);
        }
    }
};

auto nofile(rlim_t soft, rlim_t hard = 256) -> int {
    rlimit const LIMIT{soft, hard};
    return setrlimit(RLIMIT_NOFILE, &LIMIT);
}

auto wait_ok(pid_t pid) -> bool {
    if (pid < 0) {
        return false;
    }
    int status = 0;
    pid_t result;
    do {
        result = waitpid(pid, &status, 0);
    } while (result < 0 && errno == EINTR);
    return result == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

void* thread_policy(void* opaque) {
    auto& ok = *static_cast<bool*>(opaque);
    ok = nofile(3) == 0;
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        rlimit limit{};
        bool const VALID = getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur == 3 && fcntl(1, F_GETFD) >= 0;
        _exit(VALID ? 0 : 1);
    }
    ok = wait_ok(CHILD) && ok;
    return nullptr;
}

void* descriptor_race(void* opaque) {
    auto& failed = *static_cast<std::atomic<bool>*>(opaque);
    for (unsigned i = 0; i < 128; ++i) {
        int const FD = open("/dev/null", O_RDONLY);
        if (FD >= 0) {
            if (FD != 3 || close(FD) != 0) {
                failed.store(true);
            }
        } else if (errno != EMFILE) {
            failed.store(true);
        }
    }
    return nullptr;
}

auto fsize(rlim_t soft) -> int {
    rlimit const LIMIT{soft, RLIM_INFINITY};
    return setrlimit(RLIMIT_FSIZE, &LIMIT);
}

struct AppendRace {
    int fd = -1;
    bool failed = false;
};

void* append_race(void* opaque) {
    auto& state = *static_cast<AppendRace*>(opaque);
    constexpr char DATA[] = "abcdefg";
    for (unsigned i = 0; i < 32; ++i) {
        ssize_t const RET = write(state.fd, DATA, sizeof(DATA) - 1);
        if (RET == -1 && errno == EFBIG) {
            return nullptr;
        }
        if (RET <= 0 || RET > static_cast<ssize_t>(sizeof(DATA) - 1)) {
            state.failed = true;
            return nullptr;
        }
    }
    state.failed = true;
    return nullptr;
}

void file_size_matrix(Checks& checks, const char* directory) {
    checks.expect(fsize(RLIM_INFINITY) == 0, "reset file size limit");
    std::array<char, 160> path{};
    std::snprintf(path.data(), path.size(), "%s/wos-resource-size-XXXXXX", directory);
    int const FD = mkstemp(path.data());
    checks.expect(FD >= 0, "create file size fixture");
    if (FD < 0) {
        return;
    }
    checks.expect(fsize(8) == 0, "set file size boundary");
    constexpr char DATA[] = "abcdefghijk";
    checks.expect(write(FD, DATA, 7) == 7, "file one below boundary");
    checks.expect(write(FD, DATA, 2) == 1, "file write stops at exact boundary");
    checks.expect(write(FD, DATA, 1) == -1 && errno == EFBIG, "file fully denied write");
    checks.expect(pwrite(FD, DATA, 2, 7) == 1, "positional partial write");
    checks.expect(pwrite(FD, DATA, 1, 8) == -1 && errno == EFBIG, "positional denial");
    checks.expect(ftruncate(FD, 9) == -1 && errno == EFBIG, "truncate denial");
    struct stat st{};
    checks.expect(fstat(FD, &st) == 0 && st.st_size == 8, "denial preserves exact file size");
    std::array<char, 8> contents{};
    checks.expect(pread(FD, contents.data(), contents.size(), 0) == 8 && std::memcmp(contents.data(), "abcdefga", 8) == 0,
                  "denial preserves committed bytes");
    checks.expect(fsize(0) == 0, "zero file size limit");
    checks.expect(pwrite(FD, DATA, 0, 8) == 0, "empty write needs no growth");
    checks.expect(pwrite(FD, DATA, 1, 0) == -1 && errno == EFBIG, "zero bound disallows nonempty write");
    checks.expect(ftruncate(FD, 0) == 0, "zero truncate allowed");
    checks.expect(fsize(32) == 0, "append race boundary");
    std::array<AppendRace, 4> state{};
    std::array<pthread_t, 4> threads{};
    unsigned started = 0;
    for (unsigned i = 0; i < state.size(); ++i) {
        state[i].fd = open(path.data(), O_WRONLY | O_APPEND);
        if (state[i].fd < 0 || pthread_create(&threads[i], nullptr, append_race, &state[i]) != 0) {
            if (state[i].fd >= 0) {
                close(state[i].fd);
            }
            checks.expect(false, "append worker creation");
            break;
        }
        ++started;
    }
    for (unsigned i = 0; i < started; ++i) {
        checks.expect(pthread_join(threads[i], nullptr) == 0 && !state[i].failed, "append worker limit enforcement");
        close(state[i].fd);
    }
    checks.expect(fstat(FD, &st) == 0 && st.st_size == 32, "concurrent append never exceeds limit");
    checks.expect(fsize(RLIM_INFINITY) == 0, "restore infinite file size");
    checks.expect(pwrite(FD, DATA, 1, 32) == 1, "infinite limit permits growth again");
    checks.expect(close(FD) == 0 && unlink(path.data()) == 0, "remove file size fixture");
}

struct ProcReader {
    int fd = -1;
    ssize_t count = -1;
    std::array<char, 4096> text{};
};

void* proc_limits_reader(void* opaque) {
    auto& reader = *static_cast<ProcReader*>(opaque);
    reader.count = pread(reader.fd, reader.text.data(), reader.text.size() - 1, 0);
    return nullptr;
}

void proc_limits_matrix(Checks& checks) {
    int const FD = open("/proc/self/limits", O_RDONLY);
    checks.expect(FD >= 0, "open resource observations");
    if (FD < 0) {
        return;
    }
    std::array<ProcReader, 4> readers{};
    std::array<pthread_t, 4> workers{};
    unsigned started = 0;
    for (unsigned i = 0; i < readers.size(); ++i) {
        readers[i].fd = FD;
        if (pthread_create(&workers[i], nullptr, proc_limits_reader, &readers[i]) != 0) {
            checks.expect(false, "proc observation reader creation");
            break;
        }
        ++started;
    }
    for (unsigned i = 0; i < started; ++i) {
        checks.expect(pthread_join(workers[i], nullptr) == 0 && readers[i].count > 0, "concurrent initial proc snapshot read");
        if (i != 0) {
            checks.expect(readers[i].count == readers[0].count && std::strcmp(readers[i].text.data(), readers[0].text.data()) == 0,
                          "one coherent cached proc snapshot");
        }
    }
    std::array<char, 4096> text{};
    ssize_t const READ = read(FD, text.data(), text.size() - 1);
    checks.expect(READ > 0 && std::strstr(text.data(), "Resource Soft Hard Current Peak Denied Reason Units Enforcement") != nullptr,
                  "resource observation schema");
    auto* row = std::strstr(text.data(), "NOFILE ");
    unsigned long long soft = 0, hard = 0, current = 0, peak = 0, denied = 0;
    std::array<char, 32> reason{};
    bool const PARSED = row != nullptr && std::sscanf(row, "NOFILE %llu %llu %llu %llu %llu %31s", &soft, &hard, &current, &peak, &denied,
                                                      reason.data()) == 6;
    checks.expect(PARSED && soft == 256 && hard == 256 && current == 4 && peak >= current && denied > 0 &&
                      std::strcmp(reason.data(), "soft_limit") == 0,
                  "resource configured current peak and denied observations");
    checks.expect(std::strstr(text.data(), "FSIZE infinity infinity ") != nullptr, "file size infinity observations");
    // An inherited descriptor with cached contents must not bypass read-time access.
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        bool const DROPPED = setuid(1234) == 0;
        bool const DENIED = lseek(FD, 0, SEEK_SET) == 0 && read(FD, text.data(), text.size()) == -1 && errno == EACCES;
        int const OWN = open("/proc/self/limits", O_RDONLY);
        bool const SELF = OWN >= 0 && read(OWN, text.data(), text.size()) > 0;
        if (OWN >= 0) {
            close(OWN);
        }
        _exit(DROPPED && DENIED && SELF ? 0 : 1);
    }
    checks.expect(wait_ok(CHILD), "cached foreign observations denied after UID drop while self remains readable");
    checks.expect(close(FD) == 0, "close resource observations");
}

auto find_probe_core(std::array<char, 256>& path, off_t& size) -> bool {
    auto* directory = opendir("/tmp");
    if (directory == nullptr) {
        return false;
    }
    bool found = false;
    while (auto* entry = readdir(directory)) {
        if (std::strncmp(entry->d_name, "wos_resource_core_probe_", 24) != 0 || std::strstr(entry->d_name, "_coredump.bin") == nullptr) {
            continue;
        }
        std::snprintf(path.data(), path.size(), "/tmp/%s", entry->d_name);
        struct stat st{};
        found = stat(path.data(), &st) == 0;
        size = found ? st.st_size : 0;
        break;
    }
    closedir(directory);
    return found;
}

auto crash_core_probe(rlim_t core, rlim_t file_size) -> bool {
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        rlimit const LIMIT{core, RLIM_INFINITY};
        if (setrlimit(RLIMIT_CORE, &LIMIT) != 0 || fsize(file_size) != 0) {
            _exit(1);
        }
        execl("/usr/libexec/wos-resource-core-probe", "wos-resource-core-probe", static_cast<char*>(nullptr));
        _exit(2);
    }
    if (CHILD < 0) {
        return false;
    }
    int status = 0;
    pid_t waited;
    do {
        waited = waitpid(CHILD, &status, 0);
    } while (waited < 0 && errno == EINTR);
    return waited == CHILD && WIFSIGNALED(status) && WTERMSIG(status) == SIGILL;
}

void core_size_matrix(Checks& checks) {
    std::array<char, 256> path{};
    off_t size = 0;
    bool const CLEAN = !find_probe_core(path, size);
    checks.expect(CLEAN, "no preexisting core fixture");
    if (!CLEAN) {
        return;
    }
    for (auto const& limits : std::array<std::array<rlim_t, 2>, 3>{{{0, RLIM_INFINITY}, {1, RLIM_INFINITY}, {RLIM_INFINITY, 1}}}) {
        checks.expect(crash_core_probe(limits[0], limits[1]), "bounded core fixture exits by SIGILL");
        bool found = false;
        for (unsigned attempt = 0; attempt < 100; ++attempt) {
            if (find_probe_core(path, size)) {
                found = true;
                break;
            }
            usleep(10000);
        }
        checks.expect(!found, "denied core leaves no partial output file");
        if (found) {
            return;  // Preserve unexpected output for diagnosis.
        }
    }
    checks.expect(crash_core_probe(RLIM_INFINITY, RLIM_INFINITY), "unlimited core fixture exits by SIGILL");
    bool complete = false;
    off_t previous = -1;
    unsigned stable = 0;
    for (unsigned attempt = 0; attempt < 200; ++attempt) {
        if (find_probe_core(path, size) && size > 0) {
            stable = size == previous ? stable + 1 : 0;
            previous = size;
            if (stable == 5) {
                complete = true;
                break;
            }
        }
        usleep(10000);
    }
    checks.expect(complete, "unlimited core produces stable output");
    if (!complete) {
        return;
    }
    int const FD = open(path.data(), O_RDONLY);
    std::array<std::uint64_t, 2> prefix{};
    checks.expect(FD >= 0 && read(FD, prefix.data(), sizeof(prefix)) == sizeof(prefix) && prefix[0] == UINT64_C(0x504d55444f43534f) &&
                      static_cast<std::uint32_t>(prefix[1]) == 4,
                  "core file keeps binary magic and version");
    if (FD >= 0) {
        close(FD);
    }
    std::printf("resource-limits: core fixture %s size=%lld\n", path.data(), static_cast<long long>(size));
    // Keep this one owned artifact for WOSDBG integrity inspection. Re-running
    // the core matrix requires removing this exact reported fixture first.
}

void stack_matrix(Checks& checks) {
    constexpr rlim_t BOUND = 64 * 1024;
    for (bool deny : {false, true}) {
        pid_t const CHILD = fork();
        if (CHILD == 0) {
            rlimit const STACK{BOUND, RLIM_INFINITY};
            rlimit const CORE{0, RLIM_INFINITY};
            if (setrlimit(RLIMIT_STACK, &STACK) != 0 || setrlimit(RLIMIT_CORE, &CORE) != 0) {
                _exit(10);
            }
            execl("/usr/libexec/wos-resource-stack-probe", "wos-resource-stack-probe", deny ? "deny" : "grow", static_cast<char*>(nullptr));
            _exit(11);
        }
        int status = 0;
        pid_t waited;
        do {
            waited = waitpid(CHILD, &status, 0);
        } while (waited < 0 && errno == EINTR);
        bool const EXPECTED = deny ? WIFSIGNALED(status) && WTERMSIG(status) == SIGSEGV : WIFEXITED(status) && WEXITSTATUS(status) == 0;
        checks.expect(CHILD > 0 && waited == CHILD && EXPECTED,
                      deny ? "stack byte beyond boundary is SIGSEGV" : "stack exact boundary, lower, raise and accounting");
    }
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        rlimit const STACK{0, RLIM_INFINITY};
        if (setrlimit(RLIMIT_STACK, &STACK) != 0) {
            _exit(1);
        }
        execl("/usr/libexec/wos-resource-stack-probe", "wos-resource-stack-probe", static_cast<char*>(nullptr));
        _exit(errno == ENOMEM ? 0 : 2);
    }
    checks.expect(wait_ok(CHILD), "zero stack rejects exec and preserves old image");
    pid_t const ARGS_CHILD = fork();
    if (ARGS_CHILD == 0) {
        auto* argument = static_cast<char*>(std::malloc(BOUND + 1));
        if (argument == nullptr) {
            _exit(1);
        }
        std::memset(argument, 'x', BOUND);
        argument[BOUND] = 0;
        rlimit const STACK{BOUND, RLIM_INFINITY};
        if (setrlimit(RLIMIT_STACK, &STACK) != 0) {
            _exit(2);
        }
        execl("/usr/libexec/wos-resource-stack-probe", "wos-resource-stack-probe", argument, static_cast<char*>(nullptr));
        bool const DENIED = errno == E2BIG;
        std::free(argument);
        _exit(DENIED ? 0 : 3);
    }
    checks.expect(wait_ok(ARGS_CHILD), "stack-bounded argv rejects exec and preserves old image");
}

auto resource_usage(ker::abi::resource::Id id, ker::abi::resource::Usage& usage) -> bool {
    auto result = static_cast<uint64_t>(ker::abi::callnums::PROCESS);
    register uint64_t version asm("r8") = ker::abi::resource::VERSION;
    asm volatile("syscall"
                 : "+a"(result)
                 : "D"(static_cast<uint64_t>(ker::abi::process::procmgmt_ops::RESOURCE_USAGE)), "S"(static_cast<uint64_t>(id)), "d"(&usage),
                   "r"(version)
                 : "rcx", "r11", "memory");
    return result == 0;
}

auto cpu_limit(rlim_t soft, rlim_t hard) -> int {
    rlimit const LIMIT{soft, hard};
    return setrlimit(RLIMIT_CPU, &LIMIT);
}

[[noreturn]] void consume_cpu_forever() {
    for (;;) {
        asm volatile("nop" ::: "memory");
    }
}

void* cpu_worker(void*) { consume_cpu_forever(); }

volatile sig_atomic_t cpu_signals = 0;
void cpu_looping_signal_handler(int) { consume_cpu_forever(); }

void cpu_signal_handler(int) {
    cpu_signals = cpu_signals + 1;
    if (cpu_signals == 2) {
        _exit(0);
    }
}

auto consume_cpu_second() -> bool {
    for (;;) {
        ker::abi::resource::Usage usage{};
        if (!resource_usage(ker::abi::resource::Id::CPU, usage)) {
            return false;
        }
        if (usage.current >= 1) {
            return true;
        }
        for (unsigned i = 0; i < 100000; ++i) {
            asm volatile("nop" ::: "memory");
        }
    }
}

void cpu_matrix(Checks& checks) {
    struct Case {
        rlim_t soft;
        rlim_t hard;
        bool ignore_soft;
        bool threads;
        int expected;
        const char* name;
    };
    constexpr std::array<Case, 5> CASES{{
        {0, RLIM_INFINITY, false, false, SIGXCPU, "zero CPU soft limit signals"},
        {0, 0, true, false, SIGKILL, "zero CPU hard limit kills"},
        {1, 2, false, false, SIGXCPU, "finite CPU soft limit signals"},
        {1, 2, true, false, SIGKILL, "ignored CPU soft signal cannot evade hard limit"},
        {1, 1, true, true, SIGKILL, "threads share one CPU hard budget"},
    }};
    for (auto const& entry : CASES) {
        pid_t const CHILD = fork();
        if (CHILD == 0) {
            alarm(15);
            if (entry.ignore_soft) {
                signal(SIGXCPU, SIG_IGN);
            }
            if (cpu_limit(entry.soft, entry.hard) != 0) {
                _exit(1);
            }
            if (entry.threads) {
                for (unsigned i = 0; i < 4; ++i) {
                    pthread_t worker{};
                    if (pthread_create(&worker, nullptr, cpu_worker, nullptr) != 0) {
                        _exit(2);
                    }
                }
            }
            consume_cpu_forever();
        }
        int status = 0;
        pid_t waited;
        do {
            waited = waitpid(CHILD, &status, 0);
        } while (waited < 0 && errno == EINTR);
        if (!(CHILD > 0 && waited == CHILD && WIFSIGNALED(status) && WTERMSIG(status) == entry.expected)) {
            std::fprintf(stderr, "resource-limits: CPU child=%ld wait=%ld status=0x%x expected=%d\n", CHILD, waited, status,
                         entry.expected);
        }
        checks.expect(CHILD > 0 && waited == CHILD && WIFSIGNALED(status) && WTERMSIG(status) == entry.expected, entry.name);
    }
    pid_t child = fork();
    if (child == 0) {
        alarm(15);
        cpu_signals = 0;
        signal(SIGXCPU, cpu_signal_handler);
        if (cpu_limit(0, 3) != 0) {
            _exit(1);
        }
        consume_cpu_forever();
    }
    checks.expect(wait_ok(child), "caught CPU soft signal repeats with further consumption");
    child = fork();
    if (child == 0) {
        alarm(15);
        signal(SIGXCPU, cpu_looping_signal_handler);
        if (cpu_limit(0, 1) != 0) {
            _exit(1);
        }
        consume_cpu_forever();
    }
    int looping_status = 0;
    pid_t looping_wait;
    do {
        looping_wait = waitpid(child, &looping_status, 0);
    } while (looping_wait < 0 && errno == EINTR);
    checks.expect(child > 0 && looping_wait == child && WIFSIGNALED(looping_status) && WTERMSIG(looping_status) == SIGKILL,
                  "CPU hard limit terminates looping soft handler");
    child = fork();
    if (child == 0) {
        alarm(15);
        if (cpu_limit(3, 5) != 0 || !consume_cpu_second()) {
            _exit(1);
        }
        pid_t const GRANDCHILD = fork();
        if (GRANDCHILD == 0) {
            ker::abi::resource::Usage usage{};
            bool const VALID = resource_usage(ker::abi::resource::Id::CPU, usage) && usage.current == 0 && usage.limit.soft == 3 &&
                               usage.limit.hard == 5 && usage.denied == 0;
            _exit(VALID ? 0 : 2);
        }
        _exit(wait_ok(GRANDCHILD) ? 0 : 3);
    }
    checks.expect(wait_ok(child), "fork inherits CPU policy and resets consumption");
    child = fork();
    if (child == 0) {
        alarm(15);
        if (cpu_limit(3, 5) != 0 || !consume_cpu_second()) {
            _exit(1);
        }
        execl("/usr/bin/testprog", "testprog", "resource-limits", "cpu-exec", static_cast<char*>(nullptr));
        _exit(2);
    }
    checks.expect(wait_ok(child), "exec preserves CPU policy and consumption");
}

auto address_space_limit(rlim_t soft) -> int {
    rlimit const LIMIT{soft, RLIM_INFINITY};
    return setrlimit(RLIMIT_AS, &LIMIT);
}

auto address_space_usage(ker::abi::resource::Usage& usage) -> bool { return resource_usage(ker::abi::resource::Id::AS, usage); }

struct AddressSpaceRace {
    std::atomic<unsigned> ready{0};
    std::atomic<unsigned> done{0};
    std::atomic<unsigned> accepted{0};
    std::atomic<unsigned> errors{0};
    std::atomic<bool> start{false};
    std::atomic<bool> release{false};
};

void* address_space_worker(void* opaque) {
    auto& race = *static_cast<AddressSpaceRace*>(opaque);
    race.ready.fetch_add(1);
    while (!race.start.load()) asm volatile("pause");
    void* page = mmap(nullptr, 4096, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (page != MAP_FAILED)
        race.accepted.fetch_add(1);
    else if (errno != ENOMEM)
        race.errors.fetch_add(1);
    race.done.fetch_add(1);
    while (!race.release.load()) asm volatile("pause");
    if (page != MAP_FAILED && munmap(page, 4096) != 0) race.errors.fetch_add(1);
    return nullptr;
}

void address_space_matrix(Checks& checks) {
    constexpr size_t PAGE = 4096;
    ker::abi::resource::Usage before{}, after{};
    checks.expect(address_space_usage(before) && before.current >= 64 * 1024 * 1024, "AS includes automatic stack reservation");
    auto* victim = static_cast<unsigned char*>(mmap(nullptr, 2 * PAGE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    checks.expect(victim != MAP_FAILED, "AS prepare replacement fixture");
    if (victim == MAP_FAILED) return;
    checks.expect(munmap(victim + PAGE, PAGE) == 0, "AS prepare adjacent hole");
    victim[0] = 0x5a;
    checks.expect(address_space_usage(before) && address_space_limit(before.current + PAGE) == 0, "AS one-page remaining budget");
    void* reservation = mmap(nullptr, PAGE, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    checks.expect(reservation != MAP_FAILED, "AS lazy reservation exact boundary");
    checks.expect(address_space_usage(after) && after.current == before.current + PAGE, "AS charges nonresident reservation");
    checks.expect(mmap(nullptr, PAGE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0) == MAP_FAILED && errno == ENOMEM,
                  "AS denies one page above boundary");
    checks.expect(mmap(victim, 2 * PAGE, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_PRIVATE | MAP_ANONYMOUS, -1, 0) == MAP_FAILED &&
                      errno == ENOMEM && victim[0] == 0x5a,
                  "AS denied MAP_FIXED preserves old bytes");
    checks.expect(mremap(victim, PAGE, 2 * PAGE, MREMAP_MAYMOVE) == MAP_FAILED && errno == ENOMEM && victim[0] == 0x5a,
                  "AS denied mremap preserves source");
    checks.expect(address_space_usage(after) && after.current == before.current + PAGE && after.denied >= before.denied + 3,
                  "AS denials leave no partial charge");
    checks.expect(address_space_limit(0) == 0, "AS lower below existing use");
    checks.expect(mmap(victim, PAGE, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_PRIVATE | MAP_ANONYMOUS, -1, 0) == victim,
                  "AS allows replacement without growth after lowering");
    if (reservation != MAP_FAILED) {
        bool const PROTECTED = mprotect(reservation, PAGE, PROT_READ | PROT_WRITE) == 0;
        checks.expect(PROTECTED, "AS protection change adds no address space");
        if (PROTECTED) *static_cast<volatile unsigned char*>(reservation) = 0x4b;
        checks.expect(address_space_usage(after) && after.current == before.current + PAGE, "AS fault backing does not charge twice");
        checks.expect(munmap(reservation, PAGE) == 0, "AS unmap releases reservation");
    }
    checks.expect(munmap(victim, PAGE) == 0 && address_space_usage(after) && after.current == before.current - PAGE,
                  "AS release returns to baseline");
    checks.expect(address_space_limit(RLIM_INFINITY) == 0, "AS restore policy");

    int const FD = open("/tmp/wos_resource_as_file", O_CREAT | O_TRUNC | O_RDWR, 0600);
    checks.expect(FD >= 0 && ftruncate(FD, PAGE) == 0, "AS prepare file mapping");
    if (FD >= 0) {
        checks.expect(address_space_usage(before) && address_space_limit(before.current) == 0, "AS full budget for file mapping");
        checks.expect(mmap(nullptr, PAGE, PROT_READ, MAP_PRIVATE, FD, 0) == MAP_FAILED && errno == ENOMEM,
                      "AS denies file mapping before publication");
        checks.expect(address_space_limit(before.current + PAGE) == 0, "AS permit one file page");
        void* file_page = mmap(nullptr, PAGE, PROT_READ, MAP_PRIVATE, FD, 0);
        checks.expect(file_page != MAP_FAILED && address_space_usage(after) && after.current == before.current + PAGE,
                      "AS lazy file page is charged before reading");
        if (file_page != MAP_FAILED) munmap(file_page, PAGE);
        address_space_limit(RLIM_INFINITY);
        close(FD);
        unlink("/tmp/wos_resource_as_file");
    }
    int const SHM = shmget(IPC_PRIVATE, PAGE, IPC_CREAT | 0600);
    checks.expect(SHM >= 0, "AS prepare shared-memory segment");
    if (SHM >= 0) {
        checks.expect(address_space_usage(before) && address_space_limit(before.current) == 0, "AS full budget for shmat");
        checks.expect(shmat(SHM, nullptr, 0) == reinterpret_cast<void*>(-1) && errno == ENOMEM, "AS denies shared-memory attachment");
        shmid_ds state{};
        checks.expect(
            shmctl(SHM, IPC_STAT, &state) == 0 && state.shm_nattch == 0 && address_space_usage(after) && after.current == before.current,
            "AS denied shmat releases attachment pin");
        address_space_limit(before.current + PAGE);
        void* attached = shmat(SHM, nullptr, 0);
        checks.expect(attached != reinterpret_cast<void*>(-1) && address_space_usage(after) && after.current == before.current + PAGE,
                      "AS shared attachment exact boundary");
        if (attached != reinterpret_cast<void*>(-1)) shmdt(attached);
        checks.expect(address_space_usage(after) && after.current == before.current, "AS shmdt returns charge");
        address_space_limit(RLIM_INFINITY);
        shmctl(SHM, IPC_RMID, nullptr);
    }

    AddressSpaceRace race;
    std::array<pthread_t, 8> threads{};
    unsigned created = 0;
    for (auto& thread : threads) {
        if (pthread_create(&thread, nullptr, address_space_worker, &race) != 0) break;
        ++created;
    }
    while (race.ready.load() != created) asm volatile("pause");
    checks.expect(created == threads.size(), "AS prepare concurrent mapping workers");
    checks.expect(address_space_usage(before) && address_space_limit(before.current + PAGE) == 0, "AS one shared page for racing threads");
    race.start.store(true);
    while (race.done.load() != created) asm volatile("pause");
    checks.expect(
        race.accepted.load() == 1 && race.errors.load() == 0 && address_space_usage(after) && after.current == before.current + PAGE,
        "AS concurrent mappings cannot oversubscribe");
    address_space_limit(RLIM_INFINITY);
    race.release.store(true);
    for (unsigned i = 0; i < created; ++i) pthread_join(threads[i], nullptr);
    checks.expect(race.errors.load() == 0, "AS concurrent reservations unwind");

    for (rlim_t const LIMIT : {rlim_t{0}, rlim_t{64 * 1024 * 1024 + 84 * 1024}}) {
        pid_t const CHILD = fork();
        if (CHILD == 0) {
            ker::abi::resource::Usage old{}, current{};
            if (!address_space_usage(old) || address_space_limit(LIMIT) != 0) _exit(2);
            execl("/usr/libexec/wos-resource-exec-probe", "as-denied", static_cast<char*>(nullptr));
            int const ERROR = errno;
            _exit(ERROR == ENOMEM && address_space_usage(current) && current.current == old.current && current.denied > old.denied ? 0 : 3);
        }
        checks.expect(wait_ok(CHILD), LIMIT == 0 ? "AS zero rejects exec without replacing old image"
                                                 : "AS rejects ELF after stack admission without leaking staged VA");
    }
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        if (address_space_limit(65 * 1024 * 1024) != 0 || nofile(0) != 0) _exit(2);
        execl("/usr/libexec/wos-resource-exec-probe", "as-finite", static_cast<char*>(nullptr));
        _exit(3);
    }
    checks.expect(wait_ok(CHILD), "AS finite replacement image succeeds");
}

auto nproc(rlim_t soft, rlim_t hard = RLIM_INFINITY) -> int {
    rlimit const LIMIT{soft, hard};
    return setrlimit(RLIMIT_NPROC, &LIMIT);
}

auto population_is(uint64_t expected) -> bool {
    for (unsigned i = 0; i < 500; ++i) {
        ker::abi::resource::Usage usage{};
        if (resource_usage(ker::abi::resource::Id::NPROC, usage) && usage.current == expected) return true;
        usleep(1000);
    }
    return false;
}

struct ProcessRace {
    std::atomic<unsigned> ready{0}, done{0}, accepted{0}, errors{0};
    std::atomic<bool> start{false}, release{false};
    int read_fd = -1;
};

void* process_race_worker(void* opaque) {
    auto& race = *static_cast<ProcessRace*>(opaque);
    race.ready.fetch_add(1);
    while (!race.start.load()) usleep(1000);
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        char byte = 0;
        _exit(read(race.read_fd, &byte, 1) == 1 ? 0 : 2);
    }
    if (CHILD > 0)
        race.accepted.fetch_add(1);
    else if (errno != EAGAIN)
        race.errors.fetch_add(1);
    race.done.fetch_add(1);
    while (!race.release.load()) usleep(1000);
    if (CHILD > 0 && !wait_ok(CHILD)) race.errors.fetch_add(1);
    return nullptr;
}

void* unused_process_worker(void*) { return nullptr; }
void unexpected_clone_entry() { _exit(3); }

void process_count_matrix(Checks& checks) {
    // /tmp is root-owned on WOS. Prepare the path before dropping privilege,
    // retaining a writable fixture for the denied spawn's truncation action.
    std::array<char, 96> path{};
    std::snprintf(path.data(), path.size(), "/tmp/wos_resource_nproc_%ld", static_cast<long>(getpid()));
    int fd = open(path.data(), O_CREAT | O_EXCL | O_RDWR, 0600);
    uid_t const TEST_UID = static_cast<uid_t>(50000 + getpid());
    checks.expect(fd >= 0 && write(fd, "keep", 4) == 4 && chown(path.data(), TEST_UID, getgid()) == 0,
                  "NPROC prepare spawn side-effect fixture");
    // Isolate the UID population from init, ssh and other command sessions.
    checks.expect(nproc(0) == 0 && setuid(TEST_UID) == -1 && errno == EAGAIN && getuid() == 0 && geteuid() == 0,
                  "NPROC denied UID transfer retains credentials");
    checks.expect(nproc(RLIM_INFINITY) == 0 && setuid(TEST_UID) == 0, "NPROC isolate real UID");
    checks.expect(population_is(1), "NPROC initial live population");
    checks.expect(nproc(0) == 0, "NPROC zero policy");
    pid_t child = fork();
    if (child == 0) _exit(2);
    checks.expect(child == -1 && errno == EAGAIN, "NPROC denied fork returns EAGAIN");
    if (child > 0) wait_ok(child);

    pthread_t denied_thread{};
    int created = pthread_create(&denied_thread, nullptr, unused_process_worker, nullptr);
    checks.expect(created == EAGAIN, "NPROC denied pthread returns EAGAIN");
    if (created == 0) pthread_join(denied_thread, nullptr);
    ker::abi::resource::Usage before{}, after{};
    checks.expect(address_space_usage(before), "NPROC snapshot after allocator warmup");
    bool all_denied = true;
    for (unsigned i = 0; i < 8; ++i) {
        created = pthread_create(&denied_thread, nullptr, unused_process_worker, nullptr);
        all_denied = created == EAGAIN && all_denied;
        if (created == 0) pthread_join(denied_thread, nullptr);
    }
    checks.expect(all_denied && population_is(1), "NPROC repeated thread denial leaves no population charge");
    checks.expect(address_space_usage(after) && after.current == before.current, "NPROC failed pthread releases stack and TCB backing");

    constexpr size_t EXPLICIT_STACK_SIZE = 64 * 1024;
    auto* explicit_stack =
        static_cast<unsigned char*>(mmap(nullptr, EXPLICIT_STACK_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    checks.expect(explicit_stack != MAP_FAILED, "NPROC prepare caller-owned stack");
    if (explicit_stack != MAP_FAILED) {
        explicit_stack[0] = 0x5a;
        pthread_attr_t attributes;
        pthread_attr_init(&attributes);
        pthread_attr_setstack(&attributes, explicit_stack, EXPLICIT_STACK_SIZE);
        created = pthread_create(&denied_thread, &attributes, unused_process_worker, nullptr);
        pthread_attr_destroy(&attributes);
        checks.expect(created == EAGAIN && explicit_stack[0] == 0x5a, "NPROC denial retains caller-owned pthread stack");
        if (created == 0) pthread_join(denied_thread, nullptr);
        int parent_tid = -27, child_tid = -29;
        // WOS CLONE_VM_PROC's seven fixed uint64_t fields, matching its public
        // libc wrapper without mixing the legacy and kernel callnum enums.
        std::array<uint64_t, 7> args{reinterpret_cast<uint64_t>(unexpected_clone_entry),
                                     reinterpret_cast<uint64_t>(explicit_stack + EXPLICIT_STACK_SIZE - 8),
                                     0x100 | SIGCHLD,
                                     0,
                                     reinterpret_cast<uint64_t>(&parent_tid),
                                     0,
                                     reinterpret_cast<uint64_t>(&child_tid)};
        uint64_t raw = static_cast<uint64_t>(ker::abi::callnums::PROCESS);
        asm volatile("syscall"
                     : "+a"(raw)
                     : "D"(static_cast<uint64_t>(ker::abi::process::procmgmt_ops::CLONE_VM_PROC)), "S"(args.data())
                     : "rcx", "r11", "memory");
        auto const CLONE_RESULT = static_cast<int64_t>(raw);
        checks.expect(CLONE_RESULT == -EAGAIN && parent_tid == -27 && child_tid == -29,
                      "NPROC denied CLONE_VM leaves both TID outputs unchanged");
        if (CLONE_RESULT > 0) wait_ok(static_cast<pid_t>(CLONE_RESULT));
        munmap(explicit_stack, EXPLICIT_STACK_SIZE);
    }

    // File actions must not truncate anything when spawn admission fails.
    if (fd >= 0) {
        posix_spawn_file_actions_t actions;
        posix_spawn_file_actions_init(&actions);
        posix_spawn_file_actions_addopen(&actions, 5, path.data(), O_WRONLY | O_TRUNC, 0600);
        char* args[] = {const_cast<char*>("testprog"), const_cast<char*>("resource-limits"), const_cast<char*>("nproc-exec"), nullptr};
        int const RESULT = posix_spawn(&child, "/usr/bin/testprog", &actions, nullptr, args, environ);
        posix_spawn_file_actions_destroy(&actions);
        checks.expect(RESULT == EAGAIN, "NPROC rejects spawn before file actions");
        if (RESULT == 0) wait_ok(child);
        posix_spawnattr_t spawn_attributes;
        posix_spawnattr_init(&spawn_attributes);
        posix_spawnattr_setflags(&spawn_attributes, POSIX_SPAWN_SETSIGDEF);
        int const FALLBACK_RESULT = posix_spawn(&child, "/usr/bin/testprog", nullptr, &spawn_attributes, args, environ);
        posix_spawnattr_destroy(&spawn_attributes);
        checks.expect(FALLBACK_RESULT == EAGAIN, "NPROC fallback spawn preserves fork errno");
        if (FALLBACK_RESULT == 0) wait_ok(child);
        char contents[4]{};
        checks.expect(pread(fd, contents, sizeof(contents), 0) == 4 && std::memcmp(contents, "keep", 4) == 0,
                      "NPROC denied spawn preserves file content");
        close(fd);
        unlink(path.data());
    }

    int gate[2]{};
    checks.expect(pipe(gate) == 0 && nproc(2) == 0, "NPROC one additional process budget");
    child = fork();
    if (child == 0) {
        char byte = 0;
        _exit(read(gate[0], &byte, 1) == 1 ? 0 : 2);
    }
    checks.expect(child > 0 && population_is(2), "NPROC exact process boundary");
    pid_t excess = fork();
    if (excess == 0) _exit(2);
    checks.expect(excess == -1 && errno == EAGAIN, "NPROC one above process boundary");
    if (excess > 0) wait_ok(excess);
    checks.expect(nproc(0) == 0, "NPROC lower below current population");
    if (child > 0) {
        write(gate[1], "x", 1);
        checks.expect(wait_ok(child) && population_is(1), "NPROC exit returns charge after lowering");
    }
    close(gate[0]);
    close(gate[1]);

    checks.expect(nproc(RLIM_INFINITY) == 0 && pipe(gate) == 0, "NPROC prepare racing consumers");
    ProcessRace race;
    race.read_fd = gate[0];
    std::array<pthread_t, 8> threads{};
    unsigned workers = 0;
    for (auto& thread : threads) {
        if (pthread_create(&thread, nullptr, process_race_worker, &race) != 0) break;
        ++workers;
    }
    while (race.ready.load() != workers) usleep(1000);
    checks.expect(workers == threads.size() && population_is(workers + 1), "NPROC threads share UID population");
    checks.expect(nproc(workers + 2) == 0, "NPROC one process slot for concurrent forks");
    race.start.store(true);
    while (race.done.load() != workers) usleep(1000);
    checks.expect(race.accepted.load() == 1 && race.errors.load() == 0 && population_is(workers + 2),
                  "NPROC concurrent creators cannot oversubscribe");
    nproc(RLIM_INFINITY);
    for (unsigned i = 0; i < race.accepted.load(); ++i) write(gate[1], "x", 1);
    race.release.store(true);
    for (unsigned i = 0; i < workers; ++i) pthread_join(threads[i], nullptr);
    checks.expect(race.errors.load() == 0 && population_is(1), "NPROC process and thread exit release exactly once");
    close(gate[0]);
    close(gate[1]);

    child = fork();
    if (child == 0) {
        if (nproc(0) != 0) _exit(2);
        execl("/usr/bin/testprog", "testprog", "resource-limits", "nproc-exec", static_cast<char*>(nullptr));
        _exit(3);
    }
    checks.expect(wait_ok(child), "NPROC exec preserves zero policy without a new charge");
    checks.expect(nproc(1, 1) == 0 && nproc(2, 2) == -1 && errno == EPERM, "NPROC unprivileged hard raise denied");
    checks.expect(resource_usage(ker::abi::resource::Id::NPROC, after) && after.denied >= 12 && after.current == 1 && after.peak >= 10,
                  "NPROC reports current peak and denials");
}

auto process_count_run() -> int {
    Checks checks;
    process_count_matrix(checks);
    std::printf("resource-limits NPROC: %u checks, %u failed\n", checks.total, checks.failed);
    std::fflush(stdout);
    return checks.failed == 0 ? 0 : 1;
}

auto service_policy_run() -> int {
    Checks checks;
    constexpr std::array<int, 7> IDS{RLIMIT_CPU, RLIMIT_FSIZE, RLIMIT_STACK, RLIMIT_CORE, RLIMIT_NPROC, RLIMIT_NOFILE, RLIMIT_AS};
    constexpr std::array<rlim_t, 7> SOFT{3, 4, 512 * 1024, 0, 64, 8, 128 * 1024 * 1024};
    constexpr std::array<rlim_t, 7> HARD{4, 4, 512 * 1024, 0, 64, 8, 128 * 1024 * 1024};
    for (size_t i = 0; i < IDS.size(); ++i) {
        rlimit limit{};
        checks.expect(getrlimit(IDS[i], &limit) == 0 && limit.rlim_cur == SOFT[i] && limit.rlim_max == HARD[i],
                      "service applies configured limit before exec");
    }
    for (auto const op :
         {ker::abi::process::procmgmt_ops::SERVICE_RESOURCE_CONFIGURE, ker::abi::process::procmgmt_ops::SERVICE_RESOURCE_SELECT}) {
        uint64_t result = static_cast<uint64_t>(ker::abi::callnums::PROCESS);
        asm volatile("syscall" : "+a"(result) : "D"(static_cast<uint64_t>(op)), "S"(uint64_t{0}) : "rcx", "r11", "memory");
        checks.expect(static_cast<int64_t>(result) == -EPERM, "service cannot configure or select PID1 budgets");
    }
    std::array<int, 16> descriptors{};
    unsigned opened = 0;
    for (; opened < descriptors.size(); ++opened) {
        descriptors[opened] = open("/dev/null", O_RDONLY);
        if (descriptors[opened] < 0) break;
    }
    ker::abi::resource::Usage usage{};
    checks.expect(opened < descriptors.size() && errno == EMFILE && resource_usage(ker::abi::resource::Id::NOFILE, usage) &&
                      usage.current == 8 && usage.denied > 0,
                  "service NOFILE admission and telemetry");
    for (unsigned i = 0; i < opened; ++i) close(descriptors[i]);
    signal(SIGXFSZ, SIG_IGN);
    std::array<char, 80> path{};
    std::snprintf(path.data(), path.size(), "/tmp/wos_service_budget_%ld", static_cast<long>(getpid()));
    int const FD = open(path.data(), O_CREAT | O_TRUNC | O_RDWR, 0600);
    checks.expect(FD >= 0, "service file-growth fixture");
    if (FD >= 0) {
        checks.expect(write(FD, "12345678", 8) == 4 && write(FD, "x", 1) == -1 && errno == EFBIG, "service FSIZE clips then denies growth");
        close(FD);
        unlink(path.data());
    }
    void* excess = mmap(nullptr, 1024 * 1024 * 1024, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    checks.expect(excess == MAP_FAILED && errno == ENOMEM, "service AS denies oversized reservation");
    if (excess != MAP_FAILED) munmap(excess, 1024 * 1024 * 1024);
    std::printf("resource-limits service-policy: %u checks, %u failed\n", checks.total, checks.failed);
    return checks.failed == 0 ? 0 : 1;
}

auto observed_clock_us(clockid_t id, uint64_t& value) -> bool {
    timespec time{};
    if (clock_gettime(id, &time) != 0) return false;
    value = static_cast<uint64_t>(time.tv_sec) * 1000000 + static_cast<uint64_t>(time.tv_nsec) / 1000;
    return true;
}

auto observed_rusage_us(int scope, uint64_t& value) -> bool {
    rusage usage{};
    if (getrusage(scope, &usage) != 0) return false;
    value = static_cast<uint64_t>(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * 1000000 +
            static_cast<uint64_t>(usage.ru_utime.tv_usec + usage.ru_stime.tv_usec);
    return true;
}

auto observed_proc_cpu_us(uint64_t& own, uint64_t& children) -> bool {
    auto* file = std::fopen("/proc/self/stat", "r");
    if (file == nullptr) return false;
    std::array<char, 4096> bytes{};
    bool const READ = std::fgets(bytes.data(), bytes.size(), file) != nullptr;
    std::fclose(file);
    if (!READ) return false;
    auto* cursor = std::strrchr(bytes.data(), ')');
    if (cursor == nullptr) return false;
    ++cursor;
    std::array<uint64_t, 4> ticks{};
    for (unsigned field = 3; field <= 17; ++field) {
        while (*cursor == ' ') ++cursor;
        if (*cursor == 0) return false;
        if (field >= 14) {
            char* end = nullptr;
            ticks[field - 14] = std::strtoull(cursor, &end, 10);
            if (end == cursor) return false;
        }
        while (*cursor != 0 && *cursor != ' ') ++cursor;
    }
    own = (ticks[0] + ticks[1]) * 10000;
    children = (ticks[2] + ticks[3]) * 10000;
    return true;
}

struct CpuObservationWorker {
    uint64_t used_us = 0;
    bool ok = false;
};

void* cpu_observation_worker(void* argument) {
    auto& result = *static_cast<CpuObservationWorker*>(argument);
    uint64_t start = 0;
    uint64_t wall_start = 0;
    if (!observed_clock_us(CLOCK_THREAD_CPUTIME_ID, start) || !observed_clock_us(CLOCK_MONOTONIC, wall_start)) return nullptr;
    for (;;) {
        for (unsigned i = 0; i < 100000; ++i) asm volatile("nop" ::: "memory");
        uint64_t now = 0;
        uint64_t wall = 0;
        if (!observed_clock_us(CLOCK_THREAD_CPUTIME_ID, now) || !observed_clock_us(CLOCK_MONOTONIC, wall)) return nullptr;
        if (wall - wall_start > 10000000) return nullptr;
        if (now - start < 80000) continue;
        result.used_us = now - start;
        uint64_t process = 0;
        uint64_t usage = 0;
        result.ok = observed_clock_us(CLOCK_PROCESS_CPUTIME_ID, process) && observed_rusage_us(RUSAGE_SELF, usage) && process >= now &&
                    usage + 20000 >= process;
        return nullptr;
    }
}

struct CpuExitWorker {
    CpuObservationWorker cpu{};
    std::atomic<unsigned>* ready = nullptr;
    bool linger = false;
};

void* cpu_exit_worker(void* argument) {
    auto& worker = *static_cast<CpuExitWorker*>(argument);
    cpu_observation_worker(&worker.cpu);
    worker.ready->fetch_add(1, std::memory_order_release);
    if (worker.linger) consume_cpu_forever();
    return nullptr;
}

auto cpu_wait_observation_run() -> int {
    Checks checks;
    for (bool linger : {false, true}) {
        uint64_t before = 0;
        checks.expect(observed_rusage_us(RUSAGE_CHILDREN, before), "initial wait CPU observation");
        pid_t const CHILD = fork();
        if (CHILD == 0) {
            std::atomic<unsigned> ready{0};
            std::array<CpuExitWorker, 3> workers{};
            std::array<pthread_t, 3> threads{};
            for (unsigned i = 0; i < workers.size(); ++i) {
                workers[i].ready = &ready;
                workers[i].linger = linger;
                if (pthread_create(&threads[i], nullptr, cpu_exit_worker, &workers[i]) != 0) _exit(2);
            }
            if (linger) {
                while (ready.load(std::memory_order_acquire) != workers.size()) usleep(1000);
            } else {
                for (auto thread : threads)
                    if (pthread_join(thread, nullptr) != 0) _exit(3);
            }
            for (const auto& worker : workers)
                if (!worker.cpu.ok) _exit(4);
            // Live workers deliberately remain runnable during group exit.
            _exit(0);
        }
        int status = 0;
        rusage waited_usage{};
        pid_t waited;
        do {
            waited = wait4(CHILD, &status, 0, &waited_usage);
        } while (waited < 0 && errno == EINTR);
        checks.expect(CHILD > 0 && waited == CHILD && WIFEXITED(status) && WEXITSTATUS(status) == 0,
                      linger ? "live-worker group exit completes" : "joined-worker child exit completes");
        uint64_t const WAIT_US = static_cast<uint64_t>(waited_usage.ru_utime.tv_sec + waited_usage.ru_stime.tv_sec) * 1000000 +
                                 static_cast<uint64_t>(waited_usage.ru_utime.tv_usec + waited_usage.ru_stime.tv_usec);
        checks.expect(WAIT_US >= 200000, linger ? "wait4 includes terminated live-worker CPU" : "wait4 includes joined-worker CPU");
        uint64_t after = 0;
        checks.expect(observed_rusage_us(RUSAGE_CHILDREN, after) && after >= before && after - before + 20000 >= WAIT_US &&
                          after - before <= WAIT_US + 20000,
                      "parent reaped CPU matches one wait4 event");
        uint64_t proc = 0;
        uint64_t children = 0;
        checks.expect(observed_proc_cpu_us(proc, children) && children == after, "procfs agrees after threaded child reap");
        std::printf("resource-limits CPU wait: linger=%d wait-us=%llu parent-delta=%llu\n", linger,
                    static_cast<unsigned long long>(WAIT_US), static_cast<unsigned long long>(after - before));
    }
    std::printf("resource-limits CPU wait: %u checks, %u failed\n", checks.total, checks.failed);
    return checks.failed == 0 ? 0 : 1;
}

auto cpu_observation_run() -> int {
    Checks checks;
    uint64_t before = 0;
    checks.expect(observed_clock_us(CLOCK_PROCESS_CPUTIME_ID, before), "initial process CPU clock");
    uint64_t consumed = 0;
    for (unsigned i = 0; i < 3; ++i) {
        CpuObservationWorker worker{};
        pthread_t thread{};
        int const CREATED = pthread_create(&thread, nullptr, cpu_observation_worker, &worker);
        checks.expect(CREATED == 0, "CPU observation worker creation");
        if (CREATED != 0) break;
        checks.expect(pthread_join(thread, nullptr) == 0 && worker.ok, "worker sees process CPU and rusage");
        consumed += worker.used_us;
    }
    // Let exited threads leave the active registry before observing history.
    usleep(200000);
    uint64_t after = 0;
    uint64_t usage = 0;
    uint64_t proc = 0;
    uint64_t proc_children = 0;
    checks.expect(consumed >= 240000 && observed_clock_us(CLOCK_PROCESS_CPUTIME_ID, after) && after >= before + consumed,
                  "process clock retains joined-thread CPU");
    checks.expect(observed_rusage_us(RUSAGE_SELF, usage) && usage + 20000 >= after, "getrusage retains joined-thread CPU");
    checks.expect(observed_proc_cpu_us(proc, proc_children) && proc + 20000 >= after, "procfs retains joined-thread CPU");
    uint64_t children_before = 0;
    checks.expect(observed_rusage_us(RUSAGE_CHILDREN, children_before), "initial reaped-child CPU");
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        CpuObservationWorker worker{};
        cpu_observation_worker(&worker);
        _exit(worker.ok ? 0 : 1);
    }
    checks.expect(wait_ok(CHILD), "CPU observation child completes");
    uint64_t children_after = 0;
    checks.expect(observed_rusage_us(RUSAGE_CHILDREN, children_after) && children_after >= children_before + 50000,
                  "getrusage retains reaped-child CPU");
    checks.expect(observed_proc_cpu_us(proc, proc_children) && proc_children == children_after, "procfs reports reaped-child CPU");
    std::printf("resource-limits CPU observation: %u checks, %u failed; workers=%llu process=%llu rusage=%llu proc=%llu children=%llu\n",
                checks.total, checks.failed, static_cast<unsigned long long>(consumed), static_cast<unsigned long long>(after),
                static_cast<unsigned long long>(usage), static_cast<unsigned long long>(proc),
                static_cast<unsigned long long>(proc_children));
    return checks.failed == 0 ? 0 : 1;
}

auto service_cpu_restart_run() -> int {
    // Each generation stays below one CPU second. Only the persistent service
    // counter can exhaust a two-second budget over these short restart runs.
    for (;;) {
        rusage usage{};
        if (getrusage(RUSAGE_SELF, &usage) != 0) return 2;
        uint64_t const USED = static_cast<uint64_t>(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * 1000000 +
                              static_cast<uint64_t>(usage.ru_utime.tv_usec + usage.ru_stime.tv_usec);
        if (USED >= 250000) {
            std::printf("resource-limits service-cpu-restart: pid=%ld cpu-us=%llu\n", static_cast<long>(getpid()),
                        static_cast<unsigned long long>(USED));
            return 1;
        }
        for (unsigned i = 0; i < 10000; ++i) asm volatile("nop" ::: "memory");
    }
}

auto matrix() -> int {
    Checks checks;
    // Every case runs in a child, retaining only the command's standard streams.
    for (int fd = 3; fd < 256; ++fd) {
        static_cast<void>(close(fd));
    }
    rlimit initial{};
    checks.expect(getrlimit(RLIMIT_NOFILE, &initial) == 0, "kernel query");
    checks.expect(initial.rlim_max <= 256, "physical descriptor ceiling");
    checks.expect(getrlimit(-1, &initial) == -1 && errno == EINVAL, "invalid selector");
    checks.expect(nofile(5, 4) == -1 && errno == EINVAL, "soft above hard");
    checks.expect(nofile(257, 257) == -1 && errno == EPERM, "privileged physical ceiling");
    checks.expect(nofile(4) == 0, "one available descriptor");
    int const FD = open("/dev/null", O_RDONLY);
    checks.expect(FD == 3, "exact descriptor boundary");
    checks.expect(open("/dev/null", O_RDONLY) == -1 && errno == EMFILE, "one above boundary");
    checks.expect(dup(1) == -1 && errno == EMFILE, "dup exhaustion");
    checks.expect(dup2(1, 4) == -1 && errno == EBADF, "dup2 numeric boundary");
    checks.expect(fcntl(1, F_DUPFD, 4) == -1 && errno == EINVAL, "dup minimum boundary");
    checks.expect(close(FD) == 0, "descriptor release");

    std::array<int, 2> pipefd{-1, -1};
    checks.expect(pipe(pipefd.data()) == -1 && errno == EMFILE, "pipe needs two reservations");
    int const REUSED = open("/dev/null", O_RDONLY);
    checks.expect(REUSED == 3, "failed pipe leaves no charge");
    checks.expect(close(REUSED) == 0, "release reused slot");
    checks.expect(nofile(5) == 0, "two available descriptors");
    checks.expect(pipe2(pipefd.data(), O_CLOEXEC) == 0, "pipe exact boundary");
    checks.expect(pipefd[0] == 3 && pipefd[1] == 4, "pipe published pair");
    for (int fd : pipefd) {
        checks.expect((fcntl(fd, F_GETFD) & FD_CLOEXEC) != 0, "pipe cloexec");
        checks.expect(close(fd) == 0, "pipe release");
    }

    bool policy_ok = false;
    pthread_t policy_thread{};
    int const POLICY_CREATE = pthread_create(&policy_thread, nullptr, thread_policy, &policy_ok);
    checks.expect(POLICY_CREATE == 0, "thread creation");
    if (POLICY_CREATE == 0) {
        checks.expect(pthread_join(policy_thread, nullptr) == 0 && policy_ok, "thread fork shares owner descriptors");
        rlimit observed{};
        checks.expect(getrlimit(RLIMIT_NOFILE, &observed) == 0 && observed.rlim_cur == 3, "process shares thread policy");
    }
    checks.expect(nofile(4) == 0, "race limit");
    std::atomic<bool> race_failed{false};
    std::array<pthread_t, 8> workers{};
    unsigned started = 0;
    for (auto& worker : workers) {
        if (pthread_create(&worker, nullptr, descriptor_race, &race_failed) != 0) {
            checks.expect(false, "race thread creation");
            break;
        }
        ++started;
    }
    for (unsigned i = 0; i < started; ++i) {
        checks.expect(pthread_join(workers[i], nullptr) == 0, "race thread join");
    }
    checks.expect(!race_failed.load(), "concurrent descriptor bound");

    pid_t child = fork();
    if (child == 0) {
        bool const LOWERED = nofile(3, 3) == 0;
        bool const DROPPED = setuid(1234) == 0;
        bool const DENIED = nofile(3, 4) == -1 && errno == EPERM;
        _exit(LOWERED && DROPPED && DENIED ? 0 : 1);
    }
    checks.expect(wait_ok(child), "hard raise denied after privilege drop");

    child = fork();
    if (child == 0) {
        if (nofile(0) != 0) {
            _exit(1);
        }
        execl("/usr/libexec/wos-resource-exec-probe", "wos-resource-exec-probe", static_cast<char*>(nullptr));
        _exit(2);
    }
    checks.expect(wait_ok(child), "exec preserves zero nofile without temporary user fds");
    child = fork();
    if (child == 0) {
        if (nofile(16) != 0) {
            _exit(1);
        }
        execl("/usr/bin/testprog", "testprog", "resource-limits", "exec-finite", static_cast<char*>(nullptr));
        _exit(2);
    }
    checks.expect(wait_ok(child), "dynamic exec preserves finite nofile");
    checks.expect(nofile(256) == 0, "restore descriptors for file tests");
    struct sigaction ignore{};
    ignore.sa_handler = SIG_IGN;
    sigemptyset(&ignore.sa_mask);
    checks.expect(sigaction(SIGXFSZ, &ignore, nullptr) == 0, "ignore expected file size signals");
    file_size_matrix(checks, "/tmp");
    file_size_matrix(checks, "/root");
    proc_limits_matrix(checks);
    stack_matrix(checks);
    cpu_matrix(checks);
    address_space_matrix(checks);
    child = fork();
    if (child == 0) _exit(process_count_run());
    checks.expect(wait_ok(child), "NPROC isolated resource matrix");
    core_size_matrix(checks);
    std::printf("resource-limits: %u checks, %u failed\n", checks.total, checks.failed);
    std::fflush(stdout);
    return checks.failed == 0 ? 0 : 1;
}
}  // namespace

auto run_resource_limits(int argc, char** argv) -> int {
    if (argc == 1 && std::strcmp(argv[0], "cpu-wait-observation") == 0) return cpu_wait_observation_run();
    if (argc == 1 && std::strcmp(argv[0], "cpu-observation") == 0) return cpu_observation_run();
    if (argc == 1 && std::strcmp(argv[0], "service-public-status") == 0) {
        if (setuid(static_cast<uid_t>(50000 + getpid())) != 0) return 2;
        uint64_t result = static_cast<uint64_t>(ker::abi::callnums::PROCESS);
        register uint64_t version asm("r8") = 1;
        asm volatile("syscall"
                     : "+a"(result)
                     : "D"(static_cast<uint64_t>(ker::abi::process::procmgmt_ops::SERVICE_RESOURCE_READ)), "S"(uint64_t{0}),
                       "d"(uint64_t{0}), "r"(version)
                     : "rcx", "r11", "memory");
        if (static_cast<int64_t>(result) != -EACCES) return 3;
        execl("/usr/bin/servicectl", "servicectl", "status", "journald", static_cast<char*>(nullptr));
        return 4;
    }
    if (argc == 1 && std::strcmp(argv[0], "service-policy") == 0) return service_policy_run();
    if (argc == 1 && std::strcmp(argv[0], "service-cpu-restart") == 0) return service_cpu_restart_run();
    if (argc == 1 && std::strcmp(argv[0], "nproc") == 0) return process_count_run();
    if (argc == 1 && std::strcmp(argv[0], "nproc-exec") == 0) {
        rlimit limit{};
        if (getrlimit(RLIMIT_NPROC, &limit) != 0 || limit.rlim_cur != 0) return 2;
        pid_t const CHILD = fork();
        if (CHILD == 0) _exit(3);
        bool const DENIED = CHILD == -1 && errno == EAGAIN;
        if (CHILD > 0) wait_ok(CHILD);
        return DENIED ? 0 : 4;
    }
    if (argc == 1 && std::strcmp(argv[0], "as") == 0) {
        Checks checks;
        address_space_matrix(checks);
        std::printf("resource-limits AS: %u checks, %u failed\n", checks.total, checks.failed);
        return checks.failed == 0 ? 0 : 1;
    }
    if (argc == 1 && std::strcmp(argv[0], "cpu") == 0) {
        Checks checks;
        cpu_matrix(checks);
        std::printf("resource-limits CPU: %u checks, %u failed\n", checks.total, checks.failed);
        return checks.failed == 0 ? 0 : 1;
    }
    if (argc == 1 && std::strcmp(argv[0], "cpu-exec") == 0) {
        ker::abi::resource::Usage usage{};
        return resource_usage(ker::abi::resource::Id::CPU, usage) && usage.current >= 1 && usage.limit.soft == 3 && usage.limit.hard == 5
                   ? 0
                   : 1;
    }
    if (argc == 1 && std::strcmp(argv[0], "exec-finite") == 0) {
        rlimit limit{};
        return getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur == 16 && limit.rlim_max == 256 ? 0 : 1;
    }
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        _exit(matrix());
    }
    return wait_ok(CHILD) ? 0 : 1;
}
