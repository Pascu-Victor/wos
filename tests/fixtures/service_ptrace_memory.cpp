// Ordinary ptrace memory, COW, boundary-fault and dumpability controls.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../../modules/kern/src/abi/ptrace.hpp"

namespace {
namespace trace = ker::abi::ptrace;
constexpr size_t PAGE = 4096;
constexpr uint64_t ROOT_WORD = 0x1122334455667788;
constexpr uint64_t USER_WORD = 0x8877665544332211;
int64_t call(trace::request op, pid_t pid, uint64_t address = 0, uint64_t data = 0) {
    return ker::process::ptrace(static_cast<uint64_t>(op), pid, address, data);
}
bool byte(int fd, char expected) {
    char value = 0;
    ssize_t count;
    do {
        count = read(fd, &value, 1);
    } while (count < 0 && errno == EINTR);
    return count == 1 && value == expected;
}
bool filled(const void* memory, size_t length, unsigned char value) {
    auto* bytes = static_cast<const unsigned char*>(memory);
    for (size_t i = 0; i < length; ++i)
        if (bytes[i] != value) return false;
    return true;
}
int64_t transfer(pid_t target, uint64_t address, void* buffer, size_t size, bool write, size_t& done) {
    trace::MemIo io{address, buffer, size, 55};
    int64_t result = call(write ? trace::request::WRITE_MEM : trace::request::READ_MEM, target, 0, reinterpret_cast<uint64_t>(&io));
    done = io.transferred;
    return result;
}
}  // namespace

int main(int argc, char** argv) {
    char node[64]{};
    if (argc != 3 || geteuid() != 0 || gethostname(node, sizeof(node)) || strcmp(node, argv[2])) return 2;
    int report = open(argv[1], O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (report < 0) return 3;
    auto* mapping = static_cast<unsigned char*>(mmap(nullptr, 3 * PAGE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (mapping == MAP_FAILED) return 4;
    memset(mapping, 0x2a, 3 * PAGE);
    uint64_t const base = reinterpret_cast<uint64_t>(mapping);
    int ready[2], command[2];
    if (pipe(ready) || pipe(command)) return 5;
    alarm(45);
    pid_t target = fork();
    if (target < 0) return 6;
    if (target == 0) {
        close(ready[0]);
        close(command[1]);
        close(report);
        if (munmap(mapping + 2 * PAGE, PAGE) || setgroups(0, nullptr) || setgid(2000) || setuid(1000) || ker::process::prctl(4, 1, 0, 0, 0))
            _exit(10);
        if (write(ready[1], "R", 1) != 1 || !byte(command[0], 'D') || ker::process::prctl(4, 0, 0, 0, 0) || write(ready[1], "D", 1) != 1 ||
            !byte(command[0], 'X'))
            _exit(11);
        uint64_t word;
        memcpy(&word, mapping + PAGE - 4, sizeof(word));
        bool const ok = word == USER_WORD && filled(mapping, 32, 0x2a) && filled(mapping + 128, 32, 0x4b) &&
                        filled(mapping + 2 * PAGE - 16, 8, 0x2a) && filled(mapping + 2 * PAGE - 8, 8, 0x6c);
        _exit(ok ? 0 : 12);
    }
    close(ready[1]);
    close(command[0]);
    unsigned checks = 0, failures = 0;
    auto check = [&](const char* name, bool ok) {
        ++checks;
        if (!ok) ++failures;
        dprintf(report, "node=%s op=%s preserved=%d\n", node, name, ok);
    };
    auto allowed = [&](bool user) {
        uint64_t word = 0;
        check(user ? "user-peek" : "root-peek",
              call(trace::request::PEEKDATA, target, base, reinterpret_cast<uint64_t>(&word)) == 0 && word == 0x2a2a2a2a2a2a2a2a);
        uint64_t const expected = user ? USER_WORD : ROOT_WORD;
        bool ok = call(trace::request::POKEDATA, target, base + PAGE - 4, expected) == 0 &&
                  call(trace::request::PEEKDATA, target, base + PAGE - 4, reinterpret_cast<uint64_t>(&word)) == 0 && word == expected;
        check(user ? "user-poke-boundary" : "root-poke-boundary", ok);
        unsigned char buffer[32]{};
        unsigned char expected_bytes[16];
        memset(expected_bytes, 0x2a, sizeof(expected_bytes));
        memcpy(expected_bytes + 4, &expected, sizeof(expected));
        size_t done = 0;
        ok = transfer(target, base + PAGE - 8, buffer, 16, false, done) == 0 && done == 16 && !memcmp(buffer, expected_bytes, 16);
        check(user ? "user-read-boundary" : "root-read-boundary", ok);
        memset(buffer, user ? 0x4b : 0x4a, sizeof(buffer));
        ok = transfer(target, base + 128, buffer, sizeof(buffer), true, done) == 0 && done == sizeof(buffer);
        memset(buffer, 0, sizeof(buffer));
        ok = ok && transfer(target, base + 128, buffer, sizeof(buffer), false, done) == 0 && done == sizeof(buffer) &&
             filled(buffer, sizeof(buffer), user ? 0x4b : 0x4a);
        check(user ? "user-write" : "root-write", ok);
    };
    check("target-ready", byte(ready[0], 'R'));
    check("seize", call(trace::request::SEIZE, target) == 0);
    allowed(false);
    uint64_t word = 0xa5a5a5a5a5a5a5a5;
    check("peek-unmapped", call(trace::request::PEEKDATA, target, base + 2 * PAGE, reinterpret_cast<uint64_t>(&word)) == -EFAULT &&
                               word == 0xa5a5a5a5a5a5a5a5);
    unsigned char buffer[16];
    size_t done = 0;
    bool ok = call(trace::request::POKEDATA, target, base + 2 * PAGE - 4, 0) == -EFAULT &&
              transfer(target, base + 2 * PAGE - 4, buffer, 4, false, done) == 0 && done == 4 && filled(buffer, 4, 0x2a);
    check("poke-partial-page-unchanged", ok);
    memset(buffer, 0xa5, sizeof(buffer));
    ok = transfer(target, base + 2 * PAGE - 8, buffer, sizeof(buffer), false, done) == -EFAULT && done == 8 && filled(buffer, 8, 0x2a) &&
         filled(buffer + 8, 8, 0xa5);
    check("read-fault-prefix", ok);
    memset(buffer, 0x6c, sizeof(buffer));
    ok = transfer(target, base + 2 * PAGE - 8, buffer, sizeof(buffer), true, done) == -EFAULT && done == 8;
    memset(buffer, 0, sizeof(buffer));
    ok = ok && transfer(target, base + 2 * PAGE - 16, buffer, sizeof(buffer), false, done) == 0 && done == sizeof(buffer) &&
         filled(buffer, 8, 0x2a) && filled(buffer + 8, 8, 0x6c);
    check("write-fault-prefix", ok);
    check("drop-tracer", !setgroups(0, nullptr) && !setgid(2000) && !setuid(1000));
    allowed(true);
    check("target-revoke", write(command[1], "D", 1) == 1 && byte(ready[0], 'D'));
    word = 0xa5a5a5a5a5a5a5a5;
    check("denied-peek",
          call(trace::request::PEEKDATA, target, base, reinterpret_cast<uint64_t>(&word)) == -EPERM && word == 0xa5a5a5a5a5a5a5a5);
    check("denied-poke", call(trace::request::POKEDATA, target, base + PAGE - 4, 0) == -EPERM);
    for (bool write : {false, true}) {
        memset(buffer, 0xa5, sizeof(buffer));
        trace::MemIo io{base + 128, buffer, sizeof(buffer), 55};
        auto const before = io;
        ok = call(write ? trace::request::WRITE_MEM : trace::request::READ_MEM, target, 0, reinterpret_cast<uint64_t>(&io)) == -EPERM &&
             !memcmp(&io, &before, sizeof(io)) && filled(buffer, sizeof(buffer), 0xa5);
        check(write ? "denied-write" : "denied-read", ok);
    }
    check("detach", call(trace::request::DETACH, target) == 0);
    check("release-target", write(command[1], "X", 1) == 1);
    close(command[1]);
    close(ready[0]);
    int status = -1;
    pid_t waited;
    do {
        waited = waitpid(target, &status, 0);
    } while (waited < 0 && errno == EINTR);
    check("target-exit-and-memory", waited == target && status == 0);
    check("parent-cow-isolation", filled(mapping, 3 * PAGE, 0x2a));
    int const unmapped = munmap(mapping, 3 * PAGE);
    dprintf(report, "%s: ptrace-memory checks=%u failures=%u\n", failures ? "FAIL" : "PASS", checks, failures);
    int const synced = fsync(report);
    close(report);
    return failures || synced || unmapped || checks != 24 ? 1 : 0;
}
