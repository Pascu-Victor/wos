// Ordinary exec consistency regression: a running image's read-only data stays stable.
#include <elf.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
// The child does not touch this page before the parent completes its file write.
__attribute__((used, aligned(4096), section(".rodata.exec_probe"))) const char probe[4096] = "admitted-image";
constexpr size_t MARKER_SIZE = 14;

bool exact_read(int fd, void* buffer, size_t size) {
    auto* bytes = static_cast<char*>(buffer);
    while (size) {
        pollfd event{fd, POLLIN, 0};
        int ready = poll(&event, 1, 10000);
        if (ready < 0 && errno == EINTR) continue;
        if (ready <= 0) return false;
        ssize_t n = read(fd, bytes, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        bytes += n;
        size -= static_cast<size_t>(n);
    }
    return true;
}
int reap(pid_t pid) {
    int status{};
    for (unsigned i = 0; pid > 0 && i < 10000; ++i) {
        pid_t result = waitpid(pid, &status, WNOHANG);
        if (result == pid) return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
        if (result < 0 && errno != EINTR) return 90;
        usleep(1000);
    }
    if (pid > 0) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
    }
    return 90;
}
bool copy_image(const char* source, const char* target, off_t& marker_offset) {
    int input = open(source, O_RDONLY);
    if (input < 0) return false;
    Elf64_Ehdr header{};
    bool ok = exact_read(input, &header, sizeof(header)) && !memcmp(header.e_ident, ELFMAG, SELFMAG) && header.e_type == ET_EXEC &&
              header.e_phentsize == sizeof(Elf64_Phdr) && header.e_phnum < 128;
    marker_offset = -1;
    for (unsigned i = 0; ok && i < header.e_phnum; ++i) {
        Elf64_Phdr ph{};
        off_t pos = static_cast<off_t>(header.e_phoff + i * sizeof(ph));
        ok = pread(input, &ph, sizeof(ph), pos) == sizeof(ph);
        uintptr_t address = reinterpret_cast<uintptr_t>(probe);
        if (ok && ph.p_type == PT_LOAD && address >= ph.p_vaddr && address - ph.p_vaddr + MARKER_SIZE <= ph.p_filesz)
            marker_offset = static_cast<off_t>(ph.p_offset + address - ph.p_vaddr);
    }
    char original[MARKER_SIZE]{};
    ok = ok && marker_offset >= 0 && pread(input, original, sizeof(original), marker_offset) == sizeof(original) &&
         !memcmp(original, "admitted-image", MARKER_SIZE) && lseek(input, 0, SEEK_SET) == 0;
    int output = ok ? open(target, O_WRONLY | O_CREAT | O_EXCL, 0755) : -1;
    ok &= output >= 0;
    char buffer[16384];
    while (ok) {
        ssize_t n = read(input, buffer, sizeof(buffer));
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            ok = n == 0;
            break;
        }
        ssize_t done = 0;
        while (done < n) {
            ssize_t written = write(output, buffer + done, static_cast<size_t>(n - done));
            if (written < 0 && errno == EINTR) continue;
            if (written <= 0) {
                ok = false;
                break;
            }
            done += written;
        }
    }
    if (output >= 0) ok = close(output) == 0 && ok;
    close(input);
    return ok;
}
int run_case(const char* image, const char* path, unsigned which, bool sync_write) {
    off_t offset{};
    if (!copy_image(image, path, offset)) return 91;
    char alias[768];
    snprintf(alias, sizeof(alias), "%s-link", path);
    if (which == 2 && link(path, alias)) return 92;
    int writer = which == 0 ? open(path, O_RDWR | O_CLOEXEC) : -1;
    int retained_reader = which == 9 ? open(path, O_RDONLY) : -1;
    if ((which == 0 && writer < 0) || (which == 9 && retained_reader < 0)) return 93;
    if (which == 7 || which == 8) {
        int edit = open(path, O_RDWR);
        if (edit < 0) return 94;
        bool edited = false;
        if (which == 7)
            edited = pwrite(edit, "bad!", 4, 0) == 4;
        else {
            Elf64_Ehdr header{};
            if (pread(edit, &header, sizeof(header), 0) == sizeof(header)) {
                for (unsigned i = 0; i < header.e_phnum; ++i) {
                    Elf64_Phdr ph{};
                    if (pread(edit, &ph, sizeof(ph), static_cast<off_t>(header.e_phoff + i * sizeof(ph))) != sizeof(ph)) break;
                    if (ph.p_type == PT_INTERP && ph.p_filesz == 11)
                        edited = pwrite(edit, "/nil/ld.so", 11, static_cast<off_t>(ph.p_offset)) == 11;
                }
            }
        }
        close(edit);
        if (!edited) return 94;
    }
    int control[2], result[2];
    if (pipe(control) || pipe(result)) return 94;
    pid_t child = fork();
    if (!child) {
        close(control[1]);
        close(result[0]);
        if (dup2(control[0], 0) != 0 || dup2(result[1], 1) != 1) _exit(95);
        close(control[0]);
        close(result[1]);
        if (which == 9 && dup2(retained_reader, 9) != 9) _exit(95);
        execl(which == 9 ? "/proc/self/fd/9" : path, path, which == 10 ? "--child-thread" : "--child", nullptr);
        int error = errno;
        write(1, "E", 1);
        write(1, &error, sizeof(error));
        _exit(96);
    }
    close(control[0]);
    close(result[1]);
    char ready{};
    bool response = child > 0 && exact_read(result[0], &ready, 1);
    bool admitted = response && ready == 'R';
    int exec_error = 0;
    if (response && ready == 'E') exact_read(result[0], &exec_error, sizeof(exec_error));
    ssize_t written = -1;
    int synced = -2;
    int error = 0;
    void* mapping = MAP_FAILED;
    size_t map_size = 0;
    if (admitted) {
        if (writer < 0) writer = open(which == 2 ? alias : path, O_RDWR | O_CLOEXEC | (which == 4 ? O_TRUNC : 0));
        if (writer >= 0) {
            if (which == 3)
                written = ftruncate(writer, 0);
            else if (which == 4)
                written = 0;
            else if (which == 5) {
                close(writer);
                writer = open(path, O_WRONLY | O_APPEND | O_CLOEXEC);
                written = writer >= 0 ? write(writer, "append", 6) : -1;
            } else if (which == 6) {
                struct stat st{};
                if (fstat(writer, &st) == 0) {
                    map_size = static_cast<size_t>(st.st_size);
                    mapping = mmap(nullptr, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, writer, 0);
                    if (mapping != MAP_FAILED) {
                        memcpy(static_cast<char*>(mapping) + offset, "modified-image", MARKER_SIZE);
                        written = msync(mapping, map_size, MS_SYNC);
                    }
                }
            } else
                written = pwrite(writer, "modified-image", MARKER_SIZE, offset);
        }
        // Remote writes may be buffered in the client. Complete owner-side
        // writeback before allowing the child to inspect its image page.
        if (sync_write && written >= 0 && writer >= 0) {
            synced = fsync(writer);
            if (synced < 0) written = -1;
        }
        error = written < 0 ? errno : 0;
        write(control[1], "G", 1);
    }
    close(control[1]);
    char observed[MARKER_SIZE + 1]{};
    bool read_ok = admitted && exact_read(result[0], observed, MARKER_SIZE);
    close(result[0]);
    int status = reap(child);
    bool stable = read_ok && !memcmp(observed, "admitted-image", MARKER_SIZE);
    bool valid = admitted && written < 0 && error == ETXTBSY && stable && !status;
    if (which == 7 || which == 8) valid = !admitted && exec_error == ENOEXEC && status == 96;
    // The same original descriptor/mapping may outlive exec. It must not retain
    // the private image lease after the last process reference is retired.
    if (writer < 0) writer = open(path, O_RDWR);
    bool released = writer >= 0 && pwrite(writer, "released-image", MARKER_SIZE, offset) == MARKER_SIZE;
    if (sync_write && released) released = fsync(writer) == 0;
    if (mapping != MAP_FAILED) released = msync(mapping, map_size, MS_SYNC) == 0 && munmap(mapping, map_size) == 0 && released;
    if (writer >= 0) close(writer);
    if (retained_reader >= 0) close(retained_reader);
    bool ok = valid && released;
    printf("%s case=%u admitted=%d write=%ld sync=%d errno=%d exec_errno=%d observed=%s child=%d released=%d\n", ok ? "PASS" : "FAIL",
           which, admitted, static_cast<long>(written), synced, error, exec_error, read_ok ? observed : "<unread>", status, released);
    bool clean = unlink(path) == 0;
    if (which == 2) clean = unlink(alias) == 0 && clean;
    printf("case cleanup %s\n", clean ? "completed" : "FAILED");
    return ok && clean ? 0 : 1;
}
bool checkpoint(char expected) {
    pollfd event{0, POLLIN, 0};
    int ready;
    do {
        ready = poll(&event, 1, 120000);
    } while (ready < 0 && errno == EINTR);
    char command{};
    return ready > 0 && read(0, &command, 1) == 1 && command == expected;
}
int unsupported_peer_case(const char* image, const char* path) {
    if (ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL)) return 2;
    off_t offset{};
    if (!copy_image(image, path, offset)) return 91;
    int result[2];
    if (pipe(result)) return 94;
    pid_t child = fork();
    if (!child) {
        close(result[0]);
        execl(path, path, "--unsupported-child", nullptr);
        int error = errno;
        bool sent = write(result[1], &error, sizeof(error)) == sizeof(error);
        _exit(sent ? 0 : 96);
    }
    close(result[1]);
    int error{};
    bool denied = child > 0 && exact_read(result[0], &error, sizeof(error)) && error == ENOTSUP;
    close(result[0]);
    int status = reap(child);
    int writer = open(path, O_RDWR);
    char marker[MARKER_SIZE]{};
    bool unchanged =
        writer >= 0 && pread(writer, marker, sizeof(marker), offset) == sizeof(marker) && !memcmp(marker, "admitted-image", MARKER_SIZE);
    bool writable = writer >= 0 && pwrite(writer, "released-image", MARKER_SIZE, offset) == MARKER_SIZE && fsync(writer) == 0;
    if (writer >= 0) writable = close(writer) == 0 && writable;
    bool clean = unlink(path) == 0;
    bool ok = denied && status == 0 && unchanged && writable && clean;
    printf("%s unsupported exec_errno=%d child=%d unchanged=%d writable=%d cleanup=%d\n", ok ? "PASS" : "FAIL", error, status, unchanged,
           writable, clean);
    return ok ? 0 : 1;
}

int cancellation_case(const char* image, const char* path, bool long_wait = false) {
    off_t offset{};
    if (!copy_image(image, path, offset)) return 91;
    int control[2], result[2];
    if (pipe(control) || pipe(result)) return 94;
    pid_t child = fork();
    if (!child) {
        close(control[1]);
        close(result[0]);
        if (dup2(control[0], 0) != 0 || dup2(result[1], 1) != 1) _exit(95);
        close(control[0]);
        close(result[1]);
        execl(path, path, long_wait ? "--restart-child" : "--child", nullptr);
        _exit(96);
    }
    close(control[0]);
    close(result[1]);
    char ready{};
    bool ok = child > 0 && exact_read(result[0], &ready, 1) && ready == 'R';
    if (ok) {
        puts("ADMITTED");
        fflush(stdout);
        ok = checkpoint('G') && write(control[1], "G", 1) == 1;
    }
    close(control[1]);
    char observed[MARKER_SIZE]{};
    ok = ok && exact_read(result[0], observed, sizeof(observed)) && !memcmp(observed, "admitted-image", MARKER_SIZE);
    close(result[0]);
    int status{};
    pid_t waited;
    do {
        waited = waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    ok = ok && waited == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    printf("EXIT ok=%d status=%d\n", ok, status);
    fflush(stdout);
    ok = checkpoint('H') && ok;
    int writer = open(path, O_RDWR);
    bool released = writer >= 0 && pwrite(writer, "released-image", MARKER_SIZE, offset) == MARKER_SIZE && fsync(writer) == 0;
    if (writer >= 0) released = close(writer) == 0 && released;
    bool clean = unlink(path) == 0;
    printf("%s cancellation released=%d cleanup=%d\n", ok && released && clean ? "PASS" : "FAIL", released, clean);
    return ok && released && clean ? 0 : 1;
}

void* child_probe(void*) {
    char go{};
    if (write(1, "R", 1) != 1 || !exact_read(0, &go, 1) || go != 'G') return reinterpret_cast<void*>(97);
    return write(1, probe, MARKER_SIZE) == MARKER_SIZE ? nullptr : reinterpret_cast<void*>(98);
}

int peer_restart_case(const char* image, const char* path) {
    signal(SIGPIPE, SIG_IGN);
    if (ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL)) return 2;
    off_t offset{};
    if (!copy_image(image, path, offset)) return 91;
    int retained = open(path, O_RDONLY | O_CLOEXEC);
    char initial[MARKER_SIZE]{};
    if (retained < 0 || pread(retained, initial, sizeof(initial), offset) != sizeof(initial) ||
        memcmp(initial, "admitted-image", MARKER_SIZE))
        return 92;
    int control[2], result[2];
    if (pipe(control) || pipe(result)) return 94;
    pid_t child = fork();
    if (!child) {
        close(control[1]);
        close(result[0]);
        if (dup2(control[0], 0) != 0 || dup2(result[1], 1) != 1) _exit(95);
        close(control[0]);
        close(result[1]);
        execl(path, path, "--restart-child", nullptr);
        _exit(96);
    }
    close(control[0]);
    close(result[1]);
    char ready{};
    bool ok = child > 0 && exact_read(result[0], &ready, 1) && ready == 'R';
    if (ok) {
        puts("ADMITTED");
        fflush(stdout);
        ok = checkpoint('G');
    }
    char stale[MARKER_SIZE]{};
    errno = 0;
    ssize_t stale_read = ok ? pread(retained, stale, sizeof(stale), offset) : 0;
    int stale_error = errno;
    close(retained);
    bool refused = stale_read == -1 && stale_error == EIO;
    ok = refused && write(control[1], "G", 1) == 1 && ok;
    close(control[1]);
    char observed[MARKER_SIZE + 1]{};
    bool read_ok = exact_read(result[0], observed, MARKER_SIZE);
    close(result[0]);
    int status = reap(child);
    bool old_image = (read_ok && !memcmp(observed, "admitted-image", MARKER_SIZE) && status == 0) ||
                     (!read_ok && (status == 128 + SIGBUS || status == 128 + SIGSEGV));
    printf("RETIRED ok=%d stale_read=%ld errno=%d child=%d observed=%s\n", ok && old_image, static_cast<long>(stale_read), stale_error,
           status, read_ok ? observed : "<unread>");
    fflush(stdout);
    ok = checkpoint('H') && old_image && ok;
    // Live File pins correctly block unmount. Once they retire, the coordinator
    // requires the exact replacement mount before admitting new file bytes.
    bool removed = unlink(path) == 0 || errno == ENOENT;
    off_t replacement_offset{};
    bool replaced = ok && removed && copy_image(image, path, replacement_offset) && replacement_offset == offset;
    int writer = replaced ? open(path, O_RDWR | O_CLOEXEC) : -1;
    replaced = writer >= 0 && pwrite(writer, "modified-image", MARKER_SIZE, offset) == MARKER_SIZE && fsync(writer) == 0;
    if (writer >= 0) close(writer);
    ok = replaced && ok;
    // A fresh exec must admit the replacement owner's bytes successfully.
    int fresh[2];
    bool fresh_ok = false;
    if (pipe(fresh) == 0) {
        pid_t next = fork();
        if (!next) {
            close(fresh[0]);
            if (dup2(fresh[1], 1) != 1) _exit(95);
            close(fresh[1]);
            execl(path, path, "--replacement-child", nullptr);
            _exit(96);
        }
        close(fresh[1]);
        char current[MARKER_SIZE]{};
        fresh_ok = next > 0 && exact_read(fresh[0], current, sizeof(current)) && !memcmp(current, "modified-image", MARKER_SIZE);
        close(fresh[0]);
        fresh_ok = reap(next) == 0 && fresh_ok;
    }
    bool clean = unlink(path) == 0;
    printf("%s peer restart stale_refused=%d old_image=%d fresh=%d cleanup=%d\n", ok && fresh_ok && clean ? "PASS" : "FAIL", refused,
           old_image, fresh_ok, clean);
    return ok && fresh_ok && clean ? 0 : 1;
}
}  // namespace
int main(int argc, char** argv) {
    if (argc == 3 && !strcmp(argv[1], "--consumer-prepare")) {
        off_t offset{};
        if (!copy_image(argv[0], argv[2], offset)) return 91;
        printf("PREPARED offset=%ld\n", static_cast<long>(offset));
        return 0;
    }
    if (argc == 5 && !strcmp(argv[1], "--consumer-write")) {
        off_t offset = static_cast<off_t>(strtoll(argv[3], nullptr, 10));
        bool blocked = !strcmp(argv[4], "blocked");
        int fd = open(argv[2], O_RDWR);
        char before[MARKER_SIZE]{}, after[MARKER_SIZE]{};
        if (fd < 0 || offset < 0 || pread(fd, before, sizeof(before), offset) != sizeof(before)) return 92;
        errno = 0;
        ssize_t written = pwrite(fd, "released-image", MARKER_SIZE, offset);
        int error = errno;
        bool ok = blocked ? written == -1 && error == ETXTBSY : written == MARKER_SIZE && fsync(fd) == 0;
        ok = pread(fd, after, sizeof(after), offset) == sizeof(after) && !memcmp(after, blocked ? before : "released-image", MARKER_SIZE) &&
             ok;
        close(fd);
        printf("%s lease blocked=%d write=%ld errno=%d\n", ok ? "PASS" : "FAIL", blocked, static_cast<long>(written), error);
        return ok ? 0 : 1;
    }
    if ((argc == 5 && !strcmp(argv[1], "--consumer-start")) || (argc == 3 && !strcmp(argv[1], "--consumer-fresh"))) {
        if (ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL) ||
            ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL))
            return 93;
        if (argc == 5)
            execl(argv[2], argv[2], "--consumer-held", argv[3], argv[4], nullptr);
        else
            execl(argv[2], argv[2], "--replacement-child", nullptr);
        return 94;
    }
    if (argc == 4 && !strcmp(argv[1], "--consumer-held")) {
        char hostname[64]{};
        if (gethostname(hostname, sizeof(hostname)) || strcmp(hostname, argv[3])) return 95;
        int fd = open(argv[2], O_WRONLY | O_CREAT | O_EXCL, 0644);
        if (fd < 0) return 96;
        char ready[80]{};
        int length = snprintf(ready, sizeof(ready), "READY %s\n", hostname);
        bool ok = write(fd, ready, static_cast<size_t>(length)) == length && fsync(fd) == 0;
        close(fd);
        if (!ok) return 96;
        // The coordinator interrupts this disposable consumer before the
        // deadline; a natural timeout must never masquerade as view retirement.
        for (unsigned i = 0; i < 1800; ++i) usleep(100000);
        return 97;
    }
    if (argc == 3 && !strcmp(argv[2], "--control-checkpoint")) return cancellation_case(argv[0], argv[1], true);
    if (argc == 3 && !strcmp(argv[2], "--peer-restart")) return peer_restart_case(argv[0], argv[1]);
    if (argc == 2 && !strcmp(argv[1], "--restart-child")) {
        if (write(1, "R", 1) != 1 || !checkpoint('G')) return 97;
        return write(1, probe, MARKER_SIZE) == MARKER_SIZE ? 0 : 98;
    }
    if (argc == 2 && !strcmp(argv[1], "--replacement-child")) return write(1, probe, MARKER_SIZE) == MARKER_SIZE ? 0 : 98;
    if (argc == 3 && !strcmp(argv[2], "--unsupported-peer")) return unsupported_peer_case(argv[0], argv[1]);
    if (argc == 2 && !strcmp(argv[1], "--unsupported-child")) return 97;
    if (argc == 3 && !strcmp(argv[2], "--cancel-checkpoint")) return cancellation_case(argv[0], argv[1]);
    if (argc == 2 && !strcmp(argv[1], "--child")) return static_cast<int>(reinterpret_cast<uintptr_t>(child_probe(nullptr)));
    if (argc == 2 && !strcmp(argv[1], "--child-thread")) {
        pthread_t worker{};
        void* result{};
        if (pthread_create(&worker, nullptr, child_probe, nullptr) || pthread_join(worker, &result)) return 99;
        return static_cast<int>(reinterpret_cast<uintptr_t>(result));
    }
    setvbuf(stdout, nullptr, _IOLBF, 0);
    signal(SIGPIPE, SIG_IGN);
    bool const WRITE_ONLY = argc == 3 && !strcmp(argv[2], "--write-only");
    bool const REMOTE_CASES = argc == 3 && !strcmp(argv[2], "--remote-cases");
    if ((argc != 2 && !WRITE_ONLY && !REMOTE_CASES) || ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL))
        return 2;
    unsigned const CASES = WRITE_ONLY ? 2 : (REMOTE_CASES ? 10 : 11);
    unsigned failed = 0;
    for (unsigned which = 0; which < (WRITE_ONLY ? 2U : 11U); ++which) {
        // Remote VFS has no link operation. Local mode retains its hardlink
        // control; the other cases exercise the supported remote interfaces.
        if (REMOTE_CASES && which == 2) continue;
        char path[640];
        snprintf(path, sizeof(path), "%s-%u", argv[1], which);
        failed += run_case(argv[0], path, which, WRITE_ONLY || REMOTE_CASES) != 0;
    }
    printf("%s: %u exec file-stability cases\n", failed ? "FAIL" : "PASS", CASES);
    return failed ? 1 : 0;
}
