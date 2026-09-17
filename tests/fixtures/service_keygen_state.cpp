// Disposable WOS VM: compile together with init's service_state.cpp and header.
// Uses synthetic bytes only; never opens the VM's actual host-key files.
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "service_state.h"

static int control(int option, unsigned long value) {
    // WOS PROCESS=3, PRCTL operation=36; the option values are the existing
    // credential-control ABI. WOS does not install Linux's sys/prctl.h.
    uint64_t result = 3;
    register uint64_t a4 __asm__("r8") = 0;
    register uint64_t a5 __asm__("r9") = 0;
    register uint64_t a6 __asm__("r10") = 0;
    __asm__ volatile("syscall"
                     : "+a"(result)
                     : "D"(36ULL), "S"((uint64_t)option), "d"((uint64_t)value), "r"(a4), "r"(a5), "r"(a6)
                     : "rcx", "r11", "memory");
    if ((int64_t)result < 0) {
        errno = (int)-(int64_t)result;
        return -1;
    }
    return (int)result;
}

int main() {
    if (getuid() != 0) return 2;
    char path[] = "/tmp/keygen-state-XXXXXX";
    if (!mkdtemp(path)) return 2;
    int directory = open(path, O_RDONLY | O_DIRECTORY);
    if (directory < 0) return 2;
    int file = openat(directory, "dropbear_rsa_host_key", O_RDWR | O_CREAT | O_EXCL, 0600);
    if (file < 0 || write(file, "synthetic", 9) != 9 || fchmod(directory, 0755)) return 2;
    unsigned checks = 0, failed = 0;
    auto expect = [&](bool ok) {
        ++checks;
        if (!ok) {
            ++failed;
            std::printf("keygen state FAIL check=%u errno=%d\n", checks, errno);
        }
    };
    auto prepare = [&] {
        lseek(directory, 0, SEEK_SET);
        int const result = wos::init::prepare_keygen_state_at(directory, 103, 103);
        std::printf("keygen state prepare=%d\n", result);
        return result;
    };
    struct stat state{};
    expect(symlinkat("dropbear_rsa_host_key", directory, "dropbear_rsa_host_key.pub") == 0);
    expect(prepare() == EPERM);
    expect(fstat(directory, &state) == 0 && state.st_uid == 0 && state.st_mode == (S_IFDIR | 0755));
    expect(fstat(file, &state) == 0 && state.st_uid == 0 && state.st_mode == (S_IFREG | 0600));
    expect(unlinkat(directory, "dropbear_rsa_host_key.pub", 0) == 0);
    pid_t child = fork();
    if (!child) {
        if (control(38, 1) || control(0x574f5303, 0) || control(0x574f5302, 0)) _exit(2);
        // Tightening an owned directory succeeds; transferring ownership must
        // fail without authority, then restore its original 0755 mode.
        _exit(prepare() == EPERM ? 0 : 1);
    }
    int status = 0;
    expect(child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    expect(fstat(directory, &state) == 0 && state.st_uid == 0 && state.st_gid == 0 && state.st_mode == (S_IFDIR | 0755));
    expect(fstat(file, &state) == 0 && state.st_uid == 0 && state.st_gid == 0);
    expect(prepare() == 0);
    expect(fstat(directory, &state) == 0 && state.st_uid == 103 && state.st_gid == 103 && state.st_mode == (S_IFDIR | 0700));
    expect(fstat(file, &state) == 0 && state.st_uid == 103 && state.st_gid == 103 && state.st_mode == (S_IFREG | 0600) &&
           state.st_nlink == 1);
    expect(prepare() == 0);
    char bytes[10]{};
    expect(pread(file, bytes, 9, 0) == 9 && std::strcmp(bytes, "synthetic") == 0);
    expect(unlinkat(directory, "dropbear_rsa_host_key", 0) == 0);
    close(file);
    close(directory);
    expect(rmdir(path) == 0);
    std::printf("keygen state: %u checks, %u failed\n", checks, failed);
    return failed != 0;
}
