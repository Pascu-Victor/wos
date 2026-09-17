/* Disposable-VM worker credential and process-inspection fixture.
 * Install this owned binary root:root 06755 for the NNP exec check. */
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

struct process {
    long pid, parent;
    unsigned long long start;
    unsigned uid[4], gid[4];
    char name[64];
};

static int read_process(long pid, struct process* result) {
    char path[80], line[256];
    snprintf(path, sizeof(path), "/proc/%ld/status", pid);
    FILE* file = fopen(path, "r");
    if (!file) return 0;
    memset(result, 0, sizeof(*result));
    result->pid = pid;
    unsigned seen = 0;
    while (fgets(line, sizeof(line), file)) {
        if (strncmp(line, "Name:", 5) == 0) sscanf(line + 5, "%63s", result->name);
        if (strncmp(line, "PPid:", 5) == 0) sscanf(line + 5, "%ld", &result->parent);
        if (strncmp(line, "StartTime:", 10) == 0) sscanf(line + 10, "%llu", &result->start);
        if (strncmp(line, "Uid:", 4) == 0)
            seen += sscanf(line + 4, "%u %u %u %u", &result->uid[0], &result->uid[1], &result->uid[2], &result->uid[3]) == 4;
        if (strncmp(line, "Gid:", 4) == 0)
            seen += sscanf(line + 4, "%u %u %u %u", &result->gid[0], &result->gid[1], &result->gid[2], &result->gid[3]) == 4;
    }
    fclose(file);
    return seen == 2 && result->start && (!strcmp(result->name, "dropbear") || !strcmp(result->name, "dropbearmulti"));
}

static long process_call(unsigned long operation, unsigned long first, unsigned long second) {
    register unsigned long arg3 __asm__("r8") = 0;
    register unsigned long arg4 __asm__("r9") = 0;
    register unsigned long arg5 __asm__("r10") = 0;
    long result;
    __asm__ volatile("syscall"
                     : "=a"(result)
                     : "a"(3UL), "D"(operation), "S"(first), "d"(second), "r"(arg3), "r"(arg4), "r"(arg5)
                     : "rcx", "r11", "memory");
    return result;
}

static long control(unsigned long operation, unsigned long value) { return process_call(36, operation, value); }
static unsigned checks, failed;
static void expect(int result, const char* label) {
    checks++;
    if (!result) {
        failed++;
        printf("FAIL %s errno=%d\n", label, errno);
    }
}

static int ids(void) {
    uid_t r, e, s;
    gid_t gr, ge, gs;
    return getresuid(&r, &e, &s) == 0 && getresgid(&gr, &ge, &gs) == 0 && r == 104 && e == 104 && s == 104 && gr == 104 && ge == 104 &&
           gs == 104;
}

static void restricted(void) {
    expect(ids(), "all six worker IDs");
    expect(getgroups(0, NULL) == 0, "empty supplementary groups");
    expect(control(39, 0) == 1, "NNP");
    expect(control(0x574f5301, 0) == 0, "zero authority ceiling");
    expect(control(0x574f5304, 0) == 256, "sealed zero service grants");
}

int main(int argc, char** argv) {
    struct process worker, broker;
    if (argc == 2 && !strcmp(argv[1], "list")) {
        DIR* directory = opendir("/proc");
        struct dirent* entry;
        if (!directory) return 2;
        while ((entry = readdir(directory))) {
            char* end;
            long pid = strtol(entry->d_name, &end, 10);
            if (pid > 1 && !*end && read_process(pid, &worker) && worker.uid[0] == 104) printf("%ld %llu\n", pid, worker.start);
        }
        closedir(directory);
        return 0;
    }
    if (argc == 2 && !strcmp(argv[1], "after-exec")) {
        restricted();
        printf("worker set-ID exec: %u checks, %u failed\n", checks, failed);
        return failed != 0;
    }
    if (argc < 4) return 2;
    long worker_pid = strtol(argv[2], NULL, 10);
    unsigned long long worker_start = strtoull(argv[3], NULL, 10);
    if (worker_pid <= 1 || !read_process(worker_pid, &worker) || worker.start != worker_start) return 3;
    for (int i = 0; i < 4; i++)
        if (worker.uid[i] != 104 || worker.gid[i] != 104) return 4;
    if (argc == 4 && !strcmp(argv[1], "identity")) {
        printf("worker=%ld start=%llu uid=104,104,104 gid=104,104,104\n", worker.pid, worker.start);
        return 0;
    }
    if (argc != 6 || (strcmp(argv[1], "probe") && strcmp(argv[1], "session"))) return 2;
    long broker_pid = strtol(argv[4], NULL, 10);
    unsigned long long broker_start = strtoull(argv[5], NULL, 10);
    if (broker_pid <= 1 || !read_process(broker_pid, &broker) || broker.start != broker_start || broker.parent != worker.pid) return 5;
    for (int i = 0; i < 4; i++)
        if (broker.uid[i] != 0 || broker.gid[i] != 0) return 6;
    expect(getsid(worker_pid) == worker_pid, "worker owns its session");
    expect(getsid(broker_pid) == broker_pid && getpgid(broker_pid) == broker_pid, "broker owns a separate session and group");
    if (!strcmp(argv[1], "session")) {
        expect(getsid(0) != broker_pid && getsid(0) != worker_pid, "login command session is separate from broker and worker");
        expect(getsid(0) == getpgrp(), "login command retains its session group");
        printf("login session: %u checks, %u failed\n", checks, failed);
        return failed != 0;
    }
    /* Execute the production profile in this disposable process, then probe
     * the actual separately identified worker/broker without inheriting RPC FDs. */
    if (control(38, 1) || control(0x574f5303, 0) || setgroups(0, NULL) || setgid(104) || setuid(104) || control(0x574f5302, 0) ||
        control(4, 0))
        return 7;
    restricted();
    expect(control(3, 0) == 0, "dumpability disabled");
    expect(setuid(0) == -1 && errno == EPERM, "cannot regain root UID");
    expect(seteuid(0) == -1 && errno == EPERM, "cannot regain effective root UID");
    expect(setgid(0) == -1 && errno == EPERM, "cannot regain root GID");
    expect(setegid(0) == -1 && errno == EPERM, "cannot regain effective root GID");
    gid_t root = 0;
    expect(setgroups(1, &root) == -1 && errno == EPERM, "cannot add root supplementary group");
    expect(control(0x574f5302, 65535) == -EPERM, "cannot restore authority");
    expect(control(0x574f5303, 1) == -EPERM, "cannot add service grants");
    expect(control(38, 0) == -EINVAL, "cannot clear NNP");
    expect(kill(broker_pid, 0) == -1 && errno == EPERM, "cannot signal identified broker");
    expect(kill(broker_pid, SIGCONT) == -1 && errno == EPERM, "cannot continue an isolated broker");
    for (int i = 0; i < 2; i++) {
        long pid = i ? worker_pid : broker_pid;
        char path[80], unchanged[4] = {'s', 'a', 'f', 'e'};
        snprintf(path, sizeof(path), "/proc/%ld/fd/0", pid);
        int fd = open(path, O_RDONLY);
        expect(fd == -1 && errno == EACCES, "cannot duplicate foreign descriptor");
        if (fd >= 0) close(fd);
        snprintf(path, sizeof(path), "/proc/%ld/exe", pid);
        expect(readlink(path, unchanged, sizeof(unchanged)) == -1 && errno == EACCES && !memcmp(unchanged, "safe", 4),
               "cannot inspect foreign executable; denial preserves buffer");
        long result = process_call(30, 0x4206, pid); /* PTRACE_SEIZE: no stop on an unexpected admission. */
        expect(result == -EPERM, "cannot seize identified broker/worker");
        if (result == 0) (void)process_call(30, 17, pid);
    }
    int key = open("/etc/dropbear/dropbear_rsa_host_key", O_RDONLY);
    expect(key == -1 && errno == EACCES, "cannot open private host key");
    if (key >= 0) close(key); /* Never read or print key bytes, even on failure. */
    restricted();
    printf("worker policy: %u checks, %u failed\n", checks, failed);
    fflush(stdout);
    if (failed) return 1;
    execl("/tmp/goal19-ssh-worker", "/tmp/goal19-ssh-worker", "after-exec", NULL);
    return 8;
}
