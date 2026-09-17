/* Disposable-VM broker lifecycle fixture. Signals only an explicitly matched
 * Dropbear child of the matched connection worker, including start times. */
#include <dirent.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct process {
    int pid, parent;
    unsigned long long start;
    char name[64];
};

static int read_process(int pid, struct process* value) {
    char path[80], line[256];
    snprintf(path, sizeof(path), "/proc/%d/status", pid);
    FILE* file = fopen(path, "r");
    if (!file) return 0;
    memset(value, 0, sizeof(*value));
    value->pid = pid;
    while (fgets(line, sizeof(line), file)) {
        if (strncmp(line, "Name:", 5) == 0) sscanf(line + 5, "%63s", value->name);
        if (strncmp(line, "PPid:", 5) == 0) sscanf(line + 5, "%d", &value->parent);
        if (strncmp(line, "StartTime:", 10) == 0) sscanf(line + 10, "%llu", &value->start);
    }
    fclose(file);
    return value->name[0] && value->start;
}

static int is_dropbear(const struct process* value) {
    return strcmp(value->name, "dropbear") == 0 || strcmp(value->name, "dropbearmulti") == 0;
}

int main(int argc, char** argv) {
    struct process worker, broker, candidate;
    if (argc == 2 && strcmp(argv[1], "inspect") == 0) {
        int parent = getppid(), found = 0;
        for (int i = 0; i < 8; i++) {
            if (!read_process(parent, &worker)) return 1;
            if (is_dropbear(&worker)) {
                found = 1;
                break;
            }
            parent = worker.parent;
        }
        if (!found) return 1;
        /* Non-PTY sessions are children of the broker. Require the full
         * broker -> connection worker -> listening Dropbear ancestry, so an
         * older worker-owned command cannot satisfy this topology check. */
        broker = worker;
        if (!read_process(broker.parent, &worker) || !is_dropbear(&worker) || !read_process(worker.parent, &candidate) ||
            !is_dropbear(&candidate))
            return 1;
        printf("%d %llu %d %llu\n", worker.pid, worker.start, broker.pid, broker.start);
        return 0;
    }
    if (argc == 6 && strcmp(argv[1], "terminate") == 0) {
        int worker_pid = atoi(argv[2]), broker_pid = atoi(argv[4]);
        unsigned long long worker_start = strtoull(argv[3], NULL, 10);
        unsigned long long broker_start = strtoull(argv[5], NULL, 10);
        if (worker_pid <= 1 || broker_pid <= 1 || !read_process(worker_pid, &worker) || !read_process(broker_pid, &broker) ||
            worker.start != worker_start || broker.start != broker_start || !is_dropbear(&worker) || !is_dropbear(&broker) ||
            broker.parent != worker.pid)
            return 1;
        return kill(broker.pid, SIGTERM) < 0;
    }
    if (argc == 4 && strcmp(argv[1], "gone") == 0) {
        return read_process(atoi(argv[2]), &broker) && broker.start == strtoull(argv[3], NULL, 10);
    }
    return 2;
}
