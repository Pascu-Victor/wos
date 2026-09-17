// Ordinary pthread creation checks on a disposable TMPFS/XFS workspace.
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <spawn.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace {
unsigned ready, proceed;
char worker_path[512], directory_path[512], batch_path[512];
bool worker_created, directory_created, batch_created, batch_requested;
mode_t worker_mode, directory_mode, worker_previous;
mode_t batch_mode;
int worker_error;
char* executable;
bool fork_inherited, spawn_inherited;

bool child_ok(pid_t pid) {
    int status = 0;
    return pid > 0 && waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

bool wait_for(unsigned* flag) {
    for (unsigned i = 0; i < 10000; ++i) {
        if (__atomic_load_n(flag, __ATOMIC_ACQUIRE)) return true;
        usleep(1000);
    }
    return false;
}

bool create_file(const char* path, bool& created, mode_t& mode) {
    int fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0666);
    if (fd < 0) return false;
    created = true;
    struct stat info{};
    int status = fstat(fd, &info);
    int error = errno;
    if (close(fd) != 0 && status == 0) return false;
    if (status != 0) {
        errno = error;
        return false;
    }
    mode = info.st_mode & 0777;
    return true;
}

void* worker(void*) {
    __atomic_store_n(&ready, 1, __ATOMIC_RELEASE);
    if (!wait_for(&proceed)) {
        worker_error = ETIMEDOUT;
        return nullptr;
    }
    if (!create_file(worker_path, worker_created, worker_mode)) worker_error = errno;
    if (mkdir(directory_path, 0777) == 0) {
        directory_created = true;
        struct stat info{};
        if (stat(directory_path, &info) == 0)
            directory_mode = info.st_mode & 0777;
        else
            worker_error = errno;
    } else {
        worker_error = errno;
    }
    if (batch_requested) {
        namespace abi = ker::abi::vfs;
        abi::metadata_batch_entry entry{batch_path, nullptr};
        abi::metadata_batch_result result{};
        abi::metadata_batch_header header{
            .version = abi::METADATA_BATCH_VERSION, .operation = abi::metadata_batch_operation::CREATE_CLOSE, .count = 1, .mode = 0666};
        int status = abi::metadata_batch(&header, &entry, &result);
        if (status == 0 && result.status == 0) {
            batch_created = true;
            struct stat info{};
            if (stat(batch_path, &info) == 0)
                batch_mode = info.st_mode & 0777;
            else
                worker_error = errno;
        } else {
            worker_error = status != 0 ? status : result.status;
        }
    }
    // Inherit from a worker whose process changed the mask after its birth.
    // Child mutations must remain private to each new process.
    pid_t child = fork();
    if (child == 0) _exit(umask(0) == 0077 ? 0 : 1);
    fork_inherited = child_ok(child);
    char child_option[] = "--child";
    char* arguments[] = {executable, child_option, nullptr};
    child = -1;
    spawn_inherited = posix_spawn(&child, executable, nullptr, nullptr, arguments, environ) == 0 && child_ok(child);
    worker_previous = umask(0027);
    return nullptr;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 2 && (argc != 3 || strcmp(argv[2], "--batch") != 0)) return 2;
    if (strcmp(argv[1], "--child") == 0) return umask(0) == 0077 ? 0 : 1;
    executable = argv[0];
    batch_requested = argc == 3;
    char parent_path[512];
    if (snprintf(worker_path, sizeof(worker_path), "%s.worker", argv[1]) >= static_cast<int>(sizeof(worker_path)) ||
        snprintf(directory_path, sizeof(directory_path), "%s.dir", argv[1]) >= static_cast<int>(sizeof(directory_path)) ||
        snprintf(parent_path, sizeof(parent_path), "%s.parent", argv[1]) >= static_cast<int>(sizeof(parent_path)) ||
        snprintf(batch_path, sizeof(batch_path), "%s.batch", argv[1]) >= static_cast<int>(sizeof(batch_path)))
        return 2;
    mode_t original = umask(0);
    pthread_t thread;
    int status = pthread_create(&thread, nullptr, worker, nullptr);
    if (status != 0) {
        umask(original);
        printf("FAIL pthread_create=%d\n", status);
        return 1;
    }
    bool started = wait_for(&ready);
    mode_t parent_previous = umask(0077);
    __atomic_store_n(&proceed, 1, __ATOMIC_RELEASE);
    status = pthread_join(thread, nullptr);
    if (status != 0) {
        printf("FAIL pthread_join=%d\n", status);
        return 1;
    }
    bool parent_created = false;
    mode_t parent_mode = 0;
    bool parent_open = create_file(parent_path, parent_created, parent_mode);
    bool ok = started && parent_previous == 0 && worker_error == 0 && worker_created && directory_created && worker_mode == 0600 &&
              directory_mode == 0700 && worker_previous == 0077 && parent_open && parent_mode == 0640;
    printf("%s worker_mode=%03o directory_mode=%03o worker_previous=%03o parent_mode=%03o worker_error=%d\n", ok ? "PASS" : "FAIL",
           worker_mode, directory_mode, worker_previous, parent_mode, worker_error);
    printf("%s fork inherits worker's process mask\n", fork_inherited ? "PASS" : "FAIL");
    printf("%s spawn/exec inherits worker's process mask\n", spawn_inherited ? "PASS" : "FAIL");
    ok = ok && fork_inherited && spawn_inherited;
    if (batch_requested) {
        bool batch_ok = batch_created && batch_mode == 0600;
        printf("%s batch_mode=%03o\n", batch_ok ? "PASS" : "FAIL", batch_mode);
        ok = ok && batch_ok;
    }
    bool cleaned = true;
    if (worker_created && unlink(worker_path) != 0) cleaned = false;
    if (directory_created && rmdir(directory_path) != 0) cleaned = false;
    if (parent_created && unlink(parent_path) != 0) cleaned = false;
    if (batch_created && unlink(batch_path) != 0) cleaned = false;
    umask(original);
    printf("%s cleanup\n", cleaned ? "PASS" : "FAIL");
    return ok && cleaned ? 0 : 1;
}
