// Ordinary native mount lifecycle helper for the two-node export regression.
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <unistd.h>

int main(int argc, char** argv) {
    if (argc != 3 || geteuid() != 0) return 2;
    if (strcmp(argv[1], "stat") == 0) {
        struct stat value{};
        if (stat(argv[2], &value) != 0) return 1;
        printf("dev=%llu\n", static_cast<unsigned long long>(value.st_dev));
        return 0;
    }
    bool const create = strcmp(argv[1], "mount") == 0;
    if (!create && strcmp(argv[1], "unmount") != 0) return 2;
    errno = 0;
    int const result = create ? mount(nullptr, argv[2], "tmpfs", 0, nullptr) : umount(argv[2]);
    printf("operation=%s rc=%d errno=%d\n", argv[1], result, errno);
    return result == 0 ? 0 : 1;
}
