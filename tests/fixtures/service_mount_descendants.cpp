// Native mount lifetime checks in a disposable WOS guest; private tmpfs only.
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

int main() {
    if (geteuid() != 0) return 2;
    char parent[128], child[160], deep[192], sibling[128], seed[192];
    snprintf(parent, sizeof(parent), "/tmp/goal19-mount-tree-%d", getpid());
    snprintf(child, sizeof(child), "%s/child", parent);
    snprintf(deep, sizeof(deep), "%s/deep", child);
    snprintf(sibling, sizeof(sibling), "%s2", parent);
    snprintf(seed, sizeof(seed), "%s/seed", child);
    struct Cleanup {
        const char *parent, *child, *deep, *sibling, *seed;
        bool parent_mounted = false, child_mounted = false, deep_mounted = false, sibling_mounted = false;
        int fd = -1;
        ~Cleanup() {
            if (fd >= 0) close(fd);
            if (deep_mounted) umount(deep);
            unlink(seed);
            rmdir(deep);
            if (child_mounted) umount(child);
            rmdir(child);
            if (parent_mounted) umount(parent);
            if (sibling_mounted) umount(sibling);
            rmdir(parent);
            rmdir(sibling);
        }
    } cleanup{parent, child, deep, sibling, seed};
    unsigned checks = 0;
    auto check = [&](bool okay, const char* label) {
        printf("%s %s\n", okay ? "PASS" : "FAIL", label);
        fflush(stdout);
        checks += okay;
        return okay;
    };
    if (!check(mkdir(parent, 0755) == 0 && mkdir(sibling, 0755) == 0, "private directories")) return 1;
    if (!check(mount(nullptr, parent, "tmpfs", MS_NOSUID, nullptr) == 0, "parent mount")) return 1;
    cleanup.parent_mounted = true;
    if (!check(mount(nullptr, sibling, "tmpfs", 0, nullptr) == 0, "similar-prefix sibling mount")) return 1;
    cleanup.sibling_mounted = true;
    if (!check(mkdir(child, 0755) == 0 && mount(nullptr, child, "tmpfs", 0, nullptr) == 0, "child mount")) return 1;
    cleanup.child_mounted = true;
    if (!check(mkdir(deep, 0755) == 0 && mount(nullptr, deep, "tmpfs", 0, nullptr) == 0, "grandchild mount")) return 1;
    cleanup.deep_mounted = true;
    int fd = open(seed, O_CREAT | O_EXCL | O_RDWR, 0600);
    bool const SEEDED = fd >= 0 && write(fd, "seed", 4) == 4;
    if (fd >= 0) close(fd);
    if (!check(SEEDED, "child file initialized and closed")) return 1;
    struct stat before_parent{}, before_child{}, after{};
    if (!check(stat(parent, &before_parent) == 0 && stat(child, &before_child) == 0, "mount identities captured")) return 1;
    errno = 0;
    if (!check(umount(parent) == -1 && errno == EBUSY, "parent with descendants stays busy")) return 1;
    errno = 0;
    if (!check(umount(child) == -1 && errno == EBUSY, "child with grandchild stays busy")) return 1;
    if (!check(stat(parent, &after) == 0 && after.st_dev == before_parent.st_dev && after.st_ino == before_parent.st_ino &&
                   stat(child, &after) == 0 && after.st_dev == before_child.st_dev && after.st_ino == before_child.st_ino,
               "denials preserve mount identities"))
        return 1;
    struct statvfs policy{};
    if (!check(statvfs(parent, &policy) == 0 && (policy.f_flag & ST_NOSUID), "parent nosuid preserved")) return 1;
    cleanup.fd = open(seed, O_RDONLY);
    char bytes[4]{};
    if (!check(cleanup.fd >= 0 && read(cleanup.fd, bytes, sizeof(bytes)) == 4 && !memcmp(bytes, "seed", 4), "child data preserved"))
        return 1;
    if (!check(umount(deep) == 0, "grandchild removal")) return 1;
    cleanup.deep_mounted = false;
    errno = 0;
    if (!check(umount(child) == -1 && errno == EBUSY, "open child file remains busy")) return 1;
    errno = 0;
    if (!check(umount(parent) == -1 && errno == EBUSY, "busy child still protects parent")) return 1;
    close(cleanup.fd);
    cleanup.fd = -1;
    if (!check(unlink(seed) == 0 && rmdir(deep) == 0 && umount(child) == 0, "child cleanup and removal")) return 1;
    cleanup.child_mounted = false;
    if (!check(rmdir(child) == 0 && umount(parent) == 0, "parent removal after child")) return 1;
    cleanup.parent_mounted = false;
    if (!check(stat(sibling, &after) == 0 && umount(sibling) == 0, "similar-prefix sibling remains independent")) return 1;
    cleanup.sibling_mounted = false;
    if (!check(rmdir(parent) == 0 && rmdir(sibling) == 0, "private namespace cleanup")) return 1;
    printf("PASS: %u native mount descendant checks; cleanup completed\n", checks);
    return 0;
}
