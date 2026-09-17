// Ordinary peer departure: the host syncs and stops its owned wos-3 VM after READY.
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>

auto main() -> int {
    constexpr const char* PEER = "/dev/nodes/wos-3";
    alarm(45);
    int const ID = open("/dev/nodes/wos-3/id", O_RDONLY);
    DIR* peer = opendir(PEER);
    DIR* nodes = opendir("/dev/nodes");
    struct Cleanup {
        int id;
        DIR* peer;
        DIR* nodes;
        ~Cleanup() {
            if (id >= 0) close(id);
            if (peer != nullptr) closedir(peer);
            if (nodes != nullptr) closedir(nodes);
        }
    } cleanup{ID, peer, nodes};
    if (ID < 0 || peer == nullptr || nodes == nullptr) return 2;
    struct stat original{};
    if (fstat(ID, &original) != 0) return 3;
    std::array<char, 16> expected{};
    int const OBSERVED_ID = open("/dev/nodes/wos-3/id", O_RDONLY);
    if (OBSERVED_ID < 0) return 11;
    ssize_t const OBSERVED = read(OBSERVED_ID, expected.data(), expected.size());
    close(OBSERVED_ID);
    if (OBSERVED != 7 || expected[0] != '0' || expected[1] != 'x' || expected[6] != '\n') return 12;
    puts("READY peer=wos-3 retained=id,directory");
    fflush(stdout);
    unsigned scans = 0;
    bool gone = false;
    for (unsigned attempt = 0; attempt < 1500; ++attempt) {
        rewinddir(nodes);
        errno = 0;
        while (readdir(nodes) != nullptr) {
        }
        if (errno != 0) return 4;
        ++scans;
        struct stat current{};
        if (stat(PEER, &current) != 0) {
            if (errno != ENOENT) return 5;
            gone = true;
            break;
        }
        usleep(20000);
    }
    if (!gone) return 6;
    std::array<char, 16> buffer{};
    if (read(ID, buffer.data(), buffer.size()) != 7 || std::memcmp(buffer.data(), expected.data(), 7) != 0) return 7;
    struct stat retained{};
    if (fstat(ID, &retained) != 0 || retained.st_ino != original.st_ino || retained.st_uid != original.st_uid ||
        retained.st_gid != original.st_gid || retained.st_mode != original.st_mode)
        return 8;
    rewinddir(peer);
    errno = 0;
    while (auto* entry = readdir(peer)) {
        if (std::strcmp(entry->d_name, ".") != 0 && std::strcmp(entry->d_name, "..") != 0) return 9;
    }
    if (errno != 0) return 10;
    printf("PASS peer=wos-3 absent retained-id=%.*s metadata=stable directory=empty scans=%u\n", 6, expected.data(), scans);
    return 0;
}
