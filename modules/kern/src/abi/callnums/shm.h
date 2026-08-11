#pragma once
#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::abi::shm {

// Syscall operation selectors are carried in 64-bit registers.
// NOLINTNEXTLINE(performance-enum-size)
enum class ops : uint64_t {
    GET,
    ATTACH,
    DETACH,
    CTL,
};

struct IpcPerm {
    int32_t key;
    uint32_t uid;
    uint32_t gid;
    uint32_t cuid;
    uint32_t cgid;
    uint32_t mode;
    int32_t seq;
    std::array<long, 2> unused;
};

struct ShmidDs {
    IpcPerm shm_perm;
    size_t shm_segsz;
    long shm_atime;
    long shm_dtime;
    long shm_ctime;
    int64_t shm_cpid;
    int64_t shm_lpid;
    unsigned long shm_nattch;
    std::array<unsigned long, 2> unused;
};

constexpr int IPC_PRIVATE = 0;
constexpr int IPC_CREAT = 01000;
constexpr int IPC_EXCL = 02000;
constexpr int IPC_RMID = 0;
constexpr int IPC_STAT = 2;

constexpr int SHM_RDONLY = 010000;

static_assert(sizeof(IpcPerm) == 48);
static_assert(alignof(IpcPerm) == 8);
static_assert(offsetof(IpcPerm, key) == 0);
static_assert(offsetof(IpcPerm, uid) == 4);
static_assert(offsetof(IpcPerm, gid) == 8);
static_assert(offsetof(IpcPerm, cuid) == 12);
static_assert(offsetof(IpcPerm, cgid) == 16);
static_assert(offsetof(IpcPerm, mode) == 20);
static_assert(offsetof(IpcPerm, seq) == 24);
static_assert(offsetof(IpcPerm, unused) == 32);

static_assert(sizeof(ShmidDs) == 120);
static_assert(alignof(ShmidDs) == 8);
static_assert(offsetof(ShmidDs, shm_perm) == 0);
static_assert(offsetof(ShmidDs, shm_segsz) == 48);
static_assert(offsetof(ShmidDs, shm_atime) == 56);
static_assert(offsetof(ShmidDs, shm_dtime) == 64);
static_assert(offsetof(ShmidDs, shm_ctime) == 72);
static_assert(offsetof(ShmidDs, shm_cpid) == 80);
static_assert(offsetof(ShmidDs, shm_lpid) == 88);
static_assert(offsetof(ShmidDs, shm_nattch) == 96);
static_assert(offsetof(ShmidDs, unused) == 104);

}  // namespace ker::abi::shm
