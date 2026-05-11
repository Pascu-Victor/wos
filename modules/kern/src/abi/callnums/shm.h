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
    int32_t shm_cpid;
    int32_t shm_lpid;
    unsigned long shm_nattch;
    std::array<unsigned long, 2> unused;
};

constexpr int IPC_PRIVATE = 0;
constexpr int IPC_CREAT = 01000;
constexpr int IPC_EXCL = 02000;
constexpr int IPC_RMID = 0;
constexpr int IPC_STAT = 2;

constexpr int SHM_RDONLY = 010000;

}  // namespace ker::abi::shm
