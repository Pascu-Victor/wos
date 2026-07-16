#include "remote_vfs.hpp"

#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <iterator>
#include <memory>
#include <net/wki/dev_server.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/transport_roce.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/mm.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/mutex.hpp>
#include <util/hcf.hpp>
#include <utility>
#include <vfs/mount.hpp>
#include <vfs/vfs.hpp>

#include "platform/sys/spinlock.hpp"
#include "vfs/file.hpp"
#include "vfs/file_operations.hpp"
#include "vfs/stat.hpp"

namespace ker::net::wki {

// -------------------------------------------------------------------------------
// Storage
// -------------------------------------------------------------------------------

namespace {

template <typename T, size_t N>
auto raw_data(T (&buffer)[N]) -> T* {  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
    return &buffer[0];
}

template <typename T, size_t N>
auto raw_data(const T (&buffer)[N]) -> const T* {  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
    return &buffer[0];
}

auto transport_supports_vfs_write_push_rdma(const WkiTransport* transport) -> bool {
    if (transport == nullptr || transport->name == nullptr) {
        return false;
    }

    // ivshmem: rdma_write is a local memcpy — payload is visible before the control
    // response is even enqueued.
    //
    // RoCE: the control request is sent first and the server waits for the
    // receive region to fill before writing into VFS.
    return std::strcmp(transport->name, "wki-ivshmem") == 0 || std::strcmp(transport->name, "wki-roce") == 0;
}

auto transport_is_roce(const WkiTransport* transport) -> bool {
    return transport != nullptr && transport->name != nullptr && std::strcmp(transport->name, "wki-roce") == 0;
}

auto transport_supports_vfs_read_push_rdma(const WkiTransport* transport) -> bool {
    if (transport == nullptr || transport->name == nullptr) {
        return false;
    }

    // In read-push mode the server has already written into our local RDMA
    // region, and the consumer-side "rdma_read" below is just a local sync/copy.
    // That is true for ivshmem, but not for RoCE: a RoCE rdma_read would issue a
    // network read against neighbor 0/local rkey, fail, and leave stale zeros in
    // the bounce buffer. RoCE read-push is handled by the explicit tagged-write
    // path below, not by this ivshmem local-copy helper.
    return std::strcmp(transport->name, "wki-ivshmem") == 0;
}

// Returns true for transports where the safe VFS read model is client-pull:
// server stages data locally, client issues rdma_read (synchronous, spin-waits
// for DOORBELL completion) to fetch it after receiving the control response.
auto transport_supports_rdma_read_pull(const WkiTransport* transport) -> bool {
    if (transport == nullptr || transport->name == nullptr) {
        return false;
    }
    return std::strcmp(transport->name, "wki-roce") == 0;
}

constexpr uint64_t VFS_PROXY_CONTENTION_SLEEP_US = 1000;
// Remote compilers can keep the exporting node CPU-bound long enough for a
// response to arrive just after the generic WKI timeout. Retrying immediately
// then leaves the proxy one sequence behind forever, so normal remote-VFS ops
// use the same patience budget as remote task submit. Readlink keeps its
// smaller interactive timeout schedule below.
constexpr uint64_t VFS_PROXY_OP_TIMEOUT_US = 60'000'000;
constexpr uint64_t VFS_PROXY_SLOT_WAIT_TIMEOUT_US = VFS_PROXY_OP_TIMEOUT_US;
// Extra mount lanes improve aggregate throughput but are not required for a
// usable mount.  Do not let one unavailable auxiliary binding turn mount
// startup into several full RPC timeout windows.
constexpr uint64_t VFS_PROXY_AUX_ATTACH_TIMEOUT_US = 5'000'000;
static_assert(sizeof(DevOpReqPayload) <= WKI_ETH_MAX_PAYLOAD);
static_assert(WKI_ETH_MAX_PAYLOAD <= UINT16_MAX);
static_assert(WKI_ETH_MAX_PAYLOAD <= ker::mod::mm::KERNEL_STACK_SIZE / 16);

constexpr uint32_t VFS_READDIR_BATCH_MAX_ENTRIES =
    static_cast<uint32_t>((WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload) - sizeof(uint32_t)) / sizeof(ker::vfs::DirEntry));
constexpr size_t VFS_READDIR_BATCH_DATA_CAPACITY =
    sizeof(uint32_t) + (static_cast<size_t>(VFS_READDIR_BATCH_MAX_ENTRIES) * sizeof(ker::vfs::DirEntry));
constexpr size_t VFS_READDIR_BATCH_RESPONSE_CAPACITY = sizeof(DevOpRespPayload) + VFS_READDIR_BATCH_DATA_CAPACITY;
static_assert(VFS_READDIR_BATCH_MAX_ENTRIES > 0);
static_assert(VFS_READDIR_BATCH_RESPONSE_CAPACITY <= WKI_ETH_MAX_PAYLOAD);
static_assert(VFS_READDIR_BATCH_DATA_CAPACITY <= UINT16_MAX);

constexpr auto vfs_readdir_batch_payload_is_valid(uint16_t payload_len, uint32_t count) -> bool {
    if (count > VFS_READDIR_BATCH_MAX_ENTRIES) {
        return false;
    }
    size_t const REQUIRED_LEN = sizeof(uint32_t) + (static_cast<size_t>(count) * sizeof(ker::vfs::DirEntry));
    return REQUIRED_LEN <= payload_len;
}
static_assert(vfs_readdir_batch_payload_is_valid(sizeof(uint32_t), 0));
static_assert(vfs_readdir_batch_payload_is_valid(static_cast<uint16_t>(VFS_READDIR_BATCH_DATA_CAPACITY), VFS_READDIR_BATCH_MAX_ENTRIES));
static_assert(!vfs_readdir_batch_payload_is_valid(static_cast<uint16_t>(VFS_READDIR_BATCH_DATA_CAPACITY - 1),
                                                  VFS_READDIR_BATCH_MAX_ENTRIES));
static_assert(!vfs_readdir_batch_payload_is_valid(UINT16_MAX, VFS_READDIR_BATCH_MAX_ENTRIES + 1));

auto remote_vfs_strip_mount_prefix(const ker::vfs::MountPoint* mount, const char* path) -> const char* {
    if (mount == nullptr || path == nullptr) {
        return path;
    }

    size_t const MOUNT_LEN = mount->path_len;
    if (MOUNT_LEN == 1 && mount->path[0] == '/') {
        return path + 1;
    }
    if (path[MOUNT_LEN] == '/') {
        return path + MOUNT_LEN + 1;
    }
    if (path[MOUNT_LEN] == '\0') {
        return "";
    }
    return path + MOUNT_LEN;
}

auto remote_vfs_try_copy_cached_stat(RemoteFileContext* ctx, ker::vfs::Stat* statbuf) -> bool {
    if (ctx == nullptr || statbuf == nullptr) {
        return false;
    }

    ctx->stat_cache_lock.lock();
    bool const VALID = ctx->stat_cache_valid;
    if (VALID) {
        *statbuf = ctx->stat_cache;
    }
    ctx->stat_cache_lock.unlock();
    return VALID;
}

void remote_vfs_store_cached_stat(RemoteFileContext* ctx, const ker::vfs::Stat& statbuf) {
    if (ctx == nullptr) {
        return;
    }

    ctx->stat_cache_lock.lock();
    ctx->stat_cache = statbuf;
    ctx->stat_cache_valid = true;
    ctx->stat_cache_lock.unlock();
}

void remote_vfs_invalidate_cached_stat(RemoteFileContext* ctx) {
    if (ctx == nullptr) {
        return;
    }

    ctx->stat_cache_lock.lock();
    ctx->stat_cache_valid = false;
    ctx->stat_cache_lock.unlock();
}

struct OpenRespPayload {
    int32_t fd = -1;
    uint8_t is_dir = 0;
    uint8_t has_stat = 0;
    ker::vfs::Stat stat = {};
    uint32_t prefetched_bytes = 0;
} __attribute__((packed));

constexpr uint16_t OPEN_REQ_BASE_LEN = 10;
constexpr uint16_t OPEN_PREFETCH_REQ_LEN = 8;
constexpr size_t OPEN_REQ_INLINE_CAPACITY = OPEN_REQ_BASE_LEN + ker::vfs::MOUNT_PATH_MAX + OPEN_PREFETCH_REQ_LEN;
static_assert(OPEN_REQ_INLINE_CAPACITY <= WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload));
constexpr uint16_t OPEN_RESP_NO_STAT_LEN = static_cast<uint16_t>(offsetof(OpenRespPayload, stat));
constexpr uint16_t OPEN_RESP_WITH_STAT_LEN = static_cast<uint16_t>(offsetof(OpenRespPayload, prefetched_bytes));
constexpr uint16_t OPEN_RESP_WITH_PREFETCH_LEN = static_cast<uint16_t>(sizeof(OpenRespPayload));

auto perf_current_pid() -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? task->pid : 0;
}

auto perf_current_cpu() -> uint32_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? static_cast<uint32_t>(task->cpu) : 0U;
}

struct PreservedExportIdentity {
    uint32_t resource_id = 0;
    uint32_t resource_incarnation = 0;
};

struct ExportBackingIdentity {
    uint32_t dev_id = 0;
    ker::vfs::FSType fs_type = ker::vfs::FSType::TMPFS;
};

auto export_backing_identity_matches(const VfsExport& export_entry, const ExportBackingIdentity& backing) -> bool {
    return backing.dev_id != 0 && export_entry.backing_dev_id == backing.dev_id && export_entry.backing_fs_type == backing.fs_type;
}

auto take_preserved_export_identity(std::deque<VfsExport>* stale_exports, const char* name, const char* export_path,
                                    const ExportBackingIdentity& backing) -> PreservedExportIdentity {
    if (stale_exports == nullptr || name == nullptr || export_path == nullptr) {
        return {};
    }

    for (auto it = stale_exports->begin(); it != stale_exports->end(); ++it) {
        if (!it->active || std::strncmp(static_cast<const char*>(it->name), name, VFS_EXPORT_NAME_LEN) != 0 ||
            std::strncmp(static_cast<const char*>(it->export_path), export_path, VFS_EXPORT_PATH_LEN) != 0 ||
            !export_backing_identity_matches(*it, backing)) {
            continue;
        }
        PreservedExportIdentity const IDENTITY = {
            .resource_id = it->resource_id,
            .resource_incarnation = it->resource_incarnation,
        };
        stale_exports->erase(it);
        return IDENTITY;
    }

    return {};
}

auto max_preserved_export_id(const std::deque<VfsExport>& stale_exports) -> uint32_t {
    uint32_t max_resource_id = 0;
    for (const auto& exp : stale_exports) {
        if (exp.active && exp.resource_id > max_resource_id) {
            max_resource_id = exp.resource_id;
        }
    }
    return max_resource_id;
}

auto perf_vfs_op(uint16_t op_id) -> uint8_t {
    switch (op_id) {
        case OP_VFS_OPEN:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::OPEN);
        case OP_VFS_STAT:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::STAT);
        case OP_VFS_CLOSE:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::CLOSE);
        case OP_VFS_MKDIR:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::MKDIR);
        case OP_VFS_CHMOD:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::CHMOD);
        case OP_VFS_UTIMENS:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::UTIMENS);
        case OP_VFS_WRITE:
        case OP_VFS_WRITE_RDMA:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::WRITE);
        case OP_VFS_READ_RDMA:
        case OP_VFS_READ_BULK:
        case OP_VFS_READ:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::READ);
        case OP_VFS_READDIR_BATCH:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::READDIR);
        case OP_VFS_SEEK_END:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::SEEK);
        case OP_VFS_TRUNCATE:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::TRUNCATE);
        case OP_VFS_UNLINK:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::UNLINK);
        case OP_VFS_RMDIR:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::RMDIR);
        case OP_VFS_RENAME:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::RENAME);
        case OP_VFS_READLINK:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::READLINK);
        default:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::RETRY);
    }
}

auto perf_vfs_bytes(uint16_t op_id, uint16_t req_len, uint16_t resp_len) -> uint64_t {
    switch (op_id) {
        case OP_VFS_READ:
        case OP_VFS_READ_RDMA:
        case OP_VFS_READ_BULK:
        case OP_VFS_READDIR_BATCH:
        case OP_VFS_READLINK:
        case OP_VFS_STAT:
            return resp_len;
        case OP_VFS_WRITE:
        case OP_VFS_WRITE_RDMA:
            return req_len;
        default:
            return std::max<uint16_t>(req_len, resp_len);
    }
}

auto perf_vfs_server_op(uint16_t op_id) -> uint8_t {
    switch (op_id) {
        case OP_VFS_OPEN:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::OPEN);
        case OP_VFS_STAT:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::STAT);
        case OP_VFS_CLOSE:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::CLOSE);
        case OP_VFS_MKDIR:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::MKDIR);
        case OP_VFS_CHMOD:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::CHMOD);
        case OP_VFS_UTIMENS:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::UTIMENS);
        case OP_VFS_WRITE:
        case OP_VFS_WRITE_RDMA:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::WRITE);
        case OP_VFS_READ_RDMA:
        case OP_VFS_READ_BULK:
        case OP_VFS_READ:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::READ);
        case OP_VFS_READDIR:
        case OP_VFS_READDIR_BATCH:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::READDIR);
        case OP_VFS_SEEK_END:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::SEEK);
        case OP_VFS_TRUNCATE:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::TRUNCATE);
        case OP_VFS_UNLINK:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::UNLINK);
        case OP_VFS_RMDIR:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::RMDIR);
        case OP_VFS_RENAME:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::RENAME);
        case OP_VFS_READLINK:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::READLINK);
        default:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::RX);
    }
}

constexpr int VFS_PROXY_WKI_ERR_BASE = -1000;
// Bulk VFS still moves one large RDMA window, but fill the server buffer through
// modest backing-FS reads so one remote request cannot force a giant local direct read.
constexpr size_t VFS_BACKING_READ_CHUNK = size_t{32} * 1024;
// Positional reads from file-backed mmap should prefetch enough to amortize
// page-fault latency. Match the current RoCE pull window so one mmap fill uses
// the largest transport-safe bulk transfer we already allocated.
constexpr uint32_t VFS_POSITIONAL_PREFETCH_SIZE = VFS_RDMA_ROCE_BULK_SIZE;

auto encode_proxy_wki_status(int status) -> int {
    if (status >= 0) {
        return status;
    }
    return VFS_PROXY_WKI_ERR_BASE + status;
}

auto is_encoded_proxy_wki_status(int status) -> bool {
    return status <= (VFS_PROXY_WKI_ERR_BASE + WKI_ERR_NO_MEM) && status >= (VFS_PROXY_WKI_ERR_BASE + WKI_ERR_TX_FAILED);
}

auto decode_proxy_wki_status(int status) -> int {
    if (!is_encoded_proxy_wki_status(status)) {
        return status;
    }
    return status - VFS_PROXY_WKI_ERR_BASE;
}

auto normalize_proxy_status_for_errno(int status) -> int {
    if (!is_encoded_proxy_wki_status(status)) {
        return status;
    }

    switch (decode_proxy_wki_status(status)) {
        case WKI_ERR_NO_MEM:
            return -ENOMEM;
        case WKI_ERR_NO_ROUTE:
        case WKI_ERR_NOT_FOUND:
            return -EHOSTUNREACH;
        case WKI_ERR_PEER_FENCED:
            return -ENOTCONN;
        case WKI_ERR_NO_CREDITS:
        case WKI_ERR_BUSY:
            return -EAGAIN;
        case WKI_ERR_TIMEOUT:
            return -ETIMEDOUT;
        case WKI_ERR_INVALID:
            return -EINVAL;
        case WKI_ERR_TX_FAILED:
            return -EIO;
        default:
            return -EIO;
    }
}

void perf_record_vfs_point(uint8_t op, uint16_t peer, uint16_t channel, int32_t status, uint32_t aux, uint64_t callsite) {
    if (!ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }

    uint32_t const CORRELATION = ker::mod::perf::next_wki_trace_correlation();
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS, op,
                                     ker::mod::perf::WkiPerfPhase::POINT, peer, channel, CORRELATION, status, aux, callsite);
    uint32_t const LATENCY_US = (op == static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::ATTACH_WAIT) ||
                                 op == static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::PROXY_WAIT))
                                    ? aux
                                    : 0U;
    bool const HAS_LATENCY = op == static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::ATTACH_WAIT) ||
                             op == static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::PROXY_WAIT);
    uint32_t const RETRIES = (op == static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::RETRY)) ? 1U : 0U;
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, op, peer, channel, status, LATENCY_US, HAS_LATENCY,
                                       RETRIES, 0);
}

void perf_record_vfs_server_point(uint8_t op, uint16_t peer, uint16_t channel, uint32_t correlation, int32_t status, uint32_t aux,
                                  uint64_t callsite) {
    if (!ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }

    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS_SERVER, op,
                                     ker::mod::perf::WkiPerfPhase::POINT, peer, channel, correlation, status, aux, callsite);
}

void perf_record_vfs_server_begin(uint8_t op, uint16_t peer, uint16_t channel, uint32_t correlation, uint64_t callsite) {
    if (!ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }

    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS_SERVER, op,
                                     ker::mod::perf::WkiPerfPhase::BEGIN, peer, channel, correlation, 0, 0, callsite);
}

void perf_record_vfs_server_end(uint8_t op, uint16_t peer, uint16_t channel, uint32_t correlation, int32_t status, uint32_t latency_us,
                                uint64_t bytes, uint64_t callsite) {
    if (!ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }

    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS_SERVER, op,
                                     ker::mod::perf::WkiPerfPhase::END, peer, channel, correlation, status, latency_us, callsite);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS_SERVER, op, peer, channel, status, latency_us, true, 0,
                                       bytes);
}

// -----------------------------------------------------------------------------
// Server side
// -----------------------------------------------------------------------------

std::deque<VfsExport> g_vfs_exports;                  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<RemoteVfsFd> g_remote_fds;                 // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
int32_t g_next_remote_fd = 1;                         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_next_vfs_resource_id = 0x1000;             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_next_vfs_resource_incarnation = 1;         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_vfs_export_revision = 2;                   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_vfs_export_target_revision = 0;            // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_vfs_export_rebuild_prepared = false;           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_vfs_export_rebuild_accepting_entries = false;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto vfs_channel_identity_matches(const WkiChannelIdentity& expected, const WkiChannelIdentity& actual) -> bool {
    return expected.channel != nullptr && expected.channel == actual.channel && expected.peer_node_id == actual.peer_node_id &&
           expected.channel_id == actual.channel_id && expected.generation != 0 && expected.generation == actual.generation;
}

auto vfs_channel_identity_matches_header(const WkiHeader* hdr, const WkiChannelIdentity& identity) -> bool {
    return hdr != nullptr && identity.channel != nullptr && identity.generation != 0 && hdr->src_node == identity.peer_node_id &&
           hdr->channel_id == identity.channel_id;
}

auto find_remote_fd(const WkiChannelIdentity& channel_identity, int32_t fd_id) -> RemoteVfsFd* {
    for (auto& rfd : g_remote_fds) {
        if (rfd.active && !rfd.retiring && rfd.fd_id == fd_id && vfs_channel_identity_matches(rfd.channel_identity, channel_identity)) {
            return &rfd;
        }
    }
    return nullptr;
}

auto alloc_remote_fd(const WkiChannelIdentity& channel_identity, ker::vfs::File* file) -> int32_t {
    int32_t const FD_ID = g_next_remote_fd++;

    RemoteVfsFd rfd;
    rfd.active = true;
    rfd.consumer_node = channel_identity.peer_node_id;
    rfd.channel_identity = channel_identity;
    rfd.fd_id = FD_ID;
    rfd.file = file;
    rfd.last_activity_us = wki_now_us();

    g_remote_fds.push_back(rfd);
    return FD_ID;
}

// D10: Update last_activity_us on a remote FD
void touch_remote_fd(RemoteVfsFd* rfd) {
    if (rfd != nullptr) {
        rfd->last_activity_us = wki_now_us();
    }
}

auto relative_wire_path_has_safe_components(const uint8_t* path, size_t path_len) -> bool {
    if (path_len == 0) {
        return true;
    }
    if (path == nullptr || path[0] == '/' || std::memchr(path, '\0', path_len) != nullptr) {
        return false;
    }

    size_t component_start = 0;
    for (size_t pos = 0; pos <= path_len; ++pos) {
        if (pos != path_len && path[pos] != '/') {
            continue;
        }
        size_t const COMPONENT_LEN = pos - component_start;
        bool const DOT = COMPONENT_LEN == 1 && path[component_start] == '.';
        bool const DOT_DOT = COMPONENT_LEN == 2 && path[component_start] == '.' && path[component_start + 1] == '.';
        if (DOT || DOT_DOT) {
            return false;
        }
        component_start = pos + 1;
    }
    return true;
}

// Build full absolute path from export_path + relative_path
void build_full_path(char* out, size_t out_size, const char* export_path, const char* relative_path, uint16_t rel_len) {
    size_t const EXPORT_LEN = strlen(export_path);

    // Copy export path
    size_t pos = 0;
    if (EXPORT_LEN > 0 && EXPORT_LEN < out_size - 1) {
        memcpy(out, export_path, EXPORT_LEN);
        pos = EXPORT_LEN;
    }

    // Add separator if needed
    if (pos > 0 && out[pos - 1] != '/' && rel_len > 0) {
        if (pos < out_size - 1) {
            out[pos++] = '/';
        }
    }

    // Copy relative path
    size_t copy_len = rel_len;
    if (pos + copy_len >= out_size) {
        copy_len = out_size - pos - 1;
    }
    if (copy_len > 0) {
        memcpy(out + pos, relative_path, copy_len);
        pos += copy_len;
    }

    out[pos] = '\0';
}

void build_export_name(char* out, size_t out_size, const char* export_path) {
    if (out == nullptr || out_size == 0 || export_path == nullptr) {
        return;
    }

    const char* visible_path = export_path;
    if (ker::mod::sched::has_run_queues()) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            size_t const ROOT_LEN = std::strlen(task->root.data());
            if (ROOT_LEN > 1 && std::strncmp(export_path, task->root.data(), ROOT_LEN) == 0 &&
                (export_path[ROOT_LEN] == '\0' || export_path[ROOT_LEN] == '/')) {
                visible_path = export_path + ROOT_LEN;
                if (*visible_path == '\0') {
                    visible_path = "/";
                }
            }
        }
    }

    std::snprintf(out, out_size, "%s", visible_path);
}

auto read_local_file_with_retry(ker::vfs::File* local_file, void* buf, size_t len, size_t offset) -> ssize_t {
    if (local_file == nullptr || local_file->fops == nullptr || local_file->fops->vfs_read == nullptr) {
        return -EIO;
    }

    constexpr int MAX_ATTEMPTS = 3;
    bool checked_size = false;
    bool expect_more = false;
    ker::vfs::Stat statbuf{};

    for (int attempt = 1; attempt <= MAX_ATTEMPTS; ++attempt) {
        ssize_t const RC = local_file->fops->vfs_read(local_file, buf, len, offset);
        if (RC > 0) {
            return RC;
        }
        if (RC < 0) {
            if (attempt < MAX_ATTEMPTS && RC == -EIO) {
                ker::mod::sched::kern_yield();
                continue;
            }
            return RC;
        }

        if (!checked_size && local_file->vfs_path != nullptr) {
            checked_size = true;
            if (ker::vfs::vfs_stat(local_file->vfs_path, &statbuf) == 0 && statbuf.st_size >= 0) {
                auto const FILE_SIZE = static_cast<size_t>(statbuf.st_size);
                if (offset < FILE_SIZE) {
                    expect_more = true;
                }
            }
        }

        if (!expect_more) {
            return 0;
        }

        if (attempt < MAX_ATTEMPTS) {
            ker::mod::sched::kern_yield();
            continue;
        }

        char const* path_for_log = local_file->vfs_path != nullptr ? local_file->vfs_path : "?";
        ker::mod::dbg::log("[WKI] premature EOF on backing read path='%s' off=%zu size=%lld", path_for_log, offset,
                           static_cast<long long>(statbuf.st_size));
        return -EIO;
    }

    return -EIO;
}

auto dev_attach_status_to_errno(uint8_t status) -> int {
    switch (static_cast<DevAttachStatus>(status)) {
        case DevAttachStatus::OK:
            return 0;
        case DevAttachStatus::NOT_FOUND:
            return -ENOENT;
        case DevAttachStatus::NOT_REMOTABLE:
            return -ENODEV;
        case DevAttachStatus::BUSY:
            return -EBUSY;
        case DevAttachStatus::NO_PASSTHROUGH:
            return -EOPNOTSUPP;
        case DevAttachStatus::STALE_RESOURCE:
            return -ESTALE;
    }
    return -EIO;
}

auto read_local_file_windowed(ker::vfs::File* local_file, void* buf, size_t len, size_t offset) -> ssize_t {
    auto* dst = static_cast<uint8_t*>(buf);
    size_t total_read = 0;

    while (total_read < len) {
        size_t const CHUNK = std::min(VFS_BACKING_READ_CHUNK, len - total_read);
        ssize_t const RC = read_local_file_with_retry(local_file, dst + total_read, CHUNK, offset + total_read);
        if (RC < 0) {
            return total_read > 0 ? static_cast<ssize_t>(total_read) : RC;
        }
        if (RC == 0) {
            break;
        }
        total_read += static_cast<size_t>(RC);
    }

    return static_cast<ssize_t>(total_read);
}

// Check if the resolved server-side path would cross into a REMOTE (WKI proxy) mount.
// This prevents recursive proxying: e.g. a client reads /wki/nodeA/wki/nodeB/... which
// would cause the server to proxy through its own WKI mounts, creating a chain that
// times out or loops.
bool path_crosses_remote_mount(const char* path) {
    if (path == nullptr) {
        return false;
    }

    std::array<char, 512> resolved_path __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
    const char* mount_path = path;
    if (ker::vfs::resolve_mount_path(path, resolved_path.data(), resolved_path.size()) == 0) {
        mount_path = resolved_path.data();
    }

    auto mount_ref = ker::vfs::find_mount_point(mount_path);
    auto* mp = mount_ref.get();
    return mp != nullptr && mp->fs_type == ker::vfs::FSType::REMOTE;
}

bool path_crosses_remote_mount_direct(const char* path) {
    if (path == nullptr) {
        return false;
    }

    auto mount_ref = ker::vfs::find_mount_point(path);
    auto* mp = mount_ref.get();
    return mp != nullptr && mp->fs_type == ker::vfs::FSType::REMOTE;
}

bool path_crosses_recursive_self_alias(const char* path) {
    if (path == nullptr) {
        return false;
    }

    constexpr const char* HOST_PREFIX = "/wki/host";
    constexpr size_t HOST_PREFIX_LEN = 9;
    if (std::strncmp(path, HOST_PREFIX, HOST_PREFIX_LEN) == 0 && (path[HOST_PREFIX_LEN] == '\0' || path[HOST_PREFIX_LEN] == '/')) {
        return true;
    }

    std::array<char, WKI_HOSTNAME_MAX + 6> self_prefix{};
    std::snprintf(self_prefix.data(), self_prefix.size(), "/wki/%s", g_wki.local_hostname.data());
    size_t const SELF_PREFIX_LEN = std::strlen(self_prefix.data());
    return std::strncmp(path, self_prefix.data(), SELF_PREFIX_LEN) == 0 && (path[SELF_PREFIX_LEN] == '\0' || path[SELF_PREFIX_LEN] == '/');
}

bool path_crosses_recursive_wki_boundary(const char* path) {
    return path_crosses_remote_mount(path) || path_crosses_recursive_self_alias(path);
}

bool path_crosses_recursive_wki_boundary_direct(const char* backing_path, const char* visible_path) {
    return path_crosses_remote_mount_direct(backing_path) || path_crosses_recursive_self_alias(visible_path);
}

bool should_hide_recursive_wki_entry(const RemoteFileContext* ctx, const ker::vfs::DirEntry& entry) {
    if (ctx == nullptr || !ctx->hide_recursive_wki_entries) {
        return false;
    }

    if (std::strcmp(entry.d_name.data(), "host") == 0) {
        return true;
    }

    return ctx->recursive_wki_hostname.front() != '\0' && std::strcmp(entry.d_name.data(), ctx->recursive_wki_hostname.data()) == 0;
}

// -----------------------------------------------------------------------------
// Consumer side
// -----------------------------------------------------------------------------

// D7: Directory listing cache - avoids repeated round-trips for readdir()
struct DirCacheEntry {
    ProxyVfsState* proxy = nullptr;
    int32_t remote_fd = -1;
    uint64_t cache_time_us = 0;
    bool complete = false;  // true if we fetched all entries (server returned error/empty)
    std::deque<ker::vfs::DirEntry> entries;

    static constexpr uint64_t STALE_US = 5000000;  // 5 seconds
};

bool try_get_visible_cached_dirent(const DirCacheEntry* cache, const RemoteFileContext* ctx, size_t visible_index,
                                   ker::vfs::DirEntry* out) {
    if (cache == nullptr || out == nullptr) {
        return false;
    }

    size_t current_visible = 0;
    for (const auto& candidate : cache->entries) {
        if (should_hide_recursive_wki_entry(ctx, candidate)) {
            continue;
        }
        if (current_visible == visible_index) {
            *out = candidate;
            return true;
        }
        current_visible++;
    }

    return false;
}

std::deque<DirCacheEntry> g_dir_cache;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto find_dir_cache(ProxyVfsState* proxy, int32_t remote_fd) -> DirCacheEntry* {
    for (auto& dc : g_dir_cache) {
        if (dc.proxy == proxy && dc.remote_fd == remote_fd) {
            return &dc;
        }
    }
    return nullptr;
}

void invalidate_dir_cache(ProxyVfsState* proxy, int32_t remote_fd) {
    std::erase_if(g_dir_cache, [&](const DirCacheEntry& dc) { return dc.proxy == proxy && dc.remote_fd == remote_fd; });
}

void invalidate_all_dir_caches(ProxyVfsState* proxy) {
    std::erase_if(g_dir_cache, [&](const DirCacheEntry& dc) { return dc.proxy == proxy; });
}

std::deque<std::unique_ptr<ProxyVfsState>> g_vfs_proxies;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remote_vfs_initialized = false;                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint8_t g_vfs_attach_next_cookie = 1;                      // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_vfs_next_mount_group_id = 1;                    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ProxyVfsState* g_pending_vfs_detach_head = nullptr;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ProxyVfsState* g_pending_vfs_detach_tail = nullptr;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

ker::mod::sys::Spinlock s_vfs_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

constexpr size_t VFS_DETACH_RETRY_BATCH = 32;
constexpr size_t VFS_DETACH_RETRY_SCAN = VFS_DETACH_RETRY_BATCH * 2;

// Caller must hold s_vfs_lock.
auto allocate_vfs_mount_group_id_locked() -> uint64_t {
    uint64_t group_id = g_vfs_next_mount_group_id++;
    if (group_id == 0) {
        group_id = g_vfs_next_mount_group_id++;
    }
    if (group_id == 0) [[unlikely]] {
        ker::mod::dbg::panic_handler("WKI remote VFS: mount group id exhausted");
        hcf();
    }
    return group_id;
}

// Caller must hold s_vfs_lock.
auto create_vfs_proxy_state_locked(uint16_t owner_node, uint32_t resource_id, uint64_t resource_generation,
                                   const ResourceIncarnationToken& owner_incarnation, uint32_t binding_peer_boot_epoch,
                                   const char* local_mount_path, uint64_t mount_group_id, uint8_t lane_index, bool lane_anchor)
    -> ProxyVfsState* {
    g_vfs_proxies.push_back(std::make_unique<ProxyVfsState>());
    auto* state = g_vfs_proxies.back().get();

    state->owner_node = owner_node;
    state->resource_id = resource_id;
    state->resource_generation = resource_generation;
    state->owner_incarnation = owner_incarnation;
    state->binding_peer_boot_epoch = binding_peer_boot_epoch;
    state->mount_group_id = mount_group_id;
    state->lane_index = lane_index;
    state->lane_anchor = lane_anchor;
    state->lifecycle_refs = 1;

    size_t path_len = std::strlen(local_mount_path);
    if (path_len >= VFS_EXPORT_PATH_LEN) {
        path_len = VFS_EXPORT_PATH_LEN - 1;
    }
    std::memcpy(state->local_mount_path.data(), local_mount_path, path_len);
    state->local_mount_path.at(path_len) = '\0';
    return state;
}

// Caller must hold s_vfs_lock.
void mark_vfs_proxy_group_unavailable_locked(ProxyVfsState* state) {
    if (state == nullptr || state->mount_group_id == 0) {
        return;
    }

    for (auto& proxy : g_vfs_proxies) {
        if (proxy == nullptr || proxy->mount_group_id != state->mount_group_id || !proxy->lane_anchor) {
            continue;
        }
        proxy->lanes.fill(nullptr);
        proxy->lane_count = 0;
        proxy->lanes_ready = false;
        return;
    }
}

// Returns a lane with a temporary lifecycle reference.  A hidden lane may be
// retired and erased after the registry lock drops, while its anchor mount is
// still alive, so callers must release the reference after their operation (or
// after transferring to an open-file reference).
auto remote_vfs_rdma_retry_ready(const std::atomic<uint64_t>& retry_after_us, uint64_t now_us) -> bool;

auto vfs_proxy_lane_has_requested_rdma(const ProxyVfsState* candidate, bool require_read, bool require_write, uint64_t now_us) -> bool {
    if (candidate == nullptr) {
        return false;
    }
    bool const HAS_READ_RDMA = (candidate->rdma_capable && !candidate->rdma_read_disabled.load(std::memory_order_acquire) &&
                                remote_vfs_rdma_retry_ready(candidate->rdma_read_retry_after_us, now_us)) ||
                               (candidate->bulk_rdma_capable && !candidate->bulk_rdma_disabled.load(std::memory_order_acquire) &&
                                remote_vfs_rdma_retry_ready(candidate->bulk_rdma_retry_after_us, now_us));
    bool const HAS_WRITE_RDMA = transport_supports_vfs_write_push_rdma(candidate->rdma_transport) && candidate->rdma_server_write_rkey != 0;
    return (!require_read || HAS_READ_RDMA) && (!require_write || HAS_WRITE_RDMA);
}

auto acquire_vfs_proxy_lane(ProxyVfsState* anchor, bool prefer_rdma_read_anchor = false, bool prefer_rdma_write_anchor = false)
    -> ProxyVfsState* {
    if (anchor == nullptr) {
        return nullptr;
    }

    s_vfs_lock.lock();
    ProxyVfsState* selected = nullptr;
    // A remote fops call can only obtain this anchor from a published VFS
    // mount row.  Prepare lane zero before that row is inserted, so do not
    // make selection wait for the post-insert mount bookkeeping bit.
    if (anchor->lane_anchor && anchor->lanes_ready && anchor->active && !anchor->destroy_when_idle && !anchor->resources_releasing &&
        !anchor->resources_released && anchor->lane_count > 0 && anchor->lane_count <= anchor->lanes.size()) {
        std::array<ProxyVfsState*, VFS_PROXY_LANE_COUNT> live_lanes{};
        std::array<ProxyVfsState*, VFS_PROXY_RDMA_LANE_COUNT> rdma_lanes{};
        size_t live_lane_count = 0;
        size_t rdma_lane_count = 0;
        bool const PREFER_RDMA = prefer_rdma_read_anchor || prefer_rdma_write_anchor;
        uint64_t const NOW_US = PREFER_RDMA ? wki_now_us() : 0;

        // Inspect data capability inside the same lifetime boundary as lane
        // selection. O_RDWR requires both directions; a partial registration
        // remains eligible only for the direction it can actually service.
        for (size_t lane_index = 0; lane_index < anchor->lane_count; ++lane_index) {
            auto* candidate = anchor->lanes.at(lane_index);
            if (candidate == nullptr || !candidate->active || candidate->destroy_when_idle || candidate->resources_releasing ||
                candidate->resources_released || candidate->mount_group_id != anchor->mount_group_id ||
                candidate->lane_index != lane_index) {
                continue;
            }
            live_lanes.at(live_lane_count++) = candidate;

            bool const HAS_REQUESTED_RDMA =
                PREFER_RDMA && vfs_proxy_lane_has_requested_rdma(candidate, prefer_rdma_read_anchor, prefer_rdma_write_anchor, NOW_US);
            if (PREFER_RDMA && HAS_REQUESTED_RDMA && rdma_lane_count < rdma_lanes.size()) {
                rdma_lanes.at(rdma_lane_count++) = candidate;
            }
        }

        uint64_t const PID = perf_current_pid();
        if (PREFER_RDMA && rdma_lane_count > 0) {
            size_t const INDEX = PID != 0 ? static_cast<size_t>(PID % rdma_lane_count) : 0;
            selected = rdma_lanes.at(INDEX);
        } else if (live_lane_count > 0) {
            // Allocation failure and transport cooldown preserve a usable,
            // task-striped message path instead of failing the open.
            size_t const INDEX = PID != 0 ? static_cast<size_t>(PID % live_lane_count) : 0;
            selected = live_lanes.at(INDEX);
        }
        if (selected != nullptr) {
            selected->lifecycle_refs++;
        }
    }
    s_vfs_lock.unlock();
    return selected;
}

// Caller must hold s_vfs_lock.
auto vfs_detach_pending_for_resource_locked(uint16_t owner_node, uint32_t resource_id) -> bool {
    for (auto* state = g_pending_vfs_detach_head; state != nullptr; state = state->detach_next) {
        if (state->detach_owner_node == owner_node && state->detach_resource_id == resource_id) {
            return true;
        }
    }
    return false;
}

// Caller must hold s_vfs_lock. The HELLO/RX marker is deliberately limited to
// fixed state mutation, but it still owns admission until task-context cleanup
// either proves an owner reboot or stages the exact detach tuple.
auto vfs_attach_blocked_by_retiring_binding_locked(uint16_t owner_node, uint32_t resource_id) -> bool {
    if (vfs_detach_pending_for_resource_locked(owner_node, resource_id)) {
        return true;
    }
    return std::ranges::any_of(g_vfs_proxies, [owner_node, resource_id](const std::unique_ptr<ProxyVfsState>& proxy) -> bool {
        return proxy != nullptr && proxy->owner_node == owner_node && proxy->resource_id == resource_id && proxy->epoch_reset_pending;
    });
}

struct VfsProxyResponseLookup {
    ProxyVfsState* matched_state = nullptr;
    uint16_t stale_expected_op = 0;
    uint16_t stale_expected_seq = 0;
    size_t active_candidates = 0;
    size_t pending_candidates = 0;
    bool saw_pending_candidate = false;
};

auto find_vfs_proxy_for_response_locked(const WkiChannelIdentity& channel_identity, uint16_t resp_op, uint16_t resp_seq)
    -> VfsProxyResponseLookup {
    VfsProxyResponseLookup lookup = {};

    for (auto& p : g_vfs_proxies) {
        WkiChannelIdentity const EXPECTED_IDENTITY = {
            .channel = p->assigned_channel_ref,
            .peer_node_id = p->owner_node,
            .channel_id = p->assigned_channel,
            .generation = p->assigned_channel_generation,
        };
        if (!p->active || !vfs_channel_identity_matches(EXPECTED_IDENTITY, channel_identity)) {
            continue;
        }

        lookup.active_candidates++;
        if (!p->op_pending.load(std::memory_order_acquire)) {
            continue;
        }

        lookup.pending_candidates++;

        p->lock.lock();
        if (!p->op_pending.load(std::memory_order_acquire)) {
            p->lock.unlock();
            continue;
        }

        bool const OP_MATCH = resp_op == p->op_expected_id;
        // DEV_OP_RESP.reserved carries the originating request sequence cookie.
        // Treating resp_seq==0 as a wildcard lets a late seq-0 reply complete an
        // unrelated later request of the same op on this shared proxy channel.
        bool const SEQ_MATCH = resp_seq == p->op_expected_seq;
        if (OP_MATCH && SEQ_MATCH) {
            lookup.matched_state = p.get();
            return lookup;
        }

        if (!lookup.saw_pending_candidate) {
            lookup.stale_expected_op = p->op_expected_id;
            lookup.stale_expected_seq = p->op_expected_seq;
            lookup.saw_pending_candidate = true;
        }

        p->lock.unlock();
    }

    return lookup;
}

auto find_vfs_proxy_by_attach(uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie) -> ProxyVfsState* {
    for (auto& p : g_vfs_proxies) {
        if (p->attach_pending && p->owner_node == owner_node && p->resource_id == resource_id &&
            p->binding_attach_cookie == attach_cookie) {
            return p.get();
        }
    }
    return nullptr;
}

// Caller must hold s_vfs_lock. The server treats an attach with the same
// owner/resource/incarnation/cookie tuple as an idempotent retransmission.
// Never wrap onto a tuple still represented by a local proxy, or a fresh mount
// could accept the old binding's ACK and later detach that binding.
auto allocate_vfs_attach_cookie_locked(uint16_t owner_node, uint32_t resource_id, const ResourceIncarnationToken& owner_incarnation)
    -> uint8_t {
    for (uint16_t attempt = 0; attempt < UINT8_MAX; ++attempt) {
        uint8_t cookie = g_vfs_attach_next_cookie;
        if (cookie == 0) {
            cookie = 1;
        }
        g_vfs_attach_next_cookie = static_cast<uint8_t>(cookie + 1U);
        if (g_vfs_attach_next_cookie == 0) {
            g_vfs_attach_next_cookie = 1;
        }

        bool const RESERVED = std::ranges::any_of(g_vfs_proxies, [&](const std::unique_ptr<ProxyVfsState>& proxy) {
            if (proxy == nullptr || proxy->owner_node != owner_node || proxy->resource_id != resource_id ||
                proxy->binding_attach_cookie != cookie) {
                return false;
            }
            ResourceIncarnationToken const& EXISTING_INCARNATION = wki_resource_incarnation_valid(proxy->binding_incarnation)
                                                                       ? proxy->binding_incarnation
                                                                       : proxy->attach_expected_incarnation;
            return !wki_resource_incarnation_valid(owner_incarnation) || !wki_resource_incarnation_valid(EXISTING_INCARNATION) ||
                   wki_resource_incarnation_equal(EXISTING_INCARNATION, owner_incarnation);
        });
        bool pending_reserved = false;
        if (!RESERVED) {
            for (auto* proxy = g_pending_vfs_detach_head; proxy != nullptr; proxy = proxy->detach_next) {
                if (proxy->detach_owner_node != owner_node || proxy->detach_resource_id != resource_id ||
                    proxy->detach_attach_cookie != cookie) {
                    continue;
                }
                ResourceIncarnationToken const& PENDING_INCARNATION = proxy->detach_incarnation;
                pending_reserved = !wki_resource_incarnation_valid(owner_incarnation) ||
                                   !wki_resource_incarnation_valid(PENDING_INCARNATION) ||
                                   wki_resource_incarnation_equal(PENDING_INCARNATION, owner_incarnation);
                if (pending_reserved) {
                    break;
                }
            }
        }
        if (!RESERVED && !pending_reserved) {
            return cookie;
        }
    }
    return 0;
}

// Caller must hold s_vfs_lock.
auto vfs_attach_ack_matches_pending_locked(ProxyVfsState const* state, const DevAttachAckPayload& ack, const uint8_t* payload,
                                           uint16_t payload_len) -> bool {
    if (state == nullptr || payload == nullptr || !state->attach_pending.load(std::memory_order_acquire) ||
        state->attach_expected_cookie == 0 || !wki_dev_attach_ack_matches_expected(state->attach_expected_cookie, ack)) {
        return false;
    }
    size_t const EXPECTED_SIZE = sizeof(DevAttachAckPayload) + (state->attach_expect_incarnation ? sizeof(ResourceIncarnationToken) : 0);
    if (payload_len != EXPECTED_SIZE) {
        return false;
    }
    if (!state->attach_expect_incarnation || ack.status != static_cast<uint8_t>(DevAttachStatus::OK)) {
        return true;
    }

    ResourceIncarnationToken ack_incarnation = {};
    std::memcpy(&ack_incarnation, payload + sizeof(DevAttachAckPayload), sizeof(ack_incarnation));
    return wki_resource_incarnation_equal(ack_incarnation, state->attach_expected_incarnation);
}

auto find_vfs_proxy_by_mount(const char* mount_path) -> ProxyVfsState* {
    for (auto& p : g_vfs_proxies) {
        if (p->lane_anchor && p->mount_configured && !p->destroy_when_idle && !p->mount_released &&
            strncmp(p->local_mount_path.data(), mount_path, p->local_mount_path.size()) == 0) {
            return p.get();
        }
    }
    return nullptr;
}

auto find_vfs_proxy_by_channel(const WkiChannelIdentity& channel_identity) -> ProxyVfsState* {
    for (auto& p : g_vfs_proxies) {
        WkiChannelIdentity const EXPECTED_IDENTITY = {
            .channel = p->assigned_channel_ref,
            .peer_node_id = p->owner_node,
            .channel_id = p->assigned_channel,
            .generation = p->assigned_channel_generation,
        };
        if (p->active && vfs_channel_identity_matches(EXPECTED_IDENTITY, channel_identity)) {
            return p.get();
        }
    }
    return nullptr;
}

auto proxy_channel_identity_locked(const ProxyVfsState* state) -> WkiChannelIdentity {
    if (state == nullptr) {
        return {};
    }
    return {
        .channel = state->assigned_channel_ref,
        .peer_node_id = state->owner_node,
        .channel_id = state->assigned_channel,
        .generation = state->assigned_channel_generation,
    };
}

auto peek_channel_tx_seq16(const WkiChannelIdentity& identity, uint16_t* seq_out) -> bool {
    if (seq_out == nullptr || identity.channel == nullptr || identity.generation == 0) {
        return false;
    }

    WkiChannel* ch = identity.channel;
    ch->lock.lock();
    if (!ch->active || ch->peer_node_id != identity.peer_node_id || ch->channel_id != identity.channel_id ||
        ch->generation != identity.generation) {
        ch->lock.unlock();
        return false;
    }
    *seq_out = static_cast<uint16_t>(ch->tx_seq & UINT16_MAX);
    ch->lock.unlock();
    return true;
}

auto capture_peer_channel_identity(uint16_t peer_node, uint16_t channel_id, WkiChannelIdentity* identity_out) -> bool {
    if (identity_out == nullptr) {
        return false;
    }
    *identity_out = {};

    WkiPeer* peer = wki_peer_find(peer_node);
    if (!wki_peer_lifecycle_acquire(peer)) {
        return false;
    }
    if (peer->state != PeerState::CONNECTED || peer->vfs_reset_rebind_pending.load(std::memory_order_acquire)) {
        wki_peer_lifecycle_release(peer);
        return false;
    }

    WkiChannel* channel = wki_channel_get(peer_node, channel_id);
    if (channel != nullptr) {
        channel->lock.lock();
        if (channel->active && channel->peer_node_id == peer_node && channel->channel_id == channel_id && channel->generation != 0) {
            *identity_out = {
                .channel = channel,
                .peer_node_id = peer_node,
                .channel_id = channel_id,
                .generation = channel->generation,
            };
        }
        channel->lock.unlock();
    }
    wki_peer_lifecycle_release(peer);
    return identity_out->channel != nullptr;
}

// Successful symlink targets are typically stable for the lifetime of an
// interactive workload. Keep them around for a long time and rely on explicit
// mutating VFS ops to invalidate the cache, otherwise remote `ls -l` loops end
// up re-paying the same symlink walks every few seconds.
constexpr uint64_t VFS_READLINK_CACHE_SUCCESS_RETENTION_US = 60000000;
constexpr uint64_t VFS_READLINK_CACHE_NEGATIVE_RETENTION_US = 60000000;

auto readlink_cache_retention_us(int status) -> uint64_t {
    return (status == 0) ? VFS_READLINK_CACHE_SUCCESS_RETENTION_US : VFS_READLINK_CACHE_NEGATIVE_RETENTION_US;
}

void reset_readlink_cache_entry(ProxyVfsState::ReadlinkCacheEntry& entry) { entry.generation = 0; }

void invalidate_readlink_cache_locked(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }

    uint32_t const NEXT_GENERATION = state->readlink_cache_generation + 1;
    if (NEXT_GENERATION != 0) [[likely]] {
        state->readlink_cache_generation = NEXT_GENERATION;
        return;
    }

    // Generation zero is reserved for invalid entries. Clear the bounded
    // cache before restarting at one so an entry from the first cycle cannot
    // become visible again after uint32_t wrap.
    for (auto& entry : state->readlink_cache) {
        reset_readlink_cache_entry(entry);
    }
    state->readlink_cache_generation = 1;
}

void invalidate_readlink_cache(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }

    state->lock.lock();
    invalidate_readlink_cache_locked(state);
    state->lock.unlock();
}

// A successful namespace mutation on one lane must invalidate lookups cached
// by every sibling lane before another task is routed there.  The registry
// lock keeps group members alive while their per-lane cache locks are held.
void invalidate_readlink_cache_group(ProxyVfsState* state) {
    if (state == nullptr || state->mount_group_id == 0) {
        invalidate_readlink_cache(state);
        return;
    }

    s_vfs_lock.lock();
    for (auto& proxy : g_vfs_proxies) {
        auto* member = proxy.get();
        if (member == nullptr || member->mount_group_id != state->mount_group_id) {
            continue;
        }
        member->lock.lock();
        invalidate_readlink_cache_locked(member);
        member->lock.unlock();
    }
    s_vfs_lock.unlock();
}

void clear_remote_file_open_prefetch(RemoteFileContext* ctx) {
    if (ctx == nullptr) {
        return;
    }

    delete[] ctx->open_prefetch_buf;
    ctx->open_prefetch_buf = nullptr;
    ctx->open_prefetch_offset = -1;
    ctx->open_prefetch_len = 0;
}

void invalidate_remote_file_open_caches(RemoteFileContext* ctx) {
    if (ctx == nullptr) {
        return;
    }

    clear_remote_file_open_prefetch(ctx);
    remote_vfs_invalidate_cached_stat(ctx);

    if (ctx->read_cache != nullptr) {
        ctx->read_cache->cached_offset = -1;
        ctx->read_cache->cached_len = 0;
    }

    ctx->bulk_cached_offset = -1;
    ctx->bulk_cached_len = 0;
    if (ctx->proxy != nullptr) {
        ctx->proxy->bulk_owner_fd = -1;
    }
}

// RX invalidation only sets cache_invalidation_pending. Task-context callers
// consume it while holding io_lock before touching per-open cache storage.
void consume_remote_file_cache_invalidation(RemoteFileContext* ctx) {
    if (ctx != nullptr && ctx->cache_invalidation_pending.exchange(false, std::memory_order_acq_rel)) {
        invalidate_remote_file_open_caches(ctx);
    }
}

auto path_matches_export(const char* export_path, const char* local_vfs_path) -> bool {
    if (export_path == nullptr || local_vfs_path == nullptr) {
        return false;
    }

    size_t const EXPORT_LEN = std::strlen(export_path);
    if (EXPORT_LEN == 0) {
        return false;
    }
    if (std::strncmp(export_path, local_vfs_path, EXPORT_LEN) != 0) {
        return false;
    }
    return local_vfs_path[EXPORT_LEN] == '\0' || local_vfs_path[EXPORT_LEN] == '/';
}

auto trim_export_prefix(const char* export_path, const char* local_vfs_path) -> const char* {
    if (!path_matches_export(export_path, local_vfs_path)) {
        return nullptr;
    }

    size_t const EXPORT_LEN = std::strlen(export_path);
    const char* rel = local_vfs_path + EXPORT_LEN;
    while (*rel == '/') {
        rel++;
    }
    return rel;
}

void release_vfs_proxy_buffers(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }

    // Transports that provide a revocation callback own registration metadata
    // independently of the backing allocation. Drop their local registrations
    // before returning either buffer to kmalloc. Raw ivshmem intentionally
    // provides no callback: its offset keys remain pinned until ivshmem
    // transport/VM reset so an in-flight peer write cannot land in a recycled
    // region.
    WkiTransport* const RDMA_TRANSPORT = state->rdma_transport;
    if (RDMA_TRANSPORT != nullptr && RDMA_TRANSPORT->rdma_unregister_region != nullptr) {
        if (state->rdma_read_rkey != 0) {
            static_cast<void>(RDMA_TRANSPORT->rdma_unregister_region(RDMA_TRANSPORT, state->rdma_read_rkey, VFS_RDMA_BOUNCE_SIZE));
        }
        if (state->rdma_bulk_rkey != 0 && state->rdma_bulk_size != 0) {
            static_cast<void>(RDMA_TRANSPORT->rdma_unregister_region(RDMA_TRANSPORT, state->rdma_bulk_rkey, state->rdma_bulk_size));
        }
    }

    delete[] state->rdma_bounce_buf;
    state->rdma_bounce_buf = nullptr;
    state->rdma_read_rkey = 0;
    state->rdma_capable = false;
    state->rdma_transport = nullptr;
    state->rdma_server_write_rkey = 0;
    state->rdma_server_read_staging_rkey = 0;
    state->rdma_server_bulk_staging_rkey = 0;
    state->rdma_read_disabled.store(false, std::memory_order_release);
    state->rdma_read_retry_after_us.store(0, std::memory_order_release);
    state->rdma_read_failure_count.store(0, std::memory_order_release);

    delete[] state->rdma_bulk_buf;
    state->rdma_bulk_buf = nullptr;
    state->rdma_bulk_rkey = 0;
    state->rdma_bulk_size = 0;
    state->bulk_owner_fd = -1;
    state->bulk_rdma_capable = false;
    state->bulk_rdma_disabled.store(false, std::memory_order_release);
    state->bulk_rdma_retry_after_us.store(0, std::memory_order_release);
    state->bulk_rdma_failure_count.store(0, std::memory_order_release);
    state->shared_io_in_use.store(false, std::memory_order_release);
    state->op_untracked_send_pending.store(false, std::memory_order_release);
}

void release_vfs_proxy_lifecycle_ref(ProxyVfsState* state);

auto send_vfs_detach(uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie, const ResourceIncarnationToken& resource_incarnation,
                     WkiReliableTxToken* tx_token_out) -> int {
    constexpr size_t DETACH_MAX_SIZE = wki_dev_detach_payload_size(true);
    std::array<uint8_t, DETACH_MAX_SIZE> det_buf{};
    auto* det = reinterpret_cast<DevDetachPayload*>(det_buf.data());
    det->target_node = owner_node;
    det->resource_type = static_cast<uint16_t>(ResourceType::VFS);
    det->resource_id = resource_id;
    det_buf.at(WKI_DEV_DETACH_COOKIE_OFFSET) = attach_cookie;

    bool const WITH_INCARNATION = wki_resource_incarnation_negotiated(owner_node, ResourceType::VFS);
    if (WITH_INCARNATION) {
        if (!wki_resource_incarnation_valid(resource_incarnation)) {
            return WKI_ERR_INVALID;
        }
        std::memcpy(det_buf.data() + WKI_DEV_DETACH_INCARNATION_OFFSET, &resource_incarnation, sizeof(resource_incarnation));
    }
    uint16_t const DETACH_SIZE = wki_dev_detach_payload_size(WITH_INCARNATION);
    return wki_send_tracked(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, det_buf.data(), DETACH_SIZE, tx_token_out);
}

struct VfsDetachAttempt {
    ProxyVfsState* state = nullptr;
    uint16_t owner_node = WKI_NODE_INVALID;
    uint32_t resource_id = 0;
    uint8_t attach_cookie = 0;
    ResourceIncarnationToken incarnation = {};
    uint32_t peer_boot_epoch = 0;
    WkiReliableTxToken tx_token = {};
};

auto vfs_detach_incarnation_equal(const ResourceIncarnationToken& lhs, const ResourceIncarnationToken& rhs) -> bool {
    bool const LHS_VALID = wki_resource_incarnation_valid(lhs);
    bool const RHS_VALID = wki_resource_incarnation_valid(rhs);
    return LHS_VALID == RHS_VALID && (!LHS_VALID || wki_resource_incarnation_equal(lhs, rhs));
}

// Caller must hold s_vfs_lock. An epoch marker can race an in-progress attach
// before its ACK publishes binding_incarnation, so retain the exact requested
// incarnation as the possible server-binding identity in that interval.
auto vfs_detach_incarnation_snapshot_locked(const ProxyVfsState* state) -> ResourceIncarnationToken {
    return wki_resource_incarnation_valid(state->binding_incarnation) ? state->binding_incarnation : state->attach_expected_incarnation;
}

// Caller must hold s_vfs_lock.
void link_pending_vfs_detach_locked(ProxyVfsState* state) {
    state->detach_prev = g_pending_vfs_detach_tail;
    state->detach_next = nullptr;
    if (g_pending_vfs_detach_tail != nullptr) {
        g_pending_vfs_detach_tail->detach_next = state;
    } else {
        g_pending_vfs_detach_head = state;
    }
    g_pending_vfs_detach_tail = state;
}

// Caller must hold s_vfs_lock.
void unlink_pending_vfs_detach_locked(ProxyVfsState* state) {
    if (state->detach_prev != nullptr) {
        state->detach_prev->detach_next = state->detach_next;
    } else {
        g_pending_vfs_detach_head = state->detach_next;
    }
    if (state->detach_next != nullptr) {
        state->detach_next->detach_prev = state->detach_prev;
    } else {
        g_pending_vfs_detach_tail = state->detach_prev;
    }
    state->detach_prev = nullptr;
    state->detach_next = nullptr;
}

// Caller must hold s_vfs_lock.
auto pending_vfs_detach_matches_locked(const ProxyVfsState* state, uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie,
                                       const ResourceIncarnationToken& incarnation) -> bool {
    return state->detach_pending && state->detach_owner_node == owner_node && state->detach_resource_id == resource_id &&
           state->detach_attach_cookie == attach_cookie && vfs_detach_incarnation_equal(state->detach_incarnation, incarnation);
}

// Caller must hold s_vfs_lock. A deferred-only reservation starts idle so the
// task-context retry worker can claim it after teardown releases locks.
auto stage_vfs_detach_locked(ProxyVfsState* state, uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie,
                             const ResourceIncarnationToken& incarnation, bool initial_attempt_in_progress) -> bool {
    if (state == nullptr || attach_cookie == 0) {
        return false;
    }
    if (state->detach_pending) {
        bool const SAME = pending_vfs_detach_matches_locked(state, owner_node, resource_id, attach_cookie, incarnation);
        if (!SAME) [[unlikely]] {
            ker::mod::dbg::panic_handler("WKI VFS proxy: overlapping detach idempotence tuples");
            hcf();
        }
        return false;
    }

    state->detach_pending = true;
    state->detach_retry_in_progress = initial_attempt_in_progress;
    state->detach_owner_node = owner_node;
    state->detach_resource_id = resource_id;
    state->detach_attach_cookie = attach_cookie;
    state->detach_incarnation = incarnation;
    state->detach_peer_boot_epoch =
        wki_resource_incarnation_valid(incarnation) ? incarnation.owner_boot_epoch : state->binding_peer_boot_epoch;
    state->detach_tx_token = {};
    state->lifecycle_refs++;
    link_pending_vfs_detach_locked(state);
    return true;
}

auto stage_vfs_detach(ProxyVfsState* state, uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie,
                      const ResourceIncarnationToken& incarnation) -> bool {
    s_vfs_lock.lock();
    bool const STAGED = stage_vfs_detach_locked(state, owner_node, resource_id, attach_cookie, incarnation, false);
    s_vfs_lock.unlock();
    return STAGED;
}

void finish_vfs_detach_attempt(const VfsDetachAttempt& attempt, WkiReliableTxStatus tx_status, int send_result,
                               const WkiReliableTxToken& replacement_token, bool peer_epoch_invalidated = false) {
    if (attempt.state == nullptr) {
        return;
    }

    bool release_pending_ref = false;
    s_vfs_lock.lock();
    ProxyVfsState* const state = attempt.state;
    if (!pending_vfs_detach_matches_locked(state, attempt.owner_node, attempt.resource_id, attempt.attach_cookie, attempt.incarnation) ||
        !state->detach_retry_in_progress) {
        s_vfs_lock.unlock();
        return;
    }

    if (tx_status == WkiReliableTxStatus::ACKED || peer_epoch_invalidated) {
        unlink_pending_vfs_detach_locked(state);
        state->detach_pending = false;
        state->detach_retry_in_progress = false;
        state->detach_owner_node = WKI_NODE_INVALID;
        state->detach_resource_id = 0;
        state->detach_attach_cookie = 0;
        state->detach_incarnation = {};
        state->detach_peer_boot_epoch = 0;
        state->detach_tx_token = {};
        release_pending_ref = true;
    } else if (tx_status == WkiReliableTxStatus::PENDING) {
        state->detach_retry_in_progress = false;
    } else {
        state->detach_tx_token = send_result == WKI_OK ? replacement_token : WkiReliableTxToken{};
        state->detach_retry_in_progress = false;
    }
    s_vfs_lock.unlock();

    if (release_pending_ref) {
        release_vfs_proxy_lifecycle_ref(state);
    }
}

auto send_or_defer_vfs_detach(ProxyVfsState* state, uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie,
                              const ResourceIncarnationToken& incarnation) -> int {
    bool const STAGED = stage_vfs_detach(state, owner_node, resource_id, attach_cookie, incarnation);
    wki_deferred_work_notify();
    return STAGED ? WKI_OK : WKI_ERR_BUSY;
}

void discard_unpublished_proxy(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }

    bool detach_staged = false;
    s_vfs_lock.lock();
    if (state->active || state->mount_configured || state->lifecycle_refs == 0) [[unlikely]] {
        s_vfs_lock.unlock();
        ker::mod::dbg::panic_handler("WKI remote VFS: invalid unpublished proxy teardown");
        hcf();
    }
    if (state->epoch_reset_pending) {
        detach_staged = stage_vfs_detach_locked(state, state->owner_node, state->resource_id, state->binding_attach_cookie,
                                                vfs_detach_incarnation_snapshot_locked(state), false);
        if (state->detach_pending) {
            state->epoch_reset_pending = false;
        }
    }
    state->destroy_when_idle = true;
    state->mount_released = true;
    s_vfs_lock.unlock();
    if (detach_staged) {
        wki_deferred_work_notify();
    }
    release_vfs_proxy_lifecycle_ref(state);
}

void discard_failed_attached_proxy(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }

    uint16_t owner_node = WKI_NODE_INVALID;
    uint32_t resource_id = 0;
    uint8_t attach_cookie = 0;
    ResourceIncarnationToken binding_incarnation = {};
    uint16_t assigned_channel = 0;
    WkiChannel* assigned_channel_ref = nullptr;
    uint32_t assigned_channel_generation = 0;
    bool detach_staged = false;
    s_vfs_lock.lock();
    owner_node = state->owner_node;
    resource_id = state->resource_id;
    attach_cookie = state->binding_attach_cookie;
    binding_incarnation = vfs_detach_incarnation_snapshot_locked(state);
    bool const OWNS_REMOTE_DETACH = state->active || state->epoch_reset_pending;
    assigned_channel = state->assigned_channel;
    assigned_channel_ref = state->assigned_channel_ref;
    assigned_channel_generation = state->assigned_channel_generation;
    if (OWNS_REMOTE_DETACH) {
        // The pending lifecycle reference and tuple become visible before the
        // active row disappears from replacement-mount admission.
        detach_staged = stage_vfs_detach_locked(state, owner_node, resource_id, attach_cookie, binding_incarnation, false);
        if (state->detach_pending) {
            state->epoch_reset_pending = false;
        }
    }
    state->active = false;
    state->destroy_when_idle = true;
    state->mount_released = true;
    s_vfs_lock.unlock();

    // Peer cleanup wins by clearing active under the same registry lock. In
    // that case its retained teardown snapshot owns channel retirement.
    if (OWNS_REMOTE_DETACH) {
        if (detach_staged) {
            wki_deferred_work_notify();
        }
        static_cast<void>(wki_channel_close_generation(assigned_channel_ref, owner_node, assigned_channel, assigned_channel_generation));
    }

    release_vfs_proxy_lifecycle_ref(state);
}

auto readlink_status_is_cacheable(int status) -> bool {
    return status == 0 || status == -EINVAL || status == -ENOENT || status == -ENOTDIR;
}

auto try_readlink_cache_lookup(ProxyVfsState* state, const char* fs_relative_path, char* buf, size_t bufsize, ssize_t* result_out,
                               uint32_t* generation_out) -> bool {
    if (state == nullptr || fs_relative_path == nullptr || result_out == nullptr || generation_out == nullptr) {
        return false;
    }

    if (std::strlen(fs_relative_path) >= VFS_READLINK_CACHE_TEXT_MAX) {
        return false;
    }

    bool found = false;
    uint64_t const NOW = wki_now_us();
    state->lock.lock();
    uint32_t const CACHE_GENERATION = state->readlink_cache_generation;
    *generation_out = CACHE_GENERATION;
    for (auto& entry : state->readlink_cache) {
        if (entry.generation != CACHE_GENERATION) {
            continue;
        }

        if (NOW - entry.cached_at_us > readlink_cache_retention_us(entry.status)) {
            reset_readlink_cache_entry(entry);
            continue;
        }

        if (std::strncmp(entry.path.data(), fs_relative_path, VFS_READLINK_CACHE_TEXT_MAX) != 0) {
            continue;
        }

        if (entry.status != 0) {
            *result_out = entry.status;
            found = true;
            break;
        }

        size_t const TO_COPY = std::min(bufsize, static_cast<size_t>(entry.target_len));
        memcpy(buf, entry.target.data(), TO_COPY);
        *result_out = static_cast<ssize_t>(TO_COPY);
        found = true;
        break;
    }
    state->lock.unlock();

    return found;
}

void cache_readlink_result(ProxyVfsState* state, uint32_t expected_generation, const char* fs_relative_path, int status, const char* target,
                           uint16_t target_len) {
    if (state == nullptr || expected_generation == 0 || fs_relative_path == nullptr || !readlink_status_is_cacheable(status)) {
        return;
    }

    size_t const PATH_LEN = std::strlen(fs_relative_path);
    if (PATH_LEN >= VFS_READLINK_CACHE_TEXT_MAX) {
        return;
    }

    if (status == 0 && (target == nullptr || target_len == 0 || target_len >= VFS_READLINK_CACHE_TEXT_MAX)) {
        return;
    }

    uint64_t const NOW = wki_now_us();
    state->lock.lock();
    if (state->readlink_cache_generation != expected_generation) {
        state->lock.unlock();
        return;
    }
    uint32_t const CACHE_GENERATION = expected_generation;

    ProxyVfsState::ReadlinkCacheEntry* selected = nullptr;
    ProxyVfsState::ReadlinkCacheEntry* oldest = nullptr;
    for (auto& entry : state->readlink_cache) {
        if (entry.generation == CACHE_GENERATION && NOW - entry.cached_at_us > readlink_cache_retention_us(entry.status)) {
            reset_readlink_cache_entry(entry);
        }

        if (entry.generation != CACHE_GENERATION) {
            if (selected == nullptr) {
                selected = &entry;
            }
            continue;
        }

        if (std::strncmp(entry.path.data(), fs_relative_path, VFS_READLINK_CACHE_TEXT_MAX) == 0) {
            selected = &entry;
            break;
        }

        if (oldest == nullptr || entry.cached_at_us < oldest->cached_at_us) {
            oldest = &entry;
        }
    }

    if (selected == nullptr) {
        selected = oldest;
    }
    if (selected != nullptr) {
        reset_readlink_cache_entry(*selected);
        selected->status = static_cast<int16_t>(status);
        selected->cached_at_us = NOW;
        memcpy(selected->path.data(), fs_relative_path, PATH_LEN + 1);
        if (status == 0) {
            selected->target_len = target_len;
            memcpy(selected->target.data(), target, target_len);
        }
        selected->generation = CACHE_GENERATION;
    }

    state->lock.unlock();
}

auto proxy_acquire_shared_io_slot(ProxyVfsState* state, uint64_t start_us) -> int {
    if (state == nullptr) {
        return WKI_OK;
    }

    uint64_t const DEADLINE_US = wki_future_deadline_us(start_us, VFS_PROXY_SLOT_WAIT_TIMEOUT_US);
    while (true) {
        bool expected = false;
        if (state->shared_io_in_use.compare_exchange_weak(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return WKI_OK;
        }

        if (!state->shared_io_in_use.load(std::memory_order_acquire)) {
            continue;
        }
        if (wki_now_us() >= DEADLINE_US) {
            return WKI_ERR_TIMEOUT;
        }
        ker::mod::sched::kern_sleep_us(VFS_PROXY_CONTENTION_SLEEP_US);
    }
}

void proxy_release_shared_io_slot(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }
    state->shared_io_in_use.store(false, std::memory_order_release);
}

struct SharedIoSlotGuard {
    explicit SharedIoSlotGuard(ProxyVfsState* state_ref, uint64_t start_us)
        : state(state_ref), status(proxy_acquire_shared_io_slot(state_ref, start_us)) {
        if (status != WKI_OK) {
            state = nullptr;
        }
    }
    ~SharedIoSlotGuard() { proxy_release_shared_io_slot(state); }

    SharedIoSlotGuard(const SharedIoSlotGuard&) = delete;
    auto operator=(const SharedIoSlotGuard&) -> SharedIoSlotGuard& = delete;

    [[nodiscard]] auto acquired() const -> bool { return status == WKI_OK; }
    [[nodiscard]] auto result() const -> int { return status; }

    ProxyVfsState* state;
    int status;
};

struct OptionalSharedIoSlotGuard {
    OptionalSharedIoSlotGuard(ProxyVfsState* state_ref, bool enabled, uint64_t start_us) : state(enabled ? state_ref : nullptr) {
        if (enabled) {
            status = proxy_acquire_shared_io_slot(state, start_us);
            if (status != WKI_OK) {
                state = nullptr;
            }
        }
    }
    ~OptionalSharedIoSlotGuard() { proxy_release_shared_io_slot(state); }

    OptionalSharedIoSlotGuard(const OptionalSharedIoSlotGuard&) = delete;
    auto operator=(const OptionalSharedIoSlotGuard&) -> OptionalSharedIoSlotGuard& = delete;

    [[nodiscard]] auto acquired() const -> bool { return status == WKI_OK; }
    [[nodiscard]] auto result() const -> int { return status; }

    ProxyVfsState* state;
    int status = WKI_OK;
};

struct RoceTaggedReceive {
    uint32_t rkey = 0;
    uint16_t cookie = 0;
};

auto proxy_op_slot_busy(const ProxyVfsState* state) -> bool {
    return state->op_pending.load(std::memory_order_acquire) || state->op_untracked_send_pending.load(std::memory_order_acquire);
}

struct ProxySlotCaller {
    uint64_t pid = 0;
    ker::mod::sched::task::TaskType type = ker::mod::sched::task::TaskType::IDLE;
};

struct ProxySlotWaiterRemoval {
    bool removed = false;
    bool was_head = false;
};

struct ProxySlotWaiterCleanup {
    bool removed = false;
    uint64_t next_pid = 0;
};

struct ProxyTaskCleanup {
    bool removed = false;
    uint64_t next_pid = 0;
    WkiWaitEntry* op_wait_entry = nullptr;
    bool op_wait_claimed = false;
    bool op_wait_retiring = false;
};

auto current_proxy_slot_caller() -> ProxySlotCaller {
    if (!ker::mod::sched::can_query_current_task()) {
        return {};
    }
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return {};
    }
    return {.pid = task->pid, .type = task->type};
}

auto find_proxy_slot_waiter_locked(const ProxyVfsState* state, uint64_t pid) -> size_t {
    if (state == nullptr || pid == 0) {
        return VFS_PROXY_SLOT_WAITER_CAPACITY;
    }
    for (size_t i = 0; i < state->op_slot_waiter_count; ++i) {
        if (state->op_slot_waiter_pids.at(i) == pid) {
            return i;
        }
    }
    return VFS_PROXY_SLOT_WAITER_CAPACITY;
}

auto enqueue_proxy_slot_waiter_locked(ProxyVfsState* state, uint64_t pid) -> bool {
    if (state == nullptr || pid == 0) {
        return false;
    }
    if (find_proxy_slot_waiter_locked(state, pid) != VFS_PROXY_SLOT_WAITER_CAPACITY) {
        return true;
    }
    if (state->op_slot_waiter_count >= state->op_slot_waiter_pids.size()) {
        return false;
    }
    state->op_slot_waiter_pids.at(state->op_slot_waiter_count++) = pid;
    return true;
}

auto remove_proxy_slot_waiter_locked(ProxyVfsState* state, uint64_t pid) -> ProxySlotWaiterRemoval {
    size_t const INDEX = find_proxy_slot_waiter_locked(state, pid);
    if (INDEX >= state->op_slot_waiter_count) {
        return {};
    }
    for (size_t i = INDEX + 1; i < state->op_slot_waiter_count; ++i) {
        state->op_slot_waiter_pids.at(i - 1) = state->op_slot_waiter_pids.at(i);
    }
    state->op_slot_waiter_count--;
    state->op_slot_waiter_pids.at(state->op_slot_waiter_count) = 0;
    return {.removed = true, .was_head = INDEX == 0};
}

auto proxy_slot_waiter_head_locked(const ProxyVfsState* state) -> uint64_t {
    return state != nullptr && state->op_slot_waiter_count != 0 ? state->op_slot_waiter_pids.front() : 0;
}

auto proxy_slot_handoff_candidate_locked(const ProxyVfsState* state) -> uint64_t {
    if (state == nullptr || !state->active || proxy_op_slot_busy(state)) {
        return 0;
    }
    return proxy_slot_waiter_head_locked(state);
}

void clear_proxy_op_state_locked(ProxyVfsState* state, int status) {
    state->op_status = static_cast<int16_t>(status);
    state->op_expected_id = 0;
    state->op_expected_seq = 0;
    state->op_resp_buf = nullptr;
    state->op_resp_max = 0;
    state->op_resp_len = 0;
    state->op_wait_entry = nullptr;
    state->op_waiter_pid = 0;
    state->op_pending.store(false, std::memory_order_release);
}

auto advance_proxy_op_generation_locked(ProxyVfsState* state) -> uint64_t {
    state->op_generation++;
    if (state->op_generation == 0) {
        state->op_generation++;
    }
    return state->op_generation;
}

auto remove_one_proxy_slot_waiter(uint64_t pid) -> ProxySlotWaiterCleanup {
    ProxySlotWaiterCleanup cleanup{};
    if (pid == 0) {
        return cleanup;
    }

    s_vfs_lock.lock();
    for (auto& proxy : g_vfs_proxies) {
        auto* state = proxy.get();
        if (state == nullptr) {
            continue;
        }
        state->lock.lock();
        ProxySlotWaiterRemoval const REMOVAL = remove_proxy_slot_waiter_locked(state, pid);
        if (REMOVAL.removed) {
            cleanup.removed = true;
            if (REMOVAL.was_head) {
                cleanup.next_pid = proxy_slot_handoff_candidate_locked(state);
            }
            state->lock.unlock();
            break;
        }
        state->lock.unlock();
    }
    s_vfs_lock.unlock();
    return cleanup;
}

void wake_proxy_slot_waiter(uint64_t pid) {
    while (pid != 0 && !ker::mod::sched::wake_task_by_pid_from_event(pid)) {
        // Task exit normally removes the PID through wki_wait_cleanup_for_task().
        // If lookup lost that race, reap only the stale FIFO entry. Active-op
        // cleanup is reserved for the exit hook, which owns task-stack lifetime.
        ProxySlotWaiterCleanup const CLEANUP = remove_one_proxy_slot_waiter(pid);
        if (!CLEANUP.removed) {
            return;
        }
        pid = CLEANUP.next_pid;
    }
}

void unlock_proxy_slot_and_wake_next(ProxyVfsState* state) {
    uint64_t const NEXT_PID = proxy_slot_handoff_candidate_locked(state);
    state->lock.unlock();
    wake_proxy_slot_waiter(NEXT_PID);
}

void park_proxy_slot_caller(const ProxySlotCaller& caller, uint64_t deadline_us, bool registered) {
    uint64_t const NOW_US = wki_now_us();
    if (NOW_US >= deadline_us) {
        return;
    }
    uint64_t const REMAINING_US = deadline_us - NOW_US;
    if (caller.type == ker::mod::sched::task::TaskType::DAEMON && wki_current_wait_must_drive_progress()) {
        // This worker can be responsible for receiving the response that
        // releases the slot, including when the fixed FIFO is at capacity.
        wki_spin_yield();
        ker::mod::sched::kern_yield();
        return;
    }
    if (caller.type == ker::mod::sched::task::TaskType::PROCESS) {
        auto* task = ker::mod::sched::get_current_task();
        bool const SYSCALL_PARK_SAFE = task != nullptr && task->syscall_account_start_us != 0 && ker::mod::sched::preempt_count() == 0 &&
                                       ker::mod::sched::interrupts_enabled();
        if (!SYSCALL_PARK_SAFE || task->has_interrupting_signal_pending()) {
            // File-backed page faults can reach remote reads on a PROCESS task
            // outside a syscall safe point. Preserve IRQ/exception state there.
            // Pending signals also make syscall park return immediately; this
            // wait remains uninterruptible without becoming a hot park loop.
            ker::mod::sched::kern_yield();
            return;
        }
        uint64_t const PARK_DEADLINE_US =
            registered ? deadline_us : wki_future_deadline_us(NOW_US, std::min(REMAINING_US, VFS_PROXY_CONTENTION_SLEEP_US));
        ker::mod::sched::preemptible_syscall_park("wki_vfs_slot", PARK_DEADLINE_US);
        return;
    }
    if (!registered) {
        ker::mod::sched::kern_sleep_us(std::min(REMAINING_US, VFS_PROXY_CONTENTION_SLEEP_US));
        return;
    }
    if (caller.type == ker::mod::sched::task::TaskType::DAEMON) {
        ker::mod::sched::kern_sleep_us(REMAINING_US);
        return;
    }
    ker::mod::sched::kern_sleep_us(std::min(REMAINING_US, VFS_PROXY_CONTENTION_SLEEP_US));
}

auto cleanup_proxy_task_reference_locked(ProxyVfsState* state, uint64_t pid) -> ProxyTaskCleanup {
    ProxyTaskCleanup cleanup{};
    if (state == nullptr || pid == 0) {
        return cleanup;
    }

    ProxySlotWaiterRemoval const REMOVAL = remove_proxy_slot_waiter_locked(state, pid);
    bool const RELEASED_ACTIVE_OP = state->op_pending.load(std::memory_order_acquire) && state->op_waiter_pid == pid;
    bool const RELEASED_RETIRING_OP = state->op_retiring_wait_entry != nullptr && state->op_retiring_waiter_pid == pid;
    if (RELEASED_ACTIVE_OP) {
        cleanup.op_wait_entry = state->op_wait_entry;
        state->op_wait_entry = nullptr;
        cleanup.op_wait_claimed = wki_claim_op(cleanup.op_wait_entry);
        clear_proxy_op_state_locked(state, WKI_ERR_PEER_FENCED);
    } else if (RELEASED_RETIRING_OP) {
        // Teardown exclusively owns the raw waiter. Keep this exiting task's
        // stack alive until teardown drops its retirement reference.
        cleanup.op_wait_entry = state->op_retiring_wait_entry;
        cleanup.op_wait_retiring = true;
    }

    cleanup.removed = REMOVAL.removed || RELEASED_ACTIVE_OP || RELEASED_RETIRING_OP;
    if (REMOVAL.was_head || RELEASED_ACTIVE_OP) {
        cleanup.next_pid = proxy_slot_handoff_candidate_locked(state);
    }
    return cleanup;
}

auto remove_one_proxy_task_reference(uint64_t pid) -> ProxyTaskCleanup {
    ProxyTaskCleanup cleanup{};
    if (pid == 0) {
        return cleanup;
    }

    s_vfs_lock.lock();
    for (auto& proxy : g_vfs_proxies) {
        auto* state = proxy.get();
        if (state == nullptr) {
            continue;
        }
        state->lock.lock();
        cleanup = cleanup_proxy_task_reference_locked(state, pid);
        if (cleanup.removed) {
            state->lock.unlock();
            break;
        }
        state->lock.unlock();
    }
    s_vfs_lock.unlock();
    return cleanup;
}

auto acquire_proxy_slot_locked(ProxyVfsState* state, uint64_t start_us, bool claim_untracked_send) -> int {
    uint64_t const DEADLINE_US = wki_future_deadline_us(start_us, VFS_PROXY_SLOT_WAIT_TIMEOUT_US);
    ProxySlotCaller const CALLER = current_proxy_slot_caller();
    while (true) {
        state->lock.lock();
        if (!state->active) {
            static_cast<void>(remove_proxy_slot_waiter_locked(state, CALLER.pid));
            state->lock.unlock();
            return WKI_ERR_PEER_FENCED;
        }

        uint64_t const HEAD_PID = proxy_slot_waiter_head_locked(state);
        bool const CALLER_IS_HEAD = CALLER.pid != 0 && HEAD_PID == CALLER.pid;
        if (!proxy_op_slot_busy(state) && (HEAD_PID == 0 || CALLER_IS_HEAD)) {
            if (CALLER_IS_HEAD) {
                static_cast<void>(remove_proxy_slot_waiter_locked(state, CALLER.pid));
            }
            if (claim_untracked_send) {
                state->op_untracked_send_pending.store(true, std::memory_order_release);
            }
            return WKI_OK;
        }

        uint64_t const NOW_US = wki_now_us();
        if (NOW_US >= DEADLINE_US) {
            ProxySlotWaiterRemoval const REMOVAL = remove_proxy_slot_waiter_locked(state, CALLER.pid);
            uint64_t const NEXT_PID = REMOVAL.was_head ? proxy_slot_handoff_candidate_locked(state) : 0;
            state->lock.unlock();
            wake_proxy_slot_waiter(NEXT_PID);
            return WKI_ERR_TIMEOUT;
        }

        bool const REGISTERED = enqueue_proxy_slot_waiter_locked(state, CALLER.pid);
        state->lock.unlock();
        park_proxy_slot_caller(CALLER, DEADLINE_US, REGISTERED);
    }
}

auto acquire_proxy_op_slot_locked(ProxyVfsState* state, uint64_t start_us) -> int {
    return acquire_proxy_slot_locked(state, start_us, false);
}

auto acquire_proxy_untracked_send_slot_locked(ProxyVfsState* state, uint64_t start_us) -> int {
    return acquire_proxy_slot_locked(state, start_us, true);
}

// Caller must hold the lock protecting waiter_slot.
auto claim_and_clear_waiter_locked(WkiWaitEntry*& waiter_slot) -> WkiWaitEntry* {
    WkiWaitEntry* waiter = waiter_slot;
    waiter_slot = nullptr;
    if (!wki_claim_op(waiter)) {
        return nullptr;
    }
    return waiter;
}

// Response RX retains the exact waiter pointer in proxy state so task-exit or
// teardown cleanup can quiesce a claimant even before wki_wait_for_op() links
// the stack entry into the global wait list.
auto claim_response_waiter_locked(WkiWaitEntry* waiter) -> WkiWaitEntry* {
    if (!wki_claim_op(waiter)) {
        return nullptr;
    }
    return waiter;
}

void finish_claimed_waiter(WkiWaitEntry* waiter, int result) {
    if (waiter != nullptr) {
        wki_finish_claimed_op(waiter, result);
    }
}

void finish_or_quiesce_waiter(WkiWaitEntry* wait, bool claimed, int result) {
    if (wait == nullptr) {
        return;
    }
    if (claimed) {
        wki_finish_claimed_op(wait, result);
        return;
    }

    wki_quiesce_claimed_op(wait);
}

void wait_for_waiter_retirement(WkiWaitEntry* wait) {
    if (wait == nullptr) {
        return;
    }

    while (wait->retirement_pending.load(std::memory_order_acquire)) {
        wki_wait_quiescence_point();
    }
}

void cancel_proxy_op_wait(ProxyVfsState* state, WkiWaitEntry& wait, uint64_t op_generation, int result) {
    bool claimed = false;
    uint64_t next_pid = 0;
    state->lock.lock();
    if (state->op_pending.load(std::memory_order_acquire) && state->op_generation == op_generation) {
        clear_proxy_op_state_locked(state, result);
        next_pid = proxy_slot_handoff_candidate_locked(state);
    }
    claimed = wki_claim_op(&wait);
    state->lock.unlock();

    wake_proxy_slot_waiter(next_pid);
    finish_or_quiesce_waiter(&wait, claimed, result);
    wait_for_waiter_retirement(&wait);
}

struct ProxyOpResult {
    int status = -ENOTCONN;
    uint16_t response_len = 0;
    bool consumed = false;
};

auto consume_proxy_op_result_locked(ProxyVfsState* state, uint64_t op_generation) -> ProxyOpResult {
    if (!state->op_pending.load(std::memory_order_acquire) || state->op_generation != op_generation) {
        return {};
    }

    ProxyOpResult result = {
        .status = static_cast<int>(state->op_status),
        .response_len = state->op_resp_len,
        .consumed = true,
    };
    clear_proxy_op_state_locked(state, result.status);
    return result;
}

void clear_proxy_attach_state_locked(ProxyVfsState* state, uint8_t status) {
    state->attach_status = status;
    state->attach_channel = 0;
    state->attach_max_op_size = 0;
    state->attach_expected_cookie = 0;
    state->attach_pending.store(false, std::memory_order_release);
}

void cancel_proxy_attach_wait(ProxyVfsState* state, WkiWaitEntry& wait, int result) {
    bool claimed = false;
    s_vfs_lock.lock();
    if (state->attach_wait_entry == &wait) {
        state->attach_wait_entry = nullptr;
    }
    clear_proxy_attach_state_locked(state, static_cast<uint8_t>(DevAttachStatus::BUSY));
    claimed = wki_claim_op(&wait);
    s_vfs_lock.unlock();

    finish_or_quiesce_waiter(&wait, claimed, result);
}

struct PendingProxyTeardown {
    ProxyVfsState* state = nullptr;
    uint16_t owner_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    WkiChannel* assigned_channel_ref = nullptr;
    uint32_t assigned_channel_generation = 0;
    uint32_t resource_id = 0;
    uint8_t binding_attach_cookie = 0;
    ResourceIncarnationToken binding_incarnation = {};
    bool detach_staged = false;
    uint16_t op_expected_id = 0;
    uint16_t op_expected_seq = 0;
    bool had_op_pending = false;
    bool had_attach_pending = false;
    WkiWaitEntry* op_wait_entry = nullptr;
    bool op_wait_claimed = false;
    WkiWaitEntry* attach_wait_entry = nullptr;
    std::array<uint64_t, VFS_PROXY_SLOT_WAITER_CAPACITY> op_slot_waiter_pids = {};
    size_t op_slot_waiter_count = 0;
    std::array<char, VFS_EXPORT_PATH_LEN> local_mount_path = {};
};

void wake_proxy_slot_waiters(const PendingProxyTeardown& teardown) {
    for (size_t i = 0; i < teardown.op_slot_waiter_count; ++i) {
        wake_proxy_slot_waiter(teardown.op_slot_waiter_pids.at(i));
    }
}

void finish_proxy_teardown_op_waiter(const PendingProxyTeardown& teardown, int result) {
    finish_or_quiesce_waiter(teardown.op_wait_entry, teardown.op_wait_claimed, result);
    if (teardown.state == nullptr || teardown.op_wait_entry == nullptr) {
        return;
    }

    teardown.state->lock.lock();
    if (teardown.state->op_retiring_wait_entry == teardown.op_wait_entry) {
        teardown.state->op_retiring_wait_entry = nullptr;
        teardown.state->op_retiring_waiter_pid = 0;
        // Exit discovery uses this same lock. Clear the marker, then make this
        // release teardown's final dereference before exposing the unlocked
        // state or allowing the RPC owner to unwind its stack.
        teardown.op_wait_entry->retirement_pending.store(false, std::memory_order_release);
    }
    teardown.state->lock.unlock();
}

auto proxy_is_idle_for_resource_release_locked(ProxyVfsState* state) -> bool {
    return state != nullptr && !state->active && !state->epoch_reset_pending &&
           state->open_file_refs.load(std::memory_order_acquire) == 0 && state->lifecycle_refs == 0;
}

auto claim_idle_vfs_proxy_resource_release_locked(ProxyVfsState* state) -> bool {
    if (!proxy_is_idle_for_resource_release_locked(state) || state->resources_released || state->resources_releasing) {
        return false;
    }
    state->resources_releasing = true;
    return true;
}

void erase_destroyed_idle_vfs_proxy_locked(ProxyVfsState* state) {
    if (!proxy_is_idle_for_resource_release_locked(state) || !state->destroy_when_idle || !state->mount_released ||
        state->resources_releasing || !state->resources_released) {
        return;
    }

    std::erase_if(g_vfs_proxies, [state](const std::unique_ptr<ProxyVfsState>& proxy) { return proxy.get() == state; });
}

void finish_idle_vfs_proxy_resource_release(ProxyVfsState* state) {
    s_vfs_lock.lock();
    state->resources_released = true;
    state->resources_releasing = false;
    erase_destroyed_idle_vfs_proxy_locked(state);
    s_vfs_lock.unlock();
}

void release_and_maybe_destroy_idle_vfs_proxy(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }

    bool release_resources = false;
    s_vfs_lock.lock();
    release_resources = claim_idle_vfs_proxy_resource_release_locked(state);
    if (!release_resources) {
        erase_destroyed_idle_vfs_proxy_locked(state);
    }
    s_vfs_lock.unlock();

    if (release_resources) {
        release_vfs_proxy_buffers(state);
        finish_idle_vfs_proxy_resource_release(state);
    }
}

void release_vfs_proxy_lifecycle_ref(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }

    bool release_resources = false;
    s_vfs_lock.lock();
    if (state->lifecycle_refs == 0) [[unlikely]] {
        s_vfs_lock.unlock();
        ker::mod::dbg::panic_handler("WKI remote VFS: lifecycle ref underflow");
        hcf();
    }
    state->lifecycle_refs--;
    release_resources = claim_idle_vfs_proxy_resource_release_locked(state);
    if (!release_resources) {
        erase_destroyed_idle_vfs_proxy_locked(state);
    }
    s_vfs_lock.unlock();

    if (release_resources) {
        release_vfs_proxy_buffers(state);
        finish_idle_vfs_proxy_resource_release(state);
    }
}

struct ProxyLifecycleRefGuard {
    explicit ProxyLifecycleRefGuard(ProxyVfsState* state_ref) : state(state_ref) {}
    ~ProxyLifecycleRefGuard() { release_vfs_proxy_lifecycle_ref(state); }

    ProxyLifecycleRefGuard(const ProxyLifecycleRefGuard&) = delete;
    auto operator=(const ProxyLifecycleRefGuard&) -> ProxyLifecycleRefGuard& = delete;

    void disarm() { state = nullptr; }

    ProxyVfsState* state;
};

void mark_vfs_proxy_mount_released_and_maybe_destroy(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }

    s_vfs_lock.lock();
    state->mount_released = true;
    s_vfs_lock.unlock();

    release_and_maybe_destroy_idle_vfs_proxy(state);
}

auto acquire_vfs_proxy_open_ref(ProxyVfsState* state) -> bool {
    if (state == nullptr) {
        return false;
    }

    bool acquired = false;
    s_vfs_lock.lock();
    if (state->active && !state->destroy_when_idle && !state->resources_releasing && !state->resources_released) {
        uint32_t const REFS = state->open_file_refs.load(std::memory_order_acquire);
        state->open_file_refs.store(REFS + 1, std::memory_order_release);
        acquired = true;
    }
    s_vfs_lock.unlock();
    return acquired;
}

void release_vfs_proxy_open_ref(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }

    bool release_resources = false;
    s_vfs_lock.lock();
    uint32_t const REFS = state->open_file_refs.load(std::memory_order_acquire);
    if (REFS != 0) {
        state->open_file_refs.store(REFS - 1, std::memory_order_release);
    }
    release_resources = claim_idle_vfs_proxy_resource_release_locked(state);
    if (!release_resources) {
        erase_destroyed_idle_vfs_proxy_locked(state);
    }
    s_vfs_lock.unlock();

    if (release_resources) {
        release_vfs_proxy_buffers(state);
        finish_idle_vfs_proxy_resource_release(state);
    }
}

struct ProxyOpenRefGuard {
    explicit ProxyOpenRefGuard(ProxyVfsState* state_ref) : state(state_ref) {}
    ~ProxyOpenRefGuard() { release_vfs_proxy_open_ref(state); }

    ProxyOpenRefGuard(const ProxyOpenRefGuard&) = delete;
    auto operator=(const ProxyOpenRefGuard&) -> ProxyOpenRefGuard& = delete;

    void disarm() { state = nullptr; }

    ProxyVfsState* state;
};

void deactivate_vfs_proxy_locked(ProxyVfsState* state, PendingProxyTeardown& teardown, bool destroy_when_idle) {
    if (state == nullptr) {
        return;
    }

    teardown.state = state;
    teardown.owner_node = state->owner_node;
    teardown.assigned_channel = state->assigned_channel;
    teardown.assigned_channel_ref = state->assigned_channel_ref;
    teardown.assigned_channel_generation = state->assigned_channel_generation;
    teardown.resource_id = state->resource_id;
    teardown.binding_attach_cookie = state->binding_attach_cookie;
    teardown.binding_incarnation = vfs_detach_incarnation_snapshot_locked(state);
    teardown.local_mount_path = state->local_mount_path;
    state->lifecycle_refs++;

    state->lock.lock();
    // Publish inactivity before releasing tracked ownership or draining the
    // FIFO so every awakened contender fails before constructing a request.
    state->active = false;
    if (state->op_pending.load(std::memory_order_acquire)) {
        teardown.had_op_pending = true;
        teardown.op_expected_id = state->op_expected_id;
        teardown.op_expected_seq = state->op_expected_seq;
        uint64_t const OP_WAITER_PID = state->op_waiter_pid;
        state->op_status = -1;
        teardown.op_wait_entry = state->op_wait_entry;
        if (teardown.op_wait_entry != nullptr) {
            teardown.op_wait_entry->retirement_pending.store(true, std::memory_order_release);
        }
        state->op_wait_entry = nullptr;
        teardown.op_wait_claimed = wki_claim_op(teardown.op_wait_entry);
        clear_proxy_op_state_locked(state, -1);
        state->op_retiring_wait_entry = teardown.op_wait_entry;
        state->op_retiring_waiter_pid = OP_WAITER_PID;
    }
    if (state->attach_pending.load(std::memory_order_acquire)) {
        teardown.had_attach_pending = true;
        teardown.attach_wait_entry = claim_and_clear_waiter_locked(state->attach_wait_entry);
        clear_proxy_attach_state_locked(state, static_cast<uint8_t>(DevAttachStatus::BUSY));
    }
    teardown.op_slot_waiter_pids = state->op_slot_waiter_pids;
    teardown.op_slot_waiter_count = state->op_slot_waiter_count;
    state->op_slot_waiter_pids.fill(0);
    state->op_slot_waiter_count = 0;
    if (destroy_when_idle) {
        state->destroy_when_idle = true;
    }
    state->lock.unlock();
}

// Helper: send DEV_OP_REQ and wait for response
auto vfs_proxy_send_and_wait(ProxyVfsState* state, uint16_t op_id, const uint8_t* req_data, size_t req_data_len, void* resp_buf,
                             uint16_t resp_buf_max, uint16_t* resp_len_out = nullptr, uint64_t wait_timeout_us = VFS_PROXY_OP_TIMEOUT_US,
                             RoceTaggedReceive* tagged_receive = nullptr, const uint8_t* req_tail = nullptr, size_t req_tail_len = 0)
    -> int {
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint64_t const PROXY_WAIT_START = wki_now_us();

    if (resp_len_out != nullptr) {
        *resp_len_out = 0;
    }

    constexpr size_t MAX_REQ_DATA_LEN = WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload);
    if (req_data_len > MAX_REQ_DATA_LEN || req_tail_len > MAX_REQ_DATA_LEN - req_data_len) {
        return -EMSGSIZE;
    }
    if ((req_data_len > 0 && req_data == nullptr) || (req_tail_len > 0 && req_tail == nullptr)) {
        return -EINVAL;
    }
    auto const REQ_DATA_LEN = static_cast<uint16_t>(req_data_len + req_tail_len);
    size_t const REQ_PREFIX_TOTAL = sizeof(DevOpReqPayload) + req_data_len;

    // The request prefix and optional tail initialize every transmitted byte;
    // do not pattern-fill unused bounded stack capacity.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> req_buf __attribute__((uninitialized));
    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
    req->op_id = op_id;
    req->data_len = REQ_DATA_LEN;

    if (req_data_len > 0) {
        memcpy(req_buf.data() + sizeof(DevOpReqPayload), req_data, req_data_len);
    }

    // Serialize until we can claim the proxy for this operation.
    // Another thread may be using the same proxy (e.g., concurrent stat+open).
    int const SLOT_RET = acquire_proxy_op_slot_locked(state, PROXY_WAIT_START);
    auto const PROXY_WAIT_US = static_cast<uint32_t>(wki_now_us() - PROXY_WAIT_START);
    if (PROXY_WAIT_US > 0) {
        perf_record_vfs_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::PROXY_WAIT), state->owner_node, state->assigned_channel, 0,
                              PROXY_WAIT_US, CALLSITE);
    }
    if (SLOT_RET != WKI_OK) {
        return encode_proxy_wki_status(SLOT_RET);
    }

    WkiChannelIdentity const CHANNEL_IDENTITY = proxy_channel_identity_locked(state);
    uint16_t expected_seq = 0;
    if (!peek_channel_tx_seq16(CHANNEL_IDENTITY, &expected_seq)) {
        unlock_proxy_slot_and_wake_next(state);
        return -EIO;
    }
    uint32_t const CORRELATION = expected_seq;

    if (tagged_receive != nullptr) {
        if (tagged_receive->rkey == 0 || !wki_roce_region_prepare_tagged_write(tagged_receive->rkey, expected_seq)) {
            unlock_proxy_slot_and_wake_next(state);
            return -EIO;
        }
        tagged_receive->cookie = expected_seq;
    }

    // Lock held, op_pending still false - set up wait entry and response fields
    WkiWaitEntry wait = {};
    uint64_t const OP_GENERATION = advance_proxy_op_generation_locked(state);
    state->op_wait_entry = &wait;
    state->op_waiter_pid = perf_current_pid();
    state->op_expected_id = op_id;
    state->op_expected_seq = expected_seq;
    state->op_status = 0;
    state->op_resp_buf = resp_buf;
    state->op_resp_max = resp_buf_max;
    state->op_resp_len = 0;
    // Publish the request identity before making the slot visible to the RX path.
    state->op_pending.store(true, std::memory_order_release);
    state->lock.unlock();

    uint64_t const STARTED_US = wki_now_us();
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id),
                                     ker::mod::perf::WkiPerfPhase::BEGIN, state->owner_node, state->assigned_channel, CORRELATION, 0,
                                     REQ_DATA_LEN, CALLSITE);

    int send_ret = WKI_ERR_INVALID;
    if (req_tail_len == 0) {
        send_ret =
            wki_send_on_channel_identity(CHANNEL_IDENTITY, MsgType::DEV_OP_REQ, req_buf.data(), static_cast<uint16_t>(REQ_PREFIX_TOTAL));
    } else {
        send_ret =
            wki_send_on_channel_identity_split(CHANNEL_IDENTITY, MsgType::DEV_OP_REQ, req_buf.data(),
                                               static_cast<uint16_t>(REQ_PREFIX_TOTAL), req_tail, static_cast<uint16_t>(req_tail_len));
    }
    int const SEND_RET = send_ret;

    if (SEND_RET != WKI_OK) {
        cancel_proxy_op_wait(state, wait, OP_GENERATION, SEND_RET);
        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                         perf_vfs_op(op_id), ker::mod::perf::WkiPerfPhase::END, state->owner_node, state->assigned_channel,
                                         CORRELATION, SEND_RET, ELAPSED_US, CALLSITE);
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id), state->owner_node,
                                           state->assigned_channel, SEND_RET, ELAPSED_US, true, 0, REQ_DATA_LEN);
        ker::mod::dbg::log("[WKI] vfs_proxy_send_and_wait send failed: node=0x%04x ch=%u op=%u rc=%d", state->owner_node,
                           state->assigned_channel, op_id, SEND_RET);
        return encode_proxy_wki_status(SEND_RET);
    }

    int const WAIT_RC = wki_wait_for_op(&wait, wait_timeout_us);
    if (WAIT_RC != 0) {
        cancel_proxy_op_wait(state, wait, OP_GENERATION, WAIT_RC);
        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                         perf_vfs_op(op_id), ker::mod::perf::WkiPerfPhase::END, state->owner_node, state->assigned_channel,
                                         CORRELATION, WAIT_RC, ELAPSED_US, CALLSITE);
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id), state->owner_node,
                                           state->assigned_channel, WAIT_RC, ELAPSED_US, true, 0, REQ_DATA_LEN);
        if (WAIT_RC == WKI_ERR_TIMEOUT) {
            ker::mod::dbg::log("[WKI] vfs_proxy_send_and_wait timeout: node=0x%04x ch=%u op=%u wait_us=%llu", state->owner_node,
                               state->assigned_channel, op_id, static_cast<unsigned long long>(wait_timeout_us));
        } else {
            ker::mod::dbg::log("[WKI] vfs_proxy_send_and_wait aborted: node=0x%04x ch=%u op=%u rc=%d", state->owner_node,
                               state->assigned_channel, op_id, WAIT_RC);
        }
        return encode_proxy_wki_status(WAIT_RC);
    }

    state->lock.lock();
    ProxyOpResult const RESULT = consume_proxy_op_result_locked(state, OP_GENERATION);
    if (RESULT.consumed) {
        unlock_proxy_slot_and_wake_next(state);
    } else {
        state->lock.unlock();
    }
    wait_for_waiter_retirement(&wait);
    int const STATUS = RESULT.status;
    uint16_t const RESP_LEN = RESULT.response_len;

    auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id),
                                     ker::mod::perf::WkiPerfPhase::END, state->owner_node, state->assigned_channel, CORRELATION, STATUS,
                                     ELAPSED_US, CALLSITE);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id), state->owner_node,
                                       state->assigned_channel, STATUS, ELAPSED_US, true, 0, perf_vfs_bytes(op_id, REQ_DATA_LEN, RESP_LEN));

    if (resp_len_out != nullptr) {
        *resp_len_out = RESP_LEN;
    }

    return normalize_proxy_status_for_errno(STATUS);
}

auto vfs_proxy_send_split_and_wait(ProxyVfsState* state, uint16_t op_id, const uint8_t* req_prefix, size_t req_prefix_len,
                                   const uint8_t* req_tail, size_t req_tail_len, void* resp_buf, uint16_t resp_buf_max) -> int {
    return vfs_proxy_send_and_wait(state, op_id, req_prefix, req_prefix_len, resp_buf, resp_buf_max, nullptr, VFS_PROXY_OP_TIMEOUT_US,
                                   nullptr, req_tail, req_tail_len);
}

void remote_vfs_close_remote_fd_best_effort(ProxyVfsState* state, int32_t remote_fd) {
    if (state == nullptr || remote_fd < 0) {
        return;
    }

    static_cast<void>(
        vfs_proxy_send_and_wait(state, OP_VFS_CLOSE, reinterpret_cast<const uint8_t*>(&remote_fd), sizeof(remote_fd), nullptr, 0));
}

auto vfs_proxy_send_untracked(ProxyVfsState* state, uint16_t op_id, const uint8_t* req_data, size_t req_data_len) -> int {
    if (state == nullptr) {
        return -EINVAL;
    }

    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint64_t const PROXY_WAIT_START = wki_now_us();

    if (req_data_len > WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload)) {
        return -EMSGSIZE;
    }
    if (req_data_len > 0 && req_data == nullptr) {
        return -EINVAL;
    }
    auto const REQ_DATA_LEN = static_cast<uint16_t>(req_data_len);
    size_t const REQ_TOTAL = sizeof(DevOpReqPayload) + req_data_len;

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init): every transmitted byte is initialized below.
    std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> req_buf;
    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
    req->op_id = op_id;
    req->data_len = REQ_DATA_LEN;

    if (req_data_len > 0) {
        memcpy(req_buf.data() + sizeof(DevOpReqPayload), req_data, req_data_len);
    }

    int const SLOT_RET = acquire_proxy_untracked_send_slot_locked(state, PROXY_WAIT_START);
    auto const PROXY_WAIT_US = static_cast<uint32_t>(wki_now_us() - PROXY_WAIT_START);
    if (PROXY_WAIT_US > 0) {
        perf_record_vfs_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::PROXY_WAIT), state->owner_node, state->assigned_channel, 0,
                              PROXY_WAIT_US, CALLSITE);
    }
    if (SLOT_RET != WKI_OK) {
        return normalize_proxy_status_for_errno(encode_proxy_wki_status(SLOT_RET));
    }

    WkiChannelIdentity const CHANNEL_IDENTITY = proxy_channel_identity_locked(state);
    uint16_t expected_seq = 0;
    if (!peek_channel_tx_seq16(CHANNEL_IDENTITY, &expected_seq)) {
        state->op_untracked_send_pending.store(false, std::memory_order_release);
        unlock_proxy_slot_and_wake_next(state);
        return -EIO;
    }
    uint32_t const CORRELATION = expected_seq;
    state->lock.unlock();

    uint64_t const STARTED_US = wki_now_us();
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id),
                                     ker::mod::perf::WkiPerfPhase::BEGIN, state->owner_node, state->assigned_channel, CORRELATION, 0,
                                     REQ_DATA_LEN, CALLSITE);

    int const SEND_RET =
        wki_send_on_channel_identity(CHANNEL_IDENTITY, MsgType::DEV_OP_REQ, req_buf.data(), static_cast<uint16_t>(REQ_TOTAL));

    state->lock.lock();
    state->op_untracked_send_pending.store(false, std::memory_order_release);
    unlock_proxy_slot_and_wake_next(state);

    auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id),
                                     ker::mod::perf::WkiPerfPhase::END, state->owner_node, state->assigned_channel, CORRELATION, SEND_RET,
                                     ELAPSED_US, CALLSITE);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id), state->owner_node,
                                       state->assigned_channel, SEND_RET, ELAPSED_US, true, 0, REQ_DATA_LEN);

    return normalize_proxy_status_for_errno(encode_proxy_wki_status(SEND_RET));
}

auto vfs_proxy_write_rdma_and_wait(ProxyVfsState* state, int32_t remote_fd, int64_t offset, const uint8_t* src, uint32_t chunk,
                                   uint32_t* written_out) -> int {
    if (state == nullptr || src == nullptr || written_out == nullptr || state->rdma_transport == nullptr ||
        state->rdma_server_write_rkey == 0 || chunk == 0 || chunk > VFS_RDMA_WRITE_SIZE) {
        return -EINVAL;
    }

    *written_out = 0;

    SharedIoSlotGuard const SHARED_IO_GUARD(state, wki_now_us());
    if (!SHARED_IO_GUARD.acquired()) {
        return normalize_proxy_status_for_errno(encode_proxy_wki_status(SHARED_IO_GUARD.result()));
    }

    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint64_t const PROXY_WAIT_START = wki_now_us();

    int const SLOT_RET = acquire_proxy_op_slot_locked(state, PROXY_WAIT_START);
    auto const PROXY_WAIT_US = static_cast<uint32_t>(wki_now_us() - PROXY_WAIT_START);
    if (PROXY_WAIT_US > 0) {
        perf_record_vfs_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::PROXY_WAIT), state->owner_node, state->assigned_channel, 0,
                              PROXY_WAIT_US, CALLSITE);
    }
    if (SLOT_RET != WKI_OK) {
        return normalize_proxy_status_for_errno(encode_proxy_wki_status(SLOT_RET));
    }

    WkiChannelIdentity const CHANNEL_IDENTITY = proxy_channel_identity_locked(state);
    uint16_t expected_seq = 0;
    if (!peek_channel_tx_seq16(CHANNEL_IDENTITY, &expected_seq)) {
        unlock_proxy_slot_and_wake_next(state);
        return -EIO;
    }
    uint32_t const CORRELATION = expected_seq;

    WkiWaitEntry wait = {};
    uint32_t resp_written = 0;
    uint64_t const OP_GENERATION = advance_proxy_op_generation_locked(state);
    state->op_wait_entry = &wait;
    state->op_waiter_pid = perf_current_pid();
    state->op_expected_id = OP_VFS_WRITE_RDMA;
    state->op_expected_seq = expected_seq;
    state->op_status = 0;
    state->op_resp_buf = &resp_written;
    state->op_resp_max = sizeof(resp_written);
    state->op_resp_len = 0;
    state->op_pending.store(true, std::memory_order_release);
    state->lock.unlock();

    uint64_t const STARTED_US = wki_now_us();
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                     perf_vfs_op(OP_VFS_WRITE_RDMA), ker::mod::perf::WkiPerfPhase::BEGIN, state->owner_node,
                                     state->assigned_channel, CORRELATION, 0, chunk, CALLSITE);

    std::array<uint8_t, 16> ctrl{};
    memcpy(ctrl.data(), &remote_fd, sizeof(int32_t));
    memcpy(ctrl.data() + 4, &offset, sizeof(int64_t));
    memcpy(ctrl.data() + 12, &chunk, sizeof(uint32_t));

    std::array<uint8_t, sizeof(DevOpReqPayload) + 16> req_buf = {};
    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
    req->op_id = OP_VFS_WRITE_RDMA;
    req->data_len = static_cast<uint16_t>(ctrl.size());
    memcpy(req_buf.data() + sizeof(DevOpReqPayload), ctrl.data(), ctrl.size());

    bool const CONTROL_FIRST = transport_is_roce(state->rdma_transport);
    if (!CONTROL_FIRST) {
        int const RDMA_RET =
            state->rdma_transport->rdma_write(state->rdma_transport, state->owner_node, state->rdma_server_write_rkey, 0, src, chunk);
        if (RDMA_RET != 0) {
            cancel_proxy_op_wait(state, wait, OP_GENERATION, RDMA_RET);

            auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
            ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                             perf_vfs_op(OP_VFS_WRITE_RDMA), ker::mod::perf::WkiPerfPhase::END, state->owner_node,
                                             state->assigned_channel, CORRELATION, RDMA_RET, ELAPSED_US, CALLSITE);
            ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(OP_VFS_WRITE_RDMA), state->owner_node,
                                               state->assigned_channel, RDMA_RET, ELAPSED_US, true, 0, chunk);
            return -EIO;
        }
    }

    int const SEND_RET =
        wki_send_on_channel_identity(CHANNEL_IDENTITY, MsgType::DEV_OP_REQ, req_buf.data(), static_cast<uint16_t>(req_buf.size()));
    if (SEND_RET != WKI_OK) {
        cancel_proxy_op_wait(state, wait, OP_GENERATION, SEND_RET);

        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                         perf_vfs_op(OP_VFS_WRITE_RDMA), ker::mod::perf::WkiPerfPhase::END, state->owner_node,
                                         state->assigned_channel, CORRELATION, SEND_RET, ELAPSED_US, CALLSITE);
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(OP_VFS_WRITE_RDMA), state->owner_node,
                                           state->assigned_channel, SEND_RET, ELAPSED_US, true, 0, chunk);
        return normalize_proxy_status_for_errno(encode_proxy_wki_status(SEND_RET));
    }

    if (CONTROL_FIRST) {
        int const RDMA_RET =
            wki_roce_rdma_write_tagged(state->owner_node, state->rdma_server_write_rkey, 0, src, chunk, static_cast<uint16_t>(CORRELATION));
        if (RDMA_RET != 0) {
            cancel_proxy_op_wait(state, wait, OP_GENERATION, RDMA_RET);

            auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
            ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                             perf_vfs_op(OP_VFS_WRITE_RDMA), ker::mod::perf::WkiPerfPhase::END, state->owner_node,
                                             state->assigned_channel, CORRELATION, RDMA_RET, ELAPSED_US, CALLSITE);
            ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(OP_VFS_WRITE_RDMA), state->owner_node,
                                               state->assigned_channel, RDMA_RET, ELAPSED_US, true, 0, chunk);
            return -EIO;
        }
    }

    int const WAIT_RC = wki_wait_for_op(&wait, VFS_PROXY_OP_TIMEOUT_US);
    if (WAIT_RC != 0) {
        cancel_proxy_op_wait(state, wait, OP_GENERATION, WAIT_RC);

        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                         perf_vfs_op(OP_VFS_WRITE_RDMA), ker::mod::perf::WkiPerfPhase::END, state->owner_node,
                                         state->assigned_channel, CORRELATION, WAIT_RC, ELAPSED_US, CALLSITE);
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(OP_VFS_WRITE_RDMA), state->owner_node,
                                           state->assigned_channel, WAIT_RC, ELAPSED_US, true, 0, chunk);
        return normalize_proxy_status_for_errno(encode_proxy_wki_status(WAIT_RC));
    }

    state->lock.lock();
    ProxyOpResult const RESULT = consume_proxy_op_result_locked(state, OP_GENERATION);
    if (RESULT.consumed) {
        unlock_proxy_slot_and_wake_next(state);
    } else {
        state->lock.unlock();
    }
    wait_for_waiter_retirement(&wait);
    int const STATUS = RESULT.status;
    uint16_t const RESP_LEN = RESULT.response_len;

    auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                     perf_vfs_op(OP_VFS_WRITE_RDMA), ker::mod::perf::WkiPerfPhase::END, state->owner_node,
                                     state->assigned_channel, CORRELATION, STATUS, ELAPSED_US, CALLSITE);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(OP_VFS_WRITE_RDMA), state->owner_node,
                                       state->assigned_channel, STATUS, ELAPSED_US, true, 0,
                                       perf_vfs_bytes(OP_VFS_WRITE_RDMA, static_cast<uint16_t>(chunk), RESP_LEN));

    *written_out = resp_written;
    return normalize_proxy_status_for_errno(STATUS);
}

constexpr uint32_t VFS_READ_RETRIES = 2;
constexpr uint32_t VFS_DIRECT_READ_STACK_SIZE = 8192;
static_assert(VFS_DIRECT_READ_STACK_SIZE <= WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload));
constexpr uint64_t VFS_RDMA_TRANSIENT_COOLDOWN_BASE_US = 10'000'000;
constexpr uint64_t VFS_RDMA_TRANSIENT_COOLDOWN_MAX_US = 60'000'000;
constexpr uint32_t VFS_RDMA_TRANSIENT_COOLDOWN_SHIFT_MAX = 3;

// Remote readlink sits on an interactive hot path (`ls -l`, path resolution,
// loader symlink traversal). A full-second first timeout turns transient
// scheduler jitter into visibly line-by-line output. Keep the first waits well
// below human-visible stalls while still being more patient than the overly
// aggressive 50 ms schedule that previously caused late responses to overlap
// with subsequent VFS ops on the same channel.
constexpr std::array<uint64_t, 3> VFS_READLINK_TIMEOUTS_US = {250000, 1000000, 5000000};

auto vfs_read_retry_timeout_us(uint16_t op_id, uint32_t attempt) -> uint64_t {
    if (op_id != OP_VFS_READLINK) {
        return VFS_PROXY_OP_TIMEOUT_US;
    }

    size_t index = attempt;
    if (index >= VFS_READLINK_TIMEOUTS_US.size()) {
        index = VFS_READLINK_TIMEOUTS_US.size() - 1;
    }
    return *std::next(VFS_READLINK_TIMEOUTS_US.begin(), static_cast<ptrdiff_t>(index));
}

auto vfs_read_status_is_retryable(int status) -> bool {
    int const RAW_STATUS = decode_proxy_wki_status(status);
    return RAW_STATUS == WKI_ERR_TIMEOUT || RAW_STATUS == WKI_ERR_NO_CREDITS || RAW_STATUS == WKI_ERR_TX_FAILED ||
           RAW_STATUS == WKI_ERR_NO_MEM;
}

auto remote_vfs_rdma_retry_ready(const std::atomic<uint64_t>& retry_after_us, uint64_t now_us) -> bool {
    uint64_t const RETRY_AFTER_US = retry_after_us.load(std::memory_order_acquire);
    return RETRY_AFTER_US == 0 || now_us >= RETRY_AFTER_US;
}

auto remote_vfs_open_prefetch_capable(ProxyVfsState* state, int flags) -> bool {
    constexpr int OPEN_ACCMODE = 0x3;
    constexpr int OPEN_WRONLY = 0x1;

    if (state == nullptr || (flags & ker::vfs::O_NO_CACHE) != 0 || (flags & ker::vfs::O_TRUNC) != 0 || (flags & ker::vfs::O_CREAT) != 0 ||
        (flags & ker::vfs::O_DIRECTORY) != 0 || (flags & OPEN_ACCMODE) == OPEN_WRONLY) {
        return false;
    }

    return !state->bulk_rdma_disabled.load(std::memory_order_acquire) &&
           remote_vfs_rdma_retry_ready(state->bulk_rdma_retry_after_us, wki_now_us()) && transport_is_roce(state->rdma_transport) &&
           state->bulk_rdma_capable && state->rdma_transport != nullptr && state->rdma_bulk_rkey != 0 && state->rdma_bulk_buf != nullptr &&
           state->rdma_bulk_size > 0;
}

void remote_vfs_rdma_note_success(std::atomic<uint32_t>& failure_count, std::atomic<uint64_t>& retry_after_us) {
    failure_count.store(0, std::memory_order_release);
    retry_after_us.store(0, std::memory_order_release);
}

auto remote_vfs_rdma_failure_should_disable(int status) -> bool {
    int const RAW_STATUS = decode_proxy_wki_status(status);
    return RAW_STATUS == WKI_ERR_NO_ROUTE || RAW_STATUS == WKI_ERR_PEER_FENCED || RAW_STATUS == WKI_ERR_INVALID ||
           RAW_STATUS == WKI_ERR_NOT_FOUND || status == -ENOTCONN || status == -EHOSTUNREACH || status == -EINVAL;
}

auto remote_vfs_rdma_note_transient_failure(std::atomic<uint32_t>& failure_count, std::atomic<uint64_t>& retry_after_us) -> uint64_t {
    uint32_t const FAILURES = failure_count.fetch_add(1, std::memory_order_acq_rel) + 1;
    uint32_t const SHIFT = std::min<uint32_t>(FAILURES - 1, VFS_RDMA_TRANSIENT_COOLDOWN_SHIFT_MAX);
    uint64_t const COOLDOWN_US = std::min<uint64_t>(VFS_RDMA_TRANSIENT_COOLDOWN_BASE_US << SHIFT, VFS_RDMA_TRANSIENT_COOLDOWN_MAX_US);
    retry_after_us.store(wki_future_deadline_us(wki_now_us(), COOLDOWN_US), std::memory_order_release);
    return COOLDOWN_US;
}

auto vfs_proxy_read_with_retry(ProxyVfsState* state, uint16_t op_id, const uint8_t* req_data, uint16_t req_data_len, void* resp_buf,
                               uint16_t resp_buf_max, uint16_t* resp_len_out = nullptr, const char* debug_path = nullptr,
                               RoceTaggedReceive* tagged_receive = nullptr) -> int {
    for (uint32_t attempt = 0;; ++attempt) {
        int const STATUS = vfs_proxy_send_and_wait(state, op_id, req_data, req_data_len, resp_buf, resp_buf_max, resp_len_out,
                                                   vfs_read_retry_timeout_us(op_id, attempt), tagged_receive);
        if (!vfs_read_status_is_retryable(STATUS) || attempt >= VFS_READ_RETRIES) {
            return normalize_proxy_status_for_errno(STATUS);
        }

        perf_record_vfs_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::RETRY), state->owner_node, state->assigned_channel, STATUS,
                              attempt + 1, WOS_PERF_CALLSITE());

        if (op_id == OP_VFS_READLINK && debug_path != nullptr) {
            ker::mod::dbg::log("[WKI] retrying VFS readlink after transient failure: node=0x%04x ch=%u path='%s' rc=%d attempt=%u",
                               state->owner_node, state->assigned_channel, debug_path, STATUS, attempt + 1);
        } else {
            ker::mod::dbg::log("[WKI] retrying VFS read after transient failure: node=0x%04x ch=%u op=%u rc=%d attempt=%u",
                               state->owner_node, state->assigned_channel, op_id, STATUS, attempt + 1);
        }
    }
}

// -----------------------------------------------------------------------------
// D6: Write-behind flush helper
// -----------------------------------------------------------------------------

constexpr std::array<uint32_t, 8> VFS_WRITE_BEHIND_CAPACITIES = {
    28U * 1024U, 60U * 1024U, 124U * 1024U, 252U * 1024U, 508U * 1024U, 1020U * 1024U, 2044U * 1024U, 4092U * 1024U,
};
constexpr uint32_t VFS_WRITE_BEHIND_MAX_CAPACITY = 4092U * 1024U;
static_assert(VFS_WRITE_BEHIND_MAX_CAPACITY < VFS_WRITE_BEHIND_SIZE);

auto write_behind_capacity_for(uint64_t required_bytes) -> uint32_t {
    for (uint32_t const CAPACITY : VFS_WRITE_BEHIND_CAPACITIES) {
        if (required_bytes <= CAPACITY) {
            return CAPACITY;
        }
    }
    return VFS_WRITE_BEHIND_MAX_CAPACITY;
}

// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
auto install_write_behind_storage(WriteBehindBuffer* wb, std::unique_ptr<uint8_t[]> replacement, uint32_t replacement_capacity) -> bool {
    if (wb == nullptr || replacement == nullptr || replacement_capacity == 0 || replacement_capacity > VFS_WRITE_BEHIND_MAX_CAPACITY ||
        wb->pending_len > replacement_capacity || (wb->pending_len > 0 && wb->data == nullptr)) {
        return false;
    }

    if (wb->pending_len > 0) {
        memcpy(replacement.get(), wb->data.get(), wb->pending_len);
    }
    wb->data = std::move(replacement);
    wb->capacity = replacement_capacity;
    return true;
}

auto try_reserve_write_behind(WriteBehindBuffer* wb, uint64_t required_bytes) -> bool {
    if (wb == nullptr) {
        return false;
    }

    uint32_t const TARGET_CAPACITY = write_behind_capacity_for(required_bytes);
    if (wb->data != nullptr && TARGET_CAPACITY <= wb->capacity) {
        return true;
    }

    // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    std::unique_ptr<uint8_t[]> replacement{new (std::nothrow) uint8_t[TARGET_CAPACITY]};
    return install_write_behind_storage(wb, std::move(replacement), TARGET_CAPACITY);
}

// The caller holds ctx->io_lock so storage promotion cannot invalidate src
// while an RDMA or message flush is sleeping on the remote operation.
auto flush_write_behind(RemoteFileContext* ctx) -> int {
    if (ctx->write_buf == nullptr || ctx->write_buf->pending_len == 0) {
        return 0;
    }

    auto* wb = ctx->write_buf;
    if (wb->data == nullptr || wb->pending_len > wb->capacity) {
        return -EIO;
    }
    remote_vfs_invalidate_cached_stat(ctx);
    auto* src = wb->data.get();
    auto remaining = wb->pending_len;
    auto cur_offset = wb->pending_offset;
    auto keep_pending_tail = [&](const uint8_t* tail_src, uint32_t tail_len, int64_t tail_offset) {
        if (tail_len == 0) {
            wb->pending_offset = -1;
            wb->pending_len = 0;
            return;
        }

        if (tail_src != wb->data.get()) {
            memmove(wb->data.get(), tail_src, tail_len);
        }
        wb->pending_offset = tail_offset;
        wb->pending_len = tail_len;
    };

    // RDMA path: use the server's pre-registered receive buffer and send only
    // a tiny control message over WKI. Avoids embedding file data in WKI packets.
    if (transport_supports_vfs_write_push_rdma(ctx->proxy->rdma_transport) && ctx->proxy->rdma_transport != nullptr &&
        ctx->proxy->rdma_server_write_rkey != 0) {
        while (remaining > 0) {
            uint32_t const CHUNK = std::min(remaining, VFS_RDMA_WRITE_SIZE);

            uint32_t written = 0;
            int const STATUS = vfs_proxy_write_rdma_and_wait(ctx->proxy, ctx->remote_fd, cur_offset, src, CHUNK, &written);
            if (STATUS != 0) {
                keep_pending_tail(src, remaining, cur_offset);
                return STATUS;
            }

            if (written == 0 || written > CHUNK) {
                keep_pending_tail(src, remaining, cur_offset);
                return -EIO;
            }
            src += written;
            cur_offset += static_cast<int64_t>(written);
            remaining -= written;
        }
    } else {
        // Message path (fallback): embed data in the WKI packet, capped to ethernet MTU
        constexpr uint32_t WRITE_HDR_OVERHEAD = sizeof(DevOpReqPayload) + 12;
        auto max_data = static_cast<uint32_t>(WKI_ETH_MAX_PAYLOAD - WRITE_HDR_OVERHEAD);

        while (remaining > 0) {
            uint32_t const CHUNK = (remaining > max_data) ? max_data : remaining;

            std::array<uint8_t, 12> req_prefix{};
            int32_t remote_fd = ctx->remote_fd;
            memcpy(req_prefix.data(), &remote_fd, sizeof(int32_t));
            memcpy(req_prefix.data() + 4, &cur_offset, sizeof(int64_t));

            uint32_t written = 0;
            int const STATUS = vfs_proxy_send_split_and_wait(ctx->proxy, OP_VFS_WRITE, req_prefix.data(), req_prefix.size(), src, CHUNK,
                                                             &written, sizeof(uint32_t));
            if (STATUS != 0) {
                keep_pending_tail(src, remaining, cur_offset);
                return STATUS;
            }
            if (written == 0 || written > CHUNK) {
                keep_pending_tail(src, remaining, cur_offset);
                return -EIO;
            }

            src += written;
            cur_offset += static_cast<int64_t>(written);
            remaining -= written;
        }
    }

    wb->pending_offset = -1;
    wb->pending_len = 0;
    return 0;
}

// -----------------------------------------------------------------------------
// Consumer-side FileOperations
// -----------------------------------------------------------------------------

auto remote_vfs_close(ker::vfs::File* f) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return -EBADF;
    }
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    ProxyVfsState* const PROXY = ctx->proxy;
    if (PROXY == nullptr || !PROXY->active) {
        {
            ker::mod::sys::MutexGuard io_guard(ctx->io_lock);
            clear_remote_file_open_prefetch(ctx);
            delete ctx->read_cache;
            delete ctx->write_buf;
        }
        delete ctx;
        f->private_data = nullptr;
        release_vfs_proxy_open_ref(PROXY);
        return -EIO;
    }

    int flush_status = 0;
    {
        ker::mod::sys::MutexGuard io_guard(ctx->io_lock);

        // D6: Flush pending writes before closing
        flush_status = flush_write_behind(ctx);

        // D7: Invalidate directory cache for this file
        s_vfs_lock.lock();
        invalidate_dir_cache(ctx->proxy, ctx->remote_fd);
        s_vfs_lock.unlock();

        // Pending writes were flushed synchronously above, and this close remains
        // ordered on the reliable per-mount channel. Legacy four-byte closes need
        // a response; ordinary closes opt out of the success response because the
        // local descriptor is gone.
        std::array<uint8_t, WKI_VFS_CLOSE_EXTENDED_DATA_LEN> close_req{};
        int32_t const REMOTE_FD = ctx->remote_fd;
        memcpy(close_req.data(), &REMOTE_FD, sizeof(REMOTE_FD));
        close_req.at(WKI_VFS_CLOSE_FLAGS_OFFSET) = WKI_VFS_CLOSE_FLAG_NO_SUCCESS_RESPONSE;
        int const SEND_STATUS = vfs_proxy_send_untracked(ctx->proxy, OP_VFS_CLOSE, close_req.data(), close_req.size());
        if (SEND_STATUS != 0) {
            ker::mod::dbg::log("[WKI] async remote close send failed: node=0x%04x ch=%u fd=%d rc=%d", ctx->proxy->owner_node,
                               ctx->proxy->assigned_channel, REMOTE_FD, SEND_STATUS);
        }

        // D6: Free caches
        clear_remote_file_open_prefetch(ctx);
        delete ctx->read_cache;
        delete ctx->write_buf;
    }

    delete ctx;
    f->private_data = nullptr;
    release_vfs_proxy_open_ref(PROXY);
    return flush_status;
}

auto remote_vfs_read(ker::vfs::File* f, void* buf, size_t count, size_t offset) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    if (ctx->proxy == nullptr || !ctx->proxy->active) {
        return -EIO;
    }
    ker::mod::sys::MutexGuard io_guard(ctx->io_lock);
    consume_remote_file_cache_invalidation(ctx);

    if (ker::vfs::vfs_cache_notify_file_dirty(f)) {
        invalidate_remote_file_open_caches(ctx);
        ker::vfs::vfs_cache_notify_acknowledge_file(f);
    }

    // D6: Flush any pending writes to ensure read-after-write consistency
    if (ctx->write_buf != nullptr && ctx->write_buf->pending_len > 0) {
        int const FLUSH_STATUS = flush_write_behind(ctx);
        if (FLUSH_STATUS != 0) {
            return FLUSH_STATUS;
        }
    }

    auto* dest = static_cast<uint8_t*>(buf);
    auto remaining = static_cast<uint32_t>(count);
    auto cur_offset = static_cast<int64_t>(offset);
    ssize_t total_read = 0;
    bool const POSITIONAL_READ = f->positional_read_depth.load(std::memory_order_acquire) != 0;
    bool const ALLOW_READ_CACHES = (f->open_flags & ker::vfs::O_NO_CACHE) == 0;

    if (ALLOW_READ_CACHES && ctx->open_prefetch_buf != nullptr && ctx->open_prefetch_len > 0 && cur_offset >= ctx->open_prefetch_offset) {
        int64_t const PREFETCH_END = ctx->open_prefetch_offset + static_cast<int64_t>(ctx->open_prefetch_len);
        if (cur_offset < PREFETCH_END) {
            auto off_in_buf = static_cast<uint32_t>(cur_offset - ctx->open_prefetch_offset);
            auto available = static_cast<uint32_t>(PREFETCH_END - cur_offset);
            auto to_copy = std::min(remaining, available);

            memcpy(dest, ctx->open_prefetch_buf + off_in_buf, to_copy);
            dest += to_copy;
            cur_offset += static_cast<int64_t>(to_copy);
            remaining -= to_copy;
            total_read += static_cast<ssize_t>(to_copy);

            if (remaining == 0) {
                return total_read;
            }

            ker::vfs::Stat cached_stat = {};
            if (remote_vfs_try_copy_cached_stat(ctx, &cached_stat) && cached_stat.st_size >= 0 && cur_offset >= cached_stat.st_size) {
                return total_read;
            }
        }
    }

    // -- Bulk RDMA path ----------------------------------------------------
    // Two modes:
    //  (a) Direct bulk - positional reads and request > 64 KB: issue
    //      OP_VFS_READ_BULK for each exact offset/range chunk.
    //  (b) Prefetch   - non-positional request <= 64 KB (typical cp/cat): on
    //      the first small read (or cache miss), pre-fetch up to the transport
    //      bulk window and serve subsequent sequential reads from it. This
    //      turns hundreds of 4 KB RPCs into one bulk RDMA transfer.
    //
    // The bulk buffer is per-mount; `bulk_owner_fd` tracks which open file last
    // filled it.  When ownership changes the old prefetch is implicitly stale.
    // Bulk RDMA path: pull mode (RoCE) checked first, then push mode (ivshmem).
    // Pull: server stages data in its bulk staging buf; client rdma_reads to fetch.
    // Push: server rdma_writes directly into client's bulk buf (consumer_rkey).
    bool skip_read_rdma_this_call = false;
    const bool BULK_READ_ENABLED = !ctx->proxy->bulk_rdma_disabled.load(std::memory_order_acquire) &&
                                   remote_vfs_rdma_retry_ready(ctx->proxy->bulk_rdma_retry_after_us, wki_now_us());
    const bool BULK_ROCE_PUSH_CAPABLE = BULK_READ_ENABLED && transport_is_roce(ctx->proxy->rdma_transport) &&
                                        ctx->proxy->bulk_rdma_capable && ctx->proxy->rdma_transport != nullptr &&
                                        ctx->proxy->rdma_bulk_rkey != 0 && ctx->proxy->rdma_bulk_buf != nullptr;
    const bool BULK_PULL_CAPABLE = !BULK_ROCE_PUSH_CAPABLE && BULK_READ_ENABLED &&
                                   transport_supports_rdma_read_pull(ctx->proxy->rdma_transport) && ctx->proxy->bulk_rdma_capable &&
                                   ctx->proxy->rdma_transport != nullptr && ctx->proxy->rdma_server_bulk_staging_rkey != 0 &&
                                   ctx->proxy->rdma_bulk_buf != nullptr;
    const bool BULK_PUSH_CAPABLE = !BULK_PULL_CAPABLE && transport_supports_vfs_read_push_rdma(ctx->proxy->rdma_transport) &&
                                   BULK_READ_ENABLED && ctx->proxy->bulk_rdma_capable && ctx->proxy->rdma_transport != nullptr &&
                                   ctx->proxy->rdma_bulk_rkey != 0 && ctx->proxy->rdma_bulk_buf != nullptr;
    if (BULK_PULL_CAPABLE || BULK_ROCE_PUSH_CAPABLE || BULK_PUSH_CAPABLE) {
        SharedIoSlotGuard const SHARED_IO_GUARD(ctx->proxy, wki_now_us());
        if (!SHARED_IO_GUARD.acquired()) {
            skip_read_rdma_this_call = true;
        } else {
            int bulk_error = 0;
#ifdef WKI_DEBUG
            if (cur_offset == 0) {
                ker::mod::dbg::log("[WKI] remote_vfs_read: BULK RDMA %s ch=%u rd=%d count=%u", BULK_PULL_CAPABLE ? "pull" : "push",
                                   ctx->proxy->assigned_channel, ctx->remote_fd, remaining);
            }
#endif
            bool const USE_BULK_PREFETCH_CACHE = ALLOW_READ_CACHES;

            // -- (b) Prefetch path: try to satisfy from the existing bulk cache --
            if (USE_BULK_PREFETCH_CACHE && ctx->proxy->bulk_owner_fd == ctx->remote_fd && ctx->bulk_cached_len > 0 &&
                cur_offset >= ctx->bulk_cached_offset &&
                cur_offset < ctx->bulk_cached_offset + static_cast<int64_t>(ctx->bulk_cached_len)) {
                auto off_in_buf = static_cast<uint32_t>(cur_offset - ctx->bulk_cached_offset);
                auto available = ctx->bulk_cached_len - off_in_buf;
                auto to_copy = std::min(remaining, available);

                memcpy(dest, ctx->proxy->rdma_bulk_buf + off_in_buf, to_copy);
                dest += to_copy;
                cur_offset += static_cast<int64_t>(to_copy);
                remaining -= to_copy;
                total_read += static_cast<ssize_t>(to_copy);

                if (remaining == 0) {
                    return total_read;
                }
                // Partial hit - fall through to refill below
            }

            // -- Bulk fetch: either (a) direct or (b) prefetch refill ----------
            while (remaining > 0) {
                uint32_t fetch_limit = ctx->proxy->rdma_bulk_size;
                if (BULK_PULL_CAPABLE) {
                    fetch_limit = std::min(fetch_limit, VFS_RDMA_ROCE_BULK_SIZE);
                }

                bool const PREFETCH_THIS_FETCH = ALLOW_READ_CACHES && remaining <= VFS_RDMA_BOUNCE_SIZE;
                int64_t fetch_offset = cur_offset;

                // For small reads, including positional page-fault style preads,
                // prefetch the full safe bulk window and cache it by offset. This
                // keeps file-backed mmap from degenerating into one tiny RoCE read
                // transaction per page on cold start.
                uint32_t fetch_size = std::min(remaining, fetch_limit);
                if (PREFETCH_THIS_FETCH) {
                    if (POSITIONAL_READ) {
                        fetch_size = std::min(fetch_limit, VFS_POSITIONAL_PREFETCH_SIZE);
                        uint64_t const ALIGN = fetch_size;
                        fetch_offset = static_cast<int64_t>((static_cast<uint64_t>(cur_offset) / ALIGN) * ALIGN);
                    } else {
                        fetch_size = fetch_limit;
                    }
                }

                // Pull mode sends rkey=0 (server ignores it; data goes to staging buf).
                // Push mode sends the client's bulk buf rkey.
                uint32_t req_rkey = BULK_PULL_CAPABLE ? 0 : ctx->proxy->rdma_bulk_rkey;
                std::array<uint8_t, 20> req{};
                memcpy(req.data(), &ctx->remote_fd, sizeof(int32_t));
                memcpy(req.data() + 4, &fetch_size, sizeof(uint32_t));
                memcpy(req.data() + 8, &fetch_offset, sizeof(int64_t));
                memcpy(req.data() + 16, &req_rkey, sizeof(uint32_t));

                RoceTaggedReceive tagged_receive{};
                RoceTaggedReceive* tagged_receive_ptr = nullptr;
                if (BULK_ROCE_PUSH_CAPABLE) {
                    tagged_receive.rkey = ctx->proxy->rdma_bulk_rkey;
                    tagged_receive_ptr = &tagged_receive;
                }

                uint32_t bytes_read = 0;
                int const STATUS = vfs_proxy_read_with_retry(ctx->proxy, OP_VFS_READ_BULK, req.data(), 20, &bytes_read, sizeof(uint32_t),
                                                             nullptr, nullptr, tagged_receive_ptr);
                if (STATUS != 0) {
                    bulk_error = STATUS;
                    // Invalidate cache on error
                    ctx->bulk_cached_len = 0;
                    ctx->proxy->bulk_owner_fd = -1;
                    break;
                }
                if (bytes_read == 0) {
                    ctx->bulk_cached_len = 0;
                    ctx->proxy->bulk_owner_fd = -1;
                    break;
                }

                if (BULK_PULL_CAPABLE) {
                    // Pull mode: server staged data; fetch it via rdma_read from staging buf.
                    int const RDMA_RET = ctx->proxy->rdma_transport->rdma_read(ctx->proxy->rdma_transport, ctx->proxy->owner_node,
                                                                               ctx->proxy->rdma_server_bulk_staging_rkey, 0,
                                                                               ctx->proxy->rdma_bulk_buf, bytes_read);
                    if (RDMA_RET != 0) {
                        bulk_error = RDMA_RET;
                        ctx->bulk_cached_len = 0;
                        ctx->proxy->bulk_owner_fd = -1;
                        break;
                    }
                } else if (BULK_ROCE_PUSH_CAPABLE) {
                    if (!wki_roce_region_wait_tagged_write(ctx->proxy->rdma_bulk_rkey, tagged_receive.cookie, bytes_read,
                                                           VFS_PROXY_OP_TIMEOUT_US)) {
                        bulk_error = -ETIMEDOUT;
                        ctx->bulk_cached_len = 0;
                        ctx->proxy->bulk_owner_fd = -1;
                        break;
                    }
                } else {
                    // Push mode: server wrote data into shared memory at rdma_bulk_rkey.
                    int const RDMA_RET = ctx->proxy->rdma_transport->rdma_read(ctx->proxy->rdma_transport, 0, ctx->proxy->rdma_bulk_rkey, 0,
                                                                               ctx->proxy->rdma_bulk_buf, bytes_read);
                    if (RDMA_RET != 0) {
                        bulk_error = RDMA_RET;
                        ctx->bulk_cached_len = 0;
                        ctx->proxy->bulk_owner_fd = -1;
                        break;
                    }
                }
                remote_vfs_rdma_note_success(ctx->proxy->bulk_rdma_failure_count, ctx->proxy->bulk_rdma_retry_after_us);
                if (BULK_PULL_CAPABLE || BULK_ROCE_PUSH_CAPABLE) {
                    remote_vfs_rdma_note_success(ctx->proxy->rdma_read_failure_count, ctx->proxy->rdma_read_retry_after_us);
                }

                if (PREFETCH_THIS_FETCH) {
                    ctx->proxy->bulk_owner_fd = ctx->remote_fd;
                    ctx->bulk_cached_offset = fetch_offset;
                    ctx->bulk_cached_len = bytes_read;
                } else {
                    ctx->bulk_cached_len = 0;
                    ctx->proxy->bulk_owner_fd = -1;
                }

                auto off_in_buf = static_cast<uint32_t>(cur_offset - fetch_offset);
                if (off_in_buf >= bytes_read) {
                    ctx->bulk_cached_len = 0;
                    ctx->proxy->bulk_owner_fd = -1;
                    break;
                }

                auto available = bytes_read - off_in_buf;
                auto to_copy = std::min(remaining, available);
                memcpy(dest, ctx->proxy->rdma_bulk_buf + off_in_buf, to_copy);

                dest += to_copy;
                cur_offset += static_cast<int64_t>(to_copy);
                remaining -= to_copy;
                total_read += static_cast<ssize_t>(to_copy);

                if (bytes_read < fetch_size) {
                    break;  // Short read: EOF
                }

                // One prefetch window is enough; subsequent small reads, including
                // positional page faults, should hit the cache at the top.
                if (PREFETCH_THIS_FETCH) {
                    break;
                }
            }
            if (total_read > 0) {
                return total_read;
            }
            if (bulk_error == 0) {
                return 0;
            }

            bool const DISABLE_RDMA = remote_vfs_rdma_failure_should_disable(bulk_error);
            uint64_t cooldown_us = 0;
            if (DISABLE_RDMA) {
                ctx->proxy->bulk_rdma_disabled.store(true, std::memory_order_release);
                if (BULK_PULL_CAPABLE || BULK_ROCE_PUSH_CAPABLE) {
                    ctx->proxy->rdma_read_disabled.store(true, std::memory_order_release);
                }
            } else {
                cooldown_us =
                    remote_vfs_rdma_note_transient_failure(ctx->proxy->bulk_rdma_failure_count, ctx->proxy->bulk_rdma_retry_after_us);
                if (BULK_PULL_CAPABLE || BULK_ROCE_PUSH_CAPABLE) {
                    remote_vfs_rdma_note_transient_failure(ctx->proxy->rdma_read_failure_count, ctx->proxy->rdma_read_retry_after_us);
                    skip_read_rdma_this_call = true;
                }
            }

            ctx->bulk_cached_offset = -1;
            ctx->bulk_cached_len = 0;
            ctx->proxy->bulk_owner_fd = -1;
            ker::mod::dbg::log(
                "[WKI] remote_vfs_read: bulk RDMA failed node=0x%04x ch=%u fd=%d rc=%d; falling back to read path (%s cooldown=%lluus)",
                ctx->proxy->owner_node, ctx->proxy->assigned_channel, ctx->remote_fd, bulk_error, DISABLE_RDMA ? "disabled" : "transient",
                static_cast<unsigned long long>(cooldown_us));

            dest = static_cast<uint8_t*>(buf);
            remaining = static_cast<uint32_t>(count);
            cur_offset = static_cast<int64_t>(offset);
            total_read = 0;
        }
    }

    // RDMA path: 64 KB per round-trip vs ~1400 B per message.
    // Pull mode (RoCE): server stages data; client rdma_reads from server staging buf.
    // Push mode (ivshmem): server rdma_writes to client's bounce buf; client reads locally.
    const bool RDMA_READ_ENABLED = !skip_read_rdma_this_call && !ctx->proxy->rdma_read_disabled.load(std::memory_order_acquire) &&
                                   remote_vfs_rdma_retry_ready(ctx->proxy->rdma_read_retry_after_us, wki_now_us());
    const bool RDMA_ROCE_PUSH_CAPABLE = RDMA_READ_ENABLED && transport_is_roce(ctx->proxy->rdma_transport) && ctx->proxy->rdma_capable &&
                                        ctx->proxy->rdma_transport != nullptr && ctx->proxy->rdma_read_rkey != 0 &&
                                        ctx->proxy->rdma_bounce_buf != nullptr;
    const bool RDMA_PULL_CAPABLE = !RDMA_ROCE_PUSH_CAPABLE && RDMA_READ_ENABLED &&
                                   transport_supports_rdma_read_pull(ctx->proxy->rdma_transport) && ctx->proxy->rdma_capable &&
                                   ctx->proxy->rdma_transport != nullptr && ctx->proxy->rdma_server_read_staging_rkey != 0 &&
                                   ctx->proxy->rdma_bounce_buf != nullptr;
    const bool RDMA_PUSH_CAPABLE = !RDMA_PULL_CAPABLE && transport_supports_vfs_read_push_rdma(ctx->proxy->rdma_transport) &&
                                   RDMA_READ_ENABLED && ctx->proxy->rdma_capable && ctx->proxy->rdma_transport != nullptr &&
                                   ctx->proxy->rdma_read_rkey != 0 && ctx->proxy->rdma_bounce_buf != nullptr;
    if (RDMA_PULL_CAPABLE || RDMA_ROCE_PUSH_CAPABLE || RDMA_PUSH_CAPABLE) {
        SharedIoSlotGuard const SHARED_IO_GUARD(ctx->proxy, wki_now_us());
        if (SHARED_IO_GUARD.acquired()) {
            int rdma_error = 0;
            while (remaining > 0) {
                uint32_t chunk = std::min(remaining, VFS_RDMA_BOUNCE_SIZE);

                // Pull mode sends rkey=0 (server stages locally); push mode sends client bounce buf rkey.
                uint32_t req_rkey = RDMA_PULL_CAPABLE ? 0 : ctx->proxy->rdma_read_rkey;
                // Request: {fd:i32, len:u32, off:i64, consumer_rkey:u32} = 20 bytes
                std::array<uint8_t, 20> req{};
                memcpy(req.data(), &ctx->remote_fd, sizeof(int32_t));
                memcpy(req.data() + 4, &chunk, sizeof(uint32_t));
                memcpy(req.data() + 8, &cur_offset, sizeof(int64_t));
                memcpy(req.data() + 16, &req_rkey, sizeof(uint32_t));

                // Response: {bytes_read:u32} = 4 bytes
                RoceTaggedReceive tagged_receive{};
                RoceTaggedReceive* tagged_receive_ptr = nullptr;
                if (RDMA_ROCE_PUSH_CAPABLE) {
                    tagged_receive.rkey = ctx->proxy->rdma_read_rkey;
                    tagged_receive_ptr = &tagged_receive;
                }

                uint32_t bytes_read = 0;
                int const STATUS = vfs_proxy_read_with_retry(ctx->proxy, OP_VFS_READ_RDMA, req.data(), 20, &bytes_read, sizeof(uint32_t),
                                                             nullptr, nullptr, tagged_receive_ptr);
                if (STATUS != 0) {
                    rdma_error = STATUS;
                    break;
                }
                if (bytes_read == 0) {
                    break;
                }

                auto to_copy = std::min(bytes_read, remaining);
                if (RDMA_PULL_CAPABLE) {
                    // Pull mode: server staged data; fetch via rdma_read from server's staging buf.
                    int const RDMA_RET = ctx->proxy->rdma_transport->rdma_read(ctx->proxy->rdma_transport, ctx->proxy->owner_node,
                                                                               ctx->proxy->rdma_server_read_staging_rkey, 0,
                                                                               ctx->proxy->rdma_bounce_buf, to_copy);
                    if (RDMA_RET != 0) {
                        rdma_error = RDMA_RET;
                        break;
                    }
                } else if (RDMA_ROCE_PUSH_CAPABLE) {
                    if (!wki_roce_region_wait_tagged_write(ctx->proxy->rdma_read_rkey, tagged_receive.cookie, to_copy,
                                                           VFS_PROXY_OP_TIMEOUT_US)) {
                        rdma_error = -ETIMEDOUT;
                        break;
                    }
                } else {
                    // Push mode: server wrote into shared memory at rdma_read_rkey; read locally.
                    int const RDMA_RET = ctx->proxy->rdma_transport->rdma_read(ctx->proxy->rdma_transport, 0, ctx->proxy->rdma_read_rkey, 0,
                                                                               ctx->proxy->rdma_bounce_buf, to_copy);
                    if (RDMA_RET != 0) {
                        rdma_error = RDMA_RET;
                        break;
                    }
                }
                remote_vfs_rdma_note_success(ctx->proxy->rdma_read_failure_count, ctx->proxy->rdma_read_retry_after_us);
                memcpy(dest, ctx->proxy->rdma_bounce_buf, to_copy);

                dest += to_copy;
                cur_offset += static_cast<int64_t>(to_copy);
                remaining -= to_copy;
                total_read += static_cast<ssize_t>(to_copy);

                if (bytes_read < chunk) {
                    break;  // Short read: EOF
                }
            }
            if (total_read > 0) {
                return total_read;
            }
            if (rdma_error == 0) {
                return 0;
            }

            bool const DISABLE_RDMA = remote_vfs_rdma_failure_should_disable(rdma_error);
            uint64_t cooldown_us = 0;
            if (DISABLE_RDMA) {
                ctx->proxy->rdma_read_disabled.store(true, std::memory_order_release);
            } else {
                cooldown_us =
                    remote_vfs_rdma_note_transient_failure(ctx->proxy->rdma_read_failure_count, ctx->proxy->rdma_read_retry_after_us);
            }
            ker::mod::dbg::log(
                "[WKI] remote_vfs_read: RDMA read failed node=0x%04x ch=%u fd=%d rc=%d; falling back to message path (%s cooldown=%lluus)",
                ctx->proxy->owner_node, ctx->proxy->assigned_channel, ctx->remote_fd, rdma_error, DISABLE_RDMA ? "disabled" : "transient",
                static_cast<unsigned long long>(cooldown_us));

            dest = static_cast<uint8_t*>(buf);
            remaining = static_cast<uint32_t>(count);
            cur_offset = static_cast<int64_t>(offset);
            total_read = 0;
        }
    }

    // Message path (fallback): data embedded in DEV_OP_RESP, capped to ethernet MTU.
    // D6: Check read-ahead cache first
    if (ALLOW_READ_CACHES && !POSITIONAL_READ && ctx->read_cache != nullptr && ctx->read_cache->cached_len > 0) {
        auto* rc = ctx->read_cache;
        int64_t const CACHE_END = rc->cached_offset + rc->cached_len;

        if (cur_offset >= rc->cached_offset && cur_offset < CACHE_END) {
            // Cache hit (full or partial)
            auto cache_off = static_cast<uint16_t>(cur_offset - rc->cached_offset);
            auto available = static_cast<uint16_t>(rc->cached_len - cache_off);
            auto to_copy = static_cast<uint16_t>(std::min(static_cast<uint32_t>(available), remaining));

            memcpy(dest, rc->data.data() + cache_off, to_copy);
            dest += to_copy;
            cur_offset += to_copy;
            remaining -= to_copy;
            total_read += to_copy;

            if (remaining == 0) {
                return total_read;
            }
        }
    }

    // Max data per response
    auto max_resp_data = static_cast<uint32_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload));

    // The response path initializes the exact resp_len prefix consumed below.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<uint8_t, VFS_DIRECT_READ_STACK_SIZE> direct_read_buf __attribute__((uninitialized));

    while (remaining > 0) {
        bool const SHOULD_READ_AHEAD = ALLOW_READ_CACHES && !POSITIONAL_READ && remaining < VFS_CACHE_SIZE;
        if (SHOULD_READ_AHEAD && ctx->read_cache == nullptr) {
            ctx->read_cache = new (std::nothrow) ReadAheadCache;  // NOLINT(cppcoreguidelines-owning-memory)
        }
        bool const USING_CACHE = SHOULD_READ_AHEAD && ctx->read_cache != nullptr;
        auto fetch_size = USING_CACHE ? static_cast<uint32_t>(VFS_CACHE_SIZE) : std::min(remaining, VFS_DIRECT_READ_STACK_SIZE);
        fetch_size = std::min(fetch_size, max_resp_data);

        uint8_t* fetch_dest = direct_read_buf.data();
        if (USING_CACHE) {
            fetch_dest = ctx->read_cache->data.data();
        }

        uint32_t chunk = fetch_size;

        // Build request: {remote_fd:i32, len:u32, offset:i64} = 16 bytes
        std::array<uint8_t, 16> req_data{};
        memcpy(req_data.data(), &ctx->remote_fd, sizeof(int32_t));
        memcpy(req_data.data() + 4, &chunk, sizeof(uint32_t));
        memcpy(req_data.data() + 8, &cur_offset, sizeof(int64_t));

        uint16_t resp_len = 0;
        int const STATUS =
            vfs_proxy_read_with_retry(ctx->proxy, OP_VFS_READ, req_data.data(), 16, fetch_dest, static_cast<uint16_t>(chunk), &resp_len);
        if (STATUS != 0) {
            return (total_read > 0) ? total_read : STATUS;
        }

        uint16_t const BYTES_READ = resp_len;
        if (BYTES_READ == 0) {
            break;  // EOF
        }
#ifdef WKI_DEBUG
        // Diagnostic: log first chunk per read call
        if (total_read == 0 && bytes_read > 3) {
            ker::mod::dbg::log("[WKI] msg_read: ch=%u rd=%d off=%ld bytes=%u data=[%02x %02x %02x %02x]", ctx->proxy->assigned_channel,
                               ctx->remote_fd, static_cast<long>(cur_offset), bytes_read, fetch_dest[0], fetch_dest[1], fetch_dest[2],
                               fetch_dest[3]);
        }
#endif
        if (USING_CACHE) {
            // Fill read-ahead cache and copy requested portion to caller
            ctx->read_cache->cached_offset = cur_offset;
            ctx->read_cache->cached_len = BYTES_READ;

            auto to_copy = static_cast<uint16_t>(std::min(static_cast<uint32_t>(BYTES_READ), remaining));
            memcpy(dest, ctx->read_cache->data.data(), to_copy);

            dest += to_copy;
            cur_offset += to_copy;
            remaining -= to_copy;
            total_read += to_copy;
        } else {
            memcpy(dest, fetch_dest, BYTES_READ);
            dest += BYTES_READ;
            cur_offset += BYTES_READ;
            remaining -= BYTES_READ;
            total_read += BYTES_READ;
        }

        if (BYTES_READ < chunk) {
            break;  // Short read (EOF or partial)
        }
    }

    return total_read;
}

auto remote_vfs_write(ker::vfs::File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    if (ctx->proxy == nullptr || !ctx->proxy->active) {
        return -EIO;
    }
    ker::mod::sys::MutexGuard io_guard(ctx->io_lock);
    consume_remote_file_cache_invalidation(ctx);

    remote_vfs_invalidate_cached_stat(ctx);

    // D6: Invalidate read-ahead cache on write (stale data)
    if (ctx->read_cache != nullptr) {
        ctx->read_cache->cached_len = 0;
        ctx->read_cache->cached_offset = -1;
    }

    // Invalidate bulk prefetch cache on write
    clear_remote_file_open_prefetch(ctx);
    ctx->bulk_cached_len = 0;
    ctx->bulk_cached_offset = -1;

    const auto* src = static_cast<const uint8_t*>(buf);
    auto remaining = static_cast<uint32_t>(count);
    auto cur_offset = static_cast<int64_t>(offset);
    ssize_t total_written = 0;
    bool allow_write_behind_growth = true;

    // D6: Try to buffer sequential writes
    while (remaining > 0) {
        // Lazily allocate write-behind buffer
        if (ctx->write_buf == nullptr) {
            ctx->write_buf = new (std::nothrow) WriteBehindBuffer();  // NOLINT(cppcoreguidelines-owning-memory)
            if (ctx->write_buf == nullptr) {
                return (total_written > 0) ? total_written : -ENOMEM;
            }
        }

        auto* wb = ctx->write_buf;

        // Check if this write is sequential and fits in the buffer
        bool const IS_SEQUENTIAL = (wb->pending_len == 0) || (wb->pending_offset + wb->pending_len == cur_offset);
        if (!IS_SEQUENTIAL) {
            int const FLUSH_STATUS = flush_write_behind(ctx);
            if (FLUSH_STATUS != 0) {
                return (total_written > 0) ? total_written : FLUSH_STATUS;
            }
            continue;
        }

        uint64_t const REQUIRED_CAPACITY = static_cast<uint64_t>(wb->pending_len) + remaining;
        if (allow_write_behind_growth && REQUIRED_CAPACITY > wb->capacity && wb->capacity < VFS_WRITE_BEHIND_MAX_CAPACITY &&
            !try_reserve_write_behind(wb, REQUIRED_CAPACITY)) {
            // Keep using the existing allocation after one failed promotion;
            // repeated reclaim attempts in the same write would only add churn.
            allow_write_behind_growth = false;
        }

        if (wb->data == nullptr || wb->capacity == 0) {
            return (total_written > 0) ? total_written : -ENOMEM;
        }
        if (wb->capacity > VFS_WRITE_BEHIND_MAX_CAPACITY || wb->pending_len > wb->capacity) {
            return (total_written > 0) ? total_written : -EIO;
        }

        if (wb->pending_len == wb->capacity) {
            int const FLUSH_STATUS = flush_write_behind(ctx);
            if (FLUSH_STATUS != 0) {
                return (total_written > 0) ? total_written : FLUSH_STATUS;
            }
            continue;
        }

        uint32_t const SPACE = wb->capacity - wb->pending_len;
        uint32_t const TO_BUFFER = std::min(SPACE, remaining);
        memcpy(wb->data.get() + wb->pending_len, src, TO_BUFFER);
        if (wb->pending_len == 0) {
            wb->pending_offset = cur_offset;
        }
        wb->pending_len += TO_BUFFER;

        src += TO_BUFFER;
        cur_offset += TO_BUFFER;
        remaining -= TO_BUFFER;
        total_written += TO_BUFFER;

        if (wb->pending_len >= wb->capacity) {
            int const FLUSH_STATUS = flush_write_behind(ctx);
            if (FLUSH_STATUS != 0) {
                // The bytes are already owned by write-behind, including any
                // tail retained after a partial flush. Report them as accepted
                // so a retry cannot overlap data that remains queued.
                return total_written;
            }
        }
    }

    return total_written;
}

auto remote_vfs_lseek(ker::vfs::File* f, off_t offset, int whence) -> off_t {
    if (f == nullptr) {
        return -EBADF;
    }

    switch (whence) {
        case 0:  // SEEK_SET
            f->pos = offset;
            break;
        case 1:  // SEEK_CUR
            f->pos += offset;
            break;
        case 2: {  // SEEK_END - V2: server roundtrip to get file size
            if (f->private_data == nullptr) {
                ker::mod::dbg::log("remote_vfs_lseek: SEEK_END private_data is null");
                return -EBADF;
            }
            auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
            if (ctx->proxy == nullptr || !ctx->proxy->active) {
                ker::mod::dbg::log("remote_vfs_lseek: SEEK_END proxy null/inactive (proxy=%p active=%d)", ctx->proxy,
                                   (ctx->proxy != nullptr) ? static_cast<int>(ctx->proxy->active) : -1);
                return -EIO;
            }
            ker::mod::sys::MutexGuard io_guard(ctx->io_lock);
            consume_remote_file_cache_invalidation(ctx);

            // Flush pending writes before seek
            int const FLUSH_STATUS = flush_write_behind(ctx);
            if (FLUSH_STATUS != 0) {
                return FLUSH_STATUS;
            }

            ker::vfs::Stat cached_stat{};
            if ((f->open_flags & ker::vfs::O_NO_CACHE) == 0 && remote_vfs_try_copy_cached_stat(ctx, &cached_stat)) {
                auto const BASE = static_cast<int64_t>(cached_stat.st_size);
                auto const OFF = static_cast<int64_t>(offset);
                if ((OFF > 0 && BASE > INT64_MAX - OFF) || (OFF < 0 && BASE < INT64_MIN - OFF)) {
                    return -EINVAL;
                }
                int64_t const NEW_POS = BASE + OFF;
                if (NEW_POS < 0) {
                    return -EINVAL;
                }
                f->pos = static_cast<off_t>(NEW_POS);
                break;
            }

            // Request: {remote_fd:i32, offset:i64} = 12 bytes
            std::array<uint8_t, 12> req{};
            memcpy(req.data(), &ctx->remote_fd, sizeof(int32_t));
            auto off64 = static_cast<int64_t>(offset);
            memcpy(req.data() + 4, &off64, sizeof(int64_t));
#ifdef WKI_DEBUG
            ker::mod::dbg::log("remote_vfs_lseek: SEEK_END remote_fd=%d node=0x%04x ch=%u", ctx->remote_fd, ctx->proxy->owner_node,
                               ctx->proxy->assigned_channel);
#endif
            // Response: {pos:i64} = 8 bytes
            int64_t new_pos = -1;
            int const RET = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_SEEK_END, req.data(), 12, &new_pos, sizeof(new_pos));
#ifdef WKI_DEBUG
            ker::mod::dbg::log("remote_vfs_lseek: SEEK_END ret=%d new_pos=%ld", ret, (long)new_pos);
#endif
            if (RET < 0) {
                ker::mod::dbg::log("remote_vfs_lseek: SEEK_END request failed fd=%d node=0x%04x ch=%u ret=%d", ctx->remote_fd,
                                   ctx->proxy->owner_node, ctx->proxy->assigned_channel, RET);
                return RET;
            }

            f->pos = new_pos;
            break;
        }
        default:
            return -EINVAL;
    }

    return f->pos;
}

auto remote_vfs_isatty(ker::vfs::File* /*f*/) -> bool { return false; }

auto remote_vfs_readdir(ker::vfs::File* f, ker::vfs::DirEntry* entry, size_t index) -> int {
    if (f == nullptr || f->private_data == nullptr || entry == nullptr) {
        return -EINVAL;
    }
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    if (ctx->proxy == nullptr || !ctx->proxy->active) {
        return -EIO;
    }

    // D7: Check directory cache first (under lock)
    s_vfs_lock.lock();
    DirCacheEntry* cache = find_dir_cache(ctx->proxy, ctx->remote_fd);

    // Invalidate stale cache
    if (cache != nullptr && (wki_now_us() - cache->cache_time_us) > DirCacheEntry::STALE_US) {
        invalidate_dir_cache(ctx->proxy, ctx->remote_fd);
        cache = nullptr;
    }

    // Cache hit: entry already fetched
    if (cache != nullptr && try_get_visible_cached_dirent(cache, ctx, index, entry)) {
        s_vfs_lock.unlock();
        return 0;
    }

    // Cache hit but we already know directory is exhausted
    if (cache != nullptr && cache->complete) {
        s_vfs_lock.unlock();
        return -1;
    }

    // Create cache on first access
    if (cache == nullptr) {
        g_dir_cache.push_back({});
        cache = &g_dir_cache.back();
        cache->proxy = ctx->proxy;
        cache->remote_fd = ctx->remote_fd;
        cache->cache_time_us = wki_now_us();
        cache->complete = false;
    }
    s_vfs_lock.unlock();

    // Fetch entries from server in batches.
    // One OP_VFS_READDIR_BATCH request returns up to VFS_READDIR_BATCH_MAX_ENTRIES entries,
    // filling the full jumbo frame - typically the entire directory in one round-trip.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init): RX fills every byte read below before publishing the response length.
    std::array<uint8_t, VFS_READDIR_BATCH_DATA_CAPACITY> batch_buf __attribute__((uninitialized));

    while (true) {
        s_vfs_lock.lock();
        cache = find_dir_cache(ctx->proxy, ctx->remote_fd);
        if (cache == nullptr) {
            s_vfs_lock.unlock();
            return -EIO;
        }
        if (try_get_visible_cached_dirent(cache, ctx, index, entry)) {
            s_vfs_lock.unlock();
            return 0;
        }
        if (cache->complete) {
            s_vfs_lock.unlock();
            return -1;
        }
        auto fetch_idx = static_cast<uint32_t>(cache->entries.size());
        s_vfs_lock.unlock();

        // Request: {fd:i32, start_idx:u32, max_count:u32} = 12 bytes
        std::array<uint8_t, 12> req_data{};
        memcpy(req_data.data(), &ctx->remote_fd, sizeof(int32_t));
        memcpy(req_data.data() + 4, &fetch_idx, sizeof(uint32_t));
        memcpy(req_data.data() + 8, &VFS_READDIR_BATCH_MAX_ENTRIES, sizeof(uint32_t));

        uint16_t resp_len = 0;
        int const STATUS = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_READDIR_BATCH, req_data.data(), 12, batch_buf.data(),
                                                   static_cast<uint16_t>(batch_buf.size()), &resp_len);
        if (STATUS != 0 || resp_len < sizeof(uint32_t)) {
            s_vfs_lock.lock();
            cache = find_dir_cache(ctx->proxy, ctx->remote_fd);
            if (cache != nullptr) {
                cache->complete = true;
            }
            s_vfs_lock.unlock();
            return (STATUS != 0) ? STATUS : -EIO;
        }

        uint32_t count = 0;
        memcpy(&count, batch_buf.data(), sizeof(uint32_t));
        if (!vfs_readdir_batch_payload_is_valid(resp_len, count)) {
            return -EIO;
        }

        s_vfs_lock.lock();
        cache = find_dir_cache(ctx->proxy, ctx->remote_fd);
        if (cache != nullptr) {
            for (uint32_t i = 0; i < count; i++) {
                ker::vfs::DirEntry fetched = {};
                memcpy(&fetched, batch_buf.data() + sizeof(uint32_t) + (i * sizeof(ker::vfs::DirEntry)), sizeof(ker::vfs::DirEntry));
                cache->entries.push_back(fetched);
            }
            if (count < VFS_READDIR_BATCH_MAX_ENTRIES) {
                cache->complete = true;  // Server returned fewer than asked -> end of directory
            }
        }
        s_vfs_lock.unlock();
    }
}

// D8: readlink via remote VFS proxy
auto remote_vfs_readlink(ker::vfs::File* f, char* buf, size_t bufsize) -> ssize_t {  // NOLINT(readability-non-const-parameter)
    if (f == nullptr || f->private_data == nullptr || buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    if (ctx->proxy == nullptr || !ctx->proxy->active) {
        return -EIO;
    }

    // We don't have a path available from the File. readlink on an already-opened
    // remote file is not meaningful - readlink operates on paths, not FDs.
    // Return -1 as unsupported on open files.
    (void)bufsize;
    return -ENOSYS;
}

// V2: Remote truncate
auto remote_vfs_truncate(ker::vfs::File* f, off_t length) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return -EBADF;
    }
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    if (ctx->proxy == nullptr || !ctx->proxy->active) {
        return -EIO;
    }
    ker::mod::sys::MutexGuard io_guard(ctx->io_lock);
    consume_remote_file_cache_invalidation(ctx);

    remote_vfs_invalidate_cached_stat(ctx);

    // Flush pending writes before truncate
    int const FLUSH_STATUS = flush_write_behind(ctx);
    if (FLUSH_STATUS != 0) {
        return FLUSH_STATUS;
    }

    // Invalidate read-ahead cache (file size changes)
    if (ctx->read_cache != nullptr) {
        ctx->read_cache->cached_offset = -1;
        ctx->read_cache->cached_len = 0;
    }

    // Request: {remote_fd:i32, length:i64} = 12 bytes
    std::array<uint8_t, 12> req{};
    memcpy(req.data(), &ctx->remote_fd, sizeof(int32_t));
    auto len64 = static_cast<int64_t>(length);
    memcpy(req.data() + 4, &len64, sizeof(int64_t));

    int const RET = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_TRUNCATE, req.data(), 12, nullptr, 0);
    return RET;
}

auto remote_vfs_fsync_file(ker::vfs::File* f) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return -EBADF;
    }
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    if (ctx->proxy == nullptr || !ctx->proxy->active) {
        return -EIO;
    }
    ker::mod::sys::MutexGuard io_guard(ctx->io_lock);
    consume_remote_file_cache_invalidation(ctx);

    int const FLUSH_STATUS = flush_write_behind(ctx);
    if (FLUSH_STATUS != 0) {
        return FLUSH_STATUS;
    }

    int32_t remote_fd = ctx->remote_fd;
    return vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_FSYNC, reinterpret_cast<const uint8_t*>(&remote_fd), sizeof(remote_fd), nullptr, 0);
}

// Static FileOperations for remote VFS files
ker::vfs::FileOperations g_remote_vfs_fops = {
    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
    .vfs_open = nullptr,
    .vfs_close = remote_vfs_close,
    .vfs_read = remote_vfs_read,
    .vfs_write = remote_vfs_write,
    .vfs_lseek = remote_vfs_lseek,
    .vfs_isatty = remote_vfs_isatty,
    .vfs_readdir = remote_vfs_readdir,
    .vfs_readlink = remote_vfs_readlink,
    .vfs_truncate = remote_vfs_truncate,
    .vfs_poll_check = nullptr,
    .vfs_poll_register_waiter = nullptr,
    .vfs_ioctl = nullptr,
};

}  // namespace

void wki_remote_vfs_cleanup_for_task(uint64_t pid) {
    while (true) {
        ProxyTaskCleanup const CLEANUP = remove_one_proxy_task_reference(pid);
        if (!CLEANUP.removed) {
            return;
        }
        if (CLEANUP.op_wait_retiring) {
            wait_for_waiter_retirement(CLEANUP.op_wait_entry);
        } else {
            finish_or_quiesce_waiter(CLEANUP.op_wait_entry, CLEANUP.op_wait_claimed, WKI_ERR_PEER_FENCED);
        }
        wake_proxy_slot_waiter(CLEANUP.next_pid);
    }
}

auto wki_remote_vfs_fsync(ker::vfs::File* file) -> int { return remote_vfs_fsync_file(file); }

#ifdef WOS_SELFTEST
auto wki_remote_vfs_selftest_utimens_wire_path_validation() -> bool {
    constexpr std::array<uint8_t, 8> SAFE_PATH = {'d', 'i', 'r', '/', '.', '.', 'x', 'x'};
    constexpr std::array<uint8_t, 5> ABSOLUTE_PATH = {'/', 'f', 'i', 'l', 'e'};
    constexpr std::array<uint8_t, 8> DOT_PATH = {'d', 'i', 'r', '/', '.', '/', 'f', 'x'};
    constexpr std::array<uint8_t, 11> DOT_DOT_PATH = {'d', 'i', 'r', '/', '.', '.', '/', 'f', 'i', 'l', 'e'};
    constexpr std::array<uint8_t, 8> NUL_PATH = {'d', 'i', 'r', '/', '\0', 'f', 'i', 'l'};

    return relative_wire_path_has_safe_components(nullptr, 0) &&
           relative_wire_path_has_safe_components(SAFE_PATH.data(), SAFE_PATH.size()) &&
           !relative_wire_path_has_safe_components(ABSOLUTE_PATH.data(), ABSOLUTE_PATH.size()) &&
           !relative_wire_path_has_safe_components(DOT_PATH.data(), DOT_PATH.size()) &&
           !relative_wire_path_has_safe_components(DOT_DOT_PATH.data(), DOT_DOT_PATH.size()) &&
           !relative_wire_path_has_safe_components(NUL_PATH.data(), NUL_PATH.size());
}

auto wki_remote_vfs_selftest_multi_rdma_lane_selection() -> bool {
    WkiTransport transport{};
    transport.name = "wki-roce";

    ProxyVfsState anchor{};
    ProxyVfsState auxiliary{};
    anchor.active = true;
    anchor.lane_anchor = true;
    anchor.lanes_ready = true;
    anchor.mount_group_id = 1;
    anchor.lane_index = 0;
    anchor.lanes.at(0) = &anchor;
    anchor.lanes.at(1) = &auxiliary;
    anchor.lane_count = 2;
    auxiliary.active = true;
    auxiliary.mount_group_id = 1;
    auxiliary.lane_index = 1;

    anchor.rdma_transport = &transport;
    anchor.rdma_capable = true;
    anchor.rdma_server_write_rkey = 1;
    auxiliary.rdma_transport = &transport;
    auxiliary.rdma_capable = true;
    auxiliary.rdma_server_write_rkey = 0;

    // O_RDWR cannot select a read-only partial registration.
    ProxyVfsState* selected = acquire_vfs_proxy_lane(&anchor, true, true);
    if (selected != &anchor || anchor.lifecycle_refs != 1 || auxiliary.lifecycle_refs != 0) {
        return false;
    }

    // Make only the auxiliary lane fully capable: selection must no longer
    // collapse onto the logical mount anchor.
    anchor.rdma_server_write_rkey = 0;
    auxiliary.rdma_server_write_rkey = 2;
    selected = acquire_vfs_proxy_lane(&anchor, true, true);
    if (selected != &auxiliary || auxiliary.lifecycle_refs != 1) {
        return false;
    }

    // A lane in transient read-side cooldown is excluded while another
    // capable lane is available.
    auxiliary.rdma_read_retry_after_us.store(UINT64_MAX, std::memory_order_release);
    selected = acquire_vfs_proxy_lane(&anchor, true, false);
    if (selected != &anchor || anchor.lifecycle_refs != 2) {
        return false;
    }

    // Once only the auxiliary cooldown has elapsed, it becomes eligible
    // again. Complete RDMA cooldown then falls back to a live message lane.
    auxiliary.rdma_read_retry_after_us.store(0, std::memory_order_release);
    anchor.rdma_read_retry_after_us.store(UINT64_MAX, std::memory_order_release);
    selected = acquire_vfs_proxy_lane(&anchor, true, false);
    if (selected != &auxiliary || auxiliary.lifecycle_refs != 2) {
        return false;
    }
    auxiliary.rdma_read_retry_after_us.store(UINT64_MAX, std::memory_order_release);
    selected = acquire_vfs_proxy_lane(&anchor, true, false);
    if (selected == nullptr || (selected != &anchor && selected != &auxiliary)) {
        return false;
    }

    ProxyVfsState* const METADATA_LANE = acquire_vfs_proxy_lane(&anchor);
    return METADATA_LANE != nullptr && (METADATA_LANE == &anchor || METADATA_LANE == &auxiliary) &&
           anchor.lifecycle_refs + auxiliary.lifecycle_refs == 6;
}

auto wki_remote_vfs_selftest_slot_waiter_fifo() -> bool {
    ProxyVfsState state{};
    state.active = true;

    if (!enqueue_proxy_slot_waiter_locked(&state, 11) || !enqueue_proxy_slot_waiter_locked(&state, 22) ||
        !enqueue_proxy_slot_waiter_locked(&state, 33) || !enqueue_proxy_slot_waiter_locked(&state, 22) || state.op_slot_waiter_count != 3 ||
        proxy_slot_waiter_head_locked(&state) != 11) {
        return false;
    }

    ProxySlotWaiterRemoval const MIDDLE = remove_proxy_slot_waiter_locked(&state, 22);
    if (!MIDDLE.removed || MIDDLE.was_head || state.op_slot_waiter_count != 2 || state.op_slot_waiter_pids.at(1) != 33) {
        return false;
    }

    ProxySlotWaiterRemoval const HEAD = remove_proxy_slot_waiter_locked(&state, 11);
    return HEAD.removed && HEAD.was_head && proxy_slot_handoff_candidate_locked(&state) == 33;
}

auto wki_remote_vfs_selftest_stale_cancel_preserves_successor() -> bool {
    ProxyVfsState state{};
    WkiWaitEntry stale{};
    WkiWaitEntry successor{};
    state.active = true;
    state.op_generation = 18;
    state.op_wait_entry = &successor;
    state.op_waiter_pid = 22;
    state.op_expected_id = OP_VFS_STAT;
    state.op_expected_seq = 17;
    state.op_pending.store(true, std::memory_order_release);

    cancel_proxy_op_wait(&state, stale, 17, WKI_ERR_TIMEOUT);
    return state.op_generation == 18 && state.op_wait_entry == &successor && state.op_waiter_pid == 22 &&
           state.op_expected_id == OP_VFS_STAT && state.op_expected_seq == 17 && state.op_pending.load(std::memory_order_acquire);
}

auto wki_remote_vfs_selftest_response_claim_retains_waiter_slot() -> bool {
    WkiWaitEntry timed_out{};
    timed_out.state.store(WkiWaitEntry::DONE, std::memory_order_release);
    WkiWaitEntry* waiter_slot = &timed_out;
    if (claim_response_waiter_locked(waiter_slot) != nullptr || waiter_slot != &timed_out) {
        return false;
    }

    WkiWaitEntry pending{};
    waiter_slot = &pending;
    return claim_response_waiter_locked(waiter_slot) == &pending && waiter_slot == &pending &&
           pending.state.load(std::memory_order_acquire) == WkiWaitEntry::CLAIMED;
}

auto wki_remote_vfs_selftest_completed_response_cancel_releases_slot() -> bool {
    ProxyVfsState state{};
    WkiWaitEntry completed{};
    state.active = true;
    state.op_generation = 23;
    state.op_waiter_pid = 11;
    state.op_expected_id = OP_VFS_WRITE_RDMA;
    state.op_expected_seq = 23;
    state.op_pending.store(true, std::memory_order_release);
    completed.state.store(WkiWaitEntry::DONE, std::memory_order_release);

    cancel_proxy_op_wait(&state, completed, 23, -EIO);
    return !state.op_pending.load(std::memory_order_acquire) && state.op_waiter_pid == 0 && state.op_expected_id == 0 &&
           state.op_expected_seq == 0;
}

auto wki_remote_vfs_selftest_task_exit_releases_owned_slot() -> bool {
    ProxyVfsState state{};
    WkiWaitEntry exiting{};
    state.active = true;
    state.op_generation = 41;
    state.op_wait_entry = &exiting;
    state.op_waiter_pid = 11;
    state.op_expected_id = OP_VFS_STAT;
    state.op_expected_seq = 9;
    state.op_pending.store(true, std::memory_order_release);
    static_cast<void>(enqueue_proxy_slot_waiter_locked(&state, 22));

    ProxyTaskCleanup const CLEANUP = cleanup_proxy_task_reference_locked(&state, 11);
    finish_or_quiesce_waiter(CLEANUP.op_wait_entry, CLEANUP.op_wait_claimed, WKI_ERR_PEER_FENCED);
    return CLEANUP.removed && CLEANUP.next_pid == 22 && exiting.state.load(std::memory_order_acquire) == WkiWaitEntry::DONE &&
           !state.op_pending.load(std::memory_order_acquire) && state.op_wait_entry == nullptr && state.op_waiter_pid == 0 &&
           state.op_generation == 41;
}

auto wki_remote_vfs_selftest_task_exit_discovers_retiring_slot() -> bool {
    ProxyVfsState state{};
    WkiWaitEntry retiring{};
    state.op_retiring_wait_entry = &retiring;
    state.op_retiring_waiter_pid = 11;
    retiring.retirement_pending.store(true, std::memory_order_release);

    ProxyTaskCleanup const CLEANUP = cleanup_proxy_task_reference_locked(&state, 11);
    return CLEANUP.removed && CLEANUP.op_wait_retiring && CLEANUP.op_wait_entry == &retiring && !CLEANUP.op_wait_claimed &&
           state.op_retiring_wait_entry == &retiring && state.op_retiring_waiter_pid == 11 &&
           retiring.state.load(std::memory_order_acquire) == WkiWaitEntry::PENDING;
}

auto wki_remote_vfs_selftest_teardown_quiesces_retiring_slot() -> bool {
    ProxyVfsState state{};
    WkiWaitEntry retiring{};
    retiring.retirement_pending.store(true, std::memory_order_release);
    if (!wki_claim_op(&retiring)) {
        return false;
    }
    state.op_retiring_wait_entry = &retiring;
    state.op_retiring_waiter_pid = 11;
    PendingProxyTeardown teardown = {
        .state = &state,
        .op_wait_entry = &retiring,
        .op_wait_claimed = true,
    };

    finish_proxy_teardown_op_waiter(teardown, WKI_ERR_PEER_FENCED);
    return state.op_retiring_wait_entry == nullptr && state.op_retiring_waiter_pid == 0 &&
           !retiring.retirement_pending.load(std::memory_order_acquire) &&
           retiring.state.load(std::memory_order_acquire) == WkiWaitEntry::DONE;
}

auto wki_remote_vfs_selftest_inactive_slot_rejected() -> bool {
    ProxyVfsState state{};
    state.active = false;
    return acquire_proxy_op_slot_locked(&state, wki_now_us()) == WKI_ERR_PEER_FENCED;
}

auto wki_remote_vfs_selftest_readlink_cache_generation_invalidation() -> bool {
    constexpr char POSITIVE_PATH[] = "/positive-link";
    constexpr char POSITIVE_TARGET[] = "positive-target";
    constexpr char LATE_POSITIVE_PATH[] = "/late-positive-link";
    constexpr char LATE_POSITIVE_TARGET[] = "late-positive-target";
    constexpr char LATE_NEGATIVE_PATH[] = "/late-missing-link";
    constexpr char NEGATIVE_PATH[] = "/missing-link";
    constexpr char WRAP_PATH[] = "/wrap-link";
    constexpr char WRAP_TARGET[] = "wrap-target";

    ProxyVfsState state{};
    std::array<char, VFS_READLINK_CACHE_TEXT_MAX> result_buf{};
    auto lookup_positive = [&](const char* path, const char* target, size_t target_len) {
        ssize_t result = -1;
        uint32_t generation = 0;
        result_buf.fill('\0');
        return try_readlink_cache_lookup(&state, path, result_buf.data(), result_buf.size(), &result, &generation) &&
               result == static_cast<ssize_t>(target_len) && std::memcmp(result_buf.data(), target, target_len) == 0;
    };
    auto lookup_status = [&](const char* path, ssize_t expected) {
        ssize_t result = 0;
        uint32_t generation = 0;
        return try_readlink_cache_lookup(&state, path, result_buf.data(), result_buf.size(), &result, &generation) && result == expected;
    };
    auto cache_misses = [&](const char* path, uint32_t* generation_out) {
        ssize_t result = 0;
        uint32_t generation = 0;
        bool const MISSED = !try_readlink_cache_lookup(&state, path, result_buf.data(), result_buf.size(), &result, &generation);
        if (generation_out != nullptr) {
            *generation_out = generation;
        }
        return MISSED;
    };

    uint32_t positive_generation = 0;
    if (!cache_misses(POSITIVE_PATH, &positive_generation)) {
        return false;
    }
    cache_readlink_result(&state, positive_generation, POSITIVE_PATH, 0, POSITIVE_TARGET, sizeof(POSITIVE_TARGET) - 1);
    if (!lookup_positive(POSITIVE_PATH, POSITIVE_TARGET, sizeof(POSITIVE_TARGET) - 1)) {
        return false;
    }

    // Preserve a generation-one entry away from the normal first replacement
    // slot. A correct wrap invalidation must prevent this stale row reviving.
    state.lock.lock();
    state.readlink_cache.at(7) = state.readlink_cache.front();
    uint32_t const INITIAL_GENERATION = state.readlink_cache_generation;
    state.lock.unlock();

    invalidate_readlink_cache(&state);
    state.lock.lock();
    bool const GENERATION_BUMPED = state.readlink_cache_generation == INITIAL_GENERATION + 1;
    state.lock.unlock();
    if (!GENERATION_BUMPED || !cache_misses(POSITIVE_PATH, nullptr)) {
        return false;
    }

    uint32_t late_positive_generation = 0;
    if (!cache_misses(LATE_POSITIVE_PATH, &late_positive_generation)) {
        return false;
    }
    invalidate_readlink_cache(&state);
    cache_readlink_result(&state, late_positive_generation, LATE_POSITIVE_PATH, 0, LATE_POSITIVE_TARGET, sizeof(LATE_POSITIVE_TARGET) - 1);
    if (!cache_misses(LATE_POSITIVE_PATH, nullptr)) {
        return false;
    }

    uint32_t late_negative_generation = 0;
    if (!cache_misses(LATE_NEGATIVE_PATH, &late_negative_generation)) {
        return false;
    }
    invalidate_readlink_cache(&state);
    cache_readlink_result(&state, late_negative_generation, LATE_NEGATIVE_PATH, -ENOENT, nullptr, 0);
    if (!cache_misses(LATE_NEGATIVE_PATH, nullptr)) {
        return false;
    }

    uint32_t negative_generation = 0;
    if (!cache_misses(NEGATIVE_PATH, &negative_generation)) {
        return false;
    }
    cache_readlink_result(&state, negative_generation, NEGATIVE_PATH, -ENOENT, nullptr, 0);
    if (!lookup_status(NEGATIVE_PATH, -ENOENT)) {
        return false;
    }
    invalidate_readlink_cache(&state);
    if (!cache_misses(NEGATIVE_PATH, nullptr)) {
        return false;
    }

    state.lock.lock();
    state.readlink_cache_generation = UINT32_MAX;
    state.lock.unlock();
    uint32_t wrap_generation = 0;
    if (!cache_misses(WRAP_PATH, &wrap_generation) || wrap_generation != UINT32_MAX) {
        return false;
    }
    cache_readlink_result(&state, wrap_generation, WRAP_PATH, 0, WRAP_TARGET, sizeof(WRAP_TARGET) - 1);
    if (!lookup_positive(WRAP_PATH, WRAP_TARGET, sizeof(WRAP_TARGET) - 1)) {
        return false;
    }

    invalidate_readlink_cache(&state);
    state.lock.lock();
    bool wrap_cleared = state.readlink_cache_generation == 1;
    for (const auto& entry : state.readlink_cache) {
        wrap_cleared = wrap_cleared && entry.generation == 0;
    }
    state.lock.unlock();
    if (!wrap_cleared || !cache_misses(POSITIVE_PATH, nullptr) || !cache_misses(WRAP_PATH, nullptr)) {
        return false;
    }

    uint32_t post_wrap_generation = 0;
    if (!cache_misses(POSITIVE_PATH, &post_wrap_generation) || post_wrap_generation != 1) {
        return false;
    }
    cache_readlink_result(&state, post_wrap_generation, POSITIVE_PATH, 0, POSITIVE_TARGET, sizeof(POSITIVE_TARGET) - 1);
    return lookup_positive(POSITIVE_PATH, POSITIVE_TARGET, sizeof(POSITIVE_TARGET) - 1);
}

auto wki_remote_vfs_selftest_write_behind_capacity_classes() -> bool {
    struct CapacityCase {
        uint64_t required;
        uint32_t expected;
    };
    constexpr std::array<CapacityCase, 23> CASES = {
        CapacityCase{1, 28U * 1024U},
        CapacityCase{16U * 1024U, 28U * 1024U},
        CapacityCase{28U * 1024U, 28U * 1024U},
        CapacityCase{(28U * 1024U) + 1U, 60U * 1024U},
        CapacityCase{60U * 1024U, 60U * 1024U},
        CapacityCase{(60U * 1024U) + 1U, 124U * 1024U},
        CapacityCase{64U * 1024U, 124U * 1024U},
        CapacityCase{124U * 1024U, 124U * 1024U},
        CapacityCase{(124U * 1024U) + 1U, 252U * 1024U},
        CapacityCase{128U * 1024U, 252U * 1024U},
        CapacityCase{252U * 1024U, 252U * 1024U},
        CapacityCase{(252U * 1024U) + 1U, 508U * 1024U},
        CapacityCase{256U * 1024U, 508U * 1024U},
        CapacityCase{508U * 1024U, 508U * 1024U},
        CapacityCase{(508U * 1024U) + 1U, 1020U * 1024U},
        CapacityCase{1020U * 1024U, 1020U * 1024U},
        CapacityCase{(1020U * 1024U) + 1U, 2044U * 1024U},
        CapacityCase{1U * 1024U * 1024U, 2044U * 1024U},
        CapacityCase{2044U * 1024U, 2044U * 1024U},
        CapacityCase{(2044U * 1024U) + 1U, 4092U * 1024U},
        CapacityCase{4092U * 1024U, 4092U * 1024U},
        CapacityCase{(4092U * 1024U) + 1U, 4092U * 1024U},
        CapacityCase{UINT64_MAX, 4092U * 1024U},
    };

    for (const auto& test_case : CASES) {
        if (write_behind_capacity_for(test_case.required) != test_case.expected) {
            return false;
        }
    }
    return true;
}

auto wki_remote_vfs_selftest_write_behind_growth() -> bool {
    WriteBehindBuffer wb = {};
    if (!try_reserve_write_behind(&wb, 16U * 1024U) || wb.capacity != 28U * 1024U || wb.data == nullptr) {
        return false;
    }

    wb.pending_offset = 37;
    wb.pending_len = 256;
    for (uint32_t i = 0; i < wb.pending_len; ++i) {
        wb.data[i] = static_cast<uint8_t>(i ^ 0xA5U);
    }

    uint8_t* const INITIAL_STORAGE = wb.data.get();
    if (!try_reserve_write_behind(&wb, 1) || wb.data.get() != INITIAL_STORAGE || wb.capacity != 28U * 1024U) {
        return false;
    }
    if (!try_reserve_write_behind(&wb, (28U * 1024U) + 1U) || wb.data.get() == INITIAL_STORAGE || wb.capacity != 60U * 1024U ||
        wb.pending_offset != 37 || wb.pending_len != 256) {
        return false;
    }
    for (uint32_t i = 0; i < wb.pending_len; ++i) {
        if (wb.data[i] != static_cast<uint8_t>(i ^ 0xA5U)) {
            return false;
        }
    }

    uint8_t* const GROWN_STORAGE = wb.data.get();
    uint32_t const GROWN_CAPACITY = wb.capacity;
    std::unique_ptr<uint8_t[]> no_replacement{};
    if (install_write_behind_storage(&wb, std::move(no_replacement), 124U * 1024U) || wb.data.get() != GROWN_STORAGE ||
        wb.capacity != GROWN_CAPACITY || wb.pending_offset != 37 || wb.pending_len != 256) {
        return false;
    }
    return true;
}
#endif

// -------------------------------------------------------------------------------
// Init
// -------------------------------------------------------------------------------

void wki_remote_vfs_init() {
    if (g_remote_vfs_initialized) {
        return;
    }
    g_remote_vfs_initialized = true;
    ker::mod::dbg::log("[WKI] Remote VFS subsystem initialized");
}

// -------------------------------------------------------------------------------
// Server Side - VFS Export Management
// -------------------------------------------------------------------------------

namespace {

auto export_path_belongs_to_mount(const char* export_path, const char* mount_path) -> bool {
    if (export_path == nullptr || mount_path == nullptr || export_path[0] != '/') {
        return false;
    }
    size_t const MOUNT_LEN = std::strlen(mount_path);
    if (MOUNT_LEN == 0 || std::strncmp(export_path, mount_path, MOUNT_LEN) != 0) {
        return false;
    }
    return (MOUNT_LEN == 1 && mount_path[0] == '/') || export_path[MOUNT_LEN] == '\0' || export_path[MOUNT_LEN] == '/';
}

auto snapshot_export_backing_identity(const char* export_path) -> ExportBackingIdentity {
    ExportBackingIdentity backing = {};
    size_t best_path_len = 0;
    size_t const MOUNT_COUNT = ker::vfs::get_mount_count();
    for (size_t i = 0; i < MOUNT_COUNT; ++i) {
        ker::vfs::MountSnapshot snapshot = {};
        if (!ker::vfs::get_mount_snapshot_at(i, &snapshot) || snapshot.dev_id == 0) {
            continue;
        }
        const char* const MOUNT_PATH = static_cast<const char*>(snapshot.path);
        if (!export_path_belongs_to_mount(export_path, MOUNT_PATH)) {
            continue;
        }
        size_t const PATH_LEN = std::strlen(MOUNT_PATH);
        if (backing.dev_id == 0 || PATH_LEN > best_path_len) {
            backing = {.dev_id = snapshot.dev_id, .fs_type = snapshot.fs_type};
            best_path_len = PATH_LEN;
        }
    }
    return backing;
}

auto wki_remote_vfs_export_add_internal(const char* export_path, const char* name, PreservedExportIdentity preferred_identity,
                                        const ExportBackingIdentity& backing) -> uint32_t {
    if (export_path == nullptr || name == nullptr || backing.dev_id == 0) {
        return 0;
    }

    s_vfs_lock.lock();
    if (g_vfs_export_rebuild_prepared && !g_vfs_export_rebuild_accepting_entries) {
        s_vfs_lock.unlock();
        return 0;
    }

    // Check if this path is already exported (prevent duplicates)
    for (const auto& existing : g_vfs_exports) {
        if (existing.active && strcmp(raw_data(existing.export_path), export_path) == 0 &&
            export_backing_identity_matches(existing, backing)) {
            // Already exported - return existing resource_id
            uint32_t const EXISTING_ID = existing.resource_id;
            s_vfs_lock.unlock();
            return EXISTING_ID;
        }
    }

    bool const STANDALONE_MUTATION = !g_vfs_export_rebuild_prepared;
    if ((STANDALONE_MUTATION && ((g_vfs_export_revision & 1U) != 0 || g_vfs_export_revision > UINT64_MAX - 2)) ||
        (!STANDALONE_MUTATION && g_vfs_export_target_revision == 0)) {
        s_vfs_lock.unlock();
        return 0;
    }
    uint64_t const TARGET_REVISION = STANDALONE_MUTATION ? g_vfs_export_revision + 2 : g_vfs_export_target_revision;
    if (STANDALONE_MUTATION) {
        g_vfs_export_revision++;
    }

    VfsExport exp;
    if (preferred_identity.resource_id != 0) {
        exp.resource_id = preferred_identity.resource_id;
        if (g_next_vfs_resource_id <= preferred_identity.resource_id) {
            g_next_vfs_resource_id = preferred_identity.resource_id + 1;
        }
    } else {
        exp.resource_id = g_next_vfs_resource_id++;
    }
    if (preferred_identity.resource_incarnation != 0) {
        exp.resource_incarnation = preferred_identity.resource_incarnation;
    } else {
        exp.resource_incarnation = g_next_vfs_resource_incarnation++;
        if (exp.resource_incarnation == 0) {
            exp.resource_incarnation = g_next_vfs_resource_incarnation++;
        }
    }
    exp.publication_revision = TARGET_REVISION;
    exp.backing_dev_id = backing.dev_id;
    exp.backing_fs_type = backing.fs_type;

    size_t path_len = strlen(export_path);
    if (path_len >= VFS_EXPORT_PATH_LEN) {
        path_len = VFS_EXPORT_PATH_LEN - 1;
    }
    memcpy(static_cast<void*>(raw_data(exp.export_path)), export_path, path_len);
    exp.export_path[path_len] = '\0';

    size_t name_len = strlen(name);
    if (name_len >= VFS_EXPORT_NAME_LEN) {
        name_len = VFS_EXPORT_NAME_LEN - 1;
    }
    memcpy(static_cast<void*>(raw_data(exp.name)), name, name_len);
    exp.name[name_len] = '\0';

    exp.active = true;
    g_vfs_exports.push_back(exp);
    if (STANDALONE_MUTATION) {
        for (auto& published : g_vfs_exports) {
            published.publication_revision = TARGET_REVISION;
        }
        g_vfs_export_revision = TARGET_REVISION;
    }
    uint32_t const RESULT_ID = exp.resource_id;
    s_vfs_lock.unlock();

    ker::mod::dbg::log("[WKI] VFS export added: %s -> %s (resource_id=%u)", raw_data(exp.name), raw_data(exp.export_path), RESULT_ID);
    return RESULT_ID;
}

}  // namespace

auto wki_remote_vfs_export_add(const char* export_path, const char* name) -> uint32_t {
    ExportBackingIdentity const BACKING = snapshot_export_backing_identity(export_path);
    if (BACKING.dev_id == 0) {
        return 0;
    }
    return wki_remote_vfs_export_add_internal(export_path, name, {}, BACKING);
}

auto wki_remote_vfs_find_export_snapshot(uint32_t resource_id, VfsExport* out) -> bool {
    if (out == nullptr) {
        return false;
    }

    s_vfs_lock.lock();
    uint64_t const REVISION = g_vfs_export_revision;
    if ((REVISION & 1U) != 0) {
        s_vfs_lock.unlock();
        return false;
    }
    for (const auto& exp : g_vfs_exports) {
        if (exp.active && exp.resource_id == resource_id && exp.publication_revision == REVISION) {
            *out = exp;
            s_vfs_lock.unlock();
            return true;
        }
    }
    s_vfs_lock.unlock();
    return false;
}

auto wki_remote_vfs_export_snapshot_is_current(const VfsExport& expected) -> bool {
    if (!expected.active || expected.resource_id == 0 || expected.resource_incarnation == 0 || expected.publication_revision == 0 ||
        expected.backing_dev_id == 0) {
        return false;
    }

    s_vfs_lock.lock();
    uint64_t const REVISION = g_vfs_export_revision;
    bool current = (REVISION & 1U) == 0 && REVISION == expected.publication_revision;
    if (current) {
        current = false;
        for (const auto& exp : g_vfs_exports) {
            if (exp.active && exp.publication_revision == REVISION && exp.resource_id == expected.resource_id &&
                exp.resource_incarnation == expected.resource_incarnation && exp.backing_dev_id == expected.backing_dev_id &&
                exp.backing_fs_type == expected.backing_fs_type &&
                std::strncmp(raw_data(exp.export_path), raw_data(expected.export_path), VFS_EXPORT_PATH_LEN) == 0 &&
                std::strncmp(raw_data(exp.name), raw_data(expected.name), VFS_EXPORT_NAME_LEN) == 0) {
                current = true;
                break;
            }
        }
    }
    s_vfs_lock.unlock();
    return current;
}

namespace {

void advertise_exports_to_peer(uint16_t peer_node) {
    s_vfs_lock.lock();
    uint64_t const PUBLICATION_REVISION = g_vfs_export_revision;
    if ((PUBLICATION_REVISION & 1U) != 0) {
        s_vfs_lock.unlock();
        return;
    }
    size_t const EXPORT_COUNT = g_vfs_exports.size();
    s_vfs_lock.unlock();

    for (size_t idx = 0; idx < EXPORT_COUNT; idx++) {
        s_vfs_lock.lock();
        if (g_vfs_export_revision != PUBLICATION_REVISION || idx >= g_vfs_exports.size()) {
            s_vfs_lock.unlock();
            return;
        }
        VfsExport const EXP = *std::next(g_vfs_exports.begin(), static_cast<ptrdiff_t>(idx));
        s_vfs_lock.unlock();
        if (!EXP.active || EXP.publication_revision != PUBLICATION_REVISION) {
            continue;
        }

        auto const NAME_LEN = static_cast<uint8_t>(std::min<size_t>(std::strlen(raw_data(EXP.name)), 63U));

        bool const WITH_INCARNATION = wki_resource_incarnation_negotiated(peer_node, ResourceType::VFS);
        auto total_len =
            static_cast<uint16_t>(sizeof(ResourceAdvertPayload) + NAME_LEN + (WITH_INCARNATION ? sizeof(ResourceIncarnationToken) : 0));
        std::array<uint8_t, sizeof(ResourceAdvertPayload) + 64 + sizeof(ResourceIncarnationToken)> buf{};

        auto* adv = reinterpret_cast<ResourceAdvertPayload*>(buf.data());
        adv->node_id = g_wki.my_node_id;
        adv->resource_type = static_cast<uint16_t>(ResourceType::VFS);
        adv->resource_id = EXP.resource_id;
        adv->flags = 0;
        adv->name_len = NAME_LEN;
        memcpy(buf.data() + sizeof(ResourceAdvertPayload), static_cast<const void*>(raw_data(EXP.name)), NAME_LEN);
        if (WITH_INCARNATION) {
            ResourceIncarnationToken const TOKEN = {
                .owner_boot_epoch = g_wki.local_boot_epoch,
                .resource_incarnation = EXP.resource_incarnation,
            };
            memcpy(buf.data() + sizeof(ResourceAdvertPayload) + NAME_LEN, &TOKEN, sizeof(TOKEN));
        }

        wki_send(peer_node, WKI_CHAN_CONTROL, MsgType::RESOURCE_ADVERT, buf.data(), total_len);
    }
}

}  // namespace

void wki_remote_vfs_advertise_exports_to_peer(uint16_t peer_node) {
    if (!g_remote_vfs_initialized || peer_node == WKI_NODE_INVALID) {
        return;
    }

    WkiPeer const* peer = wki_peer_find(peer_node);
    if (peer == nullptr || peer->state != PeerState::CONNECTED) {
        return;
    }

    advertise_exports_to_peer(peer_node);
}

void wki_remote_vfs_advertise_exports() {
    if (!g_remote_vfs_initialized) {
        return;
    }

    for (size_t p = 0; p < WKI_MAX_PEERS; p++) {
        WkiPeer const* peer = &g_wki.peers[p];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        if (peer->node_id == WKI_NODE_INVALID || peer->state != PeerState::CONNECTED) {
            continue;
        }
        advertise_exports_to_peer(peer->node_id);
    }
}

// -------------------------------------------------------------------------------
// Server Side - VFS Operation Handlers
// -------------------------------------------------------------------------------

namespace detail {

// NOLINTNEXTLINE(readability-function-size): Protocol opcode dispatcher.
void handle_vfs_op(const WkiHeader* hdr, const WkiChannelIdentity& channel_identity, const char* export_path, const char* export_name,
                   uint16_t op_id, const uint8_t* data, uint16_t data_len) {
    if (!vfs_channel_identity_matches_header(hdr, channel_identity) || export_path == nullptr || export_name == nullptr) {
        return;
    }

    // This rejects work already associated with a retired local generation.
    // Legacy DEV_OP_REQ has no binding nonce, so a frame delayed until after
    // numeric channel re-reservation is still indistinguishable on the wire.
    uint16_t const channel_id = channel_identity.channel_id;
    const auto REQ_COOKIE = static_cast<uint16_t>(hdr->seq_num & UINT16_MAX);
    uint32_t const CORRELATION = REQ_COOKIE;
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint8_t const SERVER_OP = perf_vfs_server_op(op_id);
    perf_record_vfs_server_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::RX), hdr->src_node, channel_id, CORRELATION, 0,
                                 data_len, CALLSITE);

    auto send_simple_resp = [&](DevOpRespPayload& resp) {
        resp.reserved = REQ_COOKIE;
        perf_record_vfs_server_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::REPLY_SEND), hdr->src_node, channel_id,
                                     CORRELATION, resp.status, sizeof(resp), CALLSITE);
        static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
    };
    auto send_buffered_resp = [&](void* resp_buf, uint16_t resp_len) {
        if (resp_buf != nullptr && resp_len >= sizeof(DevOpRespPayload)) {
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
            resp->reserved = REQ_COOKIE;
            perf_record_vfs_server_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::REPLY_SEND), hdr->src_node, channel_id,
                                         CORRELATION, resp->status, resp_len, CALLSITE);
        }
        static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, resp_buf, resp_len));
    };

    switch (op_id) {
        case OP_VFS_OPEN: {
            // Request: {flags:u32, mode:u32, path_len:u16, path[path_len], optional prefetch_rkey:u32, prefetch_len:u32}
            if (data_len < OPEN_REQ_BASE_LEN) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_OPEN;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
                break;
            }

            uint32_t flags = 0;
            uint32_t mode = 0;
            uint16_t path_len = 0;
            memcpy(&flags, data, sizeof(uint32_t));
            memcpy(&mode, data + 4, sizeof(uint32_t));
            memcpy(&path_len, data + 8, sizeof(uint16_t));

            size_t const OPEN_PATH_END = OPEN_REQ_BASE_LEN + static_cast<size_t>(path_len);
            if (data_len < OPEN_PATH_END) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_OPEN;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
                break;
            }

            uint32_t prefetch_rkey = 0;
            uint32_t prefetch_len = 0;
            if (data_len >= OPEN_PATH_END + OPEN_PREFETCH_REQ_LEN) {
                memcpy(&prefetch_rkey, data + OPEN_PATH_END, sizeof(uint32_t));
                memcpy(&prefetch_len, data + OPEN_PATH_END + sizeof(uint32_t), sizeof(uint32_t));
                prefetch_len = std::min<uint32_t>(prefetch_len, VFS_RDMA_BULK_SIZE);
            }

            // Build full path: export_path + "/" + relative_path
            std::array<char, 512> full_path __attribute__((uninitialized));          // NOLINT(cppcoreguidelines-pro-type-member-init)
            std::array<char, 512> full_visible_path __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 10), path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(data + 10),
                            path_len);

            // Block recursive proxying: don't let a remote client traverse into our WKI mounts
            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_OPEN;
                resp.status = -EPERM;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
                break;
            }

            // Open using the export backing path, independent of whichever task
            // context happened to receive the packet.
            perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
            uint64_t const LOCAL_STARTED_US = wki_now_us();
            ker::vfs::File* file = ker::vfs::vfs_open_file_resolved(full_path.data(), static_cast<int>(flags), static_cast<int>(mode));
            if (file == nullptr) {
                perf_record_vfs_server_end(SERVER_OP, hdr->src_node, channel_id, CORRELATION, -1,
                                           static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US), 0, CALLSITE);
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_OPEN;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
                break;
            }

            OpenRespPayload open_resp = {};
            open_resp.is_dir = file->is_directory ? 1 : 0;

            ker::vfs::Stat open_stat = {};
            if (ker::vfs::vfs_fstat_file(file, &open_stat) == 0) {
                open_resp.has_stat = 1;
                open_resp.stat = open_stat;
            }

            if (prefetch_rkey != 0 && prefetch_len > 0 && open_resp.has_stat != 0 &&
                (open_stat.st_mode & ker::vfs::S_IFMT) == ker::vfs::S_IFREG && file->fops != nullptr && file->fops->vfs_read != nullptr) {
                WkiPeer const* prefetch_peer = wki_peer_find(hdr->src_node);
                if (prefetch_peer != nullptr && prefetch_peer->rdma_transport != nullptr &&
                    prefetch_peer->rdma_transport->rdma_write != nullptr && transport_is_roce(prefetch_peer->rdma_transport)) {
                    uint8_t* allocated_prefetch_buf = nullptr;
                    // VFS ops for one binding/channel are worker-serialized, so
                    // the per-binding bulk staging region is safe temporary
                    // scratch for open-prefetch before we send the response.
                    uint8_t* prefetch_buf = wki_dev_server_get_vfs_bulk_staging_buf(channel_identity);
                    if (prefetch_buf == nullptr) {
                        allocated_prefetch_buf = new (std::nothrow) uint8_t[prefetch_len];
                        prefetch_buf = allocated_prefetch_buf;
                    }
                    if (prefetch_buf != nullptr) {
                        ssize_t const BYTES_READ = read_local_file_windowed(file, prefetch_buf, prefetch_len, 0);
                        if (BYTES_READ > 0) {
                            int const WRITE_RET = wki_roce_rdma_write_tagged(hdr->src_node, prefetch_rkey, 0, prefetch_buf,
                                                                             static_cast<uint32_t>(BYTES_READ), REQ_COOKIE);
                            if (WRITE_RET == 0) {
                                open_resp.prefetched_bytes = static_cast<uint32_t>(BYTES_READ);
                            }
                        }
                    }
                    delete[] allocated_prefetch_buf;
                }
            }

#ifdef DEBUG_WKI_VFS
            std::array<uint8_t, 4> debug_probe = {0xEE, 0xEE, 0xEE, 0xEE};
            ssize_t debug_probe_n = 0;
            bool const DEBUG_PROBE_VALID = file->fops != nullptr && file->fops->vfs_read != nullptr && !file->is_directory;
            int const DEBUG_FS_TYPE = static_cast<int>(file->fs_type);
            if (DEBUG_PROBE_VALID) {
                debug_probe_n = file->fops->vfs_read(file, debug_probe.data(), debug_probe.size(), 0);
            }
#endif

            // Publish the opened file only after all metadata and prefetch work
            // is complete. Peer cleanup can detach and delete published files.
            s_vfs_lock.lock();
            int32_t fd_id = alloc_remote_fd(channel_identity, file);
            s_vfs_lock.unlock();
            open_resp.fd = fd_id;

            uint16_t const OPEN_DATA_LEN = (open_resp.has_stat == 0)          ? OPEN_RESP_NO_STAT_LEN
                                           : (open_resp.prefetched_bytes > 0) ? OPEN_RESP_WITH_PREFETCH_LEN
                                                                              : OPEN_RESP_WITH_STAT_LEN;
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(OpenRespPayload)> resp_buf{};  // NOLINT(modernize-avoid-c-arrays)
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_VFS_OPEN;
            resp->status = 0;
            resp->data_len = OPEN_DATA_LEN;
            resp->reserved = REQ_COOKIE;
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &open_resp, OPEN_DATA_LEN);
            perf_record_vfs_server_end(SERVER_OP, hdr->src_node, channel_id, CORRELATION, 0,
                                       static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US), OPEN_DATA_LEN, CALLSITE);
            perf_record_vfs_server_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::REPLY_SEND), hdr->src_node, channel_id,
                                         CORRELATION, 0, static_cast<uint16_t>(sizeof(DevOpRespPayload) + OPEN_DATA_LEN), CALLSITE);

            int const SEND_RET = wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, resp_buf.data(),
                                                              static_cast<uint16_t>(sizeof(DevOpRespPayload) + OPEN_DATA_LEN));
            if (SEND_RET != WKI_OK) {
                ker::vfs::File* orphan = nullptr;
                s_vfs_lock.lock();
                RemoteVfsFd* rfd = find_remote_fd(channel_identity, fd_id);
                if (rfd != nullptr && rfd->file == file) {
                    orphan = rfd->file;
                    rfd->file = nullptr;
                    rfd->retiring = true;
                    rfd->active = false;
                }
                std::erase_if(g_remote_fds, [&channel_identity, fd_id](const RemoteVfsFd& entry) {
                    return entry.fd_id == fd_id && entry.retiring && entry.file == nullptr &&
                           vfs_channel_identity_matches(entry.channel_identity, channel_identity);
                });
                s_vfs_lock.unlock();

                if (orphan != nullptr) {
                    static_cast<void>(ker::vfs::vfs_close_file(orphan));
                }
            }

#ifdef DEBUG_WKI_VFS
            if (DEBUG_PROBE_VALID) {
                ker::mod::dbg::log(
                    "[WKI-SRV] VFS_OPEN: node=0x%04x path='%s' fd=%d fs_type=%d"
                    " probe=[%02x %02x %02x %02x] probe_bytes=%ld",
                    hdr->src_node, full_path.data(), fd_id, DEBUG_FS_TYPE, debug_probe[0], debug_probe[1], debug_probe[2], debug_probe[3],
                    static_cast<long>(debug_probe_n));
            }
#endif
            break;
        }

        case OP_VFS_READ: {
            // Request: {remote_fd:i32, len:u32, offset:i64} = 16 bytes
            if (data_len < 16) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ;
                resp.status = -EINVAL;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
                break;
            }

            int32_t fd_id = 0;
            uint32_t len = 0;
            int64_t offset = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&len, data + 4, sizeof(uint32_t));
            memcpy(&offset, data + 8, sizeof(int64_t));

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(channel_identity, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_read == nullptr) {
                s_vfs_lock.unlock();
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ;
                resp.status = -EBADF;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            // Clamp to max response payload
            auto max_resp_data = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload));
            len = std::min<uint32_t>(len, max_resp_data);

            // A positive VFS read result owns an initialized prefix of exactly
            // that length. Transmit only that prefix and leave unused bounded
            // response capacity untouched.
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
            std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> resp_buf __attribute__((uninitialized));
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            uint8_t* read_buf = resp_buf.data() + sizeof(DevOpRespPayload);

            perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
            uint64_t const LOCAL_STARTED_US = wki_now_us();
            ssize_t const BYTES_READ = read_local_file_windowed(local_file, read_buf, len, static_cast<size_t>(offset));
            perf_record_vfs_server_end(
                SERVER_OP, hdr->src_node, channel_id, CORRELATION, (BYTES_READ >= 0) ? 0 : static_cast<int32_t>(BYTES_READ),
                static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US), (BYTES_READ > 0) ? static_cast<uint64_t>(BYTES_READ) : 0, CALLSITE);

#ifdef DEBUG_WKI_VFS
            // Diagnostic: log every read at offset 0 with full details
            if (offset == 0 && bytes_read > 3) {
                bool all_zero = (read_buf[0] == 0x00 && read_buf[1] == 0x00 && read_buf[2] == 0x00 && read_buf[3] == 0x00);
                ker::mod::dbg::log("[WKI-SRV] VFS_READ fd=%d bytes=%ld off=%ld fs_type=%d hdr=[%02x %02x %02x %02x]", fd_id,
                                   static_cast<long>(bytes_read), static_cast<long>(offset), static_cast<int>(local_file->fs_type),
                                   read_buf[0], read_buf[1], read_buf[2], read_buf[3]);

                // If allzero: re-read from the SAME file handle to check consistency
                if (all_zero) {
                    uint8_t vbuf[16] = {};
                    std::memset(vbuf, 0xCC, 16);
                    ssize_t vr = local_file->fops->vfs_read(local_file, vbuf, 16, 0);
                    ker::mod::dbg::log("[WKI-SRV] VERIFY re-read same fd: bytes=%ld data=[%02x %02x %02x %02x]", static_cast<long>(vr),
                                       vbuf[0], vbuf[1], vbuf[2], vbuf[3]);

                    // Also try opening and reading /etc/hostname to verify XFS works at all
                    auto* test_file = ker::vfs::vfs_open_file("/etc/hostname", 0, 0);
                    if (test_file != nullptr && test_file->fops != nullptr && test_file->fops->vfs_read != nullptr) {
                        uint8_t hbuf[16] = {};
                        ssize_t hr = test_file->fops->vfs_read(test_file, hbuf, 16, 0);
                        ker::mod::dbg::log("[WKI-SRV] VERIFY /etc/hostname: bytes=%ld fs_type=%d data=[%02x %02x %02x %02x]",
                                           static_cast<long>(hr), static_cast<int>(test_file->fs_type), hbuf[0], hbuf[1], hbuf[2], hbuf[3]);
                        if (test_file->fops->vfs_close != nullptr) {
                            test_file->fops->vfs_close(test_file);
                        }
                    }
                }
            }
#endif

            resp->op_id = OP_VFS_READ;
            if (BYTES_READ >= 0) {
                resp->status = 0;
                resp->data_len = static_cast<uint16_t>(BYTES_READ);
            } else {
                resp->status = static_cast<int16_t>(BYTES_READ);
                resp->data_len = 0;
            }
            resp->reserved = REQ_COOKIE;

            uint16_t const SEND_LEN = (BYTES_READ > 0) ? static_cast<uint16_t>(sizeof(DevOpRespPayload) + BYTES_READ)
                                                       : static_cast<uint16_t>(sizeof(DevOpRespPayload));
            perf_record_vfs_server_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::REPLY_SEND), hdr->src_node, channel_id,
                                         CORRELATION, resp->status, SEND_LEN, CALLSITE);
            static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, resp_buf.data(), SEND_LEN));
            break;
        }

        case OP_VFS_WRITE: {
            // Request: {remote_fd:i32, offset:i64, data[N]} = 12 + N bytes
            if (data_len < 12) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_WRITE;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int32_t fd_id = 0;
            int64_t offset = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&offset, data + 4, sizeof(int64_t));

            const uint8_t* write_data = data + 12;
            auto write_len = static_cast<uint16_t>(data_len - 12);

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(channel_identity, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_write == nullptr) {
                s_vfs_lock.unlock();
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_WRITE;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            ssize_t const BYTES_WRITTEN = local_file->fops->vfs_write(local_file, write_data, write_len, static_cast<size_t>(offset));
            if (BYTES_WRITTEN >= 0) {
                ker::vfs::vfs_cache_notify_file_data_changed(local_file);
            }

            // Response: {written:u32} = 4 bytes
            std::array<uint8_t, sizeof(DevOpRespPayload) + 4> resp_buf{};  // NOLINT(modernize-avoid-c-arrays)
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_VFS_WRITE;
            resp->status = (BYTES_WRITTEN >= 0) ? static_cast<int16_t>(0)
                                                : static_cast<int16_t>(std::max(BYTES_WRITTEN, static_cast<ssize_t>(SHRT_MIN)));
            resp->data_len = 4;
            auto written = static_cast<uint32_t>((BYTES_WRITTEN >= 0) ? BYTES_WRITTEN : 0);
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &written, sizeof(uint32_t));

            send_buffered_resp(resp_buf.data(), static_cast<uint16_t>(sizeof(DevOpRespPayload) + 4));
            break;
        }

        case OP_VFS_CLOSE: {
            // Legacy request: {remote_fd:i32}; extended request appends flags:u8.
            if (data_len < WKI_VFS_CLOSE_LEGACY_DATA_LEN) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_CLOSE;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int32_t fd_id = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            bool const NO_SUCCESS_RESPONSE = wki_vfs_close_no_success_response_requested(data, data_len);

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(channel_identity, fd_id);
            touch_remote_fd(rfd);
            ker::vfs::File* close_file = nullptr;
            perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
            uint64_t const LOCAL_STARTED_US = wki_now_us();
            int16_t status = -1;
            if (rfd != nullptr && rfd->file != nullptr) {
                close_file = rfd->file;
                rfd->file = nullptr;
                rfd->retiring = true;
                rfd->active = false;
                status = 0;
            }
            std::erase_if(g_remote_fds, [&channel_identity, fd_id](const RemoteVfsFd& entry) {
                return entry.fd_id == fd_id && entry.retiring && entry.file == nullptr &&
                       vfs_channel_identity_matches(entry.channel_identity, channel_identity);
            });
            s_vfs_lock.unlock();

            if (close_file != nullptr) {
                if (close_file->fops != nullptr && close_file->fops->vfs_close != nullptr) {
                    close_file->fops->vfs_close(close_file);
                }
                delete close_file;
            }
            perf_record_vfs_server_end(SERVER_OP, hdr->src_node, channel_id, CORRELATION, status,
                                       static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US), 0, CALLSITE);

            if (status != 0 || !NO_SUCCESS_RESPONSE) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_CLOSE;
                resp.status = status;
                resp.data_len = 0;
                send_simple_resp(resp);
            }
            break;
        }

        case OP_VFS_READDIR: {
            // Request: {remote_fd:i32, index:u32} = 8 bytes
            if (data_len < 8) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int32_t fd_id = 0;
            uint32_t index = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&index, data + 4, sizeof(uint32_t));

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(channel_identity, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_readdir == nullptr) {
                s_vfs_lock.unlock();
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            ker::vfs::DirEntry entry = {};
            perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
            uint64_t const LOCAL_STARTED_US = wki_now_us();
            int const RET = local_file->fops->vfs_readdir(local_file, &entry, index);
            perf_record_vfs_server_end(SERVER_OP, hdr->src_node, channel_id, CORRELATION, RET,
                                       static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US), (RET == 0) ? sizeof(ker::vfs::DirEntry) : 0,
                                       CALLSITE);

            if (RET != 0) {
                // End of directory or error
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR;
                resp.status = static_cast<int16_t>(RET);
                resp.data_len = 0;
                send_simple_resp(resp);
            } else {
                // Send entry
                auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + sizeof(ker::vfs::DirEntry));
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(ker::vfs::DirEntry)> resp_buf{};
                auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
                resp->op_id = OP_VFS_READDIR;
                resp->status = 0;
                resp->data_len = sizeof(ker::vfs::DirEntry);
                memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &entry, sizeof(ker::vfs::DirEntry));
                send_buffered_resp(resp_buf.data(), resp_total);
            }
            break;
        }

        case OP_VFS_READDIR_BATCH: {
            // Request: {fd:i32, start_idx:u32, max_count:u32} = 12 bytes
            // Response: {count:u32} + count×DirEntry - packs as many entries as
            // fit in one jumbo frame, typically the whole directory in one shot.
            if (data_len < 12) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR_BATCH;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int32_t fd_id = 0;
            uint32_t start_idx = 0;
            uint32_t max_count = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&start_idx, data + 4, sizeof(uint32_t));
            memcpy(&max_count, data + 8, sizeof(uint32_t));

            // Clamp to what fits in one frame
            if (max_count == 0 || max_count > VFS_READDIR_BATCH_MAX_ENTRIES) {
                max_count = VFS_READDIR_BATCH_MAX_ENTRIES;
            }

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(channel_identity, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_readdir == nullptr) {
                s_vfs_lock.unlock();
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR_BATCH;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init): only the exact initialized prefix is transmitted.
            std::array<uint8_t, VFS_READDIR_BATCH_RESPONSE_CAPACITY> resp_buf __attribute__((uninitialized));

            perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
            uint64_t const LOCAL_STARTED_US = wki_now_us();
            uint8_t* entries_base = resp_buf.data() + sizeof(DevOpRespPayload) + sizeof(uint32_t);
            uint32_t count = 0;
            for (uint32_t i = 0; i < max_count; i++) {
                ker::vfs::DirEntry entry = {};
                int const RET = local_file->fops->vfs_readdir(local_file, &entry, start_idx + i);
                if (RET != 0) {
                    break;
                }
                memcpy(entries_base + (i * sizeof(ker::vfs::DirEntry)), &entry, sizeof(ker::vfs::DirEntry));
                count++;
            }

            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_VFS_READDIR_BATCH;
            resp->status = 0;
            resp->data_len = static_cast<uint16_t>(sizeof(uint32_t) + (count * sizeof(ker::vfs::DirEntry)));
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &count, sizeof(uint32_t));
            perf_record_vfs_server_end(SERVER_OP, hdr->src_node, channel_id, CORRELATION, 0,
                                       static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US),
                                       sizeof(uint32_t) + (count * sizeof(ker::vfs::DirEntry)), CALLSITE);

            auto send_len = static_cast<uint16_t>(sizeof(DevOpRespPayload) + sizeof(uint32_t) + (count * sizeof(ker::vfs::DirEntry)));
            send_buffered_resp(resp_buf.data(), send_len);
            break;
        }

        case OP_VFS_STAT: {
            // Request: {path_len:u16, path[path_len]}
            if (data_len < 2) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_STAT;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            uint16_t path_len = 0;
            memcpy(&path_len, data, sizeof(uint16_t));

            if (data_len < 2 + path_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_STAT;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            std::array<char, 512> full_path __attribute__((uninitialized));          // NOLINT(cppcoreguidelines-pro-type-member-init)
            std::array<char, 512> full_visible_path __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 2), path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(data + 2),
                            path_len);

            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_STAT;
                resp.status = -EPERM;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            ker::vfs::Stat statbuf = {};
            perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
            uint64_t const LOCAL_STARTED_US = wki_now_us();
            int const RET = ker::vfs::vfs_stat_resolved(full_path.data(), &statbuf);
            perf_record_vfs_server_end(SERVER_OP, hdr->src_node, channel_id, CORRELATION, RET,
                                       static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US), (RET == 0) ? sizeof(ker::vfs::Stat) : 0,
                                       CALLSITE);

            if (RET != 0) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_STAT;
                resp.status = static_cast<int16_t>(RET);
                resp.data_len = 0;
                send_simple_resp(resp);
            } else {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init): Every transmitted byte is populated below.
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(ker::vfs::Stat)> resp_buf;
                auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
                resp->op_id = OP_VFS_STAT;
                resp->status = 0;
                resp->data_len = sizeof(ker::vfs::Stat);
                memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &statbuf, sizeof(ker::vfs::Stat));
                send_buffered_resp(resp_buf.data(), static_cast<uint16_t>(resp_buf.size()));
            }
            break;
        }

        case OP_VFS_MKDIR: {
            // Request: {mode:u32, path_len:u16, path[path_len]}
            if (data_len < 6) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_MKDIR;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            uint32_t mode = 0;
            uint16_t path_len = 0;
            memcpy(&mode, data, sizeof(uint32_t));
            memcpy(&path_len, data + 4, sizeof(uint16_t));

            if (data_len < 6 + path_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_MKDIR;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            std::array<char, 512> full_path __attribute__((uninitialized));          // NOLINT(cppcoreguidelines-pro-type-member-init)
            std::array<char, 512> full_visible_path __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 6), path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(data + 6),
                            path_len);

            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_MKDIR;
                resp.status = -EPERM;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int const RET = ker::vfs::vfs_mkdir(full_visible_path.data(), static_cast<int>(mode));

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_MKDIR;
            resp.status = static_cast<int16_t>(RET);
            resp.data_len = 0;
            send_simple_resp(resp);
            break;
        }

        case OP_VFS_READLINK: {
            // D8: Request: {path_len:u16, path[path_len]}
            if (data_len < 2) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READLINK;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
                break;
            }

            uint16_t path_len = 0;
            memcpy(&path_len, data, sizeof(uint16_t));

            if (data_len < 2 + path_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READLINK;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
                break;
            }

            std::array<char, 512> full_path __attribute__((uninitialized));          // NOLINT(cppcoreguidelines-pro-type-member-init)
            std::array<char, 512> full_visible_path __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 2), path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(data + 2),
                            path_len);

            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READLINK;
                resp.status = -EPERM;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
                break;
            }

            // Read the symlink target
            std::array<char, 512> target_buf{};
            perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
            uint64_t const LOCAL_STARTED_US = wki_now_us();
            ssize_t const TARGET_LEN = ker::vfs::vfs_readlink_resolved(full_path.data(), target_buf.data(), target_buf.size() - 1);
            perf_record_vfs_server_end(
                SERVER_OP, hdr->src_node, channel_id, CORRELATION, (TARGET_LEN >= 0) ? 0 : static_cast<int32_t>(TARGET_LEN),
                static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US), (TARGET_LEN > 0) ? static_cast<uint64_t>(TARGET_LEN) : 0, CALLSITE);

            if (TARGET_LEN < 0) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READLINK;
                resp.status = static_cast<int16_t>(TARGET_LEN);
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
            } else {
                // Response: {target_len:u16, target[]}
                auto resp_data_len = static_cast<uint16_t>(2 + TARGET_LEN);
                auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + resp_data_len);
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init): Only resp_total populated bytes are transmitted.
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(uint16_t) + 512> resp_buf;
                auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
                resp->op_id = OP_VFS_READLINK;
                resp->status = 0;
                resp->data_len = resp_data_len;
                resp->reserved = REQ_COOKIE;

                auto tlen = static_cast<uint16_t>(TARGET_LEN);
                memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &tlen, sizeof(uint16_t));
                memcpy(resp_buf.data() + sizeof(DevOpRespPayload) + 2, target_buf.data(), TARGET_LEN);

                perf_record_vfs_server_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::REPLY_SEND), hdr->src_node,
                                             channel_id, CORRELATION, 0, resp_total, CALLSITE);
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, resp_buf.data(), resp_total));
            }
            break;
        }

        case OP_VFS_SYMLINK: {
            // D8: Request: {target_len:u16, target[], link_len:u16, link[]}
            if (data_len < 4) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_SYMLINK;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            uint16_t target_len = 0;
            memcpy(&target_len, data, sizeof(uint16_t));

            if (data_len < 4 + target_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_SYMLINK;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            uint16_t link_len = 0;
            memcpy(&link_len, data + 2 + target_len, sizeof(uint16_t));

            if (data_len < 4 + target_len + link_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_SYMLINK;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            // Null-terminate target
            std::array<char, 512> target_str{};
            size_t const COPY_TLEN = std::min<size_t>(target_len, target_str.size() - 1);
            memcpy(target_str.data(), data + 2, COPY_TLEN);

            // Build full link path
            std::array<char, 512> full_link __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
            build_full_path(full_link.data(), full_link.size(), export_path, reinterpret_cast<const char*>(data + 4 + target_len),
                            link_len);

            if (path_crosses_recursive_wki_boundary(full_link.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_SYMLINK;
                resp.status = -EPERM;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int const RET = ker::vfs::vfs_symlink_resolved(target_str.data(), full_link.data());

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_SYMLINK;
            resp.status = static_cast<int16_t>(RET);
            resp.data_len = 0;
            send_simple_resp(resp);
            break;
        }

            // =====================================================================
            // V2: Missing VFS operations
            // =====================================================================

        case OP_VFS_UNLINK: {
            // Request: {path_len:u16, path[path_len]}
            if (data_len < 2) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_UNLINK;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            uint16_t path_len = 0;
            memcpy(&path_len, data, sizeof(uint16_t));
            if (data_len < 2 + path_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_UNLINK;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            std::array<char, 512> full_path __attribute__((uninitialized));          // NOLINT(cppcoreguidelines-pro-type-member-init)
            std::array<char, 512> full_visible_path __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 2), path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(data + 2),
                            path_len);

            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_UNLINK;
                resp.status = -EPERM;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int const RET = ker::vfs::vfs_unlink(full_visible_path.data());

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_UNLINK;
            resp.status = static_cast<int16_t>(RET);
            resp.data_len = 0;
            send_simple_resp(resp);
            break;
        }

        case OP_VFS_RMDIR: {
            // Request: {path_len:u16, path[path_len]}
            if (data_len < 2) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RMDIR;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            uint16_t path_len = 0;
            memcpy(&path_len, data, sizeof(uint16_t));
            if (data_len < 2 + path_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RMDIR;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            std::array<char, 512> full_path __attribute__((uninitialized));          // NOLINT(cppcoreguidelines-pro-type-member-init)
            std::array<char, 512> full_visible_path __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 2), path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(data + 2),
                            path_len);

            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RMDIR;
                resp.status = -EPERM;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int const RET = ker::vfs::vfs_rmdir(full_visible_path.data());

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_RMDIR;
            resp.status = static_cast<int16_t>(RET);
            resp.data_len = 0;
            send_simple_resp(resp);
            break;
        }

        case OP_VFS_RENAME: {
            // Request: {old_path_len:u16, old_path[], new_path_len:u16, new_path[]}
            if (data_len < 4) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RENAME;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            uint16_t old_len = 0;
            memcpy(&old_len, data, sizeof(uint16_t));
            if (data_len < 4 + old_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RENAME;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            uint16_t new_len = 0;
            memcpy(&new_len, data + 2 + old_len, sizeof(uint16_t));
            if (data_len < 4 + old_len + new_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RENAME;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            std::array<char, 512> old_full __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
            build_full_path(old_full.data(), old_full.size(), export_path, reinterpret_cast<const char*>(data + 2), old_len);

            std::array<char, 512> new_full __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
            build_full_path(new_full.data(), new_full.size(), export_path, reinterpret_cast<const char*>(data + 4 + old_len), new_len);

            if (path_crosses_recursive_wki_boundary(old_full.data()) || path_crosses_recursive_wki_boundary(new_full.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RENAME;
                resp.status = -EPERM;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int const RET = ker::vfs::vfs_rename_resolved(old_full.data(), new_full.data());

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_RENAME;
            resp.status = static_cast<int16_t>(RET);
            resp.data_len = 0;
            send_simple_resp(resp);
            break;
        }

        case OP_VFS_CHMOD: {
            // Request: {mode:u32, flags:u8, path_len:u16, path[path_len]}
            if (data_len < 7) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_CHMOD;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            uint32_t mode = 0;
            uint8_t flags = 0;
            uint16_t path_len = 0;
            memcpy(&mode, data, sizeof(uint32_t));
            memcpy(&flags, data + 4, sizeof(uint8_t));
            memcpy(&path_len, data + 5, sizeof(uint16_t));
            if ((flags & ~WKI_VFS_CHMOD_FLAG_FOLLOW_FINAL_SYMLINK) != 0 || data_len < 7 + path_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_CHMOD;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            std::array<char, 512> full_path __attribute__((uninitialized));          // NOLINT(cppcoreguidelines-pro-type-member-init)
            std::array<char, 512> full_visible_path __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 7), path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(data + 7),
                            path_len);

            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_CHMOD;
                resp.status = -EPERM;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
            uint64_t const LOCAL_STARTED_US = wki_now_us();
            int const RET = ker::vfs::vfs_chmod_resolved(full_path.data(), static_cast<int>(mode),
                                                         (flags & WKI_VFS_CHMOD_FLAG_FOLLOW_FINAL_SYMLINK) != 0);
            perf_record_vfs_server_end(SERVER_OP, hdr->src_node, channel_id, CORRELATION, RET,
                                       static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US), 0, CALLSITE);

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_CHMOD;
            resp.status = static_cast<int16_t>(RET);
            resp.data_len = 0;
            send_simple_resp(resp);
            break;
        }

        case OP_VFS_UTIMENS: {
            if (data_len < sizeof(VfsUtimensReqPrefix)) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_UTIMENS;
                resp.status = -EINVAL;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            VfsUtimensReqPrefix prefix{};
            memcpy(&prefix, data, sizeof(prefix));
            constexpr uint8_t KNOWN_FLAGS = WKI_VFS_UTIMENS_FLAG_FOLLOW_FINAL_SYMLINK | WKI_VFS_UTIMENS_FLAG_TIMES_PRESENT;
            size_t const EXPECTED_DATA_LEN = sizeof(prefix) + static_cast<size_t>(prefix.path_len);
            const uint8_t* const PATH_DATA = data + sizeof(prefix);
            auto full_path_fits = [&prefix](const char* base) {
                size_t const BASE_LEN = strlen(base);
                size_t const SEPARATOR_LEN = BASE_LEN > 0 && base[BASE_LEN - 1] != '/' && prefix.path_len > 0 ? 1U : 0U;
                return BASE_LEN + SEPARATOR_LEN + static_cast<size_t>(prefix.path_len) < 512U;
            };
            if ((prefix.flags & ~KNOWN_FLAGS) != 0 || prefix.reserved != 0 || EXPECTED_DATA_LEN != data_len ||
                !relative_wire_path_has_safe_components(PATH_DATA, prefix.path_len) || !full_path_fits(export_path) ||
                !full_path_fits(export_name)) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_UTIMENS;
                resp.status = -EINVAL;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            std::array<char, 512> full_path __attribute__((uninitialized));          // NOLINT(cppcoreguidelines-pro-type-member-init)
            std::array<char, 512> full_visible_path __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(PATH_DATA), prefix.path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(PATH_DATA),
                            prefix.path_len);

            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_UTIMENS;
                resp.status = -EPERM;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            std::array<ker::vfs::Timespec, 2> request_times = {
                ker::vfs::Timespec{.tv_sec = prefix.atime_sec, .tv_nsec = prefix.atime_nsec},
                ker::vfs::Timespec{.tv_sec = prefix.mtime_sec, .tv_nsec = prefix.mtime_nsec},
            };
            const ker::vfs::Timespec* const TIMES =
                (prefix.flags & WKI_VFS_UTIMENS_FLAG_TIMES_PRESENT) != 0 ? request_times.data() : nullptr;

            perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
            uint64_t const LOCAL_STARTED_US = wki_now_us();
            int const RET = ker::vfs::vfs_utimens_resolved_beneath(export_path, full_path.data(), TIMES,
                                                                   (prefix.flags & WKI_VFS_UTIMENS_FLAG_FOLLOW_FINAL_SYMLINK) != 0);
            perf_record_vfs_server_end(SERVER_OP, hdr->src_node, channel_id, CORRELATION, RET,
                                       static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US), 0, CALLSITE);

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_UTIMENS;
            resp.status = static_cast<int16_t>(RET);
            resp.data_len = 0;
            send_simple_resp(resp);
            break;
        }

        case OP_VFS_FSYNC: {
            // Request: {remote_fd:i32}
            if (data_len < 4) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_FSYNC;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int32_t fd_id = 0;
            memcpy(&fd_id, data, sizeof(int32_t));

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(channel_identity, fd_id);
            ker::vfs::File* local_file = nullptr;
            int ret = -1;
            if (rfd != nullptr && rfd->file != nullptr) {
                touch_remote_fd(rfd);
                local_file = rfd->file;
            }
            s_vfs_lock.unlock();

            if (local_file != nullptr) {
                ret = ker::vfs::vfs_fsync_file(local_file);
            }

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_FSYNC;
            resp.status = static_cast<int16_t>(ret);
            resp.data_len = 0;
            send_simple_resp(resp);
            break;
        }

        case OP_VFS_TRUNCATE: {
            // Request: {remote_fd:i32, length:i64} = 12 bytes
            if (data_len < 12) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_TRUNCATE;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int32_t fd_id = 0;
            int64_t length = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&length, data + 4, sizeof(int64_t));

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(channel_identity, fd_id);
            ker::vfs::File* local_file = nullptr;
            int ret = -1;
            if (rfd != nullptr && rfd->file != nullptr && rfd->file->fops != nullptr && rfd->file->fops->vfs_truncate != nullptr) {
                touch_remote_fd(rfd);
                local_file = rfd->file;
            }
            s_vfs_lock.unlock();

            if (local_file != nullptr) {
                ret = local_file->fops->vfs_truncate(local_file, static_cast<off_t>(length));
                if (ret == 0) {
                    ker::vfs::vfs_cache_notify_file_data_changed(local_file);
                }
            }

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_TRUNCATE;
            resp.status = static_cast<int16_t>(ret);
            resp.data_len = 0;
            send_simple_resp(resp);
            break;
        }

        case OP_VFS_SEEK_END: {
            // Request: {remote_fd:i32, offset:i64} = 12 bytes
            // Response: {status:i32, pos:i64} = 12 bytes
            if (data_len < 12) {
                ker::mod::dbg::log("[WKI-SRV] SEEK_END: data_len too short (%u < 12)", data_len);
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_SEEK_END;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
                break;
            }

            int32_t fd_id = 0;
            int64_t offset = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&offset, data + 4, sizeof(int64_t));

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(channel_identity, fd_id);
            int64_t new_pos = -1;
            int ret = -1;
            ker::vfs::File* local_file = nullptr;

            if (rfd == nullptr) {
                ker::mod::dbg::log("[WKI-SRV] SEEK_END: find_remote_fd FAILED node=0x%04x ch=%u fd=%d", hdr->src_node, channel_id, fd_id);
            } else if (rfd->file == nullptr) {
                ker::mod::dbg::log("[WKI-SRV] SEEK_END: rfd->file is NULL fd=%d", fd_id);
            } else if (rfd->file->fops == nullptr) {
                ker::mod::dbg::log("[WKI-SRV] SEEK_END: fops is NULL fd=%d", fd_id);
            } else if (rfd->file->fops->vfs_lseek == nullptr) {
                ker::mod::dbg::log("[WKI-SRV] SEEK_END: fops->vfs_lseek is NULL fd=%d fs_type=%d", fd_id,
                                   static_cast<int>(rfd->file->fs_type));
            } else {
                touch_remote_fd(rfd);
                local_file = rfd->file;
            }
            s_vfs_lock.unlock();

            if (local_file != nullptr) {
                off_t const OLD_POS = local_file->pos;
                off_t const POS = local_file->fops->vfs_lseek(local_file, static_cast<off_t>(offset), 2 /* SEEK_END */);
                local_file->pos = OLD_POS;
#ifdef DEBUG_WKI_VFS
                ker::mod::dbg::log("[WKI-SRV] SEEK_END: fd=%d underlying_lseek returned %ld", fd_id, (long)POS);
#endif
                if (POS >= 0) {
                    new_pos = POS;
                    ret = 0;
                }
            }

            // Response: {pos:i64} = 8 bytes
            std::array<uint8_t, sizeof(DevOpRespPayload) + 8> resp_buf{};
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_VFS_SEEK_END;
            resp->status = static_cast<int16_t>(ret);
            resp->data_len = 8;
            resp->reserved = REQ_COOKIE;
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &new_pos, sizeof(int64_t));
            static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, resp_buf.data(),
                                                           static_cast<uint16_t>(sizeof(DevOpRespPayload) + 8)));
            break;
        }

        case OP_VFS_READ_RDMA: {
            // Request: {fd:i32, len:u32, off:i64, consumer_rkey:u32} = 20 bytes
            // Two modes depending on transport:
            //   Push: rdma_write data to the consumer's bounce buf (consumer_rkey), then send a tiny response.
            //   Pull: stage data in the server's read buf; the consumer rdma_reads after the response.
            auto send_rdma_read_err = [&](int16_t status = -EIO) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ_RDMA;
                resp.status = status;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
            };

            if (data_len < 20) {
                send_rdma_read_err();
                break;
            }

            int32_t fd_id = 0;
            uint32_t len = 0;
            int64_t offset = 0;
            uint32_t consumer_rkey = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&len, data + 4, sizeof(uint32_t));
            memcpy(&offset, data + 8, sizeof(int64_t));
            memcpy(&consumer_rkey, data + 16, sizeof(uint32_t));

            len = std::min<uint32_t>(len, VFS_RDMA_BOUNCE_SIZE);

            // Determine mode from the request rkey. rkey=0 keeps the legacy
            // pull path where the server stages data and the client rdma_reads
            // it; nonzero asks the server to push into the consumer region.
            bool const PULL_MODE = consumer_rkey == 0;
            uint8_t* read_staging = wki_dev_server_get_vfs_read_staging_buf(channel_identity);
            WkiPeer const* rdma_peer = nullptr;
            if (PULL_MODE && read_staging == nullptr) {
                send_rdma_read_err(-EIO);
                break;
            }
            if (!PULL_MODE) {
                // Push mode: need rdma_write capability.
                rdma_peer = wki_peer_find(hdr->src_node);
                if (rdma_peer == nullptr || rdma_peer->rdma_transport == nullptr || rdma_peer->rdma_transport->rdma_write == nullptr) {
                    send_rdma_read_err(-EIO);
                    break;
                }
            }

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(channel_identity, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_read == nullptr) {
                s_vfs_lock.unlock();
                send_rdma_read_err(-EBADF);
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            uint8_t* allocated_read_buf = nullptr;
            uint8_t* read_buf = read_staging;
            if (read_buf == nullptr) {
                allocated_read_buf = new (std::nothrow) uint8_t[len];
                if (allocated_read_buf == nullptr) {
                    send_rdma_read_err(-ENOMEM);
                    break;
                }
                read_buf = allocated_read_buf;
            }

            ssize_t bytes_read = read_local_file_windowed(local_file, read_buf, len, static_cast<size_t>(offset));
            if (!PULL_MODE && bytes_read > 0) {
                int write_ret = 0;
                if (transport_is_roce(rdma_peer->rdma_transport)) {
                    write_ret = wki_roce_rdma_write_tagged(hdr->src_node, consumer_rkey, 0, read_buf, static_cast<uint32_t>(bytes_read),
                                                           REQ_COOKIE);
                } else {
                    write_ret = rdma_peer->rdma_transport->rdma_write(rdma_peer->rdma_transport, hdr->src_node, consumer_rkey, 0, read_buf,
                                                                      static_cast<uint32_t>(bytes_read));
                }
                if (write_ret != 0) {
                    bytes_read = -EIO;
                }
            }
            delete[] allocated_read_buf;

            // Response: {bytes_read:u32} = 4 bytes (data is now in consumer bounce buf)
            uint32_t br = (bytes_read > 0) ? static_cast<uint32_t>(bytes_read) : 0;
            std::array<uint8_t, sizeof(DevOpRespPayload) + 4> resp_buf{};
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_VFS_READ_RDMA;
            resp->status = (bytes_read >= 0) ? 0 : static_cast<int16_t>(bytes_read);
            resp->data_len = 4;
            resp->reserved = REQ_COOKIE;
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &br, sizeof(uint32_t));
            static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, resp_buf.data(),
                                                           static_cast<uint16_t>(resp_buf.size())));
            break;
        }

        case OP_VFS_READ_BULK: {
            // Bulk RDMA read - identical request layout to OP_VFS_READ_RDMA but len
            // is not capped to 64 KB.
            // Two modes:
            //   Push: rdma_write into the consumer's bulk buf (consumer_rkey).
            //   Pull: stage data in the server's bulk buf; the consumer rdma_reads after the response.
            auto send_bulk_read_err = [&](int16_t status = -EIO) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ_BULK;
                resp.status = status;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
            };

            if (data_len < 20) {
                send_bulk_read_err();
                break;
            }

            int32_t fd_id = 0;
            uint32_t len = 0;
            int64_t offset = 0;
            uint32_t consumer_rkey = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&len, data + 4, sizeof(uint32_t));
            memcpy(&offset, data + 8, sizeof(int64_t));
            memcpy(&consumer_rkey, data + 16, sizeof(uint32_t));

            // Cap to the consumer's registered bulk size. A server staging
            // window can impose a smaller cap in either transfer mode.
            len = std::min<uint32_t>(len, VFS_RDMA_BULK_SIZE);

            // Determine mode from the request rkey. rkey=0 keeps the legacy
            // pull path where the server stages data and the client rdma_reads
            // it; nonzero asks the server to push into the consumer region.
            bool const PULL_MODE = consumer_rkey == 0;
            uint8_t* bulk_staging = wki_dev_server_get_vfs_bulk_staging_buf(channel_identity);
            WkiPeer const* bulk_peer = nullptr;
            if (bulk_staging != nullptr) {
                len = std::min<uint32_t>(len, VFS_RDMA_ROCE_BULK_SIZE);
            }
            if (PULL_MODE && bulk_staging == nullptr) {
                send_bulk_read_err(-EIO);
                break;
            }
            if (!PULL_MODE) {
                bulk_peer = wki_peer_find(hdr->src_node);
                if (bulk_peer == nullptr || bulk_peer->rdma_transport == nullptr || bulk_peer->rdma_transport->rdma_write == nullptr) {
                    send_bulk_read_err(-EIO);
                    break;
                }
            }

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(channel_identity, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_read == nullptr) {
                s_vfs_lock.unlock();
                send_bulk_read_err(-EBADF);
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            uint8_t* allocated_read_buf = nullptr;
            uint8_t* read_buf = bulk_staging;
            if (read_buf == nullptr) {
                allocated_read_buf = new (std::nothrow) uint8_t[len];
                if (allocated_read_buf == nullptr) {
                    send_bulk_read_err(-ENOMEM);
                    break;
                }
                read_buf = allocated_read_buf;
            }

            ssize_t bytes_read = read_local_file_windowed(local_file, read_buf, len, static_cast<size_t>(offset));
            if (!PULL_MODE && bytes_read > 0) {
                int write_ret = 0;
                if (transport_is_roce(bulk_peer->rdma_transport)) {
                    write_ret = wki_roce_rdma_write_tagged(hdr->src_node, consumer_rkey, 0, read_buf, static_cast<uint32_t>(bytes_read),
                                                           REQ_COOKIE);
                } else {
                    write_ret = bulk_peer->rdma_transport->rdma_write(bulk_peer->rdma_transport, hdr->src_node, consumer_rkey, 0, read_buf,
                                                                      static_cast<uint32_t>(bytes_read));
                }
                if (write_ret != 0) {
                    bytes_read = -EIO;
                }
            }
            delete[] allocated_read_buf;

            uint32_t br = (bytes_read > 0) ? static_cast<uint32_t>(bytes_read) : 0;
            std::array<uint8_t, sizeof(DevOpRespPayload) + 4> bulk_resp_buf{};
            auto* bulk_resp = reinterpret_cast<DevOpRespPayload*>(bulk_resp_buf.data());
            bulk_resp->op_id = OP_VFS_READ_BULK;
            bulk_resp->status = (bytes_read >= 0) ? 0 : static_cast<int16_t>(bytes_read);
            bulk_resp->data_len = 4;
            bulk_resp->reserved = REQ_COOKIE;
            memcpy(bulk_resp_buf.data() + sizeof(DevOpRespPayload), &br, sizeof(uint32_t));
            static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, bulk_resp_buf.data(),
                                                           static_cast<uint16_t>(bulk_resp_buf.size())));
            break;
        }

        case OP_VFS_WRITE_RDMA: {
            // Request: {fd:i32, off:i64, len:u32} = 16 bytes
            // Consumer sends this control message first, then rdma_writes the
            // data into our pre-registered write buffer.
            VfsWriteRegionInfo write_region_for_cleanup = {};
            auto send_rdma_write_err = [&](int16_t status) {
                if (write_region_for_cleanup.transport != nullptr && write_region_for_cleanup.transport->name != nullptr &&
                    std::strcmp(write_region_for_cleanup.transport->name, "wki-roce") == 0) {
                    wki_roce_region_finish_tagged_write(write_region_for_cleanup.rkey, REQ_COOKIE);
                }
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_WRITE_RDMA;
                resp.status = status;
                resp.data_len = 0;
                wki_dev_server_complete_vfs_write(channel_identity, REQ_COOKIE, status, 0);
                send_simple_resp(resp);
            };

            if (data_len < 16) {
                send_rdma_write_err(-EINVAL);
                break;
            }

            int32_t fd_id = 0;
            int64_t offset = 0;
            uint32_t len = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&offset, data + 4, sizeof(int64_t));
            memcpy(&len, data + 12, sizeof(uint32_t));

            len = std::min<uint32_t>(len, VFS_RDMA_WRITE_SIZE);

            VfsWriteRegionInfo const WRITE_REGION = wki_dev_server_get_vfs_write_region(channel_identity);
            write_region_for_cleanup = WRITE_REGION;
            uint8_t const* write_src = WRITE_REGION.buf;
            if (write_src == nullptr) {
                send_rdma_write_err(-EIO);
                break;
            }

            if (WRITE_REGION.transport != nullptr && WRITE_REGION.transport->name != nullptr &&
                std::strcmp(WRITE_REGION.transport->name, "wki-roce") == 0 &&
                !wki_roce_region_wait_tagged_write(WRITE_REGION.rkey, REQ_COOKIE, len, VFS_PROXY_OP_TIMEOUT_US)) {
                send_rdma_write_err(-ETIMEDOUT);
                break;
            }

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(channel_identity, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_write == nullptr) {
                s_vfs_lock.unlock();
                send_rdma_write_err(-EBADF);
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            ssize_t const BYTES_WRITTEN = local_file->fops->vfs_write(local_file, write_src, len, static_cast<size_t>(offset));
            if (BYTES_WRITTEN >= 0) {
                ker::vfs::vfs_cache_notify_file_data_changed(local_file);
            }
            if (WRITE_REGION.transport != nullptr && WRITE_REGION.transport->name != nullptr &&
                std::strcmp(WRITE_REGION.transport->name, "wki-roce") == 0) {
                wki_roce_region_finish_tagged_write(WRITE_REGION.rkey, REQ_COOKIE);
            }

            // Response: {bytes_written:u32} = 4 bytes
            uint32_t bw = (BYTES_WRITTEN > 0) ? static_cast<uint32_t>(BYTES_WRITTEN) : 0;
            std::array<uint8_t, sizeof(DevOpRespPayload) + 4> resp_buf{};
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_VFS_WRITE_RDMA;
            resp->status = (BYTES_WRITTEN >= 0) ? 0 : static_cast<int16_t>(BYTES_WRITTEN);
            resp->data_len = 4;
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &bw, sizeof(uint32_t));
            wki_dev_server_complete_vfs_write(channel_identity, REQ_COOKIE, resp->status, bw);
            send_buffered_resp(resp_buf.data(), static_cast<uint16_t>(resp_buf.size()));
            break;
        }

        default: {
            DevOpRespPayload resp = {};
            resp.op_id = op_id;
            resp.status = -ENOSYS;
            resp.data_len = 0;
            send_simple_resp(resp);
            break;
        }
    }
}

}  // namespace detail

// -------------------------------------------------------------------------------
// Consumer Side - Mount / Open / Response Handlers
// -------------------------------------------------------------------------------

namespace {

auto mount_vfs_proxy_lane(uint16_t owner_node, uint32_t resource_id, const char* local_mount_path, uint64_t resource_generation,
                          const ResourceIncarnationToken& owner_incarnation, uint32_t binding_peer_boot_epoch, uint64_t mount_group_id,
                          uint8_t lane_index, bool lane_anchor) -> int {
    if (local_mount_path == nullptr) {
        return -EINVAL;
    }

    uint64_t const RESOURCE_GENERATION = resource_generation;
    uint32_t const BINDING_PEER_BOOT_EPOCH = binding_peer_boot_epoch;

    // Allocate proxy state
    uint8_t attach_cookie = 0;
    s_vfs_lock.lock();
    auto* state = create_vfs_proxy_state_locked(owner_node, resource_id, RESOURCE_GENERATION, owner_incarnation, BINDING_PEER_BOOT_EPOCH,
                                                local_mount_path, mount_group_id, lane_index, lane_anchor);

    WkiWaitEntry wait = {};
    attach_cookie = allocate_vfs_attach_cookie_locked(owner_node, resource_id, owner_incarnation);
    if (attach_cookie == 0) {
        s_vfs_lock.unlock();
        discard_unpublished_proxy(state);
        return -EBUSY;
    }
    state->binding_attach_cookie = attach_cookie;
    state->attach_wait_entry = &wait;
    state->attach_expected_cookie = attach_cookie;
    state->attach_expect_incarnation = wki_resource_incarnation_negotiated(owner_node, ResourceType::VFS);
    state->attach_expected_incarnation = owner_incarnation;
    state->attach_pending.store(true, std::memory_order_release);
    state->attach_status = 0;
    state->attach_channel = 0;
    s_vfs_lock.unlock();

    // Send DEV_ATTACH_REQ
    DevAttachReqPayload attach_req = {};
    attach_req.target_node = owner_node;
    attach_req.resource_type = static_cast<uint16_t>(ResourceType::VFS);
    attach_req.resource_id = resource_id;
    bool const MULTI_RDMA_LANES = wki_peer_capability_negotiated(owner_node, WKI_CAP_VFS_MULTI_RDMA_LANES);
    bool const RDMA_LANE = lane_anchor || (MULTI_RDMA_LANES && lane_index < VFS_PROXY_RDMA_LANE_COUNT);
    attach_req.attach_mode = wki_vfs_proxy_attach_mode(lane_anchor, RDMA_LANE);
    attach_req.attach_cookie = attach_cookie;

    WkiChannelIdentity reserved_channel_identity{};
    auto close_reserved_channel = [&reserved_channel_identity]() {
        static_cast<void>(wki_channel_close_generation(reserved_channel_identity.channel, reserved_channel_identity.peer_node_id,
                                                       reserved_channel_identity.channel_id, reserved_channel_identity.generation));
    };
    if (wki_requester_controls_dynamic_channel(g_wki.my_node_id, owner_node)) {
        // Remote VFS RPCs are stop-and-wait. Use latency ACKs so a long server
        // write does not look like a lost control frame to the sender.
        if (wki_channel_alloc(owner_node, PriorityClass::LATENCY, &reserved_channel_identity) == nullptr) {
            cancel_proxy_attach_wait(state, wait, -ENOMEM);
            discard_unpublished_proxy(state);
            return -ENOMEM;
        }
        attach_req.requested_channel = reserved_channel_identity.channel_id;
    } else {
        attach_req.requested_channel = 0;
    }

    WkiChannelIdentity resource_channel_identity{};
    if (!capture_peer_channel_identity(owner_node, WKI_CHAN_RESOURCE, &resource_channel_identity)) {
        cancel_proxy_attach_wait(state, wait, WKI_ERR_PEER_FENCED);
        close_reserved_channel();
        discard_unpublished_proxy(state);
        return WKI_ERR_PEER_FENCED;
    }

    std::array<uint8_t, sizeof(DevAttachReqPayload) + sizeof(ResourceIncarnationToken)> attach_buf{};
    std::memcpy(attach_buf.data(), &attach_req, sizeof(attach_req));
    uint16_t attach_len = sizeof(DevAttachReqPayload);
    if (state->attach_expect_incarnation) {
        if (!wki_resource_incarnation_valid(owner_incarnation)) {
            cancel_proxy_attach_wait(state, wait, -ENOENT);
            close_reserved_channel();
            discard_unpublished_proxy(state);
            return -ENOENT;
        }
        std::memcpy(attach_buf.data() + sizeof(DevAttachReqPayload), &owner_incarnation, sizeof(owner_incarnation));
        attach_len = static_cast<uint16_t>(attach_len + sizeof(ResourceIncarnationToken));
    }

    int const SEND_RET = wki_send_on_channel_identity(resource_channel_identity, MsgType::DEV_ATTACH_REQ, attach_buf.data(), attach_len);
    if (SEND_RET != WKI_OK) {
        cancel_proxy_attach_wait(state, wait, SEND_RET);
        close_reserved_channel();
        discard_unpublished_proxy(state);
        return SEND_RET;
    }

    uint64_t const ATTACH_STARTED_US = wki_now_us();
    uint64_t const ATTACH_TIMEOUT_US = lane_anchor ? VFS_PROXY_OP_TIMEOUT_US : VFS_PROXY_AUX_ATTACH_TIMEOUT_US;
    int const WAIT_RC = wki_wait_for_op(&wait, ATTACH_TIMEOUT_US);
    s_vfs_lock.lock();
    if (WAIT_RC == 0 && state->attach_wait_entry == &wait) {
        state->attach_wait_entry = nullptr;
        clear_proxy_attach_state_locked(state, static_cast<uint8_t>(DevAttachStatus::BUSY));
    }
    uint16_t const ATTACH_CHANNEL = state->attach_channel;
    uint8_t const ATTACH_STATUS = state->attach_status;
    s_vfs_lock.unlock();
    perf_record_vfs_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::ATTACH_WAIT), owner_node, ATTACH_CHANNEL, WAIT_RC,
                          static_cast<uint32_t>(wki_now_us() - ATTACH_STARTED_US), WOS_PERF_CALLSITE());
    if (WAIT_RC != 0) {
        cancel_proxy_attach_wait(state, wait, WAIT_RC);
        // SEND_RET succeeded, so retire any binding created by a delayed
        // request/ACK before abandoning the exact local channel allocation.
        // Stage the cookie-qualified DEV_DETACH before releasing local state;
        // the task worker retains and retries the tuple until it is ACKed.
        static_cast<void>(send_or_defer_vfs_detach(state, owner_node, resource_id, attach_cookie, owner_incarnation));
        close_reserved_channel();
        discard_unpublished_proxy(state);
        if (WAIT_RC == WKI_ERR_TIMEOUT) {
            ker::mod::dbg::log("[WKI] Remote VFS attach timeout: node=0x%04x res_id=%u", owner_node, resource_id);
        } else {
            ker::mod::dbg::log("[WKI] Remote VFS attach aborted: node=0x%04x res_id=%u rc=%d", owner_node, resource_id, WAIT_RC);
        }
        return WAIT_RC;
    }

    if (ATTACH_STATUS != static_cast<uint8_t>(DevAttachStatus::OK)) {
        int const ATTACH_ERRNO = dev_attach_status_to_errno(ATTACH_STATUS);
        ker::mod::dbg::log("[WKI] Remote VFS attach rejected: node=0x%04x res_id=%u path=%s status=%u ret=%d", owner_node, resource_id,
                           local_mount_path, ATTACH_STATUS, ATTACH_ERRNO);
        close_reserved_channel();
        discard_unpublished_proxy(state);
        return ATTACH_ERRNO;
    }

    if (reserved_channel_identity.channel != nullptr) {
        if (ATTACH_CHANNEL != reserved_channel_identity.channel_id) {
            ker::mod::dbg::log("[WKI] Remote VFS attach channel mismatch: node=0x%04x res_id=%u requested=%u assigned=%u", owner_node,
                               resource_id, reserved_channel_identity.channel_id, ATTACH_CHANNEL);
            static_cast<void>(send_or_defer_vfs_detach(state, owner_node, resource_id, attach_cookie, owner_incarnation));
            close_reserved_channel();
            discard_unpublished_proxy(state);
            return -EIO;
        }
    } else {
        if (wki_channel_reserve(owner_node, ATTACH_CHANNEL, PriorityClass::LATENCY, &reserved_channel_identity) == nullptr) {
            ker::mod::dbg::log("[WKI] Remote VFS attach local reserve failed: node=0x%04x res_id=%u ch=%u", owner_node, resource_id,
                               ATTACH_CHANNEL);
            static_cast<void>(send_or_defer_vfs_detach(state, owner_node, resource_id, attach_cookie, owner_incarnation));
            discard_unpublished_proxy(state);
            return -EIO;
        }
    }

    bool assigned_channel_published = false;
    reserved_channel_identity.channel->lock.lock();
    if (reserved_channel_identity.channel->active &&
        reserved_channel_identity.channel->peer_node_id == reserved_channel_identity.peer_node_id &&
        reserved_channel_identity.channel->channel_id == reserved_channel_identity.channel_id &&
        reserved_channel_identity.channel->generation == reserved_channel_identity.generation &&
        reserved_channel_identity.peer_node_id == owner_node && reserved_channel_identity.channel_id == ATTACH_CHANNEL) {
        // Publish proxy ownership while the channel lock still excludes close
        // and reuse of this exact pool-slot generation.
        s_vfs_lock.lock();
        if (!state->epoch_reset_pending) {
            state->assigned_channel = reserved_channel_identity.channel_id;
            state->assigned_channel_ref = reserved_channel_identity.channel;
            state->assigned_channel_generation = reserved_channel_identity.generation;
            state->max_op_size = state->attach_max_op_size;
            state->active = true;
            assigned_channel_published = true;
        }
        s_vfs_lock.unlock();
    }
    reserved_channel_identity.channel->lock.unlock();
    if (!assigned_channel_published) {
        static_cast<void>(send_or_defer_vfs_detach(state, owner_node, resource_id, attach_cookie, owner_incarnation));
        discard_unpublished_proxy(state);
        return -EIO;
    }

    // RDMA setup: bind the peer transport once, then enable read, write, and
    // bulk capabilities independently. Remote-root reads must not depend on the
    // server also having a write-receive buffer.
    WkiPeer const* peer = wki_peer_find(owner_node);
    if (RDMA_LANE && peer != nullptr && peer->rdma_transport != nullptr && peer->rdma_transport->rdma_register_region != nullptr) {
        state->rdma_transport = peer->rdma_transport;

        bool const NEED_READ_BOUNCE =
            state->rdma_server_read_staging_rkey != 0 || transport_supports_vfs_read_push_rdma(state->rdma_transport);
        if (NEED_READ_BOUNCE) {
            auto* bbuf = new (std::nothrow) uint8_t[VFS_RDMA_BOUNCE_SIZE];
            if (bbuf != nullptr) {
                uint32_t rkey = 0;
                int const REG_RET = state->rdma_transport->rdma_register_region(state->rdma_transport, reinterpret_cast<uint64_t>(bbuf),
                                                                                VFS_RDMA_BOUNCE_SIZE, &rkey);
                if (REG_RET == 0 && rkey != 0) {
                    state->rdma_bounce_buf = bbuf;
                    state->rdma_read_rkey = rkey;
                    state->rdma_capable = true;
                    ker::mod::dbg::log(
                        "[WKI] VFS RDMA enabled: node=0x%04x read_rkey=%u write_rkey=%u read_staging_rkey=%u bulk_staging_rkey=%u",
                        owner_node, rkey, state->rdma_server_write_rkey, state->rdma_server_read_staging_rkey,
                        state->rdma_server_bulk_staging_rkey);
                } else {
                    if (REG_RET == 0 && state->rdma_transport->rdma_unregister_region != nullptr) {
                        static_cast<void>(state->rdma_transport->rdma_unregister_region(state->rdma_transport, rkey, VFS_RDMA_BOUNCE_SIZE));
                    }
                    delete[] bbuf;
                    ker::mod::dbg::log("[WKI] VFS RDMA read-bounce region reg failed: node=0x%04x ret=%d", owner_node, REG_RET);
                }
            } else {
                ker::mod::dbg::log("[WKI] VFS RDMA read-bounce allocation failed: node=0x%04x size=%u", owner_node, VFS_RDMA_BOUNCE_SIZE);
            }
        }
    } else {
        state->rdma_server_write_rkey = 0;
        state->rdma_server_read_staging_rkey = 0;
        state->rdma_server_bulk_staging_rkey = 0;
    }

    // Bulk RDMA setup is independent from the 64 KiB read bounce buffer and the
    // server write-rkey. RoCE pull mode uses the advertised server staging rkey;
    // ivshmem push mode only needs our registered consumer buffer rkey.
    bool const BULK_PULL_REQUESTED = transport_supports_rdma_read_pull(state->rdma_transport) && state->rdma_server_bulk_staging_rkey != 0;
    bool const BULK_PUSH_REQUESTED = !BULK_PULL_REQUESTED && transport_supports_vfs_read_push_rdma(state->rdma_transport);
    if ((BULK_PULL_REQUESTED || BULK_PUSH_REQUESTED) && state->rdma_transport != nullptr &&
        state->rdma_transport->rdma_register_region != nullptr) {
        uint32_t const BULK_SIZE = BULK_PULL_REQUESTED ? VFS_RDMA_ROCE_BULK_SIZE : VFS_RDMA_BULK_SIZE;
        auto* bulk_buf = new (std::nothrow) uint8_t[BULK_SIZE];
        if (bulk_buf != nullptr) {
            uint32_t bulk_rkey = 0;
            int const REG_RET = state->rdma_transport->rdma_register_region(state->rdma_transport, reinterpret_cast<uint64_t>(bulk_buf),
                                                                            BULK_SIZE, &bulk_rkey);
            if (REG_RET == 0 && bulk_rkey != 0) {
                state->rdma_bulk_buf = bulk_buf;
                state->rdma_bulk_rkey = bulk_rkey;
                state->rdma_bulk_size = BULK_SIZE;
                state->bulk_rdma_capable = true;
                ker::mod::dbg::log("[WKI] VFS bulk RDMA enabled: node=0x%04x mode=%s bulk_rkey=%u size=%u", owner_node,
                                   BULK_PULL_REQUESTED ? "pull" : "push", bulk_rkey, BULK_SIZE);
            } else {
                if (REG_RET == 0 && state->rdma_transport->rdma_unregister_region != nullptr) {
                    static_cast<void>(state->rdma_transport->rdma_unregister_region(state->rdma_transport, bulk_rkey, BULK_SIZE));
                }
                delete[] bulk_buf;
                ker::mod::dbg::log("[WKI] VFS bulk RDMA region reg failed: node=0x%04x ret=%d size=%u", owner_node, REG_RET, BULK_SIZE);
            }
        } else {
            ker::mod::dbg::log("[WKI] VFS bulk RDMA allocation failed: node=0x%04x size=%u", owner_node, BULK_SIZE);
        }
    }

    if (!lane_anchor) {
        bool published = false;
        uint16_t assigned_channel = 0;
        s_vfs_lock.lock();
        for (auto& proxy : g_vfs_proxies) {
            auto* anchor = proxy.get();
            if (anchor == nullptr || !anchor->lane_anchor || anchor->mount_group_id != mount_group_id) {
                continue;
            }
            if (anchor->active && anchor->mount_configured && !anchor->destroy_when_idle && !anchor->resources_releasing &&
                !anchor->resources_released && state->active && !state->destroy_when_idle && state->lane_index == lane_index &&
                anchor->lane_count == lane_index && lane_index < anchor->lanes.size()) {
                anchor->lanes.at(lane_index) = state;
                anchor->lane_count = static_cast<uint8_t>(lane_index + 1U);
                assigned_channel = state->assigned_channel;
                published = true;
            }
            break;
        }
        s_vfs_lock.unlock();

        if (!published) {
            discard_failed_attached_proxy(state);
            return WKI_ERR_PEER_FENCED;
        }

        release_vfs_proxy_lifecycle_ref(state);
        ker::mod::dbg::log("[WKI] Remote VFS auxiliary lane mounted: %s -> node=0x%04x res_id=%u lane=%u ch=%u", local_mount_path,
                           owner_node, resource_id, lane_index, assigned_channel);
        return 0;
    }

    // Stage lane zero before publishing the VFS row.  mount_configured stays
    // false until mount_filesystem() returns, so withdrawal still treats this
    // state as private; fops selection is safe because the row is its only
    // external source of the anchor pointer.
    s_vfs_lock.lock();
    state->lanes.fill(nullptr);
    state->lanes.at(0) = state;
    state->lane_count = 1;
    bool const ANCHOR_READY = state->active && !state->destroy_when_idle && !state->resources_releasing && !state->resources_released;
    state->lanes_ready = ANCHOR_READY;
    uint16_t const ASSIGNED_CHANNEL = state->assigned_channel;
    s_vfs_lock.unlock();
    if (!ANCHOR_READY) {
        discard_failed_attached_proxy(state);
        return WKI_ERR_PEER_FENCED;
    }

    // Create the mount point with "remote" fstype.  Lane zero is already
    // selectable, so a task that resolves the just-published row cannot wait
    // behind the bounded auxiliary attach sequence below.
    int const MOUNT_RET = ker::vfs::mount_filesystem(local_mount_path, "remote", nullptr, 0, nullptr, state, &g_remote_vfs_fops);
    if (MOUNT_RET != 0) {
        ker::mod::dbg::log("[WKI] Remote VFS mount failed at %s", local_mount_path);
        discard_failed_attached_proxy(state);
        return MOUNT_RET;
    }

    s_vfs_lock.lock();
    state->mount_configured = true;
    s_vfs_lock.unlock();

    // The attach/mount handshake can block long enough for RESOURCE_WITHDRAW
    // or peer epoch cleanup to retire this exact advertised/channel
    // generation. Validate both before and after the bounded auxiliary-lane
    // sequence while retaining the construction pin through any rollback.
    auto validate_mount_binding = [&]() -> int {
        WkiPeer* final_peer = wki_peer_find(owner_node);
        bool const LIFECYCLE_ACQUIRED = wki_peer_lifecycle_acquire(final_peer);
        bool binding_still_live = false;
        bool resource_still_live = false;
        if (LIFECYCLE_ACQUIRED) {
            s_vfs_lock.lock();
            bool const PROXY_STILL_ACTIVE = state->active;
            WkiChannel* const CHANNEL_REF = state->assigned_channel_ref;
            uint16_t const CHANNEL_ID = state->assigned_channel;
            uint32_t const CHANNEL_GENERATION = state->assigned_channel_generation;
            s_vfs_lock.unlock();

            binding_still_live = final_peer->node_id == owner_node && final_peer->state == PeerState::CONNECTED &&
                                 !final_peer->vfs_reset_rebind_pending.load(std::memory_order_acquire) && PROXY_STILL_ACTIVE &&
                                 wki_channel_generation_is_live(CHANNEL_REF, owner_node, CHANNEL_ID, CHANNEL_GENERATION);
            resource_still_live =
                wki_resource_observation_is_live(owner_node, ResourceType::VFS, resource_id, RESOURCE_GENERATION, owner_incarnation);
            wki_peer_lifecycle_release(final_peer);
        }

        if (!resource_still_live) {
            return -ENOENT;
        }
        return binding_still_live ? 0 : WKI_ERR_PEER_FENCED;
    };

    int const PRE_AUX_VALIDATION = validate_mount_binding();
    if (PRE_AUX_VALIDATION != 0) {
        wki_remote_vfs_unmount_resource_generation(owner_node, resource_id, RESOURCE_GENERATION);
        release_vfs_proxy_lifecycle_ref(state);
        return PRE_AUX_VALIDATION;
    }

    for (uint8_t lane_index = 1; lane_index < VFS_PROXY_LANE_COUNT; ++lane_index) {
        int const LANE_RET = mount_vfs_proxy_lane(owner_node, resource_id, local_mount_path, RESOURCE_GENERATION, owner_incarnation,
                                                  BINDING_PEER_BOOT_EPOCH, mount_group_id, lane_index, false);
        if (LANE_RET != 0) {
            ker::mod::dbg::log("[WKI] Remote VFS auxiliary lane unavailable: node=0x%04x res_id=%u lane=%u ret=%d", owner_node, resource_id,
                               lane_index, LANE_RET);
            // Capacity and transport failures may still leave a useful
            // partial group. NOT_FOUND and STALE_RESOURCE are definitive:
            // the server proved that lane zero's advertised identity is no
            // longer attachable, so do not publish that stale anchor.
            if (LANE_RET == -ENOENT || LANE_RET == -ESTALE) {
                wki_remote_vfs_unmount_resource_generation(owner_node, resource_id, RESOURCE_GENERATION);
                release_vfs_proxy_lifecycle_ref(state);
                return LANE_RET;
            }
            break;
        }
    }

    int const POST_AUX_VALIDATION = validate_mount_binding();
    if (POST_AUX_VALIDATION != 0) {
        wki_remote_vfs_unmount_resource_generation(owner_node, resource_id, RESOURCE_GENERATION);
        release_vfs_proxy_lifecycle_ref(state);
        return POST_AUX_VALIDATION;
    }

    s_vfs_lock.lock();
    bool const ANCHOR_STILL_ACTIVE = state->active && state->mount_configured && state->lanes_ready && state->lane_count > 0;
    uint8_t const LANE_COUNT = state->lane_count;
    s_vfs_lock.unlock();
    if (!ANCHOR_STILL_ACTIVE) {
        wki_remote_vfs_unmount_resource_generation(owner_node, resource_id, RESOURCE_GENERATION);
        release_vfs_proxy_lifecycle_ref(state);
        return WKI_ERR_PEER_FENCED;
    }

    release_vfs_proxy_lifecycle_ref(state);
    ker::mod::dbg::log("[WKI] Remote VFS mounted: %s -> node=0x%04x res_id=%u ch=%u lanes=%u", local_mount_path, owner_node, resource_id,
                       ASSIGNED_CHANNEL, LANE_COUNT);
    return 0;
}

}  // namespace

auto wki_remote_vfs_mount(uint16_t owner_node, uint32_t resource_id, const char* local_mount_path, uint64_t expected_resource_generation)
    -> int {
    if (local_mount_path == nullptr) {
        return -EINVAL;
    }

    uint64_t const RESOURCE_GENERATION = expected_resource_generation != 0
                                             ? expected_resource_generation
                                             : wki_resource_generation_snapshot(owner_node, ResourceType::VFS, resource_id);
    ResourceIncarnationToken owner_incarnation = {};
    if (!wki_resource_observation_snapshot(owner_node, ResourceType::VFS, resource_id, RESOURCE_GENERATION, &owner_incarnation)) {
        return -ENOENT;
    }
    uint32_t const BINDING_PEER_BOOT_EPOCH = wki_resource_incarnation_valid(owner_incarnation)
                                                 ? owner_incarnation.owner_boot_epoch
                                                 : wki_peer_remote_boot_epoch_snapshot(owner_node);

    uint64_t mount_group_id = 0;
    s_vfs_lock.lock();
    if (vfs_attach_blocked_by_retiring_binding_locked(owner_node, resource_id)) {
        s_vfs_lock.unlock();
        return -EAGAIN;
    }
    mount_group_id = allocate_vfs_mount_group_id_locked();
    s_vfs_lock.unlock();

    return mount_vfs_proxy_lane(owner_node, resource_id, local_mount_path, RESOURCE_GENERATION, owner_incarnation, BINDING_PEER_BOOT_EPOCH,
                                mount_group_id, 0, true);
}

namespace {
struct PendingVfsProxyGroupTeardown {
    std::array<PendingProxyTeardown, VFS_PROXY_LANE_COUNT> lanes = {};
    std::array<bool, VFS_PROXY_LANE_COUNT> detach_remote = {};
    size_t count = 0;
    ProxyVfsState* anchor = nullptr;
};

// Caller must hold s_vfs_lock.  Clear the public anchor table before any
// lane is deactivated, then snapshot every group member (including a child
// whose attach is still in flight) for task-context teardown.
void claim_vfs_proxy_group_unmount_locked(ProxyVfsState* anchor, PendingVfsProxyGroupTeardown& group) {
    if (anchor == nullptr || !anchor->lane_anchor || anchor->mount_group_id == 0) [[unlikely]] {
        ker::mod::dbg::panic_handler("WKI remote VFS: invalid lane-group unmount anchor");
        hcf();
    }

    group.anchor = anchor;
    mark_vfs_proxy_group_unavailable_locked(anchor);
    for (auto& proxy : g_vfs_proxies) {
        auto* state = proxy.get();
        if (state == nullptr || state->mount_group_id != anchor->mount_group_id) {
            continue;
        }
        if (group.count >= group.lanes.size()) [[unlikely]] {
            ker::mod::dbg::panic_handler("WKI remote VFS: lane group exceeds fixed bound");
            hcf();
        }

        auto& teardown = group.lanes.at(group.count);
        bool const DETACH_REMOTE = state->active || state->epoch_reset_pending || state->attach_pending.load(std::memory_order_acquire);
        deactivate_vfs_proxy_locked(state, teardown, true);
        group.detach_remote.at(group.count) = DETACH_REMOTE || teardown.had_attach_pending;
        if (group.detach_remote.at(group.count)) {
            teardown.detach_staged = stage_vfs_detach_locked(state, teardown.owner_node, teardown.resource_id,
                                                             teardown.binding_attach_cookie, teardown.binding_incarnation, false);
            if (state->detach_pending) {
                state->epoch_reset_pending = false;
            }
        }
        // Hidden lanes have no VFS mount-table row.  They can be released as
        // soon as the group table is unpublished and their waiters quiesce.
        if (state != anchor) {
            state->mount_released = true;
        }
        invalidate_all_dir_caches(state);
        group.count++;
    }
}

auto claim_vfs_proxy_unmount_by_path(const char* local_mount_path, PendingVfsProxyGroupTeardown& group) -> bool {
    s_vfs_lock.lock();
    ProxyVfsState* const anchor = find_vfs_proxy_by_mount(local_mount_path);
    if (anchor == nullptr) {
        s_vfs_lock.unlock();
        return false;
    }
    claim_vfs_proxy_group_unmount_locked(anchor, group);
    s_vfs_lock.unlock();
    return true;
}

auto claim_vfs_proxy_unmount_by_generation(uint16_t owner_node, uint32_t resource_id, uint64_t resource_generation,
                                           PendingVfsProxyGroupTeardown& group) -> bool {
    s_vfs_lock.lock();
    ProxyVfsState* anchor = nullptr;
    for (auto& proxy : g_vfs_proxies) {
        auto* state = proxy.get();
        if (state == nullptr || !state->lane_anchor || !state->mount_configured || state->destroy_when_idle || state->mount_released ||
            state->owner_node != owner_node || state->resource_id != resource_id || state->resource_generation != resource_generation) {
            continue;
        }
        anchor = state;
        break;
    }
    if (anchor == nullptr) {
        s_vfs_lock.unlock();
        return false;
    }
    claim_vfs_proxy_group_unmount_locked(anchor, group);
    s_vfs_lock.unlock();
    return true;
}

void finish_vfs_proxy_lane_teardown(const PendingProxyTeardown& teardown, bool detach_remote) {
    if (teardown.state == nullptr) {
        return;
    }

    finish_proxy_teardown_op_waiter(teardown, -1);
    ker::vfs::vfs_stream_cache_invalidate_remote_scope(teardown.state);
    invalidate_readlink_cache(teardown.state);

    if (teardown.had_op_pending) {
        ker::mod::dbg::log("[WKI] VFS op UNMOUNT: node=0x%04x ch=%u op=%u seq=%u mount=%s", teardown.owner_node, teardown.assigned_channel,
                           teardown.op_expected_id, teardown.op_expected_seq, teardown.local_mount_path.data());
    }

    wake_proxy_slot_waiters(teardown);
    finish_claimed_waiter(teardown.attach_wait_entry, -1);

    if (detach_remote) {
        if (teardown.detach_staged) {
            wki_deferred_work_notify();
        }
        static_cast<void>(wki_channel_close_generation(teardown.assigned_channel_ref, teardown.owner_node, teardown.assigned_channel,
                                                       teardown.assigned_channel_generation));
    }
}

void finish_vfs_proxy_group_unmount(const PendingVfsProxyGroupTeardown& group) {
    if (group.anchor == nullptr || group.count == 0) {
        return;
    }

    for (size_t i = 0; i < group.count; ++i) {
        finish_vfs_proxy_lane_teardown(group.lanes.at(i), group.detach_remote.at(i));
    }

    int const UNMOUNT_RET = ker::vfs::unmount_filesystem_by_private_data(static_cast<const void*>(group.anchor));
    if (UNMOUNT_RET != 0 && UNMOUNT_RET != -ENOENT) {
        ker::mod::dbg::log("[WKI] Exact remote VFS unmount failed: node=0x%04x res_id=%u path=%s ret=%d", group.anchor->owner_node,
                           group.anchor->resource_id, group.anchor->local_mount_path.data(), UNMOUNT_RET);
    }

    for (size_t i = 0; i < group.count; ++i) {
        auto* state = group.lanes.at(i).state;
        if (state == nullptr || state == group.anchor) {
            continue;
        }
        mark_vfs_proxy_mount_released_and_maybe_destroy(state);
    }
    // Owner-only ENOENT proves no mount-table row still references the anchor.
    if (UNMOUNT_RET == 0 || UNMOUNT_RET == -ENOENT) {
        mark_vfs_proxy_mount_released_and_maybe_destroy(group.anchor);
    }

    for (size_t i = 0; i < group.count; ++i) {
        auto* state = group.lanes.at(i).state;
        if (state != nullptr && state != group.anchor) {
            release_vfs_proxy_lifecycle_ref(state);
        }
    }
    release_vfs_proxy_lifecycle_ref(group.anchor);
}
}  // namespace

void wki_remote_vfs_unmount(const char* local_mount_path) {
    if (local_mount_path == nullptr) {
        return;
    }

    PendingVfsProxyGroupTeardown group = {};
    if (claim_vfs_proxy_unmount_by_path(local_mount_path, group)) {
        finish_vfs_proxy_group_unmount(group);
    }
}

void wki_remote_vfs_unmount_resource_generation(uint16_t owner_node, uint32_t resource_id, uint64_t resource_generation) {
    if (resource_generation == 0) {
        return;
    }

    while (true) {
        PendingVfsProxyGroupTeardown group = {};
        if (!claim_vfs_proxy_unmount_by_generation(owner_node, resource_id, resource_generation, group)) {
            return;
        }
        finish_vfs_proxy_group_unmount(group);
    }
}

auto wki_remote_vfs_has_mount_for_resource_generation(uint16_t owner_node, uint32_t resource_id, uint64_t resource_generation) -> bool {
    if (resource_generation == 0) {
        return false;
    }

    bool found = false;
    s_vfs_lock.lock();
    for (auto const& proxy : g_vfs_proxies) {
        if (proxy != nullptr && proxy->lane_anchor && proxy->active && proxy->mount_configured && proxy->owner_node == owner_node &&
            proxy->resource_id == resource_id && proxy->resource_generation == resource_generation) {
            found = true;
            break;
        }
    }
    s_vfs_lock.unlock();
    return found;
}

auto wki_remote_vfs_proxy_diag_snapshot(WkiRemoteVfsProxyDiag* out, size_t max) -> size_t {
    if (out == nullptr || max == 0) {
        return 0;
    }

    size_t count = 0;
    s_vfs_lock.lock();
    for (const auto& proxy : g_vfs_proxies) {
        if (proxy == nullptr || !proxy->lane_anchor || !proxy->active || count >= max) {
            continue;
        }

        auto& row = out[count++];
        row.owner_node = proxy->owner_node;
        row.assigned_channel = proxy->assigned_channel;
        row.resource_id = proxy->resource_id;
        row.active = proxy->active;
        row.op_pending = proxy->op_pending.load(std::memory_order_acquire);
        row.op_expected_id = proxy->op_expected_id;
        row.op_expected_seq = proxy->op_expected_seq;
        row.attach_pending = proxy->attach_pending.load(std::memory_order_acquire);
        row.local_mount_path = proxy->local_mount_path;
    }
    s_vfs_lock.unlock();

    return count;
}

auto wki_remote_vfs_find_mount_for_resource(uint16_t owner_node, uint32_t resource_id, char* out, size_t out_size) -> bool {
    if (out == nullptr || out_size == 0) {
        return false;
    }

    out[0] = '\0';

    s_vfs_lock.lock();
    for (auto& proxy : g_vfs_proxies) {
        if (!proxy->lane_anchor || !proxy->active || !proxy->mount_configured || proxy->owner_node != owner_node ||
            proxy->resource_id != resource_id) {
            continue;
        }

        size_t const MOUNT_LEN = std::strlen(proxy->local_mount_path.data());
        if (MOUNT_LEN + 1 > out_size) {
            break;
        }

        std::memcpy(out, proxy->local_mount_path.data(), MOUNT_LEN + 1);
        s_vfs_lock.unlock();
        return true;
    }
    s_vfs_lock.unlock();

    return false;
}

auto wki_remote_vfs_find_resource_for_mount(uint16_t owner_node, const char* local_mount_path, uint32_t* resource_id_out) -> bool {
    if (local_mount_path == nullptr || resource_id_out == nullptr) {
        return false;
    }

    s_vfs_lock.lock();
    for (auto& proxy : g_vfs_proxies) {
        if (!proxy->lane_anchor || !proxy->active || !proxy->mount_configured || proxy->owner_node != owner_node) {
            continue;
        }
        if (std::strncmp(proxy->local_mount_path.data(), local_mount_path, proxy->local_mount_path.size()) != 0) {
            continue;
        }

        *resource_id_out = proxy->resource_id;
        s_vfs_lock.unlock();
        return true;
    }
    s_vfs_lock.unlock();

    return false;
}

auto wki_remote_vfs_open_path(const char* fs_relative_path, int flags, int mode, void* mount_private_data) -> ker::vfs::File* {
    constexpr int OPEN_ACCMODE = 0x3;
    constexpr int OPEN_RDONLY = 0x0;
    constexpr int OPEN_WRONLY = 0x1;
    constexpr int OPEN_RDWR = 0x2;

    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || fs_relative_path == nullptr) {
        return nullptr;
    }
    int const ACCESS_MODE = flags & OPEN_ACCMODE;
    // A remote FD is channel-scoped, so choose its data lane before OP_VFS_OPEN.
    // WOS opendir supplies O_DIRECTORY and therefore remains PID-striped.
    bool const NON_DIRECTORY_OPEN = (flags & ker::vfs::O_DIRECTORY) == 0;
    bool const PREFER_RDMA_READ_ANCHOR = NON_DIRECTORY_OPEN && (ACCESS_MODE == OPEN_RDONLY || ACCESS_MODE == OPEN_RDWR);
    bool const PREFER_RDMA_WRITE_ANCHOR = NON_DIRECTORY_OPEN && (ACCESS_MODE == OPEN_WRONLY || ACCESS_MODE == OPEN_RDWR);
    auto* state = acquire_vfs_proxy_lane(anchor, PREFER_RDMA_READ_ANCHOR, PREFER_RDMA_WRITE_ANCHOR);
    if (state == nullptr) {
        return nullptr;
    }
    ProxyLifecycleRefGuard lane_ref_guard(state);
    if (!acquire_vfs_proxy_open_ref(state)) {
        return nullptr;
    }
    ProxyOpenRefGuard open_ref_guard(state);

    bool const WANT_OPEN_PREFETCH = remote_vfs_open_prefetch_capable(state, flags);
    uint32_t const OPEN_PREFETCH_LEN = WANT_OPEN_PREFETCH ? std::min<uint32_t>(state->rdma_bulk_size, VFS_RDMA_BULK_SIZE) : 0;
    bool send_open_prefetch = WANT_OPEN_PREFETCH && OPEN_PREFETCH_LEN > 0;
    OptionalSharedIoSlotGuard const OPEN_PREFETCH_GUARD(state, send_open_prefetch, wki_now_us());
    if (send_open_prefetch && !OPEN_PREFETCH_GUARD.acquired()) {
        send_open_prefetch = false;
    }

    // Build request: {flags:u32, mode:u32, path_len:u16, path[N], optional prefetch_rkey:u32, prefetch_len:u32}
    size_t const PATH_LEN = strlen(fs_relative_path);
    size_t const REQ_FIXED_LEN = OPEN_REQ_BASE_LEN + (send_open_prefetch ? OPEN_PREFETCH_REQ_LEN : 0);
    if (REQ_FIXED_LEN > WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload) ||
        PATH_LEN > WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload) - REQ_FIXED_LEN) {
        return nullptr;
    }
    auto path_len = static_cast<uint16_t>(PATH_LEN);
    size_t const REQ_DATA_LEN = REQ_FIXED_LEN + PATH_LEN;
    std::array<uint8_t, OPEN_REQ_INLINE_CAPACITY> inline_req_data;  // NOLINT(cppcoreguidelines-pro-type-member-init)
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays): Variable-size nothrow fallback ownership.
    std::unique_ptr<uint8_t[]> heap_req_data;
    uint8_t* req_data = inline_req_data.data();
    if (REQ_DATA_LEN > inline_req_data.size()) {
        heap_req_data.reset(new (std::nothrow) uint8_t[REQ_DATA_LEN]);
        if (heap_req_data == nullptr) {
            return nullptr;
        }
        req_data = heap_req_data.get();
    }

    auto u_flags = static_cast<uint32_t>(flags);
    auto u_mode = static_cast<uint32_t>(mode);
    memcpy(req_data, &u_flags, sizeof(uint32_t));
    memcpy(req_data + 4, &u_mode, sizeof(uint32_t));
    memcpy(req_data + 8, &path_len, sizeof(uint16_t));
    if (path_len > 0) {
        memcpy(req_data + 10, fs_relative_path, path_len);
    }
    if (send_open_prefetch) {
        size_t const PREFETCH_OFF = OPEN_REQ_BASE_LEN + static_cast<size_t>(path_len);
        uint32_t const PREFETCH_RKEY = state->rdma_bulk_rkey;
        memcpy(req_data + PREFETCH_OFF, &PREFETCH_RKEY, sizeof(uint32_t));
        memcpy(req_data + PREFETCH_OFF + sizeof(uint32_t), &OPEN_PREFETCH_LEN, sizeof(uint32_t));
    }

    OpenRespPayload open_resp = {};
    uint16_t open_resp_len = 0;
    RoceTaggedReceive tagged_receive{};
    RoceTaggedReceive* tagged_receive_ptr = nullptr;
    if (send_open_prefetch) {
        tagged_receive.rkey = state->rdma_bulk_rkey;
        tagged_receive_ptr = &tagged_receive;
    }

    int const STATUS = vfs_proxy_send_and_wait(state, OP_VFS_OPEN, req_data, REQ_DATA_LEN, &open_resp, sizeof(open_resp), &open_resp_len,
                                               VFS_PROXY_OP_TIMEOUT_US, tagged_receive_ptr);

    if (STATUS != 0 || open_resp.fd < 0) {
        return nullptr;
    }

    uint32_t valid_prefetched_bytes = 0;
    if (send_open_prefetch && open_resp_len >= OPEN_RESP_WITH_PREFETCH_LEN && open_resp.prefetched_bytes > 0 &&
        open_resp.prefetched_bytes <= OPEN_PREFETCH_LEN) {
        if (wki_roce_region_wait_tagged_write(state->rdma_bulk_rkey, tagged_receive.cookie, open_resp.prefetched_bytes,
                                              VFS_PROXY_OP_TIMEOUT_US)) {
            valid_prefetched_bytes = open_resp.prefetched_bytes;
            remote_vfs_rdma_note_success(state->bulk_rdma_failure_count, state->bulk_rdma_retry_after_us);
            remote_vfs_rdma_note_success(state->rdma_read_failure_count, state->rdma_read_retry_after_us);
        } else {
            remote_vfs_rdma_note_transient_failure(state->bulk_rdma_failure_count, state->bulk_rdma_retry_after_us);
            remote_vfs_rdma_note_transient_failure(state->rdma_read_failure_count, state->rdma_read_retry_after_us);
            ker::mod::dbg::log("[WKI] open prefetch tagged write timeout: node=0x%04x ch=%u fd=%d bytes=%u", state->owner_node,
                               state->assigned_channel, open_resp.fd, open_resp.prefetched_bytes);
        }
    }

    // Allocate File + RemoteFileContext
    auto* file = new (std::nothrow) ker::vfs::File{};
    if (file == nullptr) {
        remote_vfs_close_remote_fd_best_effort(state, open_resp.fd);
        return nullptr;
    }

    auto* ctx = new (std::nothrow) RemoteFileContext{};
    if (ctx == nullptr) {
        delete file;
        remote_vfs_close_remote_fd_best_effort(state, open_resp.fd);
        return nullptr;
    }

    ctx->proxy = state;
    ctx->remote_fd = open_resp.fd;

    const char* path = fs_relative_path;
    while (path != nullptr && *path == '/') {
        path++;
    }
    if (path != nullptr && std::strcmp(path, "wki") == 0) {
        ctx->hide_recursive_wki_entries = true;
        const char* owner_hostname = wki_peer_get_hostname(state->owner_node);
        if (owner_hostname != nullptr) {
            size_t const HOSTNAME_LEN = std::min(std::strlen(owner_hostname), ctx->recursive_wki_hostname.size() - 1);
            std::memcpy(ctx->recursive_wki_hostname.data(), owner_hostname, HOSTNAME_LEN);
            *std::next(ctx->recursive_wki_hostname.begin(), static_cast<ptrdiff_t>(HOSTNAME_LEN)) = '\0';
        }
    }

    file->fd = -1;  // Will be assigned by vfs_alloc_fd
    file->private_data = ctx;
    file->fops = &g_remote_vfs_fops;
    file->pos = 0;
    file->is_directory = (open_resp.is_dir != 0);
    file->fs_type = ker::vfs::FSType::REMOTE;
    file->refcount = 1;
    file->open_flags = flags;
    file->fd_flags = 0;
    file->vfs_path = nullptr;                      // Set by vfs_open after return
    file->dir_fs_count = static_cast<size_t>(-1);  // Unknown FS entry count

    if ((flags & ker::vfs::O_NO_CACHE) == 0 && open_resp_len >= OPEN_RESP_WITH_STAT_LEN && open_resp.has_stat != 0) {
        remote_vfs_store_cached_stat(ctx, open_resp.stat);
    }
    if (valid_prefetched_bytes > 0) {
        auto* prefetch_copy = new (std::nothrow) uint8_t[valid_prefetched_bytes];
        if (prefetch_copy != nullptr) {
            memcpy(prefetch_copy, state->rdma_bulk_buf, valid_prefetched_bytes);
            ctx->open_prefetch_buf = prefetch_copy;
            ctx->open_prefetch_offset = 0;
            ctx->open_prefetch_len = valid_prefetched_bytes;
        }

        state->bulk_owner_fd = open_resp.fd;
        ctx->bulk_cached_offset = 0;
        ctx->bulk_cached_len = valid_prefetched_bytes;
    }

    open_ref_guard.disarm();
    return file;
}

namespace {

auto remote_vfs_stat_on_proxy(ProxyVfsState* state, const char* fs_relative_path, ker::vfs::Stat* statbuf) -> int {
    if (state == nullptr || !state->active || fs_relative_path == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }

    // Build request: {path_len:u16, path[N]}
    std::array<uint8_t, 514> req_stack __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
    size_t const PATH_LEN = strlen(fs_relative_path);
    if (PATH_LEN > req_stack.size() - 2U) {
        return -ENAMETOOLONG;
    }
    auto path_len = static_cast<uint16_t>(PATH_LEN);
    auto req_data_len = static_cast<uint16_t>(2 + path_len);
    uint8_t* req_data = req_stack.data();

    memcpy(req_data, &path_len, sizeof(uint16_t));
    if (path_len > 0) {
        memcpy(req_data + 2, fs_relative_path, path_len);
    }

    // Use a kernel-stack buffer for the proxy response.  The caller's statbuf
    // may be a user-space pointer, but handle_vfs_op_resp can run in netpoll
    // context where user pages are not mapped.
    ker::vfs::Stat kernel_buf{};
    int const STATUS = vfs_proxy_send_and_wait(state, OP_VFS_STAT, req_data, req_data_len, &kernel_buf, sizeof(ker::vfs::Stat));
    if (STATUS == 0) {
        memcpy(statbuf, &kernel_buf, sizeof(ker::vfs::Stat));
    }
    return STATUS;
}

}  // namespace

auto wki_remote_vfs_stat(void* mount_private_data, const char* fs_relative_path, ker::vfs::Stat* statbuf) -> int {
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || fs_relative_path == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) {
        return -EINVAL;
    }
    ProxyLifecycleRefGuard lane_ref_guard(state);
    return remote_vfs_stat_on_proxy(state, fs_relative_path, statbuf);
}

auto wki_remote_vfs_metadata_batch(void* mount_private_data, ker::vfs::MetadataBatchOperation operation, uint32_t mode,
                                   const ker::vfs::MetadataBatchEntry* entries, size_t count, ker::vfs::MetadataBatchResult* results,
                                   bool* mutation_request_attempted) -> int {
    constexpr size_t MAX_REQUEST_DATA = WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload);
    constexpr size_t MAX_RESPONSE_DATA = WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload);
    constexpr size_t STAT_RESULT_SIZE = sizeof(int32_t) + sizeof(ker::vfs::Stat);
    static_assert(VFS_METADATA_BATCH_MAX_STAT_ITEMS == (MAX_RESPONSE_DATA - sizeof(VfsMetadataBatchHeader)) / STAT_RESULT_SIZE);

    if (mutation_request_attempted != nullptr) {
        *mutation_request_attempted = false;
    }

    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || entries == nullptr || results == nullptr || mutation_request_attempted == nullptr || count == 0 ||
        count > VFS_METADATA_BATCH_MAX_ITEMS) {
        return -EINVAL;
    }

    VfsMetadataBatchOperation wire_operation{};
    switch (operation) {
        case ker::vfs::MetadataBatchOperation::INVALID:
            return -EINVAL;
        case ker::vfs::MetadataBatchOperation::CREATE_CLOSE:
            wire_operation = VfsMetadataBatchOperation::CREATE_CLOSE;
            break;
        case ker::vfs::MetadataBatchOperation::STAT_FOLLOW:
            wire_operation = VfsMetadataBatchOperation::STAT_FOLLOW;
            break;
        case ker::vfs::MetadataBatchOperation::UNLINK:
            wire_operation = VfsMetadataBatchOperation::UNLINK;
            break;
        case ker::vfs::MetadataBatchOperation::RENAME:
            wire_operation = VfsMetadataBatchOperation::RENAME;
            break;
        default:
            return -EINVAL;
    }
    if (operation != ker::vfs::MetadataBatchOperation::CREATE_CLOSE && mode != 0) {
        return -EINVAL;
    }

    bool const READ_ONLY = operation == ker::vfs::MetadataBatchOperation::STAT_FOLLOW;
    bool const RENAME = operation == ker::vfs::MetadataBatchOperation::RENAME;
    size_t mutation_request_len = sizeof(VfsMetadataBatchHeader);
    for (size_t index = 0; index < count; ++index) {
        results[index].status = -EINPROGRESS;  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        results[index].stat = {};              // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)

        auto const& entry = entries[index];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        if (entry.path == nullptr || (!RENAME && entry.second_path != nullptr) || (RENAME && entry.second_path == nullptr)) {
            return -EINVAL;
        }
        size_t const PATH_LEN = std::strlen(entry.path);
        if (PATH_LEN > VFS_METADATA_BATCH_MAX_PATH_LEN ||
            !relative_wire_path_has_safe_components(reinterpret_cast<const uint8_t*>(entry.path), PATH_LEN)) {
            return PATH_LEN > VFS_METADATA_BATCH_MAX_PATH_LEN ? -ENAMETOOLONG : -EINVAL;
        }
        size_t item_size = sizeof(uint16_t) + PATH_LEN;
        if (RENAME) {
            size_t const SECOND_PATH_LEN = std::strlen(entry.second_path);
            if (SECOND_PATH_LEN > VFS_METADATA_BATCH_MAX_PATH_LEN ||
                !relative_wire_path_has_safe_components(reinterpret_cast<const uint8_t*>(entry.second_path), SECOND_PATH_LEN)) {
                return SECOND_PATH_LEN > VFS_METADATA_BATCH_MAX_PATH_LEN ? -ENAMETOOLONG : -EINVAL;
            }
            item_size = (sizeof(uint16_t) * 2) + PATH_LEN + SECOND_PATH_LEN;
        }
        if (item_size > MAX_REQUEST_DATA - sizeof(VfsMetadataBatchHeader)) {
            return -EMSGSIZE;
        }
        if (!READ_ONLY) {
            if (item_size > MAX_REQUEST_DATA - mutation_request_len) {
                // The server must preflight the complete mutation set before
                // item zero can have an effect. Let callers scalar-fallback
                // while no request has been attempted.
                return -EOPNOTSUPP;
            }
            mutation_request_len += item_size;
        }
    }

    // EOPNOTSUPP is reserved for this pre-send negotiation check. Once a
    // request is attempted, callers must never scalar-replay a mutation.
    if (!wki_peer_capability_negotiated(anchor->owner_node, WKI_CAP_VFS_METADATA_BATCH)) {
        return -EOPNOTSUPP;
    }

    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) {
        return -EIO;
    }
    ProxyLifecycleRefGuard lane_ref_guard(state);
    if (!state->active) {
        return -EIO;
    }

    std::array<uint8_t, MAX_REQUEST_DATA> request{};
    std::array<uint8_t, MAX_RESPONSE_DATA> response{};
    bool mutation_attempted = false;
    auto finish = [&](int status) -> int {
        if (mutation_attempted) {
            // A malformed/lost response is completion-ambiguous. Conservatively
            // discard proxy readlink state; the attempted flag makes VFS
            // invalidate pathname metadata even when no status was parsed.
            invalidate_readlink_cache_group(state);
        }
        return status;
    };

    size_t first = 0;
    while (first < count) {
        size_t request_len = sizeof(VfsMetadataBatchHeader);
        size_t chunk_count = 0;
        size_t const CHUNK_LIMIT = operation == ker::vfs::MetadataBatchOperation::STAT_FOLLOW
                                       ? std::min(count - first, static_cast<size_t>(VFS_METADATA_BATCH_MAX_STAT_ITEMS))
                                       : std::min(count - first, static_cast<size_t>(VFS_METADATA_BATCH_MAX_ITEMS));

        while (chunk_count < CHUNK_LIMIT) {
            auto const& entry = entries[first + chunk_count];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            size_t const PATH_LEN = std::strlen(entry.path);
            size_t const SECOND_PATH_LEN = RENAME ? std::strlen(entry.second_path) : 0;
            size_t const ITEM_SIZE = RENAME ? (sizeof(uint16_t) * 2) + PATH_LEN + SECOND_PATH_LEN : sizeof(uint16_t) + PATH_LEN;
            if (request_len + ITEM_SIZE > request.size()) {
                break;
            }

            auto const PATH_LEN_WIRE = static_cast<uint16_t>(PATH_LEN);
            if (RENAME) {
                auto const SECOND_PATH_LEN_WIRE = static_cast<uint16_t>(SECOND_PATH_LEN);
                std::memcpy(request.data() + request_len, &PATH_LEN_WIRE, sizeof(PATH_LEN_WIRE));
                std::memcpy(request.data() + request_len + sizeof(PATH_LEN_WIRE), &SECOND_PATH_LEN_WIRE, sizeof(SECOND_PATH_LEN_WIRE));
                request_len += sizeof(PATH_LEN_WIRE) + sizeof(SECOND_PATH_LEN_WIRE);
                if (PATH_LEN > 0) {
                    std::memcpy(request.data() + request_len, entry.path, PATH_LEN);
                    request_len += PATH_LEN;
                }
                if (SECOND_PATH_LEN > 0) {
                    std::memcpy(request.data() + request_len, entry.second_path, SECOND_PATH_LEN);
                    request_len += SECOND_PATH_LEN;
                }
            } else {
                std::memcpy(request.data() + request_len, &PATH_LEN_WIRE, sizeof(PATH_LEN_WIRE));
                request_len += sizeof(PATH_LEN_WIRE);
                if (PATH_LEN > 0) {
                    std::memcpy(request.data() + request_len, entry.path, PATH_LEN);
                    request_len += PATH_LEN;
                }
            }
            ++chunk_count;
        }
        if (chunk_count == 0) {
            return finish(-EMSGSIZE);
        }

        VfsMetadataBatchHeader const REQUEST_HEADER{
            .version = VFS_METADATA_BATCH_VERSION,
            .operation = wire_operation,
            .count = static_cast<uint8_t>(chunk_count),
            .mode = mode,
        };
        std::memcpy(request.data(), &REQUEST_HEADER, sizeof(REQUEST_HEADER));

        size_t const ITEM_RESPONSE_SIZE = operation == ker::vfs::MetadataBatchOperation::STAT_FOLLOW ? STAT_RESULT_SIZE : sizeof(int32_t);
        size_t const EXPECTED_RESPONSE_LEN = sizeof(VfsMetadataBatchHeader) + (chunk_count * ITEM_RESPONSE_SIZE);
        if (EXPECTED_RESPONSE_LEN > response.size()) {
            return finish(-EMSGSIZE);
        }

        uint16_t response_len = 0;
        if (!READ_ONLY) {
            mutation_attempted = true;
            *mutation_request_attempted = true;
        }
        int const STATUS = vfs_proxy_send_and_wait(state, OP_VFS_METADATA_BATCH, request.data(), request_len, response.data(),
                                                   static_cast<uint16_t>(response.size()), &response_len);
        if (STATUS < 0) {
            return finish(STATUS == -EOPNOTSUPP ? -EPROTO : STATUS);
        }
        if (response_len != EXPECTED_RESPONSE_LEN) {
            return finish(-EPROTO);
        }

        VfsMetadataBatchHeader response_header{};
        std::memcpy(&response_header, response.data(), sizeof(response_header));
        if (response_header.version != REQUEST_HEADER.version || response_header.operation != REQUEST_HEADER.operation ||
            response_header.count != REQUEST_HEADER.count || response_header.mode != REQUEST_HEADER.mode) {
            return finish(-EPROTO);
        }

        size_t response_pos = sizeof(VfsMetadataBatchHeader);
        for (size_t item = 0; item < chunk_count; ++item) {
            auto& result = results[first + item];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            std::memcpy(&result.status, response.data() + response_pos, sizeof(result.status));
            response_pos += sizeof(result.status);
            if (operation == ker::vfs::MetadataBatchOperation::STAT_FOLLOW) {
                std::memcpy(&result.stat, response.data() + response_pos, sizeof(result.stat));
                response_pos += sizeof(result.stat);
            }
        }
        first += chunk_count;
    }

    return finish(0);
}

auto wki_remote_vfs_fstat(ker::vfs::File* file, ker::vfs::Stat* statbuf) -> int {
    if (file == nullptr || file->private_data == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }

    auto* ctx = static_cast<RemoteFileContext*>(file->private_data);
    if (ctx->proxy == nullptr || !ctx->proxy->active) {
        return -EIO;
    }
    ker::mod::sys::MutexGuard io_guard(ctx->io_lock);
    consume_remote_file_cache_invalidation(ctx);

    ker::vfs::MountRef mount_ref = (file->vfs_path != nullptr) ? ker::vfs::find_mount_point(file->vfs_path) : ker::vfs::MountRef{};
    auto* mount = mount_ref.get();
    if (mount == nullptr || mount->fs_type != ker::vfs::FSType::REMOTE) {
        return -ENOENT;
    }

    if (ker::vfs::vfs_cache_notify_file_dirty(file)) {
        invalidate_remote_file_open_caches(ctx);
        ker::vfs::vfs_cache_notify_acknowledge_file(file);
    }

    bool const USE_STAT_CACHE = (file->open_flags & ker::vfs::O_NO_CACHE) == 0;
    if (USE_STAT_CACHE && remote_vfs_try_copy_cached_stat(ctx, statbuf)) {
        statbuf->st_dev = mount->dev_id;
        return 0;
    }

    ker::vfs::Stat fresh = {};
    int const STATUS = remote_vfs_stat_on_proxy(ctx->proxy, remote_vfs_strip_mount_prefix(mount, file->vfs_path), &fresh);
    if (STATUS == 0) {
        fresh.st_dev = mount->dev_id;
        if (USE_STAT_CACHE) {
            remote_vfs_store_cached_stat(ctx, fresh);
        }
        *statbuf = fresh;
    }
    return STATUS;
}

auto wki_remote_vfs_mkdir(void* mount_private_data, const char* fs_relative_path, int mode) -> int {
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || fs_relative_path == nullptr) {
        return -EINVAL;
    }
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) {
        return -EINVAL;
    }
    ProxyLifecycleRefGuard lane_ref_guard(state);
    if (!state->active) {
        return -EINVAL;
    }

    // Build request: {mode:u32, path_len:u16, path[N]}
    std::array<uint8_t, 518> req_stack __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
    size_t const PATH_LEN = strlen(fs_relative_path);
    if (PATH_LEN > req_stack.size() - 6U) {
        return -ENAMETOOLONG;
    }
    auto path_len = static_cast<uint16_t>(PATH_LEN);
    auto req_data_len = static_cast<uint16_t>(6 + path_len);
    uint8_t* req_data = req_stack.data();

    auto u_mode = static_cast<uint32_t>(mode);
    memcpy(req_data, &u_mode, sizeof(uint32_t));
    memcpy(req_data + 4, &path_len, sizeof(uint16_t));
    if (path_len > 0) {
        memcpy(req_data + 6, fs_relative_path, path_len);
    }

    int const STATUS = vfs_proxy_send_and_wait(state, OP_VFS_MKDIR, req_data, req_data_len, nullptr, 0);
    if (STATUS == 0) {
        invalidate_readlink_cache_group(state);
    }
    return STATUS;
}

auto wki_remote_vfs_chmod(void* mount_private_data, const char* fs_relative_path, int mode, bool follow_final_symlink) -> int {
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || fs_relative_path == nullptr) {
        return -EINVAL;
    }
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) {
        return -EINVAL;
    }
    ProxyLifecycleRefGuard lane_ref_guard(state);
    if (!state->active) {
        return -EINVAL;
    }

    // Build request: {mode:u32, flags:u8, path_len:u16, path[N]}
    std::array<uint8_t, 519> req_stack __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
    size_t const PATH_LEN = strlen(fs_relative_path);
    if (PATH_LEN > req_stack.size() - 7U) {
        return -ENAMETOOLONG;
    }
    auto path_len = static_cast<uint16_t>(PATH_LEN);
    auto req_data_len = static_cast<uint16_t>(7 + path_len);
    uint8_t* req_data = req_stack.data();

    auto u_mode = static_cast<uint32_t>(mode);
    uint8_t const FLAGS = follow_final_symlink ? WKI_VFS_CHMOD_FLAG_FOLLOW_FINAL_SYMLINK : 0;
    memcpy(req_data, &u_mode, sizeof(uint32_t));
    memcpy(req_data + 4, &FLAGS, sizeof(uint8_t));
    memcpy(req_data + 5, &path_len, sizeof(uint16_t));
    if (path_len > 0) {
        memcpy(req_data + 7, fs_relative_path, path_len);
    }

    return vfs_proxy_send_and_wait(state, OP_VFS_CHMOD, req_data, req_data_len, nullptr, 0);
}

auto wki_remote_vfs_utimens(void* mount_private_data, const char* fs_relative_path, const ker::vfs::Timespec* times,
                            bool follow_final_symlink) -> int {
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || fs_relative_path == nullptr) {
        return -EINVAL;
    }
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) {
        return -EINVAL;
    }
    ProxyLifecycleRefGuard lane_ref_guard(state);
    if (!state->active) {
        return -EINVAL;
    }

    std::array<uint8_t, sizeof(VfsUtimensReqPrefix) + 512> req_stack{};
    size_t const PATH_LEN = strlen(fs_relative_path);
    if (PATH_LEN > req_stack.size() - sizeof(VfsUtimensReqPrefix)) {
        return -ENAMETOOLONG;
    }

    VfsUtimensReqPrefix prefix{};
    prefix.path_len = static_cast<uint16_t>(PATH_LEN);
    prefix.flags = follow_final_symlink ? WKI_VFS_UTIMENS_FLAG_FOLLOW_FINAL_SYMLINK : 0;
    if (times != nullptr) {
        prefix.atime_sec = times[0].tv_sec;
        prefix.atime_nsec = times[0].tv_nsec;
        prefix.mtime_sec = times[1].tv_sec;
        prefix.mtime_nsec = times[1].tv_nsec;
        prefix.flags |= WKI_VFS_UTIMENS_FLAG_TIMES_PRESENT;
    }

    auto const REQ_DATA_LEN = static_cast<uint16_t>(sizeof(prefix) + PATH_LEN);
    memcpy(req_stack.data(), &prefix, sizeof(prefix));
    if (PATH_LEN > 0) {
        memcpy(req_stack.data() + sizeof(prefix), fs_relative_path, PATH_LEN);
    }
    return vfs_proxy_send_and_wait(state, OP_VFS_UTIMENS, req_stack.data(), REQ_DATA_LEN, nullptr, 0);
}

// Consumer side: create a symlink on the remote server
auto wki_remote_vfs_symlink(void* mount_private_data, const char* target, const char* fs_relative_path) -> int {
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || target == nullptr || fs_relative_path == nullptr) {
        return -EINVAL;
    }
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) {
        return -EINVAL;
    }
    ProxyLifecycleRefGuard lane_ref_guard(state);
    if (!state->active) {
        return -EINVAL;
    }

    // Server expects: {target_len:u16, target[], link_len:u16, link[]}
    std::array<uint8_t, 1028> req_stack __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
    constexpr size_t SERVER_TARGET_MAX = 511;
    size_t const TARGET_LEN = strlen(target);
    size_t const LINK_LEN = strlen(fs_relative_path);
    if (TARGET_LEN > SERVER_TARGET_MAX || LINK_LEN > UINT16_MAX || TARGET_LEN + LINK_LEN > req_stack.size() - 4U) {
        return -ENAMETOOLONG;
    }

    auto target_len = static_cast<uint16_t>(TARGET_LEN);
    auto link_len = static_cast<uint16_t>(LINK_LEN);
    auto req_data_len = static_cast<uint16_t>(4 + target_len + link_len);
    uint8_t* req_data = req_stack.data();

    memcpy(req_data, &target_len, sizeof(uint16_t));
    if (target_len > 0) {
        memcpy(req_data + 2, target, target_len);
    }
    memcpy(req_data + 2 + target_len, &link_len, sizeof(uint16_t));
    if (link_len > 0) {
        memcpy(req_data + 4 + target_len, fs_relative_path, link_len);
    }

    int const STATUS = vfs_proxy_send_and_wait(state, OP_VFS_SYMLINK, req_data, req_data_len, nullptr, 0);
    if (STATUS == 0) {
        invalidate_readlink_cache_group(state);
    }
    return STATUS;
}

// Consumer side: unlink a file on the remote server
auto wki_remote_vfs_unlink(void* mount_private_data, const char* fs_relative_path) -> int {
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || fs_relative_path == nullptr) {
        return -EINVAL;
    }
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) {
        return -EINVAL;
    }
    ProxyLifecycleRefGuard lane_ref_guard(state);
    if (!state->active) {
        return -EINVAL;
    }

    std::array<uint8_t, 514> req_stack __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
    size_t const PATH_LEN = strlen(fs_relative_path);
    if (PATH_LEN > req_stack.size() - 2U) {
        return -ENAMETOOLONG;
    }
    auto path_len = static_cast<uint16_t>(PATH_LEN);
    auto req_data_len = static_cast<uint16_t>(2 + path_len);
    uint8_t* req_data = req_stack.data();

    memcpy(req_data, &path_len, sizeof(uint16_t));
    if (path_len > 0) {
        memcpy(req_data + 2, fs_relative_path, path_len);
    }

    int const STATUS = vfs_proxy_send_and_wait(state, OP_VFS_UNLINK, req_data, req_data_len, nullptr, 0);
    if (STATUS == 0) {
        invalidate_readlink_cache_group(state);
    }
    return STATUS;
}

// Consumer side: remove a directory on the remote server
auto wki_remote_vfs_rmdir(void* mount_private_data, const char* fs_relative_path) -> int {
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || fs_relative_path == nullptr) {
        return -EINVAL;
    }
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) {
        return -EINVAL;
    }
    ProxyLifecycleRefGuard lane_ref_guard(state);
    if (!state->active) {
        return -EINVAL;
    }

    std::array<uint8_t, 514> req_stack __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
    size_t const PATH_LEN = strlen(fs_relative_path);
    if (PATH_LEN > req_stack.size() - 2U) {
        return -ENAMETOOLONG;
    }
    auto path_len = static_cast<uint16_t>(PATH_LEN);
    auto req_data_len = static_cast<uint16_t>(2 + path_len);
    uint8_t* req_data = req_stack.data();

    memcpy(req_data, &path_len, sizeof(uint16_t));
    if (path_len > 0) {
        memcpy(req_data + 2, fs_relative_path, path_len);
    }

    int const STATUS = vfs_proxy_send_and_wait(state, OP_VFS_RMDIR, req_data, req_data_len, nullptr, 0);
    if (STATUS == 0) {
        invalidate_readlink_cache_group(state);
    }
    return STATUS;
}

// Consumer side: rename a file/directory on the remote server
auto wki_remote_vfs_rename(void* mount_private_data, const char* old_fs_path, const char* new_fs_path) -> int {
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || old_fs_path == nullptr || new_fs_path == nullptr) {
        return -EINVAL;
    }
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) {
        return -EINVAL;
    }
    ProxyLifecycleRefGuard lane_ref_guard(state);
    if (!state->active) {
        return -EINVAL;
    }

    std::array<uint8_t, 1028> req_stack __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
    size_t const OLD_LEN = strlen(old_fs_path);
    size_t const NEW_LEN = strlen(new_fs_path);
    if (OLD_LEN > UINT16_MAX || NEW_LEN > UINT16_MAX || OLD_LEN + NEW_LEN > req_stack.size() - 4U) {
        return -ENAMETOOLONG;
    }
    auto old_len = static_cast<uint16_t>(OLD_LEN);
    auto new_len = static_cast<uint16_t>(NEW_LEN);
    auto req_data_len = static_cast<uint16_t>(4 + old_len + new_len);
    uint8_t* req_data = req_stack.data();

    memcpy(req_data, &old_len, sizeof(uint16_t));
    if (old_len > 0) {
        memcpy(req_data + 2, old_fs_path, old_len);
    }
    memcpy(req_data + 2 + old_len, &new_len, sizeof(uint16_t));
    if (new_len > 0) {
        memcpy(req_data + 4 + old_len, new_fs_path, new_len);
    }

    int const STATUS = vfs_proxy_send_and_wait(state, OP_VFS_RENAME, req_data, req_data_len, nullptr, 0);
    if (STATUS == 0) {
        invalidate_readlink_cache_group(state);
    }
    return STATUS;
}

// Consumer side: readlink on a remote path (for vfs_readlink / resolve_symlinks)
auto wki_remote_vfs_readlink_path(void* mount_private_data, const char* fs_relative_path, char* buf, size_t bufsize) -> ssize_t {
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || fs_relative_path == nullptr || buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) {
        return -EINVAL;
    }
    ProxyLifecycleRefGuard lane_ref_guard(state);
    if (!state->active) {
        return -EINVAL;
    }

    // The root of a mounted remote export is the mount point itself, not a
    // symlink inside the export.
    if (fs_relative_path[0] == '\0') {
        return -EINVAL;
    }

    if (state->readlink_unsupported.load(std::memory_order_acquire)) {
        return -ENOSYS;
    }

    ssize_t cached_result = 0;
    uint32_t cache_generation = 0;
    if (try_readlink_cache_lookup(state, fs_relative_path, buf, bufsize, &cached_result, &cache_generation)) {
        return cached_result;
    }

    // Build request: {path_len:u16, path[N]}
    std::array<uint8_t, 514> req_stack __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
    size_t const PATH_LEN = strlen(fs_relative_path);
    if (PATH_LEN > req_stack.size() - 2U) {
        return -ENAMETOOLONG;
    }
    auto path_len = static_cast<uint16_t>(PATH_LEN);
    auto req_data_len = static_cast<uint16_t>(2 + path_len);
    uint8_t* req_data = req_stack.data();

    memcpy(req_data, &path_len, sizeof(uint16_t));
    if (path_len > 0) {
        memcpy(req_data + 2, fs_relative_path, path_len);
    }

    // Response: {target_len:u16, target[]}
    std::array<uint8_t, 514> resp_buf __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
    uint16_t resp_len = 0;
    int const STATUS = vfs_proxy_read_with_retry(state, OP_VFS_READLINK, req_data, req_data_len, resp_buf.data(),
                                                 static_cast<uint16_t>(resp_buf.size()), &resp_len, fs_relative_path);
    if (STATUS != 0) {
        if (STATUS == -ENOSYS) {
            state->readlink_unsupported.store(true, std::memory_order_release);
            return STATUS;
        }

        if (STATUS == -EINVAL || STATUS == -ENOENT) {
            cache_readlink_result(state, cache_generation, fs_relative_path, STATUS, nullptr, 0);
            return STATUS;
        }

        ker::mod::dbg::log("[WKI] remote_vfs_readlink_path failed: node=0x%04x ch=%u path='%s' status=%d", state->owner_node,
                           state->assigned_channel, fs_relative_path, STATUS);
        return STATUS;
    }

    if (resp_len < 2) {
        return -EIO;
    }

    uint16_t target_len = 0;
    memcpy(&target_len, resp_buf.data(), sizeof(uint16_t));
    if (target_len == 0 || target_len + 2 > resp_len) {
        return -EIO;
    }

    auto to_copy = static_cast<size_t>(target_len);
    to_copy = std::min(to_copy, bufsize);
    memcpy(buf, resp_buf.data() + 2, to_copy);
    cache_readlink_result(state, cache_generation, fs_relative_path, 0, reinterpret_cast<const char*>(resp_buf.data() + 2), target_len);
    return static_cast<ssize_t>(to_copy);
}

auto wki_remote_vfs_get_fops() -> ker::vfs::FileOperations* { return &g_remote_vfs_fops; }

void wki_remote_vfs_invalidate_open_file_caches(ker::vfs::File* file) {
    if (file == nullptr || file->private_data == nullptr || file->fs_type != ker::vfs::FSType::REMOTE) {
        return;
    }

    auto* ctx = static_cast<RemoteFileContext*>(file->private_data);
    // This callback is reachable from inline WKI RX while a writer may hold
    // io_lock and wait for the response that the same RX path must deliver.
    // Defer cache storage mutation to the next task-context file operation.
    ctx->cache_invalidation_pending.store(true, std::memory_order_release);
}

void wki_remote_vfs_notify_path_changed(const char* old_local_vfs_path, const char* new_local_vfs_path) {
    auto send_notify = [&](const VfsExport& export_entry, const char* old_rel_path, const char* new_rel_path) {
        uint16_t const OLD_LEN = static_cast<uint16_t>(std::min<std::size_t>(std::strlen(old_rel_path), UINT16_MAX));
        uint16_t const NEW_LEN = static_cast<uint16_t>(std::min<std::size_t>(std::strlen(new_rel_path), UINT16_MAX));

        std::array<uint8_t, sizeof(DevOpReqPayload) + 4 + (2 * VFS_EXPORT_PATH_LEN)> req{};
        auto* req_hdr = reinterpret_cast<DevOpReqPayload*>(req.data());
        req_hdr->op_id = OP_VFS_INVALIDATE;
        req_hdr->data_len = static_cast<uint16_t>(4 + OLD_LEN + NEW_LEN);
        uint8_t* data = req.data() + sizeof(DevOpReqPayload);
        std::memcpy(data, &OLD_LEN, sizeof(uint16_t));
        std::memcpy(data + 2, &NEW_LEN, sizeof(uint16_t));
        if (OLD_LEN > 0) {
            std::memcpy(data + 4, old_rel_path, OLD_LEN);
        }
        if (NEW_LEN > 0) {
            std::memcpy(data + 4 + OLD_LEN, new_rel_path, NEW_LEN);
        }

        wki_dev_server_send_vfs_notify(export_entry.resource_id, OP_VFS_INVALIDATE, req.data() + sizeof(DevOpReqPayload),
                                       req_hdr->data_len);
    };

    // Visit each export once. A same-export rename carries both paths in one
    // notification; a cross-export rename naturally emits one old-only and
    // one new-only notification for the two distinct exports.
    for (const auto& exp : g_vfs_exports) {
        if (!exp.active) {
            continue;
        }

        const char* const EXPORT_PATH = raw_data(exp.export_path);
        const char* const OLD_REL_SRC = trim_export_prefix(EXPORT_PATH, old_local_vfs_path);
        const char* const NEW_REL_SRC = trim_export_prefix(EXPORT_PATH, new_local_vfs_path);
        if (OLD_REL_SRC == nullptr && NEW_REL_SRC == nullptr) {
            continue;
        }

        std::array<char, VFS_EXPORT_PATH_LEN> old_rel{};
        std::array<char, VFS_EXPORT_PATH_LEN> new_rel{};
        if (OLD_REL_SRC != nullptr) {
            std::snprintf(old_rel.data(), old_rel.size(), "%s", OLD_REL_SRC);
        }
        if (NEW_REL_SRC != nullptr) {
            std::snprintf(new_rel.data(), new_rel.size(), "%s", NEW_REL_SRC);
        }
        send_notify(exp, old_rel.data(), new_rel.data());
    }
}

// -------------------------------------------------------------------------------
// Consumer Side - RX Handlers
// -------------------------------------------------------------------------------

namespace detail {

void handle_vfs_invalidate_notify(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len,
                                  const WkiChannelIdentity& channel_identity) {
    if (!vfs_channel_identity_matches_header(hdr, channel_identity) || payload == nullptr || payload_len < 4) {
        return;
    }

    uint16_t old_len = 0;
    uint16_t new_len = 0;
    std::memcpy(&old_len, payload, sizeof(uint16_t));
    std::memcpy(&new_len, payload + 2, sizeof(uint16_t));

    size_t const needed = 4ULL + old_len + new_len;
    if (needed > payload_len) {
        return;
    }

    struct RetainedGroup {
        std::array<ProxyVfsState*, VFS_PROXY_LANE_COUNT> members{};
        size_t count = 0;
        std::array<char, VFS_EXPORT_PATH_LEN> local_mount_path{};
    } group;

    s_vfs_lock.lock();
    ProxyVfsState* state = find_vfs_proxy_by_channel(channel_identity);
    ProxyVfsState* anchor = state;
    if (state != nullptr && !state->lane_anchor) {
        anchor = nullptr;
        for (auto& proxy : g_vfs_proxies) {
            if (proxy != nullptr && proxy->lane_anchor && proxy->mount_group_id == state->mount_group_id) {
                anchor = proxy.get();
                break;
            }
        }
    }
    if (state != nullptr) {
        group.local_mount_path = state->local_mount_path;
        if (anchor != nullptr && anchor->lane_anchor) {
            for (auto* member : anchor->lanes) {
                if (member == nullptr || group.count >= group.members.size()) {
                    continue;
                }
                bool already_retained = false;
                for (size_t retained_index = 0; retained_index < group.count; ++retained_index) {
                    if (group.members.at(retained_index) == member) {
                        already_retained = true;
                        break;
                    }
                }
                if (already_retained) {
                    continue;
                }
                member->lifecycle_refs++;
                group.members.at(group.count++) = member;
                invalidate_all_dir_caches(member);
            }
        }
        if (group.count == 0) {
            state->lifecycle_refs++;
            group.members.at(group.count++) = state;
            invalidate_all_dir_caches(state);
        }
    }
    s_vfs_lock.unlock();
    if (group.count == 0) {
        return;
    }

    auto invalidate_path = [&](const char* rel_path, uint16_t rel_len) {
        std::array<char, 512> full_path __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
        build_full_path(full_path.data(), full_path.size(), group.local_mount_path.data(), rel_path, rel_len);
        ker::vfs::vfs_cache_notify_invalidate_path(full_path.data());
    };

    const char* old_rel = reinterpret_cast<const char*>(payload + 4);
    const char* new_rel = old_rel + old_len;
    if (old_len > 0) {
        invalidate_path(old_rel, old_len);
    }
    if (new_len > 0) {
        invalidate_path(new_rel, new_len);
    }

    // The owner sends one logical notification per mount anchor even though
    // the mount has several RPC lanes. Clear every lane-local cache before
    // any retained member can be selected for the next operation.
    invalidate_readlink_cache_group(group.members.at(0));
    for (size_t member_index = 0; member_index < group.count; ++member_index) {
        ker::vfs::vfs_stream_cache_invalidate_remote_scope(group.members.at(member_index));
    }
    for (size_t member_index = 0; member_index < group.count; ++member_index) {
        release_vfs_proxy_lifecycle_ref(group.members.at(member_index));
    }
}

void handle_vfs_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevAttachAckPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const DevAttachAckPayload*>(payload);

    s_vfs_lock.lock();
    ProxyVfsState* state = find_vfs_proxy_by_attach(hdr->src_node, ack->resource_id, ack->reserved);
    if (state == nullptr) {
        s_vfs_lock.unlock();
        return;
    }
    if (!vfs_attach_ack_matches_pending_locked(state, *ack, payload, payload_len)) {
        s_vfs_lock.unlock();
        return;
    }

    WkiWaitEntry* wait_entry = nullptr;
    wait_entry = claim_and_clear_waiter_locked(state->attach_wait_entry);
    if (wait_entry != nullptr) {
        state->attach_status = ack->status;
        state->attach_channel = ack->assigned_channel;
        state->attach_max_op_size = ack->max_op_size;
        if (ack->status == static_cast<uint8_t>(DevAttachStatus::OK)) {
            state->binding_incarnation = state->attach_expected_incarnation;
        }

        // Store server write-recv rkey if VFS RDMA is available
        if ((ack->rdma_flags & DEV_ATTACH_RDMA_VFS) != 0 && ack->blk_zone_id != 0) {
            state->rdma_server_write_rkey = ack->blk_zone_id;
        }
        // Store server-side read/bulk staging rkeys for RoCE pull mode
        if ((ack->rdma_flags & DEV_ATTACH_RDMA_VFS_READ) != 0 && ack->rdma_read_staging_rkey != 0) {
            state->rdma_server_read_staging_rkey = ack->rdma_read_staging_rkey;
        }
        if ((ack->rdma_flags & DEV_ATTACH_RDMA_BULK_PULL) != 0 && ack->rdma_bulk_staging_rkey != 0) {
            state->rdma_server_bulk_staging_rkey = ack->rdma_bulk_staging_rkey;
        }
    }
    state->attach_expected_cookie = 0;
    state->attach_pending.store(false, std::memory_order_release);
    s_vfs_lock.unlock();

    finish_claimed_waiter(wait_entry, 0);
}

}  // namespace detail

auto wki_remote_vfs_selftest_attach_ack_cookie_fences_stale_completion() -> bool {
    ProxyVfsState state = {};
    DevAttachAckPayload ack = {};
    state.attach_pending.store(true, std::memory_order_release);
    state.attach_expected_cookie = 0x52;

    ack.reserved = 0x51;
    if (vfs_attach_ack_matches_pending_locked(&state, ack, reinterpret_cast<const uint8_t*>(&ack), sizeof(ack))) {
        return false;
    }

    ack.reserved = 0x52;
    if (!vfs_attach_ack_matches_pending_locked(&state, ack, reinterpret_cast<const uint8_t*>(&ack), sizeof(ack))) {
        return false;
    }

    state.attach_expected_cookie = 0;
    if (vfs_attach_ack_matches_pending_locked(&state, ack, reinterpret_cast<const uint8_t*>(&ack), sizeof(ack))) {
        return false;
    }

    state.attach_expected_cookie = 0x52;
    state.attach_pending.store(false, std::memory_order_release);
    return !vfs_attach_ack_matches_pending_locked(&state, ack, reinterpret_cast<const uint8_t*>(&ack), sizeof(ack));
}

namespace detail {

void handle_vfs_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& channel_identity) {
    if (!vfs_channel_identity_matches_header(hdr, channel_identity) || payload_len < sizeof(DevOpRespPayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const DevOpRespPayload*>(payload);
    const uint8_t* resp_data = payload + sizeof(DevOpRespPayload);
    uint16_t const RESP_DATA_LEN = resp->data_len;

    if (sizeof(DevOpRespPayload) + RESP_DATA_LEN > payload_len) {
        return;
    }

    // Match the response against the exact pending proxy on this channel.
    // Channel IDs can be reused, and under failure/recovery we can transiently
    // have more than one active proxy referencing the same channel.
    s_vfs_lock.lock();
    VfsProxyResponseLookup const LOOKUP = find_vfs_proxy_for_response_locked(channel_identity, resp->op_id, resp->reserved);
    ProxyVfsState* state = LOOKUP.matched_state;
    if (state == nullptr) {
        if (LOOKUP.saw_pending_candidate && resp->op_id != OP_VFS_CLOSE) {
            ker::mod::dbg::log(
                "[WKI] Ignoring stale VFS response: node=0x%04x ch=%u resp_op=%u expected_op=%u resp_seq=%u expected_seq=%u "
                "active_candidates=%llu pending_candidates=%llu",
                hdr->src_node, hdr->channel_id, resp->op_id, LOOKUP.stale_expected_op, resp->reserved, LOOKUP.stale_expected_seq,
                static_cast<unsigned long long>(LOOKUP.active_candidates), static_cast<unsigned long long>(LOOKUP.pending_candidates));
        }
        s_vfs_lock.unlock();
        return;
    }

    WkiWaitEntry* wait_entry = nullptr;
    wait_entry = claim_response_waiter_locked(state->op_wait_entry);
    if (wait_entry != nullptr) {
        uint16_t copy_len = 0;
        if (RESP_DATA_LEN > 0 && state->op_resp_buf != nullptr) {
            copy_len = (RESP_DATA_LEN > state->op_resp_max) ? state->op_resp_max : RESP_DATA_LEN;
            memcpy(state->op_resp_buf, resp_data, copy_len);
            state->op_resp_len = copy_len;
#ifdef WKI_DEBUG
            // Diagnostic: check if we received zeros from server
            if (resp->op_id == OP_VFS_READ && copy_len >= 4) {
                auto* d = static_cast<const uint8_t*>(state->op_resp_buf);
                auto* s = resp_data;
                ker::mod::dbg::log(
                    "[WKI-CLI] op_resp: ch=%u op=%u len=%u"
                    " src=[%02x %02x %02x %02x] dst=[%02x %02x %02x %02x]",
                    hdr->channel_id, resp->op_id, copy_len, s[0], s[1], s[2], s[3], d[0], d[1], d[2], d[3]);
            }
#endif
        } else {
            state->op_resp_len = 0;
        }
        state->op_status = resp->status;
    }
    // Keep both the operation slot and exact stack-waiter identity published
    // until the result consumer, cancellation, teardown, or task-exit cleanup
    // clears this generation. Those paths quiesce an RX claimant before the
    // owning stack can be reclaimed.
    state->lock.unlock();
    s_vfs_lock.unlock();

    finish_claimed_waiter(wait_entry, 0);
}

}  // namespace detail

// -------------------------------------------------------------------------------
// D10: Stale Remote FD Garbage Collection
// -------------------------------------------------------------------------------

constexpr uint64_t STALE_FD_TIMEOUT_US = 30000000;  // 30 seconds

void wki_remote_vfs_gc_stale_fds() {
    uint64_t const NOW = wki_now_us();
    std::array<uint16_t, WKI_MAX_PEERS> stale_peers{};
    size_t stale_peer_count = 0;

    s_vfs_lock.lock();
    for (const auto& rfd : g_remote_fds) {
        if (!rfd.active || NOW < rfd.last_activity_us || NOW - rfd.last_activity_us < STALE_FD_TIMEOUT_US) {
            continue;
        }

        WkiPeer const* peer = wki_peer_find(rfd.consumer_node);
        // RemoteVfsFd rows are created only while reliable DEV_OP admission has
        // a CONNECTED peer. Peer slots live in g_wki.peers and are never
        // invalidated after initialization, so that peer row outlives every
        // server FD. Do not perform an unguarded File close if this invariant
        // is ever violated.
        if (peer == nullptr || peer->state == PeerState::CONNECTED) {
            continue;
        }

        bool duplicate = false;
        for (size_t i = 0; i < stale_peer_count; ++i) {
            duplicate = duplicate || stale_peers.at(i) == rfd.consumer_node;
        }
        if (!duplicate && stale_peer_count < stale_peers.size()) {
            stale_peers.at(stale_peer_count++) = rfd.consumer_node;
        }
    }
    s_vfs_lock.unlock();

    for (size_t i = 0; i < stale_peer_count; ++i) {
        uint16_t const NODE_ID = stale_peers.at(i);
        WkiPeer* peer = wki_peer_find(NODE_ID);
        if (!wki_peer_lifecycle_acquire(peer)) {
            continue;
        }
        if (peer->state == PeerState::CONNECTED) {
            wki_peer_lifecycle_release(peer);
            continue;
        }

        // A deferred VFS op holds its DevServerBinding ref across every use of
        // RemoteVfsFd::file. Drain those refs before transferring any File*
        // out of the registry; peer lifecycle prevents a reconnect/reattach
        // from crossing this cleanup.
        wki_dev_server_detach_all_for_peer(NODE_ID);

        bool any_removed = false;
        std::deque<ker::vfs::File*> files_to_close;
        uint64_t const CHECK_NOW = wki_now_us();
        s_vfs_lock.lock();
        for (auto& rfd : g_remote_fds) {
            if (!rfd.active || rfd.consumer_node != NODE_ID || CHECK_NOW < rfd.last_activity_us ||
                CHECK_NOW - rfd.last_activity_us < STALE_FD_TIMEOUT_US) {
                continue;
            }
            if (rfd.file != nullptr) {
                files_to_close.push_back(rfd.file);
                rfd.file = nullptr;
            }
            rfd.retiring = true;
            rfd.active = false;
            any_removed = true;
            ker::mod::dbg::log("[WKI] GC stale remote FD %d (consumer 0x%04x)", rfd.fd_id, rfd.consumer_node);
        }
        if (any_removed) {
            std::erase_if(g_remote_fds, [NODE_ID](const RemoteVfsFd& rfd) {
                return rfd.consumer_node == NODE_ID && rfd.retiring && rfd.file == nullptr;
            });
        }
        s_vfs_lock.unlock();

        for (auto* file : files_to_close) {
            if (file != nullptr) {
                if (file->fops != nullptr && file->fops->vfs_close != nullptr) {
                    file->fops->vfs_close(file);
                }
                delete file;
            }
        }
        wki_peer_lifecycle_release(peer);
    }
}

// -------------------------------------------------------------------------------
// D9: Auto-Discover Exportable Mount Points
// -------------------------------------------------------------------------------

namespace {

void wki_remote_vfs_auto_discover_internal(std::deque<VfsExport>* stale_exports) {
    size_t const MOUNT_COUNT = ker::vfs::get_mount_count();
    for (size_t i = 0; i < MOUNT_COUNT; i++) {
        ker::vfs::MountSnapshot mount_snapshot{};
        if (!ker::vfs::get_mount_snapshot_at(i, &mount_snapshot) || mount_snapshot.path[0] == '\0') {
            continue;
        }

        // Skip REMOTE mounts (don't re-export remote filesystems)
        // Skip DEVFS mounts (not meaningful to export)
        // Skip PROCFS mounts (remote /proc shows server processes, not client's)
        if (mount_snapshot.fs_type == ker::vfs::FSType::REMOTE || mount_snapshot.fs_type == ker::vfs::FSType::DEVFS ||
            mount_snapshot.fs_type == ker::vfs::FSType::PROCFS) {
            continue;
        }

        const char* mount_path = static_cast<const char*>(mount_snapshot.path);
        std::array<char, VFS_EXPORT_NAME_LEN> export_name{};
        build_export_name(export_name.data(), export_name.size(), mount_path);
        ExportBackingIdentity const BACKING = {
            .dev_id = mount_snapshot.dev_id,
            .fs_type = mount_snapshot.fs_type,
        };

        // Check if this visible export name is already exported.
        bool already_exported = false;
        s_vfs_lock.lock();
        for (const auto& exp : g_vfs_exports) {
            if (exp.active && strncmp(raw_data(exp.name), export_name.data(), export_name.size()) == 0) {
                already_exported = true;
                break;
            }
        }
        s_vfs_lock.unlock();
        if (already_exported) {
            continue;
        }

        // Export and advertise the visible path in the caller's current root
        // namespace. After pivot_root("/rootfs", ...), this yields "/",
        // "/boot", and "/oldroot", which resolve correctly for all tasks whose
        // root has been updated to "/rootfs".
        PreservedExportIdentity const PRESERVED_IDENTITY =
            take_preserved_export_identity(stale_exports, export_name.data(), mount_path, BACKING);
        static_cast<void>(wki_remote_vfs_export_add_internal(mount_path, export_name.data(), PRESERVED_IDENTITY, BACKING));
    }
}

}  // namespace

void wki_remote_vfs_auto_discover() {
    if (!g_remote_vfs_initialized) {
        return;
    }

    wki_remote_vfs_auto_discover_internal(nullptr);

    wki_remote_vfs_advertise_exports();
}

void wki_remote_vfs_refresh_exports() {
    if (!g_remote_vfs_initialized) {
        return;
    }

    wki_remote_vfs_auto_discover_internal(nullptr);
}

namespace {

void reconcile_and_publish_vfs_exports() {
    s_vfs_lock.lock();
    uint64_t const TARGET_REVISION = g_vfs_export_target_revision;
    g_vfs_export_rebuild_accepting_entries = false;
    size_t const EXPORT_COUNT = g_vfs_exports.size();
    s_vfs_lock.unlock();

    // The prepared transition rejects every further table mutation once
    // accepting_entries is cleared. Copy one stable row under the VFS lock,
    // then reconcile it after unlocking so the dev-server lock is never
    // nested under s_vfs_lock.
    for (size_t index = 0; index < EXPORT_COUNT; ++index) {
        s_vfs_lock.lock();
        VfsExport const EXP = *std::next(g_vfs_exports.begin(), static_cast<ptrdiff_t>(index));
        s_vfs_lock.unlock();
        if (!EXP.active) {
            continue;
        }
        ResourceIncarnationToken const TOKEN = {
            .owner_boot_epoch = g_wki.local_boot_epoch,
            .resource_incarnation = EXP.resource_incarnation,
        };
        wki_dev_server_reconcile_vfs_export(EXP.resource_id, TOKEN, raw_data(EXP.export_path), raw_data(EXP.name), EXP.backing_dev_id,
                                            TARGET_REVISION);
    }

    // This drains/retire-closes every unstamped binding and its exact server
    // FD generation, but deliberately leaves reliable VFS admission closed.
    wki_dev_server_finish_vfs_export_reconciliation(TARGET_REVISION);

    s_vfs_lock.lock();
    for (auto& exp : g_vfs_exports) {
        exp.publication_revision = TARGET_REVISION;
    }
    g_vfs_export_revision = TARGET_REVISION;
    s_vfs_lock.unlock();

    // Publish even before reopening. New attaches can now take a stable
    // snapshot, while no pre-transition queued/running operation crossed the
    // backing-mount remap.
    wki_dev_server_end_vfs_export_reconciliation(TARGET_REVISION);

    // Keep transition ownership published until the admission gate has
    // actually reopened. Otherwise a concurrent pivot can observe an even,
    // unowned table in the short window before end() and collide with the old
    // dev-server reconciliation generation.
    s_vfs_lock.lock();
    g_vfs_export_target_revision = 0;
    g_vfs_export_rebuild_prepared = false;
    s_vfs_lock.unlock();
}

}  // namespace

auto wki_remote_vfs_prepare_export_rebuild() -> bool {
    if (!g_remote_vfs_initialized) {
        return true;
    }

    s_vfs_lock.lock();
    if (g_vfs_export_rebuild_prepared || (g_vfs_export_revision & 1U) != 0 || g_vfs_export_revision > UINT64_MAX - 2) {
        s_vfs_lock.unlock();
        return false;
    }
    uint64_t const TARGET_REVISION = g_vfs_export_revision + 2;
    g_vfs_export_target_revision = TARGET_REVISION;
    g_vfs_export_rebuild_prepared = true;
    g_vfs_export_rebuild_accepting_entries = false;
    s_vfs_lock.unlock();

    // Keep the old revision stable while begin() closes admission and drains
    // requests that already crossed the pre-sequence gate. Those requests may
    // still complete against the old exact table instead of consuming a
    // transient NOT_FOUND from an odd revision.
    if (!wki_dev_server_begin_vfs_export_reconciliation(TARGET_REVISION)) {
        s_vfs_lock.lock();
        g_vfs_export_target_revision = 0;
        g_vfs_export_rebuild_prepared = false;
        s_vfs_lock.unlock();
        return false;
    }

    // Admission is now closed and all pre-gate attach/op work is drained.
    // Publish odd only at this point, immediately before the caller remaps
    // backing mount paths.
    s_vfs_lock.lock();
    g_vfs_export_revision++;
    s_vfs_lock.unlock();
    return true;
}

void wki_remote_vfs_cancel_export_rebuild() {
    if (!g_remote_vfs_initialized) {
        return;
    }

    s_vfs_lock.lock();
    bool const PREPARED = g_vfs_export_rebuild_prepared;
    s_vfs_lock.unlock();
    if (PREPARED) {
        // The mount table did not change. Reconcile the unchanged exact table,
        // advance to a new even revision (fencing pre-prepare snapshots), and
        // reopen admission without retiring matching bindings or FDs.
        reconcile_and_publish_vfs_exports();
    }
}

void wki_remote_vfs_rebuild_exports() {
    if (!g_remote_vfs_initialized) {
        return;
    }

    s_vfs_lock.lock();
    bool const ALREADY_PREPARED = g_vfs_export_rebuild_prepared;
    s_vfs_lock.unlock();
    if (!ALREADY_PREPARED && !wki_remote_vfs_prepare_export_rebuild()) {
        return;
    }

    std::deque<VfsExport> stale_exports;
    s_vfs_lock.lock();
    for (const auto& exp : g_vfs_exports) {
        if (exp.active) {
            stale_exports.push_back(exp);
        }
    }
    g_vfs_exports.clear();
    g_next_vfs_resource_id = 0x1000;
    uint32_t const MAX_STALE_RESOURCE_ID = max_preserved_export_id(stale_exports);
    if (g_next_vfs_resource_id <= MAX_STALE_RESOURCE_ID) {
        g_next_vfs_resource_id = MAX_STALE_RESOURCE_ID + 1;
    }
    g_vfs_export_rebuild_accepting_entries = true;
    s_vfs_lock.unlock();

    wki_remote_vfs_auto_discover_internal(&stale_exports);

    reconcile_and_publish_vfs_exports();

    for (const auto& exp : stale_exports) {
        ResourceAdvertPayload withdraw = {};
        withdraw.node_id = g_wki.my_node_id;
        withdraw.resource_type = static_cast<uint16_t>(ResourceType::VFS);
        withdraw.resource_id = exp.resource_id;
        withdraw.flags = 0;
        withdraw.name_len = 0;

        for (size_t p = 0; p < WKI_MAX_PEERS; p++) {
            WkiPeer const* peer = &g_wki.peers[p];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
            if (peer->node_id == WKI_NODE_INVALID || peer->state != PeerState::CONNECTED) {
                continue;
            }
            std::array<uint8_t, sizeof(ResourceAdvertPayload) + sizeof(ResourceIncarnationToken)> withdraw_buf{};
            std::memcpy(withdraw_buf.data(), &withdraw, sizeof(withdraw));
            uint16_t withdraw_len = sizeof(ResourceAdvertPayload);
            if (wki_resource_incarnation_negotiated(peer->node_id, ResourceType::VFS)) {
                ResourceIncarnationToken const TOKEN = {
                    .owner_boot_epoch = g_wki.local_boot_epoch,
                    .resource_incarnation = exp.resource_incarnation,
                };
                std::memcpy(withdraw_buf.data() + sizeof(ResourceAdvertPayload), &TOKEN, sizeof(TOKEN));
                withdraw_len = static_cast<uint16_t>(withdraw_len + sizeof(ResourceIncarnationToken));
            }
            wki_send(peer->node_id, WKI_CHAN_CONTROL, MsgType::RESOURCE_WITHDRAW, withdraw_buf.data(), withdraw_len);
        }
    }

    wki_remote_vfs_advertise_exports();
}

// -------------------------------------------------------------------------------
// Fencing Cleanup
// -------------------------------------------------------------------------------

void wki_remote_vfs_mark_server_fds_for_channel(const WkiChannelIdentity& channel_identity) {
    if (channel_identity.channel == nullptr || channel_identity.generation == 0) {
        return;
    }

    bool marked = false;
    s_vfs_lock.lock();
    for (auto& rfd : g_remote_fds) {
        if (!rfd.active || !vfs_channel_identity_matches(rfd.channel_identity, channel_identity)) {
            continue;
        }
        rfd.retiring = true;
        rfd.active = false;
        marked = true;
    }
    s_vfs_lock.unlock();

    if (marked) {
        wki_deferred_work_notify();
    }
}

auto wki_remote_vfs_detach_pending_for_resource(uint16_t owner_node, uint32_t resource_id) -> bool {
    s_vfs_lock.lock();
    bool const PENDING = vfs_attach_blocked_by_retiring_binding_locked(owner_node, resource_id);
    s_vfs_lock.unlock();
    return PENDING;
}

void wki_remote_vfs_process_pending_detaches() {
    std::array<VfsDetachAttempt, VFS_DETACH_RETRY_BATCH> attempts{};
    size_t attempt_count = 0;
    size_t scanned = 0;

    s_vfs_lock.lock();
    ProxyVfsState* state = g_pending_vfs_detach_head;
    while (state != nullptr && scanned < VFS_DETACH_RETRY_SCAN && attempt_count < attempts.size()) {
        ProxyVfsState* const NEXT = state->detach_next;
        scanned++;
        if (!state->detach_retry_in_progress) {
            state->detach_retry_in_progress = true;
            attempts.at(attempt_count++) = {
                .state = state,
                .owner_node = state->detach_owner_node,
                .resource_id = state->detach_resource_id,
                .attach_cookie = state->detach_attach_cookie,
                .incarnation = state->detach_incarnation,
                .peer_boot_epoch = state->detach_peer_boot_epoch,
                .tx_token = state->detach_tx_token,
            };
            if (state != g_pending_vfs_detach_tail) {
                unlink_pending_vfs_detach_locked(state);
                link_pending_vfs_detach_locked(state);
            }
        }
        state = NEXT;
    }
    s_vfs_lock.unlock();

    for (size_t i = 0; i < attempt_count; ++i) {
        VfsDetachAttempt const& ATTEMPT = attempts.at(i);
        bool const PEER_EPOCH_INVALIDATED = wki_peer_remote_boot_epoch_invalidated(ATTEMPT.owner_node, ATTEMPT.peer_boot_epoch);
        WkiReliableTxStatus const TX_STATUS = wki_reliable_tx_status(ATTEMPT.tx_token);
        WkiReliableTxToken replacement_token = {};
        int send_ret = WKI_ERR_BUSY;
        if (!PEER_EPOCH_INVALIDATED && (TX_STATUS == WkiReliableTxStatus::INVALID || TX_STATUS == WkiReliableTxStatus::RETIRED)) {
            send_ret =
                send_vfs_detach(ATTEMPT.owner_node, ATTEMPT.resource_id, ATTEMPT.attach_cookie, ATTEMPT.incarnation, &replacement_token);
        }
        finish_vfs_detach_attempt(ATTEMPT, TX_STATUS, send_ret, replacement_token, PEER_EPOCH_INVALIDATED);
    }
}

void wki_remote_vfs_process_pending_server_fd_cleanup() {
    constexpr size_t CLOSE_BATCH = 32;
    while (true) {
        std::array<ker::vfs::File*, CLOSE_BATCH> files_to_close{};
        size_t close_count = 0;

        s_vfs_lock.lock();
        for (auto& rfd : g_remote_fds) {
            if (!rfd.retiring || rfd.file == nullptr || close_count >= files_to_close.size()) {
                continue;
            }
            files_to_close.at(close_count++) = rfd.file;
            rfd.file = nullptr;
        }
        std::erase_if(g_remote_fds, [](const RemoteVfsFd& rfd) { return rfd.retiring && rfd.file == nullptr; });
        s_vfs_lock.unlock();

        for (size_t i = 0; i < close_count; ++i) {
            static_cast<void>(ker::vfs::vfs_close_file(files_to_close.at(i)));
        }
        if (close_count < files_to_close.size()) {
            return;
        }
    }
}

void wki_remote_vfs_cleanup_server_fds_for_channel(const WkiChannelIdentity& channel_identity) {
    wki_remote_vfs_mark_server_fds_for_channel(channel_identity);
    wki_remote_vfs_process_pending_server_fd_cleanup();
}

void wki_remote_vfs_mark_epoch_reset(uint16_t node_id) {
    s_vfs_lock.lock();
    for (auto& proxy : g_vfs_proxies) {
        auto* state = proxy.get();
        if (state == nullptr || state->owner_node != node_id ||
            (!state->active && !state->attach_pending.load(std::memory_order_acquire))) {
            continue;
        }

        mark_vfs_proxy_group_unavailable_locked(state);

        // This phase runs from HELLO RX and therefore cannot allocate, close
        // files, or quiesce waiters. Stop new ID-based operations before pool
        // slots are reusable; task-context cleanup consumes the marker.
        state->lock.lock();
        if (state->active || state->attach_pending.load(std::memory_order_acquire)) {
            state->active = false;
            state->epoch_reset_pending = true;
        }
        state->lock.unlock();
    }
    s_vfs_lock.unlock();
}

void wki_remote_vfs_cleanup_for_peer(uint16_t node_id, bool owner_reboot_proven) {
    // Server side: close all remote FDs for this consumer
    std::deque<ker::vfs::File*> files_to_close;
    std::deque<PendingProxyTeardown> proxies_to_cleanup;

    s_vfs_lock.lock();
    for (auto& rfd : g_remote_fds) {
        if (rfd.consumer_node != node_id) {
            continue;
        }
        if (rfd.file != nullptr) {
            files_to_close.push_back(rfd.file);
            rfd.file = nullptr;
        }
        rfd.retiring = true;
        rfd.active = false;
    }
    std::erase_if(g_remote_fds,
                  [node_id](const RemoteVfsFd& rfd) { return rfd.consumer_node == node_id && rfd.retiring && rfd.file == nullptr; });

    // Consumer side: fail pending ops and deactivate proxies while the proxy
    // registry is locked.  Cache invalidation and channel close happen later
    // because they can take other subsystem locks.
    for (auto& proxy : g_vfs_proxies) {
        auto* p = proxy.get();
        if (p == nullptr || p->owner_node != node_id || (!p->active && !p->epoch_reset_pending)) {
            continue;
        }

        mark_vfs_proxy_group_unavailable_locked(p);
        PendingProxyTeardown cleanup = {};
        deactivate_vfs_proxy_locked(p, cleanup, false);
        // Deactivation is followed by withdrawal/unmount, which can erase the
        // final local cookie representation. Pin and reserve the exact tuple
        // first unless a concrete owner reboot proves it obsolete.
        if (!owner_reboot_proven) {
            cleanup.detach_staged = stage_vfs_detach_locked(p, cleanup.owner_node, cleanup.resource_id, cleanup.binding_attach_cookie,
                                                            cleanup.binding_incarnation, false);
        }
        if (owner_reboot_proven || p->detach_pending) {
            p->epoch_reset_pending = false;
        }
        invalidate_all_dir_caches(p);

        proxies_to_cleanup.push_back(cleanup);
    }
    s_vfs_lock.unlock();

    for (const auto& cleanup : proxies_to_cleanup) {
        finish_proxy_teardown_op_waiter(cleanup, -1);
    }

    for (auto* file : files_to_close) {
        if (file != nullptr) {
            if (file->fops != nullptr && file->fops->vfs_close != nullptr) {
                file->fops->vfs_close(file);
            }
            delete file;
        }
    }

    for (const auto& cleanup : proxies_to_cleanup) {
        ker::vfs::vfs_stream_cache_invalidate_remote_scope(cleanup.state);
        invalidate_readlink_cache(cleanup.state);

        if (cleanup.had_op_pending) {
            ker::mod::dbg::log("[WKI] VFS op PEER_CLEANUP: node=0x%04x ch=%u op=%u seq=%u mount=%s", cleanup.owner_node,
                               cleanup.assigned_channel, cleanup.op_expected_id, cleanup.op_expected_seq, cleanup.local_mount_path.data());
        }

        wake_proxy_slot_waiters(cleanup);
        finish_claimed_waiter(cleanup.attach_wait_entry, -1);

        static_cast<void>(wki_channel_close_generation(cleanup.assigned_channel_ref, cleanup.owner_node, cleanup.assigned_channel,
                                                       cleanup.assigned_channel_generation));

        if (cleanup.detach_staged) {
            wki_deferred_work_notify();
        }

        ker::mod::dbg::log("[WKI] Remote VFS proxy cleanup: %s node=0x%04x", cleanup.local_mount_path.data(), node_id);
        release_vfs_proxy_lifecycle_ref(cleanup.state);
    }
}

}  // namespace ker::net::wki
