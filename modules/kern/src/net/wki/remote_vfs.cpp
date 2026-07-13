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
#include <net/wki/timer_math.hpp>
#include <net/wki/transport_roce.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/mutex.hpp>
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

auto take_preserved_export_id(std::deque<VfsExport>* stale_exports, const char* name) -> uint32_t {
    if (stale_exports == nullptr || name == nullptr) {
        return 0;
    }

    for (auto it = stale_exports->begin(); it != stale_exports->end(); ++it) {
        if (it->active && std::strncmp(static_cast<const char*>(it->name), name, VFS_EXPORT_NAME_LEN) == 0) {
            uint32_t const RESOURCE_ID = it->resource_id;
            stale_exports->erase(it);
            return RESOURCE_ID;
        }
    }

    return 0;
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

std::deque<VfsExport> g_vfs_exports;       // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<RemoteVfsFd> g_remote_fds;      // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
int32_t g_next_remote_fd = 1;              // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_next_vfs_resource_id = 0x1000;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto find_remote_fd(uint16_t consumer_node, uint16_t channel_id, int32_t fd_id) -> RemoteVfsFd* {
    for (auto& rfd : g_remote_fds) {
        if (rfd.active && rfd.consumer_node == consumer_node && rfd.channel_id == channel_id && rfd.fd_id == fd_id) {
            return &rfd;
        }
    }
    return nullptr;
}

auto alloc_remote_fd(uint16_t consumer_node, uint16_t channel_id, ker::vfs::File* file) -> int32_t {
    int32_t const FD_ID = g_next_remote_fd++;

    RemoteVfsFd rfd;
    rfd.active = true;
    rfd.consumer_node = consumer_node;
    rfd.channel_id = channel_id;
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

    std::array<char, 512> resolved_path{};
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

ker::mod::sys::Spinlock s_vfs_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct VfsProxyResponseLookup {
    ProxyVfsState* matched_state = nullptr;
    uint16_t stale_expected_op = 0;
    uint16_t stale_expected_seq = 0;
    size_t active_candidates = 0;
    size_t pending_candidates = 0;
    bool saw_pending_candidate = false;
};

auto find_vfs_proxy_for_response_locked(uint16_t owner_node, uint16_t channel_id, uint16_t resp_op, uint16_t resp_seq)
    -> VfsProxyResponseLookup {
    VfsProxyResponseLookup lookup = {};

    for (auto& p : g_vfs_proxies) {
        if (!p->active || p->owner_node != owner_node || p->assigned_channel != channel_id) {
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

auto find_vfs_proxy_by_attach(uint16_t owner_node, uint32_t resource_id) -> ProxyVfsState* {
    for (auto& p : g_vfs_proxies) {
        if (p->attach_pending && p->owner_node == owner_node && p->resource_id == resource_id) {
            return p.get();
        }
    }
    return nullptr;
}

// Caller must hold s_vfs_lock.
auto allocate_vfs_attach_cookie_locked() -> uint8_t {
    uint8_t cookie = g_vfs_attach_next_cookie;
    if (cookie == 0) {
        cookie = 1;
    }
    g_vfs_attach_next_cookie = static_cast<uint8_t>(cookie + 1U);
    if (g_vfs_attach_next_cookie == 0) {
        g_vfs_attach_next_cookie = 1;
    }
    return cookie;
}

// Caller must hold s_vfs_lock.
auto vfs_attach_ack_matches_pending_locked(ProxyVfsState const* state, const DevAttachAckPayload& ack) -> bool {
    return state != nullptr && state->attach_pending.load(std::memory_order_acquire) && state->attach_expected_cookie != 0 &&
           wki_dev_attach_ack_matches_expected(state->attach_expected_cookie, ack);
}

auto find_vfs_proxy_by_mount(const char* mount_path) -> ProxyVfsState* {
    for (auto& p : g_vfs_proxies) {
        if (p->active && strncmp(p->local_mount_path.data(), mount_path, p->local_mount_path.size()) == 0) {
            return p.get();
        }
    }
    return nullptr;
}

auto find_vfs_proxy_by_channel(uint16_t owner_node, uint16_t channel_id) -> ProxyVfsState* {
    for (auto& p : g_vfs_proxies) {
        if (p->active && p->owner_node == owner_node && p->assigned_channel == channel_id) {
            return p.get();
        }
    }
    return nullptr;
}

auto peek_channel_tx_seq16(uint16_t owner_node, uint16_t channel_id, uint16_t* seq_out) -> bool {
    if (seq_out == nullptr) {
        return false;
    }

    WkiChannel* ch = wki_channel_get(owner_node, channel_id);
    if (ch == nullptr) {
        return false;
    }

    ch->lock.lock();
    *seq_out = static_cast<uint16_t>(ch->tx_seq & UINT16_MAX);
    ch->lock.unlock();
    return true;
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

void reset_readlink_cache_entry(ProxyVfsState::ReadlinkCacheEntry& entry) {
    entry.valid = false;
    entry.status = 0;
    entry.target_len = 0;
    entry.cached_at_us = 0;
    entry.path.front() = '\0';
    entry.target.front() = '\0';
}

void invalidate_readlink_cache_locked(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }

    for (auto& entry : state->readlink_cache) {
        reset_readlink_cache_entry(entry);
    }
}

void invalidate_readlink_cache(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }

    state->lock.lock();
    invalidate_readlink_cache_locked(state);
    state->lock.unlock();
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

void discard_failed_attached_proxy(ProxyVfsState* state, WkiChannel* reserved_channel) {
    if (state == nullptr) {
        return;
    }

    uint16_t const OWNER_NODE = state->owner_node;
    uint32_t const RESOURCE_ID = state->resource_id;
    s_vfs_lock.lock();
    state->active = false;
    s_vfs_lock.unlock();

    DevDetachPayload det = {};
    det.target_node = OWNER_NODE;
    det.resource_type = static_cast<uint16_t>(ResourceType::VFS);
    det.resource_id = RESOURCE_ID;
    wki_send(OWNER_NODE, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));

    if (reserved_channel != nullptr) {
        wki_channel_close(reserved_channel);
    }

    release_vfs_proxy_buffers(state);

    s_vfs_lock.lock();
    std::erase_if(g_vfs_proxies, [state](const std::unique_ptr<ProxyVfsState>& proxy) { return proxy.get() == state; });
    s_vfs_lock.unlock();
}

auto readlink_status_is_cacheable(int status) -> bool {
    return status == 0 || status == -EINVAL || status == -ENOENT || status == -ENOTDIR;
}

auto try_readlink_cache_lookup(ProxyVfsState* state, const char* fs_relative_path, char* buf, size_t bufsize, ssize_t* result_out) -> bool {
    if (state == nullptr || fs_relative_path == nullptr || result_out == nullptr) {
        return false;
    }

    if (std::strlen(fs_relative_path) >= VFS_READLINK_CACHE_TEXT_MAX) {
        return false;
    }

    bool found = false;
    uint64_t const NOW = wki_now_us();
    state->lock.lock();
    for (auto& entry : state->readlink_cache) {
        if (!entry.valid) {
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

void cache_readlink_result(ProxyVfsState* state, const char* fs_relative_path, int status, const char* target, uint16_t target_len) {
    if (state == nullptr || fs_relative_path == nullptr || !readlink_status_is_cacheable(status)) {
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

    ProxyVfsState::ReadlinkCacheEntry* selected = nullptr;
    ProxyVfsState::ReadlinkCacheEntry* oldest = nullptr;
    for (auto& entry : state->readlink_cache) {
        if (entry.valid && NOW - entry.cached_at_us > readlink_cache_retention_us(entry.status)) {
            reset_readlink_cache_entry(entry);
        }

        if (!entry.valid) {
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
        selected->valid = true;
        selected->status = static_cast<int16_t>(status);
        selected->cached_at_us = NOW;
        memcpy(selected->path.data(), fs_relative_path, PATH_LEN + 1);
        if (status == 0) {
            selected->target_len = target_len;
            memcpy(selected->target.data(), target, target_len);
        }
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

auto proxy_op_slot_busy(ProxyVfsState* state) -> bool {
    return state->op_pending.load(std::memory_order_acquire) || state->op_untracked_send_pending.load(std::memory_order_acquire);
}

auto acquire_proxy_op_slot_locked(ProxyVfsState* state, uint64_t start_us) -> int {
    uint64_t const DEADLINE_US = wki_future_deadline_us(start_us, VFS_PROXY_SLOT_WAIT_TIMEOUT_US);
    while (true) {
        state->lock.lock();
        if (!proxy_op_slot_busy(state)) {
            return WKI_OK;
        }
        state->lock.unlock();
        if (!proxy_op_slot_busy(state)) {
            continue;
        }
        if (wki_now_us() >= DEADLINE_US) {
            return WKI_ERR_TIMEOUT;
        }
        ker::mod::sched::kern_sleep_us(VFS_PROXY_CONTENTION_SLEEP_US);
    }
}

auto acquire_proxy_untracked_send_slot_locked(ProxyVfsState* state, uint64_t start_us) -> int {
    uint64_t const DEADLINE_US = wki_future_deadline_us(start_us, VFS_PROXY_SLOT_WAIT_TIMEOUT_US);
    while (true) {
        state->lock.lock();
        if (!proxy_op_slot_busy(state)) {
            state->op_untracked_send_pending.store(true, std::memory_order_release);
            return WKI_OK;
        }
        state->lock.unlock();
        if (!proxy_op_slot_busy(state)) {
            continue;
        }
        if (wki_now_us() >= DEADLINE_US) {
            return WKI_ERR_TIMEOUT;
        }
        ker::mod::sched::kern_sleep_us(VFS_PROXY_CONTENTION_SLEEP_US);
    }
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

void finish_claimed_waiter(WkiWaitEntry* waiter, int result) {
    if (waiter != nullptr) {
        wki_finish_claimed_op(waiter, result);
    }
}

void finish_or_wait_for_cancelled_waiter(WkiWaitEntry& wait, bool claimed, int result) {
    if (claimed) {
        wki_finish_claimed_op(&wait, result);
        return;
    }

    while (wait.state.load(std::memory_order_acquire) != static_cast<uint8_t>(WkiWaitEntry::DONE)) {
        ker::mod::sched::kern_yield();
    }
}

void clear_proxy_op_state_locked(ProxyVfsState* state, int status) {
    state->op_status = static_cast<int16_t>(status);
    state->op_expected_id = 0;
    state->op_expected_seq = 0;
    state->op_resp_buf = nullptr;
    state->op_resp_max = 0;
    state->op_resp_len = 0;
    state->op_pending.store(false, std::memory_order_release);
}

void cancel_proxy_op_wait(ProxyVfsState* state, WkiWaitEntry& wait, int result) {
    bool claimed = false;
    state->lock.lock();
    if (state->op_wait_entry == &wait) {
        state->op_wait_entry = nullptr;
    }
    clear_proxy_op_state_locked(state, result);
    claimed = wki_claim_op(&wait);
    state->lock.unlock();

    finish_or_wait_for_cancelled_waiter(wait, claimed, result);
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

    finish_or_wait_for_cancelled_waiter(wait, claimed, result);
}

struct PendingProxyTeardown {
    ProxyVfsState* state = nullptr;
    uint16_t owner_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    uint32_t resource_id = 0;
    uint16_t op_expected_id = 0;
    uint16_t op_expected_seq = 0;
    bool had_op_pending = false;
    WkiWaitEntry* op_wait_entry = nullptr;
    WkiWaitEntry* attach_wait_entry = nullptr;
    std::array<char, VFS_EXPORT_PATH_LEN> local_mount_path = {};
};

auto proxy_is_idle_for_resource_release_locked(ProxyVfsState* state) -> bool {
    return state != nullptr && !state->active && state->open_file_refs.load(std::memory_order_acquire) == 0;
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
    teardown.resource_id = state->resource_id;
    teardown.local_mount_path = state->local_mount_path;

    state->lock.lock();
    if (state->op_pending.load(std::memory_order_acquire)) {
        teardown.had_op_pending = true;
        teardown.op_expected_id = state->op_expected_id;
        teardown.op_expected_seq = state->op_expected_seq;
        state->op_status = -1;
        teardown.op_wait_entry = claim_and_clear_waiter_locked(state->op_wait_entry);
        clear_proxy_op_state_locked(state, -1);
    }
    if (state->attach_pending.load(std::memory_order_acquire)) {
        teardown.attach_wait_entry = claim_and_clear_waiter_locked(state->attach_wait_entry);
        clear_proxy_attach_state_locked(state, static_cast<uint8_t>(DevAttachStatus::BUSY));
    }
    state->active = false;
    if (destroy_when_idle) {
        state->destroy_when_idle = true;
    }
    state->lock.unlock();
}

// Helper: send DEV_OP_REQ and wait for response
auto vfs_proxy_send_and_wait(ProxyVfsState* state, uint16_t op_id, const uint8_t* req_data, uint16_t req_data_len, void* resp_buf,
                             uint16_t resp_buf_max, uint16_t* resp_len_out = nullptr, uint64_t wait_timeout_us = VFS_PROXY_OP_TIMEOUT_US,
                             RoceTaggedReceive* tagged_receive = nullptr) -> int {
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint64_t const PROXY_WAIT_START = wki_now_us();

    if (resp_len_out != nullptr) {
        *resp_len_out = 0;
    }

    auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + req_data_len);
    auto* req_buf = new (std::nothrow) uint8_t[req_total];
    if (req_buf == nullptr) {
        return -ENOMEM;
    }

    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf);
    req->op_id = op_id;
    req->data_len = req_data_len;

    if (req_data_len > 0 && req_data != nullptr) {
        memcpy(req_buf + sizeof(DevOpReqPayload), req_data, req_data_len);
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
        delete[] req_buf;
        return encode_proxy_wki_status(SLOT_RET);
    }

    uint16_t expected_seq = 0;
    if (!peek_channel_tx_seq16(state->owner_node, state->assigned_channel, &expected_seq)) {
        state->lock.unlock();
        delete[] req_buf;
        return -EIO;
    }
    uint32_t const CORRELATION = expected_seq;

    if (tagged_receive != nullptr) {
        if (tagged_receive->rkey == 0 || !wki_roce_region_prepare_tagged_write(tagged_receive->rkey, expected_seq)) {
            state->lock.unlock();
            delete[] req_buf;
            return -EIO;
        }
        tagged_receive->cookie = expected_seq;
    }

    // Lock held, op_pending still false - set up wait entry and response fields
    WkiWaitEntry wait = {};
    state->op_wait_entry = &wait;
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
                                     req_data_len, CALLSITE);

    int const SEND_RET = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf, req_total);
    delete[] req_buf;

    if (SEND_RET != WKI_OK) {
        cancel_proxy_op_wait(state, wait, SEND_RET);
        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                         perf_vfs_op(op_id), ker::mod::perf::WkiPerfPhase::END, state->owner_node, state->assigned_channel,
                                         CORRELATION, SEND_RET, ELAPSED_US, CALLSITE);
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id), state->owner_node,
                                           state->assigned_channel, SEND_RET, ELAPSED_US, true, 0, req_data_len);
        ker::mod::dbg::log("[WKI] vfs_proxy_send_and_wait send failed: node=0x%04x ch=%u op=%u rc=%d", state->owner_node,
                           state->assigned_channel, op_id, SEND_RET);
        return encode_proxy_wki_status(SEND_RET);
    }

    int const WAIT_RC = wki_wait_for_op(&wait, wait_timeout_us);
    if (WAIT_RC != 0) {
        cancel_proxy_op_wait(state, wait, WAIT_RC);
        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                         perf_vfs_op(op_id), ker::mod::perf::WkiPerfPhase::END, state->owner_node, state->assigned_channel,
                                         CORRELATION, WAIT_RC, ELAPSED_US, CALLSITE);
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id), state->owner_node,
                                           state->assigned_channel, WAIT_RC, ELAPSED_US, true, 0, req_data_len);
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
    if (state->op_wait_entry == &wait) {
        state->op_wait_entry = nullptr;
    }
    int const STATUS = static_cast<int>(state->op_status);
    uint16_t const RESP_LEN = state->op_resp_len;
    clear_proxy_op_state_locked(state, STATUS);
    state->lock.unlock();

    auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id),
                                     ker::mod::perf::WkiPerfPhase::END, state->owner_node, state->assigned_channel, CORRELATION, STATUS,
                                     ELAPSED_US, CALLSITE);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id), state->owner_node,
                                       state->assigned_channel, STATUS, ELAPSED_US, true, 0, perf_vfs_bytes(op_id, req_data_len, RESP_LEN));

    if (resp_len_out != nullptr) {
        *resp_len_out = RESP_LEN;
    }

    return normalize_proxy_status_for_errno(STATUS);
}

void remote_vfs_close_remote_fd_best_effort(ProxyVfsState* state, int32_t remote_fd) {
    if (state == nullptr || remote_fd < 0) {
        return;
    }

    static_cast<void>(
        vfs_proxy_send_and_wait(state, OP_VFS_CLOSE, reinterpret_cast<const uint8_t*>(&remote_fd), sizeof(remote_fd), nullptr, 0));
}

auto vfs_proxy_send_untracked(ProxyVfsState* state, uint16_t op_id, const uint8_t* req_data, uint16_t req_data_len) -> int {
    if (state == nullptr) {
        return -EINVAL;
    }

    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint64_t const PROXY_WAIT_START = wki_now_us();

    auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + req_data_len);
    auto* req_buf = new (std::nothrow) uint8_t[req_total];
    if (req_buf == nullptr) {
        return -ENOMEM;
    }

    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf);
    req->op_id = op_id;
    req->data_len = req_data_len;

    if (req_data_len > 0 && req_data != nullptr) {
        memcpy(req_buf + sizeof(DevOpReqPayload), req_data, req_data_len);
    }

    int const SLOT_RET = acquire_proxy_untracked_send_slot_locked(state, PROXY_WAIT_START);
    auto const PROXY_WAIT_US = static_cast<uint32_t>(wki_now_us() - PROXY_WAIT_START);
    if (PROXY_WAIT_US > 0) {
        perf_record_vfs_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::PROXY_WAIT), state->owner_node, state->assigned_channel, 0,
                              PROXY_WAIT_US, CALLSITE);
    }
    if (SLOT_RET != WKI_OK) {
        delete[] req_buf;
        return normalize_proxy_status_for_errno(encode_proxy_wki_status(SLOT_RET));
    }

    uint16_t expected_seq = 0;
    if (!peek_channel_tx_seq16(state->owner_node, state->assigned_channel, &expected_seq)) {
        state->op_untracked_send_pending.store(false, std::memory_order_release);
        state->lock.unlock();
        delete[] req_buf;
        return -EIO;
    }
    uint32_t const CORRELATION = expected_seq;
    state->lock.unlock();

    uint64_t const STARTED_US = wki_now_us();
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id),
                                     ker::mod::perf::WkiPerfPhase::BEGIN, state->owner_node, state->assigned_channel, CORRELATION, 0,
                                     req_data_len, CALLSITE);

    int const SEND_RET = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf, req_total);
    delete[] req_buf;

    state->lock.lock();
    state->op_untracked_send_pending.store(false, std::memory_order_release);
    state->lock.unlock();

    auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id),
                                     ker::mod::perf::WkiPerfPhase::END, state->owner_node, state->assigned_channel, CORRELATION, SEND_RET,
                                     ELAPSED_US, CALLSITE);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id), state->owner_node,
                                       state->assigned_channel, SEND_RET, ELAPSED_US, true, 0, req_data_len);

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

    uint16_t expected_seq = 0;
    if (!peek_channel_tx_seq16(state->owner_node, state->assigned_channel, &expected_seq)) {
        state->lock.unlock();
        return -EIO;
    }
    uint32_t const CORRELATION = expected_seq;

    WkiWaitEntry wait = {};
    uint32_t resp_written = 0;
    state->op_wait_entry = &wait;
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

    auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + ctrl.size());
    auto* req_buf = new (std::nothrow) uint8_t[req_total];
    if (req_buf == nullptr) {
        cancel_proxy_op_wait(state, wait, -ENOMEM);

        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                         perf_vfs_op(OP_VFS_WRITE_RDMA), ker::mod::perf::WkiPerfPhase::END, state->owner_node,
                                         state->assigned_channel, CORRELATION, -ENOMEM, ELAPSED_US, CALLSITE);
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(OP_VFS_WRITE_RDMA), state->owner_node,
                                           state->assigned_channel, -ENOMEM, ELAPSED_US, true, 0, 0);
        return -ENOMEM;
    }

    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf);
    req->op_id = OP_VFS_WRITE_RDMA;
    req->data_len = static_cast<uint16_t>(ctrl.size());
    memcpy(req_buf + sizeof(DevOpReqPayload), ctrl.data(), ctrl.size());

    bool const CONTROL_FIRST = transport_is_roce(state->rdma_transport);
    if (!CONTROL_FIRST) {
        int const RDMA_RET =
            state->rdma_transport->rdma_write(state->rdma_transport, state->owner_node, state->rdma_server_write_rkey, 0, src, chunk);
        if (RDMA_RET != 0) {
            delete[] req_buf;
            cancel_proxy_op_wait(state, wait, RDMA_RET);

            auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
            ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                             perf_vfs_op(OP_VFS_WRITE_RDMA), ker::mod::perf::WkiPerfPhase::END, state->owner_node,
                                             state->assigned_channel, CORRELATION, RDMA_RET, ELAPSED_US, CALLSITE);
            ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(OP_VFS_WRITE_RDMA), state->owner_node,
                                               state->assigned_channel, RDMA_RET, ELAPSED_US, true, 0, chunk);
            return -EIO;
        }
    }

    int const SEND_RET = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf, req_total);
    delete[] req_buf;
    if (SEND_RET != WKI_OK) {
        cancel_proxy_op_wait(state, wait, SEND_RET);

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
            cancel_proxy_op_wait(state, wait, RDMA_RET);

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
        cancel_proxy_op_wait(state, wait, WAIT_RC);

        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                         perf_vfs_op(OP_VFS_WRITE_RDMA), ker::mod::perf::WkiPerfPhase::END, state->owner_node,
                                         state->assigned_channel, CORRELATION, WAIT_RC, ELAPSED_US, CALLSITE);
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(OP_VFS_WRITE_RDMA), state->owner_node,
                                           state->assigned_channel, WAIT_RC, ELAPSED_US, true, 0, chunk);
        return normalize_proxy_status_for_errno(encode_proxy_wki_status(WAIT_RC));
    }

    state->lock.lock();
    if (state->op_wait_entry == &wait) {
        state->op_wait_entry = nullptr;
    }
    int const STATUS = static_cast<int>(state->op_status);
    uint16_t const RESP_LEN = state->op_resp_len;
    clear_proxy_op_state_locked(state, STATUS);
    state->lock.unlock();

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

            auto req_data_len = static_cast<uint16_t>(12 + CHUNK);
            auto* req_data = new (std::nothrow) uint8_t[req_data_len];
            if (req_data == nullptr) {
                keep_pending_tail(src, remaining, cur_offset);
                return -ENOMEM;
            }

            int32_t remote_fd = ctx->remote_fd;
            memcpy(req_data, &remote_fd, sizeof(int32_t));
            memcpy(req_data + 4, &cur_offset, sizeof(int64_t));
            memcpy(req_data + 12, src, CHUNK);

            uint32_t written = 0;
            int const STATUS = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_WRITE, req_data, req_data_len, &written, sizeof(uint32_t));
            delete[] req_data;
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

        // Send OP_VFS_CLOSE: {remote_fd:i32} = 4 bytes. Pending writes were flushed
        // synchronously above, and the close request remains ordered on the reliable
        // per-mount channel. The owner response only reports whether the remote fd
        // existed, which is not observable after the local descriptor is gone, so
        // normal closes do not need to pay another request/response RTT.
        int32_t remote_fd = ctx->remote_fd;
        int const SEND_STATUS =
            vfs_proxy_send_untracked(ctx->proxy, OP_VFS_CLOSE, reinterpret_cast<const uint8_t*>(&remote_fd), sizeof(int32_t));
        if (SEND_STATUS != 0) {
            ker::mod::dbg::log("[WKI] async remote close send failed: node=0x%04x ch=%u fd=%d rc=%d", ctx->proxy->owner_node,
                               ctx->proxy->assigned_channel, remote_fd, SEND_STATUS);
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

    std::array<uint8_t, VFS_DIRECT_READ_STACK_SIZE> direct_read_buf{};

    while (remaining > 0) {
        bool const SHOULD_READ_AHEAD = ALLOW_READ_CACHES && !POSITIONAL_READ && remaining < VFS_CACHE_SIZE;
        if (SHOULD_READ_AHEAD && ctx->read_cache == nullptr) {
            ctx->read_cache = new (std::nothrow) ReadAheadCache();  // NOLINT(cppcoreguidelines-owning-memory)
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
    // One OP_VFS_READDIR_BATCH request returns up to MAX_BATCH_ENTRIES entries,
    // filling the full jumbo frame - typically the entire directory in one round-trip.
    constexpr auto MAX_BATCH_ENTRIES =
        static_cast<uint32_t>((WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload) - sizeof(uint32_t)) / sizeof(ker::vfs::DirEntry));
    constexpr size_t BATCH_RESP_SIZE = sizeof(uint32_t) + (MAX_BATCH_ENTRIES * sizeof(ker::vfs::DirEntry));

    auto* batch_buf = new (std::nothrow) uint8_t[BATCH_RESP_SIZE];
    if (batch_buf == nullptr) {
        return -ENOMEM;
    }

    while (true) {
        s_vfs_lock.lock();
        cache = find_dir_cache(ctx->proxy, ctx->remote_fd);
        if (cache == nullptr) {
            s_vfs_lock.unlock();
            delete[] batch_buf;
            return -EIO;
        }
        if (try_get_visible_cached_dirent(cache, ctx, index, entry)) {
            s_vfs_lock.unlock();
            delete[] batch_buf;
            return 0;
        }
        if (cache->complete) {
            s_vfs_lock.unlock();
            delete[] batch_buf;
            return -1;
        }
        auto fetch_idx = static_cast<uint32_t>(cache->entries.size());
        s_vfs_lock.unlock();

        // Request: {fd:i32, start_idx:u32, max_count:u32} = 12 bytes
        std::array<uint8_t, 12> req_data{};
        memcpy(req_data.data(), &ctx->remote_fd, sizeof(int32_t));
        memcpy(req_data.data() + 4, &fetch_idx, sizeof(uint32_t));
        memcpy(req_data.data() + 8, &MAX_BATCH_ENTRIES, sizeof(uint32_t));

        memset(batch_buf, 0, BATCH_RESP_SIZE);
        uint16_t resp_len = 0;
        int const STATUS = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_READDIR_BATCH, req_data.data(), 12, batch_buf,
                                                   static_cast<uint16_t>(BATCH_RESP_SIZE), &resp_len);
        if (STATUS != 0 || resp_len < sizeof(uint32_t)) {
            s_vfs_lock.lock();
            cache = find_dir_cache(ctx->proxy, ctx->remote_fd);
            if (cache != nullptr) {
                cache->complete = true;
            }
            s_vfs_lock.unlock();
            delete[] batch_buf;
            return (STATUS != 0) ? STATUS : -EIO;
        }

        uint32_t count = 0;
        memcpy(&count, batch_buf, sizeof(uint32_t));

        s_vfs_lock.lock();
        cache = find_dir_cache(ctx->proxy, ctx->remote_fd);
        if (cache != nullptr) {
            for (uint32_t i = 0; i < count && i < MAX_BATCH_ENTRIES; i++) {
                ker::vfs::DirEntry fetched = {};
                memcpy(&fetched, batch_buf + sizeof(uint32_t) + (i * sizeof(ker::vfs::DirEntry)), sizeof(ker::vfs::DirEntry));
                cache->entries.push_back(fetched);
            }
            if (count < MAX_BATCH_ENTRIES) {
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

auto wki_remote_vfs_fsync(ker::vfs::File* file) -> int { return remote_vfs_fsync_file(file); }

#ifdef WOS_SELFTEST
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

auto wki_remote_vfs_export_add_internal(const char* export_path, const char* name, uint32_t preferred_resource_id) -> uint32_t {
    if (export_path == nullptr || name == nullptr) {
        return 0;
    }

    s_vfs_lock.lock();

    // Check if this path is already exported (prevent duplicates)
    for (const auto& existing : g_vfs_exports) {
        if (existing.active && strcmp(raw_data(existing.export_path), export_path) == 0) {
            // Already exported - return existing resource_id
            uint32_t const EXISTING_ID = existing.resource_id;
            s_vfs_lock.unlock();
            return EXISTING_ID;
        }
    }

    VfsExport exp;
    exp.active = true;
    if (preferred_resource_id != 0) {
        exp.resource_id = preferred_resource_id;
        if (g_next_vfs_resource_id <= preferred_resource_id) {
            g_next_vfs_resource_id = preferred_resource_id + 1;
        }
    } else {
        exp.resource_id = g_next_vfs_resource_id++;
    }

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

    g_vfs_exports.push_back(exp);
    uint32_t const RESULT_ID = exp.resource_id;
    s_vfs_lock.unlock();

    ker::mod::dbg::log("[WKI] VFS export added: %s -> %s (resource_id=%u)", raw_data(exp.name), raw_data(exp.export_path), RESULT_ID);
    return RESULT_ID;
}

}  // namespace

auto wki_remote_vfs_export_add(const char* export_path, const char* name) -> uint32_t {
    return wki_remote_vfs_export_add_internal(export_path, name, 0);
}

auto wki_remote_vfs_find_export_snapshot(uint32_t resource_id, VfsExport* out) -> bool {
    if (out == nullptr) {
        return false;
    }

    s_vfs_lock.lock();
    for (const auto& exp : g_vfs_exports) {
        if (exp.active && exp.resource_id == resource_id) {
            *out = exp;
            s_vfs_lock.unlock();
            return true;
        }
    }
    s_vfs_lock.unlock();
    return false;
}

namespace {

void advertise_exports_to_peer(uint16_t peer_node) {
    s_vfs_lock.lock();
    size_t const EXPORT_COUNT = g_vfs_exports.size();
    s_vfs_lock.unlock();

    for (size_t idx = 0; idx < EXPORT_COUNT; idx++) {
        s_vfs_lock.lock();
        if (idx >= g_vfs_exports.size()) {
            s_vfs_lock.unlock();
            break;
        }
        auto& exp = *std::next(g_vfs_exports.begin(), static_cast<ptrdiff_t>(idx));
        if (!exp.active) {
            s_vfs_lock.unlock();
            continue;
        }

        // Build ResourceAdvertPayload + name under lock
        auto const NAME_LEN = static_cast<uint8_t>(std::min<size_t>(std::strlen(raw_data(exp.name)), 63U));

        auto total_len = static_cast<uint16_t>(sizeof(ResourceAdvertPayload) + NAME_LEN);
        std::array<uint8_t, sizeof(ResourceAdvertPayload) + 64> buf{};

        auto* adv = reinterpret_cast<ResourceAdvertPayload*>(buf.data());
        adv->node_id = g_wki.my_node_id;
        adv->resource_type = static_cast<uint16_t>(ResourceType::VFS);
        adv->resource_id = exp.resource_id;
        adv->flags = 0;
        adv->name_len = NAME_LEN;
        memcpy(buf.data() + sizeof(ResourceAdvertPayload), static_cast<const void*>(raw_data(exp.name)), NAME_LEN);
        s_vfs_lock.unlock();

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
void handle_vfs_op(const WkiHeader* hdr, uint16_t channel_id, const char* export_path, const char* export_name, uint16_t op_id,
                   const uint8_t* data, uint16_t data_len) {
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
        wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
    };
    auto send_buffered_resp = [&](void* resp_buf, uint16_t resp_len) {
        if (resp_buf != nullptr && resp_len >= sizeof(DevOpRespPayload)) {
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
            resp->reserved = REQ_COOKIE;
            perf_record_vfs_server_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::REPLY_SEND), hdr->src_node, channel_id,
                                         CORRELATION, resp->status, resp_len, CALLSITE);
        }
        wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf, resp_len);
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
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
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
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
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
            std::array<char, 512> full_path{};
            std::array<char, 512> full_visible_path{};
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
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
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
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            // Allocate a remote FD
            s_vfs_lock.lock();
            int32_t fd_id = alloc_remote_fd(hdr->src_node, channel_id, file);
            s_vfs_lock.unlock();

            OpenRespPayload open_resp = {};
            open_resp.fd = fd_id;
            open_resp.is_dir = file->is_directory ? 1 : 0;

            ker::vfs::Stat open_stat = {};
            if (ker::vfs::vfs_stat_resolved(full_path.data(), &open_stat) == 0) {
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
                    uint8_t* prefetch_buf = wki_dev_server_get_vfs_bulk_staging_buf(hdr->src_node, channel_id);
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

            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf.data(),
                     static_cast<uint16_t>(sizeof(DevOpRespPayload) + OPEN_DATA_LEN));

#ifdef DEBUG_WKI_VFS
            // Diagnostic: immediately read first 4 bytes at open time to verify file data
            if (file->fops != nullptr && file->fops->vfs_read != nullptr && !file->is_directory) {
                uint8_t probe[4] = {0xEE, 0xEE, 0xEE, 0xEE};
                ssize_t probe_n = file->fops->vfs_read(file, probe, 4, 0);
                ker::mod::dbg::log(
                    "[WKI-SRV] VFS_OPEN: node=0x%04x path='%s' fd=%d fs_type=%d"
                    " probe=[%02x %02x %02x %02x] probe_bytes=%ld",
                    hdr->src_node, full_path.data(), fd_id, static_cast<int>(file->fs_type), probe[0], probe[1], probe[2], probe[3],
                    static_cast<long>(probe_n));
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
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int32_t fd_id = 0;
            uint32_t len = 0;
            int64_t offset = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&len, data + 4, sizeof(uint32_t));
            memcpy(&offset, data + 8, sizeof(int64_t));

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_read == nullptr) {
                s_vfs_lock.unlock();
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ;
                resp.status = -EBADF;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            // Clamp to max response payload
            auto max_resp_data = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload));
            len = std::min<uint32_t>(len, max_resp_data);

            auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + len);
            auto* resp_buf = new (std::nothrow) uint8_t[resp_total];
            if (resp_buf == nullptr) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ;
                resp.status = -ENOMEM;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
            uint8_t* read_buf = resp_buf + sizeof(DevOpRespPayload);

            // Pre-fill buffer with a sentinel pattern to detect whether vfs_read actually writes
            std::memset(read_buf, 0xAA, len);

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
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf, SEND_LEN);
            delete[] resp_buf;
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
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
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
            // Request: {remote_fd:i32} = 4 bytes
            if (data_len < 4) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_CLOSE;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int32_t fd_id = 0;
            memcpy(&fd_id, data, sizeof(int32_t));

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            touch_remote_fd(rfd);
            ker::vfs::File* close_file = nullptr;
            perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
            uint64_t const LOCAL_STARTED_US = wki_now_us();
            int16_t status = -1;
            if (rfd != nullptr && rfd->file != nullptr) {
                close_file = rfd->file;
                rfd->file = nullptr;
                rfd->active = false;
                status = 0;
            }
            std::erase_if(g_remote_fds, [](const RemoteVfsFd& r) { return !r.active; });
            s_vfs_lock.unlock();

            if (close_file != nullptr) {
                if (close_file->fops != nullptr && close_file->fops->vfs_close != nullptr) {
                    close_file->fops->vfs_close(close_file);
                }
                delete close_file;
            }
            perf_record_vfs_server_end(SERVER_OP, hdr->src_node, channel_id, CORRELATION, status,
                                       static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US), 0, CALLSITE);

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_CLOSE;
            resp.status = status;
            resp.data_len = 0;
            send_simple_resp(resp);
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
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
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
            constexpr auto HARD_MAX =
                static_cast<uint32_t>((WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload) - sizeof(uint32_t)) / sizeof(ker::vfs::DirEntry));
            if (max_count == 0 || max_count > HARD_MAX) {
                max_count = HARD_MAX;
            }

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
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

            // Allocate response: DevOpRespPayload + {count:u32} + max_count×DirEntry
            auto data_size = static_cast<uint32_t>(sizeof(uint32_t) + (max_count * sizeof(ker::vfs::DirEntry)));
            auto resp_total = static_cast<uint32_t>(sizeof(DevOpRespPayload) + data_size);
            auto* resp_buf = new (std::nothrow) uint8_t[resp_total];
            if (resp_buf == nullptr) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR_BATCH;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }
            memset(resp_buf, 0, resp_total);

            perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
            uint64_t const LOCAL_STARTED_US = wki_now_us();
            uint8_t* entries_base = resp_buf + sizeof(DevOpRespPayload) + sizeof(uint32_t);
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

            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
            resp->op_id = OP_VFS_READDIR_BATCH;
            resp->status = 0;
            resp->data_len = static_cast<uint16_t>(sizeof(uint32_t) + (count * sizeof(ker::vfs::DirEntry)));
            memcpy(resp_buf + sizeof(DevOpRespPayload), &count, sizeof(uint32_t));
            perf_record_vfs_server_end(SERVER_OP, hdr->src_node, channel_id, CORRELATION, 0,
                                       static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US),
                                       sizeof(uint32_t) + (count * sizeof(ker::vfs::DirEntry)), CALLSITE);

            auto send_len = static_cast<uint16_t>(sizeof(DevOpRespPayload) + sizeof(uint32_t) + (count * sizeof(ker::vfs::DirEntry)));
            send_buffered_resp(resp_buf, send_len);
            delete[] resp_buf;
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

            std::array<char, 512> full_path{};
            std::array<char, 512> full_visible_path{};
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
                auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + sizeof(ker::vfs::Stat));
                auto* resp_buf = new (std::nothrow) uint8_t[resp_total];
                if (resp_buf == nullptr) {
                    DevOpRespPayload resp = {};
                    resp.op_id = OP_VFS_STAT;
                    resp.status = -1;
                    resp.data_len = 0;
                    send_simple_resp(resp);
                    break;
                }

                auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
                resp->op_id = OP_VFS_STAT;
                resp->status = 0;
                resp->data_len = sizeof(ker::vfs::Stat);
                memcpy(resp_buf + sizeof(DevOpRespPayload), &statbuf, sizeof(ker::vfs::Stat));
                send_buffered_resp(resp_buf, resp_total);
                delete[] resp_buf;
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

            std::array<char, 512> full_path{};
            std::array<char, 512> full_visible_path{};
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
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
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
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            std::array<char, 512> full_path{};
            std::array<char, 512> full_visible_path{};
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 2), path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(data + 2),
                            path_len);

            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READLINK;
                resp.status = -EPERM;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
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
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            } else {
                // Response: {target_len:u16, target[]}
                auto resp_data_len = static_cast<uint16_t>(2 + TARGET_LEN);
                auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + resp_data_len);
                auto* resp_buf = new (std::nothrow) uint8_t[resp_total];
                if (resp_buf == nullptr) {
                    DevOpRespPayload resp = {};
                    resp.op_id = OP_VFS_READLINK;
                    resp.status = -1;
                    resp.data_len = 0;
                    resp.reserved = REQ_COOKIE;
                    wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                    break;
                }

                auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
                resp->op_id = OP_VFS_READLINK;
                resp->status = 0;
                resp->data_len = resp_data_len;
                resp->reserved = REQ_COOKIE;

                auto tlen = static_cast<uint16_t>(TARGET_LEN);
                memcpy(resp_buf + sizeof(DevOpRespPayload), &tlen, sizeof(uint16_t));
                memcpy(resp_buf + sizeof(DevOpRespPayload) + 2, target_buf.data(), TARGET_LEN);

                perf_record_vfs_server_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::REPLY_SEND), hdr->src_node,
                                             channel_id, CORRELATION, 0, resp_total, CALLSITE);
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf, resp_total);
                delete[] resp_buf;
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
            std::array<char, 512> full_link{};
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

            int const RET = ker::vfs::vfs_symlink(target_str.data(), full_link.data());

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

            std::array<char, 512> full_path{};
            std::array<char, 512> full_visible_path{};
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

            std::array<char, 512> full_path{};
            std::array<char, 512> full_visible_path{};
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

            std::array<char, 512> old_full{};
            build_full_path(old_full.data(), old_full.size(), export_path, reinterpret_cast<const char*>(data + 2), old_len);

            std::array<char, 512> new_full{};
            build_full_path(new_full.data(), new_full.size(), export_path, reinterpret_cast<const char*>(data + 4 + old_len), new_len);

            if (path_crosses_recursive_wki_boundary(old_full.data()) || path_crosses_recursive_wki_boundary(new_full.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RENAME;
                resp.status = -EPERM;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int const RET = ker::vfs::vfs_rename(old_full.data(), new_full.data());

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_RENAME;
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
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
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
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
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
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int32_t fd_id = 0;
            int64_t offset = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&offset, data + 4, sizeof(int64_t));

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
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
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf.data(), static_cast<uint16_t>(sizeof(DevOpRespPayload) + 8));
            break;
        }

        case OP_VFS_READ_RDMA: {
            // Request: {fd:i32, len:u32, off:i64, consumer_rkey:u32} = 20 bytes
            // Two modes depending on transport:
            //   Push (ivshmem): rdma_write data to consumer's bounce buf (consumer_rkey), send tiny resp.
            //   Pull (RoCE):    stage data in server's read staging buf; consumer rdma_reads after resp.
            auto send_rdma_read_err = [&](int16_t status = -EIO) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ_RDMA;
                resp.status = status;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
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
            uint8_t* read_staging = PULL_MODE ? wki_dev_server_get_vfs_read_staging_buf(hdr->src_node, channel_id) : nullptr;
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
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_read == nullptr) {
                s_vfs_lock.unlock();
                send_rdma_read_err(-EBADF);
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            ssize_t bytes_read = 0;
            if (read_staging != nullptr) {
                // Pull mode: read directly into server staging; client will rdma_read from it.
                bytes_read = read_local_file_windowed(local_file, read_staging, len, static_cast<size_t>(offset));
            } else {
                auto* read_buf = new (std::nothrow) uint8_t[len];
                if (read_buf == nullptr) {
                    send_rdma_read_err(-ENOMEM);
                    break;
                }

                bytes_read = read_local_file_windowed(local_file, read_buf, len, static_cast<size_t>(offset));
                if (bytes_read > 0) {
                    int write_ret = 0;
                    if (transport_is_roce(rdma_peer->rdma_transport)) {
                        write_ret = wki_roce_rdma_write_tagged(hdr->src_node, consumer_rkey, 0, read_buf, static_cast<uint32_t>(bytes_read),
                                                               REQ_COOKIE);
                    } else {
                        write_ret = rdma_peer->rdma_transport->rdma_write(rdma_peer->rdma_transport, hdr->src_node, consumer_rkey, 0,
                                                                          read_buf, static_cast<uint32_t>(bytes_read));
                    }
                    if (write_ret != 0) {
                        bytes_read = -EIO;
                    }
                }
                delete[] read_buf;
            }

            // Response: {bytes_read:u32} = 4 bytes (data is now in consumer bounce buf)
            uint32_t br = (bytes_read > 0) ? static_cast<uint32_t>(bytes_read) : 0;
            std::array<uint8_t, sizeof(DevOpRespPayload) + 4> resp_buf{};
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_VFS_READ_RDMA;
            resp->status = (bytes_read >= 0) ? 0 : static_cast<int16_t>(bytes_read);
            resp->data_len = 4;
            resp->reserved = REQ_COOKIE;
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &br, sizeof(uint32_t));
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf.data(), static_cast<uint16_t>(resp_buf.size()));
            break;
        }

        case OP_VFS_READ_BULK: {
            // Bulk RDMA read - identical request layout to OP_VFS_READ_RDMA but len
            // is not capped to 64 KB.
            // Two modes:
            //   Push (ivshmem): rdma_write up to 2 MB into consumer's bulk buf (consumer_rkey).
            //   Pull (RoCE):    stage data in server's bulk staging buf; client rdma_reads after resp.
            auto send_bulk_read_err = [&](int16_t status = -EIO) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ_BULK;
                resp.status = status;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
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

            // Cap to the consumer's registered bulk size. RoCE pull mode uses
            // a smaller server staging window than the ivshmem push path.
            len = std::min<uint32_t>(len, VFS_RDMA_BULK_SIZE);

            // Determine mode from the request rkey. rkey=0 keeps the legacy
            // pull path where the server stages data and the client rdma_reads
            // it; nonzero asks the server to push into the consumer region.
            bool const PULL_MODE = consumer_rkey == 0;
            uint8_t* bulk_staging = PULL_MODE ? wki_dev_server_get_vfs_bulk_staging_buf(hdr->src_node, channel_id) : nullptr;
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
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_read == nullptr) {
                s_vfs_lock.unlock();
                send_bulk_read_err(-EBADF);
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            ssize_t bytes_read = 0;
            if (bulk_staging != nullptr) {
                // Pull mode: read directly into server staging; client will rdma_read from it.
                bytes_read = read_local_file_windowed(local_file, bulk_staging, len, static_cast<size_t>(offset));
            } else {
                auto* read_buf = new (std::nothrow) uint8_t[len];
                if (read_buf == nullptr) {
                    send_bulk_read_err(-ENOMEM);
                    break;
                }

                bytes_read = read_local_file_windowed(local_file, read_buf, len, static_cast<size_t>(offset));
                if (bytes_read > 0) {
                    int write_ret = 0;
                    if (transport_is_roce(bulk_peer->rdma_transport)) {
                        write_ret = wki_roce_rdma_write_tagged(hdr->src_node, consumer_rkey, 0, read_buf, static_cast<uint32_t>(bytes_read),
                                                               REQ_COOKIE);
                    } else {
                        write_ret = bulk_peer->rdma_transport->rdma_write(bulk_peer->rdma_transport, hdr->src_node, consumer_rkey, 0,
                                                                          read_buf, static_cast<uint32_t>(bytes_read));
                    }
                    if (write_ret != 0) {
                        bytes_read = -EIO;
                    }
                }
                delete[] read_buf;
            }

            uint32_t br = (bytes_read > 0) ? static_cast<uint32_t>(bytes_read) : 0;
            std::array<uint8_t, sizeof(DevOpRespPayload) + 4> bulk_resp_buf{};
            auto* bulk_resp = reinterpret_cast<DevOpRespPayload*>(bulk_resp_buf.data());
            bulk_resp->op_id = OP_VFS_READ_BULK;
            bulk_resp->status = (bytes_read >= 0) ? 0 : static_cast<int16_t>(bytes_read);
            bulk_resp->data_len = 4;
            bulk_resp->reserved = REQ_COOKIE;
            memcpy(bulk_resp_buf.data() + sizeof(DevOpRespPayload), &br, sizeof(uint32_t));
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, bulk_resp_buf.data(), static_cast<uint16_t>(bulk_resp_buf.size()));
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
                wki_dev_server_complete_vfs_write(hdr->src_node, channel_id, REQ_COOKIE, status, 0);
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

            VfsWriteRegionInfo const WRITE_REGION = wki_dev_server_get_vfs_write_region(hdr->src_node, channel_id);
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
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
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
            wki_dev_server_complete_vfs_write(hdr->src_node, channel_id, REQ_COOKIE, resp->status, bw);
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

auto wki_remote_vfs_mount(uint16_t owner_node, uint32_t resource_id, const char* local_mount_path) -> int {
    if (local_mount_path == nullptr) {
        return -EINVAL;
    }

    // Allocate proxy state
    uint8_t attach_cookie = 0;
    s_vfs_lock.lock();
    g_vfs_proxies.push_back(std::make_unique<ProxyVfsState>());
    auto* state = g_vfs_proxies.back().get();

    state->owner_node = owner_node;
    state->resource_id = resource_id;

    size_t path_len = strlen(local_mount_path);
    if (path_len >= VFS_EXPORT_PATH_LEN) {
        path_len = VFS_EXPORT_PATH_LEN - 1;
    }
    memcpy(static_cast<void*>(state->local_mount_path.data()), local_mount_path, path_len);
    *std::next(state->local_mount_path.begin(), static_cast<ptrdiff_t>(path_len)) = '\0';

    WkiWaitEntry wait = {};
    attach_cookie = allocate_vfs_attach_cookie_locked();
    state->attach_wait_entry = &wait;
    state->attach_expected_cookie = attach_cookie;
    state->attach_pending.store(true, std::memory_order_release);
    state->attach_status = 0;
    state->attach_channel = 0;
    s_vfs_lock.unlock();

    // Send DEV_ATTACH_REQ
    DevAttachReqPayload attach_req = {};
    attach_req.target_node = owner_node;
    attach_req.resource_type = static_cast<uint16_t>(ResourceType::VFS);
    attach_req.resource_id = resource_id;
    attach_req.attach_mode = static_cast<uint8_t>(AttachMode::PROXY);
    attach_req.attach_cookie = attach_cookie;

    WkiChannel* reserved_channel = nullptr;
    if (wki_requester_controls_dynamic_channel(g_wki.my_node_id, owner_node)) {
        // Remote VFS RPCs are stop-and-wait. Use latency ACKs so a long server
        // write does not look like a lost control frame to the sender.
        reserved_channel = wki_channel_alloc(owner_node, PriorityClass::LATENCY);
        if (reserved_channel == nullptr) {
            cancel_proxy_attach_wait(state, wait, -ENOMEM);
            s_vfs_lock.lock();
            g_vfs_proxies.pop_back();
            s_vfs_lock.unlock();
            return -ENOMEM;
        }
        attach_req.requested_channel = reserved_channel->channel_id;
    } else {
        attach_req.requested_channel = 0;
    }

    int const SEND_RET = wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ, &attach_req, sizeof(attach_req));
    if (SEND_RET != WKI_OK) {
        cancel_proxy_attach_wait(state, wait, SEND_RET);
        if (reserved_channel != nullptr) {
            wki_channel_close(reserved_channel);
        }
        s_vfs_lock.lock();
        g_vfs_proxies.pop_back();
        s_vfs_lock.unlock();
        return SEND_RET;
    }

    uint64_t const ATTACH_STARTED_US = wki_now_us();
    int const WAIT_RC = wki_wait_for_op(&wait, VFS_PROXY_OP_TIMEOUT_US);
    s_vfs_lock.lock();
    if (WAIT_RC == 0 && state->attach_wait_entry == &wait) {
        state->attach_wait_entry = nullptr;
        clear_proxy_attach_state_locked(state, static_cast<uint8_t>(DevAttachStatus::BUSY));
    }
    uint16_t const ATTACH_CHANNEL = state->attach_channel;
    s_vfs_lock.unlock();
    perf_record_vfs_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::ATTACH_WAIT), owner_node, ATTACH_CHANNEL, WAIT_RC,
                          static_cast<uint32_t>(wki_now_us() - ATTACH_STARTED_US), WOS_PERF_CALLSITE());
    if (WAIT_RC != 0) {
        cancel_proxy_attach_wait(state, wait, WAIT_RC);
        if (reserved_channel != nullptr) {
            wki_channel_close(reserved_channel);
        }
        s_vfs_lock.lock();
        g_vfs_proxies.pop_back();
        s_vfs_lock.unlock();
        if (WAIT_RC == WKI_ERR_TIMEOUT) {
            ker::mod::dbg::log("[WKI] Remote VFS attach timeout: node=0x%04x res_id=%u", owner_node, resource_id);
        } else {
            ker::mod::dbg::log("[WKI] Remote VFS attach aborted: node=0x%04x res_id=%u rc=%d", owner_node, resource_id, WAIT_RC);
        }
        return WAIT_RC;
    }

    if (state->attach_status != static_cast<uint8_t>(DevAttachStatus::OK)) {
        int const ATTACH_ERRNO = dev_attach_status_to_errno(state->attach_status);
        ker::mod::dbg::log("[WKI] Remote VFS attach rejected: node=0x%04x res_id=%u path=%s status=%u ret=%d", owner_node, resource_id,
                           local_mount_path, state->attach_status, ATTACH_ERRNO);
        if (reserved_channel != nullptr) {
            wki_channel_close(reserved_channel);
        }
        s_vfs_lock.lock();
        g_vfs_proxies.pop_back();
        s_vfs_lock.unlock();
        return ATTACH_ERRNO;
    }

    if (reserved_channel != nullptr) {
        if (state->attach_channel != reserved_channel->channel_id) {
            ker::mod::dbg::log("[WKI] Remote VFS attach channel mismatch: node=0x%04x res_id=%u requested=%u assigned=%u", owner_node,
                               resource_id, reserved_channel->channel_id, state->attach_channel);
            DevDetachPayload det = {};
            det.target_node = owner_node;
            det.resource_type = static_cast<uint16_t>(ResourceType::VFS);
            det.resource_id = resource_id;
            wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));
            wki_channel_close(reserved_channel);
            s_vfs_lock.lock();
            g_vfs_proxies.pop_back();
            s_vfs_lock.unlock();
            return -EIO;
        }
    } else {
        reserved_channel = wki_channel_reserve(owner_node, state->attach_channel, PriorityClass::LATENCY);
        if (reserved_channel == nullptr) {
            ker::mod::dbg::log("[WKI] Remote VFS attach local reserve failed: node=0x%04x res_id=%u ch=%u", owner_node, resource_id,
                               state->attach_channel);
            DevDetachPayload det = {};
            det.target_node = owner_node;
            det.resource_type = static_cast<uint16_t>(ResourceType::VFS);
            det.resource_id = resource_id;
            wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));
            s_vfs_lock.lock();
            g_vfs_proxies.pop_back();
            s_vfs_lock.unlock();
            return -EIO;
        }
    }

    s_vfs_lock.lock();
    state->assigned_channel = reserved_channel->channel_id;
    state->max_op_size = state->attach_max_op_size;
    state->active = true;
    s_vfs_lock.unlock();

    // RDMA setup: bind the peer transport once, then enable read, write, and
    // bulk capabilities independently. Remote-root reads must not depend on the
    // server also having a write-receive buffer.
    WkiPeer const* peer = wki_peer_find(owner_node);
    if (peer != nullptr && peer->rdma_transport != nullptr && peer->rdma_transport->rdma_register_region != nullptr) {
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
                delete[] bulk_buf;
                ker::mod::dbg::log("[WKI] VFS bulk RDMA region reg failed: node=0x%04x ret=%d size=%u", owner_node, REG_RET, BULK_SIZE);
            }
        } else {
            ker::mod::dbg::log("[WKI] VFS bulk RDMA allocation failed: node=0x%04x size=%u", owner_node, BULK_SIZE);
        }
    }

    // Create the mount point with "remote" fstype
    int const MOUNT_RET = ker::vfs::mount_filesystem(local_mount_path, "remote", nullptr);
    if (MOUNT_RET != 0) {
        ker::mod::dbg::log("[WKI] Remote VFS mount failed at %s", local_mount_path);
        discard_failed_attached_proxy(state, reserved_channel);
        return MOUNT_RET;
    }

    // Configure the exact REMOTE mount we just created.  During pivot_root(),
    // mount_filesystem() and this lookup can observe different task roots, so
    // try both the current-root-resolved path and the raw mount path.  Never use
    // longest-prefix lookup here: if the exact mount is temporarily invisible,
    // that can return the containing XFS root and corrupt its private_data.
    std::array<char, 512> resolved_mount{};
    bool configured = false;
    if (ker::vfs::resolve_mount_path(local_mount_path, resolved_mount.data(), resolved_mount.size()) == 0) {
        configured = ker::vfs::configure_mount_point_exact(resolved_mount.data(), ker::vfs::FSType::REMOTE, state, &g_remote_vfs_fops);
    }
    if (!configured) {
        configured = ker::vfs::configure_mount_point_exact(local_mount_path, ker::vfs::FSType::REMOTE, state, &g_remote_vfs_fops);
    }
    if (!configured) {
        ker::mod::dbg::log("[WKI] Remote VFS mount configuration failed at %s", local_mount_path);
        ker::vfs::unmount_filesystem(local_mount_path);
        discard_failed_attached_proxy(state, reserved_channel);
        return -EIO;
    }

    ker::mod::dbg::log("[WKI] Remote VFS mounted: %s -> node=0x%04x res_id=%u ch=%u", local_mount_path, owner_node, resource_id,
                       state->assigned_channel);
    return 0;
}

void wki_remote_vfs_unmount(const char* local_mount_path) {
    if (local_mount_path == nullptr) {
        return;
    }

    PendingProxyTeardown teardown = {};
    s_vfs_lock.lock();
    auto* state = find_vfs_proxy_by_mount(local_mount_path);
    if (state == nullptr) {
        s_vfs_lock.unlock();
        return;
    }
    deactivate_vfs_proxy_locked(state, teardown, true);
    invalidate_all_dir_caches(state);
    s_vfs_lock.unlock();

    ker::vfs::vfs_stream_cache_invalidate_remote_scope(teardown.state);
    invalidate_readlink_cache(teardown.state);

    if (teardown.had_op_pending) {
        ker::mod::dbg::log("[WKI] VFS op UNMOUNT: node=0x%04x ch=%u op=%u seq=%u mount=%s", teardown.owner_node, teardown.assigned_channel,
                           teardown.op_expected_id, teardown.op_expected_seq, teardown.local_mount_path.data());
    }

    finish_claimed_waiter(teardown.op_wait_entry, -1);
    finish_claimed_waiter(teardown.attach_wait_entry, -1);

    // Send DEV_DETACH
    DevDetachPayload det = {};
    det.target_node = teardown.owner_node;
    det.resource_type = static_cast<uint16_t>(ResourceType::VFS);
    det.resource_id = teardown.resource_id;
    wki_send(teardown.owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));

    // Close the dynamic channel
    WkiChannel* ch = wki_channel_get(teardown.owner_node, teardown.assigned_channel);
    if (ch != nullptr) {
        wki_channel_close(ch);
    }

    // Unmount
    ker::vfs::unmount_filesystem(local_mount_path);
    mark_vfs_proxy_mount_released_and_maybe_destroy(teardown.state);
}

auto wki_remote_vfs_proxy_diag_snapshot(WkiRemoteVfsProxyDiag* out, size_t max) -> size_t {
    if (out == nullptr || max == 0) {
        return 0;
    }

    size_t count = 0;
    s_vfs_lock.lock();
    for (const auto& proxy : g_vfs_proxies) {
        if (proxy == nullptr || !proxy->active || count >= max) {
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
        if (!proxy->active || proxy->owner_node != owner_node || proxy->resource_id != resource_id) {
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
        if (!proxy->active || proxy->owner_node != owner_node) {
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
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || fs_relative_path == nullptr) {
        return nullptr;
    }
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
    if (PATH_LEN > UINT16_MAX - REQ_FIXED_LEN) {
        return nullptr;
    }
    auto path_len = static_cast<uint16_t>(PATH_LEN);
    auto req_data_len = static_cast<uint16_t>(OPEN_REQ_BASE_LEN + path_len + (send_open_prefetch ? OPEN_PREFETCH_REQ_LEN : 0));
    auto* req_data = new (std::nothrow) uint8_t[req_data_len];
    if (req_data == nullptr) {
        return nullptr;
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

    int const STATUS = vfs_proxy_send_and_wait(state, OP_VFS_OPEN, req_data, req_data_len, &open_resp, sizeof(open_resp), &open_resp_len,
                                               VFS_PROXY_OP_TIMEOUT_US, tagged_receive_ptr);
    delete[] req_data;

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

auto wki_remote_vfs_stat(void* mount_private_data, const char* fs_relative_path, ker::vfs::Stat* statbuf) -> int {
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active || fs_relative_path == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }

    // Build request: {path_len:u16, path[N]}
    std::array<uint8_t, 514> req_stack{};  // NOLINT(modernize-avoid-c-arrays)
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
    int const STATUS = wki_remote_vfs_stat(mount->private_data, remote_vfs_strip_mount_prefix(mount, file->vfs_path), &fresh);
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
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active || fs_relative_path == nullptr) {
        return -EINVAL;
    }

    // Build request: {mode:u32, path_len:u16, path[N]}
    std::array<uint8_t, 518> req_stack{};  // NOLINT(modernize-avoid-c-arrays)
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
        invalidate_readlink_cache(state);
    }
    return STATUS;
}

// Consumer side: create a symlink on the remote server
auto wki_remote_vfs_symlink(void* mount_private_data, const char* target, const char* fs_relative_path) -> int {
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active || target == nullptr || fs_relative_path == nullptr) {
        return -EINVAL;
    }

    // Server expects: {target_len:u16, target[], link_len:u16, link[]}
    std::array<uint8_t, 1028> req_stack{};
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
        invalidate_readlink_cache(state);
    }
    return STATUS;
}

// Consumer side: unlink a file on the remote server
auto wki_remote_vfs_unlink(void* mount_private_data, const char* fs_relative_path) -> int {
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active || fs_relative_path == nullptr) {
        return -EINVAL;
    }

    std::array<uint8_t, 514> req_stack{};
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
        invalidate_readlink_cache(state);
    }
    return STATUS;
}

// Consumer side: remove a directory on the remote server
auto wki_remote_vfs_rmdir(void* mount_private_data, const char* fs_relative_path) -> int {
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active || fs_relative_path == nullptr) {
        return -EINVAL;
    }

    std::array<uint8_t, 514> req_stack{};
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
        invalidate_readlink_cache(state);
    }
    return STATUS;
}

// Consumer side: rename a file/directory on the remote server
auto wki_remote_vfs_rename(void* mount_private_data, const char* old_fs_path, const char* new_fs_path) -> int {
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active || old_fs_path == nullptr || new_fs_path == nullptr) {
        return -EINVAL;
    }

    std::array<uint8_t, 1028> req_stack{};
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
        invalidate_readlink_cache(state);
    }
    return STATUS;
}

// Consumer side: readlink on a remote path (for vfs_readlink / resolve_symlinks)
auto wki_remote_vfs_readlink_path(void* mount_private_data, const char* fs_relative_path, char* buf, size_t bufsize) -> ssize_t {
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active || fs_relative_path == nullptr || buf == nullptr || bufsize == 0) {
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
    if (try_readlink_cache_lookup(state, fs_relative_path, buf, bufsize, &cached_result)) {
        return cached_result;
    }

    // Build request: {path_len:u16, path[N]}
    std::array<uint8_t, 514> req_stack{};
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
    std::array<uint8_t, 514> resp_buf{};
    uint16_t resp_len = 0;
    int const STATUS = vfs_proxy_read_with_retry(state, OP_VFS_READLINK, req_data, req_data_len, resp_buf.data(),
                                                 static_cast<uint16_t>(resp_buf.size()), &resp_len, fs_relative_path);
    if (STATUS != 0) {
        if (STATUS == -ENOSYS) {
            state->readlink_unsupported.store(true, std::memory_order_release);
            return STATUS;
        }

        if (STATUS == -EINVAL || STATUS == -ENOENT) {
            cache_readlink_result(state, fs_relative_path, STATUS, nullptr, 0);
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
    cache_readlink_result(state, fs_relative_path, 0, reinterpret_cast<const char*>(resp_buf.data() + 2), target_len);
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

    auto notify_one = [&](const char* local_path, const char* other_local_path, bool include_other) {
        if (local_path == nullptr || local_path[0] == '\0') {
            return;
        }

        for (const auto& exp : g_vfs_exports) {
            if (!exp.active || !path_matches_export(raw_data(exp.export_path), local_path)) {
                continue;
            }

            const char* old_rel_src = trim_export_prefix(raw_data(exp.export_path), local_path);
            if (old_rel_src == nullptr) {
                continue;
            }

            std::array<char, VFS_EXPORT_PATH_LEN> old_rel{};
            std::array<char, VFS_EXPORT_PATH_LEN> new_rel{};
            std::snprintf(old_rel.data(), old_rel.size(), "%s", old_rel_src);

            if (include_other && other_local_path != nullptr && path_matches_export(raw_data(exp.export_path), other_local_path)) {
                if (const char* new_rel_src = trim_export_prefix(raw_data(exp.export_path), other_local_path); new_rel_src != nullptr) {
                    std::snprintf(new_rel.data(), new_rel.size(), "%s", new_rel_src);
                }
            }

            send_notify(exp, old_rel.data(), new_rel.data());
        }
    };

    if (old_local_vfs_path != nullptr) {
        notify_one(old_local_vfs_path, new_local_vfs_path, true);
    }
    if (new_local_vfs_path != nullptr && (old_local_vfs_path == nullptr || std::strcmp(old_local_vfs_path, new_local_vfs_path) != 0)) {
        notify_one(new_local_vfs_path, nullptr, false);
    }
}

// -------------------------------------------------------------------------------
// Consumer Side - RX Handlers
// -------------------------------------------------------------------------------

namespace detail {

void handle_vfs_invalidate_notify(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (hdr == nullptr || payload == nullptr || payload_len < 4) {
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

    s_vfs_lock.lock();
    ProxyVfsState* state = find_vfs_proxy_by_channel(hdr->src_node, hdr->channel_id);
    s_vfs_lock.unlock();
    if (state == nullptr) {
        return;
    }

    auto invalidate_path = [&](const char* rel_path, uint16_t rel_len) {
        std::array<char, 512> full_path{};
        build_full_path(full_path.data(), full_path.size(), state->local_mount_path.data(), rel_path, rel_len);
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

    ker::vfs::vfs_stream_cache_invalidate_remote_scope(state);
    s_vfs_lock.lock();
    invalidate_all_dir_caches(state);
    s_vfs_lock.unlock();
    invalidate_readlink_cache(state);
}

void handle_vfs_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevAttachAckPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const DevAttachAckPayload*>(payload);

    s_vfs_lock.lock();
    ProxyVfsState* state = find_vfs_proxy_by_attach(hdr->src_node, ack->resource_id);
    if (state == nullptr) {
        s_vfs_lock.unlock();
        return;
    }
    if (!vfs_attach_ack_matches_pending_locked(state, *ack)) {
        s_vfs_lock.unlock();
        return;
    }

    WkiWaitEntry* wait_entry = nullptr;
    wait_entry = claim_and_clear_waiter_locked(state->attach_wait_entry);
    if (wait_entry != nullptr) {
        state->attach_status = ack->status;
        state->attach_channel = ack->assigned_channel;
        state->attach_max_op_size = ack->max_op_size;

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
    if (vfs_attach_ack_matches_pending_locked(&state, ack)) {
        return false;
    }

    ack.reserved = 0x52;
    if (!vfs_attach_ack_matches_pending_locked(&state, ack)) {
        return false;
    }

    state.attach_expected_cookie = 0;
    if (vfs_attach_ack_matches_pending_locked(&state, ack)) {
        return false;
    }

    state.attach_expected_cookie = 0x52;
    state.attach_pending.store(false, std::memory_order_release);
    return !vfs_attach_ack_matches_pending_locked(&state, ack);
}

namespace detail {

void handle_vfs_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevOpRespPayload)) {
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
    VfsProxyResponseLookup const LOOKUP = find_vfs_proxy_for_response_locked(hdr->src_node, hdr->channel_id, resp->op_id, resp->reserved);
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
    wait_entry = claim_and_clear_waiter_locked(state->op_wait_entry);
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
    // Do NOT clear op_pending here. The waiter's success teardown in
    // vfs_proxy_send_and_wait will clear it while holding state->lock after
    // consuming the result. Clearing it here (outside state->lock) opens a
    // window where a new request can steal the slot and overwrite
    // op_expected_id/seq before the original waiter's teardown runs.
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
    bool any_removed = false;
    std::deque<ker::vfs::File*> files_to_close;

    s_vfs_lock.lock();
    for (auto& rfd : g_remote_fds) {
        if (!rfd.active) {
            continue;
        }

        // Only GC if the FD has been idle for a long time
        if (NOW - rfd.last_activity_us < STALE_FD_TIMEOUT_US) {
            continue;
        }

        // Only GC if the consumer peer is NOT connected (crashed/fenced without closing)
        WkiPeer const* peer = wki_peer_find(rfd.consumer_node);
        if (peer != nullptr && peer->state == PeerState::CONNECTED) {
            continue;
        }

        // Stale: detach the file under the VFS lock, then close it afterwards.
        if (rfd.file != nullptr) {
            files_to_close.push_back(rfd.file);
            rfd.file = nullptr;
        }
        rfd.active = false;
        any_removed = true;
        ker::mod::dbg::log("[WKI] GC stale remote FD %d (consumer 0x%04x)", rfd.fd_id, rfd.consumer_node);
    }

    if (any_removed) {
        std::erase_if(g_remote_fds, [](const RemoteVfsFd& rfd) { return !rfd.active; });
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

        // Check if this visible export name is already exported.
        bool already_exported = false;
        for (const auto& exp : g_vfs_exports) {
            if (exp.active && strncmp(raw_data(exp.name), export_name.data(), export_name.size()) == 0) {
                already_exported = true;
                break;
            }
        }
        if (already_exported) {
            continue;
        }

        // Export and advertise the visible path in the caller's current root
        // namespace. After pivot_root("/rootfs", ...), this yields "/",
        // "/boot", and "/oldroot", which resolve correctly for all tasks whose
        // root has been updated to "/rootfs".
        uint32_t const PRESERVED_RESOURCE_ID = take_preserved_export_id(stale_exports, export_name.data());
        uint32_t const RESOURCE_ID = wki_remote_vfs_export_add_internal(mount_path, export_name.data(), PRESERVED_RESOURCE_ID);
        if (RESOURCE_ID != 0) {
            wki_dev_server_refresh_vfs_binding(RESOURCE_ID, mount_path, export_name.data());
        }
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

void wki_remote_vfs_rebuild_exports() {
    if (!g_remote_vfs_initialized) {
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
    s_vfs_lock.unlock();

    wki_remote_vfs_auto_discover_internal(&stale_exports);

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
            wki_send(peer->node_id, WKI_CHAN_CONTROL, MsgType::RESOURCE_WITHDRAW, &withdraw, sizeof(withdraw));
        }
    }

    wki_remote_vfs_advertise_exports();
}

// -------------------------------------------------------------------------------
// Fencing Cleanup
// -------------------------------------------------------------------------------

void wki_remote_vfs_cleanup_for_peer(uint16_t node_id) {
    // Server side: close all remote FDs for this consumer
    std::deque<ker::vfs::File*> files_to_close;
    std::deque<PendingProxyTeardown> proxies_to_cleanup;

    s_vfs_lock.lock();
    for (auto& rfd : g_remote_fds) {
        if (!rfd.active || rfd.consumer_node != node_id) {
            continue;
        }
        if (rfd.file != nullptr) {
            files_to_close.push_back(rfd.file);
            rfd.file = nullptr;
        }
        rfd.active = false;
    }
    std::erase_if(g_remote_fds, [](const RemoteVfsFd& rfd) { return !rfd.active; });

    // Consumer side: fail pending ops and deactivate proxies while the proxy
    // registry is locked.  Cache invalidation and channel close happen later
    // because they can take other subsystem locks.
    for (auto& proxy : g_vfs_proxies) {
        auto* p = proxy.get();
        if (p == nullptr || !p->active || p->owner_node != node_id) {
            continue;
        }

        PendingProxyTeardown cleanup = {};
        deactivate_vfs_proxy_locked(p, cleanup, false);
        invalidate_all_dir_caches(p);

        proxies_to_cleanup.push_back(cleanup);
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

    for (const auto& cleanup : proxies_to_cleanup) {
        ker::vfs::vfs_stream_cache_invalidate_remote_scope(cleanup.state);
        invalidate_readlink_cache(cleanup.state);

        if (cleanup.had_op_pending) {
            ker::mod::dbg::log("[WKI] VFS op PEER_CLEANUP: node=0x%04x ch=%u op=%u seq=%u mount=%s", cleanup.owner_node,
                               cleanup.assigned_channel, cleanup.op_expected_id, cleanup.op_expected_seq, cleanup.local_mount_path.data());
        }

        finish_claimed_waiter(cleanup.op_wait_entry, -1);
        finish_claimed_waiter(cleanup.attach_wait_entry, -1);

        WkiChannel* ch = wki_channel_get(cleanup.owner_node, cleanup.assigned_channel);
        if (ch != nullptr) {
            wki_channel_close(ch);
        }

        release_and_maybe_destroy_idle_vfs_proxy(cleanup.state);

        ker::mod::dbg::log("[WKI] Remote VFS proxy cleanup: %s node=0x%04x", cleanup.local_mount_path.data(), node_id);
    }
}

}  // namespace ker::net::wki
