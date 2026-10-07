#include "remote_vfs.hpp"

#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bitset>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <dev/block_device.hpp>
#include <iterator>
#include <map>
#include <memory>
#include <net/wki/auth_crypto.hpp>
#include <net/wki/auth_policy.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/secure_vfs.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/transport_roce.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/mm.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/file_growth.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/credential_record.hpp>
#include <platform/security/file_policy.hpp>
#include <platform/sys/mutex.hpp>
#include <string_view>
#include <util/hcf.hpp>
#include <utility>
#include <vfs/advisory.hpp>
#include <vfs/advisory_receipt.hpp>
#include <vfs/advisory_scope.hpp>
#include <vfs/mount.hpp>
#include <vfs/timestamps.hpp>
#include <vfs/vfs.hpp>

#include "platform/sys/spinlock.hpp"
#include "vfs/file.hpp"
#include "vfs/file_operations.hpp"
#include "vfs/stat.hpp"

namespace ker::net::wki {

struct VfsSecureContext {
    WkiChannelIdentity identity{};
    uint64_t binding_id = 0;
    uint32_t resource_id = 0;
    ResourceIncarnationToken incarnation{};
    uint64_t publication_revision = 0;
    std::unique_ptr<uint8_t[]> write_backing{};
    std::unique_ptr<uint8_t[]> read_backing{};
    rdma_protocol::RegionDescriptor write_grant{};
    rdma_protocol::RegionDescriptor read_grant{};
    std::atomic<bool> closed{false};
    ker::mod::sys::Spinlock loan_lock;
    uint64_t generation = 0;
    bool armed = false;
    bool effect_started = false;
    WkiSecureRdmaRequest request{};
    DevServerBinding* binding = nullptr;
    ker::vfs::File* file = nullptr;
};

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
std::atomic<uint32_t> g_vfs_xattr_session_counter{1};
std::atomic<uint64_t> g_vfs_reply_enqueue_failures{0};
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

// Diagnostic state only; public remote operations obtain fresh owner results.
[[maybe_unused]] auto remote_vfs_try_copy_cached_stat(RemoteFileContext* ctx, ker::vfs::Stat* statbuf) -> bool {
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
static_assert(offsetof(OpenRespPayload, stat) == 6);

void remote_vfs_store_open_stat(RemoteFileContext* ctx, const OpenRespPayload& response) {
    // The wire member starts at byte six. Never bind it to a Stat reference:
    // even an aligned response object cannot provide the member's alignment.
    ker::vfs::Stat stat{};
    std::memcpy(&stat, reinterpret_cast<const uint8_t*>(&response) + offsetof(OpenRespPayload, stat), sizeof(stat));
    remote_vfs_store_cached_stat(ctx, stat);
}

auto scalar_open_response_valid(int status, const uint8_t* data, size_t length, size_t capacity) -> bool {
    if (status < 0) return length == 0;
    if (status != 0 || data == nullptr || length > capacity ||
        (length != OPEN_RESP_NO_STAT_LEN && length != OPEN_RESP_WITH_STAT_LEN && length != OPEN_RESP_WITH_PREFETCH_LEN))
        return false;
    OpenRespPayload reply{};
    std::memcpy(&reply, data, length);
    if (reply.fd < 0 || reply.is_dir > 1 || reply.has_stat > 1) return false;
    if (length == OPEN_RESP_NO_STAT_LEN) return reply.has_stat == 0;
    if (reply.has_stat != 1) return false;
    return length != OPEN_RESP_WITH_PREFETCH_LEN ||
           (reply.is_dir == 0 && reply.prefetched_bytes != 0 && reply.prefetched_bytes <= VFS_RDMA_BULK_SIZE);
}

auto ordinary_open_response_valid(uint16_t inner_op, uint64_t challenge, int status, const uint8_t* data, size_t length, size_t capacity)
    -> bool {
    if (status < 0) return length == 0;
    if (status != 0 || length > capacity) return false;
    if (inner_op == 0) return length == 0;
    if (data == nullptr) return false;
    if (inner_op == OP_VFS_FEATURES) {
        if (length != sizeof(VfsOpenLifetimeRequest)) return false;
        VfsOpenLifetimeRequest response{};
        std::memcpy(&response, data, sizeof(response));
        return wki_vfs_open_binding_response_valid(response, challenge, length);
    }
    if (inner_op == OP_VFS_OPEN_AS || inner_op == OP_VFS_OPEN_READ_AS) {
        if (!scalar_open_response_valid(status, data, length, capacity)) return false;
        int32_t fd = 0;
        std::memcpy(&fd, data, sizeof(fd));
        return fd > 0;
    }
    if (inner_op != OP_VFS_PATH_AS || length != sizeof(VfsPathIdentityHeader) + sizeof(VfsPathOpenResult)) return false;
    VfsPathIdentityHeader header{};
    VfsPathOpenResult opened{};
    std::memcpy(&header, data, sizeof(header));
    std::memcpy(&opened, data + sizeof(header), sizeof(opened));
    return header.operation == VfsPathOperation::OPEN_AT && wki_vfs_path_response_valid(status, length, header) &&
           wki_vfs_path_open_result_valid(opened);
}

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
        case OP_VFS_OPEN_READ_AS:
        case OP_VFS_OPEN_AS:
        case OP_VFS_OPEN:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::OPEN);
        case OP_VFS_STAT:
        case OP_VFS_STAT_AS:
        case OP_VFS_FSTAT:
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
        case OP_VFS_PATH_AS:
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
        case OP_VFS_PATH_AS:
        case OP_VFS_READLINK:
        case OP_VFS_STAT:
        case OP_VFS_STAT_AS:
        case OP_VFS_FSTAT:
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
        case OP_VFS_OPEN_READ_AS:
        case OP_VFS_OPEN_AS:
        case OP_VFS_OPEN:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::OPEN);
        case OP_VFS_STAT:
        case OP_VFS_STAT_AS:
        case OP_VFS_FSTAT:
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
        case OP_VFS_PATH_AS:
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
std::map<int32_t, RemoteVfsFd> g_remote_fds;          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
int32_t g_next_remote_fd = 1;                         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_remote_fd_opens_total = 0;                 // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_remote_fd_close_retirements_total = 0;     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_remote_fd_async_close_completions = 0;     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_remote_fd_async_close_misses = 0;          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_next_vfs_resource_id = 0x1000;             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_next_vfs_resource_incarnation = 1;         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_vfs_export_revision = 2;                   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_vfs_export_target_revision = 0;            // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_vfs_export_rebuild_prepared = false;           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_vfs_export_rebuild_accepting_entries = false;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Retirement is reserved before pivot commits. One record fans out to the
// existing peer slots, which are never recycled during this kernel boot.
// Offline peers retain their obligations; capacity pressure rejects a later
// pivot before mutation instead of silently losing a revocation.
constexpr size_t VFS_WITHDRAWAL_CAPACITY = 256;
using WithdrawalPeers = std::bitset<WKI_MAX_PEERS>;
struct VfsWithdrawalAttempt {
    uint64_t serial = 0;
    uint32_t resource_id = 0;
    ResourceIncarnationToken incarnation{};
    WkiReliableTxToken tx{};
};

class VfsWithdrawalOutbox {
    struct Entry {
        uint64_t serial = 0;
        uint32_t resource_id = 0;
        ResourceIncarnationToken incarnation{};
        WithdrawalPeers peers{};
    };
    struct Delivery {
        uint64_t serial = 0;
        WkiReliableTxToken tx{};
        bool busy = false;
    };
    std::array<Entry, VFS_WITHDRAWAL_CAPACITY> entries{};
    std::array<Delivery, WKI_MAX_PEERS> deliveries{};
    uint64_t next_serial = 1;
    size_t reserved = 0;
    bool reservation_active = false;

   public:
    // All methods run under the caller's outbox mutex. No allocation or I/O.
    auto reserve(size_t count) -> bool {
        size_t const USED = std::ranges::count_if(entries, [](const Entry& entry) { return entry.serial != 0; });
        if (reservation_active || count > entries.size() - USED || count > UINT64_MAX - next_serial) return false;
        reserved = count;
        reservation_active = true;
        return true;
    }

    void cancel_reservation() {
        reserved = 0;
        reservation_active = false;
    }

    auto append(uint32_t resource_id, ResourceIncarnationToken incarnation, const WithdrawalPeers& peers) -> bool {
        if (!reservation_active || reserved == 0 || resource_id == 0 || !wki_resource_incarnation_valid(incarnation)) return false;
        if (peers.none()) {
            --reserved;
            return true;
        }
        for (auto& entry : entries) {
            if (entry.serial != 0) continue;
            entry = {.serial = next_serial++, .resource_id = resource_id, .incarnation = incarnation, .peers = peers};
            --reserved;
            return true;
        }
        return false;
    }

    [[nodiscard]] auto pending(size_t peer) const -> bool {
        return peer < WKI_MAX_PEERS && std::ranges::any_of(entries, [peer](const Entry& entry) { return entry.peers.test(peer); });
    }

    auto claim(size_t peer, VfsWithdrawalAttempt& attempt) -> bool {
        if (peer >= WKI_MAX_PEERS || deliveries.at(peer).busy) return false;
        Entry const* first = nullptr;
        for (auto const& entry : entries) {
            if (entry.peers.test(peer) && (first == nullptr || entry.serial < first->serial)) first = &entry;
        }
        if (first == nullptr) return false;
        auto& delivery = deliveries.at(peer);
        if (delivery.serial != first->serial) delivery = {.serial = first->serial};
        delivery.busy = true;
        attempt = {.serial = first->serial, .resource_id = first->resource_id, .incarnation = first->incarnation, .tx = delivery.tx};
        return true;
    }

    auto finish(size_t peer, const VfsWithdrawalAttempt& attempt, WkiReliableTxStatus status, int send_result,
                const WkiReliableTxToken& replacement) -> bool {
        if (peer >= WKI_MAX_PEERS) return false;
        auto& delivery = deliveries.at(peer);
        if (!delivery.busy || delivery.serial != attempt.serial) return false;
        if (status == WkiReliableTxStatus::ACKED) {
            for (auto& entry : entries) {
                if (entry.serial != attempt.serial) continue;
                entry.peers.reset(peer);
                if (entry.peers.none()) entry = {};
                break;
            }
            delivery = {};
        } else {
            if (status != WkiReliableTxStatus::PENDING) delivery.tx = send_result == WKI_OK ? replacement : WkiReliableTxToken{};
            delivery.busy = false;
        }
        return true;
    }
};

VfsWithdrawalOutbox g_vfs_withdrawals;

auto withdrawal_peer_slot(uint16_t node) -> size_t {
    for (size_t index = 0; index < g_wki.peers.size(); ++index) {
        if (g_wki.peers.at(index).node_id == node) return index;
    }
    return WKI_MAX_PEERS;
}

constexpr size_t VFS_XATTR_SERVER_STAGE_COUNT_MAX = 128;
constexpr size_t VFS_XATTR_SERVER_STAGE_BYTES_MAX = 1024ULL * 1024ULL;
std::atomic<size_t> g_vfs_xattr_stage_bytes{0};

struct VfsXattrStageKey {
    uint16_t peer_node_id = WKI_NODE_INVALID;
    uint16_t channel_id = 0;
    uint32_t channel_generation = 0;

    auto operator<(const VfsXattrStageKey& other) const -> bool {
        if (peer_node_id != other.peer_node_id) {
            return peer_node_id < other.peer_node_id;
        }
        if (channel_id != other.channel_id) {
            return channel_id < other.channel_id;
        }
        return channel_generation < other.channel_generation;
    }
};

struct VfsXattrServerStage {
    ~VfsXattrServerStage() {
        if (file != nullptr) {
            ker::vfs::vfs_put_file(file);
        }
        if (staged_bytes != 0) {
            g_vfs_xattr_stage_bytes.fetch_sub(staged_bytes, std::memory_order_acq_rel);
        }
    }

    VfsXattrPhaseHeader identity = {};
    ker::mod::security::Snapshot credentials;
    ker::mod::security::credential_record::Record caller_record{};
    int32_t remote_fd = -1;
    bool require_directory = false;
    uint32_t flags = 0;
    uint32_t requested_size = 0;
    uint32_t total = 0;
    uint32_t staged_bytes = 0;
    uint32_t transferred = 0;
    bool follow_final_symlink = false;
    bool committed = false;
    int16_t terminal_status = 0;
    std::array<char, VFS_EXPORT_PATH_LEN> export_root = {};
    std::array<char, VFS_EXPORT_PATH_LEN> export_name = {};
    std::array<char, 512> path = {};
    std::array<char, WKI_VFS_XATTR_NAME_MAX + 1> name = {};
    std::unique_ptr<uint8_t[]> data;
    ker::vfs::File* file = nullptr;
    std::atomic<uint32_t> refs{1};
};

std::map<VfsXattrStageKey, VfsXattrServerStage*> g_vfs_xattr_stages;

auto vfs_xattr_stage_key(const WkiChannelIdentity& identity) -> VfsXattrStageKey {
    return {.peer_node_id = identity.peer_node_id, .channel_id = identity.channel_id, .channel_generation = identity.generation};
}

void retain_vfs_xattr_stage(VfsXattrServerStage* stage) {
    if (stage != nullptr) {
        stage->refs.fetch_add(1, std::memory_order_acq_rel);
    }
}

void release_vfs_xattr_stage(VfsXattrServerStage* stage) {
    if (stage != nullptr && stage->refs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        delete stage;
    }
}

struct VfsXattrStageRefGuard {
    ~VfsXattrStageRefGuard() { release_vfs_xattr_stage(stage); }
    VfsXattrServerStage* stage = nullptr;
};

auto reserve_vfs_xattr_stage_bytes(size_t bytes) -> bool {
    size_t current = g_vfs_xattr_stage_bytes.load(std::memory_order_acquire);
    while (bytes <= VFS_XATTR_SERVER_STAGE_BYTES_MAX - current) {
        if (g_vfs_xattr_stage_bytes.compare_exchange_weak(current, current + bytes, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return true;
        }
    }
    return false;
}

// Capture both immutable binding names before replacing a prior operation.
// Failed capture leaves both outputs untouched; stages never borrow these bytes.
auto capture_vfs_xattr_export(const char* path, const char* name, std::array<char, VFS_EXPORT_PATH_LEN>& root,
                              std::array<char, VFS_EXPORT_PATH_LEN>& visible) -> int {
    if (path == nullptr || name == nullptr) return -EINVAL;
    size_t const PATH_LENGTH = strnlen(path, root.size());
    size_t const NAME_LENGTH = strnlen(name, visible.size());
    if (PATH_LENGTH == 0 || PATH_LENGTH >= root.size() || NAME_LENGTH == 0 || NAME_LENGTH >= visible.size()) return -ENAMETOOLONG;
    root.fill('\0');
    visible.fill('\0');
    std::memcpy(root.data(), path, PATH_LENGTH);
    std::memcpy(visible.data(), name, NAME_LENGTH);
    return 0;
}

auto vfs_xattr_begin_is_exact_replay(const VfsXattrServerStage& old, const VfsXattrBeginPayload& begin,
                                     const ker::mod::security::credential_record::Record& caller_record, const std::array<char, 512>& path,
                                     const std::array<char, WKI_VFS_XATTR_NAME_MAX + 1>& name,
                                     const std::array<char, VFS_EXPORT_PATH_LEN>& export_root,
                                     const std::array<char, VFS_EXPORT_PATH_LEN>& export_name) -> bool {
    return old.identity.session_id == begin.header.session_id && old.identity.operation_id == begin.header.operation_id &&
           old.identity.operation == begin.header.operation && old.identity.target == begin.header.target && old.flags == begin.flags &&
           old.requested_size == begin.data_len && old.follow_final_symlink == (begin.follow_final_symlink != 0) && old.path == path &&
           old.name == name && old.remote_fd == begin.remote_fd && old.caller_record == caller_record &&
           old.require_directory == (begin.require_directory != 0) && old.export_root == export_root && old.export_name == export_name;
}

auto vfs_xattr_new_begin_status(const VfsXattrServerStage& old, const VfsXattrPhaseHeader& incoming) -> int {
    if (old.identity.session_id != incoming.session_id || incoming.operation_id <= old.identity.operation_id) {
        return -ESTALE;
    }
    // A higher operation in the channel-bound session abandons an incomplete
    // transfer. This keeps a lost ABORT from wedging the lane indefinitely.
    return 0;
}

void release_vfs_xattr_stage_data(VfsXattrServerStage& stage) {
    std::unique_ptr<uint8_t[]> retired_data = std::move(stage.data);
    uint32_t const RETIRED_BYTES = stage.staged_bytes;
    stage.staged_bytes = 0;
    // Keep the global charge until the allocation is actually gone so cleanup
    // and replacement cannot transiently exceed the budget.
    retired_data.reset();
    if (RETIRED_BYTES != 0) {
        g_vfs_xattr_stage_bytes.fetch_sub(RETIRED_BYTES, std::memory_order_acq_rel);
    }
}

auto vfs_xattr_data_is_exact_replay(const VfsXattrServerStage& stage, uint32_t offset, const uint8_t* bytes, uint16_t length) -> bool {
    return bytes != nullptr && offset <= stage.transferred && length <= stage.transferred - offset &&
           memcmp(stage.data.get() + offset, bytes, length) == 0;
}

constexpr auto vfs_xattr_transfer_total(bool read_operation, size_t caller_size, uint32_t remote_total) -> uint32_t {
    return read_operation ? (caller_size == 0 ? 0 : remote_total) : static_cast<uint32_t>(caller_size);
}

auto vfs_channel_identity_matches(const WkiChannelIdentity& expected, const WkiChannelIdentity& actual) -> bool {
    return expected.channel != nullptr && expected.channel == actual.channel && expected.peer_node_id == actual.peer_node_id &&
           expected.channel_id == actual.channel_id && expected.generation != 0 && expected.generation == actual.generation;
}

auto vfs_channel_identity_matches_header(const WkiHeader* hdr, const WkiChannelIdentity& identity) -> bool {
    return hdr != nullptr && identity.channel != nullptr && identity.generation != 0 && hdr->src_node == identity.peer_node_id &&
           hdr->channel_id == identity.channel_id;
}

auto find_remote_fd(const WkiChannelIdentity& channel_identity, int32_t fd_id) -> RemoteVfsFd* {
    auto const IT = g_remote_fds.find(fd_id);
    if (IT == g_remote_fds.end()) {
        return nullptr;
    }
    auto& rfd = IT->second;
    if (rfd.active && !rfd.retiring && vfs_channel_identity_matches(rfd.channel_identity, channel_identity)) {
        return &rfd;
    }
    return nullptr;
}

// Caller holds s_vfs_lock. A token is meaningful only within its exact binding
// generation; another consumer/channel cannot cancel or reuse this view.
auto find_exec_view_locked(const WkiChannelIdentity& identity, uint64_t token) -> RemoteVfsFd* {
    for (auto& [fd, row] : g_remote_fds) {
        static_cast<void>(fd);
        if (token != 0 && row.active && !row.retiring && row.exec_view_token == token &&
            vfs_channel_identity_matches(row.channel_identity, identity))
            return &row;
    }
    return nullptr;
}

auto next_remote_fd_locked() -> int32_t {
    int32_t fd_id = -EMFILE;
    for (size_t attempt = 0; attempt <= g_remote_fds.size(); ++attempt) {
        int32_t const CANDIDATE = g_next_remote_fd;
        g_next_remote_fd = CANDIDATE == INT32_MAX ? 1 : CANDIDATE + 1;
        if (!g_remote_fds.contains(CANDIDATE)) {
            fd_id = CANDIDATE;
            break;
        }
    }
    return fd_id;
}

void erase_remote_fd_if_retired(const WkiChannelIdentity& channel_identity, int32_t fd_id) {
    auto const IT = g_remote_fds.find(fd_id);
    if (IT == g_remote_fds.end()) {
        return;
    }
    auto const& rfd = IT->second;
    if (rfd.retiring && rfd.file == nullptr && vfs_channel_identity_matches(rfd.channel_identity, channel_identity)) {
        g_remote_fds.erase(IT);
    }
}

// D10: Update last_activity_us on a remote FD
void touch_remote_fd(RemoteVfsFd* rfd) {
    if (rfd != nullptr) {
        rfd->last_activity_us = wki_now_us();
    }
}

auto relative_wire_path_has_safe_components(const uint8_t* path, size_t path_len, bool allow_dot_components = false) -> bool {
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
        if ((DOT || DOT_DOT) && !allow_dot_components) {
            return false;
        }
        component_start = pos + 1;
    }
    return true;
}

// Build full absolute path from export_path + relative_path without silently
// changing the requested name. Owner-side admission treats truncation as an
// error before confinement checks or filesystem calls.
auto build_full_path(char* out, size_t out_size, const char* export_path, const char* relative_path, size_t rel_len) -> int {
    if (out == nullptr || out_size == 0 || export_path == nullptr || (relative_path == nullptr && rel_len != 0)) {
        return -EINVAL;
    }
    out[0] = '\0';

    size_t const EXPORT_LEN = strlen(export_path);
    if (EXPORT_LEN >= out_size) {
        return -ENAMETOOLONG;
    }
    size_t const SEPARATOR_LEN = EXPORT_LEN > 0 && export_path[EXPORT_LEN - 1] != '/' && rel_len > 0 ? 1U : 0U;
    size_t remaining = out_size - EXPORT_LEN - 1;
    if (SEPARATOR_LEN > remaining) {
        return -ENAMETOOLONG;
    }
    remaining -= SEPARATOR_LEN;
    if (rel_len > remaining) {
        return -ENAMETOOLONG;
    }

    memcpy(out, export_path, EXPORT_LEN);
    size_t pos = EXPORT_LEN;
    if (SEPARATOR_LEN != 0) {
        out[pos++] = '/';
    }
    if (rel_len != 0) {
        memcpy(out + pos, relative_path, rel_len);
        pos += rel_len;
    }
    out[pos] = '\0';
    return 0;
}

auto exact_relative_wire_path(const uint8_t* data, size_t data_len, size_t path_offset, size_t path_len, bool allow_dot_components = false)
    -> const uint8_t* {
    if (data == nullptr || path_offset > data_len || path_len != data_len - path_offset) {
        return nullptr;
    }
    const uint8_t* const PATH = data + path_offset;
    return relative_wire_path_has_safe_components(PATH, path_len, allow_dot_components) ? PATH : nullptr;
}

auto mapped_wire_path_status(const uint8_t* path, size_t path_len, bool component_paths) -> int {
    if (!relative_wire_path_has_safe_components(path, path_len, true)) return -EINVAL;
    return component_paths || relative_wire_path_has_safe_components(path, path_len) ? 0 : -ENOTSUP;
}

struct FileMetadataRequest {
    VfsPathIdentityHeader header{};
    ker::mod::security::Snapshot credentials;
    int32_t fd = -1;
    uint32_t mode = 0;
    uint32_t owner = UINT32_MAX;
    uint32_t group = UINT32_MAX;
    std::array<ker::vfs::Timespec, 2> times{};
};

auto decode_file_metadata_request(const uint8_t* data, size_t length, FileMetadataRequest& output) -> int {
    constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
    constexpr size_t OFFSET = RECORD + sizeof(VfsPathIdentityHeader);
    if (data == nullptr || length < OFFSET) return -EINVAL;
    FileMetadataRequest candidate;
    std::memcpy(&candidate.header, data + RECORD, sizeof(candidate.header));
    size_t const BODY = length - OFFSET;
    switch (candidate.header.operation) {
        case VfsPathOperation::FCHMOD: {
            VfsFileChmodRequest request{};
            if (BODY != sizeof(request)) return -EINVAL;
            std::memcpy(&request, data + OFFSET, sizeof(request));
            if (!wki_vfs_fchmod_request_valid(candidate.header, request, BODY)) return -EINVAL;
            candidate.fd = request.fd;
            candidate.mode = request.mode;
            break;
        }
        case VfsPathOperation::FCHOWN: {
            VfsFileChownRequest request{};
            if (BODY != sizeof(request)) return -EINVAL;
            std::memcpy(&request, data + OFFSET, sizeof(request));
            if (!wki_vfs_fchown_request_valid(candidate.header, request, BODY)) return -EINVAL;
            candidate.fd = request.fd;
            candidate.owner = request.owner;
            candidate.group = request.group;
            break;
        }
        case VfsPathOperation::FUTIMENS: {
            VfsFileUtimensRequest request{};
            if (BODY != sizeof(request)) return -EINVAL;
            std::memcpy(&request, data + OFFSET, sizeof(request));
            if (!wki_vfs_futimens_request_valid(candidate.header, request, BODY)) return -EINVAL;
            candidate.fd = request.fd;
            candidate.times = {{{request.atime_sec, request.atime_nsec}, {request.mtime_sec, request.mtime_nsec}}};
            ker::vfs::TimestampUpdate validated;
            if (int const RET = ker::vfs::TimestampUpdate::prepare(candidate.times.data(), {0, 0}, validated); RET != 0) return RET;
            break;
        }
        default:
            return -ENOTSUP;
    }
    int const RET = ker::mod::security::credential_record::decode({data, RECORD}, candidate.credentials);
    if (RET != 0) return RET;
    output = candidate;
    return 0;
}

struct DirectoryPathRequest {
    VfsPathIdentityHeader header{};
    int32_t directory_fd = -1;
    uint32_t open_flags = 0;
    uint32_t open_mode = 0;
    const uint8_t* path = nullptr;
    size_t path_length = 0;
};

auto decode_directory_path_request(const uint8_t* data, size_t length, bool component_paths, bool enabled, DirectoryPathRequest& output)
    -> int {
    constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
    constexpr size_t BASE = RECORD + sizeof(VfsPathIdentityHeader);
    if (!enabled) return -ENOTSUP;
    if (data == nullptr || length <= BASE + sizeof(int32_t)) return -EINVAL;
    DirectoryPathRequest candidate{};
    std::memcpy(&candidate.header, data + RECORD, sizeof(candidate.header));
    bool const STAT = candidate.header.operation == VfsPathOperation::STAT_AT;
    bool const OPEN = candidate.header.operation == VfsPathOperation::OPEN_AT;
    if (!STAT && !OPEN && candidate.header.operation != VfsPathOperation::READLINK_AT &&
        candidate.header.operation != VfsPathOperation::REALPATH_AT)
        return -ENOTSUP;
    size_t const OFFSET = BASE + sizeof(int32_t) + (OPEN ? 2 * sizeof(uint32_t) : 0);
    uint16_t const ALLOWED = WKI_VFS_PATH_REQUIRE_DIRECTORY | (STAT ? WKI_VFS_PATH_FOLLOW_FINAL : 0);
    if (candidate.header.reserved != 0 || (candidate.header.flags & ~ALLOWED) != 0 || candidate.header.data_len != length - BASE ||
        length <= OFFSET || length - OFFSET > 511)
        return -EINVAL;
    std::memcpy(&candidate.directory_fd, data + BASE, sizeof(int32_t));
    if (candidate.directory_fd <= 0) return -EBADF;
    if (OPEN) {
        std::memcpy(&candidate.open_flags, data + BASE + sizeof(int32_t), sizeof(uint32_t));
        std::memcpy(&candidate.open_mode, data + BASE + sizeof(int32_t) + sizeof(uint32_t), sizeof(uint32_t));
        if (auth_policy::vfs_open_permissions(candidate.open_flags) == auth_policy::INVALID) return -EINVAL;
    }
    candidate.path = data + OFFSET;
    candidate.path_length = length - OFFSET;
    int const STATUS = mapped_wire_path_status(candidate.path, candidate.path_length, component_paths);
    if (STATUS != 0) return STATUS;
    output = candidate;
    return 0;
}

auto wire_payload_has_optional_exact_suffix(size_t data_len, size_t body_end, size_t suffix_len) -> bool {
    return data_len == body_end || (body_end <= data_len && suffix_len == data_len - body_end);
}

auto wire_symlink_target_is_representable(const uint8_t* target, size_t target_len, size_t buffer_capacity) -> bool {
    return target != nullptr && target_len < buffer_capacity && std::memchr(target, '\0', target_len) == nullptr;
}

#ifdef WOS_SELFTEST
auto prepare_legacy_scalar_path(const char* export_path, const char* export_name, const uint8_t* relative_path, size_t relative_path_len,
                                char* full_path, size_t full_path_size, char* full_visible_path, size_t full_visible_path_size) -> int {
    if (!relative_wire_path_has_safe_components(relative_path, relative_path_len)) {
        return -EINVAL;
    }
    int const BACKING_RET =
        build_full_path(full_path, full_path_size, export_path, reinterpret_cast<const char*>(relative_path), relative_path_len);
    if (BACKING_RET != 0) {
        return BACKING_RET;
    }
    return build_full_path(full_visible_path, full_visible_path_size, export_name, reinterpret_cast<const char*>(relative_path),
                           relative_path_len);
}

#endif

auto prepare_mapped_scalar_path(const char* export_path, const char* export_name, const uint8_t* relative_path, size_t relative_path_len,
                                bool component_paths, char* full_path, size_t full_path_size, char* full_visible_path,
                                size_t full_visible_path_size) -> int {
    int const VALIDATION = mapped_wire_path_status(relative_path, relative_path_len, component_paths);
    if (VALIDATION != 0) return VALIDATION;
    int const BACKING =
        build_full_path(full_path, full_path_size, export_path, reinterpret_cast<const char*>(relative_path), relative_path_len);
    if (BACKING != 0) return BACKING;
    return build_full_path(full_visible_path, full_visible_path_size, export_name, reinterpret_cast<const char*>(relative_path),
                           relative_path_len);
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
        case DevAttachStatus::ACCESS_DENIED:
            return -EACCES;
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

std::deque<std::unique_ptr<ProxyVfsState>> g_vfs_proxies;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remote_vfs_initialized = false;                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint8_t g_vfs_attach_next_cookie = 1;                      // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_vfs_next_mount_group_id = 1;                    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ProxyVfsState* g_pending_vfs_detach_head = nullptr;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ProxyVfsState* g_pending_vfs_detach_tail = nullptr;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

ker::mod::sys::Spinlock s_vfs_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Admission includes live views, so every PIN has infallible cancellation
// storage even when transport/allocator pressure prevents its first cleanup.
constexpr size_t VFS_EXEC_CANCEL_CAPACITY = 1024;
constexpr uint64_t VFS_EXEC_CANCEL_RETRY_TIMEOUT_US = 100'000;
constexpr uint64_t VFS_EXEC_CANCEL_RETRY_INTERVAL_US = 100'000;
enum class ExecCancellationPhase : uint8_t { FREE, OWNED, PENDING, RETRYING };
struct ExecCancellation {
    ExecCancellationPhase phase = ExecCancellationPhase::FREE;
    ProxyVfsState* proxy = nullptr;
    WkiChannelIdentity identity{};
    uint64_t token = 0;
    uint32_t peer_boot_epoch = 0;
    uint64_t retry_after_us = 0;
    bool directory = false;
};
// All record phases and the cursor are protected by s_vfs_lock. OWNED and
// RETRYING records have one exclusive caller until finish_exec_cancellation.
std::array<ExecCancellation, VFS_EXEC_CANCEL_CAPACITY>
    g_exec_cancellations{};                   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t g_exec_cancel_cursor = 0;              // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
WkiRemoteVfsExecDiag g_exec_cancel_totals{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Ordinary Files reserve their own bounded cleanup storage before OPEN. This
// does not consume immutable pin capacity or change pin diagnostics.
constexpr size_t VFS_OPEN_CANCEL_CAPACITY = 1024;
struct OrdinaryCancellation {
    ExecCancellationPhase phase = ExecCancellationPhase::FREE;
    ProxyVfsState* proxy = nullptr;
    WkiChannelIdentity identity{};
    VfsOpenLifetimeRequest request{};  // CANCEL form; token assigned only after the proxy slot is held.
    uint32_t peer_boot_epoch = 0;
    uint64_t retry_after_us = 0;
};
std::array<OrdinaryCancellation, VFS_OPEN_CANCEL_CAPACITY> g_open_cancellations{};
size_t g_open_cancel_cursor = 0;
uint64_t g_next_open_query_challenge = 1;  // All three protected by s_vfs_lock.

WkiRemoteVfsExecDiag g_directory_cancel_totals{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
auto pin_cancellation_totals(bool directory) -> WkiRemoteVfsExecDiag& {
    return directory ? g_directory_cancel_totals : g_exec_cancel_totals;
}

auto cancel_exec_view(const WkiChannelIdentity& identity, uint64_t token) -> int {
    ker::vfs::File* retired = nullptr;
    std::map<int32_t, RemoteVfsFd>::node_type retired_row;
    s_vfs_lock.lock();
    if (auto* row = find_exec_view_locked(identity, token); row != nullptr) {
        retired = row->file;
        int32_t const FD = row->fd_id;
        retired_row = g_remote_fds.extract(FD);
        ++g_remote_fd_close_retirements_total;
    }
    s_vfs_lock.unlock();
    // Acknowledgement retires this registry reference. An admitted operation
    // keeps its independent view (and writer exclusion) until its final put.
    return ker::vfs::vfs_put_file_result(retired);
}

constexpr size_t VFS_DIRECTORY_GRANTS_PER_CONSUMER = 128;
// Local staging ownership is independent of peer FD retirement/garbage
// collection. Map nodes are preallocated and final Files drain after unlock.
std::map<uint64_t, RemoteVfsFd> g_native_directory_grants;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto native_grant_resource_current_locked(const VfsDirectoryGrant& grant, uint64_t revision) -> bool {
    if (grant.owner_node != g_wki.my_node_id || grant.source_node != g_wki.my_node_id ||
        grant.source_boot_epoch != g_wki.local_boot_epoch || grant.owner_incarnation.owner_boot_epoch != g_wki.local_boot_epoch ||
        revision == 0 || revision != g_vfs_export_revision || (revision & 1U) != 0)
        return false;
    return std::ranges::any_of(g_vfs_exports, [&](const VfsExport& exp) {
        return exp.active && exp.resource_id == grant.resource_id &&
               exp.resource_incarnation == grant.owner_incarnation.resource_incarnation && exp.publication_revision == revision;
    });
}

// Protected by s_vfs_lock; never reset or reused.
uint64_t g_next_directory_grant_id = 1;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
auto take_directory_grant_id(uint64_t& next) -> uint64_t {
    auto const result = next;
    if (next != 0) next = next == UINT64_MAX ? 0 : next + 1;
    return result;
}

auto find_directory_pin_locked(const WkiChannelIdentity& identity, uint64_t token) -> RemoteVfsFd*;
auto find_directory_grant_locked(const VfsDirectoryGrant& grant) -> RemoteVfsFd* {
    if (!wki_vfs_directory_grant_valid(grant)) return nullptr;
    auto local = g_native_directory_grants.find(grant.owner_grant_id);
    if (local != g_native_directory_grants.end()) {
        auto& row = local->second;
        return row.active && !row.retiring && row.file != nullptr && wki_vfs_directory_grant_equal(row.directory_grant, grant) ? &row
                                                                                                                               : nullptr;
    }
    for (auto& [fd, row] : g_remote_fds) {
        static_cast<void>(fd);
        if (row.active && !row.retiring && row.file != nullptr && wki_vfs_directory_grant_equal(row.directory_grant, grant)) return &row;
    }
    return nullptr;
}

auto grant_binding_matches(const DevServerBinding& binding, const WkiChannelIdentity& identity) -> bool {
    // The caller already owns binding admission. Mutable lifecycle state is
    // observed only through its atomic retirement flag outside s_server_lock.
    return !binding.retiring.load(std::memory_order_acquire) && binding.resource_type == ResourceType::VFS &&
           binding.consumer_boot_epoch != 0 && binding.consumer_node == identity.peer_node_id && binding.resource_id != 0 &&
           binding.vfs_export_publication_revision != 0 && wki_resource_incarnation_valid(binding.resource_incarnation) &&
           vfs_channel_identity_matches(binding.channel_identity, identity);
}

// Protected by s_vfs_lock; never reset/reused within one owner boot.
uint64_t g_next_open_binding_id = 1;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
auto query_ordinary_open_binding(DevServerBinding& binding, const WkiChannelIdentity& identity, uint64_t challenge,
                                 VfsOpenLifetimeRequest& result) -> int {
    s_vfs_lock.lock();
    int status = 0;
    if (!grant_binding_matches(binding, identity))
        status = -ENOTCONN;
    else if (challenge == 0)
        status = -EINVAL;
    else {
        if (binding.vfs_open_binding_id == 0) binding.vfs_open_binding_id = take_directory_grant_id(g_next_open_binding_id);
        if (binding.vfs_open_binding_id == 0)
            status = -EOVERFLOW;
        else
            result = {.inner_op = OP_VFS_FEATURES,
                      .token = challenge,
                      .binding_id = binding.vfs_open_binding_id,
                      .owner_boot_epoch = binding.resource_incarnation.owner_boot_epoch};
    }
    s_vfs_lock.unlock();
    return status;
}

auto ordinary_open_binding_matches_locked(const DevServerBinding& binding, const WkiChannelIdentity& identity,
                                          const VfsOpenLifetimeRequest& request) -> bool {
    return grant_binding_matches(binding, identity) && request.binding_id != 0 && request.binding_id == binding.vfs_open_binding_id &&
           request.owner_boot_epoch != 0 && request.owner_boot_epoch == binding.resource_incarnation.owner_boot_epoch;
}

// The retained peer/channel FIFO orders this admission with ordinary CANCEL.
// A token is consumed even when a later backend operation fails: OPEN is never
// replayed after an ambiguous result. Cancellation grants no new file access.
auto admit_ordinary_open_token(DevServerBinding& binding, const WkiChannelIdentity& identity, const VfsOpenLifetimeRequest& request)
    -> int {
    s_vfs_lock.lock();
    int status = 0;
    if (!ordinary_open_binding_matches_locked(binding, identity, request))
        status = -ENOTCONN;
    else if (request.token == 0)
        status = -EINVAL;
    else if (request.token <= binding.vfs_open_token_high_water)
        status = -EALREADY;
    else
        binding.vfs_open_token_high_water = request.token;
    s_vfs_lock.unlock();
    return status;
}

auto cancel_ordinary_open(DevServerBinding& binding, const WkiChannelIdentity& identity, const VfsOpenLifetimeRequest& request) -> int {
    ker::vfs::File* file = nullptr;
    std::map<int32_t, RemoteVfsFd>::node_type retired;
    s_vfs_lock.lock();
    if (!ordinary_open_binding_matches_locked(binding, identity, request) || request.token == 0) {
        s_vfs_lock.unlock();
        return request.token == 0 ? -EINVAL : -ENOTCONN;
    }
    binding.vfs_open_token_high_water = std::max(binding.vfs_open_token_high_water, request.token);
    for (auto it = g_remote_fds.begin(); it != g_remote_fds.end(); ++it) {
        auto& row = it->second;
        if (row.ordinary_open_token != request.token || row.ordinary_open_binding_id != request.binding_id ||
            !vfs_channel_identity_matches(row.channel_identity, identity))
            continue;
        file = row.file;
        retired = g_remote_fds.extract(it);
        ++g_remote_fd_close_retirements_total;
        break;
    }
    s_vfs_lock.unlock();
    // Already admitted users own separate references. A repeat cancellation
    // cannot find or release this registry owner twice, even on close error.
    return ker::vfs::vfs_put_file_result(file);
}

auto directory_grant_source_current_locked(const RemoteVfsFd& row, const DevServerBinding* binding) -> bool {
    if (row.directory_grant_local_source)
        return binding == nullptr && native_grant_resource_current_locked(row.directory_grant, row.directory_grant_publication_revision);
    return binding != nullptr && grant_binding_matches(*binding, row.channel_identity);
}

auto cancel_native_directory_grant(uint64_t id) -> int {
    std::map<uint64_t, RemoteVfsFd>::node_type retired;
    s_vfs_lock.lock();
    retired = g_native_directory_grants.extract(id);
    s_vfs_lock.unlock();
    return retired ? ker::vfs::vfs_put_file_result(retired.mapped().file) : 0;
}

struct NativeDirectoryGrantCleanup {
    uint64_t id = 0;
};
auto close_native_directory_grant(ker::vfs::File* file) -> int {
    std::unique_ptr<NativeDirectoryGrantCleanup> cleanup(static_cast<NativeDirectoryGrantCleanup*>(file->private_data));
    file->private_data = nullptr;
    return cleanup ? cancel_native_directory_grant(cleanup->id) : 0;
}
ker::vfs::FileOperations g_native_directory_grant_cleanup_ops{
    .vfs_close = close_native_directory_grant};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Reserve registry storage before an owner OPEN can create or truncate. The
// inactive row is invisible to descriptor operations until publication.
class RemoteFdReservation {
   public:
    explicit RemoteFdReservation(const WkiChannelIdentity& identity) : identity_(identity) {
        // Allocate the map node in worker context before taking the registry
        // lock. Insertion and later publication only transfer existing storage.
        std::map<int32_t, RemoteVfsFd> prepared;
        RemoteVfsFd row{};
        row.consumer_node = identity.peer_node_id;
        row.channel_identity = identity;
        row.last_activity_us = wki_now_us();
        prepared.emplace(0, row);
        auto node = prepared.extract(0);
        s_vfs_lock.lock();
        fd_ = next_remote_fd_locked();
        if (fd_ >= 0) {
            node.key() = fd_;
            node.mapped().fd_id = fd_;
            g_remote_fds.insert(std::move(node));
        }
        s_vfs_lock.unlock();
    }
    RemoteFdReservation(const RemoteFdReservation&) = delete;
    auto operator=(const RemoteFdReservation&) -> RemoteFdReservation& = delete;
    ~RemoteFdReservation() {
        if (fd_ < 0) return;
        std::map<int32_t, RemoteVfsFd>::node_type retired;
        s_vfs_lock.lock();
        auto it = g_remote_fds.find(fd_);
        if (it != g_remote_fds.end() && it->second.file == nullptr && vfs_channel_identity_matches(it->second.channel_identity, identity_))
            retired = g_remote_fds.extract(it);
        s_vfs_lock.unlock();
    }
    [[nodiscard]] auto result() const -> int32_t { return fd_; }
    auto publish(ker::vfs::File* file, uint64_t exec_token = 0, int32_t exec_source_fd = 0, uint64_t directory_token = 0,
                 uint64_t ordinary_token = 0, uint64_t ordinary_binding_id = 0) -> bool {
        if (fd_ < 0 || file == nullptr) return false;
        s_vfs_lock.lock();
        auto it = g_remote_fds.find(fd_);
        bool const VALID = it != g_remote_fds.end() && !it->second.active && !it->second.retiring && it->second.file == nullptr &&
                           vfs_channel_identity_matches(it->second.channel_identity, identity_);
        if (VALID) {
            it->second.file = file;
            it->second.exec_view_token = exec_token;
            it->second.directory_pin_token = directory_token;
            it->second.exec_view_source_fd = exec_source_fd;
            it->second.ordinary_open_token = ordinary_token;
            it->second.ordinary_open_binding_id = ordinary_binding_id;
            it->second.active = true;
            it->second.last_activity_us = wki_now_us();
            g_remote_fd_opens_total++;
            fd_ = -1;
        }
        s_vfs_lock.unlock();
        return VALID;
    }

    auto publish_directory_grant(ker::vfs::File* file, ker::vfs::File* source_file, const DevServerBinding& binding,
                                 const VfsDirectoryGrantRequest& request, VfsDirectoryGrant grant,
                                 const ker::mod::security::credential_record::Record& credentials, VfsDirectoryGrant& output) -> int {
        if (file == nullptr || source_file == nullptr) return -EINVAL;
        int status = 0;
        s_vfs_lock.lock();
        auto* destination = directory_destination_locked();
        auto* source = find_remote_fd(identity_, request.directory_fd);
        if (destination == nullptr || !grant_binding_matches(binding, identity_) || source == nullptr || source->file != source_file ||
            source->directory_pin_token != request.source_pin_token)
            status = -ENOTCONN;
        else if (find_directory_pin_locked(identity_, request.pin_token) != nullptr)
            status = -EALREADY;
        size_t count = 0;
        if (status == 0) {
            for (auto const& [fd, row] : g_remote_fds) {
                static_cast<void>(fd);
                if (row.active && !row.retiring && row.consumer_node == identity_.peer_node_id && row.directory_grant.owner_grant_id != 0)
                    ++count;
            }
            if (count >= VFS_DIRECTORY_GRANTS_PER_CONSUMER) status = -EMFILE;
        }
        if (status == 0) {
            grant.owner_grant_id = take_directory_grant_id(g_next_directory_grant_id);
            if (grant.owner_grant_id == 0) status = -EOVERFLOW;
        }
        if (status == 0) {
            destination->directory_grant = grant;
            destination->directory_grant_credentials = credentials;
            destination->directory_grant_publication_revision = binding.vfs_export_publication_revision;
            publish_directory_row_locked(*destination, file, request.pin_token);
            touch_remote_fd(source);
        }
        s_vfs_lock.unlock();
        if (status == 0) output = grant;
        return status;
    }

    auto publish_directory_adoption(ker::vfs::File* file, ker::vfs::File* source_file, const DevServerBinding* source_binding,
                                    const DevServerBinding& destination_binding, const VfsDirectoryGrant& grant, uint64_t pin_token)
        -> int {
        if (file == nullptr || source_file == nullptr) return -EINVAL;
        int status = 0;
        s_vfs_lock.lock();
        auto* destination = directory_destination_locked();
        auto* source = find_directory_grant_locked(grant);
        if (destination == nullptr || source == nullptr || source->file != source_file ||
            !directory_grant_source_current_locked(*source, source_binding) || !grant_binding_matches(destination_binding, identity_))
            status = -ENOTCONN;
        else if (source->directory_grant_claimed || find_directory_pin_locked(identity_, pin_token) != nullptr)
            status = -EALREADY;
        if (status == 0) {
            // Native work has finished; these two registry publications cannot
            // fail or allocate. Cancellation cannot observe a partial claim.
            publish_directory_row_locked(*destination, file, pin_token);
            source->directory_grant_claimed = true;
            touch_remote_fd(source);
        }
        s_vfs_lock.unlock();
        return status;
    }

   private:
    auto directory_destination_locked() -> RemoteVfsFd* {
        auto it = g_remote_fds.find(fd_);
        return fd_ >= 0 && it != g_remote_fds.end() && !it->second.active && !it->second.retiring && it->second.file == nullptr &&
                       vfs_channel_identity_matches(it->second.channel_identity, identity_)
                   ? &it->second
                   : nullptr;
    }
    void publish_directory_row_locked(RemoteVfsFd& row, ker::vfs::File* file, uint64_t token) {
        row.file = file;
        row.directory_pin_token = token;
        row.last_activity_us = wki_now_us();
        row.active = true;
        ++g_remote_fd_opens_total;
        fd_ = -1;
    }
    WkiChannelIdentity identity_{};
    int32_t fd_ = -1;
};

// Deferred binding admission owns the channel/export. Acquire a separate File
// reference before registry unlock so CLOSE/retirement cannot destroy this I/O.
// Denial leaves the caller's output and registry activity unchanged.
enum class ServerFileUse : uint8_t { READ, DIRECTORY, METADATA, SEEK, SYNC };

auto retain_server_file(const WkiChannelIdentity& identity, int32_t fd, ServerFileUse use, ker::vfs::File*& output,
                        int invalid_descriptor = -EBADF) -> int {
    s_vfs_lock.lock();
    auto* row = find_remote_fd(identity, fd);
    int result = invalid_descriptor;
    if (row != nullptr && row->file != nullptr) {
        auto* file = row->file;
        bool const CALLBACK_PRESENT = use == ServerFileUse::METADATA || use == ServerFileUse::SYNC ||
                                      (file->fops != nullptr && ((use == ServerFileUse::READ && file->fops->vfs_read != nullptr) ||
                                                                 (use == ServerFileUse::DIRECTORY && file->fops->vfs_readdir != nullptr) ||
                                                                 (use == ServerFileUse::SEEK && file->fops->vfs_lseek != nullptr)));
        if (CALLBACK_PRESENT) {
            result = 0;
            if (use == ServerFileUse::READ || use == ServerFileUse::DIRECTORY)
                result = ker::mod::security::authorize_file_description_access(static_cast<unsigned>(file->open_flags) & 3U, false);
            if (result == 0) {
                ker::vfs::vfs_retain_file(file);
                touch_remote_fd(row);
                output = file;
            }
        }
    }
    s_vfs_lock.unlock();
    return result;
}

auto retain_server_write_file(const WkiChannelIdentity& identity, int32_t fd, int64_t offset, ker::vfs::File*& out,
                              int invalid_descriptor = -EBADF) -> int {
    s_vfs_lock.lock();
    auto* row = find_remote_fd(identity, fd);
    int result = invalid_descriptor;
    if (row != nullptr && row->file != nullptr && row->file->fops != nullptr && row->file->fops->vfs_write != nullptr) {
        auto* file = row->file;
        result = ker::mod::security::authorize_file_description_access(static_cast<unsigned>(file->open_flags) & 3U, true);
        if (result == 0 && offset < 0) result = -EINVAL;
        if (result == 0 && ker::vfs::file_mount_read_only(*file)) result = -EROFS;
        if (result == 0) {
            ker::vfs::vfs_retain_file(file);
            touch_remote_fd(row);
            out = file;
        }
    }
    s_vfs_lock.unlock();
    return result;
}

// Caller owns deferred admission of this exact authenticated export binding.
// Snapshot/backend work happens only after the retained File leaves this lock.
auto retain_server_owner_file(DevServerBinding& binding, const WkiChannelIdentity& identity, const VfsOwnerFdRequest& request,
                              ker::vfs::File*& output) -> int {
    if (!wki_vfs_owner_fd_request_valid(request, sizeof(request))) return -EINVAL;
    s_vfs_lock.lock();
    int result = -ENOTCONN;
    if (grant_binding_matches(binding, identity) && request.binding_id == binding.vfs_open_binding_id &&
        request.owner_boot_epoch == binding.resource_incarnation.owner_boot_epoch) {
        result = -EBADF;
        auto* row = find_remote_fd(identity, request.fd);
        if (row != nullptr && row->file != nullptr && row->ordinary_open_token == request.open_token &&
            row->ordinary_open_binding_id == request.binding_id) {
            ker::vfs::vfs_retain_file(row->file);
            output = row->file;
            touch_remote_fd(row);
            result = 0;
        }
    }
    s_vfs_lock.unlock();
    return result;
}

auto perform_server_mutation(DevServerBinding& binding, const WkiChannelIdentity& identity, const VfsOwnerFdRequest& owner,
                             const uint8_t* data, uint32_t length, bool append, int64_t requested_offset, VfsAppendResponse& output)
    -> int {
    if (data == nullptr || requested_offset < 0 || !wki_vfs_owner_fd_request_valid(owner, sizeof(owner))) return -EINVAL;
    // Include operation and offset: reusing an append ID for positioned WRITE
    // cannot borrow its receipt, even when the bytes themselves match.
    uint16_t const OPCODE = append ? OP_VFS_APPEND : OP_VFS_WRITE_COMMITTED;
    auth_crypto::Sha256 hash;
    hash.update(&OPCODE, sizeof(OPCODE));
    hash.update(&requested_offset, sizeof(requested_offset));
    hash.update(&length, sizeof(length));
    hash.update(data, length);
    auto const DIGEST = hash.finish();
    ker::vfs::File* file = nullptr;
    int status = retain_server_owner_file(binding, identity, owner, file);
    if (status != 0) return status;
    ker::vfs::coherency::AppendOutcome outcome{};
    s_vfs_lock.lock();
    auto* row = find_remote_fd(identity, owner.fd);
    bool const EXACT = row != nullptr && row->file == file && row->ordinary_open_token == owner.open_token &&
                       row->ordinary_open_binding_id == owner.binding_id && grant_binding_matches(binding, identity);
    auto const CLAIM =
        EXACT ? row->append_receipt.claim(owner.query_id, length, DIGEST, outcome) : ker::vfs::coherency::AppendClaim::INVALID;
    s_vfs_lock.unlock();
    if (CLAIM == ker::vfs::coherency::AppendClaim::NEW) {
        size_t offset = static_cast<size_t>(requested_offset);
        ssize_t const RESULT = append ? ker::vfs::vfs_append_file_native(file, data, length, &offset)
                                      : ker::vfs::vfs_pwrite_file(file, data, length, requested_offset);
        if (RESULT < 0) {
            outcome.status = static_cast<int32_t>(RESULT);
        } else if (static_cast<uint64_t>(RESULT) > length || offset > INT64_MAX ||
                   static_cast<uint64_t>(RESULT) > static_cast<uint64_t>(INT64_MAX) - offset) {
            outcome.status = -EPROTO;
        } else {
            outcome.bytes = static_cast<uint32_t>(RESULT);
            outcome.offset = offset;
        }
        s_vfs_lock.lock();
        row = find_remote_fd(identity, owner.fd);
        if (row != nullptr && row->file == file && row->ordinary_open_token == owner.open_token &&
            row->ordinary_open_binding_id == owner.binding_id) {
            if (!row->append_receipt.complete(owner.query_id, outcome)) status = -EPROTO;
        }
        s_vfs_lock.unlock();
    } else if (CLAIM != ker::vfs::coherency::AppendClaim::REPLAY) {
        status = CLAIM == ker::vfs::coherency::AppendClaim::PENDING    ? -EAGAIN
                 : CLAIM == ker::vfs::coherency::AppendClaim::STALE    ? -ESTALE
                 : CLAIM == ker::vfs::coherency::AppendClaim::MISMATCH ? -EPROTO
                                                                       : -ENOTCONN;
    }
    ker::vfs::vfs_put_file(file);
    if (status != 0) return status;
    output = {
        .operation = owner.query_id, .offset = static_cast<int64_t>(outcome.offset), .status = outcome.status, .bytes = outcome.bytes};
    return 0;
}

auto perform_server_append(DevServerBinding& binding, const WkiChannelIdentity& identity, const VfsAppendRequest& request,
                           const uint8_t* data, VfsAppendResponse& output) -> int {
    if (!wki_vfs_append_request_valid(request, sizeof(request) + request.data_len)) return -EINVAL;
    return perform_server_mutation(binding, identity, request.owner, data, request.data_len, true, 0, output);
}

auto perform_server_write(DevServerBinding& binding, const WkiChannelIdentity& identity, const VfsOwnerIoRequest& request,
                          const uint8_t* data, VfsAppendResponse& output) -> int {
    if (!wki_vfs_owner_io_request_valid(request, sizeof(request) + request.data_len, true)) return -EINVAL;
    return perform_server_mutation(binding, identity, request.owner, data, request.data_len, false, request.offset, output);
}

// The exact File pin spans backend read. The bounded candidate is caller-owned
// worker storage; no registry pointer or user buffer survives admission.
auto perform_server_read(DevServerBinding& binding, const WkiChannelIdentity& identity, const VfsOwnerIoRequest& request, uint8_t* data,
                         VfsOwnerReadResponse& output) -> int {
    if (data == nullptr || !wki_vfs_owner_io_request_valid(request, sizeof(request), false)) return -EINVAL;
    ker::vfs::File* file = nullptr;
    int const ADMISSION = retain_server_owner_file(binding, identity, request.owner, file);
    if (ADMISSION != 0) return ADMISSION;
    ssize_t const RESULT = ker::vfs::vfs_pread_file(file, data, request.data_len, request.offset);
    ker::vfs::vfs_put_file(file);
    if (RESULT < 0) return static_cast<int>(RESULT);
    if (static_cast<uint64_t>(RESULT) > request.data_len) return -EPROTO;
    output = {.query_id = request.owner.query_id, .bytes = static_cast<uint32_t>(RESULT)};
    return 0;
}

auto perform_server_advisory(DevServerBinding& binding, const WkiChannelIdentity& identity, const VfsOwnerLockRequest& request,
                             VfsOwnerLockResponse& output) -> int {
    if (!wki_vfs_owner_lock_request_valid(request, sizeof(request))) return -EINVAL;
    ker::vfs::File* file = nullptr;
    int const ADMISSION = retain_server_owner_file(binding, identity, request.owner, file);
    if (ADMISSION != 0) return ADMISSION;
    ker::vfs::RemoteAdvisoryCommand const COMMAND{.owner = {.generation = request.process_generation,
                                                            .boot_epoch = binding.consumer_boot_epoch,
                                                            .pid = request.pid,
                                                            .peer = identity.peer_node_id},
                                                  .scope_token = request.scope_token,
                                                  .kind = static_cast<ker::vfs::RemoteAdvisoryKind>(request.kind),
                                                  .start = request.start,
                                                  .length = request.range_length,
                                                  .position = request.position,
                                                  .type = request.type,
                                                  .whence = request.whence,
                                                  .query = (request.flags & WKI_VFS_LOCK_QUERY) != 0,
                                                  .wait = (request.flags & WKI_VFS_LOCK_WAIT) != 0};
    ker::vfs::RemoteAdvisoryReply reply{};
    int const STATUS = ker::vfs::vfs_advisory_remote_request(file, request.owner.query_id,
                                                             static_cast<ker::vfs::RemoteAdvisoryPhase>(request.phase), COMMAND, reply);
    ker::vfs::vfs_put_file(file);
    if (STATUS != 0) return STATUS;
    VfsOwnerLockResponse result{.operation = reply.operation, .status = reply.outcome.status, .state = static_cast<uint8_t>(reply.state)};
    if (COMMAND.query && reply.state == ker::vfs::RemoteAdvisoryState::COMPLETE && reply.outcome.status == 0 && reply.outcome.type != 2) {
        result.pid = reply.outcome.pid;
        result.start = reply.outcome.start;
        result.range_length = reply.outcome.length;
        result.type = reply.outcome.type;
        result.whence = reply.outcome.whence;
    }
    if (!wki_vfs_owner_lock_response_valid(result, request.owner.query_id, COMMAND.query, sizeof(result))) return -EPROTO;
    output = result;
    return 0;
}

auto perform_server_advisory_scope(DevServerBinding& binding, const WkiChannelIdentity& identity, const VfsOwnerScopeRequest& request,
                                   VfsOwnerScopeResponse& output) -> int {
    if (!wki_vfs_owner_scope_request_valid(request, sizeof(request))) return -EINVAL;
    ker::vfs::RemoteAdvisoryOwner const OWNER{.generation = request.process_generation,
                                              .boot_epoch = binding.consumer_boot_epoch,
                                              .pid = request.pid,
                                              .peer = identity.peer_node_id};
    ker::vfs::RemoteAdvisoryScopeResult result{};
    int status = 0;
    if (request.operation == VfsOwnerScopeOperation::IDENTIFY || request.operation == VfsOwnerScopeOperation::REGISTER) {
        ker::vfs::File* file = nullptr;
        status = retain_server_owner_file(binding, identity, request.owner, file);
        if (status != 0) return status;
        status = request.operation == VfsOwnerScopeOperation::IDENTIFY
                     ? ker::vfs::vfs_advisory_remote_identity(file, result.object)
                     : ker::vfs::vfs_advisory_remote_register_scope(file, OWNER, request.owner.query_id, result);
        ker::vfs::vfs_put_file(file);
    } else {
        // Independent scope authority permits close even after its witness row
        // disappears. Numeric FD/open token must be zero, never reinterpreted.
        s_vfs_lock.lock();
        bool const EXACT = grant_binding_matches(binding, identity) && request.owner.binding_id == binding.vfs_open_binding_id &&
                           request.owner.owner_boot_epoch == binding.resource_incarnation.owner_boot_epoch;
        s_vfs_lock.unlock();
        if (!EXACT) return -ENOTCONN;
        status = request.operation == VfsOwnerScopeOperation::CLOSE
                     ? ker::vfs::vfs_advisory_remote_close_scope(OWNER, request.scope_token, result)
                     : ker::vfs::vfs_advisory_remote_ack_scope(OWNER, request.scope_token, result);
    }
    if (status != 0) return status;
    VfsOwnerScopeResponse const RESPONSE{.query_id = request.owner.query_id,
                                         .scope_token = result.token,
                                         .object = result.object,
                                         .state = static_cast<uint8_t>(result.state)};
    if (!wki_vfs_owner_scope_response_valid(RESPONSE, request, sizeof(RESPONSE))) return -EPROTO;
    output = RESPONSE;
    return 0;
}

// Hooks are invoked in core task context. Every loan separately pins the exact
// binding and File; registration storage is never native FD authority.
auto secure_vfs_request_matches(const WkiSecureRdmaRequest& lhs, const WkiSecureRdmaRequest& rhs) -> bool {
    return lhs.operation_id == rhs.operation_id && lhs.receive_ticket == rhs.receive_ticket && lhs.offset == rhs.offset &&
           lhs.length == rhs.length && lhs.peer_node == rhs.peer_node && lhs.flags == rhs.flags && lhs.opcode == rhs.opcode &&
           lhs.intent_length == rhs.intent_length && lhs.native_intent == rhs.native_intent &&
           std::memcmp(&lhs.grant, &rhs.grant, sizeof(lhs.grant)) == 0;
}

auto secure_vfs_intent_matches(const VfsSecureContext& context, const WkiSecureRdmaRequest& request, const VfsSecureIoIntent& intent)
    -> bool {
    auto const& grant = request.opcode == rdma_protocol::Opcode::WRITE ? context.write_grant : context.read_grant;
    return request.flags == 0 && request.peer_node == context.identity.peer_node_id && intent.binding_id == context.binding_id &&
           intent.owner_boot_epoch == context.incarnation.owner_boot_epoch &&
           intent.resource_incarnation == context.incarnation.resource_incarnation && intent.resource_id == context.resource_id &&
           request.grant.binding_generation == context.binding_id && std::memcmp(&request.grant, &grant, sizeof(grant)) == 0;
}

// s_vfs_lock held. Caller owns a retained binding, and retains File before unlock.
auto secure_vfs_file_locked(const VfsSecureContext& context, const VfsSecureIoIntent& intent, bool write, ker::vfs::File*& output) -> int {
    if (context.closed.load(std::memory_order_acquire) || context.binding == nullptr ||
        !grant_binding_matches(*context.binding, context.identity) ||
        context.binding->vfs_export_publication_revision != context.publication_revision)
        return -ENOTCONN;
    auto* row = find_remote_fd(context.identity, intent.fd);
    if (row == nullptr || row->file == nullptr || row->ordinary_open_token != intent.open_token ||
        row->ordinary_open_binding_id != intent.binding_id)
        return -EBADF;
    auto* file = row->file;
    if (file->is_directory || file->exec_read_view != nullptr ||
        (file->fs_type != ker::vfs::FSType::TMPFS && file->fs_type != ker::vfs::FSType::XFS))
        return -ENOTSUP;
    if (file->fops == nullptr || (write ? file->fops->vfs_write == nullptr : file->fops->vfs_read == nullptr)) return -ENOTSUP;
    int const ACCESS = ker::mod::security::authorize_file_description_access(static_cast<unsigned>(file->open_flags) & 3U, write);
    if (ACCESS != 0) return ACCESS;
    if (write && ker::vfs::file_mount_read_only(*file)) return -EROFS;
    output = file;
    return 0;
}

auto secure_vfs_arm(void* opaque, const WkiSecureRdmaRequest& request, uint64_t& generation) -> int {
    auto* context = static_cast<VfsSecureContext*>(opaque);
    VfsSecureIoIntent intent{};
    if (context == nullptr || !wki_vfs_secure_decode_intent(request, intent) || !secure_vfs_intent_matches(*context, request, intent))
        return -EINVAL;
    uint64_t const FLAGS = context->loan_lock.lock_irqsave();
    if (context->closed.load(std::memory_order_acquire) || context->armed || context->generation == UINT64_MAX) {
        context->loan_lock.unlock_irqrestore(FLAGS);
        return -EBUSY;
    }
    context->armed = true;
    context->effect_started = false;
    context->request = request;
    context->loan_lock.unlock_irqrestore(FLAGS);

    DevServerBinding* binding = nullptr;
    ker::vfs::File* file = nullptr;
    int status = -ENOTCONN;
    if (wki_dev_server_vfs_rx_admission_try_acquire()) {
        binding = wki_dev_server_retain_vfs_secure_binding(context->identity, context);
        if (binding != nullptr) {
            // The reserved loan is the only writer of these pointers. Cleanup
            // cannot destroy context while the core's arm reference is retained.
            context->binding = binding;
            s_vfs_lock.lock();
            status = secure_vfs_file_locked(*context, intent, request.opcode == rdma_protocol::Opcode::WRITE, file);
            if (status == 0) ker::vfs::vfs_retain_file(file);
            s_vfs_lock.unlock();
        }
        wki_dev_server_vfs_rx_admission_release();
    }
    uint64_t const COMMIT_FLAGS = context->loan_lock.lock_irqsave();
    if (status == 0 && context->closed.load(std::memory_order_acquire)) status = -ENOTCONN;
    if (status == 0) {
        context->file = file;
        generation = ++context->generation;
    } else {
        context->binding = nullptr;
        context->armed = false;
        context->request = {};
    }
    context->loan_lock.unlock_irqrestore(COMMIT_FLAGS);
    if (status != 0) {
        if (file != nullptr) ker::vfs::vfs_put_file(file);
        wki_dev_server_release_vfs_secure_binding(binding);
    }
    return status;
}

auto secure_vfs_effect(void* opaque, const WkiSecureRdmaRequest& request, std::span<uint8_t> backing, std::span<const uint8_t> input,
                       std::span<uint8_t> output) -> WkiSecureRdmaResult {
    auto* context = static_cast<VfsSecureContext*>(opaque);
    VfsSecureIoIntent intent{};
    if (context == nullptr || !wki_vfs_secure_decode_intent(request, intent)) return {-EINVAL};
    bool const WRITE = request.opcode == rdma_protocol::Opcode::WRITE;
    auto* expected_backing = WRITE ? context->write_backing.get() : context->read_backing.get();
    if (backing.data() != expected_backing || backing.size() < request.length ||
        (WRITE ? input.size() != request.length : output.size() < request.length))
        return {-EINVAL};
    uint64_t const FLAGS = context->loan_lock.lock_irqsave();
    if (!context->armed || context->effect_started || !secure_vfs_request_matches(context->request, request)) {
        context->loan_lock.unlock_irqrestore(FLAGS);
        return {-EINVAL};
    }
    ker::vfs::File* file = nullptr;
    s_vfs_lock.lock();
    int const STATUS = secure_vfs_file_locked(*context, intent, WRITE, file);
    if (STATUS == 0 && file == context->file) context->effect_started = true;
    s_vfs_lock.unlock();
    bool const ADMITTED = STATUS == 0 && file == context->file;
    context->loan_lock.unlock_irqrestore(FLAGS);
    if (!ADMITTED) return {STATUS != 0 ? STATUS : -EBADF};

    // File and complete immutable input/output remain retained through release.
    // CLOSE after this native admission cannot free the description mid-effect.
    ssize_t const BYTES = WRITE ? ker::vfs::vfs_pwrite_file(file, input.data(), request.length, intent.file_offset)
                                : ker::vfs::vfs_pread_file(file, output.data(), request.length, intent.file_offset);
    if (BYTES < 0) return {static_cast<int32_t>(BYTES), 0, WRITE ? WkiSecureRdmaOutcome::UNCERTAIN : WkiSecureRdmaOutcome::NO_EFFECT};
    if (static_cast<uint64_t>(BYTES) > request.length || (WRITE && BYTES == 0)) return {-EIO, 0, WkiSecureRdmaOutcome::UNCERTAIN};
    // Authenticated native RESULT carries an exact short WRITE prefix even
    // when record WRITE_END cannot represent that partial native completion.
    return {0, static_cast<uint32_t>(BYTES), WkiSecureRdmaOutcome::EXACT};
}

void secure_vfs_release(void* opaque, const WkiSecureRdmaRequest& request, const WkiSecureRdmaResult&) {
    auto* context = static_cast<VfsSecureContext*>(opaque);
    if (context == nullptr) return;
    uint64_t const FLAGS = context->loan_lock.lock_irqsave();
    if (!context->armed || !secure_vfs_request_matches(context->request, request)) {
        context->loan_lock.unlock_irqrestore(FLAGS);
        return;
    }
    auto* file = std::exchange(context->file, nullptr);
    auto* binding = std::exchange(context->binding, nullptr);
    context->request = {};
    context->effect_started = false;
    context->armed = false;
    context->loan_lock.unlock_irqrestore(FLAGS);
    if (file != nullptr) ker::vfs::vfs_put_file(file);
    wki_dev_server_release_vfs_secure_binding(binding);
}

auto pin_exec_view(const WkiChannelIdentity& identity, const VfsExecViewRequest& request, VfsExecViewResponse& response) -> int {
    ker::vfs::File* source = nullptr;
    s_vfs_lock.lock();
    if (auto* existing = find_exec_view_locked(identity, request.token); existing != nullptr) {
        if (existing->exec_view_source_fd != request.source_fd) {
            s_vfs_lock.unlock();
            return -EINVAL;
        }
        response.fd = existing->fd_id;
        response.token = request.token;
        std::memcpy(response.metadata.data(), &existing->file->exec_read_view->metadata, response.metadata.size());
        touch_remote_fd(existing);
        s_vfs_lock.unlock();
        return 0;
    }
    if (auto* row = find_remote_fd(identity, request.source_fd); row != nullptr && row->file != nullptr) {
        source = row->file;
        ker::vfs::vfs_retain_file(source);
        touch_remote_fd(row);
    }
    s_vfs_lock.unlock();
    if (source == nullptr) return -EBADF;
    auto release = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
    std::unique_ptr<ker::vfs::File, decltype(release)> view(source, release);
    int const ACCESS = ker::mod::security::authorize_file_description_access(source->open_flags & 3U, false);
    if (ACCESS != 0) return ACCESS;
    if (source->is_directory) return -ENOEXEC;
    // Never recursively ask another owner, or report an ordinary unsupported
    // description as stable merely because the generic helper left it alone.
    if (source->fs_type != ker::vfs::FSType::TMPFS && source->fs_type != ker::vfs::FSType::XFS) return -ENOTSUP;
    RemoteFdReservation reservation(identity);
    if (reservation.result() < 0) return reservation.result();
    ker::vfs::File* candidate = view.release();
    int const RESULT = ker::vfs::vfs_stabilize_exec_file(&candidate);
    view.reset(candidate);
    if (RESULT != 0) return RESULT;
    if (view->exec_read_view == nullptr) return -ENOTSUP;
    static_assert(sizeof(ker::vfs::Stat) == WKI_VFS_FSTAT_RESPONSE_SIZE);
    response.token = request.token;
    std::memcpy(response.metadata.data(), &view->exec_read_view->metadata, response.metadata.size());
    response.fd = reservation.result();
    if (!reservation.publish(view.get(), request.token, request.source_fd)) return -ENOTCONN;
    static_cast<void>(view.release());
    return 0;
}

// Caller holds the registry lock; directory tokens cannot select exec views.
auto find_directory_pin_locked(const WkiChannelIdentity& identity, uint64_t token) -> RemoteVfsFd* {
    for (auto& [fd, row] : g_remote_fds) {
        static_cast<void>(fd);
        if (token != 0 && row.active && !row.retiring && row.directory_pin_token == token &&
            vfs_channel_identity_matches(row.channel_identity, identity))
            return &row;
    }
    return nullptr;
}

// Every published File may be retained by an admitted operation or transfer
// stage. Retire only the registry reference. If it is final, preserve the
// backend close result for the synchronous CLOSE response.
auto release_server_file(ker::vfs::File* file) -> int {
    ker::vfs::vfs_advisory_remote_retire(file);
    return ker::vfs::vfs_put_file_result(file);
}

auto cancel_directory_pin(const WkiChannelIdentity& identity, uint64_t token) -> int {
    std::map<int32_t, RemoteVfsFd>::node_type retired;
    s_vfs_lock.lock();
    if (auto* row = find_directory_pin_locked(identity, token); row != nullptr) {
        retired = g_remote_fds.extract(row->fd_id);
        ++g_remote_fd_close_retirements_total;
    }
    s_vfs_lock.unlock();
    return retired ? release_server_file(retired.mapped().file) : 0;
}

auto trim_export_prefix(const char* export_path, const char* local_vfs_path) -> const char*;

// Deferred caller holds exact server binding admission. Native work and final
// releases occur outside s_vfs_lock; output is published only after all checks.
auto directory_reference_owner(ker::mod::sched::task::Task& task, const ker::mod::security::Snapshot& credentials,
                               const WkiChannelIdentity& identity, const char* export_path, const char* export_name,
                               const VfsDirectoryRequest& request, const uint8_t* path, bool component_paths,
                               VfsDirectoryResponse& response, std::array<char, 512>& name) -> int {
    bool const PIN = request.operation == VfsDirectoryOperation::PIN;
    auto const REQUEST_CREDENTIALS = credentials;
    auto release = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
    std::unique_ptr<ker::vfs::File, decltype(release)> source(nullptr, release), candidate(nullptr, release), root(nullptr, release),
        directory(nullptr, release);
    ker::vfs::DirectoryViewRef view;
    s_vfs_lock.lock();
    if (PIN && find_directory_pin_locked(identity, request.token) != nullptr) {
        s_vfs_lock.unlock();
        return -EALREADY;
    }
    if (request.directory_fd > 0) {
        auto* row = find_remote_fd(identity, request.directory_fd);
        if (row != nullptr && row->file != nullptr && (PIN || row->directory_pin_token == request.token)) {
            ker::vfs::vfs_retain_file(row->file);
            source.reset(row->file);
        }
    }
    s_vfs_lock.unlock();
    if (request.directory_fd > 0 && !source) return -EBADF;
    std::array<char, 512> staged{}, full_path{}, visible{};
    size_t length = 0;
    VfsDirectoryResponse result{.fd = request.directory_fd, .token = request.token};
    if (!PIN) {
        if (!source || !source->directory_view) return -EBADF;
        view = source->directory_view;
        int const STATUS = ker::vfs::vfs_directory_path_beneath_directory(source.get(), view.root(), staged.data(), staged.size(), &length);
        if (STATUS != 0) return STATUS;
    } else {
        int status = mapped_wire_path_status(path, request.path_length, component_paths);
        if (status != 0) return status;
        const char* relative_path = full_path.data();
        if (source) {
            if (request.path_length == 0 || path[0] == '/') return -EINVAL;
            std::memcpy(full_path.data(), path, request.path_length);
            view = source->directory_view;
        } else {
            status = prepare_mapped_scalar_path(export_path, export_name, path, request.path_length, component_paths, full_path.data(),
                                                full_path.size(), visible.data(), visible.size());
            if (status == 0 && path_crosses_recursive_wki_boundary_direct(full_path.data(), visible.data())) status = -EPERM;
            if (status != 0) return status;
            relative_path = trim_export_prefix(export_path, full_path.data());
            if (relative_path == nullptr) return -EPERM;
            if (*relative_path == '\0') relative_path = ".";
        }
        RemoteFdReservation reservation(identity);
        if (reservation.result() < 0) return reservation.result();
        ker::vfs::File* file = nullptr;
        if (!view) {
            // Initial PIN and legacy ordinary OPEN sources have no retained
            // boundary yet. Admit it once; derived PINs reuse the exact view.
            status = ker::vfs::vfs_pin_directory_resolved_beneath_as(task, REQUEST_CREDENTIALS, export_path, export_path, &file, nullptr,
                                                                     !source);
            if (status != 0) return status;
            root.reset(file);
            if (source) {
                status = ker::vfs::vfs_pin_directory_resolved_beneath_as(task, REQUEST_CREDENTIALS, export_path, ".", &file, source.get());
                if (status != 0) return status;
                directory.reset(file);
            }
            status = ker::vfs::vfs_prepare_directory_view(REQUEST_CREDENTIALS, root.get(), source ? directory.get() : root.get(), {}, view);
            if (status != 0) return status;
        }
        // The immutable binding supplies the link namespace; the retained view
        // independently supplies the current inode boundary, even after rename.
        bool const VISIBLE_NAMESPACE = export_name[0] == '/';
        status = ker::vfs::vfs_pin_directory_view_as(task, REQUEST_CREDENTIALS, view, relative_path, &file,
                                                     VISIBLE_NAMESPACE ? export_path : nullptr, VISIBLE_NAMESPACE ? export_name : nullptr);
        if (status != 0) return status;
        candidate.reset(file);
        status = ker::vfs::vfs_directory_path_beneath_directory(file, file->directory_view.root(), staged.data(), staged.size(), &length);
        if (status != 0) return status;
        result.fd = reservation.result();
        if (!reservation.publish(file, 0, 0, request.token)) return -ENOTCONN;
        static_cast<void>(candidate.release());
    }
    // Revalidate the source row before refreshing it; a denied request never
    // extends its lifetime, and concurrent cancellation cannot touch a new row.
    if (source) {
        s_vfs_lock.lock();
        auto* row = find_remote_fd(identity, request.directory_fd);
        if (row != nullptr && row->file == source.get()) touch_remote_fd(row);
        s_vfs_lock.unlock();
    }
    result.path_length = static_cast<uint16_t>(length);
    response = result;
    name = staged;
    return 0;
}

struct DirectoryGrantFileRelease {
    void operator()(ker::vfs::File* file) const { ker::vfs::vfs_put_file(file); }
};
struct DirectoryGrantBindingRelease {
    void operator()(DevServerBinding* binding) const { wki_dev_server_release_vfs_grant_source(binding); }
};
struct DirectoryGrantAdmission {
    std::unique_ptr<DevServerBinding, DirectoryGrantBindingRelease> binding;
    std::unique_ptr<ker::vfs::File, DirectoryGrantFileRelease> file;
    ker::vfs::DirectoryViewRef view;
};

auto directory_grant_policy(ker::vfs::MountPolicy policy) -> uint16_t {
    return (policy.read_only ? WKI_VFS_DIRECTORY_GRANT_READONLY : 0) | (policy.nosuid ? WKI_VFS_DIRECTORY_GRANT_NOSUID : 0);
}

auto retain_directory_grant(const VfsDirectoryGrant& grant, const ker::mod::security::Snapshot& credentials, uint16_t target_node,
                            uint32_t target_boot_epoch, DirectoryGrantAdmission& output) -> int {
    if (output.binding || output.file || output.view) return -EINVAL;
    if (!wki_vfs_directory_grant_valid(grant)) return -EINVAL;
    if (grant.owner_node != g_wki.my_node_id || grant.owner_incarnation.owner_boot_epoch != g_wki.local_boot_epoch ||
        grant.target_node != target_node || grant.target_boot_epoch != target_boot_epoch)
        return -ESTALE;
    WkiChannelIdentity origin{};
    uint64_t publication_revision = 0;
    bool local = false;
    s_vfs_lock.lock();
    if (auto* row = find_directory_grant_locked(grant); row != nullptr) {
        origin = row->channel_identity;
        publication_revision = row->directory_grant_publication_revision;
        local = row->directory_grant_local_source;
    }
    s_vfs_lock.unlock();
    if (!local && origin.channel == nullptr) return -ESTALE;
    // Never nest server-binding and VFS registry locks. The independently
    // retained binding cannot be erased while native adoption runs unlocked.
    if (!local) {
        output.binding.reset(wki_dev_server_retain_vfs_grant_source(origin, grant, publication_revision));
        if (!output.binding) return -ENOTCONN;
    }
    auto const RECORD = ker::mod::security::credential_record::encode(credentials);
    int status = 0;
    s_vfs_lock.lock();
    auto* row = find_directory_grant_locked(grant);
    if (row == nullptr || !directory_grant_source_current_locked(*row, output.binding.get()) ||
        row->directory_grant_publication_revision != publication_revision)
        status = -ENOTCONN;
    else if (row->directory_grant_claimed)
        status = -EALREADY;
    else if (row->directory_grant_credentials != RECORD)
        status = -EACCES;
    else if (!row->file->directory_view || directory_grant_policy(row->file->directory_view.policy()) != grant.policy)
        status = -EPERM;
    if (status == 0) {
        ker::vfs::vfs_retain_file(row->file);
        output.file.reset(row->file);
        output.view = row->file->directory_view;
    }
    s_vfs_lock.unlock();
    return status;
}

auto claim_directory_grant(const VfsDirectoryGrant& grant, const DirectoryGrantAdmission& admission) -> int {
    int status = 0;
    s_vfs_lock.lock();
    auto* row = find_directory_grant_locked(grant);
    if (row == nullptr || row->file != admission.file.get() || !directory_grant_source_current_locked(*row, admission.binding.get()))
        status = -ENOTCONN;
    else if (row->directory_grant_claimed)
        status = -EALREADY;
    if (status == 0) {
        row->directory_grant_claimed = true;
        touch_remote_fd(row);
    }
    s_vfs_lock.unlock();
    return status;
}

#ifdef WOS_SELFTEST
// Deterministic cancellation between independent retention and final claim.
// Never invoked with a registry, path or publication lock held.
void (*directory_grant_adoption_test_hook)(const VfsDirectoryGrant&) = nullptr;
#endif

auto directory_grant_owner(ker::mod::sched::task::Task& task, const ker::mod::security::Snapshot& credentials,
                           const DevServerBinding& binding, const VfsDirectoryGrantRequest& request, VfsDirectoryGrantResponse& response,
                           std::array<char, 512>& name) -> int {
    if (!wki_vfs_directory_grant_request_valid(request, sizeof(request) + WKI_VFS_DIRECTORY_CALLER_SIZE)) return -EINVAL;
    auto const IDENTITY = binding.channel_identity;
    if (!grant_binding_matches(binding, IDENTITY) || g_wki.my_node_id == WKI_NODE_INVALID ||
        binding.resource_incarnation.owner_boot_epoch != g_wki.local_boot_epoch)
        return -ENOTCONN;
    auto const REQUEST_CREDENTIALS = credentials;
    std::unique_ptr<ker::vfs::File, DirectoryGrantFileRelease> source, candidate;
    DirectoryGrantAdmission admission;
    ker::vfs::DirectoryViewRef view;
    VfsDirectoryGrant grant{};
    if (request.operation == VfsDirectoryGrantOperation::ISSUE) {
        if (request.target_node == g_wki.my_node_id && request.target_boot_epoch != g_wki.local_boot_epoch) return -ESTALE;
        int status = 0;
        s_vfs_lock.lock();
        auto* row = find_remote_fd(IDENTITY, request.directory_fd);
        if (find_directory_pin_locked(IDENTITY, request.pin_token) != nullptr)
            status = -EALREADY;
        else if (row == nullptr || row->file == nullptr || row->directory_pin_token != request.source_pin_token)
            status = -EBADF;
        else {
            ker::vfs::vfs_retain_file(row->file);
            source.reset(row->file);
        }
        s_vfs_lock.unlock();
        if (status != 0) return status;
        if (!source->directory_view) return -ENOTSUP;
        auto const& original = source->directory_view;
        ker::vfs::MountPolicy const ADDITIONAL{.read_only = (request.policy & WKI_VFS_DIRECTORY_GRANT_READONLY) != 0,
                                               .nosuid = (request.policy & WKI_VFS_DIRECTORY_GRANT_NOSUID) != 0};
        status =
            ker::vfs::vfs_narrow_directory_view(original, REQUEST_CREDENTIALS, original.root(), original.directory(), ADDITIONAL, view);
        if (status != 0) return status;
        grant = {.version = WKI_VFS_DIRECTORY_GRANT_VERSION,
                 .policy = directory_grant_policy(view.policy()),
                 .owner_node = g_wki.my_node_id,
                 .source_node = binding.consumer_node,
                 .target_node = request.target_node,
                 .resource_id = binding.resource_id,
                 .owner_incarnation = binding.resource_incarnation,
                 .source_boot_epoch = binding.consumer_boot_epoch,
                 .target_boot_epoch = request.target_boot_epoch,
                 .submission_id = request.submission_id};
    } else {
        int const STATUS =
            retain_directory_grant(request.grant, REQUEST_CREDENTIALS, binding.consumer_node, binding.consumer_boot_epoch, admission);
        if (STATUS != 0) return STATUS;
        view = admission.view;
        grant = request.grant;
        // Import must attach to the same live owner resource, never a sibling
        // export selected by a historical or receiver-provided path.
        if (binding.resource_id != grant.resource_id ||
            !wki_resource_incarnation_equal(binding.resource_incarnation, grant.owner_incarnation))
            return -ESTALE;
    }
    RemoteFdReservation reservation(IDENTITY);
    if (reservation.result() < 0) return reservation.result();
    ker::vfs::File* file = nullptr;
    int status = ker::vfs::vfs_pin_directory_view_as(task, REQUEST_CREDENTIALS, view, ".", &file);
    if (status != 0) return status;
    candidate.reset(file);
    std::array<char, 512> staged{};
    size_t length = 0;
    status = ker::vfs::vfs_directory_path_beneath_directory(file, file->directory_view.root(), staged.data(), staged.size(), &length);
    if (status != 0) return status;
    VfsDirectoryGrantResponse result{};
    result.directory.fd = reservation.result();
    result.directory.token = request.pin_token;
    result.directory.path_length = static_cast<uint16_t>(length);
    if (request.operation == VfsDirectoryGrantOperation::ISSUE) {
        auto const RECORD = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
        status = reservation.publish_directory_grant(file, source.get(), binding, request, grant, RECORD, result.grant);
    } else {
#ifdef WOS_SELFTEST
        if (directory_grant_adoption_test_hook != nullptr) directory_grant_adoption_test_hook(grant);
#endif
        status =
            reservation.publish_directory_adoption(file, admission.file.get(), admission.binding.get(), binding, grant, request.pin_token);
        result.grant = grant;
    }
    if (status != 0) return status;
    static_cast<void>(candidate.release());
    response = result;
    name = staged;
    return 0;
}

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
        std::array<bool, VFS_PROXY_LANE_COUNT> live_lane_has_requested_rdma{};
        size_t live_lane_count = 0;
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
            bool const HAS_REQUESTED_RDMA =
                PREFER_RDMA && vfs_proxy_lane_has_requested_rdma(candidate, prefer_rdma_read_anchor, prefer_rdma_write_anchor, NOW_US);
            live_lanes.at(live_lane_count) = candidate;
            live_lane_has_requested_rdma.at(live_lane_count) = HAS_REQUESTED_RDMA;
            live_lane_count++;
        }

        if (live_lane_count > 0) {
            // Prefer an idle lane first, then the fewest in-flight selections
            // and pinned remote FDs. lifecycle_refs is incremented by this
            // function while still holding s_vfs_lock, so it also closes the
            // handoff window before an open records its durable reference.
            // RDMA is only the final tie-breaker: a mount where registration
            // succeeded on two of eight lanes must not queue every compiler
            // behind those two stop-and-wait bindings. Cursor order keeps
            // equal-pressure selections evenly striped.
            auto const START = static_cast<size_t>(anchor->lane_selection_cursor++ % live_lane_count);
            bool selected_busy = true;
            uint32_t selected_lifecycle_refs = UINT32_MAX;
            uint32_t selected_open_refs = UINT32_MAX;
            bool selected_has_requested_rdma = false;
            for (size_t offset = 0; offset < live_lane_count; ++offset) {
                size_t const INDEX = (START + offset) % live_lane_count;
                auto* candidate = live_lanes.at(INDEX);
                bool const BUSY = candidate->op_pending.load(std::memory_order_acquire) ||
                                  candidate->op_untracked_send_pending.load(std::memory_order_acquire);
                uint32_t const LIFECYCLE_REFS = candidate->lifecycle_refs;
                uint32_t const OPEN_REFS = candidate->open_file_refs.load(std::memory_order_acquire);
                bool const HAS_REQUESTED_RDMA = live_lane_has_requested_rdma.at(INDEX);
                bool const LOWER_PRESSURE =
                    selected == nullptr || (selected_busy && !BUSY) ||
                    (selected_busy == BUSY && LIFECYCLE_REFS < selected_lifecycle_refs) ||
                    (selected_busy == BUSY && LIFECYCLE_REFS == selected_lifecycle_refs && OPEN_REFS < selected_open_refs) ||
                    (selected_busy == BUSY && LIFECYCLE_REFS == selected_lifecycle_refs && OPEN_REFS == selected_open_refs &&
                     HAS_REQUESTED_RDMA && !selected_has_requested_rdma);
                if (!LOWER_PRESSURE) {
                    continue;
                }
                selected = candidate;
                selected_busy = BUSY;
                selected_lifecycle_refs = LIFECYCLE_REFS;
                selected_open_refs = OPEN_REFS;
                selected_has_requested_rdma = HAS_REQUESTED_RDMA;
            }
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
        ctx->read_cache->validation.invalidate();
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
        state->binding_retirement_confirmed = true;
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

// Retained for the diagnostic cache-generation tests; mapped READLINK never stores here.
[[maybe_unused]] void cache_readlink_result(ProxyVfsState* state, uint32_t expected_generation, const char* fs_relative_path, int status,
                                            const char* target, uint16_t target_len) {
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

struct ProxySlotCaller {
    uint64_t pid = 0;
    ker::mod::sched::task::TaskType type = ker::mod::sched::task::TaskType::IDLE;
};

auto current_proxy_slot_caller() -> ProxySlotCaller;
void park_proxy_slot_caller(const ProxySlotCaller& caller, uint64_t deadline_us, bool registered);

auto proxy_acquire_shared_io_slot(ProxyVfsState* state, uint64_t start_us) -> int {
    if (state == nullptr) {
        return WKI_OK;
    }

    uint64_t const DEADLINE_US = wki_future_deadline_us(start_us, VFS_PROXY_SLOT_WAIT_TIMEOUT_US);
    ProxySlotCaller const CALLER = current_proxy_slot_caller();
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
        park_proxy_slot_caller(CALLER, DEADLINE_US, false);
    }
}

void proxy_release_shared_io_slot(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }
    state->shared_io_in_use.store(false, std::memory_order_release);
}

class SharedIoTaskOwner;
SharedIoTaskOwner* g_shared_io_owners = nullptr;
class SharedIoTaskOwner {
   public:
    explicit SharedIoTaskOwner(ProxyVfsState* state, uint64_t pid = perf_current_pid()) : state_(state), pid_(pid) {
        if (state_ == nullptr) return;
        s_vfs_lock.lock();
        next_ = g_shared_io_owners;
        if (next_ != nullptr) next_->previous_ = this;
        g_shared_io_owners = this;
        s_vfs_lock.unlock();
    }
    ~SharedIoTaskOwner() {
        s_vfs_lock.lock();
        auto* state = take_locked();
        s_vfs_lock.unlock();
        proxy_release_shared_io_slot(state);
    }
    SharedIoTaskOwner(const SharedIoTaskOwner&) = delete;
    auto operator=(const SharedIoTaskOwner&) -> SharedIoTaskOwner& = delete;
    static void cleanup_for_task(uint64_t pid) {
        if (pid == 0) return;
        for (;;) {
            s_vfs_lock.lock();
            auto* candidate = g_shared_io_owners;
            while (candidate != nullptr && candidate->pid_ != pid) candidate = candidate->next_;
            auto* state = candidate != nullptr ? candidate->take_locked() : nullptr;
            s_vfs_lock.unlock();
            if (candidate == nullptr) return;
            proxy_release_shared_io_slot(state);
        }
    }

   private:
    auto take_locked() -> ProxyVfsState* {
        if (state_ == nullptr) return nullptr;
        if (previous_ != nullptr)
            previous_->next_ = next_;
        else
            g_shared_io_owners = next_;
        if (next_ != nullptr) next_->previous_ = previous_;
        previous_ = nullptr;
        next_ = nullptr;
        return std::exchange(state_, nullptr);
    }
    ProxyVfsState* state_;
    uint64_t pid_;
    SharedIoTaskOwner* previous_ = nullptr;
    SharedIoTaskOwner* next_ = nullptr;
};

struct SharedIoSlotGuard {
    explicit SharedIoSlotGuard(ProxyVfsState* state_ref, uint64_t start_us)
        : state(state_ref), status(proxy_acquire_shared_io_slot(state_ref, start_us)), owner(status == WKI_OK ? state_ref : nullptr) {
        if (status != WKI_OK) {
            state = nullptr;
        }
    }
    ~SharedIoSlotGuard() = default;

    SharedIoSlotGuard(const SharedIoSlotGuard&) = delete;
    auto operator=(const SharedIoSlotGuard&) -> SharedIoSlotGuard& = delete;

    [[nodiscard]] auto acquired() const -> bool { return status == WKI_OK; }
    [[nodiscard]] auto result() const -> int { return status; }

    ProxyVfsState* state;
    int status;
    SharedIoTaskOwner owner;
};

struct OptionalSharedIoSlotGuard {
    OptionalSharedIoSlotGuard(ProxyVfsState* state_ref, bool enabled, uint64_t start_us)
        : state(enabled ? state_ref : nullptr),
          status(enabled ? proxy_acquire_shared_io_slot(state_ref, start_us) : WKI_OK),
          owner(enabled && status == WKI_OK ? state_ref : nullptr) {
        if (status != WKI_OK) state = nullptr;
    }
    ~OptionalSharedIoSlotGuard() = default;

    OptionalSharedIoSlotGuard(const OptionalSharedIoSlotGuard&) = delete;
    auto operator=(const OptionalSharedIoSlotGuard&) -> OptionalSharedIoSlotGuard& = delete;

    [[nodiscard]] auto acquired() const -> bool { return status == WKI_OK; }
    [[nodiscard]] auto result() const -> int { return status; }

    ProxyVfsState* state;
    int status = WKI_OK;
    SharedIoTaskOwner owner;
};

struct RoceTaggedReceive {
    uint32_t rkey = 0;
    uint16_t cookie = 0;
};

auto proxy_op_slot_busy(const ProxyVfsState* state) -> bool {
    return state->op_pending.load(std::memory_order_acquire) || state->op_untracked_send_pending.load(std::memory_order_acquire);
}

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
    state->op_lifetime_inner = 0;
    state->op_lifetime_challenge = 0;
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
        if (!wki_current_process_wait_can_park()) {
            // File-backed page faults can reach remote reads on a PROCESS task
            // outside a syscall safe point. Preserve IRQ/exception state there.
            // Pending signals also make syscall park return immediately; this
            // wait remains uninterruptible without becoming a hot park loop.
            ker::mod::sched::kern_yield();
            return;
        }
        // Syscall entry may have IF masked. The shared WKI eligibility check
        // deliberately accepts that state because preemptible_syscall_park()
        // preserves and restores the caller's interrupt state itself.
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

// Retry workers must neither park behind an unrelated operation nor overtake
// its FIFO waiters. Success leaves the lock held, like ordinary admission.
auto try_acquire_proxy_op_slot_locked(ProxyVfsState* state) -> int {
    state->lock.lock();
    if (!state->active) {
        state->lock.unlock();
        return WKI_ERR_PEER_FENCED;
    }
    if (proxy_op_slot_busy(state) || proxy_slot_waiter_head_locked(state) != 0) {
        state->lock.unlock();
        return WKI_ERR_BUSY;
    }
    return WKI_OK;
}

auto acquire_proxy_op_slot_locked(ProxyVfsState* state, uint64_t start_us) -> int {
    return acquire_proxy_slot_locked(state, start_us, false);
}

auto acquire_proxy_untracked_send_slot_locked(ProxyVfsState* state, uint64_t start_us) -> int {
    return acquire_proxy_slot_locked(state, start_us, true);
}

auto vfs_proxy_send_status_is_retryable(int status) -> bool {
    // These failures precede reliable sequence publication. Route admission can
    // also fail briefly while another task owns the peer lifecycle gate. Retain
    // the same request and channel identity; none of these retries can duplicate
    // an already-enqueued operation (even failed initial TX returns WKI_OK).
    return status == WKI_ERR_NO_CREDITS || status == WKI_ERR_NO_MEM || status == WKI_ERR_NO_ROUTE;
}

template <typename Send, typename Wait, typename Now>
auto retry_completed_vfs_reply(const WkiChannelIdentity& identity, const std::atomic<bool>* binding_retiring, uint64_t deadline_us,
                               Send&& send, Wait&& wait, Now&& now) -> int {
    auto retry_live = [&] {
        return binding_retiring != nullptr && !binding_retiring->load(std::memory_order_acquire) && now() < deadline_us &&
               wki_channel_generation_is_live(identity.channel, identity.peer_node_id, identity.channel_id, identity.generation);
    };
    int status = send(identity);
    while (vfs_proxy_send_status_is_retryable(status) && retry_live()) {
        wait();
        // A lifecycle writer may retire the binding while waiting for this
        // worker's reference. Release promptly instead of waiting on its gate.
        if (!retry_live()) break;
        status = send(identity);
    }
    return status;
}

auto send_completed_vfs_reply(const WkiChannelIdentity& identity, const void* response, uint16_t response_len,
                              const std::atomic<bool>* binding_retiring) -> int {
    uint64_t const DEADLINE_US = wki_future_deadline_us(wki_now_us(), VFS_PROXY_OP_TIMEOUT_US);
    ProxySlotCaller const CALLER = current_proxy_slot_caller();
    // The deferred worker retains its binding and this completed byte prefix.
    // Only pre-publication failures retry; WKI_OK transfers retransmit ownership
    // and must never repeat the filesystem operation or submit this response again.
    bool const CAN_WAIT = ker::mod::sched::interrupts_enabled() && ker::mod::sched::preempt_count() == 0 && CALLER.pid != 0 &&
                          CALLER.type == ker::mod::sched::task::TaskType::DAEMON;
    return retry_completed_vfs_reply(
        identity, CAN_WAIT ? binding_retiring : nullptr, DEADLINE_US,
        [&](const WkiChannelIdentity& retained) {
            return wki_send_on_channel_identity(retained, MsgType::DEV_OP_RESP, response, response_len);
        },
        [&] { park_proxy_slot_caller(CALLER, DEADLINE_US, false); }, [] { return wki_now_us(); });
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
    void reset() { release_vfs_proxy_lifecycle_ref(std::exchange(state, nullptr)); }

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
// Called only after acquiring the exact proxy operation slot. Exhaustion
// remains terminal; a cancelled or unsent request must never recycle its token.
auto take_ordinary_open_token_locked(ProxyVfsState* state) -> uint64_t {
    uint64_t const TOKEN = state->ordinary_next_token;
    if (TOKEN != 0) state->ordinary_next_token = TOKEN == UINT64_MAX ? 0 : TOKEN + 1;
    return TOKEN;
}

auto vfs_proxy_send_and_wait(ProxyVfsState* state, uint16_t op_id, const uint8_t* req_data, size_t req_data_len, void* resp_buf,
                             uint16_t resp_buf_max, uint16_t* resp_len_out = nullptr, uint64_t wait_timeout_us = VFS_PROXY_OP_TIMEOUT_US,
                             RoceTaggedReceive* tagged_receive = nullptr, const uint8_t* req_tail = nullptr, size_t req_tail_len = 0,
                             bool try_slot_only = false, const WkiChannelIdentity* required_identity = nullptr,
                             uint16_t ordinary_slot = VFS_EXEC_CANCEL_SLOT_NONE) -> int {
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint64_t const PROXY_WAIT_START = wki_now_us();

    if (resp_len_out != nullptr) {
        *resp_len_out = 0;
    }

    OrdinaryCancellation* ordinary = nullptr;
    VfsOpenLifetimeRequest lifetime{};
    if (ordinary_slot != VFS_EXEC_CANCEL_SLOT_NONE) {
        if (ordinary_slot >= g_open_cancellations.size()) return -EINVAL;
        ordinary = &g_open_cancellations.at(ordinary_slot);
        // OWNED is exclusive to this task until preparation transfers/abandons it.
        if (ordinary->phase != ExecCancellationPhase::OWNED || ordinary->proxy != state || ordinary->request.token != 0 ||
            (op_id != OP_VFS_OPEN_AS && op_id != OP_VFS_OPEN_READ_AS && op_id != OP_VFS_PATH_AS))
            return -EINVAL;
        lifetime = ordinary->request;
        lifetime.inner_op = op_id;
        required_identity = &ordinary->identity;
    }
    size_t const WRAPPER = ordinary != nullptr ? sizeof(lifetime) : 0;
    size_t const MAX_REQ_DATA_LEN = WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload) - WRAPPER;
    if (req_data_len > MAX_REQ_DATA_LEN || req_tail_len > MAX_REQ_DATA_LEN - req_data_len) {
        return -EMSGSIZE;
    }
    if ((req_data_len > 0 && req_data == nullptr) || (req_tail_len > 0 && req_tail == nullptr)) {
        return -EINVAL;
    }
    auto const REQ_DATA_LEN = static_cast<uint16_t>(WRAPPER + req_data_len + req_tail_len);
    size_t const REQ_PREFIX_TOTAL = sizeof(DevOpReqPayload) + WRAPPER + req_data_len;

    // The request prefix and optional tail initialize every transmitted byte;
    // do not pattern-fill unused bounded stack capacity.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> req_buf __attribute__((uninitialized));
    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
    req->op_id = ordinary != nullptr ? OP_VFS_OPEN_LIFETIME : op_id;
    req->data_len = REQ_DATA_LEN;

    if (req_data_len > 0) {
        memcpy(req_buf.data() + sizeof(DevOpReqPayload) + WRAPPER, req_data, req_data_len);
    }

    // Serialize until we can claim the proxy for this operation.
    // Another thread may be using the same proxy (e.g., concurrent stat+open).
    int const SLOT_RET = try_slot_only ? try_acquire_proxy_op_slot_locked(state) : acquire_proxy_op_slot_locked(state, PROXY_WAIT_START);
    auto const PROXY_WAIT_US = static_cast<uint32_t>(wki_now_us() - PROXY_WAIT_START);
    if (PROXY_WAIT_US > 0) {
        perf_record_vfs_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::PROXY_WAIT), state->owner_node, state->assigned_channel, 0,
                              PROXY_WAIT_US, CALLSITE);
    }
    if (SLOT_RET != WKI_OK) {
        return encode_proxy_wki_status(SLOT_RET);
    }

    WkiChannelIdentity const CHANNEL_IDENTITY = proxy_channel_identity_locked(state);
    if (required_identity != nullptr && !vfs_channel_identity_matches(CHANNEL_IDENTITY, *required_identity)) {
        unlock_proxy_slot_and_wake_next(state);
        return -ENOTCONN;
    }
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

    if (ordinary != nullptr) {
        lifetime.token = take_ordinary_open_token_locked(state);
        if (lifetime.token == 0) {
            unlock_proxy_slot_and_wake_next(state);
            return -EOVERFLOW;
        }
        lifetime.data_len = static_cast<uint16_t>(req_data_len + req_tail_len);
        std::memcpy(req_buf.data() + sizeof(DevOpReqPayload), &lifetime, sizeof(lifetime));
    } else if (op_id == OP_VFS_OPEN_LIFETIME) {
        if (req_data_len < sizeof(lifetime)) {
            unlock_proxy_slot_and_wake_next(state);
            return -EINVAL;
        }
        std::memcpy(&lifetime, req_data, sizeof(lifetime));
    }

    // Lock held, op_pending still false - set up wait entry and response fields
    WkiWaitEntry wait = {};
    uint64_t const OP_GENERATION = advance_proxy_op_generation_locked(state);
    state->op_wait_entry = &wait;
    state->op_waiter_pid = perf_current_pid();
    state->op_expected_id = req->op_id;
    state->op_lifetime_inner = lifetime.inner_op;
    state->op_lifetime_challenge = lifetime.token;
    state->op_expected_seq = expected_seq;
    state->op_status = 0;
    state->op_resp_buf = resp_buf;
    state->op_resp_max = resp_buf_max;
    state->op_resp_len = 0;
    // Publish the request identity before making the slot visible to the RX path.
    state->op_pending.store(true, std::memory_order_release);
    state->lock.unlock();

    if (ordinary != nullptr) {
        // No registry lock nests under state->lock. Task-exit cleanup runs only
        // after this caller stops and its stack wait/response users quiesce.
        s_vfs_lock.lock();
        ordinary->request.token = lifetime.token;
        s_vfs_lock.unlock();
    }

    uint64_t const STARTED_US = wki_now_us();
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id),
                                     ker::mod::perf::WkiPerfPhase::BEGIN, state->owner_node, state->assigned_channel, CORRELATION, 0,
                                     REQ_DATA_LEN, CALLSITE);

    uint64_t const SEND_DEADLINE_US = wki_future_deadline_us(PROXY_WAIT_START, wait_timeout_us);
    ProxySlotCaller const SEND_CALLER = current_proxy_slot_caller();
    int send_ret = WKI_ERR_NO_CREDITS;
    while (true) {
        if (req_tail_len == 0) {
            send_ret = wki_send_on_channel_identity(CHANNEL_IDENTITY, MsgType::DEV_OP_REQ, req_buf.data(),
                                                    static_cast<uint16_t>(REQ_PREFIX_TOTAL));
        } else {
            send_ret =
                wki_send_on_channel_identity_split(CHANNEL_IDENTITY, MsgType::DEV_OP_REQ, req_buf.data(),
                                                   static_cast<uint16_t>(REQ_PREFIX_TOTAL), req_tail, static_cast<uint16_t>(req_tail_len));
        }
        if (send_ret == WKI_OK || !vfs_proxy_send_status_is_retryable(send_ret)) {
            break;
        }
        if (wki_now_us() >= SEND_DEADLINE_US) {
            send_ret = send_ret == WKI_ERR_NO_ROUTE ? WKI_ERR_NO_ROUTE : WKI_ERR_TIMEOUT;
            break;
        }
        // Retain the published operation and proxy slot while reliable-channel
        // pressure clears.  These errors precede sequence publication, so the
        // expected response sequence remains stable across retries.
        park_proxy_slot_caller(SEND_CALLER, SEND_DEADLINE_US, false);
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

// Reserve before PIN while the source File already keeps its proxy alive.
// This independent open reference survives destruction of the private File.
auto reserve_exec_cancellation(ProxyVfsState* state, uint64_t token, bool directory = false, const WkiChannelIdentity* expected = nullptr)
    -> uint16_t {
    s_vfs_lock.lock();
    if (!state->active || state->destroy_when_idle || state->resources_releasing || state->resources_released || token == 0 ||
        (expected != nullptr && !vfs_channel_identity_matches(proxy_channel_identity_locked(state), *expected))) {
        s_vfs_lock.unlock();
        return VFS_EXEC_CANCEL_SLOT_NONE;
    }
    for (size_t index = 0; index < g_exec_cancellations.size(); ++index) {
        auto& record = g_exec_cancellations.at(index);
        if (record.phase != ExecCancellationPhase::FREE) continue;
        record = {.phase = ExecCancellationPhase::OWNED,
                  .proxy = state,
                  .identity = proxy_channel_identity_locked(state),
                  .token = token,
                  .peer_boot_epoch = state->binding_peer_boot_epoch,
                  .directory = directory};
        state->open_file_refs.fetch_add(1, std::memory_order_release);
        s_vfs_lock.unlock();
        return static_cast<uint16_t>(index);
    }
    s_vfs_lock.unlock();
    return VFS_EXEC_CANCEL_SLOT_NONE;
}

// Successful cancellation or an exact, durably admitted binding retirement
// releases ownership. All other outcomes retain the token for another attempt.
void finish_exec_cancellation(uint16_t slot, int status, bool retirement_proven, bool notify) {
    ProxyVfsState* released = nullptr;
    s_vfs_lock.lock();
    auto& record = g_exec_cancellations.at(slot);
    if (status == 0 || retirement_proven) {
        if (retirement_proven)
            ++pin_cancellation_totals(record.directory).retirement_handoffs;
        else
            ++pin_cancellation_totals(record.directory).cancelled;
        released = record.proxy;
        record = {};
    } else {
        ++pin_cancellation_totals(record.directory).failed_attempts;
        record.retry_after_us = wki_future_deadline_us(wki_now_us(), VFS_EXEC_CANCEL_RETRY_INTERVAL_US);
        record.phase = ExecCancellationPhase::PENDING;
    }
    s_vfs_lock.unlock();
    release_vfs_proxy_open_ref(released);
    if (notify && released == nullptr) wki_deferred_work_notify();
}

auto exec_cancellation_retirement_proven(const ExecCancellation& record) -> bool {
    if (wki_peer_remote_boot_epoch_invalidated(record.identity.peer_node_id, record.peer_boot_epoch)) return true;
    s_vfs_lock.lock();
    bool const CONFIRMED = record.proxy->binding_retirement_confirmed &&
                           vfs_channel_identity_matches(proxy_channel_identity_locked(record.proxy), record.identity);
    s_vfs_lock.unlock();
    return CONFIRMED;
}

auto cancel_remote_exec_view(uint16_t slot, bool retry = false) -> int {
    if (slot == VFS_EXEC_CANCEL_SLOT_NONE) return -EINVAL;
    auto const& record = g_exec_cancellations.at(slot);
    bool const RETIRED = exec_cancellation_retirement_proven(record);
    int result = -ENOTCONN;
    if (!RETIRED) {
        s_vfs_lock.lock();
        ++pin_cancellation_totals(record.directory).attempts;
        s_vfs_lock.unlock();
        VfsExecViewRequest request{.operation = VfsExecViewOperation::CANCEL, .token = record.token};
        VfsDirectoryRequest directory{.operation = VfsDirectoryOperation::CANCEL, .directory_fd = 0, .token = record.token};
        uint16_t length = 0;
        result = normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(
            record.proxy, record.directory ? OP_VFS_DIRECTORY_REFERENCE : OP_VFS_EXEC_VIEW,
            record.directory ? reinterpret_cast<const uint8_t*>(&directory) : reinterpret_cast<const uint8_t*>(&request),
            record.directory ? sizeof(directory) : sizeof(request), nullptr, 0, &length,
            retry ? VFS_EXEC_CANCEL_RETRY_TIMEOUT_US : VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, retry, &record.identity));
        if (result == 0 && length != 0) result = -EPROTO;
    }
    finish_exec_cancellation(slot, result, RETIRED, !retry);
    return result;
}

auto prepare_ordinary_open_binding(ProxyVfsState* state) -> int {
    if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_RESOURCE_INCARNATION)) return -ENOTSUP;
    s_vfs_lock.lock();
    auto const IDENTITY = proxy_channel_identity_locked(state);
    uint32_t const EPOCH = state->binding_peer_boot_epoch;
    bool const READY = state->ordinary_open_binding_id != 0 && state->ordinary_owner_boot_epoch == EPOCH;
    uint64_t const CHALLENGE = READY ? 0 : take_directory_grant_id(g_next_open_query_challenge);
    s_vfs_lock.unlock();
    if (READY) return 0;
    if (CHALLENGE == 0) return -EOVERFLOW;
    VfsFeaturesRequest const query{WKI_VFS_FEATURES_VERSION, 0};
    VfsFeaturesResponse features{};
    uint16_t length = 0;
    int status = normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(state, OP_VFS_FEATURES, reinterpret_cast<const uint8_t*>(&query),
                                                                          sizeof(query), &features, sizeof(features), &length,
                                                                          VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &IDENTITY));
    if (status != 0) return status;
    if (!wki_vfs_features_response_valid(features, length)) return -EPROTO;
    if ((features.features & WKI_VFS_FEATURE_OPEN_LIFETIME) == 0) return -ENOTSUP;
    VfsOpenLifetimeRequest const request{.inner_op = OP_VFS_FEATURES, .token = CHALLENGE};
    VfsOpenLifetimeRequest response{};
    status = normalize_proxy_status_for_errno(
        vfs_proxy_send_and_wait(state, OP_VFS_OPEN_LIFETIME, reinterpret_cast<const uint8_t*>(&request), sizeof(request), &response,
                                sizeof(response), &length, VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &IDENTITY));
    if (status != 0) return status;
    if (!wki_vfs_open_binding_response_valid(response, CHALLENGE, length) || response.owner_boot_epoch != EPOCH) return -EPROTO;
    s_vfs_lock.lock();
    if (!state->active || !vfs_channel_identity_matches(proxy_channel_identity_locked(state), IDENTITY) ||
        state->binding_peer_boot_epoch != EPOCH)
        status = -ENOTCONN;
    else if (state->ordinary_open_binding_id != 0 && state->ordinary_open_binding_id != response.binding_id)
        status = -EPROTO;
    else {
        state->ordinary_open_binding_id = response.binding_id;
        state->ordinary_owner_boot_epoch = response.owner_boot_epoch;
    }
    s_vfs_lock.unlock();
    return status;
}

auto reserve_ordinary_cancellation(ProxyVfsState* state) -> uint16_t {
    s_vfs_lock.lock();
    if (!state->active || state->destroy_when_idle || state->resources_releasing || state->resources_released ||
        state->ordinary_open_binding_id == 0 || state->ordinary_owner_boot_epoch == 0 ||
        state->ordinary_owner_boot_epoch != state->binding_peer_boot_epoch) {
        s_vfs_lock.unlock();
        return VFS_EXEC_CANCEL_SLOT_NONE;
    }
    for (size_t i = 0; i < g_open_cancellations.size(); ++i) {
        auto& record = g_open_cancellations.at(i);
        if (record.phase != ExecCancellationPhase::FREE) continue;
        record = {.phase = ExecCancellationPhase::OWNED,
                  .proxy = state,
                  .identity = proxy_channel_identity_locked(state),
                  .request = {.binding_id = state->ordinary_open_binding_id, .owner_boot_epoch = state->ordinary_owner_boot_epoch},
                  .peer_boot_epoch = state->binding_peer_boot_epoch};
        state->open_file_refs.fetch_add(1, std::memory_order_release);
        s_vfs_lock.unlock();
        return static_cast<uint16_t>(i);
    }
    s_vfs_lock.unlock();
    return VFS_EXEC_CANCEL_SLOT_NONE;
}

// Caller owns s_vfs_lock. Preparation has exclusive slot ownership until this
// handoff; a closing File additionally matches its saved key against slot reuse.
auto queue_ordinary_cancellation_locked(uint16_t slot, ProxyVfsState* proxy, const VfsOpenLifetimeRequest* expected = nullptr) -> bool {
    if (slot >= g_open_cancellations.size()) return false;
    auto& record = g_open_cancellations.at(slot);
    if (record.phase != ExecCancellationPhase::OWNED || record.proxy != proxy ||
        (expected != nullptr && (record.request.token != expected->token || record.request.binding_id != expected->binding_id ||
                                 record.request.owner_boot_epoch != expected->owner_boot_epoch)))
        return false;
    record.phase = ExecCancellationPhase::PENDING;
    record.retry_after_us = 0;
    return true;
}

void finish_ordinary_cancellation(uint16_t slot, int status, bool retirement_proven, bool notify) {
    ProxyVfsState* released = nullptr;
    s_vfs_lock.lock();
    auto& finish = g_open_cancellations.at(slot);
    if (status == 0 || retirement_proven) {
        released = finish.proxy;
        finish = {};
    } else {
        finish.phase = ExecCancellationPhase::PENDING;
        finish.retry_after_us = wki_future_deadline_us(wki_now_us(), VFS_EXEC_CANCEL_RETRY_INTERVAL_US);
    }
    s_vfs_lock.unlock();
    release_vfs_proxy_open_ref(released);
    if (released == nullptr && notify) wki_deferred_work_notify();
}

auto cancel_remote_ordinary_open(uint16_t slot, bool retry = false) -> int {
    if (slot >= g_open_cancellations.size()) return -EINVAL;
    auto const& record = g_open_cancellations.at(slot);
    bool retired = record.request.token == 0;  // No mutating request was constructed.
    if (!retired) retired = wki_peer_remote_boot_epoch_invalidated(record.identity.peer_node_id, record.peer_boot_epoch);
    if (!retired) {
        s_vfs_lock.lock();
        retired = record.proxy->binding_retirement_confirmed &&
                  vfs_channel_identity_matches(proxy_channel_identity_locked(record.proxy), record.identity);
        s_vfs_lock.unlock();
    }
    int status = 0;
    if (!retired) {
        uint16_t length = 0;
        status = normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(
            record.proxy, OP_VFS_OPEN_LIFETIME, reinterpret_cast<const uint8_t*>(&record.request), sizeof(record.request), nullptr, 0,
            &length, retry ? VFS_EXEC_CANCEL_RETRY_TIMEOUT_US : VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, retry, &record.identity));
        if (status == 0 && length != 0) status = -EPROTO;
    }
    finish_ordinary_cancellation(slot, status, retired, !retry);
    return status;
}

// Private retained-pin allocations must survive an abandoned kernel stack. Task
// exit first quiesces proxy/wait references, then drains this intrusive list.
// No registry allocation is needed, and cancellation uses the reserved outbox.
enum class PinPurpose : uint8_t { DIRECTORY, EXECUTABLE, ORDINARY_OPEN };
class PinPreparation;
PinPreparation* g_pin_preparations = nullptr;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
#ifdef WOS_SELFTEST
std::atomic<size_t> g_pin_private_files_freed{0};
std::atomic<size_t> g_pin_private_contexts_freed{0};
std::atomic<size_t> g_pin_private_requests_freed{0};
std::atomic<uint64_t> g_preparation_failure_pid{0};
std::atomic<unsigned> g_preparation_failure_allocation{0};
#endif
auto fail_private_preparation_allocation(uint64_t pid, unsigned allocation) -> bool {
#ifdef WOS_SELFTEST
    return pid != 0 && pid == g_preparation_failure_pid && allocation == g_preparation_failure_allocation;
#else
    static_cast<void>(pid);
    static_cast<void>(allocation);
    return false;
#endif
}
class PinPreparation {
   public:
    explicit PinPreparation(PinPurpose purpose, uint64_t pid = perf_current_pid())
        : file_(fail_private_preparation_allocation(pid, 1) ? nullptr : new (std::nothrow) ker::vfs::File{}),
          context_(fail_private_preparation_allocation(pid, 2) ? nullptr : new (std::nothrow) RemoteFileContext{}),
          pid_(pid),
          purpose_(purpose) {
        if (!file_ || !context_) {
            status_ = -ENOMEM;
            return;
        }
        link();
    }
    PinPreparation(ProxyVfsState* state, PinPurpose purpose, uint64_t pid = perf_current_pid()) : PinPreparation(purpose, pid) {
        if (status_ == 0) status_ = retain_proxy(state);
    }
    // OPEN_AT's caller has already prepared a private File with path/mount
    // ownership. Only actual stack abandonment transfers its destruction here;
    // normal failure leaves the caller's unique owner intact.
    PinPreparation(ProxyVfsState* state, PinPurpose purpose, ker::vfs::File& prepared, uint64_t pid = perf_current_pid())
        : file_(&prepared), pid_(pid), purpose_(purpose), borrowed_file_(true) {
        // Preserve OPEN_AT's proxy-admission-before-context-allocation errno.
        if (!acquire_vfs_proxy_open_ref(state)) {
            status_ = -ENOTCONN;
            return;
        }
        proxy_ = state;
        link();
        context_ = fail_private_preparation_allocation(pid, 2) ? nullptr : new (std::nothrow) RemoteFileContext{};
        if (context_ == nullptr) status_ = -ENOMEM;
    }
    // Separate allocation and proxy admission preserve executable cloning's
    // existing metadata-allocation/content-ID/error ordering before retention.
    auto retain_proxy(ProxyVfsState* state) -> int {
        if (status_ != 0) return status_;
        if (proxy_ != nullptr) return -EINVAL;
        if (!acquire_vfs_proxy_open_ref(state)) return -ENOTCONN;
        s_vfs_lock.lock();
        proxy_ = state;
        s_vfs_lock.unlock();
        return 0;
    }
    ~PinPreparation() { cancel(); }
    PinPreparation(const PinPreparation&) = delete;
    auto operator=(const PinPreparation&) -> PinPreparation& = delete;
    auto status() const -> int { return status_; }
    auto file() const -> ker::vfs::File* { return file_; }
    auto context() const -> RemoteFileContext* { return context_; }
    // Scalar OPEN's long request must be owned across the same fatal waits as
    // its private File/context. The normal short request remains stack-local.
    auto allocate_request(size_t length) -> uint8_t* {
        if (status_ != 0 || request_ != nullptr) return nullptr;
        auto* candidate = fail_private_preparation_allocation(pid_, 3) ? nullptr : new (std::nothrow) uint8_t[length];
        s_vfs_lock.lock();
        bool const ACCEPTED = status_ == 0;
        if (ACCEPTED) {
            request_ = candidate;
            if (candidate == nullptr) status_ = -ENOMEM;
        }
        s_vfs_lock.unlock();
        if (!ACCEPTED) delete[] candidate;
        return ACCEPTED ? candidate : nullptr;
    }
    void own_cancellation(uint16_t slot, uint64_t token) {
        s_vfs_lock.lock();
        slot_ = slot;
        token_ = token;
        s_vfs_lock.unlock();
    }
    auto prepare_ordinary_open() -> int {
        int const SUPPORT = prepare_ordinary_open_binding(proxy_);
        if (SUPPORT != 0) return SUPPORT;
        uint16_t const SLOT = reserve_ordinary_cancellation(proxy_);
        if (SLOT == VFS_EXEC_CANCEL_SLOT_NONE) return -EAGAIN;
        own_ordinary_cancellation(SLOT);
        return 0;
    }
    void own_ordinary_cancellation(uint16_t slot) {
        s_vfs_lock.lock();
        ordinary_slot_ = slot;
        s_vfs_lock.unlock();
    }
    auto ordinary_slot() const -> uint16_t { return ordinary_slot_; }

    // Success has initialized both objects. Transfer every ownership together;
    // no blocking operation separates this from caller output publication.
    auto publish() -> ker::vfs::File* {
        s_vfs_lock.lock();
        unlink_locked();
        auto* result = std::exchange(file_, nullptr);
        context_->ordinary_cancel_slot = ordinary_slot_;
        if (ordinary_slot_ < g_open_cancellations.size()) {
            auto const& record = g_open_cancellations.at(ordinary_slot_);
            context_->ordinary_open_token = record.request.token;
            context_->ordinary_open_binding_id = record.request.binding_id;
            context_->ordinary_owner_boot_epoch = record.request.owner_boot_epoch;
            context_->ordinary_resource_incarnation = proxy_->owner_incarnation.resource_incarnation;
            context_->ordinary_resource_id = proxy_->resource_id;
        }
        ordinary_slot_ = VFS_EXEC_CANCEL_SLOT_NONE;
        result->private_data = std::exchange(context_, nullptr);
        proxy_ = nullptr;
        slot_ = VFS_EXEC_CANCEL_SLOT_NONE;
        token_ = 0;
        s_vfs_lock.unlock();
        return result;
    }
    void cancel() {
        s_vfs_lock.lock();
        auto retired = detach_locked();
        s_vfs_lock.unlock();
        drain(retired);
    }
    static void cleanup_for_task(uint64_t pid) {
        if (pid == 0) return;
        for (;;) {
            s_vfs_lock.lock();
            auto* candidate = g_pin_preparations;
            while (candidate != nullptr && candidate->pid_ != pid) candidate = candidate->next_;
            Retired retired{};
            if (candidate != nullptr) retired = candidate->detach_locked(true);
            s_vfs_lock.unlock();
            if (candidate == nullptr) return;
            drain(retired);
        }
    }

   private:
    void link() {
        s_vfs_lock.lock();
        next_ = g_pin_preparations;
        if (next_ != nullptr) next_->previous_ = this;
        g_pin_preparations = this;
        linked_ = true;
        s_vfs_lock.unlock();
    }
    struct Retired {
        ker::vfs::File* file = nullptr;
        RemoteFileContext* context = nullptr;
        ProxyVfsState* proxy = nullptr;
        uint8_t* request = nullptr;
        bool queued = false;
        bool finish_file = false;
    };
    void unlink_locked() {
        if (!linked_) return;
        if (previous_ != nullptr)
            previous_->next_ = next_;
        else
            g_pin_preparations = next_;
        if (next_ != nullptr) next_->previous_ = previous_;
        previous_ = nullptr;
        next_ = nullptr;
        linked_ = false;
    }
    auto detach_locked(bool abandoned = false) -> Retired {
        unlink_locked();
        Retired retired{std::exchange(file_, nullptr), std::exchange(context_, nullptr), std::exchange(proxy_, nullptr),
                        std::exchange(request_, nullptr)};
        retired.finish_file = borrowed_file_ && abandoned;
        if (borrowed_file_ && !abandoned) retired.file = nullptr;
        if (slot_ < g_exec_cancellations.size()) {
            auto& record = g_exec_cancellations.at(slot_);
            if (record.phase == ExecCancellationPhase::OWNED && record.proxy == retired.proxy && record.token == token_ &&
                record.directory == (purpose_ == PinPurpose::DIRECTORY)) {
                record.phase = ExecCancellationPhase::PENDING;
                record.retry_after_us = 0;
                retired.queued = true;
            }
        }
        retired.queued = queue_ordinary_cancellation_locked(ordinary_slot_, retired.proxy) || retired.queued;
        ordinary_slot_ = VFS_EXEC_CANCEL_SLOT_NONE;
        slot_ = VFS_EXEC_CANCEL_SLOT_NONE;
        token_ = 0;
        status_ = -ECANCELED;
        return retired;
    }
    static void drain(Retired& retired) {
#ifdef WOS_SELFTEST
        g_pin_private_files_freed.fetch_add(retired.file != nullptr ? 1U : 0U, std::memory_order_relaxed);
        g_pin_private_contexts_freed.fetch_add(retired.context != nullptr ? 1U : 0U, std::memory_order_relaxed);
        g_pin_private_requests_freed.fetch_add(retired.request != nullptr ? 1U : 0U, std::memory_order_relaxed);
#endif
        // These objects have never been published and own no backend File.
        // The outbox retains its own proxy ref independently of this release.
        if (retired.finish_file)
            ker::vfs::vfs_put_file(retired.file);
        else
            delete retired.file;
        if (retired.context != nullptr) {
            clear_remote_file_open_prefetch(retired.context);
            delete retired.context->read_cache;
            delete retired.context->write_buf;
        }
        delete retired.context;
        delete[] retired.request;
        release_vfs_proxy_open_ref(retired.proxy);
        if (retired.queued) wki_deferred_work_notify();
        retired = {};
    }
    ker::vfs::File* file_ = nullptr;
    RemoteFileContext* context_ = nullptr;
    ProxyVfsState* proxy_ = nullptr;
    uint8_t* request_ = nullptr;
    uint64_t pid_ = 0;
    uint64_t token_ = 0;
    uint16_t slot_ = VFS_EXEC_CANCEL_SLOT_NONE;
    uint16_t ordinary_slot_ = VFS_EXEC_CANCEL_SLOT_NONE;
    PinPurpose purpose_;
    bool borrowed_file_ = false;
    int status_ = 0;
    bool linked_ = false;
    PinPreparation* previous_ = nullptr;
    PinPreparation* next_ = nullptr;
};

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

    uint64_t const SEND_DEADLINE_US = wki_future_deadline_us(PROXY_WAIT_START, VFS_PROXY_OP_TIMEOUT_US);
    ProxySlotCaller const SEND_CALLER = current_proxy_slot_caller();
    int send_ret = WKI_ERR_NO_CREDITS;
    while (true) {
        send_ret = wki_send_on_channel_identity(CHANNEL_IDENTITY, MsgType::DEV_OP_REQ, req_buf.data(), static_cast<uint16_t>(REQ_TOTAL));
        if (send_ret == WKI_OK || !vfs_proxy_send_status_is_retryable(send_ret)) {
            break;
        }
        if (wki_now_us() >= SEND_DEADLINE_US) {
            send_ret = send_ret == WKI_ERR_NO_ROUTE ? WKI_ERR_NO_ROUTE : WKI_ERR_TIMEOUT;
            break;
        }
        // Keep ownership of the proxy slot so later RPCs cannot overtake this
        // close.  The shared park helper preserves syscall IF state and lets a
        // WKI progress daemon drive receive/ACK work instead of sleeping.
        park_proxy_slot_caller(SEND_CALLER, SEND_DEADLINE_US, false);
    }
    int const SEND_RET = send_ret;

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

[[maybe_unused]] auto vfs_proxy_write_rdma_and_wait(ProxyVfsState* state, int32_t remote_fd, int64_t offset, const uint8_t* src,
                                                    uint32_t chunk, uint32_t* written_out) -> int {
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

    uint64_t const SEND_DEADLINE_US = wki_future_deadline_us(PROXY_WAIT_START, VFS_PROXY_OP_TIMEOUT_US);
    ProxySlotCaller const SEND_CALLER = current_proxy_slot_caller();
    int send_ret = WKI_ERR_NO_CREDITS;
    while (true) {
        send_ret =
            wki_send_on_channel_identity(CHANNEL_IDENTITY, MsgType::DEV_OP_REQ, req_buf.data(), static_cast<uint16_t>(req_buf.size()));
        if (send_ret == WKI_OK || !vfs_proxy_send_status_is_retryable(send_ret)) {
            break;
        }
        if (wki_now_us() >= SEND_DEADLINE_US) {
            send_ret = send_ret == WKI_ERR_NO_ROUTE ? WKI_ERR_NO_ROUTE : WKI_ERR_TIMEOUT;
            break;
        }
        // Preserve control-before-data ordering for RoCE and keep the operation
        // sequence stable while pre-enqueue channel pressure clears.
        park_proxy_slot_caller(SEND_CALLER, SEND_DEADLINE_US, false);
    }
    int const SEND_RET = send_ret;
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
    if (op_id != OP_VFS_READLINK && op_id != OP_VFS_PATH_AS) {
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
                               RoceTaggedReceive* tagged_receive = nullptr, const WkiChannelIdentity* required_identity = nullptr) -> int {
    for (uint32_t attempt = 0;; ++attempt) {
        int const STATUS =
            vfs_proxy_send_and_wait(state, op_id, req_data, req_data_len, resp_buf, resp_buf_max, resp_len_out,
                                    vfs_read_retry_timeout_us(op_id, attempt), tagged_receive, nullptr, 0, false, required_identity);
        if (!vfs_read_status_is_retryable(STATUS) || attempt >= VFS_READ_RETRIES) {
            return normalize_proxy_status_for_errno(STATUS);
        }

        perf_record_vfs_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::RETRY), state->owner_node, state->assigned_channel, STATUS,
                              attempt + 1, WOS_PERF_CALLSITE());

        if ((op_id == OP_VFS_READLINK || op_id == OP_VFS_PATH_AS) && debug_path != nullptr) {
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

[[maybe_unused]] auto try_reserve_write_behind(WriteBehindBuffer* wb, uint64_t required_bytes) -> bool {
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

// Capture immutable ordinary authority before secure or message I/O. Passing the
// captured identity to fallback excludes a successor channel reusing numeric fd.
auto capture_secure_vfs_client(const RemoteFileContext& context, WkiChannelIdentity& identity) -> int {
    if (context.ordinary_open_token == 0) return -ENOTSUP;
    s_vfs_lock.lock();
    int status = -ENOTCONN;
    if (context.proxy != nullptr && context.proxy->active && !context.proxy->epoch_reset_pending && !context.proxy->resources_releasing &&
        !context.proxy->resources_released && context.ordinary_cancel_slot < g_open_cancellations.size()) {
        auto const& record = g_open_cancellations.at(context.ordinary_cancel_slot);
        if (record.phase == ExecCancellationPhase::OWNED && record.proxy == context.proxy &&
            record.request.token == context.ordinary_open_token && record.request.binding_id == context.ordinary_open_binding_id &&
            record.request.owner_boot_epoch == context.ordinary_owner_boot_epoch &&
            vfs_channel_identity_matches(record.identity, proxy_channel_identity_locked(context.proxy))) {
            identity = record.identity;
            status = 0;
        }
    }
    s_vfs_lock.unlock();
    return status;
}

// io_lock holds the exact open stable. Capability information is never shared
// with another binding or substituted for a successful version validation.
auto query_remote_owner_features(RemoteFileContext& context, WkiChannelIdentity& identity) -> int {
    int const AUTHORITY = capture_secure_vfs_client(context, identity);
    if (AUTHORITY != 0) return AUTHORITY;
    if (!context.owner_features_known) {
        VfsFeaturesRequest const request{WKI_VFS_FEATURES_VERSION, 0};
        VfsFeaturesResponse response{};
        uint16_t length = 0;
        int const STATUS = normalize_proxy_status_for_errno(
            vfs_proxy_send_and_wait(context.proxy, OP_VFS_FEATURES, reinterpret_cast<const uint8_t*>(&request), sizeof(request), &response,
                                    sizeof(response), &length, VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &identity));
        if (STATUS != 0) return STATUS;
        if (!wki_vfs_features_response_valid(response, length)) return -EPROTO;
        context.owner_features = response.features;
        context.owner_features_known = true;
    }
    return 0;
}

auto query_remote_owner_version(RemoteFileContext& context, ker::vfs::coherency::Stamp& output) -> int {
    WkiChannelIdentity identity{};
    int const FEATURES = query_remote_owner_features(context, identity);
    if (FEATURES != 0) return FEATURES;
    if ((context.owner_features & WKI_VFS_FEATURE_OWNER_VERSIONS) == 0) return -EOPNOTSUPP;
    static ker::vfs::coherency::Counter queries;
    uint64_t const QUERY = queries.take();
    if (QUERY == 0) return -EOVERFLOW;
    VfsOwnerFdRequest const request{.version = WKI_VFS_OWNER_VERSION,
                                    .fd = context.remote_fd,
                                    .open_token = context.ordinary_open_token,
                                    .binding_id = context.ordinary_open_binding_id,
                                    .owner_boot_epoch = context.ordinary_owner_boot_epoch,
                                    .query_id = QUERY};
    VfsVersionResponse response{};
    uint16_t length = 0;
    int const STATUS = normalize_proxy_status_for_errno(
        vfs_proxy_send_and_wait(context.proxy, OP_VFS_VERSION, reinterpret_cast<const uint8_t*>(&request), sizeof(request), &response,
                                sizeof(response), &length, VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &identity));
    if (STATUS != 0) return STATUS;
    if (!wki_vfs_version_response_valid(response, QUERY, length)) return -EPROTO;
    output = {response.object, response.content, response.metadata, response.directory};
    return 0;
}

auto make_secure_vfs_intent(const RemoteFileContext& context, rdma_protocol::Opcode opcode, int64_t offset, uint32_t length)
    -> VfsSecureIoIntent {
    return {.version = 1,
            .opcode = static_cast<uint8_t>(opcode),
            .fd = context.remote_fd,
            .file_offset = offset,
            .length = length,
            .binding_id = context.ordinary_open_binding_id,
            .open_token = context.ordinary_open_token,
            .owner_boot_epoch = context.ordinary_owner_boot_epoch,
            .resource_incarnation = context.ordinary_resource_incarnation,
            .resource_id = context.ordinary_resource_id};
}

auto secure_vfs_message_fallback(const WkiSecureRdmaResult& result) -> bool {
    return result.outcome == WkiSecureRdmaOutcome::NO_EFFECT && result.bytes == 0 &&
           (result.status == -ENOTSUP || result.status == -EOPNOTSUPP || result.status == -ENOMEM || result.status == -EAGAIN ||
            result.status == -EBUSY);
}

auto secure_vfs_client_write(RemoteFileContext& context, const WkiChannelIdentity& identity, int64_t offset, std::span<const uint8_t> input)
    -> WkiSecureRdmaResult {
    rdma_protocol::RegionDescriptor grant{};
    bool const COMPATIBLE = wki_secure_rdma_available(identity, rdma_protocol::Service::VFS);
    if (COMPATIBLE) context.secure_io_negotiated = true;
    if (!COMPATIBLE || !wki_secure_rdma_remote_grant(identity, rdma_protocol::Service::VFS, 0, grant, rdma_protocol::PERMIT_WRITE))
        return {-ENOTSUP};
    if (grant.binding_generation != context.ordinary_open_binding_id || input.size() > grant.size) return {-ENOTCONN};
    SharedIoSlotGuard const GUARD(context.proxy, wki_now_us());
    if (!GUARD.acquired()) return {-EAGAIN};
    auto const INTENT = make_secure_vfs_intent(context, rdma_protocol::Opcode::WRITE, offset, static_cast<uint32_t>(input.size()));
    return wki_secure_rdma_write(identity, grant, 0, input, 0, {reinterpret_cast<const uint8_t*>(&INTENT), sizeof(INTENT)});
}

auto secure_vfs_client_read(RemoteFileContext& context, const WkiChannelIdentity& identity, int64_t offset, uint32_t length)
    -> WkiSecureRdmaResult {
    rdma_protocol::RegionDescriptor grant{};
    bool const COMPATIBLE = wki_secure_rdma_available(identity, rdma_protocol::Service::VFS);
    if (COMPATIBLE) context.secure_io_negotiated = true;
    if (!COMPATIBLE || !wki_secure_rdma_remote_grant(identity, rdma_protocol::Service::VFS, 0, grant, rdma_protocol::PERMIT_READ))
        return {-ENOTSUP};
    if (grant.binding_generation != context.ordinary_open_binding_id || length > grant.size) return {-ENOTCONN};
    SharedIoSlotGuard const GUARD(context.proxy, wki_now_us());
    if (!GUARD.acquired()) return {-EAGAIN};
    if (context.secure_read_capacity < length) {
        std::unique_ptr<uint8_t[]> candidate{new (std::nothrow) uint8_t[length]};
        if (!candidate) return {-ENOMEM};
        context.secure_read_scratch = std::move(candidate);
        context.secure_read_capacity = length;
    }
    auto const INTENT = make_secure_vfs_intent(context, rdma_protocol::Opcode::READ, offset, length);
    return wki_secure_rdma_read(identity, grant, 0, {context.secure_read_scratch.get(), length},
                                {reinterpret_cast<const uint8_t*>(&INTENT), sizeof(INTENT)});
}

constexpr uint32_t VFS_SECURE_MESSAGE_READ_MAX = VFS_CACHE_SIZE;
constexpr uint32_t VFS_SECURE_MESSAGE_WRITE_MAX = WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload) - sizeof(VfsSecureIoIntent);

// The native outcome distinguishes known denial from a WRITE accepted without
// a provable completion. Never manufacture NO_EFFECT from an RPC timeout.
auto secure_vfs_message_write(RemoteFileContext& context, const WkiChannelIdentity& identity, int64_t offset,
                              std::span<const uint8_t> input) -> WkiSecureRdmaResult {
    if (input.empty() || input.size() > VFS_SECURE_MESSAGE_WRITE_MAX) return {-EINVAL};
    auto const INTENT = make_secure_vfs_intent(context, rdma_protocol::Opcode::WRITE, offset, static_cast<uint32_t>(input.size()));
    VfsSecureIoResult reply{};
    uint16_t length = 0;
    int const STATUS = normalize_proxy_status_for_errno(
        vfs_proxy_send_and_wait(context.proxy, OP_VFS_SECURE_IO, reinterpret_cast<const uint8_t*>(&INTENT), sizeof(INTENT), &reply,
                                sizeof(reply), &length, VFS_PROXY_OP_TIMEOUT_US, nullptr, input.data(), input.size(), false, &identity));
    if (STATUS != 0 || !wki_vfs_secure_message_result_valid(reply, true, static_cast<uint32_t>(input.size()), length))
        return {STATUS < 0 ? STATUS : -EPROTO, 0, WkiSecureRdmaOutcome::UNCERTAIN};
    return {reply.status, reply.bytes, static_cast<WkiSecureRdmaOutcome>(reply.outcome)};
}

auto secure_vfs_message_read(RemoteFileContext& context, const WkiChannelIdentity& identity, int64_t offset, uint32_t requested)
    -> WkiSecureRdmaResult {
    if (requested == 0 || requested > VFS_SECURE_MESSAGE_READ_MAX) return {-EINVAL};
    uint32_t const CAPACITY = requested + sizeof(VfsSecureIoResult);
    if (context.secure_read_capacity < CAPACITY) {
        std::unique_ptr<uint8_t[]> candidate{new (std::nothrow) uint8_t[CAPACITY]};
        if (!candidate) return {-ENOMEM};
        context.secure_read_scratch = std::move(candidate);
        context.secure_read_capacity = CAPACITY;
    }
    auto const INTENT = make_secure_vfs_intent(context, rdma_protocol::Opcode::READ, offset, requested);
    uint16_t length = 0;
    int const STATUS = normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(
        context.proxy, OP_VFS_SECURE_IO, reinterpret_cast<const uint8_t*>(&INTENT), sizeof(INTENT), context.secure_read_scratch.get(),
        static_cast<uint16_t>(CAPACITY), &length, VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &identity));
    if (STATUS != 0) return {STATUS};
    VfsSecureIoResult reply{};
    if (length < sizeof(reply)) return {-EPROTO};
    std::memcpy(&reply, context.secure_read_scratch.get(), sizeof(reply));
    if (!wki_vfs_secure_message_result_valid(reply, false, requested, length)) return {-EPROTO};
    if (reply.bytes != 0) std::memmove(context.secure_read_scratch.get(), context.secure_read_scratch.get() + sizeof(reply), reply.bytes);
    return {reply.status, reply.bytes, static_cast<WkiSecureRdmaOutcome>(reply.outcome)};
}

auto next_owner_mutation_id() -> uint64_t {
    static ker::vfs::coherency::Counter operations;
    return operations.take();
}

auto make_owner_io_request(const RemoteFileContext& context, uint64_t query, int64_t offset, uint32_t length) -> VfsOwnerIoRequest {
    return {.owner = {.fd = context.remote_fd,
                      .open_token = context.ordinary_open_token,
                      .binding_id = context.ordinary_open_binding_id,
                      .owner_boot_epoch = context.ordinary_owner_boot_epoch,
                      .query_id = query},
            .offset = offset,
            .data_len = length};
}

auto owner_message_read(RemoteFileContext& context, const WkiChannelIdentity& identity, int64_t offset, uint32_t requested,
                        uint8_t* destination, uint16_t& bytes) -> int {
    static ker::vfs::coherency::Counter queries;
    uint64_t const QUERY = queries.take();
    if (QUERY == 0) return -EOVERFLOW;
    auto const REQUEST = make_owner_io_request(context, QUERY, offset, requested);
    if (!wki_vfs_owner_io_request_valid(REQUEST, sizeof(REQUEST), false)) return -EINVAL;
    std::array<uint8_t, sizeof(VfsOwnerReadResponse) + WKI_VFS_OWNER_READ_MAX> candidate{};
    uint16_t length = 0;
    int const STATUS = normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(
        context.proxy, OP_VFS_READ_OWNER, reinterpret_cast<const uint8_t*>(&REQUEST), sizeof(REQUEST), candidate.data(), candidate.size(),
        &length, VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &identity));
    if (STATUS != 0) return STATUS;
    VfsOwnerReadResponse response{};
    if (length < sizeof(response)) return -EPROTO;
    std::memcpy(&response, candidate.data(), sizeof(response));
    if (!wki_vfs_owner_read_response_valid(response, QUERY, requested, length)) return -EPROTO;
    std::memcpy(destination, candidate.data() + sizeof(response), response.bytes);
    bytes = static_cast<uint16_t>(response.bytes);
    return 0;
}

auto owner_message_write(RemoteFileContext& context, const WkiChannelIdentity& identity, int64_t offset, std::span<const uint8_t> input)
    -> WkiSecureRdmaResult {
    uint64_t const OPERATION = next_owner_mutation_id();
    if (OPERATION == 0) return {-EOVERFLOW};
    auto const REQUEST = make_owner_io_request(context, OPERATION, offset, static_cast<uint32_t>(input.size()));
    if (!wki_vfs_owner_io_request_valid(REQUEST, sizeof(REQUEST) + input.size(), true)) return {-EINVAL};
    VfsAppendResponse response{};
    uint16_t length = 0;
    int const STATUS = normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(
        context.proxy, OP_VFS_WRITE_COMMITTED, reinterpret_cast<const uint8_t*>(&REQUEST), sizeof(REQUEST), &response, sizeof(response),
        &length, VFS_PROXY_OP_TIMEOUT_US, nullptr, input.data(), input.size(), false, &identity));
    if (STATUS != 0 || !wki_vfs_owner_write_response_valid(response, OPERATION, offset, input.size(), length))
        return {STATUS < 0 ? STATUS : -EPROTO, 0, WkiSecureRdmaOutcome::UNCERTAIN};
    return {response.status, response.bytes, WkiSecureRdmaOutcome::EXACT};
}

auto legacy_message_write(RemoteFileContext& context, const WkiChannelIdentity* identity, int64_t offset, std::span<const uint8_t> input)
    -> WkiSecureRdmaResult {
    std::array<uint8_t, 12> prefix{};
    std::memcpy(prefix.data(), &context.remote_fd, sizeof(context.remote_fd));
    std::memcpy(prefix.data() + 4, &offset, sizeof(offset));
    uint32_t written = 0;
    uint16_t length = 0;
    int const STATUS = normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(context.proxy, OP_VFS_WRITE, prefix.data(), prefix.size(),
                                                                                &written, sizeof(written), &length, VFS_PROXY_OP_TIMEOUT_US,
                                                                                nullptr, input.data(), input.size(), false, identity));
    if (STATUS != 0 || length != sizeof(written) || written > input.size())
        return {STATUS < 0 ? STATUS : -EPROTO, 0, WkiSecureRdmaOutcome::UNCERTAIN};
    return {0, written, WkiSecureRdmaOutcome::EXACT};
}

// The caller holds ctx->io_lock so storage promotion cannot invalidate src
// while an RDMA or message flush is sleeping on the remote operation.
auto flush_write_behind(RemoteFileContext* ctx) -> int {
    if (ctx->secure_write_error != 0) return ctx->secure_write_error;
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

    WkiChannelIdentity identity{};
    int const AUTHORITY = capture_secure_vfs_client(*ctx, identity);
    if (AUTHORITY != 0 && AUTHORITY != -ENOTSUP) return AUTHORITY;
    auto const* required_identity = AUTHORITY == 0 ? &identity : nullptr;
    constexpr uint32_t WRITE_HDR_OVERHEAD = sizeof(DevOpReqPayload) + 12;
    constexpr uint32_t MESSAGE_MAX = WKI_ETH_MAX_PAYLOAD - WRITE_HDR_OVERHEAD;
    while (remaining > 0) {
        uint32_t chunk = std::min(remaining, VFS_RDMA_WRITE_SIZE);
        uint32_t written = 0;
        bool message = required_identity == nullptr;
        if (!message) {
            auto const RESULT = secure_vfs_client_write(*ctx, identity, cur_offset, {src, chunk});
            if (RESULT.outcome == WkiSecureRdmaOutcome::EXACT && RESULT.status == 0 && RESULT.bytes > 0 && RESULT.bytes <= chunk) {
                written = RESULT.bytes;
            } else if (secure_vfs_message_fallback(RESULT)) {
                message = true;
            } else {
                keep_pending_tail(src, remaining, cur_offset);
                int const ERROR = RESULT.status < 0 ? RESULT.status : -EIO;
                if (RESULT.outcome != WkiSecureRdmaOutcome::NO_EFFECT) ctx->secure_write_error = ERROR;
                return ERROR;
            }
        }
        if (message && ctx->secure_io_negotiated) {
            chunk = std::min(remaining, VFS_SECURE_MESSAGE_WRITE_MAX);
            auto const RESULT = secure_vfs_message_write(*ctx, identity, cur_offset, {src, chunk});
            if (RESULT.outcome != WkiSecureRdmaOutcome::EXACT || RESULT.status != 0 || RESULT.bytes == 0 || RESULT.bytes > chunk) {
                keep_pending_tail(src, remaining, cur_offset);
                int const ERROR = RESULT.status < 0 ? RESULT.status : -EPROTO;
                if (RESULT.outcome != WkiSecureRdmaOutcome::NO_EFFECT) ctx->secure_write_error = ERROR;
                return ERROR;
            }
            written = RESULT.bytes;
        } else if (message) {
            chunk = std::min(remaining, MESSAGE_MAX);
            std::array<uint8_t, 12> prefix{};
            std::memcpy(prefix.data(), &ctx->remote_fd, sizeof(ctx->remote_fd));
            std::memcpy(prefix.data() + 4, &cur_offset, sizeof(cur_offset));
            int const STATUS = normalize_proxy_status_for_errno(
                vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_WRITE, prefix.data(), prefix.size(), &written, sizeof(written), nullptr,
                                        VFS_PROXY_OP_TIMEOUT_US, nullptr, src, chunk, false, required_identity));
            if (STATUS != 0) {
                keep_pending_tail(src, remaining, cur_offset);
                ctx->secure_write_error = STATUS;
                return STATUS;
            }
        }
        if (written == 0 || written > chunk) {
            keep_pending_tail(src, remaining, cur_offset);
            return -EIO;
        }
        src += written;
        cur_offset += static_cast<int64_t>(written);
        remaining -= written;
    }

    wb->pending_offset = -1;
    wb->pending_len = 0;
    return 0;
}

// -----------------------------------------------------------------------------
// Consumer-side FileOperations
// -----------------------------------------------------------------------------

auto remote_vfs_close_needs_completion(int open_flags) -> bool {
    int const ACCESS_MODE = open_flags & 3;
    return ACCESS_MODE != 0 || (open_flags & (ker::vfs::O_CREAT | ker::vfs::O_TRUNC)) != 0;
}

// Only immutable private retained pins use this path. Writable descriptions and
// Files owning native directory views retain the general close contract below.
auto immutable_pin_close(const ker::vfs::File& file, const RemoteFileContext& context) -> bool {
    unsigned const ACCESS = static_cast<unsigned>(file.open_flags) & 3U;
    return !file.directory_view && (file.open_flags & (ker::vfs::O_CREAT | ker::vfs::O_TRUNC)) == 0 &&
           (context.write_buf == nullptr || context.write_buf->pending_len == 0) &&
           ((context.directory_pin && ACCESS == ker::vfs::FILE_ACCESS_SEARCH_ONLY) || (file.exec_read_view != nullptr && ACCESS == 0));
}

// VFS already detached shared stream/cache/advisory state before backend close.
// The final File owner excludes new per-file I/O; no io_lock may span the RPC.
void finish_pin_close_backend(ker::vfs::File* file) {
    if (file == nullptr || file->private_data == nullptr) return;
    auto* context = static_cast<RemoteFileContext*>(file->private_data);
    auto* proxy = context->proxy;
    clear_remote_file_open_prefetch(context);
    delete context->read_cache;
    delete context->write_buf;
    delete context;
    file->private_data = nullptr;
    release_vfs_proxy_open_ref(proxy);
}

class OrdinaryCloseOwner;
OrdinaryCloseOwner* g_ordinary_closes = nullptr;
#ifdef WOS_SELFTEST
std::atomic<size_t> g_abandoned_ordinary_closes_finished{0};
#endif
// Final File ownership excludes per-file I/O. Track both flush and cancellation
// waits; fatal exit cannot rely on the backend or outer VFS stack unwinding.
class OrdinaryCloseOwner {
   public:
    explicit OrdinaryCloseOwner(ker::vfs::File* file, uint64_t pid = perf_current_pid())
        : file_(file), pid_(pid), metadata_(file->close_may_change_metadata) {
        auto* context = static_cast<RemoteFileContext*>(file->private_data);
        proxy_ = context->proxy;
        slot_ = context->ordinary_cancel_slot;
        s_vfs_lock.lock();
        if (slot_ < g_open_cancellations.size() && g_open_cancellations.at(slot_).proxy == proxy_)
            request_ = g_open_cancellations.at(slot_).request;
        next_ = g_ordinary_closes;
        if (next_ != nullptr) next_->previous_ = this;
        g_ordinary_closes = this;
        s_vfs_lock.unlock();
    }
    ~OrdinaryCloseOwner() {
        s_vfs_lock.lock();
        auto retired = take_locked();
        s_vfs_lock.unlock();
        finish_pin_close_backend(retired.file);
        if (retired.queued) wki_deferred_work_notify();
    }
    OrdinaryCloseOwner(const OrdinaryCloseOwner&) = delete;
    auto operator=(const OrdinaryCloseOwner&) -> OrdinaryCloseOwner& = delete;
    auto file() const -> ker::vfs::File* { return file_; }
    static void cleanup_for_task(uint64_t pid) {
        if (pid == 0) return;
        for (;;) {
            s_vfs_lock.lock();
            auto* candidate = g_ordinary_closes;
            while (candidate != nullptr && candidate->pid_ != pid) candidate = candidate->next_;
            Retired retired{};
            if (candidate != nullptr) retired = candidate->take_locked();
            s_vfs_lock.unlock();
            if (candidate == nullptr) return;
            // Never replay an interrupted flush: its last write may have reached
            // the owner. Retire the description and finish the abandoned File.
            finish_pin_close_backend(retired.file);
            static_cast<void>(ker::vfs::vfs_finish_file_close(retired.file, retired.metadata, -ECANCELED));
#ifdef WOS_SELFTEST
            g_abandoned_ordinary_closes_finished.fetch_add(1, std::memory_order_relaxed);
#endif
            if (retired.queued) wki_deferred_work_notify();
        }
    }

   private:
    struct Retired {
        ker::vfs::File* file = nullptr;
        bool metadata = false;
        bool queued = false;
    };
    auto take_locked() -> Retired {
        if (file_ == nullptr) return {};
        if (previous_ != nullptr)
            previous_->next_ = next_;
        else
            g_ordinary_closes = next_;
        if (next_ != nullptr) next_->previous_ = previous_;
        previous_ = nullptr;
        next_ = nullptr;
        Retired retired{std::exchange(file_, nullptr), metadata_, queue_ordinary_cancellation_locked(slot_, proxy_, &request_)};
        slot_ = VFS_EXEC_CANCEL_SLOT_NONE;
        proxy_ = nullptr;
        return retired;
    }
    ker::vfs::File* file_ = nullptr;
    ProxyVfsState* proxy_ = nullptr;
    uint64_t pid_ = 0;
    VfsOpenLifetimeRequest request_{};
    uint16_t slot_ = VFS_EXEC_CANCEL_SLOT_NONE;
    bool metadata_ = false;
    OrdinaryCloseOwner* previous_ = nullptr;
    OrdinaryCloseOwner* next_ = nullptr;
};

auto close_ordinary_file(ker::vfs::File* file) -> int {
    auto* context = static_cast<RemoteFileContext*>(file->private_data);
    OrdinaryCloseOwner owner(file);
    if (context->proxy == nullptr || !context->proxy->active) return -EIO;
    int const FLUSH_STATUS = flush_write_behind(context);
    s_vfs_lock.lock();

    s_vfs_lock.unlock();
    int close_status = 0;
    if (remote_vfs_close_needs_completion(file->open_flags)) close_status = cancel_remote_ordinary_open(context->ordinary_cancel_slot);
    // Readonly close hands off to the durable queue in the owner's destructor.
    // A successful writable close still completes before task waitability.
    return FLUSH_STATUS != 0 ? FLUSH_STATUS : close_status;
}

class PinCloseOwner;
PinCloseOwner* g_pin_closes = nullptr;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
#ifdef WOS_SELFTEST
std::atomic<size_t> g_abandoned_pin_closes_finished{0};
#endif
class PinCloseOwner {
   public:
    explicit PinCloseOwner(ker::vfs::File* file, uint64_t pid = perf_current_pid())
        : file_(file), pid_(pid), metadata_(file->close_may_change_metadata) {
        auto* context = static_cast<RemoteFileContext*>(file->private_data);
        proxy_ = context->proxy;
        slot_ = context->exec_cancel_slot;
        directory_ = context->directory_pin;
        s_vfs_lock.lock();
        if (slot_ < g_exec_cancellations.size()) {
            auto const& record = g_exec_cancellations.at(slot_);
            if (record.proxy == proxy_ && record.directory == directory_) token_ = record.token;
        }
        next_ = g_pin_closes;
        if (next_ != nullptr) next_->previous_ = this;
        g_pin_closes = this;
        s_vfs_lock.unlock();
    }
    ~PinCloseOwner() {
        s_vfs_lock.lock();
        auto retired = take_locked(false);
        s_vfs_lock.unlock();
        // Normal return leaves outer File finalization to vfs_destroy_file.
        finish_pin_close_backend(retired.file);
    }
    PinCloseOwner(const PinCloseOwner&) = delete;
    auto operator=(const PinCloseOwner&) -> PinCloseOwner& = delete;
    auto file() const -> ker::vfs::File* { return file_; }
    static void cleanup_for_task(uint64_t pid) {
        if (pid == 0) return;
        for (;;) {
            s_vfs_lock.lock();
            auto* candidate = g_pin_closes;
            while (candidate != nullptr && candidate->pid_ != pid) candidate = candidate->next_;
            Retired retired{};
            if (candidate != nullptr) retired = candidate->take_locked(true);
            s_vfs_lock.unlock();
            if (candidate == nullptr) return;
            // The interrupted backend and outer VFS frames will never resume.
            // Quiesced waiters cannot touch their File/context or stack again.
            finish_pin_close_backend(retired.file);
            static_cast<void>(ker::vfs::vfs_finish_file_close(retired.file, retired.metadata, -ECANCELED));
#ifdef WOS_SELFTEST
            g_abandoned_pin_closes_finished.fetch_add(1, std::memory_order_relaxed);
#endif
            if (retired.queued) wki_deferred_work_notify();
        }
    }

   private:
    struct Retired {
        ker::vfs::File* file = nullptr;
        bool metadata = false;
        bool queued = false;
    };
    auto take_locked(bool abandoned) -> Retired {
        if (file_ == nullptr) return {};
        if (previous_ != nullptr)
            previous_->next_ = next_;
        else
            g_pin_closes = next_;
        if (next_ != nullptr) next_->previous_ = previous_;
        previous_ = nullptr;
        next_ = nullptr;
        Retired retired{std::exchange(file_, nullptr), metadata_};
        if (abandoned && slot_ < g_exec_cancellations.size()) {
            auto& record = g_exec_cancellations.at(slot_);
            if (record.phase == ExecCancellationPhase::OWNED && record.proxy == proxy_ && record.token == token_ &&
                record.directory == directory_) {
                record.phase = ExecCancellationPhase::PENDING;
                record.retry_after_us = 0;
                retired.queued = true;
            }
        }
        proxy_ = nullptr;
        slot_ = VFS_EXEC_CANCEL_SLOT_NONE;
        token_ = 0;
        return retired;
    }
    ker::vfs::File* file_ = nullptr;
    ProxyVfsState* proxy_ = nullptr;
    uint64_t pid_ = 0;
    uint64_t token_ = 0;
    uint16_t slot_ = VFS_EXEC_CANCEL_SLOT_NONE;
    bool metadata_ = false;
    bool directory_ = false;
    PinCloseOwner* previous_ = nullptr;
    PinCloseOwner* next_ = nullptr;
};

auto close_immutable_pin(ker::vfs::File* file) -> int {
    auto* context = static_cast<RemoteFileContext*>(file->private_data);
    auto* proxy = context->proxy;
    PinCloseOwner owner(file);
    if (proxy == nullptr || !proxy->active) {
        if (context->exec_cancel_slot != VFS_EXEC_CANCEL_SLOT_NONE)
            static_cast<void>(cancel_remote_exec_view(context->exec_cancel_slot, true));
        return -EIO;
    }
    s_vfs_lock.lock();

    s_vfs_lock.unlock();
    // Immutable pins have no pending writes. Keep the acknowledged cancellation
    // and its errno, with neither a registry nor per-file I/O mutex held.
    return cancel_remote_exec_view(context->exec_cancel_slot);
}

auto remote_vfs_close(ker::vfs::File* f) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return -EBADF;
    }
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    if (ctx->ordinary_cancel_slot != VFS_EXEC_CANCEL_SLOT_NONE) return close_ordinary_file(f);
    if (immutable_pin_close(*f, *ctx)) return close_immutable_pin(f);
    ProxyVfsState* const PROXY = ctx->proxy;
    if (PROXY == nullptr || !PROXY->active) {
        if (ctx->exec_cancel_slot != VFS_EXEC_CANCEL_SLOT_NONE) {
            static_cast<void>(cancel_remote_exec_view(ctx->exec_cancel_slot, true));
        }
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
    int close_status = 0;
    {
        ker::mod::sys::MutexGuard io_guard(ctx->io_lock);

        // D6: Flush pending writes before closing
        flush_status = flush_write_behind(ctx);

        // D7: Invalidate directory cache for this file
        s_vfs_lock.lock();

        s_vfs_lock.unlock();

        int32_t const REMOTE_FD = ctx->remote_fd;
        bool const NEEDS_CLOSE_STATUS = remote_vfs_close_needs_completion(f->open_flags);
        if (f->exec_read_view != nullptr || ctx->directory_pin) {
            // A retained directory or read-only execution view owns an owner-side content pin.
            // Complete its token-scoped retirement before local waitability.
            close_status = cancel_remote_exec_view(ctx->exec_cancel_slot);
        } else if (NEEDS_CLOSE_STATUS) {
            // A successful remote task must not become visible before its routed
            // writable outputs have completed owner-side close and publication.
            close_status = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_CLOSE, reinterpret_cast<const uint8_t*>(&REMOTE_FD),
                                                   sizeof(REMOTE_FD), nullptr, 0);
        } else {
            // Read-only closes carry no output-publication dependency. Keep them
            // on the one-way path to avoid adding a round trip to source/header I/O.
            std::array<uint8_t, WKI_VFS_CLOSE_EXTENDED_DATA_LEN> close_req{};
            memcpy(close_req.data(), &REMOTE_FD, sizeof(REMOTE_FD));
            close_req.at(WKI_VFS_CLOSE_FLAGS_OFFSET) = WKI_VFS_CLOSE_FLAG_NO_SUCCESS_RESPONSE;
            int const SEND_STATUS = vfs_proxy_send_untracked(ctx->proxy, OP_VFS_CLOSE, close_req.data(), close_req.size());
            if (SEND_STATUS != 0) {
                ker::mod::dbg::log("[WKI] async remote close send failed: node=0x%04x ch=%u fd=%d rc=%d", ctx->proxy->owner_node,
                                   ctx->proxy->assigned_channel, REMOTE_FD, SEND_STATUS);
            }
        }

        // D6: Free caches
        clear_remote_file_open_prefetch(ctx);
        delete ctx->read_cache;
        delete ctx->write_buf;
    }

    delete ctx;
    f->private_data = nullptr;
    release_vfs_proxy_open_ref(PROXY);
    return flush_status != 0 ? flush_status : close_status;
}

auto remote_vfs_read(ker::vfs::File* f, void* buf, size_t count, size_t offset) -> ssize_t {
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == ker::vfs::FILE_ACCESS_SEARCH_ONLY) return -EBADF;
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
    // Ordinary OPEN prefetch was not bracketed by owner snapshots. It cannot
    // gain authority from a later query, even if its size/stat still matches.
    clear_remote_file_open_prefetch(ctx);
    ctx->bulk_cached_offset = -1;
    ctx->bulk_cached_len = 0;
    ker::vfs::coherency::Stamp owner_before{};
    WkiChannelIdentity owner_identity{};
    int const FEATURES = query_remote_owner_features(*ctx, owner_identity);
    if (FEATURES != 0 && FEATURES != -ENOTSUP && FEATURES != -EOPNOTSUPP && FEATURES != -ENOSYS) return FEATURES;
    bool const OWNER_IO = FEATURES == 0 && (ctx->owner_features & WKI_VFS_FEATURE_COMMITTED_IO) != 0;
    bool const ALLOW_READ_CACHES =
        OWNER_IO && (f->open_flags & ker::vfs::O_NO_CACHE) == 0 && query_remote_owner_version(*ctx, owner_before) == 0;
    if (ctx->read_cache != nullptr &&
        (!ALLOW_READ_CACHES || !ctx->read_cache->validation.matches(owner_before, ker::vfs::coherency::Domain::CONTENT))) {
        ctx->read_cache->cached_len = 0;
        ctx->read_cache->cached_offset = -1;
        ctx->read_cache->validation.invalidate();
    }

    WkiChannelIdentity secure_identity{};
    int const SECURE_AUTHORITY = capture_secure_vfs_client(*ctx, secure_identity);
    if (SECURE_AUTHORITY != 0 && SECURE_AUTHORITY != -ENOTSUP) return total_read > 0 ? total_read : SECURE_AUTHORITY;
    auto const* required_identity = SECURE_AUTHORITY == 0 ? &secure_identity : nullptr;
    while (required_identity != nullptr && remaining != 0) {
        uint32_t chunk = std::min(remaining, VFS_RDMA_BULK_SIZE);
        auto result = secure_vfs_client_read(*ctx, secure_identity, cur_offset, chunk);
        if (secure_vfs_message_fallback(result)) {
            if (!ctx->secure_io_negotiated) break;
            chunk = std::min(remaining, VFS_SECURE_MESSAGE_READ_MAX);
            result = secure_vfs_message_read(*ctx, secure_identity, cur_offset, chunk);
        }
        auto const RESULT = result;
        if (RESULT.outcome != WkiSecureRdmaOutcome::EXACT || RESULT.status != 0 || RESULT.bytes > chunk)
            return total_read > 0 ? total_read : (RESULT.status < 0 ? RESULT.status : -EIO);
        if (RESULT.bytes == 0) return total_read;
        std::memcpy(dest, ctx->secure_read_scratch.get(), RESULT.bytes);
        dest += RESULT.bytes;
        cur_offset += RESULT.bytes;
        remaining -= RESULT.bytes;
        total_read += RESULT.bytes;
        if (RESULT.bytes < chunk || remaining == 0) return total_read;
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
    if (!OWNER_IO && (BULK_PULL_CAPABLE || BULK_ROCE_PUSH_CAPABLE || BULK_PUSH_CAPABLE)) {
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
            // The shared bulk window has no pre/post version authority. Keep
            // bulk transfers as fresh exact-offset reads, without retaining it.
            bool const USE_BULK_PREFETCH_CACHE = false;

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
    const bool RDMA_READ_ENABLED = !OWNER_IO && !skip_read_rdma_this_call &&
                                   !ctx->proxy->rdma_read_disabled.load(std::memory_order_acquire) &&
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
        fetch_size = std::min(fetch_size, OWNER_IO ? WKI_VFS_OWNER_READ_MAX : max_resp_data);

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
        int const STATUS = OWNER_IO
                               ? owner_message_read(*ctx, owner_identity, cur_offset, chunk, fetch_dest, resp_len)
                               : vfs_proxy_read_with_retry(ctx->proxy, OP_VFS_READ, req_data.data(), 16, fetch_dest,
                                                           static_cast<uint16_t>(chunk), &resp_len, nullptr, nullptr, required_identity);
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
            ker::vfs::coherency::Stamp owner_after{};
            bool const VALIDATED = query_remote_owner_version(*ctx, owner_after) == 0 &&
                                   ctx->read_cache->validation.publish(owner_before, owner_after, ker::vfs::coherency::Domain::CONTENT);
            ctx->read_cache->cached_len = VALIDATED ? BYTES_READ : 0;
            if (!VALIDATED) ctx->read_cache->validation.invalidate();

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

auto remote_vfs_write_impl(ker::vfs::File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == ker::vfs::FILE_ACCESS_SEARCH_ONLY) return -EBADF;
    if (f == nullptr || f->private_data == nullptr || buf == nullptr) return -EINVAL;
    auto& context = *static_cast<RemoteFileContext*>(f->private_data);
    ker::mod::sys::MutexGuard io_guard(context.io_lock);
    consume_remote_file_cache_invalidation(&context);
    if (context.secure_write_error != 0) return context.secure_write_error;
    WkiChannelIdentity identity{};
    int const AUTHORITY = capture_secure_vfs_client(context, identity);
    if (AUTHORITY != 0 && AUTHORITY != -ENOTSUP) return AUTHORITY;
    auto const* required_identity = AUTHORITY == 0 ? &identity : nullptr;
    int const FEATURES = required_identity == nullptr ? -ENOTSUP : query_remote_owner_features(context, identity);
    if (FEATURES != 0 && FEATURES != -ENOTSUP && FEATURES != -EOPNOTSUPP && FEATURES != -ENOSYS) return FEATURES;
    bool const OWNER_IO = FEATURES == 0 && (context.owner_features & WKI_VFS_FEATURE_COMMITTED_IO) != 0;
    int const FLUSH = flush_write_behind(&context);
    if (FLUSH != 0) return FLUSH;
    invalidate_remote_file_open_caches(&context);

    auto const* source = static_cast<const uint8_t*>(buf);
    size_t remaining = count;
    int64_t current_offset = static_cast<int64_t>(offset);
    ssize_t total = 0;
    constexpr uint32_t LEGACY_MESSAGE_MAX = WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload) - 12;
    while (remaining != 0) {
        uint32_t chunk = static_cast<uint32_t>(std::min(remaining, static_cast<size_t>(VFS_RDMA_WRITE_SIZE)));
        auto result = required_identity == nullptr ? WkiSecureRdmaResult{-ENOTSUP}
                                                   : secure_vfs_client_write(context, identity, current_offset, {source, chunk});
        if (secure_vfs_message_fallback(result)) {
            if (context.secure_io_negotiated) {
                chunk = static_cast<uint32_t>(std::min(remaining, static_cast<size_t>(VFS_SECURE_MESSAGE_WRITE_MAX)));
                result = secure_vfs_message_write(context, identity, current_offset, {source, chunk});
            } else if (OWNER_IO) {
                chunk = static_cast<uint32_t>(std::min(remaining, static_cast<size_t>(WKI_VFS_OWNER_WRITE_MAX)));
                result = owner_message_write(context, identity, current_offset, {source, chunk});
            } else {
                chunk = static_cast<uint32_t>(std::min(remaining, static_cast<size_t>(LEGACY_MESSAGE_MAX)));
                result = legacy_message_write(context, required_identity, current_offset, {source, chunk});
            }
        }
        if (result.outcome != WkiSecureRdmaOutcome::EXACT || result.status != 0 || result.bytes > chunk) {
            int const ERROR = result.status < 0 ? result.status : -EPROTO;
            if (result.outcome != WkiSecureRdmaOutcome::EXACT && result.outcome != WkiSecureRdmaOutcome::NO_EFFECT)
                context.secure_write_error = ERROR;
            if (result.bytes > chunk || result.status > 0) context.secure_write_error = -EPROTO;
            return total > 0 ? total : ERROR;
        }
        if (result.bytes == 0) break;
        source += result.bytes;
        current_offset += result.bytes;
        remaining -= result.bytes;
        total += result.bytes;
        if (result.bytes < chunk) break;
    }
    return total;
}

auto remote_vfs_write(ker::vfs::File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == ker::vfs::FILE_ACCESS_SEARCH_ONLY) return -EBADF;
    if (f == nullptr || f->private_data == nullptr || (buf == nullptr && count != 0)) {
        return -EINVAL;
    }
    if (f->is_directory) {
        return -EISDIR;
    }
    ker::mod::sched::resource::FileGrowth growth;
    if (!growth.bound_write(offset, count)) {
        return -EFBIG;
    }
    if (count == 0) {
        return 0;
    }
    ssize_t const RET = remote_vfs_write_impl(f, buf, count, offset);
    if (RET > 0) {
        // Only a known native committed prefix is counted as written.
        growth.committed(offset + static_cast<uint64_t>(RET));
    }
    return RET;
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
            WkiChannelIdentity identity{};
            int const AUTHORITY = capture_secure_vfs_client(*ctx, identity);
            if (AUTHORITY != 0 && AUTHORITY != -ENOTSUP) return AUTHORITY;
            uint16_t reply_length = 0;
            int const RET =
                vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_SEEK_END, req.data(), 12, &new_pos, sizeof(new_pos), &reply_length,
                                        VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, AUTHORITY == 0 ? &identity : nullptr);
#ifdef WKI_DEBUG
            ker::mod::dbg::log("remote_vfs_lseek: SEEK_END ret=%d new_pos=%ld", ret, (long)new_pos);
#endif
            if (RET < 0) {
                ker::mod::dbg::log("remote_vfs_lseek: SEEK_END request failed fd=%d node=0x%04x ch=%u ret=%d", ctx->remote_fd,
                                   ctx->proxy->owner_node, ctx->proxy->assigned_channel, RET);
                return RET;
            }

            if (RET != 0 || reply_length != sizeof(new_pos) || new_pos < 0) return -EPROTO;
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
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == ker::vfs::FILE_ACCESS_SEARCH_ONLY) return -EBADF;
    if (f == nullptr || f->private_data == nullptr || entry == nullptr) return -EINVAL;
    if (index > UINT32_MAX) return -EOVERFLOW;
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    ker::mod::sys::MutexGuard io_guard(ctx->io_lock);
    consume_remote_file_cache_invalidation(ctx);
    if (ctx->proxy == nullptr || !ctx->proxy->active) return -EIO;
    WkiChannelIdentity identity{};
    int const AUTHORITY = capture_secure_vfs_client(*ctx, identity);
    if (AUTHORITY != 0 && AUTHORITY != -ENOTSUP) return AUTHORITY;
    auto const* required_identity = AUTHORITY == 0 ? &identity : nullptr;
    ker::vfs::coherency::Stamp before{};
    bool const VERSIONED = query_remote_owner_version(*ctx, before) == 0;

    // One fixed fresh batch: no unbounded global deque, timestamp or cached EOF.
    // Filtering changes visible indices, so filtered directories scan from zero.
    std::array<uint8_t, VFS_READDIR_BATCH_DATA_CAPACITY> batch_buf{};
    uint32_t fetch_index = ctx->hide_recursive_wki_entries ? 0 : static_cast<uint32_t>(index);
    size_t visible_index = ctx->hide_recursive_wki_entries ? 0 : index;
    while (true) {
        std::array<uint8_t, 12> request{};
        std::memcpy(request.data(), &ctx->remote_fd, sizeof(ctx->remote_fd));
        std::memcpy(request.data() + 4, &fetch_index, sizeof(fetch_index));
        std::memcpy(request.data() + 8, &VFS_READDIR_BATCH_MAX_ENTRIES, sizeof(uint32_t));
        uint16_t length = 0;
        int const STATUS =
            vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_READDIR_BATCH, request.data(), request.size(), batch_buf.data(), batch_buf.size(),
                                    &length, VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, required_identity);
        if (STATUS != 0) return STATUS;
        uint32_t count = 0;
        if (length < sizeof(count)) return -EPROTO;
        std::memcpy(&count, batch_buf.data(), sizeof(count));
        if (!vfs_readdir_batch_payload_is_valid(length, count) || length != sizeof(count) + count * sizeof(ker::vfs::DirEntry))
            return -EPROTO;
        if (VERSIONED) {
            ker::vfs::coherency::Stamp after{};
            int const VALIDATION = query_remote_owner_version(*ctx, after);
            if (VALIDATION != 0) return VALIDATION;
            if (!ker::vfs::coherency::cache_matches(before, after, ker::vfs::coherency::Domain::DIRECTORY)) return -EAGAIN;
        }
        for (uint32_t i = 0; i < count; ++i) {
            ker::vfs::DirEntry candidate{};
            std::memcpy(&candidate, batch_buf.data() + sizeof(count) + i * sizeof(candidate), sizeof(candidate));
            if (std::memchr(candidate.d_name.data(), '\0', candidate.d_name.size()) == nullptr) return -EPROTO;
            if (should_hide_recursive_wki_entry(ctx, candidate)) continue;
            if (visible_index == index) {
                *entry = candidate;
                return 0;
            }
            ++visible_index;
        }
        if (count < VFS_READDIR_BATCH_MAX_ENTRIES) return -1;
        if (fetch_index > UINT32_MAX - count) return -EOVERFLOW;
        fetch_index += count;
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
auto remote_vfs_truncate_impl(ker::vfs::File* f, off_t length) -> int {
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == ker::vfs::FILE_ACCESS_SEARCH_ONLY) return -EBADF;
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

auto remote_vfs_truncate(ker::vfs::File* f, off_t length) -> int {
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == ker::vfs::FILE_ACCESS_SEARCH_ONLY) return -EBADF;
    if (f == nullptr) return -EBADF;
    if (f->is_directory) return -EISDIR;
    if (length < 0) {
        return -EINVAL;
    }
    int const ACCESS = ker::mod::security::authorize_file_description_access(static_cast<unsigned>(f->open_flags) & 3U, true);
    if (ACCESS != 0) return ACCESS;
    ker::mod::sched::resource::FileGrowth growth;
    if (!growth.allow_resize(static_cast<uint64_t>(length))) {
        return -EFBIG;
    }
    int const RET = remote_vfs_truncate_impl(f, length);
    if (RET == 0) {
        growth.committed(static_cast<uint64_t>(length));
    }
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

void wki_remote_vfs_close_secure_context(VfsSecureContext* context) {
    if (context != nullptr) context->closed.store(true, std::memory_order_release);
}

void wki_remote_vfs_release_secure_context(VfsSecureContext* context) {
    if (context == nullptr) return;
    wki_remote_vfs_close_secure_context(context);
    // Nonblocking registry close first; failed reclaim never permits deletion.
    // Hook release takes no peer/export gate and can drain while cleanup yields.
    for (auto* grant : {&context->write_grant, &context->read_grant}) {
        if (grant->rkey == 0) continue;
        while (wki_secure_rdma_unregister_region(*grant) != 0) ker::mod::sched::kern_yield();
        *grant = {};
    }
    if (context->write_backing) auth_crypto::secure_erase(context->write_backing.get(), VFS_RDMA_WRITE_SIZE);
    if (context->read_backing) auth_crypto::secure_erase(context->read_backing.get(), VFS_RDMA_BULK_SIZE);
    delete context;
}

void wki_remote_vfs_prepare_secure_binding(DevServerBinding& binding) {
    if (binding.vfs_open_binding_id == 0 || binding.retiring.load(std::memory_order_acquire)) return;
    if (auto* prior = wki_dev_server_borrow_vfs_secure_context(&binding); prior != nullptr) {
        int const WRITE = wki_secure_rdma_local_region_current(binding.channel_identity, rdma_protocol::Service::VFS, prior->write_grant);
        int const READ = wki_secure_rdma_local_region_current(binding.channel_identity, rdma_protocol::Service::VFS, prior->read_grant);
        if (WRITE == -EAGAIN || READ == -EAGAIN || (WRITE != -ESTALE && READ != -ESTALE)) return;
        if ((WRITE != 0 && WRITE != -ESTALE) || (READ != 0 && READ != -ESTALE)) return;
        if (!wki_dev_server_take_vfs_secure_context(&binding, prior)) return;
        wki_remote_vfs_release_secure_context(prior);
    }
    // Deferred task setup owns a retained binding, not a peer RX writer gate.
    bool const COMPATIBLE = wki_secure_rdma_available(binding.channel_identity, rdma_protocol::Service::VFS);
    if (!COMPATIBLE || !wki_dev_server_vfs_secure_compatible(&binding, true)) return;
    auto* context = new (std::nothrow) VfsSecureContext;
    if (context == nullptr) return;
    context->identity = binding.channel_identity;
    context->binding_id = binding.vfs_open_binding_id;
    context->resource_id = binding.resource_id;
    context->incarnation = binding.resource_incarnation;
    context->publication_revision = binding.vfs_export_publication_revision;
    context->write_backing.reset(new (std::nothrow) uint8_t[VFS_RDMA_WRITE_SIZE]);
    context->read_backing.reset(new (std::nothrow) uint8_t[VFS_RDMA_BULK_SIZE]);
    if (!context->write_backing || !context->read_backing) {
        wki_remote_vfs_release_secure_context(context);
        return;
    }
    WkiSecureRdmaHooks const HOOKS{secure_vfs_arm, secure_vfs_effect, secure_vfs_release};
    int status =
        wki_secure_rdma_register_region(context->identity, rdma_protocol::Service::VFS, context->write_backing.get(), VFS_RDMA_WRITE_SIZE,
                                        context->binding_id, rdma_protocol::PERMIT_WRITE, HOOKS, context, context->write_grant);
    if (status == 0)
        status =
            wki_secure_rdma_register_region(context->identity, rdma_protocol::Service::VFS, context->read_backing.get(), VFS_RDMA_BULK_SIZE,
                                            context->binding_id, rdma_protocol::PERMIT_READ, HOOKS, context, context->read_grant);
    if (status != 0 || !wki_dev_server_install_vfs_secure_context(&binding, context)) wki_remote_vfs_release_secure_context(context);
}

void wki_remote_vfs_cleanup_for_task(uint64_t pid) {
    while (true) {
        ProxyTaskCleanup const CLEANUP = remove_one_proxy_task_reference(pid);
        if (!CLEANUP.removed) {
            break;
        }
        if (CLEANUP.op_wait_retiring) {
            wait_for_waiter_retirement(CLEANUP.op_wait_entry);
        } else {
            finish_or_quiesce_waiter(CLEANUP.op_wait_entry, CLEANUP.op_wait_claimed, WKI_ERR_PEER_FENCED);
        }
        wake_proxy_slot_waiter(CLEANUP.next_pid);
    }
    SharedIoTaskOwner::cleanup_for_task(pid);
    PinPreparation::cleanup_for_task(pid);
    PinCloseOwner::cleanup_for_task(pid);
    OrdinaryCloseOwner::cleanup_for_task(pid);
}

auto wki_remote_vfs_fsync(ker::vfs::File* file) -> int { return remote_vfs_fsync_file(file); }

namespace {
void pin_diag_snapshot(WkiRemoteVfsExecDiag* out, bool directory) {
    if (out == nullptr) return;
    s_vfs_lock.lock();
    auto snapshot = pin_cancellation_totals(directory);
    snapshot.capacity = g_exec_cancellations.size();
    for (const auto& record : g_exec_cancellations) {
        if (record.directory != directory) continue;
        snapshot.owned += record.phase == ExecCancellationPhase::OWNED ? 1U : 0U;
        snapshot.pending += record.phase == ExecCancellationPhase::PENDING ? 1U : 0U;
        snapshot.retrying += record.phase == ExecCancellationPhase::RETRYING ? 1U : 0U;
    }
    for (const auto& [fd, row] : g_remote_fds) {
        static_cast<void>(fd);
        snapshot.owner_views += (directory ? row.directory_pin_token : row.exec_view_token) != 0 && row.file != nullptr ? 1U : 0U;
    }
    s_vfs_lock.unlock();
    *out = snapshot;
}

}  // namespace
void wki_remote_vfs_exec_diag_snapshot(WkiRemoteVfsExecDiag* out) { pin_diag_snapshot(out, false); }
void wki_remote_vfs_directory_diag_snapshot(WkiRemoteVfsExecDiag* out) { pin_diag_snapshot(out, true); }

void wki_remote_vfs_server_diag_snapshot(WkiRemoteVfsServerDiag* out) {
    if (out == nullptr) {
        return;
    }

    WkiRemoteVfsServerDiag snapshot{};
    s_vfs_lock.lock();
    snapshot.fd_entries = g_remote_fds.size();
    for (const auto& [fd_id, rfd] : g_remote_fds) {
        static_cast<void>(fd_id);
        snapshot.active_fds += rfd.active && !rfd.retiring ? 1U : 0U;
        snapshot.retiring_fds += rfd.retiring ? 1U : 0U;
    }
    snapshot.opens_total = g_remote_fd_opens_total;
    snapshot.close_retirements_total = g_remote_fd_close_retirements_total;
    snapshot.async_close_completions = g_remote_fd_async_close_completions;
    snapshot.async_close_misses = g_remote_fd_async_close_misses;
    s_vfs_lock.unlock();

    *out = snapshot;
}

#ifdef WOS_SELFTEST
namespace {
auto selftest_xattr_staged_namespace() -> bool;
}
auto wki_remote_vfs_selftest_xattr_replay_fencing() -> bool {
    VfsXattrServerStage stage{};
    stage.identity.session_id = 7;
    stage.identity.operation_id = 9;
    stage.committed = true;

    VfsXattrPhaseHeader incoming = stage.identity;
    incoming.operation_id = 10;
    if (vfs_xattr_new_begin_status(stage, incoming) != 0) {
        return false;
    }
    incoming.operation_id = 9;
    if (vfs_xattr_new_begin_status(stage, incoming) != -ESTALE) {
        return false;
    }
    incoming.operation_id = 10;
    incoming.session_id = 8;
    if (vfs_xattr_new_begin_status(stage, incoming) != -ESTALE) {
        return false;
    }
    incoming.session_id = 7;
    stage.committed = false;
    if (vfs_xattr_new_begin_status(stage, incoming) != 0) {
        return false;
    }

    VfsXattrBeginPayload begin{};
    begin.header = stage.identity;
    begin.remote_fd = stage.remote_fd;
    auto record = stage.caller_record;
    auto exact_begin = [&] {
        return vfs_xattr_begin_is_exact_replay(stage, begin, record, stage.path, stage.name, stage.export_root, stage.export_name);
    };
    if (!exact_begin()) return false;
    begin.remote_fd = 7;
    if (exact_begin()) return false;
    begin.remote_fd = stage.remote_fd;
    begin.require_directory = 1;
    if (exact_begin()) return false;
    begin.require_directory = 0;
    for (size_t index = 0; index < record.size(); ++index) {
        record[index] ^= 1;
        if (exact_begin()) return false;
        record[index] ^= 1;
    }
    if (!exact_begin()) return false;

    std::array<char, VFS_EXPORT_PATH_LEN> captured_root{}, captured_name{};
    if (capture_vfs_xattr_export("/physical", "/visible", captured_root, captured_name) != 0) return false;
    stage.export_root = captured_root;
    stage.export_name = captured_name;
    auto const ROOT_BEFORE = captured_root;
    auto const NAME_BEFORE = captured_name;
    std::array<char, VFS_EXPORT_PATH_LEN> unterminated{};
    unterminated.fill('x');
    if (capture_vfs_xattr_export("/physical", unterminated.data(), captured_root, captured_name) != -ENAMETOOLONG ||
        captured_root != ROOT_BEFORE || captured_name != NAME_BEFORE)
        return false;
    for (bool root : {false, true}) {
        auto wrong_root = captured_root;
        auto wrong_name = captured_name;
        (root ? wrong_root : wrong_name)[1] ^= 1;
        if (vfs_xattr_begin_is_exact_replay(stage, begin, record, stage.path, stage.name, wrong_root, wrong_name)) return false;
    }
    if (!exact_begin()) return false;

    stage.data.reset(new (std::nothrow) uint8_t[4]);
    if (stage.data == nullptr) {
        return false;
    }
    stage.data[0] = 1;
    stage.data[1] = 2;
    stage.data[2] = 3;
    stage.data[3] = 4;
    stage.transferred = 4;
    std::array<uint8_t, 2> exact = {2, 3};
    std::array<uint8_t, 2> changed = {2, 4};
    return vfs_xattr_transfer_total(true, 0, 4) == 0 && vfs_xattr_transfer_total(true, 4, 4) == 4 &&
           vfs_xattr_transfer_total(false, 4, 0) == 4 && vfs_xattr_data_is_exact_replay(stage, 1, exact.data(), exact.size()) &&
           !vfs_xattr_data_is_exact_replay(stage, 1, changed.data(), changed.size()) && selftest_xattr_staged_namespace();
}

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

auto wki_remote_vfs_selftest_legacy_scalar_wire_path_admission() -> bool {
    constexpr std::array<uint8_t, 8> SAFE_PATH = {'d', 'i', 'r', '/', 'f', 'i', 'l', 'e'};
    constexpr std::array<uint8_t, 5> ABSOLUTE_PATH = {'/', 'f', 'i', 'l', 'e'};
    constexpr std::array<uint8_t, 3> DOT_PATH = {'a', '/', '.'};
    constexpr std::array<uint8_t, 4> DOT_DOT_PATH = {'a', '/', '.', '.'};
    constexpr std::array<uint8_t, 5> NUL_PATH = {'f', 'i', '\0', 'l', 'e'};
    constexpr std::array<uint8_t, 4> NUL_TARGET = {'a', '\0', 'b', 'c'};

    if (exact_relative_wire_path(SAFE_PATH.data(), SAFE_PATH.size(), 0, SAFE_PATH.size()) != SAFE_PATH.data() ||
        exact_relative_wire_path(SAFE_PATH.data(), SAFE_PATH.size(), 0, SAFE_PATH.size() - 1) != nullptr ||
        exact_relative_wire_path(SAFE_PATH.data(), SAFE_PATH.size() - 1, 0, SAFE_PATH.size()) != nullptr ||
        exact_relative_wire_path(ABSOLUTE_PATH.data(), ABSOLUTE_PATH.size(), 0, ABSOLUTE_PATH.size()) != nullptr ||
        exact_relative_wire_path(DOT_PATH.data(), DOT_PATH.size(), 0, DOT_PATH.size()) != nullptr ||
        exact_relative_wire_path(DOT_DOT_PATH.data(), DOT_DOT_PATH.size(), 0, DOT_DOT_PATH.size()) != nullptr ||
        exact_relative_wire_path(NUL_PATH.data(), NUL_PATH.size(), 0, NUL_PATH.size()) != nullptr ||
        !wire_payload_has_optional_exact_suffix(10, 10, 8) || !wire_payload_has_optional_exact_suffix(18, 10, 8) ||
        wire_payload_has_optional_exact_suffix(11, 10, 8) || wire_payload_has_optional_exact_suffix(17, 10, 8) ||
        !wire_symlink_target_is_representable(SAFE_PATH.data(), SAFE_PATH.size(), 512) ||
        wire_symlink_target_is_representable(NUL_TARGET.data(), NUL_TARGET.size(), 512)) {
        return false;
    }

    std::array<uint8_t, 504> long_path{};
    long_path.fill('x');
    std::array<char, 512> full_path{};
    std::array<char, 512> visible_path{};
    if (prepare_legacy_scalar_path("/export", "/view", long_path.data(), 503, full_path.data(), full_path.size(), visible_path.data(),
                                   visible_path.size()) != 0 ||
        std::strlen(full_path.data()) != full_path.size() - 1 ||
        prepare_legacy_scalar_path("/export", "/view", long_path.data(), long_path.size(), full_path.data(), full_path.size(),
                                   visible_path.data(), visible_path.size()) != -ENAMETOOLONG) {
        return false;
    }

    std::array<char, 512> full_export{};
    full_export.fill('e');
    full_export.back() = '\0';
    return build_full_path(full_path.data(), full_path.size(), full_export.data(), "x", 1) == -ENAMETOOLONG;
}

auto wki_remote_vfs_selftest_mapped_component_path_admission() -> bool {
    constexpr std::array<uint8_t, 8> DOT_PATH = {'a', '/', '.', '/', '.', '.', '/', 'b'};
    constexpr std::array<uint8_t, 2> ABSOLUTE = {'/', 'a'};
    constexpr std::array<uint8_t, 3> NUL_PATH = {'a', '\0', 'b'};
    if (mapped_wire_path_status(DOT_PATH.data(), DOT_PATH.size(), false) != -ENOTSUP ||
        mapped_wire_path_status(DOT_PATH.data(), DOT_PATH.size(), true) != 0 || mapped_wire_path_status(nullptr, 0, false) != 0 ||
        mapped_wire_path_status(nullptr, 1, true) != -EINVAL ||
        mapped_wire_path_status(ABSOLUTE.data(), ABSOLUTE.size(), true) != -EINVAL ||
        mapped_wire_path_status(NUL_PATH.data(), NUL_PATH.size(), true) != -EINVAL ||
        exact_relative_wire_path(DOT_PATH.data(), DOT_PATH.size(), 0, DOT_PATH.size(), true) != DOT_PATH.data() ||
        exact_relative_wire_path(DOT_PATH.data(), DOT_PATH.size(), 0, DOT_PATH.size() - 1, true) != nullptr ||
        exact_relative_wire_path(DOT_PATH.data(), DOT_PATH.size(), 1, DOT_PATH.size(), true) != nullptr)
        return false;
    std::array<char, 512> backing{}, visible{};
    backing.fill('Z');
    visible.fill('Y');
    auto const BACKING_SENTINEL = backing;
    auto const VISIBLE_SENTINEL = visible;
    if (prepare_mapped_scalar_path("/export", "/view", DOT_PATH.data(), DOT_PATH.size(), false, backing.data(), backing.size(),
                                   visible.data(), visible.size()) != -ENOTSUP ||
        backing != BACKING_SENTINEL || visible != VISIBLE_SENTINEL)
        return false;
    if (prepare_mapped_scalar_path("/export", "/view", DOT_PATH.data(), DOT_PATH.size(), true, backing.data(), backing.size(),
                                   visible.data(), visible.size()) != 0 ||
        std::strcmp(backing.data(), "/export/a/./../b") != 0 || std::strcmp(visible.data(), "/view/a/./../b") != 0)
        return false;
    return prepare_legacy_scalar_path("/export", "/view", DOT_PATH.data(), DOT_PATH.size(), backing.data(), backing.size(), visible.data(),
                                      visible.size()) == -EINVAL;
}

auto wki_remote_vfs_selftest_directory_path_admission() -> bool {
    constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
    constexpr size_t OFFSET = RECORD + sizeof(VfsPathIdentityHeader) + sizeof(int32_t);
    constexpr char PATH[] = "private/../public";
    std::array<uint8_t, OFFSET + sizeof(PATH) - 1> bytes{};
    int32_t fd = 17;
    std::memcpy(bytes.data() + OFFSET - sizeof(fd), &fd, sizeof(fd));
    std::memcpy(bytes.data() + OFFSET, PATH, sizeof(PATH) - 1);
    VfsPathIdentityHeader header{.operation = VfsPathOperation::STAT_AT,
                                 .flags = WKI_VFS_PATH_FOLLOW_FINAL,
                                 .data_len = sizeof(fd) + sizeof(PATH) - 1,
                                 .reserved = 0};
    auto write_header = [&] { std::memcpy(bytes.data() + RECORD, &header, sizeof(header)); };
    write_header();
    // Exercise reliable RX permission classification as well as the owner
    // decoder: an unclassified selector never reaches the retained-FD path.
    std::array<uint8_t, sizeof(DevOpReqPayload) + bytes.size()> envelope{};
    DevOpReqPayload request{};
    request.op_id = OP_VFS_PATH_AS;
    request.data_len = bytes.size();
    std::memcpy(envelope.data(), &request, sizeof(request));
    constexpr auto REQUIRED = wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    for (auto operation : {VfsPathOperation::READLINK_AT, VfsPathOperation::STAT_AT}) {
        header.operation = operation;
        write_header();
        std::memcpy(envelope.data() + sizeof(request), bytes.data(), bytes.size());
        if (auth_policy::required_permissions(MsgType::DEV_OP_REQ, envelope.data(), envelope.size()) != REQUIRED) return false;
    }
    DirectoryPathRequest parsed{};
    auto decode = [&](bool components = true, bool enabled = true, size_t length = OFFSET + sizeof(PATH) - 1) {
        return decode_directory_path_request(bytes.data(), length, components, enabled, parsed);
    };
    if (decode() != 0 || parsed.directory_fd != fd || parsed.path_length != sizeof(PATH) - 1 ||
        std::memcmp(parsed.path, PATH, parsed.path_length) != 0)
        return false;
    auto const SENTINEL = parsed;
    auto denied = [&](int expected, bool components = true, bool enabled = true, size_t length = OFFSET + sizeof(PATH) - 1) {
        parsed = SENTINEL;
        return decode(components, enabled, length) == expected && parsed.directory_fd == SENTINEL.directory_fd &&
               parsed.path == SENTINEL.path && parsed.path_length == SENTINEL.path_length;
    };
    if (!denied(-ENOTSUP, false) || !denied(-ENOTSUP, true, false) || !denied(-EINVAL, true, true, bytes.size() - 1) ||
        !denied(-EINVAL, true, true, OFFSET))
        return false;
    bytes[OFFSET] = '/';
    if (!denied(-EINVAL)) return false;
    bytes[OFFSET] = 'p';
    bytes[OFFSET + 1] = 0;
    if (!denied(-EINVAL)) return false;
    bytes[OFFSET + 1] = 'r';
    fd = 0;
    std::memcpy(bytes.data() + OFFSET - sizeof(fd), &fd, sizeof(fd));
    if (!denied(-EBADF)) return false;
    fd = -1;
    std::memcpy(bytes.data() + OFFSET - sizeof(fd), &fd, sizeof(fd));
    if (!denied(-EBADF)) return false;
    fd = 17;
    std::memcpy(bytes.data() + OFFSET - sizeof(fd), &fd, sizeof(fd));
    header.reserved = 1;
    write_header();
    if (!denied(-EINVAL)) return false;
    header.reserved = 0;
    header.flags = 4;
    write_header();
    if (!denied(-EINVAL)) return false;
    header.operation = VfsPathOperation::READLINK_AT;
    header.flags = WKI_VFS_PATH_FOLLOW_FINAL;
    write_header();
    if (!denied(-EINVAL)) return false;
    header.flags = WKI_VFS_PATH_REQUIRE_DIRECTORY;
    write_header();
    if (decode() != 0) return false;
    header.operation = VfsPathOperation::REALPATH_AT;
    write_header();
    if (decode() != 0) return false;
    header.flags = WKI_VFS_PATH_FOLLOW_FINAL;
    write_header();
    if (!denied(-EINVAL)) return false;
    header.flags = 0;
    write_header();
    if (decode() != 0) return false;
    std::memcpy(envelope.data() + sizeof(request), bytes.data(), bytes.size());
    if (auth_policy::required_permissions(MsgType::DEV_OP_REQ, envelope.data(), envelope.size()) != REQUIRED) return false;
    header.operation = static_cast<VfsPathOperation>(99);
    write_header();
    if (!denied(-ENOTSUP)) return false;
    header.operation = VfsPathOperation::STAT_AT;
    header.flags = 0;
    header.data_len = WKI_VFS_FSTAT_RESPONSE_SIZE;
    if (!wki_vfs_path_response_valid(0, sizeof(header) + header.data_len, header) ||
        wki_vfs_path_response_valid(0, sizeof(header) + header.data_len - 1, header) ||
        wki_vfs_path_response_valid(-EACCES, sizeof(header), header) || !wki_vfs_path_response_valid(-EACCES, 0, header))
        return false;
    header.data_len = WKI_VFS_FSTAT_RESPONSE_SIZE - 1;
    if (wki_vfs_path_response_valid(0, sizeof(header) + header.data_len, header)) return false;
    header.operation = VfsPathOperation::READLINK_AT;
    header.data_len = 512;
    if (!wki_vfs_path_response_valid(0, sizeof(header) + header.data_len, header)) return false;
    header.data_len = 513;
    return !wki_vfs_path_response_valid(0, sizeof(header) + header.data_len, header);
}

auto wki_remote_vfs_selftest_directory_open_admission() -> bool {
    constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
    constexpr size_t BASE = RECORD + sizeof(VfsPathIdentityHeader);
    constexpr size_t OFFSET = BASE + sizeof(int32_t) + 2 * sizeof(uint32_t);
    constexpr char PATH[] = "private/../public";
    std::array<uint8_t, OFFSET + sizeof(PATH) - 1> bytes{};
    VfsPathIdentityHeader header{
        .operation = VfsPathOperation::OPEN_AT, .flags = 0, .data_len = sizeof(int32_t) + 2 * sizeof(uint32_t) + sizeof(PATH) - 1};
    int32_t fd = 17;
    uint32_t flags = 2, mode = 0600;
    auto write = [&] {
        std::memcpy(bytes.data() + RECORD, &header, sizeof(header));
        std::memcpy(bytes.data() + BASE, &fd, sizeof(fd));
        std::memcpy(bytes.data() + BASE + sizeof(fd), &flags, sizeof(flags));
        std::memcpy(bytes.data() + BASE + sizeof(fd) + sizeof(flags), &mode, sizeof(mode));
    };
    write();
    std::memcpy(bytes.data() + OFFSET, PATH, sizeof(PATH) - 1);
    DirectoryPathRequest parsed{};
    if (decode_directory_path_request(bytes.data(), bytes.size(), true, true, parsed) != 0 || parsed.directory_fd != fd ||
        parsed.open_flags != flags || parsed.open_mode != mode || parsed.path_length != sizeof(PATH) - 1 ||
        std::memcmp(parsed.path, PATH, parsed.path_length) != 0)
        return false;
    auto const SENTINEL = parsed;
    auto denied = [&](int expected, bool components = true, bool enabled = true, size_t length = OFFSET + sizeof(PATH) - 1) {
        parsed = SENTINEL;
        return decode_directory_path_request(bytes.data(), length, components, enabled, parsed) == expected &&
               parsed.directory_fd == SENTINEL.directory_fd && parsed.open_flags == SENTINEL.open_flags &&
               parsed.open_mode == SENTINEL.open_mode && parsed.path == SENTINEL.path && parsed.path_length == SENTINEL.path_length;
    };
    if (!denied(-ENOTSUP, false) || !denied(-ENOTSUP, true, false) || !denied(-EINVAL, true, true, OFFSET) ||
        !denied(-EINVAL, true, true, bytes.size() - 1))
        return false;
    flags = 3;
    write();
    if (!denied(-EINVAL)) return false;
    flags = 2;
    header.flags = WKI_VFS_PATH_FOLLOW_FINAL;
    write();
    if (!denied(-EINVAL)) return false;
    header.flags = 0;
    header.reserved = 1;
    write();
    if (!denied(-EINVAL)) return false;
    header.reserved = 0;
    fd = 0;
    write();
    if (!denied(-EBADF)) return false;
    fd = 17;
    write();
    bytes[OFFSET] = '/';
    if (!denied(-EINVAL)) return false;
    bytes[OFFSET] = 0;
    if (!denied(-EINVAL)) return false;
    bytes[OFFSET] = 'p';
    std::array<uint8_t, sizeof(DevOpReqPayload) + bytes.size()> envelope{};
    DevOpReqPayload request{.op_id = OP_VFS_PATH_AS, .data_len = bytes.size()};
    std::memcpy(envelope.data(), &request, sizeof(request));
    for (uint32_t value : {0U, 1U, 2U, 0100U, 01000U, 0200000U, 0400000U}) {
        flags = value;
        write();
        std::memcpy(envelope.data() + sizeof(request), bytes.data(), bytes.size());
        if (auth_policy::required_permissions(MsgType::DEV_OP_REQ, envelope.data(), envelope.size()) !=
                auth_policy::vfs_open_permissions(flags) ||
            decode_directory_path_request(bytes.data(), bytes.size(), true, true, parsed) != 0 || parsed.open_flags != value)
            return false;
    }
    std::array<uint8_t, OFFSET + 512> oversized{};
    std::memcpy(oversized.data(), bytes.data(), OFFSET);
    std::memset(oversized.data() + OFFSET, 'a', 512);
    header.data_len = OFFSET - BASE + 512;
    std::memcpy(oversized.data() + RECORD, &header, sizeof(header));
    if (decode_directory_path_request(oversized.data(), oversized.size(), true, true, parsed) != -EINVAL) return false;
    header.data_len--;
    std::memcpy(oversized.data() + RECORD, &header, sizeof(header));
    if (decode_directory_path_request(oversized.data(), oversized.size() - 1, true, true, parsed) != 0 || parsed.path_length != 511)
        return false;
    VfsPathOpenResult reply{.fd = 17, .is_directory = 0};
    if (!wki_vfs_path_open_result_valid(reply)) return false;
    reply.is_directory = 1;
    if (!wki_vfs_path_open_result_valid(reply)) return false;
    reply.is_directory = 2;
    if (wki_vfs_path_open_result_valid(reply)) return false;
    reply.is_directory = 0;
    reply.fd = 0;
    if (wki_vfs_path_open_result_valid(reply)) return false;
    reply.fd = -1;
    if (wki_vfs_path_open_result_valid(reply)) return false;
    reply.fd = 17;
    for (size_t i = 0; i < reply.reserved.size(); ++i) {
        reply.reserved[i] = 1;
        if (wki_vfs_path_open_result_valid(reply)) return false;
        reply.reserved[i] = 0;
    }
    return true;
}

auto wki_remote_vfs_selftest_open_lifetime_owner() -> bool {
    if (g_wki.initialized) return false;
    WkiChannel channel{};
    WkiChannelIdentity identity{.channel = &channel, .peer_node_id = 0x7FFD, .channel_id = 79, .generation = 21};
    DevServerBinding binding{}, successor{};
    for (auto* target : {&binding, &successor}) {
        target->active = true;
        target->resource_type = ResourceType::VFS;
        target->consumer_node = identity.peer_node_id;
        target->consumer_boot_epoch = 1;
        target->resource_id = 1;
        target->resource_incarnation = {.owner_boot_epoch = 1, .resource_incarnation = 1};
        target->vfs_export_publication_revision = 2;
        target->channel_identity = identity;
    }
    successor.channel_identity.generation++;
    VfsOpenLifetimeRequest first_binding{}, next_binding{};
    if (query_ordinary_open_binding(binding, identity, 91, first_binding) != 0 ||
        query_ordinary_open_binding(successor, successor.channel_identity, 92, next_binding) != 0 ||
        !wki_vfs_open_binding_response_valid(first_binding, 91, sizeof(first_binding)) ||
        !wki_vfs_open_binding_response_valid(next_binding, 92, sizeof(next_binding)) || first_binding.binding_id == next_binding.binding_id)
        return false;
    auto request_for = [](const DevServerBinding& target, uint64_t token) {
        return VfsOpenLifetimeRequest{
            .token = token, .binding_id = target.vfs_open_binding_id, .owner_boot_epoch = target.resource_incarnation.owner_boot_epoch};
    };
    auto admit = [&](DevServerBinding& target, const WkiChannelIdentity& id, uint64_t token) {
        return admit_ordinary_open_token(target, id, request_for(target, token));
    };
    auto cancel = [&](DevServerBinding& target, const WkiChannelIdentity& id, uint64_t token) {
        return cancel_ordinary_open(target, id, request_for(target, token));
    };
    // Delayed old-binding requests may arrive on a newly reused numeric channel.
    // Even when RX supplies the successor's current local identity, its wire
    // namespace must refuse before changing high water or releasing any File.
    auto old = request_for(binding, 7);
    if (admit_ordinary_open_token(successor, successor.channel_identity, old) != -ENOTCONN ||
        cancel_ordinary_open(successor, successor.channel_identity, old) != -ENOTCONN || successor.vfs_open_token_high_water != 0)
        return false;
    old = request_for(successor, 7);
    old.owner_boot_epoch++;
    if (admit_ordinary_open_token(successor, successor.channel_identity, old) != -ENOTCONN ||
        cancel_ordinary_open(successor, successor.channel_identity, old) != -ENOTCONN || successor.vfs_open_token_high_water != 0)
        return false;

    // CANCEL before an OPEN, or after a failed OPEN, cannot resurrect it.
    if (cancel(binding, identity, 2) != 0 || admit(binding, identity, 2) != -EALREADY || admit(binding, identity, 1) != -EALREADY ||
        admit(binding, identity, 0) != -EINVAL || cancel(binding, identity, 0) != -EINVAL || binding.vfs_open_token_high_water != 2)
        return false;
    if (admit(binding, identity, 3) != 0 || admit(binding, identity, 3) != -EALREADY || cancel(binding, identity, 3) != 0 ||
        admit(binding, identity, 3) != -EALREADY)
        return false;
    if (admit(successor, identity, 4) != -ENOTCONN || successor.vfs_open_token_high_water != 0) return false;

    int closes = 0;
    ker::vfs::FileOperations operations{.vfs_close = [](ker::vfs::File* file) -> int {
        ++*static_cast<int*>(file->private_data);
        file->private_data = nullptr;
        return -EIO;
    }};
    auto make_file = [&]() {
        auto* file = new ker::vfs::File{};
        file->refcount = 1;
        file->fs_type = ker::vfs::FSType::REMOTE;
        file->open_flags = ker::vfs::O_NO_CACHE;
        file->fops = &operations;
        file->private_data = &closes;
        return file;
    };
    auto release = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
    std::unique_ptr<ker::vfs::File, decltype(release)> first(make_file(), release), next(make_file(), release);
    // The same numeric token on a successor binding denotes an independent File.
    if (admit(binding, identity, 4) != 0 || admit(successor, successor.channel_identity, 4) != 0) return false;
    RemoteFdReservation original(identity), replacement(successor.channel_identity);
    int32_t const FIRST_FD = original.result();
    int32_t const NEXT_FD = replacement.result();
    if (FIRST_FD <= 0 || NEXT_FD <= 0) return false;
    ker::vfs::vfs_retain_file(first.get());  // An admitted reader outlives registry cancellation.
    if (!original.publish(first.get(), 0, 0, 0, 4, binding.vfs_open_binding_id)) {
        ker::vfs::vfs_put_file(first.get());
        return false;
    }
    if (!replacement.publish(next.get(), 0, 0, 0, 4, successor.vfs_open_binding_id)) {
        static_cast<void>(cancel(binding, identity, 4));
        return false;
    }
    static_cast<void>(next.release());
    bool ok = cancel(binding, identity, 4) == 0 && closes == 0;
    s_vfs_lock.lock();
    ok = !g_remote_fds.contains(FIRST_FD) && find_remote_fd(successor.channel_identity, NEXT_FD) != nullptr && ok;
    s_vfs_lock.unlock();
    ok = cancel(binding, identity, 4) == 0 && closes == 0 && ok;
    first.reset();
    ok = closes == 1 && ok;
    // A close error still consumes the registry owner; retries cannot close twice.
    ok = cancel(successor, successor.channel_identity, 4) == -EIO && closes == 2 && ok;
    ok = cancel(successor, successor.channel_identity, 4) == 0 && closes == 2 && ok;
    ok = admit(binding, identity, 4) == -EALREADY && ok;
    binding.retiring.store(true, std::memory_order_release);
    ok = admit(binding, identity, 5) == -ENOTCONN && cancel(binding, identity, 5) == -ENOTCONN && binding.vfs_open_token_high_water == 4 &&
         ok;
    ok = admit(successor, successor.channel_identity, UINT64_MAX) == 0 && admit(successor, successor.channel_identity, 1) == -EALREADY &&
         cancel(successor, successor.channel_identity, UINT64_MAX) == 0 && ok;
    return ok;
}

auto wki_remote_vfs_selftest_directory_open_reservation() -> bool {
    WkiChannel channel{};
    WkiChannelIdentity identity{.channel = &channel, .peer_node_id = 0x7FFF, .channel_id = 77, .generation = 19};
    auto stale = identity;
    stale.generation++;
    int32_t cancelled = -1;
    bool invisible = false;
    s_vfs_lock.lock();
    size_t const BEFORE = g_remote_fds.size();
    uint64_t const OPENS = g_remote_fd_opens_total;
    s_vfs_lock.unlock();
    {
        RemoteFdReservation reservation(identity);
        cancelled = reservation.result();
        s_vfs_lock.lock();
        invisible = cancelled > 0 && g_remote_fds.contains(cancelled) && find_remote_fd(identity, cancelled) == nullptr &&
                    find_remote_fd(stale, cancelled) == nullptr && g_remote_fd_opens_total == OPENS;
        s_vfs_lock.unlock();
    }
    s_vfs_lock.lock();
    bool const CANCELLED = !g_remote_fds.contains(cancelled) && g_remote_fds.size() == BEFORE && g_remote_fd_opens_total == OPENS;
    s_vfs_lock.unlock();
    ker::vfs::File file{};
    RemoteFdReservation reservation(identity);
    int32_t const FD = reservation.result();
    bool const COMMITTED = reservation.publish(&file);
    s_vfs_lock.lock();
    auto* published = find_remote_fd(identity, FD);
    bool const EXACT = COMMITTED && published != nullptr && published->file == &file && find_remote_fd(stale, FD) == nullptr &&
                       g_remote_fd_opens_total == OPENS + 1;
    auto retired = g_remote_fds.extract(FD);
    bool const CLEAN = g_remote_fds.size() == BEFORE;
    s_vfs_lock.unlock();
    RemoteFdReservation fenced(identity);
    int32_t const FENCED_FD = fenced.result();
    wki_remote_vfs_mark_server_fds_for_channel(identity);
    bool const FENCE_REJECTED = FENCED_FD > 0 && !fenced.publish(&file);
    s_vfs_lock.lock();
    auto fenced_row = g_remote_fds.extract(FENCED_FD);
    bool const FENCED_CLEAN = g_remote_fds.size() == BEFORE && g_remote_fd_opens_total == OPENS + 1;
    s_vfs_lock.unlock();
    return invisible && CANCELLED && EXACT && CLEAN && FENCE_REJECTED && FENCED_CLEAN;
}

auto wki_remote_vfs_selftest_native_directory_grant() -> bool {
    return wki_dev_server_selftest_with_directory_grant_bindings([](DevServerBinding&, DevServerBinding& destination) {
        namespace vfs = ker::vfs;
        namespace security = ker::mod::security;
        auto fail = [](int line) {
            ker::mod::dbg::logger<"ktest">::error("native directory grant invariant failed at remote_vfs.cpp:%d", line);
            return false;
        };
        constexpr uint32_t RESOURCE = 0xD19077;
        constexpr auto BASE = "/tmp/ktest_native_directory_grant";
        constexpr auto EXPORT = "/tmp/ktest_native_directory_grant/root";
        constexpr auto DIR = "/tmp/ktest_native_directory_grant/root/dir";
        constexpr auto MOVED = "/tmp/ktest_native_directory_grant/moved";
        constexpr auto MOVED_DIR = "/tmp/ktest_native_directory_grant/moved/dir";
        constexpr auto NEW = "/tmp/ktest_native_directory_grant/root/dir/new";
        auto caller = security::Snapshot::bootstrap_root();
        if (caller.prepare({.change = security::Change::SET_GID, .real = 1003}, caller) != 0 ||
            caller.prepare({.change = security::Change::SET_UID, .real = 1003}, caller) != 0)
            return fail(__LINE__);
        ker::mod::sched::task::Task worker{};
        if (ker::mod::sched::resource::initialize_credentials(worker, caller) != 0) return fail(__LINE__);
        destination.resource_id = RESOURCE;
        destination.resource_incarnation = {g_wki.local_boot_epoch, 9};
        for (bool disk : {false, true}) {
            struct Cleanup {
                WkiChannelIdentity destination;
                bool mounted{};
                ~Cleanup() {
                    directory_grant_adoption_test_hook = nullptr;
                    static_cast<void>(cancel_directory_pin(destination, 3));
                    s_vfs_lock.lock();
                    std::erase_if(g_vfs_exports, [](const VfsExport& exp) { return exp.resource_id == RESOURCE; });
                    s_vfs_lock.unlock();
                    static_cast<void>(vfs::vfs_unlink(NEW));
                    for (auto path : {DIR, MOVED_DIR, EXPORT, MOVED}) static_cast<void>(vfs::vfs_rmdir(path));
                    if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
                    static_cast<void>(vfs::vfs_rmdir(BASE));
                }
            } cleanup{destination.channel_identity};
            if (vfs::vfs_mkdir(BASE, 0755) != 0) return fail(__LINE__);
            if (disk) {
                auto* device = ker::dev::block_device_find_by_name("sdb2");
                if (device == nullptr || vfs::mount_filesystem(BASE, "xfs", device) != 0) return fail(__LINE__);
                cleanup.mounted = true;
            }
            if (vfs::vfs_mkdir(EXPORT, 0755) != 0 || vfs::vfs_mkdir(DIR, 0777) != 0) return fail(__LINE__);
            auto make_view = [&](vfs::DirectoryViewRef& output) -> int {
                vfs::File* file = nullptr;
                int status = vfs::vfs_pin_directory_resolved_beneath_as(worker, caller, EXPORT, EXPORT, &file);
                if (status != 0) return status;
                std::unique_ptr<vfs::File, DirectoryGrantFileRelease> root(file);
                file = nullptr;
                status = vfs::vfs_pin_directory_resolved_beneath_as(worker, caller, EXPORT, DIR, &file);
                if (status != 0) return status;
                std::unique_ptr<vfs::File, DirectoryGrantFileRelease> directory(file);
                return vfs::vfs_prepare_directory_view(caller, root.get(), directory.get(), {}, output);
            };
            vfs::DirectoryViewRef view;
            if (make_view(view) != 0) return fail(__LINE__);
            VfsExport resource{.active = true,
                               .resource_id = RESOURCE,
                               .resource_incarnation = 9,
                               .publication_revision = g_vfs_export_revision,
                               .backing_dev_id = view.root()->mount_owner->dev_id,
                               .backing_fs_type = view.root()->fs_type};
            std::strcpy(resource.export_path, EXPORT);
            std::strcpy(resource.name, EXPORT);
            // Before wki_init, this fixture exclusively owns export mutation;
            // deque allocation is outside the registry lock.
            g_vfs_exports.push_back(resource);
            auto issue = [&](uint16_t target, uint32_t boot, vfs::MountPolicy policy,
                             std::unique_ptr<vfs::File, DirectoryGrantFileRelease>& staged, VfsDirectoryGrant& grant) -> int {
                vfs::File* file = nullptr;
                int const STATUS =
                    wki_remote_vfs_issue_native_directory_grant(worker, caller, RESOURCE, view, target, boot, 901, policy, &file, grant);
                if (STATUS == 0)
                    staged.reset(file);
                else if (file != nullptr)
                    return -EIO;
                return STATUS;
            };
            std::unique_ptr<vfs::File, DirectoryGrantFileRelease> staging;
            VfsDirectoryGrant grant{};
            if (issue(g_wki.my_node_id, g_wki.local_boot_epoch, {.read_only = true, .nosuid = true}, staging, grant) != 0 ||
                grant.source_node != g_wki.my_node_id || grant.policy != WKI_VFS_DIRECTORY_GRANT_POLICY ||
                staging->fops->vfs_read != nullptr || staging->fops->vfs_write != nullptr || staging->fops->vfs_readdir != nullptr)
                return fail(__LINE__);
            view = {};  // The staging row owns independent native pins.
            WkiDirectoryGrantSource const SOURCE{
                .node = g_wki.my_node_id,
                .boot_epoch = g_wki.local_boot_epoch,
                .submission_id = 901,
                .identity_policy = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN)};
            vfs::DirectoryViewRef adopted;
            auto set_resource_active = [&](bool active) {
                s_vfs_lock.lock();
                for (auto& exp : g_vfs_exports)
                    if (exp.resource_id == RESOURCE) exp.active = active;
                s_vfs_lock.unlock();
            };
            set_resource_active(false);
            if (wki_remote_vfs_adopt_directory_grant_native(caller, SOURCE, grant, adopted) != -ENOTCONN || adopted) return fail(__LINE__);
            set_resource_active(true);
            if (vfs::vfs_chmod(DIR, 0700) != 0 || wki_remote_vfs_adopt_directory_grant_native(caller, SOURCE, grant, adopted) != -EACCES ||
                adopted)
                return fail(__LINE__);
            if (vfs::vfs_chmod(DIR, 0777) != 0 || wki_remote_vfs_adopt_directory_grant_native(caller, SOURCE, grant, adopted) != 0 ||
                !adopted.policy().read_only || !adopted.policy().nosuid)
                return fail(__LINE__);
            if (wki_remote_vfs_adopt_directory_grant_native(caller, SOURCE, grant, adopted) != -EALREADY) return fail(__LINE__);
            staging.reset();
            vfs::File* denied = nullptr;
            if (vfs::vfs_open_directory_view_as(worker, caller, adopted, "new", 1 | vfs::O_CREAT, 0600, &denied) != -EROFS ||
                denied != nullptr)
                return fail(__LINE__);
            adopted = {};
            if (make_view(view) != 0 ||
                issue(destination.consumer_node, destination.consumer_boot_epoch, {.read_only = true, .nosuid = true}, staging, grant) != 0)
                return fail(__LINE__);
            view = {};
            if (vfs::vfs_rename(EXPORT, MOVED) != 0 || vfs::vfs_mkdir(EXPORT, 0755) != 0) return fail(__LINE__);
            VfsDirectoryGrantRequest request{.operation = VfsDirectoryGrantOperation::ADOPT, .pin_token = 3, .grant = grant};
            VfsDirectoryGrantResponse response{};
            std::array<char, 512> name{};
            if (directory_grant_owner(worker, caller, destination, request, response, name) != 0 || std::strcmp(name.data(), "/dir") != 0)
                return fail(__LINE__);
            staging.reset();
            std::unique_ptr<vfs::File, DirectoryGrantFileRelease> receiver;
            s_vfs_lock.lock();
            if (auto* row = find_directory_pin_locked(destination.channel_identity, 3); row != nullptr) {
                vfs::vfs_retain_file(row->file);
                receiver.reset(row->file);
            }
            s_vfs_lock.unlock();
            if (!receiver || !receiver->directory_view.policy().read_only || !receiver->directory_view.policy().nosuid ||
                cancel_directory_pin(destination.channel_identity, 3) != 0)
                return fail(__LINE__);
            view = receiver->directory_view;
            VfsDirectoryGrant untouched{};
            if (issue(g_wki.my_node_id, g_wki.local_boot_epoch, {}, staging, untouched) != -EPERM || untouched.owner_grant_id != 0)
                return fail(__LINE__);
            view = {};
            receiver.reset();
            if (vfs::vfs_rmdir(EXPORT) != 0 || vfs::vfs_rename(MOVED, EXPORT) != 0 || make_view(view) != 0) return fail(__LINE__);
            if (issue(g_wki.my_node_id, g_wki.local_boot_epoch, {}, staging, grant) != 0) return fail(__LINE__);
            directory_grant_adoption_test_hook = [](const VfsDirectoryGrant& candidate) {
                static_cast<void>(cancel_native_directory_grant(candidate.owner_grant_id));
            };
            int const CANCELLED = wki_remote_vfs_adopt_directory_grant_native(caller, SOURCE, grant, adopted);
            directory_grant_adoption_test_hook = nullptr;
            if (CANCELLED != -ENOTCONN || adopted) return fail(__LINE__);
            staging.reset();
            if (issue(g_wki.my_node_id, g_wki.local_boot_epoch, {}, staging, grant) != 0 ||
                wki_remote_vfs_adopt_directory_grant_native(caller, SOURCE, grant, adopted) != 0 || adopted.policy().read_only ||
                adopted.policy().nosuid)
                return fail(__LINE__);
            vfs::File* writable = nullptr;
            if (vfs::vfs_open_directory_view_as(worker, caller, adopted, "new", 1 | vfs::O_CREAT, 0600, &writable) != 0)
                return fail(__LINE__);
            vfs::vfs_put_file(writable);
            adopted = {};
            staging.reset();
            std::array<std::unique_ptr<vfs::File, DirectoryGrantFileRelease>, VFS_DIRECTORY_GRANTS_PER_CONSUMER> bounded;
            for (auto& pin : bounded)
                if (issue(g_wki.my_node_id, g_wki.local_boot_epoch, {}, pin, grant) != 0) return fail(__LINE__);
            untouched = {};
            if (issue(g_wki.my_node_id, g_wki.local_boot_epoch, {}, staging, untouched) != -EMFILE || staging ||
                untouched.owner_grant_id != 0)
                return fail(__LINE__);
            for (auto& pin : bounded) pin.reset();
            view = {};
            s_vfs_lock.lock();
            bool const EMPTY = g_native_directory_grants.empty();
            s_vfs_lock.unlock();
            if (!EMPTY || worker.credentials() != caller || vfs::vfs_unlink(NEW) != 0 || vfs::vfs_rmdir(DIR) != 0 ||
                vfs::vfs_rmdir(EXPORT) != 0)
                return fail(__LINE__);
            if (cleanup.mounted) {
                if (vfs::unmount_filesystem(BASE) != 0) return fail(__LINE__);
                cleanup.mounted = false;
            }
            if (vfs::vfs_rmdir(BASE) != 0) return fail(__LINE__);
        }
        return true;
    });
}

auto wki_remote_vfs_selftest_directory_grant_lifetime() -> bool {
    return wki_dev_server_selftest_with_directory_grant_bindings([](DevServerBinding& source, DevServerBinding& destination) {
        namespace vfs = ker::vfs;
        namespace security = ker::mod::security;
        auto fail = [](int line) {
            ker::mod::dbg::logger<"ktest">::error("directory grant invariant failed at remote_vfs.cpp:%d", line);
            return false;
        };
        auto const ROOT = security::Snapshot::bootstrap_root();
        auto caller = ROOT;
        if (caller.prepare({.change = security::Change::SET_GID, .real = 1003}, caller) != 0 ||
            caller.prepare({.change = security::Change::SET_UID, .real = 1003}, caller) != 0)
            return fail(__LINE__);
        ker::mod::sched::task::Task worker{};
        if (ker::mod::sched::resource::initialize_credentials(worker, caller) != 0) return fail(__LINE__);
        constexpr auto BASE = "/tmp/ktest_directory_grant";
        constexpr auto EXPORT = "/tmp/ktest_directory_grant/root";
        constexpr auto DIR = "/tmp/ktest_directory_grant/root/dir";
        constexpr auto MOVED = "/tmp/ktest_directory_grant/moved";
        constexpr auto MOVED_DIR = "/tmp/ktest_directory_grant/moved/dir";
        constexpr auto NEW = "/tmp/ktest_directory_grant/moved/dir/new";
        uint64_t next = UINT64_MAX;
        if (take_directory_grant_id(next) != UINT64_MAX || next != 0 || take_directory_grant_id(next) != 0) return fail(__LINE__);
        for (bool disk : {false, true}) {
            struct Cleanup {
                WkiChannelIdentity source, destination;
                bool mounted{};
                ~Cleanup() {
                    directory_grant_adoption_test_hook = nullptr;
                    for (uint64_t token = 1; token < 140; ++token) {
                        static_cast<void>(cancel_directory_pin(source, token));
                        static_cast<void>(cancel_directory_pin(destination, token));
                    }
                    static_cast<void>(vfs::vfs_unlink(NEW));
                    for (auto path : {DIR, EXPORT, MOVED_DIR, MOVED}) static_cast<void>(vfs::vfs_rmdir(path));
                    if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
                    static_cast<void>(vfs::vfs_rmdir(BASE));
                }
            } cleanup{source.channel_identity, destination.channel_identity};
            if (vfs::vfs_mkdir(BASE, 0755) != 0) return fail(__LINE__);
            if (disk) {
                auto* device = ker::dev::block_device_find_by_name("sdb2");
                if (device == nullptr || vfs::mount_filesystem(BASE, "xfs", device) != 0) return fail(__LINE__);
                cleanup.mounted = true;
            }
            if (vfs::vfs_mkdir(EXPORT, 0755) != 0 || vfs::vfs_mkdir(DIR, 0777) != 0) return fail(__LINE__);
            VfsDirectoryResponse original{};
            std::array<char, 512> name{};
            VfsDirectoryRequest const PIN{.token = 1, .path_length = 3};
            constexpr std::array<uint8_t, 3> PATH{'d', 'i', 'r'};
            if (directory_reference_owner(worker, caller, source.channel_identity, EXPORT, EXPORT, PIN, PATH.data(), true, original,
                                          name) != 0)
                return fail(__LINE__);
            VfsDirectoryGrantRequest issue{.policy = WKI_VFS_DIRECTORY_GRANT_POLICY,
                                           .target_node = g_wki.my_node_id,
                                           .directory_fd = original.fd,
                                           .target_boot_epoch = g_wki.local_boot_epoch,
                                           .source_pin_token = 1,
                                           .pin_token = 2,
                                           .submission_id = 701};
            VfsDirectoryGrantResponse prepared{};
            if (directory_grant_owner(worker, caller, source, issue, prepared, name) != 0 || prepared.directory.fd == original.fd ||
                !wki_vfs_directory_grant_valid(prepared.grant) || prepared.grant.policy != WKI_VFS_DIRECTORY_GRANT_POLICY)
                return fail(__LINE__);
            // Exact binding admission rejects a different generation, resource,
            // boot, publication or retiring source, and balances every pin.
            auto binding_check = [&](const WkiChannelIdentity& identity, const VfsDirectoryGrant& grant, uint64_t revision, bool expected) {
                auto* held = wki_dev_server_retain_vfs_grant_source(identity, grant, revision);
                bool const OK = (held != nullptr) == expected && source.refs.load() == (expected ? 2U : 1U);
                wki_dev_server_release_vfs_grant_source(held);
                return OK && source.refs.load() == 1;
            };
            auto stale = source.channel_identity;
            ++stale.generation;
            if (!binding_check(source.channel_identity, prepared.grant, 23, true) || !binding_check(stale, prepared.grant, 23, false) ||
                !binding_check(source.channel_identity, prepared.grant, 24, false))
                return fail(__LINE__);
            source.retiring.store(true);
            bool const RETIRED = binding_check(source.channel_identity, prepared.grant, 23, false);
            source.retiring.store(false);
            if (!RETIRED) return fail(__LINE__);
            WkiDirectoryGrantSource context{
                .node = source.consumer_node,
                .boot_epoch = source.consumer_boot_epoch,
                .submission_id = issue.submission_id,
                .identity_policy = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN)};
            vfs::DirectoryViewRef adopted;
            s_vfs_lock.lock();
            find_directory_grant_locked(prepared.grant)->last_activity_us = 1;
            s_vfs_lock.unlock();
            auto unchanged = [&] {
                s_vfs_lock.lock();
                auto* row = find_directory_grant_locked(prepared.grant);
                bool const OK = row != nullptr && !row->directory_grant_claimed && row->last_activity_us == 1;
                s_vfs_lock.unlock();
                return OK && !adopted;
            };
            // Every field is owner-bound, including apparently narrower policy.
            for (unsigned field = 0; field < 12; ++field) {
                auto wrong = prepared.grant;
                switch (field) {
                    case 0:
                        ++wrong.owner_node;
                        break;
                    case 1:
                        ++wrong.source_node;
                        break;
                    case 2:
                        ++wrong.target_node;
                        break;
                    case 3:
                        ++wrong.resource_id;
                        break;
                    case 4:
                        ++wrong.owner_incarnation.owner_boot_epoch;
                        break;
                    case 5:
                        ++wrong.owner_incarnation.resource_incarnation;
                        break;
                    case 6:
                        ++wrong.source_boot_epoch;
                        break;
                    case 7:
                        ++wrong.target_boot_epoch;
                        break;
                    case 8:
                        ++wrong.owner_grant_id;
                        break;
                    case 9:
                        ++wrong.submission_id;
                        break;
                    case 10:
                        wrong.policy = 0;
                        break;
                    default:
                        wrong.reserved = 1;
                        break;
                }
                if (wki_remote_vfs_adopt_directory_grant_native(caller, context, wrong, adopted) >= 0 || !unchanged())
                    return fail(__LINE__);
            }
            auto wrong_context = context;
            wrong_context.identity_policy = 0;
            if (wki_remote_vfs_adopt_directory_grant_native(caller, wrong_context, prepared.grant, adopted) != -EACCES || !unchanged())
                return fail(__LINE__);
            auto different = ROOT;
            if (different.prepare({.change = security::Change::SET_GID, .real = 1003}, different) != 0 ||
                different.prepare({.change = security::Change::SET_UID, .real = 1004}, different) != 0 ||
                wki_remote_vfs_adopt_directory_grant_native(different, context, prepared.grant, adopted) != -EACCES || !unchanged())
                return fail(__LINE__);
            if (vfs::vfs_chmod(DIR, 0700) != 0 ||
                wki_remote_vfs_adopt_directory_grant_native(caller, context, prepared.grant, adopted) != -EACCES || !unchanged())
                return fail(__LINE__);
            if (vfs::vfs_chmod(DIR, 0777) != 0) return fail(__LINE__);
            // Prepared grant survives original File closure and name reuse.
            if (cancel_directory_pin(source.channel_identity, 1) != 0 || vfs::vfs_rename(EXPORT, MOVED) != 0 ||
                vfs::vfs_mkdir(EXPORT, 0755) != 0 ||
                wki_remote_vfs_adopt_directory_grant_native(caller, context, prepared.grant, adopted) != 0 || !adopted ||
                !adopted.policy().read_only || !adopted.policy().nosuid)
                return fail(__LINE__);
            auto* const ADOPTED_ROOT = adopted.root();
            if (wki_remote_vfs_adopt_directory_grant_native(caller, context, prepared.grant, adopted) != -EALREADY ||
                adopted.root() != ADOPTED_ROOT || cancel_directory_pin(source.channel_identity, 2) != 0)
                return fail(__LINE__);
            size_t length = 0;
            if (vfs::vfs_directory_path_beneath_directory(adopted.directory(), adopted.root(), name.data(), name.size(), &length) != 0 ||
                std::strcmp(name.data(), "/dir") != 0)
                return fail(__LINE__);
            vfs::File* denied = nullptr;
            if (vfs::vfs_open_directory_view_as(worker, caller, adopted, "new", 1 | vfs::O_CREAT, 0600, &denied) != -EROFS ||
                denied != nullptr)
                return fail(__LINE__);
            adopted = {};
            // New source pin under the retained native root for remote adoption.
            if (directory_reference_owner(worker, caller, source.channel_identity, MOVED, MOVED, PIN, PATH.data(), true, original, name) !=
                0)
                return fail(__LINE__);
            issue.directory_fd = original.fd;
            issue.target_node = destination.consumer_node;
            issue.target_boot_epoch = destination.consumer_boot_epoch;
            if (directory_grant_owner(worker, caller, source, issue, prepared, name) != 0) return fail(__LINE__);
            VfsDirectoryGrantRequest import{.operation = VfsDirectoryGrantOperation::ADOPT, .pin_token = 3, .grant = prepared.grant};
            VfsDirectoryGrantResponse imported{};
            if (directory_grant_owner(worker, caller, destination, import, imported, name) != 0 ||
                imported.directory.fd == prepared.directory.fd || cancel_directory_pin(source.channel_identity, 2) != 0)
                return fail(__LINE__);
            std::unique_ptr<vfs::File, DirectoryGrantFileRelease> receiver;
            s_vfs_lock.lock();
            if (auto* row = find_directory_pin_locked(destination.channel_identity, 3); row != nullptr) {
                vfs::vfs_retain_file(row->file);
                receiver.reset(row->file);
            }
            s_vfs_lock.unlock();
            if (!receiver || !receiver->directory_view.policy().read_only || !receiver->directory_view.policy().nosuid ||
                cancel_directory_pin(destination.channel_identity, 3) != 0 ||
                vfs::vfs_open_directory_view_as(worker, caller, receiver->directory_view, "new", 1 | vfs::O_CREAT, 0600, &denied) != -EROFS)
                return fail(__LINE__);
            receiver.reset();
            // Cancellation after native work but before final publication leaves
            // no destination PIN and does not overwrite reply/name outputs.
            if (directory_grant_owner(worker, caller, source, issue, prepared, name) != 0) return fail(__LINE__);
            import.grant = prepared.grant;
            imported = {};
            name.fill('!');
            directory_grant_adoption_test_hook = [](const VfsDirectoryGrant& grant) {
                WkiChannelIdentity identity{};
                uint64_t token = 0;
                s_vfs_lock.lock();
                if (auto* row = find_directory_grant_locked(grant); row != nullptr) {
                    identity = row->channel_identity;
                    token = row->directory_pin_token;
                }
                s_vfs_lock.unlock();
                static_cast<void>(cancel_directory_pin(identity, token));
            };
            int const CANCELLED = directory_grant_owner(worker, caller, destination, import, imported, name);
            directory_grant_adoption_test_hook = nullptr;
            if (CANCELLED != -ENOTCONN || imported.directory.fd != -1 || name[0] != '!') return fail(__LINE__);
            s_vfs_lock.lock();
            bool const NO_PARTIAL = find_directory_pin_locked(destination.channel_identity, 3) == nullptr;
            s_vfs_lock.unlock();
            if (!NO_PARTIAL) return fail(__LINE__);
            // Retirement can race after both bindings and the native view have
            // been retained. Neither endpoint may publish a new adoption then.
            // Restore the fixture binding afterwards to prove denial did not
            // consume the grant or leave an invisible destination reservation.
            static std::atomic<bool>* retiring_endpoint = nullptr;
            for (auto* endpoint : {&source, &destination}) {
                if (directory_grant_owner(worker, caller, source, issue, prepared, name) != 0) return fail(__LINE__);
                import.grant = prepared.grant;
                imported = {};
                auto const BEFORE = imported;
                name.fill('!');
                auto const NAME_BEFORE = name;
                auto const SOURCE_REFS = source.refs.load();
                auto const DESTINATION_REFS = destination.refs.load();
                s_vfs_lock.lock();
                find_directory_grant_locked(prepared.grant)->last_activity_us = 1;
                size_t const ROWS_BEFORE = g_remote_fds.size();
                s_vfs_lock.unlock();
                retiring_endpoint = &endpoint->retiring;
                directory_grant_adoption_test_hook = [](const VfsDirectoryGrant&) { retiring_endpoint->store(true); };
                int const RETIRED = directory_grant_owner(worker, caller, destination, import, imported, name);
                directory_grant_adoption_test_hook = nullptr;
                retiring_endpoint->store(false);
                retiring_endpoint = nullptr;
                s_vfs_lock.lock();
                auto* unchanged_grant = find_directory_grant_locked(prepared.grant);
                bool const UNPUBLISHED = unchanged_grant != nullptr && !unchanged_grant->directory_grant_claimed &&
                                         unchanged_grant->last_activity_us == 1 && g_remote_fds.size() == ROWS_BEFORE &&
                                         find_directory_pin_locked(destination.channel_identity, import.pin_token) == nullptr;
                s_vfs_lock.unlock();
                if (RETIRED != -ENOTCONN || std::memcmp(&imported, &BEFORE, sizeof(imported)) != 0 || name != NAME_BEFORE ||
                    source.refs.load() != SOURCE_REFS || destination.refs.load() != DESTINATION_REFS || !UNPUBLISHED)
                    return fail(__LINE__);
                if (directory_grant_owner(worker, caller, destination, import, imported, name) != 0 ||
                    cancel_directory_pin(destination.channel_identity, import.pin_token) != 0 ||
                    cancel_directory_pin(source.channel_identity, issue.pin_token) != 0)
                    return fail(__LINE__);
            }
            // Bounded owner registry and writable positive control.
            issue.policy = 0;
            issue.target_node = g_wki.my_node_id;
            issue.target_boot_epoch = g_wki.local_boot_epoch;
            for (uint64_t token = 2; token < 2 + VFS_DIRECTORY_GRANTS_PER_CONSUMER; ++token) {
                issue.pin_token = token;
                if (directory_grant_owner(worker, caller, source, issue, prepared, name) != 0) return fail(__LINE__);
            }
            issue.pin_token = 130;
            VfsDirectoryGrantResponse overflow{};
            if (directory_grant_owner(worker, caller, source, issue, overflow, name) != -EMFILE || overflow.directory.fd != -1 ||
                wki_remote_vfs_adopt_directory_grant_native(caller, context, prepared.grant, adopted) != 0 || adopted.policy().read_only ||
                adopted.policy().nosuid)
                return fail(__LINE__);
            vfs::File* writable = nullptr;
            if (vfs::vfs_open_directory_view_as(worker, caller, adopted, "new", 1 | vfs::O_CREAT, 0600, &writable) != 0)
                return fail(__LINE__);
            vfs::vfs_put_file(writable);
            adopted = {};
            if (worker.credentials() != caller) return fail(__LINE__);
        }
        return true;
    });
}

auto wki_remote_vfs_selftest_directory_reference_lifetime() -> bool {
    auto fail = [](int line) {
        ker::mod::dbg::logger<"ktest">::error("directory reference invariant failed at remote_vfs.cpp:%d", line);
        return false;
    };
    ker::vfs::File client{};
    RemoteFileContext client_context{};
    client.private_data = &client_context;
    client.is_directory = true;
    client.open_flags = ker::vfs::FILE_ACCESS_SEARCH_ONLY;
    std::array<char, 8> untouched{};
    untouched.fill('!');
    ker::vfs::DirEntry no_entry{};
    if (remote_vfs_read(&client, untouched.data(), untouched.size(), 0) != -EBADF || untouched[0] != '!' ||
        remote_vfs_write(&client, "bad", 3, 0) != -EBADF || remote_vfs_readdir(&client, &no_entry, 0) != -EBADF ||
        remote_vfs_truncate(&client, 0) != -EBADF)
        return fail(__LINE__);
    namespace vfs = ker::vfs;
    namespace security = ker::mod::security;
    constexpr auto BASE = "/tmp/ktest_remote_directory_reference";
    constexpr auto EXPORT = "/tmp/ktest_remote_directory_reference/root";
    constexpr auto RENAMED = "/tmp/ktest_remote_directory_reference/renamed";
    constexpr auto DIR = "/tmp/ktest_remote_directory_reference/root/dir";
    constexpr auto LINK = "/tmp/ktest_remote_directory_reference/root/link";
    constexpr auto INNER_LINK = "/tmp/ktest_remote_directory_reference/root/dir/link";
    constexpr auto MOVED = "/tmp/ktest_remote_directory_reference/root/moved";
    constexpr auto RENAMED_DIR = "/tmp/ktest_remote_directory_reference/renamed/dir";
    constexpr auto RENAMED_CWD = "/tmp/ktest_remote_directory_reference/renamed/moved";
    WkiChannel channel{};
    WkiChannelIdentity const IDENTITY{&channel, 2, 37, 4};
    WkiChannelIdentity stale = IDENTITY;
    ++stale.generation;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto caller = ROOT;
    if (caller.prepare({.change = security::Change::SET_UID, .real = 1003}, caller) != 0) return fail(__LINE__);
    ker::mod::sched::task::Task worker{};
    std::strcpy(worker.root.data(), "/unrelated-worker-root");
    std::strcpy(worker.cwd.data(), "/unrelated-worker-cwd");
    if (ker::mod::sched::resource::initialize_credentials(worker, caller) != 0) return fail(__LINE__);
    for (bool disk : {false, true}) {
        struct Cleanup {
            WkiChannelIdentity identity;
            bool mounted{};
            ~Cleanup() {
                for (uint64_t token : {1U, 2U, 3U}) static_cast<void>(cancel_directory_pin(identity, token));
                for (auto path : {LINK, INNER_LINK}) static_cast<void>(vfs::vfs_unlink(path));
                for (auto path : {DIR, MOVED, RENAMED_DIR, RENAMED_CWD, EXPORT, RENAMED}) static_cast<void>(vfs::vfs_rmdir(path));
                if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
                static_cast<void>(vfs::vfs_rmdir(BASE));
            }
        } cleanup{IDENTITY};
        if (vfs::vfs_mkdir(BASE, 0755) != 0) return fail(__LINE__);
        if (disk) {
            auto* device = ker::dev::block_device_find_by_name("sdb2");
            if (device == nullptr || vfs::mount_filesystem(BASE, "xfs", device) != 0) return fail(__LINE__);
            cleanup.mounted = true;
        }
        if (vfs::vfs_mkdir(EXPORT, 0755) != 0 || vfs::vfs_mkdir(DIR, 0711) != 0) return fail(__LINE__);
        // Mapped PIN must agree with mapped OPEN/STAT on absolute aliases.
        // Requests use a deliberately unrelated worker namespace and retain
        // response/name sentinels on every denied reservation.
        struct AliasCase {
            const char* visible;
            const char* target;
            int expected;
        };
        for (auto const& test : {AliasCase{"/shared", "/shared/dir", 0}, AliasCase{"//shared///", "/shared/dir", 0},
                                 AliasCase{"/", "/dir", 0}, AliasCase{"/shared", "./dir", 0},
                                 AliasCase{"/shared", "/shared/../shared/dir", -EPERM}, AliasCase{"/shared", "/shared_other/dir", -EPERM},
                                 AliasCase{"/shared", "/dir", -EPERM}, AliasCase{"/shared/..", "/dir", -EINVAL},
                                 AliasCase{"/shared/.", "/shared/dir", -EINVAL}, AliasCase{"named-export", DIR, 0}}) {
            if (vfs::vfs_symlink(test.target, LINK) != 0) return fail(__LINE__);
            for (auto const& credentials : {ROOT, caller}) {
                VfsDirectoryRequest pin{.token = 3, .path_length = 4};
                VfsDirectoryResponse output{.fd = 91, .path_length = 17, .token = 71};
                auto const BEFORE = output;
                std::array<char, 512> text{};
                text.fill('!');
                auto const TEXT = text;
                int const STATUS = directory_reference_owner(worker, credentials, IDENTITY, EXPORT, test.visible, pin,
                                                             reinterpret_cast<const uint8_t*>("link"), true, output, text);
                if (STATUS != test.expected) return fail(__LINE__);
                if (STATUS == 0) {
                    if (std::strcmp(text.data(), "/dir") != 0 || cancel_directory_pin(IDENTITY, 3) != 0) return fail(__LINE__);
                } else if (std::memcmp(&output, &BEFORE, sizeof(output)) != 0 || text != TEXT) {
                    return fail(__LINE__);
                }
            }
            if (vfs::vfs_unlink(LINK) != 0) return fail(__LINE__);
        }
        // A derived view narrows the root to dir. /shared must not be
        // reinterpreted as that directory, while /shared/dir denotes it exactly.
        {
            vfs::File* native = nullptr;
            if (vfs::vfs_pin_directory_resolved_beneath_as(worker, ROOT, EXPORT, DIR, &native) != 0) return fail(__LINE__);
            std::unique_ptr<vfs::File, DirectoryGrantFileRelease> native_owner(native);
            vfs::DirectoryViewRef narrow;
            if (vfs::vfs_prepare_directory_view(ROOT, native, native, {.read_only = true, .nosuid = true}, narrow) != 0)
                return fail(__LINE__);
            vfs::File* pinned = nullptr;
            if (vfs::vfs_pin_directory_view_as(worker, ROOT, narrow, ".", &pinned) != 0) return fail(__LINE__);
            std::unique_ptr<vfs::File, DirectoryGrantFileRelease> pinned_owner(pinned);
            RemoteFdReservation source(IDENTITY);
            int const SOURCE_FD = source.result();
            if (SOURCE_FD < 0 || !source.publish(pinned, 0, 0, 2)) return fail(__LINE__);
            static_cast<void>(pinned_owner.release());
            for (bool inside : {false, true}) {
                if (vfs::vfs_symlink(inside ? "/shared/dir" : "/shared", INNER_LINK) != 0) return fail(__LINE__);
                for (auto const& credentials : {ROOT, caller}) {
                    VfsDirectoryRequest pin{.directory_fd = SOURCE_FD, .token = 3, .path_length = 4};
                    VfsDirectoryResponse output{.fd = 91, .path_length = 17, .token = 71};
                    auto const BEFORE = output;
                    std::array<char, 512> text{};
                    text.fill('!');
                    auto const TEXT = text;
                    int const STATUS = directory_reference_owner(worker, credentials, IDENTITY, EXPORT, "/shared", pin,
                                                                 reinterpret_cast<const uint8_t*>("link"), true, output, text);
                    if (STATUS != (inside ? 0 : -EPERM)) return fail(__LINE__);
                    if (inside) {
                        if (std::strcmp(text.data(), "/") != 0) return fail(__LINE__);
                        s_vfs_lock.lock();
                        auto* row = find_directory_pin_locked(IDENTITY, 3);
                        bool const POLICY =
                            row != nullptr && row->file->directory_view.policy().read_only && row->file->directory_view.policy().nosuid;
                        s_vfs_lock.unlock();
                        if (!POLICY || cancel_directory_pin(IDENTITY, 3) != 0) return fail(__LINE__);
                    } else if (std::memcmp(&output, &BEFORE, sizeof(output)) != 0 || text != TEXT) {
                        return fail(__LINE__);
                    }
                }
                if (vfs::vfs_unlink(INNER_LINK) != 0) return fail(__LINE__);
            }
            if (cancel_directory_pin(IDENTITY, 2) != 0) return fail(__LINE__);
        }
        VfsDirectoryRequest request{.token = 1, .path_length = 3};
        VfsDirectoryResponse first{}, second{};
        std::array<char, 512> name{};
        auto invoke = [&](const security::Snapshot& credentials, const WkiChannelIdentity& identity, const uint8_t* path,
                          VfsDirectoryResponse& output) {
            return directory_reference_owner(worker, credentials, identity, EXPORT, EXPORT, request, path, true, output, name);
        };
        constexpr std::array<uint8_t, 3> PATH{'d', 'i', 'r'};
        // An empty wire name selects the export root; the native walker
        // requires a nonempty dot path for that same object.
        request = {.token = 3, .path_length = 0};
        if (invoke(caller, IDENTITY, nullptr, second) != 0 || std::strcmp(name.data(), "/") != 0) return fail(__LINE__);
        if (cancel_directory_pin(IDENTITY, 3) != 0) return fail(__LINE__);
        second = {};
        request = {.token = 1, .path_length = 3};
        // PIN can also start from an ordinary owner OPEN directory. The
        // registry token here only gives the fixture deterministic cleanup.
        {
            vfs::File* opened = nullptr;
            if (vfs::vfs_open_file_resolved_beneath_as(worker, ROOT, EXPORT, DIR, vfs::O_DIRECTORY, 0, &opened) != 0) return fail(__LINE__);
            auto release_opened = [](vfs::File* file) { vfs::vfs_put_file(file); };
            std::unique_ptr<vfs::File, decltype(release_opened)> opened_owner(opened, release_opened);
            if (opened->directory_view) return fail(__LINE__);
            RemoteFdReservation ordinary(IDENTITY);
            int const ORDINARY_FD = ordinary.result();
            if (ORDINARY_FD < 0 || !ordinary.publish(opened, 0, 0, 3)) return fail(__LINE__);
            static_cast<void>(opened_owner.release());
            if (vfs::vfs_chmod(EXPORT, 0000) != 0) return fail(__LINE__);
            request = {.directory_fd = ORDINARY_FD, .token = 2, .path_length = 1};
            constexpr std::array<uint8_t, 1> DOT_PATH{'.'};
            if (invoke(caller, IDENTITY, DOT_PATH.data(), second) != 0 || std::strcmp(name.data(), "/dir") != 0) return fail(__LINE__);
            if (vfs::vfs_chmod(EXPORT, 0755) != 0) return fail(__LINE__);
            if (cancel_directory_pin(IDENTITY, 3) != 0) return fail(__LINE__);
            request = {.operation = VfsDirectoryOperation::PATH, .directory_fd = second.fd, .token = 2};
            if (invoke(caller, IDENTITY, nullptr, second) != 0 || std::strcmp(name.data(), "/dir") != 0) return fail(__LINE__);
            if (cancel_directory_pin(IDENTITY, 2) != 0) return fail(__LINE__);
            second = {};
            request = {.token = 1, .path_length = 3};
        }
        if (invoke(caller, IDENTITY, PATH.data(), first) != 0 || std::strcmp(name.data(), "/dir") != 0) return fail(__LINE__);
        if (invoke(caller, IDENTITY, PATH.data(), second) != -EALREADY || second.fd != -1) return fail(__LINE__);
        if (cancel_exec_view(IDENTITY, 1) != 0 || cancel_directory_pin(stale, 1) != 0) return fail(__LINE__);
        request = {.operation = VfsDirectoryOperation::PATH, .directory_fd = first.fd, .token = 1};
        if (invoke(caller, stale, nullptr, second) != -EBADF) return fail(__LINE__);
        request.token = 2;
        if (invoke(caller, IDENTITY, nullptr, second) != -EBADF) return fail(__LINE__);
        request.token = 1;
        if (invoke(caller, IDENTITY, nullptr, second) != 0 || second.fd != first.fd) return fail(__LINE__);
        vfs::File* retained = nullptr;
        s_vfs_lock.lock();
        if (auto* row = find_directory_pin_locked(IDENTITY, 1); row != nullptr) {
            retained = row->file;
            vfs::vfs_retain_file(retained);
        }
        s_vfs_lock.unlock();
        if (retained == nullptr) return fail(__LINE__);
        auto release = [](vfs::File* file) { vfs::vfs_put_file(file); };
        std::unique_ptr<vfs::File, decltype(release)> hold(retained, release);
        auto* mount = retained->mount_owner;
        auto const BUSY = mount->open_files.load(std::memory_order_acquire);
        std::array<char, 8> bytes{};
        bytes.fill('!');
        vfs::DirEntry entry{};
        if (!retained->directory_view || retained->directory_view.root() == retained || retained->directory_view.directory() == retained ||
            retained->directory_view.root()->directory_view || retained->directory_view.directory()->directory_view ||
            !retained->is_directory || retained->vfs_path != nullptr || (retained->open_flags & 3U) != vfs::FILE_ACCESS_SEARCH_ONLY ||
            retained->fops->vfs_read(retained, bytes.data(), bytes.size(), 0) != -EBADF || bytes[0] != '!' ||
            retained->fops->vfs_write(retained, "bad", 3, 0) != -EBADF || retained->fops->vfs_readdir(retained, &entry, 0) != -EBADF)
            return fail(__LINE__);
        if (vfs::vfs_rename(DIR, MOVED) != 0 || vfs::vfs_mkdir(DIR, 0755) != 0) return fail(__LINE__);
        if (invoke(caller, IDENTITY, nullptr, second) != 0 || std::strcmp(name.data(), "/moved") != 0) return fail(__LINE__);
        if (vfs::vfs_chmod(MOVED, 0700) != 0) return fail(__LINE__);
        // Querying an admitted name grants no new search authority.
        if (invoke(caller, IDENTITY, nullptr, second) != 0) return fail(__LINE__);
        request = {.directory_fd = first.fd, .token = 2, .path_length = 1};
        constexpr std::array<uint8_t, 1> DOT{'.'};
        second = {};
        name.fill('!');
        s_vfs_lock.lock();
        find_directory_pin_locked(IDENTITY, 1)->last_activity_us = 1;
        s_vfs_lock.unlock();
        if (invoke(caller, IDENTITY, DOT.data(), second) != -EACCES || second.fd != -1 || name[0] != '!') return fail(__LINE__);
        s_vfs_lock.lock();
        bool const UNTOUCHED = find_directory_pin_locked(IDENTITY, 1)->last_activity_us == 1;
        s_vfs_lock.unlock();
        if (!UNTOUCHED) return fail(__LINE__);
        // Replace the historical export name. Derivation must retain the old
        // exact root, even when a different directory now occupies its name.
        if (vfs::vfs_rename(EXPORT, RENAMED) != 0 || vfs::vfs_mkdir(EXPORT, 0755) != 0) return fail(__LINE__);
        if (invoke(ROOT, IDENTITY, DOT.data(), second) != 0 || second.fd == first.fd || std::strcmp(name.data(), "/moved") != 0)
            return fail(__LINE__);
        {
            ker::vfs::DirectoryViewRef restricted;
            if (vfs::vfs_narrow_directory_view(retained->directory_view, ROOT, retained->directory_view.root(),
                                               retained->directory_view.directory(), {.read_only = true, .nosuid = true}, restricted) != 0)
                return fail(__LINE__);
            vfs::File* pinned = nullptr;
            if (vfs::vfs_pin_directory_view_as(worker, ROOT, restricted, ".", &pinned) != 0) return fail(__LINE__);
            std::unique_ptr<vfs::File, decltype(release)> scoped(pinned, release);
            restricted = {};
            if (!pinned->mount_policy.read_only || !pinned->mount_policy.nosuid || !pinned->directory_view.policy().read_only ||
                !pinned->directory_view.policy().nosuid || pinned->directory_view.root() != retained->directory_view.root() ||
                pinned->directory_view.directory() == pinned || pinned->directory_view.directory()->directory_view)
                return fail(__LINE__);
            vfs::File* denied = nullptr;
            if (vfs::vfs_open_directory_view_as(worker, ROOT, pinned->directory_view, "new", 1 | vfs::O_CREAT, 0600, &denied) != -EROFS ||
                denied != nullptr)
                return fail(__LINE__);
        }
        if (cancel_directory_pin(IDENTITY, 2) != 0 || cancel_directory_pin(IDENTITY, 2) != 0) return fail(__LINE__);
        if (mount->open_files.load(std::memory_order_acquire) != BUSY) return fail(__LINE__);
        if (cancel_directory_pin(IDENTITY, 1) != 0 || cancel_directory_pin(IDENTITY, 1) != 0) return fail(__LINE__);
        // Our independently retained File owns all three native identities after
        // registry retirement: outer search File, detached cwd and export root.
        if (mount->open_files.load(std::memory_order_acquire) != BUSY) return fail(__LINE__);
        hold.reset();
        if (mount->open_files.load(std::memory_order_acquire) != BUSY - 3) return fail(__LINE__);
        if (worker.credentials() != caller) return fail(__LINE__);
        if (vfs::vfs_rmdir(RENAMED_DIR) != 0 || vfs::vfs_rmdir(RENAMED_CWD) != 0 || vfs::vfs_rmdir(RENAMED) != 0 ||
            vfs::vfs_rmdir(EXPORT) != 0)
            return fail(__LINE__);
        if (cleanup.mounted) {
            if (vfs::unmount_filesystem(BASE) != 0) return fail(__LINE__);
            cleanup.mounted = false;
        }
        if (vfs::vfs_rmdir(BASE) != 0) return fail(__LINE__);
    }
    ProxyVfsState proxy{};
    proxy.active = true;
    proxy.owner_node = IDENTITY.peer_node_id;
    proxy.assigned_channel_ref = IDENTITY.channel;
    proxy.assigned_channel = IDENTITY.channel_id;
    proxy.assigned_channel_generation = IDENTITY.generation;
    WkiRemoteVfsExecDiag exec_before{}, dir_before{}, exec_after{}, dir_after{};
    wki_remote_vfs_exec_diag_snapshot(&exec_before);
    wki_remote_vfs_directory_diag_snapshot(&dir_before);
    uint16_t const WRONG = reserve_exec_cancellation(&proxy, 9, true, &stale);
    if (WRONG != VFS_EXEC_CANCEL_SLOT_NONE) {
        finish_exec_cancellation(WRONG, 0, false, false);
        return fail(__LINE__);
    }
    uint16_t const EXEC = reserve_exec_cancellation(&proxy, 9);
    uint16_t const DIRECTORY = reserve_exec_cancellation(&proxy, 9, true, &IDENTITY);
    if (EXEC == VFS_EXEC_CANCEL_SLOT_NONE || DIRECTORY == VFS_EXEC_CANCEL_SLOT_NONE) {
        if (EXEC != VFS_EXEC_CANCEL_SLOT_NONE) finish_exec_cancellation(EXEC, 0, false, false);
        if (DIRECTORY != VFS_EXEC_CANCEL_SLOT_NONE) finish_exec_cancellation(DIRECTORY, 0, false, false);
        return fail(__LINE__);
    }
    wki_remote_vfs_exec_diag_snapshot(&exec_after);
    wki_remote_vfs_directory_diag_snapshot(&dir_after);
    bool ok = exec_after.owned == exec_before.owned + 1 && dir_after.owned == dir_before.owned + 1 && proxy.open_file_refs.load() == 2;
    finish_exec_cancellation(DIRECTORY, -ETIMEDOUT, false, false);
    wki_remote_vfs_directory_diag_snapshot(&dir_after);
    ok = ok && dir_after.pending == dir_before.pending + 1 && dir_after.failed_attempts == dir_before.failed_attempts + 1;
    s_vfs_lock.lock();
    g_exec_cancellations.at(DIRECTORY).phase = ExecCancellationPhase::RETRYING;
    s_vfs_lock.unlock();
    finish_exec_cancellation(DIRECTORY, 0, false, false);
    finish_exec_cancellation(EXEC, 0, false, false);
    return ok && proxy.open_file_refs.load() == 0;
}

auto wki_remote_vfs_selftest_file_metadata_decoder() -> bool {
    namespace security = ker::mod::security;
    constexpr size_t OFFSET = security::credential_record::SIZE + sizeof(VfsPathIdentityHeader);
    std::array<uint8_t, OFFSET + sizeof(VfsFileUtimensRequest) + 1> bytes{};
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto const RECORD = security::credential_record::encode(ROOT);
    auto fail = [](int line) {
        ker::mod::dbg::logger<"ktest">::error("file metadata decoder failed at remote_vfs.cpp:%d", line);
        return false;
    };
    FileMetadataRequest parsed;
    auto equal = [](const FileMetadataRequest& a, const FileMetadataRequest& b) {
        return a.header.operation == b.header.operation && a.header.flags == b.header.flags && a.header.data_len == b.header.data_len &&
               a.header.reserved == b.header.reserved && a.credentials == b.credentials && a.fd == b.fd && a.mode == b.mode &&
               a.owner == b.owner && a.group == b.group && a.times[0].tv_sec == b.times[0].tv_sec &&
               a.times[0].tv_nsec == b.times[0].tv_nsec && a.times[1].tv_sec == b.times[1].tv_sec &&
               a.times[1].tv_nsec == b.times[1].tv_nsec;
    };
    for (auto operation : {VfsPathOperation::FCHMOD, VfsPathOperation::FCHOWN, VfsPathOperation::FUTIMENS}) {
        bytes.fill(0);
        std::memcpy(bytes.data(), RECORD.data(), RECORD.size());
        size_t const SIZE = operation == VfsPathOperation::FCHMOD   ? sizeof(VfsFileChmodRequest)
                            : operation == VfsPathOperation::FCHOWN ? sizeof(VfsFileChownRequest)
                                                                    : sizeof(VfsFileUtimensRequest);
        VfsPathIdentityHeader header{.operation = operation, .data_len = static_cast<uint16_t>(SIZE)};
        std::memcpy(bytes.data() + RECORD.size(), &header, sizeof(header));
        int32_t const FD = 17;
        std::memcpy(bytes.data() + OFFSET, &FD, sizeof(FD));
        if (operation == VfsPathOperation::FCHOWN) {
            VfsFileChownRequest const request{.fd = FD, .owner = UINT32_MAX, .group = 2002};
            std::memcpy(bytes.data() + OFFSET, &request, sizeof(request));
        }
        size_t const LENGTH = OFFSET + SIZE;
        if (decode_file_metadata_request(bytes.data(), LENGTH, parsed) != 0 || parsed.fd != FD || parsed.credentials != ROOT ||
            parsed.header.operation != operation)
            return fail(__LINE__);
        if (operation == VfsPathOperation::FCHOWN && (parsed.owner != UINT32_MAX || parsed.group != 2002)) return fail(__LINE__);
        auto const ORIGINAL = bytes;
        auto rejected = [&](size_t length) {
            auto const BEFORE = parsed;
            return decode_file_metadata_request(bytes.data(), length, parsed) != 0 && equal(parsed, BEFORE);
        };
        for (size_t length = 0; length < LENGTH; ++length)
            if (!rejected(length)) return fail(__LINE__);
        if (!rejected(LENGTH + 1)) return fail(__LINE__);
        for (int32_t fd : {INT32_MIN, -1, 0}) {
            std::memcpy(bytes.data() + OFFSET, &fd, sizeof(fd));
            if (!rejected(LENGTH)) return fail(__LINE__);
        }
        bytes = ORIGINAL;
        for (unsigned bit = 0; bit < 16; ++bit) {
            header.flags = uint16_t{1} << bit;
            std::memcpy(bytes.data() + RECORD.size(), &header, sizeof(header));
            if (!rejected(LENGTH)) return fail(__LINE__);
            header.flags = 0;
            header.reserved = uint16_t{1} << bit;
            std::memcpy(bytes.data() + RECORD.size(), &header, sizeof(header));
            if (!rejected(LENGTH)) return fail(__LINE__);
            header.reserved = 0;
        }
        bytes = ORIGINAL;
        bytes[0] ^= 0xFF;
        if (!rejected(LENGTH)) return fail(__LINE__);
        bytes = ORIGINAL;
        if (operation == VfsPathOperation::FUTIMENS) {
            constexpr std::array<int64_t, 4> VALID{0, 999999999, ker::vfs::TimestampUpdate::NOW, ker::vfs::TimestampUpdate::OMIT};
            for (auto atime : VALID)
                for (auto mtime : VALID) {
                    VfsFileUtimensRequest request{.fd = FD, .atime_sec = 123, .atime_nsec = atime, .mtime_sec = 456, .mtime_nsec = mtime};
                    std::memcpy(bytes.data() + OFFSET, &request, sizeof(request));
                    if (decode_file_metadata_request(bytes.data(), LENGTH, parsed) != 0 || parsed.times[0].tv_nsec != atime ||
                        parsed.times[1].tv_nsec != mtime || parsed.times[0].tv_sec != 123 || parsed.times[1].tv_sec != 456)
                        return fail(__LINE__);
                    request.reserved = 1;
                    std::memcpy(bytes.data() + OFFSET, &request, sizeof(request));
                    if (!rejected(LENGTH)) return fail(__LINE__);
                }
            for (int64_t invalid : {int64_t{-1}, int64_t{1000000000}, INT64_MIN, INT64_MAX}) {
                for (bool atime : {false, true}) {
                    VfsFileUtimensRequest request{.fd = FD, .atime_nsec = atime ? invalid : 0, .mtime_nsec = atime ? 0 : invalid};
                    std::memcpy(bytes.data() + OFFSET, &request, sizeof(request));
                    if (!rejected(LENGTH)) return fail(__LINE__);
                }
            }
        }
    }
    auto const BEFORE = parsed;
    return decode_file_metadata_request(nullptr, bytes.size(), parsed) == -EINVAL && equal(parsed, BEFORE);
}

auto wki_remote_vfs_selftest_completed_read_reply_retry() -> bool {
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = 3;
    channel.channel_id = 45;
    channel.generation = 7;
    WkiChannelIdentity const IDENTITY{&channel, 3, 45, 7};
    std::atomic<bool> retiring{false};
    constexpr std::array<uint8_t, 12> RESPONSE{1, 4, 0, 0, 4, 0, 68, 0, 'K', 'E', 'E', 'P'};
    auto response = RESPONSE;
    unsigned sends = 0;
    unsigned waits = 0;
    uint64_t now = 0;
    bool exact = true;
    constexpr std::array RESULTS{WKI_ERR_NO_ROUTE, WKI_ERR_NO_MEM, WKI_ERR_NO_CREDITS, WKI_OK};
    int result = retry_completed_vfs_reply(
        IDENTITY, &retiring, 10,
        [&](const WkiChannelIdentity& identity) {
            exact &= vfs_channel_identity_matches(IDENTITY, identity) && response == RESPONSE;
            return sends < RESULTS.size() ? RESULTS.at(sends++) : WKI_ERR_INVALID;
        },
        [&] {
            ++waits;
            ++now;
        },
        [&] { return now; });
    if (result != WKI_OK || sends != 4 || waits != 3 || !exact || response != RESPONSE) return false;

    // Terminal errors and accepted replies must not sleep or submit twice.
    for (int terminal : {WKI_OK, WKI_ERR_INVALID, WKI_ERR_NOT_FOUND, WKI_ERR_PEER_FENCED, WKI_ERR_TX_FAILED}) {
        sends = waits = 0;
        result = retry_completed_vfs_reply(
            IDENTITY, &retiring, 10,
            [&](const WkiChannelIdentity&) {
                ++sends;
                return terminal;
            },
            [&] { ++waits; }, [&] { return now; });
        if (result != terminal || sends != 1 || waits != 0) return false;
    }

    // Recheck retirement, deadline and generation after pacing, before send.
    for (unsigned mode = 0; mode < 5; ++mode) {
        sends = waits = 0;
        now = 0;
        retiring.store(mode == 3, std::memory_order_relaxed);
        channel.generation = IDENTITY.generation;
        result = retry_completed_vfs_reply(
            IDENTITY, mode == 4 ? nullptr : &retiring, 10,
            [&](const WkiChannelIdentity& identity) {
                exact &= vfs_channel_identity_matches(IDENTITY, identity) && response == RESPONSE;
                ++sends;
                return WKI_ERR_NO_ROUTE;
            },
            [&] {
                ++waits;
                if (mode == 0) retiring.store(true, std::memory_order_release);
                if (mode == 1) now = 10;
                if (mode == 2) ++channel.generation;
            },
            [&] { return now; });
        if (result != WKI_ERR_NO_ROUTE || sends != 1 || waits != (mode < 3 ? 1U : 0U) || !exact) return false;
    }
    return response == RESPONSE;
}

auto wki_remote_vfs_selftest_owner_version_admission() -> bool {
    namespace vfs = ker::vfs;
    struct State {
        unsigned closes = 0;
        bool unlocked = true;
    } state;
    vfs::FileOperations operations{};
    operations.vfs_close = [](vfs::File* file) -> int {
        auto& state = *static_cast<State*>(file->private_data);
        ++state.closes;
        if (!s_vfs_lock.try_lock())
            state.unlocked = false;
        else
            s_vfs_lock.unlock();
        return 0;
    };
    WkiChannel channel{};
    WkiChannelIdentity const IDENTITY{&channel, 0x7FFC, 74, 61};
    DevServerBinding binding;
    binding.resource_type = ResourceType::VFS;
    binding.resource_id = 71;
    binding.resource_incarnation = {37, 41};
    binding.consumer_node = IDENTITY.peer_node_id;
    binding.consumer_boot_epoch = 43;
    binding.channel_identity = IDENTITY;
    binding.vfs_export_publication_revision = 47;
    VfsOpenLifetimeRequest binding_response{};
    if (query_ordinary_open_binding(binding, IDENTITY, 53, binding_response) != 0) return false;
    auto release = [](vfs::File* file) { vfs::vfs_put_file(file); };
    std::unique_ptr<vfs::File, decltype(release)> file(new vfs::File, release);
    file->fs_type = vfs::FSType::SOCKET;
    file->fops = &operations;
    file->private_data = &state;
    file->refcount.store(1);
    struct Cleanup {
        WkiChannelIdentity identity;
        ~Cleanup() { wki_remote_vfs_cleanup_server_fds_for_channel(identity); }
    } cleanup{IDENTITY};
    RemoteFdReservation reservation(IDENTITY);
    int32_t const FD = reservation.result();
    constexpr uint64_t TOKEN = 59;
    if (FD < 0 || !reservation.publish(file.get(), 0, 0, 0, TOKEN, binding_response.binding_id)) return false;
    auto* published = file.release();
    s_vfs_lock.lock();
    find_remote_fd(IDENTITY, FD)->last_activity_us = 19;
    s_vfs_lock.unlock();
    VfsOwnerFdRequest const REQUEST{.fd = FD,
                                    .open_token = TOKEN,
                                    .binding_id = binding_response.binding_id,
                                    .owner_boot_epoch = binding_response.owner_boot_epoch,
                                    .query_id = 67};
    vfs::File sentinel;
    auto denied = [&](const WkiChannelIdentity& identity, const VfsOwnerFdRequest& request, int expected) {
        vfs::File* output = &sentinel;
        int const RESULT = retain_server_owner_file(binding, identity, request, output);
        s_vfs_lock.lock();
        auto* row = find_remote_fd(IDENTITY, FD);
        bool const UNCHANGED = row != nullptr && row->file == published && row->last_activity_us == 19;
        s_vfs_lock.unlock();
        return RESULT == expected && output == &sentinel && UNCHANGED && published->refcount.load() == 1 && state.closes == 0;
    };
    auto stale = IDENTITY;
    ++stale.generation;
    if (!denied(stale, REQUEST, -ENOTCONN)) return false;
    auto foreign = IDENTITY;
    ++foreign.peer_node_id;
    if (!denied(foreign, REQUEST, -ENOTCONN)) return false;
    WkiChannel successor{};
    stale = IDENTITY;
    stale.channel = &successor;
    if (!denied(stale, REQUEST, -ENOTCONN)) return false;
    for (unsigned field = 0; field < 5; ++field) {
        auto wrong = REQUEST;
        switch (field) {
            case 0:
                ++wrong.binding_id;
                break;
            case 1:
                ++wrong.owner_boot_epoch;
                break;
            case 2:
                ++wrong.open_token;
                break;
            case 3:
                ++wrong.fd;
                break;
            default:
                wrong.reserved = 1;
                break;
        }
        int const EXPECTED = field < 2 ? -ENOTCONN : field < 4 ? -EBADF : -EINVAL;
        if (!denied(IDENTITY, wrong, EXPECTED)) return false;
    }
    binding.retiring.store(true);
    if (!denied(IDENTITY, REQUEST, -ENOTCONN)) return false;
    binding.retiring.store(false);
    vfs::File* admitted = nullptr;
    if (retain_server_owner_file(binding, IDENTITY, REQUEST, admitted) != 0 || admitted != published) return false;
    std::unique_ptr<vfs::File, decltype(release)> hold(admitted, release);
    if (admitted->refcount.load() != 2) return false;
    wki_remote_vfs_cleanup_server_fds_for_channel(IDENTITY);
    if (state.closes != 0 || admitted->refcount.load() != 1) return false;
    vfs::File* output = &sentinel;
    if (retain_server_owner_file(binding, IDENTITY, REQUEST, output) != -EBADF || output != &sentinel) return false;
    hold.reset();
    return state.closes == 1 && state.unlocked;
}

auto wki_remote_vfs_selftest_owner_append() -> bool {
    namespace vfs = ker::vfs;
    constexpr const char* PATH = "/tmp/ktest_owner_append26";
    static_cast<void>(vfs::vfs_unlink(PATH));
    WkiChannel channel{};
    WkiChannelIdentity const IDENTITY{&channel, 0x7FF9, 75, 63};
    struct Cleanup {
        WkiChannelIdentity identity;
        const char* path;
        ~Cleanup() {
            wki_remote_vfs_cleanup_server_fds_for_channel(identity);
            static_cast<void>(vfs::vfs_unlink(path));
        }
    } cleanup{IDENTITY, PATH};
    DevServerBinding binding;
    binding.resource_type = ResourceType::VFS;
    binding.resource_id = 72;
    binding.resource_incarnation = {37, 41};
    binding.consumer_node = IDENTITY.peer_node_id;
    binding.consumer_boot_epoch = 43;
    binding.channel_identity = IDENTITY;
    binding.vfs_export_publication_revision = 47;
    VfsOpenLifetimeRequest bound{};
    if (query_ordinary_open_binding(binding, IDENTITY, 53, bound) != 0) return false;
    auto release = [](vfs::File* file) { vfs::vfs_put_file(file); };
    std::unique_ptr<vfs::File, decltype(release)> owner(vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_EXCL | 2, 0600), release);
    if (!owner || owner->fs_type != vfs::FSType::TMPFS) return false;
    size_t offset = 99;
    if (vfs::vfs_append_file_native(owner.get(), "L", 1, &offset) != 1 || offset != 0) return false;
    RemoteFdReservation reservation(IDENTITY);
    int32_t const FD = reservation.result();
    constexpr uint64_t TOKEN = 59;
    if (FD < 0 || !reservation.publish(owner.get(), 0, 0, 0, TOKEN, bound.binding_id)) return false;
    // A separate fixture pin survives exact remote row retirement.
    vfs::vfs_retain_file(owner.get());
    VfsAppendRequest request{
        .owner = {.fd = FD, .open_token = TOKEN, .binding_id = bound.binding_id, .owner_boot_epoch = bound.owner_boot_epoch, .query_id = 1},
        .data_len = 2};
    VfsAppendResponse reply{};
    auto invoke = [&](const VfsAppendRequest& operation, const char* bytes) {
        return perform_server_append(binding, IDENTITY, operation, reinterpret_cast<const uint8_t*>(bytes), reply);
    };
    if (invoke(request, "RR") != 0 || reply.status != 0 || reply.bytes != 2 || reply.offset != 1) return false;
    if (invoke(request, "RR") != 0 || reply.bytes != 2 || reply.offset != 1) return false;
    if (invoke(request, "XX") != -EPROTO) return false;
    std::unique_ptr<vfs::File, decltype(release)> local(vfs::vfs_open_file(PATH, 2, 0), release);
    if (!local || vfs::vfs_append_file_native(local.get(), "C", 1, &offset) != 1 || offset != 3) return false;
    if (invoke(request, "RR") != 0 || reply.offset != 1) return false;
    request.owner.query_id = 2;
    request.data_len = 1;
    if (invoke(request, "Z") != 0 || reply.bytes != 1 || reply.offset != 4) return false;
    auto old = request;
    old.owner.query_id = 1;
    old.data_len = 2;
    if (invoke(old, "RR") != -ESTALE) return false;
    auto stale = request;
    ++stale.owner.open_token;
    stale.owner.query_id = 3;
    if (invoke(stale, "X") != -EBADF) return false;
    request.owner.query_id = 3;
    request.data_len = 0;
    if (invoke(request, "") != 0 || reply.bytes != 0 || reply.offset != 5) return false;
    std::array<char, 5> contents{};
    if (owner->fops->vfs_read(owner.get(), contents.data(), contents.size(), 0) != 5 ||
        std::memcmp(contents.data(), "LRRCZ", contents.size()) != 0)
        return false;
    wki_remote_vfs_cleanup_server_fds_for_channel(IDENTITY);
    if (invoke(request, "") != -EBADF) return false;
    return owner->fops->vfs_read(owner.get(), contents.data(), contents.size(), 0) == 5;
}

auto wki_remote_vfs_selftest_owner_committed_io() -> bool {
    namespace vfs = ker::vfs;
    constexpr const char* PATH = "/tmp/ktest_owner_io26";
    static_cast<void>(vfs::vfs_unlink(PATH));
    WkiChannel channel{};
    WkiChannelIdentity const IDENTITY{&channel, 0x7FF8, 76, 64};
    struct Cleanup {
        WkiChannelIdentity identity;
        ~Cleanup() {
            wki_remote_vfs_cleanup_server_fds_for_channel(identity);
            static_cast<void>(ker::vfs::vfs_unlink("/tmp/ktest_owner_io26"));
        }
    } cleanup{IDENTITY};
    DevServerBinding binding;
    binding.resource_type = ResourceType::VFS;
    binding.resource_id = 72;
    binding.resource_incarnation = {37, 41};
    binding.consumer_node = IDENTITY.peer_node_id;
    binding.consumer_boot_epoch = 43;
    binding.channel_identity = IDENTITY;
    binding.vfs_export_publication_revision = 47;
    VfsOpenLifetimeRequest bound{};
    if (query_ordinary_open_binding(binding, IDENTITY, 53, bound) != 0) return false;
    auto release = [](vfs::File* file) { vfs::vfs_put_file(file); };
    std::unique_ptr<vfs::File, decltype(release)> owner(vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_EXCL | vfs::O_APPEND | 2, 0600),
                                                        release);
    if (!owner || owner->fs_type != vfs::FSType::TMPFS || vfs::vfs_pwrite_file(owner.get(), "abcd", 4, 0) != 4) return false;
    owner->pos = 17;
    RemoteFdReservation reservation(IDENTITY);
    int32_t const FD = reservation.result();
    constexpr uint64_t TOKEN = 59;
    if (FD < 0 || !reservation.publish(owner.get(), 0, 0, 0, TOKEN, bound.binding_id)) return false;
    vfs::vfs_retain_file(owner.get());
    VfsOwnerIoRequest request{
        .owner = {.fd = FD, .open_token = TOKEN, .binding_id = bound.binding_id, .owner_boot_epoch = bound.owner_boot_epoch, .query_id = 1},
        .offset = 1,
        .data_len = 2};
    VfsAppendResponse written{};
    auto write = [&](const VfsOwnerIoRequest& operation) {
        return perform_server_write(binding, IDENTITY, operation, reinterpret_cast<const uint8_t*>("XY"), written);
    };
    if (write(request) != 0 || written.status != 0 || written.bytes != 2 || written.offset != 1 || owner->pos != 17) return false;
    // A repeated committed write cannot overwrite a newer local mutation.
    if (vfs::vfs_pwrite_file(owner.get(), "Z", 1, 1) != 1 || write(request) != 0 || written.offset != 1) return false;
    auto different = request;
    different.offset = 0;
    if (write(different) != -EPROTO) return false;
    VfsAppendRequest append{.owner = request.owner, .data_len = request.data_len};
    if (perform_server_append(binding, IDENTITY, append, reinterpret_cast<const uint8_t*>("XY"), written) != -EPROTO) return false;
    std::array<uint8_t, 8> bytes{};
    VfsOwnerReadResponse read{};
    auto read_request = request;
    read_request.owner.query_id = 97;
    read_request.offset = 0;
    read_request.data_len = bytes.size();
    if (perform_server_read(binding, IDENTITY, read_request, bytes.data(), read) != 0 || read.query_id != 97 || read.bytes != 4 ||
        std::memcmp(bytes.data(), "aZYd", 4) != 0 || owner->pos != 17)
        return false;
    auto stale = read_request;
    ++stale.owner.open_token;
    auto const SAVED_BYTES = bytes;
    auto const SAVED_REPLY = read;
    if (perform_server_read(binding, IDENTITY, stale, bytes.data(), read) != -EBADF || bytes != SAVED_BYTES ||
        std::memcmp(&read, &SAVED_REPLY, sizeof(read)) != 0)
        return false;
    read_request.offset = 4;
    ++read_request.owner.query_id;
    if (perform_server_read(binding, IDENTITY, read_request, bytes.data(), read) != 0 || read.bytes != 0 || read.query_id != 98)
        return false;
    // A completed native error is a stable outcome, even when mount policy changes.
    request.owner.query_id = 2;
    owner->mount_policy.read_only = true;
    if (write(request) != 0 || written.status != -EROFS || written.bytes != 0 || written.offset != 0) return false;
    owner->mount_policy.read_only = false;
    if (write(request) != 0 || written.status != -EROFS) return false;
    request.owner.query_id = 3;
    if (write(request) != 0 || written.status != 0 || written.bytes != 2) return false;
    binding.retiring.store(true);
    if (write(request) != -ENOTCONN) return false;
    binding.retiring.store(false);
    wki_remote_vfs_cleanup_server_fds_for_channel(IDENTITY);
    if (write(request) != -EBADF || perform_server_read(binding, IDENTITY, read_request, bytes.data(), read) != -EBADF) return false;
    return vfs::vfs_pread_file(owner.get(), bytes.data(), bytes.size(), 0) == 4 && std::memcmp(bytes.data(), "aXYd", 4) == 0;
}

auto wki_remote_vfs_selftest_owner_advisory() -> bool {
    namespace vfs = ker::vfs;
    constexpr const char* PATH = "/tmp/ktest_owner_wire_locks26";
    static_cast<void>(vfs::vfs_unlink(PATH));
    WkiChannel channel{};
    WkiChannelIdentity const IDENTITY{&channel, 0x7FF6, 77, 65};
    struct Cleanup {
        WkiChannelIdentity identity;
        ~Cleanup() {
            wki_remote_vfs_cleanup_server_fds_for_channel(identity);
            static_cast<void>(ker::vfs::vfs_unlink("/tmp/ktest_owner_wire_locks26"));
        }
    } cleanup{IDENTITY};
    DevServerBinding binding;
    binding.resource_type = ResourceType::VFS;
    binding.resource_id = 73;
    binding.resource_incarnation = {37, 41};
    binding.consumer_node = IDENTITY.peer_node_id;
    binding.consumer_boot_epoch = 43;
    binding.channel_identity = IDENTITY;
    binding.vfs_export_publication_revision = 47;
    VfsOpenLifetimeRequest bound{};
    if (query_ordinary_open_binding(binding, IDENTITY, 53, bound) != 0) return false;
    auto release = [](vfs::File* file) { vfs::vfs_put_file(file); };
    std::unique_ptr<vfs::File, decltype(release)> owner(vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_EXCL | 2, 0600), release);
    if (!owner || owner->fs_type != vfs::FSType::TMPFS) return false;
    RemoteFdReservation reservation(IDENTITY);
    int32_t const FD = reservation.result();
    constexpr uint64_t TOKEN = 59;
    if (FD < 0 || !reservation.publish(owner.get(), 0, 0, 0, TOKEN, bound.binding_id)) return false;
    vfs::vfs_retain_file(owner.get());
    VfsOwnerLockRequest parent{
        .owner = {.fd = FD, .open_token = TOKEN, .binding_id = bound.binding_id, .owner_boot_epoch = bound.owner_boot_epoch, .query_id = 1},
        .process_generation = 61,
        .pid = 7,
        .type = 1};
    VfsOwnerScopeRequest registration{.owner = parent.owner,
                                      .process_generation = parent.process_generation,
                                      .pid = parent.pid,
                                      .operation = VfsOwnerScopeOperation::REGISTER};
    VfsOwnerScopeResponse parent_scope{};
    if (perform_server_advisory_scope(binding, IDENTITY, registration, parent_scope) != 0 || parent_scope.state != 1) return false;
    parent.scope_token = parent_scope.scope_token;
    auto child = parent;
    child.owner.query_id = 2;
    child.process_generation = 62;
    child.pid = 8;
    child.flags = WKI_VFS_LOCK_WAIT;
    registration.process_generation = child.process_generation;
    registration.pid = child.pid;
    VfsOwnerScopeResponse child_scope{};
    if (perform_server_advisory_scope(binding, IDENTITY, registration, child_scope) != 0 || child_scope.state != 1) return false;
    child.scope_token = child_scope.scope_token;
    auto query = parent;
    query.owner.query_id = 3;
    query.flags = WKI_VFS_LOCK_QUERY;
    auto unlock = parent;
    unlock.owner.query_id = 4;
    unlock.type = 2;
    VfsOwnerLockResponse reply{};
    auto request = [&](const VfsOwnerLockRequest& command) { return perform_server_advisory(binding, IDENTITY, command, reply); };
    if (request(parent) != 0 || reply.status != 0 || reply.operation != 1 || request(child) != 0 || reply.state != 1 ||
        reply.status != -EAGAIN || request(query) != 0 || reply.type != 2 || request(unlock) != 0 || reply.status != 0)
        return false;
    child.phase = 2;
    if (request(child) != 0 || reply.state != 2 || reply.status != 0) return false;
    query.owner.query_id = 5;
    if (request(query) != 0 || reply.type != 1 || reply.pid != 8) return false;
    child.phase = 3;
    if (request(child) != 0 || reply.status != 0 || reply.state != 2) return false;
    auto changed = child;
    ++changed.range_length;
    auto const SAVED = reply;
    if (request(changed) != -EPROTO || std::memcmp(&reply, &SAVED, sizeof(reply)) != 0) return false;
    child.phase = 4;
    if (request(child) != 0 || reply.status != 0 || request(child) != 0 || reply.status != 0) return false;
    query.owner.query_id = 3;
    if (request(query) != -ESTALE) return false;
    query.owner.query_id = 6;
    auto denied = [&](const WkiChannelIdentity& identity, const VfsOwnerLockRequest& command, int expected) {
        auto const BEFORE = reply;
        return perform_server_advisory(binding, identity, command, reply) == expected && std::memcmp(&reply, &BEFORE, sizeof(reply)) == 0 &&
               owner->refcount.load() == 2;
    };
    for (unsigned field = 0; field < 5; ++field) {
        auto wrong = query;
        switch (field) {
            case 0:
                ++wrong.owner.open_token;
                break;
            case 1:
                ++wrong.owner.binding_id;
                break;
            case 2:
                ++wrong.owner.owner_boot_epoch;
                break;
            case 3:
                ++wrong.owner.fd;
                break;
            default:
                wrong.reserved = 1;
                break;
        }
        int const EXPECTED = field == 1 || field == 2 ? -ENOTCONN : field == 4 ? -EINVAL : -EBADF;
        if (!denied(IDENTITY, wrong, EXPECTED)) return false;
    }
    auto stale = IDENTITY;
    ++stale.generation;
    if (!denied(stale, query, -ENOTCONN)) return false;
    binding.retiring.store(true);
    if (!denied(IDENTITY, query, -ENOTCONN)) return false;
    binding.retiring.store(false);
    // The same diagnostic PID cannot redefine an admitted process generation.
    auto impostor = query;
    impostor.pid = 8;
    if (request(impostor) != 0 || reply.status != -ESTALE) return false;
    unlock.owner.query_id = 7;
    unlock.process_generation = child.process_generation;
    unlock.scope_token = child.scope_token;
    unlock.pid = child.pid;
    if (request(unlock) != 0 || reply.status != 0) return false;
    parent.owner.query_id = 8;
    parent.kind = 2;
    parent.scope_token = 0;
    if (request(parent) != 0 || reply.status != 0) return false;
    query.owner.query_id = 9;
    query.process_generation = child.process_generation;
    query.scope_token = child.scope_token;
    query.pid = child.pid;
    if (request(query) != 0 || reply.type != 1 || reply.pid != -1) return false;
    parent.owner.query_id = 10;
    parent.kind = 3;
    if (request(parent) != 0 || reply.status != 0) return false;
    wki_remote_vfs_cleanup_server_fds_for_channel(IDENTITY);
    return request(parent) == -EBADF && owner->refcount.load() == 1 && owner->remote_advisory_closed.load();
}

auto wki_remote_vfs_selftest_owner_scope_controller() -> bool {
    namespace vfs = ker::vfs;
    constexpr const char* PATH = "/tmp/ktest_owner_scope_wire26";
    static_cast<void>(vfs::vfs_unlink(PATH));
    WkiChannel channel{};
    WkiChannelIdentity const IDENTITY{&channel, 0x7FF4, 78, 66};
    struct Cleanup {
        WkiChannelIdentity identity;
        ~Cleanup() {
            wki_remote_vfs_cleanup_server_fds_for_channel(identity);
            static_cast<void>(ker::vfs::vfs_unlink("/tmp/ktest_owner_scope_wire26"));
        }
    } cleanup{IDENTITY};
    DevServerBinding binding;
    binding.resource_type = ResourceType::VFS;
    binding.resource_id = 74;
    binding.resource_incarnation = {37, 41};
    binding.consumer_node = IDENTITY.peer_node_id;
    binding.consumer_boot_epoch = 43;
    binding.channel_identity = IDENTITY;
    binding.vfs_export_publication_revision = 47;
    VfsOpenLifetimeRequest bound{};
    if (query_ordinary_open_binding(binding, IDENTITY, 53, bound) != 0) return false;
    auto release = [](vfs::File* file) { vfs::vfs_put_file(file); };
    std::unique_ptr<vfs::File, decltype(release)> owner(vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_EXCL | 2, 0600), release);
    std::unique_ptr<vfs::File, decltype(release)> observer(vfs::vfs_open_file(PATH, 2, 0), release);
    if (!owner || !observer || owner->fs_type != vfs::FSType::TMPFS) return false;
    RemoteFdReservation reservation(IDENTITY);
    int32_t const FD = reservation.result();
    constexpr uint64_t TOKEN = 59;
    if (FD < 0 || !reservation.publish(owner.get(), 0, 0, 0, TOKEN, bound.binding_id)) return false;
    vfs::vfs_retain_file(owner.get());
    VfsOwnerScopeRequest identify{
        .owner = {
            .fd = FD, .open_token = TOKEN, .binding_id = bound.binding_id, .owner_boot_epoch = bound.owner_boot_epoch, .query_id = 1}};
    VfsOwnerScopeResponse response{};
    auto scope = [&](const VfsOwnerScopeRequest& command) { return perform_server_advisory_scope(binding, IDENTITY, command, response); };
    if (scope(identify) != 0 || response.state != 0 || response.scope_token != 0 || response.object == 0) return false;
    uint64_t const OBJECT = response.object;
    auto registration = identify;
    registration.process_generation = 61;
    registration.pid = 7;
    registration.operation = VfsOwnerScopeOperation::REGISTER;
    if (scope(registration) != 0 || response.state != 1 || response.object != OBJECT || response.scope_token == 0) return false;
    uint64_t const FIRST = response.scope_token;
    if (scope(registration) != 0 || response.scope_token != FIRST) return false;
    auto close = registration;
    close.owner.fd = 0;
    close.owner.open_token = 0;
    close.owner.query_id = 3;
    close.scope_token = FIRST;
    close.operation = VfsOwnerScopeOperation::CLOSE;
    if (scope(close) != 0 || response.state != 2 || response.scope_token != FIRST) return false;
    registration.owner.query_id = 2;
    if (scope(registration) != 0 || response.state != 1 || response.scope_token == FIRST) return false;
    uint64_t const SUCCESSOR = response.scope_token;
    VfsOwnerLockRequest lock{.owner = registration.owner, .process_generation = 61, .scope_token = SUCCESSOR, .pid = 7, .type = 1};
    VfsOwnerLockResponse granted{};
    if (perform_server_advisory(binding, IDENTITY, lock, granted) != 0 || granted.status != 0) return false;
    vfs::RemoteAdvisoryOwner const OBSERVER{.generation = 1007, .boot_epoch = 43, .pid = 99, .peer = IDENTITY.peer_node_id};
    auto conflicting = [&] {
        vfs::VfsFlockAbi query{.l_type = 1};
        return vfs::vfs_advisory_remote_try(observer.get(), OBSERVER, vfs::RemoteAdvisoryKind::PROCESS, true, query) == 0 &&
               query.l_type == 1 && query.l_pid == 7;
    };
    if (!conflicting() || scope(close) != 0 || response.state != 2 || !conflicting()) return false;
    close.operation = VfsOwnerScopeOperation::ACK;
    if (scope(close) != 0 || response.scope_token != FIRST || !conflicting()) return false;
    close.operation = VfsOwnerScopeOperation::CLOSE;
    if (scope(close) != -ESTALE || !conflicting()) return false;
    close.scope_token = SUCCESSOR;
    auto denied = [&](const VfsOwnerScopeRequest& wrong, int expected) {
        auto const SAVED = response;
        return scope(wrong) == expected && std::memcmp(&response, &SAVED, sizeof(response)) == 0 && conflicting();
    };
    for (unsigned field = 0; field < 6; ++field) {
        auto wrong = close;
        switch (field) {
            case 0:
                ++wrong.owner.binding_id;
                break;
            case 1:
                ++wrong.owner.owner_boot_epoch;
                break;
            case 2:
                ++wrong.process_generation;
                break;
            case 3:
                ++wrong.pid;
                break;
            case 4:
                wrong.scope_token = UINT64_MAX;
                break;
            default:
                wrong.owner.fd = FD;
                break;
        }
        int const EXPECTED = field < 2 ? -ENOTCONN : field < 4 ? -EPERM : field == 4 ? -ESTALE : -EINVAL;
        if (!denied(wrong, EXPECTED)) return false;
    }
    binding.retiring.store(true);
    if (!denied(close, -ENOTCONN)) return false;
    binding.retiring.store(false);
    wki_remote_vfs_cleanup_server_fds_for_channel(IDENTITY);
    return scope(close) == -ESTALE && scope(identify) == -EBADF && owner->remote_advisory_closed.load();
}

auto wki_remote_vfs_selftest_owner_io_outcomes() -> bool {
    namespace vfs = ker::vfs;
    struct State {
        WkiChannelIdentity identity{};
        unsigned mode = 0;
        unsigned calls = 0;
        unsigned closes = 0;
        bool unlocked = true;
        bool retained = true;
    };
    vfs::FileOperations operations{};
    operations.vfs_write = [](vfs::File* file, const void*, size_t, size_t) -> ssize_t {
        auto& state = *static_cast<State*>(file->private_data);
        if (!s_vfs_lock.try_lock()) {
            state.unlocked = false;
            return -EIO;
        }
        s_vfs_lock.unlock();
        ++state.calls;
        if (state.mode == 3) {
            wki_remote_vfs_cleanup_server_fds_for_channel(state.identity);
            state.retained &= state.closes == 0 && file->refcount.load() == 1;
        }
        return state.mode == 1 ? -ENOSPC : state.mode == 2 ? 0 : 2;
    };
    operations.vfs_read = [](vfs::File* file, void* data, size_t, size_t) -> ssize_t {
        auto& state = *static_cast<State*>(file->private_data);
        ++state.calls;
        wki_remote_vfs_cleanup_server_fds_for_channel(state.identity);
        state.retained &= state.closes == 0 && file->refcount.load() == 1;
        std::memcpy(data, "ok", 2);
        return 2;
    };
    operations.vfs_close = [](vfs::File* file) -> int {
        auto& state = *static_cast<State*>(file->private_data);
        ++state.closes;
        if (!s_vfs_lock.try_lock())
            state.unlocked = false;
        else
            s_vfs_lock.unlock();
        return 0;
    };
    for (unsigned mode = 0; mode < 5; ++mode) {
        WkiChannel channel{};
        WkiChannelIdentity const IDENTITY{&channel, 0x7FF7, 77, 65};
        State state{.identity = IDENTITY, .mode = mode};
        struct Cleanup {
            WkiChannelIdentity identity;
            ~Cleanup() { wki_remote_vfs_cleanup_server_fds_for_channel(identity); }
        } cleanup{IDENTITY};
        DevServerBinding binding;
        binding.resource_type = ResourceType::VFS;
        binding.resource_id = 72;
        binding.resource_incarnation = {37, 41};
        binding.consumer_node = IDENTITY.peer_node_id;
        binding.consumer_boot_epoch = 43;
        binding.channel_identity = IDENTITY;
        binding.vfs_export_publication_revision = 47;
        VfsOpenLifetimeRequest bound{};
        if (query_ordinary_open_binding(binding, IDENTITY, 53, bound) != 0) return false;
        auto release = [](vfs::File* file) { vfs::vfs_put_file(file); };
        std::unique_ptr<vfs::File, decltype(release)> file(new vfs::File, release);
        file->fs_type = vfs::FSType::SOCKET;
        file->fops = &operations;
        file->private_data = &state;
        file->open_flags = 2;
        file->pos = 17;
        file->refcount.store(1);
        RemoteFdReservation reservation(IDENTITY);
        int32_t const FD = reservation.result();
        if (FD < 0 || !reservation.publish(file.get(), 0, 0, 0, 59, bound.binding_id)) return false;
        file.release();
        VfsOwnerIoRequest const REQUEST{.owner = {.fd = FD,
                                                  .open_token = 59,
                                                  .binding_id = bound.binding_id,
                                                  .owner_boot_epoch = bound.owner_boot_epoch,
                                                  .query_id = 1},
                                        .offset = 3,
                                        .data_len = 5};
        if (mode == 4) {
            std::array<uint8_t, 5> bytes{};
            VfsOwnerReadResponse read{};
            if (perform_server_read(binding, IDENTITY, REQUEST, bytes.data(), read) != 0 || read.query_id != 1 || read.bytes != 2 ||
                std::memcmp(bytes.data(), "ok", 2) != 0 || state.closes != 1)
                return false;
        } else {
            VfsAppendResponse result{};
            if (perform_server_write(binding, IDENTITY, REQUEST, reinterpret_cast<const uint8_t*>("abcde"), result) != 0 ||
                result.status != (mode == 1 ? -ENOSPC : 0) || result.bytes != (mode == 1 || mode == 2 ? 0U : 2U) ||
                result.offset != (mode == 1 ? 0 : 3))
                return false;
            auto const SAVED = result;
            int const REPLAY = perform_server_write(binding, IDENTITY, REQUEST, reinterpret_cast<const uint8_t*>("abcde"), result);
            if (REPLAY != (mode == 3 ? -EBADF : 0) || std::memcmp(&result, &SAVED, sizeof(result)) != 0) return false;
            wki_remote_vfs_cleanup_server_fds_for_channel(IDENTITY);
        }
        if (state.calls != 1 || state.closes != 1 || !state.unlocked || !state.retained) return false;
    }
    return true;
}

auto wki_remote_vfs_selftest_owner_write_policy() -> bool {
    namespace vfs = ker::vfs;
    struct State {
        std::array<char, 8> bytes{};
        unsigned writes{};
        unsigned closes{};
        bool unlocked = true;
    };
    vfs::FileOperations operations{};
    operations.vfs_write = [](vfs::File* file, const void* data, size_t length, size_t offset) -> ssize_t {
        auto& state = *static_cast<State*>(file->private_data);
        if (!s_vfs_lock.try_lock()) {
            state.unlocked = false;
            return -EIO;
        }
        s_vfs_lock.unlock();
        ++state.writes;
        if (offset > state.bytes.size() || length > state.bytes.size() - offset) return -EFBIG;
        std::memcpy(state.bytes.data() + offset, data, length);
        return static_cast<ssize_t>(length);
    };
    operations.vfs_close = [](vfs::File* file) -> int {
        auto& state = *static_cast<State*>(file->private_data);
        ++state.closes;
        if (!s_vfs_lock.try_lock())
            state.unlocked = false;
        else
            s_vfs_lock.unlock();
        return 0;
    };
    auto release = [](vfs::File* file) { vfs::vfs_put_file(file); };
    auto fail = [](int line) {
        ker::mod::dbg::logger<"ktest">::error("owner write policy failed at remote_vfs.cpp:%d", line);
        return false;
    };
    for (unsigned mode = 0; mode <= vfs::FILE_ACCESS_SEARCH_ONLY; ++mode) {
        for (bool readonly : {false, true}) {
            State state;
            WkiChannel channel{};
            WkiChannelIdentity const IDENTITY{&channel, 0x7FFC, 73, 59};
            std::unique_ptr<vfs::File, decltype(release)> file(new vfs::File, release);
            file->fs_type = vfs::FSType::SOCKET;
            file->open_flags = static_cast<int>(mode);
            file->mount_policy.read_only = readonly;
            file->fops = &operations;
            file->private_data = &state;
            file->pos = 7;
            file->refcount.store(1, std::memory_order_relaxed);
            struct Cleanup {
                WkiChannelIdentity identity;
                ~Cleanup() { wki_remote_vfs_cleanup_server_fds_for_channel(identity); }
            } cleanup{IDENTITY};
            RemoteFdReservation reservation(IDENTITY);
            int32_t const FD = reservation.result();
            if (FD < 0 || !reservation.publish(file.get())) return fail(__LINE__);
            auto* published = file.release();
            s_vfs_lock.lock();
            find_remote_fd(IDENTITY, FD)->last_activity_us = 19;
            s_vfs_lock.unlock();
            vfs::File sentinel;
            auto denied = [&](const WkiChannelIdentity& identity, int32_t fd, int64_t offset, int expected, int invalid = -EBADF) {
                vfs::File* output = &sentinel;
                int const RESULT = retain_server_write_file(identity, fd, offset, output, invalid);
                s_vfs_lock.lock();
                auto* row = find_remote_fd(IDENTITY, FD);
                bool const UNCHANGED = row != nullptr && row->last_activity_us == 19 && row->file == published;
                s_vfs_lock.unlock();
                return RESULT == expected && output == &sentinel && UNCHANGED && published->refcount.load() == 1 && state.writes == 0 &&
                       state.closes == 0 && published->pos == 7;
            };
            auto stale = IDENTITY;
            ++stale.generation;
            auto foreign = IDENTITY;
            ++foreign.peer_node_id;
            if (!denied(stale, FD, 0, -EBADF) || !denied(foreign, FD, 0, -EBADF) || !denied(IDENTITY, -1, 0, -1, -1)) return fail(__LINE__);
            bool const WRITABLE_MODE = mode == 1 || mode == 2;
            if (!denied(IDENTITY, FD, -1, WRITABLE_MODE ? -EINVAL : -EBADF)) return fail(__LINE__);
            if (!WRITABLE_MODE || readonly) {
                if (!denied(IDENTITY, FD, 2, WRITABLE_MODE ? -EROFS : -EBADF)) return fail(__LINE__);
                wki_remote_vfs_cleanup_server_fds_for_channel(IDENTITY);
                if (state.closes != 1 || state.writes != 0 || !state.unlocked) return fail(__LINE__);
                continue;
            }
            vfs::File* admitted = nullptr;
            if (retain_server_write_file(IDENTITY, FD, 2, admitted) != 0 || admitted != published) return fail(__LINE__);
            std::unique_ptr<vfs::File, decltype(release)> hold(admitted, release);
            if (admitted->refcount.load() != 2) return fail(__LINE__);
            wki_remote_vfs_cleanup_server_fds_for_channel(IDENTITY);
            if (state.closes != 0 || admitted->refcount.load() != 1) return fail(__LINE__);
            constexpr std::array<char, 4> DATA{'k', 'e', 'e', 'p'};
            if (vfs::vfs_pwrite_file(admitted, DATA.data(), DATA.size(), 2) != static_cast<ssize_t>(DATA.size()) || admitted->pos != 7 ||
                state.writes != 1 || std::memcmp(state.bytes.data() + 2, DATA.data(), DATA.size()) != 0)
                return fail(__LINE__);
            hold.reset();
            if (state.closes != 1 || !state.unlocked) return fail(__LINE__);
        }
    }
    return true;
}

auto wki_remote_vfs_selftest_owner_file_lifetime() -> bool {
    namespace vfs = ker::vfs;
    struct State {
        WkiChannelIdentity identity;
        unsigned calls = 0;
        unsigned closes = 0;
        bool safe = true;
        void retire(vfs::File* file) {
            if (!s_vfs_lock.try_lock()) {
                safe = false;
                return;
            }
            s_vfs_lock.unlock();
            wki_remote_vfs_cleanup_server_fds_for_channel(identity);
            safe &= closes == 0 && file->refcount.load() == 1;
            ++calls;
        }
    };
    vfs::FileOperations operations{};
    operations.vfs_close = [](vfs::File* file) -> int {
        auto& state = *static_cast<State*>(file->private_data);
        ++state.closes;
        if (!s_vfs_lock.try_lock())
            state.safe = false;
        else
            s_vfs_lock.unlock();
        return 0;
    };
    operations.vfs_read = [](vfs::File* file, void* buffer, size_t length, size_t) -> ssize_t {
        auto& state = *static_cast<State*>(file->private_data);
        state.retire(file);
        std::memset(buffer, 'r', length);
        return static_cast<ssize_t>(length);
    };
    operations.vfs_readdir = [](vfs::File* file, vfs::DirEntry* entry, size_t index) -> int {
        auto& state = *static_cast<State*>(file->private_data);
        state.retire(file);
        entry->d_ino = 5 + index;
        return 0;
    };
    operations.vfs_lseek = [](vfs::File* file, off_t offset, int) -> off_t {
        auto& state = *static_cast<State*>(file->private_data);
        state.retire(file);
        file->pos = offset + 13;
        return file->pos;
    };
    auto release = [](vfs::File* file) { vfs::vfs_put_file(file); };
    auto fail = [](int line) {
        ker::mod::dbg::logger<"ktest">::error("owner File lifetime failed at remote_vfs.cpp:%d", line);
        return false;
    };
    constexpr std::array USES{ServerFileUse::READ, ServerFileUse::DIRECTORY, ServerFileUse::METADATA, ServerFileUse::SEEK,
                              ServerFileUse::SYNC};
    for (auto use : USES) {
        for (unsigned mode = 0; mode <= vfs::FILE_ACCESS_SEARCH_ONLY; ++mode) {
            for (bool readonly : {false, true}) {
                WkiChannel channel{};
                WkiChannelIdentity const IDENTITY{&channel, 0x7FFB, 77, 63};
                State state{.identity = IDENTITY};
                std::unique_ptr<vfs::File, decltype(release)> file(new vfs::File, release);
                file->fs_type = vfs::FSType::SOCKET;
                file->fops = &operations;
                file->open_flags = static_cast<int>(mode);
                file->mount_policy.read_only = readonly;
                file->private_data = &state;
                file->pos = 7;
                file->refcount.store(1);
                struct Cleanup {
                    WkiChannelIdentity identity;
                    ~Cleanup() { wki_remote_vfs_cleanup_server_fds_for_channel(identity); }
                } cleanup{IDENTITY};
                RemoteFdReservation reservation(IDENTITY);
                int32_t const FD = reservation.result();
                if (FD < 0 || !reservation.publish(file.get())) return fail(__LINE__);
                auto* published = file.release();
                s_vfs_lock.lock();
                find_remote_fd(IDENTITY, FD)->last_activity_us = 19;
                s_vfs_lock.unlock();
                vfs::File sentinel;
                auto denied = [&](const WkiChannelIdentity& identity, int32_t fd, int expected, int invalid = -EBADF) {
                    vfs::File* output = &sentinel;
                    int const RESULT = retain_server_file(identity, fd, use, output, invalid);
                    s_vfs_lock.lock();
                    auto* row = find_remote_fd(IDENTITY, FD);
                    bool const UNCHANGED = row != nullptr && row->file == published && row->last_activity_us == 19;
                    s_vfs_lock.unlock();
                    return RESULT == expected && output == &sentinel && UNCHANGED && published->refcount.load() == 1 && state.calls == 0 &&
                           state.closes == 0 && published->pos == 7;
                };
                auto stale = IDENTITY;
                ++stale.generation;
                auto foreign = IDENTITY;
                ++foreign.peer_node_id;
                WkiChannel other_channel{};
                auto other = IDENTITY;
                other.channel = &other_channel;
                if (!denied(stale, FD, -EBADF) || !denied(foreign, FD, -EBADF) || !denied(other, FD, -EBADF) ||
                    !denied(IDENTITY, -1, -1, -1))
                    return fail(__LINE__);
                bool const NEEDS_CALLBACK = use != ServerFileUse::METADATA && use != ServerFileUse::SYNC;
                if (NEEDS_CALLBACK) {
                    vfs::FileOperations absent{};
                    absent.vfs_close = operations.vfs_close;
                    published->fops = &absent;
                    bool const REJECTED = denied(IDENTITY, FD, -EBADF);
                    published->fops = &operations;
                    if (!REJECTED) return fail(__LINE__);
                }
                bool const NEEDS_READ = use == ServerFileUse::READ || use == ServerFileUse::DIRECTORY;
                if (NEEDS_READ && mode != 0 && mode != 2) {
                    if (!denied(IDENTITY, FD, -EBADF)) return fail(__LINE__);
                    wki_remote_vfs_cleanup_server_fds_for_channel(IDENTITY);
                    if (state.closes != 1 || !state.safe) return fail(__LINE__);
                    continue;
                }
                vfs::File* admitted = nullptr;
                if (retain_server_file(IDENTITY, FD, use, admitted) != 0 || admitted != published) return fail(__LINE__);
                std::unique_ptr<vfs::File, decltype(release)> hold(admitted, release);
                if (admitted->refcount.load() != 2) return fail(__LINE__);
                if (use == ServerFileUse::READ) {
                    std::array<char, 4> bytes{};
                    if (read_local_file_windowed(admitted, bytes.data(), bytes.size(), 0) != 4 || bytes != std::array{'r', 'r', 'r', 'r'})
                        return fail(__LINE__);
                } else if (use == ServerFileUse::DIRECTORY) {
                    for (size_t index = 0; index < 2; ++index) {
                        vfs::DirEntry entry{};
                        if (admitted->fops->vfs_readdir(admitted, &entry, index) != 0 || entry.d_ino != 5 + index) return fail(__LINE__);
                    }
                } else if (use == ServerFileUse::SEEK) {
                    off_t const OLD_POS = admitted->pos;
                    off_t const POS = admitted->fops->vfs_lseek(admitted, 2, 2);
                    admitted->pos = OLD_POS;
                    if (POS != 15) return fail(__LINE__);
                } else {
                    state.retire(admitted);
                    if (use == ServerFileUse::METADATA) {
                        vfs::Stat stat{};
                        if (vfs::vfs_fstat_file(admitted, &stat) != 0 || (stat.st_mode & vfs::S_IFMT) != vfs::S_IFSOCK)
                            return fail(__LINE__);
                    } else if (vfs::vfs_fsync_file(admitted) != 0) {
                        return fail(__LINE__);
                    }
                }
                if (!state.safe || state.closes != 0 || admitted->refcount.load() != 1 || admitted->pos != 7 ||
                    state.calls != (use == ServerFileUse::DIRECTORY ? 2U : 1U))
                    return fail(__LINE__);
                hold.reset();
                if (state.closes != 1 || !state.safe) return fail(__LINE__);
            }
        }
    }
    return true;
}

auto wki_remote_vfs_selftest_file_retirement() -> bool {
    namespace vfs = ker::vfs;
    struct CloseState {
        unsigned calls{};
        int result{};
    };
    vfs::FileOperations operations{};
    operations.vfs_close = [](vfs::File* file) -> int {
        auto& state = *static_cast<CloseState*>(file->private_data);
        ++state.calls;
        return state.result;
    };
    auto release = [](vfs::File* file) { vfs::vfs_put_file(file); };
    auto fail = [](int line) {
        ker::mod::dbg::logger<"ktest">::error("remote File retirement failed at remote_vfs.cpp:%d", line);
        return false;
    };
    if (vfs::vfs_put_file_result(nullptr) != 0 || release_server_file(nullptr) != 0) return fail(__LINE__);
    for (unsigned mode = 0; mode <= vfs::FILE_ACCESS_SEARCH_ONLY; ++mode) {
        for (int close_result : {0, -EIO}) {
            // With no admitted borrower, synchronous CLOSE must report the
            // exact backend result and run final destruction once.
            CloseState direct{.result = close_result};
            auto* direct_file = new vfs::File;
            direct_file->fs_type = vfs::FSType::SOCKET;
            direct_file->open_flags = static_cast<int>(mode);
            direct_file->fops = &operations;
            direct_file->private_data = &direct;
            direct_file->refcount.store(1, std::memory_order_relaxed);
            int const CLOSED = release_server_file(direct_file);
            if (CLOSED != close_result || direct.calls != 1) return fail(__LINE__);
            for (unsigned route = 0; route < 4; ++route) {
                WkiChannel channel{};
                WkiChannelIdentity const IDENTITY{&channel, 0x7FFE, 76, 55};
                auto stale = IDENTITY;
                ++stale.generation;
                CloseState state{.result = close_result};
                std::unique_ptr<vfs::File, decltype(release)> file(new vfs::File, release);
                file->fs_type = vfs::FSType::SOCKET;
                file->open_flags = static_cast<int>(mode);
                file->fops = &operations;
                file->private_data = &state;
                file->refcount.store(1, std::memory_order_relaxed);
                struct Cleanup {
                    WkiChannelIdentity identity;
                    ~Cleanup() { wki_remote_vfs_cleanup_server_fds_for_channel(identity); }
                } cleanup{IDENTITY};
                RemoteFdReservation reservation(IDENTITY);
                int32_t const FD = reservation.result();
                if (FD < 0 || !reservation.publish(file.get(), route == 3 ? 7 : 0)) return fail(__LINE__);
                // A transfer stage owns a separate File reference across RPCs.
                // Exercise both stage-final and explicit-final release paths.
                VfsXattrServerStage stage;
                stage.file = file.release();
                vfs::vfs_retain_file(stage.file);
                wki_remote_vfs_mark_server_fds_for_channel(stale);
                s_vfs_lock.lock();
                bool const LIVE = find_remote_fd(IDENTITY, FD) != nullptr;
                s_vfs_lock.unlock();
                if (!LIVE || state.calls != 0 || stage.file->refcount.load() != 2) return fail(__LINE__);
                if (route == 0) {
                    std::map<int32_t, RemoteVfsFd>::node_type retired;
                    s_vfs_lock.lock();
                    retired = g_remote_fds.extract(FD);
                    s_vfs_lock.unlock();
                    if (!retired || release_server_file(retired.mapped().file) != 0) return fail(__LINE__);
                } else if (route == 1) {
                    wki_remote_vfs_mark_server_fds_for_channel(IDENTITY);
                    wki_remote_vfs_process_pending_server_fd_cleanup();
                } else if (route == 2) {
                    wki_remote_vfs_cleanup_server_fds_for_channel(IDENTITY);
                } else {
                    if (cancel_exec_view(IDENTITY, 7) != 0 || cancel_exec_view(IDENTITY, 7) != 0) return fail(__LINE__);
                }
                s_vfs_lock.lock();
                bool const RETIRED = find_remote_fd(IDENTITY, FD) == nullptr;
                s_vfs_lock.unlock();
                if (!RETIRED || state.calls != 0 || stage.file->refcount.load() != 1 || stage.file->private_data != &state ||
                    stage.file->open_flags != static_cast<int>(mode))
                    return fail(__LINE__);
                auto* admitted = stage.file;
                stage.file = nullptr;
                if ((route & 1U) == 0) {
                    if (vfs::vfs_put_file_result(admitted) != close_result) return fail(__LINE__);
                } else {
                    VfsXattrServerStage retired_stage;
                    retired_stage.file = admitted;
                }
                if (state.calls != 1) return fail(__LINE__);
                wki_remote_vfs_cleanup_server_fds_for_channel(IDENTITY);
                if (state.calls != 1) return fail(__LINE__);
            }
        }
    }
    return true;
}

auto wki_remote_vfs_selftest_exec_view_lifetime() -> bool {
    namespace vfs = ker::vfs;
    WkiChannel channel{};
    WkiChannelIdentity identity{.channel = &channel, .peer_node_id = 0x7FFF, .channel_id = 77, .generation = 29};
    auto stale = identity;
    stale.generation++;
    constexpr const char* PATH = "/tmp/ktest_remote_exec_view";
    struct Cleanup {
        WkiChannelIdentity identity;
        int32_t fd = -1;
        vfs::File* source = nullptr;
        ~Cleanup() {
            static_cast<void>(cancel_exec_view(identity, 1));
            static_cast<void>(cancel_exec_view(identity, 2));
            std::map<int32_t, RemoteVfsFd>::node_type row;
            s_vfs_lock.lock();
            if (fd > 0) row = g_remote_fds.extract(fd);
            s_vfs_lock.unlock();
            if (row) vfs::vfs_put_file(row.mapped().file);
            vfs::vfs_put_file(source);
            static_cast<void>(vfs::vfs_unlink("/tmp/ktest_remote_exec_view"));
        }
    } cleanup{identity};
    cleanup.source = vfs::vfs_open_file(PATH, vfs::O_LOCAL | vfs::O_CREAT | vfs::O_EXCL | 2, 0755);
    if (cleanup.source == nullptr) return false;
    auto* source = cleanup.source;
    if (source->fops->vfs_write(source, "old", 3, 0) != 3) return false;
    RemoteFdReservation reservation(identity);
    cleanup.fd = reservation.result();
    vfs::vfs_retain_file(source);
    if (!reservation.publish(source)) {
        vfs::vfs_put_file(source);
        return false;
    }
    VfsExecViewRequest request{.source_fd = cleanup.fd, .token = 1};
    VfsExecViewResponse first{}, repeated{}, second{};
    bool ok = pin_exec_view(stale, request, first) == -EBADF;
    source->open_flags = 1;
    ok = pin_exec_view(identity, request, first) == -EBADF && ok;
    source->open_flags = vfs::O_LOCAL | 2;
    if (pin_exec_view(identity, request, first) != 0) return false;
    ok = wki_vfs_exec_view_response_valid(first, 1) && source->exec_read_view == nullptr && ok;
    ok = source->fops->vfs_write(source, "new", 3, 0) == -ETXTBSY && ok;
    ok = vfs::vfs_chmod(PATH, 0700) == 0 && ok;
    ok = pin_exec_view(identity, request, repeated) == 0 && repeated.fd == first.fd && repeated.metadata == first.metadata && ok;
    vfs::Stat metadata{};
    std::memcpy(&metadata, first.metadata.data(), sizeof(metadata));
    ok = (metadata.st_mode & 0777) == 0755 && ok;
    request.source_fd++;
    ok = pin_exec_view(identity, request, repeated) == -EINVAL && ok;
    request.source_fd = cleanup.fd;
    request.token = 2;
    if (pin_exec_view(identity, request, second) != 0) return false;
    ok = second.fd != first.fd && cancel_exec_view(stale, 1) == 0 && ok;
    ok = source->fops->vfs_write(source, "new", 3, 0) == -ETXTBSY && ok;
    ok = cancel_exec_view(identity, 1) == 0 && cancel_exec_view(identity, 1) == 0 && ok;
    ok = source->fops->vfs_write(source, "new", 3, 0) == -ETXTBSY && ok;
    ProxyVfsState proxy{};
    proxy.active = true;
    proxy.owner_node = identity.peer_node_id;
    proxy.assigned_channel_ref = identity.channel;
    proxy.assigned_channel = identity.channel_id;
    proxy.assigned_channel_generation = identity.generation;
    uint16_t const SLOT = reserve_exec_cancellation(&proxy, 2);
    if (SLOT == VFS_EXEC_CANCEL_SLOT_NONE) return false;
    // First cancellation cannot acquire an RPC slot: the real owner pin stays
    // held, and the independent proxy reference must survive that failure.
    proxy.op_pending.store(true, std::memory_order_release);
    ok = cancel_remote_exec_view(SLOT, true) != 0 && ok;
    s_vfs_lock.lock();
    auto& record = g_exec_cancellations.at(SLOT);
    record.retry_after_us = UINT64_MAX;
    ok = record.phase == ExecCancellationPhase::PENDING && record.token == 2 && ok;
    record.phase = ExecCancellationPhase::RETRYING;
    s_vfs_lock.unlock();
    ok = proxy.open_file_refs.load(std::memory_order_acquire) == 1 && source->fops->vfs_write(source, "new", 3, 0) == -ETXTBSY && ok;
    // An admitted reader can outlive registry cancellation. Its private view
    // must keep executable bytes and writer exclusion until the final put.
    auto release_view = [](vfs::File* file) { vfs::vfs_put_file(file); };
    std::unique_ptr<vfs::File, decltype(release_view)> admitted(nullptr, release_view);
    s_vfs_lock.lock();
    if (auto* row = find_remote_fd(identity, second.fd); row != nullptr && row->file != nullptr) {
        vfs::vfs_retain_file(row->file);
        admitted.reset(row->file);
    }
    s_vfs_lock.unlock();
    bool const ADMITTED = admitted != nullptr;
    // Model a lost response after actual owner cancellation. Retrying the
    // same token must release local ownership without disturbing source FD.
    ok = cancel_exec_view(record.identity, record.token) == 0 && ok;
    ok = ADMITTED && source->fops->vfs_write(source, "new", 3, 0) == -ETXTBSY && ok;
    if (admitted) {
        std::array<char, 3> bytes{};
        ok = admitted->fops->vfs_read(admitted.get(), bytes.data(), bytes.size(), 0) == 3 && bytes == std::array<char, 3>{'o', 'l', 'd'} &&
             ok;
    }
    admitted.reset();
    finish_exec_cancellation(SLOT, -ETIMEDOUT, false, false);
    s_vfs_lock.lock();
    record.retry_after_us = UINT64_MAX;
    ok = record.phase == ExecCancellationPhase::PENDING && ok;
    record.phase = ExecCancellationPhase::RETRYING;
    s_vfs_lock.unlock();
    ok = proxy.open_file_refs.load(std::memory_order_acquire) == 1 && source->fops->vfs_write(source, "new", 3, 0) == 3 && ok;
    int const REPEATED_CANCEL = cancel_exec_view(record.identity, record.token);
    finish_exec_cancellation(SLOT, REPEATED_CANCEL, false, false);
    ok = REPEATED_CANCEL == 0 && proxy.open_file_refs.load(std::memory_order_acquire) == 0 && ok;
    return ok;
}

auto wki_remote_vfs_selftest_directory_grant_preparation() -> bool {
    if (g_wki.initialized || g_pin_preparations != nullptr) return false;
    ProxyVfsState proxy{};
    WkiChannel channel{};
    proxy.active = true;
    proxy.owner_node = 0x7FFF;
    proxy.assigned_channel_ref = &channel;
    proxy.assigned_channel = 77;
    proxy.assigned_channel_generation = 39;
    proxy.open_file_refs.store(1, std::memory_order_release);
    constexpr uint64_t FIRST_PID = 0xD190A1;
    constexpr uint64_t OTHER_PID = 0xD190A2;
    struct Cleanup {
        ProxyVfsState* proxy;
        ~Cleanup() {
            PinPreparation::cleanup_for_task(FIRST_PID);
            PinPreparation::cleanup_for_task(OTHER_PID);
            for (size_t index = 0; index < g_exec_cancellations.size(); ++index) {
                s_vfs_lock.lock();
                bool const OWNED = g_exec_cancellations.at(index).proxy == proxy;
                s_vfs_lock.unlock();
                if (OWNED) finish_exec_cancellation(static_cast<uint16_t>(index), 0, false, false);
            }
        }
    } cleanup{&proxy};
    auto fail = [](int line) {
        ker::mod::dbg::logger<"ktest">::error("directory grant preparation invariant failed at remote_vfs.cpp:%d", line);
        return false;
    };
    size_t const FILES = g_pin_private_files_freed.load();
    size_t const CONTEXTS = g_pin_private_contexts_freed.load();
    {
        PinPreparation first(&proxy, PinPurpose::DIRECTORY, FIRST_PID);
        PinPreparation other(&proxy, PinPurpose::DIRECTORY, OTHER_PID);
        PinPreparation nested(&proxy, PinPurpose::DIRECTORY, FIRST_PID);
        if (first.status() != 0 || other.status() != 0 || nested.status() != 0 || proxy.open_file_refs.load() != 4) return fail(__LINE__);
        uint16_t const SLOT = reserve_exec_cancellation(&proxy, 901, true);
        uint16_t const NESTED = reserve_exec_cancellation(&proxy, 902, true);
        if (SLOT == VFS_EXEC_CANCEL_SLOT_NONE || NESTED == VFS_EXEC_CANCEL_SLOT_NONE) return fail(__LINE__);
        first.own_cancellation(SLOT, 901);
        nested.own_cancellation(NESTED, 902);
        // The real task cleanup retires proxy waiters before private ownership.
        // No network request is required to hand both tokens to the outbox.
        wki_remote_vfs_cleanup_for_task(FIRST_PID);
        if (first.file() != nullptr || first.context() != nullptr || nested.file() != nullptr || nested.context() != nullptr ||
            other.file() == nullptr || other.context() == nullptr || proxy.open_file_refs.load() != 4 ||
            g_pin_private_files_freed.load() != FILES + 2 || g_pin_private_contexts_freed.load() != CONTEXTS + 2)
            return fail(__LINE__);
        s_vfs_lock.lock();
        bool const QUEUED = g_exec_cancellations.at(SLOT).phase == ExecCancellationPhase::PENDING &&
                            g_exec_cancellations.at(SLOT).token == 901 && g_exec_cancellations.at(SLOT).proxy == &proxy &&
                            g_exec_cancellations.at(NESTED).phase == ExecCancellationPhase::PENDING &&
                            g_exec_cancellations.at(NESTED).token == 902;
        s_vfs_lock.unlock();
        if (!QUEUED) return fail(__LINE__);
        wki_remote_vfs_cleanup_for_task(FIRST_PID);
        first.cancel();
        nested.cancel();
        if (proxy.open_file_refs.load() != 4 || g_pin_private_files_freed.load() != FILES + 2) return fail(__LINE__);
        finish_exec_cancellation(SLOT, 0, false, false);
        finish_exec_cancellation(NESTED, 0, false, false);
        wki_remote_vfs_cleanup_for_task(OTHER_PID);
        if (other.file() != nullptr || proxy.open_file_refs.load() != 1) return fail(__LINE__);
    }
    {
        // Exit while the feature query is waiting, before any owner PIN exists.
        PinPreparation query(&proxy, PinPurpose::DIRECTORY, FIRST_PID);
        if (query.status() != 0) return fail(__LINE__);
        wki_remote_vfs_cleanup_for_task(FIRST_PID);
        if (query.file() != nullptr || query.context() != nullptr || proxy.open_file_refs.load() != 1) return fail(__LINE__);
    }
    {
        // Failed proxy acquisition still frees both private allocations.
        proxy.active = false;
        PinPreparation rejected(&proxy, PinPurpose::DIRECTORY, FIRST_PID);
        if (rejected.status() != -ENOTCONN || proxy.open_file_refs.load() != 1) return fail(__LINE__);
        proxy.active = true;
    }
    if (g_pin_private_files_freed.load() != FILES + 5 || g_pin_private_contexts_freed.load() != CONTEXTS + 5) return fail(__LINE__);
    {
        PinPreparation success(&proxy, PinPurpose::DIRECTORY, FIRST_PID);
        if (success.status() != 0) return fail(__LINE__);
        uint16_t const SLOT = reserve_exec_cancellation(&proxy, 903, true);
        if (SLOT == VFS_EXEC_CANCEL_SLOT_NONE) return fail(__LINE__);
        success.own_cancellation(SLOT, 903);
        auto* context = success.context();
        context->proxy = &proxy;
        context->remote_fd = 17;
        context->exec_cancel_slot = SLOT;
        context->directory_pin = true;
        auto* file = success.file();
        file->fops = &g_remote_vfs_fops;
        file->fs_type = ker::vfs::FSType::REMOTE;
        file->refcount = 1;
        file->open_flags = ker::vfs::O_DIRECTORY | ker::vfs::O_NO_CACHE | ker::vfs::FILE_ACCESS_SEARCH_ONLY;
        file->is_directory = true;
        std::unique_ptr<ker::vfs::File, DirectoryGrantFileRelease> published(success.publish());
        wki_remote_vfs_cleanup_for_task(FIRST_PID);
        if (!published || published->private_data != context || success.file() != nullptr || success.context() != nullptr ||
            proxy.open_file_refs.load() != 3 || g_exec_cancellations.at(SLOT).phase != ExecCancellationPhase::OWNED ||
            g_pin_private_files_freed.load() != FILES + 5)
            return fail(__LINE__);
        // Confirmed exact binding retirement permits the real File close path
        // without fabricating transport activity or releasing outbox ownership early.
        proxy.binding_retirement_confirmed = true;
        int const CLOSED = ker::vfs::vfs_put_file_result(published.release());
        // Retirement releases ownership without acknowledging a live peer RPC;
        // the established close errno remains ENOTCONN.
        if (CLOSED != -ENOTCONN || proxy.open_file_refs.load() != 1 || g_exec_cancellations.at(SLOT).phase != ExecCancellationPhase::FREE)
            return fail(__LINE__);
    }
    return g_pin_preparations == nullptr && proxy.open_file_refs.load() == 1;
}

auto wki_remote_vfs_selftest_scalar_open_preparation() -> bool {
    if (g_wki.initialized || g_pin_preparations != nullptr) return false;
    constexpr uint64_t PID = 0xD190D1;
    ProxyVfsState proxy{};
    proxy.active = true;
    proxy.open_file_refs.store(1, std::memory_order_release);
    struct FaultScope {
        ~FaultScope() {
            g_preparation_failure_pid = 0;
            g_preparation_failure_allocation = 0;
        }
    } fault_scope;
    g_preparation_failure_pid = PID;
    for (unsigned allocation : {1U, 2U, 3U}) {
        g_preparation_failure_allocation = allocation;
        size_t const FILES = g_pin_private_files_freed.load();
        size_t const CONTEXTS = g_pin_private_contexts_freed.load();
        {
            PinPreparation preparation(&proxy, PinPurpose::ORDINARY_OPEN, PID);
            if (allocation == 3 && preparation.allocate_request(512) != nullptr) return false;
            if (preparation.status() != -ENOMEM) return false;
        }
        if (proxy.open_file_refs.load() != 1 || g_pin_preparations != nullptr ||
            g_pin_private_files_freed.load() != FILES + (allocation != 1 ? 1U : 0U) ||
            g_pin_private_contexts_freed.load() != CONTEXTS + (allocation != 2 ? 1U : 0U))
            return false;
    }
    g_preparation_failure_allocation = 0;
    size_t const REQUESTS = g_pin_private_requests_freed.load();
    {
        PinPreparation abandoned(&proxy, PinPurpose::ORDINARY_OPEN, PID);
        PinPreparation other(&proxy, PinPurpose::ORDINARY_OPEN, PID + 1);
        if (abandoned.status() != 0 || other.status() != 0 || abandoned.allocate_request(512) == nullptr ||
            other.allocate_request(512) == nullptr || proxy.open_file_refs.load() != 3)
            return false;
        wki_remote_vfs_cleanup_for_task(PID);
        if (abandoned.status() != -ECANCELED || abandoned.file() != nullptr || abandoned.context() != nullptr || other.status() != 0 ||
            other.file() == nullptr || proxy.open_file_refs.load() != 2 || g_pin_private_requests_freed.load() != REQUESTS + 1)
            return false;
        abandoned.cancel();
        wki_remote_vfs_cleanup_for_task(PID);
        if (proxy.open_file_refs.load() != 2 || g_pin_private_requests_freed.load() != REQUESTS + 1) return false;
    }
    if (proxy.open_file_refs.load() != 1 || g_pin_private_requests_freed.load() != REQUESTS + 2) return false;
    {
        PinPreparation preparation(&proxy, PinPurpose::ORDINARY_OPEN, PID);
        if (preparation.status() != 0 || preparation.allocate_request(512) == nullptr) return false;
        auto* context = preparation.context();
        context->proxy = &proxy;
        auto* file = preparation.publish();
        bool const TRANSFERRED = file != nullptr && file->private_data == context && proxy.open_file_refs.load() == 2;
        preparation.cancel();
        bool const REQUEST_FREED = g_pin_private_requests_freed.load() == REQUESTS + 3 && proxy.open_file_refs.load() == 2;
        // No owner RPC in this storage test. Dispose the transferred objects
        // directly and release precisely the open reference they now own.
        delete context;
        delete file;
        release_vfs_proxy_open_ref(&proxy);
        if (!TRANSFERRED || !REQUEST_FREED) return false;
    }
    return g_pin_preparations == nullptr && proxy.open_file_refs.load() == 1;
}

auto wki_remote_vfs_selftest_open_lifetime_client() -> bool {
    if (g_wki.initialized || g_pin_preparations != nullptr || g_ordinary_closes != nullptr || g_shared_io_owners != nullptr) return false;
    for (auto const& record : g_open_cancellations)
        if (record.phase != ExecCancellationPhase::FREE) return false;
    constexpr uint64_t PID = 0xD190D3;
    ProxyVfsState proxy{};
    WkiChannel channel{};
    proxy.active = true;
    proxy.owner_node = 0x7FFF;
    proxy.assigned_channel_ref = &channel;
    proxy.assigned_channel = 77;
    proxy.assigned_channel_generation = 39;
    proxy.binding_peer_boot_epoch = 83;
    proxy.ordinary_open_binding_id = 991;
    proxy.ordinary_owner_boot_epoch = 83;
    proxy.open_file_refs = 1;
    struct Cleanup {
        ProxyVfsState* proxy;
        ~Cleanup() {
            for (size_t i = 0; i < g_open_cancellations.size(); ++i)
                if (g_open_cancellations.at(i).proxy == proxy) finish_ordinary_cancellation(static_cast<uint16_t>(i), 0, false, false);
        }
    } cleanup{&proxy};
    auto fail = [](int line) {
        ker::mod::dbg::logger<"ktest">::error("ordinary open client invariant failed at remote_vfs.cpp:%d", line);
        return false;
    };
    // Missing/mismatched owner binding is refused before taking any ownership.
    proxy.ordinary_open_binding_id = 0;
    if (reserve_ordinary_cancellation(&proxy) != VFS_EXEC_CANCEL_SLOT_NONE) return fail(__LINE__);
    proxy.ordinary_open_binding_id = 991;
    proxy.ordinary_owner_boot_epoch = 84;
    if (reserve_ordinary_cancellation(&proxy) != VFS_EXEC_CANCEL_SLOT_NONE) return fail(__LINE__);
    proxy.ordinary_owner_boot_epoch = 83;
    // Full outbox cannot consume the independent directory/exec pin namespace.
    for (size_t i = 0; i < g_open_cancellations.size(); ++i)
        if (reserve_ordinary_cancellation(&proxy) != i) return fail(__LINE__);
    if (reserve_ordinary_cancellation(&proxy) != VFS_EXEC_CANCEL_SLOT_NONE || proxy.open_file_refs.load() != 1025) return fail(__LINE__);
    for (size_t i = 0; i < g_open_cancellations.size(); ++i)
        if (cancel_remote_ordinary_open(static_cast<uint16_t>(i)) != 0) return fail(__LINE__);
    if (proxy.open_file_refs.load() != 1) return fail(__LINE__);
    proxy.ordinary_next_token = UINT64_MAX - 1;
    if (take_ordinary_open_token_locked(&proxy) != UINT64_MAX - 1 || take_ordinary_open_token_locked(&proxy) != UINT64_MAX ||
        take_ordinary_open_token_locked(&proxy) != 0 || take_ordinary_open_token_locked(&proxy) != 0)
        return fail(__LINE__);
    proxy.ordinary_next_token = 1;
    {
        PinPreparation abandoned(&proxy, PinPurpose::ORDINARY_OPEN, PID);
        PinPreparation other(&proxy, PinPurpose::ORDINARY_OPEN, PID + 1);
        if (abandoned.status() != 0 || other.status() != 0) return fail(__LINE__);
        uint16_t const SLOT = reserve_ordinary_cancellation(&proxy);
        if (SLOT == VFS_EXEC_CANCEL_SLOT_NONE) return fail(__LINE__);
        abandoned.own_ordinary_cancellation(SLOT);
        g_open_cancellations.at(SLOT).request.token = 17;
        wki_remote_vfs_cleanup_for_task(PID);
        if (abandoned.file() != nullptr || other.file() == nullptr || proxy.open_file_refs.load() != 3 ||
            g_open_cancellations.at(SLOT).phase != ExecCancellationPhase::PENDING)
            return fail(__LINE__);
        wki_remote_vfs_cleanup_for_task(PID);
        // Transport errors retain ownership; exact binding retirement releases it.
        finish_ordinary_cancellation(SLOT, -ENOTCONN, false, false);
        if (g_open_cancellations.at(SLOT).phase != ExecCancellationPhase::PENDING || g_open_cancellations.at(SLOT).request.token != 17 ||
            proxy.open_file_refs.load() != 3)
            return fail(__LINE__);
        proxy.binding_retirement_confirmed = true;
        if (cancel_remote_ordinary_open(SLOT) != 0 || proxy.open_file_refs.load() != 2) return fail(__LINE__);
        proxy.binding_retirement_confirmed = false;
    }
    if (proxy.open_file_refs.load() != 1) return fail(__LINE__);
    // Fatal shared-slot cleanup releases only this task, and the abandoned
    // destructor cannot clear a successor's newly acquired slot.
    ProxyVfsState other_proxy{};
    other_proxy.shared_io_in_use = true;
    {
        proxy.shared_io_in_use = true;
        SharedIoTaskOwner abandoned(&proxy, PID);
        SharedIoTaskOwner other(&other_proxy, PID + 1);
        wki_remote_vfs_cleanup_for_task(PID);
        if (proxy.shared_io_in_use.load() || !other_proxy.shared_io_in_use.load()) return fail(__LINE__);
        proxy.shared_io_in_use = true;
        wki_remote_vfs_cleanup_for_task(PID);
        if (!proxy.shared_io_in_use.load()) return fail(__LINE__);
    }
    if (!proxy.shared_io_in_use.load() || other_proxy.shared_io_in_use.load() || g_shared_io_owners != nullptr) return fail(__LINE__);
    proxy.shared_io_in_use = false;
    ker::vfs::MountPoint mount{};
    mount.refs = 7;
    auto make_file = [&](uint64_t token, int flags) -> ker::vfs::File* {
        PinPreparation preparation(&proxy, PinPurpose::ORDINARY_OPEN, PID);
        if (preparation.status() != 0) return nullptr;
        uint16_t const SLOT = reserve_ordinary_cancellation(&proxy);
        if (SLOT == VFS_EXEC_CANCEL_SLOT_NONE) return nullptr;
        preparation.own_ordinary_cancellation(SLOT);
        g_open_cancellations.at(SLOT).request.token = token;
        auto* context = preparation.context();
        context->proxy = &proxy;
        context->remote_fd = 17;
        context->read_cache = new ReadAheadCache{};
        context->write_buf = new WriteBehindBuffer{};
        context->open_prefetch_buf = new uint8_t[32]{};
        context->open_prefetch_len = 32;
        auto* file = preparation.file();
        file->fops = &g_remote_vfs_fops;
        file->fs_type = ker::vfs::FSType::REMOTE;
        file->refcount = 1;
        file->fd = -1;
        file->open_flags = ker::vfs::O_NO_CACHE | flags;
        auto* path = new char[320]{};
        path[0] = '/';
        for (size_t i = 1; i < 319; ++i) path[i] = 'p';
        file->vfs_path = path;
        file->vfs_path_len = 319;
        file->vfs_path_heap_allocated = true;
        mount.open_files.fetch_add(1);
        file->mount_owner = &mount;
        return preparation.publish();
    };
    size_t const FINISHED = g_abandoned_ordinary_closes_finished.load();
    {
        auto* file = make_file(18, 2);
        if (file == nullptr) return fail(__LINE__);
        uint16_t const SLOT = static_cast<RemoteFileContext*>(file->private_data)->ordinary_cancel_slot;
        file->refcount = 0;  // The actual VFS close prelude has exclusive final ownership.
        OrdinaryCloseOwner owner(file, PID);
        wki_remote_vfs_cleanup_for_task(PID + 1);
        if (owner.file() == nullptr) return fail(__LINE__);
        wki_remote_vfs_cleanup_for_task(PID);
        wki_remote_vfs_cleanup_for_task(PID);
        if (owner.file() != nullptr || mount.open_files.load() != 0 || mount.refs.load() != 7 ||
            g_abandoned_ordinary_closes_finished.load() != FINISHED + 1 || proxy.open_file_refs.load() != 2 ||
            g_open_cancellations.at(SLOT).phase != ExecCancellationPhase::PENDING)
            return fail(__LINE__);
        finish_ordinary_cancellation(SLOT, 0, false, false);
    }
    // Normal readonly close hands off durably; writable close waits for retirement.
    for (int flags : {0, 2}) {
        auto* file = make_file(19 + flags, flags);
        if (file == nullptr) return fail(__LINE__);
        uint16_t const SLOT = static_cast<RemoteFileContext*>(file->private_data)->ordinary_cancel_slot;
        proxy.binding_retirement_confirmed = true;
        int const CLOSED = ker::vfs::vfs_put_file_result(file);
        if (CLOSED != 0 || mount.open_files.load() != 0 || mount.refs.load() != 7 ||
            g_open_cancellations.at(SLOT).phase != (flags == 0 ? ExecCancellationPhase::PENDING : ExecCancellationPhase::FREE))
            return fail(__LINE__);
        if (flags == 0 && cancel_remote_ordinary_open(SLOT) != 0) return fail(__LINE__);
    }
    {
        auto* file = make_file(22, 2);
        if (file == nullptr) return fail(__LINE__);
        auto* context = static_cast<RemoteFileContext*>(file->private_data);
        uint16_t const SLOT = context->ordinary_cancel_slot;
        context->write_buf->pending_len = 1;  // No backing bytes: actual flush rejects EIO.
        if (ker::vfs::vfs_put_file_result(file) != -EIO || mount.open_files.load() != 0 ||
            g_open_cancellations.at(SLOT).phase != ExecCancellationPhase::FREE)
            return fail(__LINE__);
    }
    // An ACK can recycle the slot before the normal close stack unwinds.
    {
        auto* file = make_file(23, 0);
        if (file == nullptr) return fail(__LINE__);
        uint16_t const SLOT = static_cast<RemoteFileContext*>(file->private_data)->ordinary_cancel_slot;
        {
            OrdinaryCloseOwner owner(file, PID);
            finish_ordinary_cancellation(SLOT, 0, false, false);
            if (reserve_ordinary_cancellation(&proxy) != SLOT) return fail(__LINE__);
            g_open_cancellations.at(SLOT).request.token = 24;
        }
        if (file->private_data != nullptr || g_open_cancellations.at(SLOT).phase != ExecCancellationPhase::OWNED) return fail(__LINE__);
        ker::vfs::vfs_put_file(file);
        finish_ordinary_cancellation(SLOT, 0, false, false);
    }
    return proxy.open_file_refs.load() == 1 && mount.open_files.load() == 0 && g_pin_preparations == nullptr &&
           g_ordinary_closes == nullptr && g_shared_io_owners == nullptr;
}

auto wki_remote_vfs_selftest_open_at_preparation() -> bool {
    if (g_wki.initialized || g_pin_preparations != nullptr) return false;
    constexpr uint64_t PID = 0xD190D2;
    ProxyVfsState proxy{};
    proxy.open_file_refs.store(1, std::memory_order_release);
    struct FaultScope {
        ~FaultScope() {
            g_preparation_failure_pid = 0;
            g_preparation_failure_allocation = 0;
        }
    } fault_scope;
    g_preparation_failure_pid = PID;
    g_preparation_failure_allocation = 2;
    ker::vfs::File caller{};
    {
        PinPreparation denied(&proxy, PinPurpose::ORDINARY_OPEN, caller, PID);
        if (denied.status() != -ENOTCONN || g_pin_preparations != nullptr) return false;
    }
    proxy.active = true;
    size_t const FILES = g_pin_private_files_freed.load();
    {
        PinPreparation failed(&proxy, PinPurpose::ORDINARY_OPEN, caller, PID);
        if (failed.status() != -ENOMEM || failed.file() != &caller || proxy.open_file_refs.load() != 2) return false;
    }
    if (proxy.open_file_refs.load() != 1 || g_pin_preparations != nullptr || g_pin_private_files_freed.load() != FILES) return false;
    g_preparation_failure_allocation = 0;

    ker::vfs::MountPoint mount{};
    auto release = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
    auto make_file = [&]() {
        std::unique_ptr<ker::vfs::File, decltype(release)> file(new ker::vfs::File{}, release);
        file->refcount = 1;
        file->fs_type = ker::vfs::FSType::REMOTE;
        file->open_flags = ker::vfs::O_NO_CACHE;
        auto* path = new char[sizeof("/__goal19_openat_prepared")];
        std::memcpy(path, "/__goal19_openat_prepared", sizeof("/__goal19_openat_prepared"));
        file->vfs_path = path;
        file->vfs_path_heap_allocated = true;
        file->vfs_path_len = sizeof("/__goal19_openat_prepared") - 1;
        // Model the caller's already-admitted pin; exercise actual VFS release.
        mount.open_files.fetch_add(1, std::memory_order_relaxed);
        file->mount_owner = &mount;
        return file;
    };
    {
        auto file = make_file();
        auto* path = file->vfs_path;
        {
            PinPreparation failed(&proxy, PinPurpose::ORDINARY_OPEN, *file, PID);
            if (failed.status() != 0) return false;
        }
        if (file->vfs_path != path || file->mount_owner != &mount || mount.open_files.load() != 1 || file->private_data != nullptr ||
            proxy.open_file_refs.load() != 1 || g_pin_private_files_freed.load() != FILES)
            return false;
    }
    if (mount.open_files.load() != 0) return false;
    {
        auto abandoned_file = make_file();
        auto other_file = make_file();
        PinPreparation abandoned(&proxy, PinPurpose::ORDINARY_OPEN, *abandoned_file, PID);
        PinPreparation other(&proxy, PinPurpose::ORDINARY_OPEN, *other_file, PID + 1);
        if (abandoned.status() != 0 || other.status() != 0) return false;
        static_cast<void>(abandoned_file.release());  // Its caller's stack will not resume.
        wki_remote_vfs_cleanup_for_task(PID);
        if (abandoned.file() != nullptr || abandoned.context() != nullptr || abandoned.status() != -ECANCELED ||
            other.file() != other_file.get() || other.status() != 0 || mount.open_files.load() != 1 || proxy.open_file_refs.load() != 2 ||
            g_pin_private_files_freed.load() != FILES + 1)
            return false;
        abandoned.cancel();
        wki_remote_vfs_cleanup_for_task(PID);
        if (mount.open_files.load() != 1 || proxy.open_file_refs.load() != 2 || g_pin_private_files_freed.load() != FILES + 1) return false;
    }
    if (mount.open_files.load() != 0 || proxy.open_file_refs.load() != 1 || g_pin_preparations != nullptr) return false;
    {
        auto file = make_file();
        PinPreparation success(&proxy, PinPurpose::ORDINARY_OPEN, *file, PID);
        if (success.status() != 0) return false;
        auto* context = success.context();
        context->proxy = &proxy;
        auto* result = success.publish();
        success.cancel();
        bool const TRANSFERRED =
            result == file.get() && result->private_data == context && proxy.open_file_refs.load() == 2 && mount.open_files.load() == 1;
        // No owner RPC or backend File in this private-storage test.
        delete context;
        file->private_data = nullptr;
        release_vfs_proxy_open_ref(&proxy);
        if (!TRANSFERRED) return false;
    }
    return mount.open_files.load() == 0 && proxy.open_file_refs.load() == 1 && g_pin_preparations == nullptr &&
           g_pin_private_files_freed.load() == FILES + 1;
}

auto wki_remote_vfs_selftest_pin_preparation_purpose() -> bool {
    if (g_wki.initialized || g_pin_preparations != nullptr) return false;
    ProxyVfsState proxy{};
    WkiChannel channel{};
    proxy.active = true;
    proxy.owner_node = 0x7FFF;
    proxy.assigned_channel_ref = &channel;
    proxy.assigned_channel = 77;
    proxy.assigned_channel_generation = 39;
    proxy.open_file_refs.store(1, std::memory_order_release);
    constexpr uint64_t DIRECTORY_PID = 0xD190B1;
    constexpr uint64_t EXEC_PID = 0xD190B2;
    struct Cleanup {
        ProxyVfsState* proxy;
        ~Cleanup() {
            PinPreparation::cleanup_for_task(DIRECTORY_PID);
            PinPreparation::cleanup_for_task(EXEC_PID);
            for (size_t index = 0; index < g_exec_cancellations.size(); ++index) {
                s_vfs_lock.lock();
                bool const OWNED = g_exec_cancellations.at(index).proxy == proxy;
                s_vfs_lock.unlock();
                if (OWNED) finish_exec_cancellation(static_cast<uint16_t>(index), 0, false, false);
            }
        }
    } cleanup{&proxy};
    auto fail = [](int line) {
        ker::mod::dbg::logger<"ktest">::error("pin preparation purpose invariant failed at remote_vfs.cpp:%d", line);
        return false;
    };
    size_t const FILES = g_pin_private_files_freed.load();
    size_t const CONTEXTS = g_pin_private_contexts_freed.load();
    {
        PinPreparation directory(&proxy, PinPurpose::DIRECTORY, DIRECTORY_PID);
        PinPreparation execution(PinPurpose::EXECUTABLE, EXEC_PID);
        if (directory.status() != 0 || execution.status() != 0) return fail(__LINE__);
        execution.file()->exec_read_view.reset(new (std::nothrow) ker::vfs::ExecReadView{});
        if (!execution.file()->exec_read_view || execution.retain_proxy(&proxy) != 0) return fail(__LINE__);
        uint16_t const DIR_SLOT = reserve_exec_cancellation(&proxy, 904, true);
        uint16_t const EXEC_SLOT = reserve_exec_cancellation(&proxy, 904, false);
        if (DIR_SLOT == VFS_EXEC_CANCEL_SLOT_NONE || EXEC_SLOT == VFS_EXEC_CANCEL_SLOT_NONE || DIR_SLOT == EXEC_SLOT) return fail(__LINE__);
        directory.own_cancellation(DIR_SLOT, 904);
        execution.own_cancellation(EXEC_SLOT, 904);
        wki_remote_vfs_cleanup_for_task(DIRECTORY_PID);
        if (directory.file() != nullptr || execution.file() == nullptr || !execution.file()->exec_read_view ||
            proxy.open_file_refs.load() != 4 || g_exec_cancellations.at(DIR_SLOT).phase != ExecCancellationPhase::PENDING ||
            g_exec_cancellations.at(EXEC_SLOT).phase != ExecCancellationPhase::OWNED)
            return fail(__LINE__);
        finish_exec_cancellation(DIR_SLOT, 0, false, false);
        wki_remote_vfs_cleanup_for_task(EXEC_PID);
        if (execution.file() != nullptr || execution.context() != nullptr || proxy.open_file_refs.load() != 2 ||
            g_exec_cancellations.at(EXEC_SLOT).phase != ExecCancellationPhase::PENDING || g_exec_cancellations.at(EXEC_SLOT).directory ||
            g_exec_cancellations.at(EXEC_SLOT).token != 904)
            return fail(__LINE__);
        finish_exec_cancellation(EXEC_SLOT, 0, false, false);
    }
    {
        // A failed synchronous executable cancellation already handed its row
        // to the outbox. Private preparation must neither consume nor re-own it.
        PinPreparation execution(&proxy, PinPurpose::EXECUTABLE, EXEC_PID);
        if (execution.status() != 0) return fail(__LINE__);
        uint16_t const SLOT = reserve_exec_cancellation(&proxy, 905);
        if (SLOT == VFS_EXEC_CANCEL_SLOT_NONE) return fail(__LINE__);
        execution.own_cancellation(SLOT, 905);
        finish_exec_cancellation(SLOT, -EIO, false, false);
        wki_remote_vfs_cleanup_for_task(EXEC_PID);
        execution.cancel();
        if (proxy.open_file_refs.load() != 2 || g_exec_cancellations.at(SLOT).phase != ExecCancellationPhase::PENDING ||
            g_exec_cancellations.at(SLOT).token != 905)
            return fail(__LINE__);
        finish_exec_cancellation(SLOT, 0, false, false);
    }
    {
        // Even an identical token and proxy cannot change cancellation purpose.
        PinPreparation directory(&proxy, PinPurpose::DIRECTORY, DIRECTORY_PID);
        if (directory.status() != 0) return fail(__LINE__);
        uint16_t const SLOT = reserve_exec_cancellation(&proxy, 906, false);
        if (SLOT == VFS_EXEC_CANCEL_SLOT_NONE) return fail(__LINE__);
        directory.own_cancellation(SLOT, 906);
        directory.cancel();
        if (proxy.open_file_refs.load() != 2 || g_exec_cancellations.at(SLOT).phase != ExecCancellationPhase::OWNED) return fail(__LINE__);
        finish_exec_cancellation(SLOT, 0, false, false);
    }
    if (g_pin_private_files_freed.load() != FILES + 4 || g_pin_private_contexts_freed.load() != CONTEXTS + 4 ||
        proxy.open_file_refs.load() != 1)
        return fail(__LINE__);
    {
        PinPreparation execution(PinPurpose::EXECUTABLE, EXEC_PID);
        if (execution.status() != 0) return fail(__LINE__);
        auto* file = execution.file();
        file->exec_read_view.reset(new (std::nothrow) ker::vfs::ExecReadView{});
        if (!file->exec_read_view || execution.retain_proxy(&proxy) != 0) return fail(__LINE__);
        uint16_t const SLOT = reserve_exec_cancellation(&proxy, 907);
        if (SLOT == VFS_EXEC_CANCEL_SLOT_NONE) return fail(__LINE__);
        execution.own_cancellation(SLOT, 907);
        auto* context = execution.context();
        context->proxy = &proxy;
        context->remote_fd = 17;
        context->exec_cancel_slot = SLOT;
        file->exec_read_view->content_id = 907;
        file->exec_read_view->metadata.st_mode = ker::vfs::S_IFREG | 0755;
        file->fops = &g_remote_vfs_fops;
        file->fs_type = ker::vfs::FSType::REMOTE;
        file->fd = -1;
        file->refcount = 1;
        file->open_flags = ker::vfs::O_NO_CACHE;
        std::unique_ptr<ker::vfs::File, DirectoryGrantFileRelease> published(execution.publish());
        wki_remote_vfs_cleanup_for_task(EXEC_PID);
        if (!published || published->exec_read_view->content_id != 907 ||
            published->exec_read_view->metadata.st_mode != (ker::vfs::S_IFREG | 0755) || published->private_data != context ||
            execution.file() != nullptr || proxy.open_file_refs.load() != 3 ||
            g_exec_cancellations.at(SLOT).phase != ExecCancellationPhase::OWNED)
            return fail(__LINE__);
        proxy.binding_retirement_confirmed = true;
        int const CLOSED = ker::vfs::vfs_put_file_result(published.release());
        if (CLOSED != -ENOTCONN || proxy.open_file_refs.load() != 1 || g_exec_cancellations.at(SLOT).phase != ExecCancellationPhase::FREE)
            return fail(__LINE__);
    }
    return g_pin_preparations == nullptr && proxy.open_file_refs.load() == 1;
}

auto wki_remote_vfs_selftest_pin_close_lifetime() -> bool {
    if (g_wki.initialized || g_pin_closes != nullptr || g_pin_preparations != nullptr) return false;
    ProxyVfsState proxy{};
    WkiChannel channel{};
    proxy.active = true;
    proxy.owner_node = 0x7FFF;
    proxy.assigned_channel_ref = &channel;
    proxy.assigned_channel = 77;
    proxy.assigned_channel_generation = 39;
    proxy.open_file_refs.store(1, std::memory_order_release);
    constexpr uint64_t DIRECTORY_PID = 0xD190C1;
    constexpr uint64_t EXEC_PID = 0xD190C2;
    struct Cleanup {
        ProxyVfsState* proxy;
        ~Cleanup() {
            for (size_t index = 0; index < g_exec_cancellations.size(); ++index) {
                if (g_exec_cancellations.at(index).proxy == proxy) finish_exec_cancellation(static_cast<uint16_t>(index), 0, false, false);
            }
        }
    } cleanup{&proxy};
    struct ExitCleanup {
        uint64_t pid;
        ~ExitCleanup() { wki_remote_vfs_cleanup_for_task(pid); }
    };
    auto fail = [](int line) {
        ker::mod::dbg::logger<"ktest">::error("pin close lifetime invariant failed at remote_vfs.cpp:%d", line);
        return false;
    };
    auto make_file = [&](bool directory, ker::vfs::MountPoint& mount, uint64_t token) -> ker::vfs::File* {
        PinPreparation preparation(&proxy, directory ? PinPurpose::DIRECTORY : PinPurpose::EXECUTABLE);
        if (preparation.status() != 0) return nullptr;
        auto* file = preparation.file();
        if (!directory) {
            file->exec_read_view.reset(new (std::nothrow) ker::vfs::ExecReadView{});
            if (!file->exec_read_view) return nullptr;
            file->exec_read_view->content_id = token;
        }
        std::unique_ptr<ReadAheadCache> read_cache(new (std::nothrow) ReadAheadCache{});
        std::unique_ptr<WriteBehindBuffer> write_buffer(new (std::nothrow) WriteBehindBuffer{});
        auto prefetch = std::unique_ptr<uint8_t[]>(new (std::nothrow) uint8_t[32]{});
        auto path = std::unique_ptr<char[]>(new (std::nothrow) char[320]{});
        if (!read_cache || !write_buffer || !prefetch || !path) return nullptr;
        uint16_t const SLOT = reserve_exec_cancellation(&proxy, token, directory);
        if (SLOT == VFS_EXEC_CANCEL_SLOT_NONE) return nullptr;
        preparation.own_cancellation(SLOT, token);
        auto* context = preparation.context();
        context->proxy = &proxy;
        context->remote_fd = 17;
        context->exec_cancel_slot = SLOT;
        context->directory_pin = directory;
        context->read_cache = read_cache.release();
        context->write_buf = write_buffer.release();
        context->open_prefetch_buf = prefetch.release();
        context->open_prefetch_len = 32;
        file->fops = &g_remote_vfs_fops;
        file->fs_type = ker::vfs::FSType::REMOTE;
        file->refcount = 1;
        file->fd = -1;
        file->is_directory = directory;
        file->open_flags = ker::vfs::O_NO_CACHE | (directory ? ker::vfs::O_DIRECTORY | ker::vfs::FILE_ACCESS_SEARCH_ONLY : 0);
        path[0] = '/';
        for (size_t i = 1; i < 319; ++i) path[i] = 'p';
        file->vfs_path = path.release();
        file->vfs_path_len = 319;
        file->vfs_path_heap_allocated = true;
        mount.open_files = 1;
        mount.refs = 7;
        file->mount_owner = &mount;
        return preparation.publish();
    };
    size_t const FINISHED = g_abandoned_pin_closes_finished.load();
    ker::vfs::MountPoint directory_mount{};
    ker::vfs::MountPoint exec_mount{};
    {
        auto* directory = make_file(true, directory_mount, 908);
        if (directory == nullptr) return fail(__LINE__);
        auto* directory_context = static_cast<RemoteFileContext*>(directory->private_data);
        uint16_t const DIR_SLOT = directory_context->exec_cancel_slot;
        // Fresh fixture Files have no stream/cache/advisory registration. Model
        // the exclusive zero-ref state immediately after VFS's close prelude.
        directory->refcount = 0;
        PinCloseOwner first(directory, DIRECTORY_PID);
        ExitCleanup first_exit{DIRECTORY_PID};
        auto* execution = make_file(false, exec_mount, 908);
        if (execution == nullptr) return fail(__LINE__);
        auto* exec_context = static_cast<RemoteFileContext*>(execution->private_data);
        uint16_t const EXEC_SLOT = exec_context->exec_cancel_slot;
        execution->refcount = 0;
        PinCloseOwner other(execution, EXEC_PID);
        ExitCleanup other_exit{EXEC_PID};
        if (!immutable_pin_close(*directory, *directory_context) || !immutable_pin_close(*execution, *exec_context)) return fail(__LINE__);
        execution->open_flags |= ker::vfs::O_TRUNC;
        if (immutable_pin_close(*execution, *exec_context)) return fail(__LINE__);
        constexpr int READ_WRITE_ACCESS = 2;
        execution->open_flags = ker::vfs::O_NO_CACHE | READ_WRITE_ACCESS;
        if (immutable_pin_close(*execution, *exec_context)) return fail(__LINE__);
        execution->open_flags = ker::vfs::O_NO_CACHE;
        exec_context->write_buf->pending_len = 1;
        bool const PENDING_WRITES_REJECTED = !immutable_pin_close(*execution, *exec_context);
        exec_context->write_buf->pending_len = 0;
        if (!PENDING_WRITES_REJECTED || proxy.open_file_refs.load() != 5) return fail(__LINE__);
        wki_remote_vfs_cleanup_for_task(DIRECTORY_PID + EXEC_PID);
        if (first.file() == nullptr || other.file() == nullptr || proxy.open_file_refs.load() != 5) return fail(__LINE__);
        wki_remote_vfs_cleanup_for_task(DIRECTORY_PID);
        if (first.file() != nullptr || other.file() == nullptr || directory_mount.open_files.load() != 0 ||
            directory_mount.refs.load() != 7 || exec_mount.open_files.load() != 1 || proxy.open_file_refs.load() != 4 ||
            g_abandoned_pin_closes_finished.load() != FINISHED + 1 ||
            g_exec_cancellations.at(DIR_SLOT).phase != ExecCancellationPhase::PENDING || !g_exec_cancellations.at(DIR_SLOT).directory ||
            g_exec_cancellations.at(DIR_SLOT).token != 908 || g_exec_cancellations.at(EXEC_SLOT).phase != ExecCancellationPhase::OWNED)
            return fail(__LINE__);
        wki_remote_vfs_cleanup_for_task(DIRECTORY_PID);
        if (g_abandoned_pin_closes_finished.load() != FINISHED + 1 || proxy.open_file_refs.load() != 4) return fail(__LINE__);
        finish_exec_cancellation(DIR_SLOT, 0, false, false);
        // Cancellation already handed off before stack abandonment remains pending.
        finish_exec_cancellation(EXEC_SLOT, -EIO, false, false);
        wki_remote_vfs_cleanup_for_task(EXEC_PID);
        if (other.file() != nullptr || exec_mount.open_files.load() != 0 || exec_mount.refs.load() != 7 ||
            proxy.open_file_refs.load() != 2 || g_abandoned_pin_closes_finished.load() != FINISHED + 2 ||
            g_exec_cancellations.at(EXEC_SLOT).phase != ExecCancellationPhase::PENDING || g_exec_cancellations.at(EXEC_SLOT).directory ||
            g_exec_cancellations.at(EXEC_SLOT).token != 908)
            return fail(__LINE__);
        finish_exec_cancellation(EXEC_SLOT, 0, false, false);
    }
    // Actual normal VFS close must keep the existing retirement/inactive errno
    // and release both the backend and mount ownership without task-exit drain.
    for (bool directory : {true, false}) {
        auto* file = make_file(directory, directory_mount, directory ? 909 : 910);
        if (file == nullptr) return fail(__LINE__);
        uint16_t const SLOT = static_cast<RemoteFileContext*>(file->private_data)->exec_cancel_slot;
        proxy.binding_retirement_confirmed = true;
        proxy.active = directory;
        int const CLOSED = ker::vfs::vfs_put_file_result(file);
        proxy.active = true;
        if (CLOSED != (directory ? -ENOTCONN : -EIO) || directory_mount.open_files.load() != 0 || directory_mount.refs.load() != 7 ||
            proxy.open_file_refs.load() != 1 || g_exec_cancellations.at(SLOT).phase != ExecCancellationPhase::FREE ||
            g_abandoned_pin_closes_finished.load() != FINISHED + 2)
            return fail(__LINE__);
    }
    return g_pin_closes == nullptr && g_pin_preparations == nullptr && proxy.open_file_refs.load() == 1;
}

auto wki_remote_vfs_selftest_exec_cancellation_ownership() -> bool {
    ProxyVfsState proxy{};
    WkiChannel channel{};
    proxy.active = true;
    proxy.owner_node = 0x7FFF;
    proxy.assigned_channel_ref = &channel;
    proxy.assigned_channel = 77;
    proxy.assigned_channel_generation = 39;
    // An external source reference prevents test-only resource destruction.
    proxy.open_file_refs.store(1, std::memory_order_release);
    struct Cleanup {
        ProxyVfsState* proxy;
        ~Cleanup() {
            for (size_t i = 0; i < g_exec_cancellations.size(); ++i) {
                s_vfs_lock.lock();
                bool const OWNED = g_exec_cancellations.at(i).proxy == proxy;
                s_vfs_lock.unlock();
                if (OWNED) finish_exec_cancellation(static_cast<uint16_t>(i), 0, false, false);
            }
        }
    } cleanup{&proxy};
    uint16_t const SLOT = reserve_exec_cancellation(&proxy, 123);
    if (SLOT == VFS_EXEC_CANCEL_SLOT_NONE || proxy.open_file_refs.load(std::memory_order_acquire) != 2) return false;
    auto& record = g_exec_cancellations.at(SLOT);
    // Real immediate RPC slot failure must retain cancellation ownership.
    proxy.op_pending.store(true, std::memory_order_release);
    if (cancel_remote_exec_view(SLOT, true) == 0) return false;
    s_vfs_lock.lock();
    record.retry_after_us = UINT64_MAX;
    bool const RETAINED = record.phase == ExecCancellationPhase::PENDING && record.token == 123 && record.proxy == &proxy;
    record.phase = ExecCancellationPhase::OWNED;
    s_vfs_lock.unlock();
    proxy.op_pending.store(false, std::memory_order_release);
    if (!RETAINED || proxy.open_file_refs.load(std::memory_order_acquire) != 2) return false;
    // Even an idle slot must preserve existing FIFO waiters.
    proxy.op_slot_waiter_count = 1;
    proxy.op_slot_waiter_pids.front() = 456;
    if (try_acquire_proxy_op_slot_locked(&proxy) != WKI_ERR_BUSY || proxy.op_slot_waiter_pids.front() != 456) return false;
    proxy.op_slot_waiter_count = 0;
    proxy.op_slot_waiter_pids.front() = 0;
    // Inactivity and an absent pending detach are not retirement evidence.
    proxy.active = false;
    if (exec_cancellation_retirement_proven(record)) return false;
    proxy.binding_retirement_confirmed = true;
    proxy.assigned_channel_generation++;
    if (exec_cancellation_retirement_proven(record)) return false;
    proxy.assigned_channel_generation--;
    if (!exec_cancellation_retirement_proven(record)) return false;
    static_cast<void>(cancel_remote_exec_view(SLOT, true));
    if (record.phase != ExecCancellationPhase::FREE || proxy.open_file_refs.load(std::memory_order_acquire) != 1) return false;
    proxy.active = true;
    proxy.binding_retirement_confirmed = false;
    // The fixed reservation capacity includes live views, not just failures.
    // Exhaustion occurs before callers can issue PIN and adds no proxy ref.
    size_t admitted = 0;
    while (reserve_exec_cancellation(&proxy, 1000 + admitted) != VFS_EXEC_CANCEL_SLOT_NONE) ++admitted;
    return admitted == VFS_EXEC_CANCEL_CAPACITY && proxy.open_file_refs.load(std::memory_order_acquire) == 1 + VFS_EXEC_CANCEL_CAPACITY;
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

auto wki_remote_vfs_selftest_lane_round_robin_uses_full_capacity() -> bool {
    // ProxyVfsState contains a large readlink cache. Eight automatic fixtures
    // exceed the kernel stack while KTEST still runs on the Limine boot stack.
    std::unique_ptr<ProxyVfsState[]> lanes{new (std::nothrow) ProxyVfsState[VFS_PROXY_LANE_COUNT]{}};
    if (lanes == nullptr) {
        return false;
    }

    auto& anchor = lanes[0];
    anchor.active = true;
    anchor.lane_anchor = true;
    anchor.lanes_ready = true;
    anchor.mount_group_id = 1;
    anchor.lane_index = 0;
    anchor.lanes.at(0) = &anchor;
    anchor.lane_count = VFS_PROXY_LANE_COUNT;

    for (size_t index = 1; index < VFS_PROXY_LANE_COUNT; ++index) {
        auto& lane = lanes[index];
        lane.active = true;
        lane.mount_group_id = anchor.mount_group_id;
        lane.lane_index = static_cast<uint8_t>(index);
        anchor.lanes.at(index) = &lane;
    }

    for (size_t index = 0; index < VFS_PROXY_LANE_COUNT; ++index) {
        ProxyVfsState* const SELECTED = acquire_vfs_proxy_lane(&anchor);
        if (SELECTED != anchor.lanes.at(index) || SELECTED->lifecycle_refs != 1) {
            return false;
        }
    }
    return anchor.lane_selection_cursor == VFS_PROXY_LANE_COUNT;
}

auto wki_remote_vfs_selftest_lane_pressure_precedes_rdma() -> bool {
    WkiTransport transport{};
    transport.name = "wki-roce";

    ProxyVfsState anchor{};
    ProxyVfsState rdma_lane{};
    anchor.active = true;
    anchor.lane_anchor = true;
    anchor.lanes_ready = true;
    anchor.mount_group_id = 1;
    anchor.lane_index = 0;
    anchor.lanes.at(0) = &anchor;
    anchor.lanes.at(1) = &rdma_lane;
    anchor.lane_count = 2;
    rdma_lane.active = true;
    rdma_lane.mount_group_id = 1;
    rdma_lane.lane_index = 1;
    rdma_lane.rdma_transport = &transport;
    rdma_lane.rdma_capable = true;

    rdma_lane.op_pending.store(true, std::memory_order_release);
    ProxyVfsState* selected = acquire_vfs_proxy_lane(&anchor, true, false);
    if (selected != &anchor || anchor.lifecycle_refs != 1 || rdma_lane.lifecycle_refs != 0) {
        return false;
    }

    rdma_lane.op_pending.store(false, std::memory_order_release);
    anchor.op_pending.store(true, std::memory_order_release);
    selected = acquire_vfs_proxy_lane(&anchor, true, false);
    if (selected != &rdma_lane || anchor.lifecycle_refs != 1 || rdma_lane.lifecycle_refs != 1) {
        return false;
    }

    anchor.op_pending.store(false, std::memory_order_release);
    anchor.lifecycle_refs = 0;
    rdma_lane.lifecycle_refs = 1;
    selected = acquire_vfs_proxy_lane(&anchor, true, false);
    if (selected != &anchor || anchor.lifecycle_refs != 1 || rdma_lane.lifecycle_refs != 1) {
        return false;
    }

    anchor.lifecycle_refs = 0;
    rdma_lane.lifecycle_refs = 0;
    rdma_lane.open_file_refs.store(1, std::memory_order_release);
    selected = acquire_vfs_proxy_lane(&anchor, true, false);
    return selected == &anchor && anchor.lifecycle_refs == 1 && rdma_lane.lifecycle_refs == 0;
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

auto wki_remote_vfs_selftest_writable_close_wait_policy() -> bool {
    return !remote_vfs_close_needs_completion(0) && !remote_vfs_close_needs_completion(ker::vfs::O_CLOEXEC) &&
           remote_vfs_close_needs_completion(1) && remote_vfs_close_needs_completion(2) &&
           remote_vfs_close_needs_completion(ker::vfs::O_CREAT) && remote_vfs_close_needs_completion(ker::vfs::O_TRUNC) &&
           remote_vfs_close_needs_completion(ker::vfs::O_CREAT | ker::vfs::O_CLOEXEC);
}

#endif

// -------------------------------------------------------------------------------
// Init
// -------------------------------------------------------------------------------

auto wki_remote_vfs_issue_native_directory_grant(ker::mod::sched::task::Task& task, const ker::mod::security::Snapshot& credentials,
                                                 uint32_t resource_id, const ker::vfs::DirectoryViewRef& source, uint16_t target_node,
                                                 uint32_t target_boot_epoch, uint64_t submission_id,
                                                 ker::vfs::MountPolicy additional_policy, ker::vfs::File** staging,
                                                 VfsDirectoryGrant& output) -> int {
    namespace vfs = ker::vfs;
    if (!source || staging == nullptr || resource_id == 0 || target_node == WKI_NODE_INVALID || target_node == WKI_NODE_BROADCAST ||
        target_boot_epoch == 0 || submission_id == 0 || g_wki.my_node_id == WKI_NODE_INVALID || g_wki.local_boot_epoch == 0)
        return -EINVAL;
    if (target_node == g_wki.my_node_id && target_boot_epoch != g_wki.local_boot_epoch) return -ESTALE;
    auto const REQUEST_CREDENTIALS = credentials;
    VfsExport resource{};
    if (!wki_remote_vfs_find_export_snapshot(resource_id, &resource)) return -ESTALE;
    // Pin the export boundary for identity only. Existing cwd access does not
    // require renewed search on its ancestors merely to prove confinement.
    vfs::File* root = nullptr;
    int status = vfs::vfs_pin_directory_resolved_beneath_as(task, REQUEST_CREDENTIALS, resource.export_path, resource.export_path, &root,
                                                            nullptr, false);
    if (status != 0) return status;
    std::unique_ptr<vfs::File, DirectoryGrantFileRelease> exported_root(root);
    if (root->mount_owner == nullptr || root->mount_owner->dev_id != resource.backing_dev_id || root->fs_type != resource.backing_fs_type)
        return -ESTALE;
    std::array<char, 512> membership{};
    status = vfs::vfs_directory_path_beneath_directory(source.root(), root, membership.data(), membership.size());
    if (status != 0) return status;
    vfs::DirectoryViewRef narrowed;
    status = vfs::vfs_narrow_directory_view(source, REQUEST_CREDENTIALS, source.root(), source.directory(), additional_policy, narrowed);
    if (status != 0) return status;
    vfs::File* pinned = nullptr;
    status = vfs::vfs_pin_directory_view_as(task, REQUEST_CREDENTIALS, narrowed, ".", &pinned);
    if (status != 0) return status;
    std::unique_ptr<vfs::File, DirectoryGrantFileRelease> candidate(pinned);
    // Allocate all cancellation and registry storage before publication. The
    // private cleanup File gains ownership only after its row exists.
    std::unique_ptr<vfs::File> cleanup_file(new (std::nothrow) vfs::File{});
    std::unique_ptr<NativeDirectoryGrantCleanup> cleanup(new (std::nothrow) NativeDirectoryGrantCleanup{});
    if (!cleanup_file || !cleanup) return -ENOMEM;
    VfsDirectoryGrant grant{.version = WKI_VFS_DIRECTORY_GRANT_VERSION,
                            .policy = directory_grant_policy(pinned->directory_view.policy()),
                            .owner_node = g_wki.my_node_id,
                            .source_node = g_wki.my_node_id,
                            .target_node = target_node,
                            .resource_id = resource_id,
                            .owner_incarnation = {g_wki.local_boot_epoch, resource.resource_incarnation},
                            .source_boot_epoch = g_wki.local_boot_epoch,
                            .target_boot_epoch = target_boot_epoch,
                            .submission_id = submission_id};
    std::map<uint64_t, RemoteVfsFd> prepared;
    RemoteVfsFd row{};
    row.active = true;
    row.consumer_node = g_wki.my_node_id;
    row.file = pinned;
    row.directory_grant_credentials = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
    row.directory_grant_publication_revision = resource.publication_revision;
    row.directory_grant_local_source = true;
    prepared.emplace(0, row);
    auto node = prepared.extract(0);
    s_vfs_lock.lock();
    if (!native_grant_resource_current_locked(grant, resource.publication_revision))
        status = -ESTALE;
    else if (g_native_directory_grants.size() >= VFS_DIRECTORY_GRANTS_PER_CONSUMER)
        status = -EMFILE;
    else {
        grant.owner_grant_id = take_directory_grant_id(g_next_directory_grant_id);
        if (grant.owner_grant_id == 0) status = -EOVERFLOW;
    }
    if (status == 0) {
        node.key() = grant.owner_grant_id;
        node.mapped().directory_grant = grant;
        node.mapped().last_activity_us = wki_now_us();
        g_native_directory_grants.insert(std::move(node));
    }
    s_vfs_lock.unlock();
    if (status != 0) return status;
    static_cast<void>(candidate.release());
    cleanup->id = grant.owner_grant_id;
    cleanup_file->private_data = cleanup.release();
    cleanup_file->fops = &g_native_directory_grant_cleanup_ops;
    cleanup_file->fs_type = vfs::FSType::DEVFS;
    cleanup_file->fd = -1;
    cleanup_file->refcount = 1;
    cleanup_file->open_flags = vfs::O_NO_CACHE | vfs::FILE_ACCESS_SEARCH_ONLY;
    cleanup_file->mount_policy = narrowed.policy();
    output = grant;
    *staging = cleanup_file.release();
    return 0;
}

auto wki_remote_vfs_adopt_directory_grant_native(const ker::mod::security::Snapshot& supplied, const WkiDirectoryGrantSource& source,
                                                 const VfsDirectoryGrant& grant, ker::vfs::DirectoryViewRef& output) -> int {
    if (source.node != grant.source_node || source.boot_epoch != grant.source_boot_epoch || source.submission_id != grant.submission_id)
        return -ESTALE;
    ker::mod::security::Snapshot mapped;
    int status = auth_policy::map_compute_identity(source.identity_policy, supplied, mapped);
    if (status != 0) return status;
    DirectoryGrantAdmission admission;
    status = retain_directory_grant(grant, mapped, g_wki.my_node_id, g_wki.local_boot_epoch, admission);
    if (status != 0) return status;
    ker::vfs::DirectoryViewRef prepared;
    status = ker::vfs::vfs_narrow_directory_view(admission.view, mapped, admission.view.root(), admission.view.directory(), {}, prepared);
    if (status != 0) return status;
#ifdef WOS_SELFTEST
    if (directory_grant_adoption_test_hook != nullptr) directory_grant_adoption_test_hook(grant);
#endif
    status = claim_directory_grant(grant, admission);
    if (status != 0) return status;
    // Task publication belongs to the compute receiver after complete native
    // filesystem preparation. This API only transfers independently owned scope.
    output.swap(prepared);
    return 0;
}

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

auto advertise_exports_to_peer(uint16_t peer_node) -> bool {
    size_t const PEER_SLOT = withdrawal_peer_slot(peer_node);
    s_vfs_lock.lock();
    uint64_t const PUBLICATION_REVISION = g_vfs_export_revision;
    if ((PUBLICATION_REVISION & 1U) != 0 || g_vfs_withdrawals.pending(PEER_SLOT)) {
        s_vfs_lock.unlock();
        return false;
    }
    size_t const EXPORT_COUNT = g_vfs_exports.size();
    s_vfs_lock.unlock();

    for (size_t idx = 0; idx < EXPORT_COUNT; idx++) {
        s_vfs_lock.lock();
        if (g_vfs_export_revision != PUBLICATION_REVISION || idx >= g_vfs_exports.size() || g_vfs_withdrawals.pending(PEER_SLOT)) {
            s_vfs_lock.unlock();
            return false;
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
        adv->flags = RESOURCE_FLAG_VFS_EXEC_VIEW | RESOURCE_FLAG_VFS_CHMOD_IDENTITY | RESOURCE_FLAG_VFS_UTIMENS_IDENTITY;
        adv->name_len = NAME_LEN;
        memcpy(buf.data() + sizeof(ResourceAdvertPayload), static_cast<const void*>(raw_data(EXP.name)), NAME_LEN);
        if (WITH_INCARNATION) {
            ResourceIncarnationToken const TOKEN = {
                .owner_boot_epoch = g_wki.local_boot_epoch,
                .resource_incarnation = EXP.resource_incarnation,
            };
            memcpy(buf.data() + sizeof(ResourceAdvertPayload) + NAME_LEN, &TOKEN, sizeof(TOKEN));
        }

        if (wki_send(peer_node, WKI_CHAN_CONTROL, MsgType::RESOURCE_ADVERT, buf.data(), total_len) != WKI_OK) {
            return false;
        }
    }

    return true;
}

}  // namespace

#ifdef WOS_SELFTEST
auto wki_remote_vfs_selftest_withdrawal_outbox() -> bool {
    VfsWithdrawalOutbox box;
    WithdrawalPeers both;
    both.set(0).set(1);
    WithdrawalPeers first;
    first.set(0);
    ResourceIncarnationToken const IDENTITY{17, 23};
    bool ok = !box.reserve(VFS_WITHDRAWAL_CAPACITY + 1) && box.reserve(2) && !box.reserve(1);
    ok &= box.append(100, IDENTITY, both) && box.append(200, {17, 24}, first);
    ok &= !box.append(300, IDENTITY, both);
    box.cancel_reservation();
    VfsWithdrawalAttempt attempt;
    ok &= box.claim(0, attempt) && attempt.resource_id == 100 && wki_resource_incarnation_equal(attempt.incarnation, IDENTITY);
    VfsWithdrawalAttempt competing;
    ok &= !box.claim(0, competing) && !box.claim(WKI_MAX_PEERS, competing);
    auto wrong = attempt;
    ++wrong.serial;
    ok &= !box.finish(0, wrong, WkiReliableTxStatus::ACKED, WKI_OK, {}) && box.pending(0);
    ok &= box.finish(0, attempt, WkiReliableTxStatus::INVALID, WKI_ERR_NO_ROUTE, {});
    ok &= box.claim(0, competing) && competing.serial == attempt.serial && competing.tx.sequence == 0;
    WkiReliableTxToken accepted{};
    accepted.sequence = 11;
    accepted.channel_identity.generation = 7;
    ok &= box.finish(0, competing, WkiReliableTxStatus::INVALID, WKI_OK, accepted);
    ok &= box.claim(0, attempt) && attempt.resource_id == 100 && attempt.tx.sequence == 11;
    // A live accepted sequence stays unchanged even if an unrelated send fails.
    ok &= box.finish(0, attempt, WkiReliableTxStatus::PENDING, WKI_ERR_NO_MEM, {});
    ok &= box.claim(0, attempt) && attempt.tx.sequence == 11 && attempt.tx.channel_identity.generation == 7;
    ok &= box.finish(0, attempt, WkiReliableTxStatus::ACKED, WKI_OK, {}) && box.pending(0) && box.pending(1);
    ok &= box.claim(0, attempt) && attempt.resource_id == 200;
    ok &= box.finish(0, attempt, WkiReliableTxStatus::ACKED, WKI_OK, {}) && !box.pending(0) && box.pending(1);
    // One peer's ACK cannot free another peer's obligation. Freed array slots
    // also cannot reorder a newer withdrawal ahead of the older serial.
    ok &= !box.reserve(VFS_WITHDRAWAL_CAPACITY) && box.reserve(VFS_WITHDRAWAL_CAPACITY - 1);
    ok &= box.append(300, {17, 25}, both);
    box.cancel_reservation();
    ok &= box.claim(1, attempt) && attempt.resource_id == 100;
    ok &= box.finish(1, attempt, WkiReliableTxStatus::INVALID, WKI_OK, accepted);
    ok &= box.claim(1, attempt) && attempt.tx.sequence == 11;
    WkiReliableTxToken successor{};
    successor.sequence = 0;
    successor.channel_identity.generation = 8;
    // Only a retired channel permits a new receipt for the same exact
    // idempotent withdrawal. A failed retry keeps the obligation unaccepted.
    ok &= box.finish(1, attempt, WkiReliableTxStatus::RETIRED, WKI_ERR_NO_CREDITS, {});
    ok &= box.claim(1, attempt) && attempt.resource_id == 100 && attempt.tx.channel_identity.generation == 0;
    ok &= box.finish(1, attempt, WkiReliableTxStatus::INVALID, WKI_OK, successor);
    ok &= box.claim(1, attempt) && attempt.tx.channel_identity.generation == 8 &&
          wki_resource_incarnation_equal(attempt.incarnation, IDENTITY);
    ok &= box.finish(1, attempt, WkiReliableTxStatus::ACKED, WKI_OK, {});
    ok &= box.claim(1, attempt) && attempt.resource_id == 300;
    ok &= box.finish(1, attempt, WkiReliableTxStatus::ACKED, WKI_OK, {}) && !box.pending(1) && box.pending(0);
    ok &= box.claim(0, attempt) && attempt.resource_id == 300;
    ok &= box.finish(0, attempt, WkiReliableTxStatus::ACKED, WKI_OK, {});
    ok &= !box.pending(0) && !box.pending(1) && !box.pending(WKI_MAX_PEERS);
    ok &= box.reserve(VFS_WITHDRAWAL_CAPACITY);
    box.cancel_reservation();
    ok &= box.reserve(1) && !box.append(0, IDENTITY, both) && !box.append(400, {}, both);
    ok &= box.append(400, IDENTITY, {}) && !box.pending(0);
    box.cancel_reservation();
    ok &= box.reserve(VFS_WITHDRAWAL_CAPACITY);
    box.cancel_reservation();
    return ok;
}
#endif

auto wki_remote_vfs_publish_withdrawals(uint16_t peer_node) -> bool {
    if (!g_remote_vfs_initialized) return true;
    size_t const PEER_SLOT = withdrawal_peer_slot(peer_node);
    if (PEER_SLOT == WKI_MAX_PEERS) return false;
    VfsWithdrawalAttempt attempt;
    s_vfs_lock.lock();
    if (g_vfs_export_target_revision != 0 || (g_vfs_export_revision & 1U) != 0) {
        s_vfs_lock.unlock();
        return false;
    }
    if (!g_vfs_withdrawals.pending(PEER_SLOT)) {
        s_vfs_lock.unlock();
        return true;
    }
    bool const CLAIMED = g_vfs_withdrawals.claim(PEER_SLOT, attempt);
    s_vfs_lock.unlock();
    if (!CLAIMED) return false;

    WkiReliableTxStatus const STATUS = wki_reliable_tx_status(attempt.tx);
    int result = WKI_ERR_BUSY;
    WkiReliableTxToken replacement{};
    if (STATUS == WkiReliableTxStatus::INVALID || STATUS == WkiReliableTxStatus::RETIRED) {
        auto* peer = &g_wki.peers.at(PEER_SLOT);
        // Encode negotiated wire shape under the same lease as tracked TX.
        // A concurrent HELLO must not change capabilities between encoding and TX.
        if (wki_peer_lifecycle_acquire(peer)) {
            if (peer->node_id == peer_node && peer->state == PeerState::CONNECTED &&
                !peer->vfs_reset_rebind_pending.load(std::memory_order_acquire)) {
                ResourceAdvertPayload const WITHDRAW = {.node_id = g_wki.my_node_id,
                                                        .resource_type = static_cast<uint16_t>(ResourceType::VFS),
                                                        .resource_id = attempt.resource_id,
                                                        .flags = 0,
                                                        .name_len = 0};
                std::array<uint8_t, sizeof(ResourceAdvertPayload) + sizeof(ResourceIncarnationToken)> bytes{};
                std::memcpy(bytes.data(), &WITHDRAW, sizeof(WITHDRAW));
                uint16_t length = sizeof(WITHDRAW);
                if (wki_resource_incarnation_negotiated(peer_node, ResourceType::VFS)) {
                    std::memcpy(bytes.data() + length, &attempt.incarnation, sizeof(attempt.incarnation));
                    length += sizeof(attempt.incarnation);
                }
                result = wki_send_tracked(peer_node, WKI_CHAN_CONTROL, MsgType::RESOURCE_WITHDRAW, bytes.data(), length, &replacement);
            }
            wki_peer_lifecycle_release(peer);
        }
    }
    s_vfs_lock.lock();
    static_cast<void>(g_vfs_withdrawals.finish(PEER_SLOT, attempt, STATUS, result, replacement));
    bool const COMPLETE = !g_vfs_withdrawals.pending(PEER_SLOT);
    s_vfs_lock.unlock();
    return COMPLETE;
}

auto wki_remote_vfs_advertise_exports_to_peer(uint16_t peer_node) -> bool {
    if (!g_remote_vfs_initialized || peer_node == WKI_NODE_INVALID) {
        return false;
    }

    WkiPeer const* peer = wki_peer_find(peer_node);
    if (peer == nullptr || peer->state != PeerState::CONNECTED) {
        return false;
    }

    return advertise_exports_to_peer(peer_node);
}

void wki_remote_vfs_advertise_exports() {
    if (!g_remote_vfs_initialized) {
        return;
    }

    wki_resource_advertise_all();
}

// -------------------------------------------------------------------------------
// Server Side - VFS Operation Handlers
// -------------------------------------------------------------------------------

namespace {

auto remote_xattr_name_allowed(const char* name) -> bool {
    constexpr std::string_view USER_PREFIX = "user.";
    return name != nullptr && std::string_view(name).starts_with(USER_PREFIX);
}

void send_vfs_xattr_response(const WkiChannelIdentity& channel_identity, uint16_t request_cookie, const VfsXattrPhaseHeader& identity,
                             int status, uint32_t total, uint32_t transferred, const uint8_t* bytes = nullptr, uint16_t byte_count = 0) {
    if (byte_count > WKI_VFS_XATTR_MAX_DATA_CHUNK) {
        status = -EMSGSIZE;
        bytes = nullptr;
        byte_count = 0;
    }
    std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> packet{};
    auto* response = reinterpret_cast<DevOpRespPayload*>(packet.data());
    response->op_id = OP_VFS_XATTR;
    response->status = static_cast<int16_t>(status);
    response->data_len = sizeof(VfsXattrResultPayload) + byte_count;
    response->reserved = request_cookie;
    auto* result = reinterpret_cast<VfsXattrResultPayload*>(packet.data() + sizeof(DevOpRespPayload));
    result->header = identity;
    result->data_len = total;
    result->transferred = transferred;
    if (byte_count != 0 && bytes != nullptr) {
        memcpy(packet.data() + sizeof(DevOpRespPayload) + sizeof(VfsXattrResultPayload), bytes, byte_count);
    }
    static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, packet.data(),
                                                   static_cast<uint16_t>(sizeof(DevOpRespPayload) + response->data_len)));
}

auto vfs_xattr_stage_call(VfsXattrServerStage& stage, ker::vfs::XattrOperation operation, void* value, size_t size,
                          ker::mod::sched::task::Task* worker) -> ssize_t {
    const char* const NAME = operation == ker::vfs::XattrOperation::LIST ? nullptr : stage.name.data();
    if (stage.identity.target == VfsXattrTarget::FD) {
        return ker::vfs::vfs_xattr_file_as(stage.credentials, stage.file, operation, NAME, value, size, static_cast<int>(stage.flags));
    }
    if (worker == nullptr) return -ESRCH;
    std::array<char, 512> absolute{};
    const char* path = stage.path.data();
    if (stage.file == nullptr) {
        int const RESULT = build_full_path(absolute.data(), absolute.size(), stage.export_root.data(), path, std::strlen(path));
        if (RESULT != 0) return RESULT;
        path = absolute.data();
    }
    return ker::vfs::vfs_xattr_resolved_beneath_as(*worker, stage.credentials, stage.export_root.data(), path, operation, NAME, value, size,
                                                   static_cast<int>(stage.flags), stage.follow_final_symlink, stage.require_directory,
                                                   stage.file, stage.export_name[0] == '/' ? stage.export_name.data() : nullptr);
}

auto vfs_xattr_stage_call_get(VfsXattrServerStage& stage, void* value, size_t size) -> ssize_t {
    return vfs_xattr_stage_call(stage, ker::vfs::XattrOperation::GET, value, size, ker::mod::sched::get_current_task());
}

auto vfs_xattr_stage_call_list(VfsXattrServerStage& stage, char* list, size_t size) -> ssize_t {
    return vfs_xattr_stage_call(stage, ker::vfs::XattrOperation::LIST, list, size, ker::mod::sched::get_current_task());
}

auto filter_remote_xattr_list(uint8_t* list, size_t length) -> ssize_t {
    size_t read_offset = 0;
    size_t write_offset = 0;
    while (read_offset < length) {
        auto const* name = reinterpret_cast<const char*>(list + read_offset);
        size_t const REMAINING = length - read_offset;
        size_t const NAME_LEN = strnlen(name, REMAINING);
        if (NAME_LEN == REMAINING) {
            return -EIO;
        }
        size_t const RECORD_LEN = NAME_LEN + 1;
        if (remote_xattr_name_allowed(name)) {
            memmove(list + write_offset, list + read_offset, RECORD_LEN);
            write_offset += RECORD_LEN;
        }
        read_offset += RECORD_LEN;
    }
    return static_cast<ssize_t>(write_offset);
}

auto vfs_xattr_stage_apply_mutation(VfsXattrServerStage& stage, ker::mod::sched::task::Task* worker) -> int {
    if (stage.identity.operation == VfsXattrOperation::SET)
        return static_cast<int>(vfs_xattr_stage_call(stage, ker::vfs::XattrOperation::SET, stage.data.get(), stage.total, worker));
    if (stage.identity.operation == VfsXattrOperation::REMOVE)
        return static_cast<int>(vfs_xattr_stage_call(stage, ker::vfs::XattrOperation::REMOVE, nullptr, 0, worker));
    return 0;
}

#ifdef WOS_SELFTEST
auto selftest_xattr_staged_namespace() -> bool {
    namespace vfs = ker::vfs;
    namespace security = ker::mod::security;
    auto fail = [](int line) {
        ker::mod::dbg::logger<"ktest">::error("staged XATTR namespace invariant failed at remote_vfs.cpp:%d", line);
        return false;
    };
    constexpr auto BASE = "/tmp/ktest_xattr_namespace";
    constexpr auto FILE = "/tmp/ktest_xattr_namespace/file";
    constexpr auto LINK = "/tmp/ktest_xattr_namespace/link";
    constexpr auto PRIVATE = "/tmp/ktest_xattr_namespace/private";
    // Early KTEST has no current task; use the same explicit owner dispatch.
    ker::mod::sched::task::Task worker{};
    std::strcpy(worker.root.data(), "/unrelated-worker-root");
    std::strcpy(worker.cwd.data(), "/unrelated-worker-cwd");
    auto const ROOT = security::Snapshot::bootstrap_root();
    if (ker::mod::sched::resource::initialize_credentials(worker, ROOT) != 0) return fail(__LINE__);
    auto user = ROOT;
    if (user.prepare({.change = security::Change::SET_UID, .real = 1003}, user) != 0) return fail(__LINE__);
    for (bool disk : {false, true}) {
        struct Cleanup {
            bool mounted{};
            ~Cleanup() {
                for (auto path : {LINK, FILE}) static_cast<void>(vfs::vfs_unlink(path));
                static_cast<void>(vfs::vfs_rmdir(PRIVATE));
                if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
                static_cast<void>(vfs::vfs_rmdir(BASE));
            }
        } cleanup;
        if (vfs::vfs_mkdir(BASE, 0755) != 0) return fail(__LINE__);
        if (disk) {
            auto* device = ker::dev::block_device_find_by_name("sdb2");
            if (device == nullptr || vfs::mount_filesystem(BASE, "xfs", device) != 0) return fail(__LINE__);
            cleanup.mounted = true;
        }
        if (vfs::vfs_mkdir(PRIVATE, 0700) != 0) return fail(__LINE__);
        auto* opened = vfs::vfs_open_file(FILE, vfs::O_CREAT | vfs::O_EXCL, 0666);
        if (opened == nullptr) return fail(__LINE__);
        vfs::vfs_put_file(opened);
        if (vfs::vfs_chmod(FILE, 0666) != 0) return fail(__LINE__);
        struct Case {
            const char* name;
            const char* target;
            int user_error;
            int root_error;
        };
        for (auto const& test :
             {Case{"/shared", "/shared/file", 0, 0}, Case{"//shared///", "/shared/file", 0, 0}, Case{"/", "/file", 0, 0},
              Case{"/shared", "./file", 0, 0}, Case{"/shared", "/shared/private/../file", -EACCES, 0},
              Case{"/shared", "/shared_other/file", -EPERM, -EPERM}, Case{"/shared", "/shared/../shared/file", -EPERM, -EPERM},
              Case{"/shared/..", "/file", -EINVAL, -EINVAL}, Case{"named-export", FILE, 0, 0}}) {
            if (vfs::vfs_symlink(test.target, LINK) != 0) return fail(__LINE__);
            for (auto const& caller : {ROOT, user})
                for (unsigned route = 0; route < 3; ++route) {
                    for (auto operation :
                         {vfs::XattrOperation::SET, vfs::XattrOperation::GET, vfs::XattrOperation::LIST, vfs::XattrOperation::REMOVE}) {
                        if (disk && vfs::vfs_setxattr(FILE, "user.matrix", "seed", 4, 0, true) != 0) return fail(__LINE__);
                        VfsXattrServerStage stage;
                        stage.identity.target = VfsXattrTarget::PATH;
                        stage.credentials = caller;
                        stage.follow_final_symlink = true;
                        stage.flags = operation == vfs::XattrOperation::SET ? 2 : 0;
                        std::strcpy(stage.path.data(), "link");
                        std::strcpy(stage.name.data(), "user.matrix");
                        std::array<char, VFS_EXPORT_PATH_LEN> root{}, visible{};
                        std::strcpy(root.data(), BASE);
                        std::strcpy(visible.data(), test.name);
                        if (capture_vfs_xattr_export(root.data(), visible.data(), stage.export_root, stage.export_name) != 0)
                            return fail(__LINE__);
                        // Later work must survive replacement of the caller's buffers.
                        root.fill('x');
                        visible.fill('y');
                        if (route != 0) {
                            vfs::File* pin = nullptr;
                            if (vfs::vfs_pin_directory_resolved_beneath_as(worker, ROOT, BASE, BASE, &pin) != 0) return fail(__LINE__);
                            std::unique_ptr<vfs::File, DirectoryGrantFileRelease> owner(pin);
                            vfs::DirectoryViewRef view;
                            if (vfs::vfs_prepare_directory_view(ROOT, pin, pin, {.read_only = route == 2, .nosuid = route == 2}, view) !=
                                    0 ||
                                vfs::vfs_pin_directory_view_as(worker, ROOT, view, ".", &stage.file) != 0)
                                return fail(__LINE__);
                        }
                        bool const MUTATION = operation == vfs::XattrOperation::SET || operation == vfs::XattrOperation::REMOVE;
                        int const LOOKUP = caller == user ? test.user_error : test.root_error;
                        int const ERROR = LOOKUP != 0 ? LOOKUP : route == 2 && MUTATION ? -EROFS : !disk ? -EOPNOTSUPP : 0;
                        ssize_t const EXPECTED = ERROR != 0                               ? ERROR
                                                 : operation == vfs::XattrOperation::GET  ? 4
                                                 : operation == vfs::XattrOperation::LIST ? 12
                                                                                          : 0;
                        std::array<char, 32> output{};
                        output.fill('!');
                        auto const SENTINEL = output;
                        vfs::Stat before{}, after{};
                        if (vfs::vfs_stat(FILE, &before) != 0) return fail(__LINE__);
                        ssize_t status = 0;
                        if (MUTATION) {
                            stage.identity.operation =
                                operation == vfs::XattrOperation::SET ? VfsXattrOperation::SET : VfsXattrOperation::REMOVE;
                            if (operation == vfs::XattrOperation::SET) {
                                stage.data.reset(new (std::nothrow) uint8_t[4]);
                                if (!stage.data) return fail(__LINE__);
                                std::memcpy(stage.data.get(), "next", 4);
                                stage.total = 4;
                            }
                            status = vfs_xattr_stage_apply_mutation(stage, &worker);
                        } else {
                            status = vfs_xattr_stage_call(stage, operation, output.data(), output.size(), &worker);
                        }
                        if (status != EXPECTED || stage.credentials != caller || vfs::vfs_stat(FILE, &after) != 0) return fail(__LINE__);
                        if (ERROR != 0 && (output != SENTINEL || std::memcmp(&before, &after, sizeof(before)) != 0)) return fail(__LINE__);
                        if (disk) {
                            std::array<char, 8> value{};
                            auto const READ = vfs::vfs_getxattr(FILE, "user.matrix", value.data(), value.size(), true);
                            if (ERROR == 0 && operation == vfs::XattrOperation::REMOVE) {
                                if (READ != -ENODATA) return fail(__LINE__);
                            } else if (READ != 4 ||
                                       std::memcmp(value.data(), ERROR == 0 && operation == vfs::XattrOperation::SET ? "next" : "seed",
                                                   4) != 0) {
                                return fail(__LINE__);
                            }
                            if (ERROR == 0 && operation == vfs::XattrOperation::GET && std::memcmp(output.data(), "seed", 4) != 0)
                                return fail(__LINE__);
                            if (ERROR == 0 && operation == vfs::XattrOperation::LIST && std::strcmp(output.data(), "user.matrix") != 0)
                                return fail(__LINE__);
                        }
                    }
                }
            if (vfs::vfs_unlink(LINK) != 0) return fail(__LINE__);
        }
    }
    return true;
}
#endif

// Runs only from the pre-admitted deferred VFS worker. RX merely copied the
// bounded frame into a fixed pool and never allocates, blocks, or touches VFS.
void handle_vfs_xattr_op(const WkiHeader* hdr, const WkiChannelIdentity& channel_identity, const char* export_path, const char* export_name,
                         const uint8_t* data, uint16_t data_len, uint64_t identity_policy, uint16_t identity_capabilities) {
    uint16_t const COOKIE = static_cast<uint16_t>(hdr->seq_num & UINT16_MAX);
    if (data_len < sizeof(VfsXattrPhaseHeader)) {
        VfsXattrPhaseHeader invalid = {};
        send_vfs_xattr_response(channel_identity, COOKIE, invalid, -EINVAL, 0, 0);
        return;
    }
    VfsXattrPhaseHeader identity = {};
    memcpy(&identity, data, sizeof(identity));
    if (identity.version != WKI_VFS_XATTR_VERSION) {
        send_vfs_xattr_response(channel_identity, COOKIE, identity, -ENOTSUP, 0, 0);
        return;
    }
    if (!wki_vfs_xattr_header_valid(identity)) {
        send_vfs_xattr_response(channel_identity, COOKIE, identity, -EINVAL, 0, 0);
        return;
    }
    VfsXattrStageKey const KEY = vfs_xattr_stage_key(channel_identity);

    if (identity.phase == VfsXattrPhase::BEGIN) {
        static_assert(WKI_VFS_XATTR_CALLER_SIZE == ker::mod::security::credential_record::SIZE);
        constexpr size_t PREFIX = sizeof(VfsXattrBeginPayload) + WKI_VFS_XATTR_CALLER_SIZE;
        if (data_len < PREFIX) {
            send_vfs_xattr_response(channel_identity, COOKIE, identity, -EINVAL, 0, 0);
            return;
        }
        VfsXattrBeginPayload begin = {};
        memcpy(&begin, data, sizeof(begin));
        size_t const EXPECTED = PREFIX + begin.path_len + begin.name_len;
        bool const VALID_FLAGS = begin.header.operation == VfsXattrOperation::SET
                                     ? (begin.flags == 0 || begin.flags == 1 || begin.flags == 2)
                                     : begin.flags == 0;
        if (!wki_vfs_xattr_begin_valid(begin) || !VALID_FLAGS || EXPECTED != data_len) {
            send_vfs_xattr_response(channel_identity, COOKIE, identity, -EINVAL, 0, 0);
            return;
        }
        if (identity.target == VfsXattrTarget::PATH) {
            int const PATH_STATUS =
                mapped_wire_path_status(data + PREFIX, begin.path_len, (identity_capabilities & WKI_CAP_VFS_COMPONENT_PATHS) != 0);
            if (PATH_STATUS != 0) {
                send_vfs_xattr_response(channel_identity, COOKIE, identity, PATH_STATUS, 0, 0);
                return;
            }
        }
        ker::mod::security::credential_record::Record caller_record{};
        std::memcpy(caller_record.data(), data + sizeof(begin), caller_record.size());
        ker::mod::security::Snapshot supplied, mapped;
        int const DECODE = ker::mod::security::credential_record::decode(caller_record, supplied);
        bool const READ = identity.operation == VfsXattrOperation::GET || identity.operation == VfsXattrOperation::LIST;
        int const ADMISSION = DECODE != 0 ? DECODE
                              : READ      ? auth_policy::map_vfs_read_identity(identity_policy, supplied, mapped)
                                          : auth_policy::map_vfs_mutation_identity(identity_policy, supplied, mapped);
        if (ADMISSION != 0) {
            send_vfs_xattr_response(channel_identity, COOKIE, identity, ADMISSION, 0, 0);
            return;
        }
        std::array<char, 512> relative_path{};
        std::array<char, VFS_EXPORT_PATH_LEN> captured_root{}, captured_name{};
        if (begin.header.target == VfsXattrTarget::PATH) {
            int const CAPTURE = capture_vfs_xattr_export(export_path, export_name, captured_root, captured_name);
            if (CAPTURE != 0 || begin.path_len >= relative_path.size()) {
                send_vfs_xattr_response(channel_identity, COOKIE, identity, CAPTURE != 0 ? CAPTURE : -ENAMETOOLONG, 0, 0);
                return;
            }
            memcpy(relative_path.data(), data + PREFIX, begin.path_len);
        }
        std::array<char, WKI_VFS_XATTR_NAME_MAX + 1> name{};
        if (begin.name_len != 0) {
            auto const* wire_name = data + PREFIX + begin.path_len;
            if (memchr(wire_name, '\0', begin.name_len) != nullptr) {
                send_vfs_xattr_response(channel_identity, COOKIE, identity, -EINVAL, 0, 0);
                return;
            }
            memcpy(name.data(), wire_name, begin.name_len);
            if (!remote_xattr_name_allowed(name.data())) {
                send_vfs_xattr_response(channel_identity, COOKIE, identity, -EOPNOTSUPP, 0, 0);
                return;
            }
        }

        VfsXattrServerStage* prior_stage = nullptr;
        s_vfs_lock.lock();
        auto existing = g_vfs_xattr_stages.find(KEY);
        if (existing != g_vfs_xattr_stages.end()) {
            auto& old = *existing->second;
            bool const SAME_ID = old.identity.session_id == identity.session_id && old.identity.operation_id == identity.operation_id;
            if (SAME_ID) {
                bool const EXACT =
                    vfs_xattr_begin_is_exact_replay(old, begin, caller_record, relative_path, name, captured_root, captured_name);
                int const STATUS = EXACT ? old.terminal_status : -EPROTO;
                uint32_t const TOTAL = old.total;
                uint32_t const TRANSFERRED = old.transferred;
                s_vfs_lock.unlock();
                send_vfs_xattr_response(channel_identity, COOKIE, identity, STATUS, TOTAL, TRANSFERRED);
                return;
            }
            int const TRANSITION_STATUS = vfs_xattr_new_begin_status(old, identity);
            if (TRANSITION_STATUS != 0) {
                s_vfs_lock.unlock();
                send_vfs_xattr_response(channel_identity, COOKIE, identity, TRANSITION_STATUS, 0, 0);
                return;
            }
            prior_stage = existing->second;
            retain_vfs_xattr_stage(prior_stage);
        }
        if (prior_stage == nullptr && g_vfs_xattr_stages.size() >= VFS_XATTR_SERVER_STAGE_COUNT_MAX) {
            s_vfs_lock.unlock();
            send_vfs_xattr_response(channel_identity, COOKIE, identity, -EAGAIN, 0, 0);
            return;
        }
        s_vfs_lock.unlock();
        VfsXattrStageRefGuard const PRIOR_STAGE_GUARD{prior_stage};
        if (prior_stage != nullptr && prior_stage->staged_bytes != 0) {
            if (!prior_stage->committed) {
                prior_stage->terminal_status = -ECANCELED;
                prior_stage->committed = true;
            }
            release_vfs_xattr_stage_data(*prior_stage);
        }

        auto* stage = new (std::nothrow) VfsXattrServerStage();
        if (stage == nullptr) {
            send_vfs_xattr_response(channel_identity, COOKIE, identity, -ENOMEM, 0, 0);
            return;
        }
        stage->identity = identity;
        stage->credentials = mapped;
        stage->caller_record = caller_record;
        stage->remote_fd = begin.remote_fd;
        stage->require_directory = begin.require_directory != 0;
        stage->flags = begin.flags;
        stage->requested_size = begin.data_len;
        stage->follow_final_symlink = begin.follow_final_symlink != 0;
        stage->export_root = captured_root;
        stage->export_name = captured_name;
        stage->path = relative_path;
        stage->name = name;
        if (begin.remote_fd >= 0) {
            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(channel_identity, begin.remote_fd);
            if (rfd != nullptr && rfd->file != nullptr) {
                stage->file = rfd->file;
                ker::vfs::vfs_retain_file(stage->file);
            }
            s_vfs_lock.unlock();
            if (stage->file == nullptr) {
                release_vfs_xattr_stage(stage);
                send_vfs_xattr_response(channel_identity, COOKIE, identity, -EBADF, 0, 0);
                return;
            }
        }

        if (identity.target == VfsXattrTarget::PATH && stage->file != nullptr && !stage->file->is_directory) {
            release_vfs_xattr_stage(stage);
            send_vfs_xattr_response(channel_identity, COOKIE, identity, -ENOTDIR, 0, 0);
            return;
        }

        ssize_t measured = static_cast<ssize_t>(begin.data_len);
        if (identity.operation == VfsXattrOperation::GET) {
            measured = vfs_xattr_stage_call_get(*stage, nullptr, 0);
        } else if (identity.operation == VfsXattrOperation::LIST) {
            measured = vfs_xattr_stage_call_list(*stage, nullptr, 0);
        }
        if (measured < 0 || static_cast<uint64_t>(measured) > WKI_VFS_XATTR_DATA_MAX) {
            int const STATUS = measured < 0 ? static_cast<int>(measured) : -E2BIG;
            release_vfs_xattr_stage(stage);
            send_vfs_xattr_response(channel_identity, COOKIE, identity, STATUS, 0, 0);
            return;
        }
        stage->total = static_cast<uint32_t>(measured);
        if (stage->total != 0) {
            bool const BUDGET_OK = reserve_vfs_xattr_stage_bytes(stage->total);
            if (!BUDGET_OK) {
                release_vfs_xattr_stage(stage);
                send_vfs_xattr_response(channel_identity, COOKIE, identity, -EAGAIN, 0, 0);
                return;
            }
            stage->staged_bytes = stage->total;
            stage->data.reset(new (std::nothrow) uint8_t[stage->total]);
            if (stage->data == nullptr) {
                release_vfs_xattr_stage(stage);
                send_vfs_xattr_response(channel_identity, COOKIE, identity, -ENOMEM, 0, 0);
                return;
            }
        }
        if (identity.operation == VfsXattrOperation::GET && stage->total != 0) {
            measured = vfs_xattr_stage_call_get(*stage, stage->data.get(), stage->total);
        } else if (identity.operation == VfsXattrOperation::LIST && stage->total != 0) {
            measured = vfs_xattr_stage_call_list(*stage, reinterpret_cast<char*>(stage->data.get()), stage->total);
            if (measured >= 0 && static_cast<uint64_t>(measured) <= stage->staged_bytes) {
                measured = filter_remote_xattr_list(stage->data.get(), static_cast<size_t>(measured));
            }
        }
        if (measured < 0 || static_cast<uint64_t>(measured) > stage->staged_bytes) {
            if (measured >= 0) {
                measured = -ERANGE;
            }
            release_vfs_xattr_stage(stage);
            send_vfs_xattr_response(channel_identity, COOKIE, identity, static_cast<int>(measured), 0, 0);
            return;
        }
        if (identity.operation == VfsXattrOperation::GET || identity.operation == VfsXattrOperation::LIST) {
            stage->total = static_cast<uint32_t>(measured);
        }
        uint32_t const TOTAL = stage->total;
        if (!wki_channel_generation_is_live(channel_identity.channel, channel_identity.peer_node_id, channel_identity.channel_id,
                                            channel_identity.generation)) {
            release_vfs_xattr_stage(stage);
            send_vfs_xattr_response(channel_identity, COOKIE, identity, -ENOTCONN, 0, 0);
            return;
        }
        // Preallocate a possible map node in worker context before taking the
        // VFS registry spinlock. Replacement and insertion are allocation-free.
        std::map<VfsXattrStageKey, VfsXattrServerStage*> prepared_stage;
        prepared_stage.emplace(KEY, stage);
        auto prepared_node = prepared_stage.extract(KEY);
        bool installed = false;
        int install_status = -EBUSY;
        VfsXattrServerStage* replaced_stage = nullptr;
        s_vfs_lock.lock();
        auto current = g_vfs_xattr_stages.find(KEY);
        if (prior_stage != nullptr && current != g_vfs_xattr_stages.end() && current->second == prior_stage) {
            current->second = stage;
            replaced_stage = prior_stage;
            installed = true;
        } else if (prior_stage == nullptr && current == g_vfs_xattr_stages.end() &&
                   g_vfs_xattr_stages.size() < VFS_XATTR_SERVER_STAGE_COUNT_MAX) {
            installed = g_vfs_xattr_stages.insert(std::move(prepared_node)).inserted;
        } else if (prior_stage == nullptr && current == g_vfs_xattr_stages.end()) {
            install_status = -EAGAIN;
        }
        s_vfs_lock.unlock();
        release_vfs_xattr_stage(replaced_stage);
        if (!installed) {
            release_vfs_xattr_stage(stage);
            send_vfs_xattr_response(channel_identity, COOKIE, identity, install_status, 0, 0);
            return;
        }
        send_vfs_xattr_response(channel_identity, COOKIE, identity, 0, TOTAL, 0);
        return;
    }

    s_vfs_lock.lock();
    auto found = g_vfs_xattr_stages.find(KEY);
    VfsXattrServerStage* stage = found != g_vfs_xattr_stages.end() ? found->second : nullptr;
    retain_vfs_xattr_stage(stage);
    VfsXattrStageRefGuard const STAGE_GUARD{stage};
    if (stage == nullptr || stage->identity.session_id != identity.session_id || stage->identity.operation_id != identity.operation_id ||
        stage->identity.operation != identity.operation || stage->identity.target != identity.target) {
        s_vfs_lock.unlock();
        send_vfs_xattr_response(channel_identity, COOKIE, identity, -ESTALE, 0, 0);
        return;
    }
    s_vfs_lock.unlock();

    if (identity.phase == VfsXattrPhase::DATA) {
        if (stage->committed) {
            send_vfs_xattr_response(channel_identity, COOKIE, identity, -ESTALE, stage->total, stage->transferred);
            return;
        }
        if (data_len < sizeof(VfsXattrDataPayload)) {
            send_vfs_xattr_response(channel_identity, COOKIE, identity, -EINVAL, stage->total, stage->transferred);
            return;
        }
        VfsXattrDataPayload chunk = {};
        memcpy(&chunk, data, sizeof(chunk));
        bool const READ = identity.operation == VfsXattrOperation::GET || identity.operation == VfsXattrOperation::LIST;
        size_t const EXPECTED = sizeof(chunk) + (READ ? 0 : chunk.chunk_len);
        if (chunk.reserved != 0 || chunk.chunk_len == 0 || chunk.chunk_len > WKI_VFS_XATTR_MAX_DATA_CHUNK || EXPECTED != data_len ||
            chunk.offset > stage->total || chunk.chunk_len > stage->total - chunk.offset) {
            send_vfs_xattr_response(channel_identity, COOKIE, identity, -EINVAL, stage->total, stage->transferred);
            return;
        }
        if (READ) {
            send_vfs_xattr_response(channel_identity, COOKIE, identity, 0, stage->total, chunk.chunk_len, stage->data.get() + chunk.offset,
                                    chunk.chunk_len);
            return;
        }
        auto const* incoming = data + sizeof(chunk);
        if (chunk.offset < stage->transferred) {
            bool const EXACT_REPLAY = vfs_xattr_data_is_exact_replay(*stage, chunk.offset, incoming, chunk.chunk_len);
            send_vfs_xattr_response(channel_identity, COOKIE, identity, EXACT_REPLAY ? 0 : -EPROTO, stage->total, stage->transferred);
            return;
        }
        if (chunk.offset != stage->transferred) {
            send_vfs_xattr_response(channel_identity, COOKIE, identity, -EPROTO, stage->total, stage->transferred);
            return;
        }
        memcpy(stage->data.get() + chunk.offset, incoming, chunk.chunk_len);
        stage->transferred += chunk.chunk_len;
        send_vfs_xattr_response(channel_identity, COOKIE, identity, 0, stage->total, stage->transferred);
        return;
    }

    if (data_len != sizeof(VfsXattrPhaseHeader)) {
        send_vfs_xattr_response(channel_identity, COOKIE, identity, -EINVAL, stage->total, stage->transferred);
        return;
    }
    if (identity.phase == VfsXattrPhase::COMMIT) {
        if (!stage->committed) {
            if (identity.operation == VfsXattrOperation::SET && stage->transferred != stage->total) {
                send_vfs_xattr_response(channel_identity, COOKIE, identity, -ENODATA, stage->total, stage->transferred);
                return;
            }
            stage->terminal_status = static_cast<int16_t>(vfs_xattr_stage_apply_mutation(*stage, ker::mod::sched::get_current_task()));
            stage->committed = true;
            release_vfs_xattr_stage_data(*stage);
        }
        send_vfs_xattr_response(channel_identity, COOKIE, identity, stage->terminal_status, stage->total, stage->transferred);
        return;
    }

    if (!stage->committed) {
        // Keep a terminal tombstone so the session and monotonic operation
        // high-water remain bound to this channel generation after ABORT.
        stage->terminal_status = -ECANCELED;
        stage->committed = true;
        release_vfs_xattr_stage_data(*stage);
    }
    send_vfs_xattr_response(channel_identity, COOKIE, identity, 0, 0, 0);
}

}  // namespace

namespace detail {

// NOLINTNEXTLINE(readability-function-size): Protocol opcode dispatcher.
void handle_vfs_op(const WkiHeader* hdr, const WkiChannelIdentity& channel_identity, const char* export_path, const char* export_name,
                   uint16_t op_id, const uint8_t* data, uint16_t data_len, uint64_t identity_policy, uint16_t identity_capabilities,
                   const std::atomic<bool>* binding_retiring, DevServerBinding* grant_binding) {
    bool const COMPONENT_PATHS = (identity_capabilities & WKI_CAP_VFS_COMPONENT_PATHS) != 0;
    if (!vfs_channel_identity_matches_header(hdr, channel_identity) || export_path == nullptr || export_name == nullptr) {
        return;
    }

    // Immutable name from the retained authenticated export binding.
    // Relative named exports preserve physical owner-link semantics.
    const char* absolute_symlink_root = export_name[0] == '/' ? export_name : nullptr;

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

    auto send_reply = [&](const void* response, uint16_t bytes) {
        int const RESULT = send_completed_vfs_reply(channel_identity, response, bytes, binding_retiring);
        if (RESULT < 0 && g_vfs_reply_enqueue_failures.fetch_add(1, std::memory_order_relaxed) < 16) {
            ker::mod::dbg::logger<"wki">::warn("VFS reply enqueue failed: peer=0x%04x ch=%u gen=%u op=%u cookie=%u bytes=%u result=%d",
                                               hdr->src_node, channel_id, channel_identity.generation, op_id, REQ_COOKIE, bytes, RESULT);
        }
    };
    auto send_simple_resp = [&](DevOpRespPayload& resp) {
        resp.reserved = REQ_COOKIE;
        perf_record_vfs_server_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::REPLY_SEND), hdr->src_node, channel_id,
                                     CORRELATION, resp.status, sizeof(resp), CALLSITE);
        send_reply(&resp, sizeof(resp));
    };
    auto send_buffered_resp = [&](void* resp_buf, uint16_t resp_len) {
        if (resp_buf != nullptr && resp_len >= sizeof(DevOpRespPayload)) {
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
            resp->reserved = REQ_COOKIE;
            perf_record_vfs_server_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsServerOp::REPLY_SEND), hdr->src_node, channel_id,
                                         CORRELATION, resp->status, resp_len, CALLSITE);
        }
        send_reply(resp_buf, resp_len);
    };
    auto reject_request = [&](int16_t status) {
        DevOpRespPayload resp = {};
        resp.op_id = op_id;
        resp.status = status;
        resp.data_len = 0;
        send_simple_resp(resp);
    };

    uint16_t effective_op = op_id;
    uint64_t ordinary_token = 0;
    uint64_t ordinary_binding_id = 0;
    if (op_id == OP_VFS_OPEN_LIFETIME) {
        VfsOpenLifetimeRequest lifetime{};
        if (data == nullptr || data_len < sizeof(lifetime)) {
            reject_request(-EINVAL);
            return;
        }
        std::memcpy(&lifetime, data, sizeof(lifetime));
        if (!wki_vfs_open_lifetime_request_valid(lifetime, data_len)) {
            reject_request(-EINVAL);
            return;
        }
        if (grant_binding == nullptr || (identity_capabilities & WKI_CAP_RESOURCE_INCARNATION) == 0) {
            reject_request(-ENOTSUP);
            return;
        }
        if (lifetime.inner_op == OP_VFS_FEATURES) {
            VfsOpenLifetimeRequest result{};
            int const STATUS = query_ordinary_open_binding(*grant_binding, channel_identity, lifetime.token, result);
            if (STATUS != 0) {
                reject_request(static_cast<int16_t>(STATUS));
                return;
            }
            wki_remote_vfs_prepare_secure_binding(*grant_binding);
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(result)> response{};
            DevOpRespPayload const header{.op_id = op_id, .status = 0, .data_len = sizeof(result), .reserved = REQ_COOKIE};
            std::memcpy(response.data(), &header, sizeof(header));
            std::memcpy(response.data() + sizeof(header), &result, sizeof(result));
            send_buffered_resp(response.data(), response.size());
            return;
        }
        if (lifetime.inner_op == 0) {
            reject_request(static_cast<int16_t>(cancel_ordinary_open(*grant_binding, channel_identity, lifetime)));
            return;
        }
        data += sizeof(lifetime);
        data_len = lifetime.data_len;
        effective_op = lifetime.inner_op;
        if (effective_op == OP_VFS_PATH_AS) {
            constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
            VfsPathIdentityHeader path{};
            if (data_len < RECORD + sizeof(path)) {
                reject_request(-EINVAL);
                return;
            }
            std::memcpy(&path, data + RECORD, sizeof(path));
            if (path.operation != VfsPathOperation::OPEN_AT) {
                reject_request(-EINVAL);
                return;
            }
        }
        int const ADMITTED = admit_ordinary_open_token(*grant_binding, channel_identity, lifetime);
        if (ADMITTED != 0) {
            reject_request(static_cast<int16_t>(ADMITTED));
            return;
        }
        ordinary_token = lifetime.token;
        ordinary_binding_id = lifetime.binding_id;
        wki_remote_vfs_prepare_secure_binding(*grant_binding);
    }

    switch (effective_op) {
        case OP_VFS_LOCK_SCOPE: {
            VfsOwnerScopeRequest request{};
            if (data == nullptr || data_len != sizeof(request)) {
                reject_request(-EINVAL);
                break;
            }
            std::memcpy(&request, data, sizeof(request));
            if (!wki_vfs_owner_scope_request_valid(request, data_len)) {
                reject_request(-EINVAL);
                break;
            }
            if (grant_binding == nullptr || (identity_capabilities & WKI_CAP_RESOURCE_INCARNATION) == 0) {
                reject_request(-EOPNOTSUPP);
                break;
            }
            VfsOwnerScopeResponse result{};
            int const STATUS = perform_server_advisory_scope(*grant_binding, channel_identity, request, result);
            if (STATUS != 0) {
                reject_request(static_cast<int16_t>(STATUS));
                break;
            }
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(result)> response{};
            DevOpRespPayload const header{.op_id = op_id, .status = 0, .data_len = sizeof(result), .reserved = REQ_COOKIE};
            std::memcpy(response.data(), &header, sizeof(header));
            std::memcpy(response.data() + sizeof(header), &result, sizeof(result));
            send_buffered_resp(response.data(), response.size());
            break;
        }
        case OP_VFS_LOCK_OWNER: {
            VfsOwnerLockRequest request{};
            if (data == nullptr || data_len != sizeof(request)) {
                reject_request(-EINVAL);
                break;
            }
            std::memcpy(&request, data, sizeof(request));
            if (!wki_vfs_owner_lock_request_valid(request, data_len)) {
                reject_request(-EINVAL);
                break;
            }
            if (grant_binding == nullptr || (identity_capabilities & WKI_CAP_RESOURCE_INCARNATION) == 0) {
                reject_request(-EOPNOTSUPP);
                break;
            }
            VfsOwnerLockResponse result{};
            int const STATUS = perform_server_advisory(*grant_binding, channel_identity, request, result);
            if (STATUS != 0) {
                reject_request(static_cast<int16_t>(STATUS));
                break;
            }
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(result)> response{};
            DevOpRespPayload const header{.op_id = op_id, .status = 0, .data_len = sizeof(result), .reserved = REQ_COOKIE};
            std::memcpy(response.data(), &header, sizeof(header));
            std::memcpy(response.data() + sizeof(header), &result, sizeof(result));
            send_buffered_resp(response.data(), response.size());
            break;
        }
        case OP_VFS_VERSION: {
            VfsOwnerFdRequest request{};
            if (data == nullptr || data_len != sizeof(request)) {
                reject_request(-EINVAL);
                break;
            }
            std::memcpy(&request, data, sizeof(request));
            if (!wki_vfs_owner_fd_request_valid(request, data_len)) {
                reject_request(-EINVAL);
                break;
            }
            if (grant_binding == nullptr || (identity_capabilities & WKI_CAP_RESOURCE_INCARNATION) == 0) {
                reject_request(-EOPNOTSUPP);
                break;
            }
            ker::vfs::File* file = nullptr;
            int result = retain_server_owner_file(*grant_binding, channel_identity, request, file);
            ker::vfs::coherency::Stamp stamp{};
            if (result == 0) {
                result = ker::vfs::vfs_file_coherency_snapshot(file, stamp);
                ker::vfs::vfs_put_file(file);
            }
            if (result != 0) {
                reject_request(static_cast<int16_t>(result));
                break;
            }
            VfsVersionResponse const version{.query_id = request.query_id,
                                             .object = stamp.object,
                                             .content = stamp.content,
                                             .metadata = stamp.metadata,
                                             .directory = stamp.directory};
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(version)> response{};
            DevOpRespPayload const header{.op_id = op_id, .status = 0, .data_len = sizeof(version), .reserved = REQ_COOKIE};
            std::memcpy(response.data(), &header, sizeof(header));
            std::memcpy(response.data() + sizeof(header), &version, sizeof(version));
            send_buffered_resp(response.data(), response.size());
            break;
        }
        case OP_VFS_WRITE_COMMITTED:
        case OP_VFS_READ_OWNER: {
            bool const WRITE = effective_op == OP_VFS_WRITE_COMMITTED;
            VfsOwnerIoRequest request{};
            if (data == nullptr || data_len < sizeof(request)) {
                reject_request(-EINVAL);
                break;
            }
            std::memcpy(&request, data, sizeof(request));
            if (!wki_vfs_owner_io_request_valid(request, data_len, WRITE)) {
                reject_request(-EINVAL);
                break;
            }
            if (grant_binding == nullptr || (identity_capabilities & WKI_CAP_RESOURCE_INCARNATION) == 0) {
                reject_request(-EOPNOTSUPP);
                break;
            }
            std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> response{};
            VfsAppendResponse written{};
            VfsOwnerReadResponse read{};
            int const STATUS = WRITE ? perform_server_write(*grant_binding, channel_identity, request, data + sizeof(request), written)
                                     : perform_server_read(*grant_binding, channel_identity, request,
                                                           response.data() + sizeof(DevOpRespPayload) + sizeof(read), read);
            if (STATUS != 0) {
                reject_request(static_cast<int16_t>(STATUS));
                break;
            }
            uint16_t const REPLY_LENGTH = WRITE ? sizeof(written) : sizeof(read) + read.bytes;
            DevOpRespPayload const header{.op_id = op_id, .status = 0, .data_len = REPLY_LENGTH, .reserved = REQ_COOKIE};
            std::memcpy(response.data(), &header, sizeof(header));
            if (WRITE)
                std::memcpy(response.data() + sizeof(header), &written, sizeof(written));
            else
                std::memcpy(response.data() + sizeof(header), &read, sizeof(read));
            send_buffered_resp(response.data(), sizeof(header) + REPLY_LENGTH);
            break;
        }
        case OP_VFS_APPEND: {
            VfsAppendRequest request{};
            if (data == nullptr || data_len < sizeof(request)) {
                reject_request(-EINVAL);
                break;
            }
            std::memcpy(&request, data, sizeof(request));
            if (!wki_vfs_append_request_valid(request, data_len)) {
                reject_request(-EINVAL);
                break;
            }
            if (grant_binding == nullptr || (identity_capabilities & WKI_CAP_RESOURCE_INCARNATION) == 0) {
                reject_request(-EOPNOTSUPP);
                break;
            }
            VfsAppendResponse append{};
            int const STATUS = perform_server_append(*grant_binding, channel_identity, request, data + sizeof(request), append);
            if (STATUS != 0) {
                reject_request(static_cast<int16_t>(STATUS));
                break;
            }
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(append)> response{};
            DevOpRespPayload const header{.op_id = op_id, .status = 0, .data_len = sizeof(append), .reserved = REQ_COOKIE};
            std::memcpy(response.data(), &header, sizeof(header));
            std::memcpy(response.data() + sizeof(header), &append, sizeof(append));
            send_buffered_resp(response.data(), response.size());
            break;
        }
        case OP_VFS_DIRECTORY_GRANT: {
            if ((identity_capabilities & WKI_CAP_RESOURCE_INCARNATION) == 0 || grant_binding == nullptr) {
                reject_request(-ENOTSUP);
                break;
            }
            VfsDirectoryGrantRequest request{};
            if (data == nullptr || data_len != sizeof(request) + WKI_VFS_DIRECTORY_CALLER_SIZE) {
                reject_request(-EINVAL);
                break;
            }
            std::memcpy(&request, data, sizeof(request));
            if (!wki_vfs_directory_grant_request_valid(request, data_len)) {
                reject_request(-EINVAL);
                break;
            }
            ker::mod::security::Snapshot supplied, mapped;
            int status = ker::mod::security::credential_record::decode({data + sizeof(request), WKI_VFS_DIRECTORY_CALLER_SIZE}, supplied);
            if (status == 0) status = auth_policy::map_vfs_read_identity(identity_policy, supplied, mapped);
            auto* task = ker::mod::sched::get_current_task();
            if (status == 0 && task == nullptr) status = -ESRCH;
            if (status != 0) {
                reject_request(static_cast<int16_t>(status));
                break;
            }
            VfsDirectoryGrantResponse result{};
            std::array<char, 512> name{};
            status = directory_grant_owner(*task, mapped, *grant_binding, request, result, name);
            if (status != 0) {
                reject_request(static_cast<int16_t>(status));
                break;
            }
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(result) + 511> response{};
            DevOpRespPayload const header{.op_id = OP_VFS_DIRECTORY_GRANT,
                                          .status = 0,
                                          .data_len = static_cast<uint16_t>(sizeof(result) + result.directory.path_length),
                                          .reserved = REQ_COOKIE};
            std::memcpy(response.data(), &header, sizeof(header));
            std::memcpy(response.data() + sizeof(header), &result, sizeof(result));
            std::memcpy(response.data() + sizeof(header) + sizeof(result), name.data(), result.directory.path_length);
            int const SENT = wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, response.data(),
                                                          static_cast<uint16_t>(sizeof(header) + header.data_len));
            if (SENT != WKI_OK) static_cast<void>(cancel_directory_pin(channel_identity, request.pin_token));
            break;
        }
        case OP_VFS_DIRECTORY_REFERENCE: {
            VfsDirectoryRequest request{};
            if (data == nullptr || data_len < sizeof(request)) {
                reject_request(-EINVAL);
                break;
            }
            std::memcpy(&request, data, sizeof(request));
            if (!wki_vfs_directory_request_valid(request, data_len)) {
                reject_request(-EINVAL);
                break;
            }
            if (request.operation == VfsDirectoryOperation::CANCEL) {
                reject_request(static_cast<int16_t>(cancel_directory_pin(channel_identity, request.token)));
                break;
            }
            static_assert(WKI_VFS_DIRECTORY_CALLER_SIZE == ker::mod::security::credential_record::SIZE);
            ker::mod::security::Snapshot supplied, mapped;
            int status = ker::mod::security::credential_record::decode({data + sizeof(request), WKI_VFS_DIRECTORY_CALLER_SIZE}, supplied);
            if (status == 0) status = auth_policy::map_vfs_read_identity(identity_policy, supplied, mapped);
            auto* task = ker::mod::sched::get_current_task();
            if (status == 0 && task == nullptr) status = -ESRCH;
            if (status != 0) {
                reject_request(static_cast<int16_t>(status));
                break;
            }
            VfsDirectoryResponse result{};
            std::array<char, 512> name{};
            status = directory_reference_owner(*task, mapped, channel_identity, export_path, export_name, request,
                                               data + sizeof(request) + WKI_VFS_DIRECTORY_CALLER_SIZE, COMPONENT_PATHS, result, name);
            if (status != 0) {
                reject_request(static_cast<int16_t>(status));
                break;
            }
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(result) + 511> response{};
            DevOpRespPayload const header{.op_id = OP_VFS_DIRECTORY_REFERENCE,
                                          .status = 0,
                                          .data_len = static_cast<uint16_t>(sizeof(result) + result.path_length),
                                          .reserved = REQ_COOKIE};
            std::memcpy(response.data(), &header, sizeof(header));
            std::memcpy(response.data() + sizeof(header), &result, sizeof(result));
            std::memcpy(response.data() + sizeof(header) + sizeof(result), name.data(), result.path_length);
            int const SENT = wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, response.data(),
                                                          static_cast<uint16_t>(sizeof(header) + header.data_len));
            if (SENT != WKI_OK && request.operation == VfsDirectoryOperation::PIN)
                static_cast<void>(cancel_directory_pin(channel_identity, request.token));
            break;
        }
        case OP_VFS_EXEC_VIEW: {
            VfsExecViewRequest request{};
            if (data == nullptr || data_len != sizeof(request)) {
                reject_request(-EINVAL);
                break;
            }
            std::memcpy(&request, data, sizeof(request));
            if (!wki_vfs_exec_view_request_valid(request)) {
                reject_request(-EINVAL);
                break;
            }
            if (request.operation == VfsExecViewOperation::CANCEL) {
                reject_request(static_cast<int16_t>(cancel_exec_view(channel_identity, request.token)));
                break;
            }
            VfsExecViewResponse result{};
            int const STATUS = pin_exec_view(channel_identity, request, result);
            if (STATUS != 0) {
                reject_request(static_cast<int16_t>(STATUS));
                break;
            }
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(result)> response{};
            auto* header = reinterpret_cast<DevOpRespPayload*>(response.data());
            header->op_id = OP_VFS_EXEC_VIEW;
            header->data_len = sizeof(result);
            header->reserved = REQ_COOKIE;
            std::memcpy(response.data() + sizeof(*header), &result, sizeof(result));
            int const SENT = wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, response.data(), response.size());
            if (SENT != WKI_OK) static_cast<void>(cancel_exec_view(channel_identity, request.token));
            break;
        }
        case OP_VFS_XATTR: {
            handle_vfs_xattr_op(hdr, channel_identity, export_path, export_name, data, data_len, identity_policy, identity_capabilities);
            break;
        }
        case OP_VFS_OPEN:
            // Legacy OPEN cannot represent the caller and must never borrow
            // the serving worker's credentials, even for a trusted peer.
            reject_request(-ENOTSUP);
            break;
        case OP_VFS_OPEN_READ_AS:
        case OP_VFS_OPEN_AS: {
            bool const REGULAR_ONLY = effective_op == OP_VFS_OPEN_READ_AS;
            ker::mod::security::Snapshot mapped_credentials;
            {
                uint16_t const REQUIRED_CAPABILITY = REGULAR_ONLY ? WKI_CAP_VFS_READ_IDENTITY : WKI_CAP_VFS_OPEN_IDENTITY;
                if ((identity_capabilities & REQUIRED_CAPABILITY) == 0) {
                    reject_request(-ENOTSUP);
                    break;
                }
                constexpr size_t RECORD_SIZE = ker::mod::security::credential_record::SIZE;
                if (data == nullptr || data_len < RECORD_SIZE + OPEN_REQ_BASE_LEN) {
                    reject_request(-EINVAL);
                    break;
                }
                ker::mod::security::Snapshot supplied;
                int result = ker::mod::security::credential_record::decode({data, RECORD_SIZE}, supplied);
                if (result == 0) {
                    uint32_t request_flags = 0;
                    std::memcpy(&request_flags, data + RECORD_SIZE, sizeof(request_flags));
                    result = REGULAR_ONLY
                                 ? auth_policy::map_vfs_read_identity(identity_policy, supplied, mapped_credentials)
                                 : auth_policy::map_vfs_open_identity(identity_policy, supplied, request_flags, mapped_credentials);
                }
                if (result != 0) {
                    reject_request(static_cast<int16_t>(result));
                    break;
                }
                data += RECORD_SIZE;
                data_len -= RECORD_SIZE;
            }
            // Request: {flags:u32, mode:u32, path_len:u16, path[path_len], optional prefetch_rkey:u32, prefetch_len:u32}
            if (data_len < OPEN_REQ_BASE_LEN) {
                DevOpRespPayload resp = {};
                resp.op_id = op_id;
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
            bool const HAS_PREFETCH = data_len == OPEN_PATH_END + OPEN_PREFETCH_REQ_LEN;
            // Explicit identity grants only a regular-file read. Do not allow
            // creation, mutation, directory/device opens or pre-publication
            // RDMA data exposure through this operation.
            if (REGULAR_ONLY && (flags != 0 || mode != 0 || data_len != OPEN_PATH_END)) {
                reject_request(-EINVAL);
                break;
            }
            if (!wire_payload_has_optional_exact_suffix(data_len, OPEN_PATH_END, OPEN_PREFETCH_REQ_LEN)) {
                reject_request(-EINVAL);
                break;
            }

            if (HAS_PREFETCH && ker::mod::security::authorize_file_description_access(flags & 3U, false) != 0) {
                reject_request(-EINVAL);
                break;
            }

            uint32_t prefetch_rkey = 0;
            uint32_t prefetch_len = 0;
            if (HAS_PREFETCH) {
                memcpy(&prefetch_rkey, data + OPEN_PATH_END, sizeof(uint32_t));
                memcpy(&prefetch_len, data + OPEN_PATH_END + sizeof(uint32_t), sizeof(uint32_t));
                prefetch_len = std::min<uint32_t>(prefetch_len, VFS_RDMA_BULK_SIZE);
            }

            // Build full path: export_path + "/" + relative_path
            std::array<char, 512> full_path __attribute__((uninitialized));          // NOLINT(cppcoreguidelines-pro-type-member-init)
            std::array<char, 512> full_visible_path __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
            int const PATH_RET =
                prepare_mapped_scalar_path(export_path, export_name, data + OPEN_REQ_BASE_LEN, path_len, COMPONENT_PATHS, full_path.data(),
                                           full_path.size(), full_visible_path.data(), full_visible_path.size());
            if (PATH_RET != 0) {
                reject_request(static_cast<int16_t>(PATH_RET));
                break;
            }

            // Block recursive proxying: don't let a remote client traverse into our WKI mounts
            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = op_id;
                resp.status = -EPERM;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
                break;
            }

            RemoteFdReservation reservation(channel_identity);
            if (reservation.result() < 0) {
                reject_request(static_cast<int16_t>(reservation.result()));
                break;
            }

            // Open using the export backing path, independent of whichever task
            // context happened to receive the packet.
            perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
            uint64_t const LOCAL_STARTED_US = wki_now_us();
            ker::vfs::File* file = nullptr;
            int open_result = -1;
            auto* worker = ker::mod::sched::get_current_task();
            if (worker == nullptr) {
                open_result = -ESRCH;
            } else if (REGULAR_ONLY) {
                open_result = ker::vfs::vfs_open_read_file_resolved_beneath_as(*worker, mapped_credentials, export_path, full_path.data(),
                                                                               &file, absolute_symlink_root);
            } else {
                open_result = ker::vfs::vfs_open_file_resolved_beneath_as(*worker, mapped_credentials, export_path, full_path.data(),
                                                                          static_cast<int>(flags), static_cast<int>(mode), &file, nullptr,
                                                                          absolute_symlink_root);
            }
            if (file == nullptr) {
                perf_record_vfs_server_end(SERVER_OP, hdr->src_node, channel_id, CORRELATION, open_result,
                                           static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US), 0, CALLSITE);
                DevOpRespPayload resp = {};
                resp.op_id = op_id;
                resp.status = static_cast<int16_t>(open_result);
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
                std::memcpy(reinterpret_cast<uint8_t*>(&open_resp) + offsetof(OpenRespPayload, stat), &open_stat, sizeof(open_stat));
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
            bool const DEBUG_PROBE_VALID = file->fops != nullptr && file->fops->vfs_read != nullptr && !file->is_directory &&
                                           ker::mod::security::authorize_file_description_access(flags & 3U, false) == 0;
            int const DEBUG_FS_TYPE = static_cast<int>(file->fs_type);
            if (DEBUG_PROBE_VALID) {
                debug_probe_n = file->fops->vfs_read(file, debug_probe.data(), debug_probe.size(), 0);
            }
#endif

            // Publish the opened file only after all metadata and prefetch work
            // is complete. Peer cleanup can detach and delete published files.
            int32_t const fd_id = reservation.result();
            if (!reservation.publish(file, 0, 0, 0, ordinary_token, ordinary_binding_id)) {
                static_cast<void>(ker::vfs::vfs_close_file(file));
                perf_record_vfs_server_end(SERVER_OP, hdr->src_node, channel_id, CORRELATION, -ENOTCONN,
                                           static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US), 0, CALLSITE);
                DevOpRespPayload resp = {};
                resp.op_id = op_id;
                resp.status = -ENOTCONN;
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
                break;
            }
            open_resp.fd = fd_id;

            uint16_t const OPEN_DATA_LEN = (open_resp.has_stat == 0)          ? OPEN_RESP_NO_STAT_LEN
                                           : (open_resp.prefetched_bytes > 0) ? OPEN_RESP_WITH_PREFETCH_LEN
                                                                              : OPEN_RESP_WITH_STAT_LEN;
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(OpenRespPayload)> resp_buf{};  // NOLINT(modernize-avoid-c-arrays)
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = op_id;
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
                erase_remote_fd_if_retired(channel_identity, fd_id);
                s_vfs_lock.unlock();

                if (orphan != nullptr) {
                    static_cast<void>(release_server_file(orphan));
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

        case OP_VFS_SECURE_IO: {
            if (grant_binding == nullptr || data == nullptr || data_len < sizeof(VfsSecureIoIntent)) {
                reject_request(-ENOTSUP);
                break;
            }
            VfsSecureIoIntent intent{};
            std::memcpy(&intent, data, sizeof(intent));
            bool const WRITE = intent.opcode == static_cast<uint8_t>(rdma_protocol::Opcode::WRITE);
            if (!wki_vfs_secure_intent_valid(intent) || data_len != sizeof(intent) + (WRITE ? intent.length : 0) ||
                (!WRITE && intent.length > VFS_SECURE_MESSAGE_READ_MAX) || (WRITE && intent.length > VFS_SECURE_MESSAGE_WRITE_MAX) ||
                intent.binding_id != grant_binding->vfs_open_binding_id ||
                intent.owner_boot_epoch != grant_binding->resource_incarnation.owner_boot_epoch ||
                intent.resource_incarnation != grant_binding->resource_incarnation.resource_incarnation ||
                intent.resource_id != grant_binding->resource_id) {
                reject_request(-EINVAL);
                break;
            }
            VfsSecureContext authority{};
            authority.identity = channel_identity;
            authority.binding = grant_binding;
            authority.publication_revision = grant_binding->vfs_export_publication_revision;
            ker::vfs::File* file = nullptr;
            s_vfs_lock.lock();
            int admit = secure_vfs_file_locked(authority, intent, WRITE, file);
            if (admit == 0) ker::vfs::vfs_retain_file(file);
            s_vfs_lock.unlock();
            if (admit == 0 && !wki_dev_server_vfs_secure_compatible(grant_binding)) {
                // The fixed deferred owner task has no peer RX writer gate.
                // A confirm may have arrived after the ordinary OPEN handshake.
                wki_remote_vfs_prepare_secure_binding(*grant_binding);
                if (!wki_dev_server_vfs_secure_compatible(grant_binding)) {
                    ker::vfs::vfs_put_file(file);
                    file = nullptr;
                    admit = -ENOTSUP;
                }
            }
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(VfsSecureIoResult) + VFS_SECURE_MESSAGE_READ_MAX> response;
            VfsSecureIoResult result{};
            if (admit != 0) {
                result.status = admit;
            } else {
                ssize_t const BYTES = WRITE ? ker::vfs::vfs_pwrite_file(file, data + sizeof(intent), intent.length, intent.file_offset)
                                            : ker::vfs::vfs_pread_file(file, response.data() + sizeof(DevOpRespPayload) + sizeof(result),
                                                                       intent.length, intent.file_offset);
                ker::vfs::vfs_put_file(file);
                if (BYTES < 0 || static_cast<uint64_t>(BYTES) > intent.length || (WRITE && BYTES == 0)) {
                    result.status = BYTES < 0 ? static_cast<int32_t>(BYTES) : -EIO;
                    result.outcome = static_cast<uint8_t>(WRITE ? WkiSecureRdmaOutcome::UNCERTAIN : WkiSecureRdmaOutcome::NO_EFFECT);
                } else {
                    result.bytes = static_cast<uint32_t>(BYTES);
                    result.outcome = static_cast<uint8_t>(WkiSecureRdmaOutcome::EXACT);
                }
            }
            uint16_t const RESULT_LENGTH = static_cast<uint16_t>(sizeof(result) + (WRITE ? 0 : result.bytes));
            DevOpRespPayload const header{.op_id = op_id, .status = 0, .data_len = RESULT_LENGTH, .reserved = REQ_COOKIE};
            std::memcpy(response.data(), &header, sizeof(header));
            std::memcpy(response.data() + sizeof(header), &result, sizeof(result));
            send_buffered_resp(response.data(), static_cast<uint16_t>(sizeof(header) + RESULT_LENGTH));
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

            ker::vfs::File* local_file = nullptr;
            int const ADMISSION = retain_server_file(channel_identity, fd_id, ServerFileUse::READ, local_file);
            if (ADMISSION != 0) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ;
                resp.status = static_cast<int16_t>(ADMISSION);
                resp.data_len = 0;
                resp.reserved = REQ_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
                break;
            }
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
                        static_cast<void>(ker::vfs::vfs_close_file(test_file));
                    }
                }
            }
#endif

            ker::vfs::vfs_put_file(local_file);
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
            static_cast<void>(send_completed_vfs_reply(channel_identity, resp_buf.data(), SEND_LEN, binding_retiring));
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

            ker::vfs::File* local_file = nullptr;
            int const ADMIT = retain_server_write_file(channel_identity, fd_id, offset, local_file, -1);
            if (ADMIT != 0) {
                reject_request(static_cast<int16_t>(ADMIT));
                break;
            }
            ssize_t const BYTES_WRITTEN = ker::vfs::vfs_pwrite_file(local_file, write_data, write_len, static_cast<off_t>(offset));
            ker::vfs::vfs_put_file(local_file);

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
                g_remote_fd_close_retirements_total++;
                if (NO_SUCCESS_RESPONSE) {
                    g_remote_fd_async_close_completions++;
                }
                status = 0;
            } else if (NO_SUCCESS_RESPONSE) {
                g_remote_fd_async_close_misses++;
            }
            erase_remote_fd_if_retired(channel_identity, fd_id);
            s_vfs_lock.unlock();

            if (close_file != nullptr) {
                // Remote opens use the ordinary VFS File lifecycle. Closing
                // only the filesystem backend skips generic close-time path
                // and metadata-cache publication, so a submitter can retain a
                // stale negative lookup after a peer creates an output.
                int const CLOSE_RET = release_server_file(close_file);
                status = static_cast<int16_t>(std::clamp(CLOSE_RET, SHRT_MIN, SHRT_MAX));
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

            ker::vfs::File* local_file = nullptr;
            int const ADMISSION = retain_server_file(channel_identity, fd_id, ServerFileUse::DIRECTORY, local_file, -1);
            if (ADMISSION != 0) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR;
                resp.status = static_cast<int16_t>(ADMISSION);
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }
            ker::vfs::DirEntry entry = {};
            perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
            uint64_t const LOCAL_STARTED_US = wki_now_us();
            int const RET = local_file->fops->vfs_readdir(local_file, &entry, index);
            ker::vfs::vfs_put_file(local_file);
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

            ker::vfs::File* local_file = nullptr;
            int const ADMISSION = retain_server_file(channel_identity, fd_id, ServerFileUse::DIRECTORY, local_file, -1);
            if (ADMISSION != 0) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR_BATCH;
                resp.status = static_cast<int16_t>(ADMISSION);
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }
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
            ker::vfs::vfs_put_file(local_file);

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

        case OP_VFS_FSTAT: {
            if ((identity_capabilities & WKI_CAP_VFS_FD_METADATA) == 0) {
                reject_request(-ENOTSUP);
                break;
            }
            if (data == nullptr || data_len != sizeof(int32_t)) {
                reject_request(-EINVAL);
                break;
            }
            int32_t fd_id = 0;
            std::memcpy(&fd_id, data, sizeof(fd_id));
            ker::vfs::File* local_file = nullptr;
            int const ADMISSION = retain_server_file(channel_identity, fd_id, ServerFileUse::METADATA, local_file);
            if (ADMISSION != 0) {
                reject_request(static_cast<int16_t>(ADMISSION));
                break;
            }
            ker::vfs::Stat stat{};
            static_assert(sizeof(stat) == WKI_VFS_FSTAT_RESPONSE_SIZE);
            int const RESULT = ker::vfs::vfs_fstat_file(local_file, &stat);
            ker::vfs::vfs_put_file(local_file);
            if (RESULT != 0) {
                reject_request(static_cast<int16_t>(RESULT));
                break;
            }
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(stat)> response{};
            auto* header = reinterpret_cast<DevOpRespPayload*>(response.data());
            header->op_id = OP_VFS_FSTAT;
            header->data_len = sizeof(stat);
            std::memcpy(response.data() + sizeof(*header), &stat, sizeof(stat));
            send_buffered_resp(response.data(), static_cast<uint16_t>(response.size()));
            break;
        }
        case OP_VFS_STAT:
            reject_request(-ENOTSUP);
            break;
        case OP_VFS_STAT_AS: {
            if ((identity_capabilities & WKI_CAP_VFS_STAT_IDENTITY) == 0) {
                reject_request(-ENOTSUP);
                break;
            }
            constexpr size_t RECORD_SIZE = ker::mod::security::credential_record::SIZE;
            constexpr size_t PATH_OFFSET = RECORD_SIZE + 4;
            if (data == nullptr || data_len < PATH_OFFSET) {
                reject_request(-EINVAL);
                break;
            }
            uint8_t const FLAGS = data[RECORD_SIZE];
            uint16_t path_len = 0;
            std::memcpy(&path_len, data + RECORD_SIZE + 2, sizeof(path_len));
            const uint8_t* const PATH = exact_relative_wire_path(data, data_len, PATH_OFFSET, path_len, true);
            if (data[RECORD_SIZE + 1] != 0 || (FLAGS & ~(WKI_VFS_STAT_FOLLOW | WKI_VFS_STAT_REQUIRE_DIRECTORY)) != 0 || PATH == nullptr) {
                reject_request(-EINVAL);
                break;
            }
            ker::mod::security::Snapshot supplied;
            ker::mod::security::Snapshot mapped;
            int result = ker::mod::security::credential_record::decode({data, RECORD_SIZE}, supplied);
            if (result == 0) result = auth_policy::map_vfs_read_identity(identity_policy, supplied, mapped);
            if (result != 0) {
                reject_request(static_cast<int16_t>(result));
                break;
            }
            std::array<char, 512> full_path{};
            std::array<char, 512> full_visible_path{};
            result = prepare_mapped_scalar_path(export_path, export_name, PATH, path_len, COMPONENT_PATHS, full_path.data(),
                                                full_path.size(), full_visible_path.data(), full_visible_path.size());
            if (result == 0 && path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) result = -EPERM;
            auto* task = ker::mod::sched::get_current_task();
            if (result == 0 && task == nullptr) result = -ESRCH;
            ker::vfs::Stat statbuf{};
            if (result == 0) {
                perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
                uint64_t const LOCAL_STARTED_US = wki_now_us();
                result = ker::vfs::vfs_stat_resolved_beneath_as(
                    *task, mapped, export_path, full_path.data(), &statbuf, (FLAGS & WKI_VFS_STAT_FOLLOW) != 0,
                    (FLAGS & WKI_VFS_STAT_REQUIRE_DIRECTORY) != 0, nullptr, absolute_symlink_root);
                perf_record_vfs_server_end(SERVER_OP, hdr->src_node, channel_id, CORRELATION, result,
                                           static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US), result == 0 ? sizeof(statbuf) : 0,
                                           CALLSITE);
            }
            if (result != 0) {
                reject_request(static_cast<int16_t>(result));
                break;
            }
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(ker::vfs::Stat)> response{};
            auto* header = reinterpret_cast<DevOpRespPayload*>(response.data());
            header->op_id = OP_VFS_STAT_AS;
            header->data_len = sizeof(statbuf);
            std::memcpy(response.data() + sizeof(*header), &statbuf, sizeof(statbuf));
            send_buffered_resp(response.data(), static_cast<uint16_t>(response.size()));
            break;
        }

        case OP_VFS_FEATURES: {
            VfsFeaturesRequest request{};
            if (data == nullptr || data_len != sizeof(request)) {
                reject_request(-EINVAL);
                break;
            }
            std::memcpy(&request, data, sizeof(request));
            if (!wki_vfs_features_request_valid(request, data_len)) {
                reject_request(-EINVAL);
                break;
            }
            bool const GRANTS = grant_binding != nullptr && (identity_capabilities & WKI_CAP_RESOURCE_INCARNATION) != 0 &&
                                grant_binding_matches(*grant_binding, channel_identity);
            VfsFeaturesResponse const features{
                .version = WKI_VFS_FEATURES_VERSION,
                .length = sizeof(VfsFeaturesResponse),
                .reserved = 0,
                .features = WKI_VFS_FEATURE_MKDIR_IDENTITY | WKI_VFS_FEATURE_SYMLINK_IDENTITY | WKI_VFS_FEATURE_REMOVE_IDENTITY |
                            WKI_VFS_FEATURE_RENAME_IDENTITY | WKI_VFS_FEATURE_BATCH_MUTATION_IDENTITY | WKI_VFS_FEATURE_XATTR_IDENTITY |
                            WKI_VFS_FEATURE_DIRECTORY_REFERENCE | WKI_VFS_FEATURE_FILE_METADATA_IDENTITY | WKI_VFS_FEATURE_CHOWN_IDENTITY |
                            (GRANTS ? WKI_VFS_FEATURE_DIRECTORY_GRANT | WKI_VFS_FEATURE_OPEN_LIFETIME : 0) |
                            (g_wki.vfs_realpath_enabled && g_wki.vfs_directory_paths_enabled ? WKI_VFS_FEATURE_REALPATH_AT : 0)};
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(features)> response{};
            DevOpRespPayload const header{.op_id = OP_VFS_FEATURES, .status = 0, .data_len = sizeof(features), .reserved = REQ_COOKIE};
            std::memcpy(response.data(), &header, sizeof(header));
            std::memcpy(response.data() + sizeof(header), &features, sizeof(features));
            send_buffered_resp(response.data(), response.size());
            break;
        }
        case OP_VFS_MKDIR:
            reject_request(-ENOTSUP);
            break;

        case OP_VFS_READLINK:
            reject_request(-ENOTSUP);
            break;
        case OP_VFS_PATH_AS: {
            if ((identity_capabilities & WKI_CAP_VFS_PATH_IDENTITY) == 0) {
                reject_request(-ENOTSUP);
                break;
            }
            constexpr size_t RECORD_SIZE = ker::mod::security::credential_record::SIZE;
            constexpr size_t DATA_OFFSET = RECORD_SIZE + sizeof(VfsPathIdentityHeader);
            if (data == nullptr || data_len < DATA_OFFSET) {
                reject_request(-EINVAL);
                break;
            }
            VfsPathIdentityHeader request{};
            std::memcpy(&request, data + RECORD_SIZE, sizeof(request));
            if (wki_vfs_is_file_metadata(request.operation)) {
                FileMetadataRequest metadata;
                int result = decode_file_metadata_request(data, data_len, metadata);
                ker::mod::security::Snapshot mapped;
                if (result == 0) result = auth_policy::map_vfs_mutation_identity(identity_policy, metadata.credentials, mapped);
                auto release = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
                std::unique_ptr<ker::vfs::File, decltype(release)> file(nullptr, release);
                if (result == 0) {
                    s_vfs_lock.lock();
                    if (auto* row = find_remote_fd(channel_identity, metadata.fd); row != nullptr && row->file != nullptr) {
                        touch_remote_fd(row);
                        ker::vfs::vfs_retain_file(row->file);
                        file.reset(row->file);
                    }
                    s_vfs_lock.unlock();
                    if (!file)
                        result = -EBADF;
                    else if (file->fs_type == ker::vfs::FSType::REMOTE)
                        result = -EXDEV;
                    else if (file->exec_read_view != nullptr)
                        result = -EROFS;
                }
                if (result == 0) {
                    switch (metadata.header.operation) {
                        case VfsPathOperation::FCHMOD:
                            result = ker::vfs::vfs_fchmod_file_as(file.get(), mapped, static_cast<int>(metadata.mode));
                            break;
                        case VfsPathOperation::FCHOWN:
                            result = ker::vfs::vfs_fchown_file_as(file.get(), mapped, metadata.owner, metadata.group);
                            break;
                        case VfsPathOperation::FUTIMENS:
                            result = ker::vfs::vfs_futimens_file_as(file.get(), mapped, metadata.times.data());
                            break;
                        default:
                            result = -ENOTSUP;
                            break;
                    }
                }
                if (result != 0) {
                    reject_request(static_cast<int16_t>(result));
                    break;
                }
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(VfsPathIdentityHeader)> response{};
                DevOpRespPayload const HEADER{
                    .op_id = OP_VFS_PATH_AS, .status = 0, .data_len = sizeof(VfsPathIdentityHeader), .reserved = REQ_COOKIE};
                VfsPathIdentityHeader const REPLY{.operation = request.operation, .flags = 0, .data_len = 0, .reserved = 0};
                std::memcpy(response.data(), &HEADER, sizeof(HEADER));
                std::memcpy(response.data() + sizeof(HEADER), &REPLY, sizeof(REPLY));
                send_buffered_resp(response.data(), response.size());
                break;
            }
            if (request.operation == VfsPathOperation::CHMOD) {
                VfsPathChmodPrefix prefix{};
                if (data_len < DATA_OFFSET + sizeof(prefix)) {
                    reject_request(-EINVAL);
                    break;
                }
                std::memcpy(&prefix, data + DATA_OFFSET, sizeof(prefix));
                if (!wki_vfs_chmod_request_valid(request, prefix, data_len - DATA_OFFSET)) {
                    reject_request(-EINVAL);
                    break;
                }
                size_t const PATH_LENGTH = request.data_len - sizeof(prefix);
                const uint8_t* const PATH = data + DATA_OFFSET + sizeof(prefix);
                int result = mapped_wire_path_status(PATH, PATH_LENGTH, COMPONENT_PATHS);
                ker::mod::security::Snapshot supplied, mapped;
                if (result == 0) result = ker::mod::security::credential_record::decode({data, RECORD_SIZE}, supplied);
                if (result == 0) result = auth_policy::map_vfs_mutation_identity(identity_policy, supplied, mapped);
                auto* task = ker::mod::sched::get_current_task();
                if (result == 0 && task == nullptr) result = -ESRCH;
                auto release = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
                std::unique_ptr<ker::vfs::File, decltype(release)> directory(nullptr, release);
                std::array<char, 512> path{}, visible{};
                if (result == 0 && prefix.directory_fd > 0) {
                    s_vfs_lock.lock();
                    auto* fd = find_remote_fd(channel_identity, prefix.directory_fd);
                    if (fd != nullptr && fd->file != nullptr) {
                        touch_remote_fd(fd);
                        ker::vfs::vfs_retain_file(fd->file);
                        directory.reset(fd->file);
                    }
                    s_vfs_lock.unlock();
                    if (!directory) result = -EBADF;
                    std::memcpy(path.data(), PATH, PATH_LENGTH);
                } else if (result == 0) {
                    result = prepare_mapped_scalar_path(export_path, export_name, PATH, PATH_LENGTH, COMPONENT_PATHS, path.data(),
                                                        path.size(), visible.data(), visible.size());
                    if (result == 0 && path_crosses_recursive_wki_boundary_direct(path.data(), visible.data())) result = -EPERM;
                }
                if (result == 0)
                    result = ker::vfs::vfs_chmod_resolved_beneath_as(*task, mapped, export_path, path.data(), static_cast<int>(prefix.mode),
                                                                     (request.flags & WKI_VFS_PATH_FOLLOW_FINAL) != 0,
                                                                     (request.flags & WKI_VFS_PATH_REQUIRE_DIRECTORY) != 0, directory.get(),
                                                                     absolute_symlink_root);
                if (result != 0) {
                    reject_request(static_cast<int16_t>(result));
                    break;
                }
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(VfsPathIdentityHeader)> response{};
                DevOpRespPayload const HEADER{
                    .op_id = OP_VFS_PATH_AS, .status = 0, .data_len = sizeof(VfsPathIdentityHeader), .reserved = REQ_COOKIE};
                VfsPathIdentityHeader const REPLY{.operation = VfsPathOperation::CHMOD, .flags = 0, .data_len = 0, .reserved = 0};
                std::memcpy(response.data(), &HEADER, sizeof(HEADER));
                std::memcpy(response.data() + sizeof(HEADER), &REPLY, sizeof(REPLY));
                send_buffered_resp(response.data(), response.size());
                break;
            }
            if (request.operation == VfsPathOperation::CHOWN) {
                VfsPathChownPrefix prefix{};
                if (data_len < DATA_OFFSET + sizeof(prefix)) {
                    reject_request(-EINVAL);
                    break;
                }
                std::memcpy(&prefix, data + DATA_OFFSET, sizeof(prefix));
                if (!wki_vfs_chown_request_valid(request, prefix, data_len - DATA_OFFSET)) {
                    reject_request(-EINVAL);
                    break;
                }
                size_t const PATH_LENGTH = request.data_len - sizeof(prefix);
                const uint8_t* const PATH = data + DATA_OFFSET + sizeof(prefix);
                int result = mapped_wire_path_status(PATH, PATH_LENGTH, COMPONENT_PATHS);
                ker::mod::security::Snapshot supplied, mapped;
                if (result == 0) result = ker::mod::security::credential_record::decode({data, RECORD_SIZE}, supplied);
                if (result == 0) result = auth_policy::map_vfs_mutation_identity(identity_policy, supplied, mapped);
                auto* task = ker::mod::sched::get_current_task();
                if (result == 0 && task == nullptr) result = -ESRCH;
                auto release = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
                std::unique_ptr<ker::vfs::File, decltype(release)> directory(nullptr, release);
                std::array<char, 512> path{}, visible{};
                if (result == 0 && prefix.directory_fd > 0) {
                    s_vfs_lock.lock();
                    auto* fd = find_remote_fd(channel_identity, prefix.directory_fd);
                    if (fd != nullptr && fd->file != nullptr) {
                        touch_remote_fd(fd);
                        ker::vfs::vfs_retain_file(fd->file);
                        directory.reset(fd->file);
                    }
                    s_vfs_lock.unlock();
                    if (!directory) result = -EBADF;
                    std::memcpy(path.data(), PATH, PATH_LENGTH);
                } else if (result == 0) {
                    result = prepare_mapped_scalar_path(export_path, export_name, PATH, PATH_LENGTH, COMPONENT_PATHS, path.data(),
                                                        path.size(), visible.data(), visible.size());
                    if (result == 0 && path_crosses_recursive_wki_boundary_direct(path.data(), visible.data())) result = -EPERM;
                }
                if (result == 0)
                    result = ker::vfs::vfs_chown_resolved_beneath_as(*task, mapped, export_path, path.data(), prefix.owner, prefix.group,
                                                                     (request.flags & WKI_VFS_PATH_FOLLOW_FINAL) != 0,
                                                                     (request.flags & WKI_VFS_PATH_REQUIRE_DIRECTORY) != 0, directory.get(),
                                                                     absolute_symlink_root);
                if (result != 0) {
                    reject_request(static_cast<int16_t>(result));
                    break;
                }
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(VfsPathIdentityHeader)> response{};
                DevOpRespPayload const HEADER{
                    .op_id = OP_VFS_PATH_AS, .status = 0, .data_len = sizeof(VfsPathIdentityHeader), .reserved = REQ_COOKIE};
                VfsPathIdentityHeader const REPLY{.operation = VfsPathOperation::CHOWN, .flags = 0, .data_len = 0, .reserved = 0};
                std::memcpy(response.data(), &HEADER, sizeof(HEADER));
                std::memcpy(response.data() + sizeof(HEADER), &REPLY, sizeof(REPLY));
                send_buffered_resp(response.data(), response.size());
                break;
            }
            if (request.operation == VfsPathOperation::MKDIR) {
                VfsPathMkdirPrefix prefix{};
                if (data_len < DATA_OFFSET + sizeof(prefix)) {
                    reject_request(-EINVAL);
                    break;
                }
                std::memcpy(&prefix, data + DATA_OFFSET, sizeof(prefix));
                if (!wki_vfs_mkdir_request_valid(request, prefix, data_len - DATA_OFFSET)) {
                    reject_request(-EINVAL);
                    break;
                }
                size_t const PATH_LENGTH = request.data_len - sizeof(prefix);
                const uint8_t* const PATH = data + DATA_OFFSET + sizeof(prefix);
                int result = mapped_wire_path_status(PATH, PATH_LENGTH, COMPONENT_PATHS);
                ker::mod::security::Snapshot supplied, mapped;
                if (result == 0) result = ker::mod::security::credential_record::decode({data, RECORD_SIZE}, supplied);
                if (result == 0) result = auth_policy::map_vfs_mutation_identity(identity_policy, supplied, mapped);
                auto* task = ker::mod::sched::get_current_task();
                if (result == 0 && task == nullptr) result = -ESRCH;
                auto release = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
                std::unique_ptr<ker::vfs::File, decltype(release)> directory(nullptr, release);
                std::array<char, 512> path{}, visible{};
                if (result == 0 && prefix.directory_fd > 0) {
                    s_vfs_lock.lock();
                    auto* fd = find_remote_fd(channel_identity, prefix.directory_fd);
                    if (fd != nullptr && fd->file != nullptr) {
                        touch_remote_fd(fd);
                        ker::vfs::vfs_retain_file(fd->file);
                        directory.reset(fd->file);
                    }
                    s_vfs_lock.unlock();
                    if (!directory) result = -EBADF;
                    std::memcpy(path.data(), PATH, PATH_LENGTH);
                } else if (result == 0) {
                    result = prepare_mapped_scalar_path(export_path, export_name, PATH, PATH_LENGTH, COMPONENT_PATHS, path.data(),
                                                        path.size(), visible.data(), visible.size());
                    if (result == 0 && path_crosses_recursive_wki_boundary_direct(path.data(), visible.data())) result = -EPERM;
                }
                if (result == 0)
                    result = ker::vfs::vfs_mkdir_resolved_beneath_as(*task, mapped, export_path, path.data(), static_cast<int>(prefix.mode),
                                                                     directory.get(), absolute_symlink_root);
                if (result != 0) {
                    reject_request(static_cast<int16_t>(result));
                    break;
                }
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(VfsPathIdentityHeader)> response{};
                DevOpRespPayload const HEADER{
                    .op_id = OP_VFS_PATH_AS, .status = 0, .data_len = sizeof(VfsPathIdentityHeader), .reserved = REQ_COOKIE};
                VfsPathIdentityHeader const REPLY{.operation = VfsPathOperation::MKDIR, .flags = 0, .data_len = 0, .reserved = 0};
                std::memcpy(response.data(), &HEADER, sizeof(HEADER));
                std::memcpy(response.data() + sizeof(HEADER), &REPLY, sizeof(REPLY));
                send_buffered_resp(response.data(), response.size());
                break;
            }
            if (request.operation == VfsPathOperation::DIRECTORY_BINDING) {
                if (request.flags != 0 || request.reserved != 0 || request.data_len != sizeof(int32_t) ||
                    data_len != DATA_OFFSET + sizeof(int32_t)) {
                    reject_request(-EINVAL);
                    break;
                }
                int32_t fd_id = -1;
                std::memcpy(&fd_id, data + DATA_OFFSET, sizeof(fd_id));
                if (fd_id <= 0) {
                    reject_request(-EBADF);
                    break;
                }
                ker::mod::security::Snapshot supplied, mapped;
                int result = ker::mod::security::credential_record::decode({data, RECORD_SIZE}, supplied);
                if (result == 0) result = auth_policy::map_vfs_mutation_identity(identity_policy, supplied, mapped);
                if (result == 0) {
                    s_vfs_lock.lock();
                    auto* fd = find_remote_fd(channel_identity, fd_id);
                    result = fd == nullptr || fd->file == nullptr ? -EBADF : !fd->file->is_directory ? -ENOTDIR : 0;
                    if (result == 0) touch_remote_fd(fd);
                    s_vfs_lock.unlock();
                }
                if (result != 0) {
                    reject_request(static_cast<int16_t>(result));
                    break;
                }
                VfsRenameDirectoryRef const REFERENCE{
                    .fd = fd_id, .channel_id = channel_identity.channel_id, .reserved = 0, .generation = channel_identity.generation};
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(VfsPathIdentityHeader) + sizeof(REFERENCE)> response{};
                DevOpRespPayload const HEADER{.op_id = OP_VFS_PATH_AS,
                                              .status = 0,
                                              .data_len = sizeof(VfsPathIdentityHeader) + sizeof(REFERENCE),
                                              .reserved = REQ_COOKIE};
                VfsPathIdentityHeader const REPLY{
                    .operation = VfsPathOperation::DIRECTORY_BINDING, .flags = 0, .data_len = sizeof(REFERENCE), .reserved = 0};
                std::memcpy(response.data(), &HEADER, sizeof(HEADER));
                std::memcpy(response.data() + sizeof(HEADER), &REPLY, sizeof(REPLY));
                std::memcpy(response.data() + sizeof(HEADER) + sizeof(REPLY), &REFERENCE, sizeof(REFERENCE));
                send_buffered_resp(response.data(), response.size());
                break;
            }
            if (request.operation == VfsPathOperation::RENAME) {
                VfsPathRenamePrefix prefix{};
                if (data_len < DATA_OFFSET + sizeof(prefix)) {
                    reject_request(-EINVAL);
                    break;
                }
                std::memcpy(&prefix, data + DATA_OFFSET, sizeof(prefix));
                if (!wki_vfs_rename_request_valid(request, prefix, data_len - DATA_OFFSET)) {
                    reject_request(-EINVAL);
                    break;
                }
                size_t const NEW_LENGTH = request.data_len - sizeof(prefix) - prefix.source_length;
                const uint8_t* const OLD_PATH = data + DATA_OFFSET + sizeof(prefix);
                const uint8_t* const NEW_PATH = OLD_PATH + prefix.source_length;
                int result = mapped_wire_path_status(OLD_PATH, prefix.source_length, COMPONENT_PATHS);
                if (result == 0) result = mapped_wire_path_status(NEW_PATH, NEW_LENGTH, COMPONENT_PATHS);
                ker::mod::security::Snapshot supplied, mapped;
                if (result == 0) result = ker::mod::security::credential_record::decode({data, RECORD_SIZE}, supplied);
                if (result == 0) result = auth_policy::map_vfs_mutation_identity(identity_policy, supplied, mapped);
                auto* task = ker::mod::sched::get_current_task();
                if (result == 0 && task == nullptr) result = -ESRCH;
                auto release_file = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
                auto release_binding = [](DevServerBinding* binding) { wki_dev_server_release_vfs_sibling(binding); };
                // Binding pins are destroyed after the directory File references.
                std::unique_ptr<DevServerBinding, decltype(release_binding)> source_binding(nullptr, release_binding),
                    destination_binding(nullptr, release_binding);
                std::unique_ptr<ker::vfs::File, decltype(release_file)> source_directory(nullptr, release_file),
                    destination_directory(nullptr, release_file);
                std::array<char, 512> old_path{}, new_path{};
                auto prepare = [&](const VfsRenameDirectoryRef& reference, const uint8_t* bytes, size_t length, auto& binding,
                                   auto& directory, auto& path) -> int {
                    if (reference.fd > 0) {
                        WkiChannelIdentity identity{};
                        binding.reset(
                            wki_dev_server_retain_vfs_sibling(channel_identity, reference.channel_id, reference.generation, &identity));
                        if (!binding) return -EBADF;
                        s_vfs_lock.lock();
                        auto* fd = find_remote_fd(identity, reference.fd);
                        if (fd != nullptr && fd->file != nullptr) {
                            touch_remote_fd(fd);
                            ker::vfs::vfs_retain_file(fd->file);
                            directory.reset(fd->file);
                        }
                        s_vfs_lock.unlock();
                        if (!directory) return -EBADF;
                        std::memcpy(path.data(), bytes, length);
                        return 0;
                    }
                    std::array<char, 512> visible{};
                    int const STATUS = prepare_mapped_scalar_path(export_path, export_name, bytes, length, COMPONENT_PATHS, path.data(),
                                                                  path.size(), visible.data(), visible.size());
                    if (STATUS != 0) return STATUS;
                    return path_crosses_recursive_wki_boundary_direct(path.data(), visible.data()) ? -EPERM : 0;
                };
                if (result == 0)
                    result = prepare(prefix.source, OLD_PATH, prefix.source_length, source_binding, source_directory, old_path);
                if (result == 0)
                    result = prepare(prefix.destination, NEW_PATH, NEW_LENGTH, destination_binding, destination_directory, new_path);
                if (result == 0)
                    result = ker::vfs::vfs_rename_resolved_beneath_as(
                        *task, mapped, export_path, old_path.data(), new_path.data(), (prefix.flags & WKI_VFS_RENAME_SOURCE_DIRECTORY) != 0,
                        (prefix.flags & WKI_VFS_RENAME_DESTINATION_DIRECTORY) != 0, source_directory.get(), destination_directory.get(),
                        absolute_symlink_root);
                if (result != 0) {
                    reject_request(static_cast<int16_t>(result));
                    break;
                }
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(VfsPathIdentityHeader)> response{};
                DevOpRespPayload const HEADER{
                    .op_id = OP_VFS_PATH_AS, .status = 0, .data_len = sizeof(VfsPathIdentityHeader), .reserved = REQ_COOKIE};
                VfsPathIdentityHeader const REPLY{.operation = VfsPathOperation::RENAME, .flags = 0, .data_len = 0, .reserved = 0};
                std::memcpy(response.data(), &HEADER, sizeof(HEADER));
                std::memcpy(response.data() + sizeof(HEADER), &REPLY, sizeof(REPLY));
                send_buffered_resp(response.data(), response.size());
                break;
            }
            if (request.operation == VfsPathOperation::REMOVE) {
                VfsPathRemovePrefix prefix{};
                if (data_len < DATA_OFFSET + sizeof(prefix)) {
                    reject_request(-EINVAL);
                    break;
                }
                std::memcpy(&prefix, data + DATA_OFFSET, sizeof(prefix));
                if (!wki_vfs_remove_request_valid(request, prefix, data_len - DATA_OFFSET)) {
                    reject_request(-EINVAL);
                    break;
                }
                size_t const PATH_LENGTH = request.data_len - sizeof(prefix);
                const uint8_t* const PATH = data + DATA_OFFSET + sizeof(prefix);
                int result = mapped_wire_path_status(PATH, PATH_LENGTH, COMPONENT_PATHS);
                ker::mod::security::Snapshot supplied, mapped;
                if (result == 0) result = ker::mod::security::credential_record::decode({data, RECORD_SIZE}, supplied);
                if (result == 0) result = auth_policy::map_vfs_mutation_identity(identity_policy, supplied, mapped);
                auto* task = ker::mod::sched::get_current_task();
                if (result == 0 && task == nullptr) result = -ESRCH;
                auto release = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
                std::unique_ptr<ker::vfs::File, decltype(release)> directory(nullptr, release);
                std::array<char, 512> path{}, visible{};
                if (result == 0 && prefix.directory_fd > 0) {
                    s_vfs_lock.lock();
                    auto* fd = find_remote_fd(channel_identity, prefix.directory_fd);
                    if (fd != nullptr && fd->file != nullptr) {
                        touch_remote_fd(fd);
                        ker::vfs::vfs_retain_file(fd->file);
                        directory.reset(fd->file);
                    }
                    s_vfs_lock.unlock();
                    if (!directory) result = -EBADF;
                    std::memcpy(path.data(), PATH, PATH_LENGTH);
                } else if (result == 0) {
                    result = prepare_mapped_scalar_path(export_path, export_name, PATH, PATH_LENGTH, COMPONENT_PATHS, path.data(),
                                                        path.size(), visible.data(), visible.size());
                    if (result == 0 && path_crosses_recursive_wki_boundary_direct(path.data(), visible.data())) result = -EPERM;
                }
                if (result == 0)
                    result = ker::vfs::vfs_unlink_resolved_beneath_as(
                        *task, mapped, export_path, path.data(), (prefix.flags & WKI_VFS_REMOVE_DIRECTORY) != 0,
                        (request.flags & WKI_VFS_PATH_REQUIRE_DIRECTORY) != 0, directory.get(), absolute_symlink_root);
                if (result != 0) {
                    reject_request(static_cast<int16_t>(result));
                    break;
                }
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(VfsPathIdentityHeader)> response{};
                DevOpRespPayload const HEADER{
                    .op_id = OP_VFS_PATH_AS, .status = 0, .data_len = sizeof(VfsPathIdentityHeader), .reserved = REQ_COOKIE};
                VfsPathIdentityHeader const REPLY{.operation = VfsPathOperation::REMOVE, .flags = 0, .data_len = 0, .reserved = 0};
                std::memcpy(response.data(), &HEADER, sizeof(HEADER));
                std::memcpy(response.data() + sizeof(HEADER), &REPLY, sizeof(REPLY));
                send_buffered_resp(response.data(), response.size());
                break;
            }
            if (request.operation == VfsPathOperation::SYMLINK) {
                VfsPathSymlinkPrefix prefix{};
                if (data_len < DATA_OFFSET + sizeof(prefix)) {
                    reject_request(-EINVAL);
                    break;
                }
                std::memcpy(&prefix, data + DATA_OFFSET, sizeof(prefix));
                if (!wki_vfs_symlink_request_valid(request, prefix, data_len - DATA_OFFSET)) {
                    reject_request(-EINVAL);
                    break;
                }
                const uint8_t* const TARGET = data + DATA_OFFSET + sizeof(prefix);
                if (!wire_symlink_target_is_representable(TARGET, prefix.target_length, 512)) {
                    reject_request(-EINVAL);
                    break;
                }
                std::array<char, 512> target{};
                std::memcpy(target.data(), TARGET, prefix.target_length);
                size_t const PATH_LENGTH = request.data_len - sizeof(prefix) - prefix.target_length;
                const uint8_t* const PATH = TARGET + prefix.target_length;
                int result = mapped_wire_path_status(PATH, PATH_LENGTH, COMPONENT_PATHS);
                ker::mod::security::Snapshot supplied, mapped;
                if (result == 0) result = ker::mod::security::credential_record::decode({data, RECORD_SIZE}, supplied);
                if (result == 0) result = auth_policy::map_vfs_mutation_identity(identity_policy, supplied, mapped);
                auto* task = ker::mod::sched::get_current_task();
                if (result == 0 && task == nullptr) result = -ESRCH;
                auto release = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
                std::unique_ptr<ker::vfs::File, decltype(release)> directory(nullptr, release);
                std::array<char, 512> path{}, visible{};
                if (result == 0 && prefix.directory_fd > 0) {
                    s_vfs_lock.lock();
                    auto* fd = find_remote_fd(channel_identity, prefix.directory_fd);
                    if (fd != nullptr && fd->file != nullptr) {
                        touch_remote_fd(fd);
                        ker::vfs::vfs_retain_file(fd->file);
                        directory.reset(fd->file);
                    }
                    s_vfs_lock.unlock();
                    if (!directory) result = -EBADF;
                    std::memcpy(path.data(), PATH, PATH_LENGTH);
                } else if (result == 0) {
                    result = prepare_mapped_scalar_path(export_path, export_name, PATH, PATH_LENGTH, COMPONENT_PATHS, path.data(),
                                                        path.size(), visible.data(), visible.size());
                    if (result == 0 && path_crosses_recursive_wki_boundary_direct(path.data(), visible.data())) result = -EPERM;
                }
                if (result == 0)
                    result = ker::vfs::vfs_symlink_resolved_beneath_as(*task, mapped, export_path, target.data(), path.data(),
                                                                       (request.flags & WKI_VFS_PATH_REQUIRE_DIRECTORY) != 0,
                                                                       directory.get(), absolute_symlink_root);
                if (result != 0) {
                    reject_request(static_cast<int16_t>(result));
                    break;
                }
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(VfsPathIdentityHeader)> response{};
                DevOpRespPayload const HEADER{
                    .op_id = OP_VFS_PATH_AS, .status = 0, .data_len = sizeof(VfsPathIdentityHeader), .reserved = REQ_COOKIE};
                VfsPathIdentityHeader const REPLY{.operation = VfsPathOperation::SYMLINK, .flags = 0, .data_len = 0, .reserved = 0};
                std::memcpy(response.data(), &HEADER, sizeof(HEADER));
                std::memcpy(response.data() + sizeof(HEADER), &REPLY, sizeof(REPLY));
                send_buffered_resp(response.data(), response.size());
                break;
            }
            if (request.operation == VfsPathOperation::UTIMENS) {
                VfsPathUtimensPrefix prefix{};
                if (data_len < DATA_OFFSET + sizeof(prefix)) {
                    reject_request(-EINVAL);
                    break;
                }
                std::memcpy(&prefix, data + DATA_OFFSET, sizeof(prefix));
                if (!wki_vfs_utimens_request_valid(request, prefix, data_len - DATA_OFFSET)) {
                    reject_request(-EINVAL);
                    break;
                }
                size_t const PATH_LENGTH = request.data_len - sizeof(prefix);
                const uint8_t* const PATH = data + DATA_OFFSET + sizeof(prefix);
                int result = mapped_wire_path_status(PATH, PATH_LENGTH, COMPONENT_PATHS);
                ker::mod::security::Snapshot supplied, mapped;
                if (result == 0) result = ker::mod::security::credential_record::decode({data, RECORD_SIZE}, supplied);
                if (result == 0) result = auth_policy::map_vfs_mutation_identity(identity_policy, supplied, mapped);
                auto* task = ker::mod::sched::get_current_task();
                if (result == 0 && task == nullptr) result = -ESRCH;
                auto release = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
                std::unique_ptr<ker::vfs::File, decltype(release)> directory(nullptr, release);
                std::array<char, 512> path{}, visible{};
                if (result == 0 && prefix.directory_fd > 0) {
                    s_vfs_lock.lock();
                    auto* fd = find_remote_fd(channel_identity, prefix.directory_fd);
                    if (fd != nullptr && fd->file != nullptr) {
                        touch_remote_fd(fd);
                        ker::vfs::vfs_retain_file(fd->file);
                        directory.reset(fd->file);
                    }
                    s_vfs_lock.unlock();
                    if (!directory) result = -EBADF;
                    std::memcpy(path.data(), PATH, PATH_LENGTH);
                } else if (result == 0) {
                    result = prepare_mapped_scalar_path(export_path, export_name, PATH, PATH_LENGTH, COMPONENT_PATHS, path.data(),
                                                        path.size(), visible.data(), visible.size());
                    if (result == 0 && path_crosses_recursive_wki_boundary_direct(path.data(), visible.data())) result = -EPERM;
                }
                std::array<ker::vfs::Timespec, 2> times{{{prefix.atime_sec, prefix.atime_nsec}, {prefix.mtime_sec, prefix.mtime_nsec}}};
                if (result == 0)
                    result = ker::vfs::vfs_utimens_resolved_beneath_as(
                        *task, mapped, export_path, path.data(), times.data(), (request.flags & WKI_VFS_PATH_FOLLOW_FINAL) != 0,
                        (request.flags & WKI_VFS_PATH_REQUIRE_DIRECTORY) != 0, directory.get(), absolute_symlink_root);
                if (result != 0) {
                    reject_request(static_cast<int16_t>(result));
                    break;
                }
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(VfsPathIdentityHeader)> response{};
                DevOpRespPayload const HEADER{
                    .op_id = OP_VFS_PATH_AS, .status = 0, .data_len = sizeof(VfsPathIdentityHeader), .reserved = REQ_COOKIE};
                VfsPathIdentityHeader const REPLY{.operation = VfsPathOperation::UTIMENS, .flags = 0, .data_len = 0, .reserved = 0};
                std::memcpy(response.data(), &HEADER, sizeof(HEADER));
                std::memcpy(response.data() + sizeof(HEADER), &REPLY, sizeof(REPLY));
                send_buffered_resp(response.data(), response.size());
                break;
            }
            if (request.operation == VfsPathOperation::READLINK_AT || request.operation == VfsPathOperation::STAT_AT ||
                request.operation == VfsPathOperation::OPEN_AT || request.operation == VfsPathOperation::REALPATH_AT) {
                bool const REALPATH = request.operation == VfsPathOperation::REALPATH_AT;
                bool const STAT = request.operation == VfsPathOperation::STAT_AT;
                bool const OPEN = request.operation == VfsPathOperation::OPEN_AT;
                DirectoryPathRequest relative{};
                int result =
                    (((STAT || REALPATH) && (identity_capabilities & WKI_CAP_VFS_STAT_IDENTITY) == 0) ||
                     (REALPATH && !g_wki.vfs_realpath_enabled) || (OPEN && (identity_capabilities & WKI_CAP_VFS_OPEN_IDENTITY) == 0))
                        ? -ENOTSUP
                        : decode_directory_path_request(data, data_len, COMPONENT_PATHS, g_wki.vfs_directory_paths_enabled, relative);
                ker::mod::security::Snapshot supplied;
                ker::mod::security::Snapshot mapped;
                if (result == 0) result = ker::mod::security::credential_record::decode({data, RECORD_SIZE}, supplied);
                if (result == 0)
                    result = OPEN ? auth_policy::map_vfs_open_identity(identity_policy, supplied, relative.open_flags, mapped)
                                  : auth_policy::map_vfs_read_identity(identity_policy, supplied, mapped);
                auto* task = ker::mod::sched::get_current_task();
                if (result == 0 && task == nullptr) result = -ESRCH;
                auto release = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
                std::unique_ptr<ker::vfs::File, decltype(release)> directory(nullptr, release);
                if (result == 0) {
                    s_vfs_lock.lock();
                    auto* fd = find_remote_fd(channel_identity, relative.directory_fd);
                    if (fd != nullptr && fd->file != nullptr) {
                        touch_remote_fd(fd);
                        ker::vfs::vfs_retain_file(fd->file);
                        directory.reset(fd->file);
                    }
                    s_vfs_lock.unlock();
                    if (!directory) result = -EBADF;
                }
                std::array<char, 512> path{}, target{};
                ker::vfs::Stat metadata{};
                size_t result_length = 0;
                if (OPEN) {
                    if (result != 0) {
                        reject_request(static_cast<int16_t>(result));
                        break;
                    }
                    std::memcpy(path.data(), relative.path, relative.path_length);
                    int flags = static_cast<int>(relative.open_flags) & ~ker::vfs::O_WOS_KNOWN_ABSENT;
                    if ((request.flags & WKI_VFS_PATH_REQUIRE_DIRECTORY) != 0) flags |= ker::vfs::O_DIRECTORY;
                    RemoteFdReservation reservation(channel_identity);
                    if (reservation.result() < 0) {
                        reject_request(static_cast<int16_t>(reservation.result()));
                        break;
                    }
                    ker::vfs::File* opened = nullptr;
                    result = ker::vfs::vfs_open_file_resolved_beneath_as(*task, mapped, export_path, path.data(), flags,
                                                                         static_cast<int>(relative.open_mode), &opened, directory.get(),
                                                                         absolute_symlink_root);
                    if (result != 0 || opened == nullptr) {
                        ker::vfs::vfs_put_file(opened);
                        reject_request(static_cast<int16_t>(result != 0 ? result : -EIO));
                        break;
                    }
                    VfsPathOpenResult const OPENED{.fd = reservation.result(), .is_directory = static_cast<uint8_t>(opened->is_directory)};
                    if (!reservation.publish(opened, 0, 0, 0, ordinary_token, ordinary_binding_id)) {
                        ker::vfs::vfs_put_file(opened);
                        reject_request(-ENOTCONN);
                        break;
                    }
                    std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(VfsPathIdentityHeader) + sizeof(OPENED)> response{};
                    DevOpRespPayload const HEADER{
                        .op_id = op_id, .status = 0, .data_len = sizeof(VfsPathIdentityHeader) + sizeof(OPENED), .reserved = REQ_COOKIE};
                    VfsPathIdentityHeader const REPLY{
                        .operation = VfsPathOperation::OPEN_AT, .flags = 0, .data_len = sizeof(OPENED), .reserved = 0};
                    std::memcpy(response.data(), &HEADER, sizeof(HEADER));
                    std::memcpy(response.data() + sizeof(HEADER), &REPLY, sizeof(REPLY));
                    std::memcpy(response.data() + sizeof(HEADER) + sizeof(REPLY), &OPENED, sizeof(OPENED));
                    int const SENT = wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, response.data(), response.size());
                    if (SENT != WKI_OK) {
                        ker::vfs::File* orphan = nullptr;
                        std::map<int32_t, RemoteVfsFd>::node_type retired;
                        s_vfs_lock.lock();
                        auto it = g_remote_fds.find(OPENED.fd);
                        if (it != g_remote_fds.end() && it->second.file == opened &&
                            vfs_channel_identity_matches(it->second.channel_identity, channel_identity)) {
                            orphan = it->second.file;
                            retired = g_remote_fds.extract(it);
                        }
                        s_vfs_lock.unlock();
                        ker::vfs::vfs_put_file(orphan);
                    }
                    break;
                }
                if (result == 0) {
                    std::memcpy(path.data(), relative.path, relative.path_length);
                    bool const REQUIRE_DIRECTORY = (request.flags & WKI_VFS_PATH_REQUIRE_DIRECTORY) != 0;
                    // The exact channel FIFO and deferred binding reference
                    // also exclude owner CLOSE/fence teardown during this call.
                    if (STAT) {
                        result = ker::vfs::vfs_stat_resolved_beneath_as(*task, mapped, export_path, path.data(), &metadata,
                                                                        (request.flags & WKI_VFS_PATH_FOLLOW_FINAL) != 0, REQUIRE_DIRECTORY,
                                                                        directory.get(), absolute_symlink_root);
                        result_length = sizeof(metadata);
                    } else if (REALPATH) {
                        result = ker::vfs::vfs_realpath_resolved_beneath_as(*task, mapped, export_path, path.data(), target.data(),
                                                                            target.size(), &result_length, REQUIRE_DIRECTORY,
                                                                            directory.get(), absolute_symlink_root);
                        if (result == 0 && !wki_vfs_realpath_result_valid(reinterpret_cast<const uint8_t*>(target.data()), result_length))
                            result = -EPROTO;
                    } else {
                        ssize_t const READ = ker::vfs::vfs_readlink_resolved_beneath_as(*task, mapped, export_path, path.data(),
                                                                                        target.data(), target.size(), REQUIRE_DIRECTORY,
                                                                                        directory.get(), absolute_symlink_root);
                        if (READ < 0)
                            result = static_cast<int>(READ);
                        else if (static_cast<size_t>(READ) > target.size())
                            result = -EOVERFLOW;
                        else
                            result_length = static_cast<size_t>(READ);
                    }
                }
                if (result != 0) {
                    reject_request(static_cast<int16_t>(result));
                    break;
                }
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(VfsPathIdentityHeader) + 512> response{};
                auto* header = reinterpret_cast<DevOpRespPayload*>(response.data());
                header->op_id = OP_VFS_PATH_AS;
                header->data_len = static_cast<uint16_t>(sizeof(VfsPathIdentityHeader) + result_length);
                VfsPathIdentityHeader const REPLY{
                    .operation = request.operation, .flags = 0, .data_len = static_cast<uint16_t>(result_length), .reserved = 0};
                std::memcpy(response.data() + sizeof(*header), &REPLY, sizeof(REPLY));
                std::memcpy(response.data() + sizeof(*header) + sizeof(REPLY),
                            STAT ? static_cast<const void*>(&metadata) : static_cast<const void*>(target.data()), result_length);
                send_buffered_resp(response.data(), static_cast<uint16_t>(sizeof(*header) + header->data_len));
                break;
            }
            bool const REALPATH = request.operation == VfsPathOperation::REALPATH;
            if ((request.operation != VfsPathOperation::READLINK && !REALPATH) ||
                (REALPATH && (!g_wki.vfs_realpath_enabled || (identity_capabilities & WKI_CAP_VFS_STAT_IDENTITY) == 0))) {
                reject_request(-ENOTSUP);
                break;
            }
            const uint8_t* const PATH = exact_relative_wire_path(data, data_len, DATA_OFFSET, request.data_len, true);
            if (request.reserved != 0 || (request.flags & ~WKI_VFS_PATH_REQUIRE_DIRECTORY) != 0 || PATH == nullptr) {
                reject_request(-EINVAL);
                break;
            }
            ker::mod::security::Snapshot supplied;
            ker::mod::security::Snapshot mapped;
            int result = ker::mod::security::credential_record::decode({data, RECORD_SIZE}, supplied);
            if (result == 0) result = auth_policy::map_vfs_read_identity(identity_policy, supplied, mapped);
            if (result != 0) {
                reject_request(static_cast<int16_t>(result));
                break;
            }
            std::array<char, 512> full_path{}, full_visible_path{}, target{};
            result = prepare_mapped_scalar_path(export_path, export_name, PATH, request.data_len, COMPONENT_PATHS, full_path.data(),
                                                full_path.size(), full_visible_path.data(), full_visible_path.size());
            if (result == 0 && path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) result = -EPERM;
            auto* task = ker::mod::sched::get_current_task();
            if (result == 0 && task == nullptr) result = -ESRCH;
            if (result != 0) {
                reject_request(static_cast<int16_t>(result));
                break;
            }
            perf_record_vfs_server_begin(SERVER_OP, hdr->src_node, channel_id, CORRELATION, CALLSITE);
            uint64_t const LOCAL_STARTED_US = wki_now_us();
            ssize_t target_length = 0;
            if (REALPATH) {
                size_t canonical_length = 0;
                int const STATUS = ker::vfs::vfs_realpath_resolved_beneath_as(
                    *task, mapped, export_path, full_path.data(), target.data(), target.size(), &canonical_length,
                    (request.flags & WKI_VFS_PATH_REQUIRE_DIRECTORY) != 0, nullptr, absolute_symlink_root);
                target_length = STATUS != 0 ? STATUS : static_cast<ssize_t>(canonical_length);
                if (STATUS == 0 && !wki_vfs_realpath_result_valid(reinterpret_cast<const uint8_t*>(target.data()), canonical_length))
                    target_length = -EPROTO;
            } else {
                target_length = ker::vfs::vfs_readlink_resolved_beneath_as(
                    *task, mapped, export_path, full_path.data(), target.data(), target.size(),
                    (request.flags & WKI_VFS_PATH_REQUIRE_DIRECTORY) != 0, nullptr, absolute_symlink_root);
            }
            ssize_t const TARGET_LEN = target_length;
            perf_record_vfs_server_end(SERVER_OP, hdr->src_node, channel_id, CORRELATION,
                                       TARGET_LEN < 0 ? static_cast<int32_t>(TARGET_LEN) : 0,
                                       static_cast<uint32_t>(wki_now_us() - LOCAL_STARTED_US), TARGET_LEN < 0 ? 0 : TARGET_LEN, CALLSITE);
            if (TARGET_LEN < 0 || static_cast<size_t>(TARGET_LEN) > target.size()) {
                reject_request(TARGET_LEN < 0 ? static_cast<int16_t>(TARGET_LEN) : -EOVERFLOW);
                break;
            }
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(VfsPathIdentityHeader) + 512> response{};
            auto* header = reinterpret_cast<DevOpRespPayload*>(response.data());
            header->op_id = OP_VFS_PATH_AS;
            header->data_len = static_cast<uint16_t>(sizeof(VfsPathIdentityHeader) + TARGET_LEN);
            VfsPathIdentityHeader const REPLY{
                .operation = request.operation, .flags = 0, .data_len = static_cast<uint16_t>(TARGET_LEN), .reserved = 0};
            std::memcpy(response.data() + sizeof(*header), &REPLY, sizeof(REPLY));
            std::memcpy(response.data() + sizeof(*header) + sizeof(REPLY), target.data(), static_cast<size_t>(TARGET_LEN));
            send_buffered_resp(response.data(), static_cast<uint16_t>(sizeof(*header) + header->data_len));
            break;
        }

        case OP_VFS_SYMLINK:
            reject_request(-ENOTSUP);
            break;

        case OP_VFS_UNLINK:
        case OP_VFS_RMDIR:
            reject_request(-ENOTSUP);
            break;

        case OP_VFS_RENAME:
            reject_request(-ENOTSUP);
            break;

        case OP_VFS_CHMOD:
            // No caller identity exists in the legacy request. A trusted peer
            // must still use mapped CHMOD and the inode owner's actual policy.
            reject_request(-ENOTSUP);
            break;

        case OP_VFS_UTIMENS:
            // A write-capable peer must still supply the mapped caller identity.
            reject_request(-ENOTSUP);
            break;

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

            ker::vfs::File* local_file = nullptr;
            int ret = retain_server_file(channel_identity, fd_id, ServerFileUse::SYNC, local_file, -1);
            if (ret == 0) {
                ret = ker::vfs::vfs_fsync_file(local_file);
                ker::vfs::vfs_put_file(local_file);
            }

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_FSYNC;
            resp.status = static_cast<int16_t>(ret);
            resp.data_len = 0;
            send_simple_resp(resp);
            break;
        }

        case OP_VFS_TRUNCATE: {
            // The retained description, not the serving worker, grants writes.
            if (data_len != sizeof(int32_t) + sizeof(int64_t)) {
                reject_request(-EINVAL);
                break;
            }
            int32_t fd_id = 0;
            int64_t length = 0;
            std::memcpy(&fd_id, data, sizeof(fd_id));
            std::memcpy(&length, data + sizeof(fd_id), sizeof(length));
            ker::vfs::File* local_file = nullptr;
            s_vfs_lock.lock();
            auto* rfd = find_remote_fd(channel_identity, fd_id);
            if (rfd != nullptr && rfd->file != nullptr) {
                touch_remote_fd(rfd);
                local_file = rfd->file;
                ker::vfs::vfs_retain_file(local_file);
            }
            s_vfs_lock.unlock();
            int const RESULT = ker::vfs::vfs_ftruncate_file(local_file, static_cast<off_t>(length));
            ker::vfs::vfs_put_file(local_file);
            DevOpRespPayload RESPONSE{.op_id = OP_VFS_TRUNCATE, .status = static_cast<int16_t>(RESULT), .data_len = 0, .reserved = 0};
            send_simple_resp(RESPONSE);
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

            int64_t new_pos = -1;
            int ret = -1;
            ker::vfs::File* local_file = nullptr;
            if (retain_server_file(channel_identity, fd_id, ServerFileUse::SEEK, local_file, -1) == 0) {
                off_t const OLD_POS = local_file->pos;
                off_t const POS = local_file->fops->vfs_lseek(local_file, static_cast<off_t>(offset), 2 /* SEEK_END */);
                local_file->pos = OLD_POS;
                ker::vfs::vfs_put_file(local_file);
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

        case OP_VFS_READ_RDMA:
        case OP_VFS_READ_BULK:
        case OP_VFS_WRITE_RDMA:
            // Legacy raw staging/cookie control must never consume a secure
            // region or authorize native I/O, including on upgraded bindings.
            reject_request(-ENOTSUP);
            break;

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

constexpr auto vfs_proxy_lane_count_for_mount(std::string_view local_mount_path) -> uint8_t {
    constexpr std::string_view AUTO_MOUNT_PREFIX = "/wki/";
    constexpr std::string_view TMP_EXPORT_SUFFIX = "/tmp";
    if (local_mount_path.starts_with(AUTO_MOUNT_PREFIX)) {
        local_mount_path.remove_prefix(AUTO_MOUNT_PREFIX.size());
        size_t const HOST_END = local_mount_path.find('/');
        if (HOST_END != std::string_view::npos && HOST_END > 0 && local_mount_path.substr(HOST_END) == TMP_EXPORT_SUFFIX) {
            return static_cast<uint8_t>(VFS_PROXY_LANE_COUNT);
        }
    }
    return static_cast<uint8_t>(VFS_PROXY_DEFAULT_LANE_COUNT);
}
static_assert(vfs_proxy_lane_count_for_mount("/wki/wos-1/tmp") == VFS_PROXY_LANE_COUNT);
static_assert(vfs_proxy_lane_count_for_mount("/wki/wos-1/tmp/work") == VFS_PROXY_DEFAULT_LANE_COUNT);
static_assert(vfs_proxy_lane_count_for_mount("/wki/wos-1/cache/tmp") == VFS_PROXY_DEFAULT_LANE_COUNT);
static_assert(vfs_proxy_lane_count_for_mount("/mnt/tmp") == VFS_PROXY_DEFAULT_LANE_COUNT);
static_assert(vfs_proxy_lane_count_for_mount("/wki/tmp") == VFS_PROXY_DEFAULT_LANE_COUNT);
static_assert(vfs_proxy_lane_count_for_mount("/wki//tmp") == VFS_PROXY_DEFAULT_LANE_COUNT);
static_assert(vfs_proxy_lane_count_for_mount("/wki/wos-1/tmp/") == VFS_PROXY_DEFAULT_LANE_COUNT);

void retire_constructed_vfs_mount_group(ProxyVfsState* anchor, bool unpublished);

auto mount_vfs_proxy_lane(uint16_t owner_node, uint32_t resource_id, const char* local_mount_path, uint64_t resource_generation,
                          const ResourceIncarnationToken& owner_incarnation, uint32_t binding_peer_boot_epoch, uint64_t mount_group_id,
                          uint8_t lane_index, bool lane_anchor, RemoteVfsMountPublisher publisher, void* publisher_context) -> int {
    if (local_mount_path == nullptr) {
        return -EINVAL;
    }

    uint64_t const RESOURCE_GENERATION = resource_generation;
    uint32_t const BINDING_PEER_BOOT_EPOCH = binding_peer_boot_epoch;

    // Allocate proxy state
    uint8_t attach_cookie = 0;
    s_vfs_lock.lock();
    if (vfs_attach_blocked_by_retiring_binding_locked(owner_node, resource_id)) {
        s_vfs_lock.unlock();
        return -EAGAIN;
    }
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
            if (anchor->active && anchor->lanes_ready && !anchor->destroy_when_idle && !anchor->resources_releasing &&
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

    // Build the complete lane group privately. No mount-table row exposes
    // the anchor until all fallible attach and admission work has finished.
    // Auxiliary lanes are admitted through this private ready table; the
    // construction reference owns it across peer cleanup and publication.
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

    // Attach can block long enough for resource withdrawal or peer cleanup.
    // Validate both sides of the auxiliary sequence while the entire group
    // is private. Failure retires only this candidate, never another mount
    // of the same resource generation.
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
        retire_constructed_vfs_mount_group(state, true);
        release_vfs_proxy_lifecycle_ref(state);
        return PRE_AUX_VALIDATION;
    }

    uint8_t const TARGET_LANE_COUNT = vfs_proxy_lane_count_for_mount(local_mount_path);
    for (uint8_t lane_index = 1; lane_index < TARGET_LANE_COUNT; ++lane_index) {
        int const LANE_RET = mount_vfs_proxy_lane(owner_node, resource_id, local_mount_path, RESOURCE_GENERATION, owner_incarnation,
                                                  BINDING_PEER_BOOT_EPOCH, mount_group_id, lane_index, false, nullptr, nullptr);
        if (LANE_RET != 0) {
            ker::mod::dbg::log("[WKI] Remote VFS auxiliary lane unavailable: node=0x%04x res_id=%u lane=%u ret=%d", owner_node, resource_id,
                               lane_index, LANE_RET);
            // Capacity and transport failures may still leave a useful
            // partial group. NOT_FOUND and STALE_RESOURCE are definitive:
            // the server proved that lane zero's advertised identity is no
            // longer attachable, so do not publish that stale anchor.
            if (LANE_RET == -ENOENT || LANE_RET == -ESTALE) {
                retire_constructed_vfs_mount_group(state, true);
                release_vfs_proxy_lifecycle_ref(state);
                return LANE_RET;
            }
            break;
        }
    }

    int const POST_AUX_VALIDATION = validate_mount_binding();
    if (POST_AUX_VALIDATION != 0) {
        retire_constructed_vfs_mount_group(state, true);
        release_vfs_proxy_lifecycle_ref(state);
        return POST_AUX_VALIDATION;
    }

    s_vfs_lock.lock();
    bool const ANCHOR_STILL_ACTIVE = state->active && !state->destroy_when_idle && state->lanes_ready && state->lane_count > 0;
    uint8_t const LANE_COUNT = state->lane_count;
    s_vfs_lock.unlock();
    if (!ANCHOR_STILL_ACTIVE) {
        retire_constructed_vfs_mount_group(state, true);
        release_vfs_proxy_lifecycle_ref(state);
        return WKI_ERR_PEER_FENCED;
    }

    // Publication is the commit. The callback must either publish one row
    // and return success, or return an error without publishing any row.
    // No transport failure after this point may turn that commit into a
    // failed mount syscall with visible namespace changes.
    int const MOUNT_RET = publisher != nullptr
                              ? publisher(local_mount_path, state, &g_remote_vfs_fops, publisher_context)
                              : ker::vfs::mount_filesystem(local_mount_path, "remote", nullptr, 0, nullptr, state, &g_remote_vfs_fops);
    if (MOUNT_RET != 0) {
        retire_constructed_vfs_mount_group(state, true);
        release_vfs_proxy_lifecycle_ref(state);
        return MOUNT_RET;
    }

    s_vfs_lock.lock();
    state->mount_configured = true;
    bool const RETIRED_DURING_PUBLICATION = !state->active || !state->lanes_ready || state->destroy_when_idle;
    s_vfs_lock.unlock();
    // Peer withdrawal may race the publisher, just as it may follow any
    // successful mount. Retire this committed group without changing its
    // successful result. An actual concurrent VFS removal owns its own
    // mount_released state; do not reset it or claim its teardown twice.
    if (RETIRED_DURING_PUBLICATION || validate_mount_binding() != 0) retire_constructed_vfs_mount_group(state, false);

    release_vfs_proxy_lifecycle_ref(state);
    ker::mod::dbg::log("[WKI] Remote VFS mounted: %s -> node=0x%04x res_id=%u ch=%u lanes=%u", local_mount_path, owner_node, resource_id,
                       ASSIGNED_CHANNEL, LANE_COUNT);
    return 0;
}

}  // namespace

auto wki_remote_vfs_mount(uint16_t owner_node, uint32_t resource_id, const char* local_mount_path, uint64_t expected_resource_generation,
                          RemoteVfsMountPublisher publisher, void* publisher_context) -> int {
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
                                mount_group_id, 0, true, publisher, publisher_context);
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

void finish_vfs_proxy_group_unmount(const PendingVfsProxyGroupTeardown& group, bool mount_removed = false) {
    if (group.anchor == nullptr || group.count == 0) {
        return;
    }

    for (size_t i = 0; i < group.count; ++i) {
        finish_vfs_proxy_lane_teardown(group.lanes.at(i), group.detach_remote.at(i));
    }

    int const UNMOUNT_RET = mount_removed ? 0 : ker::vfs::unmount_filesystem_by_private_data(static_cast<const void*>(group.anchor));
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

// The caller retains its construction reference. This also covers private
// auxiliary lanes, which have no mount_configured row and cannot be found by
// generation-wide mount retirement. Unpublished failure must never affect a
// separate successful mount of the same resource.
void retire_constructed_vfs_mount_group(ProxyVfsState* anchor, bool unpublished) {
    PendingVfsProxyGroupTeardown group{};
    s_vfs_lock.lock();
    if (!anchor->destroy_when_idle) claim_vfs_proxy_group_unmount_locked(anchor, group);
    s_vfs_lock.unlock();
    finish_vfs_proxy_group_unmount(group, unpublished);
}

auto claim_retired_vfs_mount_retry_locked(ProxyVfsState* state) -> bool {
    if (state == nullptr || !state->lane_anchor || !state->mount_configured || !state->destroy_when_idle || state->mount_released ||
        state->resources_releasing || !proxy_is_idle_for_resource_release_locked(state)) {
        return false;
    }
    // The unreleased mount keeps the anchor registered. This reference also
    // excludes another retry and prevents retirement while VFS drops its row.
    ++state->lifecycle_refs;
    return true;
}

auto finish_retired_vfs_mount_retry(ProxyVfsState* state) -> int {
    int const RESULT = ker::vfs::unmount_filesystem_by_private_data(state);
    if (RESULT == 0 || RESULT == -ENOENT) mark_vfs_proxy_mount_released_and_maybe_destroy(state);
    release_vfs_proxy_lifecycle_ref(state);
    return RESULT;
}
}  // namespace

auto wki_remote_vfs_pin_mount_teardown(const void* private_data) -> ProxyVfsState* {
    if (private_data == nullptr) return nullptr;
    s_vfs_lock.lock();
    ProxyVfsState* owner = nullptr;
    for (auto& proxy : g_vfs_proxies) {
        if (proxy.get() == private_data && proxy->lane_anchor) {
            owner = proxy.get();
            ++owner->lifecycle_refs;
            break;
        }
    }
    s_vfs_lock.unlock();
    return owner;
}

void wki_remote_vfs_finish_mount_teardown(ProxyVfsState* owner, bool mount_removed) {
    if (owner == nullptr) return;
    if (mount_removed) {
        PendingVfsProxyGroupTeardown group{};
        s_vfs_lock.lock();
        owner->mount_released = true;
        // Peer/resource teardown may have claimed the group while VFS was
        // removing its row. Its lifecycle pins own that cleanup; do not claim
        // the same waiters/channels twice. Publication can also still be in
        // flight, so mount_configured is deliberately not required here.
        if (!owner->destroy_when_idle) claim_vfs_proxy_group_unmount_locked(owner, group);
        s_vfs_lock.unlock();
        finish_vfs_proxy_group_unmount(group, true);
    }
    release_vfs_proxy_lifecycle_ref(owner);
}

void wki_remote_vfs_process_pending_unmounts() {
    constexpr size_t SCAN_LIMIT = 32;
    constexpr uint64_t RETRY_INTERVAL_US = 1'000'000;
    static size_t cursor = 0;
    static uint64_t retry_after_us = 0;
    ProxyVfsState* selected = nullptr;
    s_vfs_lock.lock();
    uint64_t const NOW = wki_now_us();
    if (NOW >= retry_after_us) {
        retry_after_us = wki_future_deadline_us(NOW, RETRY_INTERVAL_US);
        size_t const COUNT = g_vfs_proxies.size();
        for (size_t scanned = 0; scanned < std::min(COUNT, SCAN_LIMIT); ++scanned) {
            cursor %= COUNT;
            auto* state = g_vfs_proxies.at(cursor++).get();
            if (claim_retired_vfs_mount_retry_locked(state)) {
                selected = state;
                break;
            }
        }
    }
    s_vfs_lock.unlock();
    // A live File may still pin the mount through a hidden lane. EBUSY keeps
    // the exact anchor for a later pass; never force removal or match by path.
    if (selected != nullptr) static_cast<void>(finish_retired_vfs_mount_retry(selected));
}

auto wki_remote_vfs_auto_mount_waits_for_retired_path(const char* path) -> bool {
    if (path == nullptr || path[0] != '/') return true;
    s_vfs_lock.lock();
    bool blocked = false;
    for (auto const& proxy : g_vfs_proxies) {
        if (!proxy || !proxy->lane_anchor || !proxy->mount_configured || !proxy->destroy_when_idle || proxy->mount_released) continue;
        auto const& parent = proxy->local_mount_path;
        size_t const LENGTH = strnlen(parent.data(), parent.size());
        if (LENGTH == 0 || LENGTH == parent.size() || parent[0] != '/') continue;
        if (std::strncmp(path, parent.data(), LENGTH) == 0 && (LENGTH == 1 || path[LENGTH] == '\0' || path[LENGTH] == '/')) {
            blocked = true;
            break;
        }
    }
    s_vfs_lock.unlock();
    return blocked;
}

#ifdef WOS_SELFTEST
auto wki_remote_vfs_selftest_retired_ancestor_admission() -> bool {
    namespace vfs = ker::vfs;
    constexpr auto PATH = "/tmp/ktest_auto_retired";
    constexpr auto CHILD = "/tmp/ktest_auto_retired/child";
    s_vfs_lock.lock();
    auto const GROUP_ID = allocate_vfs_mount_group_id_locked();
    auto* anchor = create_vfs_proxy_state_locked(WKI_NODE_INVALID, 0, 0, {}, 0, PATH, GROUP_ID, 0, true);
    anchor->active = true;
    anchor->mount_configured = true;
    anchor->lanes.at(0) = anchor;
    anchor->lane_count = 1;
    anchor->lanes_ready = true;
    s_vfs_lock.unlock();
    struct Cleanup {
        ProxyVfsState* anchor;
        ~Cleanup() {
            static_cast<void>(vfs::unmount_filesystem(CHILD));
            wki_remote_vfs_unmount(PATH);
            static_cast<void>(vfs::unmount_filesystem_by_private_data(anchor));
            release_vfs_proxy_lifecycle_ref(anchor);
        }
    } cleanup{anchor};
    if (wki_remote_vfs_auto_mount_waits_for_retired_path(PATH) ||
        vfs::mount_filesystem(PATH, "remote", nullptr, 0, nullptr, anchor, &g_remote_vfs_fops) != 0 ||
        vfs::mount_filesystem(CHILD, "tmpfs", nullptr) != 0)
        return false;
    // Withdraw the old parent while its old child is still mounted. The
    // construction reference keeps the real retry worker out of this fixture.
    wki_remote_vfs_unmount(PATH);
    auto mount = vfs::find_mount_point(PATH);
    bool const RETAINED = mount && mount->private_data == anchor && anchor->destroy_when_idle && !anchor->mount_released;
    mount.reset();
    if (!RETAINED || !wki_remote_vfs_auto_mount_waits_for_retired_path(PATH) || !wki_remote_vfs_auto_mount_waits_for_retired_path(CHILD) ||
        !wki_remote_vfs_auto_mount_waits_for_retired_path("/tmp/ktest_auto_retired/child/grandchild") ||
        wki_remote_vfs_auto_mount_waits_for_retired_path("/tmp/ktest_auto_retired-sibling") ||
        wki_remote_vfs_auto_mount_waits_for_retired_path("/tmp"))
        return false;
    // Removing only the old child permits exact parent retirement. Pending
    // successors may now proceed; no force or pathname replacement is needed.
    if (vfs::unmount_filesystem(CHILD) != 0 || vfs::unmount_filesystem_by_private_data(anchor) != 0 || !anchor->mount_released ||
        wki_remote_vfs_auto_mount_waits_for_retired_path(PATH) || wki_remote_vfs_auto_mount_waits_for_retired_path(CHILD))
        return false;
    return true;
}

auto wki_remote_vfs_selftest_manual_mount_teardown() -> bool {
    namespace vfs = ker::vfs;
    constexpr auto PATH = "/tmp/ktest_manual_remote_mount";
    // Ordinary manual retirement, a WKI teardown already in progress, and
    // manual retirement during the mount publisher's construction interval.
    for (unsigned scenario = 0; scenario < 3; ++scenario) {
        s_vfs_lock.lock();
        auto const GROUP_ID = allocate_vfs_mount_group_id_locked();
        auto* anchor = create_vfs_proxy_state_locked(WKI_NODE_INVALID, 0, 0, {}, 0, PATH, GROUP_ID, 0, true);
        auto* lane = create_vfs_proxy_state_locked(WKI_NODE_INVALID, 0, 0, {}, 0, PATH, GROUP_ID, 1, false);
        anchor->active = lane->active = true;
        anchor->mount_configured = scenario != 2;
        anchor->lanes.at(0) = anchor;
        anchor->lanes.at(1) = lane;
        anchor->lane_count = 2;
        anchor->lanes_ready = true;
        s_vfs_lock.unlock();
        // Construction references keep both test objects inspectable until
        // final assertions. No peer, channel or attach cookie is installed.
        bool ok = vfs::mount_filesystem(PATH, "remote", nullptr, vfs::MOUNT_FLAG_RDONLY | vfs::MOUNT_FLAG_NOSUID, nullptr, anchor,
                                        &g_remote_vfs_fops) == 0;
        auto mount = vfs::find_mount_point(PATH);
        auto* pinned = mount.get();
        bool const PINNED = pinned != nullptr && pinned->private_data == anchor && vfs::retain_mount_for_open_file(pinned);
        mount.reset();
        ok = PINNED && ok;
        if (PINNED) ok = vfs::unmount_filesystem(PATH) == -EBUSY && ok;
        mount = vfs::find_mount_point(PATH);
        ok = mount && mount->private_data == anchor && mount->read_only && mount->nosuid && ok;
        mount.reset();
        // A cancelled teardown pin must not retire the still-published group.
        auto* cancelled = wki_remote_vfs_pin_mount_teardown(anchor);
        ok = cancelled == anchor && ok;
        wki_remote_vfs_finish_mount_teardown(cancelled, false);
        s_vfs_lock.lock();
        ok = anchor->active && lane->active && anchor->lanes_ready && anchor->lane_count == 2 && !anchor->mount_released &&
             !anchor->destroy_when_idle && anchor->lifecycle_refs == 1 && lane->lifecycle_refs == 1 && ok;
        PendingVfsProxyGroupTeardown concurrent{};
        if (scenario == 1) claim_vfs_proxy_group_unmount_locked(anchor, concurrent);
        s_vfs_lock.unlock();
        if (PINNED) vfs::release_mount_from_open_file(pinned);
        ok = vfs::unmount_filesystem(PATH) == 0 && ok;
        ProxyVfsState replacement{};
        ok = vfs::mount_filesystem(PATH, "remote", nullptr, vfs::MOUNT_FLAG_RDONLY, nullptr, &replacement, &g_remote_vfs_fops) == 0 && ok;
        if (scenario == 1) finish_vfs_proxy_group_unmount(concurrent);
        mount = vfs::find_mount_point(PATH);
        ok = mount && mount->private_data == &replacement && mount->read_only && ok;
        mount.reset();
        s_vfs_lock.lock();
        ok = !anchor->active && !lane->active && !anchor->lanes_ready && anchor->lane_count == 0 && anchor->mount_released &&
             lane->mount_released && anchor->destroy_when_idle && lane->destroy_when_idle && anchor->lifecycle_refs == 1 &&
             lane->lifecycle_refs == 1 && ok;
        s_vfs_lock.unlock();
        ok = vfs::unmount_filesystem_by_private_data(&replacement) == 0 && ok;
        // Also close out a failed assertion's surviving original row/group.
        PendingVfsProxyGroupTeardown cleanup{};
        s_vfs_lock.lock();
        if (!anchor->destroy_when_idle) claim_vfs_proxy_group_unmount_locked(anchor, cleanup);
        s_vfs_lock.unlock();
        finish_vfs_proxy_group_unmount(cleanup);
        release_vfs_proxy_lifecycle_ref(lane);
        release_vfs_proxy_lifecycle_ref(anchor);
        s_vfs_lock.lock();
        for (auto& proxy : g_vfs_proxies) {
            if (proxy->mount_group_id == GROUP_ID) ok = false;
        }
        s_vfs_lock.unlock();
        if (!ok) return false;
    }
    return true;
}

auto wki_remote_vfs_selftest_mount_candidate_isolation() -> bool {
    namespace vfs = ker::vfs;
    constexpr auto CANDIDATE = "/tmp/ktest_mount_candidate";
    constexpr auto CHILD = "/tmp/ktest_mount_candidate/child";
    constexpr auto OTHER = "/tmp/ktest_mount_other";
    for (bool committed : {false, true}) {
        s_vfs_lock.lock();
        auto const CANDIDATE_GROUP = allocate_vfs_mount_group_id_locked();
        auto const OTHER_GROUP = allocate_vfs_mount_group_id_locked();
        auto* anchor = create_vfs_proxy_state_locked(WKI_NODE_INVALID, 0, 77, {}, 0, CANDIDATE, CANDIDATE_GROUP, 0, true);
        auto* lane = create_vfs_proxy_state_locked(WKI_NODE_INVALID, 0, 77, {}, 0, CANDIDATE, CANDIDATE_GROUP, 1, false);
        auto* other = create_vfs_proxy_state_locked(WKI_NODE_INVALID, 0, 77, {}, 0, OTHER, OTHER_GROUP, 0, true);
        anchor->active = lane->active = other->active = true;
        anchor->mount_configured = committed;
        anchor->lanes.at(0) = anchor;
        anchor->lanes.at(1) = lane;
        anchor->lane_count = 2;
        anchor->lanes_ready = true;
        other->mount_configured = other->lanes_ready = true;
        other->lanes.at(0) = other;
        other->lane_count = 1;
        s_vfs_lock.unlock();
        // Same resource/generation, different groups. Construction pins retain
        // every real registry object for inspection; no peer/channel is used.
        bool ok = vfs::mount_filesystem(OTHER, "remote", nullptr, 0, nullptr, other, &g_remote_vfs_fops) == 0;
        if (committed) ok = vfs::mount_filesystem(CANDIDATE, "remote", nullptr, 0, nullptr, anchor, &g_remote_vfs_fops) == 0 && ok;
        ok = vfs::mount_filesystem(CHILD, "tmpfs", nullptr) == 0 && ok;
        auto const GENERATION = vfs::mount_table_generation_snapshot();
        retire_constructed_vfs_mount_group(anchor, !committed);
        auto retained_other = vfs::find_mount_point(OTHER);
        auto child = vfs::find_mount_point(CHILD);
        ok = retained_other && retained_other->private_data == other && child && child->fs_type == vfs::FSType::TMPFS && ok;
        retained_other.reset();
        child.reset();
        if (!committed) ok = vfs::mount_table_generation_snapshot() == GENERATION && ok;
        s_vfs_lock.lock();
        ok = !anchor->active && !lane->active && anchor->destroy_when_idle && lane->destroy_when_idle && !anchor->lanes_ready &&
             anchor->lane_count == 0 && lane->mount_released && anchor->mount_released == !committed && anchor->lifecycle_refs == 1 &&
             lane->lifecycle_refs == 1 && other->active && other->lanes_ready && other->mount_configured && !other->destroy_when_idle &&
             !other->mount_released && other->lifecycle_refs == 1 && ok;
        s_vfs_lock.unlock();
        // A committed mount may become unavailable after an asynchronous
        // retirement, but its busy child must retain the exact parent owner.
        ok = vfs::unmount_filesystem(CHILD) == 0 && ok;
        if (committed) ok = vfs::unmount_filesystem_by_private_data(anchor) == 0 && ok;
        retire_constructed_vfs_mount_group(other, false);
        release_vfs_proxy_lifecycle_ref(lane);
        release_vfs_proxy_lifecycle_ref(anchor);
        release_vfs_proxy_lifecycle_ref(other);
        s_vfs_lock.lock();
        for (auto const& proxy : g_vfs_proxies)
            if (proxy->mount_group_id == CANDIDATE_GROUP || proxy->mount_group_id == OTHER_GROUP) ok = false;
        s_vfs_lock.unlock();
        if (!ok) return false;
    }
    return true;
}

auto wki_remote_vfs_selftest_retired_mount_retry() -> bool {
    namespace vfs = ker::vfs;
    constexpr auto PATH = "/tmp/ktest_retired_remote_mount";
    for (bool replace : {false, true}) {
        auto proxy = std::make_unique<ProxyVfsState>();
        auto* state = proxy.get();
        state->lane_anchor = true;
        state->mount_configured = true;
        state->destroy_when_idle = true;
        state->resources_released = true;
        // Keep the test owner private so the live auto-attach worker cannot
        // consume a deliberate busy/replacement checkpoint between assertions.
        if (vfs::mount_filesystem(PATH, "remote", nullptr, 0, nullptr, state, &g_remote_vfs_fops) != 0) return false;
        auto mount = vfs::find_mount_point(PATH);
        auto* pinned = mount.get();
        bool const PINNED = pinned != nullptr && pinned->private_data == state && vfs::retain_mount_for_open_file(pinned);
        mount.reset();
        bool ok = PINNED;
        s_vfs_lock.lock();
        bool const FIRST_CLAIM = claim_retired_vfs_mount_retry_locked(state);
        bool const EXCLUSIVE = !claim_retired_vfs_mount_retry_locked(state);
        s_vfs_lock.unlock();
        ok = FIRST_CLAIM && EXCLUSIVE && ok;
        if (FIRST_CLAIM) ok = finish_retired_vfs_mount_retry(state) == -EBUSY && ok;
        mount = vfs::find_mount_point(PATH);
        ok = mount && mount->private_data == state && !state->mount_released && state->lifecycle_refs == 0 && ok;
        mount.reset();
        if (PINNED) vfs::release_mount_from_open_file(pinned);
        ProxyVfsState replacement{};
        if (replace) {
            ok = vfs::unmount_filesystem_by_private_data(state) == 0 && ok;
            ok = vfs::mount_filesystem(PATH, "remote", nullptr, 0, nullptr, &replacement, &g_remote_vfs_fops) == 0 && ok;
        }
        s_vfs_lock.lock();
        bool const SECOND_CLAIM = claim_retired_vfs_mount_retry_locked(state);
        s_vfs_lock.unlock();
        ok = SECOND_CLAIM && ok;
        if (SECOND_CLAIM) ok = finish_retired_vfs_mount_retry(state) == (replace ? -ENOENT : 0) && ok;
        mount = vfs::find_mount_point(PATH);
        ok = mount && mount->private_data != state && (!replace || mount->private_data == &replacement) && ok;
        mount.reset();
        if (replace) ok = vfs::unmount_filesystem_by_private_data(&replacement) == 0 && ok;
        static_cast<void>(vfs::unmount_filesystem_by_private_data(state));
        if (!ok) return false;
    }
    return true;
}
#endif

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
        if (proxy == nullptr || count >= max) {
            continue;
        }

        auto& row = out[count++];
        row.owner_node = proxy->owner_node;
        row.assigned_channel = proxy->assigned_channel;
        row.assigned_channel_generation = proxy->assigned_channel_generation;
        row.resource_id = proxy->resource_id;
        row.resource_generation = proxy->resource_generation;
        row.owner_boot_epoch = proxy->owner_incarnation.owner_boot_epoch;
        row.resource_incarnation = proxy->owner_incarnation.resource_incarnation;
        row.binding_peer_boot_epoch = proxy->binding_peer_boot_epoch;
        row.mount_group_id = proxy->mount_group_id;
        row.lane_index = proxy->lane_index;
        row.lane_count = proxy->lane_count;
        row.lane_anchor = proxy->lane_anchor;
        row.lanes_ready = proxy->lanes_ready;
        row.active = proxy->active;
        row.epoch_reset_pending = proxy->epoch_reset_pending;
        row.op_pending = proxy->op_pending.load(std::memory_order_acquire);
        row.op_expected_id = proxy->op_expected_id;
        row.op_expected_seq = proxy->op_expected_seq;
        row.op_generation = proxy->op_generation;
        row.op_waiter_pid = proxy->op_waiter_pid;
        row.op_retiring_waiter_pid = proxy->op_retiring_waiter_pid;
        row.op_slot_waiter_count = static_cast<uint32_t>(proxy->op_slot_waiter_count);
        row.attach_pending = proxy->attach_pending.load(std::memory_order_acquire);
        row.attach_expected_cookie = proxy->attach_expected_cookie;
        row.binding_attach_cookie = proxy->binding_attach_cookie;
        row.detach_pending = proxy->detach_pending;
        row.detach_retry_in_progress = proxy->detach_retry_in_progress;
        row.detach_peer_boot_epoch = proxy->detach_peer_boot_epoch;
        row.open_file_refs = proxy->open_file_refs.load(std::memory_order_acquire);
        row.lifecycle_refs = proxy->lifecycle_refs;
        row.destroy_when_idle = proxy->destroy_when_idle;
        row.mount_configured = proxy->mount_configured;
        row.mount_released = proxy->mount_released;
        row.resources_releasing = proxy->resources_releasing;
        row.resources_released = proxy->resources_released;
        row.rdma_capable = proxy->rdma_capable;
        row.bulk_rdma_capable = proxy->bulk_rdma_capable;
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

namespace {
auto directory_grant_client_rpc(ProxyVfsState* state, const WkiChannelIdentity& identity, const ker::mod::security::Snapshot& credentials,
                                VfsDirectoryGrantRequest request, ker::vfs::MountPolicy policy, ker::vfs::File** output,
                                VfsDirectoryGrant& grant_output, char* name = nullptr, size_t capacity = 0) -> int {
    if (state == nullptr || output == nullptr) return -EINVAL;
    auto const REQUEST_CREDENTIALS = credentials;
    if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_RESOURCE_INCARNATION)) return -ENOTSUP;
    PinPreparation preparation(state, PinPurpose::DIRECTORY);
    if (preparation.status() != 0) return preparation.status();
    auto* file = preparation.file();
    auto* context = preparation.context();
    s_vfs_lock.lock();
    auto const INCARNATION = state->owner_incarnation;
    auto const RESOURCE = state->resource_id;
    bool const MATCHES = vfs_channel_identity_matches(proxy_channel_identity_locked(state), identity);
    s_vfs_lock.unlock();
    if (!MATCHES || !wki_resource_incarnation_valid(INCARNATION)) return -ENOTCONN;
    if (request.operation == VfsDirectoryGrantOperation::ADOPT &&
        (request.grant.owner_node != state->owner_node || request.grant.resource_id != RESOURCE ||
         !wki_resource_incarnation_equal(request.grant.owner_incarnation, INCARNATION)))
        return -ESTALE;
    VfsFeaturesRequest const QUERY{WKI_VFS_FEATURES_VERSION, 0};
    VfsFeaturesResponse features{};
    uint16_t length = 0;
    int status = vfs_proxy_send_and_wait(state, OP_VFS_FEATURES, reinterpret_cast<const uint8_t*>(&QUERY), sizeof(QUERY), &features,
                                         sizeof(features), &length, VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &identity);
    if (status != 0) return status;
    if (!wki_vfs_features_response_valid(features, length)) return -EPROTO;
    if ((features.features & WKI_VFS_FEATURE_DIRECTORY_GRANT) == 0) return -ENOTSUP;
    uint64_t const TOKEN = ker::vfs::ExecContentState::allocate_identity();
    if (TOKEN == 0) return -EOVERFLOW;
    request.pin_token = TOKEN;
    if (!wki_vfs_directory_grant_request_valid(request, sizeof(request) + WKI_VFS_DIRECTORY_CALLER_SIZE)) return -EINVAL;
    uint16_t const SLOT = reserve_exec_cancellation(state, TOKEN, true, &identity);
    if (SLOT == VFS_EXEC_CANCEL_SLOT_NONE) return -EAGAIN;
    preparation.own_cancellation(SLOT, TOKEN);
    auto const RECORD = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
    std::array<uint8_t, sizeof(request) + WKI_VFS_DIRECTORY_CALLER_SIZE> bytes{};
    std::memcpy(bytes.data(), &request, sizeof(request));
    std::memcpy(bytes.data() + sizeof(request), RECORD.data(), RECORD.size());
    std::array<uint8_t, sizeof(VfsDirectoryGrantResponse) + 511> reply{};
    status = vfs_proxy_send_and_wait(state, OP_VFS_DIRECTORY_GRANT, bytes.data(), bytes.size(), reply.data(), reply.size(), &length,
                                     VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &identity);
    VfsDirectoryGrantResponse result{};
    if (status == 0) {
        if (length < sizeof(result)) {
            status = -EPROTO;
        } else {
            std::memcpy(&result, reply.data(), sizeof(result));
            auto const& grant = result.grant;
            bool const BASE = wki_vfs_directory_grant_response_valid(result, TOKEN, reply.data() + sizeof(result), length) &&
                              grant.owner_node == state->owner_node && grant.resource_id == RESOURCE &&
                              wki_resource_incarnation_equal(grant.owner_incarnation, INCARNATION);
            bool const EXACT = request.operation == VfsDirectoryGrantOperation::ISSUE
                                   ? grant.source_node == g_wki.my_node_id && grant.source_boot_epoch == g_wki.local_boot_epoch &&
                                         grant.target_node == request.target_node && grant.target_boot_epoch == request.target_boot_epoch &&
                                         grant.submission_id == request.submission_id &&
                                         (grant.policy & request.policy) == request.policy && result.directory.fd != request.directory_fd
                                   : wki_vfs_directory_grant_equal(grant, request.grant);
            if (!BASE || !EXACT)
                status = -EPROTO;
            else if (name != nullptr && static_cast<size_t>(result.directory.path_length) + 1 > capacity)
                status = -ERANGE;
        }
    }
    if (status != 0) {
        // Never replay ISSUE/ADOPT or use untrusted reply fd bytes for cleanup.
        // Both operations create a separate v1 directory cancellation owner.
        preparation.cancel();
        return status;
    }
    context->proxy = state;
    context->remote_fd = result.directory.fd;
    context->exec_cancel_slot = SLOT;
    context->directory_pin = true;
    file->fops = &g_remote_vfs_fops;
    file->fs_type = ker::vfs::FSType::REMOTE;
    file->fd = -1;
    file->refcount = 1;
    file->is_directory = true;
    file->open_flags = ker::vfs::O_DIRECTORY | ker::vfs::O_NO_CACHE | ker::vfs::FILE_ACCESS_SEARCH_ONLY;
    file->mount_policy = policy.restricted_by({.read_only = (result.grant.policy & WKI_VFS_DIRECTORY_GRANT_READONLY) != 0,
                                               .nosuid = (result.grant.policy & WKI_VFS_DIRECTORY_GRANT_NOSUID) != 0});
    if (name != nullptr) {
        std::memcpy(name, reply.data() + sizeof(result), result.directory.path_length);
        name[result.directory.path_length] = '\0';
    }
    grant_output = result.grant;
    *output = preparation.publish();
    return 0;
}
}  // namespace

auto wki_remote_vfs_directory_owner(ker::vfs::File* directory) -> uint16_t {
    if (directory == nullptr || directory->fops != &g_remote_vfs_fops || directory->private_data == nullptr || !directory->is_directory)
        return WKI_NODE_INVALID;
    auto const* context = static_cast<const RemoteFileContext*>(directory->private_data);
    if (!context->directory_pin || context->exec_cancel_slot >= g_exec_cancellations.size()) return WKI_NODE_INVALID;
    auto const& owner = g_exec_cancellations.at(context->exec_cancel_slot);
    if (!owner.directory || owner.proxy != context->proxy || owner.phase != ExecCancellationPhase::OWNED) return WKI_NODE_INVALID;
    return owner.identity.peer_node_id;
}

auto wki_remote_vfs_issue_directory_grant(ker::vfs::File* directory, const ker::mod::security::Snapshot& credentials, uint16_t target_node,
                                          uint32_t target_boot_epoch, uint64_t submission_id, ker::vfs::MountPolicy additional_policy,
                                          ker::vfs::File** staging_pin, VfsDirectoryGrant& output) -> int {
    if (directory == nullptr || directory->fops != &g_remote_vfs_fops || directory->private_data == nullptr || !directory->is_directory)
        return -EBADF;
    if (staging_pin == nullptr || target_node == WKI_NODE_INVALID || target_node == WKI_NODE_BROADCAST || target_boot_epoch == 0 ||
        submission_id == 0)
        return -EINVAL;
    auto const REQUEST_CREDENTIALS = credentials;
    auto* context = static_cast<RemoteFileContext*>(directory->private_data);
    if (!context->directory_pin || context->exec_cancel_slot >= g_exec_cancellations.size()) return -EBADF;
    auto const& owner = g_exec_cancellations.at(context->exec_cancel_slot);
    // The caller's File reference keeps this OWNED cancellation record stable.
    if (!owner.directory || owner.proxy != context->proxy || owner.phase != ExecCancellationPhase::OWNED) return -EBADF;
    auto const POLICY = directory->mount_policy.restricted_by(additional_policy);
    VfsDirectoryGrantRequest request{.policy = directory_grant_policy(POLICY),
                                     .target_node = target_node,
                                     .directory_fd = context->remote_fd,
                                     .target_boot_epoch = target_boot_epoch,
                                     .source_pin_token = owner.token,
                                     .submission_id = submission_id};
    return directory_grant_client_rpc(context->proxy, owner.identity, REQUEST_CREDENTIALS, request, POLICY, staging_pin, output);
}

auto wki_remote_vfs_import_directory_grant(void* mount_private_data, const ker::mod::security::Snapshot& credentials,
                                           const VfsDirectoryGrant& grant, ker::vfs::File** output, char* name, size_t capacity) -> int {
    if (output == nullptr || name == nullptr || capacity == 0 || !wki_vfs_directory_grant_valid(grant)) return -EINVAL;
    if (grant.target_node != g_wki.my_node_id || grant.target_boot_epoch != g_wki.local_boot_epoch) return -ESTALE;
    auto const REQUEST_CREDENTIALS = credentials;
    auto* state = acquire_vfs_proxy_lane(static_cast<ProxyVfsState*>(mount_private_data), false, false);
    ProxyLifecycleRefGuard lane_guard(state);
    if (state == nullptr) return -ENOTCONN;
    s_vfs_lock.lock();
    auto const IDENTITY = proxy_channel_identity_locked(state);
    s_vfs_lock.unlock();
    VfsDirectoryGrantRequest request{.operation = VfsDirectoryGrantOperation::ADOPT, .grant = grant};
    VfsDirectoryGrant copied{};
    return directory_grant_client_rpc(state, IDENTITY, REQUEST_CREDENTIALS, request, {}, output, copied, name, capacity);
}

auto wki_remote_vfs_pin_directory(const char* path, void* mount_private_data, const ker::mod::security::Snapshot& credentials,
                                  ker::vfs::File** output, char* name, size_t capacity, ker::vfs::File* directory) -> int {
    if (path == nullptr || output == nullptr || name == nullptr || capacity == 0) return -EINVAL;
    auto const REQUEST_CREDENTIALS = credentials;
    size_t const PATH_LENGTH = strnlen(path, 512);
    if (PATH_LENGTH > 511) return -ENAMETOOLONG;
    RemoteFileContext* source = nullptr;
    if (directory != nullptr) {
        if (directory->fops != &g_remote_vfs_fops || directory->private_data == nullptr) return -EBADF;
        if (!directory->is_directory) return -ENOTDIR;
        source = static_cast<RemoteFileContext*>(directory->private_data);
        if (PATH_LENGTH == 0) return -ENOENT;
    }
    auto* state = source != nullptr ? source->proxy : acquire_vfs_proxy_lane(static_cast<ProxyVfsState*>(mount_private_data), false, false);
    ProxyLifecycleRefGuard lane_guard(source == nullptr ? state : nullptr);
    if (state == nullptr) return -ENOTCONN;
    int const VALID = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(path), PATH_LENGTH,
                                              wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
    if (VALID != 0) return VALID;
    auto const RECORD = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
    std::array<uint8_t, sizeof(VfsDirectoryRequest) + WKI_VFS_DIRECTORY_CALLER_SIZE + 511> request{};
    std::memcpy(request.data() + sizeof(VfsDirectoryRequest), RECORD.data(), RECORD.size());
    std::memcpy(request.data() + sizeof(VfsDirectoryRequest) + RECORD.size(), path, PATH_LENGTH);
    PinPreparation preparation(state, PinPurpose::DIRECTORY);
    if (preparation.status() != 0) return preparation.status();
    // The preparation's open reference now owns proxy lifetime across waits.
    // Do not strand the selected lane's temporary lifecycle ref on task exit.
    lane_guard.reset();
    auto* file = preparation.file();
    auto* context = preparation.context();
    s_vfs_lock.lock();
    auto const IDENTITY = proxy_channel_identity_locked(state);
    s_vfs_lock.unlock();
    VfsFeaturesRequest const QUERY{WKI_VFS_FEATURES_VERSION, 0};
    VfsFeaturesResponse features{};
    uint16_t length = 0;
    int status = vfs_proxy_send_and_wait(state, OP_VFS_FEATURES, reinterpret_cast<const uint8_t*>(&QUERY), sizeof(QUERY), &features,
                                         sizeof(features), &length, VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &IDENTITY);
    if (status != 0) return status;
    if (!wki_vfs_features_response_valid(features, length)) return -EPROTO;
    if ((features.features & WKI_VFS_FEATURE_DIRECTORY_REFERENCE) == 0) return -ENOTSUP;
    uint64_t const TOKEN = ker::vfs::ExecContentState::allocate_identity();
    if (TOKEN == 0) return -EOVERFLOW;
    uint16_t const SLOT = reserve_exec_cancellation(state, TOKEN, true, &IDENTITY);
    if (SLOT == VFS_EXEC_CANCEL_SLOT_NONE) return -EAGAIN;
    preparation.own_cancellation(SLOT, TOKEN);
    VfsDirectoryRequest const header{
        .directory_fd = source != nullptr ? source->remote_fd : -1, .token = TOKEN, .path_length = static_cast<uint16_t>(PATH_LENGTH)};
    std::memcpy(request.data(), &header, sizeof(header));
    std::array<uint8_t, sizeof(VfsDirectoryResponse) + 511> reply{};
    status = vfs_proxy_send_and_wait(state, OP_VFS_DIRECTORY_REFERENCE, request.data(), sizeof(header) + RECORD.size() + PATH_LENGTH,
                                     reply.data(), reply.size(), &length, VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &IDENTITY);
    VfsDirectoryResponse result{};
    if (status == 0) {
        if (length < sizeof(result))
            status = -EPROTO;
        else {
            std::memcpy(&result, reply.data(), sizeof(result));
            if (!wki_vfs_directory_response_valid(result, TOKEN, reply.data() + sizeof(result), length))
                status = -EPROTO;
            else if (static_cast<size_t>(result.path_length) + 1 > capacity)
                status = -ERANGE;
        }
    }
    if (status != 0) {
        // The owner may have published despite an error/lost reply. Never use
        // reply descriptor bytes for cleanup, and never replay PIN.
        preparation.cancel();
        return status;
    }
    context->proxy = state;
    context->remote_fd = result.fd;
    context->exec_cancel_slot = SLOT;
    context->directory_pin = true;
    file->fops = &g_remote_vfs_fops;
    file->fs_type = ker::vfs::FSType::REMOTE;
    if (directory != nullptr) file->mount_policy = directory->mount_policy;
    file->fd = -1;
    file->refcount = 1;
    file->is_directory = true;
    file->open_flags = ker::vfs::O_DIRECTORY | ker::vfs::O_NO_CACHE | ker::vfs::FILE_ACCESS_SEARCH_ONLY;
    std::memcpy(name, reply.data() + sizeof(result), result.path_length);
    name[result.path_length] = '\0';
    *output = preparation.publish();
    return 0;
}

auto wki_remote_vfs_directory_path(ker::vfs::File* directory, const ker::mod::security::Snapshot& credentials, char* name, size_t capacity)
    -> int {
    if (directory == nullptr || directory->fops != &g_remote_vfs_fops || directory->private_data == nullptr) return -EBADF;
    if (name == nullptr || capacity == 0) return -EINVAL;
    auto const REQUEST_CREDENTIALS = credentials;
    auto* context = static_cast<RemoteFileContext*>(directory->private_data);
    if (!context->directory_pin || context->exec_cancel_slot >= g_exec_cancellations.size()) return -EBADF;
    auto const& owner = g_exec_cancellations.at(context->exec_cancel_slot);
    // Caller holds the File, so this OWNED record cannot be retired concurrently.
    if (!owner.directory || owner.proxy != context->proxy || owner.phase != ExecCancellationPhase::OWNED) return -EBADF;
    VfsDirectoryRequest const header{.operation = VfsDirectoryOperation::PATH, .directory_fd = context->remote_fd, .token = owner.token};
    auto const RECORD = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
    std::array<uint8_t, sizeof(header) + WKI_VFS_DIRECTORY_CALLER_SIZE> request{};
    std::memcpy(request.data(), &header, sizeof(header));
    std::memcpy(request.data() + sizeof(header), RECORD.data(), RECORD.size());
    std::array<uint8_t, sizeof(VfsDirectoryResponse) + 511> reply{};
    uint16_t length = 0;
    int const STATUS = vfs_proxy_send_and_wait(context->proxy, OP_VFS_DIRECTORY_REFERENCE, request.data(), request.size(), reply.data(),
                                               reply.size(), &length, VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &owner.identity);
    if (STATUS != 0) return STATUS;
    if (length < sizeof(VfsDirectoryResponse)) return -EPROTO;
    VfsDirectoryResponse result{};
    std::memcpy(&result, reply.data(), sizeof(result));
    if (!wki_vfs_directory_response_valid(result, owner.token, reply.data() + sizeof(result), length) || result.fd != context->remote_fd)
        return -EPROTO;
    if (static_cast<size_t>(result.path_length) + 1 > capacity) return -ERANGE;
    std::memcpy(name, reply.data() + sizeof(result), result.path_length);
    name[result.path_length] = '\0';
    return 0;
}

auto wki_remote_vfs_clone_exec_file(ker::vfs::File* source, ker::vfs::File** output) -> int {
    if (source == nullptr || output == nullptr || source->fops != &g_remote_vfs_fops) return -EINVAL;
    auto* original = static_cast<RemoteFileContext*>(source->private_data);
    if (original == nullptr || original->proxy == nullptr) return -EBADF;
    ProxyVfsState* const STATE = original->proxy;
    // Source retains this proxy identity. Read current advert support so even
    // flag-only updates apply to existing mounts, before any PIN ownership.
    int const SUPPORT =
        wki_resource_observation_require_flags(STATE->owner_node, ResourceType::VFS, STATE->resource_id, STATE->resource_generation,
                                               STATE->owner_incarnation, RESOURCE_FLAG_VFS_EXEC_VIEW);
    if (SUPPORT != 0) return SUPPORT;
    PinPreparation preparation(PinPurpose::EXECUTABLE);
    if (preparation.status() != 0) return preparation.status();
    auto* file = preparation.file();
    auto* context = preparation.context();
    file->exec_read_view.reset(new (std::nothrow) ker::vfs::ExecReadView{});
    if (file->exec_read_view == nullptr) return -ENOMEM;
    uint64_t const TOKEN = ker::vfs::ExecContentState::allocate_identity();
    if (TOKEN == 0) return -EOVERFLOW;
    int const RETAINED = preparation.retain_proxy(STATE);
    if (RETAINED != 0) return RETAINED;
    uint16_t const CANCEL_SLOT = reserve_exec_cancellation(STATE, TOKEN);
    if (CANCEL_SLOT == VFS_EXEC_CANCEL_SLOT_NONE) return -EAGAIN;
    preparation.own_cancellation(CANCEL_SLOT, TOKEN);
    auto const& cancellation = g_exec_cancellations.at(CANCEL_SLOT);
    VfsExecViewRequest request{.source_fd = original->remote_fd, .token = TOKEN};
    VfsExecViewResponse response{};
    uint16_t length = 0;
    int result = normalize_proxy_status_for_errno(
        vfs_proxy_send_and_wait(STATE, OP_VFS_EXEC_VIEW, reinterpret_cast<const uint8_t*>(&request), sizeof(request), &response,
                                sizeof(response), &length, VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &cancellation.identity));
    if (result == 0 && (length != sizeof(response) || !wki_vfs_exec_view_response_valid(response, TOKEN))) result = -EPROTO;
    if (result == 0) {
        std::memcpy(&file->exec_read_view->metadata, response.metadata.data(), response.metadata.size());
        if ((file->exec_read_view->metadata.st_mode & ker::vfs::S_IFMT) != ker::vfs::S_IFREG) result = -EPROTO;
    }
    if (result != 0) {
        // Even a lost/malformed reply may have published a new owner FD. Its
        // token is known before PIN, so cleanup never trusts reply descriptor
        // bytes. Reliable channel order puts cancellation after the request.
        // Keep preparation linked during this wait too. Preserve the existing
        // cancellation-error precedence if the caller returns normally.
        int const CANCELED = cancel_remote_exec_view(CANCEL_SLOT);
        return CANCELED != 0 ? CANCELED : result;
    }
    file->exec_read_view->content_id = TOKEN;
    context->proxy = STATE;
    context->remote_fd = response.fd;
    context->exec_cancel_slot = CANCEL_SLOT;
    file->fops = &g_remote_vfs_fops;
    file->fs_type = ker::vfs::FSType::REMOTE;
    file->fd = -1;
    file->refcount = 1;
    file->open_flags = ker::vfs::O_NO_CACHE;
    *output = preparation.publish();
    return 0;
}

auto wki_remote_vfs_open_path(const char* fs_relative_path, int flags, int mode, void* mount_private_data,
                              const ker::mod::security::Snapshot* credentials, int* error, bool regular_only) -> ker::vfs::File* {
    if (error != nullptr) *error = -EIO;
    constexpr int OPEN_ACCMODE = 0x3;
    constexpr int OPEN_RDONLY = 0x0;
    constexpr int OPEN_WRONLY = 0x1;
    constexpr int OPEN_RDWR = 0x2;

    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || fs_relative_path == nullptr) {
        return nullptr;
    }
    if (credentials == nullptr || (regular_only && (flags != 0 || mode != 0))) {
        if (error != nullptr) *error = -EINVAL;
        return nullptr;
    }
    uint16_t const REQUIRED_CAPABILITY = regular_only ? WKI_CAP_VFS_READ_IDENTITY : WKI_CAP_VFS_OPEN_IDENTITY;
    if (!wki_peer_capability_negotiated(anchor->owner_node, REQUIRED_CAPABILITY)) {
        if (error != nullptr) *error = -ENOTSUP;
        return nullptr;
    }
    size_t const PATH_LEN = std::strlen(fs_relative_path);
    int const PATH_STATUS = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(fs_relative_path), PATH_LEN,
                                                    wki_peer_capability_negotiated(anchor->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
    if (PATH_STATUS != 0) {
        if (error != nullptr) *error = PATH_STATUS;
        return nullptr;
    }
    uint16_t const OPEN_OPERATION = regular_only ? OP_VFS_OPEN_READ_AS : OP_VFS_OPEN_AS;
    auto const REQUEST_RECORD = ker::mod::security::credential_record::encode(*credentials);
    size_t const RECORD_SIZE = REQUEST_RECORD.size();
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
    PinPreparation preparation(state, PinPurpose::ORDINARY_OPEN);
    if (preparation.status() != 0) return nullptr;
    // The tracked open reference now retains the lane across RPC/fatal waits.
    lane_ref_guard.reset();
    int const LIFETIME = preparation.prepare_ordinary_open();
    if (LIFETIME != 0) {
        if (error != nullptr) *error = LIFETIME;
        return nullptr;
    }
    auto* file = preparation.file();
    auto* ctx = preparation.context();

    bool const WANT_OPEN_PREFETCH = !regular_only && remote_vfs_open_prefetch_capable(state, flags);
    uint32_t const OPEN_PREFETCH_LEN = WANT_OPEN_PREFETCH ? std::min<uint32_t>(state->rdma_bulk_size, VFS_RDMA_BULK_SIZE) : 0;
    bool send_open_prefetch = WANT_OPEN_PREFETCH && OPEN_PREFETCH_LEN > 0;
    OptionalSharedIoSlotGuard const OPEN_PREFETCH_GUARD(state, send_open_prefetch, wki_now_us());
    if (send_open_prefetch && !OPEN_PREFETCH_GUARD.acquired()) {
        send_open_prefetch = false;
    }

    // Build request: {flags:u32, mode:u32, path_len:u16, path[N], optional prefetch_rkey:u32, prefetch_len:u32}
    size_t const REQ_FIXED_LEN = RECORD_SIZE + OPEN_REQ_BASE_LEN + (send_open_prefetch ? OPEN_PREFETCH_REQ_LEN : 0);
    if (REQ_FIXED_LEN > WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload) ||
        PATH_LEN > WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload) - REQ_FIXED_LEN) {
        return nullptr;
    }
    auto path_len = static_cast<uint16_t>(PATH_LEN);
    size_t const REQ_DATA_LEN = REQ_FIXED_LEN + PATH_LEN;
    std::array<uint8_t, OPEN_REQ_INLINE_CAPACITY> inline_req_data;  // NOLINT(cppcoreguidelines-pro-type-member-init)
    uint8_t* req_data = inline_req_data.data();
    if (REQ_DATA_LEN > inline_req_data.size()) {
        req_data = preparation.allocate_request(REQ_DATA_LEN);
        if (req_data == nullptr) return nullptr;
    }

    if (RECORD_SIZE != 0) std::memcpy(req_data, REQUEST_RECORD.data(), RECORD_SIZE);
    auto* open_data = req_data + RECORD_SIZE;
    auto u_flags = static_cast<uint32_t>(flags);
    auto u_mode = static_cast<uint32_t>(mode);
    memcpy(open_data, &u_flags, sizeof(uint32_t));
    memcpy(open_data + 4, &u_mode, sizeof(uint32_t));
    memcpy(open_data + 8, &path_len, sizeof(uint16_t));
    if (path_len > 0) {
        memcpy(open_data + 10, fs_relative_path, path_len);
    }
    if (send_open_prefetch) {
        size_t const PREFETCH_OFF = OPEN_REQ_BASE_LEN + static_cast<size_t>(path_len);
        uint32_t const PREFETCH_RKEY = state->rdma_bulk_rkey;
        memcpy(open_data + PREFETCH_OFF, &PREFETCH_RKEY, sizeof(uint32_t));
        memcpy(open_data + PREFETCH_OFF + sizeof(uint32_t), &OPEN_PREFETCH_LEN, sizeof(uint32_t));
    }

    OpenRespPayload open_resp = {};
    uint16_t open_resp_len = 0;
    RoceTaggedReceive tagged_receive{};
    RoceTaggedReceive* tagged_receive_ptr = nullptr;
    if (send_open_prefetch) {
        tagged_receive.rkey = state->rdma_bulk_rkey;
        tagged_receive_ptr = &tagged_receive;
    }

    int const STATUS =
        vfs_proxy_send_and_wait(state, OPEN_OPERATION, req_data, REQ_DATA_LEN, &open_resp, sizeof(open_resp), &open_resp_len,
                                VFS_PROXY_OP_TIMEOUT_US, tagged_receive_ptr, nullptr, 0, false, nullptr, preparation.ordinary_slot());

    if (STATUS != 0 || open_resp.fd < 0) {
        if (error != nullptr) *error = STATUS != 0 ? normalize_proxy_status_for_errno(STATUS) : -EIO;
        return nullptr;
    }

    uint32_t valid_prefetched_bytes = 0;
    if (send_open_prefetch && open_resp_len >= OPEN_RESP_WITH_PREFETCH_LEN && open_resp.prefetched_bytes > 0 &&
        open_resp.prefetched_bytes <= OPEN_PREFETCH_LEN) {
        // The server transmits every prefetch frame before enqueueing this
        // response on the same NIC. Once the response is consumed, waiting
        // cannot recover a dropped raw-RoCE frame; fail the optional prefetch
        // immediately and let ordinary reliable VFS reads service the file.
        if (wki_roce_region_tagged_write_complete(state->rdma_bulk_rkey, tagged_receive.cookie, open_resp.prefetched_bytes)) {
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

    // Required private storage was allocated before sending OPEN.
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
        remote_vfs_store_open_stat(ctx, open_resp);
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

    if (error != nullptr) *error = 0;
    return preparation.publish();
}

namespace {

auto remote_vfs_stat_on_proxy(ProxyVfsState* state, const char* fs_relative_path, ker::vfs::Stat* statbuf,
                              const ker::mod::security::Snapshot& credentials, bool follow_final_symlink, bool require_directory) -> int {
    if (state == nullptr || !state->active || fs_relative_path == nullptr || statbuf == nullptr) return -EINVAL;
    if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_STAT_IDENTITY)) return -ENOTSUP;
    auto const REQUEST_CREDENTIALS = credentials;
    constexpr size_t RECORD_SIZE = ker::mod::security::credential_record::SIZE;
    constexpr size_t PATH_OFFSET = RECORD_SIZE + 4;
    std::array<uint8_t, PATH_OFFSET + 511> request{};
    size_t const PATH_LEN = std::strlen(fs_relative_path);
    if (PATH_LEN > request.size() - PATH_OFFSET) return -ENAMETOOLONG;
    int const PATH_STATUS = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(fs_relative_path), PATH_LEN,
                                                    wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
    if (PATH_STATUS != 0) return PATH_STATUS;
    auto const RECORD = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
    std::memcpy(request.data(), RECORD.data(), RECORD.size());
    request[RECORD_SIZE] = (follow_final_symlink ? WKI_VFS_STAT_FOLLOW : 0) | (require_directory ? WKI_VFS_STAT_REQUIRE_DIRECTORY : 0);
    auto const WIRE_PATH_LEN = static_cast<uint16_t>(PATH_LEN);
    std::memcpy(request.data() + RECORD_SIZE + 2, &WIRE_PATH_LEN, sizeof(WIRE_PATH_LEN));
    std::memcpy(request.data() + PATH_OFFSET, fs_relative_path, PATH_LEN);
    // RX may run with another address space; only publish this kernel candidate after an exact successful reply.
    ker::vfs::Stat candidate{};
    uint16_t response_length = 0;
    int const STATUS = normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(
        state, OP_VFS_STAT_AS, request.data(), PATH_OFFSET + PATH_LEN, &candidate, sizeof(candidate), &response_length));
    if (STATUS != 0) return STATUS;
    if (response_length != sizeof(candidate)) return -EPROTO;
    *statbuf = candidate;
    return 0;
}

auto remote_vfs_xattr_result_matches(const VfsXattrResultPayload& result, const VfsXattrPhaseHeader& expected) -> bool {
    return wki_vfs_xattr_header_valid(result.header) && result.header.session_id == expected.session_id &&
           result.header.operation_id == expected.operation_id && result.header.phase == expected.phase &&
           result.header.operation == expected.operation && result.header.target == expected.target &&
           result.data_len <= WKI_VFS_XATTR_DATA_MAX && result.transferred <= result.data_len;
}

auto remote_vfs_xattr_on_proxy(ProxyVfsState* state, VfsXattrOperation operation, VfsXattrTarget target, int32_t remote_fd,
                               const char* fs_relative_path, const char* name, void* value, size_t size, int flags,
                               bool follow_final_symlink, const ker::mod::security::Snapshot& credentials, bool require_directory)
    -> ssize_t {
    auto const REQUEST_CREDENTIALS = credentials;
    if (state == nullptr) {
        return -EINVAL;
    }
    if (!state->active) {
        return -ENOTCONN;
    }
    if (size > WKI_VFS_XATTR_DATA_MAX) {
        return -E2BIG;
    }
    // This check must precede operation-id allocation and every possible send.
    if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_XATTR)) {
        return -EOPNOTSUPP;
    }

    size_t const PATH_LEN = target == VfsXattrTarget::PATH && fs_relative_path != nullptr ? strnlen(fs_relative_path, 512) : 0;
    size_t const NAME_LEN = name != nullptr ? strnlen(name, WKI_VFS_XATTR_NAME_MAX + 1U) : 0;
    bool const NEEDS_NAME = operation != VfsXattrOperation::LIST;
    if ((target == VfsXattrTarget::PATH && (fs_relative_path == nullptr || PATH_LEN >= 512)) ||
        (target == VfsXattrTarget::FD && remote_fd < 0) || (NEEDS_NAME && (NAME_LEN == 0 || NAME_LEN > WKI_VFS_XATTR_NAME_MAX)) ||
        (!NEEDS_NAME && NAME_LEN != 0) || ((operation == VfsXattrOperation::SET) && size != 0 && value == nullptr) ||
        ((operation == VfsXattrOperation::GET || operation == VfsXattrOperation::LIST) && size != 0 && value == nullptr)) {
        return -EINVAL;
    }

    if (target == VfsXattrTarget::PATH) {
        if (remote_fd < -1 || (remote_fd >= 0 && PATH_LEN == 0)) return -EINVAL;
        int const PATH_STATUS = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(fs_relative_path), PATH_LEN,
                                                        wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
        if (PATH_STATUS != 0) return PATH_STATUS;
    }
    constexpr size_t PREFIX = sizeof(VfsXattrBeginPayload) + WKI_VFS_XATTR_CALLER_SIZE;
    std::array<uint8_t, PREFIX + 512 + WKI_VFS_XATTR_NAME_MAX> begin_buf{};
    auto const RECORD = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
    std::memcpy(begin_buf.data() + sizeof(VfsXattrBeginPayload), RECORD.data(), RECORD.size());
    if (PATH_LEN != 0) std::memcpy(begin_buf.data() + PREFIX, fs_relative_path, PATH_LEN);
    if (NAME_LEN != 0) std::memcpy(begin_buf.data() + PREFIX + PATH_LEN, name, NAME_LEN);

    state->xattr_transfer_lock.lock();
    s_vfs_lock.lock();
    WkiChannelIdentity const IDENTITY = proxy_channel_identity_locked(state);
    s_vfs_lock.unlock();
    auto invoke = [&](uint16_t operation_id, const uint8_t* data, size_t length, void* output, uint16_t capacity, uint16_t* received,
                      const uint8_t* tail = nullptr, size_t tail_length = 0) {
        return vfs_proxy_send_and_wait(state, operation_id, data, length, output, capacity, received, VFS_PROXY_OP_TIMEOUT_US, nullptr,
                                       tail, tail_length, false, &IDENTITY);
    };
    VfsFeaturesRequest const QUERY{.version = WKI_VFS_FEATURES_VERSION, .reserved = 0};
    VfsFeaturesResponse features{};
    uint16_t feature_length = 0;
    int support =
        invoke(OP_VFS_FEATURES, reinterpret_cast<const uint8_t*>(&QUERY), sizeof(QUERY), &features, sizeof(features), &feature_length);
    if (support == 0 && !wki_vfs_features_response_valid(features, feature_length)) support = -EPROTO;
    if (support == 0 && (features.features & WKI_VFS_FEATURE_XATTR_IDENTITY) == 0) support = -EOPNOTSUPP;
    if (support != 0) {
        state->xattr_transfer_lock.unlock();
        return support == -ENOSYS ? -EOPNOTSUPP : normalize_proxy_status_for_errno(support);
    }

    if (state->xattr_session_id == 0) {
        uint32_t session_low = g_vfs_xattr_session_counter.fetch_add(1, std::memory_order_relaxed);
        if (session_low == 0) {
            session_low = g_vfs_xattr_session_counter.fetch_add(1, std::memory_order_relaxed);
        }
        state->xattr_session_id = (static_cast<uint64_t>(g_wki.local_boot_epoch) << 32U) | session_low;
        if (state->xattr_session_id == 0) {
            state->xattr_session_id = session_low;
        }
    }
    uint64_t operation_id = ++state->xattr_next_operation_id;
    if (operation_id == 0) {
        operation_id = ++state->xattr_next_operation_id;
    }

    VfsXattrBeginPayload begin = {};
    begin.header = {.session_id = state->xattr_session_id,
                    .operation_id = operation_id,
                    .version = WKI_VFS_XATTR_VERSION,
                    .phase = VfsXattrPhase::BEGIN,
                    .operation = operation,
                    .target = target};
    begin.remote_fd = remote_fd;
    begin.data_len = static_cast<uint32_t>(size);
    begin.flags = static_cast<uint32_t>(flags);
    begin.path_len = static_cast<uint16_t>(PATH_LEN);
    begin.name_len = static_cast<uint16_t>(NAME_LEN);
    begin.follow_final_symlink = follow_final_symlink ? 1 : 0;
    begin.require_directory = require_directory ? 1 : 0;

    std::memcpy(begin_buf.data(), &begin, sizeof(begin));

    VfsXattrResultPayload result = {};
    uint16_t response_len = 0;
    int status = invoke(OP_VFS_XATTR, begin_buf.data(), PREFIX + PATH_LEN + NAME_LEN, &result, sizeof(result), &response_len);
    if (status != 0 || response_len != sizeof(result) || !remote_vfs_xattr_result_matches(result, begin.header)) {
        if (operation == VfsXattrOperation::SET || operation == VfsXattrOperation::REMOVE) {
            invalidate_readlink_cache_group(state);
        }
        state->xattr_transfer_lock.unlock();
        return status != 0 ? normalize_proxy_status_for_errno(status) : -EPROTO;
    }

    uint32_t const TOTAL = result.data_len;
    bool const READ_OPERATION = operation == VfsXattrOperation::GET || operation == VfsXattrOperation::LIST;
    if (READ_OPERATION && size != 0 && TOTAL > size) {
        status = -ERANGE;
    }
    uint32_t const TRANSFER_TOTAL = vfs_xattr_transfer_total(READ_OPERATION, size, TOTAL);
    uint32_t offset = 0;
    while (status == 0 && offset < TRANSFER_TOTAL) {
        auto const CHUNK = static_cast<uint16_t>(std::min<size_t>(WKI_VFS_XATTR_MAX_DATA_CHUNK, TRANSFER_TOTAL - offset));
        VfsXattrDataPayload data = {};
        data.header = begin.header;
        data.header.phase = VfsXattrPhase::DATA;
        data.offset = offset;
        data.chunk_len = CHUNK;
        std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> response{};
        uint16_t chunk_response_len = 0;
        if (READ_OPERATION) {
            status = invoke(OP_VFS_XATTR, reinterpret_cast<const uint8_t*>(&data), sizeof(data), response.data(),
                            static_cast<uint16_t>(response.size()), &chunk_response_len);
            if (status == 0) {
                if (chunk_response_len != sizeof(VfsXattrResultPayload) + CHUNK) {
                    status = -EPROTO;
                } else {
                    auto const* chunk_result = reinterpret_cast<const VfsXattrResultPayload*>(response.data());
                    if (!remote_vfs_xattr_result_matches(*chunk_result, data.header) || chunk_result->transferred != CHUNK) {
                        status = -EPROTO;
                    } else {
                        memcpy(static_cast<uint8_t*>(value) + offset, response.data() + sizeof(VfsXattrResultPayload), CHUNK);
                    }
                }
            }
        } else {
            status = invoke(OP_VFS_XATTR, reinterpret_cast<const uint8_t*>(&data), sizeof(data), &result, sizeof(result), &response_len,
                            static_cast<const uint8_t*>(value) + offset, CHUNK);
            if (status == 0 && (response_len != sizeof(result) || !remote_vfs_xattr_result_matches(result, data.header) ||
                                result.transferred != offset + CHUNK)) {
                status = -EPROTO;
            }
        }
        offset += CHUNK;
    }

    VfsXattrPhaseHeader terminal = begin.header;
    terminal.phase = status == 0 ? VfsXattrPhase::COMMIT : VfsXattrPhase::ABORT;
    VfsXattrResultPayload terminal_result = {};
    uint16_t terminal_len = 0;
    int const TERMINAL_STATUS = invoke(OP_VFS_XATTR, reinterpret_cast<const uint8_t*>(&terminal), sizeof(terminal), &terminal_result,
                                       sizeof(terminal_result), &terminal_len);
    if (status == 0) {
        status = TERMINAL_STATUS;
        if (status == 0 && (terminal_len != sizeof(terminal_result) || !remote_vfs_xattr_result_matches(terminal_result, terminal))) {
            status = -EPROTO;
        }
    }

    bool const MUTATION = operation == VfsXattrOperation::SET || operation == VfsXattrOperation::REMOVE;
    if (MUTATION && status != 0) {
        // A failure after BEGIN may be post-send ambiguity. Do not manufacture
        // a new identity or replay the mutation; invalidate conservatively.
        invalidate_readlink_cache_group(state);
    }
    state->xattr_transfer_lock.unlock();
    if (status != 0) {
        return normalize_proxy_status_for_errno(status);
    }
    return READ_OPERATION ? static_cast<ssize_t>(TOTAL) : 0;
}

}  // namespace

auto wki_remote_vfs_stat(void* mount_private_data, const char* fs_relative_path, ker::vfs::Stat* statbuf,
                         const ker::mod::security::Snapshot& credentials, bool follow_final_symlink, bool require_directory) -> int {
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || fs_relative_path == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) {
        return -EINVAL;
    }
    ProxyLifecycleRefGuard lane_ref_guard(state);
    return remote_vfs_stat_on_proxy(state, fs_relative_path, statbuf, credentials, follow_final_symlink, require_directory);
}

namespace {
auto remote_vfs_path_xattr(void* mount_private_data, const ker::mod::security::Snapshot& credentials, VfsXattrOperation operation,
                           const char* path, const char* name, void* value, size_t size, int flags, bool follow_final,
                           bool require_directory, ker::vfs::File* directory) -> ssize_t {
    auto send = [&](ProxyVfsState* state, int32_t fd) -> ssize_t {
        ssize_t const STATUS = remote_vfs_xattr_on_proxy(state, operation, VfsXattrTarget::PATH, fd, path, name, value, size, flags,
                                                         follow_final, credentials, require_directory);
        bool const MUTATION = operation == VfsXattrOperation::SET || operation == VfsXattrOperation::REMOVE;
        if (state != nullptr && MUTATION && STATUS < 0 && STATUS != -EOPNOTSUPP && STATUS != -EINVAL && STATUS != -ENAMETOOLONG)
            ker::vfs::vfs_cache_notify_path_changed(state->local_mount_path.data(), nullptr);
        return STATUS;
    };
    if (directory != nullptr) {
        if (directory->fops != wki_remote_vfs_get_fops() || directory->private_data == nullptr) return -EBADF;
        if (!directory->is_directory) return -ENOTDIR;
        auto* context = static_cast<RemoteFileContext*>(directory->private_data);
        ker::mod::sys::MutexGuard io_guard(context->io_lock);
        return send(context->proxy, context->remote_fd);
    }
    auto* state = acquire_vfs_proxy_lane(static_cast<ProxyVfsState*>(mount_private_data));
    if (state == nullptr) return -EINVAL;
    ProxyLifecycleRefGuard guard(state);
    return send(state, -1);
}

auto remote_vfs_file_xattr(const ker::mod::security::Snapshot& credentials, ker::vfs::File* file, VfsXattrOperation operation,
                           const char* name, void* value, size_t size, int flags) -> ssize_t {
    if (file == nullptr || file->fops != wki_remote_vfs_get_fops() || file->private_data == nullptr) return -EINVAL;
    auto* ctx = static_cast<RemoteFileContext*>(file->private_data);
    ker::mod::sys::MutexGuard io_guard(ctx->io_lock);
    return remote_vfs_xattr_on_proxy(ctx->proxy, operation, VfsXattrTarget::FD, ctx->remote_fd, nullptr, name, value, size, flags, true,
                                     credentials, false);
}
}  // namespace

auto wki_remote_vfs_setxattr(void* mount_private_data, const ker::mod::security::Snapshot& credentials, const char* fs_relative_path,
                             const char* name, const void* value, size_t size, int flags, bool follow_final_symlink, bool require_directory,
                             ker::vfs::File* directory) -> int {
    return static_cast<int>(remote_vfs_path_xattr(mount_private_data, credentials, VfsXattrOperation::SET, fs_relative_path, name,
                                                  const_cast<void*>(value), size, flags, follow_final_symlink, require_directory,
                                                  directory));
}
auto wki_remote_vfs_getxattr(void* mount_private_data, const ker::mod::security::Snapshot& credentials, const char* fs_relative_path,
                             const char* name, void* value, size_t size, bool follow_final_symlink, bool require_directory,
                             ker::vfs::File* directory) -> ssize_t {
    return remote_vfs_path_xattr(mount_private_data, credentials, VfsXattrOperation::GET, fs_relative_path, name, value, size, 0,
                                 follow_final_symlink, require_directory, directory);
}
auto wki_remote_vfs_listxattr(void* mount_private_data, const ker::mod::security::Snapshot& credentials, const char* fs_relative_path,
                              char* list, size_t size, bool follow_final_symlink, bool require_directory, ker::vfs::File* directory)
    -> ssize_t {
    return remote_vfs_path_xattr(mount_private_data, credentials, VfsXattrOperation::LIST, fs_relative_path, nullptr, list, size, 0,
                                 follow_final_symlink, require_directory, directory);
}
auto wki_remote_vfs_removexattr(void* mount_private_data, const ker::mod::security::Snapshot& credentials, const char* fs_relative_path,
                                const char* name, bool follow_final_symlink, bool require_directory, ker::vfs::File* directory) -> int {
    return static_cast<int>(remote_vfs_path_xattr(mount_private_data, credentials, VfsXattrOperation::REMOVE, fs_relative_path, name,
                                                  nullptr, 0, 0, follow_final_symlink, require_directory, directory));
}
auto wki_remote_vfs_fsetxattr(const ker::mod::security::Snapshot& credentials, ker::vfs::File* file, const char* name, const void* value,
                              size_t size, int flags) -> int {
    int const STATUS =
        static_cast<int>(remote_vfs_file_xattr(credentials, file, VfsXattrOperation::SET, name, const_cast<void*>(value), size, flags));
    if (STATUS < 0 && STATUS != -EOPNOTSUPP && STATUS != -EINVAL) ker::vfs::vfs_cache_notify_file_changed(file);
    return STATUS;
}
auto wki_remote_vfs_fgetxattr(const ker::mod::security::Snapshot& credentials, ker::vfs::File* file, const char* name, void* value,
                              size_t size) -> ssize_t {
    return remote_vfs_file_xattr(credentials, file, VfsXattrOperation::GET, name, value, size, 0);
}
auto wki_remote_vfs_flistxattr(const ker::mod::security::Snapshot& credentials, ker::vfs::File* file, char* list, size_t size) -> ssize_t {
    return remote_vfs_file_xattr(credentials, file, VfsXattrOperation::LIST, nullptr, list, size, 0);
}
auto wki_remote_vfs_fremovexattr(const ker::mod::security::Snapshot& credentials, ker::vfs::File* file, const char* name) -> int {
    int const STATUS = static_cast<int>(remote_vfs_file_xattr(credentials, file, VfsXattrOperation::REMOVE, name, nullptr, 0, 0));
    if (STATUS < 0 && STATUS != -EOPNOTSUPP && STATUS != -EINVAL) ker::vfs::vfs_cache_notify_file_changed(file);
    return STATUS;
}

auto wki_remote_vfs_metadata_batch(void* mount_private_data, ker::vfs::MetadataBatchOperation operation, uint32_t mode,
                                   const ker::vfs::MetadataBatchEntry* entries, size_t count, ker::vfs::MetadataBatchResult* results,
                                   bool* mutation_request_attempted, const ker::mod::security::Snapshot* credentials) -> int {
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
    if (credentials == nullptr) return -EINVAL;
    if (READ_ONLY && !wki_peer_capability_negotiated(anchor->owner_node, WKI_CAP_VFS_STAT_IDENTITY)) return -ENOTSUP;
    // One immutable record is reused for every chunk, never recaptured between items.
    ker::mod::security::credential_record::Record credential_record{};
    credential_record = ker::mod::security::credential_record::encode(*credentials);
    size_t const REQUEST_PREFIX = sizeof(VfsMetadataBatchHeader) + credential_record.size();

    bool const RENAME = operation == ker::vfs::MetadataBatchOperation::RENAME;
    size_t mutation_request_len = REQUEST_PREFIX;
    for (size_t index = 0; index < count; ++index) {
        results[index].status = -EINPROGRESS;  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        results[index].stat = {};              // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)

        auto const& entry = entries[index];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        if (entry.path == nullptr || (!RENAME && entry.second_path != nullptr) || (RENAME && entry.second_path == nullptr)) {
            return -EINVAL;
        }
        size_t const PATH_LEN = std::strlen(entry.path);
        if (PATH_LEN > VFS_METADATA_BATCH_MAX_PATH_LEN) return -ENAMETOOLONG;
        int const PATH_STATUS = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(entry.path), PATH_LEN,
                                                        wki_peer_capability_negotiated(anchor->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
        if (PATH_STATUS != 0) return PATH_STATUS;
        size_t item_size = sizeof(uint16_t) + PATH_LEN;
        if (RENAME) {
            size_t const SECOND_PATH_LEN = std::strlen(entry.second_path);
            if (SECOND_PATH_LEN > VFS_METADATA_BATCH_MAX_PATH_LEN) return -ENAMETOOLONG;
            int const SECOND_STATUS =
                mapped_wire_path_status(reinterpret_cast<const uint8_t*>(entry.second_path), SECOND_PATH_LEN,
                                        wki_peer_capability_negotiated(anchor->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
            if (SECOND_STATUS != 0) return SECOND_STATUS;
            item_size = (sizeof(uint16_t) * 2) + PATH_LEN + SECOND_PATH_LEN;
        }
        if (item_size > MAX_REQUEST_DATA - REQUEST_PREFIX) {
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

    if (!READ_ONLY && !wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_PATH_IDENTITY)) return -EOPNOTSUPP;
    if (!wki_resource_observation_is_live(state->owner_node, ResourceType::VFS, state->resource_id, state->resource_generation,
                                          state->owner_incarnation))
        return -ENOTCONN;
    s_vfs_lock.lock();
    WkiChannelIdentity const IDENTITY = proxy_channel_identity_locked(state);
    s_vfs_lock.unlock();

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
        size_t request_len = REQUEST_PREFIX;
        std::memcpy(request.data() + sizeof(VfsMetadataBatchHeader), credential_record.data(), credential_record.size());
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
            .version = READ_ONLY ? VFS_METADATA_BATCH_STAT_VERSION : VFS_METADATA_BATCH_MUTATION_VERSION,
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
            // Complete request bytes and the immutable record precede the query.
            // Both sends use one exact binding; no mutation may be replayed.
            VfsFeaturesRequest const QUERY{.version = WKI_VFS_FEATURES_VERSION, .reserved = 0};
            VfsFeaturesResponse features{};
            uint16_t feature_length = 0;
            int const PROBE = normalize_proxy_status_for_errno(
                vfs_proxy_send_and_wait(state, OP_VFS_FEATURES, reinterpret_cast<const uint8_t*>(&QUERY), sizeof(QUERY), &features,
                                        sizeof(features), &feature_length, VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &IDENTITY));
            if (PROBE != 0) return PROBE == -ENOSYS ? -EOPNOTSUPP : PROBE;
            if (!wki_vfs_features_response_valid(features, feature_length)) return -EPROTO;
            if ((features.features & WKI_VFS_FEATURE_BATCH_MUTATION_IDENTITY) == 0) return -EOPNOTSUPP;
            mutation_attempted = true;
            *mutation_request_attempted = true;
        }
        int const STATUS = vfs_proxy_send_and_wait(state, OP_VFS_METADATA_BATCH, request.data(), request_len, response.data(),
                                                   static_cast<uint16_t>(response.size()), &response_len, VFS_PROXY_OP_TIMEOUT_US, nullptr,
                                                   nullptr, 0, false, &IDENTITY);
        if (STATUS < 0) {
            return finish(STATUS == -EOPNOTSUPP ? -EPROTO : normalize_proxy_status_for_errno(STATUS));
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

        for (size_t item = 0; item < chunk_count; ++item) {
            size_t const OFFSET = sizeof(VfsMetadataBatchHeader) + item * ITEM_RESPONSE_SIZE;
            int32_t status = 0;
            std::memcpy(&status, response.data() + OFFSET, sizeof(status));
            if (status > 0) return finish(-EPROTO);
            if (READ_ONLY && status != 0) {
                for (size_t byte = sizeof(status); byte < ITEM_RESPONSE_SIZE; ++byte)
                    if (response[OFFSET + byte] != 0) return finish(-EPROTO);
            }
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

auto wki_remote_vfs_write_append(ker::vfs::File* file, const void* bytes, size_t count, size_t* offset) -> ssize_t {
    if (file == nullptr || file->private_data == nullptr || file->fops != wki_remote_vfs_get_fops()) return -EBADF;
    if (bytes == nullptr && count != 0) return -EFAULT;
    auto& context = *static_cast<RemoteFileContext*>(file->private_data);
    ker::mod::sys::MutexGuard io_guard(context.io_lock);
    consume_remote_file_cache_invalidation(&context);
    if (context.secure_write_error != 0) return context.secure_write_error;
    WkiChannelIdentity identity{};
    int const FEATURES = query_remote_owner_features(context, identity);
    if (FEATURES != 0) return FEATURES == -ENOTSUP || FEATURES == -ENOSYS ? -EOPNOTSUPP : FEATURES;
    if ((context.owner_features & WKI_VFS_FEATURE_ATOMIC_APPEND) == 0) return -EOPNOTSUPP;
    int const FLUSH = flush_write_behind(&context);
    if (FLUSH != 0) return FLUSH;
    invalidate_remote_file_open_caches(&context);
    uint64_t const OPERATION = next_owner_mutation_id();
    if (OPERATION == 0) return -EOVERFLOW;
    uint32_t const REQUESTED = static_cast<uint32_t>(std::min(count, static_cast<size_t>(WKI_VFS_APPEND_MAX)));
    VfsAppendRequest const request{.owner = {.fd = context.remote_fd,
                                             .open_token = context.ordinary_open_token,
                                             .binding_id = context.ordinary_open_binding_id,
                                             .owner_boot_epoch = context.ordinary_owner_boot_epoch,
                                             .query_id = OPERATION},
                                   .data_len = REQUESTED};
    VfsAppendResponse response{};
    uint16_t length = 0;
    int const STATUS = normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(
        context.proxy, OP_VFS_APPEND, reinterpret_cast<const uint8_t*>(&request), sizeof(request), &response, sizeof(response), &length,
        VFS_PROXY_OP_TIMEOUT_US, nullptr, static_cast<const uint8_t*>(bytes), REQUESTED, false, &identity));
    if (STATUS != 0 || !wki_vfs_append_response_valid(response, OPERATION, REQUESTED, length)) {
        // A missing/malformed completion cannot authorize another append ID.
        context.secure_write_error = STATUS < 0 ? STATUS : -EPROTO;
        return context.secure_write_error;
    }
    if (response.status < 0) return response.status;
    if (offset != nullptr) *offset = static_cast<size_t>(response.offset);
    return response.bytes;
}

auto wki_remote_vfs_fstat(ker::vfs::File* file, ker::vfs::Stat* statbuf) -> int {
    if (file == nullptr || file->private_data == nullptr || statbuf == nullptr) return -EINVAL;
    auto* ctx = static_cast<RemoteFileContext*>(file->private_data);
    ker::mod::sys::MutexGuard io_guard(ctx->io_lock);
    if (ctx->proxy == nullptr || !ctx->proxy->active) return -EIO;
    if (!wki_peer_capability_negotiated(ctx->proxy->owner_node, WKI_CAP_VFS_FD_METADATA)) return -ENOTSUP;
    consume_remote_file_cache_invalidation(ctx);

    // Metadata includes writes already accepted by this description. Do not
    // change its position or reauthorize/reopen a pathname after a rename.
    int const FLUSH = flush_write_behind(ctx);
    if (FLUSH != 0) return normalize_proxy_status_for_errno(FLUSH);
    ker::vfs::Stat candidate{};
    uint16_t response_length = 0;
    int const RESULT = normalize_proxy_status_for_errno(
        vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_FSTAT, reinterpret_cast<const uint8_t*>(&ctx->remote_fd), sizeof(ctx->remote_fd),
                                &candidate, sizeof(candidate), &response_length));
    if (RESULT != 0) return RESULT;
    if (response_length != sizeof(candidate)) return -EPROTO;
    candidate.st_dev = file->mount_dev_id;
    *statbuf = candidate;
    return 0;
}

auto wki_remote_vfs_mkdir(void* mount_private_data, const char* fs_relative_path, int mode, const ker::mod::security::Snapshot& credentials,
                          ker::vfs::File* directory) -> int {
    if (fs_relative_path == nullptr) return -EINVAL;
    auto const REQUEST_CREDENTIALS = credentials;
    auto send = [&](ProxyVfsState* state, int32_t directory_fd) -> int {
        if (state == nullptr || !state->active) return -ENOTCONN;
        if (!wki_resource_observation_is_live(state->owner_node, ResourceType::VFS, state->resource_id, state->resource_generation,
                                              state->owner_incarnation))
            return -ENOTCONN;
        if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_PATH_IDENTITY)) return -ENOTSUP;
        s_vfs_lock.lock();
        WkiChannelIdentity const IDENTITY = proxy_channel_identity_locked(state);
        s_vfs_lock.unlock();
        auto invoke = [&](uint16_t operation, const uint8_t* data, size_t length, void* output, uint16_t capacity, uint16_t* received) {
            return normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(state, operation, data, length, output, capacity, received,
                                                                            VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false,
                                                                            &IDENTITY));
        };
        size_t const LENGTH = std::strlen(fs_relative_path);
        if (LENGTH == 0 && directory_fd > 0) return -ENOENT;
        if (LENGTH > 511) return -ENAMETOOLONG;
        int const VALID = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(fs_relative_path), LENGTH,
                                                  wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
        if (VALID != 0) return VALID;
        constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
        constexpr size_t OFFSET = RECORD + sizeof(VfsPathIdentityHeader) + sizeof(VfsPathMkdirPrefix);
        std::array<uint8_t, OFFSET + 511> request{};
        auto const RECORD_BYTES = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
        VfsFeaturesRequest const QUERY{.version = WKI_VFS_FEATURES_VERSION, .reserved = 0};
        VfsFeaturesResponse features{};
        uint16_t feature_length = 0;
        int const PROBE =
            invoke(OP_VFS_FEATURES, reinterpret_cast<const uint8_t*>(&QUERY), sizeof(QUERY), &features, sizeof(features), &feature_length);
        if (PROBE != 0) return PROBE == -ENOSYS ? -ENOTSUP : PROBE;
        if (!wki_vfs_features_response_valid(features, feature_length)) return -EPROTO;
        if ((features.features & WKI_VFS_FEATURE_MKDIR_IDENTITY) == 0) return -ENOTSUP;
        VfsPathIdentityHeader const HEADER{.operation = VfsPathOperation::MKDIR,
                                           .flags = WKI_VFS_PATH_REQUIRE_DIRECTORY,
                                           .data_len = static_cast<uint16_t>(sizeof(VfsPathMkdirPrefix) + LENGTH),
                                           .reserved = 0};
        VfsPathMkdirPrefix const PREFIX{.mode = static_cast<uint32_t>(mode), .directory_fd = directory_fd};
        std::memcpy(request.data(), RECORD_BYTES.data(), RECORD_BYTES.size());
        std::memcpy(request.data() + RECORD, &HEADER, sizeof(HEADER));
        std::memcpy(request.data() + RECORD + sizeof(HEADER), &PREFIX, sizeof(PREFIX));
        std::memcpy(request.data() + OFFSET, fs_relative_path, LENGTH);
        VfsPathIdentityHeader reply{};
        uint16_t response_length = 0;
        // A mutation is submitted once on this exact lane. Never retry it as
        // a read or fall back to the callerless opcode after an uncertain reply.
        int const STATUS = invoke(OP_VFS_PATH_AS, request.data(), OFFSET + LENGTH, &reply, sizeof(reply), &response_length);
        if (STATUS != 0) return STATUS;
        if (!wki_vfs_path_response_valid(0, response_length, reply) || reply.operation != VfsPathOperation::MKDIR) return -EPROTO;
        invalidate_readlink_cache_group(state);
        return 0;
    };
    if (directory != nullptr) {
        if (directory->fops != &g_remote_vfs_fops || directory->private_data == nullptr) return -EBADF;
        if (!directory->is_directory) return -ENOTDIR;
        auto* context = static_cast<RemoteFileContext*>(directory->private_data);
        ker::mod::sys::MutexGuard io_guard(context->io_lock);
        return send(context->proxy, context->remote_fd);
    }
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr) return -EINVAL;
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) return -ENOTCONN;
    ProxyLifecycleRefGuard lane_ref_guard(state);
    return send(state, -1);
}

auto wki_remote_vfs_chown(void* mount_private_data, const char* fs_relative_path, uint32_t owner, uint32_t group, bool follow_final_symlink,
                          const ker::mod::security::Snapshot& credentials, bool require_directory, ker::vfs::File* directory) -> int {
    if (fs_relative_path == nullptr) return -EINVAL;
    auto const REQUEST_CREDENTIALS = credentials;
    auto send = [&](ProxyVfsState* state, int32_t directory_fd) -> int {
        if (state == nullptr || !state->active) return -ENOTCONN;
        if (!wki_resource_observation_is_live(state->owner_node, ResourceType::VFS, state->resource_id, state->resource_generation,
                                              state->owner_incarnation))
            return -ENOTCONN;
        if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_PATH_IDENTITY)) return -ENOTSUP;
        s_vfs_lock.lock();
        WkiChannelIdentity const IDENTITY = proxy_channel_identity_locked(state);
        s_vfs_lock.unlock();
        auto invoke = [&](uint16_t operation, const uint8_t* data, size_t length, void* output, uint16_t capacity, uint16_t* received) {
            return normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(state, operation, data, length, output, capacity, received,
                                                                            VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false,
                                                                            &IDENTITY));
        };
        size_t const LENGTH = std::strlen(fs_relative_path);
        if (LENGTH == 0 && directory_fd > 0) return -ENOENT;
        if (LENGTH > 511) return -ENAMETOOLONG;
        int const VALID = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(fs_relative_path), LENGTH,
                                                  wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
        if (VALID != 0) return VALID;
        constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
        constexpr size_t OFFSET = RECORD + sizeof(VfsPathIdentityHeader) + sizeof(VfsPathChownPrefix);
        std::array<uint8_t, OFFSET + 511> request{};
        auto const RECORD_BYTES = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
        VfsFeaturesRequest const QUERY{.version = WKI_VFS_FEATURES_VERSION, .reserved = 0};
        VfsFeaturesResponse features{};
        uint16_t feature_length = 0;
        int const PROBE =
            invoke(OP_VFS_FEATURES, reinterpret_cast<const uint8_t*>(&QUERY), sizeof(QUERY), &features, sizeof(features), &feature_length);
        if (PROBE != 0) return PROBE == -ENOSYS ? -ENOTSUP : PROBE;
        if (!wki_vfs_features_response_valid(features, feature_length)) return -EPROTO;
        if ((features.features & WKI_VFS_FEATURE_CHOWN_IDENTITY) == 0) return -ENOTSUP;
        VfsPathIdentityHeader const HEADER{.operation = VfsPathOperation::CHOWN,
                                           .flags = static_cast<uint16_t>((follow_final_symlink ? WKI_VFS_PATH_FOLLOW_FINAL : 0) |
                                                                          (require_directory ? WKI_VFS_PATH_REQUIRE_DIRECTORY : 0)),
                                           .data_len = static_cast<uint16_t>(sizeof(VfsPathChownPrefix) + LENGTH),
                                           .reserved = 0};
        VfsPathChownPrefix const PREFIX{.owner = owner, .group = group, .directory_fd = directory_fd};
        std::memcpy(request.data(), RECORD_BYTES.data(), RECORD_BYTES.size());
        std::memcpy(request.data() + RECORD, &HEADER, sizeof(HEADER));
        std::memcpy(request.data() + RECORD + sizeof(HEADER), &PREFIX, sizeof(PREFIX));
        std::memcpy(request.data() + OFFSET, fs_relative_path, LENGTH);
        VfsPathIdentityHeader reply{};
        uint16_t response_length = 0;
        // A mutation is submitted once on this exact lane. Never retry it as
        // a read or fall back to the callerless opcode after an uncertain reply.
        int const STATUS = invoke(OP_VFS_PATH_AS, request.data(), OFFSET + LENGTH, &reply, sizeof(reply), &response_length);
        if (STATUS != 0) return STATUS;
        if (!wki_vfs_path_response_valid(0, response_length, reply) || reply.operation != VfsPathOperation::CHOWN) return -EPROTO;
        invalidate_readlink_cache_group(state);
        return 0;
    };
    if (directory != nullptr) {
        if (directory->fops != &g_remote_vfs_fops || directory->private_data == nullptr) return -EBADF;
        if (!directory->is_directory) return -ENOTDIR;
        auto* context = static_cast<RemoteFileContext*>(directory->private_data);
        ker::mod::sys::MutexGuard io_guard(context->io_lock);
        return send(context->proxy, context->remote_fd);
    }
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr) return -EINVAL;
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) return -ENOTCONN;
    ProxyLifecycleRefGuard lane_ref_guard(state);
    return send(state, -1);
}

auto wki_remote_vfs_chmod(void* mount_private_data, const char* fs_relative_path, int mode, bool follow_final_symlink,
                          const ker::mod::security::Snapshot& credentials, bool require_directory, ker::vfs::File* directory) -> int {
    if (fs_relative_path == nullptr) return -EINVAL;
    auto const REQUEST_CREDENTIALS = credentials;
    auto send = [&](ProxyVfsState* state, int32_t directory_fd) -> int {
        if (state == nullptr || !state->active) return -ENOTCONN;
        s_vfs_lock.lock();
        WkiChannelIdentity const IDENTITY = proxy_channel_identity_locked(state);
        s_vfs_lock.unlock();
        int const SUPPORT =
            wki_resource_observation_require_flags(state->owner_node, ResourceType::VFS, state->resource_id, state->resource_generation,
                                                   state->owner_incarnation, RESOURCE_FLAG_VFS_CHMOD_IDENTITY);
        if (SUPPORT != 0) return SUPPORT;
        if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_PATH_IDENTITY)) return -ENOTSUP;
        size_t const LENGTH = std::strlen(fs_relative_path);
        if (LENGTH == 0 && directory_fd > 0) return -ENOENT;
        if (LENGTH > 511) return -ENAMETOOLONG;
        int const VALID = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(fs_relative_path), LENGTH,
                                                  wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
        if (VALID != 0) return VALID;
        constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
        constexpr size_t OFFSET = RECORD + sizeof(VfsPathIdentityHeader) + sizeof(VfsPathChmodPrefix);
        std::array<uint8_t, OFFSET + 511> request{};
        auto const RECORD_BYTES = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
        VfsPathIdentityHeader const HEADER{.operation = VfsPathOperation::CHMOD,
                                           .flags = static_cast<uint16_t>((follow_final_symlink ? WKI_VFS_PATH_FOLLOW_FINAL : 0) |
                                                                          (require_directory ? WKI_VFS_PATH_REQUIRE_DIRECTORY : 0)),
                                           .data_len = static_cast<uint16_t>(sizeof(VfsPathChmodPrefix) + LENGTH),
                                           .reserved = 0};
        VfsPathChmodPrefix const PREFIX{.mode = static_cast<uint32_t>(mode), .directory_fd = directory_fd};
        std::memcpy(request.data(), RECORD_BYTES.data(), RECORD_BYTES.size());
        std::memcpy(request.data() + RECORD, &HEADER, sizeof(HEADER));
        std::memcpy(request.data() + RECORD + sizeof(HEADER), &PREFIX, sizeof(PREFIX));
        std::memcpy(request.data() + OFFSET, fs_relative_path, LENGTH);
        VfsPathIdentityHeader reply{};
        uint16_t response_length = 0;
        // A mutation is submitted once on this exact lane. Never retry it as
        // a read or fall back to the callerless opcode after an uncertain reply.
        int const STATUS = normalize_proxy_status_for_errno(
            vfs_proxy_send_and_wait(state, OP_VFS_PATH_AS, request.data(), OFFSET + LENGTH, &reply, sizeof(reply), &response_length,
                                    VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &IDENTITY));
        if (STATUS != 0) return STATUS;
        return wki_vfs_path_response_valid(0, response_length, reply) && reply.operation == VfsPathOperation::CHMOD ? 0 : -EPROTO;
    };
    if (directory != nullptr) {
        if (directory->fops != &g_remote_vfs_fops || directory->private_data == nullptr) return -EBADF;
        if (!directory->is_directory) return -ENOTDIR;
        auto* context = static_cast<RemoteFileContext*>(directory->private_data);
        ker::mod::sys::MutexGuard io_guard(context->io_lock);
        return send(context->proxy, context->remote_fd);
    }
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr) return -EINVAL;
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) return -ENOTCONN;
    ProxyLifecycleRefGuard lane_ref_guard(state);
    return send(state, -1);
}

auto wki_remote_vfs_utimens(void* mount_private_data, const char* fs_relative_path, const ker::vfs::Timespec* times,
                            bool follow_final_symlink, const ker::mod::security::Snapshot& credentials, bool require_directory,
                            ker::vfs::File* directory) -> int {
    if (fs_relative_path == nullptr) return -EINVAL;
    auto const REQUEST_CREDENTIALS = credentials;
    std::array<ker::vfs::Timespec, 2> const TIMES =
        times != nullptr ? std::array<ker::vfs::Timespec, 2>{times[0], times[1]}
                         : std::array<ker::vfs::Timespec, 2>{{{0, ker::vfs::TimestampUpdate::NOW}, {0, ker::vfs::TimestampUpdate::NOW}}};
    ker::vfs::TimestampUpdate validated;
    if (int const VALID = ker::vfs::TimestampUpdate::prepare(TIMES.data(), {0, 0}, validated); VALID != 0) return VALID;
    auto send = [&](ProxyVfsState* state, int32_t directory_fd) -> int {
        if (state == nullptr || !state->active) return -ENOTCONN;
        s_vfs_lock.lock();
        WkiChannelIdentity const IDENTITY = proxy_channel_identity_locked(state);
        s_vfs_lock.unlock();
        int const SUPPORT =
            wki_resource_observation_require_flags(state->owner_node, ResourceType::VFS, state->resource_id, state->resource_generation,
                                                   state->owner_incarnation, RESOURCE_FLAG_VFS_UTIMENS_IDENTITY);
        if (SUPPORT != 0) return SUPPORT;
        if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_PATH_IDENTITY)) return -ENOTSUP;
        size_t const LENGTH = std::strlen(fs_relative_path);
        if (LENGTH == 0 && directory_fd > 0) return -ENOENT;
        if (LENGTH > 511) return -ENAMETOOLONG;
        int const VALID = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(fs_relative_path), LENGTH,
                                                  wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
        if (VALID != 0) return VALID;
        constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
        constexpr size_t OFFSET = RECORD + sizeof(VfsPathIdentityHeader) + sizeof(VfsPathUtimensPrefix);
        std::array<uint8_t, OFFSET + 511> request{};
        auto const RECORD_BYTES = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
        VfsPathIdentityHeader const HEADER{.operation = VfsPathOperation::UTIMENS,
                                           .flags = static_cast<uint16_t>((follow_final_symlink ? WKI_VFS_PATH_FOLLOW_FINAL : 0) |
                                                                          (require_directory ? WKI_VFS_PATH_REQUIRE_DIRECTORY : 0)),
                                           .data_len = static_cast<uint16_t>(sizeof(VfsPathUtimensPrefix) + LENGTH),
                                           .reserved = 0};
        VfsPathUtimensPrefix const PREFIX{.directory_fd = directory_fd,
                                          .reserved = 0,
                                          .atime_sec = TIMES[0].tv_sec,
                                          .atime_nsec = TIMES[0].tv_nsec,
                                          .mtime_sec = TIMES[1].tv_sec,
                                          .mtime_nsec = TIMES[1].tv_nsec};
        std::memcpy(request.data(), RECORD_BYTES.data(), RECORD_BYTES.size());
        std::memcpy(request.data() + RECORD, &HEADER, sizeof(HEADER));
        std::memcpy(request.data() + RECORD + sizeof(HEADER), &PREFIX, sizeof(PREFIX));
        std::memcpy(request.data() + OFFSET, fs_relative_path, LENGTH);
        VfsPathIdentityHeader reply{};
        uint16_t response_length = 0;
        // A mutation is submitted once on this exact lane. Never retry it as
        // a read or fall back to the callerless opcode after an uncertain reply.
        int const STATUS = normalize_proxy_status_for_errno(
            vfs_proxy_send_and_wait(state, OP_VFS_PATH_AS, request.data(), OFFSET + LENGTH, &reply, sizeof(reply), &response_length,
                                    VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &IDENTITY));
        if (STATUS != 0) return STATUS;
        return wki_vfs_path_response_valid(0, response_length, reply) && reply.operation == VfsPathOperation::UTIMENS ? 0 : -EPROTO;
    };
    if (directory != nullptr) {
        if (directory->fops != &g_remote_vfs_fops || directory->private_data == nullptr) return -EBADF;
        if (!directory->is_directory) return -ENOTDIR;
        auto* context = static_cast<RemoteFileContext*>(directory->private_data);
        ker::mod::sys::MutexGuard io_guard(context->io_lock);
        return send(context->proxy, context->remote_fd);
    }
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr) return -EINVAL;
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) return -ENOTCONN;
    ProxyLifecycleRefGuard lane_ref_guard(state);
    return send(state, -1);
}

auto wki_remote_vfs_symlink(void* mount_private_data, const char* target, const char* fs_relative_path,
                            const ker::mod::security::Snapshot& credentials, bool require_directory, ker::vfs::File* directory) -> int {
    if (target == nullptr || fs_relative_path == nullptr) return -EINVAL;
    auto const REQUEST_CREDENTIALS = credentials;
    auto send = [&](ProxyVfsState* state, int32_t directory_fd) -> int {
        if (state == nullptr || !state->active) return -ENOTCONN;
        if (!wki_resource_observation_is_live(state->owner_node, ResourceType::VFS, state->resource_id, state->resource_generation,
                                              state->owner_incarnation))
            return -ENOTCONN;
        if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_PATH_IDENTITY)) return -ENOTSUP;
        s_vfs_lock.lock();
        WkiChannelIdentity const IDENTITY = proxy_channel_identity_locked(state);
        s_vfs_lock.unlock();
        auto invoke = [&](uint16_t operation, const uint8_t* data, size_t length, void* output, uint16_t capacity, uint16_t* received) {
            return normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(state, operation, data, length, output, capacity, received,
                                                                            VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false,
                                                                            &IDENTITY));
        };
        size_t const LENGTH = std::strlen(fs_relative_path);
        size_t const TARGET_LENGTH = std::strlen(target);
        if (LENGTH == 0 && directory_fd > 0) return -ENOENT;
        if (LENGTH > 511 || TARGET_LENGTH > 511) return -ENAMETOOLONG;
        int const VALID = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(fs_relative_path), LENGTH,
                                                  wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
        if (VALID != 0) return VALID;
        constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
        constexpr size_t OFFSET = RECORD + sizeof(VfsPathIdentityHeader) + sizeof(VfsPathSymlinkPrefix);
        std::array<uint8_t, OFFSET + 2 * 511> request{};
        auto const RECORD_BYTES = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
        VfsPathIdentityHeader const HEADER{.operation = VfsPathOperation::SYMLINK,
                                           .flags = static_cast<uint16_t>(require_directory ? WKI_VFS_PATH_REQUIRE_DIRECTORY : 0),
                                           .data_len = static_cast<uint16_t>(sizeof(VfsPathSymlinkPrefix) + TARGET_LENGTH + LENGTH),
                                           .reserved = 0};
        VfsPathSymlinkPrefix const PREFIX{
            .directory_fd = directory_fd, .target_length = static_cast<uint16_t>(TARGET_LENGTH), .reserved = 0};
        std::memcpy(request.data(), RECORD_BYTES.data(), RECORD_BYTES.size());
        std::memcpy(request.data() + RECORD, &HEADER, sizeof(HEADER));
        std::memcpy(request.data() + RECORD + sizeof(HEADER), &PREFIX, sizeof(PREFIX));
        std::memcpy(request.data() + OFFSET, target, TARGET_LENGTH);
        std::memcpy(request.data() + OFFSET + TARGET_LENGTH, fs_relative_path, LENGTH);
        VfsFeaturesRequest const QUERY{.version = WKI_VFS_FEATURES_VERSION, .reserved = 0};
        VfsFeaturesResponse features{};
        uint16_t feature_length = 0;
        int const PROBE =
            invoke(OP_VFS_FEATURES, reinterpret_cast<const uint8_t*>(&QUERY), sizeof(QUERY), &features, sizeof(features), &feature_length);
        if (PROBE != 0) return PROBE == -ENOSYS ? -ENOTSUP : PROBE;
        if (!wki_vfs_features_response_valid(features, feature_length)) return -EPROTO;
        if ((features.features & WKI_VFS_FEATURE_SYMLINK_IDENTITY) == 0) return -ENOTSUP;

        VfsPathIdentityHeader reply{};
        uint16_t response_length = 0;
        // A mutation is submitted once on this exact lane. Never retry it as
        // a read or fall back to the callerless opcode after an uncertain reply.
        int const STATUS = invoke(OP_VFS_PATH_AS, request.data(), OFFSET + TARGET_LENGTH + LENGTH, &reply, sizeof(reply), &response_length);
        if (STATUS != 0) return STATUS;
        if (!wki_vfs_path_response_valid(0, response_length, reply) || reply.operation != VfsPathOperation::SYMLINK) return -EPROTO;
        invalidate_readlink_cache_group(state);
        return 0;
    };
    if (directory != nullptr) {
        if (directory->fops != &g_remote_vfs_fops || directory->private_data == nullptr) return -EBADF;
        if (!directory->is_directory) return -ENOTDIR;
        auto* context = static_cast<RemoteFileContext*>(directory->private_data);
        ker::mod::sys::MutexGuard io_guard(context->io_lock);
        return send(context->proxy, context->remote_fd);
    }
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr) return -EINVAL;
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) return -ENOTCONN;
    ProxyLifecycleRefGuard lane_ref_guard(state);
    return send(state, -1);
}

namespace {
auto remote_vfs_remove_path(void* mount_private_data, const char* fs_relative_path, bool remove_directory,
                            const ker::mod::security::Snapshot& credentials, bool require_directory, ker::vfs::File* directory) -> int {
    if (fs_relative_path == nullptr) return -EINVAL;
    auto const REQUEST_CREDENTIALS = credentials;
    auto send = [&](ProxyVfsState* state, int32_t directory_fd) -> int {
        if (state == nullptr || !state->active) return -ENOTCONN;
        if (!wki_resource_observation_is_live(state->owner_node, ResourceType::VFS, state->resource_id, state->resource_generation,
                                              state->owner_incarnation))
            return -ENOTCONN;
        if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_PATH_IDENTITY)) return -ENOTSUP;
        s_vfs_lock.lock();
        WkiChannelIdentity const IDENTITY = proxy_channel_identity_locked(state);
        s_vfs_lock.unlock();
        auto invoke = [&](uint16_t operation, const uint8_t* data, size_t length, void* output, uint16_t capacity, uint16_t* received) {
            return normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(state, operation, data, length, output, capacity, received,
                                                                            VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false,
                                                                            &IDENTITY));
        };
        size_t const LENGTH = std::strlen(fs_relative_path);
        if (LENGTH == 0 && directory_fd > 0) return -ENOENT;
        if (LENGTH > 511) return -ENAMETOOLONG;
        int const VALID = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(fs_relative_path), LENGTH,
                                                  wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
        if (VALID != 0) return VALID;
        constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
        constexpr size_t OFFSET = RECORD + sizeof(VfsPathIdentityHeader) + sizeof(VfsPathRemovePrefix);
        std::array<uint8_t, OFFSET + 511> request{};
        auto const RECORD_BYTES = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
        VfsPathIdentityHeader const HEADER{.operation = VfsPathOperation::REMOVE,
                                           .flags = static_cast<uint16_t>(require_directory ? WKI_VFS_PATH_REQUIRE_DIRECTORY : 0),
                                           .data_len = static_cast<uint16_t>(sizeof(VfsPathRemovePrefix) + LENGTH),
                                           .reserved = 0};
        VfsPathRemovePrefix const PREFIX{
            .directory_fd = directory_fd, .flags = static_cast<uint16_t>(remove_directory ? WKI_VFS_REMOVE_DIRECTORY : 0), .reserved = 0};
        std::memcpy(request.data(), RECORD_BYTES.data(), RECORD_BYTES.size());
        std::memcpy(request.data() + RECORD, &HEADER, sizeof(HEADER));
        std::memcpy(request.data() + RECORD + sizeof(HEADER), &PREFIX, sizeof(PREFIX));
        std::memcpy(request.data() + OFFSET, fs_relative_path, LENGTH);
        VfsFeaturesRequest const QUERY{.version = WKI_VFS_FEATURES_VERSION, .reserved = 0};
        VfsFeaturesResponse features{};
        uint16_t feature_length = 0;
        int const PROBE =
            invoke(OP_VFS_FEATURES, reinterpret_cast<const uint8_t*>(&QUERY), sizeof(QUERY), &features, sizeof(features), &feature_length);
        if (PROBE != 0) return PROBE == -ENOSYS ? -ENOTSUP : PROBE;
        if (!wki_vfs_features_response_valid(features, feature_length)) return -EPROTO;
        if ((features.features & WKI_VFS_FEATURE_REMOVE_IDENTITY) == 0) return -ENOTSUP;

        VfsPathIdentityHeader reply{};
        uint16_t response_length = 0;
        // A mutation is submitted once on this exact lane. Never retry it as
        // a read or fall back to the callerless opcode after an uncertain reply.
        int const STATUS = invoke(OP_VFS_PATH_AS, request.data(), OFFSET + LENGTH, &reply, sizeof(reply), &response_length);
        if (STATUS != 0) return STATUS;
        if (!wki_vfs_path_response_valid(0, response_length, reply) || reply.operation != VfsPathOperation::REMOVE) return -EPROTO;
        invalidate_readlink_cache_group(state);
        return 0;
    };
    if (directory != nullptr) {
        if (directory->fops != &g_remote_vfs_fops || directory->private_data == nullptr) return -EBADF;
        if (!directory->is_directory) return -ENOTDIR;
        auto* context = static_cast<RemoteFileContext*>(directory->private_data);
        ker::mod::sys::MutexGuard io_guard(context->io_lock);
        return send(context->proxy, context->remote_fd);
    }
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr) return -EINVAL;
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) return -ENOTCONN;
    ProxyLifecycleRefGuard lane_ref_guard(state);
    return send(state, -1);
}

}  // namespace

auto wki_remote_vfs_unlink(void* mount_private_data, const char* fs_relative_path, const ker::mod::security::Snapshot& credentials,
                           bool require_directory, ker::vfs::File* directory) -> int {
    return remote_vfs_remove_path(mount_private_data, fs_relative_path, false, credentials, require_directory, directory);
}

auto wki_remote_vfs_rmdir(void* mount_private_data, const char* fs_relative_path, const ker::mod::security::Snapshot& credentials,
                          ker::vfs::File* directory) -> int {
    return remote_vfs_remove_path(mount_private_data, fs_relative_path, true, credentials, true, directory);
}

// Consumer side: rename a file/directory on the remote server
auto wki_remote_vfs_rename(void* mount_private_data, const char* old_fs_path, const char* new_fs_path,
                           const ker::mod::security::Snapshot& credentials, bool source_requires_directory,
                           bool destination_requires_directory, ker::vfs::File* source_directory, ker::vfs::File* destination_directory)
    -> int {
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || old_fs_path == nullptr || new_fs_path == nullptr) return -EINVAL;
    auto const REQUEST_CREDENTIALS = credentials;
    size_t const OLD_LENGTH = std::strlen(old_fs_path), NEW_LENGTH = std::strlen(new_fs_path);
    if (OLD_LENGTH > 511 || NEW_LENGTH > 511) return -ENAMETOOLONG;
    if ((source_directory != nullptr && OLD_LENGTH == 0) || (destination_directory != nullptr && NEW_LENGTH == 0)) return -ENOENT;
    constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
    constexpr size_t OFFSET = RECORD + sizeof(VfsPathIdentityHeader) + sizeof(VfsPathRenamePrefix);
    std::array<uint8_t, OFFSET + 2 * 511> request{};
    auto const RECORD_BYTES = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
    VfsPathIdentityHeader const HEADER{.operation = VfsPathOperation::RENAME,
                                       .flags = 0,
                                       .data_len = static_cast<uint16_t>(sizeof(VfsPathRenamePrefix) + OLD_LENGTH + NEW_LENGTH),
                                       .reserved = 0};
    VfsPathRenamePrefix prefix{.source = {.fd = -1, .channel_id = 0, .reserved = 0, .generation = 0},
                               .destination = {.fd = -1, .channel_id = 0, .reserved = 0, .generation = 0},
                               .source_length = static_cast<uint16_t>(OLD_LENGTH),
                               .flags = static_cast<uint16_t>((source_requires_directory ? WKI_VFS_RENAME_SOURCE_DIRECTORY : 0) |
                                                              (destination_requires_directory ? WKI_VFS_RENAME_DESTINATION_DIRECTORY : 0))};
    std::memcpy(request.data(), RECORD_BYTES.data(), RECORD_BYTES.size());
    std::memcpy(request.data() + RECORD, &HEADER, sizeof(HEADER));
    std::memcpy(request.data() + OFFSET, old_fs_path, OLD_LENGTH);
    std::memcpy(request.data() + OFFSET + OLD_LENGTH, new_fs_path, NEW_LENGTH);
    RemoteFileContext* source = nullptr;
    RemoteFileContext* destination = nullptr;
    auto context = [](ker::vfs::File* file, RemoteFileContext*& out) -> int {
        if (file == nullptr) return 0;
        if (file->fops != &g_remote_vfs_fops || file->private_data == nullptr) return -EBADF;
        if (!file->is_directory) return -ENOTDIR;
        out = static_cast<RemoteFileContext*>(file->private_data);
        return 0;
    };
    int result = context(source_directory, source);
    if (result == 0) result = context(destination_directory, destination);
    if (result != 0) return result;
    auto* first = source;
    auto* second = destination;
    if (reinterpret_cast<uintptr_t>(first) > reinterpret_cast<uintptr_t>(second)) std::swap(first, second);
    std::optional<ker::mod::sys::MutexGuard> first_guard, second_guard;
    if (first != nullptr) first_guard.emplace(first->io_lock);
    if (second != nullptr && second != first) second_guard.emplace(second->io_lock);
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) return -ENOTCONN;
    ProxyLifecycleRefGuard lane_ref_guard(state);
    if (!state->active || !wki_resource_observation_is_live(state->owner_node, ResourceType::VFS, state->resource_id,
                                                            state->resource_generation, state->owner_incarnation))
        return -ENOTCONN;
    if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_PATH_IDENTITY)) return -ENOTSUP;
    bool const COMPONENTS = wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_COMPONENT_PATHS);
    result = mapped_wire_path_status(request.data() + OFFSET, OLD_LENGTH, COMPONENTS);
    if (result == 0) result = mapped_wire_path_status(request.data() + OFFSET + OLD_LENGTH, NEW_LENGTH, COMPONENTS);
    if (result != 0) return result;
    WkiChannelIdentity source_identity{}, destination_identity{};
    s_vfs_lock.lock();
    WkiChannelIdentity const IDENTITY = proxy_channel_identity_locked(state);
    auto capture = [&](RemoteFileContext* directory, WkiChannelIdentity& identity) {
        if (directory == nullptr) return true;
        auto* proxy = directory->proxy;
        if (proxy == nullptr || !proxy->active || directory->remote_fd <= 0 || proxy->mount_group_id != state->mount_group_id ||
            proxy->owner_node != state->owner_node || proxy->resource_id != state->resource_id ||
            proxy->resource_generation != state->resource_generation ||
            !wki_resource_incarnation_equal(proxy->owner_incarnation, state->owner_incarnation))
            return false;
        identity = proxy_channel_identity_locked(proxy);
        return identity.channel != nullptr && identity.generation != 0;
    };
    bool const DIRECTORIES_VALID = capture(source, source_identity) && capture(destination, destination_identity);
    s_vfs_lock.unlock();
    if (!DIRECTORIES_VALID) return -EXDEV;
    auto invoke = [&](ProxyVfsState* proxy, const WkiChannelIdentity& identity, uint16_t operation, const uint8_t* data, size_t length,
                      void* output, uint16_t capacity, uint16_t* received) {
        return normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(proxy, operation, data, length, output, capacity, received,
                                                                        VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &identity));
    };
    VfsFeaturesRequest const QUERY{.version = WKI_VFS_FEATURES_VERSION, .reserved = 0};
    VfsFeaturesResponse features{};
    uint16_t feature_length = 0;
    result = invoke(state, IDENTITY, OP_VFS_FEATURES, reinterpret_cast<const uint8_t*>(&QUERY), sizeof(QUERY), &features, sizeof(features),
                    &feature_length);
    if (result != 0) return result == -ENOSYS ? -ENOTSUP : result;
    if (!wki_vfs_features_response_valid(features, feature_length)) return -EPROTO;
    if ((features.features & WKI_VFS_FEATURE_RENAME_IDENTITY) == 0) return -ENOTSUP;
    auto binding = [&](RemoteFileContext* directory, const WkiChannelIdentity& identity, VfsRenameDirectoryRef& reference) -> int {
        if (directory == nullptr) return 0;
        std::array<uint8_t, RECORD + sizeof(VfsPathIdentityHeader) + sizeof(int32_t)> query{};
        VfsPathIdentityHeader const BINDING_HEADER{
            .operation = VfsPathOperation::DIRECTORY_BINDING, .flags = 0, .data_len = sizeof(int32_t), .reserved = 0};
        std::memcpy(query.data(), RECORD_BYTES.data(), RECORD);
        std::memcpy(query.data() + RECORD, &BINDING_HEADER, sizeof(BINDING_HEADER));
        std::memcpy(query.data() + RECORD + sizeof(BINDING_HEADER), &directory->remote_fd, sizeof(int32_t));
        std::array<uint8_t, sizeof(VfsPathIdentityHeader) + sizeof(VfsRenameDirectoryRef)> response{};
        uint16_t length = 0;
        int const STATUS =
            invoke(directory->proxy, identity, OP_VFS_PATH_AS, query.data(), query.size(), response.data(), response.size(), &length);
        if (STATUS != 0) return STATUS;
        VfsPathIdentityHeader reply{};
        std::memcpy(&reply, response.data(), sizeof(reply));
        if (!wki_vfs_path_response_valid(0, length, reply) || reply.operation != VfsPathOperation::DIRECTORY_BINDING) return -EPROTO;
        std::memcpy(&reference, response.data() + sizeof(reply), sizeof(reference));
        return wki_vfs_rename_directory_valid(reference) && reference.fd == directory->remote_fd &&
                       reference.channel_id == identity.channel_id
                   ? 0
                   : -EPROTO;
    };
    result = binding(source, source_identity, prefix.source);
    if (result == 0) result = binding(destination, destination_identity, prefix.destination);
    if (result != 0) return result;
    std::memcpy(request.data() + RECORD + sizeof(HEADER), &prefix, sizeof(prefix));
    VfsPathIdentityHeader reply{};
    uint16_t length = 0;
    // Exactly one namespace mutation. Directory queries never replay rename.
    result = invoke(state, IDENTITY, OP_VFS_PATH_AS, request.data(), OFFSET + OLD_LENGTH + NEW_LENGTH, &reply, sizeof(reply), &length);
    if (result != 0) return result;
    if (!wki_vfs_path_response_valid(0, length, reply) || reply.operation != VfsPathOperation::RENAME) return -EPROTO;
    invalidate_readlink_cache_group(state);
    return 0;
}

namespace {
auto remote_file_metadata_request(ker::vfs::File* file, const ker::mod::security::Snapshot& credentials, VfsPathOperation operation,
                                  uint32_t mode, uint32_t owner, uint32_t group, const ker::vfs::Timespec* times) -> int {
    if (file == nullptr || file->fs_type != ker::vfs::FSType::REMOTE || file->fops != &g_remote_vfs_fops || file->private_data == nullptr)
        return -EBADF;
    auto const REQUEST_CREDENTIALS = credentials;
    std::array<ker::vfs::Timespec, 2> const TIMES =
        times != nullptr ? std::array<ker::vfs::Timespec, 2>{times[0], times[1]}
                         : std::array<ker::vfs::Timespec, 2>{{{0, ker::vfs::TimestampUpdate::NOW}, {0, ker::vfs::TimestampUpdate::NOW}}};
    ker::vfs::TimestampUpdate validated;
    if (operation == VfsPathOperation::FUTIMENS) {
        if (int const RET = ker::vfs::TimestampUpdate::prepare(TIMES.data(), {0, 0}, validated); RET != 0) return RET;
    }
    // The retained File pins this client's immutable mount policy. The owner
    // may export a writable filesystem, so its authorization cannot enforce
    // this restriction. Reject before locking or submitting any request.
    // Both-OMIT still needs the owner's descriptor/backend validation.
    bool const MUTATION = operation != VfsPathOperation::FUTIMENS || validated.changed();
    if (MUTATION && ker::vfs::file_mount_read_only(*file)) return -EROFS;
    auto* context = static_cast<RemoteFileContext*>(file->private_data);
    ker::mod::sys::MutexGuard io_guard(context->io_lock);
    auto* state = context->proxy;
    if (state == nullptr || !state->active) return -ENOTCONN;
    if (context->remote_fd <= 0) return -EBADF;
    if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_PATH_IDENTITY)) return -ENOTSUP;
    constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
    constexpr size_t OFFSET = RECORD + sizeof(VfsPathIdentityHeader);
    std::array<uint8_t, OFFSET + sizeof(VfsFileUtimensRequest)> request{};
    uint16_t body_length = 0;
    if (operation == VfsPathOperation::FCHMOD) {
        VfsFileChmodRequest const BODY{.fd = context->remote_fd, .mode = mode};
        std::memcpy(request.data() + OFFSET, &BODY, sizeof(BODY));
        body_length = sizeof(BODY);
    } else if (operation == VfsPathOperation::FCHOWN) {
        VfsFileChownRequest const BODY{.fd = context->remote_fd, .owner = owner, .group = group};
        std::memcpy(request.data() + OFFSET, &BODY, sizeof(BODY));
        body_length = sizeof(BODY);
    } else if (operation == VfsPathOperation::FUTIMENS) {
        VfsFileUtimensRequest const BODY{.fd = context->remote_fd,
                                         .reserved = 0,
                                         .atime_sec = TIMES[0].tv_sec,
                                         .atime_nsec = TIMES[0].tv_nsec,
                                         .mtime_sec = TIMES[1].tv_sec,
                                         .mtime_nsec = TIMES[1].tv_nsec};
        std::memcpy(request.data() + OFFSET, &BODY, sizeof(BODY));
        body_length = sizeof(BODY);
    } else
        return -EINVAL;
    auto const RECORD_BYTES = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
    VfsPathIdentityHeader const HEADER{.operation = operation, .flags = 0, .data_len = body_length, .reserved = 0};
    std::memcpy(request.data(), RECORD_BYTES.data(), RECORD_BYTES.size());
    std::memcpy(request.data() + RECORD, &HEADER, sizeof(HEADER));
    s_vfs_lock.lock();
    WkiChannelIdentity const IDENTITY = proxy_channel_identity_locked(state);
    s_vfs_lock.unlock();
    auto invoke = [&](uint16_t op, const uint8_t* data, uint16_t length, void* result, uint16_t size, uint16_t* received) {
        return normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(state, op, data, length, result, size, received,
                                                                        VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &IDENTITY));
    };
    VfsFeaturesRequest const QUERY{.version = WKI_VFS_FEATURES_VERSION, .reserved = 0};
    VfsFeaturesResponse features{};
    uint16_t length = 0;
    int const PROBE =
        invoke(OP_VFS_FEATURES, reinterpret_cast<const uint8_t*>(&QUERY), sizeof(QUERY), &features, sizeof(features), &length);
    if (PROBE != 0) return PROBE == -ENOSYS ? -ENOTSUP : PROBE;
    if (!wki_vfs_features_response_valid(features, length)) return -EPROTO;
    if ((features.features & WKI_VFS_FEATURE_FILE_METADATA_IDENTITY) == 0) return -ENOTSUP;
    VfsPathIdentityHeader reply{};
    uint16_t response_length = 0;
    // Submit once on the same complete binding used for feature admission.
    int const STATUS = invoke(OP_VFS_PATH_AS, request.data(), OFFSET + body_length, &reply, sizeof(reply), &response_length);
    if (STATUS != 0) return STATUS;
    if (!wki_vfs_path_response_valid(0, response_length, reply) || reply.operation != operation) return -EPROTO;
    if (operation != VfsPathOperation::FUTIMENS || validated.changed()) wki_remote_vfs_invalidate_open_file_caches(file);
    return 0;
}
}  // namespace

auto wki_remote_vfs_fchmod(ker::vfs::File* file, const ker::mod::security::Snapshot& credentials, int mode) -> int {
    return remote_file_metadata_request(file, credentials, VfsPathOperation::FCHMOD, static_cast<uint32_t>(mode), 0, 0, nullptr);
}
auto wki_remote_vfs_fchown(ker::vfs::File* file, const ker::mod::security::Snapshot& credentials, uint32_t owner, uint32_t group) -> int {
    return remote_file_metadata_request(file, credentials, VfsPathOperation::FCHOWN, 0, owner, group, nullptr);
}
auto wki_remote_vfs_futimens(ker::vfs::File* file, const ker::mod::security::Snapshot& credentials, const ker::vfs::Timespec* times)
    -> int {
    return remote_file_metadata_request(file, credentials, VfsPathOperation::FUTIMENS, 0, 0, 0, times);
}

namespace {
auto remote_directory_path_request(ker::vfs::File* directory, const char* path, const ker::mod::security::Snapshot& credentials,
                                   VfsPathOperation operation, uint16_t flags, void* output, size_t capacity) -> ssize_t {
    if (directory == nullptr || directory->fs_type != ker::vfs::FSType::REMOTE || directory->private_data == nullptr || path == nullptr ||
        output == nullptr || capacity == 0)
        return -EINVAL;
    if (!directory->is_directory) return -ENOTDIR;
    auto const REQUEST_CREDENTIALS = credentials;
    auto* context = static_cast<RemoteFileContext*>(directory->private_data);
    ker::mod::sys::MutexGuard io_guard(context->io_lock);
    // Remote descriptors belong to this exact lane; never select or retry on
    // a different proxy using the same numeric descriptor.
    auto* state = context->proxy;
    if (state == nullptr || !state->active) return -EIO;
    if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_PATH_IDENTITY) ||
        ((operation == VfsPathOperation::STAT_AT || operation == VfsPathOperation::REALPATH_AT) &&
         !wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_STAT_IDENTITY)))
        return -ENOTSUP;
    constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
    constexpr size_t OFFSET = RECORD + sizeof(VfsPathIdentityHeader) + sizeof(int32_t);
    size_t const LENGTH = std::strlen(path);
    if (LENGTH == 0) return -ENOENT;
    if (LENGTH > 511) return -ENAMETOOLONG;
    int const VALID = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(path), LENGTH,
                                              wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
    if (VALID != 0) return VALID;
    std::array<uint8_t, OFFSET + 511> request{};
    auto const RECORD_BYTES = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
    VfsPathIdentityHeader const HEADER{
        .operation = operation, .flags = flags, .data_len = static_cast<uint16_t>(sizeof(int32_t) + LENGTH), .reserved = 0};
    std::memcpy(request.data(), RECORD_BYTES.data(), RECORD_BYTES.size());
    std::memcpy(request.data() + RECORD, &HEADER, sizeof(HEADER));
    std::memcpy(request.data() + OFFSET - sizeof(int32_t), &context->remote_fd, sizeof(context->remote_fd));
    std::memcpy(request.data() + OFFSET, path, LENGTH);
    s_vfs_lock.lock();
    WkiChannelIdentity const IDENTITY = proxy_channel_identity_locked(state);
    s_vfs_lock.unlock();
    auto invoke = [&](uint16_t op, const uint8_t* data, uint16_t length, void* result, uint16_t size, uint16_t* received) {
        return normalize_proxy_status_for_errno(vfs_proxy_send_and_wait(state, op, data, length, result, size, received,
                                                                        VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, &IDENTITY));
    };
    if (operation == VfsPathOperation::REALPATH_AT) {
        VfsFeaturesRequest const query{.version = WKI_VFS_FEATURES_VERSION, .reserved = 0};
        VfsFeaturesResponse features{};
        uint16_t length = 0;
        int const PROBE =
            invoke(OP_VFS_FEATURES, reinterpret_cast<const uint8_t*>(&query), sizeof(query), &features, sizeof(features), &length);
        if (PROBE != 0) return PROBE == -ENOSYS ? -ENOTSUP : PROBE;
        if (!wki_vfs_features_response_valid(features, length)) return -EPROTO;
        if ((features.features & WKI_VFS_FEATURE_REALPATH_AT) == 0) return -ENOTSUP;
    }
    std::array<uint8_t, sizeof(VfsPathIdentityHeader) + 512> response{};
    uint16_t response_length = 0;
    int const STATUS =
        invoke(OP_VFS_PATH_AS, request.data(), static_cast<uint16_t>(OFFSET + LENGTH), response.data(), response.size(), &response_length);
    if (STATUS != 0) return STATUS;
    VfsPathIdentityHeader reply{};
    if (response_length >= sizeof(reply)) std::memcpy(&reply, response.data(), sizeof(reply));
    if (!wki_vfs_path_response_valid(0, response_length, reply) || reply.operation != operation) return -EPROTO;
    if (operation == VfsPathOperation::REALPATH_AT) {
        auto const* canonical = response.data() + sizeof(reply);
        if (!wki_vfs_realpath_result_valid(canonical, reply.data_len)) return -EPROTO;
        if (static_cast<size_t>(reply.data_len) + 1 > capacity) return -ERANGE;
        std::memcpy(output, canonical, reply.data_len);
        static_cast<char*>(output)[reply.data_len] = '\0';
        return reply.data_len;
    }
    size_t const COPY = std::min(capacity, static_cast<size_t>(reply.data_len));
    std::memcpy(output, response.data() + sizeof(reply), COPY);
    return static_cast<ssize_t>(COPY);
}
}  // namespace

auto wki_remote_vfs_open_at(ker::vfs::File* directory, const char* relative_path, int flags, int mode, bool require_directory,
                            const ker::mod::security::Snapshot& credentials, ker::vfs::File& prepared) -> int {
    if (directory == nullptr || directory->fs_type != ker::vfs::FSType::REMOTE || directory->private_data == nullptr ||
        relative_path == nullptr || prepared.private_data != nullptr || prepared.fops != nullptr)
        return -EINVAL;
    if (!directory->is_directory) return -ENOTDIR;
    if (auth_policy::vfs_open_permissions(static_cast<uint32_t>(flags)) == auth_policy::INVALID) return -EINVAL;
    auto const REQUEST_CREDENTIALS = credentials;
    auto* original = static_cast<RemoteFileContext*>(directory->private_data);
    auto* state = original->proxy;
    if (state == nullptr || !state->active) return -EIO;
    if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_PATH_IDENTITY) ||
        !wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_OPEN_IDENTITY))
        return -ENOTSUP;
    size_t const LENGTH = std::strlen(relative_path);
    if (LENGTH == 0) return -ENOENT;
    if (LENGTH > 511) return -ENAMETOOLONG;
    int const VALID = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(relative_path), LENGTH,
                                              wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
    if (VALID != 0) return VALID;
    // The original File pins the exact proxy. The new File needs its own open
    // reference and context before sending a potentially mutating request.
    // The retained directory keeps its immutable proxy/FD identity alive. No
    // directory cache or I/O state is used, so no io_lock spans this RPC.
    PinPreparation preparation(state, PinPurpose::ORDINARY_OPEN, prepared);
    if (preparation.status() != 0) return preparation.status();
    int const LIFETIME = preparation.prepare_ordinary_open();
    if (LIFETIME != 0) return LIFETIME;
    auto* context = preparation.context();
    constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
    constexpr size_t BASE = RECORD + sizeof(VfsPathIdentityHeader);
    constexpr size_t OFFSET = BASE + sizeof(int32_t) + 2 * sizeof(uint32_t);
    std::array<uint8_t, OFFSET + 511> request{};
    auto const RECORD_BYTES = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
    VfsPathIdentityHeader const HEADER{.operation = VfsPathOperation::OPEN_AT,
                                       .flags = static_cast<uint16_t>(require_directory ? WKI_VFS_PATH_REQUIRE_DIRECTORY : 0),
                                       .data_len = static_cast<uint16_t>(OFFSET - BASE + LENGTH),
                                       .reserved = 0};
    uint32_t const FLAGS = static_cast<uint32_t>(flags);
    uint32_t const MODE = static_cast<uint32_t>(mode);
    std::memcpy(request.data(), RECORD_BYTES.data(), RECORD_BYTES.size());
    std::memcpy(request.data() + RECORD, &HEADER, sizeof(HEADER));
    std::memcpy(request.data() + BASE, &original->remote_fd, sizeof(int32_t));
    std::memcpy(request.data() + BASE + sizeof(int32_t), &FLAGS, sizeof(FLAGS));
    std::memcpy(request.data() + BASE + sizeof(int32_t) + sizeof(FLAGS), &MODE, sizeof(MODE));
    std::memcpy(request.data() + OFFSET, relative_path, LENGTH);
    std::array<uint8_t, sizeof(VfsPathIdentityHeader) + sizeof(VfsPathOpenResult)> response{};
    uint16_t response_length = 0;
    int const STATUS =
        vfs_proxy_send_and_wait(state, OP_VFS_PATH_AS, request.data(), OFFSET + LENGTH, response.data(), response.size(), &response_length,
                                VFS_PROXY_OP_TIMEOUT_US, nullptr, nullptr, 0, false, nullptr, preparation.ordinary_slot());
    if (STATUS != 0) return normalize_proxy_status_for_errno(STATUS);
    VfsPathIdentityHeader reply{};
    VfsPathOpenResult opened{};
    if (response_length != response.size()) return -EPROTO;
    std::memcpy(&reply, response.data(), sizeof(reply));
    if (!wki_vfs_path_response_valid(0, response_length, reply) || reply.operation != VfsPathOperation::OPEN_AT) return -EPROTO;
    std::memcpy(&opened, response.data() + sizeof(reply), sizeof(opened));
    if (!wki_vfs_path_open_result_valid(opened)) return -EPROTO;
    if (require_directory && opened.is_directory == 0) {
        // The reserved token owns cleanup; never close a descriptor from a rejected reply.
        return -EPROTO;
    }
    context->proxy = state;
    context->remote_fd = opened.fd;
    prepared.fops = &g_remote_vfs_fops;
    prepared.is_directory = opened.is_directory != 0;
    prepared.pos = 0;
    prepared.dir_fs_count = static_cast<size_t>(-1);
    static_cast<void>(preparation.publish());
    return 0;
}

auto wki_remote_vfs_stat_at(ker::vfs::File* directory, const char* relative_path, ker::vfs::Stat* output,
                            const ker::mod::security::Snapshot& credentials, bool follow_final, bool require_directory) -> int {
    if (output == nullptr) return -EINVAL;
    uint16_t const FLAGS = (follow_final ? WKI_VFS_PATH_FOLLOW_FINAL : 0) | (require_directory ? WKI_VFS_PATH_REQUIRE_DIRECTORY : 0);
    ker::vfs::Stat candidate{};
    ssize_t const RESULT = remote_directory_path_request(directory, relative_path, credentials, VfsPathOperation::STAT_AT, FLAGS,
                                                         &candidate, sizeof(candidate));
    if (RESULT < 0) return static_cast<int>(RESULT);
    if (RESULT != sizeof(candidate)) return -EPROTO;
    candidate.st_dev = directory->mount_dev_id;
    *output = candidate;
    return 0;
}

auto wki_remote_vfs_readlink_at(ker::vfs::File* directory, const char* relative_path, char* output, size_t capacity,
                                const ker::mod::security::Snapshot& credentials, bool require_directory) -> ssize_t {
    return remote_directory_path_request(directory, relative_path, credentials, VfsPathOperation::READLINK_AT,
                                         require_directory ? WKI_VFS_PATH_REQUIRE_DIRECTORY : 0, output, capacity);
}

// Consumer side: readlink on a remote path (for vfs_readlink / resolve_symlinks)
auto wki_remote_vfs_readlink_path(void* mount_private_data, const char* fs_relative_path, char* buf, size_t bufsize,
                                  const ker::mod::security::Snapshot& credentials, bool require_directory) -> ssize_t {
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || fs_relative_path == nullptr || buf == nullptr || bufsize == 0) return -EINVAL;
    auto const REQUEST_CREDENTIALS = credentials;
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) return -EINVAL;
    ProxyLifecycleRefGuard lane_ref_guard(state);
    if (!state->active) return -EINVAL;
    if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_PATH_IDENTITY)) return -ENOTSUP;

    constexpr size_t RECORD_SIZE = ker::mod::security::credential_record::SIZE;
    constexpr size_t DATA_OFFSET = RECORD_SIZE + sizeof(VfsPathIdentityHeader);
    std::array<uint8_t, DATA_OFFSET + 511> request{};
    size_t const PATH_LEN = std::strlen(fs_relative_path);
    if (PATH_LEN > request.size() - DATA_OFFSET) return -ENAMETOOLONG;
    int const PATH_STATUS = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(fs_relative_path), PATH_LEN,
                                                    wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
    if (PATH_STATUS != 0) return PATH_STATUS;
    auto const RECORD = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
    VfsPathIdentityHeader const HEADER{.operation = VfsPathOperation::READLINK,
                                       .flags = require_directory ? WKI_VFS_PATH_REQUIRE_DIRECTORY : uint16_t{0},
                                       .data_len = static_cast<uint16_t>(PATH_LEN),
                                       .reserved = 0};
    std::memcpy(request.data(), RECORD.data(), RECORD.size());
    std::memcpy(request.data() + RECORD_SIZE, &HEADER, sizeof(HEADER));
    std::memcpy(request.data() + DATA_OFFSET, fs_relative_path, PATH_LEN);
    // No credentialless positive, negative or unsupported cache may authorize
    // this request. RX writes only to this kernel-owned candidate.
    std::array<uint8_t, sizeof(VfsPathIdentityHeader) + 512> response{};
    uint16_t response_length = 0;
    int const STATUS = vfs_proxy_read_with_retry(state, OP_VFS_PATH_AS, request.data(), DATA_OFFSET + PATH_LEN, response.data(),
                                                 response.size(), &response_length, fs_relative_path);
    if (STATUS != 0) return STATUS;
    VfsPathIdentityHeader reply{};
    if (response_length >= sizeof(reply)) std::memcpy(&reply, response.data(), sizeof(reply));
    if (!wki_vfs_path_response_valid(0, response_length, reply) || reply.operation != VfsPathOperation::READLINK) return -EPROTO;
    size_t const COPY = std::min(bufsize, static_cast<size_t>(reply.data_len));
    std::memcpy(buf, response.data() + sizeof(reply), COPY);
    return static_cast<ssize_t>(COPY);
}

auto wki_remote_vfs_realpath_at(ker::vfs::File* directory, const char* relative_path, char* output, size_t capacity,
                                const ker::mod::security::Snapshot& credentials, bool require_directory) -> ssize_t {
    return remote_directory_path_request(directory, relative_path, credentials, VfsPathOperation::REALPATH_AT,
                                         require_directory ? WKI_VFS_PATH_REQUIRE_DIRECTORY : 0, output, capacity);
}

auto wki_remote_vfs_realpath_path(void* mount_private_data, const char* fs_relative_path, char* buf, size_t bufsize,
                                  const ker::mod::security::Snapshot& credentials, bool require_directory) -> ssize_t {
    auto* anchor = static_cast<ProxyVfsState*>(mount_private_data);
    if (anchor == nullptr || fs_relative_path == nullptr || buf == nullptr || bufsize == 0) return -EINVAL;
    auto const REQUEST_CREDENTIALS = credentials;
    auto* state = acquire_vfs_proxy_lane(anchor);
    if (state == nullptr) return -EINVAL;
    ProxyLifecycleRefGuard lane_ref_guard(state);
    if (!state->active) return -EINVAL;
    if (!wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_PATH_IDENTITY) ||
        !wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_STAT_IDENTITY))
        return -ENOTSUP;

    constexpr size_t RECORD_SIZE = ker::mod::security::credential_record::SIZE;
    constexpr size_t DATA_OFFSET = RECORD_SIZE + sizeof(VfsPathIdentityHeader);
    std::array<uint8_t, DATA_OFFSET + 511> request{};
    size_t const PATH_LEN = std::strlen(fs_relative_path);
    if (PATH_LEN > request.size() - DATA_OFFSET) return -ENAMETOOLONG;
    int const PATH_STATUS = mapped_wire_path_status(reinterpret_cast<const uint8_t*>(fs_relative_path), PATH_LEN,
                                                    wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_COMPONENT_PATHS));
    if (PATH_STATUS != 0) return PATH_STATUS;
    auto const RECORD = ker::mod::security::credential_record::encode(REQUEST_CREDENTIALS);
    VfsPathIdentityHeader const HEADER{.operation = VfsPathOperation::REALPATH,
                                       .flags = require_directory ? WKI_VFS_PATH_REQUIRE_DIRECTORY : uint16_t{0},
                                       .data_len = static_cast<uint16_t>(PATH_LEN),
                                       .reserved = 0};
    std::memcpy(request.data(), RECORD.data(), RECORD.size());
    std::memcpy(request.data() + RECORD_SIZE, &HEADER, sizeof(HEADER));
    std::memcpy(request.data() + DATA_OFFSET, fs_relative_path, PATH_LEN);
    // No credentialless positive, negative or unsupported cache may authorize
    // this request. RX writes only to this kernel-owned candidate.
    std::array<uint8_t, sizeof(VfsPathIdentityHeader) + 512> response{};
    uint16_t response_length = 0;
    int const STATUS = vfs_proxy_read_with_retry(state, OP_VFS_PATH_AS, request.data(), DATA_OFFSET + PATH_LEN, response.data(),
                                                 response.size(), &response_length, fs_relative_path);
    if (STATUS != 0) return STATUS;
    VfsPathIdentityHeader reply{};
    if (response_length >= sizeof(reply)) std::memcpy(&reply, response.data(), sizeof(reply));
    if (!wki_vfs_path_response_valid(0, response_length, reply) || reply.operation != VfsPathOperation::REALPATH) return -EPROTO;
    auto const* canonical = response.data() + sizeof(reply);
    if (!wki_vfs_realpath_result_valid(canonical, reply.data_len)) return -EPROTO;
    if (static_cast<size_t>(reply.data_len) + 1 > bufsize) return -ERANGE;
    std::memcpy(buf, canonical, reply.data_len);
    buf[reply.data_len] = '\0';
    return static_cast<ssize_t>(reply.data_len);
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
            }
        }
        if (group.count == 0) {
            state->lifecycle_refs++;
            group.members.at(group.count++) = state;
        }
    }
    s_vfs_lock.unlock();
    if (group.count == 0) {
        return;
    }

    auto invalidate_path = [&](const char* rel_path, uint16_t rel_len) {
        std::array<char, 512> full_path __attribute__((uninitialized));  // NOLINT(cppcoreguidelines-pro-type-member-init)
        if (relative_wire_path_has_safe_components(reinterpret_cast<const uint8_t*>(rel_path), rel_len) &&
            build_full_path(full_path.data(), full_path.size(), group.local_mount_path.data(), rel_path, rel_len) == 0) {
            ker::vfs::vfs_cache_notify_invalidate_path(full_path.data());
        } else {
            // A malformed or oversized notification cannot name one safe
            // cache entry. Invalidate the whole remote mount scope instead.
            ker::vfs::vfs_cache_notify_invalidate_path(group.local_mount_path.data());
        }
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

    size_t const EXPECTED_LEN = sizeof(DevOpRespPayload) + RESP_DATA_LEN;
    if ((resp->op_id == OP_VFS_XATTR || resp->op_id == OP_VFS_FSTAT || resp->op_id == OP_VFS_STAT_AS ||
         resp->op_id == OP_VFS_METADATA_BATCH || resp->op_id == OP_VFS_PATH_AS || resp->op_id == OP_VFS_EXEC_VIEW ||
         resp->op_id == OP_VFS_DIRECTORY_REFERENCE || resp->op_id == OP_VFS_DIRECTORY_GRANT || resp->op_id == OP_VFS_FEATURES ||
         resp->op_id == OP_VFS_OPEN_AS || resp->op_id == OP_VFS_OPEN_READ_AS || resp->op_id == OP_VFS_OPEN_LIFETIME ||
         resp->op_id == OP_VFS_VERSION || resp->op_id == OP_VFS_APPEND || resp->op_id == OP_VFS_WRITE_COMMITTED ||
         resp->op_id == OP_VFS_READ_OWNER || resp->op_id == OP_VFS_LOCK_OWNER || resp->op_id == OP_VFS_LOCK_SCOPE)
            ? EXPECTED_LEN != payload_len
            : EXPECTED_LEN > payload_len) {
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
        bool const INVALID_STAT = (resp->op_id == OP_VFS_FSTAT || resp->op_id == OP_VFS_STAT_AS) &&
                                  (!wki_vfs_fstat_response_valid(resp->status, RESP_DATA_LEN) || RESP_DATA_LEN > state->op_resp_max);
        bool const INVALID_BATCH = resp->op_id == OP_VFS_METADATA_BATCH &&
                                   (resp->status > 0 || (resp->status != 0 && RESP_DATA_LEN != 0) || RESP_DATA_LEN > state->op_resp_max);
        VfsPathIdentityHeader path_reply{};
        if (resp->op_id == OP_VFS_PATH_AS && RESP_DATA_LEN >= sizeof(path_reply)) std::memcpy(&path_reply, resp_data, sizeof(path_reply));
        bool const INVALID_PATH = resp->op_id == OP_VFS_PATH_AS && (!wki_vfs_path_response_valid(resp->status, RESP_DATA_LEN, path_reply) ||
                                                                    RESP_DATA_LEN > state->op_resp_max);
        bool const INVALID_EXEC =
            resp->op_id == OP_VFS_EXEC_VIEW && !wki_vfs_exec_view_reply_size_valid(resp->status, RESP_DATA_LEN, state->op_resp_max);
        bool const INVALID_FEATURES =
            resp->op_id == OP_VFS_FEATURES && (resp->status > 0 || RESP_DATA_LEN != (resp->status == 0 ? sizeof(VfsFeaturesResponse) : 0));
        bool const INVALID_DIRECTORY = resp->op_id == OP_VFS_DIRECTORY_REFERENCE &&
                                       !wki_vfs_directory_reply_size_valid(resp->status, RESP_DATA_LEN, state->op_resp_max);
        bool const INVALID_GRANT = resp->op_id == OP_VFS_DIRECTORY_GRANT &&
                                   !wki_vfs_directory_grant_reply_size_valid(resp->status, RESP_DATA_LEN, state->op_resp_max);
        bool const INVALID_OPEN = (resp->op_id == OP_VFS_OPEN_AS || resp->op_id == OP_VFS_OPEN_READ_AS) &&
                                  !scalar_open_response_valid(resp->status, resp_data, RESP_DATA_LEN, state->op_resp_max);
        bool const INVALID_LIFETIME = resp->op_id == OP_VFS_OPEN_LIFETIME &&
                                      !ordinary_open_response_valid(state->op_lifetime_inner, state->op_lifetime_challenge, resp->status,
                                                                    resp_data, RESP_DATA_LEN, state->op_resp_max);
        bool const INVALID_VERSION =
            resp->op_id == OP_VFS_VERSION && !wki_vfs_version_reply_size_valid(resp->status, RESP_DATA_LEN, state->op_resp_max);
        bool const INVALID_APPEND = (resp->op_id == OP_VFS_APPEND || resp->op_id == OP_VFS_WRITE_COMMITTED) &&
                                    !wki_vfs_append_reply_size_valid(resp->status, RESP_DATA_LEN, state->op_resp_max);
        bool const INVALID_OWNER_READ =
            resp->op_id == OP_VFS_READ_OWNER && !wki_vfs_owner_read_reply_size_valid(resp->status, RESP_DATA_LEN, state->op_resp_max);
        bool const INVALID_OWNER_LOCK =
            resp->op_id == OP_VFS_LOCK_OWNER && !wki_vfs_owner_lock_reply_size_valid(resp->status, RESP_DATA_LEN, state->op_resp_max);
        bool const INVALID_OWNER_SCOPE =
            resp->op_id == OP_VFS_LOCK_SCOPE && !wki_vfs_owner_scope_reply_size_valid(resp->status, RESP_DATA_LEN, state->op_resp_max);
        bool const INVALID_RESPONSE = INVALID_STAT || INVALID_BATCH || INVALID_PATH || INVALID_EXEC || INVALID_FEATURES ||
                                      INVALID_DIRECTORY || INVALID_GRANT || INVALID_OPEN || INVALID_LIFETIME || INVALID_VERSION ||
                                      INVALID_APPEND || INVALID_OWNER_READ || INVALID_OWNER_LOCK || INVALID_OWNER_SCOPE;
        if (INVALID_RESPONSE) {
            state->op_resp_len = 0;
            state->op_status = -EPROTO;
        } else if (resp->op_id == OP_VFS_XATTR && RESP_DATA_LEN > state->op_resp_max) {
            state->op_resp_len = 0;
            state->op_status = -EMSGSIZE;
        } else if (RESP_DATA_LEN > 0 && state->op_resp_buf != nullptr) {
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
        if (!INVALID_RESPONSE && (resp->op_id != OP_VFS_XATTR || RESP_DATA_LEN <= state->op_resp_max)) {
            state->op_status = resp->status;
        }
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

#ifdef WOS_SELFTEST
auto wki_remote_vfs_selftest_open_reply_admission() -> bool {
    if (g_wki.initialized || !g_vfs_proxies.empty()) return false;
    WkiChannel channel{};
    g_vfs_proxies.push_back(std::make_unique<ProxyVfsState>());
    struct Cleanup {
        ~Cleanup() { g_vfs_proxies.clear(); }
    } cleanup;
    auto& proxy = *g_vfs_proxies.back();
    proxy.active = true;
    proxy.owner_node = 42;
    proxy.assigned_channel = 77;
    proxy.assigned_channel_ref = &channel;
    proxy.assigned_channel_generation = 93;
    WkiChannelIdentity const IDENTITY{&channel, 42, 77, 93};
    WkiHeader header{};
    header.src_node = 42;
    header.channel_id = 77;
    alignas(ker::vfs::Stat) OpenRespPayload reply{};
    reply.fd = 23;
    // Exercise the real cache publication path with a deliberately unaligned
    // wire member. Diagnostic builds must not bind that member by reference.
    RemoteFileContext cache_context{};
    ker::vfs::Stat expected_stat{};
    expected_stat.st_dev = 0x123456789abcdef0;
    expected_stat.st_ino = 0xfedcba9876543210;
    expected_stat.st_mode = ker::vfs::S_IFREG | 0640;
    expected_stat.st_size = 524288;
    expected_stat.st_mtim = {123456789, 987654321};
    std::memcpy(reinterpret_cast<uint8_t*>(&reply) + offsetof(OpenRespPayload, stat), &expected_stat, sizeof(expected_stat));
    remote_vfs_store_open_stat(&cache_context, reply);
    ker::vfs::Stat cached_stat{};
    if (!remote_vfs_try_copy_cached_stat(&cache_context, &cached_stat) || cached_stat.st_dev != expected_stat.st_dev ||
        cached_stat.st_ino != expected_stat.st_ino || cached_stat.st_mode != expected_stat.st_mode ||
        cached_stat.st_size != expected_stat.st_size || cached_stat.st_mtim.tv_sec != expected_stat.st_mtim.tv_sec ||
        cached_stat.st_mtim.tv_nsec != expected_stat.st_mtim.tv_nsec)
        return false;
    auto check = [&](uint16_t operation, int16_t status, uint16_t length, int expected, bool ignored = false,
                     uint16_t capacity = sizeof(OpenRespPayload), bool trailing = false, bool stale_cookie = false,
                     bool stale_generation = false, uint16_t inner = 0, uint64_t challenge = 0, const void* body = nullptr) -> bool {
        std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(OpenRespPayload) + 2> input{};
        DevOpRespPayload response{};
        response.op_id = operation;
        response.status = status;
        response.data_len = length;
        response.reserved = stale_cookie ? 8 : 7;
        std::memcpy(input.data(), &response, sizeof(response));
        std::memcpy(input.data() + sizeof(response), &reply, sizeof(reply));
        if (body != nullptr) std::memcpy(input.data() + sizeof(response), body, length);
        std::array<uint8_t, sizeof(OpenRespPayload)> output{};
        output.fill(0xA5);
        auto const UNCHANGED = output;
        WkiWaitEntry waiter{};
        proxy.op_wait_entry = &waiter;
        proxy.op_expected_id = operation;
        proxy.op_expected_seq = 7;
        proxy.op_lifetime_inner = inner;
        proxy.op_lifetime_challenge = challenge;
        proxy.op_resp_buf = output.data();
        proxy.op_resp_max = capacity;
        proxy.op_resp_len = 0;
        proxy.op_status = 123;
        proxy.op_pending.store(true, std::memory_order_release);
        auto identity = IDENTITY;
        if (stale_generation) ++identity.generation;
        detail::handle_vfs_op_resp(&header, input.data(), static_cast<uint16_t>(sizeof(response) + length + (trailing ? 1 : 0)), identity);
        bool const COMPLETED = waiter.state.load(std::memory_order_acquire) == WkiWaitEntry::DONE;
        bool valid = proxy.op_pending.load(std::memory_order_acquire) && proxy.op_wait_entry == &waiter &&
                     proxy.op_expected_id == operation && proxy.op_expected_seq == 7;
        if (ignored) {
            valid = valid && !COMPLETED && proxy.op_status == 123 && proxy.op_resp_len == 0 && output == UNCHANGED;
        } else if (expected != 0) {
            valid = valid && COMPLETED && proxy.op_status == expected && proxy.op_resp_len == 0 && output == UNCHANGED;
        } else {
            valid = valid && COMPLETED && proxy.op_status == 0 && proxy.op_resp_len == length &&
                    std::memcmp(output.data(), input.data() + sizeof(response), length) == 0;
            for (size_t i = length; i < output.size(); ++i) valid = valid && output.at(i) == 0xA5;
        }
        clear_proxy_op_state_locked(&proxy, 0);
        return valid;
    };
    for (uint16_t operation : {OP_VFS_OPEN_AS, OP_VFS_OPEN_READ_AS}) {
        reply = {};
        reply.fd = 23;
        if (!check(operation, 0, OPEN_RESP_NO_STAT_LEN, 0) || !check(operation, -EACCES, 0, -EACCES) ||
            !check(operation, -ENOENT, 0, -ENOENT) || !check(operation, 1, 0, -EPROTO) ||
            !check(operation, -EACCES, OPEN_RESP_NO_STAT_LEN, -EPROTO))
            return false;
        for (uint16_t length = 0; length <= sizeof(reply) + 1; ++length) {
            if (length == OPEN_RESP_NO_STAT_LEN) continue;
            if (!check(operation, 0, length, -EPROTO)) return false;
        }
        if (!check(operation, 0, OPEN_RESP_NO_STAT_LEN, -EPROTO, false, OPEN_RESP_NO_STAT_LEN - 1) ||
            !check(operation, 0, OPEN_RESP_NO_STAT_LEN, 0, true, sizeof(reply), true) ||
            !check(operation, 0, OPEN_RESP_NO_STAT_LEN, 0, true, sizeof(reply), false, true) ||
            !check(operation, 0, OPEN_RESP_NO_STAT_LEN, 0, true, sizeof(reply), false, false, true))
            return false;
        reply.fd = -1;
        if (!check(operation, 0, OPEN_RESP_NO_STAT_LEN, -EPROTO)) return false;
        reply.fd = 23;
        reply.is_dir = 2;
        if (!check(operation, 0, OPEN_RESP_NO_STAT_LEN, -EPROTO)) return false;
        reply.is_dir = 1;
        if (!check(operation, 0, OPEN_RESP_NO_STAT_LEN, 0)) return false;
        reply.has_stat = 1;
        if (!check(operation, 0, OPEN_RESP_NO_STAT_LEN, -EPROTO) || !check(operation, 0, OPEN_RESP_WITH_STAT_LEN, 0)) return false;
        reply.is_dir = 0;
        reply.prefetched_bytes = 1;
        if (!check(operation, 0, OPEN_RESP_WITH_PREFETCH_LEN, 0)) return false;
        reply.prefetched_bytes = VFS_RDMA_BULK_SIZE;
        if (!check(operation, 0, OPEN_RESP_WITH_PREFETCH_LEN, 0)) return false;
        reply.prefetched_bytes = VFS_RDMA_BULK_SIZE + 1;
        if (!check(operation, 0, OPEN_RESP_WITH_PREFETCH_LEN, -EPROTO)) return false;
        reply.prefetched_bytes = 0;
        if (!check(operation, 0, OPEN_RESP_WITH_PREFETCH_LEN, -EPROTO)) return false;
        reply.has_stat = 2;
        if (!check(operation, 0, OPEN_RESP_WITH_STAT_LEN, -EPROTO)) return false;
    }
    for (uint16_t operation : {OP_VFS_DIRECTORY_REFERENCE, OP_VFS_DIRECTORY_GRANT}) {
        if (!check(operation, 0, 0, -EPROTO) || !check(operation, -EACCES, 1, -EPROTO) || !check(operation, -EACCES, 0, -EACCES))
            return false;
    }
    auto lifetime_check = [&](uint16_t inner, int16_t status, uint16_t length, int expected, const void* body = nullptr,
                              uint64_t challenge = 0, bool stale_cookie = false, bool stale_generation = false,
                              uint16_t capacity = sizeof(OpenRespPayload), bool trailing = false) {
        return check(OP_VFS_OPEN_LIFETIME, status, length, expected, stale_cookie || stale_generation || trailing, capacity, trailing,
                     stale_cookie, stale_generation, inner, challenge, body);
    };
    for (uint16_t inner : {OP_VFS_OPEN_AS, OP_VFS_OPEN_READ_AS}) {
        reply = {};
        reply.fd = 23;
        if (!lifetime_check(inner, 0, OPEN_RESP_NO_STAT_LEN, 0) || !lifetime_check(inner, -EACCES, 0, -EACCES) ||
            !lifetime_check(inner, -EACCES, 1, -EPROTO) || !lifetime_check(inner, 1, 0, -EPROTO))
            return false;
        for (uint16_t length = 0; length <= sizeof(reply) + 1; ++length)
            if (length != OPEN_RESP_NO_STAT_LEN && !lifetime_check(inner, 0, length, -EPROTO)) return false;
        if (!lifetime_check(inner, 0, OPEN_RESP_NO_STAT_LEN, 0, nullptr, 0, true) ||
            !lifetime_check(inner, 0, OPEN_RESP_NO_STAT_LEN, 0, nullptr, 0, false, true) ||
            !lifetime_check(inner, 0, OPEN_RESP_NO_STAT_LEN, 0, nullptr, 0, false, false, sizeof(reply), true) ||
            !lifetime_check(inner, 0, OPEN_RESP_NO_STAT_LEN, -EPROTO, nullptr, 0, false, false, OPEN_RESP_NO_STAT_LEN - 1))
            return false;
        reply.fd = 0;
        if (!lifetime_check(inner, 0, OPEN_RESP_NO_STAT_LEN, -EPROTO)) return false;
        reply.fd = 23;
        reply.has_stat = 1;
        if (!lifetime_check(inner, 0, OPEN_RESP_WITH_STAT_LEN, 0)) return false;
        reply.prefetched_bytes = 1;
        if (!lifetime_check(inner, 0, OPEN_RESP_WITH_PREFETCH_LEN, 0)) return false;
    }
    VfsOpenLifetimeRequest binding{.inner_op = OP_VFS_FEATURES, .token = 91, .binding_id = 88, .owner_boot_epoch = 73};
    if (!lifetime_check(OP_VFS_FEATURES, 0, sizeof(binding), 0, &binding, 91) ||
        !lifetime_check(OP_VFS_FEATURES, 0, sizeof(binding), -EPROTO, &binding, 92) ||
        !lifetime_check(OP_VFS_FEATURES, 0, sizeof(binding) - 1, -EPROTO, &binding, 91) ||
        !lifetime_check(OP_VFS_FEATURES, 0, sizeof(binding), 0, &binding, 91, true))
        return false;
    binding.binding_id = 0;
    if (!lifetime_check(OP_VFS_FEATURES, 0, sizeof(binding), -EPROTO, &binding, 91) || !lifetime_check(0, 0, 0, 0) ||
        !lifetime_check(0, 0, 1, -EPROTO) || !lifetime_check(0, -ENOTCONN, 0, -ENOTCONN))
        return false;
    struct PathReply {
        VfsPathIdentityHeader header;
        VfsPathOpenResult result;
    } path{};
    static_assert(sizeof(path) == sizeof(VfsPathIdentityHeader) + sizeof(VfsPathOpenResult));
    path.header.operation = VfsPathOperation::OPEN_AT;
    path.header.data_len = sizeof(VfsPathOpenResult);
    path.result.fd = 23;
    if (!lifetime_check(OP_VFS_PATH_AS, 0, sizeof(path), 0, &path)) return false;
    path.result.fd = 0;
    if (!lifetime_check(OP_VFS_PATH_AS, 0, sizeof(path), -EPROTO, &path)) return false;
    return true;
}
#endif

constexpr uint64_t STALE_FD_TIMEOUT_US = 30000000;  // 30 seconds

void wki_remote_vfs_gc_stale_fds() {
    uint64_t const NOW = wki_now_us();
    std::array<uint16_t, WKI_MAX_PEERS> stale_peers{};
    size_t stale_peer_count = 0;

    s_vfs_lock.lock();
    for (const auto& [fd_id, rfd] : g_remote_fds) {
        static_cast<void>(fd_id);
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
        for (auto& [fd_id, rfd] : g_remote_fds) {
            static_cast<void>(fd_id);
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
            std::erase_if(g_remote_fds, [NODE_ID](const auto& entry) {
                auto const& rfd = entry.second;
                return rfd.consumer_node == NODE_ID && rfd.retiring && rfd.file == nullptr;
            });
        }
        s_vfs_lock.unlock();

        for (auto* file : files_to_close) {
            if (file != nullptr) {
                static_cast<void>(release_server_file(file));
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
    g_vfs_withdrawals.cancel_reservation();
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
    if (!g_vfs_withdrawals.reserve(g_vfs_exports.size())) {
        s_vfs_lock.unlock();
        return false;
    }
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
        g_vfs_withdrawals.cancel_reservation();
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

    // Admission is still closed. Every peer that could have attached an old
    // export is now in this stable membership snapshot. New peers cannot
    // obtain an obsolete binding after reconciliation reopens admission.
    WithdrawalPeers peers;
    g_wki.peer_lock.lock();
    for (size_t index = 0; index < g_wki.peers.size(); ++index) {
        if (g_wki.peers.at(index).node_id != WKI_NODE_INVALID) peers.set(index);
    }
    g_wki.peer_lock.unlock();

    s_vfs_lock.lock();
    for (const auto& exp : stale_exports) {
        ResourceIncarnationToken const INCARNATION = {g_wki.local_boot_epoch, exp.resource_incarnation};
        if (!g_vfs_withdrawals.append(exp.resource_id, INCARNATION, peers)) [[unlikely]] {
            ker::mod::dbg::panic_handler("WKI VFS withdrawal reservation lost during pivot");
            hcf();
        }
    }
    s_vfs_lock.unlock();

    // Publish the outbox before the replacement table becomes even, so an
    // already-running advertisement pass cannot skip the new withdrawals.
    reconcile_and_publish_vfs_exports();

    wki_remote_vfs_advertise_exports();
}

void wki_remote_vfs_commit_export_unmount(uint32_t backing_dev_id) {
    if (!g_remote_vfs_initialized) return;
    // Admission is already closed and old binding users drained. Copy peer
    // membership without nesting its lock under the export registry lock.
    WithdrawalPeers peers;
    g_wki.peer_lock.lock();
    for (size_t index = 0; index < g_wki.peers.size(); ++index)
        if (g_wki.peers.at(index).node_id != WKI_NODE_INVALID) peers.set(index);
    g_wki.peer_lock.unlock();
    s_vfs_lock.lock();
    if (!g_vfs_export_rebuild_prepared || g_vfs_export_target_revision == 0 || backing_dev_id == 0) {
        s_vfs_lock.unlock();
        ker::mod::dbg::panic_handler("WKI native unmount lost prepared export transition");
        hcf();
    }
    for (auto it = g_vfs_exports.begin(); it != g_vfs_exports.end();) {
        if (!it->active || it->backing_dev_id != backing_dev_id) {
            ++it;
            continue;
        }
        ResourceIncarnationToken const INCARNATION = {g_wki.local_boot_epoch, it->resource_incarnation};
        if (!g_vfs_withdrawals.append(it->resource_id, INCARNATION, peers)) [[unlikely]] {
            s_vfs_lock.unlock();
            ker::mod::dbg::panic_handler("WKI native unmount lost withdrawal reservation");
            hcf();
        }
        it = g_vfs_exports.erase(it);
    }
    s_vfs_lock.unlock();
    // Exact old bindings retire before admission reopens. Do not rediscover
    // export names here: that would interpret them through the syscall task.
    reconcile_and_publish_vfs_exports();
    wki_remote_vfs_advertise_exports();
}

#ifdef WOS_SELFTEST
auto wki_remote_vfs_selftest_native_unmount_exports() -> bool {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_unmount_exports";
    constexpr auto OTHER = "/tmp/ktest_unmount_exports_other";
    // KTEST runs before WKI initialization. Exercise real export transitions
    // without a transport or published peer and restore all temporary state.
    if (g_wki.initialized || g_remote_vfs_initialized || !g_vfs_exports.empty() || g_vfs_export_rebuild_prepared) return false;
    for (const auto& peer : g_wki.peers)
        if (peer.node_id != WKI_NODE_INVALID) return false;
    auto check = [](bool okay, int line) {
        if (!okay) ker::mod::dbg::logger<"ktest">::error("native unmount export check failed at %d", line);
        return okay;
    };
    struct Cleanup {
        uint32_t next_id = g_next_vfs_resource_id;
        uint32_t next_incarnation = g_next_vfs_resource_incarnation;
        uint32_t boot = g_wki.local_boot_epoch;
        uint64_t revision = g_vfs_export_revision;
        std::unique_ptr<VfsWithdrawalOutbox> withdrawals{new VfsWithdrawalOutbox(g_vfs_withdrawals)};
        bool base = false, other = false;
        ~Cleanup() {
            if (g_vfs_export_rebuild_prepared) wki_remote_vfs_cancel_export_rebuild();
            g_remote_vfs_initialized = false;
            g_vfs_exports.clear();
            g_next_vfs_resource_id = next_id;
            g_next_vfs_resource_incarnation = next_incarnation;
            g_vfs_export_revision = revision;
            g_vfs_withdrawals = *withdrawals;
            g_wki.local_boot_epoch = boot;
            if (base) static_cast<void>(vfs::unmount_filesystem(BASE));
            if (other) static_cast<void>(vfs::unmount_filesystem(OTHER));
            static_cast<void>(vfs::vfs_rmdir(BASE));
            static_cast<void>(vfs::vfs_rmdir(OTHER));
        }
    } cleanup;
    if (!check(vfs::vfs_mkdir(BASE, 0755) == 0 && vfs::vfs_mkdir(OTHER, 0755) == 0, __LINE__)) return false;
    if (!check(vfs::mount_filesystem(BASE, "tmpfs", nullptr) == 0, __LINE__)) return false;
    cleanup.base = true;
    if (!check(vfs::mount_filesystem(OTHER, "tmpfs", nullptr) == 0, __LINE__)) return false;
    cleanup.other = true;
    g_remote_vfs_initialized = true;
    if (g_wki.local_boot_epoch == 0) g_wki.local_boot_epoch = 1;
    uint32_t const ID = wki_remote_vfs_export_add(BASE, BASE);
    uint32_t const OTHER_ID = wki_remote_vfs_export_add(OTHER, "/ktest_unmount_unrelated_alias");
    VfsExport original{}, unrelated{}, current{};
    if (!check(wki_remote_vfs_find_export_snapshot(ID, &original) && wki_remote_vfs_find_export_snapshot(OTHER_ID, &unrelated), __LINE__))
        return false;
    std::unique_ptr<vfs::File, DirectoryGrantFileRelease> held(vfs::vfs_open_file(BASE, 0, 0));
    if (!check(held != nullptr && wki_remote_vfs_prepare_export_rebuild(), __LINE__)) return false;
    bool const CLOSED = !wki_dev_server_vfs_rx_admission_try_acquire();
    if (!CLOSED) wki_dev_server_vfs_rx_admission_release();
    if (!check(CLOSED && vfs::unmount_filesystem_if_dev_id(BASE, original.backing_dev_id) == -EBUSY, __LINE__)) return false;
    wki_remote_vfs_cancel_export_rebuild();
    if (!check(wki_remote_vfs_find_export_snapshot(ID, &current) && current.resource_incarnation == original.resource_incarnation &&
                   current.backing_dev_id == original.backing_dev_id && wki_remote_vfs_export_snapshot_is_current(current) &&
                   !wki_remote_vfs_export_snapshot_is_current(original),
               __LINE__))
        return false;
    if (!check(wki_dev_server_vfs_rx_admission_try_acquire(), __LINE__)) return false;
    wki_dev_server_vfs_rx_admission_release();
    held.reset();
    if (!check(wki_remote_vfs_prepare_export_rebuild() && vfs::unmount_filesystem_if_dev_id(BASE, original.backing_dev_id) == 0, __LINE__))
        return false;
    cleanup.base = false;
    wki_remote_vfs_commit_export_unmount(original.backing_dev_id);
    if (!check(!wki_remote_vfs_find_export_snapshot(ID, &current) && wki_remote_vfs_find_export_snapshot(OTHER_ID, &current) &&
                   current.resource_incarnation == unrelated.resource_incarnation && current.backing_dev_id == unrelated.backing_dev_id,
               __LINE__))
        return false;
    if (!check(vfs::mount_filesystem(BASE, "tmpfs", nullptr) == 0, __LINE__)) return false;
    cleanup.base = true;
    wki_remote_vfs_refresh_exports();
    uint32_t const NEW_ID = wki_remote_vfs_export_add(BASE, BASE);
    if (!check(wki_remote_vfs_find_export_snapshot(NEW_ID, &current) && current.resource_incarnation != original.resource_incarnation &&
                   current.backing_dev_id != original.backing_dev_id && !wki_remote_vfs_find_export_snapshot(ID, &original),
               __LINE__))
        return false;
    if (!check(wki_dev_server_vfs_rx_admission_try_acquire(), __LINE__)) return false;
    wki_dev_server_vfs_rx_admission_release();
    return true;
}
#endif

// -------------------------------------------------------------------------------
// Fencing Cleanup
// -------------------------------------------------------------------------------

namespace {
auto vfs_proxy_requires_peer_cleanup(const ProxyVfsState* state, uint16_t node_id) -> bool {
    return state != nullptr && state->owner_node == node_id &&
           (state->active || state->epoch_reset_pending || state->attach_pending.load(std::memory_order_acquire));
}
}  // namespace

#ifdef WOS_SELFTEST
auto wki_remote_vfs_selftest_peer_cleanup_includes_pending_attach() -> bool {
    ProxyVfsState state = {};
    state.owner_node = 0x5101;
    if (vfs_proxy_requires_peer_cleanup(nullptr, state.owner_node) || vfs_proxy_requires_peer_cleanup(&state, 0x5102) ||
        vfs_proxy_requires_peer_cleanup(&state, state.owner_node)) {
        return false;
    }

    state.attach_pending.store(true, std::memory_order_release);
    bool const PENDING_ATTACH_INCLUDED = vfs_proxy_requires_peer_cleanup(&state, state.owner_node);
    state.attach_pending.store(false, std::memory_order_release);
    state.active = true;
    bool const ACTIVE_INCLUDED = vfs_proxy_requires_peer_cleanup(&state, state.owner_node);
    state.active = false;
    state.epoch_reset_pending = true;
    bool const EPOCH_MARKER_INCLUDED = vfs_proxy_requires_peer_cleanup(&state, state.owner_node);
    return PENDING_ATTACH_INCLUDED && ACTIVE_INCLUDED && EPOCH_MARKER_INCLUDED;
}
#endif

void wki_remote_vfs_mark_server_fds_for_channel(const WkiChannelIdentity& channel_identity) {
    if (channel_identity.channel == nullptr || channel_identity.generation == 0) {
        return;
    }

    bool marked = false;
    s_vfs_lock.lock();
    for (auto& [fd_id, rfd] : g_remote_fds) {
        static_cast<void>(fd_id);
        // Inactive OPEN reservations must also be fenced before they can
        // publish a File on this generation.
        if (rfd.retiring || !vfs_channel_identity_matches(rfd.channel_identity, channel_identity)) {
            continue;
        }
        ker::vfs::vfs_advisory_remote_close_admission(rfd.file);
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

void wki_remote_vfs_process_pending_exec_cancellations() {
    // One RPC per invocation bounds disruption of the shared client worker.
    // Advance past failed/idle records, and let the periodic timer pace retries.
    uint16_t selected = VFS_EXEC_CANCEL_SLOT_NONE;
    s_vfs_lock.lock();
    for (size_t scanned = 0; scanned < g_exec_cancellations.size(); ++scanned) {
        size_t const INDEX = g_exec_cancel_cursor;
        g_exec_cancel_cursor = (g_exec_cancel_cursor + 1) % g_exec_cancellations.size();
        auto& record = g_exec_cancellations.at(INDEX);
        if (record.phase != ExecCancellationPhase::PENDING || wki_now_us() < record.retry_after_us) continue;
        record.phase = ExecCancellationPhase::RETRYING;
        selected = static_cast<uint16_t>(INDEX);
        break;
    }
    s_vfs_lock.unlock();
    if (selected != VFS_EXEC_CANCEL_SLOT_NONE) static_cast<void>(cancel_remote_exec_view(selected, true));
}

void wki_remote_vfs_process_pending_open_cancellations() {
    uint16_t selected = VFS_EXEC_CANCEL_SLOT_NONE;
    s_vfs_lock.lock();
    for (size_t scanned = 0; scanned < g_open_cancellations.size(); ++scanned) {
        size_t const INDEX = g_open_cancel_cursor;
        g_open_cancel_cursor = (g_open_cancel_cursor + 1) % g_open_cancellations.size();
        auto& record = g_open_cancellations.at(INDEX);
        if (record.phase != ExecCancellationPhase::PENDING || wki_now_us() < record.retry_after_us) continue;
        record.phase = ExecCancellationPhase::RETRYING;
        selected = static_cast<uint16_t>(INDEX);
        break;
    }
    s_vfs_lock.unlock();
    if (selected != VFS_EXEC_CANCEL_SLOT_NONE) static_cast<void>(cancel_remote_ordinary_open(selected, true));
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
        for (auto& [fd_id, rfd] : g_remote_fds) {
            static_cast<void>(fd_id);
            if (!rfd.retiring || rfd.file == nullptr || close_count >= files_to_close.size()) {
                continue;
            }
            files_to_close.at(close_count++) = rfd.file;
            rfd.file = nullptr;
        }
        std::erase_if(g_remote_fds, [](const auto& entry) {
            auto const& rfd = entry.second;
            return rfd.retiring && rfd.file == nullptr;
        });
        s_vfs_lock.unlock();

        for (size_t i = 0; i < close_count; ++i) {
            static_cast<void>(release_server_file(files_to_close.at(i)));
        }
        if (close_count < files_to_close.size()) {
            return;
        }
    }
}

void wki_remote_vfs_cleanup_server_fds_for_channel(const WkiChannelIdentity& channel_identity) {
    VfsXattrServerStage* stage = nullptr;
    s_vfs_lock.lock();
    auto const STAGE_IT = g_vfs_xattr_stages.find(vfs_xattr_stage_key(channel_identity));
    if (STAGE_IT != g_vfs_xattr_stages.end()) {
        stage = STAGE_IT->second;
        g_vfs_xattr_stages.erase(STAGE_IT);
    }
    s_vfs_lock.unlock();
    release_vfs_xattr_stage(stage);
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
    std::array<VfsXattrServerStage*, VFS_XATTR_SERVER_STAGE_COUNT_MAX> xattr_stages_to_release{};
    size_t xattr_stage_release_count = 0;
    std::deque<PendingProxyTeardown> proxies_to_cleanup;

    s_vfs_lock.lock();
    for (auto stage = g_vfs_xattr_stages.begin(); stage != g_vfs_xattr_stages.end();) {
        if (stage->first.peer_node_id != node_id) {
            ++stage;
            continue;
        }
        xattr_stages_to_release.at(xattr_stage_release_count++) = stage->second;
        stage = g_vfs_xattr_stages.erase(stage);
    }
    for (auto& [fd_id, rfd] : g_remote_fds) {
        static_cast<void>(fd_id);
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
    std::erase_if(g_remote_fds, [node_id](const auto& entry) {
        auto const& rfd = entry.second;
        return rfd.consumer_node == node_id && rfd.retiring && rfd.file == nullptr;
    });

    // Consumer side: fail pending ops and deactivate proxies while the proxy
    // registry is locked.  Cache invalidation and channel close happen later
    // because they can take other subsystem locks.
    for (auto& proxy : g_vfs_proxies) {
        auto* p = proxy.get();
        if (!vfs_proxy_requires_peer_cleanup(p, node_id)) {
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

        proxies_to_cleanup.push_back(cleanup);
    }
    s_vfs_lock.unlock();

    for (const auto& cleanup : proxies_to_cleanup) {
        finish_proxy_teardown_op_waiter(cleanup, -1);
    }

    for (size_t index = 0; index < xattr_stage_release_count; ++index) {
        release_vfs_xattr_stage(xattr_stages_to_release.at(index));
    }

    for (auto* file : files_to_close) {
        if (file != nullptr) {
            static_cast<void>(release_server_file(file));
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
