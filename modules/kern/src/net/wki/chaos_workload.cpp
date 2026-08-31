#include "chaos_workload.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <dev/block_device.hpp>
#include <initializer_list>
#include <net/netdevice.hpp>
#include <net/wki/blk_ring.hpp>
#include <net/wki/chaos.hpp>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_net.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/mutex.hpp>

namespace ker::net::wki {

namespace {

constexpr size_t SCRATCH_BLOCK_SIZE = 512;
constexpr uint64_t SCRATCH_TOTAL_BLOCKS = WKI_CHAOS_WORKLOAD_SCRATCH_BYTES / SCRATCH_BLOCK_SIZE;
constexpr uint16_t BLOCK_TRANSFER_MAX = 8;
constexpr size_t BLOCK_TRANSFER_BYTES_MAX = 4096;
constexpr char SCRATCH_RESOURCE_NAME[] = "wki-chaos-scratch";
constexpr char BLOCK_PROXY_NAME[] = "wki-chaos-b1";
constexpr char VFS_ADVERTISED_NAME[] = "/tmp";
constexpr size_t COMPUTE_PUBLISH_WAITER_MAX = 8;
constexpr uint64_t COMPUTE_PUBLISH_POLL_MAX_US = 1'000;

enum class WorkloadOp : uint8_t {
    NONE,
    BLOCK,
    BLOCK_DISCOVERED,
    BLOCK_RDMA_DISCOVERED,
    NET_ATTACH,
    NET_ATTACH_DISCOVERED,
    NET_QUERY,
    NET_QUERY_DISCOVERED,
    NET_DETACH,
    NET_DETACH_DISCOVERED,
    VFS_UNMOUNT,
    VFS_QUERY_DISCOVERED,
    VFS_UNMOUNT_DISCOVERED,
    COMPUTE_PUBLISH_SET,
    COMPUTE_PUBLISH_QUERY,
};

struct WorkloadCommand {
    WorkloadOp op = WorkloadOp::NONE;
    uint16_t owner = WKI_NODE_INVALID;
    uint32_t resource = 0;
    uint64_t generation = 0;
    uint64_t lba = 0;
    uint16_t blocks = 0;
    uint64_t pattern = 0;
    bool hold = false;
    std::array<char, net::NETDEV_NAME_LEN> name{};
};

struct WorkloadResult {
    WorkloadOp op = WorkloadOp::NONE;
    int status = -ENODATA;
    uint16_t owner = WKI_NODE_INVALID;
    uint32_t resource = 0;
    uint64_t generation = 0;
    ResourceIncarnationToken owner_incarnation = {};
    uint16_t channel = 0;
    uint32_t channel_generation = 0;
    uint16_t attach_cookie = 0;
    uint32_t channel_tx_seq_before = 0;
    uint32_t channel_tx_seq_after = 0;
    uint32_t channel_tx_delta = 0;
    uint64_t lba = 0;
    uint16_t blocks = 0;
    uint32_t block_size = 0;
    uint32_t bytes = 0;
    uint32_t ifindex = 0;
    uint32_t rdma_zone = 0;
    uint64_t data_slots_before = 0;
    uint64_t data_slots_after = 0;
    uint64_t tags_before = 0;
    uint64_t tags_after = 0;
    BlkRingGeometry ring_geometry{};
    BlkRingIndices ring_before{};
    BlkRingIndices ring_after{};
    uint8_t lane_index = 0;
    uint8_t lane_count = 0;
    uint32_t detach_peer_boot_epoch = 0;
    int detach_status = -ENODATA;
    std::array<char, net::NETDEV_NAME_LEN> name{};
    int read_status = -ENODATA;
    int write_status = -ENODATA;
    int flush_status = -ENODATA;
    int verify_status = -ENODATA;
    int restore_status = -ENODATA;
    bool exact_binding = false;
    bool rdma = false;
    bool rdma_roce = false;
    bool verified = false;
    bool restored = false;
    bool detached = false;
    bool detach_pending = false;
    bool ring_geometry_valid = false;
    bool ring_indices_valid = false;
    bool ring_quiescent = false;
    bool compute_hold = false;
    uint32_t compute_waiters = 0;
    uint64_t compute_wait_count = 0;
    uint64_t compute_release_generation = 0;
    int compute_last_wait_status = -ENODATA;
};

struct VfsLaneTuple {
    uint8_t lane_index = 0;
    uint16_t channel = 0;
    uint32_t channel_generation = 0;
    uint8_t attach_cookie = 0;
    bool valid = false;
};

struct VfsBindingSnapshot {
    WkiRemoteVfsProxyDiag anchor{};
    std::array<VfsLaneTuple, VFS_PROXY_LANE_COUNT> lanes{};
    uint8_t lane_count = 0;
};

struct ManagedNetAttachment {
    bool owned = false;
    bool detach_issued = false;
    bool detach_binding_exact = false;
    uint16_t owner = WKI_NODE_INVALID;
    uint32_t resource = 0;
    uint64_t generation = 0;
    ResourceIncarnationToken owner_incarnation = {};
    uint32_t binding_peer_boot_epoch = 0;
    uint16_t channel = 0;
    uint32_t channel_generation = 0;
    uint8_t attach_cookie = 0;
    uint32_t ifindex = 0;
    std::array<char, net::NETDEV_NAME_LEN> name{};
    net::NetDevice* device = nullptr;
};

// All mutable adapter state is serialized by s_workload_lock. The atomics are
// only the cheap gate read used by advertisement callbacks and procfs stat/read.
mod::sys::Mutex s_workload_lock;                                // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
mod::sys::Mutex s_scratch_lock;                                 // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> s_workload_boot_allowed{false};               // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> s_scratch_registered{false};                  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> s_compute_publish_hold{false};                // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint32_t> s_compute_publish_waiters{0};             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> s_compute_publish_wait_count{0};          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> s_compute_publish_release_generation{0};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<int> s_compute_publish_last_wait_status{-ENODATA};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<std::atomic<mod::sched::task::Task*>, COMPUTE_PUBLISH_WAITER_MAX> s_compute_publish_waiter_tasks{};  // NOLINT
std::array<uint8_t, WKI_CHAOS_WORKLOAD_SCRATCH_BYTES> s_scratch_storage{};                                      // NOLINT
dev::BlockDevice s_scratch_device{};                                                                            // NOLINT
WorkloadResult s_result{};                                                                                      // NOLINT
ManagedNetAttachment s_managed_net{};                                                                           // NOLINT
std::array<VfsLaneTuple, VFS_PROXY_LANE_COUNT> s_result_vfs_lanes{};                                            // NOLINT
std::array<WkiDevProxyDiagRow, WKI_DEV_PROXY_DIAG_MAX> s_block_diag_rows{};                                     // NOLINT
std::array<WkiRemoteNetDiagRow, WKI_REMOTE_NET_DIAG_MAX> s_net_diag_rows{};                                     // NOLINT
std::array<WkiRemoteVfsProxyDiag, WKI_REMOTE_VFS_PROXY_DIAG_MAX> s_vfs_diag_rows{};                             // NOLINT
std::array<WkiChannelDiag, WKI_CHANNEL_DIAG_MAX> s_channel_diag_rows{};                                         // NOLINT

auto runtime_control_allowed() -> bool {
    WkiChaosSnapshot snapshot{};
    return wki_chaos_snapshot(&snapshot) && snapshot.runtime_control_allowed;
}

auto gates_allowed() -> bool { return s_workload_boot_allowed.load(std::memory_order_acquire) && runtime_control_allowed(); }

void release_compute_publish_hold() {
    s_compute_publish_hold.store(false, std::memory_order_release);
    s_compute_publish_release_generation.fetch_add(1, std::memory_order_acq_rel);
    // Submit workers are fixed-lifetime daemon tasks. A slot is published only
    // while its task is sleeping/yielding in the hook, and event wakeups retain
    // an event-before-park token, so no release can be lost.
    for (auto& waiter : s_compute_publish_waiter_tasks) {
        auto* task = waiter.load(std::memory_order_acquire);
        if (task != nullptr) {
            mod::sched::wake_task_from_event(task);
        }
    }
}

void copy_compute_state_to_result() {
    s_result.compute_hold = s_compute_publish_hold.load(std::memory_order_acquire);
    s_result.compute_waiters = s_compute_publish_waiters.load(std::memory_order_acquire);
    s_result.compute_wait_count = s_compute_publish_wait_count.load(std::memory_order_acquire);
    s_result.compute_release_generation = s_compute_publish_release_generation.load(std::memory_order_acquire);
    s_result.compute_last_wait_status = s_compute_publish_last_wait_status.load(std::memory_order_acquire);
}

auto scratch_range_valid(uint64_t block, size_t count) -> bool {
    return block <= SCRATCH_TOTAL_BLOCKS && count <= SCRATCH_TOTAL_BLOCKS - block;
}

auto scratch_read(dev::BlockDevice* device, uint64_t block, size_t count, void* buffer) -> int {
    if (device != &s_scratch_device || buffer == nullptr || !gates_allowed() || !scratch_range_valid(block, count)) {
        return -EINVAL;
    }
    size_t const OFFSET = static_cast<size_t>(block) * SCRATCH_BLOCK_SIZE;
    size_t const BYTES = count * SCRATCH_BLOCK_SIZE;
    mod::sys::MutexGuard const GUARD(s_scratch_lock);
    std::memcpy(buffer, s_scratch_storage.data() + OFFSET, BYTES);
    return 0;
}

auto scratch_write(dev::BlockDevice* device, uint64_t block, size_t count, const void* buffer) -> int {
    if (device != &s_scratch_device || buffer == nullptr || !gates_allowed() || !scratch_range_valid(block, count)) {
        return -EINVAL;
    }
    size_t const OFFSET = static_cast<size_t>(block) * SCRATCH_BLOCK_SIZE;
    size_t const BYTES = count * SCRATCH_BLOCK_SIZE;
    mod::sys::MutexGuard const GUARD(s_scratch_lock);
    std::memcpy(s_scratch_storage.data() + OFFSET, buffer, BYTES);
    return 0;
}

auto scratch_flush(dev::BlockDevice* device) -> int { return device == &s_scratch_device && gates_allowed() ? 0 : -EPERM; }

auto scratch_can_remote() -> bool { return gates_allowed() && s_scratch_registered.load(std::memory_order_acquire); }
auto scratch_can_share() -> bool { return true; }
auto scratch_can_passthrough() -> bool { return false; }
auto scratch_on_attach(uint16_t /*node_id*/) -> int { return scratch_can_remote() ? 0 : -EPERM; }
void scratch_on_detach(uint16_t /*node_id*/) {}
void scratch_on_fault(uint16_t /*node_id*/) {}

constexpr RemotableOps SCRATCH_REMOTABLE_OPS{
    .can_remote = scratch_can_remote,
    .can_share = scratch_can_share,
    .can_passthrough = scratch_can_passthrough,
    .on_remote_attach = scratch_on_attach,
    .on_remote_detach = scratch_on_detach,
    .on_remote_fault = scratch_on_fault,
};

class TokenCursor {
   public:
    explicit TokenCursor(char* text) : cursor_(text) {}

    auto next() -> char* {
        if (cursor_ == nullptr) {
            return nullptr;
        }
        while (*cursor_ == ' ') {
            ++cursor_;
        }
        if (*cursor_ == '\0') {
            return nullptr;
        }
        char* token = cursor_;
        while (*cursor_ != '\0' && *cursor_ != ' ') {
            ++cursor_;
        }
        if (*cursor_ == ' ') {
            *cursor_++ = '\0';
        }
        return token;
    }

   private:
    char* cursor_;
};

auto parse_decimal(const char* text, uint64_t maximum, uint64_t* out) -> bool {
    if (text == nullptr || out == nullptr || *text == '\0') {
        return false;
    }
    uint64_t value = 0;
    for (const char* cursor = text; *cursor != '\0'; ++cursor) {
        if (*cursor < '0' || *cursor > '9') {
            return false;
        }
        uint8_t const DIGIT = static_cast<uint8_t>(*cursor - '0');
        if (DIGIT > maximum || value > (maximum - DIGIT) / 10) {
            return false;
        }
        value = (value * 10) + DIGIT;
    }
    *out = value;
    return true;
}

auto parse_key_decimal(char* token, const char* key, uint64_t maximum, uint64_t* out) -> bool {
    if (token == nullptr || key == nullptr) {
        return false;
    }
    size_t const KEY_LEN = std::strlen(key);
    return std::strncmp(token, key, KEY_LEN) == 0 && token[KEY_LEN] == '=' && parse_decimal(token + KEY_LEN + 1, maximum, out);
}

auto valid_net_name(const char* name) -> bool {
    if (name == nullptr || std::strncmp(name, "wkc", 3) != 0) {
        return false;
    }
    size_t const LENGTH = std::strlen(name);
    if (LENGTH <= 3 || LENGTH >= net::NETDEV_NAME_LEN) {
        return false;
    }
    for (size_t i = 3; i < LENGTH; ++i) {
        if (name[i] < '0' || name[i] > '9') {
            return false;
        }
    }
    return true;
}

auto parse_identity(TokenCursor& tokens, WorkloadCommand* out) -> bool {
    if (out == nullptr) {
        return false;
    }
    uint64_t owner = 0;
    uint64_t resource = 0;
    if (!parse_key_decimal(tokens.next(), "owner", UINT16_MAX, &owner) || owner == WKI_NODE_INVALID || owner == WKI_NODE_BROADCAST ||
        !parse_key_decimal(tokens.next(), "resource", UINT32_MAX, &resource) ||
        !parse_key_decimal(tokens.next(), "generation", UINT64_MAX, &out->generation) || out->generation == 0) {
        return false;
    }
    out->owner = static_cast<uint16_t>(owner);
    out->resource = static_cast<uint32_t>(resource);
    return true;
}

auto parse_owner(TokenCursor& tokens, WorkloadCommand* out) -> bool {
    if (out == nullptr) {
        return false;
    }
    uint64_t owner = 0;
    if (!parse_key_decimal(tokens.next(), "owner", UINT16_MAX, &owner) || owner == WKI_NODE_INVALID || owner == WKI_NODE_BROADCAST) {
        return false;
    }
    out->owner = static_cast<uint16_t>(owner);
    return true;
}

auto parse_command(const char* command, size_t len, WorkloadCommand* out) -> bool {
    if (command == nullptr || out == nullptr || len == 0 || len > WKI_CHAOS_WORKLOAD_COMMAND_MAX) {
        return false;
    }
    *out = {};
    std::array<char, WKI_CHAOS_WORKLOAD_COMMAND_MAX + 1> copy{};
    for (size_t i = 0; i < len; ++i) {
        unsigned char const C = static_cast<unsigned char>(command[i]);
        if (C == 0 || (C < 0x20 && C != '\r' && C != '\n') || C > 0x7E) {
            return false;
        }
        copy.at(i) = static_cast<char>(C);
    }
    while (len > 0 && (copy.at(len - 1) == '\n' || copy.at(len - 1) == '\r')) {
        copy.at(--len) = '\0';
    }
    if (len == 0 || copy.at(0) == ' ' || copy.at(len - 1) == ' ' || std::strstr(copy.data(), "  ") != nullptr) {
        return false;
    }

    TokenCursor tokens(copy.data());
    char* verb = tokens.next();
    if (verb == nullptr) {
        return false;
    }
    if (std::strcmp(verb, "clear") == 0) {
        return tokens.next() == nullptr;
    }
    if (std::strcmp(verb, "block") == 0) {
        out->op = WorkloadOp::BLOCK;
        uint64_t blocks = 0;
        return parse_identity(tokens, out) && parse_key_decimal(tokens.next(), "lba", UINT64_MAX, &out->lba) &&
               parse_key_decimal(tokens.next(), "blocks", BLOCK_TRANSFER_MAX, &blocks) && blocks != 0 &&
               parse_key_decimal(tokens.next(), "pattern", UINT64_MAX, &out->pattern) && tokens.next() == nullptr &&
               ((out->blocks = static_cast<uint16_t>(blocks)), true);
    }
    if (std::strcmp(verb, "block-discovered") == 0 || std::strcmp(verb, "block-rdma-discovered") == 0) {
        out->op = std::strcmp(verb, "block-discovered") == 0 ? WorkloadOp::BLOCK_DISCOVERED : WorkloadOp::BLOCK_RDMA_DISCOVERED;
        uint64_t blocks = 0;
        return parse_owner(tokens, out) && parse_key_decimal(tokens.next(), "lba", UINT64_MAX, &out->lba) &&
               parse_key_decimal(tokens.next(), "blocks", BLOCK_TRANSFER_MAX, &blocks) && blocks != 0 &&
               parse_key_decimal(tokens.next(), "pattern", UINT64_MAX, &out->pattern) && tokens.next() == nullptr &&
               ((out->blocks = static_cast<uint16_t>(blocks)), true);
    }
    if (std::strcmp(verb, "net-attach") == 0) {
        out->op = WorkloadOp::NET_ATTACH;
        if (!parse_identity(tokens, out)) {
            return false;
        }
        char* name_token = tokens.next();
        constexpr char NAME_KEY[] = "name=";
        if (name_token == nullptr || std::strncmp(name_token, NAME_KEY, sizeof(NAME_KEY) - 1) != 0 ||
            !valid_net_name(name_token + sizeof(NAME_KEY) - 1) || tokens.next() != nullptr) {
            return false;
        }
        std::strncpy(out->name.data(), name_token + sizeof(NAME_KEY) - 1, out->name.size() - 1);
        return true;
    }
    if (std::strcmp(verb, "net-attach-discovered") == 0) {
        out->op = WorkloadOp::NET_ATTACH_DISCOVERED;
        if (!parse_owner(tokens, out)) {
            return false;
        }
        char* name_token = tokens.next();
        constexpr char NAME_KEY[] = "name=";
        if (name_token == nullptr || std::strncmp(name_token, NAME_KEY, sizeof(NAME_KEY) - 1) != 0 ||
            !valid_net_name(name_token + sizeof(NAME_KEY) - 1) || tokens.next() != nullptr) {
            return false;
        }
        std::strncpy(out->name.data(), name_token + sizeof(NAME_KEY) - 1, out->name.size() - 1);
        return true;
    }
    if (std::strcmp(verb, "net-query") == 0 || std::strcmp(verb, "net-detach") == 0) {
        out->op = std::strcmp(verb, "net-query") == 0 ? WorkloadOp::NET_QUERY : WorkloadOp::NET_DETACH;
        return parse_identity(tokens, out) && tokens.next() == nullptr;
    }
    if (std::strcmp(verb, "net-query-discovered") == 0 || std::strcmp(verb, "net-detach-discovered") == 0) {
        out->op = std::strcmp(verb, "net-query-discovered") == 0 ? WorkloadOp::NET_QUERY_DISCOVERED : WorkloadOp::NET_DETACH_DISCOVERED;
        return parse_owner(tokens, out) && tokens.next() == nullptr;
    }
    if (std::strcmp(verb, "vfs-unmount") == 0) {
        out->op = WorkloadOp::VFS_UNMOUNT;
        return parse_identity(tokens, out) && tokens.next() == nullptr;
    }
    if (std::strcmp(verb, "vfs-query-discovered") == 0 || std::strcmp(verb, "vfs-unmount-discovered") == 0) {
        out->op = std::strcmp(verb, "vfs-query-discovered") == 0 ? WorkloadOp::VFS_QUERY_DISCOVERED : WorkloadOp::VFS_UNMOUNT_DISCOVERED;
        if (!parse_owner(tokens, out)) {
            return false;
        }
        char* name_token = tokens.next();
        return name_token != nullptr && std::strcmp(name_token, "name=tmp") == 0 && tokens.next() == nullptr;
    }
    if (std::strcmp(verb, "compute-publish") == 0) {
        out->op = WorkloadOp::COMPUTE_PUBLISH_SET;
        uint64_t hold = 0;
        if (!parse_key_decimal(tokens.next(), "hold", 1, &hold) || tokens.next() != nullptr) {
            return false;
        }
        out->hold = hold != 0;
        return true;
    }
    if (std::strcmp(verb, "compute-publish-query") == 0) {
        out->op = WorkloadOp::COMPUTE_PUBLISH_QUERY;
        return tokens.next() == nullptr;
    }
    return false;
}

auto find_resource(const WorkloadCommand& command, ResourceType type, DiscoveredResource* out) -> int {
    return wki_resource_snapshot_exact(command.owner, type, command.resource, command.generation, out);
}

auto resource_still_exact(const WorkloadCommand& command, ResourceType type, const ResourceIncarnationToken& incarnation) -> bool {
    DiscoveredResource current{};
    return find_resource(command, type, &current) == 0 && wki_resource_incarnation_equal(current.owner_incarnation, incarnation);
}

auto resolve_discovered_command(const WorkloadCommand& requested, WorkloadCommand* resolved) -> int {
    if (resolved == nullptr) {
        return -EINVAL;
    }
    *resolved = requested;
    ResourceType type = ResourceType::CUSTOM;
    const char* exact_name = nullptr;
    switch (requested.op) {
        case WorkloadOp::BLOCK_DISCOVERED:
        case WorkloadOp::BLOCK_RDMA_DISCOVERED:
            type = ResourceType::BLOCK;
            exact_name = SCRATCH_RESOURCE_NAME;
            break;
        case WorkloadOp::NET_ATTACH_DISCOVERED:
        case WorkloadOp::NET_QUERY_DISCOVERED:
        case WorkloadOp::NET_DETACH_DISCOVERED:
            type = ResourceType::NET;
            break;
        case WorkloadOp::VFS_QUERY_DISCOVERED:
        case WorkloadOp::VFS_UNMOUNT_DISCOVERED:
            type = ResourceType::VFS;
            // Auto-discovery advertises the visible mount path, including its
            // leading slash. The command remains the fixed, path-free
            // `name=tmp` selector and never accepts an arbitrary VFS path.
            exact_name = VFS_ADVERTISED_NAME;
            break;
        default:
            return 0;
    }

    DiscoveredResource resource{};
    int const RESULT = wki_resource_resolve_unique(requested.owner, type, exact_name, &resource);
    if (RESULT != 0) {
        return RESULT;
    }
    if (requested.op == WorkloadOp::VFS_UNMOUNT_DISCOVERED &&
        !wki_remote_vfs_has_mount_for_resource_generation(resource.node_id, resource.resource_id, resource.generation)) {
        return -ENOENT;
    }
    resolved->resource = resource.resource_id;
    resolved->generation = resource.generation;
    return 0;
}

auto snapshot_block_proxy(const WorkloadCommand& command, WkiDevProxyDiagRow* out, bool active_only = true,
                          bool* snapshot_complete = nullptr) -> bool {
    if (out == nullptr) {
        return false;
    }
    size_t total = 0;
    size_t const COUNT = wki_dev_proxy_diag_snapshot(s_block_diag_rows.data(), s_block_diag_rows.size(), &total);
    if (snapshot_complete != nullptr) {
        *snapshot_complete = total <= s_block_diag_rows.size();
    }
    if (total > s_block_diag_rows.size()) {
        return false;
    }
    size_t matches = 0;
    for (size_t i = 0; i < COUNT; ++i) {
        auto const& row = s_block_diag_rows.at(i);
        if (row.owner_node == command.owner && row.resource_id == command.resource && row.resource_generation == command.generation &&
            (!active_only || row.active)) {
            *out = row;
            matches++;
        }
    }
    return matches == 1;
}

auto snapshot_net_proxy(const WorkloadCommand& command, WkiRemoteNetDiagRow* out, bool active_only = true,
                        bool* snapshot_complete = nullptr, size_t* match_count = nullptr) -> bool {
    if (out == nullptr) {
        return false;
    }
    size_t total = 0;
    size_t const COUNT = wki_remote_net_diag_snapshot(s_net_diag_rows.data(), s_net_diag_rows.size(), &total);
    if (snapshot_complete != nullptr) {
        *snapshot_complete = total <= s_net_diag_rows.size();
    }
    if (total > s_net_diag_rows.size()) {
        return false;
    }
    size_t matches = 0;
    for (size_t i = 0; i < COUNT; ++i) {
        auto const& row = s_net_diag_rows.at(i);
        if (row.owner_node == command.owner && row.resource_id == command.resource && row.resource_generation == command.generation &&
            (!active_only || row.active)) {
            *out = row;
            matches++;
        }
    }
    if (match_count != nullptr) {
        *match_count = matches;
    }
    return matches == 1;
}

auto snapshot_channel(uint16_t peer, uint16_t channel, uint32_t generation, WkiChannelDiag* out) -> bool {
    if (out == nullptr) {
        return false;
    }
    size_t const COUNT = wki_channel_diag_snapshot(s_channel_diag_rows.data(), s_channel_diag_rows.size());
    if (COUNT == s_channel_diag_rows.size()) {
        return false;
    }
    size_t matches = 0;
    for (size_t i = 0; i < COUNT; ++i) {
        auto const& row = s_channel_diag_rows.at(i);
        if (row.active && row.peer_node == peer && row.channel_id == channel && row.generation == generation) {
            *out = row;
            matches++;
        }
    }
    return matches == 1;
}

auto vfs_row_matches_identity(const WorkloadCommand& command, const WkiRemoteVfsProxyDiag& row) -> bool {
    return row.owner_node == command.owner && row.resource_id == command.resource && row.resource_generation == command.generation;
}

auto vfs_row_incarnation_exact(const DiscoveredResource& resource, const WkiRemoteVfsProxyDiag& row) -> bool {
    ResourceIncarnationToken const ROW_INCARNATION{
        .owner_boot_epoch = row.owner_boot_epoch,
        .resource_incarnation = row.resource_incarnation,
    };
    return wki_resource_incarnation_equal(resource.owner_incarnation, ROW_INCARNATION);
}

auto snapshot_vfs_binding(const WorkloadCommand& command, const DiscoveredResource& resource, VfsBindingSnapshot* out) -> bool {
    if (out == nullptr) {
        return false;
    }

    size_t const COUNT = wki_remote_vfs_proxy_diag_snapshot(s_vfs_diag_rows.data(), s_vfs_diag_rows.size());
    if (COUNT == s_vfs_diag_rows.size()) {
        // The diagnostic API cannot distinguish an exactly-full snapshot from
        // truncation. Fail closed rather than selecting a partially observed
        // mount group.
        return false;
    }

    WkiRemoteVfsProxyDiag anchor{};
    size_t anchor_matches = 0;
    for (size_t i = 0; i < COUNT; ++i) {
        auto const& row = s_vfs_diag_rows.at(i);
        if (vfs_row_matches_identity(command, row) && row.lane_anchor && row.active && row.mount_configured && row.lanes_ready &&
            !row.epoch_reset_pending && !row.attach_pending && !row.destroy_when_idle && !row.mount_released && row.mount_group_id != 0 &&
            row.lane_count > 0 && row.lane_count <= VFS_PROXY_LANE_COUNT && vfs_row_incarnation_exact(resource, row)) {
            anchor = row;
            anchor_matches++;
        }
    }
    if (anchor_matches != 1) {
        return false;
    }

    VfsBindingSnapshot snapshot{};
    snapshot.anchor = anchor;
    snapshot.lane_count = anchor.lane_count;
    size_t member_count = 0;
    for (size_t i = 0; i < COUNT; ++i) {
        auto const& row = s_vfs_diag_rows.at(i);
        if (!vfs_row_matches_identity(command, row)) {
            continue;
        }
        if (row.mount_group_id != anchor.mount_group_id || row.lane_index >= snapshot.lane_count ||
            snapshot.lanes.at(row.lane_index).valid || !row.active || row.epoch_reset_pending || row.attach_pending ||
            row.destroy_when_idle || row.mount_released || row.binding_attach_cookie == 0 || row.assigned_channel == 0 ||
            row.assigned_channel_generation == 0 || !vfs_row_incarnation_exact(resource, row)) {
            return false;
        }
        WkiChannelDiag channel{};
        if (!snapshot_channel(command.owner, row.assigned_channel, row.assigned_channel_generation, &channel)) {
            return false;
        }
        snapshot.lanes.at(row.lane_index) = VfsLaneTuple{
            .lane_index = row.lane_index,
            .channel = row.assigned_channel,
            .channel_generation = row.assigned_channel_generation,
            .attach_cookie = row.binding_attach_cookie,
            .valid = true,
        };
        member_count++;
    }
    if (member_count != snapshot.lane_count) {
        return false;
    }
    for (size_t i = 0; i < snapshot.lane_count; ++i) {
        if (!snapshot.lanes.at(i).valid) {
            return false;
        }
    }
    *out = snapshot;
    return true;
}

auto vfs_detach_pending(const WorkloadCommand& command, uint32_t* peer_boot_epoch_out) -> bool {
    size_t const COUNT = wki_remote_vfs_proxy_diag_snapshot(s_vfs_diag_rows.data(), s_vfs_diag_rows.size());
    if (COUNT == s_vfs_diag_rows.size()) {
        return true;
    }
    bool pending = false;
    uint32_t peer_boot_epoch = 0;
    for (size_t i = 0; i < COUNT; ++i) {
        auto const& row = s_vfs_diag_rows.at(i);
        if (!vfs_row_matches_identity(command, row) || (!row.detach_pending && !row.detach_retry_in_progress)) {
            continue;
        }
        pending = true;
        if (peer_boot_epoch == 0) {
            peer_boot_epoch = row.detach_peer_boot_epoch;
        } else if (peer_boot_epoch != row.detach_peer_boot_epoch) {
            return true;
        }
    }
    if (peer_boot_epoch_out != nullptr) {
        *peer_boot_epoch_out = peer_boot_epoch;
    }
    return pending;
}

auto block_binding_exact(const WorkloadCommand& command, const DiscoveredResource& resource, const WkiDevProxyDiagRow& proxy,
                         WkiChannelDiag* channel) -> bool {
    return proxy.lifecycle_detail_complete && proxy.io_detail_complete && proxy.active && !proxy.fenced && !proxy.attach_pending &&
           !proxy.cleanup_in_progress && proxy.binding_attach_cookie != 0 && proxy.assigned_channel != 0 &&
           wki_resource_incarnation_equal(resource.owner_incarnation, proxy.binding_incarnation) &&
           snapshot_channel(command.owner, proxy.assigned_channel, proxy.channel_generation, channel);
}

auto net_binding_exact(const WorkloadCommand& command, const DiscoveredResource& resource, const WkiRemoteNetDiagRow& proxy,
                       WkiChannelDiag* channel) -> bool {
    uint32_t const CURRENT_PEER_BOOT_EPOCH = wki_peer_remote_boot_epoch_snapshot(command.owner);
    bool const PEER_EPOCH_EXACT = proxy.binding_peer_boot_epoch != 0 && proxy.binding_peer_boot_epoch == CURRENT_PEER_BOOT_EPOCH &&
                                  (!wki_resource_incarnation_valid(resource.owner_incarnation) ||
                                   proxy.binding_peer_boot_epoch == resource.owner_incarnation.owner_boot_epoch);
    return proxy.detail_complete && proxy.active && proxy.netdev_registered && !proxy.attaching && !proxy.cleanup_started &&
           !proxy.retiring && !proxy.attach_pending && proxy.attach_cookie != 0 && proxy.assigned_channel != 0 && PEER_EPOCH_EXACT &&
           snapshot_channel(command.owner, proxy.assigned_channel, proxy.channel_generation, channel);
}

constexpr auto net_evidence_identity(const ResourceIncarnationToken& owner_incarnation, uint32_t binding_peer_boot_epoch)
    -> ResourceIncarnationToken {
    auto identity = owner_incarnation;
    // NET has no wire resource-incarnation suffix. Its exact evidence tuple is
    // the proxy's peer boot epoch plus the separately reported local resource
    // observation generation, channel generation, and attach cookie.
    identity.owner_boot_epoch = binding_peer_boot_epoch;
    return identity;
}

void fill_pattern(std::array<uint8_t, BLOCK_TRANSFER_BYTES_MAX>* buffer, size_t bytes, uint64_t seed) {
    for (size_t i = 0; i < bytes; ++i) {
        uint8_t const SEED_BYTE = static_cast<uint8_t>((seed >> ((i % sizeof(seed)) * 8U)) & 0xFFU);
        buffer->at(i) = static_cast<uint8_t>(SEED_BYTE ^ static_cast<uint8_t>((i * 131U) + 17U));
    }
}

auto first_error(std::initializer_list<int> statuses) -> int {
    for (int status : statuses) {
        if (status != 0 && status != -ENODATA) {
            return status < 0 ? status : -status;
        }
    }
    return 0;
}

auto run_block(const WorkloadCommand& command) -> int {  // NOLINT(readability-function-size)
    s_result = {};
    s_result.op = command.op;
    s_result.owner = command.owner;
    s_result.resource = command.resource;
    s_result.generation = command.generation;
    s_result.lba = command.lba;
    s_result.blocks = command.blocks;

    DiscoveredResource resource{};
    if (find_resource(command, ResourceType::BLOCK, &resource) != 0 || std::strcmp(resource.name, SCRATCH_RESOURCE_NAME) != 0 ||
        (resource.flags & (RESOURCE_FLAG_READABLE | RESOURCE_FLAG_WRITABLE)) != (RESOURCE_FLAG_READABLE | RESOURCE_FLAG_WRITABLE)) {
        return -EPERM;
    }
    s_result.owner_incarnation = resource.owner_incarnation;
    if (!resource_still_exact(command, ResourceType::BLOCK, resource.owner_incarnation)) {
        return -ESTALE;
    }

    WkiDevProxyDiagRow existing{};
    bool block_snapshot_complete = false;
    if (snapshot_block_proxy(command, &existing, true, &block_snapshot_complete)) {
        return -EBUSY;
    }
    if (!block_snapshot_complete) {
        return -EOVERFLOW;
    }

    bool const REQUIRE_RDMA = command.op == WorkloadOp::BLOCK_RDMA_DISCOVERED;
    WkiBlockAttachRdmaPolicy const RDMA_POLICY = REQUIRE_RDMA ? WkiBlockAttachRdmaPolicy::ALLOW : WkiBlockAttachRdmaPolicy::DISABLE;
    dev::BlockDevice* device = wki_dev_proxy_attach_block(command.owner, command.resource, command.generation, resource.owner_incarnation,
                                                          BLOCK_PROXY_NAME, RDMA_POLICY);
    if (device == nullptr) {
        return -EIO;
    }

    WkiDevProxyDiagRow proxy{};
    WkiChannelDiag channel_before{};
    block_snapshot_complete = false;
    bool const EXACT = snapshot_block_proxy(command, &proxy, true, &block_snapshot_complete) && block_snapshot_complete &&
                       block_binding_exact(command, resource, proxy, &channel_before) && !dev::block_device_is_read_only(device);
    s_result.channel = proxy.assigned_channel;
    s_result.channel_generation = proxy.channel_generation;
    s_result.attach_cookie = proxy.binding_attach_cookie;
    s_result.rdma = proxy.rdma_attached;
    s_result.rdma_roce = proxy.rdma_roce;
    s_result.rdma_zone = proxy.rdma_zone_id;
    s_result.data_slots_before = proxy.data_slot_bitmap;
    s_result.tags_before = proxy.tag_bitmap;
    s_result.ring_geometry = proxy.ring_geometry;
    s_result.ring_before = proxy.ring_indices;
    s_result.ring_geometry_valid = proxy.ring_geometry_valid;
    s_result.ring_indices_valid = proxy.ring_indices_valid;
    s_result.exact_binding = EXACT;
    bool const RING_BUSY =
        REQUIRE_RDMA && (proxy.data_slot_bitmap != 0 || proxy.tag_bitmap != 0 || proxy.ring_indices.sq_head != proxy.ring_indices.sq_tail ||
                         proxy.ring_indices.cq_head != proxy.ring_indices.cq_tail);
    if (!EXACT || proxy.rdma_attached != REQUIRE_RDMA ||
        (REQUIRE_RDMA && (!proxy.ring_geometry_valid || !proxy.ring_indices_valid || proxy.rdma_zone_id == 0)) || RING_BUSY) {
        wki_dev_proxy_detach_block(device);
        s_result.detached = true;
        if (!EXACT) {
            return -ESTALE;
        }
        if (RING_BUSY) {
            return -EBUSY;
        }
        return -EOPNOTSUPP;
    }

    if (device->block_size != SCRATCH_BLOCK_SIZE || device->total_blocks != SCRATCH_TOTAL_BLOCKS || command.lba > device->total_blocks ||
        command.blocks > device->total_blocks - command.lba || device->block_size > BLOCK_TRANSFER_BYTES_MAX ||
        command.blocks > BLOCK_TRANSFER_BYTES_MAX / device->block_size) {
        wki_dev_proxy_detach_block(device);
        s_result.detached = true;
        return -ERANGE;
    }

    size_t const BYTES = static_cast<size_t>(command.blocks) * device->block_size;
    s_result.block_size = static_cast<uint32_t>(device->block_size);
    s_result.bytes = static_cast<uint32_t>(BYTES);
    // These are reliable-channel sequence counters, not BlkSqEntry/CQE tags.
    // RDMA tag fencing is proved by the dedicated old-epoch-tag KTEST; this
    // live row reports only binding epochs plus bounded ring occupancy.
    s_result.channel_tx_seq_before = channel_before.tx_seq;

    std::array<uint8_t, BLOCK_TRANSFER_BYTES_MAX> baseline{};
    std::array<uint8_t, BLOCK_TRANSFER_BYTES_MAX> pattern{};
    std::array<uint8_t, BLOCK_TRANSFER_BYTES_MAX> observed{};
    fill_pattern(&pattern, BYTES, command.pattern);

    s_result.read_status = dev::block_read(device, command.lba, command.blocks, baseline.data());
    if (s_result.read_status == 0) {
        s_result.write_status = dev::block_write(device, command.lba, command.blocks, pattern.data());
        if (s_result.write_status == 0) {
            s_result.flush_status = dev::block_flush(device);
            if (s_result.flush_status == 0) {
                s_result.verify_status = dev::block_read(device, command.lba, command.blocks, observed.data());
                s_result.verified = s_result.verify_status == 0 && std::equal(pattern.begin(), pattern.begin() + BYTES, observed.begin());
                if (s_result.verify_status == 0 && !s_result.verified) {
                    s_result.verify_status = -EBADMSG;
                }
            }
        } else {
            // A rejected corrupted request must leave the scratch range at its
            // baseline. A one-shot fault rule is exhausted before this read.
            s_result.verify_status = dev::block_read(device, command.lba, command.blocks, observed.data());
            s_result.verified = s_result.verify_status == 0 && std::equal(baseline.begin(), baseline.begin() + BYTES, observed.begin());
            if (s_result.verify_status == 0 && !s_result.verified) {
                s_result.verify_status = -EBADMSG;
            }
        }

        // The bridge never leaves test data behind, including after an
        // injected failure. Restoration uses only the captured scratch range.
        int const RESTORE_WRITE = dev::block_write(device, command.lba, command.blocks, baseline.data());
        int const RESTORE_FLUSH = RESTORE_WRITE == 0 ? dev::block_flush(device) : RESTORE_WRITE;
        s_result.restore_status = RESTORE_WRITE != 0 ? RESTORE_WRITE : RESTORE_FLUSH;
        s_result.restored = s_result.restore_status == 0;
    }

    WkiChannelDiag channel_after{};
    if (snapshot_channel(command.owner, proxy.assigned_channel, proxy.channel_generation, &channel_after)) {
        s_result.channel_tx_seq_after = channel_after.tx_seq;
        s_result.channel_tx_delta = channel_after.tx_seq - channel_before.tx_seq;
    } else {
        s_result.exact_binding = false;
    }

    WkiDevProxyDiagRow post_io{};
    block_snapshot_complete = false;
    if (snapshot_block_proxy(command, &post_io, true, &block_snapshot_complete) && block_snapshot_complete &&
        post_io.lifecycle_detail_complete && post_io.io_detail_complete && post_io.active && !post_io.op_pending) {
        s_result.data_slots_after = post_io.data_slot_bitmap;
        s_result.tags_after = post_io.tag_bitmap;
        s_result.ring_after = post_io.ring_indices;
        s_result.ring_geometry_valid = s_result.ring_geometry_valid && post_io.ring_geometry_valid;
        s_result.ring_indices_valid = s_result.ring_indices_valid && post_io.ring_indices_valid;
        s_result.ring_quiescent = post_io.data_slot_bitmap == 0 && post_io.tag_bitmap == 0 &&
                                  post_io.ring_indices.sq_head == post_io.ring_indices.sq_tail &&
                                  post_io.ring_indices.cq_head == post_io.ring_indices.cq_tail;
    } else if (REQUIRE_RDMA) {
        s_result.exact_binding = false;
    }

    wki_dev_proxy_detach_block(device);
    WkiDevProxyDiagRow remaining{};
    block_snapshot_complete = false;
    s_result.detached = !snapshot_block_proxy(command, &remaining, true, &block_snapshot_complete) && block_snapshot_complete;

    bool const SAFE_WRITE_REJECTION = s_result.read_status == 0 && s_result.write_status != 0 && s_result.verify_status == 0 &&
                                      s_result.verified && s_result.restore_status == 0 && s_result.restored;
    int result = SAFE_WRITE_REJECTION ? 0
                                      : first_error({s_result.read_status, s_result.write_status, s_result.flush_status,
                                                     s_result.verify_status, s_result.restore_status});
    if (result == 0 && (!s_result.exact_binding || !s_result.verified || !s_result.restored || !s_result.detached ||
                        (REQUIRE_RDMA && !s_result.ring_quiescent))) {
        result = -EIO;
    }
    return result;
}

auto run_net_attach(const WorkloadCommand& command) -> int {
    s_result = {};
    s_result.op = command.op;
    s_result.owner = command.owner;
    s_result.resource = command.resource;
    s_result.generation = command.generation;
    s_result.name = command.name;
    if (s_managed_net.owned) {
        return -EBUSY;
    }
    DiscoveredResource resource{};
    if (find_resource(command, ResourceType::NET, &resource) != 0) {
        return -ENOENT;
    }
    s_result.owner_incarnation = resource.owner_incarnation;
    if (!resource_still_exact(command, ResourceType::NET, resource.owner_incarnation)) {
        return -ESTALE;
    }
    WkiRemoteNetDiagRow existing{};
    bool net_snapshot_complete = false;
    if (snapshot_net_proxy(command, &existing, true, &net_snapshot_complete)) {
        return -EEXIST;
    }
    if (!net_snapshot_complete) {
        return -EOVERFLOW;
    }

    net::NetDevice* device = wki_remote_net_attach(command.owner, command.resource, command.name.data(), command.generation);
    if (device == nullptr) {
        return -EIO;
    }
    if (std::strcmp(device->name.data(), command.name.data()) != 0) {
        wki_remote_net_detach(device);
        return -EIO;
    }

    WkiRemoteNetDiagRow proxy{};
    WkiChannelDiag channel{};
    net_snapshot_complete = false;
    if (!snapshot_net_proxy(command, &proxy, true, &net_snapshot_complete) || !net_snapshot_complete ||
        !net_binding_exact(command, resource, proxy, &channel)) {
        wki_remote_net_detach(device);
        return -ESTALE;
    }

    s_managed_net = {
        .owned = true,
        .owner = command.owner,
        .resource = command.resource,
        .generation = command.generation,
        .owner_incarnation = resource.owner_incarnation,
        .binding_peer_boot_epoch = proxy.binding_peer_boot_epoch,
        .channel = proxy.assigned_channel,
        .channel_generation = proxy.channel_generation,
        .attach_cookie = proxy.attach_cookie,
        .ifindex = device->ifindex,
        .name = command.name,
        .device = device,
    };
    s_result.channel = proxy.assigned_channel;
    s_result.channel_generation = proxy.channel_generation;
    s_result.attach_cookie = proxy.attach_cookie;
    s_result.ifindex = device->ifindex;
    s_result.owner_incarnation = net_evidence_identity(resource.owner_incarnation, proxy.binding_peer_boot_epoch);
    s_result.exact_binding = true;
    return 0;
}

auto managed_net_identity_matches(const WorkloadCommand& command) -> bool {
    return s_managed_net.owned && s_managed_net.owner == command.owner && s_managed_net.resource == command.resource &&
           s_managed_net.generation == command.generation;
}

auto managed_net_matches(const WorkloadCommand& command) -> bool {
    return managed_net_identity_matches(command) && !s_managed_net.detach_issued && s_managed_net.device != nullptr;
}

void copy_managed_net_result() {
    s_result.owner_incarnation = net_evidence_identity(s_managed_net.owner_incarnation, s_managed_net.binding_peer_boot_epoch);
    s_result.channel = s_managed_net.channel;
    s_result.channel_generation = s_managed_net.channel_generation;
    s_result.attach_cookie = s_managed_net.attach_cookie;
    s_result.ifindex = s_managed_net.ifindex;
    s_result.name = s_managed_net.name;
}

constexpr auto managed_net_detach_converged(bool snapshot_complete, size_t active_matches) -> bool {
    return snapshot_complete && active_matches == 0;
}

auto poll_managed_net_detach(const WorkloadCommand& command) -> int {
    WkiRemoteNetDiagRow remaining{};
    bool snapshot_complete = false;
    size_t active_matches = 0;
    static_cast<void>(snapshot_net_proxy(command, &remaining, true, &snapshot_complete, &active_matches));
    if (!snapshot_complete) {
        return -EOVERFLOW;
    }
    s_result.detached = managed_net_detach_converged(snapshot_complete, active_matches);
    if (s_result.detached) {
        // Retain this exact cleanup receipt across incomplete or busy
        // observations, and erase it only after convergence is proven.
        s_managed_net = {};
    }
    return s_result.detached ? 0 : -EBUSY;
}

auto clear_managed_net_attachment() -> int {
    s_result = {};
    s_result.op = WorkloadOp::NONE;
    s_result_vfs_lanes = {};
    if (!s_managed_net.owned) {
        s_result.detached = true;
        return 0;
    }

    WorkloadCommand const command{
        .op = WorkloadOp::NET_DETACH,
        .owner = s_managed_net.owner,
        .resource = s_managed_net.resource,
        .generation = s_managed_net.generation,
    };
    s_result.owner = s_managed_net.owner;
    s_result.resource = s_managed_net.resource;
    s_result.generation = s_managed_net.generation;
    copy_managed_net_result();

    bool snapshot_complete = false;
    size_t active_matches = 0;
    if (!s_managed_net.detach_issued) {
        WkiRemoteNetDiagRow before{};
        bool const UNIQUE_ACTIVE = snapshot_net_proxy(command, &before, true, &snapshot_complete, &active_matches) && snapshot_complete;
        if (!snapshot_complete) {
            return -EOVERFLOW;
        }
        s_managed_net.detach_binding_exact =
            UNIQUE_ACTIVE && before.detail_complete && before.netdev_registered && !before.attaching && !before.cleanup_started &&
            !before.retiring && !before.attach_pending && before.assigned_channel == s_managed_net.channel &&
            before.channel_generation == s_managed_net.channel_generation && before.attach_cookie == s_managed_net.attach_cookie &&
            before.binding_peer_boot_epoch == s_managed_net.binding_peer_boot_epoch && before.binding_peer_boot_epoch != 0;

        // The stored NetDevice pointer is the exact object created by this
        // adapter. Publish the detached phase before invoking teardown so a
        // retry never dereferences or detaches the retired object twice.
        net::NetDevice* const DEVICE = s_managed_net.device;
        if (DEVICE == nullptr) {
            return -EINVAL;
        }
        s_managed_net.detach_issued = true;
        s_managed_net.device = nullptr;
        wki_remote_net_detach(DEVICE);
    }
    s_result.exact_binding = s_managed_net.detach_binding_exact;

    return poll_managed_net_detach(command);
}

auto run_net_query(const WorkloadCommand& command) -> int {
    s_result = {};
    s_result.op = command.op;
    s_result.owner = command.owner;
    s_result.resource = command.resource;
    s_result.generation = command.generation;
    if (!managed_net_matches(command)) {
        return -ENOENT;
    }
    copy_managed_net_result();
    DiscoveredResource resource{};
    if (find_resource(command, ResourceType::NET, &resource) != 0 ||
        !wki_resource_incarnation_equal(resource.owner_incarnation, s_managed_net.owner_incarnation) ||
        !resource_still_exact(command, ResourceType::NET, s_managed_net.owner_incarnation)) {
        return -ESTALE;
    }
    WkiRemoteNetDiagRow proxy{};
    WkiChannelDiag channel{};
    bool net_snapshot_complete = false;
    s_result.exact_binding = snapshot_net_proxy(command, &proxy, true, &net_snapshot_complete) && net_snapshot_complete &&
                             net_binding_exact(command, resource, proxy, &channel) && proxy.assigned_channel == s_managed_net.channel &&
                             proxy.channel_generation == s_managed_net.channel_generation &&
                             proxy.attach_cookie == s_managed_net.attach_cookie &&
                             proxy.binding_peer_boot_epoch == s_managed_net.binding_peer_boot_epoch &&
                             resource_still_exact(command, ResourceType::NET, s_managed_net.owner_incarnation);
    return s_result.exact_binding ? 0 : -ESTALE;
}

auto run_net_detach(const WorkloadCommand& command) -> int {
    s_result = {};
    s_result.op = command.op;
    s_result.owner = command.owner;
    s_result.resource = command.resource;
    s_result.generation = command.generation;
    if (!managed_net_identity_matches(command)) {
        return -ENOENT;
    }
    copy_managed_net_result();
    if (s_managed_net.detach_issued) {
        s_result.exact_binding = s_managed_net.detach_binding_exact;
        return poll_managed_net_detach(command);
    }
    if (s_managed_net.device == nullptr) {
        return -EINVAL;
    }

    DiscoveredResource resource{};
    WkiRemoteNetDiagRow proxy{};
    WkiChannelDiag channel{};
    bool net_snapshot_complete = false;
    if (find_resource(command, ResourceType::NET, &resource) != 0 ||
        !wki_resource_incarnation_equal(resource.owner_incarnation, s_managed_net.owner_incarnation) ||
        !snapshot_net_proxy(command, &proxy, true, &net_snapshot_complete) || !net_snapshot_complete ||
        !net_binding_exact(command, resource, proxy, &channel) || proxy.assigned_channel != s_managed_net.channel ||
        proxy.channel_generation != s_managed_net.channel_generation || proxy.attach_cookie != s_managed_net.attach_cookie ||
        proxy.binding_peer_boot_epoch != s_managed_net.binding_peer_boot_epoch ||
        !resource_still_exact(command, ResourceType::NET, s_managed_net.owner_incarnation)) {
        return -ESTALE;
    }
    s_result.exact_binding = true;

    net::NetDevice* const DEVICE = s_managed_net.device;
    s_managed_net.detach_issued = true;
    s_managed_net.detach_binding_exact = true;
    s_managed_net.device = nullptr;
    wki_remote_net_detach(DEVICE);
    return poll_managed_net_detach(command);
}

auto capture_vfs_binding(const WorkloadCommand& command) -> int {
    s_result = {};
    s_result_vfs_lanes = {};
    s_result.op = command.op;
    s_result.owner = command.owner;
    s_result.resource = command.resource;
    s_result.generation = command.generation;

    DiscoveredResource resource{};
    if (find_resource(command, ResourceType::VFS, &resource) != 0) {
        return -ENOENT;
    }
    s_result.owner_incarnation = resource.owner_incarnation;

    VfsBindingSnapshot binding{};
    if (!snapshot_vfs_binding(command, resource, &binding)) {
        return -ESTALE;
    }
    s_result.lane_index = binding.anchor.lane_index;
    s_result.lane_count = binding.lane_count;
    s_result.channel = binding.anchor.assigned_channel;
    s_result.channel_generation = binding.anchor.assigned_channel_generation;
    s_result.attach_cookie = binding.anchor.binding_attach_cookie;
    s_result_vfs_lanes = binding.lanes;
    s_result.exact_binding = true;

    // Close the observation-to-use race. A later unmount resolves and checks
    // the identity again; no caller-controlled path reaches production VFS.
    if (!resource_still_exact(command, ResourceType::VFS, resource.owner_incarnation)) {
        s_result.exact_binding = false;
        return -ESTALE;
    }

    return 0;
}

auto run_vfs_query(const WorkloadCommand& command) -> int { return capture_vfs_binding(command); }

auto run_vfs_unmount(const WorkloadCommand& command) -> int {
    int const RESULT = capture_vfs_binding(command);
    if (RESULT != 0) {
        return RESULT;
    }

    wki_remote_vfs_unmount_resource_generation(command.owner, command.resource, command.generation);
    s_result.detached = !wki_remote_vfs_has_mount_for_resource_generation(command.owner, command.resource, command.generation);
    s_result.detach_pending = vfs_detach_pending(command, &s_result.detach_peer_boot_epoch);
    s_result.detach_status = s_result.detach_pending ? -EINPROGRESS : 0;
    return s_result.detached ? 0 : -EBUSY;
}

auto run_compute_publish(const WorkloadCommand& command) -> int {
    s_result = {};
    s_result.op = command.op;
    if (command.op == WorkloadOp::COMPUTE_PUBLISH_SET) {
        if (command.hold) {
            s_compute_publish_hold.store(true, std::memory_order_release);
        } else {
            release_compute_publish_hold();
        }
    }
    copy_compute_state_to_result();
    return 0;
}

auto op_name(WorkloadOp op) -> const char* {
    switch (op) {
        case WorkloadOp::NONE:
            return "none";
        case WorkloadOp::BLOCK:
            return "block";
        case WorkloadOp::BLOCK_DISCOVERED:
            return "block-discovered";
        case WorkloadOp::BLOCK_RDMA_DISCOVERED:
            return "block-rdma-discovered";
        case WorkloadOp::NET_ATTACH:
            return "net-attach";
        case WorkloadOp::NET_ATTACH_DISCOVERED:
            return "net-attach-discovered";
        case WorkloadOp::NET_QUERY:
            return "net-query";
        case WorkloadOp::NET_QUERY_DISCOVERED:
            return "net-query-discovered";
        case WorkloadOp::NET_DETACH:
            return "net-detach";
        case WorkloadOp::NET_DETACH_DISCOVERED:
            return "net-detach-discovered";
        case WorkloadOp::VFS_UNMOUNT:
            return "vfs-unmount";
        case WorkloadOp::VFS_QUERY_DISCOVERED:
            return "vfs-query-discovered";
        case WorkloadOp::VFS_UNMOUNT_DISCOVERED:
            return "vfs-unmount-discovered";
        case WorkloadOp::COMPUTE_PUBLISH_SET:
            return "compute-publish";
        case WorkloadOp::COMPUTE_PUBLISH_QUERY:
            return "compute-publish-query";
    }
    return "none";
}

}  // namespace

void wki_chaos_workload_init(bool workload_boot_allowed) {
    mod::sys::MutexGuard const GUARD(s_workload_lock);
    bool const ENABLED = workload_boot_allowed && runtime_control_allowed();
    s_workload_boot_allowed.store(ENABLED, std::memory_order_release);
    s_compute_publish_hold.store(false, std::memory_order_release);
    s_compute_publish_waiters.store(0, std::memory_order_release);
    s_compute_publish_wait_count.store(0, std::memory_order_release);
    s_compute_publish_release_generation.store(0, std::memory_order_release);
    s_compute_publish_last_wait_status.store(-ENODATA, std::memory_order_release);
    for (auto& waiter : s_compute_publish_waiter_tasks) {
        waiter.store(nullptr, std::memory_order_release);
    }
    s_result = {};
    s_result_vfs_lanes = {};
    s_managed_net = {};
    if (!ENABLED || s_scratch_registered.load(std::memory_order_acquire)) {
        return;
    }

    std::fill(s_scratch_storage.begin(), s_scratch_storage.end(), uint8_t{0});
    s_scratch_device = {};
    s_scratch_device.major = 240;
    s_scratch_device.minor = 0;
    std::copy_n(SCRATCH_RESOURCE_NAME, sizeof(SCRATCH_RESOURCE_NAME), s_scratch_device.name.begin());
    s_scratch_device.block_size = SCRATCH_BLOCK_SIZE;
    s_scratch_device.total_blocks = SCRATCH_TOTAL_BLOCKS;
    s_scratch_device.read_blocks = scratch_read;
    s_scratch_device.write_blocks = scratch_write;
    s_scratch_device.flush = scratch_flush;
    s_scratch_device.remotable = &SCRATCH_REMOTABLE_OPS;
    if (dev::block_device_register(&s_scratch_device) == 0) {
        s_scratch_registered.store(true, std::memory_order_release);
    } else {
        s_workload_boot_allowed.store(false, std::memory_order_release);
    }
}

void wki_chaos_workload_shutdown() {
    mod::sys::MutexGuard const GUARD(s_workload_lock);
    release_compute_publish_hold();
    if (s_managed_net.owned && s_managed_net.device != nullptr) {
        wki_remote_net_detach(s_managed_net.device);
    }
    s_managed_net = {};
    s_workload_boot_allowed.store(false, std::memory_order_release);
    if (s_scratch_registered.exchange(false, std::memory_order_acq_rel)) {
        static_cast<void>(dev::block_device_unregister(&s_scratch_device));
    }
    s_result = {};
    s_result_vfs_lanes = {};
}

auto wki_chaos_workload_allowed() -> bool { return gates_allowed() && s_scratch_registered.load(std::memory_order_acquire); }

auto wki_chaos_workload_compute_publish_wait(uint64_t absolute_deadline_us) -> int {
    if (!s_workload_boot_allowed.load(std::memory_order_acquire) || !s_compute_publish_hold.load(std::memory_order_acquire)) {
        return 0;
    }
    if (absolute_deadline_us == 0) {
        return -EINVAL;
    }

    auto* const TASK = mod::sched::get_current_task();
    if (TASK == nullptr || TASK->type != mod::sched::task::TaskType::DAEMON) {
        return -EPERM;
    }

    std::atomic<mod::sched::task::Task*>* waiter_slot = nullptr;
    for (auto& candidate : s_compute_publish_waiter_tasks) {
        auto* expected = static_cast<mod::sched::task::Task*>(nullptr);
        if (candidate.compare_exchange_strong(expected, TASK, std::memory_order_acq_rel, std::memory_order_acquire)) {
            waiter_slot = &candidate;
            break;
        }
    }
    if (waiter_slot == nullptr) {
        return -EOVERFLOW;
    }

    s_compute_publish_waiters.fetch_add(1, std::memory_order_acq_rel);
    s_compute_publish_wait_count.fetch_add(1, std::memory_order_acq_rel);
    uint64_t const RELEASE_GENERATION = s_compute_publish_release_generation.load(std::memory_order_acquire);
    int status = 0;
    while (true) {
        if (!s_workload_boot_allowed.load(std::memory_order_acquire)) {
            status = -ECANCELED;
            break;
        }
        if (!s_compute_publish_hold.load(std::memory_order_acquire) ||
            s_compute_publish_release_generation.load(std::memory_order_acquire) != RELEASE_GENERATION) {
            break;
        }
        uint64_t const NOW_US = wki_now_us();
        if (NOW_US >= absolute_deadline_us) {
            status = -ETIMEDOUT;
            break;
        }
        // This is a daemon/task-context-only hook. The fixed sleep cap bounds
        // release observation even if an event wake is unavailable; hold=0
        // additionally sends an event wake to every published waiter slot.
        mod::sched::kern_sleep_us(std::min(absolute_deadline_us - NOW_US, COMPUTE_PUBLISH_POLL_MAX_US));
    }

    auto* expected = TASK;
    static_cast<void>(waiter_slot->compare_exchange_strong(expected, nullptr, std::memory_order_acq_rel, std::memory_order_acquire));
    s_compute_publish_waiters.fetch_sub(1, std::memory_order_acq_rel);
    s_compute_publish_last_wait_status.store(status, std::memory_order_release);
    return status;
}

auto wki_chaos_workload_configure(const char* command, size_t len) -> int {
    WorkloadCommand parsed{};
    if (!parse_command(command, len, &parsed)) {
        mod::sys::MutexGuard const GUARD(s_workload_lock);
        s_result = {};
        s_result_vfs_lanes = {};
        s_result.status = -EINVAL;
        return -EINVAL;
    }

    mod::sys::MutexGuard const GUARD(s_workload_lock);
    if (parsed.op == WorkloadOp::NONE) {
        release_compute_publish_hold();
        int const RESULT = clear_managed_net_attachment();
        s_result.status = RESULT;
        return RESULT;
    }
    if (!wki_chaos_workload_allowed()) {
        s_result = {};
        s_result_vfs_lanes = {};
        s_result.op = parsed.op;
        s_result.status = -EPERM;
        return -EPERM;
    }

    WorkloadCommand execution = parsed;
    int const RESOLVE_RESULT = resolve_discovered_command(parsed, &execution);
    if (RESOLVE_RESULT != 0) {
        s_result = {};
        s_result_vfs_lanes = {};
        s_result.op = parsed.op;
        s_result.owner = parsed.owner;
        s_result.status = RESOLVE_RESULT;
        return RESOLVE_RESULT;
    }

    s_result_vfs_lanes = {};
    int result = -EINVAL;
    switch (execution.op) {
        case WorkloadOp::NONE:
            result = 0;
            break;
        case WorkloadOp::BLOCK:
        case WorkloadOp::BLOCK_DISCOVERED:
        case WorkloadOp::BLOCK_RDMA_DISCOVERED:
            result = run_block(execution);
            break;
        case WorkloadOp::NET_ATTACH:
        case WorkloadOp::NET_ATTACH_DISCOVERED:
            result = run_net_attach(execution);
            break;
        case WorkloadOp::NET_QUERY:
        case WorkloadOp::NET_QUERY_DISCOVERED:
            result = run_net_query(execution);
            break;
        case WorkloadOp::NET_DETACH:
        case WorkloadOp::NET_DETACH_DISCOVERED:
            result = run_net_detach(execution);
            break;
        case WorkloadOp::VFS_UNMOUNT:
            result = run_vfs_unmount(execution);
            break;
        case WorkloadOp::VFS_QUERY_DISCOVERED:
            result = run_vfs_query(execution);
            break;
        case WorkloadOp::VFS_UNMOUNT_DISCOVERED:
            result = run_vfs_unmount(execution);
            break;
        case WorkloadOp::COMPUTE_PUBLISH_SET:
        case WorkloadOp::COMPUTE_PUBLISH_QUERY:
            result = run_compute_publish(execution);
            break;
    }
    s_result.status = result;
    return result;
}

auto wki_chaos_workload_snapshot(char* out, size_t capacity) -> size_t {
    if (out == nullptr || capacity == 0) {
        return 0;
    }
    mod::sys::MutexGuard const GUARD(s_workload_lock);
    WorkloadResult result = s_result;
    auto const VFS_LANES = s_result_vfs_lanes;
    if ((result.op == WorkloadOp::VFS_UNMOUNT || result.op == WorkloadOp::VFS_UNMOUNT_DISCOVERED) && result.detached) {
        WorkloadCommand command{
            .op = WorkloadOp::VFS_UNMOUNT,
            .owner = result.owner,
            .resource = result.resource,
            .generation = result.generation,
        };
        result.detach_pending = vfs_detach_pending(command, &result.detach_peer_boot_epoch);
        result.detach_status = result.detach_pending ? -EINPROGRESS : 0;
    }
    result.compute_hold = s_compute_publish_hold.load(std::memory_order_acquire);
    result.compute_waiters = s_compute_publish_waiters.load(std::memory_order_acquire);
    result.compute_wait_count = s_compute_publish_wait_count.load(std::memory_order_acquire);
    result.compute_release_generation = s_compute_publish_release_generation.load(std::memory_order_acquire);
    result.compute_last_wait_status = s_compute_publish_last_wait_status.load(std::memory_order_acquire);
    bool const ENABLED = wki_chaos_workload_allowed();
    bool const SCRATCH_REGISTERED = s_scratch_registered.load(std::memory_order_acquire);
    const char* const NAME = result.name.at(0) != '\0' ? result.name.data() : "-";
    int const WRITTEN = std::snprintf(
        out, capacity,
        "wki_chaos_workload schema=1 enabled=%u scratch_registered=%u op=%s status=%d owner=%u resource=%u generation=%llu "
        "owner_boot_epoch=%u resource_incarnation=%u channel=%u channel_generation=%u attach_cookie=%u "
        "channel_tx_seq_before=%u channel_tx_seq_after=%u channel_tx_delta=%u lba=%llu blocks=%u block_size=%u bytes=%u "
        "ifindex=%u name=%s lane_index=%u lane_count=%u "
        "detach_peer_boot_epoch=%u detach_status=%d read_status=%d write_status=%d flush_status=%d verify_status=%d "
        "restore_status=%d exact_binding=%u rdma=%u verified=%u restored=%u detached=%u detach_pending=%u compute_hold=%u "
        "compute_waiters=%u compute_wait_count=%llu compute_release_generation=%llu compute_last_wait_status=%d rdma_lane=%s "
        "rdma_zone=%u data_slots_before=%llu data_slots_after=%llu tags_before=%llu tags_after=%llu ring_ready=%u sq_depth=%u "
        "cq_depth=%u data_slot_count=%u data_slot_size=%u ring_block_size=%u ring_total_blocks=%llu sq_head_before=%u "
        "sq_tail_before=%u cq_head_before=%u cq_tail_before=%u sq_head_after=%u sq_tail_after=%u cq_head_after=%u "
        "cq_tail_after=%u ring_geometry_valid=%u ring_indices_valid=%u ring_quiescent=%u vfs_lanes=",
        ENABLED ? 1U : 0U, SCRATCH_REGISTERED ? 1U : 0U, op_name(result.op), result.status, static_cast<unsigned>(result.owner),
        result.resource, static_cast<unsigned long long>(result.generation), result.owner_incarnation.owner_boot_epoch,
        result.owner_incarnation.resource_incarnation, static_cast<unsigned>(result.channel), result.channel_generation,
        static_cast<unsigned>(result.attach_cookie), result.channel_tx_seq_before, result.channel_tx_seq_after, result.channel_tx_delta,
        static_cast<unsigned long long>(result.lba), static_cast<unsigned>(result.blocks), result.block_size, result.bytes, result.ifindex,
        NAME, static_cast<unsigned>(result.lane_index), static_cast<unsigned>(result.lane_count), result.detach_peer_boot_epoch,
        result.detach_status, result.read_status, result.write_status, result.flush_status, result.verify_status, result.restore_status,
        result.exact_binding ? 1U : 0U, result.rdma ? 1U : 0U, result.verified ? 1U : 0U, result.restored ? 1U : 0U,
        result.detached ? 1U : 0U, result.detach_pending ? 1U : 0U, result.compute_hold ? 1U : 0U, result.compute_waiters,
        static_cast<unsigned long long>(result.compute_wait_count), static_cast<unsigned long long>(result.compute_release_generation),
        result.compute_last_wait_status, result.rdma ? (result.rdma_roce ? "roce" : "ivshmem") : "none", result.rdma_zone,
        static_cast<unsigned long long>(result.data_slots_before), static_cast<unsigned long long>(result.data_slots_after),
        static_cast<unsigned long long>(result.tags_before), static_cast<unsigned long long>(result.tags_after),
        static_cast<unsigned>(result.ring_geometry.server_ready), result.ring_geometry.sq_depth, result.ring_geometry.cq_depth,
        result.ring_geometry.data_slot_count, result.ring_geometry.data_slot_size, result.ring_geometry.block_size,
        static_cast<unsigned long long>(result.ring_geometry.total_blocks), result.ring_before.sq_head, result.ring_before.sq_tail,
        result.ring_before.cq_head, result.ring_before.cq_tail, result.ring_after.sq_head, result.ring_after.sq_tail,
        result.ring_after.cq_head, result.ring_after.cq_tail, result.ring_geometry_valid ? 1U : 0U, result.ring_indices_valid ? 1U : 0U,
        result.ring_quiescent ? 1U : 0U);
    if (WRITTEN < 0) {
        out[0] = '\0';
        return 0;
    }
    if (static_cast<size_t>(WRITTEN) >= capacity) {
        out[capacity - 1] = '\0';
        return capacity - 1;
    }
    size_t used = static_cast<size_t>(WRITTEN);
    bool appended_lane = false;
    for (size_t i = 0; i < VFS_LANES.size(); ++i) {
        auto const& lane = VFS_LANES.at(i);
        if (!lane.valid) {
            continue;
        }
        int const ADDED =
            std::snprintf(out + used, capacity - used, "%s%u:%u:%u:%u", appended_lane ? "," : "", static_cast<unsigned>(lane.lane_index),
                          static_cast<unsigned>(lane.channel), lane.channel_generation, static_cast<unsigned>(lane.attach_cookie));
        if (ADDED < 0) {
            out[0] = '\0';
            return 0;
        }
        if (static_cast<size_t>(ADDED) >= capacity - used) {
            out[capacity - 1] = '\0';
            return capacity - 1;
        }
        used += static_cast<size_t>(ADDED);
        appended_lane = true;
    }
    int const TRAILER = std::snprintf(out + used, capacity - used, "%s\n", appended_lane ? "" : "-");
    if (TRAILER < 0) {
        out[0] = '\0';
        return 0;
    }
    if (static_cast<size_t>(TRAILER) >= capacity - used) {
        out[capacity - 1] = '\0';
        return capacity - 1;
    }
    return used + static_cast<size_t>(TRAILER);
}

#ifdef WOS_SELFTEST
auto wki_chaos_workload_selftest_parser_and_bounds() -> bool {
    WorkloadCommand command{};
    constexpr char VALID_CLEAR[] = "clear";
    constexpr char VALID_BLOCK[] = "block owner=1 resource=2 generation=3 lba=4 blocks=8 pattern=5";
    constexpr char OVERSIZE_BLOCKS[] = "block owner=1 resource=2 generation=3 lba=4 blocks=9 pattern=5";
    constexpr char INVALID_OWNER[] = "net-query owner=0 resource=2 generation=3";
    constexpr char VALID_NET[] = "net-attach owner=1 resource=2 generation=3 name=wkc7\n";
    constexpr char INVALID_NET_NAME[] = "net-attach owner=1 resource=2 generation=3 name=eth0";
    constexpr char VALID_VFS[] = "vfs-unmount owner=1 resource=2 generation=3";
    constexpr char INVALID_VFS_PATH[] = "vfs-unmount owner=1 resource=2 generation=3 path=/wki/wos-2/tmp";
    constexpr char VALID_COMPUTE_HOLD[] = "compute-publish hold=1";
    constexpr char VALID_COMPUTE_QUERY[] = "compute-publish-query";
    constexpr char INVALID_COMPUTE_HOLD[] = "compute-publish hold=2";
    constexpr char VALID_BLOCK_DISCOVERED[] = "block-discovered owner=1 lba=4 blocks=8 pattern=5";
    constexpr char VALID_BLOCK_RDMA_DISCOVERED[] = "block-rdma-discovered owner=1 lba=4 blocks=8 pattern=5";
    constexpr char VALID_NET_DISCOVERED[] = "net-attach-discovered owner=1 name=wkc7";
    constexpr char VALID_NET_QUERY_DISCOVERED[] = "net-query-discovered owner=1";
    constexpr char VALID_NET_DETACH_DISCOVERED[] = "net-detach-discovered owner=1";
    constexpr char VALID_VFS_QUERY_DISCOVERED[] = "vfs-query-discovered owner=1 name=tmp";
    constexpr char VALID_VFS_DISCOVERED[] = "vfs-unmount-discovered owner=1 name=tmp";
    constexpr char INVALID_VFS_DISCOVERED[] = "vfs-unmount-discovered owner=1 name=root";
    constexpr uint32_t NET_BINDING_BOOT_EPOCH = 0x10203040;
    constexpr auto NET_EVIDENCE_IDENTITY = net_evidence_identity({}, NET_BINDING_BOOT_EPOCH);
    return NET_EVIDENCE_IDENTITY.owner_boot_epoch == NET_BINDING_BOOT_EPOCH && NET_EVIDENCE_IDENTITY.resource_incarnation == 0 &&
           managed_net_detach_converged(true, 0) && !managed_net_detach_converged(true, 1) && !managed_net_detach_converged(false, 0) &&
           parse_command(VALID_CLEAR, sizeof(VALID_CLEAR) - 1, &command) && command.op == WorkloadOp::NONE &&
           parse_command(VALID_BLOCK, sizeof(VALID_BLOCK) - 1, &command) && command.op == WorkloadOp::BLOCK && command.blocks == 8 &&
           !parse_command(OVERSIZE_BLOCKS, sizeof(OVERSIZE_BLOCKS) - 1, &command) &&
           !parse_command(INVALID_OWNER, sizeof(INVALID_OWNER) - 1, &command) &&
           parse_command(VALID_NET, sizeof(VALID_NET) - 1, &command) && command.op == WorkloadOp::NET_ATTACH &&
           std::strcmp(command.name.data(), "wkc7") == 0 && !parse_command(INVALID_NET_NAME, sizeof(INVALID_NET_NAME) - 1, &command) &&
           parse_command(VALID_VFS, sizeof(VALID_VFS) - 1, &command) && command.op == WorkloadOp::VFS_UNMOUNT &&
           !parse_command(INVALID_VFS_PATH, sizeof(INVALID_VFS_PATH) - 1, &command) &&
           parse_command(VALID_COMPUTE_HOLD, sizeof(VALID_COMPUTE_HOLD) - 1, &command) && command.op == WorkloadOp::COMPUTE_PUBLISH_SET &&
           command.hold && parse_command(VALID_COMPUTE_QUERY, sizeof(VALID_COMPUTE_QUERY) - 1, &command) &&
           command.op == WorkloadOp::COMPUTE_PUBLISH_QUERY &&
           !parse_command(INVALID_COMPUTE_HOLD, sizeof(INVALID_COMPUTE_HOLD) - 1, &command) &&
           parse_command(VALID_BLOCK_DISCOVERED, sizeof(VALID_BLOCK_DISCOVERED) - 1, &command) &&
           command.op == WorkloadOp::BLOCK_DISCOVERED &&
           parse_command(VALID_BLOCK_RDMA_DISCOVERED, sizeof(VALID_BLOCK_RDMA_DISCOVERED) - 1, &command) &&
           command.op == WorkloadOp::BLOCK_RDMA_DISCOVERED &&
           parse_command(VALID_NET_DISCOVERED, sizeof(VALID_NET_DISCOVERED) - 1, &command) &&
           command.op == WorkloadOp::NET_ATTACH_DISCOVERED &&
           parse_command(VALID_NET_QUERY_DISCOVERED, sizeof(VALID_NET_QUERY_DISCOVERED) - 1, &command) &&
           command.op == WorkloadOp::NET_QUERY_DISCOVERED &&
           parse_command(VALID_NET_DETACH_DISCOVERED, sizeof(VALID_NET_DETACH_DISCOVERED) - 1, &command) &&
           command.op == WorkloadOp::NET_DETACH_DISCOVERED &&
           parse_command(VALID_VFS_QUERY_DISCOVERED, sizeof(VALID_VFS_QUERY_DISCOVERED) - 1, &command) &&
           command.op == WorkloadOp::VFS_QUERY_DISCOVERED &&
           parse_command(VALID_VFS_DISCOVERED, sizeof(VALID_VFS_DISCOVERED) - 1, &command) &&
           command.op == WorkloadOp::VFS_UNMOUNT_DISCOVERED &&
           !parse_command(INVALID_VFS_DISCOVERED, sizeof(INVALID_VFS_DISCOVERED) - 1, &command) && scratch_range_valid(0, 1) &&
           scratch_range_valid(SCRATCH_TOTAL_BLOCKS - 1, 1) && !scratch_range_valid(SCRATCH_TOTAL_BLOCKS, 1) &&
           !scratch_range_valid(UINT64_MAX, 1);
}
#endif

}  // namespace ker::net::wki
