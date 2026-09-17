#include "auth_policy.hpp"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <platform/security/authorization.hpp>
#include <platform/security/credential_record.hpp>

#include "net/wki/auth.hpp"
#include "net/wki/wire.hpp"

namespace ker::net::wki::auth_policy {

namespace {

auto permissions_for_resource(ResourceType type, uint8_t attach_mode) -> uint64_t {
    uint64_t const ATTACH = wki_policy_bits(WkiPolicy::DEVICE_ATTACH);
    switch (type) {
        case ResourceType::BLOCK:
            return ATTACH | (dev_attach_requests_read(attach_mode) ? wki_policy_bits(WkiPolicy::BLOCK_READ) : 0) |
                   (dev_attach_requests_write(attach_mode) ? wki_policy_bits(WkiPolicy::BLOCK_WRITE) : 0);
        case ResourceType::NET:
            return ATTACH | wki_policy_bits(WkiPolicy::REMOTE_NET);
        case ResourceType::VFS:
            return ATTACH | wki_policy_bits(WkiPolicy::VFS_EXPORT) |
                   wki_policy_bits(dev_attach_requests_write(attach_mode) ? WkiPolicy::VFS_WRITE : WkiPolicy::VFS_READ);
        case ResourceType::COMPUTE:
            return ATTACH | wki_policy_bits(WkiPolicy::COMPUTE);
        case ResourceType::IPC_PIPE:
        case ResourceType::IPC_EVENTFD:
        case ResourceType::IPC_PTY:
        case ResourceType::IPC_FUTEX:
        case ResourceType::IPC_EPOLL:
        case ResourceType::IPC_SOCKET:
            return ATTACH | wki_policy_bits(WkiPolicy::IPC);
        case ResourceType::CHAR:
        case ResourceType::CUSTOM:
            return ATTACH;
    }
    return INVALID;
}

auto permissions_for_dev_op(const void* payload, uint16_t payload_length) -> uint64_t {
    if (payload == nullptr || payload_length < sizeof(DevOpReqPayload)) {
        return INVALID;
    }
    auto const* request = static_cast<const DevOpReqPayload*>(payload);
    switch (request->op_id & 0xFF00U) {
        case 0x0100:
            return wki_policy_bits(request->op_id == OP_BLOCK_READ || request->op_id == OP_BLOCK_BULK_READ ? WkiPolicy::BLOCK_READ
                                                                                                           : WkiPolicy::BLOCK_WRITE);
        case 0x0300:
            if (request->op_id == OP_NET_OPEN_AS || request->op_id == OP_NET_CLOSE_AS || request->op_id == OP_NET_LINK_AS) {
                size_t const SIZE = request->op_id == OP_NET_LINK_AS ? WKI_NET_LINK_IDENTITY_SIZE : WKI_NET_OPEN_IDENTITY_SIZE;
                if (request->data_len != SIZE || payload_length != sizeof(DevOpReqPayload) + request->data_len) return INVALID;
                return wki_policy_bits(WkiPolicy::REMOTE_NET) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
            }
            return wki_policy_bits(WkiPolicy::REMOTE_NET);
        case 0x0400:
            switch (request->op_id) {
                case OP_VFS_OPEN_LIFETIME: {
                    if (request->data_len < sizeof(VfsOpenLifetimeRequest) || payload_length != sizeof(DevOpReqPayload) + request->data_len)
                        return INVALID;
                    VfsOpenLifetimeRequest lifetime{};
                    auto const* data = static_cast<const uint8_t*>(payload) + sizeof(DevOpReqPayload);
                    std::memcpy(&lifetime, data, sizeof(lifetime));
                    if (!wki_vfs_open_lifetime_request_valid(lifetime, request->data_len)) return INVALID;
                    if (lifetime.inner_op == 0 || lifetime.inner_op == OP_VFS_FEATURES) return 0;
                    data += sizeof(lifetime);
                    constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
                    size_t flags_offset = RECORD;
                    if (lifetime.inner_op == OP_VFS_PATH_AS) {
                        if (lifetime.data_len < RECORD + sizeof(VfsPathIdentityHeader)) return INVALID;
                        VfsPathIdentityHeader path{};
                        std::memcpy(&path, data + RECORD, sizeof(path));
                        if (path.operation != VfsPathOperation::OPEN_AT) return INVALID;
                        flags_offset += sizeof(path) + sizeof(int32_t);
                    }
                    if (lifetime.data_len < flags_offset + sizeof(uint32_t)) return INVALID;
                    uint32_t flags = 0;
                    std::memcpy(&flags, data + flags_offset, sizeof(flags));
                    if (lifetime.inner_op == OP_VFS_OPEN_READ_AS)
                        return flags == 0 ? wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN)
                                          : INVALID;
                    return vfs_open_permissions(flags);
                }
                case OP_VFS_DIRECTORY_GRANT: {
                    if (request->data_len != sizeof(VfsDirectoryGrantRequest) + WKI_VFS_DIRECTORY_CALLER_SIZE ||
                        payload_length != sizeof(DevOpReqPayload) + request->data_len)
                        return INVALID;
                    VfsDirectoryGrantRequest grant{};
                    std::memcpy(&grant, static_cast<const uint8_t*>(payload) + sizeof(DevOpReqPayload), sizeof(grant));
                    if (!wki_vfs_directory_grant_request_valid(grant, request->data_len)) return INVALID;
                    return wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
                }
                case OP_VFS_DIRECTORY_REFERENCE: {
                    if (request->data_len < sizeof(VfsDirectoryRequest) || payload_length != sizeof(DevOpReqPayload) + request->data_len)
                        return INVALID;
                    VfsDirectoryRequest directory{};
                    std::memcpy(&directory, static_cast<const uint8_t*>(payload) + sizeof(DevOpReqPayload), sizeof(directory));
                    if (!wki_vfs_directory_request_valid(directory, request->data_len)) return INVALID;
                    return directory.operation == VfsDirectoryOperation::CANCEL
                               ? 0
                               : wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
                }
                case OP_VFS_EXEC_VIEW: {
                    if (request->data_len != sizeof(VfsExecViewRequest) ||
                        payload_length != sizeof(DevOpReqPayload) + sizeof(VfsExecViewRequest))
                        return INVALID;
                    VfsExecViewRequest view{};
                    std::memcpy(&view, static_cast<const uint8_t*>(payload) + sizeof(DevOpReqPayload), sizeof(view));
                    if (!wki_vfs_exec_view_request_valid(view)) return INVALID;
                    return view.operation == VfsExecViewOperation::CANCEL
                               ? 0
                               : wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
                }
                case OP_VFS_CLOSE:
                case OP_VFS_FSTAT:
                    // Releasing an existing channel-scoped description grants
                    // no new access. Its metadata likewise needs no new data
                    // access grant, including on a write-only description.
                    return 0;
                case OP_VFS_OPEN_AS: {
                    constexpr size_t OFFSET = sizeof(DevOpReqPayload) + ker::mod::security::credential_record::SIZE;
                    if (payload_length < OFFSET + sizeof(uint32_t) ||
                        request->data_len < ker::mod::security::credential_record::SIZE + sizeof(uint32_t))
                        return INVALID;
                    uint32_t flags = 0;
                    std::memcpy(&flags, static_cast<const uint8_t*>(payload) + OFFSET, sizeof(flags));
                    return vfs_open_permissions(flags);
                }
                case OP_VFS_XATTR: {
                    constexpr size_t OFFSET = sizeof(DevOpReqPayload);
                    if (payload_length < OFFSET + sizeof(VfsXattrPhaseHeader) || request->data_len < sizeof(VfsXattrPhaseHeader))
                        return INVALID;
                    VfsXattrPhaseHeader header{};
                    std::memcpy(&header, static_cast<const uint8_t*>(payload) + OFFSET, sizeof(header));
                    // The owner validates phase framing and version before any stage effect.
                    if (header.operation == VfsXattrOperation::GET || header.operation == VfsXattrOperation::LIST)
                        return wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
                    if (header.operation == VfsXattrOperation::SET || header.operation == VfsXattrOperation::REMOVE)
                        return wki_policy_bits(WkiPolicy::VFS_WRITE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
                    return INVALID;
                }
                case OP_VFS_METADATA_BATCH: {
                    constexpr size_t OFFSET = sizeof(DevOpReqPayload);
                    if (payload_length < OFFSET + sizeof(VfsMetadataBatchHeader) || request->data_len < sizeof(VfsMetadataBatchHeader))
                        return INVALID;
                    VfsMetadataBatchHeader header{};
                    std::memcpy(&header, static_cast<const uint8_t*>(payload) + OFFSET, sizeof(header));
                    if (header.operation == VfsMetadataBatchOperation::STAT_FOLLOW)
                        return wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
                    if (header.operation == VfsMetadataBatchOperation::CREATE_CLOSE ||
                        header.operation == VfsMetadataBatchOperation::UNLINK || header.operation == VfsMetadataBatchOperation::RENAME)
                        return wki_policy_bits(WkiPolicy::VFS_WRITE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
                    return INVALID;
                }
                case OP_VFS_PATH_AS: {
                    constexpr size_t RECORD_SIZE = ker::mod::security::credential_record::SIZE;
                    constexpr size_t OFFSET = sizeof(DevOpReqPayload) + RECORD_SIZE;
                    if (payload_length < OFFSET + sizeof(VfsPathIdentityHeader) ||
                        request->data_len < RECORD_SIZE + sizeof(VfsPathIdentityHeader))
                        return INVALID;
                    VfsPathIdentityHeader header{};
                    std::memcpy(&header, static_cast<const uint8_t*>(payload) + OFFSET, sizeof(header));
                    if (header.operation == VfsPathOperation::OPEN_AT) {
                        constexpr size_t FLAGS_OFFSET = OFFSET + sizeof(header) + sizeof(int32_t);
                        if (payload_length < FLAGS_OFFSET + sizeof(uint32_t) ||
                            request->data_len < FLAGS_OFFSET + sizeof(uint32_t) - sizeof(DevOpReqPayload))
                            return INVALID;
                        uint32_t flags = 0;
                        std::memcpy(&flags, static_cast<const uint8_t*>(payload) + FLAGS_OFFSET, sizeof(flags));
                        return vfs_open_permissions(flags);
                    }
                    if (wki_vfs_is_file_metadata(header.operation) || header.operation == VfsPathOperation::CHOWN ||
                        header.operation == VfsPathOperation::CHMOD || header.operation == VfsPathOperation::UTIMENS ||
                        header.operation == VfsPathOperation::MKDIR || header.operation == VfsPathOperation::SYMLINK ||
                        header.operation == VfsPathOperation::REMOVE || header.operation == VfsPathOperation::RENAME ||
                        header.operation == VfsPathOperation::DIRECTORY_BINDING)
                        return wki_policy_bits(WkiPolicy::VFS_WRITE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
                    if (header.operation != VfsPathOperation::READLINK && header.operation != VfsPathOperation::READLINK_AT &&
                        header.operation != VfsPathOperation::STAT_AT && header.operation != VfsPathOperation::REALPATH &&
                        header.operation != VfsPathOperation::REALPATH_AT)
                        return INVALID;
                    return wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
                }
                case OP_VFS_STAT_AS:
                case OP_VFS_OPEN_READ_AS:
                    return wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
                case OP_VFS_READ:
                case OP_VFS_READDIR:
                case OP_VFS_STAT:
                case OP_VFS_READLINK:
                case OP_VFS_READ_RDMA:
                case OP_VFS_READDIR_BATCH:
                case OP_VFS_READ_BULK:
                case OP_VFS_SEEK_END:
                    return wki_policy_bits(WkiPolicy::VFS_READ);
                default:
                    return wki_policy_bits(WkiPolicy::VFS_WRITE);
            }
        case 0x0700:
            return wki_policy_bits(WkiPolicy::IPC);
        case 0x0200:
            return wki_policy_bits(WkiPolicy::DEVICE_ATTACH);
        default:
            return INVALID;
    }
}

}  // namespace

auto required_permissions(MsgType message, const void* payload, uint16_t payload_length) -> uint64_t {
    switch (message) {
        case MsgType::TASK_EXEC_COMMIT:
        case MsgType::TASK_CHILD_CLAIM_REQ:
        case MsgType::TASK_CHILD_CLAIM_RESP:
        case MsgType::TASK_CHILD_QUERY_REQ:
        case MsgType::TASK_CHILD_QUERY_RESP:
        case MsgType::TASK_CHILD_SIGNAL:
        case MsgType::TASK_PARENT_DEATH:
        case MsgType::TASK_TERMINAL_SIGNAL:
        case MsgType::TASK_SIGNAL_REQ:
        case MsgType::GROUP_ROUTE_REQ:
        case MsgType::GROUP_RECEIVER_REQ:
        case MsgType::GROUP_SIGNAL_REQ:
        case MsgType::TASK_SUBMIT:
        case MsgType::TASK_SUBMIT_FRAGMENT:
            return wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
        case MsgType::ZONE_CREATE_REQ:
        case MsgType::ZONE_DESTROY:
        case MsgType::ZONE_READ_REQ:
        case MsgType::ZONE_WRITE_REQ:
        case MsgType::ZONE_NOTIFY_PRE:
        case MsgType::ZONE_NOTIFY_POST:
            return wki_policy_bits(WkiPolicy::ZONE_RDMA);
        case MsgType::EVENT_SUBSCRIBE:
        case MsgType::EVENT_UNSUBSCRIBE:
        case MsgType::EVENT_PUBLISH:
            return wki_policy_bits(WkiPolicy::IPC);
        case MsgType::DEV_ATTACH_REQ: {
            if (payload == nullptr || payload_length < sizeof(DevAttachReqPayload)) {
                return INVALID;
            }
            auto const* request = static_cast<const DevAttachReqPayload*>(payload);
            return permissions_for_resource(static_cast<ResourceType>(request->resource_type), request->attach_mode);
        }
        case MsgType::DEV_OP_REQ:
            return permissions_for_dev_op(payload, payload_length);
        default:
            return 0;
    }
}

auto map_net_control_identity(uint64_t policy, const ker::mod::security::Snapshot& supplied, ker::mod::security::Snapshot& mapped) -> int {
    constexpr auto REQUIRED = wki_policy_bits(WkiPolicy::REMOTE_NET) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    if ((policy & ~wki_policy_bits(WkiPolicy::ALL)) != 0 || (policy & REQUIRED) != REQUIRED) return -EACCES;
    auto const TRUST = (policy & wki_policy_bits(WkiPolicy::IDENTITY_ROOT)) != 0 ? ker::mod::security::RemoteIdentityTrust::SAME_DOMAIN_ROOT
                                                                                 : ker::mod::security::RemoteIdentityTrust::SAME_DOMAIN;
    return ker::mod::security::map_remote_identity(supplied, TRUST, mapped);
}

auto map_compute_identity(uint64_t policy, const ker::mod::security::Snapshot& supplied, ker::mod::security::Snapshot& mapped) -> int {
    constexpr auto REQUIRED = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    if ((policy & ~wki_policy_bits(WkiPolicy::ALL)) != 0 || (policy & REQUIRED) != REQUIRED) {
        return -EACCES;
    }
    auto const TRUST = (policy & wki_policy_bits(WkiPolicy::IDENTITY_ROOT)) != 0 ? ker::mod::security::RemoteIdentityTrust::SAME_DOMAIN_ROOT
                                                                                 : ker::mod::security::RemoteIdentityTrust::SAME_DOMAIN;
    return ker::mod::security::map_remote_identity(supplied, TRUST, mapped);
}

auto map_vfs_mutation_identity(uint64_t policy, const ker::mod::security::Snapshot& supplied, ker::mod::security::Snapshot& mapped) -> int {
    constexpr auto REQUIRED = wki_policy_bits(WkiPolicy::VFS_WRITE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    if ((policy & ~wki_policy_bits(WkiPolicy::ALL)) != 0 || (policy & REQUIRED) != REQUIRED) return -EACCES;
    auto const TRUST = (policy & wki_policy_bits(WkiPolicy::IDENTITY_ROOT)) != 0 ? ker::mod::security::RemoteIdentityTrust::SAME_DOMAIN_ROOT
                                                                                 : ker::mod::security::RemoteIdentityTrust::SAME_DOMAIN;
    return ker::mod::security::map_remote_identity(supplied, TRUST, mapped);
}

auto map_vfs_read_identity(uint64_t policy, const ker::mod::security::Snapshot& supplied, ker::mod::security::Snapshot& mapped) -> int {
    constexpr auto REQUIRED = wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    if ((policy & ~wki_policy_bits(WkiPolicy::ALL)) != 0 || (policy & REQUIRED) != REQUIRED) return -EACCES;
    auto const TRUST = (policy & wki_policy_bits(WkiPolicy::IDENTITY_ROOT)) != 0 ? ker::mod::security::RemoteIdentityTrust::SAME_DOMAIN_ROOT
                                                                                 : ker::mod::security::RemoteIdentityTrust::SAME_DOMAIN;
    return ker::mod::security::map_remote_identity(supplied, TRUST, mapped);
}

auto vfs_open_permissions(uint32_t flags) -> uint64_t {
    constexpr uint32_t ACCESS_MASK = 3;
    constexpr uint32_t CREATE = 0100;
    constexpr uint32_t TRUNCATE = 01000;
    uint32_t const ACCESS = flags & ACCESS_MASK;
    if (ACCESS == 3) return INVALID;
    uint64_t required = wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    if (ACCESS != 1) required |= wki_policy_bits(WkiPolicy::VFS_READ);
    if (ACCESS != 0 || (flags & (CREATE | TRUNCATE)) != 0) required |= wki_policy_bits(WkiPolicy::VFS_WRITE);
    return required;
}

auto map_vfs_open_identity(uint64_t policy, const ker::mod::security::Snapshot& supplied, uint32_t flags,
                           ker::mod::security::Snapshot& mapped) -> int {
    uint64_t const REQUIRED = vfs_open_permissions(flags);
    if (REQUIRED == INVALID) return -EINVAL;
    if ((policy & ~wki_policy_bits(WkiPolicy::ALL)) != 0 || (policy & REQUIRED) != REQUIRED) return -EACCES;
    auto const TRUST = (policy & wki_policy_bits(WkiPolicy::IDENTITY_ROOT)) != 0 ? ker::mod::security::RemoteIdentityTrust::SAME_DOMAIN_ROOT
                                                                                 : ker::mod::security::RemoteIdentityTrust::SAME_DOMAIN;
    return ker::mod::security::map_remote_identity(supplied, TRUST, mapped);
}

}  // namespace ker::net::wki::auth_policy
