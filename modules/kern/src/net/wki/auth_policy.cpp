#include "auth_policy.hpp"

#include <cstdint>

#include "net/wki/auth.hpp"
#include "net/wki/wire.hpp"

namespace ker::net::wki::auth_policy {

namespace {

auto permissions_for_resource(ResourceType type, uint8_t attach_mode) -> uint64_t {
    uint64_t const ATTACH = wki_policy_bits(WkiPolicy::DEVICE_ATTACH);
    switch (type) {
        case ResourceType::BLOCK:
            return ATTACH | wki_policy_bits(dev_attach_requests_write(attach_mode) ? WkiPolicy::BLOCK_WRITE : WkiPolicy::BLOCK_READ);
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
            return wki_policy_bits(WkiPolicy::REMOTE_NET);
        case 0x0400:
            switch (request->op_id) {
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
        case MsgType::TASK_SUBMIT:
        case MsgType::TASK_SUBMIT_FRAGMENT:
            return wki_policy_bits(WkiPolicy::COMPUTE);
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

}  // namespace ker::net::wki::auth_policy
