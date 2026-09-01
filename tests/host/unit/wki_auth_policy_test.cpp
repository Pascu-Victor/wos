#include <gtest/gtest.h>

#include <cstdint>
#include <net/wki/auth_policy.hpp>
#include <vfs/stat.hpp>

#include "net/wki/auth.hpp"
#include "net/wki/wire.hpp"

using namespace ker::net::wki;

namespace policy = ker::net::wki::auth_policy;

TEST(WkiAuthPolicy, IndependentlyClassifiesPrivilegedServices) {
    EXPECT_EQ(policy::required_permissions(MsgType::TASK_SUBMIT, nullptr, 0), wki_policy_bits(WkiPolicy::COMPUTE));
    EXPECT_EQ(policy::required_permissions(MsgType::EVENT_SUBSCRIBE, nullptr, 0), wki_policy_bits(WkiPolicy::IPC));
    EXPECT_EQ(policy::required_permissions(MsgType::ZONE_CREATE_REQ, nullptr, 0), wki_policy_bits(WkiPolicy::ZONE_RDMA));
    EXPECT_EQ(policy::required_permissions(MsgType::HEARTBEAT, nullptr, 0), 0U);
}

TEST(WkiAuthPolicy, VfsAttachRequiresExportAttachAndRequestedAccess) {
    DevAttachReqPayload request{};
    request.resource_type = static_cast<uint16_t>(ResourceType::VFS);
    request.attach_mode = DEV_ATTACH_ACCESS_READ;
    uint64_t const READ_REQUIRED = policy::required_permissions(MsgType::DEV_ATTACH_REQ, &request, sizeof(request));
    EXPECT_EQ(READ_REQUIRED,
              wki_policy_bits(WkiPolicy::DEVICE_ATTACH) | wki_policy_bits(WkiPolicy::VFS_EXPORT) | wki_policy_bits(WkiPolicy::VFS_READ));

    request.attach_mode = DEV_ATTACH_ACCESS_WRITE;
    uint64_t const WRITE_REQUIRED = policy::required_permissions(MsgType::DEV_ATTACH_REQ, &request, sizeof(request));
    EXPECT_EQ(WRITE_REQUIRED,
              wki_policy_bits(WkiPolicy::DEVICE_ATTACH) | wki_policy_bits(WkiPolicy::VFS_EXPORT) | wki_policy_bits(WkiPolicy::VFS_WRITE));
}

TEST(WkiAuthPolicy, SeparatesBlockReaderAndWriter) {
    DevAttachReqPayload request{};
    request.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
    request.attach_mode = DEV_ATTACH_ACCESS_READ;
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_ATTACH_REQ, &request, sizeof(request)),
              wki_policy_bits(WkiPolicy::DEVICE_ATTACH) | wki_policy_bits(WkiPolicy::BLOCK_READ));
    request.attach_mode = DEV_ATTACH_ACCESS_WRITE;
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_ATTACH_REQ, &request, sizeof(request)),
              wki_policy_bits(WkiPolicy::DEVICE_ATTACH) | wki_policy_bits(WkiPolicy::BLOCK_WRITE));
}

TEST(WkiAuthPolicy, SeparatesVfsReadWriteNetworkAndIpcOperations) {
    DevOpReqPayload request{};
    request.op_id = OP_VFS_READ;
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, &request, sizeof(request)), wki_policy_bits(WkiPolicy::VFS_READ));
    request.op_id = OP_VFS_WRITE;
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, &request, sizeof(request)), wki_policy_bits(WkiPolicy::VFS_WRITE));
    request.op_id = OP_NET_XMIT;
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, &request, sizeof(request)), wki_policy_bits(WkiPolicy::REMOTE_NET));
    request.op_id = OP_PIPE_DATA;
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, &request, sizeof(request)), wki_policy_bits(WkiPolicy::IPC));
    request.op_id = 0x6600;
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, &request, sizeof(request)), policy::INVALID);
}

TEST(WkiAuthPolicy, RejectsMalformedPrivilegedPayloads) {
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_ATTACH_REQ, nullptr, 0), policy::INVALID);
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, nullptr, 0), policy::INVALID);
}
