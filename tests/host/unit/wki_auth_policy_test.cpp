#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <net/wki/auth_policy.hpp>
#include <platform/security/authorization.hpp>
#include <platform/security/credential_record.hpp>
#include <platform/security/file_policy.hpp>
#include <vfs/stat.hpp>

#include "net/wki/auth.hpp"
#include "net/wki/wire.hpp"

using namespace ker::net::wki;

namespace policy = ker::net::wki::auth_policy;

TEST(WkiAuthPolicy, XattrPhasesRequireOperationPermissionAndMappedIdentity) {
    std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(VfsXattrPhaseHeader)> bytes{};
    DevOpReqPayload envelope{.op_id = OP_VFS_XATTR, .data_len = sizeof(VfsXattrPhaseHeader)};
    VfsXattrPhaseHeader header{};
    auto permissions = [&](size_t length) {
        std::memcpy(bytes.data(), &envelope, sizeof(envelope));
        std::memcpy(bytes.data() + sizeof(envelope), &header, sizeof(header));
        return policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), length);
    };
    for (auto operation : {VfsXattrOperation::GET, VfsXattrOperation::LIST, VfsXattrOperation::SET, VfsXattrOperation::REMOVE}) {
        header.operation = operation;
        bool const READ = operation == VfsXattrOperation::GET || operation == VfsXattrOperation::LIST;
        auto const REQUIRED =
            wki_policy_bits(READ ? WkiPolicy::VFS_READ : WkiPolicy::VFS_WRITE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
        for (auto phase : {VfsXattrPhase::BEGIN, VfsXattrPhase::DATA, VfsXattrPhase::COMMIT, VfsXattrPhase::ABORT}) {
            header.phase = phase;
            EXPECT_EQ(permissions(bytes.size()), REQUIRED);
        }
        for (size_t length = 0; length < bytes.size(); ++length) EXPECT_EQ(permissions(length), policy::INVALID);
        envelope.data_len = sizeof(header) - 1;
        EXPECT_EQ(permissions(bytes.size()), policy::INVALID);
        envelope.data_len = sizeof(header);
    }
    header.operation = static_cast<VfsXattrOperation>(0);
    EXPECT_EQ(permissions(bytes.size()), policy::INVALID);
    header.operation = static_cast<VfsXattrOperation>(5);
    EXPECT_EQ(permissions(bytes.size()), policy::INVALID);
}

TEST(WkiAuthPolicy, ExecViewPinsRequireMappedReadAndCancellationGrantsNoAccess) {
    std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(VfsExecViewRequest) + 1> bytes{};
    DevOpReqPayload envelope{.op_id = OP_VFS_EXEC_VIEW, .data_len = sizeof(VfsExecViewRequest)};
    VfsExecViewRequest request{.source_fd = 7, .token = 19};
    auto permissions = [&](size_t length) {
        std::memcpy(bytes.data(), &envelope, sizeof(envelope));
        std::memcpy(bytes.data() + sizeof(envelope), &request, sizeof(request));
        return policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), length);
    };
    constexpr auto REQUIRED = wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    EXPECT_EQ(permissions(bytes.size() - 1), REQUIRED);
    for (size_t length = 0; length < bytes.size() - 1; ++length) EXPECT_EQ(permissions(length), policy::INVALID);
    EXPECT_EQ(permissions(bytes.size()), policy::INVALID);
    request.operation = VfsExecViewOperation::CANCEL;
    EXPECT_EQ(permissions(bytes.size() - 1), policy::INVALID);
    request.source_fd = 0;
    EXPECT_EQ(permissions(bytes.size() - 1), 0U);
    request.token = 0;
    EXPECT_EQ(permissions(bytes.size() - 1), policy::INVALID);
    request.token = 19;
    request.version++;
    EXPECT_EQ(permissions(bytes.size() - 1), policy::INVALID);
    request.version = WKI_VFS_EXEC_VIEW_VERSION;
    request.operation = static_cast<VfsExecViewOperation>(3);
    EXPECT_EQ(permissions(bytes.size() - 1), policy::INVALID);
    VfsExecViewResponse response{.fd = 9, .token = 19};
    EXPECT_TRUE(wki_vfs_exec_view_response_valid(response, 19));
    EXPECT_FALSE(wki_vfs_exec_view_response_valid(response, 20));
    response.reserved = 1;
    EXPECT_FALSE(wki_vfs_exec_view_response_valid(response, 19));
    response.reserved = 0;
    response.version++;
    EXPECT_FALSE(wki_vfs_exec_view_response_valid(response, 19));
    for (size_t length = 0; length <= sizeof(response) + 1; ++length) {
        EXPECT_EQ(wki_vfs_exec_view_reply_size_valid(0, length, sizeof(response)), length == sizeof(response));
        EXPECT_EQ(wki_vfs_exec_view_reply_size_valid(0, length, 0), length == 0);
        EXPECT_EQ(wki_vfs_exec_view_reply_size_valid(-EIO, length, sizeof(response)), length == 0);
    }
    EXPECT_FALSE(wki_vfs_exec_view_reply_size_valid(1, sizeof(response), sizeof(response)));
    EXPECT_FALSE(wki_vfs_exec_view_reply_size_valid(0, 1, 1));
}

TEST(WkiAuthPolicy, PathEnvelopeReadsRequireIdentityAndBoundedSelector) {
    constexpr size_t RECORD_SIZE = ker::mod::security::credential_record::SIZE;
    constexpr size_t BODY_SIZE = RECORD_SIZE + sizeof(VfsPathIdentityHeader);
    constexpr auto REQUIRED = wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    std::array<uint8_t, sizeof(DevOpReqPayload) + BODY_SIZE> bytes{};
    DevOpReqPayload request{};
    request.op_id = OP_VFS_PATH_AS;
    request.data_len = BODY_SIZE;
    VfsPathIdentityHeader header{.operation = VfsPathOperation::READLINK, .flags = 0, .data_len = 0, .reserved = 0};
    std::memcpy(bytes.data(), &request, sizeof(request));
    std::memcpy(bytes.data() + sizeof(request) + RECORD_SIZE, &header, sizeof(header));
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), REQUIRED);
    for (size_t length = 0; length < bytes.size(); ++length)
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), length), policy::INVALID);
    for (size_t length = 0; length < BODY_SIZE; ++length) {
        request.data_len = length;
        std::memcpy(bytes.data(), &request, sizeof(request));
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), policy::INVALID);
    }
    request.data_len = BODY_SIZE;
    std::memcpy(bytes.data(), &request, sizeof(request));
    for (auto operation :
         {VfsPathOperation::READLINK_AT, VfsPathOperation::STAT_AT, VfsPathOperation::REALPATH, VfsPathOperation::REALPATH_AT}) {
        header.operation = operation;
        std::memcpy(bytes.data() + sizeof(request) + RECORD_SIZE, &header, sizeof(header));
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), REQUIRED);
    }
    for (uint16_t operation : {0U, 18U, 0xFFFFU}) {
        header.operation = static_cast<VfsPathOperation>(operation);
        std::memcpy(bytes.data() + sizeof(request) + RECORD_SIZE, &header, sizeof(header));
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), policy::INVALID);
    }
}

TEST(WkiAuthPolicy, DirectoryOpenUsesBoundedFlagsAndMatchingReadWritePolicy) {
    constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
    constexpr size_t FLAGS_OFFSET = sizeof(DevOpReqPayload) + RECORD + sizeof(VfsPathIdentityHeader) + sizeof(int32_t);
    std::array<uint8_t, FLAGS_OFFSET + sizeof(uint32_t)> bytes{};
    DevOpReqPayload request{.op_id = OP_VFS_PATH_AS, .data_len = bytes.size() - sizeof(DevOpReqPayload)};
    VfsPathIdentityHeader header{.operation = VfsPathOperation::OPEN_AT};
    std::memcpy(bytes.data(), &request, sizeof(request));
    std::memcpy(bytes.data() + sizeof(request) + RECORD, &header, sizeof(header));
    for (uint32_t flags : {0U, 1U, 2U, 3U, 0100U, 01000U, 0200000U, 0400000U, 02000200U}) {
        std::memcpy(bytes.data() + FLAGS_OFFSET, &flags, sizeof(flags));
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), policy::vfs_open_permissions(flags));
    }
    for (size_t length = 0; length < bytes.size(); ++length)
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), length), policy::INVALID);
    for (size_t length = 0; length < request.data_len; ++length) {
        auto shorter = request;
        shorter.data_len = length;
        std::memcpy(bytes.data(), &shorter, sizeof(shorter));
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), policy::INVALID);
    }
    VfsPathIdentityHeader reply{.operation = VfsPathOperation::OPEN_AT, .data_len = sizeof(VfsPathOpenResult)};
    EXPECT_TRUE(wki_vfs_path_response_valid(0, sizeof(reply) + sizeof(VfsPathOpenResult), reply));
    EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(reply) + sizeof(VfsPathOpenResult) - 1, reply));
    EXPECT_FALSE(wki_vfs_path_response_valid(-EACCES, sizeof(reply), reply));
    EXPECT_TRUE(wki_vfs_path_response_valid(-EACCES, 0, reply));
}

TEST(WkiAuthPolicy, PathRepliesRejectMalformedHeadersAndErrorData) {
    VfsPathIdentityHeader header{.operation = VfsPathOperation::READLINK, .flags = 0, .data_len = 5, .reserved = 0};
    EXPECT_TRUE(wki_vfs_path_response_valid(0, sizeof(header) + 5, header));
    EXPECT_TRUE(wki_vfs_path_response_valid(-EACCES, 0, header));
    EXPECT_TRUE(wki_vfs_path_response_valid(-ENOTSUP, 0, header));
    EXPECT_FALSE(wki_vfs_path_response_valid(-EACCES, sizeof(header), header));
    EXPECT_FALSE(wki_vfs_path_response_valid(1, sizeof(header) + 5, header));
    for (size_t size : {0U, 1U, 7U, 8U, 12U, 14U, 65535U}) EXPECT_FALSE(wki_vfs_path_response_valid(0, size, header));
    header.flags = 1;
    EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + 5, header));
    header.flags = 0;
    header.reserved = 1;
    EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + 5, header));
    header.reserved = 0;
    header.operation = static_cast<VfsPathOperation>(6);
    EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + 5, header));
    header.operation = VfsPathOperation::READLINK;
    header.data_len = 512;
    EXPECT_TRUE(wki_vfs_path_response_valid(0, sizeof(header) + 512, header));
    header.data_len = 513;
    EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + 513, header));
}

TEST(WkiAuthPolicy, PathMetadataRequiresReadAndIdentityWithoutWriteAuthority) {
    constexpr auto REQUIRED = wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(VfsMetadataBatchHeader)> bytes{};
    DevOpReqPayload request{};
    request.op_id = OP_VFS_STAT_AS;
    std::memcpy(bytes.data(), &request, sizeof(request));
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), REQUIRED);
    request.op_id = OP_VFS_METADATA_BATCH;
    request.data_len = sizeof(VfsMetadataBatchHeader);
    std::memcpy(bytes.data(), &request, sizeof(request));
    VfsMetadataBatchHeader header{
        .version = VFS_METADATA_BATCH_STAT_VERSION, .operation = VfsMetadataBatchOperation::STAT_FOLLOW, .count = 1, .mode = 0};
    std::memcpy(bytes.data() + sizeof(request), &header, sizeof(header));
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), REQUIRED);
    for (size_t length = sizeof(request); length < bytes.size(); ++length)
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), length), policy::INVALID);
    for (auto operation : {VfsMetadataBatchOperation::CREATE_CLOSE, VfsMetadataBatchOperation::UNLINK, VfsMetadataBatchOperation::RENAME}) {
        header.version = VFS_METADATA_BATCH_MUTATION_VERSION;
        header.operation = operation;
        std::memcpy(bytes.data() + sizeof(request), &header, sizeof(header));
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()),
                  wki_policy_bits(WkiPolicy::VFS_WRITE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN));
    }
    header.operation = static_cast<VfsMetadataBatchOperation>(5);
    std::memcpy(bytes.data() + sizeof(request), &header, sizeof(header));
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), policy::INVALID);
}

TEST(WkiAuthPolicy, RetainedDescriptorMetadataNeedsNoAdditionalDataGrant) {
    std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(int32_t)> bytes{};
    DevOpReqPayload request{};
    request.op_id = OP_VFS_FSTAT;
    request.data_len = sizeof(int32_t);
    std::memcpy(bytes.data(), &request, sizeof(request));
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), 0U);
}

TEST(WkiAuthPolicy, DescriptorMetadataResponsesCannotTruncateOrCarryErrorData) {
    static_assert(sizeof(ker::vfs::Stat) == WKI_VFS_FSTAT_RESPONSE_SIZE);
    EXPECT_TRUE(wki_vfs_fstat_response_valid(0, sizeof(ker::vfs::Stat)));
    for (size_t size : {0U, 1U, 143U, 145U, 65535U}) EXPECT_FALSE(wki_vfs_fstat_response_valid(0, size));
    EXPECT_TRUE(wki_vfs_fstat_response_valid(-EBADF, 0));
    EXPECT_TRUE(wki_vfs_fstat_response_valid(-ENOTSUP, 0));
    EXPECT_FALSE(wki_vfs_fstat_response_valid(-EBADF, sizeof(ker::vfs::Stat)));
    EXPECT_FALSE(wki_vfs_fstat_response_valid(1, sizeof(ker::vfs::Stat)));
}

TEST(WkiAuthPolicy, OrdinaryOpenClassifiesAccessCreationAndTruncationBeforeAdmission) {
    constexpr auto READ = wki_policy_bits(WkiPolicy::VFS_READ);
    constexpr auto WRITE = wki_policy_bits(WkiPolicy::VFS_WRITE);
    constexpr auto IDENTITY = wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
    std::array<uint8_t, sizeof(DevOpReqPayload) + RECORD + 10> bytes{};
    DevOpReqPayload request{};
    request.op_id = OP_VFS_OPEN_AS;
    request.data_len = RECORD + 10;
    std::memcpy(bytes.data(), &request, sizeof(request));
    struct Case {
        uint32_t flags;
        uint64_t permissions;
    };
    for (auto entry :
         {Case{0, READ}, Case{1, WRITE}, Case{2, READ | WRITE}, Case{0100, READ | WRITE}, Case{01000, READ | WRITE}, Case{0200000, READ}}) {
        std::memcpy(bytes.data() + sizeof(request) + RECORD, &entry.flags, sizeof(entry.flags));
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), entry.permissions | IDENTITY);
    }
    uint32_t flags = 3;
    std::memcpy(bytes.data() + sizeof(request) + RECORD, &flags, sizeof(flags));
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), policy::INVALID);
    for (size_t length = sizeof(request); length < sizeof(request) + RECORD + 4; ++length)
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), length), policy::INVALID);
}

TEST(WkiAuthPolicy, OrdinaryOpenMapsFullSnapshotAndCannotBorrowMissingWriteOrRootTrust) {
    namespace security = ker::mod::security;
    constexpr auto READ = wki_policy_bits(WkiPolicy::VFS_READ);
    constexpr auto WRITE = wki_policy_bits(WkiPolicy::VFS_WRITE);
    constexpr auto IDENTITY = wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    constexpr auto ROOT_TRUST = wki_policy_bits(WkiPolicy::IDENTITY_ROOT);
    std::array<uint32_t, 2> groups{3000, 3001};
    security::Snapshot supplied;
    ASSERT_EQ(security::Snapshot::restore_fields({.user = {1000, 1001, 1002},
                                                  .group = {2000, 2001, 2002},
                                                  .groups = groups,
                                                  .authority_ceiling = 0,
                                                  .service_privileges = 3,
                                                  .no_new_privileges = true,
                                                  .dumpable = false,
                                                  .service_privileges_locked = true},
                                                 supplied),
              0);
    auto const BEFORE = security::Snapshot::bootstrap_root();
    auto mapped = BEFORE;
    EXPECT_EQ(policy::map_vfs_open_identity(READ | WRITE, supplied, 0, mapped), -EACCES);
    EXPECT_EQ(mapped, BEFORE);
    for (uint32_t flags : {1U, 2U, 0100U, 01000U}) {
        EXPECT_EQ(policy::map_vfs_open_identity(READ | IDENTITY, supplied, flags, mapped), -EACCES);
        EXPECT_EQ(mapped, BEFORE);
    }
    EXPECT_EQ(policy::map_vfs_open_identity(READ | WRITE | IDENTITY, supplied, 3, mapped), -EINVAL);
    EXPECT_EQ(mapped, BEFORE);
    ASSERT_EQ(policy::map_vfs_open_identity(READ | WRITE | IDENTITY, supplied, 2, mapped), 0);
    EXPECT_EQ(mapped, supplied);
    EXPECT_EQ(policy::map_vfs_open_identity(READ | WRITE | IDENTITY, BEFORE, 2, mapped), -EACCES);
    EXPECT_EQ(mapped, supplied);
    ASSERT_EQ(policy::map_vfs_open_identity(READ | WRITE | IDENTITY | ROOT_TRUST, BEFORE, 2, mapped), 0);
    EXPECT_EQ(mapped, BEFORE);
}

TEST(WkiAuthPolicy, RetainedFileDescriptionDoesNotGainAccessFromTransportAuthority) {
    namespace security = ker::mod::security;
    EXPECT_EQ(security::authorize_file_description_access(0, false), 0);
    EXPECT_EQ(security::authorize_file_description_access(0, true), -EBADF);
    EXPECT_EQ(security::authorize_file_description_access(1, false), -EBADF);
    EXPECT_EQ(security::authorize_file_description_access(1, true), 0);
    EXPECT_EQ(security::authorize_file_description_access(2, false), 0);
    EXPECT_EQ(security::authorize_file_description_access(2, true), 0);
    EXPECT_EQ(security::authorize_file_description_access(3, false), -EBADF);
    EXPECT_EQ(security::authorize_file_description_access(3, true), -EBADF);
}

TEST(WkiAuthPolicy, IndependentlyClassifiesPrivilegedServices) {
    auto const COMPUTE_IDENTITY = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    EXPECT_EQ(policy::required_permissions(MsgType::TASK_SUBMIT, nullptr, 0), COMPUTE_IDENTITY);
    EXPECT_EQ(policy::required_permissions(MsgType::TASK_SUBMIT_FRAGMENT, nullptr, 0), COMPUTE_IDENTITY);
    EXPECT_EQ(policy::required_permissions(MsgType::TASK_EXEC_COMMIT, nullptr, 0), COMPUTE_IDENTITY);
    EXPECT_EQ(policy::required_permissions(MsgType::GROUP_ROUTE_REQ, nullptr, 0), COMPUTE_IDENTITY);
    EXPECT_EQ(policy::required_permissions(MsgType::GROUP_ROUTE_RESP, nullptr, 0), 0U);
    EXPECT_EQ(policy::required_permissions(MsgType::GROUP_RECEIVER_REQ, nullptr, 0), COMPUTE_IDENTITY);
    EXPECT_EQ(policy::required_permissions(MsgType::GROUP_RECEIVER_RESP, nullptr, 0), 0U);
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
    for (uint8_t mode : {uint8_t{0}, uint8_t{DEV_ATTACH_ACCESS_READ | DEV_ATTACH_ACCESS_WRITE}}) {
        request.attach_mode = mode;
        EXPECT_EQ(
            policy::required_permissions(MsgType::DEV_ATTACH_REQ, &request, sizeof(request)),
            wki_policy_bits(WkiPolicy::DEVICE_ATTACH) | wki_policy_bits(WkiPolicy::BLOCK_READ) | wki_policy_bits(WkiPolicy::BLOCK_WRITE));
    }
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

TEST(WkiAuthPolicy, ComputeIdentityNeedsExplicitTrustAndRootPermissionForEveryIdSlot) {
    namespace security = ker::mod::security;
    constexpr uint64_t SAME = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    constexpr uint64_t ROOT = SAME | wki_policy_bits(WkiPolicy::IDENTITY_ROOT);
    for (unsigned mask = 0; mask < 64; ++mask) {
        std::array<uint32_t, 6> ids{};
        for (unsigned slot = 0; slot < ids.size(); ++slot) ids[slot] = (mask & (1U << slot)) != 0 ? 1000 + slot : 0;
        security::Snapshot supplied;
        ASSERT_EQ(security::restore_legacy_remote_exec_identity({ids[0], ids[1], ids[2]}, {ids[3], ids[4], ids[5]}, supplied), 0);
        for (uint64_t bits : {uint64_t{0}, uint64_t{1}, uint64_t{1023}, uint64_t{1024}, uint64_t{2048}, uint64_t{2049}, SAME, ROOT,
                              ROOT | (uint64_t{1} << 63)}) {
            security::Snapshot mapped;
            auto const BEFORE = mapped;
            bool const ALLOWED = bits == ROOT || (bits == SAME && mask == 63);
            EXPECT_EQ(policy::map_compute_identity(bits, supplied, mapped), ALLOWED ? 0 : -EACCES);
            EXPECT_EQ(mapped, ALLOWED ? supplied : BEFORE);
        }
    }
}

TEST(WkiAuthPolicy, MappingPreservesDroppedAuthorityAndRejectsSupplementaryRootGroup) {
    namespace security = ker::mod::security;
    constexpr uint64_t SAME = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    auto supplied = security::Snapshot::bootstrap_root();
    constexpr std::array<uint32_t, 2> GROUPS{3000, 3001};
    ASSERT_EQ(supplied.prepare({.change = security::Change::SET_GROUPS, .groups = GROUPS}, supplied), 0);
    ASSERT_EQ(supplied.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, supplied), 0);
    ASSERT_EQ(supplied.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, supplied), 0);
    ASSERT_EQ(supplied.prepare({.change = security::Change::SET_GID, .real = 2000}, supplied), 0);
    ASSERT_EQ(supplied.prepare({.change = security::Change::SET_UID, .real = 1000}, supplied), 0);
    ASSERT_EQ(supplied.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, supplied), 0);
    security::Snapshot mapped;
    ASSERT_EQ(policy::map_compute_identity(SAME, supplied, mapped), 0);
    EXPECT_EQ(mapped, supplied);
    EXPECT_TRUE(mapped.no_new_privileges());
    EXPECT_TRUE(mapped.service_privileges_locked());
    EXPECT_FALSE(mapped.dumpable());
    EXPECT_EQ(mapped.service_privileges(), 3U);
    EXPECT_EQ(mapped.authority_ceiling(), 0U);
    supplied = security::Snapshot::bootstrap_root();
    constexpr std::array<uint32_t, 1> ROOT_GROUP{0};
    ASSERT_EQ(supplied.prepare({.change = security::Change::SET_GROUPS, .groups = ROOT_GROUP}, supplied), 0);
    ASSERT_EQ(supplied.prepare({.change = security::Change::SET_GID, .real = 2000}, supplied), 0);
    ASSERT_EQ(supplied.prepare({.change = security::Change::SET_UID, .real = 1000}, supplied), 0);
    auto const BEFORE = mapped;
    EXPECT_EQ(policy::map_compute_identity(SAME, supplied, mapped), -EACCES);
    EXPECT_EQ(mapped, BEFORE);
    EXPECT_EQ(policy::map_compute_identity(SAME | wki_policy_bits(WkiPolicy::IDENTITY_ROOT), supplied, mapped), 0);
    EXPECT_EQ(mapped, supplied);
}

TEST(WkiAuthPolicy, ActiveServiceGrantsNeedRootInclusiveTrust) {
    namespace security = ker::mod::security;
    constexpr auto IDENTITY = wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    constexpr auto ROOT_TRUST = wki_policy_bits(WkiPolicy::IDENTITY_ROOT);
    auto supplied = security::Snapshot::bootstrap_root();
    ASSERT_EQ(supplied.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, supplied), 0);
    ASSERT_EQ(supplied.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, supplied), 0);
    ASSERT_EQ(supplied.prepare({.change = security::Change::SET_GID, .real = 2000}, supplied), 0);
    ASSERT_EQ(supplied.prepare({.change = security::Change::SET_UID, .real = 1000}, supplied), 0);
    EXPECT_EQ(supplied.effective_service_privileges(), 3U);
    security::Snapshot mapped;
    auto const BEFORE = mapped;
    auto const COMPUTE = wki_policy_bits(WkiPolicy::COMPUTE) | IDENTITY;
    auto const READ = wki_policy_bits(WkiPolicy::VFS_READ) | IDENTITY;
    EXPECT_EQ(policy::map_compute_identity(COMPUTE, supplied, mapped), -EACCES);
    EXPECT_EQ(mapped, BEFORE);
    EXPECT_EQ(policy::map_vfs_read_identity(READ, supplied, mapped), -EACCES);
    EXPECT_EQ(mapped, BEFORE);
    ASSERT_EQ(policy::map_compute_identity(COMPUTE | ROOT_TRUST, supplied, mapped), 0);
    EXPECT_EQ(mapped, supplied);
    ASSERT_EQ(policy::map_vfs_read_identity(READ | ROOT_TRUST, supplied, mapped), 0);
    EXPECT_EQ(mapped, supplied);
    ASSERT_EQ(supplied.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, supplied), 0);
    ASSERT_EQ(policy::map_compute_identity(COMPUTE, supplied, mapped), 0);
    EXPECT_EQ(mapped.service_privileges(), 3U);
    EXPECT_EQ(mapped.effective_service_privileges(), 0U);
    EXPECT_EQ(mapped.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = security::ALL_AUTHORITY}, mapped), -EPERM);
}

TEST(WkiAuthPolicy, ExplicitReadRequiresIdentityTrustAndCanReleaseItsDescriptor) {
    constexpr auto READ = wki_policy_bits(WkiPolicy::VFS_READ);
    constexpr auto IDENTITY = wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    DevOpReqPayload request{};
    request.op_id = OP_VFS_OPEN_READ_AS;
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, &request, sizeof(request)), READ | IDENTITY);
    request.op_id = OP_VFS_CLOSE;
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, &request, sizeof(request)), 0U);
    request.op_id = OP_VFS_OPEN;
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, &request, sizeof(request)), wki_policy_bits(WkiPolicy::VFS_WRITE));
}

TEST(WkiAuthPolicy, ExplicitReadMapsFullRestrictionsWithoutComputePermission) {
    namespace security = ker::mod::security;
    constexpr auto READ = wki_policy_bits(WkiPolicy::VFS_READ);
    constexpr auto IDENTITY = wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    constexpr auto ROOT_TRUST = wki_policy_bits(WkiPolicy::IDENTITY_ROOT);
    std::array<uint32_t, 2> groups{3000, 3001};
    security::Snapshot supplied;
    ASSERT_EQ(security::Snapshot::restore_fields({.user = {1000, 1001, 1002},
                                                  .group = {2000, 2001, 2002},
                                                  .groups = groups,
                                                  .authority_ceiling = 0,
                                                  .service_privileges = 3,
                                                  .no_new_privileges = true,
                                                  .dumpable = false,
                                                  .service_privileges_locked = true},
                                                 supplied),
              0);
    security::Snapshot mapped;
    auto const BEFORE = mapped;
    for (auto bits : {uint64_t{0}, READ, IDENTITY, READ | ROOT_TRUST, wki_policy_bits(WkiPolicy::COMPUTE) | IDENTITY,
                      READ | IDENTITY | (uint64_t{1} << 63)}) {
        EXPECT_EQ(policy::map_vfs_read_identity(bits, supplied, mapped), -EACCES);
        EXPECT_EQ(mapped, BEFORE);
    }
    ASSERT_EQ(policy::map_vfs_read_identity(READ | IDENTITY, supplied, mapped), 0);
    EXPECT_EQ(mapped, supplied);
    EXPECT_EQ(security::authorize_network_administration(mapped), -EPERM);
    EXPECT_EQ(mapped.prepare({.change = security::Change::SET_EUID, .effective = 0}, mapped), -EPERM);
    EXPECT_EQ(mapped, supplied);
    auto const ROOT = security::Snapshot::bootstrap_root();
    EXPECT_EQ(policy::map_vfs_read_identity(READ | IDENTITY, ROOT, mapped), -EACCES);
    EXPECT_EQ(mapped, supplied);
    EXPECT_EQ(policy::map_vfs_read_identity(READ | IDENTITY | ROOT_TRUST, ROOT, mapped), 0);
    EXPECT_EQ(mapped, ROOT);
}

TEST(WkiAuthPolicy, RetainedRealpathUsesBoundedCanonicalReplyFraming) {
    VfsPathIdentityHeader header{.operation = VfsPathOperation::REALPATH_AT};
    for (uint16_t length : {0U, 1U, 511U, 512U}) {
        header.data_len = length;
        EXPECT_EQ(wki_vfs_path_response_valid(0, sizeof(header) + length, header), length <= 511);
        EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + length + 1, header));
        EXPECT_FALSE(wki_vfs_path_response_valid(-EACCES, sizeof(header) + length, header));
        EXPECT_TRUE(wki_vfs_path_response_valid(-EACCES, 0, header));
    }
    header.data_len = 0;
    header.flags = WKI_VFS_PATH_FOLLOW_FINAL;
    EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header), header));
    EXPECT_EQ(WKI_VFS_FEATURE_REALPATH_AT, uint64_t{128});
}

TEST(WkiAuthPolicy, RealpathRepliesRequireExactCanonicalRelativeText) {
    for (const char* path : {"", "public", "private/public", "...", "a b/c"}) {
        size_t length = std::strlen(path);
        EXPECT_TRUE(wki_vfs_realpath_result_valid(reinterpret_cast<const uint8_t*>(path), length));
        VfsPathIdentityHeader header{
            .operation = VfsPathOperation::REALPATH, .flags = 0, .data_len = static_cast<uint16_t>(length), .reserved = 0};
        EXPECT_TRUE(wki_vfs_path_response_valid(0, sizeof(header) + length, header));
        EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + length + 1, header));
        EXPECT_FALSE(wki_vfs_path_response_valid(-EACCES, sizeof(header) + length, header));
        EXPECT_TRUE(wki_vfs_path_response_valid(-EACCES, 0, header));
        header.reserved = 1;
        EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + length, header));
        header.reserved = 0;
        header.flags = WKI_VFS_PATH_REQUIRE_DIRECTORY;
        EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + length, header));
    }
    for (const char* path : {"/", "/public", ".", "..", "a/.", "a/../b", "a//b", "a/", "./a", "../a"})
        EXPECT_FALSE(wki_vfs_realpath_result_valid(reinterpret_cast<const uint8_t*>(path), std::strlen(path)));
    std::array<uint8_t, 512> bytes{};
    bytes.fill('a');
    EXPECT_TRUE(wki_vfs_realpath_result_valid(bytes.data(), 511));
    EXPECT_FALSE(wki_vfs_realpath_result_valid(bytes.data(), 512));
    bytes[12] = 0;
    EXPECT_FALSE(wki_vfs_realpath_result_valid(bytes.data(), 511));
    EXPECT_FALSE(wki_vfs_realpath_result_valid(nullptr, 1));
}

TEST(WkiAuthPolicy, ChmodMapsWriteIdentityAndValidatesExactFraming) {
    namespace security = ker::mod::security;
    constexpr auto WRITE = wki_policy_bits(WkiPolicy::VFS_WRITE);
    constexpr auto IDENTITY = wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    constexpr auto ROOT = wki_policy_bits(WkiPolicy::IDENTITY_ROOT);
    security::Snapshot supplied, mapped;
    ASSERT_EQ(security::Snapshot::restore_fields({.user = {1001, 1001, 1001}, .group = {2001, 2001, 2001}}, supplied), 0);
    auto const BEFORE = mapped;
    for (auto bits : {uint64_t{0}, WRITE, IDENTITY, WRITE | ROOT, wki_policy_bits(WkiPolicy::VFS_READ) | IDENTITY}) {
        EXPECT_EQ(policy::map_vfs_mutation_identity(bits, supplied, mapped), -EACCES);
        EXPECT_EQ(mapped, BEFORE);
    }
    EXPECT_EQ(policy::map_vfs_mutation_identity(WRITE | IDENTITY, supplied, mapped), 0);
    EXPECT_EQ(mapped, supplied);
    EXPECT_EQ(policy::map_vfs_mutation_identity(WRITE | IDENTITY, security::Snapshot::bootstrap_root(), mapped), -EACCES);
    EXPECT_EQ(mapped, supplied);
    EXPECT_EQ(policy::map_vfs_mutation_identity(WRITE | IDENTITY | ROOT, security::Snapshot::bootstrap_root(), mapped), 0);
    VfsPathIdentityHeader header{.operation = VfsPathOperation::CHMOD, .flags = WKI_VFS_PATH_FOLLOW_FINAL, .data_len = 9, .reserved = 0};
    VfsPathChmodPrefix prefix{.mode = 02750, .directory_fd = -1};
    EXPECT_TRUE(wki_vfs_chmod_request_valid(header, prefix, 9));
    for (size_t n : {size_t{0}, size_t{8}, size_t{10}, size_t{520}}) EXPECT_FALSE(wki_vfs_chmod_request_valid(header, prefix, n));
    header.data_len = sizeof(prefix);
    EXPECT_TRUE(wki_vfs_chmod_request_valid(header, prefix, sizeof(prefix)));
    prefix.directory_fd = 7;
    EXPECT_FALSE(wki_vfs_chmod_request_valid(header, prefix, sizeof(prefix)));
    header.data_len = 9;
    EXPECT_TRUE(wki_vfs_chmod_request_valid(header, prefix, 9));
    prefix.directory_fd = 0;
    EXPECT_FALSE(wki_vfs_chmod_request_valid(header, prefix, 9));
    prefix.directory_fd = -2;
    EXPECT_FALSE(wki_vfs_chmod_request_valid(header, prefix, 9));
    prefix.directory_fd = -1;
    header.reserved = 1;
    EXPECT_FALSE(wki_vfs_chmod_request_valid(header, prefix, 9));
    header.reserved = 0;
    header.flags = 4;
    EXPECT_FALSE(wki_vfs_chmod_request_valid(header, prefix, 9));
    header.flags = 0;
    header.data_len = 0;
    EXPECT_TRUE(wki_vfs_path_response_valid(0, sizeof(header), header));
    EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + 1, header));
    header.data_len = 1;
    EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + 1, header));
    std::array<uint8_t, sizeof(DevOpReqPayload) + security::credential_record::SIZE + sizeof(header) + sizeof(prefix) + 1> request{};
    DevOpReqPayload envelope{.op_id = OP_VFS_PATH_AS, .data_len = static_cast<uint16_t>(request.size() - sizeof(DevOpReqPayload))};
    std::memcpy(request.data(), &envelope, sizeof(envelope));
    std::memcpy(request.data() + sizeof(envelope) + security::credential_record::SIZE, &header, sizeof(header));
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, request.data(), request.size()), WRITE | IDENTITY);
}

TEST(WkiAuthPolicy, MappedUtimensFramingAndWriteIdentity) {
    namespace security = ker::mod::security;
    VfsPathIdentityHeader header{.operation = VfsPathOperation::UTIMENS, .flags = WKI_VFS_PATH_FOLLOW_FINAL, .data_len = 41, .reserved = 0};
    VfsPathUtimensPrefix prefix{.directory_fd = -1};
    EXPECT_TRUE(wki_vfs_utimens_request_valid(header, prefix, 41));
    for (size_t n : {size_t{0}, size_t{39}, size_t{40}, size_t{42}, size_t{552}})
        EXPECT_FALSE(wki_vfs_utimens_request_valid(header, prefix, n));
    header.data_len = 40;
    EXPECT_TRUE(wki_vfs_utimens_request_valid(header, prefix, 40));
    prefix.directory_fd = 7;
    EXPECT_FALSE(wki_vfs_utimens_request_valid(header, prefix, 40));
    header.data_len = 41;
    EXPECT_TRUE(wki_vfs_utimens_request_valid(header, prefix, 41));
    for (int fd : {0, -2}) {
        prefix.directory_fd = fd;
        EXPECT_FALSE(wki_vfs_utimens_request_valid(header, prefix, 41));
    }
    prefix.directory_fd = -1;
    prefix.reserved = 1;
    EXPECT_FALSE(wki_vfs_utimens_request_valid(header, prefix, 41));
    prefix.reserved = 0;
    header.reserved = 1;
    EXPECT_FALSE(wki_vfs_utimens_request_valid(header, prefix, 41));
    header.reserved = 0;
    header.flags = 4;
    EXPECT_FALSE(wki_vfs_utimens_request_valid(header, prefix, 41));
    header.flags = 0;
    header.data_len = 0;
    EXPECT_TRUE(wki_vfs_path_response_valid(0, sizeof(header), header));
    header.data_len = 1;
    EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + 1, header));
    std::array<uint8_t, sizeof(DevOpReqPayload) + security::credential_record::SIZE + sizeof(header) + sizeof(prefix) + 1> request{};
    DevOpReqPayload envelope{.op_id = OP_VFS_PATH_AS, .data_len = static_cast<uint16_t>(request.size() - sizeof(DevOpReqPayload))};
    std::memcpy(request.data(), &envelope, sizeof(envelope));
    std::memcpy(request.data() + sizeof(envelope) + security::credential_record::SIZE, &header, sizeof(header));
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, request.data(), request.size()),
              wki_policy_bits(WkiPolicy::VFS_WRITE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN));
}

TEST(WkiAuthPolicy, MkdirFeaturesAndMappedFraming) {
    namespace security = ker::mod::security;
    VfsFeaturesRequest query{WKI_VFS_FEATURES_VERSION, 0};
    EXPECT_TRUE(wki_vfs_features_request_valid(query, sizeof(query)));
    EXPECT_FALSE(wki_vfs_features_request_valid(query, sizeof(query) + 1));
    query.reserved = 1;
    EXPECT_FALSE(wki_vfs_features_request_valid(query, sizeof(query)));
    query.reserved = 0;
    query.version = 0;
    EXPECT_FALSE(wki_vfs_features_request_valid(query, sizeof(query)));
    VfsFeaturesResponse features{WKI_VFS_FEATURES_VERSION, sizeof(VfsFeaturesResponse), 0, WKI_VFS_FEATURE_MKDIR_IDENTITY};
    EXPECT_TRUE(wki_vfs_features_response_valid(features, sizeof(features)));
    for (size_t n : {size_t{0}, size_t{15}, size_t{17}}) EXPECT_FALSE(wki_vfs_features_response_valid(features, n));
    features.reserved = 1;
    EXPECT_FALSE(wki_vfs_features_response_valid(features, sizeof(features)));
    features.reserved = 0;
    features.version = 2;
    EXPECT_FALSE(wki_vfs_features_response_valid(features, sizeof(features)));
    features.version = WKI_VFS_FEATURES_VERSION;
    features.length = 0;
    EXPECT_FALSE(wki_vfs_features_response_valid(features, sizeof(features)));
    VfsPathIdentityHeader header{
        .operation = VfsPathOperation::MKDIR, .flags = WKI_VFS_PATH_REQUIRE_DIRECTORY, .data_len = 9, .reserved = 0};
    VfsPathMkdirPrefix prefix{.mode = 0750, .directory_fd = -1};
    EXPECT_TRUE(wki_vfs_mkdir_request_valid(header, prefix, 9));
    for (size_t n : {size_t{0}, size_t{8}, size_t{10}, size_t{520}}) EXPECT_FALSE(wki_vfs_mkdir_request_valid(header, prefix, n));
    header.data_len = 8;
    EXPECT_TRUE(wki_vfs_mkdir_request_valid(header, prefix, 8));
    prefix.directory_fd = 7;
    EXPECT_FALSE(wki_vfs_mkdir_request_valid(header, prefix, 8));
    header.data_len = 9;
    EXPECT_TRUE(wki_vfs_mkdir_request_valid(header, prefix, 9));
    for (int fd : {0, -2}) {
        prefix.directory_fd = fd;
        EXPECT_FALSE(wki_vfs_mkdir_request_valid(header, prefix, 9));
    }
    prefix.directory_fd = -1;
    header.flags = WKI_VFS_PATH_FOLLOW_FINAL;
    EXPECT_FALSE(wki_vfs_mkdir_request_valid(header, prefix, 9));
    header.flags = 0;
    header.reserved = 1;
    EXPECT_FALSE(wki_vfs_mkdir_request_valid(header, prefix, 9));
    header.reserved = 0;
    header.data_len = 0;
    EXPECT_TRUE(wki_vfs_path_response_valid(0, sizeof(header), header));
    header.data_len = 1;
    EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + 1, header));
    std::array<uint8_t, sizeof(DevOpReqPayload) + security::credential_record::SIZE + sizeof(header) + sizeof(prefix) + 1> request{};
    DevOpReqPayload envelope{.op_id = OP_VFS_PATH_AS, .data_len = static_cast<uint16_t>(request.size() - sizeof(DevOpReqPayload))};
    std::memcpy(request.data(), &envelope, sizeof(envelope));
    std::memcpy(request.data() + sizeof(envelope) + security::credential_record::SIZE, &header, sizeof(header));
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, request.data(), request.size()),
              wki_policy_bits(WkiPolicy::VFS_WRITE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN));
    envelope.op_id = OP_VFS_FEATURES;
    std::memcpy(request.data(), &envelope, sizeof(envelope));
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, request.data(), request.size()), wki_policy_bits(WkiPolicy::VFS_WRITE));
}

TEST(WkiAuthPolicy, SymlinkBoundsIndependentTargetAndDestinationAndMapsWriteIdentity) {
    namespace security = ker::mod::security;
    VfsPathIdentityHeader header{.operation = VfsPathOperation::SYMLINK, .flags = 0, .data_len = 12, .reserved = 0};
    VfsPathSymlinkPrefix prefix{.directory_fd = -1, .target_length = 3, .reserved = 0};
    EXPECT_TRUE(wki_vfs_symlink_request_valid(header, prefix, 12));
    for (size_t length : {size_t{0}, size_t{7}, size_t{11}, size_t{13}, size_t{1031}})
        EXPECT_FALSE(wki_vfs_symlink_request_valid(header, prefix, length));
    header.data_len = 11;
    EXPECT_TRUE(wki_vfs_symlink_request_valid(header, prefix, 11));
    prefix.directory_fd = 7;
    EXPECT_FALSE(wki_vfs_symlink_request_valid(header, prefix, 11));
    header.data_len = 12;
    EXPECT_TRUE(wki_vfs_symlink_request_valid(header, prefix, 12));
    for (int fd : {0, -2}) {
        prefix.directory_fd = fd;
        EXPECT_FALSE(wki_vfs_symlink_request_valid(header, prefix, 12));
    }
    prefix.directory_fd = -1;
    prefix.target_length = 0;
    header.data_len = 9;
    EXPECT_TRUE(wki_vfs_symlink_request_valid(header, prefix, 9));
    prefix.target_length = 511;
    header.data_len = 1030;
    EXPECT_TRUE(wki_vfs_symlink_request_valid(header, prefix, 1030));
    prefix.target_length = 512;
    EXPECT_FALSE(wki_vfs_symlink_request_valid(header, prefix, 1030));
    prefix.target_length = 510;
    EXPECT_FALSE(wki_vfs_symlink_request_valid(header, prefix, 1030));
    prefix.target_length = 511;
    header.flags = WKI_VFS_PATH_REQUIRE_DIRECTORY;
    EXPECT_TRUE(wki_vfs_symlink_request_valid(header, prefix, 1030));
    header.flags = WKI_VFS_PATH_FOLLOW_FINAL;
    EXPECT_FALSE(wki_vfs_symlink_request_valid(header, prefix, 1030));
    header.flags = 0;
    prefix.reserved = 1;
    EXPECT_FALSE(wki_vfs_symlink_request_valid(header, prefix, 1030));
    prefix.reserved = 0;
    header.reserved = 1;
    EXPECT_FALSE(wki_vfs_symlink_request_valid(header, prefix, 1030));
    header.reserved = 0;
    header.data_len = 0;
    EXPECT_TRUE(wki_vfs_path_response_valid(0, sizeof(header), header));
    header.data_len = 1;
    EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + 1, header));
    std::array<uint8_t, sizeof(DevOpReqPayload) + security::credential_record::SIZE + sizeof(header) + sizeof(prefix) + 1> request{};
    DevOpReqPayload envelope{.op_id = OP_VFS_PATH_AS, .data_len = static_cast<uint16_t>(request.size() - sizeof(DevOpReqPayload))};
    std::memcpy(request.data(), &envelope, sizeof(envelope));
    std::memcpy(request.data() + sizeof(envelope) + security::credential_record::SIZE, &header, sizeof(header));
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, request.data(), request.size()),
              wki_policy_bits(WkiPolicy::VFS_WRITE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN));
}

TEST(WkiAuthPolicy, RemoveValidatesDirectoryIntentAndRequiresMappedWrite) {
    namespace security = ker::mod::security;
    VfsPathIdentityHeader header{.operation = VfsPathOperation::REMOVE, .flags = 0, .data_len = 9, .reserved = 0};
    VfsPathRemovePrefix prefix{.directory_fd = -1, .flags = 0, .reserved = 0};
    for (uint16_t flags : {uint16_t{0}, WKI_VFS_REMOVE_DIRECTORY}) {
        prefix.flags = flags;
        EXPECT_TRUE(wki_vfs_remove_request_valid(header, prefix, 9));
        header.flags = WKI_VFS_PATH_REQUIRE_DIRECTORY;
        EXPECT_TRUE(wki_vfs_remove_request_valid(header, prefix, 9));
        header.flags = WKI_VFS_PATH_FOLLOW_FINAL;
        EXPECT_FALSE(wki_vfs_remove_request_valid(header, prefix, 9));
        header.flags = 0;
    }
    for (size_t length : {size_t{0}, size_t{7}, size_t{8}, size_t{10}, size_t{520}})
        EXPECT_FALSE(wki_vfs_remove_request_valid(header, prefix, length));
    header.data_len = 8;
    EXPECT_TRUE(wki_vfs_remove_request_valid(header, prefix, 8));
    prefix.directory_fd = 7;
    EXPECT_FALSE(wki_vfs_remove_request_valid(header, prefix, 8));
    header.data_len = 9;
    EXPECT_TRUE(wki_vfs_remove_request_valid(header, prefix, 9));
    for (int fd : {0, -2}) {
        prefix.directory_fd = fd;
        EXPECT_FALSE(wki_vfs_remove_request_valid(header, prefix, 9));
    }
    prefix.directory_fd = -1;
    header.data_len = 519;
    EXPECT_TRUE(wki_vfs_remove_request_valid(header, prefix, 519));
    header.data_len = 520;
    EXPECT_FALSE(wki_vfs_remove_request_valid(header, prefix, 520));
    header.data_len = 9;
    prefix.flags = 2;
    EXPECT_FALSE(wki_vfs_remove_request_valid(header, prefix, 9));
    prefix.flags = 0;
    prefix.reserved = 1;
    EXPECT_FALSE(wki_vfs_remove_request_valid(header, prefix, 9));
    prefix.reserved = 0;
    header.reserved = 1;
    EXPECT_FALSE(wki_vfs_remove_request_valid(header, prefix, 9));
    header.reserved = 0;
    header.data_len = 0;
    EXPECT_TRUE(wki_vfs_path_response_valid(0, sizeof(header), header));
    header.data_len = 1;
    EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + 1, header));
    std::array<uint8_t, sizeof(DevOpReqPayload) + security::credential_record::SIZE + sizeof(header) + sizeof(prefix) + 1> request{};
    DevOpReqPayload envelope{.op_id = OP_VFS_PATH_AS, .data_len = static_cast<uint16_t>(request.size() - sizeof(DevOpReqPayload))};
    std::memcpy(request.data(), &envelope, sizeof(envelope));
    std::memcpy(request.data() + sizeof(envelope) + security::credential_record::SIZE, &header, sizeof(header));
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, request.data(), request.size()),
              wki_policy_bits(WkiPolicy::VFS_WRITE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN));
}

TEST(WkiAuthPolicy, RenameBindsBothDirectoriesAndBoundsBothPaths) {
    namespace security = ker::mod::security;
    VfsPathIdentityHeader header{.operation = VfsPathOperation::RENAME, .flags = 0, .data_len = 30, .reserved = 0};
    VfsPathRenamePrefix prefix{.source = {-1, 0, 0, 0}, .destination = {-1, 0, 0, 0}, .source_length = 1, .flags = 0};
    EXPECT_TRUE(wki_vfs_rename_request_valid(header, prefix, 30));
    for (size_t n : {size_t{0}, size_t{27}, size_t{29}, size_t{31}, size_t{1051}})
        EXPECT_FALSE(wki_vfs_rename_request_valid(header, prefix, n));
    for (uint16_t flags : {0U, 1U, 2U, 3U}) {
        prefix.flags = flags;
        EXPECT_TRUE(wki_vfs_rename_request_valid(header, prefix, 30));
    }
    prefix.flags = 4;
    EXPECT_FALSE(wki_vfs_rename_request_valid(header, prefix, 30));
    prefix.flags = 0;
    header.flags = 1;
    EXPECT_FALSE(wki_vfs_rename_request_valid(header, prefix, 30));
    header.flags = 0;
    header.reserved = 1;
    EXPECT_FALSE(wki_vfs_rename_request_valid(header, prefix, 30));
    header.reserved = 0;
    for (auto* reference : {&prefix.source, &prefix.destination}) {
        *reference = {7, 42, 0, 123};
        EXPECT_TRUE(wki_vfs_rename_request_valid(header, prefix, 30));
        reference->generation = 0;
        EXPECT_FALSE(wki_vfs_rename_request_valid(header, prefix, 30));
        reference->generation = 123;
        reference->channel_id = 0;
        EXPECT_FALSE(wki_vfs_rename_request_valid(header, prefix, 30));
        reference->channel_id = 42;
        reference->reserved = 1;
        EXPECT_FALSE(wki_vfs_rename_request_valid(header, prefix, 30));
        reference->reserved = 0;
        reference->fd = 0;
        EXPECT_FALSE(wki_vfs_rename_request_valid(header, prefix, 30));
        *reference = {-1, 0, 0, 0};
    }
    header.data_len = 28;
    prefix.source_length = 0;
    EXPECT_TRUE(wki_vfs_rename_request_valid(header, prefix, 28));
    prefix.source = {7, 42, 0, 123};
    EXPECT_FALSE(wki_vfs_rename_request_valid(header, prefix, 28));
    prefix.source = {-1, 0, 0, 0};
    prefix.destination = {7, 42, 0, 123};
    EXPECT_FALSE(wki_vfs_rename_request_valid(header, prefix, 28));
    prefix.destination = {-1, 0, 0, 0};
    header.data_len = 1050;
    prefix.source_length = 511;
    EXPECT_TRUE(wki_vfs_rename_request_valid(header, prefix, 1050));
    prefix.source_length = 512;
    EXPECT_FALSE(wki_vfs_rename_request_valid(header, prefix, 1050));
    prefix.source_length = 510;
    EXPECT_FALSE(wki_vfs_rename_request_valid(header, prefix, 1050));
    std::array<uint8_t, sizeof(DevOpReqPayload) + security::credential_record::SIZE + sizeof(header)> bytes{};
    DevOpReqPayload envelope{.op_id = OP_VFS_PATH_AS, .data_len = static_cast<uint16_t>(bytes.size() - sizeof(DevOpReqPayload))};
    std::memcpy(bytes.data(), &envelope, sizeof(envelope));
    for (auto op : {VfsPathOperation::RENAME, VfsPathOperation::DIRECTORY_BINDING}) {
        header.operation = op;
        header.data_len = op == VfsPathOperation::RENAME ? 0 : sizeof(VfsRenameDirectoryRef);
        EXPECT_TRUE(wki_vfs_path_response_valid(0, sizeof(header) + header.data_len, header));
        EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + header.data_len + 1, header));
        std::memcpy(bytes.data() + sizeof(envelope) + security::credential_record::SIZE, &header, sizeof(header));
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()),
                  wki_policy_bits(WkiPolicy::VFS_WRITE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN));
    }
}

TEST(WkiAuthPolicy, DirectoryReferencesMapCallerAndCancelOnlyExistingOwnership) {
    std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(VfsDirectoryRequest) + WKI_VFS_DIRECTORY_CALLER_SIZE + 512> bytes{};
    VfsDirectoryRequest request{.token = 7, .path_length = 3};
    DevOpReqPayload envelope{.op_id = OP_VFS_DIRECTORY_REFERENCE};
    auto permissions = [&](size_t length) {
        envelope.data_len = static_cast<uint16_t>(length);
        std::memcpy(bytes.data(), &envelope, sizeof(envelope));
        std::memcpy(bytes.data() + sizeof(envelope), &request, sizeof(request));
        return policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), static_cast<uint16_t>(sizeof(envelope) + length));
    };
    constexpr size_t LENGTH = sizeof(request) + WKI_VFS_DIRECTORY_CALLER_SIZE + 3;
    constexpr auto REQUIRED = wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    EXPECT_EQ(permissions(LENGTH), REQUIRED);
    for (size_t i = 0; i <= LENGTH + 1; ++i)
        if (i != LENGTH) EXPECT_EQ(permissions(i), policy::INVALID);
    for (int32_t fd : {-2, 0}) {
        request.directory_fd = fd;
        EXPECT_EQ(permissions(LENGTH), policy::INVALID);
    }
    request.directory_fd = 8;
    EXPECT_EQ(permissions(LENGTH), REQUIRED);
    request.operation = VfsDirectoryOperation::PATH;
    EXPECT_EQ(permissions(LENGTH), policy::INVALID);
    request.path_length = 0;
    EXPECT_EQ(permissions(LENGTH - 3), REQUIRED);
    request.operation = VfsDirectoryOperation::CANCEL;
    EXPECT_EQ(permissions(sizeof(request)), policy::INVALID);
    request.directory_fd = 0;
    EXPECT_EQ(permissions(sizeof(request)), 0U);
    EXPECT_EQ(permissions(sizeof(request) + 1), policy::INVALID);
    request.token = 0;
    EXPECT_EQ(permissions(sizeof(request)), policy::INVALID);
    request.token = 7;
    request.reserved = 1;
    EXPECT_EQ(permissions(sizeof(request)), policy::INVALID);
    request.reserved = 0;
    request.reserved2 = 1;
    EXPECT_EQ(permissions(sizeof(request)), policy::INVALID);
    request.reserved2 = 0;
    request.version++;
    EXPECT_EQ(permissions(sizeof(request)), policy::INVALID);
}

TEST(WkiAuthPolicy, DirectoryReferenceRepliesAreExactAndCanonical) {
    VfsDirectoryResponse response{.fd = 8, .token = 7, .path_length = 2};
    std::array<uint8_t, 4> path{'/', 'x', '/', 0};
    EXPECT_TRUE(wki_vfs_directory_response_valid(response, 7, path.data(), sizeof(response) + 2));
    EXPECT_FALSE(wki_vfs_directory_response_valid(response, 8, path.data(), sizeof(response) + 2));
    EXPECT_FALSE(wki_vfs_directory_response_valid(response, 7, nullptr, sizeof(response) + 2));
    response.path_length = 1;
    EXPECT_TRUE(wki_vfs_directory_response_valid(response, 7, path.data(), sizeof(response) + 1));
    response.path_length = 3;
    EXPECT_FALSE(wki_vfs_directory_response_valid(response, 7, path.data(), sizeof(response) + 3));
    path = {'/', '.', '.', 0};
    EXPECT_FALSE(wki_vfs_directory_response_valid(response, 7, path.data(), sizeof(response) + 3));
    response.path_length = 1;
    for (size_t length = 0; length <= sizeof(response) + 512; ++length) {
        EXPECT_EQ(wki_vfs_directory_reply_size_valid(0, length, sizeof(response) + 511),
                  length > sizeof(response) && length <= sizeof(response) + 511);
        EXPECT_EQ(wki_vfs_directory_reply_size_valid(-EACCES, length, sizeof(response) + 511), length == 0);
        EXPECT_EQ(wki_vfs_directory_reply_size_valid(0, length, 0), length == 0);
        EXPECT_FALSE(wki_vfs_directory_reply_size_valid(1, length, sizeof(response) + 511));
    }
    for (auto* field : {&response.reserved, &response.reserved2}) {
        *field = 1;
        EXPECT_FALSE(wki_vfs_directory_response_valid(response, 7, path.data(), sizeof(response) + 1));
        *field = 0;
    }
    response.reserved3 = 1;
    EXPECT_FALSE(wki_vfs_directory_response_valid(response, 7, path.data(), sizeof(response) + 1));
}

TEST(WkiAuthPolicy, DescriptorMetadataHasFixedBodiesAndRequiresMappedWrite) {
    auto check = [](auto body, VfsPathOperation operation, auto valid) {
        VfsPathIdentityHeader header{.operation = operation, .data_len = sizeof(body)};
        EXPECT_TRUE(valid(header, body, sizeof(body)));
        for (size_t size = 0; size <= sizeof(body) + 2; ++size) EXPECT_EQ(valid(header, body, size), size == sizeof(body));
        for (unsigned bit = 0; bit < 16; ++bit) {
            header.flags = uint16_t{1} << bit;
            EXPECT_FALSE(valid(header, body, sizeof(body)));
            header.flags = 0;
            header.reserved = uint16_t{1} << bit;
            EXPECT_FALSE(valid(header, body, sizeof(body)));
            header.reserved = 0;
        }
        for (int32_t fd : {INT32_MIN, -1, 0}) {
            body.fd = fd;
            EXPECT_FALSE(valid(header, body, sizeof(body)));
        }
        body.fd = INT32_MAX;
        EXPECT_TRUE(valid(header, body, sizeof(body)));
        header.operation = VfsPathOperation::CHMOD;
        EXPECT_FALSE(valid(header, body, sizeof(body)));
        header.operation = operation;
        header.data_len = sizeof(body) - 1;
        EXPECT_FALSE(valid(header, body, sizeof(body)));
        header.data_len = sizeof(body) + 1;
        EXPECT_FALSE(valid(header, body, sizeof(body)));
        header.data_len = 0;
        EXPECT_TRUE(wki_vfs_path_response_valid(0, sizeof(header), header));
        EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + 1, header));
        header.data_len = 1;
        EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + 1, header));
        EXPECT_TRUE(wki_vfs_path_response_valid(-EPERM, 0, header));
        EXPECT_FALSE(wki_vfs_path_response_valid(-EPERM, sizeof(header), header));
        std::array<uint8_t, sizeof(DevOpReqPayload) + ker::mod::security::credential_record::SIZE + sizeof(header)> bytes{};
        DevOpReqPayload envelope{.op_id = OP_VFS_PATH_AS, .data_len = bytes.size() - sizeof(DevOpReqPayload)};
        std::memcpy(bytes.data(), &envelope, sizeof(envelope));
        std::memcpy(bytes.data() + sizeof(envelope) + ker::mod::security::credential_record::SIZE, &header, sizeof(header));
        auto const REQUIRED = wki_policy_bits(WkiPolicy::VFS_WRITE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), REQUIRED);
        for (size_t length = 0; length < bytes.size(); ++length)
            EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), length), policy::INVALID);
    };
    check(VfsFileChmodRequest{.fd = 7, .mode = 02750}, VfsPathOperation::FCHMOD, wki_vfs_fchmod_request_valid);
    check(VfsFileChownRequest{.fd = 7, .owner = UINT32_MAX, .group = 2002}, VfsPathOperation::FCHOWN, wki_vfs_fchown_request_valid);
    check(VfsFileUtimensRequest{.fd = 7}, VfsPathOperation::FUTIMENS, wki_vfs_futimens_request_valid);
    VfsPathIdentityHeader header{.operation = VfsPathOperation::FUTIMENS, .data_len = sizeof(VfsFileUtimensRequest)};
    VfsFileUtimensRequest body{.fd = 7, .reserved = 1};
    EXPECT_FALSE(wki_vfs_futimens_request_valid(header, body, sizeof(body)));
    EXPECT_EQ(WKI_VFS_FEATURE_FILE_METADATA_IDENTITY, uint64_t{256});
}

TEST(WkiAuthPolicy, PathChownKeepsIdentityDirectoryAndExactFraming) {
    using namespace ker::net::wki;
    namespace policy = ker::net::wki::auth_policy;
    VfsPathChownPrefix prefix{.owner = UINT32_MAX, .group = 2002, .directory_fd = -1};
    for (size_t path_length : {size_t{0}, size_t{1}, size_t{511}, size_t{512}}) {
        VfsPathIdentityHeader header{.operation = VfsPathOperation::CHOWN, .data_len = static_cast<uint16_t>(sizeof(prefix) + path_length)};
        for (int fd : {-2, -1, 0, 1, INT32_MAX}) {
            prefix.directory_fd = fd;
            bool const valid = path_length <= 511 && (fd == -1 || (fd > 0 && path_length > 0));
            EXPECT_EQ(wki_vfs_chown_request_valid(header, prefix, header.data_len), valid);
            EXPECT_FALSE(wki_vfs_chown_request_valid(header, prefix, header.data_len + 1));
            EXPECT_FALSE(wki_vfs_chown_request_valid(header, prefix, header.data_len - 1));
        }
    }
    prefix.directory_fd = 7;
    VfsPathIdentityHeader header{.operation = VfsPathOperation::CHOWN, .data_len = sizeof(prefix) + 1};
    for (unsigned flag = 0; flag < 16; ++flag) {
        header.flags = uint16_t{1} << flag;
        EXPECT_EQ(wki_vfs_chown_request_valid(header, prefix, header.data_len), flag < 2);
        header.flags = 0;
        header.reserved = uint16_t{1} << flag;
        EXPECT_FALSE(wki_vfs_chown_request_valid(header, prefix, header.data_len));
        header.reserved = 0;
    }
    constexpr size_t RECORD = 80;
    std::array<uint8_t, sizeof(DevOpReqPayload) + RECORD + sizeof(header) + sizeof(prefix) + 1> bytes{};
    DevOpReqPayload request{.op_id = OP_VFS_PATH_AS, .data_len = static_cast<uint16_t>(bytes.size() - sizeof(DevOpReqPayload))};
    std::memcpy(bytes.data(), &request, sizeof(request));
    std::memcpy(bytes.data() + sizeof(request) + RECORD, &header, sizeof(header));
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()),
              wki_policy_bits(WkiPolicy::VFS_WRITE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN));
    header.data_len = 0;
    EXPECT_TRUE(wki_vfs_path_response_valid(0, sizeof(header), header));
    EXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + 1, header));
    EXPECT_EQ(static_cast<uint16_t>(VfsPathOperation::CHOWN), 17);
    EXPECT_EQ(sizeof(prefix), 12U);
    EXPECT_EQ(offsetof(VfsPathChownPrefix, directory_fd), 8U);
    EXPECT_EQ(WKI_VFS_FEATURE_CHOWN_IDENTITY, 512U);
}

TEST(WkiAuthPolicy, NetOpenRequiresFramedCallerAndConfiguredIdentityTrust) {
    using namespace ker::mod::security;
    std::array<uint8_t, sizeof(DevOpReqPayload) + WKI_NET_OPEN_IDENTITY_SIZE> bytes{};
    DevOpReqPayload request{.op_id = OP_NET_OPEN_AS, .data_len = WKI_NET_OPEN_IDENTITY_SIZE};
    std::memcpy(bytes.data(), &request, sizeof(request));
    constexpr auto BASE = wki_policy_bits(WkiPolicy::REMOTE_NET) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), BASE);
    for (size_t size = 0; size < bytes.size(); ++size)
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), size), policy::INVALID);
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size() + 1), policy::INVALID);
    request.op_id = OP_NET_CLOSE_AS;
    std::memcpy(bytes.data(), &request, sizeof(request));
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), BASE);
    for (size_t size = 0; size < bytes.size(); ++size)
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), size), policy::INVALID);
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size() + 1), policy::INVALID);
    EXPECT_EQ(sizeof(NetCloseIdentityHeader), 8U);
    EXPECT_EQ(WKI_NET_CLOSE_IDENTITY_SIZE, 88U);
    EXPECT_EQ(DEV_ATTACH_NET_CLOSE_IDENTITY & DEV_ATTACH_NET_OPEN_IDENTITY, 0U);
    auto root = Snapshot::bootstrap_root();
    Snapshot mapped;
    auto const UNCHANGED = mapped;
    EXPECT_EQ(policy::map_net_control_identity(BASE, root, mapped), -EACCES);
    EXPECT_EQ(mapped, UNCHANGED);
    constexpr auto ROOT_POLICY = BASE | wki_policy_bits(WkiPolicy::IDENTITY_ROOT);
    EXPECT_EQ(policy::map_net_control_identity(ROOT_POLICY, root, mapped), 0);
    EXPECT_EQ(mapped, root);
    Snapshot service;
    constexpr std::array<uint32_t, 2> GROUPS{117, 119};
    ASSERT_EQ(Snapshot::restore_fields({.user = {102, 102, 102},
                                        .group = {102, 102, 102},
                                        .groups = GROUPS,
                                        .authority_ceiling = ALL_AUTHORITY,
                                        .service_privileges = 2,
                                        .no_new_privileges = true,
                                        .dumpable = false,
                                        .service_privileges_locked = true},
                                       service),
              0);
    EXPECT_EQ(policy::map_net_control_identity(BASE, service, mapped), -EACCES);
    EXPECT_EQ(mapped, root);
    EXPECT_EQ(policy::map_net_control_identity(ROOT_POLICY, service, mapped), 0);
    EXPECT_EQ(mapped, service);
    EXPECT_EQ(policy::map_net_control_identity(wki_policy_bits(WkiPolicy::REMOTE_NET), service, mapped), -EACCES);
    EXPECT_EQ(mapped, service);
    EXPECT_EQ(policy::map_net_control_identity(ROOT_POLICY | (uint64_t{1} << 63), service, mapped), -EACCES);
    EXPECT_EQ(mapped, service);
}

TEST(WkiAuthPolicy, CombinedNetLinkHasIndependentSupportAndExactFraming) {
    std::array<uint8_t, sizeof(DevOpReqPayload) + WKI_NET_LINK_IDENTITY_SIZE> bytes{};
    DevOpReqPayload request{.op_id = OP_NET_LINK_AS, .data_len = WKI_NET_LINK_IDENTITY_SIZE};
    std::memcpy(bytes.data(), &request, sizeof(request));
    constexpr auto POLICY = wki_policy_bits(WkiPolicy::REMOTE_NET) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), POLICY);
    for (size_t size = 0; size < bytes.size(); ++size)
        EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), size), policy::INVALID);
    EXPECT_EQ(policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size() + 1), policy::INVALID);
    EXPECT_EQ(sizeof(NetLinkControlPayload), 16U);
    EXPECT_EQ(WKI_NET_LINK_IDENTITY_SIZE, 104U);
    EXPECT_EQ(DEV_ATTACH_NET_LINK_IDENTITY & (DEV_ATTACH_NET_OPEN_IDENTITY | DEV_ATTACH_NET_CLOSE_IDENTITY), 0U);
}

TEST(WkiAuthPolicy, UserSignalRequiresComputeIdentityAndReplyHasNoRequestAuthority) {
    TaskSignalRequest request{};
    EXPECT_EQ(policy::required_permissions(MsgType::TASK_SIGNAL_REQ, &request, sizeof(request)),
              wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN));
    TaskSignalResponse response{};
    EXPECT_EQ(policy::required_permissions(MsgType::TASK_SIGNAL_RESP, &response, sizeof(response)), 0u);
    GroupSignalRequest group{};
    for (auto operation : {GroupSignalOperation::QUERY, GroupSignalOperation::DELIVER}) {
        group.operation = static_cast<uint8_t>(operation);
        EXPECT_EQ(policy::required_permissions(MsgType::GROUP_SIGNAL_REQ, &group, sizeof(group)),
                  wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN));
    }
    GroupSignalResponse group_response{};
    EXPECT_EQ(policy::required_permissions(MsgType::GROUP_SIGNAL_RESP, &group_response, sizeof(group_response)), 0u);
    TaskCancelPayload cancel{};
    EXPECT_EQ(policy::required_permissions(MsgType::TASK_CANCEL, &cancel, sizeof(cancel)), 0u);
}

TEST(WkiAuthPolicy, OrdinaryOpenLifetimeKeepsOperationPolicyAndExactFraming) {
    constexpr size_t RECORD = ker::mod::security::credential_record::SIZE;
    std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(VfsOpenLifetimeRequest) + RECORD + 64> bytes{};
    DevOpReqPayload envelope{.op_id = OP_VFS_OPEN_LIFETIME};
    VfsOpenLifetimeRequest request{.token = 7, .binding_id = 9, .owner_boot_epoch = 1};
    auto permissions = [&](size_t length) {
        std::memcpy(bytes.data(), &envelope, sizeof(envelope));
        std::memcpy(bytes.data() + sizeof(envelope), &request, sizeof(request));
        return policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), static_cast<uint16_t>(length));
    };
    size_t const PREFIX = sizeof(envelope) + sizeof(request);
    VfsOpenLifetimeRequest query{.inner_op = OP_VFS_FEATURES, .token = 31};
    EXPECT_TRUE(wki_vfs_open_lifetime_request_valid(query, sizeof(query)));
    auto const BOUND_CANCEL = request;
    request = query;
    envelope.data_len = sizeof(request);
    EXPECT_EQ(permissions(PREFIX), 0U);
    request.binding_id = 1;
    EXPECT_EQ(permissions(PREFIX), policy::INVALID);
    request = BOUND_CANCEL;
    request.binding_id = 0;
    EXPECT_EQ(permissions(PREFIX), policy::INVALID);
    request = BOUND_CANCEL;
    request.owner_boot_epoch = 0;
    EXPECT_EQ(permissions(PREFIX), policy::INVALID);
    request = BOUND_CANCEL;
    request.reserved2 = 1;
    EXPECT_EQ(permissions(PREFIX), policy::INVALID);
    request = BOUND_CANCEL;
    query.binding_id = 17;
    query.owner_boot_epoch = 3;
    EXPECT_FALSE(wki_vfs_open_lifetime_request_valid(query, sizeof(query)));
    EXPECT_TRUE(wki_vfs_open_binding_response_valid(query, 31, sizeof(query)));
    EXPECT_FALSE(wki_vfs_open_binding_response_valid(query, 32, sizeof(query)));
    EXPECT_FALSE(wki_vfs_open_binding_response_valid(query, 31, sizeof(query) - 1));
    query.reserved2 = 1;
    EXPECT_FALSE(wki_vfs_open_binding_response_valid(query, 31, sizeof(query)));

    envelope.data_len = sizeof(request);
    EXPECT_EQ(permissions(PREFIX), 0U);  // Cancellation grants no new authority.
    for (size_t length = 0; length < PREFIX; ++length) EXPECT_EQ(permissions(length), policy::INVALID);
    EXPECT_EQ(permissions(PREFIX + 1), policy::INVALID);
    request.token = 0;
    EXPECT_EQ(permissions(PREFIX), policy::INVALID);
    request.token = 7;
    request.version++;
    EXPECT_EQ(permissions(PREFIX), policy::INVALID);
    request.version = WKI_VFS_OPEN_LIFETIME_VERSION;
    request.reserved = 1;
    EXPECT_EQ(permissions(PREFIX), policy::INVALID);
    request.reserved = 0;
    for (uint16_t op : {OP_VFS_OPEN_AS, OP_VFS_OPEN_READ_AS, OP_VFS_PATH_AS}) {
        request.inner_op = op;
        size_t const EXTRA = op == OP_VFS_PATH_AS ? sizeof(VfsPathIdentityHeader) + sizeof(int32_t) : 0;
        size_t const FLAGS_OFFSET = PREFIX + RECORD + EXTRA;
        request.data_len = RECORD + EXTRA + sizeof(uint32_t);
        envelope.data_len = sizeof(request) + request.data_len;
        size_t const LENGTH = sizeof(envelope) + envelope.data_len;
        VfsPathIdentityHeader path{.operation = VfsPathOperation::OPEN_AT};
        if (op == OP_VFS_PATH_AS) std::memcpy(bytes.data() + PREFIX + RECORD, &path, sizeof(path));
        for (uint32_t flags : {0U, 1U, 2U, 0x40U, 0x200U, 0xffffffffU}) {
            std::memcpy(bytes.data() + FLAGS_OFFSET, &flags, sizeof(flags));
            auto const EXPECTED = op == OP_VFS_OPEN_READ_AS && flags != 0 ? policy::INVALID : policy::vfs_open_permissions(flags);
            EXPECT_EQ(permissions(LENGTH), EXPECTED);
        }
        uint32_t flags = 0;
        std::memcpy(bytes.data() + FLAGS_OFFSET, &flags, sizeof(flags));
        for (size_t length = PREFIX; length < LENGTH; ++length) EXPECT_EQ(permissions(length), policy::INVALID);
        EXPECT_EQ(permissions(LENGTH + 1), policy::INVALID);
        if (op == OP_VFS_PATH_AS) {
            path.operation = VfsPathOperation::CHMOD;
            std::memcpy(bytes.data() + PREFIX + RECORD, &path, sizeof(path));
            EXPECT_EQ(permissions(LENGTH), policy::INVALID);
        }
    }
    request.inner_op = OP_VFS_CLOSE;
    EXPECT_FALSE(wki_vfs_open_lifetime_request_valid(request, sizeof(request) + request.data_len));
    request.inner_op = 0;
    EXPECT_FALSE(wki_vfs_open_lifetime_request_valid(request, sizeof(request) + request.data_len));
}

TEST(WkiAuthPolicy, TerminalSignalsRequireComputeAndIdentityPolicy) {
    EXPECT_EQ(policy::required_permissions(MsgType::TASK_TERMINAL_SIGNAL, nullptr, 0),
              wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN));
}

TEST(WkiAuthPolicy, ParentDeathRequiresComputeAndIdentityPolicy) {
    EXPECT_EQ(policy::required_permissions(MsgType::TASK_PARENT_DEATH, nullptr, 0),
              wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN));
}

TEST(WkiAuthPolicy, ChildSignalsRequireComputeAndIdentityPolicy) {
    EXPECT_EQ(policy::required_permissions(MsgType::TASK_CHILD_SIGNAL, nullptr, 0),
              wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN));
}
