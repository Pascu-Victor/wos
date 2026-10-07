#include <gtest/gtest.h>

#include <cerrno>
#include <cstddef>
#include <net/wki/wire.hpp>
#include <vfs/coherency.hpp>

using namespace ker::net::wki;
namespace coherence = ker::vfs::coherency;

namespace {
constexpr VfsOwnerFdRequest REQUEST{.fd = 7, .open_token = 13, .binding_id = 19, .owner_boot_epoch = 23, .query_id = 29};
constexpr VfsVersionResponse RESPONSE{.query_id = 29, .object = 31, .content = 37, .metadata = 43, .directory = 41};
}  // namespace

TEST(WkiVfsCoherencyWire, AppendRequestHasExactAuthorityAndBoundedData) {
    VfsAppendRequest request{.owner = REQUEST, .data_len = WKI_VFS_APPEND_MAX};
    ASSERT_TRUE(wki_vfs_append_request_valid(request, sizeof(request) + request.data_len));
    EXPECT_EQ(sizeof(request), 48U);
    EXPECT_EQ(sizeof(VfsAppendResponse), 32U);
    EXPECT_FALSE(wki_vfs_append_request_valid(request, sizeof(request) + request.data_len - 1));
    EXPECT_FALSE(wki_vfs_append_request_valid(request, sizeof(request) + request.data_len + 1));
    ++request.data_len;
    EXPECT_FALSE(wki_vfs_append_request_valid(request, sizeof(request) + request.data_len));
    request.data_len = 0;
    ASSERT_TRUE(wki_vfs_append_request_valid(request, sizeof(request)));
    request.reserved = 1;
    EXPECT_FALSE(wki_vfs_append_request_valid(request, sizeof(request)));
    request.reserved = 0;
    request.owner.open_token = 0;
    EXPECT_FALSE(wki_vfs_append_request_valid(request, sizeof(request)));
}

TEST(WkiVfsCoherencyWire, AppendReplyPreservesExactOffsetPrefixAndErrno) {
    VfsAppendResponse reply{.operation = 29, .offset = 41, .bytes = 7};
    ASSERT_TRUE(wki_vfs_append_response_valid(reply, 29, 20, sizeof(reply)));
    EXPECT_FALSE(wki_vfs_append_response_valid(reply, 30, 20, sizeof(reply)));
    EXPECT_FALSE(wki_vfs_append_response_valid(reply, 29, 6, sizeof(reply)));
    EXPECT_FALSE(wki_vfs_append_response_valid(reply, 29, WKI_VFS_APPEND_MAX + 1, sizeof(reply)));
    EXPECT_FALSE(wki_vfs_append_response_valid(reply, 29, 20, sizeof(reply) - 1));
    EXPECT_FALSE(wki_vfs_append_response_valid(reply, 29, 20, sizeof(reply) + 1));
    reply.offset = INT64_MAX - 6;
    EXPECT_FALSE(wki_vfs_append_response_valid(reply, 29, 20, sizeof(reply)));
    reply.offset = -1;
    EXPECT_FALSE(wki_vfs_append_response_valid(reply, 29, 20, sizeof(reply)));
    reply = {.operation = 29, .status = -EIO};
    ASSERT_TRUE(wki_vfs_append_response_valid(reply, 29, 20, sizeof(reply)));
    reply.offset = 1;
    EXPECT_FALSE(wki_vfs_append_response_valid(reply, 29, 20, sizeof(reply)));
    reply.offset = 0;
    reply.bytes = 1;
    EXPECT_FALSE(wki_vfs_append_response_valid(reply, 29, 20, sizeof(reply)));
    reply = {.operation = 29, .status = 1};
    EXPECT_FALSE(wki_vfs_append_response_valid(reply, 29, 20, sizeof(reply)));
    reply = {.reserved = 1, .operation = 29};
    EXPECT_FALSE(wki_vfs_append_response_valid(reply, 29, 20, sizeof(reply)));
    for (size_t length = 0; length <= 64; ++length) {
        EXPECT_EQ(wki_vfs_append_reply_size_valid(0, length, sizeof(reply)), length == sizeof(reply));
        EXPECT_EQ(wki_vfs_append_reply_size_valid(-EIO, length, sizeof(reply)), length == 0);
    }
}

TEST(WkiVfsCoherencyWire, AdditiveLayoutsAndIndependentFeaturesPreserveV3) {
    EXPECT_EQ(sizeof(VfsOwnerFdRequest), 40U);
    EXPECT_EQ(sizeof(VfsVersionResponse), 48U);
    EXPECT_EQ(offsetof(VfsOwnerFdRequest, query_id), 32U);
    EXPECT_EQ(offsetof(VfsVersionResponse, object), 16U);
    EXPECT_EQ(sizeof(WkiHeader), 32U);
    EXPECT_EQ(sizeof(HelloPayload), 172U);
    EXPECT_EQ(sizeof(WkiAuthTrailer), 40U);
    EXPECT_EQ(WKI_VERSION, 3U);
    EXPECT_EQ(OP_VFS_VERSION, 0x0423U);
    EXPECT_EQ(WKI_VFS_FEATURE_OWNER_VERSIONS & (WKI_VFS_FEATURE_ATOMIC_APPEND | WKI_VFS_FEATURE_OWNER_LOCKS), 0U);
    EXPECT_EQ(WKI_VFS_FEATURE_ATOMIC_APPEND & WKI_VFS_FEATURE_OWNER_LOCKS, 0U);
    EXPECT_EQ((WKI_VFS_FEATURE_OWNER_VERSIONS | WKI_VFS_FEATURE_ATOMIC_APPEND | WKI_VFS_FEATURE_OWNER_LOCKS) & 4095U, 0U);
}

TEST(WkiVfsCoherencyWire, ExactRequestLengthAndEveryIdentityFieldAreRequired) {
    ASSERT_TRUE(wki_vfs_owner_fd_request_valid(REQUEST, sizeof(REQUEST)));
    for (size_t length = 0; length <= 80; ++length) EXPECT_EQ(wki_vfs_owner_fd_request_valid(REQUEST, length), length == sizeof(REQUEST));
    for (unsigned field = 0; field < 10; ++field) {
        auto request = REQUEST;
        switch (field) {
            case 0:
                request.version = 0;
                break;
            case 1:
                request.version = 2;
                break;
            case 2:
                request.reserved = 1;
                break;
            case 3:
                request.fd = 0;
                break;
            case 4:
                request.fd = -1;
                break;
            case 5:
                request.open_token = 0;
                break;
            case 6:
                request.binding_id = 0;
                break;
            case 7:
                request.owner_boot_epoch = 0;
                break;
            case 8:
                request.reserved2 = 1;
                break;
            default:
                request.query_id = 0;
                break;
        }
        EXPECT_FALSE(wki_vfs_owner_fd_request_valid(request, sizeof(request))) << field;
    }
}

TEST(WkiVfsCoherencyWire, ReplyNonceFormatAndVersionOrderingAreExact) {
    ASSERT_TRUE(wki_vfs_version_response_valid(RESPONSE, REQUEST.query_id, sizeof(RESPONSE)));
    for (size_t length = 0; length <= 96; ++length)
        EXPECT_EQ(wki_vfs_version_response_valid(RESPONSE, REQUEST.query_id, length), length == sizeof(RESPONSE));
    EXPECT_FALSE(wki_vfs_version_response_valid(RESPONSE, 0, sizeof(RESPONSE)));
    EXPECT_FALSE(wki_vfs_version_response_valid(RESPONSE, REQUEST.query_id + 1, sizeof(RESPONSE)));
    for (unsigned field = 0; field < 9; ++field) {
        auto reply = RESPONSE;
        switch (field) {
            case 0:
                reply.version = 0;
                break;
            case 1:
                reply.length -= 1;
                break;
            case 2:
                reply.reserved = 1;
                break;
            case 3:
                reply.query_id = 0;
                break;
            case 4:
                reply.object = 0;
                break;
            case 5:
                reply.content = reply.object - 1;
                break;
            case 6:
                reply.directory = reply.object - 1;
                break;
            case 7:
                reply.metadata = reply.content - 1;
                break;
            default:
                reply.metadata = reply.directory - 1;
                break;
        }
        EXPECT_FALSE(wki_vfs_version_response_valid(reply, REQUEST.query_id, sizeof(reply))) << field;
    }
}

TEST(WkiVfsCoherencyWire, ErrorRepliesNeverCarryOrTruncateAuthority) {
    for (int status : {-EAGAIN, -EOPNOTSUPP, -EBADF, -ENOTCONN, 0, 1}) {
        for (size_t length = 0; length <= 96; ++length) {
            bool const VALID = status <= 0 && length == (status == 0 ? sizeof(RESPONSE) : 0);
            EXPECT_EQ(wki_vfs_version_reply_size_valid(status, length, sizeof(RESPONSE)), VALID);
            EXPECT_EQ(wki_vfs_version_reply_size_valid(status, length, sizeof(RESPONSE) - 1), VALID && status != 0);
        }
    }
}

TEST(WkiVfsCoherencyWire, WireVersionsAgreeWithNativePublicationValidity) {
    for (uint64_t object = 0; object < 5; ++object)
        for (uint64_t content = 0; content < 7; ++content)
            for (uint64_t metadata = 0; metadata < 9; ++metadata)
                for (uint64_t directory = 0; directory < 7; ++directory) {
                    coherence::Stamp const STAMP{object, content, metadata, directory};
                    VfsVersionResponse const REPLY{
                        .query_id = REQUEST.query_id, .object = object, .content = content, .metadata = metadata, .directory = directory};
                    EXPECT_EQ(wki_vfs_version_response_valid(REPLY, REQUEST.query_id, sizeof(REPLY)), coherence::valid_stamp(STAMP));
                }
}

TEST(WkiVfsCoherencyWire, PositionedIoBoundsOffsetsAndRequiresExactTokenAndFrame) {
    VfsOwnerIoRequest request{.owner = REQUEST, .offset = 11, .data_len = WKI_VFS_OWNER_WRITE_MAX};
    EXPECT_EQ(sizeof(request), 56U);
    EXPECT_EQ(sizeof(VfsOwnerReadResponse), 24U);
    ASSERT_TRUE(wki_vfs_owner_io_request_valid(request, sizeof(request) + request.data_len, true));
    EXPECT_FALSE(wki_vfs_owner_io_request_valid(request, sizeof(request), true));
    EXPECT_FALSE(wki_vfs_owner_io_request_valid(request, sizeof(request) + request.data_len + 1, true));
    ++request.data_len;
    EXPECT_FALSE(wki_vfs_owner_io_request_valid(request, sizeof(request) + request.data_len, true));
    request.data_len = WKI_VFS_OWNER_READ_MAX;
    ASSERT_TRUE(wki_vfs_owner_io_request_valid(request, sizeof(request), false));
    EXPECT_FALSE(wki_vfs_owner_io_request_valid(request, sizeof(request) + 1, false));
    request.offset = INT64_MAX - request.data_len;
    ASSERT_TRUE(wki_vfs_owner_io_request_valid(request, sizeof(request), false));
    ++request.offset;
    EXPECT_FALSE(wki_vfs_owner_io_request_valid(request, sizeof(request), false));
    request.offset = -1;
    EXPECT_FALSE(wki_vfs_owner_io_request_valid(request, sizeof(request), false));
    request.offset = 0;
    request.reserved = 1;
    EXPECT_FALSE(wki_vfs_owner_io_request_valid(request, sizeof(request), false));
    request.reserved = 0;
    request.owner.open_token = 0;
    EXPECT_FALSE(wki_vfs_owner_io_request_valid(request, sizeof(request), false));
    EXPECT_EQ(WKI_VFS_FEATURE_COMMITTED_IO &
                  (WKI_VFS_FEATURE_OWNER_VERSIONS | WKI_VFS_FEATURE_ATOMIC_APPEND | WKI_VFS_FEATURE_OWNER_LOCKS | 4095U),
              0U);
}

TEST(WkiVfsCoherencyWire, PositionedWriteReplyCannotBorrowDifferentOffsetOrOperation) {
    VfsAppendResponse response{.operation = 29, .offset = 11, .bytes = 7};
    ASSERT_TRUE(wki_vfs_owner_write_response_valid(response, 29, 11, 20, sizeof(response)));
    EXPECT_FALSE(wki_vfs_owner_write_response_valid(response, 30, 11, 20, sizeof(response)));
    EXPECT_FALSE(wki_vfs_owner_write_response_valid(response, 29, 12, 20, sizeof(response)));
    EXPECT_FALSE(wki_vfs_owner_write_response_valid(response, 29, 11, 6, sizeof(response)));
    EXPECT_FALSE(wki_vfs_owner_write_response_valid(response, 29, -1, 20, sizeof(response)));
    EXPECT_FALSE(wki_vfs_owner_write_response_valid(response, 29, INT64_MAX - 19, 20, sizeof(response)));
    response = {.operation = 29, .status = -ENOSPC};
    EXPECT_TRUE(wki_vfs_owner_write_response_valid(response, 29, 11, 20, sizeof(response)));
    response.bytes = 1;
    EXPECT_FALSE(wki_vfs_owner_write_response_valid(response, 29, 11, 20, sizeof(response)));
    response = {.operation = 29, .offset = 11};
    EXPECT_TRUE(wki_vfs_owner_write_response_valid(response, 29, 11, 0, sizeof(response)));
}

TEST(WkiVfsCoherencyWire, ReadReplyNonceLengthAndInitializedPrefixAreExact) {
    VfsOwnerReadResponse response{.query_id = 29, .bytes = 7};
    ASSERT_TRUE(wki_vfs_owner_read_response_valid(response, 29, 20, sizeof(response) + 7));
    EXPECT_FALSE(wki_vfs_owner_read_response_valid(response, 30, 20, sizeof(response) + 7));
    EXPECT_FALSE(wki_vfs_owner_read_response_valid(response, 0, 20, sizeof(response) + 7));
    EXPECT_FALSE(wki_vfs_owner_read_response_valid(response, 29, 6, sizeof(response) + 7));
    EXPECT_FALSE(wki_vfs_owner_read_response_valid(response, 29, 20, sizeof(response) + 8));
    EXPECT_FALSE(wki_vfs_owner_read_response_valid(response, 29, 20, sizeof(response) + 6));
    for (unsigned field = 0; field < 4; ++field) {
        auto malformed = response;
        if (field == 0) malformed.version = 0;
        if (field == 1) malformed.length = 0;
        if (field == 2) malformed.reserved = 1;
        if (field == 3) malformed.reserved2 = 1;
        EXPECT_FALSE(wki_vfs_owner_read_response_valid(malformed, 29, 20, sizeof(response) + 7));
    }
    response.bytes = 0;
    EXPECT_TRUE(wki_vfs_owner_read_response_valid(response, 29, 0, sizeof(response)));
    for (size_t length = 0; length <= sizeof(response) + WKI_VFS_OWNER_READ_MAX + 1; ++length) {
        bool const SUCCESS = length >= sizeof(response) && length <= sizeof(response) + WKI_VFS_OWNER_READ_MAX;
        EXPECT_EQ(wki_vfs_owner_read_reply_size_valid(0, length, UINT16_MAX), SUCCESS);
        EXPECT_EQ(wki_vfs_owner_read_reply_size_valid(-EIO, length, UINT16_MAX), length == 0);
        EXPECT_FALSE(wki_vfs_owner_read_reply_size_valid(1, length, UINT16_MAX));
    }
    EXPECT_FALSE(wki_vfs_owner_read_reply_size_valid(0, sizeof(response) + 1, sizeof(response)));
}
