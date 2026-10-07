#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <net/wki/wire.hpp>
#include <vfs/coherency.hpp>
#include <vfs/owner_observation.hpp>

using namespace ker::net::wki;
namespace coherence = ker::vfs::coherency;

namespace {
constexpr VfsOwnerFdRequest REQUEST{.fd = 7, .open_token = 13, .binding_id = 19, .owner_boot_epoch = 23, .query_id = 29};
constexpr VfsVersionResponse RESPONSE{.query_id = 29, .object = 31, .content = 37, .metadata = 43, .directory = 41};
constexpr VfsOwnerLockRequest LOCK_REQUEST{.owner = REQUEST, .process_generation = 47, .scope_token = 59, .pid = 53, .type = 1};
}  // namespace

TEST(WkiVfsCoherencyWire, ExactOwnerObservationRejectsMalformedAuthorityAndOperation) {
    VfsOwnerObserveRequest const valid{.owner = REQUEST};
    EXPECT_EQ(sizeof(valid), 64U);
    EXPECT_EQ(sizeof(VfsOwnerObserveResponse), 40U);
    EXPECT_EQ(OP_VFS_OBSERVE_OWNER, 0x0429U);
    for (size_t length = 0; length <= 160; ++length) EXPECT_EQ(wki_vfs_owner_observe_request_valid(valid, length), length == sizeof(valid));
    for (unsigned field = 0; field < 14; ++field) {
        auto request = valid;
        switch (field) {
            case 0:
                request.owner.version = 0;
                break;
            case 1:
                request.owner.reserved = 1;
                break;
            case 2:
                request.owner.fd = 0;
                break;
            case 3:
                request.owner.open_token = 0;
                break;
            case 4:
                request.owner.binding_id = 0;
                break;
            case 5:
                request.owner.owner_boot_epoch = 0;
                break;
            case 6:
                request.owner.reserved2 = 1;
                break;
            case 7:
                request.owner.query_id = 0;
                break;
            case 8:
                request.offset = 1;
                break;
            case 9:
                request.index = 1;
                break;
            case 10:
                request.count = 1;
                break;
            case 11:
                request.operation = static_cast<VfsOwnerObservation>(4);
                break;
            case 12:
                request.reserved = 1;
                break;
            default:
                request.reserved2 = 1;
                break;
        }
        EXPECT_FALSE(wki_vfs_owner_observe_request_valid(request, sizeof(request))) << field;
    }
    auto directory = valid;
    directory.operation = VfsOwnerObservation::DIRECTORY;
    directory.count = WKI_VFS_OWNER_DIRECTORY_MAX;
    directory.index = UINT64_MAX;
    EXPECT_TRUE(wki_vfs_owner_observe_request_valid(directory, sizeof(directory)));
    directory.count = 0;
    EXPECT_FALSE(wki_vfs_owner_observe_request_valid(directory, sizeof(directory)));
    directory.count = WKI_VFS_OWNER_DIRECTORY_MAX + 1;
    EXPECT_FALSE(wki_vfs_owner_observe_request_valid(directory, sizeof(directory)));
    auto end = valid;
    end.operation = VfsOwnerObservation::END_POSITION;
    for (int64_t offset : {INT64_MIN, int64_t{-1}, int64_t{0}, INT64_MAX}) {
        end.offset = offset;
        EXPECT_TRUE(wki_vfs_owner_observe_request_valid(end, sizeof(end)));
    }
}

TEST(WkiVfsCoherencyWire, ObservationReplyMustEchoExactNonceKindAndBody) {
    VfsOwnerObserveRequest const request{.owner = REQUEST};
    VfsOwnerObserveResponse const valid{.query_id = REQUEST.query_id, .object = 41, .body_length = WKI_VFS_OWNER_STAT_BYTES};
    constexpr size_t LENGTH = sizeof(valid) + WKI_VFS_OWNER_STAT_BYTES;
    EXPECT_TRUE(wki_vfs_owner_observe_response_valid(valid, request, LENGTH));
    for (unsigned field = 0; field < 10; ++field) {
        auto response = valid;
        switch (field) {
            case 0:
                response.version = 0;
                break;
            case 1:
                --response.length;
                break;
            case 2:
                response.reserved = 1;
                break;
            case 3:
                ++response.query_id;
                break;
            case 4:
                response.object = 0;
                break;
            case 5:
                response.value = 1;
                break;
            case 6:
                response.operation = VfsOwnerObservation::END_POSITION;
                break;
            case 7:
                response.reserved2 = 1;
                break;
            case 8:
                response.count = 1;
                break;
            default:
                --response.body_length;
                break;
        }
        EXPECT_FALSE(wki_vfs_owner_observe_response_valid(response, request, LENGTH)) << field;
    }
    for (size_t length = 0; length <= LENGTH + 16; ++length)
        EXPECT_EQ(wki_vfs_owner_observe_response_valid(valid, request, length), length == LENGTH);
    for (int status : {-EAGAIN, -ESTALE, -EBADF}) {
        EXPECT_TRUE(wki_vfs_owner_observe_reply_size_valid(status, 0, LENGTH));
        EXPECT_FALSE(wki_vfs_owner_observe_reply_size_valid(status, sizeof(valid), LENGTH));
    }
    EXPECT_FALSE(wki_vfs_owner_observe_reply_size_valid(1, 0, LENGTH));
    EXPECT_FALSE(wki_vfs_owner_observe_reply_size_valid(0, sizeof(valid) - 1, LENGTH));
    EXPECT_FALSE(wki_vfs_owner_observe_reply_size_valid(0, LENGTH, LENGTH - 1));
}

TEST(WkiVfsCoherencyWire, DirectoryObservationBoundsCountAndEmptyEofWithoutRankTruncation) {
    VfsOwnerObserveRequest const request{
        .owner = REQUEST, .index = uint64_t{1} << 48, .count = WKI_VFS_OWNER_DIRECTORY_MAX, .operation = VfsOwnerObservation::DIRECTORY};
    VfsOwnerObserveResponse response{.query_id = REQUEST.query_id, .object = 41, .operation = VfsOwnerObservation::DIRECTORY};
    for (uint16_t count = 0; count <= request.count; ++count) {
        response.count = count;
        response.body_length = count * WKI_VFS_OWNER_DIRENT_BYTES;
        EXPECT_TRUE(wki_vfs_owner_observe_response_valid(response, request, sizeof(response) + response.body_length));
    }
    ++response.count;
    response.body_length += WKI_VFS_OWNER_DIRENT_BYTES;
    EXPECT_FALSE(wki_vfs_owner_observe_response_valid(response, request, sizeof(response) + response.body_length));
    response.count = 1;
    response.body_length = WKI_VFS_OWNER_DIRENT_BYTES - 1;
    EXPECT_FALSE(wki_vfs_owner_observe_response_valid(response, request, sizeof(response) + response.body_length));
}

TEST(WkiVfsCoherencyWire, EofSelectionChecksSignedBoundariesAndNeverPublishesErrorPosition) {
    namespace observation = ker::vfs::owner_observation;
    int64_t position = 71;
    EXPECT_EQ(observation::seek_end(3, -4, position), -EINVAL);
    EXPECT_EQ(position, 71);
    EXPECT_EQ(observation::seek_end(-1, 0, position), -EIO);
    EXPECT_EQ(position, 71);
    EXPECT_EQ(observation::seek_end(INT64_MAX, 1, position), -EOVERFLOW);
    EXPECT_EQ(position, 71);
    EXPECT_EQ(observation::seek_end(INT64_MAX, INT64_MIN, position), -EINVAL);
    EXPECT_EQ(position, 71);
    EXPECT_EQ(observation::seek_end(INT64_MAX, 0, position), 0);
    EXPECT_EQ(position, INT64_MAX);
    EXPECT_EQ(observation::seek_end(7, -7, position), 0);
    EXPECT_EQ(position, 0);
    EXPECT_EQ(observation::seek_end(7, 5, position), 0);
    EXPECT_EQ(position, 12);
}

TEST(WkiVfsCoherencyWire, EofReplyHasNoBodyAndDirectoryRecordUsesNativeNameLength) {
    VfsOwnerObserveRequest const request{.owner = REQUEST, .offset = -7, .operation = VfsOwnerObservation::END_POSITION};
    VfsOwnerObserveResponse response{
        .query_id = REQUEST.query_id, .object = 41, .value = INT64_MAX, .operation = VfsOwnerObservation::END_POSITION};
    EXPECT_TRUE(wki_vfs_owner_observe_response_valid(response, request, sizeof(response)));
    response.value = -1;
    EXPECT_FALSE(wki_vfs_owner_observe_response_valid(response, request, sizeof(response)));
    response.value = 0;
    response.body_length = 1;
    EXPECT_FALSE(wki_vfs_owner_observe_response_valid(response, request, sizeof(response) + 1));
    ker::vfs::DirEntry entry{};
    entry.d_off = 19;
    entry.d_reclen = ker::vfs::DIRENT_MIN_RECLEN;
    EXPECT_TRUE(ker::vfs::owner_observation::directory_entry_valid(entry, 1));
    // The NUL exists in the fixed wire image but lies outside its native record.
    entry.d_name.fill('a');
    entry.d_name.back() = '\0';
    EXPECT_FALSE(ker::vfs::owner_observation::directory_entry_valid(entry, 1));
}

TEST(WkiVfsCoherencyWire, NativeDirectoryParserFollowsSparseAndLargeCookiesAndRejectsMalformedTail) {
    namespace observation = ker::vfs::owner_observation;
    constexpr uint64_t START = uint64_t{1} << 48;
    std::array<ker::vfs::DirEntry, 3> entries{};
    entries[0].d_off = START + 7;
    entries[1].d_off = START + 53;
    entries[2].d_off = UINT64_MAX;
    for (auto& entry : entries) entry.d_reclen = sizeof(entry);
    auto bytes = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(entries.data()), sizeof(entries));
    uint64_t next = 97;
    ASSERT_TRUE(observation::directory_end_cookie(bytes, entries.size(), START, next));
    EXPECT_EQ(next, UINT64_MAX);
    for (unsigned field = 0; field < 4; ++field) {
        auto const saved = entries.back();
        next = 97;
        switch (field) {
            case 0:
                entries.back().d_off = entries[1].d_off;
                break;
            case 1:
                entries.back().d_off = 0;
                break;
            case 2:
                --entries.back().d_reclen;
                break;
            default:
                entries.back().d_name.fill('a');
                break;
        }
        EXPECT_FALSE(observation::directory_end_cookie(bytes, entries.size(), START, next)) << field;
        EXPECT_EQ(next, 97);
        entries.back() = saved;
    }
    EXPECT_FALSE(observation::directory_end_cookie(bytes.first(bytes.size() - 1), entries.size(), START, next));
    EXPECT_TRUE(observation::directory_end_cookie({}, 0, UINT64_MAX, next));
    EXPECT_EQ(next, UINT64_MAX);
}

TEST(WkiVfsCoherencyWire, DirectoryEndMarkersNeverConvertOtherBackendErrorsToEmptySuccess) {
    for (int status : {0, -1, -ENOENT, -EIO, -EINVAL, -EBADF, -EAGAIN, -EOPNOTSUPP}) {
        EXPECT_EQ(ker::vfs::owner_observation::directory_eof(status, true), status == -ENOENT);
        EXPECT_EQ(ker::vfs::owner_observation::directory_eof(status, false), status == -1);
    }
}

TEST(WkiVfsCoherencyWire, OwnerLockRequestRejectsMalformedIdentityAndIntentBeforeAdmission) {
    EXPECT_EQ(sizeof(VfsOwnerLockRequest), 96U);
    EXPECT_EQ(sizeof(VfsOwnerLockResponse), 48U);
    EXPECT_EQ(OP_VFS_LOCK_OWNER, 0x0427U);
    EXPECT_EQ(offsetof(VfsOwnerLockRequest, process_generation), 40U);
    ASSERT_TRUE(wki_vfs_owner_lock_request_valid(LOCK_REQUEST, sizeof(LOCK_REQUEST)));
    auto unscoped = LOCK_REQUEST;
    unscoped.scope_token = 0;
    EXPECT_FALSE(wki_vfs_owner_lock_request_valid(unscoped, sizeof(unscoped)));
    for (size_t length = 0; length <= 176; ++length)
        EXPECT_EQ(wki_vfs_owner_lock_request_valid(LOCK_REQUEST, length), length == sizeof(LOCK_REQUEST));
    for (unsigned field = 0; field < 20; ++field) {
        auto request = LOCK_REQUEST;
        switch (field) {
            case 0:
                request.owner.version = 0;
                break;
            case 1:
                request.owner.reserved = 1;
                break;
            case 2:
                request.owner.fd = 0;
                break;
            case 3:
                request.owner.open_token = 0;
                break;
            case 4:
                request.owner.binding_id = 0;
                break;
            case 5:
                request.owner.owner_boot_epoch = 0;
                break;
            case 6:
                request.owner.reserved2 = 1;
                break;
            case 7:
                request.owner.query_id = 0;
                break;
            case 8:
                request.process_generation = 0;
                break;
            case 9:
                request.pid = 0;
                break;
            case 10:
                request.pid = -1;
                break;
            case 11:
                request.kind = 0;
                break;
            case 12:
                request.kind = 4;
                break;
            case 13:
                request.phase = 0;
                break;
            case 14:
                request.phase = 5;
                break;
            case 15:
                request.type = -1;
                break;
            case 16:
                request.type = 3;
                break;
            case 17:
                request.whence = 3;
                break;
            case 18:
                request.flags = 4;
                break;
            default:
                request.reserved = 1;
                break;
        }
        EXPECT_FALSE(wki_vfs_owner_lock_request_valid(request, sizeof(request))) << field;
    }
}

TEST(WkiVfsCoherencyWire, OwnerLockKindsAndPhasesPreserveRawRangeAndRejectImpossibleFlags) {
    for (uint8_t kind = 1; kind <= 3; ++kind) {
        for (uint8_t phase = 1; phase <= 4; ++phase) {
            auto request = LOCK_REQUEST;
            request.kind = kind;
            request.scope_token = kind == 1 ? LOCK_REQUEST.scope_token : 0;
            request.phase = phase;
            request.flags = WKI_VFS_LOCK_WAIT;
            ASSERT_TRUE(wki_vfs_owner_lock_request_valid(request, sizeof(request)));
            request.type = 2;
            EXPECT_FALSE(wki_vfs_owner_lock_request_valid(request, sizeof(request)));
            request.type = 1;
            request.flags = WKI_VFS_LOCK_QUERY;
            EXPECT_EQ(wki_vfs_owner_lock_request_valid(request, sizeof(request)), kind != 3);
            request.flags |= WKI_VFS_LOCK_WAIT;
            EXPECT_FALSE(wki_vfs_owner_lock_request_valid(request, sizeof(request)));
            request.flags = 0;
            request.start = -17;
            request.range_length = -23;
            request.position = 29;
            request.whence = 2;
            EXPECT_EQ(wki_vfs_owner_lock_request_valid(request, sizeof(request)), kind != 3);
        }
    }
}

TEST(WkiVfsCoherencyWire, ScopeRequestSeparatesExactOpenRegistrationFromIndependentCloseAuthority) {
    EXPECT_EQ(sizeof(VfsOwnerScopeRequest), 64U);
    EXPECT_EQ(sizeof(VfsOwnerScopeResponse), 40U);
    VfsOwnerScopeRequest request{.owner = REQUEST};
    ASSERT_TRUE(wki_vfs_owner_scope_request_valid(request, sizeof(request)));
    for (size_t length = 0; length <= 128; ++length)
        EXPECT_EQ(wki_vfs_owner_scope_request_valid(request, length), length == sizeof(request));
    request.process_generation = 47;
    request.pid = 53;
    EXPECT_FALSE(wki_vfs_owner_scope_request_valid(request, sizeof(request)));
    request.operation = VfsOwnerScopeOperation::REGISTER;
    ASSERT_TRUE(wki_vfs_owner_scope_request_valid(request, sizeof(request)));
    request.scope_token = 59;
    EXPECT_FALSE(wki_vfs_owner_scope_request_valid(request, sizeof(request)));
    request.operation = VfsOwnerScopeOperation::CLOSE;
    EXPECT_FALSE(wki_vfs_owner_scope_request_valid(request, sizeof(request)));
    request.owner.fd = 0;
    request.owner.open_token = 0;
    ASSERT_TRUE(wki_vfs_owner_scope_request_valid(request, sizeof(request)));
    request.operation = VfsOwnerScopeOperation::ACK;
    EXPECT_TRUE(wki_vfs_owner_scope_request_valid(request, sizeof(request)));
    request.scope_token = 0;
    EXPECT_FALSE(wki_vfs_owner_scope_request_valid(request, sizeof(request)));
    auto ofd = LOCK_REQUEST;
    ofd.kind = 2;
    EXPECT_FALSE(wki_vfs_owner_lock_request_valid(ofd, sizeof(ofd)));
    ofd.scope_token = 0;
    EXPECT_TRUE(wki_vfs_owner_lock_request_valid(ofd, sizeof(ofd)));
}

TEST(WkiVfsCoherencyWire, ScopeCloseRequestRejectsEveryAuthorityFieldAndUnknownOperation) {
    VfsOwnerScopeRequest const CLOSE{.owner = {.binding_id = 19, .owner_boot_epoch = 23, .query_id = 29},
                                     .process_generation = 47,
                                     .scope_token = 59,
                                     .pid = 53,
                                     .operation = VfsOwnerScopeOperation::CLOSE};
    ASSERT_TRUE(wki_vfs_owner_scope_request_valid(CLOSE, sizeof(CLOSE)));
    for (unsigned field = 0; field < 16; ++field) {
        auto request = CLOSE;
        switch (field) {
            case 0:
                request.owner.version = 0;
                break;
            case 1:
                request.owner.reserved = 1;
                break;
            case 2:
                request.owner.reserved2 = 1;
                break;
            case 3:
                request.owner.binding_id = 0;
                break;
            case 4:
                request.owner.owner_boot_epoch = 0;
                break;
            case 5:
                request.owner.query_id = 0;
                break;
            case 6:
                request.owner.fd = 7;
                break;
            case 7:
                request.owner.open_token = 13;
                break;
            case 8:
                request.process_generation = 0;
                break;
            case 9:
                request.scope_token = 0;
                break;
            case 10:
                request.pid = 0;
                break;
            case 11:
                request.pid = -1;
                break;
            case 12:
                request.operation = static_cast<VfsOwnerScopeOperation>(0);
                break;
            case 13:
                request.operation = static_cast<VfsOwnerScopeOperation>(5);
                break;
            case 14:
                request.reserved = 1;
                break;
            default:
                request.reserved2 = 1;
                break;
        }
        EXPECT_FALSE(wki_vfs_owner_scope_request_valid(request, sizeof(request))) << field;
    }
}

TEST(WkiVfsCoherencyWire, ScopeReplyEchoesExactQueryAndCannotReopenClosedToken) {
    VfsOwnerScopeRequest request{.owner = REQUEST};
    VfsOwnerScopeResponse reply{.query_id = REQUEST.query_id, .object = 61};
    ASSERT_TRUE(wki_vfs_owner_scope_response_valid(reply, request, sizeof(reply)));
    for (size_t length = 0; length <= 80; ++length) {
        EXPECT_EQ(wki_vfs_owner_scope_reply_size_valid(0, length, sizeof(reply)), length == sizeof(reply));
        EXPECT_EQ(wki_vfs_owner_scope_reply_size_valid(-ESTALE, length, sizeof(reply)), length == 0);
    }
    for (unsigned field = 0; field < 10; ++field) {
        auto wrong = reply;
        switch (field) {
            case 0:
                wrong.version = 0;
                break;
            case 1:
                --wrong.length;
                break;
            case 2:
                wrong.reserved = 1;
                break;
            case 3:
                wrong.reserved2 = 1;
                break;
            case 4:
                wrong.reserved3 = 1;
                break;
            case 5:
                wrong.reserved4 = 1;
                break;
            case 6:
                ++wrong.query_id;
                break;
            case 7:
                wrong.object = 0;
                break;
            case 8:
                wrong.scope_token = 59;
                break;
            default:
                wrong.state = 1;
                break;
        }
        EXPECT_FALSE(wki_vfs_owner_scope_response_valid(wrong, request, sizeof(wrong))) << field;
    }
    request.operation = VfsOwnerScopeOperation::REGISTER;
    request.process_generation = 47;
    request.pid = 53;
    reply.scope_token = 59;
    reply.state = 1;
    ASSERT_TRUE(wki_vfs_owner_scope_response_valid(reply, request, sizeof(reply)));
    reply.state = 2;
    EXPECT_TRUE(wki_vfs_owner_scope_response_valid(reply, request, sizeof(reply)));
    request.owner.fd = 0;
    request.owner.open_token = 0;
    request.scope_token = 59;
    request.operation = VfsOwnerScopeOperation::CLOSE;
    ASSERT_TRUE(wki_vfs_owner_scope_response_valid(reply, request, sizeof(reply)));
    reply.state = 1;
    EXPECT_FALSE(wki_vfs_owner_scope_response_valid(reply, request, sizeof(reply)));
    reply.state = 2;
    ++reply.scope_token;
    EXPECT_FALSE(wki_vfs_owner_scope_response_valid(reply, request, sizeof(reply)));
}

TEST(WkiVfsCoherencyWire, OwnerLockReplyRequiresExactNonceTerminalSemanticsAndFraming) {
    VfsOwnerLockResponse reply{.operation = REQUEST.query_id};
    ASSERT_TRUE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, false, sizeof(reply)));
    for (size_t length = 0; length <= 96; ++length) {
        EXPECT_EQ(wki_vfs_owner_lock_reply_size_valid(0, length, sizeof(reply)), length == sizeof(reply));
        EXPECT_EQ(wki_vfs_owner_lock_reply_size_valid(-EIO, length, sizeof(reply)), length == 0);
        EXPECT_EQ(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, false, length), length == sizeof(reply));
    }
    EXPECT_FALSE(wki_vfs_owner_lock_reply_size_valid(1, 0, sizeof(reply)));
    EXPECT_FALSE(wki_vfs_owner_lock_reply_size_valid(0, sizeof(reply), sizeof(reply) - 1));
    EXPECT_FALSE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id + 1, false, sizeof(reply)));
    for (unsigned field = 0; field < 15; ++field) {
        auto malformed = reply;
        switch (field) {
            case 0:
                malformed.version = 0;
                break;
            case 1:
                --malformed.length;
                break;
            case 2:
                malformed.reserved = 1;
                break;
            case 3:
                malformed.reserved2 = 1;
                break;
            case 4:
                malformed.reserved3 = 1;
                break;
            case 5:
                malformed.operation = 0;
                break;
            case 6:
                malformed.status = 1;
                break;
            case 7:
                malformed.state = 0;
                break;
            case 8:
                malformed.state = 4;
                break;
            case 9:
                malformed.state = 1;
                break;
            case 10:
                malformed.state = 3;
                break;
            case 11:
                malformed.pid = 1;
                break;
            case 12:
                malformed.start = 1;
                break;
            case 13:
                malformed.range_length = 1;
                break;
            default:
                malformed.type = 1;
                break;
        }
        EXPECT_FALSE(wki_vfs_owner_lock_response_valid(malformed, REQUEST.query_id, false, sizeof(malformed))) << field;
    }
    reply.status = -EAGAIN;
    reply.state = 1;
    EXPECT_TRUE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, false, sizeof(reply)));
    EXPECT_FALSE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, true, sizeof(reply)));
    reply.status = -EINTR;
    reply.state = 3;
    EXPECT_TRUE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, false, sizeof(reply)));
    reply.status = -ENOLCK;
    EXPECT_FALSE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, false, sizeof(reply)));
    reply.state = 2;
    EXPECT_TRUE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, false, sizeof(reply)));
}

TEST(WkiVfsCoherencyWire, OwnerLockQueryReplyHasCanonicalNativeRangeAndDistinctOfdOwner) {
    VfsOwnerLockResponse reply{.operation = REQUEST.query_id, .pid = 53, .start = 7, .range_length = 11, .type = 1};
    ASSERT_TRUE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, true, sizeof(reply)));
    EXPECT_FALSE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, false, sizeof(reply)));
    reply.pid = -1;
    EXPECT_TRUE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, true, sizeof(reply)));
    reply.pid = 0;
    EXPECT_FALSE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, true, sizeof(reply)));
    reply.pid = 53;
    reply.start = INT64_MAX;
    EXPECT_FALSE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, true, sizeof(reply)));
    reply.range_length = 0;
    EXPECT_TRUE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, true, sizeof(reply)));
    reply.whence = 1;
    EXPECT_FALSE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, true, sizeof(reply)));
    reply.whence = 0;
    reply.start = -1;
    EXPECT_FALSE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, true, sizeof(reply)));
    reply = {.operation = REQUEST.query_id};
    EXPECT_TRUE(wki_vfs_owner_lock_response_valid(reply, REQUEST.query_id, true, sizeof(reply)));
}

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
