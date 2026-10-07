#include <atomic>
#include <memory>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/wki.hpp>
#include <platform/security/credential_record.hpp>
#include <test/ktest.hpp>
#include <type_traits>
#include <utility>
#include <vfs/advisory.hpp>
#include <vfs/mount.hpp>
#include <vfs/timestamps.hpp>

KTEST(WkiRemoteVfsCoherency, DiagnosticFeatureTokensAreExactAndIndependent) {
    KEXPECT_TRUE(ker::net::wki::wki_selftest_vfs_owner_qualification_tokens());
}

KTEST(WkiVfsWithdrawal, RetainsExactRevocationsUntilAcknowledged) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_withdrawal_outbox());
}

KTEST(WkiDevProxyZone, RetainedGrantGatesIoAndCleanup) { KEXPECT_TRUE(ker::net::wki::wki_dev_proxy_selftest_zone_retention()); }

KTEST(WkiDevProxyReuse, ExistingProviderMatchesSelectedObservation) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_proxy_selftest_existing_block_identity());
}

KTEST(WkiDevProxyFenceFlags, LifecycleFlagsAreAtomic) {
    using State = ker::net::wki::ProxyBlockState;

    constexpr bool ACTIVE_ATOMIC = std::is_same_v<decltype(std::declval<State&>().active), std::atomic<bool>>;
    constexpr bool FENCED_ATOMIC = std::is_same_v<decltype(std::declval<State&>().fenced), std::atomic<bool>>;

    KEXPECT_TRUE(ACTIVE_ATOMIC);
    KEXPECT_TRUE(FENCED_ATOMIC);
}

KTEST(WkiDevProxyAttachAck, CookieFencesStaleBlockCompletion) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_proxy_selftest_attach_ack_cookie_fences_stale_completion());
}

KTEST(WkiDevProxyAttachFailure, ErasesExactProxy) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_proxy_selftest_failed_attach_erases_exact_proxy());
}

KTEST(WkiDevProxyRdmaSqWait, StopsOnFenceOrInactive) { KEXPECT_TRUE(ker::net::wki::wki_dev_proxy_selftest_rdma_sq_wait_stops_on_fence()); }

KTEST(WkiDevProxyRdmaBatch, InvalidLaterRangeHasNoPartialPublication) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_proxy_selftest_batch_validation_is_atomic());
}

KTEST(WkiDevProxyRdmaCompletion, OldEpochTagCannotMatchOrFreeSuccessor) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_proxy_selftest_old_rdma_tag_cannot_match_successor());
}

KTEST(WkiDevProxyRdmaRing, FixedLayoutAndIndicesRejectCorruptPeerState) {
    using namespace ker::net::wki;

    BlkRingGeometry geometry = {.sq_depth = BLK_RING_DEFAULT_SQ_DEPTH,
                                .cq_depth = BLK_RING_DEFAULT_CQ_DEPTH,
                                .data_slot_count = BLK_RING_DEFAULT_DATA_SLOTS,
                                .data_slot_size = BLK_RING_DEFAULT_DATA_SLOT_SIZE,
                                .block_size = 512,
                                .total_blocks = 8192,
                                .server_ready = 1};
    BlkRingIndices indices = {};
    KEXPECT_TRUE(blk_ring_layout_valid(geometry));
    KEXPECT_TRUE(blk_ring_indices_valid(indices, geometry));

    indices.sq_head = geometry.sq_depth;
    KEXPECT_FALSE(blk_ring_indices_valid(indices, geometry));
    indices = {};
    indices.cq_tail = geometry.cq_depth;
    KEXPECT_FALSE(blk_ring_indices_valid(indices, geometry));

    geometry = {.sq_depth = BLK_RING_DEFAULT_SQ_DEPTH,
                .cq_depth = BLK_RING_DEFAULT_CQ_DEPTH,
                .data_slot_count = BLK_RING_DEFAULT_DATA_SLOTS,
                .data_slot_size = BLK_RING_DEFAULT_DATA_SLOT_SIZE,
                .block_size = 512,
                .total_blocks = 8192,
                .server_ready = 1};
    geometry.data_slot_size++;
    KEXPECT_FALSE(blk_ring_layout_valid(geometry));
    geometry.data_slot_size = BLK_RING_DEFAULT_DATA_SLOT_SIZE;
    geometry.block_size = geometry.data_slot_size + 1;
    KEXPECT_FALSE(blk_ring_layout_valid(geometry));
    KEXPECT_EQ(blk_ring_next_index(BLK_RING_DEFAULT_SQ_DEPTH - 1, BLK_RING_DEFAULT_SQ_DEPTH), 0U);
}

KTEST(WkiRemoteVfsAttachAck, CookieFencesStaleMountCompletion) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_attach_ack_cookie_fences_stale_completion());
}

KTEST(WkiRemoteVfsPeerCleanup, PendingAttachWaitIsIncluded) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_peer_cleanup_includes_pending_attach());
}

KTEST(WkiRemoteVfsUtimens, WirePathValidationRejectsEscapes) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_utimens_wire_path_validation());
}

KTEST(WkiRemoteVfsLegacyScalar, RejectsUnsafeInexactAndOverflowingPaths) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_legacy_scalar_wire_path_admission());
}

KTEST(WkiRemoteVfsMappedComponents, NegotiatedPathsPreserveComponentsAndLegacyRejection) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_mapped_component_path_admission());
    KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_component_batch_admission());
}

KTEST(WkiRemoteVfsDirectoryPaths, RealpathCanonicalResponseFraming) {
    using namespace ker::net::wki;
    for (const char* path : {"", "public", "private/public", "..."}) {
        size_t const LENGTH = std::strlen(path);
        KEXPECT_TRUE(wki_vfs_realpath_result_valid(reinterpret_cast<const uint8_t*>(path), LENGTH));
        VfsPathIdentityHeader header{
            .operation = VfsPathOperation::REALPATH, .flags = 0, .data_len = static_cast<uint16_t>(LENGTH), .reserved = 0};
        KEXPECT_TRUE(wki_vfs_path_response_valid(0, sizeof(header) + LENGTH, header));
        KEXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + LENGTH + 1, header));
        header.flags = WKI_VFS_PATH_REQUIRE_DIRECTORY;
        KEXPECT_FALSE(wki_vfs_path_response_valid(0, sizeof(header) + LENGTH, header));
    }
    for (const char* path : {"/public", ".", "..", "a/.", "a/../b", "a//b", "a/"})
        KEXPECT_FALSE(wki_vfs_realpath_result_valid(reinterpret_cast<const uint8_t*>(path), std::strlen(path)));
    std::array<uint8_t, 512> bytes{};
    bytes.fill('a');
    KEXPECT_TRUE(wki_vfs_realpath_result_valid(bytes.data(), 511));
    KEXPECT_FALSE(wki_vfs_realpath_result_valid(bytes.data(), 512));
    bytes[12] = 0;
    KEXPECT_FALSE(wki_vfs_realpath_result_valid(bytes.data(), 511));
}

KTEST(WkiRemoteVfsDirectoryPaths, ExactDescriptorFramingAndResponses) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_directory_path_admission());
}

KTEST(WkiRemoteVfsDirectoryPaths, OpenRequiresBoundedFlagsAndMappedPolicy) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_directory_open_admission());
}

KTEST(WkiRemoteVfsOwnerWrite, PolicyAdmissionAndRetirementKeepOneFile) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_owner_write_policy());
}

KTEST(WkiRemoteVfsOwnerRead, LegacyOperationsRetainFileAcrossRegistryRetirement) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_owner_file_lifetime());
}

KTEST(WkiRemoteVfsCoherency, ExactOwnerVersionAuthorityDeniesStaleOpenAndRetainsAdmittedFile) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_owner_version_admission());
}

KTEST(WkiRemoteVfsCoherency, ExactNativeObservationsIgnorePoisonedCacheAndKeepSparseCookies) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_owner_observations());
}

KTEST(WkiRemoteVfsCoherency, OwnerAdvisoryAdapterDerivesScopeAndPreservesSharedOpenWaits) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_owner_advisory());
}

KTEST(WkiRemoteVfsCoherency, ClientDispatchScopesOutboxesAndFatalOwnershipUseExactReferences) {
    auto const NATIVE_BEFORE = ker::vfs::vfs_advisory_diag_snapshot();
    auto const CLIENT_BEFORE = ker::net::wki::wki_remote_vfs_client_advisory_diag_snapshot();
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_client_advisory());
    KEXPECT_TRUE(ker::vfs::vfs_advisory_diag_snapshot() == NATIVE_BEFORE);
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_client_advisory_diag_snapshot() == CLIENT_BEFORE);
}

KTEST(WkiRemoteVfsCoherency, OwnerScopeControllerUsesTokenAuthorityAndNeverClosesSuccessor) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_owner_scope_controller());
}

KTEST(WkiRemoteVfsCoherency, OwnerAppendSharesNativeOffsetAndReplaysOnlyExactCompletedOutcome) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_owner_append());
}

KTEST(WkiRemoteVfsCoherency, PositionedCommittedIoHasExactLifetimeAndStableReplay) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_owner_committed_io());
}

KTEST(WkiRemoteVfsCoherency, NativeIoPartialErrorAndRetirementHaveOneOutcome) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_owner_io_outcomes());
}

KTEST(WkiRemoteVfsDirectoryPaths, OpenReservationIsInvisibleUntilCommit) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_directory_open_reservation());
}

KTEST(WkiRemoteVfsOpen, PrivatePreparationFailuresAndTaskExitReleaseOwnership) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_scalar_open_preparation());
}

KTEST(WkiRemoteVfsOpen, OwnerTokenCancellationCannotRepeatOrReachSuccessor) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_open_lifetime_owner());
}

KTEST(WkiRemoteVfsOpen, ClientCancellationAndFinalCloseSurviveAbandonedWaitsAndSlotReuse) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_open_lifetime_client());
}

KTEST(WkiRemoteVfsOpen, BorrowedPreparationPreservesCallerOrFinishesAbandonedFile) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_open_at_preparation());
}

KTEST(WkiRemoteVfsOpen, ReplyAdmissionPreservesDestinationAndProtocolErrors) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_open_reply_admission());
}

KTEST(WkiRemoteVfsExecView, IndependentPinsExactChannelAndIdempotentCancellation) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_exec_view_lifetime());
}

KTEST(WkiRemoteVfsExecView, SupportRequiresExactLiveResourceObservation) {
    KEXPECT_TRUE(ker::net::wki::wki_remotable_selftest_exec_view_support());
}

KTEST(WkiRemoteVfs, AutomaticMountPathsStayWithinPeerSubtree) { KEXPECT_TRUE(ker::net::wki::wki_remotable_selftest_vfs_mount_path()); }

KTEST(WkiRemoteVfsExecView, CancellationOwnsCapacityReferenceAndExactRetirement) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_exec_cancellation_ownership());
}

KTEST(WkiRemoteVfs, CompletedReadReplySurvivesTransientSendRefusal) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_completed_read_reply_retry());
}

KTEST(WkiRemoteVfsXattr, ReplayIdentityAndDataAreExact) { KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_xattr_replay_fencing()); }

KTEST(WkiRemoteVfsProxySlot, WaitersRemainFifo) { KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_slot_waiter_fifo()); }

KTEST(WkiRemoteVfsProxySlot, StaleCancelPreservesSuccessor) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_stale_cancel_preserves_successor());
}

KTEST(WkiRemoteVfsProxySlot, ResponseClaimRetainsWaiterSlot) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_response_claim_retains_waiter_slot());
}

KTEST(WkiRemoteVfsProxySlot, CompletedResponseCancelReleasesSlot) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_completed_response_cancel_releases_slot());
}

KTEST(WkiRemoteVfsProxySlot, TaskExitReleasesOwnedSlot) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_task_exit_releases_owned_slot());
}

KTEST(WkiRemoteVfsProxySlot, TaskExitDiscoversRetiringSlot) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_task_exit_discovers_retiring_slot());
}

KTEST(WkiRemoteVfsProxySlot, TeardownQuiescesRetiringSlot) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_teardown_quiesces_retiring_slot());
}

KTEST(WkiRemoteVfsProxySlot, InactiveProxyRejectsAcquisition) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_inactive_slot_rejected());
}

KTEST(WkiRemoteVfsWriteBehind, CapacityClassesMatchAllocator) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_write_behind_capacity_classes());
}

KTEST(WkiRemoteVfsWriteBehind, GrowthPreservesPendingData) { KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_write_behind_growth()); }

KTEST(WkiRemoteVfsClose, WritableWaitsForOwnerPublication) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_writable_close_wait_policy());
}

KTEST(WkiRemoteVfsUnmount, BusyOwnerRetriesWithoutRemovingReplacement) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_retired_mount_retry());
}

KTEST(WkiRemoteVfsUnmount, RetiredAncestorDefersSuccessorsUntilOldChildrenDrain) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_retired_ancestor_admission());
}

KTEST(WkiRemoteVfsUnmount, AutomaticDescendantsRetireInBoundedPassesAndPreserveForeignOwners) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_automatic_descendant_retirement());
}

KTEST(WkiRemoteVfsUnmount, ManualRetirementReleasesExactGroupAfterBusyDenial) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_manual_mount_teardown());
}

KTEST(WkiRemoteVfsMount, FailedCandidatePreservesSameResourceMountAndNativeChild) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_mount_candidate_isolation());
}

KTEST(WkiRemoteVfsExport, NativeUnmountCancelPreservesAndCommitRetiresExactBacking) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_native_unmount_exports());
}

KTEST(WkiRemoteVfsClose, AsyncCloseUsesFixedPreAckAdmission) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_async_vfs_close_uses_fixed_admission());
}

KTEST(WkiRemoteVfsReadlinkCache, GenerationInvalidationAndWrap) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_readlink_cache_generation_invalidation());
}

KTEST(WkiRemoteVfsLanes, MultiRdmaSelectionRequiresRequestedDirections) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_multi_rdma_lane_selection());
}

KTEST(WkiRemoteVfsLanes, RoundRobinUsesFullCapacity) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_lane_round_robin_uses_full_capacity());
}

KTEST(WkiRemoteVfsLanes, PressurePrecedesRdmaPreference) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_lane_pressure_precedes_rdma());
}

KTEST(WkiRemoteVfsDirectoryReference, SearchOnlyIdentityNamesAndPurposeScopedRetirement) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_directory_reference_lifetime());
}

KTEST(WkiRemoteVfsDirectoryGrant, ExactCredentialsRecipientScopeClaimAndCancellation) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_directory_grant_lifetime());
}

KTEST(WkiRemoteVfsFileLifetime, RegistryRetirementPreservesAdmittedReferencesAndCloseErrors) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_file_retirement());
}

KTEST(WkiRemoteVfsFileMetadata, ExactFramingCallerSnapshotAndTimestampIntent) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_file_metadata_decoder());
}

KTEST(WkiRemoteVfsFileMetadata, RetainedReadonlyMountRejectsBeforeTransport) {
    using namespace ker::net::wki;
    using ker::mod::security::Snapshot;
    using ker::vfs::Timespec;
    using ker::vfs::TimestampUpdate;

    // Private, disconnected state: allowed requests stop at ENOTCONN. This
    // exercises the actual client APIs without a peer or any packet sends.
    auto proxy = std::make_unique<ProxyVfsState>();
    RemoteFileContext context{};
    context.proxy = proxy.get();
    context.remote_fd = 17;
    context.stat_cache_valid = true;
    context.stat_cache.st_mode = 0644;
    context.stat_cache.st_ino = 123;
    ker::vfs::MountPoint readonly_mount{};
    readonly_mount.fs_type = ker::vfs::FSType::REMOTE;
    readonly_mount.read_only = true;
    ker::vfs::MountPoint writable_mount{};
    writable_mount.fs_type = ker::vfs::FSType::REMOTE;
    ker::vfs::File readonly_file{};
    readonly_file.fs_type = ker::vfs::FSType::REMOTE;
    readonly_file.fops = wki_remote_vfs_get_fops();
    readonly_file.private_data = &context;
    readonly_file.mount_owner = &readonly_mount;
    ker::vfs::File writable_file{};
    writable_file.fs_type = ker::vfs::FSType::REMOTE;
    writable_file.fops = readonly_file.fops;
    writable_file.private_data = &context;
    writable_file.mount_owner = &writable_mount;

    Snapshot restricted_root;
    KEXPECT_EQ(Snapshot::restore_fields({.user = {0, 0, 0}, .group = {0, 0, 0}, .no_new_privileges = true}, restricted_root), 0);
    std::array<Timespec, 2> const OMIT{{{0, TimestampUpdate::OMIT}, {0, TimestampUpdate::OMIT}}};
    std::array<Timespec, 2> const EXPLICIT{{{100, 1}, {200, 2}}};
    std::array<Timespec, 2> const MIXED{{{0, TimestampUpdate::OMIT}, {0, TimestampUpdate::NOW}}};
    std::array<Timespec, 2> const INVALID{{{0, TimestampUpdate::OMIT}, {0, -1}}};
    for (auto const& credentials : std::array{Snapshot::bootstrap_root(), restricted_root, Snapshot{}}) {
        auto const BEFORE = ker::mod::security::credential_record::encode(credentials);
        KEXPECT_EQ(wki_remote_vfs_fchmod(&readonly_file, credentials, 0600), -EROFS);
        KEXPECT_EQ(wki_remote_vfs_fchmod(&readonly_file, credentials, 0644), -EROFS);
        KEXPECT_EQ(wki_remote_vfs_fchown(&readonly_file, credentials, 1000, 1000), -EROFS);
        KEXPECT_EQ(wki_remote_vfs_fchown(&readonly_file, credentials, UINT32_MAX, UINT32_MAX), -EROFS);
        KEXPECT_EQ(wki_remote_vfs_futimens(&readonly_file, credentials, nullptr), -EROFS);
        KEXPECT_EQ(wki_remote_vfs_futimens(&readonly_file, credentials, EXPLICIT.data()), -EROFS);
        KEXPECT_EQ(wki_remote_vfs_futimens(&readonly_file, credentials, MIXED.data()), -EROFS);
        KEXPECT_EQ(wki_remote_vfs_futimens(&readonly_file, credentials, INVALID.data()), -EINVAL);
        KEXPECT_EQ(wki_remote_vfs_futimens(&readonly_file, credentials, OMIT.data()), -ENOTCONN);
        // Sharing a proxy/context does not share the mount restriction.
        KEXPECT_EQ(wki_remote_vfs_fchmod(&writable_file, credentials, 0600), -ENOTCONN);
        KEXPECT_EQ(wki_remote_vfs_fchown(&writable_file, credentials, 1000, 1000), -ENOTCONN);
        KEXPECT_EQ(wki_remote_vfs_futimens(&writable_file, credentials, EXPLICIT.data()), -ENOTCONN);
        KEXPECT_EQ(wki_remote_vfs_futimens(&writable_file, credentials, INVALID.data()), -EINVAL);
        KEXPECT_TRUE(ker::mod::security::credential_record::encode(credentials) == BEFORE);
    }
    auto const ROOT = Snapshot::bootstrap_root();
    KEXPECT_EQ(wki_remote_vfs_fchmod(nullptr, ROOT, 0600), -EBADF);
    KEXPECT_EQ(wki_remote_vfs_fchown(nullptr, ROOT, 1000, 1000), -EBADF);
    KEXPECT_EQ(wki_remote_vfs_futimens(nullptr, ROOT, INVALID.data()), -EBADF);
    ker::vfs::File invalid_file{};
    invalid_file.mount_owner = &readonly_mount;
    KEXPECT_EQ(wki_remote_vfs_fchmod(&invalid_file, ROOT, 0600), -EBADF);
    KEXPECT_EQ(wki_remote_vfs_futimens(&invalid_file, ROOT, INVALID.data()), -EBADF);
    // A retained retiring mount remains authoritative even with a live proxy.
    readonly_mount.retiring.store(true);
    proxy->active = true;
    KEXPECT_EQ(wki_remote_vfs_fchmod(&readonly_file, ROOT, 0600), -EROFS);
    KEXPECT_EQ(wki_remote_vfs_fchown(&readonly_file, ROOT, 1000, 1000), -EROFS);
    KEXPECT_EQ(wki_remote_vfs_futimens(&readonly_file, ROOT, EXPLICIT.data()), -EROFS);
    KEXPECT_TRUE(context.stat_cache_valid);
    KEXPECT_EQ(context.stat_cache.st_mode, 0644U);
    KEXPECT_EQ(context.stat_cache.st_ino, 123U);
    KEXPECT_FALSE(context.cache_invalidation_pending.load());
    KEXPECT_EQ(context.remote_fd, 17);
    KEXPECT_FALSE(proxy->op_pending.load());
    KEXPECT_EQ(proxy->op_generation, 0U);
    KEXPECT_EQ(proxy->op_slot_waiter_count, 0U);
    KEXPECT_EQ(readonly_mount.refs.load(), 0U);
    KEXPECT_EQ(readonly_mount.open_files.load(), 0U);
}

KTEST(WkiRemoteVfsDirectoryGrant, NativeOwnerRetainsScopeAndCancelsWithoutPeerBinding) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_native_directory_grant());
}

KTEST(WkiRemoteVfsDirectoryGrant, AbandonedPreparationQueuesCancellationAndReleasesPrivateState) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_directory_grant_preparation());
}

KTEST(WkiRemoteVfsPinPreparation, DirectoryAndExecutableLifetimesKeepCancellationPurpose) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_pin_preparation_purpose());
}

KTEST(WkiRemoteVfsPinClose, AbandonedCloseReleasesFilePathMountAndExactCancellation) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_pin_close_lifetime());
}
