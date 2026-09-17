#pragma once

#include <platform/sched/task.hpp>

#include "directory_view.hpp"

namespace ker::vfs {
// Request-local metadata observation, not a lookup grant. The caller retains
// the subject throughout capture/validation and releases this owner outside
// path, content and namespace locks. It captures no cwd or remote grant.
class FilesystemRootMetadata {
   public:
    static constexpr size_t PATH_CAPACITY = 512;
    FilesystemRootMetadata() = default;
    FilesystemRootMetadata(const FilesystemRootMetadata&) = delete;
    auto operator=(const FilesystemRootMetadata&) -> FilesystemRootMetadata& = delete;
    FilesystemRootMetadata(FilesystemRootMetadata&&) noexcept = default;
    auto operator=(FilesystemRootMetadata&&) noexcept -> FilesystemRootMetadata& = default;
    auto path() const -> const char* { return physical_.data(); }
    auto read_only() const -> bool { return read_only_; }
    auto nosuid() const -> bool { return nosuid_; }

   private:
    friend auto vfs_capture_root_metadata(ker::mod::sched::task::Task*, FilesystemRootMetadata&) -> int;
    friend auto vfs_root_metadata_current(ker::mod::sched::task::Task*, const FilesystemRootMetadata&) -> bool;
    const ker::mod::sched::task::Task* owner_ = nullptr;
    ker::mod::sched::task::Task::PathBuffer text_{};
    std::array<char, PATH_CAPACITY> physical_{};
    DirectoryAnchorRef anchor_;
    uint64_t namespace_generation_ = 0;
    uint64_t mount_generation_ = 0;
    bool read_only_ = false;
    bool nosuid_ = false;
    bool initialized_ = false;
};

// Caller authorizes observation before capture; both APIs run outside namespace
// publication. A null subject observes global root for explicit boot/model use.
// Capture failure leaves output unchanged; validation compares values only and
// does not retire ownership or authorize any subsequent filesystem operation.
auto vfs_capture_root_metadata(ker::mod::sched::task::Task* task, FilesystemRootMetadata& output) -> int;
auto vfs_root_metadata_current(ker::mod::sched::task::Task* task, const FilesystemRootMetadata& captured) -> bool;

// Owned native preparation only: not an installed process root or a transferable
// grant. A future commit must revalidate admission while publishing both anchors;
// these captured epochs never authorize later effects by themselves. Final
// release/replacement requires a sleepable context outside publication/path locks.
class PreparedFilesystemContext {
   public:
    PreparedFilesystemContext() = default;
    PreparedFilesystemContext(const PreparedFilesystemContext&) = delete;
    auto operator=(const PreparedFilesystemContext&) -> PreparedFilesystemContext& = delete;
    PreparedFilesystemContext(PreparedFilesystemContext&&) noexcept = default;
    auto operator=(PreparedFilesystemContext&&) noexcept -> PreparedFilesystemContext& = default;
    explicit operator bool() const { return filesystem_.root_anchor && filesystem_.cwd_anchor; }
    auto root_anchor() const -> const DirectoryAnchorRef& { return filesystem_.root_anchor; }
    auto filesystem() const -> const ker::mod::sched::task::Task::FilesystemContext& { return filesystem_; }
    auto credentials() const -> const ker::mod::security::Snapshot& { return credentials_; }
    auto namespace_generation() const -> uint64_t { return namespace_generation_; }
    auto mount_generation() const -> uint64_t { return mount_generation_; }
    auto directory_view() const -> DirectoryViewRef;

   private:
    friend auto vfs_prepare_filesystem_context(const ker::mod::security::Snapshot&, const DirectoryViewRef&, PreparedFilesystemContext&)
        -> int;
    friend auto vfs_prepare_filesystem_context(const ker::mod::security::Snapshot&, const DirectoryViewRef&, const DirectoryViewRef&,
                                               PreparedFilesystemContext&) -> int;
    ker::mod::sched::task::Task::FilesystemContext filesystem_;
    ker::mod::security::Snapshot credentials_;
    uint64_t namespace_generation_ = 0;
    uint64_t mount_generation_ = 0;
};

// Caller retains source and supplies admitted credentials. Native root/cwd can
// span proven current covered-mount links. Each anchor uses its own backend;
// removed cwd returns ENOENT. Failure preserves output. This staging API does
// not install either anchor or enable scoped lookup mount crossing.
// Display paths are recovered from identity, never used to reacquire authority.
auto vfs_prepare_filesystem_context(const ker::mod::security::Snapshot& credentials, const DirectoryViewRef& source,
                                    PreparedFilesystemContext& output) -> int;

// The process root and cwd can have different retained boundaries. The root of
// process_root selects the process root; cwd selects the retained directory.
// Intersect their boundaries by native identity and union inherited cwd policy.
// Absolute lookup keeps the process-root policy, independently of cwd policy.
auto vfs_prepare_filesystem_context(const ker::mod::security::Snapshot& credentials, const DirectoryViewRef& process_root,
                                    const DirectoryViewRef& cwd, PreparedFilesystemContext& output) -> int;

// Bind an explicitly selected legacy physical root for native membership
// preparation. Identity only: this does not renew root search or install a Task.
// Caller owns the immutable path; failure preserves the previous view.
auto vfs_prepare_root_directory_view_as(ker::mod::sched::task::Task& task, const ker::mod::security::Snapshot& credentials,
                                        const char* physical_root, DirectoryViewRef& output) -> int;

// Borrowed from a caller-retained snapshot, never from current Task state.
// Root transfer support is a separate admission decision. Do not hide a retained
// object behind that decision and accidentally select legacy pathname fallback.
auto vfs_captured_remote_cwd(const ker::mod::sched::task::Task::FilesystemContext& filesystem) -> File*;

// Admit legacy text submission only when it cannot discard retained scope.
// Reads caller-owned immutable anchors without locks, allocation or release.
// Mismatched cwd labels return ESTALE; views, remote cwd and owned roots need
// negotiated transfer and return ENOTSUP. Unscoped native text routing remains.
auto vfs_validate_text_filesystem_submission(const ker::mod::sched::task::Task::FilesystemContext& filesystem) -> int;

// Prepare a native cwd while preserving global process-root intent. The view's
// boundary and inherited restrictions remain attached to relative lookups.
// Names are recovered from the retained identity; failures preserve output.
auto vfs_prepare_global_cwd(const ker::mod::security::Snapshot& credentials, const DirectoryViewRef& source,
                            ker::mod::sched::task::Task::FilesystemContext& output) -> int;
}  // namespace ker::vfs
