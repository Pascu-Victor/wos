#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <platform/security/credentials.hpp>
#include <vfs/directory_view.hpp>
#include <vfs/mount.hpp>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::vfs {

struct File;

// Pathname intent is part of a lookup handle rather than an implicit property
// of the later consumer. In particular, create/remove operations retain the
// parent/name binding while read-only operations retain the final object.
enum class LookupIntent : uint8_t {
    OPEN,
    STAT,
    ACCESS,
    READLINK,
    CREATE,
    REMOVE,
    RENAME_SOURCE,
    RENAME_TARGET,
    LINK_SOURCE,
    LINK_TARGET,
    METADATA,
    MOUNT_TARGET,
    CREATE_DIRECTORY,
    CREATE_SYMLINK,
    CHANGE_MODE,  // Explicitly mapped remote metadata operation.
    CHANGE_TIMES,
    XATTR,  // Mapped extended attributes preserve raw remote components.
    CHANGE_OWNER,
    MOUNT_SOURCE,  // Retained devfs provider selection before mount I/O.
};

enum class LookupFollowPolicy : uint8_t {
    FOLLOW_FINAL,
    NOFOLLOW_FINAL,
};

struct LookupAuthorizationContext {
    uint64_t task_id{};
    // Nonreused subject mount-view identity, retained by the selected MountRef.
    // This value validates ownership; it cannot be converted into a join grant.
    uint64_t mount_namespace_generation{};
    ker::mod::security::Snapshot credentials{};
    uint64_t root_hash{};
    uint64_t cwd_hash{};
    // Value identities detect same-text root/cwd replacement without retaining
    // anchors whose final release could block under namespace publication.
    uint64_t root_anchor_identity{};
    uint64_t cwd_anchor_identity{};
    // A process-root-only view is an identity boundary. Component walking
    // supplies search grants for absolute starts and ordinary relative dirfds.
    bool root_identity_view{};
    // Whole-path native search admissions; zero is untracked for manually built
    // backend-only handles. Capture before the first component/fallback search.
    uint64_t tmpfs_search_generation{};
    uint64_t xfs_search_generation{};
};

// A short-lived, move-only pathname result. The mount and optional dirfd File
// keep their owning storage alive. Backends install retained parent/final
// identities together with release callbacks; a consumer must validate the
// recorded generations under that backend's namespace lock before acting.
//
// Handles are request-local. They must never be placed on the WKI wire or held
// across an unbounded wait, and they must be released before retrying a stale
// lookup so unmount retirement can drain. Handles carrying a directory view
// can close its Files on final release: destroy/reset/replace outside namespace
// publication, path and registry locks.
class LookupHandle {
   public:
    static constexpr size_t PATH_CAPACITY = MOUNT_PATH_MAX;
    static constexpr size_t COMPONENT_CAPACITY = 256;
    using BackendRelease = void (*)(void*);

    LookupHandle() = default;
    LookupHandle(const LookupHandle&) = delete;
    auto operator=(const LookupHandle&) -> LookupHandle& = delete;
    LookupHandle(LookupHandle&& other) noexcept;
    auto operator=(LookupHandle&& other) noexcept -> LookupHandle&;
    ~LookupHandle();

    void reset();

    [[nodiscard]] auto mount() const -> MountPoint* { return mount_ref_.get(); }
    // Selected native flags and explicitly inherited restrictions stay separate.
    // An ordinary native dirfd/cwd does not constrain unrelated mounts.
    [[nodiscard]] auto mount_policy() const -> MountPolicy {
        return directory_view_ ? mount_policy_.restricted_by(directory_view_.policy()) : mount_policy_;
    }
    [[nodiscard]] auto directory_view() const -> const DirectoryViewRef& { return directory_view_; }
    // Immutable native edge ownership recorded during acquisition. Opaque to
    // backends and never a wire identity; only VFS core interprets this cookie.
    [[nodiscard]] auto mount_membership() const -> const void* { return mount_membership_; }
    [[nodiscard]] auto procfs_view_origin() const -> File* { return procfs_view_origin_; }
    [[nodiscard]] auto procfs_view_generation() const -> uint64_t { return procfs_view_generation_; }
    [[nodiscard]] auto path() const -> const char* { return path_.data(); }
    [[nodiscard]] auto parent_path() const -> const char* { return parent_path_.data(); }
    [[nodiscard]] auto final_component() const -> const char* { return final_component_.data(); }
    [[nodiscard]] auto path_length() const -> size_t { return path_len_; }
    [[nodiscard]] auto parent_path_length() const -> size_t { return parent_path_len_; }
    [[nodiscard]] auto final_component_length() const -> size_t { return final_component_len_; }
    [[nodiscard]] auto intent() const -> LookupIntent { return intent_; }
    [[nodiscard]] auto follow_policy() const -> LookupFollowPolicy { return follow_policy_; }
    [[nodiscard]] auto requires_directory() const -> bool { return require_directory_; }
    [[nodiscard]] auto mount_generation() const -> uint64_t { return mount_generation_; }
    [[nodiscard]] auto namespace_generation() const -> uint64_t { return namespace_generation_; }
    [[nodiscard]] auto backend_generation() const -> uint64_t { return backend_generation_; }
    [[nodiscard]] auto authorization() const -> const LookupAuthorizationContext& { return authorization_; }
    [[nodiscard]] auto dirfd_anchor() const -> File* { return dirfd_anchor_; }
    [[nodiscard]] auto dirfd_path_revalidated() const -> bool { return dirfd_path_revalidated_; }
    [[nodiscard]] auto remote_relative_path() const -> const char* {
        return remote_relative_path_[0] != '\0' ? remote_relative_path_.data() : nullptr;
    }
    [[nodiscard]] auto backend_parent() const -> void* { return backend_parent_; }
    [[nodiscard]] auto backend_object() const -> void* { return backend_object_; }
    [[nodiscard]] auto parent_identity() const -> uint64_t { return parent_identity_; }
    [[nodiscard]] auto object_identity() const -> uint64_t { return object_identity_; }
    [[nodiscard]] auto parent_generation() const -> uint64_t { return parent_generation_; }

   private:
    friend struct LookupHandleBuilder;

    std::array<char, PATH_CAPACITY> path_{};
    std::array<char, PATH_CAPACITY> parent_path_{};
    std::array<char, COMPONENT_CAPACITY> final_component_{};
    // Descriptor-relative remote authority is independent of compatibility path text.
    std::array<char, PATH_CAPACITY> remote_relative_path_{};
    size_t path_len_{};
    size_t parent_path_len_{};
    size_t final_component_len_{};
    LookupIntent intent_{LookupIntent::STAT};
    LookupFollowPolicy follow_policy_{LookupFollowPolicy::FOLLOW_FINAL};
    bool require_directory_{};
    MountRef mount_ref_{};
    MountPolicy mount_policy_{};
    DirectoryViewRef directory_view_{};
    void* mount_membership_{};
    BackendRelease release_mount_membership_{};
    // A magic-link selection retains its private process authorization until
    // native commit. This metadata cookie is separate from the target backend.
    File* procfs_view_origin_{};
    BackendRelease release_procfs_view_origin_{};
    uint64_t procfs_view_generation_{};
    uint64_t mount_generation_{};
    uint64_t namespace_generation_{};
    uint64_t backend_generation_{};
    LookupAuthorizationContext authorization_{};
    File* dirfd_anchor_{};
    bool dirfd_path_revalidated_{};
    void* backend_parent_{};
    void* backend_object_{};
    BackendRelease release_parent_{};
    BackendRelease release_object_{};
    uint64_t parent_identity_{};
    uint64_t object_identity_{};
    uint64_t parent_generation_{};
};

// The builder is intentionally the only mutating view of LookupHandle. VFS
// path policy and each backend adapter populate it, while syscall consumers
// receive a read-only capability.
struct LookupHandleBuilder {
    static void set_path(LookupHandle& handle, const char* path, size_t path_len, const char* parent_path, size_t parent_path_len,
                         const char* component, size_t component_len);
    static void set_policy(LookupHandle& handle, LookupIntent intent, LookupFollowPolicy follow_policy, bool require_directory,
                           const LookupAuthorizationContext& authorization);
    static void set_mount(LookupHandle& handle, MountRef mount, uint64_t mount_generation, uint64_t namespace_generation,
                          uint64_t backend_generation);
    // Install only on an unpublished candidate. Replacement/final release
    // must occur outside path, registry and namespace publication locks.
    static void set_directory_view(LookupHandle& handle, const DirectoryViewRef& view);
    // Take sole ownership only on an empty unpublished candidate. Failure
    // preserves both handle and caller ownership; this never releases a cookie.
    static auto set_mount_membership(LookupHandle& handle, void* retained, LookupHandle::BackendRelease release) -> bool;
    static void set_procfs_view_origin(LookupHandle& handle, File* retained_origin, LookupHandle::BackendRelease release,
                                       uint64_t credential_generation);
    static void set_dirfd_anchor(LookupHandle& handle, File* retained_file, bool path_revalidated = false);
    static auto set_remote_relative_path(LookupHandle& handle, const char* path, size_t length) -> bool;
    static void set_backend_binding(LookupHandle& handle, void* retained_parent, LookupHandle::BackendRelease release_parent,
                                    uint64_t parent_identity, uint64_t parent_generation, void* retained_object,
                                    LookupHandle::BackendRelease release_object, uint64_t object_identity);
};

// Resolve one pathname into a short-lived retained lookup capability. Local
// filesystems pin their backend parent/final object; remote filesystems retain
// only the mount and exact admitted owner-relative pathname because the owner
// performs lookup and commit in one request. A stale local binding returns
// -EAGAIN and callers may retry at most max_restarts times. An optional
// kernel-owned admission snapshot is used unchanged by every lookup attempt;
// the caller must keep it alive and immutable until this function returns.
auto vfs_acquire_lookup(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, LookupIntent intent,
                        LookupFollowPolicy follow_policy, bool require_directory, bool apply_task_route, LookupHandle* out,
                        unsigned max_restarts = 3, bool already_resolved = false,
                        const ker::mod::security::Snapshot* admitted_credentials = nullptr) -> int;

// Resolve an owner-relative name through a complete retained native view.
// Absolute symlinks default to physical-path semantics. Optional paired export
// path/name maps captured visible links before intersecting the retained root. No
// task route or text/cache fallback is used. Cross-mount walks return EXDEV
// until retained covered-mount ancestry is available. One admitted Snapshot is
// kept throughout; failure preserves output. Release view handles outside locks.
// This does not install a process cwd, publish a directory File or map wire IDs.
auto vfs_acquire_directory_view_lookup(ker::mod::sched::task::Task& task, const ker::mod::security::Snapshot& credentials,
                                       const DirectoryViewRef& view, const char* relative_path, LookupIntent intent,
                                       LookupFollowPolicy follow_policy, bool require_directory, LookupHandle& output,
                                       const char* export_path = nullptr, const char* export_name = nullptr) -> int;

// Identity retention does not authorize directory reading or child traversal.
// OPEN and CHDIR still make their own access decisions. Every later view lookup
// checks current search permission, including views retained without search.
enum class DirectoryViewAdmission { SEARCH, IDENTITY_ONLY };

// Derive an independent cwd view from a scoped lookup's selected directory.
// Preserve its retained root and inherited policy; do not retain a parent-view
// chain. SEARCH is the default for cwd use; IDENTITY_ONLY retains a directory
// without adding execute permission to read-only directory OPEN. Exact root
// membership is always revalidated after pinning; SEARCH also rechecks access. Failure preserves output. Caller retains
// the lookup; this prepares ownership without publishing File/cwd/wire state.
auto vfs_rebase_directory_view(ker::mod::sched::task::Task& task, const ker::mod::security::Snapshot& credentials,
                               const LookupHandle& lookup, DirectoryViewRef& output,
                               DirectoryViewAdmission admission = DirectoryViewAdmission::SEARCH) -> int;

using LookupConsumer = int (*)(const LookupHandle& handle, void* context);

// Validate and consume a local lookup while namespace publication is
// serialized. A successful namespace mutation advances the global generation
// before the publication lock is released. Remote handles deliberately return
// -EXDEV: their consumer is the owner-side WKI request handler instead.
auto vfs_consume_lookup(const LookupHandle& handle, bool namespace_mutation, LookupConsumer consumer, void* context) -> int;
using LookupPairConsumer = int (*)(const LookupHandle& first, const LookupHandle& second, void* context);
auto vfs_consume_lookup_pair(const LookupHandle& first, const LookupHandle& second, bool namespace_mutation, LookupPairConsumer consumer,
                             void* context) -> int;

}  // namespace ker::vfs
