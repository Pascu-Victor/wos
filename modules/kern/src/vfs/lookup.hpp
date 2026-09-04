#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
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
};

enum class LookupFollowPolicy : uint8_t {
    FOLLOW_FINAL,
    NOFOLLOW_FINAL,
};

struct LookupAuthorizationContext {
    uint64_t task_id{};
    uint32_t uid{};
    uint32_t gid{};
    uint32_t euid{};
    uint32_t egid{};
    uint32_t suid{};
    uint32_t sgid{};
    uint64_t supplementary_groups_hash{};
    uint64_t root_hash{};
    uint64_t cwd_hash{};
};

// A short-lived, move-only pathname result. The mount and optional dirfd File
// keep their owning storage alive. Backends install retained parent/final
// identities together with release callbacks; a consumer must validate the
// recorded generations under that backend's namespace lock before acting.
//
// Handles are request-local. They must never be placed on the WKI wire or held
// across an unbounded wait, and they must be released before retrying a stale
// lookup so unmount retirement can drain.
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
    size_t path_len_{};
    size_t parent_path_len_{};
    size_t final_component_len_{};
    LookupIntent intent_{LookupIntent::STAT};
    LookupFollowPolicy follow_policy_{LookupFollowPolicy::FOLLOW_FINAL};
    bool require_directory_{};
    MountRef mount_ref_{};
    uint64_t mount_generation_{};
    uint64_t namespace_generation_{};
    uint64_t backend_generation_{};
    LookupAuthorizationContext authorization_{};
    File* dirfd_anchor_{};
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
    static void set_dirfd_anchor(LookupHandle& handle, File* retained_file);
    static void set_backend_binding(LookupHandle& handle, void* retained_parent, LookupHandle::BackendRelease release_parent,
                                    uint64_t parent_identity, uint64_t parent_generation, void* retained_object,
                                    LookupHandle::BackendRelease release_object, uint64_t object_identity);
};

// Resolve one pathname into a short-lived retained lookup capability. Local
// filesystems pin their backend parent/final object; remote filesystems retain
// only the mount and exact admitted owner-relative pathname because the owner
// performs lookup and commit in one request. A stale local binding returns
// -EAGAIN and callers may retry at most max_restarts times.
auto vfs_acquire_lookup(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, LookupIntent intent,
                        LookupFollowPolicy follow_policy, bool require_directory, bool apply_task_route, LookupHandle* out,
                        unsigned max_restarts = 3, bool already_resolved = false) -> int;

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
