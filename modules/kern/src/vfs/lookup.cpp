#include "lookup.hpp"

#include <cstdint>
#include <cstring>
#include <utility>
#include <vfs/vfs.hpp>

namespace ker::vfs {

LookupHandle::LookupHandle(LookupHandle&& other) noexcept { *this = std::move(other); }

auto LookupHandle::operator=(LookupHandle&& other) noexcept -> LookupHandle& {
    if (this == &other) {
        return *this;
    }
    reset();
    path_ = other.path_;
    parent_path_ = other.parent_path_;
    final_component_ = other.final_component_;
    remote_relative_path_ = other.remote_relative_path_;
    path_len_ = other.path_len_;
    parent_path_len_ = other.parent_path_len_;
    final_component_len_ = other.final_component_len_;
    intent_ = other.intent_;
    follow_policy_ = other.follow_policy_;
    require_directory_ = other.require_directory_;
    mount_ref_ = std::move(other.mount_ref_);
    mount_policy_ = other.mount_policy_;
    directory_view_ = std::move(other.directory_view_);
    mount_membership_ = std::exchange(other.mount_membership_, nullptr);
    release_mount_membership_ = std::exchange(other.release_mount_membership_, nullptr);
    procfs_view_origin_ = std::exchange(other.procfs_view_origin_, nullptr);
    release_procfs_view_origin_ = std::exchange(other.release_procfs_view_origin_, nullptr);
    procfs_view_generation_ = std::exchange(other.procfs_view_generation_, 0);
    mount_generation_ = other.mount_generation_;
    namespace_generation_ = other.namespace_generation_;
    backend_generation_ = other.backend_generation_;
    authorization_ = other.authorization_;
    dirfd_anchor_ = other.dirfd_anchor_;
    dirfd_path_revalidated_ = other.dirfd_path_revalidated_;
    backend_parent_ = other.backend_parent_;
    backend_object_ = other.backend_object_;
    release_parent_ = other.release_parent_;
    release_object_ = other.release_object_;
    parent_identity_ = other.parent_identity_;
    object_identity_ = other.object_identity_;
    parent_generation_ = other.parent_generation_;

    other.dirfd_anchor_ = nullptr;
    other.authorization_ = {};
    other.mount_policy_ = {};
    other.dirfd_path_revalidated_ = false;
    other.backend_parent_ = nullptr;
    other.backend_object_ = nullptr;
    other.release_parent_ = nullptr;
    other.release_object_ = nullptr;
    other.path_len_ = 0;
    other.parent_path_len_ = 0;
    other.final_component_len_ = 0;
    other.remote_relative_path_.fill('\0');
    return *this;
}

LookupHandle::~LookupHandle() { reset(); }

void LookupHandle::reset() {
    if (procfs_view_origin_ != nullptr && release_procfs_view_origin_ != nullptr) {
        release_procfs_view_origin_(procfs_view_origin_);
    }
    procfs_view_origin_ = nullptr;
    release_procfs_view_origin_ = nullptr;
    procfs_view_generation_ = 0;
    if (backend_object_ != nullptr && release_object_ != nullptr) {
        release_object_(backend_object_);
    }
    if (backend_parent_ != nullptr && release_parent_ != nullptr) {
        release_parent_(backend_parent_);
    }
    if (dirfd_anchor_ != nullptr) {
        vfs_put_file(dirfd_anchor_);
    }
    backend_object_ = nullptr;
    backend_parent_ = nullptr;
    release_object_ = nullptr;
    release_parent_ = nullptr;
    dirfd_anchor_ = nullptr;
    dirfd_path_revalidated_ = false;
    if (mount_membership_ != nullptr && release_mount_membership_ != nullptr) {
        release_mount_membership_(mount_membership_);
    }
    mount_membership_ = nullptr;
    release_mount_membership_ = nullptr;
    directory_view_ = {};
    mount_ref_.reset();
    mount_policy_ = {};
    authorization_ = {};
    path_.fill('\0');
    parent_path_.fill('\0');
    final_component_.fill('\0');
    remote_relative_path_.fill('\0');
    path_len_ = 0;
    parent_path_len_ = 0;
    final_component_len_ = 0;
    mount_generation_ = 0;
    namespace_generation_ = 0;
    backend_generation_ = 0;
    parent_identity_ = 0;
    object_identity_ = 0;
    parent_generation_ = 0;
}

auto LookupHandleBuilder::set_remote_relative_path(LookupHandle& handle, const char* path, size_t length) -> bool {
    if (path == nullptr || length == 0 || length >= handle.remote_relative_path_.size() || path[0] == '/') return false;
    handle.remote_relative_path_.fill('\0');
    std::memcpy(handle.remote_relative_path_.data(), path, length);
    return true;
}

void LookupHandleBuilder::set_path(LookupHandle& handle, const char* path, size_t path_len, const char* parent_path, size_t parent_path_len,
                                   const char* component, size_t component_len) {
    handle.path_.fill('\0');
    handle.parent_path_.fill('\0');
    handle.final_component_.fill('\0');
    handle.path_len_ = 0;
    handle.parent_path_len_ = 0;
    handle.final_component_len_ = 0;
    if (path != nullptr && path_len < handle.path_.size()) {
        std::memcpy(handle.path_.data(), path, path_len);
        handle.path_len_ = path_len;
    }
    if (parent_path != nullptr && parent_path_len < handle.parent_path_.size()) {
        std::memcpy(handle.parent_path_.data(), parent_path, parent_path_len);
        handle.parent_path_len_ = parent_path_len;
    }
    if (component != nullptr && component_len < handle.final_component_.size()) {
        std::memcpy(handle.final_component_.data(), component, component_len);
        handle.final_component_len_ = component_len;
    }
}

void LookupHandleBuilder::set_policy(LookupHandle& handle, LookupIntent intent, LookupFollowPolicy follow_policy, bool require_directory,
                                     const LookupAuthorizationContext& authorization) {
    handle.intent_ = intent;
    handle.follow_policy_ = follow_policy;
    handle.require_directory_ = require_directory;
    handle.authorization_ = authorization;
}

void LookupHandleBuilder::set_mount(LookupHandle& handle, MountRef mount, uint64_t mount_generation, uint64_t namespace_generation,
                                    uint64_t backend_generation) {
    handle.mount_ref_ = std::move(mount);
    auto* selected = handle.mount_ref_.get();
    handle.mount_policy_ = selected != nullptr ? MountPolicy{.read_only = selected->read_only, .nosuid = selected->nosuid} : MountPolicy{};
    handle.mount_generation_ = mount_generation;
    handle.namespace_generation_ = namespace_generation;
    handle.backend_generation_ = backend_generation;
}

void LookupHandleBuilder::set_directory_view(LookupHandle& handle, const DirectoryViewRef& view) { handle.directory_view_ = view; }

auto LookupHandleBuilder::set_mount_membership(LookupHandle& handle, void* retained, LookupHandle::BackendRelease release) -> bool {
    if (retained == nullptr || release == nullptr || handle.mount_membership_ != nullptr) return false;
    handle.mount_membership_ = retained;
    handle.release_mount_membership_ = release;
    return true;
}

void LookupHandleBuilder::set_procfs_view_origin(LookupHandle& handle, File* retained_origin, LookupHandle::BackendRelease release,
                                                 uint64_t credential_generation) {
    if (handle.procfs_view_origin_ != nullptr && handle.release_procfs_view_origin_ != nullptr) {
        handle.release_procfs_view_origin_(handle.procfs_view_origin_);
    }
    handle.procfs_view_origin_ = retained_origin;
    handle.release_procfs_view_origin_ = release;
    handle.procfs_view_generation_ = credential_generation;
}

void LookupHandleBuilder::set_dirfd_anchor(LookupHandle& handle, File* retained_file, bool path_revalidated) {
    if (handle.dirfd_anchor_ != nullptr) {
        vfs_put_file(handle.dirfd_anchor_);
    }
    handle.dirfd_anchor_ = retained_file;
    handle.dirfd_path_revalidated_ = path_revalidated;
}

void LookupHandleBuilder::set_backend_binding(LookupHandle& handle, void* retained_parent, LookupHandle::BackendRelease release_parent,
                                              uint64_t parent_identity, uint64_t parent_generation, void* retained_object,
                                              LookupHandle::BackendRelease release_object, uint64_t object_identity) {
    if (handle.backend_object_ != nullptr && handle.release_object_ != nullptr) {
        handle.release_object_(handle.backend_object_);
    }
    if (handle.backend_parent_ != nullptr && handle.release_parent_ != nullptr) {
        handle.release_parent_(handle.backend_parent_);
    }
    handle.backend_parent_ = retained_parent;
    handle.release_parent_ = release_parent;
    handle.parent_identity_ = parent_identity;
    handle.parent_generation_ = parent_generation;
    handle.backend_object_ = retained_object;
    handle.release_object_ = release_object;
    handle.object_identity_ = object_identity;
}

}  // namespace ker::vfs
