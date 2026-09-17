#pragma once

namespace ker::mod::security {
class Snapshot;
}

namespace ker::vfs {
struct File;
struct MountPolicy;
struct DirectoryView;

// Immutable inherited policy and retained search-only root/cwd identities.
// Copying only retains an atomic reference. Final release can close native
// Files: release/replace outside path, registry and namespace publication locks.
class DirectoryViewRef {
   public:
    DirectoryViewRef() = default;
    explicit DirectoryViewRef(DirectoryView* view) : view_(view) {}
    DirectoryViewRef(const DirectoryViewRef& other);
    DirectoryViewRef(DirectoryViewRef&& other) noexcept;
    auto operator=(const DirectoryViewRef& other) -> DirectoryViewRef&;
    auto operator=(DirectoryViewRef&& other) noexcept -> DirectoryViewRef&;
    ~DirectoryViewRef();
    explicit operator bool() const { return view_ != nullptr; }
    auto get() const -> const DirectoryView* { return view_; }
    // Borrowed until this reference is released. They grant identity/search,
    // not data access; operations must still admit their current credentials.
    auto root() const -> File*;
    auto directory() const -> File*;
    auto policy() const -> MountPolicy;
    void swap(DirectoryViewRef& other) noexcept;

   private:
    DirectoryView* view_ = nullptr;
};

// Pins and source must be retained by the caller. These helpers create owned
// view descriptions only; they do not install a cwd/dirfd or transfer a grant.
// Supported native root/cwd mount chains use current retained covered-object
// proof; missing/stale proof is refused. Native scoped walks record covered
// edges and consumers revalidate the selected directory/parent beneath root.
// Native mount flags remain per-lookup; inherited_policy is explicitly admitted
// source-view policy. Failure preserves output. Final replacement is unlocked.
auto vfs_prepare_directory_view(const ker::mod::security::Snapshot& credentials, File* root, File* directory, MountPolicy inherited_policy,
                                DirectoryViewRef& output) -> int;
auto vfs_narrow_directory_view(const DirectoryViewRef& source, const ker::mod::security::Snapshot& credentials, File* root, File* directory,
                               MountPolicy additional_policy, DirectoryViewRef& output) -> int;
}  // namespace ker::vfs
