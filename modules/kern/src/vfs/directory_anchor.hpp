#pragma once

namespace ker::vfs {
struct DirectoryAnchor;
// A native or remote directory identity with mount busy accounting. Copying only retains
// an atomic reference; final release may take filesystem locks and must run in
// a sleepable context outside Task path spinlocks and namespace publication.
class DirectoryAnchorRef {
   public:
    DirectoryAnchorRef() = default;
    explicit DirectoryAnchorRef(DirectoryAnchor* anchor) : anchor_(anchor) {}
    DirectoryAnchorRef(const DirectoryAnchorRef& other);
    DirectoryAnchorRef(DirectoryAnchorRef&& other) noexcept;
    auto operator=(const DirectoryAnchorRef& other) -> DirectoryAnchorRef&;
    auto operator=(DirectoryAnchorRef&& other) noexcept -> DirectoryAnchorRef&;
    ~DirectoryAnchorRef();
    auto get() const -> DirectoryAnchor* { return anchor_; }
    explicit operator bool() const { return anchor_ != nullptr; }
    void swap(DirectoryAnchorRef& other) noexcept;

   private:
    DirectoryAnchor* anchor_ = nullptr;
};
}  // namespace ker::vfs
