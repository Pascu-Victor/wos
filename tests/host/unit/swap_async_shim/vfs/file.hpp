#pragma once
namespace ker::vfs {
enum class FSType { XFS };
struct File {
    FSType fs_type{FSType::XFS};
    bool is_directory{};
};
}  // namespace ker::vfs
