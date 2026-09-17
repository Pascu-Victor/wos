#pragma once

#include <platform/security/credentials.hpp>

namespace ker::mod::security {

struct FileIdentity {
    uint32_t uid{};
    uint32_t gid{};
    uint32_t mode{};
    constexpr auto operator==(const FileIdentity&) const -> bool = default;
};

enum class FileXattrNamespace : uint8_t { USER, TRUSTED, SECURITY };

[[nodiscard]] constexpr auto authorize_file_access(const Snapshot& caller, const FileIdentity& object, int access_bits) -> int {
    unsigned const REQUESTED = static_cast<unsigned>(access_bits) & 7U;
    if (REQUESTED == 0) return 0;
    if (caller.allows(Authority::DAC_OVERRIDE)) {
        return (REQUESTED & 1U) != 0 && (object.mode & 0170000U) != 0040000U && (object.mode & 0111U) == 0 ? -EACCES : 0;
    }
    unsigned const SHIFT = caller.user().effective == object.uid ? 6U : (caller.has_group(object.gid) ? 3U : 0U);
    unsigned const PERMITTED = (object.mode >> SHIFT) & 7U;
    return (PERMITTED & REQUESTED) == REQUESTED ? 0 : -EACCES;
}

// access_mode is the existing WOS O_ACCMODE value (0/1/2). A truncating
// read-only open still needs write authority before changing the object.
[[nodiscard]] constexpr auto authorize_file_open(const Snapshot& caller, const FileIdentity& object, unsigned access_mode, bool truncate)
    -> int {
    if (access_mode > 2) return -EINVAL;
    if ((object.mode & 0170000U) == 0040000U && (access_mode != 0 || truncate)) return -EISDIR;
    int const ACCESS = (access_mode != 1 ? 4 : 0) | (access_mode != 0 || truncate ? 2 : 0);
    return authorize_file_access(caller, object, ACCESS);
}

// An open description grants only its admitted access mode. Later credentials
// or transport permission cannot upgrade a read-only/write-only description.
[[nodiscard]] constexpr auto authorize_file_description_access(unsigned access_mode, bool write) -> int {
    if (access_mode > 2 || (write ? access_mode == 0 : access_mode == 1)) return -EBADF;
    return 0;
}

// Every file mapping needs a read grant, including PROT_NONE (which may later
// gain read access). Private writes change private pages, not the open object.
[[nodiscard]] constexpr auto authorize_file_mapping(unsigned access_mode, bool shared, bool writable, bool read_only_view) -> int {
    if (authorize_file_description_access(access_mode, false) != 0) return -EACCES;
    if (shared && writable && (read_only_view || authorize_file_description_access(access_mode, true) != 0)) return -EACCES;
    return 0;
}

[[nodiscard]] constexpr auto prepare_file_create(const Snapshot& caller, const FileIdentity& parent, uint32_t mode, FileIdentity& out)
    -> int {
    if ((parent.mode & 0170000U) != 0040000U) return -ENOTDIR;
    if (int const RET = authorize_file_access(caller, parent, 3); RET != 0) return RET;
    FileIdentity next{caller.user().effective, (parent.mode & 02000U) != 0 ? parent.gid : caller.group().effective,
                      0100000U | (mode & 07777U)};
    if (!caller.allows(Authority::FILE_OWNER) && !caller.has_group(next.gid)) next.mode &= ~02000U;
    out = next;
    return 0;
}

[[nodiscard]] constexpr auto prepare_directory_create(const Snapshot& caller, const FileIdentity& parent, uint32_t mode, FileIdentity& out)
    -> int {
    FileIdentity next{};
    if (int const RET = prepare_file_create(caller, parent, mode, next); RET != 0) return RET;
    next.mode = 0040000U | (next.mode & 07777U);
    // Inherited setgid keeps the parent's group policy for later descendants,
    // including when the creator is not a member of that inherited group.
    if ((parent.mode & 02000U) != 0) next.mode |= 02000U;
    out = next;
    return 0;
}

// Links store literal target text and always retain their fixed 0777 mode.
[[nodiscard]] constexpr auto prepare_symlink_create(const Snapshot& caller, const FileIdentity& parent, FileIdentity& out) -> int {
    FileIdentity next{};
    if (int const RET = prepare_file_create(caller, parent, 0777, next); RET != 0) return RET;
    next.mode = 0120777;
    out = next;
    return 0;
}

[[nodiscard]] constexpr auto authorize_file_xattr(const Snapshot& caller, const FileIdentity& object, FileXattrNamespace name_space,
                                                  bool write) -> int {
    if (name_space == FileXattrNamespace::TRUSTED || name_space == FileXattrNamespace::SECURITY)
        return caller.allows(Authority::FILE_OWNER) ? 0 : -EPERM;
    if (name_space != FileXattrNamespace::USER) return -EINVAL;
    if (write && (object.mode & 0170000U) == 0120000U) return -EPERM;
    return authorize_file_access(caller, object, write ? 2 : 4);
}

enum class FileTimestampIntent : uint8_t { PRESERVE, SET_TO_NOW, SET_EXPLICIT };

[[nodiscard]] constexpr auto authorize_file_removal(const Snapshot& caller, const FileIdentity& parent, const FileIdentity& target) -> int {
    if ((parent.mode & 0170000U) != 0040000U) return -ENOTDIR;
    if (int const RET = authorize_file_access(caller, parent, 3); RET != 0) return RET;
    if ((parent.mode & 01000U) != 0 && caller.user().effective != parent.uid && caller.user().effective != target.uid &&
        !caller.allows(Authority::FILE_OWNER))
        return -EPERM;
    return 0;
}

[[nodiscard]] constexpr auto authorize_file_rename(const Snapshot& caller, const FileIdentity& source_parent, const FileIdentity& source,
                                                   const FileIdentity& destination_parent, const FileIdentity* displaced,
                                                   bool changes_parent) -> int {
    if (int const RET = authorize_file_removal(caller, source_parent, source); RET != 0) return RET;
    if (displaced != nullptr) {
        if (int const RET = authorize_file_removal(caller, destination_parent, *displaced); RET != 0) return RET;
    } else {
        if ((destination_parent.mode & 0170000U) != 0040000U) return -ENOTDIR;
        if (int const RET = authorize_file_access(caller, destination_parent, 3); RET != 0) return RET;
    }
    if (changes_parent && (source.mode & 0170000U) == 0040000U) return authorize_file_access(caller, source, 2);
    return 0;
}

[[nodiscard]] constexpr auto authorize_file_link(const Snapshot& caller, const FileIdentity& source_parent,
                                                 const FileIdentity& destination_parent, const FileIdentity& source) -> int {
    if ((source_parent.mode & 0170000U) != 0040000U || (destination_parent.mode & 0170000U) != 0040000U) return -ENOTDIR;
    if (int const RET = authorize_file_access(caller, source_parent, 1); RET != 0) return RET;
    if (int const RET = authorize_file_access(caller, destination_parent, 3); RET != 0) return RET;
    if ((source.mode & 0170000U) == 0040000U) return -EPERM;
    if (caller.user().effective == source.uid || caller.allows(Authority::FILE_OWNER)) return 0;
    // A foreign inode must not let the caller pin privilege-bearing or
    // inaccessible content under a name they control.
    // WOS exec honors either set-ID bit even without group execute.
    bool const SAFE_REGULAR = (source.mode & 0170000U) == 0100000U && (source.mode & 06000U) == 0;
    return SAFE_REGULAR && authorize_file_access(caller, source, 6) == 0 ? 0 : -EPERM;
}

[[nodiscard]] constexpr auto authorize_file_timestamps(const Snapshot& caller, const FileIdentity& object, FileTimestampIntent intent)
    -> int {
    if (intent == FileTimestampIntent::PRESERVE) return 0;
    if (intent != FileTimestampIntent::SET_TO_NOW && intent != FileTimestampIntent::SET_EXPLICIT) return -EINVAL;
    if (caller.user().effective == object.uid || caller.allows(Authority::FILE_OWNER)) return 0;
    if (intent == FileTimestampIntent::SET_EXPLICIT) return -EPERM;
    if (caller.allows(Authority::DAC_OVERRIDE)) return 0;
    unsigned const SHIFT = caller.has_group(object.gid) ? 3U : 0U;
    return ((object.mode >> SHIFT) & 2U) != 0 ? 0 : -EACCES;
}

// Prepare only. The backend retains its object lock across prepare and commit.
// Rejections return the original object so callers cannot publish partial IDs.
[[nodiscard]] constexpr auto prepare_file_chmod(const Snapshot& caller, const FileIdentity& object, uint32_t mode, FileIdentity& out)
    -> int {
    out = object;
    bool const PRIVILEGED = caller.allows(Authority::FILE_OWNER);
    if (!PRIVILEGED && caller.user().effective != object.uid) {
        return -EPERM;
    }
    out.mode = (object.mode & ~07777U) | (mode & 07777U);
    if (!PRIVILEGED && !caller.has_group(object.gid)) {
        out.mode &= ~02000U;
    }
    return 0;
}

[[nodiscard]] constexpr auto prepare_file_chown(const Snapshot& caller, const FileIdentity& object, uint32_t uid, uint32_t gid,
                                                FileIdentity& out) -> int {
    out = object;
    if (!caller.allows(Authority::FILE_OWNER) && (caller.user().effective != object.uid || (uid != UINT32_MAX && uid != object.uid) ||
                                                  (gid != UINT32_MAX && gid != object.gid && !caller.has_group(gid)))) {
        return -EPERM;
    }
    if (uid != UINT32_MAX) {
        out.uid = uid;
    }
    if (gid != UINT32_MAX) {
        out.gid = gid;
    }
    constexpr uint32_t TYPE_MASK = 0170000;
    constexpr uint32_t REGULAR = 0100000;
    if ((object.mode & TYPE_MASK) == REGULAR && (object.mode & 0111U) != 0) {
        out.mode &= ~06000U;
    }
    return 0;
}

// Filesystems with a fixed POSIX projection must still authorize requests, and
// may report success only when the requested result is already represented.
[[nodiscard]] constexpr auto authorize_fixed_file_chmod(const Snapshot& caller, const FileIdentity& object, uint32_t mode) -> int {
    FileIdentity prepared{};
    int const result = prepare_file_chmod(caller, object, mode, prepared);
    return result != 0 ? result : (prepared == object ? 0 : -EOPNOTSUPP);
}

[[nodiscard]] constexpr auto authorize_fixed_file_chown(const Snapshot& caller, const FileIdentity& object, uint32_t uid, uint32_t gid)
    -> int {
    FileIdentity prepared{};
    int const result = prepare_file_chown(caller, object, uid, gid, prepared);
    return result != 0 ? result : (prepared == object ? 0 : -EOPNOTSUPP);
}

}  // namespace ker::mod::security
