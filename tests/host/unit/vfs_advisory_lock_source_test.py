#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"
WOS_SYSDEPS_CPP = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "generic" / "sysdeps.cpp"
WOS_SYSDEPS_HPP = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "mlibc" / "sysdeps.hpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name_pattern: str) -> str:
    match = re.search(rf"{name_pattern}\s*\{{", source, flags=re.DOTALL)
    if match is None:
        fail(f"missing function matching {name_pattern}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function matching {name_pattern}")
    return source[match.end() : pos - 1]


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token!r}")
        cursor = found + len(token)


def test_kernel_advisory_lock_table_is_real_state() -> None:
    source = CORE_CPP.read_text()
    require_tokens(
        source,
        [
            "struct VfsFlockAbi",
            "static_assert(sizeof(VfsFlockAbi) == 32)",
            "constexpr int WOS_FLOCK_CMD = 0x5753464c",
            "enum class AdvisoryLockFamily",
            "enum class AdvisoryOwnerKind",
            "enum class AdvisoryLockType",
            "struct AdvisoryFileKey",
            "struct AdvisoryLock",
            "std::deque<AdvisoryLock> g_advisory_locks",
            "ker::mod::sys::Mutex g_advisory_lock_mutex",
            "advisory_locks_conflict",
            "advisory_unlock_owned_range_locked",
            "advisory_insert_owned_lock_locked",
            "advisory_find_conflict_locked",
            "advisory_flock",
        ],
        "kernel advisory lock state",
    )


def test_kernel_fcntl_implements_posix_and_ofd_locks() -> None:
    source = CORE_CPP.read_text()
    fcntl_body = function_body(source, r"auto\s+vfs_fcntl\(int\s+fd,\s*int\s+cmd,\s*uint64_t\s+arg\)\s*->\s*int")
    require_tokens(
        fcntl_body,
        [
            "case F_GETLK_CMD:",
            "case F_OFD_GETLK_CMD:",
            "case F_SETLK_CMD:",
            "case F_SETLKW_CMD:",
            "case F_OFD_SETLK_CMD:",
            "case F_OFD_SETLKW_CMD:",
            "advisory_copy_from_user(arg, flock)",
            "advisory_get_lock(f, ker::mod::sched::task::process_pid(*task), OWNER_KIND, AdvisoryLockFamily::RECORD, flock)",
            "advisory_copy_to_user(arg, flock)",
            "advisory_set_lock(f, ker::mod::sched::task::process_pid(*task), OWNER_KIND, AdvisoryLockFamily::RECORD, flock, WAIT)",
            "case WOS_FLOCK_CMD:",
            "advisory_flock(f, static_cast<int>(arg))",
        ],
        "vfs_fcntl advisory commands",
    )
    require_order(
        fcntl_body,
        [
            "case F_SETLK_CMD:",
            "case F_SETLKW_CMD:",
            "case F_OFD_SETLK_CMD:",
            "case F_OFD_SETLKW_CMD:",
        ],
        "vfs_fcntl lock command grouping",
    )

    set_body = function_body(
        source,
        r"auto\s+advisory_set_lock\(File\s*\*\s*file,\s*uint64_t\s+owner_pid,\s*AdvisoryOwnerKind\s+owner_kind,\s*AdvisoryLockFamily\s+family,\s*const\s+VfsFlockAbi&\s+flock,\s*bool\s+wait\)\s*->\s*int",
    )
    require_tokens(
        set_body,
        [
            "flock.l_type == F_UNLCK_TYPE",
            "advisory_unlock_owned_range_locked(key, family",
            "advisory_find_conflict_locked(requested)",
            "advisory_insert_owned_lock_locked(requested)",
            "return -EAGAIN;",
            "return -EINTR;",
            "ker::mod::sched::kern_yield();",
        ],
        "advisory_set_lock conflict behavior",
    )

    require_tokens(
        source,
        [
            "if (held.family != requested.family)",
            "requested.family = family;",
            "existing.family != lock.family",
            "lock.family != family",
        ],
        "advisory lock family isolation",
    )


def test_advisory_locks_are_released_on_close_lifetimes() -> None:
    source = CORE_CPP.read_text()
    destroy_body = function_body(source, r"auto\s+vfs_destroy_file\(File\s*\*\s*f\)\s*->\s*int")
    close_body = function_body(source, r"auto\s+vfs_close\(int\s+fd\)\s*->\s*int")
    dup2_body = function_body(source, r"auto\s+vfs_dup2\(int\s+oldfd,\s*int\s+newfd,\s*int\s+flags\)\s*->\s*int")
    require_tokens(destroy_body, ["advisory_release_file_owner_locks(f);"], "file-owner advisory lock cleanup")
    require_tokens(
        close_body,
        ["advisory_release_process_locks_for_file(ker::mod::sched::task::process_pid(*t), f);"],
        "process advisory lock cleanup",
    )
    require_tokens(
        dup2_body,
        ["advisory_release_process_locks_for_file(ker::mod::sched::task::process_pid(*task), REPLACE.existing);"],
        "dup2 advisory lock cleanup",
    )


def test_remote_advisory_keys_do_not_force_wki_stat_on_close() -> None:
    source = CORE_CPP.read_text()
    build_key_body = function_body(
        source,
        r"auto\s+advisory_build_key\(File\s*\*\s*file,\s*AdvisoryFileKey&\s+key,\s*Stat\s*\*\s*stat_out(?:\s*=\s*nullptr)?,\s*bool\s+allow_backend_stat(?:\s*=\s*true)?\)\s*->\s*int",
    )
    release_body = function_body(source, r"void\s+advisory_release_process_locks_for_file\(uint64_t\s+pid,\s*File\s*\*\s*file\)")
    set_body = function_body(
        source,
        r"auto\s+advisory_set_lock\(File\s*\*\s*file,\s*uint64_t\s+owner_pid,\s*AdvisoryOwnerKind\s+owner_kind,\s*AdvisoryLockFamily\s+family,\s*const\s+VfsFlockAbi&\s+flock,\s*bool\s+wait\)\s*->\s*int",
    )
    get_body = function_body(
        source,
        r"auto\s+advisory_get_lock\(File\s*\*\s*file,\s*uint64_t\s+owner_pid,\s*AdvisoryOwnerKind\s+owner_kind,\s*AdvisoryLockFamily\s+family,\s*VfsFlockAbi&\s+flock\)\s*->\s*int",
    )

    require_tokens(
        build_key_body,
        [
            "if (!allow_backend_stat)",
            "file->fs_type != FSType::REMOTE && st.st_ino != 0",
        ],
        "remote advisory key construction",
    )
    require_tokens(
        release_body,
        [
            "advisory_process_has_locks_locked(pid)",
            "advisory_build_key(file, cheap_key, nullptr, false)",
            "file->fs_type != FSType::REMOTE && advisory_process_has_inode_locks_locked(pid)",
        ],
        "close-time advisory cleanup avoids remote stat",
    )
    for body, context in [(set_body, "set lock remote stat gate"), (get_body, "get lock remote stat gate")]:
        require_tokens(
            body,
            [
                "bool const NEEDS_STAT_FOR_RANGE = flock.l_whence == SEEK_END_VALUE",
                "bool const ALLOW_BACKEND_STAT = file->fs_type != FSType::REMOTE || NEEDS_STAT_FOR_RANGE",
                "advisory_build_key(file, key, NEEDS_STAT_FOR_RANGE ? &stat : nullptr, ALLOW_BACKEND_STAT)",
            ],
            context,
        )


def test_mlibc_flock_and_fcntl_forward_lock_requests() -> None:
    header = WOS_SYSDEPS_HPP.read_text()
    require_tokens(header, ["Flock,", "Fcntl,"], "WOS sysdep tags")
    require_order(header, ["Flock,", "Fcntl,"], "Flock should be enabled before Fcntl in WOS tags")

    sysdeps = WOS_SYSDEPS_CPP.read_text()
    flock_body = function_body(sysdeps, r"int\s+Sysdeps<Flock>::operator\(\)\(int\s+fd,\s*int\s+options\)")
    require_tokens(
        flock_body,
        [
            "constexpr int WOS_FLOCK_CMD = 0x5753464c",
            "ker::abi::vfs::fcntl(fd, WOS_FLOCK_CMD, static_cast<uint64_t>(options))",
        ],
        "mlibc flock sysdep",
    )

    fcntl_body = function_body(sysdeps, r"int\s+Sysdeps<Fcntl>::operator\(\)\(int\s+fd,\s*int\s+request,\s*va_list\s+args,\s*int\s+\*result\)")
    require_tokens(
        fcntl_body,
        [
            "request == F_GETLK",
            "request == F_SETLK",
            "request == F_SETLKW",
            "request == F_OFD_GETLK",
            "request == F_OFD_SETLK",
            "request == F_OFD_SETLKW",
            "reinterpret_cast<uint64_t>(va_arg(args, struct flock *))",
            "ker::abi::vfs::fcntl(fd, request, arg)",
        ],
        "mlibc fcntl lock forwarding",
    )


def main() -> None:
    test_kernel_advisory_lock_table_is_real_state()
    test_kernel_fcntl_implements_posix_and_ofd_locks()
    test_advisory_locks_are_released_on_close_lifetimes()
    test_remote_advisory_keys_do_not_force_wki_stat_on_close()
    test_mlibc_flock_and_fcntl_forward_lock_requests()
    print("VFS advisory lock source invariants hold")


if __name__ == "__main__":
    main()
