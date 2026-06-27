#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WOS_VFS_H = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "sys" / "vfs.h"
WOS_SYSDEPS_CPP = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "generic" / "sysdeps.cpp"
KERNEL_SYS_VFS_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "vfs" / "sys_vfs.cpp"
KERNEL_VFS_CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"
KERNEL_FILE_OPS_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "file_operations.hpp"
MLIBC_DIRENT_H = ROOT / "toolchain" / "src" / "mlibc" / "options" / "posix" / "include" / "dirent.h"
GIT_BUILD_SCRIPT = ROOT / "scripts" / "build" / "build_git_for_wos.sh"


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
    cursor = -1
    for token in tokens:
        index = source.find(token, cursor + 1)
        if index < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = index


def parse_kernel_dt_constants(source: str) -> dict[str, int]:
    matches = re.findall(r"constexpr\s+uint8_t\s+(DT_[A-Z0-9_]+)\s*=\s*(0x[0-9A-Fa-f]+|\d+);", source)
    return {name: int(value, 0) for name, value in matches}


def parse_mlibc_dt_constants(source: str) -> dict[str, int]:
    matches = re.findall(r"^#define\s+(DT_[A-Z0-9_]+)\s+(0x[0-9A-Fa-f]+|\d+)$", source, flags=re.MULTILINE)
    return {name: int(value, 0) for name, value in matches}


def test_kernel_dirent_types_match_mlibc_public_abi() -> None:
    kernel_constants = parse_kernel_dt_constants(KERNEL_FILE_OPS_HPP.read_text())
    mlibc_constants = parse_mlibc_dt_constants(MLIBC_DIRENT_H.read_text())
    expected = {
        "DT_UNKNOWN": 0,
        "DT_FIFO": 1,
        "DT_CHR": 2,
        "DT_DIR": 4,
        "DT_BLK": 6,
        "DT_REG": 8,
        "DT_LNK": 10,
        "DT_SOCK": 12,
        "DT_WOSLINK": 0x80,
    }
    for name, value in expected.items():
        if kernel_constants.get(name) != value:
            fail(f"kernel {name} must be {value}, got {kernel_constants.get(name)}")
        if mlibc_constants.get(name) != value:
            fail(f"mlibc {name} must be {value}, got {mlibc_constants.get(name)}")


def test_git_helper_pipe_cloexec_patch_is_preserved_by_mlibc() -> None:
    git_build = GIT_BUILD_SCRIPT.read_text()
    require_tokens(
        git_build,
        [
            '"pipe(fdin)": ("pipe2(fdin, O_CLOEXEC)", 2)',
            '"pipe(fdout)": ("pipe2(fdout, O_CLOEXEC)", 2)',
            '"pipe(fderr)": ("pipe2(fderr, O_CLOEXEC)", 1)',
            '"pipe(notify_pipe)": ("pipe2(notify_pipe, O_CLOEXEC)", 1)',
        ],
        "Git run-command pipe patch",
    )

    sysdeps = WOS_SYSDEPS_CPP.read_text()
    pipe_body = function_body(sysdeps, r"int\s+Sysdeps<Pipe>::operator\(\)\(int\s+\*fds,\s*int\s+flags\)")
    require_tokens(pipe_body, ["ker::abi::vfs::pipe(fds, flags)"], "WOS mlibc pipe sysdep")
    if "(void)flags" in pipe_body:
        fail("WOS mlibc pipe sysdep must not discard pipe2 flags")


def test_mlibc_vfs_wrappers_pass_fd_creation_flags_to_kernel() -> None:
    header = WOS_VFS_H.read_text()

    dup2_body = function_body(header, r"static\s+inline\s+int\s+dup2\(int\s+oldfd,\s*int\s+newfd,\s*int\s+flags\s*=\s*0\)")
    require_tokens(
        dup2_body,
        [
            "static_cast<uint64_t>(ops::dup2)",
            "static_cast<uint64_t>(oldfd)",
            "static_cast<uint64_t>(newfd)",
            "static_cast<uint64_t>(flags)",
        ],
        "WOS mlibc dup2 ABI wrapper",
    )

    pipe_body = function_body(header, r"static\s+inline\s+int\s+pipe\(int\s+pipefd\[2\],\s*int\s+flags\s*=\s*0\)")
    require_tokens(
        pipe_body,
        [
            "static_cast<uint64_t>(ops::pipe)",
            "reinterpret_cast<uint64_t>(pipefd)",
            "static_cast<uint64_t>(flags)",
        ],
        "WOS mlibc pipe ABI wrapper",
    )

    sysdeps = WOS_SYSDEPS_CPP.read_text()
    dup2_sysdep = function_body(sysdeps, r"int\s+Sysdeps<Dup2>::operator\(\)\(int\s+fd,\s*int\s+flags,\s*int\s+newfd\)")
    require_tokens(dup2_sysdep, ["ker::abi::vfs::dup2(fd, newfd, flags)"], "WOS mlibc dup2 sysdep")
    if "(void)flags" in dup2_sysdep:
        fail("WOS mlibc dup2 sysdep must not discard dup3 flags")

    socketpair_sysdep = function_body(
        sysdeps,
        r"int\s+Sysdeps<Socketpair>::operator\(\)\(int\s+domain,\s*int\s+type_and_flags,\s*int\s+proto,\s*int\s+\*fds\)",
    )
    require_tokens(
        socketpair_sysdep,
        ["type_and_flags & (SOCK_CLOEXEC | SOCK_NONBLOCK)", "ker::abi::vfs::pipe(fds,"],
        "WOS mlibc socketpair sysdep",
    )


def test_pselect_empty_fd_sets_do_not_call_epoll_with_zero_maxevents() -> None:
    sysdeps = WOS_SYSDEPS_CPP.read_text()
    pselect_body = function_body(
        sysdeps,
        r"int\s+Sysdeps<Pselect>::operator\(\)\(\s*int\s+num_fds,\s*fd_set\s+\*read_set,\s*fd_set\s+\*write_set,\s*fd_set\s+\*except_set,\s*const\s+struct\s+timespec\s+\*timeout,\s*const\s+sigset_t\s+\*sigmask,\s*int\s+\*num_events\s*\)",
    )
    require_tokens(
        pselect_body,
        [
            "if (num_fds < 0)",
            "int watched_fds = 0;",
            "watched_fds++;",
            "if (watched_fds == 0)",
            "pselect_sleep_for_timeout_ms(timeout_ms, sigmask)",
            "*num_events = 0;",
            "int max = watched_fds < 64 ? watched_fds : 64;",
            "ker::abi::vfs::epoll_pwait_vfs(epfd, out_events, max, timeout_ms)",
        ],
        "WOS mlibc pselect empty-fd handling",
    )
    if "int max = num_fds < 64 ? num_fds : 64;" in pselect_body:
        fail("WOS mlibc pselect must not derive epoll maxevents from num_fds")

    wait_body = function_body(sysdeps, r"int\s+pselect_sleep_for_timeout_ms\(int\s+timeout_ms,\s*const\s+sigset_t\s+\*sigmask\)")
    require_tokens(
        wait_body,
        [
            "if (timeout_ms == 0)",
            "if (timeout_ms < 0)",
            "ker::process::sigsuspend(wait_mask)",
            "ker::abi::sys_time_ops::nanosleep",
            "ker::process::sigprocmask(SIG_SETMASK, sigmask, &old_mask)",
            "ker::process::sigprocmask(SIG_SETMASK, &old_mask, nullptr)",
        ],
        "WOS mlibc pselect empty wait helper",
    )


def test_pselect_and_epoll_pwait_honor_temporary_signal_masks() -> None:
    sysdeps = WOS_SYSDEPS_CPP.read_text()
    apply_body = function_body(
        sysdeps,
        r"int\s+apply_wait_signal_mask\(const\s+sigset_t\s+\*sigmask,\s*sigset_t\s+\*old_mask,\s*bool\s+\*restore_mask\)",
    )
    require_tokens(
        apply_body,
        [
            "if (!sigmask)",
            "*restore_mask = false",
            "ker::process::sigprocmask(SIG_SETMASK, sigmask, old_mask)",
            "*restore_mask = true",
        ],
        "WOS mlibc wait signal-mask apply helper",
    )
    restore_body = function_body(sysdeps, r"int\s+restore_wait_signal_mask\(const\s+sigset_t\s+\*old_mask,\s*bool\s+restore_mask\)")
    require_tokens(
        restore_body,
        [
            "if (!restore_mask)",
            "ker::process::sigprocmask(SIG_SETMASK, old_mask, nullptr)",
        ],
        "WOS mlibc wait signal-mask restore helper",
    )

    pselect_body = function_body(
        sysdeps,
        r"int\s+Sysdeps<Pselect>::operator\(\)\(\s*int\s+num_fds,\s*fd_set\s+\*read_set,\s*fd_set\s+\*write_set,\s*fd_set\s+\*except_set,\s*const\s+struct\s+timespec\s+\*timeout,\s*const\s+sigset_t\s+\*sigmask,\s*int\s+\*num_events\s*\)",
    )
    require_order(
        pselect_body,
        [
            "int max = watched_fds < 64 ? watched_fds : 64;",
            "apply_wait_signal_mask(sigmask, &old_mask, &restore_mask)",
            "ker::abi::vfs::epoll_pwait_vfs(epfd, out_events, max, timeout_ms)",
            "restore_wait_signal_mask(&old_mask, restore_mask)",
        ],
        "WOS mlibc pselect fd wait signal-mask handling",
    )

    epoll_body = function_body(
        sysdeps,
        r"int\s+Sysdeps<EpollPwait>::operator\(\)\(\s*int\s+epfd,\s*epoll_event\s+\*ev,\s*int\s+n,\s*int\s+timeout,\s*const\s+sigset_t\s+\*sigmask,\s*int\s+\*raised\s*\)",
    )
    require_order(
        epoll_body,
        [
            "apply_wait_signal_mask(sigmask, &old_mask, &restore_mask)",
            "ker::abi::vfs::epoll_pwait_vfs(epfd, ev, n, timeout)",
            "restore_wait_signal_mask(&old_mask, restore_mask)",
        ],
        "WOS mlibc epoll_pwait signal-mask handling",
    )
    if "(void)sigmask" in epoll_body:
        fail("WOS mlibc epoll_pwait must not discard the temporary signal mask")


def test_kernel_vfs_syscalls_accept_the_flags_mlibc_forwards() -> None:
    syscall_source = KERNEL_SYS_VFS_CPP.read_text()
    require_tokens(
        syscall_source,
        [
            "case ops::DUP2:",
            "int const FLAGS = static_cast<int>(a3);",
            "ker::vfs::vfs_dup2(OLDFD, NEWFD, FLAGS)",
            "case ops::PIPE:",
            "int const FLAGS = static_cast<int>(a2);",
            "ker::vfs::vfs_pipe(kernel_pipefd.data(), FLAGS)",
        ],
        "kernel vfs syscall flag plumbing",
    )

    vfs_core = KERNEL_VFS_CORE_CPP.read_text()
    require_tokens(
        vfs_core,
        [
            "constexpr int PIPE_SUPPORTED_FLAGS = ker::vfs::O_CLOEXEC | O_NONBLOCK;",
            "if ((flags & ~PIPE_SUPPORTED_FLAGS) != 0)",
            "int const READ_OPEN_FLAGS = (flags & O_NONBLOCK) != 0 ? O_NONBLOCK : 0;",
            "int const WRITE_OPEN_FLAGS = 1 | ((flags & O_NONBLOCK) != 0 ? O_NONBLOCK : 0);",
            "if ((flags & ker::vfs::O_CLOEXEC) != 0)",
            "task->set_fd_cloexec(static_cast<unsigned>(RFD));",
            "task->set_fd_cloexec(static_cast<unsigned>(WFD));",
        ],
        "kernel vfs pipe flag handling",
    )

    dup2_body = function_body(vfs_core, r"auto\s+vfs_dup2\(int\s+oldfd,\s*int\s+newfd,\s*int\s+flags\)\s*->\s*int")
    require_tokens(
        dup2_body,
        [
            "if ((flags & ~ker::vfs::O_CLOEXEC) != 0)",
            "vfs_replace_fd_for_dup2_locked(task, newfd, f, (flags & ker::vfs::O_CLOEXEC) != 0)",
        ],
        "kernel vfs dup2 flag handling",
    )


def test_kernel_dirfd_resolution_returns_task_visible_paths() -> None:
    vfs_core = KERNEL_VFS_CORE_CPP.read_text()
    body = function_body(
        vfs_core,
        r"auto\s+vfs_resolve_dirfd\(ker::mod::sched::task::Task\*\s+task,\s*int\s+dirfd,\s*const\s+char\*\s+pathname,\s*char\*\s+resolved,\s*size_t\s+resolved_size\)\s*->\s*int",
    )
    require_tokens(
        body,
        [
            "if (pathname[0] == '/')",
            "if (dirfd == AT_FDCWD)",
            "base = task->cwd.data();",
            "auto* file = vfs_get_file_retain(task, dirfd);",
            "strip_task_root_prefix(task, file->vfs_path, resolved, resolved_size, nullptr)",
            "base = resolved;",
        ],
        "kernel dirfd path resolution",
    )
    if "std::memcpy(resolved, file->vfs_path" in body:
        fail("dirfd resolution must not feed a backing vfs_path back into public VFS APIs")


def test_metadata_cache_store_uses_pre_backend_stat_generation() -> None:
    vfs_core = KERNEL_VFS_CORE_CPP.read_text()
    store_body = function_body(
        vfs_core,
        r"void\s+metadata_cache_store\(const\s+char\*\s+path,\s*FSType\s+fs_type,\s*uint64_t\s+dev_id,\s*bool\s+follow_final_symlink,\s*bool\s+require_directory,\s*int\s+result,\s*const\s+Stat\*\s+statbuf,\s*MetadataSnapshotStamp\s+stamp\)",
    )
    require_tokens(
        store_body,
        [
            "if (stamp.cache_generation != EPOCH || metadata_path_invalidated_since(path, PATH_LEN, stamp.invalidation_generation))",
            "victim->invalidation_generation = stamp.invalidation_generation;",
        ],
        "metadata cache stale stat race guard",
    )

    stat_body = function_body(
        vfs_core,
        r"static\s+auto\s+vfs_stat_impl\(const\s+char\*\s+path,\s*ker::vfs::Stat\*\s+statbuf,\s*bool\s+resolve_task_path,\s*bool\s+apply_task_policy,\s*bool\s+follow_final_symlink\)\s*->\s*int",
    )
    require_tokens(
        stat_body,
        [
            "MetadataSnapshotStamp const STAT_STAMP = metadata_snapshot_stamp();",
            "metadata_cache_store(pathBuffer, mount->fs_type, mount->dev_id, EFFECTIVE_FOLLOW_FINAL_SYMLINK, REQUIRE_DIRECTORY, result, statbuf,",
            "STAT_STAMP);",
        ],
        "vfs_stat metadata cache observation stamp",
    )

    if "KTEST(VFS, MetadataCacheRejectsStaleNegativeStore)" not in (ROOT / "modules" / "kern" / "src" / "test" / "vfs_ktest.cpp").read_text():
        fail("vfs_ktest must cover stale negative metadata stores")


if __name__ == "__main__":
    test_kernel_dirent_types_match_mlibc_public_abi()
    test_git_helper_pipe_cloexec_patch_is_preserved_by_mlibc()
    test_mlibc_vfs_wrappers_pass_fd_creation_flags_to_kernel()
    test_pselect_empty_fd_sets_do_not_call_epoll_with_zero_maxevents()
    test_pselect_and_epoll_pwait_honor_temporary_signal_masks()
    test_kernel_vfs_syscalls_accept_the_flags_mlibc_forwards()
    test_kernel_dirfd_resolution_returns_task_visible_paths()
    test_metadata_cache_store_uses_pre_backend_stat_generation()
    print("WOS mlibc VFS source invariants hold")
