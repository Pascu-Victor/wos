#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WOS_VFS_H = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "sys" / "vfs.h"
WOS_SYSDEPS_CPP = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "generic" / "sysdeps.cpp"
KERNEL_SYS_VFS_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "vfs" / "sys_vfs.cpp"
KERNEL_VFS_CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"
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


if __name__ == "__main__":
    test_git_helper_pipe_cloexec_patch_is_preserved_by_mlibc()
    test_mlibc_vfs_wrappers_pass_fd_creation_flags_to_kernel()
    test_kernel_vfs_syscalls_accept_the_flags_mlibc_forwards()
    print("WOS mlibc VFS source invariants hold")
