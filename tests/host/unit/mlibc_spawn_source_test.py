#!/usr/bin/env python3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def fail(message: str) -> None:
    raise AssertionError(message)


def require(source: str, token: str, label: str) -> None:
    if token not in source:
        fail(f"{label} missing token: {token}")


def test_wos_exposes_spawn_sysdep() -> None:
    tags = (ROOT / "toolchain/src/mlibc/options/internal/include/mlibc/sysdep-tags.hpp").read_text()
    signatures = (ROOT / "toolchain/src/mlibc/options/internal/include/mlibc/sysdep-signatures.hpp").read_text()
    wos_tags = (ROOT / "toolchain/src/mlibc/sysdeps/wos/include/mlibc/sysdeps.hpp").read_text()
    wos_impl = (ROOT / "toolchain/src/mlibc/sysdeps/wos/generic/sysdeps.cpp").read_text()
    wos_abi = (ROOT / "toolchain/src/mlibc/sysdeps/wos/include/callnums/process.h").read_text()

    require(wos_abi, "SPAWN_OPTIONS_VERSION", "WOS spawn ABI version")
    require(wos_abi, "uint64_t size;", "WOS spawn ABI size field")
    require(tags, "struct SysdepSpawnFdAction", "spawn fd action sysdep shape")
    require(tags, "struct SysdepSpawnOptions", "spawn options sysdep shape")
    require(tags, "struct Spawn {}", "spawn sysdep tag")
    require(signatures, "const SysdepSpawnOptions *options", "spawn sysdep signature options")
    require(wos_tags, "Execve,\n    Spawn,\n    Yield", "WOS spawn sysdep registration")
    require(wos_impl, "Sysdeps<Spawn>::operator()", "WOS spawn sysdep implementation")
    require(wos_impl, "ker::process::exec(", "WOS spawn sysdep fast spawn syscall")
    require(wos_impl, "ker::process::spawn(", "WOS option-aware spawn syscall")
    require(wos_impl, "kernel_options.version = ker::abi::process::SPAWN_OPTIONS_VERSION;", "WOS spawn ABI version fill")
    require(wos_impl, "SpawnFdActionType::DUP2", "WOS spawn fd-action conversion")
    require(wos_impl, "options->action_count != 0 && !options->actions", "WOS spawn action pointer validation")
    require(wos_impl, "if (r < 0)\n\t\treturn static_cast<int>(-r);", "WOS spawn negative errno translation")
    if "generic fork+exec fallback until the WOS fast-spawn fd action path is audited" in wos_impl:
        fail("WOS spawn must not reject bounded Ninja file actions")
    require(wos_impl, "return EAGAIN;", "WOS spawn sysdep failure fallback signal")


def test_posix_spawn_preserves_authoritative_errors() -> None:
    source = (ROOT / "toolchain/src/mlibc/options/posix/generic/spawn.cpp").read_text()

    require(source, "#include <mlibc/all-sysdeps.hpp>", "posix_spawn sysdep access")
    require(source, "constexpr size_t kFastSpawnActionMax = 32;", "posix_spawn bounded action fast path")
    require(source, "POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETPGROUP | POSIX_SPAWN_USEVFORK", "posix_spawn supported attr subset")
    require(source, "fast_spawn_collect_actions", "posix_spawn fd action collection")
    require(source, "for (; op; op = op->prev)", "posix_spawn preserves original fd-action order")
    collector = source[source.index("static bool fast_spawn_collect_actions("):source.index("static bool fast_spawn_supported_attrs(")]
    assert 'strcmp(op->path, "/dev/null")' not in collector
    assert 'op->oflag & O_CLOEXEC' not in collector
    require(collector, ".oflag = op->oflag", "OPEN passes complete flags")
    require(collector, "case FDOP_OPEN:", "OPEN uses bounded syscall actions")
    require(source, "mlibc::sysdep<Spawn>(path, argv, envp, need_options ? &options : nullptr, &spawned_pid)", "posix_spawn fast path call")
    require(source, "if (try_fast_spawn(res, path, file_actions, args.attr, argv, envp, ec))", "posix_spawn fast path attempt")
    require(source, "pthread_sigmask(SIG_BLOCK", "posix_spawn fallback remains available")
    require(source, "if (!error && res)", "failed fast spawn preserves output PID")
    require(source, "ret = -errno;", "fallback preserves setpgid errno")
    assert "ignoring SETPGROUP" not in source
    assert source.index("if (setpgid(0, attr->__pgrp) < 0)") < source.index("if (fa && fa->__actions)")
    fast = source[source.index("static bool try_fast_spawn("):source.index("\nint posix_spawn(")]
    submitted = fast[fast.index("mlibc::sysdep<Spawn>"):]
    assert "return false" not in submitted, "submitted syscall errors cannot select fallback"
    assert "return ec;" in source[source.index("if (try_fast_spawn("):source.index("pthread_sigmask(SIG_BLOCK")]

    require(source, "if (strchr(file, '/'))\n\t\treturn posix_spawn(pid, file, file_actions, attrp, argv, envp);", "posix_spawnp slash fast path")


def test_fallback_action_errors_and_flags() -> None:
    source = (ROOT / "toolchain/src/mlibc/options/posix/generic/spawn.cpp").read_text()
    child = source[source.index("static int child("):source.index("static bool fast_spawn_collect_actions(")]
    assert "fail_errno:" in child and "ret = -errno;" in child.split("fail_errno:", 1)[1]
    opening = child.split("case FDOP_OPEN:", 1)[1].split("case FDOP_CHDIR:", 1)[0]
    assert opening.index("getrlimit(RLIMIT_NOFILE") < opening.index("close(op->fd)") < opening.index("fd = open(")
    assert "goto fail_errno;" in opening
    assert "op->oflag & O_CLOEXEC" in opening and "fcntl(op->fd, F_SETFD, FD_CLOEXEC)" in opening
    duplicate = child.split("case FDOP_DUP2:", 1)[1].split("case FDOP_OPEN:", 1)[0]
    assert "ret = -EBADF;" in duplicate
    same = duplicate.split("ret = fcntl(fd, F_GETFD);", 1)[1]
    assert same.index("if (ret < 0)") < same.index("F_SETFD")
    for action in ("FDOP_CHDIR", "FDOP_FCHDIR"):
        assert "goto fail_errno;" in child.split("case " + action + ":", 1)[1].split("break;", 1)[0]


if __name__ == "__main__":
    test_wos_exposes_spawn_sysdep()
    test_posix_spawn_preserves_authoritative_errors()
    test_fallback_action_errors_and_flags()
