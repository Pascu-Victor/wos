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

    require(tags, "struct Spawn {}", "spawn sysdep tag")
    require(signatures, "SYSDEP_FUNC(Spawn, const char *path, char *const argv[], char *const envp[], pid_t *child)", "spawn sysdep signature")
    require(wos_tags, "Execve,\n    Spawn,\n    Yield", "WOS spawn sysdep registration")
    require(wos_impl, "Sysdeps<Spawn>::operator()", "WOS spawn sysdep implementation")
    require(wos_impl, "ker::process::exec(", "WOS spawn sysdep fast spawn syscall")
    require(wos_impl, "return EAGAIN;", "WOS spawn sysdep failure fallback signal")


def test_posix_spawn_fast_path_is_success_only() -> None:
    source = (ROOT / "toolchain/src/mlibc/options/posix/generic/spawn.cpp").read_text()

    require(source, "#include <mlibc/all-sysdeps.hpp>", "posix_spawn sysdep access")
    require(source, "if constexpr (mlibc::IsImplemented<Spawn>)", "posix_spawn optional spawn sysdep")
    require(source, "if (!file_actions && args.attr->__flags == 0 && !args.attr->__fn)", "posix_spawn simple-case guard")
    require(source, "ec = mlibc::sysdep<Spawn>(path, argv, envp, &spawned_pid);", "posix_spawn fast path call")
    require(source, "if (!ec) {\n\t\t\t\tif (res)\n\t\t\t\t\t*res = spawned_pid;", "posix_spawn fast path success return")
    require(source, "ec = 0;\n\t\t}\n\t}\n\tpthread_sigmask(SIG_BLOCK", "posix_spawn fast path fallback reset")
    require(source, "if (strchr(file, '/'))\n\t\treturn posix_spawn(pid, file, file_actions, attrp, argv, envp);", "posix_spawnp slash fast path")


if __name__ == "__main__":
    test_wos_exposes_spawn_sysdep()
    test_posix_spawn_fast_path_is_success_only()
