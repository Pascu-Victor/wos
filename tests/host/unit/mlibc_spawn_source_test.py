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
    require(wos_impl, "return EAGAIN;", "WOS spawn sysdep failure fallback signal")


def test_posix_spawn_fast_path_is_success_only() -> None:
    source = (ROOT / "toolchain/src/mlibc/options/posix/generic/spawn.cpp").read_text()

    require(source, "#include <mlibc/all-sysdeps.hpp>", "posix_spawn sysdep access")
    require(source, "constexpr size_t kFastSpawnActionMax = 32;", "posix_spawn bounded action fast path")
    require(source, "POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETPGROUP | POSIX_SPAWN_USEVFORK", "posix_spawn supported attr subset")
    require(source, "fast_spawn_collect_actions", "posix_spawn fd action collection")
    require(source, "for (; op; op = op->prev)", "posix_spawn preserves original fd-action order")
    require(source, 'strcmp(op->path, "/dev/null") != 0 || op->oflag != O_RDONLY', "posix_spawn addopen policy guard")
    require(source, "mlibc::sysdep<Spawn>(path, argv, envp, need_options ? &options : nullptr, &spawned_pid)", "posix_spawn fast path call")
    require(source, "if (try_fast_spawn(res, path, file_actions, args.attr, argv, envp))", "posix_spawn fast path attempt")
    require(source, "pthread_sigmask(SIG_BLOCK", "posix_spawn fallback remains available")
    require(source, "if (strchr(file, '/'))\n\t\treturn posix_spawn(pid, file, file_actions, attrp, argv, envp);", "posix_spawnp slash fast path")


if __name__ == "__main__":
    test_wos_exposes_spawn_sysdep()
    test_posix_spawn_fast_path_is_success_only()
