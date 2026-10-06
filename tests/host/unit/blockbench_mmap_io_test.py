#!/usr/bin/env python3
"""Run the complete mmap workload on Linux with narrow syscall fault injection."""

import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]

ADAPTER = r'''
#include "blockbench_mmap.hpp"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

extern "C" ssize_t __real_pread(int, void*, size_t, off_t);
extern "C" ssize_t __real_pwrite(int, const void*, size_t, off_t);
extern "C" int __real_fsync(int);
static unsigned reads, writes, barriers;
static unsigned read_calls[2], write_calls[2];
static const char* source_path;
static const char* mode;
static bool selected(const char* name) { return mode && std::strcmp(mode, name) == 0; }

static void check_untouched_closed_mapping(const void* pointer) {
    int fd = open("/proc/self/pagemap", O_RDONLY);
    uint64_t entry = UINT64_MAX;
    off_t offset = static_cast<off_t>((reinterpret_cast<uintptr_t>(pointer) / 4096) * sizeof(entry));
    bool absent = fd >= 0 && __real_pread(fd, &entry, sizeof(entry), offset) == sizeof(entry) && !(entry >> 63);
    if (fd >= 0) close(fd);
    struct stat source{};
    bool closed = stat(source_path, &source) == 0;
    DIR* descriptors = opendir("/proc/self/fd");
    if (!descriptors) std::abort();
    while (auto* item = readdir(descriptors)) {
        if (item->d_name[0] == '.') continue;
        struct stat candidate{};
        if (fstat(std::atoi(item->d_name), &candidate) == 0 && candidate.st_dev == source.st_dev && candidate.st_ino == source.st_ino)
            closed = false;
    }
    closedir(descriptors);
    if (!absent || !closed) {
        std::fprintf(stderr, "host mapping boundary absent=%u closed=%u\n", unsigned(absent), unsigned(closed));
        std::abort();
    }
}

extern "C" ssize_t __wrap_pwrite(int fd, const void* data, size_t bytes, off_t offset) {
    ++writes;
    auto stage = barriers == 0 ? 0 : 1;
    if (++write_calls[stage] == 1 && stage == 0) check_untouched_closed_mapping(data);
    if (selected("write-error")) { errno = ENOSPC; return -1; }
    if (selected("write-zero")) return 0;
    if (selected("interrupted-short")) {
        if (write_calls[stage] == 1) { errno = EINTR; return -1; }
        bytes = std::min(bytes, size_t{1024});
    }
    return __real_pwrite(fd, data, bytes, offset);
}
extern "C" ssize_t __wrap_pread(int fd, void* data, size_t bytes, off_t offset) {
    ++reads;
    auto stage = barriers >= 2 ? 1 : 0;
    if (++read_calls[stage] == 1 && stage == 1) check_untouched_closed_mapping(data);
    if (stage == 0 && selected("scratch-read-error")) { errno = EIO; return -1; }
    if (stage == 1 && selected("read-error")) { errno = EIO; return -1; }
    if (stage == 1 && selected("read-zero")) return 0;
    if (stage == 1 && selected("read-skip-copy")) return static_cast<ssize_t>(bytes);
    if (selected("interrupted-short")) {
        if (read_calls[stage] == 1) { errno = EINTR; return -1; }
        bytes = std::min(bytes, size_t{1024});
    }
    auto result = __real_pread(fd, data, bytes, offset);
    if (stage == 1 && selected("read-corrupt") && result > 0) static_cast<uint8_t*>(data)[0] ^= 1;
    return result;
}
extern "C" int __wrap_fsync(int fd) {
    ++barriers;
    if (selected("flush-error") || (barriers == 2 && selected("second-flush-error"))) { errno = EIO; return -1; }
    return __real_fsync(fd);
}
int main(int argc, char** argv) {
    source_path = argc > 2 ? argv[2] : "";
    mode = std::getenv("MMAP_IO_FAULT");
    if (selected("collision")) {
        char path[4096];
        int length = std::snprintf(path, sizeof(path), "%s.goal30-mmap-%ld", source_path, static_cast<long>(getpid()));
        if (length < 0 || size_t(length) >= sizeof(path)) std::abort();
        int fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);
        if (fd < 0 || write(fd, "owned", 5) != 5 || close(fd) != 0) std::abort();
    }
    int result = run_blockbench_mmap(argc - 1, argv + 1);
    if (result == 0 && (reads < 2 || writes < 2 || barriers != 2)) std::abort();
    if (result == 0 && selected("interrupted-short") && (reads != 10 || writes != 10)) std::abort();
    return result;
}
'''


def main():
    scratch = ROOT / "build/goal30-tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="blockbench-mmap-io-", dir=scratch) as temporary:
        work = Path(temporary)
        adapter = work / "main.cpp"
        adapter.write_text(ADAPTER)
        binary = work / "blockbench-mmap"
        environment = dict(os.environ, TMPDIR=str(work))
        environment.setdefault("ASAN_OPTIONS", "detect_leaks=0:halt_on_error=1")
        subprocess.run([
            "/usr/bin/clang++", "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Werror",
            "-fsanitize=address,undefined", "-fno-omit-frame-pointer",
            "-I", str(ROOT / "modules/testprog/src/tests"), str(adapter),
            str(ROOT / "modules/testprog/src/tests/blockbench_mmap.cpp"),
            "-Wl,--wrap=pwrite,--wrap=pread,--wrap=fsync", "-o", str(binary),
        ], check=True, capture_output=True, text=True, env=environment)
        cases = 0

        def run(path, *arguments, status=0, fault=None, phase=None):
            nonlocal cases
            before = path.read_bytes() if path.is_file() else None
            case_environment = dict(environment)
            if fault:
                case_environment["MMAP_IO_FAULT"] = fault
            result = subprocess.run([str(binary), "blockbench-mmap", str(path), *map(str, arguments)],
                                    check=False, capture_output=True, text=True, timeout=30, env=case_environment)
            assert result.returncode == status, (arguments, fault, result.returncode, result.stdout, result.stderr)
            if before is not None:
                assert path.read_bytes() == before, "input file changed"
            leftovers = list(work.glob(path.name + ".goal30-mmap-*"))
            if fault == "collision":
                assert len(leftovers) == 1 and leftovers[0].read_bytes() == b"owned"
                leftovers[0].unlink()
            else:
                assert not leftovers, (fault, leftovers)
            if phase:
                assert "phase=" + phase in result.stderr, result.stderr
            if status == 0:
                for marker in ("status=pass", "fd_closed=pass", "private_isolation=pass", "cold_pwrite=pass",
                               "cold_pread=pass", "usercopy_bytes=4096", "scratch_cleanup=pass"):
                    assert marker in result.stdout, result.stdout
            else:
                assert "status=pass" not in result.stdout
            cases += 1
            return result

        source = work / "source"
        source.write_bytes(bytes([90]) * (1024 * 1024))
        result = run(source)
        assert "pages=256" in result.stdout and "checksum=94371840" in result.stdout
        assert "cold_pread_pattern=165" in result.stdout
        custom = work / "custom"
        custom.write_bytes(bytes([255]) * 8192)
        run(custom, 8192, 255)
        zero = work / "zero"
        zero.write_bytes(bytes(4096))
        run(zero, 4096, 0)
        run(source, 4097, status=2)
        run(source, 4096, 256, status=2)
        run(work / "missing", status=1, phase="private-map")
        short = work / "short"
        short.write_bytes(bytes([90]) * 4095)
        run(short, 4096, status=1, phase="private-map")
        corrupt = work / "corrupt"
        corrupt.write_bytes(bytes([91]) * 4096)
        run(corrupt, 4096, status=1, phase="private-read")
        run(source, fault="interrupted-short")
        for fault, phase in (("write-error", "cold-pwrite"), ("write-zero", "cold-pwrite"),
                             ("flush-error", "cold-fsync"), ("scratch-read-error", "cold-scratch-read"),
                             ("second-flush-error", "cold-scratch-rewrite"), ("read-error", "cold-pread"),
                             ("read-zero", "cold-pread"), ("read-corrupt", "cold-usercopy-verify"),
                             ("read-skip-copy", "cold-usercopy-verify"),
                             ("collision", "cold-usercopy-scratch")):
            run(source, status=1, fault=fault, phase=phase)
    print(f"blockbench mmap actual source: {cases} ASan/UBSan cases PASS; untouched closed-FD PTEs, exact bytes, errors, cleanup")


if __name__ == "__main__":
    main()
