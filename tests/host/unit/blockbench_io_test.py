#!/usr/bin/env python3
"""Exercise the real syscall-only benchmark on disposable host files."""

import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def main():
    scratch = ROOT / "build/goal30-tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="blockbench-test-", dir=scratch) as temporary:
        work = Path(temporary)
        adapter = work / "main.cpp"
        adapter.write_text('''#include "blockbench.hpp"
#include <cerrno>
#include <cstdlib>
#include <unistd.h>
extern "C" off_t __real_lseek(int, off_t, int);
extern "C" int __real_close(int);
extern "C" off_t __wrap_lseek(int fd, off_t offset, int origin) {
    if (std::getenv("WOS_BLOCKBENCH_FAIL_SEEK")) { errno = ESPIPE; return -1; }
    return __real_lseek(fd, offset, origin);
}
extern "C" int __wrap_close(int fd) {
    int result = __real_close(fd);
    if (result == 0 && std::getenv("WOS_BLOCKBENCH_FAIL_CLOSE")) { errno = EIO; return -1; }
    return result;
}
int main(int argc, char** argv) { return run_blockbench(argc - 1, argv + 1); }
''')
        binary = work / "blockbench"
        environment = dict(os.environ, TMPDIR=str(work))
        subprocess.run(["/usr/bin/clang++", "-std=c++23", "-O2", "-Wall", "-Wextra", "-Werror", "-pthread", "-DO_NO_CACHE=0",
                        "-I", str(ROOT / "modules/testprog/src"), str(adapter), str(ROOT / "modules/testprog/src/blockbench.cpp"),
                        "-Wl,--wrap=lseek,--wrap=close", "-o", str(binary)], env=environment, check=True, capture_output=True, text=True)

        def run(*arguments, status=0, injected=None):
            result = subprocess.run([str(binary), *map(str, arguments)], check=False, capture_output=True, text=True, timeout=30,
                                    env=dict(environment, **(injected or {})))
            assert result.returncode == status, (arguments, result.returncode, result.stdout, result.stderr)
            return json.loads(result.stdout) if result.stdout.strip().startswith("{") else None

        for index in range(8):
            (work / f"read.{index}").write_bytes(bytes([0x5a]) * (4 * 1024 * 1024))
        for workers in (1, 8):
            result = run("blockbench-read", "--path", work / "read", "--path-prefix", "--workers", workers,
                         "--bytes", 4 * 1024 * 1024, "--block-bytes", 131072)
            assert result["pass"] and result["bytes"] == workers * 4 * 1024 * 1024
            assert result["workers"] == workers and result["elapsed_ns"] > 0

        bad = work / "corrupt"
        bad.write_bytes(bytes([0x5b]) * 4096)
        result = run("blockbench-read", "--path", bad, "--bytes", 4096, "--block-bytes", 4096, status=1)
        assert not result["pass"] and result["bytes"] == 0 and result["error"] != 0
        result = run("blockbench-read", "--path", work / "read.0", "--bytes", 8 * 1024 * 1024, status=1)
        assert not result["pass"] and result["bytes"] == 4 * 1024 * 1024
        run("blockbench-read", "--path", work / "read", "--workers", 8, status=2)
        run("blockbench-read", "--path", work / "read", "--workers", 9, status=2)
        run("blockbench-read", "--path", work / "read", "--bytes", "-1", status=2)
        run("blockbench-fsync", "--path", "/dev/null", status=2)
        run("blockbench-read", "--path", work / "read.0", "--verify-same-fd", status=2)

        destination = work / "durability"
        result = run("blockbench-fsync", "--path", destination)
        assert result["pass"] and result["operations"] == result["expected_operations"] == 100
        assert len(result["latencies_ns"]) == 100
        assert result["p95_ns"] == sorted(result["latencies_ns"])[94]
        data = destination.read_bytes()
        assert len(data) == 100 * 4096
        for index in range(100):
            assert data[index * 4096:(index + 1) * 4096] == bytes([(0x5a + index) & 0xff]) * 4096
        assert "verify_same_fd" not in result
        for operations in (1, 16):
            result = run("blockbench-fsync", "--path", destination, "--verify-same-fd", "--operations", operations)
            assert result["pass"] and result["verify_same_fd"] and result["operations"] == operations
            data = destination.read_bytes()
            assert len(data) == operations * 4096
            for index in range(operations):
                assert data[index * 4096:(index + 1) * 4096] == bytes([(0x5a + index) & 0xff]) * 4096
        result = run("blockbench-fsync", "--path", destination, "--verify-same-fd", "--operations", 1, status=1,
                     injected={"WOS_BLOCKBENCH_FAIL_SEEK": "1"})
        assert not result["pass"] and result["error"] == 29  # ESPIPE
        for retained in (False, True):
            result = run("blockbench-fsync", "--path", destination, "--operations", 1,
                         *(["--verify-same-fd"] if retained else []), status=1,
                         injected={"WOS_BLOCKBENCH_FAIL_CLOSE": "1"})
            assert not result["pass"] and result["error"] == 5  # EIO
    print("blockbench actual IO: QD1/QD8, default and retained-handle readback, seek/close errors, strict options and fsync latency PASS")


if __name__ == "__main__":
    main()
