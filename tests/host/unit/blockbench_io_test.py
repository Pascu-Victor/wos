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
        adapter.write_text('#include "blockbench.hpp"\nint main(int argc, char** argv) { return run_blockbench(argc - 1, argv + 1); }\n')
        binary = work / "blockbench"
        environment = dict(os.environ, TMPDIR=str(work))
        subprocess.run(["/usr/bin/clang++", "-std=c++23", "-O2", "-Wall", "-Wextra", "-Werror", "-pthread", "-DO_NO_CACHE=0",
                        "-I", str(ROOT / "modules/testprog/src"), str(adapter), str(ROOT / "modules/testprog/src/blockbench.cpp"),
                        "-o", str(binary)], env=environment, check=True, capture_output=True, text=True)

        def run(*arguments, status=0):
            result = subprocess.run([str(binary), *map(str, arguments)], check=False, capture_output=True, text=True, timeout=30)
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

        destination = work / "durability"
        result = run("blockbench-fsync", "--path", destination)
        assert result["pass"] and result["operations"] == result["expected_operations"] == 100
        assert len(result["latencies_ns"]) == 100
        assert result["p95_ns"] == sorted(result["latencies_ns"])[94]
        data = destination.read_bytes()
        assert len(data) == 100 * 4096
        for index in range(100):
            assert data[index * 4096:(index + 1) * 4096] == bytes([(0x5a + index) & 0xff]) * 4096
    print("blockbench actual IO: QD1/QD8 exact pattern, corrupt/short input, strict options, 100 fsync latency samples PASS")


if __name__ == "__main__":
    main()
