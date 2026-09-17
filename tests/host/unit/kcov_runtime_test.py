#!/usr/bin/env python3
"""Execute the real KCOV C++ runtime and assembly callback on the host."""
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def main():
    with tempfile.TemporaryDirectory(prefix="kcov-runtime-") as directory:
        tmp = Path(directory)
        headers = {
            "defines/defines.hpp": "",
            "platform/mm/page_alloc.hpp": "#include <platform/mm/phys.hpp>\n",
            "platform/mm/paging.hpp": (
                "#include <cstdint>\n"
                "namespace ker::mod::mm::paging { constexpr uint64_t PAGE_SIZE = 4096; }\n"
            ),
            "platform/mm/phys.hpp": """
#pragma once
#include <cstdint>
#include <string_view>
namespace ker::mod::mm {
enum class PhysicalPageOwner { KCOV_BUFFER };
namespace phys {
auto get_free_mem_bytes() -> uint64_t;
auto page_alloc_can_satisfy(uint64_t, uint64_t) -> bool;
auto page_alloc_may_fail(PhysicalPageOwner, uint64_t, std::string_view) -> void*;
}
}
""",
        }
        for name, content in headers.items():
            path = tmp / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        sites = tmp / "sites.asm"
        sites.write_text("""
bits 64
section .text
extern __sanitizer_cov_trace_pc
global kcov_test_sites, kcov_test_sites_end
kcov_test_sites:
%rep 128
    call __sanitizer_cov_trace_pc
%endrep
    ret
kcov_test_sites_end:
""")
        for source, name in [
            (ROOT / "modules/kern/src/sanitizer/kcov_trace_pc.asm", "callback"),
            (sites, "sites"),
        ]:
            subprocess.run([
                "nasm", "-f", "elf64", "-DWOS_KCOV=1", str(source),
                "-o", str(tmp / f"{name}.o"),
            ], check=True)
        executable = tmp / "kcov-runtime"
        subprocess.run([
            "clang++", "--target=x86_64-unknown-linux-gnu", "-std=c++23", "-O2",
            "-pthread", "-no-pie", "-Wl,-z,noexecstack", "-DWOS_KCOV=1",
            "-I", str(tmp), "-I", str(ROOT / "modules/kern/src"),
            str(ROOT / "modules/kern/src/sanitizer/kcov.cpp"),
            str(ROOT / "tests/host/unit/kcov_runtime_test.cpp"),
            str(tmp / "callback.o"), str(tmp / "sites.o"), "-o", str(executable),
        ], check=True)
        subprocess.run([str(executable)], check=True, timeout=30)
    print("KCOV runtime tests passed")


if __name__ == "__main__":
    main()
