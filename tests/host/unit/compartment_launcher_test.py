#!/usr/bin/env python3
"""Run the actual CLI parser/FD cleanup with only raw WOS calls adapted for Linux."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def main():
    with tempfile.TemporaryDirectory(prefix="wos-compartment-cli-") as temporary:
        folder = Path(temporary)
        include = folder / "include/sys"
        include.mkdir(parents=True)
        shutil.copyfile(ROOT / "toolchain/sysroot/usr/include/sys/callnums.h", include / "callnums.h")
        (include / "process.h").write_text("""#pragma once
#include <sys/callnums.h>
#include <abi/callnums/process.h>
static_assert(static_cast<uint64_t>(ker::abi::callnums::process)==3);
uint64_t test_call(uint64_t,uint64_t,uint64_t,uint64_t);
inline uint64_t syscall(ker::abi::callnums call,uint64_t op,uint64_t a2=0,uint64_t a3=0,uint64_t a4=0) {
 if(call!=ker::abi::callnums::process) __builtin_trap();
 return test_call(op,a2,a3,a4);
}
""")
        seam = folder / "seam.cpp"
        seam.write_text(r"""
#include <abi/compartment.hpp>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
namespace wire=ker::abi::compartment;
constexpr uint64_t PID=0x100000005ULL;
FILE* log_open(){return std::fopen(std::getenv("COMPARTMENT_TEST_LOG"),"a");}
extern "C" int __real_close(int);
extern "C" int __wrap_close(int fd){auto* f=log_open();std::fprintf(f,"close %d\n",fd);std::fclose(f);return __real_close(fd);}
uint64_t test_call(uint64_t op,uint64_t a2,uint64_t a3,uint64_t a4){
 if(op==48){if(a2>=16||a4!=1)std::abort();*reinterpret_cast<wire::Limit*>(a3)={128,256};return 0;}
 auto* f=log_open();if(!f)std::abort();
 if(op==wire::CREATE_OPERATION){
  auto& s=*reinterpret_cast<const wire::Spec*>(a2);
  std::fprintf(f,"create %llu %llu %u %llu %llu %llu %llu\n",
   (unsigned long long)s.size,(unsigned long long)s.version,(unsigned)s.network,
   (unsigned long long)s.authority_ceiling,(unsigned long long)s.asset_count,
   (unsigned long long)s.descriptor_count,(unsigned long long)s.envc);
  auto* assets=reinterpret_cast<const wire::Asset*>(s.assets);
  for(size_t i=0;i<s.asset_count;++i){if(fcntl(assets[i].source_fd,F_GETFD)<0)std::abort();
   std::fprintf(f,"asset %u %.*s\n",assets[i].mode,(int)assets[i].destination.bytes,(const char*)assets[i].destination.address);}
  auto* args=reinterpret_cast<const wire::String*>(s.argv);
  for(size_t i=0;i<s.argc;++i)std::fprintf(f,"arg %.*s\n",(int)args[i].bytes,(const char*)args[i].address);
  args=reinterpret_cast<const wire::String*>(s.envp);
  for(size_t i=0;i<s.envc;++i)std::fprintf(f,"env %.*s\n",(int)args[i].bytes,(const char*)args[i].address);
  std::fprintf(f,"limit 7 %llu %llu\n",(unsigned long long)s.limits[7].soft,(unsigned long long)s.limits[7].hard);
  std::fclose(f);return std::getenv("COMPARTMENT_TEST_REFUSE")?static_cast<uint64_t>(-EPERM):PID;
 }
 if(op==2){if(a2!=PID||a4!=0)std::abort();std::fprintf(f,"wait %llu\n",(unsigned long long)a2);std::fclose(f);
  static unsigned waits=0;if(std::getenv("COMPARTMENT_TEST_EINTR")&&waits++==0)return static_cast<uint64_t>(-EINTR);
  *reinterpret_cast<int*>(a3)=7<<8;return PID;
 }
 std::abort();
}
""")
        binary = folder / "wos-compartment"
        compiler = os.environ.get("WOS_HOST_CXX") or shutil.which("g++", path="/usr/bin:/bin")
        assert compiler
        command = [compiler, "-std=c++23", "-Wall", "-Wextra", "-Werror", "-fno-exceptions",
                   "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-fno-pie", "-no-pie",
                   "-I" + str(folder / "include"), "-I" + str(ROOT / "modules/kern/src"),
                   str(ROOT / "modules/compartment/src/main.cpp"), str(seam),
                   "-Wl,--wrap=close", "-o", str(binary)]
        subprocess.run(command, check=True)
        asset = folder / "asset"
        asset.write_text("retained input")
        log = folder / "effects"
        count = 0

        def run(args, status, create=False, waits=0, closes=0, extra=None):
            nonlocal count
            log.unlink(missing_ok=True)
            env = dict(os.environ, COMPARTMENT_TEST_LOG=str(log), ASAN_OPTIONS="detect_leaks=0")
            env.update(extra or {})
            result = subprocess.run([str(binary)] + args, env=env, capture_output=True, text=True, timeout=20)
            assert result.returncode == status, (args, result.returncode, result.stderr)
            assert "AddressSanitizer" not in result.stderr and "runtime error:" not in result.stderr, result.stderr
            effects = log.read_text().splitlines() if log.exists() else []
            assert sum(line.startswith("create ") for line in effects) == int(create), effects
            assert sum(line.startswith("wait ") for line in effects) == waits, effects
            assert sum(line.startswith("close ") for line in effects) == closes, effects
            count += 1
            return result, effects

        run(["--help"], 0)
        run(["--version"], 0)
        base = ["--asset", str(asset), "/app/service", "0555"]
        result, effects = run(base + ["--env", "KEY=space ; $()", "--limit", "7", "10", "20",
                                     "--", "/app/service", "space value", "$(literal)", ""],
                              7, create=True, waits=1, closes=1)
        assert effects[:2] == ["create 408 1 0 0 1 0 1", "asset 365 /app/service"], effects
        assert effects[2:7] == ["arg /app/service", "arg space value", "arg $(literal)", "arg ", "env KEY=space ; $()"], effects
        assert "host-pid=4294967301" in result.stdout and "network=denied" in result.stdout
        run(base + ["--", "/app/service"], 7, create=True, waits=2, closes=1,
            extra={"COMPARTMENT_TEST_EINTR": "1"})
        run(base + ["--detach", "--share-network", "--ceiling", "0x10", "--", "/app/service"],
            0, create=True, closes=1)
        run(base + ["--", "/app/service"], 1, create=True, closes=1,
            extra={"COMPARTMENT_TEST_REFUSE": "1"})
        for malformed in (["--fd", "-1", "2"], ["--fd", "0", "256"], ["--fd", "infinity", "0"],
                          ["--root-bytes", "4095"], ["--root-bytes", "33558528"],
                          ["--ceiling", "18446744073709551616"], ["--ceiling", "0x"],
                          ["--ceiling", "+1"], ["--limit", "16", "1", "1"],
                          ["--limit", "0", "129", "256"], ["--limit", "0", "2", "1"],
                          ["--unknown"], ["--env"], []):
            run(base + malformed, 2, closes=1)
        run(["--asset", str(asset), "/app/service", "0888", "--", "/app/service"], 2)
        run(base + ["--", "/" + "x" * 512], 2, closes=1)
        run(base + ["--", "/app/service"] + ["a"] * 256, 2, closes=1)
        run(base + sum((["--env", "X=1"] for _ in range(257)), []) + ["--", "/app/service"], 2, closes=1)
        run(base * 65 + ["--", "/app/service"], 2, closes=64)
        print(f"{count} actual launcher parsing/ownership cases passed; kernel calls adapted, no guest service claim")


if __name__ == "__main__":
    main()
