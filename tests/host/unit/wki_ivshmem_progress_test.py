#!/usr/bin/env python3
"""Actual cadence/getter bodies; scheduler, MMIO and live transport are not modeled."""
import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
KERN = ROOT / "modules/kern/src"


def function(source, name):
    match = re.search(r"(?:auto|void) " + re.escape(name) + r"\([^)]*\)(?:\s*->\s*\w+)?\s*\{", source)
    assert match, name
    start = match.start()
    pos = match.end()
    depth = 1
    while depth:
        assert pos < len(source)
        depth += (source[pos] == "{") - (source[pos] == "}")
        pos += 1
    return source[start:pos]


def cadence(source):
    body = function(source, "wki_timer_thread")
    start = body.index("        uint64_t sleep_us = WKI_TIMER_IDLE_SLEEP_US;")
    end = body.index("        s_wki_timer_sleep_armed.store", start)
    constants = "\n".join(re.findall(r"constexpr uint64_t WKI_TIMER_[A-Z_]+ = [0-9]+;", source))
    assert "WKI_TIMER_IDLE_SLEEP_US" in constants
    return constants, body[start:end]


PRELUDE = r'''
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <dev/ivshmem/callback.hpp>
static unsigned checks=0;
#define CHECK(x) do { ++checks; if(!(x)){std::fprintf(stderr,"FAIL line=%d expression=%s\n",__LINE__,#x);return 1;} }while(false)
static bool connected=false;
static auto wki_has_connected_peers()->bool {return connected;}
static unsigned hold_enters=0,hold_leaves=0;
static void (*hold_hook)()=nullptr;
namespace ker::mod::sched {
auto hold_kernel_shutdown()->KernelShutdownHold {
 ++hold_enters;
 if(hold_hook) hold_hook();
 return KernelShutdownHold{};
}
KernelShutdownHold::~KernelShutdownHold(){++hold_leaves;}
}
namespace ker::net::wki {
struct Private {
 ker::dev::ivshmem::PciResources resources{};
 std::atomic<bool> ring_failed{false};
};
static Private s_ivshmem_priv;
static std::atomic<bool> s_ivshmem_initialized{false};
'''

CASES = r'''
static void clear_init(){s_ivshmem_initialized.store(false);}
static void fail_ring(){s_ivshmem_priv.ring_failed.store(true);}
static void close_admission(){s_ivshmem_priv.resources.callbacks.close();}
}
int main(int argc,char**argv){
 using namespace ker::net::wki;
 bool const compatibility=argc>1&&std::strcmp(argv[1],"--compatibility")==0;
 bool const active_only=argc>1&&std::strcmp(argv[1],"--active-only")==0;
 auto const before=hold_enters;
 CHECK(!wki_ivshmem_transport_poll_required());
 CHECK(hold_enters==before&&hold_enters==hold_leaves);
 connected=false;
 CHECK(choose_sleep(1000,UINT64_MAX)==1000000);
 connected=true;
 CHECK(choose_sleep(1000,UINT64_MAX)==500000);
 CHECK(choose_sleep(1000,1200)==200);
 CHECK(choose_sleep(1000,999)==1000);
 s_ivshmem_initialized.store(true);
 if(compatibility){
  // Old-policy positives: absent and earlier deadlines are unchanged.
  CHECK(choose_sleep(1000,1200)==200);
  std::printf("compatibility assertions=%u ownership=0\n",checks);return 0;
 }
 CHECK(wki_ivshmem_transport_poll_required());
 CHECK(choose_sleep(1000,UINT64_MAX)==1000);
 if(active_only){std::printf("active assertions=%u\n",checks);return 0;}
 connected=false;
 CHECK(choose_sleep(1000,UINT64_MAX)==1000);
 connected=true;
 for(uint64_t delta: {1ULL,500ULL,999ULL,1000ULL,1001ULL,1000000ULL})
  CHECK(choose_sleep(1000,1000+delta)==std::min<uint64_t>(delta,1000));
 CHECK(choose_sleep(1000,0)==1000);
 CHECK(choose_sleep(UINT64_MAX-1,UINT64_MAX)==1000);
 CHECK(s_ivshmem_priv.resources.callbacks.readers()==0);
 CHECK(hold_enters==hold_leaves);
 s_ivshmem_priv.ring_failed.store(true);
 CHECK(!wki_ivshmem_transport_poll_required());
 CHECK(choose_sleep(1000,UINT64_MAX)==500000);
 s_ivshmem_priv.ring_failed.store(false);
 hold_hook=clear_init;
 CHECK(!wki_ivshmem_transport_poll_required());
 CHECK(s_ivshmem_priv.resources.callbacks.readers()==0);
 hold_hook=nullptr;s_ivshmem_initialized.store(true);
 hold_hook=fail_ring;
 CHECK(!wki_ivshmem_transport_poll_required());
 CHECK(s_ivshmem_priv.resources.callbacks.readers()==0);
 hold_hook=nullptr;s_ivshmem_priv.ring_failed.store(false);
 hold_hook=close_admission;
 CHECK(!wki_ivshmem_transport_poll_required());
 hold_hook=nullptr;
 CHECK(s_ivshmem_priv.resources.callbacks.drained());
 CHECK(!wki_ivshmem_transport_poll_required());
 CHECK(choose_sleep(1000,UINT64_MAX)==500000);
 s_ivshmem_initialized.store(false);
 auto const retired_holds=hold_enters;
 CHECK(!wki_ivshmem_transport_poll_required());
 CHECK(hold_enters==retired_holds&&hold_enters==hold_leaves);
 CHECK(s_ivshmem_priv.resources.callbacks.drained());
 std::printf("current assertions=%u ownership=0\n",checks);
 return 0;
}
'''


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path)
    p.add_argument("--peer-source", type=Path, default=KERN / "net/wki/peer.cpp")
    p.add_argument("--control", choices=("current", "old-active-negative", "old-compatibility"), default="current")
    args = p.parse_args()
    owned = tempfile.TemporaryDirectory(prefix="wki-ivshmem-progress-") if args.output is None else None
    output = Path(owned.name) if owned else args.output
    output.mkdir(parents=True, exist_ok=True)
    iv = KERN / "net/wki/transport_ivshmem.cpp"
    getter = function(iv.read_text(), "wki_ivshmem_transport_poll_required")
    constants, sleep = cadence(args.peer_source.read_text())
    shim = output / "shim"
    (shim / "dev/ivshmem").mkdir(parents=True, exist_ok=True)
    (shim / "platform/sched").mkdir(parents=True, exist_ok=True)
    (shim / "dev/ivshmem/pci_resources.hpp").write_text(
        "#pragma once\n#include <dev/dma/lifecycle.hpp>\nnamespace ker::dev::ivshmem {\n"
        "struct PciResources {dma::lifecycle::Admission callbacks{};};\n}\n")
    (shim / "platform/sched/scheduler.hpp").write_text(
        "#pragma once\nnamespace ker::mod::sched {\n"
        "struct KernelShutdownHold {KernelShutdownHold()=default;~KernelShutdownHold();"
        "KernelShutdownHold(const KernelShutdownHold&)=delete;};\n"
        "auto hold_kernel_shutdown()->KernelShutdownHold;\n}\n")
    tu = output / "actual.cpp"
    tu.write_text(PRELUDE + getter + "\n" + constants +
                  "\nstatic auto choose_sleep(uint64_t now_us,uint64_t NEXT_DEADLINE)->uint64_t{\n" +
                  sleep + "return sleep_us;\n}\n" + CASES)
    binary = output / "actual"
    cmd = [os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++"), "-std=c++23", "-O1", "-g", "-fno-omit-frame-pointer",
           "-fsanitize=address,undefined", "-I" + str(shim), "-I" + str(KERN), str(tu), "-o", str(binary)]
    compile_result = subprocess.run(cmd, text=True, capture_output=True)
    (output / "compile.log").write_text(compile_result.stdout + compile_result.stderr)
    assert compile_result.returncode == 0, compile_result.stderr
    run_args = [str(binary)]
    if args.control == "old-active-negative":
        run_args += ["--active-only"]
    elif args.control == "old-compatibility":
        run_args += ["--compatibility"]
    run_env = dict(os.environ)
    if os.environ.get("WOS_HOST_TEST_DISABLE_LSAN") == "1":
        run_env["ASAN_OPTIONS"] = run_env.get("ASAN_OPTIONS", "") + ":detect_leaks=0"
        run_env["LSAN_OPTIONS"] = run_env.get("LSAN_OPTIONS", "") + ":detect_leaks=0"
    run = subprocess.run(run_args, text=True, capture_output=True, env=run_env)
    (output / "run.log").write_text(run.stdout + run.stderr)
    expected = 1 if args.control == "old-active-negative" else 0
    assert run.returncode == expected, run.stdout + run.stderr
    if expected:
        assert "choose_sleep(1000,UINT64_MAX)==1000" in run.stderr
        assert "AddressSanitizer" not in run.stderr and "runtime error:" not in run.stderr
    files = [iv, args.peer_source, Path(__file__), KERN / "dev/ivshmem/callback.hpp", KERN / "dev/dma/lifecycle.hpp",
             tu, binary, output / "compile.log", output / "run.log",
             shim / "dev/ivshmem/pci_resources.hpp", shim / "platform/sched/scheduler.hpp"]
    result = {"status": "PASS", "control": args.control, "compile": {"command": cmd, "returncode": compile_result.returncode},
              "run": {"command": run_args, "returncode": run.returncode, "expected_returncode": expected},
              "source_scope": "Verbatim actual getter and timer sleep-selection body; real lifecycle/CallbackGuard headers",
              "seams": ["KernelShutdownHold implementation and init/retire boundary hook", "connected-peer Boolean",
                        "PciResources contains real callback admission only; no BAR/IRQ/device/scheduler"],
              "runtime_claims": False,
              "leak_sanitizer_disabled": os.environ.get("WOS_HOST_TEST_DISABLE_LSAN") == "1",
              "pins": [{"path": str(f.resolve()), "sha256": hashlib.sha256(f.read_bytes()).hexdigest()} for f in files]}
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(run.stdout.strip() or run.stderr.strip())
    if owned:
        owned.cleanup()


if __name__ == "__main__":
    main()
