#!/usr/bin/env python3
"""Run actual usercopy frame ownership with actual Object/family implementations.

Only the incoming pin, allocator, locks and queued retirement endpoint are Host
ports. This does not qualify PTE publication, WOS scheduling or hardware TLB ACK.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import shutil
import signal
import tempfile
import time


ROOT = Path(__file__).resolve().parents[3]
KERN = ROOT / "modules/kern/src"
UNIT = Path(__file__).parent


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cxx", default="/usr/bin/clang++")
    args = parser.parse_args()
    runner = runpy.run_path(str(UNIT / "futex_frame_lifetime_runtime_test.py"))
    pin = runner["pin"]
    inputs = {Path(__file__), UNIT / "device_frame_export_cases.inc",
              UNIT / "file_mmap_registry_runtime_test.py", UNIT / "futex_frame_lifetime_runtime_test.py",
              *(KERN / name for name in ("platform/sys/usercopy.hpp", "platform/sys/usercopy.cpp",
                                        "platform/mm/virt.hpp", "platform/mm/phys.hpp",
                                        "platform/mm/file_mapping_lifetime.hpp", "platform/mm/file_mapping_lifetime.cpp",
                                        "dev/device_mapping_object.hpp", "dev/device_mapping_object.cpp"))}
    for directory in (UNIT / "file_binding_snapshot_shim", UNIT / "device_mapping_object_shim"):
        inputs.update(path for path in directory.rglob("*") if path.is_file())
    compiler = Path(args.cxx).absolute()
    inputs.add(compiler.resolve(strict=True))
    with compiler.open("rb") as executable:
        if executable.read(4) != b"\x7fELF":
            raise ValueError("--cxx must name an executable Host ELF compiler")
    before = [pin(path) for path in sorted(inputs)]
    deadline = time.monotonic() + 90
    groups = []
    report = {"pass": False, "commands": [], "sources_held": False,
              "compiled_inputs_held": False, "scratch_removed": False,
              "whole_budget_seconds": 90, "scratch_byte_budget": runner["SCRATCH_BUDGET"],
              "native_runtime_qualified": False, "input_pins_before": before}
    runner["subreaper"]()
    def interrupted(signum, _frame):
        raise InterruptedError(f"received signal {signum}")
    handlers = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    os.environ["UBSAN_OPTIONS"] = "halt_on_error=1:print_stacktrace=1"
    capture = runpy.run_path(str(UNIT / "file_mmap_registry_runtime_test.py"))["capture"]
    source = (KERN / "platform/sys/usercopy.cpp").read_text()
    header = (KERN / "platform/sys/usercopy.hpp").read_text()
    virt = (KERN / "platform/mm/virt.hpp").read_text()
    phys = (KERN / "platform/mm/phys.hpp").read_text()
    rows = []

    def actual(text, anchor, declaration=False):
        body, row = capture(text, anchor, declaration=declaration)
        rows.append(row)
        return body + "\n"

    methods = []
    # The move ctor's member initializers contain braces. Its exact complete
    # source extent ends at the following uniquely named definition.
    begin = source.index("UserPageFrameRef::UserPageFrameRef(UserPageFrameRef&&")
    end = source.index("auto UserPageFrameRef::operator=", begin)
    constructor = source[begin:end]
    rows.append({"name": "complete-move-constructor", "bytes": len(constructor.encode()),
                 "sha256": hashlib.sha256(constructor.encode()).hexdigest()})
    methods.append(constructor)
    for anchor in (
        "UserPageFrameRef::~UserPageFrameRef()", "auto UserPageFrameRef::operator=",
        "auto UserPageFrameRef::valid(", "void UserPageFrameRef::reset(",
        "StableUserPage::~StableUserPage()", "auto StableUserPage::valid(",
        "auto StableUserPage::retain_frame(", "void StableUserPage::reset(",
    ):
        methods.append(actual(source, anchor))
    scratch = Path(tempfile.mkdtemp(prefix="wos-device-frame-", dir="/tmp"))
    try:
        report["scratch"] = str(scratch)
        include = scratch / "include"

        def write(name, contents):
            path = include / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(contents)

        write("platform/sys/usercopy.hpp", header)
        write("platform/sched/task.hpp", """#pragma once
#include <cstdlib>
namespace ker::mod::sched::task {
struct Task {unsigned accesses{};void release_usercopy_pagemap(){if(!accesses)std::abort();--accesses;}};
}
""")
        write("platform/mm/phys.hpp", '#pragma once\n#include "' + str(UNIT / "device_mapping_object_shim/platform/mm/phys.hpp") + '"\n'
              + 'namespace ker::mod::mm::phys {class PageAllocator;\n'
              + actual(phys, "enum class PageLookupOwner", True)
              + actual(phys, "struct PageLookupHint", True)
              + '''inline void(*after_frame_return)(void*){};
inline auto page_ref_try_inc(void*p,PageLookupHint*)->bool{return page_ref_try_inc(p);}
inline auto page_ref_dec(void*p,PageLookupHint*)->uint32_t{auto n=page_ref_dec(p);if(after_frame_return)after_frame_return(p);return n;}
}
''')
        write("platform/mm/virt.hpp", '''#pragma once
#include <platform/mm/phys.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/file_mapping_lifetime.hpp>
namespace ker::mod::mm::virt {
using vaddr_t=uint64_t;using paddr_t=uint64_t;using PageTable=paging::PageTable;
inline constexpr uint64_t PADDR_INVALID=UINT64_MAX;
''' + actual(virt, "struct UserPagePin {", True) + actual(virt, "struct UserFutexIdentity {", True) + '''void unpin_user_page(UserPagePin&);
}
''')
        generated = scratch / "actual-usercopy.cpp"
        generated.write_text('''#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <span>
#include <thread>
#include <utility>
#define private public
#include <platform/sys/usercopy.hpp>
#undef private
#include <snapshot_fixture.hpp>
#include <object_pages.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
namespace ker::mod::sys::usercopy {
''' + "\n".join(methods) + '\n}\n' + (UNIT / "device_frame_export_cases.inc").read_text())
        common = [str(compiler), "-std=c++23", "-pthread", "-I" + str(include),
                  "-I" + str(UNIT / "file_binding_snapshot_shim"), "-I" + str(UNIT / "device_mapping_object_shim"),
                  "-I" + str(KERN), "-I" + str(ROOT / "modules")]
        sources = [str(generated), str(KERN / "platform/mm/file_mapping_lifetime.cpp"),
                   str(KERN / "dev/device_mapping_object.cpp"), str(UNIT / "file_binding_snapshot_shim/retirement_port.cpp"),
                   str(UNIT / "device_mapping_object_shim/object_pages.cpp")]
        def command(arguments, name):
            result = runner["command"](arguments, scratch / name, scratch, deadline, groups)
            report["commands"].append(result)
            if result["exit"] != 0:
                print((scratch / name).read_text()[-24000:])
                raise ValueError(f"{name} native exit {result['exit']}")
        def dependencies(name):
            command(common + ["-M", *sources], name)
            paths = set()
            for line in (scratch / name).read_text().replace("\\\n", " ").splitlines():
                if not line.strip():
                    continue
                if ": " not in line:
                    raise ValueError("compiler dependency target missing")
                words = re.findall(r"(?:\\.|[^\s])+", line.split(": ", 1)[1])
                paths.update(Path(re.sub(r"\\(.)", r"\1", word)).resolve(strict=True) for word in words)
            if not paths or len(paths) > 2048:
                raise ValueError("compiler dependency count outside bounded contract")
            return sorted(paths)
        compiled_inputs = dependencies("dependencies-before.log")
        compiled_before = [pin(path) for path in compiled_inputs]
        report["compiled_inputs_before"] = compiled_before
        command(common + ["-O1", "-g", "-fsanitize=address,undefined", "-fno-omit-frame-pointer", *sources,
                          "-o", str(scratch / "test")], "compile.log")
        command([str(scratch / "test")], "run.log")
        output = (scratch / "run.log").read_text()
        print(output, end="")
        if not re.fullmatch(r"PASS device_frame_exports cases=7 assertions=\d+ actual_usercopy=1 actual_object=1 actual_family=1 hardware_root=0\n", output):
            raise ValueError("actual export cases did not all report terminal success")
        compiled_after = [pin(path) for path in dependencies("dependencies-after.log")]
        report["compiled_inputs_after"] = compiled_after
        report["compiled_inputs_held"] = compiled_before == compiled_after
        report["sources_held"] = before == [pin(path) for path in sorted(inputs)]
        if not report["sources_held"] or not report["compiled_inputs_held"]:
            raise ValueError("compiled input identities changed during qualification")
        report["actual_extractions"] = rows
        report["pass"] = True
    except (Exception, KeyboardInterrupt) as error:
        report["error"] = f"{type(error).__name__}: {error}"
    finally:
        report["owned_sessions_absent"] = not groups
        if not groups:
            shutil.rmtree(scratch)
            report["scratch_removed"] = not scratch.exists()
        else:
            report["pass"] = False
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
        print(json.dumps(report, sort_keys=True))
    return 0 if report["pass"] and report["scratch_removed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
