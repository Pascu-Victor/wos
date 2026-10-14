#!/usr/bin/env python3
"""Compile actual file-registry transactions and real SmallVec against byte oracles.

The complete FileMmapRange/register/remove/clone/range definitions and unchanged
SmallVec header are compiled. Mutex, File retain/close and PageTable are explicit
Host primitives. This checks registry geometry, policy and owner transactions,
not real PTE/TLB, backing I/O, dirty writeback, Task GC or shared-file coherence.
The real pure native entry declarations are imported so the unchanged clone
guard compiles; its native head stays empty in these RAW-only vectors. No native
entry, callback, worker or retirement is modeled or qualified here.
--vmem-source selects a complete archived predecessor for a real negative control.
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = next(path for path in Path(__file__).resolve().parents
            if (path / "modules/kern/src").is_dir() and (path / "tests/host").is_dir())
KERN = ROOT / "modules/kern/src"
SOURCE = KERN / "syscalls_impl/vmem/sys_vmem.cpp"
SMALLVEC = KERN / "util/smallvec.hpp"
FAMILY_HEADER = KERN / "platform/mm/file_mapping_lifetime.hpp"
NATIVE_HEADER = KERN / "platform/mm/unpublished_mapping_retirement.hpp"
SCRATCH_BUDGET = 64 * 1024 * 1024
LOG_BUDGET = 1024 * 1024
FILE_BUDGET = 8 * 1024 * 1024

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <new>
#include <utility>
#include <vector>
namespace fixture {
unsigned assertions{}, failures{}, cases{}, projection_failures{}, retains{}, puts{}, final_closes{};
unsigned lock_depth{}, growth_attempts{}, failed_growth{}, successful_growth{};
int fail_growth_after{-1};
void check(bool value, const char* message) {
    ++assertions;
    if (!value && failures++ < 16) std::fprintf(stderr, "FAIL: %s\n", message);
}
struct Mutex {
    bool held{};
    void lock() { check(!held && lock_depth == 0, "registry Mutex acquisition"); held=true; ++lock_depth; }
    void unlock() { check(held && lock_depth == 1, "registry Mutex release"); held=false; --lock_depth; }
};
}
// Only actual SmallVec nothrow array growth reaches this controlled seam.
// The oracle's ordinary vector allocations are not affected.
void* operator new[](std::size_t bytes, const std::nothrow_t&) noexcept {
    ++fixture::growth_attempts;
    if (fixture::fail_growth_after == 0) { ++fixture::failed_growth; return nullptr; }
    if (fixture::fail_growth_after > 0) --fixture::fail_growth_after;
    try { auto* p=::operator new[](bytes); ++fixture::successful_growth; return p; }
    catch (...) { ++fixture::failed_growth; return nullptr; }
}
#include <util/smallvec.hpp>
#include <platform/mm/unpublished_mapping_retirement.hpp>
namespace ker::mod::sys { using Mutex=fixture::Mutex; }
namespace ker::mod::mm::paging { struct PageTable { unsigned identity{}; }; }
namespace ker::vfs {
struct File { std::atomic<int> refcount{1}; unsigned identity{}; };
void vfs_retain_file(File* file) {
    fixture::check(file != nullptr && file->refcount.load() > 0, "retain live exact File");
    ++fixture::retains; ++file->refcount;
}
void vfs_put_file(File* file) {
    if (file == nullptr) return;
    int old=file->refcount.fetch_sub(1);
    fixture::check(old > 0, "no File reference underflow"); ++fixture::puts;
    if (old==1) {
        fixture::check(fixture::lock_depth==0, "actual final File close outside registry lock");
        ++fixture::final_closes;
    }
}
}
'''

CASES = r'''
namespace fixture {
namespace actual=ker::syscall::vmem;
using Range=actual::FileMmapRange;
using Root=ker::mod::mm::paging::PageTable;
constexpr uint64_t BASE=0x10000, STRIDE=64;
constexpr unsigned MAX_ROWS=160, BYTES=MAX_ROWS*STRIDE;
struct Cell { int origin{-1}; };
bool same(const Range& a,const Range& b) {
    return a.pagemap==b.pagemap && a.start==b.start && a.length==b.length &&
           a.file_offset==b.file_offset && a.file==b.file && a.writeback==b.writeback;
}
struct World {
    std::array<Root,3> roots{};
    std::array<ker::vfs::File,5> files{};
    std::vector<Range> anchors;
    std::array<std::array<Cell,BYTES>,3> expected{};
    unsigned initial_retains{retains}, initial_puts{puts}, initial_closes{final_closes};
    World() {
#if HAVE_NATIVE_REGISTRY_HEAD
        check(actual::g_native_file_mmap_ranges == nullptr, "RAW-only Host never owns native registry entries");
#endif
        check(actual::g_file_mmap_ranges.empty(), "previous fixture registry fully drained");
        actual::g_file_mmap_ranges=decltype(actual::g_file_mmap_ranges){};
        for(unsigned i=0;i<roots.size();++i) roots[i].identity=i;
        for(unsigned i=0;i<files.size();++i) files[i].identity=i;
    }
    void paint(unsigned root,unsigned origin) {
        const auto& anchor=anchors.at(origin);
        for(uint64_t va=anchor.start;va<anchor.start+anchor.length;++va)
            expected.at(root).at(va-BASE)={static_cast<int>(origin)};
    }
    void seed(unsigned count,bool one_root=false) {
        for(unsigned i=0;i<count;++i) {
            Range anchor{.pagemap=&roots.at(one_root?0:i%roots.size()), .start=BASE+i*STRIDE,
                         .length=32, .file_offset=1000+i*127, .file=&files.at(i%files.size()),
                         .writeback=(i%2)!=0};
            anchors.push_back(anchor);
            check(actual::register_file_mmap_range(anchor.pagemap,anchor.start,anchor.length,anchor.file_offset,
                                                 anchor.file,anchor.writeback)==0,"actual registry seed publication");
            paint(anchor.pagemap->identity,i);
        }
    }
    void erase(unsigned root,uint64_t first,uint64_t last) {
        for(unsigned x=0;x<BYTES;++x) {
            uint64_t va=BASE+x;
            if(va>=first && va<last) expected.at(root).at(x)={};
        }
    }
    std::vector<Range> snapshot() const {
        std::vector<Range> out;
        for(const auto& row:actual::g_file_mmap_ranges) out.push_back(row);
        return out;
    }
    std::array<int,5> references() const {
        std::array<int,5> refs{};
        for(unsigned i=0;i<files.size();++i) refs.at(i)=files.at(i).refcount.load();
        return refs;
    }
    void unchanged(const std::vector<Range>& rows,const std::array<int,5>& refs) {
        check(actual::g_file_mmap_ranges.size()==rows.size(),"failed transaction preserves row count");
        for(size_t i=0;i<std::min(rows.size(),actual::g_file_mmap_ranges.size());++i)
            check(same(rows.at(i),actual::g_file_mmap_ranges.at(i)),"failed transaction preserves all row fields");
        check(references()==refs,"failed transaction preserves every File reference");
        check(lock_depth==0 && !actual::g_file_mmap_ranges_lock.held,"failed transaction unlocks registry");
    }
    void verify() {
        ++cases;
        std::array<std::array<unsigned,BYTES>,3> visits{};
        std::array<int,5> owners{};
        for(const auto& row:actual::g_file_mmap_ranges) {
            int root=-1,file=-1;
            for(unsigned i=0;i<roots.size();++i) if(row.pagemap==&roots.at(i)) root=i;
            for(unsigned i=0;i<files.size();++i) if(row.file==&files.at(i)) file=i;
            check(root>=0 && file>=0,"published row exact root and File identity");
            if(root<0 || file<0) continue;
            ++owners.at(file);
            bool bounded=row.start>=BASE && row.length>0 && row.start<BASE+BYTES && row.length<=BASE+BYTES-row.start;
            check(bounded,"published row independently bounded");
            if(!bounded) continue;
            for(uint64_t va=row.start;va<row.start+row.length;++va) {
                unsigned x=va-BASE; ++visits.at(root).at(x);
                int origin=expected.at(root).at(x).origin;
                check(origin>=0,"removed byte has no published registry owner");
                if(origin<0) continue;
                const auto& anchor=anchors.at(origin);
                check(row.file==anchor.file && row.writeback==anchor.writeback,"preserve File and writeback policy");
                __uint128_t want=__uint128_t(anchor.file_offset)+(va-anchor.start);
                __uint128_t got=__uint128_t(row.file_offset)+(va-row.start);
                if(got!=want || got>UINT64_MAX) ++projection_failures;
                check(got==want && got<=UINT64_MAX,"anchored byte-to-file projection");
            }
        }
        for(unsigned r=0;r<roots.size();++r)
            for(unsigned x=0;x<BYTES;++x)
                check(visits.at(r).at(x)==(expected.at(r).at(x).origin>=0?1U:0U),"independent exact byte coverage without overlap");
        unsigned count=0;
        for(unsigned i=0;i<files.size();++i) {
            count+=owners.at(i);
            check(files.at(i).refcount.load()==1+owners.at(i),"one File reference per published row plus fixture witness");
        }
        check(retains-initial_retains-(puts-initial_puts)==count,"retain/put operations match published owners");
        check(lock_depth==0 && !actual::g_file_mmap_ranges_lock.held,"registry unlocked at observer boundary");
        check(final_closes==initial_closes,"no close while fixture witness retains File");
    }
    ~World() {
        fail_growth_after=-1;
        for(auto& root:roots) check(actual::unregister_file_mmap_range(&root,BASE,BYTES)==0,"actual whole-root metadata retirement");
        check(actual::g_file_mmap_ranges.empty(),"all exact fixture roots drained");
        check(retains-initial_retains==puts-initial_puts,"all published registry references returned");
        for(auto& file:files) {
            check(file.refcount.load()==1,"fixture witness is last File owner before release");
            ker::vfs::vfs_put_file(&file);
        }
        check(final_closes-initial_closes==files.size(),"every fixture File final close once unlocked");
    }
};
enum class Cut { MIDDLE, PREFIX, SUFFIX, WHOLE, NONE };
void growth_case(unsigned count,unsigned index,Cut cut) {
    World world; world.seed(count); Range anchor=world.anchors.at(index);
    uint64_t first=anchor.start,last=anchor.start+anchor.length;
    switch(cut) {
        case Cut::MIDDLE: first+=8; last-=8; break;
        case Cut::PREFIX: last=first+8; break;
        case Cut::SUFFIX: first=last-8; break;
        case Cut::WHOLE: break;
        case Cut::NONE: first=last+1; last=first+4; break;
    }
    unsigned grows=successful_growth;
    bool changed=false;
    check(actual::unregister_one_file_mmap_overlap(anchor.pagemap,first,last,&changed)==0,"actual overlap transaction succeeds");
    check(changed==(cut!=Cut::NONE),"actual removed_any exact affected-row outcome");
    if(cut==Cut::MIDDLE) check(successful_growth==grows+1,"real SmallVec heap reallocation forced");
    else check(successful_growth==grows,"trim/remove/no-match needs no allocation");
    world.erase(anchor.pagemap->identity,first,last); world.verify();
}
void allocation_failure(unsigned count,unsigned index) {
    World world; world.seed(count); Range anchor=world.anchors.at(index);
    auto before=world.snapshot(); auto refs=world.references(); unsigned failures_before=failed_growth;
    fail_growth_after=0; bool changed=true;
    int result=actual::unregister_one_file_mmap_overlap(anchor.pagemap,anchor.start+8,anchor.start+24,&changed);
    check(result==-ENOMEM && !changed && failed_growth==failures_before+1,"actual split OOM before mutation");
    world.unchanged(before,refs); world.verify(); fail_growth_after=-1;
    check(actual::unregister_file_mmap_range(anchor.pagemap,anchor.start+8,16)==0,"same transaction retry succeeds");
    world.erase(anchor.pagemap->identity,anchor.start+8,anchor.start+24); world.verify();
}
void all_geometries() {
    for(unsigned first=0;first<32;++first) for(unsigned last=first+1;last<=32;++last) {
        World world; world.seed(1); Range anchor=world.anchors.front();
        check(actual::unregister_file_mmap_range(anchor.pagemap,anchor.start+first,last-first)==0,"all nonempty byte cuts");
        world.erase(0,anchor.start+first,anchor.start+last); world.verify();
    }
}
void clone(bool oom) {
    World world; world.seed(33,true); auto before=world.snapshot(); unsigned failed_before=failed_growth;
    fail_growth_after=oom?1:-1;
    bool ok=actual::clone_file_mmap_ranges_for_pagemap_locked(&world.roots.at(0),&world.roots.at(1));
    fail_growth_after=-1;
    check(ok==!oom,"actual clone success/OOM result");
    // Real current SmallVec has heap capacity32 at seed33. The first clone
    // grow succeeds to64; exactly31 new child rows precede the next grow.
    unsigned child_rows=oom?31:33;
    check(actual::g_file_mmap_ranges.size()==33+child_rows,"clone exact owned prefix on allocation failure");
    if(oom) check(failed_growth==failed_before+1,"clone controlled second growth failed");
    for(unsigned i=0;i<child_rows;++i) world.paint(1,i);
    for(unsigned i=0;i<before.size();++i) check(same(before.at(i),actual::g_file_mmap_ranges.at(i)),"clone preserves source rows");
    world.verify();
    check(actual::unregister_file_mmap_range(&world.roots.at(1),BASE,BYTES)==0,"failed/successful child clone rollback");
    for(auto& cell:world.expected.at(1)) cell={}; world.verify();
}
void invalid_inputs() {
    World world; world.seed(1); auto before=world.snapshot(); auto refs=world.references();
    auto* root=&world.roots.at(0); auto* file=&world.files.at(0);
    check(actual::register_file_mmap_range(nullptr,BASE,32,0,file,false)==-EINVAL,"null register root");
    check(actual::register_file_mmap_range(root,BASE,32,0,nullptr,false)==-EINVAL,"null register File");
    check(actual::register_file_mmap_range(root,BASE,0,0,file,false)==-EINVAL,"zero register length");
    check(actual::register_file_mmap_range(root,UINT64_MAX-3,8,0,file,false)==-EINVAL,"overflow register geometry");
    check(actual::unregister_file_mmap_range(nullptr,BASE,1)==-EINVAL,"null removal root");
    check(actual::unregister_file_mmap_range(root,BASE,0)==-EINVAL,"zero removal length");
    check(actual::unregister_file_mmap_range(root,UINT64_MAX-3,8)==-EINVAL,"overflow removal geometry");
    check(actual::clone_file_mmap_ranges_for_pagemap_locked(nullptr,root),"null-source clone no-op");
    check(actual::clone_file_mmap_ranges_for_pagemap_locked(root,nullptr),"null-destination clone no-op");
    check(actual::clone_file_mmap_ranges_for_pagemap_locked(root,root),"same-root clone no-op");
    world.unchanged(before,refs); world.verify();
}
}
int main() {
    using namespace fixture;
    // The archived predecessor reaches this real heap-row split first. Its
    // complete body compiles, then ASan reports the post-growth stale access.
    growth_case(33,32,Cut::MIDDLE);
    for(unsigned count:std::array<unsigned,3>{33,64,128}) {
        for(unsigned index:std::array<unsigned,2>{32,count-1}) {
            for(Cut cut:std::array<Cut,5>{Cut::MIDDLE,Cut::PREFIX,Cut::SUFFIX,Cut::WHOLE,Cut::NONE})
                growth_case(count,index,cut);
            allocation_failure(count,index);
        }
    }
    all_geometries(); clone(false); clone(true); invalid_inputs();
    check(lock_depth==0 && actual::g_file_mmap_ranges.empty(),"terminal registry/lock empty");
    std::printf("%s cases=%u assertions=%u projection_failures=%u failures=%u growth_attempts=%u successful_growth=%u failed_growth=%u retains=%u puts=%u final_closes=%u real_registry=1 real_smallvec=1\n",
                failures==0?"PASS":"FAIL",cases,assertions,projection_failures,failures,growth_attempts,successful_growth,failed_growth,retains,fixture::puts,final_closes);
    return failures==0?0:3;
}
'''


def pin(path: Path) -> dict:
    data = path.read_bytes()
    return {"path": str(path.resolve()), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def capture(source: str, anchor: str, *, declaration: bool = False) -> tuple[str, dict]:
    """Extract a complete actual definition without changing any original byte."""
    clean = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                   lambda match: " " * len(match[0]), source)
    starts = [m.start() for m in re.finditer(re.escape(anchor), clean)]
    if len(starts) != 1:
        raise ValueError(f"expected one definition of {anchor}, found {len(starts)}")
    start = clean.rfind("\n", 0, starts[0]) + 1
    depth = 0
    opening = None
    for index in range(starts[0], len(clean)):
        char = clean[index]
        if char == "(": depth += 1
        elif char == ")": depth -= 1
        elif char == "{" and depth == 0:
            opening = index
            break
        elif char == ";" and depth == 0:
            raise ValueError(f"forward declaration is not a body: {anchor}")
    if opening is None:
        raise ValueError(f"definition missing body: {anchor}")
    depth = 0
    for index in range(opening, len(clean)):
        if clean[index] == "{": depth += 1
        elif clean[index] == "}":
            depth -= 1
            if depth == 0:
                end = index + 1
                if declaration:
                    if clean[end:end + 1] != ";":
                        raise ValueError("complete type terminator missing")
                    end += 1
                body = source[start:end]
                return body, {"name": anchor, "line": source.count("\n", 0, start) + 1,
                              "bytes": len(body.encode()), "sha256": hashlib.sha256(body.encode()).hexdigest()}
    raise ValueError(f"unterminated body: {anchor}")


def generate(scratch: Path, selected: Path) -> list[dict]:
    include = scratch / "include"
    actual = []
    for name, data in [("util/smallvec.hpp", SMALLVEC.read_bytes()),
                       ("platform/mm/file_mapping_lifetime.hpp", FAMILY_HEADER.read_bytes()),
                       ("platform/mm/unpublished_mapping_retirement.hpp", NATIVE_HEADER.read_bytes()),
                       ("platform/mm/dyn/kmalloc.hpp", b"#pragma once\n"),
                       ("util/hcf.hpp", b"#pragma once\n#include <cstdlib>\n[[noreturn]] inline void hcf(){std::abort();}\n")]:
        path = include / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    actual.append({"name": "whole-real-util/smallvec.hpp", **pin(SMALLVEC)})
    for header in [FAMILY_HEADER, NATIVE_HEADER]:
        actual.append({"name": "whole-real-" + header.name, **pin(header)})
    source = selected.read_text()
    chunks = [PRELUDE, "\nnamespace ker::syscall::vmem {\n"]
    for anchor, declaration in [("struct FileMmapRange {", True),
                                 ("auto ranges_overlap(", False),
                                 ("auto checked_range_end(", False),
                                 ("auto file_mmap_range_end(", False)]:
        body, row = capture(source, anchor, declaration=declaration)
        actual.append(row)
        chunks.append(f'#line {row["line"]} "actual-selected-sys_vmem.cpp"\n' + body + "\n")
    for anchor in ["ker::util::SmallVec<FileMmapRange, 32> g_file_mmap_ranges;",
                   "ker::mod::sys::Mutex g_file_mmap_ranges_lock;"]:
        if source.count(anchor) != 1:
            raise ValueError("whole real registry declaration unavailable")
        chunks.append(anchor + "\n")
        actual.append({"name": anchor, "bytes": len(anchor), "sha256": hashlib.sha256(anchor.encode()).hexdigest()})
    native = "g_native_file_mmap_ranges" in source
    if native:
        for pattern in [r"^using NativeMappingEntry = [^\n]+", r"^NativeMappingEntry\* g_native_file_mmap_ranges[^\n]+"]:
            matches = re.findall(pattern, source, re.M)
            if len(matches) != 1:
                raise ValueError("whole real native registry declaration unavailable")
            declaration = matches[0]
            chunks.append(declaration + "\n")
            actual.append({"name": "actual-native-type-boundary", "bytes": len(declaration.encode()),
                           "sha256": hashlib.sha256(declaration.encode()).hexdigest()})
    for anchor in ["auto register_file_mmap_range(", "auto unregister_one_file_mmap_overlap(",
                   "auto unregister_file_mmap_range(", "auto clone_file_mmap_ranges_for_pagemap_locked("]:
        body, row = capture(source, anchor)
        actual.append(row)
        chunks.append(f'#line {row["line"]} "actual-selected-sys_vmem.cpp"\n' + body + "\n")
    chunks.extend(["}\n", f"#define HAVE_NATIVE_REGISTRY_HEAD {int(native)}\n",
                   '#line 1 "independent-fixture-oracles"\n', CASES])
    (scratch / "harness.cpp").write_text("\n".join(chunks))
    return actual



def allocated(path: Path) -> int:
    return sum(p.stat().st_blocks * 512 for p in path.rglob("*") if p.is_file())


def subreaper() -> None:
    if not sys.platform.startswith("linux"):
        raise ValueError("bounded owned-session runner requires Linux")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")


def process_identity(pid: int) -> tuple[int, int, str, int, int]:
    fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
    return pid, int(fields[19]), fields[0], int(fields[1]), int(fields[3])


def owned_session(leader: int) -> list[tuple[int, int, str, int, int]]:
    result = []
    for path in Path("/proc").iterdir():
        if not path.name.isdecimal():
            continue
        try:
            identity = process_identity(int(path.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        # Sequential driver children only; the subreaper adopts escaped owned
        # compiler descendants. An original unreaped leader reserves the SID.
        if identity[4] == leader or identity[3] == os.getpid():
            result.append(identity)
    return result


def kill_identity(identity: tuple[int, int, str, int, int]) -> None:
    pid, started, state, _, _ = identity
    if state == "Z":
        return
    try:
        fd = os.pidfd_open(pid, 0)
    except ProcessLookupError:
        return
    try:
        if process_identity(pid)[1] == started:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
    except (FileNotFoundError, ProcessLookupError):
        pass
    finally:
        os.close(fd)


def reap_group(process: subprocess.Popen, deadline: float) -> None:
    while True:
        identities = owned_session(process.pid)
        for identity in identities:
            kill_identity(identity)
        for pid, _, _, _, _ in identities:
            if pid == process.pid:
                continue
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass
        remaining = owned_session(process.pid)
        if all(pid == process.pid and state == "Z" for pid, _, state, _, _ in remaining):
            process.wait(timeout=max(0.01, deadline - time.monotonic()))
            if owned_session(process.pid):
                raise ValueError("owned session/adopted descendants remain after leader reap")
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("owned session not proven absent; preserve scratch")
        time.sleep(0.01)


def command(argv: list[str], log: Path, scratch: Path, deadline: float, groups: list[int]) -> dict:
    if time.monotonic() >= deadline - 5:
        raise TimeoutError("60-second whole budget exhausted before command")

    def limits() -> None:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_FSIZE, (FILE_BUDGET, FILE_BUDGET))

    started = time.monotonic()
    process = None
    with log.open("xb") as output:
        try:
            process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT,
                                       start_new_session=True, preexec_fn=limits,
                                       env=dict(os.environ, TMPDIR=str(scratch),
                                                ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                                                LSAN_OPTIONS="detect_leaks=0"))
            groups.append(process.pid)
            while True:
                if log.stat().st_size > LOG_BUDGET or allocated(scratch) > SCRATCH_BUDGET:
                    raise ValueError("child log/private scratch exceeded bounded budget")
                if time.monotonic() >= deadline - 5:
                    raise TimeoutError("60-second whole budget exhausted")
                info = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                if info is not None:
                    break
                time.sleep(0.02)
        finally:
            if process is not None:
                handlers = {sig: signal.signal(sig, signal.SIG_IGN)
                            for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
                try:
                    reap_group(process, deadline)
                    groups.remove(process.pid)
                finally:
                    for sig, handler in handlers.items():
                        signal.signal(sig, handler)
    if log.stat().st_size > LOG_BUDGET or allocated(scratch) > SCRATCH_BUDGET:
        raise ValueError("completed command exceeded storage/output budget")
    return {"argv": argv, "exit": process.returncode, "seconds": round(time.monotonic() - started, 6),
            "leader_reaped": True, "owned_session_absent": True, "log": pin(log)}


def dependency_pins(depfile: Path) -> list[dict]:
    if depfile.stat().st_size > LOG_BUDGET:
        raise ValueError("compiler dependency file exceeded 1MiB")
    body = depfile.read_text().replace("\\\n", " ")
    if ": " not in body:
        raise ValueError("actual compiler depfile has no target separator")
    words = re.findall(r"(?:\\.|[^\s])+", body.split(": ", 1)[1])
    paths = {Path(re.sub(r"\\(.)", r"\1", word)).resolve(strict=True) for word in words}
    if len(paths) > 2048:
        raise ValueError("actual compiler dependency count exceeded 2048")
    return [pin(p) for p in sorted(paths) if p.is_file()]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vmem-source", type=Path, default=SOURCE)
    parser.add_argument("--cxx", default=os.environ.get("CXX", os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++")))
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    started = time.monotonic()
    deadline = started + 60
    scratch = None
    scratch_identity = None
    evidence = None
    groups = []
    watched = []
    before = []
    staged = []
    staged_before = []
    result = {"pass": False, "errors": [], "commands": [], "scratch_removed": False,
              "whole_budget_seconds": 60, "scratch_budget_bytes": SCRATCH_BUDGET,
              "command_log_budget_bytes": LOG_BUDGET, "core_dump_limit_bytes": 0,
              "compiler_external_dependencies_before": False,
              "compiler_external_dependencies_after_only": True,
              "native_runtime_qualified": False, "shared_coherence_qualified": False,
              "full_goal_complete": False}
    handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f"interrupted by signal {signum}")

    try:
        for sig in handlers:
            signal.signal(sig, interrupted)
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        selected = args.vmem_source.resolve(strict=True)
        compiler = Path(args.cxx).resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError("--cxx must be a single executable compiler path")
        with compiler.open("rb") as stream:
            if stream.read(4) != b"\x7fELF":
                raise ValueError("--cxx must resolve to a real Host ELF compiler")
        if args.evidence_dir is not None:
            evidence = args.evidence_dir.absolute()
            if evidence.parent.resolve(strict=True) != evidence.parent or os.path.lexists(evidence):
                raise ValueError("--evidence-dir needs an existing canonical parent and a fresh exclusive directory")
            evidence.mkdir(mode=0o700)
        watched = [selected, SMALLVEC, FAMILY_HEADER, NATIVE_HEADER, Path(__file__).resolve(), compiler]
        before = [pin(path) for path in watched]
        result["source_before"] = before
        result["selected_vmem_source"] = before[0]
        result["primitive_seams"] = ["Mutex", "File retain/destructive close", "PageTable identity", "nothrow array growth failure"]
        result["raw_registry_vectors_only"] = True
        result["native_registry_boundary"] = "actual pure types and empty native head; no native entries or worker modeled"
        subreaper()
        scratch = Path(tempfile.mkdtemp(prefix="wos-file-registry-", dir="/tmp"))
        info = scratch.stat()
        scratch_identity = (info.st_dev, info.st_ino, info.st_uid)
        if info.st_uid != os.getuid() or scratch.is_symlink():
            raise ValueError("fresh scratch ownership changed")
        result["scratch"] = str(scratch)
        result["actual_definitions"] = generate(scratch, selected)
        staged = sorted(path for path in scratch.rglob("*") if path.is_file())
        staged_before = [pin(path) for path in staged]
        result["staged_sources_before"] = staged_before
        binary = scratch / "file-registry-test"
        depfile = scratch / "dependencies.d"
        compile_argv = [str(compiler), "-std=c++23", "-O1", "-g0", "-pipe", "-fsanitize=address,undefined",
                        "-fno-omit-frame-pointer", "-MD", "-MF", str(depfile), "-I", str(scratch / "include"),
                        str(scratch / "harness.cpp"), "-o", str(binary)]
        for index, argv in enumerate([compile_argv, [str(binary)]]):
            log = scratch / f"command-{index}.log"
            row = command(argv, log, scratch, deadline, groups)
            result["commands"].append(row)
            text = log.read_text(errors="replace")
            if index == 0 and depfile.exists():
                result["emitted_dependency_pins_after"] = dependency_pins(depfile)
                result["dependency_file"] = pin(depfile)
                if row["exit"] == 0:
                    result["compiled_binary"] = pin(binary)
            if row["exit"] != 0:
                print(text[-32768:], end="", flush=True)
                raise ValueError(f"actual {'compile/link' if index == 0 else 'registry runtime'} failed with exit {row['exit']}")
            if index == 1:
                pattern = (r"^PASS cases=(\d+) assertions=(\d+) projection_failures=0 failures=0 "
                           r"growth_attempts=(\d+) successful_growth=(\d+) failed_growth=(\d+) "
                           r"retains=(\d+) puts=(\d+) final_closes=(\d+) real_registry=1 real_smallvec=1\n$")
                match = re.fullmatch(pattern, text)
                if match is None:
                    raise ValueError("real compiled registry lacks exact positive terminal oracle row")
                values = list(map(int, match.groups()))
                if values[0] < 500 or values[1] < 1_000_000 or values[4] != 7 or values[7] <= 0:
                    raise ValueError("actual regression coverage/growth/final-close census incomplete")
                result["runtime_census"] = dict(zip(["cases", "assertions", "growth_attempts", "successful_growth",
                                                    "failed_growth", "retains", "puts", "final_closes"], values))
                print(text, end="", flush=True)
        result["pass"] = True
    except (OSError, ValueError, TimeoutError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        for sig in handlers:
            signal.signal(sig, signal.SIG_IGN)
        try:
            if staged_before:
                result["staged_sources_after"] = [pin(path) for path in staged]
                result["staged_sources_held"] = result["staged_sources_after"] == staged_before
                if not result["staged_sources_held"]:
                    result["errors"].append("compiled staged source bytes changed")
            if before:
                result["source_after"] = [pin(path) for path in watched]
                result["source_held"] = result["source_after"] == before
                if not result["source_held"]:
                    result["errors"].append("selected actual sources/compiler changed")
            if scratch is not None:
                result["scratch_allocated_bytes_before_cleanup"] = allocated(scratch)
                result["owned_sessions_pending"] = list(groups)
                if evidence is not None:
                    saved = []
                    for path in [*staged, *sorted(scratch.glob("command-*.log")), scratch / "dependencies.d"]:
                        if not path.is_file():
                            continue
                        if path.stat().st_size > LOG_BUDGET:
                            raise ValueError("saved small evidence file exceeds1MiB")
                        destination = evidence / path.relative_to(scratch)
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        with destination.open("xb") as stream:
                            stream.write(path.read_bytes())
                        saved.append(pin(destination))
                    result["retained_small_evidence"] = saved
                info = scratch.stat()
                exact_owned = not scratch.is_symlink() and (info.st_dev, info.st_ino, info.st_uid) == scratch_identity
                if groups or not exact_owned:
                    result["errors"].append("owned session/scratch identity unproven; preserve small scratch")
                else:
                    shutil.rmtree(scratch)
                    result["scratch_removed"] = not os.path.lexists(scratch)
                    if not result["scratch_removed"]:
                        result["errors"].append("owned scratch remains after cleanup")
            result["elapsed_seconds"] = time.monotonic() - started
            if result["elapsed_seconds"] >= 60:
                result["errors"].append("whole60s actual cleanup/publication budget expired")
        except (OSError, ValueError, TimeoutError) as error:
            result["errors"].append(f"cleanup: {type(error).__name__}: {error}")
        finally:
            for sig, handler in handlers.items():
                signal.signal(sig, handler)
    result["pass"] = result["pass"] and not result["errors"] and result["scratch_removed"]
    result["qualification"] = "PASS_ACTUAL_HOST_FILE_REGISTRY_AND_SMALLVEC_ONLY" if result["pass"] else "FAIL"
    if evidence is not None:
        result["evidence_directory"] = str(evidence)
        with (evidence / "result.json").open("x") as stream:
            json.dump(result, stream, sort_keys=True, indent=2)
            stream.write("\n")
    print(json.dumps(result, sort_keys=True), flush=True)
    # Inner receipt capture precedes publication. This actual return check and
    # the external command exit must both succeed for qualification.
    return 0 if result["pass"] and time.monotonic() < deadline else 1


if __name__ == "__main__":
    raise SystemExit(main())
