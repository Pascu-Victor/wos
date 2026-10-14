#!/usr/bin/env python3
"""Compile actual lazy-range rewrite bodies against anchored projection oracles.

Uses complete current remove/add/protect/support definitions, the actual
LazyVmemRange declaration and actual SmallVec header. Task lock/admission and
File reference operations are explicit Host seams; real SmallVec nothrow growth
has a deterministic failure boundary. This checks single-Task metadata/ref
transactions and cold byte offsets, not native PTEs, backing I/O or shared-root
multi-Task atomicity. --vmem-source accepts a complete unmodified predecessor:
The actual pure ownership header supplies the native union declaration only;
every vector remains explicit RAW FILE_BACKED/ANONYMOUS, with no native worker
or lifetime qualification from this driver.
it must compile and fail the independent projection oracle. --only isolates
remove/add/protect negative controls. Never substitutes a rewritten algorithm.
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
            if (path / "modules/kern/src").is_dir() and (path / ".github/instructions").is_dir())
SOURCE = ROOT / "modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp"
TASK = ROOT / "modules/kern/src/platform/sched/task.hpp"
SMALLVEC = ROOT / "modules/kern/src/util/smallvec.hpp"
FAMILY_HEADER = ROOT / "modules/kern/src/platform/mm/file_mapping_lifetime.hpp"

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <sys/types.h>
#include <utility>
#include <vector>
namespace fixture {
unsigned failures{}, assertions{}, cases{}, projection_failures{}, retains{}, puts{}, lock_depth{};
int fail_growth_after{-1};
unsigned attempted_growth{};
void check(bool value, const char* message) {
    ++assertions;
    if (!value) { if (failures++ < 16) std::fprintf(stderr, "FAIL: %s\n", message); }
}
void projection(bool value) {
    if (!value) ++projection_failures;
    check(value, "original anchored file-byte projection changed");
}
struct Lock {
    bool held{};
    uint64_t lock_irqsave() { check(!held, "recursive lazy lock"); held=true; ++lock_depth; return 0; }
    void unlock_irqrestore(uint64_t) { check(held && lock_depth != 0, "unbalanced lazy lock"); held=false; --lock_depth; }
};
}
// The actual SmallVec calls this standard allocation boundary only for growth.
// Ordinary allocations (including oracle vectors) remain unaffected.
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
    ++fixture::attempted_growth;
    if (fixture::fail_growth_after == 0) return nullptr;
    if (fixture::fail_growth_after > 0) --fixture::fail_growth_after;
    try { return ::operator new[](size); } catch (...) { return nullptr; }
}
#include <util/smallvec.hpp>
#include <platform/mm/file_mapping_lifetime.hpp>
namespace ker::vfs {
struct File { std::atomic<int> refcount{1}; };
void vfs_retain_file(File* file) {
    fixture::check(file != nullptr && file->refcount.load() > 0, "retain without File owner");
    ++fixture::retains; ++file->refcount;
}
void vfs_put_file(File* file) {
    fixture::check(fixture::lock_depth == 0, "File put under lazy lock");
    fixture::check(file != nullptr && file->refcount.fetch_sub(1) > 1, "File ownership underflow/lost fixture baseline");
    ++fixture::puts;
}
}
'''

CASES = r'''
namespace fixture {
using Range = ker::mod::sched::task::LazyVmemRange;
using Kind = ker::mod::sched::task::LazyVmemKind;
using Task = ker::mod::sched::task::Task;
namespace actual = ker::syscall::vmem;
constexpr uint64_t BASE = 0x10000;
constexpr unsigned SPAN = 128;
bool same(const Range& a, const Range& b) {
    return a.start==b.start && a.end==b.end && a.prot==b.prot && a.flags==b.flags && a.kind==b.kind &&
        a.file==b.file && a.file_offset==b.file_offset && a.file_dev==b.file_dev && a.file_ino==b.file_ino &&
        a.file_size==b.file_size && a.file_mtime_sec==b.file_mtime_sec && a.file_mtime_nsec==b.file_mtime_nsec &&
        a.file_ctime_sec==b.file_ctime_sec && a.file_ctime_nsec==b.file_ctime_nsec;
}
struct Cell { int origin{-1}; uint64_t prot{}; };
struct World {
    Task task;
    std::array<ker::vfs::File,4> files;
    std::vector<Range> origins;
    std::array<Cell,SPAN> expected{};
    unsigned retains_at_start{retains}, puts_at_start{puts};
    int origin(unsigned begin, unsigned end, uint64_t offset, bool anonymous=false, bool null_file=false) {
        check(begin < end && end <= SPAN, "fixture origin bounds");
        unsigned id=origins.size();
        Range r{}; r.start=BASE+begin; r.end=BASE+end; r.prot=3; r.flags=0x100+id;
        r.kind=anonymous ? Kind::ANONYMOUS : Kind::FILE_BACKED;
        r.file=null_file ? nullptr : &files.at(id % files.size());
        r.file_offset=offset; r.file_dev=100+id; r.file_ino=200+id; r.file_size=UINT64_MAX;
        r.file_mtime_sec=300+id; r.file_mtime_nsec=400+id; r.file_ctime_sec=500+id; r.file_ctime_nsec=600+id;
        origins.push_back(r); return id;
    }
    void paint(int id) {
        const auto& r=origins.at(id);
        for (unsigned x=r.start-BASE; x<r.end-BASE; ++x) expected.at(x)={id,r.prot};
    }
    void seed(int id) {
        auto const& r=origins.at(id);
        check(task.lazy_vmem_ranges.push_back(r), "fixture seed growth failed");
        actual::retain_lazy_vmem_range_file(r); paint(id);
    }
    std::vector<Range> snapshot() const {
        std::vector<Range> out;
        for (auto const& r:task.lazy_vmem_ranges) out.push_back(r);
        return out;
    }
    void unchanged(const std::vector<Range>& before, const std::array<int,4>& refs) {
        check(before.size()==task.lazy_vmem_ranges.size(), "failed transaction changed row count");
        for (size_t i=0; i<std::min(before.size(),task.lazy_vmem_ranges.size()); ++i)
            check(same(before.at(i),task.lazy_vmem_ranges.at(i)), "failed transaction changed metadata");
        for (unsigned i=0;i<files.size();++i) check(files.at(i).refcount.load()==refs.at(i), "failed transaction changed File refs");
        check(lock_depth==0 && !task.lazy_vmem_lock.held, "failed transaction leaked lock");
    }
    std::array<int,4> refs() const {
        std::array<int,4> out{}; for(unsigned i=0;i<files.size();++i) out.at(i)=files.at(i).refcount.load(); return out;
    }
    void verify() {
        ++cases;
        std::array<unsigned,SPAN> visits{};
        std::array<int,4> owners{};
        for(auto const& r:task.lazy_vmem_ranges) {
            check(r.start>=BASE && r.start<r.end && r.end<=BASE+SPAN, "published fragment bounds");
            if(r.kind==Kind::FILE_BACKED && r.file) {
                bool owned=false;
                for(unsigned i=0;i<files.size();++i) if(r.file==&files.at(i)) { ++owners.at(i); owned=true; }
                check(owned,"unknown published File");
            }
            for(uint64_t va=r.start;va<r.end && va<BASE+SPAN;++va) {
                unsigned x=va-BASE; ++visits.at(x); const auto& cell=expected.at(x);
                check(cell.origin>=0,"unexpected mapped byte"); if(cell.origin<0) continue;
                const auto& anchor=origins.at(cell.origin);
                check(r.kind==anchor.kind && r.file==anchor.file && r.flags==anchor.flags && r.prot==cell.prot,
                      "fragment identity/permission changed");
                check(r.file_dev==anchor.file_dev && r.file_ino==anchor.file_ino && r.file_size==anchor.file_size &&
                      r.file_mtime_sec==anchor.file_mtime_sec && r.file_mtime_nsec==anchor.file_mtime_nsec &&
                      r.file_ctime_sec==anchor.file_ctime_sec && r.file_ctime_nsec==anchor.file_ctime_nsec,
                      "fragment stat metadata changed");
                if(r.kind==Kind::FILE_BACKED) {
                    __uint128_t const wanted=__uint128_t(anchor.file_offset)+(va-anchor.start);
                    __uint128_t const got=__uint128_t(r.file_offset)+(va-r.start);
                    projection(got==wanted && got<=UINT64_MAX);
                    // Controlled cold backing byte. Neither offset comes from
                    // a previous fragment or the implementation's slice helper.
                    auto cold=[](__uint128_t n) { return uint8_t((n ^ (n>>7) ^ (n>>15) ^ (n>>32)) & 255); };
                    check(cold(got)==cold(wanted),"cold content byte came from wrong anchored offset");
                } else check(r.file_offset==anchor.file_offset,"anonymous offset metadata was rebased");
            }
        }
        for(unsigned x=0;x<SPAN;++x) check(visits.at(x)==(expected.at(x).origin>=0 ? 1U : 0U),"coverage/overlap changed");
        int row_owners=0;
        for(unsigned i=0;i<files.size();++i) { row_owners+=owners.at(i); check(files.at(i).refcount.load()==1+owners.at(i),"File ref baseline differs from published rows"); }
        check(int(retains-retains_at_start)-int(puts-puts_at_start)==row_owners,"retained/released prefix balance");
        check(lock_depth==0 && !task.lazy_vmem_lock.held,"transaction lock leaked");
    }
    bool call(unsigned op, unsigned begin, unsigned end, int id=-1, uint64_t prot=5) {
        if(op==0) return actual::remove_lazy_vmem_range(&task,BASE+begin,end-begin);
        if(op==1) return actual::add_lazy_vmem_range_entry(&task,origins.at(id));
        return actual::protect_lazy_vmem_range(&task,BASE+begin,end-begin,prot);
    }
    void expect(unsigned op,unsigned begin,unsigned end,int id=-1,uint64_t prot=5) {
        if(op==1) { paint(id); return; }
        for(unsigned x=begin;x<end;++x) if(op==0) expected.at(x)={}; else if(expected.at(x).origin>=0) expected.at(x).prot=prot;
    }
    ~World() {
        fail_growth_after=-1;
        actual::release_lazy_vmem_range_files(task.lazy_vmem_ranges); task.lazy_vmem_ranges.clear();
        for(auto const& f:files) check(f.refcount.load()==1,"File baseline leaked after fixture retirement");
        check(retains-retains_at_start==puts-puts_at_start,"fixture retain/put balance");
    }
};
void geometry(unsigned op) {
    for(bool anon:{false,true}) for(bool null_file:{false,true})
        for(auto bounds:std::array<std::array<unsigned,2>,9>{{{0,8},{8,12},{12,20},{20,28},{28,40},{8,40},{0,40},{14,15},{0,128}}}) {
            World w; int a=w.origin(8,40,0x187c,anon,null_file); w.seed(a);
            int b=w.origin(bounds[0],bounds[1],0x9321,anon,null_file);
            check(w.call(op,bounds[0],bounds[1],b),"ordinary rewrite refused");
            w.expect(op,bounds[0],bounds[1],b); w.verify();
        }
    World w; int a=w.origin(0,128,UINT64_MAX-127); w.seed(a);
    int b=w.origin(17,91,0x1000);
    check(w.call(op,17,91,b),"representable UINT64_MAX boundary refused");
    w.expect(op,17,91,b); w.verify();
}
void sequence() {
    World w; w.seed(w.origin(0,120,0x7777)); w.verify();
    check(w.call(2,16,91,-1,0),"sequence PROT_NONE refused");w.expect(2,16,91,-1,0);w.verify();
    check(w.call(2,29,62,-1,7),"sequence promotion refused");w.expect(2,29,62,-1,7);w.verify();
    int b=w.origin(34,53,0xa113);check(w.call(1,34,53,b),"sequence replacement refused");w.expect(1,34,53,b);w.verify();
    check(w.call(0,7,31),"sequence unmap refused");w.expect(0,7,31);w.verify();
    int c=w.origin(0,18,UINT64_MAX,true);check(w.call(1,0,18,c),"sequence anonymous replacement refused");w.expect(1,0,18,c);w.verify();
    check(w.call(2,0,120,-1,1),"sequence final protection refused");w.expect(2,0,120,-1,1);w.verify();
    check(w.call(0,0,128),"sequence full retirement refused");w.expect(0,0,128);w.verify();
}
void rollback(unsigned op) {
    // Overflow occurs only after earlier ranges/left pieces have been staged.
    for(bool null_file:{false,true}) {
        World w; w.seed(w.origin(0,8,123)); w.seed(w.origin(16,48,UINT64_MAX-3,false,null_file));
        int b=w.origin(21,24,333);auto before=w.snapshot();auto refs=w.refs();
        check(!w.call(op,21,24,b),"overflow rewrite must refuse before publication");
        w.unchanged(before,refs); ++cases;
    }
    for(int fail:{0,1,2}) {
        World w;for(unsigned i=0;i<34;++i)w.seed(w.origin(i*3,i*3+2,1000+i*31));
        int b=w.origin(49,50,9000);auto before=w.snapshot();auto refs=w.refs();unsigned growth=attempted_growth;
        fail_growth_after=fail;bool ok=w.call(op,49,50,b);fail_growth_after=-1;
        check(!ok && attempted_growth>growth,"injected actual SmallVec growth must refuse");
        w.unchanged(before,refs);w.verify();
        check(w.call(op,49,50,b),"retry after failed preparation refused");w.expect(op,49,50,b);w.verify();
    }
    World w;w.seed(w.origin(8,40,111));auto before=w.snapshot();auto refs=w.refs();
    if(op==1) {
        int b=w.origin(16,24,222);w.task.usercopy_pagemap_closing.store(true);
        check(w.call(op,16,24,b),"closing add preserves its existing no-op success");w.unchanged(before,refs);
        Range bad{};check(!actual::add_lazy_vmem_range_entry(nullptr,bad),"null add must refuse");
        check(!actual::add_lazy_vmem_range_entry(&w.task,bad),"empty add must refuse");
    } else {
        bool r=op==0 ? actual::remove_lazy_vmem_range(nullptr,0,0) : actual::protect_lazy_vmem_range(nullptr,0,0,1);
        check(r,"legacy null no-op changed");
        r=op==0 ? actual::remove_lazy_vmem_range(&w.task,UINT64_MAX-3,8) : actual::protect_lazy_vmem_range(&w.task,UINT64_MAX-3,8,1);
        check(r,"legacy VA-overflow no-op changed");w.unchanged(before,refs);
    }
    w.verify();
}
void helper_boundaries() {
#if HAVE_SLICE_HELPER
    Range input{};input.start=20;input.end=40;input.file_offset=UINT64_MAX-2;input.kind=Kind::FILE_BACKED;
    Range sentinel{};sentinel.start=777;sentinel.end=888;sentinel.prot=999;Range out=sentinel;
    for(auto bounds:std::array<std::array<uint64_t,2>,5>{{{19,30},{20,20},{25,24},{20,41},{23,40}}}) {
        check(!actual::slice_lazy_vmem_range(input,bounds[0],bounds[1],out),"invalid helper slice accepted");
        check(same(out,sentinel),"refused helper slice changed output");++cases;
    }
    check(actual::slice_lazy_vmem_range(input,22,40,out) && out.file_offset==UINT64_MAX,"exact offset boundary refused");++cases;
    input.kind=Kind::ANONYMOUS;
    check(actual::slice_lazy_vmem_range(input,39,40,out) && out.file_offset==input.file_offset,"anonymous helper offset changed");++cases;
#endif
#if HAVE_OFFSET_SPAN_HELPER
    static_assert(sizeof(off_t)==8 && std::numeric_limits<off_t>::is_signed);
    constexpr uint64_t limit=INT64_MAX;
    for(uint64_t offset:std::array<uint64_t,10>{0,1,13,4095,limit-4096,limit-100,limit-1,limit,limit+1,UINT64_MAX})
        for(uint64_t size:std::array<uint64_t,10>{0,1,2,13,100,101,102,4096,4097,UINT64_MAX}) {
            bool const expected=size!=0 && __uint128_t(offset)+size-1<=__uint128_t(limit);
            check(actual::file_mapping_offset_range_valid(offset,size)==expected,"signed complete offset-span admission changed");
            ++cases;
        }
#endif
}
}
int main(int argc,char** argv) {
    const char* mode=argc==2 ? argv[1] : "all";
    for(unsigned op=0;op<3;++op) {
        const char* name=op==0 ? "remove" : op==1 ? "add" : "protect";
        if(std::strcmp(mode,"all")==0 || std::strcmp(mode,name)==0) { fixture::geometry(op);fixture::rollback(op); }
    }
    if(std::strcmp(mode,"all")==0) { fixture::sequence();fixture::helper_boundaries(); }
    fixture::check(fixture::retains==fixture::puts,"suite File ownership balance");
    std::printf("%s cases=%u assertions=%u retains=%u puts=%u projection_failures=%u failures=%u actual_rewrites=3 actual_smallvec=1\n",
        fixture::failures==0 ? "PASS" : "FAIL",fixture::cases,fixture::assertions,fixture::retains,fixture::puts,
        fixture::projection_failures,fixture::failures);
    return fixture::failures==0 ? 0 : 1;
}
'''

def pin(path: Path) -> dict:
    data = path.read_bytes()
    return {"path": str(path.resolve()), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def capture(source: str, name: str) -> tuple[str, dict]:
    """Extract complete actual definition, preserving every source byte."""
    clean = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                   lambda match: " " * len(match[0]), source)
    matches = list(re.finditer(rf"^(?:auto|void|bool) {re.escape(name)}\([^{{;]*\{{", clean, re.M))
    if len(matches) != 1:
        raise ValueError(f"expected one actual definition for {name}, found {len(matches)}")
    start = matches[0].start()
    opening = clean.index("{", start)
    depth = 0
    for end in range(opening, len(clean)):
        if clean[end] == "{": depth += 1
        elif clean[end] == "}":
            depth -= 1
            if depth == 0:
                body = source[start:end + 1]
                return body, {"name": name, "line": source.count("\n", 0, start) + 1,
                              "bytes": len(body.encode()), "sha256": hashlib.sha256(body.encode()).hexdigest()}
    raise ValueError(f"unterminated actual definition: {name}")


def generate(source: str, task: str, source_path: Path) -> tuple[str, list[dict]]:
    begin = task.index("enum class LazyVmemKind")
    end = task.index("enum class TaskType", begin)
    types = task[begin:end]
    definitions = [{"name": "actual-LazyVmemKind-Range-Vec", "bytes": len(types.encode()),
                    "sha256": hashlib.sha256(types.encode()).hexdigest()},
                   {"name": "whole-real-file_mapping_lifetime.hpp", **pin(FAMILY_HEADER)}]
    names = ["ranges_overlap", "push_lazy_vmem_range", "lazy_vmem_range_has_file", "retain_lazy_vmem_range_file",
             "release_lazy_vmem_range_file", "release_lazy_vmem_range_files", "push_lazy_vmem_range_copy"]
    # Old source genuinely has no slice helper. Only extraction availability is
    # tested here; correctness is always the compiled byte-projection oracle.
    helper = bool(re.search(r"^auto slice_lazy_vmem_range\(", source, re.M))
    if helper: names.append("slice_lazy_vmem_range")
    span_helper = bool(re.search(r"^auto file_mapping_offset_range_valid\(", source, re.M))
    if span_helper: names.append("file_mapping_offset_range_valid")
    names += ["remove_lazy_vmem_range", "add_lazy_vmem_range_entry", "protect_lazy_vmem_range"]
    bodies = []
    for name in names:
        body, row = capture(source, name); definitions.append(row)
        bodies.append(f'#line {row["line"]} {json.dumps(str(source_path))}\n{body}')
    prefix = PRELUDE + "\nnamespace ker::mod::sched::task {\n" + types + r'''
struct Task {
    fixture::Lock lazy_vmem_lock;
    LazyVmemRangeVec lazy_vmem_ranges;
    std::atomic<bool> usercopy_pagemap_closing{};
};
}
namespace ker::syscall::vmem {
using LazyVmemRange=ker::mod::sched::task::LazyVmemRange;
using LazyVmemKind=ker::mod::sched::task::LazyVmemKind;
using LazyVmemRangeVec=ker::mod::sched::task::LazyVmemRangeVec;
'''
    flags = f"#define HAVE_SLICE_HELPER {int(helper)}\n#define HAVE_OFFSET_SPAN_HELPER {int(span_helper)}\n"
    return prefix + "\n".join(bodies) + "\n}\n" + flags + CASES, definitions


SCRATCH_BUDGET = 64 * 1024 * 1024
LOG_BUDGET = 1024 * 1024
FILE_BUDGET = 8 * 1024 * 1024

# Seven complete bounded runner functions copied byte-for-byte from the installed
# actual-body close-order regression. No helper imports.

def allocated(path: Path) -> int:
    return sum(p.stat().st_blocks * 512 for p in path.rglob("*") if p.is_file())


def subreaper() -> None:
    # No privilege is required. Compiler grandchildren remain ours to reap if
    # their parent exits during a timeout. Never wait for another process group.
    if not sys.platform.startswith("linux"):
        raise ValueError("the bounded runner requires Linux child subreaping")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
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
        # Same session includes children that changed their process group.
        # Sequential execution creates no other children in this driver; a
        # reparented direct child is therefore an owned compiler descendant.
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
    # Keep the leader unreaped until its session and adopted descendants are
    # inactive. pidfds plus start times prevent PID reuse from changing targets.
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
                pass  # Still under its dying parent; subreaper will adopt it.
        remaining = owned_session(process.pid)
        if all(pid == process.pid and state == "Z" for pid, _, state, _, _ in remaining):
            process.wait(timeout=max(0.01, deadline - time.monotonic()))
            return  # Retained leader was the final owned session member.
        if time.monotonic() >= deadline:
            raise TimeoutError("owned session remains live; preserve its scratch")
        time.sleep(0.01)


def command(argv: list[str], log: Path, scratch: Path, deadline: float, groups: list[int]) -> dict:
    if time.monotonic() >= deadline - 5:
        raise TimeoutError("120-second whole-test budget exhausted before command")

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
                if log.stat().st_size > LOG_BUDGET:
                    raise ValueError("command log exceeded 1 MiB")
                if allocated(scratch) > SCRATCH_BUDGET:
                    raise ValueError("private scratch exceeded 64 MiB")
                if time.monotonic() >= deadline - 5:
                    raise TimeoutError("120-second whole-test budget exhausted")
                # WNOWAIT deliberately leaves the owned leader's PID reserved.
                info = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                if info is not None:
                    break
                time.sleep(0.02)
        finally:
            if process is not None:
                handlers = {signum: signal.signal(signum, signal.SIG_IGN)
                            for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
                try:
                    reap_group(process, deadline)
                    groups.remove(process.pid)
                finally:
                    for signum, handler in handlers.items():
                        signal.signal(signum, handler)
    if log.stat().st_size > LOG_BUDGET or allocated(scratch) > SCRATCH_BUDGET:
        raise ValueError("completed command exceeded storage/output budget")
    return {"argv": argv, "exit": process.returncode, "seconds": round(time.monotonic() - started, 3),
            "owned_group_reaped": True, "log_bytes": log.stat().st_size}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vmem-source", type=Path, default=SOURCE)
    parser.add_argument("--task-header", type=Path, default=TASK)
    parser.add_argument("--only", choices=["all", "remove", "add", "protect"], default="all")
    parser.add_argument("--cxx", default=os.environ.get("CXX", os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++")))
    args = parser.parse_args()
    started = time.monotonic(); deadline = started + 120
    scratch = None; groups = []; watched = []; before = []
    result = {"pass": False, "commands": [], "scratch_removed": False, "only": args.only,
              "raw_lazy_vectors_only": True, "native_runtime_qualified": False}
    try:
        source_path = args.vmem_source.resolve(strict=True); task_path = args.task_header.resolve(strict=True)
        compiler = Path(args.cxx).resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError("--cxx/CXX must name one host ELF compiler executable")
        with compiler.open("rb") as executable:
            if executable.read(4) != b"\x7fELF": raise ValueError("--cxx/CXX must name one host ELF compiler executable")
        watched = [source_path, task_path, SMALLVEC, FAMILY_HEADER, Path(__file__).resolve(), compiler]
        before = [pin(path) for path in watched]; result["source"] = before[0]
        harness, actual = generate(source_path.read_text(), task_path.read_text(), source_path)
        result["actual_definitions"] = actual
        subreaper()
        scratch = Path(tempfile.mkdtemp(prefix="wos-lazy-offset-", dir="/tmp")); result["scratch"] = str(scratch)
        shim = scratch / "shim"
        headers = {"platform/mm/dyn/kmalloc.hpp": "#pragma once\n",
                   "util/hcf.hpp": "#pragma once\n#include <cstdlib>\nnamespace ker::util { [[noreturn]] inline void hcf() { std::abort(); } }\n"}
        for name, contents in headers.items():
            path = shim / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_text(contents)
        src = scratch / "offset.cpp"; binary = scratch / "offset-test"; src.write_text(harness)
        commands = [[str(compiler), "-std=c++23", "-O1", "-g0", "-pipe", "-fsanitize=address,undefined",
                     "-fno-omit-frame-pointer", "-I", str(shim), "-I", str(ROOT / "modules/kern/src"), str(src), "-o", str(binary)],
                    [str(binary), args.only]]
        for index, argv in enumerate(commands):
            log_path = scratch / f"command-{index}.log"
            row = command(argv, log_path, scratch, deadline, groups); result["commands"].append(row)
            log = log_path.read_text(errors="replace")
            if index == 1 or row["exit"] != 0: print(log[-32768:], end="")
            if row["exit"] != 0:
                raise ValueError(f"actual-source {'compile/link' if index == 0 else 'runtime oracle'} failed with exit {row['exit']}")
            if index == 1 and not re.search(r"^PASS cases=[1-9]\d* assertions=[1-9]\d* retains=\d+ puts=\d+ projection_failures=0 failures=0 actual_rewrites=3 actual_smallvec=1$", log, re.M):
                raise ValueError("compiled suite terminal PASS row is missing")
        result["pass"] = True
    except (OSError, ValueError, TimeoutError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        if scratch is not None and not groups:
            shutil.rmtree(scratch); result["scratch_removed"] = not scratch.exists()
        if groups:
            result["pass"] = False; result["cleanup_error"] = f"owned groups {groups} could not be reaped; scratch preserved"
        if before:
            try: result["sources_held"] = [pin(path) for path in watched] == before
            except OSError: result["sources_held"] = False
            if not result["sources_held"]:
                result["pass"] = False; result["source_error"] = "selected source/header/driver/compiler changed during execution"
        if time.monotonic() > deadline:
            result["pass"] = False; result["deadline_error"] = "whole invocation exceeded 120 seconds including cleanup"
        result["seconds"] = round(time.monotonic() - started, 3)
        print(json.dumps({key: value for key, value in result.items() if key != "actual_definitions"}))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")
    for signum in (signal.SIGTERM, signal.SIGHUP): signal.signal(signum, interrupted)
    raise SystemExit(main())
