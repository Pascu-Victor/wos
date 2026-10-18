#!/usr/bin/env python3
"""Actual checked huge vacancy queries with finite page/Task/lock ports.

No real current-Task/PUB capability, physical placement, PS publication, Native,
CPL3, concurrency or runtime qualification is provided by these Host ports.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

from caller_pin_order_runtime_test import function

ROOT = next(p for p in Path(__file__).resolve().parents
            if (p / "modules/kern/src").is_dir() and (p / "tests/host").is_dir())
VMEM = ROOT / "modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp"

PRELUDE = r'''
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <platform/mm/paging.hpp>
#include <platform/mm/managed_huge_core.hpp>
constexpr uint64_t POISON=0x000ffffffffff000ULL;
size_t translations=0,poison_translations=0;
namespace ker::mod::mm::addr {
auto get_virt_pointer(uint64_t p)->void*{
 ++translations;if(p==POISON){++poison_translations;assert(false);}return reinterpret_cast<void*>(p);
}
}
struct FiniteLazyLock {
 bool held=false;unsigned acquired=0,released=0;
 uint64_t lock_irqsave(){assert(!held);held=true;++acquired;return 0;}
 void unlock_irqrestore(uint64_t flags){assert(held&&flags==0);held=false;++released;}
};
namespace ker::mod::sched::task {
struct LazyVmemRange {uint64_t start=0,end=0;};
struct Task {
 ker::mod::mm::paging::PageTable* pagemap=nullptr;
 std::vector<LazyVmemRange> lazy_vmem_ranges;
 FiniteLazyLock lazy_vmem_lock;
 std::atomic<uint64_t> mmap_next{0};
};
}
namespace test {
using ker::mod::mm::paging::PageTable;using ker::mod::mm::paging::PageTableEntry;
using Task=ker::mod::sched::task::Task;
CONSTANTS
TYPES
FUNCTIONS
std::vector<PageTable*>tables;
auto raw(uint64_t word)->PageTableEntry{PageTableEntry e;std::memcpy(&e,&word,8);return e;}
auto table()->PageTable*{
 auto*t=static_cast<PageTable*>(std::aligned_alloc(4096,4096));assert(t);std::memset(t,0,4096);tables.push_back(t);return t;
}
void put(PageTable*root,uint64_t va,uint64_t word,unsigned leaf=1){
 auto*t=root;
 for(unsigned level=4;level>leaf;--level){
  auto&e=t->entries[(va>>(12+9*(level-1)))&511];
  if(!e.present)e=raw(reinterpret_cast<uint64_t>(table())|7);
  t=reinterpret_cast<PageTable*>(uint64_t(e.frame)<<12);
 }
 t->entries[(va>>(12+9*(leaf-1)))&511]=raw(word);
}
// Hash only fixture data, not lock-observation counters. Query allocation or
// page/metadata/cursor writes cannot hide behind the expected result.
auto digest(const Task&t)->uint64_t{
 uint64_t h=14695981039346656037ULL;
 auto add=[&](uint64_t value){h^=value;h*=1099511628211ULL;};
 add(reinterpret_cast<uint64_t>(t.pagemap));add(t.mmap_next.load());
 add(t.lazy_vmem_ranges.size());add(t.lazy_vmem_ranges.capacity());add(reinterpret_cast<uint64_t>(t.lazy_vmem_ranges.data()));
 for(auto const&r:t.lazy_vmem_ranges){add(r.start);add(r.end);}add(tables.size());
 for(auto*p:tables){auto const*b=reinterpret_cast<const unsigned char*>(p);for(size_t i=0;i<4096;++i)add(b[i]);}
 return h;
}
unsigned cases=0;
void expect(Task&t,uint64_t length,uint64_t hint,HugeVacancyStatus status,uint64_t address=0){
 auto before=digest(t);auto result=find_huge_vacancy(&t,length,hint);++cases;
 assert(result.status==status&&result.address==address);
 assert(digest(t)==before&&!t.lazy_vmem_lock.held&&t.lazy_vmem_lock.acquired==t.lazy_vmem_lock.released);
}
void clipped(Task&t,uint64_t start,uint64_t end,uint64_t low,uint64_t high,uint64_t resume){
 auto before=digest(t);auto r=checked_huge_occupied_range(&t,start,end);++cases;
 assert(r.valid&&r.range.found&&r.range.start==low&&r.range.end==high&&r.resume_after==resume);
 assert(digest(t)==before&&!t.lazy_vmem_lock.held&&t.lazy_vmem_lock.acquired==t.lazy_vmem_lock.released);
}
int run(){
 constexpr uint64_t A=2ULL<<20,B=MMAP_START,P=4096,LIMIT=1ULL<<47;
 constexpr uint64_t LAST=(USER_SPACE_END-A)&~(A-1);
 static_assert(LAST==0x7fffffc00000ULL); // Preserve the old conservative end bound.
 Task empty;empty.pagemap=table();empty.mmap_next.store(B+P);
 expect(empty,A,0,HugeVacancyStatus::Found,B+A);
 expect(empty,A,B+3*P,HugeVacancyStatus::Found,B+A);
 expect(empty,A,USER_SPACE_START,HugeVacancyStatus::Found,USER_SPACE_START);
 expect(empty,64*A,0,HugeVacancyStatus::Found,B+A);
 expect(empty,A,UINT64_MAX,HugeVacancyStatus::Found,B+A);
 expect(empty,A,USER_SPACE_START-1,HugeVacancyStatus::Found,B+A);
 expect(empty,A,LAST,HugeVacancyStatus::Found,LAST);
 expect(empty,A,LAST+1,HugeVacancyStatus::Found,B+A);
 empty.mmap_next.store(LAST+P);expect(empty,A,0,HugeVacancyStatus::Found,B);
 empty.mmap_next.store(UINT64_MAX);expect(empty,A,0,HugeVacancyStatus::Found,B);
 for(uint64_t length:{uint64_t{0},P,A+P,65*A,UINT64_MAX})expect(empty,length,0,HugeVacancyStatus::Invalid);
 auto invalid=find_huge_vacancy(nullptr,A,0);assert(invalid.status==HugeVacancyStatus::Invalid&&invalid.address==0);++cases;
 Task missing;expect(missing,A,0,HugeVacancyStatus::Invalid);
 uint64_t aligned=123;assert(!align_huge_mmap_up(UINT64_MAX,aligned)&&aligned==0);++cases;
 assert(align_huge_mmap_up(B+P,aligned)&&aligned==B+A);++cases;
 auto beyond=find_huge_vacancy_from(&empty,A,LAST+P);
 assert(beyond.status==HugeVacancyStatus::Exhausted&&beyond.address==0);++cases;
 auto bad_start=find_huge_vacancy_from(&empty,A,USER_SPACE_START-1);
 assert(bad_start.status==HugeVacancyStatus::Invalid&&bad_start.address==0);++cases;
 // A poisoned PRESENT PML4 PS refuses before the identity-HHDM port is called.
 Task poison;poison.pagemap=table();poison.mmap_next.store(B);
 poison.pagemap->entries[(B>>39)&511]=raw(POISON|1|(1ULL<<7));
 translations=0;expect(poison,A,0,HugeVacancyStatus::Invalid);assert(!translations&&!poison_translations);
 auto bad=checked_huge_occupied_range(&poison,B,B+A);
 assert(!bad.valid&&!bad.range.found&&bad.resume_after==0&&!translations);++cases;
 // Nonpresent software markers have no intermediate occupancy; upper-half PS is outside the query.
 poison.pagemap->entries[(B>>39)&511]=raw(POISON|(1ULL<<11)|(1ULL<<7));
 poison.pagemap->entries[256]=raw(POISON|1|(1ULL<<7));
 translations=0;expect(poison,A,0,HugeVacancyStatus::Found,B);assert(!translations&&!poison_translations);
 for(unsigned level:{3U,2U}){
  Task marker;marker.pagemap=table();marker.mmap_next.store(B);put(marker.pagemap,B,POISON|(1ULL<<11)|(1ULL<<7),level);
  translations=0;expect(marker,A,0,HugeVacancyStatus::Found,B);assert(translations==4-level&&!poison_translations);
 }
 // PRESENT/RESERVED/combined4KiB leaves all block an aligned candidate.
 for(uint64_t flags:{uint64_t{1},uint64_t{1}<<11,uint64_t{1}|(uint64_t{1}<<11)}){
  Task small;small.pagemap=table();small.mmap_next.store(B);put(small.pagemap,B+P,flags);
  expect(small,A,0,HugeVacancyStatus::Found,B+A);
  clipped(small,B,B+3*P,B+P,B+2*P,B+2*P);
 }
 // Full huge-span resume skips the entire1GiB/2MiB owner, while query output clips.
 Task one_g;one_g.pagemap=table();one_g.mmap_next.store(B);put(one_g.pagemap,B,1|(1ULL<<7),3);
 clipped(one_g,B+P,B+3*P,B+P,B+3*P,B+(1ULL<<30));
 translations=0;expect(one_g,A,0,HugeVacancyStatus::Found,B+(1ULL<<30));assert(translations==2);
 Task two_m;two_m.pagemap=table();two_m.mmap_next.store(B);put(two_m.pagemap,B,1|(1ULL<<7),2);
 clipped(two_m,B+P,B+3*P,B+P,B+3*P,B+A);
 translations=0;expect(two_m,A,0,HugeVacancyStatus::Found,B+A);assert(translations==4);
 Task combined;combined.pagemap=table();combined.mmap_next.store(B);put(combined.pagemap,B+P,1);
 combined.lazy_vmem_ranges={{B+A,B+3*A}};
 expect(combined,A,0,HugeVacancyStatus::Found,B+3*A);
 // Unsorted lazy rows and a full-row-end jump do not reserve or edit anything.
 Task lazy;lazy.pagemap=table();lazy.mmap_next.store(B);
 lazy.lazy_vmem_ranges={{B+A,B+4*A},{B+P,B+2*A},{B+6*A,B+6*A}};
 expect(lazy,A,0,HugeVacancyStatus::Found,B+4*A);
 clipped(lazy,B,B+A,B+P,B+A,B+2*A);
 Task wrap;wrap.pagemap=table();wrap.mmap_next.store(B+9*A);wrap.lazy_vmem_ranges={{B+8*A,LIMIT}};
 expect(wrap,A,0,HugeVacancyStatus::Found,B);
 expect(wrap,A,B+9*A,HugeVacancyStatus::Found,B);
 Task full;full.pagemap=table();full.mmap_next.store(B+A);full.lazy_vmem_ranges={{B,LIMIT}};
 expect(full,A,0,HugeVacancyStatus::Exhausted);
 assert(full.lazy_vmem_lock.acquired==2); // Two complete scans, each skips the one full row.
 for(auto row:std::vector<ker::mod::sched::task::LazyVmemRange>{{B+P,B},{B+1,B+A},{B,LIMIT+P}}){
  Task malformed;malformed.pagemap=table();malformed.mmap_next.store(B);malformed.lazy_vmem_ranges={row};
  expect(malformed,A,0,HugeVacancyStatus::Invalid);
 }
 auto lower=checked_huge_pagemap_occupied_range(empty.pagemap,0,A);
 assert(!lower.valid&&!lower.range.found&&lower.resume_after==0);++cases;
 auto overflow=checked_huge_pagemap_occupied_range(empty.pagemap,UINT64_MAX-P,UINT64_MAX);
 assert(!overflow.valid&&!overflow.range.found&&overflow.resume_after==0);++cases;
 for(auto*t:tables)std::free(t);
 std::printf("Host actual huge-vacancy selector: %u vectors, finite ports; Native/CPL3/concurrency/runtime unqualified\n",cases);
 return 0;
}
}
int main(){return test::run();}
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vmem-source", type=Path, default=VMEM)
    args = parser.parse_args()
    source = args.vmem_source.read_text()
    constants = []
    for name in ("USER_SPACE_START", "USER_SPACE_END", "MMAP_START"):
        start = source.index("constexpr uint64_t " + name + " =")
        constants.append(source[start:source.index(";", start) + 1])
    types = [function(source, name) + ";" for name in (
        "struct OccupiedVmemRange {", "struct CheckedHugeOccupiedRange {",
        "enum class HugeVacancyStatus : uint8_t {", "struct HugeVacancySelection {")]
    names = ("pte_raw", "is_reserved_leaf", "table_from_entry", "page_table_index", "next_table_boundary", "ranges_overlap",
             "align_huge_mmap_up", "valid_huge_mmap_length", "checked_huge_pagemap_occupied_range", "checked_huge_occupied_range",
             "find_huge_vacancy_from", "find_huge_vacancy")
    bodies = [function(source, "auto " + name + "(") for name in names]
    print(json.dumps(dict(source_path=str(args.vmem_source.resolve()), source_sha256=hashlib.sha256(source.encode()).hexdigest(),
                          extracted={n: hashlib.sha256(b.encode()).hexdigest() for n, b in zip(names, bodies)}), sort_keys=True))
    code = PRELUDE.replace("CONSTANTS", "\n".join(constants)).replace("TYPES", "\n".join(types)).replace("FUNCTIONS", "\n".join(bodies))
    with tempfile.TemporaryDirectory(prefix="wos-huge-aligned-vacancy-") as folder:
        work = Path(folder)
        cpp, executable = work / "check.cpp", work / "check"
        cpp.write_text(code)
        command = ["/usr/bin/clang++", "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Werror",
                   "-I", str(ROOT / "modules/kern/src"), "-I", str(ROOT / "modules/kern/include"), "-I", str(ROOT / "modules"), str(cpp), "-o", str(executable)]
        compile_result = subprocess.run(command, capture_output=True, timeout=30)
        if len(compile_result.stdout) + len(compile_result.stderr) > 8 * 1024 * 1024:
            raise AssertionError("Host compiler output budget exceeded")
        print(compile_result.stdout.decode(errors="replace"), end="")
        print(compile_result.stderr.decode(errors="replace"), end="")
        compile_result.check_returncode()
        run = subprocess.run([str(executable)], capture_output=True, timeout=15)
        if len(run.stdout) + len(run.stderr) > 8 * 1024 * 1024:
            raise AssertionError("Host test output budget exceeded")
        print(run.stdout.decode(errors="replace"), end="")
        print(run.stderr.decode(errors="replace"), end="")
        run.check_returncode()


if __name__ == "__main__":
    main()
