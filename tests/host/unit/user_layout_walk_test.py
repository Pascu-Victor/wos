#!/usr/bin/env python3
"""Check actual placement/count walkers with controlled page tables and pins."""
import os
from pathlib import Path
import re
import subprocess
import tempfile

from exec_trace_ownership_test import function

ROOT = Path(__file__).resolve().parents[3]
MM = ROOT / "modules/kern/src/platform/mm"
source = Path(os.environ.get("WOS_LAYOUT_WALK_TEST_SOURCE", MM / "virt.opt.cpp")).read_text()
layout = Path(os.environ.get("WOS_LAYOUT_TEST_SOURCE", MM / "user_layout.cpp")).read_text()
walker = function(source, "count_virtual_bytes_in_table")
prefix = re.search(r"template <bool StopAtFirst = false>\s*(?=auto count_virtual_bytes_in_table)", source)
assert prefix
chunks = [function(source, n) for n in ["index_of", "is_reserved_leaf", "leaf_entry", "is_page_mapped_or_reserved"]]
chunks += [prefix.group() + walker, function(source, "count_user_virtual_bytes"),
           function(source, "query_user_virtual_range"), function(source, "is_user_virtual_range_free")]
range_status = re.search(r"enum class UserVirtualRangeStatus[^;]*;", (MM / "virt.hpp").read_text())
assert range_status
placement = function(layout, "range_is_free")
code = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <random>
#include <vector>
#include <platform/mm/paging.hpp>
namespace test {
using namespace ker::mod::mm;
using paging::PageTable;using paging::PageTableEntry;using vaddr_t=uint64_t;
size_t reads=0,pins=0,acquires=0,releases=0,translations=0,poison_translations=0;int guard_depth=0;bool pin_ok=true;
constexpr uint64_t POISON=0x000ffffffffff000ULL;
auto entry_at(PageTable*t,size_t i)->PageTableEntry&{assert(i<512);++reads;return t->entries[i];}
auto pte_raw(const PageTableEntry&e)->uint64_t{uint64_t n;std::memcpy(&n,&e,8);return n;}
auto raw(uint64_t n)->PageTableEntry{PageTableEntry e;std::memcpy(&e,&n,8);return e;}
namespace addr{auto get_virt_pointer(uint64_t p)->void*{++translations;if(p==POISON){++poison_translations;assert(false);}return reinterpret_cast<void*>(p);}}
namespace sched{struct PreemptGuard{PreemptGuard(){++guard_depth;}~PreemptGuard(){assert(!pins);--guard_depth;}};}
constexpr size_t USER_PAGEMAP_ACCESS_GATE_NONE=SIZE_MAX;
RANGE_STATUS
struct Gate{std::atomic<uint32_t>readers{0};};std::array<Gate,1>user_pagemap_access_gates;
auto user_pagemap_access_gate_index(PageTable*)->size_t{return 0;}
auto acquire_user_pagemap_access(PageTable*,bool wait,size_t&gate)->bool{assert(guard_depth&&!wait);++acquires;if(!pin_ok)return false;++pins;gate=0;return true;}
template<class Wait>auto admit_user_pagemap_reader(PageTable*root,size_t&gate,Wait&&)->bool{return acquire_user_pagemap_access(root,false,gate);}
void release_user_pagemap_access(size_t gate){assert(guard_depth&&gate==0&&pins);--pins;++releases;}
struct Log{template<class...T>static void critical(T...){assert(false);}};using log=Log;
FUNCTIONS
namespace virt{using ::test::is_user_virtual_range_free;using ::test::is_page_mapped_or_reserved;}
PLACEMENT
std::vector<PageTable*>tables;
auto table()->PageTable*{auto*t=static_cast<PageTable*>(std::aligned_alloc(4096,4096));assert(t);std::memset(t,0,4096);tables.push_back(t);return t;}
void put(PageTable*root,uint64_t a,uint64_t val){auto*t=root;for(int l=4;l>1;--l){auto&e=t->entries[index_of(a,l)];if(!e.present)e=raw(reinterpret_cast<uint64_t>(table())|7);t=static_cast<PageTable*>(addr::get_virt_pointer(e.frame<<12));}t->entries[index_of(a,1)]=raw(val);}
auto reference_bytes(PageTable*root,uint64_t start,uint64_t size)->uint64_t{uint64_t bytes=0;for(uint64_t a=start;a<start+size;a+=4096)if(is_page_mapped_or_reserved(root,a))bytes+=4096;return bytes;}
void check(PageTable*root,uint64_t start,uint64_t size){auto expected=reference_bytes(root,start,size);assert(range_is_free(root,start,size)==(expected==0));uint64_t bytes=0;assert(count_user_virtual_bytes(root,start,start+size,bytes));assert(bytes==expected);assert(!pins&&!guard_depth);}
int run(){
 constexpr uint64_t BASE=0x740000000000ULL,LIMIT=0x800000000000ULL,SPAN=(64ULL<<20)+5*4096;
 auto*root=table();reads=0;assert(range_is_free(root,BASE,SPAN));assert(reads==1&&acquires==1&&releases==1&&!pins);
 std::puts("Empty 64MiB+5page placement: one upper-table read, rather than16389 repeated4KiB hierarchy probes (operation count, not guest timing)");
 std::mt19937_64 rng(23456);
 for(size_t i=0;i<300;++i){uint64_t a=BASE+(rng()%16384)*4096;put(root,a,i%4==0?paging::PAGE_RESERVED:i%4==1?paging::PAGE_PRESENT:i%4==2?7:0);}
 for(size_t i=0;i<3000;++i)check(root,BASE+(rng()%16384)*4096,(rng()%512+1)*4096);
 for(uint64_t a:std::array<uint64_t,5>{BASE,BASE+(2ULL<<20)-4096,BASE+(1ULL<<30)-4096,BASE+(1ULL<<39)-4096,LIMIT-4096}){put(root,a,paging::PAGE_RESERVED);check(root,a-4096,8192);check(root,a,4096);}
 // Count retains every leaf, while occupancy stops at its first hit.
 auto*dense=table();for(size_t i=0;i<512;++i)put(dense,BASE+i*4096,paging::PAGE_RESERVED);
 reads=0;assert(!range_is_free(dense,BASE,512*4096));assert(reads==4);uint64_t bytes=0;reads=0;assert(count_user_virtual_bytes(dense,BASE,BASE+512*4096,bytes));assert(bytes==512*4096&&reads==515);
 // A complete count must scan every leaf across 32 tables, including holes
 // with unrelated software bits and combined present/reserved flags.
 auto*large=table();uint64_t expected_large=0;
 constexpr std::array<uint64_t,6> FLAGS={paging::PAGE_RESERVED,paging::PAGE_PRESENT,7,paging::PAGE_PRESENT|paging::PAGE_RESERVED,0,paging::PAGE_COW|paging::PAGE_NX};
 for(size_t i=0;i<16384;++i){auto flags=FLAGS[i%FLAGS.size()];put(large,BASE+i*4096,flags);if(flags&(paging::PAGE_PRESENT|paging::PAGE_RESERVED))expected_large+=4096;}
 reads=0;assert(count_user_virtual_bytes(large,BASE,BASE+(64ULL<<20),bytes));assert(bytes==expected_large&&reads==16384+32+2);
 check(large,BASE+4096,(2ULL<<20)+3*4096);check(large,BASE+(32ULL<<20)-4096,(2ULL<<20)+7*4096);
 assert(count_user_virtual_bytes(large,0,LIMIT,bytes)&&bytes==expected_large);
 auto*huge=table();auto*p3=table();huge->entries[0]=raw(reinterpret_cast<uint64_t>(p3)|7);p3->entries[1]=raw((1ULL<<30)|paging::PAGE_PRESENT|(1ULL<<7));
 assert(!range_is_free(huge,(1ULL<<30)+4096,8192));assert(count_user_virtual_bytes(huge,(1ULL<<30)+4096,(1ULL<<30)+3*4096,bytes)&&bytes==8192);
 auto*p2=table();p3->entries[2]=raw(reinterpret_cast<uint64_t>(p2)|7);p2->entries[5]=raw((10ULL<<20)|paging::PAGE_PRESENT|(1ULL<<7));
 assert(!range_is_free(huge,(2ULL<<30)+(10ULL<<20)-4096,8192));assert(count_user_virtual_bytes(huge,(2ULL<<30)+(10ULL<<20)-4096,(2ULL<<30)+(10ULL<<20)+4096,bytes)&&bytes==4096);
 assert(range_is_free(huge,3ULL<<30,4096));assert(!pins);
 // Every complete leaf range length, with clipped first/last indexes.
 // Prefix sums are an independent byte oracle and exact reads prove no
 // rounded tail, skipped holes, extra group, or early complete-count exit.
 auto*clipped=table();std::array<uint64_t,513>prefix_bytes{};
 for(size_t i=0;i<512;++i){auto flags=FLAGS[i%FLAGS.size()];put(clipped,BASE+i*4096,flags);prefix_bytes[i+1]=prefix_bytes[i]+((flags&(paging::PAGE_PRESENT|paging::PAGE_RESERVED))?4096:0);}
 for(size_t length=1;length<=512;++length){
  size_t first=(512-length)/2;auto expected=prefix_bytes[first+length]-prefix_bytes[first];reads=0;
  assert(count_user_virtual_bytes(clipped,BASE+first*4096,BASE+(first+length)*4096,bytes));assert(bytes==expected&&reads==length+3);
  assert(range_is_free(clipped,BASE+first*4096,length*4096)==(expected==0));assert(!pins&&!guard_depth);
 }
 // Full lower-half counting mixes huge and small leaves; non-present
 // intermediate markers occupy no span and upper-half entries are excluded.
 put(huge,0,paging::PAGE_PRESENT);put(huge,LIMIT-4096,paging::PAGE_RESERVED);
 p3->entries[3]=raw(paging::PAGE_RESERVED|(1ULL<<7));huge->entries[256]=raw(paging::PAGE_PRESENT);
 assert(count_user_virtual_bytes(huge,0,LIMIT,bytes)&&bytes==(1ULL<<30)+(2ULL<<20)+8192);
 assert(count_user_virtual_bytes(huge,4096,LIMIT-4096,bytes)&&bytes==(1ULL<<30)+(2ULL<<20));
 reads=0;assert(!is_user_virtual_range_free(huge,0,LIMIT)&&reads==4);
 for(auto bounds:std::array<std::array<uint64_t,2>,5>{{{BASE+1,BASE+4096},{BASE,BASE+4097},{BASE+4096,BASE},{0,LIMIT+4096},{0,UINT64_MAX}}}){bytes=99;reads=0;assert(!count_user_virtual_bytes(root,bounds[0],bounds[1],bytes)&&bytes==0&&!reads&&!pins&&!guard_depth);}
 bytes=99;reads=0;assert(!count_user_virtual_bytes(nullptr,0,4096,bytes)&&bytes==0&&!reads);
 bytes=99;reads=0;auto acquired=acquires;assert(count_user_virtual_bytes(root,BASE,BASE,bytes)&&bytes==0&&!reads&&acquires==acquired);
 for(auto a:std::array<std::array<uint64_t,2>,7>{{{BASE,0},{BASE+1,4096},{BASE,4097},{UINT64_MAX-4095,4096},{LIMIT,4096},{LIMIT-4096,8192},{UINT64_MAX-4095,8192}}}){reads=0;assert(!range_is_free(root,a[0],a[1]));assert(!reads&&!pins&&!guard_depth);}
 assert(!range_is_free(nullptr,BASE,4096));assert(!is_user_virtual_range_free(root,BASE+4096,BASE));assert(!is_user_virtual_range_free(root,BASE,BASE));
 pin_ok=false;reads=0;assert(!range_is_free(root,BASE+SPAN,4096));assert(!reads&&!pins&&!guard_depth);bytes=99;assert(!count_user_virtual_bytes(root,0,LIMIT,bytes)&&bytes==0&&!reads&&!pins&&!guard_depth);pin_ok=true;
 // Malformed PRESENT PML4 PS is not a child pointer or free-space authority.
 auto*malformed=table();malformed->entries[0]=raw(POISON|paging::PAGE_PRESENT|(1ULL<<7));
 bytes=99;reads=translations=0;assert(!count_user_virtual_bytes(malformed,0,4096,bytes));
 assert(bytes==0&&reads==1&&translations==0&&!poison_translations&&!pins&&!guard_depth);
 reads=translations=0;assert(!is_user_virtual_range_free(malformed,0,4096));
 assert(reads==1&&translations==0&&!poison_translations&&!pins&&!guard_depth);
 reads=translations=0;assert(!range_is_free(malformed,0,4096));
 assert(reads==1&&translations==0&&!pins&&!guard_depth);
 // A valid occupied prefix must not leak a partial successful byte count.
 constexpr uint64_t NEXT_PML4=1ULL<<39;
 auto*partial=table();auto*prefix3=table();partial->entries[0]=raw(reinterpret_cast<uint64_t>(prefix3)|7);
 prefix3->entries[0]=raw(paging::PAGE_PRESENT|(1ULL<<7));
 partial->entries[1]=raw(POISON|paging::PAGE_PRESENT|(1ULL<<7));
 bytes=99;reads=translations=0;assert(!count_user_virtual_bytes(partial,0,NEXT_PML4+4096,bytes));
 assert(bytes==0&&reads==514&&translations==1&&!poison_translations&&!pins&&!guard_depth);
 // Occupancy still stops at its first supported hit, without inspecting later malformed entries.
 reads=translations=0;assert(!is_user_virtual_range_free(partial,0,NEXT_PML4+4096));
 assert(reads==2&&translations==1&&!pins&&!guard_depth);
 reads=translations=0;assert(!is_user_virtual_range_free(partial,NEXT_PML4,NEXT_PML4+4096));
 assert(reads==1&&translations==0&&!pins&&!guard_depth);
 // Nonpresent intermediate software markers remain holes, even with PS/frame bits.
 malformed->entries[0]=raw(POISON|paging::PAGE_RESERVED|(1ULL<<7));
 malformed->entries[256]=raw(POISON|paging::PAGE_PRESENT|(1ULL<<7));
 bytes=99;reads=translations=0;assert(count_user_virtual_bytes(malformed,0,LIMIT,bytes));
 assert(bytes==0&&reads==256&&translations==0&&!poison_translations&&!pins&&!guard_depth);
 reads=translations=0;assert(is_user_virtual_range_free(malformed,0,4096));
 assert(reads==1&&translations==0&&!pins&&!guard_depth);
 assert(acquires==releases+2);for(auto*t:tables)std::free(t);
 std::puts("3000 actual-source randomized occupancy/exact full-count checks, 32-table mixed leaves, whole lower-half counts, first-hit/full-count independence, reservations/PROT_NONE, hierarchy boundaries, clipped huge pages and fail-closed pins PASS");return 0;
}
}
int main(){return test::run();}
'''.replace("RANGE_STATUS", range_status.group()).replace("FUNCTIONS", "\n".join(chunks)).replace("PLACEMENT", placement)
with tempfile.TemporaryDirectory(prefix="wos-user-layout-walk-") as directory:
    source_path = Path(directory) / "test.cpp"
    binary_path = Path(directory) / "test"
    source_path.write_text(code)
    subprocess.run(["/usr/bin/clang++", "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined",
                    "-I" + str(ROOT / "modules/kern/src"), "-I" + str(ROOT / "modules/kern/include"),
                    "-I" + str(ROOT / "modules"), str(source_path), "-o", str(binary_path)], check=True)
    environment = os.environ.copy()
    environment["ASAN_OPTIONS"] = "detect_leaks=0"
    subprocess.run([str(binary_path)], env=environment, check=True)
