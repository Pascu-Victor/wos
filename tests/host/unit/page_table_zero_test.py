#!/usr/bin/env python3
"""Check actual table allocation's private-page zeroing and pool contract.

Physical allocation and CR3 endpoints are modeled; this does not prove hardware
mapping coverage, sanitizer shadow propagation, or a guest timing improvement.
"""
import os
from pathlib import Path
import subprocess
import tempfile

from exec_trace_ownership_test import function

ROOT = Path(__file__).resolve().parents[3]
source = Path(os.environ.get("WOS_PAGE_TABLE_ZERO_TEST_SOURCE", ROOT / "modules/kern/src/platform/mm/virt.opt.cpp")).read_text()
body = function(source, "alloc_zeroed_page_table")
code = r'''
#include <cassert>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <initializer_list>
#include <string_view>
#include <platform/mm/paging.hpp>
namespace test {
namespace paging=ker::mod::mm::paging;using paging::PageTable;
static_assert(sizeof(PageTable)==paging::PAGE_SIZE);
enum class PhysicalPageOwner{PAGE_TABLE};enum class PageKind{PAGE_TABLE};
alignas(4096) PageTable page{},pooled{};
bool use_pool=false,fail=false;
uint64_t current_cr3=0x2000,kernel_cr3=0x1000;
int writes=0,allocator_clears=0,marks=0,allocs=0,infallible_calls=0,fallible_calls=0;
void check_zero(PageTable*t){auto*p=reinterpret_cast<unsigned char*>(t);for(size_t i=0;i<sizeof(PageTable);++i)assert(p[i]==0);}
auto try_alloc_page_table_from_pool()->PageTable*{return use_pool?&pooled:nullptr;}
namespace phys{
auto allocate(bool zero)->PageTable*{++allocs;if(fail)return nullptr;std::memset(&page,0xa5,sizeof(page));if(zero){uint64_t old=current_cr3;if(current_cr3!=kernel_cr3&&kernel_cr3){current_cr3=kernel_cr3;++writes;}std::memset(&page,0,sizeof(page));++allocator_clears;if(current_cr3!=old){current_cr3=old;++writes;}}return &page;}
auto page_alloc_with_reclaim(PhysicalPageOwner owner,uint64_t size,std::string_view tag)->void*{assert(owner==PhysicalPageOwner::PAGE_TABLE&&size==4096&&tag=="page_table");return allocate(true);}
auto page_alloc_full_overwrite_page_with_reclaim(PhysicalPageOwner owner,std::string_view tag)->void*{assert(owner==PhysicalPageOwner::PAGE_TABLE&&tag=="page_table");++infallible_calls;return allocate(false);}
auto page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner owner,std::string_view tag)->void*{assert(owner==PhysicalPageOwner::PAGE_TABLE&&tag=="page_table");++fallible_calls;return allocate(false);}
auto page_mark_kind(PageTable*t,PageKind kind)->bool{assert(kind==PageKind::PAGE_TABLE&&allocs==1);check_zero(t);++marks;return true;}
}
BODY
void reset(){writes=allocator_clears=marks=allocs=infallible_calls=fallible_calls=0;use_pool=fail=false;}
int run(){
 for(bool may_fail:{false,true})for(uint64_t kernel:{uint64_t{0},uint64_t{0x1000}})for(uint64_t current:{uint64_t{0x1000},uint64_t{0x2000}}){
  reset();kernel_cr3=kernel;current_cr3=current;assert(alloc_zeroed_page_table(may_fail)==&page);check_zero(&page);
  assert(current_cr3==current&&marks==1&&allocs==1&&writes==0&&allocator_clears==0);
  assert(fallible_calls==int(may_fail)&&infallible_calls==int(!may_fail));
 }
 for(bool may_fail:{false,true}){
  reset();use_pool=fail=true;assert(alloc_zeroed_page_table(may_fail)==&pooled);check_zero(&pooled);assert(!writes&&!marks&&!allocs&&!fallible_calls&&!infallible_calls);
  reset();fail=true;std::memset(&page,0xa5,sizeof(page));assert(!alloc_zeroed_page_table(may_fail));assert(!writes&&!marks&&allocs==1);assert(fallible_calls==int(may_fail)&&infallible_calls==int(!may_fail));auto const*bytes=reinterpret_cast<const unsigned char*>(&page);for(size_t i=0;i<sizeof(page);++i)assert(bytes[i]==0xa5);
 }
 std::puts("Actual table allocation: both fallibility branches zero dirty full page before kind/publication, modeled boot/kernel/process CR3 retained, zeroed pool bypasses both allocators and null preserves dirty page PASS");return 0;
}
}
int main(){return test::run();}
'''.replace("BODY", body)
with tempfile.TemporaryDirectory(prefix="wos-page-table-zero-") as directory:
    source_path = Path(directory) / "test.cpp"
    binary_path = Path(directory) / "test"
    source_path.write_text(code)
    subprocess.run(["/usr/bin/clang++", "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined",
                    "-I" + str(ROOT / "modules/kern/src"), "-I" + str(ROOT / "modules/kern/include"),
                    "-I" + str(ROOT / "modules"), str(source_path), "-o", str(binary_path)], check=True)
    environment = os.environ.copy()
    environment["ASAN_OPTIONS"] = "detect_leaks=0"
    subprocess.run([str(binary_path)], env=environment, check=True)
