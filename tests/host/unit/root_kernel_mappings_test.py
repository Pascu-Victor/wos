#!/usr/bin/env python3
"""Execute actual kernel-half refresh, wrappers and exec's staging fragment.

The real paging layout and implementation bodies run against random tables.
Task construction and permanent-table rejection are controlled endpoints; this
does not qualify concurrent hardware mappings or predict guest timing gains.
"""
import os
from pathlib import Path
import re
import subprocess
import tempfile

from exec_trace_ownership_test import function

ROOT = Path(__file__).resolve().parents[3]
virt = Path(os.environ.get("WOS_ROOT_MAPPING_TEST_VIRT", ROOT / "modules/kern/src/platform/mm/virt.opt.cpp")).read_text()
exec_source = Path(os.environ.get("WOS_ROOT_MAPPING_TEST_EXEC", ROOT / "modules/kern/src/syscalls_impl/process/exec.cpp")).read_text()
allow_old = os.environ.get("WOS_ROOT_MAPPING_ALLOW_OLD") == "1"
# Include the actual copy statement/block, after the unchanged allocation
# failure guard and before any TLS inspection or new-image mapping.
start = exec_source.index("auto* new_pagemap = mm::virt::create_pagemap();")
guard = exec_source.index("if (new_pagemap == nullptr)", start)
opening = exec_source.index("{", guard)
end, depth = opening + 1, 1
while depth:
    depth += (exec_source[end] == "{") - (exec_source[end] == "}")
    end += 1
fragment = exec_source[end:exec_source.index("// --- Create new thread", end)]
root_api = function(virt, "copy_kernel_mappings_to") if "void copy_kernel_mappings_to(" in virt else ""
# Before-source evidence invokes its existing Task wrapper, so failure is an
# allocation-free staging assertion, rather than an unavailable API/compiler.
root_call = "copy_kernel_mappings_to(root);" if root_api else "sched::task::Task task{}; task.pagemap=root; copy_kernel_mappings(&task);"
constants = "\n".join(re.findall(r"^constexpr size_t KERNEL_PML4_(?:START|END) = .*;$", virt, re.M))
code = r'''
#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <random>
#include <platform/mm/paging.hpp>
namespace ker::mod::sched::task {
int constructors=0,destructors=0,population_locks=0;
struct Task {
 ker::mod::mm::paging::PageTable* pagemap{};
 Task(){++constructors;}
 ~Task(){++destructors;++population_locks;}
};
}
namespace ker::mod::mm::virt {
namespace paging=ker::mod::mm::paging;
namespace sched=ker::mod::sched;
using paging::PageTable;using paging::PageTableEntry;
CONSTANTS
PageTable* kernel_pagemap{};
PageTable const* forbidden{};
int guard_calls=0;
struct Rejected {};
void selftest_require_mutable_pagemap_root(PageTable const* root,const char* phase){
 assert(std::strcmp(phase,"refresh-kernel-mappings")==0);++guard_calls;
 if(root==forbidden)throw Rejected{};
}
RAW
FROM_RAW
ENTRY
REFRESH
ROOT_API
TASK_API
void copy_root(PageTable* root){ROOT_CALL}
}
namespace test {
namespace virt=ker::mod::mm::virt;
namespace mm=ker::mod::mm;
namespace sched=ker::mod::sched;
using mm::paging::PageTable;
void exec_staging(PageTable* new_pagemap){FRAGMENT}
void check(PageTable const& result,PageTable const& before,PageTable const& kernel){
 for(size_t i=0;i<512;++i){
  auto expected=i<256?before.entries[i]:kernel.entries[i];
  assert(std::memcmp(&result.entries[i],&expected,sizeof(expected))==0);
 }
}
int run(){
 std::mt19937_64 random(0x57a6e);
 for(int round=0;round<3000;++round){
  PageTable kernel{},original{},direct{},via_task{},staged{};
  for(size_t i=0;i<512;++i){
   kernel.entries[i]=virt::pte_from_raw(random());
   original.entries[i]=virt::pte_from_raw(random());
   if((i+round)%4==0)original.entries[i]=kernel.entries[i];
  }
  direct=via_task=staged=original;virt::kernel_pagemap=&kernel;
  PageTable const kernel_before=kernel;
  virt::copy_root(&direct);check(direct,original,kernel);
  {sched::task::Task task{};task.pagemap=&via_task;virt::copy_kernel_mappings(&task);}
  check(via_task,original,kernel);
  int c=sched::task::constructors,d=sched::task::destructors,l=sched::task::population_locks;
  exec_staging(&staged);check(staged,original,kernel);
  if(!ALLOW_OLD)assert(sched::task::constructors==c&&sched::task::destructors==d&&sched::task::population_locks==l);
  assert(std::memcmp(&kernel,&kernel_before,sizeof(kernel))==0);
  // A later kernel mapping change must be reflected on the next refresh.
  for(size_t i=256;i<512;++i)kernel.entries[i]=virt::pte_from_raw(random());
  virt::copy_root(&direct);check(direct,original,kernel);
  staged.entries[511]=virt::pte_from_raw(0);
  exec_staging(&staged);check(staged,original,kernel);
 }
 PageTable root{},kernel{};virt::kernel_pagemap=&kernel;
 std::memset(&root,0xa5,sizeof(root));PageTable const before=root;
 int guards=virt::guard_calls;
 virt::copy_root(nullptr);virt::copy_root(&kernel);virt::copy_kernel_mappings(nullptr);
 {sched::task::Task task{};virt::copy_kernel_mappings(&task);}
 assert(virt::guard_calls==guards);
 virt::kernel_pagemap=nullptr;virt::copy_root(&root);exec_staging(&root);
 assert(virt::guard_calls==guards&&std::memcmp(&root,&before,sizeof(root))==0);
 virt::kernel_pagemap=&kernel;virt::forbidden=&root;
#ifdef WOS_SELFTEST
 for(int endpoint=0;endpoint<3;++endpoint){
  bool rejected=false;
  try{
   if(endpoint==0)virt::copy_root(&root);
   else if(endpoint==1){sched::task::Task task{};task.pagemap=&root;virt::copy_kernel_mappings(&task);}
   else exec_staging(&root);
  }catch(virt::Rejected const&){rejected=true;}
  assert(rejected&&std::memcmp(&root,&before,sizeof(root))==0);
 }
#else
 virt::copy_root(&root);check(root,before,kernel);assert(virt::guard_calls==guards);
#endif
 std::puts(ALLOW_OLD?"Actual before refresh/wrapper/staging functional oracle PASS; temporary-Task assertion disabled":
 "Actual root refresh and exec staging: 3000 random table oracles, dynamic recopy, lower-half preservation, null/self/source-null, diagnostic guard and zero temporary Task/charge-lock endpoints PASS");
 return 0;
}
}
int main(){return test::run();}
'''
for key, value in {
    "CONSTANTS": constants,
    "FROM_RAW": function(virt, "pte_from_raw"),
    "RAW": function(virt, "pte_raw"),
    "ENTRY": function(virt, "entry_at"),
    "REFRESH": function(virt, "refresh_kernel_mappings"),
    "ROOT_API": root_api,
    "TASK_API": function(virt, "copy_kernel_mappings"),
    "ROOT_CALL": root_call,
    "FRAGMENT": fragment,
    "ALLOW_OLD": "true" if allow_old else "false",
}.items():
    code = code.replace(key, value)
with tempfile.TemporaryDirectory(prefix="wos-root-kernel-mapping-") as directory:
    source_path = Path(directory) / "test.cpp"
    source_path.write_text(code)
    for diagnostic in (False, True):
        binary_path = Path(directory) / ("diagnostic" if diagnostic else "normal")
        command = ["/usr/bin/clang++", "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined",
                   "-I" + str(ROOT / "modules/kern/src"), "-I" + str(ROOT / "modules/kern/include"),
                   "-I" + str(ROOT / "modules"),
                   str(source_path), "-o", str(binary_path)]
        if diagnostic:
            command.append("-DWOS_SELFTEST")
        subprocess.run(command, check=True)
        environment = os.environ.copy()
        environment["ASAN_OPTIONS"] = "detect_leaks=0"
        subprocess.run([str(binary_path)], env=environment, check=True)
