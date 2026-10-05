"""Compile actual procfs serialization and append helpers with a metadata seam.

The collector and kernel-image ABI are unchanged. This exercises their output
consumer, including unavailable metadata, bounded build IDs and short buffers;
it does not model live relocation or interrupt delivery.
"""
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
source = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()


def function(name):
    found = re.search(rf"^void {name}\([^;{{]*\{{", source, re.M)
    assert found, name
    pos, depth = source.index("{", found.start()) + 1, 1
    while depth:
        depth += (source[pos] == "{") - (source[pos] == "}")
        pos += 1
    return source[found.start():pos]


helpers = "\n".join(function(n) for n in (
    "append_dec64", "append_sconst", "append_char", "append_bool01"))
serializer = function("append_routing_timer_kernel_metadata")
prefix = r'''
#include <abi/kernel_debug_metadata.hpp>
#include <array>
#include <cassert>
#include <cstdio>
#include <string>
namespace ker::loader::runtime {
wos::abi::KernelDebugMetadata fixture{};
bool available=true;
unsigned calls=0;
bool kernel_debug_metadata(wos::abi::KernelDebugMetadata& value){
 ++calls;value=fixture;return available;
}
}
'''
tests = r'''
using namespace ker::loader::runtime;
using namespace wos::abi;
std::string expected(){
 bool valid=available&&(fixture.flags&KERNEL_DEBUG_METADATA_EXACT_MAPPING)!=0;
 std::string s=" kernel_meta_valid="+std::to_string(valid);
 s+=" kernel_load_base="+std::to_string(valid?fixture.load_base:0);
 s+=" kernel_text_start="+std::to_string(valid?fixture.text_start:0);
 s+=" kernel_text_end="+std::to_string(valid?fixture.text_end:0);
 s+=" kernel_build_id=";
 if(!valid||!(fixture.flags&KERNEL_DEBUG_METADATA_BUILD_ID_PRESENT)
    ||fixture.build_id_size==0||fixture.build_id_size>KERNEL_DEBUG_BUILD_ID_MAX)return s+"-";
 char const* hex="0123456789abcdef";
 for(unsigned i=0;i<fixture.build_id_size;++i){
  s+=hex[fixture.build_id[i]/16];s+=hex[fixture.build_id[i]%16];
 }
 return s;
}
unsigned cases=0;
void check(){
 auto want=expected();
 for(size_t cap=1;cap<=400;++cap){
  std::array<char,408> data;data.fill('!');char* start=data.data()+4;char* p=start;
  auto before=calls;append_routing_timer_kernel_metadata(p,start+cap-1);
  assert(calls==before+1&&p>=start&&p<start+cap);*p='\0';
  for(size_t i=0;i<4;++i)assert(data[i]=='!');
  for(size_t i=cap+4;i<data.size();++i)assert(data[i]=='!');
  if(cap>want.size()+2)assert(std::string(start)==want);
 }
 ++cases;
}
int main(){
 for(uint64_t base: {0xffffffff80001000ULL,0xffffffff95587000ULL,
                    0xffffffffb1234000ULL,0xfffffffecafe1000ULL}){
  fixture={};fixture.load_base=base;fixture.text_start=base+0x190;
  fixture.text_end=base+0x48b089;
  fixture.flags=KERNEL_DEBUG_METADATA_EXACT_MAPPING|KERNEL_DEBUG_METADATA_BUILD_ID_PRESENT;
  for(unsigned i=0;i<KERNEL_DEBUG_BUILD_ID_MAX;++i)fixture.build_id[i]=(i*13+0x9b)&255;
  for(unsigned n=1;n<=KERNEL_DEBUG_BUILD_ID_MAX;++n){fixture.build_id_size=n;check();}
  fixture.build_id_size=0;check();fixture.build_id_size=33;check();
  fixture.build_id_size=255;check();fixture.build_id_size=20;
  fixture.flags=KERNEL_DEBUG_METADATA_EXACT_MAPPING;check();
  fixture.flags=KERNEL_DEBUG_METADATA_BUILD_ID_PRESENT;check();
  fixture.flags=KERNEL_DEBUG_METADATA_EXACT_MAPPING|KERNEL_DEBUG_METADATA_BUILD_ID_PRESENT;
  available=false;check();available=true;
 }
 printf("%u metadata cases PASS; 400 buffer capacities each, exact slides/build IDs and unavailable output\n",cases);
}
'''
compiler = os.environ.get("WOS_HOST_CXX") or shutil.which("clang++-23") or "/usr/bin/clang++"
with tempfile.TemporaryDirectory(prefix="wos-routing-timer-meta-") as temporary:
    out = Path(os.environ.get("WOS_ROUTING_TIMER_METADATA_ARTIFACT_DIR", temporary))
    out.mkdir(parents=True, exist_ok=True)
    variants = [
        ("actual", None, None),
        ("wrong_slide", "VALID ? metadata.load_base : 0", "VALID ? metadata.load_base + 4096 : 0"),
        ("wrong_build_id", "HEX_DIGITS.at(BYTE & 0xfU)", "HEX_DIGITS.at((BYTE & 0xfU) ^ 1U)"),
    ]
    for name, old, new in variants:
        candidate = serializer
        if old is not None:
            assert candidate.count(old) == 1, name
            candidate = candidate.replace(old, new)
        cpp, exe = out / f"{name}.cpp", out / name
        cpp.write_text(prefix + helpers + candidate + tests)
        built = subprocess.run([compiler, "-std=c++23", "-O1", "-g",
                                "-fsanitize=address,undefined", "-fno-sanitize-recover=all",
                                "-I", str(ROOT / "modules/kern/src"), str(cpp), "-o", str(exe)],
                               capture_output=True, text=True)
        (out / f"{name}-build.log").write_text(built.stdout + built.stderr)
        assert built.returncode == 0, built.stdout + built.stderr
        run = subprocess.run([str(exe)], capture_output=True, text=True, timeout=30)
        (out / f"{name}-run.log").write_text(run.stdout + run.stderr)
        if old is None:
            assert run.returncode == 0, run.stdout + run.stderr
            print(run.stdout, end="")
        else:
            assert run.returncode != 0, f"surviving semantic mutant {name}"
            print(f"Compiled semantic mutant {name} rejected")
