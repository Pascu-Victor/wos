#!/usr/bin/env python3
"""Exercise the real WOS allocator policy and complete frg slab implementation.

Only mapping/futex endpoints use host equivalents. Includes demand-page touch,
full bucket exhaustion, freed/virgin chains, realloc, map failure, poisoning and
concurrent reuse. An optional old slab header provides a negative control.
"""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <new>
#include <thread>
#include <vector>
#include <sys/mman.h>
#include <sanitizer/asan_interface.h>
#define FRG_SLAB_TRACK_REGIONS
#include <mlibc/allocator.hpp>
struct Mapping { uintptr_t base; size_t length; };
std::mutex map_lock;
std::vector<Mapping> mappings;
std::atomic<bool> fail_map=false;
uintptr_t VirtualAllocator::map(size_t length) {
 if(fail_map.exchange(false)) return 0;
 void *p=mmap(nullptr,length,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);
 assert(p!=MAP_FAILED);assert(!madvise(p,length,MADV_NOHUGEPAGE));
 std::lock_guard<std::mutex> g(map_lock);mappings.push_back({reinterpret_cast<uintptr_t>(p),length});return reinterpret_cast<uintptr_t>(p);
}
void VirtualAllocator::unmap(uintptr_t p,size_t n) {
 std::lock_guard<std::mutex> g(map_lock);auto it=std::find_if(mappings.begin(),mappings.end(),[&](auto m){return m.base==p&&m.length==n;});assert(it!=mappings.end());mappings.erase(it);__asan_unpoison_memory_region(reinterpret_cast<void*>(p),n);assert(!munmap(reinterpret_cast<void*>(p),n));
}
struct Eager:VirtualAllocator { static constexpr bool lazy_freelist=false; };
struct Poison:VirtualAllocator {
 uintptr_t map(size_t n){auto p=VirtualAllocator::map(n);if(p)__asan_poison_memory_region(reinterpret_cast<void*>(p),n);return p;}
 void poison(void*p,size_t n){__asan_poison_memory_region(p,n);}
 void unpoison(void*p,size_t n){__asan_unpoison_memory_region(p,n);}
 void unpoison_expand(void*p,size_t n){unpoison(p,n);}
};
extern "C" void frg_panic(const char *s){std::fprintf(stderr,"%s\n",s);std::abort();}
extern "C" void frg_log(const char*){}
void clean(){std::lock_guard<std::mutex>g(map_lock);for(auto m:mappings){__asan_unpoison_memory_region(reinterpret_cast<void*>(m.base),m.length);assert(!munmap(reinterpret_cast<void*>(m.base),m.length));}mappings.clear();}
size_t resident(){size_t total=0;std::lock_guard<std::mutex>g(map_lock);for(auto m:mappings){std::vector<unsigned char>v(m.length/4096);assert(!mincore(reinterpret_cast<void*>(m.base),m.length,v.data()));for(auto x:v)total+=x&1;}return total;}
template<class P>size_t first_touch(){P p;frg::slab_pool<P,std::mutex> pool(p);auto a=pool.allocate(8);assert(a&&pool.get_size(a)==8);size_t n=resident();pool.free(a);clean();return n;}
template<class P>void exercise(){
 P policy;frg::slab_pool<P,std::mutex>pool(policy);
 for(size_t size:std::array<size_t,12>{8,16,32,64,128,256,512,1024,2048,4096,8192,16384}){
  size_t count=65536/size+7;std::vector<void*> v(count);std::vector<unsigned char> mark(count);
  for(size_t i=0;i<count;i++){v[i]=pool.allocate(size);assert(v[i]&&pool.get_size(v[i])==size);mark[i]=static_cast<unsigned char>(i+1);std::memset(v[i],mark[i],size);}
  // Cross the first slab's full->partial transition, then consume freed links
  // mixed with the second slab's virgin tail. All other live contents survive.
  for(size_t i=0;i<count;i+=2){pool.free(v[i]);v[i]=nullptr;}
  for(size_t i=0;i<count;i+=2){v[i]=pool.allocate(size);assert(v[i]);std::memset(v[i],mark[i],size);}
  for(size_t i=0;i<count;i++){auto*b=static_cast<unsigned char*>(v[i]);for(size_t j=0;j<size;j++)assert(b[j]==mark[i]);pool.deallocate(v[i],size);}
 }
 auto x=pool.allocate(32);std::memset(x,0xA7,32);auto y=pool.realloc(x,100000);assert(y&&pool.get_size(y)>=100000);for(size_t j=0;j<32;j++)assert(static_cast<unsigned char*>(y)[j]==0xA7);pool.free(y);
 auto z=pool.allocate(0);assert(z);pool.free(z);pool.free(nullptr);
 fail_map=true;assert(!pool.allocate(200000));assert(pool.allocate(200000));
 std::array<std::thread,8> threads;
 for(auto&t:threads)t=std::thread([&]{for(unsigned run=0;run<500;run++){std::array<void*,40> b;for(size_t i=0;i<b.size();i++){b[i]=pool.allocate(128);assert(b[i]);std::memset(b[i],0x5A,128);}for(auto p:b){for(size_t i=0;i<128;i++)assert(static_cast<unsigned char*>(p)[i]==0x5A);pool.free(p);}}});
 for(auto&t:threads)t.join();clean();
}
int main(int argc,char**){
 if(argc>1){
  Poison policy;frg::slab_pool<Poison,std::mutex> pool(policy);
  for(size_t size:std::array<size_t,4>{8,128,4096,16384}){
   auto a=pool.allocate(size);auto b=pool.allocate(size);assert(a&&b);pool.free(a);
   auto c=pool.allocate(size);assert(c);std::memset(b,0x11,size);std::memset(c,0x22,size);
   pool.free(b);pool.free(c);
  }
  auto large=pool.allocate(200000);assert(large);pool.free(large);clean();
  std::puts("PASS real integrity walker with poisoned virgin tails and freed-slot chains");return 0;
 }
 auto eager=first_touch<Eager>();auto lazy=first_touch<VirtualAllocator>();std::fprintf(stderr,"pages eager=%zu lazy=%zu\n",eager,lazy);assert(eager==16&&lazy==1);exercise<VirtualAllocator>();exercise<Eager>();exercise<Poison>();std::puts("PASS exact WOS policy/full FRG source: capacity, mixed reuse, realloc, direct-large, map failure, poisoning, eight-thread reuse");}
'''


def main():
    compiler = next((p for p in ("/usr/bin/clang++", "/usr/bin/g++") if Path(p).is_file()), None)
    assert compiler, "a native C++ compiler is required"
    with tempfile.TemporaryDirectory(prefix="wos-mlibc-slab-") as folder:
        work = Path(folder)
        (work / "mlibc").mkdir()
        (work / "bits").mkdir()
        # Copy only this exact production header to avoid shadowing Linux bits/types.h.
        shutil.copy2(ROOT / "toolchain/src/mlibc/options/internal/include/mlibc/allocator.hpp", work / "mlibc/allocator.hpp")
        (work / "mlibc/lock.hpp").write_text("#include <mutex>\nusing FutexLock = std::mutex;\n")
        (work / "bits/ensure.h").write_text("#pragma once\n")
        (work / "internal-config.h").write_text("#define MLIBC_DEBUG_ALLOCATOR 0\n")
        if old := os.environ.get("WOS_MLIBC_SLAB_SOURCE"):
            (work / "frg").mkdir()
            shutil.copy2(old, work / "frg/slab.hpp")
        (work / "check.cpp").write_text(SOURCE)
        subprocess.run([compiler, "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-pthread", "-I", str(work), "-I", str(ROOT / "toolchain/src/mlibc/subprojects/frigg/include"), str(work / "check.cpp"), "-o", str(work / "check")], check=True)
        # LSan cannot attach under the managed sandbox. All policy mappings are
        # explicitly released; ASan access checks and UBSan remain enabled.
        environment = dict(os.environ, ASAN_OPTIONS="detect_leaks=0", UBSAN_OPTIONS="halt_on_error=1")
        subprocess.run([str(work / "check")], check=True, env=environment)
        (work / "frg").mkdir(exist_ok=True)
        header = (ROOT / "toolchain/src/mlibc/subprojects/frigg/include/frg/slab.hpp").read_text()
        assert "constexpr bool enable_checking = false;" in header
        (work / "frg/slab.hpp").write_text(header.replace("constexpr bool enable_checking = false;", "constexpr bool enable_checking = true;"))
        subprocess.run([compiler, "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-pthread", "-I", str(work), "-I", str(ROOT / "toolchain/src/mlibc/subprojects/frigg/include"), str(work / "check.cpp"), "-o", str(work / "check-integrity")], check=True)
        subprocess.run([str(work / "check-integrity"), "integrity"], check=True, env=environment)


if __name__ == "__main__":
    main()
