#!/usr/bin/env python3
"""Check actual locale defaults, deep copies and byte case-conversion bodies."""
from pathlib import Path
import shutil,subprocess,tempfile,os
ROOT=Path(__file__).resolve().parents[3];M=ROOT/'toolchain/src/mlibc'
source=(M/'options/posix/generic/posix_ctype.cpp').read_text()
def extract(name):
 start=source.index('int '+name+'(');left=source.index('{',start);depth=0
 for i in range(left,len(source)):
  if source[i]=='{':depth+=1
  elif source[i]=='}':
   depth-=1
   if depth==0:return source[start:i+1].replace(name,'wos_'+name).replace('locale_t','wos_locale_t')
 raise AssertionError(name)
code=r'''
#include <cassert>
#include <atomic>
#include <climits>
#include <array>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <thread>
#include <type_traits>
#include <mlibc/locale.hpp>
extern "C" void frg_panic(const char *s){fprintf(stderr,"%s\n",s);abort();}
extern "C" void frg_log(const char*){}
std::atomic<int> live{0};
void *MemoryAllocator::allocate(size_t n){void *p=std::malloc(n);assert(p);++live;return p;}
void MemoryAllocator::free(void *p){if(p){--live;std::free(p);}}
MemoryAllocator &getAllocator(){static MemoryAllocator a;return a;}
template<class T>void compare(const T&a,const T&b){
 assert(a.localeName==b.localeName);assert(a.localeName.data()!=b.localeName.data());
 for(size_t i=0;i<a.members.size();++i){auto &x=a.members[i].value;auto &y=b.members[i].value;assert(x.index()==y.index());
  std::visit([&](auto &v){using V=std::remove_cvref_t<decltype(v)>;auto &w=std::get<V>(y);if constexpr(std::is_same_v<V,uint32_t>)assert(v==w);else if constexpr(!std::is_same_v<V,std::monostate>){assert(v.size()==w.size());assert(v.data()==w.data());}},x);
 }
}
using wos_locale_t = void*;
''' + extract('tolower_l')+'\n'+extract('toupper_l')+r'''
void run(){
 mlibc::localeinfo c{},independent{},copied{c};
 assert(c.localeName==copied.localeName);assert(c.localeName.data()!=copied.localeName.data());
 #define CMP(n) compare(c.n,independent.n);compare(c.n,copied.n)
 CMP(ctype);CMP(numeric);CMP(time);CMP(collate);CMP(monetary);CMP(messages);CMP(paper);CMP(name);CMP(address);CMP(telephone);CMP(measurement);CMP(identification);
 assert(c.ctype.data.data()==copied.ctype.data.data());assert(c.ctype.data.size()==copied.ctype.data.size());assert(c.ctype.offsets.data()==copied.ctype.offsets.data());assert(c.ctype.offsets.size()==copied.ctype.offsets.size());
 for(int i=-128;i<256;++i){assert(wos_tolower_l(i,&c)==static_cast<int>(c.ctype.map_tolower()[i+128]));assert(wos_toupper_l(i,&c)==static_cast<int>(c.ctype.map_toupper()[i+128]));}
 assert(wos_tolower_l(EOF,&c)==EOF&&wos_toupper_l(EOF,&c)==EOF);
 for(int i:std::array<int,4>{-129,256,INT_MIN,INT_MAX})assert(wos_tolower_l(i,&c)==i&&wos_toupper_l(i,&c)==i);
 std::array<uint32_t,384> lower{},upper{};
 for(int i=0;i<384;++i){lower[i]=c.ctype.map_tolower()[i];upper[i]=c.ctype.map_toupper()[i];}
 for(int i=0;i<256;++i){lower[i+128]=(i+17)%256;upper[i+128]=(i+3)%256;}
 copied.ctype.members[_NL_CTYPE_TOLOWER&0xFFFF]=frg::span<const uint32_t>{lower.data(),lower.size()};
 copied.ctype.members[_NL_CTYPE_TOUPPER&0xFFFF]=frg::span<const uint32_t>{upper.data(),upper.size()};
 for(int i=0;i<256;++i){assert(wos_tolower_l(i,&copied)==(i+17)%256);assert(wos_toupper_l(i,&copied)==(i+3)%256);}
 assert(wos_tolower_l(EOF,&copied)==EOF&&wos_toupper_l(EOF,&copied)==EOF);
 assert(wos_tolower_l('A',&c)=='a'&&wos_toupper_l('a',&c)=='A');
 copied.localeName=frg::string<MemoryAllocator>{"changed",getAllocator()};copied.ctype.localeName=frg::string<MemoryAllocator>{"private",getAllocator()};copied.time.members[0]=uint32_t{42};
 assert(c.localeName=="C"&&c.ctype.localeName=="C");assert(!std::holds_alternative<uint32_t>(c.time.members[0].value));
}
int main(){run();assert(live==0);std::thread t[8];for(auto &x:t)x=std::thread([]{for(int i=0;i<64;++i)run();});for(auto &x:t)x.join();assert(live==0);puts("actual locale defaults/copy ownership/selected byte maps/EOF/bounds/8threads/destruction PASS");}
'''
with tempfile.TemporaryDirectory(prefix='wos-locale-copy-') as td:
 p=Path(td);(p/'mlibc').mkdir();(p/'bits').mkdir();shutil.copyfile(M/'options/internal/include/mlibc/locale.hpp',p/'mlibc/locale.hpp')
 for name in ('nl_item.h','locale-internals.h'):shutil.copyfile(M/'options/internal/include/bits'/name,p/'bits'/name)
 (p/'mlibc-config.h').write_text('#pragma once\n#define __MLIBC_POSIX_OPTION 1\n#define __MLIBC_GLIBC_OPTION 1\n')
 (p/'mlibc/allocator.hpp').write_text('#pragma once\n#include <cassert>\n#include <frg/string.hpp>\n#define __ensure(x) assert(x)\nstruct MemoryAllocator {void*allocate(size_t);void free(void*);};\nMemoryAllocator &getAllocator();\n')
 (p/'check.cpp').write_text(code);binary=p/'check'
 subprocess.run(['/usr/bin/clang++','-std=c++23','-O2','-g','-fsanitize=address,undefined','-fno-omit-frame-pointer','-pthread','-I',str(p),'-I',str(M/'subprojects/frigg/include'),str(p/'check.cpp'),str(M/'options/internal/generic/locale-defaults.cpp'),str(M/'options/internal/generic/c-locale-defaults.cpp'),'-o',str(binary)],check=True)
 subprocess.run([str(binary)],check=True,env={**os.environ,'ASAN_OPTIONS':'detect_leaks=0:halt_on_error=1','UBSAN_OPTIONS':'halt_on_error=1'})
