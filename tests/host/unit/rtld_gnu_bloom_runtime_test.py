"""Execute the current RTLD resolver and scope methods with real frg headers.

Host SharedObject/table fixtures supply symbol/version metadata; this verifies
lookup semantics and early negative filtering, not whole-loader ABI or speed.
Both ELF word widths run under ASan/UBSan. Source overrides allow old-source
and semantic negative controls without editing the production file.
"""
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
RTLD = ROOT / "toolchain/src/mlibc/options/rtld"


def definition(text, signature):
    start = text.index(signature)
    brace = text.index("{", start)
    depth = 1
    end = brace + 1
    while depth:
        depth += (text[end] == "{") - (text[end] == "}")
        end += 1
    return text[start:end]


PREFIX = r'''
#include <sys/mman.h>
#include <unistd.h>
#include <elf.h>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <string>
#include <vector>
#include <frg/string.hpp>
#include <frg/optional.hpp>
#include <frg/tuple.hpp>
#include <frg/vector.hpp>
#if FIXTURE_ELF32
#include "x86/elf.hpp"
#else
#include "x86_64/elf.hpp"
#endif
#define __ensure(x) assert(x)
struct MemoryAllocator {
 void *allocate(size_t n) { auto p=std::malloc(n);assert(p);return p; }
 void deallocate(void *p,size_t) { std::free(p); }
 void free(void *p) { std::free(p); }
};
MemoryAllocator &getAllocator() { static MemoryAllocator a;return a; }
struct Log { template<class T> Log &operator<<(const T &) {return *this;} };
namespace frg { constexpr int endlog=0; }
namespace mlibc { Log infoLogger() {return {};} }
struct SharedObject;
uint32_t elf64Hash(frg::string_view);
uint32_t gnuHash(frg::string_view);
'''

OBJECT = r'''
struct SharedObject {
 uintptr_t baseAddress=0,hashTableOffset=0,symbolTableOffset=0,stringTableOffset=0;
 HashStyle hashStyle=HashStyle::none;
 bool isMainObject=false;
 uint64_t globalRts=0;
 std::vector<SymbolVersion> versions;
 size_t symbol_reads=0;
 frg::tuple<ObjectSymbol,SymbolVersion> getSymbolByIndex(size_t index) {
  ++symbol_reads;
  return {ObjectSymbol{this,reinterpret_cast<const elf_sym *>(baseAddress+symbolTableOffset)+index},versions.at(index)};
 }
};
'''

SUFFIX = r'''
struct Def {
 std::string name;
 unsigned bind=STB_GLOBAL;
 unsigned section=1;
 SymbolVersion version=SymbolVersion{1}.makeDefault();
};
struct Fixture {
 SharedObject object;
 std::vector<uint64_t> storage;
 std::vector<Def> defs;
 GnuHashTableHeader *header=nullptr;
 elf_addr *bloom=nullptr;
 uint32_t *buckets=nullptr,*chains=nullptr;
 elf_sym *symbols=nullptr;
 Fixture(std::vector<Def> input,HashStyle style=HashStyle::gnu,unsigned nb=5,unsigned nw=8,unsigned shift=26)
 :storage(32768),defs(std::move(input)) {
  object.baseAddress=reinterpret_cast<uintptr_t>(storage.data());
  object.hashStyle=style;object.hashTableOffset=64;
  object.stringTableOffset=32768;object.symbolTableOffset=65536;
  symbols=reinterpret_cast<elf_sym *>(object.baseAddress+object.symbolTableOffset);
  assert((defs.size()+1)*sizeof(elf_sym)<storage.size()*8-object.symbolTableOffset);
  object.versions.push_back(SymbolVersion{0});
  if(style==HashStyle::gnu && nb)std::stable_sort(defs.begin(),defs.end(),[&](auto &a,auto &b){return gnuHash(a.name.c_str())%nb<gnuHash(b.name.c_str())%nb;});
  size_t pos=1;
  auto strings=reinterpret_cast<char *>(object.baseAddress+object.stringTableOffset);
  for(size_t i=0;i<defs.size();++i) {
   const auto &d=defs[i];auto &s=symbols[i+1];
   assert(pos+d.name.size()+1<32768);
   s.st_name=pos;s.st_info=d.bind<<4;s.st_shndx=d.section;s.st_value=i+100;
   std::memcpy(strings+pos,d.name.c_str(),d.name.size()+1);pos+=d.name.size()+1;
   object.versions.push_back(d.version);
  }
  if(style==HashStyle::gnu) {
   header=reinterpret_cast<GnuHashTableHeader *>(object.baseAddress+object.hashTableOffset);
   *header={nb,1,nw,shift};bloom=reinterpret_cast<elf_addr *>(header+1);
   buckets=reinterpret_cast<uint32_t *>(bloom+nw);chains=buckets+nb;
   assert(reinterpret_cast<uintptr_t>(chains+defs.size())<object.baseAddress+object.stringTableOffset);
   constexpr unsigned bits=sizeof(elf_addr)*8;
   for(size_t i=0;i<defs.size();++i) {
    auto h=gnuHash(defs[i].name.c_str());
    if(nw && shift<32)bloom[(h/bits)%nw]|=(elf_addr{1}<<(h%bits))|(elf_addr{1}<<((h>>shift)%bits));
    if(nb) {
     auto b=h%nb;if(!buckets[b])buckets[b]=i+1;
     bool last=i+1==defs.size() || gnuHash(defs[i+1].name.c_str())%nb!=b;
     chains[i]=(h&~1u)|unsigned(last);
    }
   }
  } else if(style==HashStyle::systemV) {
   auto table=reinterpret_cast<uint32_t *>(object.baseAddress+object.hashTableOffset);
   table[0]=nb;table[1]=defs.size()+1;buckets=table+2;chains=buckets+nb;
   if(nb)for(size_t j=defs.size();j;--j) {
    auto b=elf64Hash(defs[j-1].name.c_str())%nb;chains[j]=buckets[b];buckets[b]=j;
   }
  }
 }
 frg::optional<ObjectSymbol> find(const char *s,frg::optional<SymbolVersion> v={}) {
  return resolveInObject(&object,s,v);
 }
 void expect(const char *s,bool found,frg::optional<SymbolVersion> v={}) {
  auto r=find(s,v);assert(bool(r)==found);
  if(r)assert(std::strcmp(r->getString(),s)==0);
 }
};

size_t checks=0;
void semantic_cases() {
 for(auto style:{HashStyle::gnu,HashStyle::systemV}) {
  Fixture f({{"normal"},{"weak",STB_WEAK},{"unique",STB_GNU_UNIQUE},{"local",STB_LOCAL},
   {"undefined",STB_GLOBAL,SHN_UNDEF},{"versioned",STB_GLOBAL,1,SymbolVersion{"V1"}},
   {"versioned",STB_GLOBAL,1,SymbolVersion{"V2"}.makeDefault()},
   {"versioned",STB_GLOBAL,1,SymbolVersion{"V3"}},
   {"hash_alias",STB_GLOBAL,1,SymbolVersion{"OTHER",123}}},style);
  for(auto name:{"normal","weak","unique"}){f.expect(name,true);++checks;}
  for(auto name:{"local","undefined","absent"}){f.expect(name,false);++checks;}
  f.expect("versioned",true);assert(f.find("versioned")->symbol()->st_value==f.find("versioned",SymbolVersion{"V2"})->symbol()->st_value);
  f.expect("versioned",true,SymbolVersion{"V1"});f.expect("versioned",true,SymbolVersion{"V3"});
  f.expect("versioned",false,SymbolVersion{"MISSING"});
  f.expect("normal",true,SymbolVersion{0});f.expect("normal",true,SymbolVersion{1});
  f.expect("versioned",true,SymbolVersion{0});f.expect("versioned",true,SymbolVersion{1});
  f.expect("hash_alias",false,SymbolVersion{"DIFFERENT",123});checks+=9;
  // Same bucket and hash (duplicate name) must continue after an ineligible
  // or nonmatching-version candidate, preserving the first eligible result.
  Fixture duplicates({{"dup",STB_GLOBAL,SHN_UNDEF},{"dup",STB_LOCAL},
   {"dup",STB_GLOBAL,1,SymbolVersion{"OLD"}}, {"dup",STB_WEAK}, {"dup",STB_GLOBAL}},style,1);
  auto r=duplicates.find("dup");assert(r && ELF_ST_BIND(r->symbol()->st_info)==STB_WEAK);
  duplicates.expect("dup",true,SymbolVersion{"OLD"});checks+=2;
  Fixture empty({},style);empty.expect("x",false);++checks;
  Fixture no_buckets({{"x"}},style,0);no_buckets.expect("x",false);++checks;
 }
 Fixture none({{"x"}},HashStyle::none);none.expect("x",false);++checks;
 for(unsigned field=0;field<3;++field) {
  Fixture f({{"x"}});if(field==0)f.object.hashTableOffset=0;
  if(field==1)f.object.symbolTableOffset=0;if(field==2)f.object.stringTableOffset=0;
  f.expect("x",false);++checks;
 }
 // Existing fallback accepts these headers. It must still find the symbol,
 // even with a zeroed filter; shifts >=32 must never reach shift arithmetic.
 for(unsigned nw:{0u,3u}) {
  Fixture f({{"fallback"}},HashStyle::gnu,1,nw);for(unsigned i=0;i<nw;++i)f.bloom[i]=0;
  f.expect("fallback",true);++checks;
 }
 for(unsigned shift:{32u,63u,0xffffffffu}) {
  Fixture f({{"fallback"}},HashStyle::gnu,1,8,shift);f.expect("fallback",true);++checks;
 }
}

void corpus_cases() {
 // Names cover every ELF32/64 bit, multiple filter words, multiple buckets,
 // empty buckets, collisions and a dense all-ones (false positive) filter.
 std::vector<Def> defs;
 for(unsigned i=0;i<500;++i)defs.push_back({"symbol_"+std::to_string(i)});
 for(unsigned shift:{0u,5u,26u,31u})for(unsigned nw:{1u,8u,128u}) {
  Fixture f(defs,HashStyle::gnu,67,nw,shift);
  for(auto &d:defs){f.expect(d.name.c_str(),true);++checks;}
  for(unsigned i=0;i<700;++i){auto name="missing_"+std::to_string(i);f.expect(name.c_str(),false);++checks;}
  for(unsigned i=0;i<nw;++i)f.bloom[i]=~elf_addr{0};
  for(auto &d:defs){f.expect(d.name.c_str(),true);++checks;}
  f.expect("false_positive_miss",false);++checks;
 }
 // GNU chains compare hash with the low bit masked. Different names can
 // have identical masked hashes, so the string check must remain decisive.
 Fixture same_hash({{"ab"},{"ac"}},HashStyle::gnu,1);
 assert((gnuHash("ab")&~1u)==(gnuHash("ac")&~1u));
 same_hash.expect("ab",true);same_hash.expect("ac",true);checks+=2;
}

void scope_cases() {
 Fixture main({{"x"}}),weak({{"x",STB_WEAK}}),strong({{"x"}}),future({{"y"}});
 main.object.isMainObject=true;main.object.globalRts=1;weak.object.globalRts=2;
 strong.object.globalRts=3;future.object.globalRts=9;
 Scope scope{true};for(auto p:{&main.object,&weak.object,&strong.object,&future.object})scope.appendObject(p);
 auto r=scope.resolveSymbol("x",99,0,{});assert(r && r->object()==&main.object);
 r=scope.resolveSymbol("x",99,Scope::resolveCopy,{});assert(r && r->object()==&weak.object);
 assert(!scope.resolveSymbol("y",3,Scope::skipGlobalAfterRts,{}));
 r=Scope::resolveGlobalOrLocalNext(scope,nullptr,"x",&main.object,{});assert(r && r->object()==&weak.object);
 r=Scope::resolveGlobalOrLocalNext(scope,nullptr,"x",&weak.object,{});assert(r && r->object()==&strong.object);
 assert(!Scope::resolveGlobalOrLocalNext(scope,nullptr,"x",&strong.object,{}));
 Fixture local({{"y"}});local.object.globalRts=0;Scope ls;ls.appendObject(&future.object);ls.appendObject(&local.object);
 r=Scope::resolveGlobalOrLocal(scope,&ls,"y",3,0,{});assert(r && r->object()==&local.object);
 r=Scope::resolveGlobalOrLocalNext(scope,&ls,"y",&future.object,{});assert(r && r->object()==&local.object);
 assert(!Scope::resolveGlobalOrLocalNext(scope,&ls,"x",&local.object,{}));checks+=9;
}

void guarded_negative() {
 long page=::sysconf(_SC_PAGESIZE);assert(page>0);
 auto mem=static_cast<char *>(::mmap(nullptr,page*2,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0));assert(mem!=MAP_FAILED);
 assert(::mprotect(mem+page,page,PROT_NONE)==0);
 // Header/filter readable; the bucket begins in a protected page. Returning
 // a miss proves no bucket/chain load is performed on negative filtering.
 auto h=reinterpret_cast<GnuHashTableHeader *>(mem+page-sizeof(GnuHashTableHeader)-sizeof(elf_addr));
 *h={1,1,1,26};
 SharedObject object;object.baseAddress=reinterpret_cast<uintptr_t>(mem);object.hashStyle=HashStyle::gnu;
 object.hashTableOffset=reinterpret_cast<uintptr_t>(h)-object.baseAddress;object.symbolTableOffset=1;object.stringTableOffset=1;
 constexpr unsigned bits=sizeof(elf_addr)*8;auto hash=gnuHash("absent");
 auto first=elf_addr{1}<<(hash%bits),second=elf_addr{1}<<((hash>>26)%bits);
 assert(first!=second);
 for(auto word:{elf_addr{0},first,second}) {
  *reinterpret_cast<elf_addr *>(h+1)=word;
  assert(!resolveInObject(&object,"absent",{}));assert(object.symbol_reads==0);++checks;
 }
 assert(::munmap(mem,page*2)==0);
}

int main() {
 semantic_cases();corpus_cases();scope_cases();
 if(EXPECT_FILTER)guarded_negative();
 std::printf("PASS actual RTLD GNU/SysV/versions/scope/ELF%zu: %zu checks; early-filter=%d\n",sizeof(elf_addr)*8,checks,EXPECT_FILTER);
}
'''


def main():
    compiler = next((p for p in ("/usr/bin/clang++", "/usr/bin/g++") if Path(p).is_file()), None)
    assert compiler, "a native C++ compiler is required"
    path = Path(os.environ.get("WOS_RTLD_BLOOM_SOURCE", RTLD / "generic/linker.cpp"))
    source = path.read_text()
    header = (RTLD / "generic/linker.hpp").read_text()
    frg = ROOT / "toolchain/src/mlibc/subprojects/frigg/include"
    assert (frg / "frg/string.hpp").is_file(), "initialized local frigg headers are required"
    code = PREFIX
    code += definition(header, "enum class HashStyle") + ";\n"
    for name in ("GnuHashTableHeader", "ObjectSymbol", "SymbolVersion"):
        code += definition(header, "struct " + name + " {") + ";\n"
    code += OBJECT + definition(header, "struct Scope {") + ";\n"
    for signature in ("uint32_t elf64Hash(", "uint32_t gnuHash(", "const char *ObjectSymbol::getString(",
                      "frg::optional<ObjectSymbol> resolveInObject(",
                      "frg::optional<ObjectSymbol> Scope::_resolveNext(",
                      "void Scope::appendObject(",
                      "frg::optional<ObjectSymbol> Scope::resolveGlobalOrLocal(",
                      "frg::optional<ObjectSymbol> Scope::resolveGlobalOrLocalNext(",
                      "frg::optional<ObjectSymbol> Scope::resolveSymbol("):
        code += definition(source, signature) + "\n"
    # Constructor has braced member initializers; do not confuse their braces
    # with its body while extracting this actual definition.
    code += source[source.index("ObjectSymbol::ObjectSymbol("):source.index("const char *ObjectSymbol::getString(")]
    code += source[source.index("Scope::Scope("):source.index("void Scope::appendObject(")]
    code += SUFFIX
    environment = dict(os.environ, ASAN_OPTIONS="detect_leaks=1", UBSAN_OPTIONS="halt_on_error=1")
    with tempfile.TemporaryDirectory(prefix="wos-rtld-bloom-") as folder:
        work = Path(folder)
        cpp = work / "actual.cpp"
        cpp.write_text(code)
        for elf32 in (0, 1):
            binary = work / f"elf-{32 if elf32 else 64}"
            subprocess.run([compiler, "-std=c++23", "-O2", "-g", "-fsanitize=address,undefined",
                            "-fno-sanitize-recover=all", "-fno-omit-frame-pointer",
                            f"-DFIXTURE_ELF32={elf32}",
                            f"-DEXPECT_FILTER={os.environ.get('WOS_RTLD_BLOOM_EXPECT_FILTER', '1')}",
                            "-I", str(frg), "-I", str(RTLD), str(cpp), "-o", str(binary)], check=True)
            subprocess.run([str(binary)], check=True, env=environment)


if __name__ == "__main__":
    main()
