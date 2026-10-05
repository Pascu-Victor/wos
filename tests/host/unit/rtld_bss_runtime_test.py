"""Execute actual DSO loader and PHDR validation using host mapping endpoints.

Check partial-page BSS clearing, untouched anonymous pages, byte/protection/
TLS/RELRO metadata, backing-file preservation and early validation errors.
The optional source override supports old-source and semantic negative controls.
Kernel demand-zero/resource admission and complete linking remain runtime gates.
"""
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def function(text, signature):
    start = text.index(signature)
    brace = text.index('{', start)
    depth = 1
    end = brace + 1
    while depth:
        depth += (text[end] == '{') - (text[end] == '}')
        end += 1
    return text[start:end]


PREFIX = r'''
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <elf.h>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <climits>
#include <cstdio>
#include <string>
#include <vector>
#include <type_traits>
#include <algorithm>
#ifndef PT_RISCV_ATTRIBUTES
#define PT_RISCV_ATTRIBUTES 0x70000003
#endif
#define __ensure(x) do { if (!(x)) std::abort(); } while (0)
using elf_ehdr = Elf64_Ehdr;
using elf_phdr = Elf64_Phdr;
using elf_dyn = Elf64_Dyn;
constexpr int ELF_MACHINE = EM_X86_64;
constexpr int ELF_CLASS = ELFCLASS64;
enum class LinkerError {success, fileTooShort, notElf, wrongElfType, invalidProgramHeader, outOfMemory};
namespace frg {
struct success_t {};
constexpr success_t success{};
constexpr int endlog = 0;
template<typename E, typename T> struct expected {
 E error;
 expected(E e) : error(e) {}
 expected(success_t) : error(E::success) {}
};
}
struct Log {
 template<typename T> Log &operator<<(T const &) { return *this; }
};
struct VmMap {}; struct VmProtect {}; struct VmReadahead {};
struct MapCall {uintptr_t address; size_t size; int prot; int flags;};
std::vector<MapCall> maps;
std::vector<MapCall> protections;
std::vector<std::pair<uintptr_t,size_t>> clears;
size_t read_calls = 0;
bool fail_allocate = false;
bool fail_reserve = false;
std::vector<void *> phdr_allocations;
struct Allocator {
 void *allocate(size_t n) {
  void *p=fail_allocate ? nullptr : std::malloc(n);
  if(p)phdr_allocations.push_back(p);
  return p;
 }
 void deallocate(void *p, size_t) {
  auto i=std::find(phdr_allocations.begin(),phdr_allocations.end(),p);
  assert(i!=phdr_allocations.end());phdr_allocations.erase(i);std::free(p);
 }
};
Allocator &getAllocator() { static Allocator a; return a; }
int map_sysdep(void *hint, size_t size, int prot, int flags, int fd, off_t offset, void **out) {
 if (prot == PROT_NONE && fail_reserve) return ENOMEM;
 void *p = ::mmap(hint,size,prot,flags,fd,offset);
 if (p == MAP_FAILED) return errno;
 *out = p;
 maps.push_back({reinterpret_cast<uintptr_t>(p),size,prot,flags});
 return 0;
}
int protect_sysdep(void *p, size_t n, int prot) {
 if(::mprotect(p,n,prot))return errno;
 protections.push_back({reinterpret_cast<uintptr_t>(p),n,prot,0});return 0;
}
namespace mlibc {
constexpr size_t page_size = 4096;
Log infoLogger() { return {}; }
template<typename T> constexpr bool IsImplemented = std::is_same_v<T,VmProtect>;
template<typename Tag, typename... Args> int sysdep(Args... args) {
 if constexpr (std::is_same_v<Tag,VmMap>) return map_sysdep(args...);
 if constexpr (std::is_same_v<Tag,VmProtect>) return protect_sysdep(args...);
 return 0;
}
template<typename Tag, typename... Args> int sysdep_or_panic(Args... args) { return sysdep<Tag>(args...); }
template<typename Tag, typename... Args> int sysdep_or_enosys(Args... args) { return sysdep<Tag>(args...); }
}
bool tryPreadExactly(int fd, int64_t off, void *p, size_t n, const char *) {
 ++read_calls;
 size_t got = 0;
 while (got < n) {
  ssize_t r = ::pread(fd,static_cast<char *>(p)+got,n-got,off+got);
  if (r <= 0) return false;
  got += static_cast<size_t>(r);
 }
 return true;
}
struct SharedObject {
 bool isMainObject = false;
 std::string name = "fixture", path = "fixture";
 uintptr_t baseAddress = 0;
 const void *phdrPointer = nullptr;
 size_t phdrCount = 0, phdrEntrySize = 0;
 uintptr_t relroStart = 0;
 size_t relroSize = 0, tlsSegmentSize = 0, tlsAlignment = 0, tlsImageSize = 0;
 void *tlsImagePtr = nullptr;
 elf_dyn *dynamic = nullptr;
};
struct ObjectRepository { frg::expected<LinkerError,void> _fetchFromFile(SharedObject *, int); };
struct { bool debug = false; } rtldConfig;
void *tracked_memset(void *p, int value, size_t n) {
 assert(value == 0);
 clears.emplace_back(reinterpret_cast<uintptr_t>(p),n);
 return std::memset(p,value,n);
}
#define memset tracked_memset
'''

SUFFIX = r'''
#undef memset
struct Fixture {
 std::vector<unsigned char> bytes;
 size_t misalign, file_size, bss_size;
 elf_ehdr header{};
 elf_phdr phdrs[5]{};
 Fixture(size_t m, size_t f, size_t b, uint32_t flags=PF_R|PF_W) : bytes(0x4000+m+f+4096,0xa7), misalign(m), file_size(f), bss_size(b) {
  std::memcpy(header.e_ident,"\177ELF",4); header.e_ident[EI_CLASS]=ELFCLASS64;
  header.e_type=ET_DYN;header.e_machine=EM_X86_64;header.e_phoff=sizeof(header);
  header.e_phnum=5;header.e_phentsize=sizeof(elf_phdr);
  phdrs[0]={PT_LOAD,PF_R|PF_W,0,0,0,4096,4096,4096};
  phdrs[1]={PT_LOAD,flags,0x4000+m,0x4000+m,0,f,f+b,4096};
  phdrs[2]={PT_DYNAMIC,PF_R|PF_W,0x300,0x300,0,sizeof(elf_dyn),sizeof(elf_dyn),8};
  phdrs[3]={PT_TLS,PF_R,0x380,0x380,0,16,32,16};
  phdrs[4]={PT_GNU_RELRO,PF_R,0,0,0,0,4096,1};
  sync();
 }
 void sync() {
  std::memcpy(bytes.data(),&header,sizeof(header));
  std::memcpy(bytes.data()+sizeof(header),phdrs,sizeof(phdrs));
  std::memset(bytes.data()+0x300,0,sizeof(elf_dyn));
 }
 int open_file() {
  int fd=::memfd_create("rtld-bss-proof",0);assert(fd>=0);
  assert(::write(fd,bytes.data(),bytes.size())==static_cast<ssize_t>(bytes.size()));return fd;
 }
};
void cleanup(SharedObject &obj) {
 if (!maps.empty()) assert(::munmap(reinterpret_cast<void *>(maps[0].address),maps[0].size)==0);
 // Real SharedObject has no owning PHDR destructor. On reservation failure,
 // _fetchFromFile already frees the buffer; the rejected object's field is unused.
 if (obj.phdrPointer && std::find(phdr_allocations.begin(),phdr_allocations.end(),obj.phdrPointer)!=phdr_allocations.end())
  getAllocator().deallocate(const_cast<void *>(obj.phdrPointer),0);
 assert(phdr_allocations.empty());
 maps.clear();protections.clear();clears.clear();read_calls=0;
}
void valid_case(size_t m, size_t f, size_t b, uint32_t flags) {
 Fixture fixture(m,f,b,flags);int fd=fixture.open_file();SharedObject obj;ObjectRepository repository;
 auto result=repository._fetchFromFile(&obj,fd);assert(result.error==LinkerError::success);
 assert(read_calls == 5+(f!=0)+(MLIBC_MAP_DSO_SEGMENTS ? 0 : 1+(f+b!=0)));
 size_t backed=f ? ((m+f+4095)&~size_t{4095}) : 0;
 size_t tail=backed ? std::min(b,backed-m-f) : 0;
 size_t total=(m+f+b+4095)&~size_t{4095};
 size_t cleared=0;for(auto const &c:clears)cleared+=c.second;
#if MLIBC_MAP_DSO_SEGMENTS
#if RTLD_CHECK_LAZY
 assert(cleared==tail && "only declared BSS inside the file-backed tail may be cleared");
 for(auto const &call:maps) if ((call.flags&MAP_ANONYMOUS) && (call.flags&MAP_FIXED)) {
  std::vector<unsigned char> resident(call.size/4096);
  assert(::mincore(reinterpret_cast<void *>(call.address),call.size,resident.data())==0);
  assert(std::all_of(resident.begin(),resident.end(),[](unsigned char r){return !(r&1);}) && "anonymous BSS pages were eagerly materialized");
 }
#endif
#else
 assert(cleared==0);
#endif
 auto start=reinterpret_cast<unsigned char *>(obj.baseAddress+0x4000+m);
 for(size_t i=0;i<f;++i) assert(start[i]==0xa7 && "file payload changed");
 for(size_t i=0;i<b;++i) assert(start[f+i]==0 && "declared BSS byte is not zero");
#if MLIBC_MAP_DSO_SEGMENTS
 if (f && m+f+b<backed)for(size_t i=m+f+b;i<backed;++i)
  assert(reinterpret_cast<unsigned char *>(obj.baseAddress+0x4000)[i]==0xa7 && "file page bytes beyond declared BSS changed");
#endif
 std::vector<unsigned char> backing(fixture.bytes.size());
 assert(::pread(fd,backing.data(),backing.size(),0)==static_cast<ssize_t>(backing.size()));
 assert(backing==fixture.bytes && "private loading modified the backing file");
 assert(obj.dynamic==reinterpret_cast<elf_dyn *>(obj.baseAddress+0x300));
 assert(obj.phdrCount==5 && obj.phdrEntrySize==sizeof(elf_phdr));
 assert(obj.tlsSegmentSize==32 && obj.tlsImageSize==16 && obj.tlsAlignment==16);
 assert(obj.tlsImagePtr==reinterpret_cast<void *>(obj.baseAddress+0x380));
 assert(obj.relroStart==0 && obj.relroSize==4096);
 assert(!maps.empty() && maps[0].prot==PROT_NONE);
 for(size_t i=1;i<maps.size();++i)assert((maps[i].flags&MAP_FIXED) && (maps[i].flags&MAP_PRIVATE));
 int expected_prot=PROT_READ|((flags&PF_W)?PROT_WRITE:0)|((flags&PF_X)?PROT_EXEC:0);
 bool needs_protection=(f+b!=0) && (expected_prot!=(PROT_READ|PROT_WRITE)) && (!MLIBC_MAP_DSO_SEGMENTS || b!=0);
 assert(protections.size()==static_cast<size_t>(needs_protection));
 if(needs_protection)assert(protections[0].address==obj.baseAddress+0x4000 && protections[0].size==total && protections[0].prot==expected_prot);
 cleanup(obj);assert(::close(fd)==0);
}
void malformed_cases() {
 for(int which=0;which<7;++which) {
  Fixture f(0,4096,8192);
  if(which==0) f.header.e_ident[0]=0;
  if(which==1) f.header.e_phentsize=1;
  if(which==2) f.phdrs[1].p_memsz=1;
  if(which==3) f.phdrs[1].p_vaddr=UINT64_MAX-4095;
  if(which==4) f.phdrs[1].p_flags=PF_W|PF_X;
  if(which==5) f.phdrs[1].p_offset=f.bytes.size(); // truncated declared payload.
  f.sync();int fd=f.open_file();if(which==6)assert(::ftruncate(fd,12)==0);
  SharedObject obj;ObjectRepository repository;auto result=repository._fetchFromFile(&obj,fd);
  assert(result.error!=LinkerError::success && maps.empty());cleanup(obj);assert(::close(fd)==0);
 }
 Fixture f(0,1,8192);int fd=f.open_file();SharedObject obj;ObjectRepository repository;
 fail_allocate=true;auto a=repository._fetchFromFile(&obj,fd);assert(a.error==LinkerError::outOfMemory && maps.empty());fail_allocate=false;cleanup(obj);
 fail_reserve=true;auto b=repository._fetchFromFile(&obj,fd);assert(b.error==LinkerError::outOfMemory && maps.empty());fail_reserve=false;cleanup(obj);assert(::close(fd)==0);
}
int main() {
 size_t count=0;
 for(uint32_t flags:{PF_R|PF_W,PF_R,PF_R|PF_X})for(size_t m:{0u,1u,63u,4095u})for(size_t f:{0u,1u,63u,4095u,4096u,4097u,8192u})
  for(size_t b:{0u,1u,31u,4095u,4096u,4097u,65537u}){valid_case(m,f,b,flags);++count;}
 malformed_cases();std::printf("PASS actual loader/validation %zu byte/tail/lazy/private cases +9 early-error cases\n",count);
}
'''


def main():
    compiler = next((p for p in ("/usr/bin/clang++", "/usr/bin/g++") if Path(p).is_file()), None)
    assert compiler, "a native C++ compiler is required"
    path = Path(os.environ.get("WOS_RTLD_BSS_SOURCE", ROOT / "toolchain/src/mlibc/options/rtld/generic/linker.cpp"))
    text = path.read_text()
    start = text.index("constexpr uint64_t dsoPageSize")
    end = text.index("\n} // namespace", start)
    code = PREFIX + "\n" + text[start:end] + "\n" + function(text, "frg::expected<LinkerError, void> ObjectRepository::_fetchFromFile") + "\n" + SUFFIX
    with tempfile.TemporaryDirectory(prefix="wos-rtld-bss-") as folder:
        work = Path(folder)
        cpp = work / "actual.cpp"
        cpp.write_text(code)
        environment = dict(os.environ, ASAN_OPTIONS="detect_leaks=0", UBSAN_OPTIONS="halt_on_error=1")
        for segments in (1, 0):
            binary = work / f"segments-{segments}"
            subprocess.run([compiler, "-std=c++23", "-O2", "-g", "-fsanitize=address,undefined", "-fno-sanitize-recover=all", "-fno-omit-frame-pointer", "-DMLIBC_MMAP_ALLOCATE_DSO=1", f"-DMLIBC_MAP_DSO_SEGMENTS={segments}", "-DRTLD_CHECK_LAZY=1", str(cpp), "-o", str(binary)], check=True)
            subprocess.run([str(binary)], check=True, env=environment)


if __name__ == "__main__":
    main()
