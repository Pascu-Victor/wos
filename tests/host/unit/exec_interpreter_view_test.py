#!/usr/bin/env python3
"""Compile actual native ELF preparation, validation and debug registry bodies.

Controlled VFS/MM endpoints test layout, retries, ownership and registry lifetime;
this does not emulate task publication or qualify kernel runtime. Default input
is a generated ELF. --elf additionally accepts the current real interpreter.
--main-elf accepts a real ordinary-sized dynamic executable. --negative-controls
also compiles semantic mutants of the optional main-image adapter.
--baseline-exec enables old/new default-main comparison against a saved source.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

from exec_trace_ownership_test import function

R = Path(os.environ.get("WOS_INTERPRETER_VIEW_TEST_ROOT", Path(__file__).resolve().parents[3]))
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--elf", type=Path)
parser.add_argument("--main-elf", type=Path)
parser.add_argument("--negative-controls", action="store_true")
parser.add_argument("--baseline-exec", type=Path)
parser.add_argument("--exec-source", type=Path)
parser.add_argument("--loader-dir", type=Path)
parser.add_argument("--output", type=Path)
args = parser.parse_args()
temporary = tempfile.TemporaryDirectory(prefix="wos-interpreter-view-") if args.output is None else None
O = Path(temporary.name) if temporary is not None else args.output
if temporary is None:
    O.mkdir()
exec_source = args.exec_source or R / "modules/kern/src/syscalls_impl/process/exec.cpp"
loader_dir = args.loader_dir or R / "modules/kern/src/platform/loader"
inputs={}
def read(p):
 f=Path(p); data=f.read_bytes();inputs[str(f)]=hashlib.sha256(data).hexdigest();return data.decode()
def span(s,a,b):
 assert s.count(a)==1,(a,s.count(a));i=s.index(a);return s[i:s.index(b,i)]
new=read(exec_source)
old=read(args.baseline_exec) if args.baseline_exec else new
lold=read(loader_dir/'elf_loader.cpp');lnew=lold;h=read(loader_dir/'elf_loader.hpp')
inc=O/'include/defines';inc.mkdir(parents=True);(inc/'defines.hpp').write_text('#pragma once\n#include <cstdint>\n')
read(R/'modules/extern/elf.h');read(R/'modules/extern/elf-em.h')
prelude=r'''
#include <algorithm>
#include <array>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include <sys/types.h>
#include <extern/elf.h>
struct Allocation { void* p; size_t n; } allocations[256]{};
size_t live_bytes=0,live_count=0,largest=0,alloc_calls=0;int fail_after=-1;
void* operator new[](size_t n) {
 if(fail_after==0)return nullptr;if(fail_after>0)--fail_after;
 void* p=std::malloc(n?n:1);if(!p)return nullptr;
 for(auto& a:allocations)if(!a.p){a={p,n};live_bytes+=n;++live_count;++alloc_calls;largest=std::max(largest,n);return p;}
 std::abort();
}
void operator delete[](void* p) noexcept {
 if(!p)return;for(auto& a:allocations)if(a.p==p){live_bytes-=a.n;--live_count;a={};std::free(p);return;}std::abort();
}
void operator delete[](void* p,size_t) noexcept {operator delete[](p);}
void* operator new[](size_t n,const std::nothrow_t&) noexcept {return ::operator new[](n);}
void clear_stats(){assert(live_count==0);largest=alloc_calls=0;fail_after=-1;}
namespace ker::mod::mm {
 enum class PhysicalPageOwner { USER_EXECUTABLE_MAPPING };
 namespace user_layout { enum class ImageRole {MAIN,INTERPRETER};constexpr uint64_t IMAGE_ALIGNMENT=2ULL*1024*1024; }
 namespace virt { constexpr uint64_t PAGE_SIZE=4096;struct PageTable{};
 uint64_t translate(PageTable*,uint64_t a){return a^0x100000000ULL;}
 template<class...T>void map_page(T...){assert(false);}}
 namespace paging {constexpr uint64_t PAGE_SIZE=4096,PAGE_NX=1ULL<<63;namespace page_types {constexpr uint64_t USER_READONLY=5;} }
 namespace phys {enum class PageInitialization:uint8_t{ZERO,FULL_OVERWRITE};template<class...T>void* page_alloc_may_fail(T...){assert(false);return nullptr;} }
 namespace numa {
 // Registry-only path never allocates; this parameter endpoint exposes only
 // the destination index used in the forbidden physical-allocation branches.
 struct AllocationContext {uint64_t virtual_page_index{};};enum class AllocationError:uint8_t{NONE,EXHAUSTED};enum class ReclaimMode:uint8_t{NEVER};
 struct Span {bool empty()const{assert(false);return true;}void*data(){assert(false);return nullptr;}void*release(){assert(false);return nullptr;}};
 struct AllocationResult{Span span;AllocationError error{};};
 template<class...T>auto try_allocate_pages(T...)->AllocationResult{assert(false);return {};}
 }

}
namespace ker::vfs {
struct File {void* exec_read_view=reinterpret_cast<void*>(1);std::vector<uint8_t>* bytes{};size_t max_read=SIZE_MAX,calls=0;std::vector<ssize_t> plan;size_t plan_index=0;unsigned puts{};};
void vfs_put_file(File*f){assert(f&&f->puts==0);++f->puts;}
ssize_t vfs_pread_file(File* f,void* dst,size_t n,off_t offset){
 ++f->calls;assert(offset>=0);assert(static_cast<uint64_t>(offset)<=f->bytes->size());
 if(f->plan_index<f->plan.size()){ssize_t x=f->plan[f->plan_index++];if(x<=0)return x;n=std::min(n,static_cast<size_t>(x));}
 n=std::min({n,f->max_read,f->bytes->size()-static_cast<size_t>(offset)});std::memcpy(dst,f->bytes->data()+offset,n);return n;
}
}
namespace ker::mod::sched::task {struct Task {bool wki_prefer_inline=false;};}
namespace ker::loader::elf {
namespace mod=ker::mod;
constexpr uint64_t page_align_down(uint64_t a){return a&~uint64_t(4095);}
uint64_t hhdm_to_phys(uint64_t a){assert(false);return a;}
struct log {template<class...T>static void warn(const char*,T...){} };
namespace debug {
'''
dh=read(loader_dir/'debug_info.hpp')
ds=read(loader_dir/'debug_info.cpp')
prelude+=span(dh,'struct DebugSection {','// Global debug info registry')+"\nstd::vector<ProcessDebugInfo> debug_registry;\nstruct Lock {bool held=false;void lock(){assert(!held);held=true;}void unlock(){assert(held);held=false;}} debug_registry_lock;\n"+span(ds,'void register_process(','void add_debug_symbol(')+span(ds,'auto publish_staged_process(','ProcessDebugInfo* get_process_debug_info(')+span(ds,'void unregister_process(','}  // namespace ker::loader::debug')+r'''
using Row=std::tuple<uint64_t,std::string,uint64_t,uint64_t,uint64_t,uint64_t,uint32_t>;
std::vector<Row> snapshot(uint64_t pid){std::vector<Row> out;for(auto const& p:debug_registry)if(p.pid==pid)for(auto const& x:p.sections)out.emplace_back(pid,std::string(x.name),x.vaddr,x.paddr,x.size,x.file_offset,x.type);return out;}
template<class...T>void set_section_headers(T...){assert(false);}template<class...T>void set_string_table(T...){assert(false);}
}
'''
types=span(h,'struct TlsModule {','struct ElfLazyLoadRange {')+span(h,'using ElfReadAt =','auto load_elf(')
types+=span(h,'struct ElfLazyLoadRange {','using ElfLazyLoadRangeVec =')+'using ElfLazyLoadRangeVec = std::vector<ElfLazyLoadRange>;\n'
loader=function(lnew,'image_allocation_error')+'\n'+span(lold,'auto header_is_valid(','auto hhdm_to_phys(')+span(lold,'auto parse_elf(','auto view_elf(')+span(lold,'auto view_elf(','auto source_is_valid(').replace('auto view_elf(','auto baseline_view_elf(',1)+span(lnew,'auto view_elf(','auto source_is_valid(')+span(lold,'auto source_is_valid(','constexpr uint32_t SHT_RELR_VALUE')+span(lnew,'constexpr uint64_t HEADER_COPY_VADDR','struct LoadImageExtent {')
def prep(s):return span(s,'constexpr size_t EXEC_SPARSE_ELF_MIN_SIZE','struct ExecImageReadResult {')+span(s,'auto exec_range_in_file(','auto read_file_range_fully(')
code=('#define HAS_BASELINE\n' if args.baseline_exec else '')+prelude+types+loader+'\n}\n'
for label,s in [('baseline',old),('candidate',new)]:
 code+=f'namespace {label} {{\nnamespace loader=ker::loader;namespace vfs=ker::vfs;namespace mod=ker::mod;\n'+prep(s)+'\n}\n'
adapter=span(new,'auto prepare_execve_file_backed_elf(','// A synchronous, borrowed view')
execve=new[new.rindex('auto wos_proc_execve_impl('):new.rindex('auto finish_native_exec_image(')]
assert execve.count('prepare_execve_file_backed_elf(*task, exec_file, static_cast<size_t>(FILE_SIZE), prepared_elf, EXEC_LAZY_FILE_SEGMENTS)')==1
assert execve.count('bool const EXEC_LAZY_FILE_SEGMENTS =')==1
code+='namespace candidate {\n'+adapter+'\n}\n'
code+='namespace candidate {\n'+span(new,'    struct NativeScratch {','    auto native_scratch(')+'\n}\n'
code+=span(new,'constexpr int MAX_SHEBANG_DEPTH =','\n')+'\n'
code+=r'''
using namespace ker::loader::elf;
ker::mod::mm::numa::AllocationContext allocation_context{};int allocation_error=0;
using Role=ker::mod::mm::user_layout::ImageRole;
size_t cases=0,ready=0,fallback=0,invalid=0;
std::vector<uint8_t> payload;
Elf64_Ehdr& eh(std::vector<uint8_t>& b){return *reinterpret_cast<Elf64_Ehdr*>(b.data());}
Elf64_Phdr* ph(std::vector<uint8_t>& b){return reinterpret_cast<Elf64_Phdr*>(b.data()+eh(b).e_phoff);}
Elf64_Shdr* sh(std::vector<uint8_t>& b){return reinterpret_cast<Elf64_Shdr*>(b.data()+eh(b).e_shoff);}
bool original_valid(std::vector<uint8_t>& b,ElfFile& elf){return parse_elf(b.data(),b.size(),elf)&&source_is_valid(elf);}
void compare(std::vector<uint8_t> b,const char* label){
 clear_stats();ElfFile original{};bool valid=original_valid(b,original);
 ker::vfs::File f{.bytes=&b};
 {candidate::PreparedFileBackedElf p;auto outcome=candidate::prepare_file_backed_elf(&f,b.size(),p,Role::INTERPRETER);
 if(std::string(label)=="actual-ld")assert(outcome==candidate::PrepareFileBackedElfResult::READY);
 if(outcome==candidate::PrepareFileBackedElfResult::READY){++ready;auto view=view_elf(p.view);bool accepted=source_is_valid(view);
 if(accepted!=valid){std::fprintf(stderr,"COMPATIBILITY_MISMATCH %s old=%d new=%d\n",label,valid,accepted);std::abort();}
 if(valid){
 assert(p.task_metadata==nullptr);assert(p.view.source_metadata_stable);assert(view.source_pointers_stable);
 TlsModule a{},z{};assert(validated_tls_info(original,a)&&validated_tls_info(view,z));assert(std::memcmp(&a,&z,sizeof(a))==0);
 for(size_t off:{size_t(0),size_t(64),b.size()/2,b.size()-1}){std::array<uint8_t,33> a{},z{};size_t n=std::min(a.size(),b.size()-off);assert(read_source(original,off,a.data(),n));assert(read_source(view,off,z.data(),n));assert(a==z);}
 std::array<uint8_t,4> tmp{};assert(!read_source(view,UINT64_MAX,tmp.data(),4));assert(!read_source(view,b.size(),tmp.data(),1));assert(read_source(view,UINT64_MAX,nullptr,0));
 DebugMetadataPlan op{},vp{};assert(make_debug_metadata_plan(original,false,op));assert(make_debug_metadata_plan(view,false,vp));assert(!op.install&&!vp.install);
 debug::unregister_process(47);debug::register_process(47,"probe",0,0);assert(load_section_headers(original,nullptr,47,op,allocation_context,allocation_error));auto rows=debug::snapshot(47);debug::unregister_process(47);debug::register_process(47,"probe",0,0);assert(load_section_headers(view,nullptr,47,vp,allocation_context,allocation_error));assert(debug::snapshot(47)==rows);
 if(std::string(label)=="actual-ld") {std::printf("actual_ld logical=%zu metadata_and_registry=%zu largest=%zu bytes_read=%zu debug_rows=%zu\n",b.size(),live_bytes,largest,p.bytes_read,rows.size());assert(live_bytes<b.size()/100);assert(!rows.empty());}
 size_t const kept=p.view.section_names_size;p.reset();assert(live_bytes==kept&&live_count==(kept?1:0));assert(debug::snapshot(47)==rows);p.reset();assert(p.view.read_at==nullptr&&p.read_context.file==nullptr);
 }}else if(outcome==candidate::PrepareFileBackedElfResult::FALLBACK){++fallback;p.reset();assert(live_bytes==0);}else if(outcome==candidate::PrepareFileBackedElfResult::INVALID){++invalid;assert(!valid);}else{std::fprintf(stderr,"Unexpected outcome %s %d\n",label,int(outcome));std::abort();}}
 debug::unregister_process(47);assert(live_count==0);++cases;
}
void compare_main(std::vector<uint8_t> b,candidate::PrepareFileBackedElfResult expected=candidate::PrepareFileBackedElfResult::FALLBACK){
 clear_stats();ker::vfs::File z{.bytes=&b};
 {candidate::PreparedFileBackedElf now;auto y=candidate::prepare_file_backed_elf(&z,b.size(),now);assert(y==expected);
#ifdef HAS_BASELINE
 ker::vfs::File a{.bytes=&b};baseline::PreparedFileBackedElf old;auto x=baseline::prepare_file_backed_elf(&a,b.size(),old);
 assert(int(x)==int(y)&&old.bytes_read==now.bytes_read&&old.task_metadata_size==now.task_metadata_size&&a.calls==z.calls);
 if(y==candidate::PrepareFileBackedElfResult::READY)assert(std::memcmp(old.task_metadata,now.task_metadata,old.task_metadata_size)==0);
#endif
 if(y==candidate::PrepareFileBackedElfResult::READY){assert(!now.view.source_metadata_stable);assert(!view_elf(now.view).source_pointers_stable);assert(now.task_metadata_size>=sizeof(Elf64_Ehdr));uint8_t* taken=now.take_task_metadata();now.reset();assert(taken);delete[] taken;}}
 assert(live_count==0);++cases;
}
std::vector<uint8_t> synthetic(){
 std::vector<uint8_t> b(128*1024);auto& h=eh(b);std::memcpy(h.e_ident,"\177ELF",4);h.e_ident[EI_CLASS]=ELFCLASS64;h.e_ident[EI_DATA]=ELFDATA2LSB;h.e_ident[EI_VERSION]=EV_CURRENT;h.e_type=ET_DYN;h.e_machine=EM_X86_64;h.e_version=EV_CURRENT;h.e_entry=4096;h.e_ehsize=sizeof(h);h.e_phoff=sizeof(h);h.e_phentsize=sizeof(Elf64_Phdr);h.e_phnum=3;h.e_shoff=0x2100;h.e_shentsize=sizeof(Elf64_Shdr);h.e_shnum=4;h.e_shstrndx=3;
 auto* p=ph(b);p[0]={.p_type=PT_LOAD,.p_flags=PF_R,.p_filesz=4096,.p_memsz=4096,.p_align=4096};p[1]={.p_type=PT_LOAD,.p_flags=PF_R|PF_X,.p_offset=4096,.p_vaddr=4096,.p_filesz=4096,.p_memsz=4096,.p_align=4096};p[2]={.p_type=PT_GNU_STACK,.p_flags=PF_R|PF_W};
 constexpr char names[]="\0.text\0.debug_info\0.shstrtab";std::memcpy(b.data()+0x2000,names,sizeof(names));auto* s=sh(b);s[1]={.sh_name=1,.sh_type=SHT_PROGBITS,.sh_flags=SHF_ALLOC|SHF_EXECINSTR,.sh_addr=4096,.sh_offset=4096,.sh_size=4096,.sh_addralign=4096};s[2]={.sh_name=7,.sh_type=SHT_PROGBITS,.sh_offset=0x2050,.sh_size=8,.sh_addralign=1};s[3]={.sh_name=19,.sh_type=SHT_STRTAB,.sh_offset=0x2000,.sh_size=sizeof(names),.sh_addralign=1};ElfFile checked{};assert(original_valid(b,checked));return b;
}
std::vector<uint8_t> dynamic_main(){
 auto b=synthetic();ph(b)[2]={.p_type=PT_INTERP,.p_flags=PF_R,.p_offset=1024,.p_filesz=11,.p_memsz=11};
 std::memcpy(b.data()+1024,"/lib/ld.so",11);ElfFile parsed{};assert(original_valid(b,parsed));return b;
}
using Outcome=candidate::PrepareFileBackedElfResult;
void optional_main(std::vector<uint8_t> b,Outcome expected,bool lazy=true,bool inline_preferred=false,bool pinned=true){
 clear_stats();ker::vfs::File f{.exec_read_view=pinned?reinterpret_cast<void*>(1):nullptr,.bytes=&b};
 ker::mod::sched::task::Task task{.wki_prefer_inline=inline_preferred};
 {candidate::PreparedFileBackedElf p;auto result=candidate::prepare_execve_file_backed_elf(task,&f,b.size(),p,lazy);
 assert(result==expected);
 if(result==Outcome::FALLBACK){assert(live_count==0);assert(p.task_metadata==nullptr&&p.program_headers==nullptr&&p.view.read_at==nullptr);}
 if(!lazy||inline_preferred||!pinned||b.size()<=65536)assert(f.calls==0);
 }assert(live_count==0);++cases;
}
void compact_compatibility(std::vector<uint8_t> b,const char* label){
 clear_stats();ElfFile original{};assert(original_valid(b,original));ker::vfs::File f{.bytes=&b,.max_read=17};
 ker::mod::sched::task::Task task;
 {candidate::PreparedFileBackedElf p;assert(candidate::prepare_execve_file_backed_elf(task,&f,b.size(),p,true)==Outcome::READY);
 auto view=view_elf(p.view);assert(source_is_valid(view));assert(p.read_context.logical_size==b.size()&&p.view.logical_size==b.size());
 assert(p.view.source_metadata_stable&&view.source_pointers_stable&&p.view.contiguous_base==nullptr);
 assert(p.task_metadata_size==std::max(size_t(eh(b).e_phoff+eh(b).e_phnum*sizeof(Elf64_Phdr)),sizeof(Elf64_Ehdr)));
 assert(std::memcmp(p.task_metadata,b.data(),sizeof(Elf64_Ehdr))==0);
 assert(std::memcmp(p.task_metadata+eh(b).e_phoff,b.data()+eh(b).e_phoff,eh(b).e_phnum*sizeof(Elf64_Phdr))==0);
 TlsModule a{},z{};assert(validated_tls_info(original,a)&&validated_tls_info(view,z));assert(std::memcmp(&a,&z,sizeof(a))==0);
 for(size_t off:{size_t(0),size_t(64),b.size()/2,b.size()-1}){std::array<uint8_t,33> a{},z{};size_t n=std::min(a.size(),b.size()-off);assert(read_source(original,off,a.data(),n)&&read_source(view,off,z.data(),n));assert(a==z);}
 DebugMetadataPlan op{},vp{};assert(make_debug_metadata_plan(original,false,op)&&make_debug_metadata_plan(view,false,vp));
 debug::register_process(48,"main",0,0);assert(load_section_headers(original,nullptr,48,op,allocation_context,allocation_error));auto rows=debug::snapshot(48);debug::unregister_process(48);
 debug::register_process(48,"main",0,0);assert(load_section_headers(view,nullptr,48,vp,allocation_context,allocation_error));assert(debug::snapshot(48)==rows);
 std::printf("compact_main %s logical=%zu task_metadata=%zu scratch_and_registry=%zu largest=%zu bytes_read=%zu debug_rows=%zu\n",label,b.size(),p.task_metadata_size,live_bytes,largest,p.bytes_read,rows.size());
 assert(live_bytes<b.size()/10&&largest<b.size()/10);
 size_t kept=p.view.section_names_size;auto transferred=p.take_task_metadata();size_t transferred_size=p.task_metadata_size;
 auto callback=p.view.read_at;auto context=p.view.read_context;p.reset();assert(live_bytes==kept+transferred_size);assert(debug::snapshot(48)==rows);
 uint8_t out{};assert(!callback(context,0,&out,1));delete[] transferred;debug::unregister_process(48);assert(live_count==0);
 }assert(live_count==0);++cases;
}
void compact_main_cases(const char* actual){
 auto main=dynamic_main();compact_compatibility(main,"synthetic");
 if(std::strcmp(actual,"-")!=0){std::vector<uint8_t> b;{std::ifstream file(actual,std::ios::binary);assert(file);b.assign(std::istreambuf_iterator<char>(file),{});}assert(b.size()>65536&&b.size()<8*1024*1024);compact_compatibility(b,"actual-main");}
 optional_main(main,Outcome::READY);optional_main(main,Outcome::FALLBACK,false);optional_main(main,Outcome::FALLBACK,true,true);optional_main(main,Outcome::FALLBACK,true,false,false);
 clear_stats();{candidate::PreparedFileBackedElf p;ker::mod::sched::task::Task task;assert(candidate::prepare_execve_file_backed_elf(task,nullptr,main.size(),p,true)==Outcome::FALLBACK);assert(live_count==0);++cases;}
 for(size_t size:{size_t(0),size_t(63),size_t(65536),size_t(65537)}){auto b=main;b.resize(size);optional_main(b,size>65536?Outcome::READY:Outcome::FALLBACK);}
 optional_main(synthetic(),Outcome::FALLBACK);
 auto b=main;size_t off=eh(b).e_phoff,bytes=eh(b).e_phnum*sizeof(Elf64_Phdr);std::memmove(b.data()+off+1,b.data()+off,bytes);eh(b).e_phoff=off+1;optional_main(b,Outcome::FALLBACK);
 b=main;off=eh(b).e_shoff;bytes=eh(b).e_shnum*sizeof(Elf64_Shdr);std::memmove(b.data()+off+1,b.data()+off,bytes);eh(b).e_shoff=off+1;optional_main(b,Outcome::FALLBACK);
 b=main;auto& strings=sh(b)[eh(b).e_shstrndx];strings.sh_offset=b.size();strings.sh_size=4*1024*1024+1;b.resize(strings.sh_offset+strings.sh_size);ElfFile parsed{};assert(original_valid(b,parsed));optional_main(b,Outcome::FALLBACK);
 for(int index=0;index<5;++index){clear_stats();ker::vfs::File f{.bytes=&main};ker::mod::sched::task::Task task;fail_after=index;
 {candidate::PreparedFileBackedElf p;assert(candidate::prepare_execve_file_backed_elf(task,&f,main.size(),p,true)==(index<4?Outcome::FALLBACK:Outcome::READY));if(index<4)assert(live_count==0&&p.view.read_at==nullptr);}assert(live_count==0);++cases;}
 for(auto plan:std::vector<std::vector<ssize_t>>{{-5,-5,3,-5,-5,4},{-5,-5,-5},{0},{64,56,0}}){clear_stats();ker::vfs::File f{.bytes=&main,.max_read=17,.plan=plan};ker::mod::sched::task::Task task;
 {candidate::PreparedFileBackedElf p;assert(candidate::prepare_execve_file_backed_elf(task,&f,main.size(),p,true)==(plan.size()>4?Outcome::READY:Outcome::FALLBACK));if(plan.size()<=4)assert(live_count==0&&p.view.read_at==nullptr);}assert(live_count==0);++cases;}
 for(size_t offset=0;offset<64+eh(main).e_phnum*sizeof(Elf64_Phdr);offset+=3){b=main;b[offset]^=0x80;clear_stats();ker::vfs::File f{.bytes=&b};ker::mod::sched::task::Task task;
 {candidate::PreparedFileBackedElf p;auto result=candidate::prepare_execve_file_backed_elf(task,&f,b.size(),p,true);assert(result==Outcome::READY||result==Outcome::FALLBACK);if(result==Outcome::READY){ElfFile original{};bool valid=original_valid(b,original);assert(source_is_valid(view_elf(p.view))==valid);}else assert(live_count==0);}assert(live_count==0);++cases;}
 for(size_t size:{size_t(8*1024*1024),size_t(9*1024*1024)}){b=main;b.resize(size);clear_stats();ker::vfs::File f{.bytes=&b};ker::mod::sched::task::Task task;
 {candidate::PreparedFileBackedElf p;assert(candidate::prepare_execve_file_backed_elf(task,&f,b.size(),p,true)==Outcome::READY);}assert(live_count==0);++cases;
 eh(b).e_phoff=UINT64_MAX;clear_stats();ker::vfs::File bad{.bytes=&b};{candidate::PreparedFileBackedElf p;assert(candidate::prepare_execve_file_backed_elf(task,&bad,b.size(),p,true)==Outcome::INVALID);}assert(live_count==0);++cases;}
}
int main(int argc,char**argv){
 assert(argc==3);compact_main_cases(argv[2]);if(std::strcmp(argv[1],"-")!=0){std::ifstream file(argv[1],std::ios::binary);assert(file);payload.assign(std::istreambuf_iterator<char>(file),{});}else payload=synthetic();assert(payload.size()>65536&&payload.size()<8*1024*1024);
 compare(payload,"actual-ld");compare_main(payload);
 auto shifted=payload;size_t off=eh(shifted).e_phoff;size_t bytes=eh(shifted).e_phnum*sizeof(Elf64_Phdr);std::memmove(shifted.data()+off+1,shifted.data()+off,bytes);eh(shifted).e_phoff=off+1;compare(shifted,"misaligned-valid-program-table");
 shifted=payload;off=eh(shifted).e_shoff;bytes=eh(shifted).e_shnum*sizeof(Elf64_Shdr);shifted.resize(std::max(shifted.size(),off+bytes+1));std::memmove(shifted.data()+off+1,shifted.data()+off,bytes);eh(shifted).e_shoff=off+1;compare(shifted,"misaligned-valid-section-table");
 auto b=payload;b.resize(9*1024*1024);compare_main(b); // large interpreter without PT_INTERP still falls back for MAIN
 auto main=b;auto& mainhead=eh(main);auto* mainph=ph(main);auto& inter=mainph[mainhead.e_phnum-1];inter={};inter.p_type=PT_INTERP;inter.p_flags=PF_R;inter.p_offset=1024;inter.p_vaddr=1024;inter.p_filesz=11;inter.p_memsz=11;std::memcpy(main.data()+1024,"/lib/ld.so",11);
 compare_main(main,candidate::PrepareFileBackedElfResult::READY);compare(main,"main-with-interp");
 for(uint64_t n:{uint64_t(0),uint64_t(1),uint64_t(63),uint64_t(65536),uint64_t(65537)}){auto s=payload;s.resize(n);compare(s,"size-boundary");compare_main(s);}
 b=payload;eh(b).e_shnum=0;eh(b).e_shoff=0;eh(b).e_shstrndx=SHN_UNDEF;compare(b,"no-sections");
 b=payload;eh(b).e_shstrndx=SHN_UNDEF;for(size_t i=0;i<eh(b).e_shnum;++i)sh(b)[i].sh_name=0;compare(b,"unnamed-sections");
 b=payload;eh(b).e_shstrndx=SHN_UNDEF;compare(b,"undefined-with-names");
 b=payload;auto& strings=sh(b)[eh(b).e_shstrndx];strings.sh_offset=b.size();strings.sh_size=4*1024*1024+1;size_t end=strings.sh_offset+strings.sh_size;b.resize(end);ElfFile check{};assert(original_valid(b,check));compare(b,"valid-string-cap-fallback");
 b=main;auto& mainstrings=sh(b)[eh(b).e_shstrndx];mainstrings.sh_offset=b.size();mainstrings.sh_size=4*1024*1024+1;end=mainstrings.sh_offset+mainstrings.sh_size;b.resize(end);compare_main(b,candidate::PrepareFileBackedElfResult::INVALID);
 // Bitwise mutations hit actual header, PHDR and section validation without duplicating their logic.
 for(size_t offset=0;offset<64+eh(payload).e_phnum*sizeof(Elf64_Phdr);offset+=3){b=payload;b[offset]^=0x80;compare(b,"header-or-phdr-mutation");compare_main(b);}
 for(size_t i=0;i<eh(payload).e_shnum;++i){b=payload;auto* sections=sh(b);reinterpret_cast<uint8_t*>(&sections[i])[i%sizeof(Elf64_Shdr)]^=0x80;compare(b,"section-mutation");}
 for(size_t index=0;index<4;++index){clear_stats();ker::vfs::File f{.bytes=&payload};fail_after=index;{candidate::PreparedFileBackedElf p;auto o=candidate::prepare_file_backed_elf(&f,payload.size(),p,Role::INTERPRETER);if(index<3)assert(o==candidate::PrepareFileBackedElfResult::OUT_OF_MEMORY);else assert(o==candidate::PrepareFileBackedElfResult::READY);p.reset();p.reset();assert(live_count==0);}++cases;}
 for(auto plan:std::vector<std::vector<ssize_t>>{{-5,-5,3,-5,-5,4},{-5,-5,-5},{0},{64,56,0}}){clear_stats();ker::vfs::File f{.bytes=&payload,.max_read=17,.plan=plan};{candidate::PreparedFileBackedElf p;auto o=candidate::prepare_file_backed_elf(&f,payload.size(),p,Role::INTERPRETER);bool expected=plan[0]==-5&&plan.size()>3;assert(o==(expected?candidate::PrepareFileBackedElfResult::READY:candidate::PrepareFileBackedElfResult::IO_ERROR));p.reset();assert(live_count==0);}++cases;}
 clear_stats();{ker::vfs::File f{.exec_read_view=nullptr,.bytes=&payload};candidate::PreparedFileBackedElf p;assert(candidate::prepare_file_backed_elf(&f,payload.size(),p,Role::INTERPRETER)==candidate::PrepareFileBackedElfResult::FALLBACK);assert(f.calls==0);++cases;}
 // Callback fails closed after explicit metadata release; source callbacks are used only before close.
 clear_stats();{ker::vfs::File f{.bytes=&payload};candidate::PreparedFileBackedElf p;assert(candidate::prepare_file_backed_elf(&f,payload.size(),p,Role::INTERPRETER)==candidate::PrepareFileBackedElfResult::READY);auto callback=p.view.read_at;void* context=p.view.read_context;p.reset();std::array<uint8_t,1> out{};assert(!callback(context,0,out.data(),1));++cases;}
 clear_stats();{ker::vfs::File f{.bytes=&payload,.plan={-13,-13,-13}};candidate::PreparedFileBackedElf p;assert(candidate::prepare_file_backed_elf(&f,payload.size(),p,Role::INTERPRETER)==candidate::PrepareFileBackedElfResult::IO_ERROR);assert(p.read_context.error==-13);assert(f.calls==3);p.reset();++cases;}
 clear_stats();{ker::vfs::File f{.bytes=&payload};candidate::PreparedFileBackedElf p;assert(candidate::prepare_file_backed_elf(&f,payload.size(),p,Role::INTERPRETER)==candidate::PrepareFileBackedElfResult::READY);f.plan={-11,-11,-11};f.plan_index=0;auto elf=view_elf(p.view);uint8_t out{};assert(!read_source(elf,0,&out,1));assert(p.read_context.error==-11);f.plan.clear();f.plan_index=0;assert(read_source(elf,0,&out,1));assert(p.read_context.error==0);p.reset();++cases;}
 clear_stats();{
 const char names[]="name";const char* retained=reinterpret_cast<const char*>(1);assert(debug::retain_section_names(123,names,sizeof(names),retained));assert(retained==nullptr&&live_count==0);
 assert(!debug::retain_section_names(123,nullptr,5,retained));assert(!debug::retain_section_names(123,names,0,retained));assert(!debug::retain_section_names(123,names,0x400001,retained));
 debug::register_process(47,"old",0,0);fail_after=0;assert(!debug::retain_section_names(47,names,sizeof(names),retained));assert(retained==nullptr&&live_count==0);fail_after=-1;
 assert(debug::retain_section_names(47,names,sizeof(names),retained));const char* old_name=retained;debug::add_debug_section(47,old_name,1,2,3,4,5);
 for(uint64_t pid=100;pid<132;++pid){debug::register_process(pid,"other",0,0);assert(debug::retain_section_names(pid,names,sizeof(names),retained));debug::add_debug_section(pid,retained,1,2,3,4,5);assert(std::strcmp(old_name,"name")==0);}
 debug::register_process(99,"stage",0,0);char stage_names[]="stage";assert(debug::retain_section_names(99,stage_names,sizeof(stage_names),retained));const char* staged=retained;debug::add_debug_section(99,staged,1,2,3,4,5);stage_names[0]='X';assert(std::strcmp(staged,"stage")==0);
 assert(debug::publish_staged_process(99,47,"committed"));assert(std::get<1>(debug::snapshot(47)[0])=="stage");assert(std::strcmp(staged,"stage")==0);assert(live_count==33);
 debug::unregister_process(47);for(uint64_t pid=100;pid<132;++pid)debug::unregister_process(pid);assert(live_count==0);assert(debug::debug_registry.empty());
 debug::register_process(99,"stage-first",0,0);assert(debug::retain_section_names(99,names,sizeof(names),retained));debug::add_debug_section(99,retained,1,2,3,4,5);
 debug::register_process(47,"final-last",0,0);assert(debug::retain_section_names(47,names,sizeof(names),retained));debug::add_debug_section(47,retained,1,2,3,4,5);
 assert(!debug::publish_staged_process(555,47,"missing"));assert(debug::publish_staged_process(99,47,"committed"));assert(live_count==1);debug::unregister_process(47);debug::unregister_process(47);assert(live_count==0);cases+=8;
 }
 // Compile the actual NativeScratch destructor/reset with controlled vector storage.
 clear_stats();{
 auto frames=std::make_unique<std::array<candidate::NativeScratch,MAX_SHEBANG_DEPTH+1>>();
 for(size_t depth=0;depth<frames->size();++depth){auto& frame=frames->at(depth);frame.prepared_elf.task_metadata=new uint8_t[8]{};frame.prepared_elf.task_metadata[0]=static_cast<uint8_t>(depth+1);frame.prepared_interp.program_headers=new Elf64_Phdr[2]{};frame.prepared_interp.section_headers=new Elf64_Shdr[2]{};if(depth!=0)frame.prepared_interp.section_names=new char[16]{};frame.prepared_interp.view.program_headers=frame.prepared_interp.program_headers;frame.prepared_interp.view.read_context=&frame.prepared_interp.read_context;frame.argv_addrs=new uint64_t[8]{};if(depth!=0)frame.envp_addrs=new uint64_t[8]{};if(depth%2)frame.interp_buf=new uint8_t[64]{};}
 auto& first=frames->at(0);first.reset_interp_storage();first.reset_interp_storage();assert(first.prepared_interp.program_headers==nullptr&&first.prepared_interp.view.read_context==nullptr&&first.interp_buf==nullptr);
 auto& last=frames->at(MAX_SHEBANG_DEPTH);std::unique_ptr<uint8_t[]> transferred(last.prepared_elf.take_task_metadata());assert(transferred[0]==MAX_SHEBANG_DEPTH+1);
 // Drop the request owner explicitly; the transferred main metadata remains live.
 frames.reset();assert(live_count==1&&live_bytes==8);assert(transferred[0]==MAX_SHEBANG_DEPTH+1);transferred.reset();assert(live_count==0);cases+=MAX_SHEBANG_DEPTH+2;
 }
 for(int failure=0;failure<7;++failure){clear_stats();fail_after=failure;{candidate::NativeScratch frame;frame.prepared_interp.program_headers=new Elf64_Phdr[2]{};frame.prepared_interp.section_headers=new Elf64_Shdr[2]{};frame.prepared_interp.section_names=new char[16]{};frame.interp_buf=new uint8_t[64]{};frame.argv_addrs=new uint64_t[8]{};frame.envp_addrs=new uint64_t[8]{};frame.prepared_elf.task_metadata=new uint8_t[8]{};}assert(live_count==0);++cases;}
 clear_stats();
 // The actual interpreter scratch now owns and releases its retained File.
 {ker::vfs::File file;{candidate::NativeScratch frame;frame.interp_file=&file;frame.reset_interp_storage();frame.reset_interp_storage();assert(frame.interp_file==nullptr&&file.puts==1);}assert(file.puts==1);++cases;}
 assert(live_count==0);std::printf("PASS cases=%zu ready=%zu fallback=%zu invalid=%zu main_policy_oracle=1 debug_rows_copied=1\n",cases,ready,fallback,invalid);
}
'''
(O/'fixture.cpp').write_text(code)
ld=args.elf
if ld is not None: inputs[str(ld)]=hashlib.sha256(ld.read_bytes()).hexdigest()
if args.main_elf is not None: inputs[str(args.main_elf)]=hashlib.sha256(args.main_elf.read_bytes()).hexdigest()
(O/'inputs.json').write_text(json.dumps(inputs,indent=2)+'\n')
# Host runners can execute under ptrace, where LSan refuses to run. Exact
# allocation/registry ownership assertions above remain active in every vector.
env=os.environ.copy();env['TMPDIR']=str(O);env['ASAN_OPTIONS']='detect_leaks=0:abort_on_error=1';env['UBSAN_OPTIONS']='halt_on_error=1:print_stacktrace=1'
compiler=os.environ.get('WOS_HOST_CXX','/usr/bin/g++')
compile_flags=[compiler,'-std=c++23','-O1','-g','-fcheck-new','-fno-exceptions','-fsanitize=address,undefined','-fno-omit-frame-pointer','-I'+str(O/'include'),'-I'+str(R/'modules')]
fixture_args=[str(ld) if ld is not None else '-',str(args.main_elf) if args.main_elf is not None else '-']
commands=[compile_flags+[str(O/'fixture.cpp'),'-o',str(O/'fixture')],[str(O/'fixture')]+fixture_args]
receipts=[]
for i,cmd in enumerate(commands):
 started=time.monotonic();r=subprocess.run(cmd,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True);(O/f'step{i}.log').write_text(r.stdout);receipts.append({'argv':cmd,'exit':r.returncode,'wall_s':time.monotonic()-started});print(r.stdout[-7000:]);(O/'receipts.json').write_text(json.dumps(receipts,indent=2)+'\n')
 if r.returncode:raise SystemExit(r.returncode)
if args.negative_controls:
 mutations={
  'missing-admission':('EXEC_SPARSE_ELF_MIN_SIZE + 1','EXEC_FILE_BACKED_ELF_MIN_SIZE'),
  'ignored-inline':(' || task.wki_prefer_inline',''),
  'ignored-lazy':(' || !lazy_file_segments',''),
  'ignored-pin':('file->exec_read_view == nullptr || ','false || '),
  'missing-reset':('    out.reset();',''),
  'lost-debug-rows':('out.view.source_metadata_stable = true;','out.view.source_metadata_stable = false;'),
  'propagated-optional-failure':('return PrepareFileBackedElfResult::FALLBACK;','return RESULT;'),
  'ignored-phdr-alignment':('(HEADER.e_phoff % alignof(Elf64_Phdr)) == 0U','true'),
  'ignored-section-alignment':('(HEADER.e_shoff % alignof(Elf64_Shdr)) == 0U','true'),
  'changed-large-error-policy':(' || file_size >= EXEC_FILE_BACKED_ELF_MIN_SIZE',''),
 }
 mutants=[]
 for name,(before,after) in mutations.items():
  assert adapter.count(before)==1,(name,before,adapter.count(before))
  mutated=adapter.replace(before,after,1);source=O/(name+'.cpp');binary=O/name;source.write_text(code.replace(adapter,mutated,1))
  cmd=compile_flags+[str(source),'-o',str(binary)];started=time.monotonic();r=subprocess.run(cmd,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True);(O/(name+'-build.log')).write_text(r.stdout)
  assert r.returncode==0,(name,'mutant compile failed',r.stdout)
  r=subprocess.run([str(binary)]+fixture_args,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True);(O/(name+'-run.log')).write_text(r.stdout)
  assert r.returncode!=0 and 'Assertion' in r.stdout,(name,'semantic mutant did not fail an assertion',r.returncode,r.stdout)
  mutants.append({'name':name,'exit':r.returncode,'wall_s':time.monotonic()-started});(O/'mutants.json').write_text(json.dumps(mutants,indent=2)+'\n')
 print('SEMANTIC_MUTANTS_REJECTED '+str(len(mutants)))
print('ACTUAL_SOURCE_INTERPRETER_VIEW_AND_REGISTRY_PASS')
