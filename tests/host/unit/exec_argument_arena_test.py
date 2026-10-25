#!/usr/bin/env python3
"""Run actual exec snapshots against controlled usercopy and allocation endpoints.

Uses the real SmallVec and the actual generic C-string copier. Page pins and
task-exit handoff are modeled; this is not accepted remote execution or a guest
timing measurement.
"""
import os
from pathlib import Path
import re
import subprocess
import tempfile

from exec_trace_ownership_test import block, function

ROOT = Path(__file__).resolve().parents[3]
source = Path(os.environ.get("WOS_ARGUMENT_ARENA_TEST_SOURCE", ROOT / "modules/kern/src/syscalls_impl/process/exec.cpp")).read_text()
usercopy = (ROOT / "modules/kern/src/platform/sys/usercopy.cpp").read_text()
allow_old = os.environ.get("WOS_ARGUMENT_ARENA_ALLOW_OLD") == "1"
allow_vector_old = os.environ.get("WOS_EXEC_VECTOR_ALLOW_OLD") == "1"
constants = "\n".join(re.findall(r"^constexpr size_t EXEC_(?:PATH_MAX|ARG_BYTES_MAX|ARG_SMALL_BYTES) = .*;$", source, re.M))
code = r'''
#include <algorithm>
#include <array>
#include <cassert>
#include <cerrno>
#include <csetjmp>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>
#include <utility>
#include <vector>
#include <util/smallvec.hpp>
#include <platform/mm/paging.hpp>
namespace alloc {
bool tracking=false;size_t attempts=0,live=0,fail_at=0,max_size=0;
std::array<void*,128> pointers{};
void* get(size_t n){
 if(tracking){++attempts;max_size=std::max(max_size,n);if(attempts==fail_at)return nullptr;}
 void*p=std::malloc(n?n:1);assert(p);
 if(tracking){auto i=std::find(pointers.begin(),pointers.end(),nullptr);assert(i!=pointers.end());*i=p;++live;}
 return p;
}
void put(void*p){if(!p)return;auto i=std::find(pointers.begin(),pointers.end(),p);if(i!=pointers.end()){*i=nullptr;assert(live);--live;}std::free(p);}
void reset(size_t failure){assert(!live);attempts=max_size=0;fail_at=failure;}
}
void* operator new(size_t n){return alloc::get(n);}
void* operator new[](size_t n){return alloc::get(n);}
void* operator new(size_t n,const std::nothrow_t&)noexcept{return alloc::get(n);}
void* operator new[](size_t n,const std::nothrow_t&)noexcept{return alloc::get(n);}
void operator delete(void*p)noexcept{alloc::put(p);}
void operator delete[](void*p)noexcept{alloc::put(p);}
void operator delete(void*p,size_t)noexcept{alloc::put(p);}
void operator delete[](void*p,size_t)noexcept{alloc::put(p);}
void operator delete(void*p,const std::nothrow_t&)noexcept{alloc::put(p);}
void operator delete[](void*p,const std::nothrow_t&)noexcept{alloc::put(p);}
namespace memory {
struct Region{unsigned char*bytes{};size_t capacity{},logical{},fault_after=SIZE_MAX,mutated=0;bool mutate=false,is_vector=false;size_t next_read=0,read_limit=SIZE_MAX;};
std::array<Region,256> regions{};size_t count=0,raw_calls=0,value_calls=0,fail_raw=0,fail_value=0,fatal_raw=0,text_offset=0;
size_t vector_requests=0,vector_requested=0;
std::jmp_buf exit_target;
void clear(){for(size_t i=0;i<count;++i)std::free(regions[i].bytes);count=raw_calls=value_calls=vector_requests=vector_requested=0;fail_raw=fail_value=fatal_raw=0;}
uint64_t add(const void*data,size_t size){
 assert(count<regions.size());auto&r=regions[count++];r={};r.capacity=std::max(size_t{4096},(size+4095)&~size_t{4095});r.logical=size;r.fault_after=SIZE_MAX;
 void*p=nullptr;assert(!posix_memalign(&p,4096,r.capacity));r.bytes=static_cast<unsigned char*>(p);std::memset(p,0xa5,r.capacity);std::memcpy(p,data,size);return reinterpret_cast<uint64_t>(p);
}
uint64_t text(std::string const&s){std::string bytes(text_offset,'!');bytes+=s;return add(bytes.c_str(),bytes.size()+1)+text_offset;}
uint64_t vector(std::vector<std::string> const&v){std::vector<uint64_t>a;for(auto const&s:v)a.push_back(text(s));a.push_back(0);uint64_t p=add(a.data(),a.size()*8);regions[count-1].is_vector=true;return p;}
void pointer_read(Region&r,size_t offset,size_t size){if(!r.is_vector)return;++vector_requests;vector_requested+=size;assert(offset==r.next_read&&offset+size<=r.read_limit);r.next_read=offset+size;}
Region* find(uint64_t addr){for(size_t i=0;i<count;++i){auto&r=regions[i];uint64_t start=reinterpret_cast<uint64_t>(r.bytes);if(addr>=start&&addr-start<r.capacity)return &r;}return nullptr;}
}
namespace ker::mod::sched::task{struct Task{void*recoverable_arguments{};};}
namespace ker::mod::sys::usercopy {
namespace sched=ker::mod::sched;namespace mm=ker::mod::mm;
struct CopyResult{size_t bytes_copied{};bool fault{};};
enum class CStringCopyStatus:uint8_t{COMPLETE,FAULT,TOO_LONG};
auto range_valid(uint64_t addr,size_t size)->bool{uint64_t end=0;return !__builtin_add_overflow(addr,static_cast<uint64_t>(size),&end)&&addr<0x800000000000ULL&&end<=0x800000000000ULL;}
@CHUNK@
auto copy_from_task_partial(sched::task::Task&task,uint64_t addr,void*dst,size_t size)->CopyResult{
 assert(task.recoverable_arguments&&dst);++memory::raw_calls;
 if(memory::raw_calls==memory::fatal_raw)std::longjmp(memory::exit_target,1);
 if(memory::raw_calls==memory::fail_raw||!range_valid(addr,size))return {0,true};
 auto*r=memory::find(addr);if(!r)return {0,true};size_t offset=addr-reinterpret_cast<uint64_t>(r->bytes);
 assert(size<=4096-(addr&4095));
 memory::pointer_read(*r,offset,size);
 if(offset>=r->fault_after)return {0,true};
 size_t n=std::min({size,r->capacity-offset,r->fault_after-offset});
 assert(!r->mutated||offset>=r->mutated); // Captured prefix may never be read again.
 std::memcpy(dst,r->bytes+offset,n);
 if(r->mutate&&!r->mutated&&offset==0&&n<r->logical){r->mutated=n;std::memset(r->bytes,'Z',n);}
 return {n,n!=size};
}
auto copy_value_from_task(sched::task::Task&task,uint64_t addr,uint64_t&out)->bool{
 assert(task.recoverable_arguments);++memory::value_calls;if(memory::value_calls==memory::fail_value)return false;
 if(!range_valid(addr,8))return false;auto*r=memory::find(addr);if(!r)return false;size_t offset=addr-reinterpret_cast<uint64_t>(r->bytes);
 memory::pointer_read(*r,offset,8);
 if(8>r->capacity-offset||offset>=r->fault_after||8>r->fault_after-offset)return false;assert(!r->mutated||offset>=r->mutated);std::memcpy(&out,r->bytes+offset,8);
 if(r->mutate&&!r->mutated&&offset==0&&8<r->logical){r->mutated=8;std::memset(r->bytes,'Z',8);}
 return true;
}
@CSTRING@
}
namespace ker::syscall::process {
@CONSTANTS@
@SNAPSHOT@;
@STRING@
@VECTOR@
@ARGUMENTS@
}
namespace test {
using namespace ker::syscall::process;using Task=ker::mod::sched::task::Task;
struct Result{size_t allocations{},raw{},values{};};
Result one(std::vector<std::string>const&args,std::vector<std::string>const&env,int expected=0,
 bool null_args=false,bool null_env=false,bool short_case=false,size_t fail_alloc=0,size_t fail_raw=0,size_t fail_value=0,
 size_t fatal_raw=0,bool mutate=false,size_t first_arg_fault=SIZE_MAX,std::string const&path="/bin/true",size_t offset=0){
 memory::clear();memory::text_offset=offset;uint64_t path_addr=memory::text(path);size_t first_region=memory::count;
 uint64_t argv=null_args?0:memory::vector(args),envp=null_env?0:memory::vector(env);
 if(!null_args&&!args.empty()){memory::regions[first_region].mutate=mutate;memory::regions[first_region].fault_after=first_arg_fault;}
 auto*s=new ExecArgumentsSnapshot;Task task{s};alloc::reset(fail_alloc);memory::fail_raw=fail_raw;memory::fail_value=fail_value;memory::fatal_raw=fatal_raw;
 alloc::tracking=true;
 if(!setjmp(memory::exit_target)){
  int result=snapshot_exec_arguments(task,path_addr,argv,envp,*s);assert(!fatal_raw);assert(result==expected);
  if(!result){
   assert(std::strcmp(s->path.data(),path.c_str())==0);
   assert(null_args?(s->argv==nullptr):(s->argv!=nullptr&&s->argv[args.size()]==nullptr));
   assert(null_env?(s->envp==nullptr):(s->envp!=nullptr&&s->envp[env.size()]==nullptr));
   size_t budget=(args.size()+env.size()+2)*8;
   for(size_t i=0;i<args.size();++i){assert(std::strcmp(s->argv[i],args[i].c_str())==0);budget+=args[i].size()+1;}
   for(size_t i=0;i<env.size();++i){assert(std::strcmp(s->envp[i],env[i].c_str())==0);budget+=env[i].size()+1;}
   assert(s->budget_used==budget&&budget<=EXEC_ARG_BYTES_MAX);
   if(short_case&&!ALLOW_OLD)assert(alloc::max_size<=1024);
   // Input retirement cannot affect the owned output, including strings that
   // remain in the small arena after a later argument falls back.
   for(size_t i=0;i<memory::count;++i)std::memset(memory::regions[i].bytes,0,memory::regions[i].capacity);
   for(size_t i=0;i<args.size();++i)assert(std::strcmp(s->argv[i],args[i].c_str())==0);
   for(size_t i=0;i<env.size();++i)assert(std::strcmp(s->envp[i],env[i].c_str())==0);
  }
 }else assert(fatal_raw&&memory::raw_calls==fatal_raw);
 Result r{alloc::attempts,memory::raw_calls,memory::value_calls};alloc::tracking=false;delete s;assert(!alloc::live);return r;
}
void all_failures(std::vector<std::string>const&a,std::vector<std::string>const&e){
 Result r=one(a,e);
 for(size_t i=1;i<=r.allocations;++i)one(a,e,-ENOMEM,false,false,false,i);
 for(size_t i=1;i<=r.raw;++i){one(a,e,-EFAULT,false,false,false,0,i);one(a,e,0,false,false,false,0,0,0,i);}
 for(size_t i=1;i<=r.values;++i)one(a,e,-EFAULT,false,false,false,0,0,i);
}

void vector_case(size_t count,size_t offset=0,size_t accessible=SIZE_MAX,int expected=0,
 size_t fail_alloc=0,size_t read_limit=SIZE_MAX,bool mutate=false){
 memory::clear();memory::text_offset=0;std::vector<uint64_t>pointers;
 for(size_t i=0;i<count;++i)pointers.push_back(memory::text("v"));pointers.push_back(0);
 std::vector<unsigned char>bytes(offset+pointers.size()*8,0xa5);std::memcpy(bytes.data()+offset,pointers.data(),pointers.size()*8);
 uint64_t address=memory::add(bytes.data(),bytes.size())+offset;auto&r=memory::regions[memory::count-1];
 r.is_vector=true;r.next_read=offset;r.fault_after=accessible;r.read_limit=read_limit;r.mutate=mutate;
 size_t requests=0;
 {
  ExecArgumentsSnapshot snapshot;Task task{&snapshot};alloc::reset(fail_alloc);alloc::tracking=true;
  int result=snapshot_exec_vector(task,address,snapshot,snapshot.argv);alloc::tracking=false;assert(result==expected);
  if(!result){assert(snapshot.argv[count]==nullptr&&snapshot.budget_used==(count+1)*8+count*2);
   for(size_t i=0;i<count;++i)assert(std::strcmp(snapshot.argv[i],"v")==0);
   std::memset(r.bytes,0,r.capacity);for(size_t i=0;i<count;++i)assert(std::strcmp(snapshot.argv[i],"v")==0);
  }
  if(expected==-EFAULT&&accessible!=SIZE_MAX){size_t complete=(accessible-offset)/8;assert(snapshot.budget_used==complete*8);}
  requests=memory::vector_requested;
 }
 assert(!alloc::live);if(read_limit!=SIZE_MAX&&fail_alloc)assert(requests==read_limit-offset);
}
void vector_checks(){
 for(size_t n:{size_t{0},size_t{1},size_t{15},size_t{16},size_t{17},size_t{31},size_t{32},size_t{33},size_t{40},size_t{64},size_t{100}})vector_case(n);
 for(size_t n:{size_t{0},size_t{1},size_t{3},size_t{15}})vector_case(n,4096-(n+1)*8,4096,0,0,4096);
 for(size_t offset:{size_t{1},size_t{4088},size_t{4090},size_t{4094},size_t{4095}})vector_case(3,offset);
 vector_case(1,0,16);vector_case(1,0,15,-EFAULT);vector_case(2,0,19,-EFAULT);vector_case(3,0,24,-EFAULT);
 for(size_t n:{size_t{40},size_t{100}})vector_case(n,0,SIZE_MAX,0,0,SIZE_MAX,true);
 // Every real growth failure must be the last prefetched word. SmallVec's
 // total capacity doubles at sizes16,32,64,128; use200 pointers to cover all4.
 for(auto pair:{std::pair<size_t,size_t>{1,17}, {2,33},{3,65},{4,129}})
  vector_case(200,0,SIZE_MAX,-ENOMEM,pair.first,pair.second*8);

 for(size_t failure=1;failure<=6;++failure)vector_case(200,0,SIZE_MAX,-ENOMEM,failure);
 // An unterminated maximal vector consumes exactly the global pointer budget;
 // the next page/word must never be requested, and no string may be copied.
 memory::clear();memory::text_offset=0;
 std::vector<uint64_t>maximum(EXEC_ARG_BYTES_MAX/8,123);
 uint64_t maximum_addr=memory::add(maximum.data(),EXEC_ARG_BYTES_MAX);
 auto&maximum_region=memory::regions[memory::count-1];maximum_region.is_vector=true;maximum_region.read_limit=EXEC_ARG_BYTES_MAX;
 {
  ExecArgumentsSnapshot snapshot;Task task{&snapshot};alloc::reset(0);alloc::tracking=true;
  assert(snapshot_exec_vector(task,maximum_addr,snapshot,snapshot.argv)==-E2BIG);
  alloc::tracking=false;assert(snapshot.budget_used==EXEC_ARG_BYTES_MAX&&memory::vector_requested==EXEC_ARG_BYTES_MAX);
  assert(!snapshot.argv&&!snapshot.small_strings&&!snapshot.strings);
 }
 assert(!alloc::live);
 // Real before/new copier calls provide the work oracle, without changing
 // the functional/failure/fatal/arena checks for the old source.
 one({"true"},std::vector<std::string>(11,"E=x"));
 assert(ALLOW_VECTOR_OLD||memory::vector_requests<=2);
 std::puts(ALLOW_VECTOR_OLD?"Before vector semantic/readbudget/grow/partialword/page/fatal oracle PASS; batch-call assertion disabled":
 "Actual vector semantic/readbudget/grow/partialword/page/mutation/failure/fatal oracle PASS; common14 pointer reads captured in2 calls");
}

int run(){
 one({"true"},{"K=value"},0,false,false,true);
 one({}, {},0,true,true,true);one({}, {},0,false,false,true);
 one({""},{""},0,false,false,true);
 for(size_t n:{size_t{0},size_t{1},size_t{1000},size_t{1022},size_t{1023},size_t{1024},size_t{1025},size_t{4095},size_t{4096},size_t{8192}})
  one({std::string(n,'a')},{"E=tail"});
 one({"first",std::string(1200,'b'),"third"},{"E=stable"});
 one({std::string(800,'a'),std::string(600,'b')},{"E=stable"});
 one({std::string(18000,'x')},{"E=stable"},0,false,false,false,0,0,0,0,true);
 for(size_t offset:{size_t{1},size_t{1023},size_t{4094},size_t{4095}})
  one({std::string(8193,'a'),"tail"},{"E=page"},0,false,false,false,0,0,0,0,false,SIZE_MAX,"/bin/true",offset);
 one({std::string(1023,'a')},{},0,false,false,true);
 std::vector<std::string>many(40,"a");one(many,{"E=v"},0,false,false,true);
 one({std::string(EXEC_ARG_BYTES_MAX-25,'a')},{},0,false,true);
 one({std::string(EXEC_ARG_BYTES_MAX-24,'a')},{},-E2BIG,false,true);
 one({std::string(EXEC_ARG_BYTES_MAX,'a')},{},-E2BIG);
 one({"a"},{"E=v"},-ENAMETOOLONG,false,false,false,0,0,0,0,false,SIZE_MAX,std::string(EXEC_PATH_MAX,'p'));
 // Preserve generic C-string semantics: a NUL in a partially copied range
 // completes the string even when the remainder of that copy faults.
 one({"abc"},{"E=v"},0,false,false,false,0,0,0,0,false,4);
 one({"abcdef"},{"E=v"},-EFAULT,false,false,false,0,0,0,0,false,4);
 all_failures({"true"},{"K=value"});all_failures({"first",std::string(1200,'b'),"third"},{"E=v"});
 all_failures({std::string(18000,'x')},{"E=v"});all_failures(many,{"E=v"});
 // Direct vector checks exercise the budget guard before another pointer read.
 memory::clear();memory::text_offset=0;std::vector<uint64_t>v{123,0};uint64_t addr=memory::add(v.data(),16);memory::regions[memory::count-1].is_vector=true;memory::regions[memory::count-1].read_limit=8;Task task{};ExecArgumentsSnapshot s;task.recoverable_arguments=&s;
 assert(snapshot_exec_arguments(task,0,0,0,s)==-EFAULT);
 s.budget_used=EXEC_ARG_BYTES_MAX-8;assert(snapshot_exec_vector(task,addr,s,s.argv)==-E2BIG&&memory::vector_requested==8);
 s.budget_used=EXEC_ARG_BYTES_MAX-7;memory::value_calls=0;assert(snapshot_exec_vector(task,0,s,s.envp)==-E2BIG&&!memory::value_calls);
 s.budget_used=0;assert(snapshot_exec_vector(task,UINT64_MAX-4,s,s.envp)==-EFAULT);
 memory::clear();
 vector_checks();
 std::puts(ALLOW_OLD?"Actual before snapshot functional/failure/fatal-recovery oracle PASS; short-arena allocation assertion disabled":
 "Actual snapshot/SmallVec/string copier: short/tail/large/exact2MiB budgets, prefix mutations/input retirement, partial-NUL faults, all allocation/usercopy failures and modeled fatal recovery PASS; short input allocates at most1KiB arena");
 return 0;
}
}
int main(){return test::run();}
'''
for token, value in {
    "@CHUNK@": function(usercopy, "copy_chunk"),
    "@CSTRING@": function(usercopy, "copy_cstring_from_task_status"),
    "@CONSTANTS@": constants,
    "@SNAPSHOT@": block(source, r"^struct ExecArgumentsSnapshot \{"),
    "@STRING@": function(source, "snapshot_exec_string") if "auto snapshot_exec_string(" in source else "",
    "@VECTOR@": function(source, "snapshot_exec_vector"),
    "@ARGUMENTS@": function(source, "snapshot_exec_arguments"),
    "ALLOW_OLD": "true" if allow_old else "false",
    "ALLOW_VECTOR_OLD": "true" if allow_vector_old else "false",
}.items():
    code = code.replace(token, value)
with tempfile.TemporaryDirectory(prefix="wos-exec-argument-arena-") as directory:
    source_path = Path(directory) / "test.cpp"
    binary_path = Path(directory) / "test"
    source_path.write_text(code)
    subprocess.run(["/usr/bin/clang++", "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined",
                    "-I" + str(ROOT / "modules/kern/src"), "-I" + str(ROOT / "modules/kern/include"),
                    "-I" + str(ROOT / "modules"), str(source_path), "-o", str(binary_path)], check=True)
    environment = os.environ.copy()
    environment["ASAN_OPTIONS"] = "detect_leaks=0"
    subprocess.run([str(binary_path)], env=environment, check=True)
