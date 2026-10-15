#!/usr/bin/env python3
"""Execute the real file-cache path with controlled allocation/I/O/perf endpoints."""
import os
import resource
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
source = Path(os.environ.get('WOS_VMEM_CACHE_TEST_SOURCE', ROOT / 'modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp')).read_text()
start = source.index('auto materialize_lazy_file_page_impl(')  # Assert the production entrypoint still exists.
start = source.index('auto file_mmap_cached_page_for_file(')
left = source.index('{', start)
depth = 0
for end in range(left, len(source)):
    if source[end] == '{':
        depth += 1
    elif source[end] == '}':
        depth -= 1
        if depth == 0:
            break
else:
    raise AssertionError('function boundary')
function = source[start:end + 1]
code = r'''
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <sys/types.h>
namespace ker::mod::sched::task {struct Task {};}
namespace ker::vfs {
struct ExecReadView {uint64_t content_id;};
struct File {ExecReadView *exec_read_view = nullptr;bool mutable_mapping=false;uint64_t cache_identity=0;};
namespace tmpfs {struct Authority {bool bypass;uint64_t object_identity;};
Authority tmpfs_file_cache_authority(const File&f){return {f.mutable_mapping,f.cache_identity};}
bool tmpfs_file_mutable_cache_bypass(const File&f){return f.mutable_mapping;} }
struct Stat {uint64_t st_size;};
}
namespace ker::mod::mm {
enum class PhysicalPageOwner {USER_FILE_CACHE,USER_FILE_MAPPING};
namespace paging {constexpr size_t PAGE_SIZE = 4096;}
}
namespace ker::mod::dbg {[[noreturn]]void panic_handler(const char*){__builtin_trap();}}
namespace ker::mod::perf {
enum class WkiPerfLocalVmemOp {FILE_CACHE_HIT, FILE_CACHE_MISS, FILE_CACHE_FILL, FILE_CACHE_EVICT};
}
struct FileMmapPageKey {uint64_t size, offset, exec_content_id;};
enum class FileMmapCacheInsertResult {INSERTED, DUPLICATE, EVICTED};
struct Event {ker::mod::perf::WkiPerfLocalVmemOp op; uint64_t offset; uint32_t latency; bool has_latency;};
struct State {
 bool recording=false, hit=false, allocation_fail=false, read_fail=false,mark_on_lookup=false,mark_on_read=false;
 unsigned cached_drops=0,owner_transfers=0;ker::vfs::File*active_file=nullptr;
 FileMmapCacheInsertResult result=FileMmapCacheInsertResult::INSERTED;
 unsigned clocks=0, task_queries=0, lookups=0, full_allocs=0, partial_allocs=0, reads=0, inserts=0, frees=0;
 size_t read_size=0;
 FileMmapPageKey key{};
 std::array<unsigned char,4096> page{}, cached{};
 std::array<Event,4> events{};
 unsigned event_count=0;
} state;
ker::mod::sched::task::Task task;
namespace ker::mod::perf {bool is_wki_recording_enabled(){return state.recording;}}
namespace ker::mod::time {uint64_t get_us(){return ++state.clocks * 10;}}
auto get_current_task(){++state.task_queries;return &task;}
auto file_mmap_page_key(const ker::vfs::Stat &st,uint64_t offset){return FileMmapPageKey{st.st_size,offset,0};}
void *file_mmap_cache_lookup(const FileMmapPageKey &key){++state.lookups;state.key=key;if(state.mark_on_lookup)state.active_file->mutable_mapping=true;return state.hit?state.cached.data():nullptr;}
uint32_t vmem_latency_since(uint64_t started){auto now=ker::mod::time::get_us();return now>=started?now-started:0;}
void record_local_vmem_cache_event(ker::mod::sched::task::Task *t,ker::mod::perf::WkiPerfLocalVmemOp op,uint64_t offset,uint32_t latency,bool has_latency){
 if(!t||!state.recording)return;
 assert(state.event_count<state.events.size());state.events[state.event_count++]={op,offset,latency,has_latency};
}
namespace ker::mod::mm::phys {
void *page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner,const char *){++state.full_allocs;return state.allocation_fail?nullptr:state.page.data();}
void *page_alloc_with_reclaim_may_fail(PhysicalPageOwner,size_t size,const char *){assert(size==4096);++state.partial_allocs;return state.allocation_fail?nullptr:state.page.data();}
void page_ref_dec(void *page){if(page==state.cached.data()){++state.cached_drops;return;}assert(page==state.page.data());++state.frees;}
bool page_reassign_owner(void*page,PhysicalPageOwner owner){assert(page==state.page.data()&&owner==PhysicalPageOwner::USER_FILE_MAPPING);++state.owner_transfers;return true;}
}
bool read_file_mapping_exact(ker::vfs::File *,void *page,size_t size,uint64_t offset){
 assert(page==state.page.data()&&offset==state.key.offset);++state.reads;state.read_size=size;
 if(state.read_fail)return false;
 std::memset(page,0x3c,size);if(state.mark_on_read)state.active_file->mutable_mapping=true;return true;
}
FileMmapCacheInsertResult file_mmap_cache_insert_or_discard(const FileMmapPageKey &key,void *page,void **out){
 assert(page==state.page.data()&&key.size==state.key.size&&key.offset==state.key.offset&&key.exec_content_id==state.key.exec_content_id);++state.inserts;
 if(state.result==FileMmapCacheInsertResult::DUPLICATE){ker::mod::mm::phys::page_ref_dec(page);*out=state.cached.data();}else *out=page;
 return state.result;
}
''' + function + r'''
void reset(bool recording){state=State{};state.recording=recording;state.page.fill(0xa5);}
void check_recording(bool recording,unsigned clocks,std::initializer_list<ker::mod::perf::WkiPerfLocalVmemOp> events){
 assert(state.clocks==(recording?clocks:0));
 assert(state.task_queries==(recording?1:0));
 assert(state.event_count==(recording?events.size():0));
 if(recording){unsigned i=0;for(auto op:events){auto &event=state.events[i++];assert(event.op==op&&event.offset==state.key.offset);assert(event.has_latency==(op!=ker::mod::perf::WkiPerfLocalVmemOp::FILE_CACHE_EVICT));assert(event.latency==(event.has_latency?10:0));}}
}
void run(bool recording){
 using Op=ker::mod::perf::WkiPerfLocalVmemOp;
 ker::vfs::ExecReadView view{73};ker::vfs::File file{&view};ker::vfs::Stat st{4096};void *out=nullptr;
 reset(recording);assert(!file_mmap_cached_page_for_file(nullptr,st,0,&out));assert(!file_mmap_cached_page_for_file(&file,st,0,nullptr));assert(!state.clocks&&!state.task_queries&&!state.lookups);
 reset(recording);state.hit=true;assert(file_mmap_cached_page_for_file(&file,st,0,&out));assert(out==state.cached.data()&&state.lookups==1&&state.key.exec_content_id==73&&!state.full_allocs&&!state.partial_allocs&&!state.reads&&!state.inserts&&!state.frees);check_recording(recording,2,{Op::FILE_CACHE_HIT});
 for(auto result:{FileMmapCacheInsertResult::INSERTED,FileMmapCacheInsertResult::DUPLICATE,FileMmapCacheInsertResult::EVICTED}){
  reset(recording);state.result=result;assert(file_mmap_cached_page_for_file(&file,st,0,&out));assert(state.full_allocs==1&&!state.partial_allocs&&state.reads==1&&state.read_size==4096&&state.inserts==1);assert(std::all_of(state.page.begin(),state.page.end(),[](auto x){return x==0x3c;}));assert(state.frees==(result==FileMmapCacheInsertResult::DUPLICATE));assert(out==(result==FileMmapCacheInsertResult::DUPLICATE?state.cached.data():state.page.data()));
  if(result==FileMmapCacheInsertResult::EVICTED)check_recording(recording,4,{Op::FILE_CACHE_MISS,Op::FILE_CACHE_FILL,Op::FILE_CACHE_EVICT});
  else check_recording(recording,4,{Op::FILE_CACHE_MISS,result==FileMmapCacheInsertResult::DUPLICATE?Op::FILE_CACHE_HIT:Op::FILE_CACHE_FILL});
 }
 reset(recording);st.st_size=17;assert(file_mmap_cached_page_for_file(&file,st,0,&out));assert(state.partial_allocs==1&&!state.full_allocs&&state.read_size==17&&state.inserts==1);for(size_t i=0;i<4096;++i)assert(state.page[i]==(i<17?0x3c:0));check_recording(recording,4,{Op::FILE_CACHE_MISS,Op::FILE_CACHE_FILL});
 reset(recording);st.st_size=0;assert(file_mmap_cached_page_for_file(&file,st,4096,&out));assert(state.partial_allocs==1&&!state.reads&&state.inserts==1&&state.key.offset==4096);for(auto x:state.page)assert(x==0);check_recording(recording,4,{Op::FILE_CACHE_MISS,Op::FILE_CACHE_FILL});
 reset(recording);st.st_size=4096;state.allocation_fail=true;assert(!file_mmap_cached_page_for_file(&file,st,0,&out));assert(state.full_allocs==1&&!state.reads&&!state.inserts&&!state.frees);check_recording(recording,2,{Op::FILE_CACHE_MISS});
 reset(recording);state.read_fail=true;assert(!file_mmap_cached_page_for_file(&file,st,0,&out));assert(state.reads==1&&state.frees==1&&!state.inserts);check_recording(recording,3,{Op::FILE_CACHE_MISS});
 reset(recording);st.st_size=UINT64_MAX;assert(!file_mmap_cached_page_for_file(&file,st,uint64_t{1}<<63,&out));assert(state.full_allocs==1&&!state.reads&&state.frees==1&&!state.inserts);check_recording(recording,3,{Op::FILE_CACHE_MISS});
 reset(recording);file.exec_read_view=nullptr;st.st_size=4096;state.hit=true;assert(file_mmap_cached_page_for_file(&file,st,0,&out));assert(state.key.exec_content_id==0);check_recording(recording,2,{Op::FILE_CACHE_HIT});

 file.cache_identity=941;
 for(unsigned mode=0;mode<3;++mode){reset(recording);state.active_file=&file;file.mutable_mapping=mode==0;
  state.hit=true;state.mark_on_lookup=mode==1;state.mark_on_read=mode==2;if(mode==2)state.hit=false;
  state.cached.fill(0x95);assert(file_mmap_cached_page_for_file(&file,st,0,&out));
  assert(out==state.page.data()&&state.reads==1&&!state.inserts&&state.owner_transfers==1&&state.cached_drops==(mode==1));
  for(auto x:state.page)assert(x==0x3c);for(auto x:state.cached)assert(x==0x95);
  assert(file.mutable_mapping);file.mutable_mapping=false;}
 reset(recording);state.hit=true;assert(file_mmap_cached_page_for_file(&file,st,0,&out));assert(state.key.exec_content_id==941);
}
int main(){run(false);run(true);puts("actual file-cache disabled zero-clock / enabled events / key / allocation / fill / ownership / failure branches PASS");}
'''
# Expected old-code assertion failures must not leave host core images.
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
with tempfile.TemporaryDirectory(prefix='wos-vmem-cache-perf-') as td:
    p = Path(td)
    (p / 'check.cpp').write_text(code)
    subprocess.run(['/usr/bin/clang++', '-std=c++23', '-O2', '-g', '-fsanitize=address,undefined', '-fno-omit-frame-pointer', str(p / 'check.cpp'), '-o', str(p / 'check')], check=True)
    subprocess.run([str(p / 'check')], check=True, env={**os.environ, 'ASAN_OPTIONS': 'detect_leaks=0:halt_on_error=1', 'UBSAN_OPTIONS': 'halt_on_error=1'})
