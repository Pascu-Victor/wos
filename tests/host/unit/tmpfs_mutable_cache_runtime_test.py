#!/usr/bin/env python3
"""Compile actual VFS cache consumers; backend/lock/allocation endpoints are seams.

This tests cache refusal and partial-result policy, not writable mmap admission,
TMPFS implementation, native scheduling, File ABI, or complete VFS lookup.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / 'modules/kern/src/vfs/core.cpp'
FILE = ROOT / 'modules/kern/src/vfs/file.hpp'
FUNCTIONS = [
    'file_mutable_mapping_cache_bypass', 'file_stat_snapshot_anonymous_cacheable',
    'file_stat_snapshot_path_cacheable_fs', 'file_stat_snapshot_cacheable',
    'file_stat_snapshot_created_open_prefill_eligible', 'file_stat_snapshot_open_prefill_safe',
    'file_stat_snapshot_mode_cacheable', 'file_stat_snapshot_devfs_mode_cacheable',
    'file_stat_snapshot_result_cacheable', 'file_stat_snapshot_record_uncacheable_miss',
    'file_stat_snapshot_invalidate_locked', 'file_stat_snapshot_store_locked',
    'file_stat_snapshot_binding_current', 'file_stat_snapshot_path_current',
    'file_stat_snapshot_lookup', 'file_stat_snapshot_fast_current', 'vfs_prefill_file_stat_snapshot',
    'metadata_cache_tmpfs_lookup_current', 'metadata_cache_tmpfs_path_current',
    'metadata_cache_lookup_retained_stat', 'metadata_cache_lookup_mount_stat',
    'metadata_cache_store_known_path_stat_on_current_mount', 'metadata_cache_store_retained_stat',
    'stream_copy_available_locked', 'stream_attach_file', 'stream_cache_read_eligible', 'vfs_stream_cache_try_read',
]


def body(source, name):
    """Find a unique full definition; skip forward declarations and C++ literals."""
    found = []
    for match in re.finditer(r'(?m)^(?:auto|void) ' + re.escape(name) + r'\(', source):
        left = source.find('{', match.end())
        if left < 0 or ';' in source[match.end():left]:
            continue
        i, depth, quote = left, 0, None
        while i < len(source):
            if quote:
                if source[i] == '\\':
                    i += 2
                    continue
                if source[i] == quote:
                    quote = None
            elif source.startswith('//', i):
                i = source.find('\n', i)
                assert i >= 0
                continue
            elif source.startswith('/*', i):
                i = source.index('*/', i + 2) + 2
                continue
            elif source[i] in ('"', "'"):
                quote = source[i]
            elif source[i] == '{':
                depth += 1
            elif source[i] == '}':
                depth -= 1
                if depth == 0:
                    found.append(source[match.start():i + 1])
                    break
            i += 1
    assert len(found) == 1, (name, len(found))
    return found[0]


PREFIX = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <string_view>
#include <sys/stat.h>
#include <utility>
#include <vector>
static unsigned assertions=0,cases=0;
#define CHECK(x) do {++assertions;if(!(x)){std::fprintf(stderr,"line %d: %s\n",__LINE__,#x);std::abort();}}while(0)
struct Lock {bool held=false;std::function<void()> hook;void lock(){CHECK(!held);held=true;auto h=std::exchange(hook,{});if(h)h();}bool try_lock(){lock();return true;}void unlock(){CHECK(held);held=false;}};
namespace ker::mod::sys {struct MutexGuard {Lock& lock;explicit MutexGuard(Lock& l):lock(l){lock.lock();}~MutexGuard(){lock.unlock();}};}
namespace ker::mod::security {struct Snapshot {unsigned marker;static Snapshot bootstrap_root(){return {1};}};}
namespace ker::mod::sched {struct Task {security::Snapshot credentials(){return {42};}};Task current;bool current_available=true;bool can_query_current_task(){return current_available;}Task* get_current_task(){return current_available?&current:nullptr;}void kern_yield();}
namespace ker::vfs {
enum class FSType {TMPFS,FAT32,XFS,REMOTE,DEVFS,SOCKET,PROCFS};
struct Stat {mode_t st_mode=0;uint64_t st_ino=0;int64_t st_size=0;};
namespace tmpfs {struct Node {std::atomic<bool> bypass{false};uint64_t inode=0,id=0;int64_t size=8;};}
struct File;struct Fops {ssize_t(*vfs_read)(File*,void*,size_t,size_t);};
struct File {FSType fs_type=FSType::TMPFS;const char* vfs_path="/alpha";int open_flags=0;void* mount_owner=nullptr;uint64_t mount_generation=1,vfs_path_namespace_generation=1;Fops* fops=nullptr;tmpfs::Node* node=nullptr;void* stream_cache_attachment=nullptr;void* cache_notify_attachment=nullptr;int64_t pos=0;bool open_create_result_known=false,created_by_open=false;Lock stat_cache_mutex;Stat stat_cache;uint64_t stat_cache_generation=1,stat_cache_invalidation_generation=1;size_t stat_cache_path_len=6;bool stat_cache_valid=false;};
static Lock g_stream_cache_lock;static unsigned irq_depth=0,tree_calls=0,backend_calls=0,authority_calls=0;
static bool backend_marks=false,trim_marks=false,refresh_marks=false;
static File* active_file=nullptr;
namespace tmpfs {
struct TmpfsLookupHandle {Node* node=nullptr;bool valid=true;};
struct TmpfsCacheAuthority {bool bypass{};uint64_t inode{},object_identity{};};
TmpfsCacheAuthority tmpfs_file_cache_authority(const File& f) noexcept {++authority_calls;if(f.fs_type!=FSType::TMPFS)return {};if(!f.node)return {true,0,0};return {f.node->bypass.load(std::memory_order_acquire),f.node->inode,f.node->id};}
bool tmpfs_file_mutable_cache_bypass(const File& f) noexcept {return tmpfs_file_cache_authority(f).bypass;}
int tmpfs_lookup_cache_authority(const TmpfsLookupHandle& h,TmpfsCacheAuthority& out){CHECK(!g_stream_cache_lock.held&&!irq_depth);++tree_calls;if(!h.valid)return -EAGAIN;if(!h.node)return -ENOENT;out={h.node->bypass.load(std::memory_order_acquire),h.node->inode,h.node->id};return 0;}
static Node* alpha=nullptr;static Node* beta=nullptr;
int tmpfs_acquire_lookup(ker::mod::security::Snapshot credentials,Node*,const char* path,TmpfsLookupHandle* out){CHECK(!irq_depth&&!g_stream_cache_lock.held&&credentials.marker==42);++tree_calls;Node* node=std::string_view(path)=="alpha"||std::string_view(path)=="hard"?alpha:std::string_view(path)=="beta"?beta:nullptr;if(!node)return -ENOENT;out->node=node;return 0;}
}
static std::atomic<uint64_t> g_metadata_cache_generation{1},g_metadata_invalidation_generation{1},g_vfs_namespace_generation{1};
uint64_t mount_table_generation_snapshot(){return 1;}
bool metadata_cacheable_fs(FSType f){return f==FSType::TMPFS||f==FSType::XFS||f==FSType::FAT32;}
Fops tmpfs_fops;
namespace tmpfs {Fops* get_tmpfs_fops(){return &tmpfs_fops;}}
struct MetadataSnapshotStamp {uint64_t cache_generation=1,mount_generation=1,invalidation_generation=1;};
struct MetadataInvalidationCheck {bool invalidated=false;uint64_t checked_generation=1;};
MetadataSnapshotStamp metadata_snapshot_stamp(){return {};}
MetadataInvalidationCheck metadata_path_invalidation_check(const char*,size_t,uint64_t){return {};}
size_t file_vfs_path_len(const File* f){return f->vfs_path?std::strlen(f->vfs_path):0;}
bool cache_notify_file_dirty_impl(File*){return false;}
void metadata_cache_note_observation_store(){}
constexpr size_t UNKNOWN_PATH_LEN=SIZE_MAX,MAX_PATH_LEN=4096;
constexpr uint64_t UNKNOWN_PATH_HASH=UINT64_MAX;
uint64_t metadata_path_hash_raw(const char*,size_t){return 71;}
struct MountPoint {FSType fs_type=FSType::TMPFS;uint32_t dev_id=7;tmpfs::Node* root=nullptr;};
tmpfs::Node* tmpfs_root_for_mount(const MountPoint* m){return m->root;}
const char* strip_mount_prefix(const MountPoint*,const char* p){return p[0]=='/'?p+1:p;}
enum class LookupFollowPolicy {FOLLOW_FINAL,NOFOLLOW_FINAL};
struct LookupHandle {MountPoint* m;tmpfs::TmpfsLookupHandle* h;const char* p;bool stable=true;uint64_t generation=1;MountPoint* mount()const{return m;}const char* path()const{return p;}size_t path_length()const{return std::strlen(p);}uint64_t mount_generation()const{return generation;}LookupFollowPolicy follow_policy()const{return LookupFollowPolicy::FOLLOW_FINAL;}bool requires_directory()const{return false;}void* backend_parent()const{return h;}};
bool lookup_path_cache_key_still_stable(const LookupHandle& h){return h.stable;}
static Stat cached_path;static int cached_path_result=0;
int metadata_cache_lookup_prehashed(const char*,size_t,uint64_t,FSType,uint64_t,bool,bool,Stat* out,bool){CHECK(!irq_depth);++irq_depth;if(cached_path_result==0)*out=cached_path;--irq_depth;return cached_path_result;}
int metadata_cache_lookup(const char* p,FSType f,uint64_t d,bool a,bool b,Stat* out,bool rec,size_t len){return metadata_cache_lookup_prehashed(p,len,71,f,d,a,b,out,rec);}
static unsigned path_stores=0;
void metadata_cache_store_known_stat_variants(const char*,FSType,uint64_t,const Stat&,MetadataSnapshotStamp,size_t,const MountPoint*,uint64_t=UNKNOWN_PATH_HASH){CHECK(!irq_depth&&!g_stream_cache_lock.held);++path_stores;}
struct StreamChunk {uint64_t offset=0;uint32_t size=0;std::array<uint8_t,8> data{};};
struct StreamIsland {bool retired=false,eof=false,producer_active=false;int error=0;uint64_t next_fetch_offset=0,last_access_us=0;std::deque<std::unique_ptr<StreamChunk>> chunks;};
struct StreamCacheIdentity {uint64_t ino=0;};struct StreamFreshnessStamp {int64_t size=0;};
struct StreamCacheEntry {StreamCacheIdentity identity;StreamFreshnessStamp freshness;bool can_reuse_detached=false,retain_full_file=false,pinned_detached=false;uint64_t last_used_us=0,cached_bytes=0;StreamIsland island;};
struct StreamReaderAttachment {static inline int live=0;StreamCacheEntry* entry=nullptr;StreamIsland* island=nullptr;uint64_t desired_offset=0;StreamReaderAttachment(){++live;}~StreamReaderAttachment(){--live;}};
std::vector<std::unique_ptr<StreamCacheEntry>> g_stream_cache;
constexpr size_t STREAM_CHUNK_SIZE=8;constexpr int STREAM_PREMATURE_EOF_RETRIES=1;
uint64_t stream_now_us(){return 1;}
StreamIsland* stream_select_island_locked(StreamReaderAttachment* a,uint64_t){CHECK(g_stream_cache_lock.held);if(a->entry)a->island=&a->entry->island;return a->island;}
void stream_gc_locked(uint64_t){CHECK(g_stream_cache_lock.held);}
StreamCacheEntry* stream_find_entry_locked(StreamCacheIdentity i){CHECK(g_stream_cache_lock.held);for(auto& e:g_stream_cache)if(e->identity.ino==i.ino)return e.get();return nullptr;}
bool stream_entry_has_readers(StreamCacheEntry*){return true;}
bool stream_entry_should_keep_detached(StreamCacheEntry*,uint64_t){return true;}
bool stream_freshness_matches(StreamFreshnessStamp,const Stat&){return true;}
void stream_reset_entry_locked(StreamCacheEntry*){CHECK(g_stream_cache_lock.held);}
void stream_trim_island_front_locked(StreamCacheEntry*,StreamIsland*){CHECK(g_stream_cache_lock.held);if(trim_marks){trim_marks=false;active_file->node->bypass.store(true,std::memory_order_release);}}
void stream_enforce_entry_cap_locked(StreamCacheEntry*,uint64_t){CHECK(g_stream_cache_lock.held);}
uint64_t stream_island_end(StreamIsland* i){uint64_t end=0;for(auto& c:i->chunks)end=std::max(end,c->offset+c->size);return end;}
bool cache_notify_try_copy_snapshot(File*,StreamCacheIdentity*,StreamFreshnessStamp*,bool*,bool*){return false;}
void cache_notify_store_snapshot(File*,StreamCacheIdentity,StreamFreshnessStamp,bool,bool){}
int file_stat_snapshot_refresh_from_backend(File* f,Stat* out){CHECK(!g_stream_cache_lock.held&&!irq_depth);*out={S_IFREG|0644,f->node->inode,f->node->size};if(refresh_marks)f->node->bypass.store(true,std::memory_order_release);return 0;}
int stream_build_identity(File* f,const Stat& s,StreamCacheIdentity* i,StreamFreshnessStamp* stamp,bool* reuse,bool* retain){i->ino=f->node->inode;*stamp={s.st_size};*reuse=true;*retain=false;return 0;}
ssize_t clamp_io_count(ssize_t n,size_t count){return n<0?n:std::min<size_t>(n,count);}
namespace log {template<class... A>void warn(const char*,A...) {}}
ssize_t backend_read(File* f,void* p,size_t n,size_t){CHECK(!g_stream_cache_lock.held&&!irq_depth);++backend_calls;std::memset(p,0xB7,n);if(backend_marks)f->node->bypass.store(true,std::memory_order_release);return n;}
'''

CASES = r'''
struct Fixture {
 tmpfs::Node a,b;File f,peer,other;MountPoint mount;
 Fixture(){++cases;ker::mod::sched::current_available=true;a.inode=101;a.id=501;b.inode=202;b.id=502;tmpfs::alpha=&a;tmpfs::beta=&b;mount.root=&a;tmpfs_fops.vfs_read=backend_read;for(auto* p:{&f,&peer}){p->node=&a;p->fops=&tmpfs_fops;}other.node=&b;other.fops=&tmpfs_fops;other.vfs_path="/beta";active_file=&f;backend_calls=tree_calls=authority_calls=path_stores=0;backend_marks=trim_marks=refresh_marks=false;cached_path_result=0;}
 ~Fixture(){for(auto* p:{&f,&peer,&other}){delete static_cast<StreamReaderAttachment*>(p->stream_cache_attachment);p->stream_cache_attachment=nullptr;}g_stream_cache.clear();CHECK(!g_stream_cache_lock.held&&!irq_depth&&StreamReaderAttachment::live==0);}
 void mark(){a.bypass.store(true,std::memory_order_release);}
 void prefill(File& p,int64_t size=3){Stat s{S_IFREG|0644,p.node->inode,size};vfs_prefill_file_stat_snapshot(&p,s);p.stat_cache_path_len=std::strlen(p.vfs_path);}
 StreamReaderAttachment* attach(bool cached){auto* p=stream_attach_file(&f);CHECK(p&&p->entry);if(cached){auto c=std::make_unique<StreamChunk>();c->size=2;c->data.fill(0x31);p->island->chunks.push_back(std::move(c));p->island->next_fetch_offset=2;}return p;}
};
void run(){
 // A retained requester remains authoritative when no scheduler task exists.
 {
  Fixture x;
  ker::mod::sched::current_available=false;
  tmpfs::TmpfsLookupHandle lookup{&x.a};
  LookupHandle handle{&x.mount,&lookup,"/hard"};
  Stat s{S_IFREG|0644,101,3};
  metadata_cache_store_retained_stat(handle,s);
  CHECK(path_stores==1&&tree_calls==1);
  metadata_cache_store_known_path_stat_on_current_mount("/hard",&x.mount,s);
  CHECK(path_stores==1&&tree_calls==1);
 }
 {
  Fixture x;
  ker::mod::sched::current_available=false;
  tmpfs::TmpfsLookupHandle a{&x.a},b{&x.b};
  LookupHandle first{&x.mount,&a,"/alpha"},sibling{&x.mount,&b,"/beta"};
  x.mark();
  metadata_cache_store_retained_stat(first,{S_IFREG|0644,101,3});
  CHECK(!path_stores);
  metadata_cache_store_retained_stat(sibling,{S_IFREG|0644,202,9});
  CHECK(path_stores==1);
  metadata_cache_store_retained_stat(sibling,{S_IFREG|0644,101,3});
  CHECK(path_stores==1);
 }
 {
  Fixture x;
  ker::mod::sched::current_available=false;
  tmpfs::TmpfsLookupHandle lookup{&x.a,false};
  LookupHandle handle{&x.mount,&lookup,"/alpha"};
  Stat s{S_IFREG|0644,101,3};
  metadata_cache_store_retained_stat(handle,s);
  CHECK(!path_stores);
  lookup.valid=true;
  x.a.id=0;
  metadata_cache_store_retained_stat(handle,s);
  CHECK(!path_stores);
  handle.h=nullptr;
  metadata_cache_store_retained_stat(handle,s);
  CHECK(!path_stores);
 }
 {
  Fixture x;
  tmpfs::TmpfsLookupHandle lookup{&x.a};
  LookupHandle handle{&x.mount,&lookup,"/alpha",false};
  Stat s{S_IFREG|0644,101,3};
  metadata_cache_store_retained_stat(handle,s);
  CHECK(!path_stores);
  handle.stable=true;
  handle.generation=2;
  metadata_cache_store_retained_stat(handle,s);
  CHECK(!path_stores);
 }
 {
  Fixture x;
  ker::mod::sched::current_available=false;
  LookupHandle handle{&x.mount,nullptr,"/alpha"};
  metadata_cache_store_retained_stat(handle,{S_IFDIR|0755,101,3});
  CHECK(path_stores==1&&!tree_calls);
  x.mount.fs_type=FSType::XFS;
  metadata_cache_store_retained_stat(handle,{S_IFREG|0644,101,3});
  CHECK(path_stores==2&&!tree_calls);
 }
 {Fixture x;x.prefill(x.f);x.prefill(x.peer);x.prefill(x.other);Stat s{};CHECK(file_stat_snapshot_lookup(&x.f,&s)&&s.st_size==3);CHECK(file_stat_snapshot_fast_current(&x.f,&s));x.mark();s.st_size=999;CHECK(!file_stat_snapshot_lookup(&x.f,&s)&&s.st_size==999);CHECK(!file_stat_snapshot_fast_current(&x.peer,&s));CHECK(file_stat_snapshot_lookup(&x.other,&s)&&s.st_ino==202);CHECK(!file_stat_snapshot_cacheable(&x.f));}
 {Fixture x;x.prefill(x.f);Stat s{};x.f.stat_cache_mutex.hook=[&]{x.mark();};CHECK(!file_stat_snapshot_lookup(&x.f,&s));CHECK(!x.f.stat_cache_valid);}
 {Fixture x;x.prefill(x.f);Stat s{};x.f.stat_cache_mutex.hook=[&]{x.mark();};CHECK(!file_stat_snapshot_fast_current(&x.f,&s));}
 {Fixture x;x.mark();x.prefill(x.f);CHECK(!x.f.stat_cache_valid);file_stat_snapshot_store_locked(&x.f,{S_IFREG|0600,101,88},{});CHECK(!x.f.stat_cache_valid);x.other.open_flags=O_NO_CACHE;x.prefill(x.other);CHECK(!x.other.stat_cache_valid);}
 {Fixture x;cached_path={S_IFREG|0644,101,3};Stat s{};CHECK(metadata_cache_lookup_mount_stat("/alpha",&x.mount,true,false,&s,UNKNOWN_PATH_LEN,UNKNOWN_PATH_HASH)==0&&s.st_size==3);x.mark();s.st_size=999;CHECK(metadata_cache_lookup_mount_stat("/alpha",&x.mount,true,false,&s,UNKNOWN_PATH_LEN,UNKNOWN_PATH_HASH)==-EAGAIN&&s.st_size==999);CHECK(metadata_cache_lookup_mount_stat("/hard",&x.mount,true,false,&s,UNKNOWN_PATH_LEN,UNKNOWN_PATH_HASH)==-EAGAIN);cached_path={S_IFREG|0644,202,4};CHECK(metadata_cache_lookup_mount_stat("/beta",&x.mount,true,false,&s,UNKNOWN_PATH_LEN,UNKNOWN_PATH_HASH)==0&&s.st_ino==202);}
 {Fixture x;cached_path={S_IFREG|0644,999,3};Stat s{};CHECK(metadata_cache_lookup_mount_stat("/alpha",&x.mount,true,false,&s,UNKNOWN_PATH_LEN,UNKNOWN_PATH_HASH)==-EAGAIN);cached_path_result=-ENOENT;unsigned old=tree_calls;CHECK(metadata_cache_lookup_mount_stat("/missing",&x.mount,true,false,&s,UNKNOWN_PATH_LEN,UNKNOWN_PATH_HASH)==-ENOENT&&tree_calls==old);x.mount.fs_type=FSType::XFS;cached_path_result=0;CHECK(metadata_cache_lookup_mount_stat("/alpha",&x.mount,true,false,&s,UNKNOWN_PATH_LEN,UNKNOWN_PATH_HASH)==0&&tree_calls==old);}
 {Fixture x;tmpfs::TmpfsLookupHandle h{&x.a};LookupHandle handle{&x.mount,&h,"/hard"};cached_path={S_IFREG|0644,101,3};Stat s{};CHECK(metadata_cache_lookup_retained_stat(handle,&s)==0);h.valid=false;s.st_size=777;CHECK(metadata_cache_lookup_retained_stat(handle,&s)==-EAGAIN&&s.st_size==777);h.valid=true;x.mark();CHECK(metadata_cache_lookup_retained_stat(handle,&s)==-EAGAIN);}
 {Fixture x;ker::mod::sched::current_available=false;cached_path={S_IFREG|0644,101,3};Stat s{};s.st_size=999;CHECK(metadata_cache_lookup_mount_stat("/alpha",&x.mount,true,false,&s,UNKNOWN_PATH_LEN,UNKNOWN_PATH_HASH)==-EAGAIN&&s.st_size==999&&!tree_calls);metadata_cache_store_known_path_stat_on_current_mount("/alpha",&x.mount,cached_path);CHECK(!path_stores&&!tree_calls);cached_path_result=-ENOENT;CHECK(metadata_cache_lookup_mount_stat("/missing",&x.mount,true,false,&s,UNKNOWN_PATH_LEN,UNKNOWN_PATH_HASH)==-ENOENT&&!tree_calls);}
 {Fixture x;Stat a{S_IFREG|0644,101,3},b{S_IFREG|0644,202,9};metadata_cache_store_known_path_stat_on_current_mount("/hard",&x.mount,a);CHECK(path_stores==1);x.mark();metadata_cache_store_known_path_stat_on_current_mount("/alpha",&x.mount,a);CHECK(path_stores==1);metadata_cache_store_known_path_stat_on_current_mount("/beta",&x.mount,b);CHECK(path_stores==2);metadata_cache_store_known_path_stat_on_current_mount("/beta",&x.mount,a);CHECK(path_stores==2);}
 {Fixture x;CHECK(stream_cache_read_eligible(&x.f));CHECK(stream_cache_read_eligible(&x.other));auto* original=x.attach(true);x.mark();CHECK(!stream_cache_read_eligible(&x.f)&&stream_cache_read_eligible(&x.other));CHECK(!stream_attach_file(&x.f)&&x.f.stream_cache_attachment==original);std::array<unsigned char,4> buf{};ssize_t n=77;size_t actual=99;CHECK(!vfs_stream_cache_try_read(&x.f,buf.data(),4,0,&actual,&n)&&n==77&&actual==99&&!backend_calls);}
 {Fixture x;auto* original=x.attach(true);g_stream_cache_lock.hook=[&]{x.mark();};CHECK(!stream_attach_file(&x.f)&&x.f.stream_cache_attachment==original);}
 {Fixture x;refresh_marks=true;CHECK(!stream_attach_file(&x.f));CHECK(!x.f.stream_cache_attachment&&g_stream_cache.empty());}
 {Fixture x;g_stream_cache_lock.hook=[&]{x.mark();};CHECK(!stream_attach_file(&x.f));CHECK(!x.f.stream_cache_attachment&&g_stream_cache.empty());}
 {Fixture x;x.attach(true);std::array<unsigned char,4> buf{};ssize_t n=-1;size_t actual=99;trim_marks=true;CHECK(vfs_stream_cache_try_read(&x.f,buf.data(),4,0,&actual,&n));CHECK(n==2&&actual==2&&buf[0]==0x31&&buf[1]==0x31&&buf[2]==0&&!backend_calls);}
 {Fixture x;auto* p=x.attach(false);std::array<unsigned char,4> buf{};ssize_t n=77;size_t actual=99;backend_marks=true;CHECK(!vfs_stream_cache_try_read(&x.f,buf.data(),4,0,&actual,&n));CHECK(n==77&&actual==99&&backend_calls==1&&!p->island->producer_active&&p->island->chunks.empty()&&p->entry->cached_bytes==0);CHECK(std::all_of(buf.begin(),buf.end(),[](auto b){return b==0;}));}
 {Fixture x;auto* p=x.attach(true);std::array<unsigned char,4> buf{};ssize_t n=-1;size_t actual=99;backend_marks=true;CHECK(vfs_stream_cache_try_read(&x.f,buf.data(),4,0,&actual,&n));CHECK(n==2&&actual==2&&backend_calls==1&&!p->island->producer_active&&p->island->chunks.size()==1&&p->entry->cached_bytes==0);CHECK(buf[0]==0x31&&buf[1]==0x31&&buf[2]==0);}
 {Fixture x;auto* p=x.attach(true);std::array<unsigned char,4> buf{};ssize_t n=-1;size_t actual=99;CHECK(vfs_stream_cache_try_read(&x.f,buf.data(),2,0,&actual,&n)&&n==2&&actual==2&&buf[0]==0x31&&!backend_calls);CHECK(p->island->chunks.size()==1);}
 {Fixture x;x.attach(false);std::array<unsigned char,4> buf{};ssize_t n=-1;size_t actual=99;CHECK(vfs_stream_cache_try_read(&x.f,buf.data(),4,0,&actual,&n)&&n==4&&actual==4&&backend_calls==1);CHECK(std::all_of(buf.begin(),buf.end(),[](auto b){return b==0xB7;}));}
}
} // namespace ker::vfs
namespace ker::mod::sched {void kern_yield(){CHECK(false);}}
int main(){ker::vfs::run();std::printf("PASS actual_tmpfs_mutable_cache cases=%u assertions=%u\n",cases,assertions);}
'''


def pin(path):
    data = path.read_bytes()
    return dict(path=str(path), bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cxx', default=os.environ.get('CXX', '/usr/bin/clang++'))
    parser.add_argument('--evidence-dir', type=Path)
    args = parser.parse_args()
    before = [pin(p) for p in (SOURCE, FILE, Path(__file__).resolve())]
    source = SOURCE.read_text()
    selected = {name: body(source, name) for name in FUNCTIONS}
    flags = '\n'.join(re.findall(r'^constexpr int O_(?:NO_CACHE|CREAT|TRUNC|ALWAYS_CACHE|NOTIFY_CACHE_CHANGE) = [^;]+;', FILE.read_text(), re.M))
    assert flags.count('constexpr int ') == 5
    counters = sorted(set(re.findall(r'\bg_vfs_[a-z0-9_]+', '\n'.join(selected.values()))) - {'g_vfs_namespace_generation'})
    code = PREFIX + '\n' + flags + '\n' + '\n'.join('std::atomic<uint64_t> ' + name + '{0};' for name in counters)
    code += '\n' + '\n\n'.join(selected.values()) + '\n' + CASES
    evidence = args.evidence_dir
    if evidence:
        evidence.mkdir(parents=True, exist_ok=False)
        (evidence / 'actual-core.cpp').write_bytes(SOURCE.read_bytes())
        (evidence / 'actual-file.hpp').write_bytes(FILE.read_bytes())
        (evidence / 'extracted-functions.json').write_text(json.dumps({n: dict(bytes=len(s.encode()), sha256=hashlib.sha256(s.encode()).hexdigest()) for n, s in selected.items()}, indent=2) + '\n')
    started = time.monotonic()
    commands = []
    result = dict(qualification='ACTUAL_BODY_HOST_CACHE_CONSUMERS_ONLY', pass_=False, commands=commands, input_before=before)
    try:
        with tempfile.TemporaryDirectory(prefix='wos-tmpfs-mutable-cache-') as scratch:
            tmp = Path(scratch)
            (tmp / 'check.cpp').write_text(code)
            if evidence:
                (evidence / 'check.cpp').write_text(code)
            for argv in ([args.cxx, '-std=c++23', '-O1', '-g', '-fsanitize=address,undefined', '-fno-omit-frame-pointer', str(tmp / 'check.cpp'), '-o', str(tmp / 'check')], [str(tmp / 'check')]):
                command_started = time.monotonic()
                completed = subprocess.run(argv, text=True, capture_output=True, timeout=35, env={**os.environ, 'ASAN_OPTIONS': 'detect_leaks=0:halt_on_error=1', 'UBSAN_OPTIONS': 'halt_on_error=1'})
                row = dict(argv=argv, exit=completed.returncode, elapsed_seconds=time.monotonic() - command_started, stdout=completed.stdout, stderr=completed.stderr)
                commands.append(row)
                print(completed.stdout, end='')
                print(completed.stderr, end='', file=__import__('sys').stderr)
                if completed.returncode:
                    raise RuntimeError('actual-body command failed')
        result['scratch_removed'] = not tmp.exists()
        result['input_after'] = [pin(p) for p in (SOURCE, FILE, Path(__file__).resolve())]
        result['source_held'] = before == result['input_after']
        assert result['source_held']
        result['pass_'] = True
    finally:
        if 'tmp' in locals():
            result['scratch_removed'] = not tmp.exists()
        result['elapsed_seconds'] = time.monotonic() - started
        result['pass'] = result.pop('pass_')
        result['limits'] = 'Actual extracted core consumers; explicit backend/lookup/locks/containers/allocator seams. ASan/UBSan enabled; LSan disabled because ptrace-restricted hosts cannot run it. Scoped attachment counters and scratch cleanup are checked. No actual TMPFS admission, native runtime, Task/File ABI or full shared-memory qualification.'
        if evidence:
            (evidence / 'result.json').write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()
