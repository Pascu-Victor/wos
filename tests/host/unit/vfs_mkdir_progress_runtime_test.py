#!/usr/bin/env python3
"""Compile actual mkdir/serialized/consume bodies against controlled endpoints.

Persistent unrelated namespace publication invalidates each optimistic lookup.
This models endpoint invalidation and checks actual request/control flow; it does
not model the full native walker, XFS implementation or VM scheduling and cannot
attribute historical command_mix_075 or the later hang to a particular clause.
--source accepts preserved old core.cpp: it must compile, then fail the runtime
progress oracle; a compiler error is not a successful old-behavior control.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "AGENTS.md").is_file())

def function(source, name):
    match = re.search(rf"^auto {name}\([^{{;]*\{{", source, re.M)
    assert match, name
    end, depth = source.index("{", match.start()) + 1, 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[match.start():end]


PRELUDE = r'''
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
namespace fixture {
unsigned publication{}, locks{}, unlocks{}, resources{}, releases{}, credential_reads{};
unsigned optimistic{}, serialized{}, backend_calls{}, remote_calls{}, notifications{}, view_checks{};
unsigned authorization_checks{}, unrelated_mutations{}, mask_reads{}, cache_updates{}, synthetic_calls{}, synthetic_closes{}, compatibility_calls{};
enum class Churn { NONE, ACQUIRE, CONSUME };
Churn churn{};
bool invalidate_on_lock{}, unsupported{}, fail_serialized{}, bad_authorization{}, invalid_view{}, unstable_key{};
int acquire_error{}, backend_error{}, remote_error{};
struct Mutex {
 void lock();
 void unlock() { assert(publication==1); publication=0; ++unlocks; }
};
void retain() { ++resources; }
void release() { assert(publication==0 && resources); --resources; ++releases; }
}
namespace ker::mod::security {
struct Snapshot { unsigned uid{}, gid{}, token{}; bool operator==(const Snapshot&) const = default; };
}
namespace ker::vfs {
constexpr int AT_FDCWD=-100, AT_REMOVEDIR=0x200;
constexpr int RESOLVE_FAST_PATH_DECLINED=991;
constexpr size_t UNKNOWN_PATH_HASH=~size_t{};
enum class LookupIntent { OPEN, STAT, REMOVE, CREATE_DIRECTORY };
enum class LookupFollowPolicy { FOLLOW_FINAL, NOFOLLOW_FINAL };
enum class FSType { TMPFS, XFS, REMOTE, PROCFS, DEVFS, FAT32 };
struct Scope {
 bool host{true}, shut{}; uint64_t version{7};
 bool is_host() const { return host; }
 bool closed() const { return shut; }
 uint64_t generation() const { return version; }
};
struct MountPoint {
 FSType fs_type{FSType::TMPFS}; Scope namespace_owner{};
 std::atomic<bool> retiring{false}; void* private_data{this}; unsigned dev_id{11};
};
struct File { MountPoint* mount_owner{}; };
struct Policy { bool read_only{}; };
struct Node { bool present{true}, directory{true}, symlink{}; };
struct Backend { Node* node{}; bool valid{true}; };
namespace tmpfs { using TmpfsLookupHandle=Backend; }
namespace xfs { using XfsLookupHandle=Backend; }
struct Authorization { ker::mod::security::Snapshot credentials{}; uint64_t mount_namespace_generation{}; };
struct DirectoryViewRef { bool present{}; explicit operator bool() const { return present; } };
struct LocalWalkFileRelease { void operator()(File* p) const { if(p) { fixture::release(); delete p; } } };
struct LocalWalkReferences {
 bool held{};
 ~LocalWalkReferences() { if(held) fixture::release(); }
};
struct LookupHandle {
 MountPoint* selected{}; Backend binding{}; Authorization auth{}; Policy policy{};
 uint64_t epoch{}, mount_epoch{13}; bool retained{}, directory{}, anchored{}, revalidated{};
 std::string text; File anchor{}; const char* relative{};
 LookupHandle()=default;
 LookupHandle(const LookupHandle&)=delete;
 ~LookupHandle() { if(retained) fixture::release(); }
 MountPoint* mount() const { return selected; }
 const Authorization& authorization() const { return auth; }
 Policy mount_policy() const { return policy; }
 const char* path() const { return text.c_str(); }
 size_t path_length() const { return text.size(); }
 bool requires_directory() const { return directory; }
 const char* remote_relative_path() const { return relative; }
 File* dirfd_anchor() const { return anchored ? const_cast<File*>(&anchor) : nullptr; }
 bool dirfd_path_revalidated() const { return revalidated; }
 void* backend_parent() const { return const_cast<Backend*>(&binding); }
 uint64_t namespace_generation() const { return epoch; }
 uint64_t mount_generation() const { return mount_epoch; }
};
using LookupConsumer=int (*)(const LookupHandle&,void*);
std::atomic<uint64_t> g_vfs_namespace_generation{41};
fixture::Mutex g_vfs_namespace_publication_mutex;
MountPoint mount;
Node leaf, referent;
Policy policy;
Scope request_scope;
Scope* selected_scope=&request_scope;
constexpr ker::mod::security::Snapshot original{100,5,123};
constexpr ker::mod::security::Snapshot drifted{0,0,999};
bool symlink_case{}, remote_relative{}, anchored{};
const char* expected_path="child";
int expected_dirfd=29;
constexpr unsigned expected_mode=0750;
struct Stat { unsigned st_mode{}; };
}
namespace ker::mod::sched::task {
struct Task {
 ker::mod::security::Snapshot credentials() {
  return ++fixture::credential_reads==1 ? ker::vfs::original : ker::vfs::drifted;
 }
 unsigned creation_mask() { ++fixture::mask_reads; return 0027; }
 ker::vfs::Scope* mount_namespace() const { return ker::vfs::selected_scope; }
};
}
namespace ker::mod::sys {
class MutexGuard {
 fixture::Mutex& mutex;
public:
 explicit MutexGuard(fixture::Mutex& p):mutex(p) { mutex.lock(); }
 ~MutexGuard() { mutex.unlock(); }
};
}
namespace fixture {
void Mutex::lock() {
 assert(publication==0); // No recursive publication acquisition, even in fallback.
 if(invalidate_on_lock) {
  ++ker::vfs::g_vfs_namespace_generation; ++unrelated_mutations; invalidate_on_lock=false;
 }
 publication=1; ++locks;
}
}
namespace ker::vfs {
ker::mod::sched::task::Task task;
struct MountRef { MountPoint* pointer; MountPoint* get() const { return pointer; } };
MountRef find_mount_point(const char*,size_t,const Scope&) { return {&mount}; }
uint64_t mount_table_generation_snapshot() { return 13; }
bool lookup_request_context_matches(const LookupHandle& h,const ker::mod::sched::task::Task* subject,
 const ker::mod::security::Snapshot& credentials) {
 ++fixture::authorization_checks;
 return !fixture::bad_authorization && subject==&task && credentials==original && h.auth.credentials==credentials;
}
bool lookup_authorization_still_current(const LookupHandle&) { assert(false); return false; }
bool lookup_path_cache_key_still_stable(const LookupHandle&) { return !fixture::unstable_key; }
int validate_lookup_directory_view_locked(const LookupHandle&) {
 assert(fixture::publication==1); ++fixture::view_checks; return fixture::invalid_view ? -EACCES : 0;
}
void metadata_cache_note_path_changed(const char*,void*) { assert(fixture::publication==1); }
void vfs_cache_notify_path_changed(const char*,void*) { ++fixture::notifications; }
void metadata_cache_store_missing_path_on_current_mount(const char*,MountPoint*,size_t,size_t) {}
const char* strip_mount_prefix(const MountPoint*,const char* path) { return path; }
namespace tmpfs {
bool tmpfs_validate_lookup(const TmpfsLookupHandle& b) { return b.valid; }
}
namespace xfs {
int xfs_validate_lookup(const XfsLookupHandle& b) { return b.valid ? 0 : -EAGAIN; }
}
int mutate(const ker::mod::security::Snapshot& credentials,Backend& b,unsigned mode,bool readonly) {
 assert(fixture::publication==1 && credentials==original && mode==expected_mode);
 ++fixture::backend_calls;
 if(readonly) return -EROFS;
 if(fixture::backend_error) return fixture::backend_error;
 if(b.node->present) return -EEXIST;
 b.node->present=true; b.node->directory=true; return 0;
}
namespace tmpfs {
struct TmpfsObjectMetadata { unsigned mode{}; };
int tmpfs_mkdir_lookup(const ker::mod::security::Snapshot& c,TmpfsLookupHandle& b,uint32_t mode,
 TmpfsObjectMetadata* out,bool ro) { int r=mutate(c,b,mode,ro); if(!r)out->mode=mode; return r; }
void tmpfs_metadata_to_stat(const TmpfsObjectMetadata& m,unsigned dev,Stat* out) { assert(dev==11);out->st_mode=m.mode; }
}
namespace xfs {
int xfs_mkdir_lookup(const ker::mod::security::Snapshot& c,XfsLookupHandle& b,int mode,Stat* out,bool ro) {
 int r=mutate(c,b,static_cast<unsigned>(mode),ro);if(!r)out->st_mode=static_cast<unsigned>(mode);return r;
}
}
unsigned metadata_snapshot_stamp() { return 17; }
void metadata_cache_store_non_symlink_stat_variants(const char* p,FSType,unsigned dev,const Stat& st,unsigned stamp,
 size_t len,MountPoint* m) {
 assert(fixture::publication==1 && std::strcmp(p,expected_path)==0 && dev==11 && st.st_mode==expected_mode);
 assert(stamp==17 && len==std::strlen(expected_path) && m==&mount);++fixture::cache_updates;
}
namespace devfs {
bool devfs_acquire_path(const char*) { assert(!fixture::publication);++fixture::synthetic_calls;return leaf.present; }
}
namespace procfs {
struct Entry {};
Entry* procfs_lookup_path(const char*) { assert(!fixture::publication);++fixture::synthetic_calls;return leaf.present?new Entry:nullptr; }
struct Ops { void vfs_close(Entry*) { assert(!fixture::publication);++fixture::synthetic_closes; } };
Ops* get_procfs_fops() { static Ops o;return &o; }
}
int vfs_consume_lookup(const LookupHandle&,bool,LookupConsumer,void*) { assert(false);return -EIO; }
int vfs_mkdir_resolved_path(const ker::mod::security::Snapshot&,const char*,int,size_t,size_t,bool) {
 ++fixture::compatibility_calls;assert(false && "serialized non-native must decline before compatibility I/O");return -EIO;
}
void policy_arguments(ker::mod::sched::task::Task* subject,int dirfd,const char* path,LookupIntent intent,
 LookupFollowPolicy follow,bool directory,bool route,const ker::mod::security::Snapshot* credentials) {
 assert(subject==&task && dirfd==expected_dirfd && std::strcmp(path,expected_path)==0);
 assert(intent==LookupIntent::CREATE_DIRECTORY && follow==LookupFollowPolicy::NOFOLLOW_FINAL && route);
 assert(directory && credentials && *credentials==original);
}
void fill(LookupHandle* output,const char* path,bool directory,LookupFollowPolicy follow,
 const ker::mod::security::Snapshot& credentials) {
 assert(!output->retained); fixture::retain(); output->retained=true;
 output->selected=&mount; output->auth={credentials,mount.namespace_owner.generation()};
 output->epoch=g_vfs_namespace_generation.load(); output->text=path;
 output->directory=directory; output->policy=policy;
 output->binding.node=symlink_case && follow==LookupFollowPolicy::FOLLOW_FINAL ? &referent : &leaf;
 output->anchored=anchored; output->anchor.mount_owner=&mount;
 output->relative=remote_relative ? "relative-child" : nullptr;
}
int vfs_acquire_lookup(ker::mod::sched::task::Task* subject,int dirfd,const char* path,LookupIntent intent,
 LookupFollowPolicy follow,bool directory,bool route,LookupHandle* output,unsigned retries,bool resolved,
 const ker::mod::security::Snapshot* credentials) {
 assert(fixture::publication==0 && retries==0 && !resolved); ++fixture::optimistic;
 policy_arguments(subject,dirfd,path,intent,follow,directory,route,credentials);
 if(fixture::acquire_error) return fixture::acquire_error;
 if(fixture::churn==fixture::Churn::ACQUIRE) {
  ++g_vfs_namespace_generation; ++fixture::unrelated_mutations; return -EAGAIN;
 }
 fill(output,path,directory,follow,*credentials);
 if(fixture::churn==fixture::Churn::CONSUME && mount.fs_type!=FSType::REMOTE) fixture::invalidate_on_lock=true;
 return 0;
}
int acquire_local_component_lookup(ker::mod::sched::task::Task* subject,int dirfd,const char* path,LookupIntent intent,
 LookupFollowPolicy follow,bool directory,bool route,LookupHandle* output,const ker::mod::security::Snapshot* credentials,
 bool serialized,bool confined,const char* root,File* anchor,LocalWalkReferences* references) {
 assert(fixture::publication==1 && serialized && !confined && !root && !anchor && references);
 assert(!references->held && !fixture::resources); references->held=true; fixture::retain(); ++fixture::serialized;
 policy_arguments(subject,dirfd,path,intent,follow,directory,route,credentials);
 if(fixture::unsupported || mount.fs_type==FSType::REMOTE || mount.fs_type==FSType::PROCFS || mount.fs_type==FSType::DEVFS || mount.fs_type==FSType::FAT32) return RESOLVE_FAST_PATH_DECLINED;
 if(fixture::fail_serialized) return -EIO;
 fill(output,path,directory,follow,*credentials); return 0;
}
int prepare_open_directory_view_locked(ker::mod::sched::task::Task&,const ker::mod::security::Snapshot&,
 const LookupHandle&,std::unique_ptr<File,LocalWalkFileRelease>&,DirectoryViewRef&) { assert(false); return -EIO; }
}
namespace ker::net::wki {
int remote(const char* path,const ker::mod::security::Snapshot& credentials,ker::vfs::File* directory) {
 using namespace ker::vfs;
 assert(fixture::publication==0 && credentials==original);
 assert(std::strcmp(path,remote_relative ? "relative-child" : expected_path)==0);
 assert((directory!=nullptr)==remote_relative); ++fixture::remote_calls; return fixture::remote_error;
}
int wki_remote_vfs_mkdir(void*,const char* p,int mode,const ker::mod::security::Snapshot& c,ker::vfs::File* d) {
 assert(static_cast<unsigned>(mode)==ker::vfs::expected_mode);return remote(p,c,d);
}
}
namespace ker::vfs {
'''

CASES = r'''
void reset(FSType type=FSType::TMPFS) {
 assert(!fixture::publication && !fixture::resources && fixture::locks==fixture::unlocks);
 fixture::locks=fixture::unlocks=fixture::releases=fixture::credential_reads=fixture::mask_reads=0;
 fixture::optimistic=fixture::serialized=fixture::backend_calls=fixture::remote_calls=0;
 fixture::notifications=fixture::cache_updates=fixture::view_checks=fixture::authorization_checks=fixture::unrelated_mutations=0;
 fixture::synthetic_calls=fixture::synthetic_closes=fixture::compatibility_calls=0;
 fixture::churn=fixture::Churn::NONE;
 fixture::invalidate_on_lock=fixture::unsupported=fixture::fail_serialized=fixture::bad_authorization=fixture::invalid_view=fixture::unstable_key=false;
 fixture::acquire_error=fixture::backend_error=fixture::remote_error=0;
 mount.fs_type=type; mount.namespace_owner={}; mount.retiring=false;
 request_scope={}; selected_scope=&request_scope; leaf={false,false,false}; referent={true,true,false}; policy={};
 g_vfs_namespace_generation=41; symlink_case=remote_relative=anchored=false;
 expected_path="child"; expected_dirfd=29;
}
int make() { return vfs_mkdirat(&task,expected_dirfd,expected_path,0777); }
void settled() { assert(!fixture::publication && !fixture::resources && fixture::locks==fixture::unlocks); }
void authority() { assert(fixture::credential_reads==1 && fixture::mask_reads==1); }
void progress() {
 for(auto type:{FSType::TMPFS,FSType::XFS})
 for(auto churn:{fixture::Churn::ACQUIRE,fixture::Churn::CONSUME}) for(bool absolute:{false,true}) {
  reset(type);fixture::churn=churn;
  if(absolute) { expected_path="/root/child/";expected_dirfd=9876; }
  assert(make()==0 && leaf.present && leaf.directory); // Old must compile, then fail this progress oracle.
  assert(fixture::serialized==1 && fixture::backend_calls==1 && !fixture::remote_calls && fixture::optimistic==4);
  assert(fixture::unrelated_mutations==4 && g_vfs_namespace_generation==46);
  assert(fixture::notifications==1 && fixture::cache_updates==1 && fixture::releases>=2);authority();settled();
 }
}
void default_nonmutating() {
 reset();unsigned observed=0;
 auto reader=[](const LookupHandle&,void* p)->int { assert(fixture::publication==1);++*static_cast<unsigned*>(p);return 0; };
 assert(native_lookup_serialized(&task,expected_dirfd,expected_path,LookupIntent::CREATE_DIRECTORY,
  LookupFollowPolicy::NOFOLLOW_FINAL,true,true,reader,&observed,&original,nullptr)==0);
 assert(observed==1 && fixture::serialized==1 && !fixture::backend_calls && !leaf.present);
 assert(g_vfs_namespace_generation==41 && !fixture::credential_reads && fixture::releases==2);settled();
}
void fast_and_errors() {
 for(auto type:{FSType::TMPFS,FSType::XFS}) {
  reset(type);assert(make()==0 && leaf.present && !fixture::serialized && fixture::optimistic==1);
  assert(fixture::backend_calls==1 && g_vfs_namespace_generation==42 && fixture::notifications==1 && fixture::cache_updates==1);authority();settled();
  for(int error:{-EACCES,-ENOENT,-EIO}) {
   reset(type);fixture::acquire_error=error;assert(make()==error && !leaf.present && !fixture::backend_calls && !fixture::serialized);authority();settled();
  }
  for(int error:{-EEXIST,-EACCES,-EIO,-ENOSPC}) {
   reset(type);fixture::backend_error=error;assert(make()==error && !leaf.present && fixture::backend_calls==1 && !fixture::serialized);
   assert(g_vfs_namespace_generation==41 && !fixture::notifications && !fixture::cache_updates);authority();settled();
  }
  reset(type);policy.read_only=true;assert(make()==-EROFS && !leaf.present && !fixture::serialized && g_vfs_namespace_generation==41);authority();settled();
  reset(type);fixture::churn=fixture::Churn::ACQUIRE;policy.read_only=true;
  assert(make()==-EROFS && !leaf.present && fixture::serialized==1 && fixture::backend_calls==1);
  assert(g_vfs_namespace_generation==45 && !fixture::notifications && !fixture::cache_updates);authority();settled();
  reset(type);fixture::churn=fixture::Churn::ACQUIRE;fixture::backend_error=-ENOSPC;
  assert(make()==-ENOSPC && !leaf.present && fixture::serialized==1 && fixture::backend_calls==1);
  assert(g_vfs_namespace_generation==45 && !fixture::notifications);authority();settled();
  reset(type);fixture::backend_error=-EAGAIN;
  assert(make()==-EAGAIN && !leaf.present && fixture::serialized==1 && fixture::backend_calls==5);
  assert(g_vfs_namespace_generation==41 && !fixture::notifications);authority();settled();
  reset(type);fixture::unstable_key=true;anchored=true;assert(make()==0 && leaf.present);
  assert(!fixture::notifications && !fixture::cache_updates && g_vfs_namespace_generation==42);authority();settled();
 }
 reset();assert(vfs_mkdirat(nullptr,29,"child",0777)==-ESRCH);
 assert(vfs_mkdirat(&task,29,nullptr,0777)==-EINVAL && vfs_mkdirat(&task,29,"",0777)==-ENOENT);
 assert(!fixture::credential_reads && !fixture::mask_reads && !fixture::optimistic);settled();
}
void boundaries() {
 for(int scope:{0,1}) {
  reset();fixture::churn=fixture::Churn::ACQUIRE;
  if(scope==0)selected_scope=nullptr;else request_scope.host=false;
  assert(make()==-EAGAIN && !leaf.present && !fixture::serialized && !fixture::backend_calls);authority();settled();
 }
 for(auto type:{FSType::PROCFS,FSType::DEVFS,FSType::REMOTE,FSType::FAT32}) {
  reset(type);fixture::churn=fixture::Churn::ACQUIRE;
  assert(make()==-EAGAIN && !leaf.present && fixture::serialized==1 && !fixture::backend_calls && !fixture::remote_calls);
  assert(!fixture::synthetic_calls && !fixture::compatibility_calls);authority();settled();
 }
 reset();fixture::churn=fixture::Churn::ACQUIRE;fixture::fail_serialized=true;
 assert(make()==-EIO && fixture::serialized==1 && !fixture::backend_calls && fixture::releases==1);authority();settled();
 for(bool relative:{false,true})for(int error:{0,-EAGAIN,-EIO}) {
  reset(FSType::REMOTE);fixture::remote_error=error;remote_relative=relative;anchored=relative;
  assert(make()==error && fixture::remote_calls==1 && !fixture::serialized && !fixture::backend_calls && !fixture::locks);
  assert(fixture::notifications==(error==0?1u:0u) && g_vfs_namespace_generation==41);authority();settled();
 }
 reset(FSType::REMOTE);policy.read_only=true;assert(make()==-EROFS && !fixture::remote_calls && !fixture::locks);authority();settled();
 for(bool auth:{false,true}) {
  reset();fixture::churn=fixture::Churn::ACQUIRE;fixture::bad_authorization=auth;fixture::invalid_view=!auth;
  assert(make()==(auth?-EAGAIN:-EACCES) && !leaf.present && !fixture::backend_calls && !fixture::notifications);
  assert(g_vfs_namespace_generation==41+fixture::unrelated_mutations);authority();settled();
 }
 for(auto type:{FSType::TMPFS,FSType::XFS}) {
  reset(type);leaf={true,false,true};symlink_case=true;fixture::churn=fixture::Churn::CONSUME;
  assert(make()==-EEXIST && leaf.present && leaf.symlink && referent.present && !fixture::notifications);
  assert(g_vfs_namespace_generation==45);authority();settled();
 }
 for(auto type:{FSType::PROCFS,FSType::DEVFS})for(bool present:{false,true}) {
  reset(type);leaf.present=present;assert(make()==(present?-EEXIST:-ENOSYS));
  assert(fixture::synthetic_calls==1 && fixture::synthetic_closes==(type==FSType::PROCFS&&present?1u:0u));
  assert(!fixture::backend_calls && !fixture::serialized && !fixture::notifications && !fixture::cache_updates && g_vfs_namespace_generation==41);authority();settled();
 }
}
} // namespace ker::vfs
int main() {
 ker::vfs::progress();ker::vfs::default_nonmutating();ker::vfs::fast_and_errors();ker::vfs::boundaries();
 std::puts("Actual mkdir progress PASS: four optimistic invalidations, native serialized creation, unchanged request authority and umask, backend boundaries, single publication and unlocked retirement");
}
'''


def run(args, directory):
    directory.mkdir(parents=True, exist_ok=True)
    source = args.source.read_text()
    bodies = [function(source, name) for name in
              ("consume_lookup_locked", "native_lookup_serialized", "synthetic_mkdir_backend",
               "synthetic_mkdir_result", "reject_synthetic_mkdir", "vfs_mkdirat")]
    generated = PRELUDE + "\n\n".join(bodies) + "\n" + CASES
    cpp, binary = directory / "fixture.cpp", directory / "fixture"
    cpp.write_text(generated)
    command = [args.cxx, "-std=c++20", "-O1", "-g", "-fno-omit-frame-pointer",
               "-fsanitize=address,undefined", str(cpp), "-o", str(binary)]
    report = {"source": str(args.source), "source_sha256": hashlib.sha256(source.encode()).hexdigest(),
              "actual_functions": ["consume_lookup_locked", "native_lookup_serialized", "synthetic_mkdir_backend",
                                   "synthetic_mkdir_result", "reject_synthetic_mkdir", "vfs_mkdirat"],
              "compile_argv": command, "compile_exit": None, "runtime_exit": None}
    compiled = subprocess.run(command, text=True, capture_output=True, check=False)
    report["compile_exit"] = compiled.returncode
    (directory / "compile.stdout").write_text(compiled.stdout)
    (directory / "compile.stderr").write_text(compiled.stderr)
    print(compiled.stdout, end="")
    print(compiled.stderr, end="")
    if compiled.returncode == 0:
        env = dict(os.environ, ASAN_OPTIONS="detect_leaks=1:abort_on_error=1", UBSAN_OPTIONS="halt_on_error=1")
        result = subprocess.run([str(binary)], text=True, capture_output=True, check=False, env=env)
        report["runtime_exit"] = result.returncode
        (directory / "runtime.stdout").write_text(result.stdout)
        (directory / "runtime.stderr").write_text(result.stderr)
        print(result.stdout, end="")
        print(result.stderr, end="")
    (directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    return 0 if report["compile_exit"] == 0 and report["runtime_exit"] == 0 else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "modules/kern/src/vfs/core.cpp")
    parser.add_argument("--cxx", default=os.environ.get("CXX", "clang++"))
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    if args.output_dir:
        return run(args, args.output_dir)
    with tempfile.TemporaryDirectory(prefix="wos-mkdir-progress-") as temporary:
        return run(args, Path(temporary))


if __name__ == "__main__":
    raise SystemExit(main())
