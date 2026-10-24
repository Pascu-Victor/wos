#!/usr/bin/env python3
"""Compile actual unlink/serialized/consume bodies against controlled endpoints.

Persistent unrelated namespace publication invalidates every optimistic lookup;
only a walk and consume in the same publication interval can remove the target.
The fixture also checks request policy, backend boundaries and reference release.
It models neither the native walker/XFS implementation nor VM scheduling, and it
cannot attribute diagnostic case 075 to a particular real-kernel stale check.
--source accepts a preserved pre-fix core.cpp: compilation must succeed and the
progress oracle must fail at runtime, rather than treating compile errors as proof.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


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
unsigned authorization_checks{}, unrelated_mutations{};
enum class Churn { NONE, ACQUIRE, CONSUME };
Churn churn{};
bool invalidate_on_lock{}, unsupported{}, fail_serialized{}, bad_authorization{}, invalid_view{};
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
enum class LookupIntent { OPEN, STAT, REMOVE };
enum class LookupFollowPolicy { FOLLOW_FINAL, NOFOLLOW_FINAL };
enum class FSType { TMPFS, XFS, REMOTE, PROCFS };
struct Scope {
 bool host{true}, shut{}; uint64_t version{7};
 bool is_host() const { return host; }
 bool closed() const { return shut; }
 uint64_t generation() const { return version; }
};
struct MountPoint {
 FSType fs_type{FSType::TMPFS}; Scope namespace_owner{};
 std::atomic<bool> retiring{false}; void* private_data{this};
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
bool expected_directory{};
}
namespace ker::mod::sched::task {
struct Task {
 ker::mod::security::Snapshot credentials() {
  return ++fixture::credential_reads==1 ? ker::vfs::original : ker::vfs::drifted;
 }
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
bool lookup_path_cache_key_still_stable(const LookupHandle&) { return true; }
int validate_lookup_directory_view_locked(const LookupHandle&) {
 assert(fixture::publication==1); ++fixture::view_checks; return fixture::invalid_view ? -EACCES : 0;
}
void metadata_cache_note_path_changed(const char*,void*) { assert(fixture::publication==1); }
void vfs_cache_notify_path_changed(const char*,void*) { ++fixture::notifications; }
void metadata_cache_store_missing_path_on_current_mount(const char*,MountPoint*,size_t,size_t) {}
const char* strip_mount_prefix(MountPoint*,const char* path) { return path; }
namespace tmpfs {
bool tmpfs_validate_lookup(const TmpfsLookupHandle& b) { return b.valid; }
}
namespace xfs {
int xfs_validate_lookup(const XfsLookupHandle& b) { return b.valid ? 0 : -EAGAIN; }
}
int mutate(const ker::mod::security::Snapshot& credentials,Backend& b,bool directory,bool readonly) {
 assert(fixture::publication==1 && credentials==original);
 ++fixture::backend_calls;
 if(readonly) return -EROFS;
 if(fixture::backend_error) return fixture::backend_error;
 if(!b.node->present) return -ENOENT;
 if(directory && (!b.node->directory || b.node->symlink)) return -ENOTDIR;
 if(!directory && b.node->directory && !b.node->symlink) return -EISDIR;
 b.node->present=false; return 0;
}
namespace tmpfs {
int tmpfs_rmdir_lookup(const ker::mod::security::Snapshot& c,TmpfsLookupHandle& b,bool ro) { return mutate(c,b,true,ro); }
int tmpfs_unlink_lookup(const ker::mod::security::Snapshot& c,TmpfsLookupHandle& b,bool* changed,bool ro) {
 *changed=false; return mutate(c,b,false,ro);
}
}
namespace xfs {
int xfs_rmdir_lookup(const ker::mod::security::Snapshot& c,XfsLookupHandle& b,bool ro) { return mutate(c,b,true,ro); }
int xfs_unlink_lookup(const ker::mod::security::Snapshot& c,XfsLookupHandle& b,bool ro) { return mutate(c,b,false,ro); }
}
int vfs_rmdir_resolved_path(const ker::mod::security::Snapshot&,const char*,size_t,size_t,bool) {
 assert(false && "unsupported fallback must not reach compatibility backend"); return -EIO;
}
int vfs_unlink_resolved_path(const ker::mod::security::Snapshot&,const char*,size_t,size_t,bool) {
 assert(false && "unsupported fallback must not reach compatibility backend"); return -EIO;
}
void policy_arguments(ker::mod::sched::task::Task* subject,int dirfd,const char* path,LookupIntent intent,
 LookupFollowPolicy follow,bool directory,bool route,const ker::mod::security::Snapshot* credentials) {
 assert(subject==&task && dirfd==expected_dirfd && std::strcmp(path,expected_path)==0);
 assert(intent==LookupIntent::REMOVE && follow==LookupFollowPolicy::NOFOLLOW_FINAL && route);
 assert(directory==expected_directory && credentials && *credentials==original);
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
 assert(!references->held); references->held=true; fixture::retain(); ++fixture::serialized;
 policy_arguments(subject,dirfd,path,intent,follow,directory,route,credentials);
 if(fixture::unsupported || mount.fs_type==FSType::REMOTE || mount.fs_type==FSType::PROCFS) return RESOLVE_FAST_PATH_DECLINED;
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
int wki_remote_vfs_rmdir(void*,const char* p,const ker::mod::security::Snapshot& c,ker::vfs::File* d) { return remote(p,c,d); }
int wki_remote_vfs_unlink(void*,const char* p,const ker::mod::security::Snapshot& c,bool directory,ker::vfs::File* d) {
 assert(directory==ker::vfs::expected_directory); return remote(p,c,d);
}
}
namespace ker::vfs {
'''

CASES = r'''
void reset(FSType type=FSType::TMPFS,bool directory=true) {
 assert(!fixture::publication && !fixture::resources && fixture::locks==fixture::unlocks);
 fixture::locks=fixture::unlocks=fixture::releases=fixture::credential_reads=0;
 fixture::optimistic=fixture::serialized=fixture::backend_calls=fixture::remote_calls=0;
 fixture::notifications=fixture::view_checks=fixture::authorization_checks=fixture::unrelated_mutations=0;
 fixture::churn=fixture::Churn::NONE;
 fixture::invalidate_on_lock=fixture::unsupported=fixture::fail_serialized=fixture::bad_authorization=fixture::invalid_view=false;
 fixture::acquire_error=fixture::backend_error=fixture::remote_error=0;
 mount.fs_type=type; mount.namespace_owner={}; mount.retiring=false;
 request_scope={}; selected_scope=&request_scope; leaf={true,directory,false}; referent={true,true,false}; policy={};
 g_vfs_namespace_generation=41; symlink_case=remote_relative=anchored=false;
 expected_path="child"; expected_dirfd=29; expected_directory=directory;
}
int remove() { return vfs_unlinkat(&task,expected_dirfd,expected_path,expected_directory ? AT_REMOVEDIR : 0); }
void settled() { assert(!fixture::publication && !fixture::resources && fixture::locks==fixture::unlocks); }
void progress() {
 for(auto type:{FSType::TMPFS,FSType::XFS}) for(bool directory:{false,true})
 for(auto churn:{fixture::Churn::ACQUIRE,fixture::Churn::CONSUME}) for(bool absolute:{false,true}) {
  reset(type,directory); fixture::churn=churn;
  if(absolute) { expected_path="/root/child"; expected_dirfd=9876; }
  int result=remove();
  assert(result==0 && !leaf.present); // Pre-fix source compiles, then fails this progress oracle.
  assert(fixture::serialized==1 && fixture::backend_calls==1 && fixture::remote_calls==0);
  assert(fixture::credential_reads==1 && fixture::optimistic==4 && fixture::unrelated_mutations==4);
  // Churn advances once per invalidation; the real consume publishes the successful mutation once.
  assert(g_vfs_namespace_generation==41+fixture::unrelated_mutations+1);
  assert(fixture::releases>=2); settled();
 }
}
void default_nonmutating() {
 reset();
 unsigned observed=0;
 auto read_only=[](const LookupHandle&,void* output)->int {
  assert(fixture::publication==1); ++*static_cast<unsigned*>(output); return 0;
 };
 // Use the existing helper signature: its default must remain nonmutating.
 assert(native_lookup_serialized(&task,expected_dirfd,expected_path,LookupIntent::REMOVE,
        LookupFollowPolicy::NOFOLLOW_FINAL,true,true,read_only,&observed,&original,nullptr)==0);
 assert(observed==1 && fixture::serialized==1 && fixture::backend_calls==0 && leaf.present);
 assert(g_vfs_namespace_generation==41 && fixture::credential_reads==0 && fixture::releases==2); settled();
}
void fast_and_errors() {
 for(auto type:{FSType::TMPFS,FSType::XFS}) for(bool directory:{false,true}) {
  reset(type,directory); assert(remove()==0 && !leaf.present && fixture::serialized==0);
  assert(fixture::backend_calls==1 && g_vfs_namespace_generation==42 && fixture::credential_reads==1); settled();
  for(int error:{-EACCES,-ENOENT,-EIO}) {
   reset(type,directory); fixture::acquire_error=error;
   assert(remove()==error && leaf.present && fixture::backend_calls==0 && fixture::serialized==0); settled();
  }
  for(int error:{-ENOTEMPTY,-EIO}) {
   reset(type,directory); fixture::backend_error=error;
   assert(remove()==error && leaf.present && fixture::backend_calls==1 && fixture::serialized==0);
   assert(g_vfs_namespace_generation==41); settled();
  }
  reset(type,directory); policy.read_only=true;
  assert(remove()==-EROFS && leaf.present && fixture::serialized==0 && g_vfs_namespace_generation==41); settled();
 }
 reset(); assert(vfs_unlinkat(nullptr,29,"child",AT_REMOVEDIR)==-ESRCH);
 assert(vfs_unlinkat(&task,29,nullptr,0)==-EINVAL && vfs_unlinkat(&task,29,"",0)==-ENOENT);
 assert(vfs_unlinkat(&task,29,"child",AT_REMOVEDIR|1)==-EINVAL);
 assert(fixture::credential_reads==0 && fixture::optimistic==0); settled();
}
void boundaries() {
 for(int scope:{0,1}) {
  reset(); fixture::churn=fixture::Churn::ACQUIRE;
  if(scope==0) selected_scope=nullptr; else request_scope.host=false;
  assert(remove()==-EAGAIN && leaf.present && !fixture::serialized && !fixture::backend_calls); settled();
 }
 for(auto type:{FSType::PROCFS,FSType::REMOTE}) {
  reset(type); fixture::churn=fixture::Churn::ACQUIRE;
  assert(remove()==-EAGAIN && leaf.present && fixture::serialized==1 && !fixture::backend_calls && !fixture::remote_calls); settled();
 }
 reset(); fixture::churn=fixture::Churn::ACQUIRE; fixture::fail_serialized=true;
 assert(remove()==-EIO && fixture::serialized==1 && !fixture::backend_calls && fixture::releases==1); settled();
 for(bool relative:{false,true}) for(bool directory:{false,true}) for(int error:{0,-EAGAIN,-EIO}) {
  reset(FSType::REMOTE,directory); fixture::remote_error=error; remote_relative=relative; anchored=relative;
  assert(remove()==error && fixture::remote_calls==1 && !fixture::serialized && !fixture::backend_calls);
  assert(fixture::locks==0 && fixture::credential_reads==1); settled();
 }
 reset(FSType::REMOTE); policy.read_only=true;
 assert(remove()==-EROFS && !fixture::remote_calls && !fixture::locks); settled();
 for(bool auth:{false,true}) {
  reset(); fixture::churn=fixture::Churn::ACQUIRE;
  fixture::bad_authorization=auth; fixture::invalid_view=!auth;
  assert(remove()==(auth ? -EAGAIN : -EACCES) && leaf.present && !fixture::backend_calls);
  assert(g_vfs_namespace_generation==41+fixture::unrelated_mutations); settled();
 }
 // Final symlink identity must be removed, never its referent; rmdir must reject it.
 for(bool directory:{false,true}) {
  reset(FSType::XFS,directory); symlink_case=true; leaf.directory=false; leaf.symlink=true;
  fixture::churn=fixture::Churn::CONSUME;
  assert(remove()==(directory ? -ENOTDIR : 0));
  assert(leaf.present==directory && referent.present && fixture::credential_reads==1); settled();
 }
}
} // namespace ker::vfs
int main() {
 ker::vfs::progress(); ker::vfs::default_nonmutating(); ker::vfs::fast_and_errors(); ker::vfs::boundaries();
 std::puts("Actual unlink progress PASS: persistent acquisition/consume churn, request policy, host/native boundaries, mutation publication and unlocked retirement");
}
'''


def run(args, directory):
    directory.mkdir(parents=True, exist_ok=True)
    source = args.source.read_text()
    bodies = [function(source, name) for name in
              ("consume_lookup_locked", "native_lookup_serialized", "vfs_unlinkat")]
    generated = PRELUDE + "\n\n".join(bodies) + "\n" + CASES
    cpp, binary = directory / "fixture.cpp", directory / "fixture"
    cpp.write_text(generated)
    command = [args.cxx, "-std=c++20", "-O1", "-g", "-fno-omit-frame-pointer",
               "-fsanitize=address,undefined", str(cpp), "-o", str(binary)]
    report = {"source": str(args.source), "source_sha256": hashlib.sha256(source.encode()).hexdigest(),
              "actual_functions": ["consume_lookup_locked", "native_lookup_serialized", "vfs_unlinkat"],
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
    with tempfile.TemporaryDirectory(prefix="wos-unlink-progress-") as temporary:
        return run(args, Path(temporary))


if __name__ == "__main__":
    raise SystemExit(main())
