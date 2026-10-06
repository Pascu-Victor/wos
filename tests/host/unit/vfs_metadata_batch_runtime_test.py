#!/usr/bin/env python3
"""Exercise actual VFS batch dispatch and frozen fsbench fallback decisions.

Complete production function bodies are extracted. Lookup, mount retention and
the remote RPC are controlled endpoints; scalar fsbench operations use real
temporary Linux files. This is not an XFS implementation or VM replacement.
--batch-source permits a preserved pre-fix core.cpp for negative evidence.
"""
import argparse
import os
from pathlib import Path
import re
import subprocess
import tempfile

from exec_trace_ownership_test import function

ROOT = Path(__file__).resolve().parents[3]


def declaration(source, pattern):
    match = re.search(pattern, source, re.M)
    assert match, pattern
    start = match.start()
    end = source.index("{", start) + 1
    depth = 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    assert source[end] == ";"
    end += 1
    return source[start:end]


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fcntl.h>
#include <map>
#include <string>
#include <utility>
#include <vector>
#include <sys/stat.h>
#include <unistd.h>
#include "abi/callnums/vfs.h"
#undef S_IFMT
#undef S_IFBLK
#undef S_IFCHR
#undef S_IFIFO
#undef S_IFREG
#undef S_IFDIR
#undef S_IFLNK
#undef S_IFSOCK
namespace ker::mod::security { struct Snapshot { unsigned identity{19}; }; }
namespace ker::mod::sched::task {
struct Task {
 unsigned captures{};
 ker::mod::security::Snapshot credentials() { ++captures; return {}; }
 uint32_t creation_mask() const { return 0027; }
};
}
'''

LOOKUP_SEAM = r'''
namespace ker::vfs {
constexpr size_t MAX_PATH_LEN=512;
enum class FSType { XFS, REMOTE, TMPFS };
struct MountPoint {
 FSType fs_type{FSType::XFS}; void* private_data{this}; bool read_only{};
 uint64_t dev_id{41}; unsigned refs{}; bool retiring{};
 const char* prefix{"/"};
};
class MountRef {
 MountPoint* pointer{};
public:
 MountRef()=default;
 explicit MountRef(MountPoint* p):pointer(p) { if(p) ++p->refs; }
 MountRef(const MountRef&)=delete;
 MountRef& operator=(const MountRef&)=delete;
 MountRef(MountRef&& other) noexcept: pointer(std::exchange(other.pointer,nullptr)) {}
 MountRef& operator=(MountRef&& other) noexcept {
  if(pointer) --pointer->refs;
  pointer=std::exchange(other.pointer,nullptr); return *this;
 }
 ~MountRef() { if(pointer) --pointer->refs; }
 explicit operator bool() const { return pointer!=nullptr; }
 MountPoint* get() const { return pointer; }
};
MountRef retain_mount_point(MountPoint* p) { return MountRef(p && !p->retiring ? p : nullptr); }
struct LookupResponse {
 MountPoint* mount{}; bool exists{true}; int error{}; bool directory{};
 bool anchor{}; std::string resolved;
};
class LookupHandle {
 MountRef retained;
 LookupResponse response;
public:
 void set(const LookupResponse& value) { response=value; retained=MountRef(value.mount); }
 MountPoint* mount() const { return retained.get(); }
 void* dirfd_anchor() const { return response.anchor ? reinterpret_cast<void*>(1) : nullptr; }
 const char* path() const { return response.resolved.c_str(); }
 size_t path_length() const { return response.resolved.size(); }
 bool requires_directory() const { return response.directory; }
};
struct LookupCall { std::string path; LookupIntent intent; LookupFollowPolicy follow; unsigned identity; };
std::vector<LookupCall> lookup_calls;
std::map<std::string,std::deque<LookupResponse>> responses;
MountPoint local_mount{}, remote_mount{FSType::REMOTE}, other_remote{FSType::REMOTE};
unsigned remote_calls{}, notifications{}, cached_missing{};
std::vector<int> batch_statuses;
MetadataBatchOperation remote_operation{};
uint32_t remote_mode{};
std::vector<std::string> remote_primary,remote_secondary;
int remote_return{};
int remote_item_status{};
ker::mod::sched::task::Task fixture_task;
'''

ENDPOINTS = r'''
int vfs_acquire_lookup(ker::mod::sched::task::Task* task, int dirfd, const char* raw,
 LookupIntent intent, LookupFollowPolicy follow, bool require_directory, bool route,
 LookupHandle* output, unsigned retries, bool owner, const ker::mod::security::Snapshot* credentials) {
 assert(task==&fixture_task && dirfd==AT_FDCWD && !require_directory && route && retries==3 && !owner);
 assert(credentials && credentials->identity==19);
 lookup_calls.push_back({raw,intent,follow,credentials->identity});
 LookupResponse result;
 auto found=responses.find(raw);
 if(found!=responses.end() && !found->second.empty()) {
  result=found->second.front();
  if(found->second.size()>1) found->second.pop_front();
 } else {
  result={.mount=&local_mount,.exists=std::filesystem::exists(raw),.resolved=raw};
 }
 if(result.error) return result.error;
 if(!result.exists && !lookup_intent_allows_missing_final(intent)) return -ENOENT;
 if(result.resolved.empty()) result.resolved=raw;
 output->set(result); return 0;
}
size_t strip_mount_prefix_len(MountPoint* mount,const char* path,size_t length) {
 auto prefix=std::strlen(mount->prefix);
 assert(length>=prefix && std::strncmp(path,mount->prefix,prefix)==0);
 return length-prefix;
}
const char* strip_mount_prefix(MountPoint* mount,const char* path) { return path+std::strlen(mount->prefix); }
void vfs_cache_notify_path_changed(const char*,const char*) { ++notifications; }
void metadata_cache_store_missing_path_on_current_mount(const char*,MountPoint*,size_t) { ++cached_missing; }
}
namespace ker::net::wki {
int wki_remote_vfs_metadata_batch(void* provider,ker::vfs::MetadataBatchOperation operation,uint32_t mode,
 const ker::vfs::MetadataBatchEntry* entries,size_t count,ker::vfs::MetadataBatchResult* results,
 bool* attempted,const ker::mod::security::Snapshot* credentials) {
 using namespace ker::vfs;
 auto* mount=static_cast<MountPoint*>(provider);
 assert(mount && mount->refs>0 && credentials->identity==19);
 ++remote_calls; remote_operation=operation; remote_mode=mode;
 *attempted=operation!=MetadataBatchOperation::STAT_FOLLOW;
 for(size_t i=0;i<count;++i) {
  remote_primary.emplace_back(entries[i].path);
  if(entries[i].second_path) remote_secondary.emplace_back(entries[i].second_path);
  results[i].status=remote_item_status; results[i].stat.st_size=0;
 }
 return remote_return;
}
}
'''

BENCH_BRIDGE = r'''
namespace ker::abi::vfs {
int metadata_batch(const metadata_batch_header* header,const metadata_batch_entry* entries,metadata_batch_result* results) {
 assert(header && header->version==1 && header->count>0 && header->count<=64);
 std::array<ker::vfs::MetadataBatchEntry,64> copied{};
 std::array<ker::vfs::MetadataBatchResult,64> completed{};
 auto const captures=ker::vfs::fixture_task.captures;
 for(size_t i=0;i<header->count;++i) copied[i]={entries[i].path,entries[i].second_path};
 int result=ker::vfs::vfs_metadata_batch(&ker::vfs::fixture_task,
  static_cast<ker::vfs::MetadataBatchOperation>(header->operation),header->mode,copied.data(),header->count,completed.data());
 assert(ker::vfs::fixture_task.captures==captures+1);
 ker::vfs::batch_statuses.push_back(result);
 for(size_t i=0;i<header->count;++i) {
  results[i].status=completed[i].status;
  std::memcpy(&results[i].statbuf,&completed[i].stat,sizeof(completed[i].stat));
 }
 return result;
}
}
struct MetadataPathWorkers {
 uint32_t worker_count{1};
 const std::vector<std::string>* primary_paths{};
 const std::vector<std::string>* secondary_paths{};
 MetadataPathAction action{MetadataPathAction::CLEANUP};
};
'''

TESTS = r'''
using namespace ker::vfs;
unsigned failures{},checks{};
void check(bool predicate,const char* name) {
 ++checks;
 if(!predicate) { ++failures; std::fprintf(stderr,"FAIL: %s\n",name); }
}
void reset() {
 check(local_mount.refs==0 && remote_mount.refs==0 && other_remote.refs==0,"mount refs retired");
 responses.clear(); lookup_calls.clear(); remote_primary.clear(); remote_secondary.clear();
 batch_statuses.clear();
 remote_calls=notifications=cached_missing=0; remote_return=remote_item_status=0;
 fixture_task.captures=0; local_mount.read_only=remote_mount.read_only=other_remote.read_only=false;
 local_mount.retiring=remote_mount.retiring=other_remote.retiring=false;
 remote_mount.prefix=other_remote.prefix="/remote/";
}
int batch(MetadataBatchOperation op,const std::vector<MetadataBatchEntry>& entries,std::vector<MetadataBatchResult>* output=nullptr,
 uint32_t mode=0) {
 std::vector<MetadataBatchResult> results(entries.size());
 int ret=vfs_metadata_batch(&fixture_task,op,mode,entries.data(),entries.size(),results.data());
 if(output) *output=results;
 return ret;
}
void answer(const char* path,MountPoint* mount,bool exists=true,int error=0,bool directory=false,bool anchor=false,
 std::string resolved={}) {
 responses[path].push_back({mount,exists,error,directory,anchor,std::move(resolved)});
}
void cases(const std::string& directory) {
 // Drive all complete production benchmark phases on actual temporary files.
 for(unsigned count:{1U,64U,1000U}) {
  reset(); auto created=iteration_paths((directory+"/create").c_str(),".create.",count);
  MetadataPathWorkers pool{.primary_paths=&created};
  bool initial=execute_metadata_path_range(pool,0);
  std::printf("local-create count=%u initial_cleanup=%u first_batch_status=%d first_batch_lookup=%u\n",count,unsigned(initial),
              batch_statuses.front(),unsigned(lookup_calls.size()));
  check(initial,"local missing cleanup must allow scalar fallback");
  pool.action=MetadataPathAction::CREATE;
  check(execute_metadata_path_range(pool,0),"local create scalar fallback");
  pool.action=MetadataPathAction::VERIFY_EMPTY;
  check(execute_metadata_path_range(pool,0),"local created files exactly empty");
  pool.action=MetadataPathAction::CLEANUP;
  check(execute_metadata_path_range(pool,0),"local existing cleanup scalar fallback");
  check(execute_metadata_path_range(pool,0),"local repeated absent cleanup scalar fallback");
  check(remote_calls==0,"no RPC for local benchmark");
  reset(); auto sources=iteration_paths((directory+"/rename").c_str(),".source.",count);
  auto destinations=iteration_paths((directory+"/rename").c_str(),".destination.",count);
  pool={.primary_paths=&sources};
  check(execute_metadata_path_range(pool,0),"rename initial source cleanup");
  pool.primary_paths=&destinations;
  check(execute_metadata_path_range(pool,0),"rename initial destination cleanup");
  pool.primary_paths=&sources; pool.action=MetadataPathAction::CREATE;
  check(execute_metadata_path_range(pool,0),"rename create sources");
  pool.secondary_paths=&destinations; pool.action=MetadataPathAction::RENAME;
  check(execute_metadata_path_range(pool,0),"rename existing source scalar fallback");
  pool.primary_paths=&destinations; pool.secondary_paths=nullptr; pool.action=MetadataPathAction::VERIFY_EMPTY;
  check(execute_metadata_path_range(pool,0),"rename destination exact empty");
  pool.primary_paths=&sources; pool.action=MetadataPathAction::VERIFY_ABSENT;
  check(execute_metadata_path_range(pool,0),"rename sources absent");
  pool.action=MetadataPathAction::CLEANUP;
  batch_statuses.clear();
  bool final=execute_metadata_path_range(pool,0);
  std::printf("local-rename count=%u final_absent_cleanup=%u first_batch_status=%d\n",count,unsigned(final),batch_statuses.front());
  check(final,"rename final absent source cleanup scalar fallback");
  pool.primary_paths=&destinations;
  check(execute_metadata_path_range(pool,0),"rename final destination cleanup");
  check(remote_calls==0,"no RPC for local rename");
 }
 reset(); answer("/remote/file",&remote_mount);
 check(batch(MetadataBatchOperation::UNLINK,{{"/remote/file",nullptr}})==0,"existing remote unlink submitted");
 check(remote_calls==1 && remote_operation==MetadataBatchOperation::UNLINK && remote_primary==std::vector<std::string>{"file"},
       "remote unlink exact relative path");
 check(lookup_calls.size()==1 && lookup_calls[0].intent==LookupIntent::REMOVE &&
       lookup_calls[0].follow==LookupFollowPolicy::NOFOLLOW_FINAL && fixture_task.captures==1,"remote unlink original authorization");
 reset(); answer("/remote/missing",&remote_mount,false);
 check(batch(MetadataBatchOperation::UNLINK,{{"/remote/missing",nullptr}})==-ENOENT,"missing remote preserves original ENOENT");
 check(remote_calls==0 && notifications==0,"no mutation after remote preflight failure");
 reset(); answer("/looks-local/file",&remote_mount,true,0,false,false,"/remote/routed-file");
 check(batch(MetadataBatchOperation::UNLINK,{{"/looks-local/file",nullptr}})==0 &&
       remote_primary==std::vector<std::string>{"routed-file"},"task routing chooses actual remote mount before local fallback");
 reset(); answer("/looks-local/missing",&remote_mount,false,0,false,false,"/remote/routed-missing");
 check(batch(MetadataBatchOperation::UNLINK,{{"/looks-local/missing",nullptr}})==-ENOENT && remote_calls==0 &&
       fixture_task.captures==1,"missing task-routed remote entry cannot become local fallback");
 reset(); answer("/missing-parent/file",&local_mount,false,-ENOENT);
 check(batch(MetadataBatchOperation::UNLINK,{{"/missing-parent/file",nullptr}})==-ENOENT && remote_calls==0,
       "missing parent fails capability lookup and retains operation error");
 reset(); answer("/remote/a",&remote_mount); answer("/remote/b",&remote_mount,false);
 check(batch(MetadataBatchOperation::RENAME,{{"/remote/a","/remote/b"}})==0,"remote rename missing target remains supported");
 check(lookup_calls.size()==2 && lookup_calls[0].intent==LookupIntent::RENAME_SOURCE &&
       lookup_calls[1].intent==LookupIntent::RENAME_TARGET && remote_secondary==std::vector<std::string>{"b"},"rename intent/path preservation");
 reset(); answer("/remote/a",&remote_mount); answer("/remote/b",&other_remote);
 check(batch(MetadataBatchOperation::RENAME,{{"/remote/a","/remote/b"}})==-EOPNOTSUPP && remote_calls==0,"different retained remote mounts decline before RPC");
 reset(); answer("/remote/a",&remote_mount); answer("/local/b",&local_mount,false);
 check(batch(MetadataBatchOperation::RENAME,{{"/remote/a","/local/b"}})==-EOPNOTSUPP && remote_calls==0,"mixed local/remote no attempted mutation");
 reset(); answer("/remote/dir/",&remote_mount,true,0,true);
 check(batch(MetadataBatchOperation::STAT_FOLLOW,{{"/remote/dir/",nullptr}})==-EOPNOTSUPP && remote_calls==0,"trailing slash scalar semantics");
 reset(); answer("/remote/a",&remote_mount,true,0,false,true);
 check(batch(MetadataBatchOperation::UNLINK,{{"/remote/a",nullptr}})==-EOPNOTSUPP && remote_calls==0,"retained dirfd anchor declines batch");
 for(int error:{-EACCES,-EAGAIN,-ENOTDIR,-ELOOP}) {
  reset(); answer("/local/error",&local_mount,false,error);
  check(batch(MetadataBatchOperation::UNLINK,{{"/local/error",nullptr}})==error && lookup_calls.size()==1 && remote_calls==0,
        "non-ENOENT errors preserved without capability retry");
 }
 for(int error:{-EACCES,-EAGAIN,-ENOTDIR}) {
  reset(); answer("/probe/error",&local_mount,false,-ENOENT); answer("/probe/error",&local_mount,false,error);
  check(batch(MetadataBatchOperation::UNLINK,{{"/probe/error",nullptr}})==-ENOENT && remote_calls==0,
        "capability probe failure preserves original ENOENT");
 }
 reset(); answer("/race/missing",&remote_mount,false,-ENOENT); answer("/race/missing",&other_remote,false);
 check(batch(MetadataBatchOperation::UNLINK,{{"/race/missing",nullptr}})==-ENOENT && remote_calls==0,
       "changing remote mount cannot turn missing operation into mutation or fallback");
 reset(); remote_mount.read_only=true; answer("/remote/file",&remote_mount);
 std::vector<MetadataBatchResult> results;
 check(batch(MetadataBatchOperation::UNLINK,{{"/remote/file",nullptr}},&results)==-EROFS && results[0].status==-EROFS && remote_calls==0,
       "read-only mutation rejected before owner call");
 reset(); answer("/remote/new",&remote_mount,false);
 check(batch(MetadataBatchOperation::CREATE_CLOSE,{{"/remote/new",nullptr}},nullptr,0677)==0 && remote_mode==0650,
       "create mode uses captured task umask");
 reset(); answer("/remote/missing",&remote_mount,false); remote_item_status=-ENOENT;
 check(batch(MetadataBatchOperation::STAT_FOLLOW,{{"/remote/missing",nullptr}},&results)==0 && results[0].status==-ENOENT && cached_missing==1,
       "remote missing stat keeps per-item result and cache update");
 reset(); answer("/remote/file",&remote_mount); remote_return=-ETIMEDOUT; remote_item_status=-EINPROGRESS;
 check(batch(MetadataBatchOperation::UNLINK,{{"/remote/file",nullptr}},&results)==-ETIMEDOUT && remote_calls==1 && notifications==1,
       "ambiguous attempted mutation remains one owner call with invalidation");
 reset();
 std::vector<MetadataBatchEntry> too_many(65,{"/x",nullptr});
 check(batch(MetadataBatchOperation::UNLINK,too_many)==-EINVAL && lookup_calls.empty(),"bounded 64-item admission");
 check(batch(MetadataBatchOperation::UNLINK,{{nullptr,nullptr}})==-EINVAL && remote_calls==0,"null item rejected without RPC");
 check(batch(MetadataBatchOperation::UNLINK,{{"/x",nullptr}},nullptr,1)==-EINVAL && lookup_calls.empty(),"invalid non-create mode rejected before lookup");
 reset();
 std::printf("vfs-metadata-batch checks=%u failures=%u\n",checks,failures);
}
int main(int argc,char** argv) {
 assert(argc==2); cases(argv[1]); return failures==0 ? 0 : 1;
}
'''


def translation_unit(batch_source):
    core = batch_source.read_text()
    bench = (ROOT / "modules/testprog/src/fsbench.cpp").read_text()
    lookup = (ROOT / "modules/kern/src/vfs/lookup.hpp").read_text()
    vfs = (ROOT / "modules/kern/src/vfs/vfs.hpp").read_text()
    stat = (ROOT / "modules/kern/src/vfs/stat.hpp").read_text()
    user = (ROOT / "toolchain/src/mlibc/sysdeps/wos/include/sys/vfs.h").read_text()
    stat_namespace = stat[stat.index("namespace ker::vfs {"):stat.index("// File type mode flags")] + "\n}\n"
    types = "namespace ker::vfs {\n" + "\n".join([
        declaration(vfs, r"^enum class MetadataBatchOperation[^;]*\{"),
        declaration(vfs, r"^struct MetadataBatchEntry \{"),
        declaration(vfs, r"^struct MetadataBatchResult \{"),
        declaration(lookup, r"^enum class LookupIntent[^;]*\{"),
        declaration(lookup, r"^enum class LookupFollowPolicy[^;]*\{"),
    ]) + "\n}\n"
    user_types = "namespace ker::abi::vfs {\n" + "\n".join([
        declaration(user, r"^struct metadata_batch_entry \{"),
        declaration(user, r"^struct metadata_batch_result \{"),
    ]) + "\n}\n"
    bench_functions = "\n".join(function(bench, name) for name in [
        "iteration_paths", "apply_metadata_path_action", "metadata_batch_operation_for_action",
        "metadata_batch_result_succeeded", "execute_metadata_path_range"])
    return (PRELUDE + stat_namespace + types + user_types + LOOKUP_SEAM
            + function(core, "lookup_intent_allows_missing_final") + ENDPOINTS
            + "namespace ker::vfs {\n" + function(core, "vfs_metadata_batch") + "\n}\n"
            + declaration(bench, r"^enum class MetadataPathAction[^;]*\{")
            + BENCH_BRIDGE + bench_functions + TESTS)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-source", type=Path, default=ROOT / "modules/kern/src/vfs/core.cpp")
    parser.add_argument("--output", type=Path, help="Keep generated source and executable in this directory")
    args = parser.parse_args()
    env = dict(os.environ)
    env.setdefault("ASAN_OPTIONS", "detect_leaks=0")
    with tempfile.TemporaryDirectory(prefix="vfs-metadata-batch-", dir=env.get("TMPDIR")) as temporary:
        work = args.output.resolve() if args.output else Path(temporary)
        work.mkdir(parents=True, exist_ok=True)
        source = work / "actual-metadata-batch.cpp"
        binary = work / "actual-metadata-batch"
        files = work / "files"
        files.mkdir(exist_ok=True)
        source.write_text(translation_unit(args.batch_source))
        subprocess.run(["/usr/bin/clang++", "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Werror",
                        "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-I", str(ROOT / "modules/kern/src"),
                        str(source), "-o", str(binary)], check=True, env=env, timeout=60)
        subprocess.run([str(binary), str(files)], check=True, env=env, timeout=30)


if __name__ == "__main__":
    main()
