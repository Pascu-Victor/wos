#!/usr/bin/env python3
"""Compile the real prepared-mount retry against changing admission snapshots."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def function(source):
    start = source.index("auto consume_prepared_mount_target(")
    end = source.index("{", start) + 1
    depth = 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[start:end]


PRELUDE = r'''
#include <array>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
namespace ker::mod::security {
struct Snapshot { int value = 1; bool operator==(const Snapshot&) const = default; };
}
namespace ker::mod::sched::task { struct Task {}; }
using Snapshot = ker::mod::security::Snapshot;
enum class FSType { TMPFS, XFS };
enum class LookupIntent { MOUNT_TARGET };
enum class LookupFollowPolicy { FOLLOW_FINAL };
constexpr int AT_FDCWD = -100;
struct Authorization {
    uint64_t task_id = 1, root_hash = 2, cwd_hash = 3, root_anchor_identity = 4, cwd_anchor_identity = 5;
    Snapshot credentials;
    bool root_identity_view = false;
};
struct Mount { FSType fs_type = FSType::TMPFS; uint32_t dev_id = 1; };
unsigned resets = 0, acquires = 0, consumes = 0;
struct LookupHandle {
    static constexpr size_t PATH_CAPACITY = 512;
    bool alive = true, view = false;
    Mount owner;
    uint64_t parent = 10, object = 11;
    char text[PATH_CAPACITY] = "/mount";
    Authorization auth;
    Mount* mount() const { return alive ? const_cast<Mount*>(&owner) : nullptr; }
    bool directory_view() const { return view; }
    uint64_t parent_identity() const { return parent; }
    uint64_t object_identity() const { return object; }
    const char* path() const { return text; }
    size_t path_length() const { return std::strlen(text); }
    const Authorization& authorization() const { return auth; }
    void reset() { assert(alive); alive = false; ++resets; }
};
struct PreparedMount { int state = 42; };
std::array<int,4> results;
int acquire_error = 0;
LookupHandle next_lookup;
int consume_mount_target(const LookupHandle& handle, const ker::mod::sched::task::Task&, const Snapshot&, PreparedMount* prepared) {
    assert(handle.alive && prepared->state == 42 && consumes < results.size());
    return results[consumes++];
}
int vfs_acquire_lookup(ker::mod::sched::task::Task*, int fd, const char* target, LookupIntent intent, LookupFollowPolicy follow,
                       bool directory, bool route, LookupHandle* out, unsigned budget, bool resolved, const Snapshot*) {
    assert(!out->alive && fd == AT_FDCWD && std::strcmp(target,"/mount") == 0);
    assert(intent == LookupIntent::MOUNT_TARGET && follow == LookupFollowPolicy::FOLLOW_FINAL);
    assert(directory && route && budget == 3 && !resolved);
    ++acquires;
    if (acquire_error) return acquire_error;
    *out = next_lookup;
    return 0;
}
void reset() {
    resets = acquires = consumes = 0; acquire_error = 0; next_lookup = {};
    results = {-EAGAIN,0,0,0};
}
'''

CASES = r'''
int run(LookupHandle& lookup) {
    ker::mod::sched::task::Task task; Snapshot credentials; PreparedMount prepared;
    int result = consume_prepared_mount_target(lookup,"/mount",task,credentials,prepared);
    assert(prepared.state == 42); return result;
}
int main() {
    reset(); LookupHandle lookup;
    assert(run(lookup) == 0 && acquires == 1 && resets == 1 && consumes == 2);
    reset(); lookup = {}; results = {0,0,0,0};
    assert(run(lookup) == 0 && acquires == 0 && resets == 0 && consumes == 1);
    reset(); lookup = {}; results = {-EPERM,0,0,0};
    assert(run(lookup) == -EPERM && acquires == 0);
    reset(); lookup = {}; acquire_error = -EACCES;
    assert(run(lookup) == -EACCES && acquires == 1 && resets == 1 && consumes == 1);
    reset(); lookup = {}; results = {-EAGAIN,-EAGAIN,-EAGAIN,-EAGAIN};
    assert(run(lookup) == -EAGAIN && acquires == 3 && resets == 3 && consumes == 4);
    reset(); lookup = {}; results = {-EAGAIN,-EAGAIN,0,0};
    assert(run(lookup) == 0 && acquires == 2 && consumes == 3);
    for (unsigned change = 0; change < 12; ++change) {
        reset(); lookup = {};
        switch(change) {
            case 0: ++next_lookup.object; break;
            case 1: ++next_lookup.parent; break;
            case 2: ++next_lookup.owner.dev_id; break;
            case 3: next_lookup.owner.fs_type = FSType::XFS; break;
            case 4: std::strcpy(next_lookup.text,"/other"); break;
            case 5: ++next_lookup.auth.task_id; break;
            case 6: ++next_lookup.auth.credentials.value; break;
            case 7: ++next_lookup.auth.root_hash; break;
            case 8: ++next_lookup.auth.cwd_hash; break;
            case 9: ++next_lookup.auth.root_anchor_identity; break;
            case 10: ++next_lookup.auth.cwd_anchor_identity; break;
            case 11: next_lookup.auth.root_identity_view = true; break;
        }
        assert(run(lookup) == -EAGAIN && acquires == 1 && consumes == 1);
    }
    reset(); lookup = {}; next_lookup.view = true;
    assert(run(lookup) == -EAGAIN && consumes == 1);
    reset(); lookup = {}; lookup.owner.fs_type = FSType::XFS;
    assert(run(lookup) == -EAGAIN && acquires == 0);
    reset(); lookup = {}; lookup.view = true;
    assert(run(lookup) == -EAGAIN && acquires == 0);
    reset(); lookup = {}; lookup.object = 0;
    assert(run(lookup) == -EAGAIN && acquires == 0);
    reset(); lookup = {}; lookup.owner.dev_id = 0;
    assert(run(lookup) == -EAGAIN && acquires == 0);
    std::puts("PASS prepared mount reuse, bounded retries, replacement and authority rejection");
}
'''


def main():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    compiler = os.environ.get("HOST_CXX") or shutil.which("c++", path=os.defpath)
    assert compiler, "host C++ compiler required"
    with tempfile.TemporaryDirectory(prefix="wos-mount-retry-") as directory:
        harness, binary = Path(directory) / "retry.cpp", Path(directory) / "retry"
        harness.write_text(PRELUDE + function(source) + CASES)
        subprocess.run([compiler, "-std=c++20", "-Wall", "-Wextra", "-Werror", str(harness), "-o", str(binary)], check=True)
        subprocess.run([str(binary)], check=True, timeout=10)


if __name__ == "__main__":
    main()
