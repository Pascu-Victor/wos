#!/usr/bin/env python3
"""Compile actual kill/group routing source with deterministic host endpoints.

The kill function, group selector/storage/wrapper and origin conversions are
extracted unchanged. Session identity, credentials, policy and wire layouts use
real headers. Task/peer storage, capability admission, clocks and RPC endpoints
are doubles; this does not test kernel concurrency, remote authentication,
migration admission or real network transport. --source accepts an unchanged
saved process.cpp; its unaffected controls run before the collision regression.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[3]
KERN = ROOT / "modules/kern/src"
PROCESS = KERN / "syscalls_impl/process/process.cpp"
REMOTE = KERN / "net/wki/remote_compute.cpp"


def definition(source: str, name: str, *, record: bool = False) -> str:
    prefix = r"(?:class|struct)" if record else r"(?:inline\s+)?(?:auto|void)"
    match = re.search(rf"^(?:\[\[nodiscard\]\]\s+)?{prefix}\s+{re.escape(name)}\b[^;]*?\{{", source, re.MULTILINE)
    if match is None:
        raise AssertionError(f"missing actual definition: {name}")
    pos, depth = match.end(), 1
    while depth and pos < len(source):
        depth += (source[pos] == "{") - (source[pos] == "}")
        pos += 1
    if depth:
        raise AssertionError(f"unterminated actual definition: {name}")
    return source[match.start():pos] + (";" if record else "")


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <platform/sched/session.hpp>
#include <platform/security/authorization.hpp>
#include <platform/security/credential_record.hpp>
#include <net/wki/wire.hpp>

namespace fixture {
using Snapshot = ker::mod::security::Snapshot;
using Identity = ker::mod::sched::session::Identity;
using Origin = ker::mod::sched::session::Origin;
using Home = ker::net::wki::TaskSignalHome;
struct SignalHome { Origin origin{}; uint64_t serial = 0; };
void require(bool value, const char* message) {
    if (!value) { std::fprintf(stderr, "FAIL: %s\n", message); std::exit(1); }
}
}
namespace ker::mod::sched::task {
struct Task {
    /* actual task constants */
    uint64_t pid = 0, owner_pid = 0;
    uint32_t wki_target_flags = 0;
    fixture::Snapshot creds = fixture::Snapshot::bootstrap_root();
    // Keep fixture storage in the initiating node's canonical origin view.
    // RPC delivery below normalizes both identities to the receiving node.
    fixture::Identity job{};
    fixture::SignalHome home{};
    uint16_t location = 249;
    uint32_t location_boot = 11;
    uint64_t proxy_remote_pid = 0, pending = 0;
    auto credentials() const { return creds; }
    auto job_identity() const { return job; }
    auto signal_home_identity(bool = true) const { return home; }
};
/* actual process_pid */
}
namespace fixture {
using Task = ker::mod::sched::task::Task;
Task* current = nullptr;
std::vector<Task*> tasks;
unsigned local_lookups = 0, proxy_lookups = 0, group_calls = 0;
uint64_t routed_pgid = 0;
int rpc_error = 0;
}
namespace ker::mod::sched {
auto get_current_task() -> task::Task* { return fixture::current; }
auto find_task_by_pid_safe(uint64_t pid) -> task::Task* {
    ++fixture::local_lookups;
    for (auto* target : fixture::tasks)
        if (target->location == 249 && target->pid == pid) return target;
    return nullptr;
}
namespace task {} // process_pid is compiled above, not reimplemented.
}
namespace ker::syscall::process {
using LocalSignalCallback = int (*)(std::span<ker::mod::sched::task::Task* const>, void*);
using LocalSignalCleanup = void (*)(void*) noexcept;
auto with_local_signal_targets(LocalSignalCallback callback, void* context, LocalSignalCleanup cleanup) -> int {
    std::vector<fixture::Task*> local;
    for (auto* target : fixture::tasks) if (target->location == 249) local.push_back(target);
    int result = callback(local, context);
    if (cleanup != nullptr) cleanup(context);
    return result;
}
auto signal_as(fixture::Task& target, const fixture::Snapshot& caller,
               const fixture::Identity& caller_job, int sig) -> int {
    if (!ker::mod::security::may_signal(caller, target.creds,
            ker::mod::sched::session::same_session(caller_job, target.job), sig == 18)) return -EPERM;
    if (sig != 0) target.pending |= uint64_t{1} << (sig - 1);
    return 0;
}
}
namespace ker::net::wki {
enum class PeerState { CONNECTED, FENCED };
struct Lock { void lock() {} void unlock() {} };
struct Peer { Lock lock; uint16_t node_id = 0; uint32_t remote_boot_epoch = 0; PeerState state = PeerState::FENCED; };
/* actual peer count */
struct State { uint16_t my_node_id = 249; uint32_t local_boot_epoch = 11; std::array<Peer,WKI_MAX_PEERS> peers{}; } g_wki;
/* actual route timeout */
auto wki_resource_generation_snapshot(uint16_t, ResourceType, uint32_t) -> uint32_t { return 1; }
auto wki_now_us() -> uint64_t { return 100; }
auto wki_future_deadline_us(uint64_t now, uint64_t delay) -> uint64_t { return now + delay; }
void sleep_until_us(uint64_t, uint64_t) { fixture::require(false, "unexpected modeled route retry"); }
auto wki_proxy_task_find_by_remote_pid_safe(uint64_t pid) -> fixture::Task* {
    ++fixture::proxy_lookups;
    for (auto* target : fixture::tasks) if (target->proxy_remote_pid == pid) return target;
    return nullptr;
}
/* actual origin conversions */

// The endpoint returns deterministic current receivers/anchors. Selector and
// origin choice remain in the actual group_signal_with_local_targets body.
template <typename Response>
auto group_route_rpc(uint16_t node, uint32_t boot, GroupRouteRequest request,
                     const fixture::Snapshot& caller, const fixture::Identity& caller_job,
                     Response& response) -> int {
    using namespace ker::mod::sched;
    fixture::require(request.credentials == ker::mod::security::credential_record::encode(caller),
                     "routing must preserve the caller credential record");
    fixture::require(request.caller_pgid == caller_job.pgid && request.session_id == caller_job.session_id,
                     "routing must preserve the caller job identity");
    if (fixture::rpc_error != 0) return fixture::rpc_error;
    response = {};
    response.boot = boot;
    if (request.operation == static_cast<uint8_t>(GroupRouteOperation::ENUMERATE)) {
        for (auto* target : fixture::tasks) {
            bool present;
            if constexpr (std::is_same_v<Response, GroupReceiverResponse>) present = target->location == node;
            else present = concrete_signal_origin(target->home.origin).node == node;
            if (!present || target->home.serial <= request.cursor) continue;
            auto const home = TaskSignalHome{concrete_signal_origin(target->home.origin), target->home.serial};
            response.ceiling = std::max(static_cast<uint64_t>(response.ceiling), target->home.serial);
            if constexpr (std::is_same_v<Response, GroupReceiverResponse>)
                response.receivers[response.count++] = {target->home.serial, target->pid, home};
            else response.anchors[response.count++] = {target->home.serial, target->pid};
        }
        return 0;
    }
    if constexpr (!std::is_same_v<Response, GroupReceiverResponse>) {
        fixture::Task* target = nullptr;
        for (auto* candidate : fixture::tasks)
            if (candidate->location == node && candidate->location_boot == boot && candidate->pid == request.target_pid &&
                candidate->home.serial == request.home.serial) target = candidate;
        if (target == nullptr) return -ESRCH;
        if (request.operation == static_cast<uint8_t>(GroupRouteOperation::QUERY)) {
            response.target_pgid = target->job.pgid;
            response.group_origin = concrete_signal_origin(target->job.group_origin);
            return 0;
        }
        auto receiver_identity = [node, boot](fixture::Identity identity) {
            identity.origin = resolve_session_origin(concrete_signal_origin(identity.origin), node, boot);
            identity.group_origin = resolve_session_origin(concrete_signal_origin(identity.group_origin), node, boot);
            return identity;
        };
        auto const group = fixture::Identity{.pgid = request.pgid,
            .group_origin = resolve_session_origin(request.group_origin, node, boot)};
        auto const target_job = receiver_identity(target->job);
        if (!session::same_group(group, target_job)) return -ESRCH;
        if (!ker::mod::security::may_signal(caller, target->creds,
                session::same_session(receiver_identity(caller_job), target_job), request.signum == 18)) return -EPERM;
        if (request.signum != 0) target->pending |= uint64_t{1} << (request.signum - 1);
        return 0;
    }
    return -EINVAL;
}
/* actual group route bodies */
}
namespace ker::syscall::process {
/* actual kill */
}
'''


CASES = r'''
namespace fixture {
constexpr Origin FOREIGN{250, 22};
constexpr Origin OTHER{251, 33};
auto subject(uint64_t pid, uint64_t pgid, Origin origin = {}, uint16_t location = 249) -> Task {
    Task target;
    target.pid = pid;
    target.job = {.session_id = pgid, .pgid = pgid, .origin = origin, .group_origin = origin};
    target.home = {.origin = origin, .serial = pid + 1000};
    target.location = location;
    target.location_boot = location == 249 ? 11 : 22;
    return target;
}
auto user(uint32_t uid) -> Snapshot {
    Snapshot out;
    require(Snapshot::restore_fields({.user = {uid,uid,uid}, .group = {uid,uid,uid}}, out) == 0,
            "real unprivileged credential preparation");
    return out;
}
void reset(Task& caller, std::initializer_list<Task*> targets) {
    current = &caller; tasks = targets; rpc_error = 0;
    local_lookups = proxy_lookups = group_calls = 0; routed_pgid = 0;
    ker::net::wki::g_wki.peers = {};
    ker::net::wki::g_wki.peers[0] = {{},250,22,ker::net::wki::PeerState::CONNECTED};
    for (auto* target : tasks) target->pending = 0;
}
auto kill(int64_t pid, int sig) -> int64_t {
    return static_cast<int64_t>(ker::syscall::process::wos_proc_kill(pid, sig));
}
void controls() {
    Task init = subject(1,1); init.wki_target_flags = Task::WKI_TARGET_FLAG_LOCAL;
    Task ordinary = subject(40,40); ordinary.wki_target_flags = Task::WKI_TARGET_FLAG_LOCAL;
    Task foreign = subject(88,88,FOREIGN,250);
    Task local = subject(88,88);
    Task migrated = subject(188,88,{},250); migrated.home = {{},8088};
    Task old_epoch = subject(189,88,{249,10},250);

    reset(init,{&foreign,&migrated,&old_epoch});
    require(kill(-88,15) == 0 && migrated.pending == (uint64_t{1}<<14) && foreign.pending == 0 && old_epoch.pending == 0,
            "migrated same-origin member survives absent leader; foreign node and old boot remain untouched");

    reset(ordinary,{&foreign});
    require(kill(-88,15) == 0 && foreign.pending != 0, "ordinary LOCAL caller retains foreign alias fallback");
    reset(init,{&foreign}); init.wki_target_flags = 0;
    require(kill(-88,15) == 0 && foreign.pending != 0, "init without LOCAL retains foreign alias fallback");
    init.wki_target_flags = Task::WKI_TARGET_FLAG_LOCAL;
    init.job.group_origin = OTHER;
    reset(init,{&foreign});
    require(kill(-88,15) == 0 && foreign.pending != 0, "foreign-origin init retains foreign alias fallback");
    init.job.group_origin = {};

    Task not_init = subject(1,1); not_init.owner_pid = 41; not_init.wki_target_flags = Task::WKI_TARGET_FLAG_LOCAL;
    reset(not_init,{&foreign});
    require(kill(-88,15) == 0 && foreign.pending != 0, "process identity, not raw task PID, selects init");

    Task own = subject(41,40); Task other_own = subject(42,40,FOREIGN,250);
    for (auto pid : {int64_t{0},int64_t{-1}}) {
        reset(ordinary,{&own,&other_own});
        require(kill(pid,15) == 0 && own.pending != 0 && other_own.pending == 0,
                "zero and minus one keep qualified caller-group behavior");
    }

    reset(init,{&local,&foreign}); init.creds = user(100); local.creds = user(200);
    require(kill(-88,15) == -EPERM && local.pending == 0 && foreign.pending == 0,
            "namespace qualification cannot bypass real signal permission policy");
    local.creds = user(100);
    require(kill(-88,15) == 0 && local.pending != 0 && foreign.pending == 0,
            "same-UID signal permission remains accepted");
    init.creds = Snapshot::bootstrap_root(); local.creds = Snapshot::bootstrap_root();
    init.creds = user(100); migrated.creds = user(200);
    reset(init,{&foreign,&migrated});
    require(kill(-88,15) == -EPERM && migrated.pending == 0 && foreign.pending == 0,
            "modeled migrated delivery retains real different-UID permission denial");
    migrated.creds = user(100);
    require(kill(-88,15) == 0 && migrated.pending != 0 && foreign.pending == 0,
            "modeled migrated delivery retains same-UID permission acceptance");
    init.creds = Snapshot::bootstrap_root(); migrated.creds = Snapshot::bootstrap_root();

    Task proxy = subject(999,999); proxy.proxy_remote_pid = 88;
    reset(init,{&local,&proxy});
    require(kill(88,15) == 0 && local.pending != 0 && proxy.pending == 0 && proxy_lookups == 0,
            "positive PID keeps native lookup precedence");
    reset(init,{&proxy});
    require(kill(88,15) == 0 && proxy.pending != 0 && proxy_lookups == 1,
            "positive PID keeps existing proxy lookup fallback");
    reset(init,{});
    require(kill(9999,15) == -ESRCH, "missing positive PID keeps ESRCH");

    reset(init,{&local});
    require(kill(-88,64) == 0 && local.pending == (uint64_t{1}<<63), "highest valid signal remains accepted");
    reset(init,{}); current = nullptr;
    require(kill(-88,-1) == -EINVAL && kill(-88,65) == -EINVAL && kill(88,65) == -EINVAL,
            "invalid signal validation precedes current-task and target lookup");
    require(kill(-88,15) == -ESRCH && kill(88,15) == -ESRCH, "null task keeps ESRCH");
    require(local_lookups == 0 && proxy_lookups == 0, "rejected calls cannot lookup targets");

    Task huge = subject(90,uint64_t{1}<<63);
    reset(init,{&huge});
    require(kill(std::numeric_limits<int64_t>::min(),15) == 0 && huge.pending != 0,
            "INT64_MIN retains overflow-safe negative-group conversion");
    reset(init,{&local}); rpc_error = -EIO;
    require(kill(-88,15) == -EIO && local.pending == 0, "group endpoint errno propagates unchanged");
    std::puts("PASS unaffected controls");
    std::fflush(stdout);
}
void collision() {
    Task init = subject(1,1); init.wki_target_flags = Task::WKI_TARGET_FLAG_LOCAL;
    Task foreign = subject(88,88,FOREIGN,250);
    reset(init,{&foreign});
    int64_t const result = kill(-88,15);
    std::printf("collision result=%lld foreign_pending=%llu\n", static_cast<long long>(result),
                static_cast<unsigned long long>(foreign.pending));
    std::fflush(stdout);
    require(result == -ESRCH && foreign.pending == 0,
            "init local group must not select a foreign same-number group after local leader removal");
    require(kill(-88,0) == -ESRCH && foreign.pending == 0, "init group disappearance probe stays origin-qualified");
    std::puts("PASS absent-local foreign collision");
}
}
int main(int argc, char** argv) {
    fixture::controls();
    if (argc == 1 || std::string_view(argv[1]) != "controls") fixture::collision();
}
'''


def generate(source: Path) -> str:
    remote = REMOTE.read_text()
    task = (KERN / "platform/sched/task.hpp").read_text()
    wki = (KERN / "net/wki/wki.hpp").read_text()
    replacements = {
        "/* actual task constants */": "\n".join(
            re.search(rf"static constexpr [^;]+\b{name}\s*=[^;]+;", task).group(0)
            for name in ("MAX_SIGNALS", "WKI_TARGET_FLAG_LOCAL")
        ),
        "/* actual process_pid */": definition(task, "process_pid"),
        "/* actual peer count */": re.search(r"constexpr size_t WKI_MAX_PEERS =[^;]+;", wki).group(0),
        "/* actual route timeout */": re.search(r"constexpr uint64_t WKI_TASK_SUBMIT_VFS_TIMEOUT_US =[^;]+;", remote).group(0),
        "/* actual origin conversions */": "\n".join(definition(remote, name) for name in ("concrete_signal_origin", "resolve_session_origin")),
        "/* actual group route bodies */": "\n".join([
            definition(remote, "group_signal_error"),
            definition(remote, "GroupRouteTarget", record=True),
            definition(remote, "GroupRouteTargets", record=True),
            definition(remote, "group_signal_with_local_targets"),
            definition(remote, "wki_group_signal_as"),
        ]),
        "/* actual kill */": definition(source.read_text(), "wos_proc_kill"),
    }
    result = PRELUDE
    for marker, code in replacements.items():
        result = result.replace(marker, code)
    return result + CASES


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=PROCESS)
    parser.add_argument("--controls-only", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    compiler = os.environ.get("WOS_HOST_CXX") or shutil.which("clang++", path=os.defpath) or shutil.which("g++", path=os.defpath)
    if compiler is None:
        raise RuntimeError("native C++ compiler required")
    temporary = None
    if args.output_dir is None:
        temporary = tempfile.TemporaryDirectory(prefix="wos-init-group-origin-")
        directory = Path(temporary.name)
    else:
        directory = args.output_dir
        directory.mkdir(parents=True, exist_ok=True)
    cpp, binary = directory / "actual_init_group.cpp", directory / "actual_init_group"
    if cpp.exists():
        raise RuntimeError("refusing to overwrite earlier proof")
    cpp.write_text(generate(args.source))
    command = [compiler, "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Werror",
               "-fsanitize=address,undefined", "-fno-sanitize-recover=all", "-fno-omit-frame-pointer",
               "-fno-pie", "-no-pie", "-I", str(KERN), str(cpp), "-o", str(binary)]
    (directory / "compiler-command.json").write_text(json.dumps(command, indent=2) + "\n")
    paths = [args.source, REMOTE, KERN / "platform/sched/task.hpp", KERN / "platform/sched/session.hpp",
             KERN / "platform/security/authorization.hpp", KERN / "platform/security/credentials.hpp",
             KERN / "platform/security/credential_record.hpp", KERN / "net/wki/wire.hpp", KERN / "net/wki/wki.hpp",
             KERN / "platform/security/terminal.hpp", KERN / "net/address.hpp", KERN / "abi/callnums/process.h",
             Path(__file__), cpp]
    (directory / "source-pins.json").write_text(json.dumps(
        {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}, indent=2) + "\n")
    compiled = subprocess.run(command, capture_output=True, text=True, timeout=60)
    (directory / "compile.log").write_text(compiled.stdout + compiled.stderr)
    if compiled.returncode:
        raise RuntimeError(f"compile failed: {directory / 'compile.log'}\n{compiled.stderr}")
    run = [str(binary)] + (["controls"] if args.controls_only else [])
    env = os.environ | {"ASAN_OPTIONS": "detect_leaks=0:halt_on_error=1", "UBSAN_OPTIONS": "halt_on_error=1:print_stacktrace=1"}
    result = subprocess.run(run, capture_output=True, text=True, timeout=15, env=env)
    (directory / "run.log").write_text(result.stdout + result.stderr)
    (directory / "result.json").write_text(json.dumps({"compile_exit": compiled.returncode,
        "run_exit": result.returncode, "command": run, "controls_only": args.controls_only,
        "sanitizers": ["address", "undefined"], "leak_sanitizer": False}, indent=2) + "\n")
    print(result.stdout, end="")
    print(result.stderr, end="", file=sys.stderr)
    if temporary is not None:
        temporary.cleanup()
    if result.returncode:
        raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
