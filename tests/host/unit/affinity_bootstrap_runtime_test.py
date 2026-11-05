#!/usr/bin/env python3
"""Run the actual public AFF setup prefix against bounded adversarial Host endpoints.

The supervisor, first worker pin/publication, confirmation loop, raw report types
and scalar inheritance policies are extracted. Scheduling, placement admission,
clock, pthread and IO are explicit sequential Host endpoints. This tests fixture
control flow, not native scheduler progress, later controllers/pipe work or WOS
qualification. The diagnostic capture fixture keeps its separate instrumentation.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]

INCLUDES = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <map>
#include <memory>
#include <pthread.h>
#include <time.h>
#include <vector>
struct HostAbort { uint64_t code{},id{}; int64_t actual{}; };
struct HostYield {};
[[noreturn]] void invariant(const char* text) {
    std::fprintf(stderr,"affinity bootstrap invariant: %s\n",text);std::abort();
}
#define CHECK(x) do { if (!(x)) invariant(#x); } while (false)
namespace ker::abi::multiproc {
enum class threadControlOps : uint64_t {
    SET_AFFINITY=0x104,GET_AFFINITY=0x105,CREATE_DOMAIN=0x106,SET_DOMAIN=0x107,QUERY_DOMAIN=0x108
};
}
namespace kernel {
namespace task {
enum class TaskType { PROCESS,DAEMON,IDLE };
enum class TaskState { ACTIVE,EXITING,DEAD };
// Exclusively owned Host storage; no claim of actual Task ABI/queue lifetime.
struct Task {
    uint64_t cpu{},domain_mask{UINT64_MAX},pid{1}; uint32_t domain_id{};
    bool cpu_pinned{},domain_hard{},has_run{},has_exited{};
    TaskType type{TaskType::PROCESS};std::atomic<TaskState> state{TaskState::ACTIVE};
    std::atomic<bool> exit_in_progress{},gc_queued{},scheduler_published{};
    uint32_t active_registry_index{UINT32_MAX};int32_t heap_index{-1};
    enum class sched_queue { NONE,RUNNABLE,WAITING,DEAD_GC };
    enum sched_queue sched_queue{sched_queue::NONE};
    Task* sched_previous{};Task* sched_next{};std::atomic<void*> sched_list_owner{};
// INJECT_ACCESSORS
};
}
// INJECT_SNAPSHOT
// INJECT_INHERITANCE
}
'''

MODEL = r'''
enum class Position { CURRENT,QUEUED,HANDOFF,WAITING };
struct Actor {
    kernel::task::Task placement{};Position position{Position::QUEUED};unsigned self_successes{};
    explicit Actor(uint64_t tid,uint64_t cpu) { placement.pid=tid;placement.store_cpu(cpu); }
};
struct HostModel {
    std::map<uint64_t,std::unique_ptr<Actor>> actors;
    std::map<uint64_t,void*> arguments;
    std::vector<uint64_t> inherited_cpus,parent_pin_masks;
    uint64_t active{101},now{1000},seed{},origin{UINT64_MAX},pending_tid{},first_start{},first_deadline{},failure_now{};
    unsigned creates{},parent_policy_successes{},remote_successes{},remote_cross{},remote_same{},remote_busy{},restore_at{},delayed{},finite_busy{},confirm_busy{};
    int fault{-1};bool race{},race_seen{},in_confirmation{},finished{},old_counterexample{};
    State* state{};HostReport final_report{};std::array<WorkerReport,WORKERS> final_workers{};
    void*(*confirm)(void*){};
    HostModel(uint64_t cpu,uint64_t rr):seed(rr) {
        actors.emplace(101,std::make_unique<Actor>(101,cpu));actor(101).position=Position::CURRENT;
    }
    Actor& actor(uint64_t tid) { return *actors.at(tid); }
    Actor& current() { return actor(active); }
    uint64_t mask(uint64_t tid) {
        auto& value=actor(tid).placement;
        return value.load_cpu_pinned()?uint64_t{1}<<value.load_cpu():value.load_domain_mask()&ONLINE;
    }
    unsigned confirmations()const {
        unsigned count=0;if(state)for(auto const& row:state->published)count+=row.confirmed.load();return count;
    }
    void make_current(uint64_t tid) {
        auto cpu=actor(tid).placement.load_cpu();
        for(auto& [id,value]:actors)if(id!=tid && value->placement.load_cpu()==cpu && value->position==Position::CURRENT)value->position=Position::QUEUED;
        actor(tid).position=Position::CURRENT;
    }
    void dispatch() {
        if(!pending_tid || in_confirmation)return;
        if(delayed) { --delayed;return; }
        uint64_t const child=pending_tid;uint64_t const caller=active;
        active=child;make_current(child);in_confirmation=true;
        try { confirm(arguments.at(child)); } catch(HostYield const&) {}
        catch(...) { active=caller;in_confirmation=false;make_current(caller);throw; }
        active=caller;in_confirmation=false;make_current(caller);
    }
};
HostModel* model;
uint64_t host_cpu() { if(model->fault==1 && model->active==101)return 8;return model->current().placement.load_cpu(); }
uint64_t host_yield() {
    model->now+=100000;
    if(model->in_confirmation)throw HostYield{};
    model->dispatch();return 0;
}
int host_clock_gettime(clockid_t,timespec* output) {
    model->now+=100;output->tv_sec=static_cast<time_t>(model->now/1000000);
    output->tv_nsec=static_cast<long>(model->now%1000000*1000);return 0;
}
int host_nanosleep(const timespec* duration,timespec*) {
    model->now+=static_cast<uint64_t>(duration->tv_sec)*1000000+static_cast<uint64_t>(duration->tv_nsec)/1000;
    model->dispatch();return 0;
}
int64_t raw(Op op,uint64_t tid,uint64_t mask=0,uint64_t=0) {
    auto found=model->actors.find(tid);if(found==model->actors.end())return -ESRCH;
    auto& target=*found->second;auto& place=target.placement;
    if(op==Op::GET_AFFINITY) {
        if(model->fault==9 && tid==101)return -EIO;
        return static_cast<int64_t>(model->mask(tid));
    }
    CHECK(op==Op::SET_AFFINITY);
    if(!mask || (mask&~ONLINE))return -EINVAL;
    if(tid==101 && model->active==101) {
        if(mask!=ONLINE) {
            model->parent_pin_masks.push_back(mask);
            if(model->fault==2)return -EINVAL;
            if(model->race && !model->race_seen) {
                model->race_seen=true;place.store_cpu((place.load_cpu()+1)%8);return -EBUSY;
            }
            if(model->fault==3)return 0; // Accepted-no-effect transport fault must be checked by actual code.
        } else {
            if(model->fault==6)return 0;
            model->restore_at=model->confirmations();
        }
    }
    if(tid!=101 && model->active==101) {
        if(model->fault==5)return -EINVAL;
        if(model->fault==7 || model->finite_busy) {
            if(model->finite_busy)--model->finite_busy;
            ++model->remote_busy;return -EBUSY;
        }
        // Legal adversarial dispatch: while parent is elsewhere, the remote
        // target may remain sole CURRENT and keep selecting itself on yield.
        if(place.load_cpu()!=model->current().placement.load_cpu())model->make_current(tid);
    }
    uint64_t destination=place.load_cpu();
    if(std::has_single_bit(mask))destination=std::countr_zero(mask);
    bool const reserved=target.position==Position::CURRENT || target.position==Position::HANDOFF;
    if((reserved || target.position==Position::WAITING) && !(mask&(uint64_t{1}<<place.load_cpu()))) {
        if(tid!=101 && model->active==101)++model->remote_busy;
        return -EBUSY;
    }
    if(model->in_confirmation && model->confirm_busy) { --model->confirm_busy;return -EBUSY; }
    if(model->fault==10 && model->active!=101)return -EINVAL;
    if(tid!=101 && model->active==101) {
        ++model->remote_successes;
        if(destination!=place.load_cpu())++model->remote_cross;else ++model->remote_same;
        model->pending_tid=tid;
    }
    place.store_cpu(destination);place.store_domain_mask(mask==ONLINE?UINT64_MAX:mask);
    place.store_cpu_pinned(std::has_single_bit(mask));place.store_domain_hard(false);
    if(tid==model->active)++target.self_successes;
    if(tid==101 && model->active==101)++model->parent_policy_successes;
    return 0;
}
int host_pipe(int* fds) { static int next=10;fds[0]=next++;fds[1]=next++;return 0; }
int host_fcntl(int,int operation,int flags) { CHECK(operation==F_SETFL && flags==O_NONBLOCK);return 0; }
namespace ker::process {
constexpr uint64_t WKI_TARGET_FLAG_LOCAL=1;
uint64_t getpid() { return 41; }uint64_t getuid() { return 100; }
uint64_t geteuid() { return 100; }uint64_t getgid() { return 100; }
int64_t setwkitarget(const char*,uint64_t,uint64_t) { return 0; }
}
namespace ker::multiproc {
uint64_t getcurrent_cpu() { return host_cpu(); }uint64_t currentThreadId() { return model->active; }
uint64_t nativeThreadCount() { return 8; }uint64_t yield() { return host_yield(); }
}
int host_pthread_create(pthread_t*,const pthread_attr_t*,void*(*)(void*),void*);
#define clock_gettime host_clock_gettime
#define nanosleep host_nanosleep
#define pipe host_pipe
#define fcntl host_fcntl
#define pthread_create host_pthread_create
'''

ENDPOINTS = r'''
int host_pthread_create(pthread_t* output,const pthread_attr_t*,void*(*entry)(void*),void* opaque) {
    CHECK(model->active==101 && model->creates<WORKERS);
    auto& parent=model->current().placement;
    kernel::TaskPlacementSnapshot snapshot{.owner_cpu=parent.load_cpu(),.domain_mask=parent.load_domain_mask(),
        .domain_id=parent.load_domain_id(),.cpu_pinned=parent.load_cpu_pinned(),.domain_hard=parent.load_domain_hard()};
    uint64_t const tid=2000+model->creates;
    auto child=std::make_unique<Actor>(tid,0);
    CHECK(kernel::initialize_inherited_task_placement_for_cores(&child->placement,snapshot,8)==0);
    uint64_t allowed{};CHECK(kernel::inherited_task_placement_matches(&child->placement,snapshot,8,allowed));
    // Actual selector receives a changing global-RR seed in positives; repeated
    // seed is also legal under unrelated creations, and exercises the old cycle.
    uint64_t const seed=model->old_counterexample?model->seed:model->seed+model->creates*3;
    uint64_t const cpu=kernel::inherited_round_robin_cpu(snapshot,8,seed);CHECK(cpu<8);
    child->placement.store_cpu(cpu);
    if(model->fault==4 && model->creates==5)child->placement.store_cpu((cpu+1)%8);
    model->inherited_cpus.push_back(child->placement.load_cpu());
    if(model->creates==0)model->origin=parent.load_cpu();
    model->state=static_cast<Argument*>(opaque)->state;model->arguments[tid]=opaque;
    model->actors.emplace(tid,std::move(child));++model->creates;
    *output=static_cast<pthread_t>(tid);
    uint64_t const caller=model->active;model->active=tid;model->make_current(tid);
    try { CHECK(entry(opaque)==nullptr); }
    catch(...) { model->active=caller;model->make_current(caller);throw; }
    model->actor(tid).position=Position::QUEUED;model->active=caller;model->make_current(caller);
    return 0;
}
int host_finish_setup(HostReport const& report,State const& state) {
    model->finished=true;model->final_report=report;model->final_workers=state.workers;
    CHECK(state.setup_done.load());return 0;
}
'''

TAIL = r'''
#undef pthread_create
#undef fcntl
#undef pipe
#undef nanosleep
#undef clock_gettime
void positive(uint64_t cpu,uint64_t seed,bool race=false) {
    HostModel current(cpu,seed);model=&current;model->race=race;
    model->finite_busy=2;model->confirm_busy=2;model->delayed=2;model->confirm=host_confirmation;
    try { CHECK(host_setup_supervisor()==0); }catch(HostAbort const&) { invariant("actual staged setup completes within original60s"); }
    CHECK(model->finished && model->creates==16 && model->remote_successes==16);
    CHECK(model->remote_cross==14 && model->remote_same==2);
    CHECK(model->parent_policy_successes==2 && model->restore_at==16 && model->mask(101)==ONLINE);
    CHECK(model->final_report.setup_calls-model->final_report.setup_busy==16);
    CHECK(model->final_report.workers==16 && model->final_report.unique_tids==16);
    CHECK(model->final_report.initial_cpu_mask==ONLINE && model->final_report.checks.failures==0);
    CHECK(model->inherited_cpus.size()==16);
    for(unsigned index=0;index<16;++index) {
        CHECK(model->inherited_cpus[index]==model->origin);
        auto const& worker=model->final_workers[index];
        CHECK(worker.pin_calls==worker.pin_busy+2 && worker.checks.failures==0);
        CHECK(model->actor(worker.tid).self_successes==2);
        CHECK(worker.initial_cpu==(model->origin+index)%8);
    }
    if(race)CHECK(model->race_seen && model->parent_pin_masks.size()>=2 && model->parent_pin_masks.front()!=model->parent_pin_masks.back());
    // Only scalar inheritance at the later boundary is tested; no controller
    // thread or subsequent3072-call/pipe/domain result is manufactured here.
    auto& parent=model->actor(101).placement;
    kernel::TaskPlacementSnapshot restored{.owner_cpu=parent.load_cpu(),.domain_mask=parent.load_domain_mask(),
        .domain_id=parent.load_domain_id(),.cpu_pinned=parent.load_cpu_pinned(),.domain_hard=parent.load_domain_hard()};
    for(uint64_t next=0;next<8;++next)CHECK(kernel::inherited_round_robin_cpu(restored,8,next)==next);
}
void negative(int fault,uint64_t expected) {
    HostModel current(1,7);model=&current;model->fault=fault;model->confirm=host_confirmation;
    if(fault==0)model->current().placement.store_domain_mask(3);
    bool failed=false;try { static_cast<void>(host_setup_supervisor()); }
    catch(HostAbort const& error) { failed=error.code==expected; }
    CHECK(failed);
    if(fault==7)CHECK(!model->finished && model->remote_busy>1 && model->failure_now>=model->first_deadline);
}
void inheritance_controls() {
    kernel::TaskPlacementSnapshot pin{.owner_cpu=3,.domain_mask=8,.cpu_pinned=true};
    kernel::task::Task child{};child.pid=77;
    CHECK(kernel::initialize_inherited_task_placement_for_cores(&child,pin,8)==0);
    uint64_t allowed{};CHECK(kernel::inherited_task_placement_matches(&child,pin,8,allowed));
    child.store_domain_mask(ONLINE);CHECK(!kernel::inherited_task_placement_matches(&child,pin,8,allowed));
    for(uint64_t seed:{uint64_t{0},uint64_t{7},UINT64_MAX})CHECK(kernel::inherited_round_robin_cpu(pin,8,seed)==3);
    auto bad=pin;bad.owner_cpu=8;CHECK(kernel::inherited_round_robin_cpu(bad,8,0)==UINT64_MAX);
    child.has_run=true;CHECK(kernel::initialize_inherited_task_placement_for_cores(&child,pin,8)==-EBUSY);
}
void old_counterexample() {
    HostModel current(0,1);model=&current;model->old_counterexample=true;model->confirm=host_confirmation;
    bool expired=false;try { static_cast<void>(host_setup_supervisor()); }
    catch(HostAbort const& error) { expired=error.code==DEADLINE; }
    CHECK(expired && !model->finished && model->remote_busy>1 && model->remote_cross==0);
    CHECK(model->first_deadline==model->first_start+RUN_US && model->failure_now>=model->first_deadline);
}
int main() {
    static_assert(WORKERS==16 && SETTERS==2 && CONTROLLERS==3 && CALLS==1024 && EVENTS==256 && HARD_OBSERVATIONS==64);
    static_assert(RUN_US==60000000 && PARK_US==2000000);static_cast<void>(RESULTS);
#ifdef HOST_OLD_COUNTEREXAMPLE
    old_counterexample();std::puts("PASS old affinity sole-current deadline counterexample");
#else
    inheritance_controls();
    for(uint64_t cpu=0;cpu<8;++cpu)for(uint64_t seed:{uint64_t{0},uint64_t{7},UINT64_MAX}) {
        positive(cpu,seed);positive(cpu,seed,true);
    }
    negative(0,AFFINITY);negative(1,CPU);negative(2,PIN);negative(3,PIN);negative(4,CPU);
    negative(5,PIN);negative(6,AFFINITY);negative(7,DEADLINE);negative(9,AFFINITY);negative(10,PIN);
    std::puts("PASS actual affinity setup prefix and modeled endpoints");
#endif
    return 0;
}
'''


def generate(root, source_root, capture):
    """Return actual public-source fragments, with explicit Host cut boundaries."""
    inputs, fragments = {}, []
    def text(relative):
        path = source_root / relative
        if not path.is_file():
            path = root / relative
        value = path.read_text()
        inputs[str(path)] = hashlib.sha256(value.encode()).hexdigest()
        return value
    def body(value, anchor, declaration=False):
        result, row = capture(value, anchor, declaration=declaration)
        fragments.append(row)
        return result
    scheduler = text("modules/kern/src/platform/sched/scheduler.cpp")
    header = text("modules/kern/src/platform/sched/scheduler.hpp")
    task = text("modules/kern/src/platform/sched/task.hpp")
    source = text("tests/fixtures/task_placement_runtime.cpp")
    boundary = "\nnamespace weighted_fairness {\n"
    if source.count(boundary) != 1:
        raise ValueError("expected one AFF/weighted namespace boundary")
    source = source.split(boundary, 1)[0]
    prefix = source.split("namespace {\n", 1)[1].split("auto now_us(", 1)[0]
    types = "\n".join(body(source, anchor, True) for anchor in ("struct Published {", "struct State {", "struct Argument {"))
    accessors = "\n".join(body(task, anchor) for field in ("cpu", "domain_mask", "domain_id", "cpu_pinned", "domain_hard")
                          for anchor in ("auto load_"+field+"()", "void store_"+field+"("))
    inherited = "\n".join(body(scheduler, anchor) for anchor in (
        "inline auto cpu_mask_contains(", "auto task_placement_online_mask(", "auto inherited_task_placement_mask(",
        "auto inherited_task_is_fresh(", "auto initialize_inherited_task_placement_for_cores(",
        "auto inherited_task_placement_matches(", "auto inherited_round_robin_cpu("))
    kernel = INCLUDES.replace("// INJECT_ACCESSORS", accessors)
    kernel = kernel.replace("// INJECT_SNAPSHOT", body(header, "struct TaskPlacementSnapshot {", True))
    kernel = kernel.replace("// INJECT_INHERITANCE", inherited)
    endpoints = """
[[noreturn]] void abort_live(Failure code,uint64_t id=0,int64_t actual=0) {
    throw HostAbort{static_cast<uint64_t>(code),id,actual};
}
void diagnostic_failure_context(const State& state,Failure,uint64_t now) {
    model->first_start=state.start;model->first_deadline=state.deadline;model->failure_now=now;
}
void diagnostic_stage(const char* value) { CHECK(std::strcmp(value,"setup")==0); }
int host_finish_setup(HostReport const&,State const&);
"""
    functions = "\n".join(body(source, anchor) for anchor in (
        "auto now_us(", "void pause_us(", "void alive(", "void observe(", "void expect_live(", "auto get_affinity("))
    worker = body(source, "auto worker(")
    confirmation = body(worker, "while (!state.setup_done.load(")
    aliases = worker[worker.index("    auto const& argument"):worker.index("    report.nonce")]
    setup = worker[:worker.index("    // The parent uses real checked cross-CPU setters")]
    setup += "    return nullptr;\n}\n"
    confirmation = "auto host_confirmation(void* opaque)->void* {\n"+aliases+confirmation+"\nreturn nullptr;\n}\n"
    supervisor = body(source, "auto supervisor(")
    end = supervisor.index('    diagnostic_stage("setup");')+len('    diagnostic_stage("setup");')
    supervisor = supervisor[:end]+"\n    return host_finish_setup(host,state);\n}\n"
    supervisor = supervisor.replace("auto supervisor()", "auto host_setup_supervisor()", 1)
    code = kernel+prefix+"\n"+types+"\n"+MODEL+endpoints+functions+"\n"+setup+confirmation+ENDPOINTS+supervisor+TAIL
    return code, inputs, fragments


def remove_staging(supervisor):
    """Old setup counterexample: remove only the three newly added stage blocks."""
    start = supervisor.index("    // Stage bootstrap")
    end = supervisor.index("    std::array<Argument, WORKERS>", start)
    supervisor = supervisor[:start]+supervisor[end:]
    center = supervisor.index("SETUP_OWNER == bootstrap_cpu")
    start = supervisor.rfind("    expect_live(", 0, center)
    target = supervisor.index("uint64_t const CPU = (SETUP_OWNER + index)", center)
    end = supervisor.rfind("    for (unsigned index", 0, target)
    if start < 0 or end <= start:
        raise ValueError("new origin-check block extraction failed")
    supervisor = supervisor[:start]+supervisor[end:]
    start = supervisor.index("    // All16 placements")
    end = supervisor.index("    state.setup_done.store(", start)
    return supervisor[:start]+supervisor[end:]


def variants(actual, capture):
    result = {"actual": actual}
    supervisor = capture(actual, "auto host_setup_supervisor(")[0]
    result["old-counterexample"] = "#define HOST_OLD_COUNTEREXAMPLE 1\n"+actual.replace(supervisor, remove_staging(supervisor), 1)
    # Each mutant compiles actual control flow and must fail independent runtime
    # effects/order/negative-control oracles; no expected implementation string.
    stale, count = re.subn(
        r"for\s*\(;;\)\s*\{\s*alive\(state\);\s*(uint64_t const SAMPLED_CPU = ker::multiproc::getcurrent_cpu\(\);)",
        r"\1 for (;;) { alive(state);", supervisor, count=1)
    if count != 1:
        raise ValueError("sampled CPU mutation anchor changed")
    result["stale-sampled-cpu"] = actual.replace(supervisor, stale, 1)
    restore = "raw(Op::SET_AFFINITY, host.tid, ONLINE)"
    if supervisor.count(restore) != 1:
        raise ValueError("restoration call is not unique")
    result["missing-online-restore"] = actual.replace(supervisor, supervisor.replace(restore, "int64_t{0}", 1), 1)
    start = supervisor.index("    // All16 placements")
    end = supervisor.index("    state.setup_done.store(", start)
    restoration = supervisor[start:end]
    shortened = supervisor[:start]+supervisor[end:]
    target = shortened.index("uint64_t const CPU = (SETUP_OWNER + index)")
    insertion = shortened.rfind("    for (unsigned index", 0, target)
    early = shortened[:insertion]+restoration+shortened[insertion:]
    result["early-online-restore"] = actual.replace(supervisor, early, 1)
    confirmation = capture(actual, "auto host_confirmation(")[0]
    call = "raw(Op::SET_AFFINITY, report.tid, uint64_t{1} << CPU)"
    if confirmation.count(call) != 1:
        raise ValueError("worker confirmation SET is not unique")
    result["missing-second-worker-set"] = actual.replace(confirmation, confirmation.replace(call, "int64_t{0}", 1), 1)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--host-cxx", default=os.environ.get("HOST_CXX"))
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    root, source_root = args.repo_root.resolve(), args.source_root.resolve()
    helper = root / "tests/host/unit/file_mmap_registry_runtime_test.py"
    lexer = root / "tests/host/unit/weighted_fairness_bootstrap_runtime_test.py"
    def load(name, path):
        before = path.read_bytes()
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if path.read_bytes() != before:
            raise ValueError("maintained helper changed during import: "+str(path))
        return module
    owned = load("affinity_bootstrap_owned", helper)
    lexical = load("affinity_bootstrap_lexer", lexer)
    compiler = args.host_cxx or shutil.which("g++", path=os.defpath) or shutil.which("clang++", path=os.defpath)
    if not compiler:
        raise ValueError("host compiler unavailable")
    owned.FILE_BUDGET, owned.SCRATCH_BUDGET, owned.LOG_BUDGET = 32 << 20, 128 << 20, 4 << 20
    owned.subreaper()
    scratch = Path(tempfile.mkdtemp(prefix="affinity-bootstrap-"))
    started, groups, commands, errors = time.monotonic(), [], [], []
    deadline = started+140
    handlers = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    active, pending = False, None
    def interrupted(number, unused):
        nonlocal pending
        if active:
            pending = number
            return
        raise KeyboardInterrupt(str(number))
    for number in handlers:
        signal.signal(number, interrupted)
    result, last_log = {}, None
    try:
        def capture(source, anchor, *, declaration=False):
            return lexical.source_capture(source, anchor, owned.capture, declaration=declaration)
        actual, used, fragments = generate(root, source_root, capture)
        before = [owned.pin(Path(path)) for path in sorted(used)]
        if any(row["sha256"] != used[row["path"]] for row in before):
            raise ValueError("captured source changed before compiler-input hold")
        before += [owned.pin(path) for path in (Path(__file__).resolve(), helper, lexer, Path(compiler).resolve())]
        result["capture_control"] = lexical.capture_control(capture)
        cases = variants(actual, capture)
        dependencies = []
        def command(argv, label):
            nonlocal last_log, active
            if pending is not None:
                raise KeyboardInterrupt("deferred signal "+str(pending))
            last_log = scratch / (label+".log")
            active = True
            try:
                row = owned.command(list(map(str, argv)), last_log, scratch, deadline, groups)
            finally:
                active = False
                if pending is not None:
                    raise KeyboardInterrupt("deferred signal "+str(pending))
            commands.append(row)
            return row
        for name, code in cases.items():
            cpp, binary, dep = scratch/(name+".cpp"), scratch/name, scratch/(name+".d")
            cpp.write_text(code)
            compiled = command([compiler,"-std=c++23","-O1","-g","-Wall","-Wextra","-Werror","-pthread",
                "-fno-pie","-no-pie","-fsanitize=address,undefined","-fno-sanitize-recover=all",
                "-MD","-MF",dep,cpp,"-o",binary], name+"-compile")
            if compiled["exit"] != 0:
                raise ValueError("actual-source variant did not compile: "+name)
            pins = owned.dependency_pins(dep)
            dependencies.extend(pins)
            executed = command([binary], name+"-run")
            text = last_log.read_text(errors="replace")
            expected = ("PASS old affinity sole-current deadline counterexample" if name == "old-counterexample"
                        else "PASS actual affinity setup prefix and modeled endpoints")
            if name in ("actual", "old-counterexample"):
                if executed["exit"] != 0 or text.count(expected) != 1:
                    raise ValueError("actual-source runtime controls failed: "+name)
            elif executed["exit"] != -signal.SIGABRT or "affinity bootstrap invariant:" not in text:
                raise ValueError("compiled mutant did not fail runtime oracle: "+name)
            for row in pins:
                if owned.pin(Path(row["path"])) != row:
                    raise ValueError("compiler input changed: "+row["path"])
        after = [owned.pin(Path(row["path"])) for row in before]
        if before != after:
            raise ValueError("source/driver/compiler changed during checks")
        result.update(source_before=before,source_after=after,fragments=fragments,variants=list(cases),dependency_inputs=dependencies,
            limitations="Actual PUBLIC supervisor setup prefix/worker setup+confirmation and scalar inheritance policies; scheduling, admission, clock, pthread and IO are sequential Host endpoints. No capture-variant instrumentation equivalence, native progress, later controller/pipe/domain/raw oracle/qualification or old-failure-cause claim.")
    except BaseException as error:
        errors.append(type(error).__name__+": "+str(error))
        if last_log and args.evidence_dir is None and last_log.is_file():
            print(last_log.read_text(errors="replace")[-32768:], file=sys.stderr)
    finally:
        for number in handlers:
            signal.signal(number, signal.SIG_IGN)
        try:
            if groups:
                errors.append("owned command group unresolved; scratch preserved")
            else:
                if args.evidence_dir:
                    args.evidence_dir.mkdir(parents=True, exist_ok=True)
                    for log in scratch.glob("*.log"):
                        shutil.copy2(log, args.evidence_dir/log.name)
                shutil.rmtree(scratch)
        except BaseException as error:
            errors.append("cleanup: "+repr(error))
        for number, original in handlers.items():
            signal.signal(number, original)
    result.update(result="FAIL" if errors else "PASS_HOST_FIXTURE_CONTROLS",errors=errors,commands=commands,
                  seconds=time.monotonic()-started,scratch_removed=not scratch.exists(),runtime_qualified=False,native_role_required=True)
    if args.evidence_dir:
        args.evidence_dir.mkdir(parents=True, exist_ok=True)
        (args.evidence_dir/"result.json").write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps({key:result[key] for key in ("result","seconds","errors","scratch_removed")}))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
