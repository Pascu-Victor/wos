#!/usr/bin/env python3
"""Execute actual weighted setup slices against bounded adversarial Host transports.

Actual supervisor ordering, bootstrap, trial control, worker setup/confirmation,
report ABI and pure math are extracted. Only thread scheduling, clocks, syscalls,
filesystem persistence and measured compute/reporting are Host endpoints. This
is not native scheduling, CPU fairness, pthread/libc or VM qualification.
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

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <vector>
struct HostAbort { uint64_t code; };
[[noreturn]] void invariant(const char* text) {
    std::fprintf(stderr,"weighted bootstrap invariant: %s\n",text); std::abort();
}
#define CHECK(x) do { if (!(x)) invariant(#x); } while (false)
enum class Op : uint64_t { SET_AFFINITY=0x104, GET_AFFINITY=0x105 };
constexpr uint64_t ONLINE=0xff, PAGE=4096;
constexpr char RESULTS[]="/srv/task-placement-results";
enum class Position { CURRENT, QUEUED, HANDOFF, WAITING, DEAD };
struct Actor { uint64_t tid{},cpu{},mask{ONLINE}; Position position{Position::CURRENT}; int nice{}; };
struct HostModel {
    std::map<uint64_t,Actor> actors;
    uint64_t active{201}, now{1}, process{101}, child{999}, next_tid{2000};
    bool created{}, done{}, joined{}, in_child{}, sample_race{}, race_seen{}, corrupt_move{}, create_error{}, join_error{};
    unsigned injected_busy{}, creates{}, measured_creates{}, confirmations{}, joins{}, child_error{}, clock_calls{};
    int injected_error{}; uint64_t origin{}, step{10000}, first_clock{};
    bool force_deadline{};
    void* (*pending)(void*){}; void* argument{}; void* result{};
    void* (*measured)(void*){}; void* (*confirm)(void*){};
    std::map<uint64_t,void*> arguments;
    void (*finish)(void*){};
    explicit HostModel(uint64_t cpu) { actors.emplace(active,Actor{active,cpu}); }
    Actor& current() { return actors.at(active); }
    void dispatch() {
        if (!created || done || in_child) return;
        uint64_t const parent=active; auto& p=actors.at(parent); auto& c=actors.at(child);
        Position const original=p.position;
        if (p.position!=Position::WAITING && p.cpu==c.cpu) p.position=Position::QUEUED;
        active=child; c.position=Position::CURRENT; in_child=true;
        try { result=pending(argument); } catch (HostAbort e) { child_error=static_cast<unsigned>(e.code); }
        c.position=Position::DEAD; done=true; in_child=false; active=parent;
        p.position=original==Position::WAITING?Position::WAITING:Position::CURRENT;
    }
};
HostModel* host;
uint64_t host_cpu() { return host->current().cpu; }
uint64_t host_tid() { return host->active; }
uint64_t host_yield() {
    host->now+=host->step;
    if (!host->in_child) host->dispatch(); // One legal queued-child dispatch; sole remote CURRENT may pick itself.
    return 0;
}
int64_t raw(Op op,uint64_t tid,uint64_t mask=0,uint64_t=0) {
    auto found=host->actors.find(tid); if (found==host->actors.end()) return -ESRCH;
    auto& target=found->second;
    if (op==Op::GET_AFFINITY) return static_cast<int64_t>(target.mask);
    if (mask==0 || (mask&~ONLINE)!=0) return -EINVAL;
    if (host->injected_error && host->active==201 && tid==201) return host->injected_error;
    if (host->sample_race && !host->race_seen && host->active==201 && tid==201) {
        host->race_seen=true; target.cpu=(target.cpu+1)%8; return -EBUSY;
    }
    if (host->in_child && tid==201 && host->injected_busy) { --host->injected_busy; return -EBUSY; }
    uint64_t cpu=0; while ((mask&(uint64_t{1}<<cpu))==0) ++cpu;
    bool const reserved=target.position==Position::CURRENT || target.position==Position::HANDOFF;
    if ((reserved || target.position==Position::WAITING) && cpu!=target.cpu) return -EBUSY;
    if (host->corrupt_move && host->in_child && tid==201) return 0;
    target.cpu=cpu; target.mask=mask;
    if (!host->in_child && tid!=host->active && host->arguments.contains(tid)) {
        uint64_t const parent=host->active; host->active=tid; target.position=Position::CURRENT;
        host->confirm(host->arguments.at(tid)); ++host->confirmations;
        target.position=Position::QUEUED; host->active=parent;
    }
    return 0;
}
int host_clock_gettime(clockid_t, timespec* value) {
    host->now+=host->step;
    if (++host->clock_calls==1) host->first_clock=host->now;
    if (host->force_deadline && host->clock_calls==3) host->now=host->first_clock+60000000;
    value->tv_sec=static_cast<time_t>(host->now/1000000);
    value->tv_nsec=static_cast<long>((host->now%1000000)*1000); return 0;
}
int host_getpriority(int,int) { return host->current().nice; }
int host_setpriority(int,int,int nice) { host->current().nice=nice; return 0; }
int host_lstat(const char* path,struct stat* value) {
    CHECK(std::strcmp(path,RESULTS)==0); *value={};value->st_mode=S_IFDIR|0700;value->st_uid=value->st_gid=100;
    value->st_dev=42;value->st_ino=71; return 0;
}
namespace ker::process {
constexpr uint64_t WKI_TARGET_FLAG_LOCAL=1;
uint64_t getpid() { return host->process; } uint64_t getuid() { return 100; }
uint64_t geteuid() { return 100; } uint64_t getgid() { return 100; }
int64_t setwkitarget(const char*,uint64_t,uint64_t) { return 0; }
}
namespace ker::multiproc {
uint64_t getcurrent_cpu() { return host_cpu(); } uint64_t currentThreadId() { return host_tid(); }
uint64_t nativeThreadCount() { return 8; } uint64_t yield() { return host_yield(); }
}
int host_pthread_create(pthread_t* id,const pthread_attr_t*,void*(*entry)(void*),void* argument) {
    if (host->create_error) return EAGAIN;
    auto& parent=host->current(); ++host->creates;
    if (entry==host->measured) {
        CHECK(parent.tid==201 && parent.cpu==0 && parent.mask==1);
        CHECK(!host->created || (host->done && host->joined)); // The actual supervisor cannot create measured roles before join.
        uint64_t tid=host->next_tid++; *id=static_cast<pthread_t>(tid);
        host->actors.emplace(tid,Actor{tid,parent.cpu,parent.mask,Position::CURRENT});
        host->arguments[tid]=argument; uint64_t caller=host->active;
        parent.position=Position::QUEUED;host->active=tid;entry(argument);
        host->actors.at(tid).position=Position::QUEUED;host->active=caller;parent.position=Position::CURRENT;
        ++host->measured_creates; return 0;
    }
    CHECK(!host->created); host->created=true;host->origin=parent.cpu;
    uint64_t const cpu=parent.mask==ONLINE?(parent.cpu+1)%8:parent.cpu;
    host->actors.emplace(host->child,Actor{host->child,cpu,parent.mask,Position::QUEUED});
    host->pending=entry;host->argument=argument;*id=static_cast<pthread_t>(host->child); return 0;
}
int host_pthread_join(pthread_t id,void** result) {
    uint64_t const tid=static_cast<uint64_t>(id);++host->joins;
    if (host->join_error) return EINVAL;
    if (tid==host->child) {
        if (!host->done) { host->current().position=Position::WAITING;host->dispatch();host->current().position=Position::CURRENT; }
        CHECK(host->done);host->joined=true;*result=host->result;return host->child_error?ECANCELED:0;
    }
    CHECK(host->arguments.contains(tid));host->finish(host->arguments.at(tid));
    host->actors.at(tid).position=Position::DEAD;*result=nullptr;return 0;
}
#define clock_gettime host_clock_gettime
#define getpriority host_getpriority
#define setpriority host_setpriority
#define lstat host_lstat
#define pthread_create host_pthread_create
#define pthread_join host_pthread_join
'''

TAIL = r'''
#undef pthread_create
#undef pthread_join
#undef lstat
#undef clock_gettime
#undef getpriority
#undef setpriority
void finish_worker(void* opaque) {
    auto& argument=*static_cast<weighted_fairness::WorkArgument*>(opaque);
    host->now=std::max(host->now,argument.run->window.end+100); // Measurement endpoint; never move the clock backwards.
}
void reset(HostModel& model) {
    host=&model;host->measured=weighted_fairness::contender;host->confirm=weighted_fairness::host_confirm;
    host->finish=finish_worker;
}
void positive(uint64_t cpu,bool race=false,unsigned busy=0) {
    HostModel model(cpu);reset(model);model.sample_race=race;model.injected_busy=busy;
    try {
        int const rc=weighted_fairness::host_setup_supervisor();
        if (rc!=0 && weighted_fairness::host_report.parent_set_calls-weighted_fairness::host_report.parent_set_busy!=13)
            invariant("actual supervisor enforces thirteen parent SET successes");
        CHECK(rc==0);
    }
    catch (HostAbort) { invariant("actual setup completes within the unchanged deadline"); }
    CHECK(model.measured_creates==12 && model.confirmations==12);
    CHECK(model.current().cpu==0 && model.current().mask==1);
    CHECK(model.joins==12+(model.created?1:0));
    CHECK(weighted_fairness::host_report.parent_set_calls-weighted_fairness::host_report.parent_set_busy==13);
    CHECK(weighted_fairness::host_report.initial_mask==ONLINE && weighted_fairness::host_report.completed_trials==3);
}
void expected_failure(int which) {
    HostModel model(4);reset(model);
    if (which==0) model.injected_error=-EINVAL;
    if (which==1) model.create_error=true;
    if (which==2) model.join_error=true;
    if (which==3) model.force_deadline=true;
    if (which==4) model.corrupt_move=true;
    bool failed=false;try { static_cast<void>(weighted_fairness::host_setup_supervisor()); } catch (HostAbort) { failed=true; }
    CHECK(failed);
}
void endpoint_counterexample() {
    HostModel model(5);reset(model);model.actors.at(201).mask=32;
    model.actors.emplace(999,Actor{999,2,4,Position::CURRENT});model.active=999;
    weighted_fairness::Run run{};run.started=1;run.deadline=60000001;
    for (Position state:{Position::CURRENT,Position::HANDOFF,Position::WAITING}) {
        model.actors.at(201).position=state;CHECK(raw(Op::SET_AFFINITY,201,1)==-EBUSY);
    }
    model.actors.at(201).position=Position::CURRENT;
    uint64_t calls=0,busy=0;bool expired=false;
    try { weighted_fairness::set_affinity(run,201,1,calls,busy); }
    catch (HostAbort e) { expired=e.code==DEADLINE; }
    CHECK(expired && calls==busy && busy>1 && model.actors.at(201).cpu==5);
}
void verification_controls() {
    HostModel model(0);reset(model);weighted_fairness::FairnessReport report{};
    report.tid=201;weighted_fairness::Run run{};run.started=1;run.deadline=60000001;
    model.actors.at(201).mask=1;
    weighted_fairness::prepare_trial_control(run,report);
    CHECK(run.control_set.load() && run.control_pinned.load());
    model.actors.at(201).cpu=2;bool refused=false;
    try { weighted_fairness::prepare_trial_control(run,report); } catch (HostAbort e) { refused=e.code==CPU; }
    CHECK(refused);
    weighted_fairness::TaskReport task{};task.tid=201;task.index=0;refused=false;
    try { weighted_fairness::verify_inherited_contender(run,task); } catch (HostAbort e) { refused=e.code==PIN; }
    CHECK(refused);
    model.actors.at(201).cpu=0;model.actors.at(201).mask=ONLINE;refused=false;
    try { weighted_fairness::verify_inherited_contender(run,task); } catch (HostAbort e) { refused=e.code==PIN; }
    CHECK(refused);
}
void math_controls() {
    using namespace weighted_fairness;Window window{};CHECK(make_window(1,60000001,window));
    // Compute is an explicit Host endpoint; keep its captured limits without using it as a measurement.
    static_cast<void>(MAX_CHUNKS); static_cast<void>(STEPS);
    CHECK(window.start-window.warm==1000000 && window.end-window.start==5000000);
    CHECK(!make_window(53900001,60000001,window));uint64_t value{};
    CHECK(!add(UINT64_MAX,1,value) && !multiply(UINT64_MAX,2,value));
    CHECK(!clock_us(-1,0,false,value) && !clock_us(0,1,true,value));
    PairMath math{};Bounds a{2500000,2499998,2500002},b=a;CHECK(judge_pair(1024,1024,a,b,math));
    a={2800000,2799998,2800002};b={2200000,2199998,2200002};CHECK(!judge_pair(1024,1024,a,b,math));
}
int main() {
    math_controls();endpoint_counterexample();positive(4);positive(4,true,2);
    for(uint64_t cpu=0;cpu<8;++cpu) { positive(cpu);positive(cpu,true,2); }
    verification_controls();
    for(int i=0;i<5;++i)expected_failure(i);
    std::puts("PASS actual weighted bootstrap/control ordering, adversarial endpoints and math");return 0;
}
'''


def mask_numeric_separators(source):
    """Hide integer separators from the shared lexer without moving offsets.

    Comments and ordinary C++ string/character literals are protected tokens.
    The masked text is only a lexer input and is never emitted or compiled.
    """
    token = re.compile(
        r'//[^\n]*|/\*[\s\S]*?\*/|(?:u8|u|U|L)?"(?:\\.|[^"\\])*"|'
        r"(?:u8|u|U|L)?'(?:\\.|[^'\\])*'|"
        r"(?P<number>(?<![A-Za-z_0-9.])(?:0[xX][0-9a-fA-F]+(?:'[0-9a-fA-F]+)*|"
        r"0[bB][01]+(?:'[01]+)*|[0-9]+(?:'[0-9]+)*)(?:[uUlLzZ]*))"
    )
    masked = token.sub(lambda match: match[0].replace("'", " ")
                       if match.group("number") is not None else match[0], source)
    if len(masked) != len(source) or masked.count("\n") != source.count("\n"):
        raise ValueError("separator mask changed source offsets")
    return masked


def source_capture(source, anchor, capture, *, declaration=False):
    masked = mask_numeric_separators(source)
    masked_body, row = capture(masked, anchor, declaration=declaration)
    start = 0
    for _ in range(row["line"] - 1):
        start = source.index("\n", start) + 1
    end = start + len(masked_body)
    if masked[start:end] != masked_body:
        raise ValueError("shared capture offsets do not select its returned body")
    original = source[start:end]
    # Every generated fragment and witness refers to original source bytes.
    return original, dict(row, bytes=len(original.encode()), sha256=hashlib.sha256(original.encode()).hexdigest())


def capture_control(capture):
    sample = r"""constexpr auto before=1'000'000'000ULL;
char quote='\'';
const char* decoy="auto captured_value() { '1'000 }";
// auto captured_value() { 9'999 } and a character quote '
auto captured_value() -> unsigned {
    char closing='}';
    const char* text="braces { } and numeric-looking 1'234";
    /* braces } { and 4'444 must remain comment bytes */
    return 2'345U;
}
"""
    expected_mask = sample.replace("before=1'000'000'000ULL", "before=1 000 000 000ULL")
    expected_mask = expected_mask.replace("return 2'345U;", "return 2 345U;")
    if mask_numeric_separators(sample) != expected_mask:
        raise ValueError("numeric separator mask altered a protected literal/comment")
    expected = sample[sample.index("auto captured_value() ->"):].rstrip("\n")
    body, row = capture(sample, "auto captured_value(")
    if body != expected or row["bytes"] != len(expected.encode()) or row["sha256"] != hashlib.sha256(expected.encode()).hexdigest():
        raise ValueError("original C++ body capture control failed")
    return dict(original_body_sha256=row["sha256"], original_body_bytes=row["bytes"],
                odd_separator_before_definition=True, protected_character_string_comment=True)


def generate(source, capture):
    weighted = source.split("namespace weighted_fairness {", 1)[1]
    fragments = []
    def body(text, anchor, declaration=False):
        value, row = capture(text, anchor, declaration=declaration)
        fragments.append(row)
        return value
    common = body(source, "enum Failure : uint64_t {", True) + "\n" + body(source, "struct Checks {", True)
    common += "\n[[noreturn]] void abort_live(Failure code,uint64_t=0,int64_t=0) { throw HostAbort{static_cast<uint64_t>(code)}; }\n"
    common += body(source, "void expect_live(Checks& checks,")
    pure = weighted.split("// BEGIN WEIGHTED_FAIRNESS_PURE", 1)[1].split("\n", 1)[1].split("// END WEIGHTED_FAIRNESS_PURE", 1)[0]
    types = weighted[weighted.index("constexpr uint64_t REPORT_MAGIC"):weighted.index("auto time_us(")]
    functions = "\n".join(body(weighted, anchor, anchor.startswith("struct ")) for anchor in (
        "auto time_us(", "auto sample(", "void count(", "void setup_yield(", "auto affinity(", "void set_affinity(", "auto own_nice(",
        "struct BootstrapState {", "auto pin_controller_current(", "auto bootstrap_controller(",
        "void establish_controller(", "void prepare_trial_control(", "void verify_inherited_contender("))
    contender = body(weighted, "auto contender(")
    confirmation = body(contender, "while (!slot.confirmed.load(")
    prefix = contender[:contender.index("while (!slot.confirmed.load(")]
    aliases = contender[contender.index("    auto const& argument"):contender.index("    report.nonce")]
    worker = prefix + "return nullptr;\n}\n" + "auto host_confirm(void* opaque)->void* {\n" + aliases + confirmation + "\nreturn nullptr;\n}\n"
    supervisor = body(weighted, "auto supervisor(")
    # Keep the actual supervisor from entry through all three create/control/window/join sequences.
    # Only measured report inspection/pair judging and persistence are explicit Host endpoints.
    start = supervisor.index("        // All mutable worker reports")
    end = supervisor.index("        ++report.completed_trials;", start)
    supervisor = supervisor[:start] + supervisor[end:]
    tail = supervisor.index("    struct stat current{};", supervisor.index("report.ended_us"))
    supervisor = supervisor[:tail] + "    host_report=report; return report.failures==0?0:56;\n}\n"
    supervisor = supervisor.replace("auto supervisor()", "auto host_setup_supervisor()", 1)
    code = PRELUDE + common + "\nnamespace weighted_fairness {\n" + pure + types + functions + worker
    code += "\nFairnessReport host_report{};\n" + supervisor + "\n}\n" + TAIL
    return code, fragments


def once(source, pattern, replacement):
    changed, count = re.subn(pattern, replacement, source, count=1, flags=re.S)
    if count != 1:
        raise ValueError("mutation anchor missing: " + pattern)
    return changed


def variants(actual, capture):
    def mutate_function(name, transform):
        old = capture(actual, name)[0]
        return actual.replace(old, transform(old), 1)
    result = {"actual": actual}
    result["stale-cpu"] = mutate_function("auto pin_controller_current(", lambda text: once(
        text, r"(\{)\s*for\s*\(;;\)\s*\{(.*?)uint64_t\s+const\s+CURRENT\s*=\s*ker::multiproc::getcurrent_cpu\(\);",
        r"\1 uint64_t const CURRENT=ker::multiproc::getcurrent_cpu(); for(;;) {\2"))
    result["spawn-before-pin"] = once(actual, r"state\.origin_cpu\s*=\s*pin_controller_current\(state\.timing,\s*report\);",
                                      "state.origin_cpu=ker::multiproc::getcurrent_cpu();")
    result["join-before-done"] = once(actual, r"while\s*\(!state\.moved\.load\(std::memory_order_acquire\)\)",
        "void* premature{}; (void)pthread_join(thread,&premature); while(!state.moved.load(std::memory_order_acquire))")
    result["missing-verify"] = mutate_function("void verify_inherited_contender(", lambda text:
        text[:text.index("{")+1] + " (void)run; (void)report; }")
    result["extra-three-pins"] = once(actual, r"void prepare_trial_control\(Run& run,\s*FairnessReport& report\)\s*\{",
        "void prepare_trial_control(Run& run,FairnessReport& report) { count(report.parent_set_calls); "
        "int64_t extra=raw(Op::SET_AFFINITY,report.tid,1); expect_live(report.checks,extra==0,PIN);")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--host-cxx", default=os.environ.get("HOST_CXX"))
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    root = args.repo_root.resolve()
    fixture = args.source_root.resolve()/"tests/fixtures/task_placement_runtime.cpp"
    helper = root/"tests/host/unit/file_mmap_registry_runtime_test.py"
    helper_bytes = helper.read_bytes()
    spec = importlib.util.spec_from_file_location("weighted_bootstrap_owner", helper)
    owned = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(owned)
    if helper.read_bytes() != helper_bytes:
        raise ValueError("maintained owner changed during import")
    compiler = args.host_cxx or shutil.which("g++", path=os.defpath) or shutil.which("clang++", path=os.defpath)
    if not compiler:
        raise ValueError("host compiler unavailable")
    owned.FILE_BUDGET, owned.SCRATCH_BUDGET, owned.LOG_BUDGET = 32 << 20, 128 << 20, 4 << 20
    owned.subreaper()
    scratch = Path(tempfile.mkdtemp(prefix="weighted-bootstrap-"))
    started, groups, commands, errors = time.monotonic(), [], [], []
    deadline = started + 140
    handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    def interrupted(number, _frame):
        raise KeyboardInterrupt(str(number))
    for sig in handlers:
        signal.signal(sig, interrupted)
    result, last_log = {}, None
    try:
        before = [owned.pin(p) for p in (fixture, Path(__file__).resolve(), helper, Path(compiler).resolve())]
        def capture(source, anchor, *, declaration=False):
            return source_capture(source, anchor, owned.capture, declaration=declaration)
        result["capture_control"] = capture_control(capture)
        actual, fragments = generate(fixture.read_text(), capture)
        cases = variants(actual, capture)
        dependencies = []
        def command(argv, label):
            nonlocal last_log
            last_log = scratch/(label+".log")
            mask = signal.pthread_sigmask(signal.SIG_BLOCK, set(handlers))
            try:
                row = owned.command(list(map(str, argv)), last_log, scratch, deadline, groups)
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, mask)
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
            if name == "actual":
                if executed["exit"] != 0 or text.count("PASS actual weighted bootstrap/control ordering, adversarial endpoints and math") != 1:
                    raise ValueError("actual source runtime checks failed")
            elif executed["exit"] != -signal.SIGABRT or "weighted bootstrap invariant:" not in text:
                raise ValueError("compiled mutant did not fail its runtime oracle: "+name)
            for row in pins:
                if owned.pin(Path(row["path"])) != row:
                    raise ValueError("compiler input changed: "+row["path"])
        after = [owned.pin(Path(row["path"])) for row in before]
        if after != before:
            raise ValueError("actual source/driver/compiler changed during checks")
        result.update(source_before=before,source_after=after,fragments=fragments,variants=list(cases),dependency_inputs=dependencies,
            limitations="Actual supervisor bootstrap/create/control/window/join slices and worker setup/confirmation; explicit sequential Host scheduling/thread/time/syscall/persistence endpoints. Measured compute/report math is not executed as a CPU fairness measurement; no native kernel/pthread/VM/qualification claim.")
    except BaseException as error:
        errors.append(type(error).__name__+": "+str(error))
        if last_log and args.evidence_dir is None and last_log.is_file():
            print(last_log.read_text(errors="replace")[-32768:], file=sys.stderr)
    finally:
        for sig in handlers:
            signal.signal(sig, signal.SIG_IGN)
        try:
            if groups:
                errors.append("owned command group unresolved; scratch preserved")
            else:
                if args.evidence_dir:
                    args.evidence_dir.mkdir(parents=True, exist_ok=True)
                    for log in scratch.glob("*.log"):
                        shutil.copy2(log,args.evidence_dir/log.name)
                shutil.rmtree(scratch)
        except BaseException as error:
            errors.append("cleanup: "+repr(error))
        for sig, original in handlers.items():
            signal.signal(sig, original)
    result.update(errors=errors,commands=commands,seconds=time.monotonic()-started,scratch_removed=not scratch.exists(),
                  qualification="PASS" if not errors else "FAIL",runtime_qualified=False)
    if args.evidence_dir:
        args.evidence_dir.mkdir(parents=True,exist_ok=True)
        (args.evidence_dir/"result.json").write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps({key:result[key] for key in ("qualification","seconds","errors","scratch_removed")}))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
