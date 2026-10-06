#!/usr/bin/env python3
"""Compile the full production wait and probe with controlled provider/time boundaries."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE = Path(os.environ.get("WOS_BLOCK_WAIT_SOURCE", ROOT / "modules/kern/src/dev/block_request.cpp"))
HEADER = Path(os.environ.get("WOS_BLOCK_WAIT_HEADER", ROOT / "modules/kern/src/dev/block_request.hpp"))


def extract(text, anchor):
    start = text.index(anchor)
    brace = text.index("{", start)
    end, depth = brace + 1, 1
    while depth:
        depth += (text[end] == "{") - (text[end] == "}")
        end += 1
    return text[start:end] + (";" if anchor.startswith("struct ") else "")


CLOCK = r'''
#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
namespace ker::mod::time {
inline std::atomic<uint64_t> block_test_now_us{1000};
inline thread_local size_t block_test_clock_reads{};
inline thread_local std::function<void(size_t)> probe_clock_hook;
inline auto get_us() -> uint64_t {
    ++block_test_clock_reads;
    if(probe_clock_hook)probe_clock_hook(block_test_clock_reads);
    return block_test_now_us.load(std::memory_order_relaxed);
}
}
'''

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdio>
#include <functional>
#include <initializer_list>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <dev/block_device.hpp>
#include "selected-request.hpp"
#include <mod/io/serial/serial.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
namespace ker::dev {
namespace sched=ker::mod::sched;
ACTUAL_POLL_CONSTANTS
ACTUAL_QUEUE_AND_PREDICATES
struct Scenario;
Scenario* active{};
auto service_queue(BlockRequestQueue&,bool extra_poll=false)->bool;
void pause_task(uint64_t);
ACTUAL_WAIT
using Expected=std::map<std::string,uint64_t>;
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
struct Field {const char* name;uint64_t BlockWaitDiagnostics::* member;};
constexpr std::array<Field,15> fields{{
    {"bursts",&BlockWaitDiagnostics::bursts},{"completed",&BlockWaitDiagnostics::completed},
    {"no_context",&BlockWaitDiagnostics::no_context},{"mandatory_busy",&BlockWaitDiagnostics::mandatory_busy},
    {"disallowed",&BlockWaitDiagnostics::disallowed},{"clock_regression",&BlockWaitDiagnostics::clock_regression},
    {"budget",&BlockWaitDiagnostics::budget},{"deadline",&BlockWaitDiagnostics::deadline},
    {"extra_busy",&BlockWaitDiagnostics::extra_busy},{"count_cap",&BlockWaitDiagnostics::count_cap},
    {"extra_passes",&BlockWaitDiagnostics::extra_passes},{"count_cap_under_budget",&BlockWaitDiagnostics::count_cap_under_budget},
    {"cap_elapsed_us",&BlockWaitDiagnostics::cap_elapsed_us},{"cap_elapsed_max_us",&BlockWaitDiagnostics::cap_elapsed_max_us},
    {"park_attempts",&BlockWaitDiagnostics::park_attempts},
}};
void check_diagnostics(const BlockWaitDiagnostics& actual,const Expected& expected){
    for(auto const& [name,value]:expected)assert(std::any_of(fields.begin(),fields.end(),[&](auto f){return name==f.name;}));
    for(auto field:fields){auto found=expected.find(field.name);uint64_t const wanted=found==expected.end()?0:found->second;
        if(actual.*field.member!=wanted)std::fprintf(stderr,"diagnostic field=%s actual=%llu expected=%llu\n",field.name,(unsigned long long)(actual.*field.member),(unsigned long long)wanted);
        assert(actual.*field.member==wanted);
    }
}
#endif
struct Scenario {
    BlockRequestQueue queue{};
    BlockRequest request{};
    sched::task::Task task{};
    size_t services{},pauses{},checks{};
    size_t mandatory_services{},extra_services{};
    bool next_service_mandatory{true};
    uint64_t last_interval{};
    bool retain_expected{true};
    std::function<bool(size_t)> service_hook;
    std::function<void(size_t)> pause_hook;
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
    BlockWaitDiagnostics seed{};
#endif
    Scenario(){
        assert(active==nullptr&&sched::block_test_shutdown_holds.load()==0);
        active=this;task.syscall_account_start_us=1;sched::block_test_current_task=&task;
        sched::block_test_shutdown.store(false);sched::block_test_run_queues.store(true);
        ker::mod::io::serial::block_test_panic_mode.store(false);
        ker::mod::time::block_test_now_us.store(1000);ker::mod::time::block_test_clock_reads=0;ker::mod::time::probe_clock_hook={};
        ker::mod::sys::block_test_irqsave_acquisitions=0;ker::mod::sys::block_test_irqsave_hook={};
        queue.native=true;queue.limits.depth=8;
        request.description={BlockRequestOperation::READ,0,1,512};
        assert(block_model_submit(queue.model,request.description,request.token)==0);
        BlockRequestToken issued{};assert(block_model_next_issue(queue.model,issued)&&issued==request.token);
        request.queue=&queue;request.deadline_us=100000;queue.owners[request.token.slot]=&request;
    }
    void complete(int status=0){
        assert(!request.completed.load(std::memory_order_acquire));
        assert(block_model_claim_completion(queue.model,request.token,status,status==0?512:0,true));
        auto const* state=block_model_detail::lookup(queue.model,request.token);request.status=state->status;request.bytes=state->completed_bytes;
        assert(block_model_finish_completion(queue.model,request.token));request.completed.store(true,std::memory_order_release);
    }
    void check_boundary(){
        assert(ker::mod::sys::block_test_spinlock_depth==0&&sched::block_test_shutdown_holds.load()==1);
        assert(task.references.load()==(retain_expected?2U:1U));
        assert(request.waiter==(retain_expected?static_cast<void*>(&task):nullptr));
    }
    void run(const char* name,size_t service_count,size_t park_count,size_t off_clocks,size_t added_clocks,const Expected& expected,int status=0){
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
        seed=queue.wait_diagnostics;
#endif
        auto const preceding_guard=ker::mod::sys::block_test_irqsave_hook;
        ker::mod::sys::block_test_irqsave_hook=[&,preceding_guard]{
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
            // Publication must happen once at the final existing guard, never
            // at a burst or provider boundary. The hook runs BEFORE locking.
            for(auto f:fields)assert(queue.wait_diagnostics.*f.member==seed.*f.member);
#endif
            if(preceding_guard)preceding_guard();
        };
        assert(block_request_wait(request)==status);
        ker::mod::sys::block_test_irqsave_hook={};
        assert(services==service_count&&pauses==park_count&&request.waiter==nullptr&&task.references.load()==1);
        assert(sched::block_test_shutdown_holds.load()==0&&ker::mod::sys::block_test_spinlock_depth==0);
        size_t const clocks=ker::mod::time::block_test_clock_reads;
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
        assert(clocks==off_clocks+added_clocks);check_diagnostics(queue.wait_diagnostics,expected);
        if(seed.bursts==0){
            auto const& d=queue.wait_diagnostics;
            assert(d.bursts==d.completed+d.no_context+d.mandatory_busy+d.disallowed+d.clock_regression+d.budget+d.deadline+d.extra_busy+d.count_cap);
            assert(d.park_attempts==pauses&&d.count_cap_under_budget<=d.count_cap);
        }
#else
        assert(clocks==off_clocks);(void)added_clocks;(void)expected;
#endif
        assert(block_model_release(queue.model,request.token)&&queue.model.owned_requests==0&&queue.model.owned_bytes==0);
        request.queue=nullptr;
        assert(services==mandatory_services+extra_services);
        std::printf("{\"case\":\"%s\",\"services\":%zu,\"mandatory_services\":%zu,\"extra_poll_services\":%zu,\"pauses\":%zu,\"retry_us\":%llu,\"clock_reads\":%zu,\"guards\":%zu}\n",name,services,mandatory_services,extra_services,pauses,(unsigned long long)last_interval,clocks,ker::mod::sys::block_test_irqsave_acquisitions);
    }
    ~Scenario(){
        assert(sched::block_test_shutdown_holds.load()==0&&ker::mod::sys::block_test_spinlock_depth==0);
        ker::mod::sys::block_test_irqsave_hook={};ker::mod::time::probe_clock_hook={};sched::block_test_current_task=nullptr;active=nullptr;
    }
};
auto service_queue(BlockRequestQueue& queue,bool extra_poll)->bool{
    assert(active&&&queue==&active->queue);active->check_boundary();++active->services;
    bool const expected_extra=!active->next_service_mandatory;
    if(extra_poll!=expected_extra)std::fprintf(stderr,"wait service extra_poll actual=%u expected=%u\n",unsigned(extra_poll),unsigned(expected_extra));
    assert(extra_poll==expected_extra);active->next_service_mandatory=false;
    if(extra_poll)++active->extra_services;else ++active->mandatory_services;
    assert(active->services<=128);return active->service_hook?active->service_hook(active->services):true;
}
void pause_task(uint64_t interval){
    assert(active);
    if(interval!=16)std::fprintf(stderr,"wait retry interval actual=%llu expected=16\n",(unsigned long long)interval);
    assert(interval==16);active->last_interval=interval;active->check_boundary();++active->pauses;active->next_service_mandatory=true;assert(active->pauses<=4);
    if(active->pause_hook)active->pause_hook(active->pauses);else active->complete();
}
TESTS
}
int main(){ker::dev::tests();}
'''

TESTS = r'''
void tests(){
    {Scenario s;s.service_hook=[&](size_t){s.complete();return true;};s.run("mandatory-completed",1,0,1,0,{{"bursts",1},{"completed",1}});}
    {Scenario s;s.service_hook=[&](size_t n){if(n==3)s.complete();return true;};s.run("extra-completed",3,0,3,0,{{"bursts",1},{"completed",1},{"extra_passes",2}});}
    {Scenario s;s.complete();s.run("already-terminal",0,0,0,0,{});}
    {Scenario s;s.run("frozen-clock-countcap",33,1,33,1,{{"bursts",1},{"count_cap",1},{"extra_passes",32},{"count_cap_under_budget",1},{"park_attempts",1}});assert(ker::mod::sys::block_test_irqsave_acquisitions==34);}
    {Scenario s;s.service_hook=[&](size_t n){if(n==33)s.complete();return true;};s.run("completed-on-extra32",33,0,33,0,{{"bursts",1},{"completed",1},{"extra_passes",32}});}
    {Scenario s;s.service_hook=[](size_t){return false;};s.run("mandatory-busy",1,1,1,0,{{"bursts",1},{"mandatory_busy",1},{"park_attempts",1}});}
    {Scenario s;s.service_hook=[](size_t n){return n!=2;};s.run("extra-busy",2,1,2,0,{{"bursts",1},{"extra_busy",1},{"park_attempts",1}});}
    {Scenario s;s.service_hook=[&](size_t){s.complete();return false;};s.run("busy-completion-race",1,0,1,0,{{"bursts",1},{"mandatory_busy",1}});}
    {Scenario s;s.service_hook=[&](size_t n){if(n==1)s.queue.closing=true;return true;};s.run("fresh-disallowed",1,1,1,0,{{"bursts",1},{"disallowed",1},{"park_attempts",1}});}
    {Scenario s;s.service_hook=[&](size_t){s.task.preempt_disable_depth=1;return true;};s.run("context-lost-after-start",1,1,1,0,{{"bursts",1},{"disallowed",1},{"park_attempts",1}});}
    {Scenario s;ker::mod::sys::block_test_irqsave_hook=[&]{if(ker::mod::sys::block_test_irqsave_acquisitions==1)s.complete();};s.run("qualification-terminal-race",1,0,1,0,{{"bursts",1},{"disallowed",1}});}
    {Scenario s;s.service_hook=[](size_t){ker::mod::time::block_test_now_us.store(1128);return true;};s.run("initial-service-budget",1,1,2,0,{{"bursts",1},{"budget",1},{"park_attempts",1}});}
    {Scenario s;s.service_hook=[](size_t){ker::mod::time::block_test_now_us.store(999);return true;};s.run("initial-service-clock-regression",1,1,2,0,{{"bursts",1},{"clock_regression",1},{"park_attempts",1}});}
    {Scenario s;s.service_hook=[](size_t n){if(n==2)ker::mod::time::block_test_now_us.store(1050);if(n==3)ker::mod::time::block_test_now_us.store(1040);return true;};s.run("later-clock-regression",3,1,4,0,{{"bursts",1},{"clock_regression",1},{"extra_passes",2},{"park_attempts",1}});}
    {Scenario s;s.request.deadline_us=1050;s.service_hook=[](size_t){ker::mod::time::block_test_now_us.store(1050);return true;};s.pause_hook=[&](size_t){s.complete(-ETIMEDOUT);};s.run("sampled-deadline",1,1,2,0,{{"bursts",1},{"deadline",1},{"park_attempts",1}},-ETIMEDOUT);}
    {Scenario s;s.request.deadline_us=1000;s.pause_hook=[&](size_t){s.complete(-ETIMEDOUT);};s.run("qualification-deadline",1,1,1,0,{{"bursts",1},{"disallowed",1},{"park_attempts",1}},-ETIMEDOUT);}
    {Scenario s;s.request.deadline_us=1100;s.service_hook=[](size_t){ker::mod::time::block_test_now_us.store(1128);return true;};s.pause_hook=[&](size_t){s.complete(-ETIMEDOUT);};s.run("budget-before-deadline",1,1,2,0,{{"bursts",1},{"budget",1},{"park_attempts",1}},-ETIMEDOUT);}
    for(uint64_t end:{uint64_t(999),uint64_t(1127),uint64_t(1128)}){
        Scenario s;s.service_hook=[&](size_t n){if(n==33)ker::mod::time::block_test_now_us.store(end);return true;};
        Expected d{{"bursts",1},{"count_cap",1},{"extra_passes",32},{"park_attempts",1}};
        if(end>=1000){d["cap_elapsed_us"]=end-1000;d["cap_elapsed_max_us"]=end-1000;if(end<1128)d["count_cap_under_budget"]=1;}
        s.run(end==999?"regressed-cap-sample":end==1127?"cap-sample127":"cap-sample128",33,1,33,1,d);
    }
    {Scenario s;s.service_hook=[&](size_t n){if(n==33)ker::mod::time::block_test_now_us.store(1010);if(n==66)ker::mod::time::block_test_now_us.store(1020);return true;};s.pause_hook=[&](size_t n){if(n==1)ker::mod::time::block_test_now_us.store(1000);else s.complete();};s.run("two-countcap-bursts",66,2,66,2,{{"bursts",2},{"count_cap",2},{"extra_passes",64},{"count_cap_under_budget",2},{"cap_elapsed_us",30},{"cap_elapsed_max_us",20},{"park_attempts",2}});}
    {Scenario s;s.service_hook=[&](size_t n){if(n==1)s.queue.closing=true;if(n==2)ker::mod::time::block_test_now_us.store(1128);if(n==3)s.complete();return true;};s.pause_hook=[&](size_t){s.queue.closing=false;ker::mod::time::block_test_now_us.store(1000);};s.run("partition-three-bursts-one-merge",3,2,4,0,{{"bursts",3},{"completed",1},{"disallowed",1},{"budget",1},{"park_attempts",2}});}
    {Scenario s;ker::mod::time::probe_clock_hook=[&](size_t reads){if(reads==34)s.complete();};s.pause_hook=[&](size_t){if(!s.request.completed.load())s.complete();};s.run("late-cap-completion-observational-park",33,1,33,1,{{"bursts",1},{"count_cap",1},{"extra_passes",32},{"count_cap_under_budget",1},{"park_attempts",1}});}
    for(unsigned context=0;context<16;++context){
        Scenario s;
        switch(context){
            case 0:s.queue.native=false;break;case 1:s.queue.limits.depth=1;break;
            case 2:s.task.syscall_account_start_us=0;break;case 3:s.task.preempt_disable_depth=1;break;
            case 4:s.task.scheduler_transition_active.store(true);break;case 5:s.task.deferred_task_switch=true;break;
            case 6:s.task.wants_block=true;break;case 7:s.task.voluntary_block=true;break;
            case 8:s.task.wki_proxy_task_id=1;break;case 9:s.task.interrupting_signal_pending=true;break;
            case 10:sched::block_test_shutdown.store(true);break;case 11:ker::mod::io::serial::block_test_panic_mode.store(true);break;
            case 12:sched::block_test_run_queues.store(false);break;case 13:s.task.type=sched::task::TaskType::DAEMON;break;
            case 14:s.task.type=sched::task::TaskType::IDLE;break;case 15:sched::block_test_current_task=nullptr;s.retain_expected=false;break;
        }
        std::string name="no-context-"+std::to_string(context);s.run(name.c_str(),1,1,0,0,{{"bursts",1},{"no_context",1},{"park_attempts",1}});
    }
    {Scenario s;s.task.type=sched::task::TaskType::DAEMON;s.service_hook=[&](size_t){s.complete();return false;};s.run("no-context-before-busy-and-complete",1,0,0,0,{{"bursts",1},{"no_context",1}});}
    {Scenario s;s.task.reject_acquire.store(true);s.retain_expected=false;s.service_hook=[&](size_t){s.complete();return true;};s.run("waiter-retain-failure-balanced",1,0,1,0,{{"bursts",1},{"completed",1}});}
    {Scenario s;s.task.deferred_task_switch=s.task.yield_switch=true;s.service_hook=[&](size_t n){if(n==2)s.complete();return true;};s.run("authorized-return-yield-preserved",2,0,2,0,{{"bursts",1},{"completed",1},{"extra_passes",1}});assert(s.task.deferred_task_switch&&s.task.yield_switch);}
    {Scenario s;s.service_hook=[&](size_t){assert(block_model_cancel(s.queue.model,s.request.token,-ECANCELED)==BlockCancelResult::NEEDS_RETIREMENT);return true;};s.run("cancel-intent-disallowed",1,1,1,0,{{"bursts",1},{"disallowed",1},{"park_attempts",1}},-ECANCELED);}
    for(bool busy:{false,true}){
        Scenario s;s.request.queue=busy?&s.queue:nullptr;s.request.waiter=busy?&s.task:nullptr;
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
        s.queue.wait_diagnostics.bursts=17;
#endif
        assert(block_request_wait(s.request)==(busy?-EBUSY:-EINVAL));
        assert(s.task.references.load()==1&&sched::block_test_shutdown_holds.load()==0&&s.services==0&&s.pauses==0&&ker::mod::time::block_test_clock_reads==0);
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
        check_diagnostics(s.queue.wait_diagnostics,{{"bursts",17}});
#endif
        s.request.waiter=nullptr;s.request.queue=&s.queue;s.complete();assert(block_model_release(s.queue.model,s.request.token));s.request.queue=nullptr;
        std::printf("{\"case\":\"early-%s\",\"services\":0,\"pauses\":0,\"clock_reads\":0}\n",busy?"busy":"invalid");
    }
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
    {BlockWaitDiagnostics target{},source{};for(auto f:fields){target.*f.member=UINT64_MAX-1;source.*f.member=3;}
        target.cap_elapsed_max_us=17;source.cap_elapsed_max_us=3;merge_wait_diagnostics(target,source);
        Expected wanted;for(auto f:fields)wanted[f.name]=UINT64_MAX;wanted["cap_elapsed_max_us"]=17;check_diagnostics(target,wanted);
        source={};source.cap_elapsed_max_us=23;merge_wait_diagnostics(target,source);wanted["cap_elapsed_max_us"]=23;check_diagnostics(target,wanted);
        std::printf("{\"case\":\"saturating-merge-and-maximum\",\"pass\":true}\n");}
    {Scenario s;for(auto f:fields)s.queue.wait_diagnostics.*f.member=UINT64_MAX-1;
        s.service_hook=[](size_t n){if(n==33)ker::mod::time::block_test_now_us.store(1127);return true;};
        Expected wanted;for(auto f:fields)wanted[f.name]=UINT64_MAX-1;for(auto name:{"bursts","count_cap","extra_passes","count_cap_under_budget","cap_elapsed_us","park_attempts"})wanted[name]=UINT64_MAX;
        s.run("saturated-one-wait-merge",33,1,33,1,wanted);}
#endif
    std::printf("Block wait diagnostics full-body controls PASS\n");
}
'''


def main():
    actual = SOURCE.read_text()
    constants = []
    for anchor in ("constexpr size_t WAIT_POLL_EXTRA_PASSES", "constexpr uint64_t WAIT_POLL_BUDGET_US"):
        start = actual.index(anchor)
        constants.append(actual[start:actual.index(";", start) + 1])
    # Saved sources without this declaration must still compile and reach
    # the independent interval assertion, rather than fail extraction.
    retry_anchor = "constexpr uint64_t WAIT_RETRY_INTERVAL_US"
    if retry_anchor in actual:
        start = actual.index(retry_anchor)
        constants.append(actual[start:actual.index(";", start) + 1])
    sections = [extract(actual, anchor) for anchor in ("struct BlockRequestQueue {", "void increment(",
                "auto safe_syscall_task()", "auto wait_poll_context(", "auto wait_poll_allowed(")]
    if "void merge_wait_diagnostics(" in actual:
        sections.append("#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS\n" + extract(actual, "void merge_wait_diagnostics(") + "\n#endif\n")
    harness = PRELUDE.replace("ACTUAL_POLL_CONSTANTS", "\n".join(constants)).replace("ACTUAL_QUEUE_AND_PREDICATES", "\n".join(sections)).replace("ACTUAL_WAIT", extract(actual, "auto block_request_wait(")).replace("TESTS", TESTS)
    mode = os.environ.get("WOS_BLOCK_WAIT_TEST_MODE", "both")
    if mode not in ("both", "on", "off"):
        raise ValueError("unknown wait diagnostics mode: " + mode)
    modes = ["on", "off"] if mode == "both" else [mode]
    retained = os.environ.get("WOS_BLOCK_WAIT_ARTIFACT_DIR")
    with tempfile.TemporaryDirectory(prefix="block-wait-diagnostics-", dir=os.environ.get("TMPDIR")) as directory:
        work = Path(directory)
        clock = work / "platform/ktime/ktime.hpp"
        clock.parent.mkdir(parents=True)
        clock.write_text(CLOCK)
        (work / "selected-request.hpp").write_bytes(HEADER.read_bytes())
        source = work / "wait.cpp"
        source.write_text(harness)
        for flag in modes:
            compiler = [os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++"), "-std=c++23", "-O1", "-g", "-pthread",
                "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-DWOS_ASYNC_BLOCK_IO=1",
                "-I", str(work), "-I", str(ROOT / "tests/host/unit/block_request_runtime_shim"),
                "-I", str(ROOT / "modules/kern/src")]
            if flag == "on":
                compiler.append("-DWOS_BLOCK_WAIT_DIAGNOSTICS=1")
            binary = work / ("wait-" + flag)
            command = compiler + [str(source), "-o", str(binary)]
            artifact = Path(retained) / flag if retained else None
            if artifact:
                artifact.mkdir(parents=True, exist_ok=True)
                (artifact / "wait.cpp").write_text(harness)
                (artifact / "selected-request.hpp").write_bytes(HEADER.read_bytes())
                (artifact / "clock-shim.hpp").write_text(CLOCK)
                (artifact / "compiler-argv.json").write_text(json.dumps(command, indent=2) + "\n")
                pp = compiler + ["-E", "-P", str(source)]
                with (artifact / "preprocessed.cpp").open("w") as output:
                    subprocess.run(pp, stdout=output, check=True)
            subprocess.run(command, check=True)
            if artifact:
                shutil.copyfile(binary, artifact / "wait.bin")
            environment = dict(os.environ, ASAN_OPTIONS="detect_leaks=0:halt_on_error=1", LSAN_OPTIONS="detect_leaks=0")
            result = subprocess.run([str(binary)], text=True, capture_output=True, env=environment)
            print(result.stdout, end="")
            print(result.stderr, end="")
            if artifact:
                (artifact / "runtime.log").write_text(result.stdout + result.stderr)
            result.check_returncode()
            print(json.dumps({"mode": flag, "source": str(SOURCE), "source_sha256": hashlib.sha256(SOURCE.read_bytes()).hexdigest(), "pass": True}))


if __name__ == "__main__":
    main()
