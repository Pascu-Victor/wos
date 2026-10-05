"""Exercise real timer trace storage and recorder with explicit CPU/clock seams.

The extra reader-collision build injects one callback after payload reads so a
slot can deterministically wrap before validation. It does not replace storage,
publication, or generation rules. Hardware delivery and IRQ latency are runtime
checks; this host test establishes bounded, honest concurrent trace publication.
"""
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
header = (ROOT / "modules/kern/src/platform/sched/scheduler_timer_trace.hpp").read_text()
source = (ROOT / "modules/kern/src/platform/sched/scheduler.cpp").read_text()
capacity = int(re.search(r"static constexpr size_t CAPACITY = (\d+);", header)[1])


def function(name):
    found = re.search(rf"^(?:auto|void) {name}\([^;{{]*\{{", source, re.M)
    assert found, name
    pos, depth = source.index("{", found.start()) + 1, 1
    while depth:
        depth += (source[pos] == "{") - (source[pos] == "}")
        pos += 1
    return source[found.start():pos]


prefix = r'''
#include <cassert>
#include <cstdio>
#include <thread>
void trace_reader_hook();
'''
seams = r'''
namespace ker::mod::sched {
namespace task {enum class TaskType:uint8_t{DAEMON,PROCESS,IDLE};
 struct Task{uint64_t pid=19;TaskType type=TaskType::PROCESS;bool cpu_pinned=true;};}
namespace time {unsigned reads=0;uint64_t get_us(){++reads;return 91;}}
namespace smt {uint64_t get_core_count(){return 2;}}
struct RunQueue{SchedulerTimerTrace timer_trace;};
struct Queues{std::array<RunQueue,2> rows{};unsigned cpu=0;
 RunQueue* this_cpu(){return &rows.at(cpu);}RunQueue* that_cpu(uint64_t c){return &rows.at(c);}};
Queues queues;Queues* run_queues=&queues;task::Task current;
task::Task* current_task_for_preempt(){return &current;}
std::atomic<bool> scheduler_timer_trace_active{false};
'''
functions = "\n".join(function(n) for n in (
    "set_scheduler_timer_trace_enabled", "scheduler_timer_trace_enabled",
    "record_scheduler_timer_trace", "capture_scheduler_timer_trace"))
tests = r'''
} // namespace ker::mod::sched
using namespace ker::mod::sched;
SchedulerTimerTrace* hook_trace=nullptr;
void put(SchedulerTimerTrace& trace){
 auto r=trace.reserve();if(!r.claimed)return;
 trace.publish(r,{.at_us=r.sequence^0x13791,.pid=r.sequence*3+7,
                  .value=~r.sequence,.metadata=r.sequence^0x71391});
}
void trace_reader_hook(){
 if(hook_trace){auto* trace=hook_trace;hook_trace=nullptr;for(unsigned i=0;i<TRACE_CAPACITY;++i)put(*trace);}
}
void validate(SchedulerTimerTraceSnapshot const& s){
 assert(s.count<=s.events.size());
 assert(s.count+s.missing+s.omitted==s.end_sequence-s.start_sequence);
 uint64_t previous=s.start_sequence;
 for(size_t i=0;i<s.count;++i){auto const& e=s.events.at(i);
  assert(e.sequence>previous&&e.sequence<=s.end_sequence);previous=e.sequence;
  assert(e.at_us==(e.sequence^0x13791)&&e.pid==e.sequence*3+7);
  assert(e.value==~e.sequence&&e.metadata==(e.sequence^0x71391));}
 assert(s.complete()==(s.enabled&&s.omitted==0&&s.missing==0));
}
int main(){
 unsigned cases=0;
 SchedulerTimerTrace empty;auto e=empty.snapshot(0,0);assert(e.complete()&&e.count==0);++cases;
 for(unsigned i=0;i<16;++i)put(empty);
 auto normal=empty.snapshot(0,16);validate(normal);assert(normal.complete()&&normal.count==16);++cases;
 SchedulerTimerTrace observed_wake;for(unsigned i=0;i<60;++i)put(observed_wake);
 auto wake_window=observed_wake.snapshot(0,60);validate(wake_window);
 assert(wake_window.complete()&&wake_window.count==60);++cases;
 for(unsigned i=16;i<200;++i)put(empty);
 auto truncated=empty.snapshot(0,200);validate(truncated);
 assert(!truncated.complete()&&truncated.omitted==200-SCHEDULER_TIMER_TRACE_SNAPSHOT_EVENTS
        &&truncated.count==SCHEDULER_TIMER_TRACE_SNAPSHOT_EVENTS);++cases;
 auto overwritten=empty.snapshot(0,1);validate(overwritten);assert(overwritten.missing==1);++cases;
 SchedulerTimerTrace interrupted;auto held=interrupted.reserve();assert(held.claimed&&held.sequence==1);
 auto pending=interrupted.snapshot(0,1);validate(pending);assert(pending.missing==1);++cases;
 for(unsigned i=2;i<=TRACE_CAPACITY;++i)put(interrupted);
 auto collision=interrupted.reserve();assert(collision.sequence==TRACE_CAPACITY+1&&!collision.claimed);
 interrupted.publish(held,{.at_us=1^0x13791,.pid=10,.value=~uint64_t{1},.metadata=1^0x71391});
 auto late=interrupted.snapshot(0,1);validate(late);assert(late.complete());
 auto dropped=interrupted.snapshot(TRACE_CAPACITY,TRACE_CAPACITY+1);validate(dropped);assert(dropped.missing==1);++cases;
 auto invalid=interrupted.snapshot(5,4);assert(!invalid.complete()&&invalid.missing==1);++cases;
#if READER_HOOK
 SchedulerTimerTrace reader_collision;put(reader_collision);hook_trace=&reader_collision;
 auto changed=reader_collision.snapshot(0,1);validate(changed);assert(changed.missing==1);++cases;
#endif
 SchedulerTimerTrace concurrent;std::atomic<unsigned> running{2};std::atomic<uint64_t> observed{};
 auto producer=[&]{for(unsigned i=0;i<100000;++i)put(concurrent);running.fetch_sub(1);};
 std::thread a(producer),b(producer);
 do {uint64_t end=concurrent.sequence();auto s=concurrent.snapshot(end>16?end-16:0,end);
  validate(s);observed.fetch_add(s.count);}while(running.load()!=0);
 a.join();b.join();assert(concurrent.sequence()==200000&&observed.load()>0);++cases;
 auto full_end=concurrent.snapshot(199984,200000);validate(full_end);
 // Recorder OFF and early/no-runqueue cases must not read the clock or reserve.
 set_scheduler_timer_trace_enabled(false);time::reads=0;
 record_scheduler_timer_trace(SchedulerTimerTraceKind::ARM_LOCAL,1,8);
 assert(time::reads==0&&queues.rows[0].timer_trace.sequence()==0);++cases;
 set_scheduler_timer_trace_enabled(true);run_queues=nullptr;
 record_scheduler_timer_trace(SchedulerTimerTraceKind::ARM_LOCAL,1,8);assert(time::reads==0);++cases;
 run_queues=&queues;record_scheduler_timer_trace(SchedulerTimerTraceKind::ARM_LOCAL,1,8);
 auto recorded=queues.rows[0].timer_trace.snapshot(0,1);assert(recorded.complete()&&recorded.count==1);
 auto const& r=recorded.events[0];assert(r.at_us==91&&r.pid==19&&r.value==1);
 assert((r.metadata&0xff)==uint8_t(SchedulerTimerTraceKind::ARM_LOCAL));
 assert(((r.metadata>>8)&0xff)==uint8_t(task::TaskType::PROCESS)&&((r.metadata>>16)&1)==1);
 assert((r.metadata>>32)==8&&time::reads==1);++cases;
 SchedulerTimerTraceWindow window{.end_sequence=1,.enabled=true};SchedulerTimerTraceSnapshot remote{};
 queues.cpu=1;capture_scheduler_timer_trace(0,window,remote);assert(remote.complete()&&remote.count==1);++cases;
 SchedulerTimerTraceSnapshot disabled{};capture_scheduler_timer_trace(0,{},disabled);assert(!disabled.complete());
 window.enabled=true;capture_scheduler_timer_trace(2,window,remote);assert(!remote.complete());++cases;
 printf("%u bounded trace/recorder cases PASS; 200000 concurrent reservations, %lu valid observations\n",cases,observed.load());
}
'''
compiler = os.environ.get("WOS_HOST_CXX") or shutil.which("clang++-23") or "/usr/bin/clang++"
with tempfile.TemporaryDirectory(prefix="wos-timer-trace-") as temporary:
    out = Path(os.environ.get("WOS_TIMER_TRACE_ARTIFACT_DIR", temporary))
    out.mkdir(parents=True, exist_ok=True)
    variants = [
        ("actual", False, None, None),
        ("reader_collision", True, None, None),
        ("old_snapshot_capacity", False,
         "SCHEDULER_TIMER_TRACE_SNAPSHOT_EVENTS = 64;", "SCHEDULER_TIMER_TRACE_SNAPSHOT_EVENTS = 32;"),
        ("overwrite_busy", True, "(observed & BUSY) != 0 ||", "false ||"),
        ("ignore_missing", True, "omitted == 0 && missing == 0", "omitted == 0"),
        ("early_publication", True,
         "        slot.at_us.store(event.at_us, std::memory_order_release);",
         "        slot.stamp.store(reservation.sequence, std::memory_order_release);\n"
         "        std::this_thread::yield();\n"
         "        slot.at_us.store(event.at_us, std::memory_order_release);"),
        ("omit_final_generation", True,
         "            if (slot.stamp.load(std::memory_order_acquire) != seq) {\n"
         "                ++result.missing;\n                continue;\n            }\n"
         "            result.events.at(result.count++) = EVENT;",
         "            result.events.at(result.count++) = EVENT;"),
    ]
    for name, hook, old, new in variants:
        candidate = header
        if old is not None:
            assert candidate.count(old) == 1, name
            candidate = candidate.replace(old, new)
        if hook:
            anchor = "            result.events.at(result.count++) = EVENT;"
            # Put the observer before final generation validation, including
            # when the final-validation mutant removes that validation.
            at = candidate.index("            };\n", candidate.index("SchedulerTimerTraceEvent const EVENT")) + len("            };\n")
            candidate = candidate[:at] + "            ::trace_reader_hook();\n" + candidate[at:]
        code = prefix + f"constexpr unsigned TRACE_CAPACITY={capacity};\n" + candidate + seams + functions + tests
        cpp, exe = out / f"{name}.cpp", out / name
        cpp.write_text(code)
        built = subprocess.run([compiler, "-std=c++23", "-O1", "-g", "-pthread",
                                "-fsanitize=address,undefined", "-fno-sanitize-recover=all",
                                f"-DREADER_HOOK={int(hook)}", str(cpp), "-o", str(exe)],
                               capture_output=True, text=True)
        (out / f"{name}-build.log").write_text(built.stdout + built.stderr)
        assert built.returncode == 0, built.stdout + built.stderr
        run = subprocess.run([str(exe)], capture_output=True, text=True, timeout=30)
        (out / f"{name}-run.log").write_text(run.stdout + run.stderr)
        if old is None:
            assert run.returncode == 0, run.stdout + run.stderr
            print(run.stdout, end="")
        else:
            assert run.returncode != 0, f"surviving semantic mutant {name}"
            print(f"Compiled semantic mutant {name} rejected")
