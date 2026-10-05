#!/usr/bin/env python3
"""Exercise the production counter snapshot bodies with controlled CPU endpoints."""
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
source = Path(os.environ.get('WOS_CPU_SNAPSHOT_TEST_SOURCE',
                             ROOT / 'modules/kern/src/platform/sched/scheduler.cpp')).read_text()


def function(name):
    start = source.index(name)
    left = source.index('{', start)
    depth = 0
    for end in range(left, len(source)):
        depth += (source[end] == '{') - (source[end] == '}')
        if depth == 0:
            return source[start:end + 1]
    raise AssertionError(name)


snapshot = function('auto cpu_accounting_snapshot_locked(')
committed = (function('auto get_cpu_accounting_committed_snapshot(')
             if 'auto get_cpu_accounting_committed_snapshot(' in source else
             # Faithful old export: all callers received the provisional slice.
             'auto get_cpu_accounting_committed_snapshot(uint64_t cpu) -> CpuAccountingSnapshot '
             '{return get_cpu_accounting_snapshot(cpu);}')
code = r'''
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
namespace fixture {
namespace task {
enum class TaskType {PROCESS, IDLE, DAEMON};
struct Task {
 TaskType type=TaskType::PROCESS; int sched_nice=0; bool blocked=false;
 struct {struct {uint64_t cs=3;} frame;} context;
 bool is_voluntary_blocked() const {return blocked;}
};
}
namespace desc::gdt {constexpr uint64_t GDT_USER_CS=3;}
struct CpuAccountingSnapshot {uint64_t user_us,nice_us,system_us,idle_us,iowait_us,irq_us,softirq_us,steal_us;};
struct RunQueue {
 std::atomic<uint64_t> cpu_user_us{0},cpu_nice_us{0},cpu_system_us{0},cpu_idle_us{0},cpu_iowait_us{0},cpu_irq_us{0},cpu_softirq_us{0},cpu_steal_us{0};
 uint64_t last_tick_us=1000; task::Task* current_task=nullptr;
};
struct Queues {
 RunQueue rq; unsigned locks=0;
 template<class F> auto with_lock(uint64_t cpu,F f){assert(cpu==0);++locks;return f(&rq);}
};
Queues* run_queues=nullptr;
unsigned clock_reads=0;
namespace time {uint64_t get_us(){++clock_reads;return 11000;}}
''' + '\n'.join(function(n) for n in (
    'void account_cpu_runtime_delta(',
    'void add_runtime_delta_to_cpu_snapshot(',
    'auto task_in_kernel_mode_for_cpu_snapshot(')) + '\n' + snapshot + '\n' + function(
        'auto get_cpu_accounting_snapshot(') + '\n' + committed + r'''
}
using namespace fixture;
uint64_t total(CpuAccountingSnapshot s){return s.user_us+s.nice_us+s.system_us+s.idle_us+s.iowait_us+s.irq_us+s.softirq_us+s.steal_us;}
void checked_zero(CpuAccountingSnapshot s){assert(total(s)==0);}
int main(){
 checked_zero(get_cpu_accounting_committed_snapshot(0));assert(clock_reads==0);
 Queues queues;run_queues=&queues;auto& rq=queues.rq;task::Task process;rq.current_task=&process;
 auto before=get_cpu_accounting_committed_snapshot(0);
 // A saved user frame cannot publish this uncommitted kernel interval as user.
 checked_zero(before);assert(clock_reads==0 && queues.locks==1);
 auto live=get_cpu_accounting_snapshot(0);assert(live.user_us==10000 && clock_reads==1);
 account_cpu_runtime_delta(&rq,&process,10000,true,true);rq.last_tick_us=11000;
 auto after=get_cpu_accounting_committed_snapshot(0);
 assert(after.user_us==0 && after.system_us==10000 && total(after)==10000);
 // Already charged IRQ time must not be published again in a speculative category.
 rq.last_tick_us=1000;rq.cpu_irq_us=5000;
 before=get_cpu_accounting_committed_snapshot(0);assert(before.user_us==0 && before.irq_us==5000 && total(before)==15000);
 account_cpu_runtime_delta(&rq,&process,5000,false,true);rq.last_tick_us=11000;
 after=get_cpu_accounting_committed_snapshot(0);
 assert(after.user_us==5000 && after.irq_us==5000 && after.system_us==10000 && total(after)==20000);
 assert(after.user_us>=before.user_us && after.irq_us>=before.irq_us && after.system_us>=before.system_us);
 // Tickless idle remains committed-only; read must not flush or advance the ledger.
 task::Task idle;idle.type=task::TaskType::IDLE;rq.current_task=&idle;rq.last_tick_us=1000;
 before=get_cpu_accounting_committed_snapshot(0);assert(before.idle_us==0 && rq.last_tick_us==1000);
 live=get_cpu_accounting_snapshot(0);assert(live.idle_us==10000);
 account_cpu_runtime_delta(&rq,&idle,10000,true,false);rq.last_tick_us=11000;
 after=get_cpu_accounting_committed_snapshot(0);assert(after.idle_us==10000 && after.user_us==before.user_us);
 // All cumulative fields survive as exact microseconds, independent of task state.
 process.sched_nice=1;account_cpu_runtime_delta(&rq,&process,37,false,true);
 rq.cpu_iowait_us=13;rq.cpu_softirq_us=17;rq.cpu_steal_us=19;rq.current_task=nullptr;
 after=get_cpu_accounting_committed_snapshot(0);
 assert(after.nice_us==37 && after.iowait_us==13 && after.softirq_us==17 && after.steal_us==19);
 assert(clock_reads==2 && rq.last_tick_us==11000);
 checked_zero(cpu_accounting_snapshot_locked(nullptr,11000));
 puts("actual committed CPU counters / reclassification / IRQ / idle lag / unchanged live estimates PASS");
}
'''
with tempfile.TemporaryDirectory(prefix='wos-cpu-snapshot-') as td:
    folder = Path(td)
    (folder / 'check.cpp').write_text(code)
    subprocess.run(['/usr/bin/clang++', '-std=c++23', '-O1', '-g',
                    '-fsanitize=address,undefined', '-fno-sanitize-recover=all',
                    str(folder / 'check.cpp'), '-o', str(folder / 'check')], check=True)
    subprocess.run([str(folder / 'check')], check=True,
                   env={**os.environ, 'ASAN_OPTIONS': 'detect_leaks=0:halt_on_error=1',
                        'UBSAN_OPTIONS': 'halt_on_error=1'})
