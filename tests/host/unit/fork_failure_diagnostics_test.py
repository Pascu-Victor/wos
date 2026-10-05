#!/usr/bin/env python3
"""Execute actual fork error diagnostics with lock, queue and journal seams."""
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

from exec_trace_ownership_test import block, function

ROOT = Path(__file__).resolve().parents[3]
STAGES = (
    'pressure-admission', 'kernel-stack', 'task', 'vfs-rules', 'credentials',
    'pagemap', 'vm-snapshot', 'thread', 'fd-table', 'child-publication',
    'scheduler-publication',
)
PRELUDE = r'''
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <limits>
#include <string>
#include <vector>
std::vector<std::string> calls;
bool held=false;
unsigned lock_count=0, logs=0, ends=0;
uint64_t recorded_bytes=0;
int32_t recorded_status=0;
std::string message;
struct Lock {
 uint64_t lock_irqsave() { assert(!held); held=true; ++lock_count;
  calls.emplace_back("pool-lock"); return 0x202; }
 void unlock_irqrestore(uint64_t flags) { assert(held && flags==0x202);
  held=false; calls.emplace_back("pool-unlock"); }
};
namespace ker::mod::mm::phys {
SNAPSHOT
struct Pool { Lock lock; size_t capacity=0, free_count=0, high_watermark=0; } kernel_stack_pool;
GETTER
uint64_t free_pages=123456;
uint64_t get_free_mem_pages() { assert(!held); calls.emplace_back("physical"); return free_pages; }
}
namespace ker::mod::cpu { uint32_t id=7; uint32_t current_cpu() { return id; } }
namespace ker::mod::smt { uint64_t cores=4;
 uint64_t get_core_count() { assert(!held); calls.emplace_back("cores"); return cores; } }
namespace ker::mod::sched::task { struct Task { uint64_t pid=0x200000007ULL; }; }
namespace ker::mod::sched {
struct RunQueueStats { uint64_t expired_task_count, active_task_count, wait_queue_count; };
RunQueueStats queues[8]{};
RunQueueStats get_run_queue_stats(uint64_t cpu) { assert(!held && cpu<ker::mod::smt::cores);
 calls.emplace_back("rq"+std::to_string(cpu)); return queues[cpu]; }
struct SchedulerTraceStats { uint64_t gc_deferred_depth; };
uint64_t deferred=9;
SchedulerTraceStats get_scheduler_trace_stats(uint64_t cpu) {
 assert(!held && cpu==0 && ker::mod::smt::cores>0); calls.emplace_back("deferred"); return {deferred}; }
}
namespace ker::mod::perf { enum class WkiPerfLocalProcOp { FORK }; }
#define WOS_PERF_CALLSITE() 123ULL
namespace ker::syscall::process {
struct process_log {
 static void warn(const char* format, ...) {
  assert(!held); ++logs; calls.emplace_back("journal");
  char data[512]; va_list args; va_start(args,format);
  int length=vsnprintf(data,sizeof(data),format,args); va_end(args);
  assert(length>=0 && static_cast<size_t>(length)<sizeof(data)); message=data;
 }
};
DIAGNOSTIC
void end_local_proc_stage(ker::mod::sched::task::Task*, ker::mod::perf::WkiPerfLocalProcOp,
 int&, int32_t status, uint64_t bytes, uint64_t callsite) {
 assert(!held && callsite==123); ++ends; calls.emplace_back("end");
 recorded_bytes=bytes; recorded_status=status;
}
uint64_t finish(ker::mod::sched::task::Task* parent, int64_t result, uint64_t bytes, const char* stage) {
 using namespace ker::mod; int FORK_STAGE=0;
LAMBDA;
 return finish_fork(result,bytes,stage);
}
}
'''
CASES = r'''
int main() {
 using namespace ker::mod; using ker::syscall::process::finish;
 auto& pool=mm::phys::kernel_stack_pool; sched::task::Task parent;
 // Getter copies one coherent metadata tuple without allocation or logger work.
 for (uint64_t free : {0ULL, 1ULL, 256ULL}) {
  pool.capacity=256; pool.free_count=free; pool.high_watermark=256-free;
  calls.clear(); auto s=mm::phys::get_kernel_stack_pool_stats();
  assert(s.capacity==256 && s.free_slots==free && s.high_watermark==256-free);
  assert((calls==std::vector<std::string>{"pool-lock","pool-unlock"}) && !held && logs==0);
 }
 // Successful and unlabelled/other failures preserve errno/bytes and do no observation.
 for (int64_t result : {0LL, 0x200000007LL, -ENOMEM*1LL, -EAGAIN*1LL}) {
  calls.clear(); auto before=lock_count;
  const char* label=result==-ENOMEM ? nullptr : "kernel-stack";
  assert(finish(&parent,result,998877,label)==static_cast<uint64_t>(result));
  assert(recorded_bytes==998877 && recorded_status==(result<0?result:0));
  assert((calls==std::vector<std::string>{"end"}) && lock_count==before && logs==0);
 }
 for (uint64_t cpu=0;cpu<4;++cpu) sched::queues[cpu]={cpu+1,2*cpu+1,3*cpu+1};
 pool.capacity=256; pool.free_count=0; pool.high_watermark=256;
 const char* stages[]={STAGE_STRINGS};
 for (const char* stage : stages) {
  calls.clear(); auto before=logs;
  assert(finish(&parent,-ENOMEM,42,stage)==static_cast<uint64_t>(-ENOMEM));
  assert(recorded_bytes==42 && recorded_status==-ENOMEM && logs==before+1);
  assert((calls==std::vector<std::string>{"end","pool-lock","pool-unlock","physical","cores",
   "rq0","rq1","rq2","rq3","deferred","journal"}));
  assert(message.find(std::string("stage=")+stage+" status=-12 parent=8589934599")!=std::string::npos);
  assert(message.find("stack_cap=256 stack_free=0 stack_high=256")!=std::string::npos);
  assert(message.find("phys_free_pages=123456 dead=10 runnable=16 waiting=22 gc_deferred=9")!=std::string::npos);
 }
 // A zero-core snapshot skips queue access; no GC request/reclaim is a dependency.
 calls.clear(); smt::cores=0; finish(&parent,-ENOMEM,0,"task");
 assert((calls==std::vector<std::string>{"end","pool-lock","pool-unlock","physical","cores","journal"}));
 assert(message.find("dead=0 runnable=0 waiting=0 gc_deferred=0")!=std::string::npos);
 // Maximum-width values and the longest stage fit the actual journal message bound.
 smt::cores=1; auto max=std::numeric_limits<uint64_t>::max(); parent.pid=max;
 cpu::id=std::numeric_limits<uint32_t>::max(); pool.capacity=max; pool.free_count=max;
 pool.high_watermark=max; mm::phys::free_pages=max; sched::queues[0]={max,max,max}; sched::deferred=max;
 finish(&parent,-ENOMEM,0,"scheduler-publication");
 assert(message.size()<512 && message.ends_with("gc_deferred=18446744073709551615"));
 puts("PASS actual fork failure getter/helper/finish: 11 stages, errno/bytes, success silence, lock ordering, CPU bounds, 512-byte journal bound");
}
'''


def main():
    process = (ROOT / 'modules/kern/src/syscalls_impl/process/process.cpp').read_text()
    fork = function(process, 'wos_proc_fork')
    labels = re.findall(r'finish_fork\(-ENOMEM, 0, "([^"]+)"\)', fork)
    assert tuple(labels) == STAGES
    assert fork.count('finish_fork(-ENOMEM') == len(STAGES)
    snapshot = block((ROOT / 'modules/kern/src/platform/mm/phys.hpp').read_text(),
                     r'^struct KernelStackPoolStatsSnapshot \{') + ';'
    getter = function((ROOT / 'modules/kern/src/platform/mm/phys.opt.cpp').read_text(),
                      'get_kernel_stack_pool_stats')
    source = PRELUDE.replace('SNAPSHOT', snapshot).replace('GETTER', getter)
    source = source.replace('DIAGNOSTIC', function(process, 'note_fork_failure'))
    source = source.replace('LAMBDA', block(fork, r'^    auto finish_fork ='))
    source += CASES.replace('STAGE_STRINGS', ','.join('"' + s + '"' for s in STAGES))
    with tempfile.TemporaryDirectory(prefix='wos-fork-failure-') as directory:
        temp = Path(directory)
        (temp / 'test.cpp').write_text(source)
        subprocess.run([shutil.which('c++', path=os.defpath), '-std=c++23', '-O1', '-g',
                        '-Wall', '-Wextra', '-Werror', '-fsanitize=address,undefined',
                        str(temp / 'test.cpp'), '-o', str(temp / 'test')], check=True)
        subprocess.run([str(temp / 'test')], check=True)


if __name__ == '__main__':
    main()
