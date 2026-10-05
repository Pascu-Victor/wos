"""Run actual batching, NAPI and backlog worker bodies with privileged seams.

Runnable publication, migration ownership and lost-wake tokens remain covered by
the existing scheduler/NAPI tests. This verifies caller-owned batching and its
flush boundary; it does not emulate hardware IRQ delivery or timing.
"""
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
scheduler = (ROOT / "modules/kern/src/platform/sched/scheduler.cpp").read_text()
netpoll = (ROOT / "modules/kern/src/net/netpoll.cpp").read_text()
backlog = (ROOT / "modules/kern/src/net/backlog.cpp").read_text()


def function(source, name):
    match = re.search(rf"^[^\n;]*\b{re.escape(name)}\([^;{{]*\{{", source, re.M)
    assert match, name
    pos, depth = source.index("{", match.start()) + 1, 1
    while depth:
        depth += (source[pos] == "{") - (source[pos] == "}")
        pos += 1
    return source[match.start():pos]


prefix = r'''
#include <array>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <net/netpoll.hpp>
#include <net/backlog.hpp>
#include <platform/sched/local_wake_batch.hpp>
#include <platform/sched/event_wake_diagnostics.hpp>
struct Finish {};
namespace ker::mod::sched {
namespace task {
enum class TaskType {DAEMON,PROCESS,IDLE};
struct Task {
 TaskType type=TaskType::DAEMON;uint64_t cpu=0;
 uint32_t local_wake_batch_depth=0;bool local_wake_batch_pending=false;
 bool blocked=false,wants_block=false;unsigned guards=0;
 bool is_voluntary_blocked()const{return blocked;}
};
}
task::Task a,b;task::Task* current=&a;
bool irq_enabled=true,reentrant=false,in_poll=false;
unsigned requests=0,ipis=0,batched=0,clock_reads=0,poll_calls=0,yields=0;
unsigned backlog_calls=0,backlog_sleeps=0;
bool allow_backlog_sleep=false,require_one_packet_flush=false;
uint64_t last_ipi=UINT64_MAX;
task::Task* current_task_for_preempt(){return current;}
bool interrupts_enabled(){return irq_enabled;}
[[noreturn]] void hcf(){std::abort();}
namespace cpu {uint64_t current_cpu(){return current?current->cpu:0;}}
namespace time {uint64_t get_us(){++clock_reads;return 1000;}}
MigrationGuard::MigrationGuard():task_(current){if(task_)++task_->guards;}
MigrationGuard::~MigrationGuard(){if(task_){assert(task_==current);--task_->guards;}}
void record_scheduler_timer_trace(SchedulerTimerTraceKind k,uint64_t,uint32_t){
 if(k==SchedulerTimerTraceKind::LOCAL_BATCHED)++batched;
}
enum class WakeCpuMode {COALESCE,FORCE};
void event_wake_poke(uint64_t,WakeCpuMode,EventWakeDiagnostics*);
void request_local_reschedule(){
 ++requests;
 if(reentrant){reentrant=false;event_wake_poke(0,WakeCpuMode::COALESCE,nullptr);}
}
void wake_cpu(uint64_t target,WakeCpuMode){assert(target!=cpu::current_cpu());++ipis;last_ipi=target;}
// Namespace continues with the extracted production bodies.
'''
bodies = "\n".join(function(scheduler, n) for n in (
    "LocalWakeBatch::LocalWakeBatch", "LocalWakeBatch::~LocalWakeBatch",
    "event_wake_clock", "batch_local_event_wake", "event_wake_poke"))
worker_seams = r'''
void kern_block(){assert(current->local_wake_batch_depth==0&&!current->local_wake_batch_pending);throw Finish{};}
void kern_yield(){assert(current->local_wake_batch_depth==0&&!current->local_wake_batch_pending);++yields;throw Finish{};}
void kern_sleep_us(uint64_t us){
 assert(us==50&&current->local_wake_batch_depth==0&&!current->local_wake_batch_pending);
 if(allow_backlog_sleep){++backlog_sleeps;if(require_one_packet_flush)assert(requests==backlog_calls);return;}
 throw Finish{};
}
} // namespace ker::mod::sched
namespace ker::net {
struct NetDevice{std::array<char,16> name{};};
struct log{template<class...T>static void critical(const char*,T...){assert(false);}};
bool poll_fn_is_valid(NapiPollFn fn){return fn!=nullptr;}
'''
for name in ("NAPI_WORKER_MAX_FULL_BUDGET_POLLS", "NAPI_WORKER_CONTENTION_SLEEP_US"):
    worker_seams += re.search(rf"^constexpr \w+ {name} = [^;]+;", netpoll, re.M)[0] + "\n"
worker = "\n".join(function(netpoll, n) for n in (
    "napi_diag_add", "napi_diag_poll", "napi_worker_has_work", "napi_worker_loop"))
backlog_seams = r'''
struct PacketBuffer {
 PacketBuffer* next=nullptr;bool normal=false;unsigned expected_depth=1;
 bool free_on_dispatch=false,wait_timer=false,irq_wake=false,force_wake=false,remote_wake=false;
};
BacklogQueue* queues=nullptr;uint64_t num_cpus=1;
enum class BacklogConsumerStage{EMPTY,NORMAL};
void mark_consumer_stage(BacklogQueue&,BacklogConsumerStage){}
void mark_consumer_batch(BacklogQueue&,uint64_t,PacketBuffer*,PacketBuffer*){}
bool try_acquire_backlog_consumer(BacklogQueue& q){bool expected=false;return q.consumer_active.compare_exchange_strong(expected,true);}
void release_backlog_consumer(BacklogQueue& q){q.consumer_active.store(false);}
int prepare_backlog_batch(PacketBuffer* batch,BacklogQueue& q,PacketBuffer*& normal,PacketBuffer*& wki){
 PacketBuffer* normal_tail=nullptr;PacketBuffer* wki_tail=nullptr;int count=0;
 while(batch){auto* next=batch->next;batch->next=nullptr;++count;
  auto*& head=batch->normal?normal:wki;auto*& tail=batch->normal?normal_tail:wki_tail;
  if(tail)tail->next=batch;else head=batch;tail=batch;batch=next;
 }
 q.depth.fetch_sub(count);return count;
}
// Protocol delivery and packet disposal are seams; scheduling calls below use
// the extracted production batching decision. Consumer logic is unchanged and
// independently checked by the existing queue/lost-wake source tests.
void process_backlog_packet(PacketBuffer* packet){
 using namespace ker::mod::sched;
 assert(packet->next==nullptr&&current->local_wake_batch_depth==packet->expected_depth);
 ++backlog_calls;unsigned before=requests;
 for(unsigned i=0;i<3;++i)event_wake_poke(0,WakeCpuMode::COALESCE,nullptr);
 assert(requests==before+(packet->expected_depth?0U:3U));
 if(packet->wait_timer){
  request_local_reschedule(); // Explicit park/timer API bypasses event batching.
  current->wants_block=true;event_wake_poke(0,WakeCpuMode::COALESCE,nullptr);current->wants_block=false;
 }
 if(packet->irq_wake){bool prior=irq_enabled;irq_enabled=false;event_wake_poke(0,WakeCpuMode::COALESCE,nullptr);irq_enabled=prior;}
 if(packet->force_wake)event_wake_poke(0,WakeCpuMode::FORCE,nullptr);
 if(packet->remote_wake)event_wake_poke(3,WakeCpuMode::COALESCE,nullptr);
 if(packet->free_on_dispatch)delete packet;
}
'''
for name in ("BACKLOG_CONSUMER_CONTENTION_SLEEP_US", "BACKLOG_HANDLER_COOPERATIVE_BATCH",
             "BACKLOG_HANDLER_COOPERATIVE_SLEEP_US"):
    backlog_seams += re.search(rf"^constexpr \w+ {name} = [^;]+;", backlog, re.M)[0] + "\n"
backlog_bodies = "\n".join(function(backlog, name) for name in (
    "process_packet_list", "drain_backlog_queue_inline", "backlog_handler_loop"))
tests = r'''
} // namespace ker::net
using namespace ker::mod::sched;
unsigned cases=0;
void reset(){
 assert(a.guards==0&&b.guards==0);
 a={};b={};current=&a;irq_enabled=true;reentrant=in_poll=false;
 requests=ipis=batched=clock_reads=poll_calls=yields=0;last_ipi=UINT64_MAX;
 backlog_calls=backlog_sleeps=0;allow_backlog_sleep=require_one_packet_flush=false;
}
void poke(uint64_t cpu=0,WakeCpuMode mode=WakeCpuMode::COALESCE){event_wake_poke(cpu,mode,nullptr);}
bool full_budget=false;
int poll(ker::net::NapiStruct* napi,int budget,ker::net::NapiPollSource source){
 assert(source==ker::net::NapiPollSource::WORKER&&budget==64);
 assert(a.local_wake_batch_depth==1&&requests==poll_calls);
 in_poll=true;++poll_calls;
 poke();poke();poke();
 assert(requests==poll_calls-1&&a.local_wake_batch_pending);
 in_poll=false;
 if(!full_budget)napi->state.store(ker::net::NapiState::IDLE);
 return full_budget?budget:1;
}
void run_worker(ker::net::NapiStruct& napi){
 try{ker::net::napi_worker_loop(&napi);}catch(Finish const&){}
 assert(a.local_wake_batch_depth==0&&!a.local_wake_batch_pending&&a.guards==0);
}
void run_backlog_worker(ker::net::BacklogQueue& queue){
 ker::net::queues=&queue;
 try{ker::net::backlog_handler_loop(0);}catch(Finish const&){}
 assert(a.local_wake_batch_depth==0&&!a.local_wake_batch_pending&&a.guards==0);
}
void backlog_cases(){
 using namespace ker::net;
 reset();process_packet_list(nullptr,true,true);assert(requests==0&&a.guards==0);++cases;
 reset();PacketBuffer wki;BacklogQueue wki_queue;wki_queue.head.store(&wki);wki_queue.depth.store(1);
 run_backlog_worker(wki_queue);assert(backlog_calls==1&&requests==1&&batched==3&&!wki_queue.handler_active.load());++cases;
 reset();PacketBuffer normal;normal.normal=true;normal.expected_depth=0;
 BacklogQueue normal_queue;normal_queue.head.store(&normal);normal_queue.depth.store(1);
 run_backlog_worker(normal_queue);assert(backlog_calls==1&&requests==3&&batched==0);++cases;
 reset();std::array<PacketBuffer,3> mixed{};mixed[0].normal=true;mixed[0].expected_depth=0;
 for(unsigned i=0;i+1<mixed.size();++i)mixed[i].next=&mixed[i+1];
 BacklogQueue mixed_queue;mixed_queue.head.store(mixed.data());mixed_queue.depth.store(mixed.size());
 run_backlog_worker(mixed_queue);assert(backlog_calls==3&&requests==5&&batched==6&&mixed_queue.depth.load()==0);++cases;
 reset();std::array<PacketBuffer,2> inline_packets{};inline_packets[0].next=&inline_packets[1];
 for(auto& packet:inline_packets)packet.expected_depth=0;
 BacklogQueue inline_queue;inline_queue.head.store(inline_packets.data());inline_queue.depth.store(2);
 assert(drain_backlog_queue_inline(inline_queue,false)==2);
 assert(backlog_calls==2&&requests==6&&batched==0&&!inline_queue.consumer_active.load());++cases;
 for(unsigned count:{128U,129U,257U}){
  reset();std::array<PacketBuffer,257> packets{};
  for(unsigned i=0;i+1<count;++i)packets[i].next=&packets[i+1];
  allow_backlog_sleep=require_one_packet_flush=true;process_packet_list(packets.data(),true,true);
  assert(backlog_calls==count&&requests==count&&batched==3*count&&backlog_sleeps==(count-1)/128);
  assert(a.local_wake_batch_depth==0&&!a.local_wake_batch_pending&&a.guards==0);++cases;
 }
 reset();auto* owned_first=new PacketBuffer;auto* owned_second=new PacketBuffer;
 owned_first->next=owned_second;owned_first->free_on_dispatch=owned_second->free_on_dispatch=true;
 process_packet_list(owned_first,true,true);assert(backlog_calls==2&&requests==2&&a.guards==0);++cases;
 reset();PacketBuffer indirect;indirect.wait_timer=true;process_packet_list(&indirect,true,true);
 assert(requests==3&&batched==3&&!a.local_wake_batch_pending);++cases;
 reset();PacketBuffer bypass;bypass.irq_wake=bypass.force_wake=bypass.remote_wake=true;
 process_packet_list(&bypass,true,true);assert(requests==3&&ipis==1&&batched==3);++cases;
 reset();irq_enabled=false;PacketBuffer irq;irq.expected_depth=0;process_packet_list(&irq,true,true);
 assert(requests==3&&batched==0&&!irq_enabled&&a.guards==0);++cases;
 reset();a.type=task::TaskType::PROCESS;PacketBuffer process;process.expected_depth=0;
 process_packet_list(&process,true,true);assert(requests==3&&batched==0&&a.guards==0);++cases;
 reset();PacketBuffer nested;nested.expected_depth=2;{LocalWakeBatch outer;
  process_packet_list(&nested,true,true);assert(requests==0&&a.local_wake_batch_depth==1);
 }assert(requests==1&&!a.local_wake_batch_pending&&a.guards==0);++cases;
 reset();BacklogQueue empty;run_backlog_worker(empty);assert(backlog_calls==0&&requests==0);++cases;
 reset();BacklogQueue contended;contended.consumer_active.store(true);run_backlog_worker(contended);
 assert(backlog_calls==0&&requests==0);++cases;
 reset();BacklogQueue inline_contended;inline_contended.consumer_active.store(true);
 assert(drain_backlog_queue_inline(inline_contended,false)==0&&backlog_calls==0&&requests==0);++cases;
}
int main(){
 reset();{LocalWakeBatch batch;}assert(requests==0&&clock_reads==0);++cases;
 reset();{LocalWakeBatch batch;poke();poke();poke();assert(requests==0&&batched==3);}
 assert(requests==1&&a.local_wake_batch_depth==0&&!a.local_wake_batch_pending);++cases;
 reset();{LocalWakeBatch outer;poke();{LocalWakeBatch inner;poke();assert(a.local_wake_batch_depth==2);}
  assert(requests==0&&a.local_wake_batch_depth==1);}
 assert(requests==1);++cases;
 reset();{LocalWakeBatch batch;poke(3);poke();poke(2,WakeCpuMode::FORCE);assert(ipis==2&&last_ipi==2&&requests==0);}
 assert(requests==1);++cases;
 reset();{LocalWakeBatch batch;poke(0,WakeCpuMode::FORCE);assert(requests==1&&!a.local_wake_batch_pending);}
 assert(requests==1);++cases;
 reset();{LocalWakeBatch batch;irq_enabled=false;poke();assert(requests==1&&!a.local_wake_batch_pending);irq_enabled=true;}
 assert(requests==1);++cases;
 for(bool wants:{false,true}){reset();{LocalWakeBatch batch;a.wants_block=wants;a.blocked=!wants;poke();
  assert(requests==1&&!a.local_wake_batch_pending);a.wants_block=a.blocked=false;}assert(requests==1);++cases;}
 for(auto type:{task::TaskType::PROCESS,task::TaskType::IDLE}){reset();a.type=type;{LocalWakeBatch batch;poke();
  assert(a.local_wake_batch_depth==0&&requests==1);}++cases;}
 reset();irq_enabled=false;{LocalWakeBatch batch;poke();assert(a.local_wake_batch_depth==0&&requests==1);}
 assert(!irq_enabled);++cases;irq_enabled=true;
 reset();{LocalWakeBatch batch;poke();current=&b;poke();assert(requests==1&&!b.local_wake_batch_pending);current=&a;}
 assert(requests==2&&!a.local_wake_batch_pending);++cases;
 reset();{LocalWakeBatch batch;poke();request_local_reschedule();assert(requests==1);}
 assert(requests==2);++cases; // Explicit kernel park/timer rechecks bypass batching.
 reset();{LocalWakeBatch batch;poke();irq_enabled=false;}
 assert(requests==1&&!irq_enabled&&!a.local_wake_batch_pending);irq_enabled=true;++cases;
 reset();{LocalWakeBatch batch;poke();reentrant=true;}
 assert(requests==2&&!a.local_wake_batch_pending);++cases;
 reset();current=nullptr;{LocalWakeBatch batch;poke();}assert(requests==1);current=&a;++cases;
 reset();{LocalWakeBatch batch;EventWakeDiagnostics d;event_wake_poke(0,WakeCpuMode::COALESCE,&d);
  assert(d.pokes==1&&d.poke_us==0&&clock_reads==2&&requests==0);}
 assert(requests==1);++cases;
 for(bool full:{false,true}){
  reset();full_budget=full;ker::net::NapiStruct napi{};napi.poll=poll;napi.weight=64;
  napi.state.store(ker::net::NapiState::SCHEDULED);napi.has_work.store(true);run_worker(napi);
  assert(poll_calls==(full?8U:1U)&&requests==poll_calls&&batched==3*poll_calls);
  assert(napi.poll_count.load()==poll_calls&&yields==unsigned(full));++cases;
 }
 reset();ker::net::NapiStruct empty{};empty.state.store(ker::net::NapiState::IDLE);run_worker(empty);
 assert(requests==0&&poll_calls==0);++cases;
 reset();ker::net::NapiStruct contended{};contended.state.store(ker::net::NapiState::POLLING);
 contended.has_work.store(true);run_worker(contended);assert(requests==0&&poll_calls==0);++cases;
 backlog_cases();
 printf("%u actual batch/NAPI/backlog cases PASS; per-packet flush, worker WKI admission, inline/normal bypass, packet disposal,128packet sleeps and8poll budget\n",cases);
}
'''
compiler = os.environ.get("WOS_HOST_CXX") or shutil.which("clang++-23") or "/usr/bin/clang++"
with tempfile.TemporaryDirectory(prefix="wos-local-wake-batch-") as temporary:
    out = Path(os.environ.get("WOS_LOCAL_WAKE_BATCH_ARTIFACT_DIR", temporary))
    out.mkdir(parents=True, exist_ok=True)
    code = prefix + bodies + worker_seams + worker + backlog_seams + backlog_bodies + tests
    flush = "        task_->local_wake_batch_pending = false;\n        request_local_reschedule();"
    packet_list = function(backlog, "process_packet_list")
    whole_list = packet_list.replace(
        "    uint32_t processed_since_sleep = 0;",
        "    std::optional<ker::mod::sched::LocalWakeBatch> whole_batch;\n"
        "    if (batch_local_wakes) whole_batch.emplace();\n"
        "    uint32_t processed_since_sleep = 0;")
    whole_list = whole_list.replace("            ker::mod::sched::LocalWakeBatch const PACKET_WAKE_BATCH;", "")
    variants = [("actual", None, None),
                ("missing_flush", flush, "        task_->local_wake_batch_pending = false;"),
                ("ignored_irq_bypass", "if (!interrupts_enabled()) {", "if (false) {"),
                ("missing_worker_batch", "ker::mod::sched::LocalWakeBatch const WAKE_BATCH;", ""),
                ("missing_packet_batch", "ker::mod::sched::LocalWakeBatch const PACKET_WAKE_BATCH;", ""),
                ("missing_backlog_admission", "process_packet_list(wki_head, true, true)", "process_packet_list(wki_head, true, false)"),
                ("batched_normal_admission", "process_packet_list(normal_head, true)", "process_packet_list(normal_head, true, true)"),
                ("batched_inline_admission", "process_packet_list(wki_head, cooperative)", "process_packet_list(wki_head, cooperative, true)"),
                ("changed_cooperative_budget", "BACKLOG_HANDLER_COOPERATIVE_BATCH = 128", "BACKLOG_HANDLER_COOPERATIVE_BATCH = 129"),
                ("whole_list_batch", packet_list, whole_list)]
    for name, old, new in variants:
        candidate = code
        if old is not None:
            assert candidate.count(old) == 1, name
            candidate = candidate.replace(old, new)
        cpp, exe = out / f"{name}.cpp", out / name
        cpp.write_text(candidate)
        built = subprocess.run([compiler, "-std=c++23", "-O1", "-g",
                                "-fsanitize=address,undefined", "-fno-sanitize-recover=all",
                                "-I", str(ROOT / "modules/kern/src"), str(cpp), "-o", str(exe)],
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
