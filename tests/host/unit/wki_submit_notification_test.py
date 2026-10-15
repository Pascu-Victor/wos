"""Actual worker/notifier and full reschedule decisions, controlled host seams.

Four real host threads pause after the actual worker's EMPTY read/unlock. The
heap, CPU/IRQ and logging leaves are seams; this is a notification protocol
counterexample, not a reproduction of the historical VM failure.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

R = Path(__file__).resolve().parents[3]
paths = ['modules/kern/src/net/wki/remote_compute.cpp', 'modules/kern/src/platform/sched/scheduler.cpp', 'modules/kern/src/platform/sched/scheduler.hpp']
source = {p: (R / p).read_text() for p in paths}
def function(text, name):
    m = re.search(rf'^(?:inline )?(?:auto|void) {name}\([^{{;]+\{{', text, re.M)
    assert m, name
    pos, depth = text.index('{', m.start()) + 1, 1
    while depth:
        depth += (text[pos] == '{') - (text[pos] == '}'); pos += 1
    return text[m.start():pos]
sched, header, compute = [source[p] for p in [paths[1], paths[2], paths[0]]]
code = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <utility>
#include <platform/sched/event_wake_diagnostics.hpp>
namespace ker::mod::sched {
namespace time { uint64_t get_us(){return 100;} }
namespace cpu { thread_local uint64_t current=0; uint64_t current_cpu(){return current;} }
namespace smt { uint64_t get_core_count(){return 4;} }
namespace task {
enum class TaskState { ACTIVE,DEAD };
enum class TaskType { DAEMON,PROCESS };
enum class WaitChannelKind { NONE,WKI_EXECVE_PROXY };
struct Task {
 enum class sched_queue { NONE,RUNNABLE,WAITING };
 uint64_t cpu=0,pid=1,wake_at_us=0,last_sleep_start_us=0,wki_proxy_task_id=0;
 uint64_t slice_ns=1000,sched_weight=1,slice_used_ns=0;
 int64_t vruntime=0,vdeadline=0;
 int heap_index=-1;
 TaskType type=TaskType::DAEMON;
 bool cpu_pinned=true,deferred_task_switch=true,wants_block=false,voluntary=false,just_woke=false;
 const char* wait_channel=nullptr;
 WaitChannelKind wait_channel_kind=WaitChannelKind::NONE;
 sched_queue sched_queue=sched_queue::RUNNABLE;
 std::atomic<TaskState> state{TaskState::ACTIVE};
 std::atomic<bool> gc_queued{false},wakeup_pending{false},reschedule_pending{false},reschedule_in_progress{false};
 std::atomic<uint64_t> reschedule_requested_cpu{0};
 uint64_t load_cpu() const noexcept { return cpu; }
 void store_cpu(uint64_t value) noexcept { cpu = value; }
 bool load_cpu_pinned() const noexcept { return cpu_pinned; }
 bool is_voluntary_blocked()const{return voluntary;}
 void set_voluntary_blocked(bool value){voluntary=value;}
 void clear_wait_channel(){wait_channel=nullptr;wait_channel_kind=WaitChannelKind::NONE;}
 bool wait_channel_is(WaitChannelKind kind)const{return wait_channel!=nullptr&&wait_channel_kind==kind;}
 bool has_interrupting_signal_pending()const{return false;}
}; }
thread_local task::Task* subject=nullptr;
task::Task* current_task_for_preempt(){return subject;}
struct Heap {
 task::Task* member=nullptr;
 bool contains(task::Task* t){return member==t;}
 void insert(task::Task* t){assert(member==nullptr);member=t;t->heap_index=0;}
 void erase(task::Task* t){assert(member==t);member=nullptr;t->heap_index=-1;}
};
struct RunQueue {
 std::mutex mutex;
 task::Task* current_task=nullptr;task::Task* handoff_task=nullptr;task::Task* waiting=nullptr;
 Heap runnable_heap;int64_t min_vruntime=0;
 std::atomic<uint64_t> slow_reschedule_scans{0},cpu_irq_us{0},scheduler_timer_interrupts{0};
 SchedulerTimerTrace timer_trace{};
};
struct Queues {
 std::array<RunQueue,4> rows;
 RunQueue* this_cpu(){return &rows.at(cpu::current_cpu());}
 RunQueue* that_cpu(uint64_t c){return &rows.at(c);}
 template<class F>void with_lock_void(uint64_t c,F&& f){std::lock_guard g(rows.at(c).mutex);std::forward<F>(f)(&rows.at(c));}
} storage;
Queues* run_queues=&storage;
constexpr bool K_ENABLE_SCHED_OWNER_SCAN_FALLBACK=false;
enum class MigrationRejectionReason { NONE,NOT_RUNNABLE,WKI_OWNED };
struct MigrationGuardPlacement { bool disabled=false;uint64_t owner_cpu=0;MigrationRejectionReason reason=MigrationRejectionReason::NONE; };
MigrationGuardPlacement migration_guard_placement(task::Task const*,uint64_t){return {};}
void record_migration_rejection(task::Task const*,uint64_t,MigrationRejectionReason){}
void record_migration_outcome(task::Task const*,uint64_t,uint64_t,MigrationRejectionReason){}
uint64_t scheduler_single_process_cpu_target(task::Task const*,uint64_t){return UINT64_MAX;}
uint64_t scheduler_process_owner_cpu_if_migration_disabled(task::Task const*,uint64_t){return UINT64_MAX;}
uint64_t get_least_loaded_cpu(){return 0;}
uint64_t cross_cpu_migration_target_or_source(task::Task const*,uint64_t,uint64_t target,uint64_t){return target;}
bool task_is_wki_migration_owned(task::Task* t){return t->wki_proxy_task_id!=0;}
bool wait_list_contains_locked(RunQueue* q,task::Task* t){return q->waiting==t;}
bool transition_detach_wait_locked(RunQueue* q,task::Task* t){assert(q->waiting==t);q->waiting=nullptr;return true;}
bool transition_detach_runnable_locked(RunQueue* q,task::Task* t){q->runnable_heap.erase(t);return true;}
void repair_stale_wait_membership_locked(RunQueue*,task::Task*){}
void note_wait_deadline_locked(RunQueue*,task::Task*){}
bool process_has_voluntary_kernel_resume_frame(task::Task*){return false;}
bool is_low_latency_handoff_wait_channel(task::WaitChannelKind){return false;}
int64_t compute_wakeup_floor_vruntime(RunQueue*,task::Task*,uint64_t,task::WaitChannelKind){return 0;}
int64_t compute_avg_vruntime(RunQueue*){return 0;}
int64_t vdeadline_from_slice(int64_t v,uint64_t,uint64_t){return v;}
bool publish_runnable_task_locked(RunQueue* q,task::Task* t,const char*){q->runnable_heap.insert(t);t->sched_queue=task::Task::sched_queue::RUNNABLE;return true;}
void note_task_wakeup(task::Task*,uint64_t,task::WaitChannelKind){}
unsigned perf_wake_flags(uint64_t,bool,bool){return 0;}
uint64_t observed_sleep_us(task::Task*,uint64_t){return 0;}
uint64_t perf_wait_callsite(task::Task*){return 0;}
namespace perf {
uint64_t get_num_perf_cpus(){return 4;}
template<class... T>void record_wake(T...){}
}
enum class WakeCpuMode { COALESCE,FORCE };
enum class EventWakeDeferredSwitch { PRESERVE,CANCEL };
bool event_wake_should_cancel_deferred_switch(EventWakeDeferredSwitch d,bool proxy){return d==EventWakeDeferredSwitch::CANCEL&&!proxy;}
void event_wake_poke(uint64_t,WakeCpuMode,EventWakeDiagnostics*){}
bool scheduler_timer_trace_enabled(){return false;}
bool event_wake_prefers_waker_cpu(bool,bool,bool){return false;}
uint64_t event_wake_rebalance_target_cpu(task::Task const*,uint64_t target){return target;}
struct ReservedTaskWake {uint64_t wake_at_us=0;bool was_blocking=false;};
void reschedule_task_for_cpu(uint64_t,task::Task*,EventWakeDiagnostics* = nullptr);
void wake_task_from_event(task::Task*,EventWakeDeferredSwitch = EventWakeDeferredSwitch::PRESERVE,EventWakeDiagnostics* = nullptr);
enum class SchedulerHaltWaitKind { YIELD,BLOCK,PROCESS_PARK };
'''
for name in ['runqueue_task_is_reserved_locked', 'publish_reserved_task_wakeup_locked', 'event_wake_clock', 'queue_reschedule_request', 'consume_reschedule_request', 'retain_or_reacquire_reschedule_leader']:
    code += function(sched, name) + '\n'
code += 'template <typename Dispatch>\n' + function(sched, 'drain_reschedule_leader') + '\n'
code += 'template <typename Func>\n' + function(sched, 'with_event_wake_runqueue_lock') + '\n'
for name in ['reschedule_task_for_cpu_once', 'reschedule_task_for_cpu', 'kern_wake', 'event_wake_target_cpu', 'wake_task_from_event_on_cpu', 'wake_task_from_event']:
    code += function(sched, name) + '\n'
code += function(header, 'scheduler_wait_should_cancel') + '\n'
code += r'''
std::atomic<unsigned> blocked_with_work{0},cancelled{0},handled{0};
struct Stop {};
void kern_block();
}
namespace ker::net::wki {
using Task=ker::mod::sched::task::Task;
std::array<Task,4> tasks;
std::array<Task*,4> s_compute_submit_tasks{&tasks[0],&tasks[1],&tasks[2],&tasks[3]};
std::atomic<size_t> s_compute_submit_task_count{4};
constexpr size_t WKI_COMPUTE_SUBMIT_QUEUE_MAX=8;
struct TaskSubmitPayload {uint32_t task_id;};
struct PendingTaskSubmit {uint16_t src_node=1;uint8_t* payload=nullptr;uint16_t payload_len=0;uint64_t deadline_us=0,queued_at_us=0;unsigned session=0;};
std::array<PendingTaskSubmit,8> g_pending_task_submits{};
size_t s_pending_task_submit_head=0,s_pending_task_submit_tail=0,s_pending_task_submit_count=0;
std::barrier observed_empty(5),resume_workers(5);
bool adversarial=true;
class PendingLock {
 std::mutex mutex;
public:
 void lock(){mutex.lock();}
 void unlock(){
  mutex.unlock();
  static thread_local unsigned unlocks=0;
  if(ker::mod::sched::subject!=nullptr && adversarial && ++unlocks==2){
   observed_empty.arrive_and_wait();resume_workers.arrive_and_wait();
  }
 }
} s_pending_submit_lock;
uint64_t wki_now_us(){return 100;}
uint64_t WOS_PERF_CALLSITE(){return 0;}
namespace dummy_perf { enum class Op { DEFER_WAIT }; }
void handle_task_submit_work(uint16_t,const uint8_t* p,uint16_t n,uint64_t,unsigned){
 assert(n==sizeof(TaskSubmitPayload)&&reinterpret_cast<const TaskSubmitPayload*>(p)->task_id==19);
 ker::mod::sched::handled.fetch_add(1);
}
template<class... T>void perf_record_compute_begin(T...){}
template<class... T>void perf_record_compute_end(T...){}
'''
code += '}\nnamespace ker::mod::perf {enum class WkiPerfComputeOp { DEFER_WAIT };}\nnamespace ker::net::wki {\n'
code += function(compute, 'drain_pending_task_submits') + '\n'
# The semantic mutant removes only the notifier's event-before-park token.
notifier = function(compute, 'wki_remote_compute_notify_pending_submit')
assert notifier.count('ker::mod::sched::wake_task_from_event(') == 1
code += '\n// NOTIFIER_SLOT\n'
worker = function(compute.replace('[[noreturn]] void wki_compute_submit_thread', 'void wki_compute_submit_thread'), 'wki_compute_submit_thread')
code += worker + '\n}\n'
code += r'''
namespace ker::mod::sched {
void kern_block(){
 assert(subject!=nullptr);
 subject->wants_block=true;subject->wait_channel="kern_block";
 // CPU/HLT is a seam; the actual cancellation decision is executed above.
 bool const stopped=scheduler_wait_should_cancel(subject,SchedulerHaltWaitKind::BLOCK);
 if(stopped){++cancelled;subject->wants_block=false;subject->clear_wait_channel();}
 else{
  ker::net::wki::s_pending_submit_lock.lock();
  if(ker::net::wki::s_pending_task_submit_count!=0) ++blocked_with_work;
  ker::net::wki::s_pending_submit_lock.unlock();
 }
 if(!stopped||handled.load()!=0)throw Stop{};
}
}
int main(int argc,char** argv){
 using namespace ker::mod::sched;using namespace ker::net::wki;
 assert(argc==2);int const mode=std::atoi(argv[1]);
 if(mode==4){
  s_compute_submit_task_count.store(0);wki_remote_compute_notify_pending_submit();
  for(auto& t:tasks) assert(!t.wakeup_pending.load());
  s_compute_submit_task_count.store(2);
  for(unsigned i=0;i<4;++i){tasks[i].cpu=i;tasks[i].heap_index=0;storage.rows[i].runnable_heap.member=&tasks[i];storage.rows[i].current_task=&tasks[i];}
  wki_remote_compute_notify_pending_submit();
  assert(tasks[0].wakeup_pending.load()&&tasks[1].wakeup_pending.load());
  assert(!tasks[2].wakeup_pending.load()&&!tasks[3].wakeup_pending.load());
  std::puts("publication count controls PASS");return 0;
 }
 if(mode==5){
  for(unsigned i=0;i<4;++i){tasks[i].cpu=i;tasks[i].sched_queue=Task::sched_queue::WAITING;tasks[i].wants_block=true;storage.rows[i].waiting=&tasks[i];}
  wki_remote_compute_notify_pending_submit();
  for(auto& t:tasks){assert(!t.wants_block&&t.sched_queue==Task::sched_queue::RUNNABLE);assert(scheduler_wait_should_cancel(&t,SchedulerHaltWaitKind::BLOCK));assert(t.deferred_task_switch);}
  std::puts("already waiting controls PASS");return 0;
 }
 adversarial=mode!=3;
 for(unsigned i=0;i<4;++i){
  tasks[i].cpu=i;tasks[i].pid=100+i;tasks[i].heap_index=0;storage.rows[i].runnable_heap.member=&tasks[i];
  if(mode==1)storage.rows[i].current_task=&tasks[i];
  if(mode==2)storage.rows[i].handoff_task=&tasks[i];
 }
 auto enqueue=[](){
  s_pending_submit_lock.lock();assert(s_pending_task_submit_count==0);
  auto* p=new TaskSubmitPayload{19};
  g_pending_task_submits[0]={1,reinterpret_cast<uint8_t*>(p),sizeof(*p),1000,0,0};
  s_pending_task_submit_count=1;s_pending_submit_lock.unlock();
 };
 if(mode==3)enqueue();
 std::array<std::thread,4> workers;
 for(unsigned i=0;i<4;++i)workers[i]=std::thread([i](){cpu::current=i;subject=&tasks[i];try{wki_compute_submit_thread();}catch(Stop const&){};subject=nullptr;});
 if(adversarial){observed_empty.arrive_and_wait();enqueue();wki_remote_compute_notify_pending_submit();resume_workers.arrive_and_wait();}
 for(auto& t:workers)t.join();
 std::printf("mode=%d handled=%u queued=%zu blocked_with_work=%u cancelled=%u\n",mode,handled.load(),s_pending_task_submit_count,blocked_with_work.load(),cancelled.load());
 bool const pass=handled.load()==1&&s_pending_task_submit_count==0;
 for(auto& t:tasks)assert(t.deferred_task_switch&&!t.reschedule_pending.load()&&!t.reschedule_in_progress.load());
 if(!pass){delete[] g_pending_task_submits[0].payload;g_pending_task_submits[0].payload=nullptr;}
 return pass?0:1;
}
'''
# Queue owns byte arrays in production; keep that exact delete[] ownership.
code = code.replace('auto* p=new TaskSubmitPayload{19};', 'auto* bytes=new uint8_t[sizeof(TaskSubmitPayload)];auto* p=new(bytes) TaskSubmitPayload{19};')
code = code.replace('#include <mutex>', '#include <mutex>\n#include <new>')
compiler = shutil.which('clang++-23') or '/usr/bin/clang++'
artifact_dir = os.environ.get('WOS_SUBMIT_NOTIFY_ARTIFACT_DIR')
temporary = None
if artifact_dir:
    output = Path(artifact_dir)
    output.mkdir(parents=True, exist_ok=False)
else:
    temporary = tempfile.TemporaryDirectory(prefix='wos-submit-notification-')
    output = Path(temporary.name)
cases = []
for label, notify in [('actual', notifier), ('missing_event_token', notifier.replace('ker::mod::sched::wake_task_from_event(', 'ker::mod::sched::kern_wake('))]:
    cpp, exe = output / (label + '.cpp'), output / label
    cpp.write_text(code.replace('// NOTIFIER_SLOT', notify))
    built = subprocess.run([compiler, '-std=c++23', '-O1', '-g', '-pthread', '-fsanitize=address,undefined', '-fno-sanitize-recover=all', '-I', str(R / 'modules/kern/src'), str(cpp), '-o', str(exe)], capture_output=True, text=True)
    (output / (label + '-build.log')).write_text(built.stdout + built.stderr)
    assert built.returncode == 0, built.stdout + built.stderr
    for mode in range(6):
        result = subprocess.run([str(exe), str(mode)], capture_output=True, text=True, timeout=15)
        (output / (label + '-mode-' + str(mode) + '.log')).write_text(result.stdout + result.stderr)
        wanted = 1 if label == 'missing_event_token' and mode == 0 else 0
        assert result.returncode == wanted, (label, mode, result.returncode, result.stdout, result.stderr)
        if wanted: assert 'handled=0 queued=1 blocked_with_work=4 cancelled=0' in result.stdout
        cases.append(dict(label=label, mode=mode, returncode=result.returncode, stdout=result.stdout, stderr=result.stderr))
with (output / 'proof.json').open('x') as out:
    json.dump(dict(status='ACTUAL_SUBMIT_NOTIFICATION_SIX_CONTROLS_PASS_AND_MISSING_EVENT_TOKEN_STRANDS_QUEUED_WORK',
        cases=cases, test_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), source_inputs={p: hashlib.sha256((R / p).read_bytes()).hexdigest() for p in paths},
        scope='Actual submit worker/notifier, full scheduler reschedule_once/leader/event-wake/cancel decisions; real barrier-controlled host threads. Heap/CPU/IRQ/perf leaves are seams. Proves one notification interleaving; does not establish historical VM cause or timing.'), out, indent=2)
print('Actual submit notification controls PASS; missing-token mutant strands one request with all four workers blocked')
if temporary is not None:
    temporary.cleanup()
