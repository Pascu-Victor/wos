#!/usr/bin/env python3
"""Run actual submit-tail and perf-filter code with controlled host leaves.

Checks diagnostic ordering, unlocked recording, disabled cost, identity/status
columns and unchanged accepted/denied/timeout behavior. Kernel concurrency and
elapsed interval costs require separate WOS runtime evidence.
"""
import os
from pathlib import Path
import re
import resource
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
COMPUTE = (ROOT / 'modules/kern/src/net/wki/remote_compute.cpp').read_text()
PERF = (ROOT / 'modules/kern/src/platform/perf/perf_events.cpp').read_text()
HEADER = (ROOT / 'modules/kern/src/platform/perf/perf_events.hpp').read_text()
WIRE = (ROOT / 'modules/kern/src/net/wki/wire.hpp').read_text()
WKI = (ROOT / 'modules/kern/src/net/wki/wki.hpp').read_text()


def extract(text, marker, declaration=False):
    start = text.index(marker)
    position = text.index('{', start) + 1
    depth = 1
    while depth:
        depth += (text[position] == '{') - (text[position] == '}')
        position += 1
    return text[start:position] + (';' if declaration else '')


PRELUDE = r'''
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <tuple>
#include <vector>
struct Lock {bool held=false;void lock(){assert(!held);held=true;}
 void unlock(){assert(held);held=false;}} s_compute_lock;
struct Task {uint64_t pid=123;uint32_t cpu=2;};
Task owner;unsigned lookups=0;
namespace ker::mod::sched {Task* get_current_task(){++lookups;return &owner;}}
struct Record {uint8_t op;uint16_t peer,channel;uint32_t correlation;int32_t status;};
std::vector<Record> records;std::string actions;unsigned summaries=0,logs=0;
#define WOS_PERF_CALLSITE() uint64_t{999}
namespace ker::mod::perf {
std::atomic<bool> g_enabled{true};std::atomic<uint16_t> g_event_mask{0};
'''

RECORDERS = r'''
void record_wki_event(uint32_t cpu,uint64_t pid,WkiPerfScope scope,uint8_t op,
 WkiPerfPhase phase,uint16_t peer,uint16_t channel,uint32_t correlation,
 int32_t status,uint32_t aux,uint64_t callsite){
 assert(!s_compute_lock.held&&cpu==2&&pid==123&&scope==WkiPerfScope::REMOTE_COMPUTE);
 assert(phase==WkiPerfPhase::POINT&&peer==19&&channel==3&&correlation==7);
 assert(aux==0&&callsite==999);
 if(wki_should_record(g_event_mask.load(),scope,op))records.push_back({op,peer,channel,correlation,status});
 actions+='P';
}
void record_wki_summary(WkiPerfScope scope,uint8_t op,uint16_t peer,uint16_t channel,
 int32_t status,uint32_t latency,bool has_latency,uint32_t retries,uint64_t bytes){
 assert(!s_compute_lock.held&&scope==WkiPerfScope::REMOTE_COMPUTE);
 assert(peer==19&&channel==3&&latency==0&&!has_latency&&retries==0&&bytes==0);
 assert(status==0||status<0);
 if(wki_should_record(g_event_mask.load(),scope,op))++summaries;
}
}
'''

LEAVES = r'''
struct Wait {};Wait* expected_wait=nullptr;
struct SubmittedTask {
 bool active=true,submission_started=true;uint8_t accept_status=5;
 Wait* response_wait_entry=nullptr;Wait* response_consumer_wait_entry=nullptr;
 std::atomic<bool> response_pending{true};
 void cancel_service_report(bool started){assert(s_compute_lock.held&&started);actions+='C';}
} row;
bool missing=false;int wait_result=0,cutover_result=0;unsigned cancellations=0;
struct SubmittedIpcHandoffCleanup {std::array<int,1> handoff{};uint16_t count=0;};
struct WkiIpcSubmissionGuard {template<class... A>WkiIpcSubmissionGuard(A...){}};
SubmittedTask* find_submitted_task_any(uint32_t id){assert(s_compute_lock.held&&id==7);return missing?nullptr:&row;}
int wait_for_exec_submit(Wait& wait,uint64_t,uint32_t id,Task& task,const void*,int& error){
 assert(!s_compute_lock.held&&id==7&&&task==&owner);expected_wait=&wait;
 row.response_wait_entry=row.response_consumer_wait_entry=&wait;
 actions+='W';error=cutover_result;return wait_result;
}
void restore_submitted_ipc_preparation_locked(SubmittedTask& task,int*,uint16_t,WkiIpcSubmissionGuard&){
 assert(s_compute_lock.held&&&task==&row&&!row.active);actions+='R';
}
uint8_t* consume_submitted_task_result_locked(SubmittedTask* task){
 assert(s_compute_lock.held&&task==&row&&!row.active);actions+='D';return new uint8_t[1]{42};
}
SubmittedIpcHandoffCleanup take_submitted_ipc_handoff_locked(SubmittedTask* task){
 assert(s_compute_lock.held&&task==&row&&row.active);actions+='T';return {{},1};
}
uint16_t wki_ipc_commit_task_fd_handoff(Task*,int*,uint16_t count){
 assert(!s_compute_lock.held&&count==1);actions+='H';return count;
}
void wki_ipc_activate_task_fds(int*,uint16_t,uint16_t node){assert(!s_compute_lock.held&&node==19);actions+='A';}
int send_task_cancel_request(uint16_t node,uint32_t id,int,void*,uint32_t){
 assert(!s_compute_lock.held&&node==19&&id==7);++cancellations;actions+='Q';return 0;
}
uint64_t wki_now_us(){return 100;}
const char* errno_name(int){return "controlled";}
namespace ker::mod::dbg {
template<class... A>void log(const char* format,A... args){
 assert(!s_compute_lock.held);++logs;actions+='L';
 if constexpr(sizeof...(A)==2){
  assert(std::strcmp(format,"[WKI] VFS_REF task rejected: task_id=%u status=%u")==0);
  auto tuple=std::tuple(args...);assert(std::get<0>(tuple)==7&&std::get<1>(tuple)==(missing?0:row.accept_status));
 }
}
}
void perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp op,uint16_t peer,
 uint32_t id,int32_t status,uint32_t,uint64_t,uint64_t){
 assert(!s_compute_lock.held&&op==ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF&&peer==19&&id==7);
 assert(status==(wait_result?wait_result:missing?-1:-int(row.accept_status)));actions+='E';
}
uint32_t run_submit(int& error){
 constexpr uint32_t TASK_ID=7;constexpr uint16_t target_node=19,PATH_LEN_WIRE=12;
 constexpr uint64_t STARTED_US=90,CALLSITE=777,WKI_TASK_SUBMIT_VFS_TIMEOUT_US=500000;
 constexpr int WKI_SIGKILL_NUM=9;
 Task* local_task=&owner;const void* transfer=nullptr;Wait wait{};
 struct {void* channel=nullptr;uint32_t generation=1;} SUBMIT_CHANNEL;
 int ipc_fd_handoff[1]{},ipc_fd_map[1]{};uint16_t ipc_fd_count=1;
 WkiIpcSubmissionGuard ipc_guard;
 auto cleanup_submit_ipc=[](){assert(!s_compute_lock.held);actions+='I';};
 auto fail_submit=[&](int value)->uint32_t{error=value;return 0;};
'''

CASES = r'''
void reset(bool enabled=true){
 assert(!s_compute_lock.held);records.clear();actions.clear();summaries=logs=lookups=cancellations=0;
 missing=false;wait_result=cutover_result=0;row.active=true;row.submission_started=true;
 row.accept_status=5;row.response_pending=true;
 ker::mod::perf::g_enabled=enabled;ker::mod::perf::g_event_mask=ker::mod::perf::PERF_MASK_WKI_LAUNCH;
}
int main(){
 using namespace ker::mod::perf;
 static_assert(uint8_t(WkiPerfComputeOp::COMPLETE_HOLD)==13);
 for(uint8_t op=1;op<=16;++op){
  assert(wki_should_record(PERF_MASK_WKI_LAUNCH,WkiPerfScope::REMOTE_COMPUTE,op));
  assert(wki_should_record(PERF_MASK_ALL,WkiPerfScope::REMOTE_COMPUTE,op));
  assert(!wki_should_record(PERF_MASK_LOCAL_PROC,WkiPerfScope::REMOTE_COMPUTE,op));
  assert(!wki_should_record(0,WkiPerfScope::REMOTE_COMPUTE,op));
  auto data=wki_pack_event_data(WkiPerfScope::REMOTE_COMPUTE,op,WkiPerfPhase::POINT,19,3);
  WkiPerfScope scope;WkiPerfPhase phase;uint8_t decoded;uint16_t peer,channel;
  wki_unpack_event_data(data,scope,decoded,phase,peer,channel);
  assert(scope==WkiPerfScope::REMOTE_COMPUTE&&decoded==op&&phase==WkiPerfPhase::POINT&&peer==19&&channel==3);
 }
 assert(!wki_should_record(PERF_MASK_WKI_LAUNCH,WkiPerfScope::TRANSPORT,14));
 assert(!wki_should_record(PERF_MASK_WKI_LAUNCH,WkiPerfScope::REMOTE_COMPUTE,17));
 assert(std::string(compute_op_name(14))=="submit_wait_return");
 assert(std::string(compute_op_name(15))=="submit_rejection_log_begin");
 assert(std::string(compute_op_name(16))=="submit_rejection_log_end");
 int error=0;reset();assert(run_submit(error)==0&&error==-EACCES);
 assert(actions=="WPCRDIPLPE"&&logs==1&&summaries==3&&lookups==6);
 assert(records.size()==3&&records[0].op==14&&records[0].status==0);
 assert(records[1].op==15&&records[1].status==-5&&records[2].op==16&&records[2].status==-5);
 assert(row.response_wait_entry==nullptr&&row.response_consumer_wait_entry==nullptr);
 reset(false);error=0;assert(run_submit(error)==0&&error==-EACCES);
 assert(actions=="WCRDILE"&&records.empty()&&summaries==0&&lookups==0&&logs==1);
 reset();missing=true;error=0;assert(run_submit(error)==0&&error==-EIO);
 assert(actions=="WPIPLPE"&&records[1].status==-1&&records[2].status==-1);
 reset();row.accept_status=0;error=0;assert(run_submit(error)==7&&error==0);
 assert(actions=="WPTHAE"&&records.size()==1&&logs==0&&row.active);
 reset();wait_result=WKI_ERR_TIMEOUT;error=0;assert(run_submit(error)==0&&error==-ETIMEDOUT);
 assert(actions=="WPCRDQILE"&&records.size()==1&&records[0].status==WKI_ERR_TIMEOUT&&cancellations==1);
 reset();wait_result=WKI_ERR_BUSY;cutover_result=-ECANCELED;error=0;
 assert(run_submit(error)==0&&error==-ECANCELED&&records.size()==1);
 puts("actual submit phase: masks, packing, names, unlocked order, disabled guard, denial/accept/timeout/cutover pass");
}
'''


def main():
    enums = ''.join(extract(HEADER, 'enum class ' + name, True)
                    for name in ('WkiPerfScope', 'WkiPerfPhase', 'WkiPerfComputeOp'))
    masks = '\n'.join(re.findall(r'^constexpr uint16_t PERF_MASK_[^;]+;', HEADER, re.M))
    helpers = ''.join(extract(PERF, marker) for marker in (
        'auto wki_is_launch_measurement_event(', 'auto wki_should_record(',
        'bool is_wki_recording_enabled(', 'uint64_t wki_pack_event_data(', 'void wki_unpack_event_data('))
    names = extract(PERF[PERF.index('const char* wki_op_name('):],
                    'switch (static_cast<WkiPerfComputeOp>(op))')
    constants = '\n'.join(re.findall(r'^constexpr (?:int|uint16_t) WKI_(?:OK|ERR_\w+|CHAN_RESOURCE|NODE_INVALID) = [^;]+;', WKI + WIRE, re.M))
    tail = extract(COMPUTE, 'auto wki_task_submit_vfs_ref(')
    tail = tail[tail.index('    int cutover_error = 0;'):-1]
    program = PRELUDE + enums + masks + helpers
    program += '\nauto compute_op_name(uint8_t op)->const char*{' + names + '}\n' + RECORDERS
    program += constants + extract(WIRE, 'enum class TaskRejectReason', True)
    program += extract(COMPUTE, 'auto task_rejection_errno(') + extract(COMPUTE, 'auto task_transport_errno(')
    program += extract(COMPUTE, 'auto perf_current_pid(') + extract(COMPUTE, 'auto perf_current_cpu(')
    program += extract(COMPUTE, 'void perf_record_compute_point(')
    program += LEAVES + tail + '}\n' + CASES
    compiler = shutil.which('clang++', path=os.defpath) or shutil.which('c++', path=os.defpath)
    with tempfile.TemporaryDirectory(prefix='wki-submit-phase-') as directory:
        source = Path(directory) / 'phase.cpp'
        binary = Path(directory) / 'phase'
        source.write_text(program)
        subprocess.run([compiler, '-std=c++23', '-O1', '-g', '-Wall', '-Wextra', '-Werror',
                        '-fsanitize=address,undefined', str(source), '-o', str(binary)], check=True)
        subprocess.run([str(binary)], check=True, env={**os.environ, 'ASAN_OPTIONS': 'detect_leaks=0'})


if __name__ == '__main__':
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    main()
