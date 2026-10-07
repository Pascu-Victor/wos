#!/usr/bin/env python3
"""Actual IPC flush/pump shutdown ownership, with explicit scheduler/VFS seams.

No network or kernel scheduling claim: the production worker bodies, Task
publication, File loans, slot retirement and task-side drain are extracted.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import wki_secure_ipc_test as base

ROOT = base.ROOT
CPP = base.CPP


def fn(source, name):
    for match in re.finditer(r'^(?:\[\[noreturn\]\] )?(?:auto|void|bool) ' + re.escape(name) + r'\(', source, re.M):
        brace = source.index('{', match.start())
        if ';' not in source[match.start():brace]:
            return base.extent(source, match.start())
    raise ValueError(name)


SCHED = r'''
struct CleanExit {};
struct CooperativeExit {};
namespace fixture {
thread_local unsigned holds=0;
bool shutdown=false,post_ok=true;
unsigned parks=0,reads=0,discards=0;
std::function<void()> park_hook,wake_hook,post_hook,create_hook,file_release_hook;
}
namespace ker::mod::sched {namespace task {
struct Task {
 std::atomic<unsigned>refs{1};uint64_t pid=19;bool destroyed=false;
 static Task*next;void set_wait_channel(const char*){}
 bool try_acquire_lifetime_ref(){auto n=refs.load();while(n&& !refs.compare_exchange_weak(n,n+1)){};return n!=0;}
 void release(){CHECK(!fixture::guards);CHECK(refs.fetch_sub(1)>1);}
 static Task*create_kernel_thread(const char*,void(*)()){CHECK(!fixture::guards);if(fixture::create_hook)fixture::create_hook();return std::exchange(next,nullptr);}
};Task*Task::next=nullptr;
void destroy_unpublished_user_thread(Task*t){CHECK(!fixture::guards&&t->refs==1);t->refs=0;t->destroyed=true;}
}
task::Task current;
auto get_current_task()->task::Task*{return &current;}
struct Hold {Hold(){++fixture::holds;}~Hold(){CHECK(fixture::holds);--fixture::holds;}};
auto hold_kernel_shutdown()->Hold{return {};}
bool kernel_threads_shutdown_requested(){return fixture::shutdown;}
void kern_block(){CHECK(!fixture::guards);++fixture::parks;if(fixture::park_hook)fixture::park_hook();if(fixture::shutdown&&!fixture::holds)throw CooperativeExit{};}
void kern_sleep_us(uint64_t){kern_block();}
void kern_yield(){kern_block();}
void kern_wake(task::Task*t){CHECK(!fixture::guards&&t&&t->refs>1);++fixture::wakes;if(fixture::wake_hook)fixture::wake_hook();}
bool post_task_balanced(task::Task*){CHECK(!fixture::guards);if(fixture::post_hook)fixture::post_hook();return fixture::post_ok;}
[[noreturn]]void exit_current_kernel_thread(){CHECK(!fixture::guards&&!fixture::holds);throw CleanExit{};}
}
namespace ker::mod::dbg {
template<class...A>void log(A...){}
template<size_t N>struct String{char a[N];constexpr String(const char(&s)[N]){std::copy(s,s+N,a);}};
template<String S>struct logger{template<class...A>static void warn(A...){};};
}
namespace dev::pty {constexpr int POLLOUT=4,POLLERR=8;}
'''

SUPPORT = r'''
void promote_latency_sensitive_daemon(ker::mod::sched::task::Task*){CHECK(!fixture::guards);}
void schedule_pending_pipe_discard(const WkiChannelIdentity&,uint32_t){CHECK(!fixture::guards);++fixture::discards;}
void release_pending_pipe_chunk(const PendingPipeChunk&){ }
auto export_pipe_rdma_front_locked(WkiIpcExport*,uint32_t*,uint16_t*,uint8_t**)->bool{CHECK(fixture::guards);return false;}
auto wki_send_on_channel_identity(const WkiChannelIdentity&,MsgType,const void*,uint16_t)->int{CHECK(!fixture::guards);return WKI_OK;}
void log_pipe_pump_send_retry_diag(const char*,uint16_t,uint32_t,ssize_t,int,uint32_t,uint64_t){}
auto stop_pipe_pump_locked(WkiIpcExport*)->ker::mod::sched::task::Task*;
void wake_pipe_pump(ker::mod::sched::task::Task*);
auto export_has_pipe_pump_slot_locked(const WkiIpcExport*)->bool;
void queue_export_pipe_write_flush_locked(ExportPipeWriteBacklog*);
auto export_pipe_write_backlog_shard(const ExportPipeWriteBacklog*)->size_t;
void wake_export_pipe_write_flush_worker(size_t);
void free_export_pipe_write_backlog(ExportPipeWriteBacklog*);
void erase_export_pipe_write_backlog_locked(ExportPipeWriteBacklog*);
'''

CASES = r'''
}
using namespace ker::net::wki;
namespace {
ker::vfs::FileOperations ops;
ker::vfs::File file;
WkiChannel control{.channel_id=WKI_CHAN_RESOURCE},data;
void reset(){
 CHECK(!fixture::guards&&!fixture::holds&&g_ipc_exports.size()==0&&g_export_pipe_write_backlogs.size()==0&&!g_secure_retired_exports);
 CHECK(g_pipe_pump_queue_head==nullptr&&g_pipe_pump_queue_tail==nullptr);
 fixture::shutdown=false;fixture::post_ok=true;fixture::park_hook=fixture::wake_hook=fixture::post_hook=fixture::create_hook=fixture::file_release_hook={};
 fixture::native_hook={};fixture::registration_hook={};fixture::busy=false;fixture::available=true;fixture::register_fail=false;
 fixture::native_pipe=true;fixture::effect_growth_forbidden=false;fixture::native_result=-EAGAIN;
 g_ipc_consumer_stopping=false;g_ipc_shutdown_drain_owner=false;g_ipc_flush_workers_starting=false;registered=nullptr;
 for(auto&t:g_export_pipe_write_flush_tasks)CHECK(t==nullptr);
 for(auto&s:g_export_pipe_write_flush_stopped)s=true;
 for(auto&q:g_export_pipe_write_flush_queues)CHECK(q.empty());
 for(auto&a:g_pump_args){CHECK(a.worker==nullptr&&a.exp==nullptr);a.stopped=true;a.starting=false;}
 epoch={.used=true,.node_id=2,.epoch=3};file.refcount=1;file.open_flags=0;file.fops=&ops;
 ops={};ops.vfs_poll_check=[](ker::vfs::File*,int){CHECK(!fixture::guards);return 1;};
 ops.vfs_poll_register_waiter=[](ker::vfs::File*,uint64_t){CHECK(!fixture::guards);return true;};
 ops.vfs_write=[](ker::vfs::File*f,const void*,size_t,size_t)->ssize_t{CHECK(!fixture::guards&&fixture::holds==1&&(f->open_flags&WKI_IPC_O_NONBLOCK));if(fixture::native_hook)fixture::native_hook();return -EAGAIN;};
 ops.vfs_read=[](ker::vfs::File*f,void*,size_t,size_t)->ssize_t{CHECK(!fixture::guards&&fixture::holds==1);CHECK(f!=&file&&(f->open_flags&WKI_IPC_O_NONBLOCK)&&file.open_flags==0&&file.refcount>=3);++fixture::reads;if(fixture::native_hook)fixture::native_hook();return -EAGAIN;};
}
auto setup(bool secure=false)->WkiIpcExport*{
 auto*e=new WkiIpcExport;e->resource_id=124;e->consumer_node=2;e->res_type=ResourceType::IPC_PIPE;
 e->channels={{&control,2,WKI_CHAN_RESOURCE,7},{&data,2,WKI_CHAN_IPC_DATA,7}};
 if(secure){ExportPipeWriteBacklog*b=nullptr;prepare_export_pipe_rdma(*e,b,&file);CHECK(e->pipe_rdma_enabled);}
 e->file=&file;file.refcount.fetch_add(1);e->active=true;g_ipc_exports.push_back(e);return e;
}
void detach(WkiIpcExport*e){
 auto flags=s_ipc_lock.lock_irqsave();e->active=false;e->file=nullptr;auto*t=stop_pipe_pump_locked(e);s_ipc_lock.unlock_irqrestore(flags);
 wake_pipe_pump(t);ipc_release_file_ref(&file);
}
void publish(ker::mod::sched::task::Task&t,size_t shard){CHECK(t.try_acquire_lifetime_ref());g_export_pipe_write_flush_tasks.at(shard)=&t;g_export_pipe_write_flush_stopped.at(shard)=false;}
void idle_flushes(){for(size_t s=0;s<4;++s){reset();ker::mod::sched::task::Task t;publish(t,s);fixture::park_hook=[](){CHECK(fixture::holds==1);fixture::shutdown=true;};
 export_pipe_write_flush_thread_loop(s);CHECK(g_export_pipe_write_flush_tasks.at(s)==nullptr&&g_export_pipe_write_flush_stopped.at(s)&&t.refs==1&&!fixture::holds);CHECK(wki_ipc_secure_shutdown_drain());++fixture::cases;}}
void flush_late_retirement(){reset();auto*e=setup(true);auto*b=new ExportPipeWriteBacklog;b->exp=e;b->resource_id=e->resource_id;b->persistent_for_rdma=true;static uint8_t payload[4]={1,2,3,4};b->chunks.push_back({payload,4});b->buffered_bytes=4;g_export_pipe_write_backlogs.push_back(b);
 auto flags=s_ipc_lock.lock_irqsave();queue_export_pipe_write_flush_locked(b);s_ipc_lock.unlock_irqrestore(flags);ker::mod::sched::task::Task t;publish(t,0);
 fixture::native_hook=[&](){CHECK(!g_export_pipe_write_flush_stopped[0]&&e->pipe_write_in_flight&&file.refcount==3);fixture::shutdown=true;};export_pipe_write_flush_thread_loop(0);
 CHECK(!e->pipe_write_in_flight&&file.refcount==2&&t.refs==1&&g_export_pipe_write_flush_tasks[0]==nullptr);
 detach(e);fixture::busy=true;CHECK(!wki_ipc_secure_shutdown_drain()&&g_secure_retired_exports!=nullptr&&file.refcount==1);
 fixture::busy=false;CHECK(wki_ipc_secure_shutdown_drain()&&g_secure_retired_exports==nullptr);++fixture::cases;
}
void assigned_pump(){reset();auto*e=setup();auto*next=setup();ker::mod::sched::task::Task t;CHECK(t.try_acquire_lifetime_ref());auto&a=g_pump_args[0];a.worker=&t;a.stopped=false;a.exp=e;e->pump_task=&t;e->pump_running=true;
 auto flags=s_ipc_lock.lock_irqsave();enqueue_pipe_pump_locked(next);s_ipc_lock.unlock_irqrestore(flags);
 fixture::native_hook=[](){ops.vfs_poll_check=[](ker::vfs::File*,int){return 0;};};fixture::park_hook=[&](){CHECK(fixture::holds==1&&file.refcount==4);fixture::shutdown=true;};
 fixture::file_release_hook=[&](){CHECK(fixture::holds==1&&!a.stopped&&a.exp==e&&e->pump_task==&t);};
 try{pipe_pump_thread_entry<0>();CHECK(false);}catch(CleanExit&){}
 CHECK(a.worker==nullptr&&a.exp==nullptr&&a.stopped&&e->pump_task==nullptr&&!e->pump_running&&next->pump_queued&&file.refcount==3&&t.refs==1);
 fixture::file_release_hook={};detach(e);detach(next);CHECK(wki_ipc_secure_shutdown_drain()&&file.refcount==1);++fixture::cases;
}
void wake_pins(){reset();ker::mod::sched::task::Task t;publish(t,1);fixture::shutdown=true;
 fixture::wake_hook=[&](){CHECK(t.refs==3);export_pipe_write_flush_thread_loop(1);CHECK(t.refs==2);};wake_export_pipe_write_flush_worker(1);
 CHECK(t.refs==1&&g_export_pipe_write_flush_tasks[1]==nullptr);fixture::wake_hook={};auto prior=fixture::wakes;wake_export_pipe_write_flush_worker(1);CHECK(fixture::wakes==prior);CHECK(wki_ipc_secure_shutdown_drain());++fixture::cases;
 reset();auto*e=setup();ker::mod::sched::task::Task pump;CHECK(pump.try_acquire_lifetime_ref());auto&a=g_pump_args[0];a.worker=&pump;a.stopped=false;a.exp=e;e->pump_task=&pump;e->pump_running=true;
 auto flags=s_ipc_lock.lock_irqsave();auto*retained=stop_pipe_pump_locked(e);s_ipc_lock.unlock_irqrestore(flags);CHECK(retained==&pump&&pump.refs==3);fixture::shutdown=true;
 fixture::wake_hook=[&](){pipe_pump_thread_fn<0>();CHECK(pump.refs==2&&a.worker==nullptr&&a.exp==nullptr&&a.stopped);};wake_pipe_pump(retained);CHECK(pump.refs==1);fixture::wake_hook={};detach(e);CHECK(wki_ipc_secure_shutdown_drain());++fixture::cases;
}
void startup_pressure(){reset();ker::mod::sched::task::Task t;ker::mod::sched::task::Task::next=&t;fixture::post_ok=false;
 CHECK(create_pipe_pump_worker(g_pump_args[0],pipe_pump_thread_entry<0>)==nullptr);CHECK(t.destroyed&&t.refs==0&&g_pump_args[0].worker==nullptr&&!g_pump_args[0].starting&&g_pump_args[0].stopped);CHECK(wki_ipc_secure_shutdown_drain());++fixture::cases;
 reset();ker::mod::sched::task::Task t2;ker::mod::sched::task::Task::next=&t2;fixture::create_hook=[](){CHECK(!wki_ipc_secure_shutdown_drain());};
 CHECK(create_pipe_pump_worker(g_pump_args[0],pipe_pump_thread_entry<0>)==nullptr&&t2.destroyed&&t2.refs==0);CHECK(wki_ipc_secure_shutdown_drain());++fixture::cases;
 reset();ker::mod::sched::task::Task t3;ker::mod::sched::task::Task::next=&t3;fixture::post_hook=[](){fixture::shutdown=true;pipe_pump_thread_fn<0>();};
 CHECK(create_pipe_pump_worker(g_pump_args[0],pipe_pump_thread_entry<0>)==nullptr&&!t3.destroyed&&t3.refs==1&&g_pump_args[0].stopped&&!g_pump_args[0].starting);CHECK(wki_ipc_secure_shutdown_drain());++fixture::cases;
 reset();g_ipc_shutdown_drain_owner=true;CHECK(!wki_ipc_secure_shutdown_drain());g_ipc_shutdown_drain_owner=false;CHECK(wki_ipc_secure_shutdown_drain());++fixture::cases;
}
void drain_refuses_live(){reset();auto*e=setup(true);CHECK(!wki_ipc_secure_shutdown_drain()&&e->pipe_rdma_region&&e->active);detach(e);CHECK(wki_ipc_secure_shutdown_drain());++fixture::cases;}
}
int main(){idle_flushes();flush_late_retirement();assigned_pump();wake_pins();startup_pressure();drain_refuses_live();CHECK(!fixture::guards&&!fixture::holds);std::printf("IPC shutdown actual-source PASS cases=%u reads=%u\n",fixture::cases,fixture::reads);}
'''


def make_tu(source):
    tu = base.make_tu(source).split('WkiChannel control{', 1)[0]
    for name in ['proxy_pipe_write', 'proxy_pipe_write_rdma']:
        tu = tu.replace(fn(tu, name), '')
    start = tu.index('namespace ker::mod::sched {namespace task')
    end = tu.index('namespace ker::vfs {', start)
    tu = tu[:start] + SCHED + tu[end:]
    tu = tu.replace('struct FileOperations {ssize_t(*vfs_write)(File*,const void*,size_t,size_t)=nullptr;};',
                    'struct FileOperations {ssize_t(*vfs_write)(File*,const void*,size_t,size_t)=nullptr;ssize_t(*vfs_read)(File*,void*,size_t,size_t)=nullptr;int(*vfs_poll_check)(File*,int)=nullptr;bool(*vfs_poll_register_waiter)(File*,uint64_t)=nullptr;};')
    tu = tu.replace('enum class WkiPerfIpcOp{PROXY_WRITE}', 'enum class WkiPerfIpcOp{PROXY_WRITE,PIPE_PUMP_READ,PIPE_PUMP_SEND}')
    tu = tu.replace('if(f)CHECK(f->refcount.fetch_sub(1)>1);', 'if(f){if(fixture::file_release_hook)fixture::file_release_hook();CHECK(f->refcount.fetch_sub(1)>1);}')
    tu = tu.replace('struct PendingPipeChunk {const uint8_t*data=nullptr;uint16_t len=0;uint16_t flow_credit=0;IpcDevOpWork*dev_op_owner=nullptr;};', base.decl(source, 'PendingPipeChunk'))
    old_backlog = 'struct ExportPipeWriteBacklog {WkiIpcExport*exp=nullptr;std::deque<PendingPipeChunk>chunks;bool close_pending=false;int32_t write_error=0;uint32_t buffered_bytes=0;};'
    tu = tu.replace(old_backlog, base.decl(source, 'ExportPipeWriteBacklog'))
    registry = source[source.index('template <typename Entry>\nclass IpcRegistry'):source.index('using IpcExportRegistry =')]
    tu = tu.replace('std::vector<WkiIpcExport*>g_ipc_exports;', registry + 'IpcRegistry<WkiIpcExport>g_ipc_exports;')
    tu = tu.replace('std::vector<ExportPipeWriteBacklog*>g_export_pipe_write_backlogs;', 'IpcRegistry<ExportPipeWriteBacklog>g_export_pipe_write_backlogs;')
    for name in ['export_has_pipe_pump_slot_locked', 'queue_export_pipe_write_flush_locked', 'export_pipe_write_backlog_shard', 'wake_export_pipe_write_flush_worker']:
        tu = tu.replace(fn(tu, name), '')
    tu = tu.replace('std::atomic<bool>g_ipc_consumer_stopping{false};', '')
    constants = []
    for name in ['WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_COUNT', 'WKI_IPC_PIPE_WRITE_RETRY_US', 'WKI_IPC_PIPE_READ_POLL_RECHECK_US', 'WKI_IPC_PIPE_EOF_MAX_SEND_ATTEMPTS', 'WKI_IPC_PIPE_RDMA_WATCH_US', 'WKI_IPC_MAX_EXPORTS']:
        constants.append(re.search(r'constexpr[^\n;]*\b' + name + r'\s*=[^\n]*;', source + base.HPP.read_text())[0])
    state = source[source.index('std::array<std::deque<ExportPipeWriteBacklog*>'):source.index('constexpr std::array<const char*', source.index('std::array<std::deque<ExportPipeWriteBacklog*>'))]
    pumps = source[source.index('struct PipePumpArg {'):source.index('auto export_has_pipe_pump_slot_locked', source.index('struct PipePumpArg {'))]
    declarations = '\n'.join(constants) + '\n' + state + pumps + base.decl(source, 'FailedExportPipeWriteRetirement') + SUPPORT
    pos = tu.index('auto export_has_write_backlog_locked(')
    tu = tu[:pos] + declarations + tu[pos:]
    helper_names = ['export_pipe_write_backlog_shard', 'wake_export_pipe_write_flush_worker', 'wake_all_export_pipe_write_flush_workers', 'erase_export_pipe_write_backlog_locked', 'free_export_pipe_write_backlog', 'queue_export_pipe_write_flush_locked', 'send_export_pipe_write_flow', 'complete_export_pipe_flow_receipt_locked', 'retire_failed_export_pipe_write_locked', 'finish_export_pipe_write_locked', 'export_has_pipe_pump_slot_locked', 'remove_pipe_pump_queue_locked', 'enqueue_pipe_pump_locked', 'pop_pipe_pump_queue_locked', 'assign_next_pipe_pump_locked', 'release_pipe_pump_slot_locked', 'collect_pipe_pump_retirement_locked', 'finish_pipe_pump_retirement', 'stop_pipe_pump_locked', 'wake_pipe_pump', 'register_poll_read_waiter', 'pipe_pump_read_ready', 'register_poll_write_waiter', 'export_pipe_write_flush_thread_loop']
    tu += '\n' + '\n'.join(fn(source, n) for n in helper_names)
    tu += '\ntemplate <int SLOT>\n' + fn(source, 'pipe_pump_thread_fn')
    tu += '\ntemplate <int SLOT>\n' + fn(source, 'pipe_pump_thread_entry')
    tu += '\nusing PumpFn=void(*)();\n' + fn(source, 'create_pipe_pump_worker') + '\n' + fn(source, 'wki_ipc_secure_shutdown_drain')
    return tu + CASES


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, default=CPP)
    parser.add_argument('--keep', type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='wki-ipc-shutdown-') as tmp:
        out = args.keep or Path(tmp)
        out.mkdir(parents=True, exist_ok=True)
        tu = out / 'ipc-shutdown-tu.cpp'
        tu.write_text(make_tu(args.source.read_text()))
        command = ['/usr/bin/clang++', '-std=c++23', '-Wall', '-Wextra', '-g', '-O1', '-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-fno-pie', '-no-pie', '-pthread', '-I' + str(base.KERN), str(tu), '-o', str(out / 'ipc-shutdown-test')]
        compile_result = subprocess.run(command, capture_output=True, text=True)
        (out / 'compile.log').write_text(compile_result.stdout + compile_result.stderr)
        run = subprocess.run([str(out / 'ipc-shutdown-test')], capture_output=True, text=True, env=os.environ | {'ASAN_OPTIONS': 'detect_leaks=0', 'UBSAN_OPTIONS': 'halt_on_error=1'}) if compile_result.returncode == 0 else None
        if run:
            (out / 'run.log').write_text(run.stdout + run.stderr)
        receipt = {'compile_exit': compile_result.returncode, 'run_exit': run.returncode if run else None, 'command': command, 'source': str(args.source), 'source_sha256': hashlib.sha256(args.source.read_bytes()).hexdigest(), 'fixture_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'base_fixture_sha256': hashlib.sha256(Path(base.__file__).read_bytes()).hexdigest()}
        (out / 'result.json').write_text(json.dumps(receipt, indent=2) + '\n')
        print(compile_result.stdout + compile_result.stderr if compile_result.returncode else run.stdout + run.stderr)
        return compile_result.returncode or run.returncode


if __name__ == '__main__':
    raise SystemExit(main())
