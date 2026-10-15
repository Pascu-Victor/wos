#!/usr/bin/env python3
"""Actual service CPU/ownership bodies; explicit sequential Host Task/IRQ primitives.

Compiles real Epoch, resource/service models and declarations, selected complete
service functions, plus the candidate's real local lock wrapper. No native
scheduler, real interrupt/preemption, concurrency or full lifecycle credit.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import sys
import tempfile
import time

ROOT = next(p for p in Path(__file__).resolve().parents if (p/'modules/kern/src').is_dir() and (p/'.github/instructions').is_dir())
KERN = ROOT/'modules/kern/src'
RUNNER = ROOT/'tests/host/unit/file_mmap_registry_runtime_test.py'

LOCKS = r"""
#pragma once
#include <cstdint>
#include <cstdlib>
namespace host {
inline uint64_t flags=0x200, depth=0, acquisitions=0, releases=0;
inline void require(bool v) { if(!v) std::abort(); }
}
namespace ker::mod::sys {
class Spinlock {
 bool held_=false;
public:
 constexpr Spinlock()=default;
 ~Spinlock() {host::require(!held_);}
 void lock(){host::require(!held_); ++host::depth;held_=true;++host::acquisitions;}
 bool try_lock(){if(held_)return false;lock();return true;}
 void unlock(){host::require(held_ && host::depth);held_=false;--host::depth;++host::releases;}
 uint64_t lock_irqsave(){auto f=host::flags;host::flags=0;lock();return f;}
 void unlock_irqrestore(uint64_t f){unlock();host::flags=f;}
};
}
"""
PRELUDE = r"""
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <new>
#include <utility>
#include <platform/sched/service_resource.hpp>
namespace ker::mod::sched::task {
enum class TaskType {PROCESS, IDLE}; enum class TaskState {ACTIVE, DEAD};
struct Task;
struct BorrowedOwner {Task* value=nullptr;Task* get()const{return value;}};
struct Task {
 resource::ServiceMember service_member{};
 resource::State resources{};
 ker::mod::sys::Spinlock resource_lock{};
 bool is_thread=false;
 TaskType type=TaskType::PROCESS;
 std::atomic<TaskState> state{TaskState::ACTIVE};
 std::atomic<bool> scheduler_published{false},exit_notify_ready{false};
 std::atomic<uint64_t> resource_cpu_threads{0},resource_deferred_observations{0};
 BorrowedOwner resource_owner{};
};
}
namespace ker::mod::sched::resource {
namespace service_abi=ker::abi::service_resource;
"""
TESTS = r"""
} // resource
namespace actual=ker::mod::sched::resource;
namespace census=ker::mod::mm::lifecycle_census;
using ker::mod::sched::task::Task;
unsigned assertions=0,failures=0;
void check(bool v,const char* why){++assertions;if(!v){++failures;std::fprintf(stderr,"FAIL %s\n",why);}}
auto count(){auto c=actual::selftest_service_census();check(c.complete&&census::quiescent(c.stamp),"complete real reader");return c;}
void changed(const actual::SelftestServiceCensus&a,const actual::SelftestServiceCensus&b,const char*why){check(!census::unchanged(a.stamp,b.stamp),why);}
int main(){
 auto initial=count();
 // Explicit local fixed-slot input setup; actual owner lock must invalidate it.
 auto f=actual::service_lock.lock_irqsave();
 check(!census::quiescent(actual::selftest_service_epoch.snapshot()),"actual owner acquisition inflight");
 actual::services[0].configured=true;
 actual::services[0].state.totals.entries[0].limit={1,2};
 {
 Task local;
 local.service_member.slot=0;local.service_member.subject=&local;
 actual::services[0].members=&local.service_member;
 actual::service_lock.unlock_irqrestore(f);
 auto local_before=count();changed(initial,local_before,"owner setup advances epoch");
 check(local_before.configured_slots==initial.configured_slots+1&&local_before.local_members==initial.local_members+1,"actual census sees local ownership");
 check(actual::charge_service_cpu(local,500000)==actual::CpuAction::NONE,"local subsecond none");
 check(actual::charge_service_cpu(local,500000)==actual::CpuAction::SOFT_SIGNAL,"local actual soft threshold");
 check(actual::charge_service_cpu(local,1000000)==actual::CpuAction::HARD_KILL,"local actual hard threshold");
 auto local_after=count();
 check(local_before==local_after,"local CPU billing preserves ALL service debt fields and stamp");
 const auto& cpu=actual::services[0].state.totals;
 check(cpu.cpu_time_us==2000000&&cpu.cpu_system_time_us==2000000&&cpu.cpu_user_time_us==0,"local actual billed time");
 check(cpu.entries[0].current==2&&cpu.entries[0].peak==2&&cpu.entries[0].denied==2&&cpu.cpu_hard_notified,"local actual usage and denial");
 check(actual::charge_service_cpu(local,1)==actual::CpuAction::HARD_KILL,"hard limit remains enforced");
 check(count()==local_after,"repeat hard action preserves owner projection");
 // Actual nonwrapping owner operations remain versioned, including equal-count ABA.
 auto* remote=actual::RemoteServiceAccounting::create();check(remote!=nullptr,"actual remote allocation");
 auto created=count();changed(local_after,created,"actual create advances epoch");
 check(created.remote_objects==initial.remote_objects+1&&created.remote_references==initial.remote_references+1,"actual remote storage/ref counters");
 remote->retain();remote->release();auto equal_refs=count();
 check(equal_refs.remote_references==created.remote_references,"retain/release equal-count prefix");
 changed(created,equal_refs,"actual off-lock retain/release remains versioned");
 {
  Task remote_task;check(remote->attach(remote_task),"actual remote attach");
  auto rb=count();changed(equal_refs,rb,"actual remote member publication advances epoch");
  check(rb.remote_members==created.remote_members+1&&rb.remote_references==created.remote_references+1,"actual remote member retains");
  for(unsigned i=0;i<1000;++i)check(actual::charge_service_cpu(remote_task,1234)==actual::CpuAction::NONE,"remote CPU actual none");
  auto ra=count();check(rb==ra,"1000 remote bills preserve ALL debt fields and epoch");
  check(remote->snapshot().cpu_time_us==1234000,"remote actual CPU total");
  actual::ServiceReportTarget report;
  auto before_bind=count();check(report.bind(remote_task)==1,"actual report bind");auto bound=count();
  changed(before_bind,bound,"actual report link versioned");check(bound.remote_reports==before_bind.remote_reports+1,"actual remote report counter");
  actual::RemoteServiceObservation observation{};
  check(report.apply(1,observation,true)==actual::ServiceReportResult::APPLIED,"actual final report apply");auto finished=count();
  changed(bound,finished,"actual final report remains versioned");
  actual::ServiceReportTarget moved(std::move(report));auto relocated=count();
  check(relocated.remote_reports==finished.remote_reports&&relocated.remote_references==finished.remote_references,"same-count report relocation");
  changed(finished,relocated,"actual same-count move remains versioned");
  moved.cancel(false);auto canceled=count();changed(relocated,canceled,"actual cancel and outside release remains versioned");
  check(canceled.remote_reports==before_bind.remote_reports&&canceled.remote_references==before_bind.remote_references,"actual report owner returned");
 }
 auto detached=count();check(detached.remote_members==initial.remote_members&&detached.remote_references==created.remote_references,"actual member destructor releases outside lock");
 remote->release();auto freed=count();changed(detached,freed,"actual final free remains versioned");
 check(freed.remote_objects==initial.remote_objects&&freed.remote_references==initial.remote_references,"actual remote storage returned after delete");
 // Local report.finished is an actual returned census field, so apply is never exempt.
 f=actual::service_lock.lock_irqsave();actual::services[0].state.totals.entries[0].limit={UINT64_MAX,UINT64_MAX};actual::service_lock.unlock_irqrestore(f);
 {
  actual::ServiceReportTarget report;check(report.bind(local)==1,"actual local report bind");auto b=count();
  check(b.local_reports==1&&b.unfinished_local_reports==1,"actual unfinished local report");
  actual::RemoteServiceObservation observation{};check(report.apply(1,observation,true)==actual::ServiceReportResult::APPLIED,"actual local final report");
  auto a=count();changed(b,a,"actual local finished transition advances epoch");check(a.local_reports==1&&a.unfinished_local_reports==0,"actual finished debt projection");
 }
 } // Actual local ServiceMember destructor returns before final owner oracle.
 auto detached_local=count();check(detached_local.local_members==initial.local_members,"actual local member destructor returned");
 f=actual::service_lock.lock_irqsave();actual::services[0]={};actual::service_lock.unlock_irqrestore(f);
 auto end=count();initial.stamp=end.stamp;
 check(initial==end,"all actual configured/member/report/remote owner debt returned");
 check(host::depth==0&&host::flags==0x200&&host::acquisitions==host::releases,"explicit Host lock/IRQ primitive balanced");
 std::printf("%s assertions=%u failures=%u bills=1004 real_epoch=1 real_charge=1 real_census=1 real_cpu_model=1 native_qualification=0\n",failures?"FAIL":"PASS",assertions,failures);
 return failures?3:0;
}
"""


def generate(runner,scratch,selected):
    include=scratch/'include/platform/sys';include.mkdir(parents=True)
    (include/'spinlock.hpp').write_text(LOCKS)
    source=selected.read_text(); spans=[]
    start=source.index('struct ServiceSlot {')
    end=source.index('\nauto is_init(',start)
    globals_body=source[start:end]
    spans.append({'name':'complete actual service globals/local wrapper','line':source.count('\n',0,start)+1,'bytes':len(globals_body.encode()),'sha256':hashlib.sha256(globals_body.encode()).hexdigest()})
    definitions=[]
    anchors=['auto observed_member_usage(', 'ServiceMember::~ServiceMember()',
      'auto RemoteServiceAccounting::attach_locked(', 'auto RemoteServiceAccounting::attach(',
      'auto RemoteServiceAccounting::snapshot()', 'ServiceReportTarget::~ServiceReportTarget()',
      'auto ServiceReportTarget::list_head_locked()', 'void ServiceReportTarget::move_from_locked(',
      'ServiceReportTarget::ServiceReportTarget(ServiceReportTarget&&',
      'auto ServiceReportTarget::operator=(', 'auto ServiceReportTarget::bind(',
      'auto ServiceReportTarget::apply(', 'void ServiceReportTarget::cancel(',
      'void ServiceReportTarget::add_current_locked(', 'void ServiceReportTarget::observe_reports_locked(',
      'auto charge_service_cpu(', 'auto selftest_service_census()']
    for anchor in anchors:
        body,row=runner.capture(source,anchor);definitions.append(body);spans.append(row)
    # Capture the genuine full diagnostic definitions, not their Normal alternatives.
    first=source.index('auto RemoteServiceAccounting::create()')
    stop=source.index('\n#else\nauto RemoteServiceAccounting::create()',first)
    subset=source[first:stop]
    remotes=[]
    for anchor in ['auto RemoteServiceAccounting::create()', 'void RemoteServiceAccounting::retain()', 'void RemoteServiceAccounting::release()']:
        body,row=runner.capture(subset,anchor);row['line']+=source.count('\n',0,first);remotes.append(body);spans.append(row)
    # All original complete definition bytes are staged without substitutions.
    (scratch/'harness.cpp').write_text(PRELUDE+globals_body+'\n'+'\n\n'.join(remotes+definitions)+'\n'+TESTS)
    return spans


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--service-source',type=Path,default=KERN/'platform/sched/service_resource.cpp')
    parser.add_argument('--evidence-dir',type=Path)
    parser.add_argument('--cxx',default='/usr/bin/clang++')
    args=parser.parse_args()
    # The shared bounded helper is executed from exactly bound source bytes.
    runner_bytes=RUNNER.read_bytes();spec=importlib.util.spec_from_file_location('wos_service_epoch_bounded',RUNNER)
    runner=importlib.util.module_from_spec(spec);exec(compile(runner_bytes,str(RUNNER),'exec'),runner.__dict__)
    imported={'path':str(RUNNER),'bytes':len(runner_bytes),'sha256':hashlib.sha256(runner_bytes).hexdigest()}
    start=time.monotonic();deadline=start+60;groups=[];scratch=None;identity=None;staged=[];staged_before=[]
    out={'pass':False,'errors':[],'commands':[],'native_runtime_qualified':False,
         'primitive_seams':['Host Task facade, not Task ABI/scheduler/lifetime','sequential Host Spinlock/IRQ/preemption primitive','hosted allocation/deallocation'],
         'imported_runner_source_snapshot':imported,'external_dependencies_before':False,'external_dependencies_after_only':True}
    evidence=None
    if args.evidence_dir is not None:
        evidence=args.evidence_dir.absolute();evidence.mkdir(mode=0o700)
    selected=args.service_source.resolve(strict=True);compiler=Path(args.cxx).resolve(strict=True)
    headers=[KERN/p for p in ['platform/mm/lifecycle_census.hpp','platform/sched/service_resource.hpp','platform/sched/resource_model.hpp','platform/sched/service_resource_model.hpp','abi/resource.hpp','abi/service_resource.hpp']]
    watched=[selected,*headers,Path(__file__).resolve(),RUNNER,compiler];before=[runner.pin(p) for p in watched];out['source_before']=before
    handlers={sig:signal.getsignal(sig) for sig in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP)}
    def interrupted(sig,_):raise KeyboardInterrupt(f'signal {sig}')
    try:
        for sig in handlers:signal.signal(sig,interrupted)
        resource.setrlimit(resource.RLIMIT_CORE,(0,0));runner.subreaper()
        if runner.pin(RUNNER)!=imported:raise ValueError('imported helper changed')
        with compiler.open('rb') as f:
            if f.read(4)!=b'\x7fELF':raise ValueError('Host compiler must be explicit ELF')
        scratch=Path(tempfile.mkdtemp(prefix='wos-service-epoch-',dir='/tmp'));s=scratch.stat();identity=(s.st_dev,s.st_ino,s.st_uid)
        out['actual_definition_spans']=generate(runner,scratch,selected)
        staged=sorted(p for p in scratch.rglob('*') if p.is_file());staged_before=[runner.pin(p) for p in staged]
        out['staged_sources_before']=staged_before
        binary=scratch/'service-epoch';dep=scratch/'dependencies.d'
        command=[str(compiler),'-std=c++23','-O1','-g0','-pipe','-DWOS_SELFTEST=1','-fsanitize=address,undefined','-fno-omit-frame-pointer','-MD','-MF',str(dep),'-I',str(scratch/'include'),'-I',str(KERN),str(scratch/'harness.cpp'),'-o',str(binary)]
        for n,argv in enumerate([command,[str(binary)]]):
            log=scratch/f'command-{n}.log';row=runner.command(argv,log,scratch,deadline,groups);out['commands'].append(row)
            if n==0 and dep.exists():out['emitted_dependency_pins_after']=runner.dependency_pins(dep)
            text=log.read_text(errors='replace');print(text[-32768:],end='',flush=True)
            if row['exit']!=0:raise ValueError(f'actual command {n} exit {row["exit"]}')
            if n==1 and not re.fullmatch(r'PASS assertions=\d+ failures=0 bills=1004 real_epoch=1 real_charge=1 real_census=1 real_cpu_model=1 native_qualification=0\n',text):raise ValueError('exact positive actual-body oracle missing')
        out['pass']=True
    except (OSError,ValueError,TimeoutError,KeyboardInterrupt) as error:out['errors'].append(f'{type(error).__name__}: {error}')
    finally:
        for sig in handlers:signal.signal(sig,signal.SIG_IGN)
        try:
            out['source_after']=[runner.pin(p) for p in watched];out['source_held']=before==out['source_after']
            if not out['source_held']:out['errors'].append('actual source/tool/runner changed')
            if scratch:
                out['staged_sources_after']=[runner.pin(p) for p in staged]
                out['staged_sources_held']=staged_before==out['staged_sources_after']
                if not out['staged_sources_held']:out['errors'].append('staged source changed')
                if evidence is not None:
                    saved=[]
                    for p in [*staged,*sorted(scratch.glob('command-*.log')),scratch/'dependencies.d']:
                        if not p.is_file():continue
                        if p.stat().st_size>runner.LOG_BUDGET:raise ValueError('retained file exceeded1MiB')
                        target=evidence/p.relative_to(scratch);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(p.read_bytes());saved.append(runner.pin(target))
                    out['retained_small_evidence']=saved
                out['owned_groups_pending']=groups[:]
                s=scratch.stat()
                if groups or scratch.is_symlink() or (s.st_dev,s.st_ino,s.st_uid)!=identity or s.st_uid!=os.getuid():raise ValueError('owned cleanup unproven')
                out['scratch_allocated_bytes']=runner.allocated(scratch);shutil.rmtree(scratch);out['scratch_removed']=not scratch.exists()
        except (OSError,ValueError,TimeoutError) as error:out['errors'].append(f'cleanup: {error}')
        for sig,handler in handlers.items():signal.signal(sig,handler)
    out['seconds']=time.monotonic()-start
    out['pass']=out['pass'] and not out['errors'] and out.get('scratch_removed',False) and out['seconds']<60
    out['qualification']='PASS_ACTUAL_HOST_SERVICE_OWNER_EPOCH_ONLY' if out['pass'] else 'FAIL'
    if evidence is not None:(evidence/'result.json').write_text(json.dumps(out,indent=2)+'\n')
    print(json.dumps({'pass':out['pass'],'errors':out['errors'],'seconds':out['seconds'],'scratch_removed':out.get('scratch_removed',False),'owned_groups_pending':out.get('owned_groups_pending',[]),'source_held':out.get('source_held',False)}))
    return 0 if out['pass'] else 1
if __name__=='__main__':raise SystemExit(main())
