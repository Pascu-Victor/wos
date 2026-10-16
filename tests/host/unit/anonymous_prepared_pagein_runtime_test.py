#!/usr/bin/env python3
"""Private actual-transaction/factory Host check; no native execution."""
from pathlib import Path
import argparse
import sys
sys.dont_write_bytecode = True
import hashlib,importlib.util,json,os,re,shlex,shutil,signal,tempfile,time,traceback
ap=argparse.ArgumentParser(description=__doc__)
ap.add_argument('--root',type=Path)
ap.add_argument('--virt-source',type=Path)
ap.add_argument('--host-build',required=True,type=Path)
ap.add_argument('--allocator-source',type=Path)
ap.add_argument('--evidence-dir',type=Path)
opt=ap.parse_args()
R=(opt.root or next(p for p in Path(__file__).resolve().parents if (p/'modules/kern/src').is_dir())).resolve()
K=R/'modules/kern/src';D=Path(__file__).resolve().parent
SOURCE=(opt.virt_source or K/'platform/mm/virt.opt.cpp').resolve()
DB=(opt.host_build/'compile_commands.json').resolve()
spec=importlib.util.spec_from_file_location('process',R/'tests/host/unit/file_mmap_registry_runtime_test.py');process=importlib.util.module_from_spec(spec);spec.loader.exec_module(process)
process.FILE_BUDGET=32*1024**2;process.SCRATCH_BUDGET=96*1024**2
ephemeral=opt.evidence_dir is None
OUT=Path(tempfile.mkdtemp(prefix='wos-anonymous-pagein-evidence-')) if ephemeral else opt.evidence_dir.resolve()
if not ephemeral: OUT.mkdir(parents=True,exist_ok=False)
T=Path(tempfile.mkdtemp(prefix='scratch-',dir=OUT));start=time.monotonic();commands=[];groups=[];errors=[];before=[];after=[];outputs=[];fixtures=[]
def pin(p):
 p=Path(p).resolve();s=p.stat();b=p.read_bytes();t=p.stat();assert(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)==(t.st_dev,t.st_ino,t.st_size,t.st_mtime_ns,t.st_ctime_ns);return dict(path=str(p),bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
def extract(source,name):
 m=re.search(r'^(?:\[\[(?:nodiscard|noreturn)\]\] )?(?:auto|void|bool) '+name+r'\([^;{]*\{',source,re.M);assert m,name
 p=source.index('{',m.start())+1;depth=1
 while depth:depth+=(source[p]=='{')-(source[p]=='}');p+=1
 return source[m.start():p]
def method(s,name):
 i=s.index(name);start=s.index('{',i)+1;end=start;n=1
 while n:n+=(s[end]=='{')-(s[end]=='}');end+=1
 return s[i:end]
def between(s,a,b):return s[s.index(a):s.index(b)]
def run(label,args,expect=0):
 global owned_command_active,pending_signal
 owned_command_active=True
 try:row=process.command(list(map(str,args)),OUT/(label+'.log'),T,start+120,groups)
 finally:
  owned_command_active=False
  if pending_signal is not None:
   number=pending_signal;pending_signal=None;raise KeyboardInterrupt('deferred signal '+str(number))
 row['label']=label;commands.append(row)
 if(row['exit']==0)!=(expect==0):raise RuntimeError(label+': '+(OUT/(label+'.log')).read_text(errors='replace')[-7500:])
 if process.allocated(OUT)>96*1024**2:raise RuntimeError('96MiB cap')
owned_command_active=False;pending_signal=None
handlers={s:signal.getsignal(s)for s in(signal.SIGINT,signal.SIGTERM,signal.SIGHUP)}
def interrupted(s,f):
 global pending_signal
 if owned_command_active:
  pending_signal=s;return
 raise KeyboardInterrupt('signal '+str(s))
try:
 for s in handlers:signal.signal(s,interrupted)
 process.subreaper()
 source=SOURCE;header=source.with_name('virt.hpp');template=D/'anonymous_prepared_pagein_fixture.cpp.in';factory=(opt.allocator_source or K/'platform/mm/numa_alloc.cpp').resolve()
 text=source.read_text();tpl=template.read_text();scheduler=(K/'platform/sched/scheduler.hpp').read_text()
 m=re.search(r'struct PreemptGuard \{[\s\S]*?\n\};',scheduler);assert m
 shim=T/'shim/platform/sched';shim.mkdir(parents=True);(shim/'scheduler.hpp').write_text('#pragma once\nnamespace ker::mod::sched {void preempt_disable();void preempt_enable();\n'+m[0]+'\n}\n')
 types=between(text,'enum class AnonymousSwapRuntimeState','constexpr size_t ANONYMOUS_PAGEIN_QUEUE_CAPACITY')
 core_names=['next_anonymous_swap_generation','find_anonymous_swap_mapping_locked','insert_anonymous_swap_mapping_locked','remove_anonymous_swap_mapping_locked','anonymous_swap_marker_raw','anonymous_pagein_enqueue','anonymous_pagein_dequeue','release_anonymous_swap_object','detach_anonymous_swap_object_locked','anonymous_swap_waiter_idle_locked','detach_anonymous_swap_waiters_locked','wake_anonymous_swap_waiters','anonymous_swap_worker_admitted_locked','anonymous_swap_worker_admitted','valid_anonymous_placement','legacy_anonymous_worker','anonymous_allocation_error','capture_anonymous_snapshot_locked','anonymous_root_matches_locked','probe_anonymous_swap_fault_locked','probe_anonymous_swap_fault']
 core=between(text,'struct AnonymousPageinSnapshot {','auto valid_anonymous_placement')+between(text,'enum class AnonymousSwapFaultDisposition','auto probe_anonymous_swap_fault_locked')+'\n'.join(extract(text,n)for n in core_names)
 op=between(text,'struct AnonymousPageinOperation {','#ifdef WOS_ASYNC_BLOCK_IO\nconstexpr size_t ANONYMOUS_PAGEIN_WINDOW')+between(text,'struct AnonymousPageinCompletion {','auto finish_anonymous_pagein(')
 replace={'ACTUAL_FACTORY':str(factory),'DEMAND_TYPES':'enum class DemandOrigin : uint8_t { HardwareFault, SoftwareEnsure };\n'+between(text,'struct DemandContext {','static_assert(alignof(DemandContext)'),'DEMAND_STORAGE':between(text,'class DemandContextStorage {','struct DemandCounters {'),'SWAP_TYPES':types,'CORE_FUNCTIONS':core,'OPERATION_TYPES':op,'PAGEIN_FUNCTIONS':'\n'.join(extract(text,n)for n in ['begin_anonymous_pagein','finish_anonymous_pagein','service_anonymous_pagein']),'MIGRATE_CLONE_PURGE':'\n'.join(extract(text,n)for n in ['migrate_anonymous_swap_area','clone_anonymous_swap_mappings','purge_anonymous_swap_mappings']), 'PAGEOUT_TYPES':between(text,'enum class AnonymousPageoutOrigin','auto snapshot_anonymous_candidate('), 'PAGEOUT_FUNCTIONS':'\n'.join(extract(text,n)for n in ['pageout_anonymous_candidate','anonymous_swap_reclaim_count','anonymous_swap_reclaim_scan']), 'WORKER_FUNCTIONS':extract(text,'anonymous_pagein_worker_main')+'\n'+extract(text,'start_anonymous_swap_worker'), 'ACTUAL_TRY_ACQUIRE':method((K/'platform/sched/task.hpp').read_text(),'bool try_acquire()'), 'STACK_BYTES':re.search(r'KERNEL_STACK_SIZE = ([^;]+);',(K/'platform/mm/mm.hpp').read_text())[1], 'STARTUP_TASK_ENDPOINTS':(D/'anonymous_readiness_endpoints.cpp.in').read_text(), 'STARTUP_CASES':(D/'anonymous_readiness_cases.cpp.in').read_text(), 'SNAPSHOT_ENDPOINT':(D/'anonymous_readiness_snapshot.cpp.in').read_text(), 'ACTUAL_WINDOW':between(text,'#ifdef WOS_ASYNC_BLOCK_IO\nconstexpr size_t ANONYMOUS_PAGEIN_WINDOW','void begin_anonymous_pagein('), 'ACTUAL_FULL_DESTROY':extract((K/'platform/sched/task.cpp').read_text(),'destroy_unpublished_user_thread'), 'ACTUAL_USERCOPY_TASK_METHODS':'\n'.join(extract((K/'platform/sched/task.cpp').read_text(),n)for n in ['Task::try_acquire_usercopy_pagemap','Task::release_usercopy_pagemap']), 'CURRENT_PREPARE_FUNCTIONS':between(text,'enum class AnonymousCurrentSurvey','auto current_anonymous_wait_context')+'\n'.join(extract(text,n)for n in ['current_anonymous_wait_context','cancel_current_anonymous_wait','wait_current_anonymous_page','capture_current_anonymous_user_page','prepare_current_anonymous_user_page']), 'CURRENT_PREPARE_CASES':(D/'anonymous_current_prepare_cases.inc').read_text(), 'CURRENT_WAIT_CASES':(D/'anonymous_current_wait_cases.inc').read_text()}
 for a,b in replace.items():assert tpl.count(a)==1,(a,tpl.count(a));tpl=tpl.replace(a,b)
 hostdb=DB;row=next(x for x in json.loads(hostdb.read_text())if x['file'].endswith('/page_allocator_interval_test.cpp'));args=shlex.split(row['command']);base=[args[0],'-I'+str(K/'platform/mm'),'-I'+str(T/'shim'),'-I'+str(K),'-I'+str(R/'tests/host/kern_shim'),'-I'+str(K/'std'),'-I'+str(R/'modules'),'-std=gnu++23','-DWOS_HOST_TEST=1','-g','-O1','-fno-omit-frame-pointer','-fsanitize=address,undefined','-fno-sanitize=vptr','-Wall','-Wextra']
 normal=T/'actual.cpp';normal.write_text(tpl);(OUT/'actual.cpp').write_text(tpl)
 dep=T/'actual.d';run('dependencies',base+['-M','-MF',dep,'-MT','out',normal]);deps={Path(x).resolve()for x in shlex.split(dep.read_text().replace('\\\n',' ').split(':',1)[1])if not str(Path(x).resolve()).startswith(str(T))};deps.update([source,header,template,factory,K/'platform/sched/scheduler.hpp',Path(__file__),hostdb,R/'tests/host/unit/file_mmap_registry_runtime_test.py', D/'anonymous_readiness_endpoints.cpp.in',D/'anonymous_readiness_cases.cpp.in',D/'anonymous_readiness_snapshot.cpp.in',D/'anonymous_readiness_before.cpp.in',D/'anonymous_current_prepare_cases.inc',D/'anonymous_current_wait_cases.inc',K/'platform/sched/task.cpp',K/'platform/sched/task.hpp',K/'platform/mm/mm.hpp']);before=[pin(p)for p in sorted(deps)]
 run('compile-actual',base+[normal,'-o',T/'actual']);run('execute-actual',[T/'actual'])
 assert 'PASS actual anonymous pagein cases=' in (OUT/'execute-actual.log').read_text()
 assert 'PASS actual anonymous readiness' in (OUT/'execute-actual.log').read_text()
 assert 'PASS actual ordinary preparation' in (OUT/'execute-actual.log').read_text()
 assert 'PASS actual current continuation' in (OUT/'execute-actual.log').read_text()
 # Actual extracted destructive-failure and UMA-mode negative controls.
 mutations=[('wrong-legacy-mode','topology->node_count == 1','topology->node_count >= 1'),('destructive-preio-failure','mapping->state = AnonymousSwapRuntimeState::FAILED;\n                mapping->error = result < 0 ? result : -EIO;\n                anonymous_swap_stats.swapped_pages','object_to_release = detach_anonymous_swap_object_locked(mapping);\n                mapping->state = AnonymousSwapRuntimeState::FAILED;\n                mapping->error = result < 0 ? result : -EIO;\n                anonymous_swap_stats.swapped_pages')]
 for label,a,b in mutations:
  assert tpl.count(a)==1,(label,tpl.count(a));p=T/(label+'.cpp');p.write_text(tpl.replace(a,b));run('compile-'+label,base+[p,'-o',T/label]);run('execute-'+label,[T/label],expect=1)
  expected='FAIL NUMA default prepares' if label=='wrong-legacy-mode' else 'FAIL terminal failure retains stored ref'
  assert expected in (OUT/('execute-'+label+'.log')).read_text(), 'negative control failed for an unexpected reason'
 for label,name in [('old-unchecked-post','start_anonymous_swap_worker'),('old-unheld-scan','anonymous_swap_reclaim_scan')]:
  original=extract((Path(__file__).parents[4]/'before/modules/kern/src/platform/mm/virt.opt.cpp').read_text(),name) if False else extract((Path(__file__).parent/'anonymous_readiness_before.cpp.in').read_text(),name)
  actual=extract(text,name);assert tpl.count(actual)==1
  p=T/(label+'.cpp');p.write_text(tpl.replace(actual,original));run('compile-'+label,base+[p,'-o',T/label]);run('execute-'+label,[T/label],expect=1)
  expected='private boot ref and shutdown hold before post' if label=='old-unchecked-post' else 'source hold survives slot contention'
  assert expected in (OUT/('execute-'+label+'.log')).read_text(), 'predecessor control failed unexpectedly'
  if label=='old-unheld-scan':
   run('execute-old-unheld-postwrite',[T/label,'--post-write-only'],expect=1)
   assert 'source hold survives post-write cleanup' in (OUT/'execute-old-unheld-postwrite.log').read_text()
 for label,a,b,expected in [
  ('ordinary-boot-bypass','if (EMPTY) return 0;','if (!anonymous_swap_enabled()) return 0;','anonOFF tracked page uses actual ordinary service'),
  ('ordinary-wrong-policy','numa::capture_allocation_context(*copied_policy, placement)','numa::capture_allocation_context(numa::Policy{}, placement)','ordinary copied actor BIND confinement')]:
  assert tpl.count(a)==1,(label,tpl.count(a));p=T/(label+'.cpp');p.write_text(tpl.replace(a,b));run('compile-'+label,base+[p,'-o',T/label]);run('execute-'+label,[T/label],expect=1)
  assert expected in (OUT/('execute-'+label+'.log')).read_text(),'ordinary mutant wrong failure'
 # Each new control mutates an actual selected producer/continuation body;
 # all old vectors and controls above remain unchanged.
 wait_mutations=[
  ('wait-early-completion','detach_anonymous_swap_waiters_locked',
   'task->anonymous_swap_wait_mapping = nullptr;',
   'task->anonymous_swap_wait_mapping = nullptr; task->anonymous_swap_completion_generation.store(task->anonymous_swap_wait_generation, std::memory_order_release);',
   'detached head and tail cannot be reused before retirement'),
  ('wait-idle-pointer-only','anonymous_swap_waiter_idle_locked',
   '(task.anonymous_swap_wait_generation == 0 ||\n            task.anonymous_swap_completion_generation.load(std::memory_order_acquire) == task.anonymous_swap_wait_generation)',
   'true','detached head and tail cannot be reused before retirement'),
  ('wait-cancel-no-reset','cancel_current_anonymous_wait',
   'actor.anonymous_swap_wait_generation = 0;','/* omitted cancellation reset */',
   'linked cancellation returns only own ref and resets idle generation without completion forgery'),
  ('wait-late-link-clear','wake_anonymous_swap_waiters',
   'task->anonymous_swap_wait_next = nullptr;','/* incorrectly clear after canonical wake */',
   'wake may reuse retired node before prior off-lock ref return'),
  ('wait-no-pageout-commit','pageout_anonymous_candidate',
   'if (committed) waiters = detach_anonymous_swap_waiters_locked(mapping);',
   'if (false) waiters = detach_anonymous_swap_waiters_locked(mapping);',
   'PAGEOUT terminal detach and completion cover both outcomes'),
  ('wait-no-pageout-rollback','pageout_anonymous_candidate',
   'waiters = detach_anonymous_swap_waiters_locked(mapping);\n            remove_anonymous_swap_mapping_locked(mapping);',
   'waiters = nullptr;\n            remove_anonymous_swap_mapping_locked(mapping);',
   'PAGEOUT terminal detach and completion cover both outcomes'),
  ('wait-second-issue','prepare_current_anonymous_user_page',
   'if (issued) return -EAGAIN;','/* incorrectly issue twice */',
   'generation race resets candidate without second issue'),
  ('wait-invalid-generation-publication','probe_anonymous_swap_fault_locked',
   'if (GENERATION == 0 || GENERATION == UINT64_MAX) return AnonymousSwapFaultDisposition::FAILED;',
   '/* incorrectly publish invalid generation */',
   'invalid new hardware generation refuses before gate frame or queue publication')]
 for label,name,a,b,expected in wait_mutations:
  original=extract(text,name);assert original.count(a)==1,(label,original.count(a))
  changed=original.replace(a,b)
  if label=='wait-late-link-clear':
   changed=changed.replace('sched::wake_task_from_event(task);','sched::wake_task_from_event(task); task->anonymous_swap_wait_next = nullptr;')
  assert tpl.count(original)==1,(label,'actual body selection')
  p=T/(label+'.cpp');p.write_text(tpl.replace(original,changed));run('compile-'+label,base+[p,'-o',T/label]);run('execute-'+label,[T/label],expect=1)
  assert expected in (OUT/('execute-'+label+'.log')).read_text(),'continuation mutant wrong failure'
 after=[pin(p)for p in sorted(deps)];assert before==after,'source drift' 
except BaseException as e:errors.append(type(e).__name__+': '+str(e));(OUT/'failure01.txt').write_text(traceback.format_exc())
finally:
 for s in handlers:signal.signal(s,signal.SIG_IGN)
 scratch_removed=False;preserved=None;removed=0
 try:
  try:
   if before and not after:after=[pin(p['path'])for p in before]
   if before and before!=after:errors.append('source changed')
  except BaseException as e:errors.append('recheck '+str(e))
  if groups:errors.append('unproved owned groups');preserved=str(T)
  else:
   try:removed=process.allocated(T);shutil.rmtree(T);scratch_removed=not T.exists()
   except BaseException as e:errors.append('cleanup '+str(e));preserved=str(T)
  result=dict(qualification='PASS_ACTUAL_ANONYMOUS_PAGEIN_HOST_SEAMS_ONLY'if not errors else'FAIL_PRIVATE_ANONYMOUS_PAGEIN_HOST',errors=errors,commands=commands,input_before=before,input_after=after,source_held=bool(before)and before==after,scratch_removed=scratch_removed,preserved_scratch=preserved,scratch_allocated_removed=removed,owned_groups_pending=groups[:],seconds=time.monotonic()-start,scope='Actual extracted transactions, actual numa_alloc.cpp+OwnedSpan+policy selection. Explicit Task/lock/root/PTE/I-O/physical ledger endpoints; not native scheduler/reclaim/TLB/SMP.')
  (OUT/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:result[k]for k in['qualification','errors','source_held','scratch_removed','seconds']}))
 finally:
  for s,h in handlers.items():signal.signal(s,h)
 if ephemeral and scratch_removed and not groups:
  shutil.rmtree(OUT)
raise SystemExit(bool(errors)or not scratch_removed or bool(groups))
