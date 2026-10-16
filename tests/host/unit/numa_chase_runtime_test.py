#!/usr/bin/env python3
"""Compile actual benchmark/helpers with explicit WOS Host endpoints."""
from pathlib import Path
import argparse,copy,hashlib,importlib.util,json,math,os,shlex,shutil,signal,sys,tempfile,time,traceback
sys.dont_write_bytecode=True
parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path);parser.add_argument('--bench-source',type=Path);parser.add_argument('--cxx',required=True);parser.add_argument('--evidence-dir',type=Path);args=parser.parse_args()
R=(args.root or next(p for p in Path(__file__).resolve().parents if(p/'modules/kern/src').is_dir())).resolve();S=(args.bench_source or R/'modules/testprog/src/numa_bench.cpp').resolve();U=Path(__file__).resolve().parent
# Load dependency definitions before allocating any owned output/scratch.
def module(name,path):
 spec=importlib.util.spec_from_file_location(name,path);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value
owned=module('owned',R/'tests/host/unit/file_mmap_registry_runtime_test.py');codec=module('codec',U/'validate_numa_chase.py');owned.FILE_BUDGET=64*1024**2;owned.SCRATCH_BUDGET=128*1024**2
fresh=args.evidence_dir is None;O=Path(tempfile.mkdtemp(prefix='wos-numa-chase-evidence-'))if fresh else args.evidence_dir.resolve()
if not fresh:O.mkdir(parents=True,exist_ok=False)
T=Path(tempfile.mkdtemp(prefix='scratch-',dir=O));start=time.monotonic();groups=[];commands=[];errors=[];a=[];b=[];checks=[];deps=set()
def pin(p):
 p=Path(p).resolve();x=p.stat();d=p.read_bytes();y=p.stat();assert(x.st_dev,x.st_ino,x.st_size,x.st_mtime_ns,x.st_ctime_ns)==(y.st_dev,y.st_ino,y.st_size,y.st_mtime_ns,y.st_ctime_ns);return dict(path=str(p),bytes=len(d),sha256=hashlib.sha256(d).hexdigest())
def run(label,argv,expect=0):
 row=owned.command(list(map(str,argv)),O/(label+'.log'),T,start+120,groups);commands.append(dict(label=label,**row));assert(row['exit']==0)==(expect==0),(label,(O/(label+'.log')).read_text(errors='replace')[-6000:]);assert owned.allocated(O)<=128*1024**2
 return (O/(label+'.log')).read_text()
def check(label,condition):checks.append(dict(check=label,passed=bool(condition)));assert condition,label
handlers={s:signal.getsignal(s)for s in [signal.SIGINT,signal.SIGTERM,signal.SIGHUP]}
def interrupted(s,f):raise KeyboardInterrupt(str(s))
def records_text(rows):return ''.join(codec.PREFIX+json.dumps(r,separators=(',',':'))+'\n'for r in rows)
try:
 for s in handlers:signal.signal(s,interrupted)
 owned.subreaper();shim=T/'shim/sys';shim.mkdir(parents=True)
 abi=R/'toolchain/src/mlibc/sysdeps/wos/include/sys/numa.h'
 (shim/'numa.h').write_text('#include '+json.dumps(str(abi))+'\n')
 (shim/'multiproc.h').write_text('#pragma once\n#include <cstdint>\nnamespace ker::multiproc {uint64_t currentThreadId();uint64_t getcurrent_cpu();uint64_t yield();int64_t setThreadAffinityMask(uint64_t,uint64_t);int64_t getThreadAffinityMask(uint64_t);}\n')
 (shim/'vmem.h').write_text('#pragma once\n#include <sys/numa.h>\nnamespace ker::vmem {int64_t query_numa(const ker::abi::vmem::numa::QueryRequest&);int64_t set_memory_policy(const ker::abi::vmem::numa::PolicyRequest&);}\n')
 fixture=T/'fixture.cpp';fixture.write_text((U/'numa_chase_fixture.cpp.in').read_text().replace('ACTUAL_BENCH_HEADER',str(S.with_name('numa_bench.hpp'))))
 core=T/'core.cpp';core.write_text((U/'numa_chase_core_test.cpp.in').read_text().replace('ACTUAL_CORE_HEADER',str(S.with_name('numa_bench_core.hpp'))))
 compiler=Path(shutil.which(args.cxx)or args.cxx).resolve();base=[compiler,'-std=c++23','-g','-O1','-fno-omit-frame-pointer','-fsanitize=address,undefined','-fno-sanitize=vptr','-Wall','-Wextra','-Werror','-pthread','-I'+str(T/'shim'),'-I'+str(S.parent)]
 deps={S,S.with_name('numa_bench.hpp'),S.with_name('numa_bench_core.hpp'),Path(__file__),U/'numa_chase_fixture.cpp.in',U/'numa_chase_core_test.cpp.in',U/'validate_numa_chase.py',R/'tests/host/unit/file_mmap_registry_runtime_test.py',compiler,abi}
 for label,source in [('actual',S),('fixture',fixture),('core',core)]:
  dep=T/(label+'.d');run('deps-'+label,base+['-M','-MF',dep,'-MT','out',source]);deps.update(Path(p).resolve()for p in shlex.split(dep.read_text().replace('\\\n',' ').split(':',1)[1])if not Path(p).resolve().is_relative_to(T))
 a=[pin(p)for p in sorted(deps)]
 run('compile-actual',base+['-c',S,'-o',T/'actual.o']);run('compile-fixture',base+['-c',fixture,'-o',T/'fixture.o'])
 wrap=['-Wl,--wrap='+n for n in ['mmap','munmap','pthread_create','pthread_join','clock_getres','_ZnamRKSt9nothrow_t','pthread_mutex_init','pthread_mutex_destroy','pthread_cond_init','pthread_cond_destroy','pthread_cond_broadcast','pthread_mutex_unlock','fflush']]
 run('link-actual',base+[T/'actual.o',T/'fixture.o',*wrap,'-Wl,-t','-o',T/'actual'])
 run('compile-core',base+[core,'-o',T/'core']);corelog=run('execute-core',[T/'core']);check('actual pure helper executable passed','PASS actual permutation/dependent/options helpers' in corelog)
 decoded={}
 for case in ['happy','cpu63','control-init','partial-create','map-oom','bad-node','bad-reserved','clock-error','scratch-oom','placement-timeout','late-broadcast','late-unlock']:
  log=run('execute-'+case,[T/'actual',case]);check(case+' real-owner closure', 'PASS actual benchmark Host ports' in log)
  if case in ['happy','cpu63']:
   decoded[case]=codec.decode(log,require_default=False);check(case+' actual raw smoke decoder',decoded[case]['end']['joined_threads']==3)
  else:
   try:codec.decode(log,require_default=False)
   except ValueError:check(case+' saved refusal',True)
   else:check(case+' saved refusal',False)
 # Genuine one-line actual-source controls, compiled and executed independently.
 source=S.read_text();core_source=S.with_name('numa_bench_core.hpp').read_text()
 for label,needle,replacement,case,reason in [
  ('nondependent-loop','cursor = *reinterpret_cast<const volatile uintptr_t*>(cursor);','(void)*reinterpret_cast<const volatile uintptr_t*>(cursor);','happy','"status":-84'),
  ('placement-waiver','if (row.node != expected) return -ERANGE;','if (false && row.node != expected) return -ERANGE;','bad-node','actual run status against injected endpoint')]:
  M=T/label;M.mkdir();(M/'numa_bench.hpp').write_bytes(S.with_name('numa_bench.hpp').read_bytes())
  cs=core_source;ss=source
  if label=='nondependent-loop':assert cs.count(needle)==1;cs=cs.replace(needle,replacement)
  else:assert ss.count(needle)==1;ss=ss.replace(needle,replacement)
  (M/'numa_bench_core.hpp').write_text(cs);(M/'numa_bench.cpp').write_text(ss)
  (O/(label+'-source.cpp')).write_text(ss);(O/(label+'-core.hpp')).write_text(cs)
  run('compile-'+label,base+['-c',M/'numa_bench.cpp','-o',M/'actual.o']);run('link-'+label,base+[M/'actual.o',T/'fixture.o',*wrap,'-o',M/'mutant'])
  log=run('execute-'+label,[M/'mutant',case],expect=1);check(label+' compiled counterexample intended failure',reason in log)
 # Genuine predecessor check: only remove the newly added post-join error
 # consumption; the endpoint proves completion was already observed.
 final_source=S.read_text();begin=final_source.index("    // Completion publication precedes the worker's final broadcast/unlock")
 end=final_source.index('    bool controls_closed = joined == created;',begin)
 old_source=final_source[:begin]+final_source[end:]
 M=T/'old-late-completion';M.mkdir();(M/'numa_bench.hpp').write_bytes(S.with_name('numa_bench.hpp').read_bytes());(M/'numa_bench_core.hpp').write_bytes(S.with_name('numa_bench_core.hpp').read_bytes());(M/'numa_bench.cpp').write_text(old_source)
 (O/'old-late-completion-source.cpp').write_text(old_source)
 run('compile-old-late-completion',base+['-c',M/'numa_bench.cpp','-o',M/'actual.o']);run('link-old-late-completion',base+[M/'actual.o',T/'fixture.o',*wrap,'-o',M/'old'])
 for case in ['late-broadcast','late-unlock']:
  log=run('execute-old-'+case,[M/'old',case],expect=1);check(case+' genuine predecessor incorrectly returns success','actual run status against injected endpoint' in log and '"kind":"end","status":0' in log)
 # Synthetic20-row strict-codec and analytic tests are protocol evidence only.
 happy=decoded['happy'];profile=copy.deepcopy(happy['profile']);profile.update(codec.DEFAULT);expected=codec.expected_indices(profile['bytes'],profile['loads'],profile['seed']);rows=[profile];sample_rows=[]
 for pair in range(10):
  for condition in (['local','remote']if pair%2==0 else['remote','local']):
   row=copy.deepcopy(next(x for x in happy['samples']if x['condition']==condition));node=profile['remote_node']if condition=='remote'else profile['local_node'];cpu=profile['remote_init_cpu']if condition=='remote'else profile['local_init_cpu']
   row.update({k:profile[k]for k in ['seed','bytes','stride','loads','chase_cpu','chase_node','node_count','clock_resolution_ns']});row.update(pair=pair,order='LR'if pair%2==0 else'RL',condition=condition,elapsed_ns=(pair+1)*profile['loads']*(2 if condition=='remote'else 1),start_index=expected[0],warm_checksum=expected[0],checksum=expected[1],warm_loads=profile['bytes']//64,init_cpu=cpu,expected_node=node,cpu_before=profile['chase_cpu'],cpu_after=profile['chase_cpu'],mask_before=1<<profile['chase_cpu'],mask_after=1<<profile['chase_cpu'],init_cpu_before=cpu,init_cpu_after=cpu,init_mask_before=1<<cpu,init_mask_after=1<<cpu,pages=profile['bytes']//4096,virgin_pages=profile['bytes']//4096)
   row['before_histogram']=[0]*64;row['before_histogram'][node]=row['pages'];row['after_histogram']=row['before_histogram'][:];sample_rows.append(row)
 rows+=sample_rows
 for row in sample_rows:rows.append(dict(version=1,kind='cleanup',pair=row['pair'],condition=row['condition'],unmapped=True,error=0,created_threads=3,joined_threads=3))
 rows.append(dict(version=1,kind='end',status=0,samples=20,completed_pairs=10,created_threads=3,joined_threads=3,mappings_created=20,mappings_unmapped=20,peak_mappings=20))
 text=records_text(rows);(O/'synthetic-codec20.txt').write_text(text)
 # Exercise real saved-file CLI, including universal-newline predecessor.
 old_cli=T/'old-newline-cli.py';new_cli=U/'validate_numa_chase.py';current_cli=new_cli.read_text();needle="args.input.read_bytes().decode('utf-8', errors='strict')";assert current_cli.count(needle)==1;old_cli.write_text(current_cli.replace(needle,'args.input.read_text()',1));(O/'old-newline-cli.py').write_text(old_cli.read_text())
 for label,raw in [('LF',text.encode()),('CRLF',text.replace('\n','\r\n').encode()),('bareCR',text.replace('\n','\r').encode())]:
  saved=O/('saved-'+label+'.txt');saved.write_bytes(raw)
  log=run('cli-new-'+label,[sys.executable,'-B',new_cli,saved],expect=0 if label=='LF'else 1);check('new CLI '+label+' raw boundary',('PASS_SAVED_EMULATED_NUMA_POINTER_CHASE_PROTOCOL_ONLY' if label=='LF'else 'FAIL_SAVED_NUMA_CHASE_PROTOCOL')in log)
  log=run('cli-old-'+label,[sys.executable,'-B',old_cli,saved]);check('old CLI '+label+' accepts normalized file','PASS_SAVED_EMULATED_NUMA_POINTER_CHASE_PROTOCOL_ONLY'in log)
 value=codec.decode(text);check('analytic ratios/medians',value['median_paired_ratio']==2 and value['ratio_of_medians']==2)
 stats=codec.summary([1.,2.,3.]);check('analytic sample variance/stddev/CV',stats['sample_variance']==1 and stats['sample_stddev']==1 and stats['coefficient_of_variation']==0.5 and stats['median']==2 and stats['minimum']==1 and stats['maximum']==3)
 mutants=[]
 for label,fn in [('missing',lambda r:r.pop(2)),('duplicate',lambda r:r.insert(2,copy.deepcopy(r[1]))),('cleanup-failed',lambda r:r[-2].update(unmapped=False)),('missing-join',lambda r:r[-1].update(joined_threads=2)),('wrong-checksum',lambda r:r[1].update(checksum=r[1]['checksum']+1)),('wrong-order',lambda r:r[1].update(order='RL')),('wrong-work',lambda r:r[1].update(loads=1)),('wrong-cpu',lambda r:r[1].update(cpu_after=7)),('wrong-node',lambda r:r[1]['after_histogram'].__setitem__(0,0)),('noncomplete',lambda r:r[1].update(phase='after_placement')),('failure',lambda r:r.insert(1,dict(version=1,kind='failure',pair=0,condition='local',status=-12,phase='scratch_oom'))),('unused-field',lambda r:r[1].update(extra=0)),('bool-count',lambda r:r[-1].update(samples=True))]:
  changed=copy.deepcopy(rows);fn(changed);mutants.append((label,records_text(changed)))
 mutants.extend([('no-finalLF',text[:-1]),('CR-boundary',text.replace('\n','\r\n')),('NEL-boundary',text.replace('\n','\u0085')),('LS-boundary',text.replace('\n','\u2028')),('duplicate-key',text.replace('"version":1','"version":1,"version":1',1))])
 for name,changed in mutants:
  try:codec.decode(changed)
  except(ValueError,TypeError):check('codec reject '+name,True)
  else:check('codec reject '+name,False)
 (O/'codec-result.json').write_text(json.dumps(dict(checks=checks,synthetic_protocol_only=True,wos_runtime_qualified=False),indent=2)+'\n')
 b=[pin(p)for p in sorted(deps)];check('all actual compile/helper sources held',a==b)
except BaseException as e:errors.append(type(e).__name__+': '+str(e));(O/'failure01.txt').write_text(traceback.format_exc())
finally:
 for s in handlers:signal.signal(s,signal.SIG_IGN)
 removed=False;preserved=None
 try:
  try:
   if a and not b:b=[pin(Path(p['path']))for p in a]
   if a and a!=b:errors.append('source drift')
  except BaseException as e:errors.append('recheck '+str(e))
  if groups:errors.append('unproved owned groups');preserved=str(T)
  else:
   try:shutil.rmtree(T);removed=not T.exists()
   except BaseException as e:errors.append('cleanup '+str(e));preserved=str(T)
  result=dict(result='PASS_ACTUAL_NUMA_CHASE_HOST_PORTS_AND_CODEC_ONLY'if not errors else'FAIL_PRIVATE_NUMA_CHASE_HOST',errors=errors,checks=checks,commands=commands,inputs_before=a,inputs_after=b,source_held=bool(a)and a==b,scratch_removed=removed,preserved_scratch=preserved,owned_groups_pending=groups,seconds=time.monotonic()-start,scope='Actual target source/helpers with real pthread/mmap/clock and explicit WOS topology/affinity/policy/page-metadata ports. Synthetic full20-row vectors are codec-only. No WOS/kernel/physical NUMA/VM claim.')
  (O/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:result[k]for k in ['result','errors','source_held','scratch_removed','seconds']}))
 finally:
  for s,h in handlers.items():signal.signal(s,h)
 if fresh and removed and not groups:shutil.rmtree(O)
raise SystemExit(bool(errors)or bool(groups)or not removed)
