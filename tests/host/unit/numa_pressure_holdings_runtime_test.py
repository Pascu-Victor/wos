#!/usr/bin/env python3
"""Actual extracted Holdings/factory; small Host capacity and physical seams disclosed."""
from pathlib import Path
import ast,hashlib,json,os,resource,shlex,shutil,signal,subprocess,tempfile,time,traceback
import argparse, importlib.util, re
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--host-build',type=Path,required=True)
parser.add_argument('--gtest-main',type=Path,required=True)
parser.add_argument('--gtest-lib',type=Path,required=True)
parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[3])
parser.add_argument('--source',type=Path)
parser.add_argument('--evidence-dir',type=Path)
a=parser.parse_args()
R=a.root.resolve();native=(a.source or R/'modules/kern/src/test/numa_pressure_runtime_ktest.cpp').resolve()
fixture_spec=importlib.util.spec_from_file_location('pressure_process_fixture',R/'tests/host/unit/file_mmap_registry_runtime_test.py')
process_fixture=importlib.util.module_from_spec(fixture_spec);fixture_spec.loader.exec_module(process_fixture)
_ephemeral=a.evidence_dir is None
if _ephemeral:
 OUT=Path(tempfile.mkdtemp(prefix='wos-pressure-holdings-'))
else:
 OUT=a.evidence_dir.resolve();OUT.mkdir()
D=OUT;T=Path(tempfile.mkdtemp(prefix='scratch-',dir=OUT));DB=a.host_build.resolve()/'compile_commands.json';commands=[];owned=[];errors=[];before=[];after=[];start=time.monotonic()

def pin(p):
 p=Path(p).resolve();s=p.stat();b=p.read_bytes();t=p.stat();assert(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)==(t.st_dev,t.st_ino,t.st_size,t.st_mtime_ns,t.st_ctime_ns);return dict(path=str(p),bytes=len(b),sha256=hashlib.sha256(b).hexdigest())

handlers={sig:signal.getsignal(sig)for sig in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP)}
def interrupted(signum,_frame):raise KeyboardInterrupt('interrupted '+str(signum))
def run(label,args,expected=0):
 if shutil.disk_usage(D).free<64*1024**2:raise RuntimeError('private free bound')
 log=OUT/(label+'.log')
 previous_ubsan=os.environ.get('UBSAN_OPTIONS')
 os.environ['UBSAN_OPTIONS']='halt_on_error=1'
 try:row=process_fixture.command(list(map(str,args)),log,T,start+120,owned)
 finally:
  if previous_ubsan is None:os.environ.pop('UBSAN_OPTIONS',None)
  else:os.environ['UBSAN_OPTIONS']=previous_ubsan
 row.update(label=label,expected=expected);commands.append(row)
 if row['exit']!=expected:raise RuntimeError(label+' '+str(row['exit'])+' '+log.read_text(errors='replace')[-6000:])

def deps(p):return {Path(x).resolve()for x in shlex.split(p.read_text().replace('\\\n',' ').split(':',1)[1])}
try:
 for sig in handlers:signal.signal(sig,interrupted)
 process_fixture.subreaper()
 helper=R/'tests/host/unit/numa_node_reclaim_runtime_test.py';tree=ast.parse(helper.read_text());f=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='body');env={};exec(compile(ast.Module(body=[f],type_ignores=[]),'pure-body-reader','exec'),env);body=env['body']
 text=native.read_text();hold=body(text,'struct Holdings','};');actual_check=body(text,'auto check(');capture=body(text,'void capture_limit(')
 factory_test=R/'tests/host/unit/numa_alloc_test.cpp';base_source=factory_test.read_text()
 # Physical/ref/address seams. The actual factory/OwnedSpan and extracted
 # production geometry, index, take and clear are compiled, not modelled.
 base_source=base_source.replace('    bool fail_all{};', '    bool fail_all{}; uint64_t cursors[64]{}; std::vector<unsigned> script{}; size_t script_index{}; void* arena{}; void* alias{}; bool bad_batch_stats{}; std::vector<size_t> batches{};')
 base_source=base_source.replace('0x10000000ULL + node * 0x10000000ULL, 2 * LARGE','0x1000000000ULL + node * 0x400000000ULL, 0x200000000ULL').replace('topology.nodes[node].usable_bytes = 2 * LARGE','topology.nodes[node].usable_bytes = 0x200000000ULL')
 needle='        uint64_t const BYTES = static_cast<uint64_t>(pages) * PAGE;'
 scripted=r'''        if(alias!=nullptr) {uint32_t at=0;auto* b=containing(reinterpret_cast<uint64_t>(alias),at);if(!b||at+pages>b->pages)std::abort();for(uint32_t i=0;i<pages;++i)++b->refs[at+i];return alias;}
        unsigned node=0;while(node+1<topology.node_count && physical>=topology.memory[node+1].base)++node;
        if(pages==1 && !script.empty()) {
            if(arena==nullptr){constexpr unsigned N=1024;arena=std::aligned_alloc(PAGE,N*PAGE);if(!arena)std::abort();
                physical+=cursors[node];cursors[node]+=N*PAGE;
                blocks.push_back({std::unique_ptr<void,FreeStorage>{arena},physical,N,std::vector<uint32_t>(N,0)});}
            if(script_index>=script.size())std::abort();unsigned index=script[script_index++];if(index>=1024)std::abort();uint32_t at=0;auto* b=containing(reinterpret_cast<uint64_t>(arena),at);if(!b||b->refs[index])std::abort();b->refs[index]=1;return static_cast<uint8_t*>(arena)+index*PAGE;
        }
        cursors[node]=(cursors[node]+alignment-1)&~(alignment-1);physical+=cursors[node];cursors[node]+=static_cast<uint64_t>(pages)*PAGE;
'''
 assert base_source.count(needle)==1;base_source=base_source.replace(needle,scripted+needle)
 endpoint=r'''
namespace ker::mod::mm::addr {
auto get_hhdm_offset()->uint64_t{return 0;}
auto get_virt_pointer(paddr_t physical)->paddr_t* {
 for(auto& b:backend().blocks)if(b.storage && physical>=b.physical && physical-b.physical<static_cast<uint64_t>(b.pages)*PAGE)
  return reinterpret_cast<paddr_t*>(reinterpret_cast<uint64_t>(b.storage.get())+physical-b.physical);
 return nullptr;
}}
namespace ker::mod::mm::phys {
auto page_ref_dec_batch(std::span<void* const> pages)->PageRefBatchStats {
 backend().batches.push_back(pages.size());PageRefBatchStats out{};for(auto* p:pages){uint32_t i=0;auto* b=backend().containing(reinterpret_cast<uint64_t>(p),i);if(!b||!b->refs[i]){ADD_FAILURE()<<"batch unowned ref";continue;}++out.refs_decremented;if(page_ref_dec(p)==0)++out.pages_freed;}
 if(backend().bad_batch_stats)out.pages_freed=0;return out;
}
auto snapshot_node_counters(NodeCounterSnapshot&,NodeCounterWorkspace&)noexcept->NodeCounterError{return NodeCounterError::NOT_READY;}
}
'''
 common=r'''
namespace {
namespace phys=mm::phys;
constexpr uint64_t MAX_HELD=1024*1024;
constexpr size_t FRAME_WORDS=32768;
constexpr uint64_t FRAME_BITS=FRAME_WORDS*64;
'''+body(text,'struct FrameOwnership','};')+r'''
struct HostState {
 FrameOwnership frames{};void* batch[128]{};size_t bulk_allocations{},order0_allocations{};
 uint64_t held{},released{},assertions{},tail_pages{},held_highwater{},bulk_highwater{},order0_highwater{},tail_pages_highwater{};
 uint64_t limit_held{},limit_bulk{},limit_order0{},limit_tail_pages{},limit_next_pages{},limit_buddy{},limit_cached{},limit_usable{};
 uint32_t limit_reasons{};bool limit_captured{},limit_sample_valid{};numa::NodeId selected{};
 phys::NodeCounterSnapshot baseline{},sample{};phys::NodeCounterWorkspace work{};bool cleanup_valid{true};const char* failure{};
};static HostState state;
'''
 helpers=actual_check+'\n'+capture+'\n'+body(text,'auto prepare_frame_geometry(')+'\n'+body(text,'auto frame_span_index(')+'\n'+hold
 support=Path(__file__).with_name('numa_pressure_holdings_cases.inc');legacy=Path(__file__).with_name('numa_pressure_holdings_legacy.inc')
 source=OUT/'actual-bitmap-fixture.cpp';source.write_text(base_source+endpoint+common+helpers+'\n'+support.read_text()+'\n}\n')
 legacy_source=OUT/'actual-legacy-fixture.cpp';legacy_source.write_text(base_source+endpoint+legacy.read_text())
 row=next(r for r in json.loads(DB.read_text())if r['file'].endswith('/tests/host/unit/numa_alloc_test.cpp')and'numa_alloc_test.dir/'in r['command']);args=shlex.split(row['command']);base=[args[0],'-I'+str(R/'modules/kern/src/platform/mm')];i=1
 while i<len(args):
  if args[i]in['-o','-MF','-MT','-MQ']:i+=2;continue
  if args[i]in['-c','-MD','-MMD','-MP']or args[i]==row['file']:i+=1;continue
  base.append(args[i]);i+=1
 libs=[a.gtest_main.resolve(),a.gtest_lib.resolve()];factory=R/'modules/kern/src/platform/mm/numa_alloc.cpp'
 seen={DB,Path(__file__),R/'tests/host/unit/file_mmap_registry_runtime_test.py',native,helper,factory_test,source,legacy_source,support,legacy,*libs};sources=[('test',source),('legacy',legacy_source),('factory',factory)];objects={}
 controls=[
 ('duplicate','(state.frames.owned[index / 64] & BIT) == 0','true','PressureBitmap.GenuineExtraRefDuplicateRefusesBeforeTransfer'),
 ('modulo','uint64_t const INDEX = (physical - frames.first) / PAGE;','uint64_t const INDEX = ((physical - frames.first) / PAGE) % (FRAME_BITS / 2);','PressureBitmap.HighAbsolutePaAndLastRelativeBitRemainExact'),
 ('partial','capture_limit(64, pages); return false;\n            }\n        }','capture_limit(64, pages); return false;\n            }\n            state.frames.owned[index / 64] |= BIT;\n        }','PressureBitmap.Whole512RefusalDoesNotAdoptEligiblePrefix'),
 ('unowned','size_t pending = 0;','size_t pending = 0; if (state.held != 0) state.frames.owned[0] |= uint64_t{1} << 63;','PressureBitmap.DisjointAlternatingDirectionNeverClaimsHoles'),
 ('batch','if (pending == 128) flush();','if (pending == 127) flush();','PressureBitmap.Batch127128129AndRemainderAreOnceOnly'),
 ('usable','state.held <= state.baseline.nodes[state.selected].usable - pages','true','PressureBitmap.UsableBoundRefusalCapturesBeforeDestructorAndDoesNotAdopt')]
 for name,old,new,filter_ in controls:
  pattern=r'\s*'.join(re.escape(t)for t in re.findall(r'\w+|[^\w\s]',old))
  changed,count=re.subn(pattern,lambda _:new,source.read_text());assert count==1,(name,count)
  mutant=OUT/(name+'-mutant.cpp');mutant.write_text(changed);sources.append((name,mutant));seen.add(mutant)
 for name,src in sources:
  dep=T/(name+'.d');run('deps-'+name,base+['-M','-MF',dep,'-MT','out',src]);seen.update(deps(dep))
 before=[pin(p)for p in sorted(seen)]
 for name,src in sources[:3]:
  obj=T/(name+'.o');run('compile-'+name,base+['-c',src,'-o',obj]);objects[name]=obj
 for kind,test,filter_,expected in [('candidate','test','PressureBitmap.*',0),('legacy','legacy','PressureHoldings.*',0)]+[(n,n,f,1)for n,_,_,f in controls]:
  if expected:
   obj=T/(test+'.o');run('compile-'+test,base+['-c',OUT/(test+'-mutant.cpp'),'-o',obj]);objects[test]=obj
  binary=T/kind;run('link-'+kind,[base[0],'-fsanitize=address,undefined',objects[test],objects['factory'],*libs,'-pthread','-o',binary]);report=OUT/(kind+'-gtest.json');run('run-'+kind,[binary,'--gtest_filter='+filter_,'--gtest_output=json:'+str(report)],expected)
  parsed=json.loads(report.read_text());assert (parsed['tests'],parsed['failures'])==((23,0)if kind=='candidate'else(13,0)if kind=='legacy'else(1,1));assert not any(x in (OUT/('run-'+kind+'.log')).read_text()for x in ['ERROR: AddressSanitizer','runtime error:'])
  binary.unlink();objects[test].unlink()
 after=[pin(p)for p in sorted(seen)];assert before==after
except BaseException as e:errors.append(type(e).__name__+': '+str(e));(OUT/'failure01.txt').write_text(traceback.format_exc())
finally:
 for sig in handlers:signal.signal(sig,signal.SIG_IGN)
 removed=0;scratch_removed=False;preserved_scratch=None
 try:
  try:
   if before and not after:after=[pin(p['path'])for p in before]
   if before and before!=after:errors.append('watched inputs changed')
  except BaseException as e:errors.append('input recheck: '+type(e).__name__+': '+str(e))
  if owned:
   errors.append('owned sessions not proven absent; preserving all scratch/evidence')
   preserved_scratch=str(T)
  else:
   try:
    removed=process_fixture.allocated(T);shutil.rmtree(T);scratch_removed=not T.exists()
   except BaseException as e:
    errors.append('scratch cleanup: '+type(e).__name__+': '+str(e));preserved_scratch=str(T)
  j=dict(qualification='PASS_ACTUAL_HOLDINGS_FACTORY_HOST_SEAMS_ONLY'if not errors else'FAIL_HOLDINGS_HOST',errors=errors,commands=commands,input_before=before,input_after=after,source_held=bool(before)and before==after,scratch_removed=scratch_removed,preserved_scratch=preserved_scratch,scratch_allocated_bytes_removed=removed,owned_groups_pending=owned[:],seconds=time.monotonic()-start,scope='Actual extracted geometry/frame index/Holdings/check/capture and current factory/OwnedSpan. Physical/address/ref/batch seams, immutable canonical views supplied by Host; no native list traversal claim. Unchanged13 pre37e helper vectors compiled separately with disclosed legacyTAIL16; new bitmap uses full32768words and real bounded actual factory spans. Logical geometry tests do not allocate payload for their eligible windows. No real buddy/IRQ/scheduler/kernel pressure claim; ASAN+UBSAN enabled, LSan disabled in sandbox.')
  try:
   (OUT/'result.json').write_text(json.dumps(j,indent=2)+'\n')
  except BaseException as e:
   errors.append('evidence publication: '+type(e).__name__+': '+str(e))
  print(json.dumps({k:j[k]for k in ['qualification','errors','source_held','scratch_removed','seconds']}))
  if _ephemeral:
   if not errors and scratch_removed and not owned:
    try:shutil.rmtree(OUT)
    except BaseException as e:errors.append('ephemeral evidence cleanup: '+type(e).__name__+': '+str(e));print('preserved evidence: '+str(OUT))
   else:print('preserved evidence: '+str(OUT))
 finally:
  for sig,handler in handlers.items():signal.signal(sig,handler)
raise SystemExit(bool(errors)or not scratch_removed or bool(owned))
