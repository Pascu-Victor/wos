#!/usr/bin/env python3
"""Actual extracted Holdings/factory; small Host capacity and physical seams disclosed."""
from pathlib import Path
import ast,hashlib,json,os,resource,shlex,shutil,signal,subprocess,tempfile,time,traceback
import argparse, importlib.util
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
 try:row=process_fixture.command(list(map(str,args)),log,T,start+60,owned)
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
 text=native.read_text();hold=body(text,'struct Holdings','};');actual_check=body(text,'auto check(');capture=body(text,'void capture_limit(');join=body(text,'auto tail_join(')
 factory_test=R/'tests/host/unit/numa_alloc_test.cpp';base_source=factory_test.read_text()
 assert base_source.count('    bool fail_all{};')==1
 base_source=base_source.replace('    bool fail_all{};','    bool fail_all{};\n    std::vector<unsigned> script{}; size_t script_index{}; void* arena{};')
 needle='''        uint64_t const BYTES = static_cast<uint64_t>(pages) * PAGE;'''
 scripted='''        if(pages==1 && !script.empty()) {
            if(arena==nullptr){constexpr unsigned N=1024;arena=std::aligned_alloc(PAGE,N*PAGE);if(arena==nullptr)std::abort();
                blocks.push_back({std::unique_ptr<void,FreeStorage>{arena},physical,N,std::vector<uint32_t>(N,0)});}
            if(script_index>=script.size())std::abort();unsigned index=script[script_index++];
            if(index>=1024)std::abort();uint32_t page=0;auto* b=containing(reinterpret_cast<uint64_t>(arena),page);
            if(b==nullptr || b->refs[index]!=0)std::abort();b->refs[index]=1;
            return static_cast<uint8_t*>(arena)+index*PAGE;
        }
'''
 assert base_source.count(needle)==1;base_source=base_source.replace(needle,scripted+needle)
 old_tests=r'''class PressureHoldings:public NumaAllocation {protected:void SetUp()override{NumaAllocation::SetUp();state={};state.cleanup_valid=true;state.baseline.nodes[0].usable=1024*1024;}};
TEST_F(PressureHoldings, RealOwnedSpanBulkAndTailPrefixFullyReturns) {
 {Holdings h;for(unsigned i=0;i<2;++i){auto r=numa::try_allocate_pages(context(),512,LARGE,OWNER,Initialization::FULL_OVERWRITE);ASSERT_TRUE(h.take(r,512));EXPECT_TRUE(r.span.empty());}
 for(unsigned i=0;i<7;++i){auto r=numa::try_allocate_pages(context(),1,PAGE,OWNER);ASSERT_TRUE(h.take(r,1));}EXPECT_EQ(state.held,1031U);}
 EXPECT_EQ(state.held,0U);EXPECT_EQ(state.released,1031U);EXPECT_EQ(state.spans_count,0U);EXPECT_EQ(state.tail_count,0U);EXPECT_TRUE(state.cleanup_valid);EXPECT_EQ(model.live_refs(),0U);
}
TEST_F(PressureHoldings, EarlyReturnFreesExactSuccessfulPrefix) {
 auto path=[](){Holdings h;for(unsigned i=0;i<3;++i){auto r=numa::try_allocate_pages(context(),1,PAGE,OWNER);if(!h.take(r,1))return false;}return false;};
 EXPECT_FALSE(path());EXPECT_EQ(state.released,3U);EXPECT_EQ(state.held,0U);EXPECT_EQ(model.live_refs(),0U);EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureHoldings, InvalidSpanShapeRetainsActualResultUntilItsDestructor) {
 {Holdings h;auto r=numa::try_allocate_pages(context(),512,LARGE,OWNER);EXPECT_FALSE(h.take(r,1));EXPECT_FALSE(r.span.empty());EXPECT_EQ(state.held,0U);}
 EXPECT_EQ(model.live_refs(),0U);EXPECT_EQ(state.released,0U);EXPECT_NE(state.failure,nullptr);
}
TEST_F(PressureHoldings, PostTransferNodeRefusalStillReturnsCommittedOwnership) {
 model.domains(2);{Holdings h;auto r=numa::try_allocate_pages(context(numa::Mode::BIND,2,0),1,PAGE,OWNER);ASSERT_EQ(r.actual,1);EXPECT_FALSE(h.take(r,1));EXPECT_TRUE(r.span.empty());EXPECT_EQ(state.held,1U);}
 EXPECT_EQ(model.live_refs(),0U);EXPECT_EQ(state.released,1U);EXPECT_EQ(state.held,0U);EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureHoldings, ExplicitClearThenDestructorDoesNotDoubleRelease) {
 {Holdings h;auto r=numa::try_allocate_pages(context(),512,LARGE,OWNER);ASSERT_TRUE(h.take(r,512));h.clear();EXPECT_EQ(state.released,512U);h.clear();}
 EXPECT_EQ(state.released,512U);EXPECT_EQ(model.decremented.size(),512U);EXPECT_EQ(model.live_refs(),0U);
}
'''
 extra=r'''// Backend scripts only the returned Host addresses/ref ledger. The actual
// production factory constructs OwnedSpan; actual take/clear are extracted.
void sequence(Backend& model, std::initializer_list<unsigned> indices) {
    model.script.assign(indices.begin(), indices.end());
}
auto claim_page() -> numa::AllocationResult {
    return numa::try_allocate_pages({{0, numa::Mode::LOCAL}, 0, 0}, 1, PAGE, OWNER);
}
TEST_F(PressureHoldings, ForwardAdjacentIndependentPagesUseOneRecordAndBatchReturn) {
    sequence(model,{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19});
    {Holdings h;for(unsigned i=0;i<20;++i){auto r=claim_page();ASSERT_TRUE(h.take(r,1));}
     EXPECT_EQ(state.tail_count,1U);EXPECT_EQ(state.tail[0].pages,20U);EXPECT_EQ(state.tail_pages,20U);EXPECT_EQ(state.held_highwater,20U);}
    EXPECT_EQ(state.released,20U);EXPECT_EQ(model.live_refs(),0U);EXPECT_EQ(model.decremented.size(),20U);EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureHoldings, ReverseAdjacentIndependentPagesExtendExactBase) {
    sequence(model,{5,4,3,2,1,0});
    {Holdings h;for(unsigned i=0;i<6;++i){auto r=claim_page();ASSERT_TRUE(h.take(r,1));}
     EXPECT_EQ(state.tail_count,1U);EXPECT_EQ(state.tail[0].pages,6U);EXPECT_EQ(state.tail[0].base,model.arena);}
    EXPECT_EQ(state.released,6U);EXPECT_EQ(model.live_refs(),0U);EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureHoldings, DisjointRecordsAndAlternatingDirectionNeverClaimHoles) {
    sequence(model,{0,1,10,9,20,21,30,29});
    {Holdings h;for(unsigned i=0;i<8;++i){auto r=claim_page();ASSERT_TRUE(h.take(r,1));}
     ASSERT_EQ(state.tail_count,4U);for(unsigned i=0;i<4;++i)EXPECT_EQ(state.tail[i].pages,2U);EXPECT_EQ(state.tail_pages,8U);}
    EXPECT_EQ(state.released,8U);EXPECT_EQ(model.decremented.size(),8U);EXPECT_EQ(model.live_refs(),0U);
}
TEST_F(PressureHoldings, FullRecordLedgerStillAllowsAdjacentActualCandidate) {
    for(unsigned i=0;i<TAIL;++i)model.script.push_back(i*3);
    model.script.push_back((TAIL-1)*3+1);
    {Holdings h;for(unsigned i=0;i<TAIL+1;++i){auto r=claim_page();ASSERT_TRUE(h.take(r,1));}
     EXPECT_EQ(state.tail_count,TAIL);EXPECT_EQ(state.tail[TAIL-1].pages,2U);EXPECT_EQ(state.tail_pages,TAIL+1);EXPECT_FALSE(state.limit_captured);}
    EXPECT_EQ(state.released,TAIL+1);EXPECT_EQ(model.live_refs(),0U);EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureHoldings, FullNonadjacentRefusesBeforeTransferAndKeepsFailurePrefix) {
    for(unsigned i=0;i<TAIL+1;++i)model.script.push_back(i*3);
    {Holdings h;for(unsigned i=0;i<TAIL;++i){auto r=claim_page();ASSERT_TRUE(h.take(r,1));}
     {auto refused=claim_page();EXPECT_FALSE(h.take(refused,1));EXPECT_FALSE(refused.span.empty());
      EXPECT_EQ(model.live_refs(),TAIL+1);EXPECT_EQ(state.held,TAIL);EXPECT_EQ(state.tail_count,TAIL);
      EXPECT_EQ(state.limit_reasons,2U);EXPECT_EQ(state.limit_held,TAIL);EXPECT_EQ(state.limit_tail_records,TAIL);
      EXPECT_EQ(state.limit_tail_pages,TAIL);EXPECT_FALSE(state.limit_sample_valid);}
     EXPECT_EQ(model.live_refs(),TAIL);}
    EXPECT_EQ(state.held,0U);EXPECT_EQ(state.tail_pages,0U);EXPECT_EQ(state.released,TAIL);EXPECT_EQ(model.live_refs(),0U);
    EXPECT_EQ(state.limit_held,TAIL);EXPECT_EQ(state.tail_records_highwater,TAIL);EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureHoldings, AddressArithmeticAndSupportedShapeRejectBeforeAnyTransfer) {
    EXPECT_EQ(tail_join({reinterpret_cast<void*>(UINTPTR_MAX-PAGE+1),1},PAGE),TailJoin::INVALID);
    EXPECT_EQ(tail_join({reinterpret_cast<void*>(PAGE),1},UINTPTR_MAX-PAGE+1),TailJoin::INVALID);
    EXPECT_EQ(tail_join({reinterpret_cast<void*>(PAGE+1),1},PAGE),TailJoin::INVALID);
    EXPECT_EQ(tail_join({reinterpret_cast<void*>(PAGE),0},PAGE),TailJoin::INVALID);
    EXPECT_EQ(tail_join({reinterpret_cast<void*>(PAGE),1},PAGE),TailJoin::INVALID);
    {Holdings h;auto r=claim_page();EXPECT_FALSE(h.take(r,2));EXPECT_FALSE(r.span.empty());EXPECT_EQ(state.limit_reasons,8U);EXPECT_EQ(state.held,0U);}
    EXPECT_EQ(model.live_refs(),0U);EXPECT_EQ(state.released,0U);
}
TEST_F(PressureHoldings, UsableBoundRefusalCapturesBeforeDestructorAndDoesNotAdopt) {
    sequence(model,{0,1});state.baseline.nodes[0].usable=1;
    {Holdings h;{auto r=claim_page();ASSERT_TRUE(h.take(r,1));}{auto r=claim_page();EXPECT_FALSE(h.take(r,1));EXPECT_FALSE(r.span.empty());
     EXPECT_EQ(state.limit_reasons,4U);EXPECT_EQ(state.limit_held,1U);EXPECT_EQ(state.held,1U);}}
    EXPECT_EQ(state.released,1U);EXPECT_EQ(model.live_refs(),0U);EXPECT_EQ(state.limit_held,1U);EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureHoldings, MixedBulkAndLongTailReturnEachIndependentReferenceOnce) {
    {Holdings h;auto bulk=numa::try_allocate_pages(context(),512,LARGE,OWNER);ASSERT_TRUE(h.take(bulk,512));
     for(unsigned i=0;i<300;++i)model.script.push_back(i);
     for(unsigned i=0;i<300;++i){auto r=claim_page();ASSERT_TRUE(h.take(r,1));}
     ASSERT_EQ(state.tail_count,1U);EXPECT_EQ(state.tail[0].pages,300U);EXPECT_EQ(state.tail_pages,300U);h.clear();h.clear();}
    EXPECT_EQ(state.released,812U);EXPECT_EQ(model.decremented.size(),812U);EXPECT_EQ(state.held,0U);EXPECT_EQ(state.tail_pages,0U);
    EXPECT_EQ(model.live_refs(),0U);EXPECT_TRUE(state.cleanup_valid);
}
'''
 common='''
namespace ker::mod::mm::phys {
auto page_ref_dec_batch(std::span<void* const> pages) -> PageRefBatchStats {
 PageRefBatchStats result{};for(auto* page:pages){uint32_t index=0;auto* b=backend().containing(reinterpret_cast<uint64_t>(page),index);
 if(b==nullptr||b->refs[index]==0)continue;++result.refs_decremented;if(page_ref_dec(page)==0)++result.pages_freed;}return result;
}
auto snapshot_node_counters(NodeCounterSnapshot&,NodeCounterWorkspace&) noexcept -> NodeCounterError {return NodeCounterError::NOT_READY;}
}
namespace {
namespace phys=mm::phys;
constexpr uint64_t MAX_HELD=1024*1024;
constexpr size_t SPANS=2048,TAIL=16; // Deliberate scaled Host capacity; kernel32768 proven separately.
struct SpanRecord{void* base{};uint32_t pages{};};
struct HostState {
 SpanRecord spans[SPANS]{}; TAIL_DECL;void* batch[128]{};size_t spans_count{},tail_count{};
 uint64_t held{},released{},assertions{},tail_pages{},held_highwater{},bulk_highwater{},tail_records_highwater{},tail_pages_highwater{};
 uint64_t limit_held{},limit_bulk{},limit_tail_records{},limit_tail_pages{},limit_next_pages{},limit_buddy{},limit_cached{},limit_usable{};
 uint32_t limit_reasons{};bool limit_captured{},limit_sample_valid{};numa::NodeId selected{};
 phys::NodeCounterSnapshot baseline{},sample{};phys::NodeCounterWorkspace work{};bool cleanup_valid{true};const char* failure{};
};static HostState state;
'''
 fixture=base_source+common.replace('TAIL_DECL','SpanRecord tail[TAIL]{}')+actual_check+'\n'+capture+'\nenum class TailJoin:uint8_t { NEW_RECORD,FORWARD,BACKWARD,INVALID };\n'+join+'\n'+hold+'\n'+old_tests+extra+'\n}\n'
 source=OUT/'actual-holdings-fixture.cpp';source.write_text(fixture)
 row=next(r for r in json.loads(DB.read_text())if r['file'].endswith('/tests/host/unit/numa_alloc_test.cpp')and'numa_alloc_test.dir/'in r['command']);args=shlex.split(row['command']);base=[args[0],'-I'+str(R/'modules/kern/src/platform/mm')];i=1
 while i<len(args):
  if args[i]in['-o','-MF','-MT','-MQ']:i+=2;continue
  if args[i]in['-c','-MD','-MMD','-MP']or args[i]==row['file']:i+=1;continue
  base.append(args[i]);i+=1
 libs=[a.gtest_main.resolve(),a.gtest_lib.resolve()];factory=R/'modules/kern/src/platform/mm/numa_alloc.cpp'
 seen={DB,Path(__file__),R/'tests/host/unit/file_mmap_registry_runtime_test.py',native,helper,factory_test,source,*libs};sources=[('test',source),('factory',factory)];objects={}
 for name,src in sources:
  dep=T/(name+'.d');run('deps-'+name,base+['-M','-MF',dep,'-MT','out',src]);seen.update(deps(dep))
 before=[pin(p)for p in sorted(seen)]
 for name,src in sources:
  obj=T/(name+'.o');run('compile-'+name,base+['-c',src,'-o',obj]);objects[name]=obj
 for kind,test,filter_,expected in [('candidate','test','PressureHoldings.*',0)]:
  binary=T/kind;run('link-'+kind,[base[0],'-fsanitize=address,undefined',objects[test],objects['factory'],*libs,'-pthread','-o',binary]);report=OUT/(kind+'-gtest.json');run('run-'+kind,[binary,'--gtest_filter='+filter_,'--gtest_output=json:'+str(report)],expected)
  parsed=json.loads(report.read_text());assert (parsed['tests'],parsed['failures'])==((13,0)if expected==0 else(1,1))
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
  j=dict(qualification='PASS_ACTUAL_HOLDINGS_FACTORY_HOST_SEAMS_ONLY'if not errors else'FAIL_HOLDINGS_HOST',errors=errors,commands=commands,input_before=before,input_after=after,source_held=bool(before)and before==after,scratch_removed=scratch_removed,preserved_scratch=preserved_scratch,scratch_allocated_bytes_removed=removed,owned_groups_pending=owned[:],seconds=time.monotonic()-start,scope='Exact actual Holdings/check/capture_limit/tail_join and current factory/OwnedSpan. Existing physical/ref/topology seams plus scripted independent addresses, batch and unavailable snapshot seams. Host TAIL16; kernel actual32768 separately compiled. No real buddy/IRQ/scheduler/kernel pressure claim; ASAN+UBSAN enabled, LSan disabled in sandbox.')
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
