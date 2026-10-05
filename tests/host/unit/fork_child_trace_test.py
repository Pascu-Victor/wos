#!/usr/bin/env python3
"""Run actual fork trace helpers against the actual perf capture implementation."""
import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

from exec_trace_ownership_test import block, function
from perf_capture_kernel_test import PERF, PRELUDE

ROOT = Path(__file__).resolve().parents[3]
FACES = r'''
#include <utility>
namespace ker::mod::cpu { unsigned cpu=0; unsigned current_cpu() { return cpu; } }
namespace ker::mod::time { uint64_t now_us=100; unsigned reads=0;
uint64_t get_us() { ++reads; return now_us; } }
namespace ker::mod::sched::task { struct Task { uint64_t pid=0x200000007ULL; }; }
using Task=ker::mod::sched::task::Task;
namespace ker::syscall::process {
HELPERS
}
'''
CASES = r'''
namespace p=ker::mod::perf;
namespace t=ker::mod::time;
using namespace ker::syscall::process;
constexpr auto FORK=p::WkiPerfLocalProcOp::FORK;
constexpr uint64_t CHILD=0x300000007ULL, PARENT=0x200000007ULL;
LocalProcStage begin(Task* parent) {return begin_local_proc_stage(parent,FORK,0,123);}
uint32_t end(Task* parent,LocalProcStage& s,int status=0) {return end_local_proc_stage(parent,FORK,s,status,99,123);}
std::vector<p::PerfEvent> drain() {
 p::disable();std::array<p::PerfEvent,16> v{};
 size_t n=p::drain_events(v.data(),v.size(),UINT32_MAX);return {v.begin(),v.begin()+n};
}
uint32_t corr(const p::PerfEvent& e) {return p::wki_unpack_trace_correlation(e.lag_v);}
uint8_t op(const p::PerfEvent& e) {p::WkiPerfScope scope;uint8_t operation;p::WkiPerfPhase phase;uint16_t peer,channel;p::wki_unpack_event_data(e.data,scope,operation,phase,peer,channel);return operation;}
unsigned summaries() {unsigned n=0;for(auto& b:p::g_wki_summary)if(b.used)n+=b.calls;return n;}
int main() {
 p::init();Task parent;LocalProcStage s;
 // Session zero is valid before the first capture reset.
 if (!OLD_STAGES) {
  p::enable();s=begin(&parent);assert(s.capture_session==0);
  record_fork_child(s,CHILD,7,123);end(&parent,s);assert(drain().size()==4);
 }
 // Saved actual old BEGIN/END bodies must fail this reused-correlation regression.
 p::start_recording(p::PERF_MASK_LOCAL_PROC);s=begin(&parent);
 p::start_recording(p::PERF_MASK_LOCAL_PROC);end(&parent,s);
 assert(p::get_capture_status().written==0 && summaries()==0);
 // Full-width same-low32-bit IDs, caller migration, immutable subject and linked root.
 p::start_recording(p::PERF_MASK_LOCAL_PROC);parent.pid=PARENT;t::now_us=100;s=begin(&parent);
 parent.pid=999;t::now_us=350;ker::mod::cpu::cpu=1;
 record_fork_child(s,CHILD,7,123);assert(end(nullptr,s)==250);
 auto v=drain();assert(v.size()==4 && summaries()==1);
 assert(v[0].pid==PARENT && v[3].pid==PARENT && v[1].pid==CHILD && v[2].pid==CHILD);
 assert(corr(v[0])==corr(v[2]) && corr(v[2])==corr(v[3]) && corr(v[1])!=corr(v[0]));
 assert(op(v[1])==1 && op(v[2])==18);
 assert(p::wki_op_name(p::WkiPerfScope::LOCAL_PROC,18)==std::string_view("fork_child"));
 assert(v[1].aux==7 && v[2].aux==7 && v[3].aux==250 && v[3].cpu==1);
 auto reads=t::reads,clocks_before=clocks.load();record_fork_child(s,CHILD,7,123);end(nullptr,s);
 assert(t::reads==reads && clocks==clocks_before && summaries()==1);
 // Failed fork remains parent-only. No child link is manufactured by END.
 p::start_recording(p::PERF_MASK_LOCAL_PROC);s=begin(&parent);end(&parent,s,-12);v=drain();
 assert(v.size()==2 && p::wki_unpack_trace_status(v.back().lag_v)==-12 && summaries()==1);
 // Disabled/excluded/null admission avoids clocks and correlation allocation.
 for(unsigned mode=0;mode<3;++mode) {
  p::start_recording(p::PERF_MASK_LOCAL_PROC);
  if(mode==0)p::disable();if(mode==1)p::set_event_mask(p::PERF_MASK_SWITCH);
  reads=t::reads;clocks_before=clocks.load();auto correlation=p::g_wki_trace_correlation.load();
  s=begin(mode==2?nullptr:&parent);record_fork_child(s,CHILD,7,123);end(&parent,s);
  assert(!s.active && t::reads==reads && clocks==clocks_before && p::g_wki_trace_correlation==correlation);
  assert(p::get_capture_status().written==0 && summaries()==0);
 }
 // Disable/exclude/reset after BEGIN: no endpoint/child emission or HPET work.
 for(unsigned mode=0;mode<3;++mode) {
  p::start_recording(p::PERF_MASK_LOCAL_PROC);s=begin(&parent);
  if(mode==0)p::disable();if(mode==1)p::set_event_mask(p::PERF_MASK_SWITCH);
  if(mode==2)p::start_recording(p::PERF_MASK_LOCAL_PROC);
  reads=t::reads;clocks_before=clocks.load();auto before=p::get_capture_status().written;
  record_fork_child(s,CHILD,7,123);end(&parent,s);
  assert(t::reads==reads && clocks==clocks_before && p::get_capture_status().written==before && summaries()==0);
 }
 // Reset under admission-to-ring races cannot place stale legacy/link/END in new ring.
 for(unsigned operation=0;operation<3;++operation) {
  p::start_recording(p::PERF_MASK_LOCAL_PROC);
  if(operation!=0)s=begin(&parent);
  before_lock=[] {p::start_recording(p::PERF_MASK_LOCAL_PROC);};
  if(operation==0)s=begin(&parent);
  if(operation==1)record_fork_child(s,CHILD,7,123);
  if(operation==2)end(&parent,s);
  assert(!before_lock && p::get_capture_status().written==0 && summaries()==0);
 }
 // Bound summary must also reject a reset after the END ring emission.
 p::start_recording(p::PERF_MASK_LOCAL_PROC);s=begin(&parent);
 before_lock=[] {before_lock=[] {p::start_recording(p::PERF_MASK_LOCAL_PROC);};};
 end(&parent,s);assert(!before_lock && p::get_capture_status().written==0 && summaries()==0);
 // Clock anomalies do not wrap an END's wall duration.
 for(bool backward:{false,true}) {
  p::start_recording(p::PERF_MASK_LOCAL_PROC);t::now_us=100;s=begin(&parent);
  t::now_us=backward?99:UINT64_MAX;assert(end(&parent,s)==(backward?0:UINT32_MAX));
 }
 static_assert(sizeof(p::PerfEvent)==48);
 puts("PASS actual fork trace: full64 parent/child, linked and legacy correlations, success/error, off/filter, session0/reset races, immutable subject and bounded clocks");
}
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--old-stages', type=Path)
    args = parser.parse_args()
    source = (ROOT/'modules/kern/src/syscalls_impl/process/process.cpp').read_text()
    fork = function(source, 'wos_proc_fork')
    assert fork.index('child_events::commit_publication(*child)') < fork.index('record_fork_child(FORK_STAGE, child->pid, child->cpu') < fork.index('return finish_fork(static_cast<int64_t>(child->pid)')
    assert fork.count('record_fork_child(') == 1
    assert 'record_fork_child' not in fork[:fork.index('if (!child_events::commit_publication')]
    helpers = [block(source, r'^inline auto clamp_perf_aux\('), block(source, r'^struct LocalProcStage \{')+';']
    stages = args.old_stages.read_text() if args.old_stages else source
    if args.old_stages:
        helpers.append(block(stages, r'^inline void record_local_proc_event\('))
    helpers += [function(stages, name) for name in ('begin_local_proc_stage', 'end_local_proc_stage')]
    helpers.append(function(source, 'record_fork_child'))
    header = re.sub(r'^#(?:include|pragma).*$', '', (PERF/'perf_events.hpp').read_text(), flags=re.M)
    perf = re.sub(r'^#include.*$', '', (PERF/'perf_events.cpp').read_text(), flags=re.M)
    with tempfile.TemporaryDirectory(prefix='wos-fork-trace-') as temp:
        p = Path(temp)
        (p/'test.cpp').write_text(PRELUDE+header+perf+FACES.replace('HELPERS','\n'.join(helpers))+CASES.replace('OLD_STAGES', 'true' if args.old_stages else 'false'))
        subprocess.run([shutil.which('c++',path=os.defpath),'-std=c++23','-O1','-g','-pthread','-fsanitize=address,undefined',str(p/'test.cpp'),'-o',str(p/'test')],check=True)
        subprocess.run([str(p/'test')],check=True,env={**os.environ,'ASAN_OPTIONS':'detect_leaks=0'})


if __name__ == '__main__':
    main()
