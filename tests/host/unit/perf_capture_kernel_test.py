#!/usr/bin/env python3
"""Exercise actual perf rings, admission, and procfs formatting with host locks."""
import os
from pathlib import Path
import re
import resource
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
PERF = ROOT / 'modules/kern/src/platform/perf'
PROCFS = ROOT / 'modules/kern/src/vfs/fs/procfs.cpp'

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
thread_local unsigned lock_depth = 0;
thread_local std::function<void()> before_lock;
std::atomic<unsigned> clocks{0};
namespace ker::mod::sys {
struct Spinlock {
    std::mutex mutex;
    uint64_t lock_irqsave() {
        if (before_lock) { auto hook = std::move(before_lock); before_lock = {}; hook(); }
        mutex.lock(); ++lock_depth; return 0;
    }
    void unlock_irqrestore(uint64_t) { assert(lock_depth > 0); --lock_depth; mutex.unlock(); }
};
}
namespace ker::mod::smt { size_t get_core_count() { return 2; } }
namespace ker::mod::time { uint64_t get_monotonic_ns() { return ++clocks; } }
'''

CASES = r'''
namespace p = ker::mod::perf;
void invariant() {
    auto s = p::get_capture_status();
    assert(s.written == s.consumed + s.overwritten + s.pending && lock_depth == 0);
}
void put(unsigned cpu, uint64_t pid) {
    p::record_switch(cpu, pid, pid+1, 0, 0, 0, 0);
}
struct Collector { std::array<uint64_t, 20000> ids{}; size_t count = 0; };
bool collect(const p::PerfEvent& e, void* context) {
    assert(lock_depth == 1);
    auto& out = *static_cast<Collector*>(context);
    assert(out.count < out.ids.size()); out.ids.at(out.count++) = e.pid; return true;
}
int main() {
    p::init();
    static_assert(sizeof(p::PerfEvent) == 48);
    p::start_recording(p::PERF_MASK_ALL);
    // Exact fit / one byte short / zero buffer retain the rejected head.
    put(0, 42); p::disable();
    std::array<char, 512> full{};
    auto s = p::get_capture_status();
    auto e = p::g_rings[0].events[0];
    PerfTextOutput out{full.data(), full.data()+full.size()-1};
    assert(append_perf_event(e, &out));
    auto line = std::string(full.data(), out.cursor-full.data());
    assert(generate_kperf(full.data(), 0) == 0);
    assert(generate_kperf(full.data(), 1) == 0);
    assert(generate_kperf(full.data(), line.size()) == 0);
    assert(p::get_capture_status().pending == 1);
    assert(generate_kperf(full.data(), line.size()+1) == line.size());
    assert(std::string(full.data()) == line);
    assert(p::get_capture_status().pending == 0); invariant();
    // All six formats, extremes, batches and repeated tiny snapshots.
    p::start_recording(p::PERF_MASK_ALL);
    for (unsigned i = 0; i < 150; ++i) {
        p::PerfEvent v{}; v.ts_ns=UINT64_MAX; v.pid=i+1; v.cpu=i%2;
        v.type=i%6; v.flags=255; v.aux=UINT32_MAX; v.callsite=0x123;
        if (v.type == static_cast<uint8_t>(p::PerfEventType::WKI)) {
            v.data=p::wki_pack_event_data(p::WkiPerfScope::LOCAL_PROC, 14, p::WkiPerfPhase::END, 65535, 65535);
            v.lag_v=static_cast<int64_t>(p::wki_pack_trace_state(UINT32_MAX, INT32_MIN));
        } else if (v.type == 2 || v.type == 3) v.lag_v=reinterpret_cast<int64_t>("test_channel");
        else v.lag_v=INT64_MIN;
        auto& r=p::g_rings[v.cpu]; auto f=r.lock.lock_irqsave(); p::perf_push_event(r,v); r.lock.unlock_irqrestore(f);
    }
    p::disable(); size_t lines=0;
    for (unsigned n=0;n<200;++n) {
        auto len=generate_kperf(full.data(),full.size());
        if (len==0) break;
        assert(full[len-1]=='\n'); lines+=std::count(full.begin(),full.begin()+len,'\n'); invariant();
    }
    assert(lines==150 && p::get_capture_status().pending==0);
    // Explicit overflow keeps the newest entries and counts exact loss.
    p::start_recording(p::PERF_MASK_SWITCH);
    for (uint64_t i=0;i<p::PERF_RING_ENTRIES+17;++i) put(0,i);
    p::disable(); s=p::get_capture_status();
    assert(s.overwritten==17 && s.pending==p::PERF_RING_ENTRIES && s.consumed==0);
    Collector a;
    assert(p::drain_events_to(collect,&a,3000,0)==p::PERF_RING_ENTRIES);
    assert(a.ids[0]==17 && a.ids[a.count-1]==p::PERF_RING_ENTRIES+16); invariant();
    // CPU filter and legacy consumer are accounted together.
    p::start_recording(p::PERF_MASK_SWITCH); put(0,1);put(1,2);p::disable();
    a.count=0; assert(p::drain_events_to(collect,&a,5,1)==1 && a.ids[0]==2);
    p::PerfEvent raw{}; assert(p::drain_events(&raw,1,UINT32_MAX)==1 && raw.pid==1);
    assert(p::get_capture_status().consumed==2); invariant();
    // Producer fast gate can race disable, reset, and mask update before locking.
    auto produce=[](unsigned type) {
        switch(type) {
            case 0: for(unsigned i=0;i<10;++i) p::record_sample(0,1,2,false,0); break;
            case 1: put(0,1); break;
            case 2: p::record_wake(0,1,0,0,0,0,nullptr); break;
            case 3: p::record_sleep(0,1,0,0,0,0,nullptr); break;
            case 4: p::record_container_stat(0,1,p::PerfSubsystem::FD_TABLE,0,p::PERF_FLAG_CT_RESIZE,0,0,0);break;
            case 5: p::record_wki_event(0,1,p::WkiPerfScope::LOCAL_PROC,14,p::WkiPerfPhase::BEGIN,0,0,1,0,0,0);break;
        }
    };
    for(unsigned type=0;type<6;++type) for(unsigned action=0;action<3;++action) {
        p::start_recording(p::PERF_MASK_ALL); unsigned reads=clocks;
        before_lock=[action] {
            if(action==0) p::disable();
            if(action==1) p::start_recording(p::PERF_MASK_ALL);
            if(action==2) p::set_event_mask(0);
        };
        produce(type); assert(!before_lock); assert(p::get_capture_status().written==0 && clocks==reads);
    }
    p::disable(); unsigned reads=clocks;
    for(unsigned type=0;type<6;++type) produce(type);
    assert(clocks==reads); invariant();
    // A deferred exec endpoint's admission must retain its original capture.
    p::start_recording(p::PERF_MASK_ALL);
    auto original=p::current_wki_capture_session(p::WkiPerfScope::LOCAL_PROC,2);
    p::record_wki_event(0,UINT64_MAX,p::WkiPerfScope::LOCAL_PROC,17,p::WkiPerfPhase::POINT,0,0,1,0,0,0,original);
    assert(p::get_capture_status().written==1);
    p::start_recording(p::PERF_MASK_ALL);reads=clocks;
    p::record_wki_event(0,UINT64_MAX,p::WkiPerfScope::LOCAL_PROC,17,p::WkiPerfPhase::POINT,0,0,1,0,0,0,original);
    p::record_wki_summary(p::WkiPerfScope::LOCAL_PROC,2,0,0,0,7,true,0,0,original);
    assert(p::get_capture_status().written==0 && clocks==reads);
    assert(std::none_of(p::g_wki_summary.begin(),p::g_wki_summary.end(),[](auto const& b){return b.used;}));
    original=p::current_wki_capture_session(p::WkiPerfScope::LOCAL_PROC,2);
    before_lock=[] {p::start_recording(p::PERF_MASK_ALL);};
    p::record_wki_event(0,1,p::WkiPerfScope::LOCAL_PROC,17,p::WkiPerfPhase::POINT,0,0,1,0,0,0,original);
    assert(!before_lock && p::get_capture_status().written==0 && clocks==reads);
    original=p::current_wki_capture_session(p::WkiPerfScope::LOCAL_PROC,2);
    before_lock=[] {p::start_recording(p::PERF_MASK_ALL);};
    p::record_wki_summary(p::WkiPerfScope::LOCAL_PROC,2,0,0,0,7,true,0,0,original);
    assert(!before_lock && std::none_of(p::g_wki_summary.begin(),p::g_wki_summary.end(),[](auto const& b){return b.used;}));
    // Two consumers and one producer: accepted IDs cannot duplicate or vanish.
    p::start_recording(p::PERF_MASK_SWITCH);
    Collector b; a.count=0; std::atomic<bool> done{false};
    std::thread producer([&] { for(unsigned i=0;i<10000;++i) put(i%2,i); p::disable(); done=true; });
    auto consume=[&](Collector& c) {
        while(!done) p::drain_events_to(collect,&c,64,UINT32_MAX);
        while(p::drain_events_to(collect,&c,64,UINT32_MAX)>0) {}
    };
    std::thread first([&] {consume(a);}); std::thread second([&] {consume(b);});
    producer.join(); first.join(); second.join();
    s=p::get_capture_status(); assert(s.written==10000 && s.pending==0 && s.consumed==a.count+b.count);
    std::array<bool,10000> seen{};
    for(auto* c:{&a,&b}) for(size_t i=0;i<c->count;++i) {assert(!seen[c->ids[i]]);seen[c->ids[i]]=true;}
    invariant();
    // Reset retains always-on stats; status output is itself all-or-empty.
    auto stats=p::get_cpu_stats(0); p::start_recording(p::PERF_MASK_SWITCH);
    assert(p::get_cpu_stats(0).ctx_switches==stats.ctx_switches);
    assert(p::get_capture_status().written==0);
    assert(generate_kperfstatus(full.data(),1)==0);
    assert(generate_kperfstatus(full.data(),full.size())>0);
    assert(std::string(full.data()).starts_with("perf_capture version=1 session="));
    puts("perf capture kernel: boundaries, 6 producers, 18 admission races, overflow and concurrent drains pass");
}
'''

def main():
    header = re.sub(r'^#(?:include|pragma).*$', '', (PERF/'perf_events.hpp').read_text(), flags=re.M)
    source = re.sub(r'^#include.*$', '', (PERF/'perf_events.cpp').read_text(), flags=re.M)
    proc = PROCFS.read_text()
    formatter = proc[proc.index('class PerfEventTextWriter'):proc.index('// Generate content for /proc/kcpustat')]
    if len(sys.argv) == 2:
        old = Path(sys.argv[1]).read_text()
        def body(source):
            start = source.index('auto generate_kperf(')
            end = source.index('{', start) + 1
            depth = 1
            while depth:
                depth += (source[end] == '{') - (source[end] == '}')
                end += 1
            return source[start:end]
        formatter = formatter.replace(body(formatter), body(old))
    compiler = shutil.which('c++', path=os.defpath)
    with tempfile.TemporaryDirectory(prefix='wos-perf-capture-') as temp:
        path = Path(temp)
        test = path/'test.cpp'; binary = path/'test'
        test.write_text(PRELUDE+header+source+formatter+CASES)
        subprocess.run([compiler,'-std=c++23','-O1','-g','-pthread','-fsanitize=address,undefined',str(test),'-o',str(binary)],check=True)
        subprocess.run([str(binary)],check=True,env={**os.environ,'ASAN_OPTIONS':'detect_leaks=0'})

if __name__ == '__main__':
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    main()
