#!/usr/bin/env python3
"""Run the actual observation destructor with controlled capture/clock endpoints."""
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
source = Path(os.environ.get('WOS_VAS_OBSERVER_SOURCE', ROOT / 'modules/kern/src/platform/sched/resource_as.cpp')).read_text()
header = (ROOT / 'modules/kern/src/platform/sched/resource_as.hpp').read_text()
perf_header = (ROOT / 'modules/kern/src/platform/perf/perf_events.hpp').read_text()
perf_source = (ROOT / 'modules/kern/src/platform/perf/perf_events.cpp').read_text()

def block(text, start, terminator):
    begin = text.index(start)
    end = text.index(terminator, begin) + len(terminator)
    return text[begin:end]

destructor = block(source, 'AddressSpaceObservation::~AddressSpaceObservation()', '\n}')
declaration = block(header, 'class AddressSpaceObservation', '\n};')
enum = block(perf_header, 'enum class WkiPerfLocalVmemOp', '\n};')
name_source = perf_source[perf_source.index('const char* wki_op_name('):]
names = block(name_source, '        case WkiPerfScope::LOCAL_VMEM:', '\n        case WkiPerfScope::LOCAL_LOADER:')
names = names[:names.rindex('        case WkiPerfScope::LOCAL_LOADER:')]
code = r'''
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <initializer_list>
#include <limits>
#include <vector>
namespace ker::mod::mm::paging { struct PageTable; }
namespace ker::mod::sched::task { struct Task { uint64_t pid = 73; }; }
struct State {
 bool enabled = false, included = true, observed = true, disable_during = false;
 bool turn_over = false;
 uint64_t session = 0, start = 10, finish = 91;
 unsigned queries = 0, clocks = 0, observes = 0, cpu_queries = 0, correlations = 0;
 unsigned event_calls = 0, summary_calls = 0, emitted = 0, summarized = 0;
 std::vector<int> order;
} s;
namespace ker::mod::time {
uint64_t get_us() { s.order.push_back(1); return s.clocks++ ? s.finish : s.start; }
}
namespace ker::mod::cpu { unsigned current_cpu() { ++s.cpu_queries; return 3; } }
namespace ker::mod::perf {
enum class WkiPerfScope { LOCAL_VMEM, LOCAL_LOADER };
enum class WkiPerfPhase { END };
''' + enum + r'''
constexpr uint64_t PERF_ANY_SESSION = UINT64_MAX;
const char *name(WkiPerfScope scope, uint8_t op) {
 switch (scope) {
''' + names + r'''
 default: return "unknown";
 }
}
uint64_t current_wki_capture_session(WkiPerfScope scope, uint8_t op) {
 assert(scope == WkiPerfScope::LOCAL_VMEM && op == 11); ++s.queries;
 return s.enabled && s.included ? s.session : PERF_ANY_SESSION;
}
uint32_t next_wki_trace_correlation() { ++s.correlations; return 41; }
void check(WkiPerfScope scope, uint8_t op, uint16_t peer, uint16_t channel,
           int32_t status, uint32_t elapsed, uint64_t session) {
 assert(scope == WkiPerfScope::LOCAL_VMEM && op == 11 && !peer && !channel);
 assert(status == (s.observed ? 0 : -EFAULT));
 assert(elapsed == std::min<uint64_t>(UINT32_MAX, s.finish >= s.start ? s.finish - s.start : 0));
 assert(session != PERF_ANY_SESSION && s.clocks == 2 && s.observes == 1);
 assert(session == (s.turn_over ? s.session - 1 : s.session));
}
void record_wki_event(uint32_t cpu, uint64_t pid, WkiPerfScope scope, uint8_t op,
 WkiPerfPhase phase, uint16_t peer, uint16_t channel, uint32_t correlation,
 int32_t status, uint32_t elapsed, uint64_t callsite, uint64_t session) {
 check(scope, op, peer, channel, status, elapsed, session);
 assert(cpu == 3 && pid == 73 && phase == WkiPerfPhase::END && correlation == 41 && !callsite);
 ++s.event_calls; s.order.push_back(3);
 if (s.enabled && s.included && session == s.session) ++s.emitted;
}
void record_wki_summary(WkiPerfScope scope, uint8_t op, uint16_t peer, uint16_t channel,
 int32_t status, uint32_t elapsed, bool latency, uint32_t retries, uint64_t bytes, uint64_t session) {
 check(scope, op, peer, channel, status, elapsed, session);
 assert(latency && !retries && !bytes); ++s.summary_calls; s.order.push_back(4);
 if (s.enabled && s.included && session == s.session) ++s.summarized;
}
}
namespace ker::mod::sched::resource {
''' + declaration + r'''
bool observe_address_space(task::Task &task) {
 assert(task.pid == 73); ++s.observes; s.order.push_back(2);
 if (s.turn_over) ++s.session;
 if (s.disable_during) s.enabled = false;
 return s.observed;
}
auto AddressSpaceObservation::observe() -> bool { return observe_address_space(*m_subject); }
''' + destructor + r'''
}
void run(bool enabled, bool included, bool observed, uint64_t session,
         uint64_t start, uint64_t finish, bool turnover, bool disable) {
 s = State{}; s.enabled = enabled; s.included = included; s.observed = observed;
 s.session = session; s.start = start; s.finish = finish;
 s.turn_over = turnover; s.disable_during = disable;
 ker::mod::sched::task::Task task;
 { ker::mod::sched::resource::AddressSpaceObservation observation(&task); }
 bool const trace = enabled && included;
 assert(s.queries == 1 && s.observes == 1);
 assert(s.clocks == (trace ? 2 : 0) && s.cpu_queries == unsigned(trace));
 assert(s.correlations == unsigned(trace) && s.event_calls == unsigned(trace) && s.summary_calls == unsigned(trace));
 assert(s.emitted == unsigned(trace && !turnover && !disable) && s.summarized == s.emitted);
 assert(s.order == (trace ? std::vector<int>{1,2,1,3,4} : std::vector<int>{2}));
}
int main() {
 using namespace ker::mod::perf;
 static_assert(static_cast<uint8_t>(WkiPerfLocalVmemOp::ANON_MMAP) == 1);
 static_assert(static_cast<uint8_t>(WkiPerfLocalVmemOp::COW_PROMOTE) == 10);
 static_assert(static_cast<uint8_t>(WkiPerfLocalVmemOp::VAS_OBSERVE) == 11);
 assert(std::string_view(name(WkiPerfScope::LOCAL_VMEM, 11)) == "address_space_observe");
 unsigned cases = 0;
 for (bool enabled : {true, false}) for (bool included : {true, false})
 for (bool observed : {true, false}) for (uint64_t session : {uint64_t{0}, uint64_t{87}})
 for (auto clocks : {std::pair<uint64_t,uint64_t>{10,91}, {8,8}, {19,7}, {0,UINT32_MAX},
                     {0,uint64_t{UINT32_MAX}+1}, {0,uint64_t{UINT32_MAX}+9}, {11,UINT64_MAX}, {0,UINT64_MAX}})
 for (bool turnover : {false,true}) for (bool disable : {false,true}) {
  run(enabled,included,observed,session,clocks.first,clocks.second,turnover,disable); ++cases;
 }
 s = State{}; s.enabled = true;
 { ker::mod::sched::resource::AddressSpaceObservation observation(nullptr); }
 assert(!s.queries && !s.clocks && !s.observes && !s.event_calls && !s.summary_calls);
 printf("actual observation destructor %u cases: exact-once / null / disabled-filter / session-zero-turnover / failure / timer boundaries PASS\n", cases);
}
'''
code = code.replace('#include <vector>', '#include <vector>\n#include <string_view>\n#include <utility>')
with tempfile.TemporaryDirectory(prefix='wos-vas-observer-') as temp:
    p = Path(temp)
    (p / 'check.cpp').write_text(code)
    subprocess.run(['/usr/bin/clang++', '-std=c++23', '-O2', '-g', '-fsanitize=address,undefined',
                    '-fno-omit-frame-pointer', str(p / 'check.cpp'), '-o', str(p / 'check')], check=True)
    subprocess.run([str(p / 'check')], check=True)
