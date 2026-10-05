#!/usr/bin/env python3
"""Run the actual perf command with controlled process, clock and file I/O.

No host children, signal handlers or files are created by the command fixture.
The observer implementation and command body are compiled without rewriting.
"""
from pathlib import Path
import os
import re
import resource
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def declaration(source, name):
    match = re.search(r'^(?:class|struct) ' + name + r' \{', source, re.M)
    assert match, name
    end = source.index('{', match.start()) + 1
    depth = 1
    while depth:
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    assert source[end] == ';'
    return source[match.start():end + 1]


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <cassert>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <fcntl.h>
#include <optional>
#include <string>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>
#include "capture.hpp"
namespace std { template<class... Args> void println(Args&&...) {} }
namespace fixture {
constexpr int64_t CHILD = 0x100000009LL, PARENT = 0x20000000aLL;
struct Replaced {}; struct Exited { int status; };
int64_t time_ns, fork_result, wait_calls, sleeps, exit_after, descendant_after;
int clock_calls, bad_clock_at, scans;
bool cancel, open_failed, write_failed, enable_failed, exec_failed;
std::vector<std::string> operations, exec_args;
std::vector<std::pair<int64_t, int>> kills;
std::vector<std::pair<std::string, int64_t>> stamps;
std::array<void(*)(int), 64> handlers{};
perf::CaptureReceipt saved;
void op(std::string name, int64_t duration = 100) {
    operations.push_back(name); stamps.emplace_back(name, time_ns); time_ns += duration;
}
int access(const char*, int) { op("access"); return 0; }
int open(const char*, int) { op("script_open"); return 7; }
ssize_t read(int, void*, size_t) { op("script_read"); return 0; }
int close(int) { op("close"); return 0; }
int64_t getpid() { op("getpid"); return PARENT; }
auto signal(int signum, void(*handler)(int)) -> void(*)(int) {
    op("signal"); auto old = handlers.at(signum); handlers.at(signum) = handler; return old;
}
[[noreturn]] void _exit(int status) { throw Exited{status}; }
}
#define access fixture::access
#define open fixture::open
#define read fixture::read
#define close fixture::close
#define getpid fixture::getpid
#define signal fixture::signal
#define _exit fixture::_exit
namespace ker::process {
int64_t fork() { fixture::op("fork"); return fixture::fork_result; }
int64_t setpgid(int64_t a, int64_t b) {
    assert((a == 0 && b == 0) || (a == fixture::CHILD && a == b)); fixture::op("setpgid"); return 0;
}
int64_t kill(int64_t pid, int sig) {
    fixture::op("kill"); fixture::kills.emplace_back(pid, sig); return 0;
}
int64_t execve(const char* path, const char* const* args, const char* const* env) {
    fixture::op("execve"); assert(std::string_view(path) == "/bin/true" && env != nullptr);
    for (; *args; ++args) fixture::exec_args.emplace_back(*args);
    if (!fixture::exec_failed) throw fixture::Replaced{};
    return -1;
}
}
namespace perf {
'''

IO = r'''
struct WkiDisplayOptions {};
using fixture::op;
bool parse_display_arg(std::string_view, WkiDisplayOptions&) { return false; }
int64_t now_ms() { return fixture::time_ns / 1000000; }
std::optional<int64_t> read_clock_ns(int clock) {
    assert(clock == CLOCK_MONOTONIC); ++fixture::clock_calls;
    op("clock"); if (fixture::clock_calls == fixture::bad_clock_at) return std::nullopt;
    return fixture::time_ns;
}
void wall_sleep_ms(int ms) {
    assert(ms == SLEEP_SLICE_MS); op("sleep", ms * 1000000LL); ++fixture::sleeps;
}
int64_t waitpid_nohang(int64_t pid, int32_t*) {
    assert(pid == -1); op("wait"); ++fixture::wait_calls;
    if (fixture::cancel && fixture::wait_calls == 1) fixture::handlers.at(SIGTERM)(SIGTERM);
    if (fixture::wait_calls == fixture::exit_after) return fixture::CHILD;
    if (fixture::wait_calls > fixture::exit_after) return -10;
    return 0;
}
std::vector<StatInfo> collect_main_stats() {
    op("proc_scan", 1000000); ++fixture::scans;
    if (fixture::scans == 1 || fixture::wait_calls > fixture::descendant_after) return {};
    return {{.pid=fixture::CHILD, .pgid=fixture::CHILD, .comm="child", .state='R'},
            {.pid=fixture::CHILD+1, .pgid=fixture::CHILD, .comm="descendant", .state='R'}};
}
std::string read_proc_images(uint64_t) { op("images"); return "map"; }
std::string read_cmdline(uint64_t) { op("cmdline"); return "command"; }
ScopedFd open_write_trunc(std::string_view) { op("data_open", 100000); return ScopedFd(fixture::open_failed ? -1 : 8); }
bool write_all(int, std::string_view) { op("write", 10000); return !fixture::write_failed; }
bool write_section_timebase(int) { op("timebase", 100000); return true; }
bool set_recording_enabled(bool value) { assert(!value); op("stop"); return true; }
CaptureReceipt begin_capture(const char* filter) {
    op("start"); CaptureReceipt r; r.requested_filter = filter; r.control_failed = fixture::enable_failed;
    r.started = CaptureStatus{.session=0, .control_version=2, .cpus=8, .mask=256, .enabled=true}; return r;
}
bool drain_capture(int, CaptureReceipt&) { op("drain", 1000000); return true; }
void finish_capture(int, CaptureReceipt& r) {
    op("finish", 1000000); r.finished = r.started; r.finished->enabled=false; ++r.finished->control_version;
}
bool write_capture_receipt(int, CaptureReceipt& r) { op("receipt"); fixture::saved=r; return !r.write_failed; }
ssize_t write_section_wki_summary(int) { op("wki"); return 0; }
ssize_t write_section_ipc_stats(int) { op("ipc"); return 0; }
ssize_t write_section_contstat(int) { op("contstat"); return 0; }
ssize_t write_section_memacc_alloc_totals(int) { op("memacc"); return 0; }
bool write_section_peer_map(int) { op("peers"); return true; }
bool write_section_image_map(int, const std::vector<TrackedProc>*) { op("image_map"); return true; }
std::string proc_map_line(uint64_t, std::string_view, std::string_view) { op("proc_line"); return "line"; }
bool finalize_structured_perf_data() { op("structured"); return true; }
void cmd_sched(int, WkiDisplayOptions) { op("display"); }
} // namespace perf
'''

CASES = r'''
namespace fixture {
void reset(int64_t until = 1, int64_t descendant = 0) {
    time_ns=1000000000; fork_result=CHILD; wait_calls=sleeps=scans=clock_calls=bad_clock_at=0;
    exit_after=until; descendant_after=descendant; cancel=open_failed=write_failed=enable_failed=exec_failed=false;
    operations.clear(); stamps.clear(); exec_args.clear(); kills.clear(); handlers.fill(nullptr); saved={};
}
bool run(std::vector<std::string> args) {
    std::vector<char*> argv; for(auto& arg:args) argv.push_back(arg.data());
    return perf::cmd_run(static_cast<int>(argv.size()), argv.data());
}
void boundaries(const perf::RunObserverTrace& trace) {
    using P=perf::RunObserverPhase;
    for (const auto& [name, ns] : stamps) {
        if (ns < static_cast<int64_t>(trace.spans.front().begin_ns) || ns >= static_cast<int64_t>(trace.spans.back().end_ns)) continue;
        std::optional<P> phase;
        if(name=="data_open") phase=P::DATA_OPEN;
        if(name=="timebase") phase=P::TIMEBASE_WRITE;
        if(name=="sleep") phase=P::SLEEP;
        if(name=="drain") phase=P::RING_DRAIN;
        if(name=="stop") phase=P::CAPTURE_STOP;
        if(name=="finish") phase=P::FINAL_DRAIN;
        if(name=="images") phase=P::PROC_IMAGES;
        if(name=="cmdline") phase=P::PROC_CMDLINE;
        if(!phase) continue;
        assert(std::ranges::any_of(trace.spans, [&](const auto& s){return s.phase==*phase && s.begin_ns<=ns && ns<s.end_ns;}));
    }
    for (const auto& [name, ns] : stamps) {
        if (name != "proc_scan" || ns < static_cast<int64_t>(trace.spans.front().begin_ns) ||
            ns >= static_cast<int64_t>(trace.spans.back().end_ns)) continue;
        assert(std::ranges::any_of(trace.spans, [&](const auto& s){return (s.phase==P::PROC_STATS || s.phase==P::FINAL_STATS) && s.begin_ns<=ns && ns<s.end_ns;}));
    }
    assert(trace.parent_pid==PARENT && trace.child_pid==CHILD);
    assert(std::ranges::any_of(trace.spans, [](const auto& s){return s.phase==P::WAIT_PROBE && s.result==CHILD;}));
}
int main() {
    reset(); assert(run({"/bin/true"})); assert(!saved.run_observer && clock_calls==0);
    auto default_ops=operations;
    reset(); assert(run({"--observer-stages", "/bin/true"})); assert(saved.run_observer);
    auto trace=*saved.run_observer; assert(perf::run_observer_complete(trace)); boundaries(trace);
    auto measured_ops=operations; std::erase(measured_ops, "clock"); std::erase(measured_ops, "getpid");
    assert(measured_ops==default_ops);
    assert(std::ranges::find(operations, "timebase") < std::ranges::find(operations, "wait"));
    assert(stamps.back().second>static_cast<int64_t>(trace.spans.back().end_ns));
    reset(2, 3); assert(run({"--observer-stages", "--structured", "/bin/true"}));
    assert(wait_calls>=5 && sleeps==2 && kills.empty()); boundaries(*saved.run_observer);
    // Repeated group scans now include detailed I/O phases and can hit the same bound.
    assert(saved.run_observer->dropped_spans>0 && !perf::run_observer_complete(*saved.run_observer));
    reset(50, 0); assert(run({"--observer-stages", "/bin/true"}));
    assert(saved.run_observer->spans.size()==perf::RUN_OBSERVER_SPAN_LIMIT && saved.run_observer->dropped_spans>0);
    assert(clock_calls==perf::RUN_OBSERVER_SPAN_LIMIT+1 && !saved.run_observer->clock_failed);
    assert(!perf::run_observer_complete(*saved.run_observer) && saved.run_observer->finished);
    assert(std::ranges::count(operations, "drain")>=1);
    reset(230, 500); cancel=true; assert(run({"--observer-stages", "/bin/true"}));
    assert((kills==std::vector<std::pair<int64_t,int>>{{-CHILD,SIGTERM},{-CHILD,SIGKILL}}));
    for(int s:{SIGINT,SIGTERM,SIGHUP,SIGQUIT}) assert(handlers.at(s)==nullptr);
    reset(); bad_clock_at=3; assert(run({"--observer-stages", "/bin/true"}));
    assert(saved.run_observer->clock_failed && !perf::run_observer_complete(*saved.run_observer));
    reset(); open_failed=true; assert(!run({"--observer-stages", "/bin/true"})); assert(wait_calls>=3);
    reset(); write_failed=true; assert(!run({"--observer-stages", "/bin/true"})); assert(saved.write_failed);
    reset(); fork_result=-12; assert(!run({"--observer-stages", "/bin/true"})); assert(clock_calls==0 && wait_calls==0);
    reset(); enable_failed=true; assert(!run({"--observer-stages", "/bin/true"})); assert(clock_calls==0 && wait_calls==0);
    for(bool enabled:{false,true}) {
        reset(); fork_result=0;
        try { run(enabled?std::vector<std::string>{"--observer-stages","/bin/true","--","--observer-stages"}:
                          std::vector<std::string>{"/bin/true","--","--observer-stages"}); assert(false); }
        catch(const Replaced&) {}
        assert((exec_args==std::vector<std::string>{"/bin/true","--observer-stages"}) && clock_calls==0);
    }
    reset(); fork_result=0; exec_failed=true;
    try { run({"--observer-stages","/bin/true"}); assert(false); }
    catch(const Exited& e) { assert(e.status==127 && clock_calls==0); }
    std::puts("actual perf run: phase boundaries, disabled behavior, argv, descendants, cancellation, I/O, clocks and clipping pass");
    return 0;
}
}
int main() { return fixture::main(); }
'''


def main():
    header = (ROOT / 'modules/perf/src/perf.hpp').read_text()
    command = (ROOT / 'modules/perf/src/run.cpp').read_text()
    assert command.startswith('#include "perf.hpp"\n')
    constants = '\n'.join(line for line in header.splitlines() if line.startswith('constexpr ') and
                          any(name in line for name in ('DEFAULT_MAX_EVENTS', 'MIN_CPU_PCT', 'MILLISECONDS_PER_SECOND',
                              'SLEEP_SLICE_MS', 'PERF_RUN_', 'DRAIN_INTERVAL_MS', 'EXEC_FAILURE_EXIT_CODE',
                              'EXITED_STATE', 'PERF_DATA_FILE', 'SECTION_')))
    types = '\n'.join(declaration(header, name) for name in ('ScopedFd', 'StatInfo', 'TrackedProc', 'CpuRow'))
    code = PRELUDE + constants + types + IO + command.split('\n', 1)[1] + CASES
    with tempfile.TemporaryDirectory(prefix='wos-perf-run-') as temp:
        source, binary = Path(temp) / 'test.cpp', Path(temp) / 'test'
        source.write_text(code)
        subprocess.run([shutil.which('c++', path=os.defpath), '-std=c++23', '-O1', '-g',
                        '-fsanitize=address,undefined', '-I' + str(ROOT / 'modules/perf/src'), str(source),
                        str(ROOT / 'modules/perf/src/observer.cpp'), '-o', str(binary)], check=True)
        subprocess.run([str(binary)], check=True, env={**os.environ, 'ASAN_OPTIONS': 'detect_leaks=0'})


if __name__ == '__main__':
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    main()
