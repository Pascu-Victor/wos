#!/usr/bin/env python3
"""Execute actual record/drain/survey bodies under finite userspace I/O ports.

WOS_PERF_SOURCE_ROOT selects a private candidate tree. No Host children or files
are opened by the extracted C++ command. This script itself compiles/runs only
when explicitly invoked by the later test owner; source generation is pure.
"""
from pathlib import Path
import os
import re
import resource
import shutil
import subprocess
import tempfile

ROOT = next(p for p in Path(__file__).resolve().parents if (p / '.github/instructions').is_dir())
SOURCE_ROOT = Path(os.environ.get('WOS_PERF_SOURCE_ROOT', str(ROOT)))


def function(source, name):
    match = re.search(r'^(?:auto|void) ' + name + r'\([^)]*\)(?: -> [^{\n]+)? \{', source, re.M)
    assert match, name
    end = source.index('{', match.start()) + 1
    depth = 1
    while depth:
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    return source[match.start():end]


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
#include <cerrno>
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <limits>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <system_error>
#include <sys/types.h>
#include <utility>
#include <vector>
#include "capture.hpp"
namespace fixture { std::vector<std::string> messages; }
namespace std { template<class... Args> void println(std::string_view text, Args&&...) { fixture::messages.emplace_back(text); } }
namespace perf {
struct dirent { char d_name[64]{}; };
struct DIR { std::string path; std::vector<std::string> names; size_t index{}; dirent entry; };
auto opendir(const char*) -> DIR*;
auto readdir(DIR*) -> dirent*;
int closedir(DIR*);
int close(int);
'''

PORTS = r'''
std::deque<std::string> chunks;
std::deque<long> write_results;
CaptureStatus status;
CaptureReceipt saved_receipt;
std::vector<TrackedProc> saved_tracked;
std::string saved;
std::vector<std::string> operations;
bool read_error, missing_status, endless, open_failed, timebase_failed, enable_failed, structured_failed;
bool live_directories, initial_directories, proc_open_failed, tid_gone, main_only, sleep_error, sleep_eintr;
unsigned event_reads, status_reads, enable_calls, disable_calls, sleeps, clock_calls, bad_clock_at, invalid_clock_at;
unsigned change_on_event, change_on_status, change_kind, enable_backlog, backlog, dir_owners;
int64_t time_ns, clock_cost_ns, event_cost_ns, image_cost_ns, cmdline_cost_ns, enable_cost_ns, status_cost_ns;
int64_t first_clock_value;
void op(std::string name) { operations.push_back(std::move(name)); }
int close(int) { op("close"); return 0; }
auto write(int, const char* data, size_t count) -> long {
    op("write"); long result=static_cast<long>(count);
    if(!write_results.empty()){result=write_results.front();write_results.pop_front();}
    if(result<0){errno=static_cast<int>(-result);return -1;}
    assert(static_cast<size_t>(result)<=count);saved.append(data,static_cast<size_t>(result));return result;
}
int fixture_clock_gettime(int clock, timespec* stamp) {
    assert(clock==CLOCK_MONOTONIC); ++clock_calls; op("clock");
    if(clock_calls==bad_clock_at) return -1;
    auto value=clock_calls==1 && first_clock_value>=0 ? first_clock_value : time_ns;
    stamp->tv_sec=value/1000000000; stamp->tv_nsec=value%1000000000;
    if(clock_calls==invalid_clock_at) stamp->tv_nsec=1000000000;
    time_ns+=clock_cost_ns; return 0;
}
int fixture_nanosleep(const timespec* duration, timespec*) {
    assert(duration->tv_sec==0 && duration->tv_nsec>0 && duration->tv_nsec<=10000000);
    op("sleep"); ++sleeps;
    if(sleep_error){errno=EIO;return -1;}
    if(sleep_eintr){sleep_eintr=false;errno=EINTR;return -1;}
    time_ns+=duration->tv_nsec; return 0;
}
inline constexpr auto clock_gettime = &fixture_clock_gettime;
inline constexpr auto nanosleep = &fixture_nanosleep;
auto read_clock_ns(int) -> std::optional<int64_t> { return time_ns; }
auto opendir(const char* path) -> DIR* {
    op(std::string("opendir:")+path); std::string p(path);
    std::vector<std::string> names;
    if(p=="/proc") { if(proc_open_failed)return nullptr;
        if((live_directories && enable_calls>0 && status.enabled) || (initial_directories && enable_calls==0)) names={".","100"}; }
    else if(p=="/proc/100/task") { if(main_only)return nullptr; names={".","101"}; }
    else return nullptr;
    ++dir_owners;return new DIR{p,std::move(names),0,{}};
}
auto readdir(DIR* dir) -> dirent* {
    op("readdir:"+dir->path);
    if(dir->index==dir->names.size())return nullptr;
    auto const& name=dir->names.at(dir->index++);assert(name.size()<sizeof(dir->entry.d_name));
    std::strcpy(dir->entry.d_name,name.c_str());return &dir->entry;
}
int closedir(DIR* dir) { op("closedir:"+dir->path);assert(dir_owners>0);--dir_owners;delete dir;return 0; }
std::string status_text() {
    return "perf_capture version=1 session="+std::to_string(status.session)+" control_version="+std::to_string(status.control_version)+
        " enabled="+std::to_string(status.enabled)+" mask="+std::to_string(status.mask)+" cpus="+std::to_string(status.cpus)+
        " written="+std::to_string(status.written)+" consumed="+std::to_string(status.consumed)+
        " overwritten="+std::to_string(status.overwritten)+" pending="+std::to_string(status.pending)+"\n";
}
auto read_file(std::string_view path,size_t) -> std::optional<std::string> {
    if(path==KPERFSTATUS_PATH){
        op("status");time_ns+=status_cost_ns;++status_reads;
        if(status_reads==change_on_status){
            if(change_kind==0)++status.session;
            if(change_kind==1)++status.control_version;
            if(change_kind==2)status.mask^=1;
            if(change_kind==3)++status.cpus;
            if(change_kind==4)status.enabled=true;
            if(change_kind==5)missing_status=true;
        }
        return missing_status?std::nullopt:std::optional<std::string>{status_text()};
    }
    if(path==KPERF_PATH){
        op("events");++event_reads;time_ns+=event_cost_ns;if(read_error)return std::nullopt;
        std::string text;
        if(endless)text="event\n";
        else if(!chunks.empty()){text=std::move(chunks.front());chunks.pop_front();}
        else if(backlog){--backlog;text="event\n";}
        assert(text.size()<=PERF_PROC_READ_LIMIT);
        auto count=static_cast<uint64_t>(std::count(text.begin(),text.end(),'\n'));
        if(!endless && !missing_status){assert(count<=status.pending);status.pending-=count;status.consumed+=count;}
        if(event_reads==change_on_event){
            if(change_kind==0)++status.session;
            if(change_kind==1)++status.control_version;
            if(change_kind==2)status.mask^=1;
            if(change_kind==3)++status.cpus;
            if(change_kind==4)status.enabled=false;
            if(change_kind==5)missing_status=true;
        }
        return text;
    }
    op("read:"+std::string(path));
    if(path.ends_with("/images")){time_ns+=image_cost_ns;return "image_for_"+std::string(path)+"\n";}
    if(path.ends_with("/cmdline")){time_ns+=cmdline_cost_ns;return std::string("worker\0arg",10);}
    if(path=="/proc/100/task/101/stat"){
        if(tid_gone)return std::nullopt;
        tid_gone=true;return "101 (short_lived_tid) R 1 100 0 0 0 0 0 0 0 0 11 12\n";
    }
    if(path=="/proc/100/stat")return "100 (main_fallback) R 1 100 0 0 0 0 0 0 0 0 11 12\n";
    return std::nullopt;
}
auto open_write_trunc(std::string_view) -> ScopedFd {op("open");return ScopedFd(open_failed?-1:7);}
bool write_section_timebase(int) {op("timebase");return !timebase_failed;}
bool set_recording_enabled(bool enabled,const char* = nullptr) {
    op(enabled?"enable":"disable");
    if(enabled){++enable_calls;time_ns+=enable_cost_ns;if(enable_failed)return false;
        ++status.session;++status.control_version;status.enabled=true;status.mask=14;status.cpus=2;
        status.written=status.pending=enable_backlog;status.consumed=status.overwritten=0;backlog=enable_backlog;
    }else{++disable_calls;status.enabled=false;++status.control_version;}
    return true;
}
ssize_t write_section_wki_summary(int){return 0;}
ssize_t write_section_ipc_stats(int){return 0;}
ssize_t write_section_contstat(int){return 0;}
ssize_t write_section_memacc_alloc_totals(int){return 0;}
bool write_section_peer_map(int){return true;}
bool write_capture_receipt(int,CaptureReceipt& receipt){op("receipt");saved_receipt=receipt;return capture_stream_complete(receipt);}
bool finalize_structured_perf_data(){op("structured");return !structured_failed;}
'''

LEGACY_CASES = '\nvoid reset(){chunks.clear();write_results.clear();saved.clear();status={.session=1,.control_version=2,.cpus=2};\n    read_error=missing_status=endless=false;event_reads=0;}\nint main(){\n    reset();CaptureReceipt r;\n    chunks={std::string(65000,\'x\')+"\\n",std::string(64000,\'y\')+"\\n","last\\n"};\n    status.written=status.pending=3;\n    finish_capture(1,r);\n    assert(r.saved_events==3 && r.saved_bytes==129007 && r.finished->pending==0 && event_reads==4);\n    assert(saved.ends_with("last\\n") && !r.limit_reached && !r.read_failed);\n    reset();r={};status.written=status.pending=1;chunks={"event\\n"};write_results={-EINTR,2,4};\n    finish_capture(1,r);assert(saved=="event\\n" && r.saved_events==1 && !r.write_failed);\n    reset();r={};status.written=status.pending=1;chunks={"event\\n"};write_results={2,-EIO};\n    finish_capture(1,r);assert(r.write_failed && r.saved_events==0 && saved=="ev" && event_reads==1);\n    reset();r={};status.written=status.pending=1;chunks={"event\\n"};write_results={0};\n    finish_capture(1,r);assert(r.write_failed && r.saved_events==0);\n    reset();r={};read_error=true;finish_capture(1,r);assert(r.read_failed && r.saved_events==0);\n    reset();r={};chunks={"partial"};finish_capture(1,r);assert(r.read_failed && saved.empty());\n    reset();r={};status.written=status.pending=1;chunks={"event\\n"};r.saved_bytes=CAPTURE_EVENT_BYTE_LIMIT-1;\n    finish_capture(1,r);assert(r.limit_reached && r.saved_events==0 && saved.empty());\n    reset();r={};status.enabled=true;finish_capture(1,r);assert(event_reads==0 && r.finished->enabled);\n    reset();r={};status.written=status.pending=1; // Head too large for a procfs snapshot: empty with pending >0.\n    finish_capture(1,r);assert(event_reads==1 && r.finished->pending==1 && r.saved_events==0);\n    reset();r={};missing_status=true;chunks={"old kernel\\n","second\\n"};finish_capture(1,r);\n    assert(r.saved_events==2 && !r.finished && event_reads==3);\n    reset();r={};missing_status=true;endless=true;finish_capture(1,r);\n    assert(event_reads==CAPTURE_FINAL_READ_LIMIT && r.limit_reached);\n    puts("perf capture I/O: multi-snapshot final drain, EINTR/partial writes, failures, live capture and bounds pass");\n    return 0;\n}\n} // namespace perf\n'

RECORD_CASES = r'''
namespace perf {
void record_reset() {
    reset();status={.session=1,.control_version=2,.cpus=2,.mask=14,.enabled=true};
    saved_receipt={};saved_tracked.clear();operations.clear();fixture::messages.clear();
    open_failed=timebase_failed=enable_failed=structured_failed=live_directories=initial_directories=proc_open_failed=tid_gone=main_only=sleep_error=sleep_eintr=false;
    status_reads=enable_calls=disable_calls=sleeps=clock_calls=bad_clock_at=invalid_clock_at=0;
    change_on_event=change_on_status=enable_backlog=backlog=0;change_kind=0;assert(dir_owners==0);
    time_ns=1000000000;clock_cost_ns=1000;event_cost_ns=image_cost_ns=cmdline_cost_ns=enable_cost_ns=status_cost_ns=0;first_clock_value=-1;
}
CaptureReceipt live_receipt(){CaptureReceipt r;r.started=status;return r;}
void record_cases() {
    record_reset();open_failed=true;assert(!cmd_record(25,nullptr,false));assert(enable_calls==0);
    record_reset();timebase_failed=true;assert(!cmd_record(25,nullptr,false));assert(enable_calls==0);
    record_reset();write_results={0};assert(!cmd_record(25,nullptr,false));assert(enable_calls==0);
    record_reset();proc_open_failed=true;assert(!cmd_record(25,nullptr,false));assert(enable_calls==0 && dir_owners==0);
    record_reset();enable_failed=true;assert(!cmd_record(25,nullptr,false));assert(enable_calls==1 && disable_calls==0);
    record_reset();initial_directories=true;assert(cmd_record(25,nullptr,false));
    assert(std::ranges::find(operations,"read:/proc/101/images")<std::ranges::find(operations,"enable"));
    assert(std::ranges::find(operations,"read:/proc/101/cmdline")<std::ranges::find(operations,"enable"));
    record_reset();enable_backlog=70;assert(cmd_record(25,nullptr,false));
    assert(saved_receipt.saved_events==70 && sleeps>0 && disable_calls==1 && dir_owners==0);
    assert(std::ranges::find(fixture::messages,"perf: recording stopped.")!=fixture::messages.end());
    assert(std::ranges::find(fixture::messages,"perf: stopping was not confirmed.")==fixture::messages.end());
    auto enable=std::ranges::find(operations,"enable"),sleep=std::ranges::find(operations,"sleep");
    assert(std::ranges::find(operations,"open")<enable && std::ranges::find(operations,"timebase")<enable);
    assert(std::count(enable,sleep,"events")>=71); // all full quanta plus empty precede the first sleep/metadata.
    record_reset();enable_backlog=4;live_directories=true;assert(cmd_record(100,nullptr,false));
    assert(saved.find("pid=101 comm=short_lived_tid cmd=worker arg")!=std::string::npos);
    assert(saved.find("pid=101 image_for_/proc/101/images")!=std::string::npos);
    assert(dir_owners==0); // exited TID retained even though the stopped final live scan is empty.
    auto image=std::ranges::find(operations,"read:/proc/101/images");assert(image!=operations.end());
    assert(std::find(operations.begin(),image,"events")!=image);
    assert(image+1!=operations.end() && *(image+1)=="clock");
    record_reset();live_directories=true;main_only=true;assert(cmd_record(100,nullptr,false));
    assert(saved.find("pid=100 comm=main_fallback")!=std::string::npos && dir_owners==0);
    record_reset();bad_clock_at=1;assert(!cmd_record(25,nullptr,false));assert(enable_calls==0 && disable_calls==0);
    record_reset();invalid_clock_at=1;assert(!cmd_record(25,nullptr,false));assert(enable_calls==0 && disable_calls==0);
    record_reset();first_clock_value=std::numeric_limits<int64_t>::max();assert(!cmd_record(25,nullptr,false));
    assert(enable_calls==0 && disable_calls==0);
    for(bool slow_status:{false,true}){
        record_reset();if(slow_status)status_cost_ns=200000000;else enable_cost_ns=200000000;
        assert(cmd_record(25,nullptr,false));assert(sleeps==0 && enable_calls==1 && disable_calls==1);
        auto stop=std::ranges::find(operations,"disable");
        assert(stop!=operations.end() && std::find(operations.begin(),stop,"events")==stop);
        assert(event_reads==1); // stopped final empty read only; live epoch was never renewed.
    }
    record_reset();sleep_error=true;assert(!cmd_record(25,nullptr,false));assert(saved_receipt.read_failed);
    record_reset();sleep_eintr=true;assert(cmd_record(25,nullptr,false));assert(sleeps>1);
    record_reset();structured_failed=true;assert(!cmd_record(25,nullptr,true));assert(std::ranges::find(operations,"structured")!=operations.end());
    record_reset();enable_backlog=1;change_on_event=1;change_kind=0;
    assert(!cmd_record(25,nullptr,false));assert(saved_receipt.control_failed && disable_calls==0);
    assert(std::ranges::find(fixture::messages,"perf: recording stopped.")==fixture::messages.end());
    assert(std::ranges::find(fixture::messages,"perf: stopping was not confirmed.")!=fixture::messages.end());
    for(unsigned kind=0;kind<6;++kind){
        record_reset();auto r=live_receipt();status.written=status.pending=1;chunks={"e\n"};
        change_on_event=1;change_kind=kind;
        assert(drain_capture_quantum(7,r,time_ns+1000000)==CaptureDrainResult::ERROR && r.control_failed);
        assert(event_reads==1);
    }
    record_reset();auto r=live_receipt();backlog=70;status.written=status.pending=70;
    assert(drain_capture_quantum(7,r,time_ns+100000000)==CaptureDrainResult::FULL && event_reads==32);
    assert(drain_capture_quantum(7,r,time_ns+100000000)==CaptureDrainResult::FULL && event_reads==64);
    assert(drain_capture_quantum(7,r,time_ns+100000000)==CaptureDrainResult::EMPTY && event_reads==71 && r.saved_events==70);
    record_reset();r=live_receipt();status.written=status.pending=32;
    for(int i=0;i<32;++i)chunks.push_back(std::string(65535,'x')+"\n");
    assert(drain_capture_quantum(7,r,time_ns+100000000)==CaptureDrainResult::FULL);
    assert(event_reads==32 && r.saved_bytes==CAPTURE_QUANTUM_BYTE_LIMIT);
    record_reset();r=live_receipt();status.written=status.pending=1;
    assert(drain_capture_quantum(7,r,time_ns+100000000)==CaptureDrainResult::FULL && event_reads==32);
    record_reset();r=live_receipt();++status.session;
    assert(drain_capture_quantum(7,r,time_ns+100000000)==CaptureDrainResult::ERROR && r.control_failed && event_reads==0);
    record_reset();r=live_receipt();clock_cost_ns=100000000;
    assert(drain_capture_quantum(7,r,time_ns+10000000)==CaptureDrainResult::DEADLINE && event_reads==0);
    record_reset();r=live_receipt();status.written=status.pending=1;chunks={"e\n"};event_cost_ns=100000000;
    assert(drain_capture_quantum(7,r,time_ns+10000000)==CaptureDrainResult::DEADLINE && event_reads==1 && r.saved_events==1);
    record_reset();time_ns=-1;assert(!capture_record_clock());
    record_reset();r=live_receipt();assert(drain_capture_quantum(7,r,time_ns)==CaptureDrainResult::DEADLINE && event_reads==0);
    record_reset();r=live_receipt();bad_clock_at=1;
    assert(drain_capture_quantum(7,r,time_ns+1000000)==CaptureDrainResult::ERROR && r.read_failed && event_reads==0);
    record_reset();r=live_receipt();bad_clock_at=4;status.written=status.pending=1;chunks={"e\n"};
    assert(drain_capture_quantum(7,r,time_ns+1000000)==CaptureDrainResult::ERROR && r.read_failed);
    record_reset();r=live_receipt();status.written=status.pending=1;chunks={"e\n"};write_results={-EINTR,1,1};
    assert(drain_capture_quantum(7,r,time_ns+1000000)==CaptureDrainResult::EMPTY && r.saved_events==1);
    record_reset();r=live_receipt();read_error=true;
    assert(drain_capture_quantum(7,r,time_ns+1000000)==CaptureDrainResult::ERROR && r.read_failed);
    record_reset();r=live_receipt();status.written=status.pending=1;chunks={"e\n"};write_results={1,-EIO};
    assert(drain_capture_quantum(7,r,time_ns+1000000)==CaptureDrainResult::ERROR && r.write_failed && saved=="e");
    record_reset();r=live_receipt();status.written=status.pending=1;chunks={"e\n"};r.saved_bytes=CAPTURE_EVENT_BYTE_LIMIT-1;
    assert(drain_capture_quantum(7,r,time_ns+1000000)==CaptureDrainResult::ERROR && r.limit_reached);
    record_reset();r=live_receipt();status.written=status.pending=1;chunks={"partial"};
    assert(drain_capture_quantum(7,r,time_ns+1000000)==CaptureDrainResult::ERROR && r.read_failed);
    record_reset();live_directories=true;image_cost_ns=200000000;assert(cmd_record(100,nullptr,false));
    assert(!saved_receipt.read_failed); // ordinary expiry is not fabricated I/O failure.
    assert(saved.find("pid=101 comm=short_lived_tid")!=std::string::npos);
    auto image_end=std::ranges::find(operations,"read:/proc/101/images");
    assert(image_end!=operations.end() && std::find(image_end,operations.end(),"read:/proc/101/cmdline")==operations.end());
    record_reset();r=live_receipt();r.finished=status;r.finished->enabled=false;++r.finished->control_version;
    assert(capture_stream_complete(r));r.finished->written=1;r.finished->overwritten=1;
    assert(!capture_stream_complete(r));r.finished->overwritten=0;r.finished->pending=1;
    assert(!capture_stream_complete(r));
    puts("actual record/drain/survey source: setup, backlog, clocks, session, metadata and loss refusal pass");
}
} // namespace perf
int main(){perf::main();perf::record_cases();return 0;}
'''

OWNED_FINAL_CASES = r'''
namespace perf {
void record_reset();
CaptureReceipt live_receipt();
CaptureReceipt owned_stop(unsigned pending) {
    record_reset();auto receipt=live_receipt();
    status.enabled=false;++status.control_version;status.written=status.pending=pending;
    return receipt;
}
void owned_final_cases() {
    auto r=owned_stop(1);assert(check_record_capture(r,false));
    // Actual reachable order: own STOPPED confirmation, replacement start+stop,
    // then finish's FIRST fresh BEFORE sees the stopped replacement.
    ++status.session;status.control_version+=2;chunks={"replacement\n"};
    auto const BEFORE_READS=event_reads;finish_capture(7,r);
    assert(r.control_failed && event_reads==BEFORE_READS && r.saved_events==0 && chunks.size()==1);
    assert(r.finished && r.finished->session==status.session && r.finished->control_version==status.control_version);
    assert(!capture_stream_complete(r));
    for(unsigned kind=0;kind<6;++kind){
        r=owned_stop(1);chunks={"untouched\n"};change_on_status=1;change_kind=kind;
        finish_capture(7,r);assert(r.control_failed && event_reads==0 && r.saved_events==0 && chunks.size()==1);
        assert(!capture_stream_complete(r));
        if(kind==5)assert(!r.finished);else assert(r.finished);
    }
    for(unsigned kind=0;kind<6;++kind){
        r=owned_stop(2);chunks={"first\n","untouched\n"};change_on_status=3;change_kind=kind;
        // BEFORE(1), first snapshot, post(2), next pre(3) observes replacement.
        finish_capture(7,r);assert(r.control_failed && event_reads==1 && r.saved_events==1 && chunks.size()==1);
        assert(saved=="first\n" && !capture_stream_complete(r));
    }
    for(unsigned kind=0;kind<6;++kind){
        r=owned_stop(2);chunks={"first\n","untouched\n"};change_kind=kind;
        if(kind==4)change_on_status=2;else change_on_event=1;
        // Replacement inside the unavoidable status/snapshot gap is detected by
        // the fresh post observation; no SECOND consuming snapshot is allowed.
        finish_capture(7,r);assert(r.control_failed && event_reads==1 && r.saved_events==1 && chunks.size()==1);
        assert(!capture_stream_complete(r));
    }
    for(unsigned invalid=0;invalid<3;++invalid){
        r=owned_stop(1);chunks={"untouched\n"};
        if(invalid==0)r.started->enabled=false;
        if(invalid==1)r.started->cpus=0;
        if(invalid==2){r.started->control_version=UINT64_MAX;status.control_version=0;}
        finish_capture(7,r);assert(r.control_failed && event_reads==0 && chunks.size()==1 && !capture_stream_complete(r));
    }
    r=owned_stop(1);chunks={"untouched\n"};r.control_failed=true;
    finish_capture(7,r);assert(r.control_failed && event_reads==0 && chunks.size()==1);
    r=owned_stop(3);chunks={"one\n","two\n","three\n"};finish_capture(7,r);
    assert(event_reads==4 && r.saved_events==3 && saved=="one\ntwo\nthree\n");
    assert(!r.control_failed && !r.read_failed && !r.write_failed && !r.limit_reached);
    assert(r.finished && r.finished->pending==0 && capture_stream_complete(r));
    puts("actual owned final drain: first/later replacement refusal, post-read detection, missing/overflow and valid own stop pass");
}
} // namespace perf
'''

def generate():
    capture = (SOURCE_ROOT / 'modules/perf/src/capture.cpp').read_text()
    support = (SOURCE_ROOT / 'modules/perf/src/support.cpp').read_text()
    header = (SOURCE_ROOT / 'modules/perf/src/perf.hpp').read_text()
    policy = (ROOT / 'modules/perf/src/capture_policy.cpp').read_text()
    constants = '\n'.join(line for line in header.splitlines() if line.startswith('constexpr ') and any(
        name in line for name in ('DEFAULT_SAMPLE_MS', 'NANOSECONDS_PER_', 'SLEEP_SLICE_MS', 'PROC_STAT_SKIP_FIELDS',
            'PARSE_BASE_DECIMAL', 'PROC_READ_CAPACITY', 'PERF_DRAIN_CAPACITY', 'PERF_PROC_READ_LIMIT', 'PERF_DATA_FILE', 'PROC_ROOT',
            'PROC_STAT_SUFFIX', 'PROC_TASK_SUFFIX', 'PROC_CMDLINE_SUFFIX', 'PROC_IMAGES_SUFFIX', 'KPERF_PATH', 'KPERFSTATUS_PATH', 'SECTION_')))
    types = '\n'.join(declaration(header, name) for name in ('ScopedFd', 'ScopedDir', 'StatInfo', 'TrackedProc'))
    code = PRELUDE + constants + types + PORTS
    code += function(policy, 'parse_capture_status') + function(policy, 'capture_stream_complete')
    # Actual parsing/path helpers, TID enumeration, cursor, tracked snapshots and final image merge.
    code += '\nauto parse_u64(std::string_view,int base=PARSE_BASE_DECIMAL)->uint64_t;\n'
    code += function(support, 'write_all') + function(support, 'parse_u64')
    for name in ('trim_left', 'next_token', 'next_line', 'is_all_digits', 'parse_stat', 'build_proc_task_path'):
        code += function(support, name)
    # build_proc_path is overloaded; preserve both exact definitions in source order.
    for signature in ('auto build_proc_path(std::string_view', 'auto build_proc_path(uint64_t'):
        code += function(support[support.index(signature):], 'build_proc_path')
    code += 'template <typename Func>\n' + function(support, 'emit_proc_stat')
    for name in ('for_each_process_stat', 'for_each_process_main_stat'):
        code += 'template <typename Func>\n' + function(support, name)
    for name in ('collect_stats', 'collect_main_stats', 'read_cmdline', 'read_proc_images', 'proc_map_line',
                 'write_image_map_snapshot', 'write_section_image_map'):
        code += function(support, name)
    for name in ('read_capture_status', 'capture_status_matches', 'begin_capture', 'capture_record_clock', 'check_record_capture', 'drain_capture_quantum',
                 'drain_capture', 'finish_capture'):
        code += function(capture, name)
    code += declaration(support, 'RecordProcCursor')
    code += 'template <typename Service>\n' + function(support, 'update_record_tracked')
    code += function(support, 'sleep_record_slice') + function(support, 'cmd_record')
    return code + LEGACY_CASES + OWNED_FINAL_CASES + RECORD_CASES.replace(
        "perf::record_cases();return 0;", "perf::record_cases();perf::owned_final_cases();return 0;")


def main():
    with tempfile.TemporaryDirectory(prefix='wos-perf-io-') as temp:
        source, binary = Path(temp) / 'test.cpp', Path(temp) / 'test'
        source.write_text(generate())
        subprocess.run([shutil.which('c++', path=os.defpath), '-std=c++23', '-O1', '-g', '-fsanitize=address,undefined',
            '-I' + str(SOURCE_ROOT / 'modules/perf/src'), '-I' + str(ROOT / 'modules/perf/src'), str(source), '-o', str(binary)], check=True)
        subprocess.run([str(binary)], check=True, env={**os.environ, 'ASAN_OPTIONS': 'detect_leaks=0'})


if __name__ == '__main__':
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    main()
