#!/usr/bin/env python3
"""Exercise the real final drain and checked-write code with controlled I/O."""
from pathlib import Path
import os
import re
import resource
import shutil
import subprocess
import tempfile
ROOT=Path(__file__).resolve().parents[3]

def function(source,name):
    match=re.search(r'^(?:auto|void) '+name+r'\([^)]*\)(?: -> [^{\n]+)? \{',source,re.M)
    assert match,name
    end=source.index('{',match.start())+1;depth=1
    while depth:
        depth+=(source[end]=='{')-(source[end]=='}');end+=1
    return source[match.start():end]

PRELUDE=r'''
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <optional>
#include <string>
#include <string_view>
#include "capture.hpp"
namespace perf {
constexpr std::string_view KPERF_PATH="events", KPERFSTATUS_PATH="status";
constexpr size_t PERF_DRAIN_CAPACITY=65536, PROC_READ_CAPACITY=512;
constexpr int CLOCK_MONOTONIC=1;
std::deque<std::string> chunks;
std::deque<long> write_results;
CaptureStatus status;
std::string saved;
bool read_error, missing_status, endless;
unsigned event_reads;
auto write(int,std::string::const_pointer data,size_t count)->long {
    long result=count;
    if(!write_results.empty()){result=write_results.front();write_results.pop_front();}
    if(result<0){errno=-result;return -1;}
    assert(static_cast<size_t>(result)<=count);saved.append(data,result);return result;
}
auto read_file(std::string_view path,size_t)->std::optional<std::string> {
    if(path==KPERFSTATUS_PATH) return missing_status?std::nullopt:std::optional<std::string>{"status"};
    ++event_reads; if(read_error)return std::nullopt;
    if(endless)return "event\n";
    if(chunks.empty())return "";
    auto text=chunks.front();chunks.pop_front();
    auto count=std::count(text.begin(),text.end(),'\n');status.pending-=count;status.consumed+=count;
    return text;
}
auto parse_capture_status(std::string_view)->std::optional<CaptureStatus>{return status;}
auto read_clock_ns(int)->std::optional<int64_t>{return 123;}
'''
CASES=r'''
void reset(){chunks.clear();write_results.clear();saved.clear();status={.session=1,.control_version=2,.cpus=2};
    read_error=missing_status=endless=false;event_reads=0;}
int main(){
    reset();CaptureReceipt r;
    chunks={std::string(65000,'x')+"\n",std::string(64000,'y')+"\n","last\n"};
    status.written=status.pending=3;
    finish_capture(1,r);
    assert(r.saved_events==3 && r.saved_bytes==129007 && r.finished->pending==0 && event_reads==4);
    assert(saved.ends_with("last\n") && !r.limit_reached && !r.read_failed);
    reset();r={};status.written=status.pending=1;chunks={"event\n"};write_results={-EINTR,2,4};
    finish_capture(1,r);assert(saved=="event\n" && r.saved_events==1 && !r.write_failed);
    reset();r={};status.written=status.pending=1;chunks={"event\n"};write_results={2,-EIO};
    finish_capture(1,r);assert(r.write_failed && r.saved_events==0 && saved=="ev" && event_reads==1);
    reset();r={};status.written=status.pending=1;chunks={"event\n"};write_results={0};
    finish_capture(1,r);assert(r.write_failed && r.saved_events==0);
    reset();r={};read_error=true;finish_capture(1,r);assert(r.read_failed && r.saved_events==0);
    reset();r={};chunks={"partial"};finish_capture(1,r);assert(r.read_failed && saved.empty());
    reset();r={};status.written=status.pending=1;chunks={"event\n"};r.saved_bytes=CAPTURE_EVENT_BYTE_LIMIT-1;
    finish_capture(1,r);assert(r.limit_reached && r.saved_events==0 && saved.empty());
    reset();r={};status.enabled=true;finish_capture(1,r);assert(event_reads==0 && r.finished->enabled);
    reset();r={};status.written=status.pending=1; // Head too large for a procfs snapshot: empty with pending >0.
    finish_capture(1,r);assert(event_reads==1 && r.finished->pending==1 && r.saved_events==0);
    reset();r={};missing_status=true;chunks={"old kernel\n","second\n"};finish_capture(1,r);
    assert(r.saved_events==2 && !r.finished && event_reads==3);
    reset();r={};missing_status=true;endless=true;finish_capture(1,r);
    assert(event_reads==CAPTURE_FINAL_READ_LIMIT && r.limit_reached);
    puts("perf capture I/O: multi-snapshot final drain, EINTR/partial writes, failures, live capture and bounds pass");
    return 0;
}
} // namespace perf
int main(){return perf::main();}
'''
def main():
    capture=(ROOT/'modules/perf/src/capture.cpp').read_text()
    support=(ROOT/'modules/perf/src/support.cpp').read_text()
    code=PRELUDE+function(support,'write_all')+function(capture,'read_capture_status')+function(capture,'drain_capture')+function(capture,'finish_capture')+CASES
    with tempfile.TemporaryDirectory(prefix='wos-perf-io-') as temp:
        p=Path(temp);source=p/'test.cpp';binary=p/'test';source.write_text(code)
        subprocess.run([shutil.which('c++',path=os.defpath),'-std=c++23','-O1','-g','-fsanitize=address,undefined',
            '-I'+str(ROOT/'modules/perf/src'),str(source),'-o',str(binary)],check=True)
        subprocess.run([str(binary)],check=True,env={**os.environ,"ASAN_OPTIONS":"detect_leaks=0"})
if __name__=='__main__':
    resource.setrlimit(resource.RLIMIT_CORE,(0,0));main()
