#!/usr/bin/env python3
"""Execute init's actual session admission and cleanup functions with real pipes.

The process-ID queries and model callbacks are controlled boundaries; admission,
report parsing, descriptor lifetime and signal target selection use source code.
"""
import subprocess
import tempfile
from pathlib import Path

from init_source_test import function_body

ROOT = Path(__file__).resolve().parents[3]


def test_session_admission_runtime():
    source = (ROOT / 'modules/init/src/services.cpp').read_text()
    signatures = [
        'auto set_close_on_exec(int fd) -> bool',
        'auto set_nonblocking(int fd) -> bool',
        'auto move_above_standard_io(int& fd) -> bool',
        'auto create_parent_read_pipe(std::array<int, 2>& pipefd) -> bool',
        'auto create_session_ack(std::array<int, 2>& pipefd) -> bool',
        'void close_session_ack(std::array<int, 2>& pipefd)',
        '[[noreturn]] void report_child_failure(int fd, ChildFailureStage stage, int32_t error)',
        'void await_session_admission(int report_fd, int ack_fd)',
        'void process_exec_report(uint8_t service, uint64_t now_ms)',
        'void signal_service_group(const SupervisorAction& action, int signal)',
    ]
    prefix = r'''
#include <array>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>
#include <utility>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <sys/wait.h>
constexpr size_t PIPE_READ=0, PIPE_WRITE=1;
enum class ChildFailureStage:int32_t { PROCESS_GROUP=1, EXEC=5, SESSION_READY=7, SESSION_CANCELLED=8 };
struct ExecFailureReport { ChildFailureStage stage=ChildFailureStage::EXEC; int32_t error=EIO; };
enum class SupervisorEventKind { EXEC_SUCCEEDED, EXEC_FAILED };
struct SupervisorEvent { SupervisorEventKind kind{}; uint8_t service{}; uint32_t generation{}; int detail{}; };
struct SupervisorAction { uint8_t service{}; uint32_t generation{}; };
struct Slot {
 int64_t pid=200, pgid=200; uint32_t generation=3;
 int exec_report_fd=-1; std::array<int,2> session_ack{-1,-1};
 bool exec_resolved=false, session_admitted=false, session_cancel_requested=false, leader_reaped=false, quiescence_reported=false;
 int last_error=0;
};
struct Spec { std::string name="test"; };
struct Runtime { std::array<Slot,1> services; struct { uint8_t service_count=1; std::array<Spec,1> services; } manifest; } runtime;
size_t error_logs=0, cancellation_logs=0, case_count=0;
struct init_log {
 template<class... T> static void error(T...) { ++error_logs; }
 template<class... T> static void warn(T...) {}
 template<class... T> static void info(T...) { ++cancellation_logs; }
};
std::vector<SupervisorEvent> events;
std::vector<std::pair<int64_t,int>> signals;
int64_t observed_sid=200, observed_pgid=200;
namespace ker::process {
[[noreturn]] void exit(int status) { ::_exit(status); }
int64_t getsid(int64_t pid) { assert(pid==200); return observed_sid; }
int64_t getpgid(int64_t pid) { assert(pid==200); return observed_pgid; }
int64_t kill(int64_t pid, int signal) { signals.push_back({pid,signal}); return 0; }
}
void close_fd(int& fd) { if(fd>=0) ::close(fd); fd=-1; }
bool emit_model_event(SupervisorEvent event,uint64_t) { events.push_back(event); return true; }
void deliver_pending_exit(uint8_t,uint64_t) {}
bool service_group_gone(const Slot&) { return false; }
'''
    functions = '\n'.join(signature + ' {' + function_body(source, signature.split('(')[0].split()[-1]) + '}\n'
                          for signature in signatures)
    cases = r'''
struct Fixture {
 std::array<int,2> report{-1,-1}; int child_ack=-1;
 Fixture() {
  runtime={}; events.clear(); signals.clear(); error_logs=cancellation_logs=0; ++case_count; observed_sid=observed_pgid=200;
  assert(create_parent_read_pipe(report)); auto& s=runtime.services[0];
  s.exec_report_fd=report[0]; report[0]=-1;
  assert(create_session_ack(s.session_ack)); child_ack=::dup(s.session_ack[0]); assert(child_ack>=0);
  assert(!(fcntl(child_ack,F_GETFL)&O_NONBLOCK));
  for(int fd:s.session_ack) assert(fd>2 && (fcntl(fd,F_GETFD)&FD_CLOEXEC));
  assert(fcntl(s.session_ack[1],F_GETFL)&O_NONBLOCK);
 }
 void ready() { ExecFailureReport r{ChildFailureStage::SESSION_READY,0}; assert(write(report[1],&r,sizeof(r))==sizeof(r)); }
 void send_report(ChildFailureStage stage,int error) { ExecFailureReport r{stage,error}; assert(write(report[1],&r,sizeof(r))==sizeof(r)); }
 void poll() { process_exec_report(0,10); }
 void eof() { close_fd(report[1]); poll(); }
 ~Fixture() { close_fd(report[1]); close_fd(child_ack); close_fd(runtime.services[0].exec_report_fd); close_session_ack(runtime.services[0].session_ack); }
};
void denied() { assert(events.size()==1 && events[0].kind==SupervisorEventKind::EXEC_FAILED); assert(runtime.services[0].exec_resolved); }
void cancelled() {
 denied(); auto& slot=runtime.services[0];
 assert(events[0].detail==ECANCELED && slot.last_error==ECANCELED);
 assert(!slot.session_admitted && slot.session_cancel_requested);
 assert(error_logs==0 && cancellation_logs==1);
}
void child_ack_protocol(int mode) {
 ++case_count;
 std::array<int,2> report{-1,-1}, ack{-1,-1};
 assert(pipe(report.data())==0 && pipe(ack.data())==0);
 pid_t child=fork(); assert(child>=0);
 if(child==0) {
  close(report[0]); close(ack[1]);
  int child_ack=ack[0];
  if(mode==3) { close(child_ack); child_ack=-1; }
  await_session_admission(report[1],child_ack);
  ::_exit(42);
 }
 close(report[1]); close(ack[0]);
 ExecFailureReport message{};
 assert(read(report[0],&message,sizeof(message))==sizeof(message));
 assert(message.stage==ChildFailureStage::SESSION_READY && message.error==0);
 if(mode==0 || mode==2) {
  char answer=mode==0 ? 'A' : 'X'; assert(write(ack[1],&answer,1)==1);
 }
 close(ack[1]);
 auto amount=read(report[0],&message,sizeof(message));
 if(mode==0) assert(amount==0);
 else {
  assert(amount==sizeof(message));
  assert(message.stage==(mode==1 ? ChildFailureStage::SESSION_CANCELLED : ChildFailureStage::PROCESS_GROUP));
  assert(message.error==(mode==1 ? ECANCELED : mode==2 ? EIO : EBADF));
 }
 close(report[0]); int status=0; assert(waitpid(child,&status,0)==child && WIFEXITED(status));
 assert(WEXITSTATUS(status)==(mode==0 ? 42 : 127));
}
int main() {
 alarm(10);
 {
  Fixture f; f.poll(); assert(events.empty() && !runtime.services[0].session_admitted);
  f.ready(); f.poll(); auto& s=runtime.services[0];
  assert(s.session_admitted && !s.exec_resolved && events.empty());
  assert(s.session_ack[0]==-1 && s.session_ack[1]==-1);
  char ack=0; assert(read(f.child_ack,&ack,1)==1 && ack=='A');
  f.eof(); assert(events.size()==1 && events[0].kind==SupervisorEventKind::EXEC_SUCCEEDED);
  assert(error_logs==0 && cancellation_logs==0);
 }
 for(int scenario=0;scenario<5;scenario++) {
  Fixture f;
  if(scenario==0) observed_sid=1;
  if(scenario==1) observed_pgid=1;
  if(scenario==2) runtime.services[0].leader_reaped=true;
  if(scenario==3) close_session_ack(runtime.services[0].session_ack);
  if(scenario==4) runtime.services[0].session_admitted=true;
  f.ready(); f.poll(); denied(); assert(error_logs==1 && cancellation_logs==0); char ack=0; assert(read(f.child_ack,&ack,1)==0);
 }
 { Fixture f; f.eof(); denied(); assert(error_logs==1 && cancellation_logs==0); }
 { Fixture f; char partial=7; assert(write(f.report[1],&partial,1)==1); f.poll(); denied(); assert(error_logs==1 && cancellation_logs==0); }
 { Fixture f; ExecFailureReport r{ChildFailureStage::EXEC,EACCES}; assert(write(f.report[1],&r,sizeof(r))==sizeof(r)); f.poll(); denied(); assert(events[0].detail==EACCES && error_logs==1 && cancellation_logs==0); }
 {
  Fixture f; // Even if the child has closed its reader, PID1's guard prevents SIGPIPE.
  close_fd(f.child_ack); f.ready(); f.poll(); assert(runtime.services[0].session_admitted && events.empty());
 }
 for(bool queued:{false,true}) {
  Fixture f; if(queued) f.ready();
  signal_service_group({0,3},SIGKILL);
  auto& slot=runtime.services[0];
  assert(signals.size()==1 && signals[0].first==200 && signals[0].second==SIGKILL);
  assert(slot.session_cancel_requested && slot.session_ack[0]==-1 && slot.session_ack[1]==-1);
  if(!queued) f.ready();
  f.poll();
  assert(events.empty() && !slot.exec_resolved && !slot.session_admitted && slot.exec_report_fd>=0);
  assert(error_logs==0 && cancellation_logs==0);
  char ack=0; assert(read(f.child_ack,&ack,1)==0); // Cancellation never emits an ACK.
  f.eof(); cancelled(); // A delayed READY keeps the actual terminal handshake alive.
 }
 {
  Fixture f; signal_service_group({0,3},SIGTERM);
  f.ready(); f.poll();
  f.send_report(ChildFailureStage::SESSION_CANCELLED,ECANCELED); f.poll(); cancelled();
 }
 { Fixture f; signal_service_group({0,3},SIGTERM); f.eof(); cancelled(); }
 for(int counterexample=0;counterexample<6;++counterexample) {
  Fixture f;
  if(counterexample!=0) signal_service_group({0,3},SIGTERM);
  if(counterexample==0) f.send_report(ChildFailureStage::SESSION_CANCELLED,ECANCELED); // No cancellation witness.
  if(counterexample==1) f.send_report(ChildFailureStage::SESSION_CANCELLED,EIO);       // Wrong cancellation payload.
  if(counterexample==2) f.send_report(ChildFailureStage::PROCESS_GROUP,EIO);          // Genuine setup failure remains an error.
  if(counterexample==3) f.send_report(ChildFailureStage::EXEC,EACCES);                // Genuine exec error remains an error.
  if(counterexample==4) { char partial=7; assert(write(f.report[1],&partial,1)==1); }
  if(counterexample==5) f.send_report(ChildFailureStage::SESSION_READY,EIO);
  f.poll(); denied(); assert(error_logs==1 && cancellation_logs==0);
 }
 {
  Fixture f; auto before=runtime.services[0].session_ack;
  signal_service_group({0,2},SIGKILL);
  assert(signals.empty() && !runtime.services[0].session_cancel_requested && runtime.services[0].session_ack==before);
  f.ready(); f.poll(); f.eof();
  assert(events.size()==1 && events[0].kind==SupervisorEventKind::EXEC_SUCCEEDED && error_logs==0 && cancellation_logs==0);
 }
 {
  Fixture f; f.ready(); f.poll(); signal_service_group({0,3},SIGTERM);
  assert(signals.size()==1 && signals[0].first==-200 && !runtime.services[0].session_cancel_requested);
  signal_service_group({0,2},SIGKILL); assert(signals.size()==1);
 }
 for(int mode=0;mode<4;++mode) child_ack_protocol(mode);
 printf("init session runtime: %zu scenarios passed\n",case_count);
}
'''
    with tempfile.TemporaryDirectory(prefix='wos-init-session-') as directory:
        path = Path(directory) / 'test.cpp'
        path.write_text(prefix + functions + cases)
        binary = Path(directory) / 'test'
        subprocess.run(['/usr/bin/clang++', '-std=c++23', '-O2', '-Wall', '-Wextra', str(path), '-o', str(binary)], check=True)
        subprocess.run([str(binary)], check=True, timeout=15)


if __name__ == '__main__':
    test_session_admission_runtime()
