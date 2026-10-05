#!/usr/bin/env python3
"""Run the actual broker readiness/relay functions against native POSIX pipes.

Only crypto, credentials and RPC transport are stubbed. Readiness, chunking,
stream close, EOF and signal wake code are taken from the Dropbear source.
"""

from pathlib import Path
import os
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "toolchain/src/dropbear/src/svr-kex-broker.c"


def function(source: str, name: str) -> str:
    match = re.search(r"^(?:static )?(?:void|buffer \*)\s*" + name + r"\([^\n]*\) \{", source, re.M)
    assert match, name
    start = source.index("{", match.start())
    depth = 1
    end = start + 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[match.start():end]


PRELUDE = r'''
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/wait.h>
#include <unistd.h>
#define UNUSED(x) x __attribute__((unused))
#define MAX_CHANNELS 4
#define BROKER_IO_MAX 16384
#define BROKER_OUTPUT_CLOSED UINT_MAX
#define BROKER_IO 6
#define BROKER_REARM 10
#define LOG_INFO 0
#define dropbear_log(...) ((void)0)
struct exitinfo { int exitpid, exitstatus, exitsignal, exitcore; };
struct BrokerRelay;
struct ChanSess {
 int pid, master;
 char *tty;
 unsigned int broker_session_id;
 struct exitinfo exit;
 struct BrokerRelay *broker_relay;
};
struct Channel { const int *type; struct ChanSess *typedata; };
static const int svrchansess;
static struct { int maxfd; unsigned int chansize; struct Channel *channels[MAX_CHANNELS]; int channel_signal_pending; } ses;
static int notification_fd, broker_signal_pipe[2], notification_pending, notification_sent;
static unsigned int relay_count, rpc_count;
static void dropbear_exit(const char *reason) { fprintf(stderr, "%s\n", reason); abort(); }
typedef struct { unsigned char *data; unsigned int pos, len, size; } buffer;
static buffer *buf_new(unsigned int n) {
 buffer *b=calloc(1,sizeof(*b)); assert(b); b->data=malloc(n); assert(b->data); b->size=n; return b;
}
static void buf_free(buffer *b) { free(b->data); free(b); }
#define buf_burn_free buf_free
static void buf_putbyte(buffer *b, unsigned int v) { assert(b->len<b->size); b->data[b->len++]=v; }
static unsigned int buf_getbyte(buffer *b) { assert(b->pos<b->len); return b->data[b->pos++]; }
#define buf_getbool buf_getbyte
static void buf_putint(buffer *b, unsigned int v) { for(int i=3;i>=0;i--) buf_putbyte(b,v>>(i*8)); }
static unsigned int buf_getint(buffer *b) { unsigned int v=0; for(int i=0;i<4;i++) v=(v<<8)|buf_getbyte(b); return v; }
static unsigned char *buf_getptr(buffer *b, unsigned int n) { assert(n<=b->len-b->pos); return b->data+b->pos; }
static void buf_incrpos(buffer *b, unsigned int n) { assert(n<=b->len-b->pos); b->pos+=n; }
static void buf_putstring(buffer *b, const char *v, unsigned int n) {
 buf_putint(b,n); assert(n<=b->size-b->len); memcpy(b->data+b->len,v,n); b->len+=n;
}
static buffer *read_blob(buffer *b) { unsigned int n=buf_getint(b); buffer *v=buf_new(n+1); memcpy(v->data,buf_getptr(b,n),n); v->len=n; buf_incrpos(b,n); return v; }
'''

RPC_STUB = r'''
static struct PreparedSession *read_session(buffer *b) {
 unsigned int id=buf_getint(b); assert(prepared_sessions[0] && id==prepared_sessions[0]->id); return prepared_sessions[0];
}
static volatile sig_atomic_t mock_exit;
static void reap_sessions(void) {
 if(mock_exit) { assert(prepared_sessions[0]); prepared_sessions[0]->command.exit.exitpid=1; mock_exit=0; }
}
'''

RPC_DISPATCH = r'''
static buffer *session_rpc(buffer *b) {
 unsigned int op=buf_getbyte(b); buffer *reply;
 rpc_count++;
 if (op==BROKER_IO) reply=session_io_request(b);
 else { assert(op==BROKER_REARM && notification_sent); notification_sent=notification_pending=0; reply=buf_new(1); buf_putbyte(reply,1); }
 buf_free(b); return reply;
}
'''

CASES = r'''
static void make_pipe(int p[2]) {
 assert(pipe(p)==0); for(int i=0;i<2;i++) { assert(p[i]<FD_SETSIZE); assert(fcntl(p[i],F_SETFL,O_NONBLOCK)==0); if(p[i]>ses.maxfd) ses.maxfd=p[i]; }
}
static void close_pipe(int p[2]) { close(p[0]); close(p[1]); }
static void ready_io(void) {
 fd_set r,w; struct timeval timeout={0}; FD_ZERO(&r); FD_ZERO(&w); svr_kex_broker_setfds(&r,&w);
 assert(select(ses.maxfd+1,&r,&w,NULL,&timeout)>=0); svr_kex_broker_io(&r,&w);
}
static void notify(int writer) { char c='R'; notification_sent=1; assert(write(writer,&c,1)==1); }
static size_t fill(int fd) { char b[1024]; size_t total=0; ssize_t n; memset(b,'F',sizeof(b)); while((n=write(fd,b,sizeof(b)))>0) total+=n; assert(errno==EAGAIN); return total; }
static size_t drain(int fd) { char b[1024]; size_t total=0; ssize_t n; while((n=read(fd,b,sizeof(b)))>0) total+=n; assert(n==0 || errno==EAGAIN); return total; }
static void mock_child_exit(int sig) { broker_child_signal(sig); mock_exit=1; }
static void relay_cases(void) {
 int notice[2], input[2], child_input[2], output[2], child_output[2], error[2], child_error[2];
 struct PreparedSession entry={0}; struct ChanSess session={0}; struct BrokerRelay relay={0}; struct Channel channel={&svrchansess,&session};
 make_pipe(notice); make_pipe(input); make_pipe(child_input); make_pipe(output); make_pipe(child_output); make_pipe(error); make_pipe(child_error);
 notification_fd=notice[0]; entry.id=1; entry.command.pid=1; entry.command.exit.exitpid=-1;
 entry.fd[0]=child_input[1]; entry.fd[1]=child_output[0]; entry.fd[2]=child_error[0]; prepared_sessions[0]=&entry;
 session.broker_session_id=1; session.broker_relay=&relay; session.exit.exitpid=-1;
 relay.fd[0]=input[0]; relay.fd[1]=output[1]; relay.fd[2]=error[1]; relay.exit.exitpid=-1; relay.pending=1;
 ses.chansize=1; ses.channels[0]=&channel; relay_count=1;
 svr_kex_broker_io(NULL,NULL); assert(entry.watch[1] && entry.watch[2]);
 unsigned int before=rpc_count;
 for(int i=0;i<50;i++) ready_io();
 assert(rpc_count==before); /* An idle relay generates no RPC. */
 fill(child_input[1]); assert(write(input[1],"hello",5)==5); ready_io();
 assert(relay.chunk[0].len==5 && entry.watch[0]);
 fd_set r,w; FD_ZERO(&r); FD_ZERO(&w); svr_kex_broker_setfds(&r,&w); assert(!FD_ISSET(input[0],&r));
 drain(child_input[0]); notify(notice[1]); ready_io();
 char b[32]; assert(read(child_input[0],b,sizeof(b))==5 && !memcmp(b,"hello",5));
 assert(relay.chunk[0].len==0 && !entry.watch[0] && !notification_sent);
 /* Fresh output must enter a writable adapter in this notification service,
  * without needing another select snapshot to observe adapter writability. */
 assert(write(child_output[1],"quick",5)==5); notify(notice[1]); ready_io();
 assert(read(output[0],b,sizeof(b))==5 && !memcmp(b,"quick",5));
 assert(relay.chunk[1].len==0 && relay.pending); ready_io();
 assert(entry.watch[1] && !relay.pending);
 /* An SSH slow reader fills the adapter; one retained chunk disables the
  * broker read watch until the adapter can accept it. */
 char payload[BROKER_IO_MAX]; memset(payload,'P',sizeof(payload));
 size_t filled=fill(output[1]); ssize_t length=write(child_output[1],payload,sizeof(payload)); assert(length>4096);
 notify(notice[1]); ready_io();
 assert(relay.chunk[1].len==(unsigned int)length && !entry.watch[1]); before=rpc_count;
 for(int i=0;i<50;i++) ready_io();
 assert(rpc_count==before && relay.chunk[1].len==(unsigned int)length);
 char block[4096]; assert(read(output[0],block,sizeof(block))==sizeof(block)); ready_io();
 assert(relay.chunk[1].pos>0 && relay.chunk[1].pos<relay.chunk[1].len && rpc_count==before);
 size_t remaining=filled-sizeof(block);
 while(remaining) { ssize_t n=read(output[0],block,remaining<sizeof(block)?remaining:sizeof(block)); assert(n>0); remaining-=n; }
 ready_io();
 size_t received=0;
 while(received<(size_t)length) { ssize_t n=read(output[0],block,sizeof(block)); assert(n>0); for(ssize_t i=0;i<n;i++)assert(block[i]=='P'); received+=n; }
 assert(relay.chunk[1].len==0 && entry.watch[1]);
 /* Failure of the adapter read end must revoke the broker read end too. */
 close(output[0]); output[0]=-1;
 assert(write(child_output[1],"broken",6)==6); notify(notice[1]); ready_io(); ready_io();
 assert(relay.fd[1]==-1 && entry.fd[1]==-1 && !entry.watch[1]);
 assert(write(child_output[1],"x",1)<0 && errno==EPIPE);
 /* EOF and exit status arrive without a timeout or a separate input event. */
 close(child_error[1]); child_error[1]=-1; entry.command.exit.exitpid=1; entry.command.exit.exitstatus=23;
 notify(notice[1]); ready_io();
 assert(relay.fd[2]==-1 && session.exit.exitpid==1 && session.exit.exitstatus==23 && ses.channel_signal_pending);
 assert(entry.exit_reported && !notification_sent);
 close_pipe(notice); close_pipe(input); close_pipe(child_input); close_pipe(output); close_pipe(child_output); close_pipe(error); close_pipe(child_error);
 memset(prepared_sessions,0,sizeof(prepared_sessions)); memset(&ses,0,sizeof(ses)); relay_count=0;
}
static void wait_case(int delayed, int signal_only, int full_notice) {
 int request[2], notice[2], output[2]; struct PreparedSession entry={0};
 make_pipe(request); make_pipe(notice); make_pipe(output); make_pipe(broker_signal_pipe);
 notification_fd=notice[1]; notification_sent=notification_pending=0;
 entry.command.pid=1; entry.command.exit.exitpid=signal_only && !delayed ? 1 : -1;
 entry.fd[0]=entry.fd[2]=-1; entry.fd[1]=output[0]; entry.watch[1]=!signal_only; prepared_sessions[0]=&entry;
 size_t filler=full_notice ? fill(notice[1]) : 0;
 if(!delayed && !signal_only) assert(write(output[1],"x",1)==1);
 if(signal_only && !delayed) { errno=EDOM; broker_child_signal(0); assert(errno==EDOM); }
 if(signal_only && delayed) signal(SIGUSR1,mock_child_exit);
 pid_t pid=fork(); assert(pid>=0);
 if(pid==0) {
  alarm(3); if(delayed) usleep(20000);
  if(full_notice) { size_t n=0; char b[1024]; while(n<filler) { ssize_t k=read(notice[0],b,sizeof(b)); if(k>0)n+=k; else { assert(errno==EAGAIN); usleep(1000); } } }
  if(delayed) { if(signal_only) assert(kill(getppid(),SIGUSR1)==0); else assert(write(output[1],"x",1)==1); }
  char c; while(read(notice[0],&c,1)<0) { assert(errno==EAGAIN); usleep(1000); } assert(c=='R');
  assert(read(notice[0],&c,1)<0 && errno==EAGAIN); assert(write(request[1],"Q",1)==1); _exit(0);
 }
 alarm(3); broker_wait_for_request(request[0]); alarm(0);
 assert(notification_sent && !notification_pending); int status; assert(waitpid(pid,&status,0)==pid && status==0);
 signal(SIGUSR1,SIG_DFL);
 /* Child output remains level-ready, but one hint is enough before rearm. */
 broker_wait_for_request(request[0]); char c; assert(read(notice[0],&c,1)<0 && errno==EAGAIN);
 close_pipe(request); close_pipe(notice); close_pipe(output); close_pipe(broker_signal_pipe);
 memset(prepared_sessions,0,sizeof(prepared_sessions)); memset(&ses,0,sizeof(ses));
}
int main(void) {
 signal(SIGPIPE,SIG_IGN); relay_cases();
 wait_case(0,0,0); wait_case(1,0,0); wait_case(0,1,0); wait_case(1,1,0); wait_case(1,0,1);
 puts("PASS broker idle, input/output backpressure, broken adapter, EOF/exit, coalescing, full notification pipe and readiness-before/after-wait");
}
'''


def main() -> None:
    source = SOURCE.read_text()
    declarations = []
    for name in ("PreparedSession", "RelayChunk", "BrokerRelay"):
        declarations.append(re.search(r"struct " + name + r" \{.*?\n\};", source, re.S).group())
    declarations.append("static struct PreparedSession *prepared_sessions[MAX_CHANNELS];")
    text = PRELUDE + "\n".join(declarations) + RPC_STUB
    for name in ("close_stream", "close_session_stream", "broker_child_signal", "broker_wait_for_request", "put_exit", "session_io_request"):
        text += "\n" + function(source, name)
    text += RPC_DISPATCH
    for name in ("svr_kex_broker_setfds", "drain_relay_output", "svr_kex_broker_io"):
        text += "\n" + function(source, name)
    text += CASES
    compiler = os.environ.get("HOST_CC") or shutil.which("cc", path=os.defpath) or shutil.which("clang", path=os.defpath)
    assert compiler, "native C compiler required"
    with tempfile.TemporaryDirectory(prefix="wos-broker-readiness-") as directory:
        path = Path(directory)
        (path / "test.c").write_text(text)
        subprocess.run([compiler, "-std=gnu11", "-Wall", "-Wextra", "-Werror", str(path / "test.c"), "-o", str(path / "test")], check=True)
        subprocess.run([str(path / "test")], check=True, timeout=15)


if __name__ == "__main__":
    main()
