#!/usr/bin/env python3
"""Exercise actual denial RX, fixed queue, identity gates and full worker loop.

Peer admission, scheduler and reliable TX are controlled host leaves. Unrelated
worker handlers abort if called. This checks ownership/retry, not cryptography,
kernel concurrency, or the exact cause of any historical VM timeout.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
PATHS = [
    'modules/kern/src/net/wki/wki.cpp',
    'modules/kern/src/net/wki/remote_compute.cpp',
    'modules/kern/src/net/wki/remote_compute.hpp',
    'modules/kern/src/net/wki/wire.hpp',
    'modules/kern/src/net/wki/wki.hpp',
]
source = [(ROOT / path).read_text() for path in PATHS]
core, compute, api, wire, header = source


def extract(text, marker, declaration=False):
    start = text.index(marker)
    position = text.index('{', start) + 1
    depth = 1
    while depth:
        depth += (text[position] == '{') - (text[position] == '}')
        position += 1
    if declaration:
        position = text.index(';', position) + 1
    return text[start:position]


def constant(text, name):
    match = re.search(rf'^constexpr [^\n]+\b{name} = [^;]+;', text, re.M)
    assert match, name
    return match[0]


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <utility>
struct Lock {
 bool held=false;
 void lock(){assert(!held);held=true;}
 void unlock(){assert(held);held=false;}
};
namespace ker::mod::sys {using Spinlock=Lock;}
struct WkiChannel {
 Lock lock;bool active=true,ack_pending=false;
 uint16_t peer_node_id=1,channel_id=3;
 uint32_t generation=1,rx_seq=153,rx_dispatch_seq=153,rx_ack_pending=152;
 uint64_t ack_pending_since_us=0,bytes_received=0;
};
struct WkiHeader {uint8_t msg_type=0x50;uint16_t src_node=1,channel_id=3;uint32_t seq_num=153;};
enum class PeerState {CONNECTED,FENCED};
struct WkiPeer {
 uint16_t node_id=1,capabilities=0xffff;
 uint32_t remote_boot_epoch=11;PeerState state=PeerState::CONNECTED;
 struct {bool active=true;uint32_t generation=2;
 std::array<uint8_t,16> session_id{};uint64_t policy=0x3ff;} auth_session;
};
WkiPeer peer;
struct {uint16_t capabilities=0xffff;} g_wki;
bool peer_exists=true,lease_available=true,lease_held=false,ordered=false;
unsigned sends=0,enqueued=0,notifications=0,legacy=0,progress=0,timer=0,sleeps=0,iterations=0;
unsigned iteration_limit=1000;int tx_result=0;
std::array<uint8_t,16> last_response{};
WkiChannel* observed_channel=nullptr;
WkiPeer* wki_peer_find(uint16_t node){return peer_exists&&node==peer.node_id?&peer:nullptr;}
bool wki_peer_lifecycle_acquire(WkiPeer* p){
 assert(!lease_held);if(!p||!lease_available)return false;lease_held=true;return true;
}
void wki_peer_lifecycle_release(WkiPeer* p){if(p){assert(lease_held);lease_held=false;}}
struct Park {};
struct IterationLimit {};
namespace ker::mod::sched {
namespace task {struct Task {};}
void wake_task_from_event(task::Task*){
 assert(observed_channel&&!observed_channel->lock.held);++notifications;
}
void kern_sleep_us(unsigned us){assert(us==1000&&!lease_held);++sleeps;}
[[noreturn]] void kern_block(){assert(!lease_held);throw Park{};}
}
using DispatchWaiterList=std::array<void*,1>;
bool reliable_dispatch_needs_serial_order(const WkiChannel*){return ordered;}
void drain_reliable_dispatch_waiters_locked(WkiChannel* ch,DispatchWaiterList&){assert(ch->lock.held);}
void wake_reliable_dispatch_waiters(DispatchWaiterList&){assert(!observed_channel->lock.held);}
void mark_peer_rx_progress(uint16_t){++progress;}
void wki_timer_notify(){++timer;}
'''

STUBS = r'''
bool run_one_proxy_exit(){if(++iterations>iteration_limit)throw IterationLimit{};return false;}
bool run_one_child_signal(){return false;}
bool run_one_child_wait_retirement(){return false;}
bool run_one_native_child_wait(){return false;}
bool run_one_child_query_retirement(){return false;}
bool run_one_child_query_send(){return false;}
bool run_one_child_claim_send(){return false;}
// These branches remain in the full extracted worker but must never execute
// for a retained policy denial. Positive admission has separate source tests.
struct TaskChildClaimRequest {};
struct TaskChildClaimResponse {};
struct TaskChildQueryRequest {};
struct TaskChildQueryResponse {};
struct ComputeSubmitSessionToken {uint64_t epoch=0;};
ker::mod::sys::Spinlock s_compute_lock;
template<class... T>ComputeSubmitSessionToken capture_compute_submit_session_locked(T...){assert(false);return {};}
template<class... T>TaskChildClaimResponse execute_child_claim_packet(T&&...){assert(false);return {};}
template<class... T>int receive_child_claim_response(T&&...){assert(false);return 0;}
template<class... T>TaskChildQueryResponse execute_child_query_packet(T&&...){assert(false);return {};}
template<class... T>int receive_child_query_response(T&&...){assert(false);return 0;}
namespace detail {
template<class... T>void handle_task_complete(T...){assert(false);}
template<class... T>void handle_task_cancel(T...){assert(false);}
}
template<class... T>int deliver_child_signal(T...){assert(false);return 0;}
template<class... T>int deliver_parent_death(T...){assert(false);return 0;}
template<class... T>int deliver_terminal_signal(T...){assert(false);return 0;}
template<class... T>uint32_t authorize_user_signal(T...){assert(false);return 0;}
template<class... T>uint32_t authorize_group_receivers(T...){assert(false);return 0;}
template<class... T>uint32_t authorize_group_route(T...){assert(false);return 0;}
template<class... T>uint32_t authorize_group_signal(T...){assert(false);return 0;}
template<class... T>void handle_task_submit_fragment_work(T...){assert(false);}
int wki_send_on_channel_generation(uint16_t node,WkiChannel* channel,uint32_t generation,
                                   MsgType type,const void* payload,uint16_t length){
 assert(lease_held&&node==peer.node_id&&channel==observed_channel);
 assert(!channel->lock.held&&generation==channel->generation);
 assert(type==MsgType::TASK_REJECT&&length==sizeof(TaskResponsePayload));
 ++sends;std::memcpy(last_response.data(),payload,length);
 if(tx_result==0)++enqueued;
 return tx_result;
}
void send_policy_denial(const WkiHeader&,const uint8_t*,uint16_t,WkiChannel*,uint32_t){++legacy;}
// Classification is a leaf of this fixture. The actual public wrapper and
// shared copy run here; the existing source suite covers real classification.
WkiComputeRxAdmission classification=WkiComputeRxAdmission::INLINE;
WkiComputeRxAdmission classify_compute_rx_payload(MsgType,const uint8_t*,uint16_t len,uint16_t* copy){
 *copy=len;return classification;
}
'''

CASES = r'''
size_t used(){return std::count(s_compute_rx_slot_used.begin(),s_compute_rx_slot_used.end(),true);}
size_t queued(){size_t n=0;for(auto const& q:s_compute_rx_shards)n+=q.count;return n;}
void reset(WkiChannel& channel){
 assert(!lease_held&&!s_compute_rx_lock.held&&!channel.lock.held);
 channel=WkiChannel{};peer=WkiPeer{};observed_channel=&channel;
 peer_exists=lease_available=true;ordered=false;g_wki.capabilities=0xffff;
 sends=enqueued=notifications=legacy=progress=timer=sleeps=iterations=0;
 tx_result=0;iteration_limit=1000;classification=WkiComputeRxAdmission::INLINE;
 s_compute_rx_slot_used.fill(false);s_compute_rx_shards.fill({});s_compute_rx_slot_cursor=0;
 s_compute_rx_task_count.store(WKI_COMPUTE_RX_WORKER_MAX);last_response.fill(0xff);
}
std::array<uint8_t,1751> request(uint32_t id=101){
 std::array<uint8_t,1751> bytes{};std::memcpy(bytes.data(),&id,sizeof(id));return bytes;
}
void drive(){
 try {compute_rx_worker_loop(compute_rx_worker_index(peer.node_id,s_compute_rx_task_count.load()));}
 catch(Park const&){}catch(IterationLimit const&){}
 assert(!lease_held&&!s_compute_rx_lock.held);
}
void response(){
 TaskResponsePayload actual{};std::memcpy(&actual,last_response.data(),sizeof(actual));
 assert(actual.task_id==101&&actual.status==static_cast<uint8_t>(TaskRejectReason::UNAUTHORIZED));
 assert(actual.remote_pid==0&&(actual.reserved==std::array<uint8_t,3>{}));
}
int main(){
 WkiChannel ch;WkiHeader hdr;
 // Refused reliable enqueue retains the ACKed decision until later success.
 for(bool serial:{false,true})for(int refusal:{WKI_ERR_NOT_FOUND,WKI_ERR_NO_CREDITS}){
  reset(ch);ordered=serial;auto payload=request();hdr=WkiHeader{};
  current_denied_rx(&ch,&hdr,payload.data(),payload.size());
  assert(ch.rx_seq==154&&ch.rx_ack_pending==153&&ch.ack_pending);
  assert(ch.rx_dispatch_seq==(serial?154:153)&&used()==1&&queued()==1);
  assert(notifications==1&&legacy==0&&sends==0&&progress==1&&timer==1);
  payload.fill(0xff); // Work owns a canonical copy independent of RX bytes.
  tx_result=refusal;iteration_limit=2;drive();
  assert(sends==2&&enqueued==0&&used()==1&&queued()==1&&sleeps==2);response();
  // Retransmitting the consumed request cannot publish another reply slot.
  current_denied_rx(&ch,&hdr,payload.data(),payload.size());
  assert(used()==1&&queued()==1&&notifications==1&&progress==1);
  tx_result=0;iteration_limit=1000;drive();
  assert(sends==3&&enqueued==1&&used()==0&&queued()==0);response();
 }
 // Backpressure, unavailable worker/peer snapshot and no dispatch turn leave
 // the valid request unconsumed. Each later retransmission can gain ownership.
 for(unsigned unavailable=0;unavailable<8;++unavailable){
  reset(ch);hdr=WkiHeader{};auto payload=request();
  auto const shard=compute_rx_worker_index(1,WKI_COMPUTE_RX_WORKER_MAX);
  if(unavailable==0)s_compute_rx_task_count.store(0);
  if(unavailable==1)s_compute_rx_task_count.store(WKI_COMPUTE_RX_WORKER_MAX+1);
  if(unavailable==2)s_compute_rx_slot_used.fill(true);
  if(unavailable==3)s_compute_rx_shards.at(shard).count=WKI_COMPUTE_RX_QUEUE_MAX;
  if(unavailable==4)peer_exists=false;
  if(unavailable==5)peer.auth_session.active=false;
  if(unavailable==6){ordered=true;ch.rx_dispatch_seq=152;}
  if(unavailable==7)ch.generation=0;
  size_t const before=used();current_denied_rx(&ch,&hdr,payload.data(),payload.size());
  assert(ch.rx_seq==153&&ch.rx_ack_pending==152&&!ch.ack_pending&&ch.bytes_received==0);
  assert(progress==0&&timer==0&&notifications==0&&legacy==0&&sends==0&&used()==before);
  reset(ch);current_denied_rx(&ch,&hdr,payload.data(),payload.size());drive();
  assert(enqueued==1&&used()==0);response();
 }
 // Exact boot/auth/policy/capability/channel identity must retire old work.
 for(unsigned stale=0;stale<13;++stale){
  reset(ch);hdr=WkiHeader{};auto payload=request();
  current_denied_rx(&ch,&hdr,payload.data(),payload.size());
  assert(used()==1&&queued()==1);
  if(stale==0)++peer.remote_boot_epoch;
  if(stale==1)++peer.auth_session.generation;
  if(stale==2)++peer.auth_session.session_id.at(0);
  if(stale==3)peer.auth_session.policy^=1;
  if(stale==4)peer.capabilities^=1;
  if(stale==5)g_wki.capabilities^=1;
  if(stale==6)peer.state=PeerState::FENCED;
  if(stale==7)peer.auth_session.active=false;
  if(stale==8)++ch.generation;
  if(stale==9)ch.active=false;
  if(stale==10)ch.peer_node_id=2;
  if(stale==11)ch.channel_id=4;
  if(stale==12)lease_available=false;
  drive();assert(sends==0&&enqueued==0&&used()==0&&queued()==0);
 }
 // Both request encodings, uint32 sequence wrap and future sequence handling.
 for(auto type:{MsgType::TASK_SUBMIT,MsgType::TASK_SUBMIT_FRAGMENT}){
  reset(ch);ordered=true;hdr=WkiHeader{};hdr.msg_type=static_cast<uint8_t>(type);
  ch.rx_seq=ch.rx_dispatch_seq=hdr.seq_num=UINT32_MAX;auto payload=request();
  current_denied_rx(&ch,&hdr,payload.data(),payload.size());
  assert(ch.rx_seq==0&&ch.rx_dispatch_seq==0&&ch.rx_ack_pending==UINT32_MAX&&used()==1);
  drive();assert(enqueued==1&&used()==0);response();
  reset(ch);hdr.seq_num=154;current_denied_rx(&ch,&hdr,payload.data(),payload.size());
  assert(ch.rx_seq==153&&!ch.ack_pending&&used()==0&&legacy==0);
 }
 // Invalid/short task IDs keep the legacy fallback, without a retained slot.
 for(unsigned malformed=0;malformed<3;++malformed){
  reset(ch);hdr=WkiHeader{};auto payload=request(malformed==0?0:101);
  current_denied_rx(&ch,&hdr,malformed==1?nullptr:payload.data(),malformed==2?3:payload.size());
  assert(ch.rx_seq==154&&legacy==1&&used()==0&&queued()==0&&notifications==0);
 }
 // Existing admitted work still follows classification and bounded copying.
 reset(ch);hdr=WkiHeader{};auto payload=request();
 for(auto kind:{WkiComputeRxAdmission::INLINE,WkiComputeRxAdmission::DISCARD,WkiComputeRxAdmission::RETRY}){
  classification=kind;assert(wki_remote_compute_admit_rx(MsgType::TASK_COMPLETE,&hdr,payload.data(),
         payload.size(),&ch,ch.generation)==kind);assert(used()==0);
 }
 classification=WkiComputeRxAdmission::DEFERRED;
 assert(wki_remote_compute_admit_rx(MsgType::TASK_COMPLETE,&hdr,payload.data(),payload.size(),&ch,1)==classification);
 uint16_t slot=0;assert(dequeue_compute_rx(compute_rx_worker_index(1,4),&slot));
 assert(s_compute_rx_slots.at(slot).type==static_cast<uint8_t>(MsgType::TASK_COMPLETE));
 assert(std::memcmp(s_compute_rx_slots.at(slot).payload.data(),payload.data(),payload.size())==0);
 release_compute_rx_slot(slot);assert(used()==0&&queued()==0);
 std::puts("SUBMIT_DENIAL_OWNERSHIP_RETRY_BACKPRESSURE_AND_SESSION_RETIREMENT_PASS");
}
'''


def program():
    pieces = [PRELUDE]
    for name in ['WKI_NODE_INVALID', 'WKI_AUTH_SESSION_ID_SIZE', 'WKI_AUTH_TAG_SIZE']:
        pieces.append(constant(wire, name))
    pieces += [extract(wire, 'struct WkiAuthTrailer {', True), constant(wire, 'WKI_AUTH_TRAILER_SIZE'),
               constant(wire, 'WKI_ETH_MAX_PAYLOAD'), constant(wire, 'WKI_CHAN_RESOURCE'),
               extract(wire, 'enum class MsgType : uint8_t {', True),
               extract(wire, 'enum class TaskRejectReason : uint8_t {', True),
               extract(wire, 'struct TaskResponsePayload {', True),
               extract(wire, 'struct TaskCompletePayload {', True),
               constant(api, 'WKI_TASK_MAX_OUTPUT'), constant(header, 'WKI_CREDITS_RESOURCE')]
    pieces += [constant(compute, name) for name in
               ['WKI_COMPUTE_RX_WORKER_MAX', 'WKI_COMPUTE_RX_QUEUE_MAX', 'WKI_COMPUTE_RX_PAYLOAD_MAX']]
    pieces += [line for line in header.splitlines() if line.startswith('constexpr int WKI_')]
    pieces += [extract(api, 'enum class WkiComputeRxAdmission : uint8_t {', True)]
    pieces += [extract(compute, marker, True) for marker in
               ['class PeerLifecycleLease {', 'struct ComputePeerIdentity {',
                'struct PendingComputeRx {', 'struct ComputeRxShard {']]
    pieces.append(compute[compute.index('std::array<PendingComputeRx, WKI_COMPUTE_RX_QUEUE_MAX> s_compute_rx_slots'):
                          compute.index('struct ProxyExitQueue {')])
    pieces.append(STUBS)
    for name in ['compute_submit_channel_token_matches', 'compute_peer_identity', 'capture_compute_peer_identity',
                 'compute_rx_identity_matches', 'compute_rx_worker_index', 'dequeue_compute_rx']:
        pieces.append(extract(compute, f'auto {name}('))
    for name in ['release_compute_rx_slot', 'requeue_compute_rx_slot']:
        pieces.append(extract(compute, f'void {name}('))
    pieces.append(extract(compute, '[[noreturn]] void compute_rx_worker_loop('))
    for name in ['queue_compute_rx_work', 'wki_remote_compute_admit_rx', 'wki_remote_compute_admit_submit_denial_rx']:
        pieces.append(extract(compute, f'auto {name}('))
    pieces.append(extract(compute, 'void wki_remote_compute_notify_deferred_rx('))
    for name in ['policy_denial_dispatch_ready_locked', 'try_consume_policy_denial_locked']:
        pieces.append(extract(core, f'auto {name}('))
    in_order = core[core.index('            if (hdr->seq_num == ch->rx_seq) {'):]
    pieces.append('void current_denied_rx(WkiChannel* ch,const WkiHeader* hdr,const uint8_t* payload,uint16_t PAYLOAD_LEN) {\n'
                  'ch->lock.lock();auto const msg=static_cast<MsgType>(hdr->msg_type);bool authorized=false;\n'
                  'if(hdr->seq_num==ch->rx_seq) {\n' + extract(in_order, '                if (!authorized) {') +
                  '\n}\nch->lock.unlock();\n}\n')
    pieces.append(CASES)
    return '\n'.join(pieces)


def main():
    compiler = os.environ.get('HOSTCXX') or shutil.which('g++', path='/usr/bin:/bin') or shutil.which('clang++', path='/usr/bin:/bin')
    assert compiler, 'native host C++ compiler required'
    code = program()
    with tempfile.TemporaryDirectory(prefix='wki-submit-denial-') as directory:
        cpp, binary = Path(directory) / 'denial.cpp', Path(directory) / 'denial'
        cpp.write_text(code)
        subprocess.run([compiler, '-std=c++23', '-O2', '-Wall', '-Werror', str(cpp), '-o', str(binary)], check=True)
        subprocess.run([str(binary)], check=True)
        # Semantic controls: freeing a refused reply or ACKing a request whose
        # denial has no slot both lose ownership of the required decision.
        controls = [
            ('REFUSED_REPLY_RELEASE', 'if (retry_reply) {\n            requeue_compute_rx_slot',
             'if (false && retry_reply) {\n            requeue_compute_rx_slot'),
            ('ACK_WITHOUT_OWNED_REPLY', 'if (denial_admission == WkiComputeRxAdmission::RETRY) {',
             'if (false && denial_admission == WkiComputeRxAdmission::RETRY) {'),
        ]
        for name, before, after in controls:
            broken = code.replace(before, after, 1)
            assert broken != code
            cpp.write_text(broken)
            subprocess.run([compiler, '-std=c++23', '-O2', '-Wall', '-Werror', str(cpp), '-o', str(binary)], check=True)
            result = subprocess.run([str(binary)], capture_output=True)
            assert result.returncode != 0, f'{name} semantic control unexpectedly passed'
            print(f'{name}_CONTROL_REJECTED')
    print(json.dumps({'source_sha256': {path: hashlib.sha256(text.encode()).hexdigest()
                                       for path, text in zip(PATHS, source)},
                      'actual_source_fixture_sha256': hashlib.sha256(code.encode()).hexdigest()}))


if __name__ == '__main__':
    main()
