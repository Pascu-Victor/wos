#!/usr/bin/env python3
"""Two complete engines/models, isolated only by their kernel namespace.

Owned authenticated-control queues model reliable publication/delivery. Actual
Session, maintenance, control admission, registry and teardown code is compiled
for both nodes. This does not model NIC dispatch, real timers or core TX RTO.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
KERN = ROOT / 'modules/kern/src'
BASE = Path(__file__).with_name('wki_secure_rdma_engine_test.py')
CASES = ('initial', 'steady', 'duplicate-unconfirmed', 'prepublication-confirm',
         'accepted-lost-confirm', 'provider-rotation', 'auth-rotation')
API = r'''#pragma once
#include <cstdint>
struct PairView {uint64_t incarnation=0,retry=0,next_tx=0,rx_high=0;uint32_t keys=0,queued=0;bool installed=false,confirmed=false,sent=false;};
extern "C" int paired_publish(unsigned,unsigned,const void*,unsigned);
#define PAIR_API(N) extern "C" {void N##_init();void N##_step(uint64_t);bool N##_ingress(unsigned,const void*,unsigned,uint32_t);void N##_view(PairView*);void N##_offer(void*);void N##_sid(void*);unsigned N##_record(void*);unsigned N##_decode(void*,unsigned);void N##_register();int N##_close();int N##_local_current();void N##_rotate(bool);void N##_stop();void N##_finish();}
PAIR_API(pair_a)
PAIR_API(pair_b)
'''
EXPORTS = r'''
static Native pair_native;
static p::RegionDescriptor pair_local{};
extern "C" void @NAME@_init(){
 caller.pid=17;worker.pid=18;current=&caller;g_wki.my_node_id=@SELF@;g_wki.initialized=true;auto&peer=g_wki.peers[0];peer.node_id=@PEER@;peer.transport=&provider;peer.is_direct=true;peer.state=PeerState::CONNECTED;peer.auth_session.active=true;peer.auth_session.generation=1;peer.auth_session.policy=UINT64_MAX;peer.auth_session.session_id[0]=77;
 peer.auth_session.tx_key.fill(@TX@);peer.auth_session.rx_key.fill(@RX@);kind=p::Transport::ROCE;
 provider.registration_generation=7;provider.secure_kind=kind;provider.secure_tx=[](WkiTransport*,uint16_t,const void*,uint16_t){return WKI_OK;};provider.mtu=p::RECORD_BYTES;g_wki.transports=&provider;
 control.active=true;control.peer_node_id=@PEER@;control.channel_id=4;control.generation=1;data.active=true;data.peer_node_id=@PEER@;data.channel_id=40;data.generation=1;
 CHECK(wki_secure_rdma_init());wki_secure_rdma_worker_start();CHECK(s_worker==&worker);
}
extern "C" void @NAME@_step(uint64_t time){current=&worker;now=time;maintain_peer(0,now);drain_rx(now);maintain_regions();for(auto&job:s_jobs)progress_job(job,now);current=&caller;}
extern "C" bool @NAME@_ingress(unsigned type,const void*payload,unsigned size,uint32_t seq){
 current=&caller;auto&peer=g_wki.peers[0];WkiHeader h{};h.src_node=@PEER@;h.dst_node=@SELF@;h.channel_id=4;h.msg_type=static_cast<uint8_t>(type);h.seq_num=seq;h.payload_len=static_cast<uint16_t>(size);
 CHECK(wki_peer_lifecycle_rx_try_acquire(&peer));control.lock.lock();auto result=wki_secure_rdma_admit_control_rx(&h,payload,static_cast<uint16_t>(size),control_identity(),&peer);control.lock.unlock();wki_peer_lifecycle_rx_release(&peer);
 if(result==WkiSecureRdmaControlRxAdmission::QUEUED)wki_secure_rdma_tick();
 return result!=WkiSecureRdmaControlRxAdmission::RETRY;
}
extern "C" void @NAME@_view(PairView*out){auto&ctx=s_peers[0];rdma_session::Statistics stats{};CHECK(ctx.session.statistics(stats)==p::Drop::NONE);out->incarnation=ctx.incarnation;out->retry=ctx.retry_at;out->installed=ctx.installed;out->confirmed=ctx.confirmed;out->sent=ctx.confirm_sent;out->keys=stats.live_keys;out->queued=static_cast<uint32_t>(s_control_count);out->next_tx=ctx.session.domains_[1].next_tx;out->rx_high=ctx.session.domains_[1].replay.high;}
extern "C" void @NAME@_offer(void*out){std::memcpy(out,&s_peers[0].local,sizeof(p::Offer));}
extern "C" void @NAME@_sid(void*out){std::memcpy(out,g_wki.peers[0].auth_session.session_id.data(),16);}
extern "C" unsigned @NAME@_record(void*out){p::Header h{};h.transport=static_cast<uint8_t>(p::Transport::ROCE);h.service=static_cast<uint8_t>(p::Service::VFS);h.opcode=static_cast<uint8_t>(p::Opcode::NOTIFY);h.src_node=@SELF@;h.dst_node=@PEER@;h.channel_id=40;h.session_id=s_peers[0].sid;h.region_generation=1;h.binding_generation=2;h.capability.fill(0x42);h.rkey=3;h.operation_id=(uint64_t{@SELF@}<<48)|1;h.receive_ticket=(uint64_t{@SELF@}<<48)|2;CHECK(s_peers[0].session.send(h,nullptr,out,p::HEADER_BYTES)==p::Drop::NONE);return p::HEADER_BYTES;}
extern "C" unsigned @NAME@_decode(void*record,unsigned size){p::Header header{};return static_cast<unsigned>(s_peers[0].session.receive(p::Transport::ROCE,record,size,header));}
extern "C" void @NAME@_register(){CHECK(pair_local.rkey==0);pair_local=local_region(pair_native);}
extern "C" int @NAME@_close(){if(pair_local.rkey==0)return 0;int status=wki_secure_rdma_unregister_region(pair_local);if(status==0)pair_local={};return status;}
extern "C" int @NAME@_local_current(){return wki_secure_rdma_local_region_current(identity(),p::Service::VFS,pair_local);}
extern "C" void @NAME@_rotate(bool auth){auto&peer=g_wki.peers[0];if(auth){
 // Auth publishes the new SID/generation/keys before dropping its replay guard.
 // Maintenance may retire and begin the replacement in the same bounded pass.
 peer.auth_session.replay_lock.lock();wki_secure_rdma_retire_peer(&peer);
 auth_crypto::secure_erase(peer.auth_session.tx_key.data(),32);auth_crypto::secure_erase(peer.auth_session.rx_key.data(),32);
 ++peer.auth_session.generation;++peer.auth_session.session_id[0];peer.auth_session.tx_key.fill(@TX@+4);peer.auth_session.rx_key.fill(@RX@+4);peer.auth_session.replay_lock.unlock();
 }else{provider.chaos_unregistering.store(true);wki_secure_rdma_retire_transport(&provider,provider.registration_generation);}
 current=&worker;maintain_peer(0,now);maintain_regions();current=&caller;
 if(auth){CHECK(s_peers[0].begun&&!s_peers[0].installed&&!s_peers[0].confirmed&&s_peers[0].sid==peer.auth_session.session_id);}
 else{CHECK(!s_peers[0].begun);++provider.registration_generation;provider.chaos_unregistering.store(false);}}
extern "C" void @NAME@_stop(){static_cast<void>(wki_secure_rdma_shutdown());}
extern "C" void @NAME@_finish(){CHECK(pair_local.rkey==0);finish();WkiSecureRdmaDiagnostics d{};CHECK(wki_secure_rdma_diagnostics(d));CHECK(d.live_keys==0&&d.replay_windows==0&&d.live_regions==0&&d.live_operations==0&&d.region_references==0&&d.region_claims==0&&d.consumer_pins==0&&d.pending_revocations==0&&provider.chaos_in_flight.load()==0&&caller.refs.load()==1);CHECK(publications==0&&park_calls==0&&halt_calls==0&&!effect_entered);auth_crypto::secure_erase(g_wki.peers[0].auth_session.tx_key.data(),32);auth_crypto::secure_erase(g_wki.peers[0].auth_session.rx_key.data(),32);std::printf("paired_node name=@NAME@ checks=%u publications=%u wakes=%u parks=%u halts=%u yields=%u timer=%u signal_parks=%u PASS\n",checks,publications,wake_calls,park_calls,halt_calls,yield_calls,timer_rechecks,park_with_signal);}
'''
CONTROLLER = r'''
#include "paired_api.hpp"
#include <array>
#include <deque>
#include <vector>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <net/wki/wki.hpp>
#include <net/wki/rdma_protocol.hpp>
using ker::net::wki::MsgType;
namespace p=ker::net::wki::rdma_protocol;
static unsigned checks=0;
#define CHECK(x) do{++checks;if(!(x)){std::fprintf(stderr,"FAIL:%d:%s\n",__LINE__,#x);std::abort();}}while(false)
struct Packet {unsigned src=0,type=0,size=0;uint32_t seq=0;std::array<uint8_t,184> data{};std::array<uint8_t,16> sid{};unsigned attempts=0;uint64_t held_until=0;};
static std::array<std::deque<Packet>,2> queues;
static std::array<uint32_t,2> sequence{10,10};
static uint64_t now=1000000,generated=0,injected=0;
static std::array<unsigned,2> reject_confirm{};
static bool lose_confirm=false,lost=false;
static unsigned retained_retries=0,auth_drops=0;
static uint32_t retained_seq=0;
static std::vector<uint64_t> rejected_at;
static void sid(unsigned node,void*out){if(node==1)pair_a_sid(out);else pair_b_sid(out);}
extern "C" int paired_publish(unsigned node,unsigned type,const void*data,unsigned size){
 CHECK(node==1||node==2);CHECK(size<=184);++generated;
 if(type==static_cast<unsigned>(MsgType::SECURE_RDMA_CONFIRM)&&reject_confirm[node-1]){--reject_confirm[node-1];rejected_at.push_back(now);return ker::net::wki::WKI_ERR_TX_FAILED;}
 CHECK(queues[node-1].size()<256);Packet p{};p.src=node;p.type=type;p.size=size;p.seq=sequence[node-1]++;sid(node,p.sid.data());std::memcpy(p.data.data(),data,size);queues[node-1].push_back(p);return 0;
}
static void inject_offer(unsigned node,const std::array<uint8_t,64>&offer){Packet p{};p.src=node;p.type=static_cast<unsigned>(MsgType::SECURE_RDMA_OFFER);p.size=64;p.seq=sequence[node-1]++;p.data.fill(0);std::memcpy(p.data.data(),offer.data(),64);sid(node,p.sid.data());queues[node-1].push_back(p);++injected;}
static void deliver(){for(unsigned node=1;node<=2;++node){auto&q=queues[node-1];unsigned budget=8;while(!q.empty()&&budget--){auto&packet=q.front();std::array<uint8_t,16> current{};sid(node==1?2:1,current.data());if(current!=packet.sid){++auth_drops;q.pop_front();continue;}
 if(lose_confirm&&!lost&&node==1&&packet.type==static_cast<unsigned>(MsgType::SECURE_RDMA_CONFIRM)){lost=true;packet.held_until=now+50000;packet.attempts=1;retained_seq=packet.seq;break;}
 if(packet.held_until&&now<packet.held_until)break;
 if(packet.held_until){CHECK(packet.seq==retained_seq&&packet.attempts==1);++packet.attempts;++retained_retries;packet.held_until=0;}
 bool accepted=node==1?pair_b_ingress(packet.type,packet.data.data(),packet.size,packet.seq):pair_a_ingress(packet.type,packet.data.data(),packet.size,packet.seq);if(!accepted)break;q.pop_front();
 }} }
static void turn(uint64_t elapsed=1000){now+=elapsed;pair_a_step(now);pair_b_step(now);deliver();}
static PairView a(){PairView v{};pair_a_view(&v);return v;}static PairView b(){PairView v{};pair_b_view(&v);return v;}
static bool ready(){auto x=a(),y=b();return x.installed&&x.confirmed&&x.sent&&x.keys==8&&y.installed&&y.confirmed&&y.sent&&y.keys==8;}
static void settle(){for(unsigned n=0;n<1000&&!ready();++n)turn();CHECK(ready());for(unsigned n=0;n<4;++n)turn();}
static void drain(){for(unsigned n=0;n<8;++n)turn();CHECK(queues[0].empty()&&queues[1].empty());CHECK(a().queued==0&&b().queued==0);}
static std::array<uint8_t,64> offer(unsigned node){std::array<uint8_t,64> out{};if(node==1)pair_a_offer(out.data());else pair_b_offer(out.data());return out;}
static void close_regions(){for(unsigned n=0;n<100;++n){int x=pair_a_close(),y=pair_b_close();if(x==0&&y==0)return;CHECK(x==0||x==-11||x==-16);CHECK(y==0||y==-11||y==-16);turn();}CHECK(false);}
int main(int argc,char**argv){CHECK(argc==2);std::string mode=argv[1];
 if(mode=="prepublication-confirm")reject_confirm[0]=2;
 if(mode=="duplicate-unconfirmed")reject_confirm={1,1};
 if(mode=="accepted-lost-confirm")lose_confirm=true;
 pair_a_init();pair_b_init();
 if(mode=="duplicate-unconfirmed"){
  for(unsigned n=0;n<4;++n)turn();CHECK(a().installed&&b().installed&&!a().confirmed&&!b().confirmed);auto oa=offer(1),ob=offer(2);auto before_a=a(),before_b=b();uint64_t count=generated;
  for(unsigned n=0;n<16;++n){inject_offer(1,oa);inject_offer(2,ob);turn(0);turn(0);}
  CHECK(a().retry==before_a.retry&&b().retry==before_b.retry&&generated==count);now+=100000;settle();
 }else settle();
 CHECK(a().incarnation!=0&&b().incarnation!=0);
 if(mode=="prepublication-confirm"){CHECK(rejected_at.size()==2&&rejected_at[1]>=rejected_at[0]+100000);}
 if(mode=="accepted-lost-confirm"){CHECK(lost&&retained_retries==1&&retained_seq!=0);}
 if(mode=="steady"){
  pair_a_register();pair_b_register();drain();CHECK(pair_a_local_current()==0&&pair_b_local_current()==0);
  std::array<uint8_t,144> record{};unsigned size=pair_a_record(record.data());auto original=record;CHECK(pair_b_decode(record.data(),size)==static_cast<unsigned>(p::Drop::NONE));auto oa=offer(1),ob=offer(2);auto before_a=a(),before_b=b();uint64_t count=generated;
  for(unsigned n=0;n<32;++n){inject_offer(1,oa);inject_offer(2,ob);turn(0);turn(0);}
  CHECK(generated==count&&a().retry==before_a.retry&&b().retry==before_b.retry);
  for(unsigned n=0;n<20;++n)turn(100000);CHECK(generated==count);
  CHECK(a().incarnation==before_a.incarnation&&b().incarnation==before_b.incarnation&&a().next_tx==before_a.next_tx&&b().rx_high==before_b.rx_high);
  CHECK(pair_a_local_current()==0&&pair_b_local_current()==0);record=original;CHECK(pair_b_decode(record.data(),size)==static_cast<unsigned>(p::Drop::DUPLICATE));CHECK((record==std::array<uint8_t,144>{}));
  pair_a_record(record.data());p::Header h{};std::memcpy(&h,record.data(),144);CHECK(h.counter==2&&pair_b_decode(record.data(),144)==static_cast<unsigned>(p::Drop::NONE));
 }
 if(mode=="provider-rotation"||mode=="auth-rotation"){
  pair_a_register();pair_b_register();drain();auto oa=offer(1),ob=offer(2);auto before_a=a(),before_b=b();std::array<uint8_t,144> old_record{};pair_a_record(old_record.data());
  pair_a_rotate(mode=="auth-rotation");pair_b_rotate(mode=="auth-rotation");CHECK(pair_a_local_current()!=0&&pair_b_local_current()!=0);close_regions();settle();CHECK(a().incarnation>before_a.incarnation&&b().incarnation>before_b.incarnation&&offer(1)!=oa&&offer(2)!=ob);
  auto fresh_a=a(),fresh_b=b();uint64_t count=generated;inject_offer(1,oa);inject_offer(2,ob);drain();CHECK(a().incarnation==fresh_a.incarnation&&b().incarnation==fresh_b.incarnation&&ready()&&generated==count);
  CHECK((pair_b_decode(old_record.data(),144)!=static_cast<unsigned>(p::Drop::NONE)&&old_record==std::array<uint8_t,144>{}));
  pair_a_register();pair_b_register();drain();CHECK(pair_a_local_current()==0&&pair_b_local_current()==0);
 }
 close_regions();pair_a_stop();pair_b_stop();for(unsigned n=0;n<20;++n)turn();pair_a_finish();pair_b_finish();deliver();CHECK(queues[0].empty()&&queues[1].empty());CHECK(a().keys==0&&b().keys==0);
 std::printf("paired_negotiation case=%s checks=%u generated=%llu injected=%llu retained_retries=%u auth_drops=%u PASS\n",mode.c_str(),checks,static_cast<unsigned long long>(generated),static_cast<unsigned long long>(injected),retained_retries,auth_drops);
}
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, default=KERN / 'net/wki/secure_rdma.cpp')
    parser.add_argument('--evidence', type=Path)
    parser.add_argument('--case', action='append', choices=CASES)
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location('engine_fixture', BASE)
    base = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(base)
    with tempfile.TemporaryDirectory(prefix='wki-paired-negotiation-') as temp:
        folder = (args.evidence or Path(temp)).resolve()
        folder.mkdir(parents=True, exist_ok=True)
        if (folder / 'result.json').exists():
            raise RuntimeError('Use fresh evidence; all outcomes are immutable')
        (folder / 'fixture.py').write_bytes(Path(__file__).read_bytes())
        (folder / 'engine-fixture.py').write_bytes(BASE.read_bytes())
        (folder / 'paired_api.hpp').write_text(API)
        (folder / 'controller.cpp').write_text(CONTROLLER)
        (folder / 'tmp').mkdir(exist_ok=True)
        env = dict(os.environ, TMPDIR=str(folder / 'tmp'), ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1')
        flags = ['-std=c++23', '-O1', '-g', '-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-pthread',
                 '-Wall', '-Wextra', '-Werror', '-Wno-unused-function', '-Wno-unused-variable']
        commands, objects, logs = [], [], []
        for suffix, node, other, tx, rx in [('a', 1, 2, 11, 22), ('b', 2, 1, 22, 11)]:
            node_folder = folder / ('node-' + suffix)
            target, include, snapshot = base.prepare(node_folder, args.source)
            text = target.read_text().split('int main(int argc', 1)[0]
            signature = 'auto wki_send_on_channel_identity('
            original = base.extract_function(text, signature)
            text = text.replace(original, f'auto wki_send_on_channel_identity(const WkiChannelIdentity&,MsgType type,const void*payload,uint16_t size)->int{{return paired_publish({node},static_cast<unsigned>(type),payload,size);}}', 1)
            text = '#include "paired_api.hpp"\n' + text
            text = text.replace('if(peer!=2)return nullptr;', f'if(peer!={other})return nullptr;', 1)
            text = text.replace('return {&data,2,40,data.generation};', f'return {{&data,{other},40,data.generation}};', 1)
            text = text.replace('return {&control,2,4,control.generation};', f'return {{&control,{other},4,control.generation}};', 1)
            exports = EXPORTS
            for token, value in [('NAME', 'pair_' + suffix), ('SELF', node), ('PEER', other), ('TX', tx), ('RX', rx)]:
                exports = exports.replace('@' + token + '@', str(value))
            target.write_text(text + exports)
            compile_flags = flags + ['-I', str(folder), '-I', str(include), '-I', str(node_folder / 'source'), '-I', str(KERN)]
            if suffix == 'b':
                compile_flags += ['-Dker=ker_pair_b']
            for index, source in enumerate([target] + [snapshot / name for name in base.SOURCES]):
                obj = node_folder / (str(index) + '.o')
                command = ['/usr/bin/clang++'] + compile_flags + ['-c', str(source), '-o', str(obj)]
                commands.append(command)
                run = subprocess.run(command, capture_output=True, text=True, env=env, timeout=180)
                log = node_folder / (str(index) + '-compile.log')
                log.write_text(run.stdout + run.stderr)
                logs.append(log)
                if run.returncode:
                    print(run.stderr)
                    (folder / 'compile-failure.json').write_text(json.dumps({'exit': run.returncode, 'command': command}, indent=2) + '\n')
                    return run.returncode
                objects.append(obj)
        binary = folder / 'paired-test'
        command = ['/usr/bin/clang++'] + flags + ['-I', str(folder), '-I', str(folder / 'node-a/include'), '-I', str(folder / 'node-a/source'), '-I', str(KERN), str(folder / 'controller.cpp')] + [str(obj) for obj in objects] + ['-o', str(binary)]
        commands.append(command)
        run = subprocess.run(command, capture_output=True, text=True, env=env, timeout=180)
        (folder / 'link.log').write_text(run.stdout + run.stderr)
        if run.returncode:
            print(run.stderr)
            (folder / 'compile-failure.json').write_text(json.dumps({'exit': run.returncode, 'command': command}, indent=2) + '\n')
            return run.returncode
        outcomes = []
        for case in args.case or CASES:
            run = subprocess.run([str(binary), case], capture_output=True, text=True, env=env, timeout=30)
            (folder / (case + '.log')).write_text(run.stdout + run.stderr)
            print(run.stdout + run.stderr, end='')
            outcomes.append({'case': case, 'exit': run.returncode})
            if run.returncode:
                break
        def pin(path):
            return {'path': str(path.resolve()), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        inputs = [pin(path) for node in (folder / 'node-a', folder / 'node-b') for path in sorted(node.rglob('*'))
                  if path.is_file() and path.suffix not in ('.o', '.log')]
        for outcome in outcomes:
            outcome['log'] = pin(folder / (outcome['case'] + '.log'))
        (folder / 'result.json').write_text(json.dumps({'compile_exit': 0, 'commands': commands,
            'compiler_binary': pin(Path('/usr/bin/clang++').resolve()), 'compile_logs': [pin(path) for path in logs],
            'link_log': pin(folder / 'link.log'), 'fixture': pin(folder / 'fixture.py'), 'engine_fixture': pin(folder / 'engine-fixture.py'),
            'controller': pin(folder / 'controller.cpp'), 'api': pin(folder / 'paired_api.hpp'), 'binary': pin(binary),
            'objects': [pin(path) for path in objects], 'inputs': inputs, 'cases': outcomes,
            'sanitizers': {key: env[key] for key in ('ASAN_OPTIONS', 'UBSAN_OPTIONS')},
            'boundaries': ['Kernel namespace rename isolates two verbatim complete engines/model sets',
                           'Task/platform seams inherited from complete-engine fixture',
                           'Owned authenticated control queues and reliable scheduling are host seams, not NIC/core-TX-RTO execution',
                           'Actual pre-ACK admission and worker control processing preserve queue ownership'],
            'not_claimed': 'No physical network timing, scheduling, sixth-WRITE loss attribution or VM qualification'}, indent=2) + '\n')
        return 0 if all(outcome['exit'] == 0 for outcome in outcomes) else 1


if __name__ == '__main__':
    raise SystemExit(main())
