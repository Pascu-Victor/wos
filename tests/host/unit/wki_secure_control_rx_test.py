#!/usr/bin/env python3
"""Actual pre-ACK core blocks composed with the complete secure-RDMA engine.

Authentication and the outer peer reader are entry preconditions. Generic
reorder insertion, other service admissions, timers and scheduling are explicit
boundaries. The tested sequence/ACK/unlink and secure dispatch bodies are copied
verbatim from wki.cpp; all engine/model/crypto code is compiled unmodified.
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
CORE = KERN / 'net/wki/wki.cpp'
BASE = Path(__file__).with_name('wki_secure_rdma_engine_test.py')
CASES = ('in-order-gate', 'reorder-gate', 'in-order-full', 'reorder-full',
         'private-fifo', 'reorder-private-fifo', 'eight-controls', 'discard', 'default-off', 'ordinary-control',
         'worker-peer-pressure', 'worker-channel-pressure', 'worker-gate-pressure',
         'worker-rotation', 'region-maintenance-pressure', 'region-maintenance-close', 'effect-pressure')


def balanced(text, start):
    opening = text.index('{', start)
    depth = 0
    for pos in range(opening, len(text)):
        if text[pos] == '{':
            depth += 1
        elif text[pos] == '}':
            depth -= 1
            if not depth:
                return text[start:pos + 1]
    raise ValueError('Unterminated actual source block')


def extract(text):
    helper = balanced(text, text.index('auto admit_secure_control('))
    i = text.index('WkiSecureRdmaControlRxAdmission const SECURE_ADMISSION =')
    end = text.index('                if (SECURE_ADMISSION !=', i)
    admission = text[i:end]
    dispatch = balanced(text, end)
    r = text.index('WkiSecureRdmaControlRxAdmission const RO_SECURE_ADMISSION =')
    rend = text.index('                    if (RO_SECURE_ADMISSION !=', r)
    reorder = text[r:rend]
    rdispatch = balanced(text, rend)
    release = 'wki_reorder_entry_release(ro);'
    assert text.count(release) >= 1
    # The actual top-level switch must route each control into the reliable arm.
    rx = text[text.index('void wki_rx('):]
    route = rx[rx.index('switch (msg)'):rx.index('            ch->lock.lock();', rx.index('switch (msg)'))]
    names = ('OFFER', 'CONFIRM', 'GRANT', 'REVOKE', 'ARM', 'ARM_ACK', 'CANCEL', 'RESULT')
    for name in names:
        assert f'case MsgType::SECURE_RDMA_{name}:' in route, f'Missing outer route {name}'
    lifecycle = balanced(text, text.index('auto message_uses_rx_peer_lifecycle('))
    shared = balanced(text, text.index('auto message_allows_shared_rx_peer_lifecycle('))
    assert 'SECURE_RDMA_OFFER' in lifecycle and 'SECURE_RDMA_RESULT' in lifecycle
    assert 'WKI_CHAN_SECURE_RDMA' in shared and 'SECURE_RDMA_OFFER' in shared
    assert admission.index('admit_secure_control(') < admission.index('ch->rx_seq++')
    assert reorder.index('admit_secure_control(') < reorder.index('ch->reorder_head = ro->next')
    return {'helper': helper, 'in_order': admission, 'in_order_dispatch': dispatch,
            'reorder': reorder, 'reorder_dispatch': rdispatch, 'release': release,
            'outer_route': route, 'lifecycle': lifecycle, 'shared_lifecycle': shared}


WRAPPERS = r'''
static unsigned turns=0,releases=0,progress=0;
static void mark_peer_rx_progress(uint16_t){progress++;}
static void finish_pre_admitted_reliable_msg_ordered(WkiChannel* ch,uint32_t gen,const WkiHeader*){
 CHECK(gen==ch->generation);CHECK(ch->lock.try_lock());ch->lock.unlock();turns++;
}
static void refresh_rx_credits(WkiChannel*){}
static void wki_reorder_entry_release(WkiReorderEntry* ro){releases++;delete[] ro->data;delete ro;}
@HELPER@
static void core_in_order(const WkiHeader* hdr,const uint8_t* payload,uint16_t PAYLOAD_LEN){
 auto* ch=&control;auto msg=static_cast<MsgType>(hdr->msg_type);
 CHECK(wki_peer_lifecycle_rx_try_acquire(&g_wki.peers[0]));ch->lock.lock();
 core_active=true;auto body=[&](){
@IN_ORDER@
@IN_ORDER_DISPATCH@
 };
 body();core_active=false;wki_peer_lifecycle_rx_release(&g_wki.peers[0]);
}
static void core_reorder(){
 auto* ch=&control;CHECK(wki_peer_lifecycle_rx_try_acquire(&g_wki.peers[0]));ch->lock.lock();
 core_active=true;while(ch->reorder_head&&ch->reorder_head->seq==ch->rx_seq){
 auto RO_MSG=static_cast<MsgType>(ch->reorder_head->msg_type);
@REORDER@
@REORDER_DISPATCH@
@RELEASE@
 ch->lock.lock();
 }
 ch->lock.unlock();core_active=false;wki_peer_lifecycle_rx_release(&g_wki.peers[0]);
}
static WkiHeader header(MsgType type,uint16_t size){WkiHeader h{};h.src_node=2;h.dst_node=1;h.channel_id=4;h.msg_type=static_cast<uint8_t>(type);h.seq_num=control.rx_seq;h.payload_len=size;return h;}
static p::RegionDescriptor grant(uint32_t key){p::RegionDescriptor d{};d.rkey=key;d.size=16384;d.generation=10;d.binding_generation=20;d.capability.fill(0x72);d.session_id=g_wki.peers[0].auth_session.session_id;d.channel_id=40;d.transport=static_cast<uint8_t>(kind);d.service=static_cast<uint8_t>(p::Service::VFS);d.permissions=p::PERMIT_READ|p::PERMIT_WRITE;return d;}
static void deliver(p::RegionDescriptor&d){auto h=header(MsgType::SECURE_RDMA_GRANT,sizeof(d));core_in_order(&h,reinterpret_cast<const uint8_t*>(&d),sizeof(d));}
static WkiReorderEntry* buffered(p::RegionDescriptor&d){auto* ro=new WkiReorderEntry{};ro->hdr=header(MsgType::SECURE_RDMA_GRANT,sizeof(d));ro->seq=control.rx_seq;ro->msg_type=ro->hdr.msg_type;ro->len=sizeof(d);ro->channel_generation=control.generation;ro->data=new uint8_t[sizeof(d)];std::memcpy(ro->data,&d,sizeof(d));control.reorder_head=ro;control.reorder_count=1;return ro;}
struct RxState{uint32_t seq,ack,count;bool pending;uint64_t bytes;WkiReorderEntry*head;};
static auto saved()->RxState{return {control.rx_seq,control.rx_ack_pending,control.reorder_count,control.ack_pending,control.bytes_received,control.reorder_head};}
static void unchanged(RxState before){CHECK(control.rx_seq==before.seq);CHECK(control.rx_ack_pending==before.ack);CHECK(control.reorder_count==before.count);CHECK(control.ack_pending==before.pending);CHECK(control.bytes_received==before.bytes);CHECK(control.reorder_head==before.head);}
static auto queued(size_t index)->ControlCell&{CHECK(index<s_control_count);return s_controls[s_control_order[(s_control_head+index)%CONTROL_CELLS]];}
static uint32_t queued_key(size_t index){p::RegionDescriptor d{};std::memcpy(&d,queued(index).payload.data(),sizeof(d));return d.rkey;}
static void clear_controls(){for(unsigned i=0;i<20&&s_control_count;i++)pump();CHECK(s_control_count==0);}
int main(int argc,char**argv){CHECK(argc==2);std::string mode=argv[1];if(mode=="default-off")profile="";setup();clear_controls();
 control.rx_seq=90;control.rx_ack_pending=17;control.ack_pending=false;control.bytes_received=3;
 if(mode=="in-order-gate"||mode=="reorder-gate"){
 auto d=grant(334);if(mode=="reorder-gate")buffered(d);auto before=saved();auto wakes=wake_calls;auto count=s_control_count;
 s_lock.lock();if(mode=="in-order-gate")deliver(d);else core_reorder();s_lock.unlock();
 unchanged(before);CHECK(s_control_count==count&&wake_calls==wakes&&turns==0&&releases==0);
 if(mode=="in-order-gate")deliver(d);else core_reorder();CHECK(control.rx_seq==before.seq+1&&control.ack_pending&&turns==1&&s_control_count==1);
 CHECK(queued_key(0)==334);clear_controls();p::RegionDescriptor out{};CHECK(wki_secure_rdma_remote_grant(identity(),p::Service::VFS,334,out));
 }else if(mode=="in-order-full"||mode=="reorder-full"){
 for(uint32_t i=0;i<64;i++){auto d=grant(1000+i);deliver(d);}CHECK(s_control_count==64&&turns==64);
 auto d=grant(1999);if(mode=="reorder-full")buffered(d);auto before=saved();auto wakes=wake_calls;
 if(mode=="in-order-full")deliver(d);else core_reorder();unchanged(before);CHECK(s_control_count==64&&wake_calls==wakes&&turns==64&&releases==0);
 current=&worker;drain_rx(now);current=&caller;CHECK(s_control_count<64);
 if(mode=="in-order-full")deliver(d);else core_reorder();CHECK(control.rx_seq==before.seq+1&&turns==65&&queued_key(s_control_count-1)==1999);
 clear_controls();
 }else if(mode=="private-fifo"||mode=="reorder-private-fifo"){
 WkiReorderEntry* first=nullptr;WkiReorderEntry* tail=nullptr;
 for(uint32_t i=0;i<3;i++){auto d=grant(800+i);if(mode=="private-fifo")deliver(d);else{auto* ro=buffered(d);ro->seq=90+i;ro->hdr.seq_num=ro->seq;if(tail)tail->next=ro;else first=ro;tail=ro;}std::memset(&d,0xa5,sizeof(d));}
 if(mode=="reorder-private-fifo"){control.reorder_head=first;control.reorder_count=3;core_reorder();CHECK(releases==3);}
 CHECK(s_control_count==3&&turns==3);for(uint32_t i=0;i<3;i++){CHECK(queued_key(i)==800+i);CHECK(queued(i).auth_generation==1&&queued(i).sid==g_wki.peers[0].auth_session.session_id);CHECK(queued(i).identity.channel==&control&&queued(i).identity.generation==1);}
 clear_controls();for(uint32_t i=0;i<3;i++){p::RegionDescriptor out{};CHECK(wki_secure_rdma_remote_grant(identity(),p::Service::VFS,800+i,out)&&out.rkey==800+i);}
 }else if(mode=="eight-controls"){
 for(auto type:{MsgType::SECURE_RDMA_OFFER,MsgType::SECURE_RDMA_CONFIRM,MsgType::SECURE_RDMA_GRANT,MsgType::SECURE_RDMA_REVOKE,MsgType::SECURE_RDMA_ARM,MsgType::SECURE_RDMA_ARM_ACK,MsgType::SECURE_RDMA_CANCEL,MsgType::SECURE_RDMA_RESULT}){
 std::array<uint8_t,sizeof(rdma_control::Result)> payload{};auto size=control_size(type);CHECK(size>0);auto h=header(type,size);auto count=s_control_count;core_in_order(&h,payload.data(),size);CHECK(s_control_count==count+1&&queued(count).type==type);}
 CHECK(turns==8&&control.rx_seq==98);clear_controls();
 }else if(mode=="discard"||mode=="default-off"){
 auto d=grant(700);auto h=header(MsgType::SECURE_RDMA_GRANT,sizeof(d)-1);core_in_order(&h,reinterpret_cast<const uint8_t*>(&d),sizeof(d)-1);
 CHECK(control.rx_seq==91&&turns==1&&s_control_count==0);auto wakes=wake_calls;
 if(mode=="discard"){h=header(MsgType::SECURE_RDMA_GRANT,sizeof(d));h.channel_id=3;core_in_order(&h,reinterpret_cast<const uint8_t*>(&d),sizeof(d));}
 else deliver(d);CHECK(control.rx_seq==92&&turns==2&&s_control_count==0&&wake_calls==wakes);
 }else if(mode=="ordinary-control"){
 std::array<uint8_t,8> bytes{};auto h=header(MsgType::LSA,bytes.size());auto wakes=wake_calls;core_in_order(&h,bytes.data(),bytes.size());
 CHECK(control.rx_seq==91&&control.ack_pending&&control.bytes_received==11&&s_control_count==0&&turns==0&&wake_calls==wakes);
 }else if(mode=="worker-peer-pressure"||mode=="worker-channel-pressure"||mode=="worker-gate-pressure"){
 auto d=grant(900);deliver(d);CHECK(s_control_count==1);auto head=s_control_head;auto raw=queued(0).payload;
 if(mode=="worker-peer-pressure")deny_rx=true;else if(mode=="worker-channel-pressure")control.lock.lock();else s_lock.lock();
 current=&worker;drain_rx(now);current=&caller;
 if(mode=="worker-peer-pressure")deny_rx=false;else if(mode=="worker-channel-pressure")control.lock.unlock();else s_lock.unlock();
 CHECK(s_control_count==1&&s_control_head==head&&queued(0).payload==raw&&queued(0).state==CellState::QUEUED);
 clear_controls();p::RegionDescriptor out{};CHECK(wki_secure_rdma_remote_grant(identity(),p::Service::VFS,900,out));
 }else if(mode=="region-maintenance-pressure"){
 Native n;auto d=local_region(n);
 data.lock.lock();maintain_regions();data.lock.unlock();CHECK(wki_secure_rdma_local_region_current(identity(),p::Service::VFS,d)==0);
 deny_rx=true;maintain_regions();deny_rx=false;CHECK(wki_secure_rdma_local_region_current(identity(),p::Service::VFS,d)==0);
 g_wki.transport_lock.lock();maintain_regions();g_wki.transport_lock.unlock();CHECK(wki_secure_rdma_local_region_current(identity(),p::Service::VFS,d)==0);
 s_lock.lock();maintain_regions();s_lock.unlock();CHECK(wki_secure_rdma_local_region_current(identity(),p::Service::VFS,d)==0);
 CHECK(n.arms==0&&n.effects==0&&n.releases==0);close_region(d);
 }else if(mode=="effect-pressure"){
 Native n;auto d=local_region(n);auto a=arm_for(d,1,8);enqueue_arm(a);CHECK(n.arms==1&&n.effects==0);
 std::array<uint8_t,8> input{};input.fill(0xa7);record(incoming(a,p::Opcode::WRITE,0,8),input.data());current=&worker;drain_rx(now);
 auto found=std::find_if(s_jobs.begin(),s_jobs.end(),[&](auto const&j){return j.arm.operation_id==a.operation_id&&j.state.load()==JobState::IN_WAIT;});CHECK(found!=s_jobs.end());
 data.lock.lock();progress_effect(*found);data.lock.unlock();CHECK(!found->cancel.load()&&n.effects==0);
 deny_rx=true;progress_effect(*found);deny_rx=false;CHECK(!found->cancel.load()&&n.effects==0);
 g_wki.transport_lock.lock();progress_job(*found,now);g_wki.transport_lock.unlock();CHECK(!found->cancel.load()&&n.effects==0);
 s_lock.lock();progress_effect(*found);s_lock.unlock();CHECK(!found->cancel.load()&&n.effects==0);
 progress_effect(*found);current=&caller;CHECK(n.effects==1);for(unsigned i=0;i<12;i++)pump();CHECK(n.releases==1&&n.bytes[0]==0xa7);close_region(d);
 }else if(mode=="region-maintenance-close"){
 Native n;auto d=local_region(n);provider.chaos_unregistering.store(true);maintain_regions();CHECK(wki_secure_rdma_local_region_current(identity(),p::Service::VFS,d)==-ESTALE);
 provider.chaos_unregistering.store(false);CHECK(n.arms==0&&n.effects==0&&n.releases==0);close_region(d);
 }else if(mode=="worker-rotation"){
 auto d=grant(901);deliver(d);CHECK(s_control_count==1);wki_secure_rdma_retire_peer(&g_wki.peers[0]);g_wki.peers[0].auth_session.generation++;g_wki.peers[0].auth_session.session_id[0]++;
 current=&worker;drain_rx(now);current=&caller;CHECK(s_control_count==0);p::RegionDescriptor out{};CHECK(!wki_secure_rdma_remote_grant(identity(),p::Service::VFS,901,out));
 }else CHECK(false);
 CHECK(control.reorder_head==nullptr&&control.reorder_count==0);CHECK(g_wki.peers[0].lifecycle_state.load()==0);finish();
 std::printf("secure_control_rx case=%s checks=%u PASS\n",mode.c_str(),checks);
}
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence', type=Path)
    parser.add_argument('--core', type=Path, default=CORE)
    parser.add_argument('--engine', type=Path, default=KERN / 'net/wki/secure_rdma.cpp')
    parser.add_argument('--case', action='append', choices=CASES)
    parser.add_argument('--mutation', choices=('advance-before-admission', 'unlink-on-retry', 'legacy-double-enqueue'))
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location('engine_fixture', BASE)
    base = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(base)
    with tempfile.TemporaryDirectory(prefix='wki-secure-control-rx-') as temp:
        folder = (args.evidence or Path(temp)).resolve()
        folder.mkdir(parents=True, exist_ok=True)
        if (folder / 'result.json').exists():
            raise RuntimeError('Choose fresh evidence; existing results are immutable')
        target, include, snapshot = base.prepare(folder, args.engine)
        # prepare() renders the actual WKI/scheduler wait policies as well as
        # the selected complete engine. Compose from that rendered TU, rather
        # than rebuilding the old raw template with unresolved placeholders.
        rendered = target.read_text()
        (folder / 'engine-fixture.py').write_bytes(BASE.read_bytes())
        (folder / 'fixture.py').write_bytes(Path(__file__).read_bytes())
        source = args.core.read_text()
        (snapshot / 'wki-wait-policy.cpp').write_bytes((snapshot / 'wki.cpp').read_bytes())
        (snapshot / 'wki.cpp').write_text(source)
        blocks = extract(source)
        for name, body in blocks.items():
            (folder / (name + '.inc')).write_text(body)
        wrappers = WRAPPERS
        for name, body in blocks.items():
            wrappers = wrappers.replace('@' + name.upper() + '@', body)
        if args.mutation == 'advance-before-admission':
            wrappers = wrappers.replace('WkiSecureRdmaControlRxAdmission const SECURE_ADMISSION =', 'ch->rx_seq++;\nWkiSecureRdmaControlRxAdmission const SECURE_ADMISSION =', 1)
        elif args.mutation == 'unlink-on-retry':
            wrappers = wrappers.replace('if (RO_SECURE_ADMISSION == WkiSecureRdmaControlRxAdmission::RETRY) break;', 'if (RO_SECURE_ADMISSION == WkiSecureRdmaControlRxAdmission::RETRY) {}', 1)
        elif args.mutation == 'legacy-double-enqueue':
            wrappers = wrappers.replace('if (SECURE_ADMISSION == WkiSecureRdmaControlRxAdmission::QUEUED) wki_secure_rdma_tick();', 'if (SECURE_ADMISSION == WkiSecureRdmaControlRxAdmission::QUEUED) { wki_secure_rdma_tick(); wki_secure_rdma_control_ingress(hdr,payload,PAYLOAD_LEN,control_identity()); }', 1)
        harness = rendered[:rendered.index('int main(int argc')]
        harness = harness.replace('static unsigned publications=', 'static bool core_active=false;\nstatic unsigned publications=', 1)
        harness = harness.replace('void kern_wake(task::Task*){wake_calls++;}', 'void kern_wake(task::Task*){if(core_active){CHECK(control.lock.try_lock());control.lock.unlock();}wake_calls++;}', 1)
        harness = harness.replace('bool wki_peer_lifecycle_rx_try_acquire(WkiPeer*v){', 'bool wki_peer_lifecycle_rx_try_acquire(WkiPeer*v){CHECK(!core_active);', 1)
        target.write_text(harness + wrappers)
        binary = folder / 'test'
        command = ['/usr/bin/clang++', '-std=c++23', '-O1', '-g', '-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-Wno-unknown-attributes', '-Wno-ignored-attributes', '-Wno-address-of-packed-member', '-I', str(include), '-I', str(folder / 'source'), '-I', str(KERN), str(target)]
        command += [str(snapshot / name) for name in base.SOURCES] + ['-pthread', '-o', str(binary)]
        (folder / 'tmp').mkdir(exist_ok=True)
        env = dict(os.environ, TMPDIR=str(folder / 'tmp'), ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1')
        compile_result = subprocess.run(command, capture_output=True, text=True, env=env, timeout=180)
        (folder / 'compile.log').write_text(compile_result.stdout + compile_result.stderr)
        cases = []
        if compile_result.returncode == 0:
            for case in args.case or CASES:
                run = subprocess.run([str(binary), case], capture_output=True, text=True, env=env, timeout=30)
                (folder / (case + '.log')).write_text(run.stdout + run.stderr)
                print(run.stdout + run.stderr, end='')
                cases.append({'case': case, 'exit': run.returncode})
                if run.returncode:
                    break
        else:
            print(compile_result.stderr)
        sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
        def pin(path):
            return {'path': str(path.resolve()), 'sha256': sha(path)}
        result = {'compile_exit': compile_result.returncode, 'command': command,
                  'mutation': args.mutation, 'cases': cases, 'inputs': [pin(path) for parent in (folder / 'source', include) for path in sorted(parent.rglob('*')) if path.is_file()],
                  'core_blocks': [pin(folder / (name + '.inc')) for name in blocks],
                  'fixture': pin(folder / 'fixture.py'), 'engine_fixture': pin(folder / 'engine-fixture.py'),
                  'wait_extractions': pin(folder / 'wait-extractions.json'),
                  'translation_unit': pin(target), 'compile_log': pin(folder / 'compile.log'),
                  'limits': 'Actual narrow pre-ACK/commit blocks and complete engine; authentication, generic reorder insertion, scheduler and unrelated ordinary admissions are explicit boundaries.'}
        compiler = Path(command[0]).resolve()
        version = subprocess.run([str(compiler), '--version'], capture_output=True, text=True, timeout=10)
        (folder / 'compiler-version.log').write_text(version.stdout + version.stderr)
        result['compiler_binary'] = pin(compiler)
        result['compiler_version'] = pin(folder / 'compiler-version.log')
        result['sanitizers'] = {key: env[key] for key in ('ASAN_OPTIONS', 'UBSAN_OPTIONS')}
        if binary.exists():
            result['binary'] = pin(binary)
        for case in cases:
            case['log'] = pin(folder / (case['case'] + '.log'))
        (folder / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        return compile_result.returncode or (1 if any(case['exit'] for case in cases) else 0)


if __name__ == '__main__':
    raise SystemExit(main())
