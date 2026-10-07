#!/usr/bin/env python3
"""Compile the complete actual ZONE TU and declarations with bounded host seams.

Core secure admission/crypto and scheduler execution are explicit seams. Tests
exercise native arm/effect/release, actual transfer fallback, private allocation,
message correlation/copy exclusion, references and nonblocking retirement.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
KERN = ROOT / 'modules/kern/src'
ZONE = KERN / 'net/wki/zone.cpp'
HEADER = KERN / 'net/wki/zone.hpp'
WKI = KERN / 'net/wki/wki.hpp'


def declaration(text, name):
    start = text.index('struct ' + name + ' {')
    pos = start + text[start:].index('{')
    depth = 1
    end = pos + 1
    while depth:
        depth += (text[end] == '{') - (text[end] == '}')
        end += 1
    return text[start:end + 1]


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <span>
#include <thread>
#include <utility>
#include <vector>
#include <net/wki/wire.hpp>
#include <net/wki/secure_rdma.hpp>
#define RX_CALL(...) do {CHECK(!ker::net::wki::lifecycle);ker::net::wki::lifecycle=true;__VA_ARGS__;ker::net::wki::lifecycle=false;} while(0)
#define CHECK(x) do { if (!(x)) {std::fprintf(stderr,"CHECK failed %s:%d: %s\n",__FILE__,__LINE__,#x); std::abort();}} while(0)
namespace fixture {
thread_local unsigned guards=0;
unsigned raw_allocations=0;
std::array<uint8_t,8192> raw_pool{};
unsigned allocations=0,frees=0,registrations=0,unregistrations=0,wakes=0,secure_reads=0,secure_writes=0,message_reads=0,message_writes=0;
bool available=true,grant_live=true,capture_pressure=false,unregister_busy=false,registration_fail=false,auto_reply=true;
std::mutex pool_guard;
std::map<void*,size_t> pool;
std::vector<std::pair<ker::net::wki::MsgType,std::vector<uint8_t>>> sent;
std::function<void()> registration_hook,onwait,grant_hook;
std::function<void()> onpark;
thread_local unsigned shutdown_holds=0;
bool shutdown_requested=false,post_success=true;
ker::net::wki::WkiSecureRdmaHooks hooks{};
void* context=nullptr;
ker::net::wki::rdma_protocol::RegionDescriptor remote{};
ker::net::wki::WkiSecureRdmaResult result{0,4,ker::net::wki::WkiSecureRdmaOutcome::EXACT};
}
namespace ker::mod::sys {
struct Spinlock {
 std::mutex mutex;
 void lock(){mutex.lock();++fixture::guards;}
 void unlock(){CHECK(fixture::guards);--fixture::guards;mutex.unlock();}
 auto lock_irqsave()->uint64_t {lock();return 0;}
 void unlock_irqrestore(uint64_t){unlock();}
};
}
namespace ker::mod::dbg {
template<size_t N> struct Name {char bytes[N];constexpr Name(const char(&s)[N]){std::copy_n(s,N,bytes);}};
template<Name N> struct logger {
 template<class...A> static void info(const char*,A...){}
 template<class...A> static void error(const char*,A...){}
 template<class...A> static void warn(const char*,A...){}
};
}
namespace ker::mod::sched {
namespace task {struct Task {
 uint64_t slice_ns=0;int pid=1;
 std::atomic<int>refs{1};bool destroyed=false;inline static Task*next=nullptr;
 auto try_acquire_lifetime_ref()->bool{CHECK(!destroyed);if(refs.load()==0)return false;refs.fetch_add(1);return true;}
 void release(){CHECK(!fixture::guards&&!destroyed&&refs.fetch_sub(1)>1);}
 static auto create_kernel_thread(const char*,void(*)())->Task* {CHECK(!fixture::guards);return std::exchange(next,nullptr);}
};void destroy_unpublished_user_thread(Task*t){CHECK(!fixture::guards&&t&&!t->destroyed&&t->refs==1);t->refs=0;t->destroyed=true;}}
struct ShutdownHold{ShutdownHold(){++fixture::shutdown_holds;}~ShutdownHold(){CHECK(fixture::shutdown_holds);--fixture::shutdown_holds;}};
auto hold_kernel_shutdown()->ShutdownHold{return {};}
auto kernel_threads_shutdown_requested()->bool{return fixture::shutdown_requested;}
struct CleanExit{};
struct CooperativeExit{};
[[noreturn]] void exit_current_kernel_thread(){CHECK(!fixture::guards&&!fixture::shutdown_holds);throw CleanExit{};}
void wake_task_from_event(task::Task*){CHECK(!fixture::guards);++fixture::wakes;}
void kern_block(){CHECK(!fixture::guards);if(fixture::onpark)fixture::onpark();if(fixture::shutdown_requested&&!fixture::shutdown_holds)throw CooperativeExit{};}
void kern_yield(){CHECK(!fixture::guards);std::this_thread::yield();}
void kern_sleep_us(uint64_t interval){CHECK(!fixture::guards && interval==1000);}
void set_task_nice(task::Task*,int){}
auto post_task_balanced(task::Task*)->bool{return fixture::post_success;}
}
namespace ker::mod::mm { enum class PhysicalPageOwner {WKI_ZONE}; }
namespace ker::mod::mm::addr { auto get_phys_pointer(uint64_t v)->void* {return reinterpret_cast<void*>(v);} }
namespace ker::mod::mm::phys {
auto page_alloc(PhysicalPageOwner,size_t size,const char*)->void* {
 CHECK(!fixture::guards);auto*p=std::malloc(size);CHECK(p);std::lock_guard g(fixture::pool_guard);fixture::pool[p]=size;++fixture::allocations;return p;
}
void page_free(void*p) {
 CHECK(!fixture::guards);std::lock_guard g(fixture::pool_guard);CHECK(fixture::pool.contains(p));
 auto size=fixture::pool[p];for(size_t i=0;i<size;++i)CHECK(static_cast<uint8_t*>(p)[i]==0);
 fixture::pool.erase(p);++fixture::frees;std::free(p);
}
}
namespace ker::net::wki {
/* actual core declarations */
enum class PeerState:uint8_t {UNKNOWN,HELLO_SENT,CONNECTED,FENCED,RECONNECTING};
struct WkiTransport {bool rdma_capable=false;
 int(*rdma_register_region)(WkiTransport*,uint64_t,uint32_t,uint32_t*)=nullptr;
};
struct WkiChannel {bool active=true;uint16_t peer_node_id=2,channel_id=WKI_CHAN_ZONE_MGMT;uint32_t generation=7;ker::mod::sys::Spinlock lock;};
struct WkiPeer {uint16_t node_id=2;PeerState state=PeerState::CONNECTED;std::atomic<bool> vfs_reset_rebind_pending{false};WkiTransport*transport=nullptr,*rdma_transport=nullptr;};
WkiPeer peer;
WkiChannel channel;
bool lifecycle=false;
auto wki_peer_find(uint16_t id)->WkiPeer*{return id==peer.node_id?&peer:nullptr;}
auto wki_peer_lifecycle_acquire(WkiPeer*p)->bool {if(!p||lifecycle)return false;lifecycle=true;return true;}
auto wki_peer_lifecycle_try_acquire(WkiPeer*p)->bool{return wki_peer_lifecycle_acquire(p);}
void wki_peer_lifecycle_release(WkiPeer*){CHECK(lifecycle);lifecycle=false;}
auto wki_channel_generation_is_live(WkiChannel*c,uint16_t node,uint16_t id,uint32_t generation)->bool {
 CHECK(!fixture::guards);return c&&c->active&&c->peer_node_id==node&&c->channel_id==id&&c->generation==generation;
}
auto wki_channel_get(uint16_t,uint16_t)->WkiChannel*{return &channel;}
auto wki_claim_op(WkiWaitEntry*w)->bool{if(!w)return false;uint8_t state=WkiWaitEntry::PENDING;return w->state.compare_exchange_strong(state,WkiWaitEntry::CLAIMED);}
void wki_finish_claimed_op(WkiWaitEntry*w,int result){CHECK(!fixture::guards);w->result=result;w->state.store(WkiWaitEntry::DONE);}
auto wki_wait_for_op(WkiWaitEntry*w,uint64_t)->int {CHECK(!fixture::guards&&!lifecycle);if(fixture::onwait)fixture::onwait();return w->state.load()==WkiWaitEntry::DONE?w->result:WKI_ERR_TIMEOUT;}
constexpr uint32_t EVENT_ZONE_CREATED=2,EVENT_ZONE_DESTROYED=3;
auto wki_ivshmem_rdma_alloc(uint32_t size)->int64_t {CHECK(size<=fixture::raw_pool.size());++fixture::raw_allocations;return 0;}
auto wki_ivshmem_rdma_ptr(int64_t)->void* {return fixture::raw_pool.data();}
void wki_ivshmem_rdma_free(int64_t,uint32_t){}

void wki_event_publish(uint32_t,uint32_t,const void*,size_t){CHECK(!fixture::guards);}
auto wki_send_on_channel_identity(const WkiChannelIdentity&,MsgType,const void*,uint16_t)->int;
auto wki_secure_rdma_available(const WkiChannelIdentity&i,rdma_protocol::Service s)->bool {
 CHECK(!fixture::guards);if(lifecycle)return false;CHECK(s==rdma_protocol::Service::ZONE);return fixture::available&&!fixture::capture_pressure&&wki_channel_generation_is_live(i.channel,i.peer_node_id,i.channel_id,i.generation);
}
auto wki_secure_rdma_register_region(const WkiChannelIdentity&i,rdma_protocol::Service s,void*p,uint32_t size,uint64_t gen,uint8_t perms,
 const WkiSecureRdmaHooks&h,void*ctx,rdma_protocol::RegionDescriptor&out)->int {
 CHECK(!fixture::guards&&!lifecycle);CHECK(p&&size&&gen&&s==rdma_protocol::Service::ZONE);++fixture::registrations;
 if(fixture::registration_hook)fixture::registration_hook();if(fixture::registration_fail)return WKI_ERR_BUSY;
 out={};out.rkey=91;out.size=size;out.generation=17;out.binding_generation=gen;out.capability.fill(0xa7);out.session_id.fill(0x3a);
 out.channel_id=i.channel_id;out.transport=1;out.service=1;out.permissions=perms;fixture::hooks=h;fixture::context=ctx;return 0;
}
auto wki_secure_rdma_unregister_region(const rdma_protocol::RegionDescriptor&d)->int {
 CHECK(!fixture::guards&&!lifecycle&&d.rkey==91);++fixture::unregistrations;return fixture::unregister_busy?WKI_ERR_BUSY:0;
}
auto wki_secure_rdma_remote_grant(const WkiChannelIdentity&i,rdma_protocol::Service s,uint32_t key,rdma_protocol::RegionDescriptor&out,uint8_t required_permissions)->bool {
 CHECK(!fixture::guards&&!lifecycle&&s==rdma_protocol::Service::ZONE);if(!fixture::grant_live||(fixture::remote.permissions&required_permissions)!=required_permissions||key!=fixture::remote.rkey||!wki_secure_rdma_available(i,s))return false;if(fixture::grant_hook)fixture::grant_hook();out=fixture::remote;return true;
}
auto wki_secure_rdma_write(const WkiChannelIdentity&,const rdma_protocol::RegionDescriptor&d,uint64_t,std::span<const uint8_t>in,uint16_t,std::span<const uint8_t>)->WkiSecureRdmaResult {
 CHECK(!fixture::guards&&!lifecycle&&d==fixture::remote);CHECK(in.size()==4);++fixture::secure_writes;return fixture::result;
}
auto wki_secure_rdma_read(const WkiChannelIdentity&,const rdma_protocol::RegionDescriptor&d,uint64_t,std::span<uint8_t>out,std::span<const uint8_t>)->WkiSecureRdmaResult {
 CHECK(!fixture::guards&&!lifecycle&&std::memcmp(&d,&fixture::remote,sizeof(d))==0);CHECK(out.size()==4);++fixture::secure_reads;
 if(fixture::result.outcome==WkiSecureRdmaOutcome::EXACT)std::fill(out.begin(),out.end(),0x75);return fixture::result;
}
}
'''

AFTER = r'''
namespace ker::net::wki {
auto wki_send_on_channel_identity(const WkiChannelIdentity&i,MsgType type,const void*data,uint16_t size)->int {
 CHECK(!fixture::guards);auto*p=static_cast<const uint8_t*>(data);fixture::sent.emplace_back(type,std::vector<uint8_t>(p,p+size));
 if(!fixture::auto_reply)return 0;
 WkiHeader h{};h.src_node=i.peer_node_id;h.channel_id=i.channel_id;
 if(type==MsgType::ZONE_WRITE_REQ){++fixture::message_writes;auto const&r=*static_cast<const ZoneWriteReqPayload*>(data);ZoneWriteAckPayload a{r.zone_id,r.offset,r.length,0,r.op_cookie};RX_CALL(detail::handle_zone_write_ack(&h,reinterpret_cast<uint8_t*>(&a),sizeof(a),i));}
 if(type==MsgType::ZONE_READ_REQ){++fixture::message_reads;auto const&r=*static_cast<const ZoneReadReqPayload*>(data);std::vector<uint8_t>b(sizeof(ZoneReadRespPayload)+r.length,0x68);ZoneReadRespPayload a{r.zone_id,r.offset,r.length,r.op_cookie};std::memcpy(b.data(),&a,sizeof(a));RX_CALL(detail::handle_zone_read_resp(&h,b.data(),b.size(),i));}
 return 0;
}
}
using namespace ker::net::wki;
namespace {
unsigned cases=0;
auto identity()->WkiChannelIdentity{return {&channel,2,WKI_CHAN_ZONE_MGMT,channel.generation};}
void reset(){CHECK(fixture::pool.empty());CHECK(!fixture::guards&&!lifecycle);fixture::sent.clear();fixture::registration_hook={};fixture::onwait={};fixture::grant_hook={};fixture::hooks={};fixture::context=nullptr;
 fixture::onpark={};fixture::shutdown_requested=false;fixture::post_success=true;s_zone_stopping=false;s_zone_worker_stopped=true;s_zone_worker_starting=false;
 fixture::available=true;fixture::grant_live=true;fixture::capture_pressure=false;fixture::unregister_busy=false;fixture::registration_fail=false;fixture::auto_reply=true;fixture::result={0,4,WkiSecureRdmaOutcome::EXACT};
 fixture::registrations=fixture::unregistrations=fixture::secure_reads=fixture::secure_writes=fixture::message_reads=fixture::message_writes=0;
 channel.active=true;channel.generation=7;s_zone_create_worker_task=nullptr;s_zone_create_work_pending=false;}
auto setup(ZoneTypeHint hint=ZoneTypeHint::BUFFER)->WkiZone* {
 ZoneCreateReqPayload q{};q.zone_id=121;q.size=4096;q.access_policy=15;q.zone_type_hint=static_cast<uint8_t>(hint);
 WkiHeader h{};h.src_node=2;h.channel_id=WKI_CHAN_ZONE_MGMT;process_zone_create_request(h,q,identity());auto*z=acquire_zone_slot(121);CHECK(z&&z->state==ZoneState::ACTIVE);return z;
}
void drain(WkiZone*z){wki_zones_destroy_for_peer(2);release_zone(z);fixture::unregister_busy=false;process_zone_retirement_work();CHECK(fixture::pool.empty());CHECK(!fixture::guards);++cases;}
auto request(WkiZone*z,rdma_protocol::Opcode op=rdma_protocol::Opcode::WRITE)->WkiSecureRdmaRequest {
 WkiSecureRdmaRequest r{};r.grant=z->secure_descriptor;r.operation_id=0x10002;r.receive_ticket=0x20003;r.offset=7;r.length=4;r.peer_node=2;r.opcode=op;return r;
}
void private_setup(){reset();auto*z=setup();CHECK(z->secure_registered&&z->local_phys_addr==0&&!z->is_rdma&&!z->is_roce&&!z->rdma_transport);
 auto ack=fixture::sent.back();CHECK(ack.first==MsgType::ZONE_CREATE_ACK);ZoneCreateAckPayload a{};std::memcpy(&a,ack.second.data(),sizeof(a));CHECK(a.phys_addr==0&&a.rkey==91);
 auto ref=WkiZoneRef::acquire(121,2,3);CHECK(ref&&ref.live()&&ref.memory().data==nullptr);WkiZoneMemory out{.data=z,.size=39};CHECK(!ref.snapshot_memory(out)&&out.data==z&&out.size==39);ref.reset();drain(z);}
void excluded_hints(){for(auto hint:{ZoneTypeHint::MSG_QUEUE,ZoneTypeHint::LOCK,ZoneTypeHint::CUSTOM}){reset();auto*z=setup(hint);CHECK(!z->secure_registered&&fixture::registrations==0&&z->local_rkey==0);
 if(hint==ZoneTypeHint::MSG_QUEUE){std::array<uint8_t,4>out{};CHECK(wki_zone_read(121,0,out.data(),4)==WKI_ERR_ZONE_ACCESS);CHECK(wki_zone_write(121,0,out.data(),4)==WKI_ERR_ZONE_ACCESS);
  WkiHeader h{};h.src_node=2;h.channel_id=WKI_CHAN_ZONE_MGMT;ZoneReadReqPayload rd{121,0,4,18};auto count=fixture::sent.size();RX_CALL(detail::handle_zone_read_req(&h,reinterpret_cast<uint8_t*>(&rd),sizeof(rd),identity()));CHECK(fixture::sent.size()==count);
  std::array<uint8_t,20>msg{};ZoneWriteReqPayload wr{121,0,4,19};std::memcpy(msg.data(),&wr,16);std::fill(msg.begin()+16,msg.end(),0x66);RX_CALL(detail::handle_zone_write_req(&h,msg.data(),msg.size(),identity()));ZoneWriteAckPayload ack{};std::memcpy(&ack,fixture::sent.back().second.data(),sizeof(ack));CHECK(ack.status==WKI_ERR_ZONE_ACCESS&&ack.op_cookie==19&&static_cast<uint8_t*>(z->local_vaddr)[0]==0);
 }
 auto ref=WkiZoneRef::acquire(121,2,3);CHECK(ref&&ref.memory().data==z->local_vaddr);ref.reset();drain(z);}}
void arm_negatives(){reset();auto*z=setup();auto original=request(z);for(unsigned field=0;field<13;++field){auto r=original;switch(field){case 0:r.grant.capability[0]^=1;break;case 1:r.grant.session_id[0]^=1;break;case 2:++r.grant.binding_generation;break;case 3:++r.grant.generation;break;case 4:++r.grant.channel_id;break;case 5:r.peer_node=3;break;case 6:r.operation_id=0;break;case 7:r.receive_ticket=0;break;case 8:r.flags=1;break;case 9:r.offset=UINT64_MAX;break;case 10:r.offset=4095;r.length=2;break;case 11:r.opcode=rdma_protocol::Opcode::NOTIFY;break;case 12:r.native_intent[63]=1;break;}uint64_t gen=999;CHECK(zone_secure_arm(z,r,gen)<0&&gen==999&&!z->memory_reserved);++cases;}drain(z);}
void coverage_and_effect(){reset();auto*z=setup();auto r=request(z);uint64_t gen=0;auto before=z->refs.load();CHECK(zone_secure_arm(z,r,gen)==0&&gen==z->binding_generation&&z->refs.load()==before+1);CHECK(!reserve_zone_message_memory(z));
 std::array<uint8_t,4>in{1,2,3,4};auto*p=static_cast<uint8_t*>(z->local_vaddr);CHECK(std::all_of(p,p+4096,[](auto v){return v==0;}));
 auto bad=zone_secure_effect(z,r,{p,4096},{in.data(),3},{});CHECK(bad.outcome==WkiSecureRdmaOutcome::NO_EFFECT&&p[7]==0);
 auto wrong=r;wrong.operation_id+=0x10000;CHECK(zone_secure_effect(z,wrong,{p,4096},in,{}).outcome==WkiSecureRdmaOutcome::NO_EFFECT);
 CHECK(zone_secure_effect(z,r,{p,4095},in,{}).outcome==WkiSecureRdmaOutcome::NO_EFFECT);
 auto ok=zone_secure_effect(z,r,{p,4096},in,{});CHECK(ok.status==0&&ok.bytes==4&&ok.outcome==WkiSecureRdmaOutcome::EXACT&&std::memcmp(p+7,in.data(),4)==0);
 zone_secure_release(z,wrong,ok);CHECK(z->memory_reserved&&z->refs.load()==before+1);zone_secure_release(z,r,ok);zone_secure_release(z,r,ok);CHECK(!z->memory_reserved&&z->refs.load()==before);
 auto rd=request(z,rdma_protocol::Opcode::READ);CHECK(zone_secure_arm(z,rd,gen)==0);std::array<uint8_t,4>out{};CHECK(zone_secure_effect(z,rd,{p,4096},{},out).bytes==4&&out==in);zone_secure_release(z,rd,{0,4,WkiSecureRdmaOutcome::EXACT});drain(z);}
void overlap_and_busy(){reset();auto*z=setup();auto r=request(z);uint64_t gen=0;CHECK(reserve_zone_message_memory(z));CHECK(zone_secure_arm(z,r,gen)==WKI_ERR_BUSY);release_zone_message_memory(z);CHECK(zone_secure_arm(z,r,gen)==0);CHECK(zone_secure_arm(z,r,gen)==WKI_ERR_BUSY);
 WkiHeader h{};h.src_node=2;h.channel_id=WKI_CHAN_ZONE_MGMT;std::array<uint8_t,20>bytes{};ZoneWriteReqPayload wr{121,7,4,92};std::memcpy(bytes.data(),&wr,sizeof(wr));std::fill(bytes.begin()+16,bytes.end(),0x79);
 RX_CALL(detail::handle_zone_write_req(&h,bytes.data(),bytes.size(),identity()));ZoneWriteAckPayload ack{};std::memcpy(&ack,fixture::sent.back().second.data(),sizeof(ack));CHECK(ack.status==WKI_ERR_BUSY&&ack.op_cookie==92&&static_cast<uint8_t*>(z->local_vaddr)[7]==0);
 ZoneReadReqPayload rr{121,7,4,93};RX_CALL(detail::handle_zone_read_req(&h,reinterpret_cast<uint8_t*>(&rr),sizeof(rr),identity()));auto msg=fixture::sent.back();CHECK(msg.first==MsgType::ZONE_READ_RESP&&msg.second.size()==20);int32_t status=0;std::memcpy(&status,msg.second.data()+16,4);CHECK(status==WKI_ERR_BUSY);
 auto count=fixture::sent.size();fixture::available=false;z->secure_peer_compatible=false;RX_CALL(detail::handle_zone_read_req(&h,reinterpret_cast<uint8_t*>(&rr),sizeof(rr),identity()));CHECK(fixture::sent.size()==count);fixture::available=true;z->secure_peer_compatible=true;zone_secure_release(z,r,{WKI_ERR_BUSY,0,WkiSecureRdmaOutcome::NO_EFFECT});drain(z);}
void retirement(){reset();auto*z=setup();auto r=request(z);uint64_t gen=0;CHECK(zone_secure_arm(z,r,gen)==0);fixture::unregister_busy=true;wki_zones_destroy_for_peer(2);auto*p=z->local_vaddr;CHECK(z->retiring&&p&&z->secure_registered);CHECK(process_zone_retirement_work()&&z->local_vaddr==p&&z->secure_registered&&fixture::pool.contains(p));CHECK(zone_secure_arm(z,r,gen)==WKI_ERR_ZONE_INACTIVE);
 std::array<uint8_t,4>in{8,6,4,2};CHECK(zone_secure_effect(z,r,{static_cast<uint8_t*>(p),4096},in,{}).outcome==WkiSecureRdmaOutcome::EXACT);zone_secure_release(z,r,{0,4,WkiSecureRdmaOutcome::EXACT});release_zone(z);CHECK(fixture::pool.contains(p));fixture::unregister_busy=false;CHECK(!process_zone_retirement_work()&&fixture::pool.empty());++cases;}
void fallback(){for(auto outcome:{WkiSecureRdmaOutcome::EXACT,WkiSecureRdmaOutcome::UNCERTAIN,WkiSecureRdmaOutcome::NO_EFFECT}){reset();auto*z=setup();z->remote_rkey=33;fixture::remote=z->secure_descriptor;fixture::remote.rkey=33;fixture::result={outcome==WkiSecureRdmaOutcome::UNCERTAIN?WKI_ERR_TIMEOUT:0,4,outcome};std::array<uint8_t,4>in{};
 auto rc=wki_zone_write(121,0,in.data(),4);CHECK((outcome==WkiSecureRdmaOutcome::UNCERTAIN?rc==WKI_ERR_TIMEOUT:rc==0));CHECK(fixture::secure_writes==1&&fixture::message_writes==(outcome==WkiSecureRdmaOutcome::NO_EFFECT?1U:0U));
 rc=wki_zone_read(121,0,in.data(),4);CHECK((outcome==WkiSecureRdmaOutcome::UNCERTAIN?rc==WKI_ERR_TIMEOUT:rc==0));CHECK(fixture::secure_reads==1&&fixture::message_reads==(outcome==WkiSecureRdmaOutcome::NO_EFFECT?1U:0U));drain(z);}
 reset();auto*z=setup();z->remote_rkey=33;fixture::grant_live=false;std::array<uint8_t,4>in{};CHECK(wki_zone_write(121,0,in.data(),4)==0&&fixture::secure_writes==0&&fixture::message_writes==1);drain(z);}
void error_parser(){for(unsigned variant=0;variant<8;++variant){reset();if(variant==7)fixture::available=false;auto*z=setup();WkiWaitEntry wait{};std::array<uint8_t,4>dest{};z->read_wait_entry=&wait;z->read_pending=true;z->read_dest_buf=dest.data();z->read_expected_cookie=91;z->read_expected_offset=4;z->read_expected_len=4;
 std::array<uint8_t,21>buf{};ZoneReadRespPayload resp{121,4,0,91};int32_t status=WKI_ERR_BUSY;unsigned len=20;switch(variant){case 1:status=0;break;case 2:status=1;break;case 3:len=19;break;case 4:len=21;break;case 5:++resp.op_cookie;break;case 6:++resp.offset;break;case 7:fixture::available=false;break;}std::memcpy(buf.data(),&resp,16);std::memcpy(buf.data()+16,&status,4);WkiHeader h{};h.src_node=2;h.channel_id=WKI_CHAN_ZONE_MGMT;RX_CALL(detail::handle_zone_read_resp(&h,buf.data(),len,identity()));CHECK((dest==std::array<uint8_t,4>{}));CHECK((wait.state==WkiWaitEntry::DONE)==(variant==0));if(variant==0)CHECK(z->read_status==WKI_ERR_BUSY&&wait.result==0);else CHECK(z->read_pending);z->read_wait_entry=nullptr;z->read_pending=false;z->read_dest_buf=nullptr;fixture::available=true;drain(z);}}
void registration_retire_race(){reset();fixture::registration_hook=[] {wki_zones_destroy_for_peer(2);};ZoneCreateReqPayload q{};q.zone_id=121;q.size=4096;q.access_policy=15;WkiHeader h{};h.src_node=2;h.channel_id=WKI_CHAN_ZONE_MGMT;process_zone_create_request(h,q,identity());CHECK(fixture::registrations==1&&fixture::pool.size()==1);process_zone_retirement_work();CHECK(fixture::pool.empty()&&find_zone_slot(121)==nullptr);++cases;}
void reservation_races(){
 for(unsigned iteration=0;iteration<32;++iteration){reset();auto*z=setup();auto r=request(z);std::atomic<bool>go=false;int armed=99;bool message=false;uint64_t gen=0;
  std::thread first([&]{while(!go.load(std::memory_order_acquire))std::this_thread::yield();armed=zone_secure_arm(z,r,gen);});
  std::thread second([&]{while(!go.load(std::memory_order_acquire))std::this_thread::yield();message=reserve_zone_message_memory(z);});
  go.store(true,std::memory_order_release);first.join();second.join();CHECK((armed==0)!=message);CHECK(armed==0||armed==WKI_ERR_BUSY);
  if(armed==0)zone_secure_release(z,r,{0,0,WkiSecureRdmaOutcome::NO_EFFECT});else release_zone_message_memory(z);
  CHECK(!z->memory_reserved);drain(z);
 }
}
void failed_registration_and_late_channel(){
 reset();fixture::registration_fail=true;auto*z=setup();CHECK(!z->secure_registered&&z->local_rkey==0);auto ref=WkiZoneRef::acquire(121,2,3);CHECK(ref.memory().data==z->local_vaddr);ref.reset();drain(z);
 reset();fixture::registration_hook=[] {++channel.generation;};ZoneCreateReqPayload q{};q.zone_id=121;q.size=4096;q.access_policy=15;WkiHeader h{};h.src_node=2;h.channel_id=WKI_CHAN_ZONE_MGMT;process_zone_create_request(h,q,identity());CHECK(fixture::registrations==1);process_zone_retirement_work();CHECK(fixture::pool.empty()&&find_zone_slot(121)==nullptr);++cases;
}

void initiating_pin_and_ack(){reset();fixture::onwait=[] {
 WkiHeader h{};h.src_node=2;h.channel_id=WKI_CHAN_ZONE_MGMT;ZoneCreateAckPayload ack{};ack.zone_id=121;ack.rkey=33;ack.phys_addr=UINT64_MAX;process_zone_create_ack(h,ack,identity());
};WkiZoneRef ref;CHECK(wki_zone_create(2,121,4096,15,ZoneNotifyMode::NONE,ZoneTypeHint::BUFFER,&ref)==0);CHECK(ref&&ref.live()&&!ref.memory().data);auto*z=acquire_zone_slot(121);CHECK(z&&z->secure_registered&&z->remote_phys_addr==0&&z->remote_rkey==33);
 CHECK(std::any_of(fixture::sent.begin(),fixture::sent.end(),[](const auto&pair){if(pair.first!=MsgType::ZONE_NOTIFY_POST)return false;ZoneNotifyPayload p{};std::memcpy(&p,pair.second.data(),sizeof(p));return p.op_type==0xfe&&p.offset==91&&p.length==0;}));ref.reset();drain(z);}
void task_only_free(){reset();fixture::available=false;auto*z=setup();ker::mod::sched::task::Task worker;CHECK(worker.try_acquire_lifetime_ref());s_zone_create_worker_task=&worker;s_zone_worker_stopped=false;auto*p=z->local_vaddr;unsigned wakes=fixture::wakes;wki_zones_destroy_for_peer(2);release_zone(z);CHECK(fixture::pool.contains(p)&&fixture::wakes>wakes);CHECK(!process_zone_retirement_work()&&fixture::pool.empty());s_zone_create_worker_task=nullptr;s_zone_worker_stopped=true;worker.release();CHECK(worker.refs==1);++cases;}

void compatible_hint_is_not_authority(){reset();auto*z=setup();CHECK(z->secure_peer_compatible);fixture::available=false;fixture::grant_live=false;
 WkiHeader h{};h.src_node=2;h.channel_id=WKI_CHAN_ZONE_MGMT;ZoneNotifyPayload hint{};hint.zone_id=121;hint.offset=33;hint.op_type=0xfe;RX_CALL(detail::handle_zone_notify_post(&h,reinterpret_cast<uint8_t*>(&hint),sizeof(hint),identity()));CHECK(z->remote_rkey==33);
 std::array<uint8_t,4>data{};CHECK(wki_zone_write(121,0,data.data(),4)==0&&fixture::secure_writes==0&&fixture::message_writes==1);CHECK(wki_zone_read(121,0,data.data(),4)==0&&fixture::secure_reads==0&&fixture::message_reads==1);drain(z);}

void exact_ref_transfer(){reset();auto*z=setup();z->remote_rkey=33;fixture::remote=z->secure_descriptor;fixture::remote.rkey=33;WkiZoneRef ref=WkiZoneRef::acquire(121,2,ZONE_ACCESS_LOCAL_READ|ZONE_ACCESS_LOCAL_WRITE);CHECK(ref);std::array<uint8_t,4>bytes{};
 CHECK(ref.write(0,bytes.data(),bytes.size())==0&&ref.read(0,bytes.data(),bytes.size())==0);CHECK(fixture::secure_writes==1&&fixture::secure_reads==1);ref.reset();drain(z);
 reset();z=setup();WkiZoneRef old=WkiZoneRef::acquire(121,2,ZONE_ACCESS_LOCAL_READ|ZONE_ACCESS_LOCAL_WRITE);CHECK(old);s_zone_table_lock.lock();mark_zone_retiring_locked(z);s_zone_table_lock.unlock();auto*fresh=setup();CHECK(fresh!=z);fresh->remote_rkey=33;fixture::remote=fresh->secure_descriptor;fixture::remote.rkey=33;
 CHECK(old.read(0,bytes.data(),bytes.size())==WKI_ERR_ZONE_NOT_FOUND);CHECK(old.write(0,bytes.data(),bytes.size())==WKI_ERR_ZONE_NOT_FOUND);CHECK(old.destroy()==WKI_ERR_ZONE_NOT_FOUND);CHECK(fixture::secure_reads==0&&fixture::secure_writes==0&&fixture::message_reads==0&&fixture::message_writes==0);
 WkiZoneRef current=WkiZoneRef::acquire(121,2,ZONE_ACCESS_LOCAL_READ|ZONE_ACCESS_LOCAL_WRITE);CHECK(current&&current.live());CHECK(current.read(0,bytes.data(),bytes.size())==0&&current.write(0,bytes.data(),bytes.size())==0);CHECK(wki_zone_read(121,0,bytes.data(),bytes.size())==0&&wki_zone_write(121,0,bytes.data(),bytes.size())==0);CHECK(fixture::secure_reads==2&&fixture::secure_writes==2);
 old.reset();release_zone(z);current.reset();drain(fresh);WkiZoneRef empty;CHECK(empty.read(0,bytes.data(),4)==WKI_ERR_ZONE_NOT_FOUND&&empty.write(0,bytes.data(),4)==WKI_ERR_ZONE_NOT_FOUND);++cases;}

void secure_readiness(){WkiZoneRef empty;CHECK(!empty.secure_ready());++cases;
 for(unsigned v=0;v<8;++v){reset();auto*z=setup();z->remote_rkey=33;fixture::remote=z->secure_descriptor;fixture::remote.rkey=33;auto ref=WkiZoneRef::acquire(121,2,3);CHECK(ref&&ref.secure_ready());
  if(v==0)fixture::grant_live=false;if(v==1)fixture::capture_pressure=true;if(v==2)fixture::remote.permissions=rdma_protocol::PERMIT_READ;if(v==3)--fixture::remote.size;if(v==4)++channel.generation;if(v==5)fixture::available=false;if(v==6)fixture::grant_hook=[&]{s_zone_table_lock.lock();mark_zone_retiring_locked(z);s_zone_table_lock.unlock();};if(v==7){s_zone_table_lock.lock();mark_zone_retiring_locked(z);s_zone_table_lock.unlock();auto*fresh=setup();fresh->remote_rkey=33;fixture::remote=fresh->secure_descriptor;fixture::remote.rkey=33;auto current=WkiZoneRef::acquire(121,2,3);CHECK(current.secure_ready()&&!ref.secure_ready());current.reset();release_zone(fresh);}
  CHECK(!ref.secure_ready());CHECK(fixture::secure_reads==0&&fixture::secure_writes==0&&fixture::message_reads==0&&fixture::message_writes==0);ref.reset();fixture::grant_hook={};fixture::capture_pressure=false;drain(z);
 }
}

void generation_and_channel(){reset();auto*z=setup();auto gen=z->binding_generation;auto r=request(z);++channel.generation;uint64_t out=0;CHECK(zone_secure_arm(z,r,out)==WKI_ERR_ZONE_INACTIVE);drain(z);reset();z=setup();CHECK(z->binding_generation>gen);drain(z);}
void shutdown_handoff(){
 reset();ker::mod::sched::task::Task worker;CHECK(worker.try_acquire_lifetime_ref());s_zone_create_worker_task=&worker;s_zone_worker_stopped=false;fixture::onpark=[]{CHECK(fixture::shutdown_holds==1);fixture::shutdown_requested=true;};try{zone_create_worker_thread();CHECK(false);}catch(ker::mod::sched::CleanExit const&){}CHECK(worker.refs==1&&!s_zone_create_worker_task&&s_zone_worker_stopped&&s_zone_stopping&&!fixture::shutdown_holds);CHECK(wki_zone_shutdown_drain());++cases;
 reset();CHECK(worker.try_acquire_lifetime_ref());s_zone_create_worker_task=&worker;s_zone_worker_stopped=false;ZoneCreateWork work{};work.channel_identity=identity();work.header.src_node=2;work.header.channel_id=WKI_CHAN_ZONE_MGMT;work.request.zone_id=121;work.request.size=4096;work.request.access_policy=15;CHECK(enqueue_zone_create_work(work));fixture::registration_hook=[]{CHECK(fixture::shutdown_holds==1);fixture::shutdown_requested=true;};try{zone_create_worker_thread();CHECK(false);}catch(ker::mod::sched::CleanExit const&){}auto*z=acquire_zone_slot(121);CHECK(z&&z->secure_registered&&worker.refs==1&&s_zone_worker_stopped&&!s_zone_create_worker_task);auto*p=z->local_vaddr;auto wakes=fixture::wakes;wki_zones_destroy_for_peer(2);CHECK(fixture::wakes==wakes&&z->retiring&&fixture::pool.contains(p));fixture::unregister_busy=true;CHECK(!wki_zone_shutdown_drain()&&z->secure_registered&&fixture::pool.contains(p));fixture::unregister_busy=false;CHECK(!wki_zone_shutdown_drain()&&fixture::pool.contains(p));release_zone(z);CHECK(wki_zone_shutdown_drain()&&fixture::pool.empty()&&worker.refs==1);CHECK(!enqueue_zone_create_work(work));WkiZoneRef denied;CHECK(wki_zone_create(2,122,4096,15,ZoneNotifyMode::NONE,ZoneTypeHint::BUFFER,&denied)==-ESHUTDOWN&&!denied);++cases;
 reset();ker::mod::sched::task::Task failed;ker::mod::sched::task::Task::next=&failed;fixture::post_success=false;wki_zone_worker_start();CHECK(failed.destroyed&&failed.refs==0&&!s_zone_create_worker_task&&s_zone_worker_stopped&&!s_zone_worker_starting&&fixture::shutdown_holds==0);CHECK(wki_zone_shutdown_drain());++cases;
}
}
int main(){wki_zone_init();private_setup();excluded_hints();arm_negatives();coverage_and_effect();overlap_and_busy();retirement();fallback();error_parser();registration_retire_race();reservation_races();failed_registration_and_late_channel();initiating_pin_and_ack();task_only_free();compatible_hint_is_not_authority();exact_ref_transfer();secure_readiness();generation_and_channel();shutdown_handoff();CHECK(fixture::allocations==fixture::frees);std::printf("secure zone actual-source %u bounded cases PASS; allocations=%u frees=%u\n",cases,fixture::allocations,fixture::frees);}
'''


BOUNDARY = r'''
namespace ker::net::wki {
auto wki_send_on_channel_identity(const WkiChannelIdentity&,MsgType,const void*,uint16_t)->int {CHECK(!fixture::guards);return 0;}
}
using namespace ker::net::wki;
int main(){wki_zone_init();WkiTransport provider;provider.rdma_capable=true;peer.transport=&provider;
 WkiChannelIdentity id{&channel,2,WKI_CHAN_ZONE_MGMT,7};WkiHeader h{};h.src_node=2;h.channel_id=WKI_CHAN_ZONE_MGMT;
 ZoneCreateReqPayload request{};request.zone_id=121;request.size=4096;request.access_policy=15;process_zone_create_request(h,request,id);
 auto*z=acquire_zone_slot(121);CHECK(z);
 if(BOUNDARY_EXPECT_RAW){CHECK(fixture::raw_allocations==1&&z->local_vaddr==fixture::raw_pool.data());}
 else {CHECK(fixture::raw_allocations==0&&fixture::pool.contains(z->local_vaddr));}
 wki_zones_destroy_for_peer(2);release_zone(z);
 // Current close/reclaim is asynchronous; this control only qualifies backing
 // selection, not core crypto or eventual worker cleanup.
 std::printf("actual zone private/raw boundary expected=%s PASS\n",BOUNDARY_EXPECT_RAW?"historical-raw":"private");
}
'''


def make_tu(source, boundary=None):
    wki = WKI.read_text()
    declarations = declaration(wki, 'WkiChannelIdentity') + '\n' + declaration(wki, 'WkiWaitEntry')
    # WkiChannelIdentity refers to the forward declaration only.
    declarations = 'struct WkiChannel;\n' + declarations
    for name in ['WKI_OK','WKI_ERR_NO_MEM','WKI_ERR_TIMEOUT','WKI_ERR_INVALID','WKI_ERR_NOT_FOUND','WKI_ERR_BUSY','WKI_OP_TIMEOUT_US']:
        declarations += '\n' + re.search(r'constexpr[^\n]*\b'+name+r'\b[^\n]*;', wki)[0]
    header = re.sub(r'^#(?:include|pragma)[^\n]*\n', '', HEADER.read_text(), flags=re.M)
    cpp = re.sub(r'^#include[^\n]*\n', '', source, flags=re.M)
    # Descriptor packed declaration has no comparison operator.
    prelude = PRELUDE.replace('/* actual core declarations */', declarations).replace('d==fixture::remote','std::memcmp(&d,&fixture::remote,sizeof(d))==0')
    return prelude + header + '\n' + cpp + '\n' + (AFTER if boundary is None else BOUNDARY.replace('BOUNDARY_EXPECT_RAW', 'true' if boundary == 'raw' else 'false'))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--source',type=Path,default=ZONE)
    parser.add_argument('--keep',type=Path)
    parser.add_argument('--boundary',choices=['private','raw'])
    args=parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='wki-secure-zone-') as tmp:
        path=args.keep or Path(tmp)
        path.mkdir(parents=True,exist_ok=True)
        tu=path/'zone-tu.cpp';tu.write_text(make_tu(args.source.read_text(),args.boundary))
        binary=path/'zone-test'
        command=['/usr/bin/clang++','-std=c++23','-Wall','-Wextra','-g','-O1','-fsanitize=address,undefined','-fno-omit-frame-pointer','-fno-pie','-no-pie','-pthread','-I'+str(KERN),str(tu),'-o',str(binary)]
        compile_result=subprocess.run(command,capture_output=True,text=True)
        (path/'compile.log').write_text(compile_result.stdout+compile_result.stderr)
        print(compile_result.stdout+compile_result.stderr,end='')
        if compile_result.returncode: return compile_result.returncode
        environment=os.environ|{'ASAN_OPTIONS':'detect_leaks=0','UBSAN_OPTIONS':'halt_on_error=1'}
        result=subprocess.run([str(binary)],capture_output=True,text=True,env=environment)
        (path/'run.log').write_text(result.stdout+result.stderr)
        print(result.stdout+result.stderr,end='')
        (path/'result.json').write_text(json.dumps({'compile_exit':compile_result.returncode,'run_exit':result.returncode,'source':str(args.source),'source_sha256':hashlib.sha256(args.source.read_bytes()).hexdigest(),'fixture_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'command':command},indent=2)+'\n')
        return result.returncode

if __name__=='__main__':raise SystemExit(main())
