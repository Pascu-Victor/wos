#!/usr/bin/env python3
"""Compile the complete secure BLOCK TU and actual consumer/owner bodies.

Native device, authenticated core, binding registry and task scheduling are
instrumented boundaries. This qualifies consumer admission and ownership,
not the common engine, real DMA, crypto, peer fencing or a VM workload.
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
SOURCE = KERN / 'net/wki/secure_block.cpp'


def body(text, marker):
    start = text.index(marker)
    pos = text.index('{', start)
    end = pos + 1
    depth = 1
    while depth:
        depth += (text[end] == '{') - (text[end] == '}')
        end += 1
    return text[start:end]


def function(text, name):
    match = re.search(r'\b(?:auto|void)\s+' + name + r'\(', text)
    if not match:
        raise ValueError('missing actual body: ' + name)
    return body(text, text[match.start():text.index('{', match.start())])


BOUNDARY = r'''
#pragma once
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <span>
#include <utility>
#include <vector>
#include <net/wki/secure_block.hpp>
#include <net/wki/auth_crypto.hpp>
#define CHECK(x) do {if(!(x)){std::fprintf(stderr,"CHECK %d: %s\n",__LINE__,#x);std::abort();}}while(0)
namespace fixture {
unsigned guards=0,reads=0,writes=0,flushes=0,cases=0,registrations=0,unregistrations=0,yields=0,messages=0,cancels=0,core_calls=0;
int native_status=0,register_status=0,local_current=0,unregister_busy=0,message_status=0,send_status=0,wait_status=0,prepare_status=0;
bool available=true,exchange=true,retain=true,grant_live=true,late_prepare_identity=false,backing_freed_wiped=false,array_failure=false,actual_message_io=false;
ker::net::wki::rdma_protocol::Transport kind=ker::net::wki::rdma_protocol::Transport::ROCE;
void* registered_backing=nullptr;size_t registered_size=0;
std::function<void()> on_native,on_yield;
std::map<uint32_t,void*> live;
std::vector<uint8_t> reply;
std::vector<ker::net::wki::WkiSecureRdmaResult> results;
std::vector<uint64_t> lbas,sequences;
std::vector<uint32_t> counts;
std::array<uint8_t,4096> media{};
}
void* operator new[](size_t n){if(void*p=std::malloc(n))return p;throw std::bad_alloc();}
void* operator new[](size_t n,const std::nothrow_t&) noexcept {if(fixture::array_failure&&n==ker::net::wki::WKI_SECURE_BLOCK_BYTES)return nullptr;return std::malloc(n);}
void operator delete[](void*p) noexcept {
 if(p&&p==fixture::registered_backing){auto*b=static_cast<uint8_t*>(p);for(size_t i=0;i<fixture::registered_size;++i)CHECK(b[i]==0);
 fixture::backing_freed_wiped=true;fixture::registered_backing=nullptr;fixture::registered_size=0;}
 std::free(p);
}
void operator delete[](void*p,size_t) noexcept {::operator delete[](p);}
void operator delete[](void*p,const std::nothrow_t&) noexcept {::operator delete[](p);}
namespace ker::mod::sys {
struct Spinlock {bool held=false;void lock(){CHECK(!held);held=true;++fixture::guards;}bool try_lock(){if(held)return false;lock();return true;}
 void unlock(){CHECK(held&&fixture::guards);held=false;--fixture::guards;}uint64_t lock_irqsave(){lock();return 0;}void unlock_irqrestore(uint64_t){unlock();}};
}
namespace ker::mod::sched {void kern_yield(){CHECK(!fixture::guards);++fixture::yields;if(fixture::on_yield)fixture::on_yield();}}
namespace ker::mod::dbg {template<class...T>void log(const char*,T...) {}}
namespace ker::dev {
struct BlockDevice {uint64_t block_size=512,total_blocks=16384;bool readonly=false;void*private_data=nullptr;};
bool block_device_is_read_only(const BlockDevice*d){return d->readonly;}
int block_read(BlockDevice*d,uint64_t lba,size_t count,void*out){CHECK(!fixture::guards&&d&&out);++fixture::reads;
fixture::lbas.push_back(lba);fixture::counts.push_back(count);if(fixture::on_native)fixture::on_native();
std::memset(out,0x5a,count*d->block_size);return fixture::native_status;}
int block_write(BlockDevice*d,uint64_t lba,size_t count,const void*input){CHECK(!fixture::guards&&d&&input);++fixture::writes;
fixture::lbas.push_back(lba);fixture::counts.push_back(count);std::memcpy(fixture::media.data(),input,std::min<size_t>(fixture::media.size(),count*d->block_size));
if(fixture::on_native)fixture::on_native();return fixture::native_status;}
int block_flush(BlockDevice*d){CHECK(!fixture::guards&&d);++fixture::flushes;return fixture::native_status;}
}
namespace ker::net::wki {
struct WkiChannel {};
/* ACTUAL_IDENTITY */
struct WriterLease {bool live=true;bool active()const{return live;}};
struct DevServerBinding {bool active=true;std::atomic<bool>retiring{false};bool epoch_reset_pending=false,detach_cleanup_pending=false;
ResourceType resource_type=ResourceType::BLOCK;ker::dev::BlockDevice*block_dev=nullptr;uint8_t attach_cookie=7;uint32_t resource_id=9;
ResourceIncarnationToken resource_incarnation{8,6};WkiChannelIdentity channel_identity{};bool block_secure_disabled=false;
bool block_read_allowed=true,block_read_only=false;WriterLease block_writer_lease{};std::atomic<bool>block_native_busy{false};
BlockSecureContext*block_secure_context=nullptr;unsigned refs=0;};
ker::mod::sys::Spinlock s_server_lock;DevServerBinding* owner=nullptr;
bool channel_identity_matches(const WkiChannelIdentity&a,const WkiChannelIdentity&b){return a.channel==b.channel&&a.peer_node_id==b.peer_node_id&&a.channel_id==b.channel_id&&a.generation==b.generation&&a.generation!=0;}
bool channel_identity_equal(const WkiChannelIdentity&a,const WkiChannelIdentity&b){return channel_identity_matches(a,b);}
DevServerBinding*find_binding_by_channel_identity(const WkiChannelIdentity&id){return owner&&channel_identity_matches(id,owner->channel_identity)?owner:nullptr;}
bool retain_binding_locked(DevServerBinding*b){CHECK(fixture::guards);if(!b->active||b->retiring||!fixture::retain)return false;++b->refs;return true;}
void release_binding(DevServerBinding*b){CHECK(!fixture::guards);if(b){CHECK(b->refs);--b->refs;}}
BlockSecureContext*wki_dev_server_block_secure_context(DevServerBinding*);
bool wki_dev_server_exchange_block_secure_context(DevServerBinding*,BlockSecureContext*,BlockSecureContext*);
DevServerBinding*wki_dev_server_retain_block_secure_binding(const WkiChannelIdentity&,const BlockSecureContext*);
void wki_dev_server_release_block_secure_binding(DevServerBinding*);
namespace auth_crypto {void secure_erase(void*p,size_t n){if(!p)return;volatile uint8_t*b=static_cast<volatile uint8_t*>(p);while(n--)*b++=0;}}
bool wki_secure_rdma_available(const WkiChannelIdentity&,rdma_protocol::Service s){CHECK(!fixture::guards&&s==rdma_protocol::Service::BLOCK);return fixture::available;}
int wki_secure_rdma_register_region(const WkiChannelIdentity&id,rdma_protocol::Service service,void*backing,uint32_t size,uint64_t generation,uint8_t permissions,
const WkiSecureRdmaHooks&hooks,void*,rdma_protocol::RegionDescriptor&out){CHECK(!fixture::guards&&backing&&hooks.arm&&hooks.effect&&hooks.release);
++fixture::registrations;if(fixture::register_status)return fixture::register_status;if(!permissions)return -EACCES;
out={};out.rkey=fixture::registrations;out.size=size;out.binding_generation=generation;out.generation=12;out.channel_id=id.channel_id;
out.service=static_cast<uint8_t>(service);out.permissions=permissions;out.capability.fill(0x42);fixture::live.emplace(out.rkey,backing);
fixture::registered_backing=backing;fixture::registered_size=size;return 0;}
int wki_secure_rdma_unregister_region(const rdma_protocol::RegionDescriptor&g){CHECK(!fixture::guards);++fixture::unregistrations;
if(fixture::unregister_busy){--fixture::unregister_busy;return -EBUSY;}CHECK(fixture::live.erase(g.rkey)==1);return 0;}
int wki_secure_rdma_local_region_current(const WkiChannelIdentity&,rdma_protocol::Service,const rdma_protocol::RegionDescriptor&){CHECK(!fixture::guards);return fixture::local_current;}
}
'''


CLIENT_BOUNDARY = r'''
namespace ker::net::wki {
/* ACTUAL_INFO_OPCODE */
/* ACTUAL_WAIT_CONSTANTS */
struct WkiZoneRef {bool retained=false;};
struct WkiTransport {};
struct ProxyBlockState {bool rdma_disabled=false,secure_block_compatible=false,secure_block_write_uncertain=false;
uint8_t binding_attach_cookie=7;uint32_t resource_id=9;ResourceIncarnationToken binding_incarnation{8,6};
WkiChannelIdentity identity{};rdma_protocol::RegionDescriptor secure_block_grant{};uint64_t secure_block_sequence=0;
uint64_t secure_block_attempts=0,secure_block_completions=0,secure_block_bytes=0,secure_block_fallbacks=0;
rdma_protocol::Transport secure_block_transport=rdma_protocol::Transport::NONE;
WkiZoneRef rdma_zone_ref{};bool rdma_attached=false,rdma_roce=false;void*rdma_zone_ptr=nullptr;
uint32_t rdma_zone_id=0,rdma_zone_size=0,rdma_remote_rkey=0;WkiTransport*rdma_transport=nullptr;};
struct WkiWaitEntry {void*output=nullptr;uint16_t capacity=0;};
WkiChannelIdentity block_channel_identity_snapshot(const ProxyBlockState*p){return p->identity;}
int prepare_block_op_wait(ProxyBlockState*p,uint16_t op,WkiWaitEntry&w,void*out,uint16_t cap,WkiChannelIdentity*id){CHECK(!fixture::guards&&op==OP_BLOCK_SECURE_PREPARE);
w={out,cap};*id=p->identity;if(fixture::late_prepare_identity)++id->generation;return fixture::prepare_status;}
void cancel_op_waiter(ProxyBlockState*,WkiWaitEntry&,int){++fixture::cancels;}
WkiWaitEntry*current_wait=nullptr;
int wki_wait_for_op(WkiWaitEntry*w,uint64_t){current_wait=w;return fixture::wait_status;}
void consume_block_op_result(ProxyBlockState*,WkiWaitEntry&w,int*status,uint16_t*len){*status=fixture::message_status;
*len=static_cast<uint16_t>(fixture::reply.size()-sizeof(DevOpRespPayload));if(!*status){CHECK(*len<=w.capacity);std::memcpy(w.output,fixture::reply.data()+sizeof(DevOpRespPayload),*len);}}
bool wki_secure_rdma_remote_grant(const WkiChannelIdentity&,rdma_protocol::Service s,uint32_t key,rdma_protocol::RegionDescriptor&out,uint8_t){
CHECK(!fixture::guards&&s==rdma_protocol::Service::BLOCK);if(!fixture::grant_live||!owner->block_secure_context||owner->block_secure_context->grant.rkey!=key)return false;
out=owner->block_secure_context->grant;return true;}
rdma_protocol::Transport wki_secure_rdma_transport(const WkiChannelIdentity&,rdma_protocol::Service service) {
CHECK(!fixture::guards&&service==rdma_protocol::Service::BLOCK);return fixture::kind;}
WkiSecureRdmaResult client_result(std::span<const uint8_t>bytes,uint32_t size){CHECK(bytes.size()==64&&!fixture::guards);BlockSecureIntent i{};
std::memcpy(&i,bytes.data(),64);CHECK(i.sequence&&i.count*i.block_size==size&&i.attach_cookie==7&&i.resource_id==9);
++fixture::core_calls;fixture::sequences.push_back(i.sequence);CHECK(!fixture::results.empty());auto r=fixture::results.front();fixture::results.erase(fixture::results.begin());return r;}
WkiSecureRdmaResult wki_secure_rdma_write(const WkiChannelIdentity&,const rdma_protocol::RegionDescriptor&,uint64_t offset,std::span<const uint8_t>input,uint16_t flags,std::span<const uint8_t>intent){CHECK(offset==0&&flags==0);return client_result(intent,input.size());}
WkiSecureRdmaResult wki_secure_rdma_read(const WkiChannelIdentity&,const rdma_protocol::RegionDescriptor&,uint64_t offset,std::span<uint8_t>out,std::span<const uint8_t>intent){CHECK(offset==0);auto r=client_result(intent,out.size());if(r.outcome==WkiSecureRdmaOutcome::EXACT&&!r.status&&r.bytes<=out.size())std::memset(out.data(),0x5a,r.bytes);return r;}
int actual_ordinary_write(uint64_t,uint32_t,const void*);
int remote_block_write_msg(ProxyBlockState*,ker::dev::BlockDevice*,uint64_t lba,uint32_t count,const void*input){++fixture::messages;
if(fixture::actual_message_io)return actual_ordinary_write(lba,count,input);fixture::lbas.push_back(lba);fixture::counts.push_back(count);return fixture::message_status;}
int remote_block_read_msg(ProxyBlockState*,ker::dev::BlockDevice*d,uint64_t lba,uint32_t count,void*out){++fixture::messages;fixture::lbas.push_back(lba);fixture::counts.push_back(count);if(!fixture::message_status)std::memset(out,0x5a,count*d->block_size);return fixture::message_status;}
struct DeferredVfsOp {WkiHeader hdr{};DevServerBinding*retained_binding=nullptr;WkiChannelIdentity channel_identity{};uint16_t op_id=0,req_data_len=0;const uint8_t*req_data=nullptr;};
uint16_t req_cookie_from_header(const WkiHeader*){return 3;}
void send_block_status_response(const WkiChannelIdentity&,uint16_t op,int status,uint16_t cookie){DevOpRespPayload h{.op_id=op,.status=static_cast<int16_t>(status),.data_len=0,.reserved=cookie};fixture::reply.assign(reinterpret_cast<uint8_t*>(&h),reinterpret_cast<uint8_t*>(&h)+sizeof(h));fixture::message_status=status;}
void run_deferred_block_op(DeferredVfsOp*);
int wki_send_on_channel_identity(const WkiChannelIdentity&id,MsgType type,const void*data,uint16_t size){CHECK(!fixture::guards);if(fixture::send_status)return fixture::send_status;
if(type==MsgType::DEV_OP_RESP){fixture::reply.assign(static_cast<const uint8_t*>(data),static_cast<const uint8_t*>(data)+size);return 0;}
CHECK(type==MsgType::DEV_OP_REQ);DevOpReqPayload h{};std::memcpy(&h,data,sizeof(h));CHECK(size==sizeof(h)+h.data_len);
DeferredVfsOp op{.retained_binding=owner,.channel_identity=id,.op_id=h.op_id,.req_data_len=h.data_len,.req_data=static_cast<const uint8_t*>(data)+sizeof(h)};
run_deferred_block_op(&op);return 0;}
}
'''


CASES = r'''
namespace ker::net::wki {
ker::dev::BlockDevice device;DevServerBinding binding;WkiChannel channel;
void reset(){CHECK(!fixture::guards&&fixture::live.empty()&&!binding.refs);fixture::reads=fixture::writes=fixture::flushes=fixture::messages=fixture::cancels=fixture::core_calls=0;
fixture::registrations=fixture::unregistrations=fixture::yields=0;fixture::native_status=fixture::register_status=fixture::local_current=fixture::unregister_busy=fixture::message_status=fixture::send_status=fixture::wait_status=fixture::prepare_status=0;
fixture::available=fixture::exchange=fixture::retain=fixture::grant_live=true;fixture::late_prepare_identity=fixture::backing_freed_wiped=fixture::array_failure=fixture::actual_message_io=false;fixture::media.fill(0);
fixture::kind=rdma_protocol::Transport::ROCE;
fixture::on_native=fixture::on_yield={};fixture::results.clear();fixture::lbas.clear();fixture::counts.clear();fixture::sequences.clear();fixture::reply.clear();
device={};binding.active=true;binding.retiring=false;binding.epoch_reset_pending=binding.detach_cleanup_pending=false;binding.resource_type=ResourceType::BLOCK;
binding.block_dev=&device;binding.attach_cookie=7;binding.resource_id=9;binding.resource_incarnation={8,6};binding.block_secure_disabled=binding.block_read_only=false;
binding.block_read_allowed=binding.block_writer_lease.live=true;binding.block_native_busy=false;binding.block_secure_context=nullptr;
binding.channel_identity={.channel=&channel,.peer_node_id=2,.channel_id=17,.generation=3};owner=&binding;}
BlockSecurePrepare prepare(){return {.attach_cookie=7,.resource_id=9,.incarnation={8,6}};}
BlockSecureContext*setup(){rdma_protocol::RegionDescriptor g{};CHECK(wki_secure_block_prepare(&binding,prepare(),g)==0);CHECK(binding.block_secure_context&&g.rkey);
CHECK(g.permissions==(rdma_protocol::PERMIT_READ|rdma_protocol::PERMIT_WRITE));return binding.block_secure_context;}
void cleanup(BlockSecureContext*c){binding.block_secure_context=nullptr;wki_secure_block_release_context(c);CHECK(fixture::live.empty()&&!binding.refs&&!binding.block_native_busy);}
WkiSecureRdmaRequest request(BlockSecureContext*c,bool write=true,uint64_t seq=1){BlockSecureIntent i{.opcode=static_cast<uint8_t>(write?rdma_protocol::Opcode::WRITE:rdma_protocol::Opcode::READ),.attach_cookie=7,.resource_id=9,.incarnation={8,6},.binding_generation=c->grant.binding_generation,.lba=4,.count=8,.block_size=512,.sequence=seq};
WkiSecureRdmaRequest r{.grant=c->grant,.operation_id=22,.receive_ticket=33,.length=4096,.peer_node=2,.opcode=write?rdma_protocol::Opcode::WRITE:rdma_protocol::Opcode::READ,.intent_length=64};std::memcpy(r.native_intent.data(),&i,64);return r;}
WkiSecureRdmaResult effect(BlockSecureContext*c,const WkiSecureRdmaRequest&r,size_t in=4096,size_t out=4096){static std::array<uint8_t,4096>input{};static std::array<uint8_t,4096>output{};input.fill(0x17);output.fill(0xa5);
return block_effect(c,r,{c->backing,WKI_SECURE_BLOCK_BYTES},r.opcode==rdma_protocol::Opcode::WRITE?std::span<const uint8_t>(input.data(),in):std::span<const uint8_t>{},r.opcode==rdma_protocol::Opcode::READ?std::span<uint8_t>(output.data(),out):std::span<uint8_t>{});}
void native_cases(){for(bool write:{false,true})for(int status:{0,-EIO,7}){reset();auto*c=setup();auto r=request(c,write);uint64_t g=0;
CHECK(block_arm(c,r,g)==0&&binding.refs==1&&binding.block_native_busy&&g==c->grant.binding_generation);fixture::native_status=status;
auto result=effect(c,r);CHECK(result.outcome==(status?WkiSecureRdmaOutcome::UNCERTAIN:WkiSecureRdmaOutcome::EXACT));CHECK(result.bytes==(status?0U:4096U));
CHECK((write?fixture::writes:fixture::reads)==1);CHECK(effect(c,r).status==-EALREADY&&(write?fixture::writes:fixture::reads)==1);
block_release(c,r,result);CHECK(!binding.refs&&!binding.block_native_busy);for(unsigned n=0;n<4096;++n)CHECK(!c->backing[n]);std::memset(c->backing,0xa5,WKI_SECURE_BLOCK_BYTES);cleanup(c);CHECK(fixture::backing_freed_wiped);++fixture::cases;}}
void denial_cases(){for(unsigned n=0;n<21;++n){reset();auto*c=setup();auto r=request(c);BlockSecureIntent i{};std::memcpy(&i,r.native_intent.data(),64);
switch(n){case 0:i.version++;break;case 1:i.reserved[15]=1;break;case 2:i.attach_cookie++;break;case 3:i.resource_id++;break;case 4:i.incarnation.owner_boot_epoch++;break;
case 5:i.incarnation.resource_incarnation++;break;case 6:i.binding_generation++;break;case 7:i.sequence=0;break;case 8:i.block_size++;break;case 9:i.count=0;break;
case 10:i.lba=UINT64_MAX;break;case 11:r.offset=1;break;case 12:r.flags=1;break;case 13:r.operation_id=0;break;case 14:r.receive_ticket=0;break;
case 15:r.peer_node++;break;case 16:r.grant.capability[0]^=1;break;case 17:r.intent_length--;break;case 18:r.length++;break;case 19:binding.block_read_only=true;break;case 20:binding.block_writer_lease.live=false;break;}
std::memcpy(r.native_intent.data(),&i,64);uint64_t gen=88;CHECK(block_arm(c,r,gen)<0&&gen==88&&!binding.refs&&!binding.block_native_busy&&!fixture::writes);cleanup(c);++fixture::cases;}}
void ownership(){reset();auto*c=setup();auto r=request(c);uint64_t gen=0;binding.block_native_busy=true;CHECK(block_arm(c,r,gen)==-EBUSY&&!c->loan&&!binding.refs);binding.block_native_busy=false;
CHECK(block_arm(c,r,gen)==0);CHECK(effect(c,r,2048).outcome==WkiSecureRdmaOutcome::NO_EFFECT&&!fixture::writes);CHECK(!c->effect_started);
auto wrong=r;wrong.receive_ticket++;block_release(c,wrong,{});CHECK(binding.refs==1&&c->loan);CHECK(effect(c,r).outcome==WkiSecureRdmaOutcome::EXACT);block_release(c,r,{});
CHECK(block_arm(c,r,gen)==-ESTALE&&!binding.refs);auto next=request(c,true,2);CHECK(block_arm(c,next,gen)==0);wki_secure_block_close_context(c);
CHECK(effect(c,next).outcome==WkiSecureRdmaOutcome::NO_EFFECT&&fixture::writes==1);block_release(c,next,{});cleanup(c);++fixture::cases;
reset();c=setup();r=request(c);CHECK(block_arm(c,r,gen)==0);fixture::unregister_busy=1;fixture::on_yield=[&]{CHECK(c->closed&&binding.refs==1&&binding.block_native_busy&&c->backing);block_release(c,r,{});};cleanup(c);CHECK(fixture::yields==1&&fixture::backing_freed_wiped);++fixture::cases;
reset();c=setup();r=request(c,false);binding.block_read_allowed=false;CHECK(block_arm(c,r,gen)==-EACCES&&!binding.refs);cleanup(c);++fixture::cases;
reset();c=setup();r=request(c);device.readonly=true;CHECK(block_arm(c,r,gen)==-EROFS&&!binding.refs);cleanup(c);++fixture::cases;}
void prepare_cases(){for(unsigned n=0;n<8;++n){reset();rdma_protocol::RegionDescriptor out{};out.rkey=99;auto p=prepare();
switch(n){case 0:p.reserved=1;break;case 1:p.attach_cookie++;break;case 2:binding.block_secure_disabled=true;break;case 3:fixture::available=false;break;case 4:fixture::register_status=-ENOMEM;break;case 5:binding.retiring=true;break;case 6:binding.epoch_reset_pending=true;break;case 7:fixture::array_failure=true;break;}
CHECK(wki_secure_block_prepare(&binding,p,out)!=0&&out.rkey==99&&!binding.block_secure_context&&fixture::live.empty());++fixture::cases;}
reset();auto*c=setup();rdma_protocol::RegionDescriptor out{};fixture::local_current=-EAGAIN;CHECK(wki_secure_block_prepare(&binding,prepare(),out)==-EAGAIN&&binding.block_secure_context==c&&!c->closed);
fixture::local_current=-ESTALE;fixture::unregister_busy=1;CHECK(wki_secure_block_prepare(&binding,prepare(),out)==-EBUSY&&binding.block_secure_context==c&&c->closed);
auto old_generation=c->grant.binding_generation;CHECK(wki_secure_block_prepare(&binding,prepare(),out)==0&&binding.block_secure_context&&out.binding_generation!=old_generation);
cleanup(binding.block_secure_context);++fixture::cases;}
void ordinary_reservation(){reset();auto*c=setup();auto r=request(c);uint64_t gen=0;CHECK(block_arm(c,r,gen)==0);DeferredVfsOp op{.retained_binding=&binding,.channel_identity=binding.channel_identity,.op_id=OP_BLOCK_FLUSH};
run_deferred_block_op(&op);CHECK(fixture::message_status==-EBUSY&&!fixture::flushes&&binding.block_native_busy);block_release(c,r,{});fixture::message_status=0;
run_deferred_block_op(&op);CHECK(fixture::flushes==1&&!binding.block_native_busy);cleanup(c);++fixture::cases;}
int actual_ordinary_write(uint64_t lba,uint32_t count,const void*input){std::vector<uint8_t>bytes(12+count*device.block_size);
std::memcpy(bytes.data(),&lba,8);std::memcpy(bytes.data()+8,&count,4);std::memcpy(bytes.data()+12,input,bytes.size()-12);
DeferredVfsOp op{.retained_binding=&binding,.channel_identity=binding.channel_identity,.op_id=OP_BLOCK_WRITE,.req_data_len=static_cast<uint16_t>(bytes.size()),.req_data=bytes.data()};run_deferred_block_op(&op);
CHECK(fixture::reply.size()==sizeof(DevOpRespPayload));DevOpRespPayload h{};std::memcpy(&h,fixture::reply.data(),sizeof(h));return h.status;}
void ordinary_error_uncertainty(){for(int error:{-EINVAL,-EAGAIN,-EBUSY,-EROFS,-EACCES}){reset();auto*c=setup();ProxyBlockState p;p.identity=binding.channel_identity;p.secure_block_grant=c->grant;
std::array<uint8_t,4096>data{};data.fill(0x3c);fixture::results={{-EAGAIN,0,WkiSecureRdmaOutcome::NO_EFFECT}};fixture::actual_message_io=true;fixture::native_status=error;
CHECK(remote_block_write_secure_or_msg(&p,&device,4,8,data.data())==error&&fixture::writes==1&&fixture::media[0]==0x3c&&p.secure_block_write_uncertain);
CHECK(remote_block_write_secure_or_msg(&p,&device,4,8,data.data())==-EIO&&fixture::writes==1&&fixture::messages==1);cleanup(c);++fixture::cases;}}
void read_publication(){for(int error:{0,-EIO}){reset();auto*c=setup();auto r=request(c,false);uint64_t generation=0;CHECK(block_arm(c,r,generation)==0);
std::array<uint8_t,4100>out{};out.fill(0xa5);fixture::native_status=error;auto result=block_effect(c,r,{c->backing,WKI_SECURE_BLOCK_BYTES},{},{out.data(),4096});
CHECK(result.outcome==(error?WkiSecureRdmaOutcome::UNCERTAIN:WkiSecureRdmaOutcome::EXACT));
for(size_t n=0;n<out.size();++n)CHECK(out[n]==(!error&&n<4096?0x5a:0xa5));CHECK(c->backing[0]==0x5a);
block_release(c,r,result);cleanup(c);++fixture::cases;}
reset();auto*c=setup();auto r=request(c);uint64_t generation=0;CHECK(block_arm(c,r,generation)==0);binding.block_read_only=true;
CHECK(effect(c,r).outcome==WkiSecureRdmaOutcome::NO_EFFECT&&!fixture::writes&&!c->effect_started);block_release(c,r,{});cleanup(c);++fixture::cases;}
void client_cases(){for(unsigned n=0;n<7;++n){reset();auto*c=setup();ProxyBlockState p;p.identity=binding.channel_identity;p.secure_block_grant=c->grant;
std::array<uint8_t,4096>data{};if(n==0)fixture::results={{0,4096,WkiSecureRdmaOutcome::EXACT}};
if(n==1)fixture::results={{-EIO,0,WkiSecureRdmaOutcome::UNCERTAIN}};
if(n==2)fixture::results={{-EAGAIN,0,WkiSecureRdmaOutcome::NO_EFFECT}};
if(n==3)fixture::results={{-EACCES,0,WkiSecureRdmaOutcome::NO_EFFECT}};
if(n==4)fixture::results={{0,2048,WkiSecureRdmaOutcome::EXACT}};
if(n==5){fixture::available=false;}
if(n==6){p.secure_block_sequence=UINT64_MAX;}
int status=remote_block_write_secure_or_msg(&p,&device,4,8,data.data());
if(n==0)CHECK(!status&&!fixture::messages&&p.secure_block_sequence==1&&p.secure_block_attempts==1&&p.secure_block_completions==1&&p.secure_block_bytes==4096&&p.secure_block_transport==rdma_protocol::Transport::ROCE);
if(n==2||n==5||n==6)CHECK(!status&&fixture::messages==1&&p.secure_block_fallbacks==1&&!p.secure_block_completions&&!p.secure_block_bytes);
if(n!=0)CHECK(p.secure_block_transport==rdma_protocol::Transport::NONE);
CHECK(p.secure_block_attempts==static_cast<unsigned>(n!=5&&n!=6));
if(n==1||n==4){CHECK(status==-EIO&&p.secure_block_write_uncertain&&!fixture::messages);unsigned calls=fixture::core_calls;CHECK(remote_block_write_secure_or_msg(&p,&device,4,8,data.data())==-EIO&&fixture::core_calls==calls);}
if(n==3)CHECK(status==-EACCES&&!fixture::messages&&!p.secure_block_write_uncertain);
cleanup(c);++fixture::cases;}
reset();auto*c=setup();ProxyBlockState p;p.identity=binding.channel_identity;p.secure_block_grant=c->grant;std::vector<uint8_t>data(WKI_SECURE_BLOCK_BYTES+4096);
fixture::results={{0,WKI_SECURE_BLOCK_BYTES,WkiSecureRdmaOutcome::EXACT},{-EAGAIN,0,WkiSecureRdmaOutcome::NO_EFFECT}};
CHECK(remote_block_write_secure_or_msg(&p,&device,4,4104,data.data())==0&&fixture::messages==1&&fixture::lbas.back()==4100&&fixture::counts.back()==8);
CHECK(fixture::sequences==std::vector<uint64_t>({1,2})&&p.secure_block_attempts==2&&p.secure_block_completions==1&&p.secure_block_bytes==WKI_SECURE_BLOCK_BYTES&&p.secure_block_fallbacks==1);cleanup(c);++fixture::cases;
reset();c=setup();p={};p.identity=binding.channel_identity;p.secure_block_grant=c->grant;fixture::results={{0,4096,WkiSecureRdmaOutcome::EXACT}};std::array<uint8_t,4096>out{};
CHECK(remote_block_read_secure_or_msg(&p,&device,4,8,out.data())==0&&out.front()==0x5a&&out.back()==0x5a&&!fixture::messages&&p.secure_block_attempts==1&&p.secure_block_completions==1&&p.secure_block_bytes==4096);cleanup(c);++fixture::cases;}
void diagnostic_counters(){for(auto kind:{rdma_protocol::Transport::ROCE,rdma_protocol::Transport::IVSHMEM}){reset();auto*c=setup();ProxyBlockState p;p.identity=binding.channel_identity;p.secure_block_grant=c->grant;
fixture::kind=kind;fixture::results={{0,4096,WkiSecureRdmaOutcome::EXACT}};std::array<uint8_t,4096>data{};
CHECK(remote_block_write_secure_or_msg(&p,&device,4,8,data.data())==0&&p.secure_block_transport==kind&&p.secure_block_completions==1&&p.secure_block_bytes==4096);
p.secure_block_attempts=p.secure_block_completions=p.secure_block_bytes=UINT64_MAX;fixture::results={{0,4096,WkiSecureRdmaOutcome::EXACT}};
CHECK(remote_block_read_secure_or_msg(&p,&device,4,8,data.data())==0&&p.secure_block_attempts==UINT64_MAX&&p.secure_block_completions==UINT64_MAX&&p.secure_block_bytes==UINT64_MAX);
p.secure_block_fallbacks=UINT64_MAX;fixture::results={{-EAGAIN,0,WkiSecureRdmaOutcome::NO_EFFECT}};
CHECK(remote_block_read_secure_or_msg(&p,&device,4,8,data.data())==0&&p.secure_block_fallbacks==UINT64_MAX&&p.secure_block_transport==kind);cleanup(c);++fixture::cases;}
reset();ProxyBlockState p;p.secure_block_grant.rkey=9;p.secure_block_grant.capability.fill(0xa5);p.secure_block_write_uncertain=true;
p.secure_block_attempts=3;p.secure_block_completions=2;p.secure_block_bytes=8192;p.secure_block_fallbacks=1;p.secure_block_transport=rdma_protocol::Transport::IVSHMEM;
p.rdma_zone_ref.retained=true;p.rdma_attached=p.rdma_roce=true;p.rdma_zone_ptr=&p;p.rdma_zone_id=7;p.rdma_zone_size=4096;p.rdma_remote_rkey=8;WkiTransport transport;p.rdma_transport=&transport;
auto ref=take_proxy_zone_locked(&p);CHECK(ref.retained&&!p.rdma_attached&&!p.rdma_roce&&!p.rdma_zone_ptr&&!p.rdma_zone_id&&!p.rdma_zone_size&&!p.rdma_remote_rkey&&!p.rdma_transport);
auto*grant_bytes=reinterpret_cast<const uint8_t*>(&p.secure_block_grant);for(size_t n=0;n<sizeof(p.secure_block_grant);++n)CHECK(grant_bytes[n]==0);
CHECK(p.secure_block_write_uncertain);
CHECK(p.secure_block_attempts==3&&p.secure_block_completions==2&&p.secure_block_bytes==8192&&p.secure_block_fallbacks==1&&p.secure_block_transport==rdma_protocol::Transport::IVSHMEM);++fixture::cases;}
void descriptor_prepare(){for(unsigned n=0;n<4;++n){reset();auto*c=setup();ProxyBlockState p;p.identity=binding.channel_identity;rdma_protocol::RegionDescriptor out{};out.rkey=99;
if(n==0){CHECK(prepare_secure_block_grant(&p,p.identity,out)==0&&out.rkey==c->grant.rkey&&p.secure_block_grant.rkey==c->grant.rkey);}
if(n==1){fixture::late_prepare_identity=true;CHECK(prepare_secure_block_grant(&p,p.identity,out)==-ENOTCONN&&fixture::cancels==1&&!p.secure_block_grant.rkey&&out.rkey==99);}
if(n==2){fixture::grant_live=false;CHECK(prepare_secure_block_grant(&p,p.identity,out)==-EAGAIN&&!p.secure_block_grant.rkey&&out.rkey==99);}
if(n==3){fixture::send_status=-1;CHECK(prepare_secure_block_grant(&p,p.identity,out)==-ENOTCONN&&fixture::cancels==1&&!p.secure_block_grant.rkey);}
cleanup(c);++fixture::cases;}}
}
int main(){using namespace ker::net::wki;native_cases();denial_cases();ownership();prepare_cases();ordinary_reservation();ordinary_error_uncertainty();read_publication();client_cases();diagnostic_counters();descriptor_prepare();CHECK(fixture::live.empty()&&!fixture::guards&&!binding.refs);std::printf("secure BLOCK actual-source %u cases PASS\n",fixture::cases);}
'''


def build_source(source, private, mutation):
    original = source
    replacements = {
        'once': ('if (!context->effect_started.compare_exchange_strong(expected, true, std::memory_order_acq_rel))', 'if (false)'),
        'coverage': ('input.size() != request.length', 'false'),
        'cookie': ('intent.attach_cookie != binding.attach_cookie', 'false'),
        'sequence': ('intent.sequence <= context->last_sequence', 'false'),
        'wipe': ('auth_crypto::secure_erase(context->backing, request.length);', ''),
        'pressure': ('if (CURRENT != -ESTALE) return CURRENT;', ''),
    }
    if mutation in replacements:
        before, after = replacements[mutation]
        assert source.count(before) == 1, (mutation, source.count(before))
        source = source.replace(before, after, 1)
        assert source != original
    identity = body((KERN / 'net/wki/wki.hpp').read_text(), 'struct WkiChannelIdentity {') + ';'
    for filename, text in [('boundary.hpp', BOUNDARY.replace('/* ACTUAL_IDENTITY */', identity)),
                           ('dev/block_device.hpp', '#pragma once\n#include <boundary.hpp>\n'),
                           ('net/wki/dev_server.hpp', '#pragma once\n#include <boundary.hpp>\n'),
                           ('platform/sched/scheduler.hpp', '#pragma once\n#include <boundary.hpp>\n')]:
        p = private / filename
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    server = (KERN / 'net/wki/dev_server.cpp').read_text()
    registry = '\n'.join(function(server, name) for name in ['wki_dev_server_block_secure_context', 'wki_dev_server_exchange_block_secure_context',
                             'wki_dev_server_retain_block_secure_binding', 'wki_dev_server_release_block_secure_binding'])
    client = (KERN / 'net/wki/dev_proxy.cpp').read_text()
    if mutation == 'fallback':
        before = 'if (FALLBACK && state->secure_block_compatible) state->secure_block_write_uncertain = true;'
        assert client.count(before) == 1
        client = client.replace(before, 'if (FALLBACK && state->secure_block_compatible && STATUS != -EINVAL && STATUS != -EAGAIN) state->secure_block_write_uncertain = true;', 1)
    if mutation == 'diagnostics':
        before = 'if (state->secure_block_completions != UINT64_MAX) ++state->secure_block_completions;'
        assert client.count(before) == 1
        client = client.replace(before, 'static_cast<void>(state->secure_block_completions);', 1)
    if mutation == 'grant-wipe':
        before = 'auth_crypto::secure_erase(&state->secure_block_grant, sizeof(state->secure_block_grant));'
        assert client.count(before) == 1
        client = client.replace(before, '', 1)
    if mutation == 'ordinary':
        before = 'if (!BINDING->block_native_busy.compare_exchange_strong(expected, true, std::memory_order_acq_rel))'
        assert server.count(before) == 1
        server = server.replace(before, 'if (false)', 1)
    selected = '\n'.join(function(client, name) for name in ['secure_block_fallback_allowed', 'prepare_secure_block_grant',
                         'secure_block_transfer', 'secure_block_range_valid', 'remote_block_read_secure_or_msg', 'remote_block_write_secure_or_msg', 'take_proxy_zone_locked'])
    actual = private / 'actual-secure-block.cpp'
    actual.write_text(source)
    info = re.search(r'constexpr uint16_t OP_BLOCK_INFO[^;]*;', (KERN / 'net/wki/remotable.hpp').read_text()).group()
    wki = (KERN / 'net/wki/wki.hpp').read_text()
    constants = '\n'.join(re.search(r'constexpr int ' + name + r'[^;]*;', wki).group() for name in ['WKI_OK', 'WKI_ERR_NOT_FOUND'])
    constants += '\n' + re.search(r'constexpr uint64_t WKI_DEV_PROXY_TIMEOUT_US[^;]*;', (KERN / 'net/wki/dev_proxy.hpp').read_text()).group()
    prelude = CLIENT_BOUNDARY.replace('/* ACTUAL_INFO_OPCODE */', info).replace('/* ACTUAL_WAIT_CONSTANTS */', constants)
    return '#include <boundary.hpp>\n#include "actual-secure-block.cpp"\nnamespace ker::net::wki {\n' + registry + '\n}\n' + prelude + '\nnamespace ker::net::wki {\n' + function(server, 'run_deferred_block_op') + '\n' + selected + '\n}\n' + CASES


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, default=SOURCE)
    parser.add_argument('--keep', type=Path)
    parser.add_argument('--mutation', choices=['once', 'coverage', 'cookie', 'sequence', 'wipe', 'pressure', 'fallback', 'ordinary', 'diagnostics', 'grant-wipe'])
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='wki-secure-block-') as temporary:
        path = args.keep or Path(temporary)
        path.mkdir(parents=True, exist_ok=True)
        (path / 'test.cpp').write_text(build_source(args.source.read_text(), path, args.mutation))
        command = ['/usr/bin/clang++', '-std=c++23', '-Wall', '-Wextra', '-O1', '-g', '-fsanitize=address,undefined',
                   '-fno-omit-frame-pointer', '-fno-pie', '-no-pie', '-MMD', '-MF', str(path / 'inputs.d'),
                   '-I' + str(path), '-I' + str(KERN), '-I' + str(KERN / 'net/wki'),
                   str(path / 'test.cpp'), '-o', str(path / 'block-test')]
        compiled = subprocess.run(command, capture_output=True, text=True)
        (path / 'compile.log').write_text(compiled.stdout + compiled.stderr)
        print(compiled.stdout + compiled.stderr, end='')
        if compiled.returncode:
            return compiled.returncode
        run = subprocess.run([str(path / 'block-test')], capture_output=True, text=True,
                             env=os.environ | {'ASAN_OPTIONS': 'detect_leaks=0', 'UBSAN_OPTIONS': 'halt_on_error=1'})
        (path / 'run.log').write_text(run.stdout + run.stderr)
        print(run.stdout + run.stderr, end='')
        deps = (path / 'inputs.d').read_text().replace('\\\n', '').split(':', 1)[1].split()
        inputs = {str(Path(p).resolve()): hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in deps}
        (path / 'result.json').write_text(json.dumps({'compile_exit': compiled.returncode, 'run_exit': run.returncode,
            'mutation': args.mutation, 'command': command, 'input_sha256': inputs,
            'source_sha256': hashlib.sha256(args.source.read_bytes()).hexdigest(),
            'fixture_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}, indent=2) + '\n')
        return run.returncode


if __name__ == '__main__':
    raise SystemExit(main())
