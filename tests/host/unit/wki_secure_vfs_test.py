#!/usr/bin/env python3
"""Compile verbatim VFS native hooks/cleanup/client flush with bounded seams.

The packed native intent and secure protocol types are actual headers. File,
binding, registry and synchronous core boundaries are instrumented; this does
not qualify crypto, core task abandonment, hardware, or the whole remote TU.
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
SOURCE = KERN / 'net/wki/remote_vfs.cpp'


def block(text, marker):
    start = text.index(marker)
    pos = text.index('{', start)
    depth = 1
    end = pos + 1
    while depth:
        depth += (text[end] == '{') - (text[end] == '}')
        end += 1
    return text[start:end]


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <span>
#include <set>
#include <utility>
#include <vector>
#include <net/wki/wire.hpp>
#include <net/wki/secure_vfs.hpp>
#include <net/wki/auth_crypto.hpp>
#define CHECK(x) do { if (!(x)) {std::fprintf(stderr,"CHECK %d: %s\n",__LINE__,#x);std::abort();}} while(0)
namespace fixture {
unsigned guards=0,reads=0,writes=0,puts=0,yields=0,secure_calls=0,messages=0,cases=0;
unsigned registrations=0,unregistrations=0;
std::set<uint32_t> live_regions;
std::vector<std::pair<void*,size_t>> erased;
int native_bytes=4,unregister_busy=0,register_fail=0,current_region=0;
bool available=true,grant_live=true,slot=true,admission=true,install=true,message_timeout=false,bad_reply=false;
std::vector<uint8_t> reply;
std::function<void()> onio,onyield;
std::vector<ker::net::wki::WkiSecureRdmaResult> results;
std::vector<int64_t> offsets;
std::vector<unsigned> lengths;
}
namespace ker::mod::sys {
struct Spinlock {bool held=false;void lock(){CHECK(!held);held=true;++fixture::guards;}
void unlock(){CHECK(held&&fixture::guards);held=false;--fixture::guards;}
auto lock_irqsave()->uint64_t {lock();return 0;}void unlock_irqrestore(uint64_t){unlock();}};
}
namespace ker::mod::sched {void kern_yield(){CHECK(!fixture::guards);++fixture::yields;if(fixture::onyield)fixture::onyield();}}
namespace ker::vfs {
enum class FSType {TMPFS,XFS,REMOTE};
struct FileOperations {void(*vfs_read)()=nullptr;void(*vfs_write)()=nullptr;};
struct File {int refs=1;int open_flags=2;bool is_directory=false,readonly=false;void* exec_read_view=nullptr;
 FSType fs_type=FSType::XFS;FileOperations* fops=nullptr;};
void vfs_retain_file(File*f){CHECK(f&&f->refs);++f->refs;}
void vfs_put_file(File*f){CHECK(!fixture::guards&&f&&f->refs);--f->refs;++fixture::puts;}
auto file_mount_read_only(const File&f)->bool {return f.readonly;}
auto vfs_pwrite_file(File*f,const void*,size_t len,int64_t)->ssize_t {
 CHECK(!fixture::guards&&f&&f->refs);++fixture::writes;if(fixture::onio)fixture::onio();
 return fixture::native_bytes<0?fixture::native_bytes:std::min<size_t>(fixture::native_bytes,len);}
auto vfs_pread_file(File*f,void*out,size_t len,int64_t)->ssize_t {
 CHECK(!fixture::guards&&f&&f->refs);++fixture::reads;if(fixture::onio)fixture::onio();
 if(fixture::native_bytes<0)return fixture::native_bytes;
 auto n=std::min<size_t>(fixture::native_bytes,len);std::memset(out,0x5a,n);return n;}
}
namespace ker::mod::security {auto authorize_file_description_access(unsigned mode,bool write)->int {
return mode==3||(write?mode==0:mode==1)?-EBADF:0;}}
namespace ker::net::wki {
struct WkiChannel {int tag=0;};
/* actual identity */
struct DevServerBinding {bool active=true;std::atomic<bool> retiring{false};ResourceType resource_type=ResourceType::VFS;
 uint32_t consumer_boot_epoch=3,resource_id=9;uint16_t consumer_node=2;uint64_t vfs_export_publication_revision=10,vfs_open_binding_id=77;
 ResourceIncarnationToken resource_incarnation{8,6};WkiChannelIdentity channel_identity{};bool compatible=false;unsigned refs=0;VfsSecureContext* secure=nullptr;};
struct RemoteVfsFd {ker::vfs::File*file=nullptr;uint64_t ordinary_open_token=55,ordinary_open_binding_id=77;};
ker::mod::sys::Spinlock s_vfs_lock;RemoteVfsFd row;DevServerBinding* owner=nullptr;
constexpr uint32_t VFS_CACHE_SIZE=8192;
constexpr uint32_t VFS_RDMA_WRITE_SIZE=4*1024*1024,VFS_RDMA_BULK_SIZE=2*1024*1024;
auto vfs_channel_identity_matches(const WkiChannelIdentity&a,const WkiChannelIdentity&b)->bool {
return a.channel==b.channel&&a.peer_node_id==b.peer_node_id&&a.channel_id==b.channel_id&&a.generation==b.generation&&a.generation!=0;}
auto find_remote_fd(const WkiChannelIdentity&,int fd)->RemoteVfsFd* {return fd==12&&row.file?&row:nullptr;}
auto wki_dev_server_vfs_rx_admission_try_acquire()->bool {return fixture::admission;}
void wki_dev_server_vfs_rx_admission_release(){}
auto wki_dev_server_retain_vfs_secure_binding(const WkiChannelIdentity&id,const VfsSecureContext*c)->DevServerBinding* {
if(!owner||owner->retiring||owner->secure!=c||!vfs_channel_identity_matches(id,owner->channel_identity))return nullptr;
++owner->refs;return owner;}
void wki_dev_server_release_vfs_secure_binding(DevServerBinding*b){CHECK(!fixture::guards);if(b){CHECK(b->refs);--b->refs;}}
auto wki_dev_server_has_vfs_secure_context(const DevServerBinding*b)->bool {return b->secure!=nullptr;}
auto wki_dev_server_borrow_vfs_secure_context(const DevServerBinding*b)->VfsSecureContext* {return b->secure;}
void wki_remote_vfs_close_secure_context(VfsSecureContext*);
auto wki_dev_server_take_vfs_secure_context(DevServerBinding*b,VfsSecureContext*c)->bool {
if(b->secure!=c)return false;wki_remote_vfs_close_secure_context(c);b->secure=nullptr;return true;}
auto wki_dev_server_vfs_secure_compatible(DevServerBinding*b,bool publish=false)->bool {if(publish)b->compatible=true;return b->compatible;}
auto wki_dev_server_install_vfs_secure_context(DevServerBinding*b,VfsSecureContext*c)->bool {
CHECK(!fixture::guards);if(!fixture::install||b->secure||b->retiring)return false;b->secure=c;return true;}
auto wki_secure_rdma_available(const WkiChannelIdentity&,rdma_protocol::Service)->bool {CHECK(!fixture::guards);return fixture::available;}
auto wki_secure_rdma_register_region(const WkiChannelIdentity&id,rdma_protocol::Service,void* backing,uint32_t size,uint64_t generation,
 uint8_t permissions,const WkiSecureRdmaHooks&,void*,rdma_protocol::RegionDescriptor&out)->int {
CHECK(!fixture::guards);std::memset(backing,0xa5,size);++fixture::registrations;if(static_cast<int>(fixture::registrations)==fixture::register_fail)return -ENOMEM;
out={};out.rkey=fixture::registrations;out.size=size;out.binding_generation=generation;out.generation=44;out.channel_id=id.channel_id;
out.service=static_cast<uint8_t>(rdma_protocol::Service::VFS);out.permissions=permissions;out.capability.fill(0x11);fixture::live_regions.insert(out.rkey);return 0;}
auto wki_secure_rdma_unregister_region(const rdma_protocol::RegionDescriptor&grant)->int {
CHECK(!fixture::guards);++fixture::unregistrations;if(fixture::unregister_busy){--fixture::unregister_busy;return -EBUSY;}
CHECK(fixture::live_regions.erase(grant.rkey)==1);return 0;}
auto wki_secure_rdma_local_region_current(const WkiChannelIdentity&,rdma_protocol::Service,const rdma_protocol::RegionDescriptor&)->int {
CHECK(!fixture::guards);return fixture::current_region;}
namespace auth_crypto {
void secure_erase(void* data,size_t length) {
CHECK(!fixture::guards&&fixture::live_regions.empty()&&(!owner||owner->refs==0));
CHECK(data&&(length==VFS_RDMA_WRITE_SIZE||length==VFS_RDMA_BULK_SIZE));
auto*bytes=static_cast<volatile uint8_t*>(data);for(size_t n=0;n<length;++n)bytes[n]=0;
for(size_t n=0;n<length;++n)CHECK(bytes[n]==0);
fixture::erased.emplace_back(data,length);
}
}
struct ProxyVfsState {bool active=true,epoch_reset_pending=false,resources_releasing=false,resources_released=false;
 WkiChannelIdentity identity{};};
struct WriteBehindBuffer {int64_t pending_offset=10;uint32_t pending_len=4,capacity=4;std::unique_ptr<uint8_t[]> data;};
struct RemoteFileContext {ProxyVfsState*proxy=nullptr;int32_t remote_fd=12;uint16_t ordinary_cancel_slot=0;
 uint64_t ordinary_open_token=55,ordinary_open_binding_id=77;uint32_t ordinary_owner_boot_epoch=8,ordinary_resource_incarnation=6,ordinary_resource_id=9;
 int secure_write_error=0;bool secure_io_negotiated=false;std::unique_ptr<uint8_t[]>secure_read_scratch;uint32_t secure_read_capacity=0;WriteBehindBuffer*write_buf=nullptr;};
enum class ExecCancellationPhase {FREE,OWNED};
struct Record {ExecCancellationPhase phase=ExecCancellationPhase::OWNED;ProxyVfsState*proxy=nullptr;
 VfsOpenLifetimeRequest request{.token=55,.binding_id=77,.owner_boot_epoch=8};WkiChannelIdentity identity{};};
std::array<Record,1> g_open_cancellations;
auto proxy_channel_identity_locked(const ProxyVfsState*p)->WkiChannelIdentity{return p->identity;}
struct SharedIoSlotGuard {SharedIoSlotGuard(ProxyVfsState*,uint64_t){}auto acquired()const->bool{return fixture::slot;}};
auto wki_now_us()->uint64_t{return 1;}
void remote_vfs_invalidate_cached_stat(RemoteFileContext*){}
auto normalize_proxy_status_for_errno(int s)->int{return s;}
constexpr uint64_t VFS_PROXY_OP_TIMEOUT_US=1000;
auto wki_secure_rdma_remote_grant(const WkiChannelIdentity&,rdma_protocol::Service,uint32_t key,
 rdma_protocol::RegionDescriptor&out,uint8_t permissions)->bool {
 CHECK(!fixture::guards&&key==0);if(!fixture::grant_live)return false;out={};out.size=permissions==rdma_protocol::PERMIT_READ?VFS_RDMA_BULK_SIZE:VFS_RDMA_WRITE_SIZE;
 out.binding_generation=77;out.permissions=permissions;return true;}
auto take_result(int64_t offset,unsigned length)->WkiSecureRdmaResult {
CHECK(!fixture::guards);++fixture::secure_calls;fixture::offsets.push_back(offset);fixture::lengths.push_back(length);
CHECK(!fixture::results.empty());auto r=fixture::results.front();fixture::results.erase(fixture::results.begin());return r;}
auto wki_secure_rdma_write(const WkiChannelIdentity&,const rdma_protocol::RegionDescriptor&,uint64_t offset,std::span<const uint8_t>input,
uint16_t flags,std::span<const uint8_t>intent)->WkiSecureRdmaResult {
CHECK(offset==0&&flags==0&&intent.size()==64);VfsSecureIoIntent i{};std::memcpy(&i,intent.data(),64);CHECK(i.length==input.size()&&i.open_token==55);
return take_result(i.file_offset,i.length);}
auto wki_secure_rdma_read(const WkiChannelIdentity&,const rdma_protocol::RegionDescriptor&,uint64_t offset,std::span<uint8_t>out,
std::span<const uint8_t>intent)->WkiSecureRdmaResult {
CHECK(offset==0&&intent.size()==64);VfsSecureIoIntent i{};std::memcpy(&i,intent.data(),64);auto r=take_result(i.file_offset,i.length);
if(r.outcome==WkiSecureRdmaOutcome::EXACT&&r.status==0)std::memset(out.data(),0x5a,std::min<size_t>(r.bytes,out.size()));return r;}
void actual_message(const uint8_t*,uint16_t);
auto vfs_proxy_send_and_wait(ProxyVfsState*p,uint16_t op,const uint8_t*prefix,size_t prefixlen,void*out,uint16_t cap,uint16_t*outlen,uint64_t,void*,
const uint8_t*tail,size_t len,bool,const WkiChannelIdentity*required)->int {
CHECK(!fixture::guards);if(required&&!vfs_channel_identity_matches(p->identity,*required))return -ENOTCONN;
++fixture::messages;if(fixture::message_timeout)return -ETIMEDOUT;
if(op==OP_VFS_WRITE){*static_cast<uint32_t*>(out)=len;return 0;}
CHECK(op==OP_VFS_SECURE_IO);std::vector<uint8_t>packet(prefix,prefix+prefixlen);if(tail)packet.insert(packet.end(),tail,tail+len);
fixture::reply.clear();actual_message(packet.data(),packet.size());CHECK(fixture::reply.size()>=sizeof(DevOpRespPayload));
DevOpRespPayload h{};std::memcpy(&h,fixture::reply.data(),sizeof(h));if(h.status)return h.status;
CHECK(h.data_len<=cap&&fixture::reply.size()==sizeof(h)+h.data_len);std::memcpy(out,fixture::reply.data()+sizeof(h),h.data_len);
if(fixture::bad_reply)static_cast<uint8_t*>(out)[15]=1;if(outlen)*outlen=h.data_len;return 0;}
'''

CASES = r'''
void dummy(){}
ker::vfs::FileOperations fops{dummy,dummy};WkiChannel channel;DevServerBinding binding;ker::vfs::File file;
void reset(){CHECK(!fixture::guards&&fixture::live_regions.empty());fixture::reads=fixture::writes=fixture::puts=fixture::yields=fixture::secure_calls=fixture::messages=0;
fixture::registrations=fixture::unregistrations=0;fixture::native_bytes=4;fixture::unregister_busy=fixture::register_fail=fixture::current_region=0;
fixture::available=fixture::grant_live=fixture::slot=fixture::admission=fixture::install=true;fixture::message_timeout=fixture::bad_reply=false;fixture::onio={};fixture::onyield={};
fixture::results.clear();fixture::offsets.clear();fixture::lengths.clear();binding.active=true;binding.retiring=false;binding.refs=0;binding.secure=nullptr;binding.compatible=false;
fixture::erased.clear();
binding.channel_identity={&channel,2,23,7};binding.vfs_export_publication_revision=10;binding.vfs_open_binding_id=77;
file={};file.fops=&fops;row={&file,55,77};owner=&binding;}
auto setup()->VfsSecureContext* {wki_remote_vfs_prepare_secure_binding(binding);CHECK(binding.secure&&fixture::registrations==2);return binding.secure;}
auto request(VfsSecureContext*c,bool write=true)->WkiSecureRdmaRequest {
VfsSecureIoIntent i{.version=1,.opcode=static_cast<uint8_t>(write?rdma_protocol::Opcode::WRITE:rdma_protocol::Opcode::READ),.fd=12,.file_offset=10,.length=4,
.binding_id=77,.open_token=55,.owner_boot_epoch=8,.resource_incarnation=6,.resource_id=9};
WkiSecureRdmaRequest r{};r.grant=write?c->write_grant:c->read_grant;r.operation_id=33;r.receive_ticket=34;r.peer_node=2;r.length=4;
r.opcode=write?rdma_protocol::Opcode::WRITE:rdma_protocol::Opcode::READ;r.intent_length=64;std::memcpy(r.native_intent.data(),&i,64);return r;}
void cleanup(VfsSecureContext*c){auto*write=c->write_backing.get();auto*read=c->read_backing.get();auto prior=fixture::erased.size();
binding.secure=nullptr;wki_remote_vfs_release_secure_context(c);CHECK(binding.refs==0);
CHECK(fixture::erased.size()==prior+2);
CHECK(fixture::erased[prior]==std::make_pair(static_cast<void*>(write),size_t(VFS_RDMA_WRITE_SIZE)));
CHECK(fixture::erased[prior+1]==std::make_pair(static_cast<void*>(read),size_t(VFS_RDMA_BULK_SIZE)));}
auto effect(VfsSecureContext*c,const WkiSecureRdmaRequest&r,unsigned inputlen=4)->WkiSecureRdmaResult {
std::array<uint8_t,4> input{1,2,3,4},output{0xa5,0xa5,0xa5,0xa5};bool write=r.opcode==rdma_protocol::Opcode::WRITE;
auto result=secure_vfs_effect(c,r,{write?c->write_backing.get():c->read_backing.get(),4},{input.data(),inputlen},output);
if(!write&&result.outcome==WkiSecureRdmaOutcome::EXACT){for(unsigned n=0;n<result.bytes;++n)CHECK(output[n]==0x5a);
for(unsigned n=result.bytes;n<4;++n)CHECK(output[n]==0xa5);}return result;}
void native_prefix(){for(bool write:{false,true})for(int bytes:{4,2,0,-EIO}){reset();auto*c=setup();auto r=request(c,write);uint64_t gen=0;
CHECK(secure_vfs_arm(c,r,gen)==0&&gen==1&&file.refs==2&&binding.refs==1);fixture::native_bytes=bytes;auto result=effect(c,r);
if(bytes>0||(!write&&bytes==0))CHECK(result.outcome==WkiSecureRdmaOutcome::EXACT&&result.bytes==static_cast<unsigned>(bytes));
else CHECK(result.bytes==0&&result.status<0&&result.outcome==(write?WkiSecureRdmaOutcome::UNCERTAIN:WkiSecureRdmaOutcome::NO_EFFECT));
CHECK((write?fixture::writes:fixture::reads)==1);CHECK(effect(c,r).outcome==WkiSecureRdmaOutcome::NO_EFFECT);secure_vfs_release(c,r,result);
CHECK(file.refs==1&&binding.refs==0);secure_vfs_release(c,r,result);cleanup(c);++fixture::cases;}}
void intent_denials(){for(unsigned which=0;which<15;++which){reset();auto*c=setup();auto r=request(c);VfsSecureIoIntent i{};std::memcpy(&i,r.native_intent.data(),64);
switch(which){case 0:i.open_token++;break;case 1:i.binding_id++;break;case 2:i.owner_boot_epoch++;break;case 3:i.resource_incarnation++;break;
case 4:i.resource_id++;break;case 5:i.reserved0=1;break;case 6:i.reserved1=1;break;case 7:i.reserved2=1;break;case 8:i.reserved3=1;break;
case 9:i.file_offset=INT64_MAX;break;case 10:i.fd++;break;case 11:r.receive_ticket=0;break;case 12:r.flags=1;break;
case 13:r.grant.capability[0]^=1;break;case 14:r.offset=1;break;}
std::memcpy(r.native_intent.data(),&i,64);uint64_t gen=88;CHECK(secure_vfs_arm(c,r,gen)<0&&gen==88&&file.refs==1&&binding.refs==0);
CHECK(!fixture::writes&&!fixture::reads);cleanup(c);++fixture::cases;}}
void authority_and_coverage(){for(unsigned which=0;which<7;++which){reset();auto*c=setup();auto r=request(c);uint64_t gen=0;
switch(which){case 0:file.open_flags=0;break;case 1:file.readonly=true;break;case 2:file.is_directory=true;break;case 3:file.fs_type=ker::vfs::FSType::REMOTE;break;
case 4:fixture::admission=false;break;case 5:binding.retiring=true;break;case 6:row.ordinary_open_token++;break;}
CHECK(secure_vfs_arm(c,r,gen)<0&&file.refs==1&&binding.refs==0);cleanup(c);++fixture::cases;}
reset();auto*c=setup();auto r=request(c);uint64_t gen=0;CHECK(secure_vfs_arm(c,r,gen)==0);CHECK(effect(c,r,2).outcome==WkiSecureRdmaOutcome::NO_EFFECT&&!fixture::writes);
secure_vfs_release(c,r,{});cleanup(c);++fixture::cases;}
void close_and_retirement(){reset();auto*c=setup();auto r=request(c);uint64_t gen=0;CHECK(secure_vfs_arm(c,r,gen)==0);
auto other=r;other.operation_id++;CHECK(secure_vfs_arm(c,other,gen)==-EBUSY);row.file=nullptr;ker::vfs::vfs_put_file(&file);
CHECK(effect(c,r).outcome==WkiSecureRdmaOutcome::NO_EFFECT&&!fixture::writes);secure_vfs_release(c,other,{});CHECK(file.refs==1&&binding.refs==1);
secure_vfs_release(c,r,{});CHECK(file.refs==0&&binding.refs==0);cleanup(c);++fixture::cases;
reset();c=setup();r=request(c);CHECK(secure_vfs_arm(c,r,gen)==0);fixture::onio=[&]{wki_remote_vfs_close_secure_context(c);row.file=nullptr;ker::vfs::vfs_put_file(&file);CHECK(file.refs==1);};
CHECK(effect(c,r).outcome==WkiSecureRdmaOutcome::EXACT);secure_vfs_release(c,r,{});CHECK(!file.refs);cleanup(c);++fixture::cases;
reset();c=setup();r=request(c);CHECK(secure_vfs_arm(c,r,gen)==0);fixture::unregister_busy=1;
fixture::onyield=[&]{CHECK(c->closed&&file.refs==2&&c->write_backing&&c->read_backing&&fixture::erased.empty());
CHECK(c->write_backing[0]==0xa5&&c->write_backing[VFS_RDMA_WRITE_SIZE-1]==0xa5);
CHECK(c->read_backing[0]==0xa5&&c->read_backing[VFS_RDMA_BULK_SIZE-1]==0xa5);secure_vfs_release(c,r,{});};cleanup(c);CHECK(fixture::yields==1&&file.refs==1);++fixture::cases;}
void setup_failures(){for(int failure:{1,2}){reset();fixture::register_fail=failure;wki_remote_vfs_prepare_secure_binding(binding);
CHECK(!binding.secure&&fixture::unregistrations==static_cast<unsigned>(failure-1)&&fixture::erased.size()==2);++fixture::cases;}
reset();fixture::install=false;wki_remote_vfs_prepare_secure_binding(binding);CHECK(!binding.secure&&fixture::unregistrations==2&&fixture::erased.size()==2);++fixture::cases;}
void partial_backing_cleanup(){for(bool write:{false,true}){reset();auto*c=new VfsSecureContext;
auto size=write?VFS_RDMA_WRITE_SIZE:VFS_RDMA_BULK_SIZE;auto&backing=write?c->write_backing:c->read_backing;
backing.reset(new uint8_t[size]);auto*owned=backing.get();std::memset(owned,0xa5,size);
wki_remote_vfs_release_secure_context(c);CHECK(fixture::unregistrations==0&&fixture::erased.size()==1);
CHECK(fixture::erased[0]==std::make_pair(static_cast<void*>(owned),size_t(size)));++fixture::cases;}}
void client_flush(){for(unsigned which=0;which<5;++which){reset();ProxyVfsState p;p.identity=binding.channel_identity;WriteBehindBuffer wb;
wb.data.reset(new uint8_t[4]{1,2,3,4});RemoteFileContext ctx;ctx.proxy=&p;ctx.write_buf=&wb;g_open_cancellations[0].proxy=&p;g_open_cancellations[0].identity=p.identity;
if(which==0)fixture::results={{0,2,WkiSecureRdmaOutcome::EXACT},{0,2,WkiSecureRdmaOutcome::EXACT}};
if(which==1)fixture::results={{-EIO,0,WkiSecureRdmaOutcome::UNCERTAIN}};
if(which==2)fixture::results={{-EAGAIN,0,WkiSecureRdmaOutcome::NO_EFFECT}};
if(which==3)fixture::results={{-EBADF,0,WkiSecureRdmaOutcome::NO_EFFECT}};
if(which==4){fixture::available=false;}
int status=flush_write_behind(&ctx);
if(which==0){CHECK(!status&&wb.pending_len==0&&fixture::offsets==std::vector<int64_t>({10,12})&&fixture::lengths==std::vector<unsigned>({4,2}));}
if(which==1){CHECK(status==-EIO&&wb.pending_len==4&&ctx.secure_write_error==-EIO);CHECK(flush_write_behind(&ctx)==-EIO&&fixture::secure_calls==1&&!fixture::messages);}
if(which==2||which==4)CHECK(!status&&fixture::messages==1&&!wb.pending_len);
if(which==3)CHECK(status==-EBADF&&!fixture::messages&&wb.pending_len==4&&!ctx.secure_write_error);
if(binding.secure)cleanup(binding.secure);
++fixture::cases;}}
void client_read(){reset();ProxyVfsState p;p.identity=binding.channel_identity;RemoteFileContext ctx;ctx.proxy=&p;
fixture::results={{0,2,WkiSecureRdmaOutcome::EXACT}};auto r=secure_vfs_client_read(ctx,p.identity,10,4);
CHECK(r.bytes==2&&ctx.secure_read_capacity==4&&ctx.secure_read_scratch[0]==0x5a&&ctx.secure_read_scratch[1]==0x5a);
auto cap=ctx.secure_read_capacity;fixture::grant_live=false;CHECK(secure_vfs_client_read(ctx,p.identity,10,4).status==-ENOTSUP&&ctx.secure_read_capacity==cap);++fixture::cases;}
void read_publication(){for(int bytes:{4,2,0,-EIO}){reset();ProxyVfsState p;p.identity=binding.channel_identity;RemoteFileContext context;context.proxy=&p;
g_open_cancellations[0].proxy=&p;g_open_cancellations[0].identity=p.identity;std::array<uint8_t,4>out{0xa5,0xa5,0xa5,0xa5};
fixture::results={{bytes<0?bytes:0,bytes<0?0U:static_cast<unsigned>(bytes),bytes<0?WkiSecureRdmaOutcome::NO_EFFECT:WkiSecureRdmaOutcome::EXACT}};
CHECK(actual_secure_read_prefix(&context,out.data(),4,10)==bytes);
for(unsigned n=0;n<out.size();++n)CHECK(out[n]==(bytes>0&&n<static_cast<unsigned>(bytes)?0x5a:0xa5));++fixture::cases;}}
auto envelope(bool write=true)->std::vector<uint8_t> {VfsSecureIoIntent i{.version=1,.opcode=static_cast<uint8_t>(write?rdma_protocol::Opcode::WRITE:rdma_protocol::Opcode::READ),
.fd=12,.file_offset=10,.length=4,.binding_id=77,.open_token=55,.owner_boot_epoch=8,.resource_incarnation=6,.resource_id=9};
std::vector<uint8_t>bytes(sizeof(i)+(write?4:0));std::memcpy(bytes.data(),&i,sizeof(i));return bytes;}
auto message_result()->VfsSecureIoResult {CHECK(fixture::reply.size()>=sizeof(DevOpRespPayload));DevOpRespPayload h{};std::memcpy(&h,fixture::reply.data(),sizeof(h));
CHECK(h.status==0&&h.op_id==OP_VFS_SECURE_IO);VfsSecureIoResult r{};std::memcpy(&r,fixture::reply.data()+sizeof(h),sizeof(r));return r;}
void token_messages(){for(unsigned which=0;which<7;++which){reset();auto bytes=envelope();VfsSecureIoIntent i{};std::memcpy(&i,bytes.data(),64);
if(which==0)i.open_token++;if(which==1)i.binding_id++;if(which==2)i.owner_boot_epoch++;if(which==3)i.resource_incarnation++;
if(which==4)i.reserved3=1;if(which==5)bytes.pop_back();if(which==6)fixture::available=false;std::memcpy(bytes.data(),&i,64);
actual_message(bytes.data(),bytes.size());CHECK(!fixture::writes&&!fixture::reads&&file.refs==1&&binding.refs==0);
if(binding.secure)cleanup(binding.secure);++fixture::cases;}
for(bool write:{false,true}){reset();auto bytes=envelope(write);fixture::native_bytes=2;actual_message(bytes.data(),bytes.size());auto r=message_result();
CHECK(r.outcome==static_cast<uint8_t>(WkiSecureRdmaOutcome::EXACT)&&r.bytes==2&&file.refs==1);
CHECK(fixture::reply.size()==sizeof(DevOpRespPayload)+sizeof(r)+(write?0:2));cleanup(binding.secure);++fixture::cases;}}
void fallback_identity_and_uncertainty(){for(unsigned which=0;which<4;++which){reset();ProxyVfsState p;p.identity=binding.channel_identity;WriteBehindBuffer wb;
wb.data.reset(new uint8_t[4]{1,2,3,4});RemoteFileContext ctx;ctx.proxy=&p;ctx.write_buf=&wb;g_open_cancellations[0].proxy=&p;g_open_cancellations[0].identity=p.identity;
fixture::grant_live=false;
if(which==0)row.ordinary_open_token++;
if(which==1)fixture::message_timeout=true;
if(which==2)fixture::bad_reply=true;
if(which==3){ctx.secure_io_negotiated=true;fixture::available=false;}
int status=flush_write_behind(&ctx);CHECK(status<0&&ctx.secure_io_negotiated&&wb.pending_len==4);
if(which==0||which==3)CHECK(!fixture::writes&&!ctx.secure_write_error);
else {CHECK(ctx.secure_write_error==status);unsigned m=fixture::messages;CHECK(flush_write_behind(&ctx)==status&&fixture::messages==m);}
if(binding.secure)cleanup(binding.secure);++fixture::cases;}}
void context_session_refresh(){reset();auto*c=setup();auto*old=c->write_backing.get();wki_remote_vfs_prepare_secure_binding(binding);
CHECK(binding.secure==c&&fixture::registrations==2);fixture::current_region=-EAGAIN;wki_remote_vfs_prepare_secure_binding(binding);
CHECK(binding.secure==c&&c->write_backing.get()==old&&!c->closed&&fixture::unregistrations==0);++fixture::cases;
auto r=request(c);uint64_t gen=0;CHECK(secure_vfs_arm(c,r,gen)==0);fixture::current_region=-ESTALE;fixture::unregister_busy=1;
fixture::onyield=[&]{CHECK(c->closed&&file.refs==2&&binding.refs==1&&fixture::erased.empty());secure_vfs_release(c,r,{});};wki_remote_vfs_prepare_secure_binding(binding);
CHECK(binding.secure&&fixture::registrations==4&&fixture::unregistrations==3&&file.refs==1&&binding.refs==0&&fixture::erased.size()==2);
fixture::onyield={};cleanup(binding.secure);++fixture::cases;}
}
int main(){using namespace ker::net::wki;native_prefix();intent_denials();authority_and_coverage();close_and_retirement();setup_failures();partial_backing_cleanup();client_flush();client_read();read_publication();token_messages();fallback_identity_and_uncertainty();context_session_refresh();CHECK(!fixture::guards);
std::printf("secure VFS actual-source %u bounded cases PASS\n",fixture::cases);}
'''


def make_tu(source, mutation):
    original = source
    if mutation == 'token':
        source = source.replace('row->ordinary_open_token != intent.open_token', 'false', 1)
    elif mutation == 'coverage':
        source = source.replace('input.size() != request.length', 'false', 1)
    elif mutation == 'replay':
        source = source.replace('ctx->secure_write_error = ERROR', 'static_cast<void>(ERROR)', 1)
    elif mutation == 'downgrade':
        source = source.replace('message && ctx->secure_io_negotiated', 'false', 1)
    elif mutation == 'result':
        source = source.replace('!wki_vfs_secure_message_result_valid(reply, true, static_cast<uint32_t>(input.size()), length)', 'false', 1)
    elif mutation == 'pressure':
        source = source.replace('if (WRITE == -EAGAIN || READ == -EAGAIN || (WRITE != -ESTALE && READ != -ESTALE)) return;', '')
        source = source.replace('if ((WRITE != 0 && WRITE != -ESTALE) || (READ != 0 && READ != -ESTALE)) return;', '')
    elif mutation == 'wipe':
        source = source.replace('if (context->write_backing) auth_crypto::secure_erase(context->write_backing.get(), VFS_RDMA_WRITE_SIZE);', '', 1)
    if mutation and source == original:
        raise ValueError('mutation did not change the actual source: ' + mutation)
    identity = block((KERN / 'net/wki/wki.hpp').read_text(), 'struct WkiChannelIdentity {') + ';'
    context = block(source, 'struct VfsSecureContext {') + ';'
    prelude = PRELUDE.replace('/* actual identity */', identity).replace('struct DevServerBinding {', 'struct DevServerBinding;\n' + context + '\nstruct DevServerBinding {')
    functions = ['grant_binding_matches', 'secure_vfs_request_matches', 'secure_vfs_intent_matches', 'secure_vfs_file_locked',
                 'secure_vfs_arm', 'secure_vfs_effect', 'secure_vfs_release', 'wki_remote_vfs_close_secure_context',
                 'wki_remote_vfs_release_secure_context', 'wki_remote_vfs_prepare_secure_binding', 'capture_secure_vfs_client',
                 'make_secure_vfs_intent', 'secure_vfs_message_fallback', 'secure_vfs_client_write', 'secure_vfs_client_read', 'secure_vfs_message_write', 'secure_vfs_message_read', 'flush_write_behind']
    bodies = []
    for name in functions:
        match = re.search(r'\b(?:auto|void)\s+' + name + r'\(', source)
        bodies.append(block(source, source[match.start():source.index('{', match.start())]))
    start = source.index('    WkiChannelIdentity secure_identity{};', source.index('auto remote_vfs_read('))
    end = source.index('    // -- Bulk RDMA path', start)
    publication = 'auto actual_secure_read_prefix(RemoteFileContext*ctx,uint8_t*dest,uint32_t remaining,int64_t cur_offset)->ssize_t { ssize_t total_read=0;\n' + source[start:end] + '\nreturn -ENOTSUP;\n}'
    constants = '\n'.join(re.findall(r'constexpr uint32_t VFS_SECURE_MESSAGE_[^\n]*;', source))
    case = block(source, 'case OP_VFS_SECURE_IO: {')
    message = r'''
void actual_message(const uint8_t*data,uint16_t data_len) {
auto*grant_binding=owner;auto channel_identity=owner->channel_identity;uint16_t op_id=OP_VFS_SECURE_IO,REQ_COOKIE=3;
auto send_buffered_resp=[](const void*p,uint16_t size){auto*b=static_cast<const uint8_t*>(p);fixture::reply.assign(b,b+size);};
auto reject_request=[&](int16_t status){DevOpRespPayload h{.op_id=op_id,.status=status,.data_len=0,.reserved=REQ_COOKIE};send_buffered_resp(&h,sizeof(h));};
switch(op_id){
''' + case + '\n}\n}\n'
    return prelude + '\n' + constants + '\n' + '\n'.join(bodies) + '\n' + publication + '\n' + message + '\n' + CASES


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, default=SOURCE)
    parser.add_argument('--keep', type=Path)
    parser.add_argument('--mutation', choices=['token', 'coverage', 'replay', 'downgrade', 'result', 'pressure', 'wipe'])
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='wki-secure-vfs-') as tmp:
        path = args.keep or Path(tmp)
        path.mkdir(parents=True, exist_ok=True)
        tu = path / 'actual-vfs.cpp'
        tu.write_text(make_tu(args.source.read_text(), args.mutation))
        binary = path / 'vfs-test'
        command = ['/usr/bin/clang++', '-std=c++23', '-Wall', '-Wextra', '-O1', '-g', '-fsanitize=address,undefined',
                   '-fno-omit-frame-pointer', '-fno-pie', '-no-pie', '-MMD', '-MF', str(path / 'inputs.d'),
                   '-I' + str(KERN), str(tu), '-o', str(binary)]
        compiled = subprocess.run(command, capture_output=True, text=True)
        (path / 'compile.log').write_text(compiled.stdout + compiled.stderr)
        print(compiled.stdout + compiled.stderr, end='')
        if compiled.returncode:
            return compiled.returncode
        env = os.environ | {'ASAN_OPTIONS': 'detect_leaks=0', 'UBSAN_OPTIONS': 'halt_on_error=1'}
        result = subprocess.run([str(binary)], capture_output=True, text=True, env=env)
        (path / 'run.log').write_text(result.stdout + result.stderr)
        print(result.stdout + result.stderr, end='')
        dependencies = (path / 'inputs.d').read_text().replace('\\\n', '').split(':', 1)[1].split()
        inputs = {str(Path(name).resolve()): hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in dependencies}
        (path / 'result.json').write_text(json.dumps({'compile_exit': compiled.returncode, 'run_exit': result.returncode,
            'mutation': args.mutation, 'source': str(args.source), 'source_sha256': hashlib.sha256(args.source.read_bytes()).hexdigest(),
            'fixture_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'command': command,
            'input_sha256': inputs}, indent=2) + '\n')
        return result.returncode


if __name__ == '__main__':
    raise SystemExit(main())
