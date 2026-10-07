#!/usr/bin/env python3
"""Compile actual secure IPC consumer helpers and declarations with explicit host seams.

Native VFS callback, scheduler, peer/channel and authenticated common-engine
boundaries are instrumented. This proves consumer ownership/prefix/fallback,
not native hardware, crypto or cross-node execution.
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
CPP = KERN / 'net/wki/remote_ipc.cpp'
HPP = KERN / 'net/wki/remote_ipc.hpp'
WKI = KERN / 'net/wki/wki.hpp'


def decl(source, name):
    start = source.index('struct ' + name + ' {')
    return extent(source, start, semicolon=True)


def extent(source, start, semicolon=False):
    brace = source.index('{', start)
    depth, end = 1, brace + 1
    while depth:
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    if semicolon:
        end = source.index(';', end) + 1
    return source[start:end]


def function(source, name):
    for m in re.finditer(r'^(?:auto|void) ' + re.escape(name) + r'\(', source, re.M):
        brace = source.index('{', m.start())
        if ';' not in source[m.start():brace]:
            return extent(source, m.start())
    raise ValueError(name)


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <sys/types.h>
#include <net/wki/wire.hpp>
#include <net/wki/secure_rdma.hpp>
#define CHECK(x) do {if(!(x)){std::fprintf(stderr,"CHECK failed %s:%d: %s\n",__FILE__,__LINE__,#x);std::abort();}}while(0)
#define WOS_PERF_CALLSITE() 0
namespace fixture {
thread_local unsigned guards=0;
unsigned cases=0,grow_calls=0,native_calls=0,secure_calls=0,message_calls=0,register_calls=0,revoke_calls=0,wakes=0;
ssize_t native_result=4;
bool busy=false,available=true,grant_live=true,register_fail=false,lose_result=false,query_fail=false,native_pipe=true,pump_pin=false,effect_growth_forbidden=true;
std::vector<std::string> trace;
std::vector<uint8_t> delivered;
std::function<void()> native_hook,registration_hook;
}
namespace ker::mod::sys {struct Spinlock {
 std::mutex m;void lock(){m.lock();++fixture::guards;}void unlock(){CHECK(fixture::guards);--fixture::guards;m.unlock();}
 auto lock_irqsave()->uint64_t{lock();return 0;}void unlock_irqrestore(uint64_t){unlock();}
};}
namespace ker::util {template<class T,size_t N> struct SmallVec:std::vector<T>{};}
namespace ker::mod::sched {namespace task {struct Task{};}
void kern_yield(){CHECK(!fixture::guards);std::this_thread::yield();}
void kern_sleep_us(uint64_t){CHECK(!fixture::guards);}
}
namespace ker::vfs {
struct File;
struct FileOperations {ssize_t(*vfs_write)(File*,const void*,size_t,size_t)=nullptr;};
struct File {
 int fd=0;void*private_data=nullptr;FileOperations*fops=nullptr;size_t pos=0;bool is_directory=false;int fs_type=0;
 std::atomic<int>refcount{1};int open_flags=0,fd_flags=0;void*vfs_path=nullptr;size_t dir_fs_count=0;
 void*stream_cache_attachment=nullptr,*cache_notify_attachment=nullptr;std::atomic<unsigned>positional_read_depth{0};
};
bool vfs_is_pipe_file(File*f){return f&&fixture::native_pipe;}
bool vfs_is_socket_file(File*){return false;}
bool vfs_pipe_reserve_capacity(File*,size_t){CHECK(!fixture::guards);CHECK(!fixture::effect_growth_forbidden);++fixture::grow_calls;return true;}
}
namespace ker::mod::perf {enum class WkiPerfIpcOp{PROXY_WRITE};}
namespace ker::net::wki {
/* actual declarations */
struct WkiChannel {bool active=true;uint32_t generation=7;uint16_t peer_node_id=2,channel_id=240;};
auto wki_channel_generation_is_live(WkiChannel*c,uint16_t peer,uint16_t channel,uint32_t gen)->bool {
 CHECK(!fixture::guards);return c&&c->active&&c->peer_node_id==peer&&c->channel_id==channel&&c->generation==gen;
}
/* actual constants/types */
/* actual native declarations */
ker::mod::sys::Spinlock s_ipc_lock;
IpcPeerCleanupEpoch epoch{.used=true,.node_id=2,.epoch=3};
WkiIpcExport*g_secure_retired_exports=nullptr;
std::atomic<bool>g_ipc_consumer_stopping{false};
uint64_t g_next_ipc_secure_lifetime=1;
struct IpcDevOpWork{};
struct PendingPipeChunk {const uint8_t*data=nullptr;uint16_t len=0;uint16_t flow_credit=0;IpcDevOpWork*dev_op_owner=nullptr;};
struct ExportPipeWriteBacklog {WkiIpcExport*exp=nullptr;std::deque<PendingPipeChunk>chunks;bool close_pending=false;int32_t write_error=0;uint32_t buffered_bytes=0;};
std::vector<WkiIpcExport*>g_ipc_exports;
std::vector<ExportPipeWriteBacklog*>g_export_pipe_write_backlogs;
bool ipc_dev_op_work_can_back_pipe_chunk(IpcDevOpWork*,const uint8_t*,uint16_t){return false;}
auto find_export_by_resource_id(uint32_t id)->WkiIpcExport*{CHECK(fixture::guards);for(auto*e:g_ipc_exports)if(e&&e->resource_id==id)return e;return nullptr;}
auto ensure_export_pipe_write_backlog_locked(WkiIpcExport*)->ExportPipeWriteBacklog*;
void note_export_pipe_data_received_locked(WkiIpcExport*e,uint16_t len){CHECK(fixture::guards);e->pipe_bytes_received+=len;}
bool export_has_pipe_pump_slot_locked(const WkiIpcExport*){CHECK(fixture::guards);return fixture::pump_pin;}
ExportPipeWriteBacklog backlog;
bool has_backlog=false;
auto ensure_export_pipe_write_backlog_locked(WkiIpcExport*e)->ExportPipeWriteBacklog*{CHECK(fixture::guards);has_backlog=true;backlog.exp=e;return &backlog;}
auto find_export_pipe_write_backlog_locked(WkiIpcExport*)->ExportPipeWriteBacklog*{CHECK(fixture::guards);return has_backlog?&backlog:nullptr;}
void queue_export_pipe_write_flush_locked(ExportPipeWriteBacklog*){CHECK(fixture::guards);}
auto export_pipe_write_backlog_shard(ExportPipeWriteBacklog*)->size_t{return 0;}
void wake_export_pipe_write_flush_worker(size_t){CHECK(!fixture::guards);++fixture::wakes;}
void ipc_release_file_ref(ker::vfs::File*f){CHECK(!fixture::guards);if(f)CHECK(f->refcount.fetch_sub(1)>1);}
auto compact_inactive_exports_locked()->WkiIpcExport*;
auto ipc_peer_cleanup_active_locked(uint16_t peer)->bool{CHECK(fixture::guards);return peer!=epoch.node_id||epoch.cleanup_active;}
auto ipc_peer_cleanup_epoch_locked(uint16_t peer)->uint64_t{CHECK(fixture::guards);return peer==epoch.node_id?epoch.epoch:0;}
auto ipc_peer_cleanup_slot_locked(uint16_t peer,bool)->IpcPeerCleanupEpoch*{CHECK(fixture::guards);return peer==epoch.node_id?&epoch:nullptr;}
void release_ipc_dev_op_worker_locked(uint16_t peer){CHECK(fixture::guards&&peer==epoch.node_id&&epoch.active_workers);--epoch.active_workers;}
bool current_task_has_deliverable_signal(){return false;}
bool is_proxy_pipe_file(const ker::vfs::File*){return false;}
void clear_current_daemon_sigpipe(){CHECK(!fixture::guards);}
auto clamp_io_count(ssize_t result,size_t size)->ssize_t{return result>0?std::min<ssize_t>(result,size):result;}
std::atomic<uint64_t>g_proxy_write_payload_bytes{0},g_proxy_write_no_credit_waits{0},g_proxy_write_block_us{0};
void perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp,uint16_t,uint16_t,uint32_t,int32_t,uint32_t,uint64_t){}
struct IpcPerfTrace {template<class...A>IpcPerfTrace(A...){};template<class...A>void finish(A...){};};
void proxy_wake_message_write_waiters(ProxyIpcState*){CHECK(!fixture::guards);}
void wki_ipc_proxy_wake_poll_waiters(ProxyIpcState*){CHECK(!fixture::guards);}
auto proxy_reserve_message_write_credits(ProxyIpcState*p,size_t n,bool)->ssize_t{
 if(p->message_write_error.load()!=0)return p->message_write_error.load();
 auto take=std::min<size_t>(n,p->message_write_credits.load());p->message_write_credits.fetch_sub(take);return take?take:-EAGAIN;
}
void proxy_grant_message_write_credits(ProxyIpcState*p,uint32_t n){p->message_write_credits.fetch_add(n);}
auto ipc_pipe_nonblocking_send_status(int)->ssize_t{return -EAGAIN;}
auto wki_now_us()->uint64_t{return 10;}
auto ipc_pipe_send_retry_sleep_us(int,uint32_t)->uint64_t{return 1000;}
bool ipc_diag_due(uint64_t,uint64_t&){return false;}
void log_ipc_send_retry_diag(const char*,ProxyIpcState*,uint32_t,size_t,size_t,int,uint32_t,uint64_t){}
void pause_for_ipc_send_retry(int,uint32_t){CHECK(!fixture::guards);}
auto wki_send_accounted(const WkiChannelIdentity&,MsgType,const void*,uint16_t,std::atomic<uint64_t>&accepted,uint16_t n)->int {
 CHECK(!fixture::guards);fixture::trace.push_back("ordinary-send");++fixture::message_calls;accepted.fetch_add(n);return 0;
}
WkiIpcExport*registered=nullptr;
WkiSecureRdmaHooks hooks{};
auto wki_secure_rdma_available(const WkiChannelIdentity&,rdma_protocol::Service s)->bool{CHECK(!fixture::guards&&s==rdma_protocol::Service::IPC);return fixture::available;}
auto wki_secure_rdma_register_region(const WkiChannelIdentity&i,rdma_protocol::Service s,void*p,uint32_t size,uint64_t gen,uint8_t perms,
 const WkiSecureRdmaHooks&h,void*ctx,rdma_protocol::RegionDescriptor&out)->int{
 CHECK(!fixture::guards&&s==rdma_protocol::Service::IPC&&p&&size==65536&&gen);++fixture::register_calls;
 if(fixture::registration_hook)fixture::registration_hook();if(fixture::register_fail)return -ENOMEM;
 out={};out.rkey=91;out.size=size;out.generation=17;out.binding_generation=gen;out.service=static_cast<uint8_t>(s);out.permissions=perms;
 out.channel_id=i.channel_id;out.capability.fill(0xa7);out.session_id.fill(0x3a);out.transport=1;registered=static_cast<WkiIpcExport*>(ctx);hooks=h;return 0;
}
auto wki_secure_rdma_unregister_region(const rdma_protocol::RegionDescriptor&d)->int{
 CHECK(!fixture::guards&&d.rkey==91);++fixture::revoke_calls;
 fixture::trace.push_back("unregister");return fixture::busy||(registered&&registered->pipe_secure_reserved)?-EBUSY:0;
}
auto wki_secure_rdma_remote_grant(const WkiChannelIdentity&,rdma_protocol::Service s,uint32_t rkey,rdma_protocol::RegionDescriptor&out,uint8_t perms)->bool{
 CHECK(!fixture::guards&&s==rdma_protocol::Service::IPC);if(!fixture::grant_live||!registered||rkey!=registered->pipe_secure_descriptor.rkey)return false;
 out=registered->pipe_secure_descriptor;CHECK((out.permissions&perms)==perms);return true;
}
auto wki_secure_rdma_read(const WkiChannelIdentity&,const rdma_protocol::RegionDescriptor&,uint64_t,std::span<uint8_t>,std::span<const uint8_t>)->WkiSecureRdmaResult;
auto wki_secure_rdma_write(const WkiChannelIdentity&,const rdma_protocol::RegionDescriptor&,uint64_t,std::span<const uint8_t>,uint16_t,std::span<const uint8_t>)->WkiSecureRdmaResult;
/* actual functions */
WkiChannel control{.channel_id=WKI_CHAN_RESOURCE},data;
ker::vfs::FileOperations native_ops;
ker::vfs::File file;
uint64_t op_id=1;
auto fresh_request(rdma_protocol::Opcode opcode,uint64_t offset,uint32_t len,std::span<const uint8_t>intent)->WkiSecureRdmaRequest{
 WkiSecureRdmaRequest r{};r.grant=registered->pipe_secure_descriptor;r.operation_id=op_id++;r.receive_ticket=op_id++;r.peer_node=2;
 r.opcode=opcode;r.offset=offset;r.length=len;r.intent_length=intent.size();std::copy(intent.begin(),intent.end(),r.native_intent.begin());return r;
}
auto wki_secure_rdma_read(const WkiChannelIdentity&,const rdma_protocol::RegionDescriptor&,uint64_t off,std::span<uint8_t>out,std::span<const uint8_t>intent)->WkiSecureRdmaResult{
 CHECK(!fixture::guards);if(fixture::query_fail)return {-EAGAIN,0,WkiSecureRdmaOutcome::NO_EFFECT};auto r=fresh_request(rdma_protocol::Opcode::READ,off,out.size(),intent);uint64_t gen=0;
 int rc=hooks.arm(registered,r,gen);if(rc)return {rc,0,WkiSecureRdmaOutcome::NO_EFFECT};auto result=hooks.effect(registered,r,{registered->pipe_rdma_region,registered->pipe_rdma_region_size},{},out);hooks.release(registered,r,result);return result;
}
auto wki_secure_rdma_write(const WkiChannelIdentity&,const rdma_protocol::RegionDescriptor&,uint64_t off,std::span<const uint8_t>in,uint16_t,std::span<const uint8_t>intent)->WkiSecureRdmaResult{
 CHECK(!fixture::guards);++fixture::secure_calls;auto r=fresh_request(rdma_protocol::Opcode::WRITE,off,in.size(),intent);uint64_t gen=0;
 int rc=hooks.arm(registered,r,gen);if(rc)return {rc,0,WkiSecureRdmaOutcome::NO_EFFECT};auto result=hooks.effect(registered,r,{registered->pipe_rdma_region,registered->pipe_rdma_region_size},in,{});hooks.release(registered,r,result);CHECK(!registered->pipe_secure_reserved&&registered->pipe_secure_file==nullptr);fixture::trace.push_back("release-joined");
 if(fixture::lose_result)return {-ETIMEDOUT,0,WkiSecureRdmaOutcome::UNCERTAIN};return result;
}
}
using namespace ker::net::wki;
namespace {
auto channels()->WkiIpcChannels{return {{&control,2,WKI_CHAN_RESOURCE,7},{&data,2,WKI_CHAN_IPC_DATA,7}};}
void reset(){CHECK(!fixture::guards&&!g_secure_retired_exports);registered=nullptr;hooks={};has_backlog=false;backlog={};epoch={.used=true,.node_id=2,.epoch=3};
 CHECK(g_ipc_exports.empty()&&g_export_pipe_write_backlogs.empty());fixture::trace.clear();fixture::effect_growth_forbidden=true;fixture::grow_calls=0;fixture::native_pipe=true;fixture::pump_pin=false;fixture::native_calls=fixture::secure_calls=fixture::message_calls=0;fixture::native_result=4;fixture::busy=false;fixture::available=true;fixture::grant_live=true;
 fixture::register_fail=fixture::lose_result=fixture::query_fail=false;fixture::native_hook={};fixture::registration_hook={};fixture::delivered.clear();control.active=data.active=true;control.generation=data.generation=7;
 file.refcount.store(1);file.fops=&native_ops;native_ops.vfs_write=[](ker::vfs::File*f,const void*p,size_t n,size_t)->ssize_t{
  CHECK(!fixture::guards&&(f->open_flags&WKI_IPC_O_NONBLOCK));fixture::trace.push_back("native-attempt");++fixture::native_calls;if(fixture::native_hook)fixture::native_hook();
  auto rc=fixture::native_result==2&&fixture::native_calls>1?ssize_t{-EAGAIN}:fixture::native_result>0?std::min<ssize_t>(fixture::native_result,n):fixture::native_result;
  if(rc>0)fixture::delivered.insert(fixture::delivered.end(),static_cast<const uint8_t*>(p),static_cast<const uint8_t*>(p)+rc);return rc;
 };
}
auto setup()->WkiIpcExport*{auto*e=new WkiIpcExport;e->resource_id=123;e->consumer_node=2;e->res_type=ResourceType::IPC_PIPE;e->channels=channels();
 ExportPipeWriteBacklog*b=nullptr;prepare_export_pipe_rdma(*e,b,&file);CHECK(e->pipe_rdma_enabled&&e->pipe_rdma_region&&b==nullptr);e->file=&file;e->active=true;e->pipe_secure_activated=true;return e;}
void retire(WkiIpcExport*e){e->active=false;e->file=nullptr;free_retired_exports(e);fixture::busy=false;service_secure_retired_exports();registered=nullptr;CHECK(file.refcount.load()==1&&!fixture::guards&&!g_secure_retired_exports);++fixture::cases;}
auto append(WkiIpcExport*e,uint32_t len=4)->WkiSecureRdmaRequest{
 IpcSecureIntent i{.kind=IPC_SECURE_APPEND,.resource_id=e->resource_id,.lifetime=e->pipe_secure_lifetime,.head=e->pipe_bytes_received,.tail=e->pipe_bytes_delivered,.credit=ipc_secure_credit_locked(*e)};
 return fresh_request(rdma_protocol::Opcode::WRITE,i.head%e->pipe_rdma_capacity,len,{reinterpret_cast<const uint8_t*>(&i),sizeof(i)});
}
void fill_proxy(ProxyIpcState&p){p.active=true;p.home_node=2;p.resource_id=123;p.res_type=ResourceType::IPC_PIPE;p.data_channel=channels().data;
 p.pipe_rdma_enabled=true;p.pipe_rdma_rkey=91;p.pipe_rdma_capacity=65536;p.message_write_flow_control=true;p.message_write_credits=100;}
void native_prefixes(){for(ssize_t n:{4,2,-EAGAIN,-EPIPE,-EIO,0}){reset();auto*e=setup();auto flags=s_ipc_lock.lock_irqsave();auto r=append(e);s_ipc_lock.unlock_irqrestore(flags);uint64_t gen=0;CHECK(hooks.arm(e,r,gen)==0&&file.refcount.load()==2);
 fixture::native_result=n;std::array<uint8_t,4>b{1,2,3,4};auto result=hooks.effect(e,r,{e->pipe_rdma_region,e->pipe_rdma_region_size},b,{});
 CHECK(result.bytes==(n>0?n:0)&&e->pipe_bytes_delivered==result.bytes&&e->pipe_bytes_received==result.bytes);
 CHECK(result.outcome==(n>0?WkiSecureRdmaOutcome::EXACT:n==-EAGAIN||n==-EPIPE?WkiSecureRdmaOutcome::NO_EFFECT:WkiSecureRdmaOutcome::UNCERTAIN));
 CHECK(epoch.active_workers==0&&e->pipe_secure_reserved&&fixture::grow_calls==0);hooks.release(e,r,result);hooks.release(e,r,result);retire(e);}}
void rejection_matrix(){for(unsigned v=0;v<30;++v){reset();auto*e=setup();auto flags=s_ipc_lock.lock_irqsave();auto r=append(e);s_ipc_lock.unlock_irqrestore(flags);IpcSecureIntent i{};std::memcpy(&i,r.native_intent.data(),sizeof(i));
 switch(v){case 0:++r.grant.generation;break;case 1:++r.grant.capability[0];break;case 2:++r.peer_node;break;case 3:r.operation_id=0;break;case 4:r.receive_ticket=0;break;case 5:++i.lifetime;break;case 6:++i.head;break;case 7:i.credit=e->pipe_rdma_capacity-1;break;case 8:i.reserved[0]=1;break;case 9:r.intent_length=63;break;case 10:r.offset=65535;break;case 11:e->pipe_secure_activated=false;break;case 12:epoch.cleanup_active=true;break;case 13:++data.generation;break;case 14:r.flags=1;break;case 15:has_backlog=true;backlog.close_pending=true;break;case 16:r.opcode=rdma_protocol::Opcode::NOTIFY;break;
 case 17:++r.grant.session_id[0];break;case 18:++r.grant.binding_generation;break;case 19:r.grant.service=static_cast<uint8_t>(rdma_protocol::Service::ZONE);break;case 20:r.grant.permissions=rdma_protocol::PERMIT_READ;break;case 21:++r.grant.channel_id;break;case 22:++r.grant.rkey;break;
 case 23:++i.resource_id;break;case 24:++i.tail;break;case 25:i.version=2;break;case 26:r.length=UINT16_MAX+1U;break;case 27:e->pipe_write_in_flight=true;break;case 28:e->active=false;break;case 29:++epoch.epoch;break;}
 std::memcpy(r.native_intent.data(),&i,sizeof(i));uint64_t gen=99;CHECK(hooks.arm(e,r,gen)!=0&&gen==99&&!e->pipe_secure_reserved&&file.refcount.load()==1);retire(e);}}
void close_and_reservations(){reset();auto*e=setup();auto flags=s_ipc_lock.lock_irqsave();auto r=append(e);s_ipc_lock.unlock_irqrestore(flags);uint64_t gen=0;CHECK(hooks.arm(e,r,gen)==0);CHECK(hooks.arm(e,r,gen)==-EAGAIN);
 auto stale=r;++stale.receive_ticket;hooks.release(e,stale,{});CHECK(e->pipe_secure_reserved);e->active=false;std::array<uint8_t,4>b{};auto result=hooks.effect(e,r,{e->pipe_rdma_region,e->pipe_rdma_region_size},b,{});CHECK(result.outcome==WkiSecureRdmaOutcome::NO_EFFECT&&fixture::native_calls==0);hooks.release(e,r,result);retire(e);
 reset();e=setup();flags=s_ipc_lock.lock_irqsave();r=append(e);s_ipc_lock.unlock_irqrestore(flags);CHECK(hooks.arm(e,r,gen)==0);fixture::native_hook=[e]{CHECK(epoch.active_workers==1);e->active=false;e->file=nullptr;};result=hooks.effect(e,r,{e->pipe_rdma_region,e->pipe_rdma_region_size},b,{});CHECK(result.bytes==4&&result.outcome==WkiSecureRdmaOutcome::EXACT);hooks.release(e,r,result);retire(e);}
void reclaim_busy(){reset();auto*e=setup();auto*p=e->pipe_rdma_region;fixture::busy=true;e->active=false;e->file=nullptr;free_retired_exports(e);service_secure_retired_exports();CHECK(g_secure_retired_exports==e&&e->pipe_rdma_region==p&&e->pipe_secure_descriptor.rkey==91);fixture::busy=false;service_secure_retired_exports();registered=nullptr;++fixture::cases;
 reset();e=setup();e->pipe_rdma_enabled=false;e->active=false;e->file=nullptr;fixture::busy=true;free_retired_exports(e);service_secure_retired_exports();CHECK(g_secure_retired_exports==e);fixture::busy=false;service_secure_retired_exports();registered=nullptr;++fixture::cases;}
void frontend(){for(unsigned v=0;v<7;++v){reset();auto*e=setup();ProxyIpcState p;fill_proxy(p);ker::vfs::File pf;pf.private_data=&p;pf.open_flags=WKI_IPC_O_NONBLOCK;std::array<uint8_t,4>b{1,2,3,4};
 switch(v){case 1:fixture::native_result=2;break;case 2:fixture::native_result=-EAGAIN;break;case 3:fixture::lose_result=true;break;case 4:fixture::grant_live=false;break;case 5:fixture::native_result=-EPIPE;break;case 6:has_backlog=true;backlog.chunks.push_back({.len=4});break;}
 auto rc=proxy_pipe_write(&pf,b.data(),b.size(),0);
 if(v==3){CHECK(rc==-EIO&&fixture::message_calls==0&&p.pipe_secure_uncertain&&p.bytes_delivered==0);CHECK(proxy_pipe_write(&pf,b.data(),4,0)==-EIO);}
 else if(v==5){CHECK(rc==-EPIPE&&fixture::message_calls==0);}
 else {CHECK(rc==(v==1?2:4));CHECK(fixture::message_calls==(v==2||v==4||v==6?1U:0U));CHECK(p.bytes_delivered==(v==0?4:v==1?2:0));}
 if(v==2){CHECK(fixture::trace==std::vector<std::string>({"native-attempt","release-joined","ordinary-send"}));CHECK(e->pipe_bytes_received==0&&e->pipe_bytes_delivered==0&&fixture::delivered.empty());}
 CHECK(!p.pipe_rdma_writer_active&&!e->pipe_secure_reserved&&file.refcount==1);retire(e);}}
void ordinary_growth_positive(){reset();fixture::effect_growth_forbidden=false;std::array<uint8_t,4>bytes{1,2,3,4};int const FLAGS=file.open_flags;CHECK(export_pipe_write_nonblocking(&file,bytes.data(),bytes.size())==4&&fixture::grow_calls==1&&file.open_flags==FLAGS);++fixture::cases;}
void preparation_matrix(){for(unsigned v=0;v<7;++v){reset();auto*e=new WkiIpcExport;e->resource_id=123;e->consumer_node=2;e->res_type=ResourceType::IPC_PIPE;e->channels=channels();
 auto before=fixture::register_calls;ExportPipeWriteBacklog*b=nullptr;
 switch(v){case 0:fixture::available=false;break;case 1:fixture::native_pipe=false;break;case 2:fixture::register_fail=true;break;case 3:g_next_ipc_secure_lifetime=0;break;case 4:g_next_ipc_secure_lifetime=UINT64_MAX;break;}
 prepare_export_pipe_rdma(*e,b,&file,v==5?0:v==6?1:-1);
 CHECK(!e->pipe_rdma_enabled&&e->pipe_rdma_region==nullptr&&e->pipe_rdma_region_size==0&&e->pipe_secure_descriptor.rkey==0&&b==nullptr);
 CHECK(fixture::register_calls-before==(v==2?1U:0U));delete e;g_next_ipc_secure_lifetime=1;++fixture::cases;}}
void message_reservation(){reset();auto*e=setup();g_ipc_exports.push_back(e);auto flags=s_ipc_lock.lock_irqsave();auto r=append(e);s_ipc_lock.unlock_irqrestore(flags);uint64_t gen=0;
 CHECK(hooks.arm(e,r,gen)==0);std::array<uint8_t,4>b{1,2,3,4};CHECK(!queue_export_pipe_write_data(e->resource_id,b.data(),b.size(),nullptr,true));CHECK(!has_backlog&&e->pipe_bytes_received==0);
 hooks.release(e,r,{});CHECK(queue_export_pipe_write_data(e->resource_id,b.data(),b.size(),nullptr,true));CHECK(backlog.chunks.size()==1&&e->pipe_bytes_received==4&&e->pipe_bytes_delivered==0);
 flags=s_ipc_lock.lock_irqsave();r=append(e);s_ipc_lock.unlock_irqrestore(flags);CHECK(hooks.arm(e,r,gen)!=0);
 delete[]backlog.chunks.front().data;backlog={};has_backlog=false;g_ipc_exports.clear();retire(e);}
void published_cleanup_trace(){reset();auto*e=setup();g_ipc_exports.push_back(e);auto flags=s_ipc_lock.lock_irqsave();auto r=append(e);s_ipc_lock.unlock_irqrestore(flags);uint64_t gen=0;
 CHECK(hooks.arm(e,r,gen)==0);flags=s_ipc_lock.lock_irqsave();e->active=false;e->file=nullptr;CHECK(compact_inactive_exports_locked()==nullptr);s_ipc_lock.unlock_irqrestore(flags);
 fixture::busy=true;hooks.release(e,r,{});CHECK(g_ipc_exports.empty()&&g_secure_retired_exports==e&&file.refcount==1);service_secure_retired_exports();CHECK(g_secure_retired_exports==e);
 fixture::busy=false;service_secure_retired_exports();registered=nullptr;CHECK(g_secure_retired_exports==nullptr&&fixture::trace==std::vector<std::string>({"unregister","unregister"}));++fixture::cases;}
void concurrent_reservation(){for(unsigned trial=0;trial<32;++trial){reset();auto*e=setup();auto flags=s_ipc_lock.lock_irqsave();auto r=append(e);s_ipc_lock.unlock_irqrestore(flags);auto other=r;++other.operation_id;++other.receive_ticket;
 std::atomic<unsigned>ready=0;std::atomic<bool>start=false;int rc[2]{};uint64_t gen[2]{99,99};std::thread a([&]{ready.fetch_add(1);while(!start.load())std::this_thread::yield();rc[0]=hooks.arm(e,r,gen[0]);});std::thread b([&]{ready.fetch_add(1);while(!start.load())std::this_thread::yield();rc[1]=hooks.arm(e,other,gen[1]);});
 while(ready.load()!=2)std::this_thread::yield();start=true;a.join();b.join();CHECK((rc[0]==0&&rc[1]==-EAGAIN)||(rc[1]==0&&rc[0]==-EAGAIN));CHECK(file.refcount==2);CHECK(gen[rc[0]==0?1:0]==99);hooks.release(e,rc[0]==0?r:other,{});retire(e);}}
void effect_requalification(){for(unsigned v=0;v<6;++v){reset();auto*e=setup();auto flags=s_ipc_lock.lock_irqsave();auto r=append(e);s_ipc_lock.unlock_irqrestore(flags);uint64_t gen=0;CHECK(hooks.arm(e,r,gen)==0);
 std::array<uint8_t,4>b{1,2,3,4};std::span<uint8_t>backing(e->pipe_rdma_region,e->pipe_rdma_region_size),out{};std::span<const uint8_t>input(b);auto sent=r;
 switch(v){case 0:backing=backing.subspan(1);break;case 1:++sent.receive_ticket;break;case 2:input=input.first(3);break;case 3:out=b;break;case 4:epoch.active_workers=UINT32_MAX;break;case 5:++epoch.epoch;break;}
 auto result=hooks.effect(e,sent,backing,input,out);CHECK(result.outcome==WkiSecureRdmaOutcome::NO_EFFECT&&result.bytes==0&&fixture::native_calls==0&&e->pipe_bytes_received==0&&e->pipe_bytes_delivered==0);epoch.active_workers=0;hooks.release(e,r,result);retire(e);}}
void wrap_and_exhaustion(){reset();auto*e=setup();e->pipe_bytes_received=e->pipe_bytes_delivered=65534;auto flags=s_ipc_lock.lock_irqsave();auto r=append(e,2);s_ipc_lock.unlock_irqrestore(flags);uint64_t gen=0;CHECK(hooks.arm(e,r,gen)==0);std::array<uint8_t,2>b{1,2};auto result=hooks.effect(e,r,{e->pipe_rdma_region,e->pipe_rdma_region_size},b,{});CHECK(result.bytes==2&&e->pipe_bytes_delivered==65536);hooks.release(e,r,result);
 e->pipe_bytes_received=e->pipe_bytes_delivered=UINT64_MAX-1;flags=s_ipc_lock.lock_irqsave();r=append(e,4);s_ipc_lock.unlock_irqrestore(flags);CHECK(hooks.arm(e,r,gen)!=0);retire(e);}
}
int main(){native_prefixes();rejection_matrix();close_and_reservations();reclaim_busy();frontend();ordinary_growth_positive();preparation_matrix();message_reservation();published_cleanup_trace();concurrent_reservation();effect_requalification();wrap_and_exhaustion();
 CHECK(!fixture::guards&&!g_secure_retired_exports&&epoch.active_workers==0);std::printf("secure IPC actual-source PASS cases=%u\n",fixture::cases);}
'''


def make_tu(source):
    wki = WKI.read_text()
    declarations = 'struct WkiChannel;\n' + decl(wki, 'WkiChannelIdentity') + '\n' + decl(wki, 'WkiWaitEntry')
    for name in ['WKI_OK', 'WKI_ERR_NO_MEM', 'WKI_ERR_TIMEOUT', 'WKI_ERR_INVALID', 'WKI_ERR_NOT_FOUND', 'WKI_ERR_BUSY', 'WKI_ERR_NO_CREDITS', 'WKI_ERR_TX_FAILED']:
        declarations += '\n' + re.search(r'constexpr[^\n]*\b' + name + r'\b[^\n]*;', wki)[0]
    header = HPP.read_text()
    declarations2 = '\n'.join(decl(header, n) for n in ['ProxyIpcState', 'WkiIpcChannels', 'WkiIpcExport'])
    names = ['WKI_IPC_MAX_UNANSWERED_CONTROLS', 'WKI_IPC_PIPE_REGION_SIZE', 'WKI_IPC_MESSAGE_PIPE_WINDOW', 'WKI_IPC_PIPE_RDMA_CAPACITY', 'WKI_IPC_PIPE_RDMA_TOTAL_SIZE', 'WKI_IPC_PIPE_DATA_HEADER_SIZE', 'WKI_IPC_PIPE_DATA_MAX_CHUNK', 'WKI_IPC_EXPORT_PIPE_CAPACITY', 'WKI_IPC_EXPORT_PIPE_WRITE_BURST_CALLS', 'WKI_IPC_O_NONBLOCK', 'IPC_SECURE_VERSION', 'IPC_SECURE_QUERY', 'IPC_SECURE_APPEND']
    constants = '\n'.join(re.search(r'constexpr[^=;\n]*\b' + n + r'\s*=[^\n]*;', source + header)[0] for n in names)
    constants += '\n' + '\n'.join(decl(source, n) for n in ['IpcPeerCleanupEpoch', 'IpcSecureIntent', 'IpcSecureSnapshot'])
    functions = '\n'.join(function(source, n) for n in ['export_has_write_backlog_locked', 'export_can_retire_locked', 'compact_inactive_exports_locked', 'publish_export_pipe_write_chunk_locked', 'queue_export_pipe_write_data', 'ipc_channels_live', 'init_nonblocking_pipe_write_view', 'export_pipe_write_bounded', 'export_pipe_write_nonblocking', 'release_export_pipe_rdma', 'free_retired_exports', 'service_secure_retired_exports', 'ipc_secure_same_request', 'ipc_secure_intent', 'ipc_secure_credit_locked', 'ipc_secure_arm', 'ipc_secure_effect', 'ipc_secure_release'])
    functions += '\nconstexpr WkiSecureRdmaHooks IPC_SECURE_HOOKS{ipc_secure_arm,ipc_secure_effect,ipc_secure_release};\n'
    functions += '\n'.join(function(source, n) for n in ['prepare_export_pipe_rdma', 'acquire_proxy_pipe_rdma_writer', 'release_proxy_pipe_rdma_writer', 'proxy_pipe_write_rdma'])
    functions += '\n' + decl(source, 'ProxyPipeWriterLoan') + '\n' + function(source, 'proxy_pipe_write')
    return PRELUDE.replace('/* actual declarations */', declarations).replace('/* actual native declarations */', declarations2).replace('/* actual constants/types */', constants).replace('/* actual functions */', functions)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, default=CPP)
    parser.add_argument('--keep', type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='wki-secure-ipc-') as tmp:
        out = args.keep or Path(tmp)
        out.mkdir(parents=True, exist_ok=True)
        tu = out / 'ipc-tu.cpp'
        tu.write_text(make_tu(args.source.read_text()))
        command = ['/usr/bin/clang++', '-std=c++23', '-Wall', '-Wextra', '-g', '-O1', '-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-fno-pie', '-no-pie', '-pthread', '-I' + str(KERN), str(tu), '-o', str(out / 'ipc-test')]
        compiled = subprocess.run(command, capture_output=True, text=True)
        (out / 'compile.log').write_text(compiled.stdout + compiled.stderr)
        env = os.environ | {'ASAN_OPTIONS': 'detect_leaks=0', 'UBSAN_OPTIONS': 'halt_on_error=1'}
        run = subprocess.run([str(out / 'ipc-test')], capture_output=True, text=True, env=env) if compiled.returncode == 0 else None
        if run:
            (out / 'run.log').write_text(run.stdout + run.stderr)
        receipt = {'compile_exit': compiled.returncode, 'run_exit': run.returncode if run else None, 'source': str(args.source), 'source_sha256': hashlib.sha256(args.source.read_bytes()).hexdigest(), 'fixture_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'command': command}
        (out / 'result.json').write_text(json.dumps(receipt, indent=2) + '\n')
        print((compiled.stdout + compiled.stderr) if compiled.returncode else (run.stdout + run.stderr))
        return compiled.returncode or run.returncode


if __name__ == '__main__':
    raise SystemExit(main())
