#!/usr/bin/env python3
"""Actual complete secure-RDMA engine with private platform/core-endpoint seams.

No production model/record/native-effect logic is substituted. Core reliable
control and typed transport endpoints emulate an independently keyed peer.
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
ENGINE = KERN / 'net/wki/secure_rdma.cpp'
WKI = KERN / 'net/wki/wki.cpp'
SCHEDULER = KERN / 'platform/sched/scheduler.hpp'
SHIMS = {
'platform/sys/spinlock.hpp': r'''#pragma once
#include <atomic>
namespace ker::mod::sys { class Spinlock { std::atomic_flag held=ATOMIC_FLAG_INIT; unsigned test_try=0;void(*before_try)()=nullptr; public:
// A finite platform seam injects guard contention at a selected actual call.
void fail_after_tries(unsigned count){test_try=count;}
void before_next_try(void(*hook)()){before_try=hook;}
bool try_lock(){if(before_try){auto hook=before_try;before_try=nullptr;hook();}if(test_try!=0&&--test_try==0)return false;return !held.test_and_set(std::memory_order_acquire);} void lock(){while(!try_lock()){} }
void unlock(){held.clear(std::memory_order_release);} }; }
''',
'platform/sched/task.hpp': r'''#pragma once
#include <atomic>
#include <cstdint>
namespace ker::mod::sched::task {
enum class TaskType {PROCESS,DAEMON,IDLE}; enum class TaskState {ACTIVE,EXITING,DEAD};
enum class WaitChannelKind {NONE,GENERIC,WKI_EXECVE_PROXY};
struct Task { uint64_t pid=17; TaskType type=TaskType::DAEMON; std::atomic<TaskState> state{TaskState::ACTIVE};
std::atomic<uint64_t> death_epoch{0}; std::atomic<unsigned> refs{1};
using WaitChannelKind=ker::mod::sched::task::WaitChannelKind;
uint64_t syscall_account_start_us=0;uint32_t preempt_disable_depth=0,wki_proxy_task_id=0;
uint64_t wake_at_us=0,perf_wait_callsite=0;
const char* wait_channel=nullptr;WaitChannelKind wait_channel_kind=WaitChannelKind::NONE;
bool deferred_task_switch=false,yield_switch=false,wants_block=false,voluntary=false,signal=false,wki_proxy_task=false;
std::atomic<bool> wakeup_pending{false};
std::atomic<bool> scheduler_transition_active{false};
bool is_voluntary_blocked()const{return voluntary;}bool has_interrupting_signal_pending(){return signal;}
bool wait_channel_is(WaitChannelKind kind)const{return wait_channel!=nullptr&&wait_channel_kind==kind;}
void set_wait_channel(const char*name,WaitChannelKind kind=WaitChannelKind::GENERIC){wait_channel=name;wait_channel_kind=kind;}
void clear_wait_channel(){set_wait_channel(nullptr,WaitChannelKind::NONE);}
bool try_acquire(){if(state.load()!=TaskState::ACTIVE)return false;refs++;return true;}
bool try_acquire_lifetime_ref(){refs++;return true;} void release(){refs--;}
static Task* create_kernel_thread(const char*,void(*)()); };
void destroy_unpublished_user_thread(Task*);
}
''',
'platform/sched/scheduler.hpp': r'''#pragma once
#include <platform/sched/task.hpp>
namespace ker::mod::sched { struct KernelShutdownHold{~KernelShutdownHold(){}}; inline auto hold_kernel_shutdown()->KernelShutdownHold{return {};}
auto get_current_task()->task::Task*; bool post_task_balanced(task::Task*);void kern_wake(task::Task*);
unsigned preempt_count();void kern_yield();void kern_sleep_us(uint64_t);void preemptible_syscall_park(const char*,uint64_t);
bool kernel_threads_shutdown_requested();[[noreturn]]void exit_current_kernel_thread(); bool has_run_queues();
struct SchedulerCpuState{uint64_t current_pid=0,handoff_pid=0;};auto get_scheduler_cpu_state(uint64_t)->SchedulerCpuState;
}
''',
'platform/sched/epoch.hpp': r'''#pragma once
#include <cstdint>
namespace ker::mod::sched {struct EpochManager{static bool is_safe_to_reclaim(uint64_t);};}
''',
'platform/smt/smt.hpp': r'''#pragma once
#include <cstdint>
namespace ker::mod::smt {auto get_core_count()->uint64_t;}
''',
'platform/init/limine_requests.hpp': r'''#pragma once
namespace ker::init {auto get_kernel_cmdline()->const char*;}
''',
'platform/mm/phys.hpp': r'''#pragma once
#include <cstddef>
namespace ker::mod::mm {enum class PhysicalPageOwner{WKI_ZONE};namespace phys {auto page_alloc(PhysicalPageOwner,size_t,const char*)->void*;void page_free(void*);}}
''',
'platform/random/entropy.hpp': r'''#pragma once
#include <cstddef>
namespace ker::mod::random::entropy {bool get_bytes(void*,size_t);}
''',
'platform/dbg/dbg.hpp': r'''#pragma once
#include <cstddef>
namespace ker::mod::dbg {template<size_t N>struct FixedString{char value[N];constexpr FixedString(const char(&s)[N]){for(size_t i=0;i<N;++i)value[i]=s[i];}};
void test_warn(const char*,...);template<FixedString Tag>struct logger{template<class...Args>static void warn(const char*f,Args...args){test_warn(f,args...);}};}
''',
}
HARNESS = r'''
#include <cstdio>
#include <cstdarg>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <stdexcept>
#include <array>
#include <atomic>
#define private public
#include <net/wki/rdma_registry.hpp>
#include <net/wki/rdma_session.hpp>
#include <net/wki/rdma_operations.hpp>
#undef private
#include "@ENGINE@"
using namespace ker::net::wki;
namespace p=ker::net::wki::rdma_protocol;
namespace sc=ker::mod::sched;
static unsigned checks=0;
#define CHECK(x) do {++checks;if(!(x)){std::fprintf(stderr,"FAIL:%d:%s\n",__LINE__,#x);std::abort();}}while(false)
static thread_local sc::task::Task* current=nullptr;
static sc::task::Task caller,worker;
static uint64_t now=1'000'000;
static bool safe_epoch=true,deny_rx=false,post_ok=true;
static uint64_t current_cpu_pid=0;
static const char* profile="wki.secure_rdma";
static WkiChannel control,data,ipc_data;
// Finite channel-pool endpoint seam: negotiation may create one fixed IPC slot;
// lookup never allocates and hostile descriptor channel IDs stay absent.
static bool ipc_create_ok=true,observe_ipc_offer=false,defer_ready=false,fail_confirm_commit=false;
static unsigned ipc_create_calls=0,channel_get_calls=0,offer_controls=0,confirm_controls=0;
static WkiTransport provider{};
static rdma_session::Session remote_session;
static p::Offer remote_offer{},local_offer{};
static bool remote_begun=false;
static uint64_t remote_inc=9001;
static unsigned publications=0,arm_controls=0,wake_calls=0;
static unsigned park_calls=0,halt_calls=0,yield_calls=0,timer_rechecks=0,park_with_signal=0;
static bool test_interrupts=false,signal_after_arm=false;
static bool hold_engine_at_record_ingress=false;
static std::vector<std::string> task_gate_logs;
static bool entropy_ok=true,allocation_ok=true;
static p::Transport kind=p::Transport::IVSHMEM;
static int publish_failure=0;
static bool drop_result=false,wrong_result=false,echo_grants=false,revoke_error=false,revoke_after_write=false,result_pressure=false;
static unsigned release_data_on_pump=0;
static unsigned revoke_publications=0,native_results=0;
static rdma_control::Result emitted_result{};
static uint32_t remote_prefix=UINT32_MAX;
static std::vector<uint8_t> remote_bytes(16384,0x5a);
static rdma_control::Arm remote_arm{};
static std::span<uint8_t> mutate_after_arm{};
static int remote_status=0;static uint8_t remote_outcome=static_cast<uint8_t>(WkiSecureRdmaOutcome::EXACT);
static bool block_effect=false,effect_entered=false,effect_continue=false;
static std::mutex effect_mutex;static std::condition_variable effect_cv;
static p::Coverage remote_coverage{};
static p::RegionDescriptor remote_descriptor{};
static void pump();
static unsigned receive_domain();
namespace ker::init {auto get_kernel_cmdline()->const char*{return profile;}}
namespace ker::mod::dbg {void test_warn(const char*format,...){char text[512]{};va_list args;va_start(args,format);int n=std::vsnprintf(text,sizeof(text),format,args);va_end(args);CHECK(n>0&&static_cast<size_t>(n)<sizeof(text));task_gate_logs.emplace_back(text);}}
namespace ker::mod::random::entropy {bool get_bytes(void*ptr,size_t n){if(!entropy_ok)return false;static unsigned seed=1;auto*b=static_cast<uint8_t*>(ptr);for(size_t i=0;i<n;i++)b[i]=static_cast<uint8_t>(++seed);return true;}}
namespace ker::mod::mm::phys {auto page_alloc(PhysicalPageOwner,size_t n,const char*)->void*{return allocation_ok?std::malloc(n):nullptr;}void page_free(void*p){std::free(p);}}
namespace ker::mod::smt {auto get_core_count()->uint64_t{return 1;}}
namespace ker::mod::sched {
auto get_current_task()->task::Task*{return current;}bool post_task_balanced(task::Task*){return post_ok;}
void kern_wake(task::Task*){wake_calls++;} unsigned preempt_count(){return current!=nullptr?current->preempt_disable_depth:0;}
void kern_yield(){yield_calls++;if(current==&worker)now+=1000;else pump();}
void kern_sleep_us(uint64_t n){if(current==&worker)now+=n;else pump();}
// Privileged IF/HLT, timer dispatch and runqueue accounting are platform seams.
// The PROCESS park/cancel/restore bodies below come from the actual scheduler.
bool interrupts_enabled(){return test_interrupts;}void test_irq(bool enabled){test_interrupts=enabled;}
void request_local_timer_recheck(){timer_rechecks++;}
void note_preempt_disabled_block(const char*,uint64_t){CHECK(false);}
bool begin_kernel_wait_accounting(task::Task*t){bool previous=t!=nullptr&&t->voluntary;if(t){t->voluntary=true;if(t->type==task::TaskType::PROCESS)t->syscall_account_start_us=0;}return previous;}
void finish_kernel_wait_accounting(task::Task*t,bool previous){if(t){t->voluntary=previous;if(t->type==task::TaskType::PROCESS&&t->syscall_account_start_us==0)t->syscall_account_start_us=now;}}
void halt_once_preserving_interrupt_state(bool){CHECK(!test_interrupts);halt_calls++;auto*t=current;if(t){CHECK(t->wants_block&&t->voluntary&&t->wait_channel!=nullptr&&t->wake_at_us!=0);CHECK(t->wait_channel_kind==task::WaitChannelKind::GENERIC);}test_irq(true);pump();test_irq(false);if(t)t->wakeup_pending.store(true);}
@SCHEDULER_WAIT_SOURCE@
void preemptible_syscall_park(const char*name,uint64_t deadline){park_calls++;if(current&&current->signal)park_with_signal++;preemptible_syscall_park_impl(name,task::WaitChannelKind::GENERIC,deadline,73);}
bool kernel_threads_shutdown_requested(){return false;}[[noreturn]]void exit_current_kernel_thread(){throw std::runtime_error("clean exit");}
bool has_run_queues(){return true;}auto get_scheduler_cpu_state(uint64_t)->SchedulerCpuState{return {current_cpu_pid,0};}
bool EpochManager::is_safe_to_reclaim(uint64_t){return safe_epoch;}
namespace task {auto Task::create_kernel_thread(const char*,void(*)())->Task*{return &worker;}void destroy_unpublished_user_thread(Task*){}}
}
namespace ker::net::wki {
WkiState g_wki;
auto wki_now_us()->uint64_t{return now;}
auto wki_peer_find(uint16_t id)->WkiPeer*{for(auto&v:g_wki.peers)if(v.node_id==id)return &v;return nullptr;}
bool wki_peer_lifecycle_rx_try_acquire(WkiPeer*v){if(!v||deny_rx)return false;v->lifecycle_state.fetch_add(1);return true;}
void wki_peer_lifecycle_rx_release(WkiPeer*v){v->lifecycle_state.fetch_sub(1);}
auto wki_channel_lookup(uint16_t peer,uint16_t channel)->WkiChannel*{if(peer!=2)return nullptr;return channel==4?&control:channel==40?&data:channel==WKI_CHAN_IPC_DATA&&ipc_data.active?&ipc_data:nullptr;}
auto wki_channel_get(uint16_t peer,uint16_t channel)->WkiChannel*{
++channel_get_calls;auto* found=wki_channel_lookup(peer,channel);if(found||peer!=control.peer_node_id||channel!=WKI_CHAN_IPC_DATA)return found;
++ipc_create_calls;if(!ipc_create_ok)return nullptr;ipc_data.peer_node_id=peer;ipc_data.channel_id=channel;ipc_data.generation=1;ipc_data.active=true;return &ipc_data;}
// Only protocol-worker identity is a seam; all classification/eligibility
// functions are extracted verbatim from the current WKI source.
static std::atomic<mod::sched::task::Task*> s_timer_deferred_task{nullptr};
@WKI_WAIT_SOURCE@
void wki_spin_yield_channel_identity(const WkiChannelIdentity&){}
}
static WkiChannelIdentity identity(){return {&data,2,40,data.generation};}
static WkiChannelIdentity control_identity(){return {&control,2,4,control.generation};}
static void ctrl(MsgType type,const void*payload,size_t size){WkiHeader h{};h.src_node=2;h.dst_node=1;h.channel_id=4;h.msg_type=static_cast<uint8_t>(type);wki_secure_rdma_control_ingress(&h,payload,static_cast<uint16_t>(size),control_identity());}
static bool deferred_read_data=false, discard_read_data=false, read_barrier_pending=false;
static unsigned read_barrier_observations=0;
static std::span<const uint8_t> read_caller_output{};
static void record(const p::Header&header,const void*plain,bool tamper=false){std::array<uint8_t,p::RECORD_BYTES> bytes{};CHECK(remote_session.send(header,plain,bytes.data(),bytes.size())==p::Drop::NONE);if(tamper)bytes[p::AAD_BYTES]^=1;
WkiRxMetadata metadata{};metadata.has_direct_peer=true;metadata.direct_peer=2;metadata.has_src_mac=true;metadata.src_mac=g_wki.peers[0].mac;wki_secure_rdma_rx_ingress(&provider,bytes.data(),static_cast<uint16_t>(p::HEADER_BYTES+header.payload_length),&metadata);}
static p::Header incoming(const rdma_control::Arm&arm,p::Opcode opcode,uint32_t off=0,uint32_t n=0,uint32_t total=UINT32_MAX,int status=0){p::Header h{};h.transport=static_cast<uint8_t>(kind);h.service=arm.grant.service;h.opcode=static_cast<uint8_t>(opcode);h.src_node=2;h.dst_node=1;h.channel_id=40;h.flags=(opcode==p::Opcode::READ_DATA||opcode==p::Opcode::READ_END||opcode==p::Opcode::WRITE_END)?p::FLAG_RESPONSE:arm.flags;h.session_id=arm.grant.session_id;h.region_generation=arm.grant.generation;h.binding_generation=arm.grant.binding_generation;h.capability=arm.grant.capability;h.rkey=arm.grant.rkey;h.operation_id=arm.operation_id;h.receive_ticket=arm.receive_ticket;h.region_offset=arm.offset;h.fragment_offset=off;h.payload_length=n;h.total_length=total==UINT32_MAX?arm.length:total;h.status=status;return h;}
namespace ker::net::wki {
auto wki_send_on_channel_identity(const WkiChannelIdentity&,MsgType type,const void*payload,uint16_t size)->int{
if(type==MsgType::SECURE_RDMA_GRANT&&echo_grants){CHECK(size==sizeof(p::RegionDescriptor));ctrl(type,payload,size);}
else if(type==MsgType::SECURE_RDMA_RESULT){CHECK(size==sizeof(emitted_result));std::memcpy(&emitted_result,payload,size);++native_results;}
else if(type==MsgType::SECURE_RDMA_REVOKE){CHECK(size==sizeof(p::RegionDescriptor));if(revoke_error)return WKI_ERR_TX_FAILED;revoke_publications++;if(echo_grants)ctrl(type,payload,size);}
else if(type==MsgType::SECURE_RDMA_OFFER){++offer_controls;if(observe_ipc_offer)CHECK(ipc_data.active&&ipc_data.channel_id==WKI_CHAN_IPC_DATA&&ipc_data.generation==1&&ipc_create_calls==1);p::Offer offer{};CHECK(size==sizeof(offer));std::memcpy(&offer,payload,size);if(!remote_begun||std::memcmp(&offer,&local_offer,sizeof(offer))!=0){if(remote_begun){CHECK(remote_session.retire()==p::Drop::NONE);++remote_inc;}
local_offer=offer;remote_offer={};remote_offer.transports=p::transport_bit(kind);remote_offer.services=p::SERVICES;remote_offer.record_bytes=p::RECORD_BYTES;remote_offer.session_id=offer.session_id;for(size_t i=0;i<8;i++)remote_offer.nonce[i]=static_cast<uint8_t>(remote_inc>>(i*8));remote_offer.nonce[8]=41;CHECK(remote_session.begin(2,1,remote_inc,remote_offer)==p::Drop::NONE);CHECK(remote_session.install(offer,g_wki.peers[0].auth_session.rx_key,g_wki.peers[0].auth_session.tx_key)==p::Drop::NONE);remote_begun=true;}
ctrl(MsgType::SECURE_RDMA_OFFER,&remote_offer,sizeof(remote_offer));rdma_control::Confirm c{};c.session_id=offer.session_id;c.transports=offer.transports&remote_offer.transports;c.services=offer.services&remote_offer.services;CHECK(p::negotiation_digest(2,1,remote_offer,offer,c.negotiation));ctrl(MsgType::SECURE_RDMA_CONFIRM,&c,sizeof(c));}
else if(type==MsgType::SECURE_RDMA_CONFIRM){++confirm_controls;if(fail_confirm_commit){fail_confirm_commit=false;s_lock.fail_after_tries(1);}}
else if(type==MsgType::SECURE_RDMA_ARM){arm_controls++;if(signal_after_arm)caller.signal=true;if(!mutate_after_arm.empty())std::fill(mutate_after_arm.begin(),mutate_after_arm.end(),0xff);CHECK(size==sizeof(remote_arm));std::memcpy(&remote_arm,payload,size);CHECK(p::coverage_begin(remote_coverage,remote_arm.length));rdma_control::ArmAck a{};a.request=remote_arm;ctrl(MsgType::SECURE_RDMA_ARM_ACK,&a,sizeof(a));}
return WKI_OK;}
int wki_transport_send_secure(WkiTransport*,uint16_t peer,const void*raw,uint16_t size){CHECK(peer==2);publications++;if(publish_failure)return publish_failure;
std::array<uint8_t,p::RECORD_BYTES> bytes{};std::memcpy(bytes.data(),raw,size);p::Header h{};CHECK(remote_session.receive(kind,bytes.data(),size,h)==p::Drop::NONE);
if(h.opcode==static_cast<uint8_t>(p::Opcode::WRITE)&&remote_arm.operation_id==h.operation_id){CHECK(p::coverage_admit(remote_coverage,h.fragment_offset,h.payload_length)==p::Drop::NONE);std::memcpy(remote_bytes.data()+h.fragment_offset,bytes.data()+p::HEADER_BYTES,h.payload_length);
if(p::coverage_complete(remote_coverage)){rdma_control::Result r{};r.request=remote_arm;if(wrong_result)r.request.receive_ticket++;r.bytes=remote_prefix==UINT32_MAX?remote_arm.length:remote_prefix;r.status=remote_status;r.outcome=remote_outcome;if(!drop_result)ctrl(MsgType::SECURE_RDMA_RESULT,&r,sizeof(r));if(revoke_after_write)ctrl(MsgType::SECURE_RDMA_REVOKE,&remote_descriptor,sizeof(remote_descriptor));if(result_pressure){data.lock.lock();release_data_on_pump=2;}record(incoming(remote_arm,p::Opcode::WRITE_END,0,0,remote_arm.length,r.bytes==remote_arm.length?0:-EIO),nullptr);}}
else if(h.opcode==static_cast<uint8_t>(p::Opcode::READ)&&remote_arm.operation_id==h.operation_id){uint32_t n=remote_prefix==UINT32_MAX?remote_arm.length:remote_prefix;
// The typed transport endpoint delivers actual independently encrypted records
// while the real engine table gate is contended. No ingress/decoder is replaced.
if(hold_engine_at_record_ingress)s_lock.lock();record(incoming(remote_arm,p::Opcode::READ_END,0,0,n),nullptr);
if(deferred_read_data)read_barrier_pending=true;
else for(uint32_t off=0;off<n;off+=p::FRAGMENT_BYTES){auto count=std::min(p::FRAGMENT_BYTES,n-off);record(incoming(remote_arm,p::Opcode::READ_DATA,off,count,n),remote_bytes.data()+off);}
if(hold_engine_at_record_ingress){CHECK(s_record_count==1+(n+p::FRAGMENT_BYTES-1)/p::FRAGMENT_BYTES);CHECK(s_peers[0].session.domains_[receive_domain()].replay.high==0);s_lock.unlock();}}
return WKI_OK;}
}
static void pump(){if(release_data_on_pump!=0&&--release_data_on_pump==0)data.lock.unlock();auto* saved=current;current=&worker;now+=1000;service_pass();current=saved;
if(read_barrier_pending&&s_record_count==0){
 // END has passed actual authentication/replay and the worker drainer. Test
 // completion between service passes, before any independently keyed DATA.
 CHECK(s_peers[0].session.domains_[receive_domain()].replay.high==1);
 auto job=std::find_if(s_jobs.begin(),s_jobs.end(),[](auto const&j){return j.arm.operation_id==remote_arm.operation_id&&j.state.load()!=JobState::FREE;});CHECK(job!=s_jobs.end());
 ops::Result result{};CHECK(s_operations.observe(job->operation,result)==p::Drop::PRESSURE);CHECK(job->state.load()!=JobState::COMPLETE);
 CHECK(!read_caller_output.empty()&&std::all_of(read_caller_output.begin(),read_caller_output.end(),[](auto b){return b==0xc3;}));
 if(++read_barrier_observations==4){read_barrier_pending=false;if(!discard_read_data){uint32_t n=remote_arm.length;for(uint32_t off=0;off<n;off+=p::FRAGMENT_BYTES){auto count=std::min(p::FRAGMENT_BYTES,n-off);record(incoming(remote_arm,p::Opcode::READ_DATA,off,count,n),remote_bytes.data()+off);}}}
}}
static void ready(){for(unsigned i=0;i<400&&!wki_secure_rdma_available(identity(),p::Service::VFS);i++)pump();CHECK(wki_secure_rdma_available(identity(),p::Service::VFS));}
static void setup(){caller.pid=17;worker.pid=18;current=&caller;g_wki.my_node_id=1;g_wki.initialized=true;auto&peer=g_wki.peers[0];peer.node_id=2;peer.transport=&provider;peer.is_direct=true;peer.state=PeerState::CONNECTED;peer.auth_session.active=true;peer.auth_session.generation=1;peer.auth_session.policy=UINT64_MAX;peer.auth_session.session_id[0]=77;peer.auth_session.tx_key.fill(11);peer.auth_session.rx_key.fill(22);
provider.registration_generation=7;provider.secure_kind=kind;provider.secure_tx=[](WkiTransport*,uint16_t,const void*,uint16_t){return WKI_OK;};provider.mtu=p::RECORD_BYTES;g_wki.transports=&provider;
control.active=true;control.peer_node_id=2;control.channel_id=4;control.generation=1;data.active=true;data.peer_node_id=2;data.channel_id=40;data.generation=1;CHECK(wki_secure_rdma_init());wki_secure_rdma_worker_start();if(s_services!=0&&post_ok&&!defer_ready)ready();}
static void install_remote(p::Service service,bool drain=true){remote_descriptor={};remote_descriptor.rkey=333;remote_descriptor.size=16384;remote_descriptor.generation=10;remote_descriptor.binding_generation=20;remote_descriptor.capability.fill(0x72);remote_descriptor.session_id=g_wki.peers[0].auth_session.session_id;remote_descriptor.channel_id=40;remote_descriptor.transport=static_cast<uint8_t>(kind);remote_descriptor.service=static_cast<uint8_t>(service);remote_descriptor.permissions=p::PERMIT_READ|p::PERMIT_WRITE;ctrl(MsgType::SECURE_RDMA_GRANT,&remote_descriptor,sizeof(remote_descriptor));if(!drain)return;pump();p::RegionDescriptor out{};CHECK(wki_secure_rdma_remote_grant(identity(),service,333,out));CHECK(std::memcmp(&out,&remote_descriptor,sizeof(out))==0);}
struct Native {std::vector<uint8_t> bytes=std::vector<uint8_t>(16384,0x6a);unsigned arms=0,effects=0,releases=0;bool close_in_effect=false;int arm_error=0;};
static int native_arm(void*context,const WkiSecureRdmaRequest&,uint64_t&gen){auto&n=*static_cast<Native*>(context);CHECK(s_lock.try_lock());s_lock.unlock();CHECK(g_wki.peers[0].lifecycle_state.load()==0);n.arms++;gen=77;return n.arm_error;}
static WkiSecureRdmaResult native_effect(void*context,const WkiSecureRdmaRequest&r,std::span<uint8_t>backing,std::span<const uint8_t>input,std::span<uint8_t>output){auto&n=*static_cast<Native*>(context);CHECK(s_lock.try_lock());s_lock.unlock();CHECK(g_wki.peers[0].lifecycle_state.load()==0);CHECK(backing.data()==n.bytes.data());n.effects++;if(block_effect){std::unique_lock<std::mutex>lock(effect_mutex);effect_entered=true;effect_cv.notify_all();effect_cv.wait(lock,[]{return effect_continue;});}if(n.close_in_effect){CHECK(wki_secure_rdma_unregister_region(r.grant)==-EBUSY);}
if(r.opcode==p::Opcode::WRITE){CHECK(input.size()==r.length);std::memcpy(backing.data()+r.offset,input.data(),input.size());}else {CHECK(output.size()==r.length);std::memcpy(output.data(),backing.data()+r.offset,output.size());}return {0,r.length,WkiSecureRdmaOutcome::EXACT};}
static void native_release(void*context,const WkiSecureRdmaRequest&,const WkiSecureRdmaResult&){auto&n=*static_cast<Native*>(context);CHECK(s_lock.try_lock());s_lock.unlock();CHECK(g_wki.peers[0].lifecycle_state.load()==0);n.releases++;}
static p::RegionDescriptor local_region(Native&n,p::Service service=p::Service::VFS){p::RegionDescriptor d{};CHECK(wki_secure_rdma_register_region(identity(),service,n.bytes.data(),static_cast<uint32_t>(n.bytes.size()),5,p::PERMIT_READ|p::PERMIT_WRITE,{native_arm,native_effect,native_release},&n,d)==0);CHECK(wki_secure_rdma_local_region_current(identity(),service,d)==0);return d;}
static rdma_control::Arm arm_for(p::RegionDescriptor d,uint64_t seq,uint32_t count=8193){rdma_control::Arm a{};a.grant=d;a.operation_id=(uint64_t{2}<<48)|seq;a.receive_ticket=(uint64_t{2}<<48)|(seq+1000);a.opcode=static_cast<uint8_t>(p::Opcode::WRITE);a.length=count;return a;}
static void enqueue_arm(rdma_control::Arm a){ctrl(MsgType::SECURE_RDMA_ARM,&a,sizeof(a));for(unsigned i=0;i<3;i++)pump();}
static void write_frag(rdma_control::Arm a,uint32_t off,uint32_t n,bool tamper=false){std::vector<uint8_t> input(n,0xa7);record(incoming(a,p::Opcode::WRITE,off,n),input.data(),tamper);pump();}
static void close_region(p::RegionDescriptor d){for(unsigned i=0;i<50;i++){int s=wki_secure_rdma_unregister_region(d);if(s==0)return;CHECK(s==-EBUSY||s==-EAGAIN);pump();}CHECK(false);}
static void finish(){bool had_worker=worker.refs.load()>1;if(had_worker)for(unsigned i=0;i<50;i++)pump();wki_secure_rdma_shutdown();auto* saved=current;if(had_worker){current=&worker;worker_body();current=saved;}for(unsigned i=0;i<50&&!wki_secure_rdma_shutdown();i++)pump();CHECK(wki_secure_rdma_shutdown());CHECK(provider.chaos_in_flight.load()==0);CHECK(worker.refs.load()==1);CHECK(std::none_of(s_regions.begin(),s_regions.end(),[](auto const&r){return r.revoke_pending;}));}
static void shutdown_operations_case(const std::string& mode){
 Native native;auto descriptor=local_region(native);
 ops::Statistics operations{};rdma_registry::Statistics registry{};
 if(mode=="shutdown-multiple-terminal"){
  for(uint64_t sequence=1;sequence<=ops::OPERATION_SLOTS;++sequence)enqueue_arm(arm_for(descriptor,sequence,8));
  CHECK(native.arms==ops::OPERATION_SLOTS&&native.effects==0&&native.releases==0);
  CHECK(s_operations.statistics(operations)==p::Drop::NONE&&operations.live==ops::OPERATION_SLOTS&&operations.handles==ops::OPERATION_SLOTS&&operations.work==0);
  CHECK(s_registry.statistics(registry)==p::Drop::NONE&&registry.references==ops::OPERATION_SLOTS&&registry.claims==0);
  // Distinct full-slot sentinels prove erasure, not just zero initial storage.
  for(size_t slot=0;slot<ops::OPERATION_SLOTS;++slot)std::memset(s_scratch+slot*p::MAX_TRANSFER_BYTES,static_cast<int>(slot+1),p::MAX_TRANSFER_BYTES);
  // Peer0 retires after cancellation+handle release; one erase per pass still
  // leaves six terminal models at the old worker's first stop opportunity.
  s_peer_cursor=WKI_MAX_PEERS-2*WORK_BUDGET;
  CHECK(!wki_secure_rdma_shutdown());auto* saved=current;current=&worker;worker_body();current=saved;
  CHECK(s_operations.statistics(operations)==p::Drop::NONE&&operations.live==0);
  CHECK(native.releases==ops::OPERATION_SLOTS&&native.effects==0);
 }else if(mode=="shutdown-operation-pressure"){
  CHECK(!wki_secure_rdma_shutdown());for(unsigned pass=0;pass<WKI_MAX_PEERS/WORK_BUDGET;++pass)pump();
  CHECK(std::none_of(s_peers.begin(),s_peers.end(),[](auto const&peer){return peer.begun;}));
  CHECK(s_operations.statistics(operations)==p::Drop::NONE&&operations.live==0);
  CHECK(!s_operations.guard_.test_and_set());
  {EngineGuard const guard(s_lock);CHECK(guard);CHECK(!drained_locked());}
  CHECK(!s_worker_stopped.load()&&s_worker==&worker&&worker.refs.load()==2&&s_scratch!=nullptr);
  CHECK(!wki_secure_rdma_shutdown());s_operations.guard_.clear();
  auto* saved=current;current=&worker;worker_body();current=saved;
 }else{
  auto arm=arm_for(descriptor,1,8);enqueue_arm(arm);
  auto found=std::find_if(s_jobs.begin(),s_jobs.end(),[&](auto const&job){return job.arm.operation_id==arm.operation_id;});CHECK(found!=s_jobs.end());
  std::memset(s_scratch,0xa9,ops::SCRATCH_BYTES);
  // Use actual cancellation and COMPLETE cleanup while retaining an explicit
  // model loan across the following full service passes and peer retirement.
  cancel_job(*found,-ECANCELED);CHECK(found->state.load()==JobState::COMPLETE);
  ops::Work loan;ops::EraseWork eraser;ops::Result result{};
  if(mode=="shutdown-result-loan")CHECK(s_operations.take_result(found->operation,loan,result)==p::Drop::NONE&&result.outcome==ops::Outcome::NO_EFFECT);
  else CHECK(mode=="shutdown-held-eraser");
  progress_job(*found,now);CHECK(found->state.load()==JobState::FREE);
  if(mode=="shutdown-held-eraser")CHECK(s_operations.begin_erase(eraser)==p::Drop::NONE&&eraser.buffer().size()==p::MAX_TRANSFER_BYTES);
  CHECK(!wki_secure_rdma_shutdown());for(unsigned pass=0;pass<WKI_MAX_PEERS/WORK_BUDGET;++pass)pump();
  CHECK(std::none_of(s_peers.begin(),s_peers.end(),[](auto const&peer){return peer.begun;}));
  CHECK(s_operations.statistics(operations)==p::Drop::NONE&&operations.live==1&&operations.handles==0&&operations.work==1);
  CHECK(s_registry.statistics(registry)==p::Drop::NONE&&registry.references==1&&registry.claims==0);
  CHECK(wki_secure_rdma_unregister_region(descriptor)==-EBUSY&&native.effects==0&&native.releases==1);
  {EngineGuard const guard(s_lock);CHECK(guard);CHECK(!s_eraser&&!drained_locked());}
  CHECK(std::all_of(s_operations.scratch_.begin(),s_operations.scratch_.begin()+p::MAX_TRANSFER_BYTES,[](auto byte){return byte==0xa9;}));
  if(loan){CHECK(operations.terminal==1&&result.bytes==0&&loan.staging().empty());loan.reset();}
  else {CHECK(operations.erasing==1);auto bytes=eraser.buffer();auth_crypto::secure_erase(bytes.data(),bytes.size());CHECK(s_operations.finish_erase(eraser)==p::Drop::NONE);}
  auto* saved=current;current=&worker;worker_body();current=saved;
 }
 CHECK(s_worker_stopped.load()&&s_worker==nullptr&&worker.refs.load()==1);
 CHECK(s_operations.statistics(operations)==p::Drop::NONE&&operations.live==0&&operations.handles==0&&operations.work==0&&operations.claimed==0&&operations.erasing==0);
 CHECK(s_registry.statistics(registry)==p::Drop::NONE&&registry.references==0&&registry.claims==0&&registry.live==1);
 CHECK(provider.chaos_in_flight.load()==0&&s_scratch!=nullptr);
 // Only allocated slots are guaranteed erased before final whole-scratch wipe.
 size_t const erased=mode=="shutdown-multiple-terminal"?ops::SCRATCH_BYTES:p::MAX_TRANSFER_BYTES;
 if(mode!="shutdown-operation-pressure")CHECK(std::all_of(s_scratch,s_scratch+erased,[](auto byte){return byte==0;}));
 CHECK(std::all_of(native.bytes.begin(),native.bytes.end(),[](auto byte){return byte==0x6a;}));
 CHECK(wki_secure_rdma_unregister_region(descriptor)==0);
 CHECK(s_registry.statistics(registry)==p::Drop::NONE&&registry.live==0&&registry.bytes==0);
 std::printf("shutdown drain: live=%u handles=%u work=%u refs=%u claims=%u native=%u/%u/%u provider=%u\n",operations.live,operations.handles,operations.work,registry.references,registry.claims,native.arms,native.effects,native.releases,provider.chaos_in_flight.load());
}
static unsigned receive_domain(){return (static_cast<unsigned>(kind)-1)*4+1;}
static bool test_record_decoded(const RecordCell& cell){
#if WKI_TEST_OWNED_RECORD_RETRY
return cell.decoded;
#else
static_cast<void>(cell);return false;
#endif
}
static void test_decode_scratch_erased(){
#if WKI_TEST_OWNED_RECORD_RETRY
CHECK(std::all_of(s_record_decode.begin(),s_record_decode.end(),[](auto b){return b==0;}));
#endif
}
static void test_record_cell_erased(const RecordCell& cell){
CHECK(cell.state==CellState::FREE&&cell.provider.transport==nullptr);CHECK(std::all_of(cell.record.begin(),cell.record.end(),[](auto b){return b==0;}));
#if WKI_TEST_OWNED_RECORD_RETRY
CHECK(!cell.decoded&&!cell.retry&&cell.secure_incarnation==0);auto* header=reinterpret_cast<const uint8_t*>(&cell.header);CHECK(std::all_of(header,header+sizeof(cell.header),[](auto b){return b==0;}));auto* binding=reinterpret_cast<const uint8_t*>(&cell.binding);CHECK(std::all_of(binding,binding+sizeof(cell.binding),[](auto b){return b==0;}));
#endif
}
static std::array<uint8_t,p::RECORD_BYTES> owned_record(rdma_control::Arm arm,uint32_t offset=0,uint32_t size=8,bool tamper=false){
std::array<uint8_t,p::RECORD_BYTES> bytes{};std::vector<uint8_t> input(size,0xa7);CHECK(remote_session.send(incoming(arm,p::Opcode::WRITE,offset,size),input.data(),bytes.data(),bytes.size())==p::Drop::NONE);if(tamper)bytes[p::AAD_BYTES]^=1;return bytes;
}
static void queue_owned_record(const std::array<uint8_t,p::RECORD_BYTES>& bytes,uint32_t size){WkiRxMetadata metadata{};metadata.has_direct_peer=true;metadata.direct_peer=2;metadata.has_src_mac=true;metadata.src_mac=g_wki.peers[0].mac;wki_secure_rdma_rx_ingress(&provider,bytes.data(),static_cast<uint16_t>(p::HEADER_BYTES+size),&metadata);}
static void drain_owned_record(){auto* saved=current;current=&worker;drain_rx(now);current=saved;test_decode_scratch_erased();}
static void record_pressure_case(const std::string& mode){
Native n;auto descriptor=local_region(n);bool fifo=mode=="record-fifo-pressure";uint32_t length=fifo?8193:8;auto arm=arm_for(descriptor,1,length);enqueue_arm(arm);CHECK(n.arms==1&&n.effects==0);
auto bytes=owned_record(arm,0,fifo?8192:8,mode=="record-invalid-pressure");queue_owned_record(bytes,fifo?8192:8);CHECK(s_record_count==1);auto* cell=&s_records[s_record_order[s_record_head]];auto original=cell->record;uint64_t initial_replay=s_peers[0].session.domains_[receive_domain()].replay.high;
if(fifo){auto tail=owned_record(arm,8192,1);queue_owned_record(tail,1);CHECK(s_record_count==2);}
bool operations=mode=="record-operation-pressure"||mode=="record-fifo-pressure"||mode=="record-auth-retirement"||mode=="record-provider-retirement"||mode=="record-channel-retirement"||mode=="record-region-retirement"||mode=="record-shutdown-denied"||mode=="record-peer-retirement"||mode=="record-incarnation-retirement";
if(mode=="record-peer-pressure")deny_rx=true;
else if(mode=="record-provider-pressure")g_wki.transport_lock.lock();
// Moving record-pop to its independent gate removes exactly one engine try.
// This source-bound scheduling seam changes only platform guard denial timing;
// the archived engine is compiled unchanged and retains its original schedule.
else if(mode=="record-engine-pressure")s_lock.fail_after_tries(3-WKI_TEST_RECORD_QUEUE);
else if(mode=="record-binding-pressure"||mode=="record-channel-capture-retirement")s_lock.fail_after_tries(4-WKI_TEST_RECORD_QUEUE);
else if(mode=="record-postdecode-pressure")s_lock.fail_after_tries(5-WKI_TEST_RECORD_QUEUE);
else if(mode=="record-channel-pressure")data.lock.lock();
else if(mode=="record-channel-postdecode")data.lock.fail_after_tries(3);
else if(mode=="record-session-pressure"||mode=="record-invalid-pressure")CHECK(!s_peers[0].session.guard_.test_and_set());
else {CHECK(operations);CHECK(!s_operations.guard_.test_and_set());}
drain_owned_record();CHECK(s_record_count==(fifo?2u:1u)&&cell->state==CellState::QUEUED&&cell->provider.transport==&provider);CHECK(n.effects==0&&n.releases==0&&provider.chaos_in_flight.load()==(fifo?3u:2u));
bool decoded=operations||mode=="record-postdecode-pressure"||mode=="record-channel-postdecode";CHECK(test_record_decoded(*cell)==decoded);
CHECK(s_peers[0].session.domains_[receive_domain()].replay.high==(decoded?initial_replay+1:initial_replay));
if(!decoded)CHECK(cell->record==original);else CHECK(std::all_of(cell->record.begin()+p::HEADER_BYTES,cell->record.begin()+p::HEADER_BYTES+8,[](auto b){return b==0xa7;}));
if(mode=="record-peer-pressure")deny_rx=false;
if(mode=="record-provider-pressure")g_wki.transport_lock.unlock();
if(mode=="record-channel-pressure")data.lock.unlock();
if(mode=="record-session-pressure"||mode=="record-invalid-pressure")s_peers[0].session.guard_.clear();
if(operations)s_operations.guard_.clear();
auto replay=s_peers[0].session.domains_[receive_domain()].replay;
bool retirement=mode.find("retirement")!=std::string::npos||mode=="record-shutdown-denied";
if(mode=="record-auth-retirement"){auto&peer=g_wki.peers[0];peer.auth_session.replay_lock.lock();wki_secure_rdma_retire_peer(&peer);++peer.auth_session.generation;++peer.auth_session.session_id[0];peer.auth_session.replay_lock.unlock();}
if(mode=="record-provider-retirement"){provider.chaos_unregistering.store(true);wki_secure_rdma_retire_transport(&provider,7);deny_rx=true;g_wki.transport_lock.lock();}
if(mode=="record-channel-retirement"||mode=="record-channel-capture-retirement"){data.lock.lock();++data.generation;data.lock.unlock();}
if(mode=="record-region-retirement")CHECK(wki_secure_rdma_unregister_region(descriptor)==-EBUSY);
if(mode=="record-peer-retirement"){wki_secure_rdma_retire_peer(&g_wki.peers[0]);deny_rx=true;}
if(mode=="record-incarnation-retirement"){
 // Schedule this already-owned cell after a complete same-SID renegotiation.
 // No cell bytes/binding/header are changed by this finite dispatch seam.
 uint64_t old_inc=s_peers[0].incarnation;CHECK(s_record_count==1);s_record_count=0;cell->state=CellState::PROCESSING;
 wki_secure_rdma_retire_peer(&g_wki.peers[0]);auto* saved=current;current=&worker;for(unsigned i=0;i<4;i++)for(auto&job:s_jobs)progress_job(job,now);maintain_peer(0,now);current=saved;ready();CHECK(s_peers[0].incarnation>old_inc);s_record_count=1;cell->state=CellState::QUEUED;
}
if(mode=="record-shutdown-denied"){deny_rx=true;CHECK(!wki_secure_rdma_shutdown());}
drain_owned_record();CHECK(s_record_count==0);test_record_cell_erased(*cell);CHECK(n.effects==0);
if(mode=="record-provider-retirement"){g_wki.transport_lock.unlock();deny_rx=false;}
if(mode=="record-peer-retirement"||mode=="record-shutdown-denied")deny_rx=false;
if(decoded&&!retirement){if(fifo)CHECK(s_peers[0].session.domains_[receive_domain()].replay.high==replay.high+1);else CHECK(std::memcmp(&replay,&s_peers[0].session.domains_[receive_domain()].replay,sizeof(replay))==0);}
if(mode=="record-invalid-pressure")ctrl(MsgType::SECURE_RDMA_CANCEL,&arm,sizeof(arm));
for(unsigned i=0;i<20;i++)pump();
if(retirement||mode=="record-invalid-pressure"){CHECK(n.effects==0&&n.releases==1&&native_results==0);}
else {CHECK(n.arms==1&&n.effects==1&&n.releases==1&&native_results==1&&emitted_result.status==0&&emitted_result.bytes==length&&emitted_result.outcome==static_cast<uint8_t>(WkiSecureRdmaOutcome::EXACT)&&same_arm(emitted_result.request,arm)&&std::all_of(n.bytes.begin(),n.bytes.begin()+length,[](auto b){return b==0xa7;}));queue_owned_record(bytes,fifo?8192:8);drain_owned_record();CHECK(s_record_count==0&&s_drops[static_cast<size_t>(p::Drop::DUPLICATE)].load()>0&&n.effects==1);}
if(mode=="record-invalid-pressure")CHECK(s_drops[static_cast<size_t>(p::Drop::AUTHENTICATION)].load()==1);
close_region(descriptor);
}
static auto record_queue_gate()->ker::mod::sys::Spinlock&{
#if WKI_TEST_RECORD_QUEUE
return s_record_lock;
#else
// Layout/observation compatibility only; archived production is never edited.
return s_lock;
#endif
}
static void ingress_queue_case(const std::string& mode){
if(mode=="ingress-read-engine-pressure"){
 install_remote(p::Service::VFS);hold_engine_at_record_ingress=true;
 std::vector<uint8_t> output(8193,0xc3);auto r=wki_secure_rdma_read(identity(),remote_descriptor,0,output);
 CHECK(r.status==0&&r.bytes==8193&&r.outcome==WkiSecureRdmaOutcome::EXACT);
 CHECK(std::all_of(output.begin(),output.end(),[](auto b){return b==0x5a;}));
 CHECK(publications==1&&arm_controls==1&&s_record_count==0);
 for(unsigned i=0;i<3;++i)test_record_cell_erased(s_records[i]);test_decode_scratch_erased();
 for(unsigned i=0;i<10;++i)pump();CHECK(provider.chaos_in_flight.load()==0);return;
}
if(mode=="ingress-unbegun"){
 CHECK(!s_peers[0].begun&&s_peers[0].closed.load());std::array<uint8_t,p::RECORD_BYTES> bytes{};
 p::Header hint{};hint.src_node=2;hint.dst_node=1;hint.session_id=g_wki.peers[0].auth_session.session_id;
 std::memcpy(bytes.data(),&hint,sizeof(hint));s_lock.lock();queue_owned_record(bytes,0);s_lock.unlock();
 CHECK(s_record_count==0&&provider.chaos_in_flight.load()==0&&s_drops[static_cast<size_t>(p::Drop::SESSION)].load()==1);
 CHECK(publications==0&&arm_controls==0);ready();return;
}
Native n;bool fifo=mode=="ingress-fifo-budget",capacity=mode=="ingress-capacity";
uint32_t length=capacity?8193:8,chunk=capacity?8192:8;auto descriptor=local_region(n);auto arm=arm_for(descriptor,1,length);
enqueue_arm(arm);CHECK(n.arms==1&&n.effects==0&&n.releases==0);auto bytes=owned_record(arm,0,chunk);
auto original=bytes;auto replay=s_peers[0].session.domains_[receive_domain()].replay;
auto sid=g_wki.peers[0].auth_session.session_id;uint32_t authgen=g_wki.peers[0].auth_session.generation;
uint64_t incarnation=s_peers[0].incarnation;uint32_t pins=provider.chaos_in_flight.load();CHECK(pins==1);
if(mode=="ingress-peer-pressure"||mode=="ingress-provider-pressure"||mode=="ingress-queue-pressure"){
 if(mode=="ingress-peer-pressure")deny_rx=true;
 else if(mode=="ingress-provider-pressure")g_wki.transport_lock.lock();
 else record_queue_gate().lock();
 queue_owned_record(bytes,chunk);CHECK(s_record_count==0&&provider.chaos_in_flight.load()==pins&&n.effects==0);
 auto reason=mode=="ingress-queue-pressure"?p::Drop::PRESSURE:p::Drop::IDENTITY;
 CHECK(s_drops[static_cast<size_t>(reason)].load()==1);
 if(mode=="ingress-peer-pressure")deny_rx=false;
 else if(mode=="ingress-provider-pressure")g_wki.transport_lock.unlock();
 else record_queue_gate().unlock();
}
if(mode=="ingress-channel-pressure")data.lock.lock();
if(mode=="ingress-unconfirmed")s_peers[0].confirmed=false;
if(mode=="ingress-closed")wki_secure_rdma_retire_peer(&g_wki.peers[0]);
// This is the semantic regression point: real public ingress executes while
// the production table gate is held; the saved 0bff engine must fail queue1.
s_lock.lock();
// One finite scheduling hook closes admission after initial checks/provider
// pinning but immediately before the actual queue tryguard. It executes before
// acquisition, never from inside a production queue critical section.
if(mode=="ingress-close-recheck")record_queue_gate().before_next_try([]{wki_secure_rdma_retire_peer(&g_wki.peers[0]);});
if(mode=="ingress-stop-recheck")record_queue_gate().before_next_try([]{CHECK(!wki_secure_rdma_shutdown());});
queue_owned_record(bytes,chunk);s_lock.unlock();
if(mode=="ingress-closed"||mode=="ingress-close-recheck"||mode=="ingress-stop-recheck"){
 CHECK(s_record_count==0&&provider.chaos_in_flight.load()==pins);for(unsigned i=0;i<20;++i)pump();
 CHECK(n.effects==0&&n.releases==1);close_region(descriptor);return;
}
CHECK(s_record_count==1&&provider.chaos_in_flight.load()==pins+1);
auto* cell=&s_records[s_record_order[s_record_head]];
CHECK(cell->record==original&&cell->sid==sid&&cell->auth_generation==authgen&&cell->secure_incarnation==incarnation);
CHECK(!test_record_decoded(*cell)&&n.effects==0&&n.releases==0);
CHECK(std::memcmp(&replay,&s_peers[0].session.domains_[receive_domain()].replay,sizeof(replay))==0);
bytes.fill(0xee);CHECK(cell->record==original); // caller/packet bytes never escape
if(mode=="ingress-fifo-budget"||mode=="ingress-capacity"){
 s_lock.lock();for(unsigned i=1;i<(fifo?9u:65u);++i){
  if(fifo){auto next=owned_record(arm,0,8);queue_owned_record(next,8);}
  else queue_owned_record(original,chunk);
 }s_lock.unlock();CHECK(s_record_count==(fifo?9u:64u));
 CHECK(provider.chaos_in_flight.load()==pins+(fifo?9u:64u));
 if(capacity)CHECK(s_drops[static_cast<size_t>(p::Drop::PRESSURE)].load()==1);
 if(fifo){
  auto order=s_record_order;size_t head=s_record_head;CHECK(!s_operations.guard_.test_and_set());
  drain_owned_record();CHECK(s_record_count==9&&s_record_head==head&&s_record_order==order&&test_record_decoded(*cell));
  CHECK(s_peers[0].session.domains_[receive_domain()].replay.high==replay.high+1);
  for(unsigned i=1;i<9;++i)CHECK(!test_record_decoded(s_records[order[(head+i)%RECORD_CELLS]]));
  s_operations.guard_.clear();drain_owned_record();CHECK(s_record_count==1&&n.effects==0);
  CHECK(s_peers[0].session.domains_[receive_domain()].replay.high==replay.high+8);
  for(unsigned i=0;i<8;++i)test_record_cell_erased(s_records[order[(head+i)%RECORD_CELLS]]);
 }else{
  for(unsigned i=0;i<8;++i)drain_owned_record();CHECK(s_record_count==0&&n.effects==0);
  CHECK(s_peers[0].session.domains_[receive_domain()].replay.high==replay.high+1);
  for(auto const& value:s_records)test_record_cell_erased(value);
  CHECK(provider.chaos_in_flight.load()==pins);auto tail=owned_record(arm,8192,1);queue_owned_record(tail,1);
 }
}
if(mode=="ingress-diagnostics-pressure"){
 WkiSecureRdmaDiagnostics output{};output.consumer_pins=777;
 record_queue_gate().lock();CHECK(!wki_secure_rdma_diagnostics(output)&&output.consumer_pins==777);
 s_lock.lock();CHECK(!drained_locked());s_lock.unlock();record_queue_gate().unlock();
 s_lock.lock();CHECK(!wki_secure_rdma_diagnostics(output)&&output.consumer_pins==777);s_lock.unlock();
 CHECK(wki_secure_rdma_diagnostics(output)&&output.queued_records==1&&output.consumer_pins==1);
}
if(mode=="ingress-channel-pressure"){
 drain_owned_record();CHECK(s_record_count==1&&!test_record_decoded(*cell)&&cell->record==original&&cell->binding.channel_cookie==0);
 CHECK(std::memcmp(&replay,&s_peers[0].session.domains_[receive_domain()].replay,sizeof(replay))==0);data.lock.unlock();
}
if(mode=="ingress-unconfirmed"){
 drain_owned_record();CHECK(s_record_count==0&&n.effects==0);test_record_cell_erased(*cell);
 CHECK(std::memcmp(&replay,&s_peers[0].session.domains_[receive_domain()].replay,sizeof(replay))==0);
 s_peers[0].confirmed=true;auto fresh=owned_record(arm);queue_owned_record(fresh,8);
 // The preconfirmation rejection committed no replay; test duplication of
 // the subsequently authenticated counter, rather than that unseen counter.
 original=fresh;
}
bool retired=mode=="ingress-auth-generation"||mode=="ingress-sid-retirement"||mode=="ingress-provider-generation"||
 mode=="ingress-provider-retirement"||mode=="ingress-incarnation-retirement"||mode=="ingress-channel-retirement"||mode=="ingress-shutdown-denied";
if(mode=="ingress-auth-generation")++g_wki.peers[0].auth_session.generation;
if(mode=="ingress-sid-retirement")++g_wki.peers[0].auth_session.session_id[0];
if(mode=="ingress-provider-generation")++provider.registration_generation;
if(mode=="ingress-provider-retirement"){
 provider.chaos_unregistering.store(true);wki_secure_rdma_retire_transport(&provider,7);deny_rx=true;g_wki.transport_lock.lock();
}
if(mode=="ingress-channel-retirement"){
 CHECK(!s_operations.guard_.test_and_set());drain_owned_record();CHECK(test_record_decoded(*cell)&&cell->binding.channel_generation==data.generation);
 ++data.generation;s_operations.guard_.clear();
}
if(mode=="ingress-incarnation-retirement"){
 // Scheduling seam retains the same captured cell across actual same-SID
 // retirement/negotiation. No ciphertext, binding or dispatch is substituted.
 record_queue_gate().lock();s_record_count=0;cell->state=CellState::PROCESSING;record_queue_gate().unlock();
 wki_secure_rdma_retire_peer(&g_wki.peers[0]);auto* saved=current;current=&worker;
 for(unsigned i=0;i<4;++i)for(auto&job:s_jobs)progress_job(job,now);maintain_peer(0,now);current=saved;ready();
 CHECK(s_peers[0].incarnation>incarnation&&s_peers[0].sid==sid&&cell->secure_incarnation==incarnation);
 record_queue_gate().lock();s_record_count=1;cell->state=CellState::QUEUED;record_queue_gate().unlock();
}
if(mode=="ingress-shutdown-denied"){deny_rx=true;CHECK(!wki_secure_rdma_shutdown());}
drain_owned_record();CHECK(s_record_count==0);test_record_cell_erased(*cell);
if(mode=="ingress-provider-retirement"){g_wki.transport_lock.unlock();deny_rx=false;}
if(mode=="ingress-shutdown-denied")deny_rx=false;
for(unsigned i=0;i<25;++i)pump();
if(retired){CHECK(n.effects==0&&n.releases==1&&native_results==0&&n.bytes[0]==0x6a);}
else {
 CHECK(n.arms==1&&n.effects==1&&n.releases==1&&native_results==1&&emitted_result.bytes==length&&emitted_result.status==0);
 CHECK(emitted_result.outcome==static_cast<uint8_t>(WkiSecureRdmaOutcome::EXACT)&&same_arm(emitted_result.request,arm));
 CHECK(std::all_of(n.bytes.begin(),n.bytes.begin()+length,[](auto b){return b==0xa7;}));
 auto duplicate_replay=s_peers[0].session.domains_[receive_domain()].replay;queue_owned_record(original,chunk);drain_owned_record();
 CHECK(s_record_count==0&&n.effects==1&&s_drops[static_cast<size_t>(p::Drop::DUPLICATE)].load()>0);
 CHECK(std::memcmp(&duplicate_replay,&s_peers[0].session.domains_[receive_domain()].replay,sizeof(duplicate_replay))==0);
}
close_region(descriptor);CHECK(provider.chaos_in_flight.load()==0);
for(auto const& value:s_records){CHECK(value.state==CellState::FREE&&value.provider.transport==nullptr);CHECK(std::all_of(value.record.begin(),value.record.end(),[](auto b){return b==0;}));}
}
static size_t remote_grants(){return std::count_if(s_remote.begin(),s_remote.end(),[](auto const& entry){return entry.live;});}
static p::RegionDescriptor readiness_grant(p::Service service=p::Service::VFS,uint16_t channel=40){
 p::RegionDescriptor grant{};grant.rkey=333;grant.size=16384;grant.generation=10;grant.binding_generation=20;
 grant.capability.fill(0x72);grant.session_id=g_wki.peers[0].auth_session.session_id;grant.channel_id=channel;
 grant.transport=static_cast<uint8_t>(kind);grant.service=static_cast<uint8_t>(service);grant.permissions=p::PERMIT_READ|p::PERMIT_WRITE;return grant;
}
static WkiChannelIdentity ipc_identity(){return {&ipc_data,control.peer_node_id,WKI_CHAN_IPC_DATA,ipc_data.generation};}
static rdma_control::Confirm current_confirm(){rdma_control::Confirm confirm{};auto const& context=s_peers[0];
 confirm.session_id=context.sid;confirm.negotiation=context.negotiation;confirm.transports=context.local.transports&context.remote.transports;
 confirm.services=context.local.services&context.remote.services;return confirm;
}
static void maintain_test_peer(){auto* saved=current;current=&worker;maintain_peer(0,now);current=saved;}
static void hold_confirmation_accounting(){
 // Real OFFER/CONFIRM handling installs and remotely confirms the context.
 // The send endpoint then denies exactly the accounting guard after an actual
 // accepted CONFIRM; no engine/session/dispatch method is replaced.
 for(unsigned i=0;i<200&&!(s_peers[0].installed&&s_peers[0].confirmed);++i)pump();
 CHECK(s_peers[0].installed&&s_peers[0].confirmed&&!s_peers[0].confirm_sent);
 unsigned sent=confirm_controls;fail_confirm_commit=true;maintain_test_peer();
 CHECK(!fail_confirm_commit&&confirm_controls==sent+1&&!s_peers[0].confirm_sent);
 drain_owned_record();CHECK(s_control_count==0&&s_peers[0].confirmed&&!s_peers[0].confirm_sent);
}
static void channel_readiness_case(const std::string& mode){
 if(mode=="ipc-fixed-readiness"){
  CHECK(ipc_create_calls==1&&ipc_data.active&&offer_controls>0&&confirm_controls>0);
  CHECK(wki_channel_lookup(2,WKI_CHAN_IPC_DATA)==&ipc_data&&s_peers[0].confirmed&&s_peers[0].confirm_sent);
  auto grant=readiness_grant(p::Service::IPC,WKI_CHAN_IPC_DATA);ctrl(MsgType::SECURE_RDMA_GRANT,&grant,sizeof(grant));
  CHECK(s_control_count==1&&remote_grants()==0);drain_owned_record();CHECK(s_control_count==0&&remote_grants()==1);
  p::RegionDescriptor found{};CHECK(wki_secure_rdma_remote_grant(ipc_identity(),p::Service::IPC,grant.rkey,found));CHECK(same_descriptor(found,grant));
  // The first simulated FD-import get happens only after this GRANT is usable.
  CHECK(ipc_create_calls==1);CHECK(wki_channel_get(2,WKI_CHAN_IPC_DATA)==&ipc_data&&ipc_create_calls==1);
 }else if(mode=="ipc-create-blocked"||mode=="ipc-capture-pressure"){
  uint64_t next=s_next_incarnation;for(unsigned i=0;i<8;++i)maintain_test_peer();
  CHECK(!s_peers[0].begun&&!remote_begun&&offer_controls==0&&confirm_controls==0&&s_next_incarnation==next);
  rdma_session::Statistics stats{};CHECK(s_peers[0].session.statistics(stats)==p::Drop::NONE&&!stats.begun&&stats.live_keys==0);
  if(mode=="ipc-create-blocked"){CHECK(ipc_create_calls==8&&!ipc_data.active);ipc_create_ok=true;}else ipc_data.lock.unlock();
  ready();CHECK(ipc_data.active&&offer_controls>0&&s_peers[0].confirm_sent);
 }else if(mode=="ipc-disabled-no-create"){
  CHECK((s_services&p::service_bit(p::Service::IPC))==0&&ipc_create_calls==0&&!ipc_data.active&&offer_controls>0);
 }else{
  hold_confirmation_accounting();auto grant=readiness_grant();
  bool terminal=mode=="early-grant-unconfirmed"||mode=="early-grant-malformed"||mode=="early-grant-unsupported-service"||mode=="early-grant-unsupported-transport"||mode=="early-grant-unknown-channel";
  if(mode=="early-grant-unconfirmed"||mode=="early-grant-malformed")s_peers[0].confirmed=false;
  if(mode=="early-grant-malformed")grant.reserved=1;
  if(mode=="early-grant-unsupported-transport")grant.transport=static_cast<uint8_t>(p::Transport::ROCE);
  if(mode=="early-grant-unknown-channel")grant.channel_id=199;
  unsigned gets=channel_get_calls;ctrl(MsgType::SECURE_RDMA_GRANT,&grant,sizeof(grant));
  auto* cell=&s_controls[s_control_order[s_control_head]];CHECK(s_control_count==1&&remote_grants()==0);
  if(mode=="early-grant-unconfirmed"||mode=="early-grant-malformed"){auto confirm=current_confirm();ctrl(MsgType::SECURE_RDMA_CONFIRM,&confirm,sizeof(confirm));CHECK(s_control_count==2);}
  drain_owned_record();CHECK(channel_get_calls==gets&&remote_grants()==0&&arm_controls==0&&publications==0&&native_results==0);
  if(terminal){CHECK(s_control_count==0&&cell->state==CellState::FREE&&!s_peers[0].confirm_sent);if(mode=="early-grant-unconfirmed"||mode=="early-grant-malformed")CHECK(s_peers[0].confirmed);}
  else{
   CHECK(s_control_count==1&&cell->state==CellState::QUEUED&&cell->retry&&std::memcmp(cell->payload.data(),&grant,sizeof(grant))==0);
   CHECK(!wki_secure_rdma_available(identity(),p::Service::VFS)&&provider.chaos_in_flight.load()==0);
   bool retire=mode!="early-confirm-grant";
   if(retire){
    // Finite retirement publications target exact fields captured by the real
    // pre-ACK admission; accounting completion alone grants no authority.
    s_peers[0].confirm_sent=true;
    if(mode=="early-grant-provider-retirement")++provider.registration_generation;
    else if(mode=="early-grant-auth-retirement"){wki_secure_rdma_retire_peer(&g_wki.peers[0]);++g_wki.peers[0].auth_session.generation;}
    else if(mode=="early-grant-sid-retirement"){wki_secure_rdma_retire_peer(&g_wki.peers[0]);g_wki.peers[0].auth_session.session_id[0]^=1;}
    else if(mode=="early-grant-incarnation-retirement")++s_peers[0].incarnation;
    else if(mode=="early-grant-control-retirement")++control.generation;
    else if(mode=="early-grant-channel-closed")data.active=false;
    else CHECK(false);
    drain_owned_record();CHECK(s_control_count==0&&cell->state==CellState::FREE&&remote_grants()==0);
   }else{
    uint64_t incarnation=s_peers[0].incarnation;now+=RETRY_US;maintain_test_peer();
    CHECK(s_peers[0].confirm_sent);drain_owned_record();CHECK(s_control_count==0&&cell->state==CellState::FREE&&remote_grants()==1);
    p::RegionDescriptor found{};CHECK(wki_secure_rdma_remote_grant(identity(),p::Service::VFS,grant.rkey,found)&&same_descriptor(found,grant));
    ctrl(MsgType::SECURE_RDMA_GRANT,&grant,sizeof(grant));drain_owned_record();CHECK(remote_grants()==1&&s_peers[0].incarnation==incarnation);
   }
  }
 }
 CHECK(arm_controls==0&&publications==0&&native_results==0&&provider.chaos_in_flight.load()==0);
}
int main(int argc,char**argv){CHECK(argc==2);std::string mode=argv[1];
if(mode=="task-gate-diagnostics")profile="wki.secure_rdma wki.chaos";
if(mode=="default-off")profile="";
if(mode=="post-failure")post_ok=false;
if(mode=="roce-read-write"||mode=="ingress-engine-roce"||mode=="read-end-barrier-roce")kind=p::Transport::ROCE;
if(mode=="ingress-unbegun")defer_ready=true;
if(mode.starts_with("early-grant-")||mode=="early-confirm-grant"||mode=="ipc-create-blocked"||mode=="ipc-capture-pressure")defer_ready=true;
if(mode=="early-grant-unsupported-service")profile="wki.secure_rdma wki.secure_rdma_vfs_off";
if(mode=="ipc-fixed-readiness")observe_ipc_offer=true;
if(mode=="ipc-create-blocked")ipc_create_ok=false;
if(mode=="ipc-capture-pressure"){ipc_data.active=true;ipc_data.peer_node_id=2;ipc_data.channel_id=WKI_CHAN_IPC_DATA;ipc_data.generation=1;ipc_data.lock.lock();}
if(mode=="ipc-disabled-no-create")profile="wki.secure_rdma wki.secure_rdma_ipc_off";
setup();
if(mode.starts_with("shutdown-"))shutdown_operations_case(mode);
else if(mode.starts_with("early-grant-")||mode=="early-confirm-grant"||mode=="ipc-fixed-readiness"||mode=="ipc-create-blocked"||mode=="ipc-capture-pressure"||mode=="ipc-disabled-no-create")channel_readiness_case(mode);
else if(mode.starts_with("ingress-"))ingress_queue_case(mode);
else if(mode=="outgoing-write"){install_remote(p::Service::VFS);std::vector<uint8_t> input(8193,0x49);mutate_after_arm=input;auto r=wki_secure_rdma_write(identity(),remote_descriptor,0,input);CHECK(r.status==0&&r.bytes==8193&&r.outcome==WkiSecureRdmaOutcome::EXACT);CHECK(std::all_of(remote_bytes.begin(),remote_bytes.begin()+8193,[](auto b){return b==0x49;}));CHECK(std::all_of(input.begin(),input.end(),[](auto b){return b==0xff;}));CHECK(arm_controls==1);}
else if(mode=="read-end-barrier"||mode=="read-end-barrier-roce"||mode=="read-end-timeout"){
 install_remote(p::Service::VFS);std::vector<uint8_t> output(8193,0xc3);read_caller_output=output;deferred_read_data=true;discard_read_data=mode=="read-end-timeout";
 auto r=wki_secure_rdma_read(identity(),remote_descriptor,0,output);CHECK(read_barrier_observations==4&&!read_barrier_pending&&publications==1&&arm_controls==1);
 if(discard_read_data){CHECK(r.status<0&&r.bytes==0&&std::all_of(output.begin(),output.end(),[](auto b){return b==0xc3;}));}
 else CHECK(r.status==0&&r.bytes==8193&&r.outcome==WkiSecureRdmaOutcome::EXACT&&std::all_of(output.begin(),output.end(),[](auto b){return b==0x5a;}));
 read_caller_output={};
}
else if(mode=="outgoing-read"){install_remote(p::Service::VFS);std::vector<uint8_t> output(8193,0xc3);remote_prefix=8193;auto r=wki_secure_rdma_read(identity(),remote_descriptor,0,output);CHECK(r.status==0&&r.bytes==8193&&r.outcome==WkiSecureRdmaOutcome::EXACT);CHECK(std::all_of(output.begin(),output.end(),[](auto b){return b==0x5a;}));}
else if(mode=="partial-ipc"){install_remote(p::Service::IPC);remote_prefix=4;std::array<uint8_t,8> input{};auto r=wki_secure_rdma_write(identity(),remote_descriptor,0,input);CHECK(r.status==0&&r.bytes==4&&r.outcome==WkiSecureRdmaOutcome::EXACT);}
else if(mode=="ipc-query-and-payload"||mode=="ipc-query-lost-payload"){
install_remote(p::Service::IPC);std::array<uint8_t,48> query{};query.fill(0xc3);auto r=wki_secure_rdma_read(identity(),remote_descriptor,0,query);CHECK(r.status==0&&r.bytes==48&&r.outcome==WkiSecureRdmaOutcome::EXACT&&std::all_of(query.begin(),query.end(),[](auto b){return b==0x5a;}));
drop_result=mode=="ipc-query-lost-payload";std::vector<uint8_t> input(4096,0xa7);r=wki_secure_rdma_write(identity(),remote_descriptor,0,input);
if(drop_result)CHECK(r.status<0&&r.outcome==WkiSecureRdmaOutcome::UNCERTAIN);else CHECK(r.status==0&&r.bytes==4096&&r.outcome==WkiSecureRdmaOutcome::EXACT);
CHECK(publications==2&&arm_controls==2&&std::all_of(remote_bytes.begin(),remote_bytes.begin()+4096,[](auto b){return b==0xa7;}));
}
else if(mode=="lost-result"||mode=="wrong-result"){install_remote(p::Service::VFS);drop_result=mode=="lost-result";wrong_result=mode=="wrong-result";std::array<uint8_t,8>input{};auto r=wki_secure_rdma_write(identity(),remote_descriptor,0,input);CHECK(r.status<0&&r.outcome==WkiSecureRdmaOutcome::UNCERTAIN);CHECK(publications==1);}
else if(mode=="provider-error"){install_remote(p::Service::VFS);publish_failure=WKI_ERR_TX_FAILED;std::array<uint8_t,8>input{};auto r=wki_secure_rdma_write(identity(),remote_descriptor,0,input);CHECK(r.status==-EIO&&r.outcome==WkiSecureRdmaOutcome::UNCERTAIN);}
else if(mode=="coverage-replay-close"){Native n;auto d=local_region(n);auto a=arm_for(d,1);enqueue_arm(a);CHECK(n.arms==1);enqueue_arm(a);CHECK(n.arms==1);write_frag(a,8192,1);CHECK(n.effects==0);write_frag(a,0,8192);for(unsigned i=0;i<8;i++)pump();CHECK(n.effects==1&&n.releases==1);enqueue_arm(a);write_frag(a,0,8192);CHECK(n.effects==1&&n.arms==1);CHECK(n.bytes[0]==0xa7&&n.bytes[8192]==0xa7);close_region(d);}
else if(mode=="tamper-unarmed"){Native n;auto d=local_region(n);auto a=arm_for(d,1,8);write_frag(a,0,8);CHECK(n.arms==0&&n.effects==0);enqueue_arm(a);write_frag(a,0,8,true);CHECK(n.effects==0);CHECK(n.bytes[0]==0x6a);ctrl(MsgType::SECURE_RDMA_CANCEL,&a,sizeof(a));for(unsigned i=0;i<8;i++)pump();CHECK(n.releases==1);close_region(d);}
else if(mode=="claim-close"){Native n;n.close_in_effect=true;auto d=local_region(n);auto a=arm_for(d,1,8);enqueue_arm(a);write_frag(a,0,8);for(unsigned i=0;i<8;i++)pump();CHECK(n.effects==1&&n.releases==1);close_region(d);}
else if(mode=="retire-drain"){Native n;auto d=local_region(n);auto a=arm_for(d,1,8);enqueue_arm(a);wki_secure_rdma_retire_peer(&g_wki.peers[0]);provider.chaos_unregistering.store(true);wki_secure_rdma_retire_transport(&provider,7);CHECK(wki_secure_rdma_unregister_region(d)==-EBUSY);for(unsigned i=0;i<12;i++)pump();CHECK(n.effects==0&&n.releases==1);close_region(d);CHECK(provider.chaos_in_flight.load()==0);}
else if(mode=="arm-malformed"){Native n;auto d=local_region(n);auto a=arm_for(d,1,8);a.operation_id=(uint64_t{1}<<48)|1;enqueue_arm(a);CHECK(n.arms==0);a=arm_for(d,2,8);a.native_intent[63]=1;enqueue_arm(a);CHECK(n.arms==0);close_region(d);}

else if(mode=="partial-vfs"){install_remote(p::Service::VFS);remote_prefix=3;std::array<uint8_t,8> input{};auto r=wki_secure_rdma_write(identity(),remote_descriptor,0,input);CHECK(r.status==0&&r.bytes==3&&r.outcome==WkiSecureRdmaOutcome::EXACT);}
else if(mode=="invalid-result"){install_remote(p::Service::BLOCK);remote_prefix=4;std::array<uint8_t,8>input{};auto r=wki_secure_rdma_write(identity(),remote_descriptor,0,input);CHECK(r.status<0&&r.outcome==WkiSecureRdmaOutcome::UNCERTAIN);}
else if(mode=="read-eof"){install_remote(p::Service::VFS);remote_prefix=0;std::array<uint8_t,8> output{};output.fill(0xc3);auto r=wki_secure_rdma_read(identity(),remote_descriptor,0,output);CHECK(r.status==0&&r.bytes==0&&r.outcome==WkiSecureRdmaOutcome::EXACT);CHECK(std::all_of(output.begin(),output.end(),[](auto b){return b==0xc3;}));}
else if(mode=="slots-pressure"){Native n;auto d=local_region(n);for(uint64_t i=1;i<=9;i++)enqueue_arm(arm_for(d,i,8));CHECK(n.arms==8&&n.effects==0);for(uint64_t i=1;i<=8;i++){auto a=arm_for(d,i,8);ctrl(MsgType::SECURE_RDMA_CANCEL,&a,sizeof(a));}for(unsigned i=0;i<25;i++)pump();CHECK(n.releases==8&&n.effects==0);close_region(d);}
else if(mode=="queue-pressure"){Native n;auto d=local_region(n);auto a=arm_for(d,1,8);std::array<uint8_t,8> input{};for(unsigned i=0;i<65;i++)record(incoming(a,p::Opcode::WRITE,0,8),input.data());WkiSecureRdmaDiagnostics diag{};CHECK(wki_secure_rdma_diagnostics(diag));CHECK(diag.queued_records==64);CHECK(provider.chaos_in_flight.load()==64);CHECK(diag.drops[static_cast<size_t>(p::Drop::PRESSURE)]>0);for(unsigned i=0;i<10;i++)pump();CHECK(n.effects==0&&provider.chaos_in_flight.load()==0);close_region(d);}
else if(mode=="owner-query"){Native n;auto d=local_region(n);deny_rx=true;CHECK(wki_secure_rdma_local_region_current(identity(),p::Service::VFS,d)==-EAGAIN);deny_rx=false;CHECK(g_wki.transport_lock.try_lock());CHECK(wki_secure_rdma_local_region_current(identity(),p::Service::VFS,d)==-EAGAIN);g_wki.transport_lock.unlock();provider.chaos_unregistering.store(true);CHECK(wki_secure_rdma_local_region_current(identity(),p::Service::VFS,d)==-ESTALE);close_region(d);}
else if(mode=="identity-exhaustion"){install_remote(p::Service::VFS);s_next_operation=NODE_SEQUENCE_MAX;std::array<uint8_t,8> input{};auto r=wki_secure_rdma_write(identity(),remote_descriptor,0,input);CHECK(r.status==0&&static_cast<uint16_t>(remote_arm.operation_id>>48)==1&&(remote_arm.operation_id&NODE_SEQUENCE_MAX)==NODE_SEQUENCE_MAX);r=wki_secure_rdma_write(identity(),remote_descriptor,0,input);CHECK(r.status==-EOVERFLOW&&r.outcome==WkiSecureRdmaOutcome::NO_EFFECT);}
else if(mode=="held-claim-rundown"){Native n;auto d=local_region(n);auto a=arm_for(d,1,8);enqueue_arm(a);block_effect=true;std::array<uint8_t,8> input{};record(incoming(a,p::Opcode::WRITE,0,8),input.data());std::thread service([]{current=&worker;service_pass();});{std::unique_lock<std::mutex>lock(effect_mutex);effect_cv.wait(lock,[]{return effect_entered;});}CHECK(wki_secure_rdma_unregister_region(d)==-EBUSY);wki_secure_rdma_retire_peer(&g_wki.peers[0]);provider.chaos_unregistering.store(true);wki_secure_rdma_retire_transport(&provider,7);CHECK(n.releases==0);WkiSecureRdmaDiagnostics diag{};CHECK(wki_secure_rdma_diagnostics(diag));CHECK(diag.region_claims==1&&diag.consumer_pins==1);{std::lock_guard<std::mutex>lock(effect_mutex);effect_continue=true;}effect_cv.notify_all();service.join();for(unsigned i=0;i<15;i++)pump();CHECK(n.effects==1&&n.releases==1);close_region(d);}
else if(mode=="abandoned-prepare"){install_remote(p::Service::VFS);auto&job=s_jobs[0];ProviderPin pin;rdma_registry::Binding binding{};CHECK(capture_authority(identity(),p::Service::VFS,p::Opcode::WRITE,0,binding,pin)==0);CHECK(caller.try_acquire());job.prepare_owner=&caller;job.binding=binding;job.provider=std::move(pin);job.outgoing=true;job.frontend=true;job.arm.grant=remote_descriptor;job.arm.operation_id=(uint64_t{1}<<48)|1;job.arm.receive_ticket=(uint64_t{1}<<48)|1;job.arm.opcode=static_cast<uint8_t>(p::Opcode::WRITE);job.arm.length=8;job.deadline=now+OPERATION_US;CHECK(s_operations.prepare(model_spec(job,ops::Role::SEND_WRITE),{},job.operation)==p::Drop::NONE);CHECK(s_operations.begin_prepare(job.operation,job.work)==p::Drop::NONE);job.state.store(JobState::RESERVED);caller.state.store(sc::task::TaskState::DEAD);caller.death_epoch.store(1);safe_epoch=false;pump();CHECK(job.state.load()==JobState::RESERVED&&caller.refs.load()==2);safe_epoch=true;current_cpu_pid=caller.pid;pump();CHECK(job.state.load()==JobState::RESERVED&&caller.refs.load()==2);current_cpu_pid=0;for(unsigned i=0;i<8;i++)pump();CHECK(job.state.load()==JobState::FREE&&caller.refs.load()==1&&publications==0);caller.state.store(sc::task::TaskState::ACTIVE);}
else if(mode=="result-loan-retention"){install_remote(p::Service::VFS);std::array<uint8_t,8> output{};auto r=wki_secure_rdma_read(identity(),remote_descriptor,0,output);CHECK(r.status==0);auto found=std::find_if(s_jobs.begin(),s_jobs.end(),[](auto const&j){return j.state.load()==JobState::COMPLETE;});CHECK(found!=s_jobs.end());ops::Work loan;ops::Result result{};CHECK(s_operations.take_result(found->operation,loan,result)==p::Drop::NONE);for(unsigned i=0;i<8;i++)pump();CHECK(std::all_of(loan.staging().begin(),loan.staging().end(),[](auto b){return b==0x5a;}));ops::Statistics stats{};CHECK(s_operations.statistics(stats)==p::Drop::NONE&&stats.work==1);loan.reset();for(unsigned i=0;i<8;i++)pump();CHECK(s_operations.statistics(stats)==p::Drop::NONE&&stats.live==0);}
else if(mode=="effect-pressure"){Native n;auto d=local_region(n);auto a=arm_for(d,1,8);enqueue_arm(a);std::array<uint8_t,8> input{};record(incoming(a,p::Opcode::WRITE,0,8),input.data());current=&worker;drain_rx(now);current=&caller;auto found=std::find_if(s_jobs.begin(),s_jobs.end(),[&](auto const&j){return j.arm.operation_id==a.operation_id&&j.state.load()==JobState::IN_WAIT;});CHECK(found!=s_jobs.end());data.lock.lock();current=&worker;progress_effect(*found);current=&caller;CHECK(!found->cancel.load()&&n.effects==0&&n.releases==0);data.lock.unlock();g_wki.transport_lock.lock();current=&worker;progress_job(*found,now);current=&caller;CHECK(!found->cancel.load()&&n.effects==0);g_wki.transport_lock.unlock();deny_rx=true;current=&worker;progress_effect(*found);current=&caller;CHECK(!found->cancel.load()&&n.effects==0);deny_rx=false;s_lock.lock();current=&worker;progress_effect(*found);current=&caller;CHECK(!found->cancel.load()&&n.effects==0);s_lock.unlock();for(unsigned i=0;i<10;i++)pump();CHECK(n.effects==1&&n.releases==1&&wki_secure_rdma_local_region_current(identity(),p::Service::VFS,d)==0);close_region(d);}
// Installed duplicate OFFERs consume one bounded pass without Session.install;
// channel/peer pressure above still retains every privately admitted control.
else if(mode.starts_with("record-"))record_pressure_case(mode);
else if(mode=="admission-pressure"){using A=WkiSecureRdmaControlRxAdmission;auto&peer=g_wki.peers[0];WkiHeader h{};h.src_node=2;h.dst_node=1;h.channel_id=4;h.msg_type=static_cast<uint8_t>(MsgType::SECURE_RDMA_OFFER);auto payload=remote_offer;unsigned wakes=wake_calls;CHECK(wki_peer_lifecycle_rx_try_acquire(&peer));control.lock.lock();s_lock.lock();CHECK(wki_secure_rdma_admit_control_rx(&h,&payload,sizeof(payload),control_identity(),&peer)==A::RETRY&&s_control_count==0);s_lock.unlock();for(unsigned i=0;i<64;i++)CHECK(wki_secure_rdma_admit_control_rx(&h,&payload,sizeof(payload),control_identity(),&peer)==A::QUEUED);CHECK(wki_secure_rdma_admit_control_rx(&h,&payload,sizeof(payload),control_identity(),&peer)==A::RETRY&&s_control_count==64&&wake_calls==wakes);payload.nonce[15]^=1;CHECK(std::memcmp(s_controls[s_control_order[s_control_head]].payload.data(),&remote_offer,sizeof(remote_offer))==0);CHECK(wki_secure_rdma_admit_control_rx(&h,&payload,0,control_identity(),&peer)==A::DISCARDED);h.msg_type=0xff;CHECK(wki_secure_rdma_admit_control_rx(&h,&payload,sizeof(payload),control_identity(),&peer)==A::NOT_APPLICABLE);current=&worker;drain_rx(now);current=&caller;CHECK(s_control_count==64);control.lock.unlock();wki_peer_lifecycle_rx_release(&peer);deny_rx=true;current=&worker;drain_rx(now);current=&caller;CHECK(s_control_count==64);deny_rx=false;CHECK(!s_peers[0].session.guard_.test_and_set());current=&worker;drain_rx(now);current=&caller;CHECK(s_control_count==64-WORK_BUDGET);s_peers[0].session.guard_.clear();for(unsigned i=0;i<12;i++)pump();CHECK(s_control_count==0&&wki_secure_rdma_available(identity(),p::Service::VFS));}
else if(mode=="grant-worker-pressure"){install_remote(p::Service::VFS,false);data.lock.lock();current=&worker;drain_rx(now);current=&caller;CHECK(s_control_count==1);data.lock.unlock();g_wki.transport_lock.lock();current=&worker;drain_rx(now);current=&caller;CHECK(s_control_count==1);g_wki.transport_lock.unlock();s_lock.lock();current=&worker;drain_rx(now);current=&caller;CHECK(s_control_count==1);s_lock.unlock();pump();p::RegionDescriptor out{};CHECK(wki_secure_rdma_remote_grant(identity(),p::Service::VFS,333,out)&&same_descriptor(out,remote_descriptor));}
else if(mode=="result-worker-pressure"){install_remote(p::Service::VFS);result_pressure=true;std::array<uint8_t,8> input{};auto r=wki_secure_rdma_write(identity(),remote_descriptor,0,input);CHECK(r.status==0&&r.bytes==8&&r.outcome==WkiSecureRdmaOutcome::EXACT&&publications==1&&release_data_on_pump==0);}
else if(mode=="queued-old-grant"){install_remote(p::Service::VFS,false);auto old=remote_descriptor;auto saved=s_controls[s_control_order[s_control_head]];provider.chaos_unregistering.store(true);wki_secure_rdma_retire_transport(&provider,7);for(unsigned i=0;i<300&&s_peers[0].begun;i++)pump();provider.registration_generation=8;provider.chaos_unregistering.store(false);ready();s_controls[0]=saved;s_controls[0].state=CellState::QUEUED;s_control_order[s_control_head]=0;s_control_count=1;pump();p::RegionDescriptor out{};CHECK(!wki_secure_rdma_remote_grant(identity(),p::Service::VFS,old.rkey,out));install_remote(p::Service::VFS);CHECK(wki_secure_rdma_remote_grant(identity(),p::Service::VFS,0,out,p::PERMIT_READ)&&same_descriptor(out,remote_descriptor));}
else if(mode=="revoke-discovery"||mode=="revoke-pressure"){echo_grants=true;Native first,second;auto old=local_region(first);for(unsigned i=0;i<4;i++)pump();p::RegionDescriptor discovered{};CHECK(wki_secure_rdma_remote_grant(identity(),p::Service::VFS,0,discovered,p::PERMIT_READ)&&same_descriptor(discovered,old));revoke_error=mode=="revoke-pressure";CHECK(wki_secure_rdma_unregister_region(old)==0);for(unsigned i=0;i<4;i++)pump();if(revoke_error){WkiSecureRdmaDiagnostics d{};CHECK(wki_secure_rdma_diagnostics(d)&&d.pending_revocations==1&&d.region_references==0&&d.region_claims==0);auto t=std::find_if(s_regions.begin(),s_regions.end(),[](auto const&r){return !r.live&&r.revoke_pending;});CHECK(t!=s_regions.end()&&t->context==nullptr&&t->hooks.arm==nullptr&&t->references==0);}
auto fresh=local_region(second);CHECK(fresh.rkey!=old.rkey);for(unsigned i=0;i<4;i++)pump();if(revoke_error){CHECK(!wki_secure_rdma_remote_grant(identity(),p::Service::VFS,0,discovered,p::PERMIT_READ));revoke_error=false;for(unsigned i=0;i<4;i++)pump();}CHECK(wki_secure_rdma_remote_grant(identity(),p::Service::VFS,0,discovered,p::PERMIT_READ)&&same_descriptor(discovered,fresh));CHECK(revoke_publications==1);ctrl(MsgType::SECURE_RDMA_REVOKE,&old,sizeof(old));ctrl(MsgType::SECURE_RDMA_REVOKE,&old,sizeof(old));for(unsigned i=0;i<4;i++)pump();CHECK(wki_secure_rdma_remote_grant(identity(),p::Service::VFS,0,discovered,p::PERMIT_READ)&&same_descriptor(discovered,fresh));auto a=arm_for(fresh,9,8);enqueue_arm(a);write_frag(a,0,8);for(unsigned i=0;i<8;i++)pump();CHECK(second.effects==1&&second.releases==1);close_region(fresh);}
else if(mode=="revoke-live-write"){install_remote(p::Service::VFS);drop_result=true;revoke_after_write=true;std::array<uint8_t,8> input{};auto r=wki_secure_rdma_write(identity(),remote_descriptor,0,input);CHECK(r.status<0&&r.outcome==WkiSecureRdmaOutcome::UNCERTAIN&&publications==1);}
else if(mode=="diagnostic-saturation"){install_remote(p::Service::VFS);s_client_completions[5].store(UINT64_MAX);s_client_bytes[5].store(UINT64_MAX-3);s_client_write_completions[5].store(UINT64_MAX);s_client_write_bytes[5].store(UINT64_MAX-3);std::array<uint8_t,8> input{};CHECK(wki_secure_rdma_write(identity(),remote_descriptor,0,input).status==0);CHECK(s_client_completions[5].load()==UINT64_MAX&&s_client_bytes[5].load()==UINT64_MAX);CHECK(s_client_write_completions[5].load()==UINT64_MAX&&s_client_write_bytes[5].load()==UINT64_MAX&&s_client_read_completions[5].load()==0);wki_secure_rdma_retire_peer(&g_wki.peers[0]);CHECK(s_client_completions[5].load()==UINT64_MAX&&s_client_write_completions[5].load()==UINT64_MAX);}
else if(mode=="diagnostic-read-saturation"){install_remote(p::Service::VFS);s_client_read_completions[5].store(UINT64_MAX);s_client_read_bytes[5].store(UINT64_MAX-3);std::array<uint8_t,8> output{};CHECK(wki_secure_rdma_read(identity(),remote_descriptor,0,output).status==0&&output[0]==0x5a);CHECK(s_client_read_completions[5].load()==UINT64_MAX&&s_client_read_bytes[5].load()==UINT64_MAX);CHECK(s_client_completions[5].load()==1&&s_client_bytes[5].load()==8&&s_client_write_completions[5].load()==0);}
else if(mode=="close-before-revoke-pressure"){Native n;auto d=local_region(n);auto a=arm_for(d,1,8);enqueue_arm(a);std::array<uint8_t,8> input{};record(incoming(a,p::Opcode::WRITE,0,8),input.data());current=&worker;drain_rx(now);current=&caller;CHECK(!s_registry.guard_.test_and_set());CHECK(wki_secure_rdma_unregister_region(d)==-EAGAIN);CHECK(n.effects==0);s_registry.guard_.clear();for(unsigned i=0;i<10;i++)pump();CHECK(n.effects==0&&n.releases==1&&n.bytes[0]==0x6a);close_region(d);}
else if(mode=="retire-gates"){auto&peer=g_wki.peers[0];unsigned wakes=wake_calls;peer.auth_session.replay_lock.lock();s_lock.lock();wki_secure_rdma_retire_peer(&peer);CHECK(s_peers[0].closed.load()&&wake_calls==wakes);s_lock.unlock();peer.auth_session.replay_lock.unlock();ready();}
else if(mode=="provider-rotation"){Native n;auto d=local_region(n);auto old=remote_offer;auto old_local=local_offer;uint64_t old_inc=s_peers[0].incarnation;provider.chaos_unregistering.store(true);s_lock.lock();wki_secure_rdma_retire_transport(&provider,7);CHECK(s_peers[0].closed.load());s_lock.unlock();CHECK(wki_secure_rdma_transport(identity(),p::Service::VFS)==p::Transport::NONE);for(unsigned i=0;i<300&&s_peers[0].begun;i++)pump();CHECK(!s_peers[0].begun&&provider.chaos_in_flight.load()==0);close_region(d);provider.registration_generation=8;kind=p::Transport::ROCE;provider.secure_kind=kind;provider.chaos_unregistering.store(false);ready();CHECK(s_peers[0].incarnation>old_inc&&local_offer.nonce!=old_local.nonce&&wki_secure_rdma_transport(identity(),p::Service::VFS)==kind);uint64_t inc=s_peers[0].incarnation;ctrl(MsgType::SECURE_RDMA_OFFER,&old,sizeof(old));pump();CHECK(wki_secure_rdma_available(identity(),p::Service::VFS)&&s_peers[0].incarnation==inc);install_remote(p::Service::VFS);std::array<uint8_t,8> output{};auto r=wki_secure_rdma_read(identity(),remote_descriptor,0,output);CHECK(r.status==0&&output[0]==0x5a);}
else if(mode=="offer-idempotence"){install_remote(p::Service::VFS);std::array<uint8_t,8> input{};auto r=wki_secure_rdma_write(identity(),remote_descriptor,0,input);CHECK(r.status==0);uint64_t inc=s_peers[0].incarnation;auto same=remote_offer;ctrl(MsgType::SECURE_RDMA_OFFER,&same,sizeof(same));pump();r=wki_secure_rdma_write(identity(),remote_descriptor,0,input);CHECK(r.status==0&&s_peers[0].incarnation==inc);auto stale=same;uint64_t lower=remote_inc-1;for(size_t i=0;i<8;i++)stale.nonce[i]=static_cast<uint8_t>(lower>>(i*8));ctrl(MsgType::SECURE_RDMA_OFFER,&stale,sizeof(stale));pump();CHECK(wki_secure_rdma_available(identity(),p::Service::VFS)&&s_peers[0].incarnation==inc);CHECK(s_peers[0].remote_history_incarnation==remote_inc);}
else if(mode=="auth-rotation"){auto old=remote_offer;uint64_t inc=s_peers[0].incarnation;wki_secure_rdma_retire_peer(&g_wki.peers[0]);g_wki.peers[0].auth_session.generation++;g_wki.peers[0].auth_session.session_id[0]++;ready();CHECK(s_peers[0].incarnation>inc&&s_peers[0].sid==g_wki.peers[0].auth_session.session_id);inc=s_peers[0].incarnation;ctrl(MsgType::SECURE_RDMA_OFFER,&old,sizeof(old));pump();CHECK(s_peers[0].incarnation==inc&&wki_secure_rdma_available(identity(),p::Service::VFS));}
else if(mode=="incarnation-exhaustion"){s_next_incarnation=UINT64_MAX;wki_secure_rdma_retire_peer(&g_wki.peers[0]);for(unsigned i=0;i<400;i++)pump();CHECK(s_next_incarnation==0&&!s_peers[0].begun&&!wki_secure_rdma_available(identity(),p::Service::VFS));rdma_session::Statistics stats{};CHECK(s_peers[0].session.statistics(stats)==p::Drop::NONE&&stats.live_keys==0);}
else if(mode=="transport-query"){CHECK(wki_secure_rdma_transport(identity(),p::Service::VFS)==kind);s_lock.lock();CHECK(wki_secure_rdma_transport(identity(),p::Service::VFS)==p::Transport::NONE);s_lock.unlock();deny_rx=true;CHECK(wki_secure_rdma_transport(identity(),p::Service::VFS)==p::Transport::NONE);deny_rx=false;g_wki.transport_lock.lock();CHECK(wki_secure_rdma_transport(identity(),p::Service::VFS)==p::Transport::NONE);g_wki.transport_lock.unlock();}
else if(mode=="roce-read-write"){CHECK(wki_secure_rdma_transport(identity(),p::Service::VFS)==p::Transport::ROCE);install_remote(p::Service::VFS);std::array<uint8_t,8> input{};input.fill(0x91);CHECK(wki_secure_rdma_write(identity(),remote_descriptor,0,input).status==0);std::array<uint8_t,8> output{};CHECK(wki_secure_rdma_read(identity(),remote_descriptor,0,output).status==0&&output[0]==0x91);}
else if(mode=="default-off"){CHECK(s_services==0&&s_scratch==nullptr&&s_worker==nullptr&&!wki_secure_rdma_available(identity(),p::Service::VFS));CHECK(wki_secure_rdma_transport(identity(),p::Service::VFS)==p::Transport::NONE);}
else if(mode=="post-failure"){CHECK(s_worker==nullptr&&s_worker_stopped.load()&&worker.refs.load()==1);}
else if(mode=="register-pressure"){Native n;p::RegionDescriptor d{};CHECK(s_lock.try_lock());int status=wki_secure_rdma_register_region(identity(),p::Service::VFS,n.bytes.data(),32,1,p::PERMIT_WRITE,{native_arm,native_effect,native_release},&n,d);s_lock.unlock();CHECK(status!=0);CHECK(d.rkey==0);CHECK(n.arms==0&&n.effects==0);}
else if(mode=="frontend-return-yield"||mode=="frontend-ordinary-process"){
install_remote(p::Service::BLOCK);caller.type=sc::task::TaskType::PROCESS;caller.syscall_account_start_us=777;
caller.deferred_task_switch=mode=="frontend-return-yield";caller.yield_switch=caller.deferred_task_switch;
CHECK(wki_current_process_wait_can_park()==!caller.deferred_task_switch);std::vector<uint8_t> output(8193,0xc3),input(8193,0x49);
unsigned parks_before=park_calls;test_interrupts=false;auto r=wki_secure_rdma_read(identity(),remote_descriptor,0,output);
CHECK(r.status==0&&r.bytes==8193&&r.outcome==WkiSecureRdmaOutcome::EXACT);CHECK(std::all_of(output.begin(),output.end(),[](auto b){return b==0x5a;}));
CHECK(test_interrupts&&park_calls>parks_before&&halt_calls>0&&timer_rechecks==halt_calls&&park_with_signal==0);
r=wki_secure_rdma_write(identity(),remote_descriptor,0,input);CHECK(r.status==0&&r.bytes==8193&&r.outcome==WkiSecureRdmaOutcome::EXACT);
CHECK(std::all_of(remote_bytes.begin(),remote_bytes.begin()+8193,[](auto b){return b==0x49;}));
CHECK(caller.deferred_task_switch==(mode=="frontend-return-yield")&&caller.yield_switch==caller.deferred_task_switch);
CHECK(caller.syscall_account_start_us!=0&&caller.wait_channel==nullptr&&caller.wait_channel_kind==sc::task::WaitChannelKind::NONE&&!caller.wants_block&&!caller.voluntary&&caller.wake_at_us==0);
CHECK(caller.refs.load()==1&&arm_controls==2&&park_with_signal==0);
}
else if(mode=="frontend-unsafe-context"){
install_remote(p::Service::BLOCK);std::array<uint8_t,8> output{};output.fill(0xc3);
auto reset=[&]{current=&caller;caller.type=sc::task::TaskType::PROCESS;caller.syscall_account_start_us=777;caller.deferred_task_switch=true;caller.yield_switch=true;caller.wants_block=false;caller.voluntary=false;caller.wait_channel=nullptr;caller.wait_channel_kind=sc::task::WaitChannelKind::NONE;caller.wki_proxy_task_id=0;caller.wki_proxy_task=false;caller.preempt_disable_depth=0;caller.signal=false;caller.scheduler_transition_active.store(false);s_timer_deferred_task.store(nullptr);};
auto reject=[&]{auto*subject=current;bool deferred=subject&&subject->deferred_task_switch,yield=subject&&subject->yield_switch;auto r=wki_secure_rdma_read(identity(),remote_descriptor,0,output);CHECK(r.status==-EAGAIN&&r.bytes==0&&r.outcome==WkiSecureRdmaOutcome::NO_EFFECT);CHECK(std::all_of(output.begin(),output.end(),[](auto b){return b==0xc3;}));CHECK(publications==0&&arm_controls==0&&park_calls==0);CHECK(caller.refs.load()==1);for(auto const&j:s_jobs)CHECK(j.state.load()==JobState::FREE);if(subject)CHECK(subject->deferred_task_switch==deferred&&subject->yield_switch==yield);};
reset();caller.syscall_account_start_us=0;reject();reset();caller.yield_switch=false;reject();reset();caller.wait_channel="other-wait";caller.wait_channel_kind=sc::task::WaitChannelKind::GENERIC;reject();reset();caller.wait_channel_kind=sc::task::WaitChannelKind::GENERIC;reject();reset();caller.wait_channel="exec-handoff";caller.wait_channel_kind=sc::task::WaitChannelKind::WKI_EXECVE_PROXY;reject();reset();caller.wants_block=true;reject();reset();caller.voluntary=true;reject();reset();caller.wki_proxy_task_id=3;reject();reset();caller.wki_proxy_task=true;reject();reset();caller.preempt_disable_depth=1;reject();reset();caller.signal=true;reject();reset();caller.scheduler_transition_active.store(true);reject();reset();s_timer_deferred_task.store(&caller);CHECK(wki_current_wait_must_drive_progress());reject();reset();caller.type=sc::task::TaskType::IDLE;reject();reset();current=nullptr;reject();reset();current=&worker;reject();reset();
// Transition/proxy markers also exclude the central ordinary PROCESS branch.
caller.deferred_task_switch=false;caller.yield_switch=false;caller.scheduler_transition_active.store(true);CHECK(wki_current_process_wait_can_park());reject();reset();caller.deferred_task_switch=false;caller.yield_switch=false;caller.wki_proxy_task=true;CHECK(wki_current_process_wait_can_park());reject();reset();
}
else if(mode=="frontend-signal-after-admission"){
install_remote(p::Service::BLOCK);caller.type=sc::task::TaskType::PROCESS;caller.syscall_account_start_us=777;caller.deferred_task_switch=true;caller.yield_switch=true;signal_after_arm=true;
std::array<uint8_t,8> output{};output.fill(0xc3);unsigned yields_before=yield_calls;auto r=wki_secure_rdma_read(identity(),remote_descriptor,0,output);
CHECK(r.status==0&&r.bytes==8&&r.outcome==WkiSecureRdmaOutcome::EXACT&&output[0]==0x5a);CHECK(caller.signal&&yield_calls>yields_before&&park_with_signal==0);
CHECK(caller.deferred_task_switch&&caller.yield_switch&&caller.wait_channel==nullptr&&!caller.voluntary&&!caller.wants_block&&caller.refs.load()==1);caller.signal=false;
}
else if(mode=="scheduler-process-park"){
caller.type=sc::task::TaskType::PROCESS;caller.syscall_account_start_us=777;caller.deferred_task_switch=true;caller.yield_switch=true;
for(bool enabled:{false,true}){test_interrupts=enabled;unsigned halts=halt_calls;sc::preemptible_syscall_park("probe",now+1000);CHECK(test_interrupts&&halt_calls==halts+1&&caller.perf_wait_callsite==73);CHECK(caller.deferred_task_switch&&caller.yield_switch&&caller.syscall_account_start_us!=0&&!caller.voluntary&&!caller.wants_block&&caller.wait_channel==nullptr&&caller.wait_channel_kind==sc::task::WaitChannelKind::NONE&&caller.wake_at_us==0&&!caller.wakeup_pending.load());}
caller.signal=true;test_interrupts=false;unsigned halts=halt_calls;sc::preemptible_syscall_park("signal",now+1000);CHECK(halt_calls==halts&&test_interrupts&&caller.deferred_task_switch&&caller.yield_switch&&!caller.voluntary&&!caller.wants_block&&caller.wait_channel==nullptr);caller.signal=false;
caller.wakeup_pending.store(true);test_interrupts=false;sc::preemptible_syscall_park("wake",now+1000);CHECK(halt_calls==halts&&!caller.wakeup_pending.load()&&test_interrupts);
CHECK(sc::scheduler_wait_should_cancel(nullptr,sc::SchedulerHaltWaitKind::PROCESS_PARK)==false);caller.wants_block=false;caller.wake_at_us=0;CHECK(sc::scheduler_wait_should_cancel(&caller,sc::SchedulerHaltWaitKind::PROCESS_PARK));CHECK(!sc::scheduler_wait_should_cancel(&caller,sc::SchedulerHaltWaitKind::YIELD));caller.signal=true;caller.wants_block=true;caller.wake_at_us=1;CHECK(sc::scheduler_wait_should_cancel(&caller,sc::SchedulerHaltWaitKind::PROCESS_PARK));CHECK(!sc::scheduler_wait_should_cancel(&caller,sc::SchedulerHaltWaitKind::BLOCK));caller.signal=false;caller.wants_block=false;caller.wake_at_us=0;
}
else if(mode=="task-gate-diagnostics"||mode=="task-gate-diagnostics-off"){
install_remote(p::Service::BLOCK);std::array<uint8_t,8> output{};output.fill(0xc3);
caller.type=sc::task::TaskType::PROCESS;caller.syscall_account_start_us=777;caller.deferred_task_switch=true;caller.yield_switch=false;
auto reject=[&](){auto r=wki_secure_rdma_read(identity(),remote_descriptor,0,output);CHECK(r.status==-EAGAIN&&r.bytes==0&&r.outcome==WkiSecureRdmaOutcome::NO_EFFECT);CHECK(std::all_of(output.begin(),output.end(),[](uint8_t b){return b==0xc3;}));CHECK(publications==0&&arm_controls==0);for(auto const&j:s_jobs)CHECK(j.state.load()==JobState::FREE);};
reject();
if(mode=="task-gate-diagnostics"){
CHECK(task_gate_logs.size()==1);auto const&line=task_gate_logs.back();CHECK(line.find("reason=process-wait-ineligible")!=std::string::npos);CHECK(line.find("pid=17")!=std::string::npos&&line.find("deferred=1 yield=0 wait_kind=0 wait= account=777")!=std::string::npos);CHECK(line.find("preempt=0")!=std::string::npos&&line.find("wants=0 voluntary=0 proxy=0 signal=0 transition=0")!=std::string::npos);
current=nullptr;reject();CHECK(task_gate_logs.back().find("reason=no-task")!=std::string::npos);current=&worker;reject();CHECK(task_gate_logs.back().find("reason=service-worker")!=std::string::npos);current=&caller;caller.type=sc::task::TaskType::DAEMON;caller.preempt_disable_depth=1;reject();CHECK(task_gate_logs.back().find("reason=preempt-disabled")!=std::string::npos);caller.preempt_disable_depth=0;caller.type=sc::task::TaskType::PROCESS;
caller.wait_channel="abcdefghijklmnopqrstuvwxyz0123456789long-private-channel";caller.wait_channel_kind=sc::task::Task::WaitChannelKind::GENERIC;caller.wants_block=true;caller.voluntary=true;caller.wki_proxy_task_id=9;caller.signal=true;caller.scheduler_transition_active.store(true);reject();CHECK(task_gate_logs.back().find("wait=abcdefghijklmnopqrstuvwxyz012345 account=777")!=std::string::npos);CHECK(task_gate_logs.back().find("wants=1 voluntary=1 proxy=9 signal=1 transition=1")!=std::string::npos);
for(unsigned i=0;i<20;++i)reject();CHECK(task_gate_logs.size()==8&&s_task_gate_warnings.load()==8);
}else{for(unsigned i=0;i<12;++i)reject();CHECK(task_gate_logs.empty()&&s_task_gate_warnings.load()==0);}
caller.type=sc::task::TaskType::DAEMON;caller.preempt_disable_depth=0;caller.deferred_task_switch=false;caller.yield_switch=false;caller.wait_channel=nullptr;caller.wait_channel_kind=sc::task::Task::WaitChannelKind::NONE;caller.wants_block=false;caller.voluntary=false;caller.wki_proxy_task_id=0;caller.signal=false;caller.scheduler_transition_active.store(false);CHECK(wki_secure_rdma_read(identity(),remote_descriptor,0,output).status==0&&output[0]==0x5a);CHECK(task_gate_logs.size()==(mode=="task-gate-diagnostics"?8U:0U));
}
else CHECK(false);
std::array<uint64_t,8> completions{},byte_counts{};
if(mode=="outgoing-write"||mode=="outgoing-read"||mode=="ingress-read-engine-pressure"||mode=="read-end-barrier"){completions[5]=1;byte_counts[5]=8193;}
else if(mode=="partial-ipc"){completions[6]=1;byte_counts[6]=4;}
else if(mode=="ipc-query-and-payload"){completions[6]=2;byte_counts[6]=4144;}
else if(mode=="ipc-query-lost-payload"){completions[6]=1;byte_counts[6]=48;}
else if(mode=="partial-vfs"){completions[5]=1;byte_counts[5]=3;}
else if(mode=="read-eof"){completions[5]=1;}
else if(mode=="identity-exhaustion"||mode=="result-loan-retention"||mode=="result-worker-pressure"){completions[5]=1;byte_counts[5]=8;}
else if(mode=="offer-idempotence"){completions[5]=2;byte_counts[5]=16;}
else if(mode=="read-end-barrier-roce"){completions[1]=1;byte_counts[1]=8193;}
else if(mode=="provider-rotation"){completions[1]=1;byte_counts[1]=8;}
else if(mode=="roce-read-write"){completions[1]=2;byte_counts[1]=16;}
else if(mode=="diagnostic-saturation"){completions[5]=UINT64_MAX;byte_counts[5]=UINT64_MAX;}
else if(mode=="task-gate-diagnostics"||mode=="task-gate-diagnostics-off"||mode=="frontend-signal-after-admission"){completions[7]=1;byte_counts[7]=8;}
else if(mode=="frontend-return-yield"||mode=="frontend-ordinary-process"){completions[7]=2;byte_counts[7]=16386;}
std::array<uint64_t,8> read_completions{},read_bytes{},write_completions{},write_bytes{};
if(mode=="outgoing-read"||mode=="ingress-read-engine-pressure"||mode=="read-end-barrier"){read_completions[5]=1;read_bytes[5]=8193;}
else if(mode=="outgoing-write"){write_completions[5]=1;write_bytes[5]=8193;}
else if(mode=="partial-ipc"){write_completions[6]=1;write_bytes[6]=4;}
else if(mode=="ipc-query-and-payload"||mode=="ipc-query-lost-payload"){read_completions[6]=1;read_bytes[6]=48;if(mode=="ipc-query-and-payload"){write_completions[6]=1;write_bytes[6]=4096;}}
else if(mode=="partial-vfs"){write_completions[5]=1;write_bytes[5]=3;}
else if(mode=="read-eof"){read_completions[5]=1;}
else if(mode=="identity-exhaustion"||mode=="result-worker-pressure"){write_completions[5]=1;write_bytes[5]=8;}
else if(mode=="result-loan-retention"){read_completions[5]=1;read_bytes[5]=8;}
else if(mode=="offer-idempotence"){write_completions[5]=2;write_bytes[5]=16;}
else if(mode=="read-end-barrier-roce"){read_completions[1]=1;read_bytes[1]=8193;}
else if(mode=="provider-rotation"){read_completions[1]=1;read_bytes[1]=8;}
else if(mode=="roce-read-write"){read_completions[1]=1;read_bytes[1]=8;write_completions[1]=1;write_bytes[1]=8;}
else if(mode=="diagnostic-saturation"){write_completions[5]=UINT64_MAX;write_bytes[5]=UINT64_MAX;}
else if(mode=="diagnostic-read-saturation"){read_completions[5]=UINT64_MAX;read_bytes[5]=UINT64_MAX;completions[5]=1;byte_counts[5]=8;}
else if(mode=="task-gate-diagnostics"||mode=="task-gate-diagnostics-off"||mode=="frontend-signal-after-admission"){read_completions[7]=1;read_bytes[7]=8;}
else if(mode=="frontend-return-yield"||mode=="frontend-ordinary-process"){read_completions[7]=1;read_bytes[7]=8193;write_completions[7]=1;write_bytes[7]=8193;}
WkiSecureRdmaDiagnostics counters{};CHECK(wki_secure_rdma_diagnostics(counters));CHECK(counters.client_completions==completions&&counters.client_bytes==byte_counts);
CHECK(counters.client_read_completions==read_completions&&counters.client_read_bytes==read_bytes);
CHECK(counters.client_write_completions==write_completions&&counters.client_write_bytes==write_bytes);
finish();std::printf("%s checks=%u PASS\n",mode.c_str(),checks);}
'''
CASES = ('read-end-barrier', 'read-end-barrier-roce', 'read-end-timeout', 'frontend-return-yield', 'frontend-ordinary-process', 'frontend-unsafe-context', 'frontend-signal-after-admission', 'scheduler-process-park', 'task-gate-diagnostics', 'task-gate-diagnostics-off', 'outgoing-write', 'outgoing-read', 'partial-ipc', 'ipc-query-and-payload', 'ipc-query-lost-payload', 'lost-result', 'wrong-result', 'provider-error',
         'coverage-replay-close', 'tamper-unarmed', 'claim-close', 'retire-drain', 'arm-malformed', 'register-pressure', 'partial-vfs', 'invalid-result', 'read-eof',
         'slots-pressure', 'queue-pressure', 'owner-query', 'identity-exhaustion', 'held-claim-rundown',
         'abandoned-prepare', 'result-loan-retention', 'close-before-revoke-pressure', 'retire-gates',
         'shutdown-multiple-terminal', 'shutdown-operation-pressure', 'shutdown-result-loan', 'shutdown-held-eraser',
         'provider-rotation', 'offer-idempotence', 'auth-rotation', 'incarnation-exhaustion', 'transport-query',
         'roce-read-write', 'default-off', 'post-failure', 'diagnostic-saturation', 'diagnostic-read-saturation',
         'revoke-discovery', 'revoke-pressure', 'revoke-live-write',
         'admission-pressure', 'grant-worker-pressure', 'result-worker-pressure', 'queued-old-grant', 'effect-pressure',
         'record-peer-pressure', 'record-provider-pressure', 'record-engine-pressure', 'record-binding-pressure',
         'record-postdecode-pressure', 'record-channel-pressure', 'record-channel-postdecode', 'record-session-pressure',
         'record-operation-pressure', 'record-fifo-pressure', 'record-invalid-pressure', 'record-auth-retirement',
         'record-provider-retirement', 'record-channel-retirement', 'record-channel-capture-retirement',
         'record-region-retirement', 'record-shutdown-denied', 'record-peer-retirement', 'record-incarnation-retirement',
         'early-confirm-grant', 'early-grant-unconfirmed', 'early-grant-malformed', 'early-grant-provider-retirement',
         'early-grant-auth-retirement', 'early-grant-sid-retirement', 'early-grant-incarnation-retirement', 'early-grant-control-retirement',
         'early-grant-channel-closed', 'early-grant-unsupported-service', 'early-grant-unsupported-transport',
         'early-grant-unknown-channel', 'ipc-fixed-readiness', 'ipc-create-blocked', 'ipc-capture-pressure', 'ipc-disabled-no-create',
         'ingress-engine-pressure', 'ingress-engine-roce', 'ingress-read-engine-pressure', 'ingress-unbegun',
         'ingress-unconfirmed', 'ingress-closed', 'ingress-close-recheck', 'ingress-stop-recheck',
         'ingress-peer-pressure', 'ingress-provider-pressure', 'ingress-queue-pressure',
         'ingress-auth-generation', 'ingress-sid-retirement', 'ingress-provider-generation', 'ingress-provider-retirement',
         'ingress-incarnation-retirement', 'ingress-channel-pressure', 'ingress-channel-retirement', 'ingress-shutdown-denied',
         'ingress-capacity', 'ingress-fifo-budget', 'ingress-diagnostics-pressure')
SOURCES = ('auth_crypto.cpp', 'rdma_crypto.cpp', 'rdma_protocol.cpp', 'rdma_session.cpp', 'rdma_registry.cpp', 'rdma_operations.cpp')

def extract_function(text, signature):
    """Retain the exact function, balancing braces outside comments/literals."""
    start = text.index(signature)
    masked = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                    lambda match: ' ' * len(match.group()), text)
    opening = masked.index('{', start)
    depth = 1
    end = opening + 1
    while depth:
        if masked[end] == '{':
            depth += 1
        elif masked[end] == '}':
            depth -= 1
        end += 1
    return text[start:end]

def wait_sources(folder):
    """Actual policies, with only CLI/STI instructions translated to IF seams."""
    wki = WKI.read_text()
    scheduler = SCHEDULER.read_text()
    wki_signatures = (
        'auto is_timer_deferred_waiter(',
        '[[nodiscard]] auto process_wait_can_park(',
        'auto wki_current_process_wait_can_park()',
        'auto wki_current_wait_must_drive_progress()',
    )
    scheduler_signatures = (
        'inline auto scheduler_wait_should_cancel(',
        'inline auto scheduler_wait_halt_once(',
        'inline void restore_interrupts_after_scheduler_wait(',
        'inline void restore_process_syscall_park_interrupts(',
        'inline void preemptible_syscall_park_impl(const char* wait_channel, task::WaitChannelKind wait_kind, uint64_t deadline_us,',
    )
    functions = []
    bodies = []
    for original, text, signatures in ((WKI, wki, wki_signatures), (SCHEDULER, scheduler, scheduler_signatures)):
        group = []
        for index, signature in enumerate(signatures):
            body = extract_function(text, signature)
            path = folder / ('extracted-' + original.stem + '-' + str(index) + '.txt')
            path.write_text(body)
            functions.append({'source': str(original), 'signature': signature, 'body': str(path.resolve()),
                              'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
            group.append(body)
        bodies.append('\n'.join(group))
    enum_start = scheduler.index('enum class SchedulerHaltWaitKind')
    enum_end = scheduler.index('};', enum_start) + 2
    scheduler_body = scheduler[enum_start:enum_end] + '\n' + bodies[1]
    # Preserve every policy expression and ordering; no scheduler function is
    # replaced by a synthetic eligibility decision or simplified park loop.
    scheduler_body = scheduler_body.replace('asm volatile("cli" ::: "memory");', 'test_irq(false);')
    scheduler_body = scheduler_body.replace('asm volatile("sti" ::: "memory");', 'test_irq(true);')
    assert 'asm volatile' not in scheduler_body
    (folder / 'source/net/wki/wki.cpp').write_text(wki)
    platform = folder / 'source/platform/sched'
    platform.mkdir(parents=True, exist_ok=True)
    (platform / 'scheduler.hpp').write_text(scheduler)
    (platform / 'task.hpp').write_bytes((KERN / 'platform/sched/task.hpp').read_bytes())
    (folder / 'wait-extractions.json').write_text(json.dumps({'functions': functions,
        'seams': ['privileged CLI/STI/HLT and timer dispatch', 'runqueue accounting boundary',
                  'protocol worker Task identity', 'Task storage/refcount platform shim'],
        'not_claimed': ['host task scheduling or physical timer/IRQ behavior', 'complete runqueue accounting implementation',
                        'unrestricted IRQ/PF frontend eligibility']}, indent=2) + '\n')
    return bodies[0], scheduler_body

def prepare(folder, source):
    snapshot = folder / 'source/net/wki'
    snapshot.mkdir(parents=True, exist_ok=True)
    (folder / 'fixture.py').write_bytes(Path(__file__).read_bytes())
    address = folder / 'source/net/address.hpp'
    address.write_bytes((KERN / 'net/address.hpp').read_bytes())
    for original in (KERN / 'net/wki').glob('*.hpp'):
        (snapshot / original.name).write_bytes(original.read_bytes())
    for name in SOURCES:
        (snapshot / name).write_bytes((KERN / 'net/wki' / name).read_bytes())
    engine = snapshot / 'secure_rdma.cpp'
    engine.write_bytes(source.read_bytes())
    include = folder / 'include'
    for name, content in SHIMS.items():
        target = include / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
    target = folder / 'engine.cpp'
    wki_wait, scheduler_wait = wait_sources(folder)
    engine_text = engine.read_text()
    target.write_text('#define WKI_TEST_OWNED_RECORD_RETRY ' + str(int('s_record_decode' in engine_text)) + '\n' +
                      '#define WKI_TEST_RECORD_QUEUE ' + str(int('ker::mod::sys::Spinlock s_record_lock;' in engine_text)) + '\n' +
                      HARNESS.replace('@ENGINE@', str(engine.resolve())).replace('@WKI_WAIT_SOURCE@', wki_wait)
                      .replace('@SCHEDULER_WAIT_SOURCE@', scheduler_wait))
    return target, include, snapshot

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, default=ENGINE)
    parser.add_argument('--evidence', type=Path)
    parser.add_argument('--case', action='append', choices=CASES)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='wki-secure-engine-') as temp:
        folder = args.evidence or Path(temp)
        folder.mkdir(parents=True, exist_ok=True)
        if (folder / 'result.json').exists():
            raise RuntimeError('Evidence already exists; choose a new directory to retain history')
        target, include, snapshot = prepare(folder, args.source)
        binary = folder / 'engine-test'
        command = ['/usr/bin/clang++', '-std=c++23', '-O1', '-g', '-fsanitize=address,undefined', '-fno-omit-frame-pointer',
                   '-Wall', '-Wextra', '-Werror', '-I', str(include), '-I', str(folder / 'source'), '-I', str(KERN), str(target)]
        command += [str(snapshot / name) for name in SOURCES]
        command += ['-pthread', '-o', str(binary)]
        (folder / 'tmp').mkdir(exist_ok=True)
        compile_env = dict(os.environ, TMPDIR=str((folder / 'tmp').resolve()))
        compile_result = subprocess.run(command, capture_output=True, text=True, env=compile_env)
        (folder / 'compile.log').write_text(compile_result.stdout + compile_result.stderr)
        if compile_result.returncode:
            print(compile_result.stderr)
            return compile_result.returncode
        env = dict(os.environ, ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1')
        results = []
        for case in args.case or CASES:
            run = subprocess.run([str(binary), case], capture_output=True, text=True, env=env, timeout=30)
            (folder / (case + '.log')).write_text(run.stdout + run.stderr)
            print(run.stdout + run.stderr, end='')
            results.append({'case': case, 'exit': run.returncode})
            if run.returncode:
                break
        sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
        def pin(path):
            return {'path': str(path.resolve()), 'sha256': sha(path)}
        compiler_binary = Path(command[0]).resolve()
        version = subprocess.run([str(compiler_binary), '--version'], capture_output=True, text=True)
        (folder / 'compiler-version.log').write_text(version.stdout + version.stderr)
        for outcome in results:
            outcome['log'] = pin(folder / (outcome['case'] + '.log'))
        inputs = [pin(path) for parent in (folder / 'source', include) for path in sorted(parent.rglob('*')) if path.is_file()]
        (folder / 'result.json').write_text(json.dumps({'source': str(args.source), 'source_sha256': sha(snapshot / 'secure_rdma.cpp'),
            'source_snapshot': pin(snapshot / 'secure_rdma.cpp'), 'fixture_sha256': sha(folder / 'fixture.py'),
            'fixture': pin(folder / 'fixture.py'), 'translation_unit': pin(target), 'binary': pin(binary),
            'compiler_binary': pin(compiler_binary), 'compiler_version': pin(folder / 'compiler-version.log'),
            'compile_log': pin(folder / 'compile.log'), 'compile_exit': compile_result.returncode, 'compiler': command,
            'wait_extractions': pin(folder / 'wait-extractions.json'),
            'sanitizers': {key: env[key] for key in ('ASAN_OPTIONS', 'UBSAN_OPTIONS')}, 'inputs': inputs, 'cases': results}, indent=2) + '\n')
        return 0 if all(r['exit'] == 0 for r in results) else 1

if __name__ == '__main__':
    raise SystemExit(main())
