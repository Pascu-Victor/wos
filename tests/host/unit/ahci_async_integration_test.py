#!/usr/bin/env python3
"""Compile verbatim AHCI native functions with instrumented DMA/MMIO seams."""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE = Path(os.environ.get("WOS_AHCI_SOURCE", ROOT / "modules/kern/src/dev/ahci.cpp")).read_text()


def extract(prefix):
    start = SOURCE.index(prefix)
    brace = SOURCE.index("{", start)
    depth = 1
    end = brace + 1
    while depth:
        depth += (SOURCE[end] == "{") - (SOURCE[end] == "}")
        end += 1
    return SOURCE[start:end]


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <functional>
#include <span>
#include <utility>
#include <vector>
#include <dev/ahci.hpp>
#include <dev/ahci_async.hpp>
#include <dev/ahci_dma.hpp>
#include <dev/ahci_lifecycle.hpp>
#include <dev/block_request.hpp>
#include <dev/dma/dma.hpp>
int mutex_depth=0, spin_depth=0, dma_completes=0, quarantines=0, resets=0, wakes=0, identifies=0, identify_fault=0, guarded_irqs=0, final_status_fault=0, never_submitted=0, device_quiesced=0;
size_t mmio_reads=0,mmio_writes=0,clock_reads=0;
void (*after_spin_unlock)()=nullptr;
uint64_t clock_us=0;
bool stop_proven=false, cancel_in_prepare=false, reset_pending=false, reset_reopened=false, irq_during_status_read=false, irq_during_stop=false, identify_retiring=false, fault_during_active_read=false;
bool finish_after_doorbell=false,finish_on_next_ci_read=false;
bool finish_flush_doorbell=false,flush_doorbell_published=false,flush_fresh_active=false,flush_fresh_hardware_fault=false,use_cancel_queue=false;
bool finish_data_doorbell=false,flush_fresh_sact=false,short_write_prdbc=false,reject_data_completion=false;
bool finish_after_published_readback=false;
int flush_doorbell_fault=0,flush_ci_reads=0;
int service_completion_calls=0,dma_rejections=0;
void (*completion_retirement_check)(ker::dev::BlockRequestToken)=nullptr;
std::array<ker::dev::BlockRequestToken,8> prepared_tokens{};
void (*before_data_retirement)(ker::dev::dma::Submission,size_t,ker::dev::dma::Retirement)=nullptr;
ker::dev::BlockQueueModel cancel_queue{};
int legacy_sync_status=0;
std::vector<ker::dev::BlockRequestOperation> legacy_sync_operations;
struct Completed {ker::dev::BlockRequestToken token;int status;size_t bytes;};
std::vector<Completed> completed;
bool poll_observing=false;
std::vector<char> poll_read_order;
std::function<void(char)> poll_after_read;
std::function<void()> poll_after_clock;
namespace ker::mod::sys {
struct Mutex {bool held{};bool try_lock(){if(held)return false;held=true;++mutex_depth;return true;}void unlock(){assert(held);held=false;--mutex_depth;}};
struct Spinlock {bool held{};uint64_t lock_irqsave(){assert(!held);held=true;++spin_depth;return 0;}void unlock_irqrestore(uint64_t){assert(held);held=false;--spin_depth;if(auto hook=std::exchange(after_spin_unlock,nullptr))hook();}};
}
namespace ker::mod::time {uint64_t get_us(){++clock_reads;clock_us+=100;if(poll_observing&&poll_after_clock)poll_after_clock();return clock_us;}}
namespace ker::mod::mm::addr {uint64_t get_hhdm_offset(){return 0;}}
namespace ker::mod::sched {namespace task {struct Task{};} bool can_query_current_task(){return false;} task::Task* get_current_task(){return nullptr;}}
namespace ker::mod::sys::usercopy {
bool copy_from_task_mapped(ker::mod::sched::task::Task&,uint64_t,void*,size_t){assert(false);return false;}
bool copy_to_task_mapped(ker::mod::sched::task::Task&,uint64_t,const void*,size_t){assert(false);return false;}
}
namespace ker::dev {
int block_request_sync(BlockDevice*,BlockRequestOperation operation,uint64_t,size_t,void*){assert(mutex_depth==0&&spin_depth==0);legacy_sync_operations.push_back(operation);return legacy_sync_status;}
void block_queue_wake(BlockDevice*){assert(spin_depth==0);++wakes;}
bool block_request_complete(BlockDevice*,BlockRequestToken token,int status,size_t bytes){assert(mutex_depth==0&&spin_depth==0);if(completion_retirement_check)completion_retirement_check(token);for(auto const& c:completed)assert(!(c.token==token));if(use_cancel_queue){assert(block_model_claim_completion(cancel_queue,token,status,bytes,true));auto const* state=block_model_detail::lookup(cancel_queue,token);status=state->status;bytes=state->completed_bytes;assert(block_model_finish_completion(cancel_queue,token));}completed.push_back({token,status,bytes});return true;}
bool block_request_complete_from_service(BlockDevice* device,BlockRequestToken token,int status,size_t bytes){++service_completion_calls;return block_request_complete(device,token,status,bytes);}
int block_queue_begin_reset(BlockDevice*,int reason){assert(spin_depth==0);if(use_cancel_queue)assert(block_model_begin_reset(cancel_queue,reason)==0);reset_pending=true;return 0;}
int block_queue_finish_reset(BlockDevice*,bool reopen){assert(mutex_depth==0&&spin_depth==0);if(!reset_pending)return -EINVAL;if(use_cancel_queue)assert(block_model_finish_reset(cancel_queue,reopen)==0);reset_pending=false;reset_reopened=reopen;return 0;}
}
namespace ker::dev::ahci {namespace {
struct log {template<class...T>static void info(const char*,T...){} template<class...T>static void warn(const char*,T...){} };
void ahci_log(const char*){}
constexpr size_t MAX_PORTS=32,COMMAND_SLOT_COUNT=32,TRANSFER_CAPACITY=512*1024;
constexpr uint32_t SECTOR_SIZE=512,AHCI_MAX_SECTORS_PER_CMD=1024,HBA_PX_CMD_ST=1,HBA_PX_CMD_FRE=16;
constexpr uint8_t ATA_DEV_BUSY=128,ATA_DEV_DRQ=8,ATA_CMD_FLUSH_CACHE_EXT=0xea,ATA_CMD_IDENTIFY=0xec,ATA_CMD_READ_DMA_EX=0x25,ATA_CMD_WRITE_DMA_EX=0x35,HBA_PORT_DET_PRESENT=3;
constexpr uint64_t ENGINE_TIMEOUT_NS=1000000000,COMMAND_TIMEOUT_NS=30000000000;
constexpr uint32_t NATIVE_IRQ_MASK=0x7d400049;
HbaMem registers{};volatile HbaMem* hba_mem=&registers;
std::atomic<bool> controller_failed{false};lifecycle::Admission admission;
uint8_t native_irq_vector=48;
std::array<mod::sys::Mutex,MAX_PORTS> port_locks{};
std::array<std::atomic<uint32_t>,MAX_PORTS> port_slots_in_use{};
'''

MIDDLE = r'''
std::array<PortMemory,MAX_PORTS> port_memory{};
std::array<std::array<std::byte,TRANSFER_CAPACITY>,8> pools{};
std::array<std::array<std::byte,4096>,32> tables{};
std::array<HbaCmdHeader,32> headers{};
AHCIDevice device{};
struct DmaState {size_t bytes{};uint64_t sequence{};bool owned{},quarantined{};std::span<std::byte> target{};};
std::array<DmaState,8> dma_states{};
std::vector<uint32_t> ci_doorbells,sact_doorbells;
void ahci_async_cancel(BlockDevice*,BlockRequestToken,int);
void native_irq(uint8_t,void*);
void quarantine_port(size_t);
void apply_doorbell_fault(size_t slot){
    auto& p=registers.ports[0];
    if(flush_doorbell_fault==1)p.is=1U<<29;
    if(flush_doorbell_fault==2)p.tfd=1;
    if(flush_doorbell_fault==3)p.tfd=0x20;
    if(flush_doorbell_fault==4)controller_failed.store(true);
    if(flush_doorbell_fault==5)port_memory[0].interrupt_status.fetch_or(1U<<29);
    if(flush_doorbell_fault==6)clock_us=100001;
    if(flush_doorbell_fault>=7&&flush_doorbell_fault<=10)assert(port_memory[0].injected_fault!=SelftestAsyncFault::none);
    if(flush_doorbell_fault==11)admission.close();
    if(flush_doorbell_fault==12)port_memory[0].failed=true;
    if(flush_doorbell_fault==13){port_memory[0].queue_reset_pending.store(true);reset_pending=true;if(use_cancel_queue)assert(ker::dev::block_model_begin_reset(cancel_queue,-ESHUTDOWN)==0);}
    if(flush_doorbell_fault==14){assert(use_cancel_queue);assert(ker::dev::block_model_cancel(cancel_queue,port_memory[0].async.slots[slot].token,-ECANCELED)!=ker::dev::BlockCancelResult::REJECTED);}
    if(flush_doorbell_fault==15){p.is=1U<<29;registers.is=1;irq_during_status_read=true;}
    if(flush_doorbell_fault==16)flush_fresh_hardware_fault=true;
}
auto mmio_read32(volatile uint32_t& r)->uint32_t{
    ++mmio_reads;
    if(poll_observing){
        assert(mutex_depth==1&&spin_depth==1);
        char const which=&r==&registers.ports[0].ci?'C':&r==&registers.ports[0].sact?'A':&r==&registers.ports[0].is?'I':'?';
        poll_read_order.push_back(which);uint32_t const captured=r;if(poll_after_read)poll_after_read(which);return captured;
    }
    if(&r==&registers.ports[0].ci&&flush_doorbell_published){
        ++flush_ci_reads;
        if(flush_ci_reads==1&&finish_after_published_readback){
            // Capture genuine published activity before independently retiring
            // hardware. The driver must take a later full snapshot to claim.
            uint32_t const captured=r;finish_after_published_readback=false;
            registers.ports[0].ci=registers.ports[0].sact=0;
            for(size_t i=0;i<8;++i)if(port_memory[0].async.slots[i].phase==async_rules::Phase::active)
                headers[i].prdbc=port_memory[0].async.slots[i].ncq?0:static_cast<uint32_t>(port_memory[0].async.slots[i].bytes);
            apply_doorbell_fault(0);
            return captured;
        }
        if(flush_ci_reads==1&&(flush_fresh_active||flush_fresh_sact)){
            // Hardware changes AFTER the captured published readback, whether
            // or not the driver performs its required second full snapshot.
            uint32_t const captured=r;
            if(flush_fresh_active){flush_fresh_active=false;registers.ports[0].ci=1;}
            if(flush_fresh_sact){flush_fresh_sact=false;registers.ports[0].sact=1;}
            return captured;
        }
        if(flush_fresh_hardware_fault&&flush_ci_reads==2){flush_fresh_hardware_fault=false;registers.ports[0].ci=registers.ports[0].sact=0;registers.ports[0].is=1U<<29;}
    }
    if(&r==&registers.ports[0].ci&&finish_on_next_ci_read){finish_on_next_ci_read=false;registers.ports[0].sact=0;}
    if(&r==&registers.ports[0].ci&&fault_during_active_read){
        fault_during_active_read=false;
        registers.ports[0].ci=registers.ports[0].sact=0;registers.ports[0].is=1U<<29;
        // Hardware changes independently of IRQ masking. Reading active bits
        // before error latches must catch this fault before claiming a slot.
    }
    if(&r==&registers.ports[0].is&&irq_during_status_read){
        irq_during_status_read=false;
        // IRQ-save guards mask this exact preemption boundary. Without the
        // guard, IRQ publishes and clears PxIS after service exchanged its
        // deferred latch, reproducing a false successful read completion.
        if(spin_depth==0)native_irq(48,nullptr);else ++guarded_irqs;
    }
    return r;
}
void mmio_write32(volatile uint32_t& r,uint32_t value){
    ++mmio_writes;
    auto& p=registers.ports[0];
    if(&r==&p.ci){
        r=r|value;
        auto const slot=value?__builtin_ctz(value):0;
        auto* fis=reinterpret_cast<FisRegH2D*>(tables[slot].data());
        if(value&&fis->command==ATA_CMD_IDENTIFY){
            auto const& m=port_memory[0];
            assert(mutex_depth==1&&spin_depth==0&&m.async.frozen&&async_rules::owned(m.async)==0&&m.pending_issue_mask==0&&p.ie==0&&p.sact==0);
            assert(p.tfd==0x130&&dma_states[0].owned);for(size_t i=1;i<8;++i)assert(!dma_states[i].owned);
            ++identifies;identify_retiring=true;std::array<uint16_t,256> words{};
            std::copy(m.identify_serial.begin(),m.identify_serial.end(),words.begin()+10);
            words[75]=m.stats.identify_queue_depth;words[76]=m.stats.identify_sata_caps;words[83]=m.stats.identify_command_sets;
            uint64_t total=device.total_sectors+(identify_fault==1);
            for(size_t i=0;i<4;++i)words[100+i]=uint16_t(total>>(16*i));
            if(identify_fault==2)words[76]^=1;if(identify_fault==3)words[10]^=1;
            std::memcpy(pools[0].data(),words.data(),512);headers[slot].prdbc=512;
            p.tfd=identify_fault==4?0x21:0x50;r=r&~value;
        }else if(value){
            ci_doorbells.push_back(value);
            for(size_t i=0;i<8;++i)if((value&(1U<<i))!=0){assert(port_memory[0].async.slots[i].phase==async_rules::Phase::active);if(headers[i].prdtl)assert(dma_states[i].owned);}
            if(headers[slot].prdtl!=0&&port_memory[0].async.slots[slot].ncq)r=r&~value;
            if(finish_after_doorbell){finish_after_doorbell=false;finish_on_next_ci_read=true;}
            if(finish_after_published_readback){flush_doorbell_published=true;flush_ci_reads=0;}
            if((finish_flush_doorbell&&fis->command==ATA_CMD_FLUSH_CACHE_EXT)||(finish_data_doorbell&&headers[slot].prdtl!=0)){
                assert(spin_depth==1&&mutex_depth==1);
                if(headers[slot].prdtl==0){assert(async_rules::owned(port_memory[0].async)==1);for(auto const& state:dma_states)assert(!state.owned);}
                else for(size_t i=0;i<8;++i)if((value&(1U<<i))!=0){assert(dma_states[i].owned);headers[i].prdbc=port_memory[0].async.slots[i].ncq?0:static_cast<uint32_t>(dma_states[i].bytes-(short_write_prdbc?512:0));}
                p.sact=p.sact&~value;
                r=r&~value;flush_doorbell_published=true;flush_ci_reads=0;
                apply_doorbell_fault(slot);
            }
        }
    }
    else if(&r==&p.sact){sact_doorbells.push_back(value);r=r|value;}
    else if(&r==&p.is||&r==&registers.is)r=r&~value;
    else if(&r==&p.sctl){auto const old=r;r=value;if((value&15)==1){assert(spin_depth==0&&stop_proven);for(auto const& state:dma_states)assert(!state.owned);++resets;}else if((old&15)==1)p.tfd=0x130;}
    else r=value;
}
void mmio_or32(volatile uint32_t& r,uint32_t v){++mmio_reads;++mmio_writes;r=r|v;}
void mmio_and32(volatile uint32_t& r,uint32_t v){++mmio_reads;++mmio_writes;r=r&v;}
template<class F>bool wait_until(F fn,uint64_t){assert(spin_depth==0);for(int i=0;i<10000;++i)if(fn())return true;return false;}
bool stop_cmd(volatile HbaPort* p){assert(spin_depth==0);p->cmd=0;p->ci=0;p->sact=0;stop_proven=true;if(irq_during_stop){irq_during_stop=false;p->is=1U<<29;registers.is=1;native_irq(48,nullptr);}return true;}
bool start_cmd(volatile HbaPort* p){assert(spin_depth==0);p->cmd=HBA_PX_CMD_ST|HBA_PX_CMD_FRE;stop_proven=false;return true;}
void fail_stop(size_t i){controller_failed.store(true);quarantine_port(i);}
'''

DMA = r'''
}} // driver namespace
namespace ker::dev::dma {
Error bind_streaming(Handle h,std::span<std::byte> b,Direction){assert(spin_depth==0);auto& s=ahci::dma_states[h.slot];assert(!s.owned&&!s.quarantined);s.bytes=b.size();s.target=b;return Error::none;}
SubmissionResult prepare(Handle h){assert(spin_depth==0);auto& s=ahci::dma_states[h.slot];assert(!s.owned&&!s.quarantined);s.owned=true;++s.sequence;auto const& slot=ahci::port_memory[0].async.slots[h.slot];if(slot.phase==ahci::async_rules::Phase::preparing)prepared_tokens[h.slot]=slot.token;if(cancel_in_prepare){cancel_in_prepare=false;ahci::ahci_async_cancel(&ahci::device.bdev,{1,1,0},-ECANCELED);}return {.submission={h,s.sequence,reinterpret_cast<uint64_t>(ahci::pools[h.slot].data()),s.bytes}};}
Error complete(Submission submission,size_t bytes,Retirement proof){assert(spin_depth==0);auto& s=ahci::dma_states[submission.handle.slot];assert(s.owned&&!s.quarantined&&s.sequence==submission.sequence&&bytes<=s.bytes);if(before_data_retirement&&!identify_retiring)before_data_retirement(submission,bytes,proof);auto const bit=1U<<submission.handle.slot;if(proof==Retirement::device_quiesced){assert(stop_proven);++device_quiesced;}else{assert(((ahci::registers.ports[0].ci|ahci::registers.ports[0].sact)&bit)==0);if(proof==Retirement::never_submitted)++never_submitted;}if(reject_data_completion&&proof==Retirement::command_completed&&!identify_retiring){reject_data_completion=false;++dma_rejections;return Error::invalid_completion;}if(bytes&&s.target.data()!=ahci::pools[submission.handle.slot].data())std::memcpy(s.target.data(),ahci::pools[submission.handle.slot].data(),bytes);s.owned=false;++dma_completes;if(identify_retiring){identify_retiring=false;if(final_status_fault){ahci::registers.ports[0].is=1U<<29;ahci::registers.is=1;if(final_status_fault==2)ahci::native_irq(48,nullptr);}}return Error::none;}
Error quarantine(Handle h){assert(spin_depth==0);auto& s=ahci::dma_states[h.slot];s.quarantined=true;s.owned=false;++quarantines;return Error::quarantined;}
}
namespace ker::dev::ahci {namespace {
void quarantine_port(size_t i){auto& m=port_memory[i];m.failed=true;for(size_t j=0;j<8;++j)(void)dma::quarantine(slot_transfer(m,j).handle);}
bool read_disk(volatile HbaPort*,int,uint32_t,uint32_t,uint32_t,uint8_t*){assert(false);return false;}
bool write_disk(volatile HbaPort*,int,uint32_t,uint32_t,uint32_t,uint8_t*){assert(false);return false;}
bool flush_disk(volatile HbaPort*,int){assert(false);return false;}
'''

TESTS = r'''
void setup(){
    auto& m=port_memory[0];assert(async_rules::owned(m.async)==0&&m.pending_issue_mask==0);m.async={};m.async.depth=8;m.provider=&device.bdev;m.native_enabled=true;m.ncq_supported=true;m.flush_supported=true;m.prepared=true;m.failed=false;m.stats={};m.stats.identify_queue_depth=31;m.stats.identify_sata_caps=0x100;m.stats.identify_command_sets=0x7400;for(size_t i=0;i<10;++i)m.identify_serial[i]=uint16_t(10+i);m.queue_reset_pending.store(false);m.interrupt_status.store(0);m.injected_fault=SelftestAsyncFault::none;
#ifdef WOS_AHCI_HAS_HEALTH_DUE
    m.health_check_due=true;
#endif
    device.port_num=0;device.total_sectors=100000;device.bdev.private_data=&device;device.bdev.total_blocks=device.total_sectors;
    m.clb_virt=headers.data();for(size_t i=0;i<32;++i)m.ctb_virt[i]=reinterpret_cast<uint8_t*>(tables[i].data());
    for(size_t i=0;i<8;++i){auto& pool=slot_transfer(m,i);pool.cpu=pools[i].data();pool.size=TRANSFER_CAPACITY;pool.handle={reinterpret_cast<void*>(1),nullptr,1,i};dma_states[i]={};m.requests[i]=nullptr;m.async_submissions[i]={};}
    registers={};registers.pi=1;registers.ports[0].cmd=17;registers.ports[0].ssts=3;controller_failed.store(false);admission.open_for_probe();completed.clear();ci_doorbells.clear();sact_doorbells.clear();dma_completes=quarantines=resets=wakes=identifies=identify_fault=guarded_irqs=final_status_fault=never_submitted=device_quiesced=0;clock_us=0;stop_proven=false;reset_pending=reset_reopened=false;cancel_in_prepare=irq_during_status_read=irq_during_stop=identify_retiring=fault_during_active_read=finish_after_doorbell=finish_on_next_ci_read=false;legacy_sync_status=0;legacy_sync_operations.clear();port_slots_in_use[0].store(0);
    mmio_reads=mmio_writes=clock_reads=0;after_spin_unlock=nullptr;native_irq_vector=48;
    poll_observing=false;poll_read_order.clear();poll_after_read={};poll_after_clock={};
    finish_flush_doorbell=flush_doorbell_published=flush_fresh_active=flush_fresh_hardware_fault=use_cancel_queue=false;flush_doorbell_fault=flush_ci_reads=0;cancel_queue={};
    finish_data_doorbell=flush_fresh_sact=short_write_prdbc=reject_data_completion=false;service_completion_calls=dma_rejections=0;
    finish_after_published_readback=false;before_data_retirement=nullptr;
    prepared_tokens={};completion_retirement_check=[](BlockRequestToken token){assert(mutex_depth==0&&spin_depth==0);for(size_t i=0;i<8;++i){assert(port_memory[0].requests[i]==nullptr||!(port_memory[0].requests[i]->token==token));assert(!(port_memory[0].async.slots[i].token==token));if(prepared_tokens[i]==token)assert(port_memory[0].async_submissions[i].sequence==0&&!dma_states[i].owned);}};
}
void request_init(BlockRequest& r,std::span<std::byte> bytes,uint16_t index,BlockRequestOperation op=BlockRequestOperation::READ){r.description={op,index,bytes.size()/512,bytes.size()};r.token={1,uint64_t(index)+1,index};r.deadline_us=100000;r.segment_count=bytes.empty()?0:1;r.segments[0]={bytes.data(),bytes.size()};}
int submit_issue(BlockDevice* d,BlockRequest& request){auto const before=completed.size();auto const result=ahci_async_submit(d,request);if(result==0){ahci_async_service(d);assert(completed.size()==before&&port_memory[0].pending_issue_mask==0);}return result;}
void no_irq_recovery_tests(){
    // No IRQ is required to recover fatal IS, taskfile ERR or DF behind BUSY
    // and DRQ. A naive owner-empty return strands this rejected queue request.
    for(uint32_t busy:{uint32_t(ATA_DEV_BUSY),uint32_t(ATA_DEV_DRQ)})for(int fatal=0;fatal<3;++fatal){
        setup();native_irq_vector=0;ahci_async_service(&device.bdev);
        registers.ports[0].tfd=busy|(fatal==1?1U:fatal==2?0x20U:0U);registers.ports[0].is=fatal==0?1U<<29:0;
        BlockRequest rejected;std::array<std::byte,512> rejectedbytes{};request_init(rejected,rejectedbytes,0);
        assert(ahci_async_submit(&device.bdev,rejected)==-EAGAIN&&never_submitted==1&&async_rules::owned(port_memory[0].async)==0&&port_memory[0].interrupt_status.load()==0);
        ahci_async_service(&device.bdev);
        std::fprintf(stderr,"AHCI no-IRQ-recovery busy=%u fatal=%d resets=%d identifies=%d reopened=%u\n",busy,fatal,resets,identifies,unsigned(reset_reopened));
        assert(resets==1&&identifies==1&&reset_reopened&&port_memory[0].async.epoch==2&&!port_memory[0].async.offline&&completed.empty()&&ci_doorbells.empty()&&sact_doorbells.empty());
        assert(dma_completes==2&&device_quiesced==0&&wakes==0);for(auto const& dma:dma_states)assert(!dma.owned);
        // Qualified reset does not introduce an endless follow-up obligation.
        mmio_reads=mmio_writes=clock_reads=0;ahci_async_service(&device.bdev);assert(mmio_reads==0&&mmio_writes==0&&clock_reads==0);
    }
}
void idle_health_tests(){
    // The first ownerless pass qualifies hardware; subsequent truly idle passes
    // consume neither MMIO nor time. This is also the saved-v8 cost control.
    setup();ahci_async_service(&device.bdev);
    assert(mmio_reads==4&&mmio_writes==1&&clock_reads==1);
    mmio_reads=mmio_writes=clock_reads=0;
    for(size_t i=0;i<16;++i)ahci_async_service(&device.bdev);
    std::fprintf(stderr,"AHCI idle-cost passes=16 reads=%zu writes=%zu clock_reads=%zu\n",mmio_reads,mmio_writes,clock_reads);
    assert(mmio_reads==0&&mmio_writes==0&&clock_reads==0);
    // A port-lock rejection does not create a reserved attempt or lose the
    // existing service owner's recovery responsibility.
    BlockRequest contended;std::array<std::byte,512> contendbytes{};request_init(contended,contendbytes,0);
    assert(port_locks[0].try_lock());assert(ahci_async_submit(&device.bdev,contended)==-EAGAIN);ahci_async_service(&device.bdev);port_locks[0].unlock();ahci_async_service(&device.bdev);
    assert(mmio_reads==0&&mmio_writes==0&&clock_reads==0&&dma_completes==0);
    // Healthy BUSY/DRQ rejection retires prepared DMA but obligates exactly the
    // following full POST snapshot even though every software slot is FREE.
    for(uint32_t busy:{uint32_t(ATA_DEV_BUSY),uint32_t(ATA_DEV_DRQ)}){
        setup();ahci_async_service(&device.bdev);registers.ports[0].tfd=busy;
        BlockRequest rejected;std::array<std::byte,512> rejectedbytes{};request_init(rejected,rejectedbytes,0);
        assert(ahci_async_submit(&device.bdev,rejected)==-EAGAIN&&never_submitted==1&&async_rules::owned(port_memory[0].async)==0);
        mmio_reads=mmio_writes=clock_reads=0;ahci_async_service(&device.bdev);
        assert(mmio_reads==4&&mmio_writes==1&&clock_reads==1&&resets==0&&ci_doorbells.empty()&&completed.empty());
        mmio_reads=mmio_writes=clock_reads=0;ahci_async_service(&device.bdev);assert(mmio_reads==0&&mmio_writes==0&&clock_reads==0);
    }
    no_irq_recovery_tests();
    // Truly idle health may be lazy, but the next accepted stage must inspect
    // hidden hardware errors BEFORE any data doorbell or successful copy.
    for(int fatal=0;fatal<3;++fatal){
        setup();native_irq_vector=0;ahci_async_service(&device.bdev);registers.ports[0].is=fatal==0?1U<<29:0;registers.ports[0].tfd=fatal==1?1U:fatal==2?0x20U:0;
        mmio_reads=mmio_writes=clock_reads=0;ahci_async_service(&device.bdev);assert(mmio_reads==0&&mmio_writes==0&&clock_reads==0);
        BlockRequest staged;std::array<std::byte,512> stagedbytes{};stagedbytes.fill(std::byte{0xcc});request_init(staged,stagedbytes,0);
        assert(ahci_async_submit(&device.bdev,staged)==0);ahci_async_service(&device.bdev);
        assert(completed.size()==1&&completed[0].status==-EIO&&completed[0].bytes==0&&stagedbytes[0]==std::byte{0xcc}&&never_submitted==1&&device_quiesced==0&&ci_doorbells.empty()&&sact_doorbells.empty()&&resets==1&&reset_reopened);
    }
    // IRQ arrives immediately after the fast-return guard releases, while the
    // port mutex is still held. Its latch and wake survive for the next pass.
    setup();ahci_async_service(&device.bdev);
    after_spin_unlock=[](){assert(mutex_depth==1&&spin_depth==0);registers.ports[0].is=1U<<29;registers.is=1;native_irq(48,nullptr);};
    ahci_async_service(&device.bdev);assert(wakes==1&&port_memory[0].interrupt_status.load()==1U<<29&&resets==0);
    ahci_async_service(&device.bdev);assert(resets==1&&identifies==1&&reset_reopened&&port_memory[0].interrupt_status.load()==0);
    // A queue reset obligation cannot take the healthy idle shortcut. Failed
    // ownerless ports retain their existing reset-finish(false) branch.
    setup();ahci_async_service(&device.bdev);port_memory[0].queue_reset_pending.store(true);reset_pending=true;
    mmio_reads=mmio_writes=clock_reads=0;ahci_async_service(&device.bdev);assert(mmio_reads==4&&mmio_writes==1&&clock_reads==1&&reset_reopened&&!reset_pending&&!port_memory[0].queue_reset_pending.load());
    setup();port_memory[0].failed=true;port_memory[0].queue_reset_pending.store(true);reset_pending=true;
    ahci_async_service(&device.bdev);assert(mmio_reads==0&&mmio_writes==0&&clock_reads==0&&!reset_reopened&&!reset_pending&&!port_memory[0].queue_reset_pending.load());
    for(bool controller_fault:{false,true}){
        setup();ahci_async_service(&device.bdev);if(controller_fault)controller_failed.store(true);else async_rules::freeze(port_memory[0].async);
        mmio_reads=mmio_writes=clock_reads=0;ahci_async_service(&device.bdev);
        assert(mmio_reads>=8&&clock_reads>=1&&port_memory[0].stats.resets==1&&reset_reopened==!controller_fault);
        assert(controller_fault?(port_memory[0].failed&&port_memory[0].async.offline):(!port_memory[0].failed&&!port_memory[0].async.offline));
    }
}
void flush_completion_tests(){
    // The command has no native payload DMA. A real inactive readback can
    // qualify it in ONE publishing provider pass without waiting for service2.
    setup();BlockRequest instant;request_init(instant,{},0,BlockRequestOperation::FLUSH);finish_flush_doorbell=true;
    assert(ahci_async_submit(&device.bdev,instant)==0);ahci_async_service(&device.bdev);
    std::fprintf(stderr,"AHCI instant-flush publish_passes=1 completions=%zu data_dma_retires=%d\n",completed.size(),dma_completes);
    assert(completed.size()==1&&completed[0].status==0&&completed[0].bytes==0&&dma_completes==0&&never_submitted==0&&device_quiesced==0&&ci_doorbells==std::vector<uint32_t>{1}&&sact_doorbells.empty());
    assert(async_rules::owned(port_memory[0].async)==0&&port_memory[0].pending_issue_mask==0&&port_memory[0].stats.submitted==1&&port_memory[0].stats.completed==1&&clock_reads==2);
    ahci_async_service(&device.bdev);assert(completed.size()==1);for(auto const& state:dma_states)assert(!state.owned);
    BlockRequest reused;request_init(reused,{},1,BlockRequestOperation::FLUSH);reused.description.lba=0;
    assert(ahci_async_submit(&device.bdev,reused)==0);ahci_async_service(&device.bdev);
    assert(completed.size()==2&&completed[1].token==reused.token&&completed[1].status==0&&port_memory[0].async.slots[0].generation==2&&port_memory[0].stats.submitted==2&&port_memory[0].stats.completed==2&&dma_completes==0);
    // One fresh full qualification observes still-active hardware and retains ownership.
    setup();BlockRequest active;request_init(active,{},0,BlockRequestOperation::FLUSH);assert(ahci_async_submit(&device.bdev,active)==0);ahci_async_service(&device.bdev);
    assert(completed.empty()&&registers.ports[0].ci==1&&clock_reads==2&&mmio_reads==11&&port_memory[0].async.slots[0].phase==async_rules::Phase::active);
    registers.ports[0].ci=0;ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==0&&dma_completes==0);
    // The complete fresh snapshot remains authoritative if hardware becomes
    // active between readback and qualification, and stats record that sample.
    setup();BlockRequest refreshed;request_init(refreshed,{},0,BlockRequestOperation::FLUSH);finish_flush_doorbell=flush_fresh_active=true;
    assert(ahci_async_submit(&device.bdev,refreshed)==0);ahci_async_service(&device.bdev);
    assert(completed.empty()&&registers.ports[0].ci==1&&clock_reads==2&&port_memory[0].stats.current_ci==1&&port_memory[0].stats.hardware_high_water==1&&port_memory[0].async.slots[0].phase==async_rules::Phase::active);
    registers.ports[0].ci=0;ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==0);
    // Clear active readback never hides a newly latched fault or a deadline
    // crossing the doorbell. Selftest intentions exist before publication and
    // must be consumed AFTER the slot becomes issued, before any fast claim.
    for(int cause:{1,2,3,4,5,6,7,8,9,10,11,12,13,15,16}){
        setup();BlockRequest failed;request_init(failed,{},0,BlockRequestOperation::FLUSH);finish_flush_doorbell=true;flush_doorbell_fault=cause;
        use_cancel_queue=true;assert(block_model_submit(cancel_queue,failed.description,failed.token)==0);BlockRequestToken issued{};assert(block_model_next_issue(cancel_queue,issued)&&issued==failed.token);
        if(cause>=7&&cause<=10){SelftestAsyncFault faults[]{SelftestAsyncFault::timeout,SelftestAsyncFault::taskfile,SelftestAsyncFault::stop,SelftestAsyncFault::reset};port_memory[0].injected_fault=faults[cause-7];}
        assert(ahci_async_submit(&device.bdev,failed)==0);ahci_async_service(&device.bdev);
        int const expected=cause==6||cause==7?-ETIMEDOUT:cause>=11&&cause<=13?-ESHUTDOWN:-EIO;
        std::fprintf(stderr,"AHCI postdoorbell-flush cause=%d completions=%zu status=%d\n",cause,completed.size(),completed.empty()?999:completed[0].status);
        assert(completed.size()==1&&completed[0].status==expected&&completed[0].bytes==0&&port_memory[0].pending_issue_mask==0&&async_rules::owned(port_memory[0].async)==0&&port_memory[0].stats.completed==1&&port_memory[0].stats.resets==1);
        bool const can_reopen=cause!=4&&cause!=9&&cause!=10&&cause!=11&&cause!=12;
        assert(reset_reopened==can_reopen&&port_memory[0].failed==!can_reopen&&port_memory[0].async.offline==!can_reopen);
        assert(ci_doorbells==std::vector<uint32_t>{1}&&sact_doorbells.empty()&&never_submitted==0&&device_quiesced==0&&dma_completes==identifies&&quarantines==(cause==9?8:0));
        assert(port_memory[0].injected_fault==SelftestAsyncFault::none);for(auto const& state:dma_states)assert(!state.owned);
        if(cause==15)assert(guarded_irqs==1&&!irq_during_status_read);if(cause==16)assert(!flush_fresh_hardware_fault);
        ahci_async_service(&device.bdev);assert(completed.size()==1);
        assert(block_model_release(cancel_queue,failed.token)&&cancel_queue.owned_requests==0&&cancel_queue.owned_bytes==0&&cancel_queue.stats.submitted==cancel_queue.stats.completed&&cancel_queue.stats.completed==cancel_queue.stats.released);
    }
    // Runtime cancellation intention can race while the driver guard excludes
    // native cancel. Its actual pure model remains authoritative on completion.
    setup();BlockRequest canceled;request_init(canceled,{},0,BlockRequestOperation::FLUSH);use_cancel_queue=true;
    assert(block_model_submit(cancel_queue,canceled.description,canceled.token)==0);BlockRequestToken issued{};assert(block_model_next_issue(cancel_queue,issued)&&issued==canceled.token);
    finish_flush_doorbell=true;flush_doorbell_fault=14;assert(ahci_async_submit(&device.bdev,canceled)==0);ahci_async_service(&device.bdev);
    assert(completed.size()==1&&completed[0].status==-ECANCELED&&completed[0].bytes==0&&dma_completes==0&&resets==0&&async_rules::owned(port_memory[0].async)==0);
    assert(block_model_release(cancel_queue,canceled.token)&&cancel_queue.owned_requests==0&&cancel_queue.owned_bytes==0&&cancel_queue.stats.submitted==cancel_queue.stats.completed&&cancel_queue.stats.completed==cancel_queue.stats.released);
    // A later IRQ cannot undo an already qualified completion or poison a
    // reused tag unnoticed. Next staged work observes it before any doorbell.
    setup();BlockRequest early;request_init(early,{},0,BlockRequestOperation::FLUSH);finish_flush_doorbell=true;assert(ahci_async_submit(&device.bdev,early)==0);
    after_spin_unlock=[](){assert(mutex_depth==1&&spin_depth==0);registers.ports[0].is=1U<<29;registers.is=1;native_irq(48,nullptr);};
    ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==0&&wakes==1&&port_memory[0].interrupt_status.load()==1U<<29);
    BlockRequest later;request_init(later,{},1,BlockRequestOperation::FLUSH);later.description.lba=0;assert(ahci_async_submit(&device.bdev,later)==0);ahci_async_service(&device.bdev);
    assert(completed.size()==2&&completed[1].status==-EIO&&ci_doorbells==std::vector<uint32_t>{1}&&port_memory[0].async.epoch==2&&port_memory[0].async.slots[0].generation==2&&reset_reopened&&async_rules::owned(port_memory[0].async)==0&&dma_completes==identifies);
}
void flush_legacy_two_pass_control(){
    setup();BlockRequest flush;request_init(flush,{},0,BlockRequestOperation::FLUSH);finish_flush_doorbell=true;
    assert(ahci_async_submit(&device.bdev,flush)==0);ahci_async_service(&device.bdev);
    assert(completed.empty()&&registers.ports[0].ci==0&&registers.ports[0].sact==0&&port_memory[0].async.slots[0].phase==async_rules::Phase::active);
    ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==0&&completed[0].bytes==0&&dma_completes==0&&async_rules::owned(port_memory[0].async)==0);
    std::fprintf(stderr,"AHCI legacy-instant-flush publishing_completions=0 after_second_pass=1 data_dma_retires=0\n");
}
void write_completion_tests(){
    // WRITE uses the existing DMA-owned pool, including FPDMA on NCQ ports.
    // A command may finish in the doorbell exit, but retirement still precedes
    // slot release and the task-service completion outside both driver locks.
    for(bool ncq:{false,true})for(size_t size:{size_t(512),size_t(4096),TRANSFER_CAPACITY}){
        setup();port_memory[0].ncq_supported=ncq;BlockRequest request;std::vector<std::byte> bytes(size,std::byte{0x5a});
        request_init(request,bytes,0,BlockRequestOperation::WRITE);
        request.segment_count=2;request.segments[0]={bytes.data(),size/2};request.segments[1]={bytes.data()+size/2,size/2};
        finish_data_doorbell=true;assert(ahci_async_submit(&device.bdev,request)==0);
        assert(std::memcmp(pools[0].data(),bytes.data(),size)==0&&dma_states[0].owned&&headers[0].w==1);
        assert(reinterpret_cast<FisRegH2D*>(tables[0].data())->command==(ncq?0x61:ATA_CMD_WRITE_DMA_EX));
        ahci_async_service(&device.bdev);
        std::fprintf(stderr,"AHCI instant-write ncq=%u bytes=%zu publishing_completions=%zu dma_retirements=%d\n",unsigned(ncq),size,completed.size(),dma_completes);
        assert(completed.size()==1&&completed[0].token==request.token&&completed[0].status==0&&completed[0].bytes==size);
        assert(dma_completes==1&&device_quiesced==0&&never_submitted==0&&service_completion_calls==1&&!dma_states[0].owned);
        assert(async_rules::owned(port_memory[0].async)==0&&port_memory[0].pending_issue_mask==0&&port_memory[0].stats.submitted==1&&port_memory[0].stats.completed==1&&clock_reads==2);
        assert(port_memory[0].stats.hardware_high_water==0&&port_memory[0].stats.current_ci==0&&port_memory[0].stats.current_sact==0);
        assert(ci_doorbells==std::vector<uint32_t>{1}&&sact_doorbells==(ncq?std::vector<uint32_t>{1}:std::vector<uint32_t>{}));
        assert(headers[0].prdbc==(ncq?0:size));ahci_async_service(&device.bdev);assert(completed.size()==1&&dma_completes==1);
        BlockRequest reused;request_init(reused,bytes,1,BlockRequestOperation::WRITE);assert(ahci_async_submit(&device.bdev,reused)==0);ahci_async_service(&device.bdev);
        assert(completed.size()==2&&completed[1].token==reused.token&&completed[1].status==0&&completed[1].bytes==size&&dma_completes==2&&service_completion_calls==2&&port_memory[0].async.slots[0].generation==2);
    }
    for(bool ncq:{false,true}){
        // The single fresh qualification cannot claim active published hardware.
        setup();port_memory[0].ncq_supported=ncq;BlockRequest active;std::array<std::byte,512> bytes{};request_init(active,bytes,0,BlockRequestOperation::WRITE);
        assert(ahci_async_submit(&device.bdev,active)==0);ahci_async_service(&device.bdev);
        assert(completed.empty()&&dma_states[0].owned&&clock_reads==2&&mmio_reads==11&&port_memory[0].stats.hardware_high_water==1);
        assert((ncq?registers.ports[0].sact:registers.ports[0].ci)==1);
        registers.ports[0].ci=registers.ports[0].sact=0;headers[0].prdbc=512;ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==0);
        // Even clear published readback cannot override freshly active CI/SACT.
        for(bool sact:{false,true}){
            setup();port_memory[0].ncq_supported=ncq;BlockRequest fresh;request_init(fresh,bytes,0,BlockRequestOperation::WRITE);
            finish_data_doorbell=true;flush_fresh_active=!sact;flush_fresh_sact=sact;
            assert(ahci_async_submit(&device.bdev,fresh)==0);ahci_async_service(&device.bdev);
            assert(completed.empty()&&dma_states[0].owned&&clock_reads==2&&port_memory[0].stats.hardware_high_water==1);
            assert((sact?registers.ports[0].sact:registers.ports[0].ci)==1&&port_memory[0].async.slots[0].phase==async_rules::Phase::active);
            registers.ports[0].ci=registers.ports[0].sact=0;ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==0);
        }
    }
    // Sole READ uses the same qualified DMA-before-copy path. Multiple owners
    // remain excluded even if the entire published hardware batch is clear.
    setup();BlockRequest read;std::array<std::byte,512> readbytes;readbytes.fill(std::byte{0xcc});request_init(read,readbytes,0);
    finish_data_doorbell=true;assert(ahci_async_submit(&device.bdev,read)==0);std::memset(pools[0].data(),0xaa,512);ahci_async_service(&device.bdev);
    assert(completed.size()==1&&!dma_states[0].owned&&clock_reads==2&&readbytes[0]==std::byte{0xaa});ahci_async_service(&device.bdev);assert(completed.size()==1&&dma_completes==1);
    setup();std::array<BlockRequest,2> batch;std::array<std::array<std::byte,512>,2> batchbytes{};finish_data_doorbell=true;
    for(size_t i=0;i<2;++i){request_init(batch[i],batchbytes[i],i,BlockRequestOperation::WRITE);assert(ahci_async_submit(&device.bdev,batch[i])==0);}
    ahci_async_service(&device.bdev);assert(completed.empty()&&dma_completes==0&&clock_reads==1&&ci_doorbells==std::vector<uint32_t>{3}&&sact_doorbells==std::vector<uint32_t>{3});
    ahci_async_service(&device.bdev);assert(completed.size()==2&&dma_completes==2);
    // Bad retained eligibility never claims the newly published slot. Restore
    // it before the ordinary later completion to retire the fixture's owner.
    for(int bad=0;bad<6;++bad){
        setup();BlockRequest malformed;std::array<std::byte,512> bytes{};request_init(malformed,bytes,0,BlockRequestOperation::WRITE);
        finish_data_doorbell=true;assert(ahci_async_submit(&device.bdev,malformed)==0);
        auto& slot=port_memory[0].async.slots[0];auto& submission=port_memory[0].async_submissions[0];auto const token=malformed.token;auto const description=malformed.description;auto const saved_slot=slot;auto const saved_submission=submission;
        if(bad==0)++malformed.token.request_generation;if(bad==1)slot.bytes=0;if(bad==2)malformed.description.bytes+=512;if(bad==3)submission.size+=512;if(bad==4)submission.sequence=0;if(bad==5)malformed.description.op=BlockRequestOperation::FLUSH;
        ahci_async_service(&device.bdev);assert(completed.empty()&&dma_states[0].owned&&clock_reads==1&&slot.phase==async_rules::Phase::active);
        malformed.token=token;malformed.description=description;slot.bytes=saved_slot.bytes;submission=saved_submission;
        ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==0&&dma_completes==1);
    }
    setup();port_memory[0].ncq_supported=false;BlockRequest short_write;std::array<std::byte,4096> shortbytes{};request_init(short_write,shortbytes,0,BlockRequestOperation::WRITE);
    finish_data_doorbell=short_write_prdbc=true;assert(ahci_async_submit(&device.bdev,short_write)==0);ahci_async_service(&device.bdev);
    assert(completed.size()==1&&completed[0].status==-EIO&&completed[0].bytes==0&&dma_completes==1&&!dma_states[0].owned&&quarantines==0);
    setup();BlockRequest rejected;std::array<std::byte,512> rejectedbytes{};request_init(rejected,rejectedbytes,0,BlockRequestOperation::WRITE);
    finish_data_doorbell=reject_data_completion=true;assert(ahci_async_submit(&device.bdev,rejected)==0);ahci_async_service(&device.bdev);
    assert(completed.size()==1&&completed[0].status==-EIO&&completed[0].bytes==0&&dma_rejections==1&&dma_completes==0&&quarantines==1&&dma_states[0].quarantined&&port_memory[0].failed);
    assert(ahci_async_submit(&device.bdev,rejected)==-ENODEV);ahci_async_service(&device.bdev);assert(completed.size()==1);
    // Repeat the post-doorbell FLUSH matrix with real retained DATA ownership.
    // Published payloads must retire via quiescence, never never_submitted.
    for(bool ncq:{false,true})for(int cause:{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}){
        setup();port_memory[0].ncq_supported=ncq;BlockRequest failed;std::array<std::byte,4096> bytes;bytes.fill(std::byte{0x5a});request_init(failed,bytes,0,BlockRequestOperation::WRITE);
        finish_data_doorbell=true;flush_doorbell_fault=cause;use_cancel_queue=true;assert(block_model_submit(cancel_queue,failed.description,failed.token)==0);BlockRequestToken issued{};assert(block_model_next_issue(cancel_queue,issued)&&issued==failed.token);
        if(cause>=7&&cause<=10){SelftestAsyncFault faults[]{SelftestAsyncFault::timeout,SelftestAsyncFault::taskfile,SelftestAsyncFault::stop,SelftestAsyncFault::reset};port_memory[0].injected_fault=faults[cause-7];}
        assert(ahci_async_submit(&device.bdev,failed)==0);ahci_async_service(&device.bdev);
        int const expected=cause==14?-ECANCELED:cause==6||cause==7?-ETIMEDOUT:cause>=11&&cause<=13?-ESHUTDOWN:-EIO;
        std::fprintf(stderr,"AHCI postdoorbell-write ncq=%u cause=%d completions=%zu status=%d dma_quiesced=%d dma_retirements=%d identify=%d hardware_resets=%d\n",unsigned(ncq),cause,completed.size(),completed.empty()?999:completed[0].status,device_quiesced,dma_completes,identifies,resets);
        assert(completed.size()==1&&completed[0].status==expected&&completed[0].bytes==0&&port_memory[0].pending_issue_mask==0&&async_rules::owned(port_memory[0].async)==0&&service_completion_calls==1&&port_memory[0].stats.completed==1);
        assert(never_submitted==0&&device_quiesced==(cause==9||cause==14?0:1)&&quarantines==(cause==9?8:0));
        assert(dma_completes==identifies+(cause==9?0:1)&&port_memory[0].stats.resets==(cause==14?0:1)&&port_memory[0].injected_fault==SelftestAsyncFault::none);
        bool const can_reopen=cause!=4&&cause!=9&&cause!=10&&cause!=11&&cause!=12&&cause!=14;
        assert(reset_reopened==can_reopen);if(cause!=14)assert(port_memory[0].failed==!can_reopen&&port_memory[0].async.offline==!can_reopen);
        for(auto const& state:dma_states)assert(!state.owned);for(auto byte:bytes)assert(byte==std::byte{0x5a});
        if(cause==15)assert(guarded_irqs==1&&!irq_during_status_read);if(cause==16)assert(!flush_fresh_hardware_fault);
        assert(block_model_release(cancel_queue,failed.token)&&cancel_queue.owned_requests==0&&cancel_queue.owned_bytes==0&&cancel_queue.stats.submitted==cancel_queue.stats.completed&&cancel_queue.stats.completed==cancel_queue.stats.released);
        ahci_async_service(&device.bdev);assert(completed.size()==1);
    }
    // Late IRQ status is retained across qualified completion and prevents
    // publication of a reused slot. No future-error detection is fabricated.
    setup();BlockRequest early;std::array<std::byte,512> earlybytes{};request_init(early,earlybytes,0,BlockRequestOperation::WRITE);finish_data_doorbell=true;assert(ahci_async_submit(&device.bdev,early)==0);
    after_spin_unlock=[](){registers.ports[0].is=1U<<29;registers.is=1;native_irq(48,nullptr);};ahci_async_service(&device.bdev);
    assert(completed.size()==1&&completed[0].status==0&&dma_completes==1&&wakes==1&&port_memory[0].interrupt_status.load()==1U<<29);
    BlockRequest later;std::array<std::byte,512> laterbytes{};request_init(later,laterbytes,1,BlockRequestOperation::WRITE);assert(ahci_async_submit(&device.bdev,later)==0);ahci_async_service(&device.bdev);
    assert(completed.size()==2&&completed[1].status==-EIO&&completed[1].bytes==0&&ci_doorbells==std::vector<uint32_t>{1}&&never_submitted==1&&dma_completes==3&&resets==1&&reset_reopened&&port_memory[0].async.epoch==2&&port_memory[0].async.slots[0].generation==2);
    BlockRequest recovered;request_init(recovered,laterbytes,2,BlockRequestOperation::WRITE);assert(ahci_async_submit(&device.bdev,recovered)==0);ahci_async_service(&device.bdev);
    assert(completed.size()==3&&completed[2].token==recovered.token&&completed[2].status==0&&completed[2].bytes==512&&dma_completes==4&&port_memory[0].async.slots[0].generation==3&&port_memory[0].async.epoch==2&&ci_doorbells==std::vector<uint32_t>({1,1}));
    std::fprintf(stderr,"AHCI sole-write matrix PASS\n");
}
void write_legacy_two_pass_control(){
    for(bool ncq:{false,true}){
        setup();port_memory[0].ncq_supported=ncq;BlockRequest write;std::array<std::byte,4096> bytes;bytes.fill(std::byte{0x5a});request_init(write,bytes,0,BlockRequestOperation::WRITE);finish_data_doorbell=true;
        assert(ahci_async_submit(&device.bdev,write)==0);ahci_async_service(&device.bdev);
        assert(completed.empty()&&dma_states[0].owned&&registers.ports[0].ci==0&&registers.ports[0].sact==0&&clock_reads==1&&port_memory[0].async.slots[0].phase==async_rules::Phase::active);
        ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==0&&completed[0].bytes==4096&&dma_completes==1&&!dma_states[0].owned);
        std::fprintf(stderr,"AHCI legacy-instant-write ncq=%u publishing_completions=0 after_second_pass=1 data_dma_retires=1\n",unsigned(ncq));
    }
}
void write_fresh_fault_control(){
    for(bool ncq:{false,true}){
        setup();port_memory[0].ncq_supported=ncq;BlockRequest write;std::array<std::byte,4096> bytes{};request_init(write,bytes,0,BlockRequestOperation::WRITE);
        finish_data_doorbell=true;flush_doorbell_fault=1;assert(ahci_async_submit(&device.bdev,write)==0);ahci_async_service(&device.bdev);
        std::fprintf(stderr,"AHCI fresh-write-fault ncq=%u completions=%zu status=%d dma_quiesced=%d\n",unsigned(ncq),completed.size(),completed.empty()?999:completed[0].status,device_quiesced);
        assert(completed.size()==1&&completed[0].status==-EIO&&completed[0].bytes==0&&device_quiesced==1&&never_submitted==0&&resets==1&&reset_reopened);
    }
}
void write_fresh_active_control(){
    for(bool ncq:{false,true})for(bool sact:{false,true}){
        setup();port_memory[0].ncq_supported=ncq;BlockRequest write;std::array<std::byte,4096> bytes{};request_init(write,bytes,0,BlockRequestOperation::WRITE);
        finish_data_doorbell=true;flush_fresh_active=!sact;flush_fresh_sact=sact;assert(ahci_async_submit(&device.bdev,write)==0);ahci_async_service(&device.bdev);
        std::fprintf(stderr,"AHCI fresh-write-active ncq=%u sact=%u completions=%zu dma_owned=%u\n",unsigned(ncq),unsigned(sact),completed.size(),unsigned(dma_states[0].owned));
        assert(completed.empty()&&dma_states[0].owned&&dma_completes==0&&(sact?registers.ports[0].sact:registers.ports[0].ci)==1);
        registers.ports[0].ci=registers.ports[0].sact=0;ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==0);
    }
}
std::span<std::byte> observed_read;
size_t read_retirement_checks=0;
void expect_poison(std::span<const std::byte> bytes){assert(std::all_of(bytes.begin(),bytes.end(),[](auto byte){return byte==std::byte{0xcc};}));}
void watch_read_retirement(dma::Submission submission,size_t bytes,dma::Retirement proof){
    assert(mutex_depth==1&&spin_depth==0&&dma_states[submission.handle.slot].owned);
    assert(proof==dma::Retirement::command_completed&&bytes==observed_read.size());
    expect_poison(observed_read);++read_retirement_checks;
}
void sole_active_to_inactive_tests(bool legacy=false){
    for(auto operation:{BlockRequestOperation::READ,BlockRequestOperation::WRITE,BlockRequestOperation::FLUSH})for(bool ncq:{false,true}){
        if(operation==BlockRequestOperation::FLUSH&&ncq)continue;
        setup();port_memory[0].ncq_supported=ncq;BlockRequest request;std::array<std::byte,4096> bytes;bytes.fill(std::byte{0xcc});
        bool const DATA=operation!=BlockRequestOperation::FLUSH;
        request_init(request,DATA?std::span<std::byte>{bytes}:std::span<std::byte>{},0,operation);
        finish_after_published_readback=true;assert(ahci_async_submit(&device.bdev,request)==0);std::memset(pools[0].data(),0x5a,bytes.size());
        ahci_async_service(&device.bdev);
        std::fprintf(stderr,"AHCI sole-active-to-inactive op=%u ncq=%u publishing_completions=%zu dma_retirements=%d current_active=%zu hardware_high=%zu\n",unsigned(operation),unsigned(ncq),completed.size(),dma_completes,port_memory[0].stats.hardware_active,port_memory[0].stats.hardware_high_water);
        assert(registers.ports[0].ci==0&&registers.ports[0].sact==0&&port_memory[0].stats.hardware_high_water==1);
        if(legacy){
            assert(completed.empty()&&clock_reads==1&&port_memory[0].async.slots[0].phase==async_rules::Phase::active);
            assert(DATA?dma_states[0].owned:!dma_states[0].owned);if(operation==BlockRequestOperation::READ)expect_poison(bytes);
            ahci_async_service(&device.bdev);
            std::fprintf(stderr,"AHCI legacy-active-to-inactive op=%u ncq=%u after_second_pass=%zu\n",unsigned(operation),unsigned(ncq),completed.size());
        }else assert(clock_reads==2&&port_memory[0].stats.hardware_active==0);
        assert(completed.size()==1&&completed[0].status==0&&completed[0].bytes==(DATA?bytes.size():0)&&dma_completes==int(DATA)&&service_completion_calls==1);
        assert(!dma_states[0].owned&&async_rules::owned(port_memory[0].async)==0&&port_memory[0].pending_issue_mask==0);
        if(operation==BlockRequestOperation::READ)assert(std::all_of(bytes.begin(),bytes.end(),[](auto byte){return byte==std::byte{0x5a};}));
    }
}
void read_completion_tests(){
    for(bool ncq:{false,true})for(size_t size:{size_t(512),size_t(4096),TRANSFER_CAPACITY})for(size_t segments:{size_t(1),size_t(32)}){
        setup();port_memory[0].ncq_supported=ncq;BlockRequest request;std::vector<std::byte> guarded(size+64,std::byte{0x7d});
        std::span<std::byte> bytes{guarded.data()+32,size};std::fill(bytes.begin(),bytes.end(),std::byte{0xcc});request_init(request,bytes,0);
        request.segment_count=segments;for(size_t i=0;i<segments;++i)request.segments[i]={bytes.data()+i*(size/segments),size/segments};
        finish_data_doorbell=true;assert(ahci_async_submit(&device.bdev,request)==0);
        assert(dma_states[0].owned&&headers[0].w==0&&reinterpret_cast<FisRegH2D*>(tables[0].data())->command==(ncq?0x60:ATA_CMD_READ_DMA_EX));
        for(size_t i=0;i<size;++i)pools[0][i]=std::byte((i*37+0x5a)&0xff);
        std::vector<std::byte> const expected(pools[0].begin(),pools[0].begin()+size);
        observed_read=bytes;read_retirement_checks=0;before_data_retirement=watch_read_retirement;
        ahci_async_service(&device.bdev);
        std::fprintf(stderr,"AHCI instant-read ncq=%u bytes=%zu sg=%zu publishing_completions=%zu dma_retirements=%d retirement_checks=%zu\n",unsigned(ncq),size,segments,completed.size(),dma_completes,read_retirement_checks);
        assert(completed.size()==1&&completed[0].status==0&&completed[0].bytes==size&&service_completion_calls==1&&dma_completes==1&&read_retirement_checks==1);
        assert(std::equal(bytes.begin(),bytes.end(),expected.begin())&&!dma_states[0].owned&&async_rules::owned(port_memory[0].async)==0&&clock_reads==2);
        assert(std::all_of(guarded.begin(),guarded.begin()+32,[](auto b){return b==std::byte{0x7d};})&&std::all_of(guarded.end()-32,guarded.end(),[](auto b){return b==std::byte{0x7d};}));
        assert(headers[0].prdbc==(ncq?0:size)&&port_memory[0].stats.hardware_high_water==0);before_data_retirement=nullptr;
        ahci_async_service(&device.bdev);assert(completed.size()==1&&dma_completes==1);
    }
    // A published or freshly reasserted active bit prevents READ copy/retirement.
    for(bool ncq:{false,true})for(int active:{0,1,2}){
        setup();port_memory[0].ncq_supported=ncq;BlockRequest read;std::array<std::byte,4096> bytes;bytes.fill(std::byte{0xcc});request_init(read,bytes,0);
        if(active){finish_data_doorbell=true;flush_fresh_active=active==1;flush_fresh_sact=active==2;}
        assert(ahci_async_submit(&device.bdev,read)==0);std::memset(pools[0].data(),0x5a,bytes.size());ahci_async_service(&device.bdev);
        assert(completed.empty()&&dma_states[0].owned&&dma_completes==0&&clock_reads==2&&port_memory[0].stats.hardware_high_water==1);expect_poison(bytes);
        registers.ports[0].ci=registers.ports[0].sact=0;headers[0].prdbc=ncq?0:bytes.size();ahci_async_service(&device.bdev);
        assert(completed.size()==1&&completed[0].status==0&&bytes[0]==std::byte{0x5a});
    }
    // Malformed retained READ eligibility cannot bypass the ordinary later
    // observation; repair the descriptor only to drain its fixture lifetime.
    for(bool ncq:{false,true})for(int bad=0;bad<6;++bad){
        setup();port_memory[0].ncq_supported=ncq;BlockRequest malformed;std::array<std::byte,4096> bytes;bytes.fill(std::byte{0xcc});request_init(malformed,bytes,0);
        finish_data_doorbell=true;assert(ahci_async_submit(&device.bdev,malformed)==0);std::memset(pools[0].data(),0x5a,bytes.size());
        auto& slot=port_memory[0].async.slots[0];auto& submission=port_memory[0].async_submissions[0];auto const token=malformed.token;auto const description=malformed.description;auto const saved_slot=slot;auto const saved_submission=submission;
        if(bad==0)++malformed.token.request_generation;if(bad==1)slot.bytes=0;if(bad==2)malformed.description.bytes+=512;if(bad==3)submission.size+=512;if(bad==4)submission.sequence=0;if(bad==5)malformed.description.op=BlockRequestOperation::FLUSH;
        ahci_async_service(&device.bdev);assert(completed.empty()&&dma_states[0].owned&&clock_reads==1&&slot.phase==async_rules::Phase::active);expect_poison(bytes);
        malformed.token=token;malformed.description=description;slot.bytes=saved_slot.bytes;submission=saved_submission;
        ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==0&&dma_completes==1&&bytes[0]==std::byte{0x5a});
    }
    // Every postpublication error is qualified before DMA receive/copy, both
    // for initially clear and genuinely active captured published readback.
    for(bool ncq:{false,true})for(bool published_active:{false,true})for(int cause:{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}){
        setup();port_memory[0].ncq_supported=ncq;BlockRequest failed;std::array<std::byte,4096> bytes;bytes.fill(std::byte{0xcc});request_init(failed,bytes,0);
        finish_data_doorbell=!published_active;finish_after_published_readback=published_active;flush_doorbell_fault=cause;use_cancel_queue=true;
        assert(block_model_submit(cancel_queue,failed.description,failed.token)==0);BlockRequestToken issued{};assert(block_model_next_issue(cancel_queue,issued)&&issued==failed.token);
        if(cause>=7&&cause<=10){SelftestAsyncFault faults[]{SelftestAsyncFault::timeout,SelftestAsyncFault::taskfile,SelftestAsyncFault::stop,SelftestAsyncFault::reset};port_memory[0].injected_fault=faults[cause-7];}
        assert(ahci_async_submit(&device.bdev,failed)==0);std::memset(pools[0].data(),0x5a,bytes.size());ahci_async_service(&device.bdev);
        int const expected=cause==14?-ECANCELED:cause==6||cause==7?-ETIMEDOUT:cause>=11&&cause<=13?-ESHUTDOWN:-EIO;
        std::fprintf(stderr,"AHCI postdoorbell-read ncq=%u published_active=%u cause=%d completions=%zu status=%d dma_quiesced=%d\n",unsigned(ncq),unsigned(published_active),cause,completed.size(),completed.empty()?999:completed[0].status,device_quiesced);
        assert(completed.size()==1&&completed[0].status==expected&&completed[0].bytes==0&&service_completion_calls==1&&port_memory[0].pending_issue_mask==0&&async_rules::owned(port_memory[0].async)==0);
        assert(never_submitted==0&&device_quiesced==(cause==9||cause==14?0:1)&&quarantines==(cause==9?8:0));
        if(cause==14){assert(resets==0&&std::all_of(bytes.begin(),bytes.end(),[](auto b){return b==std::byte{0x5a};}));}
        else expect_poison(bytes);
        for(auto const& state:dma_states)assert(!state.owned);
        if(cause==15)assert(guarded_irqs==1&&!irq_during_status_read);if(cause==16)assert(!flush_fresh_hardware_fault);
        assert(port_memory[0].injected_fault==SelftestAsyncFault::none&&block_model_release(cancel_queue,failed.token));
        assert(cancel_queue.owned_requests==0&&cancel_queue.owned_bytes==0&&cancel_queue.stats.submitted==cancel_queue.stats.completed&&cancel_queue.stats.completed==cancel_queue.stats.released);
        ahci_async_service(&device.bdev);assert(completed.size()==1);
    }
    for(bool rejected_dma:{false,true}){
        setup();port_memory[0].ncq_supported=rejected_dma;BlockRequest failed;std::array<std::byte,4096> bytes;bytes.fill(std::byte{0xcc});request_init(failed,bytes,0);
        finish_data_doorbell=true;short_write_prdbc=!rejected_dma;reject_data_completion=rejected_dma;
        assert(ahci_async_submit(&device.bdev,failed)==0);std::memset(pools[0].data(),0x5a,bytes.size());ahci_async_service(&device.bdev);
        assert(completed.size()==1&&completed[0].status==-EIO&&completed[0].bytes==0&&!dma_states[0].owned);expect_poison(bytes);
        assert(dma_rejections==int(rejected_dma)&&quarantines==int(rejected_dma)&&dma_completes==int(!rejected_dma));
        if(rejected_dma){assert(dma_states[0].quarantined&&port_memory[0].failed&&ahci_async_submit(&device.bdev,failed)==-ENODEV);}
    }
    // A cancellation delivered to the still-active driver slot freezes it,
    // requiring quiesced retirement and excluding the successful READ copy.
    setup();BlockRequest canceled;std::array<std::byte,4096> canceled_bytes;canceled_bytes.fill(std::byte{0xcc});request_init(canceled,canceled_bytes,0);
    assert(ahci_async_submit(&device.bdev,canceled)==0);std::memset(pools[0].data(),0x5a,canceled_bytes.size());ahci_async_service(&device.bdev);
    assert(completed.empty()&&dma_states[0].owned);ahci_async_cancel(&device.bdev,canceled.token,-ECANCELED);ahci_async_service(&device.bdev);
    assert(completed.size()==1&&completed[0].status==-ECANCELED&&completed[0].bytes==0&&device_quiesced==1&&never_submitted==0);expect_poison(canceled_bytes);
    // READ late errors remain latched and fence later tag reuse before publish.
    setup();BlockRequest early;std::array<std::byte,512> bytes;bytes.fill(std::byte{0xcc});request_init(early,bytes,0);finish_data_doorbell=true;
    assert(ahci_async_submit(&device.bdev,early)==0);std::memset(pools[0].data(),0x5a,bytes.size());
    after_spin_unlock=[](){registers.ports[0].is=1U<<29;registers.is=1;native_irq(48,nullptr);};ahci_async_service(&device.bdev);
    assert(completed.size()==1&&completed[0].status==0&&bytes[0]==std::byte{0x5a}&&wakes==1&&port_memory[0].interrupt_status.load()==1U<<29);
    BlockRequest later;bytes.fill(std::byte{0xcc});request_init(later,bytes,1);assert(ahci_async_submit(&device.bdev,later)==0);ahci_async_service(&device.bdev);
    assert(completed.size()==2&&completed[1].status==-EIO&&completed[1].bytes==0&&ci_doorbells==std::vector<uint32_t>{1}&&never_submitted==1&&reset_reopened&&port_memory[0].async.epoch==2);expect_poison(bytes);
    BlockRequest recovered;request_init(recovered,bytes,2);assert(ahci_async_submit(&device.bdev,recovered)==0);std::memset(pools[0].data(),0xa5,bytes.size());ahci_async_service(&device.bdev);
    assert(completed.size()==3&&completed[2].status==0&&bytes[0]==std::byte{0xa5}&&port_memory[0].async.slots[0].generation==3&&port_memory[0].async.epoch==2);
    std::fprintf(stderr,"AHCI sole-read matrix PASS\n");
}
void read_legacy_two_pass_control(){
    for(bool ncq:{false,true}){
        setup();port_memory[0].ncq_supported=ncq;BlockRequest read;std::array<std::byte,4096> bytes;bytes.fill(std::byte{0xcc});request_init(read,bytes,0);finish_data_doorbell=true;
        assert(ahci_async_submit(&device.bdev,read)==0);std::memset(pools[0].data(),0x5a,bytes.size());ahci_async_service(&device.bdev);
        assert(completed.empty()&&dma_states[0].owned&&dma_completes==0&&clock_reads==1);expect_poison(bytes);ahci_async_service(&device.bdev);
        std::fprintf(stderr,"AHCI legacy-instant-read ncq=%u after_second_pass=%zu dma_retirements=%d\n",unsigned(ncq),completed.size(),dma_completes);
        assert(completed.size()==1&&completed[0].status==0&&completed[0].bytes==bytes.size()&&dma_completes==1&&bytes[0]==std::byte{0x5a});
    }
}
void read_fresh_safety_control(bool active){
    for(bool ncq:{false,true})for(bool sact:{false,true}){
        if(!active&&sact)continue;
        setup();port_memory[0].ncq_supported=ncq;BlockRequest read;std::array<std::byte,4096> bytes;bytes.fill(std::byte{0xcc});request_init(read,bytes,0);finish_data_doorbell=true;
        if(active){flush_fresh_active=!sact;flush_fresh_sact=sact;}else flush_doorbell_fault=1;
        assert(ahci_async_submit(&device.bdev,read)==0);std::memset(pools[0].data(),0x5a,bytes.size());ahci_async_service(&device.bdev);
        std::fprintf(stderr,"AHCI fresh-read-safety active=%u ncq=%u completions=%zu status=%d dma_owned=%u destination=%u\n",unsigned(active),unsigned(ncq),completed.size(),completed.empty()?999:completed[0].status,unsigned(dma_states[0].owned),std::to_integer<unsigned>(bytes[0]));
        if(active){assert(completed.empty()&&dma_states[0].owned&&dma_completes==0);expect_poison(bytes);registers.ports[0].ci=registers.ports[0].sact=0;ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==0);}
        else {assert(completed.size()==1&&completed[0].status==-EIO&&device_quiesced==1&&never_submitted==0);expect_poison(bytes);}
    }
}

#ifdef WOS_AHCI_HAS_ACTIVE_POLL
size_t poll_cases=0;
// Snapshot real native ownership and DMA metadata. The hook may read only;
// time and deliberately injected external events are fixture observations.
void poll_check(const char* name,bool expected,size_t reads=3,bool pure=true,BlockDevice* selected=nullptr){
    auto& m=port_memory[0];
    std::array<std::byte,sizeof(PortMemory)> before{};std::memcpy(before.data(),&m,sizeof(m));
    std::array<std::byte,sizeof(dma_states)> dma_before{};std::memcpy(dma_before.data(),dma_states.data(),sizeof(dma_states));
    auto const callbacks=completed.size(),writes=mmio_writes,clocks=clock_reads,mmios=mmio_reads;
    auto const dma=dma_completes,quarantine=quarantines,wake=wakes,reset=resets,service_calls=service_completion_calls;
    auto const mutex_before=mutex_depth,spin_before=spin_depth;
    poll_read_order.clear();poll_observing=true;
    bool const result=ahci_async_poll_active(selected?selected:&device.bdev);
    poll_observing=false;
    std::printf("AHCI active-poll case=%s actual=%d expected=%d reads=%zu clocks=%zu callbacks=%zu dma=%d writes=%zu\n",name,result,expected,mmio_reads-mmios,clock_reads-clocks,completed.size()-callbacks,dma_completes-dma,mmio_writes-writes);std::fflush(stdout);
    assert(result==expected&&mmio_reads-mmios==reads&&clock_reads-clocks==(reads?1:0));
    assert((reads==0&&poll_read_order.empty())||(reads==3&&poll_read_order==std::vector<char>({'C','A','I'})));
    assert(completed.size()==callbacks&&mmio_writes==writes&&dma_completes==dma&&quarantines==quarantine&&resets==reset&&service_completion_calls==service_calls);
    assert(wakes==wake&&mutex_depth==mutex_before&&spin_depth==spin_before);
    assert(std::memcmp(dma_before.data(),dma_states.data(),sizeof(dma_states))==0);
    if(pure)assert(std::memcmp(before.data(),&m,sizeof(m))==0);
    ++poll_cases;
}
struct PollBatch {
    std::array<BlockRequest,8> requests{};
    std::array<std::array<std::byte,512>,8> bytes{};
    size_t count;
    explicit PollBatch(size_t n=2,BlockRequestOperation operation=BlockRequestOperation::READ):count(n){
        setup();assert(n>=2&&n<=8);
        for(size_t i=0;i<n;++i){bytes[i].fill(std::byte{0xcc});request_init(requests[i],bytes[i],i,operation);assert(ahci_async_submit(&device.bdev,requests[i])==0);std::memset(pools[i].data(),0x5a,512);}
        ahci_async_service(&device.bdev);assert(completed.empty()&&dma_completes==0&&port_memory[0].pending_issue_mask==0);
        for(size_t i=0;i<n;++i)assert(port_memory[0].async.slots[i].phase==async_rules::Phase::active&&dma_states[i].owned);
    }
    void finish(){
        poll_after_read={};poll_after_clock={};
        registers.ports[0].ci=0;registers.ports[0].sact=0;ahci_async_service(&device.bdev);
        assert(async_rules::owned(port_memory[0].async)==0&&port_memory[0].pending_issue_mask==0&&completed.size()==count);
        for(auto const& c:completed)assert(c.status==0&&c.bytes==512);
        for(size_t i=0;i<count;++i){assert(!dma_states[i].owned);if(requests[i].description.op==BlockRequestOperation::READ)assert(bytes[i][0]==std::byte{0x5a});}
    }
};
void healthy_active_poll_tests(){
    poll_cases=0;
    for(size_t n=2;n<=8;++n)for(auto op:{BlockRequestOperation::READ,BlockRequestOperation::WRITE}){
        PollBatch batch(n,op);uint32_t const mask=(1U<<n)-1;
        poll_check("healthy-retained-SACT",true);poll_check("healthy-repeat",true);
        registers.ports[0].ci=mask;registers.ports[0].sact=0;poll_check("healthy-retained-CI",true);
        registers.ports[0].ci=mask&0x55;registers.ports[0].sact=mask&~0x55U;poll_check("healthy-retained-union",true);
        for(size_t i=0;i<n;++i)assert(dma_states[i].owned&&batch.bytes[i][0]==std::byte{0xcc});
        batch.finish();
    }
    // Each rejected software eligibility is restored before the unchanged
    // full service retires the original requests; no malformed fixture leaks.
    for(int cause=0;cause<29;++cause){
        PollBatch batch;auto& m=port_memory[0];auto saved_slot=m.async.slots[1];auto saved_submission=m.async_submissions[1];
        auto saved_token=batch.requests[1].token;auto saved_bytes=batch.requests[1].description.bytes;
        switch(cause){
            case 0:m.pending_issue_mask=1;break;case 1:m.health_check_due=true;break;
            case 2:m.async.frozen=true;break;case 3:m.async.offline=true;break;case 4:m.failed=true;break;
            case 5:m.queue_reset_pending.store(true);break;case 6:admission.close();break;
            case 7:controller_failed.store(true);break;case 8:m.interrupt_status.store(8);break;
            case 9:m.injected_fault=SelftestAsyncFault::timeout;break;
            case 10:m.injected_fault=SelftestAsyncFault::stop;break;
            case 11:m.injected_fault=SelftestAsyncFault::reset;break;
            case 12:m.injected_fault=SelftestAsyncFault::taskfile;break;
            case 13:m.async.slots[1].phase=async_rules::Phase::preparing;break;
            case 14:m.async.slots[1].phase=async_rules::Phase::completing;break;
            case 15:m.async.slots[1].ncq=false;break;case 16:m.async.slots[1].cancel_reason=-ECANCELED;break;
            case 17:m.async.slots[1].generation=0;break;case 18:m.async.slots[1].epoch=0;break;
            case 19:++m.async.slots[1].epoch;break;case 20:m.requests[1]=nullptr;break;
            case 21:++batch.requests[1].token.request_generation;break;case 22:m.async.slots[1].bytes=0;break;
            case 23:--batch.requests[1].description.bytes;break;case 24:--m.async_submissions[1].size;break;
            case 25:m.async_submissions[1].sequence=0;break;case 26:m.native_enabled=false;break;
            case 27:m.provider=nullptr;break;case 28:device.port_num=32;break;
        }
        char name[80];std::snprintf(name,sizeof(name),"software-ineligible-%d",cause);poll_check(name,false,0);
        m.pending_issue_mask=0;m.health_check_due=false;m.async.frozen=m.async.offline=m.failed=false;m.queue_reset_pending.store(false);
        admission.open_for_probe();controller_failed.store(false);m.interrupt_status.store(0);m.injected_fault=SelftestAsyncFault::none;
        m.async.slots[1]=saved_slot;m.async_submissions[1]=saved_submission;m.requests[1]=&batch.requests[1];
        batch.requests[1].token=saved_token;batch.requests[1].description.bytes=saved_bytes;m.native_enabled=true;m.provider=&device.bdev;device.port_num=0;
        batch.finish();
    }
    {
        PollBatch batch;assert(port_locks[0].try_lock());poll_check("port-mutex-contention",false,0);port_locks[0].unlock();
        BlockDevice invalid{};poll_check("invalid-descriptor",false,0,true,&invalid);
        device.bdev.private_data=nullptr;poll_check("null-private-data",false,0);device.bdev.private_data=&device;
        batch.finish();
    }
    // No owner, one NCQ owner, and exclusive FLUSH retain mandatory full service.
    setup();ahci_async_service(&device.bdev);poll_check("empty-provider",false,0);
    for(auto op:{BlockRequestOperation::READ,BlockRequestOperation::WRITE,BlockRequestOperation::FLUSH}){
        setup();BlockRequest request;std::array<std::byte,512> bytes{};
        request_init(request,op==BlockRequestOperation::FLUSH?std::span<std::byte>{}:std::span<std::byte>{bytes},0,op);
        assert(submit_issue(&device.bdev,request)==0);poll_check("sole-owner",false,0);
        registers.ports[0].ci=0;registers.ports[0].sact=0;ahci_async_service(&device.bdev);assert(completed.size()==1);
    }
    for(int cause=0;cause<7;++cause){
        PollBatch batch;auto& m=port_memory[0];auto& p=registers.ports[0];
        switch(cause){
            case 0:p.sact=1;break;case 1:p.ci=1U<<31;break;case 2:p.sact=3U|(1U<<31);break;
            case 3:p.is=8;break;case 4:p.is=1U<<29;break;
            case 5:m.async.slots[0].deadline_us=clock_us+100;break;
            case 6:m.async.slots[1].deadline_us=clock_us+100;break;
        }
        char name[80];std::snprintf(name,sizeof(name),"fresh-observation-fallback-%d",cause);poll_check(name,false);
        p.ci=0;p.sact=3;p.is=0;m.async.slots[0].deadline_us=m.async.slots[1].deadline_us=100000;batch.finish();
    }
    {PollBatch batch;for(auto& s:port_memory[0].async.slots)s.deadline_us=0;poll_check("no-slot-deadline",true);batch.finish();}
    // These events arrive after individual register values were captured.
    // Rechecks forbid success while preserving hardware status and DMA owners.
    for(int cause=0;cause<5;++cause){
        PollBatch batch;auto& m=port_memory[0];
        poll_after_read=[&](char which){if(which!='A')return;if(cause==0)registers.ports[0].is=1U<<29;else if(cause==1)m.interrupt_status.store(1U<<29);};
        poll_after_clock=[&]{if(cause==2)m.interrupt_status.store(8);else if(cause==3)controller_failed.store(true);else if(cause==4)admission.close();};
        char name[80];std::snprintf(name,sizeof(name),"post-read-event-%d",cause);poll_check(name,false,3,false);
        poll_after_read={};poll_after_clock={};registers.ports[0].is=0;m.interrupt_status.store(0);controller_failed.store(false);admission.open_for_probe();batch.finish();
    }
    // A raw event after IS itself was sampled can remain retained once. It is
    // never acknowledged or claimed; the next poll falls back to full service.
    {
        PollBatch batch;poll_after_read=[](char which){if(which=='I')registers.ports[0].is=1U<<29;};
        poll_check("late-raw-IS-retained",true);poll_after_read={};poll_check("late-raw-IS-next-fallback",false);
        ahci_async_service(&device.bdev);assert(completed.size()==2&&resets==1&&reset_reopened);
        for(size_t i=0;i<2;++i)assert(completed[i].status==-EIO&&completed[i].bytes==0&&batch.bytes[i][0]==std::byte{0xcc}&&!dma_states[i].owned);
    }
    // A real deferred IRQ writer runs only after the hook's IRQ guard releases.
    {
        PollBatch batch;after_spin_unlock=[](){poll_observing=false;registers.ports[0].is=1U<<29;registers.is=1;native_irq(48,nullptr);};
        // IRQ changes the latch/wake after observation; manually inspect this
        // permitted external event instead of treating it as hook mutation.
        poll_observing=true;assert(ahci_async_poll_active(&device.bdev));poll_observing=false;
        assert(completed.empty()&&dma_completes==0&&wakes==1&&port_memory[0].interrupt_status.load()==(1U<<29));
        poll_check("IRQ-after-guard-fallback",false,0);ahci_async_service(&device.bdev);
        assert(completed.size()==2&&resets==1);for(size_t i=0;i<batch.count;++i)assert(batch.bytes[i][0]==std::byte{0xcc});
    }
    // TFD-only ERR/DF is intentionally outside this pure three-read observation.
    // The next mandatory full service qualifies failure before any READ copy.
    for(uint32_t error:{1U,0x20U}){
        PollBatch batch;registers.ports[0].tfd=error;poll_check("TFD-only-retained",true);
        ahci_async_service(&device.bdev);assert(completed.size()==2&&resets==1&&reset_reopened);
        for(size_t i=0;i<2;++i)assert(completed[i].status==-EIO&&completed[i].bytes==0&&batch.bytes[i][0]==std::byte{0xcc}&&!dma_states[i].owned);
    }
    // A completion-bit transition always requests a full snapshot before DMA
    // retirement, copy and callback. Remaining sole ownership also falls back.
    {
        PollBatch batch;registers.ports[0].sact=2;poll_check("one-command-inactive",false);
        ahci_async_service(&device.bdev);assert(completed.size()==1&&dma_completes==1&&batch.bytes[0][0]==std::byte{0x5a}&&batch.bytes[1][0]==std::byte{0xcc});
        poll_check("remaining-single-command",false,0);batch.finish();
    }
    std::printf("AHCI active-poll matrix PASS cases=%zu preserved_no_claim=1\n",poll_cases);
}
#endif

int tests(){
#ifdef WOS_AHCI_POLL_ONLY
    healthy_active_poll_tests();assert(mutex_depth==0&&spin_depth==0);return 0;
#endif

#ifdef WOS_AHCI_READ_ONLY
    read_completion_tests();assert(mutex_depth==0&&spin_depth==0);return 0;
#endif
#ifdef WOS_AHCI_SOLE_OBSERVATION_ONLY
    sole_active_to_inactive_tests();assert(mutex_depth==0&&spin_depth==0);return 0;
#endif
#ifdef WOS_AHCI_LEGACY_READ_CONTROL
    read_legacy_two_pass_control();assert(mutex_depth==0&&spin_depth==0);return 0;
#endif
#ifdef WOS_AHCI_LEGACY_OBSERVATION_CONTROL
    sole_active_to_inactive_tests(true);assert(mutex_depth==0&&spin_depth==0);return 0;
#endif
#ifdef WOS_AHCI_READ_FAULT_CONTROL
    read_fresh_safety_control(false);assert(mutex_depth==0&&spin_depth==0);return 0;
#endif
#ifdef WOS_AHCI_READ_ACTIVE_CONTROL
    read_fresh_safety_control(true);assert(mutex_depth==0&&spin_depth==0);return 0;
#endif
#ifdef WOS_AHCI_RECOVERY_ONLY
    no_irq_recovery_tests();assert(mutex_depth==0&&spin_depth==0);return 0;
#endif
#ifdef WOS_AHCI_IDLE_ONLY
    idle_health_tests();assert(mutex_depth==0&&spin_depth==0);return 0;
#endif
#ifdef WOS_AHCI_FLUSH_ONLY
    flush_completion_tests();assert(mutex_depth==0&&spin_depth==0);return 0;
#endif
#ifdef WOS_AHCI_LEGACY_FLUSH_CONTROL
    flush_legacy_two_pass_control();assert(mutex_depth==0&&spin_depth==0);return 0;
#endif
#ifdef WOS_AHCI_WRITE_ONLY
    write_completion_tests();assert(mutex_depth==0&&spin_depth==0);return 0;
#endif
#ifdef WOS_AHCI_LEGACY_WRITE_CONTROL
    write_legacy_two_pass_control();assert(mutex_depth==0&&spin_depth==0);return 0;
#endif
#ifdef WOS_AHCI_WRITE_FAULT_CONTROL
    write_fresh_fault_control();assert(mutex_depth==0&&spin_depth==0);return 0;
#endif
#ifdef WOS_AHCI_WRITE_ACTIVE_CONTROL
    write_fresh_active_control();assert(mutex_depth==0&&spin_depth==0);return 0;
#endif
#ifdef WOS_AHCI_HAS_ACTIVE_POLL
#ifndef WOS_AHCI_NATIVE_CONTROLS_ONLY
    healthy_active_poll_tests();
#endif
#endif
    flush_completion_tests();
#ifndef WOS_AHCI_NATIVE_CONTROLS_ONLY
    write_completion_tests();
    read_completion_tests();
    sole_active_to_inactive_tests();
#endif
    idle_health_tests();
    // Eight published NCQ commands have cleared CI but remain DMA owned until SACT clears.
    setup();std::array<BlockRequest,8> reqs;std::array<std::array<std::byte,512>,8> bytes{};
    for(size_t i=0;i<8;++i){request_init(reqs[i],bytes[i],i);assert(ahci_async_submit(&device.bdev,reqs[i])==0);std::memset(pools[i].data(),int(i+1),512);assert(headers[i].prdbc==0);auto* fis=reinterpret_cast<FisRegH2D*>(tables[i].data());assert(fis->command==0x60&&fis->countl==(i<<3)&&fis->featurel==1);}
    assert(wakes==0&&port_memory[0].stats.high_water==8&&registers.ports[0].ci==0&&registers.ports[0].sact==0&&port_memory[0].pending_issue_mask==255&&ci_doorbells.empty()&&sact_doorbells.empty());
    for(auto const& slot:port_memory[0].async.slots)assert(slot.phase==async_rules::Phase::preparing);
    ahci_async_service(&device.bdev);assert(completed.empty()&&dma_completes==0&&port_memory[0].pending_issue_mask==0&&ci_doorbells==std::vector<uint32_t>{255}&&sact_doorbells==std::vector<uint32_t>{255});
    for(auto const& slot:port_memory[0].async.slots)assert(slot.phase==async_rules::Phase::active);
    assert(port_memory[0].stats.hardware_high_water==8&&port_memory[0].stats.hardware_active==8&&port_memory[0].stats.current_ci==0&&port_memory[0].stats.current_sact==255);
    ahci_async_service(&device.bdev);assert(completed.empty()&&dma_completes==0);
    for(size_t i:{size_t(7),size_t(2),size_t(5),size_t(0),size_t(3),size_t(6),size_t(1),size_t(4)}){registers.ports[0].sact=registers.ports[0].sact&~(1U<<i);ahci_async_service(&device.bdev);assert(completed.back().token==reqs[i].token&&completed.back().status==0&&completed.back().bytes==512);assert(bytes[i][0]==std::byte(i+1));}
    assert(dma_completes==8&&async_rules::owned(port_memory[0].async)==0);
    BlockRequest flush;request_init(flush,{},9,BlockRequestOperation::FLUSH);flush.description.lba=0;assert(ahci_async_submit(&device.bdev,flush)==0);assert(reinterpret_cast<FisRegH2D*>(tables[0].data())->command==0xea);ahci_async_service(&device.bdev);assert(registers.ports[0].ci==1&&completed.size()==8);registers.ports[0].ci=0;ahci_async_service(&device.bdev);assert(completed.back().bytes==0&&completed.back().status==0);
    // IRQ publication never synchronizes DMA, copies a buffer or calls completion.
    setup();BlockRequest irqreq;std::array<std::byte,512> irqbytes{};request_init(irqreq,irqbytes,0);assert(submit_issue(&device.bdev,irqreq)==0&&wakes==0);registers.ports[0].sact=0;registers.ports[0].is=8;registers.is=1;native_irq(48,nullptr);assert(wakes==1&&completed.empty()&&dma_completes==0&&port_memory[0].interrupt_status.load()==8);ahci_async_service(&device.bdev);assert(completed.size()==1);
    // Fatal IRQ between deferred exchange and hardware status read must never expose a successful read.
    setup();BlockRequest raced;std::array<std::byte,512> racedbytes{};racedbytes.fill(std::byte{0xcc});request_init(raced,racedbytes,0);assert(submit_issue(&device.bdev,raced)==0);std::memset(pools[0].data(),0xaa,512);registers.ports[0].sact=0;registers.ports[0].is=1U<<29;registers.is=1;irq_during_status_read=true;ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==-EIO&&completed[0].bytes==0&&racedbytes[0]==std::byte{0xcc});assert(guarded_irqs==1&&resets==1&&identifies==1&&reset_reopened);
    setup();BlockRequest hardware;std::array<std::byte,512> hardwarebytes{};hardwarebytes.fill(std::byte{0xcc});request_init(hardware,hardwarebytes,0);assert(submit_issue(&device.bdev,hardware)==0);std::memset(pools[0].data(),0xaa,512);fault_during_active_read=true;ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==-EIO&&completed[0].bytes==0&&hardwarebytes[0]==std::byte{0xcc});assert(resets==1&&identifies==1&&reset_reopened);
    // A captured old fault is retired with old DMA; it cannot poison the qualified reset epoch.
    setup();BlockRequest oldirq;std::array<std::byte,512> oldbytes{};request_init(oldirq,oldbytes,0);assert(submit_issue(&device.bdev,oldirq)==0);registers.ports[0].is=1U<<29;irq_during_stop=true;ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==-EIO&&resets==1&&identifies==1&&reset_reopened&&!port_memory[0].async.offline&&port_memory[0].interrupt_status.load()==0);
    // A new raw or IRQ-published fault after IDENTIFY DMA retirement prevents reset reopen.
    for(int delivery=1;delivery<=2;++delivery){setup();BlockRequest late;std::array<std::byte,512> latebytes{};latebytes.fill(std::byte{0xcc});request_init(late,latebytes,0);assert(submit_issue(&device.bdev,late)==0);registers.ports[0].is=1U<<29;final_status_fault=delivery;ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==-EIO&&completed[0].bytes==0&&latebytes[0]==std::byte{0xcc});assert(resets==1&&identifies==1&&!reset_reopened&&port_memory[0].async.offline&&port_memory[0].failed&&quarantines==0);assert(ahci_async_submit(&device.bdev,late)==-ENODEV);}
    // Fatal error is observed before cleared registers can falsely succeed; all buffers remain unchanged.
    setup();std::array<BlockRequest,8> failed;std::array<std::array<std::byte,512>,8> failedbytes;
    for(size_t i=0;i<8;++i){failedbytes[i].fill(std::byte{0xcc});request_init(failed[i],failedbytes[i],i);assert(ahci_async_submit(&device.bdev,failed[i])==0);}
    ahci_async_service(&device.bdev);assert(completed.empty()&&ci_doorbells==std::vector<uint32_t>{255});registers.ports[0].is=1U<<30;ahci_async_service(&device.bdev);assert(completed.size()==8&&dma_completes==9&&resets==1&&identifies==1&&reset_reopened&&port_memory[0].async.epoch==2&&device_quiesced==8&&never_submitted==0);
    for(size_t i=0;i<8;++i){assert(completed[i].status==-EIO&&completed[i].bytes==0);assert(failedbytes[i][0]==std::byte{0xcc});}
    BlockRequest reused;std::array<std::byte,512> reusebytes{};request_init(reused,reusebytes,20);assert(submit_issue(&device.bdev,reused)==0);assert(port_memory[0].async.slots[0].generation==2&&port_memory[0].async.slots[0].epoch==2);registers.ports[0].sact=0;ahci_async_service(&device.bdev);
    // A completed exclusive IDENTIFY must preserve geometry, capabilities and identity, with no lingering ERR/DF.
    for(int mismatch=1;mismatch<=4;++mismatch){setup();BlockRequest changed;std::array<std::byte,512> changedbytes{};changedbytes.fill(std::byte{0xcc});request_init(changed,changedbytes,0);assert(submit_issue(&device.bdev,changed)==0);identify_fault=mismatch;registers.ports[0].is=1U<<30;ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==-EIO&&completed[0].bytes==0&&changedbytes[0]==std::byte{0xcc});assert(resets==1&&identifies==1&&dma_completes==2&&!reset_reopened&&port_memory[0].async.offline&&port_memory[0].failed&&quarantines==0);for(auto const& state:dma_states)assert(!state.owned);assert(ahci_async_submit(&device.bdev,changed)==-ENODEV);ahci_async_service(&device.bdev);assert(completed.size()==1);}
    // Cancellation during DMA prepare never rings the doorbell and uses never_submitted proof.
    setup();BlockRequest preparing;std::array<std::byte,512> preparingbytes{};request_init(preparing,preparingbytes,0);cancel_in_prepare=true;assert(ahci_async_submit(&device.bdev,preparing)==-ECANCELED);assert(registers.ports[0].ci==0&&registers.ports[0].sact==0&&dma_completes==1&&completed.empty());ahci_async_service(&device.bdev);
    // Accepted prepared owners remain unissued on cancel/fatal/timeout/admission close.
    for(int cause=0;cause<4;++cause){
        setup();BlockRequest staged;std::array<std::byte,512> stagedbytes{};stagedbytes.fill(std::byte{0xcc});request_init(staged,stagedbytes,0);if(cause==2)staged.deadline_us=1;
        assert(ahci_async_submit(&device.bdev,staged)==0&&port_memory[0].async.slots[0].phase==async_rules::Phase::preparing&&port_memory[0].pending_issue_mask==1&&ci_doorbells.empty());
        int expected=-EIO;if(cause==0){assert(wakes==0);ahci_async_cancel(&device.bdev,staged.token,-ECANCELED);assert(wakes==1);expected=-ECANCELED;}else if(cause==1)registers.ports[0].is=1U<<29;else if(cause==2)expected=-ETIMEDOUT;else admission.close();
        ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==expected&&completed[0].bytes==0&&stagedbytes[0]==std::byte{0xcc});
        assert(ci_doorbells.empty()&&sact_doorbells.empty()&&never_submitted==1&&device_quiesced==0&&port_memory[0].pending_issue_mask==0&&async_rules::owned(port_memory[0].async)==0);
        assert(port_memory[0].stats.submitted==1&&port_memory[0].stats.completed==1&&identifies==(cause==3?0:1)&&reset_reopened==(cause!=3));
    }
    // Transient TFD backpressure retains preparing DMA until a later bounded service can issue.
    setup();BlockRequest busy;std::array<std::byte,512> busybytes{};request_init(busy,busybytes,0);assert(ahci_async_submit(&device.bdev,busy)==0);registers.ports[0].tfd=ATA_DEV_BUSY;ahci_async_service(&device.bdev);assert(completed.empty()&&ci_doorbells.empty()&&dma_states[0].owned&&port_memory[0].pending_issue_mask==1&&port_memory[0].async.slots[0].phase==async_rules::Phase::preparing);registers.ports[0].tfd=0;ahci_async_service(&device.bdev);assert(completed.empty()&&ci_doorbells==std::vector<uint32_t>{1}&&port_memory[0].pending_issue_mask==0);registers.ports[0].sact=0;ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==0);
    // A reset distinguishes issued DMA from retained but never published DMA.
    setup();std::array<BlockRequest,2> mixed;std::array<std::array<std::byte,512>,2> mixedbytes{};for(size_t i=0;i<2;++i){mixedbytes[i].fill(std::byte{0xcc});request_init(mixed[i],mixedbytes[i],i);}assert(submit_issue(&device.bdev,mixed[0])==0);assert(ahci_async_submit(&device.bdev,mixed[1])==0);registers.ports[0].is=1U<<29;ahci_async_service(&device.bdev);assert(completed.size()==2&&device_quiesced==1&&never_submitted==1&&identifies==1&&ci_doorbells==std::vector<uint32_t>{1}&&port_memory[0].pending_issue_mask==0&&reset_reopened);for(auto const& b:mixedbytes)assert(b[0]==std::byte{0xcc});
    setup();std::array<BlockRequest,2> unproven;std::array<std::array<std::byte,512>,2> unprovenbytes{};for(size_t i=0;i<2;++i)request_init(unproven[i],unprovenbytes[i],i);assert(submit_issue(&device.bdev,unproven[0])==0);assert(ahci_async_submit(&device.bdev,unproven[1])==0);port_memory[0].injected_fault=SelftestAsyncFault::stop;ahci_async_service(&device.bdev);assert(completed.size()==2&&never_submitted==1&&device_quiesced==0&&dma_completes==1&&quarantines==8&&port_memory[0].pending_issue_mask==0&&!reset_reopened);
    // Timeout and withheld stop both fail without exposing a partially read buffer.
    setup();BlockRequest timed;std::array<std::byte,512> timedbytes{};request_init(timed,timedbytes,0);assert(submit_issue(&device.bdev,timed)==0);clock_us=100001;ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==-ETIMEDOUT&&resets==1&&device_quiesced==1&&never_submitted==0);
    setup();BlockRequest pinned;std::array<std::byte,512> pinnedbytes{};pinnedbytes.fill(std::byte{0xcc});request_init(pinned,pinnedbytes,0);assert(submit_issue(&device.bdev,pinned)==0);port_memory[0].injected_fault=SelftestAsyncFault::stop;ahci_async_service(&device.bdev);assert(completed.size()==1&&completed[0].status==-EIO&&quarantines==8&&dma_completes==0&&resets==0&&pinnedbytes[0]==std::byte{0xcc});assert(ahci_async_submit(&device.bdev,pinned)==-ENODEV);ahci_async_service(&device.bdev);assert(completed.size()==1);
    // Observe actual SACT before a following MMIO exit completes the batch.
    setup();std::array<BlockRequest,8> sampled;std::array<std::array<std::byte,512>,8> sampledbytes{};
    for(size_t i=0;i<8;++i){request_init(sampled[i],sampledbytes[i],i);assert(ahci_async_submit(&device.bdev,sampled[i])==0);}
    finish_after_doorbell=true;ahci_async_service(&device.bdev);assert(registers.ports[0].sact==0&&completed.empty()&&dma_completes==0&&port_memory[0].stats.current_sact==255&&port_memory[0].stats.hardware_high_water==8);ahci_async_service(&device.bdev);assert(completed.size()==8&&dma_completes==8);
    // Static legacy providers retain EIO after shutdown, while native admission
    // retains ENODEV. No released DMA backing or polled path is accessed.
    setup();std::array<std::byte,512> legacybytes{};
    assert(ahci_read_blocks(&device.bdev,0,1,legacybytes.data())==0&&ahci_write_blocks(&device.bdev,0,1,legacybytes.data())==0&&ahci_flush_blocks(&device.bdev)==0);
    assert(legacy_sync_operations==std::vector<BlockRequestOperation>({BlockRequestOperation::READ,BlockRequestOperation::WRITE,BlockRequestOperation::FLUSH}));
    legacy_sync_status=-EINVAL;assert(ahci_read_blocks(&device.bdev,0,1,legacybytes.data())==-EINVAL);
    admission.close();port_memory[0].failed=true;for(size_t i=0;i<8;++i)slot_transfer(port_memory[0],i).cpu=nullptr;
    for(int result:{-ENODEV,-ESHUTDOWN}){legacy_sync_status=result;assert(ahci_read_blocks(&device.bdev,0,1,legacybytes.data())==-EIO&&ahci_write_blocks(&device.bdev,0,1,legacybytes.data())==-EIO&&ahci_flush_blocks(&device.bdev)==-EIO);}
    BlockRequest closed;request_init(closed,legacybytes,0);assert(ahci_async_submit(&device.bdev,closed)==-ENODEV&&dma_completes==0&&ci_doorbells.empty()&&sact_doorbells.empty());
    assert(mutex_depth==0&&spin_depth==0);return 0;
}
}}int main(){return ker::dev::ahci::tests();}
'''


def main():
    functions = [
        "auto native_memory(", "auto native_validate(", "void native_copy(",
        "void native_build(", "void native_observe_hardware(", "auto ahci_async_submit(", "void ahci_async_cancel(",
        "auto native_port_reset(", "auto native_revalidate(", "void ahci_async_service(", "void native_irq(",
    ]
    # Declaration prefixes occur before definitions; start the search at the actual native region.
    global SOURCE
    definitions = SOURCE[SOURCE.index("struct NativeCompletion") :]
    original = SOURCE
    has_poll = "auto ahci_async_poll_active(" in definitions
    if has_poll:
        functions.insert(-1, "auto ahci_async_poll_active(")
    native = []
    SOURCE = definitions
    for prefix in functions:
        native.append(extract(prefix))
    SOURCE = original
    text = ("#define WOS_AHCI_HAS_HEALTH_DUE 1\n" if "bool health_check_due" in SOURCE else "")
    if has_poll:
        text += "#define WOS_AHCI_HAS_ACTIVE_POLL 1\n"
    mode = os.environ.get("WOS_AHCI_TEST_MODE", "all")
    if mode == "poll-active":
        text += "#define WOS_AHCI_POLL_ONLY 1\n"
    elif mode == "no-irq-recovery":
        text += "#define WOS_AHCI_RECOVERY_ONLY 1\n"
    elif mode == "idle-health":
        text += "#define WOS_AHCI_IDLE_ONLY 1\n"
    elif mode == "flush-completion":
        text += "#define WOS_AHCI_FLUSH_ONLY 1\n"
    elif mode == "legacy-flush-two-pass":
        text += "#define WOS_AHCI_LEGACY_FLUSH_CONTROL 1\n"
    elif mode == "write-completion":
        text += "#define WOS_AHCI_WRITE_ONLY 1\n"
    elif mode == "legacy-write-two-pass":
        text += "#define WOS_AHCI_LEGACY_WRITE_CONTROL 1\n"
    elif mode == "native-controls":
        text += "#define WOS_AHCI_NATIVE_CONTROLS_ONLY 1\n"
    elif mode == "write-fresh-fault":
        text += "#define WOS_AHCI_WRITE_FAULT_CONTROL 1\n"
    elif mode == "write-fresh-active":
        text += "#define WOS_AHCI_WRITE_ACTIVE_CONTROL 1\n"
    elif mode == "read-completion":
        text += "#define WOS_AHCI_READ_ONLY 1\n"
    elif mode == "sole-active-to-inactive":
        text += "#define WOS_AHCI_SOLE_OBSERVATION_ONLY 1\n"
    elif mode == "legacy-read-two-pass":
        text += "#define WOS_AHCI_LEGACY_READ_CONTROL 1\n"
    elif mode == "legacy-sole-active-two-pass":
        text += "#define WOS_AHCI_LEGACY_OBSERVATION_CONTROL 1\n"
    elif mode == "read-fresh-fault":
        text += "#define WOS_AHCI_READ_FAULT_CONTROL 1\n"
    elif mode == "read-fresh-active":
        text += "#define WOS_AHCI_READ_ACTIVE_CONTROL 1\n"
    elif mode != "all":
        raise ValueError("unknown AHCI test mode: " + mode)
    text += PRELUDE + extract("struct CoherentRegion") + ";\n" + extract("struct PortMemory") + ";\n" + MIDDLE
    text += extract("auto slot_transfer(") + "\n" + DMA
    for prefix in ("auto valid_port(", "auto port_index(", "void clear_port_errors(", "void log_command_failure(", "auto recover_port_after_error(", "auto find_cmdslot("):
        text += extract(prefix) + "\n"
    text += extract("constexpr auto command_table_size(") + "\n"
    text += extract("auto command_table_prdt_entries(") + "\n" + extract("void reset_command_header(") + "\n"
    # Extract the definition, excluding its forward declaration before native code.
    SOURCE = original[original.index("// Caller holds this port mutex.") :]
    text += extract("auto execute_command_locked(") + "\n"
    SOURCE = original
    text += extract("struct NativeCompletion") + ";\n" + "\n".join(native)
    for prefix in ("auto native_legacy_sync(", "auto ahci_flush_blocks(", "auto ahci_read_blocks(", "auto ahci_write_blocks("):
        text += extract(prefix) + "\n"
    text += TESTS
    with tempfile.TemporaryDirectory(prefix="ahci-native-") as directory:
        temp = Path(directory)
        shim = temp / "platform/dbg"
        shim.mkdir(parents=True)
        (shim / "dbg.hpp").write_text("#pragma once\n")
        source = temp / "native.cpp"
        source.write_text(text)
        binary = temp / "native"
        command = [
            os.environ.get("HOST_CXX", "/usr/bin/clang++"), "--target=x86_64-linux-gnu", "--gcc-toolchain=/usr", "-std=c++23", "-O1", "-g",
            "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-DWOS_ASYNC_BLOCK_IO=1", "-DWOS_SELFTEST=1",
            "-I" + str(temp), "-I" + str(ROOT / "modules/kern/src"), str(source), "-o", str(binary),
        ]
        artifacts = os.environ.get("WOS_AHCI_ARTIFACT_DIR")
        if artifacts:
            artifact_path = Path(artifacts)
            artifact_path.mkdir(parents=True, exist_ok=True)
            (artifact_path / "native.cpp").write_text(text)
            (artifact_path / "compiler-argv.txt").write_text("\n".join(command) + "\n")
        subprocess.run(command, check=True)
        if artifacts:
            shutil.copyfile(binary, artifact_path / "native.bin")
        subprocess.run([str(binary)], check=True)
    if mode == "all":
        subprocess.run([sys.executable, str(ROOT / "tests/host/unit/ahci_controller_init_integration_test.py")], check=True)
    print("Actual-source AHCI native integration: PASS (ASan/UBSan)")


if __name__ == "__main__":
    main()
