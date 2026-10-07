#!/usr/bin/env python3
"""Compose the actual workload and complete ZONE TU with explicit host seams.

Transport/core crypto and scheduling are modeled; real zone allocation,
exact references, public transfers, correlation, teardown and the workload
body execute under ASan/UBSan. This does not qualify paired runtime transport.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
KERN = ROOT / 'modules/kern/src'
SOURCE = KERN / 'net/wki/secure_zone_workload.cpp'
HEADER = KERN / 'net/wki/secure_zone_workload.hpp'
ZONE_FIXTURE = Path(__file__).with_name('wki_secure_zone_test.py')


def replace_function(source, name, replacement):
    start = source.index('auto ' + name + '(')
    brace = source.index('{', start)
    depth, end = 1, brace + 1
    while depth:
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    return source[:start] + replacement + source[end:]


AFTER = r'''
namespace ker::net::wki {
auto wki_now_us()->uint64_t {CHECK(!fixture::guards&&!lifecycle);++fixture::clock_reads;return fixture::now;}
auto wki_send_on_channel_identity(const WkiChannelIdentity&i,MsgType type,const void*data,uint16_t size)->int {
 CHECK(!fixture::guards);auto*p=static_cast<const uint8_t*>(data);fixture::sent.emplace_back(type,std::vector<uint8_t>(p,p+size));
 if(type==MsgType::ZONE_CREATE_REQ&&fixture::create_failure)return -EHOSTUNREACH;
 if(type==MsgType::ZONE_WRITE_REQ){++fixture::message_writes;auto const&r=*static_cast<const ZoneWriteReqPayload*>(data);CHECK(r.offset+r.length<=fixture::remote_data.size());std::memcpy(fixture::remote_data.data()+r.offset,p+sizeof(r),r.length);ZoneWriteAckPayload a{r.zone_id,r.offset,r.length,0,r.op_cookie};WkiHeader h{};h.src_node=2;h.channel_id=i.channel_id;RX_CALL(detail::handle_zone_write_ack(&h,reinterpret_cast<uint8_t*>(&a),sizeof(a),i));}
 if(type==MsgType::ZONE_READ_REQ){++fixture::message_reads;auto const&r=*static_cast<const ZoneReadReqPayload*>(data);CHECK(r.offset+r.length<=fixture::remote_data.size());std::vector<uint8_t>b(sizeof(ZoneReadRespPayload)+r.length);ZoneReadRespPayload a{r.zone_id,r.offset,r.length,r.op_cookie};std::memcpy(b.data(),&a,sizeof(a));std::memcpy(b.data()+sizeof(a),fixture::remote_data.data()+r.offset,r.length);WkiHeader h{};h.src_node=2;h.channel_id=i.channel_id;RX_CALL(detail::handle_zone_read_resp(&h,b.data(),b.size(),i));}
 return 0;
}
}
using namespace ker::net::wki;
namespace {
unsigned cases=0;
auto identity()->WkiChannelIdentity{return {&channel,2,WKI_CHAN_ZONE_MGMT,channel.generation};}
void reset(bool available=true){CHECK(fixture::pool.empty()&&!fixture::guards&&!lifecycle);fixture::sent.clear();fixture::remote_data.fill(0);fixture::available=available;fixture::grant_live=true;fixture::capture_pressure=false;fixture::grant_hook={};fixture::unregister_busy=false;fixture::registration_fail=false;fixture::registration_hook={};fixture::onwait={};fixture::write_hook={};fixture::read_hook={};fixture::sleep_hook={};fixture::after_create={};fixture::now=100;fixture::sleep_advance=1000;fixture::sleep_calls=fixture::clock_reads=0;fixture::create_failure=false;fixture::write_status=fixture::read_status=0;fixture::corrupt=false;fixture::secure_reads=fixture::secure_writes=fixture::message_reads=fixture::message_writes=0;channel.active=true;channel.generation=7;fixture::created_id=0;
 fixture::onwait=[] {if(fixture::created_id)return;CHECK(!fixture::sent.empty()&&fixture::sent.back().first==MsgType::ZONE_CREATE_REQ);ZoneCreateReqPayload request{};std::memcpy(&request,fixture::sent.back().second.data(),sizeof(request));fixture::created_id=request.zone_id;WkiHeader h{};h.src_node=2;h.channel_id=WKI_CHAN_ZONE_MGMT;ZoneCreateAckPayload ack{};ack.zone_id=request.zone_id;ack.rkey=fixture::available?33:0;process_zone_create_ack(h,ack,identity());auto*z=acquire_zone_slot(request.zone_id);if(z){fixture::remote=z->secure_descriptor;fixture::remote.rkey=33;release_zone(z);}if(fixture::after_create)fixture::after_create();};
}
void finish(){wki_zones_destroy_for_peer(2);fixture::unregister_busy=false;process_zone_retirement_work();CHECK(fixture::pool.empty()&&!fixture::guards&&!lifecycle);++cases;}
auto successor()->WkiZone*{auto*old=acquire_zone_slot(fixture::created_id);CHECK(old);s_zone_table_lock.lock();mark_zone_retiring_locked(old);s_zone_table_lock.unlock();release_zone(old);ZoneCreateReqPayload request{};request.zone_id=fixture::created_id;request.size=4096;request.access_policy=15;WkiHeader h{};h.src_node=2;h.channel_id=WKI_CHAN_ZONE_MGMT;process_zone_create_request(h,request,identity());auto*fresh=acquire_zone_slot(request.zone_id);CHECK(fresh);return fresh;}
void bounds(){for(unsigned v=0;v<5;++v){reset();WkiSecureZoneWorkloadResult r;r.verified=true;r.destroyed=true;auto rc=wki_secure_zone_workload_run(v==0?0:v==1?WKI_NODE_BROADCAST:2,v==2?0:121,v==3?0:v==4?4097:4,7,r);CHECK(rc==-EINVAL&&r.status==-EINVAL&&r.create_status==-ENODATA&&!r.created&&!r.verified&&!r.destroyed&&fixture::sent.empty());finish();}}
void success(){for(bool enabled:{true,false})for(uint32_t bytes:{1,1024,1025,4096}){reset(enabled);WkiSecureZoneWorkloadResult r;CHECK(wki_secure_zone_workload_run(2,121,bytes,0xFEDCBA9876543210ULL,r)==0);CHECK(r.status==0&&r.create_status==0&&r.prepare_status==-ENODATA&&r.write_status==0&&r.read_status==0&&r.destroy_status==0&&r.created&&r.verified&&r.destroyed&&r.type==ZoneTypeHint::BUFFER&&r.written_bytes==bytes&&r.read_bytes==bytes&&r.mismatch_offset==UINT32_MAX);CHECK(find_zone_slot(121)==nullptr);CHECK(fixture::clock_reads==0&&fixture::sleep_calls==0);CHECK(fixture::secure_writes==(enabled?1U:0U)&&fixture::secure_reads==(enabled?1U:0U));CHECK(fixture::message_writes==(enabled?0U:(bytes+1023)/1024)&&fixture::message_reads==(enabled?0U:(bytes+1023)/1024));CHECK(std::all_of(fixture::sent.begin(),fixture::sent.end(),[](const auto&entry){if(entry.first!=MsgType::ZONE_CREATE_REQ)return true;ZoneCreateReqPayload q{};std::memcpy(&q,entry.second.data(),sizeof(q));return q.size==4096&&q.access_policy==15&&q.zone_type_hint==static_cast<uint8_t>(ZoneTypeHint::BUFFER);}));finish();}}
void failures(){for(unsigned v=0;v<4;++v){reset();fixture::create_failure=v==0;fixture::write_status=v==1?-EPIPE:0;fixture::read_status=v==2?-ESTALE:0;fixture::corrupt=v==3;WkiSecureZoneWorkloadResult r;auto rc=wki_secure_zone_workload_run(2,121,4,5,r);CHECK(rc==(v==0?-EHOSTUNREACH:v==1?-EPIPE:v==2?-ESTALE:-EIO)&&r.status==rc&&!r.verified);CHECK(r.created==(v!=0)&&r.destroyed==(v!=0));if(v==0)CHECK(r.write_status==-ENODATA&&r.read_status==-ENODATA&&r.destroy_status==-ENODATA);if(v==1)CHECK(r.read_status==-ENODATA&&r.written_bytes==0);if(v==2)CHECK(r.written_bytes==4&&r.read_bytes==0);if(v==3)CHECK(r.read_status==0&&r.read_bytes==4&&r.mismatch_offset==2);finish();}}
void existing(){reset();ZoneCreateReqPayload q{};q.zone_id=121;q.size=4096;q.access_policy=15;WkiHeader h{};h.src_node=2;h.channel_id=WKI_CHAN_ZONE_MGMT;process_zone_create_request(h,q,identity());auto*old=acquire_zone_slot(121);CHECK(old);auto before=fixture::sent.size();WkiSecureZoneWorkloadResult r;CHECK(wki_secure_zone_workload_run(2,121,4,5,r)==WKI_ERR_ZONE_EXISTS&&!r.created&&!r.destroyed&&r.destroy_status==-ENODATA&&fixture::sent.size()==before&&find_zone_slot(121)==old);release_zone(old);finish();}
void replaced(){for(bool during_read:{false,true}){reset();WkiZone*fresh=nullptr;if(during_read)fixture::read_hook=[&]{fresh=successor();};else{fixture::write_status=-EIO;fixture::write_hook=[&]{fresh=successor();};}WkiSecureZoneWorkloadResult r;auto rc=wki_secure_zone_workload_run(2,121,4,5,r);CHECK(fresh&&find_zone_slot(121)==fresh&&!fresh->retiring&&fresh->state==ZoneState::ACTIVE);CHECK(r.destroy_status==WKI_ERR_ZONE_NOT_FOUND&&!r.destroyed&&rc==(during_read?WKI_ERR_ZONE_NOT_FOUND:-EIO));CHECK(r.verified==during_read);release_zone(fresh);finish();}}
void ready_success(){for(unsigned v=0;v<3;++v){reset();if(v==1)fixture::grant_live=false;if(v==2)fixture::after_create=[]{fixture::capture_pressure=true;};fixture::sleep_hook=[v]{if(fixture::sleep_calls==3){fixture::grant_live=true;fixture::capture_pressure=false;}};WkiSecureZoneWorkloadResult r;CHECK(wki_secure_zone_workload_run(2,121,4096,13,r,true)==0&&r.prepare_status==0&&r.verified&&r.destroyed&&fixture::secure_writes==1&&fixture::secure_reads==1&&fixture::message_writes==0&&fixture::message_reads==0);CHECK(fixture::sleep_calls==(v==0?0U:3U));finish();}}
void readiness_failures(){for(unsigned v=0;v<9;++v){reset();fixture::grant_live=false;if(v==1)fixture::sleep_hook=[]{if(fixture::sleep_calls==50)fixture::grant_live=true;};if(v==2)fixture::sleep_advance=2000;if(v==3)fixture::sleep_advance=0;if(v==4)fixture::sleep_hook=[]{fixture::now=0;};if(v==5)fixture::now=UINT64_MAX-500;if(v==6){fixture::grant_live=true;fixture::after_create=[]{fixture::capture_pressure=true;};}if(v==7){fixture::grant_live=true;fixture::after_create=[]{--fixture::remote.size;};}if(v==8){fixture::grant_live=true;fixture::after_create=[]{fixture::remote.permissions=rdma_protocol::PERMIT_READ;};}WkiSecureZoneWorkloadResult r;CHECK(wki_secure_zone_workload_run(2,121,4,9,r,true)==-ETIMEDOUT&&r.status==-ETIMEDOUT&&r.prepare_status==-ETIMEDOUT&&r.created&&r.destroyed&&!r.verified&&r.write_status==-ENODATA&&r.read_status==-ENODATA);CHECK(fixture::secure_writes==0&&fixture::secure_reads==0&&fixture::message_writes==0&&fixture::message_reads==0);CHECK(fixture::sleep_calls==(v==2?25U:v==4||v==5?1U:50U));finish();}}
void stale_readiness(){reset();WkiZone*fresh=nullptr;fixture::after_create=[&]{fixture::grant_hook=[&]{fresh=successor();fresh->remote_rkey=33;fixture::remote=fresh->secure_descriptor;fixture::remote.rkey=33;};};WkiSecureZoneWorkloadResult r;CHECK(wki_secure_zone_workload_run(2,121,4,9,r,true)==WKI_ERR_ZONE_INACTIVE&&r.prepare_status==WKI_ERR_ZONE_INACTIVE&&r.write_status==-ENODATA&&r.read_status==-ENODATA&&!r.destroyed&&r.destroy_status==WKI_ERR_ZONE_NOT_FOUND);CHECK(fresh&&find_zone_slot(121)==fresh&&!fresh->retiring&&fixture::secure_writes==0&&fixture::message_writes==0&&fixture::sleep_calls==0);fixture::grant_hook={};release_zone(fresh);finish();}
}
int main(){wki_zone_init();bounds();success();failures();existing();replaced();ready_success();readiness_failures();stale_readiness();CHECK(fixture::allocations==fixture::frees&&fixture::raw_allocations==0);std::printf("secure zone workload actual-source %u cases PASS; allocations=%u frees=%u\n",cases,fixture::allocations,fixture::frees);}
'''


def make_tu(source, zone_source):
    spec = importlib.util.spec_from_file_location('zone_fixture', ZONE_FIXTURE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    tu = module.make_tu(zone_source)
    assert tu.endswith(module.AFTER)
    tu = tu[:-len(module.AFTER)]
    tu = tu.replace('bool available=true,grant_live=true,', 'std::array<uint8_t,4096>remote_data{};int write_status=0,read_status=0;bool corrupt=false,create_failure=false;uint32_t created_id=0;uint64_t now=100,sleep_advance=1000;unsigned sleep_calls=0,clock_reads=0;std::function<void()>write_hook,read_hook,sleep_hook,after_create;\nbool available=true,grant_live=true,')
    tu = tu.replace('namespace ker::mod::sched {', 'namespace ker::net::wki { extern bool lifecycle; }\nnamespace ker::mod::sched {')
    tu = tu.replace('void kern_sleep_us(uint64_t interval){CHECK(!fixture::guards && interval==1000);}', 'void kern_sleep_us(uint64_t interval){CHECK(!fixture::guards&&!ker::net::wki::lifecycle&&interval>0&&interval<=1000);++fixture::sleep_calls;uint64_t step=fixture::sleep_advance==1000?interval:fixture::sleep_advance;fixture::now=fixture::now>UINT64_MAX-step?UINT64_MAX:fixture::now+step;if(fixture::sleep_hook)fixture::sleep_hook();}')
    tu = replace_function(tu, 'wki_secure_rdma_write', r'''auto wki_secure_rdma_write(const WkiChannelIdentity&,const rdma_protocol::RegionDescriptor&d,uint64_t offset,std::span<const uint8_t>in,uint16_t,std::span<const uint8_t>)->WkiSecureRdmaResult {
 CHECK(!fixture::guards&&!lifecycle&&std::memcmp(&d,&fixture::remote,sizeof(d))==0&&offset+in.size()<=fixture::remote_data.size());++fixture::secure_writes;if(fixture::write_hook)fixture::write_hook();if(fixture::write_status)return {fixture::write_status,0,WkiSecureRdmaOutcome::UNCERTAIN};std::copy(in.begin(),in.end(),fixture::remote_data.begin()+offset);return {0,static_cast<uint32_t>(in.size()),WkiSecureRdmaOutcome::EXACT};
}''')
    tu = replace_function(tu, 'wki_secure_rdma_read', r'''auto wki_secure_rdma_read(const WkiChannelIdentity&,const rdma_protocol::RegionDescriptor&d,uint64_t offset,std::span<uint8_t>out,std::span<const uint8_t>)->WkiSecureRdmaResult {
 CHECK(!fixture::guards&&!lifecycle&&std::memcmp(&d,&fixture::remote,sizeof(d))==0&&offset+out.size()<=fixture::remote_data.size());++fixture::secure_reads;if(fixture::read_status)return {fixture::read_status,0,WkiSecureRdmaOutcome::UNCERTAIN};std::copy_n(fixture::remote_data.begin()+offset,out.size(),out.begin());if(fixture::corrupt&&out.size()>2)out[2]^=1;if(fixture::read_hook)fixture::read_hook();return {0,static_cast<uint32_t>(out.size()),WkiSecureRdmaOutcome::EXACT};
}''')
    header = re.sub(r'^#(?:include|pragma)[^\n]*\n', '', HEADER.read_text(), flags=re.M)
    cpp = re.sub(r'^#include[^\n]*\n', '', source, flags=re.M)
    for forbidden in ['.memory(', 'snapshot_memory(', 'wki_zone_find(', 'rdma_transport', 'local_vaddr']:
        assert forbidden not in source, forbidden
    clock = 'namespace ker::net::wki { auto wki_now_us()->uint64_t; }\n'
    return tu + '\n' + clock + header + '\n' + cpp + '\n' + AFTER


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, default=SOURCE)
    parser.add_argument('--zone-source', type=Path, default=KERN / 'net/wki/zone.cpp')
    parser.add_argument('--keep', type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='wki-zone-workload-') as temp:
        out = args.keep or Path(temp)
        out.mkdir(parents=True, exist_ok=True)
        tu = out / 'workload-tu.cpp'
        tu.write_text(make_tu(args.source.read_text(), args.zone_source.read_text()))
        command = ['/usr/bin/clang++', '-std=c++23', '-g', '-O1', '-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-fno-pie', '-no-pie', '-pthread', '-I' + str(KERN), str(tu), '-o', str(out / 'workload-test')]
        compiled = subprocess.run(command, capture_output=True, text=True)
        (out / 'compile.log').write_text(compiled.stdout + compiled.stderr)
        run = subprocess.run([str(out / 'workload-test')], capture_output=True, text=True, env=os.environ | {'ASAN_OPTIONS': 'detect_leaks=0', 'UBSAN_OPTIONS': 'halt_on_error=1'}) if compiled.returncode == 0 else None
        if run:
            (out / 'run.log').write_text(run.stdout + run.stderr)
        pins = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in [args.source, args.zone_source, HEADER, KERN / 'net/wki/zone.hpp', ZONE_FIXTURE, Path(__file__)]}
        (out / 'result.json').write_text(json.dumps({'compile_exit': compiled.returncode, 'run_exit': run.returncode if run else None, 'pins': pins, 'command': command}, indent=2) + '\n')
        print(compiled.stdout + compiled.stderr if compiled.returncode else run.stdout + run.stderr)
        return compiled.returncode or run.returncode


if __name__ == '__main__':
    raise SystemExit(main())
