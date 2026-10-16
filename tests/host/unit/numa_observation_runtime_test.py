#!/usr/bin/env python3
"""Compile the actual bounded NUMA handler with controlled Host primitives.

Actual shared records, policy validation, topology declarations, request handler
and range_valid body are compiled unchanged. Task policy storage and checked
usercopy/StableUserPage/immutable publication are explicit Host seams. They
record real handler operands, ownership, call ordering and refusal side effects;
this is not native Task locking, physical frame/TLB lifetime or CPL3 execution.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
TASK = r'''#pragma once
#include <platform/mm/numa_policy.hpp>
namespace ker::mod::sched::task {
struct Task {
    ker::mod::mm::numa::Policy policy{};
    Task* resource_owner{};
    mutable unsigned snapshots{};
    unsigned publications{};
    auto memory_policy_snapshot() const noexcept -> ker::mod::mm::numa::Policy;
    void publish_memory_policy_snapshot(ker::mod::mm::numa::Policy) noexcept;
};
}
'''
USERCOPY = r'''#pragma once
#include <cstddef>
#include <cstdint>
#include <platform/sched/task.hpp>
namespace ker::mod::sys::usercopy {
inline constexpr uint64_t USER_ADDR_LIMIT = 0x0000800000000000ULL;
class StableUserPage {
public:
    StableUserPage() = default;
    ~StableUserPage();
    StableUserPage(const StableUserPage&) = delete;
    StableUserPage(StableUserPage&&) = delete;
    auto operator=(const StableUserPage&) -> StableUserPage& = delete;
    auto operator=(StableUserPage&&) -> StableUserPage& = delete;
    auto valid() const -> bool;
    auto physical_address() const -> uint64_t;
    auto still_mapped() const -> bool;
    // Explicit fixture-only storage, not production layout equivalence.
    bool held{}, coherent{}, validity{true};
    uint64_t physical{};
};
auto range_valid(uint64_t, size_t) -> bool;
auto copy_from_task(ker::mod::sched::task::Task&, uint64_t, void*, size_t) -> bool;
auto copy_to_task(ker::mod::sched::task::Task&, uint64_t, const void*, size_t) -> bool;
auto pin_task_user_page(ker::mod::sched::task::Task&, uint64_t, bool, bool, StableUserPage&) -> bool;
}
'''
TEST = r'''
#include <abi/numa.h>
#include <syscalls_impl/vmem/numa.hpp>
#include <platform/acpi/numa_boot.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/usercopy.hpp>
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <vector>
namespace f {
namespace abi=ker::abi::vmem::numa;
namespace topo=ker::mod::acpi::numa;
namespace numa=ker::mod::mm::numa;
namespace uc=ker::mod::sys::usercopy;
using Task=ker::mod::sched::task::Task;
unsigned assertions{},cases{};
void check(bool v,const char* why){++assertions;if(!v){std::fprintf(stderr,"FAIL %s\n",why);std::abort();}}
constexpr uint64_t REQ=0x1007, OUT=0x8003, PAYLOAD=0x200000;
struct Page{uint64_t physical{};bool pinnable{true},mapped{true},valid{true};};
struct State {
 topo::Topology topology{};topo::BootStatus status{};
 bool topology_present{true},status_present{true},read_fail{},write_fail{},mutate_request{};
 unsigned reads{},writes{},active{},takes{},drops{},node_lookups{},mapped_checks{};
 Task actor{},owner{};
 std::array<unsigned char,0x10000> memory{};
 std::map<uint64_t,Page> pages;
 std::map<uint64_t,numa::NodeId> physical_nodes;
 std::vector<uint64_t> pin_addresses;
 State(){
  topology.node_count=64;topology.cpu_count=256;
  topology.provenance=topo::Provenance::SRAT_SLIT;
  for(unsigned i=0;i<64;++i){topology.nodes[i].proximity_id=0x80000000U+i;topology.nodes[i].usable_bytes=(i+1)*4096;}
  topology.nodes[63].cpus.words={1,2,4,1ULL<<63};
  actor.resource_owner=&owner;owner.policy={1,numa::Mode::BIND};memory.fill(0xa5);
 }
 template<class T>void store(uint64_t address,const T& value){check(address+sizeof(T)<=memory.size(),"fixture store bounds");std::memcpy(memory.data()+address,&value,sizeof(value));}
 template<class T>auto load(uint64_t address=OUT)->T{T value{};check(address+sizeof(T)<=memory.size(),"fixture load bounds");std::memcpy(&value,memory.data()+address,sizeof(value));return value;}
 auto request(abi::QueryKind kind=abi::QueryKind::SUMMARY)->abi::QueryRequest{
  abi::QueryRequest r{};r.version=1;r.bytes=sizeof(r);r.kind=static_cast<uint32_t>(kind);r.output_address=OUT;
  r.output_bytes=kind==abi::QueryKind::SELF_POLICY?sizeof(abi::SelfPolicy):sizeof(abi::Summary);
  if(kind==abi::QueryKind::PAGE_NODES){r.address=PAYLOAD;r.count=1;r.output_bytes=sizeof(abi::PageNode);}
  return r;
 }
 auto query(abi::QueryRequest r,uint64_t bytes=sizeof(abi::QueryRequest),uint64_t a3=0,uint64_t a4=0)->int64_t{
  store(REQ,r);return static_cast<int64_t>(ker::syscall::vmem::query_numa_for_task(actor,REQ,bytes,a3,a4));
 }
 auto set(abi::PolicyRequest r,uint64_t bytes=sizeof(abi::PolicyRequest),uint64_t a3=0,uint64_t a4=0)->int64_t{
  store(REQ,r);return static_cast<int64_t>(ker::syscall::vmem::set_memory_policy_for_task(actor,REQ,bytes,a3,a4));
 }
 void page(uint64_t address,uint64_t physical,numa::NodeId node,bool mapped=true){pages[address]={physical,true,mapped,true};physical_nodes[physical]=node;}
 void released(){check(active==0&&takes==drops,"all payload leases returned");check(reads<=1,"request copied once");check(writes<=1,"at most one output copy");}
};
State* state{};
struct Scope{State value{};Scope(){state=&value;++cases;}~Scope(){value.released();state=nullptr;}auto operator->()->State*{return &value;}};
auto policy(uint32_t mode=0,uint64_t nodes=0)->abi::PolicyRequest{return {1,sizeof(abi::PolicyRequest),mode,0,nodes,0};}
void no_effect(State& s){check(s.writes==0&&s.takes==0&&s.actor.publications==0,"refusal no side effect");}
void verify_policy_publication(abi::PolicyRequest requested, uint16_t node_count, bool mutate = false) {
 Scope s;
 s->topology.node_count = node_count;
 s->mutate_request = mutate;
 auto const owner_before = s->owner.policy;
 check(s->set(requested) == 0, "validated explicit policy accepted");
 numa::Policy const expected{requested.nodes, static_cast<numa::Mode>(requested.mode)};
 check(s->actor.policy == expected && s->actor.publications == 1 && s->actor.snapshots == 0,
       "exact actor policy published once without recapture");
 check(s->owner.policy == owner_before && s->owner.publications == 0 && s->owner.snapshots == 0,
       "policy publication leaves resource owner unchanged");
 check(s->reads == 1 && s->writes == 0 && s->takes == 0 && s->drops == 0,
       "setter copied one input and has no postcommit copy or payload lease");
 // Each invocation has its own one-copy bound; query uses a fresh request.
 s->reads = 0;
 check(s->query(s->request(abi::QueryKind::SELF_POLICY)) == 0, "published policy SELF_POLICY readback");
 auto const value = s->load<abi::SelfPolicy>();
 check(value.version == abi::VERSION && value.bytes == sizeof(value) && value.mode == requested.mode &&
       value.nodes == requested.nodes && value.reserved32 == 0 && value.reserved64 == 0,
       "published mode and complete mask read back exactly");
 check(s->actor.publications == 1 && s->owner.publications == 0 && s->owner.snapshots == 0,
       "readback cannot republish or inspect resource owner");
}
}
namespace ker::mod::sched::task {
auto Task::memory_policy_snapshot() const noexcept -> f::numa::Policy {
 f::check(this==&f::state->actor,"snapshot actual executor not resource owner");
 f::check(f::state->active==0,"policy snapshot outside payload lease");++snapshots;return policy;
}
void Task::publish_memory_policy_snapshot(f::numa::Policy p) noexcept {
 f::check(this==&f::state->actor,"publish actual executor not owner");
 f::check(f::state->active==0,"policy commit outside payload lease");++publications;policy=p;
}
}
namespace ker::mod::acpi::numa {
auto boot_topology() noexcept -> const Topology* {return f::state->topology_present?&f::state->topology:nullptr;}
auto boot_status() noexcept -> const BootStatus* {return f::state->status_present?&f::state->status:nullptr;}
}
namespace ker::mod::mm::numa {
auto physical_node(uint64_t p) noexcept -> NodeId {
 auto& s=*f::state;f::check(s.active==1,"physical node read with exact live frame");++s.node_lookups;
 auto i=s.physical_nodes.find(p);return i==s.physical_nodes.end()?INVALID_NODE:i->second;
}
}
namespace ker::mod::sys::usercopy {
// ACTUAL_RANGE_VALID
StableUserPage::~StableUserPage(){if(held){--f::state->active;++f::state->drops;}}
auto StableUserPage::valid() const -> bool{return held&&validity;}
auto StableUserPage::physical_address() const -> uint64_t{f::check(valid(),"physical scalar only with valid lease");return physical;}
auto StableUserPage::still_mapped() const -> bool{f::check(held,"mapping revalidation while lease held");++f::state->mapped_checks;return coherent;}
auto pin_task_user_page(f::Task& actor,uint64_t va,bool write,bool fault,StableUserPage& out)->bool{
 auto& s=*f::state;f::check(&actor==&s.actor,"pin actual executor");f::check(!write&&!fault,"resident lookup false,false");
 f::check(s.active==0&&!out.held,"previous row pin released before next");s.pin_addresses.push_back(va);
 auto i=s.pages.find(va);if(i==s.pages.end()||!i->second.pinnable)return false;
 ++s.active;++s.takes;out.held=true;out.physical=i->second.physical;out.coherent=i->second.mapped;out.validity=i->second.valid;return true;
}
auto copy_from_task(f::Task& actor,uint64_t address,void* out,size_t bytes)->bool{
 auto& s=*f::state;f::check(&actor==&s.actor&&s.active==0,"request copy exact actor before leases");++s.reads;
 if(s.read_fail||!range_valid(address,bytes)||address>s.memory.size()||bytes>s.memory.size()-address)return false;
 std::memcpy(out,s.memory.data()+address,bytes);
 if(s.mutate_request)std::fill_n(s.memory.data()+address,bytes,0xff);
 return true;
}
auto copy_to_task(f::Task& actor,uint64_t address,const void* in,size_t bytes)->bool{
 auto& s=*f::state;f::check(&actor==&s.actor&&s.active==0,"output copy exact actor after every lease");++s.writes;
 if(!range_valid(address,bytes)||address>s.memory.size()||bytes>s.memory.size()-address)return false;
 std::memcpy(s.memory.data()+address,in,s.write_fail?std::min(bytes,size_t{4}):bytes);return !s.write_fail;
}
}
int main(){
 using namespace f;
 // Exact immutable summaries distinguish rejected firmware from absent SRAT.
 for(unsigned provenance=0;provenance<4;++provenance){Scope s;s->topology.provenance=static_cast<topo::Provenance>(provenance==3?0:provenance);s->status.rejected_firmware=provenance==3;
  check(s->query(s->request())==0,"summary accepts all four provenance states");auto v=s->load<abi::Summary>();
  check(v.version==1&&v.bytes==64&&v.features==3&&v.provenance==provenance&&v.node_count==64&&v.cpu_count==256,"exact summary fields");
  check(v.page_bytes==4096&&v.max_page_nodes==64&&v.cpu_mask_words==4,"exact bounded features");for(auto r:v.reserved64)check(r==0,"summary reserved initialized");}
 {Scope s;auto q=s->request(abi::QueryKind::NODE);q.index=63;check(s->query(q)==0,"node63 valid");auto v=s->load<abi::Node>();
  check(v.node==63&&v.proximity_id==0x8000003fU&&v.managed_bytes==64*4096&&v.reserved16==0&&v.reserved64==0,"full node63");
  for(unsigned i=0;i<4;++i)check(v.cpu_words[i]==s->topology.nodes[63].cpus.words[i],"all CPU words including bit255");}
 {Scope s;auto q=s->request(abi::QueryKind::NODE);q.index=5;s->topology.nodes[5].usable_bytes=0;check(s->query(q)==0&&s->load<abi::Node>().managed_bytes==0,"CPU-only zero-RAM node exposed");}
 {Scope s;auto q=s->request(abi::QueryKind::NODE);q.index=4;s->topology.nodes[4].cpus.words={};check(s->query(q)==0,"memory-only node exposed");auto v=s->load<abi::Node>();for(auto w:v.cpu_words)check(w==0,"memory-only CPU masks empty");}
 for(uint64_t bytes:std::array<uint64_t,4>{0,63,65,UINT64_MAX}){Scope s;check(s->query(s->request(),bytes)==-EINVAL,"outer query exact size");check(s->reads==0,"outer size rejected before copy");no_effect(s.value);}
 for(unsigned arg:{3U,4U}){Scope s;check(s->query(s->request(),64,arg==3, arg==4)==-EINVAL&&s->reads==0,"outer query reserved args before copy");}
 for(unsigned field=0;field<12;++field){Scope s;auto q=s->request();int error=EINVAL;
  switch(field){case 0:q.bytes=63;break;case 1:q.version=2;error=ENOTSUP;break;case 2:q.flags=1;error=ENOTSUP;break;
  case 3:q.kind=4;break;case 4:q.kind=0x100;break;case 5:q.reserved32_a=1;break;case 6:q.reserved32_b=1;break;case 7:q.reserved64=1;break;
  case 8:q.index=1;break;case 9:q.address=4096;break;case 10:q.count=1;break;case 11:q.output_bytes=63;break;}
  check(s->query(q)==-error,"query malformed fixed fields rejected");no_effect(s.value);}
 for(unsigned field=0;field<3;++field){Scope s;auto q=s->request(abi::QueryKind::SELF_POLICY);if(field==0)q.index=1;if(field==1)q.address=4096;if(field==2)q.count=1;
  check(s->query(q)==-EINVAL,"self policy unused fields rejected");no_effect(s.value);}
 for(unsigned field=0;field<4;++field){Scope s;auto q=s->request(abi::QueryKind::NODE);if(field==0)q.index=64;if(field==1)q.index=UINT64_MAX;if(field==2)q.address=4096;if(field==3)q.count=1;
  check(s->query(q)==-EINVAL,"node invalid index or unused fields");no_effect(s.value);}
 for(unsigned field=0;field<9;++field){Scope s;auto q=s->request(abi::QueryKind::PAGE_NODES);
  switch(field){case 0:q.index=1;break;case 1:q.count=0;break;case 2:q.count=65;break;case 3:q.count=UINT32_MAX;break;case 4:q.address=PAYLOAD+1;break;
  case 5:q.address=uc::USER_ADDR_LIMIT;break;case 6:q.address=uc::USER_ADDR_LIMIT-4096;q.count=2;q.output_bytes=16;break;case 7:q.address=UINT64_MAX-4095;break;case 8:q.output_bytes=9;break;}
  check(s->query(q)==-EINVAL,"page request bounds/count/unused geometry");no_effect(s.value);}
 for(auto address:{uc::USER_ADDR_LIMIT-63,UINT64_MAX,uc::USER_ADDR_LIMIT}){Scope s;auto q=s->request();q.output_address=address;check(s->query(q)==-EFAULT,"output overflow rejected before effects");no_effect(s.value);}
 {Scope s;s->read_fail=true;check(s->query(s->request())==-EFAULT,"request copy failure");no_effect(s.value);}
 {Scope s;s->write_fail=true;check(s->query(s->request())==-EFAULT&&s->writes==1,"ordinary partial output failure explicit");check(s->memory[OUT]==1&&s->memory[OUT+4]==0xa5,"partial output no atomicity claim");}
 {Scope s;s->topology_present=false;check(s->query(s->request())==-ENODEV,"missing topology");no_effect(s.value);}
 {Scope s;s->status_present=false;check(s->query(s->request())==-ENODEV,"missing permanent status");no_effect(s.value);}
 {Scope s;s->topology.provenance=static_cast<topo::Provenance>(255);check(s->query(s->request())==-ENODEV,"unknown internal provenance fails closed");no_effect(s.value);}
 for(unsigned shape=0;shape<4;++shape){Scope s;if(shape==0)s->topology.node_count=0;if(shape==1)s->topology.node_count=65;if(shape==2)s->topology.cpu_count=0;if(shape==3)s->topology.cpu_count=257;
  check(s->query(s->request())==-ENODEV,"malformed permanent topology refusal");no_effect(s.value);}
 {Scope s;s->actor.policy={1ULL<<63,numa::Mode::BIND};check(s->query(s->request(abi::QueryKind::SELF_POLICY))==0,"actual executor self policy");auto v=s->load<abi::SelfPolicy>();check(v.mode==2&&v.nodes==(1ULL<<63)&&v.reserved32==0&&v.reserved64==0,"internal nonLOCAL truthfully observed");check(s->owner.snapshots==0,"owner not queried");}
 {Scope s;s->actor.policy.mode=static_cast<numa::Mode>(255);check(s->query(s->request(abi::QueryKind::SELF_POLICY))==-EINVAL&&s->writes==0,"invalid internal policy not narrowed");}
 {Scope s;s->mutate_request=true;check(s->query(s->request())==0&&s->reads==1,"request copied once despite later user mutation");}
 {Scope s;auto q=s->request();q.output_address=REQ;check(s->query(q)==0&&s->load<abi::Summary>(REQ).version==1,"overlapping control transport copied once");}
 {Scope s;auto q=s->request(abi::QueryKind::PAGE_NODES);q.address=uc::USER_ADDR_LIMIT-4096;s->page(q.address,0xabc000,63);
  check(s->query(q)==0,"final complete user page accepted");auto v=s->load<abi::PageNode>();check(v.node==63&&v.status==0&&v.reserved32==0,"last page resident");}
 {Scope s;auto q=s->request(abi::QueryKind::PAGE_NODES);q.count=64;q.output_bytes=512;
  for(unsigned i=0;i<64;++i)s->page(PAYLOAD+i*4096,0x10000000ULL+i*4096,i);
  check(s->query(q)==0&&s->pin_addresses.size()==64&&s->takes==64&&s->drops==64,"64 separate exact row leases");
  auto rows=s->load<std::array<abi::PageNode,64>>();for(unsigned i=0;i<64;++i)check(rows[i].node==i&&rows[i].status==0&&rows[i].reserved32==0,"dense row node no paddr disclosure");}
 for(unsigned kind=0;kind<6;++kind){Scope s;auto q=s->request(abi::QueryKind::PAGE_NODES);
  if(kind!=0)s->page(PAYLOAD,0x400000,kind==3?numa::INVALID_NODE:(kind==4?64:2),kind!=2);
  if(kind==1)s->pages[PAYLOAD].pinnable=false; // Unsupported huge/missing/teardown primitive refusal.
  if(kind==5)s->pages[PAYLOAD].valid=false;
  check(s->query(q)==0,"unavailable row is successful query");auto v=s->load<abi::PageNode>();check(v.node==65535&&v.status==1&&v.reserved32==0,"unknown/stale/invalid pin stays unavailable");}
 {Scope s;auto q=s->request(abi::QueryKind::PAGE_NODES);s->page(PAYLOAD,0x400000,1);s->write_fail=true;check(s->query(q)==-EFAULT&&s->takes==s->drops,"output failure after pin release");}
 for(uint64_t bytes:std::array<uint64_t,4>{0,31,33,UINT64_MAX}){Scope s;check(s->set(policy(),bytes)==-EINVAL&&s->reads==0,"outer setter exact bytes before copy");no_effect(s.value);}
 for(unsigned arg:{3U,4U}){Scope s;check(s->set(policy(),32,arg==3,arg==4)==-EINVAL&&s->reads==0,"outer setter extra args before copy");}
 for(unsigned kind=0;kind<10;++kind){Scope s;auto p=policy();int error=EINVAL;
  switch(kind){case 0:p.bytes=31;break;case 1:p.version=2;error=ENOTSUP;break;case 2:p.flags=1;error=ENOTSUP;break;case 3:p.reserved64=1;break;
  case 4:p.mode=4;break;case 5:p.mode=0x100;break;case 6:p.nodes=1;break;case 7:p=policy(1,3);break;case 8:p=policy(2,0);break;case 9:p=policy(3,0);break;}
  auto before=s->actor.policy;check(s->set(p)==-error&&s->actor.policy==before,"setter malformed request no publication");no_effect(s.value);}
 // Valid nonLOCAL refusals are deliberately superseded by public publication.
 for (unsigned mode = 1; mode < 4; ++mode) {
  verify_policy_publication(policy(mode, 1), 64);
  verify_policy_publication(policy(mode, 1ULL << 63), 64);
  verify_policy_publication(policy(mode, 1), 1);
  if (mode != 1) verify_policy_publication(policy(mode, 1 | (1ULL << 63)), 64);
  verify_policy_publication(policy(mode, 1), 64, true);
 }
 for (unsigned mode = 1; mode < 4; ++mode) {
  Scope s;
  s->topology.node_count = 1;
  check(s->set(policy(mode, 2)) == -EINVAL, "UMA outside-node mask refused unchanged");
  no_effect(s.value);
 }
 {Scope s;s->topology.node_count=63;check(s->set(policy(2,1ULL<<63))==-EINVAL,"offline mask not truncated");no_effect(s.value);}
 {Scope s;s->topology_present=false;check(s->set(policy())==-ENODEV,"setter missing topology");no_effect(s.value);}
 {Scope s;s->read_fail=true;check(s->set(policy())==-EFAULT,"setter bad request memory");no_effect(s.value);}
 {Scope s;s->actor.policy={1ULL<<63,numa::Mode::BIND};check(s->set(policy())==0&&s->actor.policy==numa::Policy{}&&s->actor.publications==1,"LOCAL actor publication exactly once");check(s->owner.policy.mode==numa::Mode::BIND&&s->owner.publications==0&&s->writes==0,"owner unchanged no postcommit output");}
 std::printf("PASS NUMA actual handler: %u cases %u assertions\n",cases,assertions);return 0;
}
'''


def actual_range_valid(source: str) -> str:
    start = source.index("auto range_valid(uint64_t user_addr, size_t size) -> bool {")
    end = source.index("\n}\n", start) + 3
    return source[start:end]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--kernel-includes", type=Path)
    parser.add_argument("--handler", type=Path)
    parser.add_argument("--cxx", default=os.environ.get("WOS_HOST_CXX", "/usr/bin/g++"))
    args = parser.parse_args()
    root = args.root.resolve()
    kern = root / "modules/kern/src"
    includes = (args.kernel_includes or kern).resolve()
    source = args.handler or kern / "syscalls_impl/vmem/numa.cpp"
    # CXX may name the freestanding cross compiler without Host headers.
    compiler = args.cxx
    if not shutil.which(compiler):
        raise SystemExit(f"C++ compiler unavailable: {compiler}")
    range_body = actual_range_valid((includes / "platform/sys/usercopy.cpp").read_text())
    with tempfile.TemporaryDirectory(prefix="wos-numa-observation-") as name:
        work = Path(name)
        for relative, contents in [("platform/sched/task.hpp", TASK), ("platform/sys/usercopy.hpp", USERCOPY)]:
            path = work / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(contents)
        test = work / "test.cpp"
        test.write_text(TEST.replace("// ACTUAL_RANGE_VALID", range_body))
        binary = work / "test"
        command = [compiler, "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined", "-fno-omit-frame-pointer",
                   "-DWOS_HOST_TEST=1", "-I", str(work), "-I", str(kern), "-I", str(includes),
                   "-I", str(includes.parents[1]), str(source), str(test), "-o", str(binary)]
        subprocess.run(command, check=True, timeout=90, cwd=work)
        env = dict(os.environ, ASAN_OPTIONS="detect_leaks=0:halt_on_error=1", UBSAN_OPTIONS="halt_on_error=1")
        subprocess.run([str(binary)], check=True, timeout=20, env=env, cwd=work)


if __name__ == "__main__":
    main()
