"""Run actual RX handlers and timing/snapshot bodies with independent clock/lock inputs."""
import os
import importlib.util
import sys
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE = Path(os.environ.get("WOS_ROUTING_RX_SOURCE_ROOT", ROOT))
routing = (SOURCE / "modules/kern/src/net/wki/routing.cpp").read_text()
header = (SOURCE / "modules/kern/src/net/wki/routing.hpp").read_text()
wire = (ROOT / "modules/kern/src/net/wki/wire.hpp").read_text()
core = ((SOURCE if (SOURCE / "modules/kern/src/net/wki/wki.cpp").exists() else ROOT) / "modules/kern/src/net/wki/wki.cpp").read_text()
proc = ((SOURCE if (SOURCE / "modules/kern/src/vfs/fs/procfs.cpp").exists() else ROOT) / "modules/kern/src/vfs/fs/procfs.cpp").read_text()


def block(source, pattern, declaration=False):
    match = re.search(pattern, source, re.M)
    assert match, pattern
    pos = source.index("{", match.start()) + 1
    depth = 1
    while depth:
        depth += (source[pos] == "{") - (source[pos] == "}")
        pos += 1
    if declaration:
        pos = source.index(";", pos) + 1
    return source[match.start():pos]


def function(name):
    return block(routing, rf"^(?:auto|void) {name}\(")


def definition(source, name):
    return block(source, rf"^(?:struct|class|enum class) {name}\b", True)


code = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <utility>
#include <cstdio>
#include <numeric>
#include <ranges>
#include <span>
#include <abi/kernel_debug_metadata.hpp>
#include <platform/sched/event_wake_diagnostics.hpp>
#include <net/wki/routing_model.hpp>
#include <net/wki/routing_lease.hpp>
uint64_t clock_us = 1000, step_us = 1, current_cpu = 2, wake_delay = 500;
unsigned clock_reads = 0, cpu_reads = 0, wakes = 0, lock_depth = 0;
uint64_t trace_copy_delay = 0; unsigned trace_captures = 0;
namespace ker::loader::runtime {
bool kernel_debug_metadata(wos::abi::KernelDebugMetadata&){return false;}
}
namespace ker::mod::cpu {
uint64_t current_cpu_fast() { ++cpu_reads; return current_cpu; }
}
namespace ker::mod::sched {
// Actual trace publication/storage is qualified by scheduler_timer_trace_test.
// Keep the existing RX clock/lock seams and original timing assertions intact.
void set_scheduler_timer_trace_enabled(bool){}
void capture_scheduler_timer_trace(uint64_t cpu,SchedulerTimerTraceWindow const& window,SchedulerTimerTraceSnapshot& out){
 assert(lock_depth==1);++trace_captures;clock_us+=trace_copy_delay;
 out={};out.enabled=window.enabled;out.start_sequence=window.start_sequence;out.end_sequence=window.end_sequence;
 if(window.enabled){assert(cpu==current_cpu);out.count=1;out.events[0].sequence=window.end_sequence;}
}
}
namespace ker::net::wki {
constexpr uint16_t WKI_NODE_INVALID = 0;
constexpr size_t WKI_MAX_PEERS = 256, WKI_MAX_NEIGHBORS_PER_LSA = 32, WKI_AUTH_NONCE_SIZE = 32;
constexpr uint64_t DISCOVERY_REFRESH_INTERVAL_US = 30'000'000;
uint64_t wki_now_us() { ++clock_reads; clock_us += step_us; return clock_us; }
'''
code += re.search(r"^constexpr uint16_t WKI_CAP_ROUTING_LEASE = [^;]+;", wire, re.M)[0] + "\n"
for name in ("MsgType", "WkiHeader"):
    code += definition(wire, name) + "\n"
code += wire[wire.index("struct LsaNeighborEntry {"):wire.index("// FENCE_NOTIFY Payload")].rsplit("// -----------------------------------------------------------------------------", 1)[0]
code += definition(header, "LsdbEntry") + "\n"
code += re.search(r"^constexpr size_t WKI_ROUTING_RX_STAGES = [^;]+;", header, re.M)[0] + "\n"
for name in ("WkiRoutingRxDiag", "WkiRoutingDiag"):
    code += definition(header, name) + "\n"
code += r'''
WkiRoutingDiag s_diag{};
struct Lock {
 bool held=false; unsigned calls=0; uint64_t delay=0, unlock_delay=2;
 uint64_t inject_max=0;
 void lock(){assert(!held);held=true;++calls;++lock_depth;clock_us+=delay;
  if(inject_max){s_diag.max_rx_us=inject_max;s_diag.max_rx_sample.total_us=inject_max;
   s_diag.max_rx_sample.origin=222;inject_max=0;}}
 void unlock(){assert(held);held=false;--lock_depth;clock_us+=unlock_delay;}
};
Lock s_routing_lock;
std::atomic<bool> s_rx_timing_enabled{false}, s_initialized{false}, s_own_pending{false};
std::array<LsdbEntry,WKI_MAX_PEERS> s_lsdb{};
uint64_t s_revision=1, s_invalidation_revision=0;
struct Global{uint16_t my_node_id=1,capabilities=0;}g_wki;
struct Peer{
 struct Auth{bool active=true; uint32_t generation=7;Lock replay_lock;}auth_session;
 uint16_t capabilities=WKI_CAP_ROUTING_LEASE;
 uint32_t remote_boot_epoch=12;
 std::array<uint8_t,WKI_AUTH_NONCE_SIZE> remote_auth_nonce{};
} peer;
bool peer_present=true;
Peer* wki_peer_find(uint16_t node){return peer_present&&node==2?&peer:nullptr;}
void notify_routing_worker(mod::sched::EventWakeDiagnostics* diagnostics=nullptr){
 assert(lock_depth==0);++wakes;
 if(diagnostics){diagnostics->timer_trace={.start_sequence=10,.end_sequence=11,.enabled=true};
  diagnostics->recorded=true;diagnostics->started_us=clock_us;
  diagnostics->cpu_start=current_cpu;diagnostics->target_us=3;diagnostics->prepare_us=4;
  diagnostics->admission_us=5;diagnostics->reschedule_us=wake_delay-12;
  diagnostics->rq_wait_us=21;diagnostics->rq_body_us=22;diagnostics->rq_return_us=23;
  diagnostics->poke_us=24;diagnostics->locks=2;diagnostics->passes=1;diagnostics->pokes=1;
  diagnostics->leader=true;diagnostics->counters_valid=true;
  diagnostics->irq_start_us=17;diagnostics->irq_end_us=29;
  diagnostics->timer_start=31;diagnostics->timer_end=32;}
 clock_us+=wake_delay;
 if(diagnostics){diagnostics->total_us=wake_delay;diagnostics->cpu_end=current_cpu;}
}
'''
code += definition(routing, "OriginPeer") + "\n"
for name in ("lsdb_find", "lsdb_alloc", "leases_enabled", "rx_origin", "confirmed_rx_origin"):
    code += function(name) + "\n"
code += definition(routing, "RoutingRxStage") + "\n"
code += definition(routing, "RoutingRxTiming") + "\n"
for name in ("note_rx_time", "handle_lsa", "handle_lsa_ack", "wki_routing_diag_snapshot", "wki_routing_init"):
    code += function(name) + "\n"
code += block(core, r"^auto cmdline_has_token\(") + "\n"
for name in ("append_dec64", "append_sconst", "append_char", "append_bool01", "append_routing_timer_kernel_metadata"):
    code += block(proc, rf"^void {name}\(") + "\n"
row = proc[proc.index('    append_sconst(p, end, "wki_routing_rx version=1 enabled=");'):proc.index('    auto* origins = new (std::nothrow) ker::net::wki::WkiRoutingOriginDiag')]
code += "std::string format_rx(WkiRoutingDiag const& ROUTING, size_t limit) { char buf[2048]{}; char* p=buf; char const* end=buf+limit;" + row + " return std::string(buf, p); }\n"
init_call = re.search(r'    wki_routing_init\(cmdline_has_token\(.*\)\);', core)[0].replace("ker::init::get_kernel_cmdline()", "command")
code += "void initialize_from_command(char const* command) {" + init_call + "}\n"
code += r'''
unsigned cases=0;
void reset(bool enabled){
 assert(lock_depth==0);clock_us=1000;step_us=1;current_cpu=2;clock_reads=cpu_reads=wakes=0;
 trace_copy_delay=0;trace_captures=0;
 wake_delay=500; s_diag={};s_diag.rx_timing_enabled=enabled;s_rx_timing_enabled.store(enabled);
 s_lsdb={};s_routing_lock={};s_routing_lock.delay=23;s_routing_lock.unlock_delay=2;
 peer={};peer.remote_auth_nonce[0]=1;peer.auth_session.replay_lock.delay=11;
 peer_present=true;g_wki={};s_revision=1;s_invalidation_revision=0;
}
WkiHeader frame(){WkiHeader h{};h.src_node=2;h.seq_num=91;return h;}
struct Payload{LsaPayload lsa{55,9,1,0};LsaNeighborEntry n{2,1,1500};};
static_assert(sizeof(Payload)==sizeof(LsaPayload)+sizeof(LsaNeighborEntry));
void check(bool enabled,unsigned expected_reads,uint8_t outcome,uint8_t mask,bool notify){
 assert(lock_depth==0&&!s_routing_lock.held&&wakes==unsigned(notify));
 assert(clock_reads==expected_reads);
 auto const copy=wki_routing_diag_snapshot();
 assert(copy.max_rx_us==s_diag.max_rx_us&&copy.rx_timing_enabled==enabled);
 if(enabled){auto const& rx=copy.max_rx_sample;assert(rx.total_us==copy.max_rx_us);
  assert(rx.outcome==outcome&&rx.stage_mask==mask&&rx.notified==notify);
  assert(std::accumulate(rx.stages_us.begin(),rx.stages_us.end(),uint64_t{0})==rx.total_us);
  assert(rx.cpu_start==2&&rx.cpu_end==2&&cpu_reads==2);
  if(notify){assert(rx.stages_us[5]>=wake_delay);assert(rx.stages_us[4]<50);
   assert(rx.wake.recorded&&rx.wake.total_us==wake_delay&&rx.wake.rq_wait_us==21);}
  else assert(!rx.wake.recorded&&rx.wake.total_us==0);
 }else{assert(cpu_reads==0&&copy.max_rx_sample.total_us==0&&copy.max_rx_sample.stage_mask==0);}
 ++cases;
}
void authenticated(){auto& e=s_lsdb[0];e.origin_node=2;e.authenticated_generation=7;
 e.authenticated_capabilities=WKI_CAP_ROUTING_LEASE;e.authenticated_identity={12,peer.remote_auth_nonce};}
void handlers(){
 for(bool enabled:{false,true}){
  auto h=frame();Payload p{};
  reset(enabled);handle_lsa(nullptr,nullptr,0);assert(s_diag.malformed==1);
  check(enabled,enabled?6:2,1,61,false);
  reset(enabled);p.lsa.num_neighbors=33;handle_lsa(&h,reinterpret_cast<uint8_t*>(&p),sizeof(p));
  assert(s_diag.malformed==1);check(enabled,enabled?6:2,1,61,false);p={};
  reset(enabled);p.n.node_id=p.lsa.origin_node;handle_lsa(&h,reinterpret_cast<uint8_t*>(&p),sizeof(p));
  assert(s_diag.malformed==1);check(enabled,enabled?6:2,1,61,false);p={};
  reset(enabled);p.lsa.origin_node=1;handle_lsa(&h,reinterpret_cast<uint8_t*>(&p),sizeof(p));
  assert(s_diag.max_rx_us==0&&s_diag.accepted==0&&wakes==0&&clock_reads==(enabled?2U:1U));++cases;p={};
  reset(enabled);handle_lsa(&h,reinterpret_cast<uint8_t*>(&p),sizeof(p));
  assert(s_diag.accepted==1&&s_lsdb[0].origin_node==55&&s_lsdb[0].num_neighbors==1);
  assert(s_lsdb[0].lsa_seq==9&&s_lsdb[0].neighbors[0].node_id==2&&s_lsdb[0].valid);
  check(enabled,enabled?7:2,3,63,true);
  if(enabled){assert((s_diag.max_rx_sample.stages_us==std::array<uint64_t,6>{1,39,1,24,3,501}));
   assert(s_diag.max_rx_sample.source==2&&s_diag.max_rx_sample.origin==55);
   assert(s_diag.max_rx_sample.sequence==9&&s_diag.max_rx_sample.payload_len==sizeof(p));}
  reset(enabled);auto& e=s_lsdb[0];e.origin_node=55;e.sequence_known=true;e.lsa_seq=9;
  e.hint_valid=true;e.hint_sequence=9;e.hint_revision_time_us=999;e.valid=true;
  handle_lsa(&h,reinterpret_cast<uint8_t*>(&p),sizeof(p));assert(s_diag.stale==1&&s_diag.accepted==0);
  check(enabled,enabled?7:2,3,63,false);
  reset(enabled);for(size_t i=0;i<s_lsdb.size();++i)s_lsdb[i].origin_node=static_cast<uint16_t>(100+i);
  handle_lsa(&h,reinterpret_cast<uint8_t*>(&p),sizeof(p));assert(s_diag.full==1&&s_diag.accepted==0);
  check(enabled,enabled?7:2,3,63,false);
  reset(enabled);peer_present=false;handle_lsa(&h,reinterpret_cast<uint8_t*>(&p),sizeof(p));
  assert(s_diag.accepted==1);check(enabled,enabled?7:2,3,63,true);
  reset(enabled);g_wki.capabilities=WKI_CAP_ROUTING_LEASE;authenticated();
  struct Extended{LsaPayload lsa{55,9,0,0};LsaLeaseSuffix suffix{};}__attribute__((packed)) ext;
  ext.suffix.boot_epoch=12;ext.suffix.origin_nonce=peer.remote_auth_nonce;ext.suffix.token=99;
  handle_lsa(&h,reinterpret_cast<uint8_t*>(&ext),sizeof(ext));assert(s_diag.lease_rejected==1&&s_diag.accepted==0);
  check(enabled,enabled?7:2,2,63,false);
  reset(enabled);g_wki.capabilities=WKI_CAP_ROUTING_LEASE;authenticated();
  LsaLeaseRequest q{};q.token=41;handle_lsa_ack(&h,reinterpret_cast<uint8_t*>(&q),sizeof(q));
  assert(s_lsdb[0].lease_response_token==41&&s_lsdb[0].lease_response_generation==7);
  check(enabled,enabled?7:2,5,63,true);
  reset(enabled);g_wki.capabilities=WKI_CAP_ROUTING_LEASE;authenticated();q.token=0;
  handle_lsa_ack(&h,reinterpret_cast<uint8_t*>(&q),sizeof(q));assert(s_diag.lease_rejected==1);
  check(enabled,enabled?7:2,4,63,false);
  reset(enabled);q.token=41;handle_lsa_ack(&h,reinterpret_cast<uint8_t*>(&q),sizeof(q));
  assert(clock_reads==1&&s_diag.max_rx_us==0&&wakes==0);++cases;
  reset(enabled);g_wki.capabilities=WKI_CAP_ROUTING_LEASE;handle_lsa_ack(&h,nullptr,0);
  assert(clock_reads==1&&s_diag.max_rx_us==0&&wakes==0);++cases;
 }
}
void publication(){
 auto h=frame();
 reset(true);step_us=0;clock_us=1000;s_routing_lock.delay=1000;
 RoutingRxTiming a{1000,MsgType::LSA,&h,12};a.identify(55,11);
 clock_us=1321;note_rx_time(1000,a,3);
 assert(s_diag.max_rx_us==321&&s_diag.max_rx_sample.total_us==321&&clock_us==2323);
 assert(s_diag.max_rx_sample.stages_us[5]==321&&s_diag.max_rx_sample.stage_mask==32);++cases;
 RoutingRxTiming b{clock_us,MsgType::LSA,&h,12};b.identify(66,22);
 clock_us+=321;note_rx_time(2323,b,3);
 assert(s_diag.max_rx_us==321&&s_diag.max_rx_sample.origin==55&&s_diag.max_rx_sample.sequence==11);++cases;
 auto const start=clock_us;RoutingRxTiming d{start,MsgType::LSA_ACK,&h,16};d.identify(77,33);
 clock_us+=22;note_rx_time(start,d,5);assert(s_diag.max_rx_sample.origin==55);++cases;
 auto const larger_start=clock_us;RoutingRxTiming e{larger_start,MsgType::LSA_ACK,&h,16};e.identify(88,44);
 clock_us+=322;current_cpu=7;note_rx_time(larger_start,e,5);
 assert(s_diag.max_rx_us==322&&s_diag.max_rx_sample.origin==88&&s_diag.max_rx_sample.sequence==44);
 assert(s_diag.max_rx_sample.cpu_start==2&&s_diag.max_rx_sample.cpu_end==7);++cases;
 current_cpu=2;auto const race_start=clock_us;RoutingRxTiming f{race_start,MsgType::LSA,&h,12};f.identify(99,55);
 clock_us+=323;s_routing_lock.inject_max=999;note_rx_time(race_start,f,3);
 assert(s_diag.max_rx_us==999&&s_diag.max_rx_sample.origin==222&&s_diag.max_rx_sample.total_us==999);++cases;
 reset(true);step_us=0;clock_us=UINT64_MAX-100;s_routing_lock.delay=s_routing_lock.unlock_delay=0;
 RoutingRxTiming g{clock_us,MsgType::LSA,&h,12};g.identify(55,UINT32_MAX);
 clock_us=UINT64_MAX-5;g.mark(RoutingRxStage::VALIDATION);note_rx_time(UINT64_MAX-100,g,3);
 assert(s_diag.max_rx_us==95&&s_diag.max_rx_sample.stages_us[0]==95&&s_diag.max_rx_sample.stages_us[5]==0);++cases;
 reset(false);step_us=0;RoutingRxTiming off{clock_us,MsgType::LSA,nullptr,0};
 for(unsigned i=0;i<6;++i)off.mark(static_cast<RoutingRxStage>(i));off.identify(55,1);off.notified();
 assert(clock_reads==0&&cpu_reads==0);clock_us+=321;note_rx_time(1000,off,3);
 assert(clock_reads==1&&cpu_reads==0&&s_diag.max_rx_us==321&&s_diag.max_rx_sample.total_us==0);++cases;
}
void trace_endpoint(){
 reset(true);trace_copy_delay=1000;Payload p{};auto h=frame();
 handle_lsa(&h,reinterpret_cast<uint8_t*>(&p),sizeof(p));
 assert(s_diag.max_rx_us==569&&s_diag.max_rx_sample.total_us==569);
 assert(s_diag.max_rx_sample.wake.total_us==500&&s_diag.max_rx_timer_trace.complete());
 assert(s_diag.max_rx_timer_trace.count==1&&trace_captures==1&&clock_us>2500);++cases;
}
void format_and_init(){
 char const* commands[]{nullptr,"","wki.routing_rx_timing"," x\twki.routing_rx_timing  y ",
 "wki.routing_rx_timingx","wki.routing_rx_timing=1","xwki.routing_rx_timing","wki.routing_rx_timing\tx"};
 bool const expected[]{false,false,true,true,false,false,false,true};
 for(size_t i=0;i<std::size(commands);++i){
  reset(false);s_initialized.store(false);s_own_pending.store(false);initialize_from_command(commands[i]);
  assert(s_initialized.load()&&s_own_pending.load());
  assert(s_rx_timing_enabled.load()==expected[i]&&s_diag.rx_timing_enabled==expected[i]);
  wki_routing_init(!expected[i]);assert(s_rx_timing_enabled.load()==expected[i]);++cases;
 }
 reset(false);auto const off=format_rx(s_diag,2048);
 assert(off=="wki_routing_rx version=1 enabled=0\nwki_routing_wake version=1 enabled=0\n");
 std::printf("%s",off.c_str());++cases;
 reset(true);Payload p{};auto h=frame();handle_lsa(&h,reinterpret_cast<uint8_t*>(&p),sizeof(p));
 auto const enabled=format_rx(wki_routing_diag_snapshot(),2048);
 assert(enabled.find("total_us=569")!=std::string::npos&&enabled.find("source=2 origin=55 sequence=9")!=std::string::npos);
 assert(enabled.find("stage_mask=63 notified=1 validation_us=1 origin_us=39 prepare_us=1 lock_wait_us=24 apply_us=3 tail_us=501\n")!=std::string::npos);
 assert(enabled.find("wki_routing_wake version=1 enabled=1 rx_started_us=1001 recorded=1")!=std::string::npos);
 assert(enabled.find("total_us=500 target_us=3 prepare_us=4 admission_us=5 reschedule_us=488 rq_wait_us=21 rq_body_us=22 rq_return_us=23 poke_us=24")!=std::string::npos);
 assert(enabled.find("kernel_meta_valid=0 kernel_load_base=0 kernel_text_start=0 kernel_text_end=0 kernel_build_id=-\n")!=std::string::npos);
 std::printf("%s",enabled.c_str());++cases;
 for(size_t n:{size_t{0},size_t{1},size_t{7},enabled.size()-1}){auto const small=format_rx(s_diag,n);
  // Existing append helpers reserve the last byte and emit decimals atomically.
  assert(small.size()<n||n==0);if(n<=7)assert(small==enabled.substr(0,n==0?0:n-1));++cases;}
 assert(clock_reads==7&&cpu_reads==2);++cases;
}
} // namespace ker::net::wki
int main(){ker::net::wki::handlers();ker::net::wki::publication();ker::net::wki::format_and_init();ker::net::wki::trace_endpoint();
 std::printf("%u actual RX/timer cases PASS (ASan/UBSan): disabled clocks, early returns, stages, policy/counters, wake tail, atomic maximum/ties, final lock exclusion\n",ker::net::wki::cases);}
'''
compiler = os.environ.get("WOS_HOST_CXX") or shutil.which("clang++-23") or "/usr/bin/clang++"
with tempfile.TemporaryDirectory(prefix="wos-routing-rx-") as tmp:
    path = Path(tmp)
    output = Path(os.environ.get("WOS_ROUTING_RX_ARTIFACT_DIR", path))
    output.mkdir(parents=True, exist_ok=True)
    src = output / "actual-routing-rx.cpp"
    exe = output / "actual-routing-rx"
    src.write_text(code)
    command = [compiler, "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined",
               "-fno-sanitize-recover=all", "-I", str(ROOT / "modules/kern/src"), str(src), "-o", str(exe)]
    build = subprocess.run(command, capture_output=True, text=True)
    (output / "build.log").write_text(build.stdout + build.stderr)
    assert build.returncode == 0, build.stdout + build.stderr
    run = subprocess.run([str(exe)], capture_output=True, text=True)
    (output / "run.log").write_text(run.stdout + run.stderr)
    print(run.stdout, end="")
    assert run.returncode == 0, run.stdout + run.stderr
    spec = importlib.util.spec_from_file_location("wki_rx_fixture_chaos", ROOT / "scripts/test/wki_chaos.py")
    consumer = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = consumer
    spec.loader.exec_module(consumer)
    rows = [line for line in run.stdout.splitlines() if line.startswith("wki_routing_rx ")]
    assert len(rows) == 2
    parsed = consumer.parse_snapshot_rows("netdiag", "\n".join(rows) + "\nwki_netdiag_end complete=1\n")
    assert parsed[0]["values"] == {"version": "1", "enabled": "0"}
    values = parsed[1]["values"]
    assert values["total_us"] == "569" and values["stage_mask"] == "63"
    assert sum(int(values[key]) for key in ("validation_us", "origin_us", "prepare_us", "lock_wait_us", "apply_us", "tail_us")) == 569
    wake_rows = [line for line in run.stdout.splitlines() if line.startswith("wki_routing_wake ")]
    assert len(wake_rows) == 2
    wakes_parsed = consumer.parse_snapshot_rows("netdiag", "\n".join(wake_rows) + "\nwki_netdiag_end complete=1\n")
    assert wakes_parsed[0]["values"] == {"version": "1", "enabled": "0"}
    wake_values = wakes_parsed[1]["values"]
    assert wake_values["rx_started_us"] == values["started_us"]
    assert wake_values["total_us"] == "500" and wake_values["rq_return_us"] == "23"
    trace_rows = [line for line in run.stdout.splitlines() if line.startswith("wki_routing_timer_trace ")]
    assert len(trace_rows) == 1
    trace_parsed = consumer.parse_snapshot_rows("netdiag", "\n".join(trace_rows) + "\nwki_netdiag_end complete=1\n")
    assert trace_parsed[0]["values"]["kernel_meta_valid"] == "0"
    assert trace_parsed[0]["values"]["kernel_build_id"] == "-"
    print("Actual boot-token/init, bounded procfs formatting and existing named-row consumer PASS")
