#!/usr/bin/env python3
"""Compile real HELLO retry publication/progress/drain/registry/timer boundaries.

Only peer/session objects, locks, clock and transport endpoints are controlled.
This is scheduling/lifetime evidence, not cryptography or kernel emulation.
WOS_HELLO_RETRY_OLD_PEER can select a saved old real timer for the negative run.
WOS_HELLO_RETRY_OLD_CORE can select the old discovery branch for its negative run.
WOS_HELLO_RETRY_OLD_PROGRESS can select the old routed-session progress check.
"""
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def function(source, name):
    match = re.search(rf"^(?:void|auto) {name}\([^)]*\)(?: -> [^{{\n]+)? \{{", source, re.M)
    assert match, f"missing {name}"
    end, depth = source.index("{", match.start()) + 1, 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[match.start():end] + "\n"


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <functional>
constexpr size_t WKI_MAX_PEERS = 16, WKI_AUTH_NONCE_SIZE = 32;
constexpr uint16_t WKI_NODE_INVALID = 0, WKI_NODE_BROADCAST = 65535;
constexpr uint32_t WKI_HELLO_MAGIC = 0x574b4900;
constexpr uint8_t WKI_VERSION = 3, WKI_PEER_FENCE_PROBE_ROUNDS = 3;
unsigned lock_calls=0;
struct Lock {
    bool held=false;
    std::function<void()> unlocked;
    void lock() { assert(!held); held=true; ++lock_calls; }
    void unlock() { assert(held); held=false; if(unlocked) { auto f=std::move(unlocked); unlocked={}; f(); } }
    uint64_t lock_irqsave() { lock(); return 17; }
    void unlock_irqrestore(uint64_t flags) { assert(flags==17); unlock(); }
};
namespace ker::mod::sys { using Spinlock=::Lock; }
namespace ker::mod::dbg { template<typename... Args> void log(const char*,Args...) {} }
struct TestLogger { template<typename... Args> static void warn(const char*,Args...) {} };
unsigned shutdown_holds=0;
namespace mod::sched {
struct ShutdownHold { ShutdownHold(){++shutdown_holds;} ~ShutdownHold(){--shutdown_holds;} };
auto hold_kernel_shutdown() { return ShutdownHold{}; }
}
enum class PeerState { UNKNOWN, HELLO_SENT, CONNECTED, FENCED };
struct WkiPeer {
    uint16_t node_id=0;
    bool is_direct=true, writer=false;
    unsigned readers=0;
    PeerState state=PeerState::UNKNOWN;
    uint32_t remote_boot_epoch=0;
    std::array<uint8_t,32> remote_auth_nonce{};
    struct { bool active=false; uint64_t rx_counter_high=0; Lock replay_lock; } auth_session;
    uint64_t fence_defer_until_us=0, last_heartbeat=0, last_rx_activity=UINT64_MAX;
    uint8_t missed_beats=0;
};
struct WkiRxMetadata { bool has_src_mac=false; unsigned src_mac=0; };
struct WkiHeader { uint16_t src_node=0; };
enum class WkiAuthFrameResult { DISCOVERY_HINT, REJECTED };
unsigned neighbor_learns=0;
void wki_eth_neighbor_add(uint16_t,unsigned) { ++neighbor_learns; }
struct WkiTransport;
using WkiRxHandler=void(*)(WkiTransport*,const void*,uint16_t,const WkiRxMetadata*);
struct WkiTransport {
    uint64_t registration_generation=0;
    std::atomic<bool> chaos_unregistering{false};
    std::atomic<uint32_t> chaos_in_flight{0};
    WkiTransport* next=nullptr;
    const char* name="fixture";
    unsigned mtu=9000; bool rdma_capable=false;
    void(*set_rx_handler)(WkiTransport*,WkiRxHandler)=nullptr;
};
struct { uint16_t my_node_id=1; bool initialized=true; Lock peer_lock,transport_lock;
         WkiTransport* transports=nullptr; unsigned transport_count=0;
         std::array<WkiPeer,WKI_MAX_PEERS> peers; } g_wki;
uint64_t now=100;
unsigned sends=0, notifications=0, broadcasts=0;
bool configured=true, initial_send=false;
std::function<void()> on_send;
WkiPeer* wki_peer_find(uint16_t n) { for(auto& p:g_wki.peers) if(p.node_id==n)return &p;return nullptr; }
bool wki_peer_lifecycle_rx_try_acquire(WkiPeer* p) { assert(g_wki.peer_lock.held); if(p->writer)return false; ++p->readers;return true; }
void wki_peer_lifecycle_rx_release(WkiPeer* p) { if(p){assert(p->readers);--p->readers;} }
struct HelloPayload {
    uint32_t magic=WKI_HELLO_MAGIC; uint8_t protocol_version=WKI_VERSION; uint16_t node_id=4;
    uint32_t boot=7; std::array<uint8_t,32> auth_nonce{};
};
uint32_t hello_boot_epoch(const HelloPayload* h) { return h->boot; }
bool wki_auth_peer_configured(uint16_t) { return configured; }
uint64_t wki_now_us() { return now; }
uint64_t wki_future_deadline_us(uint64_t a,uint64_t b) { return UINT64_MAX-a<b ? UINT64_MAX:a+b; }
void wki_timer_notify() { assert(!g_wki.transport_lock.held); ++notifications; }
void wki_peer_send_hello(WkiTransport* t,uint16_t node) {
    assert(!g_wki.transport_lock.held && !g_wki.peer_lock.held);
    if(initial_send) {
        assert(t->registration_generation!=0);
    } else {
        assert(t->chaos_in_flight.load()==1 && shutdown_holds==1);
        if(auto* p=wki_peer_find(node)) { assert(p->readers==1 && !p->writer && !p->auth_session.replay_lock.held); }
    }
    ++sends; if(on_send){auto f=std::move(on_send);on_send={};f();}
}
void rx_handler(WkiTransport*,WkiRxHandler) { assert(!g_wki.transport_lock.held); }
void wki_chaos_transport_register(WkiTransport* t){t->chaos_unregistering.store(false);}
void wki_chaos_transport_unregister(WkiTransport* t){assert(!g_wki.transport_lock.held && t->chaos_in_flight.load()==0);t->chaos_unregistering.store(true);}
void wki_chaos_rx_ingress(WkiTransport*,const void*,uint16_t,const WkiRxMetadata*){}
uint64_t s_next_transport_generation=1;
void wki_peer_cancel_hello_retries(WkiTransport*,uint64_t);
unsigned wki_ivshmem_transport_poll(unsigned){return 0;}
void drain_pending_epoch_reset_cleanups(){}
void drain_pending_fence_notifies(){}
void drain_pending_peer_goodbyes(){}
void wki_peer_send_hello_broadcast(){++broadcasts;}
void wki_peer_send_heartbeats(){}
uint64_t wki_hello_broadcast_interval_us(){return 30000000;}
uint64_t wki_min_heartbeat_interval_us(){return UINT64_MAX;}
uint64_t wki_schedule_next_heartbeat_deadline(uint64_t t,uint64_t d){return t+d;}
bool wki_peer_startup_grace_pending(const WkiPeer*,uint64_t){return false;}
uint64_t wki_peer_startup_grace_deadline_us(const WkiPeer*){return UINT64_MAX;}
uint64_t wki_peer_timeout_deadline_us(const WkiPeer*,uint64_t){return UINT64_MAX;}
uint64_t wki_peer_timeout_us(WkiPeer*){return UINT64_MAX;}
uint64_t wki_peer_confirm_grace_us(WkiPeer*){return 1000;}
uint64_t wki_peer_timeout_with_confirm_grace_us(WkiPeer*){return UINT64_MAX;}
bool wki_eth_recent_tx_pressure(uint64_t){return false;}
int wki_send_heartbeat_probe(WkiPeer*){return 0;}
void log_heartbeat_netdiag(uint16_t,const char*,uint64_t,uint64_t){}
void wki_peer_fence(WkiPeer*){assert(false);}
void wki_event_timer_tick(uint64_t){}
void wki_dev_proxy_fence_timeout_tick(uint64_t){}
void wki_load_report_send(){}
void wki_remote_compute_check_completions(){}
void wki_remote_vfs_gc_stale_fds(){}
void wki_routing_timer_tick(uint64_t){}
void wki_timer_tick(uint64_t){}
void wki_wait_timeout_scan(uint64_t){}
uint64_t s_last_hello_broadcast=0,s_next_heartbeat_send_deadline=0,s_last_heartbeat_send=0,s_last_vfs_fd_gc=0;
constexpr uint64_t VFS_FD_GC_INTERVAL_US=10000000;
'''

CASES = r'''
HelloPayload hint(uint16_t node=4,uint32_t boot=7,uint8_t nonce=9) {
    HelloPayload h;h.node_id=node;h.boot=boot;h.auth_nonce[0]=nonce;return h;
}
WkiTransport a,b;
void reset() {
    assert(shutdown_holds==0 && !s_hello_retry_lock.held);
    s_hello_retries={};s_hello_retry_count.store(0);
    g_wki.transports=nullptr;g_wki.transport_count=0;g_wki.my_node_id=1;g_wki.initialized=true;
    g_wki.peer_lock.unlocked={};g_wki.transport_lock.unlocked={};
    a.next=b.next=nullptr;a.registration_generation=b.registration_generation=0;
    a.chaos_in_flight.store(0);b.chaos_in_flight.store(0);a.set_rx_handler=b.set_rx_handler=rx_handler;
    for(auto& p:g_wki.peers){p.node_id=0;p.state=PeerState::UNKNOWN;p.remote_boot_epoch=0;p.remote_auth_nonce={};
        p.writer=false;p.readers=0;p.is_direct=true;p.auth_session.active=false;p.auth_session.rx_counter_high=0;}
    now=100;sends=notifications=broadcasts=neighbor_learns=0;on_send={};configured=true;initial_send=false;
    s_last_hello_broadcast=s_next_heartbeat_send_deadline=s_last_heartbeat_send=s_last_vfs_fd_gc=0;
    wki_transport_register(&a);
}
void tick(uint64_t t){now=t;wki_peer_timer_tick(t);assert(a.chaos_in_flight.load()==0 && shutdown_holds==0);}
WkiPeer& peer(const HelloPayload& h) {
    auto& p=g_wki.peers[0];p.node_id=h.node_id;p.remote_boot_epoch=h.boot;p.remote_auth_nonce=h.auth_nonce;return p;
}
void discovery(WkiTransport* t, const HelloPayload& h, uint16_t src=4, uint16_t len=sizeof(HelloPayload)) {
    WkiHeader hdr{src};WkiRxMetadata metadata{true,17};initial_send=true;
    discovery_rx(t,&hdr,&h,len,&metadata);initial_send=false;
}
int main() {
    // Execute the actual unsigned-discovery RX branch, including initial TX.
    reset();auto h=hint();discovery(&a,h);
    assert(sends==1 && notifications==1 && neighbor_learns==1 && s_hello_retry_count.load()==1);
    tick(now+1000000);assert(sends==2); // first HELLO/ACK was lost
    reset();auto& stable=peer(h);stable.state=PeerState::CONNECTED;
    stable.auth_session.active=true;stable.auth_session.rx_counter_high=9;
    for(unsigned i=0;i<20;++i) { now+=30000000;discovery(&a,h); }
    assert(sends==0 && notifications==0 && s_hello_retry_count.load()==0 && neighbor_learns==20);
    stable.auth_session.rx_counter_high=0;discovery(&a,h);assert(sends==1); // lost CONFIRM
    stable.auth_session.rx_counter_high=1;discovery(&a,h);assert(sends==1);
    tick(now+1000000);assert(sends==1 && s_hello_retry_count.load()==0);
    stable.remote_boot_epoch=8;discovery(&a,h);assert(sends==2); // new boot hint
    stable.remote_boot_epoch=h.boot;stable.remote_auth_nonce[0]=10;discovery(&a,h);assert(sends==3);
    stable.remote_auth_nonce=h.auth_nonce;stable.auth_session.active=false;discovery(&a,h);assert(sends==4);
    stable.auth_session.active=true;stable.state=PeerState::FENCED;discovery(&a,h);assert(sends==5);
    // A routed authenticated peer can become a newly discovered L2 neighbor.
    reset();auto& routed=peer(h);routed.state=PeerState::CONNECTED;routed.is_direct=false;
    routed.auth_session.active=true;routed.auth_session.rx_counter_high=5;
    discovery(&a,h);assert(sends==1 && s_hello_retry_count.load()==1);
    tick(now+1000000);assert(sends==2);
    routed.is_direct=true;tick(now+2000000);assert(sends==2 && s_hello_retry_count.load()==0);
    reset();discovery(&a,h,5);discovery(&a,h,4,0);discovery(nullptr,h);
    auto malformed=h;malformed.magic=0;discovery(&a,malformed);
    malformed=h;malformed.protocol_version=2;discovery(&a,malformed);
    malformed=h;malformed.boot=0;discovery(&a,malformed);
    malformed=h;malformed.auth_nonce={};discovery(&a,malformed);
    configured=false;discovery(&a,h);configured=true;discovery(&b,h);
    a.chaos_unregistering.store(true);discovery(&a,h);a.chaos_unregistering.store(false);
    g_wki.my_node_id=5;discovery(&a,h);
    assert(sends==0 && notifications==0 && s_hello_retry_count.load()==0);
    // Capacity exhaustion must retain the initial attempt for a valid hint.
    reset();for(unsigned i=0;i<WKI_MAX_PEERS;++i)wki_peer_retry_discovery(&a,hint(4+i));
    auto overflow=hint(200);discovery(&a,overflow,200);
    assert(sends==1 && s_hello_retry_count.load()==WKI_MAX_PEERS);
    // Real timer integration: existing stable mesh, target absent, lost firstHELLO.
    reset();wki_peer_retry_discovery(&a,h);
    assert(wki_peer_find(4)==nullptr && notifications==1 && s_hello_retry_count.load()==1);
    assert(hello_retry_next_deadline_us()==100+HELLO_RETRY_INITIAL_US);
    assert(wki_next_periodic_deadline_us(now)==100+HELLO_RETRY_INITIAL_US);
    tick(100+HELLO_RETRY_INITIAL_US-1);assert(sends==0);
    tick(100+HELLO_RETRY_INITIAL_US);assert(sends==1 && broadcasts==0);
    assert(hello_retry_next_deadline_us()==100+3000000);
    now+=100;wki_peer_retry_discovery(&a,h);assert(notifications==1 && hello_retry_next_deadline_us()==100+3000000);
    tick(100+3000000);assert(sends==2 && hello_retry_next_deadline_us()==100+7000000);
    tick(100+7000000);assert(sends==3 && hello_retry_next_deadline_us()==100+11000000);
    // FirstACK loss: staged peer UNKNOWN, no active reciprocal session.
    reset();auto& p=peer(h);wki_peer_retry_discovery(&a,h);tick(now+1000000);assert(sends==1 && p.readers==0);
    // FirstCONFIRM loss: initiator CONNECTED/active still must retry.
    reset();auto& q=peer(h);q.state=PeerState::CONNECTED;q.auth_session.active=true;
    wki_peer_retry_discovery(&a,h);tick(now+1000000);assert(sends==1);
    q.auth_session.rx_counter_high=1;tick(now+2000000);assert(sends==1 && s_hello_retry_count.load()==0);
    // Already current sessions stay cold; old active nonce/boot cannot suppress newboot.
    reset();auto& r=peer(h);r.state=PeerState::CONNECTED;r.auth_session.active=true;r.auth_session.rx_counter_high=99;
    wki_peer_retry_discovery(&a,h);assert(s_hello_retry_count.load()==0 && notifications==0);
    auto fresh=hint(4,8,10);wki_peer_retry_discovery(&a,fresh);tick(now+1000000);assert(sends==1);
    r.remote_boot_epoch=fresh.boot;tick(now+2000000);assert(sends==2); // nonce stillold
    r.remote_auth_nonce=fresh.auth_nonce;r.auth_session.active=false;tick(now+4000000);assert(sends==3);
    r.auth_session.active=true;r.auth_session.rx_counter_high=0;tick(now+4000000);assert(sends==4);
    r.auth_session.rx_counter_high=1;tick(now+4000000);assert(s_hello_retry_count.load()==0 && sends==4);
    // Writer contention doesn't wait/read key state/send; later admission succeeds.
    reset();auto& busy=peer(h);busy.writer=true;wki_peer_retry_discovery(&a,h);tick(now+1000000);assert(sends==0);
    busy.writer=false;tick(now+2000000);assert(sends==1 && busy.readers==0);
    // Direct ACK-installed sessions, including higher-ID initiators; routed skipped.
    reset();g_wki.my_node_id=5;auto& direct=peer(h);direct.state=PeerState::CONNECTED;direct.auth_session.active=true;
    wki_peer_retry_discovery(&a,h);assert(s_hello_retry_count.load()==0);
    direct.is_direct=false;wki_peer_retry_confirmation(&a,h);assert(s_hello_retry_count.load()==0);
    direct.is_direct=true;wki_peer_retry_confirmation(&a,h);tick(now+1000000);assert(sends==1);
    // Invalid/unprovisioned/self/higher-ID-only hint and unknown registry don't arm.
    reset();configured=false;wki_peer_retry_discovery(&a,h);configured=true;
    auto bad=h;bad.auth_nonce={};wki_peer_retry_discovery(&a,bad);bad=h;bad.boot=0;wki_peer_retry_discovery(&a,bad);
    bad=h;bad.magic=0;wki_peer_retry_discovery(&a,bad);bad=h;bad.protocol_version=2;wki_peer_retry_discovery(&a,bad);
    for(auto node:{0,1,65535})wki_peer_retry_discovery(&a,hint(node));
    wki_peer_retry_discovery(nullptr,h);wki_peer_retry_discovery(&b,h);assert(s_hello_retry_count.load()==0);
    // Cold idle doesn't lock/scan or transmit; coalescing never extends expiry.
    auto locks=lock_calls;drain_hello_retries(now);assert(hello_retry_next_deadline_us()==UINT64_MAX && lock_calls==locks && sends==0);
    wki_peer_retry_discovery(&a,h);auto expires=s_hello_retries[0].expires_us;auto due=s_hello_retries[0].due_us;
    now+=20;wki_peer_retry_discovery(&a,fresh);assert(s_hello_retries[0].expires_us==expires && s_hello_retries[0].due_us==due);
    tick(expires);assert(s_hello_retry_count.load()==0 && sends==0);
    // Full configured target table and per-tick TX budget.
    reset();for(unsigned i=0;i<WKI_MAX_PEERS;++i)wki_peer_retry_discovery(&a,hint(4+i));
    wki_peer_retry_discovery(&a,hint(200));assert(s_hello_retry_count.load()==WKI_MAX_PEERS);
    tick(now+1000000);assert(sends==HELLO_RETRY_TX_BUDGET);
    assert(hello_retry_next_deadline_us()==now);tick(now);assert(sends==WKI_MAX_PEERS);
    // Latest registered NIC replaces ingress without resetting retry deadlines.
    reset();wki_transport_register(&b);wki_peer_retry_discovery(&a,h);
    auto migrated=s_hello_retries[0];now+=10;wki_peer_retry_discovery(&b,h);
    assert(s_hello_retries[0].transport==&b && s_hello_retries[0].due_us==migrated.due_us && s_hello_retries[0].expires_us==migrated.expires_us);
    wki_transport_unregister(&a);finish_hello_retry(migrated);assert(s_hello_retry_count.load()==1);
    wki_transport_unregister(&b);assert(s_hello_retry_count.load()==0);
    // Saturated timestamps terminalize instead of wrapping/immediate infinite work.
    reset();now=UINT64_MAX-100;wki_peer_retry_discovery(&a,h);
    assert(s_hello_retries[0].due_us==UINT64_MAX && s_hello_retries[0].expires_us==UINT64_MAX);
    drain_hello_retries(UINT64_MAX);assert(sends==0 && s_hello_retry_count.load()==0);
    // Exact cancellation + allocator reuse: captured old pointer/generation can't pin.
    reset();wki_peer_retry_discovery(&a,h);auto old=s_hello_retries[0];auto gen=a.registration_generation;
    wki_transport_unregister(&a);assert(s_hello_retry_count.load()==0 && a.registration_generation==0);
    wki_transport_register(&a);assert(a.registration_generation!=gen && hello_retry_transport_pin(old)==nullptr);
    wki_peer_retry_discovery(&a,h);g_wki.transport_lock.lock();wki_peer_cancel_hello_retries(&a,gen);g_wki.transport_lock.unlock();
    assert(s_hello_retry_count.load()==1);
    // Replacement after snapshot: old completion can't delete fresh nonce work.
    old=s_hello_retries[0];wki_peer_retry_discovery(&a,fresh);finish_hello_retry(old);assert(s_hello_retry_count.load()==1);
    // Unregister/re-register between progress read and pin rejects captured work.
    reset();wki_peer_retry_discovery(&a,h);
    g_wki.peer_lock.unlocked=[](){wki_transport_unregister(&a);wki_transport_register(&a);};
    tick(now+1000000);assert(sends==0 && s_hello_retry_count.load()==0);
    // Detach during admitted TX leaves the transport alive through pin release.
    reset();wki_peer_retry_discovery(&a,h);
    on_send=[](){assert(!s_hello_retry_lock.held);g_wki.transport_lock.lock();g_wki.transports=nullptr;g_wki.transport_count=0;
        wki_peer_cancel_hello_retries(&a,a.registration_generation);a.registration_generation=0;g_wki.transport_lock.unlock();
        assert(a.chaos_in_flight.load()==1);};
    tick(now+1000000);assert(sends==1 && s_hello_retry_count.load()==0 && a.chaos_in_flight.load()==0);
    wki_chaos_transport_unregister(&a);
    // Shutdown complete cancellation, multiple live registrations.
    reset();wki_transport_register(&b);wki_peer_retry_discovery(&a,h);wki_peer_retry_discovery(&b,hint(5));
    g_wki.transport_lock.lock();g_wki.transports=nullptr;wki_peer_cancel_hello_retries(nullptr,0);g_wki.transport_lock.unlock();
    assert(hello_retry_next_deadline_us()==UINT64_MAX && s_hello_retry_count.load()==0);
    // Registration uint64 exhaustion is terminal and independent of chaos ID wrap.
    reset();s_next_transport_generation=UINT64_MAX;wki_transport_register(&b);assert(b.registration_generation==UINT64_MAX);
    wki_transport_unregister(&b);wki_transport_register(&b);assert(b.registration_generation==0 && s_next_transport_generation==0);
    wki_peer_retry_discovery(&b,h);assert(s_hello_retry_count.load()==0);
    assert(next_transport_registration_generation()==0);
    puts("PASS actual HELLO retry/timer/progress/publication/transport lifetime boundaries");
}
'''


def main():
    source = (ROOT / "modules/kern/src/net/wki/peer.cpp").read_text()
    core = (ROOT / "modules/kern/src/net/wki/wki.cpp").read_text()
    start = source.index("constexpr uint64_t HELLO_RETRY_INITIAL_US")
    end = source.index("}  // namespace\n\n// -----------------------------------------------------------------------------\n// HELLO broadcast", start)
    real = source[start:end]
    if "WOS_HELLO_RETRY_OLD_PROGRESS" in os.environ:
        old_progress = Path(os.environ["WOS_HELLO_RETRY_OLD_PROGRESS"]).read_text()
        real = real.replace(function(source, "hello_retry_session_progress"), function(old_progress, "hello_retry_session_progress"))
    for name in ("queue_hello_retry", "wki_peer_retry_discovery", "wki_peer_retry_confirmation", "wki_peer_cancel_hello_retries"):
        real += function(source, name)
    for name in ("next_transport_registration_generation", "wki_transport_register", "wki_transport_unregister"):
        real += function(core, name)
    timer = Path(os.environ["WOS_HELLO_RETRY_OLD_PEER"]).read_text() if "WOS_HELLO_RETRY_OLD_PEER" in os.environ else source
    real += function(timer, "wki_peer_timer_tick") + function(source, "wki_next_periodic_deadline_us")
    # Integration asserts use real functions to enforce publication/cancel ordering.
    rx = function(core, "wki_rx")
    assert rx.index("WkiAuthFrameResult::DISCOVERY_HINT") < rx.index("wki_peer_retry_discovery(transport, *hello)") < rx.index("wki_peer_send_hello(transport, hello->node_id);")
    branch_core = Path(os.environ["WOS_HELLO_RETRY_OLD_CORE"]).read_text() if "WOS_HELLO_RETRY_OLD_CORE" in os.environ else core
    start = branch_core.index("    if (AUTH_RESULT == WkiAuthFrameResult::DISCOVERY_HINT) {")
    end, depth = branch_core.index("{", start) + 1, 1
    while depth:
        depth += (branch_core[end] == "{") - (branch_core[end] == "}")
        end += 1
    real += '''
void discovery_rx(WkiTransport* transport, const WkiHeader* hdr, const void* payload,
                  uint16_t PAYLOAD_LEN, const WkiRxMetadata* metadata) {
    auto const AUTH_RESULT = WkiAuthFrameResult::DISCOVERY_HINT;
''' + branch_core[start:end] + "\n}\n"
    assert rx.index("if (installed)") < rx.index("wki_peer_retry_confirmation(") < rx.index("wki_peer_send_hello_confirm(peer);")
    shutdown = function(core, "wki_shutdown")
    assert shutdown.index("g_wki.transports = nullptr;") < shutdown.index("wki_peer_cancel_hello_retries(nullptr, 0);") < shutdown.index("wki_chaos_transport_unregister(t);")
    compiler = os.environ.get("HOST_CXX") or shutil.which("c++", path=os.defpath)
    assert compiler
    with tempfile.TemporaryDirectory(prefix="wos-hello-retry-") as directory:
        harness = Path(directory) / "retry.cpp"
        binary = Path(directory) / "retry"
        harness.write_text(PRELUDE + real.replace("log::warn", "TestLogger::warn") + CASES)
        subprocess.run([compiler, "-std=c++20", "-Wall", "-Wextra", "-Werror", "-fsanitize=address,undefined", "-fno-omit-frame-pointer", str(harness), "-o", str(binary)], check=True)
        env = os.environ | {"ASAN_OPTIONS": "detect_leaks=0"}
        subprocess.run([str(binary)], env=env, check=True, timeout=20)


if __name__ == "__main__":
    main()
