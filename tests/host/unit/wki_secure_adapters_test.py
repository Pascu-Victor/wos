#!/usr/bin/env python3
"""Compile complete actual ETH/RoCE TUs against instrumented external boundaries.

Actual wire/record/packet declarations are included. NetDevice, peer lifecycle,
registry, driver TX and common admission are seams; authentication, hardware,
registry lease acquisition and engine operation completion are not claimed.
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
KERN = ROOT / "modules/kern/src"
ETH = KERN / "net/wki/transport_eth.cpp"
ROCE = KERN / "net/wki/transport_roce.cpp"

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <set>
#include <span>
#include <string_view>
#include <vector>
#include <net/address.hpp>
#include <net/packet.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/rdma_protocol.hpp>
#include <net/wki/transport_roce.hpp>
#define WOS_SELFTEST 1
#define CHECK(x) do { if (!(x)) { std::fprintf(stderr,"CHECK failed %s:%d: %s\n",__FILE__,__LINE__,#x); std::abort(); } } while(0)
namespace ker::net::wki { struct WkiTransport; }
namespace fixture {
ker::net::wki::WkiTransport* received_transport=nullptr;
struct Counts { unsigned bulk=0,reserve=0,frees=0,transmits=0,ingress=0,ordinary=0,registrations=0,locks=0; bool operator==(const Counts&) const=default; } counts;
std::set<void*> owned;
bool fail_pool=false;
int tx_status=0;
uint64_t now=1000;
std::vector<uint8_t> transmitted,received;
ker::net::NetDevice* transmitted_dev=nullptr;
ker::net::proto::MacAddress transmitted_mac{},received_mac{};
uint16_t transmitted_type=0;
}
namespace ker::mod::sys {
struct Spinlock { void lock(){++fixture::counts.locks;} void unlock(){}; };
}
namespace ker::mod::dbg {
template<size_t N> struct Name { char data[N]; constexpr Name(const char(&v)[N]) {std::copy_n(v,N,data);} };
template<Name N> struct logger { template<class... A> static void info(const char*,A...) {} };
template<class... A> void log(const char*,A...) {}
}
namespace ker::net {
struct NetDevice {
    std::array<char,16> name{};
    proto::MacAddress mac{};
    uint32_t mtu=9000;
    void* remotable=nullptr;
    bool wki_transport=false;
};
auto pkt_alloc_tx() -> PacketBuffer* {
    ++fixture::counts.bulk;
    if(fixture::fail_pool) return nullptr;
    auto* p=new PacketBuffer; p->data=p->storage.data()+PKT_HEADROOM; fixture::owned.insert(p); return p;
}
auto pkt_alloc() -> PacketBuffer* {
    ++fixture::counts.reserve;
    if(fixture::fail_pool) return nullptr;
    auto* p=new PacketBuffer; p->data=p->storage.data()+PKT_HEADROOM; fixture::owned.insert(p); return p;
}
void pkt_free(PacketBuffer* p) {
    if(!p) return;
    CHECK(fixture::owned.erase(p)==1); ++fixture::counts.frees;
    if(p->lifetime_release) p->lifetime_release(p->lifetime_ctx);
    delete p;
}
namespace proto {
/* actual ETH_HLEN inserted here */
const MacAddress ETH_BROADCAST=MacAddress::broadcast();
auto eth_tx(NetDevice* d, PacketBuffer* p,const MacAddress& mac,uint16_t type) -> int {
    CHECK(d && p && p->dev==d);
    fixture::transmitted.assign(p->data,p->data+p->len);
    fixture::transmitted_dev=d; fixture::transmitted_mac=mac; fixture::transmitted_type=type;
    ++fixture::counts.transmits; pkt_free(p); return fixture::tx_status;
}
}
}
namespace ker::net::wki {
/* actual transport declarations inserted here */
constexpr size_t WKI_MAX_PEERS=256;
constexpr size_t WKI_MAX_TRANSPORTS=8;
constexpr uint32_t WKI_PEER_FENCE_CONFIRM_GRACE_MS=1000;
enum class PeerState : uint8_t { UNKNOWN,HELLO_SENT,CONNECTED,FENCED,RECONNECTING };
struct WkiPeer {uint16_t node_id=2; proto::MacAddress mac{{2,3,4,5,6,7}}; PeerState state=PeerState::CONNECTED; bool is_direct=true;
    struct { bool active=true; } auth_session;
    WkiTransport* transport=nullptr; WkiTransport* rdma_transport=nullptr;
};
WkiPeer peer;
struct WkiState {uint16_t my_node_id=1; proto::MacAddress my_mac; ker::mod::sys::Spinlock transport_lock;
    WkiTransport* transports=nullptr; size_t transport_count=0; bool initialized=false;
} g_wki;
auto wki_now_us() -> uint64_t {return fixture::now;}
auto wki_peer_find(uint16_t id) -> WkiPeer* {return id==peer.node_id ? &peer : nullptr;}
auto wki_peer_frame_was_forwarded(const WkiHeader*) -> bool {return false;}
void wki_peer_note_rx_contact(WkiTransport*,uint16_t,const proto::MacAddress&) {}
void ordinary_rx(WkiTransport*,const void*,uint16_t,const WkiRxMetadata*) {++fixture::counts.ordinary;}
void wki_transport_register(WkiTransport* t) {
    ++fixture::counts.registrations; t->registration_generation=fixture::counts.registrations;
    t->next=g_wki.transports; g_wki.transports=t; ++g_wki.transport_count;
    t->set_rx_handler(t,ordinary_rx);
}
void wki_chaos_secure_rx_ingress(WkiTransport* transport,const void* data,uint16_t len,const WkiRxMetadata* meta) {
    CHECK(meta && meta->has_src_mac && !meta->has_direct_peer);
    ++fixture::counts.ingress; fixture::received_transport=transport; fixture::received.assign(static_cast<const uint8_t*>(data),static_cast<const uint8_t*>(data)+len);
    fixture::received_mac=meta->src_mac;
}
auto wki_eth_transport_find(net::NetDevice*) -> WkiTransport*;
}
'''

CASES = r'''
using namespace ker::net;
using namespace ker::net::wki;
namespace {
NetDevice primary,secondary,unknown;
std::vector<uint8_t> record(size_t len=rdma_protocol::RECORD_BYTES) {
    std::vector<uint8_t> bytes(len,0x6a); uint32_t magic=rdma_protocol::MAGIC;
    if(len>=4) std::memcpy(bytes.data(),&magic,4); return bytes;
}
void reset(uint32_t mtu=9000) {
    CHECK(fixture::owned.empty());
    std::destroy_at(&s_eth_transport); std::construct_at(&s_eth_transport);
    std::destroy_at(&s_eth_priv); std::construct_at(&s_eth_priv);
    for(auto& slot:s_secondary_transports) {std::destroy_at(&slot); std::construct_at(&slot);}
    s_eth_initialized=false; fixture::counts={}; fixture::fail_pool=false; fixture::tx_status=0;
    fixture::transmitted.clear(); fixture::received.clear(); fixture::transmitted_dev=nullptr; fixture::received_transport=nullptr;
    g_wki.transports=nullptr; g_wki.transport_count=0;
    primary={}; secondary={}; unknown={}; primary.mtu=mtu;
    primary.mac=proto::MacAddress{{2,0,0,0,0,1}}; secondary.mac=proto::MacAddress{{2,0,0,0,0,2}};
    peer={};
}
void initialize(uint32_t mtu=9000) {reset(mtu); wki_eth_transport_init(&primary); peer.transport=&s_eth_transport;}
auto packet(std::span<const uint8_t> bytes) -> PacketBuffer* {
    auto* p=pkt_alloc_tx(); CHECK(p); CHECK(bytes.size()<=PKT_BUF_SIZE-PKT_HEADROOM);
    if (!bytes.empty()) std::memcpy(p->data,bytes.data(),bytes.size()); p->len=bytes.size(); p->src_mac=proto::MacAddress{{2,9,8,7,6,5}}; return p;
}
void capability() {
    for(auto mtu:{uint32_t{1500},uint32_t{8349},uint32_t{8350},uint32_t{9000}}) {
        initialize(mtu); CHECK(!s_eth_transport.rdma_capable);
        CHECK((s_eth_transport.secure_kind==rdma_protocol::Transport::ROCE)==(mtu>=8350));
        CHECK((s_eth_transport.secure_tx!=nullptr)==(mtu>=8350));
    }
}
void tx_exact() {
    initialize(); auto bytes=record();
    for(auto len:{size_t{144},size_t{8336}}) {
        CHECK(s_eth_transport.secure_tx(&s_eth_transport,2,bytes.data(),len)==0);
        CHECK(fixture::transmitted_dev==&primary && fixture::transmitted_type==0x88b8 && fixture::transmitted_mac==peer.mac);
        CHECK(fixture::transmitted.size()==len && std::memcmp(fixture::transmitted.data(),bytes.data(),len)==0);
    }
    std::array<uint8_t,WKI_HEADER_SIZE> ordinary{};
    wki_eth_rx(&secondary,packet(ordinary));
    auto* second=wki_eth_transport_find(&secondary); CHECK(second && second!=&s_eth_transport && second->secure_tx);
    peer.transport=second;
    CHECK(second->secure_tx(second,2,bytes.data(),bytes.size())==0 && fixture::transmitted_dev==&secondary);
    auto original=fixture::transmitted; std::fill(bytes.begin(),bytes.end(),0);
    CHECK(fixture::transmitted==original && fixture::counts.reserve==0 && fixture::owned.empty());
}
void tx_identity() {
    initialize(); auto bytes=record(); auto reject=[&] {
        auto n=fixture::counts.bulk;
        CHECK(eth_secure_tx(&s_eth_transport,2,bytes.data(),bytes.size())==-1 && fixture::counts.bulk==n);
    };
    peer.state=PeerState::FENCED; reject(); peer.state=PeerState::CONNECTED;
    peer.auth_session.active=false; reject(); peer.auth_session.active=true;
    peer.is_direct=false; reject(); peer.is_direct=true;
    peer.transport=nullptr; reject(); peer.transport=&s_eth_transport;
    for(auto mac:{proto::MacAddress::zero(),proto::MacAddress::broadcast(),proto::MacAddress{{1,2,3,4,5,6}}}) {peer.mac=mac; reject();}
    peer.mac=proto::MacAddress{{2,3,4,5,6,7}};
    for(auto id:{uint16_t{0},uint16_t{1},uint16_t{3},uint16_t{65535}})
        CHECK(eth_secure_tx(&s_eth_transport,id,bytes.data(),bytes.size())==-1);
    CHECK(fixture::counts.bulk==0);
}
void tx_format() {
    initialize(); auto bytes=record();
    for(auto len:{uint16_t{0},uint16_t{3},uint16_t{143},uint16_t{8337},uint16_t{65535}})
        CHECK(eth_secure_tx(&s_eth_transport,2,bytes.data(),len)==-1);
    bytes[0]^=1; CHECK(eth_secure_tx(&s_eth_transport,2,bytes.data(),bytes.size())==-1); bytes[0]^=1;
    CHECK(eth_secure_tx(nullptr,2,bytes.data(),bytes.size())==-1);
    CHECK(eth_secure_tx(&s_eth_transport,2,nullptr,144)==-1);
    WkiTransport clone{}; clone.secure_kind=rdma_protocol::Transport::ROCE; clone.private_data=&s_eth_priv;
    CHECK(eth_secure_tx(&clone,2,bytes.data(),bytes.size())==-1);
    auto* old_priv=s_eth_transport.private_data; s_eth_transport.private_data=nullptr;
    CHECK(eth_secure_tx(&s_eth_transport,2,bytes.data(),bytes.size())==-1); s_eth_transport.private_data=old_priv;
    primary.mtu=8349; CHECK(eth_secure_tx(&s_eth_transport,2,bytes.data(),bytes.size())==-1); primary.mtu=9000;
    s_eth_transport.registration_generation=0; CHECK(eth_secure_tx(&s_eth_transport,2,bytes.data(),bytes.size())==-1);
    s_eth_transport.registration_generation=1; s_eth_transport.chaos_unregistering=true;
    CHECK(eth_secure_tx(&s_eth_transport,2,bytes.data(),bytes.size())==-1);
    CHECK(fixture::counts.bulk==0 && fixture::counts.reserve==0);
}
void ownership() {
    initialize(); auto bytes=record(); fixture::fail_pool=true;
    CHECK(eth_secure_tx(&s_eth_transport,2,bytes.data(),bytes.size())==-1 && fixture::owned.empty());
    fixture::fail_pool=false; fixture::tx_status=-7;
    CHECK(eth_secure_tx(&s_eth_transport,2,bytes.data(),bytes.size())==-7 && fixture::owned.empty());
    CHECK(fixture::counts.reserve==0 && fixture::counts.frees==1);
}
void rx_exact() {
    initialize(); auto bytes=record();
    for(auto len:{size_t{144},size_t{8336}}) {
        auto original=std::vector(bytes.begin(),bytes.begin()+len);
        auto* p=packet(std::span(bytes.data(),len)); auto mac=p->src_mac;
        auto frees=fixture::counts.frees, ingress=fixture::counts.ingress;
        roce_rx(&primary,p);
        CHECK(fixture::counts.frees==frees+1 && fixture::counts.ingress==ingress+1);
        CHECK(fixture::received==original && fixture::received_mac==mac && fixture::received_transport==&s_eth_transport && fixture::owned.empty());
    }
    std::array<uint8_t,WKI_HEADER_SIZE> ordinary{}; wki_eth_rx(&secondary,packet(ordinary));
    auto* second=wki_eth_transport_find(&secondary); CHECK(second);
    roce_rx(&secondary,packet(bytes));
    CHECK(fixture::received_transport==second && fixture::received==bytes && fixture::owned.empty());
}
void rx_reject() {
    initialize(); auto bytes=record(); auto regs=fixture::counts.registrations;
    roce_rx(&unknown,packet(bytes)); CHECK(!wki_eth_transport_find(&unknown));
    roce_rx(nullptr,packet(bytes));
    for(auto len:{size_t{0},size_t{3},size_t{143},size_t{8337},size_t{9000}}) {
        auto b=record(len); roce_rx(&primary,packet(b));
    }
    bytes[0]^=1; roce_rx(&primary,packet(bytes)); bytes[0]^=1;
    primary.mtu=8349; roce_rx(&primary,packet(bytes)); primary.mtu=9000;
    s_eth_transport.chaos_unregistering=true; roce_rx(&primary,packet(bytes)); s_eth_transport.chaos_unregistering=false;
    s_eth_transport.registration_generation=0; roce_rx(&primary,packet(bytes)); s_eth_transport.registration_generation=1;
    std::array<uint8_t,28> raw{}; raw[0]=1; raw[1]=1; roce_rx(&primary,packet(raw));
    roce_rx(&primary,nullptr);
    CHECK(fixture::counts.ingress==0 && fixture::counts.registrations==regs && fixture::counts.ordinary==0 && fixture::owned.empty());
}
void raw_closure() {
    reset(); uint32_t key=0x12345678;
    CHECK(roce_rdma_register_region(reinterpret_cast<WkiTransport*>(1),UINT64_MAX,UINT32_MAX,&key)==-1 && key==0x12345678);
    CHECK(roce_rdma_unregister_region(nullptr,UINT32_MAX,UINT32_MAX)==-1);
    for(auto len:{uint32_t{0},uint32_t{4096},UINT32_MAX}) {
        CHECK(!wki_roce_region_reset_received(UINT32_MAX));
        CHECK(!wki_roce_region_prepare_tagged_write(UINT32_MAX,UINT16_MAX));
        CHECK(!wki_roce_region_tagged_write_complete(UINT32_MAX,UINT16_MAX,len));
        CHECK(!wki_roce_region_wait_received(UINT32_MAX,len,UINT64_MAX));
        CHECK(!wki_roce_region_wait_tagged_write(UINT32_MAX,UINT16_MAX,len,UINT64_MAX));
        CHECK(wki_roce_rdma_write_tagged(65535,UINT32_MAX,UINT64_MAX,reinterpret_cast<void*>(1),len,65535)==-1);
        wki_roce_region_finish_tagged_write(UINT32_MAX,UINT16_MAX);
    }
    wki_roce_transport_init(); CHECK(wki_roce_transport_get()==nullptr && fixture::counts==fixture::Counts{});
    CHECK(wki_roce_selftest_uninitialized_receive_preserves_region()); CHECK(fixture::owned.empty());
}
void ordinary() {
    initialize(); WkiHeader hdr{}; hdr.version_flags=wki_version_flags(WKI_VERSION,0); hdr.msg_type=static_cast<uint8_t>(MsgType::HELLO);
    hdr.channel_id=WKI_CHAN_CONTROL; hdr.payload_len=0;
    CHECK(eth_wki_tx(&s_eth_transport,WKI_NODE_BROADCAST,&hdr,sizeof(hdr))==0);
    CHECK(fixture::counts.reserve==1 && fixture::transmitted_type==WKI_ETHERTYPE && fixture::transmitted_mac==proto::ETH_BROADCAST);
    hdr.msg_type=static_cast<uint8_t>(MsgType::DEV_OP_REQ);
    CHECK(eth_wki_tx(&s_eth_transport,2,&hdr,sizeof(hdr))==0 && fixture::counts.bulk==1);
    auto* p=packet(std::span(reinterpret_cast<const uint8_t*>(&hdr),sizeof(hdr)));
    wki_eth_rx(&secondary,p); CHECK(fixture::counts.ordinary==1 && fixture::counts.registrations==2);
    CHECK(fixture::counts.ingress==0 && fixture::owned.empty());
}
}
int main(int argc,char** argv) {
    struct Test{const char* name;void(*run)();};
    Test tests[]={{"capability",capability},{"tx_exact",tx_exact},{"tx_identity",tx_identity},{"tx_format",tx_format},
        {"ownership",ownership},{"rx_exact",rx_exact},{"rx_reject",rx_reject},{"raw_closure",raw_closure},{"ordinary",ordinary}};
    size_t n=0; for(auto t:tests) if(argc==1 || std::string_view(argv[1])==t.name) {t.run();++n;std::printf("PASS %s\n",t.name);}
    CHECK(n && fixture::owned.empty()); std::printf("%zu adapter groups passed\n",n);
}
'''


def body(path: Path) -> str:
    return re.sub(r"^#include[^\n]*\n", "", path.read_text(), flags=re.MULTILINE)


def generate(eth: Path, roce: Path) -> str:
    source = (KERN / "net/wki/wki.hpp").read_text()
    start = source.index("struct WkiRxMetadata {")
    end = source.index("// Peer State Machine", start)
    declarations = source[start:end]
    declarations = declarations[:declarations.rfind("// -----------------------------------------------------------------------------")]
    eth_hlen = re.search(r"^constexpr size_t ETH_HLEN =[^\n]+;", (KERN / "net/proto/ethernet.hpp").read_text(), re.MULTILINE)
    assert eth_hlen
    prelude = PRELUDE.replace("/* actual ETH_HLEN inserted here */", eth_hlen.group(0))
    prelude = prelude.replace("/* actual transport declarations inserted here */", declarations)
    return prelude + body(KERN / "net/wki/transport_eth.hpp").replace("#pragma once", "") + body(eth) + body(roce) + CASES


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--eth-source", type=Path, default=ETH)
    parser.add_argument("--roce-source", type=Path, default=ROCE)
    parser.add_argument("--case")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    directory = args.output_dir or Path(tempfile.mkdtemp(prefix="wki-secure-adapters-"))
    directory.mkdir(parents=True, exist_ok=True)
    cpp, binary = directory / "actual_adapters.cpp", directory / "actual_adapters"
    if cpp.exists():
        raise RuntimeError("refusing to overwrite earlier proof")
    cpp.write_text(generate(args.eth_source, args.roce_source))
    command = [os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++"), "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Werror",
               "-Wno-unused-const-variable", "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-I", str(KERN), str(cpp), "-o", str(binary)]
    (directory / "compiler-command.json").write_text(json.dumps(command, indent=2)+"\n")
    result = subprocess.run(command, capture_output=True, text=True)
    (directory / "compile.log").write_text(result.stdout+result.stderr)
    if result.returncode:
        raise RuntimeError(f"compile failed {result.returncode}: {directory / 'compile.log'}")
    env = os.environ.copy()
    if env.get("WOS_HOST_TEST_DISABLE_LSAN"):
        env["ASAN_OPTIONS"] = env.get("ASAN_OPTIONS", "")+":detect_leaks=0"
    run = [str(binary)] + ([args.case] if args.case else [])
    (directory / "run-command.json").write_text(json.dumps(run, indent=2)+"\n")
    env["UBSAN_OPTIONS"] = env.get("UBSAN_OPTIONS", "")+":halt_on_error=1"
    result = subprocess.run(run, capture_output=True, text=True, env=env)
    (directory / "run.log").write_text(result.stdout+result.stderr)
    inputs = [args.eth_source,args.roce_source,KERN / "net/wki/wki.hpp",KERN / "net/wki/transport_roce.hpp", KERN / "net/wki/transport_eth.hpp",
              KERN / "net/wki/wire.hpp",KERN / "net/wki/rdma_protocol.hpp",KERN / "net/wki/rdma_crypto.hpp",
              KERN / "net/wki/auth_crypto.hpp",KERN / "net/proto/ethernet.hpp",KERN / "net/address.hpp",KERN / "net/packet.hpp",Path(__file__)]
    outputs = [cpp,binary,directory / "compiler-command.json",directory / "compile.log",directory / "run-command.json",directory / "run.log"]
    receipt = {"compile_returncode":0,"run_returncode":result.returncode,"case":args.case or "all",
               "complete_transport_bodies_verbatim_after_include_removal":True,
               "scope":__doc__,"inputs":[{"path":str(p.resolve()),"sha256":digest(p)} for p in inputs],
               "outputs":[{"path":str(p.resolve()),"sha256":digest(p)} for p in outputs]}
    (directory / "result.json").write_text(json.dumps(receipt,indent=2)+"\n")
    print(result.stdout,end="")
    if result.returncode or "runtime error:" in result.stderr or "AddressSanitizer:" in result.stderr:
        raise RuntimeError(f"execution failed {result.returncode}: {directory / 'run.log'}")


if __name__ == "__main__":
    main()
