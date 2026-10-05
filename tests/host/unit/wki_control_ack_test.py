#!/usr/bin/env python3
"""Compile real resolver, ACK sender and bounded RX forwarding with host doubles.

Peer/lock/clock/SPF/auth/transport doubles check routing and lifetime contracts,
not cryptographic tags or kernel concurrency. --source accepts saved old code.
"""
import argparse,os,re,shutil,subprocess,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
def extract(source,marker,struct=False):
 start=source.index(marker);brace=source.index('{',start);pos=brace+1;depth=1
 while depth:
  depth+=(source[pos]=='{')-(source[pos]=='}');pos+=1
 if struct:pos=source.index(';',pos)+1
 return source[start:pos]
PRELUDE=r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>
using std::memcpy;
constexpr uint32_t WKI_COST_INFINITY=0xffffffff;
namespace mod::sched {struct KernelShutdownHold {};inline KernelShutdownHold hold_kernel_shutdown(){return {};}}
constexpr uint16_t WKI_NODE_INVALID=0,WKI_NODE_BROADCAST=65535;
constexpr size_t WKI_HEADER_SIZE=32,WKI_AUTH_TRAILER_SIZE=40,WKI_MAX_FRAME_SIZE=9000;
constexpr uint8_t WKI_VERSION=3,WKI_FLAG_ACK_PRESENT=8,WKI_FLAG_PRIORITY=4,WKI_FLAG_FRAGMENT=2,WKI_FLAG_ACK_CHANNEL=1;
constexpr uint16_t WKI_CHAN_CONTROL=0,WKI_CAP_ROUTING_LEASE=128;
constexpr uint32_t WKI_ACK_CHANNEL_MASK=65535;
constexpr uint8_t wki_flags(uint8_t v){return v&15;}
constexpr uint8_t wki_version(uint8_t v){return v>>4;}
constexpr uint8_t wki_version_flags(uint8_t v,uint8_t f){return (v<<4)|(f&15);}
struct Lock {bool held=false;void lock(){assert(!held);held=true;}void unlock(){assert(held);held=false;}};
namespace net {struct PacketBuffer{};void pkt_free(PacketBuffer*){}}
struct WkiTransport {WkiTransport* next=nullptr;std::atomic<bool> chaos_unregistering=false;std::atomic<unsigned> chaos_in_flight=0;};
enum class PeerState {CONNECTED,FENCED};
struct WkiPeer {
 uint16_t node_id=0;bool is_direct=false;PeerState state=PeerState::CONNECTED;WkiTransport* transport=nullptr;
 Lock lock;uint32_t remote_boot_epoch=11;struct {Lock replay_lock;bool active=true;uint32_t generation=2;} auth_session;
 bool busy=false,owned=false;unsigned readers=0;
};
struct {uint16_t my_node_id=1,capabilities=0;Lock peer_lock,transport_lock;WkiTransport* transports=nullptr;} g_wki;
std::array<WkiPeer,5> peers;WkiTransport link;uint64_t now_us=100;
bool service=false,discovery=true,publication=true,authenticate=true,expire_on_auth=false;
unsigned sends=0,signs=0,lookups=0;int tx_result=0;bool last_discovery=false;
std::vector<uint8_t> sent;
uint64_t wki_now_us(){return now_us;}
WkiPeer* wki_peer_find(uint16_t node){return node>0&&node<5&&peers[node].node_id==node?&peers[node]:nullptr;}
bool wki_peer_lifecycle_rx_try_acquire(WkiPeer* p){if(!p||p->busy)return false;++p->readers;return true;}
bool wki_peer_lifecycle_owned_by_current_task(WkiPeer* p){return p&&p->owned;}
void wki_peer_lifecycle_rx_release(WkiPeer* p){if(p){assert(p->readers);--p->readers;}}
'''
STUBS=r'''
bool wki_routing_lookup(uint16_t dst,RoutingEntry* r,bool allow_discovery){
 assert(!g_wki.peer_lock.held);++lookups;last_discovery=allow_discovery;
 if(!(allow_discovery?discovery:service))return false;
 *r={};r->dst_node=dst;r->next_hop=2;r->hop_count=3;r->valid=true;r->transport=&link;
 r->publication_generation=7;r->lease_expires_us=1000;r->next_hop_boot_epoch=11;r->next_hop_auth_generation=2;return true;
}
bool wki_routing_snapshot_current(uint64_t gen,uint64_t expiry){assert(gen==7);return publication&&now_us<expiry;}
int wki_transport_send(WkiTransport* t,uint16_t hop,const void* bytes,uint16_t len){
 assert(t==&link&&hop==2&&t->chaos_in_flight==1&&peers[2].readers==1);
 assert(!g_wki.peer_lock.held&&!g_wki.transport_lock.held);++sends;
 sent.assign(static_cast<const uint8_t*>(bytes),static_cast<const uint8_t*>(bytes)+len);return tx_result;
}
int wki_transport_send_pkt(WkiTransport*,uint16_t,net::PacketBuffer*){assert(false);return -1;}
constexpr int WKI_ERR_INVALID=-1,WKI_ERR_NO_ROUTE=-2;
size_t wki_auth_append_frame(WkiHeader* h,uint8_t* f,size_t cap){
 assert(peers[h->dst_node].readers==1||peers[h->dst_node].owned);
 assert(peers[2].readers==1&&link.chaos_in_flight==1);++signs;
 if(!authenticate||!peers[h->dst_node].auth_session.active)return 0;
 if(expire_on_auth)publication=false;
 assert(cap==WKI_HEADER_SIZE+WKI_AUTH_TRAILER_SIZE);std::fill(f+WKI_HEADER_SIZE,f+cap,0xa5);return cap;
}
uint32_t wki_frame_checksum(const WkiHeader&,const uint8_t*){return 123;}
struct WkiRxMetadata{};
struct WkiForwardFrameSlot {std::array<uint8_t,WKI_MAX_FRAME_SIZE> frame{};bool claimed=false;} slot;
bool pool_available=true;unsigned claims=0;
WkiForwardFrameSlot* forward_frame_claim(){++claims;if(!pool_available)return nullptr;assert(!slot.claimed);slot.claimed=true;return &slot;}
void forward_frame_release(WkiForwardFrameSlot* p){if(p){assert(p==&slot&&p->claimed);p->claimed=false;}}
'''
CASES=r'''
void clean(){
 assert(peers[2].readers==0&&peers[4].readers==0&&link.chaos_in_flight==0&&!slot.claimed);
 service=false;discovery=true;publication=true;authenticate=true;expire_on_auth=false;sends=signs=lookups=claims=0;
 tx_result=0;pool_available=true;now_us=100;link.chaos_unregistering=false;g_wki.transports=&link;g_wki.capabilities=0;
 peers[2].busy=peers[4].busy=false;peers[2].owned=peers[4].owned=false;peers[4].auth_session.active=true;
 peers[2].auth_session.generation=2;peers[2].state=peers[4].state=PeerState::CONNECTED;sent.clear();
}
WkiHeader ack(){WkiHeader h{};h.version_flags=wki_version_flags(3,8);h.msg_type=static_cast<uint8_t>(MsgType::HEARTBEAT_ACK);h.src_node=1;h.dst_node=4;h.channel_id=0;h.ack_num=14;h.hop_ttl=16;h.credits=63;return h;}
void send_ack(WkiHeader h,int expected){assert(wki_send_authenticated_header(nullptr,0,h)==expected);}
int main(){
 peers[2].node_id=2;peers[2].is_direct=true;peers[2].transport=&link;peers[4].node_id=4;
 // Saved failure state: origin CONNECTED/current, invalid service route,
 // discovery still exists and incoming seq22 waits behind missing seq15.
 clean();auto h=ack();send_ack(h,0);assert(sends==1&&signs==1&&last_discovery);
 auto wire=sent;assert(wire.size()==72&&std::memcmp(wire.data(),&h,32)==0);
 clean();h.version_flags=wki_version_flags(3,8|4);send_ack(h,0);assert(sends==1);
 // Empty ACK can retire accepted control records, but not bypass service APIs.
 for(unsigned field=0;field<7;++field){
  clean();auto bad=ack();
  if(field==0)bad.msg_type=static_cast<uint8_t>(MsgType::RESOURCE_ADVERT);
  if(field==1)bad.version_flags=wki_version_flags(3,0);
  if(field==2)bad.channel_id=3;
  if(field==3)bad.seq_num=1;
  if(field==4)bad.reserved=3;
  if(field==5)bad.version_flags=wki_version_flags(3,8|1);
  if(field==6)bad.version_flags=wki_version_flags(3,8|2);
  send_ack(bad,-2);assert(!sends&&!signs&&!last_discovery);
 }
 clean();h=ack();h.payload_len=1;send_ack(h,-1);assert(!lookups&&!sends&&!signs);
 clean();h=ack();h.channel_id=1;service=true;send_ack(h,0);assert(!last_discovery);
 clean();h=ack();peers[4].busy=true;send_ack(h,-2);assert(!sends&&!signs&&!lookups);
 clean();h=ack();peers[4].busy=true;peers[4].owned=true;send_ack(h,0);assert(sends==1);
 clean();h=ack();peers[2].busy=true;send_ack(h,-2);assert(!sends&&!signs);
 clean();h=ack();peers[2].auth_session.generation=3;send_ack(h,-2);assert(!sends&&!signs);
 clean();h=ack();discovery=false;send_ack(h,-2);assert(!sends&&!signs);
 clean();h=ack();link.chaos_unregistering=true;send_ack(h,-2);assert(!sends&&!signs);
 clean();h=ack();peers[4].auth_session.active=false;send_ack(h,-1);assert(!sends&&signs==1);
 clean();h=ack();expire_on_auth=true;send_ack(h,-2);assert(!sends&&signs==1);
 clean();h=ack();tx_result=-77;send_ack(h,-77);assert(sends==1);
 clean();h=ack();g_wki.capabilities=WKI_CAP_ROUTING_LEASE;send_ack(h,0);assert(last_discovery);
 // Actual forwarding prefix: preserve authentication trailer and ACK bytes,
 // decrement TTL and use existing pool/current next-hop discovery admission.
 clean();h=ack();auto frame=wire;std::memcpy(frame.data(),&h,32);
 wki_rx(&link,frame.data(),frame.size(),nullptr);assert(sends==1&&signs==0&&claims==1&&last_discovery);
 assert(sent.size()==frame.size());auto expected=h;--expected.hop_ttl;
 assert(std::memcmp(sent.data(),&expected,32)==0&&std::equal(sent.begin()+32,sent.end(),frame.begin()+32));
 for(unsigned fault=0;fault<7;++fault){
  clean();h=ack();
  if(fault==0)h.channel_id=3;
  if(fault==1)h.hop_ttl=1;
  if(fault==2)h.version_flags=wki_version_flags(2,8);
  if(fault==3)pool_available=false;
  if(fault==4)publication=false;
  if(fault==5)h.payload_len=73;
  if(fault==6)h.checksum=1;
  std::memcpy(frame.data(),&h,32);wki_rx(&link,frame.data(),frame.size(),nullptr);assert(!sends&&!signs);
 }
 clean();h=ack();assert(send_routed_frame(4,wire.data(),wire.size())==0&&last_discovery);
 clean();assert(send_routed_frame(4,nullptr,0)==-1&&!sends);
 clean();std::puts("ACTUAL_CONTROL_ACK_DISCOVERY_SEND_FORWARD_LIFETIME_AND_SERVICE_GUARDS_PASS");
}
'''
MATRIX=r'''
void classifier_matrix(){
 for(unsigned type=0;type<256;++type)for(unsigned flags=0;flags<16;++flags)
 for(auto channel: {0,1,2,3,16,240,255,65535})for(unsigned payload=0;payload<2;++payload)
 for(unsigned sequence=0;sequence<2;++sequence)for(unsigned reserved=0;reserved<2;++reserved){
  auto h=ack();h.msg_type=type;h.version_flags=wki_version_flags(3,flags);h.channel_id=channel;
  h.payload_len=payload;h.seq_num=sequence;h.reserved=reserved;
  bool want=type==4&&(flags==8||flags==12)&&channel==0&&!payload&&!sequence&&!reserved;
  assert(routing_control_ack(h)==want);
 }
}
'''

def harness(source):
 wire=(ROOT/'modules/kern/src/net/wki/wire.hpp').read_text();routing=(ROOT/'modules/kern/src/net/wki/routing.hpp').read_text()
 enums=extract(wire,'enum class MsgType : uint8_t {',True)
 layouts=extract(wire,'struct WkiHeader {',True)+extract(routing,'struct RoutingEntry {',True)
 prefix=extract(source,'void wki_rx(').split('    bool const SHARED_RX_LIFECYCLE')[0]+'}\n'
 funcs='\n'.join(extract(source,'auto '+n+'(') for n in ['routing_discovery_message','routing_handshake_message'])
 if 'auto routing_control_ack(' in source:funcs+='\n'+extract(source,'auto routing_control_ack(')
 cases=CASES
 if 'auto routing_control_ack(' in source:
  cases=cases.replace('int main(){',MATRIX+'\nint main(){\n classifier_matrix();')
 return PRELUDE+enums+layouts+STUBS+extract(source,'class ResolvedRoute {',True)+funcs+extract(source,'auto ResolvedRoute::resolve(')+extract(source,'auto wki_send_authenticated_header(')+extract(source,'auto send_routed_frame(uint16_t dst_node, const void* frame, uint16_t len) -> int {')+prefix+cases

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--source',type=Path,default=ROOT/'modules/kern/src/net/wki/wki.cpp');parser.add_argument('--expect-old-failure',action='store_true');parser.add_argument('--out',type=Path)
 a=parser.parse_args();source=a.source.read_text();compiler=shutil.which('clang++',path='/usr/bin') or '/usr/bin/g++'
 with tempfile.TemporaryDirectory(prefix='wki-control-ack-',dir=a.out) as temp:
  d=Path(temp);cpp=d/'test.cpp';cpp.write_text(harness(source));elf=d/'test'
  r=subprocess.run([compiler,'-std=c++23','-O1','-g','-fsanitize=address,undefined','-fno-omit-frame-pointer',str(cpp),'-o',str(elf)],capture_output=True,text=True,env={**os.environ,'TMPDIR':temp})
  assert r.returncode==0,r.stderr
  r=subprocess.run([str(elf)],capture_output=True,text=True,env={**os.environ,'ASAN_OPTIONS':'detect_leaks=0'})
  if a.expect_old_failure:
   assert r.returncode!=0 and 'send_ack' in r.stderr,(r.returncode,r.stdout,r.stderr)
   print('SAVED_OLD_SOURCE_FAILS_BOOTSTRAP_ACK_REGRESSION',flush=True)
  else:
   assert r.returncode==0,(r.returncode,r.stdout,r.stderr);print(r.stdout,end='',flush=True)
if __name__=='__main__':main()
