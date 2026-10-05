"""Execute actual routing/channel snapshot/format/read bodies; other sections modeled."""
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SRC = Path(os.environ.get('WOS_NETDIAG_SOURCE_ROOT', ROOT))
proc = (SRC / 'modules/kern/src/vfs/fs/procfs.cpp').read_text()
wki = (SRC / 'modules/kern/src/net/wki/wki.cpp').read_text()
wh = (SRC / 'modules/kern/src/net/wki/wki.hpp').read_text()
ph = (ROOT / 'modules/kern/src/vfs/fs/procfs.hpp').read_text()
wire = (ROOT / 'modules/kern/src/net/wki/wire.hpp').read_text()
rh = (SRC / 'modules/kern/src/net/wki/routing.hpp').read_text()
routing = (SRC / 'modules/kern/src/net/wki/routing.cpp').read_text()

def block(text, pattern):
    match = re.search(pattern, text, re.M)
    assert match, pattern
    pos = text.index('{', match.start()) + 1
    depth = 1
    while depth:
        depth += (text[pos] == '{') - (text[pos] == '}')
        pos += 1
    return text[match.start():pos]

def function(text, name):
    return block(text, rf'^(?:auto|void) {name}\(')

def definition(text, name):
    content = block(text, rf'^(?:struct|enum class) {name}\b')
    end = text.index(';', text.index(content) + len(content))
    return text[text.index(content):end + 1]

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <ranges>
#include <string>
#include <vector>
#include <sys/types.h>
#include <net/wki/routing_lease.hpp>
unsigned spin_depth = 0, content_depth = 0, generations = 0, snapshots = 0;
size_t fail_size = 0, last_content_alloc = 0;
void* operator new[](size_t n, const std::nothrow_t&) noexcept {
    assert(spin_depth == 0);
    if (n == fail_size) return nullptr;
    if (n >= 262144) last_content_alloc = n;
    return std::malloc(n);
}
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, size_t) noexcept { std::free(p); }
struct Lock { bool held = false; unsigned locks = 0; void lock() { assert(!held); held = true; ++spin_depth; ++locks; }
    void unlock() { assert(held); held = false; --spin_depth; } };
namespace ker::mod::sys {
struct Mutex { bool held = false; };
struct MutexGuard { Mutex& m; explicit MutexGuard(Mutex& v): m(v) { assert(!m.held); m.held=true; ++content_depth; }
 ~MutexGuard() { m.held=false; --content_depth; } };
}
namespace ker::mod::security {
struct Snapshot { bool admitted = true; };
int authorize_diagnostic(Snapshot s, int, bool) { return s.admitted ? 0 : -EACCES; }
}
namespace ker::mod::sched::task { struct Task { uint64_t pid=1; };
uint64_t process_pid(Task& t) { return t.pid; } }
using ProcTask = ker::mod::sched::task::Task;
struct File { void* private_data = nullptr; };
namespace ker::net::wki {
constexpr uint16_t WKI_NODE_INVALID = 0;
constexpr uint32_t WKI_ACK_NONE = UINT32_MAX;
constexpr size_t WKI_HEADER_SIZE = 32;
'''

def harness():
    code = PRELUDE
    pool = re.search(r'^constexpr size_t WKI_CHANNEL_POOL_SIZE = [^;]+;', wh, re.M)
    code += (pool[0] if pool else 'constexpr size_t WKI_CHANNEL_POOL_SIZE = 1024;')
    code += '\n' + re.search(r'^constexpr size_t WKI_CHANNEL_DIAG_MAX = [^;]+;', wh, re.M)[0]
    for text, name in ((wh, 'PeerState'), (wire, 'PriorityClass'), (wire, 'WkiHeader'), (wh, 'WkiChannelDiag')):
        code += '\n' + definition(text, name)
    code += '\n' + re.search(r'^constexpr size_t WKI_MAX_PEERS = [^;]+;', wh, re.M)[0]
    code += '\n' + re.search(r'^constexpr size_t WKI_MAX_NEIGHBORS_PER_LSA = [^;]+;', rh, re.M)[0]
    for text, name in ((wire, 'LsaNeighborEntry'), (rh, 'LsdbEntry'), (rh, 'WkiRoutingOriginDiag')):
        code += '\n' + definition(text, name)
    code += '\nstd::array<LsdbEntry, WKI_MAX_PEERS> s_lsdb{}; Lock s_routing_lock;\n'
    code += function(routing, 'wki_routing_origin_diag_snapshot')
    code += r'''
struct WkiPeer { PeerState state = PeerState::CONNECTED; bool is_direct=true; uint16_t next_hop=0; uint8_t hop_count=1; } peer;
struct Entry { uint32_t seq=UINT32_MAX, retries=UINT32_MAX; uint64_t send_time_us=UINT64_MAX;
 unsigned char* data=nullptr; size_t len=0; uint8_t msg_type=255; };
struct WkiChannel : WkiChannelDiag {
 Lock lock; uint16_t peer_node_id=0; PriorityClass priority = PriorityClass::THROUGHPUT;
 bool retransmit_in_progress=true; uint32_t rto_us=UINT32_MAX, fast_retransmit_seq=UINT32_MAX;
 uint64_t retransmit_deadline=UINT64_MAX; uint8_t dup_ack_count=255;
 Entry* retransmit_head=nullptr; Entry* reorder_head=nullptr;
 std::array<void*,8> rx_dispatch_waiters{};
};
'''
    declaration = re.search(r'^std::array<WkiChannel, \w+> s_channel_pool;', wki, re.M)[0]
    if 'CHANNEL_POOL_SIZE>' in declaration and 'WKI_CHANNEL_POOL_SIZE>' not in declaration:
        code += '\n' + re.search(r'^constexpr size_t CHANNEL_POOL_SIZE = [^;]+;', wki, re.M)[0]
    code += '\n' + declaration + '\nLock s_channel_pool_lock;'
    code += '\nvoid channel_pool_init() { ++snapshots; }\nWkiPeer* wki_peer_find(uint16_t) { assert(s_channel_pool_lock.held); return &peer; }\n'
    code += function(wki, 'wki_channel_diag_snapshot') + '\n}\n'
    code += definition(ph, 'ProcNodeType') + '\n' + definition(ph, 'ProcNode') + '\n' + definition(ph, 'ProcFileData')
    code += '\nProcFileData::~ProcFileData() {}\n'
    net_const = re.search(r'^constexpr size_t MAX_WKI_NETDIAG_BUF = [^;]+;', proc, re.M)
    if net_const: code += '\n' + net_const[0]
    code += '\n' + re.search(r'^constexpr size_t MAX_WKI_NAPI_BUF = [^;]+;', proc, re.M)[0]
    for name in ('append_char', 'append_dec64', 'append_sconst', 'append_bool01', 'append_hex16', 'peer_state_name', 'priority_name'):
        code += '\n' + function(proc, name)
    aggregate = function(proc, 'generate_wki_netdiag')
    head = aggregate[:aggregate.index('    auto pool =')]
    # Other subsystem snapshot callbacks are a modeled 150KiB well-formed prefix.
    # Real routing/channel snapshots, formatters, allocation branches and footer
    # remain byte-for-byte source; other subsystem formatters are not executed.
    origin_section = aggregate[aggregate.index('    auto* origins ='):aggregate.index('    auto* routes =')]
    tail = aggregate[aggregate.index('    auto* channels ='):]
    code += '\nstd::string prefix;\n' + head + '''
    ++generations;
    append_sconst(p, end, prefix.c_str());
''' + origin_section + tail
    dispatcher = function(proc, 'procfs_read_with_credentials')
    generated = set(re.findall(r'\b(generate_\w+)\(', dispatcher)) - {'generate_wki_netdiag'}
    for name in sorted(generated): code += f'\ntemplate<class... T> size_t {name}(T...) {{ return 0; }}'
    code += r'''
int diagnostic_access(ProcNodeType) { return 0; }
bool private_procfs_node(ProcNodeType) { return false; }
template<class... T> ssize_t procfs_read_mounts_as(T...) { assert(false); return -1; }
template<class... T> ssize_t procfs_read_private(T&&...) { assert(false); return -1; }
template<class... T> ssize_t procfs_read_limits(T&&...) { assert(false); return -1; }
'''
    code += '\nnamespace ker::vfs { template<class... T> size_t vfs_generate_local_pipe_diag(T...) { return 0; } }\nnamespace ker::net::wki { template<class... T> size_t wki_chaos_workload_snapshot(T...) { return 0; } }\n'
    code += '\n' + dispatcher + '\n' + function(proc, 'procfs_close')
    code += CASES
    return code

CASES = r'''
size_t rows(const std::string& s) {
 size_t count=0, at=0; while ((at=s.find("wki_channel peer=", at)) != std::string::npos) { ++count; ++at; } return count;
}
void configure(size_t n) {
 for (size_t i=0; i<ker::net::wki::s_channel_pool.size(); ++i) {
  auto& ch=ker::net::wki::s_channel_pool[i]; ch.active=i<n; ch.peer_node_id=uint16_t(1+i/256); ch.channel_id=uint16_t(i%256);
  ch.generation=ch.tx_seq=ch.tx_ack=ch.rx_seq=ch.rx_ack_pending=UINT32_MAX;
  ch.tx_credits=ch.rx_credits=UINT16_MAX; ch.retransmit_count=ch.reorder_count=ch.retransmits=UINT32_MAX;
  ch.bytes_sent=ch.bytes_received=UINT64_MAX;
 }
 for (size_t i=0; i<ker::net::wki::s_lsdb.size(); ++i) {
  auto& e=ker::net::wki::s_lsdb[i]; e={}; e.origin_node=uint16_t(i+1);
  e.lsa_seq=e.authenticated_generation=e.authenticated_identity.boot_epoch=e.hint_sequence=UINT32_MAX;
  e.origin_replay_generation=e.lease.session_generation=UINT32_MAX;
  e.authenticated_capabilities=UINT16_MAX; e.authenticated_identity.nonce.fill(255);
  e.received_time_us=e.revision=e.hint_received_time_us=e.lease.lease_expires_us=UINT64_MAX;
  e.valid=false; e.fenced=e.sequence_known=e.hint_valid=e.discovery_expired=true;
  e.origin_replay_pending=e.origin_replay_retry=e.lease.bound=e.lease.extended_required=e.lease.withdrawn=true;
  e.num_neighbors=32; for(auto& neighbor:e.neighbors) neighbor={65534,65535,65535};
 }
 prefix.clear(); for (unsigned i=0; i<1500; ++i) prefix += "model_other_snapshot field=" + std::string(73,'x') + "\n";
}
size_t origin_rows(const std::string& s) {
 size_t count=0, at=0; while ((at=s.find("wki_routing_origin version=1 origin=",at))!=std::string::npos) {++count; ++at;} return count;
}
void origin_snapshot() {
 using namespace ker::net::wki;
 configure(0);
 auto saved=s_lsdb; std::array<WkiRoutingOriginDiag,WKI_MAX_PEERS+1> output{};
 output.back().origin_node=65534;
 auto locks=s_routing_lock.locks;
 assert(wki_routing_origin_diag_snapshot(nullptr,SIZE_MAX)==0);
 assert(wki_routing_origin_diag_snapshot(output.data(),0)==0 && s_routing_lock.locks==locks);
 for (size_t cap:{size_t(1),size_t(255),size_t(256),size_t(257),SIZE_MAX}) {
  auto count=wki_routing_origin_diag_snapshot(output.data(),cap);
  assert(count==std::min(cap,WKI_MAX_PEERS) && s_routing_lock.locks==++locks && !spin_depth);
  assert(output.back().origin_node==65534);
  for(size_t i=0;i<count;++i) {
   auto const& row=output[i]; assert(row.origin_node==i+1);
   assert(!row.valid && row.fenced && row.sequence_known && row.lsa_seq==UINT32_MAX);
   assert(row.authenticated_generation==UINT32_MAX && row.authenticated_boot_epoch==UINT32_MAX);
   assert(row.authenticated_capabilities==UINT16_MAX && row.authenticated_identity_valid);
   assert(row.received_time_us==UINT64_MAX && row.revision==UINT64_MAX);
   assert(row.hint_valid && row.hint_sequence==UINT32_MAX && row.hint_received_time_us==UINT64_MAX && row.discovery_expired);
   assert(row.origin_replay_generation==UINT32_MAX && row.origin_replay_pending && row.origin_replay_retry);
   assert(row.lease_session_generation==UINT32_MAX && row.lease_bound && row.lease_extended_required && row.lease_withdrawn);
   assert(row.lease_expires_us==UINT64_MAX && row.num_neighbors==32);
   for(auto const& neighbor:row.neighbors) assert(neighbor.node_id==65534 && neighbor.link_cost==65535 && neighbor.transport_mtu==65535);
  }
 }
 assert(std::memcmp(saved.data(),s_lsdb.data(),sizeof(s_lsdb))==0);
 s_lsdb[0]={}; s_lsdb[254]={}; s_lsdb[255].authenticated_identity={};
 auto count=wki_routing_origin_diag_snapshot(output.data(),output.size());
 assert(count==254 && output[0].origin_node==2 && output[count-1].origin_node==256);
 assert(!output[count-1].authenticated_identity_valid && output[count-1].authenticated_boot_epoch==0);
 s_lsdb={}; assert(wki_routing_origin_diag_snapshot(output.data(),output.size())==0 && !spin_depth);
}
std::string render(size_t cap) {
 std::vector<char> buffer(cap+2,'!'); size_t length=generate_wki_netdiag(buffer.data()+1,cap);
 assert(buffer.front()=='!' && buffer.back()=='!' && (cap==0 || length<cap));
 if (cap) assert(buffer[length+1]=='\0');
 return {buffer.data()+1,length};
}
void direct() {
 for (size_t n: {size_t(0),size_t(1),size_t(256),size_t(257),size_t(512),size_t(1024)}) {
  configure(n); auto s=render(4*1024*1024);
  assert(rows(s)==n); assert(s.ends_with("wki_netdiag_end complete=1\n"));
  assert(origin_rows(s)==256);
  assert(s.find("valid=0 fenced=1 sequence_known=1 sequence=4294967295")!=std::string::npos);
  assert(s.find("completed_generation=4294967295 completed_boot_epoch=4294967295 completed_capabilities=65535")!=std::string::npos);
  assert(s.find("neighbor_count=32 neighbors=0xfffe:65535:65535")!=std::string::npos);
  assert(s.find("wki_channel_truncated")==std::string::npos);
  if (n) assert(s.find("bytes_tx=18446744073709551615 bytes_rx=18446744073709551615")!=std::string::npos);
 }
 configure(1024);
 for (size_t cap: {size_t(0),size_t(1),size_t(2),size_t(19),size_t(26),size_t(4096),size_t(262144)}) {
  auto s=render(cap); assert(!s.ends_with("wki_netdiag_end complete=1\n"));
 }
 fail_size=ker::net::wki::WKI_CHANNEL_POOL_SIZE*sizeof(ker::net::wki::WkiChannelDiag);
 auto s=render(4*1024*1024); assert(s.find("wki_channel allocation_failed=1")!=std::string::npos);
 assert(!s.ends_with("wki_netdiag_end complete=1\n")); fail_size=0;
 fail_size=ker::net::wki::WKI_MAX_PEERS*sizeof(ker::net::wki::WkiRoutingOriginDiag);
 s=render(4*1024*1024); assert(s.find("wki_routing_origin allocation_failed=1")!=std::string::npos);
 assert(!s.ends_with("wki_netdiag_end complete=1\n")); fail_size=0;
 assert(!spin_depth);
}
void read_cache() {
 configure(1024); generations=0; snapshots=0; last_content_alloc=0;
 auto* data=new ProcFileData{}; data->node.type=ProcNodeType::WKI_NETDIAG_FILE;
 File file{data}; ker::mod::security::Snapshot allow{}, deny{false};
 std::array<char,4096> out{}; out.fill('S');
 assert(procfs_read_with_credentials(deny,&file,out.data(),out.size(),0)==-EACCES);
 assert(out.front()=='S' && !data->content && !generations && !snapshots);
 fail_size=4*1024*1024;
 assert(procfs_read_with_credentials(allow,&file,out.data(),out.size(),0)==-ENOMEM);
 assert(!data->content && out.front()=='S' && !generations && !content_depth);
 fail_size=0;
 assert(procfs_read_with_credentials(allow,&file,out.data(),0,0)==0);
 assert(data->content && generations==1 && last_content_alloc==4*1024*1024);
 auto expected=std::string(data->content,data->content_len); assert(rows(expected)==1024);
 assert(origin_rows(expected)==256);
 assert(expected.ends_with("wki_netdiag_end complete=1\n"));
 configure(0); std::string reconstructed;
 for (size_t at=0; at<expected.size();) {
  size_t request=1+at%out.size(); auto n=procfs_read_with_credentials(allow,&file,out.data(),request,at);
  assert(n>0 && size_t(n)<=request); reconstructed.append(out.data(),size_t(n)); at+=size_t(n);
 }
 assert(reconstructed==expected && generations==1 && !content_depth);
 for (size_t at: {size_t(0),expected.size(),std::numeric_limits<size_t>::max()}) {
  out.fill('S'); assert(procfs_read_with_credentials(deny,&file,out.data(),0,at)==-EACCES);
  assert(procfs_read_with_credentials(deny,&file,out.data(),out.size(),at)==-EACCES && out.front()=='S');
 }
 assert(procfs_read_with_credentials(allow,&file,out.data(),out.size(),expected.size())==0);
 assert(procfs_close(&file)==0 && !file.private_data && !content_depth && !spin_depth);
}
int main() { static_assert(ker::net::wki::WKI_CHANNEL_DIAG_MAX==256); origin_snapshot(); direct(); read_cache(); puts("actual routing/channel snapshots and renderers/procfs read/cache/authorization/OOM/close PASS; other formatters modeled"); }
'''

def main():
    compiler = shutil.which('clang++', path=os.defpath)
    assert compiler
    code = harness()
    variants = [('candidate', code, None)]
    for label, before, after, expected in (
        ('snapshot_skip_final', '!pool_entry.active || count >= max', '!pool_entry.active || count + 1 >= max', 'rows(s)==n'),
        ('renderer_omit_bytes', 'append_dec64(p, end, ch.bytes_received);', 'append_dec64(p, end, 0);', 's.find('),
        ('cached_authority_bypass', 'if (POLICY != 0) return POLICY;',
            'if (POLICY != 0 && pfd->content == nullptr) return POLICY;', 'procfs_read_with_credentials(deny,'),
        ('oom_success', 'return -ENOMEM;', 'return 0;', 'procfs_read_with_credentials(allow,'),
        ('regenerate_cached', '// Lazily generate content',
            'if (pfd->content != nullptr) { delete[] pfd->content; pfd->content = nullptr; }\n// Lazily generate content',
            'n>0 && size_t(n)<=request'),
        ('allocation_small', 'alloc_sz = MAX_WKI_NETDIAG_BUF;', 'alloc_sz = 262144;', 'heap-buffer-overflow'),
        ('format_small', 'generate_wki_netdiag(pfd->content, MAX_WKI_NETDIAG_BUF)',
            'generate_wki_netdiag(pfd->content, 262144)', 'rows(expected)==1024'),
    ):
        assert code.count(before) == 1, label
        variants.append((label, code.replace(before, after), expected))
    snapshot = function(routing, 'wki_routing_origin_diag_snapshot')
    for label, before, after, expected in (
        ('origin_skip_tombstone', 'entry.origin_node == WKI_NODE_INVALID',
            'entry.origin_node == WKI_NODE_INVALID || entry.fenced', 'count==std::min('),
        ('origin_skip_final', 'count >= max', 'count + 1 >= max', 'count==std::min('),
        ('origin_generation_erased', '.authenticated_generation = entry.authenticated_generation',
            '.authenticated_generation = 0', 'row.authenticated_generation=='),
        ('origin_aliases_sequence', '.hint_sequence = entry.hint_sequence',
            '.hint_sequence = 0', 'row.hint_valid && row.hint_sequence=='),
    ):
        assert snapshot.count(before)==1, label
        variants.append((label,code.replace(snapshot,snapshot.replace(before,after)),expected))
    with tempfile.TemporaryDirectory(prefix='wos-netdiag-proof-') as directory:
        source, binary = Path(directory)/'proof.cpp', Path(directory)/'proof'
        for label, content, expected in variants:
            source.write_text(content)
            subprocess.run([compiler, '-std=c++23', '-Wall', '-Wextra', '-Werror', '-g',
                '-fsanitize=address,undefined', '-I', str(ROOT/'modules/kern/src'), str(source), '-o', str(binary)], check=True)
            run = subprocess.run([str(binary)], capture_output=True, text=True,
                env={**os.environ, 'ASAN_OPTIONS':'detect_leaks=0'})
            if expected is None:
                assert run.returncode == 0, run.stdout + run.stderr
                print(run.stdout.strip())
            else:
                assert run.returncode != 0 and expected in run.stderr, (label, run.returncode, run.stderr)
        print('Eleven independent routing/channel snapshot/field/authority/cache/OOM/capacity mutants rejected')

if __name__ == '__main__': main()
