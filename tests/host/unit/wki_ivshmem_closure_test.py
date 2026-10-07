#!/usr/bin/env python3
"""Run the entire actual ivshmem transport TU against instrumented boundaries.

The transport body, ring algorithms, frame constants, and transport declaration
come from the source tree. PCI/MMIO, packet allocation, registry, shutdown hold,
and firmware/time are host seams; hardware and common RX authentication are not
claimed. --source permits behavioral controls from a retained older source.
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
SOURCE = KERN / "net/wki/transport_ivshmem.cpp"


PRELUDE = r"""
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <string_view>
#include <vector>
#include <dev/ivshmem/shared_ring.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/rdma_protocol.hpp>
#include <net/wki/transport_ivshmem.hpp>
#define WOS_SELFTEST 1
#define CHECK(expr) do { if (!(expr)) { std::fprintf(stderr, "CHECK failed: %s:%d: %s\n", __FILE__, __LINE__, #expr); std::abort(); } } while (0)
namespace fixture {
struct Counts {
    uint32_t guards=0, held=0, shutdown_holds=0, rings=0, masks=0,
             irq_callbacks=0, registrations=0, unregistrations=0, logs=0,
             packets=0, frees=0, spin_locks=0, closes=0, secure_ingress=0;
    bool operator==(const Counts&) const = default;
} counts;
std::vector<uint8_t> backing(16 * 1024 * 1024);
std::array<uint32_t, 4> registers{};
uint32_t* published_head=nullptr;
uint32_t expected_head=0;
bool assert_published=false;
bool preparation_ok=true, activation_ok=true;
bool check_secure=false;
const char* direct_peer_value="2";
std::vector<uint8_t> secure_expected;
uint64_t clock_us=0;
}
namespace ker::mod::sys {
struct Spinlock {
    auto lock_irqsave() -> uint64_t { ++fixture::counts.spin_locks; return 0; }
    void unlock_irqrestore(uint64_t) {}
    void lock() { ++fixture::counts.spin_locks; }
    void unlock() {}
};
}
namespace ker::dev::pci {
struct PCIDevice { uint16_t vendor_id=0x1af4, device_id=0x1110; } device;
auto pci_device_count() -> size_t { return 1; }
auto pci_get_device(size_t n) -> PCIDevice* { return n == 0 ? &device : nullptr; }
}
namespace ker::dev::ivshmem {
constexpr uint16_t IVSHMEM_VENDOR=0x1af4, IVSHMEM_DEVICE=0x1110;
auto ivshmem_net_is_claimed(pci::PCIDevice*) -> bool { return false; }
auto pci_selftest_reserved() -> bool { return false; }
struct CallbackState {
    bool is_closed=false;
    auto closed() const -> bool { return is_closed; }
};
struct PciResources {
    CallbackState callbacks;
    pci::PCIDevice* device=nullptr;
    volatile uint32_t* regs=nullptr;
    uint8_t* shared=nullptr;
    size_t shared_size=0;
    std::atomic<uint32_t> test_irqs{0};
    auto prepare(pci::PCIDevice* d, size_t size) -> bool {
        if (!fixture::preparation_ok) return false;
        device=d; regs=fixture::registers.data(); shared=fixture::backing.data();
        shared_size=fixture::backing.size(); CHECK(size <= shared_size); return true;
    }
    auto activate(void(*)(uint8_t,void*),void*,const char*) -> bool { return fixture::activation_ok; }
    void ring(uint32_t peer) {
        CHECK(peer <= 1); ++fixture::counts.rings;
        if (fixture::assert_published) CHECK(*fixture::published_head == fixture::expected_head);
    }
    void mask_vector(bool) { ++fixture::counts.masks; }
    auto retire(void(*unpublish)(void*),void* arg,bool(*drained)(void*)) -> int {
        callbacks.is_closed=true;
        CHECK(fixture::counts.held == 0);
        if (!drained(arg)) return -1;
        unpublish(arg); return 0;
    }
};
class CallbackGuard {
    bool admitted_;
  public:
    explicit CallbackGuard(PciResources& r):admitted_(!r.callbacks.closed()) {
        ++fixture::counts.guards; ++fixture::counts.shutdown_holds;
        if (admitted_) ++fixture::counts.held;
    }
    ~CallbackGuard() { if (admitted_) --fixture::counts.held; }
    explicit operator bool() const { return admitted_; }
};
void selftest_start(PciResources*,const char*,int(*)(),bool(*)(),void(*)(bool)) {}
}
namespace ker::platform::fw {
auto fw_cfg_read_file(const char* key,void* out,size_t size) -> int {
    const char* value=std::strcmp(key,"opt/wos/wki-ivshmem-role")==0 ? "1" : fixture::direct_peer_value;
    auto len=std::strlen(value); CHECK(len <= size); std::memcpy(out,value,len); return static_cast<int>(len);
}
}
namespace ker::mod::time { auto get_us() -> uint64_t { return fixture::clock_us++; } }
namespace ker::mod::dbg { template<class... A> void log(const char*,A...) { ++fixture::counts.logs; } }
namespace ker::net {
constexpr size_t PKT_BUF_SIZE=10240;
struct PacketBuffer {
    std::array<uint8_t, PKT_BUF_SIZE> storage{};
    uint8_t* data=storage.data();
    size_t len=0;
    auto headroom() const -> size_t { return static_cast<size_t>(data-storage.data()); }
};
auto pkt_alloc_tx() -> PacketBuffer* { ++fixture::counts.packets; return new PacketBuffer; }
void pkt_free(PacketBuffer* p) { if (p) { ++fixture::counts.frees; delete p; } }
auto pkt_pool_free_count() -> uint32_t { return 1024-fixture::counts.packets+fixture::counts.frees; }
namespace proto { struct MacAddress { std::array<uint8_t,6> bytes{}; }; }
}
namespace ker::net::wki {
/* actual wire constants and transport declaration inserted here */
"""

BOUNDARIES = r"""
struct WkiState { ker::mod::sys::Spinlock transport_lock; } g_wki;
void wki_transport_register(WkiTransport*) { ++fixture::counts.registrations; }
void wki_transport_unregister(WkiTransport*) { ++fixture::counts.unregistrations; }
void wki_chaos_transport_close(WkiTransport* t) { t->chaos_unregistering.store(true); ++fixture::counts.closes; }
void wki_irq_fwd_doorbell_rx(uint16_t,uint16_t,uint16_t,uint32_t) { ++fixture::counts.irq_callbacks; }
void wki_chaos_secure_rx_ingress(WkiTransport*, const void* data, uint16_t len, const WkiRxMetadata* meta) {
    ++fixture::counts.secure_ingress;
    if (!fixture::check_secure) return;
    CHECK(fixture::counts.held >= 1 && meta && meta->has_direct_peer && meta->direct_peer==2 && !meta->has_src_mac);
    auto addr=reinterpret_cast<uintptr_t>(data), bar=reinterpret_cast<uintptr_t>(fixture::backing.data());
    CHECK(addr < bar || addr >= bar+fixture::backing.size());
    CHECK(len==fixture::secure_expected.size() && std::memcmp(data,fixture::secure_expected.data(),len)==0);
    std::fill(fixture::backing.begin()+64,fixture::backing.end(),0xd9);
    CHECK(std::memcmp(data,fixture::secure_expected.data(),len)==0);
}
}
"""

CASES = r"""
using namespace ker::net::wki;
namespace {
uint32_t delivered=0;
std::vector<uint8_t> expected;
void handler(WkiTransport*, const void* data, uint16_t len, const WkiRxMetadata* meta) {
    CHECK(fixture::counts.held >= 1);
    CHECK(meta && meta->has_direct_peer && meta->direct_peer==2);
    auto* bytes=static_cast<const uint8_t*>(data);
    auto addr=reinterpret_cast<uintptr_t>(bytes), bar=reinterpret_cast<uintptr_t>(fixture::backing.data());
    CHECK(addr < bar || addr >= bar+fixture::backing.size());
    CHECK(len==expected.size() && std::memcmp(bytes,expected.data(),len)==0);
    std::fill(fixture::backing.begin()+64,fixture::backing.begin()+64+65536,0xd9);
    // The complete private frame must remain stable after the shared payload changes.
    CHECK(std::memcmp(bytes,expected.data(),len)==0);
    ++delivered;
}
void reset() {
    CHECK(fixture::counts.held==0);
    std::destroy_at(&s_ivshmem_priv); std::construct_at(&s_ivshmem_priv);
    std::destroy_at(&s_ivshmem_transport); std::construct_at(&s_ivshmem_transport);
    s_ivshmem_initialized=false;
    fixture::counts={}; fixture::assert_published=false;
    fixture::preparation_ok=true; fixture::activation_ok=true; fixture::clock_us=0;
    fixture::check_secure=false; fixture::secure_expected.clear(); fixture::direct_peer_value="2";
    std::fill(fixture::backing.begin(),fixture::backing.end(),0);
    delivered=0; expected.clear();
}
void initialize() {
    reset(); wki_ivshmem_transport_init(); CHECK(s_ivshmem_initialized.load());
    CHECK(fixture::counts.registrations==1 && s_ivshmem_priv.my_vm_id==1);
}
void raw() {
    initialize();
    auto before=fixture::backing; auto counts=fixture::counts;
    uint32_t key=0x19283746;
    std::array<uint8_t,32> out; out.fill(0x57); auto original=out;
    CHECK(s_ivshmem_transport.rdma_register_region(&s_ivshmem_transport,0,4096,&key)==-1);
    CHECK(key==0x19283746);
    CHECK(s_ivshmem_transport.rdma_read(&s_ivshmem_transport,2,4096,0,out.data(),out.size())==-1);
    CHECK(s_ivshmem_transport.rdma_write(&s_ivshmem_transport,2,4096,0,out.data(),out.size())==-1);
    CHECK(out==original);
    for (auto size : {0U,1U,4096U,UINT32_MAX}) CHECK(wki_ivshmem_rdma_alloc(size)==-1);
    for (auto offset : {INT64_MIN,int64_t{-1},int64_t{0},int64_t{4096},INT64_MAX}) {
        CHECK(wki_ivshmem_rdma_ptr(offset)==nullptr); wki_ivshmem_rdma_free(offset,UINT32_MAX);
    }
    wki_ivshmem_irq_mailbox_write(&s_ivshmem_transport,UINT16_MAX,UINT16_MAX,UINT32_MAX);
    CHECK(fixture::backing==before && fixture::counts==counts);
    // Rejections must not inspect self, key/output, or data pointers.
    auto* poison=reinterpret_cast<WkiTransport*>(uintptr_t{1});
    CHECK(ivshmem_wki_rdma_register_region(poison,UINT64_MAX,UINT32_MAX,reinterpret_cast<uint32_t*>(uintptr_t{1}))==-1);
    CHECK(ivshmem_wki_rdma_register_region(nullptr,0,0,nullptr)==-1);
    CHECK(ivshmem_wki_rdma_read(poison,UINT16_MAX,UINT32_MAX,UINT64_MAX,reinterpret_cast<void*>(uintptr_t{1}),UINT32_MAX)==-1);
    CHECK(ivshmem_wki_rdma_write(nullptr,0,0,0,nullptr,0)==-1);
    for (auto key_arg : {0U,4096U,UINT32_MAX}) {
        for (auto offset : {uint64_t{0},UINT64_MAX}) {
            CHECK(ivshmem_wki_rdma_read(&s_ivshmem_transport,2,key_arg,offset,out.data(),UINT32_MAX)==-1);
            CHECK(ivshmem_wki_rdma_write(&s_ivshmem_transport,2,key_arg,offset,out.data(),UINT32_MAX)==-1);
        }
    }
    wki_ivshmem_irq_mailbox_write(poison,1,2,3);
    CHECK(fixture::backing==before && fixture::counts==counts);
}
void capability() { initialize(); CHECK(!s_ivshmem_transport.rdma_capable); CHECK(!s_ivshmem_transport.rdma_unregister_region); }
void doorbell() {
    initialize(); auto before=fixture::counts;
    CHECK(s_ivshmem_transport.doorbell(&s_ivshmem_transport,2,123)==-1);
    CHECK(ivshmem_wki_doorbell(nullptr,UINT16_MAX,UINT32_MAX)==-1);
    CHECK(fixture::counts==before);
}
void mailbox() {
    initialize();
    // Exact legacy packed slot layout, forged solely by the peer.
    std::array<uint8_t,24> forged{};
    uint16_t device=17, vector=41; uint32_t status=0x12345678,pending=1;
    std::memcpy(forged.data(),&device,2); std::memcpy(forged.data()+2,&vector,2);
    std::memcpy(forged.data()+4,&status,4); std::memcpy(forged.data()+8,&pending,4);
    std::memcpy(fixture::backing.data()+40,forged.data(),24);
    CHECK(drain_rx_ring(&s_ivshmem_priv,4)==0);
    CHECK(fixture::counts.irq_callbacks==0);
    CHECK(std::memcmp(fixture::backing.data()+40,forged.data(),24)==0);
    expected.assign(128,0x8d);
    CHECK(shared_ring::write(s_ivshmem_priv.rx_ring,s_ivshmem_priv.rx_ring.size,std::span<const uint8_t>(expected)).error==shared_ring::Error::none);
    ivshmem_wki_set_rx_handler(&s_ivshmem_transport,handler);
    CHECK(drain_rx_ring(&s_ivshmem_priv,1)==1 && delivered==1);
    CHECK(fixture::counts.irq_callbacks==0 && std::memcmp(fixture::backing.data()+40,forged.data(),24)==0);
}
void ordinary() {
    CHECK(wki_ivshmem_selftest_complete_frames());
    for (auto length : {size_t{1},size_t{8192},size_t{8193},WKI_MAX_FRAME_SIZE}) {
        for (bool wrapped : {false,true}) {
            initialize(); expected.resize(length);
            for (size_t i=0;i<length;++i) expected[i]=static_cast<uint8_t>(i*17+3);
            auto& rx=s_ivshmem_priv.rx_ring;
            auto start=wrapped ? rx.size-4 : 0;
            shared_ring::store_word(rx.head_ptr,start); shared_ring::store_word(rx.tail_ptr,start);
            CHECK(shared_ring::write(rx,rx.size,std::span<const uint8_t>(expected)).error==shared_ring::Error::none);
            ivshmem_wki_set_rx_handler(&s_ivshmem_transport,handler);
            CHECK(wki_ivshmem_transport_poll(1)==1 && delivered==1);
            CHECK(fixture::counts.irq_callbacks==0 && fixture::counts.held==0);
        }
    }
}
void tx() {
    initialize(); std::array<uint8_t,17> bytes{}; bytes.fill(0x39);
    auto& ring=s_ivshmem_priv.tx_ring;
    fixture::published_head=ring.head_ptr; fixture::expected_head=20; fixture::assert_published=true;
    CHECK(ivshmem_wki_tx(&s_ivshmem_transport,2,bytes.data(),bytes.size())==0);
    CHECK(fixture::counts.rings==1 && shared_ring::load_word(ring.head_ptr)==20);
    std::array<uint8_t,32> received{};
    CHECK(shared_ring::read(ring,ring.size,std::span(received)).bytes==bytes.size());
    CHECK(std::memcmp(received.data(),bytes.data(),bytes.size())==0);
    auto before=fixture::counts.rings;
    CHECK(ivshmem_wki_tx(&s_ivshmem_transport,2,nullptr,1)==-1);
    CHECK(ivshmem_wki_tx(&s_ivshmem_transport,2,bytes.data(),0)==-1);
    CHECK(fixture::counts.rings==before);
    s_ivshmem_priv.tx_active.test_and_set();
    CHECK(ivshmem_wki_tx(&s_ivshmem_transport,2,bytes.data(),1)==-1);
    s_ivshmem_priv.tx_active.clear(); CHECK(fixture::counts.rings==before);
}
void budget() {
    initialize(); std::array<uint8_t,64> data{};
    auto& ring=s_ivshmem_priv.rx_ring;
    for (int i=0;i<3;++i) CHECK(shared_ring::write(ring,ring.size,std::span<const uint8_t>(data)).error==shared_ring::Error::none);
    auto tail=shared_ring::load_word(ring.tail_ptr);
    CHECK(drain_rx_ring(&s_ivshmem_priv,0)==0 && shared_ring::load_word(ring.tail_ptr)==tail);
    s_ivshmem_priv.rx_drain_active.test_and_set();
    CHECK(drain_rx_ring(&s_ivshmem_priv,4)==0 && shared_ring::load_word(ring.tail_ptr)==tail);
    s_ivshmem_priv.rx_drain_active.clear();
    CHECK(drain_rx_ring(&s_ivshmem_priv,2)==2); CHECK(drain_rx_ring(&s_ivshmem_priv,2)==1);
    CHECK(drain_rx_ring(&s_ivshmem_priv,2)==0 && fixture::counts.held==0);
}
void malformed() {
    initialize(); auto& tx=s_ivshmem_priv.tx_ring;
    shared_ring::store_word(tx.head_ptr,tx.size);
    uint8_t byte=1; CHECK(ivshmem_wki_tx(&s_ivshmem_transport,2,&byte,1)==-1);
    CHECK(s_ivshmem_priv.ring_failed.load() && fixture::counts.rings==0 && fixture::counts.masks==1);
    initialize(); auto& rx=s_ivshmem_priv.rx_ring;
    shared_ring::store_word(rx.head_ptr,rx.size);
    CHECK(drain_rx_ring(&s_ivshmem_priv,3)==0 && s_ivshmem_priv.ring_failed.load());
    CHECK(fixture::counts.irq_callbacks==0);
    initialize(); auto& incomplete=s_ivshmem_priv.rx_ring;
    incomplete.data[0]=100; incomplete.data[1]=0; shared_ring::store_word(incomplete.head_ptr,4);
    CHECK(drain_rx_ring(&s_ivshmem_priv,1)==0 && s_ivshmem_priv.ring_failed.load());
}
void irq() {
    initialize(); expected.assign(1,0x37);
    CHECK(shared_ring::write(s_ivshmem_priv.rx_ring,s_ivshmem_priv.rx_ring.size,std::span<const uint8_t>(expected)).error==shared_ring::Error::none);
    ivshmem_wki_set_rx_handler(&s_ivshmem_transport,handler);
    ivshmem_wki_irq(0,&s_ivshmem_priv);
    CHECK(delivered==0 && fixture::counts.masks==1 && s_ivshmem_priv.resources.test_irqs==1);
    CHECK(wki_ivshmem_transport_poll(1)==1 && delivered==1);
}
void geometry() {
    initialize();
    auto tx=s_ivshmem_priv.tx_ring, rx=s_ivshmem_priv.rx_ring;
    auto* header=reinterpret_cast<WkiIvshmemHeader*>(fixture::backing.data());
    header->ring0_offset=UINT32_MAX; header->ring0_size=UINT32_MAX;
    header->ring1_offset=0; header->ring1_size=0;
    header->rdma_offset=0; header->rdma_size=UINT32_MAX;
    // After admission, hostile header edits cannot redirect either ring.
    expected.assign(64,0x91);
    CHECK(shared_ring::write(rx,rx.size,std::span<const uint8_t>(expected)).error==shared_ring::Error::none);
    ivshmem_wki_set_rx_handler(&s_ivshmem_transport,handler);
    CHECK(drain_rx_ring(&s_ivshmem_priv,1)==1 && delivered==1);
    CHECK(ivshmem_wki_tx(&s_ivshmem_transport,2,expected.data(),expected.size())==0);
    std::array<uint8_t,64> read{};
    CHECK(shared_ring::read(tx,tx.size,std::span(read)).bytes==read.size());
    CHECK(std::memcmp(read.data(),expected.data(),read.size())==0);
    CHECK(s_ivshmem_priv.tx_ring.head_ptr==tx.head_ptr && s_ivshmem_priv.rx_ring.head_ptr==rx.head_ptr);
}
void packets() {
    initialize(); fixture::assert_published=false;
    auto* packet=ker::net::pkt_alloc_tx(); packet->len=32;
    CHECK(ivshmem_wki_tx_pkt(&s_ivshmem_transport,2,packet)==0);
    packet=ker::net::pkt_alloc_tx(); packet->data+=16; packet->len=ker::net::PKT_BUF_SIZE;
    CHECK(ivshmem_wki_tx_pkt(&s_ivshmem_transport,2,packet)==-1);
    CHECK(ivshmem_wki_tx_pkt(&s_ivshmem_transport,2,nullptr)==-1);
    auto transport=WkiTransport{};
    packet=ker::net::pkt_alloc_tx(); CHECK(ivshmem_wki_tx_pkt(&transport,2,packet)==-1);
    CHECK(wki_ivshmem_transport_shutdown()==0);
    packet=ker::net::pkt_alloc_tx(); CHECK(ivshmem_wki_tx_pkt(&s_ivshmem_transport,2,packet)==-1);
    CHECK(fixture::counts.packets==fixture::counts.frees && fixture::counts.held==0);
}
void shutdown() {
    initialize(); auto* pinned=s_ivshmem_priv.shmem;
    s_ivshmem_transport.chaos_in_flight=1;
    CHECK(wki_ivshmem_transport_shutdown()==-1 && s_ivshmem_initialized.load());
    CHECK(s_ivshmem_priv.shmem==pinned && fixture::counts.unregistrations==0);
    s_ivshmem_transport.chaos_in_flight=0;
    CHECK(wki_ivshmem_transport_shutdown()==0 && !s_ivshmem_initialized.load());
    CHECK(fixture::counts.unregistrations==1 && s_ivshmem_priv.shmem==pinned);
    CHECK(selftest_late_calls());
    auto guards=fixture::counts.guards;
    CHECK(wki_ivshmem_rdma_alloc(UINT32_MAX)==-1 && !wki_ivshmem_rdma_ptr(INT64_MIN));
    wki_ivshmem_rdma_free(INT64_MAX,UINT32_MAX); wki_ivshmem_irq_mailbox_write(nullptr,1,2,3);
    CHECK(guards==fixture::counts.guards);
    CHECK(fixture::counts.packets==fixture::counts.frees);
}
std::vector<uint8_t> secure_record(size_t length=rdma_protocol::RECORD_BYTES) {
    std::vector<uint8_t> bytes(length,0x59);
    uint32_t magic=rdma_protocol::MAGIC;
    if (length>=4) std::memcpy(bytes.data(),&magic,4);
    return bytes;
}
void secure_tx() {
    initialize();
    CHECK(s_ivshmem_transport.secure_kind==rdma_protocol::Transport::IVSHMEM && s_ivshmem_transport.secure_tx);
    CHECK(!s_ivshmem_transport.rdma_capable);
    auto& ring=s_ivshmem_priv.tx_ring;
    for (auto length:{size_t{144},size_t{8336}}) {
        auto record=secure_record(length);
        *ring.head_ptr=ring.size-8; *ring.tail_ptr=ring.size-8;
        CHECK(s_ivshmem_transport.secure_tx(&s_ivshmem_transport,2,record.data(),record.size())==0);
        auto original=record; std::fill(record.begin(),record.end(),0);
        std::vector<uint8_t> copy(length);
        CHECK(shared_ring::read(ring,ring.size,std::span(copy)).bytes==length && copy==original);
    }
    auto record=secure_record();
    auto before=fixture::backing; auto rings=fixture::counts.rings;
    for (auto peer:{uint16_t{0},uint16_t{1},uint16_t{3},uint16_t{65535}})
        CHECK(ivshmem_secure_tx(&s_ivshmem_transport,peer,record.data(),record.size())==-1);
    for (auto length:{uint16_t{0},uint16_t{3},uint16_t{143},uint16_t{8337},uint16_t{65535}})
        CHECK(ivshmem_secure_tx(&s_ivshmem_transport,2,record.data(),length)==-1);
    record[0]^=1;
    CHECK(ivshmem_secure_tx(&s_ivshmem_transport,2,record.data(),record.size())==-1);
    CHECK(ivshmem_secure_tx(nullptr,2,record.data(),record.size())==-1);
    CHECK(ivshmem_secure_tx(&s_ivshmem_transport,2,nullptr,144)==-1);
    CHECK(fixture::backing==before && fixture::counts.rings==rings);
    record[0]^=1;
    WkiTransport other{};
    CHECK(ivshmem_secure_tx(&other,2,record.data(),record.size())==-1);
    auto& full=s_ivshmem_priv.tx_ring; *full.head_ptr=4; *full.tail_ptr=8;
    CHECK(ivshmem_secure_tx(&s_ivshmem_transport,2,record.data(),record.size())==-1);
    CHECK(*full.head_ptr==4 && *full.tail_ptr==8 && !s_ivshmem_priv.ring_failed && fixture::counts.rings==rings);
    *full.head_ptr=0; *full.tail_ptr=0; before=fixture::backing;
    s_ivshmem_priv.tx_active.test_and_set();
    CHECK(ivshmem_secure_tx(&s_ivshmem_transport,2,record.data(),record.size())==-1);
    s_ivshmem_priv.tx_active.clear();
    s_ivshmem_priv.ring_failed=true;
    CHECK(ivshmem_secure_tx(&s_ivshmem_transport,2,record.data(),record.size())==-1);
    s_ivshmem_priv.ring_failed=false;
    s_ivshmem_priv.resources.callbacks.is_closed=true;
    CHECK(ivshmem_secure_tx(&s_ivshmem_transport,2,record.data(),record.size())==-1);
    CHECK(fixture::backing==before && fixture::counts.rings==rings && fixture::counts.held==0);
}
void secure_rx() {
    for (auto length:{size_t{144},size_t{8336}}) {
        initialize(); auto& ring=s_ivshmem_priv.rx_ring;
        *ring.head_ptr=ring.size-8; *ring.tail_ptr=ring.size-8;
        ivshmem_wki_set_rx_handler(&s_ivshmem_transport,handler);
        fixture::secure_expected=secure_record(length); fixture::check_secure=true;
        CHECK(shared_ring::write(ring,ring.size,std::span(fixture::secure_expected)).error==shared_ring::Error::none);
        CHECK(drain_rx_ring(&s_ivshmem_priv,1)==1);
        CHECK(fixture::counts.secure_ingress==1 && delivered==0);
    }
    for (auto length:{size_t{4},size_t{143},size_t{8337}}) {
        initialize(); auto& ring=s_ivshmem_priv.rx_ring;
        ivshmem_wki_set_rx_handler(&s_ivshmem_transport,handler);
        auto record=secure_record(length);
        CHECK(shared_ring::write(ring,ring.size,std::span(record)).error==shared_ring::Error::none);
        CHECK(drain_rx_ring(&s_ivshmem_priv,1)==1);
        CHECK(fixture::counts.secure_ingress==0 && delivered==0); // no ordinary fallback
    }
    initialize(); auto oversized=secure_record(9000);
    CHECK(shared_ring::write(s_ivshmem_priv.rx_ring,s_ivshmem_priv.rx_ring.size,std::span(oversized)).error==shared_ring::Error::none);
    CHECK(drain_rx_ring(&s_ivshmem_priv,1)==0 && s_ivshmem_priv.ring_failed && fixture::counts.secure_ingress==0);
    initialize(); auto record=secure_record(); auto& ring=s_ivshmem_priv.rx_ring;
    s_ivshmem_priv.direct_peer=WKI_NODE_INVALID;
    CHECK(shared_ring::write(ring,ring.size,std::span(record)).error==shared_ring::Error::none);
    CHECK(drain_rx_ring(&s_ivshmem_priv,1)==1 && fixture::counts.secure_ingress==0);
    initialize();
    CHECK(shared_ring::write(s_ivshmem_priv.rx_ring,s_ivshmem_priv.rx_ring.size,std::span(record)).error==shared_ring::Error::none);
    s_ivshmem_priv.resources.callbacks.is_closed=true;
    CHECK(drain_rx_ring(&s_ivshmem_priv,1)==0 && fixture::counts.secure_ingress==0);
}

void secure_unavailable() {
    for (auto value:{"","0","65535","x"}) {
        reset(); fixture::direct_peer_value=value; wki_ivshmem_transport_init();
        CHECK(s_ivshmem_initialized && !s_ivshmem_transport.rdma_capable);
        CHECK(s_ivshmem_transport.secure_kind==rdma_protocol::Transport::NONE && !s_ivshmem_transport.secure_tx);
        auto bytes=secure_record();
        CHECK(ivshmem_secure_tx(&s_ivshmem_transport,2,bytes.data(),bytes.size())==-1);
        CHECK(ivshmem_wki_tx(&s_ivshmem_transport,2,bytes.data(),32)==0); // ordinary facet preserved
    }
    initialize(); auto bytes=secure_record();
    for(int n=0;n<3;++n) CHECK(shared_ring::write(s_ivshmem_priv.rx_ring,s_ivshmem_priv.rx_ring.size,std::span(bytes)).error==shared_ring::Error::none);
    CHECK(drain_rx_ring(&s_ivshmem_priv,2)==2 && fixture::counts.secure_ingress==2);
    CHECK(drain_rx_ring(&s_ivshmem_priv,1)==1 && fixture::counts.secure_ingress==3);
    CHECK(drain_rx_ring(&s_ivshmem_priv,1)==0);
}

void uninitialized() {
    reset(); uint32_t key=123;
    CHECK(ivshmem_wki_rdma_register_region(nullptr,UINT64_MAX,UINT32_MAX,&key)==-1 && key==123);
    CHECK(wki_ivshmem_rdma_alloc(4096)==-1 && wki_ivshmem_rdma_ptr(4096)==nullptr);
    wki_ivshmem_rdma_free(4096,4096); wki_ivshmem_irq_mailbox_write(nullptr,1,2,3);
    CHECK(fixture::counts==fixture::Counts{});
}
}
int main(int argc,char** argv) {
    struct Test { const char* name; void(*run)(); };
    const Test tests[]={{"raw",raw},{"capability",capability},{"doorbell",doorbell},{"mailbox",mailbox},
        {"ordinary",ordinary},{"tx",tx},{"budget",budget},{"malformed",malformed},{"irq",irq},
        {"geometry",geometry},{"packets",packets},{"shutdown",shutdown},{"uninitialized",uninitialized},{"secure_tx",secure_tx},{"secure_rx",secure_rx},{"secure_unavailable",secure_unavailable}};
    size_t ran=0;
    for (auto test:tests) if(argc==1 || std::string_view(argv[1])==test.name) {
        test.run(); std::printf("PASS %s\n",test.name); ++ran;
    }
    CHECK(ran!=0 && fixture::counts.held==0);
    std::printf("ivshmem_closure cases=%zu pass=%zu\n",ran,ran);
}
"""


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_body(source: str) -> str:
    # No transport function is rewritten or extracted; only include directives
    # are replaced by the declared host boundary definitions above.
    return re.sub(r"^#include[^\n]*\n", "", source, flags=re.MULTILINE)


def generate(source: Path) -> str:
    wire = (KERN / "net/wki/wire.hpp").read_text()
    constants = wire[wire.index("constexpr uint16_t WKI_ETHERTYPE"):wire.index("// Header Flags")]
    constants = constants[:constants.rfind("// -----------------------------------------------------------------------------")]
    transport = (KERN / "net/wki/wki.hpp").read_text()
    begin = transport.index("struct WkiRxMetadata {")
    end = transport.index("// Peer State Machine", begin)
    declarations = transport[begin:end]
    declarations = declarations[:declarations.rfind("// -----------------------------------------------------------------------------")]
    max_frame = re.search(r"^constexpr size_t WKI_MAX_FRAME_SIZE =[^\n]+;", transport, re.MULTILINE)
    assert max_frame
    return PRELUDE + constants + max_frame.group(0) + "\n" + declarations + BOUNDARIES + source_body(source.read_text()) + CASES


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=SOURCE)
    parser.add_argument("--case")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    directory = args.output_dir or Path(tempfile.mkdtemp(prefix="wki-ivshmem-closure-"))
    directory.mkdir(parents=True, exist_ok=True)
    cpp, binary = directory / "actual_ivshmem.cpp", directory / "actual_ivshmem"
    if cpp.exists():
        raise RuntimeError("refusing to overwrite an earlier proof")
    cpp.write_text(generate(args.source))
    command = [os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++"), "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Werror",
               "-Wno-unused-const-variable",
               "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-I", str(KERN), str(cpp), "-o", str(binary)]
    (directory / "compiler-command.json").write_text(json.dumps(command, indent=2)+"\n")
    compile_result = subprocess.run(command, capture_output=True, text=True)
    (directory / "compile.log").write_text(compile_result.stdout+compile_result.stderr)
    if compile_result.returncode:
        raise RuntimeError(f"compile failed ({compile_result.returncode}): {directory / 'compile.log'}")
    environment = os.environ.copy()
    if environment.get("WOS_HOST_TEST_DISABLE_LSAN"):
        environment["ASAN_OPTIONS"] = environment.get("ASAN_OPTIONS", "")+":detect_leaks=0"
    run_command = [str(binary)] + ([args.case] if args.case else [])
    (directory / "run-command.json").write_text(json.dumps(run_command, indent=2)+"\n")
    environment["UBSAN_OPTIONS"] = environment.get("UBSAN_OPTIONS", "")+":halt_on_error=1"
    result = subprocess.run(run_command, capture_output=True, text=True, env=environment)
    (directory / "run.log").write_text(result.stdout+result.stderr)
    inputs = [args.source, KERN / "dev/ivshmem/shared_ring.hpp", KERN / "net/wki/timer_math.hpp", KERN / "net/wki/rdma_protocol.hpp",
              KERN / "net/wki/auth_crypto.hpp", KERN / "net/wki/rdma_crypto.hpp",
              KERN / "net/wki/transport_ivshmem.hpp", KERN / "net/wki/wki.hpp", KERN / "net/wki/wire.hpp", Path(__file__)]
    outputs = [cpp, binary, directory / "compiler-command.json", directory / "compile.log", directory / "run-command.json", directory / "run.log"]
    receipt = {"compile_returncode": 0, "run_returncode": result.returncode, "case": args.case or "all",
               "transport_body_verbatim_after_include_removal": True,
               "scope": "actual transport and shared ring with instrumented external boundaries; no hardware/authentication claim",
               "inputs": [{"path": str(p.resolve()), "sha256": digest(p)} for p in inputs],
               "outputs": [{"path": str(p.resolve()), "sha256": digest(p)} for p in outputs]}
    (directory / "result.json").write_text(json.dumps(receipt, indent=2)+"\n")
    print(result.stdout, end="")
    if result.returncode or "runtime error:" in result.stderr or "AddressSanitizer:" in result.stderr:
        raise RuntimeError(f"execution failed ({result.returncode}): {directory / 'run.log'}")


if __name__ == "__main__":
    main()
