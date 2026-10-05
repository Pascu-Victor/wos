#!/usr/bin/env python3
"""Execute actual opt-in IRQ/RX hooks and recovery verdict with host DMA/clock seams.

This checks eligibility, ownership and verdicts, not real IRQ delivery or DMA.
The separate socket fixture supplies those runtime observations.
"""

import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile

from napi_handoff_test import function, structure

ROOT = Path(__file__).resolve().parents[3]

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>
#include <net/netpoll.hpp>
#include <dev/virtio/napi_irq_test.hpp>
uint64_t now_us = 1000;
namespace ker::mod::time { uint64_t get_us() { return now_us; } }
namespace ker::net {
struct PacketBuffer { uint8_t* data; size_t len; unsigned freed = 0; };
void pkt_free(PacketBuffer* packet) { assert(packet->freed++ == 0); }
}
namespace ker::dev::virtio {
struct VirtqUsedElem { uint32_t id, len; };
struct VirtqUsed { uint16_t idx; std::array<VirtqUsedElem, 8> ring; };
struct Virtqueue {
    VirtqUsed* used; uint16_t size = 8, last_used_idx = 0;
    std::atomic<bool> failed{false};
};
struct VirtIONetDevice { uint8_t hdr_size = 12; };
struct VirtIONetQueuePair {
    VirtIONetDevice* dev; unsigned index = 0; Virtqueue* rxq;
    ker::net::NapiStruct napi{};
};
'''

TESTS = r'''
void exercise() {
    using ker::net::NapiState;
    using ker::net::NapiPollSource;
    VirtIONetDevice dev{}, other{};
    VirtqUsed used{};
    Virtqueue queue{&used};
    VirtIONetQueuePair pair{&dev, 0, &queue};
    std::array<uint8_t, napi_test::FRAME_BYTES> bytes{};
    auto frame = [&](uint64_t cookie, bool reply) {
        for (size_t i = 0; i < bytes.size(); ++i) bytes[i] = napi_test::packet_byte(i, cookie, reply);
    };
    for (uint64_t cookie : {uint64_t{1}, uint64_t{0x1020304050607080}, UINT64_MAX}) {
        for (bool reply : {false, true}) {
            frame(cookie, reply);
            auto parsed = napi_test::inspect(bytes);
            assert(parsed && parsed->cookie == cookie && parsed->reply == reply);
            for (size_t i = 0; i < bytes.size(); ++i) {
                bytes[i] ^= 4;
                auto changed = napi_test::inspect(bytes);
                assert(!changed || changed->cookie != cookie);
                bytes[i] ^= 4;
            }
            for (size_t size = 0; size < bytes.size(); ++size)
                assert(!napi_test::inspect(std::span<const uint8_t>(bytes.data(), size)));
        }
    }
    frame(0, true); assert(!napi_test::inspect(bytes));
    std::array<uint8_t, 65> oversized{};
    frame(1, true); std::copy(bytes.begin(), bytes.end(), oversized.begin());
    assert(!napi_test::inspect(oversized));

    auto fresh = [&] {
        std::destroy_at(&napi_irq_test); std::construct_at(&napi_irq_test);
        napi_irq_test.device.store(&dev);
        napi_irq_test.expected_cookie.store(UINT64_MAX);
        napi_irq_test.armed_cookie.store(UINT64_MAX);
        pair.dev = &dev; pair.index = 0; pair.napi.state.store(NapiState::IDLE); pair.napi.has_work.store(false);
        queue.failed.store(false); queue.last_used_idx = 0;
        used.idx = 1; used.ring[0] = {3, 76}; now_us = 1000;
    };
    fresh(); napi_irq_test.device.store(nullptr); assert(!selftest_napi_irq(&pair));
    fresh(); pair.dev = &other; assert(!selftest_napi_irq(&pair));
    fresh(); pair.index = 1; assert(!selftest_napi_irq(&pair));
    fresh(); napi_irq_test.armed_cookie.store(0); assert(!selftest_napi_irq(&pair));
    for (auto state : {NapiState::SCHEDULED, NapiState::POLLING, NapiState::DISABLED}) {
        fresh(); pair.napi.state.store(state); assert(!selftest_napi_irq(&pair));
    }
    fresh(); pair.napi.has_work.store(true); assert(!selftest_napi_irq(&pair));
    fresh(); queue.failed.store(true); assert(!selftest_napi_irq(&pair));
    fresh(); used.idx = 0; assert(!selftest_napi_irq(&pair));
    fresh(); used.idx = 2; assert(!selftest_napi_irq(&pair));
    fresh(); used.ring[0].id = 8; assert(!selftest_napi_irq(&pair));
    fresh(); used.ring[0].len = 75; assert(!selftest_napi_irq(&pair));
    fresh(); used.ring[0].len = 77; assert(!selftest_napi_irq(&pair));
    fresh();
    assert(selftest_napi_irq(&pair));
    assert(napi_irq_test.armed_cookie.load() == 0);
    assert(napi_irq_test.suppressed.load() == 1 && napi_irq_test.suppressed_cookie.load() == UINT64_MAX);
    assert(napi_irq_test.suppressed_idx.load() == 1 && napi_irq_test.suppressed_desc.load() == 3);
    assert(napi_irq_test.suppressed_len.load() == 76 && napi_irq_test.suppressed_us.load() == 1000);
    assert(!selftest_napi_irq(&pair));
    frame(UINT64_MAX, true);
    queue.last_used_idx = 1; now_us = 101000;
    ker::net::PacketBuffer packet{bytes.data(), bytes.size()};
    assert(selftest_napi_rx(&dev, &queue, &packet, NapiPollSource::RESCUE, 3));
    assert(packet.freed == 1 && napi_irq_test.received_cookie.load() == UINT64_MAX);
    assert(napi_irq_test.source.load() == NapiPollSource::RESCUE);
    assert(napi_irq_test.received_idx.load() == 1 && napi_irq_test.received_desc.load() == 3);
    ker::net::NapiDiagSnapshot before{}, after{}; before.enabled = after.enabled = true;
    assert(selftest_napi_fault_valid(UINT64_MAX, 0, 12, before, after));
    after.schedules[1] = 1;
    assert(!selftest_napi_fault_valid(UINT64_MAX, 0, 12, before, after));
    after.schedules[1] = 0;
    after.enabled = false; assert(!selftest_napi_fault_valid(UINT64_MAX, 0, 12, before, after)); after.enabled = true;
    assert(!selftest_napi_fault_valid(1, 0, 12, before, after));
    assert(!selftest_napi_fault_valid(UINT64_MAX, 1, 12, before, after));
    assert(!selftest_napi_fault_valid(UINT64_MAX, 0, 10, before, after));
    napi_irq_test.received_us.store(151000); assert(selftest_napi_fault_valid(UINT64_MAX, 0, 12, before, after));
    napi_irq_test.received_us.store(151001); assert(!selftest_napi_fault_valid(UINT64_MAX, 0, 12, before, after));
    napi_irq_test.received_us.store(999); assert(!selftest_napi_fault_valid(UINT64_MAX, 0, 12, before, after));
    packet.freed = 0; assert(selftest_napi_rx(&dev, &queue, &packet, NapiPollSource::WORKER, 3));
    assert(packet.freed == 1 && napi_irq_test.duplicate.load() == 1);
    frame(UINT64_MAX, false); packet.freed = 0;
    assert(selftest_napi_rx(&dev, &queue, &packet, NapiPollSource::WORKER, 4));
    assert(packet.freed == 1 && napi_irq_test.reflections.load() == 1);
    frame(1, true); packet.freed = 0;
    assert(selftest_napi_rx(&dev, &queue, &packet, NapiPollSource::RESCUE, 4));
    assert(packet.freed == 1 && napi_irq_test.bad.load() == 1);
    frame(UINT64_MAX, true); bytes[63] ^= 1; packet.freed = 0;
    assert(selftest_napi_rx(&dev, &queue, &packet, NapiPollSource::RESCUE, 4));
    assert(packet.freed == 1 && napi_irq_test.bad.load() == 2);
    bytes[14] = 0; packet.freed = 0;
    assert(!selftest_napi_rx(&dev, &queue, &packet, NapiPollSource::RESCUE, 4));
    assert(packet.freed == 0 && napi_irq_test.foreign.load() == 1);
    frame(UINT64_MAX, true);
    assert(!selftest_napi_rx(&other, &queue, &packet, NapiPollSource::RESCUE, 3));
    assert(packet.freed == 0);
    // Used-index wrap must retain the same single-completion identity.
    fresh(); queue.last_used_idx = 65535; used.idx = 0; used.ring[7] = {7, 76};
    assert(selftest_napi_irq(&pair));
    queue.last_used_idx = 0; packet.freed = 0; now_us = 101000;
    assert(selftest_napi_rx(&dev, &queue, &packet, NapiPollSource::RESCUE, 7));
    assert(selftest_napi_fault_valid(UINT64_MAX, 0, 12, before, after));
}
}
'''


BOOT_PRELUDE = r'''
#include <string_view>
#define WOS_SELFTEST
unsigned claims = 0, transports = 0, broadcasts = 0;
const char* boot_cmdline = nullptr;
namespace ker::net {
constexpr size_t MAX_NET_DEVICES = 4;
struct NetDevice {};
struct NetDeviceRef {
    NetDevice* ptr{};
    NetDevice* get() const { return ptr; }
    explicit operator bool() const { return ptr != nullptr; }
};
NetDevice fixture_device;
NetDeviceRef netdev_find_by_name_ref(const char* name) {
    return {std::string_view(name) == "eth0" ? &fixture_device : nullptr};
}
namespace wki {
void wki_eth_transport_claim(NetDevice*) { ++claims; }
void wki_eth_transport_init(NetDevice*) { ++transports; }
void wki_peer_send_hello_broadcast() { ++broadcasts; }
}
}
namespace ker::util::netdevconf {
size_t find_devices(const char*, std::array<ker::net::NetDeviceRef, ker::net::MAX_NET_DEVICES>&) { return 0; }
}
namespace ker::mod::dbg {
template<size_t N> struct Tag { char value[N]; constexpr Tag(const char (&s)[N]) { std::copy_n(s, N, value); } };
template<Tag Name> struct logger { static void info(const char*) {} };
}
namespace ker::init::fns {
const char* get_kernel_cmdline() { return boot_cmdline; }
'''
BOOT_TESTS = r'''
void exercise_boot() {
    for (const char* cmdline : {"", "--selftest", "--napi-irq-test-extra", "prefix--napi-irq-test"}) {
        claims = transports = broadcasts = 0; boot_cmdline = cmdline;
        wki_eth_transport_init();
        assert(claims == 1 && transports == 1 && broadcasts == 1);
    }
    for (const char* cmdline : {"--napi-irq-test", " net.napi_diag\t--napi-irq-test\n"}) {
        claims = transports = broadcasts = 0; boot_cmdline = cmdline;
        wki_eth_transport_init();
        assert(claims == 1 && transports == 0 && broadcasts == 0);
    }
}
}
int main() { ker::dev::virtio::exercise(); ker::init::fns::exercise_boot(); }
'''


def main():
    source = (ROOT / 'modules/kern/src/dev/virtio/virtio_net.cpp').read_text()
    irq = function(source, 'virtio_net_irq')
    # Hook placement is itself part of the software-loss experiment.
    positions = [irq.index(token) for token in (
        'CallbackGuard', 'notification_lock.lock_irqsave', 'virtio_net_set_pair_notifications(pair, false)',
        'selftest_napi_irq', 'notification_lock.unlock_irqrestore', 'if (SUPPRESSED) return', 'napi_schedule')]
    assert positions == sorted(positions), positions
    rx = function(source, 'process_rx_budget_for')
    assert rx.index('virtq_get_buf') < rx.index('pkt->data =') < rx.index('selftest_napi_rx') < rx.index('netdev_rx')
    bodies = structure(source, 'NapiIrqTest') + '\nNapiIrqTest napi_irq_test;\n'
    for name in ('selftest_napi_irq', 'selftest_napi_rx', 'selftest_napi_fault_valid'):
        bodies += function(source[source.index('struct NapiIrqTest {'):], name) + '\n'
    mutants = [
        ('one_shot', '!napi_irq_test.armed_cookie.compare_exchange_strong(cookie, 0, std::memory_order_acq_rel)',
         'false', 'napi_irq_test.armed_cookie.load() == 0'),
        ('single_pending', 'static_cast<uint16_t>(USED - queue->last_used_idx) != 1',
         'false', '!selftest_napi_irq(&pair)'),
        ('origin', 'napi_irq_test.source.store(source, std::memory_order_relaxed);',
         '(void)source; napi_irq_test.source.store(ker::net::NapiPollSource::INLINE, std::memory_order_relaxed);',
         'napi_irq_test.source.load() == NapiPollSource::RESCUE'),
        ('completion_index', 'napi_irq_test.received_idx.store(queue->last_used_idx, std::memory_order_relaxed);',
         'napi_irq_test.received_idx.store(queue->last_used_idx + 1, std::memory_order_relaxed);',
         'napi_irq_test.received_idx.load() == 1'),
        ('producer', 'before.schedules != after.schedules ||', '', '!selftest_napi_fault_valid(UINT64_MAX, 0, 12, before, after)'),
        ('bound', 'END - BEGIN <= napi_test::RECOVERY_LIMIT_US', 'true', '!selftest_napi_fault_valid(UINT64_MAX, 0, 12, before, after)'),
    ]
    boot_source = (ROOT / 'modules/kern/src/platform/init/init_wrappers.cpp').read_text()
    boot = function(boot_source, 'cmdline_has_token') + '\n' + function(boot_source, 'wki_eth_transport_init')
    compiler = os.environ.get('HOST_CXX') or shutil.which('clang++', path=os.defpath)
    assert compiler, 'clang++ is required'
    with tempfile.TemporaryDirectory(prefix='wos-napi-irq-') as tmp:
        root = Path(tmp)
        variants = [('qualified', bodies, None)]
        for name, old, new, assertion in mutants:
            assert bodies.count(old) == 1, name
            variants.append((name, bodies.replace(old, new), assertion))
        for name, body, expected in variants:
            path, binary = root / f'{name}.cpp', root / name
            path.write_text(PRELUDE + body + TESTS + BOOT_PRELUDE + boot + BOOT_TESTS)
            subprocess.run([compiler, '-std=c++20', '-O1', '-g', '-Wall', '-Wextra', '-Werror',
                            '-fsanitize=address,undefined', '-fno-omit-frame-pointer',
                            '-I', str(ROOT / 'modules/kern/src'), str(path), '-o', str(binary)], check=True, timeout=60)
            result = subprocess.run([str(binary)], capture_output=True, text=True, timeout=20, env=os.environ.copy())
            if expected is None:
                assert result.returncode == 0, result.stderr
                print('actual hooks/protocol/verdict ASan/UBSan PASS')
            else:
                assert result.returncode == -signal.SIGABRT and expected in result.stderr, (name, result.returncode, result.stderr)
                print(f'negative control {name}: expected semantic assertion PASS')


if __name__ == '__main__':
    main()
