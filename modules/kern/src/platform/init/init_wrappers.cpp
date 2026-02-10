// Init wrapper functions for the kernel initialization dependency system
// Each wrapper calls the actual init function from the appropriate module
#include <defines/defines.hpp>
#include <dev/ahci.hpp>
#include <dev/block_device.hpp>
#include <dev/console.hpp>
#include <dev/device.hpp>
#include <dev/e1000e/e1000e.hpp>
#include <dev/ivshmem/ivshmem_net.hpp>
#include <dev/pci.hpp>
#include <dev/usb/cdc_ether.hpp>
#include <dev/usb/xhci.hpp>
#include <dev/virtio/virtio_net.hpp>
#include <mod/gfx/fb.hpp>
#include <mod/io/serial/serial.hpp>
#include <net/loopback.hpp>
#include <net/net.hpp>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/packet.hpp>
#include <net/proto/ipv6.hpp>
#include <net/proto/ndp.hpp>
#include <net/wki/peer.hpp>
#include <net/wki/transport_eth.hpp>
#include <net/wki/transport_ivshmem.hpp>
#include <net/wki/wki.hpp>
#include <platform/acpi/acpi.hpp>
#include <platform/acpi/apic/apic.hpp>
#include <platform/acpi/ioapic/ioapic.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/boot/handover.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/interrupt/idt.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/mm.hpp>
#include <platform/pic/pic.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>
#include <platform/sys/syscall.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/initramfs.hpp>
#include <vfs/vfs.hpp>

#include "init_registry.hpp"
#include "limine_requests.hpp"

namespace ker::init::fns {

void fb_init() {
    if constexpr (mod::gfx::fb::WOS_HAS_GFX_FB) {
        mod::gfx::fb::init();
    }
}

void serial_init() { mod::io::serial::init(); }

void dbg_init() {
    // Note: dbg::init() internally calls serial::init(), but serial::init()
    // is idempotent (has isInit guard), so this is safe.
    // We still call serial_init separately to make the dependency explicit.
    mod::dbg::init();
}

void mm_init() { mod::mm::init(); }

void fsgsbase_init() {
    // Enable FSGSBASE instructions
    mod::cpu::enableFSGSBASE();
}

void gdt_init() {
    // Initialize GDT with the captured stack pointer
    uint64_t rsp = get_kernel_rsp();
    auto* stack = reinterpret_cast<uint8_t*>(rsp);
    mod::desc::gdt::initDescriptors(reinterpret_cast<uint64_t*>(stack) + KERNEL_STACK_SIZE, 0);  // BSP is CPU 0
}

void kmalloc_init() {
    mod::mm::dyn::kmalloc::init();
    mod::dbg::enableKmalloc();
}

void pic_remap() { mod::pic::remap(); }

void acpi_init() { mod::acpi::init(); }

void apic_init() { mod::apic::init(); }

void apic_mp_init() { mod::apic::initApicMP(); }

void time_init() {
    mod::time::init();
    mod::dbg::enableTime();
}

void idt_init() { mod::desc::idt::idtInit(); }

void sys_init() { mod::sys::init(); }

void ioapic_init() { mod::ioapic::init(); }

void dev_init() { dev::dev_init(); }

void pci_enumerate() { dev::pci::pci_enumerate_all(); }

void console_init() { dev::console::console_init(); }

void ahci_init() { dev::ahci::ahci_controller_init(); }

void block_device_init() { dev::block_device_init(); }

void vfs_init() { vfs::init(); }

void devfs_populate_partitions() { vfs::devfs::devfs_populate_partition_symlinks(); }

void net_init() {
    // net::init() calls pkt_pool_init() and loopback_init() internally
    net::init();
}

void virtio_net_init() { dev::virtio::virtio_net_init(); }

void e1000e_init() { dev::e1000e::e1000e_init(); }

void cdc_ether_init() { dev::usb::cdc_ether_init(); }

void xhci_init() { dev::usb::xhci_init(); }

void ivshmem_init() { dev::ivshmem::ivshmem_net_init(); }

void pkt_pool_expand() { net::pkt_pool_expand_for_nics(); }

void ndp_init() { net::proto::ndp_init(); }

void wki_init() { net::wki::wki_init(); }

void devfs_populate_net() { vfs::devfs::devfs_populate_net_nodes(); }

void smt_init() { mod::smt::init(); }

void epoch_manager_init() { mod::sched::EpochManager::init(); }

void wki_eth_transport_init() {
    // Search for eth1, fall back to eth0
    auto* wki_dev = net::netdev_find_by_name("eth1");
    if (wki_dev == nullptr) {
        wki_dev = net::netdev_find_by_name("eth0");
    }
    if (wki_dev != nullptr) {
        net::wki::wki_eth_transport_init(wki_dev);
        net::wki::wki_peer_send_hello_broadcast();
    }
}

void wki_ivshmem_transport_init() { net::wki::wki_ivshmem_transport_init(); }

void ipv6_linklocal_init() {
    // Configure IPv6 link-local addresses on eth0 and eth1
    auto* eth0 = net::netdev_find_by_name("eth0");
    if (eth0 != nullptr) {
        std::array<uint8_t, 16> ll_addr{};
        net::proto::ipv6_make_link_local(ll_addr, eth0->mac);
        net::netif_add_ipv6(eth0, ll_addr, 64);
    }

    auto* eth1 = net::netdev_find_by_name("eth1");
    if (eth1 != nullptr) {
        std::array<uint8_t, 16> ll_addr{};
        net::proto::ipv6_make_link_local(ll_addr, eth1->mac);
        net::netif_add_ipv6(eth1, ll_addr, 64);
    }
}

void sse_init() {
    // Enable SSE instructions and mark CPU ID available for serial output
    mod::cpu::enableSSE();
    mod::io::serial::markCpuIdAvailable();
}

void initramfs_init() {
    // Unpack CPIO initramfs from Limine modules into tmpfs root
    auto& module_request = get_kernel_module_request();
    if (module_request.response == nullptr) {
        return;
    }

    for (size_t i = 0; i < module_request.response->module_count; i++) {
        auto* mod_data = static_cast<uint8_t*>(module_request.response->modules[i]->address);
        size_t mod_size = module_request.response->modules[i]->size;
        // Check for CPIO newc magic "070701"
        if (mod_size >= 6 && mod_data[0] == '0' && mod_data[1] == '7' && mod_data[2] == '0' && mod_data[3] == '7' && mod_data[4] == '0' &&
            mod_data[5] == '1') {
            mod::dbg::log("Found CPIO initramfs module at index %u (%u bytes)", static_cast<unsigned>(i), static_cast<unsigned>(mod_size));
            vfs::initramfs::unpack_initramfs(mod_data, mod_size);
        }
    }
}

void sched_init() {
    mod::sched::setup_queues();

    // Build HandoverModules from limine module request
    mod::boot::HandoverModules modules{};
    auto& module_request = get_kernel_module_request();

    if (module_request.response == nullptr) {
        mod::dbg::log("Kernel module request failed");
        hcf();
    }

    if (module_request.response->module_count > 32) {
        mod::dbg::log("Too many modules loaded by limine (%x/32)", module_request.response->module_count);
        hcf();
    }

    modules.count = module_request.response->module_count;

    for (size_t i = 0; i < modules.count; i++) {
        mod::dbg::log("Module: %s", module_request.response->modules[i]->path);
        modules.modules[i].entry = module_request.response->modules[i]->address;
        modules.modules[i].size = module_request.response->modules[i]->size;
        modules.modules[i].cmdline = module_request.response->modules[i]->path;
        modules.modules[i].name = module_request.response->modules[i]->path;
    }

    // Start other CPUs and enter scheduler loop (never returns)
    mod::smt::start_smt(modules, get_kernel_rsp());
}

void kernel_start() { mod::sched::start_scheduler(); }

}  // namespace ker::init::fns
