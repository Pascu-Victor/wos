#include <limine.h>

#include <cstdint>
#include <defines/defines.hpp>
#include <dev/ahci.hpp>
#include <dev/block_device.hpp>
#include <dev/console.hpp>
#include <dev/device.hpp>
#include <dev/pci.hpp>
#include <mod/gfx/fb.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/boot/handover.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/interrupt/interrupt.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/mm.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>
#include <platform/sys/syscall.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/initramfs.hpp>
#include <vfs/mount.hpp>
#include <vfs/vfs.hpp>

#include "platform/asm/cpu.hpp"

using namespace ker::mod;

// Linker-provided symbols for init/fini arrays
extern "C" {
extern void (*__preinit_array_start[])();
extern void (*__preinit_array_end[])();
extern void (*__init_array_start[])();
extern void (*__init_array_end[])();
extern void (*__fini_array_start[])();
extern void (*__fini_array_end[])();
}
namespace {
void callGlobalConstructors() {
    for (auto* ctor = static_cast<void (**)()>(__preinit_array_start); ctor < static_cast<void (**)()>(__preinit_array_end); ++ctor) {
        (*ctor)();
    }

    for (auto* ctor = static_cast<void (**)()>(__init_array_start); ctor < static_cast<void (**)()>(__init_array_end); ++ctor) {
        (*ctor)();
    }
}

void callGlobalDestructors() {
    for (auto* dtor = static_cast<void (**)()>(__fini_array_end); dtor > static_cast<void (**)()>(__fini_array_start);) {
        --dtor;
        (*dtor)();
    }
}

__attribute__((used, section(".requests"))) volatile LIMINE_BASE_REVISION(3);  // NOLINT

__attribute__((used, section(".requests_start_marker"))) volatile LIMINE_REQUESTS_START_MARKER;  // NOLINT

__attribute__((used, section(".requests"))) volatile limine_module_request kernelModuleRequest = {
    .id = LIMINE_MODULE_REQUEST,
    .revision = 1,
    .response = nullptr,
    .internal_module_count = 0,
    .internal_modules = nullptr,
};

__attribute__((used, section(".requests_end_marker"))) volatile LIMINE_REQUESTS_END_MARKER;  // NOLINT

}  // namespace

// Kernel entry point.
extern "C" void _start(void) {
    if (LIMINE_BASE_REVISION_SUPPORTED == 0) {
        hcf();
    }

    callGlobalConstructors();

    // Init the framebuffer.
    if constexpr (gfx::fb::WOS_HAS_GFX_FB) {
        gfx::fb::init();
    }
    // Init logging.
    dbg::init();
    dbg::log("Hi from WOs");

    // Init memory manager.
    ker::mod::mm::init();
    if constexpr (gfx::fb::WOS_HAS_GFX_FB) {
        gfx::fb::mapFramebuffer();
        dbg::log("Framebuffer mapped");
    }
    dbg::log("Pages mapped");

    // Enable FSGSBASE instructions
    cpu::enableFSGSBASE();

    uint8_t* stack = stack;
    // Init gds.
    asm volatile("mov %%rsp, %0" : "=r"(stack));
    ker::mod::desc::gdt::initDescriptors((uint64_t*)stack + KERNEL_STACK_SIZE, 0);  // BSP is CPU 0
    // asm volatile("mov %0, %%rsp" ::"r"((uint64_t)stack + KERNEL_STACK_SIZE));

    // Init kmalloc
    ker::mod::mm::dyn::kmalloc::init();

    // Init interrupts.
    ker::mod::interrupt::init();
    ker::mod::sys::init();

    // Init device subsystem
    ker::dev::dev_init();

    // Init console devices
    ker::dev::console::console_init();

    // Init AHCI controller
    ker::dev::ahci::ahci_controller_init();

    // Init block devices and mount filesystems
    ker::dev::block_device_init();

    // Init VFS
    ker::vfs::init();

    // Populate /dev/disk/by-partuuid/ symlinks from GPT partitions
    ker::vfs::devfs::devfs_populate_partition_symlinks();

    // Unpack CPIO initramfs from Limine modules into tmpfs root
    if (kernelModuleRequest.response != nullptr) {
        for (size_t i = 0; i < kernelModuleRequest.response->module_count; i++) {
            auto* mod_data = static_cast<uint8_t*>(
                kernelModuleRequest.response->modules[i]->address);
            size_t mod_size = kernelModuleRequest.response->modules[i]->size;
            // Check for CPIO newc magic "070701"
            if (mod_size >= 6 &&
                mod_data[0] == '0' && mod_data[1] == '7' &&
                mod_data[2] == '0' && mod_data[3] == '7' &&
                mod_data[4] == '0' && mod_data[5] == '1') {
                dbg::log("Found CPIO initramfs module at index %u (%u bytes)",
                         static_cast<unsigned>(i),
                         static_cast<unsigned>(mod_size));
                ker::vfs::initramfs::unpack_initramfs(mod_data, mod_size);
            }
        }
    }

    ker::mod::sched::init();

    boot::HandoverModules modules;

    if (kernelModuleRequest.response == nullptr) {
        dbg::log("Kernel module request failed");
        hcf();
    }

    if (kernelModuleRequest.response->module_count > 32) {
        dbg::log("Too many modules loaded by limine (%x/32)", kernelModuleRequest.response->module_count);
        hcf();
    }

    modules.count = kernelModuleRequest.response->module_count;

    for (size_t i = 0; i < modules.count; i++) {
        ker::mod::dbg::log("Module: %s", kernelModuleRequest.response->modules[i]->path);
        modules.modules[i].entry = kernelModuleRequest.response->modules[i]->address;
        modules.modules[i].size = kernelModuleRequest.response->modules[i]->size;
        modules.modules[i].cmdline = kernelModuleRequest.response->modules[i]->path;
        modules.modules[i].name = kernelModuleRequest.response->modules[i]->path;
    }

    // Enable SSE instructions
    // Here so we can fail hard for sse instructions in the kernel easily for as long as possible
    cpu::enableSSE();
    ker::mod::io::serial::markCpuIdAvailable();

    // Init smt
    ker::mod::smt::startSMT(modules, (uint64_t)stack);

    callGlobalDestructors();
    // Kernel should halt and catch fire if it reaches this point.
    hcf();
}
