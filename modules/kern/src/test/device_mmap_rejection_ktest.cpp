#include <abi/callnums/vmem.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/device.hpp>
#include <initializer_list>
#include <memory>
#include <new>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/credentials.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <test/ktest.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/vfs.hpp>

namespace {
namespace addr = ker::mod::mm::addr;
namespace paging = ker::mod::mm::paging;
namespace phys = ker::mod::mm::phys;
namespace task = ker::mod::sched::task;
namespace virt = ker::mod::mm::virt;
namespace vm = ker::abi::vmem;
namespace vmem = ker::syscall::vmem;
namespace vfs = ker::vfs;
namespace devfs = ker::vfs::devfs;

constexpr uint64_t ADDRESS = 0x5D000000ULL;
constexpr uint8_t MARKER = 0xA7;
constexpr char NAME[] = "ktest_device_mmap_refusal";

struct Counters {
    unsigned opens{}, reads{}, closes{};
};
Counters counters;

ker::dev::CharDeviceOps operations{
    .close = [](vfs::File*) -> int {
        ++counters.closes;
        return 0;
    },
    .read = [](vfs::File*, void* output, size_t count) -> ssize_t {
        ++counters.reads;
        std::memset(output, 0x41, count);
        return static_cast<ssize_t>(count);
    },
    .open_with_credentials = [](vfs::File*, const ker::mod::security::Snapshot&, task::Task* opener) -> int {
        if (opener == nullptr) return -EACCES;
        ++counters.opens;
        return 0;
    },
};
// Endpoint storage has kernel lifetime. This fixture does not claim dynamic
// Device-unregister ownership or a hardware/device-backed mapping provider.
ker::dev::Device character{.name = NAME, .type = ker::dev::DeviceType::CHAR, .char_ops = &operations};
ker::dev::Device block{.name = NAME, .type = ker::dev::DeviceType::BLOCK, .char_ops = &operations};

auto physical(const void* page) -> uint64_t { return reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(page))); }

auto leaf_snapshot(paging::PageTable* root, uint64_t& out) -> bool {
    auto* table = root;
    for (unsigned shift : {39U, 30U, 21U}) {
        auto const& entry = table->entries.at((ADDRESS >> shift) & 511U);
        if (!entry.present || entry.pagesize) return false;
        table = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT));
    }
    auto const& leaf = table->entries.at((ADDRESS >> paging::PAGE_SHIFT) & 511U);
    std::memcpy(&out, &leaf, sizeof(out));
    return leaf.present;
}

struct Fixture {
    std::unique_ptr<task::Task> subject{new (std::nothrow) task::Task{}};
    void* marker{};  // Observer owns one frame reference independently of PTE.
    bool published{};
    int fd = -1;

    ~Fixture() {
        if (subject != nullptr) {
            task::release_lazy_vmem_ranges(*subject);
            auto* root = subject->detach_pagemap_after_usercopy_quiescence();
            if (root != nullptr) vmem::release_file_mmap_ranges_for_pagemap(root);
            close();
            if (root != nullptr) {
                virt::destroy_user_space(root, 0, "device-mmap-ktest", "refusal-fixture");
                virt::release_pagemap(root);
            }
        }
        if (marker != nullptr) {
            KEXPECT_EQ(phys::page_ref_get(marker), 1U);
            static_cast<void>(phys::page_ref_dec(marker));
        }
        if (published) KEXPECT_TRUE(devfs::devfs_remove_node(NAME));
    }

    auto init(ker::dev::Device& device) -> bool {
        if (subject == nullptr) return false;
        subject->type = task::TaskType::PROCESS;
        subject->pagemap = virt::create_pagemap_may_fail();
        if (subject->pagemap == nullptr) return false;
        marker = phys::page_alloc_may_fail(ker::mod::mm::PhysicalPageOwner::SELFTEST, paging::PAGE_SIZE, "device-mmap-marker");
        if (marker == nullptr) return false;
        std::memset(marker, MARKER, paging::PAGE_SIZE);
        if (!phys::page_ref_try_inc(marker)) return false;
        virt::map_page(subject->pagemap, ADDRESS, physical(marker), paging::page_types::USER_READONLY | paging::PAGE_NX);
        if (virt::translate(subject->pagemap, ADDRESS) != physical(marker)) {
            static_cast<void>(phys::page_ref_dec(marker));
            return false;
        }
        std::array<char, devfs::DEVFS_NAME_MAX> name{};
        std::memcpy(name.data(), NAME, sizeof(NAME));
        auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
        // Exclusive owned publication refuses an existing node, so cleanup
        // cannot remove a user/other test endpoint with the same name.
        published = devfs::devfs_add_device_node(name, &device, &ROOT) != nullptr;
        return published;
    }

    auto open(int flags) -> vfs::File* {
        auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
        auto* file = devfs::devfs_open_path(ROOT, NAME, flags, 0, nullptr, false, subject.get());
        if (file == nullptr) return nullptr;
        fd = vfs::vfs_alloc_fd(subject.get(), file);
        if (fd >= 0) return file;
        vfs::vfs_put_file(file);
        return nullptr;
    }

    void close() {
        if (fd < 0) return;
        KEXPECT_EQ(vfs::vfs_close_for_task(subject.get(), fd), 0);
        fd = -1;
    }
};

void check_refusal(ker::dev::Device& device, uint32_t expected_type) {
    Fixture fixture;
    KREQUIRE_TRUE(fixture.init(device));
    counters = {};
    struct Request {
        int open_flags;
        uint64_t prot, flags, length;
        int error;
    };
    constexpr std::array REQUESTS{
        Request{0, vm::PROT_READ, vm::MAP_SHARED | vm::MAP_FIXED, paging::PAGE_SIZE, -ENODEV},
        Request{2, vm::PROT_READ | vm::PROT_WRITE, vm::MAP_SHARED | vm::MAP_FIXED, paging::PAGE_SIZE + 1, -ENODEV},
        Request{0, vm::PROT_READ | vm::PROT_WRITE, vm::MAP_PRIVATE | vm::MAP_FIXED, paging::PAGE_SIZE - 7, -ENODEV},
        Request{2, vm::PROT_NONE, vm::MAP_PRIVATE | vm::MAP_FIXED, paging::PAGE_SIZE, -ENODEV},
        Request{0, vm::PROT_READ | vm::PROT_WRITE, vm::MAP_SHARED | vm::MAP_FIXED, paging::PAGE_SIZE, -EACCES},
        Request{1, vm::PROT_READ, vm::MAP_SHARED | vm::MAP_FIXED, paging::PAGE_SIZE, -EACCES},
        Request{2, vm::PROT_READ, vm::MAP_SHARED, paging::PAGE_SIZE, -ENODEV},
        Request{2, vm::PROT_READ, vm::MAP_PRIVATE, paging::PAGE_SIZE, -ENODEV},
    };
    uint64_t before{};
    KREQUIRE_TRUE(leaf_snapshot(fixture.subject->pagemap, before));
    for (auto const& request : REQUESTS) {
        auto* file = fixture.open(request.open_flags);
        KREQUIRE_TRUE(file != nullptr);
        vfs::Stat stat{};
        KEXPECT_EQ(vfs::vfs_fstat_file(file, &stat), 0);
        KEXPECT_EQ(stat.st_mode & vfs::S_IFMT, expected_type);
        file->pos = 17;
        auto const CURSOR = fixture.subject->mmap_next.load();
        unsigned const CLOSES = counters.closes;
        KEXPECT_EQ(vmem::selftest_file_allocate(*fixture.subject, ADDRESS, request.length, request.prot, request.flags, fixture.fd, 0),
                   static_cast<uint64_t>(request.error));
        uint64_t after{};
        KEXPECT_TRUE(leaf_snapshot(fixture.subject->pagemap, after));
        KEXPECT_EQ(after, before);
        KEXPECT_EQ(virt::translate(fixture.subject->pagemap, ADDRESS), physical(fixture.marker));
        KEXPECT_FALSE(virt::is_page_mapped_or_reserved(fixture.subject->pagemap, ADDRESS + paging::PAGE_SIZE));
        KEXPECT_EQ(phys::page_ref_get(fixture.marker), 2U);
        KEXPECT_EQ(file->refcount.load(), 1);
        KEXPECT_EQ(file->pos, 17);
        KEXPECT_EQ(fixture.subject->mmap_next.load(), CURSOR);
        KEXPECT_TRUE(fixture.subject->lazy_vmem_ranges.empty());
        KEXPECT_EQ(counters.reads, 0U);
        KEXPECT_EQ(counters.closes, CLOSES);
        bool intact = true;
        for (size_t i = 0; i < paging::PAGE_SIZE; ++i) intact = intact && static_cast<const uint8_t*>(fixture.marker)[i] == MARKER;
        KEXPECT_TRUE(intact);
        fixture.close();
        KEXPECT_EQ(counters.closes, CLOSES + 1);
    }
    KEXPECT_EQ(counters.opens, static_cast<unsigned>(REQUESTS.size()));
    KEXPECT_EQ(counters.closes, counters.opens);
}
}  // namespace

KTEST(DeviceMmap, CharacterRefusalKeepsOriginalMappingAndOpen) { check_refusal(character, vfs::S_IFCHR); }

KTEST(DeviceMmap, BlockRefusalKeepsOriginalMappingAndOpen) { check_refusal(block, vfs::S_IFBLK); }
