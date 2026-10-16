// Actual swap transactions/facade with finite block/queue/owner endpoints.
// This does not manufacture a current Task, CPL3 frame or scheduled success.
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <dev/block_device.hpp>
#include <dev/block_request.hpp>
#include <memory>
#include <platform/mm/swap.hpp>
#include <platform/sys/mutex.hpp>
#include <span>
#include <test/anonymous_usercopy_wire.hpp>
#include <test/fault_block_device_impl.hpp>
#include <util/crc32c.hpp>
#include <util/smallvec.hpp>

namespace ker::mod::mm::paging {
constexpr size_t PAGE_SIZE = 4096;
}
namespace fixture {
unsigned checks{}, cases{}, mutex_admissions{}, mutex_releases{}, closed_files{};
bool registered{}, configured{};
void (*before_callback)(){};
void (*after_mutex_admission)(){};
void check(bool value, const char* why) {
    ++checks;
    if (!value) {
        std::fprintf(stderr, "FAIL %s\n", why);
        std::exit(1);
    }
}
}  // namespace fixture
namespace ker::vfs {
struct File {};
void vfs_close_file(File*) { ++fixture::closed_files; }
}  // namespace ker::vfs
namespace ker::mod::sched {
auto hold_kernel_shutdown() -> int { return 0; }
}  // namespace ker::mod::sched
namespace ker::mod::sys {
void Mutex::record_owner(uint64_t) {
    ++fixture::mutex_admissions;
    if (fixture::after_mutex_admission != nullptr) {
        auto function = fixture::after_mutex_admission;
        fixture::after_mutex_admission = nullptr;
        function();
    }
}
void Mutex::clear_owner() { ++fixture::mutex_releases; }
void Mutex::lock() {
    // Sequential fixture admission only; no replacement sleeping algorithm.
    if (!try_lock()) std::abort();
}
ACTUAL_MUTEX
}  // namespace ker::mod::sys
namespace ker::dev {
auto block_device_is_registered(const BlockDevice*) -> bool { return fixture::registered; }
auto block_queue_configured(const BlockDevice*) -> bool { return fixture::configured; }
auto block_device_is_read_only(const BlockDevice* device) -> bool { return device->read_only; }
auto block_queue_available(const BlockDevice*) -> bool { return false; }
auto block_read(BlockDevice* device, uint64_t block, size_t count, void* bytes) -> int {
    if (fixture::before_callback != nullptr) fixture::before_callback();
    return device->read_blocks(device, block, count, bytes);
}
auto block_write(BlockDevice* device, uint64_t block, size_t count, const void* bytes) -> int {
    if (fixture::before_callback != nullptr) fixture::before_callback();
    return device->write_blocks(device, block, count, bytes);
}
auto block_request_submit(BlockDevice*, BlockRequest&, BlockRequestOperation, uint64_t, size_t, std::span<const BlockSegment>, uint64_t,
                          BlockCompletion, void*) -> int {
    std::abort();
}
auto block_request_wait(BlockRequest&) -> int { std::abort(); }
auto block_request_release(BlockRequest&) -> int { std::abort(); }
void block_queue_wait_for_capacity(BlockDevice*) { std::abort(); }
}  // namespace ker::dev
namespace ker::mod::mm::swap {
ACTUAL_SWAP_TYPES
ACTUAL_SWAP_FUNCTIONS
}  // namespace ker::mod::mm::swap
namespace ker::mod::sys::usercopy {
constexpr uint64_t USER_ADDR_LIMIT = 0x0000800000000000ULL;
ACTUAL_RANGE_VALID
}  // namespace ker::mod::sys::usercopy
namespace ker::test::anonymous_usercopy {
namespace mm = ker::mod::mm;
namespace swap = mm::swap;
constexpr uint64_t PAGE_BYTES = 4096, DEVICE_PAGES = 16;
namespace uc = ker::mod::sys::usercopy;
ACTUAL_REQUEST_HELPERS
ACTUAL_DEVICE_HISTORY
ACTUAL_SERIALIZED_FACADE
}  // namespace ker::test::anonymous_usercopy

namespace fixture {
namespace swap = ker::mod::mm::swap;
namespace diagnostic = ker::test::anonymous_usercopy;
using Storage = diagnostic::SerializedFaultStorage;
auto area() -> swap::SwapArea* {
    check(swap::g_swap_areas.size() == 1, "exact owned area");
    return swap::g_swap_areas.at(0);
}
auto expectation() -> swap::SelftestEmptyAreaExpectation { return {"case", area()->extents[0].device, 0, 16}; }
auto fresh() -> std::unique_ptr<Storage> {
    check(swap::g_swap_areas.size() == 0 && !swap::g_swap_lock.is_locked(), "previous descriptor/lock closed");
    registered = configured = false;
    before_callback = after_mutex_admission = nullptr;
    auto storage = std::make_unique<Storage>();
    check(storage->activate("case") == 0, "real area activation");
    ++cases;
    return storage;
}
void io_and_free(swap::SwapSlot slot) {
    std::array<uint8_t, 4096> written{}, read{};
    for (size_t i = 0; i < written.size(); ++i) written[i] = static_cast<uint8_t>((i * 131 + 0x53) & 255);
    check(swap::write_slot(slot, written.data()) == 0 && swap::read_slot(slot, read.data()) == 0 && written == read,
          "real stored write/read CRC and exact pattern");
    check(swap::free_slot(slot) == 0, "real slot release");
}
void empty_detach(Storage& storage) {
    check(storage.detach() == 0 && swap::g_swap_areas.size() == 0, "empty detach unlinks/disposes actual descriptor");
}
void positive() {
    auto storage = fresh();
    diagnostic::DeviceHistory history{};
    check(!storage->final_history(history), "history refused before actual detach");
    swap::SwapSlot slot;
    check(swap::allocate_slot(&slot) == 0, "actual reserved slot");
    auto expected = expectation();
    auto* original = area();
    check(swap::selftest_try_detach_empty_area(expected) == -EBUSY && area() == original && area()->used_pages == 1,
          "occupied detach leaves exact descriptor");
    io_and_free(slot);
    empty_detach(*storage);
    check(storage->final_history(history) && history.exact && history.writes == 1 && history.reads == 1,
          "history authority follows detach and exact two joined operations");
    std::array<uint8_t, 4096> untouched{};
    check(swap::read_slot(slot, untouched.data()) < 0 && swap::g_swap_areas.size() == 0, "stale submit after detach cannot reach callback");
}
void live_io() {
    auto storage = fresh();
    swap::SwapSlot slot;
    check(swap::allocate_slot(&slot) == 0, "I/O slot reserved");
    std::array<uint8_t, 4096> page{};
    before_callback = [] {
        auto* owned = area();
        check(owned->used_pages == 1 && owned->io_in_flight == 1 && owned->slots[0].io_in_flight,
              "actual IO ownership published before provider");
        auto expected = expectation();
        check(swap::selftest_try_detach_empty_area(expected) == -EBUSY, "callback-time detach refusal");
    };
    swap::SlotIo io;
    check(swap::submit_slot_io(io, slot, page.data(), true) == 0, "actual synchronous submit");
    before_callback = nullptr;
    check(io.active && area()->io_in_flight == 1 && swap::free_slot(slot) == -EBUSY, "callback return alone does not release slot IO");
    check(swap::finish_slot_io(io) == 0 && !io.active && area()->io_in_flight == 0, "actual finish retires IO after CRC/request tail");
    check(swap::free_slot(slot) == 0, "post-finish slot free");
    empty_detach(*storage);
}
void exact_identity() {
    auto storage = fresh();
    auto expected = expectation();
    auto* original = area();
    check(swap::selftest_try_detach_empty_area({nullptr, expected.device, 0, 16}) == -EINVAL, "null name refusal");
    check(swap::selftest_try_detach_empty_area({"case", expected.device, 1, 16}) == -EINVAL, "nonzero extent refusal");
    check(swap::selftest_try_detach_empty_area({"case", expected.device, 0, 15}) == -EINVAL, "wrong bounded extent refusal");
    check(swap::selftest_try_detach_empty_area({"other", expected.device, 0, 16}) == -ENOENT, "wrong name refusal");
    ker::dev::BlockDevice foreign{};
    check(swap::selftest_try_detach_empty_area({"case", &foreign, 0, 16}) == -ESTALE && area() == original,
          "exact provider identity required");
    check(swap::g_swap_lock.try_lock(), "hold actual swap mutex");
    check(swap::selftest_try_detach_empty_area(expected) == -EAGAIN, "detach performs one try admission");
    swap::g_swap_lock.unlock();
    area()->state = swap::AreaState::DRAINING;
    check(swap::selftest_try_detach_empty_area(expected) == -EBUSY, "DRAINING never detached");
    area()->state = swap::AreaState::ACTIVE;
    ker::vfs::File backing;
    area()->backing_file = &backing;
    check(swap::selftest_try_detach_empty_area(expected) == -ESTALE && closed_files == 0, "backing ownership refused without close");
    area()->backing_file = nullptr;
    empty_detach(*storage);
}
void corrupt_free_state() {
    auto storage = fresh();
    auto expected = expectation();
    area()->used_pages = 1;
    check(swap::selftest_try_detach_empty_area(expected) == -EBUSY, "used ownership cannot be hidden by free counters");
    area()->used_pages = 0;
    area()->slots[0].io_in_flight = true;
    check(swap::selftest_try_detach_empty_area(expected) == -EBUSY, "every slot IO flag checked");
    area()->slots[0].io_in_flight = false;
    auto const NEXT = area()->slots[0].next_free;
    area()->slots[0].next_free = 0;
    check(swap::selftest_try_detach_empty_area(expected) == -EIO, "free chain duplicate refusal");
    area()->slots[0].next_free = 16;
    check(swap::selftest_try_detach_empty_area(expected) == -EIO, "free chain bounds refusal");
    area()->slots[0].next_free = NEXT;
    area()->slots[15].state = swap::SlotState::RETIRED;
    check(swap::selftest_try_detach_empty_area(expected) == -EIO, "all sixteen free states required");
    area()->slots[15].state = swap::SlotState::FREE;
    empty_detach(*storage);
}
void extra_activity(bool flush) {
    auto storage = fresh();
    swap::SwapSlot slot;
    check(swap::allocate_slot(&slot) == 0, "foreign history slot");
    io_and_free(slot);
    auto* provider = area()->extents[0].device;
    std::array<uint8_t, 4096> bytes{};
    check((flush ? provider->flush(provider) : provider->write_blocks(provider, 0, 1, bytes.data())) == 0, "actual unexpected callback");
    empty_detach(*storage);
    diagnostic::DeviceHistory history{};
    check(storage->final_history(history) && !history.exact, "extra foreign/flush event never qualifies");
}
ker::dev::BlockDevice* reentry_provider{};
void overlap() {
    auto storage = fresh();
    swap::SwapSlot slot;
    check(swap::allocate_slot(&slot) == 0, "overlap slot");
    reentry_provider = area()->extents[0].device;
    before_callback = [] {
        before_callback = nullptr;
        after_mutex_admission = [] { check(reentry_provider->flush(reentry_provider) == -EBUSY, "all callbacks use one nonwaiting gate"); };
    };
    io_and_free(slot);
    empty_detach(*storage);
    diagnostic::DeviceHistory history{};
    check(storage->final_history(history) && history.events == 2 && history.overlap == 1 && !history.exact,
          "missing history event still records sticky overlap refusal");
    reentry_provider = nullptr;
}
void request_geometry() {
    using Request = diagnostic::Request;
    Request request{.version = 1,
                    .bytes = 80,
                    .case_id = 0,
                    .flags = 0,
                    .nonce = 7,
                    .address = 0x4000,
                    .length = 4096,
                    .fd = 3,
                    .timeout_address = 0,
                    .output_address = 0x9000,
                    .output_bytes = 256,
                    .reserved = 0};
    check(diagnostic::request_valid(request, 0x8000), "exact bounded write request");
    for (unsigned index = 0; index < 4; ++index) {
        auto candidate = request;
        candidate.case_id = index;
        candidate.length = index == 2 ? 8192 : 4096;
        if (index == 3) {
            candidate.fd = -1;
            candidate.timeout_address = 0xa000;
        }
        check(diagnostic::request_valid(candidate, 0x8000), "four exact request geometries");
    }
    for (unsigned field = 0; field < 7; ++field) {
        auto candidate = request;
        switch (field) {
            case 0:
                candidate.version = 2;
                break;
            case 1:
                candidate.bytes = 79;
                break;
            case 2:
                candidate.case_id = 4;
                break;
            case 3:
                candidate.flags = 1;
                break;
            case 4:
                candidate.reserved = 1;
                break;
            case 5:
                candidate.nonce = 0;
                break;
            case 6:
                candidate.length = 8192;
                break;
        }
        check(!diagnostic::request_valid(candidate, 0x8000), "unknown/unused request scalar refusal");
    }
    auto bad = request;
    bad.output_address = 0x4000;
    check(!diagnostic::request_valid(bad, 0x8000), "selected mapping cannot overlap output");
    bad = request;
    bad.address = 0x7ffffffff000ULL;
    bad.length = 8192;
    bad.case_id = 2;
    check(!diagnostic::request_valid(bad, 0x8000), "checked range end refusal");
    bad = request;
    bad.case_id = 3;
    bad.fd = -1;
    bad.timeout_address = 0x4000;
    check(!diagnostic::request_valid(bad, 0x8000), "timeout cannot overlap consumed mapping");
    ++cases;
}
void provider_escape() {
    check(swap::g_swap_areas.size() == 0, "provider rejection starts empty");
    auto storage = std::make_unique<Storage>();
    registered = true;
    check(storage->activate("case") == -EINVAL && swap::g_swap_areas.size() == 0, "registered device never admitted");
    registered = false;
    configured = true;
    check(storage->activate("case") == -EINVAL && swap::g_swap_areas.size() == 0, "configured queue never admitted");
    configured = false;
    ++cases;
}
}  // namespace fixture
auto main() -> int {
    fixture::positive();
    fixture::live_io();
    fixture::exact_identity();
    fixture::corrupt_free_state();
    fixture::extra_activity(false);
    fixture::extra_activity(true);
    fixture::overlap();
    fixture::provider_escape();
    fixture::request_geometry();
    fixture::check(
        fixture::swap::g_swap_areas.size() == 0 && fixture::closed_files == 0 && fixture::mutex_admissions == fixture::mutex_releases,
        "all exact slot/descriptor/lock owners closed");
    std::printf("PASS actual scheduled diagnostic storage cases=%u checks=%u\n", fixture::cases, fixture::checks);
}
