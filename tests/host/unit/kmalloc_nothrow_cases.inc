// Actual slab/mini-malloc and small kmalloc bodies; physical/IRQ/debug endpoints
// are portable ports. No kernel scheduler, GS, physical allocator or native OOM
// execution is implied by these checks.
#include <algorithm>
#include <array>
#include <atomic>
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <platform/mm/page_alloc.hpp>
#include <string_view>

#define CHECK(condition)                                                               \
    do {                                                                               \
        if (!(condition)) {                                                            \
            std::fprintf(stderr, "CHECK %s:%d: %s\n", __FILE__, __LINE__, #condition); \
            std::abort();                                                              \
        }                                                                              \
    } while (false)

namespace host_slab {
struct Backing {
    void* address{};
    size_t bytes{};
    bool slab{};
};
std::array<Backing, 128> backing{};
unsigned lock_depth{}, required_calls{}, may_fail_calls{}, marks{}, releases{}, logs{}, histories{}, recorded_count{};
unsigned debug_registrations{}, debug_releases{}, unpoisons{}, poisons{}, cases{};
unsigned metadata_attempt{}, deny_metadata_attempt{};
bool deny_refill{}, deny_metadata{}, irq_enabled{true};
void (*during_refill)(){};
void* race_block{};

auto owned(uintptr_t address) -> bool {
    for (auto const& row : backing)
        if (row.address && address >= reinterpret_cast<uintptr_t>(row.address) &&
            address - reinterpret_cast<uintptr_t>(row.address) < row.bytes)
            return true;
    return false;
}
auto active(bool slabs) -> size_t {
    size_t count = 0;
    for (auto const& row : backing) count += row.address && ((row.bytes > 4096) == slabs);
    return count;
}
auto allocate(size_t bytes, bool may_fail) -> void* {
    if (may_fail)
        ++may_fail_calls;
    else
        ++required_calls;
    bool const REFILL = bytes > 4096;
    CHECK(lock_depth == (REFILL ? 0U : 1U));
    if (REFILL && during_refill) {
        auto* callback = during_refill;
        during_refill = nullptr;
        callback();
    }
    bool const DENIED = REFILL ? deny_refill : (deny_metadata || ++metadata_attempt == deny_metadata_attempt);
    if (DENIED) return nullptr;  // Direct-null port; native default phys instead enters terminal OOM dumping.
    void* address = std::aligned_alloc(4096, (bytes + 4095) & ~size_t{4095});
    CHECK(address);
    std::memset(address, 0, bytes);
    for (auto& row : backing)
        if (!row.address) {
            row = {address, bytes, false};
            return address;
        }
    CHECK(false);
    return nullptr;
}
void release(void* address) {
    CHECK(lock_depth == 0);
    for (auto& row : backing)
        if (row.address == address) {
            CHECK(!row.slab);
            row = {};
            ++releases;
            std::free(address);
            return;
        }
    CHECK(false);
}
void cleanup_metadata() {
    CHECK(lock_depth == 0 && active(true) == 0);
    for (auto& row : backing)
        if (row.address) {
            row.slab = false;
            release(row.address);
        }
}
void clear_observations() {
    CHECK(lock_depth == 0);
    required_calls = may_fail_calls = marks = releases = logs = histories = recorded_count = 0;
    debug_registrations = debug_releases = unpoisons = poisons = metadata_attempt = deny_metadata_attempt = 0;
    deny_refill = deny_metadata = false;
    during_refill = nullptr;
}
void log(const char* format, ...) {
    ++logs;
    if (std::strstr(format, "last_recorded_free:") != nullptr) {
        va_list args;
        va_start(args, format);
        CHECK(va_arg(args, void*) != nullptr);
        recorded_count = va_arg(args, unsigned);
        va_end(args);
        ++histories;
    }
}
auto save_irq() -> uint64_t {
    bool const OLD = irq_enabled;
    irq_enabled = false;
    return OLD ? 0x200 : 0;
}
void restore_irq() { irq_enabled = true; }
}  // namespace host_slab

namespace ker::mod::dbg {
using host_slab::log;
[[noreturn]] inline void panic_handler(const char*) { std::abort(); }
namespace emergency_serial_unlocked {
inline void write(const char*) {}
inline void write_hex(uint64_t) {}
}  // namespace emergency_serial_unlocked
}  // namespace ker::mod::dbg
namespace ker::mod::sys {
class Spinlock {
    bool held_{};

   public:
    void lock() {
        CHECK(!held_);
        held_ = true;
        ++host_slab::lock_depth;
    }
    void unlock() {
        CHECK(held_ && host_slab::lock_depth);
        held_ = false;
        --host_slab::lock_depth;
    }
};
}  // namespace ker::mod::sys
namespace ker::mod::mm::phys {
auto page_alloc(PhysicalPageOwner owner, uint64_t bytes, std::string_view) -> void* {
    CHECK(owner == PhysicalPageOwner::KMALLOC_SLAB);
    return host_slab::allocate(bytes, false);
}
auto page_alloc_may_fail(PhysicalPageOwner owner, uint64_t bytes, std::string_view) -> void* {
    CHECK(owner == PhysicalPageOwner::KMALLOC_SLAB);
    return host_slab::allocate(bytes, true);
}
auto page_mark_kind(void* address, PageKind kind) -> bool {
    CHECK(address);
    for (auto& row : host_slab::backing)
        if (row.address == address) {
            row.slab = kind == PageKind::SLAB;
            ++host_slab::marks;
            return true;
        }
    CHECK(false);
    return false;
}
void page_free(void* address) { host_slab::release(address); }
}  // namespace ker::mod::mm::phys
extern "C" char __executable_start, end;
namespace ker::mod::mm::kernel_image {
auto contains(uintptr_t address) -> bool {
    return (address >= reinterpret_cast<uintptr_t>(&__executable_start) && address < reinterpret_cast<uintptr_t>(&end)) ||
           host_slab::owned(address);
}
}  // namespace ker::mod::mm::kernel_image
namespace ker::mod::cpu {
constexpr uint64_t GATE_IF_MASK = 0x200;
}
namespace ker::mod::mm::kasan {
inline void unpoison_range(void*, uint64_t) { ++host_slab::unpoisons; }
constexpr int SHADOW_HEAP_FREED = -1;
inline void poison_range(void*, uint64_t, int) { ++host_slab::poisons; }
}  // namespace ker::mod::mm::kasan

// GENERATED_ACTUAL_BODIES

namespace host_slab {
namespace mini = ker::mod::mm::mini_malloc;
namespace km = ker::mod::mm::dyn::kmalloc;
using Policy = mini::AllocationFailurePolicy;
constexpr std::array<size_t, 9> SIZES{0x10, 0x20, 0x40, 0x80, 0x100, 0x200, 0x300, 0x400, 0x800};

auto stats(size_t bytes) -> mini::MiniSlabStats {
    std::array<mini::MiniSlabStats, 9> rows{};
    CHECK(mini::mini_collect_slab_stats(rows.data(), rows.size()) == rows.size());
    for (auto const& row : rows)
        if (row.object_size == bytes) return row;
    CHECK(false);
    return {};
}
void begin_case(const char* name) {
    cleanup_metadata();
    mini::mini_malloc_init();
    clear_observations();
    ++cases;
    std::printf("CASE %s\n", name);
    std::fflush(stdout);
}
auto fill_root(size_t bytes, std::array<void*, 256>& pointers) -> size_t {
    size_t const COUNT = stats(bytes).total_blocks;
    CHECK(COUNT > 0 && COUNT < pointers.size());
    for (size_t i = 0; i < COUNT; ++i) {
        pointers[i] = mini::mini_malloc(bytes, Policy::MAY_FAIL);
        CHECK(pointers[i] && reinterpret_cast<uintptr_t>(pointers[i]) % 16 == 0);
        CHECK(mini::mini_get_slab_size(pointers[i]) == bytes);
    }
    CHECK(stats(bytes).free_blocks == 0);
    return COUNT;
}
void free_root(std::array<void*, 256>& pointers, size_t count) {
    for (size_t i = 0; i < count; ++i)
        if (pointers[i]) {
            mini::mini_free(pointers[i]);
            pointers[i] = nullptr;
        }
    CHECK(active(true) == 0 && lock_depth == 0);
}
void denial_and_recovery(size_t bytes) {
    begin_case("class denial and recovery");
    CHECK(mini::mini_malloc(0, Policy::MAY_FAIL) == nullptr && mini::mini_malloc(2049, Policy::MAY_FAIL) == nullptr);
    std::array<void*, 256> pointers{};
    size_t const COUNT = fill_root(bytes, pointers);
    auto const BEFORE = stats(bytes);
    deny_refill = true;
    CHECK(mini::mini_malloc(bytes, Policy::MAY_FAIL) == nullptr);
    auto const AFTER = stats(bytes);
    CHECK(AFTER.slab_count == BEFORE.slab_count && AFTER.total_blocks == BEFORE.total_blocks && AFTER.free_blocks == 0);
    CHECK(required_calls == 0 && may_fail_calls == 1 && marks == 0 && releases == 0 && logs == 0);
    deny_refill = false;
    void* extra = mini::mini_malloc(bytes, Policy::MAY_FAIL);
    CHECK(extra && stats(bytes).slab_count == 2 && active(true) == 1);
    mini::mini_free(extra);
    CHECK(stats(bytes).slab_count == 1 && active(true) == 0);
    free_root(pointers, COUNT);
    CHECK(stats(bytes).free_blocks == BEFORE.total_blocks);
}
void history() {
#ifdef WOS_KMALLOC_DEBUG_INFO
    begin_case("optional history is a recorded suffix");
    std::array<void*, 256> pointers{};
    size_t const COUNT = fill_root(0x800, pointers);
    void* object = pointers[0];
    deny_metadata = true;
    mini::mini_free(object);
    CHECK(stats(0x800).free_blocks == 1 && active(false) == 0 && metadata_attempt == 0);
    CHECK(mini::mini_malloc(0x800, Policy::MAY_FAIL) == object);
    deny_metadata = false;
    deny_metadata_attempt = 2;
    mini::mini_free(object);
    CHECK(active(false) == 1 && metadata_attempt == 2);
    for (auto const& row : backing)
        if (row.address)
            for (size_t i = 0; i < row.bytes; ++i) CHECK(static_cast<unsigned char*>(row.address)[i] == 0);
    CHECK(mini::mini_malloc(0x800, Policy::MAY_FAIL) == object);
    deny_metadata = true;
    mini::mini_free(object);
    CHECK(active(false) == 1);
    CHECK(mini::mini_malloc(0x800, Policy::MAY_FAIL) == object);
    deny_metadata = false;
    deny_metadata_attempt = 0;
    mini::mini_free(object);
    CHECK(active(false) == 2);
    CHECK(mini::mini_malloc(0x800, Policy::MAY_FAIL) == object);
    mini::mini_free(object);
    unsigned const BEFORE_LOGS = histories;
    mini::mini_free(object);  // Actual invalid-free path diagnoses, skips actual release.
    CHECK(histories == BEFORE_LOGS + 1 && recorded_count == 2 && stats(0x800).free_blocks == 1);
    pointers[0] = nullptr;
    free_root(pointers, COUNT);
#endif
}
void partial_retirement(unsigned denied_attempt) {
    begin_case("retire independent optional arrays");
    std::array<void*, 256> pointers{};
    size_t const COUNT = fill_root(0x800, pointers);
    void* extra = mini::mini_malloc(0x800, Policy::MAY_FAIL);
    CHECK(extra);
    deny_metadata = denied_attempt == 3;
    deny_metadata_attempt = denied_attempt;
    mini::mini_free(extra);
    CHECK(active(true) == 0 && active(false) == 0 && stats(0x800).slab_count == 1);
    deny_metadata = true;
    free_root(pointers, COUNT);
}
void free_during_refill() {
    CHECK(lock_depth == 0 && race_block);
    mini::mini_free(race_block);
}
void spare_race() {
    begin_case("refill recheck frees unused spare outside lock");
    std::array<void*, 256> pointers{};
    size_t const COUNT = fill_root(0x800, pointers);
    race_block = pointers[0];
    during_refill = free_during_refill;
    deny_metadata = true;
    void* returned = mini::mini_malloc(0x800, Policy::MAY_FAIL);
    CHECK(returned == race_block && active(true) == 0 && releases == 1 && stats(0x800).slab_count == 1);
    race_block = nullptr;
    free_root(pointers, COUNT);
}
void small_wrapper_and_magazine() {
    begin_case("actual small kmalloc wrappers and magazine overflow");
    CHECK(km::malloc_may_fail(0) == nullptr);
    std::array<void*, 256> pointers{};
    size_t const COUNT = fill_root(0x800, pointers);
    deny_refill = true;
    CHECK(allocate_scalar_nothrow(0x800, std::nothrow) == nullptr);
    CHECK(allocate_array_nothrow(0x800, std::nothrow) == nullptr);
    CHECK(required_calls == 0 && may_fail_calls == 2 && debug_registrations == 0 && unpoisons == 0);
    deny_refill = false;
    free_root(pointers, COUNT);
    // The generated IRQ statements are explicit Host save/restore ports; every
    // surrounding instruction and the complete actual small free tail is real.
    km::PerCpuAllocator allocator{};
    allocator.initialized = true;
    km::per_cpu_allocators = &allocator;
    km::per_cpu_ready.store(true);
    std::array<void*, 33> objects{};
    for (auto& object : objects) {
        object = allocate_scalar_nothrow(0x800, std::nothrow);
        CHECK(object);
    }
    for (auto* object : objects) km::free(object);
    CHECK(allocator.mag_count[8] == 1 && irq_enabled);
    unsigned const BEFORE = may_fail_calls;
    void* cached = allocate_array_nothrow(0x800, std::nothrow);
    CHECK(cached == objects.back() && may_fail_calls == BEFORE && allocator.mag_count[8] == 0 && irq_enabled);
    km::free(cached);
    auto* retained = allocator.magazine[8][0];
    allocator.mag_count[8] = 0;
    km::per_cpu_ready.store(false);
    km::per_cpu_allocators = nullptr;
    mini::mini_free(retained);
    CHECK(active(true) == 0);
}
}  // namespace host_slab
int main(int argc, char** argv) {
    using namespace host_slab;
    if (argc > 1 && std::strcmp(argv[1], "required-null") == 0) {
        begin_case("local required direct-null provider guard");
        std::array<void*, 256> pointers{};
        (void)fill_root(0x800, pointers);
        deny_refill = true;
        CHECK(mini::mini_malloc(0x800) == nullptr);
        CHECK(required_calls == 1 && logs > 0);
        return 0;
    }
    if (argc > 1 && std::strcmp(argv[1], "corrupt") == 0) {
        begin_case("corruption stays fatal in every policy");
        void* object = mini::mini_malloc(0x800, Policy::MAY_FAIL);
        auto* block = reinterpret_cast<MemoryBlock<0>*>(static_cast<char*>(object) - MemoryBlock<0>::DATA_OFFSET);
        reinterpret_cast<SlabHeader<1, 1>*>(block->slab_ptr)->magic = 0;
        (void)mini::mini_malloc(0x800, Policy::MAY_FAIL);
        CHECK(false);
    }
    for (auto bytes : SIZES) denial_and_recovery(bytes);
    history();
    for (unsigned denied = 0; denied < 4; ++denied) partial_retirement(denied);
    spare_race();
    small_wrapper_and_magazine();
    cleanup_metadata();
    CHECK(lock_depth == 0 && active(true) == 0 && active(false) == 0);
    std::printf("PASS cases=%u\n", cases);
    return 0;
}
