#!/usr/bin/env python3
"""Run actual cache allocation/free bodies at instrumented native boundaries."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[3]


def function(source, signature):
    start = source.index(signature)
    opening = source.index("{", start)
    clean = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"',
                   lambda match: " " * len(match[0]), source)
    depth = 0
    for index in range(opening, len(source)):
        if clean[index] == "{":
            depth += 1
        elif clean[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start:index + 1]
    raise ValueError(f"unterminated actual function: {signature}")


ADAPTER = r'''
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <string_view>
#include <dev/block_request.hpp>

#ifdef WOS_HOST_TEST
#error This harness must exercise native allocator and vmap branches.
#endif

namespace fixture {
enum class Kind { HEAP, BUDDY, VMAP };
struct Allocation { void* data{}; size_t bytes{}; Kind kind{}; };
std::array<Allocation, 16> owned{};
std::array<Kind, 16> attempts{};
size_t attempt_count{}, buddy_calls{}, vmap_calls{}, heap_calls{}, free_calls{}, live_bytes{};
bool fail_buddy{}, fail_vmap{};
bool descriptor_mode{}, fail_metadata{}, fail_descriptor_heap{};
size_t metadata_calls{};
const char* case_name{};
size_t cases{};

void require(bool condition, const char* message) {
    if (!condition) {
        std::fprintf(stderr, "allocation contract failed case=%s: %s\n", case_name, message);
        std::exit(1);
    }
}
void attempt(Kind kind, size_t bytes) {
    require(attempt_count < attempts.size(), "bounded attempt trace");
    attempts[attempt_count++] = kind;
    require(bytes <= 4 * 1024 * 1024, "allocator must not receive above-ceiling request");
}
void* allocate(Kind kind, size_t bytes) {
    void* data = std::malloc(std::max(bytes, size_t{1}));
    require(data != nullptr, "host allocation boundary");
    std::memset(data, 0xa7, std::max(bytes, size_t{1}));
    for (auto& entry : owned) {
        if (!entry.data) {
            entry = {data, bytes, kind};
            live_bytes += bytes;
            return data;
        }
    }
    require(false, "bounded ownership table");
    return nullptr;
}
void release(void* data, Kind kind, size_t bytes, bool verify_size) {
    for (auto& entry : owned) {
        if (entry.data == data && data != nullptr) {
            require(entry.kind == kind, "backing freed through wrong allocator");
            require(!verify_size || entry.bytes == bytes, "vmap free must retain exact original size");
            live_bytes -= entry.bytes;
            entry = {};
            ++free_calls;
            std::free(data);
            return;
        }
    }
    require(false, "unknown or duplicate backing free");
}
void reset(const char* name, bool buddy_failure = false, bool vmap_failure = false) {
    case_name = name;
    require(std::all_of(owned.begin(), owned.end(), [](const auto& entry) { return entry.data == nullptr; }),
            "previous allocation not retired");
    require(live_bytes == 0, "physical bytes leaked");
    attempt_count = buddy_calls = vmap_calls = heap_calls = free_calls = 0;
    metadata_calls = 0;
    descriptor_mode = fail_metadata = fail_descriptor_heap = false;
    fail_buddy = buddy_failure;
    fail_vmap = vmap_failure;
}
}  // namespace fixture

void* operator new(size_t bytes, const std::nothrow_t&) noexcept {
    fixture::require(fixture::descriptor_mode, "unexpected descriptor heap admission");
    ++fixture::heap_calls;
    fixture::attempt(fixture::Kind::HEAP, bytes);
    return fixture::fail_descriptor_heap ? nullptr : fixture::allocate(fixture::Kind::HEAP, bytes);
}
void operator delete(void* data) noexcept {
    if (data) fixture::release(data, fixture::Kind::HEAP, 0, false);
}
void operator delete(void* data, size_t) noexcept { ::operator delete(data); }
void operator delete(void* data, const std::nothrow_t&) noexcept { ::operator delete(data); }

void* operator new[](size_t bytes) {
    ++fixture::heap_calls;
    fixture::attempt(fixture::Kind::HEAP, bytes);
    return fixture::allocate(fixture::Kind::HEAP, bytes);
}
void operator delete[](void* data) noexcept {
    if (data) fixture::release(data, fixture::Kind::HEAP, 0, false);
}

namespace ker::mod::mm {
@OWNER_ENUM@
namespace paging { constexpr size_t PAGE_SIZE = 4096; }
namespace phys {
auto page_alloc_full_overwrite_may_fail(PhysicalPageOwner owner, uint64_t bytes, std::string_view name) -> void* {
    if (fixture::descriptor_mode) {
        fixture::require(owner == PhysicalPageOwner::BUFFER_CACHE_METADATA && name == "buffer_cache_writeback",
                         "descriptor metadata owner/site");
        ++fixture::metadata_calls;
        fixture::attempt(fixture::Kind::BUDDY, bytes);
        return fixture::fail_metadata ? nullptr : fixture::allocate(fixture::Kind::BUDDY, bytes);
    }
    fixture::require(owner == PhysicalPageOwner::BUFFER_CACHE_DATA && name == "buffer_cache", "buddy owner/site");
    ++fixture::buddy_calls;
    fixture::attempt(fixture::Kind::BUDDY, bytes);
    return fixture::fail_buddy ? nullptr : fixture::allocate(fixture::Kind::BUDDY, bytes);
}
void page_free(void* data) { fixture::release(data, fixture::Kind::BUDDY, 0, false); }
}  // namespace phys
namespace virt {
auto kernel_vmap_alloc(PhysicalPageOwner owner, uint64_t bytes, std::string_view name) -> void* {
    fixture::require(owner == PhysicalPageOwner::BUFFER_CACHE_DATA && name == "buffer_cache", "vmap owner/site");
    ++fixture::vmap_calls;
    fixture::attempt(fixture::Kind::VMAP, bytes);
    return fixture::fail_vmap ? nullptr : fixture::allocate(fixture::Kind::VMAP, bytes);
}
void kernel_vmap_free(void* data, uint64_t bytes) { fixture::release(data, fixture::Kind::VMAP, bytes, true); }
}  // namespace virt
}  // namespace ker::mod::mm

namespace ker::vfs {
@FLAGS@
@CONSTANTS@
@ALLOCATE@
@FREE@
struct BufHead;
@DESCRIPTOR_STRUCTS@
@ALLOCATE_DESCRIPTOR@
@DESTROY_DESCRIPTOR@
}  // namespace ker::vfs

namespace fixture {
using namespace ker::vfs;
constexpr uint32_t PRESERVED = BH_DIRTY | BH_VALID | BH_LOCKED | BH_WRITEBACK | BH_DIRTY_INDEXED |
                               BH_RANGE_INDEXED | BH_LRU_REFERENCED | BH_FILE_DATA;
constexpr uint32_t STORAGE = BH_DATA_PAGE_ALLOC | BH_DATA_VMAP;

void check_case(const char* name, size_t bytes, size_t buddies, size_t vmaps, size_t heaps,
                bool succeeds, Kind backing, bool buddy_failure = false, bool vmap_failure = false) {
    reset(name, buddy_failure, vmap_failure);
    uint32_t flags = PRESERVED | STORAGE;
    auto* data = allocate_buffer_data(bytes, flags);
    require(buddy_calls == buddies && vmap_calls == vmaps && heap_calls == heaps, "unexpected allocator attempts");
    require(attempt_count == buddies + vmaps + heaps, "unexpected retries");
    if (buddies && vmaps) require(attempts[0] == Kind::BUDDY && attempts[1] == Kind::VMAP, "fallback order");
    require((flags & ~STORAGE) == PRESERVED, "unrelated buffer flags changed");
    require((data != nullptr) == succeeds, "success/failure mismatch");
    uint32_t const expected_flags = !succeeds || backing == Kind::HEAP ? 0 :
                                    backing == Kind::BUDDY ? BH_DATA_PAGE_ALLOC : BH_DATA_VMAP;
    require((flags & STORAGE) == expected_flags, "stale or wrong backing flags");
    if (data) {
        require(live_bytes == bytes, "hidden allocation rounding");
        require(std::all_of(data, data + bytes, [](uint8_t byte) { return byte == 0xa7; }),
                "full-overwrite allocator data was zeroed or altered");
        std::memset(data, 0x5c, bytes);
    }
    free_data_buffer(data, flags, bytes);
    require(free_calls == (succeeds ? 1U : 0U) && live_bytes == 0, "allocation cleanup");
    ++cases;
}

void retained_batch() {
    reset("eight simultaneously retained maximum-size backings");
    std::array<uint8_t*, 8> data{};
    std::array<uint32_t, 8> flags{};
#ifdef WOS_ASYNC_BLOCK_IO
    constexpr size_t BYTES = 4 * 1024 * 1024;
    constexpr Kind BACKING = Kind::BUDDY;
#else
    constexpr size_t BYTES = 2 * 1024 * 1024;
    constexpr Kind BACKING = Kind::VMAP;
#endif
    for (size_t index = 0; index < data.size(); ++index) {
        flags[index] = PRESERVED | STORAGE;
        data[index] = allocate_buffer_data(BYTES, flags[index]);
        require(data[index] != nullptr && live_bytes == (index + 1) * BYTES, "bounded retained backing bytes");
        require(owned[index].data == data[index] && owned[index].kind == BACKING, "batch backing choice");
        std::memset(data[index], static_cast<int>(index + 1), BYTES);
    }
    require(attempt_count == data.size(), "one attempt per successful retained allocation");
    for (size_t index = data.size(); index != 0; --index) {
        require(data[index - 1][0] == index && data[index - 1][BYTES - 1] == index, "retained buffer independence");
        free_data_buffer(data[index - 1], flags[index - 1], BYTES);
    }
    require(free_calls == data.size() && live_bytes == 0, "retained batch retired");
    ++cases;
}

void initialized_descriptor(const DirtyWritebackIo& io, bool physical) {
    require(std::all_of(io.run.buffers.begin(), io.run.buffers.end(), [](auto* value) { return value == nullptr; }),
            "poisoned buffer references were not initialized");
    require(std::all_of(io.run.dirty_epochs.begin(), io.run.dirty_epochs.end(), [](auto value) { return value == 0; }),
            "poisoned dirty generations were not initialized");
    require(io.run.bdev == nullptr && io.run.block_no == 0 && io.run.block_count == 0 && io.run.bytes == 0 &&
            io.run.count == 0 && io.run.writeback_epoch == 0, "run scalar initialization");
    require(io.snapshot.data == nullptr && io.snapshot.flags == 0 && io.snapshot.size == 0, "snapshot initialization");
    const auto& request = io.request;
    require(request.description.op == ker::dev::BlockRequestOperation::READ && request.description.lba == 0 &&
            request.description.count == 0 && request.description.bytes == 0, "request description initialization");
    require(std::all_of(request.segments.begin(), request.segments.end(), [](const auto& segment) {
                return segment.data == nullptr && segment.size == 0;
            }), "poisoned SG entries were not initialized");
    require(request.segment_count == 0 && request.token.queue_generation == 0 && request.token.request_generation == 0 &&
            request.token.slot == 0 && request.deadline_us == 0 && request.status == 0 && request.bytes == 0 &&
            !request.completed.load() && request.callback == nullptr && request.callback_arg == nullptr &&
            request.queue == nullptr && request.submitted_us == 0 && request.waiter == nullptr, "request ownership initialization");
    require(io.limits.depth == 1 && io.limits.max_transfer_bytes == ker::dev::BLOCK_MODEL_MAX_REQUEST_BYTES &&
            io.limits.max_segments == ker::dev::BLOCK_MODEL_MAX_SG && io.limits.max_requests == ker::dev::BLOCK_MODEL_MAX_REQUESTS &&
            io.limits.max_inflight_bytes == 8 * 1024 * 1024, "provider limit initialization");
    require(io.offset == 0 && io.submitted_bytes == 0 && io.admission_started_us == 0 && !io.accepted &&
            io.physical_backing == physical && io.active_next == nullptr, "descriptor backing and cursor initialization");
}

void descriptor_case(const char* name, bool physical_failure, bool heap_failure) {
    reset(name);
    descriptor_mode = true;
    fail_metadata = physical_failure;
    fail_descriptor_heap = heap_failure;
    auto* io = allocate_writeback_io();
    require(metadata_calls == 1 && heap_calls == (physical_failure ? 1U : 0U), "bounded metadata attempt and immediate fallback");
    require(attempts[0] == Kind::BUDDY && (!physical_failure || attempts[1] == Kind::HEAP), "descriptor fallback order");
    bool const succeeds = !physical_failure || !heap_failure;
    require((io != nullptr) == succeeds, "descriptor success/failure");
    if (io != nullptr) {
        initialized_descriptor(*io, !physical_failure);
        require(live_bytes == sizeof(DirtyWritebackIo), "descriptor logical size");
        destroy_writeback_io(io);
    }
    require(free_calls == (succeeds ? 1U : 0U) && live_bytes == 0, "descriptor exact matching free");
    ++cases;
}

void retained_descriptors() {
    reset("eight simultaneously retained poisoned metadata descriptors");
    descriptor_mode = true;
    std::array<DirtyWritebackIo*, 8> entries{};
    for (size_t index = 0; index < entries.size(); ++index) {
        entries[index] = allocate_writeback_io();
        require(entries[index] != nullptr, "retained descriptor admission");
        initialized_descriptor(*entries[index], true);
        entries[index]->run.block_no = index + 1;
        require(live_bytes == (index + 1) * sizeof(DirtyWritebackIo), "bounded metadata retained bytes");
    }
    require(metadata_calls == entries.size() && heap_calls == 0, "retained metadata attempt count");
    for (size_t index = entries.size(); index != 0; --index) {
        require(entries[index - 1]->run.block_no == index, "retained descriptor independence");
        destroy_writeback_io(entries[index - 1]);
    }
    require(free_calls == entries.size() && live_bytes == 0, "all retained descriptors retired");
    ++cases;
}
}  // namespace fixture

int main() {
    using namespace fixture;
    reset("unchanged data and dirty allocation ceilings");
    require(ker::vfs::BUFFER_CACHE_CONTIG_ALLOC_MAX_BYTES == 2 * 1024 * 1024, "dirty aggregation ceiling");
#ifdef WOS_ASYNC_BLOCK_IO
    require(ker::vfs::BUFFER_CACHE_DATA_ALLOC_MAX_BYTES == 4 * 1024 * 1024, "ON data ceiling");
#else
    require(ker::vfs::BUFFER_CACHE_DATA_ALLOC_MAX_BYTES == 2 * 1024 * 1024, "OFF data ceiling");
#endif
    ++cases;
    check_case("zero heap compatibility", 0, 0, 0, 1, true, Kind::HEAP);
    check_case("small sector heap", 512, 0, 0, 1, true, Kind::HEAP);
    check_case("small odd heap", 4095, 0, 0, 1, true, Kind::HEAP);
    check_case("one page buddy", 4096, 1, 0, 0, true, Kind::BUDDY);
    check_case("two page buddy", 8192, 1, 0, 0, true, Kind::BUDDY);
    check_case("small nonpower page buddy", 12288, 1, 0, 0, true, Kind::BUDDY);
    check_case("below buddy threshold odd heap", 65535, 0, 0, 1, true, Kind::HEAP);
    check_case("buddy threshold", 65536, 1, 0, 0, true, Kind::BUDDY);
    check_case("one page fragmented fallback", 4096, 1, 1, 0, true, Kind::VMAP, true);
    check_case("buddy threshold fragmented fallback", 65536, 1, 1, 0, true, Kind::VMAP, true);
    check_case("small both allocators fail", 8192, 1, 1, 0, false, Kind::VMAP, true, true);
    check_case("large odd vmap", 65537, 0, 1, 0, true, Kind::VMAP);
    check_case("large odd vmap failure", 65537, 0, 1, 0, false, Kind::VMAP, false, true);
    check_case("large page-aligned nonpower vmap", 98304, 0, 1, 0, true, Kind::VMAP);
    check_case("large page-aligned nonpower failure", 98304, 0, 1, 0, false, Kind::VMAP, true, true);
    check_case("large page-aligned17pages vmap", 69632, 0, 1, 0, true, Kind::VMAP);
#ifdef WOS_ASYNC_BLOCK_IO
    for (size_t bytes : {size_t{128 * 1024}, size_t{512 * 1024}, size_t{1024 * 1024}, size_t{2 * 1024 * 1024}, size_t{4 * 1024 * 1024}}) {
        check_case("large exact buddy preference", bytes, 1, 0, 0, true, Kind::BUDDY);
        check_case("large fragmented immediate vmap", bytes, 1, 1, 0, true, Kind::VMAP, true);
        check_case("large both allocators fail", bytes, 1, 1, 0, false, Kind::VMAP, true, true);
    }
    check_case("three MiB no rounding", 3 * 1024 * 1024, 0, 1, 0, true, Kind::VMAP);
    check_case("two MiB plus page no rounding", 2 * 1024 * 1024 + 4096, 0, 1, 0, true, Kind::VMAP);
    check_case("maximum minus byte vmap", 4 * 1024 * 1024 - 1, 0, 1, 0, true, Kind::VMAP);
#else
    for (size_t bytes : {size_t{128 * 1024}, size_t{512 * 1024}, size_t{1024 * 1024}, size_t{2 * 1024 * 1024}}) {
        check_case("OFF unchanged vmap", bytes, 0, 1, 0, true, Kind::VMAP);
        check_case("OFF no buddy rescue", bytes, 0, 1, 0, false, Kind::VMAP, false, true);
    }
    check_case("OFF ceiling plus page", 2 * 1024 * 1024 + 4096, 0, 0, 0, false, Kind::VMAP);
    check_case("OFF three MiB rejected", 3 * 1024 * 1024, 0, 0, 0, false, Kind::VMAP);
    check_case("OFF four MiB rejected", 4 * 1024 * 1024, 0, 0, 0, false, Kind::VMAP);
#endif
    check_case("ceiling plus byte", ker::vfs::BUFFER_CACHE_DATA_ALLOC_MAX_BYTES + 1, 0, 0, 0, false, Kind::VMAP);
    check_case("size_t maximum", std::numeric_limits<size_t>::max(), 0, 0, 0, false, Kind::VMAP);
    reset("null frees have no allocator side effects");
    ker::vfs::free_data_buffer(nullptr, ker::vfs::BH_DATA_PAGE_ALLOC | ker::vfs::BH_DATA_VMAP, 4096);
    require(free_calls == 0 && attempt_count == 0, "null free");
    ++cases;
    retained_batch();
    descriptor_case("physical descriptor construction", false, false);
    descriptor_case("physical success does not attempt failed heap", false, true);
    descriptor_case("one physical failure uses initialized heap", true, false);
    descriptor_case("both descriptor allocators fail", true, true);
    retained_descriptors();
    std::printf("native_cache_allocation status=pass cases=%zu owned_bytes=%zu host_macro=absent\n", cases, live_bytes);
}
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-source", type=Path, default=ROOT / "modules/kern/src/vfs/buffer_cache.cpp")
    parser.add_argument("--mode", choices=("on", "off", "both"), default="both")
    parser.add_argument("--cxx", default=os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++"))
    args = parser.parse_args()
    source = args.cache_source.read_text()
    header = ROOT / "modules/kern/src/vfs/buffer_cache.hpp"
    flags = "\n".join(re.findall(r"^constexpr uint32_t BH_[^;]*;", header.read_text(), re.M))
    constants = source[source.index("constexpr size_t BUFFER_CACHE_CONTIG_ALLOC_MAX_BYTES"):
                       source.index("constexpr size_t DIRTY_WRITEBACK_RUN_MAX_BYTES")]
    owner_header = ROOT / "modules/kern/src/platform/mm/page_alloc.hpp"
    owner_enum = re.search(r"enum class PhysicalPageOwner[^}]*};", owner_header.read_text())[0]
    adapter = ADAPTER.replace("@OWNER_ENUM@", owner_enum).replace("@FLAGS@", flags).replace("@CONSTANTS@", constants)
    adapter = adapter.replace("@ALLOCATE@", function(source, "auto allocate_buffer_data("))
    adapter = adapter.replace("@FREE@", function(source, "void free_data_buffer("))
    descriptor_structs = []
    for name in ("WritebackSnapshot", "DirtyWritebackRun", "DirtyWritebackIo"):
        match = re.search(rf"struct {name} \{{[\s\S]*?\n\}};", source)
        if match is None:
            raise ValueError(f"missing actual descriptor type: {name}")
        descriptor_structs.append(match[0])
    count = re.search(r"constexpr size_t DIRTY_WRITEBACK_RUN_MAX_BUFFERS = [^;]*;", source)[0]
    adapter = adapter.replace("@DESCRIPTOR_STRUCTS@", count + "\n" + "\n".join(descriptor_structs))
    adapter = adapter.replace("@ALLOCATE_DESCRIPTOR@", function(source, "auto allocate_writeback_io("))
    adapter = adapter.replace("@DESTROY_DESCRIPTOR@", function(source, "void destroy_writeback_io("))
    scratch = Path(os.environ.get("TMPDIR", str(ROOT / "build/goal30-tmp")))
    scratch.mkdir(parents=True, exist_ok=True)
    modes = ("on", "off") if args.mode == "both" else (args.mode,)
    with tempfile.TemporaryDirectory(prefix="cache-allocation-native-", dir=scratch) as temporary:
        work = Path(temporary)
        test = work / "main.cpp"
        test.write_text(adapter)
        environment = dict(os.environ, TMPDIR=str(work))
        environment.setdefault("ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1")
        for mode in modes:
            binary = work / mode
            command = [args.cxx, "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Werror",
                       "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-I", str(ROOT / "modules/kern/src"),
                       str(test), "-o", str(binary)]
            if mode == "on":
                command.append("-DWOS_ASYNC_BLOCK_IO=1")
            subprocess.run(command, check=True, env=environment)
            subprocess.run([str(binary)], check=True, env=environment, timeout=30)
    print(json.dumps({"cache_source_sha256": hashlib.sha256(args.cache_source.read_bytes()).hexdigest(),
                      "cache_header_sha256": hashlib.sha256(header.read_bytes()).hexdigest(),
                      "physical_owner_header_sha256": hashlib.sha256(owner_header.read_bytes()).hexdigest(),
                      "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                      "native_branches": True, "host_macro": False, "modes": modes,
                      "request_header_sha256": hashlib.sha256((ROOT / "modules/kern/src/dev/block_request.hpp").read_bytes()).hexdigest(),
                      "asan_options": environment["ASAN_OPTIONS"], "production_functions": ["allocate_buffer_data", "free_data_buffer",
                          "allocate_writeback_io", "destroy_writeback_io"],
                      "boundary_mocks": ["non-reclaiming physical allocation", "virtual allocation", "matching frees"]}))


if __name__ == "__main__":
    main()
