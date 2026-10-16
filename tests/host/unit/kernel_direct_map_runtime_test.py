#!/usr/bin/env python3
"""Execute real boot-HHDM recognition and mapper entrypoints with Host primitives.

Physical-address translation, IRQ lock, allocation/ref and TLB endpoints are
explicit seams. No WOS boot, hardware TLB, cache-type transition or SMP is run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import tempfile
import time


def function(source: str, signature: str) -> str:
    if source.count(signature) != 1:
        raise RuntimeError(f"nonunique actual function: {signature}")
    start = source.index(signature)
    opening = source.index("{", start)
    depth = 1
    cursor = opening + 1
    while depth:
        if source[cursor] == "{":
            depth += 1
        elif source[cursor] == "}":
            depth -= 1
        cursor += 1
    return source[start:cursor]


PRELUDE = r'''
#include <platform/mm/paging.hpp>
#include <extern/limine.h>
#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
using namespace ker::mod::mm;
using namespace ker::mod::mm::paging;
using vaddr_t = uint64_t;
using paddr_t = uint64_t;
constexpr uint64_t LARGE_PAGE_2M_BYTES = 2 * 1024 * 1024;
constexpr uint64_t PTE_FRAME_MASK = 0x000FFFFFFFFFF000ULL;
struct Refused {};
size_t checks = 0;
#define CHECK(expression) do { ++checks; if (!(expression)) throw std::runtime_error(#expression); } while (false)
struct Lock {
    bool held{};
    uint64_t lock_irqsave() { CHECK(!held); held = true; return 0x202; }
    void unlock_irqrestore(uint64_t saved) { CHECK(held && saved == 0x202); held = false; }
} user_mapping_pin_lock;
alignas(4096) PageTable tables[32]{};
PageTable* kernel_pagemap{};
uint64_t hhdm = 0xffff800000000000ULL;
bool kernel_nx_supported = true;
size_t allocations{}, refs{}, tlbs{}, untracks{}, tracks{}, next_table{};
uint64_t physical_of(PageTable const* table) {
    CHECK(table >= tables && table < tables + 32);
    return 0x1000 + static_cast<uint64_t>(table - tables) * 4096;
}
namespace addr {
auto get_hhdm_offset() -> uint64_t { return hhdm; }
auto get_virt_pointer(uint64_t physical) -> uint64_t* {
    if (physical >= 0x1000 && physical < 0x21000 && (physical & 4095) == 0)
        return reinterpret_cast<uint64_t*>(&tables[(physical - 0x1000) / 4096]);
    return reinterpret_cast<uint64_t*>(hhdm + physical);
}
auto get_phys_pointer(uint64_t address) -> uint64_t* {
    return reinterpret_cast<uint64_t*>(physical_of(reinterpret_cast<PageTable*>(address)));
}
}
namespace dbg { [[noreturn]] void panic_handler(const char*) { throw Refused{}; } }
struct log { template<class... T> static void critical(const char*, T...) { throw Refused{}; } };
auto rdcr3() -> uint64_t { return 0; }
void wrcr3(uint64_t) { ++tlbs; }
auto alloc_zeroed_page_table() -> PageTable* {
    CHECK(!user_mapping_pin_lock.held && next_table < 32); ++allocations;
    return &tables[next_table++];
}
void owned_frame_untrack_leaf(PageTable*, uint64_t, PageTableEntry const&) { ++untracks; }
void owned_frame_track_private_mapping(PageTable*, uint64_t, uint64_t, uint64_t) { ++tracks; }
void invalidate_local_tlb_if_current(PageTable*, uint64_t, bool) { ++tlbs; }
void shootdown_remote_user_pagemap(PageTable*, uint64_t, bool) { ++tlbs; }
void drop_present_leaf_ref(PageTableEntry const&) { CHECK(!user_mapping_pin_lock.held); ++refs; }
void flush_pagemap_after_update(PageTable*, uint64_t, bool) { ++tlbs; }
bool is_reserved_leaf(PageTableEntry const& e) { return !e.present && e.available; }
struct Range { uint64_t start{}, end{}; };
auto range_valid(Range r) -> bool { return r.start < r.end; }
void map_page_batched(struct PageMapBatch*, vaddr_t, paddr_t, uint64_t);
void init_page_map_batch(struct PageMapBatch*, PageTable*, uint64_t);
void flush_page_map_batch(struct PageMapBatch*);
auto advance_page_table(paging::PageTable*, size_t, uint64_t, bool* = nullptr) -> paging::PageTable*;
'''

CASES = r'''
constexpr uint64_t DATA = 0x400000;
constexpr uint64_t P = paging::PAGE_PRESENT, W = paging::PAGE_WRITE, U = paging::PAGE_USER;
constexpr uint64_t NX = paging::PAGE_NX, PS = uint64_t{1} << 7, AD = (uint64_t{1} << 5) | (uint64_t{1} << 6);
constexpr uint64_t FLAGS = P | W | NX;
uint64_t& raw(PageTable& t, size_t i) { return reinterpret_cast<uint64_t*>(&t)[i]; }
void reset(bool large = true) {
    CHECK(!user_mapping_pin_lock.held);
    std::memset(tables, 0, sizeof(tables));
    kernel_pagemap = &tables[0]; hhdm = 0xffff800000000000ULL; next_table = 4;
    raw(tables[0], 256) = physical_of(&tables[1]) | P | W;
    raw(tables[1], 0) = physical_of(&tables[2]) | P | W;
    raw(tables[2], 2) = large ? DATA | FLAGS | PS : physical_of(&tables[3]) | P | W;
    allocations = refs = tlbs = untracks = tracks = 0;
}
template<class F> bool refused(F action) {
    try { action(); return false; } catch (Refused const&) { return true; }
}
void no_mapping_side_effects() {
    CHECK(allocations == 0 && refs == 0 && tlbs == 0 && untracks == 0 && tracks == 0);
    CHECK(!user_mapping_pin_lock.held);
}
int main() {
    try {
        reset();
        std::array<unsigned char, sizeof(tables)> before{};
        std::memcpy(before.data(), tables, sizeof(tables));
        for (uint64_t page = 0; page < 512; ++page) {
            auto pa = DATA + page * 4096; auto va = hhdm + pa;
            CHECK(kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, pa, FLAGS));
            map_page(kernel_pagemap, va, pa, FLAGS);
            PageMapBatch batch{}; init_page_map_batch(&batch, kernel_pagemap, FLAGS);
            batch.cached_idx4 = 7; batch.cached_idx3 = 11; batch.cached_idx2 = 13;
            batch.pml3 = &tables[7]; batch.pml2 = &tables[8]; batch.pml1 = &tables[9];
            auto original = batch;
            map_page_batched(&batch, va, pa, FLAGS);
            CHECK(batch.root == original.root && batch.pml3 == original.pml3 && batch.pml2 == original.pml2 &&
                  batch.pml1 == original.pml1 && batch.cached_idx4 == original.cached_idx4 &&
                  batch.cached_idx3 == original.cached_idx3 && batch.cached_idx2 == original.cached_idx2 &&
                  batch.table_flags == original.table_flags && !batch.dirty);
        }
        CHECK(std::memcmp(before.data(), tables, sizeof(tables)) == 0); no_mapping_side_effects();
        auto va = hhdm + DATA;
        CHECK(!kernel_hhdm_2m_mapping_matches(nullptr, va, DATA, FLAGS));
        CHECK(!kernel_hhdm_2m_mapping_matches(&tables[4], va, DATA, FLAGS));
        CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, va + 1, DATA, FLAGS));
        CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, DATA + 1, FLAGS));
        CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, DATA + 4096, FLAGS));
        CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, DATA, DATA, FLAGS));
        CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, uint64_t{1} << 52, FLAGS));
        CHECK(!kernel_hhdm_2m_mapping_matches(reinterpret_cast<PageTable*>(reinterpret_cast<uintptr_t>(kernel_pagemap) + 1), va, DATA, FLAGS));
        reset(); raw(tables[2], 2) = (DATA + LARGE_PAGE_2M_BYTES) | FLAGS | PS;
        CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, DATA, FLAGS));
        reset();
        for (uint64_t extra : {U, uint64_t{1} << 7, uint64_t{1} << 8, uint64_t{1} << 9, uint64_t{1} << 10, uint64_t{1} << 11, uint64_t{1} << 52})
            CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, DATA, FLAGS | extra));
        for (uint64_t changed : {uint64_t{0}, FLAGS & ~P, FLAGS & ~W, FLAGS & ~NX,
                                FLAGS | paging::PAGE_PWT, FLAGS | paging::PAGE_PCD})
            CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, DATA, changed));
        for (uint64_t bad : {U, uint64_t{1} << 8, uint64_t{1} << 9, uint64_t{1} << 10, uint64_t{1} << 11, uint64_t{1} << 12,
                             uint64_t{1} << 13, uint64_t{1} << 20, uint64_t{1} << 52, uint64_t{1} << 62}) {
            reset(); raw(tables[2], 2) |= bad;
            CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, DATA, FLAGS));
            CHECK(refused([&] { map_page(kernel_pagemap, va, DATA, FLAGS); }));
            PageMapBatch batch{}; init_page_map_batch(&batch, kernel_pagemap, FLAGS);
            CHECK(refused([&] { map_page_batched(&batch, va, DATA, FLAGS); }));
            no_mapping_side_effects();
        }
        reset(); raw(tables[2], 2) |= AD;
        CHECK(kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, DATA, FLAGS));
        for (uint64_t cache : {paging::PAGE_PWT, paging::PAGE_PCD, paging::PAGE_PWT | paging::PAGE_PCD}) {
            reset(); raw(tables[2], 2) |= cache;
            CHECK(kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, DATA, FLAGS | cache));
            CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, DATA, FLAGS));
        }
        for (size_t ancestor : {size_t{0}, size_t{1}}) {
            size_t slot = ancestor == 0 ? 256 : 0;
            for (uint64_t bad : {U, PS, uint64_t{1} << 8, uint64_t{1} << 9, uint64_t{1} << 11, uint64_t{1} << 52}) {
                reset(); raw(tables[ancestor], slot) |= bad;
                CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, DATA, FLAGS));
            }
            reset(); raw(tables[ancestor], slot) &= ~W;
            CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, DATA, FLAGS));
            // Effective-only equality would hide a latent writable leaf here.
            CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, DATA, FLAGS & ~W));
            reset(); raw(tables[2], 2) &= ~NX; raw(tables[ancestor], slot) |= NX;
            CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, DATA, FLAGS & ~NX));
            CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, va, DATA, FLAGS));
        }
        reset(); raw(tables[1], 0) |= PS;
        CHECK(refused([&] { map_page(kernel_pagemap, va, DATA, FLAGS); }));
        reset(); hhdm = UINT64_MAX - DATA + 1;
        CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, 0, DATA, FLAGS));
        reset(); hhdm += 4096;
        CHECK(!kernel_hhdm_2m_mapping_matches(kernel_pagemap, hhdm + DATA, DATA, FLAGS));
        reset(false); map_page(kernel_pagemap, va, DATA, FLAGS);
        CHECK(raw(tables[3], 0) == (DATA | FLAGS));
        CHECK(untracks == 1 && tracks == 1 && tlbs == 1 && refs == 0 && allocations == 0);
        reset(false); PageMapBatch mixed{}; init_page_map_batch(&mixed, kernel_pagemap, FLAGS);
        map_page_batched(&mixed, va, DATA, FLAGS);
        CHECK(mixed.dirty && tracks == 1);
        auto ordinary_cached = mixed; raw(tables[2], 3) = (DATA + LARGE_PAGE_2M_BYTES) | FLAGS | PS;
        map_page_batched(&mixed, va + LARGE_PAGE_2M_BYTES, DATA + LARGE_PAGE_2M_BYTES, FLAGS);
        CHECK(mixed.dirty && mixed.pml1 == ordinary_cached.pml1 && mixed.cached_idx2 == ordinary_cached.cached_idx2);
        map_page_batched(&mixed, va + 4096, DATA + 4096, FLAGS);
        CHECK(raw(tables[3], 1) == ((DATA + 4096) | FLAGS) && tracks == 2);
        flush_page_map_batch(&mixed); CHECK(!mixed.dirty && tlbs == 1);
        for (uint64_t type = 0; type < 32; ++type) {
            CHECK(boot_direct_map_allows_2m(type) == (type == LIMINE_MEMMAP_USABLE));
            bool ro = type == LIMINE_MEMMAP_RESERVED || type == LIMINE_MEMMAP_BAD_MEMORY ||
                      type == LIMINE_MEMMAP_EXECUTABLE_AND_MODULES;
            uint64_t boot_flags = (ro ? P : P | W) | NX;
            CHECK(direct_map_flags_for_entry(type) == boot_flags);
            reset(false); raw(tables[2], 2) = 0;
            auto stats = map_boot_direct_range(kernel_pagemap, Range{DATA, DATA + LARGE_PAGE_2M_BYTES}, boot_flags,
                                              boot_direct_map_allows_2m(type));
            CHECK(stats.pages == 512);
            CHECK(stats.large_2m == (type == LIMINE_MEMMAP_USABLE ? 1U : 0U));
            CHECK(stats.small_4k == (type == LIMINE_MEMMAP_USABLE ? 0U : 512U));
            CHECK(allocations == (type == LIMINE_MEMMAP_USABLE ? 0U : 1U));
            if (type != LIMINE_MEMMAP_USABLE) {
                auto table_index = ((raw(tables[2], 2) & PTE_FRAME_MASK) - 0x1000) / 4096;
                for (uint64_t page = 0; page < 512; ++page)
                    CHECK(raw(tables[table_index], page) == ((DATA + page * 4096) | boot_flags));
            }
        }
        kernel_nx_supported = false;
        CHECK(direct_map_flags_for_entry(LIMINE_MEMMAP_RESERVED) == P);
        CHECK(direct_map_flags_for_entry(LIMINE_MEMMAP_USABLE) == (P | W));
        kernel_nx_supported = true;
        reset(false); raw(tables[2], 2) = 0; hhdm += 4096;
        auto stats = map_boot_direct_range(kernel_pagemap, Range{DATA, DATA + LARGE_PAGE_2M_BYTES}, FLAGS, true);
        CHECK(stats.large_2m == 0 && stats.small_4k == 512);
        std::cout << "PASS real boot/HHDM helpers and mapper entrypoints: " << checks << " checks\n";
        return 0;
    } catch (std::exception const& error) {
        std::cerr << "FAIL: " << error.what() << "\n";
    } catch (Refused const&) { std::cerr << "FAIL: unexpected original PS guard\n"; }
    return 1;
}
'''


def pin(path: Path) -> dict:
    path = path.resolve()
    first = path.stat()
    data = path.read_bytes()
    last = path.stat()
    identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if identity(first) != identity(last):
        raise RuntimeError(f"input changed during read: {path}")
    return {"path": str(path), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
            "device": last.st_dev, "inode": last.st_ino, "mtime_ns": last.st_mtime_ns, "ctime_ns": last.st_ctime_ns}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--virt-source", type=Path, help="explicit staged source; headers remain from source-root")
    parser.add_argument("--cxx", default="g++", help="Host C++ compiler; ignores WOS target CXX")
    parser.add_argument("--output", type=Path, help="fresh result directory; owned scratch is always removed")
    parser.add_argument("--negative-controls", action="store_true", help="compile four genuine source mutations")
    args = parser.parse_args()
    root = args.source_root.resolve()
    source_path = (args.virt_source or root / "modules/kern/src/platform/mm/virt.opt.cpp").resolve()
    header_path = root / "modules/kern/src/platform/mm/virt.hpp"
    paging_path = root / "modules/kern/src/platform/mm/paging.opt.cpp"
    compiler = Path(shutil.which(args.cxx) or args.cxx).resolve()
    output = args.output.resolve() if args.output else Path(tempfile.mkdtemp(prefix="kernel-hhdm-result-"))
    if args.output:
        output.mkdir(parents=True, exist_ok=False)
    scratch = Path(tempfile.mkdtemp(prefix="scratch-", dir=output))
    started = time.monotonic()
    commands, errors, groups = [], [], {}
    selected = [Path(__file__), source_path, header_path, paging_path, compiler]
    before, emitted = [], []
    environment = dict(os.environ, TMPDIR=str(scratch), ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                       UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1")
    cancellation_signals = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    previous_handlers = {number: signal.getsignal(number) for number in cancellation_signals}
    launching, deferred_signal = False, None

    def cancelled(number, _frame):
        nonlocal deferred_signal
        if launching:
            deferred_signal = number
            return
        raise KeyboardInterrupt(f"received signal {number}")

    def retire_group(process) -> bool:
        try:
            os.killpg(process.pid, 0)
            return False
        except ProcessLookupError:
            # Do not retain an absent PGID: it can be reused by another job.
            groups.pop(process.pid, None)
            return True

    def stop_group(process) -> None:
        if process.pid not in groups:
            return
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)
        if not retire_group(process):
            raise RuntimeError(f"owned group {process.pid} remains after kill/reap")

    def run(label: str, command: list, expected: int = 0) -> None:
        nonlocal launching, deferred_signal
        remaining = 120 - (time.monotonic() - started)
        if remaining <= 0 or sum(p.stat().st_blocks * 512 for p in scratch.rglob("*") if p.is_file()) > 128 * 1024 * 1024:
            raise RuntimeError("time/scratch budget exhausted")
        log = output / (label + ".log")
        when = time.monotonic()
        process = None
        with log.open("wb") as stream:
            try:
                # Publish ownership before a pending cancellation can run.
                # Defer the handler rather than leaking a blocked signal mask
                # into the compiler or executable child.
                launching = True
                try:
                    process = subprocess.Popen([str(x) for x in command], cwd=root, env=environment,
                                               stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
                    groups[process.pid] = process
                finally:
                    launching = False
                if deferred_signal is not None:
                    number, deferred_signal = deferred_signal, None
                    cancelled(number, None)
                code = process.wait(timeout=min(35, remaining))
            except BaseException:
                if process is not None:
                    stop_group(process)
                raise
        absent = retire_group(process)
        commands.append({"label": label, "argv": [str(x) for x in command], "exit": code,
                         "expected_exit": expected, "seconds": time.monotonic() - when,
                         "owned_group_absent": absent, "leader_reaped": True, "log": pin(log)})
        if code != expected or not absent:
            raise RuntimeError(f"{label}: exit {code}, expected {expected}")

    try:
        for number in cancellation_signals:
            signal.signal(number, cancelled)
        before = [pin(path) for path in selected]
        source = source_path.read_text()
        header = header_path.read_text()
        batch_start = header.index("struct PageMapBatch {")
        batch = header[batch_start:header.index("\n};", batch_start) + 3]
        stats_start = source.index("struct BootDirectMapStats {")
        stats = source[stats_start:source.index("\n};", stats_start) + 3]
        names = ["auto index_of(", "auto entry_at(PageTable*", "auto table_flags_for_leaf(", "auto is_aligned_to(", "auto direct_map_flags_for_entry(",
                 "auto boot_direct_map_allows_2m(", "auto kernel_hhdm_2m_flags_match(", "auto kernel_hhdm_entry_raw(",
                 "auto kernel_hhdm_2m_mapping_matches(", "auto advance_page_table(paging::PageTable* page_table, size_t level, uint64_t flags, bool* promoted)",
                 "void map_page(PageTable*", "void init_page_map_batch(", "void map_page_batched(PageMapBatch* batch, const",
                 "void flush_page_map_batch(", "auto map_2m_page(", "auto map_boot_direct_range("]
        # Functions with forward declarations use the unique definition signature.
        bodies = []
        for signature in names:
            if signature == "void flush_page_map_batch(":
                signature = "void flush_page_map_batch(PageMapBatch* batch) {"
            if signature == "void init_page_map_batch(":
                signature = "void init_page_map_batch(PageMapBatch* batch, PageTable* page_table, const uint64_t FLAGS) {"
            bodies.append(function(source, signature))
        # The real constructor is compiled from its authoritative TU body.
        constructor = function(paging_path.read_text(), "auto create_page_table_entry(")
        code = PRELUDE + "\n" + batch + "\n" + stats + "\nnamespace ker::mod::mm::paging {\n" + constructor + "\n}\n"
        code += "\n".join(bodies) + "\n" + CASES
        # Admission must use the actual type helper, not an inferred RO flag.
        compact = "".join(source.split())
        required = "map_boot_direct_range(kernel_pagemap,entry_range,direct_map_flags_for_entry(entry->type),boot_direct_map_allows_2m(entry->type))"
        if compact.count(required) != 1:
            raise RuntimeError("actual boot caller is not wired to explicit type eligibility")
        variants = [("actual", code, 0)]
        if args.negative_controls:
            mutations = [
                ("lost-leaf-permissions", "(leaf_raw & REQUEST) != flags", "false"),
                ("wrong-subpage-identity", "BASE + (address & (LARGE_PAGE_2M_BYTES - 1)) == physical", "true"),
                ("all-boot-types-large", "return type == LIMINE_MEMMAP_USABLE;", "return true;"),
                ("lost-batch-noop", "if (kernel_hhdm_2m_mapping_matches(batch->root, VADDR, PADDR, FLAGS)) return;", "if (false) return;"),
            ]
            for label, original, replacement in mutations:
                if code.count(original) != 1:
                    raise RuntimeError(f"nonunique genuine mutation: {label}")
                variants.append((label, code.replace(original, replacement), 1))
        for label, body, expected in variants:
            cpp, binary, dep = (scratch / (label + suffix) for suffix in (".cpp", ".exe", ".d"))
            cpp.write_text(body)
            flags = [compiler, "-std=c++23", "-O1", "-g", "-fno-strict-aliasing", "-fno-pie", "-no-pie",
                     "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-I" + str(root / "modules/kern/src"),
                     "-I" + str(root / "modules")]
            run("compile-" + label, flags + ["-MD", "-MF", dep, cpp, "-o", binary])
            dependencies = [Path(x).resolve() for x in shlex.split(dep.read_text().replace("\\\n", " ").split(":", 1)[1])]
            direct = [pin(path) for path in dependencies if path != cpp]
            if not emitted:
                emitted = direct
            elif direct != emitted:
                raise RuntimeError("variant emitted dependency closure differs")
            run("execute-" + label, [binary], expected)
    except BaseException as error:
        errors.append(f"{type(error).__name__}: {error}")
    finally:
        # A second termination signal must not abandon an in-progress cleanup.
        for number in cancellation_signals:
            signal.signal(number, signal.SIG_IGN)
        try:
            for process in list(groups.values()):
                try:
                    stop_group(process)
                except Exception as error:
                    errors.append(f"cleanup: {error}")
            after, emitted_after = [], []
            try:
                after = [pin(Path(row["path"])) for row in before]
                emitted_after = [pin(Path(row["path"])) for row in emitted]
                if before != after or emitted != emitted_after:
                    errors.append("source/dependency changed")
            except Exception as error:
                errors.append(f"input recheck: {error}")
            scratch_bytes = sum(p.stat().st_blocks * 512 for p in scratch.rglob("*") if p.is_file())
            shutil.rmtree(scratch)
            result = {"qualification": "PASS_ACTUAL_EXTRACTED_BOOT_HHDM_HOST_ONLY" if not errors else "FAIL_HOST_ONLY",
                      "errors": errors, "commands": commands, "seconds": time.monotonic() - started,
                      "input_pins": before, "input_pins_after": after, "emitted_inputs": emitted,
                      "emitted_inputs_after": emitted_after, "scratch_removed": not scratch.exists(),
                      "scratch_allocated_bytes_removed": scratch_bytes, "pending_owned_groups": list(groups),
                      "limits": ["Explicit Host physical/lock/ref/TLB endpoints; no WOS boot or SMP/cache proof.",
                                 "Hardware A/D mutation and table races are not simulated."]}
            (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps({"result": str(output / "result.json"), "errors": errors, "commands": len(commands)}))
        finally:
            for number, handler in previous_handlers.items():
                signal.signal(number, handler)
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
