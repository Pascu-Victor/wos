#!/usr/bin/env python3
"""Execute actual legacy registry/writeback transactions with diagnostic debt.

All existing registry vectors remain exact. File references, sleeping mutex,
physical page snapshots/writeback and allocation refusal are explicit Host
endpoints. This is not a scheduled kernel, native backend-return or SMP test.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]

OBSERVERS = r"""
namespace fixture {
namespace nl = ker::test::numa_lifecycle;
namespace debt = nl::mm_debt;
auto debt_snapshot() -> nl::MmDebtSnapshot {
    nl::MmDebtSnapshot output{};
    debt::snapshot(output);
    return output;
}
void check_debt_clean() {
    auto const output = debt_snapshot();
    check(output.valid && output.inflight == 0, "registry counter epoch/arithmetic valid");
    check(output.legacy_file_registry_rows == 0 && output.native_file_registry_rows == 0 && output.registry_returns == 0,
          "all registry owners and synchronous return tails drained");
}
void observe_retain() {
    auto const output = debt_snapshot();
    check(output.valid && output.inflight > 0, "actual retain enclosed by short ownership epoch");
}
void observe_put(bool final) {
    auto const output = debt_snapshot();
    check(output.valid && output.inflight == 0, "no broad epoch across actual put/close");
    if (!final) check(output.registry_returns > 0, "real temporary/registry owner remains through actual put");
}
}
"""

SYNC_ENDPOINTS = r"""
namespace ker::mod::sched::task { struct Task {}; }
namespace ker::syscall::vmem {
constexpr uint64_t USER_SPACE_END = 0x7fffffffffffULL;
uint64_t page_align_down(uint64_t value) { return value & ~uint64_t{4095}; }
namespace fixture_sync {
int preflight_result{}, write_result{};
unsigned snapshots{}, writes{};
bool skip{};
}
enum class FileMmapPageSnapshot : uint8_t { SKIP, READY, FAULT };
auto preflight_file_mmap_sync_ranges(ker::mod::mm::paging::PageTable*, ker::mod::sched::task::Task*,
                                   ker::util::SmallVec<FileMmapSyncRange, 8>& ranges) -> int {
    auto const debt = fixture::debt_snapshot();
    fixture::check(debt.inflight == 0 && debt.registry_returns == ranges.size(),
                   "all collected File loans survive unlocked preflight");
    for (auto& range : ranges) range.skip_writeback = fixture_sync::skip;
    return fixture_sync::preflight_result;
}
auto snapshot_file_mmap_page(ker::mod::mm::paging::PageTable*, ker::mod::sched::task::Task*, uint64_t, uint64_t,
                             size_t, std::array<uint8_t, 4096>&) -> FileMmapPageSnapshot {
    fixture::check(fixture::lock_depth == 0 && fixture::debt_snapshot().inflight == 0,
                   "actual sync IO snapshot outside registry mutex/epoch");
    ++fixture_sync::snapshots;
    return FileMmapPageSnapshot::READY;
}
auto write_file_mapping_bytes(ker::vfs::File*, const uint8_t*, size_t, uint64_t) -> int {
    auto const debt = fixture::debt_snapshot();
    fixture::check(fixture::lock_depth == 0 && debt.inflight == 0 && debt.registry_returns > 0,
                   "actual writeback retains File loan across unlocked backend endpoint");
    ++fixture_sync::writes;
    return fixture_sync::write_result;
}
}
"""

EXTRA_CASES = r"""
int main() {
    int const old_result = original_registry_main();
    if (old_result != 0) return old_result;
    fixture::check_debt_clean();
    using namespace fixture;
    using namespace ker::syscall::vmem;
    Root root{77};
    ker::vfs::File file;
    unsigned extra_cases = 0;
    for (unsigned mode = 0; mode != 5; ++mode) {
        fixture_sync::preflight_result = mode == 1 ? -EACCES : 0;
        fixture_sync::write_result = mode == 3 ? -EIO : 0;
        fixture_sync::skip = mode == 2;
        fixture_sync::snapshots = fixture_sync::writes = 0;
        for (unsigned i = 0; i != 9; ++i) {
            check(register_file_mmap_range(&root, 0x10000 + i * 8192, 4096, i * 4096, &file, true) == 0,
                  "actual sync registry seed");
        }
        auto output = debt_snapshot();
        check(output.legacy_file_registry_rows == 9 && output.registry_returns == 0 && file.refcount.load() == 10,
              "nine actual retained rows before writeback");
        fail_growth_after = mode == 4 ? 0 : -1;
        int const result = sync_file_mmap_range_impl(&root, nullptr, 0x10000, 9 * 8192);
        fail_growth_after = -1;
        int const expected = mode == 1 ? -EACCES : mode == 3 ? -EIO : mode == 4 ? -ENOMEM : 0;
        check(result == expected, "actual sync error/skip/success preserved");
        output = debt_snapshot();
        check(output.valid && output.inflight == 0 && output.registry_returns == 0 &&
                  output.legacy_file_registry_rows == 9 && file.refcount.load() == 10,
              "all collected prefix/current/suffix loans returned exactly once");
        check(fixture_sync::writes == (mode == 0 ? 9U : mode == 3 ? 1U : 0U),
              "full old success, skipped preflight, and exact error prefix");
        check(unregister_file_mmap_range(&root, 0, USER_SPACE_END) == 0, "actual rows unregistered after sync");
        check(file.refcount.load() == 1, "only caller File ref survives cleanup");
        check_debt_clean();
        ++extra_cases;
    }
    // Versions detect net-zero geometry changes; counts alone cannot.
    check(register_file_mmap_range(&root, 0x10000, 4096, 71, &file, false) == 0, "epoch fixture row");
    auto const before = debt_snapshot();
    check(unregister_file_mmap_range(&root, 0x10000, 1) == 0, "actual head trim");
    auto const after = debt_snapshot();
    check(before.legacy_file_registry_rows == after.legacy_file_registry_rows &&
              before.version_end != after.version_begin && after.inflight == 0,
          "real geometry mutation advances version without changing live owners");
    check(unregister_file_mmap_range(&root, 0, USER_SPACE_END) == 0, "trim fixture cleanup");
    check_debt_clean();
    ker::vfs::vfs_put_file(&file);
    std::printf("%s actual-registry-census extra_sync_cases=%u checks=%u failures=%u fields=23 snapshot_bytes=216\n",
                failures == 0 ? "PASS" : "FAIL", extra_cases, assertions, failures);
    return failures == 0 ? 0 : 3;
}
"""

NATIVE_ENDPOINTS = r"""
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <test/numa_lifecycle_mm_debt.hpp>
#include <platform/mm/lifecycle_census.hpp>
@CORE@
namespace model {
namespace nl = ker::test::numa_lifecycle;
unsigned checks{}, locks{}, registry_releases{}, binding_releases{}, prepared_releases{};
auto snapshot() -> nl::MmDebtSnapshot { nl::MmDebtSnapshot output{}; nl::mm_debt::snapshot(output); return output; }
void check(bool ok) { ++checks; if (!ok) { std::fprintf(stderr, "native registry check=%u failed\n", checks); std::abort(); } }
struct Mutex {
    void lock() { check(locks++ == 0); }
    void unlock() { check(locks-- == 1); }
};
void clean() { auto s = snapshot(); check(s.valid && s.inflight == 0 && s.native_file_registry_rows == 0 && s.registry_returns == 0); }
}
namespace ker::mod::mm::paging { struct PageTable { unsigned id{}; }; }
namespace ker::mod::mm::file_mapping {
enum class FamilyOwner { PREPARED, REGISTRY };
enum class BindingOwner { PREPARED };
struct RetirementCompletion {};
class FileMappingLifetime {
  public:
    bool allow = true;
    unsigned prepared = 1, registry = 0;
    auto try_acquire(FamilyOwner role) -> bool {
        auto s = model::snapshot(); model::check(role == FamilyOwner::REGISTRY && s.valid && s.inflight > 0);
        if (!allow) return false;
        ++registry; return true;
    }
    void release(FamilyOwner role) {
        auto s = model::snapshot(); model::check(s.valid && s.inflight == 0 && model::locks == 0);
        if (role == FamilyOwner::REGISTRY) {
            model::check(registry == 1 && s.native_file_registry_rows > 0 && s.registry_returns > 0);
            --registry; ++model::registry_releases;
        } else { model::check(prepared == 1); --prepared; ++model::prepared_releases; }
    }
};
class FileMappingBinding {
  public:
    unsigned prepared = 1;
    void release(BindingOwner) {
        auto s = model::snapshot(); model::check(s.valid && s.inflight == 0 && model::locks == 0 && prepared == 1);
        --prepared; ++model::binding_releases;
    }
};
@ENTRY@
}
namespace ker::syscall::vmem {
namespace fm = ker::mod::mm::file_mapping;
using NativeMappingEntry = fm::UnpublishedMappingEntry;
NativeMappingEntry* g_native_file_mmap_ranges{};
model::Mutex g_file_mmap_ranges_lock;
auto stage_owned(fm::FileMappingLifetime& family, fm::FileMappingBinding& owned_binding,
                 NativeMappingEntry& entry, bool cancel) -> int {
    auto* lifetime = &family;
    auto* binding = &owned_binding;
@ACQUIRE@
@DISCARD@
    if (cancel) { discard_prepared(); return -ENOMEM; }
    entry.lifetime = lifetime;
    entry.binding = binding;
@LINK@
    return 0;
}
@UNLINK@
}
int main() {
    namespace actual = ker::syscall::vmem;
    namespace fm = ker::mod::mm::file_mapping;
    ker::mod::mm::paging::PageTable root{1}, other{2};
    for (unsigned mode = 0; mode != 3; ++mode) {
        model::clean();
        fm::FileMappingLifetime family;
        fm::FileMappingBinding binding;
        fm::UnpublishedMappingEntry entry{};
        entry.root = &root;
        family.allow = mode != 1;
        int result = actual::stage_owned(family, binding, entry, mode == 2);
        model::check(result == (mode == 1 ? -EOVERFLOW : mode == 2 ? -ENOMEM : 0));
        if (mode == 0) {
            auto s = model::snapshot();
            model::check(s.valid && s.inflight == 0 && s.native_file_registry_rows == 1 && s.registry_returns == 0);
            model::check(actual::g_native_file_mmap_ranges == &entry && entry.registered && family.registry == 1);
            actual::release_file_mmap_ranges_for_pagemap(&other);
            model::check(actual::g_native_file_mmap_ranges == &entry && family.registry == 1);
            actual::release_file_mmap_ranges_for_pagemap(&root);
            model::check(!actual::g_native_file_mmap_ranges && !entry.registered && !entry.registry_next && family.registry == 0);
            actual::release_file_mmap_ranges_for_pagemap(&root);
            binding.release(fm::BindingOwner::PREPARED);
            family.release(fm::FamilyOwner::PREPARED);
        }
        model::check(family.prepared == 0 && family.registry == 0 && binding.prepared == 0);
        model::clean();
    }
    model::check(model::registry_releases == 2 && model::binding_releases == 3 && model::prepared_releases == 3);
    std::printf("PASS actual native acquire/discard/link/unlink branches checks=%u; Family/Binding are explicit release endpoints; no RETURNED claim\n", model::checks);
}
"""


def load_fixture(root: Path):
    path = root / "tests/host/unit/file_mmap_registry_runtime_test.py"
    spec = importlib.util.spec_from_file_location("existing_registry_fixture", path)
    if spec is None or spec.loader is None:
        raise ValueError("actual registered registry fixture unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def generate(root: Path, scratch: Path, source: Path, virt: Path, header: Path, fixture):
    debt_source = virt.read_text()
    core = debt_source[debt_source.index("#ifdef WOS_SELFTEST\nnamespace ker::test::numa_lifecycle::mm_debt {"):]
    # PRELUDE remains the existing complete primitive shell, with observers at
    # actual retain/put endpoints. CASES remains byte-exact, including all old
    # growth, clone-prefix, byte projection and destructor assertions.
    fixture.PRELUDE = fixture.PRELUDE.replace(
        '    ++fixture::retains; ++file->refcount;',
        '    fixture::observe_retain();\n    ++fixture::retains; ++file->refcount;')
    fixture.PRELUDE = fixture.PRELUDE.replace(
        '    int old=file->refcount.fetch_sub(1);',
        '    fixture::observe_put(file->refcount.load() == 1);\n    int old=file->refcount.fetch_sub(1);')
    declaration = '\nvoid observe_retain();\nvoid observe_put(bool final);\n'
    fixture.PRELUDE = fixture.PRELUDE.replace('namespace fixture {', 'namespace fixture {' + declaration, 1)
    fixture.generate(scratch, source)
    include = scratch / "include"
    for rel, path in [("test/numa_lifecycle_mm_debt.hpp", header),
                      ("platform/mm/lifecycle_census.hpp", root / "modules/kern/src/platform/mm/lifecycle_census.hpp")]:
        target = include / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(path.read_bytes())
    harness = scratch / "harness.cpp"
    generated = harness.read_text().replace('int main() {', 'int original_registry_main() {', 1)
    # Snapshot declarations are available at the primitive endpoints; the core
    # and observer definitions precede the old runtime case body.
    core_at = generated.index('#line 1 "independent-fixture-oracles"')
    generated = generated[:core_at] + core + OBSERVERS + generated[core_at:]
    generated = '#include <atomic>\n#include <test/numa_lifecycle_mm_debt.hpp>\n#include <platform/mm/lifecycle_census.hpp>\n' + generated
    text = source.read_text()
    chunks = []
    for anchor, declaration in [("struct FileMmapSyncRange {", True),
                                ("auto collect_file_mmap_sync_ranges(", False),
                                ("void release_file_mmap_sync_ranges(", False)]:
        body, _ = fixture.capture(text, anchor, declaration=declaration)
        chunks.append(body)
    actual_sync, _ = fixture.capture(text, "auto sync_file_mmap_range_impl(")
    sync = ('namespace ker::mod::mm::paging { constexpr uint64_t PAGE_SIZE = 4096; }\n'
            + 'namespace ker::syscall::vmem {\n' + '\n'.join(chunks) + '\n}\n'
            + SYNC_ENDPOINTS + '\nnamespace ker::syscall::vmem {\n' + actual_sync + '\n}\n')
    generated += sync + EXTRA_CASES
    harness.write_text(generated)
    # Actual native transaction branches use supplied owned entries and explicit
    # Family/Binding release endpoints. This does not replace native retirement
    # admission, root cleanup, or the real returned-completion reader.
    acquired = text.index('bool const REGISTRY_ACQUIRED = lifetime->try_acquire')
    acquire_start = text.rfind('#ifdef WOS_SELFTEST', 0, acquired)
    acquire_end = text.index('    auto discard_prepared = [&]', acquired)
    discard, _ = fixture.capture(text, 'auto discard_prepared = [&]')
    linked = text.index('            entry.registry_next = g_native_file_mmap_ranges;')
    link_start = text.rfind('            g_file_mmap_ranges_lock.lock();', 0, linked)
    link_end = text.index('            g_file_mmap_ranges_lock.unlock();', linked) + len('            g_file_mmap_ranges_lock.unlock();')
    unlink, _ = fixture.capture(text, 'void release_file_mmap_ranges_for_pagemap(')
    unlink = unlink[:unlink.index('    // Teardown is one writeback operation:')] + '}\n'
    entry, _ = fixture.capture(fixture.NATIVE_HEADER.read_text(), 'struct UnpublishedMappingEntry {', declaration=True)
    native = NATIVE_ENDPOINTS
    for key, value in {'CORE': core, 'ENTRY': entry, 'ACQUIRE': text[acquire_start:acquire_end],
                       'DISCARD': discard + ';', 'LINK': text[link_start:link_end], 'UNLINK': unlink}.items():
        native = native.replace('@' + key + '@', value)
    (scratch / 'native.cpp').write_text(native)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--vmem-source", type=Path)
    parser.add_argument("--virt-source", type=Path)
    parser.add_argument("--debt-header", type=Path)
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    source = args.vmem_source or root / "modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp"
    virt = args.virt_source or root / "modules/kern/src/platform/mm/virt.opt.cpp"
    header = args.debt_header or root / "modules/kern/src/test/numa_lifecycle_mm_debt.hpp"
    fixture = load_fixture(root)
    started = time.monotonic()
    deadline = started + 60
    groups = []
    scratch = None
    depfile = None
    before = []
    result = {"pass": False, "errors": [], "commands": [], "native_runtime_qualified": False,
              "full_census_qualified": False, "scratch_removed": False, "source_held": False,
              "preserved_scratch": None}
    evidence = args.evidence_dir
    if evidence is not None:
        evidence.mkdir(mode=0o700)
    watched = [Path(__file__).resolve(), source, virt, header,
               root / "tests/host/unit/file_mmap_registry_runtime_test.py",
               root / "modules/kern/src/platform/mm/lifecycle_census.hpp", fixture.SMALLVEC,
               fixture.FAMILY_HEADER, fixture.NATIVE_HEADER]
    handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f"interrupted by signal {signum}")

    try:
        for sig in handlers:
            signal.signal(sig, interrupted)
        before = [fixture.pin(path) for path in watched]
        fixture.subreaper()
        scratch = Path(tempfile.mkdtemp(prefix="wos-registry-debt-", dir="/tmp"))
        depfile = scratch / "dependencies.d"
        generate(root, scratch, source, virt, header, fixture)
        compiler = os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++")
        binary = scratch / "registry-census"
        command = [compiler, "-std=c++23", "-DWOS_SELFTEST=1", "-DWOS_HOST_TEST=1", "-O1", "-g0",
                   "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-MD", "-MF", str(depfile),
                   "-I" + str(scratch / "include"), "-I" + str(root / "tests/host/kern_shim"),
                   "-I" + str(root / "modules/kern/src"), "-I" + str(root / "modules"),
                   str(scratch / "harness.cpp"), "-o", str(binary)]
        native_binary = scratch / 'native-registry-census'
        native_command = command[:]
        native_command[native_command.index(str(depfile))] = str(scratch / 'native.d')
        native_command[native_command.index(str(scratch / 'harness.cpp'))] = str(scratch / 'native.cpp')
        native_command[-1] = str(native_binary)
        for index, argv in enumerate((command, [str(binary)], native_command, [str(native_binary)])):
            row = fixture.command(argv, scratch / f"command-{index}.log", scratch, deadline, groups)
            result["commands"].append(row)
            text = (scratch / f"command-{index}.log").read_text()
            print(text[-16384:], end="", flush=True)
            if row['exit'] != 0:
                raise ValueError(f"actual registry census command {index} exit {row['exit']}")
        result["emitted_dependencies"] = fixture.dependency_pins(depfile)
        result["native_emitted_dependencies"] = fixture.dependency_pins(scratch / 'native.d')
        result["pass"] = True
    except BaseException as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        for sig in handlers:
            signal.signal(sig, signal.SIG_IGN)
        try:
            try:
                if before:
                    result["source_before"] = before
                    result["source_after"] = [fixture.pin(path) for path in watched]
                    result["source_held"] = result["source_before"] == result["source_after"]
                    if not result["source_held"]:
                        result["errors"].append("watched input changed")
            except BaseException as error:
                result["errors"].append(f"input recheck: {type(error).__name__}: {error}")
            result["pending_owned_groups"] = groups[:]
            if scratch is not None:
                try:
                    if evidence is not None:
                        for path in [scratch / 'harness.cpp', scratch / 'native.cpp', depfile,
                                     scratch / 'native.d', *scratch.glob('command-*.log')]:
                            if path is not None and path.is_file():
                                shutil.copyfile(path, evidence / path.name)
                except BaseException as error:
                    result["errors"].append(f"evidence copy: {type(error).__name__}: {error}")
                if groups:
                    result["errors"].append("owned group remains; preserve scratch")
                    result["preserved_scratch"] = str(scratch)
                else:
                    try:
                        result["scratch_allocated_removed"] = fixture.allocated(scratch)
                        shutil.rmtree(scratch)
                        result["scratch_removed"] = not scratch.exists()
                    except BaseException as error:
                        result["errors"].append(f"scratch cleanup: {type(error).__name__}: {error}")
                        result["preserved_scratch"] = str(scratch)
        finally:
            for sig, handler in handlers.items():
                signal.signal(sig, handler)
        result["seconds"] = round(time.monotonic() - started, 6)
        result["pass"] = result["pass"] and not result["errors"] and result["scratch_removed"]
        if evidence is not None:
            (evidence / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    return 0 if result["pass"] and not result["errors"] and result["scratch_removed"] else 1


if __name__ == '__main__':
    raise SystemExit(main())
