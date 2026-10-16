#!/usr/bin/env python3
"""Exercise actual lazy ownership bodies with private diagnostic counters.

The existing registered lazy fixture supplies explicit Task/root/IRQ/allocation
and queued-executor endpoints. This candidate reuses its complete 149 old cases,
actual SmallVec/Family code and bounded process helpers, then adds ownership/tail
checks. It does not attest native MM, real IRQ, backend completion or SMP.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import re
import shutil
import signal
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]


def held_pin(path: Path) -> dict:
    canonical = path.resolve(strict=True)
    before = canonical.stat()
    data = canonical.read_bytes()
    after = canonical.stat()
    identity = lambda st: (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)
    if identity(before) != identity(after) or len(data) != after.st_size:
        raise ValueError(f"input changed while hashing: {canonical}")
    return {
        "path": str(canonical),
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "device": after.st_dev,
        "inode": after.st_ino,
        "mtime_ns": after.st_mtime_ns,
    }


def load_fixture(root: Path):
    path = root / "tests/host/unit/lazy_vmem_staging_runtime_test.py"
    spec = importlib.util.spec_from_file_location("wos_existing_lazy_fixture", path)
    if spec is None or spec.loader is None:
        raise ValueError("registered lazy fixture unavailable")
    module = importlib.util.module_from_spec(spec)
    bytecode = sys.dont_write_bytecode
    try:
        sys.dont_write_bytecode = True
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = bytecode
    module.ROOT = root
    module.KERN = root / "modules/kern/src"
    return module


EXTRA_CASES = r"""

namespace fixture {
namespace nl = ker::test::numa_lifecycle;
namespace debt = nl::mm_debt;
bool check_lazy_put{}, check_lazy_submit{};
uint8_t expected_kind{};
unsigned lazy_cases{};

auto lazy_snapshot() -> nl::MmDebtSnapshot {
    nl::MmDebtSnapshot output{};
    debt::snapshot(output);
    return output;
}
void lazy_case(bool ok, const char* why) {
    ++lazy_cases;
    check(ok, why);
}
void lazy_clean() {
    auto snapshot = lazy_snapshot();
    check(snapshot.valid && snapshot.inflight == 0 && snapshot.lazy_returns == 0, "lazy snapshot valid, outside every transition/return");
    for (auto count : snapshot.lazy_rows) check(count == 0, "all lazy kinds drained");
}
void observe_lazy_put() {
    if (!check_lazy_put) return;
    auto snapshot = lazy_snapshot();
    check(snapshot.valid && snapshot.inflight == 0, "no broad epoch across File put");
    check(snapshot.lazy_rows[expected_kind] > 0 && snapshot.lazy_returns > 0, "actual File put still owns row and complete return tail");
}
void observe_lazy_submit() {
    if (!check_lazy_submit) return;
    auto snapshot = lazy_snapshot();
    check(snapshot.valid && snapshot.inflight == 0, "no broad epoch across Family submit");
    check(snapshot.lazy_rows[expected_kind] > 0 && snapshot.lazy_returns > 0, "actual Family final submit still owns row/return debt");
}
void lazy_vectors() {
    lazy_clean();
    for (uint8_t kind = 0; kind < 5; ++kind) {
        ker::vfs::File file;
        Row value{};
        value.kind = static_cast<Kind>(kind);
        fm::FileMappingLifetime* family = nullptr;
        if (kind == 1) value.file = &file;
        if (kind >= 2) {
            family = fm::FileMappingLifetime::prepare(file);
            check(family != nullptr, "real Family preparation");
            value.lifetime = family;
        }
        lazy_case(st::try_retain_lazy_vmem_range(value), "each actual kind acquires row");
        auto snapshot = lazy_snapshot();
        check(snapshot.inflight == 0 && snapshot.lazy_rows[kind] == 1, "actual acquire publishes exactly one matching kind");
        for (uint8_t other = 0; other < 5; ++other)
            if (other != kind) check(snapshot.lazy_rows[other] == 0, "no other kind adopted");
        if (family != nullptr) family->release(fm::FamilyOwner::PREPARED);
        expected_kind = kind;
        check_lazy_put = kind == 1;
        check_lazy_submit = kind >= 2;
        st::release_lazy_vmem_range(value);
        check_lazy_put = check_lazy_submit = false;
        lazy_clean();
        if (family != nullptr) {
            bool queued = false;
            for (size_t i = 0; i < used; ++i) queued |= tickets[i].ready;
            lazy_case(queued && file.refcount == 2, "row return is not deferred Family backend completion");
            drain();
        }
        lazy_case(file.refcount == 1, "actual File/Family reference returned once");
    }
    {
        Row value{};
        value.kind = Kind::FILE_BACKED;
        value.file = nullptr;
        lazy_case(st::try_retain_lazy_vmem_range(value), "raw-null row retains metadata ownership");
        check(lazy_snapshot().lazy_rows[1] == 1, "raw-null row counted");
        st::release_lazy_vmem_range(value);
        lazy_clean();
    }
    {
        ker::vfs::File saturated;
        saturated.refcount = INT_MAX;
        Row value{};
        value.kind = Kind::FILE_BACKED;
        value.file = &saturated;
        lazy_case(!st::try_retain_lazy_vmem_range(value), "actual raw checked retain refuses saturation");
        lazy_clean();
        value.kind = Kind::NATIVE_FILE_BACKED;
        value.lifetime = nullptr;
        lazy_case(!st::try_retain_lazy_vmem_range(value), "missing typed owner refuses unadopted");
        lazy_clean();
    }
    {
        ker::vfs::File file;
        ker::mod::loader::elf::ElfLazyLoadRangeVec metadata;
        for (unsigned i = 0; i < 9; ++i) check(metadata.push_back({0x40000 + i * 4096, 4096, 1, 2, i * 4096}), "loader metadata seed");
        ker::vfs::Stat stat{};
        stat.st_size = 16384;
        st::LazyVmemRangeVec rows;
        fail_array_after = 0;
        lazy_case(!actual_exec::append_exec_lazy_file_ranges(rows, metadata, &file, stat),
                  "actual exec append preserves fallible growth refusal");
        fail_array_after = -1;
        lazy_case(rows.size() == 8 && lazy_snapshot().lazy_rows[1] == 8 && file.refcount == 9,
                  "only actual successful prefix acquired File and row");
        check_lazy_put = true;
        expected_kind = 1;
        actual_exec::release_lazy_file_refs(rows);
        check_lazy_put = false;
        lazy_clean();
        lazy_case(file.refcount == 1, "exec prefix cleanup returns actual refs");
        lazy_case(actual_exec::append_exec_lazy_file_ranges(rows, metadata, &file, stat), "actual exec heap-growth append succeeds");
        st::LazyVmemRangeVec moved(std::move(rows));
        lazy_case(rows.empty() && moved.size() == 9 && lazy_snapshot().lazy_rows[1] == 9, "move transfers owned rows without recount");
        actual_exec::release_lazy_file_refs(moved);
        lazy_clean();
    }
    {
        ker::vfs::File file;
        st::LazyVmemRangeVec rows;
        auto* family = fm::FileMappingLifetime::prepare(file);
        check(family != nullptr, "native family prepared");
        actual_native_row(rows, family);
        lazy_case(rows.size() == 1 && lazy_snapshot().lazy_rows[2] == 1, "actual PREPARED to LAZY and row publication adopted once");
        st::release_lazy_vmem_range_refs(rows);
        lazy_clean();
        drain();
        lazy_case(file.refcount == 1, "native actual Family return drains once");
    }
    {
        ker::vfs::File file;
        st::LazyVmemRangeVec rows;
        lazy_case(actual_local_exec_row(rows, &file), "actual local exec vector acquires File row");
        lazy_case(rows.size() == 1 && file.refcount == 2 && lazy_snapshot().lazy_rows[1] == 1,
                  "local exec bypass owns actual counted extra reference");
        st::LazyVmemRangeVec consumed(std::move(rows));
        lazy_case(rows.empty() && lazy_snapshot().lazy_rows[1] == 1, "prepared-image-style consumption is an unchanged owned-vector move");
        actual_exec::release_lazy_file_refs(consumed);
        lazy_clean();
        lazy_case(file.refcount == 1, "local exec cancel/retirement uses common actual release");
    }
    {
        ker::vfs::File file, old;
        st::Task source;
        Row value{};
        value.kind = Kind::FILE_BACKED;
        value.file = &file;
        check(st::try_retain_lazy_vmem_range(value), "manual transfer seed actual File owner");
        check(source.lazy_vmem_ranges.push_back(value), "manual transfer seed actual row");
        ++old.refcount;
        {
            actual_cutover::LazyOwnershipCutover cutover(source);
            cutover.transferred.reset(&old);
            expected_kind = 1;
            check_lazy_put = true;
            actual_cutover::LazyOwnershipCutover::inject(source);
            check_lazy_put = false;
            lazy_case(source.lazy_vmem_ranges.empty() && cutover.transferred.get() == &file,
                      "actual native clear transfers File without another put");
            lazy_clean();
            lazy_case(file.refcount == 2 && old.refcount == 1, "row debt returns after actual reset, external File owner remains");
            actual_cutover::LazyOwnershipCutover::inject(source);
            lazy_case(!cutover.transferred && file.refcount == 1, "empty second transfer drops no row and returns old external File");
            lazy_clean();
        }
        lazy_case(ker::vfs::unpublished_self_refs_test_hook == nullptr, "actual scoped native callback registration restored");
    }
    // Invalid kind is last: the real global counter validity is never reset.
    {
        Row invalid{};
        invalid.kind = static_cast<Kind>(255);
        lazy_case(st::try_retain_lazy_vmem_range(invalid), "invalid diagnostic kind preserves old bool");
        auto snapshot = lazy_snapshot();
        lazy_case(!snapshot.valid && snapshot.lazy_returns == 0, "invalid kind poisons without aliasing RETURNS");
        st::release_lazy_vmem_range(invalid);
        snapshot = lazy_snapshot();
        lazy_case(!snapshot.valid && snapshot.inflight == 0 && snapshot.lazy_returns == 0,
                  "invalid diagnostic state never regains valid success");
        for (auto count : snapshot.lazy_rows) check(count == 0, "invalid kind never indexes another count");
    }
}
}  // namespace fixture
int main() {
    int old_result = old_fixture_main();
    if (old_result != 0) return old_result;
    fixture::lazy_vectors();
    std::printf("LAZY_OWNERS cases=%u assertions=%u failures=%u real_body=1 deferred_backend_unqualified=1\n", fixture::lazy_cases,
                fixture::assertions, fixture::failures);
    return fixture::failures ? 3 : 0;
}
"""


def generate(root: Path, scratch: Path, fixture):
    kern = root / "modules/kern/src"
    fixture.COMMON = fixture.COMMON.replace(
        "inline void (*publication_hook)(){};",
        "inline void (*publication_hook)(){};\nvoid observe_lazy_put();\nvoid observe_lazy_submit();",
    )
    fixture.COMMON = fixture.COMMON.replace(
        "    bool held{};",
        "    bool held{};\n"
        "    void lock() { (void)lock_irqsave(); }\n"
        "    bool try_lock() { if (held) return false; lock(); return true; }\n"
        "    void unlock() { unlock_irqrestore(0); }",
    )
    fixture.COMMON = fixture.COMMON.replace(
        "inline void vfs_put_file(File* f) {",
        "inline void vfs_put_file(File* f) {\n    fixture::observe_lazy_put();",
    )
    fixture.SEAMS = fixture.SEAMS.replace(
        "void submit(void*,fm::LifetimeRetirementTicket& ticket) noexcept {",
        "void submit(void*,fm::LifetimeRetirementTicket& ticket) noexcept {\n    observe_lazy_submit();",
    )
    fixture.generate(scratch)
    include = scratch / "include"
    for rel in [
        "test/numa_lifecycle_mm_debt.hpp",
        "platform/mm/lifecycle_census.hpp",
        "abi/callnums/vmem.h",
    ]:
        path = include / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((kern / rel).read_bytes())
    # Storage diagnostics are explicit unrelated endpoints, not coverage credit.
    (include / "platform/mm/lifecycle_storage.hpp").write_text(
        "#pragma once\nnamespace ker::mod::mm::lifecycle_storage {"
        "enum class Kind { FAMILY, BINDING };"
        "inline void begin_allocation(Kind) noexcept {}"
        "inline void finish_allocation(Kind,bool) noexcept {}"
        "inline void finish_deallocation(Kind) noexcept {} }\n"
    )
    tmpfs = include / "vfs/fs/tmpfs.hpp"
    tmpfs.write_text(
        tmpfs.read_text().replace(
            "class TmpfsReadSharedBacking { public:",
            "class TmpfsReadSharedBacking { public: uint64_t eof() const { return 0; }",
        )
    )
    virt = (kern / "platform/mm/virt.opt.cpp").read_text()
    core = virt[
        virt.index(
            "#ifdef WOS_SELFTEST\nnamespace ker::test::numa_lifecycle::mm_debt {"
        ) :
    ]
    source = (
        (scratch / "harness.cpp")
        .read_text()
        .replace("int main(){", "int old_fixture_main(){", 1)
    )
    append = fixture.capture(
        (kern / "syscalls_impl/process/exec.cpp").read_text(),
        "auto append_exec_lazy_file_ranges(",
    )
    loader_header = (kern / "platform/loader/elf_loader.hpp").read_text()
    metadata = fixture.capture(
        loader_header, "struct ElfLazyLoadRange ", declaration=True
    )
    additions = (
        """
#include <test/numa_lifecycle_mm_debt.hpp>
#include <platform/mm/lifecycle_census.hpp>
#include <abi/callnums/vmem.h>
#include <vfs/fs/tmpfs.hpp>
"""
        + core
        + """
namespace ker::vfs {
struct Stat {
    uint64_t st_dev{}, st_ino{};
    int64_t st_size{};
    struct Times { int64_t tv_sec{}, tv_nsec{}; } st_mtim{}, st_ctim{};
};
inline void vfs_retain_file(File* file) { ++file->refcount; }
}
namespace ker::mod::loader::elf {
"""
        + metadata
        + """
using ElfLazyLoadRangeVec = ker::util::SmallVec<ElfLazyLoadRange,16>;
}
namespace actual_exec {
namespace loader = ker::mod::loader;
namespace vfs = ker::vfs;
using ker::mod::sched::task::LazyVmemRange;
using ker::mod::sched::task::LazyVmemKind;
"""
        + append
        + "\n}\n"
    )
    exec_source = (kern / "syscalls_impl/process/exec.cpp").read_text()
    local_start = exec_source.index(
        "        LazyVmemRangeVec ranges;",
        exec_source.index("auto make_exec_selftest_owned_image("),
    )
    local_end = exec_source.index("        NativeExecImage const IMAGE{", local_start)
    local = exec_source[local_start:local_end]
    local = local.replace("        LazyVmemRangeVec ranges;\n", "")
    # Same actual interval and failure branch; supplied buffers are owned here.
    additions += (
        """
auto actual_local_exec_row(ker::mod::sched::task::LazyVmemRangeVec& ranges, ker::vfs::File* file) -> bool {
    using namespace ker::mod::sched::task;
    namespace vfs = ker::vfs;
    auto* buffer = new (std::nothrow) uint8_t[8]{};
    auto* name = new (std::nothrow) char[8]{};
"""
        + local.replace("return nullptr;", "return false;")
        + """
    delete[] buffer;
    delete[] name;
    return true;
}
"""
    )
    vmem = (kern / "syscalls_impl/vmem/sys_vmem.cpp").read_text()
    start = vmem.index(
        "#ifdef WOS_SELFTEST\n                ::ker::test::numa_lifecycle::mm_debt::begin_change();",
        vmem.index("if (!lifetime->reclassify") - 200,
    )
    end = vmem.index("                entry.lifetime = lifetime;", start)
    fragment = vmem[start:end]
    additions += """
void actual_native_row(ker::mod::sched::task::LazyVmemRangeVec& rows,
                       ker::mod::mm::file_mapping::FileMappingLifetime* lifetime) {
    namespace fm = ker::mod::mm::file_mapping;
    using ker::mod::sched::task::LazyVmemRange;
    using ker::mod::sched::task::LazyVmemKind;
    struct Child { ker::mod::sched::task::LazyVmemRangeVec& lazy_vmem_ranges; } child{rows};
    uint64_t address=0x60000, length=4096, offset=0, prot=1;
    bool read_shared_tmpfs=false;
    ker::vfs::Stat st{};
""" + fragment + "\n}\n"
    compartment = (kern / "test/compartment_ktest.cpp").read_text()
    additions += (
        """
namespace ker::vfs {
inline void (*unpublished_self_refs_test_hook)(ker::mod::sched::task::Task&){};
}
namespace actual_cutover {
namespace vfs = ker::vfs;
using Task = ker::mod::sched::task::Task;
"""
        + fixture.capture(compartment, "struct PutFile ", declaration=True)
        + """
using NativeFile = std::unique_ptr<vfs::File, PutFile>;
"""
        + fixture.capture(compartment, "struct LazyOwnershipCutover ", declaration=True)
        + "\n}\n"
    )
    # The fixtures above need definitions before the added test cases. The old
    # harness itself never names these types, so its 149 cases remain unchanged.
    source = source + "\n" + additions + EXTRA_CASES
    (scratch / "harness.cpp").write_text(source)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--cxx", default=os.environ.get("HOST_CXX", "/usr/bin/clang++"))
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    root = args.source_root.resolve(strict=True)
    fixture = load_fixture(root)
    started = time.monotonic()
    deadline = started + 120
    compiler = Path(args.cxx).resolve(strict=True)
    os.environ["UBSAN_OPTIONS"] = "halt_on_error=1:print_stacktrace=1"
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    result = {
        "qualification": "PENDING",
        "errors": [],
        "commands": [],
        "runtime_qualified": False,
    }
    evidence = args.evidence_dir.resolve() if args.evidence_dir else None
    if evidence:
        evidence.mkdir(exist_ok=False)
    paths = [
        Path(__file__).resolve(),
        compiler,
        root / "tests/host/unit/lazy_vmem_staging_runtime_test.py",
    ]
    paths += [
        root / "modules/kern/src" / name
        for name in [
            "platform/mm/virt.opt.cpp",
            "test/numa_lifecycle_mm_debt.hpp",
            "platform/mm/lifecycle_census.hpp",
            "util/smallvec.hpp",
            "platform/sched/lazy_vmem.hpp",
            "platform/sched/lazy_vmem.cpp",
            "platform/sched/task.hpp",
            "platform/sched/task.cpp",
            "syscalls_impl/process/exec.cpp",
            "platform/mm/file_mapping_lifetime.hpp",
            "platform/mm/file_mapping_lifetime.cpp",
            "vfs/core.cpp",
            "syscalls_impl/vmem/sys_vmem.cpp",
            "syscalls_impl/vmem/sys_vmem.hpp",
            "platform/sched/scheduler.cpp",
            "platform/sys/syscall_preparation.hpp",
            "platform/loader/elf_loader.hpp",
            "abi/callnums/vmem.h",
            "test/compartment_ktest.cpp",
        ]
    ]
    before = [held_pin(path) for path in paths]
    groups = []
    fixture.subreaper()
    scratch = None
    identity = None
    handlers = {
        number: signal.getsignal(number)
        for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    }

    def interrupted(number, _frame):
        raise KeyboardInterrupt(f"signal {number}")

    try:
        for number in handlers:
            signal.signal(number, interrupted)
        scratch = Path(tempfile.mkdtemp(prefix="wos-mm-debt-lazy-"))
        info = scratch.stat()
        identity = (info.st_dev, info.st_ino, info.st_uid)
        generate(root, scratch, fixture)
        flags = [
            str(compiler),
            "-std=c++23",
            "-DWOS_SELFTEST=1",
            "-DWOS_HOST_TEST=1",
            "-O1",
            "-g0",
            "-fsanitize=address,undefined",
            "-fno-omit-frame-pointer",
            "-I",
            str(scratch / "include"),
        ]
        units = [
            scratch / name for name in ("harness.cpp", "lazy_vmem.cpp", "family.cpp")
        ]
        dependency_paths = set()
        for index, unit in enumerate(units):
            depfile = scratch / f"dependencies-{index}.d"
            log = scratch / f"dependencies-{index}.log"
            row = fixture.command(
                flags + ["-M", "-MT", "actual", "-MF", str(depfile), str(unit)],
                log,
                scratch,
                deadline,
                groups,
            )
            result["commands"].append(row)
            if row["exit"]:
                raise ValueError(
                    f"dependency command {index} exit {row['exit']}: {log.read_text()}"
                )
            dependency_paths.update(
                Path(pin["path"]) for pin in fixture.dependency_pins(depfile)
            )
        external = sorted(
            path for path in dependency_paths if not path.is_relative_to(scratch)
        )
        generated = sorted(
            path for path in dependency_paths if path.is_relative_to(scratch)
        )
        external_before = [held_pin(path) for path in external]
        generated_before = [held_pin(path) for path in generated]
        result["external_dependencies_before"] = external_before
        result["generated_dependencies_before"] = generated_before
        argv = flags + [str(unit) for unit in units] + ["-o", str(scratch / "test")]
        for index, command in enumerate([argv, [str(scratch / "test")]]):
            log = scratch / f"command-{index}.log"
            row = fixture.command(command, log, scratch, deadline, groups)
            result["commands"].append(row)
            output = log.read_text(errors="replace")
            if output:
                print(output, end="", flush=True)
            if row["exit"]:
                raise ValueError(f"actual command{index} exit{row['exit']}")
        runtime_log = (scratch / "command-1.log").read_text()
        if not re.search(
            r"^PASS cases=149 assertions=\d+ failures=0 .*real_smallvec=1 real_staging=1 real_family_core=1 real_shared_transaction=1 shared_cases=46 real_public_fork_preparation=1 fork_cases=50$",
            runtime_log,
            re.M,
        ):
            raise ValueError("complete original 149-case fixture did not pass")
        if not re.search(
            r"^LAZY_OWNERS cases=34 assertions=\d+ failures=0 real_body=1 deferred_backend_unqualified=1$",
            runtime_log,
            re.M,
        ):
            raise ValueError("complete 34-case lazy ownership fixture did not pass")
        result["external_dependencies_after"] = [held_pin(path) for path in external]
        result["generated_dependencies_after"] = [held_pin(path) for path in generated]
        result["external_dependencies_held"] = (
            external_before == result["external_dependencies_after"]
        )
        result["generated_dependencies_held"] = (
            generated_before == result["generated_dependencies_after"]
        )
        if (
            not result["external_dependencies_held"]
            or not result["generated_dependencies_held"]
        ):
            raise ValueError("actual compiler dependency changed")
        if groups:
            raise ValueError("owned groups remain")
        result["qualification"] = "PASS_ACTUAL_LAZY_MM_DEBT_HOST_ONLY"
    except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
        result["errors"].append(repr(error))
        result["qualification"] = "FAIL_ACTUAL_LAZY_MM_DEBT_HOST_ONLY"
    finally:
        for number in handlers:
            signal.signal(number, signal.SIG_IGN)
        if scratch is not None:
            if evidence:
                for path in [*scratch.glob("*.cpp"), *scratch.glob("command-*.log")]:
                    if path.stat().st_size <= 2 << 20:
                        shutil.copyfile(path, evidence / path.name)
            info = scratch.stat()
            if (
                not groups
                and not scratch.is_symlink()
                and identity == (info.st_dev, info.st_ino, info.st_uid)
            ):
                shutil.rmtree(scratch)
                result["scratch_removed"] = not scratch.exists()
            else:
                result["errors"].append("owned scratch cleanup unproven")
                result["preserved_scratch"] = str(scratch)
        for number, handler in handlers.items():
            signal.signal(number, handler)
    result["input_pins"] = before
    result["input_pins_second"] = [held_pin(path) for path in paths]
    result["sources_held"] = before == result["input_pins_second"]
    if not result["sources_held"]:
        result["errors"].append("source changed")
    result["seconds"] = time.monotonic() - started
    if result["errors"]:
        result["qualification"] = "FAIL_ACTUAL_LAZY_MM_DEBT_HOST_ONLY"
    if evidence:
        (evidence / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, sort_keys=True))
    return int(bool(result["errors"]))


if __name__ == "__main__":
    raise SystemExit(main())
