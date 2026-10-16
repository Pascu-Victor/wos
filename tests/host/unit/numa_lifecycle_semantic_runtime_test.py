#!/usr/bin/env python3
"""Exercise actual current-census epochs, root/index and child ownership bodies.

Task/IRQ/namespace/PMM/retirement-port endpoints are explicit Host seams.
No native scheduler, physical allocator, callback executor or SMP qualification.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError("missing actual Host process/extraction helper")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def generate(root, source_root, scratch, capture, variant):
    used = {}
    fragments = []

    def text(relative):
        staged = source_root / "modules/kern/src" / relative
        path = staged if staged.is_file() else root / "modules/kern/src" / relative
        data = path.read_text()
        used[str(path)] = hashlib.sha256(data.encode()).hexdigest()
        return data

    def definition(data, anchor, declaration=False, template=False):
        body, receipt = capture(data, anchor, declaration=declaration)
        if template:
            offset = data.index(body)
            previous = data[:offset].rstrip().rsplit("\n", 1)[-1]
            if not previous.startswith("template <"):
                raise ValueError("actual template declaration missing: " + anchor)
            body = previous + "\n" + body
        fragments.append(receipt)
        return body

    def region(data, first, last):
        a = data.index(first)
        return data[a : data.index(last, a)]

    task = text("platform/sched/task.hpp")
    child = text("syscalls_impl/process/child_events.cpp")
    child_header = text("syscalls_impl/process/child_events.hpp")
    virt = text("platform/mm/virt.opt.cpp")
    virt_header = text("platform/mm/virt.hpp")
    index = text("platform/mm/file_mapping_lifetime.cpp")
    index_header = text("platform/mm/file_mapping_lifetime.hpp")
    text("platform/mm/lifecycle_census.hpp")
    page_alloc = text("platform/mm/page_alloc.hpp")
    address = text("platform/mm/addr.hpp")
    # Actual scalar type declarations; address translation remains a Host seam.
    address_aliases = "\n".join(
        next(
            line
            for line in address.splitlines()
            if line.startswith("using " + name + " =")
        )
        for name in ("vaddr_t", "paddr_t")
    )
    shell = Path(__file__).with_suffix(".inc").read_text()
    child_types = region(task, "enum class ChildMembershipState", "struct Context {")
    declarations = region(
        child_header, "constexpr uint64_t SELECT_ANY", "struct ReapedCpuTimes"
    )
    declarations += definition(child_header, "enum class ProbeResult", True) + "\n"
    declarations += (
        definition(child_header, "struct SelftestLifecycleCensus", True) + "\n"
    )
    declarations += (
        definition(child_header, "struct SelftestOwnerLifecycleCensus", True) + "\n"
    )
    global_start = child.index(
        "ker::mod::mm::lifecycle_census::Epoch selftest_child_epoch{};"
    )
    global_end = child.index("#else", global_start)
    source = child[global_start:global_end]
    source += "\nstd::atomic<uint64_t> g_event_sequence{1}, g_claim_cookie{1}, g_waiter_generation{1};\n"
    source += "SelftestLifecycleCensus selftest_lifecycle_edges{}; uint64_t g_group_version{1};\n"
    anchors = [
        "void selftest_edge_add(",
        "void selftest_edge_remove(",
        "auto take_lifetime_ref(",
        "void drop_lifetime_ref(",
        "void advance_version_locked(",
        "void advance_queue_version_locked(",
        "void child_list_insert_locked(",
        "void child_list_remove_locked(",
        "void queue_insert_locked(",
        "auto has_queued_exit_event_locked(",
        "void update_unparented_membership_locked(",
        "auto subject_claimed_by_owner_locked(",
        "void publish_subject_claim_locked(",
        "auto enqueue_event_locked(",
        "void clear_event_claim_locked(",
        "void queue_unlink_locked(",
        "void remove_event_locked(",
        "void move_event_locked(",
        "void remove_subject_events_locked(",
        "auto selector_matches(",
        "auto subject_matches(",
        "auto event_matches(",
        "auto first_matching_event_locked(",
        "auto has_potential_subject_locked(",
        "void waiter_unlink_locked(",
        "auto waiter_register_locked(",
        "auto select_waiter_locked(",
        "auto select_no_child_waiter_locked(",
        "auto make_wake_selection_locked(",
        "void deliver_wake(",
        "void wake_no_child_waiters(",
        "void tracee_list_insert_locked(",
        "void tracee_list_remove_locked(",
        "void unlink_tracer_locked(",
        "auto transfer_parent_events_locked(",
        "auto acquire_process_owner(",
        "auto begin_publication(",
        "auto commit_publication(",
        "void abort_publication(",
        "auto publish_exit(",
        "auto publish_job_control_stop(",
        "auto publish_continued(",
        "auto publish_ptrace_stop(",
        "auto acknowledge_ptrace_stop(",
        "auto attach_tracer(",
        "void detach_tracer(",
        "auto private_reaper_matches(",
        "void prepare_process_exit_impl(",
        "void prepare_process_exit(",
        "void cancel_wait(",
        "auto claim_locked(",
        "auto public_admission_locked(",
        "auto try_claim(",
        "auto claim_or_register(",
        "void cancel_claim(",
        "void release_claim(Claim& claim",
        "auto commit_claim(Task& owner, Claim& claim",
        "auto selftest_lifecycle_census()",
        "auto selftest_owner_lifecycle_census(",
        "void selftest_enable_current_lifecycle_census()",
        "void selftest_current_lifecycle_census_into(",
        "void selftest_current_owner_lifecycle_census_into(",
    ]
    source += definition(child, "struct WakeSelection", True) + "\n"
    source += "constexpr int32_t STOP_STATUS_LOW=0x7f, CONTINUED_STATUS=0xffff;\n"
    for anchor in anchors:
        source += (
            definition(
                child, anchor, template=anchor == "void prepare_process_exit_impl("
            )
            + "\n"
        )
    if variant == "empty-cancel-mutant":
        before = "if (REGISTRATION_OWNER != nullptr || waiter.child_claim.event != nullptr) current_operation.begin();"
        assert source.count(before) == 1
        source = source.replace(before, "current_operation.begin();")
    elif variant == "cancel-tail-mutant":
        before = "if (REGISTRATION_OWNER != nullptr || waiter.child_claim.event != nullptr) current_operation.begin();"
        assert source.count(before) == 1
        source = source.replace(before, "static_cast<void>(0);")
    elif variant == "registration-mutant":
        before, _ = capture(child, "auto waiter_register_locked(")
        changed = before.replace(
            "CurrentChildSemanticOperation current_operation{true};",
            "CurrentChildSemanticOperation current_operation;",
        )
        assert before != changed and source.count(before) == 1
        source = source.replace(before, changed)
    elif variant == "legacy-reader-control":
        before, _ = capture(child, "void selftest_current_lifecycle_census_into(")
        after = """void selftest_current_lifecycle_census_into(SelftestLifecycleCensus& out,
                          ker::mod::mm::lifecycle_census::Stamp& before) noexcept {
            out = {};
            if (!__atomic_load_n(&selftest_current_child_enabled, __ATOMIC_ACQUIRE)) return;
            out = selftest_lifecycle_census(); before = out.stamp;
        }"""
        assert source.count(before) == 1
        source = source.replace(before, after)

    index_declarations = (
        definition(index_header, "enum class BindingOwner", True) + "\n"
    )
    index_declarations += (
        definition(index_header, "struct BindingOwnerCounts", True) + "\n"
    )
    index_declarations += "class RetirementCompletion; class FileMappingIndex; class FileMappingTlbRetirement;\n"
    binding = definition(index_header, "class FileMappingBinding {", True).replace(
        "   private:", "   public:"
    )
    index_declarations += (
        binding
        + "\n"
        + definition(index_header, "class FileMappingTlbRetirement {", True)
        + "\n"
    )
    index_declarations += (
        definition(index_header, "enum class FileMappingPinResult", True) + "\n"
    )
    index_declarations += (
        definition(index_header, "class FileMappingIndex {", True) + "\n"
    )
    # Declare the scalar production hooks before their actual index callers.
    index_source = """namespace root_hook = ker::mod::mm::virt;
"""
    index_source += region(
        index,
        "constinit sys::Spinlock owner_count_lock{};",
        "template <typename Counts>\nauto counts_valid",
    )
    for anchor, templated in [
        ("auto release_owner(", True),
        ("auto reclassify_owner(", True),
        ("FileMappingBinding::FileMappingBinding(", False),
        ("void FileMappingBinding::release(", False),
        ("auto FileMappingBinding::reclassify(", False),
        ("auto FileMappingBinding::configure_unpublished(", False),
        ("FileMappingTlbRetirement::~FileMappingTlbRetirement()", False),
        ("void FileMappingTlbRetirement::append(", False),
        ("void FileMappingTlbRetirement::release_after_completed_tlb()", False),
        ("auto FileMappingIndex::bucket(", False),
        ("auto FileMappingIndex::root_matches_locked(", False),
        ("auto FileMappingIndex::attach_locked(", False),
        ("auto FileMappingIndex::detach_locked(", False),
        ("auto FileMappingIndex::detach_root_locked(", False),
        ("auto FileMappingIndex::root_empty_locked(", False),
        ("auto FileMappingIndex::selftest_count_all_locked(", False),
    ]:
        index_source += definition(index, anchor, template=templated) + "\n"
    if variant == "return-tail-mutant":
        before = "virt::selftest_current_root_detached_binding_returned();"
        assert index_source.count(before) == 1
        index_source = index_source.replace(before, "static_cast<void>(0);")

    root_declarations = (
        definition(virt_header, "struct UserRootControlStats", True) + "\n"
    )
    for anchor in (
        "enum class SelftestRootAllocationSource",
        "enum class SelftestRootReturnDisposition",
        "struct SelftestRootPoolInsertion",
        "struct SelftestRootReleaseOutput",
    ):
        root_declarations += definition(virt_header, anchor, True) + "\n"
    root_declarations += (
        definition(virt_header, "struct SelftestRootFactoryOptions", True) + "\n"
    )
    root_declarations += (
        definition(virt_header, "struct SelftestRootFactoryOutput", True) + "\n"
    )
    root_declarations += (
        definition(virt_header, "struct SelftestRootCensus", True) + "\n"
    )
    root_source = region(
        virt,
        "constexpr size_t USER_ROOT_CONTROL_BUCKET_COUNT",
        "enum class AnonymousSwapRuntimeState",
    )
    root_source += definition(virt, "class CurrentRootSemanticOperation", True) + "\n"
    for anchor in [
        "auto user_root_control_bucket(",
        "auto find_user_root_control_locked(",
        "auto next_user_root_serial(",
        "enum class UserRootAttachResult",
        "auto attach_user_root_control_locked(",
        "auto allocate_user_root_control_chunk(",
        "struct RootReserveLocality",
        "auto capture_root_reserve_locality(",
        "auto return_root_to_emergency_reserve(",
        "void selftest_capture_root_release(",
        "void release_unregistered_user_root(",
        "auto create_user_root_common(",
        "auto retire_user_root_control_locked(",
        "void release_pagemap_exclusive_owned(",
        "auto capture_user_root_incarnation(",
        "auto selftest_root_census()",
        "void selftest_enable_current_root_census()",
        "auto selftest_current_root_change_begin()",
        "void selftest_current_root_change_end(",
        "void selftest_current_root_detached_binding_returned()",
        "void selftest_current_root_census_into(",
    ]:
        root_source += (
            definition(virt, anchor, anchor.startswith(("enum ", "struct "))) + "\n"
        )
    for marker, value in [
        ("PAGE_KIND", definition(page_alloc, "enum class PageKind", True)),
        ("ADDRESS_ALIASES", address_aliases),
        ("CHILD_TYPES", child_types),
        ("CHILD_DECLARATIONS", declarations),
        ("CHILD_SOURCE", source),
        ("INDEX_DECLARATIONS", index_declarations),
        ("INDEX_SOURCE", index_source),
        ("ROOT_DECLARATIONS", root_declarations),
        ("ROOT_SOURCE", root_source),
        (
            "CASES",
            Path(__file__).with_name("numa_lifecycle_semantic_cases.inc").read_text(),
        ),
    ]:
        token = "/* @" + marker + "@ */"
        assert shell.count(token) == 1
        shell = shell.replace(token, value)
    (scratch / "actual-semantic.cpp").write_text(shell)
    (scratch / "extraction.json").write_text(
        json.dumps(dict(used=used, fragments=fragments), indent=2) + "\n"
    )
    return used


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--controls", action="store_true")
    parser.add_argument(
        "--cxx", default=os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++")
    )
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    source_root = (args.source_root or root).resolve(strict=True)
    helper = load(
        root / "tests/host/unit/file_mmap_registry_runtime_test.py",
        "bounded_semantic_process",
    )
    capture = load(
        root / "tests/host/unit/futex_root_identity_runtime_test.py",
        "actual_semantic_capture",
    ).capture
    output = args.evidence_dir
    automatic = output is None
    if output is None:
        output = Path(tempfile.mkdtemp(prefix="wos-semantic-evidence-"))
    else:
        output.mkdir(mode=0o700)
    scratch = None
    groups = []
    report = dict(
        pass_=False,
        errors=[],
        commands=[],
        runtime_qualified=False,
        pending_groups=[],
        preserved_scratch=None,
    )
    handlers = {
        s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    }
    watched = [
        Path(__file__).resolve(),
        Path(__file__).with_suffix(".inc"),
        Path(__file__).with_name("numa_lifecycle_semantic_cases.inc"),
        root / "tests/host/unit/file_mmap_registry_runtime_test.py",
        root / "tests/host/unit/futex_root_identity_runtime_test.py",
    ]
    before = []
    try:
        for signum in handlers:
            signal.signal(
                signum,
                lambda signum, frame: (_ for _ in ()).throw(
                    KeyboardInterrupt(str(signum))
                ),
            )
        helper.subreaper()
        scratch = Path(tempfile.mkdtemp(prefix="semantic-", dir=output))
        seam = scratch / "platform/sys/spinlock.hpp"
        seam.parent.mkdir(parents=True)
        seam.write_text("""#pragma once
namespace ker::mod::sys {
class Spinlock {
public:
 std::mutex mutex; bool held{};
 void lock() { fixture::check(!held,"nonrecursive actual lock order");mutex.lock();held=true;++fixture::locks; }
 void unlock() { fixture::check(held&&fixture::locks!=0,"matched actual unlock");held=false;--fixture::locks;mutex.unlock();if(fixture::on_unlock)fixture::on_unlock(this); }
 auto try_lock()->bool {if(held||!mutex.try_lock())return false;held=true;++fixture::locks;return true;}
 auto lock_irqsave()->uint64_t {auto old=fixture::irq_depth;lock();++fixture::irq_depth;return old;}
 void unlock_irqrestore(uint64_t old) {fixture::check(fixture::irq_depth==old+1,"nested IRQ restoration");fixture::irq_depth=old;unlock();}
};
}
""")
        variants = ["current"] + (
            [
                "legacy-reader-control",
                "empty-cancel-mutant",
                "cancel-tail-mutant",
                "registration-mutant",
                "return-tail-mutant",
            ]
            if args.controls
            else []
        )
        deadline = time.monotonic() + 180
        compiler = shutil.which(args.cxx)
        if compiler is None:
            raise ValueError("Host C++ compiler unavailable")
        for variant in variants:
            used = generate(root, source_root, scratch, capture, variant)
            shutil.copyfile(
                scratch / "actual-semantic.cpp", output / (variant + "-actual.cpp")
            )
            shutil.copyfile(
                scratch / "extraction.json", output / (variant + "-extraction.json")
            )
            for p in used:
                path = Path(p)
                if path not in watched:
                    watched.append(path)
            if not before:
                before = [helper.pin(p) for p in watched]
            binary = scratch / (variant + ".bin")
            depfile = scratch / (variant + ".d")
            argv = [
                compiler,
                "-std=c++23",
                "-O1",
                "-g",
                "-DWOS_SELFTEST=1",
                "-fsanitize=address,undefined",
                "-fno-omit-frame-pointer",
                "-pthread",
                "-I" + str(scratch),
                "-I" + str(source_root / "modules/kern/src"),
                "-I" + str(root / "modules/kern/src"),
                "-MD",
                "-MF",
                str(depfile),
                str(scratch / "actual-semantic.cpp"),
                "-o",
                str(binary),
            ]
            compiled = helper.command(
                argv, output / (variant + "-compile.log"), scratch, deadline, groups
            )
            report["commands"].append(compiled)
            if compiled["exit"] != 0:
                raise ValueError("actual semantic body compile refused " + variant)
            command = helper.command(
                [str(binary)],
                output / (variant + "-runtime.log"),
                scratch,
                deadline,
                groups,
            )
            report["commands"].append(command)
            report.setdefault("dependencies", {})[variant] = helper.dependency_pins(
                depfile
            )
            if variant == "current":
                if command["exit"] != 0:
                    raise ValueError("actual semantic current runtime refused")
            else:
                expected = {
                    "legacy-reader-control": "empty public polling preserves current owner stamp",
                    "empty-cancel-mutant": "empty public polling preserves current owner stamp",
                    "cancel-tail-mutant": "unlinked registration stays inflight through real IRQ unlock",
                    "registration-mutant": "in-place registration update invalidates unchanged pair",
                    "return-tail-mutant": "actual destructive release returns semantic inflight owner",
                }[variant]
                observed = (output / (variant + "-runtime.log")).read_text()
                if (
                    command["exit"] != -signal.SIGABRT
                    or "FAIL: " + expected not in observed
                ):
                    raise ValueError(
                        "compiled control missed its exact actual assertion " + variant
                    )
                report.setdefault("logical_controls", {})[variant] = expected
        report["pass_"] = True
    except BaseException as error:
        report["errors"].append(type(error).__name__ + ": " + str(error))
    finally:
        try:
            report["source_before"] = before
            report["source_after"] = [helper.pin(Path(p["path"])) for p in before]
            report["source_held"] = bool(before) and before == report["source_after"]
            if not report["source_held"]:
                report["errors"].append("source hold failed")
        except BaseException as error:
            report["errors"].append("source recheck " + repr(error))
        for group in list(groups):
            try:
                for identity in helper.owned_session(group):
                    helper.kill_identity(identity)
                until = time.monotonic() + 3
                while helper.owned_session(group) and time.monotonic() < until:
                    for identity in helper.owned_session(group):
                        try:
                            os.waitpid(identity[0], os.WNOHANG)
                        except ChildProcessError:
                            pass
                    time.sleep(0.02)
                if helper.owned_session(group):
                    report["pending_groups"].append(group)
            except BaseException as error:
                report["pending_groups"].append(group)
                report["errors"].append("owned group cleanup " + repr(error))
        if scratch is not None:
            for name in ("actual-semantic.cpp", "extraction.json"):
                try:
                    path = scratch / name
                    if path.exists():
                        shutil.copyfile(path, output / name)
                except BaseException as error:
                    report["errors"].append("evidence copy " + repr(error))
            if not report["pending_groups"]:
                try:
                    shutil.rmtree(scratch)
                except BaseException as error:
                    report["errors"].append("scratch cleanup " + repr(error))
                    report["preserved_scratch"] = str(scratch)
            else:
                report["preserved_scratch"] = str(scratch)
        for signum, handler in handlers.items():
            try:
                signal.signal(signum, handler)
            except BaseException as error:
                report["errors"].append("restore handler " + repr(error))
        report["pass_"] &= not report["errors"] and not report["pending_groups"]
        (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            dict(pass_=report["pass_"], errors=report["errors"], evidence=str(output))
        )
    )
    if automatic and report["pass_"]:
        shutil.rmtree(output)
    return 0 if report["pass_"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
