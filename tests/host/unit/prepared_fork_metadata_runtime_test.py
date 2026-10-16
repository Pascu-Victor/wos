#!/usr/bin/env python3
"""Actual prepared fork metadata bodies, with declared Host context/physical endpoints.
No production fork wiring, real PTE/TLB, scheduling or backing I/O is qualified.
"""

from __future__ import annotations
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]
REPOSITORY = next(
    p for p in Path(__file__).resolve().parents if (p / ".github/instructions").is_dir()
)
HELPER = REPOSITORY / "tests/host/unit/file_mmap_registry_runtime_test.py"
TYPES = [
    ("file", "syscalls_impl/vmem/sys_vmem"),
    ("shm", "syscalls_impl/shm/shm"),
    ("swap", "platform/mm/virt"),
]


def load_helper():
    spec = importlib.util.spec_from_file_location("actual_registry_runner", HELPER)
    if spec is None or spec.loader is None:
        raise ValueError("registry runner unavailable")
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    return helper


def generate(root: Path, scratch: Path, mode: str, helper) -> list:
    base = root / "modules/kern/src"
    unit = Path(__file__).parent
    actual = []

    def read(rel):
        p = base / rel
        if not p.exists():
            p = REPOSITORY / "modules/kern/src" / rel
        actual.append(helper.pin(p))
        return p.read_text()

    def body(text, anchor, declaration=False):
        data, row = helper.capture(text, anchor, declaration=declaration)
        actual.append(row)
        return data + "\n"

    common = (unit / "prepared_fork_metadata_common.inc").read_text()
    paging = (REPOSITORY / "modules/kern/src/platform/mm/paging.hpp").read_text()
    constants = []
    for name in ["PAGE_WRITE", "PAGE_USER", "PAGE_COW", "PAGE_NX", "PAGE_RESERVED"]:
        matches = re.findall(
            r"^const static uint64_t " + name + r" = [^;]+;", paging, re.M
        )
        if len(matches) != 1:
            raise ValueError("actual paging constant " + name)
        constants += matches
    chunks = [common, "namespace ker::mod::mm::paging {" + "\n".join(constants) + "}"]
    if mode == "file":
        source = read("syscalls_impl/vmem/sys_vmem.cpp")
        header = read("syscalls_impl/vmem/sys_vmem.hpp")
        inc = scratch / "include"
        (inc / "util").mkdir(parents=True)
        (inc / "platform/mm/dyn").mkdir(parents=True)
        p = REPOSITORY / "modules/kern/src/util/smallvec.hpp"
        (inc / "util/smallvec.hpp").write_bytes(p.read_bytes())
        actual.append(helper.pin(p))
        (inc / "platform/mm/dyn/kmalloc.hpp").write_text("#pragma once\n")
        (inc / "util/hcf.hpp").write_text(
            "#pragma once\n#include <cstdlib>\n[[noreturn]] inline void hcf(){std::abort();}\n"
        )
        chunks += [
            "#include <util/smallvec.hpp>\nnamespace ker::vfs {struct File {std::atomic<int> refcount{1};};\n",
            body(
                (REPOSITORY / "modules/kern/src/vfs/core.cpp").read_text(),
                "auto vfs_try_retain_file(",
            ),
            """void vfs_put_file(File*f){if(!f)return;fixture::check(!fixture::pub&&!fixture::spin&&!fixture::mutex&&fixture::irq&&!fixture::preempt,"put outside every owner");fixture::check(f->refcount.fetch_sub(1)>0,"File live reference");++fixture::puts;} }\nnamespace ker::syscall::vmem {\n""",
            body(source, "struct FileMmapRange {", True),
            """ker::util::SmallVec<FileMmapRange,32> g_file_mmap_ranges;fixture::Mutex g_file_mmap_ranges_lock;struct NativeMappingEntry {ker::mod::mm::paging::PageTable*root{};NativeMappingEntry*registry_next{};};NativeMappingEntry*g_native_file_mmap_ranges{};\n#define private public\n""",
            body(header, "class LegacyFileForkPreparation {", True),
            "#undef private\n",
        ]
        a = source.index("namespace {\nauto prepared_fork_actor(")
        b = source.index("struct PublicFileForkPreparation::Entry", a)
        chunks += [source[a:b], "}\n"]
    elif mode == "shm":
        source = read("syscalls_impl/shm/shm.cpp")
        header = read("syscalls_impl/shm/shm.hpp")
        chunks += [
            "namespace ker::mod::security{struct IpcOwnership{};}namespace ker::mod::sched::ipc{struct NamespaceRef{};}namespace ker::abi::shm{constexpr int IPC_PRIVATE=0;}\nnamespace ker::syscall::shm {using ker::mod::sched::ipc::NamespaceRef;constexpr uint64_t USER_SPACE_START=0x400000,USER_SPACE_END=0x7fffffffffff;\n",
            body(source, "struct ShmSegment {", True),
            body(source, "struct ShmAttachment {", True),
            """fixture::Spin g_lock;std::array<ShmSegment,64>g_segments{};std::array<ShmAttachment,256>g_attachments{};\n""",
            body(source, "auto process_pid_for_task("),
            body(source, "auto find_retained_segment_by_id("),
            body(source, "auto find_free_attachment("),
            body(header, "class ShmForkPreparation {", True),
        ]
        a = source.index("namespace {\nauto prepared_shm_actor(")
        b = source.index("void shm_cleanup_for_task(", a)
        chunks += [source[a:b], "}\n"]
    elif mode == "swap":
        source = read("platform/mm/virt.opt.cpp")
        header = read("platform/mm/virt.hpp")
        front = read("platform/mm/managed_huge.cpp")
        policy = read("platform/mm/numa_policy.hpp")
        chunks += [
            '#include <bit>\nnamespace ker::mod::mm::numa{using NodeId=uint16_t;using NodeMask=uint64_t;constexpr size_t MAX_NODES=64;enum class Mode:uint8_t{LOCAL,PREFERRED,BIND,INTERLEAVE};struct Policy{NodeMask nodes{};Mode mode{Mode::LOCAL};};struct AllocationContext{Policy policy{};NodeId local_node{};uint64_t virtual_page_index{};};}\nnamespace ker::mod::mm::virt {using namespace ker::mod::mm::paging;using vaddr_t=uint64_t;namespace swap{struct SwapSlot{uint64_t id{};};unsigned frees{};auto free_slot(SwapSlot)->int{fixture::check(!fixture::pub&&!fixture::spin&&!fixture::mutex&&fixture::irq,"swap slot return outside owners");++frees;return 0;}}\nconstexpr size_t USER_PAGEMAP_ACCESS_GATE_NONE=size_t(-1);namespace detail{struct PreparedSwapForkAccess;}\n',
            "#define private public\n",
            body(header, "class PreparedAnonymousSwapFork {", True),
            "#undef private\n",
            """namespace detail {auto prepare_anonymous_swap_fork(sched::task::Task&,sched::task::Task&,const sys::SyscallPreparationScope&,PreparedAnonymousSwapFork&)->int;void reset_anonymous_swap_fork(PreparedAnonymousSwapFork&)noexcept;}\nnamespace paging = ker::mod::mm::paging;\n""",
            body(source, "enum class AnonymousSwapRuntimeState", True),
            body(source, "struct AnonymousSwapObject {", True),
            body(source, "enum class AnonymousPageinAllocation", True),
            body(source, "struct AnonymousSwapMapping {", True),
            """namespace debt=::ker::test::numa_lifecycle::mm_debt;using DebtField=::ker::test::numa_lifecycle::CounterField;fixture::Spin cow_pte_lock;AnonymousSwapMapping*anonymous_swap_mappings{};struct{std::atomic<uint64_t>swapped_pages{};}anonymous_swap_stats;void anonymous_swap_add(std::atomic<uint64_t>&v){++v;}\n""",
        ]
        for anchor in [
            "auto allocate_swap_descriptor(",
            "void delete_swap_descriptor(",
            "void insert_anonymous_swap_mapping_locked(",
            "void remove_anonymous_swap_mapping_locked(",
            "auto detach_anonymous_swap_object_locked(",
            "void release_anonymous_swap_object(",
            "auto anonymous_swap_marker_raw(",
        ]:
            if anchor.startswith(("auto allocate_swap", "void delete_swap")):
                chunks += ["template<typename T>\n"]
            chunks += [body(source, anchor)]
        chunks += [
            "} namespace ker::mod::mm::numa {",
            body(policy, "[[nodiscard]] constexpr auto valid_policy("),
            "} namespace ker::mod::mm::virt {",
            (unit / "prepared_fork_swap_endpoints.inc").read_text(),
        ]
        a = source.index("struct PreparedAnonymousSwapFork::Storage")
        b = source.index("\nnamespace {\nauto clone_anonymous_swap_mappings", a)
        chunks += [
            source[a:b],
            body(source, "auto clone_anonymous_swap_mappings("),
            body(source, "void purge_anonymous_swap_mappings("),
        ]
        for anchor in [
            "PreparedAnonymousSwapFork::PreparedAnonymousSwapFork(",
            "PreparedAnonymousSwapFork::~PreparedAnonymousSwapFork(",
            "auto PreparedAnonymousSwapFork::prepare(",
        ]:
            if anchor.startswith(
                "PreparedAnonymousSwapFork::PreparedAnonymousSwapFork("
            ):
                chunks += [
                    "PreparedAnonymousSwapFork::PreparedAnonymousSwapFork() noexcept = default;\n"
                ]
            else:
                chunks += [body(front, anchor)]
        chunks += ["}\n"]
    else:
        raise ValueError(mode)
    chunks += [(unit / f"prepared_fork_{mode}_cases.inc").read_text()]
    (scratch / f"{mode}.cpp").write_text("\n".join(chunks))
    return actual


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument(
        "--cxx", default=os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++")
    )
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--mode", choices=["file", "swap", "shm", "all"], default="all")
    parser.add_argument(
        "--control",
        choices=[
            "none",
            "file-retain",
            "swap-transfer",
            "shm-capacity",
            "legacy-swap-allocation",
            "prepared-descriptor-throwing",
        ],
        default="none",
    )
    args = parser.parse_args()
    helper = load_helper()
    result = {
        "qualification": "FAIL",
        "errors": [],
        "commands": [],
        "actual_definitions": {},
        "limits": [
            "Host Task/current/PUB/IRQ/Spinlock, root serial, PTE lookup and swap I/O endpoints; no scheduling/PTE/TLB/backing qualification.",
            "Actual metadata/container bodies only; private internals inspected for refusal coverage.",
        ],
    }
    scratch = None
    groups = []
    identity = None
    watched = []
    before = []
    handlers = {}
    command_active, deferred_signal = False, None
    output = (
        args.evidence_dir.absolute()
        if args.evidence_dir
        else Path(tempfile.mkdtemp(prefix="wos-prepared-fork-evidence-"))
    )
    if args.evidence_dir:
        output.mkdir(exist_ok=False)
    deadline = time.monotonic() + 110
    try:
        compiler = Path(args.cxx).resolve(strict=True)
        root = args.source_root.resolve(strict=True)
        helper.subreaper()

        def interrupted(number, frame):
            nonlocal deferred_signal
            if command_active:
                deferred_signal = number
                return
            raise InterruptedError("signal " + str(number))

        handlers = {
            sig: signal.signal(sig, interrupted)
            for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]
        }
        modes = ["file", "shm", "swap"] if args.mode == "all" else [args.mode]
        watched = [
            Path(__file__).resolve(),
            HELPER,
            compiler,
            *Path(__file__).parent.glob("prepared_fork_*.inc"),
        ]
        for _, stem in TYPES:
            suffix = ".opt.cpp" if stem.endswith("/virt") else ".cpp"
            watched.extend(
                [
                    root / "modules/kern/src" / Path(stem + suffix),
                    root / "modules/kern/src" / Path(stem + ".hpp"),
                ]
            )
        watched += [
            root / "modules/kern/src/platform/mm/managed_huge.cpp",
            REPOSITORY / "modules/kern/src/util/smallvec.hpp",
            REPOSITORY / "modules/kern/src/vfs/core.cpp",
            REPOSITORY / "modules/kern/src/platform/mm/paging.hpp",
            REPOSITORY / "modules/kern/src/platform/mm/numa_policy.hpp",
        ]
        watched = sorted(set(watched))
        before = [helper.pin(p) for p in watched]
        result["before"] = before
        scratch = Path(tempfile.mkdtemp(prefix="wos-prepared-fork-"))
        st = scratch.stat()
        identity = (st.st_dev, st.st_ino, st.st_uid)
        for mode in modes:
            result["actual_definitions"][mode] = generate(root, scratch, mode, helper)
            binary = scratch / mode
            generated = scratch / f"{mode}.cpp"
            text = generated.read_text()
            controls = {
                "file-retain": (
                    "bool const acquired = ker::vfs::vfs_try_retain_file(file);",
                    "bool const acquired = true;",
                ),
                "swap-transfer": (
                    "plan.storage_->rows[adopted++] = nullptr;",
                    "++adopted;",
                ),
                "shm-capacity": (
                    "if (count > free) return -ENOSPC;",
                    "if (false) return -ENOSPC;",
                ),
            }
            if args.control in controls:
                old, new = controls[args.control]
                if text.count(old) != 1:
                    raise ValueError("control target not unique")
                generated.write_text(text.replace(old, new))
                result["control"] = args.control
            if args.control == "prepared-descriptor-throwing":
                # Invoke the unchanged real compatibility helper without the
                # former -fcheck-new masking. Only this legacy contrast may
                # fault on a returned-null descriptor before initialization.
                old = """#ifdef WOS_SELFTEST
                debt::begin_change();
#endif
                plan.storage_->rows[i] = new (std::nothrow) AnonymousSwapMapping{};
#ifdef WOS_SELFTEST
                if (plan.storage_->rows[i] != nullptr) debt::add(DebtField::SWAP_MAPPINGS);
                debt::end_change();
#endif"""
                if text.count(old) != 1:
                    raise ValueError("prepared descriptor contrast not unique")
                generated.write_text(
                    text.replace(
                        old,
                        "plan.storage_->rows[i] = allocate_swap_descriptor<AnonymousSwapMapping>(DebtField::SWAP_MAPPINGS);",
                    )
                )
                result["control"] = args.control
            if args.control == "legacy-swap-allocation":
                old = r"int\s+main\(\)\s*\{\s*fixture::swap_tests\(\);\s*\}"
                new = 'int main(){fixture::SwapWorld world;world.seed(0);fixture::Pub pub;fixture::check(ker::mod::mm::virt::clone_anonymous_swap_mappings(&world.source,&world.childroot),"old clone");}'
                if len(re.findall(old, text)) != 1:
                    raise ValueError("legacy control actual target not unique")
                generated.write_text(re.sub(old, lambda _: new, text))
                result["control"] = args.control
            argv = [
                str(compiler),
                "-std=c++23",
                "-O1",
                "-g0",
                "-Wno-new-returns-null",
                "-fsanitize=address,undefined",
                "-fno-omit-frame-pointer",
                "-DWOS_SELFTEST=1",
                "-I" + str(scratch / "include"),
                str(scratch / f"{mode}.cpp"),
                "-o",
                str(binary),
            ]
            shutil.copy2(scratch / f"{mode}.cpp", output / f"{mode}.cpp")
            for n, cmd in enumerate([argv, [str(binary)]]):
                log = scratch / f"{mode}-{n}.log"
                command_active = True
                try:
                    row = helper.command(cmd, log, scratch, deadline, groups)
                finally:
                    command_active = False
                if deferred_signal is not None:
                    raise InterruptedError("deferred signal " + str(deferred_signal))
                result["commands"].append(row)
                text = log.read_text(errors="replace")
                (output / log.name).write_text(text)
                row["retained_log"] = helper.pin(output / log.name)
                if n == 1 and args.control != "none":
                    if args.control == "prepared-descriptor-throwing":
                        if row["exit"] == 0 or not (
                            "null pointer" in text or "zero page" in text
                        ):
                            raise ValueError(
                                "old descriptor helper did not expose returned-null initialization"
                            )
                    elif row["exit"] not in [-6, 134] or "ASSERT:" not in text:
                        raise ValueError(
                            "compiled control lacked semantic assertion refusal"
                        )
                    result["compiled_control_rejected"] = True
                    continue
                if row["exit"] != 0:
                    raise ValueError(
                        f"{mode} command {n} exit {row['exit']}: {text[-9000:]}"
                    )
                if n == 1 and not re.fullmatch(
                    r"PASS "
                    + mode
                    + r" cases=\d+ checks=\d+( retains=\d+ puts=\d+)?\n",
                    text,
                ):
                    raise ValueError("missing actual terminal oracle")
            shutil.copy2(scratch / f"{mode}.cpp", output / f"{mode}.cpp")
        result["qualification"] = "PASS_ACTUAL_PREPARED_METADATA_HOST_ONLY"
    except BaseException as error:
        result["errors"].append(repr(error))
    finally:
        if groups:
            result["errors"].append(
                "owned session uncertain; preserve scratch and evidence"
            )
        try:
            result["after"] = [helper.pin(p) for p in watched]
            result["source_held"] = before == result["after"]
        except BaseException as error:
            result["errors"].append("recheck " + repr(error))
        try:
            if scratch and not groups:
                st = scratch.stat()
                if (
                    (st.st_dev, st.st_ino, st.st_uid) == identity
                    and st.st_uid == os.getuid()
                    and not scratch.is_symlink()
                ):
                    shutil.rmtree(scratch)
                    result["scratch_removed"] = True
            if scratch and scratch.exists():
                result["preserved_scratch"] = str(scratch)
        except BaseException as error:
            result["errors"].append("scratch cleanup " + repr(error))
            if scratch:
                result["preserved_scratch"] = str(scratch)
        result["pending_owned_groups"] = groups
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
        if result["errors"] or not result.get("source_held"):
            result["qualification"] = "FAIL"
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {
                "qualification": result["qualification"],
                "errors": result["errors"],
                "evidence": str(output),
            }
        )
    )
    return int(result["qualification"] == "FAIL")


if __name__ == "__main__":
    raise SystemExit(main())
