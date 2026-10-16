#!/usr/bin/env python3
"""Exercise real slab/mini bodies and the small kmalloc branch with explicit ports.

The physical allocator, privileged IRQ operations and optional debug-slot backend
are Host endpoints. These checks do not execute native physical OOM or certify
IRQ/SMP safety. Medium/large allocation bodies are outside this fixture.
"""
from pathlib import Path
import hashlib
import json
import os
import shutil
import signal
import subprocess
import tempfile
import time

ROOT = Path(os.environ.get("WOS_KMALLOC_ROOT", Path(__file__).resolve().parents[3]))
KERNEL = ROOT / "modules/kern/src"
SOURCE = Path(os.environ.get("WOS_KMALLOC_SOURCE_ROOT", KERNEL))
CASES = Path(os.environ.get("WOS_KMALLOC_CASES", Path(__file__).with_name("kmalloc_nothrow_cases.inc")))
OVERALL_SECONDS = 85
COMMAND_SECONDS = 20
STORAGE_BYTES = 128 * 1024 * 1024
PENDING_INTERRUPT = None


def defer_interrupt(number, _frame):
    global PENDING_INTERRUPT
    if PENDING_INTERRUPT is None:
        PENDING_INTERRUPT = number


def check_interrupt() -> None:
    if PENDING_INTERRUPT is not None:
        raise KeyboardInterrupt(str(PENDING_INTERRUPT))


def group_absent(pid: int) -> bool:
    try:
        os.killpg(pid, 0)
    except ProcessLookupError:
        return True
    except OSError:
        return False
    return False


def await_natural_group_absence(pid: int, deadline: float) -> None:
    """Accept a leader result only after its group has disappeared naturally."""
    while True:
        check_interrupt()
        if time.monotonic() >= deadline:
            raise RuntimeError("owned allocator Host natural-exit work deadline")
        absent = False
        try:
            os.killpg(pid, 0)
        except ProcessLookupError:
            absent = True
        check_interrupt()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("owned allocator Host group remained through natural-exit deadline")
        if absent:
            return
        time.sleep(min(0.01, remaining))


def settle_group(process: subprocess.Popen, deadline: float) -> bool:
    """Reap our leader and prove its new-session group absent within the bound."""
    try:
        if not group_absent(process.pid):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        process.communicate(timeout=max(0, deadline - time.monotonic()))
        if process.returncode is None:
            return False
        while not group_absent(process.pid):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(0.01, remaining))
        return True
    except BaseException:
        return False


def source(relative: str) -> str:
    private = SOURCE / relative
    return (private if private.exists() else KERNEL / relative).read_text()


def definition(text: str, anchor: str) -> str:
    if text.count(anchor) != 1:
        raise RuntimeError(f"actual definition changed: {anchor}")
    start = text.index(anchor)
    end = text.index("{", start) + 1
    depth = 1
    while depth:
        depth += (text[end] == "{") - (text[end] == "}")
        end += 1
    return text[start:end + (text[end:end + 1] == ";")]


def generated(kmalloc: str, mini: str) -> tuple[str, list[dict]]:
    actual = []
    def take(anchor: str) -> str:
        body = definition(kmalloc, anchor)
        actual.append({"anchor": anchor, "sha256": hashlib.sha256(body.encode()).hexdigest(), "source": body})
        return body
    enum = take("enum class SlabClass")
    allocator = take("struct PerCpuAllocator {")
    index = take("auto size_to_slab_idx(")
    reverse_index = take("auto slab_size_to_idx(")
    alloc = take("auto malloc_impl(")
    alloc = alloc[:alloc.index("\n    // Tier 2:")] + "\n    return nullptr;\n}"
    free = take("void free(void* ptr)")
    free = "void free(void* ptr) { if (ptr == nullptr) return;\n" + free[free.index("    // Otherwise, it's a small allocation"):]
    # Exactly two save statements and their conditional restore statements are
    # transcribed to explicit Host IRQ endpoints. Surrounding bodies stay actual.
    bodies = alloc + "\n" + free
    save = 'asm volatile("pushfq; popq %0; cli" : "=r"(flags));'
    if bodies.count(save) != 2 or bodies.count('asm volatile("sti");') != 5:
        raise RuntimeError("actual small IRQ blocks changed")
    bodies = bodies.replace(save, "flags = host_slab::save_irq();").replace('asm volatile("sti");', "host_slab::restore_irq();")
    wrappers = take("auto malloc(uint64_t size)") + "\n" + take("auto malloc_may_fail(uint64_t size)")
    global_wrappers = []
    for anchor, renamed in [("auto operator new(std::size_t size,", "allocate_scalar_nothrow"),
                            ("auto operator new[](std::size_t size,", "allocate_array_nothrow")]:
        body = take(anchor)
        body = body.replace("operator new[]" if "new[]" in anchor else "operator new", renamed, 1)
        global_wrappers.append(body)
    prolog = """
#include <minimalist_malloc/slab_allocator.hpp>
namespace ker::mod::mm::dyn::kmalloc {
namespace mini_malloc = ker::mod::mm::mini_malloc;
constexpr int NUM_SLAB_CLASSES = 9;
constexpr int MAGAZINE_CAPACITY = 32;
constexpr uint64_t SLAB_MAX_SIZE = 0x800;
""" + allocator + """
PerCpuAllocator* per_cpu_allocators = nullptr;
std::atomic<bool> per_cpu_ready{false};
auto get_current_cpu_id() -> uint64_t { return 0; } // Explicit single-CPU Host endpoint.
#ifdef WOS_KMALLOC_DEBUG_INFO
struct AllocDebugInfo {};
auto slab_debug_slot(void* ptr) -> AllocDebugInfo** {
    return reinterpret_cast<AllocDebugInfo**>(static_cast<uint8_t*>(ptr) - sizeof(uintptr_t));
}
auto register_alloc_debug(uintptr_t, const char*) -> AllocDebugInfo* { ++host_slab::debug_registrations; return nullptr; }
void release_alloc_debug(AllocDebugInfo*) { ++host_slab::debug_releases; }
#endif
"""
    return mini + "\n" + prolog + enum + index + reverse_index + bodies + wrappers + "\n}\n" + "\n".join(global_wrappers), actual


def run() -> None:
    started = time.monotonic()
    commands = []
    evidence = Path(os.environ["WOS_KMALLOC_EVIDENCE"]) if "WOS_KMALLOC_EVIDENCE" in os.environ else None
    if evidence:
        evidence.mkdir(parents=True, exist_ok=False)
    safe_to_remove = True
    owned_pid = None
    def command(argv: list[str], directory: Path, label: str) -> subprocess.CompletedProcess:
        nonlocal safe_to_remove, owned_pid
        check_interrupt()
        remaining = OVERALL_SECONDS - (time.monotonic() - started)
        if remaining <= 0:
            raise RuntimeError("overall allocator Host deadline")
        if sum(p.stat().st_blocks * 512 for p in directory.rglob("*") if p.is_file()) > STORAGE_BYTES:
            raise RuntimeError("allocator Host storage budget")
        before = time.monotonic()
        deadline = min(started + OVERALL_SECONDS, before + COMMAND_SECONDS)
        work_deadline = deadline - 1  # Cleanup stays inside the original command/overall bounds.
        if work_deadline <= before:
            raise RuntimeError("allocator Host cleanup deadline reserve")
        process = None
        group_owned = False
        safe_to_remove = False
        try:
            # Handlers only record pending signals, including across Popen return/adoption.
            process = subprocess.Popen(argv, cwd=directory, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       text=True, start_new_session=True)
            owned_pid = process.pid
            if owned_pid <= 0 or os.getpgid(owned_pid) != owned_pid or os.getsid(owned_pid) != owned_pid:
                raise RuntimeError("allocator Host child did not own its requested new session")
            group_owned = True
            while True:
                check_interrupt()
                remaining = work_deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(argv, COMMAND_SECONDS)
                try:
                    output, _ = process.communicate(timeout=min(0.1, remaining))
                    break
                except subprocess.TimeoutExpired:
                    continue
            check_interrupt()
            await_natural_group_absence(owned_pid, work_deadline)
        finally:
            if group_owned:
                safe_to_remove = settle_group(process, deadline)
            elif process is not None:
                # A returned child is ours, but never signal an unverified group.
                try:
                    process.kill()
                    process.communicate(timeout=max(0, deadline - time.monotonic()))
                except BaseException:
                    pass
            if not safe_to_remove:
                raise RuntimeError(f"allocator Host ownership unresolved; preserve {directory}; pid={owned_pid}")
        commands.append({"label": label, "argv": argv, "exit": process.returncode,
                         "seconds": time.monotonic() - before, "output": output,
                         "leader_reaped": True, "owned_group_absent": True})
        if evidence:
            (evidence / f"{label}.log").write_text(output)
            (evidence / "commands01.json").write_text(json.dumps(commands, indent=2) + "\n")
        return subprocess.CompletedProcess(argv, process.returncode, output)
    kmalloc = source("platform/mm/dyn/kmalloc.opt.cpp")
    mini = source("minimalist_malloc/mini_malloc.cpp")
    slab = source("minimalist_malloc/slab_allocator.hpp")
    header = source("minimalist_malloc/mini_malloc.hpp")
    input_sources = {relative: source(relative) for relative in [
        "platform/mm/dyn/kmalloc.opt.cpp", "minimalist_malloc/mini_malloc.cpp",
        "minimalist_malloc/mini_malloc.hpp", "minimalist_malloc/slab_allocator.hpp", "minimalist_malloc/bitmap.hpp"]}
    input_sources["Host cases"] = CASES.read_text()
    input_hashes = {relative: hashlib.sha256(text.encode()).hexdigest() for relative, text in input_sources.items()}
    controls = {
        "nothrow-required": ("kmalloc", "kmalloc::malloc_may_fail(size)", "kmalloc::malloc(size)"),
        "dispatch-required": ("mini", "return slab_0x20.alloc(policy);", "return slab_0x20.alloc();"),
        "fatal-physical-provider": ("slab", "phys::page_alloc_may_fail(", "phys::page_alloc("),
        "adopt-null-refill": ("slab", "    if (new_slab == nullptr) {\n        return nullptr;\n    }\n", ""),
        "leak-unused-spare": ("slab", "        free_memory_to_os(new_slab, sizeof(Slab));", "        (void)new_slab;"),
        "required-free-history": ("slab", "AllocationFailurePolicy::MAY_FAIL);", "AllocationFailurePolicy::REQUIRED);"),
        "record-missing-array": ("slab", "if (header.last_free_caller && header.free_count)", "if (header.last_free_caller)"),
        "leak-history-array": ("slab", "            free_memory_to_os(retired_last_free_caller, sizeof(uintptr_t) * MAX_BLOCKS);", "            (void)retired_last_free_caller;"),
    }
    directory = Path(tempfile.mkdtemp(prefix="kmalloc-nothrow-"))
    identity = directory.stat()
    try:
        include = directory / "include"
        for relative in ["platform/mm/phys.hpp", "platform/mm/kernel_image.hpp", "platform/sys/spinlock.hpp", "platform/dbg/dbg.hpp"]:
            path = include / relative; path.parent.mkdir(parents=True, exist_ok=True); path.write_text("#pragma once\n")
        for label in ["normal-assert", "normal-ndebug", "debug-assert", "debug-ndebug", *controls]:
            current = {"kmalloc": kmalloc, "mini": mini, "slab": slab}
            if label in controls:
                target, old, new = controls[label]
                if old not in current[target]:
                    raise RuntimeError(f"actual control anchor changed: {label}")
                current[target] = current[target].replace(old, new)
            mini_dir = include / "minimalist_malloc"; mini_dir.mkdir(exist_ok=True)
            (mini_dir / "mini_malloc.hpp").write_text(header)
            (mini_dir / "slab_allocator.hpp").write_text(current["slab"])
            (mini_dir / "bitmap.hpp").write_text(source("minimalist_malloc/bitmap.hpp"))
            bodies, extraction = generated(current["kmalloc"], current["mini"])
            text = CASES.read_text().replace("// GENERATED_ACTUAL_BODIES", bodies)
            translation = directory / "actual.cpp"; translation.write_text(text)
            executable = directory / "actual"
            argv = [os.environ.get("CXX", "clang++"), "-std=c++23", "-O0", "-g", "-fno-omit-frame-pointer",
                    "-fno-optimize-sibling-calls", "-fsanitize=address,undefined", "-I", str(include), "-I", str(mini_dir),
                    "-I", str(KERNEL), "-I", str(ROOT / "modules"), str(translation), "-o", str(executable), "-DWOS_KASAN=1"]
            if label.startswith("debug") or label in controls:
                argv.append("-DWOS_KMALLOC_DEBUG_INFO=1")
            if label.endswith("ndebug"):
                argv.append("-DNDEBUG")
            if evidence:
                (evidence / f"{label}-actual.cpp").write_text(text)
                (evidence / f"{label}-slab.hpp").write_text(current["slab"])
                (evidence / f"{label}-extraction.json").write_text(json.dumps(extraction, indent=2) + "\n")
            result = command(argv, directory, f"{label}-compile")
            if result.returncode:
                raise RuntimeError(f"{label} compile failed\n{result.stdout}")
            result = command([str(executable)], directory, f"{label}-run")
            if (result.returncode == 0) != (label not in controls):
                raise RuntimeError(f"{label} did not produce expected real-body result\n{result.stdout}")
            if label not in controls:
                required = command([str(executable), "required-null"], directory, f"{label}-required")
                if (required.returncode == 0) != label.endswith("ndebug"):
                    raise RuntimeError("required local null assertion behavior changed")
                corruption = command([str(executable), "corrupt"], directory, f"{label}-corrupt")
                if corruption.returncode == 0:
                    raise RuntimeError("corruption stopped being fatal")
            if evidence and label == "debug-assert":
                (evidence / "actual.cpp").write_text(text)
                (evidence / "extraction01.json").write_text(json.dumps(extraction, indent=2) + "\n")
            executable.unlink()
    finally:
        try:
            current = directory.lstat()
            owned_directory = (not directory.is_symlink() and
                               (current.st_dev, current.st_ino, current.st_uid) ==
                               (identity.st_dev, identity.st_ino, os.getuid()))
        except OSError:
            owned_directory = False
        if safe_to_remove and owned_directory:
            shutil.rmtree(directory)
        else:
            details = {"status": "OWNERSHIP_UNRESOLVED_SCRATCH_PRESERVED", "directory": str(directory),
                       "pid": owned_pid, "directory_identity_matches": owned_directory}
            if evidence:
                (evidence / "ownership-unresolved01.json").write_text(json.dumps(details, indent=2) + "\n")
            print(json.dumps(details))
            raise RuntimeError(f"allocator Host scratch ownership unresolved; preserve {directory}")
    check_interrupt()
    after_hashes = {relative: hashlib.sha256((CASES.read_text() if relative == "Host cases" else source(relative)).encode()).hexdigest()
                    for relative in input_sources}
    if input_hashes != after_hashes:
        raise RuntimeError("actual allocator Host input content changed")
    result = {"status": "PASS_ACTUAL_SMALL_ALLOCATOR_HOST_PORTS", "input_before": input_hashes, "input_after": after_hashes, "seconds": time.monotonic() - started,
              "profiles": 4, "compiled_controls": list(controls), "commands": commands,
              "limits": "Physical/IRQ/diagnostic endpoints ported; natural default physical OOM is terminal before a null return; no native/SMP/IRQ closure."}
    if evidence:
        (evidence / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "commands"}))


if __name__ == "__main__":
    handlers = {number: signal.getsignal(number) for number in [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]}
    for number in handlers:
        signal.signal(number, defer_interrupt)
    try:
        run()
    except BaseException as error:
        if "WOS_KMALLOC_EVIDENCE" in os.environ:
            evidence = Path(os.environ["WOS_KMALLOC_EVIDENCE"])
            if evidence.is_dir():
                (evidence / "failure01.json").write_text(json.dumps({"status": "FAIL", "error": repr(error)}, indent=2) + "\n")
        raise

    finally:
        for number, handler in handlers.items():
            signal.signal(number, handler)
