#!/usr/bin/env python3
"""Check secure worker boot ordering in actual sources; no boot execution."""

import argparse
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
KERNEL = ROOT / "modules/kern/src"
WKI = KERNEL / "net/wki/wki.cpp"
WORKER = "wki_secure_rdma_worker_start"
CONSUMERS = {
    "wki_routing_init",
    "wki_zone_init",
    "wki_remotable_init",
    "wki_dev_server_init",
    "wki_dev_proxy_init",
    "wki_event_init",
    "wki_irq_fwd_init",
    "wki_remote_vfs_init",
    "wki_remote_net_init",
    "wki_remote_compute_init",
    "wki_ipc_subsystem_init",
    "wki_chaos_workload_init",
}


def masked(source: str) -> str:
    """Blank comments/literals without changing offsets or brace structure."""
    pattern = re.compile(
        r'//[^\n]*|/\*[\s\S]*?\*/|R"([^ ()\\\t\r\n]{0,16})\([\s\S]*?\)\1"'
        r'|"(?:\\[\s\S]|[^"\\])*"|\'(?:\\[\s\S]|[^\'\\])*\''
    )
    return pattern.sub(lambda match: re.sub(r"[^\n]", " ", match.group()), source)


def block_end(source: str, opening: int) -> int:
    depth = 0
    for pos in range(opening, len(source)):
        depth += (source[pos] == "{") - (source[pos] == "}")
        if depth == 0:
            return pos
    raise AssertionError("unterminated source block")


def function_body(source: str, name: str) -> str:
    code = masked(source)
    match = re.search(rf"\bvoid\s+{name}\s*\([^)]*\)\s*\{{", code)
    assert match is not None, f"missing actual function {name}"
    opening = match.end() - 1
    return code[opening + 1 : block_end(code, opening)]


def calls(body: str, name: str) -> list[int]:
    return [match.start() for match in re.finditer(rf"\b{name}\s*\(", body)]


def depth_at(body: str, pos: int) -> int:
    prefix = body[:pos]
    return prefix.count("{") - prefix.count("}")


def require_before(body: str, before: str, after_pos: int) -> None:
    positions = calls(body, before)
    assert len(positions) == 1, f"expected exactly one {before} initialization"
    assert positions[0] < after_pos, f"{before} must precede secure worker publication"


def check_init_order(source: str) -> None:
    init = function_body(source, "wki_init")
    starts = calls(init, WORKER)
    assert len(starts) == 1, "successful wki_init must start secure progress exactly once"
    start = starts[0]
    assert depth_at(init, start) == 0, "worker start must be outside nested lexical/guard scopes"
    # A top-level RAII guard would remain held despite brace depth zero.
    guards = re.finditer(r"\b(?:[\w:]*Guard|[\w:]*(?:lock_guard|unique_lock|scoped_lock))\s+(?:const\s+)?\w+", init[:start])
    assert all(depth_at(init, guard.start()) != 0 for guard in guards), "worker start under function-scope RAII guard"
    require_before(init, "wki_secure_rdma_init", start)
    for consumer in sorted(CONSUMERS):
        require_before(init, consumer, start)
    # Discover later-added WKI init calls too, rather than trusting the list alone.
    actual_inits = re.finditer(r"\b(wki_\w*init)\s*\(", init)
    assert all(match.start() < start for match in actual_inits), "worker precedes an actual WKI initializer"
    for name in ("devfs_nodes_init", "devfs_nodes_add_peer"):
        require_before(init, name, start)
    deferred = function_body(source, "wki_deferred_work_thread_start")
    assert not calls(deferred, WORKER), "early deferred startup must not consume secure startup"
    assert calls(deferred, "wki_zone_worker_start"), "ordinary ZONE startup must remain intact"


def check_single_caller(source: str) -> None:
    sites = []
    for path in sorted(KERNEL.rglob("*.cpp")):
        code = masked(source if path == WKI else path.read_text())
        definition = re.search(rf"\bvoid\s+({WORKER})\s*\([^)]*\)\s*\{{", code)
        for match in re.finditer(rf"\b{WORKER}\s*\(", code):
            if definition is None or match.start() != definition.start(1):
                sites.append(path.relative_to(KERNEL).as_posix())
    assert sites == ["net/wki/wki.cpp"], f"expected one production direct caller, found {sites}"


def check_boot_map() -> None:
    registry = (KERNEL / "platform/init/init_registry.hpp").read_text()
    phase4 = re.search(r'\.name\s*=\s*"sched"\s*,\s*\.phase\s*=\s*BootPhase::PHASE_4_SCHEDULER_SETUP', registry)
    phase5 = re.search(r'\.name\s*=\s*"wki"\s*,\s*\.phase\s*=\s*BootPhase::PHASE_5_DRIVERS', registry)
    assert phase4 is not None and phase5 is not None and phase4.start() < phase5.start(), "scheduler setup must precede WKI init"
    executor = masked((KERNEL / "platform/init/init_executor.hpp").read_text())
    assert re.search(r"for\s*\(int\s+p\s*=\s*0;[^;]+;\s*\+\+p\)", executor), "boot phases must advance in order"
    assert "run_phase(static_cast<BootPhase>(p))" in executor
    setup = function_body((KERNEL / "platform/init/init_wrappers.cpp").read_text(), "sched_init")
    start_smt = calls(setup, "start_smt")
    assert len(start_smt) == 1 and calls(setup, "setup_queues")[0] < start_smt[0]
    smt = function_body((KERNEL / "platform/smt/smt.cpp").read_text(), "start_smt")
    timer = calls(smt, "wki_timer_thread_start")
    assert len(timer) == 1 and calls(smt, "percpu_init")[0] < timer[0]
    timer_body = function_body((KERNEL / "net/wki/peer.cpp").read_text(), "wki_timer_thread_start")
    assert len(calls(timer_body, "wki_deferred_work_thread_start")) == 1
    worker = function_body((KERNEL / "net/wki/secure_rdma.cpp").read_text(), WORKER)
    initialized = worker.find("s_initialized.load(std::memory_order_acquire)")
    creation = calls(worker, "create_kernel_thread")
    assert len(creation) == 1 and 0 <= initialized < creation[0], "early uninitialized worker start must remain inert"
    assert "return;" in worker[initialized : creation[0]]


def check_scanner_and_negative_controls(source: str) -> None:
    sample = 'void probe() { const char* s = "}"; /* { */ if (true) { } // }\n }'
    assert depth_at(function_body(sample, "probe"), len(function_body(sample, "probe"))) == 0
    marker = f"{WORKER}();"
    assert source.count(marker) == 1
    without = source.replace(marker, "", 1)
    mutations = {
        "early deferred startup": without.replace("void wki_deferred_work_thread_start() {", f"void wki_deferred_work_thread_start() {{\n{marker}", 1),
        "before consumers": without.replace("    // Init routing subsystem", f"    {marker}\n    // Init routing subsystem", 1),
        "nested guard scope": source.replace(marker, f"{{ EngineGuard const guard(lock); {marker} }}", 1),
        "function-scope guard": source.replace(marker, f"EngineGuard const guard(lock); {marker}", 1),
    }
    for name, mutation in mutations.items():
        assert mutation != source, f"ineffective negative {name}"
        try:
            check_init_order(mutation)
        except AssertionError:
            continue
        raise AssertionError(f"startup regression accepted {name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wki-source", type=Path, default=WKI, help="selected actual source for historical ordering controls")
    args = parser.parse_args()
    source = args.wki_source.read_text()
    check_init_order(source)
    check_single_caller(source)
    check_boot_map()
    check_scanner_and_negative_controls(source)
    print("PASS: actual secure startup ordering, sole caller, boot map, lexical scanner and four semantic source controls (no boot execution)")


if __name__ == "__main__":
    main()
