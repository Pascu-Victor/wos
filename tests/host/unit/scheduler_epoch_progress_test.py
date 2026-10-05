#!/usr/bin/env python3
"""Check the actual timer IRQ epoch gate under adversarial SMP ordering."""

from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "modules/kern/src/platform/sys/context_switch.cpp"

PRELUDE = r'''
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <initializer_list>
uint64_t on_cpu = 0, epochs = 0, requests = 0;
std::atomic<uint64_t> timer_tick_count{0};
std::atomic<uint64_t> epoch_timer_tick_count{0};
namespace cpu { uint64_t current_cpu() { return on_cpu; } }
namespace sched {
struct EpochManager { static void advance_epoch() { assert(on_cpu == 0); ++epochs; } };
void request_gc() { assert(on_cpu == 0); ++requests; }
}
void irq(uint64_t id) {
    on_cpu = id;
'''

CASES = r'''
    static_cast<void>(TICKS);
}
void reset(uint64_t ticks = 0) {
    timer_tick_count.store(ticks); epoch_timer_tick_count.store(ticks);
    epochs = 0; requests = 0;
}
void check(uint64_t expected) { assert(epochs == expected && requests == expected); }
int main() {
    // One initial AP interrupt shifts the shared ticket parity forever when
    // followed by repeating eight-CPU bursts. CPU0 still needs to progress.
    irq(1);
    for (unsigned n = 0; n < 10000; ++n) {
        irq(0);
        for (unsigned c = 1; c < 8; ++c) irq(c);
    }
    check(1000);
    assert(epoch_timer_tick_count.load() == 10000);
    assert(timer_tick_count.load() == 80001);

    reset();
    for (unsigned n = 0; n < 10000; ++n) irq(1 + n % 7);
    check(0);
    assert(epoch_timer_tick_count.load() == 0);
    assert(timer_tick_count.load() == 10000);

    // Preserve the old single-CPU first-tick and modulo cadence exactly.
    reset();
    irq(0); check(1);
    for (unsigned n = 1; n < 10; ++n) irq(0);
    check(1);
    irq(0); check(2);

    // CPU count, permutation and AP burst length cannot change BSP cadence.
    for (unsigned cores : {2U, 4U, 8U, 16U}) {
        reset();
        unsigned bsp = 0;
        uint32_t random = 0x12345678;
        for (unsigned n = 0; n < 20000; ++n) {
            random = random * 1664525U + 1013904223U;
            unsigned id = (random >> 24) % cores;
            irq(id);
            if (id == 0) ++bsp;
            check((bsp + 9) / 10);
        }
        assert(epoch_timer_tick_count.load() == bsp);
        assert(timer_tick_count.load() == 20000);
    }

    // Unsigned counter wrap remains defined; AP IRQs consume no slots.
    reset(UINT64_MAX - 3);
    uint64_t expected = 0;
    for (unsigned n = 0; n < 40; ++n) {
        irq(7);
        auto before = epoch_timer_tick_count.load();
        if (before % 10 == 0) ++expected;
        irq(0);
        check(expected);
    }
    std::puts("actual timer IRQ epoch progress PASS");
}
'''


def main():
    source = SOURCE.read_text()
    start = source.index("    // Advance epoch and request garbage collection periodically on CPU 0 only.")
    end = source.index("    kasan_unpoison_irq_save_area(stack_ptr);", start)
    block = source[start:end]
    # Compile the unchanged old gate as a negative control for the same cases.
    old = """
    uint64_t const TICKS = timer_tick_count.fetch_add(1, std::memory_order_relaxed);
    if (cpu::current_cpu() == 0 && (TICKS % 10) == 0) {
        sched::EpochManager::advance_epoch();
        sched::request_gc();
    }
"""
    compiler = shutil.which("g++")
    assert compiler, "native g++ required"
    with tempfile.TemporaryDirectory(prefix="wos-epoch-progress-") as directory:
        tmp = Path(directory)
        for name, gate in (("actual", block), ("old-global-ticket", old)):
            cpp, binary = tmp / (name + ".cpp"), tmp / name
            cpp.write_text(PRELUDE + gate + CASES)
            subprocess.run([compiler, "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Werror",
                            "-fsanitize=address,undefined", "-fno-sanitize-recover=all",
                            "-fno-omit-frame-pointer", str(cpp), "-o", str(binary)], check=True)
            result = subprocess.run([str(binary)], capture_output=True, text=True)
            if name == "actual":
                assert result.returncode == 0, result.stdout + result.stderr
                print(result.stdout, end="")
            else:
                assert result.returncode != 0 and "epochs == expected" in result.stderr, result.stderr
                print("old shared-ticket gate rejected by identical progress assertion")


if __name__ == "__main__":
    main()
