#include "top/types.hpp"

namespace top {

auto cpu_total(const CpuTimes& cpu) -> uint64_t {
    return cpu.user + cpu.nice + cpu.system + cpu.idle + cpu.iowait + cpu.irq + cpu.softirq + cpu.steal;
}

auto diff_counter(uint64_t now, uint64_t old) -> uint64_t { return now >= old ? now - old : 0; }

auto diff_cpu(const CpuTimes& now, const CpuTimes& old) -> CpuTimes {
    return CpuTimes{
        .user = diff_counter(now.user, old.user),
        .nice = diff_counter(now.nice, old.nice),
        .system = diff_counter(now.system, old.system),
        .idle = diff_counter(now.idle, old.idle),
        .iowait = diff_counter(now.iowait, old.iowait),
        .irq = diff_counter(now.irq, old.irq),
        .softirq = diff_counter(now.softirq, old.softirq),
        .steal = diff_counter(now.steal, old.steal),
    };
}

auto cpu_percent_from_delta(const CpuTimes& delta) -> CpuPercent {
    uint64_t const TOTAL = cpu_total(delta);
    if (TOTAL == 0) {
        return {};
    }
    auto pct = [TOTAL](uint64_t value) -> double { return (static_cast<double>(value) * 100.0) / static_cast<double>(TOTAL); };
    return CpuPercent{
        .user = pct(delta.user),
        .nice = pct(delta.nice),
        .system = pct(delta.system),
        .idle = pct(delta.idle),
        .iowait = pct(delta.iowait),
        .irq = pct(delta.irq),
        .softirq = pct(delta.softirq),
        .steal = pct(delta.steal),
    };
}

}  // namespace top
