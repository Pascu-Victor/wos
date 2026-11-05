#include <array>
#include <atomic>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/cache_policy.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <string_view>
#include <test/cache_policy_runtime_ktest.hpp>
#include <test/ktest.hpp>

namespace ker::test {
namespace {
namespace cache = ker::mod::mm::cache;
namespace sched = ker::mod::sched;
namespace task = sched::task;
namespace virt = ker::mod::mm::virt;
using log = ker::mod::dbg::logger<"cache-test">;
std::atomic<bool> started{false};

void require(bool condition, const char* reason) {
    if (condition) return;
    log::error("CACHE_POLICY_RUNTIME FAIL %s", reason);
    ker::mod::dbg::panic_handler("scheduled cache policy selftest failed");
}

auto wait_for_reports() -> cache::PatQualification {
    uint64_t const CPUS = ker::mod::smt::get_core_count();
    require(CPUS != 0 && CPUS <= 64, "bounded actual CPU topology");
    uint64_t const EXPECTED = CPUS == 64 ? UINT64_MAX : (uint64_t{1} << CPUS) - 1;
    auto const DEADLINE = sched::saturating_deadline_us(ker::mod::time::get_us(), 10'000'000);
    for (;;) {
        auto const value = cache::qualification();
        require(value.configured && value.expected_mask == EXPECTED, "configured reports match actual CPU topology");
        require((value.reported_mask & ~EXPECTED) == 0, "no unexpected CPU report");
        if (value.reported_mask == EXPECTED) {
            require(value.complete, "all reports collected without duplicate or invalid-input poison");
            return value;
        }
        require(ker::mod::time::get_us() < DEADLINE, "bounded wait for actual AP reports");
        sched::kern_sleep_us(1000);
    }
}

void wait_for_init_mounts() {
    auto const DEADLINE = sched::saturating_deadline_us(ker::mod::time::get_us(), 30'000'000);
    for (;;) {
        auto* init = sched::find_task_by_pid_safe(1);
        if (init != nullptr) {
            task::Task::FilesystemContext context;
            bool const READY = init->capture_filesystem_context(context) && context.root_len == 7 &&
                               std::string_view(context.root.data(), context.root_len) == "/rootfs";
            init->release();
            if (READY) {
                log::info("CACHE_POLICY_RUNTIME INIT_READY root=/rootfs");
                return;
            }
        }
        require(ker::mod::time::get_us() < DEADLINE, "PID 1 finishes actual root mount/pivot before shutdown");
        sched::kern_sleep_us(10000);
    }
}

void observations() {
    auto const PAT = wait_for_reports();
    std::array<cache::CpuPat, 64> captured{};
    bool wb = true, uc = true, wc = true;
    uint64_t mismatch = 0;
    size_t cpus = 0;
    for (size_t i = 0; i < captured.size(); ++i) {
        if ((PAT.expected_mask & (uint64_t{1} << i)) == 0) continue;
        auto& value = captured.at(i);
        require(cache::cpu_pat(i, value), "each published CPU has a readable immutable observation");
        bool const WB = !value.supported || (value.raw & 255U) == 6;
        bool const UC = !value.supported || ((value.raw >> 24U) & 255U) == 0;
        bool const WC = value.supported && ((value.raw >> 40U) & 255U) == 1;
        if (value.supported && (!WB || !UC || !WC)) mismatch |= uint64_t{1} << i;
        wb &= WB;
        uc &= UC;
        wc &= WC;
        ++cpus;
        log::info("CACHE_POLICY_RUNTIME CPU cpu=%lu supported=%u pat=%016lx wb0=%u uc3=%u wc5=%u", i, value.supported ? 1U : 0U, value.raw,
                  WB ? 1U : 0U, UC ? 1U : 0U, WC ? 1U : 0U);
    }
    wc &= wb && uc;
    require(PAT.mismatch_mask == mismatch && PAT.wb_encoding == wb && PAT.uc_encoding == uc && PAT.wc_encoding == wc,
            "aggregate encodings match independent per-CPU observations");
    log::info("CACHE_POLICY_RUNTIME PAT expected=%016lx reported=%016lx mismatch=%016lx complete=%u wb=%u uc=%u wc=%u", PAT.expected_mask,
              PAT.reported_mask, PAT.mismatch_mask, PAT.complete ? 1U : 0U, wb ? 1U : 0U, uc ? 1U : 0U, wc ? 1U : 0U);
    log::info("CACHE_POLICY_RUNTIME CASE_PASS actual_bsp_ap_pat");

    auto const FB = virt::boot_framebuffer_cache_audit();
    require(FB.complete && FB.inherited_hash == FB.kernel_hash, "boot source/target framebuffer cache identities match");
    require((FB.pages == 0) == (FB.selector_mask == 0), "framebuffer page coverage and selector presence agree");
    log::info("CACHE_POLICY_RUNTIME FRAMEBUFFER pages=%lu selectors=0x%x source=%016lx target=%016lx complete=%u coverage=%s", FB.pages,
              static_cast<unsigned>(FB.selector_mask), FB.inherited_hash, FB.kernel_hash, FB.complete ? 1U : 0U,
              FB.pages != 0 ? "present" : "absent");
    log::info("CACHE_POLICY_RUNTIME CASE_PASS inherited_framebuffer_selectors");

    sched::kern_sleep_us(1000);
    auto const AFTER_PAT = cache::qualification();
    auto const AFTER_FB = virt::boot_framebuffer_cache_audit();
    require(AFTER_PAT.expected_mask == PAT.expected_mask && AFTER_PAT.reported_mask == PAT.reported_mask &&
                AFTER_PAT.mismatch_mask == PAT.mismatch_mask && AFTER_PAT.configured == PAT.configured &&
                AFTER_PAT.complete == PAT.complete && AFTER_PAT.wb_encoding == PAT.wb_encoding &&
                AFTER_PAT.uc_encoding == PAT.uc_encoding && AFTER_PAT.wc_encoding == PAT.wc_encoding,
            "CPU qualification remains immutable across ordinary scheduling");
    require(AFTER_FB.pages == FB.pages && AFTER_FB.selector_mask == FB.selector_mask && AFTER_FB.inherited_hash == FB.inherited_hash &&
                AFTER_FB.kernel_hash == FB.kernel_hash && AFTER_FB.complete == FB.complete,
            "framebuffer audit remains immutable across ordinary scheduling");
    for (size_t i = 0; i < captured.size(); ++i) {
        if ((PAT.expected_mask & (uint64_t{1} << i)) == 0) continue;
        cache::CpuPat value{};
        require(cache::cpu_pat(i, value) && value.supported == captured.at(i).supported && value.raw == captured.at(i).raw,
                "per-CPU raw observations remain unchanged");
    }
    log::info("CACHE_POLICY_RUNTIME CASE_PASS immutable_boot_observations");
    log::info("CACHE_POLICY_RUNTIME PASS cases=3 cpus=%lu framebuffer_pages=%lu wb=%u uc=%u wc=%u encoding_observation_only=1", cpus,
              FB.pages, wb ? 1U : 0U, uc ? 1U : 0U, wc ? 1U : 0U);
}

void runtime_entry() {
    {
        auto const HOLD = sched::hold_kernel_shutdown();
        require(ker::mod::mm::phys::can_wait_for_reclaim(), "ordinary scheduled test context");
        log::info("CACHE_POLICY_RUNTIME BEGIN");
        observations();
        wait_for_init_mounts();
    }
    {
        auto const HOLD = sched::hold_kernel_shutdown();
        auto* init = sched::find_task_by_pid_safe(1);
        require(init != nullptr, "retain PID 1 for normal userspace shutdown request");
        constexpr int WOS_SIGUSR2 = 12;
        init->signal_generate_pending_mask(uint64_t{1} << (WOS_SIGUSR2 - 1));
        sched::wake_task_for_signal(init);
        init->release();
        log::info("CACHE_POLICY_RUNTIME POWEROFF_REQUESTED via=init-sigusr2");
    }
    sched::exit_current_kernel_thread();
}
}  // namespace

KTEST(CachePolicy, ReadOnlyBootReportsPublishedBeforeEarlyTests) {
    auto const PAT = cache::qualification();
    KEXPECT_TRUE(PAT.configured && PAT.complete);
    KEXPECT_NE(PAT.expected_mask, 0U);
    KEXPECT_EQ(PAT.reported_mask, PAT.expected_mask);
    KEXPECT_TRUE(!PAT.wc_encoding || (PAT.wb_encoding && PAT.uc_encoding));
    for (size_t i = 0; i < 64; ++i) {
        if ((PAT.expected_mask & (uint64_t{1} << i)) == 0) continue;
        cache::CpuPat value{};
        KEXPECT_TRUE(cache::cpu_pat(i, value));
        if (!value.supported) KEXPECT_FALSE(PAT.wc_encoding);
    }
    auto const FB = virt::boot_framebuffer_cache_audit();
    KEXPECT_TRUE(FB.complete);
    KEXPECT_EQ(FB.inherited_hash, FB.kernel_hash);
    KEXPECT_EQ(FB.pages == 0, FB.selector_mask == 0);
}

auto start_cache_policy_runtime_selftest() -> bool {
    bool expected = false;
    if (!started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return false;
    auto* worker = task::Task::create_kernel_thread("cache_policy_runtime_selftest", runtime_entry);
    if (worker == nullptr || !sched::post_task_balanced(worker)) {
        if (worker != nullptr) task::destroy_unpublished_user_thread(worker);
        started.store(false, std::memory_order_release);
        return false;
    }
    return true;
}
}  // namespace ker::test
