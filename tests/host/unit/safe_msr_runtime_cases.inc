#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <platform/asm/cpu.hpp>
#include <platform/asm/cpu_safe_msr.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/interrupt/idt.hpp>

namespace fixture {
unsigned assertions{}, failures{}, cases{}, cpuid_reads{}, sidt_reads{}, dispatches{};
uint32_t features{};
ker::mod::desc::idt::IdtPtr loaded{};
void check(bool value, const char* expression, unsigned line) {
    ++assertions;
    if (!value) {
        ++failures;
        std::fprintf(stderr, "FAIL %u: %s\n", line, expression);
    }
}
}  // namespace fixture
#define CHECK(expression) fixture::check(static_cast<bool>(expression), #expression, __LINE__)

namespace ker::mod::cpu {
void cpuid(CpuidContext* context) {
    CHECK(context->function == 1);
    ++fixture::cpuid_reads;
    context->edx = static_cast<int>(fixture::features);
}
}  // namespace ker::mod::cpu
namespace ker::mod::desc::idt {
void host_sidt(IdtPtr& output) {
    ++fixture::sidt_reads;
    output = fixture::loaded;
}
}  // namespace ker::mod::desc::idt

extern "C" {
extern const unsigned char wos_optional_msr_fault;
extern const unsigned char wos_optional_msr_recovery;
auto __real_wos_try_recover_msr_frame(ker::mod::gates::InterruptFrame*) -> bool;
auto __wrap_wos_try_recover_msr_frame(ker::mod::gates::InterruptFrame* frame) -> bool {
    ++fixture::dispatches;
    return __real_wos_try_recover_msr_frame(frame);
}
auto host_safe_msr_hook(void* saved_registers) -> bool;
}

namespace fixture {
using Frame = ker::mod::gates::InterruptFrame;
struct Saved {
    ker::mod::cpu::GPRegs registers;
    Frame frame;
};
static_assert(offsetof(Saved, frame) == 120);
auto fault() -> uint64_t { return reinterpret_cast<uint64_t>(&wos_optional_msr_fault); }
auto recovery() -> uint64_t { return reinterpret_cast<uint64_t>(&wos_optional_msr_recovery); }
auto original_frame() -> Frame { return Frame{13, 0, fault(), 8, 0x202, 0x12345678, 0x10}; }

void prerequisites() {
    ++cases;
    using namespace ker::mod::desc::idt;
    uint64_t output = 0xcafebabefeedfaceULL;
    features = 0;
    CHECK(!ker::mod::cpu::try_read_msr(UINT32_MAX, output));
    CHECK(output == 0xcafebabefeedfaceULL && sidt_reads == 0 && cpuid_reads == 1);
    features = uint32_t{1} << 5;
    idt_ready_state = 0;
    CHECK(!ker::mod::cpu::try_read_msr(UINT32_MAX, output));
    CHECK(output == 0xcafebabefeedfaceULL && sidt_reads == 0);
    idt_ready_state = IDT_READY;
    idt_ptr = IdtPtr{4095, 0x12345000};
    loaded = IdtPtr{4095, 0x23456000};
    CHECK(!ker::mod::cpu::try_read_msr(UINT32_MAX, output));
    CHECK(output == 0xcafebabefeedfaceULL && sidt_reads == 1);
    loaded = IdtPtr{4094, idt_ptr.base};
    CHECK(!ker::mod::cpu::try_read_msr(UINT32_MAX, output));
    CHECK(output == 0xcafebabefeedfaceULL && sidt_reads == 2);
    loaded = idt_ptr;
    CHECK(kernel_idt_loaded_on_current_cpu());
    CHECK(sidt_reads == 3);
    // Do not call the successful read branch: the linked leaf is real RDMSR,
    // and this host fixture deliberately executes no host MSR instruction.
}

void exact_match() {
    ++cases;
    for (uint64_t vector = 0; vector < 256; ++vector) {
        if (vector == 13) continue;
        uint64_t rip = fault();
        CHECK(!ker::mod::cpu::try_recover_msr_read(vector, 0, 8, rip));
        CHECK(rip == fault());
    }
    for (uint64_t error : {uint64_t{1}, uint64_t{8}, UINT64_MAX}) {
        uint64_t rip = fault();
        CHECK(!ker::mod::cpu::try_recover_msr_read(13, error, 8, rip));
        CHECK(rip == fault());
    }
    for (uint64_t cs : {uint64_t{1}, uint64_t{2}, uint64_t{3}, uint64_t{0x1b}, UINT64_MAX}) {
        uint64_t rip = fault();
        CHECK(!ker::mod::cpu::try_recover_msr_read(13, 0, cs, rip));
        CHECK(rip == fault());
    }
    for (uint64_t address : {uint64_t{0}, fault() - 1, fault() + 1, fault() + 2, recovery(), UINT64_MAX}) {
        uint64_t rip = address;
        CHECK(!ker::mod::cpu::try_recover_msr_read(13, 0, 8, rip));
        CHECK(rip == address);
    }
    for (uint64_t cs : {uint64_t{0}, uint64_t{8}, uint64_t{0x10}}) {
        uint64_t rip = fault();
        CHECK(ker::mod::cpu::try_recover_msr_read(13, 0, cs, rip));
        CHECK(rip == recovery());
    }
}

void real_frame_dispatch() {
    ++cases;
    CHECK(!wos_try_recover_msr_frame(nullptr));
    Frame frame = original_frame();
    Frame expected = frame;
    expected.rip = recovery();
    CHECK(wos_try_recover_msr_frame(&frame));
    CHECK(std::memcmp(&frame, &expected, sizeof(frame)) == 0);
    Frame const ORIGINAL = original_frame();
    for (unsigned mismatch = 0; mismatch < 4; ++mismatch) {
        frame = ORIGINAL;
        if (mismatch == 0) frame.int_num = 14;
        if (mismatch == 1) frame.err_code = 1;
        if (mismatch == 2) frame.cs = 3;
        if (mismatch == 3) frame.rip += 2;
        expected = frame;
        CHECK(!wos_try_recover_msr_frame(&frame));
        CHECK(std::memcmp(&frame, &expected, sizeof(frame)) == 0);
    }
}

void actual_nasm_hook() {
    ++cases;
    // Leave ample host storage below the saved-register area for the actual
    // C++ dispatcher's stack. The actual low-level TU is sanitizer-excluded,
    // matching kernel policy; the surrounding assertions remain ASan/UBSan.
    auto storage = std::make_unique<std::array<std::byte, 1 << 20>>();
    for (unsigned alignment : {0U, 8U}) {
        auto* saved = reinterpret_cast<Saved*>(storage->data() + (1 << 19) + alignment);
        for (unsigned mismatch = 0; mismatch < 6; ++mismatch) {
            std::memset(saved, 0xa5, sizeof(*saved));
            saved->frame = original_frame();
            if (mismatch == 1) saved->frame.int_num = 14;
            if (mismatch == 2) saved->frame.err_code = 1;
            if (mismatch == 3) saved->frame.cs = 3;
            if (mismatch == 4) saved->frame.rip += 1;
            if (mismatch == 5) saved->frame.rip += 9;  // later output-store vicinity
            Saved expected = *saved;
            unsigned const BEFORE = dispatches;
            bool const MATCHED = host_safe_msr_hook(saved);
            CHECK(MATCHED == (mismatch == 0));
            CHECK(dispatches == BEFORE + (mismatch == 1 ? 0 : 1));
            if (MATCHED) expected.frame.rip = recovery();
            CHECK(std::memcmp(saved, &expected, sizeof(*saved)) == 0);
        }
    }
}
}  // namespace fixture

int main() {
    fixture::prerequisites();
    fixture::exact_match();
    fixture::real_frame_dispatch();
    fixture::actual_nasm_hook();
    std::printf("%s safe_msr cases=%u assertions=%u failures=%u host_msr_executed=0\n", fixture::failures == 0 ? "PASS" : "FAIL",
                fixture::cases, fixture::assertions, fixture::failures);
    return fixture::failures == 0 ? 0 : 1;
}
