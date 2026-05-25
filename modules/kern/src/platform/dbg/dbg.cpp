#include "dbg.hpp"

#include <array>
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <platform/asm/cpu.hpp>
#include <platform/smt/smt.hpp>

#include "mod/gfx/fb.hpp"
#include "mod/io/serial/serial.hpp"
#include "platform/dbg/journal.hpp"
#include "platform/ktime/ktime.hpp"
#include "platform/sys/spinlock.hpp"
#include "util/hcf.hpp"

namespace ker::mod::dbg {
namespace {
constexpr int MAX_FRAMES_BACKTRACE = 32;
constexpr int MAX_RAW_STACK_TRACE = 64;
constexpr size_t TIME_SEC_BUF_SIZE = 10;  // good enough for 30 years of uptime
constexpr size_t TIME_MS_BUF_SIZE = 5;
constexpr size_t U64_CONVERSION_BUF_SIZE = 32;
sys::Spinlock log_lock{};
bool is_init = false;
bool is_time_available = false;
bool is_kmalloc_available = false;
uint64_t lines_logged = 0;
}  // namespace

using namespace ker::mod;

void init() {
    if (is_init) {
        return;
    }
    io::serial::init();
    journal::init();
    is_init = true;
}

void enable_time() {
    if (is_time_available) {
        // Panic! should only be called once when ktime is initialized
        panic_handler("Kernel time was already initialized");
    }
    is_time_available = true;
    journal::enable_time();
    log("Kernel time is now available");
}

void break_into_debugger() { __asm__ volatile("int $3"); }

void enable_kmalloc() {
    if (is_kmalloc_available) {
        // Panic! should only be called once when kmalloc is initialized
        panic_handler("Kernel kmalloc already initialized");
    }
    is_kmalloc_available = true;
    log("Kernel memory allocator is now available");
}

namespace {

void fb_log(const char* str) {
    if constexpr (gfx::fb::WOS_HAS_GFX_FB) {
        uint64_t line = lines_logged;
        if (lines_logged >= gfx::fb::viewport_height_chars()) {
            gfx::fb::scroll();
            line = gfx::fb::viewport_height_chars() - 1;
        }

        gfx::fb::draw_char(0, line, '[');
        // todo maybe print cpu id
        int stamp_len = 1;
        if (is_time_available) [[likely]] {
            std::array<char, TIME_SEC_BUF_SIZE> time_sec{};
            std::array<char, TIME_MS_BUF_SIZE> time_ms{};
            uint64_t const LOG_TIME = time::get_ms();
            uint64_t const LOG_TIME_MS_PART = LOG_TIME % 1000;
            uint64_t const LOG_TIME_SEC_PART = LOG_TIME / 1000;
            auto u64toa_local2 = []<size_t N>(uint64_t n, std::array<char, N>& s, uint64_t base) -> int {
                if (n == 0) {
                    s.at(0) = '0';
                    s.at(1) = '\0';
                    return 1;
                }
                std::array<char, U64_CONVERSION_BUF_SIZE> buf{};
                size_t i = 0;
                while (n > 0) {
                    uint64_t const DIGIT = n % base;
                    buf.at(i++) = static_cast<char>((DIGIT < 10) ? ('0' + DIGIT) : ('a' + DIGIT - 10));
                    n /= base;
                }
                int j = 0;
                while (i > 0) {
                    s.at(static_cast<size_t>(j++)) = buf.at(--i);
                }
                s.at(static_cast<size_t>(j)) = '\0';
                return j;
            };
            int const MS_LEN = u64toa_local2(LOG_TIME_MS_PART, time_ms, 10);
            int const SEC_LEN = u64toa_local2(LOG_TIME_SEC_PART, time_sec, 10);
            gfx::fb::draw_string(stamp_len, line, time_sec.data());
            stamp_len += SEC_LEN;
            gfx::fb::draw_char(stamp_len, line, '.');
            stamp_len++;
            gfx::fb::draw_string(stamp_len, line, time_ms.data());
            stamp_len += MS_LEN;
        }
        gfx::fb::draw_char(stamp_len, line, ']');
        stamp_len++;
        gfx::fb::draw_char(stamp_len, line, ':');
        stamp_len++;
        lines_logged += gfx::fb::draw_string(stamp_len, line, str);
    } else {
        mod::io::serial::write("Tried to write to framebuffer, module not enabled\n");
    }
}

}  // namespace

void log_string(const char* str) {
    journal::emit(LogLevel::INFO, "kernel", str, journal::JOURNAL_FLAG_KERNEL);
    // logLock only protects the framebuffer state and linesLogged counter.
    log_lock.lock();
    if constexpr (gfx::fb::WOS_HAS_GFX_FB) {
        fb_log(str);
    }
    lines_logged++;
    log_lock.unlock();
}

namespace {

void log_va(const char* format, va_list& args) {
    // va_list is intentionally the C ABI formatting carrier for kernel logging.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    journal::emit_v(LogLevel::INFO, "kernel", format, args, journal::JOURNAL_FLAG_KERNEL);
}

}  // namespace

// NOLINTNEXTLINE(modernize-avoid-variadic-functions)
void log_var(const char* format, ...) {
    va_list args;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    va_start(args, format);
    log_va(format, args);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    va_end(args);
}

void log_fb_only(const char* str) {
    log_lock.lock();
    fb_log(str);
    log_lock.unlock();
}

void log_fb_advance() {
    log_lock.lock();
    lines_logged++;
    log_lock.unlock();
}

void error(const char* str) {
    // TODO: pretty print error
    journal::emit(LogLevel::ERROR, "kernel", str, journal::JOURNAL_FLAG_KERNEL);
}

namespace {

void emit_kernel_log_va(const char* module, LogLevel level, const char* format, va_list& args) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    journal::emit_v(level, module, format, args, journal::JOURNAL_FLAG_KERNEL);
}

}  // namespace

// NOLINTNEXTLINE(modernize-avoid-variadic-functions)
void emit_log(const char* module, LogLevel level, const char* format, ...) {
    va_list args;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    va_start(args, format);
    emit_kernel_log_va(module, level, format, args);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    va_end(args);
}

// NOLINTNEXTLINE(modernize-avoid-variadic-functions)
void emit_kernel_log(const char* module, LogLevel level, const char* format, ...) {
    va_list args;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    va_start(args, format);
    emit_kernel_log_va(module, level, format, args);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    va_end(args);
}

void set_serial_threshold(LogLevel level) { journal::set_serial_threshold(level); }

auto get_serial_threshold() -> LogLevel { return journal::get_serial_threshold(); }

namespace {

// Minimal hex printer that avoids any allocations or locks.
void panic_write_hex(uint64_t val) {
    constexpr size_t BUF_SIZE = 19;  // "0x" + 16 hex digits + NUL
    std::array<char, BUF_SIZE> buf{};
    buf.at(0) = '0';
    buf.at(1) = 'x';
    constexpr const char* HEX = "0123456789abcdef";
    for (int i = 15; i >= 0; --i) {
        uint8_t const NIBBLE = (val >> (i * 4)) & 0xF;
        size_t const OUT_IDX = 2U + static_cast<size_t>(15 - i);
        buf.at(OUT_IDX) = HEX[NIBBLE];
    }
    buf.at(BUF_SIZE - 1) = '\0';
    io::serial::write_unlocked(buf.data());
}

void panic_write_dec(uint64_t val) {
    constexpr size_t BUF_SIZE = 21;  // enough for 64-bit decimal + NUL
    std::array<char, BUF_SIZE> buf{};
    size_t pos = BUF_SIZE - 1;
    buf.at(pos) = '\0';
    if (val == 0) {
        buf.at(--pos) = '0';
    } else {
        while (val > 0) {
            buf.at(--pos) = static_cast<char>('0' + (val % 10));
            val /= 10;
        }
    }
    io::serial::write_unlocked(buf.data() + pos);
}

void panic_write_reg(const char* name, uint64_t val) {
    io::serial::write_unlocked("  ");
    io::serial::write_unlocked(name);
    io::serial::write_unlocked(": 0x");
    panic_write_hex(val);
    io::serial::write_unlocked("\n");
}

// Walk the frame-pointer (RBP) chain and store return addresses.
void panic_walk_stack(void** fp, std::array<void*, MAX_FRAMES_BACKTRACE>& out) {
    for (int i = 0; i < MAX_FRAMES_BACKTRACE; i++) {
        auto& frame = out.at(static_cast<size_t>(i));
        frame = nullptr;
        if (fp == nullptr || reinterpret_cast<uint64_t>(fp) < 0xffff000000000000ULL) {
            break;
        }
        frame = *(fp + 1);  // return address at [rbp+8]
        auto* next = reinterpret_cast<void**>(*fp);
        if (next <= fp) {
            break;
        }
        fp = next;
    }
}

}  // namespace

void panic_handler(const char* msg) {
    // Enter panic mode so serial writes bypass locking (avoids deadlock if
    // we panicked while the serial lock was already held).
    io::serial::enter_panic_mode();

    // If another CPU already owns the panic, halt immediately.  This covers
    // races where multiple CPUs trigger sanitizer violations simultaneously,
    // and also UBSAN paths that call enterPanicMode() themselves before
    // calling panic_handler (making the second enterPanicMode() return false
    // even though this IS the owner — isPanicOwner() handles that correctly).
    if (!io::serial::is_panic_owner()) {
        hcf();
    }

    // Halt other CPUs immediately so they stop writing to serial.
    ker::mod::smt::halt_other_cores();

    io::serial::write_unlocked("\n========== KERNEL PANIC ==========\n");
    io::serial::write_unlocked("Reason: ");
    io::serial::write_unlocked(msg);
    io::serial::write_unlocked("\n");

    // CPU id (best-effort)
    io::serial::write_unlocked("CPU: ");
    panic_write_dec(cpu::get_current_cpu_id_safe());
    io::serial::write_unlocked("\n");

    // Capture general-purpose registers via inline asm.
    // NOLINTBEGIN(misc-const-correctness)
    uint64_t rax = 0;
    uint64_t rbx = 0;
    uint64_t rcx = 0;
    uint64_t rdx = 0;
    uint64_t rsi = 0;
    uint64_t rdi = 0;
    uint64_t rbp = 0;
    uint64_t rsp = 0;
    uint64_t r8 = 0;
    uint64_t r9 = 0;
    uint64_t r10 = 0;
    uint64_t r11 = 0;
    uint64_t r12 = 0;
    uint64_t r13 = 0;
    uint64_t r14 = 0;
    uint64_t r15 = 0;
    uint64_t rflags = 0;
    uint64_t cr2 = 0;
    uint64_t cr3 = 0;
    // NOLINTEND(misc-const-correctness)
    asm volatile("movq %%rax, %0" : "=m"(rax));
    asm volatile("movq %%rbx, %0" : "=m"(rbx));
    asm volatile("movq %%rcx, %0" : "=m"(rcx));
    asm volatile("movq %%rdx, %0" : "=m"(rdx));
    asm volatile("movq %%rsi, %0" : "=m"(rsi));
    asm volatile("movq %%rdi, %0" : "=m"(rdi));
    asm volatile("movq %%rbp, %0" : "=m"(rbp));
    asm volatile("movq %%rsp, %0" : "=m"(rsp));
    asm volatile("movq %%r8,  %0" : "=m"(r8));
    asm volatile("movq %%r9,  %0" : "=m"(r9));
    asm volatile("movq %%r10, %0" : "=m"(r10));
    asm volatile("movq %%r11, %0" : "=m"(r11));
    asm volatile("movq %%r12, %0" : "=m"(r12));
    asm volatile("movq %%r13, %0" : "=m"(r13));
    asm volatile("movq %%r14, %0" : "=m"(r14));
    asm volatile("movq %%r15, %0" : "=m"(r15));
    asm volatile("pushfq; popq %0" : "=r"(rflags));
    asm volatile("movq %%cr2, %0" : "=r"(cr2));
    asm volatile("movq %%cr3, %0" : "=r"(cr3));

    io::serial::write_unlocked("\n--- Registers ---\n");
    panic_write_reg("RAX", rax);
    panic_write_reg("RBX", rbx);
    panic_write_reg("RCX", rcx);
    panic_write_reg("RDX", rdx);
    panic_write_reg("RSI", rsi);
    panic_write_reg("RDI", rdi);
    panic_write_reg("RBP", rbp);
    panic_write_reg("RSP", rsp);
    panic_write_reg("R8 ", r8);
    panic_write_reg("R9 ", r9);
    panic_write_reg("R10", r10);
    panic_write_reg("R11", r11);
    panic_write_reg("R12", r12);
    panic_write_reg("R13", r13);
    panic_write_reg("R14", r14);
    panic_write_reg("R15", r15);
    panic_write_reg("RFLAGS", rflags);
    panic_write_reg("CR2", cr2);
    panic_write_reg("CR3", cr3);

    // RIP via return address of this function
    void const* rip = __builtin_return_address(0);
    panic_write_reg("RIP (caller)", reinterpret_cast<uint64_t>(rip));

    // Stack trace via RBP chain
    io::serial::write_unlocked("\n--- Stack Trace ---\n");
    std::array<void*, MAX_FRAMES_BACKTRACE> frames{};
    auto* fp = reinterpret_cast<void**>(__builtin_frame_address(0));
    panic_walk_stack(fp, frames);
    for (int i = 0; i < MAX_FRAMES_BACKTRACE; i++) {
        const void* const FRAME = frames.at(static_cast<size_t>(i));
        if (FRAME == nullptr) {
            break;
        }
        io::serial::write_unlocked("  #");
        panic_write_dec(static_cast<uint64_t>(i));
        io::serial::write_unlocked(" 0x");
        panic_write_hex(reinterpret_cast<uint64_t>(FRAME));
        io::serial::write_unlocked("\n");
    }

    // Raw stack dump (top 64 qwords)
    io::serial::write_unlocked("\n--- Raw Stack (top 64 qwords) ---\n");
    auto* rsp_ptr = reinterpret_cast<uint64_t*>(rsp);
    auto rsp_addr = reinterpret_cast<uintptr_t>(rsp_ptr);
    bool const RSP_VALID = (rsp_addr >= 0xffff800000000000ULL && rsp_addr < 0xffff900000000000ULL) ||
                           (rsp_addr >= 0xffffffff80000000ULL && rsp_addr < 0xffffffffc0000000ULL);
    if (RSP_VALID) {
        for (uint64_t i = 0; i < MAX_RAW_STACK_TRACE; i++) {
            io::serial::write_unlocked("  [RSP+0x");
            panic_write_hex(i * sizeof(uint64_t));
            io::serial::write_unlocked("] 0x");
            panic_write_hex(rsp_ptr[i]);
            io::serial::write_unlocked("\n");
        }
    } else {
        io::serial::write_unlocked("  RSP 0x");
        panic_write_hex(rsp_addr);
        io::serial::write_unlocked(" is not in a valid kernel range, skipping\n");
    }

    io::serial::write_unlocked("==================================\n\n");

    hcf();
}

}  // namespace ker::mod::dbg
