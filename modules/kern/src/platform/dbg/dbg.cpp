#include "dbg.hpp"

#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <platform/asm/cpu.hpp>
#include <platform/smt/smt.hpp>
#include <util/string.hpp>

#include "mod/gfx/fb.hpp"
#include "mod/io/serial/serial.hpp"
#include "platform/ktime/ktime.hpp"
#include "platform/sys/spinlock.hpp"
#include "util/hcf.hpp"

namespace ker::mod::dbg {
namespace {
sys::Spinlock logLock{};
bool isInit = false;
bool isTimeAvailable = false;
bool isKmallocAvailable = false;
uint64_t linesLogged = 0;
}  // namespace

using namespace ker::mod;

void init() {
    if (isInit) {
        return;
    }
    io::serial::init();
    isInit = true;
}

void enableTime() {
    if (isTimeAvailable) {
        // Panic! should only be called once when ktime is initialized
        panic_handler("Kernel time was already initialized");
    }
    isTimeAvailable = true;
    log("Kernel time is now available");
}

void break_into_debugger() { __asm__ volatile("int $3"); }

void enableKmalloc() {
    if (isKmallocAvailable) {
        // Panic! should only be called once when kmalloc is initialized
        panic_handler("Kernel kmalloc already initialized");
    }
    isKmallocAvailable = true;
    log("Kernel memory allocator is now available");
}

// Write timestamp + message + newline to serial.
// In panic mode the panic-dump caller already holds panicAcquireLock() for its
// entire dump, so we must NOT re-acquire (that would deadlock).  We write
// directly with the unlocked variants instead.
// Outside panic mode we take ScopedLock to prevent per-line interleaving.
inline void serialLogLine(const char* str) {
    // Helper: write timestamp + str + newline using only unlocked writes.
    // Called both in panic mode (no lock) and in normal mode (lock already held
    // via ScopedLock below).
    auto writeLineUnlocked = [&]() {
        if (isTimeAvailable) [[likely]] {
            char timeSec[10] = {0};
            char timeMs[5] = {0};
            int logTime = time::getMs();
            int logTimeMsPart = logTime % 1000;
            int logTimeSecPart = logTime / 1000;
            auto u64toa_local = [](uint64_t n, char* s, int base) -> int {
                if (n == 0) {
                    s[0] = '0';
                    s[1] = '\0';
                    return 1;
                }
                char buf[32];
                int i = 0;
                while (n > 0) {
                    int digit = n % base;
                    buf[i++] = (digit < 10) ? ('0' + digit) : ('a' + digit - 10);
                    n /= base;
                }
                int j = 0;
                while (i > 0) {
                    s[j++] = buf[--i];
                }
                s[j] = '\0';
                return j;
            };
            u64toa_local(static_cast<uint64_t>(logTimeMsPart), timeMs, 10);
            u64toa_local(static_cast<uint64_t>(logTimeSecPart), timeSec, 10);
            io::serial::writeUnlocked('[');
            io::serial::writeUnlocked(timeSec);
            io::serial::writeUnlocked('.');
            io::serial::writeUnlocked(timeMs);
            io::serial::writeUnlocked("]:");
        }
        io::serial::writeUnlocked(str);
        io::serial::writeUnlocked('\n');
    };

    // In panic mode the caller already holds the panic lock for the whole dump.
    if (io::serial::isPanicMode()) {
        writeLineUnlocked();
        return;
    }
    io::serial::ScopedLock lock;
    writeLineUnlocked();
}

inline void fbLog(const char* str) {
    if constexpr (gfx::fb::WOS_HAS_GFX_FB) {
        uint64_t line = linesLogged;
        if (linesLogged >= gfx::fb::viewportHeightChars()) {
            gfx::fb::scroll();
            line = gfx::fb::viewportHeightChars() - 1;
        }

        gfx::fb::drawChar(0, line, '[');
        // todo maybe print cpu id
        int stamp_len = 1;
        if (isTimeAvailable) [[likely]] {
            char timeSec[10] = {0};  // good enough for 30 years of uptime
            char timeMs[5] = {0};
            int logTime = time::getMs();
            int logTimeMsPart = logTime % 1000;
            int logTimeSecPart = logTime / 1000;
            auto u64toa_local2 = [](uint64_t n, char* s, int base) -> int {
                if (n == 0) {
                    s[0] = '0';
                    s[1] = '\0';
                    return 1;
                }
                char buf[32];
                int i = 0;
                while (n > 0) {
                    int digit = n % base;
                    buf[i++] = (digit < 10) ? ('0' + digit) : ('a' + digit - 10);
                    n /= base;
                }
                int j = 0;
                while (i > 0) {
                    s[j++] = buf[--i];
                }
                s[j] = '\0';
                return j;
            };
            int msLen = u64toa_local2(logTimeMsPart, timeMs, 10);
            int secLen = u64toa_local2(logTimeSecPart, timeSec, 10);
            gfx::fb::drawString(stamp_len, line, timeSec);
            stamp_len += secLen;
            gfx::fb::drawChar(stamp_len, line, '.');
            stamp_len++;
            gfx::fb::drawString(stamp_len, line, timeMs);
            stamp_len += msLen;
        }
        gfx::fb::drawChar(stamp_len, line, ']');
        stamp_len++;
        gfx::fb::drawChar(stamp_len, line, ':');
        stamp_len++;
        linesLogged += gfx::fb::drawString(stamp_len, line, str);
    } else {
        mod::io::serial::write("Tried to write to framebuffer, module not enabled\n");
    }
}

void logString(const char* str) {
    serialLogLine(str);
    // logLock only protects the framebuffer state and linesLogged counter.
    logLock.lock();
    if constexpr (gfx::fb::WOS_HAS_GFX_FB) {
        fbLog(str);
    }
    linesLogged++;
    logLock.unlock();
}

void logVa(const char* format, va_list& args) {
    // 4k should be enough for everyone
    char buf[4096];

    std::vsnprintf(buf, sizeof(buf), format, args);
    logString(buf);
}

void __logVar(const char* format, ...) {
    va_list args;
    va_start(args, format);
    logVa(format, args);
    va_end(args);
}

void logFbOnly(const char* str) {
    logLock.lock();
    fbLog(str);
    logLock.unlock();
}

void logFbAdvance(void) {
    logLock.lock();
    linesLogged++;
    logLock.unlock();
}

void error(const char* str) {
    // TODO: pretty print error
    log(str);
}

namespace {

// Minimal hex printer that avoids any allocations or locks.
void panic_write_hex(uint64_t val) {
    char hex[17];
    for (int i = 15; i >= 0; --i) {
        uint8_t nibble = (val >> (i * 4)) & 0xF;
        hex[15 - i] = nibble < 10 ? static_cast<char>('0' + nibble) : static_cast<char>('a' + nibble - 10);
    }
    hex[16] = '\0';
    io::serial::writeUnlocked(hex);
}

void panic_write_dec(uint64_t val) {
    char buf[21];
    int pos = 20;
    buf[pos] = '\0';
    if (val == 0) {
        buf[--pos] = '0';
    } else {
        while (val > 0) {
            buf[--pos] = static_cast<char>('0' + (val % 10));
            val /= 10;
        }
    }
    io::serial::writeUnlocked(buf + pos);
}

void panic_write_reg(const char* name, uint64_t val) {
    io::serial::writeUnlocked("  ");
    io::serial::writeUnlocked(name);
    io::serial::writeUnlocked(": 0x");
    panic_write_hex(val);
    io::serial::writeUnlocked("\n");
}

// Walk the frame-pointer (RBP) chain and store return addresses.
void panic_walk_stack(void** fp, void** out, int depth) {
    for (int i = 0; i < depth; i++) {
        out[i] = nullptr;
        if (fp == nullptr || reinterpret_cast<uint64_t>(fp) < 0xffff000000000000ULL) {
            break;
        }
        out[i] = *(fp + 1);  // return address at [rbp+8]
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
    io::serial::enterPanicMode();

    // Halt other CPUs immediately so they stop writing to serial.
    ker::mod::smt::halt_other_cores();

    io::serial::writeUnlocked("\n========== KERNEL PANIC ==========\n");
    io::serial::writeUnlocked("Reason: ");
    io::serial::writeUnlocked(msg);
    io::serial::writeUnlocked("\n");

    // CPU id (best-effort)
    io::serial::writeUnlocked("CPU: ");
    panic_write_dec(cpu::getCurrentCpuIdSafe());
    io::serial::writeUnlocked("\n");

    // Capture general-purpose registers via inline asm.
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

    io::serial::writeUnlocked("\n--- Registers ---\n");
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
    void* rip = __builtin_return_address(0);
    panic_write_reg("RIP (caller)", reinterpret_cast<uint64_t>(rip));

    // Stack trace via RBP chain
    io::serial::writeUnlocked("\n--- Stack Trace ---\n");
    constexpr int MAX_FRAMES = 32;
    void* frames[MAX_FRAMES] = {};
    auto* fp = reinterpret_cast<void**>(__builtin_frame_address(0));
    panic_walk_stack(fp, frames, MAX_FRAMES);
    for (int i = 0; i < MAX_FRAMES; i++) {
        if (frames[i] == nullptr) {
            break;
        }
        io::serial::writeUnlocked("  #");
        panic_write_dec(static_cast<uint64_t>(i));
        io::serial::writeUnlocked(" 0x");
        panic_write_hex(reinterpret_cast<uint64_t>(frames[i]));
        io::serial::writeUnlocked("\n");
    }

    // Raw stack dump (top 64 qwords)
    io::serial::writeUnlocked("\n--- Raw Stack (top 64 qwords) ---\n");
    auto* rsp_ptr = reinterpret_cast<uint64_t*>(rsp);
    auto rsp_addr = reinterpret_cast<uintptr_t>(rsp_ptr);
    bool rsp_valid = (rsp_addr >= 0xffff800000000000ULL && rsp_addr < 0xffff900000000000ULL) ||
                     (rsp_addr >= 0xffffffff80000000ULL && rsp_addr < 0xffffffffc0000000ULL);
    if (rsp_valid) {
        for (int i = 0; i < 64; i++) {
            io::serial::writeUnlocked("  [RSP+0x");
            panic_write_hex(static_cast<uint64_t>(i * 8));
            io::serial::writeUnlocked("] 0x");
            panic_write_hex(rsp_ptr[i]);
            io::serial::writeUnlocked("\n");
        }
    } else {
        io::serial::writeUnlocked("  RSP 0x");
        panic_write_hex(rsp_addr);
        io::serial::writeUnlocked(" is not in a valid kernel range, skipping\n");
    }

    io::serial::writeUnlocked("==================================\n\n");

    hcf();
}

}  // namespace ker::mod::dbg
