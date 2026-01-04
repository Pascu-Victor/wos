#include "dbg.hpp"

#include <cstdarg>
#include <platform/smt/smt.hpp>
#include <util/string.hpp>

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

void enableTime(void) {
    if (isTimeAvailable) {
        // Panic! should only be called once when ktime is initialized
        panicHandler("Kernel time was already initialized");
    }
    isTimeAvailable = true;
    log("Kernel time is now available");
}

void breakIntoDebugger(void) { __asm__ volatile("int $3"); }

void enableKmalloc(void) {
    if (isKmallocAvailable) {
        // Panic! should only be called once when kmalloc is initialized
        panicHandler("Kernel kmalloc already initialized");
    }
    isKmallocAvailable = true;
    log("Kernel memory allocator is now available");
}

// Write a complete log line atomically to serial (timestamp + message + newline)
inline void serialLogLine(const char* str) {
    // Hold the serial lock for the entire log line to prevent interleaving
    io::serial::ScopedLock lock;

    if (isTimeAvailable) [[likely]] {
        char timeSec[10] = {0};
        char timeMs[5] = {0};
        int logTime = time::getMs();
        int logTimeMsPart = logTime % 1000;
        int logTimeSecPart = logTime / 1000;
        // simple u64 to ascii helper (avoid depending on userspace std here)
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
        u64toa_local(logTimeMsPart, timeMs, 10);
        u64toa_local(logTimeSecPart, timeSec, 10);
        io::serial::writeUnlocked('[');
        io::serial::writeUnlocked(timeSec);
        io::serial::writeUnlocked('.');
        io::serial::writeUnlocked(timeMs);
        io::serial::writeUnlocked("]:");
    }
    io::serial::writeUnlocked(str);
    io::serial::writeUnlocked('\n');
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
        int stampLen = 1;
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
            gfx::fb::drawString(stampLen, line, timeSec);
            stampLen += secLen;
            gfx::fb::drawChar(stampLen, line, '.');
            stampLen++;
            gfx::fb::drawString(stampLen, line, timeMs);
            stampLen += msLen;
        }
        gfx::fb::drawChar(stampLen, line, ']');
        stampLen++;
        gfx::fb::drawChar(stampLen, line, ':');
        stampLen++;
        linesLogged += gfx::fb::drawString(stampLen, line, str);
    } else {
        mod::io::serial::write("Tried to write to framebuffer, module not enabled\n");
    }
}

void logString(const char* str) {
    logLock.lock();
    // Write atomically to serial (includes newline)
    serialLogLine(str);
    // Framebuffer logging
    if constexpr (gfx::fb::WOS_HAS_GFX_FB) {
        fbLog(str);
    }
    linesLogged++;
    logLock.unlock();
}

void logVa(const char* format, va_list& args) {
    // 4k should be enough for everyone
    char buf[4096];

    // Defensive: avoid dereferencing potentially-invalid %s pointers from varargs.
    // Replace simple "%s" specifiers with "%p" so vsnprintf prints the pointer
    // value instead of calling strlen() on an invalid address.
    char safe_fmt[1024];
    size_t si = 0;
    for (size_t i = 0; format[i] != '\0' && si + 2 < sizeof(safe_fmt); ++i) {
        if (format[i] == '%') {
            // Copy the '%' first
            safe_fmt[si++] = '%';
            char next = format[i + 1];
            if (next == '%') {
                // literal %% -> copy one % and advance past second
                ++i;
                safe_fmt[si - 1] = '%';
            } else if (next == 's') {
                // replace %s with %p
                safe_fmt[si++] = 'p';
                ++i;  // skip 's'
            } else {
                // copy next char as-is (handles %d, %x, %u etc.)
                if (next != '\0') {
                    safe_fmt[si++] = next;
                    ++i;
                }
            }
        } else {
            safe_fmt[si++] = format[i];
        }
    }
    safe_fmt[si] = '\0';

    std::vsnprintf(buf, sizeof(buf), safe_fmt, args);
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
    logLock.lock();
    log(str);
    // TODO: pretty print error
    logLock.unlock();
}

void panicHandler(const char* msg) {
    // Log the panic message
    log("KERNEL PANIC: %s", msg);

    // Try to halt other CPUs to stabilize global state for any dump/inspection
    ker::mod::smt::haltOtherCores();

    // If stacktrace printing is desired and available, it can be called here.
    // Finally, stop this CPU.
    hcf();
}

}  // namespace ker::mod::dbg
