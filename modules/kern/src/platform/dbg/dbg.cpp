#include "dbg.hpp"

namespace ker::mod::dbg {
bool isInit = false;
bool isTimeAvailable = false;
bool isKmallocAvailable = false;
sys::Spinlock logLock;
using namespace ker::mod;

uint64_t linesLogged = 0;

void init(void) {
    if (isInit) {
        return;
    }
    io::serial::init();
    logLock = sys::Spinlock();
    isInit = true;
}

void enableTime(void) {
    if (isTimeAvailable) {
        // Panic! should only be called once when ktime is initialized
        hcf();
    }
    isTimeAvailable = true;
    log("Kernel time is now available");
}

void enableKmalloc(void) {
    if (isKmallocAvailable) {
        // Panic! should only be called once when kmalloc is initialized
        hcf();
    }
    isKmallocAvailable = true;
    log("Kernel memory allocator is now available");
}

inline void serialLog(const char* str) {
    if (isTimeAvailable) [[likely]] {
        char timeSec[10] = {0};
        char timeMs[5] = {0};
        int logTime = time::getMs();
        int logTimeMsPart = logTime % 1000;
        int logTimeSecPart = logTime / 1000;
        std::u64toa(logTimeMsPart, timeMs, 10);
        std::u64toa(logTimeSecPart, timeSec, 10);
        io::serial::write('[');
        io::serial::write(timeSec);
        io::serial::write('.');
        io::serial::write(timeMs);
        io::serial::write("]:");
    }
    io::serial::write(str);
}

inline void serialNewline() { io::serial::write('\n'); }

inline void fbLog(const char* str) {
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
        int msLen = std::u64toa(logTimeMsPart, timeMs, 10);
        int secLen = std::u64toa(logTimeSecPart, timeSec, 10);
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
    gfx::fb::drawString(stampLen, line, str);
}

void logNewLineNoSync(void) {
    serialNewline();
    linesLogged++;
}

void logNoSync(const char* str) {
    serialLog(str);
    fbLog(str);
}

void logString(const char* str) {
    logLock.lock();
    logNoSync(str);
    logNewLineNoSync();
    logLock.unlock();
}

void logVa(const char* format, va_list& args) {
    // 4k should be enough for everyone
    char buf[4096];
    va_list argsCopy;
    va_copy(argsCopy, args);
    // if we have args
    if (args) {
        std::vsnprintf(buf, 4096ul, format, argsCopy);
    } else {
        std::strncpy(buf, format, 4096);
    }
    va_end(argsCopy);
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

}  // namespace ker::mod::dbg
