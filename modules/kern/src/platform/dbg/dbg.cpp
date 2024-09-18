#include "dbg.hpp"

namespace ker::mod::dbg {
bool isInit = false;
bool isTimeAvailable = false;
bool isKmallocAvailable = false;
using namespace ker::mod;

uint64_t linesLogged = 0;

void init(void) {
    if (isInit) {
        return;
    }
    io::serial::init();
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
    io::serial::write('\n');
}

inline void fbLog(const char* str) {
    uint64_t line = linesLogged;
    if (linesLogged >= gfx::fb::viewportHeightChars()) {
        gfx::fb::scroll();
        line = gfx::fb::viewportHeightChars() - 1;
    }

    gfx::fb::drawChar(0, line, '[');
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

void log(const char* str) {
    serialLog(str);
    fbLog(str);
    linesLogged++;
}

void error(const char* str) {
    log(str);
    // TODO: pretty print error
}

}  // namespace ker::mod::dbg
