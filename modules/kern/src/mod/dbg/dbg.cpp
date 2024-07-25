#include "dbg.hpp"

inline static  bool isInit = false;

namespace ker::mod::dbg {
    using namespace ker::mod;

    void init(void) {
        if(isInit) {
            return;
        }
        io::serial::init();
        time::init();
        isInit = true;
    }

    void log(const char *str) {
        char timeSec[21] = {0};
        char timeMs[8] = {0};
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
        io::serial::write(str);
        io::serial::write('\n');
    }

    void error(const char *str) {
        log(str);
        //TODO: pretty print error
    }
}