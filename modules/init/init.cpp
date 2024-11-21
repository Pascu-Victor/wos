#include <abi/syslog.hpp>

extern "C" int _start(void) {
    for (;;) ker::abi::syslog::log("init\n", 5);
}
