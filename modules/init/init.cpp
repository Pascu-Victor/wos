#include <abi/syslog.hpp>

extern "C" int _start(void) {
    for (;;) ker::abi::syslog::log("init\n", 5, ker::abi::inter::sysLog::sys_log_device::vga);
}
