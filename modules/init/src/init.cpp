#include <abi/multiproc.hpp>
#include <abi/syslog.hpp>
#include <std/string.hpp>

void* operator new[](unsigned long v) { return (void*)12; }
void operator delete[](void*) noexcept {}

extern "C" int _start(void) {
    uint64_t threadId = ker::abi::multiproc::currentThreadId();
    char message[16];
    std::snprintf(message, 16, "init: %x\n", threadId);
    for (;;) {
        ker::abi::syslog::log(message, 5, ker::abi::inter::sysLog::sys_log_device::vga);
    }
}
