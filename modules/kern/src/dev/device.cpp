#include "device.hpp"

#include <mod/io/serial/serial.hpp>

namespace ker::dev {

auto dev_register(unsigned major, unsigned minor, const char* name, void* priv) -> int {
    mod::io::serial::write("dev_register stub\n");
    return 0;
}

auto dev_find(unsigned major, unsigned minor) -> Device* {
    mod::io::serial::write("dev_find stub\n");
    return nullptr;
}

}  // namespace ker::dev
