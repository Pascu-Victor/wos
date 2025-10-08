#pragma once

#include <cstdint>

namespace ker::dev {

struct Device {
    unsigned major;
    unsigned minor;
    const char* name;
    void* private_data;
};

int dev_register(unsigned major, unsigned minor, const char* name, void* priv);
Device* dev_find(unsigned major, unsigned minor);

}  // namespace ker::dev
