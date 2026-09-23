#pragma once

#ifdef WOS_SELFTEST
#include <dev/ivshmem/pci_resources.hpp>

namespace ker::dev::ivshmem {
void selftest_start(PciResources* resources, const char* kind, int (*retire)(), bool (*late_calls)(), void (*lease)(bool) = nullptr);
}
#endif
