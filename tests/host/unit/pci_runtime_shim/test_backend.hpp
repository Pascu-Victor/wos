#pragma once
#include <atomic>
#include <cstdint>
namespace pci_runtime {
extern thread_local unsigned lock_depth;
extern std::atomic<uint64_t> lock_acquisitions;
}  // namespace pci_runtime
