#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "reclaim.hpp"

namespace ker::mod::mm::reclaim::callback_boundary {
namespace {
static_assert(std::is_trivially_copyable_v<ReclaimRequest> && std::is_trivially_copyable_v<ReclaimCount> &&
              std::is_trivially_copyable_v<ReclaimScanResult>);

// These exact representation accesses execute in this instrumented TU before
// any callback can acquire its source locks. Objects are already alive and
// initialized; no poison suppression or masked shadow write is performed.
[[gnu::noinline]] void prepare_boundary(void* output, size_t bytes, const ReclaimRequest& request) {
    auto* destination = static_cast<volatile uint8_t*>(output);
    for (size_t i = 0; i < bytes; ++i) destination[i] = 0;
    auto const* source = reinterpret_cast<const volatile uint8_t*>(&request);
    for (size_t i = 0; i < sizeof(request); ++i) static_cast<void>(source[i]);
}
}  // namespace

[[gnu::noinline]] void invoke_node_count_into(ReclaimCount& output, CountCallback callback, void* opaque, const ReclaimRequest& request) {
    prepare_boundary(&output, sizeof(output), request);
    ReclaimCount const RESULT = callback(opaque, request);
    output = RESULT;
}

[[gnu::noinline]] void invoke_node_scan_into(ReclaimScanResult& output, ScanCallback callback, void* opaque,
                                             const ReclaimRequest& request) {
    prepare_boundary(&output, sizeof(output), request);
    ReclaimScanResult const RESULT = callback(opaque, request);
    output = RESULT;
}
}  // namespace ker::mod::mm::reclaim::callback_boundary
