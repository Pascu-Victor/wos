#pragma once

#include <dev/device_mapping_object.hpp>
#include <platform/mm/device_mapping_vm.hpp>

namespace ker::mod::mm::device_vm {

// One ordinary-context attempt. Closes new Object admission permanently, then
// withdraws actual indexed CPU aliases. EAGAIN retains closed backing for retry
// while prepared owners, busy roots, callbacks, pins or exports still exist.
// Caller holds a real Object owner, no incoming PUB/root/IRQ guard. Success is
// CPU translation/owner drain only, never hardware DMA/reset completion.
[[nodiscard]] auto revoke_cpu_mappings(const ker::dev::mapping::ObjectRef&, Limits = {}) -> int;

}  // namespace ker::mod::mm::device_vm
