#pragma once

namespace ker::mod::ntp {

// Spawn the background SNTP sync kernel thread.
// Must be called after the scheduler and network drivers are initialised
// (PHASE_6 or later).
void init();

}  // namespace ker::mod::ntp
