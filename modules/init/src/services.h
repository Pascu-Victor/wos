#pragma once

constexpr long SERVICE_SUPERVISOR_TICK_MS = 100;

// Load, validate, and start the fixed-capacity PID 1 service supervisor. A
// false result leaves PID 1 alive in failed-supervisor mode so it can continue
// reaping children, accepting shutdown, and publishing status.
auto start_service_supervisor() -> bool;

// One nonblocking supervisor iteration. This is the sole owner of root init's
// waitpid(-1, WNOHANG) loop.
void service_supervisor_tick();

// Bounded model-driven shutdown phases. The first call stops and drains every
// non-journal service in reverse dependency order. The second call is made
// only after the first filesystem sync and stops journald last.
void stop_services_for_shutdown();
void stop_journald_for_shutdown();
