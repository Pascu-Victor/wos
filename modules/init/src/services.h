#pragma once

#include <cstdint>

enum class ServiceKind : uint8_t {
    NETWORK,
    NORMAL,
    JOURNAL,
};

auto spawn_local_service(const char* path, const char* const* argv, const char* const* envp) -> uint64_t;
void register_service(const char* name, uint64_t pid, ServiceKind kind);
void note_service_reaped(uint64_t pid);
void stop_services_for_shutdown();
void stop_journald_for_shutdown();

void start_journald();
void start_httpd();
void start_dropbear();
void start_testd();
