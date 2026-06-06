#pragma once

#include <cstdint>

auto spawn_local_service(const char* path, const char* const* argv, const char* const* envp) -> uint64_t;

void start_journald();
void start_httpd();
void start_dropbear();
void start_testd();
