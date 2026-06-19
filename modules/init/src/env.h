#pragma once

#include <array>

struct InitEnv {
    std::array<char, 5 + 256> home{};
    std::array<char, 9 + 64> hostname{};
    std::array<char, 128> llvm_profile_file{};
    std::array<char, 5 + 256> user{};
    std::array<const char*, 6> envp{};
};

auto make_init_env() -> InitEnv;
void configure_init_coverage_profile();
