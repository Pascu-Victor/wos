#pragma once

#include <array>

struct InitEnv {
    std::array<char, 5 + 256> home{};
    std::array<char, 9 + 64> hostname{};
    std::array<char, 5 + 256> user{};
    std::array<const char*, 5> envp{};
};

auto make_init_env() -> InitEnv;
