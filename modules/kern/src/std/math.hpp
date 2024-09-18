#pragma once

int abs(int x);
int max(int x, int y);
int min(int x, int y);
int pow(int x, int y);

namespace std {
namespace math {
constexpr int abs(int x) { return x < 0 ? -x : x; }

constexpr int max(int x, int y) { return x > y ? x : y; }

constexpr int min(int x, int y) { return x < y ? x : y; }

constexpr int pow(int x, int y) {
    int result = 1;
    for (int i = 0; i < y; i++) {
        result *= x;
    }
    return result;
}
}  // namespace math
}  // namespace std
