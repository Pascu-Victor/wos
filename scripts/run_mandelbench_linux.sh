#!/bin/bash
# Build and run mandelbench natively on Linux using the exact same sources and
# compiler flags as the WOS build. This gives an apples-to-apples comparison.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$SCRIPT_DIR/.."
SRC="$REPO/modules/testprog/src/mandelbench"
OUT_DIR="/tmp/mandelbench_linux"

mkdir -p "$OUT_DIR"

# Same flags as modules/CMakeLists.txt add_compile_options (minus WOS-specific ones)
# Removed: --sysroot, -fno-stack-protector, -fno-stack-check, -fno-sanitize=safe-stack
# (those are for the freestanding kernel environment, not relevant for native Linux)
FLAGS="-O3 -march=native -flto -std=c++23 -Wall -Wextra -pipe -fno-omit-frame-pointer"

# Stub main that drives the benchmark the same way testprog does
cat > "$OUT_DIR/main_linux.cpp" << 'EOF'
#include <cstdlib>
#include <cstring>
#include "mandelbench/config.hpp"
#include "mandelbench/mandelbench.hpp"
#include "mandelbench/util.hpp"

int main() {
    auto* image    = static_cast<unsigned char*>(malloc(WIDTH * HEIGHT * 4));
    auto* colormap = static_cast<unsigned char*>(malloc((MAX_ITERATION + 1) * 3));
    init_colormap(MAX_ITERATION, colormap);
    mandelbench(WIDTH, HEIGHT, MAX_ITERATION, THREADS, REPEAT, image, colormap);
    free(image);
    free(colormap);
    return 0;
}
EOF

echo "Building mandelbench for Linux (native)..."
clang++ $FLAGS \
    -I"$REPO/modules/testprog/src" \
    "$SRC/mandelbench.cpp" \
    "$SRC/util.cpp" \
    "$SRC/lodepng.cpp" \
    "$SRC/tinycthread.cpp" \
    "$OUT_DIR/main_linux.cpp" \
    -lpthread \
    -o "$OUT_DIR/mandelbench_linux"

echo "Running in $OUT_DIR ..."
cd "$OUT_DIR"
"$OUT_DIR/mandelbench_linux"
