#!/bin/bash
# Build and run mandelbench inside the Ubuntu KVM guest.
# Sources are copied to the guest and compiled there with the guest's clang,
# so -march=native reflects exactly what the KVM guest CPU exposes.
#
# Usage: ./scripts/run_mandelbench_ubuntu_kvm.sh <ubuntu-vm-ip>
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <ubuntu-vm-ip>" >&2
  exit 1
fi

TARGET_IP="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$SCRIPT_DIR/.."

REMOTE_USER="${WOS_LINUX_USER:-user}"
REMOTE_PASS="${WOS_LINUX_PASSWORD:-1234}"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)
SCP="sshpass -p $REMOTE_PASS scp ${SSH_OPTS[*]}"
SSH="sshpass -p $REMOTE_PASS ssh ${SSH_OPTS[*]} ${REMOTE_USER}@${TARGET_IP}"

echo "=== Preparing source tarball ==="
TARBALL="/tmp/mandelbench_src.tar.gz"
tar -czf "$TARBALL" \
  -C "$REPO/modules/testprog/src" \
  mandelbench/config.hpp \
  mandelbench/mandelbench.cpp \
  mandelbench/mandelbench.hpp \
  mandelbench/util.cpp \
  mandelbench/util.hpp \
  mandelbench/lodepng.cpp \
  mandelbench/lodepng.hpp \
  mandelbench/tinycthread.cpp \
  mandelbench/tinycthread.hpp

cat > /tmp/mandelbench_main.cpp << 'EOF'
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

echo "=== Uploading sources to Ubuntu VM ($TARGET_IP) ==="
sshpass -p "$REMOTE_PASS" scp "${SSH_OPTS[@]}" "$TARBALL" "${REMOTE_USER}@${TARGET_IP}:/tmp/mandelbench_src.tar.gz"
sshpass -p "$REMOTE_PASS" scp "${SSH_OPTS[@]}" "/tmp/mandelbench_main.cpp" "${REMOTE_USER}@${TARGET_IP}:/tmp/mandelbench_main.cpp"

echo ""
echo "=== Building and running mandelbench on Ubuntu KVM guest ($TARGET_IP) ==="
sshpass -p "$REMOTE_PASS" ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${TARGET_IP}" \
  'set -e
   cd /tmp
   rm -rf mandelbench_build && mkdir mandelbench_build && cd mandelbench_build
   tar -xzf /tmp/mandelbench_src.tar.gz
   cp /tmp/mandelbench_main.cpp .
   if command -v clang++ >/dev/null 2>&1; then CXX=clang++; else CXX=g++; fi
   echo "Compiler: $($CXX --version | head -1)"
   $CXX -O3 -march=native -std=c++23 -pipe -fno-omit-frame-pointer \
       -I. \
       mandelbench/mandelbench.cpp mandelbench/util.cpp \
       mandelbench/lodepng.cpp mandelbench/tinycthread.cpp \
       mandelbench_main.cpp \
       -lpthread -o mandelbench_linux
   echo "Build complete. Running..."
   echo ""
   ./mandelbench_linux'
