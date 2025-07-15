#!/bin/bash
set -e

cd /home/womywomwoo/git/wos
B=$(pwd)/toolchain
TARGET_ARCH=x86_64-pc-wos

# Set up environment
export CC="$B/target1/bin/clang"
export CXX="$B/target1/bin/clang++"
export LD="$B/target1/bin/ld.lld"
export PATH=$B/target1/bin:$PATH
export LD_LIBRARY_PATH="$B/target1/lib"

# Prepare cross-file (always regenerate to ensure correct paths)
mkdir -p $B/../tools
cat > $B/../tools/x86_64-pc-wos-mlibc.txt << EOF
[binaries]
c = ['clang', '--target=x86_64-pc-wos', '--sysroot=$B/target1', '-nostdinc', '-isystem', '$B/target1/lib/clang/21/include', '-isystem', '$B/target1/include']
cpp = ['clang++', '--target=x86_64-pc-wos', '--sysroot=$B/target1', '-nostdinc++', '-isystem', '$B/target1/include/c++/v1', '-isystem', '$B/target1/lib/clang/21/include', '-isystem', '$B/target1/include']
ar = 'llvm-ar'
strip = 'llvm-strip'

[host_machine]
system = 'wos'
cpu_family = 'x86_64'
cpu = 'x86_64'
endian = 'little'
EOF

# Clear conflicting flags for mlibc build
unset CFLAGS CXXFLAGS LDFLAGS

mkdir -p $B/mlibc-build
cd $B/mlibc-build

meson setup --prefix=$B/target1 \
  --sysconfdir=etc \
  --default-library=static \
  --buildtype=release \
  --cross-file=$B/../tools/x86_64-pc-wos-mlibc.txt \
  -Dheaders_only=false \
  -Dwos_option=enabled \
  -Dglibc_option=enabled \
  $B/src/mlibc

echo "Meson setup complete. Starting build..."
ninja
