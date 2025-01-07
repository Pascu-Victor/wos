exit # Dont run this as a script

# LLVM seems to work great on Musl, there only seems some build errors
# when trying to build the sanitizers.  So sanitizers have been disabled.
# This is rougly based on this: 
# https://wiki.musl-libc.org/building-llvm.html
# But updated for LLVM 8

# We are also focusing on making a GNU-less toolchain, as there is
# no libgcc and no libstdc++, instead we favor LLVM's own compiler-rt
# and libc++, also I suspect that this is very close to what you would
# want to do if you were crosscompiling for another architecture.
# have not tested cross compile yet

# It should be noted, that this is most likely not the most efficient 
# way to get an LLVM toolchain on Musl, your best bet it to grab a 
# binary distribution, possibly from Apline Linux.  This is mostly 
# just to serve as documentation on how to boostrap LLVM from a 
# non-musl system.

# I did this from a Ubuntu chroot, but this should work on 
# pretty much any system.  Along with the common system 
# utilities, you need ninja, cmake and a c++ compiler, on a 
# barebones debian-like system you may want:

    sudo apt install -y git wget cmake unzip g++

    wget https://github.com/ninja-build/ninja/releases/download/v1.9.0/ninja-linux.zip
    unzip ninja-linux.zip
    chmod +x ninja
    sudo mv ninja /usr/local/bin

# Set B to where you want to put all the files, This will probably 
# consume at-least 24GB of disk space:

    B=/tmp/

# Do all the things:

OLD_PATH=$PATH
HOSTCC=$(which cc)
export NINJA_STATUS="[%f/%t %e] "
TARGET_ARCH=x86_64-pc-linux-musl

mkdir -p $B/src
cd $B/src

git clone --depth=1 --branch v1.1.21 git://git.musl-libc.org/musl
git clone --depth=1 --branch=1_29_stable git://git.busybox.net/busybox.git
git clone --depth=1 --branch release/8.x https://github.com/llvm/llvm-project.git
git clone --depth=1 https://github.com/sabotage-linux/kernel-headers.git
git clone --depth=1 https://github.com/pathscale/libcxxrt.git
git clone --depth=1 https://github.com/ninja-build/ninja.git


wget http://mirror.easyname.at/nongnu/libunwind/libunwind-1.3.1.tar.gz
tar xvf libunwind-1.3.1.tar.gz
mv libunwind-1.3.1 libunwind

wget https://ftp.gnu.org/gnu/make/make-4.2.1.tar.bz2
tar xvf make-4.2.1.tar.bz2
mv make-4.2.1 make

wget https://github.com/Kitware/CMake/releases/download/v3.13.4/cmake-3.13.4.tar.Z
tar xvf cmake-3.13.4.tar.Z
mv cmake-3.13.4 cmake

wget https://www.python.org/ftp/python/3.7.2/Python-3.7.2.tar.xz
tar xvf Python-3.7.2.tar.xz
mv Python-3.7.2 Python


mkdir -p $B/stage1-build
cd $B/stage1-build
cmake -G Ninja \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_INSTALL_PREFIX=$B/stage1-prefix \
 -DLLVM_TARGETS_TO_BUILD=X86 \
 -DLLVM_ENABLE_PROJECTS='clang;compiler-rt;lld' \
 -DLLVM_PARALLEL_LINK_JOBS=3 \
 -DLLVM_ENABLE_LIBXML2=Off \
 -DLLVM_ENABLE_LLD=On \
 -DLLVM_DEFAULT_TARGET_TRIPLE=$TARGET_ARCH \
 -DCLANG_DEFAULT_LINKER=lld \
 -DCLANG_DEFAULT_CXX_STDLIB=libc++ \
 -DCLANG_DEFAULT_RTLIB=compiler-rt \
 -DCOMPILER_RT_DEFAULT_TARGET_TRIPLE=$TARGET_ARCH \
 $B/src/llvm-project/llvm && ninja install

cd $B/stage1-prefix/bin; ln -s clang++ c++; ln -s clang cc; ln -s llvm-ar ar;

export CC="clang"
export CXX="clang++"
export LD="ld.lld"
export PATH=$B/stage1-prefix/bin/:$OLD_PATH
export LD_LIBRARY_PATH="$B/stage1-prefix/lib/"
export CFLAGS="  --sysroot $B/target1 "
export CXXFLAGS="$CFLAGS"
export LDFLAGS=" --sysroot $B/target1 "

cd $B/src/kernel-headers
make ARCH=x86_64 prefix=$B/target1 install

mkdir -p  $B/musl-build1
cd $B/musl-build1

mkdir -p $B/target1/lib/
ln -s . $B/target1/usr

touch empty.c
clang -Ofast -c empty.c       -o $B/target1/lib/crtbegin.o
clang -Ofast -c empty.c -fPIC -o $B/target1/lib/crtbeginS.o
clang -Ofast -c empty.c       -o $B/target1/lib/crtend.o
clang -Ofast -c empty.c -fPIC -o $B/target1/lib/crtendS.o

cd $B/musl-build1
LIBCC="$B/stage1-prefix//lib/clang/8.0.0/lib/linux/libclang_rt.builtins-x86_64.a" \
  $B/src/musl/configure --prefix=$B/target1 --syslibdir=$B/target1/lib
make -j $(nproc) && make install
_linker=$( ls $B/target1/lib/ld-musl-* )
rm $_linker
ln -s libc.so $_linker
cd $B/target1/bin; ln -s ../lib/libc.so ldd

sed -i 's/LIBCRTS="-lgcc_s"/LIBCRTS=""/'  $B/src/libunwind/configure
mkdir -p  $B/libunwind-build
cd $B/libunwind-build
$B/src/libunwind/configure --prefix=$B/target1 --enable-cxx-exceptions \
  --disable-tests --host=$TARGET_ARCH
make -j $(nproc) && make install

mkdir -p  $B/libcxxrt-build
cd $B/libcxxrt-build
CXXFLAGS=" -nostdlib++ -lunwind -Wno-unused-command-line-argument " \
 cmake -G Ninja $B/src/libcxxrt && ninja
cp -a lib/libcxxrt.* $B/target1/lib

mkdir -p $B/libcxx
cd $B/libcxx
CXXFLAGS="$CXXFLAGS -D_LIBCPP_HAS_MUSL_LIBC -Wno-macro-redefined -nostdlib++" \
cmake -G Ninja \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_INSTALL_PREFIX=$B/target1 \
 -DCMAKE_CROSSCOMPILING=True \
 -DLLVM_TARGETS_TO_BUILD=X86 \
 -DLLVM_PARALLEL_LINK_JOBS=3 \
 -DLLVM_ENABLE_LLD=On \
 -DLLVM_TABLEGEN=$B/stage1-prefix/bin/llvm-tblgen \
 -DLIBCXX_HAS_MUSL_LIBC=On \
 -DLIBCXX_SYSROOT=$B/target1 \
 -DLIBCXX_CXX_ABI=libcxxrt  \
 -DLIBCXX_CXX_ABI_INCLUDE_PATHS="$B/src/libcxxrt/lib;$B/src/libcxxrt/src" \
 $B/src/llvm-project/libcxx && ninja install

mkdir -p $B/stage2-build
cd $B/stage2-build
cmake -G Ninja \
 -DCMAKE_SYSROOT=$B/target1 \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_INSTALL_PREFIX=$B/target1 \
 -DCMAKE_CROSSCOMPILING=True \
 -DLLVM_DEFAULT_TARGET_TRIPLE=$TARGET_ARCH \
 -DLLVM_ENABLE_PROJECTS='clang;libcxx;libcxxabi;libunwind;compiler-rt;lld' \
 -DLLVM_TARGETS_TO_BUILD=X86 \
 -DLLVM_PARALLEL_LINK_JOBS=3 \
 -DLLVM_ENABLE_LLD=On \
 -DLLVM_ENABLE_LIBXML2=Off \
 -DLLVM_TABLEGEN=$B/stage1-prefix/bin/llvm-tblgen \
 -DCLANG_TABLEGEN=$B/stage1-build/bin/clang-tblgen \
 -DCLANG_DEFAULT_LINKER=lld \
 -DCLANG_DEFAULT_CXX_STDLIB=libc++ \
 -DCLANG_DEFAULT_RTLIB=compiler-rt \
 -DLIBCXX_HAS_MUSL_LIBC=On \
 -DLIBCXX_SYSROOT=$B/target1 \
 -DLIBCXXABI_USE_COMPILER_RT=YES \
 -DLIBCXXABI_USE_LLVM_UNWINDER=1 \
 -DLIBCXXABI_ENABLE_STATIC_UNWINDER=On \
 -DCOMPILER_RT_BUILD_SANITIZERS=Off \
 -DCOMPILER_RT_BUILD_XRAY=Off \
 -DCOMPILER_RT_BUILD_LIBFUZZER=Off \
 $B/src/llvm-project/llvm &&
ninja install-unwind &&
ninja install-cxxabi &&
ninja install-cxx   &&
ninja install

mkdir -p $B/busybox-build
cd $B/src/busybox; make O=$B/busybox-build defconfig
# https://wiki.musl-libc.org/building-busybox.html
cd $B/busybox-build
make -j $(nproc) CC="$CC -Ofast -Wno-ignored-optimization-argument -Wno-unused-command-line-argument" 
cp busybox $B/target1/bin

# We cant simply use busybox --list or --install because can't assume
#  that the host can execute target binaries...
cat <<EOF > $B/busybox-build/print_applets.c
#define SKIP_applet_main
#define ALIGN1
#define ALIGN2
#include <stdio.h>
#include <stdint.h>
#include "include/applet_tables.h"
int main() {
  for(const char*s=applet_names;*s||*(s+1);s++) { 
    putchar(*s?*s:'\n'); } }
EOF
$HOSTCC $B/busybox-build/print_applets.c -o $B/busybox-build/print_applets.exec

cd $B/target1/bin
for i in $($B/busybox-build/print_applets.exec ); do ln -sf ./busybox $i; done
ln -s clang++ c++; ln -s clang cc; ln -s llvm-ar ar; ln -s ld.lld ld

mkdir -p $B/make-build
cd $B/make-build
$B/src/make/configure --prefix=$B/target1 --host=$TARGET_ARCH
make -j $(nproc) && make install


mkdir -p $B/target1/{src,proc,dev,etc,tmp} 

# if you were cross compiling, then you would need to get this
# onto the target arch, but we just setup a chroot

cp /etc/resolv.conf $B/target1/etc/resolv.conf
sudo mount -o bind $B/src/  $B/target1/src/
sudo mount -t proc none     $B/target1/proc/
sudo mount -t devtmpfs none $B/target1/dev/
sudo chroot $B/target1 /bin/sh

TARGET_ARCH=x86_64-pc-linux-musl
export NINJA_STATUS="[%f/%t %e] "

mkdir -p /build/cmake
cd /build/cmake
/src/cmake/bootstrap --parallel=$(nproc) --prefix=/
make -j $(nproc) && make install

mkdir -p /build/Python
cd /build/Python
/src/Python/configure --prefix=/
# TODO consider doing /configure --enable-optimizations after bootstrapping...
make -j $(nproc) 
make install || true # python install will error here due to missing zlib, but it did actually install
cd /bin; ln -s python3 python

mkdir -p /build/ninja
cd /build/ninja
CXX=clang++ CXXFLAGS=" -D_POSIX_C_SOURCE=200809L " /src/ninja/configure.py --bootstrap
mv ninja /bin/


mkdir -p /build/stage3
cd /build/stage3
cmake -G Ninja \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_INSTALL_PREFIX=/ \
 -DLLVM_DEFAULT_TARGET_TRIPLE=$TARGET_ARCH \
 -DLLVM_ENABLE_PROJECTS='clang;libcxx;libcxxabi;libunwind;compiler-rt;lld' \
 -DLLVM_TARGETS_TO_BUILD=X86 \
 -DLLVM_PARALLEL_LINK_JOBS=3 \
 -DLLVM_ENABLE_LLD=On \
 -DLLVM_ENABLE_LIBXML2=Off \
 -DCLANG_DEFAULT_LINKER=lld \
 -DCLANG_DEFAULT_CXX_STDLIB=libc++ \
 -DCLANG_DEFAULT_RTLIB=compiler-rt \
 -DLIBCXX_HAS_MUSL_LIBC=On \
 -DLIBCXXABI_USE_COMPILER_RT=YES \
 -DLIBCXXABI_USE_LLVM_UNWINDER=1 \
 -DLIBCXXABI_ENABLE_STATIC_UNWINDER=On \
 -DCOMPILER_RT_BUILD_SANITIZERS=Off \
 -DCOMPILER_RT_BUILD_XRAY=Off \
 -DCOMPILER_RT_BUILD_LIBFUZZER=Off \
 /src/llvm-project/llvm &&
ninja install-unwind &&
ninja install-cxxabi &&
ninja install-cxx   &&
ninja install


