# #!/bin/bash
# # cmake  -S llvm-project/llvm \
# #        -B llvm-build \
# #        -GNinja \
# #        -DLLVM_USE_LINKER=lld \
# #        -DCMAKE_C_COMPILER=clang \
# #        -DCMAKE_CXX_COMPILER=clang++ \
# #        -DLLVM_TARGETS_TO_BUILD=X86 \
# #        -DLLVM_DEFAULT_TARGET_TRIPLE=x86_64-pc-wos-gnu \
# #        -DLLVM_ENABLE_PROJECTS="clang-tools-extra;clang;lld;compiler-rt;" \
# #        -DCMAKE_BUILD_TYPE=Release
# # cmake --build llvm-build

#     #    -DLLVM_DEFAULT_TARGET_TRIPLE=x86_64-pc-wos-gnu \

# cmake  -S llvm-project/llvm \
#        -GNinja \
#        -B libcxx-build \
#        -DCMAKE_C_COMPILER=clang \
#        -DCMAKE_CXX_COMPILER=clang++ \
#        -DCLANG_DEFAULT_LINKER=lld \
#        -DLLVM_ENABLE_PROJECTS="clang;lld;clang-tools-extra" \
#        -DLLVM_ENABLE_RUNTIMES="libc;compiler-rt" \
#        -DLLVM_LIBC_FULL_BUILD=ON \
#        -DLLVM_DEFAULT_TARGET_TRIPLE=x86_64-pc-wos-gnu \
#        -DLLVM_RUNTIME_TARGETS=x86_64-pc-wos-gnu \
#        -DLIBC_ENABLE_UNITTESTS=OFF \
#        -DLLVM_INCLUDE_TESTS=OFF \
#        -DCXX_SUPPORTS_FNO_EXCEPTIONS_FLAG=ON \
#        -DCXX_SUPPORTS_FUNWIND_TABLES_FLAG=ON \
#        -DCOMPILER_RT_BUILD_SCUDO_STANDALONE_WITH_LLVM_LIBC=ON \
#        -DLIBCXXABI_USE_LLVM_UNWINDER=OFF \
#        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
#        -DCMAKE_BUILD_TYPE=Release
#     #    -DLLVM_USE_LINKER=lld \
#     #    -DLLVM_TARGETS_TO_BUILD=X86 \
#     #    -DLLVM_ENABLE_PROJECTS="clang;clang-tools-extra;lld;compiler-rt;" \
#     #    -DLLVM_ENABLE_RUNTIMES="libc;libcxx;libcxxabi;libunwind" \
#     #    -DCMAKE_INSTALL_LIBDIR=lib \
#     #    -DCMAKE_C_FLAGS="-std=c23 -DLIBC_FULL_BUILD" \
#     #    -DCMAKE_CXX_FLAGS="-std=c++17 -DLIBC_FULL_BUILD" \
#     #    -DLIBC_ENABLE_UNITTESTS=OFF \
#     #    -DLLVM_INCLUDE_TESTS=OFF \
#     #    -DLIBC_FULL_BUILD=ON \
#     #    -DCXX_SUPPORTS_CUSTOM_LINKER=ON \
#     #    -DCMAKE_BUILD_TYPE=Release

if [ -d toolchain ]; then
    rm -fr toolchain
fi

mkdir -p toolchain

B=$(pwd)/toolchain

# Build LLVM

OLD_PATH=$PATH
HOSTCC=clang
export NINJA_STATUS="[%f/%t %e] "
TARGET_ARCH=x86_64-pc-wos-musl

mkdir -p $B/src
cd $B/src

git clone --depth=1 --branch master git://git.musl-libc.org/musl
git clone --depth=1 --branch=1_37_stable git://git.busybox.net/busybox.git
git clone --depth=1 --branch wos https://github.com/Pascu-Victor/llvm-project.git
# git clone --depth=1 https://github.com/sabotage-linux/kernel-headers.git
git clone --depth=1 https://github.com/libcxxrt/libcxxrt.git
git clone --depth=1 https://github.com/Pascu-Victor/libunwind.git
git clone --depth=1 https://git.savannah.gnu.org/git/autoconf.git

mkdir -p $B/autoconf-build
cd $B/src/autoconf
cd $B/autoconf-build
$B/src/autoconf/bootstrap
#  --prefix=$B/autoconf-build
make -j $(nproc) && make install

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
 -DCOMPILER_RT_BUILD_SANITIZERS=On \
 -DCOMPILER_RT_HAS_SAFESTACK=On \
 -DCOMPILER_RT_DEFAULT_TARGET_TRIPLE=$TARGET_ARCH \
 $B/src/llvm-project/llvm && ninja install

cd $B/stage1-prefix/bin; ln -s clang++ c++; ln -s clang cc; ln -s llvm-ar ar;

export CC="$B/stage1-build/bin/clang"
export CXX="$B/stage1-build/bin/clang++"
export LD=" $B/stage1-build/bin/ld.lld"
export PATH=$B/stage1-prefix/bin/:$OLD_PATH
export LD_LIBRARY_PATH="$B/stage1-prefix/lib/"
export CFLAGS="  --sysroot $B/target1 "
export CXXFLAGS="$CFLAGS"
export LDFLAGS=" --sysroot $B/target1 "

mkdir -p  $B/musl-build1
cd $B/musl-build1

mkdir -p $B/target1/lib/
ln -s . $B/target1/usr



touch empty.c
clang -O3 -c empty.c       -o $B/target1/lib/crtbegin.o
clang -O3 -c empty.c -fPIC -o $B/target1/lib/crtbeginS.o
clang -O3 -c empty.c       -o $B/target1/lib/crtend.o
clang -O3 -c empty.c -fPIC -o $B/target1/lib/crtendS.o


cd $B/musl-build1
LIBCC="$B/stage1-prefix/lib/clang/20/lib/x86_64-pc-wos-musl/libclang_rt.builtins.a" \
LDFLAGS="-lclang_rt.safestack" \
  $B/src/musl/configure --prefix=$B/target1 --syslibdir=$B/target1/lib --enable-safe-stack
make -j $(nproc) && make install
_linker=$( ls $B/target1/lib/ld-musl-* )
rm $_linker
ln -s libc.so $_linker
mkdir -p $B/target1/bin
cd $B/target1/bin; ln -s ../lib/libc.so ldd

mkdir -p  $B/libunwind-build
cd $B/libunwind-build
autoreconf -i $B/src/libunwind
sed -i 's/LIBCRTS="-lgcc_s"/LIBCRTS=""/'  $B/src/libunwind/configure

CFLAGS="$CFLAGS -fPIC" CXXFLAGS="$CXXFLAGS -fPIC" LDFLAGS="$LDFLAGS -fPIC" \
$B/src/libunwind/configure --prefix=$B/target1 --enable-cxx-exceptions \
  --disable-tests --host=$TARGET_ARCH
make -j $(nproc) && make install

mkdir -p  $B/libcxxrt-build
cd $B/libcxxrt-build
CXXFLAGS="$CXXFLAGS -nostdlib++ -lunwind -Wno-unused-command-line-argument" \
 cmake -G Ninja -DCMAKE_CXX_STANDARD=17 -DCMAKE_CXX_COMPILER_WORKS=ON -DCMAKE_C_COMPILER_WORKS=ON $B/src/libcxxrt && ninja
cp -a lib/libcxxrt.* $B/target1/lib

mkdir -p $B/libcxx
cd $B/libcxx
CXXFLAGS="$CXXFLAGS -v -D_LIBCPP_HAS_MUSL_LIBC -Wno-macro-redefined -nostdlib++ -D_GNU_SOURCE" \
LDFLAGS="-lclang_rt.safestack" \
cmake -G Ninja \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_CXX_STANDARD=23 \
 -DCMAKE_INSTALL_PREFIX=$B/target1 \
 -DCMAKE_CROSSCOMPILING=True \
 -DLLVM_TARGETS_TO_BUILD=X86 \
 -DLLVM_PARALLEL_LINK_JOBS=3 \
 -DLLVM_ENABLE_RUNTIMES='libcxx;libcxxabi;libunwind' \
 -DLLVM_ENABLE_LLD=On \
 -DLLVM_TABLEGEN=$B/stage1-prefix/bin/llvm-tblgen \
 -DLIBCXX_HAS_MUSL_LIBC=On \
 -DCMAKE_SYSROOT=$B/target1 \
 -DLIBCXX_CXX_ABI=libcxxrt  \
 -DLIBCXX_HAS_PTHREAD_API=On \
 -DLIBCXX_CXX_ABI_INCLUDE_PATHS="$B/src/libcxxrt/lib;$B/src/libcxxrt/src" \
 $B/src/llvm-project/runtimes && ninja install





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
