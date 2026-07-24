#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y \
  bison \
  build-essential \
  clang \
  cmake \
  flex \
  g++ \
  git \
  jq \
  lld \
  meson \
  nasm \
  ninja-build \
  openmpi-bin \
  libopenmpi-dev \
  pkg-config \
  python3 \
  python3-pip \
  sshpass \
  unzip \
  yasm

if command -v growpart >/dev/null 2>&1 &&
   [ -b /dev/vda ] &&
   [ -b /dev/vda3 ] &&
   pvs --noheadings -o pv_name 2>/dev/null | grep -q '/dev/vda3'; then
  growpart /dev/vda 3 || true
  pvresize /dev/vda3
  if vgs --noheadings -o vg_free_count ubuntu-vg | awk '$1 + 0 > 0 { found = 1 } END { exit !found }'; then
    lvextend --resizefs --extents +100%FREE /dev/ubuntu-vg/ubuntu-lv
  fi
fi

mkdir -p /var/lib/wos-bench/results
chmod 777 /var/lib/wos-bench/results

echo "Ubuntu benchmark guest provisioning complete."
