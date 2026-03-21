#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y \
  build-essential \
  clang \
  cmake \
  g++ \
  git \
  jq \
  ninja-build \
  openmpi-bin \
  libopenmpi-dev \
  python3 \
  python3-pip \
  sshpass

mkdir -p /var/lib/wos-bench/results
chmod 777 /var/lib/wos-bench/results

echo "Ubuntu benchmark guest provisioning complete."
