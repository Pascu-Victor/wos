#!/bin/bash
set -euo pipefail

DEFAULT_BASE_IMAGE="${WOS_LINUX_BASE_IMAGE:-cluster-data/ubuntu25.10.qcow2}"

if [[ $# -gt 3 ]]; then
  echo "Usage: $0 [ubuntu-base-image] [num-vms] [overlay-dir]" >&2
  exit 1
fi

BASE_IMAGE="$DEFAULT_BASE_IMAGE"
NUM_VMS="2"
OVERLAY_DIR="linux-cluster-overlays"

if [[ $# -ge 1 ]]; then
  if [[ -f "$1" ]]; then
    BASE_IMAGE="$1"
    shift
  fi
fi

if [[ $# -ge 1 ]]; then
  NUM_VMS="$1"
  shift
fi

if [[ $# -ge 1 ]]; then
  OVERLAY_DIR="$1"
fi

DATA_DISK_SIZE="${WOS_LINUX_DATA_DISK_SIZE:-1G}"

if [[ ! -f "$BASE_IMAGE" ]]; then
  echo "ERROR: base image '$BASE_IMAGE' not found" >&2
  exit 1
fi

mkdir -p "$OVERLAY_DIR"

for ((i = 0; i < NUM_VMS; i++)); do
  ROOT_DISK="${OVERLAY_DIR}/ubuntu-root-vm${i}.qcow2"
  DATA_DISK="${OVERLAY_DIR}/ubuntu-data-vm${i}.qcow2"

  rm -f "$ROOT_DISK" "$DATA_DISK"

  echo "Creating Ubuntu root overlay for VM${i}: ${ROOT_DISK}"
  qemu-img create -f qcow2 -b "$(realpath "$BASE_IMAGE")" -F qcow2 "$ROOT_DISK" >/dev/null

  echo "Creating Ubuntu data disk for VM${i}: ${DATA_DISK} (${DATA_DISK_SIZE})"
  qemu-img create -f qcow2 "$DATA_DISK" "$DATA_DISK_SIZE" >/dev/null

done

echo "Ubuntu cluster overlays created in ${OVERLAY_DIR}"
