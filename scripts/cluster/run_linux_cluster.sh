#!/bin/bash
set -euo pipefail

WOS_ROOT="${WOS_WORKSPACE_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
cd "$WOS_ROOT"

NUM_VMS=2
NUM_VMS_SET=0
SKIP=0
DETACH=0
STOP=0
STATUS=0
CLUSTER_CONFIG=""
LIBVIRT_NETWORK=""

BASE_DOMAIN="${WOS_LINUX_BASE_DOMAIN:-ubuntu25.10}"
LIBVIRT_URI="${WOS_LINUX_LIBVIRT_URI:-qemu:///system}"
LIBVIRT_STORAGE_POOL="${WOS_LINUX_STORAGE_POOL:-default}"
LIBVIRT_OVERLAY_CAPACITY="${WOS_LINUX_OVERLAY_CAPACITY:-64G}"
LAN_BRIDGE="${WOS_LINUX_LAN_BRIDGE:-wos-lan-br}"
LAN_TAP_PREFIX="${WOS_LINUX_LAN_TAP_PREFIX:-wlu-lan-N}"
TAP_OWNER="${WOS_LINUX_TAP_OWNER:-libvirt-qemu}"
DOMAIN_PREFIX="${WOS_LINUX_DOMAIN_PREFIX:-wos-ubuntu-vm}"
STORAGE_DIR="${WOS_LINUX_STORAGE_DIR:-/var/lib/libvirt/images/wos-linux-cluster}"
NVRAM_DIR="${WOS_LINUX_NVRAM_DIR:-/var/lib/libvirt/qemu/nvram}"
OVMF_VARS_TEMPLATE="${WOS_LINUX_OVMF_VARS_TEMPLATE:-/usr/share/edk2/x64/OVMF_VARS.4m.fd}"
STATE_DIR="${WOS_LINUX_STATE_DIR:-cluster-data/linux-cluster-state}"
DOMAIN_FILE="${STATE_DIR}/domains.tsv"
RESOURCE_HELPER="${WOS_ROOT}/scripts/cluster/linux_cluster_config.py"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --skip requires a node index." >&2
        exit 2
      fi
      SKIP="$2"
      shift 2
      ;;
    --cluster-config)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --cluster-config requires a path." >&2
        exit 2
      fi
      CLUSTER_CONFIG="$2"
      shift 2
      ;;
    --libvirt-network)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --libvirt-network requires a network name." >&2
        exit 2
      fi
      LIBVIRT_NETWORK="$2"
      shift 2
      ;;
    --detach)
      DETACH=1
      shift
      ;;
    --stop)
      STOP=1
      shift
      ;;
    --status)
      STATUS=1
      shift
      ;;
    --*)
      echo "ERROR: unknown option '$1'." >&2
      exit 2
      ;;
    *)
      if [[ "$NUM_VMS_SET" == "1" ]]; then
        echo "ERROR: multiple VM counts were provided." >&2
        exit 2
      fi
      NUM_VMS="$1"
      NUM_VMS_SET=1
      shift
      ;;
  esac
done

run_virsh() {
  if virsh -c "$LIBVIRT_URI" uri >/dev/null 2>&1; then
    virsh -c "$LIBVIRT_URI" "$@"
  else
    sudo virsh -c "$LIBVIRT_URI" "$@"
  fi
}

domain_exists() {
  run_virsh dominfo "$1" >/dev/null 2>&1
}

base_disk_path() {
  run_virsh domblklist "$BASE_DOMAIN" | awk '$1 == "vda" { print $2; exit }'
}

base_mac_address() {
  run_virsh domiflist "$BASE_DOMAIN" | awk 'NF >= 5 && $5 ~ /^([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}$/ { print $5; exit }'
}

bridge_mtu() {
  cat "/sys/class/net/${LAN_BRIDGE}/mtu"
}

cleanup_tap() {
  local tap_name="$1"

  if [[ -n "$tap_name" ]] && ip link show "$tap_name" &>/dev/null; then
    sudo ip link set "$tap_name" down >/dev/null 2>&1 || true
    sudo ip tuntap del dev "$tap_name" mode tap >/dev/null 2>&1 || true
  fi
}

ensure_tap() {
  local tap_name="$1"
  local mtu="$2"

  cleanup_tap "$tap_name"
  sudo ip tuntap add dev "$tap_name" mode tap user "$TAP_OWNER"
  sudo ip link set "$tap_name" master "$LAN_BRIDGE"
  sudo ip link set "$tap_name" mtu "$mtu"
  sudo ip link set "$tap_name" up
}

patch_guest_identity() {
  local disk_path="$1"
  local hostname="$2"
  local guest_mac="$3"
  local fqdn="${hostname}.wos"
  local temp_dir
  temp_dir="$(mktemp -d)"
  trap 'rm -rf "$temp_dir"' RETURN

  cat > "${temp_dir}/00-installer-config.yaml" <<EOF
network:
  ethernets:
    enp1s0:
      dhcp4: true
      dhcp6: true
      dhcp-identifier: mac
      match:
        macaddress: ${guest_mac}
      set-name: enp1s0
  version: 2
EOF

  printf '%s\n' "$fqdn" > "${temp_dir}/hostname"
  cat > "${temp_dir}/hosts" <<EOF
127.0.0.1 localhost
127.0.1.1 localhost

# The following lines are desirable for IPv6 capable hosts
::1 ip6-localhost ip6-loopback
fe00::0 ip6-localnet
ff00::0 ip6-mcastprefix
ff02::1 ip6-allnodes
ff02::2 ip6-allrouters
EOF
  : > "${temp_dir}/machine-id"

  sudo env LIBGUESTFS_BACKEND=direct virt-customize \
    -a "$disk_path" \
    --upload "${temp_dir}/00-installer-config.yaml:/etc/netplan/00-installer-config.yaml" \
    --upload "${temp_dir}/hostname:/etc/hostname" \
    --upload "${temp_dir}/hosts:/etc/hosts" \
    --upload "${temp_dir}/machine-id:/etc/machine-id" \
    --run-command 'mkdir -p /var/lib/dbus && : > /var/lib/dbus/machine-id' >/dev/null
}

cleanup_domain() {
  local domain_name="$1"
  local disk_path="$2"
  local nvram_path="$3"
  local tap_name="${4:-}"
  local storage_mode="${5:-file}"
  local storage_pool="${6:-}"
  local storage_volume="${7:-}"

  [[ "$tap_name" == "-" ]] && tap_name=""
  [[ "$storage_pool" == "-" ]] && storage_pool=""
  [[ "$storage_volume" == "-" ]] && storage_volume=""

  if domain_exists "$domain_name"; then
    local state
    state="$(run_virsh domstate "$domain_name" 2>/dev/null | tr -d '\r')"
    if [[ "$state" == "running" || "$state" == "in shutdown" || "$state" == "paused" || "$state" == "idle" ]]; then
      run_virsh destroy "$domain_name" >/dev/null 2>&1 || true
    fi
    run_virsh undefine "$domain_name" --nvram >/dev/null 2>&1 || run_virsh undefine "$domain_name" >/dev/null 2>&1 || true
  fi

  if [[ "$storage_mode" == "volume" ]]; then
    if [[ -n "$storage_pool" && -n "$storage_volume" ]]; then
      run_virsh vol-delete "$storage_volume" --pool "$storage_pool" >/dev/null 2>&1 || true
    fi
  else
    sudo rm -f "$disk_path" "$nvram_path"
  fi
  cleanup_tap "$tap_name"
}

stop_cluster() {
  if [[ ! -f "$DOMAIN_FILE" ]]; then
    echo "No Linux cluster state file found at '$DOMAIN_FILE'."
    return 0
  fi

  while IFS=$'\t' read -r domain_name disk_path nvram_path mac_addr tap_name storage_mode storage_pool storage_volume network_name; do
    [[ -z "$domain_name" ]] && continue
    cleanup_domain \
      "$domain_name" "$disk_path" "$nvram_path" "$tap_name" \
      "$storage_mode" "$storage_pool" "$storage_volume"
  done < "$DOMAIN_FILE"

  rm -f "$DOMAIN_FILE"
  echo "Linux cluster stopped."
}

show_status() {
  if [[ ! -f "$DOMAIN_FILE" ]]; then
    echo "Linux cluster is not running (no state file at '$DOMAIN_FILE')."
    return 0
  fi

  local any_running=0
  echo "Linux cluster state file: $DOMAIN_FILE"
  while IFS=$'\t' read -r domain_name disk_path nvram_path mac_addr tap_name storage_mode storage_pool storage_volume network_name; do
    [[ -z "$domain_name" ]] && continue
    local attachment="$tap_name"
    if [[ -z "$attachment" || "$attachment" == "-" ]]; then
      attachment="${network_name:-unknown-network}"
    fi
    if domain_exists "$domain_name"; then
      local state
      state="$(run_virsh domstate "$domain_name" | tr -d '\r')"
      echo "  ${domain_name}: ${state} (${mac_addr}, ${attachment})"
      if [[ "$state" == "running" || "$state" == "idle" || "$state" == "paused" || "$state" == "in shutdown" ]]; then
        any_running=1
      fi
    else
      echo "  ${domain_name}: undefined (${mac_addr}, ${attachment})"
    fi
  done < "$DOMAIN_FILE"

  if [[ "$any_running" == "0" ]]; then
    return 1
  fi
}

if [[ "$STOP" == "1" ]]; then
  stop_cluster
  exit 0
fi

if [[ "$STATUS" == "1" ]]; then
  show_status
  exit $?
fi

if [[ ! "$NUM_VMS" =~ ^[0-9]+$ || "$NUM_VMS" -le 0 ]]; then
  echo "ERROR: VM count must be a positive integer." >&2
  exit 2
fi
if [[ ! "$SKIP" =~ ^[0-9]+$ || "$SKIP" -lt 0 || "$SKIP" -ge "$NUM_VMS" ]]; then
  echo "ERROR: --skip must be an integer from 0 through VM count minus one." >&2
  exit 2
fi
if [[ -n "$LIBVIRT_NETWORK" && "$((NUM_VMS - SKIP))" -ne 1 ]]; then
  echo "ERROR: --libvirt-network currently supports exactly one launched VM." >&2
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required on the host." >&2
  exit 1
fi

declare -a NODE_VCPUS=()
declare -a NODE_MEMORY_KIB=()
if [[ -n "$CLUSTER_CONFIG" ]]; then
  RESOURCE_ROWS="$(
    python3 "$RESOURCE_HELPER" resources \
      --config "$CLUSTER_CONFIG" \
      --num-vms "$NUM_VMS"
  )"
  while IFS=$'\t' read -r node_id node_vcpus node_memory_kib; do
    [[ -z "$node_id" ]] && continue
    NODE_VCPUS[node_id]="$node_vcpus"
    NODE_MEMORY_KIB[node_id]="$node_memory_kib"
  done <<< "$RESOURCE_ROWS"

  for ((i = 0; i < NUM_VMS; i++)); do
    if [[ ! -v "NODE_VCPUS[$i]" || ! -v "NODE_MEMORY_KIB[$i]" ]]; then
      echo "ERROR: cluster config did not resolve resources for node ${i}." >&2
      exit 2
    fi
  done
fi

mkdir -p "$STATE_DIR"

if [[ -z "$LIBVIRT_NETWORK" ]] && ! command -v qemu-img >/dev/null 2>&1; then
  echo "ERROR: qemu-img is required on the host." >&2
  exit 1
fi

if ! command -v virsh >/dev/null 2>&1; then
  echo "ERROR: virsh is required on the host." >&2
  exit 1
fi

if [[ -n "$LIBVIRT_NETWORK" ]]; then
  if ! run_virsh net-info "$LIBVIRT_NETWORK" >/dev/null 2>&1; then
    echo "ERROR: libvirt network '$LIBVIRT_NETWORK' was not found." >&2
    exit 1
  fi
  if ! run_virsh pool-info "$LIBVIRT_STORAGE_POOL" >/dev/null 2>&1; then
    echo "ERROR: libvirt storage pool '$LIBVIRT_STORAGE_POOL' was not found." >&2
    exit 1
  fi
else
  if ! ip link show "$LAN_BRIDGE" &>/dev/null; then
    echo "ERROR: $LAN_BRIDGE bridge not found. Run wos-cluster --setup or equivalent first." >&2
    exit 1
  fi
  if [[ ! -f "$OVMF_VARS_TEMPLATE" ]]; then
    echo "ERROR: OVMF vars template '$OVMF_VARS_TEMPLATE' not found" >&2
    exit 1
  fi
fi

if ! domain_exists "$BASE_DOMAIN"; then
  echo "ERROR: base libvirt domain '$BASE_DOMAIN' not found." >&2
  exit 1
fi

BASE_DISK="$(base_disk_path)"
if [[ -z "$BASE_DISK" ]]; then
  echo "ERROR: could not determine the base disk for '$BASE_DOMAIN'." >&2
  exit 1
fi
BASE_MAC_ADDRESS="$(base_mac_address)"
if [[ -n "$LIBVIRT_NETWORK" && -z "$BASE_MAC_ADDRESS" ]]; then
  echo "ERROR: could not determine the base domain MAC address." >&2
  exit 1
fi

if [[ -z "$LIBVIRT_NETWORK" ]]; then
  sudo mkdir -p "$STORAGE_DIR" "$NVRAM_DIR"
  if ! getent passwd "$TAP_OWNER" >/dev/null 2>&1; then
    echo "ERROR: TAP owner '$TAP_OWNER' not found on this host." >&2
    exit 1
  fi
fi

if [[ -f "$DOMAIN_FILE" ]]; then
  stop_cluster >/dev/null 2>&1 || true
fi

declare -a CREATED_DOMAINS=()
declare -a CREATED_TAPS=()
TMP_XML="$(mktemp)"
CURRENT_DOMAIN_XML=""
CURRENT_DEFINED_XML=""
run_virsh dumpxml "$BASE_DOMAIN" > "$TMP_XML"
LAN_MTU=""
if [[ -z "$LIBVIRT_NETWORK" ]]; then
  LAN_MTU="$(bridge_mtu)"
fi

cleanup() {
  echo
  echo "=== Shutting down Linux cluster ==="
  for domain_name in "${CREATED_DOMAINS[@]}"; do
    run_virsh destroy "$domain_name" >/dev/null 2>&1 || true
  done
  for tap_name in "${CREATED_TAPS[@]}"; do
    cleanup_tap "$tap_name"
  done
  stop_cluster >/dev/null 2>&1 || true
  rm -f "$TMP_XML" "$CURRENT_DOMAIN_XML" "$CURRENT_DEFINED_XML"
}
trap cleanup EXIT INT TERM

for ((i = SKIP; i < NUM_VMS; i++)); do
  domain_name="${DOMAIN_PREFIX}${i}"
  storage_mode="file"
  storage_pool=""
  storage_volume=""
  disk_path="${STORAGE_DIR}/${domain_name}.qcow2"
  nvram_path="${NVRAM_DIR}/${domain_name}_VARS.fd"
  tap_name="${LAN_TAP_PREFIX}${i}"
  mac_addr="52:54:00:22:34:$(printf '%02x' $((0x80 + i)))"
  CURRENT_DOMAIN_XML="$(mktemp)"
  domain_xml="$CURRENT_DOMAIN_XML"

  echo "--- Cloning Ubuntu VM${i} from libvirt domain '${BASE_DOMAIN}' ---"
  if [[ -n "$LIBVIRT_NETWORK" ]]; then
    storage_mode="volume"
    storage_pool="$LIBVIRT_STORAGE_POOL"
    storage_volume="${domain_name}.qcow2"
    tap_name=""
    mac_addr="$BASE_MAC_ADDRESS"
    cleanup_domain \
      "$domain_name" "$disk_path" "$nvram_path" "$tap_name" \
      "$storage_mode" "$storage_pool" "$storage_volume"
    run_virsh vol-create-as \
      "$storage_pool" "$storage_volume" "$LIBVIRT_OVERLAY_CAPACITY" \
      --allocation 0 --format qcow2 \
      --backing-vol "$BASE_DISK" --backing-vol-format qcow2 >/dev/null
    disk_path="$(run_virsh vol-path "$storage_volume" --pool "$storage_pool")"
  else
    cleanup_domain "$domain_name" "$disk_path" "$nvram_path" "$tap_name"
    ensure_tap "$tap_name" "$LAN_MTU"
    sudo qemu-img create -f qcow2 -F qcow2 -b "$BASE_DISK" "$disk_path" >/dev/null
    sudo cp "$OVMF_VARS_TEMPLATE" "$nvram_path"
    patch_guest_identity "$disk_path" "$domain_name" "$mac_addr"
  fi
  state_tap_name="${tap_name:--}"
  state_storage_pool="${storage_pool:--}"
  state_storage_volume="${storage_volume:--}"
  state_network_name="${LIBVIRT_NETWORK:--}"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$domain_name" "$disk_path" "$nvram_path" "$mac_addr" "$state_tap_name" \
    "$storage_mode" "$state_storage_pool" "$state_storage_volume" "$state_network_name" >> "$DOMAIN_FILE"

  cp "$TMP_XML" "$domain_xml"
  patch_args=(
    python3 "$RESOURCE_HELPER" patch-domain "$domain_xml"
    --domain-name "$domain_name"
    --base-disk "$BASE_DISK"
    --disk-path "$disk_path"
    --nvram-path "$nvram_path"
    --mac-address "$mac_addr"
  )
  if [[ -n "$LIBVIRT_NETWORK" ]]; then
    patch_args+=(--network-name "$LIBVIRT_NETWORK")
  else
    patch_args+=(--tap-name "$tap_name")
  fi
  if [[ -n "$CLUSTER_CONFIG" ]]; then
    patch_args+=(
      --cpus "${NODE_VCPUS[i]}"
      --memory-kib "${NODE_MEMORY_KIB[i]}"
    )
    echo "    resources: ${NODE_VCPUS[i]} vCPU, ${NODE_MEMORY_KIB[i]} KiB"
  fi
  "${patch_args[@]}"

  run_virsh define "$domain_xml" >/dev/null
  if [[ -n "$CLUSTER_CONFIG" ]]; then
    CURRENT_DEFINED_XML="$(mktemp)"
    run_virsh dumpxml --inactive "$domain_name" > "$CURRENT_DEFINED_XML"
    verify_args=(
      python3 "$RESOURCE_HELPER" verify-domain "$CURRENT_DEFINED_XML"
      --domain-name "$domain_name" \
      --base-disk "$BASE_DISK" \
      --disk-path "$disk_path" \
      --nvram-path "$nvram_path" \
      --mac-address "$mac_addr" \
      --cpus "${NODE_VCPUS[i]}" \
      --memory-kib "${NODE_MEMORY_KIB[i]}"
    )
    if [[ -n "$LIBVIRT_NETWORK" ]]; then
      verify_args+=(--network-name "$LIBVIRT_NETWORK")
    else
      verify_args+=(--tap-name "$tap_name")
    fi
    "${verify_args[@]}"
    rm -f "$CURRENT_DEFINED_XML"
    CURRENT_DEFINED_XML=""
  fi
  run_virsh start "$domain_name" >/dev/null

  CREATED_DOMAINS+=("$domain_name")
  if [[ -n "$tap_name" ]]; then
    CREATED_TAPS+=("$tap_name")
  fi
  rm -f "$domain_xml"
  CURRENT_DOMAIN_XML=""
done

rm -f "$TMP_XML"

echo "=== Linux cluster launched: $((NUM_VMS - SKIP)) VM(s) ==="
echo "State file: $DOMAIN_FILE"
if [[ -n "$LIBVIRT_NETWORK" ]]; then
  echo "Libvirt network: $LIBVIRT_NETWORK"
else
  echo "Bridge: $LAN_BRIDGE"
fi

if [[ "$DETACH" == "1" ]]; then
  trap - EXIT INT TERM
  echo "Launcher exiting in detached mode; libvirt domains remain running."
  echo "Stop them with: wos-run-linux-cluster --stop"
  exit 0
fi

echo "Press Ctrl+C to stop all VMs."

while true; do
  sleep 60
done
