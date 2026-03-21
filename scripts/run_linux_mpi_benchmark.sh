#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROUTER_IP="${WOS_ROUTER_IP:-10.10.0.1}"
HOST_IP="${WOS_HOST_IP:-10.10.0.100}"
REMOTE_USER="${WOS_LINUX_USER:-user}"
REMOTE_PASS="${WOS_LINUX_PASSWORD:-1234}"
LAUNCHER=""
HOSTS_CSV=""
BENCHMARK="net"
REMOTE_DIR="/home/${REMOTE_USER}/.local/lib/wos-bench/bin"
REMOTE_AGENT="${REMOTE_DIR}/plm_ssh_agent.sh"
MODE="pingpong"
PAYLOAD_SIZE="1024"
ITERATIONS="10000"
TOTAL_BYTES="268435456"
FILE_PATH=""
CHUNK_SIZE="65536"
GENERATE_FILE_BYTES="67108864"
NP=""
OUTPUT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --launcher)
      LAUNCHER="$2"
      shift 2
      ;;
    --hosts)
      HOSTS_CSV="$2"
      shift 2
      ;;
    --benchmark)
      BENCHMARK="$2"
      shift 2
      ;;
    --remote-dir)
      REMOTE_DIR="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --payload-size)
      PAYLOAD_SIZE="$2"
      shift 2
      ;;
    --iterations)
      ITERATIONS="$2"
      shift 2
      ;;
    --total-bytes)
      TOTAL_BYTES="$2"
      shift 2
      ;;
    --file-path)
      FILE_PATH="$2"
      shift 2
      ;;
    --chunk-size)
      CHUNK_SIZE="$2"
      shift 2
      ;;
    --generate-file-bytes)
      GENERATE_FILE_BYTES="$2"
      shift 2
      ;;
    --np)
      NP="$2"
      shift 2
      ;;
    --router-ip)
      ROUTER_IP="$2"
      shift 2
      ;;
    --host-ip)
      HOST_IP="$2"
      shift 2
      ;;
    --output)
      OUTPUT="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$LAUNCHER" || -z "$HOSTS_CSV" ]]; then
  echo "Usage: $0 --launcher <host> --hosts host1,host2[,hostN] [--benchmark net|file] [...]" >&2
  exit 1
fi

if [[ "$BENCHMARK" != "net" && "$BENCHMARK" != "file" ]]; then
  echo "ERROR: --benchmark must be 'net' or 'file'" >&2
  exit 1
fi

IFS=',' read -r -a HOSTS <<< "$HOSTS_CSV"
if [[ ${#HOSTS[@]} -lt 2 ]]; then
  echo "ERROR: provide at least two Linux hosts in --hosts" >&2
  exit 1
fi

if [[ -z "$NP" ]]; then
  NP="${#HOSTS[@]}"
fi

if ! ping -c 1 -W 1 "$ROUTER_IP" >/dev/null 2>&1; then
  echo "WARNING: router VM $ROUTER_IP is not reachable from host $HOST_IP; Ubuntu DHCP or routed LAN behavior may not be ready yet." >&2
fi

LOCAL_BINARY="tools/build/bin/wos_mpi_netbench"
REMOTE_BINARY="${REMOTE_DIR}/wos_mpi_netbench"
if [[ "$BENCHMARK" == "file" ]]; then
  LOCAL_BINARY="tools/build/bin/wos_mpi_filebench"
  REMOTE_BINARY="${REMOTE_DIR}/wos_mpi_filebench"
fi

if [[ ! -x "$LOCAL_BINARY" ]]; then
  echo "ERROR: benchmark binary '$LOCAL_BINARY' not found or not executable" >&2
  exit 1
fi

for host in "${HOSTS[@]}"; do
  "${SCRIPT_DIR}/linux_ssh.sh" "$host" mkdir -p "$REMOTE_DIR"
  "${SCRIPT_DIR}/linux_scp.sh" "$LOCAL_BINARY" "${REMOTE_USER}@${host}:${REMOTE_BINARY}"
done

declare -a DNS_HOSTS=()
for host in "${HOSTS[@]}"; do
  host_name="$(${SCRIPT_DIR}/linux_ssh.sh "$host" hostname | tr -d '\r')"
  if [[ "$host_name" == *.internal ]]; then
    DNS_HOSTS+=("$host_name")
  else
    DNS_HOSTS+=("${host_name}.internal")
  fi
done

DNS_HOSTS_CSV="$(IFS=,; printf '%s' "${DNS_HOSTS[*]}")"

printf -v REMOTE_PASS_QUOTED '%q' "$REMOTE_PASS"
for host in "${HOSTS[@]}"; do
  "${SCRIPT_DIR}/linux_ssh.sh" "$host" bash -s -- "$REMOTE_AGENT" "$REMOTE_PASS_QUOTED" <<'EOF'
set -euo pipefail
agent_path="$1"
remote_pass_quoted="$2"
cat > "$agent_path" <<AGENT
#!/bin/sh
exec sshpass -p ${remote_pass_quoted} ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "\$@"
AGENT
chmod 700 "$agent_path"
EOF
done

if [[ "$BENCHMARK" == "file" && -z "$FILE_PATH" ]]; then
  FILE_PATH="${REMOTE_DIR}/mpi-filebench-input.bin"
fi

if [[ "$BENCHMARK" == "file" ]]; then
  "${SCRIPT_DIR}/linux_ssh.sh" "$LAUNCHER" sh -lc "if [ ! -f $(printf '%q' "$FILE_PATH") ]; then dd if=/dev/zero of=$(printf '%q' "$FILE_PATH") bs=1 count=0 seek=$(printf '%q' "$GENERATE_FILE_BYTES") status=none; fi"
fi

REMOTE_ARGS=(
  mpirun
  -np "$NP"
  --host "$DNS_HOSTS_CSV"
  --prtemca prte_keep_fqdn_hostnames 1
  --prtemca prte_if_include enp1s0
  --mca btl_tcp_if_include enp1s0
  --mca plm_rsh_agent "$REMOTE_AGENT"
  "$REMOTE_BINARY"
)

if [[ "$BENCHMARK" == "net" ]]; then
  REMOTE_ARGS+=(--mode "$MODE" --payload-size "$PAYLOAD_SIZE")
  if [[ "$MODE" == "pingpong" ]]; then
    REMOTE_ARGS+=(--iterations "$ITERATIONS")
  else
    REMOTE_ARGS+=(--total-bytes "$TOTAL_BYTES")
  fi
else
  REMOTE_ARGS+=(--path "$FILE_PATH" --chunk-size "$CHUNK_SIZE")
fi

printf -v REMOTE_CMD '%q ' "${REMOTE_ARGS[@]}"
RAW_RESULT="$(${SCRIPT_DIR}/linux_ssh.sh "$LAUNCHER" bash -s -- <<EOF
set -euo pipefail
${REMOTE_CMD}
EOF
2>&1)"
RESULT="$(printf '%s\n' "$RAW_RESULT" | awk '/^\{.*\}$/ { line = $0 } END { print line }')"

if [[ -z "$RESULT" ]]; then
  echo "ERROR: remote MPI benchmark produced no JSON output" >&2
  if [[ -n "$RAW_RESULT" ]]; then
    printf '%s\n' "$RAW_RESULT" >&2
  fi
  exit 1
fi

if [[ -z "$OUTPUT" ]]; then
  TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
  SAFE_LAUNCHER="${LAUNCHER//[^A-Za-z0-9._-]/_}"
  OUTPUT="benchmarks/results/linux/${SAFE_LAUNCHER}-mpi-${BENCHMARK}-${TIMESTAMP}.json"
fi

mkdir -p "$(dirname "$OUTPUT")"
printf '%s\n' "$RESULT" > "$OUTPUT"
printf '%s\n' "$RESULT"
echo "Saved result to $OUTPUT"
