#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROUTER_IP="${WOS_ROUTER_IP:-10.10.0.1}"
HOST_IP="${WOS_HOST_IP:-10.10.0.100}"
REMOTE_USER="${WOS_LINUX_USER:-user}"
REMOTE_PASS="${WOS_LINUX_PASSWORD:-1234}"
MPI_HOST_DOMAIN="${WOS_LINUX_MPI_DOMAIN:-wos}"
USE_HOST_BINARY="${WOS_LINUX_USE_HOST_BINARY:-0}"
LAUNCHER=""
HOSTS_CSV=""
BENCHMARK="net"
REMOTE_DIR="/home/${REMOTE_USER}/.local/lib/wos-bench/bin"
REMOTE_AGENT="${REMOTE_DIR}/plm_ssh_agent.sh"
REMOTE_BUILD_ROOT=""
MODE="pingpong"
PAYLOAD_SIZE="1024"
ITERATIONS="10000"
TOTAL_BYTES="268435456"
FILE_PATH=""
CHUNK_SIZE="65536"
GENERATE_FILE_BYTES="67108864"
RENDER_SCENE=""
RENDER_WIDTH="640"
RENDER_HEIGHT="360"
RENDER_SPP="16"
RENDER_MAX_DEPTH="6"
RENDER_TILE_SIZE="32"
RENDER_PLACEMENT="node-threads"
RENDER_THREADS=""
RENDER_RUN_ID=""
RENDER_OUTPUT_ROOT="/var/lib/wos-bench/results/tracebench"
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
    --remote-build-root)
      REMOTE_BUILD_ROOT="$2"
      shift 2
      ;;
    --use-host-binary)
      USE_HOST_BINARY="1"
      shift
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
    --scene)
      RENDER_SCENE="$2"
      shift 2
      ;;
    --width)
      RENDER_WIDTH="$2"
      shift 2
      ;;
    --height)
      RENDER_HEIGHT="$2"
      shift 2
      ;;
    --spp)
      RENDER_SPP="$2"
      shift 2
      ;;
    --max-depth)
      RENDER_MAX_DEPTH="$2"
      shift 2
      ;;
    --tile-size)
      RENDER_TILE_SIZE="$2"
      shift 2
      ;;
    --placement)
      RENDER_PLACEMENT="$2"
      shift 2
      ;;
    --threads)
      RENDER_THREADS="$2"
      shift 2
      ;;
    --run-id)
      RENDER_RUN_ID="$2"
      shift 2
      ;;
    --render-output-root)
      RENDER_OUTPUT_ROOT="$2"
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
REMOTE_AGENT="${REMOTE_DIR}/plm_ssh_agent.sh"
if [[ -z "$REMOTE_BUILD_ROOT" ]]; then
  REMOTE_BUILD_ROOT="${REMOTE_DIR}/src"
fi

if [[ -z "$LAUNCHER" || -z "$HOSTS_CSV" ]]; then
  echo "Usage: $0 --launcher <host> --hosts host1,host2[,hostN] [--benchmark net|file|render] [...]" >&2
  exit 1
fi

if [[ "$BENCHMARK" != "net" && "$BENCHMARK" != "file" && "$BENCHMARK" != "render" ]]; then
  echo "ERROR: --benchmark must be 'net', 'file', or 'render'" >&2
  exit 1
fi

IFS=',' read -r -a HOSTS <<< "$HOSTS_CSV"
if [[ "$BENCHMARK" == "render" && ${#HOSTS[@]} -lt 1 ]]; then
  echo "ERROR: provide at least one Linux host in --hosts" >&2
  exit 1
fi
if [[ "$BENCHMARK" != "render" && ${#HOSTS[@]} -lt 2 ]]; then
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
elif [[ "$BENCHMARK" == "render" ]]; then
  LOCAL_BINARY="tools/build/bin/wos_mpi_renderbench"
  REMOTE_BINARY="${REMOTE_DIR}/wos_mpi_renderbench"
fi

for host in "${HOSTS[@]}"; do
  "${SCRIPT_DIR}/linux_ssh.sh" "$host" mkdir -p "$REMOTE_DIR"
done

if [[ "$USE_HOST_BINARY" == "1" ]]; then
  if [[ ! -x "$LOCAL_BINARY" ]]; then
    echo "ERROR: benchmark binary '$LOCAL_BINARY' not found or not executable" >&2
    echo "Hint: configure with -DWOS_BUILD_LINUX_BENCHMARKS=ON and rebuild the tools target." >&2
    exit 1
  fi

  for host in "${HOSTS[@]}"; do
    "${SCRIPT_DIR}/linux_scp.sh" "$LOCAL_BINARY" "${REMOTE_USER}@${host}:${REMOTE_BINARY}"
  done
else
  "${SCRIPT_DIR}/linux_ssh.sh" "$LAUNCHER" mkdir -p \
    "${REMOTE_BUILD_ROOT}/benchmarks" \
    "${REMOTE_BUILD_ROOT}/renderbench" \
    "${REMOTE_BUILD_ROOT}/testprog/mandelbench"

  case "$BENCHMARK" in
    net)
      "${SCRIPT_DIR}/linux_scp.sh" "tools/benchmarks/src/mpi_netbench.cpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/benchmarks/mpi_netbench.cpp"
      ;;
    file)
      "${SCRIPT_DIR}/linux_scp.sh" "tools/benchmarks/src/mpi_filebench.cpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/benchmarks/mpi_filebench.cpp"
      ;;
    render)
      "${SCRIPT_DIR}/linux_scp.sh" "modules/renderbench/src/main.cpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/renderbench/main.cpp"
      "${SCRIPT_DIR}/linux_scp.sh" "modules/renderbench/src/render_core.cpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/renderbench/render_core.cpp"
      "${SCRIPT_DIR}/linux_scp.sh" "modules/renderbench/src/render_core.hpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/renderbench/render_core.hpp"
      "${SCRIPT_DIR}/linux_scp.sh" "modules/testprog/src/mandelbench/lodepng.cpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/testprog/mandelbench/lodepng.cpp"
      "${SCRIPT_DIR}/linux_scp.sh" "modules/testprog/src/mandelbench/lodepng.hpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/testprog/mandelbench/lodepng.hpp"
      ;;
  esac

  "${SCRIPT_DIR}/linux_ssh.sh" "$LAUNCHER" bash -s -- "$BENCHMARK" "$REMOTE_BUILD_ROOT" "$REMOTE_BINARY" <<'EOF'
set -euo pipefail
benchmark="$1"
source_root="$2"
output_binary="$3"

case "$benchmark" in
  net)
    mpicxx -O3 -std=c++20 -Wall -Wextra -o "$output_binary" \
      "$source_root/benchmarks/mpi_netbench.cpp"
    ;;
  file)
    mpicxx -O3 -std=c++20 -Wall -Wextra -o "$output_binary" \
      "$source_root/benchmarks/mpi_filebench.cpp"
    ;;
  render)
    mpicxx -O3 -std=c++23 -Wall -Wextra -DTRACEBENCH_ENABLE_MPI=1 \
      -I "$source_root/renderbench" \
      -I "$source_root/testprog" \
      -pthread \
      -o "$output_binary" \
      "$source_root/renderbench/main.cpp" \
      "$source_root/renderbench/render_core.cpp" \
      "$source_root/testprog/mandelbench/lodepng.cpp"
    ;;
  *)
    echo "unknown benchmark: $benchmark" >&2
    exit 1
    ;;
esac
chmod 755 "$output_binary"
EOF

  TMP_BINARY="$(mktemp "/tmp/wos-mpi-${BENCHMARK}.XXXXXX")"
  trap 'rm -f "$TMP_BINARY"' EXIT
  "${SCRIPT_DIR}/linux_ssh.sh" "$LAUNCHER" cat "$REMOTE_BINARY" > "$TMP_BINARY"
  chmod 755 "$TMP_BINARY"
  for host in "${HOSTS[@]}"; do
    "${SCRIPT_DIR}/linux_scp.sh" "$TMP_BINARY" "${REMOTE_USER}@${host}:${REMOTE_BINARY}"
  done
fi

REMOTE_RENDER_SCENE="$RENDER_SCENE"
if [[ "$BENCHMARK" == "render" ]]; then
  for host in "${HOSTS[@]}"; do
    "${SCRIPT_DIR}/linux_ssh.sh" "$host" bash -s -- "$RENDER_OUTPUT_ROOT" "$REMOTE_PASS" <<'EOF'
set -euo pipefail
output_root="$1"
remote_pass="$2"

if mkdir -p "$output_root" 2>/dev/null && [ -w "$output_root" ]; then
  exit 0
fi

printf '%s\n' "$remote_pass" | sudo -S -p '' install -d -m 0777 "$output_root"
EOF
  done
  if [[ -n "$RENDER_SCENE" && -f "$RENDER_SCENE" ]]; then
    SCENE_BASENAME="${RENDER_SCENE##*/}"
    SCENE_BASENAME="${SCENE_BASENAME//[^A-Za-z0-9._-]/_}"
    REMOTE_RENDER_SCENE="${REMOTE_DIR}/renderbench-scene-${SCENE_BASENAME}"
    for host in "${HOSTS[@]}"; do
      "${SCRIPT_DIR}/linux_scp.sh" "$RENDER_SCENE" "${REMOTE_USER}@${host}:${REMOTE_RENDER_SCENE}"
    done
  fi
  if [[ -z "$RENDER_RUN_ID" ]]; then
    RENDER_RUN_ID="mpi-render-$(date +%Y%m%d-%H%M%S)"
  fi
fi

declare -a DNS_HOSTS=()
for host in "${HOSTS[@]}"; do
  host_name="$(${SCRIPT_DIR}/linux_ssh.sh "$host" hostname | tr -d '\r')"
  if [[ "$host_name" == *.* ]]; then
    DNS_HOSTS+=("$host_name")
  else
    DNS_HOSTS+=("${host_name}.${MPI_HOST_DOMAIN}")
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
elif [[ "$BENCHMARK" == "file" ]]; then
  REMOTE_ARGS+=(--path "$FILE_PATH" --chunk-size "$CHUNK_SIZE")
else
  REMOTE_ARGS+=(
    --backend mpi
    --scene "$REMOTE_RENDER_SCENE"
    --placement "$RENDER_PLACEMENT"
    --width "$RENDER_WIDTH"
    --height "$RENDER_HEIGHT"
    --spp "$RENDER_SPP"
    --max-depth "$RENDER_MAX_DEPTH"
    --tile-size "$RENDER_TILE_SIZE"
    --output-root "$RENDER_OUTPUT_ROOT"
    --run-id "$RENDER_RUN_ID"
  )
  if [[ -n "$RENDER_THREADS" ]]; then
    REMOTE_ARGS+=(--threads "$RENDER_THREADS")
  fi
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

if [[ "$BENCHMARK" == "render" ]]; then
  ARTIFACT_DIR="${OUTPUT%.json}-artifacts"
  REMOTE_FRAME="${RENDER_OUTPUT_ROOT}/${RENDER_RUN_ID}/frame_000.png"
  LOCAL_FRAME="${ARTIFACT_DIR}/frame_000.png"
  declare -a ARTIFACT_HOSTS=()
  add_artifact_host() {
    local candidate="$1"
    local existing
    for existing in "${ARTIFACT_HOSTS[@]}"; do
      if [[ "$existing" == "$candidate" ]]; then
        return 0
      fi
    done
    ARTIFACT_HOSTS+=("$candidate")
  }

  add_artifact_host "${HOSTS[0]}"
  add_artifact_host "$LAUNCHER"
  for host in "${HOSTS[@]}"; do
    add_artifact_host "$host"
  done

  ARTIFACT_FETCHED="0"
  ARTIFACT_ERROR=""
  for host in "${ARTIFACT_HOSTS[@]}"; do
    if ARTIFACT_ERROR="$("${SCRIPT_DIR}/linux_sftp_get.sh" "$host" "$REMOTE_FRAME" "$LOCAL_FRAME" 2>&1)"; then
      ARTIFACT_FETCHED="1"
      echo "Saved image to $LOCAL_FRAME"
      break
    fi
  done

  if [[ "$ARTIFACT_FETCHED" != "1" ]]; then
    echo "ERROR: failed to fetch render output image '$REMOTE_FRAME' over SFTP" >&2
    if [[ -n "$ARTIFACT_ERROR" ]]; then
      printf '%s\n' "$ARTIFACT_ERROR" >&2
    fi
    exit 1
  fi
fi
