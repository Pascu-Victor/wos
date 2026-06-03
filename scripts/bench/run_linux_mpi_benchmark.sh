#!/bin/bash
set -euo pipefail

WOS_ROOT="${WOS_WORKSPACE_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
REMOTE_SCRIPTS="$WOS_ROOT/scripts/remote"
cd "$WOS_ROOT"
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
RENDER_DEBUG_CONSTANT_TILE_US="0"
MANDEL_RANKS_PER_NODE=""
MANDEL_MAX_ITER="5000"
MANDEL_REPEAT="5"
MANDEL_OUTPUT_ROOT=""
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
    --max-iter)
      MANDEL_MAX_ITER="$2"
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
      MANDEL_RANKS_PER_NODE="$2"
      shift 2
      ;;
    --run-id)
      RENDER_RUN_ID="$2"
      shift 2
      ;;
    --repeat)
      MANDEL_REPEAT="$2"
      shift 2
      ;;
    --render-output-root)
      RENDER_OUTPUT_ROOT="$2"
      shift 2
      ;;
    --debug-constant-tile-us)
      RENDER_DEBUG_CONSTANT_TILE_US="$2"
      shift 2
      ;;
    --mandel-output-root)
      MANDEL_OUTPUT_ROOT="$2"
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
  echo "Usage: $0 --launcher <host> --hosts host1,host2[,hostN] [--benchmark net|file|mandel|render] [...]" >&2
  exit 1
fi

if [[ "$BENCHMARK" != "net" && "$BENCHMARK" != "file" && "$BENCHMARK" != "mandel" && "$BENCHMARK" != "render" ]]; then
  echo "ERROR: --benchmark must be 'net', 'file', 'mandel', or 'render'" >&2
  exit 1
fi

IFS=',' read -r -a HOSTS <<< "$HOSTS_CSV"
if [[ "$BENCHMARK" == "render" || "$BENCHMARK" == "mandel" ]]; then
  if [[ ${#HOSTS[@]} -lt 1 ]]; then
    echo "ERROR: provide at least one Linux host in --hosts" >&2
    exit 1
  fi
elif [[ ${#HOSTS[@]} -lt 2 ]]; then
  echo "ERROR: provide at least two Linux hosts in --hosts" >&2
  exit 1
fi

if [[ -z "$NP" && "$BENCHMARK" != "mandel" ]]; then
  NP="${#HOSTS[@]}"
fi

declare -a HOST_CPUS=()
TOTAL_HOST_CPUS=0
if [[ "$BENCHMARK" == "render" || "$BENCHMARK" == "mandel" ]]; then
  for host in "${HOSTS[@]}"; do
    host_cpus_raw="$(${REMOTE_SCRIPTS}/linux_ssh.sh "$host" getconf _NPROCESSORS_ONLN | tr -d '\r')"
    if [[ ! "$host_cpus_raw" =~ ^[0-9]+$ || "$host_cpus_raw" -le 0 ]]; then
      echo "ERROR: failed to determine CPU count for Linux host '$host' (got '$host_cpus_raw')" >&2
      exit 1
    fi
    HOST_CPUS+=("$host_cpus_raw")
    TOTAL_HOST_CPUS=$((TOTAL_HOST_CPUS + host_cpus_raw))
  done
fi

if [[ "$BENCHMARK" == "render" ]]; then
  if [[ -z "$RENDER_THREADS" && "$RENDER_PLACEMENT" == "node-threads" ]]; then
    lowest_host_cpus="${HOST_CPUS[0]}"
    for host_cpus in "${HOST_CPUS[@]}"; do
      if [[ "$host_cpus" -lt "$lowest_host_cpus" ]]; then
        lowest_host_cpus="$host_cpus"
      fi
    done
    RENDER_THREADS="$lowest_host_cpus"
  fi

  if [[ -z "${NP:-}" || "$NP" == "${#HOSTS[@]}" ]]; then
    if [[ "$RENDER_PLACEMENT" == "process-per-core" ]]; then
      NP="$TOTAL_HOST_CPUS"
    else
      NP="${#HOSTS[@]}"
    fi
  fi
elif [[ "$BENCHMARK" == "mandel" ]]; then
  if [[ -n "$MANDEL_RANKS_PER_NODE" && ! "$MANDEL_RANKS_PER_NODE" =~ ^[0-9]+$ ]]; then
    echo "ERROR: mandel --threads must be a positive integer rank count per node" >&2
    exit 1
  fi
  if [[ -n "$MANDEL_RANKS_PER_NODE" && "$MANDEL_RANKS_PER_NODE" -le 0 ]]; then
    echo "ERROR: mandel --threads must be a positive integer rank count per node" >&2
    exit 1
  fi
  if [[ -z "$NP" ]]; then
    if [[ -n "$MANDEL_RANKS_PER_NODE" ]]; then
      NP=$((MANDEL_RANKS_PER_NODE * ${#HOSTS[@]}))
    else
      NP="$TOTAL_HOST_CPUS"
    fi
  fi
  if [[ -n "$MANDEL_RANKS_PER_NODE" ]]; then
    EXPECTED_NP=$((MANDEL_RANKS_PER_NODE * ${#HOSTS[@]}))
    if [[ "$NP" != "$EXPECTED_NP" ]]; then
      echo "ERROR: mandel --np must equal --threads * host_count when per-node mapping is used (expected $EXPECTED_NP)" >&2
      exit 1
    fi
  fi
  if [[ -z "$MANDEL_OUTPUT_ROOT" ]]; then
    MANDEL_OUTPUT_ROOT="${REMOTE_DIR}/mandelbench-output"
  fi
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
elif [[ "$BENCHMARK" == "mandel" ]]; then
  LOCAL_BINARY="tools/build/bin/wos_mpi_mandelbench"
  REMOTE_BINARY="${REMOTE_DIR}/wos_mpi_mandelbench"
fi

for host in "${HOSTS[@]}"; do
  "${REMOTE_SCRIPTS}/linux_ssh.sh" "$host" mkdir -p "$REMOTE_DIR"
done

if [[ "$USE_HOST_BINARY" == "1" ]]; then
  if [[ ! -x "$LOCAL_BINARY" ]]; then
    echo "ERROR: benchmark binary '$LOCAL_BINARY' not found or not executable" >&2
    echo "Hint: configure with -DWOS_BUILD_LINUX_BENCHMARKS=ON and rebuild the tools target." >&2
    exit 1
  fi

  for host in "${HOSTS[@]}"; do
    "${REMOTE_SCRIPTS}/linux_ssh.sh" "$host" rm -f "$REMOTE_BINARY"
    "${REMOTE_SCRIPTS}/linux_scp.sh" "$LOCAL_BINARY" "${REMOTE_USER}@${host}:${REMOTE_BINARY}"
  done
else
  "${REMOTE_SCRIPTS}/linux_ssh.sh" "$LAUNCHER" mkdir -p \
    "${REMOTE_BUILD_ROOT}/benchmarks" \
    "${REMOTE_BUILD_ROOT}/renderbench" \
    "${REMOTE_BUILD_ROOT}/testprog/mandelbench"

  case "$BENCHMARK" in
    net)
      "${REMOTE_SCRIPTS}/linux_scp.sh" "tools/benchmarks/src/mpi_netbench.cpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/benchmarks/mpi_netbench.cpp"
      ;;
    file)
      "${REMOTE_SCRIPTS}/linux_scp.sh" "tools/benchmarks/src/mpi_filebench.cpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/benchmarks/mpi_filebench.cpp"
      ;;
    mandel)
      "${REMOTE_SCRIPTS}/linux_scp.sh" "tools/benchmarks/src/mpi_mandelbench.cpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/benchmarks/mpi_mandelbench.cpp"
      "${REMOTE_SCRIPTS}/linux_scp.sh" "modules/testprog/src/mandelbench/config.hpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/testprog/mandelbench/config.hpp"
      "${REMOTE_SCRIPTS}/linux_scp.sh" "modules/testprog/src/mandelbench/util.cpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/testprog/mandelbench/util.cpp"
      "${REMOTE_SCRIPTS}/linux_scp.sh" "modules/testprog/src/mandelbench/util.hpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/testprog/mandelbench/util.hpp"
      "${REMOTE_SCRIPTS}/linux_scp.sh" "modules/testprog/src/mandelbench/lodepng.cpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/testprog/mandelbench/lodepng.cpp"
      "${REMOTE_SCRIPTS}/linux_scp.sh" "modules/testprog/src/mandelbench/lodepng.hpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/testprog/mandelbench/lodepng.hpp"
      ;;
    render)
      "${REMOTE_SCRIPTS}/linux_scp.sh" "modules/renderbench/src/main.cpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/renderbench/main.cpp"
      "${REMOTE_SCRIPTS}/linux_scp.sh" "modules/renderbench/src/render_core.cpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/renderbench/render_core.cpp"
      "${REMOTE_SCRIPTS}/linux_scp.sh" "modules/renderbench/src/render_core.hpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/renderbench/render_core.hpp"
      "${REMOTE_SCRIPTS}/linux_scp.sh" "modules/testprog/src/mandelbench/lodepng.cpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/testprog/mandelbench/lodepng.cpp"
      "${REMOTE_SCRIPTS}/linux_scp.sh" "modules/testprog/src/mandelbench/lodepng.hpp" \
        "${REMOTE_USER}@${LAUNCHER}:${REMOTE_BUILD_ROOT}/testprog/mandelbench/lodepng.hpp"
      ;;
  esac

  "${REMOTE_SCRIPTS}/linux_ssh.sh" "$LAUNCHER" bash -s -- "$BENCHMARK" "$REMOTE_BUILD_ROOT" "$REMOTE_BINARY" <<'EOF'
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
  mandel)
    mpicxx -O3 -std=c++23 -Wall -Wextra \
      -I "$source_root/testprog" \
      -o "$output_binary" \
      "$source_root/benchmarks/mpi_mandelbench.cpp" \
      "$source_root/testprog/mandelbench/util.cpp" \
      "$source_root/testprog/mandelbench/lodepng.cpp"
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
  "${REMOTE_SCRIPTS}/linux_ssh.sh" "$LAUNCHER" cat "$REMOTE_BINARY" > "$TMP_BINARY"
  chmod 755 "$TMP_BINARY"
  for host in "${HOSTS[@]}"; do
    if [[ "$host" == "$LAUNCHER" ]]; then
      continue
    fi
    "${REMOTE_SCRIPTS}/linux_ssh.sh" "$host" mkdir -p "$REMOTE_DIR"
    "${REMOTE_SCRIPTS}/linux_ssh.sh" "$host" rm -f "$REMOTE_BINARY"
    "${REMOTE_SCRIPTS}/linux_scp.sh" "$TMP_BINARY" "${REMOTE_USER}@${host}:${REMOTE_BINARY}"
  done
fi

REMOTE_RENDER_SCENE=""
if [[ "$BENCHMARK" == "render" ]]; then
  for host in "${HOSTS[@]}"; do
    "${REMOTE_SCRIPTS}/linux_ssh.sh" "$host" bash -s -- "$RENDER_OUTPUT_ROOT" "$REMOTE_PASS" <<'EOF'
set -euo pipefail
output_root="$1"
remote_pass="$2"

if mkdir -p "$output_root" 2>/dev/null && [ -w "$output_root" ]; then
  exit 0
fi

printf '%s\n' "$remote_pass" | sudo -S -p '' install -d -m 0777 "$output_root"
EOF
  done
  if [[ -n "$RENDER_SCENE" ]]; then
    if [[ ! -f "$RENDER_SCENE" ]]; then
      echo "ERROR: requested render scene '$RENDER_SCENE' does not exist on the local host" >&2
      exit 1
    fi
    SCENE_BASENAME="${RENDER_SCENE##*/}"
    SCENE_BASENAME="${SCENE_BASENAME//[^A-Za-z0-9._-]/_}"
    REMOTE_RENDER_SCENE="${REMOTE_DIR}/renderbench-scene-${SCENE_BASENAME}"
    echo "Staging render scene '$RENDER_SCENE' to Linux MPI guests as '$REMOTE_RENDER_SCENE'" >&2
    for host in "${HOSTS[@]}"; do
      "${REMOTE_SCRIPTS}/linux_scp.sh" "$RENDER_SCENE" "${REMOTE_USER}@${host}:${REMOTE_RENDER_SCENE}"
    done
  fi
  if [[ -z "$RENDER_RUN_ID" ]]; then
    RENDER_RUN_ID="mpi-render-$(date +%Y%m%d-%H%M%S)"
  fi
elif [[ "$BENCHMARK" == "mandel" ]]; then
  for host in "${HOSTS[@]}"; do
    "${REMOTE_SCRIPTS}/linux_ssh.sh" "$host" mkdir -p "$MANDEL_OUTPUT_ROOT"
  done
fi

declare -a DNS_HOSTS=()
for host in "${HOSTS[@]}"; do
  host_name="$(${REMOTE_SCRIPTS}/linux_ssh.sh "$host" hostname | tr -d '\r')"
  if [[ "$host_name" == *.* ]]; then
    DNS_HOSTS+=("$host_name")
  else
    DNS_HOSTS+=("${host_name}.${MPI_HOST_DOMAIN}")
  fi
done

DNS_HOSTS_CSV="$(IFS=,; printf '%s' "${DNS_HOSTS[*]}")"
DNS_HOST_SLOTS_CSV=""
if [[ "$BENCHMARK" == "render" || "$BENCHMARK" == "mandel" ]]; then
  declare -a DNS_HOST_SLOTS=()
  for index in "${!DNS_HOSTS[@]}"; do
    DNS_HOST_SLOTS+=("${DNS_HOSTS[index]}:${HOST_CPUS[index]}")
  done
  DNS_HOST_SLOTS_CSV="$(IFS=,; printf '%s' "${DNS_HOST_SLOTS[*]}")"
fi

printf -v REMOTE_PASS_QUOTED '%q' "$REMOTE_PASS"
for host in "${HOSTS[@]}"; do
  "${REMOTE_SCRIPTS}/linux_ssh.sh" "$host" bash -s -- "$REMOTE_AGENT" "$REMOTE_PASS_QUOTED" <<'EOF'
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
  "${REMOTE_SCRIPTS}/linux_ssh.sh" "$LAUNCHER" sh -lc "if [ ! -f $(printf '%q' "$FILE_PATH") ]; then dd if=/dev/zero of=$(printf '%q' "$FILE_PATH") bs=1 count=0 seek=$(printf '%q' "$GENERATE_FILE_BYTES") status=none; fi"
fi

REMOTE_ARGS=(
  mpirun
  -np "$NP"
  --prtemca prte_keep_fqdn_hostnames 1
  --prtemca prte_if_include enp1s0
  --mca btl_tcp_if_include enp1s0
  --mca plm_rsh_agent "$REMOTE_AGENT"
)

if [[ "$BENCHMARK" == "render" && "$RENDER_PLACEMENT" == "process-per-core" ]]; then
  REMOTE_ARGS+=(
    --host "$DNS_HOST_SLOTS_CSV"
    --map-by slot
    --bind-to core
  )
elif [[ "$BENCHMARK" == "render" && "$RENDER_PLACEMENT" == "node-threads" ]]; then
  REMOTE_ARGS+=(
    --host "$DNS_HOSTS_CSV"
    --map-by ppr:1:node
    --bind-to none
  )
elif [[ "$BENCHMARK" == "mandel" ]]; then
  if [[ -n "$MANDEL_RANKS_PER_NODE" ]]; then
    REMOTE_ARGS+=(
      --host "$DNS_HOST_SLOTS_CSV"
      --map-by "ppr:${MANDEL_RANKS_PER_NODE}:node"
      --bind-to core
    )
  else
    REMOTE_ARGS+=(
      --host "$DNS_HOST_SLOTS_CSV"
      --map-by slot
      --bind-to core
    )
  fi
else
  REMOTE_ARGS+=(--host "$DNS_HOSTS_CSV")
fi

REMOTE_ARGS+=("$REMOTE_BINARY")

if [[ "$BENCHMARK" == "net" ]]; then
  REMOTE_ARGS+=(--mode "$MODE" --payload-size "$PAYLOAD_SIZE")
  if [[ "$MODE" == "pingpong" ]]; then
    REMOTE_ARGS+=(--iterations "$ITERATIONS")
  else
    REMOTE_ARGS+=(--total-bytes "$TOTAL_BYTES")
  fi
elif [[ "$BENCHMARK" == "file" ]]; then
  REMOTE_ARGS+=(--path "$FILE_PATH" --chunk-size "$CHUNK_SIZE")
elif [[ "$BENCHMARK" == "mandel" ]]; then
  REMOTE_ARGS+=(
    --width "$RENDER_WIDTH"
    --height "$RENDER_HEIGHT"
    --max-iter "$MANDEL_MAX_ITER"
    --repeat "$MANDEL_REPEAT"
    --output-root "$MANDEL_OUTPUT_ROOT"
  )
else
  REMOTE_ARGS+=(--backend mpi)
  if [[ -n "$REMOTE_RENDER_SCENE" ]]; then
    REMOTE_ARGS+=(--scene "$REMOTE_RENDER_SCENE")
  fi
  REMOTE_ARGS+=(
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
  if [[ "$RENDER_DEBUG_CONSTANT_TILE_US" != "0" ]]; then
    REMOTE_ARGS+=(--debug-constant-tile-us "$RENDER_DEBUG_CONSTANT_TILE_US")
  fi
fi

printf -v REMOTE_CMD '%q ' "${REMOTE_ARGS[@]}"
RAW_RESULT="$(${REMOTE_SCRIPTS}/linux_ssh.sh "$LAUNCHER" bash -s -- <<EOF
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
    if ARTIFACT_ERROR="$("${REMOTE_SCRIPTS}/linux_sftp_get.sh" "$host" "$REMOTE_FRAME" "$LOCAL_FRAME" 2>&1)"; then
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
