#!/bin/sh
set -eu

DIR="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
# shellcheck disable=SC1091
. "$DIR/showcase-common.sh"

data_path="$(showcase_data_path)"
stat_iterations="${WOS_SHOWCASE_STAT_ITERATIONS:-$(showcase_scale_value stat_iterations)}"
read_iterations="${WOS_SHOWCASE_READ_ITERATIONS:-$(showcase_scale_value read_iterations)}"
metadata_iterations="${WOS_SHOWCASE_METADATA_TOTAL_ITERATIONS:-${WOS_SHOWCASE_METADATA_ITERATIONS:-$(showcase_scale_value metadata_iterations)}}"
mandel_width="${WOS_SHOWCASE_MANDEL_WIDTH:-$(showcase_scale_value mandel_width)}"
mandel_height="${WOS_SHOWCASE_MANDEL_HEIGHT:-$(showcase_scale_value mandel_height)}"
mandel_iter="${WOS_SHOWCASE_MANDEL_ITER:-$(showcase_scale_value mandel_iter)}"
mandel_threads="${WOS_SHOWCASE_MANDEL_THREADS:-$(showcase_scale_value mandel_threads)}"

if ! showcase_have /usr/bin/testprog; then
    printf 'skip: /usr/bin/testprog is not installed\n'
    exit 0
fi

showcase_section "vfsbench, local and remote-preferred"
showcase_cmd /usr/bin/testprog vfsbench-stat --path "$data_path" --iterations "$stat_iterations"
showcase_cmd locally /usr/bin/testprog vfsbench-read --path "$data_path" --read-size 65536 --iterations "$read_iterations"
showcase_cmd remotely /usr/bin/testprog vfsbench-stat --path "$data_path" --iterations "$stat_iterations"

showcase_section "vfsbench through forward +/srv -/tmp"
showcase_cmd forward +/srv -/tmp -- /usr/bin/testprog vfsbench-read --path "$data_path" --read-size 65536 --iterations "$read_iterations"
showcase_cmd remotely forward +/srv -/tmp -- /usr/bin/testprog vfsbench-stat --path "$data_path" --iterations "$stat_iterations"

showcase_section "fixed-total VFS create and rename through forward +/tmp"
metadata_prefix="/tmp/wos-showcase-vfsbench"
metadata_hosts="${WOS_SHOWCASE_HOSTS:-$(hostname)}"
metadata_launcher="$(hostname)"
metadata_log_dir="${WOS_SHOWCASE_OUTPUT_ROOT:-/tmp/wos-showcase-metadata}/logs/30-bench-wki-metadata"
metadata_timeout="${WOS_SHOWCASE_METADATA_TIMEOUT_SECONDS:-120}"

showcase_cmd locally /usr/bin/python3 "$DIR/metadata_bench.py" \
    --operation create \
    --hosts "$metadata_hosts" \
    --launcher "$metadata_launcher" \
    --path "$metadata_prefix" \
    --total-work-units "$metadata_iterations" \
    --timeout-seconds "$metadata_timeout" \
    --log-dir "$metadata_log_dir"
showcase_cmd locally /usr/bin/python3 "$DIR/metadata_bench.py" \
    --operation rename \
    --hosts "$metadata_hosts" \
    --launcher "$metadata_launcher" \
    --path "$metadata_prefix" \
    --total-work-units "$metadata_iterations" \
    --timeout-seconds "$metadata_timeout" \
    --log-dir "$metadata_log_dir"

showcase_section "small distributed mandelbench"
mandel_nodes="${WOS_SHOWCASE_HOSTS:-}"
if [ -n "$mandel_nodes" ]; then
    showcase_cmd /usr/bin/testprog mandelbench --width "$mandel_width" --height "$mandel_height" --max-iter "$mandel_iter" --threads "$mandel_threads" --repeat 1 --nodes "$mandel_nodes"
else
    showcase_cmd /usr/bin/testprog mandelbench --width "$mandel_width" --height "$mandel_height" --max-iter "$mandel_iter" --threads "$mandel_threads" --repeat 1
fi

showcase_section "perf summaries after the run"
if showcase_have /usr/bin/perf; then
    showcase_cmd /usr/bin/perf wki-report
    showcase_cmd /usr/bin/perf ipc-report 10
else
    printf 'skip: /usr/bin/perf is not installed\n'
fi
