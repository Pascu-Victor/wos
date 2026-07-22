#!/usr/bin/env python3

import csv
import hashlib
import json
import os
import shlex
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SELFHOST_RUNNER = ROOT / "scripts" / "bench" / "run_wos_selfhost_build.sh"
SELFHOST_REPEATABILITY = ROOT / "scripts" / "bench" / "run_wos_selfhost_repeatability.sh"
SELFHOST_LINUX_BASELINE = ROOT / "scripts" / "bench" / "run_linux_selfhost_baseline.sh"
SELFHOST_COMPARE = ROOT / "scripts" / "bench" / "compare_wos_selfhost_reports.py"
SELFHOST_CLUSTER = ROOT / "configs" / "cluster_selfhost.json"
DISTRIBUTED_CLUSTER = ROOT / "configs" / "cluster_bench_4.json"
DISTRIBUTED_SELFHOST_CLUSTER = ROOT / "configs" / "cluster_selfhost_4.json"
GIT_MIRROR = ROOT / "scripts" / "dev" / "git_mirror_for_wos.sh"
ROOTFS_ALIASES = ROOT / "configs" / "rootfs" / "aliases.tsv"


def fail(message: str) -> None:
    raise AssertionError(message)


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def test_selfhost_runner_covers_acceptance_flow() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            'DEFAULT_REPO="https://github.com/Pascu-Victor/wos.git"',
            'DEFAULT_LINUX_WORKDIR="${HOME:-/tmp}/wos-selfhost-bench"',
            'DEFAULT_WOS_WORKDIR="/root/wos-selfhost-bench"',
            'DEFAULT_JOBS="32"',
            "wos-local",
            "Direct GitHub cloning is the default",
            "--source-cache is only for faster debugging",
            "buildability, not Git history traversal throughput",
            "bin/wos-cluster --config configs/cluster_selfhost.json --launch --no-setup",
            "bin/wos-ktest --no-setup",
            "--distributed",
            'local distributed_command="locally forward"',
            'distributed_command+=" -- $remote_env bash \\\"\\$payload\\\""',
            'WOS_SELFHOST_DISTRIBUTED=$(shell_quote "$distributed")',
            "WOS_DISTRIBUTED_COMPILER=1",
            'WOS_DISTRIBUTED_COMPILER_HOSTS="$distributed_hosts"',
            'distributed_compiler_state="$workdir/tmp/distributed-compiler"',
            'WOS_DISTRIBUTED_COMPILER_STATE="$distributed_compiler_state"',
            "WOS_DISTRIBUTED_COMPILER_TRANSPORT=rewritten",
            'WOS_DISTRIBUTED_COMPILER_JOBS_PER_HOST="$distributed_jobs_per_host"',
            'distributed_jobs_per_host="$(((jobs + ${#distributed_host_list[@]} - 1) / ${#distributed_host_list[@]}))"',
            'WOS_SELFHOST_DISTRIBUTED_JOBS_PER_HOST=$(shell_quote "$distributed_jobs_per_host")',
            '--distributed-hosts',
            'for routed_path in /root /usr /bin /lib /lib64 /libexec /share /etc /proc /dev /run /tmp; do',
            "clone_cmd=(git clone)",
            "run_timed_event \"clone\" \"wos_repo\" run_git_http",
            "submodule_paths()",
            "clone_submodules()",
            "local submodule_cmd=(git -C \"$checkout\" submodule update --init --recursive)",
            "submodule_cmd+=(--depth 1)",
            'submodule_cmd+=(--jobs "$jobs" --)',
            'submodule_cmd+=("$path")',
            'run_timed_event "clone" "submodules" run_git_http "${submodule_cmd[@]}"',
            "--full-history",
            "full_history=0",
            'jobs="${WOS_SELFHOST_JOBS:-32}"',
            'jobs="$DEFAULT_JOBS"',
            'WOS_BUILD_JOBS="$jobs"',
            'WOS_NINJA_JOBS="$jobs"',
            'WOS_MAKE_JOBS="$jobs"',
            "unset WOS_BUILD_JOBS WOS_MAKE_JOBS WOS_NINJA_JOBS CMAKE_BUILD_PARALLEL_LEVEL",
            "./tools/bootstrap.sh",
            "WOS_HOST_CLANG_TIDY_CACHE=0",
            "WOS_USE_CCACHE=0",
            'log "bootstrap_ccache=disabled"',
            "canonical_path()",
            'canonical_workdir="$(canonical_path "$workdir")"',
            'if [ "$canonical_workdir" != "${workdir%/}" ]; then',
            "scratch workdir must be an absolute normalized path without symlink aliases",
            "source distdir must be outside the scratch workdir",
            "configure_cmake_command()",
            'printf \'%s\\n\' "$host_toolchain/bin/cmake"',
            'local cmake_args=("$cmake_command" -GNinja',
            "-DWOS_BUILD_WOSDBG=OFF",
            "-DWOS_BUILD_HOST_TOOLS=OFF",
            "-DWOS_BUILD_CMAKE_FOR_HOST=OFF",
            "-DWOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN=ON",
            "-DWOS_BUILD_DISK_IMAGES=OFF",
            "cmake --build \"$checkout/$build_dir\" --target \"$target\"",
            'DEFAULT_TARGET="wos_full"',
            'mode="${WOS_SELFHOST_MODE:?}"',
            'workdir="$DEFAULT_WOS_WORKDIR"',
            'workdir="$DEFAULT_LINUX_WORKDIR"',
            'WOS_SELFHOST_MODE="linux"',
            'WOS_SELFHOST_MODE="wos"',
            "WOS_SELFHOST_MODE=$(shell_quote wos)",
            'WOS_SELFHOST_FULL_HISTORY="$full_history"',
            "selfhost-report.tsv",
            'printf \'%s\\t%s\\n\' "total" "$total_elapsed" >> "$report"',
        ],
        "WOS self-host benchmark acceptance flow",
    )

    configure_start = source.find("configure_wos()")
    configure_end = source.find("build_wos()", configure_start)
    configure_block = source[configure_start:configure_end]
    if configure_start < 0 or configure_end < 0:
        fail("self-host benchmark configure flow is missing")
    if configure_block.count("-DWOS_BUILD_DISK_IMAGES=OFF") != 1:
        fail("self-host benchmark must disable host-only disk packaging in both operating systems")
    if 'if [ "$mode" = "wos" ]' in configure_block:
        fail("self-host benchmark Linux and WOS configure target graphs diverge")


def test_wos_bootstrap_distributes_only_compiler_processes() -> None:
    source = (ROOT / "tools" / "bootstrap.sh").read_text()
    require_tokens(
        source,
        [
            'compiler=("$system_clang" -resource-dir "$resource_dir")',
            r'if [ "\${WOS_DISTRIBUTED_COMPILER:-0}" = "1" ] && [ "\$compile_only" -eq 1 ]; then',
            r'IFS=, read -r -a compiler_hosts <<< "\${WOS_DISTRIBUTED_COMPILER_HOSTS:-}"',
            r'compiler_transport="\${WOS_DISTRIBUTED_COMPILER_TRANSPORT:-source}"',
            r"source|preprocessed|rewritten)",
            r'compiler_jobs_per_host="\$(((compiler_total_jobs + \${#compiler_hosts[@]} - 1) / \${#compiler_hosts[@]}))"',
            r'compiler_slots="\$compiler_state.source-slots"',
            r'compiler_start_index="\$((\$\$ % \${#compiler_hosts[@]}))"',
            r'on "\$compiler_host" "\${compiler[@]}" -fno-temp-file "@\$compiler_response"',
            r'compiler_jobs_per_host="\${WOS_DISTRIBUTED_COMPILER_JOBS_PER_HOST:-}"',
            r'compiler_total_jobs="\${WOS_NINJA_JOBS:-\${WOS_BUILD_JOBS:-}}"',
            r'compiler_local_jobs="\$compiler_total_jobs"',
            r'compiler_remote_jobs_per_host="\${WOS_DISTRIBUTED_COMPILER_REMOTE_JOBS_PER_HOST:-1}"',
            r'compiler_persist_remote_slots=1',
            r'compiler_local_jobs="\$compiler_jobs_per_host"',
            r'compiler_remote_jobs_per_host="\$compiler_jobs_per_host"',
            r'compiler_persist_remote_slots=0',
            r'if [ "\$compiler_persist_remote_slots" -eq 1 ]; then',
            r'for ((compiler_candidate_index = 1; compiler_candidate_index < \${#compiler_hosts[@]}; compiler_candidate_index++)); do',
            r'compiler_min_preprocessed_bytes="\${WOS_DISTRIBUTED_COMPILER_MIN_PREPROCESSED_BYTES:-0}"',
            r'compiler_preprocessed_language=cpp-output',
            r'compiler_preprocessed_language=c++-cpp-output',
            r'while ! mkdir "\$compiler_lock" 2>/dev/null; do',
            r'compiler_job_dir="\$(mktemp -d "\$compiler_responses/clang-job.XXXXXX" || true)"',
            r'compiler_input="\$compiler_job_dir/input"',
            r'"\${compiler[@]}" "\$@" "\${compiler_preprocess_args[@]}" -o "\$compiler_input" -Wno-unused-command-line-argument',
            r'compiler_input_size="\$(stat -c %s -- "\$compiler_input" 2>/dev/null || true)"',
            r'compiler_preprocess_args+=(-frewrite-includes)',
            r'compiler_forward_args+=(-x "\$compiler_remote_language" -Wno-unused-command-line-argument "\$compiler_input")',
            r'if [ "\$compiler_input_size" -lt "\$compiler_min_preprocessed_bytes" ]; then',
            r'"\${compiler[@]}" "\$@"',
            r'compiler_candidate_slot="\$compiler_host_slots/\$compiler_slot_index"',
            r'compiler_candidate_jobs="\$compiler_remote_jobs_per_host"',
            r'compiler_candidate_jobs="\$compiler_local_jobs"',
            r'compiler_slot_persistent=1',
            r'compiler_successes="\$compiler_state.successes"',
            r'''printf '%s\n' "\$success_host" > "\$compiler_successes/\$success_index"''',
            r'if mkdir "\$compiler_candidate_slot" 2>/dev/null; then',
            r'compiler_host="\${compiler_hosts[\$compiler_candidate_index]}"',
            r'compiler_slot_cleanup() {',
            r'if [ "\$compiler_candidate_index" -eq 0 ]; then',
            r'compiler_response="\$compiler_job_dir.response"',
            r'''printf '%q\n' "\$arg" >> "\$compiler_response"''',
            r'for arg in "\${compiler_forward_args[@]}"; do',
            r'env -i PATH="\$compiler_remote_path" HOME="\${HOME:-/root}" TMPDIR="\${TMPDIR:-/tmp}" TZ=UTC0',
            r'on "\$compiler_host" forward "+\$compiler_responses" --',
            r'"\${compiler[@]}" -fno-temp-file "@\$compiler_response"',
            r'rm -f -- "\$compiler_input"',
            r'compiler_slot_cleanup',
            r"warning: distributed compiler on \$compiler_host failed with status \$compiler_status; retrying locally",
            r'if "\${compiler[@]}" "\$@"; then',
            r'[ "\$link_output" -eq 1 ]',
            r'[ -n "\$output_file" ]',
            r'[ ! -x "\$output_file" ]',
            r'chmod a+x -- "\$output_file"',
            r"linked output '\$output_file' is not executable and could not be repaired",
        ],
        "WOS native compiler-only distribution",
    )


def test_wos_bootstrap_repairs_only_link_output_mode() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        script = r'''
set -euo pipefail
WOS_TARGET_ARCH=x86_64-pc-wos
source <(sed -n '/^write_clang_wrapper()/,/^}/p' tools/bootstrap.sh)
touch "$1/link-output" "$1/object-output"
chmod 0644 "$1/link-output" "$1/object-output"
write_clang_wrapper "$1/clang" /usr/bin/true /tmp
"$1/clang" -o "$1/link-output" input.c
"$1/clang" -c -o "$1/object-output" input.c
test -x "$1/link-output"
test ! -x "$1/object-output"
'''
        result = subprocess.run(
            ["bash", "-c", script, "wos-bootstrap-shim-test", temp_dir],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            fail(f"WOS compiler shim output-mode test failed: {result.stderr}")


def test_wos_bootstrap_caps_concurrent_compilers_per_host() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        (Path(temp_dir) / "input.c").write_text("int input;\n", encoding="ascii")
        mock_compiler = Path(temp_dir) / "system-clang"
        mock_compiler.write_text(
            r'''#!/bin/bash
set -u
output=
next_is_output=0
for arg in "$@"; do
    if [ "$next_is_output" -eq 1 ]; then output="$arg"; next_is_output=0; continue; fi
    if [ "$arg" = -o ]; then next_is_output=1; fi
done
[ -n "$output" ] && : > "$output"
''',
            encoding="ascii",
        )
        mock_compiler.chmod(0o755)
        mock_on = Path(temp_dir) / "on"
        mock_state = Path(temp_dir) / "mock-on"
        mock_on.write_text(
            r'''#!/bin/bash
set -u
host="$1"
state=MOCK_STATE_PATH
lock="$state.lock"
while ! mkdir "$lock" 2>/dev/null; do :; done
active_file="$state.active.$host"
max_file="$state.max.$host"
count_file="$state.count.$host"
active=0
maximum=0
count=0
if [ -s "$active_file" ]; then read -r active < "$active_file" || active=0; fi
if [ -s "$max_file" ]; then read -r maximum < "$max_file" || maximum=0; fi
if [ -s "$count_file" ]; then read -r count < "$count_file" || count=0; fi
active=$((active + 1))
count=$((count + 1))
printf '%s\n' "$active" > "$active_file"
printf '%s\n' "$count" > "$count_file"
if [ "$active" -gt "$maximum" ]; then printf '%s\n' "$active" > "$max_file"; fi
rmdir "$lock"
cleanup() {
    while ! mkdir "$lock" 2>/dev/null; do :; done
    active=0
    if [ -s "$active_file" ]; then read -r active < "$active_file" || active=0; fi
    printf '%s\n' "$((active - 1))" > "$active_file"
    rmdir "$lock"
}
trap cleanup EXIT
sleep 0.1
exit 0
'''.replace("MOCK_STATE_PATH", shlex.quote(str(mock_state))),
            encoding="ascii",
        )
        mock_on.chmod(0o755)
        script = r'''
set -euo pipefail
WOS_TARGET_ARCH=x86_64-pc-wos
source <(sed -n '/^write_clang_wrapper()/,/^}/p' tools/bootstrap.sh)
write_clang_wrapper "$1/clang" "$1/system-clang" /tmp
for index in 0 1 2 3 4 5 6 7; do
    PATH="$1:$PATH" \
        WOS_DISTRIBUTED_COMPILER=1 \
        WOS_DISTRIBUTED_COMPILER_HOSTS=wos-0,wos-1 \
        WOS_DISTRIBUTED_COMPILER_STATE="$1/compiler-state" \
        WOS_DISTRIBUTED_COMPILER_JOBS_PER_HOST=1 \
        WOS_DISTRIBUTED_COMPILER_MIN_PREPROCESSED_BYTES=0 \
        WOS_NINJA_JOBS=4 \
        "$1/clang" -c -o "$1/object-$index" "$1/input.c" &
done
wait
test ! -e "$1/mock-on.max.wos-0"
test "$(cat "$1/mock-on.max.wos-1")" -eq 1
test "$(cat "$1/mock-on.count.wos-1")" -ge 1
test "$(cat "$1/compiler-state.successes/0")" = wos-0
test "$(cat "$1/compiler-state.successes/1")" = wos-1
'''
        result = subprocess.run(
            ["bash", "-c", script, "wos-bootstrap-cap-test", temp_dir],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            fail(f"WOS compiler shim per-host cap test failed: {result.stderr}")


def test_wos_bootstrap_remote_response_file_preserves_arguments() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        source = temp / "source with spaces.cpp"
        c_source = temp / "source with spaces.c"
        header = temp / "header with spaces.h"
        mock_on = temp / "on"
        mock_on.write_text(
            r'''#!/bin/bash
set -u
[ "${TZ:-}" = UTC0 ]
shift
[ "$1" = forward ]
[ "$2" = "+$(dirname "${!#}")" ]
[ "$3" = -- ]
shift 3
response="${!#}"
response="${response#@}"
if grep -F -- CPP_SOURCE_PATH "$response" >/dev/null || \
        grep -F -- C_SOURCE_PATH "$response" >/dev/null; then
    echo "remote compiler response still names the original source" >&2
    exit 90
fi
if grep -Fx -- -MD "$response" >/dev/null || grep -Fx -- -MF "$response" >/dev/null; then
    echo "remote compiler response still contains dependency flags" >&2
    exit 91
fi
if ! grep -Ex -- 'c|c\+\+' "$response" >/dev/null; then
    echo "remote compiler response does not select rewritten C/C++" >&2
    exit 92
fi
mv CPP_SOURCE_PATH CPP_HIDDEN_SOURCE_PATH
mv C_SOURCE_PATH C_HIDDEN_SOURCE_PATH
mv HEADER_PATH HEADER_HIDDEN_PATH
"$@"
status=$?
mv CPP_HIDDEN_SOURCE_PATH CPP_SOURCE_PATH
mv C_HIDDEN_SOURCE_PATH C_SOURCE_PATH
mv HEADER_HIDDEN_PATH HEADER_PATH
exit "$status"
'''.replace("CPP_SOURCE_PATH", shlex.quote(str(source)))
            .replace("CPP_HIDDEN_SOURCE_PATH", shlex.quote(str(source) + ".hidden"))
            .replace("C_SOURCE_PATH", shlex.quote(str(c_source)))
            .replace("C_HIDDEN_SOURCE_PATH", shlex.quote(str(c_source) + ".hidden"))
            .replace("HEADER_PATH", shlex.quote(str(header)))
            .replace("HEADER_HIDDEN_PATH", shlex.quote(str(header) + ".hidden")),
            encoding="ascii",
        )
        mock_on.chmod(0o755)
        source.write_text(
            r'''#include "header with spaces.h"
#ifndef TEST_TEXT
#error TEST_TEXT was not preserved
#endif
static const char *text = TEST_TEXT;
int response_file_test(void) { return text[0] + TEST_HEADER_VALUE; }
''',
            encoding="ascii",
        )
        c_source.write_text(
            '#include "header with spaces.h"\nint c_response_file_test(void) { return TEST_HEADER_VALUE; }\n',
            encoding="ascii",
        )
        header.write_text("#define TEST_HEADER_VALUE 11\n", encoding="ascii")
        system_clang = ROOT / "toolchain" / "host" / "bin" / "clang"
        resource_dir = ROOT / "toolchain" / "host" / "lib" / "clang" / "22"
        script = r'''
set -euo pipefail
WOS_TARGET_ARCH=x86_64-pc-wos
source <(sed -n '/^write_clang_wrapper()/,/^}/p' tools/bootstrap.sh)
write_clang_wrapper "$1/clang" "$2" "$3"
printf '1\n' > "$1/compiler-state"
PATH="$1:$PATH" \
    WOS_DISTRIBUTED_COMPILER=1 \
    WOS_DISTRIBUTED_COMPILER_HOSTS=wos-0,wos-1 \
    WOS_DISTRIBUTED_COMPILER_STATE="$1/compiler-state" \
    WOS_DISTRIBUTED_COMPILER_TRANSPORT=rewritten \
    WOS_DISTRIBUTED_COMPILER_JOBS_PER_HOST=1 \
    WOS_DISTRIBUTED_COMPILER_MIN_PREPROCESSED_BYTES=0 \
    WOS_NINJA_JOBS=1 \
    "$1/clang" -D'TEST_TEXT="quoted value"' -MD -MF "$1/dependencies with spaces.d" \
        -c "$1/source with spaces.cpp" \
        -o "$1/object with spaces.o"
test -s "$1/object with spaces.o"
test -s "$1/dependencies with spaces.d"
"$2" -resource-dir "$3" -D'TEST_TEXT="quoted value"' -c "$1/source with spaces.cpp" \
    -o "$1/direct object with spaces.o"
objcopy="${2%/clang}/llvm-objcopy"
"$objcopy" --dump-section ".text=$1/rewritten cpp text" "$1/object with spaces.o"
"$objcopy" --dump-section ".text=$1/direct cpp text" "$1/direct object with spaces.o"
cmp "$1/rewritten cpp text" "$1/direct cpp text"
printf '1\n' > "$1/compiler-state"
PATH="$1:$PATH" \
    WOS_DISTRIBUTED_COMPILER=1 \
    WOS_DISTRIBUTED_COMPILER_HOSTS=wos-0,wos-1 \
    WOS_DISTRIBUTED_COMPILER_STATE="$1/compiler-state" \
    WOS_DISTRIBUTED_COMPILER_TRANSPORT=rewritten \
    WOS_DISTRIBUTED_COMPILER_JOBS_PER_HOST=1 \
    WOS_DISTRIBUTED_COMPILER_MIN_PREPROCESSED_BYTES=0 \
    WOS_NINJA_JOBS=1 \
    "$1/clang" -MD -MF "$1/c dependencies with spaces.d" -c "$1/source with spaces.c" \
        -o "$1/c object with spaces.o"
test -s "$1/c dependencies with spaces.d"
"$2" -resource-dir "$3" -c "$1/source with spaces.c" -o "$1/direct c object with spaces.o"
"$objcopy" --dump-section ".text=$1/rewritten c text" "$1/c object with spaces.o"
"$objcopy" --dump-section ".text=$1/direct c text" "$1/direct c object with spaces.o"
cmp "$1/rewritten c text" "$1/direct c text"
'''
        result = subprocess.run(
            [
                "bash",
                "-c",
                script,
                "wos-bootstrap-response-test",
                temp_dir,
                str(system_clang),
                str(resource_dir),
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            fail(f"WOS compiler shim response-file test failed: stdout={result.stdout!r} stderr={result.stderr!r}")


def test_wos_bootstrap_keeps_small_preprocessed_inputs_local() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        source = temp / "small.c"
        source.write_text("int small_compile(void) { return 17; }\n", encoding="ascii")
        remote_marker = temp / "remote-marker"
        mock_on = temp / "on"
        mock_on.write_text(
            "#!/bin/bash\ntouch REMOTE_MARKER\nexit 99\n".replace(
                "REMOTE_MARKER", shlex.quote(str(remote_marker))
            ),
            encoding="ascii",
        )
        mock_on.chmod(0o755)
        system_clang = ROOT / "toolchain" / "host" / "bin" / "clang"
        resource_dir = ROOT / "toolchain" / "host" / "lib" / "clang" / "22"
        script = r'''
set -euo pipefail
WOS_TARGET_ARCH=x86_64-pc-wos
source <(sed -n '/^write_clang_wrapper()/,/^}/p' tools/bootstrap.sh)
write_clang_wrapper "$1/clang" "$2" "$3"
PATH="$1:$PATH" \
    WOS_DISTRIBUTED_COMPILER=1 \
    WOS_DISTRIBUTED_COMPILER_HOSTS=wos-0,wos-1 \
    WOS_DISTRIBUTED_COMPILER_STATE="$1/compiler-state" \
    WOS_DISTRIBUTED_COMPILER_TRANSPORT=preprocessed \
    WOS_DISTRIBUTED_COMPILER_MIN_PREPROCESSED_BYTES=999999999 \
    WOS_NINJA_JOBS=1 \
    "$1/clang" -MD -MF "$1/small.d" -c "$1/small.c" -o "$1/small.o"
test -s "$1/small.o"
test -s "$1/small.d"
test ! -e "$1/remote-marker"
"$2" -resource-dir "$3" -c "$1/small.c" -o "$1/direct-small.o"
cmp "$1/small.o" "$1/direct-small.o"
'''
        result = subprocess.run(
            [
                "bash",
                "-c",
                script,
                "wos-bootstrap-small-local-test",
                temp_dir,
                str(system_clang),
                str(resource_dir),
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            fail(f"WOS compiler shim small-input local test failed: {result.stderr}")


def test_wos_bootstrap_compiles_submitter_jobs_once() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        (temp / "input.c").write_text("int input;\n", encoding="ascii")
        mock_compiler = temp / "system-clang"
        mock_compiler.write_text(
            r'''#!/bin/bash
set -u
count=0
if [ -s "$WOS_COMPILER_COUNT" ]; then read -r count < "$WOS_COMPILER_COUNT" || count=0; fi
printf '%s\n' "$((count + 1))" > "$WOS_COMPILER_COUNT"
output=
next_is_output=0
for arg in "$@"; do
    if [ "$next_is_output" -eq 1 ]; then output="$arg"; next_is_output=0; continue; fi
    if [ "$arg" = -o ]; then next_is_output=1; fi
done
[ -n "$output" ] && printf 'object\n' > "$output"
''',
            encoding="ascii",
        )
        mock_compiler.chmod(0o755)
        mock_on = temp / "on"
        mock_on.write_text("#!/bin/bash\nexit 99\n", encoding="ascii")
        mock_on.chmod(0o755)
        script = r'''
set -euo pipefail
WOS_TARGET_ARCH=x86_64-pc-wos
source <(sed -n '/^write_clang_wrapper()/,/^}/p' tools/bootstrap.sh)
write_clang_wrapper "$1/clang" "$1/system-clang" /tmp
mkdir -p "$1/compiler-state.slots/1/0"
PATH="$1:$PATH" \
    WOS_COMPILER_COUNT="$1/compiler-count" \
    WOS_DISTRIBUTED_COMPILER=1 \
    WOS_DISTRIBUTED_COMPILER_HOSTS=wos-0,wos-1 \
    WOS_DISTRIBUTED_COMPILER_STATE="$1/compiler-state" \
    WOS_DISTRIBUTED_COMPILER_TRANSPORT=preprocessed \
    WOS_DISTRIBUTED_COMPILER_MIN_PREPROCESSED_BYTES=0 \
    WOS_NINJA_JOBS=2 \
    "$1/clang" -c -o "$1/object.o" "$1/input.c"
test "$(cat "$1/compiler-count")" -eq 1
test "$(cat "$1/compiler-state.successes/0")" = wos-0
test ! -e "$1/compiler-state.responses"
'''
        result = subprocess.run(
            ["bash", "-c", script, "wos-bootstrap-local-once-test", temp_dir],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            fail(f"WOS compiler shim local single-pass test failed: {result.stderr}")


def test_wos_bootstrap_retries_failed_remote_compiler_locally() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        mock_on = temp / "on"
        mock_on.write_text("#!/bin/bash\nexit 1\n", encoding="ascii")
        mock_on.chmod(0o755)
        mock_compiler = temp / "system-clang"
        mock_compiler.write_text(
            r'''#!/bin/bash
set -u
output=
preprocess=0
next_is_output=0
for arg in "$@"; do
    if [ "$next_is_output" -eq 1 ]; then output="$arg"; next_is_output=0; continue; fi
    case "$arg" in
        -E) preprocess=1 ;;
        -o) next_is_output=1 ;;
    esac
done
if [ "$preprocess" -eq 1 ]; then
    [ -n "$output" ] && : > "$output"
else
    printf 'local\n' > "$WOS_FALLBACK_MARKER"
fi
''',
            encoding="ascii",
        )
        mock_compiler.chmod(0o755)
        (temp / "input.c").write_text("int input;\n", encoding="ascii")
        script = r'''
set -euo pipefail
WOS_TARGET_ARCH=x86_64-pc-wos
source <(sed -n '/^write_clang_wrapper()/,/^}/p' tools/bootstrap.sh)
write_clang_wrapper "$1/clang" "$1/system-clang" /tmp
printf '1\n' > "$1/compiler-state"
PATH="$1:$PATH" \
    WOS_FALLBACK_MARKER="$1/fallback-marker" \
    WOS_DISTRIBUTED_COMPILER=1 \
    WOS_DISTRIBUTED_COMPILER_HOSTS=wos-0,wos-1 \
    WOS_DISTRIBUTED_COMPILER_STATE="$1/compiler-state" \
    WOS_DISTRIBUTED_COMPILER_TRANSPORT=preprocessed \
    WOS_DISTRIBUTED_COMPILER_MIN_PREPROCESSED_BYTES=0 \
    WOS_NINJA_JOBS=1 \
    "$1/clang" -c -o "$1/object.o" "$1/input.c"
test "$(cat "$1/fallback-marker")" = local
test "$(cat "$1/compiler-state.successes/0")" = wos-0
test ! -e "$1/compiler-state.successes/1"
'''
        result = subprocess.run(
            ["bash", "-c", script, "wos-bootstrap-fallback-test", temp_dir],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            fail(f"WOS compiler shim local fallback test failed: {result.stderr}")
        if "failed with status 1; retrying locally" not in result.stderr:
            fail("WOS compiler shim did not report the remote failure before local retry")


def test_distributed_compiler_hosts_are_validated_before_launch() -> None:
    invalid_cases = [
        (["--distributed"], "--distributed requires --distributed-hosts"),
        (
            ["--distributed", "--distributed-hosts", "wos-0,wos-0"],
            "duplicate distributed host",
        ),
        (
            ["--distributed", "--distributed-hosts", "wos-1,wos-2"],
            "distributed hosts must include the submitter",
        ),
    ]
    for arguments, expected_error in invalid_cases:
        result = subprocess.run(
            [str(SELFHOST_RUNNER), "wos", *arguments],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0 or expected_error not in result.stderr:
            fail(f"distributed host validation accepted {arguments!r}")


def test_selfhost_runner_locks_workdir_before_replacing_it() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            'workdir_lock=""',
            "acquire_workdir_lock()",
            'workdir_lock="${workdir%/}.lock"',
            'mkdir "$workdir_lock"',
            "self-host benchmark workdir is already in use",
            "lock directory: $workdir_lock",
            "remove the lock only after confirming no benchmark process is using that workdir",
            "trap selfhost_cleanup EXIT",
            "release_workdir_lock()",
            'rm -rf -- "$workdir_lock"',
            "selfhost_cleanup()",
            'stop_log_heartbeat "$heartbeat_pid"',
        ],
        "WOS self-host benchmark workdir lock",
    )

    lock_pos = source.find("    acquire_workdir_lock\n\n    if [ \"$keep_workdir\"")
    remove_pos = source.find('rm -rf -- "$workdir"', lock_pos)
    if lock_pos < 0 or remove_pos < 0:
        fail("self-host benchmark must acquire the workdir lock before replacing the workdir")


def test_selfhost_repeatability_runner_preserves_acceptance_evidence() -> None:
    source = SELFHOST_REPEATABILITY.read_text()
    require_tokens(
        source,
        [
            "DEFAULT_RUNS=50",
            'DEFAULT_WORKDIR="/root/wos-selfhost-bench"',
            'DEFAULT_SERIAL_LOG="$WORKSPACE_ROOT/serial-vm0.log"',
            "DEFAULT_RUN_TIMEOUT_SECONDS=7200",
            'initial_boot_id="$(serial_boot_id || true)"',
            "for ((run_number = 1; run_number <= runs; ++run_number))",
            'runner_cmd=("$SELFHOST_RUNNER" wos --host "$host" --jobs "$jobs" --workdir "$workdir" --log-dir "$remote_log_dir"',
            '--history-file "$remote_log_dir/selfhost-history.tsv")',
            "runner_cmd+=(--heartbeat-sync)",
            'timeout --signal=TERM --kill-after=60s "${run_timeout_seconds}s" "${runner_cmd[@]}" > "$console_log" 2>&1',
            'runner_cmd+=(--distdir "$distdir")',
            'append_reason "runner_timeout"',
            "wait_for_guest_runner_cleanup()",
            'timeout --signal=KILL "${probe_seconds}s" "$WOS_SSH"',
            "no further guest evidence collection or mutation was attempted",
            "stopping to avoid unsafe guest reuse",
            'expected_bytes=$((after - before))',
            'iflag=skip_bytes,count_bytes',
            'capture_serial_range "$serial_start" "$serial_end" "$run_dir/serial.log"',
            'grep -Ein "$serial_fail_regex" "$run_dir/serial.log"',
            "hung task|blocked for more than",
            "allocation failure|allocator failure|failed to allocate",
            "async remote close send failed",
            "I/O error|input/output error",
            "filesystem.*(corrupt|shutdown)",
            "xfs.*(error|corrupt|shutdown|failed)",
            'guest_output "git -C $(shell_quote "$checkout_path") rev-parse HEAD"',
            'append_reason "commit_mismatch"',
            'append_reason "submodule_manifest_mismatch"',
            'append_reason "vm_boot_id_mismatch_during"',
            'append_reason "vm_reboot_or_unreachable_during"',
            "selfhost-report.tsv\tselfhost-report.tsv",
            "bootstrap-detail.tsv\tbootstrap-detail.tsv",
            "selfhost-cache-deltas.tsv\tselfhost-cache-deltas.tsv",
            "wos/build-selfhost/CMakeCache.txt\tCMakeCache.txt",
            'archive_guest_logs "$remote_log_dir" "$run_dir/command-logs.tar"',
            'runs_tsv="$output_dir/runs.tsv"',
            'summary_tsv="$output_dir/summary.tsv"',
            'summary_json="$output_dir/summary.json"',
            'json.dump(data, result, indent=2, sort_keys=True)',
            'if [ "$overall_pass" -ne 1 ]; then',
            "mirror_commit",
            "distdir_enabled",
            "distdir_manifest_sha256",
            "timed_out_runs",
            "--distributed-hosts",
            "--distributed-serial-logs",
            'runner_cmd+=(--distributed --distributed-hosts "$distributed_hosts_csv")',
            'capture_distributed_telemetry "$runner_pid" "$run_dir/distributed-telemetry.tsv"',
            'running_active=\\([0-9][0-9]*\\)',
            'capture_distributed_success_evidence "$console_log" "$run_dir/distributed-success.tsv"',
            'expected_compiler_state="${workdir%/}/tmp/distributed-compiler"',
            'if [ "$compiler_state" != "$expected_compiler_state" ]; then',
            'distributed_workload_succeeded_on_${distributed_successful_hosts}_hosts',
            'capture_distributed_serial_evidence "$run_dir"',
            'distributed_success_evidence',
            'required_distributed_successful_hosts',
        ],
        "WOS self-host repeatability evidence runner",
    )

    command_start = source.find('runner_cmd=("$SELFHOST_RUNNER"')
    command_end = source.find('echo "[$run_label/$runs] start', command_start)
    if command_start < 0 or command_end < 0:
        fail("repeatability runner command construction is missing")
    command_block = source[command_start:command_end]
    for forbidden in ("--resume-checkout", "--source-cache", "--skip-bootstrap"):
        if forbidden in command_block:
            fail(f"repeatability runner must keep every attempt fresh: {forbidden}")

    help_result = subprocess.run(
        ["bash", str(SELFHOST_REPEATABILITY), "--help"],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if help_result.returncode != 0 or "flow 50" not in help_result.stdout:
        fail(f"repeatability runner help failed: {help_result.stderr or help_result.stdout}")

    invalid_result = subprocess.run(
        ["bash", str(SELFHOST_REPEATABILITY), "--runs", "0"],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if invalid_result.returncode == 0 or "--runs must be a positive integer" not in invalid_result.stderr:
        fail("repeatability runner accepted an invalid run count")

    invalid_timeout = subprocess.run(
        ["bash", str(SELFHOST_REPEATABILITY), "--run-timeout-seconds", "0"],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if invalid_timeout.returncode == 0 or "--run-timeout-seconds must be a positive integer" not in invalid_timeout.stderr:
        fail("repeatability runner accepted an invalid per-run timeout")


def test_linux_selfhost_baseline_preserves_full_process_evidence() -> None:
    source = SELFHOST_LINUX_BASELINE.read_text()
    require_tokens(
        source,
        [
            "DEFAULT_RUNS=50",
            'DEFAULT_WORKDIR="/tmp/wos-selfhost-linux-baseline"',
            'DEFAULT_REPO="https://github.com/Pascu-Victor/wos.git"',
            "DEFAULT_RUN_TIMEOUT_SECONDS=7200",
            "for ((run_number = 1; run_number <= runs; ++run_number))",
            'runner_cmd=("$SELFHOST_RUNNER" linux --jobs "$jobs" --workdir "$workdir" --repo "$repo"',
            '--log-dir "$local_log_dir" --history-file "$local_history_file")',
            "runner_cmd+=(--heartbeat-sync)",
            'start_ms="$(now_ms)"',
            'timeout --signal=TERM --kill-after=60s "${run_timeout_seconds}s" "${runner_cmd[@]}" > "$console_log" 2>&1',
            'runner_cmd+=(--distdir "$distdir")',
            'append_reason "runner_timeout"',
            'end_ms="$(now_ms)"',
            'wall_ms=$((end_ms - start_ms))',
            'commit="$(git -C "$checkout_path" rev-parse HEAD',
            'append_reason "commit_mismatch"',
            'append_reason "submodule_manifest_mismatch"',
            'append_reason "linux_boot_id_mismatch_during"',
            "selfhost-report.tsv\tselfhost-report.tsv",
            "bootstrap-detail.tsv\tbootstrap-detail.tsv",
            "selfhost-history.tsv\tselfhost-history.tsv",
            "wos/build-selfhost/CMakeCache.txt\tCMakeCache.txt",
            'archive_command_logs "$local_log_dir" "$run_dir/command-logs.tar"',
            'runs_tsv="$output_dir/runs.tsv"',
            'summary_tsv="$output_dir/summary.tsv"',
            'summary_json="$output_dir/summary.json"',
            '"run" "start_utc" "end_utc" "wall_ms" "runner_status" "evidence_status"',
            '"accepted" "commit" "submodules_sha256"',
            'json.dump(data, result, indent=2, sort_keys=True)',
            'if [ "$overall_pass" -ne 1 ]; then',
            "mirror_commit",
            "distdir_enabled",
            "distdir_manifest_sha256",
            "timed_out_runs",
        ],
        "Linux full-process self-host baseline evidence runner",
    )

    command_start = source.find('runner_cmd=("$SELFHOST_RUNNER"')
    command_end = source.find('echo "[$run_label/$runs] start', command_start)
    if command_start < 0 or command_end < 0:
        fail("Linux baseline runner command construction is missing")
    command_block = source[command_start:command_end]
    for forbidden in ("--resume-checkout", "--source-cache", "--skip-bootstrap", "--host-toolchain"):
        if forbidden in command_block:
            fail(f"Linux baseline must keep every full-process attempt fresh: {forbidden}")

    timer_start = source.find('start_ms="$(now_ms)"', command_end)
    invocation = source.find('"${runner_cmd[@]}" > "$console_log" 2>&1', timer_start)
    timer_end = source.find('end_ms="$(now_ms)"', invocation)
    if timer_start < 0 or invocation < timer_start or timer_end < invocation:
        fail("Linux baseline outer clock does not enclose the complete runner invocation")

    help_result = subprocess.run(
        ["bash", str(SELFHOST_LINUX_BASELINE), "--help"],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if help_result.returncode != 0 or "flow 50" not in help_result.stdout:
        fail(f"Linux baseline help failed: {help_result.stderr or help_result.stdout}")

    invalid_result = subprocess.run(
        ["bash", str(SELFHOST_LINUX_BASELINE), "--runs", "0"],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if invalid_result.returncode == 0 or "--runs must be a positive integer" not in invalid_result.stderr:
        fail("Linux baseline accepted an invalid run count")

    invalid_timeout = subprocess.run(
        ["bash", str(SELFHOST_LINUX_BASELINE), "--run-timeout-seconds", "0"],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if invalid_timeout.returncode == 0 or "--run-timeout-seconds must be a positive integer" not in invalid_timeout.stderr:
        fail("Linux baseline accepted an invalid per-run timeout")

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        fake_runner = tmp_path / "fake-selfhost-runner.sh"
        fake_runner.write_text(
            """#!/bin/bash
set -euo pipefail

mode="${1:-}"
shift
workdir=""
log_dir=""
history_file=""
while (($# > 0)); do
    case "$1" in
        --workdir)
            workdir="$2"
            shift 2
            ;;
        --log-dir)
            log_dir="$2"
            shift 2
            ;;
        --history-file)
            history_file="$2"
            shift 2
            ;;
        --jobs|--repo|--mirror-file|--distdir)
            shift 2
            ;;
        --heartbeat-sync)
            shift
            ;;
        *)
            exit 64
            ;;
    esac
done
[ "$mode" = linux ]
[ -n "$workdir" ]
[ -n "$log_dir" ]
[ -n "$history_file" ]
should_timeout=0
if [ -n "${FAKE_TIMEOUT_MARKER:-}" ] && [ ! -e "$FAKE_TIMEOUT_MARKER" ]; then
    : > "$FAKE_TIMEOUT_MARKER"
    should_timeout=1
fi
rm -rf -- "$workdir"
mkdir -p "$workdir"
git clone -q --no-checkout "$FAKE_SOURCE_REPO" "$workdir/wos"
mkdir -p "$workdir/wos/build-selfhost" "$log_dir"
printf 'clone_sources\\t1\\nbuild_wos\\t1\\ntotal\\t2\\n' > "$workdir/selfhost-report.tsv"
printf 'run_id\\ttimestamp_utc\\n' > "$workdir/selfhost-detail.tsv"
printf 'phase\\telapsed_ms\\n' > "$workdir/bootstrap-detail.tsv"
printf 'run_id\\ttimestamp_utc\\n' > "$workdir/selfhost-cache-deltas.tsv"
printf 'run_id\\ttimestamp_utc\\n' > "$history_file"
printf ' same-submodule-pin path\\n' > "$workdir/submodules.txt"
printf 'WOS_BUILD_DISK_IMAGES:BOOL=OFF\\n' > "$workdir/wos/build-selfhost/CMakeCache.txt"
printf 'fake command log\\n' > "$log_dir/build.log"
if [ "$should_timeout" -eq 1 ]; then
    if [ "${FAKE_STALE_LOCK:-0}" = "1" ]; then
        mkdir "${workdir%/}.lock"
    fi
    sleep 5
fi
""",
            encoding="ascii",
        )
        fake_runner.chmod(0o755)
        output_dir = tmp_path / "results"
        workdir = tmp_path / "work"
        distdir = tmp_path / "distfiles"
        distdir.mkdir()
        distfile = distdir / "fixture-source.tar.xz"
        distfile.write_bytes(b"fixture source archive\n")
        distfile_sha256 = hashlib.sha256(distfile.read_bytes()).hexdigest()
        distdir_manifest = f"{distfile_sha256}  ./fixture-source.tar.xz\n"
        distdir_manifest_sha256 = hashlib.sha256(distdir_manifest.encode("ascii")).hexdigest()
        environment = os.environ.copy()
        environment["WOS_LINUX_SELFHOST_BASELINE_RUNNER"] = str(fake_runner)
        environment["FAKE_SOURCE_REPO"] = str(ROOT)
        result = subprocess.run(
            [
                str(SELFHOST_LINUX_BASELINE),
                "--runs",
                "2",
                "--jobs",
                "1",
                "--workdir",
                str(workdir),
                "--output-dir",
                str(output_dir),
                "--distdir",
                str(distdir),
                "--no-heartbeat-sync",
            ],
            cwd=ROOT,
            env=environment,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            fail(f"Linux baseline fixture run failed: {result.stderr or result.stdout}")
        with (output_dir / "runs.tsv").open(encoding="utf-8", newline="") as file:
            rows = list(csv.DictReader(file, delimiter="\t"))
        if len(rows) != 2 or any(row["accepted"] != "1" for row in rows):
            fail(f"Linux baseline did not emit two accepted outer-wall rows: {rows!r}")
        if any(int(row["wall_ms"]) < 0 for row in rows):
            fail(f"Linux baseline emitted an invalid outer wall time: {rows!r}")
        if any(
            row["distdir_enabled"] != "1"
            or row["distdir_manifest_sha256"] != distdir_manifest_sha256
            or row["timed_out"] != "0"
            for row in rows
        ):
            fail(f"Linux baseline did not preserve per-run distdir/timeout provenance: {rows!r}")
        summary = json.loads((output_dir / "summary.json").read_text(encoding="ascii"))
        if summary.get("distdir") != str(distdir) or summary.get("run_timeout_seconds") != 7200:
            fail(f"Linux baseline did not preserve distdir/timeout summary provenance: {summary!r}")
        if summary.get("distdir_manifest_sha256") != distdir_manifest_sha256:
            fail(f"Linux baseline did not preserve the pre-run distdir manifest: {summary!r}")
        for run_number in (1, 2):
            run_dir = output_dir / "runs" / f"run-{run_number:04d}"
            for name in (
                "console.log",
                "selfhost-report.tsv",
                "selfhost-detail.tsv",
                "bootstrap-detail.tsv",
                "selfhost-history.tsv",
                "submodules.txt",
                "CMakeCache.txt",
                "command-logs.tar",
                "host-before.txt",
                "host-after.txt",
            ):
                if not (run_dir / name).is_file():
                    fail(f"Linux baseline fixture is missing per-run evidence: {run_dir / name}")
            if "WOS_BUILD_DISK_IMAGES:BOOL=OFF" not in (run_dir / "CMakeCache.txt").read_text(encoding="ascii"):
                fail("Linux baseline fixture did not preserve the parity configure cache")

        timeout_output_dir = tmp_path / "timeout-results"
        timeout_workdir = tmp_path / "timeout-work"
        timeout_marker = tmp_path / "timeout-marker"
        environment["FAKE_TIMEOUT_MARKER"] = str(timeout_marker)
        result = subprocess.run(
            [
                str(SELFHOST_LINUX_BASELINE),
                "--runs",
                "2",
                "--jobs",
                "1",
                "--workdir",
                str(timeout_workdir),
                "--output-dir",
                str(timeout_output_dir),
                "--distdir",
                str(distdir),
                "--run-timeout-seconds",
                "2",
                "--no-heartbeat-sync",
            ],
            cwd=ROOT,
            env=environment,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0:
            fail("Linux baseline timeout fixture unexpectedly passed")
        with (timeout_output_dir / "runs.tsv").open(encoding="utf-8", newline="") as file:
            timeout_rows = list(csv.DictReader(file, delimiter="\t"))
        if len(timeout_rows) != 2:
            fail(f"Linux baseline timeout did not attempt every requested run: {timeout_rows!r}")
        if timeout_rows[0]["timed_out"] != "1" or "runner_timeout" not in timeout_rows[0]["failure_reasons"]:
            fail(f"Linux baseline did not classify the timed-out run: {timeout_rows!r}")
        if timeout_rows[1]["runner_status"] != "0" or timeout_rows[1]["accepted"] != "1":
            fail(f"Linux baseline did not continue after a timed-out run: {timeout_rows!r}")

        stale_output_dir = tmp_path / "stale-timeout-results"
        stale_workdir = tmp_path / "stale-timeout-work"
        environment["FAKE_TIMEOUT_MARKER"] = str(tmp_path / "stale-timeout-marker")
        environment["FAKE_STALE_LOCK"] = "1"
        result = subprocess.run(
            [
                str(SELFHOST_LINUX_BASELINE),
                "--runs",
                "2",
                "--jobs",
                "1",
                "--workdir",
                str(stale_workdir),
                "--output-dir",
                str(stale_output_dir),
                "--distdir",
                str(distdir),
                "--run-timeout-seconds",
                "2",
                "--no-heartbeat-sync",
            ],
            cwd=ROOT,
            env=environment,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0:
            fail("Linux baseline accepted a timeout with unproven cleanup")
        with (stale_output_dir / "runs.tsv").open(encoding="utf-8", newline="") as file:
            stale_rows = list(csv.DictReader(file, delimiter="\t"))
        if len(stale_rows) != 1 or "runner_cleanup_incomplete" not in stale_rows[0]["failure_reasons"]:
            fail(f"Linux baseline did not stop after unproven cleanup: {stale_rows!r}")
        if (stale_output_dir / "runs" / "run-0002").exists():
            fail("Linux baseline reused the workdir after unproven cleanup")
        if (stale_output_dir / "runs" / "run-0001" / "command-logs.tar").exists():
            fail("Linux baseline mutated/archived active workdir evidence after unproven cleanup")
        if not Path(f"{stale_workdir}.lock").is_dir():
            fail("Linux baseline removed an unproven active-workdir lock")

        contained_workdir = tmp_path / "contained-work"
        contained_distdir = contained_workdir / "distfiles"
        contained_distdir.mkdir(parents=True)
        distdir_alias = tmp_path / "contained-distdir-alias"
        distdir_alias.symlink_to(contained_distdir, target_is_directory=True)
        result = subprocess.run(
            [
                str(SELFHOST_LINUX_BASELINE),
                "--runs",
                "1",
                "--workdir",
                str(contained_workdir),
                "--output-dir",
                str(tmp_path / "contained-results"),
                "--distdir",
                str(distdir_alias),
            ],
            cwd=ROOT,
            env=environment,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0 or "--distdir must be outside the scratch workdir" not in result.stderr:
            fail("Linux baseline accepted a symlinked distdir inside the scratch workdir")

        mirror_root = tmp_path / "mirror-root"
        mirror_root.mkdir()
        invalid_mirror_repos = (
            "https://github.com/owner/repo.git/extra",
            "https://github.com/../repo.git",
            "https://github.com/owner/repo.git?ref=unsafe",
            "https://github.com/owner/repo.git#unsafe",
        )
        for index, invalid_repo in enumerate(invalid_mirror_repos):
            result = subprocess.run(
                [
                    str(SELFHOST_LINUX_BASELINE),
                    "--runs",
                    "1",
                    "--output-dir",
                    str(tmp_path / f"invalid-mirror-results-{index}"),
                    "--repo",
                    invalid_repo,
                    "--mirror-file",
                    str(mirror_root),
                    "--expected-commit",
                    "a" * 40,
                ],
                cwd=ROOT,
                env=environment,
                check=False,
                text=True,
                capture_output=True,
            )
            if result.returncode == 0 or "unsafe mirror" not in result.stderr:
                fail(f"Linux baseline accepted an unsafe mirror repository path: {invalid_repo}")

        result = subprocess.run(
            [
                str(SELFHOST_LINUX_BASELINE),
                "--runs",
                "1",
                "--output-dir",
                str(tmp_path / "uppercase-commit-results"),
                "--mirror-file",
                str(mirror_root),
                "--expected-commit",
                "A" * 40,
            ],
            cwd=ROOT,
            env=environment,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0 or "full lowercase 40-hex commit" not in result.stderr:
            fail("Linux baseline accepted a non-canonical expected commit")


def test_selfhost_cluster_profile_is_single_large_vm() -> None:
    config = json.loads(SELFHOST_CLUSTER.read_text())
    zones = config["zones"]
    global_zone = zones[0]

    if global_zone["id"] != "GLOBAL":
        fail("self-host cluster profile must keep GLOBAL zone first")

    vm = global_zone["vm"]
    if vm["memory"] != "32G" or vm["cpus"] != 32:
        fail("self-host cluster profile must expose one large VM")

    node_zones = [zone for zone in zones if zone["id"] != "GLOBAL"]
    if [zone["nodes"] for zone in node_zones] != [1, 1]:
        fail("self-host cluster profile must launch only node 0")

    lan_zone = node_zones[0]
    if lan_zone["name"] != "lan" or lan_zone["nic_queues"] < 4 or lan_zone["vhost"] is not True:
        fail("self-host cluster profile must keep SSH LAN responsive under full build load")

    if vm["disk0"] != "disk.qcow2" or vm["disk1"] != "mountfs.qcow2":
        fail("self-host cluster profile must use the normal WOS disks")


def test_distributed_cluster_has_plausible_per_core_memory() -> None:
    config = json.loads(DISTRIBUTED_CLUSTER.read_text())
    global_vm = config["zones"][0]["vm"]
    if global_vm["memory"] != "32768M" or global_vm["cpus"] != 8:
        fail("distributed benchmark nodes must provide 4 GiB per core")

    node_zones = [zone for zone in config["zones"] if zone["id"] != "GLOBAL"]
    if [zone["nodes"] for zone in node_zones] != [4, 4]:
        fail("distributed benchmark profile must launch four WOS systems")


def test_distributed_selfhost_cluster_balances_cpu_and_memory() -> None:
    config = json.loads(DISTRIBUTED_SELFHOST_CLUSTER.read_text())
    global_vm = config["zones"][0]["vm"]
    if global_vm["memory"] != "16384M" or global_vm["cpus"] != 8:
        fail("distributed self-host workers must provide 2 GiB per core")

    node_zones = [zone for zone in config["zones"] if zone["id"] != "GLOBAL"]
    if [zone["nodes"] for zone in node_zones] != [4, 4]:
        fail("distributed self-host profile must launch four WOS systems")

    controller_memory_mib = 0
    for zone in node_zones:
        overrides = zone.get("nodes_config", [])
        if overrides != [{"id": 0, "vm": {"memory": "49152M", "cpus": 8}}]:
            fail("distributed self-host controller must provide 8 CPUs and 48 GiB")
        controller_memory_mib = int(overrides[0]["vm"]["memory"].removesuffix("M"))

    if 4 * global_vm["cpus"] != 32:
        fail("distributed self-host profile must not oversubscribe the 32-thread host")
    worker_memory_mib = int(global_vm["memory"].removesuffix("M"))
    if controller_memory_mib + (3 * worker_memory_mib) != 98304:
        fail("distributed self-host profile must fit its 96 GiB VM allocation on the benchmark host")


def test_selfhost_runner_verifies_toolchain_kernel_and_utilities() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            'require_file "toolchain/sysroot/bin/clang"',
            'require_file "toolchain/sysroot/bin/ld.lld"',
            'require_file "toolchain/sysroot/bin/lld"',
            'require_file "toolchain/sysroot/bin/llvm-ar"',
            'require_file "toolchain/sysroot/bin/llvm-ranlib"',
            'require_file "toolchain/sysroot/bin/llvm-nm"',
            'require_file "toolchain/sysroot/bin/llvm-objcopy"',
            'require_file "toolchain/sysroot/bin/llvm-strip"',
            'require_file "toolchain/sysroot/bin/llvm-readelf"',
            'require_file "toolchain/sysroot/bin/llvm-objdump"',
            'require_file "toolchain/sysroot/bin/llvm-symbolizer"',
            'require_file "toolchain/sysroot/bin/llvm-as"',
            'require_file "toolchain/sysroot/bin/llvm-dis"',
            'require_file "toolchain/sysroot/bin/llvm-link"',
            'require_file "toolchain/sysroot/bin/llvm-size"',
            'require_file "toolchain/sysroot/bin/llvm-strings"',
            'require_file "toolchain/sysroot/bin/llvm-dwarfdump"',
            'require_file "toolchain/sysroot/bin/llc"',
            'require_file "toolchain/sysroot/bin/opt"',
            'require_file "toolchain/sysroot/bin/obj2yaml"',
            'require_file "toolchain/sysroot/bin/yaml2obj"',
            'require_file "toolchain/sysroot/bin/llvm-tblgen"',
            'require_file "toolchain/sysroot/bin/clang-tblgen"',
            'require_file "toolchain/sysroot/bin/x86_64-pc-wos.cfg"',
            'require_file "toolchain/sysroot/lib/clang"',
            'require_file "toolchain/sysroot/bin/ninja"',
            'require_file "toolchain/sysroot/bin/cmake"',
            'require_file "toolchain/sysroot/bin/ctest"',
            'require_file "toolchain/sysroot/bin/cpack"',
            'require_file "toolchain/sysroot/bin/make"',
            'require_file "toolchain/sysroot/bin/git"',
            'require_file "toolchain/sysroot/bin/git-shell"',
            'require_file "toolchain/sysroot/bin/scalar"',
            'require_file "toolchain/sysroot/libexec/git-core/git-remote-https"',
            'require_file "toolchain/sysroot/bin/curl"',
            'require_file "toolchain/sysroot/etc/ssl/certs/ca-certificates.crt"',
            'require_file "toolchain/sysroot/bin/bash"',
            'require_file "toolchain/sysroot/bin/dropbearmulti"',
            'require_file "toolchain/busybox-install/bin/busybox"',
            'require_file "toolchain/sysroot/bin/meson"',
            'require_file "toolchain/sysroot/bin/ndisasm"',
            'require_any "python" "toolchain/sysroot/bin/python" "toolchain/sysroot/bin/python*"',
            'require_any "python3" "toolchain/sysroot/bin/python3" "toolchain/sysroot/bin/python3.*"',
            'require_file "$build_dir/modules/kern/wos"',
            'require_file "$build_dir/modules/init/init"',
            'require_file "$build_dir/modules/testprog/testprog"',
            'require_file "$build_dir/modules/testd/testd"',
            'require_file "$build_dir/modules/netd/netd"',
            'require_file "$build_dir/modules/httpd/httpd"',
            'require_file "$build_dir/modules/debugserver/debugserver"',
            'require_file "$build_dir/modules/perf/perf"',
            'require_file "$build_dir/modules/top/top"',
            'require_file "$build_dir/modules/memacc/memacc"',
            'require_file "$build_dir/modules/journal/journal"',
            'require_file "$build_dir/modules/journal/libjournal.so"',
            'require_file "$build_dir/modules/wkictl/wkictl"',
            'require_file "$build_dir/modules/powerctl/powerctl"',
            'require_file "$build_dir/modules/renderbench/renderbench"',
            'require_file "$build_dir/modules/strace/strace"',
            'require_file "$build_dir/modules/sftpserver/sftp-server"',
            "cmake_cache_value()",
            "verify_disk_artifacts()",
            "WOS_BUILD_DISK_IMAGES",
            "skip disk image artifact verification",
            'require_file "disk.qcow2"',
            'require_file "mountfs.qcow2"',
        ],
        "WOS self-host benchmark artifact coverage",
    )


def test_selfhost_runner_preflights_wos_only_self_host_tools() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            "require_wos_selfhost_tools()",
            "sh env make tar sed grep mktemp sha256sum xz yes sleep tail wc stat \\",
            "ld.lld lld llvm-ar llvm-ranlib llvm-nm llvm-objcopy llvm-strip \\",
            "llvm-readelf llvm-objdump llvm-symbolizer llvm-tblgen clang-tblgen \\",
            "llvm-as llvm-dis llvm-link llc opt",
            'case "$mode" in',
            "wos)",
            "require_wos_selfhost_tools",
            "linux)",
            "unknown self-host mode",
        ],
        "WOS self-host benchmark WOS-mode tool preflight",
    )


def test_selfhost_runner_scrubs_inherited_toolchain_environment() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            'clean_path="${WOS_SELFHOST_CLEAN_PATH:-}"',
            'priority_reset="${WOS_SELFHOST_PRIORITY_RESET:-}"',
            "sanitize_selfhost_environment()",
            "reset_selfhost_priority()",
            'priority_nice_delta="${WOS_SELFHOST_PRIORITY_NICE_DELTA:-}"',
            'priority_reset_helper="${WOS_SELFHOST_PRIORITY_RESET_HELPER:-0}"',
            "reset_selfhost_priority_with_helper()",
            'clang -O2 -o "$helper" "$source"',
            "setpriority(PRIO_PROCESS, 0, prio)",
            'exec "$helper" "$priority_reset" /bin/bash "$0" "$@"',
            "unset CC CXX CPP LD AR AS NM OBJCOPY OBJDUMP RANLIB READELF STRIP",
            "unset CPPFLAGS CFLAGS CXXFLAGS LDFLAGS",
            "unset CPATH C_INCLUDE_PATH CPLUS_INCLUDE_PATH LIBRARY_PATH LD_LIBRARY_PATH",
            "unset PKG_CONFIG PKG_CONFIG_PATH PKG_CONFIG_LIBDIR PKG_CONFIG_SYSROOT_DIR",
            "unset CMAKE_PREFIX_PATH CMAKE_TOOLCHAIN_FILE CMAKE_GENERATOR CMAKE_BUILD_TYPE",
            "unset WOS_WORKSPACE_ROOT",
            "unset WOS_SYSROOT WOS_TARGET_ARCH WOS_TOOLCHAIN_MODE WOS_TOOLCHAIN_ROOT",
            'export PATH="$clean_path"',
            'WOS_SELFHOST_CLEAN_PATH="${WOS_ORIGINAL_PATH:-$PATH}"',
            'WOS_SELFHOST_CLEAN_PATH="/usr/bin:/bin:/usr/sbin:/sbin"',
            "WOS_SELFHOST_CLEAN_PATH=/usr/bin:/bin:/usr/sbin:/sbin",
            "WOS_SELFHOST_PRIORITY_RESET_DONE",
            'exec nice -n "$priority_nice_delta" /bin/bash "$0" "$@"',
            'exec python3 - "$0" "$@"',
            "os.getpriority(os.PRIO_PROCESS, 0)",
            'os.execvp("nice", ["nice", "-n", str(delta), "/bin/bash"',
            "WOS_SELFHOST_PRIORITY_RESET=0",
            "WOS_SELFHOST_PRIORITY_NICE_DELTA=5",
            "WOS_SELFHOST_PRIORITY_RESET_HELPER=1",
        ],
        "WOS self-host benchmark environment sanitizer",
    )
    if "os.setpriority(" in source:
        fail("WOS self-host priority reset must not require Python os.setpriority")


def test_rootfs_stages_self_hosting_tablegen_tools() -> None:
    aliases = ROOTFS_ALIASES.read_text()
    require_tokens(
        aliases,
        [
            "copy-glob\ttoolchain/sysroot/bin/clang*\t/usr/bin",
            "copy-glob\ttoolchain/sysroot/bin/llvm-*\t/usr/bin",
            "copy-glob\ttoolchain/sysroot/bin/llc\t/usr/bin",
            "copy-glob\ttoolchain/sysroot/bin/opt\t/usr/bin",
            "copy-glob\ttoolchain/sysroot/bin/obj2yaml\t/usr/bin",
            "copy-glob\ttoolchain/sysroot/bin/yaml2obj\t/usr/bin",
        ],
        "WOS rootfs self-hosting LLVM tool staging",
    )


def test_rootfs_stages_git_helpers_at_configured_runtime_paths() -> None:
    aliases = ROOTFS_ALIASES.read_text()
    require_tokens(
        aliases,
        [
            "copy\ttoolchain/sysroot/libexec/git-core\t/libexec/git-core",
            "copy\ttoolchain/sysroot/share/git-core\t/share/git-core",
            "copy\ttoolchain/sysroot/libexec/git-core\t/usr/libexec/git-core",
            "copy\ttoolchain/sysroot/share/git-core\t/usr/share/git-core",
        ],
        "WOS rootfs Git runtime helper staging",
    )


def test_selfhost_runner_mirror_is_optional_not_default() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            "--mirror-file PATH",
            "--mirror-local-path PATH",
            "--mirror-http-prefix U",
            "Rewrite https://github.com/",
            'mirror_file=""',
            'mirror_local_path=""',
            'mirror_http_prefix=""',
            'git config --global protocol.file.allow always',
            'git config --global "url.file://$mirror_file/".insteadOf https://github.com/',
            'git config --global "url.$mirror_local_path/".insteadOf https://github.com/',
            'git config --global "url.$mirror_http_prefix".insteadOf https://github.com/',
        ],
        "WOS self-host benchmark mirror controls",
    )


def test_selfhost_runner_source_cache_is_iteration_only() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            "--source-cache PATH",
            "--distdir PATH",
            "iteration-only",
            "depth-1 checkout",
            'source_cache="${WOS_SELFHOST_SOURCE_CACHE:-}"',
            'distdir="${WOS_SELFHOST_DISTDIR:-}"',
            'source_cache=""',
            'distdir=""',
            "seed_sources_from_cache()",
            'source_cache="${source_cache%/}"',
            'bootstrap_detail_report="$workdir/bootstrap-detail.tsv"',
            'checkout="$source_cache"',
            'if [ ! -f "$source_cache/.gitmodules" ]; then',
            "validate_depth1_git_repo()",
            'rev-parse --is-shallow-repository',
            'rev-list --count HEAD',
            "validate_git_worktree_clean()",
            'git -C "$path" status --porcelain --untracked-files=no',
            "must have a populated clean Git worktree",
            'validate_source_cache_checkout()',
            'validate_source_cache_submodules()',
            "source cache must be a depth-1 Git checkout, not an exported source tree",
            'run_timed_event "source_cache" "depth1_checkout" validate_source_cache_checkout',
            'run_timed_event "source_cache" "submodule_status" write_submodule_status',
            'run_timed_event "source_cache" "depth1_submodules" validate_source_cache_submodules',
            "source cache has uninitialized submodules",
            'WOS_SELFHOST_SOURCE_CACHE="$source_cache"',
            'WOS_SELFHOST_DISTDIR="$distdir"',
            'WOS_SELFHOST_MIRROR_LOCAL_PATH="$mirror_local_path"',
            "WOS_SELFHOST_SOURCE_CACHE=$(shell_quote \"$source_cache\")",
            "WOS_SELFHOST_DISTDIR=$(shell_quote \"$distdir\")",
            "WOS_SELFHOST_MIRROR_LOCAL_PATH=$(shell_quote \"$mirror_local_path\")",
            'WOS_BOOTSTRAP_DETAIL_TSV="$bootstrap_detail_report"',
            'WOS_SOURCE_DISTDIR="$distdir"',
            "log \"bootstrap_detail_report=$bootstrap_detail_report\"",
        ],
        "WOS self-host benchmark source-cache controls",
    )

    cache_block_start = source.find('if [ -n "$source_cache" ]; then')
    cache_block_end = source.find("configure_git_mirror", cache_block_start)
    if cache_block_start < 0 or cache_block_end < 0:
        fail("source-cache branch must be present before mirror/direct clone setup")
    cache_block = source[cache_block_start:cache_block_end]
    if "git clone" in cache_block or "submodule update" in cache_block or "cp -a" in cache_block:
        fail("source-cache branch must not fetch from the network or copy the source tree")
    for forbidden in [
        "reuse_exported_tree",
        "validate_exported_source_tree",
        "write_exported_submodule_status",
        "exported-source-tree",
    ]:
        if forbidden in source:
            fail(f"source-cache branch must reject exported source trees, found {forbidden}")


def test_selfhost_runner_resume_checkout_is_validation_only() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            "--resume-checkout",
            'resume_checkout="${WOS_SELFHOST_RESUME_CHECKOUT:-0}"',
            'resume_checkout=0',
            "--resume-checkout requires an existing Git checkout",
            "--keep-workdir and --resume-checkout are mutually exclusive",
            "refresh_resume_checkout_worktrees()",
            'git -C "$checkout" checkout -f HEAD',
            "submodule foreach --recursive 'git checkout -f HEAD'",
            "resume_existing_checkout()",
            'log "resume_checkout=$checkout"',
            'run_timed_event "resume_checkout" "refresh_worktrees" refresh_resume_checkout_worktrees',
            'run_timed_event "resume_checkout" "depth1_checkout" validate_source_cache_checkout',
            'run_timed_event "resume_checkout" "submodule_status" write_submodule_status',
            'run_timed_event "resume_checkout" "depth1_submodules" validate_source_cache_submodules',
            "must have a populated clean Git worktree",
            "resume checkout has uninitialized submodules",
            'WOS_SELFHOST_RESUME_CHECKOUT="$resume_checkout"',
            "WOS_SELFHOST_RESUME_CHECKOUT=$(shell_quote \"$resume_checkout\")",
        ],
        "WOS self-host benchmark resume-checkout controls",
    )

    resume_start = source.find("resume_existing_checkout()")
    clone_start = source.find("clone_sources()", resume_start)
    if resume_start < 0 or clone_start < 0:
        fail("resume-checkout helper must appear before clone_sources")
    resume_block = source[resume_start:clone_start]
    if "git clone" in resume_block or "submodule update" in resume_block or "cp -a" in resume_block:
        fail("resume-checkout branch must only validate the existing depth-1 checkout")


def test_selfhost_debug_mirror_path_is_shallow_and_not_source_tree_copy() -> None:
    runner = SELFHOST_RUNNER.read_text()
    mirror = GIT_MIRROR.read_text()
    docs = (ROOT / "scripts" / "README.md").read_text()

    require_tokens(
        runner,
        [
            "clone_cmd+=(--depth 1)",
            "submodule_cmd+=(--depth 1)",
            'submodule_cmd+=(--jobs "$jobs" --)',
            'submodule_cmd+=("$path")',
            'run_timed_event "clone" "submodules" run_git_http "${submodule_cmd[@]}"',
            'git config --global "url.file://$mirror_file/".insteadOf https://github.com/',
        ],
        "WOS self-host benchmark shallow mirror clone path",
    )
    require_tokens(
        mirror,
        [
            "snapshot [--worktree]",
            "snapshot creates shallow bare repositories",
            "Use --worktree to include uncommitted",
            "including dirty submodule worktrees",
            "Local Meson wrap-git checkouts",
            "depth-1 Git clone from the",
            'git -C "$target" fetch --depth=1 "$source" "$sha:$ref"',
            'git -C "$target" repack -ad',
            'git -C "$target" prune-packed',
            "remove_pack_bitmaps()",
            'rm -f -- "$bitmap"',
            "snapshot_is_complete()",
            "fsck --connectivity-only --no-dangling",
            "Local snapshot for $github_path is incomplete",
            'git -C "$target" fetch --depth=1 "$url" "$sha:$ref"',
            "snapshot for $github_path is missing objects required by $sha",
            "meson_git_wraps()",
            "snapshot_meson_wraps()",
            "local_meson_wrap_source_for_url()",
            "hydrate_snapshot_from_worktree()",
            'hydrate_snapshot_from_worktree "$target" "$source" "$sha"',
            'snapshot_is_complete "$target" "$sha"',
            'git -C "$target" rev-list --objects --missing=print "$treeish"',
            'git -C "$target" rev-list --objects --missing=print "$sha"',
            'sed -n \'s/^?//p\'',
            'if [ ! -s "$missing_objects" ]; then',
            'git -C "$source" ls-tree -r "$treeish"',
            'grep -qx "$oid" "$missing_objects"',
            'git --git-dir="$target" cat-file -e "$oid"',
            'if [ -L "$source/$path" ]; then',
            'readlink "$source/$path"',
            "git hash-object -w --stdin",
            'elif [ -f "$source/$path" ]; then',
            'git hash-object -w --path="$path" "$source/$path"',
            "failed to hydrate blob",
            "toolchain/src/mlibc/subprojects",
            "expected $revision from $wrap",
            "Skipping Meson wrap snapshot for missing local source",
            "WORKTREE_SUBMODULE_PATHS",
            "WORKTREE_SUBMODULE_SHAS",
            "make_synthetic_worktree_source()",
            "snapshot_sha",
            'git -C "$repo" fetch --depth=1 "$source" "$ref:refs/heads/wos-worktree-base"',
            'GIT_WORK_TREE="$source"',
            "git update-index --add --cacheinfo",
            '\"160000,${WORKTREE_SUBMODULE_SHAS[$i]},${WORKTREE_SUBMODULE_PATHS[$i]}\"',
            "submodule_worktree_dirty()",
            "snapshot_configured_submodules()",
            "status --porcelain=v1 --untracked-files=all",
            "make_worktree_snapshot_source()",
            "git read-tree refs/heads/wos-worktree-base",
            "git add -A",
            "git write-tree",
            'git commit-tree "$tree" -p "$parent" -m "$message"',
            "refs/heads/wos-worktree-snapshot",
            'snapshot_configured_submodules "$include_worktree"',
            "make_worktree_snapshot_source",
            'top_sha="$WORKTREE_SNAPSHOT_SHA"',
            "trap cleanup_worktree_snapshot EXIT",
            "reject_source_tree_repos_root()",
            'refusing to treat a source checkout as the Git mirror root',
            'tar -C "$REPOS_ROOT" -cf "$archive" .',
            '"cat > $quoted_remote_archive_path" < "$archive"',
            'archive_size="$(wc -c < "$archive")"',
            "remote archive size mismatch",
            'tar xf "$remote_archive_path" -C "$remote_copied_path"',
            'chown -R "$(id -u):$(id -g)" "$remote_copied_path"',
            'if [ ! -d "$remote_copied_path/Pascu-Victor/wos.git" ]; then',
            "synced mirror is missing Pascu-Victor/wos.git",
            "command -v chown",
            "command -v id",
            "chown -R",
            "cmd_print_wos_config file \"$wos_path\"",
        ],
        "WOS Git mirror shallow snapshot and sync guard",
    )
    require_tokens(
        docs,
        [
            "prefer a shallow",
            "scripts/dev/git_mirror_for_wos.sh snapshot --worktree",
            "scripts/dev/git_mirror_for_wos.sh sync-file-mirror wos-0 /tmp/wos-git-repos",
            "--jobs 32 --mirror-file /tmp/wos-git-repos",
            "depth-1 Git clone inside WOS",
            "captures the",
            "current non-ignored worktree in a temporary synthetic commit",
            "Do not copy the live workspace into",
        ],
        "WOS self-host shallow mirror documentation",
    )


def test_selfhost_runner_records_detailed_historical_timings() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            "--history-file PATH",
            "--log-dir PATH",
            "selfhost-detail.tsv",
            'history_file="${WOS_SELFHOST_HISTORY_FILE:-}"',
            'log_dir="${WOS_SELFHOST_LOG_DIR:-}"',
            'git_http_low_speed_limit="${WOS_SELFHOST_GIT_HTTP_LOW_SPEED_LIMIT:-1}"',
            'git_http_low_speed_time="${WOS_SELFHOST_GIT_HTTP_LOW_SPEED_TIME:-120}"',
            'heartbeat_interval="${WOS_SELFHOST_HEARTBEAT_INTERVAL:-30}"',
            'heartbeat_tail="${WOS_SELFHOST_HEARTBEAT_TAIL:-4}"',
            'heartbeat_sync="${WOS_SELFHOST_HEARTBEAT_SYNC:-0}"',
            'heartbeat_stall_snapshots="${WOS_SELFHOST_HEARTBEAT_STALL_SNAPSHOTS:-6}"',
            'history_file="${workdir%/}-history.tsv"',
            "default_log_dir()",
            "printf '/tmp/wos-selfhost-logs-%s\\n' \"$run_id\"",
            "printf '%s/logs\\n' \"$workdir\"",
            'log_dir="$(default_log_dir)"',
            "log_path_for()",
            "print_log_tail()",
            "heartbeat_progress_file()",
            "heartbeat_snapshot_file()",
            "heartbeat_append_progress()",
            "heartbeat_file_bytes()",
            "heartbeat_file_lines()",
            "heartbeat_process_snapshot()",
            'log "process snapshot $phase $label"',
            'snapshot_output="$(heartbeat_snapshot_file)"',
            'is_wos="$(uname -s 2>/dev/null || printf unknown)"',
            '>> "$snapshot_output" 2>&1 || true',
            'log "snapshot saved $snapshot_output"',
            "ps w 2>/dev/null",
            "heartbeat_wos_procfs_snapshot()",
            "heartbeat_wos_procfs_snapshot",
            "/proc/kcpustate",
            "/proc/memacc/alloc_totals",
            "/proc/memacc/dead",
            "WaitpidCompletionClaimed",
            "ExitInProgress",
            "ExitNotifyReady",
            "WakeupPending",
            "ps -eo pid,ppid,stat,pcpu,pmem,comm,args",
            "tail -n 120",
            "stat -c %s",
            'printf \'skipped\\n\'',
            "start_log_heartbeat()",
            "stop_log_heartbeat()",
            "validate_runtime_settings()",
            "run_git_http()",
            'GIT_TERMINAL_PROMPT=0',
            'GIT_HTTP_LOW_SPEED_LIMIT="$git_http_low_speed_limit"',
            'GIT_HTTP_LOW_SPEED_TIME="$git_http_low_speed_time"',
            'git \\',
            '-c http.lowSpeedLimit="$git_http_low_speed_limit"',
            '-c http.lowSpeedTime="$git_http_low_speed_time"',
            'log "heartbeat=${heartbeat_interval}s tail=${heartbeat_tail} stall_snapshots=${heartbeat_stall_snapshots} sync=${heartbeat_sync}"',
            'start_log_heartbeat "$phase" "$label" "$output"',
            'start_log_heartbeat "step" "$name" "$output"',
            'local previous_bytes=""',
            "stalled_ticks=0",
            'bytes="$(heartbeat_file_bytes "$output")"',
            'lines="$(heartbeat_file_lines "$output")"',
            'progress="progress $phase $label log=$output bytes=$bytes lines=$lines"',
            'if [ "$bytes" = "$previous_bytes" ]; then',
            'stalled_ticks=$((stalled_ticks + 1))',
            'heartbeat_append_progress "$progress"',
            'stall="no log growth for $stalled_ticks heartbeat intervals during $phase $label"',
            'heartbeat_append_progress "$stall"',
            'heartbeat_process_snapshot "$phase" "$label"',
            'log "$progress"',
            'tail -n "$heartbeat_tail" "$output" 2>/dev/null',
            'sync || true',
            'output="$(log_path_for step "$name")"',
            '"$@" >"$output" 2>&1',
            'WOS_SELFHOST_LOG_DIR="$log_dir"',
            "WOS_SELFHOST_LOG_DIR=$(shell_quote \"$log_dir\")",
            'WOS_SELFHOST_HEARTBEAT_INTERVAL="$heartbeat_interval"',
            'WOS_SELFHOST_HEARTBEAT_TAIL="$heartbeat_tail"',
            'WOS_SELFHOST_HEARTBEAT_STALL_SNAPSHOTS="$heartbeat_stall_snapshots"',
            'WOS_SELFHOST_HEARTBEAT_SYNC="$heartbeat_sync"',
            "WOS_SELFHOST_HEARTBEAT_INTERVAL=$(shell_quote \"$heartbeat_interval\")",
            "WOS_SELFHOST_HEARTBEAT_TAIL=$(shell_quote \"$heartbeat_tail\")",
            "WOS_SELFHOST_HEARTBEAT_STALL_SNAPSHOTS=$(shell_quote \"$heartbeat_stall_snapshots\")",
            "WOS_SELFHOST_HEARTBEAT_SYNC=$(shell_quote \"$heartbeat_sync\")",
            "write_timing_header()",
            "record_timing()",
            "run_timed_event()",
            '"run_id" "timestamp_utc" "mode" "phase" "label" "elapsed_ms"',
            '"status" "repo" "commit" "target" "jobs" "full_history"',
            'record_timing "metadata" "checkout_commit" "0" "ok"',
            'run_timed_event "clone" "submodule_init"',
            'run_timed_event "clone" "submodules"',
            'run_timed_event "clone" "submodule_status"',
            'record_timing "step" "$name" "$elapsed" "ok"',
            'record_timing "step" "$name" "$elapsed" "fail:$status"',
            'record_timing "step" "total" "$total_elapsed" "ok"',
            'bootstrap_detail_report="$workdir/bootstrap-detail.tsv"',
            'WOS_BOOTSTRAP_DETAIL_TSV="$bootstrap_detail_report"',
            'WOS_SELFHOST_HISTORY_FILE="$history_file"',
            "WOS_SELFHOST_HISTORY_FILE=$(shell_quote \"$history_file\")",
        ],
        "WOS self-host benchmark detailed historical timing records",
    )

    heartbeat_body = source[source.index("start_log_heartbeat()") : source.index("stop_log_heartbeat()")]
    if "sync || true" in heartbeat_body:
        fail("self-host heartbeat loop must not run global sync while clone/build commands are active")


def test_selfhost_runner_timing_avoids_python_when_epochrealtime_exists() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            "now_ms()",
            "timestamp_utc()",
            'local epoch="${EPOCHREALTIME:-}"',
            'if [ -n "$epoch" ]; then',
            'TZ=UTC printf -v prefix',
            'printf \'%s\\n\' "$((10#$seconds * 1000 + 10#${fraction:0:3}))"',
            "python3 - <<'PY'",
        ],
        "WOS self-host benchmark timing fast path",
    )

    now_start = source.index("now_ms()")
    log_start = source.index("log()", now_start)
    now_block = source[now_start:log_start]
    if now_block.find("python3 - <<'PY'") < now_block.find('if [ -n "$epoch" ]; then'):
        fail("self-host now_ms must try EPOCHREALTIME before Python")

    timestamp_start = source.index("timestamp_utc()")
    ensure_parent_start = source.index("ensure_parent_dir()", timestamp_start)
    timestamp_block = source[timestamp_start:ensure_parent_start]
    if timestamp_block.find("python3 - <<'PY'") < timestamp_block.find('if [ -n "$epoch" ]; then'):
        fail("self-host timestamp_utc must try EPOCHREALTIME before Python")


def test_selfhost_runner_keeps_remote_and_scratch_handling_guarded() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            "shell_quote()",
            'remote_env="env"',
            'remote_command="payload=\\${TMPDIR:-/tmp}/wos-selfhost-build.\\$\\$.sh"',
            'remote_command+="; cat > \\"\\$payload\\" || exit \\$?"',
            'remote_command+="; $remote_env bash \\"\\$payload\\""',
            "remote_env+=\" WOS_SELFHOST_PRIORITY_RESET=0\"",
            'remote_command+="; rm -f \\"\\$payload\\""',
            'selfhost_payload | "$WOS_SSH" "$host" "$remote_command"',
            "/tmp|/tmp/|/var/tmp|/var/tmp/)",
            "/root/wos-selfhost-*|/home/*/wos-selfhost-*)",
            "refusing to replace temporary directory root",
            '[[ "$jobs" =~ ^[1-9][0-9]*$ ]]',
            'reject_whitespace "--history-file" "$history_file"',
            'reject_whitespace "--log-dir" "$log_dir"',
            'reject_whitespace "--source-cache" "$source_cache"',
            'reject_whitespace "--mirror-local-path" "$mirror_local_path"',
            "source cache must be outside the scratch workdir",
            '[[ "$heartbeat_interval" =~ ^[0-9]+$ ]]',
            '[[ "$heartbeat_tail" =~ ^[0-9]+$ ]]',
            '[[ "$heartbeat_stall_snapshots" =~ ^[0-9]+$ ]]',
        ],
        "WOS self-host benchmark guarded remote and scratch handling",
    )

    skip_block_start = source.find('if [ "$skip_bootstrap" = "1" ]; then')
    skip_block_end = source.find("fi", skip_block_start)
    if skip_block_start < 0 or skip_block_end < 0:
        fail("bootstrap skip branch is missing")
    skip_block = source[skip_block_start:skip_block_end]
    if ">> \"$report\"" in skip_block:
        fail("bootstrap skip branch must leave timing output to time_step")
    if "< <(" in source:
        fail("WOS self-host benchmark must avoid Bash process substitution for WOS shell compatibility")
    if 'case "$workdir" in' in source:
        fail("destructive scratch allowlist must validate the canonical workdir")


def test_selfhost_report_comparator_checks_clone_build_and_total() -> None:
    source = SELFHOST_COMPARE.read_text()
    require_tokens(
        source,
        [
            'DEFAULT_STEPS = ("clone_sources", "build_wos", "total")',
            'FULL_PROCESS_ACCEPTANCE_PROFILE = "full-process"',
            "FULL_PROCESS_MAX_WOS_RATIO = 1.10",
            '"metric": "complete_outer_wall_ms"',
            '"submodules_sha256",',
            '"mirror_commit",',
            '"distdir_enabled",',
            '"distdir_manifest_sha256",',
            '"timed_out",',
            '"provenance": provenance',
            "full-process acceptance ratio cannot exceed",
            "--max-wos-ratio",
            "ratio = wos_ms / linux_ms",
            "--json-output",
            "return 0 if result[\"pass\"] else 1",
        ],
        "WOS self-host report comparator",
    )

    with tempfile.TemporaryDirectory(prefix="wos-selfhost-compare-") as tmp:
        tmp_path = Path(tmp)
        wos_report = tmp_path / "wos.tsv"
        linux_report = tmp_path / "linux.tsv"
        output_json = tmp_path / "comparison.json"
        wos_report.write_text(
            "clone_sources\t1100\nbootstrap_toolchain\t2000\nconfigure_wos\t300\nbuild_wos\t12000\ntotal\t15400\n",
            encoding="ascii",
        )
        linux_report.write_text(
            "clone_sources\t1000\nbootstrap_toolchain\t1900\nconfigure_wos\t280\nbuild_wos\t10000\ntotal\t13180\n",
            encoding="ascii",
        )
        result = subprocess.run(
            [
                str(SELFHOST_COMPARE),
                "--wos",
                str(wos_report),
                "--linux",
                str(linux_report),
                "--max-wos-ratio",
                "1.25",
                "--json-output",
                str(output_json),
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            fail(f"self-host comparator rejected acceptable reports: {result.stderr or result.stdout}")
        data = json.loads(output_json.read_text(encoding="ascii"))
        if not data["pass"]:
            fail(f"self-host comparator JSON did not record pass: {data!r}")

        wos_report.write_text("clone_sources\t1400\nbuild_wos\t13000\ntotal\t14400\n", encoding="ascii")
        linux_report.write_text("clone_sources\t1000\nbuild_wos\t10000\ntotal\t11000\n", encoding="ascii")
        result = subprocess.run(
            [
                str(SELFHOST_COMPARE),
                "--wos",
                str(wos_report),
                "--linux",
                str(linux_report),
                "--max-wos-ratio",
                "1.25",
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0:
            fail("self-host comparator accepted WOS reports beyond the ratio threshold")

        wos_runs = tmp_path / "wos-runs.tsv"
        linux_runs = tmp_path / "linux-runs.tsv"
        full_process_json = tmp_path / "full-process.json"
        runs_header = (
            "run\twall_ms\trunner_status\tevidence_status\taccepted\tcommit\t"
            "submodules_sha256\trepo\tmirror_commit\tdistdir_enabled\t"
            "distdir_manifest_sha256\ttimed_out\n"
        )
        provenance = f"{'a' * 40}\t{'b' * 64}\thttps://github.com/Pascu-Victor/wos.git\t\t1\t{'e' * 64}\t0"
        wos_runs.write_text(
            runs_header + f"1\t1100\t0\t0\t1\t{provenance}\n2\t1090\t0\t0\t1\t{provenance}\n",
            encoding="ascii",
        )
        linux_runs.write_text(runs_header + f"baseline\t1000\t0\t0\t1\t{provenance}\n", encoding="ascii")
        result = subprocess.run(
            [
                str(SELFHOST_COMPARE),
                "--wos-runs",
                str(wos_runs),
                "--linux-runs",
                str(linux_runs),
                "--acceptance-profile",
                "full-process",
                "--json-output",
                str(full_process_json),
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            fail(f"full-process comparator rejected the 1.10 ceiling: {result.stderr or result.stdout}")
        data = json.loads(full_process_json.read_text(encoding="ascii"))
        if data.get("metric") != "complete_outer_wall_ms" or data.get("max_wos_ratio") != 1.1:
            fail(f"full-process comparator did not record outer-wall acceptance: {data!r}")
        if len(data.get("steps", [])) != 2 or not all(row["pass"] for row in data["steps"]):
            fail(f"full-process comparator did not check every WOS run: {data!r}")

        wos_runs.write_text(runs_header + f"1\t1101\t0\t0\t1\t{provenance}\n", encoding="ascii")
        result = subprocess.run(
            [
                str(SELFHOST_COMPARE),
                "--wos-runs",
                str(wos_runs),
                "--linux-runs",
                str(linux_runs),
                "--acceptance-profile",
                "full-process",
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0 or "1.101 > 1.100" not in result.stderr:
            fail("full-process comparator accepted an outer-wall ratio above 1.10")

        result = subprocess.run(
            [
                str(SELFHOST_COMPARE),
                "--wos-runs",
                str(wos_runs),
                "--linux-runs",
                str(linux_runs),
                "--acceptance-profile",
                "full-process",
                "--max-wos-ratio",
                "1.11",
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0 or "cannot exceed 1.10" not in result.stderr:
            fail("full-process comparator allowed the acceptance ceiling to be relaxed")

        provenance_mismatches = [
            (
                f"{'c' * 40}\t{'b' * 64}\thttps://github.com/Pascu-Victor/wos.git\t\t1\t{'e' * 64}\t0",
                "identical commit provenance",
            ),
            (
                f"{'a' * 40}\t{'d' * 64}\thttps://github.com/Pascu-Victor/wos.git\t\t1\t{'e' * 64}\t0",
                "identical submodules_sha256 provenance",
            ),
            (
                f"{'a' * 40}\t{'b' * 64}\thttps://example.invalid/wos.git\t\t1\t{'e' * 64}\t0",
                "identical repo provenance",
            ),
            (
                f"{'a' * 40}\t{'b' * 64}\thttps://github.com/Pascu-Victor/wos.git\t\t0\t\t0",
                "identical distdir_enabled provenance",
            ),
            (
                f"{'a' * 40}\t{'b' * 64}\thttps://github.com/Pascu-Victor/wos.git\t\t1\t{'f' * 64}\t0",
                "identical distdir_manifest_sha256 provenance",
            ),
            (
                f"{'a' * 40}\t{'b' * 64}\thttps://github.com/Pascu-Victor/wos.git\t{'a' * 40}\t1\t{'e' * 64}\t0",
                "same direct-or-mirror source mode",
            ),
        ]
        wos_runs.write_text(runs_header + f"1\t1100\t0\t0\t1\t{provenance}\n", encoding="ascii")
        for bad_provenance, expected_error in provenance_mismatches:
            linux_runs.write_text(
                runs_header + f"baseline\t1000\t0\t0\t1\t{bad_provenance}\n",
                encoding="ascii",
            )
            result = subprocess.run(
                [
                    str(SELFHOST_COMPARE),
                    "--wos-runs",
                    str(wos_runs),
                    "--linux-runs",
                    str(linux_runs),
                    "--acceptance-profile",
                    "full-process",
                ],
                cwd=ROOT,
                check=False,
                text=True,
                capture_output=True,
            )
            if result.returncode == 0 or expected_error not in result.stderr:
                fail(f"full-process comparator accepted mismatched provenance: {bad_provenance!r}")

        bad_mirror_provenance = (
            f"{'a' * 40}\t{'b' * 64}\thttps://github.com/Pascu-Victor/wos.git\t{'c' * 40}\t1\t{'e' * 64}\t0"
        )
        wos_runs.write_text(
            runs_header + f"1\t1100\t0\t0\t1\t{bad_mirror_provenance}\n",
            encoding="ascii",
        )
        linux_runs.write_text(
            runs_header + f"baseline\t1000\t0\t0\t1\t{bad_mirror_provenance}\n",
            encoding="ascii",
        )
        result = subprocess.run(
            [
                str(SELFHOST_COMPARE),
                "--wos-runs",
                str(wos_runs),
                "--linux-runs",
                str(linux_runs),
                "--acceptance-profile",
                "full-process",
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0 or "mirror HEAD provenance" not in result.stderr:
            fail("full-process comparator accepted a mirror HEAD different from the checkout commit")

        result = subprocess.run(
            [
                str(SELFHOST_COMPARE),
                "--wos",
                str(wos_report),
                "--linux",
                str(linux_report),
                "--acceptance-profile",
                "full-process",
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0 or "inner selfhost-report.tsv totals do not include complete outer wall time" not in result.stderr:
            fail("full-process comparator accepted unsafe inner phase totals")


if __name__ == "__main__":
    test_selfhost_runner_covers_acceptance_flow()
    test_wos_bootstrap_distributes_only_compiler_processes()
    test_wos_bootstrap_repairs_only_link_output_mode()
    test_wos_bootstrap_caps_concurrent_compilers_per_host()
    test_wos_bootstrap_remote_response_file_preserves_arguments()
    test_wos_bootstrap_keeps_small_preprocessed_inputs_local()
    test_wos_bootstrap_compiles_submitter_jobs_once()
    test_wos_bootstrap_retries_failed_remote_compiler_locally()
    test_distributed_compiler_hosts_are_validated_before_launch()
    test_selfhost_repeatability_runner_preserves_acceptance_evidence()
    test_linux_selfhost_baseline_preserves_full_process_evidence()
    test_selfhost_runner_verifies_toolchain_kernel_and_utilities()
    test_selfhost_runner_preflights_wos_only_self_host_tools()
    test_selfhost_runner_scrubs_inherited_toolchain_environment()
    test_rootfs_stages_self_hosting_tablegen_tools()
    test_rootfs_stages_git_helpers_at_configured_runtime_paths()
    test_selfhost_runner_mirror_is_optional_not_default()
    test_selfhost_runner_source_cache_is_iteration_only()
    test_selfhost_runner_resume_checkout_is_validation_only()
    test_selfhost_debug_mirror_path_is_shallow_and_not_source_tree_copy()
    test_selfhost_runner_records_detailed_historical_timings()
    test_selfhost_runner_timing_avoids_python_when_epochrealtime_exists()
    test_selfhost_runner_keeps_remote_and_scratch_handling_guarded()
    test_distributed_cluster_has_plausible_per_core_memory()
    test_distributed_selfhost_cluster_balances_cpu_and_memory()
    test_selfhost_report_comparator_checks_clone_build_and_total()
    print("WOS self-host benchmark source invariants hold")
