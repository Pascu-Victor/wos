#!/usr/bin/env python3

import json
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SELFHOST_RUNNER = ROOT / "scripts" / "bench" / "run_wos_selfhost_build.sh"
SELFHOST_COMPARE = ROOT / "scripts" / "bench" / "compare_wos_selfhost_reports.py"
SELFHOST_CLUSTER = ROOT / "configs" / "cluster_selfhost.json"
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
            'DEFAULT_LINUX_WORKDIR="/tmp/wos-selfhost-bench"',
            'DEFAULT_WOS_WORKDIR="/root/wos-selfhost-bench"',
            'DEFAULT_JOBS="32"',
            "wos-local",
            "Direct GitHub cloning is the default",
            "--source-cache is only for faster debugging",
            "buildability, not Git history traversal throughput",
            "bin/wos-cluster --config configs/cluster_selfhost.json --launch --no-setup",
            "bin/wos-ktest --no-setup",
            "clone_cmd=(git clone)",
            "run_timed_event \"clone\" \"wos_repo\" run_git_http",
            'local submodule_list="$workdir/submodule-paths.tsv"',
            "git -C \"$checkout\" config --file .gitmodules --get-regexp '^submodule\\..*\\.path$' > \"$submodule_list\"",
            "while read -r key path; do",
            "local submodule_cmd=(git -C \"$checkout\" submodule update --init --recursive)",
            "submodule_cmd+=(--depth 1)",
            'submodule_cmd+=(--jobs "$jobs" -- "$path")',
            'run_timed_event "clone_submodule" "$path" run_git_http',
            "--full-history",
            "full_history=0",
            'jobs="${WOS_SELFHOST_JOBS:-32}"',
            'jobs="$DEFAULT_JOBS"',
            'WOS_BUILD_JOBS="$jobs"',
            'WOS_NINJA_JOBS="$jobs"',
            'WOS_MAKE_JOBS="$jobs"',
            "unset WOS_BUILD_JOBS WOS_MAKE_JOBS WOS_NINJA_JOBS CMAKE_BUILD_PARALLEL_LEVEL",
            "./tools/bootstrap.sh",
            "cmake -GNinja",
            "-DWOS_BUILD_WOSDBG=OFF",
            "-DWOS_BUILD_CMAKE_FOR_HOST=OFF",
            "-DWOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN=ON",
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
            "llvm-readelf llvm-objdump llvm-tblgen clang-tblgen",
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
            "copy\ttoolchain/sysroot/bin/llvm-tblgen\t/usr/bin/llvm-tblgen",
            "copy\ttoolchain/sysroot/bin/clang-tblgen\t/usr/bin/clang-tblgen",
        ],
        "WOS rootfs self-hosting tablegen staging",
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
            'WOS_SOURCE_DISTDIR="$distdir" run_with_jobs_env ./tools/bootstrap.sh',
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
            'submodule_cmd+=(--jobs "$jobs" -- "$path")',
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
            'log_dir="$workdir/logs"',
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
            'run_timed_event "clone_submodule" "$path"',
            'run_timed_event "clone" "submodule_status"',
            'record_timing "step" "$name" "$elapsed" "ok"',
            'record_timing "step" "$name" "$elapsed" "fail:$status"',
            'record_timing "step" "total" "$total_elapsed" "ok"',
            'WOS_SELFHOST_HISTORY_FILE="$history_file"',
            "WOS_SELFHOST_HISTORY_FILE=$(shell_quote \"$history_file\")",
        ],
        "WOS self-host benchmark detailed historical timing records",
    )

    heartbeat_body = source[source.index("start_log_heartbeat()") : source.index("stop_log_heartbeat()")]
    if "sync || true" in heartbeat_body:
        fail("self-host heartbeat loop must not run global sync while clone/build commands are active")


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


def test_selfhost_report_comparator_checks_clone_build_and_total() -> None:
    source = SELFHOST_COMPARE.read_text()
    require_tokens(
        source,
        [
            'DEFAULT_STEPS = ("clone_sources", "build_wos", "total")',
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


if __name__ == "__main__":
    test_selfhost_runner_covers_acceptance_flow()
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
    test_selfhost_runner_keeps_remote_and_scratch_handling_guarded()
    test_selfhost_report_comparator_checks_clone_build_and_total()
    print("WOS self-host benchmark source invariants hold")
