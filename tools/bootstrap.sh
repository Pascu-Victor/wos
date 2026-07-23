#!/bin/bash
# Full WOS toolchain bootstrap.
# Builds the host compiler (clang/lld) when needed, then the WOS target
# toolchain. On native WOS hosts, the system image already ships clang/lld, so
# bootstrap creates a local compatibility shim instead of rebuilding LLVM.
#
# Usage: bootstrap.sh
#   Run from the WOS workspace root directory.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WOS_TARGET_ARCH="x86_64-pc-wos"
WOS_CLANG_VERSION="22"
WOS_BOOTSTRAP_HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"
WOS_HOST_TOOLCHAIN_ROOT="$WORKSPACE_ROOT/toolchain/host"

cd "$WORKSPACE_ROOT"

strip_path_entry() {
    local input="$1"
    local remove="$2"
    local out=""
    local part

    IFS=':' read -r -a _wos_bootstrap_parts <<< "$input"
    for part in "${_wos_bootstrap_parts[@]}"; do
        if [ -n "$part" ] && [ "$part" != "$remove" ]; then
            if [ -n "$out" ]; then
                out="$out:$part"
            else
                out="$part"
            fi
        fi
    done

    printf '%s' "$out"
}

system_path() {
    local clean_path="${PATH:-}"

    clean_path="$(strip_path_entry "$clean_path" "$WORKSPACE_ROOT/toolchain/host/bin")"
    clean_path="$(strip_path_entry "$clean_path" "$WORKSPACE_ROOT/toolchain/wos-host-shim/bin")"
    clean_path="$(strip_path_entry "$clean_path" "$WORKSPACE_ROOT/tools/build/bin")"
    clean_path="$(strip_path_entry "$clean_path" "$WORKSPACE_ROOT/bin")"

    printf '%s' "$clean_path"
}

find_system_tool() {
    local tool="$1"
    local clean_path
    local path

    clean_path="$(system_path)"
    path="$(PATH="$clean_path" command -v "$tool" 2>/dev/null || true)"
    if [ -z "$path" ]; then
        echo "ERROR: required system tool '$tool' was not found in PATH." >&2
        exit 1
    fi

    printf '%s\n' "$path"
}

wos_host_tool_is_usable() {
    local path="$1"

    if [ "$WOS_BOOTSTRAP_HOST_SYSTEM" = "WOS" ]; then
        [ -f "$path" ] && "$path" --version >/dev/null 2>&1
        return $?
    fi

    [ -x "$path" ]
}

write_clang_wrapper() {
    local output="$1"
    local system_clang="$2"
    local resource_dir="$3"

    cat > "$output" << EOF
#!/bin/bash
config_dir="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
config="\$config_dir/$WOS_TARGET_ARCH.cfg"
use_config=0
next_is_target=0
next_is_output=0
compile_only=0
link_output=1
output_file=""
for arg in "\$@"; do
    if [ "\$next_is_output" -eq 1 ]; then
        output_file="\$arg"
        next_is_output=0
        continue
    fi
    if [ "\$next_is_target" -eq 1 ]; then
        if [ "\$arg" = "$WOS_TARGET_ARCH" ]; then
            use_config=1
        fi
        next_is_target=0
        continue
    fi

    case "\$arg" in
        -o)
            next_is_output=1
            ;;
        -c)
            compile_only=1
            link_output=0
            ;;
        -E|-S|-M|-MM|-fsyntax-only|--analyze)
            link_output=0
            ;;
        --target=$WOS_TARGET_ARCH|-target=$WOS_TARGET_ARCH)
            use_config=1
            ;;
        --target|-target)
            next_is_target=1
            ;;
    esac
done

compiler=("$system_clang" -resource-dir "$resource_dir")
if [ "\$use_config" -eq 1 ] && [ -f "\$config" ]; then
    compiler+=(--config="\$config")
fi

if [ "\${WOS_DISTRIBUTED_COMPILER:-0}" = "1" ] && [ "\$compile_only" -eq 1 ]; then
    IFS=, read -r -a compiler_hosts <<< "\${WOS_DISTRIBUTED_COMPILER_HOSTS:-}"
    if [ "\${#compiler_hosts[@]}" -lt 2 ]; then
        echo "ERROR: distributed compiler requires at least two WOS hosts" >&2
        exit 1
    fi
    compiler_state="\${WOS_DISTRIBUTED_COMPILER_STATE:-}"
    if [ -z "\$compiler_state" ]; then
        echo "ERROR: distributed compiler state path is missing" >&2
        exit 1
    fi
    compiler_transport="\${WOS_DISTRIBUTED_COMPILER_TRANSPORT:-source}"
    case "\$compiler_transport" in
        source|preprocessed|rewritten)
            ;;
        *)
            echo "ERROR: distributed compiler transport must be 'source', 'preprocessed', or 'rewritten'" >&2
            exit 1
            ;;
    esac
    compiler_slot_pause=(sleep 0)
    if command -v usleep >/dev/null 2>&1; then
        compiler_slot_pause=(usleep 1000)
    fi
    if [ "\$compiler_transport" = source ]; then
        compiler_total_jobs="\${WOS_NINJA_JOBS:-\${WOS_BUILD_JOBS:-}}"
        case "\$compiler_total_jobs" in
            ''|*[!0-9]*|0)
                compiler_total_jobs=32
                ;;
        esac
        compiler_jobs_per_host="\${WOS_DISTRIBUTED_COMPILER_JOBS_PER_HOST:-}"
        if [ -z "\$compiler_jobs_per_host" ]; then
            compiler_jobs_per_host="\$(((compiler_total_jobs + \${#compiler_hosts[@]} - 1) / \${#compiler_hosts[@]}))"
        fi
        case "\$compiler_jobs_per_host" in
            ''|*[!0-9]*|0)
                echo "ERROR: distributed compiler jobs per host must be a positive integer" >&2
                exit 1
                ;;
        esac
        compiler_local_jobs="\${WOS_DISTRIBUTED_COMPILER_LOCAL_JOBS:-\$compiler_jobs_per_host}"
        compiler_remote_jobs_per_host="\${WOS_DISTRIBUTED_COMPILER_REMOTE_JOBS_PER_HOST:-\$compiler_jobs_per_host}"
        case "\$compiler_local_jobs" in
            ''|*[!0-9]*|0)
                echo "ERROR: distributed compiler local jobs must be a positive integer" >&2
                exit 1
                ;;
        esac
        case "\$compiler_remote_jobs_per_host" in
            ''|*[!0-9]*|0)
                echo "ERROR: distributed compiler remote jobs per host must be a positive integer" >&2
                exit 1
                ;;
        esac
        compiler_slots="\$compiler_state.source-slots"
        compiler_successes="\$compiler_state.successes"
        if ! mkdir -p "\$compiler_slots" "\$compiler_successes"; then
            echo "ERROR: distributed compiler state directories could not be created" >&2
            exit 1
        fi
        compiler_record_success() {
            local success_index="\$1"
            local success_host="\$2"
            if ! printf '%s\n' "\$success_host" > "\$compiler_successes/\$success_index"; then
                echo "warning: distributed compiler could not record successful host \$success_host" >&2
            fi
        }
        compiler_host=""
        compiler_slot=""
        # Slot directories are the admission authority, so selecting a host
        # does not need a global lock. A shared next-host file made every
        # compiler serialize on the submitter before any work could be sent to
        # a peer; under oversubscription, wrappers spent whole cores repeatedly
        # spawning `sleep 0` while the peer CPUs sat idle. Start each wrapper at
        # an independently mixed host and use atomic mkdir claims directly.
        compiler_start_index="\$((RANDOM % \${#compiler_hosts[@]}))"
        while [ -z "\$compiler_slot" ]; do
            for ((compiler_offset = 0; compiler_offset < \${#compiler_hosts[@]}; compiler_offset++)); do
                compiler_candidate_index="\$(((compiler_start_index + compiler_offset) % \${#compiler_hosts[@]}))"
                compiler_host_slots="\$compiler_slots/\$compiler_candidate_index"
                if ! mkdir -p "\$compiler_host_slots"; then
                    echo "ERROR: distributed compiler host slot directory could not be created" >&2
                    exit 1
                fi
                compiler_candidate_jobs="\$compiler_remote_jobs_per_host"
                if [ "\$compiler_candidate_index" -eq 0 ]; then
                    compiler_candidate_jobs="\$compiler_local_jobs"
                fi
                for ((compiler_slot_index = 0; compiler_slot_index < compiler_candidate_jobs; compiler_slot_index++)); do
                    compiler_candidate_slot="\$compiler_host_slots/\$compiler_slot_index"
                    if mkdir "\$compiler_candidate_slot" 2>/dev/null; then
                        compiler_host="\${compiler_hosts[\$compiler_candidate_index]}"
                        compiler_slot="\$compiler_candidate_slot"
                        break 2
                    fi
                done
            done
            if [ -z "\$compiler_slot" ]; then
                compiler_start_index="\$(((compiler_start_index + 1) % \${#compiler_hosts[@]}))"
                "\${compiler_slot_pause[@]}"
            fi
        done
        compiler_slot_release() {
            rmdir "\$compiler_slot" 2>/dev/null || true
        }
        trap compiler_slot_release EXIT HUP INT TERM
        if [ "\$compiler_candidate_index" -eq 0 ]; then
            if "\${compiler[@]}" "\$@"; then
                compiler_status=0
                compiler_record_success 0 "\${compiler_hosts[0]}"
            else
                compiler_status=\$?
            fi
            compiler_slot_release
            trap - EXIT HUP INT TERM
            exit "\$compiler_status"
        fi
        compiler_responses="\$compiler_state.responses"
        if ! mkdir -p "\$compiler_responses"; then
            echo "ERROR: distributed compiler response directory could not be created" >&2
            exit 1
        fi
        compiler_response="\$(mktemp "\$compiler_responses/clang.XXXXXX")"
        if [ -z "\$compiler_response" ]; then
            echo "ERROR: distributed compiler response file could not be created" >&2
            exit 1
        fi
        # Keep response files until the scratch workdir is removed. Concurrent
        # forwarded unlink operations can leave peers with stale lookups.
        for arg in "\$@"; do
            if ! printf '%q\n' "\$arg" >> "\$compiler_response"; then
                echo "ERROR: distributed compiler response file could not be written" >&2
                exit 1
            fi
        done
        compiler_remote_path="\${PATH:-/usr/bin:/bin}"
        if env -i PATH="\$compiler_remote_path" HOME="\${HOME:-/root}" TMPDIR="\${TMPDIR:-/tmp}" TZ=UTC0 \
            on "\$compiler_host" "\${compiler[@]}" -fno-temp-file "@\$compiler_response"; then
            compiler_status=0
        else
            compiler_status=\$?
        fi
        compiler_slot_release
        trap - EXIT HUP INT TERM
        if [ "\$compiler_status" -eq 0 ]; then
            compiler_record_success "\$compiler_candidate_index" "\$compiler_host"
            exit 0
        fi
        echo "warning: distributed compiler on \$compiler_host failed with status \$compiler_status; retrying locally" >&2
        if "\${compiler[@]}" "\$@"; then
            compiler_status=0
            compiler_record_success 0 "\${compiler_hosts[0]}"
        else
            compiler_status=\$?
        fi
        exit "\$compiler_status"
    fi
    compiler_total_jobs="\${WOS_NINJA_JOBS:-\${WOS_BUILD_JOBS:-}}"
    case "\$compiler_total_jobs" in
        ''|*[!0-9]*|0)
            compiler_local_jobs=8
            ;;
        *)
            compiler_local_jobs="\$compiler_total_jobs"
            ;;
    esac
    # The self-host runner validates that compiler_hosts[0] is the submitter
    # and supplies separate local, preprocessing, and peer limits for reusable
    # slots. Persistent proof-only slots retain the full local Ninja width.
    compiler_jobs_per_host="\${WOS_DISTRIBUTED_COMPILER_JOBS_PER_HOST:-}"
    case "\$compiler_jobs_per_host" in
        '')
            compiler_remote_jobs_per_host="\${WOS_DISTRIBUTED_COMPILER_REMOTE_JOBS_PER_HOST:-1}"
            compiler_persist_remote_slots=1
            ;;
        *[!0-9]*|0)
            echo "ERROR: distributed compiler jobs per host must be a positive integer" >&2
            exit 1
            ;;
        *)
            compiler_local_jobs="\${WOS_DISTRIBUTED_COMPILER_LOCAL_JOBS:-\$compiler_jobs_per_host}"
            compiler_remote_jobs_per_host="\${WOS_DISTRIBUTED_COMPILER_REMOTE_JOBS_PER_HOST:-\$compiler_jobs_per_host}"
            compiler_persist_remote_slots=0
            ;;
    esac
    case "\$compiler_local_jobs" in
        ''|*[!0-9]*|0)
            echo "ERROR: distributed compiler local jobs must be a positive integer" >&2
            exit 1
            ;;
    esac
    case "\$compiler_remote_jobs_per_host" in
        ''|*[!0-9]*|0)
            echo "ERROR: distributed compiler remote jobs per host must be a positive integer" >&2
            exit 1
            ;;
    esac
    compiler_preprocess_jobs="\${WOS_DISTRIBUTED_COMPILER_PREPROCESS_JOBS:-\$compiler_local_jobs}"
    case "\$compiler_preprocess_jobs" in
        ''|*[!0-9]*|0)
            echo "ERROR: distributed compiler preprocess jobs must be a positive integer" >&2
            exit 1
            ;;
    esac
    # Default to dispatching the bounded peer job immediately. Deferring small
    # inputs would preprocess three out of every four jobs locally until a
    # large source happened to occupy each persistent peer slot.
    compiler_min_preprocessed_bytes="\${WOS_DISTRIBUTED_COMPILER_MIN_PREPROCESSED_BYTES:-0}"
    case "\$compiler_min_preprocessed_bytes" in
        ''|*[!0-9]*)
            echo "ERROR: distributed compiler minimum preprocessed size must be a non-negative integer" >&2
            exit 1
            ;;
    esac
    compiler_args=("\$@")
    compiler_source_index=-1
    compiler_source_count=0
    compiler_explicit_language=""
    compiler_skip_arg=0
    for ((compiler_arg_index = 0; compiler_arg_index < \${#compiler_args[@]}; compiler_arg_index++)); do
        arg="\${compiler_args[\$compiler_arg_index]}"
        if [ "\$compiler_skip_arg" -eq 1 ]; then
            compiler_skip_arg=0
            continue
        fi
        case "\$arg" in
            -x)
                if [ "\$((compiler_arg_index + 1))" -lt "\${#compiler_args[@]}" ]; then
                    compiler_explicit_language="\${compiler_args[\$((compiler_arg_index + 1))]}"
                fi
                compiler_skip_arg=1
                ;;
            -x*)
                compiler_explicit_language="\${arg#-x}"
                ;;
            -o|-MF|-MT|-MQ|-MJ|-I|-isystem|-include|-imacros|-iquote|-idirafter|-iprefix|-iwithprefix|-isysroot|--sysroot|-target|--target|-resource-dir|-Xclang|-mllvm)
                compiler_skip_arg=1
                ;;
            -*)
                ;;
            *)
                if [ -f "\$arg" ]; then
                    case "\$compiler_explicit_language:\$arg" in
                        c:*|c++:*|objective-c:*|objective-c++:*|*:*.c|*:*.cc|*:*.cp|*:*.cpp|*:*.cxx|*:*.C|*:*.m|*:*.mm)
                            compiler_source_index="\$compiler_arg_index"
                            compiler_source_count="\$((compiler_source_count + 1))"
                            ;;
                    esac
                fi
                ;;
        esac
    done
    compiler_preprocessed_language=""
    if [ "\$compiler_source_count" -eq 1 ]; then
        compiler_source="\${compiler_args[\$compiler_source_index]}"
        case "\$compiler_explicit_language" in
            c) compiler_preprocessed_language=cpp-output ;;
            c++) compiler_preprocessed_language=c++-cpp-output ;;
            objective-c) compiler_preprocessed_language=objective-c-cpp-output ;;
            objective-c++) compiler_preprocessed_language=objective-c++-cpp-output ;;
            '')
                case "\$compiler_source" in
                    *.c) compiler_preprocessed_language=cpp-output ;;
                    *.cc|*.cp|*.cpp|*.cxx|*.C) compiler_preprocessed_language=c++-cpp-output ;;
                    *.m) compiler_preprocessed_language=objective-c-cpp-output ;;
                    *.mm) compiler_preprocessed_language=objective-c++-cpp-output ;;
                esac
                ;;
        esac
    fi
    if [ -n "\$compiler_preprocessed_language" ]; then
        compiler_remote_language="\$compiler_preprocessed_language"
        compiler_preprocess_args=(-E)
        if [ "\$compiler_transport" = rewritten ]; then
            compiler_preprocess_args+=(-frewrite-includes)
            case "\$compiler_preprocessed_language" in
                cpp-output) compiler_remote_language=c ;;
                c++-cpp-output) compiler_remote_language=c++ ;;
                objective-c-cpp-output) compiler_remote_language=objective-c ;;
                objective-c++-cpp-output) compiler_remote_language=objective-c++ ;;
            esac
        fi
        compiler_slots="\$compiler_state.slots"
        if ! mkdir -p "\$compiler_slots"; then
            echo "ERROR: distributed compiler slot directory could not be created" >&2
            exit 1
        fi
        compiler_successes="\$compiler_state.successes"
        compiler_record_success() {
            local success_index="\$1"
            local success_host="\$2"
            if ! mkdir -p "\$compiler_successes" ||
               ! printf '%s\n' "\$success_host" > "\$compiler_successes/\$success_index"; then
                echo "warning: distributed compiler could not record successful host \$success_host" >&2
            fi
        }
        compiler_host=""
        compiler_slot=""
        compiler_slot_persistent=0
        if [ "\$compiler_persist_remote_slots" -eq 1 ]; then
            # The default mode needs only one successful compile per peer. Use
            # atomic directory creation to claim those jobs; local jobs must
            # never queue behind the shared reusable-slot scheduler.
            for ((compiler_candidate_index = 1; compiler_candidate_index < \${#compiler_hosts[@]}; compiler_candidate_index++)); do
                compiler_host_slots="\$compiler_slots/\$compiler_candidate_index"
                if ! mkdir -p "\$compiler_host_slots"; then
                    echo "ERROR: distributed compiler host slot directory could not be created" >&2
                    exit 1
                fi
                for ((compiler_slot_index = 0; compiler_slot_index < compiler_remote_jobs_per_host; compiler_slot_index++)); do
                    compiler_candidate_slot="\$compiler_host_slots/\$compiler_slot_index"
                    if mkdir "\$compiler_candidate_slot" 2>/dev/null; then
                        compiler_host="\${compiler_hosts[\$compiler_candidate_index]}"
                        compiler_slot="\$compiler_candidate_slot"
                        compiler_slot_persistent=1
                        break 2
                    fi
                done
            done
            if [ -z "\$compiler_slot" ]; then
                if "\${compiler[@]}" "\$@"; then
                    compiler_status=0
                    compiler_record_success 0 "\${compiler_hosts[0]}"
                else
                    compiler_status=\$?
                fi
                exit "\$compiler_status"
            fi
        else
            # Atomic slot directories are the admission authority. Avoid a
            # global next-host lock here: under oversubscription its spin loop
            # serialized preprocessing on the submitter while peer CPUs idled.
            compiler_start_index="\$((RANDOM % \${#compiler_hosts[@]}))"
            while [ -z "\$compiler_slot" ]; do
                for ((compiler_offset = 0; compiler_offset < \${#compiler_hosts[@]}; compiler_offset++)); do
                    compiler_candidate_index="\$(((compiler_start_index + compiler_offset) % \${#compiler_hosts[@]}))"
                    compiler_host_slots="\$compiler_slots/\$compiler_candidate_index"
                    if ! mkdir -p "\$compiler_host_slots"; then
                        echo "ERROR: distributed compiler host slot directory could not be created" >&2
                        exit 1
                    fi
                    compiler_candidate_jobs="\$compiler_remote_jobs_per_host"
                    if [ "\$compiler_candidate_index" -eq 0 ]; then
                        compiler_candidate_jobs="\$compiler_local_jobs"
                    fi
                    for ((compiler_slot_index = 0; compiler_slot_index < compiler_candidate_jobs; compiler_slot_index++)); do
                        compiler_candidate_slot="\$compiler_host_slots/\$compiler_slot_index"
                        if mkdir "\$compiler_candidate_slot" 2>/dev/null; then
                            compiler_host="\${compiler_hosts[\$compiler_candidate_index]}"
                            compiler_slot="\$compiler_candidate_slot"
                            break 2
                        fi
                    done
                done
                if [ -z "\$compiler_slot" ]; then
                    compiler_start_index="\$(((compiler_start_index + 1) % \${#compiler_hosts[@]}))"
                    "\${compiler_slot_pause[@]}"
                fi
            done
        fi
        compiler_slot_release() {
            rmdir "\$compiler_slot" 2>/dev/null || true
        }
        compiler_slot_cleanup() {
            if [ "\$compiler_slot_persistent" -eq 0 ]; then
                compiler_slot_release
            fi
        }
        if [ "\$compiler_candidate_index" -eq 0 ]; then
            trap compiler_slot_release EXIT HUP INT TERM
            if "\${compiler[@]}" "\$@"; then
                compiler_status=0
                compiler_record_success 0 "\${compiler_hosts[0]}"
            else
                compiler_status=\$?
            fi
            compiler_slot_release
            trap - EXIT HUP INT TERM
            exit "\$compiler_status"
        fi
        compiler_responses="\$compiler_state.responses"
        compiler_preprocess_slots="\$compiler_state.preprocess-slots"
        if ! mkdir -p "\$compiler_responses" "\$compiler_preprocess_slots"; then
            compiler_slot_release
            echo "ERROR: distributed compiler scratch directories could not be created" >&2
            exit 1
        fi
        # WOS mktemp currently races when many wrappers create directories at
        # once. Each wrapper is a distinct process, so its PID provides a
        # collision-free name without serializing preprocessing.
        compiler_job_dir="\$compiler_responses/clang-job.\$\$"
        if ! mkdir "\$compiler_job_dir" 2>/dev/null; then
            compiler_job_dir=""
        fi
        trap compiler_slot_release EXIT HUP INT TERM
        if [ -z "\$compiler_job_dir" ] || [ ! -d "\$compiler_job_dir" ]; then
            compiler_slot_release
            trap - EXIT HUP INT TERM
            echo "ERROR: distributed compiler job directory could not be created" >&2
            exit 1
        fi
        compiler_input="\$compiler_job_dir/input"
        compiler_preprocess_slot=""
        compiler_preprocess_slot_release() {
            if [ -n "\$compiler_preprocess_slot" ]; then
                rmdir "\$compiler_preprocess_slot" 2>/dev/null || true
                compiler_preprocess_slot=""
            fi
        }
        compiler_input_cleanup() {
            rm -f -- "\$compiler_input" 2>/dev/null || true
            rmdir "\$compiler_job_dir" 2>/dev/null || true
        }
        compiler_input_and_slot_cleanup() {
            compiler_input_cleanup
            compiler_preprocess_slot_release
            compiler_slot_release
        }
        trap compiler_input_and_slot_cleanup EXIT HUP INT TERM
        compiler_preprocess_start="\$((RANDOM % compiler_preprocess_jobs))"
        while [ -z "\$compiler_preprocess_slot" ]; do
            for ((compiler_preprocess_offset = 0; compiler_preprocess_offset < compiler_preprocess_jobs; compiler_preprocess_offset++)); do
                compiler_preprocess_index="\$(((compiler_preprocess_start + compiler_preprocess_offset) % compiler_preprocess_jobs))"
                compiler_preprocess_candidate="\$compiler_preprocess_slots/\$compiler_preprocess_index"
                if mkdir "\$compiler_preprocess_candidate" 2>/dev/null; then
                    compiler_preprocess_slot="\$compiler_preprocess_candidate"
                    break
                fi
            done
            if [ -z "\$compiler_preprocess_slot" ]; then
                compiler_preprocess_start="\$(((compiler_preprocess_start + 1) % compiler_preprocess_jobs))"
                "\${compiler_slot_pause[@]}"
            fi
        done
        if "\${compiler[@]}" "\$@" "\${compiler_preprocess_args[@]}" -o "\$compiler_input" -Wno-unused-command-line-argument; then
            compiler_status=0
        else
            compiler_status=\$?
            compiler_input_and_slot_cleanup
            trap - EXIT HUP INT TERM
            exit "\$compiler_status"
        fi
        compiler_preprocess_slot_release
        compiler_input_size="\$(stat -c %s -- "\$compiler_input" 2>/dev/null || true)"
        case "\$compiler_input_size" in
            ''|*[!0-9]*)
                echo "ERROR: distributed compiler input size could not be determined" >&2
                exit 1
                ;;
        esac
        compiler_forward_args=()
        compiler_skip_arg=0
        for ((compiler_arg_index = 0; compiler_arg_index < \${#compiler_args[@]}; compiler_arg_index++)); do
            arg="\${compiler_args[\$compiler_arg_index]}"
            if [ "\$compiler_skip_arg" -eq 1 ]; then
                compiler_skip_arg=0
                continue
            fi
            if [ "\$compiler_arg_index" -eq "\$compiler_source_index" ]; then
                continue
            fi
            case "\$arg" in
                -MD|-MMD|-MP|-MG)
                    continue
                    ;;
                -Wp,-MD,*|-Wp,-MMD,*|-Wp,-MP|-Wp,-MG)
                    continue
                    ;;
                -MF|-MT|-MQ|-MJ)
                    compiler_skip_arg=1
                    continue
                    ;;
                -MF*|-MT*|-MQ*|-MJ*)
                    continue
                    ;;
            esac
            compiler_forward_args+=("\$arg")
        done
        if [ -z "\$output_file" ]; then
            # Replacing the source with .../clang-job.XXXXXX/input changes
            # Clang's implicit object name. Preserve the original -c foo.c
            # contract for Autoconf probes and handwritten make rules.
            compiler_default_output="\${compiler_source##*/}"
            compiler_default_output="\${compiler_default_output%.*}.o"
            compiler_forward_args+=(-o "\$compiler_default_output")
        fi
        compiler_forward_args+=(-x "\$compiler_remote_language" -Wno-unused-command-line-argument "\$compiler_input")
        if [ "\$compiler_input_size" -lt "\$compiler_min_preprocessed_bytes" ]; then
            compiler_input_and_slot_cleanup
            trap - EXIT HUP INT TERM
            if "\${compiler[@]}" "\$@"; then
                compiler_status=0
                compiler_record_success 0 "\${compiler_hosts[0]}"
            else
                compiler_status=\$?
            fi
            exit "\$compiler_status"
        fi
        compiler_response="\$compiler_job_dir.response"
        if ! : > "\$compiler_response"; then
            compiler_input_cleanup
            compiler_slot_release
            echo "ERROR: distributed compiler response file could not be created" >&2
            exit 1
        fi
        # Keep response files until the scratch workdir is removed. Concurrent
        # forwarded unlink operations can leave remote VFS peers with a stale
        # negative lookup for a file that still exists.
        compiler_remote_cleanup() {
            compiler_input_cleanup
            compiler_slot_release
        }
        trap compiler_remote_cleanup EXIT HUP INT TERM
        for arg in "\${compiler_forward_args[@]}"; do
            if ! printf '%q\n' "\$arg" >> "\$compiler_response"; then
                echo "ERROR: distributed compiler response file could not be written" >&2
                exit 1
            fi
        done
        compiler_remote_path="\${PATH:-/usr/bin:/bin}"
        if env -i PATH="\$compiler_remote_path" HOME="\${HOME:-/root}" TMPDIR="\${TMPDIR:-/tmp}" TZ=UTC0 \
            on "\$compiler_host" forward "+\$compiler_responses" -- \
            "\${compiler[@]}" -fno-temp-file "@\$compiler_response"; then
            compiler_status=0
        else
            compiler_status=\$?
        fi
        compiler_input_cleanup
        if [ "\$compiler_status" -eq 0 ]; then
            compiler_record_success "\$compiler_candidate_index" "\$compiler_host"
            compiler_slot_cleanup
        else
            compiler_slot_release
        fi
        trap - EXIT HUP INT TERM
        if [ "\$compiler_status" -eq 0 ]; then
            exit 0
        fi
        echo "warning: distributed compiler on \$compiler_host failed with status \$compiler_status; retrying locally" >&2
        if "\${compiler[@]}" "\$@"; then
            compiler_status=0
            compiler_record_success 0 "\${compiler_hosts[0]}"
        else
            compiler_status=\$?
        fi
        exit "\$compiler_status"
    fi
fi
if "\${compiler[@]}" "\$@"; then
    compiler_status=0
else
    compiler_status=\$?
fi
if [ "\$compiler_status" -eq 0 ] && [ "\$link_output" -eq 1 ] && [ -n "\$output_file" ] && [ -f "\$output_file" ] &&
   [ ! -x "\$output_file" ]; then
    if ! chmod a+x -- "\$output_file"; then
        echo "ERROR: linked output '\$output_file' is not executable and could not be repaired" >&2
        exit 1
    fi
fi
exit "\$compiler_status"
EOF
    chmod +x "$output"
}

setup_wos_host_toolchain_shim() {
    local shim_root="$1"
    local shim_bin="$shim_root/bin"
    local shim_lib="$shim_root/lib"
    local compat_root="$WORKSPACE_ROOT/toolchain/host"
    local clang_path
    local clangxx_path
    local resource_dir
    local local_resource_dir
    local tool
    local tool_path

    clang_path="$(find_system_tool clang)"
    clangxx_path="$(find_system_tool clang++)"
    resource_dir="$("$clang_path" --target="$WOS_TARGET_ARCH" -print-resource-dir 2>/dev/null || true)"
    if [ -z "$resource_dir" ]; then
        resource_dir="$("$clang_path" -print-resource-dir)"
    fi
    if [ ! -d "$resource_dir/include" ]; then
        echo "ERROR: system clang resource headers not found at $resource_dir/include" >&2
        exit 1
    fi

    mkdir -p "$shim_bin" "$shim_lib"
    local_resource_dir="$shim_lib/clang/$WOS_CLANG_VERSION"
    mkdir -p "$local_resource_dir"
    if [ -L "$local_resource_dir/include" ]; then
        rm -f "$local_resource_dir/include"
    fi
    if [ ! -e "$local_resource_dir/include" ]; then
        cp -a "$resource_dir/include" "$local_resource_dir/include"
    fi

    write_clang_wrapper "$shim_bin/clang" "$clang_path" "$local_resource_dir"
    write_clang_wrapper "$shim_bin/clang++" "$clangxx_path" "$local_resource_dir"
    ln -sfn clang "$shim_bin/cc"
    ln -sfn clang++ "$shim_bin/c++"

    for tool in ld.lld llvm-ar llvm-ranlib llvm-strip llvm-objcopy llvm-nm llvm-readelf llvm-objdump llvm-config llvm-tblgen clang-tblgen; do
        tool_path="$(find_system_tool "$tool")"
        ln -sfn "$tool_path" "$shim_bin/$tool"
    done
    ln -sfn llvm-ar "$shim_bin/ar"

    if [ -L "$compat_root" ]; then
        ln -sfn "$(basename "$shim_root")" "$compat_root"
    elif [ ! -e "$compat_root" ]; then
        (
            cd "$WORKSPACE_ROOT/toolchain"
            ln -sfn "$(basename "$shim_root")" host
        )
    elif ! wos_host_tool_is_usable "$compat_root/bin/clang"; then
        echo "ERROR: $compat_root exists but does not provide bin/clang for native WOS bootstrap." >&2
        echo "Remove or fix it so build scripts can use the WOS host-toolchain shim." >&2
        exit 1
    fi

    echo "Using WOS system clang: $clang_path"
    echo "Host toolchain shim: $shim_root"
    echo "Host toolchain compatibility path: $compat_root"
}

if [ "$WOS_BOOTSTRAP_HOST_SYSTEM" = "WOS" ]; then
    WOS_HOST_TOOLCHAIN_ROOT="$WORKSPACE_ROOT/toolchain/wos-host-shim"
    echo "=== Phase 1: WOS system toolchain shim ==="
    setup_wos_host_toolchain_shim "$WOS_HOST_TOOLCHAIN_ROOT"
else
    echo "=== Phase 1: Host toolchain (clang/lld) ==="
    "$SCRIPT_DIR/host-toolchain.sh"
fi

echo ""
if [ "$WOS_BOOTSTRAP_HOST_SYSTEM" = "WOS" ]; then
    echo "=== Phase 2: System CMake ==="
    echo "Using system CMake: $(find_system_tool cmake)"
else
    echo "=== Phase 2: Host CMake with WOS platform support ==="
    WOS_HOST_TOOLCHAIN_ROOT="$WOS_HOST_TOOLCHAIN_ROOT" \
        "$WORKSPACE_ROOT/scripts/build/build_cmake_for_host.sh"
fi

echo ""
echo "=== Phase 3: WOS target toolchain ==="
WOS_BUILD_CMAKE_FOR_HOST=0 \
    WOS_HOST_TOOLCHAIN_ROOT="$WOS_HOST_TOOLCHAIN_ROOT" \
    "$SCRIPT_DIR/wos-toolchain.sh"

echo ""
echo "=== Bootstrap complete ==="
