#!/usr/bin/bash

if [[ ${1-} == "--" ]]; then
    shift
fi

if (( $# == 0 )); then
    printf 'usage: coproc <command> [args...]\n' >&2
    exit 64
fi

child_pid=
forward_signal() {
    local signal=$1
    local status=$2
    local target_pid=${child_pid:-${WOS_EXTERNAL_COPROC_PID-}}

    trap - HUP INT TERM
    if [[ -n $target_pid ]]; then
        kill -s "$signal" "$target_pid" 2>/dev/null || true
        wait "$target_pid" 2>/dev/null || true
    fi
    exit "$status"
}

trap 'forward_signal HUP 129' HUP
trap 'forward_signal INT 130' INT
trap 'forward_signal TERM 143' TERM

# An external command cannot publish COPROC[] back into its parent shell. Keep
# the process-level form useful by running the real Bash construct internally,
# preserving this adapter's standard descriptors, and waiting for its result.
exec {coproc_stdin}<&0
exec {coproc_stdout}>&1

# Bash creates WOS_EXTERNAL_COPROC_PID from this name; the array itself is
# intentionally unused because the child is wired to this adapter's stdio.
# shellcheck disable=SC2034
coproc WOS_EXTERNAL_COPROC {
    exec 0<&"$coproc_stdin"
    exec 1>&"$coproc_stdout"
    exec {coproc_stdin}<&-
    exec {coproc_stdout}>&-
    exec "$@"
}

child_pid=$WOS_EXTERNAL_COPROC_PID
exec {coproc_stdin}<&-
exec {coproc_stdout}>&-

wait "$child_pid"
