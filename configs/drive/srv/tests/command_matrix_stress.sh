#!/bin/sh
# Diagnostic exposure only. The caller owns the wall-clock deadline; no throughput credit.
set -eu

usage() {
    printf 'usage: %s [iterations=100 (1..1000)] [existing-base-dir=/tmp]\n' "$0" >&2
    exit 2
}
[ "$#" -le 2 ] || usage
iterations=${1-100}
case "$iterations" in ''|0*|*[!0-9]*) usage ;; esac
[ "${#iterations}" -le 4 ] && [ "$iterations" -le 1000 ] || usage
base=$(CDPATH= cd -P "${2-/tmp}" && pwd -P) || usage
[ -f /srv/tests/command_matrix.sh ] && [ -r /srv/tests/command_matrix.sh ] || {
    printf 'missing dispatcher: /srv/tests/command_matrix.sh\n' >&2
    exit 1
}
PATH=/usr/bin:/usr/sbin:/bin:/sbin
USER=${USER:-root}; HOME=${HOME:-/root}; HOSTNAME=${HOSTNAME:-uma-reference-vm0}
export PATH USER HOME HOSTNAME
umask 077
work= identity=
cleanup() {
    rc=$?
    trap - EXIT HUP INT TERM
    if [ -n "$work" ]; then
        if [ -n "$identity" ] && [ -d "$work" ] && [ ! -L "$work" ] &&
            [ "$(stat -c '%d:%i' "$work" 2>/dev/null)" = "$identity" ]; then
            if ! rm -rf -- "$work"; then [ "$rc" -ne 0 ] || rc=1; fi
        else
            printf 'cleanup refused changed/unknown workroot: %s\n' "$work" >&2
            [ "$rc" -ne 0 ] || rc=1
        fi
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
work=$(mktemp -d "$base/command-matrix-stress.XXXXXX")
identity=$(stat -c '%d:%i' "$work")
printf 'COMMAND_MATRIX_STRESS_START v=1 pid=%s iterations=%s root=%s diagnostic=1\n' "$$" "$iterations" "$work"
i=1
while [ "$i" -le "$iterations" ]; do
    for case_id in 90 91; do
        printf 'COMMAND_MATRIX_STRESS_BEGIN v=1 pid=%s iteration=%s case=%03d\n' "$$" "$i" "$case_id"
        rc=0
        sh /srv/tests/command_matrix.sh "$case_id" "$work" || rc=$?
        printf 'COMMAND_MATRIX_STRESS_END v=1 pid=%s iteration=%s case=%03d rc=%s\n' "$$" "$i" "$case_id" "$rc"
        [ "$rc" -eq 0 ] || exit "$rc"
    done
    i=$((i + 1))
done
printf 'COMMAND_MATRIX_STRESS_DONE v=1 pid=%s iterations=%s cases=%s\n' "$$" "$iterations" "$((iterations * 2))"
