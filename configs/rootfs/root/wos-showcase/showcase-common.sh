#!/bin/sh

showcase_have() {
    command -v "$1" >/dev/null 2>&1 || [ -x "$1" ]
}

showcase_section() {
    printf '\n## %s\n' "$*"
}

showcase_cmd() {
    printf '\n$'
    for arg in "$@"; do
        printf ' %s' "$arg"
    done
    printf '\n'
    "$@"
}

showcase_shell() {
    printf '\n$ sh -c %s\n' "$*"
    sh -c "$*"
}

showcase_first_remote_host() {
    hosts="${WOS_SHOWCASE_HOSTS:-}"
    local_name="$(hostname 2>/dev/null || true)"
    old_ifs="$IFS"
    IFS=,
    for host in $hosts; do
        IFS="$old_ifs"
        [ -n "$host" ] || continue
        case "$host" in
            "$local_name"|"$local_name".*)
                ;;
            *)
                printf '%s\n' "$host"
                return 0
                ;;
        esac
        IFS=,
    done
    IFS="$old_ifs"
    return 1
}

showcase_data_path() {
    if [ -f /srv/bee.txt ]; then
        printf '%s\n' /srv/bee.txt
    else
        printf '%s\n' /srv/hello.txt
    fi
}

showcase_scale_value() {
    scale="${WOS_SHOWCASE_SCALE:-quick}"
    key="$1"
    case "$scale:$key" in
        quick:stat_iterations) printf '%s\n' 750 ;;
        full:stat_iterations) printf '%s\n' 5000 ;;
        stress:stat_iterations) printf '%s\n' 20000 ;;
        quick:read_iterations) printf '%s\n' 1 ;;
        full:read_iterations) printf '%s\n' 3 ;;
        stress:read_iterations) printf '%s\n' 8 ;;
        quick:mandel_width) printf '%s\n' 320 ;;
        full:mandel_width) printf '%s\n' 800 ;;
        stress:mandel_width) printf '%s\n' 1600 ;;
        quick:mandel_height) printf '%s\n' 180 ;;
        full:mandel_height) printf '%s\n' 450 ;;
        stress:mandel_height) printf '%s\n' 900 ;;
        quick:mandel_iter) printf '%s\n' 250 ;;
        full:mandel_iter) printf '%s\n' 1000 ;;
        stress:mandel_iter) printf '%s\n' 3000 ;;
        quick:mandel_threads) printf '%s\n' 2 ;;
        full:mandel_threads) printf '%s\n' 8 ;;
        stress:mandel_threads) printf '%s\n' 16 ;;
        *) printf '%s\n' 1 ;;
    esac
}
