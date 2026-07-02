#!/bin/sh
set -eu

DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
# shellcheck disable=SC1091
. "$DIR/showcase-common.sh"

CXX="${CXX:-clang++}"
SRC="${WOS_LIVE_DEMO_SRC:-$DIR/live-cpp-demo.cpp}"
BIN="${WOS_LIVE_DEMO_BIN:-/tmp/wos-live-demo}"
case "${WOS_SHOWCASE_SCALE:-quick}" in
    quick) DEFAULT_BYTES=65536 ;;
    full) DEFAULT_BYTES=1048576 ;;
    stress) DEFAULT_BYTES=4194304 ;;
    *) DEFAULT_BYTES=65536 ;;
esac
BYTES="${WOS_LIVE_DEMO_BYTES:-$DEFAULT_BYTES}"
LOCAL_FILE="${WOS_LIVE_DEMO_LOCAL_FILE:-/tmp/wos-live-demo-local.dat}"
REMOTE_FILE="${WOS_LIVE_DEMO_REMOTE_FILE:-/tmp/wos-live-demo-remote.dat}"

if ! showcase_have "$CXX"; then
    printf 'skip: %s is not installed\n' "$CXX"
    exit 0
fi

if [ ! -f "$SRC" ]; then
    printf 'skip: demo source not found: %s\n' "$SRC"
    exit 0
fi

showcase_section "live compile c++ on WOS"
showcase_cmd "$CXX" -std=c++23 -O2 "$SRC" -o "$BIN"

showcase_section "one process: pipe plus vfs"
showcase_cmd "$BIN" pipevfs "$LOCAL_FILE" "$BYTES" direct
showcase_cmd locally "$BIN" pipevfs "$LOCAL_FILE" "$BYTES" locally
showcase_cmd remotely forward +/tmp -- "$BIN" pipevfs "$REMOTE_FILE" "$BYTES" remotely

showcase_section "shell pipe: producer into vfs sink"
showcase_shell "\"$BIN\" emit \"$BYTES\" shell-local | \"$BIN\" sink \"$LOCAL_FILE.pipe\" shell-local"
showcase_shell "remotely forward +/tmp -- sh -c '\"\$1\" emit \"\$2\" remote-pipe | \"\$1\" sink \"\$3\" remote-pipe' sh \"$BIN\" \"$BYTES\" \"$REMOTE_FILE.pipe\""

showcase_section "run once on each requested WOS host"
hosts="${WOS_SHOWCASE_HOSTS:-}"
if [ -z "$hosts" ]; then
    printf 'skip: set WOS_SHOWCASE_HOSTS=host0,host1 or pass run-all --hosts host0,host1\n'
    exit 0
fi

old_ifs="$IFS"
IFS=,
set -- $hosts
IFS="$old_ifs"

for host in "$@"; do
    [ -n "$host" ] || continue
    showcase_cmd on "$host" forward +/tmp -- "$BIN" pipevfs "$REMOTE_FILE.$host" "$BYTES" "on:$host"
done
