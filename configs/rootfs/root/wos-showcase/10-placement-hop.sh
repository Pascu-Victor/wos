#!/bin/sh
set -eu

DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
# shellcheck disable=SC1091
. "$DIR/showcase-common.sh"

showcase_section "explicit placement wrappers"
showcase_cmd locally wosid
showcase_cmd remotely wosid

target="$(showcase_first_remote_host || true)"
if [ -n "$target" ]; then
    showcase_cmd on "$target" wosid
else
    printf 'skip: no non-local WOS host in WOS_SHOWCASE_HOSTS for: on <host> wosid\n'
fi

showcase_section "remote shell, then homeward"
showcase_cmd remotely sh -c '
printf "remote-preferred shell: "
wosid
printf "homeward child: "
homeward wosid
printf "locally inside that shell: "
locally wosid
'
