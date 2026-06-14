#!/bin/sh
set -eu

DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
# shellcheck disable=SC1091
. "$DIR/showcase-common.sh"

showcase_section "identity"
showcase_cmd hostname
showcase_cmd wosid

showcase_section "target policy"
showcase_cmd wkictl target show

showcase_section "vfs routing"
showcase_cmd wkictl vfs defaults
showcase_cmd wkictl vfs list

showcase_section "self proc wki fields"
for path in /proc/self/wki_launcher /proc/self/wki_runner /proc/self/wki_remote_pid; do
    if [ -r "$path" ]; then
        showcase_cmd cat "$path"
    else
        printf 'skip: %s is not readable\n' "$path"
    fi
done

showcase_section "perf hint"
if showcase_have /usr/bin/perf; then
    showcase_cmd wkictl perf show
else
    printf 'skip: /usr/bin/perf is not installed\n'
fi
