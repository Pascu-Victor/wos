#!/bin/sh
set -eu

DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
# shellcheck disable=SC1091
. "$DIR/showcase-common.sh"

data_path="$(showcase_data_path)"
local_note="/tmp/wos-showcase-local-note.txt"
printf 'local tmp note from %s\n' "$(hostname 2>/dev/null || echo wos)" > "$local_note"

showcase_section "baseline probes"
showcase_cmd wkictl vfs defaults
showcase_cmd wkictl vfs probe "$data_path"
showcase_cmd wkictl vfs probe "$local_note"

showcase_section "forward +/srv -/tmp around a command"
showcase_cmd forward +/srv -/tmp -- sh -c '
printf "task-local VFS rules after forward wrapper:\n"
wkictl vfs list
printf "\nforwarded /srv probe:\n"
wkictl vfs probe "$1"
printf "\nlocal /tmp probe:\n"
wkictl vfs probe "$2"
printf "\nreadable data sample:\n"
head -n 3 "$1" 2>/dev/null || cat "$1"
printf "\nlocal note:\n"
cat "$2"
' sh "$data_path" "$local_note"

showcase_section "remote-preferred command with the same route shape"
showcase_cmd remotely forward +/srv -/tmp -- sh -c '
printf "runner identity: "
wosid
printf "\nroute table:\n"
wkictl vfs list
printf "\nstat forwarded data path:\n"
wc -c "$1"
' sh "$data_path"
