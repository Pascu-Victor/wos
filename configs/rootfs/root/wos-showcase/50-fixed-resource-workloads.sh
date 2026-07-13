#!/bin/sh
set -eu

DIR="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"

scale="${WOS_SHOWCASE_SCALE:-quick}"
hosts="${WOS_SHOWCASE_HOSTS:-}"
output_root="${WOS_SHOWCASE_OUTPUT_ROOT:-/tmp/wos-showcase-fixed-output-$$}"
timeout_seconds="${WOS_SHOWCASE_FIXED_TIMEOUT_SECONDS:-1800}"

case "$scale" in
    quick|full|stress) ;;
    *)
        echo "invalid WOS showcase scale: $scale" >&2
        exit 1
        ;;
esac

if [ -z "$hosts" ]; then
    hosts="$(hostname)"
fi

for tool in /usr/bin/python3 /usr/bin/git /usr/bin/locally /usr/bin/on /usr/bin/forward /usr/bin/wkictl; do
    if [ ! -x "$tool" ]; then
        echo "fixed-resource showcase requires $tool" >&2
        exit 1
    fi
done

helper="$DIR/fixed_resource_workloads.py"
if [ ! -r "$helper" ]; then
    echo "fixed-resource showcase helper is missing: $helper" >&2
    exit 1
fi

exec /usr/bin/locally /usr/bin/python3 "$helper" coordinator \
    --hosts "$hosts" \
    --scale "$scale" \
    --output-root "$output_root" \
    --timeout-seconds "$timeout_seconds"
