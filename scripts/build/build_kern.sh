#!/bin/bash
set -e

WOS_ROOT="${WOS_WORKSPACE_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
cd "$WOS_ROOT"

function check_headers() {
    sh scripts/build/check_headers.sh "$1"
    if [ "$?" != "0" ]; then
        echo "Error: check_headers.sh failed for $1"
        exit 1
    else
        echo "$1 headers are good"
    fi
}
# run check_headers
check_headers "modules/kern"
check_headers "modules/init"

cmake -B build -GNinja .
cmake --build build

# run scripts/build/create_mountfs_disk.sh
result=$(sh scripts/build/create_mountfs_disk.sh)
if [ "$?" != "0" ]; then
    echo "Error: create_mountfs_disk.sh failed"
    exit 1
else
    echo "mountfs image created successfully"
fi

# run scripts/build/make_image.sh (creates boot disk, builds initramfs, populates boot partition)
result=$(sh scripts/build/make_image.sh)
if [ "$?" != "0" ]; then
    echo "Error: make_image.sh failed"
    exit 1
else
    echo "kernel image created successfully"
fi
