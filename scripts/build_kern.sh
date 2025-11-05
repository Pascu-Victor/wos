#!/bin/bash
set -e

function check_headers() {
    sh scripts/check_headers.sh $1
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

# run scripts/create_fat32_test_disk.sh
result=$(sh scripts/create_fat32_test_disk.sh)
if [ "$?" != "0" ]; then
    echo "Error: create_fat32_test_disk.sh failed"
    exit 1
else
    echo "fat32 image created successfully"
fi

# run scripts/make_image.sh
result=$(sh scripts/make_image.sh)
if [ "$?" != "0" ]; then
    echo "Error: make_image.sh failed"
    exit 1
else
    echo "kernel image created successfully"
fi
