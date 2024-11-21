#!/bin/bash

# Path to the executable
EXECUTABLE="./bin/wos"

# Process each log file that matches the pattern
for LOG_FILE in qemu.*.log; do
    # Temporary file to store the modified log
    TEMP_LOG_FILE="modified_$LOG_FILE"

    # Extract addresses and call addr2line
    grep -oE '0xffffffff8[0-9a-fA-F]{7}' "$LOG_FILE" | sort | uniq | while read -r address; do
        # Get the source code location
        location=$(addr2line -e "$EXECUTABLE" "$address" 2>/dev/null)
        # Replace the address with the source code location in the log file
        sed -i "s|$address|$address ($location)|g" "$LOG_FILE"
    done

    # Save the modified log
    mv "$LOG_FILE" "$TEMP_LOG_FILE"
    echo "Modified log saved to $TEMP_LOG_FILE"
done
