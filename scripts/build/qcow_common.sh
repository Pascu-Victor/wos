#!/bin/bash
# Shared qcow2/libguestfs diagnostics for WOS image build scripts.

wos_qcow_log_has_lock() {
    local log_file="$1"

    grep -Eiq \
        'failed to get .*lock|is another process using the image|could not.*lock|cannot lock|failed to lock byte|locked by another process|image is locked|resource temporarily unavailable|device or resource busy' \
        "$log_file"
}

wos_qcow_log_has_corruption() {
    local log_file="$1"

    grep -Eiq \
        'image is corrupt|corrupt(ed|ion)?|not in qcow2 format|invalid qcow2|qcow2.*invalid|refcount.*(error|inconsistent|corrupt)|cluster.*(out of range|inconsistent|refcount|corrupt)|l1 table|l2 table|header extension|unsupported qcow2 feature' \
        "$log_file"
}

wos_qcow_print_evidence() {
    local log_file="$1"
    local pattern="$2"
    local line

    line=$(grep -Eim1 "$pattern" "$log_file" || true)
    if [ -n "$line" ]; then
        printf '  evidence: %s\n' "$line" >&2
    fi
}

wos_qcow_open_process_summary() {
    local disk="$1"
    local real_disk
    local fd
    local target
    local pid
    local comm
    local shown=0
    local found=0

    declare -A seen_pids=()

    real_disk=$(readlink -f -- "$disk" 2>/dev/null || printf '%s\n' "$disk")

    for fd in /proc/[0-9]*/fd/*; do
        [ -e "$fd" ] || continue

        target=$(readlink "$fd" 2>/dev/null || true)
        case "$target" in
            "$real_disk"|"$real_disk (deleted)")
                pid="${fd#/proc/}"
                pid="${pid%%/*}"
                if [ -n "${seen_pids[$pid]:-}" ]; then
                    continue
                fi
                seen_pids[$pid]=1
                comm=$(cat "/proc/$pid/comm" 2>/dev/null || printf '?')
                printf '  process: pid=%s command=%s\n' "$pid" "$comm"
                found=1
                shown=$((shown + 1))
                if [ "$shown" -ge 5 ]; then
                    printf '  process: additional users omitted\n'
                    break
                fi
                ;;
        esac
    done

    [ "$found" -eq 1 ]
}

wos_qcow_print_in_use_message() {
    local operation="$1"
    local disk="$2"
    local processes="${3:-}"
    local log_file="${4:-}"

    printf "ERROR: Could not %s.\n" "$operation" >&2
    printf "  image: %s\n" "$disk" >&2
    printf "  reason: qcow image is locked because it is currently in use.\n" >&2
    if [ -n "$processes" ]; then
        printf '%s\n' "$processes" >&2
    fi
    if [ -n "$log_file" ]; then
        wos_qcow_print_evidence "$log_file" \
            'failed to get .*lock|is another process using the image|could not.*lock|cannot lock|failed to lock byte|locked by another process|image is locked|resource temporarily unavailable|device or resource busy'
    fi
    printf "Action: stop the WOS cluster/debug/KTEST VM or any qemu/guestfish process using this image, then rerun the build.\n" >&2
}

wos_qcow_print_corruption_message() {
    local operation="$1"
    local disk="$2"
    local log_file="${3:-}"

    printf "ERROR: Could not %s.\n" "$operation" >&2
    printf "  image: %s\n" "$disk" >&2
    printf "  reason: qcow image appears corrupted, invalid, or unsupported by qemu-img.\n" >&2
    if [ -n "$log_file" ]; then
        wos_qcow_print_evidence "$log_file" \
            'image is corrupt|corrupt(ed|ion)?|not in qcow2 format|invalid qcow2|qcow2.*invalid|refcount.*(error|inconsistent|corrupt)|cluster.*(out of range|inconsistent|refcount|corrupt)|l1 table|l2 table|header extension|unsupported qcow2 feature'
    fi
    printf "Action: stop any VMs first, then inspect with: qemu-img check -f qcow2 %q\n" "$disk" >&2
    printf "Generated image recovery: delete/regenerate the image after confirming no VM is using it.\n" >&2
}

wos_qcow_report_failure() {
    local operation="$1"
    local disk="$2"
    local log_file="$3"
    local processes
    local check_log
    local check_status

    processes=$(wos_qcow_open_process_summary "$disk" || true)
    if [ -n "$processes" ]; then
        wos_qcow_print_in_use_message "$operation" "$disk" "$processes" "$log_file"
        return 0
    fi

    if wos_qcow_log_has_lock "$log_file"; then
        wos_qcow_print_in_use_message "$operation" "$disk" "" "$log_file"
        return 0
    fi

    if wos_qcow_log_has_corruption "$log_file"; then
        wos_qcow_print_corruption_message "$operation" "$disk" "$log_file"
        return 0
    fi

    if [ ! -f "$disk" ] || ! command -v qemu-img >/dev/null 2>&1; then
        printf "ERROR: Could not %s.\n" "$operation" >&2
        printf "  image: %s\n" "$disk" >&2
        printf "  reason: unable to classify the qcow image failure from the tool output above.\n" >&2
        return 0
    fi

    check_log=$(mktemp)
    if qemu-img check -f qcow2 "$disk" > "$check_log" 2>&1; then
        printf "NOTE: qemu-img metadata check passed for %s; the failure above is likely libguestfs/filesystem state, not qcow corruption.\n" "$disk" >&2
        rm -f "$check_log"
        return 0
    else
        check_status=$?
    fi

    if wos_qcow_log_has_lock "$check_log"; then
        wos_qcow_print_in_use_message "$operation" "$disk" "" "$check_log"
    elif wos_qcow_log_has_corruption "$check_log" || [ "$check_status" -eq 2 ] || [ "$check_status" -eq 3 ]; then
        wos_qcow_print_corruption_message "$operation" "$disk" "$check_log"
    else
        printf "ERROR: Could not %s.\n" "$operation" >&2
        printf "  image: %s\n" "$disk" >&2
        printf "  reason: qemu-img check could not classify the image failure (status %s).\n" "$check_status" >&2
        wos_qcow_print_evidence "$check_log" '.+'
    fi

    rm -f "$check_log"
}

wos_qcow_run() {
    local operation="$1"
    local disk="$2"
    local log_file
    local status

    shift 2

    log_file=$(mktemp)
    if "$@" > "$log_file" 2>&1; then
        cat "$log_file"
        rm -f "$log_file"
        return 0
    fi

    status=$?
    cat "$log_file" >&2
    wos_qcow_report_failure "$operation" "$disk" "$log_file"
    rm -f "$log_file"
    return "$status"
}

wos_qcow_guestfish() {
    local operation="$1"
    local disk="$2"

    shift 2
    wos_qcow_run "$operation" "$disk" guestfish "$@"
}

wos_qcow_validate_for_update() {
    local disk="$1"
    local operation="$2"
    local processes
    local log_file
    local status

    if [ ! -e "$disk" ]; then
        return 0
    fi

    processes=$(wos_qcow_open_process_summary "$disk" || true)
    if [ -n "$processes" ]; then
        wos_qcow_print_in_use_message "$operation" "$disk" "$processes"
        return 1
    fi

    log_file=$(mktemp)
    if qemu-img info -f qcow2 "$disk" > "$log_file" 2>&1; then
        rm -f "$log_file"
        return 0
    else
        status=$?
    fi

    if wos_qcow_log_has_lock "$log_file"; then
        wos_qcow_print_in_use_message "$operation" "$disk" "" "$log_file"
    elif wos_qcow_log_has_corruption "$log_file"; then
        wos_qcow_print_corruption_message "$operation" "$disk" "$log_file"
    else
        printf "ERROR: Could not %s.\n" "$operation" >&2
        printf "  image: %s\n" "$disk" >&2
        printf "  reason: qemu-img could not open the qcow image for validation (status %s).\n" "$status" >&2
    fi
    rm -f "$log_file"
    return "$status"
}

wos_qcow_guard_replace() {
    local disk="$1"
    local operation="$2"
    local processes
    local log_file
    local status

    if [ ! -e "$disk" ]; then
        return 0
    fi

    processes=$(wos_qcow_open_process_summary "$disk" || true)
    if [ -n "$processes" ]; then
        wos_qcow_print_in_use_message "$operation" "$disk" "$processes"
        return 1
    fi

    log_file=$(mktemp)
    if qemu-img info -f qcow2 "$disk" > "$log_file" 2>&1; then
        rm -f "$log_file"
        return 0
    else
        status=$?
    fi

    if wos_qcow_log_has_lock "$log_file"; then
        wos_qcow_print_in_use_message "$operation" "$disk" "" "$log_file"
        rm -f "$log_file"
        return "$status"
    fi

    if wos_qcow_log_has_corruption "$log_file"; then
        printf "WARNING: Existing qcow image is corrupted/invalid and will be recreated: %s\n" "$disk" >&2
        wos_qcow_print_evidence "$log_file" \
            'image is corrupt|corrupt(ed|ion)?|not in qcow2 format|invalid qcow2|qcow2.*invalid|refcount.*(error|inconsistent|corrupt)|cluster.*(out of range|inconsistent|refcount|corrupt)|l1 table|l2 table|header extension|unsupported qcow2 feature'
        rm -f "$log_file"
        return 0
    fi

    printf "ERROR: Refusing to replace qcow image because qemu-img could not classify it safely.\n" >&2
    printf "  image: %s\n" "$disk" >&2
    wos_qcow_print_evidence "$log_file" '.+'
    rm -f "$log_file"
    return "$status"
}
