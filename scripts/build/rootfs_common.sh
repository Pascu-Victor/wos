#!/bin/bash
# Shared rootfs staging helpers for create_mountfs_disk.sh and sync_rootfs.sh.

# shellcheck source=scripts/build/qcow_common.sh
source "$(dirname "${BASH_SOURCE[0]}")/qcow_common.sh"

ROOTFS_REPO=""
ROOTFS_STAGING=""
ROOTFS_MANAGED_TMP=""
ROOTFS_CHANGED=0
ROOTFS_BUILD_DIR="${WOS_BUILD_DIR:-build}"
ROOTFS_SYSROOT_DIR="${WOS_SYSROOT_PATH:-}"
ROOTFS_BUSYBOX_INSTALL_DIR="${WOS_BUSYBOX_INSTALL_DIR:-}"
ROOTFS_CONTENT_MANIFEST="/etc/wos-rootfs-manifest.tsv"
ROOTFS_MANIFEST_HASH_LIMIT_BYTES="${WOS_ROOTFS_MANIFEST_HASH_LIMIT_BYTES:-16777216}"
case "$ROOTFS_MANIFEST_HASH_LIMIT_BYTES" in
    ''|*[!0-9]*)
        ROOTFS_MANIFEST_HASH_LIMIT_BYTES=16777216
        ;;
esac

rootfs_default_staging_parent() {
    local repo="$1"
    local build_dir="$ROOTFS_BUILD_DIR"

    case "$build_dir" in
        /*)
            printf '%s/rootfs-staging\n' "${build_dir%/}"
            ;;
        *)
            printf '%s/%s/rootfs-staging\n' "$repo" "${build_dir%/}"
            ;;
    esac
}

rootfs_make_staging_dir() {
    local repo="$1"
    local base="${WOS_ROOTFS_STAGING_TMPDIR:-}"

    if [ -z "$base" ]; then
        base="$(rootfs_default_staging_parent "$repo")"
    fi

    mkdir -p "$base"
    mktemp -d "$base/staging.XXXXXX"
}

rootfs_record_managed_path() {
    printf '%s\n' "$1" >> "$ROOTFS_MANAGED_TMP"
}

rootfs_write_content_manifest() {
    local manifest="$1"
    local tmp

    tmp=$(mktemp)
    mkdir -p "$(dirname "$manifest")"

    # Most large payloads are hardlinked into staging; track those by metadata
    # so rootfs sync does not read gigabytes just to prove they are unchanged.
    (
        cd "$ROOTFS_STAGING"
        find . -mindepth 1 ! -path ".${ROOTFS_CONTENT_MANIFEST}" -print0 | sort -z | while IFS= read -r -d '' entry; do
            local hash
            local identity
            local identity_kind
            local links
            local mode
            local mtime
            local rel
            local size
            local target

            rel="/${entry#./}"
            if [ -L "$entry" ]; then
                target=$(readlink "$entry")
                printf '%s\tL\t\t\t\t%s\n' "$rel" "$target"
            elif [ -d "$entry" ]; then
                mode=$(stat -c %a "$entry")
                printf '%s\tD\t%s\t\t\t\n' "$rel" "$mode"
            elif [ -f "$entry" ]; then
                mode=$(stat -c %a "$entry")
                size=$(stat -c %s "$entry")
                links=$(stat -c %h "$entry")
                if [ "${WOS_ROOTFS_MANIFEST_FORCE_HASH:-0}" = "1" ] ||
                   { [ "$links" -le 1 ] && [ "$size" -le "$ROOTFS_MANIFEST_HASH_LIMIT_BYTES" ]; }; then
                    hash=$(sha256sum -b "$entry" | awk '{print $1}')
                    identity="$hash"
                    identity_kind="sha256"
                else
                    mtime=$(TZ=UTC0 stat -c %y "$entry")
                    identity="$mtime"
                    identity_kind="mtime"
                fi
                printf '%s\tF\t%s\t%s\t%s\t%s\n' "$rel" "$mode" "$size" "$identity" "$identity_kind"
            fi
        done
    ) > "$tmp"
    mv -f "$tmp" "$manifest"
}

rootfs_resolve_source() {
    local source="$1"
    if [[ "$source" != /* && "$source" == build/* && "$ROOTFS_BUILD_DIR" != "build" ]]; then
        source="${ROOTFS_BUILD_DIR%/}/${source#build/}"
    elif [[ "$source" != /* && "$source" == toolchain/sysroot/* && -n "$ROOTFS_SYSROOT_DIR" ]]; then
        source="${ROOTFS_SYSROOT_DIR%/}/${source#toolchain/sysroot/}"
    elif [[ "$source" != /* && "$source" == toolchain/busybox-install/* && -n "$ROOTFS_BUSYBOX_INSTALL_DIR" ]]; then
        source="${ROOTFS_BUSYBOX_INSTALL_DIR%/}/${source#toolchain/busybox-install/}"
    fi
    if [[ "$source" = /* ]]; then
        printf '%s\n' "$source"
    else
        printf '%s/%s\n' "$ROOTFS_REPO" "$source"
    fi
}

rootfs_copy_entry() {
    local source="$1"
    local target="$2"
    local mode="${3:-}"
    local resolved
    local target_path
    local target_parent

    resolved=$(rootfs_resolve_source "$source")
    if [ ! -e "$resolved" ]; then
        return 0
    fi

    mkdir -p "$ROOTFS_STAGING$(dirname "$target")"
    target_path="$ROOTFS_STAGING$target"
    target_parent="$(dirname "$target_path")"
    if [ -z "$mode" ] && [ "$(stat -c %d "$resolved" 2>/dev/null || true)" = "$(stat -c %d "$target_parent" 2>/dev/null || true)" ]; then
        cp -al "$resolved" "$target_path" 2>/dev/null || cp -a "$resolved" "$target_path"
    else
        cp -a "$resolved" "$target_path"
    fi
    if [ -n "$mode" ]; then
        chmod "$mode" "$ROOTFS_STAGING$target"
    fi
    rootfs_record_managed_path "$target"
    ROOTFS_CHANGED=1
}

rootfs_copy_glob_entry() {
    local source_pattern="$1"
    local target_dir="$2"
    local resolved_pattern
    local resolved

    if [[ "$source_pattern" != /* && "$source_pattern" == build/* && "$ROOTFS_BUILD_DIR" != "build" ]]; then
        resolved_pattern="${ROOTFS_BUILD_DIR%/}/${source_pattern#build/}"
    elif [[ "$source_pattern" != /* && "$source_pattern" == toolchain/sysroot/* && -n "$ROOTFS_SYSROOT_DIR" ]]; then
        resolved_pattern="${ROOTFS_SYSROOT_DIR%/}/${source_pattern#toolchain/sysroot/}"
    elif [[ "$source_pattern" != /* && "$source_pattern" == toolchain/busybox-install/* && -n "$ROOTFS_BUSYBOX_INSTALL_DIR" ]]; then
        resolved_pattern="${ROOTFS_BUSYBOX_INSTALL_DIR%/}/${source_pattern#toolchain/busybox-install/}"
    elif [[ "$source_pattern" = /* ]]; then
        resolved_pattern="$source_pattern"
    else
        resolved_pattern="$ROOTFS_REPO/$source_pattern"
    fi

    while IFS= read -r resolved; do
        [ -e "$resolved" ] || [ -L "$resolved" ] || continue
        rootfs_copy_entry "$resolved" "$target_dir/$(basename "$resolved")"
    done < <(compgen -G "$resolved_pattern" | sort)
}

rootfs_symlink_entry() {
    local source="$1"
    local target="$2"

    mkdir -p "$ROOTFS_STAGING$(dirname "$target")"
    ln -sfn "$source" "$ROOTFS_STAGING$target"
    rootfs_record_managed_path "$target"
    ROOTFS_CHANGED=1
}

rootfs_stage_sysroot_libs() {
    local sysroot_lib
    local file

    if [ -n "$ROOTFS_SYSROOT_DIR" ]; then
        sysroot_lib="$ROOTFS_SYSROOT_DIR/lib"
    else
        sysroot_lib="$ROOTFS_REPO/toolchain/sysroot/lib"
    fi
    if [ ! -d "$sysroot_lib" ]; then
        return 0
    fi

    mkdir -p "$ROOTFS_STAGING/usr/lib"
    for file in "$sysroot_lib"/*.so "$sysroot_lib"/*.so.* "$sysroot_lib"/crt*.o "$sysroot_lib"/Scrt1.o "$sysroot_lib"/ld.so; do
        [ -e "$file" ] || continue
        rootfs_copy_entry "$file" "/usr/lib/$(basename "$file")"
    done
}

rootfs_stage_sysroot_headers() {
    local sysroot_include

    if [ -n "$ROOTFS_SYSROOT_DIR" ]; then
        sysroot_include="$ROOTFS_SYSROOT_DIR/include"
    else
        sysroot_include="$ROOTFS_REPO/toolchain/sysroot/include"
    fi
    if [ ! -d "$sysroot_include" ]; then
        return 0
    fi

    rootfs_copy_entry "$sysroot_include" "/usr/include"
}

rootfs_stage_sysroot_cmake_data() {
    local sysroot_share
    local entry

    if [ -n "$ROOTFS_SYSROOT_DIR" ]; then
        sysroot_share="$ROOTFS_SYSROOT_DIR/share"
    else
        sysroot_share="$ROOTFS_REPO/toolchain/sysroot/share"
    fi
    if [ ! -d "$sysroot_share" ]; then
        return 0
    fi

    for entry in "$sysroot_share"/cmake-*; do
        [ -e "$entry" ] || continue
        rootfs_copy_entry "$entry" "/usr/share/$(basename "$entry")"
    done
}

rootfs_stage_sysroot_python() {
    local sysroot_dir
    local entry

    if [ -n "$ROOTFS_SYSROOT_DIR" ]; then
        sysroot_dir="$ROOTFS_SYSROOT_DIR"
    else
        sysroot_dir="$ROOTFS_REPO/toolchain/sysroot"
    fi

    if [ -d "$sysroot_dir/bin" ]; then
        for entry in "$sysroot_dir"/bin/python "$sysroot_dir"/bin/python[0-9] "$sysroot_dir"/bin/python[0-9].[0-9]*; do
            [ -e "$entry" ] || [ -L "$entry" ] || continue
            rootfs_copy_entry "$entry" "/usr/bin/$(basename "$entry")"
        done
    fi

    if [ -d "$sysroot_dir/lib" ]; then
        for entry in "$sysroot_dir"/lib/python[0-9]*; do
            [ -d "$entry" ] || continue
            rootfs_copy_entry "$entry" "/usr/lib/$(basename "$entry")"
        done
    fi
}

rootfs_stage_busybox_install() {
    local bb_install
    local source_dir
    local target_dir
    local entry

    if [ -n "$ROOTFS_BUSYBOX_INSTALL_DIR" ]; then
        bb_install="$ROOTFS_BUSYBOX_INSTALL_DIR"
    else
        bb_install="$ROOTFS_REPO/toolchain/busybox-install"
    fi
    if [ ! -d "$bb_install" ]; then
        return 0
    fi

    mkdir -p "$ROOTFS_STAGING/usr/bin" "$ROOTFS_STAGING/usr/sbin" "$ROOTFS_STAGING/usr/lib"

    if [ -d "$bb_install/lib" ]; then
        for entry in "$bb_install"/lib/*; do
            [ -e "$entry" ] || continue
            rootfs_copy_entry "$entry" "/usr/lib/$(basename "$entry")"
        done
    fi

    for source_dir in bin sbin usr/bin usr/sbin; do
        case "$source_dir" in
            bin|usr/bin)
                target_dir="/usr/bin"
                ;;
            sbin|usr/sbin)
                target_dir="/usr/sbin"
                ;;
            *)
                continue
                ;;
        esac

        if [ ! -d "$bb_install/$source_dir" ]; then
            continue
        fi

        for entry in "$bb_install/$source_dir"/*; do
            [ -e "$entry" ] || continue
            rootfs_copy_entry "$entry" "$target_dir/$(basename "$entry")"
        done
    done
}

rootfs_stage_manifest() {
    local manifest
    local action
    local source
    local target
    local mode
    local extra

    manifest="$ROOTFS_REPO/configs/rootfs/aliases.tsv"
    if [ ! -f "$manifest" ]; then
        return 0
    fi

    while IFS=$'\t' read -r action source target mode extra; do
        case "$action" in
            ""|\#*)
                continue
                ;;
            copy)
                rootfs_copy_entry "$source" "$target"
                ;;
            copy-mode)
                if [ -z "$mode" ] || [ -n "$extra" ]; then
                    echo "Invalid copy-mode rootfs manifest entry in $manifest" >&2
                    return 1
                fi
                rootfs_copy_entry "$source" "$target" "$mode"
                ;;
            copy-glob)
                if [ -n "$mode" ] || [ -n "$extra" ]; then
                    echo "Invalid copy-glob rootfs manifest entry in $manifest" >&2
                    return 1
                fi
                rootfs_copy_glob_entry "$source" "$target"
                ;;
            symlink)
                rootfs_symlink_entry "$source" "$target"
                ;;
            *)
                echo "Unknown rootfs manifest action '$action' in $manifest" >&2
                return 1
                ;;
        esac
    done < "$manifest"
}

rootfs_stage_etc() {
    local tz_source=""

    mkdir -p "$ROOTFS_STAGING/etc/dropbear"

    cat > "$ROOTFS_STAGING/etc/passwd" <<'EOF'
root:!:0:0:root:/root:/bin/bash
EOF

    cat > "$ROOTFS_STAGING/etc/group" <<'EOF'
root:x:0:root
EOF

    cat > "$ROOTFS_STAGING/etc/profile" <<'EOF'
export USER="${USER:-root}"
export HOSTNAME="${HOSTNAME:-wos}"
export HOME="${HOME:-/root}"
export SHELL="${SHELL:-/bin/bash}"
export TERM="${TERM:-xterm-256color}"
export PS1="$USER@$HOSTNAME:\w\$ "
export ENV="/etc/profile"
export CMAKE_GENERATOR="${CMAKE_GENERATOR:-Ninja}"
EOF

    cat > "$ROOTFS_STAGING/etc/shells" <<'EOF'
/bin/bash
/usr/bin/bash
/bin/sh
/usr/bin/sh
EOF

    cat > "$ROOTFS_STAGING/etc/filesystems" <<'EOF'
fat32
vfat
tmpfs
EOF

    if [ -f "$ROOTFS_REPO/configs/vfstab" ]; then
        rootfs_copy_entry "configs/vfstab" "/etc/vfstab"
    else
        cat > "$ROOTFS_STAGING/etc/vfstab" <<'EOF'
# prefix route
/wki local
/proc local
/dev local
/tmp local
/run local
/ host
EOF
        rootfs_record_managed_path "/etc/vfstab"
    fi

    if [ -f "$ROOTFS_REPO/configs/system.conf" ]; then
        # shellcheck disable=SC1091
        source "$ROOTFS_REPO/configs/system.conf"
        printf '%s' "${WOS_HOSTNAME:-wos}" > "$ROOTFS_STAGING/etc/hostname"
    else
        printf '%s' "wos" > "$ROOTFS_STAGING/etc/hostname"
    fi

    if [ -f "/usr/share/zoneinfo/Etc/UTC" ]; then
        tz_source="/usr/share/zoneinfo/Etc/UTC"
    elif [ -f "/usr/share/zoneinfo/UTC" ]; then
        tz_source="/usr/share/zoneinfo/UTC"
    fi

    if [ -n "$tz_source" ]; then
        rootfs_copy_entry "$tz_source" "/etc/localtime"
    fi
}

rootfs_append_authorized_keys() {
    local line
    local source="$1"
    local target="$2"

    [ -f "$source" ] || return 0

    while IFS= read -r line || [ -n "$line" ]; do
        [ -n "$line" ] || continue
        if ! grep -qxF -- "$line" "$target" 2>/dev/null; then
            printf '%s\n' "$line" >> "$target"
        fi
    done < "$source"
}

rootfs_stage_root_home() {
    local authorized_keys
    local keyfile
    local repo_authorized_keys
    local ssh_pubkey

    mkdir -p "$ROOTFS_STAGING/root/.ssh"
    chmod 700 "$ROOTFS_STAGING/root/.ssh"
    rootfs_record_managed_path "/root/.ssh"

    authorized_keys="$ROOTFS_STAGING/root/.ssh/authorized_keys"
    repo_authorized_keys="$ROOTFS_REPO/configs/rootfs/root/.ssh/authorized_keys"
    : > "$authorized_keys"

    ssh_pubkey=""
    for keyfile in ~/.ssh/id_ed25519.pub ~/.ssh/id_rsa.pub ~/.ssh/id_ecdsa.pub; do
        if [ -f "$keyfile" ]; then
            ssh_pubkey="$keyfile"
            break
        fi
    done

    rootfs_append_authorized_keys "$repo_authorized_keys" "$authorized_keys"
    if [ -n "$ssh_pubkey" ]; then
        rootfs_append_authorized_keys "$ssh_pubkey" "$authorized_keys"
    fi

    if [ -s "$authorized_keys" ]; then
        chmod 600 "$authorized_keys"
        rootfs_record_managed_path "/root/.ssh/authorized_keys"
    else
        rm -f "$authorized_keys"
    fi
}

rootfs_stage_srv() {
    mkdir -p "$ROOTFS_STAGING/srv"
    if [ -d "$ROOTFS_REPO/configs/drive/srv" ]; then
        cp -r "$ROOTFS_REPO/configs/drive/srv/." "$ROOTFS_STAGING/srv/" 2>/dev/null || true
    fi

    printf '%s\n' "Hello from XFS filesystem!" > "$ROOTFS_STAGING/srv/hello.txt"
    printf '%s' "Binary test data 1234567890ABCDEF" > "$ROOTFS_STAGING/srv/test.bin"

    rootfs_record_managed_path "/srv/hello.txt"
    rootfs_record_managed_path "/srv/test.bin"
}

rootfs_stage_misc_dirs() {
    mkdir -p "$ROOTFS_STAGING/home"
    mkdir -p "$ROOTFS_STAGING/dev/pts"
    mkdir -p "$ROOTFS_STAGING/tmp"
    mkdir -p "$ROOTFS_STAGING/run"
    mkdir -p "$ROOTFS_STAGING/var/log/journal"
    mkdir -p "$ROOTFS_STAGING/oldroot"
}

rootfs_finalize_usr_merge_link() {
    local link_name="$1"
    local target="$2"
    local link_path="$ROOTFS_STAGING/$link_name"
    local target_path="$ROOTFS_STAGING/$target"
    local entry

    mkdir -p "$target_path"

    if [ -d "$link_path" ] && [ ! -L "$link_path" ]; then
        # Earlier staging steps may place explicit aliases under /bin, /sbin, or /lib.
        # Preserve those entries under /usr before replacing the top-level path.
        rm -f "$link_path/$link_name"
        for entry in "$link_path"/* "$link_path"/.[!.]* "$link_path"/..?*; do
            [ -e "$entry" ] || [ -L "$entry" ] || continue
            mv -f "$entry" "$target_path/"
        done
        rmdir "$link_path"
    else
        rm -f "$link_path"
    fi

    ln -sfn "$target" "$link_path"
    rootfs_record_managed_path "/$link_name"
}

rootfs_finalize_usr_merge() {
    rootfs_finalize_usr_merge_link lib usr/lib
    rootfs_finalize_usr_merge_link bin usr/bin
    rootfs_finalize_usr_merge_link sbin usr/sbin
}

rootfs_write_managed_paths() {
    local managed_out

    managed_out="$ROOTFS_STAGING/etc/wos-managed-paths"
    mkdir -p "$(dirname "$managed_out")"
    sort -u "$ROOTFS_MANAGED_TMP" > "$managed_out"
}

rootfs_stage_tree() {
    ROOTFS_REPO="$1"
    ROOTFS_STAGING="$2"
    ROOTFS_MANAGED_TMP="$ROOTFS_STAGING/.wos-managed-paths.tmp"
    ROOTFS_CHANGED=1

    mkdir -p "$ROOTFS_STAGING"
    : > "$ROOTFS_MANAGED_TMP"

    rootfs_stage_sysroot_libs
    rootfs_stage_sysroot_headers
    rootfs_stage_sysroot_cmake_data
    rootfs_stage_sysroot_python
    rootfs_stage_busybox_install
    rootfs_stage_manifest
    rootfs_stage_etc
    rootfs_stage_root_home
    rootfs_stage_srv
    rootfs_stage_misc_dirs
    rootfs_finalize_usr_merge
    rootfs_record_managed_path "$ROOTFS_CONTENT_MANIFEST"
    rootfs_record_managed_path "/etc/wos-managed-paths"
    rootfs_write_managed_paths
    rm -f "$ROOTFS_MANAGED_TMP"
    rootfs_write_content_manifest "$ROOTFS_STAGING$ROOTFS_CONTENT_MANIFEST"
}

rootfs_remove_old_managed_paths() {
    local disk="$1"
    local new_managed="${2:-}"
    local managed_tmp
    local stale_tmp
    local cmd_tmp
    local read_log
    local read_status
    local path_count
    local path_list

    managed_tmp=$(mktemp)
    stale_tmp=$(mktemp)
    cmd_tmp=$(mktemp)
    read_log=$(mktemp)

    wos_qcow_prepare_libguestfs_env

    if guestfish --ro -a "$disk" > "$read_log" 2>&1 <<EOF
run
mount /dev/sda1 /
download /etc/wos-managed-paths $managed_tmp
EOF
    then
        if [ -n "$new_managed" ] && [ -f "$new_managed" ]; then
            grep -Fvx -f "$new_managed" "$managed_tmp" > "$stale_tmp" || true
            path_list="$stale_tmp"
        else
            path_list="$managed_tmp"
        fi

        path_count=$(grep -cve '^[[:space:]]*$' "$path_list" || true)
        if [ "$path_count" -gt 0 ]; then
            {
                echo "run"
                echo "mount /dev/sda1 /"
                # Clean up legacy usr-merge artifacts from older image creation
                # flows that could leave recursive links like /bin/bin.
                echo "rm-f /usr/bin/bin"
                echo "rm-f /usr/sbin/sbin"
                echo "rm-f /usr/lib/lib"
                echo "rm-f /bin/bin"
                echo "rm-f /sbin/sbin"
                echo "rm-f /lib/lib"
                while IFS= read -r path; do
                    [ -n "$path" ] || continue
                    printf 'glob rm-rf %s\n' "$path"
                done < "$path_list"
                echo "sync"
            } > "$cmd_tmp"
            wos_qcow_guestfish "remove stale managed rootfs paths from qcow image" "$disk" --rw -a "$disk" < "$cmd_tmp"
        fi
    else
        read_status=$?
        if wos_qcow_log_has_lock "$read_log" || wos_qcow_log_has_corruption "$read_log"; then
            wos_qcow_report_failure "read managed rootfs paths from qcow image" "$disk" "$read_log"
            rm -f "$managed_tmp" "$stale_tmp" "$cmd_tmp" "$read_log"
            return "$read_status"
        fi
    fi

    rm -f "$managed_tmp" "$stale_tmp" "$cmd_tmp" "$read_log"
}
