#!/bin/bash
# Build and serve a local GitHub mirror for WOS development VMs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

MIRROR_ROOT="${WOS_GIT_MIRROR_ROOT:-${XDG_CACHE_HOME:-$HOME/.cache}/wos/git-mirror}"
REPOS_ROOT="${WOS_GIT_REPOS_ROOT:-$MIRROR_ROOT/repos}"
HTTP_ROOT="${WOS_GIT_HTTP_ROOT:-$MIRROR_ROOT/http}"
DEFAULT_BIND="${WOS_GIT_HTTP_BIND:-0.0.0.0}"
DEFAULT_PORT="${WOS_GIT_HTTP_PORT:-8019}"
DEFAULT_WOS_HOST="${WOS_GIT_WOS_HOST:-wos-0}"
DEFAULT_WOS_PATH="${WOS_GIT_WOS_PATH:-/tmp/wos-git-repos}"
WORKTREE_SNAPSHOT_REPO=""
WORKTREE_INDEX_FILE=""
WORKTREE_SNAPSHOT_SHA=""
declare -a WORKTREE_TEMP_REPOS=()
declare -a WORKTREE_TEMP_INDEX_FILES=()
declare -a WORKTREE_SUBMODULE_PATHS=()
declare -a WORKTREE_SUBMODULE_SHAS=()

usage() {
    cat <<EOF
Usage:
  scripts/dev/git_mirror_for_wos.sh list
  scripts/dev/git_mirror_for_wos.sh mirror [--remote-only]
  scripts/dev/git_mirror_for_wos.sh snapshot [--worktree]
  scripts/dev/git_mirror_for_wos.sh serve-http [--bind ADDR] [--port PORT]
  scripts/dev/git_mirror_for_wos.sh print-wos-config http HOST [PORT]
  scripts/dev/git_mirror_for_wos.sh print-wos-config file PATH
  scripts/dev/git_mirror_for_wos.sh sync-file-mirror [--allow-non-temp] [WOS_HOST] [REMOTE_PATH]

Defaults:
  mirror root   $MIRROR_ROOT
  repo root     $REPOS_ROOT
  HTTP bind     $DEFAULT_BIND
  HTTP port     $DEFAULT_PORT
  WOS host      $DEFAULT_WOS_HOST
  WOS path      $DEFAULT_WOS_PATH

Environment:
  WOS_GIT_MIRROR_ROOT  Persistent host-side mirror root.
  WOS_GIT_REPOS_ROOT   Bare repository root served by smart HTTP.
  WOS_GIT_HTTP_ROOT    Generated Python CGI document root.
  WOS_GIT_HTTP_BIND    Address for serve-http.
  WOS_GIT_HTTP_PORT    Port for serve-http.
  WOS_GIT_WOS_HOST     Default host for sync-file-mirror.
  WOS_GIT_WOS_PATH     Default remote mirror path for sync-file-mirror.

Notes:
  mirror uses local checkout/submodule repositories first when present, so a
  prepared host checkout does not immediately re-download everything from
  GitHub. Pass --remote-only to force GitHub remotes.

  snapshot creates shallow bare repositories for the current checkout and the
  exact checked-out submodule commits. Use --worktree to include uncommitted
  non-ignored worktree files, including dirty submodule worktrees, in temporary
  synthetic commits without touching the real indexes. Local Meson wrap-git checkouts, such as mlibc fallback
  dependencies, are also snapshotted when their checked-out commit matches the
  wrap revision. Use this for WOS clone/submodule workflow testing when full Git
  history is unnecessary or too large. This is the preferred debug path for
  pre-staged self-host sources: WOS still runs a depth-1 Git clone from the
  mirror instead of receiving a copied source tree.

  serve-http exposes repositories as:
    http://HOST:PORT/cgi-bin/git/OWNER/REPO.git

  If WOS cannot reach the host HTTP port, use sync-file-mirror and the file
  rewrite printed at the end. That copies the bare mirror repositories only,
  not the live workspace, and still avoids GitHub completely. By default
  sync-file-mirror only replaces paths under /tmp or /var/tmp; pass
  --allow-non-temp for large mirrors that need persistent WOS disk space.
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "$1 is not installed or not in PATH"
}

reject_source_tree_repos_root() {
    if [ -f "$REPOS_ROOT/.gitmodules" ] || [ -d "$REPOS_ROOT/.git" ]; then
        die "refusing to treat a source checkout as the Git mirror root: $REPOS_ROOT"
    fi
}

cleanup_worktree_snapshot() {
    if [ -n "$WORKTREE_INDEX_FILE" ]; then
        rm -f -- "$WORKTREE_INDEX_FILE"
        WORKTREE_INDEX_FILE=""
    fi
    if [ -n "$WORKTREE_SNAPSHOT_REPO" ]; then
        rm -rf -- "$WORKTREE_SNAPSHOT_REPO"
        WORKTREE_SNAPSHOT_REPO=""
    fi
    local path
    for path in "${WORKTREE_TEMP_INDEX_FILES[@]}"; do
        [ -n "$path" ] && rm -f -- "$path"
    done
    for path in "${WORKTREE_TEMP_REPOS[@]}"; do
        [ -n "$path" ] && rm -rf -- "$path"
    done
    WORKTREE_TEMP_INDEX_FILES=()
    WORKTREE_TEMP_REPOS=()
    WORKTREE_SUBMODULE_PATHS=()
    WORKTREE_SUBMODULE_SHAS=()
    WORKTREE_SNAPSHOT_SHA=""
}

normalize_github_path() {
    local url="$1"
    local path

    case "$url" in
        https://github.com/*)
            path="${url#https://github.com/}"
            ;;
        git@github.com:*)
            path="${url#git@github.com:}"
            ;;
        ssh://git@github.com/*)
            path="${url#ssh://git@github.com/}"
            ;;
        *)
            die "unsupported non-GitHub URL: $url"
            ;;
    esac

    path="${path%.git}.git"
    printf '%s\n' "$path"
}

submodule_urls() {
    git -C "$WORKSPACE_ROOT" config -f .gitmodules --get-regexp '^submodule\..*\.url$' |
        awk '{ print $2 }'
}

wrap_value() {
    local wrap="$1"
    local key="$2"

    sed -n "s/^[[:space:]]*$key[[:space:]]*=[[:space:]]*//p" "$wrap" | sed -n '1p'
}

meson_git_wraps() {
    local wrap_root="$WORKSPACE_ROOT/toolchain/src/mlibc/subprojects"
    local wrap

    [ -d "$wrap_root" ] || return 0

    for wrap in "$wrap_root"/*.wrap; do
        [ -f "$wrap" ] || continue

        local url revision directory source_path
        url="$(wrap_value "$wrap" url)"
        revision="$(wrap_value "$wrap" revision)"
        directory="$(wrap_value "$wrap" directory)"
        if [ -z "$url" ] || [ -z "$revision" ]; then
            continue
        fi
        case "$url" in
            https://github.com/*|git@github.com:*|ssh://git@github.com/*)
                ;;
            *)
                continue
                ;;
        esac
        if [ -z "$directory" ]; then
            directory="$(basename "$wrap" .wrap)"
        fi
        source_path="$wrap_root/$directory"
        printf '%s\t%s\t%s\t%s\n' "$url" "$revision" "$source_path" "$wrap"
    done
}

top_level_url() {
    local url
    url="$(git -C "$WORKSPACE_ROOT" config --get remote.origin.url || true)"
    if [ -z "$url" ]; then
        url="https://github.com/Pascu-Victor/wos.git"
    fi
    printf '%s\n' "${url%.git}.git"
}

repo_urls() {
    {
        top_level_url
        submodule_urls
        meson_git_wraps | awk -F '\t' '{ print $1 }'
    } | awk '!seen[$0]++'
}

local_meson_wrap_source_for_url() {
    local wanted_url="$1"
    local url revision source_path wrap current

    while IFS="$(printf '\t')" read -r url revision source_path wrap; do
        if [ "$url" != "$wanted_url" ]; then
            continue
        fi
        if [ ! -d "$source_path" ]; then
            return 1
        fi
        current="$(git -C "$source_path" rev-parse HEAD 2>/dev/null || true)"
        if [ "$current" != "$revision" ]; then
            die "Meson wrap source $source_path is at $current, expected $revision from $wrap"
        fi
        printf '%s\n' "$source_path"
        return 0
    done < <(meson_git_wraps)

    return 1
}

local_source_for_url() {
    local url="$1"
    local top_url
    local wrap_source

    top_url="$(top_level_url)"
    if [ "$url" = "$top_url" ]; then
        printf '%s\n' "$WORKSPACE_ROOT"
        return 0
    fi

    while IFS= read -r name; do
        local sub_url path gitdir

        sub_url="$(git -C "$WORKSPACE_ROOT" config -f .gitmodules --get "submodule.$name.url")"
        if [ "${sub_url%.git}.git" != "$url" ]; then
            continue
        fi

        path="$(git -C "$WORKSPACE_ROOT" config -f .gitmodules --get "submodule.$name.path")"
        if [ ! -d "$WORKSPACE_ROOT/$path" ]; then
            return 1
        fi

        gitdir="$(git -C "$WORKSPACE_ROOT/$path" rev-parse --git-dir 2>/dev/null || true)"
        if [ -n "$gitdir" ]; then
            printf '%s\n' "$WORKSPACE_ROOT/$path"
            return 0
        fi

        return 1
    done < <(git -C "$WORKSPACE_ROOT" config -f .gitmodules --get-regexp '^submodule\..*\.path$' |
        sed -E 's/^submodule\.([^.]*)\.path .*/\1/')

    if wrap_source="$(local_meson_wrap_source_for_url "$url")"; then
        printf '%s\n' "$wrap_source"
        return 0
    fi

    return 1
}

mirror_one() {
    local url="$1"
    local prefer_local="$2"
    local github_path target source

    github_path="$(normalize_github_path "$url")"
    target="$REPOS_ROOT/$github_path"
    mkdir -p "$(dirname "$target")"

    if [ -d "$target" ]; then
        echo "Updating $github_path"
        git -C "$target" remote update --prune
        git -C "$target" update-server-info
        return
    fi

    source="$url"
    if [ "$prefer_local" -eq 1 ] && source="$(local_source_for_url "$url")"; then
        echo "Mirroring $github_path from local checkout"
    else
        source="$url"
        echo "Mirroring $github_path from remote"
    fi

    git clone --mirror "$source" "$target"
    git -C "$target" update-server-info
}

cmd_list() {
    repo_urls
}

cmd_mirror() {
    local prefer_local=1

    while (($# > 0)); do
        case "$1" in
            --remote-only)
                prefer_local=0
                ;;
            -h|--help)
                usage
                return 0
                ;;
            *)
                die "unknown mirror option: $1"
                ;;
        esac
        shift
    done

    need_cmd git
    mkdir -p "$REPOS_ROOT"

    while IFS= read -r url; do
        mirror_one "$url" "$prefer_local"
    done < <(repo_urls)
}

snapshot_one() {
    local url="$1"
    local source="$2"
    local sha="$3"
    local ref="$4"
    local github_path target

    github_path="$(normalize_github_path "$url")"
    target="$REPOS_ROOT/$github_path"
    mkdir -p "$(dirname "$target")"
    rm -rf -- "$target"

    echo "Snapshotting $github_path at $sha"
    git init --bare "$target" >/dev/null
    git -C "$target" fetch --depth=1 "$source" "$sha:$ref"
    hydrate_snapshot_from_worktree "$target" "$source" "$sha"
    if ! snapshot_is_complete "$target" "$sha"; then
        if git -C "$source" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
            echo "Local snapshot for $github_path is incomplete; fetching shallow objects from $url" >&2
            rm -rf -- "$target"
            git init --bare "$target" >/dev/null
            git -C "$target" fetch --depth=1 "$url" "$sha:$ref"
            hydrate_snapshot_from_worktree "$target" "$source" "$sha"
        fi
    fi
    if ! snapshot_is_complete "$target" "$sha"; then
        git -C "$target" fsck --connectivity-only --no-dangling >&2 || true
        git -C "$target" rev-list --objects --missing=print "$sha" | sed -n 's/^?//p' >&2 || true
        die "snapshot for $github_path is missing objects required by $sha"
    fi
    git -C "$target" symbolic-ref HEAD "$ref"
    git -C "$target" config uploadpack.allowReachableSHA1InWant true
    git -C "$target" repack -ad >/dev/null
    git -C "$target" prune-packed >/dev/null
    remove_pack_bitmaps "$target"
    git -C "$target" update-server-info
}

remove_pack_bitmaps() {
    local target="$1"
    local bitmap

    for bitmap in "$target"/objects/pack/*.bitmap; do
        [ -e "$bitmap" ] || continue
        rm -f -- "$bitmap"
    done
}

snapshot_is_complete() {
    local target="$1"
    local treeish="$2"
    local missing_objects

    git -C "$target" fsck --connectivity-only --no-dangling >/dev/null 2>&1 || return 1
    missing_objects="$(mktemp "$MIRROR_ROOT/missing-target-objects.XXXXXX")"
    git -C "$target" rev-list --objects --missing=print "$treeish" |
        sed -n 's/^?//p' > "$missing_objects"
    if [ -s "$missing_objects" ]; then
        rm -f -- "$missing_objects"
        return 1
    fi
    rm -f -- "$missing_objects"
    return 0
}

hydrate_snapshot_from_worktree() {
    local target="$1"
    local source="$2"
    local treeish="$3"

    if [ ! -d "$source" ]; then
        return 0
    fi
    if ! git -C "$source" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        return 0
    fi

    local missing_objects
    missing_objects="$(mktemp "$MIRROR_ROOT/missing-objects.XXXXXX")"
    git -C "$target" rev-list --objects --missing=print "$treeish" |
        sed -n 's/^?//p' > "$missing_objects"
    if [ ! -s "$missing_objects" ]; then
        rm -f -- "$missing_objects"
        return 0
    fi

    local meta path mode type oid
    while IFS="$(printf '\t')" read -r meta path; do
        mode="${meta%% *}"
        meta="${meta#* }"
        type="${meta%% *}"
        oid="${meta##* }"
        if [ "$type" != "blob" ]; then
            continue
        fi
        if ! grep -qx "$oid" "$missing_objects"; then
            continue
        fi
        if git --git-dir="$target" cat-file -e "$oid" 2>/dev/null; then
            continue
        fi
        if [ -L "$source/$path" ]; then
            printf '%s' "$(readlink "$source/$path")" |
                GIT_DIR="$target" git hash-object -w --stdin >/dev/null
        elif [ -f "$source/$path" ]; then
            GIT_DIR="$target" GIT_WORK_TREE="$source" \
                git hash-object -w --path="$path" "$source/$path" >/dev/null
        else
            continue
        fi
        if ! git --git-dir="$target" cat-file -e "$oid" 2>/dev/null; then
            rm -f -- "$missing_objects"
            die "failed to hydrate blob $oid from $source/$path"
        fi
    done < <(git -C "$source" ls-tree -r "$treeish")
    rm -f -- "$missing_objects"
}

snapshot_meson_wraps() {
    local url revision source_path wrap current

    while IFS="$(printf '\t')" read -r url revision source_path wrap; do
        if [ ! -d "$source_path" ]; then
            echo "Skipping Meson wrap snapshot for missing local source: $wrap" >&2
            continue
        fi
        current="$(git -C "$source_path" rev-parse HEAD 2>/dev/null || true)"
        if [ "$current" != "$revision" ]; then
            die "Meson wrap source $source_path is at $current, expected $revision from $wrap"
        fi
        snapshot_one "${url%.git}.git" "$source_path" "$revision" refs/heads/wos-snapshot
    done < <(meson_git_wraps)
}

make_synthetic_worktree_source() {
    local source="$1"
    local ref="$2"
    local snapshot_ref="$3"
    local repo_var="$4"
    local sha_var="$5"
    local index repo parent tree message i snapshot_sha

    index="$(mktemp "$MIRROR_ROOT/worktree-index.XXXXXX")"
    repo="$(mktemp -d "$MIRROR_ROOT/worktree-snapshot.XXXXXX")"
    WORKTREE_TEMP_INDEX_FILES+=("$index")
    WORKTREE_TEMP_REPOS+=("$repo")

    git init --bare "$repo" >/dev/null
    git -C "$repo" fetch --depth=1 "$source" "$ref:refs/heads/wos-worktree-base" >/dev/null
    parent="$(git -C "$repo" rev-parse refs/heads/wos-worktree-base)"
    GIT_DIR="$repo" \
        GIT_WORK_TREE="$source" \
        GIT_INDEX_FILE="$index" \
        git read-tree refs/heads/wos-worktree-base
    GIT_DIR="$repo" \
        GIT_WORK_TREE="$source" \
        GIT_INDEX_FILE="$index" \
        git add -A

    if [ "$source" = "$WORKSPACE_ROOT" ]; then
        for ((i = 0; i < ${#WORKTREE_SUBMODULE_PATHS[@]}; ++i)); do
            GIT_DIR="$repo" \
                GIT_WORK_TREE="$source" \
                GIT_INDEX_FILE="$index" \
                git update-index --add --cacheinfo \
                "160000,${WORKTREE_SUBMODULE_SHAS[$i]},${WORKTREE_SUBMODULE_PATHS[$i]}"
        done
    fi

    tree="$(
        GIT_DIR="$repo" \
            GIT_WORK_TREE="$source" \
            GIT_INDEX_FILE="$index" \
            git write-tree
    )"
    message="WOS worktree snapshot $(date -u +%Y%m%dT%H%M%SZ)"
    snapshot_sha="$(
        GIT_AUTHOR_NAME="${GIT_AUTHOR_NAME:-WOS Snapshot}" \
            GIT_AUTHOR_EMAIL="${GIT_AUTHOR_EMAIL:-wos-snapshot@example.invalid}" \
            GIT_COMMITTER_NAME="${GIT_COMMITTER_NAME:-WOS Snapshot}" \
            GIT_COMMITTER_EMAIL="${GIT_COMMITTER_EMAIL:-wos-snapshot@example.invalid}" \
            GIT_DIR="$repo" \
            git commit-tree "$tree" -p "$parent" -m "$message"
    )"
    git -C "$repo" update-ref "$snapshot_ref" "$snapshot_sha"
    printf -v "$repo_var" '%s' "$repo"
    printf -v "$sha_var" '%s' "$snapshot_sha"
}

make_worktree_snapshot_source() {
    make_synthetic_worktree_source \
        "$WORKSPACE_ROOT" \
        HEAD \
        refs/heads/wos-worktree-snapshot \
        WORKTREE_SNAPSHOT_REPO \
        WORKTREE_SNAPSHOT_SHA
}

submodule_worktree_dirty() {
    local path="$1"
    [ -n "$(git -C "$WORKSPACE_ROOT/$path" status --porcelain=v1 --untracked-files=all)" ]
}

snapshot_configured_submodules() {
    local include_worktree="$1"

    while IFS= read -r name; do
        local path url sha source snapshot_repo

        path="$(git -C "$WORKSPACE_ROOT" config -f .gitmodules --get "submodule.$name.path")"
        url="$(git -C "$WORKSPACE_ROOT" config -f .gitmodules --get "submodule.$name.url")"
        source="$WORKSPACE_ROOT/$path"
        if [ ! -d "$source" ]; then
            die "submodule path is missing: $path"
        fi

        if [ "$include_worktree" -eq 1 ] && submodule_worktree_dirty "$path"; then
            snapshot_repo=""
            make_synthetic_worktree_source \
                "$source" \
                HEAD \
                refs/heads/wos-worktree-snapshot \
                snapshot_repo \
                sha
            WORKTREE_SUBMODULE_PATHS+=("$path")
            WORKTREE_SUBMODULE_SHAS+=("$sha")
            snapshot_one "${url%.git}.git" "$snapshot_repo" "$sha" refs/heads/wos-snapshot
        else
            sha="$(git -C "$source" rev-parse HEAD)"
            snapshot_one "${url%.git}.git" "$source" "$sha" refs/heads/wos-snapshot
        fi
    done < <(git -C "$WORKSPACE_ROOT" config -f .gitmodules --get-regexp '^submodule\..*\.path$' |
        sed -E 's/^submodule\.([^.]*)\.path .*/\1/')
}

cmd_snapshot() {
    local include_worktree=0

    while (($# > 0)); do
        case "$1" in
            --worktree)
                include_worktree=1
                ;;
            -h|--help)
                usage
                return 0
                ;;
            *)
                die "unknown snapshot option: $1"
                ;;
        esac
        shift
    done

    need_cmd git
    mkdir -p "$REPOS_ROOT"

    local top_url top_branch top_ref top_sha
    top_url="$(top_level_url)"
    top_branch="$(git -C "$WORKSPACE_ROOT" branch --show-current)"
    top_ref="refs/heads/${top_branch:-wos-snapshot}"
    if [ "$include_worktree" -eq 1 ]; then
        trap cleanup_worktree_snapshot EXIT
        snapshot_configured_submodules "$include_worktree"
        make_worktree_snapshot_source
        top_sha="$WORKTREE_SNAPSHOT_SHA"
        snapshot_one "$top_url" "$WORKTREE_SNAPSHOT_REPO" "$top_sha" "$top_ref"
    else
        top_sha="$(git -C "$WORKSPACE_ROOT" rev-parse HEAD)"
        snapshot_one "$top_url" "$WORKSPACE_ROOT" "$top_sha" "$top_ref"
        snapshot_configured_submodules "$include_worktree"
    fi

    snapshot_meson_wraps

    cleanup_worktree_snapshot
    trap - EXIT
}

write_cgi_wrapper() {
    local cgi_dir="$HTTP_ROOT/cgi-bin"
    mkdir -p "$cgi_dir"

    cat >"$cgi_dir/git" <<EOF
#!/bin/sh
export GIT_PROJECT_ROOT='$REPOS_ROOT'
export GIT_HTTP_EXPORT_ALL=1
exec git http-backend
EOF
    chmod +x "$cgi_dir/git"
}

cmd_serve_http() {
    local bind="$DEFAULT_BIND"
    local port="$DEFAULT_PORT"

    while (($# > 0)); do
        case "$1" in
            --bind)
                bind="${2:-}"
                [ -n "$bind" ] || die "--bind requires an address"
                shift
                ;;
            --port)
                port="${2:-}"
                [[ "$port" =~ ^[0-9]+$ ]] || die "--port requires a decimal port"
                shift
                ;;
            -h|--help)
                usage
                return 0
                ;;
            *)
                die "unknown serve-http option: $1"
                ;;
        esac
        shift
    done

    need_cmd git
    need_cmd python3
    [ -d "$REPOS_ROOT" ] || die "repo root does not exist: $REPOS_ROOT; run mirror first"

    write_cgi_wrapper
    echo "Serving $REPOS_ROOT"
    echo "Rewrite WOS with:"
    echo "  scripts/dev/git_mirror_for_wos.sh print-wos-config http HOST $port"
    exec python3 -m http.server --cgi "$port" --bind "$bind" --directory "$HTTP_ROOT"
}

cmd_print_wos_config() {
    local mode="${1:-}"
    shift || true

    case "$mode" in
        http)
            local host="${1:-}"
            local port="${2:-$DEFAULT_PORT}"
            [ -n "$host" ] || die "print-wos-config http requires HOST"
            cat <<EOF
git config --global url.http://$host:$port/cgi-bin/git/.insteadOf https://github.com/
EOF
            ;;
        file)
            local path="${1:-}"
            [ -n "$path" ] || die "print-wos-config file requires PATH"
            cat <<EOF
git config --global protocol.file.allow always
git config --global url.file://$path/.insteadOf https://github.com/
EOF
            ;;
        *)
            die "usage: print-wos-config http HOST [PORT] | file PATH"
            ;;
    esac
}

cmd_sync_file_mirror() {
    local allow_non_temp=0
    while (($# > 0)); do
        case "$1" in
            --allow-non-temp)
                allow_non_temp=1
                ;;
            -h|--help)
                usage
                return 0
                ;;
            --)
                shift
                break
                ;;
            -*)
                die "unknown sync-file-mirror option: $1"
                ;;
            *)
                break
                ;;
        esac
        shift
    done

    local wos_host="${1:-$DEFAULT_WOS_HOST}"
    local wos_path="${2:-$DEFAULT_WOS_PATH}"
    local remote_parent
    local remote_copied_path

    case "$wos_path" in
        /tmp/*|/var/tmp/*)
            ;;
        /*)
            if [ "$allow_non_temp" -ne 1 ]; then
                die "refusing to replace non-temporary WOS path: $wos_path (pass --allow-non-temp to override)"
            fi
            case "$wos_path" in
                /|/bin|/boot|/dev|/etc|/home|/lib|/lib64|/mnt|/oldroot|/proc|/root|/run|/sbin|/sys|/tmp|/usr|/var)
                    die "refusing to replace broad WOS path: $wos_path"
                    ;;
            esac
            ;;
        *)
            die "remote WOS path must be absolute: $wos_path"
            ;;
    esac

    need_cmd ssh
    need_cmd tar
    [ -d "$REPOS_ROOT" ] || die "repo root does not exist: $REPOS_ROOT; run mirror first"
    reject_source_tree_repos_root

    local remote_user="${WOS_SSH_USER:-root}"
    local remote_host="$wos_host"
    if [[ "$remote_host" == *@* ]]; then
        remote_user="${remote_host%@*}"
        remote_host="${remote_host#*@}"
    fi
    local remote_target
    remote_target="$("$WORKSPACE_ROOT/scripts/remote/wos_resolve.py" target "$remote_host")"
    local remote_login="$remote_user@$remote_target"
    local ssh_connect_timeout="${WOS_SSH_CONNECT_TIMEOUT:-10}"
    local ssh_server_alive_interval="${WOS_SSH_SERVER_ALIVE_INTERVAL:-15}"
    local ssh_server_alive_count_max="${WOS_SSH_SERVER_ALIVE_COUNT_MAX:-4}"
    local ssh_args=(
        -F /dev/null
        -o BatchMode=yes
        -o ConnectTimeout="$ssh_connect_timeout"
        -o ConnectionAttempts=1
        -o ServerAliveInterval="$ssh_server_alive_interval"
        -o ServerAliveCountMax="$ssh_server_alive_count_max"
        -o StrictHostKeyChecking=no
        -o UserKnownHostsFile=/dev/null
        -o LogLevel=ERROR
    )
    if [ -n "${WOS_SSH_PORT:-}" ]; then
        ssh_args+=(-p "$WOS_SSH_PORT")
    fi

    remote_parent="$(dirname "$wos_path")"
    remote_copied_path="$wos_path.copying"
    local remote_archive_path="$wos_path.tar.copying"
    local quoted_remote_copied_path
    local quoted_remote_archive_path
    printf -v quoted_remote_copied_path '%q' "$remote_copied_path"
    printf -v quoted_remote_archive_path '%q' "$remote_archive_path"
    local archive
    archive="$(mktemp "$MIRROR_ROOT/sync.XXXXXX.tar")"
    trap 'if [ -n "${archive:-}" ]; then rm -f -- "$archive"; fi; trap - RETURN' RETURN

    echo "Copying $REPOS_ROOT to $remote_login:$wos_path"
    ssh "${ssh_args[@]}" "$remote_login" sh -s -- "$wos_path" "$remote_copied_path" "$remote_archive_path" <<'EOF'
set -e
wos_path="$1"
remote_copied_path="$2"
remote_archive_path="$3"
rm -rf -- "$wos_path"
rm -rf -- "$remote_copied_path"
rm -f -- "$remote_archive_path"
mkdir -p -- "$(dirname "$wos_path")"
EOF
    tar -C "$REPOS_ROOT" -cf "$archive" .
    ssh "${ssh_args[@]}" "$remote_login" "cat > $quoted_remote_archive_path" < "$archive"
    local archive_size
    archive_size="$(wc -c < "$archive")"
    ssh "${ssh_args[@]}" "$remote_login" sh -s -- "$remote_archive_path" "$archive_size" <<'EOF'
set -e
remote_archive_path="$1"
expected_size="$2"
actual_size="$(stat -c %s "$remote_archive_path" 2>/dev/null || wc -c < "$remote_archive_path" 2>/dev/null || printf 0)"
if [ "$actual_size" != "$expected_size" ]; then
    echo "remote archive size mismatch: $remote_archive_path expected $expected_size got $actual_size" >&2
    exit 1
fi
EOF
    ssh "${ssh_args[@]}" "$remote_login" sh -s -- "$remote_archive_path" "$remote_copied_path" <<'EOF'
set -e
remote_archive_path="$1"
remote_copied_path="$2"
mkdir -p -- "$remote_copied_path"
tar xf "$remote_archive_path" -C "$remote_copied_path"
rm -f -- "$remote_archive_path"
if command -v chown >/dev/null 2>&1 && command -v id >/dev/null 2>&1; then
    chown -R "$(id -u):$(id -g)" "$remote_copied_path"
fi
if [ ! -d "$remote_copied_path/Pascu-Victor/wos.git" ]; then
    echo "synced mirror is missing Pascu-Victor/wos.git under $remote_copied_path" >&2
    exit 1
fi
EOF
    ssh "${ssh_args[@]}" "$remote_login" sh -s -- "$wos_path" "$remote_copied_path" <<'EOF'
set -e
wos_path="$1"
remote_copied_path="$2"
mv -- "$remote_copied_path" "$wos_path"
EOF
    echo
    echo "Run this inside WOS:"
    cmd_print_wos_config file "$wos_path"
}

main() {
    local command="${1:-}"
    if [ -n "$command" ]; then
        shift
    fi

    case "$command" in
        list)
            cmd_list "$@"
            ;;
        mirror)
            cmd_mirror "$@"
            ;;
        snapshot)
            cmd_snapshot "$@"
            ;;
        serve-http)
            cmd_serve_http "$@"
            ;;
        print-wos-config)
            cmd_print_wos_config "$@"
            ;;
        sync-file-mirror)
            cmd_sync_file_mirror "$@"
            ;;
        -h|--help|"")
            usage
            ;;
        *)
            die "unknown command: $command"
            ;;
    esac
}

main "$@"
