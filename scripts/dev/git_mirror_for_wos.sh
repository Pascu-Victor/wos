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
DEFAULT_WOS_HOST="${WOS_GIT_WOS_HOST:-root@wos-0.wos}"
DEFAULT_WOS_PATH="${WOS_GIT_WOS_PATH:-/tmp/wos-git-repos}"

usage() {
    cat <<EOF
Usage:
  scripts/dev/git_mirror_for_wos.sh list
  scripts/dev/git_mirror_for_wos.sh mirror [--remote-only]
  scripts/dev/git_mirror_for_wos.sh snapshot
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
  exact checked-out submodule commits. Use this for WOS clone/submodule
  workflow testing when full Git history is unnecessary or too large.

  serve-http exposes repositories as:
    http://HOST:PORT/cgi-bin/git/OWNER/REPO.git

  If WOS cannot reach the host HTTP port, use sync-file-mirror and the file
  rewrite printed at the end. That still avoids GitHub completely. By default
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
    } | awk '!seen[$0]++'
}

local_source_for_url() {
    local url="$1"
    local top_url

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
    git -C "$target" symbolic-ref HEAD "$ref"
    git -C "$target" config uploadpack.allowReachableSHA1InWant true
    git -C "$target" update-server-info
}

cmd_snapshot() {
    while (($# > 0)); do
        case "$1" in
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
    top_sha="$(git -C "$WORKSPACE_ROOT" rev-parse HEAD)"
    snapshot_one "$top_url" "$WORKSPACE_ROOT" "$top_sha" "$top_ref"

    while IFS= read -r name; do
        local path url sha

        path="$(git -C "$WORKSPACE_ROOT" config -f .gitmodules --get "submodule.$name.path")"
        url="$(git -C "$WORKSPACE_ROOT" config -f .gitmodules --get "submodule.$name.url")"
        if [ ! -d "$WORKSPACE_ROOT/$path" ]; then
            die "submodule path is missing: $path"
        fi
        sha="$(git -C "$WORKSPACE_ROOT/$path" rev-parse HEAD)"
        snapshot_one "${url%.git}.git" "$WORKSPACE_ROOT/$path" "$sha" refs/heads/wos-snapshot
    done < <(git -C "$WORKSPACE_ROOT" config -f .gitmodules --get-regexp '^submodule\..*\.path$' |
        sed -E 's/^submodule\.([^.]*)\.path .*/\1/')
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
    need_cmd scp
    need_cmd tar
    [ -d "$REPOS_ROOT" ] || die "repo root does not exist: $REPOS_ROOT; run mirror first"

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

    echo "Copying $REPOS_ROOT to $wos_host:$wos_path"
    ssh "$wos_host" sh -s -- "$wos_path" "$remote_copied_path" "$remote_archive_path" <<'EOF'
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
    scp -O "$archive" "$wos_host:$remote_archive_path"
    ssh "$wos_host" "mkdir -p -- $quoted_remote_copied_path && tar xf $quoted_remote_archive_path -C $quoted_remote_copied_path && rm -f -- $quoted_remote_archive_path"
    ssh "$wos_host" sh -s -- "$wos_path" "$remote_copied_path" <<'EOF'
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
