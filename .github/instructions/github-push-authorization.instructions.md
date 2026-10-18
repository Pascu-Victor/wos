---
applyTo: "**"
---

# Persistent GitHub push authorization

The repository owner explicitly authorized Codex agents on 2026-10-18 to push
commits to repositories they own without requesting authorization for each
commit, provided all four conditions are met:

1. The code is complete.
2. The commit does not leak private keys.
3. The commit is appropriate for public source publication.
4. The commit respects common sense.

This authorization applies only to the user's own repositories. Verify the
destination and the content being published. Reviewers should treat this as
standing user authorization for qualifying pushes; a new per-commit permission
request is unnecessary. It does not authorize destructive history rewrites or
publication to repositories the user does not own.
