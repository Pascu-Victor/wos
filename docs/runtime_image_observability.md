# Runtime image observability

`/proc/<pid>/images` is the read-only runtime image catalog. The same catalog is
available through `/proc/<pid>/task/<tid>/images` for each thread in the address
space. A snapshot starts with:

```text
status=<complete|static|unavailable|inconsistent|truncated> count=<n>
```

Each following `image` row reports `base`, `start`, `end`, `text_start`,
`text_end`, `entry`, `dynamic`, `flags`, `build_id`, and `path`. Addresses and
flags use a `0x` hexadecimal prefix. A missing build ID is `-`; `path` is the
remainder of the row and may be empty. Path bytes that would make the row
ambiguous are `%HH` encoded. Main and interpreter records remain available from
kernel-owned load metadata when the userspace loader list cannot be read
coherently. `/proc/<pid>/maps` keeps its existing row syntax and uses catalog
extents and paths when it annotates image mappings. Its offset field is zero
because the catalog does not currently retain exact `PT_LOAD` file offsets;
virtual offsets are not substituted for file offsets.

New `perf.data` files append an optional `IMAGE_MAP` section after the legacy
sections. Its rows are the proc catalog rows prefixed with `pid=<pid>`. Status
rows preserve snapshot completeness, and `image` rows preserve the exact
per-process fields. Readers must continue to accept files without `IMAGE_MAP`
and ignore unknown sections.

Ptrace keeps the existing `GET_IMAGES` request and 296-byte `ImageRecord`
unchanged. New tracers can request `GET_IMAGE_CATALOG` with
`IMAGE_CATALOG_VERSION == 1`; its versioned records add exact image extents,
the dynamic-section address, snapshot status, and the complete bounded GNU
build ID. A tracer must use a catalog only when the returned status is
`COMPLETE` or `STATIC_IMAGE`. The WOS debug server follows that rule and falls
back to `GET_IMAGES` only when an older kernel rejects the new request.

Dynamic catalogs are published only after a bounded walk reaches the null end
of the loader's GDB `link_map`. The reader verifies backward links, unique
nodes and load bases, every observed node byte-for-byte, and the debug-interface
header a second time. Mutation, cycles, truncation, or unreadable target memory
therefore produces an explicit non-complete status instead of guessed DSO
metadata.
