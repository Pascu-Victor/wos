# Local virtualization manuals

The local corpus is in `reference/virtualization/`, with acquisition provenance
in [manifest.json](../reference/virtualization/manifest.json) and availability in
[INDEX.md](../reference/virtualization/INDEX.md). It supports the native
Intel/AMD virtualization, userspace VMM and WKI goals. This directory is already
ignored by the repository's `reference/` rule; large vendor files and generated
indexes are local workspace material. The indexing script and this guide are
ordinary repository files. No kernel or cluster execution is involved.

## Sources

| ID | Reference | Acquisition |
| --- | --- | --- |
| `intel-sdm` | [Intel complete SDM, all combined volumes](https://cdrdv2.intel.com/v1/dl/getContent/671200), linked from the [publisher catalog](https://www.intel.com/content/www/us/en/developer/articles/technical/intel-sdm.html) | Downloaded revision 092; 5,363 physical PDF pages |
| `amd-apm-vol2` | [AMD64 APM Volume 2, System Programming, document 24593](https://www.amd.com/en/developer/sev.html) | Manually imported revision 3.45, July 2026; 875 physical PDF pages |
| `kvm-api` | [Linux KVM API](https://www.kernel.org/doc/html/latest/virt/kvm/api.html) | Downloaded HTML snapshot |
| `linux-x86-boot` | [Linux x86 boot protocol](https://docs.kernel.org/arch/x86/boot.html) | Downloaded HTML snapshot |
| `virtio-1.2` | [OASIS virtio 1.2](https://docs.oasis-open.org/virtio/virtio/v1.2/virtio-v1.2.html) | Downloaded HTML snapshot |

The manifest records acquisition time, original and resolved URLs, response
type/status, original size and SHA-256. In particular, `latest` HTML pages are
dated snapshots, not a promise that the local text remains current. Original
publisher files are retained unchanged.

The saved AMD PDF's cover identifies revision **3.45**, newer than the **3.44**
link used by the earlier automated attempt. The manifest retains that attempt's
URL and HTML response metadata separately from the successful local import;
the PDF cover and checksum identify the actual indexed edition.

Publisher access is limited to normal requests to official links. No challenge
scripts, CAPTCHA solving, browser impersonation, alternate mirrors or
bot-detection circumvention were used. The helper below is entirely offline.

## Search

Run from the repository root:

```sh
python3 scripts/dev/index_virtualization_manuals.py search 'VMX' --source intel-sdm
python3 scripts/dev/index_virtualization_manuals.py search 'EPT AND invalidation' --source intel-sdm
python3 scripts/dev/index_virtualization_manuals.py search 'VMCB' --source amd-apm-vol2
python3 scripts/dev/index_virtualization_manuals.py search '"nested paging"' --source amd-apm-vol2
python3 scripts/dev/index_virtualization_manuals.py search 'KVM_RUN' --source kvm-api
python3 scripts/dev/index_virtualization_manuals.py search 'boot_params' --source linux-x86-boot
python3 scripts/dev/index_virtualization_manuals.py search 'virtqueue' --source virtio-1.2
```

Search uses SQLite FTS5, supports quoted phrases and boolean operators, and
prints source, locator, original local file and excerpt. `--limit` is bounded
to 1–100. Restrict by source when a hardware term also occurs in the instruction
index or table of contents. An absent search result is not evidence that a
vendor lacks a feature; consult the original manual and conversion limitations.

PDF locators are **one-based physical PDF pages**, usable with `#page=N` in a
PDF viewer. Printed page labels such as chapter-relative `Vol. 3C` numbers are
different. Extracted per-page text is at
`reference/virtualization/pages/<source>/NNNNN.md`, with `intel-sdm` and
`amd-apm-vol2` as PDF source IDs. HTML results retain section
headings and publisher anchors in the local source file.

## Import a manually downloaded PDF

The AMD PDF is already imported and indexed. To repeat the import for a future
edition, use the AMD publisher's download interface manually. The document is
**AMD64 Architecture Programmer's Manual, Volume 2: System Programming,
document 24593**. Download the actual PDF, not an HTML
viewer saved with a `.pdf` extension. Then run:

```sh
python3 scripts/dev/index_virtualization_manuals.py import-pdf amd-apm-vol2 /absolute/path/to/24593.pdf
python3 scripts/dev/index_virtualization_manuals.py convert amd-apm-vol2
python3 scripts/dev/index_virtualization_manuals.py index
python3 scripts/dev/index_virtualization_manuals.py search 'VMCB' --source amd-apm-vol2
```

Alternatively place the file directly at
`reference/virtualization/originals/amd-apm-vol2.pdf`, then use that path for
`import-pdf`. Import records its checksum and updates availability. Verify the
PDF title and revision; import checks the PDF signature but cannot establish
publisher authenticity. It refuses to overwrite a different existing PDF.
Before replacing an edition, archive its source and generated artifacts, then
record the new cover revision and `expected_pdf_pages` in the manifest before
conversion; the current AMD page-count expectation is 875.

## Conversion and indexing

Installed at preparation time: Docling **2.126.0**, Core **2.95.0**, Docling Parse
**7.18.0**. PDF conversion uses the real Docling native pipeline, with eight
parser threads, JSON and Markdown outputs, and placeholder image export:

```sh
docling convert reference/virtualization/originals/intel-sdm.pdf --pipeline native --parser-threads 8 --image-export-mode placeholder --to json --to md --output reference/virtualization/converted
```

Prefer the helper for subsequent conversions because it records the conversion
checksum and source association in the manifest:

```sh
python3 scripts/dev/index_virtualization_manuals.py convert intel-sdm
python3 scripts/dev/index_virtualization_manuals.py index
```

The PDF index is built from Docling JSON text items and their page provenance.
Native conversion reads embedded PDF text without downloading models or running
OCR. It does **not** infer reading order, heading hierarchy, reconstruct tables,
or interpret diagrams. Search retrieves candidate pages; always verify bit
positions, column associations, pseudocode and control legality against the
original PDF. Image-only content is not searchable with this pipeline.

HTML snapshots are parsed locally with Python's `HTMLParser`; external scripts,
images and styles are never fetched or executed. Section text and Markdown are
retained, and bounded passages are inserted into the same FTS5 index. HTML
table text is searchable but the original HTML preserves the authoritative
table structure.

Indexing verifies original/conversion SHA-256 values and SQLite integrity. The
known page counts (Intel 5,363; AMD 875) are checked before recording completed
conversions.
The manifest distinguishes downloaded, conversion-pending and manually missing
sources; unavailable content is never replaced by an index of its error page.
Rebuilding writes a temporary database and replaces the previous index only
after successful completion. Search opens the database read-only.

## Files

```text
reference/virtualization/
  manifest.json                 Acquisition, hashes, conversion/index metadata
  INDEX.md                      Current source availability and passage counts
  index.sqlite3                 Searchable SQLite FTS5 database
  originals/                    Unmodified publisher downloads
  converted/                    Docling PDF JSON/Markdown; HTML-derived Markdown
  pages/<source>/NNNNN.md        Docling text grouped by physical PDF page
```

The script requires Python 3 with SQLite FTS5. PDF conversion additionally
requires a Docling CLI with the `convert --pipeline native` interface. This is
a lexical local index; it does not require embeddings, a vector service or an
OpenAI API key.
