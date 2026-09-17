#!/usr/bin/env python3
"""Offline conversion and search for the local virtualization manual corpus.

PDF conversion uses the installed Docling CLI, never a separate PDF extractor.
No networking, browser automation, model download, or publisher retry is used.
"""

from __future__ import annotations

import argparse
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone


REPO = Path(__file__).resolve().parents[2]
TITLES = {
    "intel-sdm": "Intel SDM — combined volumes 1, 2A–D, 3A–D and 4",
    "amd-apm-vol2": "AMD64 APM Volume 2 — System Programming (24593)",
    "kvm-api": "Linux KVM API",
    "linux-x86-boot": "Linux x86 boot protocol",
    "virtio-1.2": "OASIS virtio 1.2",
}


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def save_manifest(root: Path, manifest: dict) -> None:
    temporary = root / "manifest.json.tmp"
    temporary.write_text(json.dumps(manifest, indent=2) + "\n")
    temporary.replace(root / "manifest.json")


class HtmlSections(HTMLParser):
    """Preserve text, heading context and publisher anchors without fetching assets."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.sections = []
        self.title = "Introduction"
        self.anchor = ""
        self.pending_anchor = ""
        self.parts = []
        self.heading = None
        self.heading_parts = []
        self.skip = 0

    def flush(self):
        text = "\n".join(line.strip() for line in "".join(self.parts).splitlines() if line.strip())
        if text:
            self.sections.append((self.title, self.anchor, text))
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style"}:
            self.skip += 1
        if self.skip:
            return
        attrs = dict(attrs)
        if attrs.get("id") or attrs.get("name"):
            self.pending_anchor = attrs.get("id") or attrs["name"]
        if tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self.flush()
            self.heading = tag
            self.heading_parts = []
            self.anchor = self.pending_anchor
        if tag in {"p", "div", "br", "li", "tr", "pre", "section"}:
            self.parts.append("\n")
        elif tag in {"td", "th"}:
            self.parts.append(" | ")

    def handle_endtag(self, tag):
        if tag in {"script", "style"}:
            self.skip = max(0, self.skip - 1)
            return
        if self.skip:
            return
        if tag == self.heading:
            self.title = " ".join("".join(self.heading_parts).split())
            self.heading = None
            self.parts.append(self.title + "\n")
        elif tag in {"p", "div", "li", "tr", "pre", "section"}:
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.skip:
            (self.heading_parts if self.heading else self.parts).append(data)


def import_pdf(root, manifest, source_id, input_path):
    row = next(r for r in manifest["sources"] if r["id"] == source_id)
    if row["format"] != "pdf":
        raise ValueError("import-pdf only accepts PDF sources")
    with input_path.open("rb") as stream:
        if stream.read(5) != b"%PDF-":
            raise ValueError("File is not a PDF; HTML viewer/challenge content is not accepted")
    output = root / "originals" / f"{source_id}.pdf"
    if input_path.resolve() != output.resolve():
        if output.exists() and sha256(output) != sha256(input_path):
            raise ValueError(f"Refusing to overwrite a different source: archive {output} first")
        shutil.copyfile(input_path, output)
    row.update(status="downloaded", path=str(output.relative_to(root)),
               bytes=output.stat().st_size, sha256=sha256(output),
               acquired_via="manual local import",
               imported_at=datetime.now(timezone.utc).isoformat())
    row.pop("reason", None)
    row.pop("conversion", None)
    save_manifest(root, manifest)
    print(f"Imported {source_id}; inspect its title/revision, then run convert and index.")


def convert(root, manifest, source_id):
    selected = [r for r in manifest["sources"] if source_id is None or r["id"] == source_id]
    for row in selected:
        if row["status"] != "downloaded" or row["format"] != "pdf":
            continue
        original = root / row["path"]
        if sha256(original) != row["sha256"]:
            raise ValueError(f"Source checksum mismatch: {original}")
        output = root / "converted"
        output.mkdir(exist_ok=True)
        command = ["docling", "convert", str(original), "--pipeline", "native",
                   "--parser-threads", "8", "--image-export-mode", "placeholder",
                   "--to", "json", "--to", "md", "--output", str(output)]
        print("Running:", " ".join(command), flush=True)
        subprocess.run(command, check=True)
        record_conversion(root, row)
        row["conversion"]["cli_version"] = subprocess.check_output(
            ["docling", "--version"], text=True
        ).strip()
        save_manifest(root, manifest)


def record_conversion(root, row):
    path = root / "converted" / f"{row['id']}.json"
    data = json.loads(path.read_text())
    pages = data.get("pages", {})
    if not pages or not data.get("texts"):
        raise ValueError(f"Empty Docling conversion: {path}")
    expected = row.get("expected_pdf_pages")
    if expected is not None and len(pages) != expected:
        raise ValueError(f"Incomplete conversion: {len(pages)} of {expected} pages")
    row["conversion"] = {
        "tool": "Docling", "pipeline": "native", "json": str(path.relative_to(root)),
        "json_sha256": sha256(path), "pages": len(pages),
        "parser_threads": 8, "image_export_mode": "placeholder",
        "source_sha256": row["sha256"],
        "limitations": "Embedded text in parser order; no OCR, inferred headings, table reconstruction or diagram interpretation",
    }


def blocks(text, limit=3200):
    """Bound search excerpts by lines; PDF blocks never cross a physical page."""
    current = []
    size = 0
    for line in text.splitlines():
        for start in range(0, max(1, len(line)), limit):
            part = line[start:start + limit]
            if current and size + len(part) > limit:
                yield "\n".join(current)
                current, size = [], 0
            current.append(part)
            size += len(part) + 1
    if current:
        yield "\n".join(current)


def build_index(root, manifest):
    path = root / "index.sqlite3.tmp"
    path.unlink(missing_ok=True)
    db = sqlite3.connect(path)
    db.execute("CREATE VIRTUAL TABLE passages USING fts5(source UNINDEXED, title, locator UNINDEXED, url UNINDEXED, body)")
    inventory = []
    for row in manifest["sources"]:
        if row["status"] != "downloaded":
            inventory.append((row["id"], "manual download required", 0))
            continue
        original = root / row["path"]
        if sha256(original) != row["sha256"]:
            raise ValueError(f"Source checksum mismatch: {original}")
        units = []
        if row["format"] == "pdf":
            conversion = row.get("conversion")
            if not conversion:
                inventory.append((row["id"], "conversion pending", 0))
                continue
            if conversion["source_sha256"] != row["sha256"]:
                raise ValueError(f"Run convert for {row['id']} first")
            converted = root / conversion["json"]
            if sha256(converted) != conversion["json_sha256"]:
                raise ValueError(f"Conversion checksum mismatch: {converted}")
            document = json.loads(converted.read_text())
            pages = {int(number): [] for number in document["pages"]}
            for item in document["texts"]:
                for number in {p["page_no"] for p in item.get("prov", [])}:
                    pages[number].append(item.get("text", ""))
            page_dir = root / "pages" / row["id"]
            page_dir.mkdir(parents=True, exist_ok=True)
            for number, lines in sorted(pages.items()):
                text = "\n".join(lines)
                locator = f"PDF page {number}"
                local = f"pages/{row['id']}/{number:05d}.md"
                (root / local).write_text(f"# {TITLES[row['id']]} — {locator}\n\n" + text + "\n")
                units.append((locator, str(original.resolve()) + f"#page={number}", text))
            row["indexed_pages"] = len(pages)
            row["pages_with_text"] = sum(bool(lines) for lines in pages.values())
        else:
            parser = HtmlSections()
            parser.feed(original.read_text(encoding="utf-8"))
            parser.flush()
            output = root / "converted" / f"{row['id']}.md"
            output.parent.mkdir(exist_ok=True)
            output.write_text("\n\n".join(f"## {title}\n\n{text}" for title, _, text in parser.sections) + "\n")
            for title, anchor, text in parser.sections:
                units.append((title, str(original.resolve()) + ("#" + anchor if anchor else ""), text))
            row["conversion"] = {"tool": "Python HTMLParser", "markdown": str(output.relative_to(root)),
                                 "source_sha256": row["sha256"], "sections": len(units)}
        count = 0
        for locator, url, text in units:
            for block in blocks(text):
                db.execute("INSERT INTO passages VALUES (?, ?, ?, ?, ?)",
                           (row["id"], TITLES[row["id"]], locator, url, block))
                count += 1
        row["indexed_passages"] = count
        inventory.append((row["id"], "indexed", count))
    db.commit()
    assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    db.close()
    path.replace(root / "index.sqlite3")
    manifest["index"] = {"format": "SQLite FTS5", "path": "index.sqlite3",
                         "built_at": datetime.now(timezone.utc).isoformat()}
    save_manifest(root, manifest)
    lines = ["# Local virtualization manual index", "", "See [manifest](manifest.json) for provenance and checksums.", "",
             "| Source | Status | Passages |", "| --- | --- | --- |"]
    for source, status, count in inventory:
        lines.append(f"| {source} | {status} | {count} |")
    lines.extend(["", "Search with `python3 scripts/dev/index_virtualization_manuals.py search 'VMX'` from the repository root.",
                  "", "PDF page numbers are physical, one-based PDF pages, not printed chapter page labels.",
                  "Native Docling text has no reconstructed tables/diagrams: verify architectural details in the original PDF.", ""])
    (root / "INDEX.md").write_text("\n".join(lines))
    print("Indexed:", inventory)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=REPO / "reference/virtualization")
    commands = parser.add_subparsers(dest="command", required=True)
    imp = commands.add_parser("import-pdf")
    imp.add_argument("source", choices=["intel-sdm", "amd-apm-vol2"])
    imp.add_argument("path", type=Path)
    conv = commands.add_parser("convert")
    conv.add_argument("source", nargs="?", choices=["intel-sdm", "amd-apm-vol2"])
    commands.add_parser("index")
    search = commands.add_parser("search")
    search.add_argument("query", help="SQLite FTS5 query; quote hyphenated phrases")
    search.add_argument("--source", choices=TITLES)
    search.add_argument("--limit", type=int, default=8)
    args = parser.parse_args()
    root = args.root.resolve()
    if args.command == "search":
        if not 1 <= args.limit <= 100:
            parser.error("--limit must be between 1 and 100")
        with sqlite3.connect((root / "index.sqlite3").as_uri() + "?mode=ro", uri=True) as db:
            sql = "SELECT source, locator, url, snippet(passages,4,'[',']',' … ',40) FROM passages WHERE passages MATCH ?"
            params = [args.query]
            if args.source:
                sql += " AND source=?"
                params.append(args.source)
            sql += " ORDER BY rank LIMIT ?"
            params.append(args.limit)
            rows = db.execute(sql, params).fetchall()
        for source, locator, url, snippet in rows:
            print(f"{source} | {locator}\n  {url}\n  {snippet}\n")
        if not rows:
            print("No matches in the currently indexed sources.")
        return
    manifest = json.loads((root / "manifest.json").read_text())
    if args.command == "import-pdf":
        import_pdf(root, manifest, args.source, args.path)
    elif args.command == "convert":
        convert(root, manifest, args.source)
    else:
        build_index(root, manifest)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, sqlite3.Error, subprocess.CalledProcessError) as exc:
        sys.exit(str(exc))
