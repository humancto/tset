# Binary Layout — supplementary notes

This file complements [`../SPEC.md`](../SPEC.md). It clarifies behaviours
that are too implementation-detail to belong in the normative spec but too
important to leave to reader chance.

## Writing order

Writers MUST emit sections in the order: header, document blocks,
tokenization views, manifest, footer. Readers MUST NOT depend on this for
parsing — the manifest is the source of truth for offsets — but writers that
deviate sacrifice streaming-friendliness.

## Padding and alignment

v0.1 adds no padding between sections beyond the fixed-size header. v0.2 may
introduce 4 KB alignment for the manifest to enable mmap'd page-aligned
parsing on Linux.

## File extension and MIME

Recommended extension: `.tset`. Recommended MIME: `application/x-tset`.

## Atomicity on update

The `add_tokenizer_view` operation rewrites the header and footer and may
write a new manifest larger than the old one. The reference implementation
performs an `os.fsync()` after each rewrite. Filesystems without atomic
rename (FAT, some NFS configurations) can leave the shard inconsistent on
power loss; v0.2 will offer a temp-file-and-rename writer mode.

## Garbage in the body

Repeated `add_tokenizer_view` calls leave old manifest bytes between the
last view and the new manifest. v0.1 does not compact this; a future
`tset compact` tool will rewrite the file with all dead bytes removed.

## Endianness

All integers little-endian. There is no big-endian variant.

## Maximum sizes

- Manifest: bounded by uint64 (`manifest_size`) — practically unlimited.
- Documents: bounded by uint64 (`content_size`).
- Tokens per chunk: uint64 (`num_tokens`).
- Total tokens per view: uint64 (`total_tokens`).

A v0.1 reader that cannot fit the manifest in memory is non-conforming.
Streaming-manifest support is deferred to v0.3+.
