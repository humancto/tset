# TSET Binary Layout Specification — v0.2

> **Stability**
>
> | Section | Status |
> |---|---|
> | §2 Header (4096 B, fixed offset 0) | **frozen** at v0.2 — change requires a major-version bump |
> | §3 Footer (40 B, fixed at end) | **frozen** at v0.2 |
> | §4 Document store (zstd content-addressed blocks) | **frozen** at v0.2 |
> | §5 Tokenization view (incl. per-chunk content_hash, mandatory) | **frozen** at v0.2 |
> | §6 SMT (Sparse Merkle Tree) | **design under review** — RFC §10 #14–16 cryptographer sign-off pending |
> | §7 Reader/writer obligations | **frozen** at v0.2 |
> | §8 Manifest (canonical JSON, sort_keys, separators=(",",":")) | **frozen** at v0.2; protobuf migration is a v0.3 item per RFC §10 #1 |
> | §9 Out of scope | **frozen** at v0.2 |
>
> "Frozen" means the wire format and reader semantics are stable for the
> v0.2 series. TSET uses **major-only-breaking** versioning, not strict
> SemVer:
> - **Patch** (v0.2.x): doc/test changes only, no wire format change.
> - **Minor** (v0.2 → v0.3): may add new optional fields and tokenizer
>   IDs; readers from the previous minor version MUST still open shards.
> - **Major** (v0.x → v1.0; v1 → v2): may make new fields mandatory,
>   change existing field semantics, or alter byte layout. Older readers
>   are expected to reject new-major shards via the version-byte check.

This document is the normative binary-layout spec for TSET. v0.2 is the
current revision; v0.1 shards remain readable. The high-level
design rationale lives in [`RFC.md`](RFC.md); this file fixes the bytes.

All multi-byte integers are little-endian (LE). All hashes are BLAKE3 (32 bytes
unless otherwise noted). All sizes are in bytes unless tagged with `tokens`.

## 1. File overview

A v0.1 TSET shard is a single self-contained file with this top-level layout:

```
0                                   end
+--------+---------+----------+--------+
| HEADER | BODY... | MANIFEST | FOOTER |
+--------+---------+----------+--------+
4 KB     variable    JSON      40 B
```

- **HEADER** at offset 0, fixed 4096 bytes
- **BODY** holds document blocks and tokenization-view sections, in write order
- **MANIFEST** is the JSON object describing all sections
- **FOOTER** at the last 40 bytes of the file

Readers MAY parse the header first (streaming) or seek to end and read the
footer first (random access). Both paths must agree on the manifest location
and hash.

## 2. Header (4096 bytes, offset 0)

| Range     | Field                         | Type          | Notes                              |
|-----------|-------------------------------|---------------|------------------------------------|
| `0:4`     | `magic`                       | bytes         | ASCII `"TSET"`                     |
| `4:5`     | `version_major`               | uint8         | `0` for v0.x                       |
| `5:6`     | `version_minor`               | uint8         | `1` for v0.1 shards; `2` for v0.2  |
| `6:8`     | `reserved`                    | bytes         | zeros                              |
| `8:12`    | `flags`                       | uint32 LE     | reserved, must be 0                |
| `12:16`   | `reserved`                    | bytes         | zeros                              |
| `16:24`   | `manifest_offset`             | uint64 LE     | absolute offset of manifest start  |
| `24:32`   | `manifest_size`               | uint64 LE     | length in bytes of manifest        |
| `32:64`   | `shard_merkle_root`           | bytes (32)    | BLAKE3 over sorted document hashes |
| `64:96`   | `manifest_hash`               | bytes (32)    | BLAKE3 of manifest bytes           |
| `96:4096` | `reserved`                    | bytes         | zeros                              |

The header is rewritten atomically when the manifest is updated (e.g. when a
new tokenization view is appended). Writers MUST `fsync` the file after a
header rewrite.

## 3. Footer (40 bytes, last 40 bytes of file)

| Range   | Field             | Type        | Notes                                  |
|---------|-------------------|-------------|----------------------------------------|
| `0:8`   | `manifest_size`   | uint64 LE   | duplicate of header for end-seek read  |
| `8:36`  | `manifest_hash28` | bytes (28)  | first 28 bytes of `manifest_hash`      |
| `36:40` | `magic`           | bytes       | ASCII `"TEND"`                         |

The full 32-byte hash lives in the header; the truncated 28 bytes here is for
quick end-of-file integrity check (~224 bits collision resistance).

## 4. Document blocks

Document blocks live in the body region between the header and the manifest.
Each block has the form:

```
[0:4]   block_magic   = b"DBLK"
[4:8]   num_documents = uint32 LE
[8:16]  uncompressed_size = uint64 LE
[16:24] compressed_size   = uint64 LE
[24:24+compressed_size]   = zstd payload
```

The zstd payload, once decompressed, contains `num_documents` records:

```
[0:32]    doc_hash      = BLAKE3(content)        (32 bytes)
[32:40]   content_size  = uint64 LE
[40:40+content_size]    = UTF-8 document bytes
```

Documents within a block are not internally indexed; the manifest's
`document_index` provides `(block_idx, offset_in_block, content_size)` for each
hash, enabling random access to a single document via one decompression of the
containing block.

A block targets ~8–16 MiB compressed in production usage. For tiny corpora the
v0.1 reference writer may emit a single block.

## 5. Tokenization view sections

Each view section has the form:

```
[0:4]    view_magic   = b"TVEW"
[4:36]   config_hash  = BLAKE3(canonicalized tokenizer config)  (32 bytes)
[36:44]  total_tokens = uint64 LE
[44:52]  num_chunks   = uint64 LE
[52:...] chunks back-to-back
```

Each token chunk:

```
[0:8]    uncompressed_size = uint64 LE   (= num_tokens * sizeof(token))
[8:16]   compressed_size   = uint64 LE
[16:24]  num_tokens        = uint64 LE
[24:24+compressed_size]    = zstd(token array)
```

For v0.1 token IDs are stored as **uint32 LE**. Bit-packing (17-bit) is
deferred to v0.2 (RFC §10.7). Vocabulary size is recorded in the manifest;
readers MUST reject token IDs ≥ vocab_size.

The chunk layout is recoverable from the manifest's per-view `chunks` array
which records `(byte_offset_in_view, compressed_size, num_tokens, content_hash)`
for each chunk.

**v0.2: per-chunk content hashing.** Each chunk manifest entry includes
`content_hash` = BLAKE3 of the **compressed** chunk payload (the bytes
between `[24..24+compressed_size]`). Readers MUST verify on read. v0.1
shards omit `content_hash`; v0.2 readers MUST accept v0.1 shards but
content-tampering of v0.1 chunk bodies is only detected if it disturbs
chunk header fields (the manifest hash binds those).

### 5.1 Source map

Per-view, per-document source-map records are stored in the **manifest** (not
in the body) for v0.1. Each record:

```json
{ "doc_hash": "<hex>", "token_offset": <uint64>, "token_count": <uint64> }
```

This binds a token range in the view to a source document. Records appear in
the order tokens were produced; the cumulative `token_count` equals the view's
`total_tokens`.

### 5.2 Sparse offset index

Per-view, an array recorded in the manifest providing seek points every N
tokens (default N = 65536):

```json
{ "token_offset": <uint64>, "chunk_id": <uint32>, "in_chunk_offset": <uint32> }
```

Readers use this to locate a chunk for a given global token offset in O(log K)
where K is the number of seek points.

### 5.3 Reproducibility proof

Each view manifest entry includes:

```json
{
  "test_vector": {
    "doc_hashes": ["<hex>", ...],          // documents from this shard
    "expected_token_arrays_hash": "<hex>"  // BLAKE3 over concatenated token bytes
  }
}
```

Conforming readers MUST, on opening a view, retokenize the listed test
documents with the named tokenizer and verify that the BLAKE3 of the
concatenated token byte stream equals `expected_token_arrays_hash`. A mismatch
is a fatal error and the view MUST NOT be used.

The proof scope is **drift detection only** (RFC §5.3). It does not prove
semantic correctness of the tokenizer for arbitrary inputs.

## 6. Manifest (JSON)

The manifest is a UTF-8 JSON object. Top-level shape (v0.1):

```json
{
  "version": "0.1.0",
  "shard_id": "<uuid-or-blake3-hex>",
  "created_at": "<RFC3339 UTC timestamp>",
  "writer": { "name": "tset-py", "version": "0.1.0" },

  "document_store": {
    "blocks": [
      { "offset": 4096, "compressed_size": 12345, "uncompressed_size": 23456, "num_documents": 10 }
    ],
    "document_index": {
      "<doc_hash_hex>": { "block_idx": 0, "in_block_offset": 0, "content_size": 1024 }
    }
  },

  "tokenization_views": {
    "<tokenizer_id>": {
      "view_offset": <uint64>,
      "view_size": <uint64>,
      "vocab_size": <uint32>,
      "tokenizer_config": { ... },
      "config_hash": "<hex>",
      "total_tokens": <uint64>,
      "chunks": [ { "byte_offset_in_view": 52, "compressed_size": 999, "num_tokens": 65536 } ],
      "source_map": [ { "doc_hash": "<hex>", "token_offset": 0, "token_count": 1234 } ],
      "sparse_offset_index": [ { "token_offset": 0, "chunk_id": 0, "in_chunk_offset": 0 } ],
      "test_vector": { "doc_hashes": ["<hex>"], "expected_token_arrays_hash": "<hex>" }
    }
  },

  "shard_merkle_root": "<hex>"
}
```

`shard_merkle_root` is computed as a balanced binary Merkle tree over the
sorted (ascending) list of document hashes, each leaf hashed as
`BLAKE3(0x00 || doc_hash)`, internal nodes as `BLAKE3(0x01 || left || right)`.
For an odd node count at any level, the last node is duplicated. (This is the
v0.1 placeholder for the SMT specified for v0.2; the SMT supersedes it.)

## 7. Conformance obligations (v0.1)

A conforming v0.1 reader MUST:

1. Verify `magic == "TSET"` and reject `version_major > 0`.
2. Parse the header to locate the manifest and verify
   `BLAKE3(manifest_bytes) == header.manifest_hash`.
3. Verify the footer `magic == "TEND"` and that
   `header.manifest_hash[:28] == footer.manifest_hash28`.
4. For each tokenization view it opens, run the reproducibility proof and
   refuse to serve tokens from a view whose proof fails.
5. Reject token IDs `>= vocab_size`.
6. Honor exclusions recorded in v0.2+ provenance sections when present (no-op
   for pure v0.1 files).

A conforming v0.1 writer MUST:

1. Emit a header at offset 0 and a footer at end-of-file.
2. Compute and embed `shard_merkle_root` and `manifest_hash` in the header.
3. For each tokenization view, embed a non-empty `test_vector` over at least
   one document drawn from the shard.
4. Make `add_tokenizer_view` append-only: existing views and document blocks
   MUST NOT be modified.

## 8. Versioning

The major version is bumped on incompatible binary layout changes (header
geometry, magic bytes, manifest top-level required keys). The minor version
covers additive changes (new optional manifest keys, new section types
delimited by their own magic bytes).

v0.1 readers MUST ignore unknown manifest keys and unknown body sections whose
4-byte magic they do not recognise (skipping by length is supported because
all body sections start with a magic + size header).

## 9. Out of scope for this version

The following are deferred and not part of v0.1 binary layout:

- Sparse Merkle tree and inclusion / non-inclusion proofs (v0.2)
- Append-only Merkle audit log (v0.2)
- Metadata column section with statistics + bloom filters (v0.2)
- Mixture / subset definitions in the manifest (v0.2)
- Dataset-level manifest binding multiple shards (v0.2)
- Bit-packed token IDs (v0.2)
- Encryption (v1+)
