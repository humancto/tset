"""Smoke test for the HF BPE tokenizer recipe.

Skipped cleanly when ``tokenizers`` isn't installed. When it is, runs
the recipe end-to-end as a subprocess so any regression in the public
API path it exercises (Writer, Reader, HfTokenizer, verify_tokenizer_view)
shows up in CI rather than at recipe time.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


_REPO = Path(__file__).resolve().parents[2]


def test_hf_bpe_recipe_runs_end_to_end():
    pytest.importorskip("tokenizers")
    env = os.environ.copy()
    env["TSET_PREFER_RUST"] = "0"
    result = subprocess.run(
        [sys.executable, "-m", "examples.recipes.hf_tokenizer_bpe"],
        cwd=_REPO,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    assert result.returncode == 0, (
        f"recipe failed:\n--- stdout ---\n{result.stdout}\n"
        f"--- stderr ---\n{result.stderr}"
    )
    out = result.stdout
    # Each verified milestone the recipe prints
    assert "wrapped tokenizer" in out
    assert "wrote shard" in out
    assert "documents tokenize identically on read" in out
    assert "re-tokenization is byte-identical" in out
    assert "tokenizer JSON round-trips" in out


def test_reader_does_not_mask_malformed_manifest_keyerror(tmp_path):
    """Codex P1 on PR #13 regression check.

    The narrowed ``except KeyError`` only suppresses the
    registry-lookup case; if a built-in registered tokenizer's
    manifest is malformed (missing ``test_vector``, etc.) the
    KeyError MUST propagate so the integrity-check at open time
    still fires. Build a shard with the byte-level tokenizer, then
    surgically remove ``test_vector`` from the manifest and rewrite
    the manifest hash. Reader open MUST reject — silent acceptance
    would defeat the open-time integrity invariants.
    """
    sys.path.insert(0, str(_REPO / "python"))
    import json

    from tset.constants import HEADER_SIZE
    from tset.hashing import hash_bytes
    from tset.tokenizers import ByteLevelTokenizer
    from tset.writer import Writer

    out = tmp_path / "broken.tset"
    with Writer(str(out)) as w:
        w.add_document(b"alpha")
        w.add_tokenizer_view(ByteLevelTokenizer())

    raw = out.read_bytes()
    from tset.header import Header

    header = Header.decode(raw[:HEADER_SIZE])
    manifest_bytes = raw[
        header.manifest_offset : header.manifest_offset + header.manifest_size
    ]
    manifest = json.loads(manifest_bytes)
    # Drop the field verify_tokenizer_view needs.
    del manifest["tokenization_views"]["byte-level-v1"]["test_vector"]
    new_manifest = json.dumps(
        manifest, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    # Pad back to original length so header offsets stay valid.
    new_manifest = new_manifest + b" " * (header.manifest_size - len(new_manifest))
    assert len(new_manifest) == header.manifest_size

    patched = bytearray(raw)
    patched[
        header.manifest_offset : header.manifest_offset + header.manifest_size
    ] = new_manifest
    new_hash = hash_bytes(bytes(new_manifest))
    patched[64:96] = new_hash
    patched[-40 + 8 : -40 + 36] = new_hash[:28]
    out.write_bytes(bytes(patched))

    from tset.reader import Reader

    # Reader MUST reject — KeyError or related integrity failure,
    # but NOT silently accept the corrupted view.
    with pytest.raises(Exception):
        Reader(str(out))
