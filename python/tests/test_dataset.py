import os

import pytest

from tset.dataset import Dataset, DatasetWriter
from tset.hashing import hash_bytes
from tset.tokenizers import ByteLevelTokenizer


@pytest.fixture
def dataset_root(tmp_path):
    root = str(tmp_path / "ds.tset")
    docs = [b"alpha doc.", b"beta doc.", b"gamma doc.", b"delta doc."]
    dw = DatasetWriter(root)
    for i, pair in enumerate([(docs[0], docs[1]), (docs[2], docs[3])]):
        with dw.shard_writer(f"part-{i:05d}") as sw:
            for d in pair:
                sw.add_document(d)
            sw.add_tokenizer_view(ByteLevelTokenizer())
        dw.register_shard(f"part-{i:05d}")
    dw.close()
    return root


def test_dataset_load(dataset_root):
    ds = Dataset(dataset_root)
    assert len(ds.shard_paths()) == 2
    assert ds.exclusions() == set()


def test_dataset_streaming_concatenates_shards(dataset_root):
    ds = Dataset(dataset_root)
    total = sum(int(b.size) for b, _ in ds.stream_tokens("byte-level-v1", 4))
    assert total == sum(len(d) for d in [b"alpha doc.", b"beta doc.", b"gamma doc.", b"delta doc."])


def test_exclusion_overlay_drops_doc(dataset_root):
    h = hash_bytes(b"beta doc.")
    dw = DatasetWriter(dataset_root)
    for shard_path in Dataset(dataset_root).shard_paths():
        dw.register_shard(os.path.splitext(os.path.basename(shard_path))[0])
    dw.add_exclusion(h, "test")
    dw.close()
    ds = Dataset(dataset_root)
    assert h.hex() in ds.exclusions()
    streamed = bytearray()
    for batch, _ in ds.stream_tokens("byte-level-v1", 4):
        streamed.extend(batch.astype("uint8").tobytes())
    assert b"beta doc." not in streamed
    assert b"alpha doc." in streamed


def test_dataset_non_inclusion_proof(dataset_root):
    ds = Dataset(dataset_root)
    absent = hash_bytes(b"never ingested")
    proof = ds.prove_non_inclusion(absent)
    assert ds.verify_non_inclusion_proof(proof)


def test_inclusion_present_doc(dataset_root):
    ds = Dataset(dataset_root)
    shard_path, proof = ds.prove_inclusion(hash_bytes(b"alpha doc."))
    assert os.path.exists(shard_path)
    from tset.reader import Reader

    with Reader(shard_path) as r:
        assert proof.verify(r.smt_root())


def test_single_shard_as_dataset(tmp_path):
    from tset.writer import Writer

    p = str(tmp_path / "single.tset")
    with Writer(p) as w:
        w.add_document(b"only doc")
        w.add_tokenizer_view(ByteLevelTokenizer())
    ds = Dataset(p)
    assert len(ds.shard_paths()) == 1
    total = sum(int(b.size) for b, _ in ds.stream_tokens("byte-level-v1", 4))
    assert total == len(b"only doc")


# ── Issue #4: dataset_merkle_root binds the exclusion overlay ─────────


def test_root_changes_when_exclusion_added(tmp_path):
    """The headline guarantee: adding an exclusion changes the root."""
    root = tmp_path / "ds"
    with DatasetWriter(str(root)) as dw:
        with dw.shard_writer("a") as sw:
            sw.add_document(b"keep")
            sw.add_document(b"drop")
            sw.add_tokenizer_view(ByteLevelTokenizer())
        dw.register_shard("a")

    ds = Dataset(str(root))
    root_before = ds.dataset_merkle_root()

    target = next(iter(next(iter(ds.shards())).documents()))[0]
    with DatasetWriter(str(root)) as dw:
        dw.add_exclusion(target, reason="test")

    root_after = Dataset(str(root)).dataset_merkle_root()
    assert root_before != root_after


def test_root_subroots_are_domain_separated(tmp_path):
    """The composite root mixes a domain tag (0x42) so it cannot
    collide with either subroot taken alone."""
    from tset.dataset import _exclusions_subroot, _shards_subroot, _dataset_merkle_root

    root = tmp_path / "ds"
    with DatasetWriter(str(root)) as dw:
        with dw.shard_writer("a") as sw:
            sw.add_document(b"x")
            sw.add_tokenizer_view(ByteLevelTokenizer())
        dw.register_shard("a")
        dw.add_exclusion(b"\x77" * 32)

    ds = Dataset(str(root))
    composite = ds.dataset_merkle_root()
    # Composite must equal NEITHER subroot — the 0x42 prefix prevents
    # an attacker from passing off a bare subroot as the dataset root.
    entries = [
        type(_) for _ in [ds._dataset_manifest["shards"]]  # noqa: F841
    ]
    from tset.dataset import ShardEntry as _SE

    se = [_SE(**e) for e in ds._dataset_manifest["shards"]]
    assert composite != _shards_subroot(se)
    assert composite != _exclusions_subroot(ds.exclusions())


def test_legacy_v01_manifest_uses_shards_only_root(tmp_path):
    """Backward compat: a manifest claiming version='0.1.0' must verify
    with the legacy shards-only computation, even after the fix lands.
    Otherwise existing datasets in the wild would suddenly have a
    different "current" root than the one they were written with."""
    import json as _json

    from tset.dataset import (
        DATASET_MANIFEST_NAME,
        ShardEntry,
        _is_legacy_overlay,
        _shards_subroot,
    )

    root = tmp_path / "legacy"
    with DatasetWriter(str(root)) as dw:
        with dw.shard_writer("only") as sw:
            sw.add_document(b"old data")
            sw.add_tokenizer_view(ByteLevelTokenizer())
        dw.register_shard("only")
        dw.add_exclusion(b"\x99" * 32, reason="excluded since legacy time")

    # Hand-rewrite the manifest to claim "0.1.0", emulating an
    # in-the-wild dataset written by the original Python writer.
    manifest_path = root / DATASET_MANIFEST_NAME
    manifest = _json.loads(manifest_path.read_text())
    manifest["version"] = "0.1.0"
    entries = [ShardEntry(**e) for e in manifest["shards"]]
    expected_legacy_root = _shards_subroot(entries)
    manifest["dataset_merkle_root"] = expected_legacy_root.hex()
    manifest_path.write_text(_json.dumps(manifest, indent=2, sort_keys=True))

    assert _is_legacy_overlay("0.1.0")
    assert _is_legacy_overlay("0.2.0")
    assert not _is_legacy_overlay("0.3.0")

    ds = Dataset(str(root))
    # Reader picks the legacy computation on a 0.1.0 manifest, ignoring
    # the exclusion overlay even though one is on disk.
    assert ds.dataset_merkle_root() == expected_legacy_root
