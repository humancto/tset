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
