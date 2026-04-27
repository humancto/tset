import json
from typing import Any

from tset.constants import VERSION_MAJOR, VERSION_MINOR


def encode_manifest(manifest: dict) -> bytes:
    return json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")


def decode_manifest(raw: bytes) -> dict:
    return json.loads(raw.decode("utf-8"))


def empty_manifest(shard_id: str) -> dict:
    return {
        "version": f"{VERSION_MAJOR}.{VERSION_MINOR}.0",
        "shard_id": shard_id,
        "writer": {"name": "tset-py", "version": "0.1.0"},
        "document_store": {"blocks": [], "document_index": {}},
        "tokenization_views": {},
        "shard_merkle_root": "",
        "audit_log": {"entries": [], "log_root": ""},
        "smt_root": "",
        "metadata_columns": {},
        "subsets": [],
    }


def manifest_set_documents(
    manifest: dict, blocks: list[dict], index: dict[str, dict]
) -> None:
    manifest["document_store"]["blocks"] = blocks
    manifest["document_store"]["document_index"] = index


def manifest_add_view(manifest: dict, tokenizer_id: str, entry: dict) -> None:
    manifest["tokenization_views"][tokenizer_id] = entry


def manifest_views(manifest: dict) -> dict[str, dict]:
    return manifest.get("tokenization_views", {})


def manifest_get_block_infos(manifest: dict) -> list[dict]:
    return manifest["document_store"]["blocks"]


def manifest_get_doc_index(manifest: dict) -> dict[str, dict]:
    return manifest["document_store"]["document_index"]


def manifest_set_smt_root(manifest: dict, root_hex: str) -> None:
    manifest["smt_root"] = root_hex


def manifest_set_shard_merkle_root(manifest: dict, root_hex: str) -> None:
    manifest["shard_merkle_root"] = root_hex


def manifest_set_audit_log(manifest: dict, entries: list[dict], log_root_hex: str) -> None:
    manifest["audit_log"] = {"entries": entries, "log_root": log_root_hex}


def manifest_set_columns(manifest: dict, columns: dict[str, Any]) -> None:
    manifest["metadata_columns"] = columns


def manifest_set_subsets(manifest: dict, subsets: list[dict]) -> None:
    manifest["subsets"] = subsets
