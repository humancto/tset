import json

from tset.converters import jsonl_to_tset
from tset.reader import Reader
from tset.tokenizers import ByteLevelTokenizer


def test_jsonl_round_trip(tmp_path):
    jp = tmp_path / "in.jsonl"
    rows = [
        {"text": "hello world", "lang": "en", "qs": 0.9},
        {"text": "foo bar", "lang": "en", "qs": 0.5},
        {"text": "baz qux quux", "lang": "fr", "qs": 0.3},
    ]
    with open(jp, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    out = tmp_path / "out.tset"
    res = jsonl_to_tset(
        str(jp), str(out), tokenizer=ByteLevelTokenizer(), metadata_fields=["lang", "qs"]
    )
    assert res["documents"] == 3
    with Reader(str(out)) as r:
        cols = r.metadata_columns()
        assert cols.filter_sql_like("lang = 'en'") == [0, 1]
        assert cols.filter_sql_like("qs <= 0.5") == [1, 2]
