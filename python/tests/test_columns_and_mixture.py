import pytest

from tset.columns import MetadataColumns
from tset.mixture import Subset, WeightedSampler


def make_cols():
    cols = MetadataColumns()
    rows = [
        {"lang": "en", "qs": 0.9, "url": "https://nyt.com/a"},
        {"lang": "en", "qs": 0.5, "url": "https://example.com/b"},
        {"lang": "la", "qs": 0.3, "url": "https://nyt.com/c"},
        {"lang": "en", "qs": 0.8, "url": "https://example.com/d"},
        {"lang": "fr", "qs": 0.7, "url": "https://lemonde.fr/e"},
    ]
    for r in rows:
        cols.add_row(r)
    return cols


def test_filter_basic():
    cols = make_cols()
    assert cols.filter_sql_like("lang = 'en'") == [0, 1, 3]
    assert cols.filter_sql_like("qs >= 0.7") == [0, 3, 4]
    assert cols.filter_sql_like("lang IN ('la', 'fr')") == [2, 4]


def test_filter_like():
    cols = make_cols()
    assert cols.filter_sql_like("url LIKE '%nyt.com%'") == [0, 2]


def test_filter_compound():
    cols = make_cols()
    assert cols.filter_sql_like("lang = 'en' AND qs >= 0.8") == [0, 3]
    assert cols.filter_sql_like("lang = 'la' OR qs > 0.85") == [0, 2]


def test_filter_handles_missing_column():
    cols = make_cols()
    assert cols.filter_sql_like("missing_col = 'x'") == []


def test_chunk_stats():
    cols = make_cols()
    stats = cols.compute_stats(chunk_size=2)
    assert stats["qs"][0].max == 0.9
    assert stats["qs"][1].min == 0.3


def test_round_trip_via_dict():
    cols = make_cols()
    d = cols.to_dict()
    rebuilt = MetadataColumns.from_dict(d)
    assert rebuilt.row_count == cols.row_count
    assert rebuilt.filter_sql_like("lang = 'en'") == [0, 1, 3]


def test_weighted_sampler_deterministic():
    cols = make_cols()
    subsets = [
        Subset("english", "lang = 'en'", 0.7),
        Subset("other", "lang != 'en'", 0.3),
    ]
    s1 = WeightedSampler(subsets, cols, seed=42)
    s2 = WeightedSampler(subsets, cols, seed=42)
    assert s1.sample(50) == s2.sample(50)


def test_weighted_sampler_respects_predicates():
    cols = make_cols()
    subsets = [
        Subset("english", "lang = 'en'", 1.0),
    ]
    sampler = WeightedSampler(subsets, cols)
    out = sampler.sample(100)
    english_idx = {0, 1, 3}
    assert all(i in english_idx for i in out)
