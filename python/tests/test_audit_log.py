from tset.audit_log import AuditLog


def test_append_and_verify():
    log = AuditLog()
    log.append("ingestion", {"doc_hash": "00", "size": 1})
    log.append("ingestion", {"doc_hash": "11", "size": 2})
    log.append("version_snapshot", {"snapshot_id": "abc"})
    assert log.verify()
    assert log.log_root != ""


def test_round_trip_via_dict():
    log = AuditLog()
    log.append("ingestion", {"doc_hash": "00"})
    log.append("exclusion", {"doc_hash": "00"})
    d = log.to_dict()
    rebuilt = AuditLog.from_dict(d)
    assert rebuilt.verify()
    assert rebuilt.log_root == log.log_root


def test_tampered_payload_fails():
    log = AuditLog()
    log.append("ingestion", {"doc_hash": "00"})
    log.entries[0].payload["doc_hash"] = "ff"
    assert not log.verify()


def test_chained_root_changes_with_each_append():
    log = AuditLog()
    log.append("a", {"i": 0})
    r1 = log.log_root
    log.append("b", {"i": 1})
    r2 = log.log_root
    assert r1 != r2
