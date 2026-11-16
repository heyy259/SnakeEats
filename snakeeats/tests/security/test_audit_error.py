from __future__ import annotations

import pytest

from snakeeats.security import audit as audit_mod
from snakeeats.security.audit import AuditLog


def test_audit_append_error_includes_path(tmp_path, monkeypatch):
    log = AuditLog(tmp_path / "audit.jsonl")

    def _boom(*args, **kwargs):
        raise OSError(22, "Invalid argument")

    monkeypatch.setattr(audit_mod, "append_only_write", _boom)
    with pytest.raises(OSError) as ei:
        log.append("tester", "control.mint", "reason")
    text = str(ei.value)
    assert "audit append failed at" in text
    assert str(log.path) in text
