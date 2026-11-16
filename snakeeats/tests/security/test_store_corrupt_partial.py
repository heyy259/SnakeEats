"""MP-2：安全状态"部分损坏"（合法 JSON、条目非 dict）必须置 corrupt 并按受限优先。

登记：MP-2（缺陷登记表与改造方案已随改造收尾移除，ID 保留供追溯）。
修复前：条目级损坏被静默丢弃且 corrupt=False（fail-open）。
"""

from __future__ import annotations

import json

import pytest
from .conftest import ROOT_TOKEN, call_domain

from snakeeats.errors import SnakeEatsError
from snakeeats.security.api import boot_sequence
from snakeeats.security import store as sstore


def test_partial_lockdown_marks_corrupt(tmp_path):
    """disabled_scopes 条目值非 dict（顶层仍合法 JSON）→ corrupt + root 子树 disabled。"""
    root_dir = tmp_path / "p"
    boot_sequence(ROOT_TOKEN, control_dir=root_dir)
    path = sstore.lockdown_path(root_dir)
    path.write_text(
        json.dumps({"version": 1, "disabled_scopes": {"root": "oops"}}),
        encoding="utf-8",
    )
    _mapping, corrupt = sstore.load_lockdown(root_dir)
    assert corrupt is True  # 修复前 False（静默丢弃）

    b2 = boot_sequence(ROOT_TOKEN, control_dir=root_dir)
    root2 = b2.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    assert root2 is not None
    with pytest.raises(SnakeEatsError):
        b2.verifier.mint("normal", 60, "late", caller=root2)
    # 操作员 release 后恢复
    call_domain(b2, "control.release", {"subtree": "root", "auth_proof": ROOT_TOKEN},
                principal=root2)
    tok, _ = b2.verifier.mint("normal", 60, "fresh", caller=root2)
    assert b2.verifier.verify(tok) is not None


def test_partial_classify_marks_corrupt(tmp_path):
    """streams 某条目值非 dict → corrupt + 全流受限（受限优先）。"""
    root_dir = tmp_path / "p"
    boot_sequence(ROOT_TOKEN, control_dir=root_dir)
    path = sstore.classify_streams_path(root_dir)
    path.write_text(
        json.dumps(
            {"version": 1, "streams": {"s1": "oops", "s2": {"state": "blocked"}}}
        ),
        encoding="utf-8",
    )
    _mapping, corrupt = sstore.load_classify_streams(root_dir)
    assert corrupt is True  # 修复前 False（静默丢弃 s1）

    b2 = boot_sequence(ROOT_TOKEN, control_dir=root_dir)
    assert b2.classification.corrupt_restricted is True
    assert b2.classification.is_scan_allowed("any-stream") is False


def test_partial_corruption_absent_keeps_clean(tmp_path):
    """回归：条目全为 dict 时不置 corrupt。"""
    root_dir = tmp_path / "p"
    boot_sequence(ROOT_TOKEN, control_dir=root_dir)
    path = sstore.classify_streams_path(root_dir)
    path.write_text(
        json.dumps({"version": 1, "streams": {"s2": {"state": "blocked"}}}),
        encoding="utf-8",
    )
    _mapping, corrupt = sstore.load_classify_streams(root_dir)
    assert corrupt is False
