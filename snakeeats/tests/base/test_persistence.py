"""persistence.py 契约测试（T1 tests/base）。"""

from __future__ import annotations

from snakeeats import persistence


# ── ① ensure_persistence_dirs 幂等 ──────────────────────────────────────


def test_ensure_persistence_dirs_idempotent(tmp_path):
    layout1 = persistence.ensure_persistence_dirs(tmp_path)
    layout2 = persistence.ensure_persistence_dirs(tmp_path)
    assert layout1.registry == layout2.registry
    for directory in (layout2.root, layout2.registry, layout2.snapshots,
                      layout2.journal, layout2.control):
        assert directory.is_dir(), f"missing persistence dir: {directory}"


# ── ② InternalStore 读写 / 重启即清 ─────────────────────────────────────


def test_internal_store_set_get_exists_list():
    store = persistence.InternalStore()
    store.set("_control_lockdown", '{"on": true}')
    assert store.get("_control_lockdown") == '{"on": true}'
    assert store.exists("_control_lockdown")
    assert "_control_lockdown" in store.list_names()
    assert store.delete("_control_lockdown") is True
    assert not store.exists("_control_lockdown")


def test_internal_store_fresh_instance_is_empty():
    store = persistence.InternalStore()
    store.set("_scan_end_s1", "42")
    fresh = persistence.InternalStore()
    # 新实例为空 → 重启即清语义（纯内存，不落盘）
    assert not fresh.exists("_scan_end_s1")
    assert fresh.list_names() == []


# ── ③ append_only 往返 / 追加不截断 ─────────────────────────────────────


def test_append_only_roundtrip_and_append_no_truncate(tmp_path):
    path = tmp_path / "journal" / "events.jsonl"
    persistence.append_only_write(path, '{"seq": 1}')
    persistence.append_only_write(path, '{"seq": 2}')
    lines = persistence.append_only_read(path)
    assert lines == ['{"seq": 1}', '{"seq": 2}']


def test_append_only_read_missing_file_returns_empty(tmp_path):
    assert persistence.append_only_read(tmp_path / "nope.jsonl") == []


# ── ④ resolve_port 链：env → registry → default ─────────────────────────


def test_resolve_port_env_wins_over_registry(tmp_path):
    persistence.register_port("inst1", 9001, root=tmp_path)
    port = persistence.resolve_port(
        "inst1", root=tmp_path, environ={"SNAKEEATS_PORT": "1234"}
    )
    assert port == 1234


def test_resolve_port_registry_file_hit(tmp_path):
    persistence.register_port("inst1", 9001, root=tmp_path)
    assert persistence.discover_port("inst1", root=tmp_path) == 9001
    port = persistence.resolve_port("inst1", root=tmp_path, environ={})
    assert port == 9001


def test_resolve_port_falls_back_to_default(tmp_path):
    assert persistence.discover_port("ghost", root=tmp_path) is None
    port = persistence.resolve_port("ghost", root=tmp_path, environ={})
    assert port == 9876


def test_register_unregister_port_roundtrip(tmp_path):
    persistence.register_port("inst2", 9002, root=tmp_path)
    assert persistence.discover_port("inst2", root=tmp_path) == 9002
    assert persistence.unregister_port("inst2", root=tmp_path) is True
    assert persistence.discover_port("inst2", root=tmp_path) is None
    assert persistence.unregister_port("inst2", root=tmp_path) is False


# ── ⑤ MP-18：形状异常防御 + 原子写 + 跨进程互斥 ─────────────────────────


def _register_worker(system_id: str, port: int, root: str) -> None:
    from snakeeats import persistence as _p

    _p.register_port(system_id, port, root=root)


def test_discover_port_non_dict_json_returns_none(tmp_path):
    path = persistence.ensure_persistence_dirs(tmp_path).registry_file
    path.write_text("[1, 2]", encoding="utf-8")
    assert persistence.discover_port("inst", root=tmp_path) is None  # 修复前 AttributeError


def test_discover_port_missing_port_key_returns_none(tmp_path):
    path = persistence.ensure_persistence_dirs(tmp_path).registry_file
    path.write_text('{"inst": {"x": 1}}', encoding="utf-8")
    assert persistence.discover_port("inst", root=tmp_path) is None  # 修复前 KeyError


def test_register_port_shape_safe_and_atomic(tmp_path):
    path = persistence.ensure_persistence_dirs(tmp_path).registry_file
    path.write_text('["not-a-dict"]', encoding="utf-8")
    persistence.register_port("inst", 9100, root=tmp_path)  # 修复前 TypeError
    assert persistence.discover_port("inst", root=tmp_path) == 9100
    assert not (path.parent / (path.name + ".tmp")).exists()  # 原子写无残留


def test_register_port_preserves_other_entries(tmp_path):
    persistence.register_port("a", 9101, root=tmp_path)
    persistence.register_port("b", 9102, root=tmp_path)
    assert persistence.discover_port("a", root=tmp_path) == 9101
    assert persistence.discover_port("b", root=tmp_path) == 9102


def test_register_port_concurrent_processes_no_lost_update(tmp_path):
    """真跨进程：两进程并发注册不同实例 → 两者都在（修复前可能丢一个）。"""
    import multiprocessing

    procs = [
        multiprocessing.Process(
            target=_register_worker, args=(f"s{i}", 9200 + i, str(tmp_path))
        )
        for i in range(2)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=60)
    assert all(p.exitcode == 0 for p in procs)
    assert persistence.discover_port("s0", root=tmp_path) == 9200
    assert persistence.discover_port("s1", root=tmp_path) == 9201


# ── ⑥ Wave-0 G3：默认根绝对化 + env 档（显式 > env > 平台默认）──────────


def test_default_root_constant_is_absolute():
    assert persistence.DEFAULT_ROOT.is_absolute()
    assert persistence.DEFAULT_ROOT.name == "snakeeats"


def test_ensure_persistence_dirs_explicit_beats_env(monkeypatch, tmp_path):
    monkeypatch.setenv("SNAKEEATS_STORAGE", str(tmp_path / "envroot"))
    layout = persistence.ensure_persistence_dirs(tmp_path / "explicit")
    assert layout.root == tmp_path / "explicit"
    assert not (tmp_path / "envroot").exists()


def test_ensure_persistence_dirs_env_beats_default(monkeypatch, tmp_path):
    envroot = tmp_path / "envroot"
    monkeypatch.setenv("SNAKEEATS_STORAGE", str(envroot))
    layout = persistence.ensure_persistence_dirs()
    assert layout.root == envroot
    assert layout.root.is_absolute()
    for directory in (layout.registry, layout.snapshots, layout.journal,
                      layout.control):
        assert directory.is_dir()
