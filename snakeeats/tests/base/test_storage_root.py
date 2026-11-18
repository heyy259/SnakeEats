"""Wave-0 G3：持久化默认根绝对化 + ``SNAKEEATS_STORAGE`` env 档契约测试。"""

from __future__ import annotations

import os
import sys

import pytest

from snakeeats import persistence


@pytest.fixture
def fake_home(monkeypatch, tmp_path):
    home = tmp_path / "home"
    monkeypatch.setattr(persistence, "_home_dir", lambda: home)
    return home


def test_default_storage_root_is_absolute_platform_appropriate():
    root = persistence.default_storage_root()
    assert root.is_absolute()
    assert root.name == "snakeeats"


def test_default_storage_root_honours_platform_env_without_io(
    monkeypatch, tmp_path, fake_home
):
    """平台 resolver：Windows 读 LOCALAPPDATA / POSIX 读 XDG_DATA_HOME；只解析不建目录。"""
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "localappdata"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdgdata"))
    root = persistence.default_storage_root()
    assert root.is_absolute()
    if sys.platform == "darwin":
        assert root == fake_home / "Library" / "Application Support" / "snakeeats"
    elif os.name == "nt":
        assert root == tmp_path / "localappdata" / "snakeeats"
    else:
        assert root == tmp_path / "xdgdata" / "snakeeats"
    assert not root.exists()


def test_env_tier_beats_default_and_creates_absolute_layout(monkeypatch, tmp_path):
    custom = tmp_path / "custom-store"
    monkeypatch.setenv("SNAKEEATS_STORAGE", str(custom))
    layout = persistence.ensure_persistence_dirs(None)
    assert layout.root == custom
    assert layout.root.is_absolute()
    for directory in (layout.registry, layout.snapshots, layout.journal,
                      layout.control):
        assert directory.is_dir()


def test_explicit_root_beats_env(monkeypatch, tmp_path):
    monkeypatch.setenv("SNAKEEATS_STORAGE", str(tmp_path / "env-store"))
    explicit = tmp_path / "explicit-store"
    layout = persistence.ensure_persistence_dirs(explicit)
    assert layout.root == explicit
    assert not (tmp_path / "env-store").exists()


def test_blank_env_falls_back_to_platform_default(monkeypatch, tmp_path, fake_home):
    monkeypatch.setenv("SNAKEEATS_STORAGE", "   ")
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "localappdata"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdgdata"))
    layout = persistence.ensure_persistence_dirs(None)
    assert layout.root.is_absolute()
    if sys.platform == "darwin":
        assert layout.root == fake_home / "Library" / "Application Support" / "snakeeats"
    elif os.name == "nt":
        assert layout.root == tmp_path / "localappdata" / "snakeeats"
    else:
        assert layout.root == tmp_path / "xdgdata" / "snakeeats"
    assert layout.registry_file.parent.is_dir()


def test_resolve_port_with_env_storage_does_not_touch_cwd(monkeypatch, tmp_path):
    workdir = tmp_path / "cwd"
    workdir.mkdir()
    store = tmp_path / "store"
    monkeypatch.chdir(workdir)
    monkeypatch.setenv("SNAKEEATS_STORAGE", str(store))
    monkeypatch.delenv("SNAKEEATS_PORT", raising=False)
    assert persistence.resolve_port("nonexistent-xyz", environ={}) == 9876
    assert list(workdir.iterdir()) == []
    assert not (workdir / "tmp").exists()
    assert (store / "registry").is_dir()


def test_no_module_level_mkdir_on_import(monkeypatch, tmp_path, fake_home):
    """导入/解析期零 IO：仅 default_storage_root() 不创建目录（验收不变量）。"""
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "la"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    root = persistence.default_storage_root()
    assert not root.exists()
