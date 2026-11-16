"""snakeeats/tests/config 测试共享配置。

- sys.path 兜底：仓库根（改造中的副本）入 sys.path（对齐 tests/base 与
  tests/streams 的 conftest 做法）；
- 共享 fixtures：examples 目录常量、manifest 写盘助手、计数/失败注入的
  executor 工厂、busy 探测工厂。
"""

from __future__ import annotations

import json
import pathlib
import sys
from typing import Any

import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

#: examples 目录（snakeeats/config/examples）＝ parents[2]/config/examples
_EXAMPLES = pathlib.Path(__file__).resolve().parents[2] / "config" / "examples"


@pytest.fixture
def examples() -> pathlib.Path:
    """测试用 manifest/bundle 样例目录（tests 引用它的稳定落点）。"""
    return _EXAMPLES


def write_manifest(dirpath, name: str, data: dict[str, Any]) -> pathlib.Path:
    """把 manifest dict 写盘（UTF-8 + ensure_ascii=False）。"""
    dirpath = pathlib.Path(dirpath)
    dirpath.mkdir(parents=True, exist_ok=True)
    target = dirpath / name
    with open(target, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    return target


@pytest.fixture
def write_json(tmp_path):
    """写 JSON fixture 的工厂（自动落到 tmp_path 下任意子目录）。"""
    def _w(relpath: str, data) -> pathlib.Path:
        target = tmp_path / relpath
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        return target
    return _w


@pytest.fixture
def write_text(tmp_path):
    """写文本 fixture 的工厂（source 数据 / 工具脚本用）。"""
    def _w(relpath: str, text: str) -> pathlib.Path:
        target = tmp_path / relpath
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "w", encoding="utf-8") as fh:
            fh.write(text)
        return target
    return _w


@pytest.fixture
def make_executor():
    """executor_fn 工厂：记录调用，可按名注入失败/抛异常。

    返回可调用对象（暴露 ``.calls`` / ``.fail_names`` 便于断言）。
    """
    class _ExecStub:
        def __init__(self, fail_names=(), raise_names=()):
            self.calls = []
            self.fail_names = list(fail_names)
            self.raise_names = list(raise_names)

        def __call__(self, op):
            self.calls.append(op)
            if op.name in self.raise_names:
                raise RuntimeError(f"injected raise for {op.name}")
            if op.name in self.fail_names:
                return {
                    "success": False,
                    "error": {
                        "kind": "execution-failed",
                        "message": f"intentional failure for {op.name}",
                        "hint": "本失败为测试注入。",
                    },
                }
            return {"success": True}

    def _make(*, fail_names=(), raise_names=()):
        return _ExecStub(fail_names=fail_names, raise_names=raise_names)
    return _make


@pytest.fixture
def busy_probe_factory():
    """is_stream_busy 工厂：按名返回真/假，并记录探测调用。"""
    class _ProbeStub:
        def __init__(self, busy_names=(), others=False):
            self.probed = []
            self.busy_names = set(busy_names)
            self.others = others

        def __call__(self, name: str) -> bool:
            self.probed.append(name)
            if name in self.busy_names:
                return True
            return self.others

    def _make(busy_names=(), others=False):
        return _ProbeStub(busy_names=busy_names, others=others)
    return _make
