"""投影出口与 _scan_end_* 零写纪律断言（§4.6 / §4.5.8）。

- 凡本域出口（rebase/read_rebased），position 一律 head 绝对偏移 int，
  绝不外漏裸 ``{anchor_version, offset}``；
- 服务端代码零写 ``_scan_end_*`` 键（grep 断言兜底）；
- 旧 env 名（两旧副本前缀族 env 词表）不出现在
  任何新代码中（§2.5.1 清除纪律）。
"""

import ast
import io
import pathlib
import tokenize

import pytest

from snakeeats.journal.api import JournalHook
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.system import StreamsSystem

_STREAMS_SRC = pathlib.Path(__file__).resolve().parents[2] / "streams"


def _code_only(src: str) -> str:
    """剥离注释与 docstring，返回代码本体文本（供 grep 兜底断言）。"""
    tree = ast.parse(src)
    doc_lines: set[int] = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if isinstance(body, list) and body and isinstance(
            body[0], ast.Expr
        ) and isinstance(
            body[0].value, ast.Constant
        ) and isinstance(body[0].value.value, str):
            for lineno in range(body[0].lineno, (body[0].end_lineno or 0) + 1):
                doc_lines.add(lineno)
    kept = [line for i, line in enumerate(src.splitlines(), 1)
            if i not in doc_lines]
    stripped = "\n".join(kept)
    # 再去注释（tokenize 保字符串安全）
    toks = [tok for tok in tokenize.generate_tokens(io.StringIO(stripped).readline)
            if tok.type != tokenize.COMMENT]
    return tokenize.untokenize(toks)


class TestProjectionExitsAreInts:
    async def test_rebase_and_read_rebased_return_plain_ints(self, tmp_path):
        hook = JournalHook(journal_dir=tmp_path / "journal")
        config = SystemConfig(snapshot_dir=str(tmp_path / "snap"))
        system = await StreamsSystem.start(
            system_id="t-proj", config=config,
            journal_hook_factory=lambda s: hook,
            # delta_fn=None = 真身（journal.api.delta）；真几何出口断言
            start_session_sweeper=False,
        )
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "hello world")

            off = await client.rebase("s", anchor_version=1, offset=4)
            assert type(off) is int  # head 绝对偏移 int

            r = await client.read_rebased("s", anchor_version=1, offset=6,
                                          length=5)
            assert type(r["abs_offset"]) is int
            assert type(r["version"]) is int
            assert type(r["text"]) is str

            # 任何出口不得外漏裸 anchored blob
            assert "anchor_version" not in r
            assert not any(isinstance(v, dict) and "anchor_version" in v
                           for v in r.values())
        finally:
            await system.shutdown()
            hook.close()


class TestScanEndZeroWrite:
    """服务端（本域）代码零写 ``_scan_end_*``（§4.5.8 硬约束）。

    grep 兜底：剥离注释与 docstring 后匹配代码本体——文档性提及不犯规，
    任何真实读写点（字符串字面量/标识符）一票否决。
    """

    def test_no_scan_end_key_in_streams_source(self):
        offenders = []
        for path in _STREAMS_SRC.rglob("*.py"):
            code = _code_only(path.read_text(encoding="utf-8"))
            if "_scan_end_" in code:
                offenders.append(str(path))
        assert offenders == [], (
            f"streams 子树出现 _scan_end_ 引用（单一写者=扫描任务方）: {offenders}"
        )


class TestOldEnvNamesAbsent:
    """旧 env 名全仓清除、不留别名（§2.5.1）。"""

    # 旧名字面量以拼接构造，避免本文件自身命中全仓 grep 兜底断言
    # （tests/base/test_env.py 扫描全部 snakeeats .py 原文）。
    @pytest.mark.parametrize("old_name", [
        "_".join(("TEXT", "STREAM", suffix))
        for suffix in ("PORT", "HOST", "AUTH_TOKEN", "SYSTEM_ID")
    ] + [
        "_".join(("TOOL", "REGISTRY", suffix))
        for suffix in ("PORT", "HOST", "RPC_TIMEOUT")
    ] + ["_".join(("SCAN", "STREAM", "NAME"))])
    def test_old_env_name_absent(self, old_name):
        offenders = []
        for path in _STREAMS_SRC.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if old_name in text:
                offenders.append(str(path))
        assert offenders == [], f"{old_name} 出现于: {offenders}"
