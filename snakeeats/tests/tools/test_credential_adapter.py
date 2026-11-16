"""``ScanCredentialAdapter.mint`` caller 透传测试（R3-F2 / W2 真身）。

旧桩面新增 *caller* 关键字参数并原样转交真身 mint：真身 depth 只来自 caller
记录（= caller.depth+1，credentials.py 铸造点），适配器 *depth* 参数仍被忽略
（del depth）；不传 caller → None（root 铸造路径，depth 1，与既有行为一致）。
"""

from __future__ import annotations

from .harness import ROOT_TOKEN

from snakeeats.tools.credential_adapter import ScanCredentialAdapter


class TestScanCredentialAdapterMintCaller:
    def test_caller_depth2_yields_depth3(self, verifier):
        """经适配器 mint 时传入 depth-2 caller → 子凭据 verify depth = 3。"""
        root = verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
        assert root is not None and root.is_root
        t1, _ = verifier.mint("normal", 60, "l1-caller", caller=root)
        caller1 = verifier.verify(t1)
        assert caller1 is not None and caller1.depth == 1
        t2, _ = verifier.mint("normal", 60, "l2-caller", caller=caller1)
        caller2 = verifier.verify(t2)
        assert caller2 is not None and caller2.depth == 2

        adapter = ScanCredentialAdapter(verifier)
        out = adapter.mint("tool:scan:deep", depth=2, ceiling="normal",
                           caller=caller2)
        assert out["token"] and out["principal"] == "tool:scan:deep"
        principal = adapter.verify(out["token"])
        assert principal is not None
        # 真身 depth = caller.depth+1 = 3；适配器 depth 参数（=2）被忽略
        assert principal.depth == 3
        assert principal.principal == "tool:scan:deep"

    def test_no_caller_depth_is_1(self, verifier):
        """不传 caller（None，root 铸造路径）→ depth 1，与既有行为一致。"""
        adapter = ScanCredentialAdapter(verifier)
        out = adapter.mint("tool:scan:shallow", depth=2, ceiling="normal")
        principal = adapter.verify(out["token"])
        assert principal is not None
        assert principal.depth == 1  # caller=None → issuer depth 0 → d+1=1
