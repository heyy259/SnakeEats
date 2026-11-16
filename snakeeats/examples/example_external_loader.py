#!/usr/bin/env python3
"""SnakeEats 外部 loader 薄封装示例（D6/M6 边界 · P4-2，约 100 行）。

外部 loader 薄封装示例：系统侧不交付 loader 本体，本示例仅消费系统既有能力
(External thin-wrapper loader demo. D6/M6: the system ships no loader
implementation, this example only consumes existing capabilities)。

组合 bundle 目录只靠两条既有入口（§5.9 / §2.10 / P4-2）：
1. ``snakeeats.config.api.load_manifest``（只读预检）——无服务、无 root token 即可
   解析 manifest + 命名空间契约（无自动前缀=作者手写扁平全名 / attach / preview
   只读，D4 修订）；
2. 运行期 ``config.load``（经 snakeeats.sdk）——真落地，与 CLI
   ``snake serve --manifest`` 共享同一管线。
命名空间契约元素见预览打印（will_create 为扁平全名 / attach_missing）；
嵌套原语（需求4）经 ``--scan-stream`` 走同 SDK 对已加载流发起 scan.start。

用法（仓库根运行）：``...example_external_loader.py --manifest <bundle|file> --dry-run``
（只读预演）；``--apply --port <p> --token <t> [--scan-stream 流]`` 真落地——仅在
真端点可达时产生副作用，不可达则打印降级说明后安全返回。
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from snakeeats.config.api import load_manifest
import snakeeats.sdk as sdk

_EXAMPLE_BUNDLE = str(Path(__file__).resolve().parents[1] / "config" / "examples" / "bundle")

def _summarize(result: dict) -> dict:
    """打印契约 4 键结果的 loaded/skipped/errors/conflict 摘要；返回原 dict。"""
    print("-- load_manifest 摘要 --")
    for ent in result["loaded"]:
        print(f"  [loaded]  {ent.get('op')} {ent.get('entity')}")
    for ent in result["skipped"]:
        print(f"  [skipped] {ent.get('entity')} - {ent.get('reason')}")
    for err in result["errors"]:
        print(f"  [error]   kind={err.get('kind')} msg={err.get('message')}")
    preview = result["preview"]
    if preview["will_create"]:
        names = "、".join(item["name"] for item in preview["will_create"])
        print(f"  [will_create] {len(preview['will_create'])} 项：{names}")
    if preview["attach_missing"]:
        for item in preview["attach_missing"]:
            print(f"  [attach_missing] kind={item.get('kind')} msg={item.get('message')}")
    if preview["conflicts"]:
        print(f"  [conflict] {len(preview['conflicts'])} 条")
    if preview["failures"]:
        print(f"  [failure] {len(preview['failures'])} 条预检失败")
    return result


def preview_manifest(path_or_dir: str) -> dict:
    """只读预演：load_manifest(dry_run=True)，零落地、零服务依赖。

    返回契约 4 键 dict——外部 loader 无需服务即可预检 manifest 与命名空间契约
    （attach/preview 语义；对象名 = 作者手写扁平全名，D4 无自动前缀）。
    """
    print(f"[preview] 只读预检 {path_or_dir}")
    return _summarize(load_manifest(str(path_or_dir), dry_run=True))


def _degrade(path_or_dir: str) -> dict:
    """真服务不可达/无凭据 → 降级只读预演（本示例不发明任何 loader 机制）。"""
    print("[note] 未连接真服务：config.load 需运行中的统一服务与凭据；降级为只读预演。")
    return preview_manifest(path_or_dir)


def apply_manifest(path_or_dir: str, *, system: Any = None, host: str = "127.0.0.1",
                   port: int | None = None, token: str | None = None,
                   system_id: str = "main", scan_stream: str | None = None) -> dict:
    """真落地：经 snakeeats.sdk 调 config.load（与 CLI --manifest 同一管线）。

    *system* 传入已连接 RealSystem 时复用；否则按 host/port/token 自建（用后自行
    断开）。不可达 → 降级只读预演。返回契约 4 键 dict。
    """
    own = system is None
    if own:
        if not port or not token:
            return _degrade(path_or_dir)
        try:
            system = sdk.RealSystem(host=host, port=port, auth_token=token,
                                    system_id=system_id).connect()
        except Exception as exc:
            print(f"[note] config.load 真落地不可用：{exc}")
            return _degrade(path_or_dir)
    try:
        print(f"[apply] config.load {path_or_dir}")
        result = system.config_load(path=str(path_or_dir), dry_run=False)
        _summarize(result)
        if scan_stream:
            print(f"[scan.start] 目标流 {scan_stream}（需求4 嵌套原语演示）")
            try:
                print("  ->", system.scan_start(stream_name=scan_stream))
            except Exception as exc:
                print(f"  -> scan.start 降级说明：{exc}")
        return result
    finally:
        if own:
            try:
                system.disconnect()
            except Exception:
                pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SnakeEats 外部 loader 薄封装示例")
    parser.add_argument("--manifest", default=_EXAMPLE_BUNDLE)
    parser.add_argument("--dry-run", action="store_true", help="只读预演（默认）")
    parser.add_argument("--apply", action="store_true", help="真落地（需服务+凭据）")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--token", default=None)
    parser.add_argument("--system-id", default="main")
    parser.add_argument("--scan-stream", default=None, help="apply 后 scan.start 演示")
    args = parser.parse_args(argv)
    if args.apply:
        apply_manifest(args.manifest, host=args.host, port=args.port, token=args.token,
                       system_id=args.system_id, scan_stream=args.scan_stream)
    else:
        preview_manifest(args.manifest)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())