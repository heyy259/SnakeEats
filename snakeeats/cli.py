"""snake CLI 入口（init / serve / down / status / recover）。

- ``snake init``（S1-2）：生成强随机 root token 并写入
  ``<storage>/.snakeeats.token``（单行 UTF-8），打印文件路径、token、后续命令
  与安全提示；已存在且无 ``--force`` 时拒绝覆盖并非零退出。让
  ``pip install`` → ``snake init`` → ``snake serve`` → ``snake status`` 无需
  用户手搓 token。
- ``snake serve``：单端口统一 WS 服务（S1 W1：真实域经
  ``snakeeats.assembly.assemble_serve`` 装配——stream/tools/security/
  subscriptions/config 六域；端口默认走 persistence.resolve_port 发现链
  （SNAKEEATS_PORT → .registry.json → 9876），显式 ``--port 0`` 时随机端口并
  打印实际值；启动后 register_port(system_id, bound_port)，finally 幂等摘除。
  transport.start 后把本实例连接坐标（host/port/system_id，通配 host 归一为
  127.0.0.1）快照注入 ToolsDomain，扫描工具子进程据此精准回连本实例（多实例
  各自 --storage + --port 0 即自洽；详见 serve 子命令 epilog 的多实例说明）。
- ``snake serve --manifest <path>``：冷启动双入口之一，与运行期 ``config.load``
  RPC **共享同一管线**（assembly.manifest_loader，同一 executor/is_stream_busy/
  existing_names 注入，§2.10）。
- ``snake down``：RPC-first——经 SDK 发 ``system.shutdown``（root 凭据）优雅停服
  且有界等端口释放；``--force`` 回退 = 读兄弟 pidfile 强杀（Windows
  ``os.kill(SIGTERM)`` = TerminateProcess）。
- ``snake status``：读 .registry.json + 尝试 TCP 连通；提供 root token 时经
  ``system.info`` 增报 ``ready`` / ``draining``（否则保持纯 TCP 探测输出）。
- ``snake recover --stream <s> [--to N]``（D6 descope，人用）：与 RPC 同一
  入口——经 SDK RealSystem 走 ``control.list_checkpoints``（无 --to 时列出该流
  可恢复窗口/留存快照）或 ``control.recover_stream``（有 --to 时恢复流到窗内
  版本）。认证 = admin root 凭据（--token 或 env SNAKEEATS_ADMIN_TOKEN）；
  目标端口 = .registry.json 发现（可显式 --port 覆盖）。

serve 认证：admin root token（``--token`` > env ``SNAKEEATS_ADMIN_TOKEN`` >
``<storage>/.snakeeats.token``，即 ``snake init`` 落盘值）；
无 token → 拒绝引导（no-auth serve 模式随 S1 M8 消灭）。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import secrets
import signal
import socket
import sys
import time
from pathlib import Path
from typing import Any, Callable

from . import __version__, env, logging_setup, persistence
from .assembly import Assembly, assemble_serve

#: serve 默认只绑本机回环（G8）：对外暴露须显式 --host 0.0.0.0。
DEFAULT_SERVE_HOST = "127.0.0.1"

logger = logging.getLogger(__name__)

#: 可接管的停机信号名（回环：Windows ProactorEventLoop 不支持
#: ``loop.add_signal_handler``，故用 ``signal.signal`` + ``loop.call_soon_threadsafe``；
#: SIGINT 交给 ``asyncio.run`` 自身的取消管理，**不覆盖**）。
_STOP_SIGNAL_NAMES = ("SIGTERM", "SIGHUP", "SIGBREAK")


class ServeContext:
    """一次 serve 运行上下文（薄壳：委托 :class:`Assembly`）。"""

    def __init__(self, assembly: Assembly) -> None:
        self.assembly = assembly

    # ── 兼容访问面（旧消费：system_id/layout）
    @property
    def system_id(self) -> str:
        return self.assembly.system_id

    @property
    def layout(self) -> Any:
        return self.assembly.layout

    @property
    def bound_port(self) -> int | None:
        return self.assembly.bound_port

    async def stop(self, *, delete_snapshots: bool = False) -> None:
        """停服（委托 assembly：先收工具/扫描、停 transport、停 streams、
        摘端口注册；幂等）。*delete_snapshots*：W1 默认 False = 保留最近快照
        （干净停机可恢复）；显式清理/测试传 True。"""
        await self.assembly.stop(delete_snapshots=delete_snapshots)


async def start_serve(
    port: int,
    token: str | None = None,
    *,
    host: str = DEFAULT_SERVE_HOST,
    system_id: str | None = None,
    storage_dir: str | None = None,
    snapshot_dir: str | None = None,
    manifest: str | None = None,
) -> ServeContext:
    """构造并启动统一 WS 服务（CLI serve 与冒烟测试共用入口）。

    - *port*：显式端口；0 = 随机端口（经 ctx.bound_port 读实际值）；
    - *token*：admin root token；None → env ``SNAKEEATS_ADMIN_TOKEN``；
      仍无 → RuntimeError（fail-closed，无 no-auth serve 形态）；
    - *manifest*：冷启动供给（可选）；移交装配层在 ready 之前幂等
      reconcile 流/工具，start_scan op 延后至 ready 后按原 mode 起扫；
    - 返回前已完成 register_port(system_id, bound_port)。
    """
    assembly = await assemble_serve(
        host=host,
        port=port,
        token=token,
        system_id=system_id,
        storage_dir=storage_dir,
        snapshot_dir=snapshot_dir,
        start_sweeper=True,
        manifest=manifest,
    )
    return ServeContext(assembly)


# ── 子命令实现 ──────────────────────────────────────────────────────────


def _init_command(args: argparse.Namespace) -> int:
    """``snake init``：生成并落盘 root token，打印路径/值/后续命令/安全提示。

    无 ``--token`` → ``secrets.token_urlsafe(32)``；写入
    ``<storage>/.snakeeats.token``（单行 UTF-8，POSIX 权限 best-effort 0o600）。
    已存在且无 ``--force`` → 打印明确信息并返回非零（绝不覆盖既有凭据）。
    """
    layout = persistence.ensure_persistence_dirs(args.storage)
    path = persistence.token_file_path(layout.root)
    if path.exists() and not args.force:
        print(
            f"error: token 文件已存在，未覆盖：{path}\n"
            f"hint: 如需用新 token 覆盖（旧 token 立即失效），请加 --force。",
            file=sys.stderr,
        )
        return 1
    token = args.token or secrets.token_urlsafe(32)
    persistence.write_token_file(layout.root, token)
    print(
        "SnakeEats 初始化完成。\n\n"
        f"  token 文件 : {path}\n"
        f"  root token : {token}\n\n"
        "下一步：\n"
        "  snake serve     # 启动服务（自动读取上面的 token 文件）\n"
        "  snake status    # 确认服务在线\n\n"
        "安全提示：这枚 token 是本服务的 root 凭据，请立即妥善保存、切勿外泄；\n"
        "本服务没有 no-auth 模式——丢失该 token 只能 `snake init --force` 重发。"
    )
    return 0


def _install_stop_handlers(
    loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event
) -> Callable[[], None]:
    """把 SIGTERM/SIGHUP/SIGBREAK 接到 *stop_event*；返回幂等 restore()。

    handler 只做一件事：``loop.call_soon_threadsafe(stop_event.set)``（事件循环
    可能停在 Proactor I/O 等待上、asyncio.Event 非线程安全）。二次信号 → 强退
    ``os._exit(1)``（停机卡死时仍可退出）。仅注册本平台可用信号；``signal.signal``
    在非主线程/受限环境抛 ValueError/OSError → 跳过（尽力而为）。
    """
    installed: list[tuple[Any, Any]] = []

    def _handler(signum: int, frame: Any) -> None:
        if stop_event.is_set():
            os._exit(1)
        loop.call_soon_threadsafe(stop_event.set)

    for name in _STOP_SIGNAL_NAMES:
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            installed.append((sig, signal.signal(sig, _handler)))
        except (ValueError, OSError):
            continue

    def _restore() -> None:
        while installed:
            sig, previous = installed.pop()
            try:
                signal.signal(sig, previous)
            except (ValueError, OSError):
                pass

    return _restore


async def _teardown(ctx: ServeContext) -> None:
    """有界停机：整个 ``ctx.stop()`` 包在 ``wait_for`` 内（W1-3 / D5）。

    超时 → CRITICAL 日志 + ``os._exit(1)``（不等待可能卡住的 ``to_thread``）；
    优雅路径正常返回，exit 0 由 ``main`` 返回。
    """
    timeout = env.get_shutdown_timeout()
    try:
        await asyncio.wait_for(ctx.stop(), timeout=timeout)
    except asyncio.TimeoutError:
        logger.critical(
            "graceful shutdown exceeded %.1fs deadline; forcing exit", timeout
        )
        os._exit(1)


async def _serve_command(args: argparse.Namespace) -> None:
    if args.port is None:
        # 端口发现链：SNAKEEATS_PORT → .registry.json → 默认 9876
        port = persistence.resolve_port(default=9876, root=args.storage)
    else:
        port = args.port

    # W1-2：boot 前创建停机事件并装 handler——boot 期收到信号，boot 完成后立即
    # 进入停机（而非走默认动作、无清理）。
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    restore_signals = _install_stop_handlers(loop, stop_event)
    try:
        ctx = await start_serve(
            port,
            args.token,
            host=args.host,
            system_id=getattr(args, "system_id", None),
            storage_dir=args.storage,
            snapshot_dir=args.snapshot_dir,
            manifest=args.manifest,
        )
    except BaseException:
        restore_signals()
        raise

    # W1-1：in-process request_stop()（及后续 RPC）与信号共用同一停机事件。
    ctx.assembly.stop_event = stop_event
    print(f"SnakeEats server listening on {args.host}:{ctx.bound_port}")
    # manifest 已移交装配层（boot 内 ready 前 reconcile + 延后扫描），
    # 此处不再有启动后应用块。
    try:
        # 长驻直至停机事件（信号 / request_stop）或 CancelledError（Ctrl+C）。
        await stop_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        # W1-2：先恢复默认信号再 teardown（避免停机窗口内二次信号/误触发）。
        restore_signals()
        await _teardown(ctx)
        print("SnakeEats server stopped")


#: down/status 的 RPC 连接超时（秒）。
_CLI_RPC_TIMEOUT_S = 10.0
#: 端口释放轮询间隔（秒）。
_PORT_RELEASE_POLL_S = 0.05


def _port_is_bound(host: str, port: int, timeout: float = 0.5) -> bool:
    """TCP 探测 *port* 是否有监听者（连接成功 = 已绑定）。"""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _wait_port_released(host: str, port: int, timeout: float) -> bool:
    """有界轮询直到 *port* 释放；返回 True = 已释放，False = 超时仍占用。"""
    deadline = time.monotonic() + max(0.0, timeout)
    while True:
        if not _port_is_bound(host, port):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(_PORT_RELEASE_POLL_S)


def _resolve_cli_token(
    args: argparse.Namespace, root: str | os.PathLike[str] | None = None
) -> str | None:
    """CLI root 凭据：``--token`` > env ``SNAKEEATS_ADMIN_TOKEN`` > token 文件。

    token 文件 = ``<root>/.snakeeats.token``（``snake init`` 落盘）。三者皆无
    → ``None``（调用方 fail-closed 报错，绝不回落空 token）。
    """
    token = getattr(args, "token", None)
    if token:
        return token
    token = env.get_admin_token()
    if token:
        return token
    return persistence.read_token_file(root)


def _down_command(args: argparse.Namespace) -> None:
    """``snake down``：RPC-first（root ``system.shutdown``）→ ``--force`` pidfile。

    - 无 root 凭据 → 明确报错 + 提示（**不**静默回退为仅摘注册项）；
    - 有凭据 → SDK 连服务发 ``system.shutdown``，有界轮询直到端口释放；
    - ``--force`` → 读兄弟 pidfile，校验端口仍占用后 ``os.kill(SIGTERM)``
      （POSIX 优雅 / Windows TerminateProcess）。
    """
    sid = getattr(args, "system_id", None) or env.get_system_id()
    layout = persistence.ensure_persistence_dirs(args.storage)
    port = persistence.discover_port(sid, root=layout.root)
    if args.force:
        _down_force(sid, layout, port)
        return
    token = _resolve_cli_token(args, layout.root)
    if not token:
        print(json.dumps({
            "error": f"system_id={sid!r} 停服需要 root 凭据（RPC system.shutdown）",
            "hint": "传 --token、设置 SNAKEEATS_ADMIN_TOKEN，或先 snake init；"
                    "RPC 不可达时可用 snake down --force（读 pidfile 强杀）。",
        }, ensure_ascii=False, indent=2))
        return
    if port is None:
        print(json.dumps({
            "error": f"system_id={sid!r} 未发现运行中端口（.registry.json 无注册项）",
            "hint": "实例可能未运行；如进程仍在且 RPC 不可达，用 snake down --force。",
        }, ensure_ascii=False, indent=2))
        return
    import snakeeats.sdk as sdk

    system = sdk.RealSystem(
        host=env.get_host(), port=port, system_id=sid, auth_token=token,
        timeout=_CLI_RPC_TIMEOUT_S,
    )
    try:
        system.connect()
        assert system.is_connected
        ack = system.request("system.shutdown", {})
    except sdk.SnakeEatsSDKError as exc:  # 服务端错误帧 / 传输失败
        print(json.dumps({
            "error": {
                "code": getattr(exc, "code", None),
                "kind": getattr(exc, "kind", None),
                "message": str(exc),
            },
            "hint": "RPC 停服失败；如进程无响应，用 snake down --force。",
        }, ensure_ascii=False, indent=2))
        return
    finally:
        try:
            system.disconnect()
        except Exception:  # noqa: BLE001 —— 停服中断连失败不影响退出
            pass
    released = _wait_port_released(
        env.get_host(), port, env.get_shutdown_timeout()
    )
    print(json.dumps({
        "system_id": sid,
        "port": port,
        "shutdown_ack": ack,
        "port_released": released,
        "message": (
            "优雅停机已受理，进程已退出、端口已释放。" if released else
            "优雅停机已受理，但端口在 deadline 内仍被占用（见服务端日志）。"
        ),
    }, ensure_ascii=False, indent=2))


def _down_force(sid: str, layout: Any, port: int | None) -> None:
    """``snake down --force``：pidfile 强杀回退。

    防误杀：先校验目标端口仍占用（陈旧 pidfile / pid 复用 → 不发送信号，
    仅清理陈旧文件）。
    """
    pid = persistence.read_pidfile(sid, root=layout.root)
    if pid is None:
        print(json.dumps({
            "error": f"system_id={sid!r} 无可用 pidfile"
                     f"（{persistence.pidfile_path(sid, root=layout.root)}）",
            "hint": "无 pidfile 无法强杀；如实例确实在跑，请先用 snake down（RPC）"
                    "或手动结束进程。",
        }, ensure_ascii=False, indent=2))
        return
    host = env.get_host()
    if port is None or not _port_is_bound(host, port):
        persistence.remove_pidfile(sid, root=layout.root)
        print(json.dumps({
            "system_id": sid, "pid": pid, "port": port, "killed": False,
            "message": "端口未占用：pidfile 视为陈旧，未发送信号"
                       "（已清理陈旧 pidfile）。",
        }, ensure_ascii=False, indent=2))
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError as exc:
        print(json.dumps({
            "error": f"向 pid={pid} 发送 SIGTERM 失败：{exc}",
            "hint": "确认进程仍在运行且当前用户有权限；Windows 下 "
                    "SIGTERM = TerminateProcess。",
        }, ensure_ascii=False, indent=2))
        return
    released = _wait_port_released(host, port, env.get_shutdown_timeout())
    persistence.unregister_port(sid, root=layout.root)
    persistence.remove_pidfile(sid, root=layout.root)
    print(json.dumps({
        "system_id": sid, "pid": pid, "port": port, "killed": True,
        "port_released": released,
        "message": (
            "已发送停止信号，端口已释放（未走 RPC 优雅停机）。" if released else
            "已发送停止信号，但端口在 deadline 内仍被占用。"
        ),
    }, ensure_ascii=False, indent=2))


def _status_command(args: argparse.Namespace) -> None:
    """``snake status``：注册表 + TCP 探测；有 token 时经 ``system.info`` 增报。

    无 token → 保持今日纯 TCP 输出（``system_id`` / ``registry_port`` /
    ``reachable``）；有 token 且可达 → 追加 ``ready`` / ``draining``。
    """
    sid = getattr(args, "system_id", None) or env.get_system_id()
    layout = persistence.ensure_persistence_dirs(args.storage)
    port = persistence.discover_port(sid, root=layout.root)
    host = env.get_host()
    reachable = port is not None and _port_is_bound(host, port, timeout=1.0)
    status: dict[str, Any] = {
        "system_id": sid, "registry_port": port, "reachable": reachable,
    }
    token = _resolve_cli_token(args, layout.root)
    if token and port is not None and reachable:
        import snakeeats.sdk as sdk

        system = sdk.RealSystem(
            host=host, port=port, system_id=sid, auth_token=token,
            timeout=_CLI_RPC_TIMEOUT_S,
        )
        try:
            system.connect()
            info = system.request("system.info", {})
            status["ready"] = info.get("ready")
            status["draining"] = info.get("draining")
        except sdk.SnakeEatsSDKError as exc:  # 认证/传输失败 → 保留 TCP 结论
            status["info_error"] = str(exc)
        finally:
            try:
                system.disconnect()
            except Exception:  # noqa: BLE001 —— 断开失败不影响退出
                pass
    print(json.dumps(status, ensure_ascii=False, indent=2))


def _recover_command(args: argparse.Namespace) -> None:
    """``snake recover --stream <s> [--to N]``（D6 descope；人用，RPC 同一入口）。

    - 无 ``--to``：调 ``control.list_checkpoints{stream}`` 打印该流可恢复窗口
      [base, head) + 留存快照——操作者据此判断窗内可回退范围；
    - 有 ``--to``：调 ``control.recover_stream{stream, to_version}`` 执行窗内
      恢复（root 门 + 审计；busy-guard / 窗下拒绝走服务端错误面）。
    - 认证 = admin root 凭据（``--token`` 或 env ``SNAKEEATS_ADMIN_TOKEN``）；
      目标端口 = ``.registry.json`` 发现（显式 ``--port`` 覆盖）。
    """
    import snakeeats.sdk as sdk

    sid = env.get_system_id()
    layout = persistence.ensure_persistence_dirs(args.storage)
    port = args.port if args.port is not None else persistence.discover_port(
        sid, root=layout.root)
    if port is None:
        print(json.dumps({
            "error": f"system_id={sid!r} 在 {layout.root} 未发现运行中端口"
                     "（.registry.json 无此实例注册）",
            "hint": "先 snake serve 启动服务（或显式 --port 指定目标端口），"
                    "再执行 recover。",
        }, ensure_ascii=False, indent=2))
        return
    token = _resolve_cli_token(args, layout.root)
    if not token:
        print(json.dumps({
            "error": "recover 需要 root/elevated 凭据",
            "hint": "传 --token 或设置 SNAKEEATS_ADMIN_TOKEN（恢复控制面 = "
                    "root/elevated 门）。",
        }, ensure_ascii=False, indent=2))
        return

    system = sdk.RealSystem(
        host="127.0.0.1", port=port, system_id=sid, auth_token=token,
        timeout=30.0,
    )
    try:
        system.connect()
        assert system.is_connected
        if args.to is None:
            # 与 RPC/SDK 薄方法同一入口（control.list_checkpoints）
            result = system.request("control.list_checkpoints",
                                    {"stream": args.stream})
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return
        # 与 RPC/SDK 薄方法同一入口（control.recover_stream；root/elevated 门
        # + 审计 + busy-guard/窗校验全部经服务端错误面返回）
        result = system.request("control.recover_stream",
                                {"stream": args.stream, "to_version": int(args.to)})
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except sdk.SnakeEatsSDKError as exc:  # 服务端错误帧 / 传输失败
        payload: dict[str, Any] = {
            "error": {
                "code": getattr(exc, "code", None),
                "kind": getattr(exc, "kind", None),
                "message": str(exc),
                "data": getattr(exc, "data", None),
            }
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    finally:
        try:
            system.disconnect()
        except Exception:  # noqa: BLE001 —— 断开失败不影响退出
            pass


# ── argparse 装配 ────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="snake",
        description="SnakeEats 统一服务 CLI（init / serve / down / status / recover）。",
    )
    parser.add_argument("--version", action="version", version=f"snakeeats {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser(
        "init",
        help="生成 root token 并写入 <storage>/.snakeeats.token（后续 serve/status 自动读取）")
    p_init.add_argument(
        "--storage", default=None,
        help=("持久化根目录（缺省平台数据目录下 snakeeats/，可经 env "
              "SNAKEEATS_STORAGE 覆盖）"))
    p_init.add_argument(
        "--token", default=None,
        help="显式 root token（缺省生成强随机值 secrets.token_urlsafe(32)）")
    p_init.add_argument(
        "--force", action="store_true",
        help="覆盖既有 token 文件（旧 token 立即失效）")

    p_serve = sub.add_parser(
        "serve",
        help="启动统一 WS JSON-RPC 服务",
        epilog=(
            "多实例用法：每个实例用独立的 --storage <目录>（同一存储根由单写者锁"
            " fail-fast），端口用 --port 0 取随机端口（实际值见启动输出/"
            ".registry.json）。子进程回连坐标由服务端在 transport.start 后显式"
            "注入，故每个实例的扫描工具精准连回本实例，与实例数量无关。"
            "限制：① 同一进程内嵌入多个 SE 实例不受支持（模块级全局不隔离）；"
            "② 工具内再起 SE 必须显式 --port，否则继承父实例真端口 → 绑定冲突。"
        ),
    )
    p_serve.add_argument(
        "--manifest", default=None,
        help="manifest 路径（冷启动双入口：与 config.load 共享同一管线）")
    p_serve.add_argument("--port", type=int, default=None,
                         help="监听端口；缺省走发现链（SNAKEEATS_PORT → registry → 9876）")
    p_serve.add_argument("--token", default=None,
                         help="admin root token（缺省读 env SNAKEEATS_ADMIN_TOKEN）")
    p_serve.add_argument(
        "--host", default=DEFAULT_SERVE_HOST,
        help=("监听 host（缺省 127.0.0.1，仅本机可达；对外暴露须显式 "
              "--host 0.0.0.0）"))
    p_serve.add_argument(
        "--storage", default=None,
        help=("持久化根目录（缺省平台数据目录下 snakeeats/，可经 env "
              "SNAKEEATS_STORAGE 覆盖）"))
    p_serve.add_argument("--snapshot-dir", default=None, help="快照目录（缺省持久化 snapshots 子区）")
    p_serve.add_argument(
        "--system-id", default=None,
        help="实例身份（缺省读 env SNAKEEATS_SYSTEM_ID，默认 main）")

    p_down = sub.add_parser(
        "down",
        help=("停服：RPC system.shutdown（root 凭据）优雅停机；--force 读 "
              "pidfile 强杀"))
    p_down.add_argument("--token", default=None,
                        help="admin root 凭据（缺省读 env SNAKEEATS_ADMIN_TOKEN）")
    p_down.add_argument("--force", action="store_true",
                        help=("RPC 不可达时回退：读 <storage>/registry/<system_id>.pid "
                              "并 os.kill(SIGTERM)（POSIX 优雅/Windows 强杀）"))
    p_down.add_argument("--storage", default=None, help="持久化根目录")
    p_down.add_argument(
        "--system-id", default=None,
        help="实例身份（缺省读 env SNAKEEATS_SYSTEM_ID，默认 main）")

    p_status = sub.add_parser(
        "status",
        help=("读注册表 + TCP 探测；提供 --token 时经 system.info 增报 "
              "ready/draining"))
    p_status.add_argument("--token", default=None,
                          help="admin root 凭据（缺省读 env SNAKEEATS_ADMIN_TOKEN）")
    p_status.add_argument("--storage", default=None, help="持久化根目录")
    p_status.add_argument(
        "--system-id", default=None,
        help="实例身份（缺省读 env SNAKEEATS_SYSTEM_ID，默认 main）")

    p_recover = sub.add_parser(
        "recover",
        help="恢复控制面（D6）：列检查点或把流恢复到窗内版本")
    p_recover.add_argument("--stream", required=True, help="目标流名")
    p_recover.add_argument("--to", type=int, default=None,
                           help="回退目标版本 to_version；省略 = 只列该流可恢复窗口")
    p_recover.add_argument("--port", type=int, default=None,
                           help="目标服务端口（缺省 .registry.json 发现）")
    p_recover.add_argument("--token", default=None,
                           help="admin root 凭据（缺省读 env SNAKEEATS_ADMIN_TOKEN）")
    p_recover.add_argument("--storage", default=None,
                           help=("持久化根目录（缺省平台数据目录下 snakeeats/，"
                                 "可经 env SNAKEEATS_STORAGE 覆盖）"))

    return parser


def main(argv: list[str] | None = None) -> int:
    logging_setup.configure_logging(env.get_log_level())
    args = build_parser().parse_args(argv)
    if getattr(args, "storage", None) is not None:
        args.storage = str(Path(args.storage).expanduser().resolve())
    if args.command == "serve":
        try:
            asyncio.run(_serve_command(args))
        except persistence.StorageRootBusyError as exc:
            payload = {
                "code": "storage-root-busy",
                "message": str(exc),
                "pid": exc.pid,
                "root": exc.root,
                "lock": exc.lock,
            }
            print("SNAKEEATS_ERROR " + json.dumps(payload), file=sys.stderr)
            raise
        except KeyboardInterrupt:
            print("snake serve interrupted", file=sys.stderr)
    elif args.command == "init":
        return _init_command(args)
    elif args.command == "down":
        _down_command(args)
    elif args.command == "recover":
        _recover_command(args)
    else:  # status
        _status_command(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
