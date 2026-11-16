"""tools/scan/task 域 — C6 Domain 协议实现（T3 交付 + W2 真身接线）。

方法面（MUST DO #7 + §2.3.1/§2.3.3/§2.3.4 + S1 W2）：

- ``tool.*`` 23 键全量（旧 tool-registry 36 键去 system.info 后的工具面）；
- ``scan.*`` 提级一等域（含 ``scan.cancel`` / ``scan.rescan``）；
- ``system.info``：**S1 已从本域移除**——统一 system.info 由 StreamDomain 承载
  （``system.`` 前缀归属单一化；Router 前缀冲突即 ValueError，T3 HANDOFF §7），
  工具/扫描计数经 StreamDomain.info_provider 注入并入同一上报。

W2 接线形态（S1 MUST DO #4）：
- 扫描启动门：``scan.start`` / ``scan.run`` 在启动前咨询
  ``classification_store.is_scan_allowed(stream_name, {"tags": <流元数据 tags>})``；
  受限（blocked / classified 谓词不匹配）→ 抛 -33008 kind=classify-denied。
- 阻塞型扫描调用（run/start + 嵌套链记录）经 ``asyncio.to_thread`` 落到工作线程，
  再经 SyncStreamClient 桥回宿主事件循环 —— 域处理器本身在事件循环线程上，
  不得在同一线程同步阻塞桥调用（会自锁）。

统一返回面（M2/§2.3.4/§5.6）：业务层返回 ``{success,...}`` 形态；失败时 error 携带
可分辨 kind；对 RPC 帧层抛 SnakeEatsError（经 UnifiedRouter.error_from_exception
转 M2 错误帧）；对结果字典面由 ToolSDK 收敛。
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional

from snakeeats import errors as _errors
from snakeeats.security.api import (
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    Resource,
    authorize,
    authorize_with_cards,
)

from .error_dict import success_false
from .exceptions import ScannerNotRunningError
from .workspace_registration import (
    contained_realpath,
    workspace_root,
    workspace_tool_name_error,
)
from snakeeats.kernel.positions import encode_anchored

if TYPE_CHECKING:
    from snakeeats.security.api import Principal
    from .tool_definition import ToolDefinition

from .sdk import ToolSDK, TextStreamToolIntegration
from ._scan_authority import ScanAuthority, resolve_scan_authority
from ._scan_control_authority import authorize_scan_control
from ._scan_set_authority import resolve_authority_switch
from .session_adapter import make_session_lookup
from .stream_sdk_wrapper import StreamSDKWrapper
from .task_manager import derive_scan_depth

logger = logging.getLogger(__name__)

__all__ = ["ToolsDomain"]


class ToolsDomain:
    """工具注册/执行/扫描统一域（C6 Domain）。

    prefixes = (``tool.``, ``scan.``, ``task.``)。``system.info`` 已由 S1
    移除（统一 system.info 归 StreamDomain；工具/扫描计数经
    StreamDomain.info_provider 注入，见类模块 docstring）。
    """

    prefixes: tuple[str, ...] = ("tool.", "scan.", "task.")

    def __init__(
        self,
        sdk: Optional[ToolSDK] = None,
        *,
        client: Any = None,
        credential_verifier: Any = None,
        classification_store: Any = None,
        audit_cb: Optional[Callable[[str, str, str, Optional[float]], None]] = None,
        max_concurrent_scans: Optional[int] = None,
        max_scan_depth: Optional[int] = None,
        cursor_flush: Optional[Callable[[str], Any]] = None,
        storage_root: str | Path | None = None,
    ):
        """构造域。

        Args:
            sdk: 共享 ToolSDK（registry + task_manager）。缺省自建。
            client: 进程内同步流桥（W2：SyncStreamClient；传给 scan 启动面复用）。
            credential_verifier: C4 凭据验证器（真身；装配注入 task_manager +
                M6 可见性谓词卡查询源，self._credential_verifier）。
            classification_store: C4 分类存储（真身；scan 门禁 + classify 转发位）。
            audit_cb: audit(who, what, reason, ts=None) 只追加回调（C4 audit sink）。
            max_concurrent_scans: G13 全局并发上限（W3-4；装配从 env 解析后注入；
                缺省 None = ToolSDK/ScanTaskManager 默认 8）。仅在自建 sdk 时生效。
            max_scan_depth: G13 全局深度上限（W3-4；缺省 None = 默认 3）。
            cursor_flush: 可选 ``(stream) -> bool`` 游标立即落盘回调（P1：
                assembly 注入 ``CoalescingCursorWriter.flush_stream``；
                scan.pause 成功后 best-effort 刷新暂停位置）。None = 不刷新。
            storage_root: 服务端持久化根（P4.1：workspace 工具根由
                ``<storage_root>/agent_tools/<principal.namespace>`` 派生；
                绝不读取请求参数/客户端路径作为根）。None = 尝试 sdk 注入值。
        """
        self.sdk = sdk if sdk is not None else ToolSDK(
            max_concurrent_scans=max_concurrent_scans,
            max_scan_depth=max_scan_depth,
        )
        if client is not None and self.sdk.task_manager is not None:
            # 进程内流 client 复用：把默认 StreamSDKWrapper 换成注入 client 的包装器
            pass  # 复用 client 在 run_scan/start_scan 的 sdk= 参数位传入
        self._credential_verifier = credential_verifier  # M6 可见性：持卡查询源
        if credential_verifier is not None:
            self.sdk.task_manager.credential_verifier = credential_verifier
        # M4 归因解析器注入链：client（SyncStreamClient）若带可调用
        # resolve_writers → 推入 task_manager（ScanTask 构造时再拷贝到
        # controller._resolve_writers，M5 执行闸消费）。零装配改动——
        # assembly 已把共享 sync 桥作为 client 注入；client=None 或旧形态
        # client 缺该方法时跳过（独立单测/纯 sdk 构造不受影响）。
        resolve_writers = getattr(client, "resolve_writers", None)
        if resolve_writers is not None and self.sdk.task_manager is not None:
            self.sdk.task_manager.resolve_writers = resolve_writers
        self.classification_store = classification_store
        self._audit_cb = audit_cb
        self._integration = TextStreamToolIntegration(self.sdk)
        self._client = client
        self._cursor_flush = cursor_flush
        injected_root = storage_root
        if injected_root is None:
            injected_root = getattr(self.sdk, "storage_root", None)
        self._storage_root = (
            Path(injected_root) if injected_root is not None else None
        )

    # ── C6 协议 ──────────────────────────────────────────────────────

    def method_map(self) -> dict[str, str]:
        """完整方法名 → 说明（统一 Router 登记表用；新旧名逐行标注）。"""
        m = {
            # ── tool.* 35 键（旧名 = 现名，无改名）──────────────────
            "tool.list": "列出工具（可 tags 过滤）",
            "tool.get": "获取单工具完整定义",
            "tool.register": "注册工具",
            "tool.unregister": "注销工具（目标不存在归 -33001 not-found【M7】）",
            "tool.search": "按名称/描述/简要描述搜索工具",
            "tool.get_manual": "获取工具手册内容",
            "tool.list_for_llm": "LLM 视图工具列表（MCP tools/list 兼容）",
            "tool.get_for_llm": "单个工具 LLM 视图",
            "tool.summarize_for_prompt": "工具文本摘要（prompt 注入）",
            "tool.format_call": "格式化 @{name|{json}} 标记",
            "tool.parse_calls": "解析标记",
            "tool.send_input": "向后台进程发送 stdin",
            "tool.list_processes": "列后台进程（可 tool/status 过滤）",
            "tool.get_process_info": "单进程详情",
            "tool.stop_process": "停止进程（单个）",
            "tool.stop_processes": "批量停止进程",
            "tool.stop_all_processes": "停止全部后台进程",
            "tool.clear": "清空工具注册表",
            "tool.classified.list": "分类内工具列表（完整）",
            "tool.classified.list_for_llm": "分类内工具 LLM 列表",
            "tool.classified.summarize_for_prompt": "分类内工具摘要",
            "tool.classified.get": "分类内单工具完整",
            "tool.classified.get_for_llm": "分类内单工具 LLM 视图",
            # ── scan.* 提级一等域（tool.scan.* 兼容别名已删除）────
            "scan.run": "同步运行扫描直到完成",
            "scan.start": "非阻塞启动扫描",
            "scan.pause": "暂停扫描",
            "scan.resume": "恢复扫描",
            "scan.stop": "停止扫描，保游标",
            "scan.move": "移动扫描标记",
            "scan.status": "扫描状态/空闲快照",
            "scan.list": "列出扫描任务（含只读投影 depth/parent_stream）",
            "scan.cancel": "撤任务+杀后台子代理+吊销票据（新键 M9）",
            "scan.rescan": "从头重扫：游标归零后重新起扫（新键，阶段6）",
            "scan.set_authority": (
                "切换扫描当前权限（仅 settled PAUSED；starter/root）"
            ),
        }
        return m

    async def handle(self, method: str, params: dict[str, Any], ctx: Any = None) -> Any:
        """分派到域后的业务入口（C6）。

        域异常在此统一转帧（§2.4.3 归码 / §5.6 判类）：
        - SnakeEatsError 直接上抛（帧层转 M2 错误帧）；
        - 已知域异常 → 按 kind 归码上抛（绝不坍缩成无 kind 的 -32603）；
        - 真正未知异常 → -32603 兜底（INTERNAL_ERROR 仅兜底，不作缺省垃圾桶）。
        """
        dispatch = self._dispatch_table()
        fn = dispatch.get(method)
        if fn is None:
            _errors.raise_error(
                _errors.METHOD_NOT_FOUND,
                _errors.KIND_METHOD_NOT_FOUND,
                entity={"method": method},
                expected="registered tool./scan./task. method",
                actual=method,
            )
        try:
            return await fn(params, ctx)
        except _errors.SnakeEatsError:
            raise
        except Exception as exc:  # 已知域异常 → kind 归码；未知 → -32603 兜底
            self._raise_domain_exception(exc, method=method)

    def _raise_domain_exception(self, exc: Exception, *, method: str) -> None:
        """域异常 → SnakeEatsError（经 errors.raise_error 五要素路径）。"""
        from .error_dict import _exception_code_kind

        code, kind = _exception_code_kind(exc)
        if code is None or kind is None:
            _errors.raise_error(
                _errors.INTERNAL_ERROR, _errors.KIND_INTERNAL,
                entity={"method": method, "exception": type(exc).__name__},
                actual=repr(exc),
            )
        entity = {"method": method}
        # 尽力带实体上下文（异常自带 name/stream_name 时）
        for attr in ("name", "stream_name", "tool_name"):
            if hasattr(exc, attr):
                entity[attr] = getattr(exc, attr)
        _errors.raise_error(code, kind, entity=entity, actual=repr(exc))

    # ── 内部：分派表 ────────────────────────────────────────────────

    def _dispatch_table(self) -> dict[str, Callable]:
        return {
            # tool CRUD / 查询
            "tool.list": self._tool_list,
            "tool.get": self._tool_get,
            "tool.register": self._tool_register,
            "tool.unregister": self._tool_unregister,
            "tool.search": self._tool_search,
            "tool.get_manual": self._tool_get_manual,
            "tool.list_for_llm": self._tool_list_for_llm,
            "tool.get_for_llm": self._tool_get_for_llm,
            "tool.summarize_for_prompt": self._tool_summarize_for_prompt,
            "tool.format_call": self._tool_format_call,
            "tool.parse_calls": self._tool_parse_calls,
            "tool.clear": self._tool_clear,
            # 进程管理
            "tool.send_input": self._tool_send_input,
            "tool.list_processes": self._tool_list_processes,
            "tool.get_process_info": self._tool_get_process_info,
            "tool.stop_process": self._tool_stop_process,
            "tool.stop_processes": self._tool_stop_processes,
            "tool.stop_all_processes": self._tool_stop_all_processes,
            # 分类内查询
            "tool.classified.list": self._tool_classified_list,
            "tool.classified.list_for_llm": self._tool_classified_list_for_llm,
            "tool.classified.summarize_for_prompt": self._tool_classified_summarize,
            "tool.classified.get": self._tool_classified_get,
            "tool.classified.get_for_llm": self._tool_classified_get_for_llm,
            # scan.* 新名
            "scan.run": self._scan_run,
            "scan.start": self._scan_start,
            "scan.pause": self._scan_pause,
            "scan.resume": self._scan_resume,
            "scan.stop": self._scan_stop,
            "scan.move": self._scan_move,
            "scan.status": self._scan_status,
            "scan.list": self._scan_list,
            "scan.cancel": self._scan_cancel,
            "scan.rescan": self._scan_rescan,
            "scan.set_authority": self._scan_set_authority,
        }

    # ── 主体/审计助手 ────────────────────────────────────────────────

    def _actor(self, params: dict[str, Any], ctx: Any = None) -> Optional[str]:
        """从 ctx（transport 注入 Principal）或 params.actor 取主体标识。

        「永不信任流内容里的 scope/权限字段」：actor 仅作注解/审计登记，
        不直接作授权判定（授权由上层凭据闸）。
        ctx 形态兼容：dict（tools 单测现状）取 principal/actor 键；object
        （WS TransportContext，S1）取 ctx.principal（C4 Principal → label）。
        """
        if ctx is not None:
            if isinstance(ctx, dict):
                principal = ctx.get("principal") or ctx.get("actor")
            else:
                principal = getattr(ctx, "principal", None)
                if principal is not None and not isinstance(principal, str):
                    label = getattr(principal, "label", None)
                    if label is None:
                        label = getattr(principal, "principal", None)
                    principal = label
                if principal is None:
                    principal = params.get("actor") or params.get("$actor")
            if principal:
                return str(principal)
        return params.get("actor") or params.get("$actor")

    def _principal_of(self, params: dict[str, Any],
                      ctx: Any = None) -> Optional["Principal"]:
        """取调用方**完整 Principal 对象**（M3 身份透传源；不做 _actor 的降级）。

        scan.start/run 把本返回值作为扫描发起者身份透传到
        sdk → task_manager → executor/controller（子凭据 mint caller 链 +
        scan.cancel 授权闸）。ctx 形态兼容同 _actor（dict 取 principal/actor
        键；object 取 ctx.principal）：

        - ctx.principal 已是完整主体对象（C4 Principal，WS 认证绑定）→ 原样返回；
        - ctx.principal 是 str label（进程内 RequestContext 现状）→ 返回 None
          （无法从 label 重建完整对象；宁可无链，不造伪对象）；
        - ctx 为 None / dict 无 principal 对象 → None（调用方未带可透传身份）。

        params 内 actor 仅为审计标签（str），永远不升级为完整主体 ——
        服务端只认 transport 打标的 ctx.principal（《永不信任流内容》）。

        注意：dict 形态 ctx 的 principal 值若为 dict 会原样返回（历史透传形态）；
        H1-B 闸（_require_rpc_starter_principal）以 isinstance(Mapping) 补偿拒绝。
        """
        if ctx is not None:
            if isinstance(ctx, dict):
                principal = ctx.get("principal") or ctx.get("actor")
            else:
                principal = getattr(ctx, "principal", None)
            if principal is not None and not isinstance(principal, str):
                return principal
        return None

    def _require_rpc_starter_principal(self, ctx: Any,
                                       starter_principal: Optional["Principal"],
                                       *, stream: str, method: str) -> None:
        """H1-B：RPC 入口必须携带完整启动主体 Principal（fail-closed）。

        ctx 在场但无完整主体——未认证 / str label / dict 形态——即拒；在任何
        线程分派、分类 I/O、游标写之前（三个 scan 处理器在 to_thread 前调用）。
        进程内/manifest 路径（ctx=None / assembly._scan_launcher 直调 sdk）
        不受本闸影响（root 名义，见 _register_starter）。
        """
        if ctx is not None and (starter_principal is None
                                or isinstance(starter_principal, Mapping)):
            _errors.raise_error(
                _errors.AUTH_REQUIRED, _errors.KIND_DENIED,
                entity={"stream_name": stream, "method": method},
                expected="WS/RPC 入口经 auth 绑定的完整 Principal",
                actual="ctx 在场但无完整启动主体（未认证 / str label / dict 形态）",
                hint="先经 auth 认证再调用；深度链以凭据记录为唯一真源。",
            )

    def _audit(self, who: str, what: str, reason: str) -> None:
        if self._audit_cb is not None:
            try:
                self._audit_cb(who, what, reason, time.time())
            except Exception:  # 审计失败不阻断业务
                pass

    # ── M6：root 主体门 + subject_executable 可见性 ─────────────────────
    #
    # 注册面（tool.register/unregister/clear）与 config.load（config 域）为
    # root 门；工具读面（tool.get/list/search/*_for_llm/summarize/
    # get_manual + classified.* 读）按「不可执行即不可见」过滤：root = 全量；
    # 持卡者 = 无 requires 工具 ∪ 单张活动卡 methods ⊇ requires 的守卫工具；
    # 无卡主体/ctx 无完整身份 = 仅无 requires 工具（fail-closed）。
    # 判定只用 ctx.principal（完整 Principal），永不信任 params.actor/$actor。

    def _principal_identity(self, principal: Any) -> str:
        """Stable service-written provenance/owner identity for a principal."""
        for attr in ("principal", "label"):
            value = getattr(principal, attr, None)
            if isinstance(value, str) and value:
                return value
        return "unknown"

    @staticmethod
    def _is_root(principal: Any) -> bool:
        return bool(getattr(principal, "is_root", False))

    def _authorize_tool_write(self, method: str, resource: Resource,
                              params: dict[str, Any], ctx: Any) -> Any:
        """Single ``security.api.authorize`` gate for the registry write face.

        Anonymous/incomplete identities stay ``-32001 kind=auth-required``;
        every policy denial stays ``-32001 kind=denied`` (reason_code carried in
        entity). Root is decided by policy, never by a local bypass.
        """
        principal = self._principal_of(params, ctx)
        if principal is None:
            self._audit("unknown", f"{method} denied", "无完整主体身份")
            _errors.raise_error(
                _errors.AUTH_REQUIRED, _errors.KIND_AUTH_REQUIRED,
                entity={"method": method},
                expected="已认证连接 principal", actual="匿名/无完整身份",
                hint=f"{method} 只接受已认证连接；先 auth。",
            )
        decision = authorize(principal, method, resource)
        if decision.allow:
            return principal
        label = getattr(principal, "label", None) or "<unknown>"
        self._audit(label, f"{method} denied", decision.reason_code)
        _errors.raise_error(
            _errors.AUTH_REQUIRED, _errors.KIND_DENIED,
            entity={
                "method": method,
                "principal": label,
                "reason_code": decision.reason_code,
                "required": decision.required,
                "actual": decision.actual,
                "remedy": decision.remedy,
            },
            expected=decision.required,
            actual=decision.actual,
            hint=decision.remedy or f"{method} 被统一授权政策拒绝。",
        )

    def _deny_workspace_provenance(self, method: str, reason: str,
                                   required: Any = None, actual: Any = None) -> None:
        _errors.raise_error(
            _errors.AUTH_REQUIRED, _errors.KIND_DENIED,
            entity={
                "method": method,
                "reason_code": "namespace_mismatch",
                "required": required,
                "actual": actual,
                "remedy": reason,
            },
            expected=required, actual=actual, hint=reason,
        )

    def _raise_path_outside(self, method: str, field: str, value: Any,
                            root: Path) -> None:
        remedy = (
            "Place workspace tool files under the server-derived "
            f"{root} and pass that real path."
        )
        _errors.raise_error(
            _errors.INVALID_TOOL_DEFINITION, _errors.KIND_INVALID_DEFINITION,
            entity={
                "method": method,
                "reason_code": "path_outside_tool_root",
                "field": field,
                "path": value,
                "tool_root": str(root),
                "remedy": remedy,
            },
            expected=f"path inside {root}",
            actual=value,
            hint="workspace tool paths must stay inside the namespace tool root.",
        )

    def _workspace_fields(self, method: str, principal: Any,
                          params: dict[str, Any]) -> tuple[
                              str, Optional[str], Optional[str], str
                          ]:
        """Server-written provenance plus workspace path containment (P4.1).

        Plain ``full``/``root`` names stay global. A ``full`` credential that
        also carries a namespace and registers a workspace-shaped
        ``agent.<namespace>.<slug>`` name is treated as a workspace
        registration: the service still writes provenance/containment, so the
        unified RPC plane accepts the dotted name without inventing a per-tool
        authority grant.
        """
        name = params["name"]
        registered_by = self._principal_identity(principal)
        authority = getattr(principal, "authority_ceiling", None)
        namespace = getattr(principal, "namespace", None)
        namespaced_full = (
            authority == AUTHORITY_FULL
            and isinstance(namespace, str)
            and bool(namespace)
            and workspace_tool_name_error(name, namespace) is None
        )
        if self._is_root(principal) or (
            authority == AUTHORITY_FULL and not namespaced_full
        ):
            return (params["tool_path"], params.get("execution_path"),
                    None, registered_by)
        if authority not in (AUTHORITY_WORKSPACE_WRITE, AUTHORITY_FULL):
            _errors.raise_error(
                _errors.INTERNAL_ERROR, _errors.KIND_INTERNAL,
                entity={"method": method, "authority": authority},
                expected="workspace-write|full|root policy allowance",
                actual=authority,
            )
        reason = workspace_tool_name_error(name, namespace)
        root = workspace_root(self._storage_root, namespace)
        if reason is not None or root is None:
            _deny_reason = reason or "server-derived storage root is not configured"
            self._deny_workspace_provenance(
                method, _deny_reason, required=f"agent.{namespace}.<slug>",
                actual=name)
        tool_real = contained_realpath(params["tool_path"], root)
        if tool_real is None:
            self._raise_path_outside(method, "tool_path", params["tool_path"], root)
        execution_path = params.get("execution_path")
        execution_candidate = (
            execution_path if isinstance(execution_path, str) and execution_path
            else os.path.dirname(tool_real)
        )
        execution_real = contained_realpath(execution_candidate, root)
        if execution_real is None:
            self._raise_path_outside(
                method, "execution_path", execution_candidate, root)
        return tool_real, execution_real, namespace, registered_by

    def _card_holder_visible(self, principal: Any,
                             tool: "Optional[ToolDefinition]") -> bool:
        """守卫工具可见性（D4 单点）——经统一政策，不再自行匹配卡片。

        - 现代主体：由 ``authority_ceiling``/``resource_scope`` 裁决（full 全局，
          workspace 需有效权限）；不读取 legacy 卡片。
        - legacy 持卡主体（无 authority_ceiling）：只在 ``authorize_with_cards``
          边界把单张活动卡适配成政策输入——单卡 methods ⊇ requires、过期卡不授、
          显式空 ``stream_scope`` 不覆盖（D4/D6 语义保留）。
        - requires 为空（无守卫）在调用方短路，不进入本方法。
        """
        if tool is None:
            return False
        verifier = self._credential_verifier
        label = getattr(principal, "label", None)
        cards = None
        if verifier is not None and label:
            cards = lambda: verifier.active_cards_for(label)  # noqa: E731
        decision = authorize_with_cards(
            principal,
            "tool.read",
            {"requires": tuple(tool.requires or ())},
            cards=cards,
        )
        return decision.allow

    def _subject_executable(self, principal: Any,
                            tool: "Optional[ToolDefinition]") -> bool:
        """单工具可见性谓词（不可执行即不可见）。"""
        if tool is None:
            return False  # 查找 miss → 视为不存在（fail-closed）
        if principal is not None and getattr(principal, "is_root", False):
            return True  # root = 全量
        if not tool.requires:
            return True  # 无 requires = 未守卫 → 人人可见
        return self._card_holder_visible(principal, tool)

    def _executable_predicate(self, params: dict[str, Any],
                              ctx: Any) -> Callable[["Optional[ToolDefinition]"], bool]:
        """把当前调用方 principal 固化成可复用谓词（读面 handler 用）。

        接受 Optional（registry.get 可能 miss）：None → 不可见（fail-closed，
        由 _subject_executable 统一处理）。
        """
        principal = self._principal_of(params, ctx)
        return lambda tool: self._subject_executable(principal, tool)

    def _name_is_executable(self, params: dict[str, Any], ctx: Any,
                            name: str) -> bool:
        """按名字查 def 判可见（get 型：invisible ≡ 不存在）。"""
        tool = self.sdk.registry.get(name)
        if tool is None:
            return False
        return self._subject_executable(self._principal_of(params, ctx), tool)

    # ── 工具 CRUD / 查询 ─────────────────────────────────────────────

    async def _tool_list(self, params, ctx):
        # M6 可见性：不可执行即不可见（list 同样过滤，不只 get）
        pred = self._executable_predicate(params, ctx)
        return [t for t in self.sdk.list_tools(tags=params.get("tags"))
                if pred(self.sdk.registry.get(t["name"]))]

    async def _tool_get(self, params, ctx):
        name = params.get("name")
        if not name:
            _errors.raise_error(
                _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
                entity={"method": "tool.get"}, expected="params.name", actual=params,
            )
        # M6：invisible ≡ 不存在（无存在性 oracle）
        if not self._name_is_executable(params, ctx, name):
            _errors.raise_error(
                _errors.TOOL_NOT_FOUND, _errors.KIND_NOT_FOUND,
                entity={"tool": name}, expected="registered tool", actual=name,
            )
        tool = self.sdk.get_tool(name)
        if tool is None:
            _errors.raise_error(
                _errors.TOOL_NOT_FOUND, _errors.KIND_NOT_FOUND,
                entity={"tool": name}, expected="registered tool", actual=name,
            )
        return tool

    async def _tool_register(self, params, ctx):
        # Unified policy gate (root is decided inside authorize): before params.
        raw_name = params.get("name")
        principal = self._authorize_tool_write(
            "tool.register",
            Resource(name=raw_name if isinstance(raw_name, str) else None),
            params, ctx)
        # 必填参数校验（M2：缺参 → -32602 invalid-params，而不是 -32603 内部桶）
        for required in ("name", "description", "tool_path"):
            value = params.get(required)
            if not isinstance(value, str) or not value:
                _errors.raise_error(
                    _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
                    entity={"method": "tool.register"},
                    expected=f"非空 params.{required}",
                    actual=value,
                    hint="tool.register 需要 name/description/tool_path 三个必填参数。",
                )
        tool_path, execution_path, namespace, registered_by = (
            self._workspace_fields("tool.register", principal, params)
        )
        result = self.sdk.register_tool(
            name=params["name"], description=params["description"],
            tool_path=tool_path,
            execution_path=execution_path,
            brief_description=params.get("brief_description", ""),
            manual_path=params.get("manual_path"),
            tags=params.get("tags"),
            version=params.get("version", "1.0.0"),
            input_schema=params.get("input_schema"),
            requires=params.get("requires"),
            namespace=namespace,
            registered_by=registered_by,
        )
        self._audit(self._actor(params, ctx) or "unknown", "tool.register",
                    params.get("name", ""))
        if not result.get("success"):
            self._raise_result_error(result, entity={"method": "tool.register"})
        return result

    async def _tool_unregister(self, params, ctx):
        raw_name = params.get("name")
        name = raw_name if isinstance(raw_name, str) and raw_name else None
        existing = self.sdk.registry.get(name) if name else None
        principal = self._authorize_tool_write(
            "tool.unregister",
            Resource(
                name=name,
                namespace=existing.namespace if existing is not None else None,
                owner=existing.registered_by if existing is not None else None,
            ),
            params, ctx)
        if not self._is_root(principal) and not name:
            # ReactFlow §10.1 frozen probe: non-root stays -32001 before params.
            self._audit(self._actor(params, ctx) or "unknown",
                        "tool.unregister denied", "非 root probe 先于参数校验")
            _errors.raise_error(
                _errors.AUTH_REQUIRED, _errors.KIND_DENIED,
                entity={"method": "tool.unregister",
                        "reason_code": "invalid_params"},
                expected="params.name", actual=raw_name,
                hint="非 root 主体在参数校验前被授权门拒绝（ReactFlow probe）。",
            )
        if not raw_name:
            _errors.raise_error(
                _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
                entity={"method": "tool.unregister"}, expected="params.name",
                actual=params,
            )
        result = self.sdk.unregister_tool(name)
        self._audit(self._actor(params, ctx) or "unknown", "tool.unregister", name)
        if not result.get("success"):
            # §2.4.3：tool.unregister 失败归 -33001 not-found【M7】
            self._raise_result_error(result, entity={"tool": name,
                                                     "method": "tool.unregister"})
        return result

    async def _tool_search(self, params, ctx):
        # M6 可见性：不可执行即不可见（search 同 list）
        pred = self._executable_predicate(params, ctx)
        return [t for t in self.sdk.search_tools(
            params.get("pattern", ""), tags=params.get("tags"))
            if pred(self.sdk.registry.get(t["name"]))]

    async def _tool_get_manual(self, params, ctx):
        name = params.get("name")
        if not name:
            _errors.raise_error(
                _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
                entity={"method": "tool.get_manual"}, expected="params.name",
                actual=params,
            )
        # M6（G2）：invisible ≡ 不存在 → {"manual": None}（与现不存在形状一致）
        if not self._name_is_executable(params, ctx, name):
            return {"manual": None}
        return {"manual": self.sdk.get_tool_manual(name)}

    async def _tool_list_for_llm(self, params, ctx):
        # M6 可见性：不可执行即不可见（LLM 视图同 list）
        pred = self._executable_predicate(params, ctx)
        out = self.sdk.list_tools_for_llm(tags=params.get("tags"))
        return {"tools": [t for t in out.get("tools", [])
                          if pred(self.sdk.registry.get(t["name"]))]}

    async def _tool_get_for_llm(self, params, ctx):
        name = params.get("name")
        if not name:
            _errors.raise_error(
                _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
                entity={"method": "tool.get_for_llm"}, expected="params.name",
                actual=params,
            )
        # M6：invisible ≡ 不存在（无存在性 oracle）
        if not self._name_is_executable(params, ctx, name):
            _errors.raise_error(
                _errors.TOOL_NOT_FOUND, _errors.KIND_NOT_FOUND,
                entity={"tool": name}, expected="registered tool", actual=name,
            )
        tool = self.sdk.get_tool_for_llm(name)
        if tool is None:
            _errors.raise_error(
                _errors.TOOL_NOT_FOUND, _errors.KIND_NOT_FOUND,
                entity={"tool": name}, expected="registered tool", actual=name,
            )
        return tool

    async def _tool_summarize_for_prompt(self, params, ctx):
        # M6 可见性：不可执行即不可见（summarize 只含可见工具行）
        pred = self._executable_predicate(params, ctx)
        summary = self.sdk.summarize_tools_for_prompt(tags=params.get("tags"))
        lines = summary.split("\n") if summary else []
        visible_lines = [
            ln for ln in lines
            if ln and pred(self.sdk.registry.get(ln.split(":", 1)[0]))
        ]
        return {"summary": "\n".join(visible_lines)}

    async def _tool_format_call(self, params, ctx):
        name = params.get("tool_name")
        if not name:
            _errors.raise_error(
                _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
                entity={"method": "tool.format_call"}, expected="params.tool_name",
                actual=params,
            )
        return {"formatted": self.sdk.format_tool_call(
            name, params.get("params") or {})}

    async def _tool_parse_calls(self, params, ctx):
        return self.sdk.parse_tool_calls(params.get("text", ""))

    async def _tool_clear(self, params, ctx):
        # Unified policy gate: tool.clear is root-reserved, before side effects.
        self._authorize_tool_write("tool.clear", Resource(), params, ctx)
        self.sdk.registry.clear()
        self._audit(self._actor(params, ctx) or "unknown", "tool.clear", "registry cleared")
        return {"success": True}

    # ── 进程管理 ─────────────────────────────────────────────────────

    async def _tool_send_input(self, params, ctx):
        return self.sdk.send_input(
            params.get("process_name", ""), params.get("data", ""),
            append_newline=params.get("append_newline", True),
            encoding=params.get("encoding", "utf-8"),
        )

    async def _tool_list_processes(self, params, ctx):
        return self.sdk.list_processes(tool_name=params.get("tool_name"),
                                       status=params.get("status", "alive"))

    async def _tool_get_process_info(self, params, ctx):
        info = self.sdk.get_process_info(params.get("process_name", ""))
        if info is None:
            _errors.raise_error(
                _errors.TOOL_NOT_FOUND, _errors.KIND_NOT_FOUND,
                entity={"process": params.get("process_name")},
                expected="running background process", actual=None,
            )
        return info

    async def _tool_stop_process(self, params, ctx):
        return self.sdk.stop_process(params.get("process_name", ""),
                                     timeout=params.get("timeout", 2))

    async def _tool_stop_processes(self, params, ctx):
        return self.sdk.stop_process(params.get("targets", []),
                                     timeout=params.get("timeout", 2))

    async def _tool_stop_all_processes(self, params, ctx):
        return self.sdk.stop_all_processes(timeout=params.get("timeout", 2))

    # ── 分类内查询 ───────────────────────────────────────────────────

    def _require_stream(self, params, method: str) -> str:
        stream = params.get("stream_name")
        if not stream:
            _errors.raise_error(
                _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
                entity={"method": method}, expected="params.stream_name",
                actual=params,
            )
        return stream

    async def _tool_classified_list(self, params, ctx):
        stream = self._require_stream(params, "tool.classified.list")
        # G1：会话谓词（SDK 内）保留；结果面叠加守卫可见性（不可执行即不可见）
        pred = self._executable_predicate(params, ctx)
        return [t for t in self.sdk.list_classified_tools(
            stream, tags=params.get("tags"))
            if pred(self.sdk.registry.get(t["name"]))]

    async def _tool_classified_list_for_llm(self, params, ctx):
        stream = self._require_stream(params, "tool.classified.list_for_llm")
        pred = self._executable_predicate(params, ctx)
        out = self.sdk.list_classified_tools_for_llm(
            stream, tags=params.get("tags"))
        return {"tools": [t for t in out.get("tools", [])
                          if pred(self.sdk.registry.get(t["name"]))]}

    async def _tool_classified_summarize(self, params, ctx):
        stream = self._require_stream(params, "tool.classified.summarize_for_prompt")
        pred = self._executable_predicate(params, ctx)
        summary = self.sdk.summarize_classified_tools_for_prompt(
            stream, tags=params.get("tags"))
        lines = summary.split("\n") if summary else []
        visible_lines = [
            ln for ln in lines
            if ln and pred(self.sdk.registry.get(ln.split(":", 1)[0]))
        ]
        return {"summary": "\n".join(visible_lines)}

    async def _tool_classified_get(self, params, ctx):
        stream = self._require_stream(params, "tool.classified.get")
        name = params.get("name", "")
        # G1：会话谓词保留；守卫工具对无卡主体在 classified 读面同样隐藏
        #（invisible ≡ 不存在 → None，与现分类外形状一致）
        if not self._name_is_executable(params, ctx, name):
            return None
        return self.sdk.get_classified_tool(stream, name)

    async def _tool_classified_get_for_llm(self, params, ctx):
        stream = self._require_stream(params, "tool.classified.get_for_llm")
        name = params.get("name", "")
        if not self._name_is_executable(params, ctx, name):
            return None
        return self.sdk.get_classified_tool_for_llm(stream, name)

    # ── scan.* 提级一等域 ─────────────────────────────────────────────

    def _authorize_scan_control(self, method: str, stream: str,
                                params: dict[str, Any], ctx: Any) -> None:
        authorize_scan_control(
            principal=self._principal_of(params, ctx),
            action=method,
            stream=stream,
            task_manager=getattr(self.sdk, "task_manager", None),
        )

    def _wrap_stream_sdk(self, params: dict[str, Any]) -> Optional[StreamSDKWrapper]:
        """构造本次调用的进程内流连接（注入桥 client 复用；无注入 → None）。

        W2：进程内直连没有可无参构造的真客户端 —— 装配把 SyncStreamClient 注入
        ``self._client`` 后这里复用；未注入时返回 None（调用方不启动默认连接）。
        """
        del params
        if self._client is not None:
            return StreamSDKWrapper(client=self._client)
        return None

    async def _scan_run(self, params, ctx):
        stream = self._require_stream(params, "scan.run")
        actor = self._actor(params, ctx)
        # M3 身份透传：完整启动主体 Principal（None = 无完整 ctx 身份；
        # 经 to_thread → _launch_scan → sdk/task_manager 落到 executor/controller）
        starter_principal = self._principal_of(params, ctx)
        # H1-B：RPC 入口完整启动主体闸（fail-closed；见 _require_rpc_starter_principal）
        self._require_rpc_starter_principal(
            ctx, starter_principal, stream=stream, method="scan.run")
        scan_authority = resolve_scan_authority(
            starter_principal, params.get("authority"),
            stream=stream, method="scan.run")
        # H1：深度不再读 params.depth（自报键废弃，接受但忽略）；
        # 嵌套审计链/护栏深度由 _launch_scan 内经 derive_scan_depth 凭据派生
        parent_stream = params.get("parent_stream") or params.get("$parent_stream")
        parent_tool = params.get("parent_tool") or params.get("$parent_tool")
        # 阻塞型扫描 + 门禁/嵌套链记录都落到工作线程（桥调用面见 _launch_scan）
        return await asyncio.to_thread(
            self._launch_scan, stream, mode="run", actor=actor,
            starter_principal=starter_principal,
            parent_stream=parent_stream, parent_tool=parent_tool,
            scan_authority=scan_authority,
        )

    async def _scan_start(self, params, ctx):
        stream = self._require_stream(params, "scan.start")
        actor = self._actor(params, ctx)
        # M3 身份透传：完整启动主体 Principal（同 _scan_run）
        starter_principal = self._principal_of(params, ctx)
        # H1-B：RPC 入口完整启动主体闸（fail-closed；见 _require_rpc_starter_principal）
        self._require_rpc_starter_principal(
            ctx, starter_principal, stream=stream, method="scan.start")
        scan_authority = resolve_scan_authority(
            starter_principal, params.get("authority"),
            stream=stream, method="scan.start")
        # H1：深度不再读 params.depth（自报键废弃，接受但忽略）
        parent_stream = params.get("parent_stream") or params.get("$parent_stream")
        parent_tool = params.get("parent_tool") or params.get("$parent_tool")
        return await asyncio.to_thread(
            self._launch_scan, stream, mode="start", actor=actor,
            starter_principal=starter_principal,
            parent_stream=parent_stream, parent_tool=parent_tool,
            scan_authority=scan_authority,
        )

    async def _scan_rescan(self, params, ctx):
        """scan.rescan：从头重扫（游标归零 + 起扫；计划 §5 / 阶段 6）。

        与 scan.start/run 并列的一等 RPC：对 idle 流先把 ``_scan_end_{stream}``
        归零（F2c anchored 零值 ``{anchor_version: head, offset: 0}``，经
        StreamSDKWrapper.set_internal 写穿透落盘），再起扫。

        - 参数：``stream_name``（必填；兼容计划 §5.1 的 ``stream`` 别名）；
          ``mode``（可选 ``"run"`` | ``"start"``，默认 ``"run"``）。
        - 流不存在（桥 ``ValueError``）→ 帧级 -32002 kind=not-found（显式
          存在性校验，先于任何状态变更——区别于 scan.start/run 的异步失败）；
          其余读流异常（超时/基础设施/域错误）原样上抛，不吞成 not-found。
        - 该流有活动扫描 → 结果字典 ``success=false + error.kind=flow-busy``
          （-33006 名义码，复用 ScannerStateError；不隐式 cancel），且游标
          不被归零（零副作用）。
        - 门禁/启动者复核经 ``_launch_scan`` 自然继承（-33008 / -32001）。
        - 归零置于全部可失败复核之后、launch 之前；launch 失败（复核抛错或
          SDK 返回失败字典）→ 写回旧游标，不留归零残留。
        """
        # 参数名兼容：一等 scan.* 面用 stream_name；计划 §5.1 亦以 stream 命名。
        stream = params.get("stream_name") or params.get("stream")
        if not stream:
            _errors.raise_error(
                _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
                entity={"method": "scan.rescan"},
                expected="params.stream_name", actual=params,
            )
        mode = params.get("mode", "run")
        if mode not in ("run", "start"):
            _errors.raise_error(
                _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
                entity={"method": "scan.rescan", "stream_name": stream},
                expected="params.mode ∈ {'run', 'start'}", actual=mode,
            )
        actor = self._actor(params, ctx)
        # M3 身份透传：完整启动主体 Principal（同 scan.run/scan.start）
        starter_principal = self._principal_of(params, ctx)
        # H1-B：RPC 入口完整启动主体闸（fail-closed；见 _require_rpc_starter_principal）
        self._require_rpc_starter_principal(
            ctx, starter_principal, stream=stream, method="scan.rescan")
        scan_authority = resolve_scan_authority(
            starter_principal, params.get("authority"),
            stream=stream, method="scan.rescan")
        # H1：深度不再读 params.depth（自报键废弃，接受但忽略）
        parent_stream = params.get("parent_stream") or params.get("$parent_stream")
        parent_tool = params.get("parent_tool") or params.get("$parent_tool")
        return await asyncio.to_thread(
            self._rescan, stream, mode=mode, actor=actor,
            starter_principal=starter_principal,
            parent_stream=parent_stream, parent_tool=parent_tool,
            scan_authority=scan_authority,
        )

    def _rescan(self, stream: str, *, mode: str, actor: Optional[str],
                starter_principal: Optional["Principal"] = None,
                parent_stream: Optional[str] = None,
                parent_tool: Optional[str] = None,
                scan_authority: Optional[ScanAuthority] = None) -> dict[str, Any]:
        """工作线程内执行从头重扫（scan.rescan 真身；序列见 _scan_rescan）。

        序列：``_enforce_scan_gate``（分类门 -33008）→ 显式存在性校验
        （-32002）→ 活动扫描拒（flow-busy 结果字典，零副作用）→ 游标归零
        → ``_launch_scan``（继承 -32001 启动者复核 + 起扫）。
        本方法只在线程上下文调用（域处理器经 asyncio.to_thread 分发）。
        """
        self._enforce_scan_gate(stream)
        sdk_wrapper = self._wrap_stream_sdk({})
        # 显式存在性校验：先于任何状态变更（rescan 要确定性报错）。元数据同
        # 时取作归零锚的 head version（见下），避免二次读流。
        # I2：只把桥的 not-found（``ValueError``，见 stream_bridge.get_metadata）
        # 判 -32002；超时/基础设施/域错误等一律原样上抛，不吞成 not-found。
        meta: dict[str, Any] = {}
        if sdk_wrapper is not None:
            try:
                meta = sdk_wrapper.get_metadata(stream)
            except ValueError:
                _errors.raise_error(
                    _errors.STREAM_NOT_FOUND, _errors.KIND_NOT_FOUND,
                    entity={"stream_name": stream, "method": "scan.rescan"},
                    expected="已存在的流", actual=stream,
                    hint="先用 stream.list 确认流名，或 stream.create 创建。",
                )
        # 活动扫描拒（安全默认，不隐式 cancel）：flow-busy 结果字典，零副作用。
        # 先于归零判定——被拒时游标必须原样（内存 + 磁盘）。
        status = self.sdk.task_manager.status(stream)
        if status.get("state") in ("scanning", "paused"):
            message, hint = _errors.code_message(_errors.SCANNER_STATE_ERROR)
            return success_false(
                _errors.KIND_FLOW_BUSY, message=message, hint=hint,
                context={"stream_name": stream, "method": "scan.rescan"},
            )
        # 游标归零：置于所有可失败复核之后、launch 之前最近点。先快照旧游标，
        # launch 被拒（复核抛错 -32001 / SDK 返回失败字典 -33006 竞态 / -33009
        # 等）时写回——否则失败残留归零游标，下次扫描重放保留标记（F2c 要消除
        # 的形态）。归零写 anchored 零值 {anchor_version: head, offset: 0}：
        # offset=0 投影恒等（delta(V,V,0)=0），anchor_version 取当前 head 满足
        # 锚点窗口 base≤anchor≤head（取旧值/0 可能 < base → stale）。
        cursor_key = f"_scan_end_{stream}"
        frontier_key = f"_scan_frontier_{stream}"
        old_cursor: str | None = None
        old_frontier: str | None = None
        if sdk_wrapper is not None:
            old_cursor = sdk_wrapper.get_internal(cursor_key)
            old_frontier = sdk_wrapper.get_internal(frontier_key)
            sdk_wrapper.set_internal(
                cursor_key, self._rescan_zero_payload(stream, meta)
            )
            # scan.rescan 是唯一清空语义前沿的入口（从 0 开始，旧前沿不得复活）。
            sdk_wrapper.delete_internal(frontier_key)
        try:
            result = self._launch_scan(
                stream, mode=mode, actor=actor,
                starter_principal=starter_principal,
                parent_stream=parent_stream, parent_tool=parent_tool,
                scan_authority=scan_authority,
            )
        except Exception:
            self._restore_scan_cursor(sdk_wrapper, cursor_key, old_cursor)
            self._restore_scan_cursor(sdk_wrapper, frontier_key, old_frontier)
            raise
        # SDK 复核失败以结果字典返回（success=false + error，非抛错）——同样
        # 恢复旧游标；完成扫描的字典无 error 键（见 task_manager.run 返回），
        # 故不误判「跑完但工具失败」为 launch 失败。
        if isinstance(result, dict) and result.get("error"):
            self._restore_scan_cursor(sdk_wrapper, cursor_key, old_cursor)
            self._restore_scan_cursor(sdk_wrapper, frontier_key, old_frontier)
        return result

    @staticmethod
    def _rescan_zero_payload(stream: str, meta: dict[str, Any]) -> str:
        """rescan 归零载荷：唯一产出 anchored 零值 ``{anchor_version: head, offset: 0}``。

        stream 元数据缺 ``version`` ⇒ 无法计算有效锚（静默写 ``anchor_version=0``
        在 base>0 流上即 stale，legacy ``{"end": 0}`` 也不再支持）→ fail-closed
        抛 internal-error（-32603）。
        """
        version = meta.get("version")
        if version is None:
            _errors.raise_error(
                _errors.INTERNAL_ERROR, _errors.KIND_INTERNAL,
                entity={"stream": stream, "method": "scan.rescan"},
                expected="stream metadata containing 'version'",
                actual=meta,
                hint=(
                    "流元数据缺 version，无法计算 anchored 归零游标；"
                    "先用 stream.list/stream.read 确认流状态后重试。"
                ),
            )
        return encode_anchored(int(version), 0)

    @staticmethod
    def _restore_scan_cursor(sdk_wrapper: Optional[StreamSDKWrapper],
                             cursor_key: str, old_cursor: str | None) -> None:
        """launch 被拒时写回 rescan 前的游标（旧值缺省则删除键）。

        恢复失败不反噬原始失败原因：记 warning 后继续（原始异常/结果帧照常
        返回）。``sdk_wrapper is None``（未注入 client 的独立形态）为 no-op。
        """
        if sdk_wrapper is None:
            return
        try:
            if old_cursor is None:
                sdk_wrapper.delete_internal(cursor_key)
            else:
                sdk_wrapper.set_internal(cursor_key, old_cursor)
        except Exception as e:  # noqa: BLE001 - 恢复失败不得掩盖 launch 失败因
            logger.warning(
                "scan.rescan: internal restore failed for %r: %s", cursor_key, e,
            )

    def _launch_scan(self, stream: str, *, mode: str, actor: Optional[str],
                     starter_principal: Optional["Principal"] = None,
                     parent_stream: Optional[str] = None,
                     parent_tool: Optional[str] = None,
                     scan_authority: Optional[ScanAuthority] = None) -> dict[str, Any]:
        """工作线程内执行一次扫描启动（run/start 的共享真身）。

        本方法只在线程上下文调用（域处理器经 asyncio.to_thread 分发）：门禁查询、
        嵌套链记录、SDK 启动三者都会做同步流桥调用 —— 必须离开宿主事件循环线程。

        Args:
            starter_principal: M3 完整启动主体（WS ctx.principal 原样透传；
                None = manifest/无完整 ctx 身份）。
            parent_stream/parent_tool: 观测投影（§5.8）：嵌套来源父流/父工具。

        Returns:
            sdk.run_scan / start_scan 结果字典。
        """
        self._enforce_scan_gate(stream)
        # M8 revalidate（a6 §三.5.1）：启动主体逐次重验——凭据吊销/过期即刻
        # 生效，不留已认证活连接窗口。starter None（root 名义 / manifest /
        # 无完整 ctx）不重验、照旧放行（D5 3c root 名义）；重验失败 → 拒启动。
        if starter_principal is not None and not self._starter_still_valid(
            starter_principal
        ):
            _errors.raise_error(
                _errors.AUTH_REQUIRED, _errors.KIND_DENIED,
                entity={"stream_name": stream, "method": f"scan.{mode}"},
                expected="有效的启动主体凭据（revalidate 通过）",
                actual="启动主体凭据已吊销/过期",
                hint="启动主体凭据已吊销或过期；重新出示有效凭据后再起扫。",
            )
        # H1：嵌套审计链深度由凭据派生（唯一真源），不再读 params.depth
        effective_depth = derive_scan_depth(starter_principal)
        if effective_depth > 0 and parent_stream:
            self._record_nested_start(parent_stream, stream, effective_depth,
                                      parent_tool)
        sdk_wrapper = self._wrap_stream_sdk({})
        if mode == "run":
            return self.sdk.run_scan(
                stream, sdk=sdk_wrapper, depth=effective_depth, actor=actor,
                starter_principal=starter_principal,
                parent_stream=parent_stream, parent_tool=parent_tool,
                scan_authority=scan_authority,
            )
        return self.sdk.start_scan(
            stream, sdk=sdk_wrapper, depth=effective_depth, actor=actor,
            starter_principal=starter_principal,
            parent_stream=parent_stream, parent_tool=parent_tool,
            scan_authority=scan_authority,
        )

    def _enforce_scan_gate(self, stream_name: str) -> None:
        """scan.start/scan.run 启动门（W2 S1 MUST DO #4）：受限流拒绝起扫。

        咨询 ``classification_store.is_scan_allowed(stream_name, {"tags": 流tags})``：
        - 无分类存储（未装配）→ 放行（旧恒真缺省；测试兼容）；
        - unclassified / 谓词命中 → 放行；blocked / 谓词不匹配 / 受限且无元数据
          → 抛 -33008 kind=classify-denied（fail-closed）。
        """
        store = self.classification_store
        if store is None:
            return
        tags: dict[str, Any] | None = None
        wrapper = self._wrap_stream_sdk({})
        if wrapper is not None:
            try:
                meta = wrapper.get_metadata(stream_name)
                tags = dict(meta.get("tags") or {})
            except Exception:
                tags = None  # 流不存在/读元数据失败 → 按无元数据处理（受限流拒绝）
        ctx_payload = {"tags": tags} if tags is not None else None
        if store.is_scan_allowed(stream_name, ctx_payload):
            return
        _errors.raise_error(
            _errors.CLASSIFY_DENIED, _errors.KIND_CLASSIFY_DENIED,
            entity={"stream_name": stream_name, "method": "scan.start"},
            expected="扫描目标在分类范围内",
            actual="受限/被封锁（classify-denied）",
            hint="该流处于受限分类（blocked / 谓词不匹配 / 受限无元数据）；"
                 "先查 classify.get(target=streams) 并按需调整。",
        )

    def _starter_still_valid(self, starter_principal: Optional["Principal"]) -> bool:
        """M8 revalidate（a6 §三.5）：启动主体凭据是否仍有效（吊销即刻生效）。

        - starter_principal None → 放行（root 名义 / manifest，无从复验）；
        - 无 token_hash 的 principal（duck-typed ctx / 测试形态）→ 放行
          （无法查表；真实装配 ctx.principal 一律来自 AuthShell verify，
          必带 token_hash——M8 对真实凭据生效）；
        - 无真身 verifier（独立单测形态）→ 放行（保持既有行为零改动）；
        - revalidate 成功 → True；失败（吊销/过期/scope 门/复验异常）→ False。
        """
        if starter_principal is None:
            return True
        if not getattr(starter_principal, "token_hash", None):
            return True
        verifier = self._credential_verifier
        if verifier is None:
            return True
        rv = getattr(verifier, "revalidate", None)
        if not callable(rv):
            return True
        try:
            return rv(starter_principal) is not None
        except Exception as e:  # noqa: BLE001 - 复验异常按 fail-closed 处理
            import logging
            logging.getLogger(__name__).warning(
                "Starter revalidate error for scan: %s", e
            )
            return False

    def _record_nested_start(self, parent_stream: str, child_stream: str,
                             depth: int, parent_tool: Optional[str]) -> None:
        """观测投影（§4.11）：在父流 {stream}_scan_log 追加 nested 链记录。

        记录 = 同 envelope 的 JSONL（``type="nested"``），与诊断行同一行语法：
        ``{"_snakeeats":"scan_diag","v":1,"type":"nested",...}``。经 C2 普通流写。
        定位=审计/可视化，不参与分派、不继承限制、不作执行控制。
        仅在线程上下文调用（_launch_scan 经 to_thread）。
        """
        try:
            from .scan_diagnostics import build_nested_record, encode_record

            sdk_wrapper = self._wrap_stream_sdk({})
            if sdk_wrapper is None:
                return
            log_name = f"{parent_stream}_scan_log"
            log_stream = sdk_wrapper.get_or_create_internal(
                log_name, f"{parent_stream} 扫描日志"
            )
            record = build_nested_record(
                parent_stream=parent_stream,
                child_stream=child_stream,
                depth=depth,
                parent_tool=parent_tool,
            )
            log_stream.append("\n" + encode_record(record) + "\n")
            sdk_wrapper.shutdown()
        except Exception as e:
            import logging
            logging.getLogger(__name__).debug(f"nested record write failed: {e}")

    async def _scan_pause(self, params, ctx):
        stream = self._require_stream(params, "scan.pause")
        self._authorize_scan_control("scan.pause", stream, params, ctx)
        result = await asyncio.to_thread(self.sdk.pause_scan, stream)
        # P1 暂停耐久化：pause 成功后立即 flush 该流 pending 游标，尽量让
        # 优雅重启从暂停位置续扫。契约：若 result 为 ``"pausing"``（轮/重试链
        # 尚未 settle），此处是 best-effort——settle 期间的写由 2s 周期 flusher
        # 覆盖；崩溃窗口约 5s（SCAN_CURSOR_FLUSH_INTERVAL_S）。flush 失败只
        # warning，绝不让迁移/写盘问题把已有的 pause RPC 变成失败。
        if self._cursor_flush is not None:
            try:
                await asyncio.to_thread(self._cursor_flush, stream)
            except Exception as exc:  # noqa: BLE001 —— pause 已成功，flush 是尽力而为
                logger.warning(
                    "scan.pause cursor flush failed for %r: %s", stream, exc)
        return result

    async def _scan_resume(self, params, ctx):
        """``scan.resume{stream_name, reanchor_to?}``：恢复暂停扫描。

        *reanchor_to*（可选，head 绝对坐标）= UI 候选选择；缺省 = 自动重锚
        （后端按语义前沿定位，旧客户端不受影响）。重锚失败不抛帧级错误：
        事件 ``resume-target-missing`` 带候选，扫描保持 PAUSED。
        """
        stream = self._require_stream(params, "scan.resume")
        self._authorize_scan_control("scan.resume", stream, params, ctx)
        reanchor_to = params.get("reanchor_to")
        target = None if reanchor_to is None else int(reanchor_to)
        return await asyncio.to_thread(
            self.sdk.resume_scan, stream, reanchor_to=target)

    async def _scan_stop(self, params, ctx):
        stream = self._require_stream(params, "scan.stop")
        self._authorize_scan_control("scan.stop", stream, params, ctx)
        return await asyncio.to_thread(self.sdk.stop_scan, stream)

    async def _scan_move(self, params, ctx):
        stream = self._require_stream(params, "scan.move")
        self._authorize_scan_control("scan.move", stream, params, ctx)
        return await asyncio.to_thread(
            self.sdk.move_scan_marker, stream, int(params.get("position", 0))
        )

    async def _scan_set_authority(self, params, ctx):
        """``scan.set_authority{stream_name, authority}``（P3.2）。

        仅 settled PAUSED、仅 root/当前 starter、scan child 恒拒。解析与事务
        都落到工作线程：事务按 scan lock → table lock 顺序同步 ScanTask
        authority/generation 与 P3.1 子票替换，事件/审计在锁外执行（no IO
        inside the publish critical section）。
        """
        stream = self._require_stream(params, "scan.set_authority")
        actor = self._actor(params, ctx) or "unknown"
        principal = self._principal_of(params, ctx)
        self._require_rpc_starter_principal(
            ctx, principal, stream=stream, method="scan.set_authority")
        requested = params.get("authority")
        self._audit(
            actor, "scan.set_authority request",
            f"{stream} -> {requested!r}",
        )
        try:
            result = await asyncio.to_thread(
                self._apply_authority_switch, stream, requested, principal, actor
            )
        except _errors.SnakeEatsError as exc:
            self._audit(actor, "scan.set_authority denied", repr(exc))
            raise
        self._audit(
            actor, "scan.set_authority result",
            f"{result['previous_authority']} -> {result['authority']} "
            f"generation={result['generation']}",
        )
        return result

    def _apply_authority_switch(self, stream: str, requested: Any,
                                principal: Any, actor: str) -> dict[str, Any]:
        """Worker-thread resolution + transaction (bridge/table IO allowed)."""
        manager = self.sdk.task_manager
        context = manager.scan_context_of(stream)
        if context is None:
            raise ScannerNotRunningError(stream)
        plan = resolve_authority_switch(
            principal,
            requested,
            stream=stream,
            task_context=context,
            starter_principal=manager.starter_principal_of(stream),
            verifier=self._credential_verifier,
        )
        return manager.switch_scan_authority(stream, plan, actor=actor)

    async def _scan_status(self, params, ctx):
        stream = self._require_stream(params, "scan.status")
        return await asyncio.to_thread(self.sdk.get_scan_status, stream)

    async def _scan_list(self, params, ctx):
        """scan.list：任务状态列表 + depth/parent_stream 只读投影（§5.8 最小）。

        数据源 = 任务登记行；depth/parent_stream 无记录时置 null。
        活动任务分类态经会话适配器读控制器（会话级；只读投影，不触发 store 写）。
        经 to_thread 落到工作线程（list_tasks 对活动任务读 position = 同步桥调用）。
        """
        return await asyncio.to_thread(self._list_scan_tasks_with_projection)

    def _list_scan_tasks_with_projection(self) -> dict[str, Any]:
        tasks = self.sdk.list_scan_tasks()
        lookup = None
        for t in tasks:
            stream = t.get("stream_name")
            t.setdefault("depth", None)
            t.setdefault("parent_stream", None)
            if stream:
                if lookup is None:
                    lookup = make_session_lookup(self.sdk.task_manager)
                adapter = lookup(stream)
                if adapter is not None:
                    try:
                        cls = adapter.get_classification()
                    except Exception:
                        cls = None
                    if isinstance(cls, dict):
                        t.setdefault("classification_state", cls.get("state"))
        return {"tasks": tasks}

    async def _scan_cancel(self, params, ctx):
        """scan.cancel：撤任务 + 杀后台子代理 + 吊销票据（M9/§5.4 第3条）。

        授权归属与 pause/resume/stop/move 统一走 P2.3 政策面：root、starter
        或本扫描派生主体的显式 own-scan 操作可撤；跨扫描拒绝以 -32001
        kind=denied + owner_mismatch 表达，不再冒用 classify-denied。
        """
        stream = self._require_stream(params, "scan.cancel")
        actor = self._actor(params, ctx) or "unknown"
        self._authorize_scan_control("scan.cancel", stream, params, ctx)

        def _cancel() -> dict[str, Any]:
            result = self.sdk.cancel_scan(stream)
            self._audit(actor, "scan.cancel", stream)
            return result

        result = await asyncio.to_thread(_cancel)
        if not result.get("success"):
            self._raise_result_error(result, entity={"stream": stream,
                                                     "method": "scan.cancel"})
        return result

    # ── system.info（S1 移除）──────────────────────────────────────────

    # 统一 system.info 由 StreamDomain 承载（system. 前缀归属单一化），
    # 工具/扫描计数经 StreamDomain.info_provider 注入（assembly 装配）；
    # 本域不再登记 system.info，避免 Router 精确键/前缀双真值（T3 HANDOFF §7）。

    # ── 结果字典错误 → 帧层 SnakeEatsError（§2.4.3 归码）─────────────

    def _raise_result_error(self, result: dict[str, Any], *, entity: dict[str, Any]) -> None:
        """把 success=false 结果字典收敛成 C1 SnakeEatsError（帧层）。

        error 载荷 = {kind, message, hint, context}（本域结果字典面规范）。
        kind → code 归码经 errors.raise_error 完成；code 缺省按 kind 找码位，
        找不到按 internal-error 兜底。
        """
        err = result.get("error") or {}
        kind = err.get("kind") if isinstance(err, dict) else "internal-error"
        code = _kind_code(kind)
        _errors.raise_error(
            code, kind,
            entity=entity,
            message=err.get("message") if isinstance(err, dict) else str(err),
            hint=err.get("hint") if isinstance(err, dict) else None,
        )


def _kind_code(kind: str) -> int:
    """C1 kind → 错误码（真源见 errors.KIND_TO_CODE，M11 两段制）。"""
    return _errors.code_for_kind(kind)
