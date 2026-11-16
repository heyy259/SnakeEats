"""统一 SDK 方法面登记表 —— 94 个精确 RPC 键（ROUTER_REGISTRY.md 真源）。

- ``METHOD_KEYS``：94 键有序元组（= Router method_registry 精确键并集）。
- ``IDEMPOTENT_METHODS``：幂等 RPC 键集合（模块级显式声明，§2.12/§5.3.4）。
- ``_RPC_SPECS``：键 → (中文描述, 参数 docstring)，供薄方法生成器消费。

幂等集合口径（重试收敛决策，HANDOFF 记录）：
读类幂等 = stream.read/get/list/query/search/search_between/search_in_range/
search_in_stream/read_lines/read_rebased/get_metadata、tool.list/list_for_llm/
get/get_for_llm/get_manual/search/parse_calls/summarize_for_prompt/
list_processes/get_process_info、tool.classified.*（5）、scan.list/status、
system.info、internal.get/list、control.audit_read、control.list_checkpoints
（D6 descope 新增读类，33 键）；
**其余一律非幂等**（写/scan 控制/control.mint/control.recover_stream/
clear/register… 零自动重发——recover_stream 非幂等**禁入重试词表**，模块级
断言守卫）。
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "METHOD_KEYS",
    "METHOD_NAME_TO_KEY",
    "IDEMPOTENT_METHODS",
    "rpc_method_attr",
    "RPC_SPECS",
]


def rpc_method_attr(key: str) -> str:
    """RPC 键（点号）→ 薄方法名（下划线）：``stream.create`` → ``stream_create``。"""
    return key.replace(".", "_")


#: 幂等 RPC 键集合（模块级显式声明 —— 薄方法/重试策略消费同一词表）。
IDEMPOTENT_METHODS: frozenset[str] = frozenset({
    # stream 读类
    "stream.read",
    "stream.get",
    "stream.list",
    "stream.query",
    "stream.search",
    "stream.search_between",
    "stream.search_in_range",
    "stream.search_in_stream",
    "stream.read_lines",
    "stream.read_rebased",
    "stream.get_metadata",
    # tool 读类
    "tool.list",
    "tool.list_for_llm",
    "tool.get",
    "tool.get_for_llm",
    "tool.get_manual",
    "tool.search",
    "tool.parse_calls",
    "tool.summarize_for_prompt",
    "tool.list_processes",
    "tool.get_process_info",
    # tool.classified.*（分类内读查询）
    "tool.classified.get",
    "tool.classified.get_for_llm",
    "tool.classified.list",
    "tool.classified.list_for_llm",
    "tool.classified.summarize_for_prompt",
    # scan 读类（新域）
    "scan.list",
    "scan.status",
    # 系统/内部/审计读类
    "system.info",
    "internal.get",
    "internal.list",
    "control.audit_read",
    # 恢复控制面读类（D6 descope：list_checkpoints 只读 → 幂等词表）
    "control.list_checkpoints",
})


def _spec(key: str, desc: str, params: str = "") -> tuple[str, str, str]:
    return key, desc, params


#: (key, 中文一句描述, 参数说明)。94 键与 ROUTER_REGISTRY.md 逐行对齐。
_RPC_SPECS: tuple[tuple[str, str, str], ...] = (
    # ── stream.* 36 ─────────────────────────────────────────────────
    _spec("stream.append", "追加文本到流尾", "name(流名); text(文本); expected_version(可选CAS)"),
    _spec("stream.create", "创建流", "name(必填); description(描述); title(展示标题，自由Unicode，可空); tags(标签dict)"),
    _spec("stream.delete", "删除流", "name(必填)"),
    _spec("stream.delete_lines", "按行号删除", "name; start_line; end_line; expected_version(可选)"),
    _spec("stream.delete_range", "按区间删除文本", "name; start; end; expected_version(可选)"),
    _spec("stream.get", "获取流元数据（不存在→-32002）", "name(必填)"),
    _spec("stream.get_metadata", "获取流元数据(含tags)", "name(必填)"),
    _spec("stream.insert", "在 pos 位置插入文本", "name; pos; text; expected_version(可选)"),
    _spec("stream.list", "列出流（可按 tags 过滤）", "tags(可选)"),
    _spec("stream.prepend", "在流头前插文本", "name; text; expected_version(可选)"),
    _spec("stream.query", "按 name/description/tags 查询流", "name/description/tags(可选)"),
    _spec("stream.read", "读流文本区间 [start,end)", "name; start(可选); end(可选)"),
    _spec("stream.read_lines", "按行读取", "name; start_line(可选); end_line(可选)"),
    _spec("stream.read_rebased", "读最新基线投影(几何)；响应含 clamped(投影越界被钳制到[0,length]置真)", "name; anchor_version; offset; length"),
    _spec("stream.rebase", "重定基线(几何)", "name; 基线参数对齐服务端契约"),
    _spec("stream.remove_tag", "移除流标签", "name; key"),
    _spec("stream.rename", "重命名流", "old_name; new_name"),
    _spec("stream.replace_after", "替换 end 之后全部文本", "name; end; text; expected_version(可选)"),
    _spec("stream.replace_before", "替换 start 之前全部文本", "name; start; text; expected_version(可选)"),
    _spec("stream.replace_lines", "按行号替换", "name; start_line; end_line; text; expected_version(可选)"),
    _spec("stream.replace_range", "按区间替换", "name; start; end; text; expected_version(可选)"),
    _spec("stream.rollback", "回滚到目标版本(几何)", "name; to_version; force(可选)"),
    _spec("stream.search", "全库内容搜索", "pattern; regex(可选)"),
    _spec("stream.search_between", "before/after 外框搜索", "name; before; after; regex(可选)"),
    _spec("stream.search_in_range", "区间内搜索", "name; pattern; start; end; regex(可选)"),
    _spec("stream.search_in_stream", "流内搜索", "name; pattern; context_before/context_after(可选); regex(可选)"),
    _spec("stream.set_tag", "设置流标签", "name; key; value"),
    _spec("stream.stream_append_chunk", "三段式 append chunk", "operation_id; text"),
    _spec("stream.stream_append_end", "三段式 append 收口", "operation_id"),
    _spec("stream.stream_append_start", "三段式 append 起步", "name; position(可选)"),
    _spec("stream.subscribe", "订阅流变更（WS 收编 Hub；历史回放不保留）", "name; from_position(可选)"),
    _spec("stream.undo", "撤销最近 n 次变更", "name; n(默认1)"),
    _spec("stream.unsubscribe", "退订流变更", "subscription_id"),
    _spec("stream.update_description", "更新流描述", "name; text"),
    _spec("stream.update_title", "更新流展示标题", "name; text"),
    _spec("stream.write", "整文覆写（replace 语义）", "name; text; expected_version(可选)"),
    # ── tool.* 23 ───────────────────────────────────────────────────
    _spec("tool.classified.get", "分类内单工具完整定义", "stream_name; name"),
    _spec("tool.classified.get_for_llm", "分类内单工具 LLM 视图", "stream_name; name"),
    _spec("tool.classified.list", "分类内工具列表（完整元数据）", "stream_name; tags(可选)"),
    _spec("tool.classified.list_for_llm", "分类内工具 LLM 列表", "stream_name; tags(可选)"),
    _spec("tool.classified.summarize_for_prompt", "分类内工具摘要(prompt 注入)", "stream_name; tags(可选)"),
    _spec("tool.clear", "清空工具注册表", "（无参）"),
    _spec("tool.format_call", "格式化 @{name|{json}} 标记", "tool_name; params(可选dict)"),
    _spec("tool.get", "获取单工具完整定义（不存在→-33001）", "name(必填)"),
    _spec("tool.get_for_llm", "单个工具 LLM 视图", "name(必填)"),
    _spec("tool.get_manual", "获取工具手册内容", "name(必填)"),
    _spec("tool.get_process_info", "单进程详情（不存在→-33001）", "process_name(必填)"),
    _spec("tool.list", "列出工具（可按 tags 过滤）", "tags(可选)"),
    _spec("tool.list_for_llm", "LLM 视图工具列表", "tags(可选)"),
    _spec("tool.list_processes", "列出后台进程", "tool_name/status(可选)"),
    _spec("tool.parse_calls", "解析文本中的工具调用标记", "text"),
    _spec("tool.register", "注册工具", "name; description; tool_path(必填) + 可选注册字段"),
    _spec("tool.search", "按名称/描述/简要描述搜索工具", "pattern(默认\"\"); tags(可选)"),
    _spec("tool.send_input", "向后台进程发送 stdin", "process_name; data; append_newline(默认True); encoding(默认utf-8)"),
    _spec("tool.stop_all_processes", "停止全部后台进程", "timeout(默认2)"),
    _spec("tool.stop_process", "停止单个后台进程", "process_name; timeout(默认2)"),
    _spec("tool.stop_processes", "批量停止后台进程", "targets(str|list); timeout(默认2)"),
    _spec("tool.summarize_for_prompt", "工具文本摘要(prompt 注入)", "tags(可选)"),
    _spec("tool.unregister", "注销工具（目标不存在→-33001）", "name(必填)"),
    # ── scan.* 10（提级一等域；tool.scan.* 兼容别名已删除）──────────
    _spec("scan.cancel", "撤任务+杀后台子代理+吊销票据(M9)", "stream_name"),
    _spec("scan.list", "列出扫描任务（含 depth/parent_stream 投影）", "（无参）"),
    _spec("scan.move", "移动扫描位置", "stream_name; position"),
    _spec("scan.pause", "暂停扫描（无任务→-33007）", "stream_name"),
    _spec("scan.rescan", "从头重扫：游标归零后重新起扫（idle 流；非幂等）", "stream_name; mode(可选 run|start，默认 run); authority(可选 workspace-write|full，缺省=凭证 ceiling)"),
    _spec("scan.resume", "恢复扫描（无任务→-33007）", "stream_name"),
    _spec("scan.set_authority", "切换扫描当前权限（仅 settled PAUSED；starter/root）", "stream_name; authority(workspace-write|full)"),
    _spec("scan.run", "同步运行扫描直到自然结束", "stream_name; parent_stream/parent_tool(可选); depth(已废弃忽略·H1); authority(可选 workspace-write|full，缺省=凭证 ceiling)"),
    _spec("scan.start", "非阻塞启动扫描（每流至多一个活动任务）", "stream_name; parent_stream/parent_tool(可选); depth(已废弃忽略·H1); authority(可选 workspace-write|full，缺省=凭证 ceiling)"),
    _spec("scan.status", "扫描状态（含 authority/allowed_authorities/can_set_authority；无任务返回空闲快照）", "stream_name"),
    _spec("scan.stop", "停止扫描保游标（幂等）", "stream_name"),
    # ── classify.* 4（C4 真身域）────────────────────────────────────
    _spec("classify.block", "封锁分类（target=streams/tools）", "target; name(可选)"),
    _spec("classify.clear", "清除分类", "target; name(可选)"),
    _spec("classify.get", "查询分类状态", "target; name(可选)"),
    _spec("classify.set", "设置分类谓词", "target; predicate(dict); name(可选)"),
    # ── control.* 7（5 security + 2 恢复控制面）────────────────────
    _spec("control.audit_read", "读取审计记录", "（无参或按服务端契约）"),
    _spec("control.mint", "铸造 scoped 票据", "scope_ceiling 等参数对齐服务端契约"),
    _spec("control.release", "吊销 scoped 票据", "参数对齐服务端契约"),
    _spec("control.rotate_root", "轮换根凭据", "参数对齐服务端契约"),
    _spec("control.trip", "紧急熔断（fail-closed）", "参数对齐服务端契约"),
    # ── 恢复控制面 2（D6 descope；精确键域 ControlRecoveryDomain）─────
    _spec("control.list_checkpoints", "列出流恢复检查点（可恢复窗/留存快照）", "stream(可选)"),
    _spec("control.recover_stream", "恢复流到窗内版本（root/elevated+审计；非幂等）", "stream; to_version; force(可选)"),
    # ── config 1 / system 2 / internal 4 / auth / 订阅 ──────────────
    _spec("config.load", "载入 manifest（dry_run 预演）", "path; dry_run(可选); skip_streams(可选)"),
    _spec("system.info", "统一系统信息（版本/端口/计数）", "（无参）"),
    _spec("system.shutdown", "优雅停机（root 门；先 ack 后进入唯一停机路径，非幂等）", "（无参）"),
    _spec("system.cleanup_snapshots", "清理过期快照（非幂等）", "（无参）"),
    _spec("internal.set", "写入内部条目", "name; data(JSON字符串)"),
    _spec("internal.get", "读取内部条目（不存在→-32011）", "name"),
    _spec("internal.delete", "删除内部条目", "name"),
    _spec("internal.list", "列出内部条目名", "（无参）"),
    _spec("auth", "连接认证（transport 认证门）", "token"),
    _spec("subscribe", "统一订阅 Hub（单主题空间）", "topics(非空list); stream(可选,逐流过滤)"),
    _spec("unsubscribe", "统一退订 Hub", "subscription_id"),
    _spec("notification.subscribe", "订阅别名 → subscribe", "topics; stream(可选)"),
    _spec("notification.unsubscribe", "退订别名 → unsubscribe", "subscription_id"),
)

#: 94 精确键（有序；与 ROUTER_REGISTRY.md 逐键一致）。
METHOD_KEYS: tuple[str, ...] = tuple(s[0] for s in _RPC_SPECS)

#: 薄方法名 → RPC 键（``stream_create`` → ``stream.create``）。
METHOD_NAME_TO_KEY: dict[str, str] = {
    rpc_method_attr(key): key for key in METHOD_KEYS
}

#: (method_name, key, desc, params_doc) —— 生成器消费。
RPC_SPECS: tuple[tuple[str, str, str, str], ...] = tuple(
    (rpc_method_attr(key), key, desc, params) for key, desc, params in _RPC_SPECS
)

# 基数断言：94 精确键、键/方法名双向唯一、幂等集 ⊆ 键集。
assert len(METHOD_KEYS) == 94, f"METHOD_KEYS != 94: {len(METHOD_KEYS)}"
assert len(set(METHOD_KEYS)) == 94, "duplicate RPC keys"
assert len(set(METHOD_NAME_TO_KEY)) == 94, "duplicate method attrs"
assert IDEMPOTENT_METHODS <= set(METHOD_KEYS), (
    f"IDEMPOTENT_METHODS 含非登记键: {sorted(IDEMPOTENT_METHODS - set(METHOD_KEYS))}"
)

#: recover_stream 非幂等（禁入重试词表）——模块级显式断言防误入（D6 §三.4）。
assert "control.recover_stream" not in IDEMPOTENT_METHODS, (
    "control.recover_stream 非幂等，禁入重试词表（恢复操作不得自动重发）"
)
#: list_checkpoints 读类 → 已入幂等词表（防误删守卫）。
assert "control.list_checkpoints" in IDEMPOTENT_METHODS


#: 供 type hints 使用的泛型返回标注（薄方法实际返回 Any）。
#: （各薄方法返回类型 = RPC result 原样；写操作多为 ``{"success": bool, ...}``。）
_ANY: Any = None
