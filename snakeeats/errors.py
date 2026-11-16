"""SnakeEats 统一错误面（M2 单层模型）——错误码/kind 词表唯一真源。

定稿依据：《SnakeEats 统一改造·终版（修订后可执行版）》§2.4 + M2/M3/M4/M7/M11。

- 错误帧 = JSON-RPC ``error{code, message}`` + ``error.data`` 五要素载荷
  （kind / 实体上下文 / expected / actual / hint）【M2】；
- 业务层结果字典保持 ``{success, ...}`` 形态，``success=false`` 时 error 位置
  携带可分辨 kind + 人读文本（kind 与本词表同表）【M2】；
- 码值不承担区分职责，区分由 kind 承担【M2】【M11】；
- 码号不重编号；两段边界 -32001~-32099（系统/流）、-33001~-33099（工具/扫描）
  为唯一分配约束【M11】；
- -33004 激活 = 工具执行失败【M4】；-33005 激活 = malformed kind 码
  （帧/SDK 异常层；流内注解层仍以字符串 kind ``malformed`` 表达，同词表不同
  承载）【M3】；
- 每码必配 (message, hint)，禁止裸码【卷01-E5】。
"""

from __future__ import annotations

from typing import Any, NoReturn

# ── JSON-RPC 2.0 标准码（语义不变）───────────────────────────────────────

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603  # 仅兜底保留，禁止作缺省垃圾桶【§2.4.3】

# ── 系统/流段：-32001 ~ -32099（唯一分配约束【M11】）─────────────────────
# 段内未占码点 -32006/-32008 留作几何/对账族预留点，工具/扫描族不得借用【M11】。

AUTH_REQUIRED = -32001
STREAM_NOT_FOUND = -32002
STREAM_ALREADY_EXISTS = -32003
INVALID_POSITION = -32004
UNDO_LIMIT_EXCEEDED = -32005
STALE_ANCHOR = -32006            # 几何/对账族注册位（第 4 章消费：锚定偏移失效）
STREAM_WRITE_LOCKED = -32007
REBASE_CONFLICT = -32008         # 几何/对账族注册位（第 4 章消费：base_version 对账冲突）
STREAM_APPEND_TIMEOUT = -32009
VERSION_MISMATCH = -32010
INTERNAL_ENTRY_NOT_FOUND = -32011
FORCE_REQUIRED = -32012          # 几何/对账族：rollback 目标之后有外部提交、须显式 force【§4.3.2】
CORRUPTED_JOURNAL = -32013       # journal 族：落盘 journal 损坏（半行/脏行；尾部半行除外——走 reconcile 降级）

# ── 工具/扫描段：-33001 ~ -33099（唯一分配约束【M11】）────────────────────
# 门禁族自 -33008 起逐一登记（classify-denied/depth-exceeded/concurrency-limit 等
# 结果字典门禁拒绝种类；flow-busy 沿用 -33006 名义码）【M11】。

TOOL_NOT_FOUND = -33001          # tool.unregister 目标不存在亦归此码【M7】
TOOL_ALREADY_EXISTS = -33002     # tool.register 目标已存在【M7】
INVALID_TOOL_DEFINITION = -33003  # tool.register 定义无效【M7】
TOOL_EXECUTION_ERROR = -33004    # 激活 = 工具执行失败（预检非法 tool_path/脚本类型/超时/异常退出）【M4】
INVALID_TOOL_CALL_FORMAT = -33005  # 激活 = malformed kind 码（帧/SDK 异常层）【M3】
SCANNER_STATE_ERROR = -33006     # flow-busy 名义码【M11】
SCANNER_NOT_RUNNING = -33007
CLASSIFY_DENIED = -33008         # 门禁族起始【M11】：分类门拒绝
DEPTH_EXCEEDED = -33009          # 门禁族：嵌套深度越限
CONCURRENCY_LIMIT = -33010       # 门禁族：并发扫描数越限

# ── 码段分配约束（唯一真源）──────────────────────────────────────────────

_SYSTEM_BAND = (-32001, -32099)
_TOOL_BAND = (-33001, -33099)
_STANDARD_CODES = frozenset(
    {PARSE_ERROR, INVALID_REQUEST, METHOD_NOT_FOUND, INVALID_PARAMS, INTERNAL_ERROR}
)


def assert_code_in_band(code: int) -> int:
    """校验 *code* 属于标准码或两条分配段之一，返回 code 本身。

    新码分配的唯一约束入口：任何新增错误码必须先过此函数【M11】。
    """
    if code in _STANDARD_CODES:
        return code
    in_system = _SYSTEM_BAND[0] >= code >= _SYSTEM_BAND[1]
    in_tool = _TOOL_BAND[0] >= code >= _TOOL_BAND[1]
    if not (in_system or in_tool):
        raise ValueError(
            f"error code {code} out of allocated bands "
            f"(-32001~-32099 system/stream, -33001~-33099 tool/scan)"
        )
    return code


# ── 统一 kind 词表（帧 error.data / 结果字典 error / 流内注解 同表）────────
# 第 3/5 章引用本表为唯一真源【§2.4.1/§2.4.2】。

KIND_MALFORMED = "malformed"                # 帧/SDK 异常层码位 -33005；注解层同词表字符串【M3】
KIND_SKIPPED = "skipped"
KIND_FAILED = "failed"
KIND_DENIED = "denied"
KIND_FLOW_BUSY = "flow-busy"                # 名义码 -33006
KIND_SERVER_DRAINING = "server-draining"    # 名义码 -33006（与 flow-busy 共码）
KIND_CLASSIFY_DENIED = "classify-denied"    # 码位 -33008
KIND_DEPTH_EXCEEDED = "depth-exceeded"      # 码位 -33009
KIND_CONCURRENCY_LIMIT = "concurrency-limit"  # 码位 -33010
KIND_EXECUTION_FAILED = "execution-failed"  # 码位 -33004
KIND_STALE_ANCHOR = "stale-anchor"          # 码位 -32006
KIND_REBASE_CONFLICT = "rebase-conflict"    # 码位 -32008（base_version 对账冲突）
KIND_FORCE_REQUIRED = "force-required"
KIND_METHOD_NOT_FOUND = "method-not-found"  # 码位 -32601（未注册前缀/方法）
KIND_AUTH_REQUIRED = "auth-required"        # 码位 -32001
KIND_INVALID_PARAMS = "invalid-params"      # 码位 -32602
KIND_CORRUPTED_JOURNAL = "corrupted-journal"  # 码位 -32013
KIND_NOT_FOUND = "not-found"
KIND_ALREADY_EXISTS = "already-exists"
KIND_INVALID_DEFINITION = "invalid-definition"
KIND_INTERNAL = "internal-error"            # 码位 -32603 兜底

#: 全词表（遍历断言用；新增 kind 必须登记于此）
ALL_KINDS: tuple[str, ...] = (
    KIND_MALFORMED,
    KIND_SKIPPED,
    KIND_FAILED,
    KIND_DENIED,
    KIND_FLOW_BUSY,
    KIND_SERVER_DRAINING,
    KIND_CLASSIFY_DENIED,
    KIND_DEPTH_EXCEEDED,
    KIND_CONCURRENCY_LIMIT,
    KIND_EXECUTION_FAILED,
    KIND_STALE_ANCHOR,
    KIND_REBASE_CONFLICT,
    KIND_FORCE_REQUIRED,
    KIND_METHOD_NOT_FOUND,
    KIND_AUTH_REQUIRED,
    KIND_INVALID_PARAMS,
    KIND_CORRUPTED_JOURNAL,
    KIND_NOT_FOUND,
    KIND_ALREADY_EXISTS,
    KIND_INVALID_DEFINITION,
    KIND_INTERNAL,
)


# ── kind → 码（唯一真源：结果字典 / 域归码共用）───────────────────────────
# kind 跨域共享（如 not-found）时按本表既有口径取码：工具/扫描域的结果字典
# 判类集合以本表为准；流域异常则在异常类上直接声明 (code, kind) 配对。

KIND_TO_CODE: dict[str, int] = {
    KIND_NOT_FOUND: TOOL_NOT_FOUND,
    KIND_ALREADY_EXISTS: TOOL_ALREADY_EXISTS,
    KIND_INVALID_DEFINITION: INVALID_TOOL_DEFINITION,
    KIND_EXECUTION_FAILED: TOOL_EXECUTION_ERROR,
    KIND_MALFORMED: INVALID_TOOL_CALL_FORMAT,
    KIND_FLOW_BUSY: SCANNER_STATE_ERROR,
    KIND_CLASSIFY_DENIED: CLASSIFY_DENIED,
    KIND_DEPTH_EXCEEDED: DEPTH_EXCEEDED,
    KIND_CONCURRENCY_LIMIT: CONCURRENCY_LIMIT,
    KIND_INVALID_PARAMS: INVALID_PARAMS,
    KIND_METHOD_NOT_FOUND: METHOD_NOT_FOUND,
}


def code_for_kind(kind: str) -> int:
    """kind → 错误码（未登记 kind 归 INTERNAL_ERROR 兜底，禁止裸码）。"""
    return KIND_TO_CODE.get(kind, INTERNAL_ERROR)


# ── error.data 五要素载荷构造【M2】───────────────────────────────────────


def make_error_data(
    kind: str,
    *,
    entity: dict[str, Any],
    expected: Any = None,
    actual: Any = None,
    hint: str,
) -> dict[str, Any]:
    """构造错误帧 ``error.data`` 五要素载荷。

    - ``kind``：机器可辨失败种类（本模块 KIND_* 词表）；
    - ``entity``：实体上下文（哪个流/工具/扫描/方法/参数；尽力给出对象存在性）；
    - ``expected`` / ``actual``：期望值 / 实际值；
    - ``hint``：修复指引（一句人读）。
    """
    if not isinstance(entity, dict) or not entity:
        raise ValueError("entity must be a non-empty dict (实体上下文必填)")
    if not hint:
        raise ValueError("hint must be a non-empty human-readable string (禁止裸 kind)")
    return {
        "kind": kind,
        "entity": entity,
        "expected": expected,
        "actual": actual,
        "hint": hint,
    }


# ── 每码 (message, hint) 文案表【卷01-E5】【D4】【M11】────────────────────
# message = 一句人读结论（不是 kind 复读、不是裸码）；hint = 修复指引。
# 占位符 {entity} 在构造时由实体上下文填入主对象名（见 describe()）。

CODE_MESSAGES: dict[int, tuple[str, str]] = {
    PARSE_ERROR: (
        "请求不是合法 JSON，无法解析。",
        "检查请求体 JSON 语法后重发。",
    ),
    INVALID_REQUEST: (
        "请求不符合 JSON-RPC 2.0 帧结构。",
        "确认 jsonrpc=\"2.0\"、method/params 字段形态正确。",
    ),
    METHOD_NOT_FOUND: (
        "方法未注册：该前缀或方法在当前统一 Router 中不存在。",
        "核对方法名前缀（stream./tool./scan./task./control./classify./system./internal.）与拼写。",
    ),
    INVALID_PARAMS: (
        "调用参数缺失或类型不符。",
        "按目标方法的参数契约补齐缺失参数。",
    ),
    INTERNAL_ERROR: (
        "服务端内部错误（兜底）。",
        "查看服务端日志定位根因；若可复现请附请求帧上报。",
    ),
    AUTH_REQUIRED: (
        "连接未完成认证。",
        "先发送 auth 请求（params.token），再发业务调用。",
    ),
    STREAM_NOT_FOUND: (
        "目标流不存在。",
        "先用 stream.list 确认流名，或 stream.create 创建。",
    ),
    STREAM_ALREADY_EXISTS: (
        "目标流已存在。",
        "换名创建，或对既有流直接读写。",
    ),
    INVALID_POSITION: (
        "位置/偏移越界或非法。",
        "位置需在 [0, 流长度] 区间内；先读取当前长度再定位。",
    ),
    UNDO_LIMIT_EXCEEDED: (
        "undo 深度超出可回退窗口。",
        "减少回退步数；更早历史经 snapshot/rollback 路径处理。",
    ),
    STALE_ANCHOR: (
        "锚定版本已失效，偏移无法投影。",
        "用最新 version 重新锚定（anchor_version 过期后需重新读取）。",
    ),
    STREAM_WRITE_LOCKED: (
        "流写锁被占用，本次写入未获准。",
        "等待持锁方释放后重试；长事务拆小。",
    ),
    REBASE_CONFLICT: (
        "base_version 对账冲突，写入基线已漂移。",
        "先 read_rebased 取最新基线，再以新 base_version 重发。",
    ),
    STREAM_APPEND_TIMEOUT: (
        "跨 chunk append 超时，未完成部分已回滚。",
        "缩短单次 append 内容或提高超时；确认对端持续供数。",
    ),
    VERSION_MISMATCH: (
        "版本号与预期不一致（CAS 失败）。",
        "重新读取当前 version 后携带重试。",
    ),
    INTERNAL_ENTRY_NOT_FOUND: (
        "内部存储条目不存在。",
        "先用 internal.list 确认键名（含 _scan_end_/_control_ 前缀）。",
    ),
    FORCE_REQUIRED: (
        "rollback 目标版本之后存在非本操作序列提交，须显式 force。",
        "确认后续提交可被回退后带 force=true 重发；或先用 stream.history 审查。",
    ),
    CORRUPTED_JOURNAL: (
        "journal 文件损坏，无法安全载入（尾部半行按未落盘降级，此处为文件中部/文件头损坏）。",
        "该流 journal 需人工核对：文件中部损坏不可静默丢行（会破坏 1:1）；确认快照与 journal 后处置。",
    ),
    TOOL_NOT_FOUND: (
        "目标工具不存在。",
        "用 tool.list 确认工具名；unregister 前确认其已注册。",
    ),
    TOOL_ALREADY_EXISTS: (
        "同名工具已注册。",
        "先 unregister 旧定义再注册，或更换工具名。",
    ),
    INVALID_TOOL_DEFINITION: (
        "工具定义无效。",
        "检查 name/description/tool_path/input_schema 是否符合注册契约。",
    ),
    TOOL_EXECUTION_ERROR: (
        "工具执行失败（预检非法 tool_path/脚本类型/超时/异常退出）。",
        "核对 tool_path 存在且脚本类型受支持；查看该流 `_console`（完整 stdout/stderr）与 `_scan_log`（JSONL 诊断记录）定位退出原因。",
    ),
    INVALID_TOOL_CALL_FORMAT: (
        "工具调用标记格式 malformed。",
        "标记语法为 @{name|{json}}；检查花括号配对与 JSON 合法性。",
    ),
    SCANNER_STATE_ERROR: (
        "扫描器状态冲突（该流已有活动扫描任务）。",
        "每流至多一个活动扫描；先 stop_scan 或改用既有任务。",
    ),
    SCANNER_NOT_RUNNING: (
        "目标流无运行中的扫描任务。",
        "先 start_scan 再执行 pause/resume/move/分类操作。",
    ),
    CLASSIFY_DENIED: (
        "分类门拒绝：目标不在当前分类范围内。",
        "检查 classify.get 当前 state/predicate，或请求分类内目标。",
    ),
    DEPTH_EXCEEDED: (
        "嵌套扫描深度越限。",
        "降低嵌套层数；上限见 safety.max_scan_depth（默认 3）。",
    ),
    CONCURRENCY_LIMIT: (
        "并发扫描数越限。",
        "等待既有扫描结束；上限见 safety.max_concurrent_scans（默认 8）。",
    ),
}

#: 全量已登记码（遍历断言用；新增码必须同时登记 CODE_MESSAGES）
ALL_CODES: tuple[int, ...] = (
    PARSE_ERROR,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    INVALID_PARAMS,
    INTERNAL_ERROR,
    AUTH_REQUIRED,
    STREAM_NOT_FOUND,
    STREAM_ALREADY_EXISTS,
    INVALID_POSITION,
    UNDO_LIMIT_EXCEEDED,
    STALE_ANCHOR,
    STREAM_WRITE_LOCKED,
    REBASE_CONFLICT,
    STREAM_APPEND_TIMEOUT,
    VERSION_MISMATCH,
    INTERNAL_ENTRY_NOT_FOUND,
    FORCE_REQUIRED,
    CORRUPTED_JOURNAL,
    TOOL_NOT_FOUND,
    TOOL_ALREADY_EXISTS,
    INVALID_TOOL_DEFINITION,
    TOOL_EXECUTION_ERROR,
    INVALID_TOOL_CALL_FORMAT,
    SCANNER_STATE_ERROR,
    SCANNER_NOT_RUNNING,
    CLASSIFY_DENIED,
    DEPTH_EXCEEDED,
    CONCURRENCY_LIMIT,
)

for _code in ALL_CODES:
    assert_code_in_band(_code)
    assert _code in CODE_MESSAGES, f"code {_code} missing (message, hint)"


def code_message(code: int) -> tuple[str, str]:
    """取 *code* 的 (message, hint) 文案；未登记码抛 KeyError（禁止裸码）。"""
    return CODE_MESSAGES[code]


# ── 统一 raise 助手：域代码抛出 → jsonrpc 层转帧 ─────────────────────────


class SnakeEatsError(Exception):
    """统一域异常：携带码位 + 五要素载荷，由 jsonrpc 层转成错误帧。

    message 取 CODE_MESSAGES 的人读结论；error.data 由 make_error_data 构造。
    """

    def __init__(
        self,
        code: int,
        kind: str,
        *,
        entity: dict[str, Any],
        expected: Any = None,
        actual: Any = None,
        hint: str | None = None,
        message: str | None = None,
    ):
        assert_code_in_band(code)
        default_message, default_hint = code_message(code)
        self.code = code
        self.kind = kind
        self.data = make_error_data(
            kind,
            entity=entity,
            expected=expected,
            actual=actual,
            hint=hint or default_hint,
        )
        super().__init__(message or default_message)


def raise_error(
    code: int,
    kind: str,
    *,
    entity: dict[str, Any],
    expected: Any = None,
    actual: Any = None,
    hint: str | None = None,
    message: str | None = None,
) -> NoReturn:
    """统一 raise 助手：任何错误帧必经此路径，保证五要素载荷齐全（恒抛）。"""
    raise SnakeEatsError(
        code,
        kind,
        entity=entity,
        expected=expected,
        actual=actual,
        hint=hint,
        message=message,
    )
