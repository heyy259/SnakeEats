"""rename 位置键迁移（强制补丁 P-3 / §4.7.3 / §6.3）。

**背景**：现码 rename 只搬 Stream 对象与订阅路由，不迁移 InternalStore 中
的 ``_scan_end_{旧名}``；改名后按新名 resume 读到
不存在的键 → 位置归 0 → 静默从头重扫（重复副作用）【卷03 附F-1】。

**处置定稿（§6.3.2 推荐选型 = 迁移，T6 落地）**：rename 事务内把位置键从
旧名**整键迁移**至新名——``_scan_end_{old} → _scan_end_{new}``
（键存取经 C1 InternalStore/统一持久化）。
迁移失败 → **显式报错**（载荷含原因与既有键清单），禁止静默归零、禁止
部分迁移后装作成功。

**与服务端零写 ``_scan_end_*`` 纪律（§4.5.8）的边界**（模块 docstring
登记，§6.6 注记口径）：键整搬属 rename **元数据操作例外**——服务端不
"读-改-写游标内容"，只在 rename 时把既有键从旧名整体搬至新名（值原样，
不解释内容）。游标内容写者仍唯一 = 扫描任务方【D5】。

**锚定游标重锚（F2c Task 10）——本模块对"值原样"的显式、有界破例**：
rename 后 journal hook 以 ``base_version=head`` 重登记新名段（见
``streams/domain.py::_migrate_rename_state``），旧坐标锚 ``anchor_version``
可能落出保留窗口（``anchor_version < base``）→ delta 判 stale-anchor
（§4.5.6）。rename **不改文本**，故 ``offset`` 数值在新 base 坐标下仍有效，
只需把 anchored 游标 ``{anchor_version, offset}`` 的 ``anchor_version``
重写为新 base（offset 不变）。破例严格限界：仅当值呈 anchored 游标形态
``{"anchor_version": ..., "offset": ...}`` 时才重锚（**值形态门**，非键种类
门），且仅在调用方显式传入 ``reanchor_version`` 时；其他形态
的值（如 legacy ``{"end": N}``）自然原样搬移。此破例
不改动游标偏移几何（无第二份投影算术）。

**与 rebase 的关系（§6.3.2）**：rename 之后位置坐标若受并发编辑影响，
重定位按第 4 章 rebase/边界事件语义处理；rename 本身不产生 rebase 特例、
不改写 journal（§4.2.3：version 即指针，rename 不重排 journal）。
"""

from __future__ import annotations

import json
from typing import Any, Protocol

from snakeeats import persistence as _persistence


class _KeyStore(Protocol):
    """C1 InternalStore 键存取面（protocol 鸭子类型，兼容统一实现）。"""

    def get(self, name: str) -> str | None: ...
    def set(self, name: str, json_str: str) -> None: ...
    def delete(self, name: str) -> bool: ...
    def list_names(self) -> list[str]: ...


def position_keys_for(store: _KeyStore, stream: str) -> list[str]:
    """列出某流名下的持久化位置键（``_scan_end_*``）。"""
    scan_key = f"{_persistence.NS_SCAN_END_PREFIX}{stream}"
    return [k for k in (scan_key,) if store.get(k) is not None]


def _reanchor_value(raw: str, version: int) -> str:
    """anchored 游标值重锚到新 base（offset 不变）；其他值原样返回。

    仅识别 ``{"anchor_version": ..., "offset": ...}`` 形态（F2c 扫描游标）：
    - rename 不改文本 → ``offset`` 数值在新 base 坐标下仍有效，只需重写
      ``anchor_version`` 为新 base，避免 ``anchor_version < base`` → stale；
    - 非 JSON / 非 dict / 缺键的 legacy ``{"end": N}`` 或任意配置值 → 原样
      返回（不解释、不迁移）。

    本函数是有意破例点（见模块 docstring「锚定游标重锚」）；只改写版本锚字段，
    不做任何偏移投影算术。
    """
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw
    if isinstance(data, dict) and "anchor_version" in data and "offset" in data:
        data = dict(data)
        data["anchor_version"] = int(version)
        return json.dumps(data)
    return raw


def migrate_position_keys(store: _KeyStore, old_name: str,
                          new_name: str,
                          reanchor_version: int | None = None
                          ) -> dict[str, Any]:
    """rename 事务内把 *old_name* 的位置键整键迁移至 *new_name*。

    - ``_scan_end_{old} → _scan_end_{new}``
      （值原样搬移，不解释游标内容——零写纪律例外）；
    - *reanchor_version* 非 None 时，anchored 游标值
      ``{anchor_version, offset}`` 的 ``anchor_version`` 重写为该值
      （新 journal base=head）、``offset`` 不变（rename 不改文本）；非
      anchored 形态的值（legacy ``{"end": N}`` 等）仍
      原样搬移（缺省 None = 既有语义不变）；
    - 目标新键已存在 → 视为冲突：不覆盖，整次迁移失败显式报错；
    - 任一键迁移失败 → 已搬键回滚（恢复各自原值），返回失败（含原因 + 既有键
      清单），禁止静默归零 / 部分成功；
    - 无键可迁 → 返回 ``{"migrated": [], "conflicts": []}``（正常空迁）。

    返回：``{"migrated": [key...], "conflicts": [key...]}``。
    """
    _scan_key = lambda n: f"{_persistence.NS_SCAN_END_PREFIX}{n}"

    old_keys = {
        "scan": _scan_key(old_name),
    }
    new_keys = {
        "scan": _scan_key(new_name),
    }

    existing_old = {k: v for k, v in old_keys.items() if store.get(v) is not None}
    conflicts = {k: v for k, v in new_keys.items() if store.get(v) is not None}

    migrated: list[str] = []
    if not existing_old and not conflicts:
        return {"migrated": [], "conflicts": []}

    # 冲突 → 整体失败显式报错（不覆盖新名既有键）
    if conflicts:
        from snakeeats import errors as _errors
        _errors.raise_error(
            _errors.INTERNAL_ERROR, _errors.KIND_ALREADY_EXISTS,
            entity={"old_name": old_name, "new_name": new_name},
            expected={},
            actual={"conflicting_keys": list(conflicts.values())},
            hint="rename 目标名已有位置键；先清空/合并目标侧键再 rename，"
                 "或选用显式报错失效路径。",
        )

    # 逐个整键搬移（值原样；anchored 游标按需重锚），任一步失败回滚已搬键
    originals: dict[str, str] = {}
    try:
        for kind, old_key in existing_old.items():
            new_key = new_keys[kind]
            value = store.get(old_key)
            if value is None:
                continue
            originals[old_key] = value
            if reanchor_version is not None:
                value = _reanchor_value(value, reanchor_version)
            store.set(new_key, value)
            store.delete(old_key)
            migrated.append(old_key)
    except Exception as exc:
        # 回滚已搬键（尽力，恢复各自原值而非重锚值）；随后显式报错
        for old_key in migrated:
            new_key = new_keys["scan"]
            value = originals.get(old_key)
            if value is not None and store.get(old_key) is None:
                store.set(old_key, value)
                store.delete(new_key)
        from snakeeats import errors as _errors
        _errors.raise_error(
            _errors.INTERNAL_ERROR, _errors.KIND_FAILED,
            entity={"old_name": old_name, "new_name": new_name},
            expected={},
            actual={"error": f"{type(exc).__name__}: {exc}",
                    "existing_keys": list(existing_old.values())},
            hint="rename 位置键迁移失败，已回滚；确认 InternalStore 可用后重试。",
        )
    return {"migrated": migrated, "conflicts": []}
