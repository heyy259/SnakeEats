"""CLI 日志初始化（Wave-0 G7）。

- :func:`configure_logging` 幂等：``dictConfig`` 全量替换 ``snakeeats`` 包
  logger 的 handlers，重复调用不叠加输出；
- 级别：显式 *level* 参数 > env ``SNAKEEATS_LOG_LEVEL``
  （:func:`snakeeats.env.get_log_level`）> ``INFO``；
- 输出：``snakeeats`` 包 logger → stderr ``StreamHandler``，格式含
  时间/级别/logger 名/消息；propagate 保持默认 True（caplog 等 root
  handler 仍可见；自身 handler 已处理时不会触发 lastResort 重复输出）；
- 脱敏：:class:`RedactionFilter` 按 key 名（``token|auth|credential|secret|
  password``，大小写不敏感）擦除日志消息与 args 中的凭据值：先结构化擦
  Mapping/tuple/list args，再对格式化后的消息做键值对文本擦除（覆盖
  ``token=%s`` 这类「键在格式串、值在 args」形态）。脱敏失败绝不阻断日志。
- 导入期零副作用：本模块只在 :func:`configure_logging` 被调用时才触碰
  logging 配置（模块级仅有常量/正则/类定义）。
"""

from __future__ import annotations

import logging
import logging.config
import re
import threading
from typing import Any

from . import env

LOGGER_NAME = "snakeeats"
REDACTED = "***REDACTED***"

_SENSITIVE_KEY_RE = re.compile(r"token|auth|credential|secret|password", re.IGNORECASE)

_QUOTED_PAIR_RE = re.compile(
    r"(?P<kq>[\"'])(?P<key>[A-Za-z0-9_.\-]+)(?P=kq)"
    r"(?P<sep>\s*[:=]\s*)"
    r"(?P<vq>[\"'])(?P<inner>[^\"']*)(?P=vq)"
)

_KEY_QUOTED_VALUE_RE = re.compile(
    r"(?P<key>[A-Za-z0-9_.\-]+)(?P<sep>\s*[:=]\s*)"
    r"(?P<vq>[\"'])(?P<inner>[^\"']*)(?P=vq)"
)

_BARE_PAIR_RE = re.compile(
    r"(?P<kq>[\"']?)(?P<key>[A-Za-z0-9_.\-]+)(?P=kq)"
    r"(?P<sep>\s*[:=]\s*)(?P<value>[^\s,;)\]}\"']+)"
)

_CONFIG_LOCK = threading.Lock()


def _is_sensitive_key(key: Any) -> bool:
    return bool(_SENSITIVE_KEY_RE.search(str(key)))


def _redact_text(text: str) -> str:
    """擦除消息文本中的敏感键值对（``"token": "x"`` / ``token=x``）。"""

    def _quoted(match: re.Match[str]) -> str:
        if not _is_sensitive_key(match.group("key")):
            return match.group(0)
        return (
            f'{match.group("kq")}{match.group("key")}{match.group("kq")}'
            f'{match.group("sep")}{match.group("vq")}{REDACTED}{match.group("vq")}'
        )

    def _bare(match: re.Match[str]) -> str:
        if not _is_sensitive_key(match.group("key")):
            return match.group(0)
        return (
            f'{match.group("kq")}{match.group("key")}{match.group("kq")}'
            f'{match.group("sep")}{REDACTED}'
        )

    def _key_quoted(match: re.Match[str]) -> str:
        if not _is_sensitive_key(match.group("key")):
            return match.group(0)
        return (
            f'{match.group("key")}{match.group("sep")}'
            f'{match.group("vq")}{REDACTED}{match.group("vq")}'
        )

    text = _QUOTED_PAIR_RE.sub(_quoted, text)
    text = _KEY_QUOTED_VALUE_RE.sub(_key_quoted, text)
    return _BARE_PAIR_RE.sub(_bare, text)


def _redact_value(value: Any) -> Any:
    """递归擦除 args 结构中敏感键对应的值；未知类型原样返回。"""
    if isinstance(value, dict):
        return {
            key: (REDACTED if _is_sensitive_key(key) else _redact_value(item))
            for key, item in value.items()
        }
    if isinstance(value, tuple):
        return tuple(_redact_value(item) for item in value)
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    return value


class RedactionFilter(logging.Filter):
    """按 key 名擦除日志消息/args 中的凭据值（永不抛错、永不丢日志）。"""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if record.args:
                record.args = _redact_value(record.args)
            if not isinstance(record.msg, str):
                return True
            if record.args:
                try:
                    formatted = record.getMessage()
                except Exception:  # noqa: BLE001 —— 格式不匹配交还 logging 自身报错
                    return True
                scrubbed = _redact_text(formatted)
                if scrubbed != formatted:
                    record.msg = scrubbed
                    record.args = ()
            else:
                record.msg = _redact_text(record.msg)
        except Exception:  # noqa: BLE001 —— 脱敏失败绝不阻断日志
            pass
        return True


def _resolve_level(level: str | None) -> int:
    raw = (level or "").strip() or env.get_log_level().strip()
    raw = raw.upper()
    if raw.isdigit():
        return int(raw)
    value = getattr(logging, raw, None)
    return value if isinstance(value, int) else logging.INFO


def configure_logging(level: str | None = None) -> None:
    """初始化 ``snakeeats`` 包日志（幂等；重复调用不叠加 handler）。

    *level* 为 None 时读 :func:`snakeeats.env.get_log_level`；非法级别名
    回落 ``INFO``。输出到 stderr，格式 = 时间 + 级别 + logger 名 + 消息。
    """
    resolved = _resolve_level(level)
    with _CONFIG_LOCK:
        logging.config.dictConfig(
            {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "snakeeats": {
                        "format": "%(asctime)s %(levelname)s %(name)s: %(message)s",
                        "datefmt": "%Y-%m-%d %H:%M:%S",
                    }
                },
                "filters": {"redact": {"()": RedactionFilter}},
                "handlers": {
                    "stderr": {
                        "class": "logging.StreamHandler",
                        "stream": "ext://sys.stderr",
                        "formatter": "snakeeats",
                        "filters": ["redact"],
                    }
                },
                "loggers": {
                    LOGGER_NAME: {
                        "level": resolved,
                        "handlers": ["stderr"],
                    }
                },
            }
        )
