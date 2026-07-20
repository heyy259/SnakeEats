"""Host 解析器 — 统一 host 地址处理"""

# 标准 host 映射
HOST_ALIASES = {
    "localhost": "127.0.0.1",
    "0.0.0.0": "0.0.0.0",
    "127.0.0.1": "127.0.0.1",
    "::1": "127.0.0.1",  # IPv6 回环 → IPv4
}

DEFAULT_HOST = "127.0.0.1"


def resolve_host(host: str | None = None) -> str:
    """统一 host 解析

    Args:
        host: 原始 host 地址

    Returns:
        解析后的 host 地址

    Examples:
        resolve_host(None) → "127.0.0.1"
        resolve_host("localhost") → "127.0.0.1"
        resolve_host("0.0.0.0") → "0.0.0.0"
        resolve_host("192.168.1.1") → "192.168.1.1"
    """
    if host is None:
        return DEFAULT_HOST

    # 标准化：转小写，去除空白
    normalized = host.strip().lower()

    # 查找别名映射
    if normalized in HOST_ALIASES:
        return HOST_ALIASES[normalized]

    # 其他地址保持原样
    return host


def is_loopback(host: str) -> bool:
    """检查是否为回环地址

    Args:
        host: host 地址

    Returns:
        是否为回环地址
    """
    resolved = resolve_host(host)
    return resolved in ("127.0.0.1", "::1")


def is_wildcard(host: str) -> bool:
    """检查是否为通配符地址

    Args:
        host: host 地址

    Returns:
        是否为通配符地址（0.0.0.0 或 ::）
    """
    resolved = resolve_host(host)
    return resolved in ("0.0.0.0", "::")
