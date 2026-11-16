"""InternalStore — 薄代理到 C1 persistence 的统一 InternalStore。

统一包内只有一份 InternalStore 实现（``snakeeats.persistence.InternalStore``，
纯内存、重启即清、键命名空间 ``_scan_end_*``/``_control_*``
分开【§2.13】）。本模块不再持有独立实现。

单例语义由集成方（S1）在装配时注入同一实例完成——
``StreamRegistry(internal_store=...)`` 接受外部实例。
"""

from snakeeats.persistence import InternalStore

__all__ = ["InternalStore"]
