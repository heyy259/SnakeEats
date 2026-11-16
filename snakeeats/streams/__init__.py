"""SnakeEats 流域（streams 子树，T2 交付）。

text-stream 流内核与服务端方法面的统一包形态：
- core：Stream 内核（写原语统一收口 + audit_hook 注入点 + undo 补偿化）、
  DiffChain（≤20 快速 undo 索引）、锁族（RWLock/GlobalLock/
  ExclusiveSessionLock）、registry、snapshot、internal（C1 薄代理）；
- domain：StreamDomain（40 键面 + rebase/read_rebased/rollback 几何 3 键）；
- api：InProcessStreamClient（C2 进程内直连，直连不豁免同一把锁/CAS/journal）；
- system：StreamsSystem 进程内装配体（journal hook 装配期强制）。
"""

from snakeeats.kernel.diff import DiffRecord, fold_to_version
from .core.diff import CompoundRollbackRecord, DiffChain
from .core.locks import (
    AppendSessionHandle,
    ExclusiveSessionLock,
    GlobalLock,
    StreamRWLock,
)
from .core.registry import StreamRegistry
from .core.snapshot import SnapshotManager
from .core.stream import AnchorHolder, Stream, assemble_journal_hook
from .domain import RequestContext, StreamDomain, StreamDomainError
from .api import InProcessStreamClient, InProcessSubscription
from .system import StreamsSystem

__all__ = [
    "AnchorHolder",
    "AppendSessionHandle",
    "CompoundRollbackRecord",
    "DiffChain",
    "DiffRecord",
    "ExclusiveSessionLock",
    "GlobalLock",
    "InProcessStreamClient",
    "InProcessSubscription",
    "RequestContext",
    "SnapshotManager",
    "Stream",
    "StreamDomain",
    "StreamDomainError",
    "StreamRWLock",
    "StreamRegistry",
    "StreamsSystem",
    "assemble_journal_hook",
    "fold_to_version",
]
