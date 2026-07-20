"""RPCHandler — dispatches JSON-RPC 2.0 method calls to registry/stream operations."""

import asyncio
import logging
import uuid
from typing import Any

from text_stream_system.config import SystemConfig
from text_stream_system.core.diff import DiffRecord
from text_stream_system.core.locks import GlobalLock
from text_stream_system.core.registry import StreamRegistry
from text_stream_system.exceptions import (
    AuthenticationError,
    InternalEntryNotFoundError,
    InvalidPositionError,
    StreamAlreadyExistsError,
    StreamAppendTimeoutError,
    StreamNotFoundError,
    StreamWriteLockedError,
    TextStreamError,
    UndoLimitExceededError,
    VersionMismatchError,
)
from text_stream_system.server.auth import AuthManager
from text_stream_system.server.jsonrpc import (
    AUTH_REQUIRED,
    INTERNAL_ENTRY_NOT_FOUND,
    INVALID_PARAMS,
    INVALID_REQUEST,
    INTERNAL_ERROR,
    INVALID_POSITION,
    METHOD_NOT_FOUND,
    PARSE_ERROR,
    STREAM_ALREADY_EXISTS,
    STREAM_APPEND_TIMEOUT,
    STREAM_NOT_FOUND,
    STREAM_WRITE_LOCKED,
    UNDO_LIMIT_EXCEEDED,
    VERSION_MISMATCH,
    JsonRpcError,
    JsonRpcNotification,
    JsonRpcRequest,
    JsonRpcResponse,
)

logger = logging.getLogger(__name__)


logger = logging.getLogger(__name__)


# ── Exception → JSON-RPC error code mapping ────────────────────────────

EXCEPTION_CODE_MAP: dict[type, int] = {
    StreamNotFoundError: STREAM_NOT_FOUND,
    StreamAlreadyExistsError: STREAM_ALREADY_EXISTS,
    InvalidPositionError: INVALID_POSITION,
    UndoLimitExceededError: UNDO_LIMIT_EXCEEDED,
    StreamWriteLockedError: STREAM_WRITE_LOCKED,
    StreamAppendTimeoutError: STREAM_APPEND_TIMEOUT,
    VersionMismatchError: VERSION_MISMATCH,
    AuthenticationError: AUTH_REQUIRED,
    InternalEntryNotFoundError: INTERNAL_ENTRY_NOT_FOUND,
}


class RPCHandler:
    """Maps JSON-RPC method names to registry/stream operations with locking."""

    def __init__(
        self,
        registry: StreamRegistry,
        auth_manager: AuthManager,
        config: SystemConfig,
        start_time: float | None = None,
    ):
        self._registry = registry
        self._auth_manager = auth_manager
        self._config = config
        self._start_time = start_time

        # Pending stream_append operations: {operation_id → StreamAppendOp}
        self._pending_ops: dict[str, _StreamAppendOp] = {}

    # ── Main dispatch ──────────────────────────────────────────────────

    async def dispatch(
        self, request: JsonRpcRequest, transport_ctx: "_TransportContext | None" = None
    ) -> JsonRpcResponse | JsonRpcError:
        """Route a JSON-RPC request to the correct handler method.

        *transport_ctx* provides per-connection state (auth status, subscriptions, ws).
        """
        is_notification = isinstance(request, JsonRpcNotification)
        req_id = None if is_notification else request.id

        method_name = self._resolve_method(request.method)
        if method_name is None:
            if is_notification:
                return None  # notifications: no response to send
            return JsonRpcError(
                id=req_id, code=METHOD_NOT_FOUND,
                message=f"Method not found: {request.method}"
            )

        # Auth check: if auth required and not yet authenticated, only allow auth/login
        if self._auth_manager.requires_auth and transport_ctx is not None:
            if not transport_ctx.authenticated and request.method != "auth":
                if is_notification:
                    return None  # notifications: no response to send
                return JsonRpcError(
                    id=req_id, code=AUTH_REQUIRED,
                    message="Authentication required"
                )

        try:
            return await method_name(self, request, transport_ctx)
        except TextStreamError as exc:
            if is_notification:
                return None
            code = EXCEPTION_CODE_MAP.get(type(exc), INTERNAL_ERROR)
            return JsonRpcError(id=req_id, code=code, message=str(exc))
        except Exception as e:
            logger.error(f"Unhandled exception in {request.method}: {e}", exc_info=True)
            if is_notification:
                return None
            return JsonRpcError(
                id=req_id, code=INTERNAL_ERROR, message="Internal error"
            )

    # ── Method mapping ─────────────────────────────────────────────────

    def _resolve_method(self, method: str):
        return _METHOD_MAP.get(method)

    # ── Auth ───────────────────────────────────────────────────────────

    async def _auth(self, req: JsonRpcRequest, ctx: "_TransportContext") -> JsonRpcResponse:
        params = _require_params(req, ["token"])
        ok = self._auth_manager.authenticate(str(params["token"]))
        if not ok:
            return JsonRpcError(id=req.id, code=AUTH_REQUIRED, message="Invalid token")
        if ctx is not None:
            ctx.authenticated = True
        return JsonRpcResponse(id=req.id, result={"status": "ok", "system_id": self._config.system_id})

    # ── stream.create ──────────────────────────────────────────────────

    async def _stream_create(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name"])
        stream = await self._registry.create(
            name=params["name"],
            description=params.get("description", ""),
            tags=params.get("tags"),
        )
        return JsonRpcResponse(id=req.id, result=stream.get_metadata())

    # ── stream.delete ──────────────────────────────────────────────────

    async def _stream_delete(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name"])
        await self._registry.delete(params["name"])
        return JsonRpcResponse(id=req.id, result={"deleted": True})

    # ── stream.list ────────────────────────────────────────────────────

    async def _stream_list(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        tags = req.params.get("tags") if req.params else None
        streams = await self._registry.list_streams(tags=tags)
        return JsonRpcResponse(id=req.id, result=streams)

    # ── stream.query ───────────────────────────────────────────────────

    async def _stream_query(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, [])
        streams = await self._registry.query(
            name=params.get("name"),
            description=params.get("description"),
            tags=params.get("tags"),
        )
        return JsonRpcResponse(id=req.id, result=streams)

    # ── stream.search ──────────────────────────────────────────────────

    async def _stream_search(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["pattern"])
        streams = await self._registry.search_streams(
            pattern=params["pattern"],
            regex=params.get("regex", False),
        )
        return JsonRpcResponse(id=req.id, result=streams)

    # ── stream.get ─────────────────────────────────────────────────────

    async def _stream_get(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            text = stream.get_text()
            preview = stream.get_text_preview()
        return JsonRpcResponse(id=req.id, result={"text": text, "preview": preview})

    # ── stream.read ────────────────────────────────────────────────────

    async def _stream_read(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            if "start" in params or "end" in params:
                length = stream.get_length()
                start = int(params.get("start", 0))
                end = int(params.get("end", length))
                text = stream.read_range(start, end)
            else:
                text = stream.get_text()
        return JsonRpcResponse(id=req.id, result={"text": text})

    # ── stream.read_lines ──────────────────────────────────────────────

    async def _stream_read_lines(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            result = stream.read_lines(
                start_line=params.get("start_line"),
                end_line=params.get("end_line"),
            )
        return JsonRpcResponse(id=req.id, result=result)

    # ── stream.write ───────────────────────────────────────────────────

    async def _stream_write(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.write(
                text=params["text"],
                expected_version=params.get("expected_version"),
            )
            new_version = stream.version
        await self._notify_subscribers(params["name"], ctx, "set",
                                       stream.get_text(), stream.version)
        return JsonRpcResponse(id=req.id, result={"version": new_version})

    # ── stream.append ──────────────────────────────────────────────────

    async def _stream_append(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            length_before = stream.get_length()
            stream.append(
                text=params["text"],
                expected_version=params.get("expected_version"),
            )
            new_version = stream.version
        await self._notify_subscribers(params["name"], ctx, "append",
                                       stream.get_text(), new_version,
                                       position=length_before,
                                       text=params["text"])
        return JsonRpcResponse(id=req.id, result={"version": new_version})

    # ── stream.prepend ─────────────────────────────────────────────────

    async def _stream_prepend(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.prepend(
                text=params["text"],
                expected_version=params.get("expected_version"),
            )
            new_version = stream.version
        await self._notify_subscribers(params["name"], ctx, "insert",
                                       stream.get_text(), new_version,
                                       position=0, text=params["text"])
        return JsonRpcResponse(id=req.id, result={"version": new_version})

    # ── stream.insert ──────────────────────────────────────────────────

    async def _stream_insert(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "pos", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.insert(
                pos=int(params["pos"]),
                text=params["text"],
                expected_version=params.get("expected_version"),
            )
            new_version = stream.version
        await self._notify_subscribers(params["name"], ctx, "insert",
                                       stream.get_text(), new_version,
                                       position=int(params["pos"]),
                                       text=params["text"])
        return JsonRpcResponse(id=req.id, result={"version": new_version})

    # ── stream.delete_range ────────────────────────────────────────────

    async def _stream_delete_range(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "start", "end"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.delete_range(
                start=int(params["start"]),
                end=int(params["end"]),
                expected_version=params.get("expected_version"),
            )
            new_version = stream.version
        await self._notify_subscribers(params["name"], ctx, "delete",
                                       stream.get_text(), new_version,
                                       position=int(params["start"]),
                                       end=int(params["end"]))
        return JsonRpcResponse(id=req.id, result={"version": new_version})

    # ── stream.replace_range ───────────────────────────────────────────

    async def _stream_replace_range(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "start", "end", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        start_pos = int(params["start"])
        end_pos = int(params["end"])
        async with lock.write_lock():
            old_text = stream.read_range(start_pos, end_pos)
            stream.replace_range(
                start=start_pos,
                end=end_pos,
                text=params["text"],
                expected_version=params.get("expected_version"),
            )
            new_version = stream.version
        await self._notify_subscribers(params["name"], ctx, "replace",
                                        stream.get_text(), new_version,
                                        position=start_pos,
                                        end=end_pos,
                                        text=params["text"],
                                        old_text=old_text)
        return JsonRpcResponse(id=req.id, result={"version": new_version})

    # ── stream.replace_lines ───────────────────────────────────────────

    async def _stream_replace_lines(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "start_line", "end_line", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.replace_lines(
                start_line=int(params["start_line"]),
                end_line=int(params["end_line"]),
                text=params["text"],
                expected_version=params.get("expected_version"),
            )
            new_version = stream.version
        await self._notify_subscribers(params["name"], ctx, "replace",
                                       stream.get_text(), new_version)
        return JsonRpcResponse(id=req.id, result={"version": new_version})

    # ── stream.delete_lines ─────────────────────────────────────────────

    async def _stream_delete_lines(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "start_line", "end_line"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.delete_lines(
                start_line=int(params["start_line"]),
                end_line=int(params["end_line"]),
                expected_version=params.get("expected_version"),
            )
            new_version = stream.version
        await self._notify_subscribers(params["name"], ctx, "delete",
                                        stream.get_text(), new_version)
        return JsonRpcResponse(id=req.id, result={"version": new_version})

    # ── stream.replace_before ──────────────────────────────────────────

    async def _stream_replace_before(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "start", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.replace_before(
                start=int(params["start"]),
                text=params["text"],
                expected_version=params.get("expected_version"),
            )
            new_version = stream.version
        await self._notify_subscribers(params["name"], ctx, "replace",
                                        stream.get_text(), new_version)
        return JsonRpcResponse(id=req.id, result={"version": new_version})

    # ── stream.replace_after ───────────────────────────────────────────

    async def _stream_replace_after(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "end", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.replace_after(
                end=int(params["end"]),
                text=params["text"],
                expected_version=params.get("expected_version"),
            )
            new_version = stream.version
        await self._notify_subscribers(params["name"], ctx, "replace",
                                        stream.get_text(), new_version)
        return JsonRpcResponse(id=req.id, result={"version": new_version})

    # ── stream.undo ────────────────────────────────────────────────────

    async def _stream_undo(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name"])
        stream = self._registry.get(params["name"])
        n = int(params.get("n", 1))
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.undo(n)
            new_version = stream.version
        await self._notify_subscribers(params["name"], ctx, "set",
                                       stream.get_text(), new_version)
        return JsonRpcResponse(id=req.id, result={"version": new_version})

    # ── stream.search_in_stream ────────────────────────────────────────

    async def _stream_search_in_stream(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "pattern"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        ml = self._config.search_match_limit
        async with lock.read_lock():
            if "start" in params or "end" in params:
                length = stream.get_length()
                result = stream.search_in_range(
                    pattern=params["pattern"],
                    start=int(params.get("start", 0)),
                    end=int(params.get("end", length)),
                    regex=params.get("regex", False),
                    match_limit=ml,
                )
            else:
                ctx_before = int(params.get("context_before", 100))
                ctx_after = int(params.get("context_after", 100))
                result = stream.search(
                    target=params["pattern"],
                    context_before=ctx_before,
                    context_after=ctx_after,
                    regex=params.get("regex", False),
                    match_limit=ml,
                )
        return JsonRpcResponse(id=req.id, result=result)

    # ── stream.search_between ───────────────────────────────────────────

    async def _stream_search_between(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "before", "after"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            result = stream.search_between(
                before=params["before"],
                after=params["after"],
                regex=params.get("regex", False),
                match_limit=self._config.search_match_limit,
            )
        return JsonRpcResponse(id=req.id, result=result)

    # ── stream.search_in_range ──────────────────────────────────────────

    async def _stream_search_in_range(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "pattern", "start", "end"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            result = stream.search_in_range(
                pattern=params["pattern"],
                start=int(params["start"]),
                end=int(params["end"]),
                regex=params.get("regex", False),
                match_limit=self._config.search_match_limit,
            )
        return JsonRpcResponse(id=req.id, result=result)

    # ── stream.update_description ───────────────────────────────────────

    async def _stream_update_description(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.description = params["text"]
        return JsonRpcResponse(id=req.id, result={"description": stream.description})

    # ── stream.set_tag ─────────────────────────────────────────────────

    async def _stream_set_tag(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "key", "value"])
        stream = self._registry.get(params["name"])
        # Tag ops are lightweight; read lock is sufficient for metadata
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            stream.set_tag(params["key"], params["value"])
        return JsonRpcResponse(id=req.id, result={"tags": dict(stream.tags)})

    # ── stream.remove_tag ──────────────────────────────────────────────

    async def _stream_remove_tag(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "key"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            stream.remove_tag(params["key"])
        return JsonRpcResponse(id=req.id, result={"tags": dict(stream.tags)})

    # ── stream.get_metadata ────────────────────────────────────────────

    async def _stream_get_metadata(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            meta = stream.get_metadata()
        return JsonRpcResponse(id=req.id, result=meta)

    # ── stream.rename ──────────────────────────────────────────────────

    async def _stream_rename(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["old_name", "new_name"])
        old_name, new_name = params["old_name"], params["new_name"]
        await self._registry.rename(old_name, new_name)
        # 通知 transport 层更新订阅路由
        if ctx is not None and hasattr(ctx, "_transport") and ctx._transport is not None:
            await ctx._transport._handle_stream_rename(old_name, new_name)
        return JsonRpcResponse(id=req.id, result={"renamed": True})

    # ── stream.subscribe ───────────────────────────────────────────────

    async def _stream_subscribe(self, req: JsonRpcRequest, ctx: "_TransportContext") -> JsonRpcResponse:
        if ctx is None or ctx.ws is None:
            return JsonRpcError(
                id=req.id, code=INVALID_REQUEST,
                message="Subscribe requires active WebSocket connection"
            )
        params = _require_params(req, ["name"])
        # Validate stream exists
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])

        sub_id = str(uuid.uuid4())
        # Default to tail (current buffer length) per spec §7.2.1:
        # "subscribe() 不传参数 → from_position = 当前缓冲区长度"
        from_pos = int(params.get("from_position", stream.get_length()))
        async with lock.read_lock():
            text = stream.get_text()
            version = stream.version

        ctx.subscriptions[sub_id] = {
            "stream_name": params["name"],
            "from_position": from_pos,
        }

        return JsonRpcResponse(id=req.id, result={
            "subscription_id": sub_id,
            "text": text[from_pos:],
            "version": version,
        })

    # ── stream.unsubscribe ─────────────────────────────────────────────

    async def _stream_unsubscribe(self, req: JsonRpcRequest, ctx: "_TransportContext") -> JsonRpcResponse:
        params = _require_params(req, ["subscription_id"])
        if ctx is not None:
            ctx.subscriptions.pop(params["subscription_id"], None)
        return JsonRpcResponse(id=req.id, result={"unsubscribed": True})

    # ── stream.stream_append_start ─────────────────────────────────────

    async def _stream_append_start(self, req: JsonRpcRequest, ctx: "_TransportContext") -> JsonRpcResponse:
        params = _require_params(req, ["name"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])

        # CAS version check at start (spec §4.6)
        expected_version = params.get("expected_version")
        if expected_version is not None and expected_version != stream.version:
            return JsonRpcError(
                id=req.id, code=VERSION_MISMATCH,
                message=f"Version mismatch: expected {expected_version}, got {stream.version}"
            )

        # Acquire write lock eagerly
        await lock.acquire_write()

        position = params.get("position")
        is_insert = position is not None
        start_pos = int(position) if is_insert else stream.get_length()

        operation_id = str(uuid.uuid4())
        self._pending_ops[operation_id] = _StreamAppendOp(
            stream_name=params["name"],
            lock=lock,
            stream=stream,
            last_chunk_time=asyncio.get_event_loop().time(),
            start_pos=start_pos,
            is_insert=is_insert,
            expected_version=expected_version,
        )

        return JsonRpcResponse(id=req.id, result={
            "operation_id": operation_id,
            "start_position": start_pos,
        })

    # ── stream.stream_append_chunk ─────────────────────────────────────

    async def _stream_append_chunk(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["operation_id", "text"])
        op_id = params["operation_id"]
        op = self._pending_ops.get(op_id)
        if op is None:
            return JsonRpcError(
                id=req.id, code=INVALID_REQUEST,
                message=f"Unknown or expired operation: {op_id}"
            )

        op.last_chunk_time = asyncio.get_event_loop().time()
        if op.is_insert:
            op.stream.silent_insert(op.current_pos, params["text"])
            op.current_pos += len(params["text"])
        else:
            op.stream.silent_append(text=params["text"])
        op.chunk_count += 1
        return JsonRpcResponse(id=req.id, result={
            "operation_id": op_id,
            "chunk_index": op.chunk_count - 1,
            "new_size": op.stream.get_length(),
        })

    # ── stream.stream_append_end ───────────────────────────────────────

    async def _stream_append_end(self, req: JsonRpcRequest, ctx: "_TransportContext") -> JsonRpcResponse:
        params = _require_params(req, ["operation_id"])
        op_id = params["operation_id"]
        op = self._pending_ops.pop(op_id, None)
        if op is None:
            return JsonRpcError(
                id=req.id, code=INVALID_REQUEST,
                message=f"Unknown or expired operation: {op_id}"
            )

        # Push ONE diff covering all accumulated chunks (spec §4.3.1)
        if op.is_insert:
            end_pos = op.current_pos
        else:
            end_pos = op.stream.get_length()
        accumulated_text = op.stream.read_range(op.start_pos, end_pos)
        if accumulated_text:
            op.stream._diff_chain.push(
                DiffRecord.create_insert(op.start_pos, accumulated_text)
            )
            op.stream._version += 1

        new_version = op.stream.version
        await op.lock.release_write()

        await self._notify_subscribers(op.stream_name, ctx, "set",
                                        op.stream.get_text(), new_version)
        return JsonRpcResponse(id=req.id, result={
            "total_chunks": op.chunk_count,
            "final_size": op.stream.get_length(),
            "version": new_version,
        })

    # ── system.info ────────────────────────────────────────────────────

    async def _system_info(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        import time
        uptime = time.time() - self._start_time if self._start_time else 0
        info = {
            "system_id": self._config.system_id,
            "port": self._config.port,
            "stream_count": len(self._registry._streams),
            "uptime_seconds": round(uptime, 1),
        }
        return JsonRpcResponse(id=req.id, result=info)

    # ── system.cleanup_snapshots ────────────────────────────────────────

    async def _system_cleanup_snapshots(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        """Delete all stale snapshot directories and leftover registry files.

        Preserves the current system's own snapshot directory.
        RPC: system.cleanup_snapshots {}
        """
        from text_stream_system.core.snapshot import SnapshotManager

        result = SnapshotManager.cleanup_all_stale_snapshots(
            snapshot_dir=self._config.snapshot_dir,
            keep_system_id=self._config.system_id,
            also_clean_registry=True,
        )
        return JsonRpcResponse(id=req.id, result=result)

    # ── internal.set ──────────────────────────────────────────────────

    async def _internal_set(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name", "data"])
        self._registry.internal.set(
            name=params["name"],
            json_str=params["data"],
        )
        return JsonRpcResponse(id=req.id, result={"ok": True})

    # ── internal.get ──────────────────────────────────────────────────

    async def _internal_get(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name"])
        data = self._registry.internal.get(params["name"])
        if data is None:
            raise InternalEntryNotFoundError(params["name"])
        return JsonRpcResponse(id=req.id, result={"data": data})

    # ── internal.delete ───────────────────────────────────────────────

    async def _internal_delete(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        params = _require_params(req, ["name"])
        deleted = self._registry.internal.delete(params["name"])
        return JsonRpcResponse(id=req.id, result={"deleted": deleted})

    # ── internal.list ─────────────────────────────────────────────────

    async def _internal_list(self, req: JsonRpcRequest, ctx) -> JsonRpcResponse:
        names = self._registry.internal.list_names()
        return JsonRpcResponse(id=req.id, result={"names": names})

    # ── Helpers ────────────────────────────────────────────────────────

    async def _notify_subscribers(
        self, stream_name: str, ctx: "_TransportContext | None",
        change_type: str, full_text: str, version: int,
        **extra
    ) -> None:
        """Notify the transport layer about changes for subscribers.

        The actual sending is handled by the transport — the handler just
        signals that a change occurred.  ctx is the originating connection.
        """
        if ctx is None or not hasattr(ctx, "_transport"):
            return
        transport = ctx._transport
        if transport is None:
            return
        await transport._notify_stream_change(
            stream_name=stream_name,
            change_type=change_type,
            version=version,
            full_text=full_text,
            **extra,
        )

    # ── Cleanup ────────────────────────────────────────────────────────

    async def cleanup_canceled_ops(self, now: float) -> list[str]:
        """Cancel timed-out stream_append operations; return canceled op IDs."""
        timeout = self._config.stream_append_chunk_timeout_seconds
        canceled = []
        for op_id, op in list(self._pending_ops.items()):
            if now - op.last_chunk_time > timeout:
                await op.lock.release_write()
                del self._pending_ops[op_id]
                canceled.append(op_id)
        return canceled


# ── Internal helper ────────────────────────────────────────────────────


class _StreamAppendOp:
    __slots__ = ("stream_name", "lock", "stream", "last_chunk_time",
                 "start_pos", "current_pos", "chunk_count", "is_insert",
                 "expected_version")

    def __init__(self, stream_name, lock, stream, last_chunk_time,
                 start_pos=0, is_insert=False, expected_version=None):
        self.stream_name = stream_name
        self.lock = lock
        self.stream = stream
        self.last_chunk_time = last_chunk_time
        self.start_pos = start_pos
        self.current_pos = start_pos
        self.chunk_count = 0
        self.is_insert = is_insert
        self.expected_version = expected_version


class _TransportContext:
    """Per-connection state managed by the transport layer."""

    __slots__ = ("authenticated", "ws", "subscriptions", "_transport")

    def __init__(self):
        self.authenticated: bool = False
        self.ws: Any = None
        self.subscriptions: dict[str, dict] = {}
        self._transport: Any = None


# ── Method map ─────────────────────────────────────────────────────────

_METHOD_MAP: dict[str, callable] = {
    "auth": RPCHandler._auth,
    "stream.create": RPCHandler._stream_create,
    "stream.delete": RPCHandler._stream_delete,
    "stream.list": RPCHandler._stream_list,
    "stream.query": RPCHandler._stream_query,
    "stream.search": RPCHandler._stream_search,
    "stream.get": RPCHandler._stream_get,
    "stream.read": RPCHandler._stream_read,
    "stream.read_lines": RPCHandler._stream_read_lines,
    "stream.write": RPCHandler._stream_write,
    "stream.append": RPCHandler._stream_append,
    "stream.prepend": RPCHandler._stream_prepend,
    "stream.insert": RPCHandler._stream_insert,
    "stream.delete_range": RPCHandler._stream_delete_range,
    "stream.replace_range": RPCHandler._stream_replace_range,
    "stream.replace_lines": RPCHandler._stream_replace_lines,
    "stream.delete_lines": RPCHandler._stream_delete_lines,
    "stream.replace_before": RPCHandler._stream_replace_before,
    "stream.replace_after": RPCHandler._stream_replace_after,
    "stream.undo": RPCHandler._stream_undo,
    "stream.search_in_stream": RPCHandler._stream_search_in_stream,
    "stream.search_between": RPCHandler._stream_search_between,
    "stream.search_in_range": RPCHandler._stream_search_in_range,
    "stream.set_tag": RPCHandler._stream_set_tag,
    "stream.remove_tag": RPCHandler._stream_remove_tag,
    "stream.get_metadata": RPCHandler._stream_get_metadata,
    "stream.update_description": RPCHandler._stream_update_description,
    "stream.rename": RPCHandler._stream_rename,
    "stream.subscribe": RPCHandler._stream_subscribe,
    "stream.unsubscribe": RPCHandler._stream_unsubscribe,
    "stream.stream_append_start": RPCHandler._stream_append_start,
    "stream.stream_insert_start": RPCHandler._stream_append_start,
    "stream.stream_append_chunk": RPCHandler._stream_append_chunk,
    "stream.stream_append_end": RPCHandler._stream_append_end,
    "system.info": RPCHandler._system_info,
    "system.cleanup_snapshots": RPCHandler._system_cleanup_snapshots,
    "internal.set": RPCHandler._internal_set,
    "internal.get": RPCHandler._internal_get,
    "internal.delete": RPCHandler._internal_delete,
    "internal.list": RPCHandler._internal_list,
}


# ── Param validation helper ────────────────────────────────────────────


class text_stream_system_exceptions:
    """Re-export for param validation errors."""
    pass


class _ParamValidationError(TextStreamError):
    """Raised when required params are missing — handled by dispatch()."""
    pass


EXCEPTION_CODE_MAP[_ParamValidationError] = INVALID_PARAMS


def _require_params(req: JsonRpcRequest, required: list[str]) -> dict:
    """Validate that *required* params exist in the request; return the params dict.

    Raises _ParamValidationError if any required param is missing.
    """
    params = req.params or {}
    missing = [k for k in required if k not in params]
    if missing:
        raise _ParamValidationError(
            f"Missing required params: {', '.join(missing)}"
        )
    return params
