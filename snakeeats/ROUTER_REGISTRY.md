# SnakeEats Router 方法登记表（S1 总装验收产物）

> 由 `tests/base/test_router_registry.py` 逐键经真实 WS ping 生成（装配 = port 0 + root token + tmp storage；每次请求带最小/空 params）。ping 结果 = 有响应帧（result 或良构 M2 错误帧五要素）；参数校验类错误可接受——任何响应都证明可达性。

## 基数对账（36 + 40 + 4 → S1 终态登记集）

- **旧 tool-registry 36 键面**：含旧 `system.info`。S1 后 `system.info` 并归 StreamDomain（`system.` 前缀归属单一化， 见 docs/history/INTEGRATION_S1.md），剩余工具面 + 收编面 = `tool.*` **23** 精确键（tool.scan.* 兼容别名与 classify 转发位已在阶段 4 删除）。
- **旧 text-stream 40 键面**：`stream.* 33 + system.* 2 + internal.* 4 + auth 1`。S1 别名去重（`stream.stream_insert_start` → `stream.stream_append_start`，登记表只记一行）得 39，再加第 4 章几何 3 键（rebase / read_rebased / rollback）= `stream.*` **35** 精确键；`system.*` **2**、`internal.*` **4**、`auth` **1**。2026-09-21 起 `stream.*` 增 `stream.update_title`（title 展示字段维护）→ `stream.*` **36** 精确键。
- **W1-5 新增**：`system.shutdown`（root 门优雅停机 RPC）→ `system.*` **3** 精确键。
- **4 个旧 transport 拦截点**：auth → 认证门收编； notification.subscribe/unsubscribe → 统一订阅 Hub 别名； stream.subscribe → 统一 Hub（WS 翻译单一路径）。S1 全收编， transport 无 -32601 拦截门。登记表下方对 4 点填处置注记。
- **新增/并归**：control.* **5**、classify.* **4**（T5 真身域）； **D6 descope 恢复控制面** control.list_checkpoints + control.recover_stream（**ControlRecoveryDomain** 独立无前缀 精确键域，Router 精确优先，与 SecurityDomain 的 control. 前缀 零冲突）→ control.* **7**； subscribe/unsubscribe + 别名 **4**（含 notification.* 2）； config.load **1**（双入口 RPC 面）；`task.*` 仅作前缀占位（本域未宣告精确键，记 0）。

**终态**：精确方法键 **94**（= 域 method_map 全量 并集）+ 前缀占位键 **9** = 登记表总键 **103**。

## 前缀占位面（`<prefix>*`；域归属）

| 占位键 | 归属域 |
| --- | --- |
| `auth*` | StreamDomain |
| `classify.*` | SecurityDomain |
| `control.*` | SecurityDomain |
| `internal.*` | StreamDomain |
| `scan.*` | ToolsDomain |
| `stream.*` | StreamDomain |
| `system.*` | StreamDomain |
| `task.*` | ToolsDomain |
| `tool.*` | ToolsDomain |

## 精确方法键逐键 ping 结果

| method | domain | 处置注记（4 拦截点） | ping 结果 |
| --- | --- | --- | --- |
| `auth` | StreamDomain | 收编 → transport 认证门（AuthShell.authenticate_with_principal 绑定 ctx.principal）；Router 另注册精确键 auth 供进程内/auth RPC 面 | responded: error -32001 kind=auth-required |
| `classify.block` | SecurityDomain | - | responded: error -32602 kind=invalid-params |
| `classify.clear` | SecurityDomain | - | responded: error -32602 kind=invalid-params |
| `classify.get` | SecurityDomain | - | responded: error -32602 kind=invalid-params |
| `classify.set` | SecurityDomain | - | responded: error -32602 kind=invalid-params |
| `config.load` | ConfigDomain | - | responded: error -32602 kind=invalid-params |
| `control.audit_read` | SecurityDomain | - | responded: result |
| `control.list_checkpoints` | ControlRecoveryDomain | - | responded: result |
| `control.mint` | SecurityDomain | - | responded: error -32602 kind=invalid-params |
| `control.recover_stream` | ControlRecoveryDomain | - | responded: error -32602 kind=invalid-params |
| `control.release` | SecurityDomain | - | responded: error -32602 kind=invalid-params |
| `control.rotate_root` | SecurityDomain | - | responded: result |
| `control.trip` | SecurityDomain | - | responded: error -32602 kind=invalid-params |
| `internal.delete` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `internal.get` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `internal.list` | StreamDomain | - | responded: result |
| `internal.set` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `notification.subscribe` | SubscriptionDomain | 收编 → 统一订阅 Hub 别名 subscribe（同一代码路径） | responded: error -32602 kind=invalid-params |
| `notification.unsubscribe` | SubscriptionDomain | 收编 → 统一订阅 Hub 别名 unsubscribe（同一代码路径） | responded: error -32602 kind=invalid-params |
| `scan.cancel` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `scan.list` | ToolsDomain | - | responded: result |
| `scan.move` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `scan.pause` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `scan.rescan` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `scan.resume` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `scan.run` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `scan.set_authority` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `scan.start` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `scan.status` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `scan.stop` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `stream.append` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.create` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.delete` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.delete_lines` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.delete_range` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.get` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.get_metadata` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.insert` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.list` | StreamDomain | - | responded: result |
| `stream.prepend` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.query` | StreamDomain | - | responded: result |
| `stream.read` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.read_lines` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.read_rebased` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.rebase` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.remove_tag` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.rename` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.replace_after` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.replace_before` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.replace_lines` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.replace_range` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.rollback` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.search` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.search_between` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.search_in_range` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.search_in_stream` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.set_tag` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.stream_append_chunk` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.stream_append_end` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.stream_append_start` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.subscribe` | StreamDomain | 收编 → WS 经 transport 翻译为 subscribe{topics:[stream.on_change], stream}（assembly 开启 Hub 翻译）；进程内订阅仍走 StreamDomain 引擎，双路径不复活 | responded: error -32602 kind=invalid-params |
| `stream.undo` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.unsubscribe` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.update_description` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.update_title` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `stream.write` | StreamDomain | - | responded: error -32602 kind=invalid-params |
| `subscribe` | SubscriptionDomain | - | responded: error -32602 kind=invalid-params |
| `system.cleanup_snapshots` | StreamDomain | - | responded: result |
| `system.info` | StreamDomain | - | responded: result |
| `system.shutdown` | StreamDomain | - | responded: result |
| `tool.classified.get` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `tool.classified.get_for_llm` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `tool.classified.list` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `tool.classified.list_for_llm` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `tool.classified.summarize_for_prompt` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `tool.clear` | ToolsDomain | - | responded: result |
| `tool.format_call` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `tool.get` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `tool.get_for_llm` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `tool.get_manual` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `tool.get_process_info` | ToolsDomain | - | responded: error -33001 kind=not-found |
| `tool.list` | ToolsDomain | - | responded: result |
| `tool.list_for_llm` | ToolsDomain | - | responded: result |
| `tool.list_processes` | ToolsDomain | - | responded: result |
| `tool.parse_calls` | ToolsDomain | - | responded: result |
| `tool.register` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `tool.search` | ToolsDomain | - | responded: result |
| `tool.send_input` | ToolsDomain | - | responded: result |
| `tool.stop_all_processes` | ToolsDomain | - | responded: result |
| `tool.stop_process` | ToolsDomain | - | responded: result |
| `tool.stop_processes` | ToolsDomain | - | responded: result |
| `tool.summarize_for_prompt` | ToolsDomain | - | responded: result |
| `tool.unregister` | ToolsDomain | - | responded: error -32602 kind=invalid-params |
| `unsubscribe` | SubscriptionDomain | - | responded: error -32602 kind=invalid-params |

