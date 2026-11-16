"""kernel：概念分层最底端——零依赖共享原语（单向依赖锚点）。

只承载跨域共享的纯原语；不感知 journal/streams/tools/security 任何域。
依赖方向：domain 各域 → kernel，kernel 不反向 import 任何域模块。
"""
