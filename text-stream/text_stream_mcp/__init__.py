"""TextStreamSystem MCP Server.

Exposes text-stream system operations as MCP (Model Context Protocol)
tools and resources for AI agents.
"""

from .server import mcp, main

__all__ = ["mcp", "main"]
