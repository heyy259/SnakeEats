"""Configuration constants for the text-stream MCP server."""

# Default connection parameters
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 9876

# MCP Server identity
SERVER_NAME = "text-stream"
SERVER_VERSION = "0.1.0"

# Resource URI schemes
RESOURCE_STREAM = "text-stream://streams/{name}"
RESOURCE_STREAM_META = "text-stream://streams/{name}/meta"
RESOURCE_SYSTEM_INFO = "text-stream://system/info"
RESOURCE_INTERNAL = "text-stream://internal/{name}"

# Dry-run mode messages
DRY_RUN_WARNING = (
    "No text-stream backend connected. "
    "Set TEXT_STREAM_SYSTEM_ID environment variable to connect to a running system."
)
DRY_RUN_TOOL_PREFIX = "[dry-run] "

# Env var names used by the SDK
ENV_SYSTEM_ID = "TEXT_STREAM_SYSTEM_ID"
ENV_PORT = "TEXT_STREAM_PORT"
ENV_AUTH_TOKEN = "TEXT_STREAM_AUTH_TOKEN"
