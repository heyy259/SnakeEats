# SnakeEats container image (W2-6, plan D7).
#
# BUILD CONTEXT MUST BE THE REPO ROOT, because the build backend (hatchling) needs
# `snakeeats/` plus `pyproject.toml` to build the wheel. Build with:
#
#   docker build -f deploy/Dockerfile -t snakeeats:local .
#
# (Running it from inside deploy/ would fail: no pyproject.toml there.)
#
# Design intent only, from snakeeats/docs/PACKAGING_PLAN.md 5.3: python:3.12-slim,
# install the package (image content == package), storage at /data. The "现状"
# section of that doc is outdated and is intentionally not followed.

FROM python:3.12-slim

# PYTHONUNBUFFERED: don't lose logs on SIGKILL.
# PYTHONDONTWRITEBYTECODE: the root filesystem is read-only at runtime.
# SNAKEEATS_STORAGE/PORT: writable data volume + stable in-container port.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SNAKEEATS_STORAGE=/data \
    SNAKEEATS_PORT=9876

WORKDIR /app

# Copy only what the wheel build needs. The hatch wheel target packages
# `snakeeats` and excludes tests/examples/docs, so drop those from the payload.
COPY pyproject.toml ./
COPY snakeeats/ ./snakeeats/
RUN rm -rf snakeeats/tests snakeeats/examples snakeeats/docs

# Install the local package so image content == package (no PyPI dependency yet).
RUN pip install --no-cache-dir .

# Non-root runtime user. chown /data inside the image so a named volume mounted
# at /data inherits snakeeats ownership on first mount.
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin snakeeats \
    && install -d -o snakeeats -g snakeeats -m 0750 /data

USER snakeeats

# Informational only; nothing is published unless you map it at run time.
EXPOSE 9876

# Explicit for clarity: SIGTERM is the graceful-stop path (see README shutdown
# contract). The app has no sd_notify and no ExecStop-style second signal.
STOPSIGNAL SIGTERM

# EXEC form, never shell form: shell form makes /bin/sh PID 1 and it would
# swallow SIGTERM instead of forwarding it to the app.
ENTRYPOINT ["python", "-m", "snakeeats"]

# Default command is just `serve`. The app defaults to --host 127.0.0.1, so to be
# reachable from outside the container you MUST pass `--host 0.0.0.0` (compose does
# this). Storage/port come from the ENV above. The admin token is NEVER baked in;
# supply SNAKEEATS_ADMIN_TOKEN at run time.
CMD ["serve"]
