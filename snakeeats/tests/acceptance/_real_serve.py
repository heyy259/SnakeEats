"""Test-only harness for real ``snakeeats serve`` subprocesses.

Why this module exists
----------------------
On Windows + uv, ``sys.executable`` can be a launcher/trampoline: the PID returned
by ``subprocess.Popen`` is the *launcher*, while the interpreter that actually runs
``snakeeats`` is its child. Production code correctly writes ``os.getpid()`` to the
pidfile, so comparing the pidfile against ``Popen.pid`` is wrong by construction.

We therefore spawn ``sys.executable -c <wrapper> serve ...``. The wrapper runs in
the *actual* interpreter process, prints a unique PID banner (flushed immediately),
then imports ``snakeeats.cli.main`` and raises ``SystemExit(main())``. Arguments
after ``-c`` remain CLI arguments. The banner is parsed from the captured log with
a bounded deadline: the discovered PID is independent from the pidfile (reading the
pidfile here would make the PID assertions tautological).

The launcher PID stays available as :attr:`RealServe.launcher_pid` for cleanup only;
:attr:`RealServe.pid` is the actual service PID and raises before discovery.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Mapping

from snakeeats import persistence

#: Unique marker printed by the actual interpreter before importing the CLI.
PID_BANNER_PREFIX = "__SNAKEEATS_TEST_PID__="

#: Bounded waits (seconds) — never wait unboundedly.
DEFAULT_BOOT_DEADLINE_S = 30.0
DEFAULT_EXIT_DEADLINE_S = 30.0
DEFAULT_POLL_INTERVAL_S = 0.05

_DEFAULT_HOST = "127.0.0.1"

#: Runs inside the *actual* interpreter process (a child of any OS launcher).
#: ``sys.argv[1:]`` already holds the CLI args that follow ``-c``.
_WRAPPER_SOURCE = (
    "import os, sys\n"
    f"sys.stdout.write({PID_BANNER_PREFIX!r} + str(os.getpid()) + '\\n')\n"
    "sys.stdout.flush()\n"
    "from snakeeats.cli import main\n"
    "raise SystemExit(main())\n"
)


def tcp_open(port: int, host: str = _DEFAULT_HOST, timeout: float = 0.5) -> bool:
    """Return True if *port* accepts a TCP connection (i.e. is bound)."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@dataclass(frozen=True, slots=True)
class ServeSpec:
    """Immutable description of one ``serve`` subprocess launch."""

    storage: Path
    sid: str
    token: str
    log_path: Path
    cwd: Path
    port: int = 0


class RealServe:
    """Mutable handle to a real ``serve`` subprocess.

    ``pid`` is the actual service process PID discovered from the startup banner,
    never the launcher/trampoline PID returned by ``Popen``.
    """

    __slots__ = ("proc", "spec", "_log_handle", "_actual_pid")

    def __init__(
        self,
        proc: subprocess.Popen[bytes],
        spec: ServeSpec,
        log_handle: IO[str],
    ) -> None:
        self.proc = proc
        self.spec = spec
        self._log_handle = log_handle
        self._actual_pid: int | None = None

    @property
    def storage(self) -> Path:
        return self.spec.storage

    @property
    def sid(self) -> str:
        return self.spec.sid

    @property
    def log_path(self) -> Path:
        return self.spec.log_path

    @property
    def launcher_pid(self) -> int:
        """PID of the ``Popen`` process (launcher/trampoline). Cleanup only."""
        return self.proc.pid

    @property
    def pid(self) -> int:
        """Actual service process PID; raises until it has been discovered."""
        if self._actual_pid is None:
            raise RuntimeError(
                "actual serve PID not discovered yet; "
                "call wait_registry_port() (or wait_actual_pid()) first"
            )
        return self._actual_pid

    def _read_actual_pid(self) -> int | None:
        """Parse the PID banner from the log; None while not yet visible."""
        try:
            text = self.log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return None
        for line in text.splitlines():
            if line.startswith(PID_BANNER_PREFIX):
                try:
                    pid = int(line[len(PID_BANNER_PREFIX):].strip())
                except ValueError:
                    continue
                if pid > 0:
                    return pid
        return None

    def log_text(self) -> str:
        try:
            return self.log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return "<log unavailable>"

    def wait_actual_pid(
        self, timeout: float = DEFAULT_BOOT_DEADLINE_S
    ) -> int:
        """Bounded wait for the actual service PID banner."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            discovered = self._read_actual_pid()
            if discovered is not None:
                self._actual_pid = discovered
                return discovered
            code = self.proc.poll()
            if code is not None:
                raise AssertionError(
                    f"serve exited early with code={code} before PID banner\n"
                    f"{self.log_text()}"
                )
            time.sleep(DEFAULT_POLL_INTERVAL_S)
        raise AssertionError(
            f"actual serve PID not discovered within {timeout}s\n"
            f"{self.log_text()}"
        )

    def wait_registry_port(
        self, timeout: float = DEFAULT_BOOT_DEADLINE_S
    ) -> int:
        """Bounded wait for port readiness *and* actual-PID discovery."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            code = self.proc.poll()
            if code is not None:
                raise AssertionError(
                    f"serve exited early with code={code}\n{self.log_text()}"
                )
            if self._actual_pid is None:
                self._actual_pid = self._read_actual_pid()
            port = persistence.discover_port(self.sid, root=str(self.storage))
            if (
                self._actual_pid is not None
                and port is not None
                and tcp_open(port)
            ):
                return port
            time.sleep(DEFAULT_POLL_INTERVAL_S)
        raise AssertionError(
            f"serve not ready within {timeout}s\n{self.log_text()}"
        )

    def wait_pidfile_equals(
        self, expected_pid: int, timeout: float = DEFAULT_BOOT_DEADLINE_S
    ) -> None:
        """Bounded wait until the sibling pidfile holds *expected_pid*.

        Race background (D9 / W2-7): ``register_port`` runs before
        ``write_pidfile``, so the pidfile may not exist yet when the registry
        port becomes visible — poll instead of asserting immediately.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            code = self.proc.poll()
            if code is not None:
                raise AssertionError(
                    f"serve exited early with code={code}\n{self.log_text()}"
                )
            if (
                persistence.read_pidfile(self.sid, root=str(self.storage))
                == expected_pid
            ):
                return
            time.sleep(DEFAULT_POLL_INTERVAL_S)
        raise AssertionError(
            f"pidfile did not match pid={expected_pid} within {timeout}s\n"
            f"{self.log_text()}"
        )

    def wait_exit(self, timeout: float = DEFAULT_EXIT_DEADLINE_S) -> int:
        """Bounded wait for the subprocess to exit; returns its real exit code."""
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise AssertionError(
                f"serve did not exit within {timeout}s (returncode={exc})\n"
                f"{self.log_text()}"
            ) from exc

    @staticmethod
    def _taskkill_tree(pid: int) -> None:
        """Best-effort Windows process-tree kill; never raises."""
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                check=False,
            )
        except OSError:  # pragma: no cover - taskkill missing is non-fatal
            pass

    def _terminate_tree(self) -> None:
        if os.name == "nt":
            # The actual service is a descendant of the launcher/trampoline.
            self._taskkill_tree(self.proc.pid)
            # If the launcher already exited (e.g. an explicit hard kill), the
            # actual process may be orphaned — kill it too, best effort.
            if (
                self.proc.poll() is not None
                and self._actual_pid is not None
                and self._actual_pid != self.proc.pid
            ):
                self._taskkill_tree(self._actual_pid)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    self.proc.kill()
                except OSError:  # pragma: no cover
                    pass
                try:
                    self.proc.wait(timeout=10)
                except subprocess.TimeoutExpired:  # pragma: no cover
                    pass
        else:
            if self.proc.poll() is None:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
                    try:
                        self.proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:  # pragma: no cover
                        pass

    def cleanup(self) -> None:
        """Kill the whole process tree without masking an in-flight assertion."""
        try:
            self._terminate_tree()
        except Exception:  # noqa: BLE001 - cleanup must not mask assertions
            pass
        finally:
            try:
                self._log_handle.close()
            except Exception:  # noqa: BLE001
                pass


def spawn_serve(spec: ServeSpec, env: Mapping[str, str]) -> RealServe:
    """Launch a real ``serve`` via the PID-banner wrapper (port discovered)."""
    log_handle = open(spec.log_path, "w", encoding="utf-8")
    argv = [
        sys.executable,
        "-c",
        _WRAPPER_SOURCE,
        "serve",
        "--token",
        spec.token,
        "--storage",
        str(spec.storage),
        "--port",
        str(spec.port),
    ]
    try:
        proc = subprocess.Popen(
            argv,
            cwd=str(spec.cwd),
            env=dict(env),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
    except BaseException:
        log_handle.close()
        raise
    return RealServe(proc, spec, log_handle)
