"""Launch the zvec server subprocess (py3.12)."""

import json
import subprocess
from pathlib import Path
from typing import Optional

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_ZVEC_PYTHON = _PROJECT_ROOT / ".pixi" / "envs" / "zvec" / "bin" / "python"
_SERVER_SCRIPT = Path(__file__).resolve().parent / "server.py"


def start_server(
    hub_url: str,
    cwd: Optional[str | Path] = None,
    vector_dim: int = 1024,
    *,
    connect_timeout: float = 5.0,
    base_delay: float = 0.5,
    max_delay: float = 8.0,
    jitter: float = 0.1,
) -> subprocess.Popen:
    """Start the zvec server and wait for it to be ready.

    The server connects to the Hub at *hub_url* as a WebSocket client and
    registers itself to handle ``vec.*`` topics.

    Args:
        hub_url: WebSocket URL of the Hub (e.g. ``ws://127.0.0.1:9600``).
        cwd: Working directory for the server. Defaults to project root.
             The .zvec/ collection directory is created relative to this path.
        vector_dim: 向量维度，传入 zvec collection schema。

    Returns:
        A running subprocess.Popen (stdout/stderr still available for
        diagnostics; stdin is not used).
    """
    proc = subprocess.Popen(
        [
            str(_ZVEC_PYTHON),
            str(_SERVER_SCRIPT),
            hub_url,
            str(vector_dim),
            f"--connect-timeout={connect_timeout}",
            f"--base-delay={base_delay}",
            f"--max-delay={max_delay}",
            f"--jitter={jitter}",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(cwd or _PROJECT_ROOT),
        text=True,
    )
    ready_line = proc.stdout.readline()  # type: ignore[union-attr]
    if not ready_line:
        stderr = proc.stderr.read()  # type: ignore[union-attr]
        proc.kill()
        raise RuntimeError(f"zvec server failed to start: {stderr}")
    resp = json.loads(ready_line)
    if not resp.get("ok"):
        proc.kill()
        raise RuntimeError(f"zvec server failed to start: {resp}")
    return proc, resp.get("migrated", False)
