"""Step-level checkpoint for long-running analysis scripts (11, 12, 12b).

Usage:
    from _checkpoint import done, mark
    if done(step_dir): continue
    ... run step ...
    mark(step_dir)

Override with env var FORCE=1 to recompute all steps, or rm step_XX/.ok for selective rerun.
"""
import os
from pathlib import Path
from datetime import datetime

FORCE = os.environ.get("FORCE") == "1"

def done(step_dir: Path) -> bool:
    """True if step_dir has result.txt + chart.png + .ok sentinel AND not forced."""
    if FORCE:
        return False
    return (
        (step_dir / "result.txt").exists()
        and (step_dir / "chart.png").exists()
        and (step_dir / ".ok").exists()
    )

def mark(step_dir: Path):
    (step_dir / ".ok").write_text(datetime.now().isoformat() + "\n")
