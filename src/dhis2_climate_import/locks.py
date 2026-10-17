from __future__ import annotations

from contextlib import contextmanager
import fcntl
from pathlib import Path
from typing import Iterator, TextIO

from .errors import PipelineError, TransientPipelineError


@contextmanager
def exclusive_lock(
    path: Path,
    *,
    purpose: str,
    transient_when_busy: bool = False,
) -> Iterator[None]:
    """Hold a non-blocking process lock for the duration of the context."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            error_type = TransientPipelineError if transient_when_busy else PipelineError
            raise error_type(
                f"Cannot start {purpose}: lock already held at {path}."
            ) from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"purpose={purpose}\n")
        handle.flush()
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


@contextmanager
def concurrency_slot(root: Path, *, maximum: int) -> Iterator[int]:
    """Acquire one of a bounded number of process-wide execution slots."""
    if maximum <= 0:
        raise PipelineError("Maximum parallel jobs must be greater than zero.")
    slot_dir = root / "locks" / "concurrency"
    slot_dir.mkdir(parents=True, exist_ok=True)
    handles: list[TextIO] = []
    selected: tuple[int, TextIO] | None = None
    try:
        for index in range(maximum):
            handle = (slot_dir / f"slot-{index}.lock").open("a+", encoding="utf-8")
            handles.append(handle)
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                continue
            selected = (index, handle)
            break
        if selected is None:
            raise TransientPipelineError(
                f"All {maximum} transfer concurrency slots are currently occupied."
            )
        yield selected[0]
    finally:
        for handle in handles:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            handle.close()
