from __future__ import annotations

from contextlib import contextmanager
import fcntl
from pathlib import Path
import time
from typing import Iterator, TextIO

from .errors import PipelineError, TransientPipelineError


@contextmanager
def exclusive_lock(
    path: Path,
    *,
    purpose: str,
    transient_when_busy: bool = False,
    wait_when_busy: bool = False,
    poll_interval_seconds: float = 1.0,
) -> Iterator[None]:
    """Hold an exclusive process lock for the duration of the context."""
    if transient_when_busy and wait_when_busy:
        raise PipelineError(
            "A lock cannot both fail transiently and wait when it is busy."
        )
    if poll_interval_seconds <= 0:
        raise PipelineError("Lock poll interval must be greater than zero.")
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as exc:
                if wait_when_busy:
                    time.sleep(poll_interval_seconds)
                    continue
                error_type = (
                    TransientPipelineError if transient_when_busy else PipelineError
                )
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
def concurrency_slot(
    root: Path,
    *,
    maximum: int,
    wait_when_busy: bool = False,
    poll_interval_seconds: float = 1.0,
) -> Iterator[int]:
    """Acquire one of a bounded number of process-wide execution slots."""
    if maximum <= 0:
        raise PipelineError("Maximum parallel jobs must be greater than zero.")
    if poll_interval_seconds <= 0:
        raise PipelineError("Concurrency slot poll interval must be greater than zero.")
    slot_dir = root / "locks" / "concurrency"
    slot_dir.mkdir(parents=True, exist_ok=True)
    handles: list[TextIO] = [
        (slot_dir / f"slot-{index}.lock").open("a+", encoding="utf-8")
        for index in range(maximum)
    ]
    selected: tuple[int, TextIO] | None = None
    try:
        while selected is None:
            for index, handle in enumerate(handles):
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue
                selected = (index, handle)
                break
            if selected is not None:
                break
            if not wait_when_busy:
                raise TransientPipelineError(
                    f"All {maximum} transfer concurrency slots are currently occupied."
                )
            time.sleep(poll_interval_seconds)
        yield selected[0]
    finally:
        for handle in handles:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            handle.close()
