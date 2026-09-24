"""Retry wrapper for Delta concurrent-write conflicts.

The continuous pipeline writes to the same ST while the utility runs DELETE/INSERT.
Delta serializes commits, but a concurrent commit can still raise a transient
conflict; retry with exponential backoff lets it self-heal.
"""
from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")

# Substrings that identify a retryable Delta concurrency conflict.
_RETRYABLE = (
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException",
    "ProtocolChangedException",
    "MetadataChangedException",
    "concurrent update",
    "concurrent transaction",
)


def is_retryable(exc: BaseException) -> bool:
    msg = str(exc)
    return any(tok in msg for tok in _RETRYABLE)


def with_retry(fn: Callable[[], T], attempts: int = 5, backoff_seconds: int = 5,
               logger=None) -> T:
    """Run fn(); on a retryable Delta conflict, retry with exponential backoff."""
    last: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except BaseException as exc:  # noqa: BLE001 - re-raised below if not retryable
            if not is_retryable(exc) or attempt == attempts:
                raise
            last = exc
            wait = backoff_seconds * (2 ** (attempt - 1))
            if logger:
                logger(f"retryable conflict (attempt {attempt}/{attempts}), "
                        f"waiting {wait}s: {exc}")
            time.sleep(wait)
    # unreachable, but keeps type-checkers happy
    raise last  # type: ignore[misc]
