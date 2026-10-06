"""Executes task handlers with timeout and cancellation."""
from __future__ import annotations

import asyncio
from typing import Any, Optional

from .models import (
    CancellationToken,
    TaskCancelledError,
    TaskDefinition,
    TaskInstance,
)


class AttemptOutcome:
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"


class Executor:
    async def run_attempt(
        self,
        instance: TaskInstance,
        definition: TaskDefinition,
        token: CancellationToken,
    ) -> tuple[str, Any, Optional[str]]:
        """Run one attempt. Returns (outcome, result, error).

        - handler returned            -> (SUCCEEDED, value, None)
        - handler raised              -> (FAILED, None, repr(exc))
        - exceeded definition.timeout -> (TIMED_OUT, None, message)
        - cancelled (cooperative or   -> (CANCELLED, None, None)
          task cancellation)
        """
        coro = definition.handler(instance, token)
        try:
            if definition.timeout is not None:
                result = await asyncio.wait_for(
                    asyncio.ensure_future(coro), timeout=definition.timeout
                )
            else:
                result = await coro
            return AttemptOutcome.SUCCEEDED, result, None
        except TaskCancelledError:
            return AttemptOutcome.CANCELLED, None, None
        except asyncio.CancelledError:
            return AttemptOutcome.CANCELLED, None, None
        except asyncio.TimeoutError:
            return (
                AttemptOutcome.TIMED_OUT,
                None,
                f"attempt {instance.attempt} exceeded timeout of"
                f" {definition.timeout}s",
            )
        except Exception as exc:  # noqa: BLE001 - any handler failure
            return AttemptOutcome.FAILED, None, f"{type(exc).__name__}: {exc}"
