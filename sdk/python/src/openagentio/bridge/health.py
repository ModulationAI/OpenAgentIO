"""Bridge health snapshot types.

These types are intentionally small and additive: the :class:`Bridge` base class
returns an ``UNKNOWN`` snapshot by default, so handler-style bridges that do not
spawn background tasks do not need to implement anything.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class BridgeHealth(enum.Enum):
    """High-level health state of a Bridge."""

    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass(frozen=True)
class BridgeHealthSnapshot:
    """Immutable snapshot of a Bridge's health at a point in time."""

    health: BridgeHealth = BridgeHealth.UNKNOWN
    message: str = ""
    last_error: BaseException | None = None
    consecutive_failures: int = 0
    restarts_in_window: int = 0
    last_success_at: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def unknown(cls, message: str = "health not reported") -> "BridgeHealthSnapshot":
        """Return the default snapshot for bridges that do not report health."""
        return cls(health=BridgeHealth.UNKNOWN, message=message)

    @classmethod
    def healthy(
        cls, *, message: str = "healthy", last_success_at: float | None = None
    ) -> "BridgeHealthSnapshot":
        """Convenience factory for a healthy snapshot."""
        return cls(
            health=BridgeHealth.HEALTHY,
            message=message,
            last_success_at=last_success_at,
        )

    @classmethod
    def degraded(
        cls,
        *,
        message: str = "degraded",
        last_error: BaseException | None = None,
        consecutive_failures: int = 0,
        restarts_in_window: int = 0,
    ) -> "BridgeHealthSnapshot":
        """Convenience factory for a degraded snapshot."""
        return cls(
            health=BridgeHealth.DEGRADED,
            message=message,
            last_error=last_error,
            consecutive_failures=consecutive_failures,
            restarts_in_window=restarts_in_window,
        )

    @classmethod
    def unhealthy(
        cls,
        *,
        message: str = "unhealthy",
        last_error: BaseException | None = None,
        consecutive_failures: int = 0,
        restarts_in_window: int = 0,
    ) -> "BridgeHealthSnapshot":
        """Convenience factory for an unhealthy snapshot."""
        return cls(
            health=BridgeHealth.UNHEALTHY,
            message=message,
            last_error=last_error,
            consecutive_failures=consecutive_failures,
            restarts_in_window=restarts_in_window,
        )


__all__ = ["BridgeHealth", "BridgeHealthSnapshot"]
