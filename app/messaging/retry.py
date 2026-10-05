from dataclasses import dataclass

from app.config import Settings


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Exponential backoff: the delay after attempt n is base * multiplier ** (n - 1)."""

    max_attempts: int = 3
    base_delay_seconds: float = 5.0
    multiplier: float = 2.0

    @classmethod
    def from_settings(cls, settings: Settings) -> "RetryPolicy":
        return cls(
            max_attempts=settings.max_attempts,
            base_delay_seconds=settings.retry_base_delay_seconds,
            multiplier=settings.retry_backoff_multiplier,
        )

    def delay_ms_after(self, attempt: int) -> int | None:
        """Delay before the next attempt, or None when `attempt` was the last allowed one."""
        if attempt >= self.max_attempts:
            return None
        return round(self.base_delay_seconds * self.multiplier ** (attempt - 1) * 1000)

    @property
    def delays_ms(self) -> tuple[int, ...]:
        return tuple(
            round(self.base_delay_seconds * self.multiplier ** (attempt - 1) * 1000)
            for attempt in range(1, self.max_attempts)
        )
