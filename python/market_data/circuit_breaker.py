"""Per-connector circuit breaker. Tracks failures and enforces cooldown windows."""
import time
from dataclasses import dataclass, field

FAIL_DEGRADED = 3     # failures before degraded state
FAIL_DOWN     = 10    # failures before down state
COOLDOWN_DEGRADED = 300    # 5 min
COOLDOWN_DOWN     = 1800   # 30 min


@dataclass
class _State:
    failures: int = 0
    open_until: float = 0.0
    state: str = "ok"          # "ok" | "degraded" | "down"


class CircuitBreaker:
    def __init__(self):
        self._states: dict[str, _State] = {}

    def _get(self, name: str) -> _State:
        if name not in self._states:
            self._states[name] = _State()
        return self._states[name]

    def is_open(self, name: str) -> bool:
        s = self._get(name)
        if s.state == "ok":
            return False
        if time.monotonic() >= s.open_until:
            s.state = "ok"
            s.failures = 0
            return False
        return True

    def record_failure(self, name: str):
        s = self._get(name)
        s.failures += 1
        if s.failures >= FAIL_DOWN:
            s.state = "down"
            s.open_until = time.monotonic() + COOLDOWN_DOWN
        elif s.failures >= FAIL_DEGRADED:
            s.state = "degraded"
            s.open_until = time.monotonic() + COOLDOWN_DEGRADED

    def record_success(self, name: str):
        s = self._get(name)
        s.failures = max(0, s.failures - 1)
        if s.failures < FAIL_DEGRADED:
            s.state = "ok"

    def status(self) -> dict[str, dict]:
        now = time.monotonic()
        return {
            name: {
                "state":     s.state,
                "failures":  s.failures,
                "cooldown_remaining": max(0.0, round(s.open_until - now, 1)),
            }
            for name, s in self._states.items()
        }
