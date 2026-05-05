from __future__ import annotations

from collections import deque
from typing import Deque, Optional, Tuple

import numpy as np

from .utils.helpers import now_ms

try:
    from sklearn.linear_model import LinearRegression
except Exception:  # pragma: no cover
    LinearRegression = None


class LoadPredictor:
    def __init__(self, enabled: bool = True, window: int = 200) -> None:
        self.enabled = enabled
        self.history: Deque[Tuple[int, float]] = deque(maxlen=window)
        self.model = LinearRegression() if LinearRegression else None

    def add_sample(self, value: float) -> None:
        if not self.enabled:
            return
        self.history.append((now_ms(), value))

    def predict(self, steps: int = 1) -> Optional[float]:
        if not self.enabled or len(self.history) < 5:
            return None
        values = [v for _, v in self.history]
        if self.model:
            x = np.arange(len(values)).reshape(-1, 1)
            y = np.array(values)
            self.model.fit(x, y)
            next_idx = len(values) + max(0, steps - 1)
            return float(self.model.predict([[next_idx]])[0])
        return float(sum(values) / len(values))
