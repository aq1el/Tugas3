from __future__ import annotations

from src.nodes.lock_manager import LockManager


def test_shared_and_exclusive_locks() -> None:
    manager = LockManager()
    result = manager._acquire("res", "alice", 1000, "shared")
    assert result["ok"]
    result = manager._acquire("res", "bob", 1000, "shared")
    assert result["ok"]
    result = manager._acquire("res", "carol", 1000, "exclusive")
    assert not result["ok"]


def test_deadlock_detection() -> None:
    manager = LockManager()
    manager._acquire("r1", "a", 1000, "exclusive")
    manager._acquire("r2", "b", 1000, "exclusive")
    manager._acquire("r2", "a", 1000, "exclusive")
    result = manager._acquire("r1", "b", 1000, "exclusive")
    assert result["error"] == "deadlock_detected"
