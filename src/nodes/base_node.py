from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BaseNode:
    node_id: str
    node_url: str
