from __future__ import annotations

from copy import deepcopy

from app.application.work_intent import WorkIntent, WorkIntentRepository


class InMemoryWorkIntentRepository(WorkIntentRepository):
    """Deterministic repository double for transactional work-intent tests."""

    def __init__(self, items: dict[str, WorkIntent] | None = None) -> None:
        self._items = items if items is not None else {}

    def add_if_absent(self, intent: WorkIntent) -> bool:
        existing = self._items.get(intent.intent_key)
        if existing is not None:
            if existing != intent:
                raise ValueError(
                    f"work intent identity reused with different contents: {intent.intent_key}"
                )
            return False
        self._items[intent.intent_key] = deepcopy(intent)
        return True

    def get(self, intent_key: str) -> WorkIntent | None:
        intent = self._items.get(intent_key)
        return deepcopy(intent) if intent is not None else None

    def all(self) -> tuple[WorkIntent, ...]:
        return tuple(deepcopy(self._items[key]) for key in sorted(self._items))
