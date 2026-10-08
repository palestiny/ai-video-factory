from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class WorkIntent:
    """A durable request for future work, independent of any delivery attempt.

    The intent key identifies the logical work, not a queue delivery. Retrying
    delivery must reuse this identity rather than creating another intent.
    """

    intent_key: str
    job_id: str
    due_at: datetime
    kind: str
    provider: str | None = None
    operation_id: str | None = None
    generation: int | None = None

    def __post_init__(self) -> None:
        for field_name in ("intent_key", "job_id", "kind"):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} cannot be blank")
        if self.due_at.tzinfo is None or self.due_at.utcoffset() is None:
            raise ValueError("due_at must be timezone-aware")
        if self.kind == "PROVIDER_POLL":
            if not self.provider or not self.provider.strip():
                raise ValueError("provider poll intent requires provider")
            if not self.operation_id or not self.operation_id.strip():
                raise ValueError("provider poll intent requires operation_id")
            if self.generation is None or self.generation < 1:
                raise ValueError("provider poll intent generation must be >= 1")
            expected = self.provider_poll_key(
                self.provider, self.operation_id, self.generation
            )
            if self.intent_key != expected:
                raise ValueError("provider poll intent_key does not match its identity")

    @staticmethod
    def provider_poll_key(provider: str, operation_id: str, generation: int) -> str:
        if not provider.strip() or not operation_id.strip():
            raise ValueError("provider and operation_id cannot be blank")
        if generation < 1:
            raise ValueError("generation must be >= 1")
        return f"provider-poll:{provider}:{operation_id}:{generation}"

    @classmethod
    def provider_poll(
        cls,
        *,
        job_id: str,
        provider: str,
        operation_id: str,
        generation: int,
        due_at: datetime,
    ) -> WorkIntent:
        return cls(
            intent_key=cls.provider_poll_key(provider, operation_id, generation),
            job_id=job_id,
            due_at=due_at,
            kind="PROVIDER_POLL",
            provider=provider,
            operation_id=operation_id,
            generation=generation,
        )


class WorkIntentRepository(Protocol):
    """Transactional storage for logical work intents."""

    def add_if_absent(self, intent: WorkIntent) -> bool:
        """Insert once; return False for an identical already-persisted intent.

        Raise ValueError if the same stable identity is reused for different
        intent contents. The insert must participate in the enclosing DB
        transaction with the state transition that requires this work.
        """
        ...

    def get(self, intent_key: str) -> WorkIntent | None:
        ...
