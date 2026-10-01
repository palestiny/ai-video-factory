from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from app.domain.generation import normalize_idempotency_key


class IdempotencyConflict(ValueError):
    """Raised when a key is reused for a different logical request."""


class ReservationStatus(str, Enum):
    CREATED = "CREATED"
    EXISTING = "EXISTING"


@dataclass(frozen=True)
class IdempotencyReservation:
    key: str
    request_fingerprint: str
    job_id: str
    status: ReservationStatus


class IdempotencyRepository(Protocol):
    def reserve(
        self,
        *,
        key: str,
        request_fingerprint: str,
        job_id: str,
    ) -> IdempotencyReservation:
        """Atomically reserve a logical request key."""
        ...


def normalize_request_fingerprint(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError("request fingerprint cannot be blank")
    return normalized


@dataclass
class InMemoryIdempotencyRepository:
    """Deterministic test double for the idempotency repository contract."""

    _reservations: dict[str, IdempotencyReservation] | None = None

    def __post_init__(self) -> None:
        if self._reservations is None:
            self._reservations = {}

    def reserve(
        self,
        *,
        key: str,
        request_fingerprint: str,
        job_id: str,
    ) -> IdempotencyReservation:
        normalized_key = normalize_idempotency_key(key)
        normalized_fingerprint = normalize_request_fingerprint(request_fingerprint)
        if not job_id.strip():
            raise ValueError("job_id cannot be blank")

        existing = self._reservations.get(normalized_key)
        if existing is not None:
            if existing.request_fingerprint != normalized_fingerprint:
                raise IdempotencyConflict(
                    "idempotency key is already reserved for a different request"
                )
            return IdempotencyReservation(
                key=existing.key,
                request_fingerprint=existing.request_fingerprint,
                job_id=existing.job_id,
                status=ReservationStatus.EXISTING,
            )

        reservation = IdempotencyReservation(
            key=normalized_key,
            request_fingerprint=normalized_fingerprint,
            job_id=job_id.strip(),
            status=ReservationStatus.CREATED,
        )
        self._reservations[normalized_key] = reservation
        return reservation
