from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol

from app.domain.events import JobEvent


@dataclass(frozen=True)
class GenerationRequest:
    job_id: str
    capability: str
    inputs: Mapping[str, object]
    references: tuple[str, ...] = ()
    constraints: Mapping[str, object] = field(default_factory=dict)
    idempotency_key: str = ""

    def __post_init__(self) -> None:
        if not self.job_id.strip():
            raise ValueError("job_id cannot be blank")
        if not self.capability.strip():
            raise ValueError("capability cannot be blank")
        if not self.idempotency_key.strip():
            raise ValueError("idempotency_key cannot be blank")


@dataclass(frozen=True)
class GenerationResult:
    provider: str
    provider_operation_id: str | None
    artifact_refs: tuple[str, ...]
    usage: Mapping[str, object] = field(default_factory=dict)
    cost: Mapping[str, object] = field(default_factory=dict)
    diagnostics: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.provider.strip():
            raise ValueError("provider cannot be blank")


class VideoGenerationPort(Protocol):
    def generate(self, request: GenerationRequest) -> GenerationResult:
        ...


class ImageGenerationPort(Protocol):
    def generate(self, request: GenerationRequest) -> GenerationResult:
        ...


class VoiceGenerationPort(Protocol):
    def generate(self, request: GenerationRequest) -> GenerationResult:
        ...


class TextGenerationPort(Protocol):
    def generate(self, request: GenerationRequest) -> GenerationResult:
        ...


class JobEventStore(Protocol):
    def append(self, event: JobEvent) -> None:
        """Append an immutable JobEvent without rewriting prior history."""
        ...
