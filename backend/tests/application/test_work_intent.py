from datetime import datetime, timedelta, timezone

import pytest

from app.application.work_intent import WorkIntent
from app.infrastructure.in_memory_persistence import InMemoryPersistenceTransaction


def poll_intent(generation: int = 1) -> WorkIntent:
    return WorkIntent.provider_poll(
        job_id="job-1",
        provider="provider-a",
        operation_id="operation-9",
        generation=generation,
        due_at=datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)
        + timedelta(seconds=generation),
    )


def test_provider_poll_intent_key_is_stable_for_logical_generation():
    first = poll_intent()
    replay = poll_intent()

    assert first.intent_key == "provider-poll:10:provider-a:11:operation-9:1"
    assert first == replay


def test_provider_poll_intent_key_is_unambiguous_when_ids_contain_separators():
    first = WorkIntent.provider_poll_key("provider:a", "operation", 1)
    second = WorkIntent.provider_poll_key("provider", "a:operation", 1)

    assert first != second


def test_work_intent_repository_is_idempotent_for_same_intent():
    tx = InMemoryPersistenceTransaction()
    intent = poll_intent()

    assert tx.work_intents.add_if_absent(intent) is True
    assert tx.work_intents.add_if_absent(intent) is False
    tx.commit()

    assert tx.work_intents.get(intent.intent_key) == intent


def test_work_intent_identity_cannot_be_reused_for_different_contents():
    tx = InMemoryPersistenceTransaction()
    intent = poll_intent()
    tx.work_intents.add_if_absent(intent)
    conflicting = WorkIntent.provider_poll(
        job_id="job-1",
        provider="provider-a",
        operation_id="operation-9",
        generation=1,
        due_at=intent.due_at + timedelta(seconds=30),
    )

    with pytest.raises(ValueError, match="identity reused"):
        tx.work_intents.add_if_absent(conflicting)


def test_work_intent_is_rolled_back_with_the_enclosing_transaction():
    tx = InMemoryPersistenceTransaction()
    intent = poll_intent()
    tx.work_intents.add_if_absent(intent)
    tx.fail_commit = True

    with pytest.raises(RuntimeError, match="commit failed"):
        tx.commit()
    tx.rollback()

    assert tx.work_intents.get(intent.intent_key) is None


@pytest.mark.parametrize(
    "provider,operation_id,generation",
    [("", "operation-9", 1), ("provider-a", "", 1), ("provider-a", "operation-9", 0)],
)
def test_provider_poll_key_rejects_invalid_identity(provider, operation_id, generation):
    with pytest.raises(ValueError):
        WorkIntent.provider_poll_key(provider, operation_id, generation)
