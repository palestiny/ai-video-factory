from app.application.reconciliation import (
    GenerationReconciliationPort,
    ProviderExecutionContract,
    ProviderOperationSafety,
    ReconciliationDecision,
    ReconciliationQuery,
    decide_ambiguous_recovery,
)


def test_idempotent_provider_can_reuse_stable_operation_safely():
    contract = ProviderExecutionContract("fake-video", ProviderOperationSafety.IDEMPOTENT)

    assert contract.can_recover_ambiguity is True
    assert decide_ambiguous_recovery(contract) == ReconciliationDecision(
        safe_to_retry=True,
        requires_reconciliation=False,
        reason="provider guarantees idempotent operation reuse",
    )


def test_reconcilable_provider_requires_lookup_before_retry():
    contract = ProviderExecutionContract("provider-x", ProviderOperationSafety.RECONCILABLE)

    assert contract.can_recover_ambiguity is True
    decision = decide_ambiguous_recovery(contract)
    assert decision.safe_to_retry is False
    assert decision.requires_reconciliation is True


def test_non_reconcilable_provider_must_not_be_treated_as_retry_safe():
    contract = ProviderExecutionContract("provider-y", ProviderOperationSafety.NON_RECONCILABLE)

    decision = decide_ambiguous_recovery(contract)
    assert decision.safe_to_retry is False
    assert decision.requires_reconciliation is True


def test_reconciliation_query_requires_stable_operation_identity():
    query = ReconciliationQuery("scene-1/v1")

    assert query.idempotency_key == "scene-1/v1"


def test_reconciliation_port_is_explicit_protocol():
    assert GenerationReconciliationPort


def test_ambiguous_outcome_carries_recovery_contract_and_lookup_port():
    from app.application.reconciliation import AmbiguousProviderOutcome

    class Lookup:
        def reconcile(self, query):
            return None

    lookup = Lookup()
    contract = ProviderExecutionContract("provider-x", ProviderOperationSafety.RECONCILABLE)
    outcome = AmbiguousProviderOutcome(
        provider="provider-x",
        contract=contract,
        reconciliation=lookup,
    )

    assert outcome.provider == "provider-x"
    assert outcome.contract.operation_safety is ProviderOperationSafety.RECONCILABLE
    assert outcome.reconciliation is lookup
