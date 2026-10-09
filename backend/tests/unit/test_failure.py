import pytest

from app.domain.failure import Failure, FailureCode


def test_failure_normalizes_known_code_and_marks_retryability():
    failure = Failure.from_code(
        " timeout ",
        "provider did not respond",
        provider=" fake ",
    )

    assert failure.code is FailureCode.TIMEOUT
    assert failure.retryable is True
    assert failure.provider == "fake"


def test_failure_unknown_code_is_safe_and_non_retryable():
    failure = Failure.from_code(
        "vendor-specific-new-error",
        "provider returned an unmapped error",
    )

    assert failure.code is FailureCode.UNKNOWN
    assert failure.retryable is False


def test_non_retryable_failure_is_explicit():
    failure = Failure.from_code(
        "content_rejected",
        "policy rejection",
        provider_operation_id="op-1",
    )

    assert failure.code is FailureCode.CONTENT_REJECTED
    assert failure.retryable is False
    assert failure.provider_operation_id == "op-1"


def test_blank_failure_message_is_rejected():
    with pytest.raises(ValueError):
        Failure(code=FailureCode.UNKNOWN, message="   ")


def test_blank_optional_provider_is_rejected():
    with pytest.raises(ValueError):
        Failure(code=FailureCode.UNKNOWN, message="error", provider="   ")
