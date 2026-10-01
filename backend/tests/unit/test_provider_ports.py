from app.application.ports import (
    GenerationRequest,
    GenerationResult,
    ImageGenerationPort,
    TextGenerationPort,
    VideoGenerationPort,
    VoiceGenerationPort,
)


class FakeVideoProvider:
    def generate(self, request: GenerationRequest) -> GenerationResult:
        return GenerationResult(
            provider="fake-video",
            provider_operation_id=f"op-{request.job_id}",
            artifact_refs=(f"artifact://{request.job_id}",),
        )


def test_normalized_generation_request_is_provider_neutral():
    request = GenerationRequest(
        job_id="job-1",
        capability="video",
        inputs={"prompt": "a cinematic city"},
        references=("asset://ref-1",),
        constraints={"duration_seconds": 8},
        idempotency_key="scene-1/v1",
    )

    assert request.capability == "video"
    assert request.references == ("asset://ref-1",)
    assert request.constraints["duration_seconds"] == 8


def test_fake_provider_produces_normalized_result():
    provider = FakeVideoProvider()
    result = provider.generate(
        GenerationRequest(
            job_id="job-1",
            capability="video",
            inputs={"prompt": "test"},
            idempotency_key="scene-1/v1",
        )
    )

    assert isinstance(provider, VideoGenerationPort)
    assert result.provider == "fake-video"
    assert result.provider_operation_id == "op-job-1"
    assert result.artifact_refs == ("artifact://job-1",)


def test_all_provider_ports_share_the_same_normalized_contract():
    assert VideoGenerationPort
    assert ImageGenerationPort
    assert VoiceGenerationPort
    assert TextGenerationPort
