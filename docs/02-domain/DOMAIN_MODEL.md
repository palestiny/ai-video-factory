# Domain Model

## Content

### VideoProject
Owns a production workspace.

### Video
Represents one produced video and its lifecycle.

### Script
Versioned narrative source for a video.

### Storyboard
Ordered set of scenes derived from a script.

### Scene
Atomic production unit with prompt, duration, visual references, voice content, generated artifacts, and status.

## Generation

### GenerationJob
Logical request to execute one generation capability.

### GenerationAttempt
One concrete external execution attempt.

### GenerationPolicy
Rules for timeout, retryability, maximum attempts, and provider selection.

## Assets

### Asset
Durable reference to a generated or uploaded media object.

### Character
Continuity identity with reusable reference assets.

### VoiceProfile
Reusable voice configuration.

## Rendering

### Render
Composition request and resulting artifact.

### CaptionTrack
Timed caption representation.

## Quality

### QualityCheck
Validation run against a generated/rendered artifact.

### ValidationResult
Individual rule outcome with severity and evidence.

## Operations

### CostRecord
Normalized spend associated with an operation/attempt.

### JobEvent
Append-only operational event for observability and recovery.

### Failure
Normalized failure category, provider information, retryability, and diagnostic context.
