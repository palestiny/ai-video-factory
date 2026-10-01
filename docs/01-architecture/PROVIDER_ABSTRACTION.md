# Provider Abstraction

## Rule

Vendor SDKs belong only in infrastructure adapters.

A provider adapter must:
1. accept the application's normalized request;
2. translate it to the vendor API;
3. normalize the vendor response;
4. expose provider errors as categorized application errors;
5. never leak vendor-specific response types into the domain.

## Capability ports

### Video
Input includes prompt, duration, aspect ratio, optional reference assets, and generation options.

Output includes provider operation identity, produced artifact references, usage metadata, and normalized cost when available.

### Image
Input includes prompt and references.

### Voice
Input includes text, voice identity, language/style options, and output format.

### Text
Input includes task/prompt/context and structured output requirements.

## Why ports exist

Provider replacement should be an adapter change, not a domain rewrite.

Provider-specific features may be exposed through optional capability metadata. A provider-specific feature must not become a mandatory domain assumption without a new architecture decision.
