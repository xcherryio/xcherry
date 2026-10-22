# Dex Skills bundle baselines

Use these immutable release tags when the loaded Skill needs to inspect the
reference sources packaged with this Dex Skills release. Project dependencies
and lockfiles remain authoritative for application code.

```text
DEX_BASELINE=sdk-go/v1.13.0
DEX_SERVER_BASELINE=server/v1.13.0
DEX_CLI_BASELINE=cli-v1.14.0
DEX_JAVA_BASELINE=sdk-java/v1.14.0
DEX_PYTHON_BASELINE=sdk-python/v1.14.0
DEX_TYPESCRIPT_BASELINE=sdk-typescript/v1.14.0
DEX_RUST_BASELINE=sdk-rust/v1.14.0
```

These values mirror the release metadata at the Dex Skills repository root.
The package validator rejects any drift between the two copies.

The Go SDK and Server baselines remain at v1.13.0. Java, Python, TypeScript,
and Rust use their published v1.14.0 tags; the CLI baseline is cli-v1.14.0.
Each SDK and its language examples use that language's exact tag. **DEX_BASELINE**
is the Go SDK pin, not a shared version for every language.
The Go example dependency manifest selects SDK v1.11.0; the other language
examples select SDK v1.12.0. They are runnable source examples, not the
API-version authority.
Hard-delete Attributes need SDK v1.11.0 and Server v1.11.0, so their
excerpts come from the SDK READMEs and SDK integration tests at the corresponding language tag. Go SDK
v1.5.0 changed default Go Flow and Step type names; see
[Go versioning](../go/versioning.md#default-flow-and-step-type-names).
For a new project, select the released SDK version deliberately. For an
existing project, inspect its installed SDK and lockfile before applying
precise syntax or changing dependencies.
