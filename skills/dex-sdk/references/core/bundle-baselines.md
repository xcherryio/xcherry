# Dex Skills bundle baselines

Use these immutable release tags when the loaded Skill needs to inspect the
reference sources packaged with this Dex Skills release. Project dependencies
and lockfiles remain authoritative for application code.

```text
DEX_BASELINE=sdk-go/v1.5.0
DEX_SERVER_BASELINE=server/v1.6.1
DEX_CLI_BASELINE=cli-v1.7.0
```

These values mirror the release metadata at the Dex Skills repository root.
The package validator rejects any drift between the two copies.

The SDK API baseline is Go SDK v1.5.0 and v1.4.0 for Python, TypeScript, Java,
and Rust. The Python, TypeScript, Java, and Rust sources at this tag match their
v1.4.0 releases. The example dependency manifests retained at this source tag
still select SDK v1.2.1, which has the same public error names. They are source
examples, not the API-version authority. Go SDK v1.5.0 changes default Go Flow
and Step type names; see [Go versioning](../go/versioning.md#default-flow-and-step-type-names). For a new project, select the released SDK version deliberately.
For an existing project, inspect its installed SDK and lockfile before applying
precise syntax or changing dependencies.
