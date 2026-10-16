# Dex Skills bundle baselines

Use these immutable release tags when the loaded Skill needs to inspect the
reference sources packaged with this Dex Skills release. Project dependencies
and lockfiles remain authoritative for application code.

```text
DEX_BASELINE=sdk-go/v1.10.1
DEX_SERVER_BASELINE=server/v1.10.1
DEX_CLI_BASELINE=cli-v1.10.1
```

These values mirror the release metadata at the Dex Skills repository root.
The package validator rejects any drift between the two copies.

The SDK API baseline is v1.10.1 for Go, Python, TypeScript, Java, and Rust.
The example dependency manifests at this source tag select SDK v1.10.0;
they are runnable source examples, not the API-version authority. Go SDK
v1.5.0 changed default Go Flow and Step type names; see
[Go versioning](../go/versioning.md#default-flow-and-step-type-names).
For a new project, select the released SDK version deliberately. For an
existing project, inspect its installed SDK and lockfile before applying
precise syntax or changing dependencies.
