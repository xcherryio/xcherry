# Dex — Codex Instructions

Durable workflow framework: Go server (`server/`), OpenAPI IDL (`protos/`), and
SDKs/samples for Go, Java, and Python. See [README.md](README.md) for the module
map.

## Plan Mode

Every plan must include all three sections below. Use `N/A: <one-line reason>`
only when a section genuinely doesn't apply.

- `## Tests` — list specific scenarios (integration vs E2E, why each)
- `## Documentation` — which module READMEs / `CONTRIBUTING.md` to create or update
- `## UI/UX` — usually `N/A: no in-repo web UI`

### Tests

- Default to **integration tests** (`server/integ/`) and SDK integ suites.
- Do NOT add unit tests unless explicitly asked or the edge case is unreachable
  through integration/E2E paths.
- List specific test scenarios — not just "add tests".

### Documentation

- Product docs live in [`docs/`](docs/) (start at [`docs/README.md`](docs/README.md)).
- Contributor / module docs: update module READMEs or `CONTRIBUTING.md`.
- Application code snippets in `docs/content/` must use `<SdkTabs>` /
  `<SdkSnippet>` so readers can switch Python, Go, Java, TypeScript, and Rust
  when `examples/rust` has the same sample.
  Do not use per-language headings (`### Java`) or stacked fenced blocks.
  `bash` / `text` fences are exempt.
- Product docs should read naturally: plain English, short sentences, no
  chatbot filler or marketing tone. See `.cursor/rules/docs-writing.mdc`.
- In product-doc prose (`docs/content/` and the matching `zh-Hans` pages), do
  not use inline backticks for API names, methods, types, or identifiers. Use
  **bold** instead (for example **WaitFor**, **StepDecision**). Fenced code
  blocks and `bash` / `text` fences are exempt.
- Product docs ship in English and Simplified Chinese (`zh-Hans`); keep both
  locales in sync. See `.cursor/rules/docs-i18n.mdc`.
- Treat every published docs URL as permanent. Before moving, renaming, or
  deleting a page, add direct entries to `docs/redirects.json` for every
  affected locale. Redirect to the closest successor, never an unrelated page
  or the home page. Do not create redirect chains or loops. Update internal
  links to use the destination directly.
- Every docs change must preserve all four search-index integrity gates:
  routes and internal links resolve without accidental 404s; only declared
  legacy URLs redirect; each indexable page has one unique, self-referencing
  canonical URL; and robots directives, noindex/nofollow metadata, internal
  links, and the sitemap keep canonical pages crawlable.
- After any change under `docs/`, run `cd docs && npm run check`. This command
  must typecheck, build both locales, reject broken links, anchors, and duplicate
  routes, and pass the generated-site route, redirect, canonical, robots, and
  sitemap audit. Do not merge docs changes when it fails.
- Application snippets in product docs must be copied from runnable files under
  `examples/`. Link the example (and the examples playground when the sample has
  HTTP and is catalogued). Do not invent APIs; add the example first.
  See `.cursor/rules/docs-examples.mdc`.
- Whenever runnable code for a documented design pattern or product example is
  added or changed, run `cd docs && npm run generate:flow-definitions`. Review
  and commit the affected JSON under `docs/src/data/flow-definitions/` in the
  same change, even when the MDX import is unchanged.
- Design-pattern Step graph PNGs must remain responsive. Set only a percentage
  `max-width`, never a fixed width or height. Choose each maximum so its Flow type
  name renders at the same size as `DrainInternalChannelFlow` in
  `drain-internal-channel.png`, and keep the image centered. Use an HTML `img`
  with its `/img/design-patterns/` path so Docusaurus cannot inline the PNG as a
  data URL, then match its stable filename fragment in `docs/src/css/custom.css`.
  Apply this rule when adding or replacing a Step graph.

### UI/UX

- No in-repo web UI. Do not invent Temporal Web work unless the change adds a UI.

## Code Quality Rules

### Commit Every Changing Turn

End every agent turn that changes repository files with one commit on the
current branch. Leave a clean working tree. Do not create empty commits for
discussion-only turns.

Never add Cursor as Author, Committer, or `Co-authored-by`. Cursor's commit
wrapper often injects `cursoragent@cursor.com`; strip it before pushing. After
every commit run `git log -1 --format=%B`. Do not use `--no-verify`. See
`.cursor/rules/git-commit.mdc`.

### Branch From Latest Main

Before creating a feature branch, fetch and branch from `origin/main`. See
`.cursor/rules/git-branch.mdc`. Cursor and Codex hooks enforce this workflow
(`script/branch_off_main_policy.py`; Codex requires `features.codex_hooks =
true` in `~/.codex/config.toml`).

### Agent Rule Synchronization

Keep the Cursor (`.cursor/rules/`), Codex (`AGENTS.md`), and Claude
(`CLAUDE.md`) coding-agent rules synchronized. Any addition or modification to
one requires equivalent updates to all three in the same commit.

### Application-Facing Changes Require Skill Updates

When adding, removing, renaming, or materially changing an application-visible
API, primitive, feature, design pattern, or operational semantic, update
Dex `skills/` in the same work item. Merging `skills/` to main automatically
syncs it to the read-only `superdurable/dex-skills` mirror, which publishes the
skill release. Pure internal
refactors and fixes that preserve application behavior do not trigger this rule.

Commit Dex first. After that change is included in a published Dex release,
advance `skills/DEX_BASELINE` to that exact release tag,
refresh pinned source links, update affected core and all five language
references, and update the version, changelog, and plugin manifests. Never use
a branch name or commit SHA for `DEX_BASELINE` or pinned source links. Run every
skill validator before completion.

If no release tag contains the Dex change yet, report the release as the
blocker. Do not update `skills/` from an unreleased commit.

If the skills mirror is unavailable or contains work that cannot be safely
synced, do not mark the feature complete. Report the blocker explicitly.

### No Downstream Product Information

Dex is an open-source product. Repository files, docs, examples, receipts,
comments, and commit messages contain no information about products built on
Dex: no product names, deployments, environments, dependency pins, or platform
concepts. Describe a requirement in Dex terms, as any Dex application would
need it.

The docs site shell (navbar and footer in `docs/docusaurus.config.ts` and their
zh-Hans translations) may link to company product pages. Content pages must
still not describe downstream products.

### Temporal Skill Routing

- In this repository, use the `temporal-developer` skill only when changing or
  diagnosing `server/service/interpreter/**` requires Temporal runtime or SDK
  semantics.
- Do not use it for other modules, SDKs, examples, docs, routine integration
  tests, or merely because Temporal, workflow, signal, query, worker,
  heartbeat, or `temporalIntegTests` is mentioned.
- An explicit user request to use `temporal-developer` overrides this
  restriction.

### Dex SDK Skill Routing

- Before changing or reviewing `examples/**`, application code snippets in
  product docs, or documented Flow application patterns, load the installed
  `dex-sdk` skill through the host's native skill mechanism and follow its
  routed references. Use the invocation syntax for the host's Plugin or
  standalone installation.
- Do not load it for pure Server, IDL, or SDK implementation work. When a
  change includes both an SDK API and examples, current source and API decisions
  are authoritative; use the skill for application modeling guidance.
- If the skill is unavailable, stop the examples or snippet portion of the
  change and provide the installation instructions at
  https://docs.superdurable.io/build-with-ai/dex-developer-skill.

### User-Facing Concept Boundary

User-facing docs, examples and skills describe Dex concepts and guarantees only.
Do not mention Continue-As-New except in the Flow options reference for the
Continue-As-New threshold and the TriggerContinueAsNew Client API reference. Do
not describe Temporal or Cadence internals (activity timeout names, Signals,
Queries, Updates, workflow history mechanics) as part of the application model;
state the Dex behavior instead. Deployment and operations guidance may name the
backend and its configuration.

### Regenerate the Entire Repository After Proto Changes

Whenever any `.proto` file changes, run `make generated-code` from the repository
root and commit every resulting change. Proto-changing PRs must refresh all
checked-in generated code across the server and SDKs. Do not use component-only
codegen targets for these PRs; they leave stale outputs for the next PR.

### Preserve Protobuf Compatibility

Dex is in Beta. Preserve wire compatibility and decoding of persisted payloads.
Never renumber existing fields or reuse any previously used field number or name.
When deleting a field, reserve both its original number and name. Preserve the
numbers and names of remaining fields; gaps are intentional. Apply the same rule
to deleted enum values. Do not change existing field types, cardinality, or
oneof membership incompatibly. Add new fields with unused numbers, then
regenerate the entire repository.

### License Headers

Every new or edited `.go` / `.java` / `.py` / `.rs` / `.proto` file, Web
`.ts` / `.tsx` / `.css` / `.html` source, and hand-written OpenAPI YAML under
`protos/` must start with the classification header from
[`script/licenseheaders/`](script/licenseheaders/). The committed
legacy manifest determines whether a managed file is `legacy-only`, `mixed`,
or `new`. Examples retain their existing MIT or Apache-2.0 headers.

Skip generated trees: `**/gen/**`, `*.pb.go`, `*_pb.go`, `*.gen.*`.

When creating or modifying such a file, check the top; if the header is missing,
add it. From the repo root:

- `make copyright` — safely add or upgrade required headers
- `make copyright-check` — verify classifications, body hashes, and headers

Do not replace legacy headers. Editing a `legacy-only` file upgrades it to
`mixed`; files first created after the cutoff use the `new` header.

### Beta Compatibility

Dex is in Beta. Preserve published APIs, SDK source compatibility, configuration
files, wire formats, and persisted data. Do not remove or rename published
interfaces, change defaults or behavior incompatibly, or reject previously valid
configuration without explicit user authorization for that specific breaking
change. Prefer additive changes and deprecation with a migration path. Keep
compatibility code while supported releases or persisted data require it.
Before an authorized breaking change, explain its scope and migration, and add
compatibility tests. General permission to refactor or fix a bug does not
authorize a breaking change. Protobuf field numbers and names must still never
be reused.

### Rust Workflow Schema Definitions

In Rust examples and SDK tests under `sdk-rust/crates/dex-sdk/tests/integ/`,
define every Attribute, Channel, and Stream as a module-level `static
LazyLock<T>`. Do not use a function to construct or return one of these
definitions. Reuse the static directly, or clone its initialized value only
when an owned field is required.

### FDG 2.0 Step Explanations

Every Step type in a Flow Definition Graph 2.0 Go source must declare exactly
one `// dex:explanation text:"..."` directive next to its `dex:group`. The text
is one sentence that states what the Step does. The analyzer stores it on the
Step node as `metadata.explanation` for Dex Web v2 Definition.

### Python Examples

Do not use `del` in Python examples merely to mark parameters or local values
as unused. Keep required callback parameters unused instead.

### RPC Names Start With Verbs

Every application-example RPC handler and explicit RPC name must begin with a
concrete action verb. Use verbs such as `get`, `list`, or `describe` for reads
and domain actions such as `send`, `update`, `delete`, or `move` for writes. Do
not use noun-only names such as `queuedMessages`, `history`, or `snapshot`, and
do not use placeholders such as `exampleRPC`.

### Prefer Complete, Precise Names

For every API, interface, class, type, method, function, field, variable, and
constant, prefer a complete and precise domain name over a short name. The name
must communicate its operation and subject at the call site or registration
boundary. Do not use generic names such as `Get`, `Update`, `Manager`, `Data`,
or `Handler` when a specific domain name is available. Brevity is not a goal;
remove words only when the remaining name is equally precise and unambiguous.

### Go Flow and Step Type Names

The Go SDK registers a Flow or Step by its Go type name without the package.
Name every Go Flow type after its domain, such as `ApprovalFlow` or
`ReplyFlow`; never name it just `Flow`. Flow types must be unique within one
Registry, and Step types must be unique within one Flow. A generic Flow or Step
type must implement `GetFlowType` or `GetStepType`. Do not add an override that
returns the default name.

### Fluent SDK Call Sites

Design SDK APIs so application code reads like a natural phrase in its host
language. Judge names and ownership at the call site, including nested calls.
Prefer domain-noun factories such as
`Wait.allOf(Timer.byDuration(duration))` over awkward ownership such as
`Wait.allOf(Wait.timer(duration))`. Preserve each language's idioms rather than
forcing identical syntax across SDKs.

### No Setter Injection

Constructor injection only. Never add `SetXyz`, `Inject*`, `Wire*`, or exported
mutable fields for wiring components after `New*()` returns. Fix bootstrap
ordering instead.

### Inject Config Sections by Pointer, Not Individual Fields

When a component needs tunables from a config section, pass a pointer to that
whole section into its constructor and read fields off it — do NOT thread
individual fields as separate constructor params.

- Store an unexported `cfg *config.XyzConfig`; read `h.cfg.SomeKnob` where used.
  Panic in the constructor if nil.
- Pass the **section**, not the whole `config.Config`, and by **pointer**, not
  value.

### No Stateful Closures — Use Methods on Structs

A closure that captures 3+ outer variables, mutates outer state, is called from
more than one site, or outlives a single statement → lift it into a method on a
struct with explicit fields.

Fine: one-shot callbacks (`sort.Slice`, `errgroup.Go`, `defer`), tiny pure
transforms, IIFEs for scoping.

### No Obvious Comments — New Comments Only

For comments you add, write the fewest needed. Do not restate what the code or a
well-named identifier already says. Explain only a non-obvious *why*. When in
doubt, improve the name instead.

### Short New Comments — Under 20 Words

Every new comment block you add (a contiguous group of `//` lines) must be fewer
than 20 words. If a longer new comment is necessary, ask the user first.

An explicit user request for a paragraph, detailed explanation, or longer
comment authorizes exceeding this limit without asking again.

These rules do not apply to comments that existed before the current change.
Never edit an existing comment merely to simplify, shorten, rephrase, fix
grammar, or satisfy the word limit.

The new-comment simplification and 20-word rules also do not apply to
`server/config/`. Configuration comments should favor complete operational
semantics.

Comments documenting function or method return values are also exempt from the
20-word limit and should describe complete return semantics.

### Detailed Public SDK API Documentation

Public SDK API documentation is exempt from the short-comment and
no-obvious-comment rules. Document every public or exported SDK type,
interface, class, method, function, constructor, field, constant, enum value,
struct member, annotation element, and equivalent language construct. Do not
add API documentation to non-public or generated declarations.

Keep documentation direct. Put one idea in each sentence. Split behavior,
constraints, and rationale into separate sentences.

Write each API document from the application developer's perspective. Start
with a one-sentence summary, then explain when and how the API is used. Give a
complete language-native example for each related API family; overloads and
simple accessors may refer to their enclosing type's example instead of
repeating it. Explain defaults, units, lifecycle, blocking or concurrency
behavior, nullability, ownership, side effects, errors, and other tricky
semantics whenever users must understand them. Finish with language-native
documentation for every type parameter, input, output, and user-visible error.
Do not impose a word limit; prefer a complete paragraph over a terse fragment.
Use the exact nouns exposed by the API or protocol. Do not invent one umbrella
term for distinct identifiers; say Flow type, Step type, RPC name, Attribute
name, Channel name, or map instance as appropriate.
When a documented limitation has a practical workaround, explain it immediately
and include a language-native example when the solution is not obvious.

### Preserve Comments During Refactoring

Existing comments are user-owned text. Preserve them verbatim and move them with
the code they describe. If a code change makes a comment factually stale, change
only the outdated references while retaining every original detail and meaning.
Never delete, shorten, or rewrite an existing comment for style.

### Top-Down File Ordering (Go files)

In the same file, a callee always appears **below** its caller. High-level
orchestration at the top, leaf helpers at the bottom. Preferred order for a
struct-based file:

1. Type declaration
2. Constructor (`new<Type>`)
3. Constructor's own helpers
4. Main entry method (`Run`, `Serve`, `Handle`, `Process`)
5. Per-event/step handlers (in dispatch order)
6. Sub-handlers
7. Mutators / state-changing helpers
8. Encoders / converters / pure transforms
9. Tiny accessors at the very bottom

Exceptions: generated code (leave as-is); tightly grouped methods on different
subsystems in one file (keep the cluster intact).

Keep a struct's methods in one primary file. Do not split its method set across feature-specific files.

### `ptr.Any(...)` for Pointer Literals (Go)

Use `ptr.Any(value)` instead of a throwaway local variable taken by address.
Import: `github.com/superdurable/dex/service/common/ptr` (server) or
`github.com/superdurable/dex/sdk-go/dex/ptr` (SDK). Use explicit types for
numerics: `ptr.Any(int64(0))`, `ptr.Any(int32(1))`.

Do not use `ptr.Any` when the pointer must alias an existing named variable that
is also read or mutated elsewhere.

### No Defensive Cloning (Go)

Do not call `proto.Clone` or deep-copy messages "just in case". Passing a
message transfers ownership unless the API says otherwise. Workflow inputs,
signals, and activity payloads are freshly deserialized and single-threaded.
Prefer capturing a small id/string or building a new tiny value over cloning.
Copy only when an algorithm must mutate a distinct shared value in place.

### Update Ignore Files When Producing Binaries

Before running `go build -o <path>` or adding a new `main` package, add the
output path to both `.gitignore` **and** `.dockerignore`. Use exact paths, not
overly broad globs. Delete stray uncommitted binaries.

### Run Tests After Every Change

After code changes, run tests via the Makefile — not bare `go test` for full
suites:

- `make -C server unitTests`
- `make -C server integTests` / `temporalIntegTests` / `cadenceIntegTests`

Always tee output: `make -C server unitTests 2>&1 | tee /tmp/test-<scope>.log`

Fix all failures before moving on. If stuck after multiple attempts, pause and
ask the user with: (1) the failure, (2) what you tried, (3) where you're blocked.

### Do Not Casually Skip Failing Tests

Never skip/gate/early-return around a failing assertion just to green the suite
(e.g. `if backend == Cadence { return }` or `t.Skip` for a known flake) unless
the user explicitly asks. On failure: add targeted logs, dump Temporal/Cadence
workflow history (plus describe/query output), find the root cause, then fix
product code or the expectation. Backend-specific skips need a proven platform
limit and user agreement.

## Go-Specific Rules

### Strongly Typed Log Tags

Never use or reintroduce `tag.Value`. Add a semantic, strongly typed constructor in `server/service/common/log/tag` for each structured field.

### Config Field Comments

Every config struct field must have a Go doc comment:

1. Always state the default value.
2. Explain what it means/controls if non-obvious.
3. State immutability, relationships, valid ranges if constrained.
4. Add a concrete example if tricky.

For address fields, explain protocol served, who connects, and bind-vs-advertise
relationship.

### Go Package Aliases

Use the package's declared name. Only alias when:

- Two packages share the same name in one file.
- The default name is misleading or ambiguous.
- An established repo convention applies (e.g. `dexpb` for generated OpenAPI).

Do not invent aliases like `servermetrics` or `mongostore`.

### No Unnecessary Nil Checks

Required dependencies must panic or `log.Fatal` if nil — fail fast at startup.
Do not add `if x == nil { return nil }` guards that silently swallow bugs. Only
add nil checks when nil is a valid, expected value.

### Server Error Handling (`server/`)

API failures that reach Gin handlers should use `errors.ErrorAndStatus` from
`github.com/superdurable/dex/service/common/errors` with an
`dexpb.ErrorSubStatus` and HTTP status code.

- Prefer `NewErrorAndStatus` / `NewErrorAndStatusWithWorkerError`.
- Bad client/SDK input → 4xx + appropriate `ErrorSubStatus`.
- Infrastructure / unexpected failures → 5xx.

### Never Silently Ignore Errors

Every returned error must be handled — returned, logged, or explicitly acted on.
Never `_ = f()`, `x, _ := f()`, or `if err == nil { use(x) }` with no `else`.
If you genuinely must ignore one, leave a code comment explaining why **and**
call it out in review.

Trusted vs untrusted decides fail-fast vs graceful:

- **Trusted** (our own store rows, server-minted ids, invariants we control): a
  violated invariant is a bug — fail fast. Use a `Must*` helper rather than
  silently ignoring. Better still, thread the typed value end-to-end.
- **Untrusted** (HTTP request fields, SDK/worker payloads, anything a client can
  set): must handle gracefully — validate and return an input-style
  `ErrorAndStatus`; **never** `Must*`/panic.

### Naming — No 1-2 Letter Variable Names

Variables (struct fields, parameters, locals) must use descriptive names. Method
receivers are exempt (Go convention: `func (w *Worker) ...`).

Allowed short non-receiver names: `i j k n err ctx ok t mu wg id r w ch`

### Boolean Names Are Predicates

Boolean variables and constants, and methods returning booleans, must use names
that clearly signal boolean semantics, such as `isXxx`, `hasXxx`, `canXxx`,
`shouldXxx`, or `supportsXxx`. Follow each language's capitalization conventions.

### No `NormalizeXyz` Names

Do not introduce `NormalizeXyz` identifiers. Name the concrete operations, such
as `ValidateAndSortSelections`, `TrimWhitespace`, or `CanonicalizeURL`.

### Reuse Existing Domain Terms

When a concept already has a repository or public API term, reuse that exact
term in identifiers. Do not invent a synonym for an existing term such as
instance, name, message, or definition.

### Interpreter Components

Within `server/service/interpreter/`, every component type must be exported.
Helper and value types that are not components may remain unexported.
Constructors and methods called by another component must also be exported,
even when both components share the `interpreter` package. Component-internal
methods remain unexported.

Components may use `UnifiedContext` during construction but must not retain it.
Pass the operation-scoped context explicitly to methods. Handle every `Await`
error immediately. After a successful `Await`, write synchronously without a
redundant context check; otherwise check the context before mutating state.

## Go SDK Conventions (`sdk-go/`)

Prefer explicit domain naming, thin public APIs, apply-style options, and
Def/Impl layering. Keep design docs and examples in sync with API changes.

### Naming

- Factory/constructor names include the domain noun: `InitialAttribute`,
  `InitialAttributeMapValue` — not bare `Initial` / `InitialMapValue`.
- Schema erasure interfaces use `*Def` (`AttributeDef`, `ChannelDef`,
  `InitialAttributeDef`, `StepDef`).
- Unexported concrete implementations use `*Impl` (e.g. `conditionImpl`), not
  `*Value`, unless the type is a true value object.
- Identity types use `*ID` (`StepExecutionID`, `TimerID`), not `*Ref`.
- Optional fields that may be omitted use pointers (`ExecutionNumber *int32`).
- Names describe real semantics, not a misleading action. Prefer
  `SkipWaitImmediately` over `ExecuteImmediately` when the value means “skip
  waiting,” not “run Execute.”
- When an entry method needs a recursive or stateful helper (extra args such as
  a cycle-tracking set), name the helper `doXxx` for the same verb phrase:
  `validateStepOptions` → `doValidateStepOptions`. Keep the entry thin;
  put the real work in `doXxx`.

### API shape

- Omit parameters the SDK can default (e.g. client methods target the current
  run; do not take `runID` unless an operation truly needs a specific run).
- Do not wrap a few flat inputs in an Options struct. Prefer direct parameters
  (`SearchFlows(ctx, query, pageSize, nextPageToken)`).
- Prefer batch APIs alongside or instead of thin single-item variants when that
  matches product shape; drop unused singles (e.g. public Delete helpers) rather
  than keeping dead surface area.
- Optional overrides are pointer fields on options (`RequestID *string`), with
  SDK-generated defaults when nil.

### Functional options

Use apply methods on the option interface — never an empty marker plus
`switch option.(type)`:

```go
type ConditionOption interface {
	applyCondition(*conditionImpl)
}

func applyConditionOptions(condition *conditionImpl, options []ConditionOption) {
	for _, option := range options {
		option.applyCondition(condition)
	}
}
```

Same pattern as `AttributeOption.applyAttribute` and
`StepMoveOption.applyStepMovement`.

### Sealed Def interfaces

Def interfaces exist for SDK-internal schema erasure. Seal them with
unexported methods only. Application-facing names stay on concrete typed values
(e.g. `Attribute[T].AttributeName()`), not on the Def interface.

### Documentation

When changing public SDK types or Client signatures, update in the same change:
[`docs/design/plan/go-sdk-rewrite.md`](docs/design/plan/go-sdk-rewrite.md),
[`sdk-go/README.md`](sdk-go/README.md), examples, and compile-contract tests.

## Test Isolation Rules

### No `time.Sleep` for Async Convergence

Use `require.Eventually` or a polling loop. `time.Sleep` is only acceptable
inside the system under test itself.

### Unique IDs Per Test

Generate unique workflow IDs (and namespaces when applicable) per test when
sharing a Temporal/Cadence stack. Never rely on leftover state from another test.

## Rust Code Conventions

These rules apply to all hand-written Rust workspaces and standalone packages.
Run `make rust-rules-check` and each package's formatting, Clippy, and test targets.
The checker discovers Cargo manifests automatically through Cargo metadata.

| Rule | Convention | Enforcement |
| --- | --- | --- |
| R1 | Non-test Rust files contain at most 1000 lines, excluding the license header. | Rust rules checker; existing oversized files use a shrink-only baseline. |
| R2 | A module directory contains at most 10 non-test Rust files and 10 submodules. Split by responsibility. | Rust rules checker; existing oversized directories use the same baseline. |
| R3 | Every crate has a README with an opening Purpose sentence of at most 30 words. Start its lib.rs and main.rs crate documentation with the same sentence. | Rust rules checker. |
| R4 | Use pub(crate) for internal items and pub only for public APIs, including in the SDK. | rustc unreachable_pub. |
| R5 | Test-only public items end in _for_test and require cfg(test) or the test-util feature. | Rust rules checker verifies the configuration of suffixed items; review verifies naming. |
| R6 | Register every crate in script/rust-crate-layers.json. Depend only on crates in the same or a lower layer. | Rust rules checker using Cargo metadata. |
| R7 | Each executable has one typed configuration. Read environment settings only in configuration modules or tests. Configuration modules are config, *_config, and *_options. | Rust rules checker verifies std::env::var and std::env::var_os locations; review verifies typed ownership. |
| R8 | Use complete domain names. Avoid utils, helpers, common, and misc modules and generic Manager, Handler, Data, or Info types. | Rust rules checker rejects generic module filenames; review checks type names. |
| R9 | Represent closed sets with enums rather than repeated strings. Across crates, use non-exhaustive enums or newtypes. | Clippy match_wildcard_for_single_variants and review. |
| R10 | Handle every error. Do not discard fallible results with let _ or .ok(); explain intentional ignores. | rustc unused_must_use and Clippy let_underscore_must_use, let_underscore_future, and unused_result_ok; review. |
| R11 | Libraries use typed errors, such as thiserror. Generic errors such as anyhow belong only at binary boundaries. Include the operation and object in errors. | Rust rules checker rejects non-development anyhow dependencies in library crates; review checks error context. |
| R12 | Panic on programming errors with expect explaining why failure is impossible. Production code must not use bare unwrap. Never use todo!, unimplemented!, or dbg!. | Clippy unwrap_used, todo, unimplemented, and dbg_macro; Rust rules checker rejects empty expect explanations. Tests may unwrap. |
| R13 | Libraries log with tracing. Keep messages constant and values in fields. Examples, CLI crates, and build scripts may print through registered crate lint overrides. | Clippy print_stdout and print_stderr; review checks structured messages. |
| R14 | Every background task, connection, and child process has an owner and cancellation path. Bind tokio::spawn JoinHandles; never use let _ to discard them. Explain deliberate detachment. | Rust rules checker verifies binding; review checks lifecycle and cancellation. |
| R15 | Forbid unsafe outside registered FFI crates. FFI crates use unsafe_code = "deny" and an expect with a safety reason at each unsafe operation. | rustc unsafe_code; Rust rules checker verifies registered FFI lint tables. |
| R16 | Suppress lints with #[expect(lint, reason = "...")], never #[allow]. Remove expectations when the lint no longer applies. | Clippy allow_attributes and allow_attributes_without_reason; rustc unfulfilled_lint_expectations. |
| R17 | Never hand-edit generated code. | make generated-code-check. |
| R18 | Preserve existing comments. New comments explain non-obvious reasons and contain fewer than 20 words per block. Public SDK API documentation follows the detailed SDK documentation rules. | SDK missing_docs and review. |
| R19 | Format Rust with rustfmt. | cargo fmt --all --check. |

Use the frozen workspace Rust and Clippy lint tables in every workspace. Members
inherit with `[lints] workspace = true`. Registered FFI and printing crates copy
the entire table, changing only unsafe_code to deny for FFI and print_stdout /
print_stderr to allow for printing crates. The checker rejects other differences.
Keep clippy.toml test settings identical across workspaces. CI runs Clippy with
`--all-targets -- -D warnings`.

The checker uses one generated-code predicate: opening Code generated or
@generated markers, or a gen/ or OUT_DIR path. Generated and test source files
are excluded from structural counts. Inline test modules still count toward a
production file's size. The baseline may only shrink against origin/main:
never add an entry or increase its ceiling. Remove an entry as soon as the
file or directory meets its limit; stale entries fail the check.

## server-zd Engine Rules (`server-zd/**`)

- `dexzd-apply` is a deterministic state machine. It never reads clocks,
  random numbers, configuration, the environment, files, or the network, and
  it never iterates a hash map. Values that change an apply result come from
  the command or from persisted state. Its `clippy.toml` bans the common
  offenders; keep that file's frozen test settings identical to the workspace.
- A change to the result of applying an already committed command needs a new
  behavior version with a named predicate. Never change the behavior of an
  existing version. This rule starts with server-zd 1.0.0, the first
  `server-zd/v1.*` release tag. Until then, server-zd stays at behavior
  version 1, an apply-result change needs no new version, and no work goes
  into version barriers or raising them.
- Persistent formats (`server-zd/proto/dexzd/`, the RocksDB key layout, the
  object storage layout, and snapshots) follow the protobuf compatibility
  rules: additive only; never renumber or reuse field numbers or names.
- Generated protobuf maps use `BTreeMap`.
- Local tests use the in-process s3mock or the in-memory object store, never
  MinIO.
