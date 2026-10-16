# Go FDG 2.0 authoring

Use this page for Summary, Display, Start input, Actions and analyzer diagnostics
before searching the CLI implementation. The syntax below is checked against
[Dex CLI v1.11.0 directives](https://github.com/superdurable/dex/blob/cli-v1.11.0/cli/internal/flowviz/v2_directives.go)
and its [typed input example](https://github.com/superdurable/dex/blob/cli-v1.11.0/cli/internal/command/testfixtures/visualization-v2-start/workflow.go).
Keep the application's installed SDK and `dexcli` pins. The same definitions
drive local `dexcli dev` and a deployed Dex Web; the application does not run
its own Web server for them.

## File and registration boundaries

Keep the Flow, its indexed/view/Action Attributes, view RPCs and Action input
structs in the same source file. Register every durable primitive in
`GetPersistenceSchema`, every Step in `GetSteps`, and both view RPCs in
`GetRPCs`. Use Go type-derived Flow and Step identities. Connector factory
Steps use their generated configuration's static `StepType`, `ConnectionName`
and `Annotations`; inspect that exact released factory type.

Each ordinary Step type has one group and explanation:

```go
// dex:group group-id:process group-label:"Process"
// dex:explanation text:"Perform the accepted operation."
```

A group of two or more Steps does one thing, so Dex Web can fold it into one
stage that opens in place. Make it one run, where ordinary transitions inside
the group join all of its Steps, or one choice, where one Step outside the group
leads into every part, such as an agent's decision Step and its tools. The
analyzer warns about any other group with `v2_step_group_shape` and names the
parts it falls into; Dex Web then shows those Steps in the group's band instead
of folding them.

Groups are judged on the Flow's main story. A Step that only failure
transitions reach, directly or through other such Steps, is failure handling:
Dex Web leaves it out of the drawing, counts the failures on the Step card they
leave, and draws them beside that Step when it is selected or a run went
through them. A failure transition joins no group, and a failure-handling
Step's group is not checked, so give it the group of the Step it handles.

When a Step only records breakage, such as exhausted retries or an uncertain
outcome, make it the failing Step's Execute failure route
(`ProceedToOnExecuteFailure`) rather than an ordinary decision, so that Dex Web
shows it as failure handling. Business rejections and connector outcome
branches stay ordinary decisions and stay in the main story.

Every indexed Attribute also has a matching directive:

```go
// dex:indexed-attribute attribute-key:status index-key:keyword2 index-type:keyword value-type:string description:"Current status"
var Status = dex.DefineAttribute[string]("status",
    dex.Indexed(dex.AttributeIndex{Type: dex.IndexKeyword, IndexKey: "keyword2"}))
```

## Summary and Display

Both methods accept `(ctx dex.Context, _ dex.None)` and return
`(*dex.RPCResult[map[string]any], error)`. Register them through
`dex.DefineRPC(flow.GetDexSummary, nil)` and the corresponding Display method.
Read only the declared Attributes. Return a map literal with exactly the field
keys declared on that method, including null for a missing value. Do not build
the map through loops or helpers that hide its keys from static analysis.

```go
// dex:field attribute-key:status value-type:string editable:false description:"Current status" ui-slot:status
func (*ProcessFlow) GetDexDisplay(ctx dex.Context, _ dex.None) (*dex.RPCResult[map[string]any], error) {
    status, err := Status.Get(ctx)
    if err != nil { return nil, err }
    return &dex.RPCResult[map[string]any]{Output: map[string]any{"status": status}}, nil
}
```

The exact arguments are `attribute-key`, `value-type`, `editable`,
`description`, and optional `ui-slot`. There is no `field-key`, `rpc`,
`dex:display`, or implicit label syntax. A field refers to an Attribute declared
in this file, not an arbitrary computed map entry. An empty view returns an
empty map and has no field directives. Do not repeat indexed Attributes in
Summary; they already appear in the list. Display may include them. Never
expose a hard-delete Attribute in Summary or Display, and do not use one as an
Action condition; Dex rejects hard-delete Attributes in Action conditions.

Use `ui-slot:title`, `subtitle`, `status`, `recommendation`, or `reason`.
Each slot except `reason` is unique within one view. Ordinary fields need no
slot. Summary is always read-only. Editable Display fields must be supported
scalars. Keep structured business results in their owning Attribute and expose
its actual FDG type instead of duplicating business state for presentation.

## Actions and input

An operator Action is typed RPC registration, not a comment directive:

```go
dex.DefineRPC(flow.SubmitDecision, &dex.RPCOptions{
    Action: dex.DefineAction("Submit decision",
        dex.WhenAttributeMatches(Status, dex.AttributeMatchEqual("PENDING")),
        dex.ActionRequiresPermission("process.review")),
    LockAttributes: []dex.AttributeLock{dex.LockAttribute(Status)},
})
```

The label, permission and condition values are compile-time constants. Declare
exactly one permission. The condition uses a scalar Attribute in this file.
Do not use removed `dex:action` or `dex:when` comments. The handler rechecks
current state under the business locks required for atomic admission; the UI
condition alone does not authorize or deduplicate a write. Follow the Flow's
accepted revision and operation identity rules.

An Action with no fields accepts `dex.None` and returns
`*dex.RPCResult[dex.None]`. Otherwise use a named same-file struct with one
`dex:input` per exported JSON field, attached to the RPC method:

```go
type DecisionInput struct {
    Decision string `json:"decision"`
    Note *string `json:"note,omitempty"`
}

// dex:input field-name:decision value-type:string source:user required:true description:"Decision"
// dex:input field-name:note value-type:string source:user required:false description:"Optional note"
```

Arguments are `field-name`, `value-type`, `source`, `required`, `description`,
and optional `attribute-key` or `capture`. `source:user` forbids
`attribute-key`. `source:attribute` requires a matching scalar Attribute key;
that input is supplied from the Run and is hidden from the user. Requiredness
must match pointer semantics: optional fields use pointers. QR input adds
`capture:qr-code` only to a user-sourced string field; manual input stays usable.

Start Flow is different: its form comes from the input type of the Step
registered with `DefineStartStep`. Use a concrete typed business input with
JSON tags. Do not add Action input directives to the Start Step or put Worker
addresses, actor permissions, or engine RunIDs in business input.

## Focused verification

After changing definitions, render every Flow source file with
`dexcli visualize path/to/flow.go --schema-version 2.0 --json --out DIRECTORY/<flow-name>`.
Analysis does not deploy or call providers. Require `valid: true` and resolve
blocking diagnostics; with blocking diagnostics the command still writes the
partial JSON and exits with status 1. Keep that rendering in one project
command so every Flow is checked, not only the one being edited.
If analysis fails, read the bounded partial JSON diagnostic before fetching
unrelated source. Compile first when the diagnostic reports Go syntax/types.

| Diagnostic | Concrete correction |
| --- | --- |
| `v2_view_rpc` | Define and register both view methods with the exact signatures. |
| `v2_view_rpc_output` | Return one literal map with exactly the declared field keys. |
| `v2_directive` | Match the Attribute's actual type and argument names; an unnamed slice is `array` (`[]string` is `string-array`), not `json`. |
| `v2_action` | Use direct typed Action registration, a declared scalar condition and one permission. |
| `v2_action_input` | Declare every JSON field once; use a pointer for `required:false`. |
| `hidden_dex_decision` | Return Dex decisions directly from Step methods; helpers may compute input or record state. |
| `unknown_step_target` | Register the target and use its real Go type or exact static Connector Step reference. |
| `connector_factory_step_type` | Give the operation factory a nonempty compile-time StepType. |
| `unused_resource` | Make primitive use visible in the Flow's Step or RPC methods or in the same-package helpers they call, up to six levels deep; do not retain obsolete scaffold state. |
| `connector_release_required` | Require an exact official published module version without a local replacement. The diagnostic clears for a release or an `-rc.N` release candidate, but handoff still pins the exact official release. |
| `attribute_map_read_not_loaded`, `attribute_map_enumeration_not_loaded`, `attribute_map_instance_not_loaded`, `channel_messages_not_loaded` | Select the AttributeMap instance or whole map, or the Channel messages, in the options of the Step, RPC or timeout handler that reads them; an unselected read fails at run time. |
| `step_progress_outside_step` | Write Stream output only in a Step; an RPC publishes a Channel message or returns `NextSteps` to schedule that Step. |
| `start_flow_input_type_mismatch` | Pass the starting Step's input type to `StartFlow`, or declare `dex.None` and pass `nil`. |
| `v2_step_group_shape` | A warning, never blocking: the group is neither one run nor one choice, and the message names its parts. Split it so that each group does one thing; failure-handling Steps are not part of the check. |

A successful FDG check proves a definition, not an actual provider call. Keep
source readiness separate from configured real business acceptance.
