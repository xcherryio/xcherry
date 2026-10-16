// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

use std::sync::LazyLock;

use dex_sdk::{
    Attribute, AttributeMap, Channel, Context, Flow, HandlerResult, PersistenceSchema, Rpc,
    RpcList, RpcResult, Step, StepDecision, StepList, StepOptions, Wait,
};

pub(crate) const INITIAL_API_KEY: &str = "rust-initial-api-key";
pub(crate) const STEP_API_KEY: &str = "rust-step-rotated-api-key";
pub(crate) const RPC_API_KEY: &str = "rust-rpc-rotated-api-key";
pub(crate) const INITIAL_TENANT_TOKEN: &str = "rust-initial-tenant-token";
pub(crate) const TENANT: &str = "tenant-a";
pub(crate) const ABSENT: &str = "absent";

pub(crate) static API_KEY: LazyLock<Attribute<String>> =
    LazyLock::new(|| Attribute::new("api-key").hard_delete_on_null());
pub(crate) static TENANT_TOKENS: LazyLock<AttributeMap<TenantToken>> =
    LazyLock::new(|| AttributeMap::new("tenant-tokens").hard_delete_on_null());
pub(crate) static PLAIN_NOTE: LazyLock<Attribute<String>> =
    LazyLock::new(|| Attribute::new("plain-note"));
pub(crate) static ROTATION_OBSERVATION: LazyLock<Attribute<String>> =
    LazyLock::new(|| Attribute::new("rotation-observation"));
pub(crate) static DELETION_OBSERVATION: LazyLock<Attribute<String>> =
    LazyLock::new(|| Attribute::new("deletion-observation"));
static DELETION_APPROVALS: LazyLock<Channel<()>> =
    LazyLock::new(|| Channel::new("deletion-approvals"));
static COMPLETION_APPROVALS: LazyLock<Channel<()>> =
    LazyLock::new(|| Channel::new("completion-approvals"));

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct TenantToken {
    pub(crate) token: String,
}

pub(crate) struct HardDeleteAttributeFlow {
    rotate: RotateSecretsStep,
    delete: DeleteSecretsStep,
    complete: CompleteAfterDeletionStep,
}

impl HardDeleteAttributeFlow {
    pub(crate) const GET_API_KEY: Rpc<(), Option<String>> = Rpc::new("get_api_key");
    pub(crate) const ROTATE_API_KEY: Rpc<String, ()> = Rpc::new("rotate_api_key");
    pub(crate) const APPROVE_DELETION: Rpc<(), ()> = Rpc::new("approve_deletion");
    pub(crate) const APPROVE_COMPLETION: Rpc<(), ()> = Rpc::new("approve_completion");

    pub(crate) fn new() -> Self {
        Self {
            rotate: RotateSecretsStep,
            delete: DeleteSecretsStep,
            complete: CompleteAfterDeletionStep,
        }
    }

    fn get_api_key(&self, context: &mut Context) -> HandlerResult<RpcResult<Option<String>>> {
        Ok(RpcResult::new(API_KEY.get(context)?))
    }

    fn rotate_api_key(&self, context: &mut Context, api_key: String) -> HandlerResult<()> {
        API_KEY.set(context, api_key)
    }

    fn approve_deletion(&self, context: &mut Context) -> HandlerResult<()> {
        DELETION_APPROVALS.publish(context, ())
    }

    fn approve_completion(&self, context: &mut Context) -> HandlerResult<()> {
        COMPLETION_APPROVALS.publish(context, ())
    }
}

impl Flow for HardDeleteAttributeFlow {
    type StartInput = ();

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.rotate)
            .and(&self.delete)
            .and(&self.complete)
    }

    fn persistence(&self) -> PersistenceSchema {
        PersistenceSchema::new()
            .attribute(&API_KEY)
            .attribute_map(&TENANT_TOKENS)
            .attribute(&PLAIN_NOTE)
            .attribute(&ROTATION_OBSERVATION)
            .attribute(&DELETION_OBSERVATION)
            .channel(&DELETION_APPROVALS)
            .channel(&COMPLETION_APPROVALS)
    }

    fn rpcs(&self) -> RpcList<Self> {
        RpcList::new()
            .function_without_input(Self::GET_API_KEY, Self::get_api_key)
            .procedure(Self::ROTATE_API_KEY, Self::rotate_api_key)
            .procedure_without_input(Self::APPROVE_DELETION, Self::approve_deletion)
            .procedure_without_input(Self::APPROVE_COMPLETION, Self::approve_completion)
    }
}

struct RotateSecretsStep;

impl Step for RotateSecretsStep {
    type Input = ();

    fn execute(&self, context: &mut Context, _input: ()) -> HandlerResult<StepDecision> {
        let api_key = API_KEY.get_required(context)?;
        let tenant_token = TENANT_TOKENS.get_required(context, TENANT)?;
        let plain_note = PLAIN_NOTE.get_required(context)?;
        ROTATION_OBSERVATION.set(
            context,
            format!("{api_key}|{}|{plain_note}", tenant_token.token),
        )?;
        API_KEY.set(context, STEP_API_KEY.to_string())?;
        Ok(StepDecision::go_to(&DeleteSecretsStep, ()))
    }

    fn options(&self) -> StepOptions<Self::Input> {
        StepOptions::new().execute_load_attribute_map(&TENANT_TOKENS)
    }
}

struct DeleteSecretsStep;

impl Step for DeleteSecretsStep {
    type Input = ();

    fn wait_for(&self, _context: &mut Context, _input: ()) -> HandlerResult<Wait> {
        Ok(Wait::until(DELETION_APPROVALS.for_one()))
    }

    fn execute(&self, context: &mut Context, _input: ()) -> HandlerResult<StepDecision> {
        let api_key = API_KEY.get(context)?.unwrap_or_else(|| ABSENT.to_string());
        DELETION_OBSERVATION.set(context, api_key)?;
        API_KEY.delete(context)?;
        TENANT_TOKENS.delete(context, TENANT)?;
        Ok(StepDecision::go_to(&CompleteAfterDeletionStep, ()))
    }
}

struct CompleteAfterDeletionStep;

impl Step for CompleteAfterDeletionStep {
    type Input = ();

    fn wait_for(&self, _context: &mut Context, _input: ()) -> HandlerResult<Wait> {
        Ok(Wait::until(COMPLETION_APPROVALS.for_one()))
    }

    fn execute(&self, context: &mut Context, _input: ()) -> HandlerResult<StepDecision> {
        let api_key = API_KEY.get(context)?.unwrap_or_else(|| ABSENT.to_string());
        let tenant_token = TENANT_TOKENS
            .get(context, TENANT)?
            .map_or_else(|| ABSENT.to_string(), |tenant_token| tenant_token.token);
        Ok(StepDecision::graceful_complete(format!(
            "{api_key}|{tenant_token}"
        )))
    }

    fn options(&self) -> StepOptions<Self::Input> {
        StepOptions::new().execute_load_attribute_map(&TENANT_TOKENS)
    }
}
