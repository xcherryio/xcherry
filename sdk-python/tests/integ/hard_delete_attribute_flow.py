# Portions of this file are derived from indeedeng/iwf-java-sdk.
# Those portions are licensed under the Apache License, Version 2.0.
# See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
#
# Modifications Copyright (c) 2026 Super Durable, Inc.
#
# Modifications are licensed under the Sustainable Use License 1.0.
# Third-Party Materials remain under the Apache License, Version 2.0.
# See LICENSE and LEGACY_NOTICES.md.

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from dex import (
    Attribute,
    AttributeMap,
    Channel,
    Context,
    Flow,
    PersistenceSchema,
    RPCResult,
    Step,
    StepDecision,
    StepList,
    StepOptions,
    Wait,
    go_to,
    graceful_complete,
    rpc,
)

PAYMENTS_TOKEN = "payments"
BILLING_TOKEN = "billing"
INITIAL_API_KEY = "initial-api-key-5c1e"
INITIAL_PAYMENTS_TOKEN = "initial-payments-token-9a3d"
STEP_API_KEY = "step-api-key-2f7b"
RPC_API_KEY = "rpc-api-key-81c4"
EXTERNAL_BILLING_TOKEN = "external-billing-token-6d0e"
ALL_SECRETS = (
    INITIAL_API_KEY,
    INITIAL_PAYMENTS_TOKEN,
    STEP_API_KEY,
    RPC_API_KEY,
    EXTERNAL_BILLING_TOKEN,
)

API_KEY = Attribute("api-key", str, hard_delete_on_null=True)
TOKENS = AttributeMap("provider-token", str, hard_delete_on_null=True)
STEP_OBSERVATIONS = Attribute("step-observations", str)
DELETE_SECRETS = Channel("delete-secrets", type(None))
FINISH = Channel("finish", type(None))


@dataclass(frozen=True)
class SecretSnapshot:
    api_key: str | None
    payments_token: str | None
    billing_token: str | None


class ReadAndOverwriteSecretsStep(Step[None]):
    def get_step_options(self) -> StepOptions:
        return StepOptions(execute_load_attribute_maps=(TOKENS,))

    def execute(self, context: Context, input: None) -> StepDecision:
        STEP_OBSERVATIONS.set(
            context,
            _observation(
                API_KEY.get(context) == INITIAL_API_KEY,
                TOKENS.get(context, PAYMENTS_TOKEN) == INITIAL_PAYMENTS_TOKEN,
            ),
        )
        API_KEY.set(context, STEP_API_KEY)
        return go_to(DeleteSecretsStep, None)


class DeleteSecretsStep(Step[None]):
    def get_step_options(self) -> StepOptions:
        return StepOptions(execute_load_attribute_maps=(TOKENS,))

    def wait_for(self, context: Context, input: None) -> Wait:
        return Wait.until(DELETE_SECRETS.for_one())

    def execute(self, context: Context, input: None) -> StepDecision:
        API_KEY.delete(context)
        TOKENS.delete(context, PAYMENTS_TOKEN)
        TOKENS.set(context, BILLING_TOKEN, cast(str, None))
        return go_to(VerifySecretsDeletedStep, None)


class VerifySecretsDeletedStep(Step[None]):
    def get_step_options(self) -> StepOptions:
        return StepOptions(execute_load_attribute_maps=(TOKENS,))

    def wait_for(self, context: Context, input: None) -> Wait:
        return Wait.until(FINISH.for_one())

    def execute(self, context: Context, input: None) -> StepDecision:
        return graceful_complete(
            _observation(
                API_KEY.get(context) is None,
                TOKENS.get(context, PAYMENTS_TOKEN) is None,
                TOKENS.get(context, BILLING_TOKEN) is None,
                TOKENS.get_all_instance_keys(context) == (),
            )
        )


class HardDeleteAttributeFlow(Flow[None]):
    def __init__(self) -> None:
        self.read_and_overwrite = ReadAndOverwriteSecretsStep()
        self.delete_secrets = DeleteSecretsStep()
        self.verify_deleted = VerifySecretsDeletedStep()

    def get_steps(self) -> StepList[None]:
        return StepList.start_step(self.read_and_overwrite).other_steps(
            self.delete_secrets, self.verify_deleted
        )

    def get_persistence_schema(self) -> PersistenceSchema:
        return PersistenceSchema.of(
            API_KEY, TOKENS, STEP_OBSERVATIONS, DELETE_SECRETS, FINISH
        )

    @rpc(load_attribute_maps=(TOKENS,))
    def get_secret_snapshot(self, context: Context) -> RPCResult[SecretSnapshot]:
        return RPCResult(
            SecretSnapshot(
                api_key=API_KEY.get(context),
                payments_token=TOKENS.get(context, PAYMENTS_TOKEN),
                billing_token=TOKENS.get(context, BILLING_TOKEN),
            )
        )

    @rpc
    def get_step_observations(self, context: Context) -> RPCResult[str]:
        return RPCResult(STEP_OBSERVATIONS.get(context))

    @rpc
    def set_api_key(self, context: Context, api_key: str) -> None:
        API_KEY.set(context, api_key)

    @rpc
    def clear_api_key(self, context: Context) -> None:
        API_KEY.set(context, cast(str, None))

    @rpc
    def delete_api_key(self, context: Context) -> None:
        API_KEY.delete(context)

    @rpc
    def request_secret_deletion(self, context: Context) -> None:
        DELETE_SECRETS.publish(context, None)

    @rpc
    def finish(self, context: Context) -> None:
        FINISH.publish(context, None)


def _observation(*checks: bool) -> str:
    return ",".join("ok" if check else "mismatch" for check in checks)
