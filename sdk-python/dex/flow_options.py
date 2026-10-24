# Legacy Materials in this file remain under their original licenses.
# See LEGACY_NOTICES.md.

# Modifications Copyright (c) 2026 Super Durable, Inc.
#
# Modifications after the Legacy Cutoff are licensed under the
# Sustainable Use License 1.0.
# Legacy Materials remain under their original licenses.
# See LICENSE and LEGACY_NOTICES.md.

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, TypeVar, overload

from dex._utils import require_map_instance
from dex.attribute import Attribute, AttributeLock, AttributeMap, AttributeMapLoad
from dex.channel import Channel, ChannelMap, ChannelMapLoad
from dex.flow_config import FlowConfig
from dex.step import RetryPolicy, Step, StepDurability, StepOptions

ValueT = TypeVar("ValueT")


class IdReusePolicy(Enum):
    """Control whether ``start_flow`` may reuse an existing Flow ID.

    Attributes:
        DEFAULT: Use the Dex server default, which rejects every previously used Flow ID.
        ALLOW_IF_PREVIOUS_FAILED: Reuse only after an unsuccessful closed run.
        ALLOW_IF_NOT_RUNNING: Reuse after any closed run.
        ALLOW_TERMINATE_IF_RUNNING: Terminate an active run and start the new run.
        DISALLOW: Reject every previously used Flow ID.
    """

    DEFAULT = "default"
    ALLOW_IF_PREVIOUS_FAILED = "allow_if_previous_failed"
    ALLOW_IF_NOT_RUNNING = "allow_if_not_running"
    ALLOW_TERMINATE_IF_RUNNING = "allow_terminate_if_running"
    DISALLOW = "disallow"


class FlowTimeoutPolicy(Enum):
    """Control how Dex responds when a positive soft Flow timeout expires.

    Attributes:
        DEFAULT: Invoke ``Flow.handle_timeout`` when overridden; otherwise fail.
        FAIL: Fail with ``FlowErrorType.FLOW_TIMEOUT`` and permit Flow retries.
        CANCEL: Cancel without retrying the Flow.
        HANDLER: Start one retryable ``Flow.handle_timeout`` execution after its timer.
    """

    DEFAULT = "default"
    FAIL = "fail"
    CANCEL = "cancel"
    HANDLER = "handler"


def _resolve_flow_timeout_policy(
    flow_name: str,
    has_timeout_handler: bool,
    timeout: timedelta | None,
    policy: FlowTimeoutPolicy,
) -> FlowTimeoutPolicy:
    if timeout is None or timeout.total_seconds() <= 0:
        if policy is not FlowTimeoutPolicy.DEFAULT:
            raise ValueError("Flow timeout policy requires a positive timeout")
        return FlowTimeoutPolicy.DEFAULT
    if policy is FlowTimeoutPolicy.DEFAULT:
        return (
            FlowTimeoutPolicy.HANDLER if has_timeout_handler else FlowTimeoutPolicy.FAIL
        )
    if policy is FlowTimeoutPolicy.HANDLER and not has_timeout_handler:
        raise ValueError(f"Flow {flow_name} has no handle_timeout override")
    return policy


@dataclass(frozen=True)
class _AttributeInitialization:
    definition: Attribute[Any] | AttributeMap[Any]
    instance: str | None
    value: object


@dataclass(frozen=True)
class FlowTimeoutHandlerOptions:
    """Configure timeout-handler execution and selective state loading.

    Ordinary Attributes and Channel size metadata load automatically. AttributeMap
    values and pending Channel messages require an explicit selection. One logical
    handler execution may contain multiple retry attempts.

    Attributes:
        method_timeout: Maximum duration of one handler attempt.
        heartbeat_timeout: Progress deadline for one regular attempt.
        retry: Optional retry policy; ``None`` uses server defaults.
        durability: Durability for successful handler side effects.
        lock_attributes: Attribute locks held during the handler invocation.
        load_attribute_maps: AttributeMaps whose current instances load.
        load_attribute_map_instances: Exact AttributeMap instances to load.
        load_channels: Channels whose pending messages load.
        load_channel_maps: ChannelMaps whose current instance messages load.
        load_channel_map_instances: Exact ChannelMap instances to load.

    Examples:
        >>> options = FlowTimeoutHandlerOptions(
        ...     method_timeout=timedelta(seconds=30),
        ...     retry=RetryPolicy(maximum_attempts=3),
        ...     load_channels=(commands,),
        ... ).on_failure_proceed_to(TimeoutRecovery)
    """

    method_timeout: timedelta | None = None
    heartbeat_timeout: timedelta | None = None
    retry: RetryPolicy | None = None
    durability: StepDurability = StepDurability.DEFAULT
    lock_attributes: tuple[AttributeLock, ...] = ()
    load_attribute_maps: tuple[AttributeMap[Any], ...] = ()
    load_attribute_map_instances: tuple[AttributeMapLoad, ...] = ()
    load_channels: tuple[Channel[Any], ...] = ()
    load_channel_maps: tuple[ChannelMap[Any], ...] = ()
    load_channel_map_instances: tuple[ChannelMapLoad, ...] = ()
    _failure_target: type[Step[None]] | None = None
    _failure_options: StepOptions | None = None

    def on_failure_proceed_to(
        self,
        step: type[Step[None]],
        options: StepOptions | None = None,
    ) -> FlowTimeoutHandlerOptions:
        """Return a copy routing exhausted retries to a registered no-input Step.

        The recovery Step receives ``None`` and reads the final handler failure from
        ``Context.recovery_error``.

        Args:
            step: A Step with ``None`` input registered by the timed-out Flow.
            options: Optional execution overrides for the recovery movement.

        Returns:
            A new options value with failure routing configured.
        """
        return replace(self, _failure_target=step, _failure_options=options)


@dataclass(frozen=True)
class StartFlowOptions:
    """Configure creation of a new Flow execution.

    Durations are :class:`datetime.timedelta` values. ``None`` uses the registered
    Flow or server default. Use :meth:`with_attribute` to add initial Attribute writes.

    To retry one logical start safely, for example after a lost response, set a
    stable ``request_id`` and ``ignore_already_started=True`` together. Reuse the
    same values on every retry.

    Attributes:
        timeout: Optional durable soft timeout. ``None`` or zero disables it.
        timeout_policy: Action taken when a positive timeout expires.
        timeout_handler_options: Execute policy used when timeout policy is ``HANDLER``.
        start_delay: Optional delay before the starting Step becomes eligible.
        id_reuse_policy: Flow ID reuse policy; defaults to ``DEFAULT``.
        retry_policy: Optional Flow-level retry policy.
        config_override: Optional FlowConfig applied over registered defaults.
        ignore_already_started: Return the existing run rather than raising
            ``FlowAlreadyStartedError`` when supported by the server. It applies
            only when ``request_id`` matches the request ID that started the
            existing run. A different or generated request ID still raises
            ``FlowAlreadyStartedError``.
        request_id: Optional non-empty idempotency key. ``None`` generates one.
            A retried start returns the existing run only when
            ``ignore_already_started`` is ``True`` and this request ID matches
            the request that started that run. Without that option, the retry
            raises ``FlowAlreadyStartedError`` even when the first attempt
            created the Flow. The match applies only when the ID reuse policy
            rejects the start. A policy that permits another run starts a new
            run instead. Dex compares only request IDs, not input or other
            options, so never reuse a request ID for a different logical start.

    Examples:
        >>> options = (
        ...     StartFlowOptions(timeout=timedelta(hours=1))
        ...     .with_attribute(status, "pending")
        ...     .with_attribute(balances, "merchant-7", 1200)
        ... )
        >>> retry_safe_options = StartFlowOptions(
        ...     ignore_already_started=True,
        ...     request_id=f"start-order-{order_id}",
        ... )
        >>> run_id = client.start_flow(
        ...     order_flow, f"order-{order_id}", order_input, retry_safe_options
        ... )
    """

    timeout: timedelta | None = None
    timeout_policy: FlowTimeoutPolicy = FlowTimeoutPolicy.DEFAULT
    timeout_handler_options: FlowTimeoutHandlerOptions | None = None
    start_delay: timedelta | None = None
    id_reuse_policy: IdReusePolicy = IdReusePolicy.DEFAULT
    retry_policy: RetryPolicy | None = None
    _attribute_initializations: tuple[_AttributeInitialization, ...] = ()
    config_override: FlowConfig | None = None
    ignore_already_started: bool = False
    request_id: str | None = None

    @overload
    def with_attribute(
        self,
        attribute: Attribute[ValueT],
        value: ValueT,
        /,
    ) -> StartFlowOptions: ...

    @overload
    def with_attribute(
        self,
        attribute: AttributeMap[ValueT],
        instance: str,
        value: ValueT,
        /,
    ) -> StartFlowOptions: ...

    def with_attribute(
        self,
        attribute: Attribute[Any] | AttributeMap[Any],
        /,
        *args: object,
    ) -> StartFlowOptions:
        """Return a copy with one initial Attribute value appended.

        Singleton Attributes take ``(attribute, value)``; AttributeMaps take
        ``(attribute, instance, value)``. Calls preserve initialization order.

        Args:
            attribute: A registered Attribute or AttributeMap definition.
            *args: The value, or the AttributeMap instance followed by its value.

        Returns:
            A new options object containing the additional initialization.

        Raises:
            TypeError: If arguments do not match either supported form.
            ValueError: If an AttributeMap instance is empty or contains ``/``.
        """
        if isinstance(attribute, Attribute) and len(args) == 1:
            initialization = _AttributeInitialization(attribute, None, args[0])
        elif isinstance(attribute, AttributeMap) and len(args) == 2:
            instance = args[0]
            if not isinstance(instance, str):
                raise TypeError("attribute-map instance must be a string")
            require_map_instance(instance)
            initialization = _AttributeInitialization(attribute, instance, args[1])
        else:
            raise TypeError("with_attribute received invalid arguments")
        return replace(
            self,
            _attribute_initializations=self._attribute_initializations
            + (initialization,),
        )


class SubFlowReusePolicy(Enum):
    """Control how a generated SubFlow Flow ID resolves an existing execution.

    Attributes:
        ATTACH: Attach to a running execution or return its terminal result.
        RESTART_IF_PREVIOUS_EXITS_ABNORMALLY: Attach while running, return a
            successful result, and restart an abnormal previous execution.
        ALWAYS_RESTART: Replace any different existing execution.
    """

    ATTACH = "attach"
    RESTART_IF_PREVIOUS_EXITS_ABNORMALLY = "restart_if_previous_exits_abnormally"
    ALWAYS_RESTART = "always_restart"


@dataclass(frozen=True)
class SubFlowOptions:
    """Configure one durable SubFlow Condition.

    The SubFlow inherits the parent's effective Flow configuration. ``None`` values
    preserve normal start defaults. Dex generates the Flow ID and request ID.

    Attributes:
        timeout: Optional maximum SubFlow lifetime.
        timeout_policy: Action taken when a positive timeout expires.
        timeout_handler_options: Execute policy used when timeout policy is ``HANDLER``.
        start_delay: Optional delay before its starting Step.
        retry_policy: Optional whole-Flow retry policy.
        config_override: Fields applied over the inherited parent configuration.
        reuse_policy: Existing-execution resolution policy.
        condition_id: Stable ID required by ``Wait.any_combination_of``.
    """

    timeout: timedelta | None = None
    timeout_policy: FlowTimeoutPolicy = FlowTimeoutPolicy.DEFAULT
    timeout_handler_options: FlowTimeoutHandlerOptions | None = None
    start_delay: timedelta | None = None
    retry_policy: RetryPolicy | None = None
    _attribute_initializations: tuple[_AttributeInitialization, ...] = ()
    config_override: FlowConfig | None = None
    reuse_policy: SubFlowReusePolicy = (
        SubFlowReusePolicy.RESTART_IF_PREVIOUS_EXITS_ABNORMALLY
    )
    condition_id: str | None = None

    @overload
    def with_attribute(
        self, attribute: Attribute[ValueT], value: ValueT, /
    ) -> SubFlowOptions: ...

    @overload
    def with_attribute(
        self,
        attribute: AttributeMap[ValueT],
        instance: str,
        value: ValueT,
        /,
    ) -> SubFlowOptions: ...

    def with_attribute(
        self,
        attribute: Attribute[Any] | AttributeMap[Any],
        /,
        *args: object,
    ) -> SubFlowOptions:
        """Return a copy with one target-Flow Attribute initialization appended.

        Args:
            attribute: A singleton Attribute or AttributeMap owned by the SubFlow.
            *args: Its value, or a map instance followed by its value.

        Returns:
            A new immutable options value.

        Raises:
            TypeError: If arguments do not match either supported form.
            ValueError: If an AttributeMap instance is empty or contains ``/``.
        """
        if isinstance(attribute, Attribute) and len(args) == 1:
            initialization = _AttributeInitialization(attribute, None, args[0])
        elif isinstance(attribute, AttributeMap) and len(args) == 2:
            instance = args[0]
            if not isinstance(instance, str):
                raise TypeError("attribute-map instance must be a string")
            require_map_instance(instance)
            initialization = _AttributeInitialization(attribute, instance, args[1])
        else:
            raise TypeError("with_attribute received invalid arguments")
        return replace(
            self,
            _attribute_initializations=self._attribute_initializations
            + (initialization,),
        )


class TimeTravelType(Enum):
    """Select the historical point from which time travel should create a new run.

    Attributes:
        BEGINNING: Restart from the beginning of the Flow.
        HISTORY_EVENT_TIME: Resume at the first event at or after a timestamp.
        STEP_TYPE: Resume at the latest execution of a Step type.
        STEP_EXECUTION_ID: Resume at one exact Step execution ID.
    """

    BEGINNING = "beginning"
    HISTORY_EVENT_TIME = "history_event_time"
    STEP_TYPE = "step_type"
    STEP_EXECUTION_ID = "step_execution_id"


class TimeTravelStepMethod(Enum):
    """Select the Step method used as a Step execution time travel boundary.

    Attributes:
        WAIT_FOR: Rerun WaitFor and everything after it.
        EXECUTE: Keep the WaitFor result and rerun Execute and everything after it.
    """

    WAIT_FOR = "wait_for"
    EXECUTE = "execute"


@dataclass(frozen=True)
class TimeTravelOptions:
    """Configure creation of a new run from existing Flow history.

    Exactly one selector matching ``type`` must be set. The Client
    validates combinations before sending the request.

    Attributes:
        type: The time travel point selector kind.
        history_event_time: Timestamp used by ``HISTORY_EVENT_TIME``.
        step_type: Registered Step type used by ``STEP_TYPE``.
        step_execution_id: Exact execution ID used by ``STEP_EXECUTION_ID``.
        step_method: WaitFor or Execute boundary required by ``STEP_EXECUTION_ID``.
        reason: Optional operator-readable time travel reason.
        skip_writes_reapply: Do not replay RPCs, Channel publications, or Attribute
            writes after the selected point.
    """

    type: TimeTravelType
    history_event_time: datetime | None = None
    step_type: str | None = None
    step_execution_id: str | None = None
    step_method: TimeTravelStepMethod | None = None
    reason: str | None = None
    skip_writes_reapply: bool = False


class StopType(Enum):
    """Select how an active Flow should close.

    Attributes:
        CANCEL: Request cooperative cancellation.
        TERMINATE: Force immediate termination.
        FAIL: Close the Flow as failed with the reason in :class:`StopFlowOptions`.
    """

    CANCEL = "cancel"
    TERMINATE = "terminate"
    FAIL = "fail"


@dataclass(frozen=True)
class StopFlowOptions:
    """Configure a ``stop_flow`` request.

    Attributes:
        type: The stop behavior; defaults to cooperative cancellation.
        reason: Optional operator-readable reason recorded by Dex.
    """

    type: StopType = StopType.CANCEL
    reason: str | None = None
