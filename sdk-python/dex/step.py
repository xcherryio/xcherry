# Legacy Materials in this file remain under their original licenses.
# See LEGACY_NOTICES.md.

# Modifications Copyright (c) 2026 Super Durable, Inc.
#
# Modifications after the Legacy Cutoff are licensed under the
# Sustainable Use License 1.0.
# Legacy Materials remain under their original licenses.
# See LICENSE and LEGACY_NOTICES.md.

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from datetime import timedelta
from enum import Enum
from typing import Any, Generator, Generic, Iterator, TypeVar

from dex.attribute import AttributeLock, AttributeMap, AttributeMapLoad
from dex.channel import Channel, ChannelMap, ChannelMapLoad
from dex.context import Context
from dex.dexpb import dex_pb2 as pb
from dex.wait import Wait

InputT = TypeVar("InputT")
StartT = TypeVar("StartT")
_NO_HEARTBEAT_VALUE = object()


class StepOutput:
    """Represent one progress frame yielded by a synchronous Step handler.

    Create outputs with :func:`heartbeat` or :meth:`dex.stream.Stream.write`.
    A synchronous generator yields these frames and returns its final Wait or
    StepDecision through the generator return value.
    """

    def __new__(cls, *args: object, **kwargs: object) -> StepOutput:
        """Reject direct construction of the abstract output marker."""
        if cls is StepOutput:
            raise TypeError("StepOutput must be created by heartbeat or Stream.write")
        return super().__new__(cls)


@dataclass(frozen=True)
class _HeartbeatStepOutput(StepOutput):
    has_value: bool
    value: object
    encoded_value: pb.Value | None = None


@dataclass(frozen=True)
class _StreamStepOutput(StepOutput):
    stream_write: pb.StepStreamWrite


def heartbeat(value: object = _NO_HEARTBEAT_VALUE) -> StepOutput:
    """Create a heartbeat frame for a synchronous Step generator.

    Yield ``heartbeat()`` to clear persisted heartbeat details. Passing a value,
    including ``None``, persists that codec-supported value for the next regular
    activity attempt. Local activity execution ignores heartbeat frames.

    Args:
        value: Optional checkpoint value. Omitting it clears prior details.

    Returns:
        A StepOutput that must be yielded by the current synchronous handler.

    Examples:
        >>> def execute(context: Context, input: int) -> Generator[StepOutput, None, StepDecision]:
        ...     yield heartbeat({"completed": input})
        ...     return graceful_complete()
    """
    return _HeartbeatStepOutput(value is not _NO_HEARTBEAT_VALUE, value)


class StepDurability(Enum):
    """Control when a Step handler result is durably acknowledged.

    Attributes:
        DEFAULT: Use FlowConfig, then the server's synchronous default.
        SYNC: Persist the result before acknowledging the Worker call.
        ASYNC: Allow acknowledgement before persistence completes.
    """

    DEFAULT = "default"
    SYNC = "sync"
    ASYNC = "async"


class WaitForFailurePolicy(Enum):
    """Control what Dex does when a Step's ``wait_for`` method fails.

    Attributes:
        FAIL_FLOW: Fail the Flow without invoking ``execute``.
        PROCEED: Invoke ``execute`` and expose the failure through Context.
            This also covers a SubFlow Condition that the server cannot start
            within its retry budget. ``execute`` then runs immediately with
            ``wait_for_method_failed()`` true. Each SubFlow that did not start
            reports ``FlowStatus.FAILED`` with
            ``FlowErrorType.SUB_FLOW_START_FAILED``. SubFlows that did start keep
            running, and their results stay nonterminal.
    """

    FAIL_FLOW = "fail_flow"
    PROCEED = "proceed"


@dataclass(frozen=True)
class RetryPolicy:
    """Configure exponential retries for a Step handler or Flow.

    A ``None`` duration or zero numeric value uses the server default. The default
    total duration is four hours.
    Attempts include the initial call. Retries stop at the first configured limit.
    With asynchronous Step durability, local and regular execution share attempts
    and elapsed duration. Fallback is immediate; later regular retries continue the
    cumulative backoff sequence.

    Attributes:
        initial_interval: Delay before the first retry.
        backoff_coefficient: Interval multiplier; zero uses the server default.
        maximum_interval: Upper bound for one retry delay.
        maximum_attempts: Total attempt limit; zero uses the server default.
        total_duration: Overall elapsed-time limit for all attempts; ``None`` uses
            four hours.
    """

    initial_interval: timedelta | None = None
    backoff_coefficient: float = 0.0
    maximum_interval: timedelta | None = None
    maximum_attempts: int = 0
    total_duration: timedelta | None = None


@dataclass(frozen=True)
class StepOptions:
    """Configure timeouts, retries, durability, locks, and failure routing for a Step.

    Fields set to ``None`` or ``DEFAULT`` use Flow or server policy. Attribute
    locks are acquired separately for ``wait_for`` and ``execute`` and must reference
    definitions in the containing Flow's ``PersistenceSchema``. Asynchronous
    durability first uses a local activity capped at seven seconds and three
    attempts; that phase ignores method and heartbeat timeouts before regular
    fallback applies the remaining retry budget.

    Attributes:
        wait_for_method_timeout: Maximum duration of one regular ``wait_for``
            attempt; ``None`` uses two hours.
        execute_method_timeout: Maximum duration of one regular ``execute``
            attempt; ``None`` uses two hours.
        heartbeat_timeout: Regular-activity progress deadline; ``None`` or zero uses
            one minute. The server-configured explicit minimum defaults to ten seconds.
        wait_for_retry: Optional retry policy for ``wait_for``.
        execute_retry: Optional retry policy for ``execute``.
        wait_for_failure: Behavior after all ``wait_for`` attempts fail.
        wait_for_durability: Durability used for the ``wait_for`` result.
        execute_durability: Durability used for the ``execute`` result.
        wait_for_lock_attributes: Attribute locks held during ``wait_for``.
        execute_lock_attributes: Attribute locks held during ``execute``.
        wait_for_load_attribute_maps: AttributeMaps fully loaded for ``wait_for``.
        wait_for_load_attribute_map_instances: Exact AttributeMap instances loaded for
            ``wait_for``.
        wait_for_load_channels: Channels whose pending messages load for ``wait_for``.
        wait_for_load_channel_maps: ChannelMaps fully loaded for ``wait_for``.
        wait_for_load_channel_map_instances: Exact ChannelMap instances loaded for
            ``wait_for``.
        execute_load_attribute_maps: AttributeMaps fully loaded for ``execute``.
        execute_load_attribute_map_instances: Exact AttributeMap instances loaded for
            ``execute``.
        execute_load_channels: Channels whose pending messages load for ``execute``.
        execute_load_channel_maps: ChannelMaps fully loaded for ``execute``.
        execute_load_channel_map_instances: Exact ChannelMap instances loaded for
            ``execute``.
    """

    wait_for_method_timeout: timedelta | None = None
    execute_method_timeout: timedelta | None = None
    heartbeat_timeout: timedelta | None = None
    wait_for_retry: RetryPolicy | None = None
    execute_retry: RetryPolicy | None = None
    wait_for_failure: WaitForFailurePolicy = WaitForFailurePolicy.FAIL_FLOW
    wait_for_durability: StepDurability = StepDurability.DEFAULT
    execute_durability: StepDurability = StepDurability.DEFAULT
    wait_for_lock_attributes: tuple[AttributeLock, ...] = ()
    execute_lock_attributes: tuple[AttributeLock, ...] = ()
    wait_for_load_attribute_maps: tuple[AttributeMap[Any], ...] = ()
    wait_for_load_attribute_map_instances: tuple[AttributeMapLoad, ...] = ()
    wait_for_load_channels: tuple[Channel[Any], ...] = ()
    wait_for_load_channel_maps: tuple[ChannelMap[Any], ...] = ()
    wait_for_load_channel_map_instances: tuple[ChannelMapLoad, ...] = ()
    execute_load_attribute_maps: tuple[AttributeMap[Any], ...] = ()
    execute_load_attribute_map_instances: tuple[AttributeMapLoad, ...] = ()
    execute_load_channels: tuple[Channel[Any], ...] = ()
    execute_load_channel_maps: tuple[ChannelMap[Any], ...] = ()
    execute_load_channel_map_instances: tuple[ChannelMapLoad, ...] = ()
    _execute_failure_target: type[Step[Any]] | None = None
    _execute_failure_options: StepOptions | None = None

    def on_execute_failure_proceed_to(
        self,
        step: type[Step[Any]],
        options: StepOptions | None = None,
    ) -> StepOptions:
        """Return a copy that routes exhausted ``execute`` retries to another Step.

        Args:
            step: The registered fallback Step.
            options: Optional options applied to the fallback movement.

        Returns:
            A new StepOptions object with failure routing configured.
        """
        return replace(
            self,
            _execute_failure_target=step,
            _execute_failure_options=options,
        )


class Step(Generic[InputT], ABC):
    """Define one typed unit of durable Flow application logic.

    Subclasses implement ``execute`` and may implement ``wait_for``. Handler methods
    must remain deterministic with respect to their inputs and Context state; external
    side effects belong behind idempotent application integrations.

    Examples:
        >>> class Charge(Step[ChargeInput]):
        ...     def execute(self, context: Context, input: ChargeInput) -> StepDecision:
        ...         status.set(context, "charged")
        ...         return graceful_complete(input.order_id)
    """

    @abstractmethod
    def execute(
        self,
        context: Context,
        input: InputT,
    ) -> StepDecision | Generator[StepOutput, None, StepDecision]:
        """Execute application logic and produce the next durable decision.

        An ordinary handler returns StepDecision directly. A progress handler yields
        heartbeat and Stream StepOutput values, then returns StepDecision as its
        generator return value.

        Args:
            context: Execution metadata and decision-local persistence operations.
            input: The decoded value passed by the incoming Step movement.

        Returns:
            A StepDecision, or a generator that yields StepOutput and returns the
            StepDecision describing movements or Flow completion.

        Raises:
            Exception: Application exceptions are retried according to StepOptions
                and may ultimately fail or reroute the Flow.
        """
        raise NotImplementedError

    def wait_for(
        self,
        context: Context,
        input: InputT,
    ) -> Wait | Generator[StepOutput, None, Wait]:
        """Describe durable conditions that must be ready before ``execute``.

        Override this method only when the Step waits. Dex skips the base
        implementation and invokes ``execute`` immediately. A progress handler
        yields heartbeat and Stream StepOutput values, then returns Wait as its
        generator return value.

        Args:
            context: Execution metadata and decision-local persistence operations.
            input: The same decoded input later passed to ``execute``.

        Returns:
            A durable Wait condition tree, or a generator that yields StepOutput
            and returns that Wait.
        """
        del context, input
        raise RuntimeError("framework must skip the default wait_for")

    def get_step_type(self) -> str:
        """Return the name used to register and address this Step.

        Override it to decouple the protocol Step type from the Python class name.

        Returns:
            A non-empty name unique within the containing Flow.
        """
        return type(self).__name__

    def get_step_options(self) -> StepOptions | None:
        """Return this Step's static execution options.

        Returns:
            Step-specific options, or ``None`` to use Flow and server defaults.
        """
        return None


@dataclass(frozen=True)
class _StepDef:
    step: Step[Any]
    is_start_step: bool


@dataclass(frozen=True)
class StepList(Generic[StartT]):
    """Build the ordered Step definitions for a Flow.

    A Flow may have zero or one starting Step. Other Steps are reachable only through
    decisions, RPC results, or failure routing.
    """

    _definitions: tuple[_StepDef, ...]

    @classmethod
    def empty(cls) -> StepList[StartT]:
        """Create a StepList with no starting or other Steps.

        Returns:
            An empty StepList.
        """
        return cls(())

    @staticmethod
    def start_step(step: Step[StartT]) -> StepList[StartT]:
        """Create a StepList whose first definition is the Flow starting Step.

        Args:
            step: The Step receiving ``start_flow`` input.

        Returns:
            A StepList with exactly one starting Step.
        """
        return StepList((_StepDef(step, True),))

    @classmethod
    def without_start_step(cls, *steps: Step[Any]) -> StepList[StartT]:
        """Create a StepList with only non-starting Steps.

        Args:
            *steps: Registered Steps reachable through other entry paths.

        Returns:
            A StepList that requires ``None`` start input.
        """
        return cls(tuple(_StepDef(step, False) for step in steps))

    def other_steps(self, *steps: Step[Any]) -> StepList[StartT]:
        """Return a copy with additional non-starting Steps appended.

        Args:
            *steps: Steps to append in registration order.

        Returns:
            A new StepList.
        """
        return StepList(
            self._definitions + tuple(_StepDef(step, False) for step in steps)
        )

    def __iter__(self) -> Iterator[_StepDef]:
        """Iterate internal definitions in stable registration order.

        Returns:
            An iterator over the registered Step definitions.
        """
        return iter(self._definitions)


@dataclass(frozen=True)
class StepMovement(Generic[InputT]):
    """Schedule one registered Step with typed input and optional options.

    Attributes:
        step: The destination Step class from the containing Registry.
        input: The value decoded by the destination Step's input codec.
        options: Optional per-movement StepOptions override.
    """

    step: type[Step[InputT]]
    input: InputT
    options: StepOptions | None = None

    @staticmethod
    def of(
        step: type[Step[InputT]],
        input: InputT,
        *,
        options: StepOptions | None = None,
    ) -> StepMovement[InputT]:
        """Create a typed movement to a Step.

        Args:
            step: The registered destination Step.
            input: A value compatible with the Step's input annotation.
            options: Optional per-movement options.

        Returns:
            A StepMovement.
        """
        return StepMovement(step, input, options)


class DecisionKind(Enum):
    NEXT = "next"
    GRACEFUL_COMPLETE = "graceful_complete"
    FORCE_COMPLETE = "force_complete"
    FORCE_COMPLETE_IF_CHANNELS_EMPTY = "force_complete_if_channels_empty"
    FORCE_FAIL = "force_fail"
    DEAD_END = "dead_end"


_NO_OUTPUT = object()


@dataclass(frozen=True)
class StepDecision:
    """Describe the durable state transition returned by ``Step.execute``.

    Prefer the module-level factory functions instead of constructing this class
    directly; they populate the fields required for each decision kind.

    Attributes:
        kind: The transition behavior interpreted by Dex.
        movements: Destination Steps for a next decision.
        output: Flow completion value, or an internal marker when omitted.
        reason: Human-readable force-failure reason.
        empty_channels: Channels that must be empty for conditional completion.
        fallback: Movement used when conditional completion cannot proceed.
        canceling_steps: Flow-wide Step type cancellation selectors.
        canceling_sibling_steps: Same-source Step type cancellation selectors.
    """

    kind: DecisionKind
    movements: tuple[StepMovement[Any], ...] = ()
    output: object = _NO_OUTPUT
    reason: str = ""
    empty_channels: tuple[Channel[Any] | ChannelMap[Any], ...] = ()
    fallback: StepMovement[Any] | None = None
    canceling_steps: tuple[type[Step[Any]], ...] = ()
    canceling_sibling_steps: tuple[type[Step[Any]], ...] = ()

    def _has_output(self) -> bool:
        return self.output is not _NO_OUTPUT

    def with_canceling_steps(self, *steps: type[Step[Any]]) -> StepDecision:
        """Return a decision canceling all current executions of selected Step types.

        Dex resolves one snapshot after this Execute succeeds. Finished, already-canceled,
        and absent executions are no-ops. Steps scheduled by this decision are excluded.
        Repeated calls take the union, and Flow-wide selection supersedes sibling selection.

        Args:
            *steps: Step classes registered with the current Flow.

        Returns:
            A new decision containing the combined Flow-wide selectors.
        """
        global_steps = _union_steps(self.canceling_steps, steps)
        sibling_steps = tuple(
            step
            for step in self.canceling_sibling_steps
            if all(step is not global_step for global_step in global_steps)
        )
        return replace(
            self,
            canceling_steps=global_steps,
            canceling_sibling_steps=sibling_steps,
        )

    def with_canceling_sibling_steps(self, *steps: type[Step[Any]]) -> StepDecision:
        """Return a decision canceling selected same-source Step executions.

        A sibling has the same ``Context.from_step_execution_id`` as the execution
        returning this decision. Snapshot and no-op behavior match
        ``with_canceling_steps``. Flow-wide selection wins for the same Step type.

        Args:
            *steps: Step classes registered with the current Flow.

        Returns:
            A new decision containing the combined sibling selectors.
        """
        siblings = _union_steps(self.canceling_sibling_steps, steps)
        siblings = tuple(
            step
            for step in siblings
            if all(step is not global_step for global_step in self.canceling_steps)
        )
        return replace(self, canceling_sibling_steps=siblings)


def _union_steps(
    existing: tuple[type[Step[Any]], ...],
    added: tuple[type[Step[Any]], ...],
) -> tuple[type[Step[Any]], ...]:
    combined = list(existing)
    for step in added:
        if all(step is not current for current in combined):
            combined.append(step)
    return tuple(combined)


def go_to(step: type[Step[InputT]], input: InputT) -> StepDecision:
    """Create a decision that schedules one next Step.

    Args:
        step: The registered destination Step.
        input: A value compatible with the destination input annotation.

    Returns:
        A next decision containing one StepMovement.
    """
    return go_to_many(StepMovement.of(step, input))


def go_to_many(*movements: StepMovement[Any]) -> StepDecision:
    """Create a decision that schedules several next Steps.

    Args:
        *movements: Typed destination movements applied in argument order.

    Returns:
        A next decision with all movements.
    """
    return StepDecision(DecisionKind.NEXT, movements=movements)


def graceful_complete(output: object = _NO_OUTPUT) -> StepDecision:
    """Request successful completion after already-scheduled Steps finish.

    Args:
        output: Optional codec-supported Flow result. Omit it for no output.

    Returns:
        A graceful-completion decision.
    """
    return StepDecision(DecisionKind.GRACEFUL_COMPLETE, output=output)


def force_complete(output: object = _NO_OUTPUT) -> StepDecision:
    """Request immediate successful completion of the Flow.

    Args:
        output: Optional codec-supported Flow result. Omit it for no output.

    Returns:
        A force-completion decision that does not await other active Steps.
    """
    return StepDecision(DecisionKind.FORCE_COMPLETE, output=output)


def force_complete_if_channels_empty(
    output: object,
    fallback: StepMovement[Any],
    *channels: Channel[Any] | ChannelMap[Any],
) -> StepDecision:
    """Complete only when selected Channels are empty, otherwise schedule fallback.

    Args:
        output: The codec-supported completion result.
        fallback: Movement scheduled when any selected Channel is non-empty.
        *channels: Registered Channel or ChannelMap definitions to inspect.

    Returns:
        A conditional force-completion decision.
    """
    return StepDecision(
        DecisionKind.FORCE_COMPLETE_IF_CHANNELS_EMPTY,
        output=output,
        empty_channels=channels,
        fallback=fallback,
    )


def force_fail(reason: str) -> StepDecision:
    """Request immediate Flow failure with an application reason.

    Args:
        reason: Non-empty human-readable failure detail.

    Returns:
        A force-failure decision.
    """
    return StepDecision(DecisionKind.FORCE_FAIL, reason=reason)


def dead_end() -> StepDecision:
    """End this execution path without scheduling work or closing the Flow.

    Returns:
        A dead-end decision; other active paths may continue.
    """
    return StepDecision(DecisionKind.DEAD_END)
