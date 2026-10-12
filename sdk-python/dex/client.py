# Legacy Materials in this file remain under their original licenses.
# See LEGACY_NOTICES.md.

# Modifications Copyright (c) 2026 Super Durable, Inc.
#
# Modifications after the Legacy Cutoff are licensed under the
# Sustainable Use License 1.0.
# Legacy Materials remain under their original licenses.
# See LICENSE and LEGACY_NOTICES.md.

from __future__ import annotations

from datetime import timedelta, timezone
from types import TracebackType
from typing import Any, Callable, TypeVar, cast, overload
from uuid import uuid4

import grpc

from dex._grpc_errors import FlowTargetRequirement, translate_rpc_error
from dex._utils import require_name
from dex._value_hydrator import ValueHydrator
from dex._value_mapper import ValueMapper
from dex._worker_dispatcher import WorkerDispatcher
from dex.attribute import Attribute, AttributeMap, _apply_attribute_store_sync
from dex.attribute_match import AttributeMatch, _encode_attribute_match
from dex.blob_cache import BlobCache
from dex.channel import Channel, ChannelMap
from dex.client_options import ClientOptions
from dex.context import Context
from dex.dexpb import dex_pb2 as pb
from dex.dexpb import dex_pb2_grpc
from dex.flow import Flow, Registry, RPCResult
from dex.flow_config import ActiveStepSearchMode, FlowConfig
from dex.flow_info import FlowInfo, FlowStatus, SearchFlowEntry, SearchFlowsPage
from dex.flow_options import (
    FlowTimeoutPolicy,
    IdReusePolicy,
    StartFlowOptions,
    StopFlowOptions,
    StopType,
    TimeTravelOptions,
    TimeTravelStepMethod,
    TimeTravelType,
    _resolve_flow_timeout_policy,
)
from dex.flow_result import FlowResult, flow_result_from_proto
from dex.runtime_errors import DexServiceError, FlowErrorType, LongPollTimeoutError
from dex.rpc_invoke_options import RPCInvokeOptions
from dex.step import RetryPolicy, StepDurability
from dex.step_execution import StepExecutionId, TimerId
from dex.stream import Stream, StreamMessage, StreamMessagesPage
from dex.wait_options import (
    WaitForAttributeOptions,
    WaitForStepCompletionOptions,
    _ClientRequestBudget,
    _duration_seconds32,
)

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")
ValueT = TypeVar("ValueT")


class Client:
    """Call Dex FlowService through registered, typed Flow definitions.

    Client methods are synchronous and may block on network I/O. Create one Client
    per Registry, share it across calling threads, and close it at application
    shutdown. The Client owns its gRPC channel but not the Registry or BlobCache.

    Attributes:
        registry: Flow definitions used to validate typed calls.
        blob_cache: The open cache used to hydrate large values.
        options: The effective ClientOptions.

    Examples:
        >>> with Client(registry, cache) as client:
        ...     run_id = client.start_flow(orders, "order-42", OrderInput("42"))
        ...     result = client.wait_for_flow("order-42").single_output(OrderResult)
    """

    def __init__(
        self,
        registry: Registry,
        blob_cache: BlobCache,
        options: ClientOptions | None = None,
    ) -> None:
        """Construct a Client and its lazy plaintext gRPC channel.

        Args:
            registry: The Flow Registry used for call validation and codecs.
            blob_cache: An open cache used to hydrate large response values.
            options: Service address and default Worker target; ``None`` uses defaults.
        """
        self.registry = registry
        self.blob_cache = blob_cache
        self.options = options or ClientOptions()
        self._channel = grpc.insecure_channel(self.options.server_address)
        self._service = dex_pb2_grpc.FlowServiceStub(  # type: ignore[no-untyped-call]
            self._channel
        )
        self._values = ValueMapper(registry.codec_registry)
        self._hydrator = ValueHydrator(self._service, blob_cache)
        self._mappings = WorkerDispatcher(
            registry,
            self._values,
            self._hydrator,
        )
        self._closed = False

    def __enter__(self) -> Client:
        """Return this open Client for context-manager use.

        Returns:
            This Client instance.
        """
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the Client when leaving a context-manager block.

        Args:
            exception_type: The active exception type, if any.
            exception: The active exception value, if any.
            traceback: The active traceback, if any.
        """
        self.close()

    def start_flow(
        self,
        flow: Flow[InputT],
        flow_id: str,
        input: InputT,
        options: StartFlowOptions = StartFlowOptions(),
    ) -> str:
        """Start a Flow and return its server-assigned run ID.

        The call returns after Dex accepts the Flow, not after completion. ``flow``
        must be the exact instance registered with this Client, and ``input`` must
        match its starting Step annotation. Options control timeout, reuse, retries,
        configuration, idempotency, and initial Attributes.

        Args:
            flow: The registered Flow instance to start.
            flow_id: A non-empty application ID stable across runs.
            input: The starting Step input, or ``None`` when no starting Step exists.
            options: Settings for the new Flow execution.

        Returns:
            The server-assigned run ID.

        Raises:
            FlowAlreadyStartedError: If reuse policy rejects an existing Flow ID.
            FlowDefinitionError: If ``flow`` is not registered.
            ValueMappingError: If ``input`` or an initial Attribute cannot be encoded.
            DexServiceError: If FlowService rejects or cannot perform the request.
        """
        registered = self.registry._flow_for_instance(flow)
        request = pb.StartFlowRequest(
            flow_id=require_name(flow_id),
            flow_type=registered.name,
            request_id=options.request_id or str(uuid4()),
            flow_start_options=self._map_start_options(options),
        )
        if registered.start_step is not None:
            start = registered.start_step
            request.start_step_type = start.name
            request.step_input.CopyFrom(self._values.encode(input, start.input_codec))
            step_options = self._mappings.map_step_options(
                registered,
                start.step.get_step_options(),
            )
            if step_options is not None:
                request.step_options.CopyFrom(step_options)
            request.step_options.skip_wait_for = start.skips_wait_for
        elif input is not None:
            raise ValueError("Flow without a start Step requires None input")
        if options.timeout is not None:
            request.flow_timeout_seconds = self._seconds32(options.timeout)
        timeout_policy = _resolve_flow_timeout_policy(
            registered.name,
            registered.has_timeout_handler,
            options.timeout,
            options.timeout_policy,
        )
        request.flow_timeout_policy = self._map_timeout_policy(timeout_policy)
        timeout_handler_options = self._mappings.map_flow_timeout_handler_options(
            registered,
            options.timeout,
            timeout_policy,
            options.timeout_handler_options,
        )
        if timeout_handler_options is not None:
            request.flow_start_options.timeout_handler_options.CopyFrom(
                timeout_handler_options
            )
        response = cast(
            pb.StartFlowResponse,
            self._call(self._service.StartFlow, request, "start_flow", flow_id, "none"),
        )
        return response.run_id

    @overload
    def invoke_rpc(
        self,
        rpc_method: Callable[[Context, InputT], RPCResult[OutputT]],
        flow_id: str,
        input: InputT,
        *,
        run_id: str = "",
        options: RPCInvokeOptions = RPCInvokeOptions(),
    ) -> OutputT: ...

    @overload
    def invoke_rpc(
        self,
        rpc_method: Callable[[Context], RPCResult[OutputT]],
        flow_id: str,
        *,
        run_id: str = "",
        options: RPCInvokeOptions = RPCInvokeOptions(),
    ) -> OutputT: ...

    @overload
    def invoke_rpc(
        self,
        rpc_method: Callable[[Context, InputT], None],
        flow_id: str,
        input: InputT,
        *,
        run_id: str = "",
        options: RPCInvokeOptions = RPCInvokeOptions(),
    ) -> None: ...

    @overload
    def invoke_rpc(
        self,
        rpc_method: Callable[[Context], None],
        flow_id: str,
        *,
        run_id: str = "",
        options: RPCInvokeOptions = RPCInvokeOptions(),
    ) -> None: ...

    def invoke_rpc(
        self,
        rpc_method: Callable[..., Any],
        flow_id: str,
        input: object = None,
        *,
        run_id: str = "",
        options: RPCInvokeOptions = RPCInvokeOptions(),
    ) -> Any:
        """Synchronously invoke a registered RPC on a Flow execution.

        Pass the bound method from the same registered Flow instance. RPC timeout and
        Attribute locks come from its ``@rpc`` decorator. The call blocks until the
        handler returns, fails, or times out. A non-transactional RPC without Attribute
        locks starts from a backend query. If the handler returns no durable effects, a
        retained terminal execution can serve that query. Locks, transactional execution,
        returned effects, or server policy can require an active execution.

        Args:
            rpc_method: A bound method decorated with ``@rpc``.
            flow_id: The non-empty target Flow ID.
            input: The annotated RPC input, or ``None`` for an input-free RPC.
            run_id: Optional exact run. With ``""``, the server resolves the current
                execution.
            options: Additive runtime AttributeMap locks and exact map-instance loads.

        Returns:
            The decoded ``RPCResult.output``, or ``None`` for a no-output RPC.

        Raises:
            FlowDefinitionError: If the method is not registered.
            RpcLockConflictError: If requested Attribute locks cannot be acquired.
            WorkerInvocationError: If the application handler fails.
            ValueMappingError: If input or output mapping fails.
            FlowNotActiveOrNotFoundError: If the Flow is missing or the selected path
                cannot use its closed execution.
            DexServiceError: If the service call otherwise fails.
        """
        registered_flow, rpc = self.registry._rpc_for_method(rpc_method)
        (
            invocation_locks,
            invocation_attribute_map_loads,
            invocation_channel_map_loads,
        ) = self.registry._resolve_rpc_invoke_options(registered_flow, options)
        encoded_input = (
            self._values.encode(input, rpc.input_codec)
            if rpc.input_codec is not None
            else self._values.encode_dynamic(None)
        )
        timeout = (
            self._seconds32(rpc.options.timeout)
            if rpc.options.timeout is not None
            else 0
        )
        response = cast(
            pb.InvokeRPCResponse,
            self._call(
                self._service.InvokeRPC,
                pb.InvokeRPCRequest(
                    flow_id=require_name(flow_id),
                    run_id=run_id,
                    rpc_name=rpc.name,
                    input=encoded_input,
                    timeout_seconds=timeout,
                    lock_attribute_keys=tuple(
                        sorted(set(rpc.locks + invocation_locks))
                    ),
                    request_id=str(uuid4()),
                    is_transactional=rpc.options.is_transactional,
                    load_attribute_map_instances=tuple(
                        sorted(
                            set(
                                rpc.load_attribute_map_instances
                                + invocation_attribute_map_loads
                            )
                        )
                    ),
                    load_channel_names=rpc.load_channel_names,
                    load_channel_map_instances=tuple(
                        sorted(
                            set(
                                rpc.load_channel_map_instances
                                + invocation_channel_map_loads
                            )
                        )
                    ),
                ),
                "invoke_rpc",
                flow_id,
                "active",
            ),
        )
        if rpc.output_codec is None:
            return None
        return self._values.decode(
            self._hydrator.hydrate(flow_id, response.output),
            rpc.output_codec,
        )

    def write_stream(
        self,
        flow_id: str,
        stream: Stream[ValueT],
        source: str,
        value: ValueT,
    ) -> None:
        """Append one typed best-effort Stream message with source metadata.

        Args:
            flow_id: Logical Flow instance ID; the Flow need not exist or be active.
            stream: Exact Stream object registered in one Flow schema.
            source: Non-empty informational source. Repeated values still append.
            value: Typed message to append.

        Raises:
            ValueError: If the Flow ID or source is empty.
            FlowDefinitionError: If the Stream is not registered.
            ValueMappingError: If the message cannot be encoded.
            DexServiceError: If FlowService cannot append the message.
        """
        require_name(source)
        flow = self.registry._flow_for_stream(stream)
        self._call(
            self._service.WriteStream,
            pb.WriteStreamRequest(
                flow_id=require_name(flow_id),
                flow_type=flow.name,
                stream_name=stream.name,
                stream_capacity_bytes=stream.stream_capacity_bytes,
                value=self._values.encode(
                    value,
                    self._values.codec(stream.value_type),
                ),
                source=source,
            ),
            "write_stream",
            flow_id,
            "none",
        )

    def read_stream(
        self,
        flow_id: str,
        stream: Stream[ValueT],
        resume_token: str = "",
        timeout: timedelta | None = None,
    ) -> StreamMessage[ValueT]:
        """Block for the next retained Stream message after a resume token.

        Args:
            flow_id: Logical Flow instance ID used as the Stream instance key.
            stream: Exact Stream object registered in one Flow schema.
            resume_token: Previous message token, or empty for the retained head.
            timeout: Optional non-negative server-side long-poll duration.

        Returns:
            The decoded message, next resume token, creation time, and source.

        Raises:
            LongPollTimeoutError: If no message arrives before the server wait expires.
            FlowDefinitionError: If the Stream is not registered.
            ValueMappingError: If the retained message cannot be decoded.
            DexServiceError: If FlowService cannot perform the read.
        """
        flow = self.registry._flow_for_stream(stream)
        response = cast(
            pb.ReadStreamResponse,
            self._call(
                self._service.ReadStream,
                pb.ReadStreamRequest(
                    flow_id=require_name(flow_id),
                    flow_type=flow.name,
                    stream_name=stream.name,
                    resume_token=resume_token,
                    wait_time_seconds=(
                        0 if timeout is None else self._seconds32(timeout)
                    ),
                ),
                "read_stream",
                flow_id,
                "none",
            ),
        )
        if (
            not response.HasField("message")
            or not response.message.HasField("value")
            or not response.message.HasField("created_time")
            or not response.message.resume_token
        ):
            raise ValueError("Dex returned an incomplete Stream message")
        return StreamMessage(
            self._values.decode(
                response.message.value,
                self._values.codec(stream.value_type),
            ),
            response.message.resume_token,
            response.message.created_time.ToDatetime(tzinfo=timezone.utc),
            response.message.source,
        )

    def list_stream_messages(
        self,
        flow_id: str,
        stream: Stream[ValueT],
        page_size: int,
        before_page_token: str = "",
    ) -> StreamMessagesPage[ValueT]:
        """Return one newest-first page of retained Stream messages without waiting.

        An empty token starts at the retained tail. Pass the returned page token
        unchanged to read older messages. Trimming may remove messages between pages.

        Args:
            flow_id: Logical Flow instance ID used as the Stream instance key.
            stream: Exact Stream object registered in one Flow schema.
            page_size: Positive maximum message count within the server-configured limit.
            before_page_token: Opaque token from the preceding page, or ``""`` first.

        Returns:
            Decoded messages in newest-first order and the next-page token.

        Raises:
            ValueError: If ``page_size`` is not positive.
            FlowDefinitionError: If the Stream is not registered.
            ValueMappingError: If a retained message cannot be decoded.
            DexServiceError: If FlowService cannot list the Stream.
        """
        if page_size < 1:
            raise ValueError("Stream page size must be positive")
        flow = self.registry._flow_for_stream(stream)
        response = cast(
            pb.ListStreamMessagesResponse,
            self._call(
                self._service.ListStreamMessages,
                pb.ListStreamMessagesRequest(
                    flow_id=require_name(flow_id),
                    flow_type=flow.name,
                    stream_name=stream.name,
                    page_size=page_size,
                    before_page_token=before_page_token,
                ),
                "list_stream_messages",
                flow_id,
                "none",
            ),
        )
        messages: list[StreamMessage[ValueT]] = []
        for message in response.messages:
            if (
                not message.HasField("value")
                or not message.HasField("created_time")
                or not message.resume_token
            ):
                raise ValueError("Dex returned an incomplete Stream message")
            messages.append(
                StreamMessage(
                    self._values.decode(
                        message.value,
                        self._values.codec(stream.value_type),
                    ),
                    message.resume_token,
                    message.created_time.ToDatetime(tzinfo=timezone.utc),
                    message.source,
                )
            )
        return StreamMessagesPage(messages, response.next_page_token)

    def wait_for_flow(
        self,
        flow_id: str,
        timeout: timedelta | None = None,
    ) -> FlowResult:
        """Block until a Flow closes and return its terminal result.

        ``timeout`` is the server-side long-poll duration. A long-poll timeout is
        retryable by calling this method again. Parallel completion order is not
        deterministic; select results by Step identity rather than position.

        Args:
            flow_id: The non-empty target Flow ID.
            timeout: Optional non-negative server-side wait duration.

        Returns:
            The terminal status, failure detail, and output-bearing completions.

        Raises:
            LongPollTimeoutError: If the Flow remains open when the wait expires.
            FlowNotFoundError: If ``flow_id`` does not exist.
            ValueMappingError: If Dex returns a malformed or incompatible hydrated value.
            DexServiceError: If FlowService cannot perform the wait.
        """
        response = self._wait_for_flow_response(flow_id, timeout)
        hydrated = self._hydrator.step_outputs(flow_id, list(response.results))
        mapped = pb.FlowResult()
        mapped.CopyFrom(response)
        del mapped.results[:]
        mapped.results.extend(hydrated)
        return flow_result_from_proto(
            mapped,
            lambda value, output_type: self._values.decode(
                value, self._values.codec(output_type)
            ),
        )

    def stop_flow(
        self,
        flow_id: str,
        options: StopFlowOptions = StopFlowOptions(),
    ) -> None:
        """Request cancellation, termination, or failure of an active Flow.

        The call returns after Dex accepts the request and does not wait for terminal
        status.

        Args:
            flow_id: The non-empty target Flow ID.
            options: Stop mode and optional recorded reason.

        Raises:
            FlowNotActiveOrNotFoundError: If no active run exists.
            DexServiceError: If FlowService rejects or cannot perform the request.
        """
        self._call(
            self._service.StopFlow,
            pb.StopFlowRequest(
                flow_id=require_name(flow_id),
                reason=options.reason or "",
                stop_type={
                    StopType.CANCEL: pb.STOP_TYPE_CANCEL,
                    StopType.TERMINATE: pb.STOP_TYPE_TERMINATE,
                    StopType.FAIL: pb.STOP_TYPE_FAIL,
                }[options.type],
            ),
            "stop_flow",
            flow_id,
            "active",
        )

    def describe_flow(self, flow_id: str) -> FlowInfo:
        """Return summary metadata for the current or latest Flow run.

        Args:
            flow_id: The non-empty Flow ID to describe.

        Returns:
            Flow ID, run ID, Flow type, status, and UTC start time.

        Raises:
            FlowNotFoundError: If ``flow_id`` does not exist.
            DexServiceError: If FlowService cannot return the summary.
        """
        response = cast(
            pb.GetFlowSummaryResponse,
            self._call(
                self._service.GetFlowSummary,
                pb.GetFlowSummaryRequest(flow_id=require_name(flow_id)),
                "describe_flow",
                flow_id,
                "existing",
            ),
        )
        return FlowInfo(
            response.flow_execution_id.flow_id,
            response.flow_execution_id.run_id,
            response.flow_type,
            self._map_flow_status(response.flow_status),
            response.start_time.ToDatetime(tzinfo=timezone.utc),
        )

    def search_flows(
        self,
        query: str,
        page_size: int,
        next_page_token: str = "",
        *,
        include_continued_as_new: bool = False,
    ) -> SearchFlowsPage:
        """Return one page of Flow runs matching a visibility query.

        Args:
            query: A Dex visibility query; an empty string matches server defaults.
            page_size: The non-negative maximum result count requested.
            next_page_token: Opaque token from the preceding page, or ``""`` first.
            include_continued_as_new: Include earlier runs without overriding query filters.

        Returns:
            Server-ordered entries with hydrated indexed Attributes and a next token.

        Raises:
            ValueError: If ``page_size`` is negative.
            ValueMappingError: If an indexed Attribute cannot be hydrated.
            DexServiceError: If FlowService cannot execute the query.
        """
        if page_size < 0:
            raise ValueError("search page size must not be negative")
        response = cast(
            pb.SearchFlowsResponse,
            self._call(
                self._service.SearchFlows,
                pb.SearchFlowsRequest(
                    query=query,
                    page_size=page_size,
                    next_page_token=next_page_token,
                    include_continued_as_new=include_continued_as_new,
                ),
                "search_flows",
                None,
                "none",
            ),
        )
        flows = [self._map_search_entry(entry) for entry in response.flow_runs]
        return SearchFlowsPage(flows, response.next_page_token)

    def _map_search_entry(self, entry: pb.SearchFlowsResponseEntry) -> SearchFlowEntry:
        attributes = {
            kv.key: self._values.to_value(
                self._hydrator.hydrate(entry.flow_id, kv.value)
            )
            for kv in entry.indexed_attributes
        }
        closed_at = (
            entry.close_time.ToDatetime(tzinfo=timezone.utc)
            if entry.HasField("close_time")
            else None
        )
        return SearchFlowEntry(
            entry.flow_id,
            entry.run_id,
            entry.flow_type,
            self._map_flow_status(entry.flow_status),
            entry.start_time.ToDatetime(tzinfo=timezone.utc),
            closed_at,
            attributes,
        )

    def time_travel(self, flow_id: str, options: TimeTravelOptions) -> str:
        """Create a new run from a selected point in existing Flow history.

        Args:
            flow_id: The non-empty Flow ID whose history supplies the new run.
            options: Time travel selector, reason, and replay controls.

        Returns:
            The new server-assigned run ID; the Flow ID is unchanged.

        Raises:
            FlowNotFoundError: If ``flow_id`` does not exist.
            ValueError: If time travel selector fields are invalid.
            DexServiceError: If FlowService cannot create the new run.
        """
        request = pb.ResetFlowRequest(
            flow_id=require_name(flow_id),
            reset_type={
                TimeTravelType.BEGINNING: pb.FLOW_RESET_TYPE_BEGINNING,
                TimeTravelType.HISTORY_EVENT_TIME: pb.FLOW_RESET_TYPE_HISTORY_EVENT_TIME,
                TimeTravelType.STEP_TYPE: pb.FLOW_RESET_TYPE_STEP_TYPE,
                TimeTravelType.STEP_EXECUTION_ID: pb.FLOW_RESET_TYPE_STEP_EXECUTION_ID,
            }[options.type],
            reason=options.reason or "",
            skip_writes_reapply=options.skip_writes_reapply,
        )
        if options.history_event_time is not None:
            request.history_event_time = options.history_event_time.isoformat()
        if options.step_type is not None:
            request.step_type = options.step_type
        if options.step_execution_id is not None:
            request.step_execution_id = options.step_execution_id
        if options.step_method is not None:
            request.step_method = {
                TimeTravelStepMethod.WAIT_FOR: pb.FLOW_RESET_STEP_METHOD_WAIT_FOR,
                TimeTravelStepMethod.EXECUTE: pb.FLOW_RESET_STEP_METHOD_EXECUTE,
            }[options.step_method]
        response = cast(
            pb.ResetFlowResponse,
            self._call(
                self._service.ResetFlow,
                request,
                "time_travel",
                flow_id,
                "existing",
            ),
        )
        return response.run_id

    def skip_timer(
        self,
        flow_id: str,
        step_execution_id: StepExecutionId,
        timer_id: TimerId,
    ) -> None:
        """Make one waiting Timer condition immediately ready.

        Args:
            flow_id: The non-empty active Flow ID.
            step_execution_id: The Step type and positive execution number.
            timer_id: Exactly one Timer condition ID or zero-based index.

        Raises:
            FlowNotActiveOrNotFoundError: If the Flow is missing or closed.
            ValueError: If an identifier is invalid.
            DexServiceError: If FlowService cannot skip the Timer.
        """
        request = pb.SkipTimerRequest(
            flow_id=require_name(flow_id),
            step_execution_id=(
                f"{step_execution_id.step_type}-{step_execution_id.number}"
            ),
        )
        if timer_id.condition_id is not None:
            request.timer_condition_id = timer_id.condition_id
        if timer_id.condition_index is not None:
            request.timer_condition_index = timer_id.condition_index
        self._call(self._service.SkipTimer, request, "skip_timer", flow_id, "active")

    def wait_for_step_completion(
        self,
        flow_id: str,
        step_execution_id: StepExecutionId,
        options: WaitForStepCompletionOptions,
    ) -> None:
        """Block until one Step execution completes or its request timeout expires.

        The server derives a stable Request ID from the Step execution when none
        is supplied. Request timeout covers transparent transport reattachments.
        Internal handler timeout controls Update generation rollover without ending
        this request.

        Args:
            flow_id: The non-empty active Flow ID.
            step_execution_id: The Step type and positive execution number.
            options: The optional Request ID, request timeout, and internal handler
                timeout controls.

        Raises:
            ValueError: If the Request ID or request timeout is invalid.
            RequestTimeoutError: If a positive request timeout expires first.
            FlowNotActiveOrNotFoundError: If the Flow is missing or closes first.
            DexServiceError: If FlowService cannot perform the wait.
        """
        request_budget = _ClientRequestBudget(options.request_timeout)
        internal_handler_timeout_seconds = _duration_seconds32(
            options.internal_handler_timeout
        )
        while True:
            request_attempt = request_budget.next_attempt(
                "wait_for_step_completion", flow_id
            )
            try:
                self._call(
                    self._service.WaitForStepCompletion,
                    pb.WaitForStepCompletionRequest(
                        flow_id=require_name(flow_id),
                        step_type=step_execution_id.step_type,
                        step_execution_number=str(step_execution_id.number),
                        request_timeout_seconds=request_attempt.request_timeout_seconds,
                        internal_handler_timeout_seconds=(
                            internal_handler_timeout_seconds
                        ),
                        request_id=options.request_id,
                    ),
                    "wait_for_step_completion",
                    flow_id,
                    "active",
                    request_attempt.transport_timeout_seconds,
                )
                return
            except LongPollTimeoutError:
                continue
            except DexServiceError as error:
                if request_budget.is_timeout(error.code):
                    raise request_budget.timeout_error(
                        "wait_for_step_completion", flow_id
                    ) from error
                raise

    @overload
    def wait_for_attribute_match(
        self,
        flow_id: str,
        attribute: Attribute[ValueT],
        match: AttributeMatch[ValueT],
        options: WaitForAttributeOptions,
    ) -> ValueT:
        """Wait for a singleton Attribute in the current run to satisfy ``match``.

        The Client returns the value observed by the successful wait. The server
        derives a stable Request ID from the condition when none is supplied.
        Request timeout covers transparent transport reattachments. Internal handler
        timeout controls Update generation rollover without ending this request.
        JSON, bytes, and null operands raise ``ValueError`` before transport.

        Args:
            flow_id: The non-empty active Flow ID.
            attribute: The registered singleton Attribute to observe.
            match: The scalar predicate to await.
            options: The optional Request ID, request timeout, and internal handler
                timeout controls.

        Returns:
            The current Attribute value that satisfied ``match``.

        Raises:
            ValueError: If an identifier, option, or match operand is invalid.
            RequestTimeoutError: If a positive request timeout expires first.
            FlowNotActiveOrNotFoundError: If the Flow is missing or closes first.
            DexServiceError: If FlowService cannot perform the wait.
        """
        ...

    @overload
    def wait_for_attribute_match(
        self,
        flow_id: str,
        attribute: AttributeMap[ValueT],
        instance: str,
        match: AttributeMatch[ValueT],
        options: WaitForAttributeOptions,
    ) -> ValueT:
        """Wait for one AttributeMap instance in the current run to match.

        ``instance`` is encoded as a map key. Match restrictions, Request ID,
        request-timeout and handler-generation behavior, and service errors match
        :meth:`wait_for_attribute_match`.

        Args:
            flow_id: The non-empty active Flow ID.
            attribute: The registered AttributeMap to observe.
            instance: The map instance to observe. Slash is prohibited because it is a reserved character.
            match: The scalar predicate to await.
            options: The optional Request ID override and total handler request timeout.

        Returns:
            The current AttributeMap value that satisfied ``match``.

        Raises:
            ValueError: If an identifier, option, or match operand is invalid.
            RequestTimeoutError: If a positive request timeout expires first.
            FlowNotActiveOrNotFoundError: If the Flow is missing or closes first.
            DexServiceError: If FlowService cannot perform the wait.
        """
        ...

    def wait_for_attribute_match(
        self,
        flow_id: str,
        attribute: Attribute[Any] | AttributeMap[Any],
        *args: object,
        **kwargs: object,
    ) -> Any:
        """Wait for a singleton Attribute or AttributeMap instance to match.

        Singleton form is ``wait_for_attribute_match(flow_id, attribute, match,
        options)``; map form adds ``instance`` before ``match``.

        Args:
            flow_id: The non-empty active Flow ID.
            attribute: The registered Attribute or AttributeMap to observe.
            *args: Positional match and options, optionally preceded by a map instance.
            **kwargs: The same arguments supplied by name.

        Returns:
            The current value that satisfied the match.

        Raises:
            TypeError: If arguments do not match the Attribute definition.
            ValueError: If an identifier, option, or expected value is invalid.
            RequestTimeoutError: If a positive request timeout expires first.
            FlowNotActiveOrNotFoundError: If the Flow is missing or closes first.
            DexServiceError: If FlowService cannot perform the wait.
        """
        instance, match, options = self._attribute_wait_arguments(
            attribute, args, kwargs
        )
        return self._wait_for_attribute_match(
            flow_id, attribute, instance, match, options
        )

    def _wait_for_attribute_match(
        self,
        flow_id: str,
        attribute: Attribute[Any] | AttributeMap[Any],
        instance: str | None,
        match: object,
        options: WaitForAttributeOptions,
    ) -> Any:
        encoded_match = _encode_attribute_match(
            cast(AttributeMatch[Any], match),
            self._values,
            self._values.codec(attribute.value_type),
        )
        encoded_match.key = self._definition_name(attribute, instance)
        request_budget = _ClientRequestBudget(options.request_timeout)
        internal_handler_timeout_seconds = _duration_seconds32(
            options.internal_handler_timeout
        )
        while True:
            request_attempt = request_budget.next_attempt(
                "wait_for_attribute_match", flow_id
            )
            try:
                response = cast(
                    pb.WaitForAttributeResponse,
                    self._call(
                        self._service.WaitForAttribute,
                        pb.WaitForAttributeRequest(
                            flow_id=require_name(flow_id),
                            match=encoded_match,
                            request_timeout_seconds=(
                                request_attempt.request_timeout_seconds
                            ),
                            internal_handler_timeout_seconds=(
                                internal_handler_timeout_seconds
                            ),
                            request_id=options.request_id,
                        ),
                        "wait_for_attribute_match",
                        flow_id,
                        "active",
                        request_attempt.transport_timeout_seconds,
                    ),
                )
                if not response.HasField("matched_value"):
                    raise ValueError("wait_for_attribute_match response is incomplete")
                return self._values.decode(
                    response.matched_value,
                    self._values.codec(attribute.value_type),
                )
            except LongPollTimeoutError:
                continue
            except DexServiceError as error:
                if request_budget.is_timeout(error.code):
                    raise request_budget.timeout_error(
                        "wait_for_attribute_match", flow_id
                    ) from error
                raise

    def update_flow_config(self, flow_id: str, config: FlowConfig) -> None:
        """Replace mutable configuration for an active Flow.

        The update affects later decisions and does not recall work already
        dispatched.

        Args:
            flow_id: The non-empty active Flow ID.
            config: The new optional configuration fields.

        Raises:
            FlowNotActiveOrNotFoundError: If the Flow is missing or closed.
            ValueError: If a configuration value is invalid.
            DexServiceError: If FlowService cannot apply the update.
        """
        self._call(
            self._service.UpdateFlowConfig,
            pb.UpdateFlowConfigRequest(
                flow_id=require_name(flow_id),
                flow_config=self._map_flow_config(config),
            ),
            "update_flow_config",
            flow_id,
            "active",
        )

    def trigger_continue_as_new(self, flow_id: str) -> None:
        """Ask an active Flow to roll history into a new run.

        The request returns after acceptance; the Flow ID remains stable and the run
        ID changes when rollover occurs.

        Args:
            flow_id: The non-empty active Flow ID.

        Raises:
            FlowNotActiveOrNotFoundError: If the Flow is missing or closed.
            DexServiceError: If FlowService cannot accept the request.
        """
        self._call(
            self._service.TriggerContinueAsNew,
            pb.TriggerContinueAsNewRequest(flow_id=require_name(flow_id)),
            "trigger_continue_as_new",
            flow_id,
            "active",
        )

    def health_check(self) -> bool:
        """Call the FlowService health endpoint.

        Returns:
            ``True`` when the service returns successfully.

        Raises:
            DexServiceError: If the service is unhealthy or unreachable.
        """
        from google.protobuf import empty_pb2

        self._call(
            self._service.HealthCheck,
            empty_pb2.Empty(),
            "health_check",
            None,
            "none",
        )
        return True

    def close(self) -> None:
        """Close the owned gRPC channel.

        ``close`` is idempotent. Other Client methods must not be called afterward.
        The Registry and BlobCache remain owned by the caller.
        """
        if self._closed:
            return
        self._closed = True
        self._channel.close()

    def _wait_for_flow_response(
        self,
        flow_id: str,
        timeout: timedelta | None,
    ) -> pb.FlowResult:
        request = pb.WaitForFlowRequest(
            flow_id=require_name(flow_id),
            needs_results=True,
        )
        if timeout is not None:
            request.wait_time_seconds = self._seconds32(timeout)
        response = cast(
            pb.FlowResult,
            self._call(
                self._service.WaitForFlow,
                request,
                "wait_for_flow",
                flow_id,
                "existing",
            ),
        )
        return response

    def _map_start_options(self, options: StartFlowOptions) -> pb.FlowStartOptions:
        mapped = pb.FlowStartOptions(
            id_reuse_policy={
                IdReusePolicy.DEFAULT: pb.ID_REUSE_POLICY_UNSPECIFIED,
                IdReusePolicy.ALLOW_IF_PREVIOUS_FAILED: (
                    pb.ID_REUSE_POLICY_ALLOW_IF_PREVIOUS_EXISTS_ABNORMALLY
                ),
                IdReusePolicy.ALLOW_IF_NOT_RUNNING: (
                    pb.ID_REUSE_POLICY_ALLOW_IF_NO_RUNNING
                ),
                IdReusePolicy.ALLOW_TERMINATE_IF_RUNNING: (
                    pb.ID_REUSE_POLICY_ALLOW_TERMINATE_IF_RUNNING
                ),
                IdReusePolicy.DISALLOW: pb.ID_REUSE_POLICY_DISALLOW_REUSE,
            }[options.id_reuse_policy],
            flow_already_started_options=pb.FlowAlreadyStartedOptions(
                ignore_already_started_error=options.ignore_already_started
            ),
        )
        if options.start_delay is not None:
            mapped.flow_start_delay_seconds = self._seconds32(options.start_delay)
        if options.retry_policy is not None:
            mapped.retry_policy.CopyFrom(self._map_flow_retry(options.retry_policy))
        for initialization in options._attribute_initializations:
            definition = initialization.definition
            key = self._definition_name(definition, initialization.instance)
            write = pb.AttributeWrite(
                key=key,
                value=self._values.encode(
                    initialization.value,
                    self._values.codec(definition.value_type),
                ),
            )
            _apply_attribute_store_sync(write, definition)
            mapped.attributes.append(write)
        if (
            options.config_override is not None
            or self.options.worker_target is not None
        ):
            mapped.flow_config_override.CopyFrom(
                self._map_flow_config(options.config_override)
            )
        return mapped

    def _map_flow_config(self, config: FlowConfig | None) -> pb.FlowConfig:
        mapped = pb.FlowConfig()
        if config is not None:
            if config.active_step_search_mode is not None:
                mapped.active_step_search_mode = {
                    ActiveStepSearchMode.DEFAULT: (
                        pb.ACTIVE_STEP_SEARCH_MODE_UNSPECIFIED
                    ),
                    ActiveStepSearchMode.ALL: (
                        pb.ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_ALL
                    ),
                    ActiveStepSearchMode.WITH_WAIT_FOR: (
                        pb.ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_STEPS_WITH_WAIT_FOR
                    ),
                    ActiveStepSearchMode.DISABLED: (
                        pb.ACTIVE_STEP_SEARCH_MODE_DISABLED
                    ),
                }[config.active_step_search_mode]
            if config.continue_as_new_threshold is not None:
                mapped.continue_as_new_threshold = config.continue_as_new_threshold
            if config.continue_as_new_page_size_bytes is not None:
                mapped.continue_as_new_page_size_in_bytes = (
                    config.continue_as_new_page_size_bytes
                )
            if config.step_durability is not None:
                mapped.step_durability = {
                    StepDurability.DEFAULT: pb.STEP_DURABILITY_UNSPECIFIED,
                    StepDurability.SYNC: pb.STEP_DURABILITY_SYNC,
                    StepDurability.ASYNC: pb.STEP_DURABILITY_ASYNC,
                }[config.step_durability]
            if config.attribute_store_names is not None:
                mapped.attribute_store_names.CopyFrom(
                    pb.AttributeStoreNames(names=config.attribute_store_names)
                )
        target = (
            config.worker_target
            if config is not None and config.worker_target is not None
            else self.options.worker_target
        )
        if target is not None:
            mapped.worker_target.CopyFrom(
                pb.WorkerTarget(
                    address=target.address,
                    is_headless_address=target.headless,
                )
            )
        return mapped

    @staticmethod
    def _map_flow_retry(retry: RetryPolicy) -> pb.FlowRetryPolicy:
        mapped = pb.FlowRetryPolicy(
            backoff_coefficient=retry.backoff_coefficient,
            maximum_attempts=retry.maximum_attempts,
        )
        if retry.initial_interval is not None:
            mapped.initial_interval_seconds = Client._seconds32(retry.initial_interval)
        if retry.maximum_interval is not None:
            mapped.maximum_interval_seconds = Client._seconds32(retry.maximum_interval)
        return mapped

    @staticmethod
    def _definition_name(
        definition: Attribute[Any] | AttributeMap[Any] | Channel[Any] | ChannelMap[Any],
        instance: str | None,
    ) -> str:
        if isinstance(definition, (AttributeMap, ChannelMap)):
            if instance is None:
                raise ValueError("dynamic definition requires an instance")
            return Registry.physical_name(definition.name, instance)
        if instance is not None:
            raise ValueError("static definition cannot use an instance")
        return definition.name

    @staticmethod
    def _attribute_wait_arguments(
        definition: Attribute[Any] | AttributeMap[Any],
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> tuple[str | None, object, WaitForAttributeOptions]:
        parameter_names: tuple[str, ...]
        if isinstance(definition, Attribute):
            parameter_names = ("match", "options")
        elif isinstance(definition, AttributeMap):
            parameter_names = ("instance", "match", "options")
        else:
            raise TypeError("wait_for_attribute_match received invalid arguments")
        if len(args) > len(parameter_names):
            raise TypeError("wait_for_attribute_match received invalid arguments")
        arguments = dict(zip(parameter_names, args))
        for name, value in kwargs.items():
            if name not in parameter_names or name in arguments:
                raise TypeError("wait_for_attribute_match received invalid arguments")
            arguments[name] = value
        if set(arguments) != set(parameter_names):
            raise TypeError("wait_for_attribute_match received invalid arguments")
        options = arguments["options"]
        if not isinstance(options, WaitForAttributeOptions):
            raise TypeError("wait_for_attribute_match received invalid arguments")
        if isinstance(definition, Attribute):
            return None, arguments["match"], options
        instance = arguments["instance"]
        if not isinstance(instance, str):
            raise TypeError("wait_for_attribute_match received invalid arguments")
        return instance, arguments["match"], options

    @staticmethod
    def _resolve_timeout_policy(
        registered: Any, options: StartFlowOptions
    ) -> pb.FlowTimeoutPolicy:
        policy = _resolve_flow_timeout_policy(
            registered.name,
            registered.has_timeout_handler,
            options.timeout,
            options.timeout_policy,
        )
        return {
            FlowTimeoutPolicy.DEFAULT: pb.FLOW_TIMEOUT_POLICY_UNSPECIFIED,
            FlowTimeoutPolicy.FAIL: pb.FLOW_TIMEOUT_POLICY_FAIL,
            FlowTimeoutPolicy.CANCEL: pb.FLOW_TIMEOUT_POLICY_CANCEL,
            FlowTimeoutPolicy.HANDLER: pb.FLOW_TIMEOUT_POLICY_HANDLER,
        }[policy]

    @staticmethod
    def _map_timeout_policy(policy: FlowTimeoutPolicy) -> pb.FlowTimeoutPolicy:
        return {
            FlowTimeoutPolicy.DEFAULT: pb.FLOW_TIMEOUT_POLICY_UNSPECIFIED,
            FlowTimeoutPolicy.FAIL: pb.FLOW_TIMEOUT_POLICY_FAIL,
            FlowTimeoutPolicy.CANCEL: pb.FLOW_TIMEOUT_POLICY_CANCEL,
            FlowTimeoutPolicy.HANDLER: pb.FLOW_TIMEOUT_POLICY_HANDLER,
        }[policy]

    @staticmethod
    def _seconds32(duration: timedelta) -> int:
        seconds = duration.total_seconds()
        if seconds < 0 or not seconds.is_integer() or seconds > 2**31 - 1:
            raise ValueError("duration must be whole seconds within int32")
        return int(seconds)

    @staticmethod
    def _map_flow_status(status: int) -> FlowStatus:
        statuses: dict[int, FlowStatus] = {
            int(pb.FLOW_STATUS_RUNNING): FlowStatus.RUNNING,
            int(pb.FLOW_STATUS_COMPLETED): FlowStatus.COMPLETED,
            int(pb.FLOW_STATUS_FAILED): FlowStatus.FAILED,
            int(pb.FLOW_STATUS_SERVER_SIDE_TIMEOUT_INTERNAL_ONLY): (
                FlowStatus.SERVER_SIDE_TIMEOUT_INTERNAL_ONLY
            ),
            int(pb.FLOW_STATUS_TERMINATED): FlowStatus.TERMINATED,
            int(pb.FLOW_STATUS_CANCELED): FlowStatus.CANCELED,
            int(pb.FLOW_STATUS_CONTINUED_AS_NEW): FlowStatus.CONTINUED_AS_NEW,
        }
        try:
            return statuses[status]
        except KeyError as error:
            raise ValueError(f"unknown Flow status {status}") from error

    @staticmethod
    def _map_flow_error_type(error_type: int) -> FlowErrorType | None:
        error_types: dict[int, FlowErrorType] = {
            int(pb.FLOW_ERROR_TYPE_STEP_DECISION_FAILING_FLOW): (
                FlowErrorType.STEP_DECISION_FAILED
            ),
            int(
                pb.FLOW_ERROR_TYPE_CLIENT_API_FAILING_FLOW
            ): FlowErrorType.CLIENT_API_FAILED,
            int(pb.FLOW_ERROR_TYPE_WORKER_API_FAIL): FlowErrorType.WORKER_API_FAILED,
            int(pb.FLOW_ERROR_TYPE_INVALID_USER_FLOW_CODE): (
                FlowErrorType.INVALID_USER_FLOW_CODE
            ),
            int(pb.FLOW_ERROR_TYPE_INTERNAL): FlowErrorType.INTERNAL,
            int(pb.FLOW_ERROR_TYPE_FLOW_TIMEOUT): FlowErrorType.FLOW_TIMEOUT,
        }
        return error_types.get(error_type)

    @staticmethod
    def _call(
        method: Callable[..., Any],
        request: Any,
        operation: str,
        flow_id: str | None,
        requirement: FlowTargetRequirement,
        timeout: float | None = None,
    ) -> Any:
        try:
            if timeout is None:
                return method(request)
            return method(request, timeout=timeout)
        except grpc.RpcError as error:
            raise translate_rpc_error(error, operation, flow_id, requirement) from error
