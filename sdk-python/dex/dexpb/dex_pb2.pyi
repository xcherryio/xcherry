import datetime

from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class IndexType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INDEX_TYPE_UNSPECIFIED: _ClassVar[IndexType]
    INDEX_TYPE_KEYWORD: _ClassVar[IndexType]
    INDEX_TYPE_TEXT: _ClassVar[IndexType]
    INDEX_TYPE_KEYWORD_ARRAY: _ClassVar[IndexType]
    INDEX_TYPE_INT: _ClassVar[IndexType]
    INDEX_TYPE_DOUBLE: _ClassVar[IndexType]
    INDEX_TYPE_BOOL: _ClassVar[IndexType]
    INDEX_TYPE_DATETIME: _ClassVar[IndexType]

class WaitForMethodFailurePolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    WAIT_FOR_METHOD_FAILURE_POLICY_UNSPECIFIED: _ClassVar[WaitForMethodFailurePolicy]
    WAIT_FOR_METHOD_FAILURE_POLICY_FAIL_FLOW_ON_FAILURE: _ClassVar[WaitForMethodFailurePolicy]
    WAIT_FOR_METHOD_FAILURE_POLICY_PROCEED_ON_FAILURE: _ClassVar[WaitForMethodFailurePolicy]

class ExecuteMethodFailurePolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EXECUTE_METHOD_FAILURE_POLICY_UNSPECIFIED: _ClassVar[ExecuteMethodFailurePolicy]
    EXECUTE_METHOD_FAILURE_POLICY_FAIL_FLOW_ON_EXECUTE_METHOD_FAILURE: _ClassVar[ExecuteMethodFailurePolicy]
    EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP: _ClassVar[ExecuteMethodFailurePolicy]

class IdReusePolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ID_REUSE_POLICY_UNSPECIFIED: _ClassVar[IdReusePolicy]
    ID_REUSE_POLICY_ALLOW_IF_PREVIOUS_EXISTS_ABNORMALLY: _ClassVar[IdReusePolicy]
    ID_REUSE_POLICY_ALLOW_IF_NO_RUNNING: _ClassVar[IdReusePolicy]
    ID_REUSE_POLICY_DISALLOW_REUSE: _ClassVar[IdReusePolicy]
    ID_REUSE_POLICY_ALLOW_TERMINATE_IF_RUNNING: _ClassVar[IdReusePolicy]

class ActiveStepSearchMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACTIVE_STEP_SEARCH_MODE_UNSPECIFIED: _ClassVar[ActiveStepSearchMode]
    ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_ALL: _ClassVar[ActiveStepSearchMode]
    ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_STEPS_WITH_WAIT_FOR: _ClassVar[ActiveStepSearchMode]
    ACTIVE_STEP_SEARCH_MODE_DISABLED: _ClassVar[ActiveStepSearchMode]

class StepDurability(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STEP_DURABILITY_UNSPECIFIED: _ClassVar[StepDurability]
    STEP_DURABILITY_SYNC: _ClassVar[StepDurability]
    STEP_DURABILITY_ASYNC: _ClassVar[StepDurability]

class FlowTimeoutPolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FLOW_TIMEOUT_POLICY_UNSPECIFIED: _ClassVar[FlowTimeoutPolicy]
    FLOW_TIMEOUT_POLICY_FAIL: _ClassVar[FlowTimeoutPolicy]
    FLOW_TIMEOUT_POLICY_CANCEL: _ClassVar[FlowTimeoutPolicy]
    FLOW_TIMEOUT_POLICY_HANDLER: _ClassVar[FlowTimeoutPolicy]

class StopType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STOP_TYPE_UNSPECIFIED: _ClassVar[StopType]
    STOP_TYPE_CANCEL: _ClassVar[StopType]
    STOP_TYPE_TERMINATE: _ClassVar[StopType]
    STOP_TYPE_FAIL: _ClassVar[StopType]

class FlowStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FLOW_STATUS_UNSPECIFIED: _ClassVar[FlowStatus]
    FLOW_STATUS_RUNNING: _ClassVar[FlowStatus]
    FLOW_STATUS_COMPLETED: _ClassVar[FlowStatus]
    FLOW_STATUS_FAILED: _ClassVar[FlowStatus]
    FLOW_STATUS_SERVER_SIDE_TIMEOUT_INTERNAL_ONLY: _ClassVar[FlowStatus]
    FLOW_STATUS_TERMINATED: _ClassVar[FlowStatus]
    FLOW_STATUS_CANCELED: _ClassVar[FlowStatus]
    FLOW_STATUS_CONTINUED_AS_NEW: _ClassVar[FlowStatus]

class FlowErrorType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FLOW_ERROR_TYPE_UNSPECIFIED: _ClassVar[FlowErrorType]
    FLOW_ERROR_TYPE_STEP_DECISION_FAILING_FLOW: _ClassVar[FlowErrorType]
    FLOW_ERROR_TYPE_CLIENT_API_FAILING_FLOW: _ClassVar[FlowErrorType]
    FLOW_ERROR_TYPE_WORKER_API_FAIL: _ClassVar[FlowErrorType]
    FLOW_ERROR_TYPE_INVALID_USER_FLOW_CODE: _ClassVar[FlowErrorType]
    FLOW_ERROR_TYPE_FLOW_TIMEOUT: _ClassVar[FlowErrorType]
    FLOW_ERROR_TYPE_INTERNAL: _ClassVar[FlowErrorType]

class PendingStepMethodPhase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PENDING_STEP_METHOD_PHASE_UNSPECIFIED: _ClassVar[PendingStepMethodPhase]
    PENDING_STEP_METHOD_PHASE_SCHEDULED: _ClassVar[PendingStepMethodPhase]
    PENDING_STEP_METHOD_PHASE_STARTED: _ClassVar[PendingStepMethodPhase]

class ActiveStepPhase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACTIVE_STEP_PHASE_UNSPECIFIED: _ClassVar[ActiveStepPhase]
    ACTIVE_STEP_PHASE_ACTIVE: _ClassVar[ActiveStepPhase]
    ACTIVE_STEP_PHASE_WAITING: _ClassVar[ActiveStepPhase]

class FlowResetType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FLOW_RESET_TYPE_UNSPECIFIED: _ClassVar[FlowResetType]
    FLOW_RESET_TYPE_BEGINNING: _ClassVar[FlowResetType]
    FLOW_RESET_TYPE_HISTORY_EVENT_TIME: _ClassVar[FlowResetType]
    FLOW_RESET_TYPE_STEP_TYPE: _ClassVar[FlowResetType]
    FLOW_RESET_TYPE_STEP_EXECUTION_ID: _ClassVar[FlowResetType]

class FlowResetStepMethod(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FLOW_RESET_STEP_METHOD_UNSPECIFIED: _ClassVar[FlowResetStepMethod]
    FLOW_RESET_STEP_METHOD_WAIT_FOR: _ClassVar[FlowResetStepMethod]
    FLOW_RESET_STEP_METHOD_EXECUTE: _ClassVar[FlowResetStepMethod]

class AttributeMatchOperator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ATTRIBUTE_MATCH_OPERATOR_UNSPECIFIED: _ClassVar[AttributeMatchOperator]
    ATTRIBUTE_MATCH_OPERATOR_EQUAL: _ClassVar[AttributeMatchOperator]
    ATTRIBUTE_MATCH_OPERATOR_NOT_EQUAL: _ClassVar[AttributeMatchOperator]
    ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN: _ClassVar[AttributeMatchOperator]
    ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN_OR_EQUAL: _ClassVar[AttributeMatchOperator]
    ATTRIBUTE_MATCH_OPERATOR_LESS_THAN: _ClassVar[AttributeMatchOperator]
    ATTRIBUTE_MATCH_OPERATOR_LESS_THAN_OR_EQUAL: _ClassVar[AttributeMatchOperator]

class ErrorSubStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ERROR_SUB_STATUS_UNSPECIFIED: _ClassVar[ErrorSubStatus]
    ERROR_SUB_STATUS_UNCATEGORIZED: _ClassVar[ErrorSubStatus]
    ERROR_SUB_STATUS_FLOW_ALREADY_STARTED: _ClassVar[ErrorSubStatus]
    ERROR_SUB_STATUS_FLOW_NOT_EXISTS: _ClassVar[ErrorSubStatus]
    ERROR_SUB_STATUS_WORKER_API_ERROR: _ClassVar[ErrorSubStatus]
    ERROR_SUB_STATUS_LONG_POLL_TIME_OUT: _ClassVar[ErrorSubStatus]
    ERROR_SUB_STATUS_CHANNEL_MESSAGE_NOT_FOUND: _ClassVar[ErrorSubStatus]
    ERROR_SUB_STATUS_REQUEST_TIMEOUT: _ClassVar[ErrorSubStatus]

class CloseDecisionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CLOSE_DECISION_TYPE_UNSPECIFIED: _ClassVar[CloseDecisionType]
    CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY: _ClassVar[CloseDecisionType]
    CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE: _ClassVar[CloseDecisionType]
    CLOSE_DECISION_TYPE_FORCE_COMPLETE: _ClassVar[CloseDecisionType]
    CLOSE_DECISION_TYPE_FORCE_FAIL: _ClassVar[CloseDecisionType]
    CLOSE_DECISION_TYPE_DEAD_END: _ClassVar[CloseDecisionType]

class WaitingConditionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    WAITING_CONDITION_TYPE_UNSPECIFIED: _ClassVar[WaitingConditionType]
    WAITING_CONDITION_TYPE_ALL_COMPLETED: _ClassVar[WaitingConditionType]
    WAITING_CONDITION_TYPE_ANY_COMPLETED: _ClassVar[WaitingConditionType]
    WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED: _ClassVar[WaitingConditionType]

class SubFlowReusePolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SUB_FLOW_REUSE_POLICY_UNSPECIFIED: _ClassVar[SubFlowReusePolicy]
    SUB_FLOW_REUSE_POLICY_ATTACH: _ClassVar[SubFlowReusePolicy]
    SUB_FLOW_REUSE_POLICY_RESTART_IF_PREVIOUS_EXITS_ABNORMALLY: _ClassVar[SubFlowReusePolicy]
    SUB_FLOW_REUSE_POLICY_ALWAYS_RESTART: _ClassVar[SubFlowReusePolicy]

class ConditionStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONDITION_STATUS_UNSPECIFIED: _ClassVar[ConditionStatus]
    CONDITION_STATUS_WAITING: _ClassVar[ConditionStatus]
    CONDITION_STATUS_COMPLETED: _ClassVar[ConditionStatus]

class InternalTimerStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INTERNAL_TIMER_STATUS_UNSPECIFIED: _ClassVar[InternalTimerStatus]
    INTERNAL_TIMER_STATUS_PENDING: _ClassVar[InternalTimerStatus]
    INTERNAL_TIMER_STATUS_FIRED: _ClassVar[InternalTimerStatus]
    INTERNAL_TIMER_STATUS_SKIPPED: _ClassVar[InternalTimerStatus]

class UpdateErrorType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    UPDATE_ERROR_TYPE_UNSPECIFIED: _ClassVar[UpdateErrorType]
    UPDATE_ERROR_TYPE_CONTINUE_AS_NEW_PREEMPTED: _ClassVar[UpdateErrorType]
    UPDATE_ERROR_TYPE_INVALID_ARGUMENT: _ClassVar[UpdateErrorType]
    UPDATE_ERROR_TYPE_FAILED_PRECONDITION: _ClassVar[UpdateErrorType]
    UPDATE_ERROR_TYPE_DEADLINE_EXCEEDED: _ClassVar[UpdateErrorType]
    UPDATE_ERROR_TYPE_RPC_ACQUIRE_LOCK_FAILURE: _ClassVar[UpdateErrorType]
    UPDATE_ERROR_TYPE_SERVER_INTERNAL: _ClassVar[UpdateErrorType]
    UPDATE_ERROR_TYPE_CHANNEL_MESSAGE_NOT_FOUND: _ClassVar[UpdateErrorType]

class SubFlowCompletionDeliveryStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SUB_FLOW_COMPLETION_DELIVERY_STATUS_UNSPECIFIED: _ClassVar[SubFlowCompletionDeliveryStatus]
    SUB_FLOW_COMPLETION_DELIVERY_STATUS_DELIVERED: _ClassVar[SubFlowCompletionDeliveryStatus]
    SUB_FLOW_COMPLETION_DELIVERY_STATUS_PARENT_CLOSED_OR_NOT_FOUND: _ClassVar[SubFlowCompletionDeliveryStatus]
INDEX_TYPE_UNSPECIFIED: IndexType
INDEX_TYPE_KEYWORD: IndexType
INDEX_TYPE_TEXT: IndexType
INDEX_TYPE_KEYWORD_ARRAY: IndexType
INDEX_TYPE_INT: IndexType
INDEX_TYPE_DOUBLE: IndexType
INDEX_TYPE_BOOL: IndexType
INDEX_TYPE_DATETIME: IndexType
WAIT_FOR_METHOD_FAILURE_POLICY_UNSPECIFIED: WaitForMethodFailurePolicy
WAIT_FOR_METHOD_FAILURE_POLICY_FAIL_FLOW_ON_FAILURE: WaitForMethodFailurePolicy
WAIT_FOR_METHOD_FAILURE_POLICY_PROCEED_ON_FAILURE: WaitForMethodFailurePolicy
EXECUTE_METHOD_FAILURE_POLICY_UNSPECIFIED: ExecuteMethodFailurePolicy
EXECUTE_METHOD_FAILURE_POLICY_FAIL_FLOW_ON_EXECUTE_METHOD_FAILURE: ExecuteMethodFailurePolicy
EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP: ExecuteMethodFailurePolicy
ID_REUSE_POLICY_UNSPECIFIED: IdReusePolicy
ID_REUSE_POLICY_ALLOW_IF_PREVIOUS_EXISTS_ABNORMALLY: IdReusePolicy
ID_REUSE_POLICY_ALLOW_IF_NO_RUNNING: IdReusePolicy
ID_REUSE_POLICY_DISALLOW_REUSE: IdReusePolicy
ID_REUSE_POLICY_ALLOW_TERMINATE_IF_RUNNING: IdReusePolicy
ACTIVE_STEP_SEARCH_MODE_UNSPECIFIED: ActiveStepSearchMode
ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_ALL: ActiveStepSearchMode
ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_STEPS_WITH_WAIT_FOR: ActiveStepSearchMode
ACTIVE_STEP_SEARCH_MODE_DISABLED: ActiveStepSearchMode
STEP_DURABILITY_UNSPECIFIED: StepDurability
STEP_DURABILITY_SYNC: StepDurability
STEP_DURABILITY_ASYNC: StepDurability
FLOW_TIMEOUT_POLICY_UNSPECIFIED: FlowTimeoutPolicy
FLOW_TIMEOUT_POLICY_FAIL: FlowTimeoutPolicy
FLOW_TIMEOUT_POLICY_CANCEL: FlowTimeoutPolicy
FLOW_TIMEOUT_POLICY_HANDLER: FlowTimeoutPolicy
STOP_TYPE_UNSPECIFIED: StopType
STOP_TYPE_CANCEL: StopType
STOP_TYPE_TERMINATE: StopType
STOP_TYPE_FAIL: StopType
FLOW_STATUS_UNSPECIFIED: FlowStatus
FLOW_STATUS_RUNNING: FlowStatus
FLOW_STATUS_COMPLETED: FlowStatus
FLOW_STATUS_FAILED: FlowStatus
FLOW_STATUS_SERVER_SIDE_TIMEOUT_INTERNAL_ONLY: FlowStatus
FLOW_STATUS_TERMINATED: FlowStatus
FLOW_STATUS_CANCELED: FlowStatus
FLOW_STATUS_CONTINUED_AS_NEW: FlowStatus
FLOW_ERROR_TYPE_UNSPECIFIED: FlowErrorType
FLOW_ERROR_TYPE_STEP_DECISION_FAILING_FLOW: FlowErrorType
FLOW_ERROR_TYPE_CLIENT_API_FAILING_FLOW: FlowErrorType
FLOW_ERROR_TYPE_WORKER_API_FAIL: FlowErrorType
FLOW_ERROR_TYPE_INVALID_USER_FLOW_CODE: FlowErrorType
FLOW_ERROR_TYPE_FLOW_TIMEOUT: FlowErrorType
FLOW_ERROR_TYPE_INTERNAL: FlowErrorType
PENDING_STEP_METHOD_PHASE_UNSPECIFIED: PendingStepMethodPhase
PENDING_STEP_METHOD_PHASE_SCHEDULED: PendingStepMethodPhase
PENDING_STEP_METHOD_PHASE_STARTED: PendingStepMethodPhase
ACTIVE_STEP_PHASE_UNSPECIFIED: ActiveStepPhase
ACTIVE_STEP_PHASE_ACTIVE: ActiveStepPhase
ACTIVE_STEP_PHASE_WAITING: ActiveStepPhase
FLOW_RESET_TYPE_UNSPECIFIED: FlowResetType
FLOW_RESET_TYPE_BEGINNING: FlowResetType
FLOW_RESET_TYPE_HISTORY_EVENT_TIME: FlowResetType
FLOW_RESET_TYPE_STEP_TYPE: FlowResetType
FLOW_RESET_TYPE_STEP_EXECUTION_ID: FlowResetType
FLOW_RESET_STEP_METHOD_UNSPECIFIED: FlowResetStepMethod
FLOW_RESET_STEP_METHOD_WAIT_FOR: FlowResetStepMethod
FLOW_RESET_STEP_METHOD_EXECUTE: FlowResetStepMethod
ATTRIBUTE_MATCH_OPERATOR_UNSPECIFIED: AttributeMatchOperator
ATTRIBUTE_MATCH_OPERATOR_EQUAL: AttributeMatchOperator
ATTRIBUTE_MATCH_OPERATOR_NOT_EQUAL: AttributeMatchOperator
ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN: AttributeMatchOperator
ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN_OR_EQUAL: AttributeMatchOperator
ATTRIBUTE_MATCH_OPERATOR_LESS_THAN: AttributeMatchOperator
ATTRIBUTE_MATCH_OPERATOR_LESS_THAN_OR_EQUAL: AttributeMatchOperator
ERROR_SUB_STATUS_UNSPECIFIED: ErrorSubStatus
ERROR_SUB_STATUS_UNCATEGORIZED: ErrorSubStatus
ERROR_SUB_STATUS_FLOW_ALREADY_STARTED: ErrorSubStatus
ERROR_SUB_STATUS_FLOW_NOT_EXISTS: ErrorSubStatus
ERROR_SUB_STATUS_WORKER_API_ERROR: ErrorSubStatus
ERROR_SUB_STATUS_LONG_POLL_TIME_OUT: ErrorSubStatus
ERROR_SUB_STATUS_CHANNEL_MESSAGE_NOT_FOUND: ErrorSubStatus
ERROR_SUB_STATUS_REQUEST_TIMEOUT: ErrorSubStatus
CLOSE_DECISION_TYPE_UNSPECIFIED: CloseDecisionType
CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY: CloseDecisionType
CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE: CloseDecisionType
CLOSE_DECISION_TYPE_FORCE_COMPLETE: CloseDecisionType
CLOSE_DECISION_TYPE_FORCE_FAIL: CloseDecisionType
CLOSE_DECISION_TYPE_DEAD_END: CloseDecisionType
WAITING_CONDITION_TYPE_UNSPECIFIED: WaitingConditionType
WAITING_CONDITION_TYPE_ALL_COMPLETED: WaitingConditionType
WAITING_CONDITION_TYPE_ANY_COMPLETED: WaitingConditionType
WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED: WaitingConditionType
SUB_FLOW_REUSE_POLICY_UNSPECIFIED: SubFlowReusePolicy
SUB_FLOW_REUSE_POLICY_ATTACH: SubFlowReusePolicy
SUB_FLOW_REUSE_POLICY_RESTART_IF_PREVIOUS_EXITS_ABNORMALLY: SubFlowReusePolicy
SUB_FLOW_REUSE_POLICY_ALWAYS_RESTART: SubFlowReusePolicy
CONDITION_STATUS_UNSPECIFIED: ConditionStatus
CONDITION_STATUS_WAITING: ConditionStatus
CONDITION_STATUS_COMPLETED: ConditionStatus
INTERNAL_TIMER_STATUS_UNSPECIFIED: InternalTimerStatus
INTERNAL_TIMER_STATUS_PENDING: InternalTimerStatus
INTERNAL_TIMER_STATUS_FIRED: InternalTimerStatus
INTERNAL_TIMER_STATUS_SKIPPED: InternalTimerStatus
UPDATE_ERROR_TYPE_UNSPECIFIED: UpdateErrorType
UPDATE_ERROR_TYPE_CONTINUE_AS_NEW_PREEMPTED: UpdateErrorType
UPDATE_ERROR_TYPE_INVALID_ARGUMENT: UpdateErrorType
UPDATE_ERROR_TYPE_FAILED_PRECONDITION: UpdateErrorType
UPDATE_ERROR_TYPE_DEADLINE_EXCEEDED: UpdateErrorType
UPDATE_ERROR_TYPE_RPC_ACQUIRE_LOCK_FAILURE: UpdateErrorType
UPDATE_ERROR_TYPE_SERVER_INTERNAL: UpdateErrorType
UPDATE_ERROR_TYPE_CHANNEL_MESSAGE_NOT_FOUND: UpdateErrorType
SUB_FLOW_COMPLETION_DELIVERY_STATUS_UNSPECIFIED: SubFlowCompletionDeliveryStatus
SUB_FLOW_COMPLETION_DELIVERY_STATUS_DELIVERED: SubFlowCompletionDeliveryStatus
SUB_FLOW_COMPLETION_DELIVERY_STATUS_PARENT_CLOSED_OR_NOT_FOUND: SubFlowCompletionDeliveryStatus

class ServerInfo(_message.Message):
    __slots__ = ("server_version", "minimum_supported_protocol_version", "current_protocol_version")
    SERVER_VERSION_FIELD_NUMBER: _ClassVar[int]
    MINIMUM_SUPPORTED_PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    CURRENT_PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    server_version: str
    minimum_supported_protocol_version: int
    current_protocol_version: int
    def __init__(self, server_version: _Optional[str] = ..., minimum_supported_protocol_version: _Optional[int] = ..., current_protocol_version: _Optional[int] = ...) -> None: ...

class Value(_message.Message):
    __slots__ = ("internal_blob_id_for_string_value", "internal_blob_id_for_obj_value", "string_value", "obj_value", "int_value", "double_value", "bool_value", "null_value")
    INTERNAL_BLOB_ID_FOR_STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    INTERNAL_BLOB_ID_FOR_OBJ_VALUE_FIELD_NUMBER: _ClassVar[int]
    STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    OBJ_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT_VALUE_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_VALUE_FIELD_NUMBER: _ClassVar[int]
    BOOL_VALUE_FIELD_NUMBER: _ClassVar[int]
    NULL_VALUE_FIELD_NUMBER: _ClassVar[int]
    internal_blob_id_for_string_value: str
    internal_blob_id_for_obj_value: str
    string_value: str
    obj_value: EncodedObject
    int_value: int
    double_value: float
    bool_value: bool
    null_value: _struct_pb2.NullValue
    def __init__(self, internal_blob_id_for_string_value: _Optional[str] = ..., internal_blob_id_for_obj_value: _Optional[str] = ..., string_value: _Optional[str] = ..., obj_value: _Optional[_Union[EncodedObject, _Mapping]] = ..., int_value: _Optional[int] = ..., double_value: _Optional[float] = ..., bool_value: _Optional[bool] = ..., null_value: _Optional[_Union[_struct_pb2.NullValue, str]] = ...) -> None: ...

class EncodedObject(_message.Message):
    __slots__ = ("encoding", "payload")
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    encoding: str
    payload: bytes
    def __init__(self, encoding: _Optional[str] = ..., payload: _Optional[bytes] = ...) -> None: ...

class AttributeWrite(_message.Message):
    __slots__ = ("key", "value", "index_config", "sync_config")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    INDEX_CONFIG_FIELD_NUMBER: _ClassVar[int]
    SYNC_CONFIG_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: Value
    index_config: IndexConfig
    sync_config: AttributeSyncConfig
    def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ..., index_config: _Optional[_Union[IndexConfig, _Mapping]] = ..., sync_config: _Optional[_Union[AttributeSyncConfig, _Mapping]] = ...) -> None: ...

class ActionPermissionMapping(_message.Message):
    __slots__ = ("attribute_key", "equal_values", "required_permission")
    ATTRIBUTE_KEY_FIELD_NUMBER: _ClassVar[int]
    EQUAL_VALUES_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_PERMISSION_FIELD_NUMBER: _ClassVar[int]
    attribute_key: str
    equal_values: _containers.RepeatedCompositeFieldContainer[Value]
    required_permission: str
    def __init__(self, attribute_key: _Optional[str] = ..., equal_values: _Optional[_Iterable[_Union[Value, _Mapping]]] = ..., required_permission: _Optional[str] = ...) -> None: ...

class ActionPermissionMappings(_message.Message):
    __slots__ = ("mappings",)
    MAPPINGS_FIELD_NUMBER: _ClassVar[int]
    mappings: _containers.RepeatedCompositeFieldContainer[ActionPermissionMapping]
    def __init__(self, mappings: _Optional[_Iterable[_Union[ActionPermissionMapping, _Mapping]]] = ...) -> None: ...

class AttributeSyncConfig(_message.Message):
    __slots__ = ("enabled",)
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    def __init__(self, enabled: _Optional[bool] = ...) -> None: ...

class KV(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: Value
    def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class IndexConfig(_message.Message):
    __slots__ = ("enable", "type", "index_key")
    ENABLE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    INDEX_KEY_FIELD_NUMBER: _ClassVar[int]
    enable: bool
    type: IndexType
    index_key: str
    def __init__(self, enable: _Optional[bool] = ..., type: _Optional[_Union[IndexType, str]] = ..., index_key: _Optional[str] = ...) -> None: ...

class Context(_message.Message):
    __slots__ = ("flow_id", "run_id", "flow_started_timestamp", "step_execution_id", "first_attempt_timestamp", "attempt", "from_step_execution_id", "recovery_error", "last_heartbeat_value")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    FLOW_STARTED_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    FIRST_ATTEMPT_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    FROM_STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    RECOVERY_ERROR_FIELD_NUMBER: _ClassVar[int]
    LAST_HEARTBEAT_VALUE_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    flow_started_timestamp: int
    step_execution_id: str
    first_attempt_timestamp: int
    attempt: int
    from_step_execution_id: str
    recovery_error: RecoveryErrorInfo
    last_heartbeat_value: Value
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., flow_started_timestamp: _Optional[int] = ..., step_execution_id: _Optional[str] = ..., first_attempt_timestamp: _Optional[int] = ..., attempt: _Optional[int] = ..., from_step_execution_id: _Optional[str] = ..., recovery_error: _Optional[_Union[RecoveryErrorInfo, _Mapping]] = ..., last_heartbeat_value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class LocalActivityMetadata(_message.Message):
    __slots__ = ("current_step_execution_id", "from_step_execution_id")
    CURRENT_STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    current_step_execution_id: str
    from_step_execution_id: str
    def __init__(self, current_step_execution_id: _Optional[str] = ..., from_step_execution_id: _Optional[str] = ...) -> None: ...

class RetryPolicy(_message.Message):
    __slots__ = ("initial_interval_seconds", "backoff_coefficient", "maximum_interval_seconds", "maximum_attempts", "total_duration_seconds")
    INITIAL_INTERVAL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_COEFFICIENT_FIELD_NUMBER: _ClassVar[int]
    MAXIMUM_INTERVAL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    MAXIMUM_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_DURATION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    initial_interval_seconds: int
    backoff_coefficient: float
    maximum_interval_seconds: int
    maximum_attempts: int
    total_duration_seconds: int
    def __init__(self, initial_interval_seconds: _Optional[int] = ..., backoff_coefficient: _Optional[float] = ..., maximum_interval_seconds: _Optional[int] = ..., maximum_attempts: _Optional[int] = ..., total_duration_seconds: _Optional[int] = ...) -> None: ...

class FlowRetryPolicy(_message.Message):
    __slots__ = ("initial_interval_seconds", "backoff_coefficient", "maximum_interval_seconds", "maximum_attempts")
    INITIAL_INTERVAL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_COEFFICIENT_FIELD_NUMBER: _ClassVar[int]
    MAXIMUM_INTERVAL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    MAXIMUM_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    initial_interval_seconds: int
    backoff_coefficient: float
    maximum_interval_seconds: int
    maximum_attempts: int
    def __init__(self, initial_interval_seconds: _Optional[int] = ..., backoff_coefficient: _Optional[float] = ..., maximum_interval_seconds: _Optional[int] = ..., maximum_attempts: _Optional[int] = ...) -> None: ...

class StepOptions(_message.Message):
    __slots__ = ("wait_for_timeout_seconds", "execute_timeout_seconds", "wait_for_retry_policy", "execute_retry_policy", "wait_for_failure_policy", "execute_failure_policy", "execute_failure_proceed_step_type", "execute_failure_proceed_step_options", "skip_wait_for", "wait_for_durability_override", "execute_durability_override", "wait_for_lock_attribute_keys", "execute_lock_attribute_keys", "heartbeat_timeout_seconds", "wait_for_load_attribute_map_instances", "wait_for_load_channel_names", "wait_for_load_channel_map_instances", "execute_load_attribute_map_instances", "execute_load_channel_names", "execute_load_channel_map_instances")
    WAIT_FOR_TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    WAIT_FOR_RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    WAIT_FOR_FAILURE_POLICY_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_FAILURE_POLICY_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_FAILURE_PROCEED_STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_FAILURE_PROCEED_STEP_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SKIP_WAIT_FOR_FIELD_NUMBER: _ClassVar[int]
    WAIT_FOR_DURABILITY_OVERRIDE_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_DURABILITY_OVERRIDE_FIELD_NUMBER: _ClassVar[int]
    WAIT_FOR_LOCK_ATTRIBUTE_KEYS_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_LOCK_ATTRIBUTE_KEYS_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    WAIT_FOR_LOAD_ATTRIBUTE_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    WAIT_FOR_LOAD_CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    WAIT_FOR_LOAD_CHANNEL_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_LOAD_ATTRIBUTE_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_LOAD_CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_LOAD_CHANNEL_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    wait_for_timeout_seconds: int
    execute_timeout_seconds: int
    wait_for_retry_policy: RetryPolicy
    execute_retry_policy: RetryPolicy
    wait_for_failure_policy: WaitForMethodFailurePolicy
    execute_failure_policy: ExecuteMethodFailurePolicy
    execute_failure_proceed_step_type: str
    execute_failure_proceed_step_options: StepOptions
    skip_wait_for: bool
    wait_for_durability_override: StepDurability
    execute_durability_override: StepDurability
    wait_for_lock_attribute_keys: _containers.RepeatedScalarFieldContainer[str]
    execute_lock_attribute_keys: _containers.RepeatedScalarFieldContainer[str]
    heartbeat_timeout_seconds: int
    wait_for_load_attribute_map_instances: _containers.RepeatedScalarFieldContainer[str]
    wait_for_load_channel_names: _containers.RepeatedScalarFieldContainer[str]
    wait_for_load_channel_map_instances: _containers.RepeatedScalarFieldContainer[str]
    execute_load_attribute_map_instances: _containers.RepeatedScalarFieldContainer[str]
    execute_load_channel_names: _containers.RepeatedScalarFieldContainer[str]
    execute_load_channel_map_instances: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, wait_for_timeout_seconds: _Optional[int] = ..., execute_timeout_seconds: _Optional[int] = ..., wait_for_retry_policy: _Optional[_Union[RetryPolicy, _Mapping]] = ..., execute_retry_policy: _Optional[_Union[RetryPolicy, _Mapping]] = ..., wait_for_failure_policy: _Optional[_Union[WaitForMethodFailurePolicy, str]] = ..., execute_failure_policy: _Optional[_Union[ExecuteMethodFailurePolicy, str]] = ..., execute_failure_proceed_step_type: _Optional[str] = ..., execute_failure_proceed_step_options: _Optional[_Union[StepOptions, _Mapping]] = ..., skip_wait_for: _Optional[bool] = ..., wait_for_durability_override: _Optional[_Union[StepDurability, str]] = ..., execute_durability_override: _Optional[_Union[StepDurability, str]] = ..., wait_for_lock_attribute_keys: _Optional[_Iterable[str]] = ..., execute_lock_attribute_keys: _Optional[_Iterable[str]] = ..., heartbeat_timeout_seconds: _Optional[int] = ..., wait_for_load_attribute_map_instances: _Optional[_Iterable[str]] = ..., wait_for_load_channel_names: _Optional[_Iterable[str]] = ..., wait_for_load_channel_map_instances: _Optional[_Iterable[str]] = ..., execute_load_attribute_map_instances: _Optional[_Iterable[str]] = ..., execute_load_channel_names: _Optional[_Iterable[str]] = ..., execute_load_channel_map_instances: _Optional[_Iterable[str]] = ...) -> None: ...

class FlowTimeoutHandlerOptions(_message.Message):
    __slots__ = ("method_timeout_seconds", "heartbeat_timeout_seconds", "retry_policy", "failure_policy", "failure_proceed_step_type", "failure_proceed_step_options", "durability_override", "lock_attribute_keys", "load_attribute_map_instances", "load_channel_names", "load_channel_map_instances")
    METHOD_TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    FAILURE_POLICY_FIELD_NUMBER: _ClassVar[int]
    FAILURE_PROCEED_STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_PROCEED_STEP_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_OVERRIDE_FIELD_NUMBER: _ClassVar[int]
    LOCK_ATTRIBUTE_KEYS_FIELD_NUMBER: _ClassVar[int]
    LOAD_ATTRIBUTE_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    LOAD_CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    LOAD_CHANNEL_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    method_timeout_seconds: int
    heartbeat_timeout_seconds: int
    retry_policy: RetryPolicy
    failure_policy: ExecuteMethodFailurePolicy
    failure_proceed_step_type: str
    failure_proceed_step_options: StepOptions
    durability_override: StepDurability
    lock_attribute_keys: _containers.RepeatedScalarFieldContainer[str]
    load_attribute_map_instances: _containers.RepeatedScalarFieldContainer[str]
    load_channel_names: _containers.RepeatedScalarFieldContainer[str]
    load_channel_map_instances: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, method_timeout_seconds: _Optional[int] = ..., heartbeat_timeout_seconds: _Optional[int] = ..., retry_policy: _Optional[_Union[RetryPolicy, _Mapping]] = ..., failure_policy: _Optional[_Union[ExecuteMethodFailurePolicy, str]] = ..., failure_proceed_step_type: _Optional[str] = ..., failure_proceed_step_options: _Optional[_Union[StepOptions, _Mapping]] = ..., durability_override: _Optional[_Union[StepDurability, str]] = ..., lock_attribute_keys: _Optional[_Iterable[str]] = ..., load_attribute_map_instances: _Optional[_Iterable[str]] = ..., load_channel_names: _Optional[_Iterable[str]] = ..., load_channel_map_instances: _Optional[_Iterable[str]] = ...) -> None: ...

class FlowAlreadyStartedOptions(_message.Message):
    __slots__ = ("ignore_already_started_error",)
    IGNORE_ALREADY_STARTED_ERROR_FIELD_NUMBER: _ClassVar[int]
    ignore_already_started_error: bool
    def __init__(self, ignore_already_started_error: _Optional[bool] = ...) -> None: ...

class FlowStartOptions(_message.Message):
    __slots__ = ("id_reuse_policy", "flow_start_delay_seconds", "retry_policy", "attributes", "flow_config_override", "flow_already_started_options", "timeout_handler_options")
    ID_REUSE_POLICY_FIELD_NUMBER: _ClassVar[int]
    FLOW_START_DELAY_SECONDS_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    FLOW_CONFIG_OVERRIDE_FIELD_NUMBER: _ClassVar[int]
    FLOW_ALREADY_STARTED_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_HANDLER_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    id_reuse_policy: IdReusePolicy
    flow_start_delay_seconds: int
    retry_policy: FlowRetryPolicy
    attributes: _containers.RepeatedCompositeFieldContainer[AttributeWrite]
    flow_config_override: FlowConfig
    flow_already_started_options: FlowAlreadyStartedOptions
    timeout_handler_options: FlowTimeoutHandlerOptions
    def __init__(self, id_reuse_policy: _Optional[_Union[IdReusePolicy, str]] = ..., flow_start_delay_seconds: _Optional[int] = ..., retry_policy: _Optional[_Union[FlowRetryPolicy, _Mapping]] = ..., attributes: _Optional[_Iterable[_Union[AttributeWrite, _Mapping]]] = ..., flow_config_override: _Optional[_Union[FlowConfig, _Mapping]] = ..., flow_already_started_options: _Optional[_Union[FlowAlreadyStartedOptions, _Mapping]] = ..., timeout_handler_options: _Optional[_Union[FlowTimeoutHandlerOptions, _Mapping]] = ...) -> None: ...

class FlowConfig(_message.Message):
    __slots__ = ("active_step_search_mode", "continue_as_new_threshold", "continue_as_new_page_size_in_bytes", "step_durability", "worker_target", "attribute_store_names")
    ACTIVE_STEP_SEARCH_MODE_FIELD_NUMBER: _ClassVar[int]
    CONTINUE_AS_NEW_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    CONTINUE_AS_NEW_PAGE_SIZE_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    STEP_DURABILITY_FIELD_NUMBER: _ClassVar[int]
    WORKER_TARGET_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_STORE_NAMES_FIELD_NUMBER: _ClassVar[int]
    active_step_search_mode: ActiveStepSearchMode
    continue_as_new_threshold: int
    continue_as_new_page_size_in_bytes: int
    step_durability: StepDurability
    worker_target: WorkerTarget
    attribute_store_names: AttributeStoreNames
    def __init__(self, active_step_search_mode: _Optional[_Union[ActiveStepSearchMode, str]] = ..., continue_as_new_threshold: _Optional[int] = ..., continue_as_new_page_size_in_bytes: _Optional[int] = ..., step_durability: _Optional[_Union[StepDurability, str]] = ..., worker_target: _Optional[_Union[WorkerTarget, _Mapping]] = ..., attribute_store_names: _Optional[_Union[AttributeStoreNames, _Mapping]] = ...) -> None: ...

class AttributeStoreNames(_message.Message):
    __slots__ = ("names",)
    NAMES_FIELD_NUMBER: _ClassVar[int]
    names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, names: _Optional[_Iterable[str]] = ...) -> None: ...

class WorkerTarget(_message.Message):
    __slots__ = ("address", "is_headless_address")
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    IS_HEADLESS_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    address: str
    is_headless_address: bool
    def __init__(self, address: _Optional[str] = ..., is_headless_address: _Optional[bool] = ...) -> None: ...

class StartFlowRequest(_message.Message):
    __slots__ = ("flow_id", "flow_type", "flow_timeout_seconds", "flow_timeout_policy", "start_step_type", "step_input", "step_options", "flow_start_options", "request_id")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    FLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    FLOW_TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    FLOW_TIMEOUT_POLICY_FIELD_NUMBER: _ClassVar[int]
    START_STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    STEP_INPUT_FIELD_NUMBER: _ClassVar[int]
    STEP_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    FLOW_START_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    flow_type: str
    flow_timeout_seconds: int
    flow_timeout_policy: FlowTimeoutPolicy
    start_step_type: str
    step_input: Value
    step_options: StepOptions
    flow_start_options: FlowStartOptions
    request_id: str
    def __init__(self, flow_id: _Optional[str] = ..., flow_type: _Optional[str] = ..., flow_timeout_seconds: _Optional[int] = ..., flow_timeout_policy: _Optional[_Union[FlowTimeoutPolicy, str]] = ..., start_step_type: _Optional[str] = ..., step_input: _Optional[_Union[Value, _Mapping]] = ..., step_options: _Optional[_Union[StepOptions, _Mapping]] = ..., flow_start_options: _Optional[_Union[FlowStartOptions, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class StartFlowResponse(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class PublishToChannelRequest(_message.Message):
    __slots__ = ("flow_id", "run_id", "messages")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    messages: _containers.RepeatedCompositeFieldContainer[ChannelMessage]
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., messages: _Optional[_Iterable[_Union[ChannelMessage, _Mapping]]] = ...) -> None: ...

class ChannelMessage(_message.Message):
    __slots__ = ("channel_name", "value", "message_id")
    CHANNEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    channel_name: str
    value: Value
    message_id: str
    def __init__(self, channel_name: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ..., message_id: _Optional[str] = ...) -> None: ...

class GetChannelMessagesRequest(_message.Message):
    __slots__ = ("flow_id", "run_id", "channel_name")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_NAME_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    channel_name: str
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., channel_name: _Optional[str] = ...) -> None: ...

class GetChannelMessagesResponse(_message.Message):
    __slots__ = ("messages",)
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[ChannelMessage]
    def __init__(self, messages: _Optional[_Iterable[_Union[ChannelMessage, _Mapping]]] = ...) -> None: ...

class DeleteChannelMessageRequest(_message.Message):
    __slots__ = ("flow_id", "run_id", "channel_name", "message_id", "request_id")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    channel_name: str
    message_id: str
    request_id: str
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., channel_name: _Optional[str] = ..., message_id: _Optional[str] = ..., request_id: _Optional[str] = ...) -> None: ...

class ChannelMessageDeletion(_message.Message):
    __slots__ = ("channel_name", "message_id")
    CHANNEL_NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    channel_name: str
    message_id: str
    def __init__(self, channel_name: _Optional[str] = ..., message_id: _Optional[str] = ...) -> None: ...

class WriteStreamRequest(_message.Message):
    __slots__ = ("flow_id", "flow_type", "stream_name", "stream_capacity_bytes", "value", "source")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    FLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    STREAM_CAPACITY_BYTES_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    flow_type: str
    stream_name: str
    stream_capacity_bytes: int
    value: Value
    source: str
    def __init__(self, flow_id: _Optional[str] = ..., flow_type: _Optional[str] = ..., stream_name: _Optional[str] = ..., stream_capacity_bytes: _Optional[int] = ..., value: _Optional[_Union[Value, _Mapping]] = ..., source: _Optional[str] = ...) -> None: ...

class ReadStreamRequest(_message.Message):
    __slots__ = ("flow_id", "flow_type", "stream_name", "resume_token", "wait_time_seconds")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    FLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    RESUME_TOKEN_FIELD_NUMBER: _ClassVar[int]
    WAIT_TIME_SECONDS_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    flow_type: str
    stream_name: str
    resume_token: str
    wait_time_seconds: int
    def __init__(self, flow_id: _Optional[str] = ..., flow_type: _Optional[str] = ..., stream_name: _Optional[str] = ..., resume_token: _Optional[str] = ..., wait_time_seconds: _Optional[int] = ...) -> None: ...

class ReadStreamResponse(_message.Message):
    __slots__ = ("message",)
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    message: StreamMessage
    def __init__(self, message: _Optional[_Union[StreamMessage, _Mapping]] = ...) -> None: ...

class ListStreamMessagesRequest(_message.Message):
    __slots__ = ("flow_id", "flow_type", "stream_name", "page_size", "before_page_token")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    FLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    BEFORE_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    flow_type: str
    stream_name: str
    page_size: int
    before_page_token: str
    def __init__(self, flow_id: _Optional[str] = ..., flow_type: _Optional[str] = ..., stream_name: _Optional[str] = ..., page_size: _Optional[int] = ..., before_page_token: _Optional[str] = ...) -> None: ...

class ListStreamMessagesResponse(_message.Message):
    __slots__ = ("messages", "next_page_token")
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[StreamMessage]
    next_page_token: str
    def __init__(self, messages: _Optional[_Iterable[_Union[StreamMessage, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class StreamMessage(_message.Message):
    __slots__ = ("value", "resume_token", "created_time", "source")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    RESUME_TOKEN_FIELD_NUMBER: _ClassVar[int]
    CREATED_TIME_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    value: Value
    resume_token: str
    created_time: _timestamp_pb2.Timestamp
    source: str
    def __init__(self, value: _Optional[_Union[Value, _Mapping]] = ..., resume_token: _Optional[str] = ..., created_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., source: _Optional[str] = ...) -> None: ...

class StopFlowRequest(_message.Message):
    __slots__ = ("flow_id", "run_id", "reason", "stop_type")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    STOP_TYPE_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    reason: str
    stop_type: StopType
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., reason: _Optional[str] = ..., stop_type: _Optional[_Union[StopType, str]] = ...) -> None: ...

class GetAttributesRequest(_message.Message):
    __slots__ = ("flow_id", "run_id", "keys", "all_keys")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    ALL_KEYS_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    keys: _containers.RepeatedScalarFieldContainer[str]
    all_keys: bool
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., keys: _Optional[_Iterable[str]] = ..., all_keys: _Optional[bool] = ...) -> None: ...

class GetAttributesResponse(_message.Message):
    __slots__ = ("attributes",)
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    attributes: _containers.RepeatedCompositeFieldContainer[KV]
    def __init__(self, attributes: _Optional[_Iterable[_Union[KV, _Mapping]]] = ...) -> None: ...

class SetAttributesRequest(_message.Message):
    __slots__ = ("flow_id", "run_id", "attributes", "request_id", "action_permission_mappings")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    ACTION_PERMISSION_MAPPINGS_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    attributes: _containers.RepeatedCompositeFieldContainer[AttributeWrite]
    request_id: str
    action_permission_mappings: ActionPermissionMappings
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., attributes: _Optional[_Iterable[_Union[AttributeWrite, _Mapping]]] = ..., request_id: _Optional[str] = ..., action_permission_mappings: _Optional[_Union[ActionPermissionMappings, _Mapping]] = ...) -> None: ...

class LoadBlobRequestEntry(_message.Message):
    __slots__ = ("flow_id", "blob_value")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    BLOB_VALUE_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    blob_value: Value
    def __init__(self, flow_id: _Optional[str] = ..., blob_value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class LoadBlobsRequest(_message.Message):
    __slots__ = ("entries",)
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[LoadBlobRequestEntry]
    def __init__(self, entries: _Optional[_Iterable[_Union[LoadBlobRequestEntry, _Mapping]]] = ...) -> None: ...

class LoadBlobsResponse(_message.Message):
    __slots__ = ("values",)
    class ValuesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Value
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.MessageMap[str, Value]
    def __init__(self, values: _Optional[_Mapping[str, Value]] = ...) -> None: ...

class WaitForFlowRequest(_message.Message):
    __slots__ = ("flow_id", "run_id", "needs_results", "wait_time_seconds")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    NEEDS_RESULTS_FIELD_NUMBER: _ClassVar[int]
    WAIT_TIME_SECONDS_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    needs_results: bool
    wait_time_seconds: int
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., needs_results: _Optional[bool] = ..., wait_time_seconds: _Optional[int] = ...) -> None: ...

class StepCompletionOutput(_message.Message):
    __slots__ = ("completed_step_type", "completed_step_execution_id", "completed_step_output")
    COMPLETED_STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_STEP_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    completed_step_type: str
    completed_step_execution_id: str
    completed_step_output: Value
    def __init__(self, completed_step_type: _Optional[str] = ..., completed_step_execution_id: _Optional[str] = ..., completed_step_output: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class FlowResult(_message.Message):
    __slots__ = ("flow_status", "results", "error_type", "error_message")
    FLOW_STATUS_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    flow_status: FlowStatus
    results: _containers.RepeatedCompositeFieldContainer[StepCompletionOutput]
    error_type: FlowErrorType
    error_message: str
    def __init__(self, flow_status: _Optional[_Union[FlowStatus, str]] = ..., results: _Optional[_Iterable[_Union[StepCompletionOutput, _Mapping]]] = ..., error_type: _Optional[_Union[FlowErrorType, str]] = ..., error_message: _Optional[str] = ...) -> None: ...

class SearchFlowsRequest(_message.Message):
    __slots__ = ("query", "page_size", "next_page_token", "include_continued_as_new")
    QUERY_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_CONTINUED_AS_NEW_FIELD_NUMBER: _ClassVar[int]
    query: str
    page_size: int
    next_page_token: str
    include_continued_as_new: bool
    def __init__(self, query: _Optional[str] = ..., page_size: _Optional[int] = ..., next_page_token: _Optional[str] = ..., include_continued_as_new: _Optional[bool] = ...) -> None: ...

class SearchFlowsResponse(_message.Message):
    __slots__ = ("flow_runs", "next_page_token")
    FLOW_RUNS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    flow_runs: _containers.RepeatedCompositeFieldContainer[SearchFlowsResponseEntry]
    next_page_token: str
    def __init__(self, flow_runs: _Optional[_Iterable[_Union[SearchFlowsResponseEntry, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class SearchFlowsResponseEntry(_message.Message):
    __slots__ = ("flow_id", "run_id", "indexed_attributes", "flow_type", "flow_status", "start_time", "close_time")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    INDEXED_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    FLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    FLOW_STATUS_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    CLOSE_TIME_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    indexed_attributes: _containers.RepeatedCompositeFieldContainer[KV]
    flow_type: str
    flow_status: FlowStatus
    start_time: _timestamp_pb2.Timestamp
    close_time: _timestamp_pb2.Timestamp
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., indexed_attributes: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., flow_type: _Optional[str] = ..., flow_status: _Optional[_Union[FlowStatus, str]] = ..., start_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., close_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SyncAttributeIndexRequest(_message.Message):
    __slots__ = ("attribute_indexes",)
    class AttributeIndexesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: IndexType
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[IndexType, str]] = ...) -> None: ...
    ATTRIBUTE_INDEXES_FIELD_NUMBER: _ClassVar[int]
    attribute_indexes: _containers.ScalarMap[str, IndexType]
    def __init__(self, attribute_indexes: _Optional[_Mapping[str, IndexType]] = ...) -> None: ...

class SyncAttributeIndexResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FlowExecutionID(_message.Message):
    __slots__ = ("flow_id", "run_id")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ...) -> None: ...

class GetFlowSummaryRequest(_message.Message):
    __slots__ = ("flow_id", "run_id")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ...) -> None: ...

class GetFlowSummaryResponse(_message.Message):
    __slots__ = ("flow_execution_id", "first_run_id", "request_id", "flow_type", "flow_status", "start_time", "close_time")
    FLOW_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    FIRST_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    FLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    FLOW_STATUS_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    CLOSE_TIME_FIELD_NUMBER: _ClassVar[int]
    flow_execution_id: FlowExecutionID
    first_run_id: str
    request_id: str
    flow_type: str
    flow_status: FlowStatus
    start_time: _timestamp_pb2.Timestamp
    close_time: _timestamp_pb2.Timestamp
    def __init__(self, flow_execution_id: _Optional[_Union[FlowExecutionID, _Mapping]] = ..., first_run_id: _Optional[str] = ..., request_id: _Optional[str] = ..., flow_type: _Optional[str] = ..., flow_status: _Optional[_Union[FlowStatus, str]] = ..., start_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., close_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class InternalAsyncStepInputSnapshot(_message.Message):
    __slots__ = ("method_options", "wait_for_request", "execute_request")
    METHOD_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    WAIT_FOR_REQUEST_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_REQUEST_FIELD_NUMBER: _ClassVar[int]
    method_options: StepMethodOptions
    wait_for_request: InvokeWaitForMethodRequest
    execute_request: InvokeExecuteMethodRequest
    def __init__(self, method_options: _Optional[_Union[StepMethodOptions, _Mapping]] = ..., wait_for_request: _Optional[_Union[InvokeWaitForMethodRequest, _Mapping]] = ..., execute_request: _Optional[_Union[InvokeExecuteMethodRequest, _Mapping]] = ...) -> None: ...

class InternalLocalActivityInput(_message.Message):
    __slots__ = ("current_run_started_timestamp", "method_options")
    CURRENT_RUN_STARTED_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    METHOD_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    current_run_started_timestamp: int
    method_options: StepMethodOptions
    def __init__(self, current_run_started_timestamp: _Optional[int] = ..., method_options: _Optional[_Union[StepMethodOptions, _Mapping]] = ...) -> None: ...

class GetHistoryEventsRequest(_message.Message):
    __slots__ = ("flow_id", "run_id", "start_internal_event_id", "estimate_page_size", "next_page_token")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    START_INTERNAL_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ESTIMATE_PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    start_internal_event_id: int
    estimate_page_size: int
    next_page_token: bytes
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., start_internal_event_id: _Optional[int] = ..., estimate_page_size: _Optional[int] = ..., next_page_token: _Optional[bytes] = ...) -> None: ...

class GetHistoryEventsResponse(_message.Message):
    __slots__ = ("events", "next_page_token", "next_internal_event_id")
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    NEXT_INTERNAL_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    events: _containers.RepeatedCompositeFieldContainer[FlowHistoryEvent]
    next_page_token: bytes
    next_internal_event_id: int
    def __init__(self, events: _Optional[_Iterable[_Union[FlowHistoryEvent, _Mapping]]] = ..., next_page_token: _Optional[bytes] = ..., next_internal_event_id: _Optional[int] = ...) -> None: ...

class FlowHistoryEvent(_message.Message):
    __slots__ = ("event_id", "event_time", "flow_started_or_continued", "flow_closed", "step_wait_for_completed", "step_wait_for_failed", "step_execute_completed", "step_execute_failed", "rpc_execution_completed", "channel_external_publish", "step_wait_for_pending", "step_execute_pending", "time_travel_fork", "channel_external_delete")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_FIELD_NUMBER: _ClassVar[int]
    FLOW_STARTED_OR_CONTINUED_FIELD_NUMBER: _ClassVar[int]
    FLOW_CLOSED_FIELD_NUMBER: _ClassVar[int]
    STEP_WAIT_FOR_COMPLETED_FIELD_NUMBER: _ClassVar[int]
    STEP_WAIT_FOR_FAILED_FIELD_NUMBER: _ClassVar[int]
    STEP_EXECUTE_COMPLETED_FIELD_NUMBER: _ClassVar[int]
    STEP_EXECUTE_FAILED_FIELD_NUMBER: _ClassVar[int]
    RPC_EXECUTION_COMPLETED_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_EXTERNAL_PUBLISH_FIELD_NUMBER: _ClassVar[int]
    STEP_WAIT_FOR_PENDING_FIELD_NUMBER: _ClassVar[int]
    STEP_EXECUTE_PENDING_FIELD_NUMBER: _ClassVar[int]
    TIME_TRAVEL_FORK_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_EXTERNAL_DELETE_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time: _timestamp_pb2.Timestamp
    flow_started_or_continued: FlowStartedOrContinuedHistoryEvent
    flow_closed: FlowClosedHistoryEvent
    step_wait_for_completed: StepWaitForCompletedEvent
    step_wait_for_failed: StepWaitForFailedEvent
    step_execute_completed: StepExecuteCompletedEvent
    step_execute_failed: StepExecuteFailedEvent
    rpc_execution_completed: RpcExecutionCompletedEvent
    channel_external_publish: ChannelExternalPublishEvent
    step_wait_for_pending: StepMethodPendingEvent
    step_execute_pending: StepMethodPendingEvent
    time_travel_fork: TimeTravelForkHistoryEvent
    channel_external_delete: ChannelExternalDeleteEvent
    def __init__(self, event_id: _Optional[int] = ..., event_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., flow_started_or_continued: _Optional[_Union[FlowStartedOrContinuedHistoryEvent, _Mapping]] = ..., flow_closed: _Optional[_Union[FlowClosedHistoryEvent, _Mapping]] = ..., step_wait_for_completed: _Optional[_Union[StepWaitForCompletedEvent, _Mapping]] = ..., step_wait_for_failed: _Optional[_Union[StepWaitForFailedEvent, _Mapping]] = ..., step_execute_completed: _Optional[_Union[StepExecuteCompletedEvent, _Mapping]] = ..., step_execute_failed: _Optional[_Union[StepExecuteFailedEvent, _Mapping]] = ..., rpc_execution_completed: _Optional[_Union[RpcExecutionCompletedEvent, _Mapping]] = ..., channel_external_publish: _Optional[_Union[ChannelExternalPublishEvent, _Mapping]] = ..., step_wait_for_pending: _Optional[_Union[StepMethodPendingEvent, _Mapping]] = ..., step_execute_pending: _Optional[_Union[StepMethodPendingEvent, _Mapping]] = ..., time_travel_fork: _Optional[_Union[TimeTravelForkHistoryEvent, _Mapping]] = ..., channel_external_delete: _Optional[_Union[ChannelExternalDeleteEvent, _Mapping]] = ...) -> None: ...

class TimeTravelForkHistoryEvent(_message.Message):
    __slots__ = ("previous_run_id",)
    PREVIOUS_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    previous_run_id: str
    def __init__(self, previous_run_id: _Optional[str] = ...) -> None: ...

class FlowStartedOrContinuedHistoryEvent(_message.Message):
    __slots__ = ("flow_execution_id", "flow_type", "flow_config", "flow_timeout", "flow_timeout_policy", "timeout_handler_options", "initial_start", "continued_start")
    FLOW_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    FLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    FLOW_CONFIG_FIELD_NUMBER: _ClassVar[int]
    FLOW_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    FLOW_TIMEOUT_POLICY_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_HANDLER_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    INITIAL_START_FIELD_NUMBER: _ClassVar[int]
    CONTINUED_START_FIELD_NUMBER: _ClassVar[int]
    flow_execution_id: FlowExecutionID
    flow_type: str
    flow_config: FlowConfig
    flow_timeout: _duration_pb2.Duration
    flow_timeout_policy: FlowTimeoutPolicy
    timeout_handler_options: FlowTimeoutHandlerOptions
    initial_start: FlowInitialStart
    continued_start: FlowContinuedStart
    def __init__(self, flow_execution_id: _Optional[_Union[FlowExecutionID, _Mapping]] = ..., flow_type: _Optional[str] = ..., flow_config: _Optional[_Union[FlowConfig, _Mapping]] = ..., flow_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., flow_timeout_policy: _Optional[_Union[FlowTimeoutPolicy, str]] = ..., timeout_handler_options: _Optional[_Union[FlowTimeoutHandlerOptions, _Mapping]] = ..., initial_start: _Optional[_Union[FlowInitialStart, _Mapping]] = ..., continued_start: _Optional[_Union[FlowContinuedStart, _Mapping]] = ...) -> None: ...

class FlowInitialStart(_message.Message):
    __slots__ = ("start_step_type", "step_input", "step_options", "initial_attributes")
    START_STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    STEP_INPUT_FIELD_NUMBER: _ClassVar[int]
    STEP_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    INITIAL_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    start_step_type: str
    step_input: Value
    step_options: StepOptions
    initial_attributes: _containers.RepeatedCompositeFieldContainer[KV]
    def __init__(self, start_step_type: _Optional[str] = ..., step_input: _Optional[_Union[Value, _Mapping]] = ..., step_options: _Optional[_Union[StepOptions, _Mapping]] = ..., initial_attributes: _Optional[_Iterable[_Union[KV, _Mapping]]] = ...) -> None: ...

class FlowContinuedStart(_message.Message):
    __slots__ = ("previous_run_id", "steps_to_start", "steps_to_resume", "pending_channel_messages", "attributes", "completed_steps")
    class PendingChannelMessagesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ChannelValues
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ChannelValues, _Mapping]] = ...) -> None: ...
    PREVIOUS_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    STEPS_TO_START_FIELD_NUMBER: _ClassVar[int]
    STEPS_TO_RESUME_FIELD_NUMBER: _ClassVar[int]
    PENDING_CHANNEL_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_STEPS_FIELD_NUMBER: _ClassVar[int]
    previous_run_id: str
    steps_to_start: _containers.RepeatedCompositeFieldContainer[StepMovement]
    steps_to_resume: _containers.RepeatedCompositeFieldContainer[StepExecutionResumeInfo]
    pending_channel_messages: _containers.MessageMap[str, ChannelValues]
    attributes: _containers.RepeatedCompositeFieldContainer[KV]
    completed_steps: _containers.RepeatedCompositeFieldContainer[StepCompletionOutput]
    def __init__(self, previous_run_id: _Optional[str] = ..., steps_to_start: _Optional[_Iterable[_Union[StepMovement, _Mapping]]] = ..., steps_to_resume: _Optional[_Iterable[_Union[StepExecutionResumeInfo, _Mapping]]] = ..., pending_channel_messages: _Optional[_Mapping[str, ChannelValues]] = ..., attributes: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., completed_steps: _Optional[_Iterable[_Union[StepCompletionOutput, _Mapping]]] = ...) -> None: ...

class FlowClosedHistoryEvent(_message.Message):
    __slots__ = ("flow_status", "results", "error_type", "error_message", "continued_to_run_id")
    FLOW_STATUS_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    CONTINUED_TO_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    flow_status: FlowStatus
    results: _containers.RepeatedCompositeFieldContainer[StepCompletionOutput]
    error_type: FlowErrorType
    error_message: str
    continued_to_run_id: str
    def __init__(self, flow_status: _Optional[_Union[FlowStatus, str]] = ..., results: _Optional[_Iterable[_Union[StepCompletionOutput, _Mapping]]] = ..., error_type: _Optional[_Union[FlowErrorType, str]] = ..., error_message: _Optional[str] = ..., continued_to_run_id: _Optional[str] = ...) -> None: ...

class StepMethodPendingEvent(_message.Message):
    __slots__ = ("input", "context", "phase")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    input: StepMethodEventInput
    context: StepMethodEventContext
    phase: PendingStepMethodPhase
    def __init__(self, input: _Optional[_Union[StepMethodEventInput, _Mapping]] = ..., context: _Optional[_Union[StepMethodEventContext, _Mapping]] = ..., phase: _Optional[_Union[PendingStepMethodPhase, str]] = ...) -> None: ...

class StepMethodFailure(_message.Message):
    __slots__ = ("backend_error", "details", "attempt")
    BACKEND_ERROR_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    backend_error: str
    details: ServiceErrorResponse
    attempt: int
    def __init__(self, backend_error: _Optional[str] = ..., details: _Optional[_Union[ServiceErrorResponse, _Mapping]] = ..., attempt: _Optional[int] = ...) -> None: ...

class StepMethodOptions(_message.Message):
    __slots__ = ("timeout_seconds", "retry_policy", "heartbeat_timeout_seconds")
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    timeout_seconds: int
    retry_policy: RetryPolicy
    heartbeat_timeout_seconds: int
    def __init__(self, timeout_seconds: _Optional[int] = ..., retry_policy: _Optional[_Union[RetryPolicy, _Mapping]] = ..., heartbeat_timeout_seconds: _Optional[int] = ...) -> None: ...

class StepMethodEventInput(_message.Message):
    __slots__ = ("unavailable", "step_input", "condition_results", "attributes", "step_execution_locals")
    UNAVAILABLE_FIELD_NUMBER: _ClassVar[int]
    STEP_INPUT_FIELD_NUMBER: _ClassVar[int]
    CONDITION_RESULTS_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    STEP_EXECUTION_LOCALS_FIELD_NUMBER: _ClassVar[int]
    unavailable: bool
    step_input: Value
    condition_results: ConditionResults
    attributes: _containers.RepeatedCompositeFieldContainer[KV]
    step_execution_locals: _containers.RepeatedCompositeFieldContainer[KV]
    def __init__(self, unavailable: _Optional[bool] = ..., step_input: _Optional[_Union[Value, _Mapping]] = ..., condition_results: _Optional[_Union[ConditionResults, _Mapping]] = ..., attributes: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., step_execution_locals: _Optional[_Iterable[_Union[KV, _Mapping]]] = ...) -> None: ...

class StepMethodEventContext(_message.Message):
    __slots__ = ("step_execution_id", "from_step_execution_id", "step_type", "durability", "final_attempt", "started_time", "duration", "method_options", "last_failure_info")
    STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_FIELD_NUMBER: _ClassVar[int]
    FINAL_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    STARTED_TIME_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    METHOD_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    LAST_FAILURE_INFO_FIELD_NUMBER: _ClassVar[int]
    step_execution_id: str
    from_step_execution_id: str
    step_type: str
    durability: StepDurability
    final_attempt: int
    started_time: _timestamp_pb2.Timestamp
    duration: _duration_pb2.Duration
    method_options: StepMethodOptions
    last_failure_info: StepMethodFailure
    def __init__(self, step_execution_id: _Optional[str] = ..., from_step_execution_id: _Optional[str] = ..., step_type: _Optional[str] = ..., durability: _Optional[_Union[StepDurability, str]] = ..., final_attempt: _Optional[int] = ..., started_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., method_options: _Optional[_Union[StepMethodOptions, _Mapping]] = ..., last_failure_info: _Optional[_Union[StepMethodFailure, _Mapping]] = ...) -> None: ...

class StepWaitForCompletedOutput(_message.Message):
    __slots__ = ("wait_for_condition", "upsert_attributes", "publish_to_channel", "record_events", "upsert_step_execution_locals", "delete_from_channel")
    WAIT_FOR_CONDITION_FIELD_NUMBER: _ClassVar[int]
    UPSERT_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    PUBLISH_TO_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    RECORD_EVENTS_FIELD_NUMBER: _ClassVar[int]
    UPSERT_STEP_EXECUTION_LOCALS_FIELD_NUMBER: _ClassVar[int]
    DELETE_FROM_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    wait_for_condition: WaitingCondition
    upsert_attributes: _containers.RepeatedCompositeFieldContainer[AttributeWrite]
    publish_to_channel: _containers.RepeatedCompositeFieldContainer[ChannelMessage]
    record_events: _containers.RepeatedCompositeFieldContainer[KV]
    upsert_step_execution_locals: _containers.RepeatedCompositeFieldContainer[KV]
    delete_from_channel: _containers.RepeatedCompositeFieldContainer[ChannelMessageDeletion]
    def __init__(self, wait_for_condition: _Optional[_Union[WaitingCondition, _Mapping]] = ..., upsert_attributes: _Optional[_Iterable[_Union[AttributeWrite, _Mapping]]] = ..., publish_to_channel: _Optional[_Iterable[_Union[ChannelMessage, _Mapping]]] = ..., record_events: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., upsert_step_execution_locals: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., delete_from_channel: _Optional[_Iterable[_Union[ChannelMessageDeletion, _Mapping]]] = ...) -> None: ...

class StepExecuteCompletedOutput(_message.Message):
    __slots__ = ("step_decision", "upsert_attributes", "publish_to_channel", "record_events", "upsert_step_execution_locals", "delete_from_channel")
    STEP_DECISION_FIELD_NUMBER: _ClassVar[int]
    UPSERT_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    PUBLISH_TO_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    RECORD_EVENTS_FIELD_NUMBER: _ClassVar[int]
    UPSERT_STEP_EXECUTION_LOCALS_FIELD_NUMBER: _ClassVar[int]
    DELETE_FROM_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    step_decision: StepDecision
    upsert_attributes: _containers.RepeatedCompositeFieldContainer[AttributeWrite]
    publish_to_channel: _containers.RepeatedCompositeFieldContainer[ChannelMessage]
    record_events: _containers.RepeatedCompositeFieldContainer[KV]
    upsert_step_execution_locals: _containers.RepeatedCompositeFieldContainer[KV]
    delete_from_channel: _containers.RepeatedCompositeFieldContainer[ChannelMessageDeletion]
    def __init__(self, step_decision: _Optional[_Union[StepDecision, _Mapping]] = ..., upsert_attributes: _Optional[_Iterable[_Union[AttributeWrite, _Mapping]]] = ..., publish_to_channel: _Optional[_Iterable[_Union[ChannelMessage, _Mapping]]] = ..., record_events: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., upsert_step_execution_locals: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., delete_from_channel: _Optional[_Iterable[_Union[ChannelMessageDeletion, _Mapping]]] = ...) -> None: ...

class StepMethodFailedOutput(_message.Message):
    __slots__ = ("failure",)
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    failure: StepMethodFailure
    def __init__(self, failure: _Optional[_Union[StepMethodFailure, _Mapping]] = ...) -> None: ...

class StepWaitForCompletedEvent(_message.Message):
    __slots__ = ("input", "output", "context")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    input: StepMethodEventInput
    output: StepWaitForCompletedOutput
    context: StepMethodEventContext
    def __init__(self, input: _Optional[_Union[StepMethodEventInput, _Mapping]] = ..., output: _Optional[_Union[StepWaitForCompletedOutput, _Mapping]] = ..., context: _Optional[_Union[StepMethodEventContext, _Mapping]] = ...) -> None: ...

class StepWaitForFailedEvent(_message.Message):
    __slots__ = ("input", "output", "context")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    input: StepMethodEventInput
    output: StepMethodFailedOutput
    context: StepMethodEventContext
    def __init__(self, input: _Optional[_Union[StepMethodEventInput, _Mapping]] = ..., output: _Optional[_Union[StepMethodFailedOutput, _Mapping]] = ..., context: _Optional[_Union[StepMethodEventContext, _Mapping]] = ...) -> None: ...

class StepExecuteCompletedEvent(_message.Message):
    __slots__ = ("input", "output", "context")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    input: StepMethodEventInput
    output: StepExecuteCompletedOutput
    context: StepMethodEventContext
    def __init__(self, input: _Optional[_Union[StepMethodEventInput, _Mapping]] = ..., output: _Optional[_Union[StepExecuteCompletedOutput, _Mapping]] = ..., context: _Optional[_Union[StepMethodEventContext, _Mapping]] = ...) -> None: ...

class StepExecuteFailedEvent(_message.Message):
    __slots__ = ("input", "output", "context")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    input: StepMethodEventInput
    output: StepMethodFailedOutput
    context: StepMethodEventContext
    def __init__(self, input: _Optional[_Union[StepMethodEventInput, _Mapping]] = ..., output: _Optional[_Union[StepMethodFailedOutput, _Mapping]] = ..., context: _Optional[_Union[StepMethodEventContext, _Mapping]] = ...) -> None: ...

class RpcExecutionCompletedEvent(_message.Message):
    __slots__ = ("rpc_name", "input", "output", "step_decision", "upsert_attributes", "record_events", "publish_to_channel", "is_set_attribute_api", "delete_from_channel")
    RPC_NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    STEP_DECISION_FIELD_NUMBER: _ClassVar[int]
    UPSERT_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    RECORD_EVENTS_FIELD_NUMBER: _ClassVar[int]
    PUBLISH_TO_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    IS_SET_ATTRIBUTE_API_FIELD_NUMBER: _ClassVar[int]
    DELETE_FROM_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    rpc_name: str
    input: Value
    output: Value
    step_decision: StepDecision
    upsert_attributes: _containers.RepeatedCompositeFieldContainer[AttributeWrite]
    record_events: _containers.RepeatedCompositeFieldContainer[KV]
    publish_to_channel: _containers.RepeatedCompositeFieldContainer[ChannelMessage]
    is_set_attribute_api: bool
    delete_from_channel: _containers.RepeatedCompositeFieldContainer[ChannelMessageDeletion]
    def __init__(self, rpc_name: _Optional[str] = ..., input: _Optional[_Union[Value, _Mapping]] = ..., output: _Optional[_Union[Value, _Mapping]] = ..., step_decision: _Optional[_Union[StepDecision, _Mapping]] = ..., upsert_attributes: _Optional[_Iterable[_Union[AttributeWrite, _Mapping]]] = ..., record_events: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., publish_to_channel: _Optional[_Iterable[_Union[ChannelMessage, _Mapping]]] = ..., is_set_attribute_api: _Optional[bool] = ..., delete_from_channel: _Optional[_Iterable[_Union[ChannelMessageDeletion, _Mapping]]] = ...) -> None: ...

class ChannelExternalPublishEvent(_message.Message):
    __slots__ = ("messages",)
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[ChannelMessage]
    def __init__(self, messages: _Optional[_Iterable[_Union[ChannelMessage, _Mapping]]] = ...) -> None: ...

class ChannelExternalDeleteEvent(_message.Message):
    __slots__ = ("messages",)
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[ChannelMessageDeletion]
    def __init__(self, messages: _Optional[_Iterable[_Union[ChannelMessageDeletion, _Mapping]]] = ...) -> None: ...

class WaitForHistoryEventRequest(_message.Message):
    __slots__ = ("flow_id", "run_id", "next_internal_event_id")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    NEXT_INTERNAL_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    next_internal_event_id: int
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., next_internal_event_id: _Optional[int] = ...) -> None: ...

class WaitForHistoryEventResponse(_message.Message):
    __slots__ = ("event_available", "available_internal_event_id", "flow_status")
    EVENT_AVAILABLE_FIELD_NUMBER: _ClassVar[int]
    AVAILABLE_INTERNAL_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    FLOW_STATUS_FIELD_NUMBER: _ClassVar[int]
    event_available: bool
    available_internal_event_id: int
    flow_status: FlowStatus
    def __init__(self, event_available: _Optional[bool] = ..., available_internal_event_id: _Optional[int] = ..., flow_status: _Optional[_Union[FlowStatus, str]] = ...) -> None: ...

class ActiveStepExecutionState(_message.Message):
    __slots__ = ("step_execution_id", "from_step_execution_id", "step_type", "phase", "movement", "waiting_condition", "completed_conditions", "step_execution_locals", "timers", "last_failure_info")
    STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    MOVEMENT_FIELD_NUMBER: _ClassVar[int]
    WAITING_CONDITION_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_CONDITIONS_FIELD_NUMBER: _ClassVar[int]
    STEP_EXECUTION_LOCALS_FIELD_NUMBER: _ClassVar[int]
    TIMERS_FIELD_NUMBER: _ClassVar[int]
    LAST_FAILURE_INFO_FIELD_NUMBER: _ClassVar[int]
    step_execution_id: str
    from_step_execution_id: str
    step_type: str
    phase: ActiveStepPhase
    movement: StepMovement
    waiting_condition: WaitingConditionState
    completed_conditions: StepExecutionCompletedConditions
    step_execution_locals: _containers.RepeatedCompositeFieldContainer[KV]
    timers: _containers.RepeatedCompositeFieldContainer[TimerInfo]
    last_failure_info: StepMethodFailure
    def __init__(self, step_execution_id: _Optional[str] = ..., from_step_execution_id: _Optional[str] = ..., step_type: _Optional[str] = ..., phase: _Optional[_Union[ActiveStepPhase, str]] = ..., movement: _Optional[_Union[StepMovement, _Mapping]] = ..., waiting_condition: _Optional[_Union[WaitingConditionState, _Mapping]] = ..., completed_conditions: _Optional[_Union[StepExecutionCompletedConditions, _Mapping]] = ..., step_execution_locals: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., timers: _Optional[_Iterable[_Union[TimerInfo, _Mapping]]] = ..., last_failure_info: _Optional[_Union[StepMethodFailure, _Mapping]] = ...) -> None: ...

class GetFlowStateRequest(_message.Message):
    __slots__ = ("flow_id", "run_id")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ...) -> None: ...

class GetFlowStateResponse(_message.Message):
    __slots__ = ("flow_config", "attributes", "active_step_executions", "queued_steps", "pending_channel_messages", "completed_steps")
    class PendingChannelMessagesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ChannelValues
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ChannelValues, _Mapping]] = ...) -> None: ...
    FLOW_CONFIG_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_STEP_EXECUTIONS_FIELD_NUMBER: _ClassVar[int]
    QUEUED_STEPS_FIELD_NUMBER: _ClassVar[int]
    PENDING_CHANNEL_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_STEPS_FIELD_NUMBER: _ClassVar[int]
    flow_config: FlowConfig
    attributes: _containers.RepeatedCompositeFieldContainer[KV]
    active_step_executions: _containers.RepeatedCompositeFieldContainer[ActiveStepExecutionState]
    queued_steps: _containers.RepeatedCompositeFieldContainer[StepMovement]
    pending_channel_messages: _containers.MessageMap[str, ChannelValues]
    completed_steps: _containers.RepeatedCompositeFieldContainer[StepCompletionOutput]
    def __init__(self, flow_config: _Optional[_Union[FlowConfig, _Mapping]] = ..., attributes: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., active_step_executions: _Optional[_Iterable[_Union[ActiveStepExecutionState, _Mapping]]] = ..., queued_steps: _Optional[_Iterable[_Union[StepMovement, _Mapping]]] = ..., pending_channel_messages: _Optional[_Mapping[str, ChannelValues]] = ..., completed_steps: _Optional[_Iterable[_Union[StepCompletionOutput, _Mapping]]] = ...) -> None: ...

class ResetFlowRequest(_message.Message):
    __slots__ = ("flow_id", "run_id", "reset_type", "reason", "history_event_time", "step_type", "step_execution_id", "skip_writes_reapply", "step_method")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    RESET_TYPE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    HISTORY_EVENT_TIME_FIELD_NUMBER: _ClassVar[int]
    STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    SKIP_WRITES_REAPPLY_FIELD_NUMBER: _ClassVar[int]
    STEP_METHOD_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    reset_type: FlowResetType
    reason: str
    history_event_time: str
    step_type: str
    step_execution_id: str
    skip_writes_reapply: bool
    step_method: FlowResetStepMethod
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., reset_type: _Optional[_Union[FlowResetType, str]] = ..., reason: _Optional[str] = ..., history_event_time: _Optional[str] = ..., step_type: _Optional[str] = ..., step_execution_id: _Optional[str] = ..., skip_writes_reapply: _Optional[bool] = ..., step_method: _Optional[_Union[FlowResetStepMethod, str]] = ...) -> None: ...

class ResetFlowResponse(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class InvokeRPCRequest(_message.Message):
    __slots__ = ("flow_id", "run_id", "rpc_name", "input", "timeout_seconds", "lock_attribute_keys", "request_id", "is_transactional", "load_attribute_map_instances", "load_channel_names", "load_channel_map_instances")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    RPC_NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    LOCK_ATTRIBUTE_KEYS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    IS_TRANSACTIONAL_FIELD_NUMBER: _ClassVar[int]
    LOAD_ATTRIBUTE_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    LOAD_CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    LOAD_CHANNEL_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    rpc_name: str
    input: Value
    timeout_seconds: int
    lock_attribute_keys: _containers.RepeatedScalarFieldContainer[str]
    request_id: str
    is_transactional: bool
    load_attribute_map_instances: _containers.RepeatedScalarFieldContainer[str]
    load_channel_names: _containers.RepeatedScalarFieldContainer[str]
    load_channel_map_instances: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., rpc_name: _Optional[str] = ..., input: _Optional[_Union[Value, _Mapping]] = ..., timeout_seconds: _Optional[int] = ..., lock_attribute_keys: _Optional[_Iterable[str]] = ..., request_id: _Optional[str] = ..., is_transactional: _Optional[bool] = ..., load_attribute_map_instances: _Optional[_Iterable[str]] = ..., load_channel_names: _Optional[_Iterable[str]] = ..., load_channel_map_instances: _Optional[_Iterable[str]] = ...) -> None: ...

class InvokeRPCResponse(_message.Message):
    __slots__ = ("output",)
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: Value
    def __init__(self, output: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class SkipTimerRequest(_message.Message):
    __slots__ = ("flow_id", "run_id", "step_execution_id", "timer_condition_id", "timer_condition_index")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    TIMER_CONDITION_ID_FIELD_NUMBER: _ClassVar[int]
    TIMER_CONDITION_INDEX_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    step_execution_id: str
    timer_condition_id: str
    timer_condition_index: int
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., step_execution_id: _Optional[str] = ..., timer_condition_id: _Optional[str] = ..., timer_condition_index: _Optional[int] = ...) -> None: ...

class UpdateFlowConfigRequest(_message.Message):
    __slots__ = ("flow_id", "run_id", "flow_config")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    FLOW_CONFIG_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    flow_config: FlowConfig
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., flow_config: _Optional[_Union[FlowConfig, _Mapping]] = ...) -> None: ...

class WaitForStepCompletionRequest(_message.Message):
    __slots__ = ("flow_id", "step_type", "step_execution_number", "request_timeout_seconds", "internal_handler_timeout_seconds", "request_id")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    STEP_EXECUTION_NUMBER_FIELD_NUMBER: _ClassVar[int]
    REQUEST_TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    INTERNAL_HANDLER_TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    step_type: str
    step_execution_number: str
    request_timeout_seconds: int
    internal_handler_timeout_seconds: int
    request_id: str
    def __init__(self, flow_id: _Optional[str] = ..., step_type: _Optional[str] = ..., step_execution_number: _Optional[str] = ..., request_timeout_seconds: _Optional[int] = ..., internal_handler_timeout_seconds: _Optional[int] = ..., request_id: _Optional[str] = ...) -> None: ...

class WaitForStepCompletionResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class WaitForAttributeRequest(_message.Message):
    __slots__ = ("flow_id", "match", "request_timeout_seconds", "internal_handler_timeout_seconds", "request_id")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    MATCH_FIELD_NUMBER: _ClassVar[int]
    REQUEST_TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    INTERNAL_HANDLER_TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    match: AttributeMatch
    request_timeout_seconds: int
    internal_handler_timeout_seconds: int
    request_id: str
    def __init__(self, flow_id: _Optional[str] = ..., match: _Optional[_Union[AttributeMatch, _Mapping]] = ..., request_timeout_seconds: _Optional[int] = ..., internal_handler_timeout_seconds: _Optional[int] = ..., request_id: _Optional[str] = ...) -> None: ...

class WaitForAttributeResponse(_message.Message):
    __slots__ = ("matched_value",)
    MATCHED_VALUE_FIELD_NUMBER: _ClassVar[int]
    matched_value: Value
    def __init__(self, matched_value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class AttributeMatch(_message.Message):
    __slots__ = ("key", "operator", "operand")
    KEY_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_FIELD_NUMBER: _ClassVar[int]
    OPERAND_FIELD_NUMBER: _ClassVar[int]
    key: str
    operator: AttributeMatchOperator
    operand: Value
    def __init__(self, key: _Optional[str] = ..., operator: _Optional[_Union[AttributeMatchOperator, str]] = ..., operand: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class TriggerContinueAsNewRequest(_message.Message):
    __slots__ = ("flow_id", "run_id")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ...) -> None: ...

class HealthInfo(_message.Message):
    __slots__ = ("condition", "hostname", "duration")
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    condition: str
    hostname: str
    duration: int
    def __init__(self, condition: _Optional[str] = ..., hostname: _Optional[str] = ..., duration: _Optional[int] = ...) -> None: ...

class ServiceErrorResponse(_message.Message):
    __slots__ = ("detail", "sub_status", "original_worker_error_detail", "original_worker_error_type", "original_worker_error_status", "original_worker_error_stack_trace")
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    SUB_STATUS_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_WORKER_ERROR_DETAIL_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_WORKER_ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_WORKER_ERROR_STATUS_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_WORKER_ERROR_STACK_TRACE_FIELD_NUMBER: _ClassVar[int]
    detail: str
    sub_status: ErrorSubStatus
    original_worker_error_detail: str
    original_worker_error_type: str
    original_worker_error_status: int
    original_worker_error_stack_trace: str
    def __init__(self, detail: _Optional[str] = ..., sub_status: _Optional[_Union[ErrorSubStatus, str]] = ..., original_worker_error_detail: _Optional[str] = ..., original_worker_error_type: _Optional[str] = ..., original_worker_error_status: _Optional[int] = ..., original_worker_error_stack_trace: _Optional[str] = ...) -> None: ...

class WorkerErrorResponse(_message.Message):
    __slots__ = ("detail", "error_type", "stack_trace", "retry_after_seconds")
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    STACK_TRACE_FIELD_NUMBER: _ClassVar[int]
    RETRY_AFTER_SECONDS_FIELD_NUMBER: _ClassVar[int]
    detail: str
    error_type: str
    stack_trace: str
    retry_after_seconds: int
    def __init__(self, detail: _Optional[str] = ..., error_type: _Optional[str] = ..., stack_trace: _Optional[str] = ..., retry_after_seconds: _Optional[int] = ...) -> None: ...

class InternalActivityError(_message.Message):
    __slots__ = ("server_detail", "worker_grpc_status", "worker_error")
    SERVER_DETAIL_FIELD_NUMBER: _ClassVar[int]
    WORKER_GRPC_STATUS_FIELD_NUMBER: _ClassVar[int]
    WORKER_ERROR_FIELD_NUMBER: _ClassVar[int]
    server_detail: str
    worker_grpc_status: int
    worker_error: InternalWorkerError
    def __init__(self, server_detail: _Optional[str] = ..., worker_grpc_status: _Optional[int] = ..., worker_error: _Optional[_Union[InternalWorkerError, _Mapping]] = ...) -> None: ...

class InternalWorkerError(_message.Message):
    __slots__ = ("detail", "error_type", "stack_trace")
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    STACK_TRACE_FIELD_NUMBER: _ClassVar[int]
    detail: str
    error_type: str
    stack_trace: str
    def __init__(self, detail: _Optional[str] = ..., error_type: _Optional[str] = ..., stack_trace: _Optional[str] = ...) -> None: ...

class InternalFlowError(_message.Message):
    __slots__ = ("server_detail", "activity_error")
    SERVER_DETAIL_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_ERROR_FIELD_NUMBER: _ClassVar[int]
    server_detail: str
    activity_error: InternalActivityError
    def __init__(self, server_detail: _Optional[str] = ..., activity_error: _Optional[_Union[InternalActivityError, _Mapping]] = ...) -> None: ...

class ChannelInfo(_message.Message):
    __slots__ = ("size",)
    SIZE_FIELD_NUMBER: _ClassVar[int]
    size: int
    def __init__(self, size: _Optional[int] = ...) -> None: ...

class InvokeWaitForMethodRequest(_message.Message):
    __slots__ = ("context", "flow_type", "step_type", "step_input", "attributes", "channel_infos", "loaded_channel_messages", "loaded_attribute_map_instances", "loaded_channel_names", "loaded_channel_map_instances")
    class ChannelInfosEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ChannelInfo
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ChannelInfo, _Mapping]] = ...) -> None: ...
    class LoadedChannelMessagesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ChannelValues
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ChannelValues, _Mapping]] = ...) -> None: ...
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    FLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    STEP_INPUT_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_INFOS_FIELD_NUMBER: _ClassVar[int]
    LOADED_CHANNEL_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    LOADED_ATTRIBUTE_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    LOADED_CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    LOADED_CHANNEL_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    context: Context
    flow_type: str
    step_type: str
    step_input: Value
    attributes: _containers.RepeatedCompositeFieldContainer[KV]
    channel_infos: _containers.MessageMap[str, ChannelInfo]
    loaded_channel_messages: _containers.MessageMap[str, ChannelValues]
    loaded_attribute_map_instances: _containers.RepeatedScalarFieldContainer[str]
    loaded_channel_names: _containers.RepeatedScalarFieldContainer[str]
    loaded_channel_map_instances: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, context: _Optional[_Union[Context, _Mapping]] = ..., flow_type: _Optional[str] = ..., step_type: _Optional[str] = ..., step_input: _Optional[_Union[Value, _Mapping]] = ..., attributes: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., channel_infos: _Optional[_Mapping[str, ChannelInfo]] = ..., loaded_channel_messages: _Optional[_Mapping[str, ChannelValues]] = ..., loaded_attribute_map_instances: _Optional[_Iterable[str]] = ..., loaded_channel_names: _Optional[_Iterable[str]] = ..., loaded_channel_map_instances: _Optional[_Iterable[str]] = ...) -> None: ...

class InvokeWaitForMethodResponse(_message.Message):
    __slots__ = ("local_activity_metadata", "upsert_attributes", "waiting_condition", "upsert_step_exe_locals", "record_events", "publish_to_channel", "delete_from_channel", "action_permission_mappings")
    LOCAL_ACTIVITY_METADATA_FIELD_NUMBER: _ClassVar[int]
    UPSERT_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    WAITING_CONDITION_FIELD_NUMBER: _ClassVar[int]
    UPSERT_STEP_EXE_LOCALS_FIELD_NUMBER: _ClassVar[int]
    RECORD_EVENTS_FIELD_NUMBER: _ClassVar[int]
    PUBLISH_TO_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    DELETE_FROM_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    ACTION_PERMISSION_MAPPINGS_FIELD_NUMBER: _ClassVar[int]
    local_activity_metadata: LocalActivityMetadata
    upsert_attributes: _containers.RepeatedCompositeFieldContainer[AttributeWrite]
    waiting_condition: WaitingCondition
    upsert_step_exe_locals: _containers.RepeatedCompositeFieldContainer[KV]
    record_events: _containers.RepeatedCompositeFieldContainer[KV]
    publish_to_channel: _containers.RepeatedCompositeFieldContainer[ChannelMessage]
    delete_from_channel: _containers.RepeatedCompositeFieldContainer[ChannelMessageDeletion]
    action_permission_mappings: ActionPermissionMappings
    def __init__(self, local_activity_metadata: _Optional[_Union[LocalActivityMetadata, _Mapping]] = ..., upsert_attributes: _Optional[_Iterable[_Union[AttributeWrite, _Mapping]]] = ..., waiting_condition: _Optional[_Union[WaitingCondition, _Mapping]] = ..., upsert_step_exe_locals: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., record_events: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., publish_to_channel: _Optional[_Iterable[_Union[ChannelMessage, _Mapping]]] = ..., delete_from_channel: _Optional[_Iterable[_Union[ChannelMessageDeletion, _Mapping]]] = ..., action_permission_mappings: _Optional[_Union[ActionPermissionMappings, _Mapping]] = ...) -> None: ...

class StepMethodHeartbeat(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: Value
    def __init__(self, value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class StepStreamWrite(_message.Message):
    __slots__ = ("stream_name", "stream_capacity_bytes", "value")
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    STREAM_CAPACITY_BYTES_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    stream_name: str
    stream_capacity_bytes: int
    value: Value
    def __init__(self, stream_name: _Optional[str] = ..., stream_capacity_bytes: _Optional[int] = ..., value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class InvokeWaitForMethodOutput(_message.Message):
    __slots__ = ("heartbeat", "stream_write", "result")
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    STREAM_WRITE_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    heartbeat: StepMethodHeartbeat
    stream_write: StepStreamWrite
    result: InvokeWaitForMethodResponse
    def __init__(self, heartbeat: _Optional[_Union[StepMethodHeartbeat, _Mapping]] = ..., stream_write: _Optional[_Union[StepStreamWrite, _Mapping]] = ..., result: _Optional[_Union[InvokeWaitForMethodResponse, _Mapping]] = ...) -> None: ...

class InvokeExecuteMethodRequest(_message.Message):
    __slots__ = ("context", "flow_type", "step_type", "step_input", "attributes", "step_exe_locals", "condition_results", "channel_infos", "loaded_channel_messages", "loaded_attribute_map_instances", "loaded_channel_names", "loaded_channel_map_instances")
    class ChannelInfosEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ChannelInfo
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ChannelInfo, _Mapping]] = ...) -> None: ...
    class LoadedChannelMessagesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ChannelValues
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ChannelValues, _Mapping]] = ...) -> None: ...
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    FLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    STEP_INPUT_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    STEP_EXE_LOCALS_FIELD_NUMBER: _ClassVar[int]
    CONDITION_RESULTS_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_INFOS_FIELD_NUMBER: _ClassVar[int]
    LOADED_CHANNEL_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    LOADED_ATTRIBUTE_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    LOADED_CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    LOADED_CHANNEL_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    context: Context
    flow_type: str
    step_type: str
    step_input: Value
    attributes: _containers.RepeatedCompositeFieldContainer[KV]
    step_exe_locals: _containers.RepeatedCompositeFieldContainer[KV]
    condition_results: ConditionResults
    channel_infos: _containers.MessageMap[str, ChannelInfo]
    loaded_channel_messages: _containers.MessageMap[str, ChannelValues]
    loaded_attribute_map_instances: _containers.RepeatedScalarFieldContainer[str]
    loaded_channel_names: _containers.RepeatedScalarFieldContainer[str]
    loaded_channel_map_instances: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, context: _Optional[_Union[Context, _Mapping]] = ..., flow_type: _Optional[str] = ..., step_type: _Optional[str] = ..., step_input: _Optional[_Union[Value, _Mapping]] = ..., attributes: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., step_exe_locals: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., condition_results: _Optional[_Union[ConditionResults, _Mapping]] = ..., channel_infos: _Optional[_Mapping[str, ChannelInfo]] = ..., loaded_channel_messages: _Optional[_Mapping[str, ChannelValues]] = ..., loaded_attribute_map_instances: _Optional[_Iterable[str]] = ..., loaded_channel_names: _Optional[_Iterable[str]] = ..., loaded_channel_map_instances: _Optional[_Iterable[str]] = ...) -> None: ...

class InvokeExecuteMethodResponse(_message.Message):
    __slots__ = ("local_activity_metadata", "step_decision", "upsert_attributes", "record_events", "upsert_step_exe_locals", "publish_to_channel", "delete_from_channel", "action_permission_mappings")
    LOCAL_ACTIVITY_METADATA_FIELD_NUMBER: _ClassVar[int]
    STEP_DECISION_FIELD_NUMBER: _ClassVar[int]
    UPSERT_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    RECORD_EVENTS_FIELD_NUMBER: _ClassVar[int]
    UPSERT_STEP_EXE_LOCALS_FIELD_NUMBER: _ClassVar[int]
    PUBLISH_TO_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    DELETE_FROM_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    ACTION_PERMISSION_MAPPINGS_FIELD_NUMBER: _ClassVar[int]
    local_activity_metadata: LocalActivityMetadata
    step_decision: StepDecision
    upsert_attributes: _containers.RepeatedCompositeFieldContainer[AttributeWrite]
    record_events: _containers.RepeatedCompositeFieldContainer[KV]
    upsert_step_exe_locals: _containers.RepeatedCompositeFieldContainer[KV]
    publish_to_channel: _containers.RepeatedCompositeFieldContainer[ChannelMessage]
    delete_from_channel: _containers.RepeatedCompositeFieldContainer[ChannelMessageDeletion]
    action_permission_mappings: ActionPermissionMappings
    def __init__(self, local_activity_metadata: _Optional[_Union[LocalActivityMetadata, _Mapping]] = ..., step_decision: _Optional[_Union[StepDecision, _Mapping]] = ..., upsert_attributes: _Optional[_Iterable[_Union[AttributeWrite, _Mapping]]] = ..., record_events: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., upsert_step_exe_locals: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., publish_to_channel: _Optional[_Iterable[_Union[ChannelMessage, _Mapping]]] = ..., delete_from_channel: _Optional[_Iterable[_Union[ChannelMessageDeletion, _Mapping]]] = ..., action_permission_mappings: _Optional[_Union[ActionPermissionMappings, _Mapping]] = ...) -> None: ...

class InvokeExecuteMethodOutput(_message.Message):
    __slots__ = ("heartbeat", "stream_write", "result")
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    STREAM_WRITE_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    heartbeat: StepMethodHeartbeat
    stream_write: StepStreamWrite
    result: InvokeExecuteMethodResponse
    def __init__(self, heartbeat: _Optional[_Union[StepMethodHeartbeat, _Mapping]] = ..., stream_write: _Optional[_Union[StepStreamWrite, _Mapping]] = ..., result: _Optional[_Union[InvokeExecuteMethodResponse, _Mapping]] = ...) -> None: ...

class InvokeWorkerRPCRequest(_message.Message):
    __slots__ = ("context", "flow_type", "rpc_name", "input", "attributes", "channel_infos", "loaded_channel_messages", "loaded_attribute_map_instances", "loaded_channel_names", "loaded_channel_map_instances")
    class ChannelInfosEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ChannelInfo
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ChannelInfo, _Mapping]] = ...) -> None: ...
    class LoadedChannelMessagesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ChannelValues
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ChannelValues, _Mapping]] = ...) -> None: ...
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    FLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    RPC_NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_INFOS_FIELD_NUMBER: _ClassVar[int]
    LOADED_CHANNEL_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    LOADED_ATTRIBUTE_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    LOADED_CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    LOADED_CHANNEL_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    context: Context
    flow_type: str
    rpc_name: str
    input: Value
    attributes: _containers.RepeatedCompositeFieldContainer[KV]
    channel_infos: _containers.MessageMap[str, ChannelInfo]
    loaded_channel_messages: _containers.MessageMap[str, ChannelValues]
    loaded_attribute_map_instances: _containers.RepeatedScalarFieldContainer[str]
    loaded_channel_names: _containers.RepeatedScalarFieldContainer[str]
    loaded_channel_map_instances: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, context: _Optional[_Union[Context, _Mapping]] = ..., flow_type: _Optional[str] = ..., rpc_name: _Optional[str] = ..., input: _Optional[_Union[Value, _Mapping]] = ..., attributes: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., channel_infos: _Optional[_Mapping[str, ChannelInfo]] = ..., loaded_channel_messages: _Optional[_Mapping[str, ChannelValues]] = ..., loaded_attribute_map_instances: _Optional[_Iterable[str]] = ..., loaded_channel_names: _Optional[_Iterable[str]] = ..., loaded_channel_map_instances: _Optional[_Iterable[str]] = ...) -> None: ...

class InvokeWorkerRPCResponse(_message.Message):
    __slots__ = ("output", "step_decision", "upsert_attributes", "record_events", "delete_from_channel", "publish_to_channel", "action_permission_mappings")
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    STEP_DECISION_FIELD_NUMBER: _ClassVar[int]
    UPSERT_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    RECORD_EVENTS_FIELD_NUMBER: _ClassVar[int]
    DELETE_FROM_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    PUBLISH_TO_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    ACTION_PERMISSION_MAPPINGS_FIELD_NUMBER: _ClassVar[int]
    output: Value
    step_decision: StepDecision
    upsert_attributes: _containers.RepeatedCompositeFieldContainer[AttributeWrite]
    record_events: _containers.RepeatedCompositeFieldContainer[KV]
    delete_from_channel: _containers.RepeatedCompositeFieldContainer[ChannelMessageDeletion]
    publish_to_channel: _containers.RepeatedCompositeFieldContainer[ChannelMessage]
    action_permission_mappings: ActionPermissionMappings
    def __init__(self, output: _Optional[_Union[Value, _Mapping]] = ..., step_decision: _Optional[_Union[StepDecision, _Mapping]] = ..., upsert_attributes: _Optional[_Iterable[_Union[AttributeWrite, _Mapping]]] = ..., record_events: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., delete_from_channel: _Optional[_Iterable[_Union[ChannelMessageDeletion, _Mapping]]] = ..., publish_to_channel: _Optional[_Iterable[_Union[ChannelMessage, _Mapping]]] = ..., action_permission_mappings: _Optional[_Union[ActionPermissionMappings, _Mapping]] = ...) -> None: ...

class StepDecision(_message.Message):
    __slots__ = ("next_steps", "close_decision", "cancel_step_types", "cancel_sibling_step_types")
    NEXT_STEPS_FIELD_NUMBER: _ClassVar[int]
    CLOSE_DECISION_FIELD_NUMBER: _ClassVar[int]
    CANCEL_STEP_TYPES_FIELD_NUMBER: _ClassVar[int]
    CANCEL_SIBLING_STEP_TYPES_FIELD_NUMBER: _ClassVar[int]
    next_steps: _containers.RepeatedCompositeFieldContainer[StepMovement]
    close_decision: CloseDecision
    cancel_step_types: _containers.RepeatedScalarFieldContainer[str]
    cancel_sibling_step_types: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, next_steps: _Optional[_Iterable[_Union[StepMovement, _Mapping]]] = ..., close_decision: _Optional[_Union[CloseDecision, _Mapping]] = ..., cancel_step_types: _Optional[_Iterable[str]] = ..., cancel_sibling_step_types: _Optional[_Iterable[str]] = ...) -> None: ...

class CloseDecision(_message.Message):
    __slots__ = ("close_decision_type", "conditional_channel_names", "close_input")
    CLOSE_DECISION_TYPE_FIELD_NUMBER: _ClassVar[int]
    CONDITIONAL_CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    CLOSE_INPUT_FIELD_NUMBER: _ClassVar[int]
    close_decision_type: CloseDecisionType
    conditional_channel_names: _containers.RepeatedScalarFieldContainer[str]
    close_input: Value
    def __init__(self, close_decision_type: _Optional[_Union[CloseDecisionType, str]] = ..., conditional_channel_names: _Optional[_Iterable[str]] = ..., close_input: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class StepMovement(_message.Message):
    __slots__ = ("step_type", "step_input", "step_options", "from_step_execution_id_internal_only", "recovery_error_internal_only")
    STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    STEP_INPUT_FIELD_NUMBER: _ClassVar[int]
    STEP_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    FROM_STEP_EXECUTION_ID_INTERNAL_ONLY_FIELD_NUMBER: _ClassVar[int]
    RECOVERY_ERROR_INTERNAL_ONLY_FIELD_NUMBER: _ClassVar[int]
    step_type: str
    step_input: Value
    step_options: StepOptions
    from_step_execution_id_internal_only: str
    recovery_error_internal_only: RecoveryErrorInfo
    def __init__(self, step_type: _Optional[str] = ..., step_input: _Optional[_Union[Value, _Mapping]] = ..., step_options: _Optional[_Union[StepOptions, _Mapping]] = ..., from_step_execution_id_internal_only: _Optional[str] = ..., recovery_error_internal_only: _Optional[_Union[RecoveryErrorInfo, _Mapping]] = ...) -> None: ...

class ConditionCombination(_message.Message):
    __slots__ = ("condition_ids",)
    CONDITION_IDS_FIELD_NUMBER: _ClassVar[int]
    condition_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, condition_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class WaitingCondition(_message.Message):
    __slots__ = ("waiting_condition_type", "timer_conditions", "channel_conditions", "condition_combinations", "sub_flow_conditions")
    WAITING_CONDITION_TYPE_FIELD_NUMBER: _ClassVar[int]
    TIMER_CONDITIONS_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_CONDITIONS_FIELD_NUMBER: _ClassVar[int]
    CONDITION_COMBINATIONS_FIELD_NUMBER: _ClassVar[int]
    SUB_FLOW_CONDITIONS_FIELD_NUMBER: _ClassVar[int]
    waiting_condition_type: WaitingConditionType
    timer_conditions: _containers.RepeatedCompositeFieldContainer[TimerCondition]
    channel_conditions: _containers.RepeatedCompositeFieldContainer[ChannelCondition]
    condition_combinations: _containers.RepeatedCompositeFieldContainer[ConditionCombination]
    sub_flow_conditions: _containers.RepeatedCompositeFieldContainer[SubFlowCondition]
    def __init__(self, waiting_condition_type: _Optional[_Union[WaitingConditionType, str]] = ..., timer_conditions: _Optional[_Iterable[_Union[TimerCondition, _Mapping]]] = ..., channel_conditions: _Optional[_Iterable[_Union[ChannelCondition, _Mapping]]] = ..., condition_combinations: _Optional[_Iterable[_Union[ConditionCombination, _Mapping]]] = ..., sub_flow_conditions: _Optional[_Iterable[_Union[SubFlowCondition, _Mapping]]] = ...) -> None: ...

class WaitingConditionState(_message.Message):
    __slots__ = ("waiting_condition_type", "timer_conditions", "channel_conditions", "condition_combinations", "sub_flow_conditions")
    WAITING_CONDITION_TYPE_FIELD_NUMBER: _ClassVar[int]
    TIMER_CONDITIONS_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_CONDITIONS_FIELD_NUMBER: _ClassVar[int]
    CONDITION_COMBINATIONS_FIELD_NUMBER: _ClassVar[int]
    SUB_FLOW_CONDITIONS_FIELD_NUMBER: _ClassVar[int]
    waiting_condition_type: WaitingConditionType
    timer_conditions: _containers.RepeatedCompositeFieldContainer[TimerCondition]
    channel_conditions: _containers.RepeatedCompositeFieldContainer[ChannelCondition]
    condition_combinations: _containers.RepeatedCompositeFieldContainer[ConditionCombination]
    sub_flow_conditions: _containers.RepeatedCompositeFieldContainer[SubFlowConditionState]
    def __init__(self, waiting_condition_type: _Optional[_Union[WaitingConditionType, str]] = ..., timer_conditions: _Optional[_Iterable[_Union[TimerCondition, _Mapping]]] = ..., channel_conditions: _Optional[_Iterable[_Union[ChannelCondition, _Mapping]]] = ..., condition_combinations: _Optional[_Iterable[_Union[ConditionCombination, _Mapping]]] = ..., sub_flow_conditions: _Optional[_Iterable[_Union[SubFlowConditionState, _Mapping]]] = ...) -> None: ...

class SubFlowOptions(_message.Message):
    __slots__ = ("reuse_policy", "flow_timeout_seconds", "flow_start_delay_seconds", "retry_policy", "attributes", "flow_config_override", "flow_timeout_policy", "timeout_handler_options")
    REUSE_POLICY_FIELD_NUMBER: _ClassVar[int]
    FLOW_TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    FLOW_START_DELAY_SECONDS_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    FLOW_CONFIG_OVERRIDE_FIELD_NUMBER: _ClassVar[int]
    FLOW_TIMEOUT_POLICY_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_HANDLER_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    reuse_policy: SubFlowReusePolicy
    flow_timeout_seconds: int
    flow_start_delay_seconds: int
    retry_policy: FlowRetryPolicy
    attributes: _containers.RepeatedCompositeFieldContainer[AttributeWrite]
    flow_config_override: FlowConfig
    flow_timeout_policy: FlowTimeoutPolicy
    timeout_handler_options: FlowTimeoutHandlerOptions
    def __init__(self, reuse_policy: _Optional[_Union[SubFlowReusePolicy, str]] = ..., flow_timeout_seconds: _Optional[int] = ..., flow_start_delay_seconds: _Optional[int] = ..., retry_policy: _Optional[_Union[FlowRetryPolicy, _Mapping]] = ..., attributes: _Optional[_Iterable[_Union[AttributeWrite, _Mapping]]] = ..., flow_config_override: _Optional[_Union[FlowConfig, _Mapping]] = ..., flow_timeout_policy: _Optional[_Union[FlowTimeoutPolicy, str]] = ..., timeout_handler_options: _Optional[_Union[FlowTimeoutHandlerOptions, _Mapping]] = ...) -> None: ...

class SubFlowCondition(_message.Message):
    __slots__ = ("condition_id", "sub_flow_type", "start_step_type", "step_input", "step_options", "options", "sub_flow_index")
    CONDITION_ID_FIELD_NUMBER: _ClassVar[int]
    SUB_FLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    START_STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    STEP_INPUT_FIELD_NUMBER: _ClassVar[int]
    STEP_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SUB_FLOW_INDEX_FIELD_NUMBER: _ClassVar[int]
    condition_id: str
    sub_flow_type: str
    start_step_type: str
    step_input: Value
    step_options: StepOptions
    options: SubFlowOptions
    sub_flow_index: int
    def __init__(self, condition_id: _Optional[str] = ..., sub_flow_type: _Optional[str] = ..., start_step_type: _Optional[str] = ..., step_input: _Optional[_Union[Value, _Mapping]] = ..., step_options: _Optional[_Union[StepOptions, _Mapping]] = ..., options: _Optional[_Union[SubFlowOptions, _Mapping]] = ..., sub_flow_index: _Optional[int] = ...) -> None: ...

class SubFlowConditionState(_message.Message):
    __slots__ = ("condition_id",)
    CONDITION_ID_FIELD_NUMBER: _ClassVar[int]
    condition_id: str
    def __init__(self, condition_id: _Optional[str] = ...) -> None: ...

class TimerCondition(_message.Message):
    __slots__ = ("condition_id", "duration_seconds", "firing_unix_timestamp_seconds")
    CONDITION_ID_FIELD_NUMBER: _ClassVar[int]
    DURATION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    FIRING_UNIX_TIMESTAMP_SECONDS_FIELD_NUMBER: _ClassVar[int]
    condition_id: str
    duration_seconds: int
    firing_unix_timestamp_seconds: int
    def __init__(self, condition_id: _Optional[str] = ..., duration_seconds: _Optional[int] = ..., firing_unix_timestamp_seconds: _Optional[int] = ...) -> None: ...

class ChannelCondition(_message.Message):
    __slots__ = ("condition_id", "channel_name", "at_least", "at_most")
    CONDITION_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_NAME_FIELD_NUMBER: _ClassVar[int]
    AT_LEAST_FIELD_NUMBER: _ClassVar[int]
    AT_MOST_FIELD_NUMBER: _ClassVar[int]
    condition_id: str
    channel_name: str
    at_least: int
    at_most: int
    def __init__(self, condition_id: _Optional[str] = ..., channel_name: _Optional[str] = ..., at_least: _Optional[int] = ..., at_most: _Optional[int] = ...) -> None: ...

class ConditionResults(_message.Message):
    __slots__ = ("channel_results", "timer_results", "wait_for_failed", "sub_flow_results")
    CHANNEL_RESULTS_FIELD_NUMBER: _ClassVar[int]
    TIMER_RESULTS_FIELD_NUMBER: _ClassVar[int]
    WAIT_FOR_FAILED_FIELD_NUMBER: _ClassVar[int]
    SUB_FLOW_RESULTS_FIELD_NUMBER: _ClassVar[int]
    channel_results: _containers.RepeatedCompositeFieldContainer[ChannelResult]
    timer_results: _containers.RepeatedCompositeFieldContainer[TimerResult]
    wait_for_failed: bool
    sub_flow_results: _containers.RepeatedCompositeFieldContainer[FlowResult]
    def __init__(self, channel_results: _Optional[_Iterable[_Union[ChannelResult, _Mapping]]] = ..., timer_results: _Optional[_Iterable[_Union[TimerResult, _Mapping]]] = ..., wait_for_failed: _Optional[bool] = ..., sub_flow_results: _Optional[_Iterable[_Union[FlowResult, _Mapping]]] = ...) -> None: ...

class TimerResult(_message.Message):
    __slots__ = ("condition_id", "condition_status")
    CONDITION_ID_FIELD_NUMBER: _ClassVar[int]
    CONDITION_STATUS_FIELD_NUMBER: _ClassVar[int]
    condition_id: str
    condition_status: ConditionStatus
    def __init__(self, condition_id: _Optional[str] = ..., condition_status: _Optional[_Union[ConditionStatus, str]] = ...) -> None: ...

class ChannelResult(_message.Message):
    __slots__ = ("condition_id", "condition_status", "channel_name", "values")
    CONDITION_ID_FIELD_NUMBER: _ClassVar[int]
    CONDITION_STATUS_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    condition_id: str
    condition_status: ConditionStatus
    channel_name: str
    values: _containers.RepeatedCompositeFieldContainer[Value]
    def __init__(self, condition_id: _Optional[str] = ..., condition_status: _Optional[_Union[ConditionStatus, str]] = ..., channel_name: _Optional[str] = ..., values: _Optional[_Iterable[_Union[Value, _Mapping]]] = ...) -> None: ...

class ContinueAsNewDumpRequest(_message.Message):
    __slots__ = ("flow_id", "run_id", "page_num", "page_size_in_bytes")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_NUM_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    run_id: str
    page_num: int
    page_size_in_bytes: int
    def __init__(self, flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., page_num: _Optional[int] = ..., page_size_in_bytes: _Optional[int] = ...) -> None: ...

class ContinueAsNewDumpResponse(_message.Message):
    __slots__ = ("page_content", "page_num", "total_pages", "checksum")
    PAGE_CONTENT_FIELD_NUMBER: _ClassVar[int]
    PAGE_NUM_FIELD_NUMBER: _ClassVar[int]
    TOTAL_PAGES_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    page_content: bytes
    page_num: int
    total_pages: int
    checksum: str
    def __init__(self, page_content: _Optional[bytes] = ..., page_num: _Optional[int] = ..., total_pages: _Optional[int] = ..., checksum: _Optional[str] = ...) -> None: ...

class ChannelValues(_message.Message):
    __slots__ = ("messages",)
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[ChannelMessage]
    def __init__(self, messages: _Optional[_Iterable[_Union[ChannelMessage, _Mapping]]] = ...) -> None: ...

class StepExecutionCompletedConditions(_message.Message):
    __slots__ = ("completed_timer_conditions", "completed_sub_flow_results")
    class CompletedTimerConditionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: InternalTimerStatus
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[InternalTimerStatus, str]] = ...) -> None: ...
    class CompletedSubFlowResultsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: FlowResult
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[FlowResult, _Mapping]] = ...) -> None: ...
    COMPLETED_TIMER_CONDITIONS_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_SUB_FLOW_RESULTS_FIELD_NUMBER: _ClassVar[int]
    completed_timer_conditions: _containers.ScalarMap[int, InternalTimerStatus]
    completed_sub_flow_results: _containers.MessageMap[int, FlowResult]
    def __init__(self, completed_timer_conditions: _Optional[_Mapping[int, InternalTimerStatus]] = ..., completed_sub_flow_results: _Optional[_Mapping[int, FlowResult]] = ...) -> None: ...

class StepExecutionResumeInfo(_message.Message):
    __slots__ = ("step_execution_id", "step", "completed_conditions", "waiting_condition", "step_exe_locals")
    STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_CONDITIONS_FIELD_NUMBER: _ClassVar[int]
    WAITING_CONDITION_FIELD_NUMBER: _ClassVar[int]
    STEP_EXE_LOCALS_FIELD_NUMBER: _ClassVar[int]
    step_execution_id: str
    step: StepMovement
    completed_conditions: StepExecutionCompletedConditions
    waiting_condition: WaitingConditionState
    step_exe_locals: _containers.RepeatedCompositeFieldContainer[KV]
    def __init__(self, step_execution_id: _Optional[str] = ..., step: _Optional[_Union[StepMovement, _Mapping]] = ..., completed_conditions: _Optional[_Union[StepExecutionCompletedConditions, _Mapping]] = ..., waiting_condition: _Optional[_Union[WaitingConditionState, _Mapping]] = ..., step_exe_locals: _Optional[_Iterable[_Union[KV, _Mapping]]] = ...) -> None: ...

class StepExecutionCounterInfo(_message.Message):
    __slots__ = ("step_type_started_count", "step_type_currently_executing_count", "total_currently_executing_count", "step_active_execution_nums")
    class StepTypeStartedCountEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    class StepTypeCurrentlyExecutingCountEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    class StepActiveExecutionNumsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: StepExecutionNumbers
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[StepExecutionNumbers, _Mapping]] = ...) -> None: ...
    STEP_TYPE_STARTED_COUNT_FIELD_NUMBER: _ClassVar[int]
    STEP_TYPE_CURRENTLY_EXECUTING_COUNT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_CURRENTLY_EXECUTING_COUNT_FIELD_NUMBER: _ClassVar[int]
    STEP_ACTIVE_EXECUTION_NUMS_FIELD_NUMBER: _ClassVar[int]
    step_type_started_count: _containers.ScalarMap[str, int]
    step_type_currently_executing_count: _containers.ScalarMap[str, int]
    total_currently_executing_count: int
    step_active_execution_nums: _containers.MessageMap[str, StepExecutionNumbers]
    def __init__(self, step_type_started_count: _Optional[_Mapping[str, int]] = ..., step_type_currently_executing_count: _Optional[_Mapping[str, int]] = ..., total_currently_executing_count: _Optional[int] = ..., step_active_execution_nums: _Optional[_Mapping[str, StepExecutionNumbers]] = ...) -> None: ...

class StaleSkipTimer(_message.Message):
    __slots__ = ("step_execution_id", "timer_condition_id", "timer_condition_index")
    STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    TIMER_CONDITION_ID_FIELD_NUMBER: _ClassVar[int]
    TIMER_CONDITION_INDEX_FIELD_NUMBER: _ClassVar[int]
    step_execution_id: str
    timer_condition_id: str
    timer_condition_index: int
    def __init__(self, step_execution_id: _Optional[str] = ..., timer_condition_id: _Optional[str] = ..., timer_condition_index: _Optional[int] = ...) -> None: ...

class ContinueAsNewDump(_message.Message):
    __slots__ = ("steps_to_start_from_beginning", "step_executions_to_resume", "channel_received", "counter_info", "step_outputs", "stale_skip_timers", "attributes", "pending_attribute_sync_items")
    class ChannelReceivedEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ChannelValues
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ChannelValues, _Mapping]] = ...) -> None: ...
    STEPS_TO_START_FROM_BEGINNING_FIELD_NUMBER: _ClassVar[int]
    STEP_EXECUTIONS_TO_RESUME_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_RECEIVED_FIELD_NUMBER: _ClassVar[int]
    COUNTER_INFO_FIELD_NUMBER: _ClassVar[int]
    STEP_OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    STALE_SKIP_TIMERS_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    PENDING_ATTRIBUTE_SYNC_ITEMS_FIELD_NUMBER: _ClassVar[int]
    steps_to_start_from_beginning: _containers.RepeatedCompositeFieldContainer[StepMovement]
    step_executions_to_resume: _containers.RepeatedCompositeFieldContainer[StepExecutionResumeInfo]
    channel_received: _containers.MessageMap[str, ChannelValues]
    counter_info: StepExecutionCounterInfo
    step_outputs: _containers.RepeatedCompositeFieldContainer[StepCompletionOutput]
    stale_skip_timers: _containers.RepeatedCompositeFieldContainer[StaleSkipTimer]
    attributes: _containers.RepeatedCompositeFieldContainer[KV]
    pending_attribute_sync_items: _containers.RepeatedCompositeFieldContainer[AttributeSyncItem]
    def __init__(self, steps_to_start_from_beginning: _Optional[_Iterable[_Union[StepMovement, _Mapping]]] = ..., step_executions_to_resume: _Optional[_Iterable[_Union[StepExecutionResumeInfo, _Mapping]]] = ..., channel_received: _Optional[_Mapping[str, ChannelValues]] = ..., counter_info: _Optional[_Union[StepExecutionCounterInfo, _Mapping]] = ..., step_outputs: _Optional[_Iterable[_Union[StepCompletionOutput, _Mapping]]] = ..., stale_skip_timers: _Optional[_Iterable[_Union[StaleSkipTimer, _Mapping]]] = ..., attributes: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., pending_attribute_sync_items: _Optional[_Iterable[_Union[AttributeSyncItem, _Mapping]]] = ...) -> None: ...

class ContinueAsNewInput(_message.Message):
    __slots__ = ("previous_internal_run_id",)
    PREVIOUS_INTERNAL_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    previous_internal_run_id: str
    def __init__(self, previous_internal_run_id: _Optional[str] = ...) -> None: ...

class InterpreterWorkflowInput(_message.Message):
    __slots__ = ("flow_type", "configured_flow_timeout_seconds", "flow_timeout_policy", "start_step_type", "step_input", "step_options", "init_attributes", "config", "is_resume_from_continue_as_new", "continue_as_new_input", "timeout_handler_options", "tenant_id")
    FLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    CONFIGURED_FLOW_TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    FLOW_TIMEOUT_POLICY_FIELD_NUMBER: _ClassVar[int]
    START_STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    STEP_INPUT_FIELD_NUMBER: _ClassVar[int]
    STEP_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    INIT_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    IS_RESUME_FROM_CONTINUE_AS_NEW_FIELD_NUMBER: _ClassVar[int]
    CONTINUE_AS_NEW_INPUT_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_HANDLER_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    flow_type: str
    configured_flow_timeout_seconds: int
    flow_timeout_policy: FlowTimeoutPolicy
    start_step_type: str
    step_input: Value
    step_options: StepOptions
    init_attributes: _containers.RepeatedCompositeFieldContainer[AttributeWrite]
    config: FlowConfig
    is_resume_from_continue_as_new: bool
    continue_as_new_input: ContinueAsNewInput
    timeout_handler_options: FlowTimeoutHandlerOptions
    tenant_id: str
    def __init__(self, flow_type: _Optional[str] = ..., configured_flow_timeout_seconds: _Optional[int] = ..., flow_timeout_policy: _Optional[_Union[FlowTimeoutPolicy, str]] = ..., start_step_type: _Optional[str] = ..., step_input: _Optional[_Union[Value, _Mapping]] = ..., step_options: _Optional[_Union[StepOptions, _Mapping]] = ..., init_attributes: _Optional[_Iterable[_Union[AttributeWrite, _Mapping]]] = ..., config: _Optional[_Union[FlowConfig, _Mapping]] = ..., is_resume_from_continue_as_new: _Optional[bool] = ..., continue_as_new_input: _Optional[_Union[ContinueAsNewInput, _Mapping]] = ..., timeout_handler_options: _Optional[_Union[FlowTimeoutHandlerOptions, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class InterpreterWorkflowOutput(_message.Message):
    __slots__ = ("step_completion_outputs",)
    STEP_COMPLETION_OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    step_completion_outputs: _containers.RepeatedCompositeFieldContainer[StepCompletionOutput]
    def __init__(self, step_completion_outputs: _Optional[_Iterable[_Union[StepCompletionOutput, _Mapping]]] = ...) -> None: ...

class BlobStoreCleanupWorkflowInput(_message.Message):
    __slots__ = ("store_id",)
    STORE_ID_FIELD_NUMBER: _ClassVar[int]
    store_id: str
    def __init__(self, store_id: _Optional[str] = ...) -> None: ...

class BlobStoreCleanupWorkflowOutput(_message.Message):
    __slots__ = ("total_deleted",)
    TOTAL_DELETED_FIELD_NUMBER: _ClassVar[int]
    total_deleted: int
    def __init__(self, total_deleted: _Optional[int] = ...) -> None: ...

class InvokeWaitForMethodActivityInput(_message.Message):
    __slots__ = ("worker_target", "request", "tenant_id")
    WORKER_TARGET_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    worker_target: WorkerTarget
    request: InvokeWaitForMethodRequest
    tenant_id: str
    def __init__(self, worker_target: _Optional[_Union[WorkerTarget, _Mapping]] = ..., request: _Optional[_Union[InvokeWaitForMethodRequest, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class InvokeWaitForMethodActivityOutput(_message.Message):
    __slots__ = ("response",)
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    response: InvokeWaitForMethodResponse
    def __init__(self, response: _Optional[_Union[InvokeWaitForMethodResponse, _Mapping]] = ...) -> None: ...

class InvokeExecuteMethodActivityInput(_message.Message):
    __slots__ = ("worker_target", "request", "tenant_id")
    WORKER_TARGET_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    worker_target: WorkerTarget
    request: InvokeExecuteMethodRequest
    tenant_id: str
    def __init__(self, worker_target: _Optional[_Union[WorkerTarget, _Mapping]] = ..., request: _Optional[_Union[InvokeExecuteMethodRequest, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class RecoveryErrorInfo(_message.Message):
    __slots__ = ("detail", "error_type")
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    detail: str
    error_type: str
    def __init__(self, detail: _Optional[str] = ..., error_type: _Optional[str] = ...) -> None: ...

class InternalLocalStepActivityFailure(_message.Message):
    __slots__ = ("local_activity_metadata", "first_attempt_timestamp", "method_options", "attempt", "activity_error")
    LOCAL_ACTIVITY_METADATA_FIELD_NUMBER: _ClassVar[int]
    FIRST_ATTEMPT_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    METHOD_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_ERROR_FIELD_NUMBER: _ClassVar[int]
    local_activity_metadata: LocalActivityMetadata
    first_attempt_timestamp: int
    method_options: StepMethodOptions
    attempt: int
    activity_error: InternalActivityError
    def __init__(self, local_activity_metadata: _Optional[_Union[LocalActivityMetadata, _Mapping]] = ..., first_attempt_timestamp: _Optional[int] = ..., method_options: _Optional[_Union[StepMethodOptions, _Mapping]] = ..., attempt: _Optional[int] = ..., activity_error: _Optional[_Union[InternalActivityError, _Mapping]] = ...) -> None: ...

class InvokeExecuteMethodActivityOutput(_message.Message):
    __slots__ = ("response",)
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    response: InvokeExecuteMethodResponse
    def __init__(self, response: _Optional[_Union[InvokeExecuteMethodResponse, _Mapping]] = ...) -> None: ...

class DumpFlowForContinueAsNewActivityInput(_message.Message):
    __slots__ = ("request",)
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    request: ContinueAsNewDumpRequest
    def __init__(self, request: _Optional[_Union[ContinueAsNewDumpRequest, _Mapping]] = ...) -> None: ...

class DumpFlowForContinueAsNewActivityOutput(_message.Message):
    __slots__ = ("response",)
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    response: ContinueAsNewDumpResponse
    def __init__(self, response: _Optional[_Union[ContinueAsNewDumpResponse, _Mapping]] = ...) -> None: ...

class InvokeWorkerRPCActivityInput(_message.Message):
    __slots__ = ("rpc_prep", "request", "tenant_id")
    RPC_PREP_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    rpc_prep: PrepareRpcQueryResponse
    request: InvokeRPCRequest
    tenant_id: str
    def __init__(self, rpc_prep: _Optional[_Union[PrepareRpcQueryResponse, _Mapping]] = ..., request: _Optional[_Union[InvokeRPCRequest, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class InvokeWorkerRPCActivityOutput(_message.Message):
    __slots__ = ("response", "request_id")
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    response: InvokeWorkerRPCResponse
    request_id: str
    def __init__(self, response: _Optional[_Union[InvokeWorkerRPCResponse, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class CleanupBlobStoreActivityInput(_message.Message):
    __slots__ = ("store_id",)
    STORE_ID_FIELD_NUMBER: _ClassVar[int]
    store_id: str
    def __init__(self, store_id: _Optional[str] = ...) -> None: ...

class CleanupBlobStoreActivityOutput(_message.Message):
    __slots__ = ("total_deleted",)
    TOTAL_DELETED_FIELD_NUMBER: _ClassVar[int]
    total_deleted: int
    def __init__(self, total_deleted: _Optional[int] = ...) -> None: ...

class AttributeSyncItem(_message.Message):
    __slots__ = ("config_name", "key", "value")
    CONFIG_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    config_name: str
    key: str
    value: Value
    def __init__(self, config_name: _Optional[str] = ..., key: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class SyncAttributeBatchActivityInput(_message.Message):
    __slots__ = ("flow_id", "config_name", "items")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_NAME_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    config_name: str
    items: _containers.RepeatedCompositeFieldContainer[AttributeSyncItem]
    def __init__(self, flow_id: _Optional[str] = ..., config_name: _Optional[str] = ..., items: _Optional[_Iterable[_Union[AttributeSyncItem, _Mapping]]] = ...) -> None: ...

class StartSubFlowActivityInput(_message.Message):
    __slots__ = ("condition", "parent_flow_config", "parent_step_execution_id", "tenant_id")
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    PARENT_FLOW_CONFIG_FIELD_NUMBER: _ClassVar[int]
    PARENT_STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    condition: SubFlowCondition
    parent_flow_config: FlowConfig
    parent_step_execution_id: str
    tenant_id: str
    def __init__(self, condition: _Optional[_Union[SubFlowCondition, _Mapping]] = ..., parent_flow_config: _Optional[_Union[FlowConfig, _Mapping]] = ..., parent_step_execution_id: _Optional[str] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class StartSubFlowActivityOutput(_message.Message):
    __slots__ = ("immediate_flow_result",)
    IMMEDIATE_FLOW_RESULT_FIELD_NUMBER: _ClassVar[int]
    immediate_flow_result: FlowResult
    def __init__(self, immediate_flow_result: _Optional[_Union[FlowResult, _Mapping]] = ...) -> None: ...

class SubFlowCompletionSignalRequest(_message.Message):
    __slots__ = ("sub_flow_id", "flow_result")
    SUB_FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    FLOW_RESULT_FIELD_NUMBER: _ClassVar[int]
    sub_flow_id: str
    flow_result: FlowResult
    def __init__(self, sub_flow_id: _Optional[str] = ..., flow_result: _Optional[_Union[FlowResult, _Mapping]] = ...) -> None: ...

class ReportSubFlowCompletionActivityInput(_message.Message):
    __slots__ = ("parent_flow_id", "request")
    PARENT_FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    parent_flow_id: str
    request: SubFlowCompletionSignalRequest
    def __init__(self, parent_flow_id: _Optional[str] = ..., request: _Optional[_Union[SubFlowCompletionSignalRequest, _Mapping]] = ...) -> None: ...

class ReportSubFlowCompletionActivityOutput(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: SubFlowCompletionDeliveryStatus
    def __init__(self, status: _Optional[_Union[SubFlowCompletionDeliveryStatus, str]] = ...) -> None: ...

class ExecuteRpcSignalRequest(_message.Message):
    __slots__ = ("upsert_attributes", "step_decision", "record_events", "publish_to_channel", "is_set_attribute_api", "delete_from_channel", "is_delete_channel_message_api", "action_permission_mappings")
    UPSERT_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    STEP_DECISION_FIELD_NUMBER: _ClassVar[int]
    RECORD_EVENTS_FIELD_NUMBER: _ClassVar[int]
    PUBLISH_TO_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    IS_SET_ATTRIBUTE_API_FIELD_NUMBER: _ClassVar[int]
    DELETE_FROM_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    IS_DELETE_CHANNEL_MESSAGE_API_FIELD_NUMBER: _ClassVar[int]
    ACTION_PERMISSION_MAPPINGS_FIELD_NUMBER: _ClassVar[int]
    upsert_attributes: _containers.RepeatedCompositeFieldContainer[AttributeWrite]
    step_decision: StepDecision
    record_events: _containers.RepeatedCompositeFieldContainer[KV]
    publish_to_channel: _containers.RepeatedCompositeFieldContainer[ChannelMessage]
    is_set_attribute_api: bool
    delete_from_channel: _containers.RepeatedCompositeFieldContainer[ChannelMessageDeletion]
    is_delete_channel_message_api: bool
    action_permission_mappings: ActionPermissionMappings
    def __init__(self, upsert_attributes: _Optional[_Iterable[_Union[AttributeWrite, _Mapping]]] = ..., step_decision: _Optional[_Union[StepDecision, _Mapping]] = ..., record_events: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., publish_to_channel: _Optional[_Iterable[_Union[ChannelMessage, _Mapping]]] = ..., is_set_attribute_api: _Optional[bool] = ..., delete_from_channel: _Optional[_Iterable[_Union[ChannelMessageDeletion, _Mapping]]] = ..., is_delete_channel_message_api: _Optional[bool] = ..., action_permission_mappings: _Optional[_Union[ActionPermissionMappings, _Mapping]] = ...) -> None: ...

class SkipTimerSignalRequest(_message.Message):
    __slots__ = ("step_execution_id", "timer_condition_id", "timer_condition_index")
    STEP_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    TIMER_CONDITION_ID_FIELD_NUMBER: _ClassVar[int]
    TIMER_CONDITION_INDEX_FIELD_NUMBER: _ClassVar[int]
    step_execution_id: str
    timer_condition_id: str
    timer_condition_index: int
    def __init__(self, step_execution_id: _Optional[str] = ..., timer_condition_id: _Optional[str] = ..., timer_condition_index: _Optional[int] = ...) -> None: ...

class StopFlowSignalRequest(_message.Message):
    __slots__ = ("stop_type", "reason")
    STOP_TYPE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    stop_type: StopType
    reason: str
    def __init__(self, stop_type: _Optional[_Union[StopType, str]] = ..., reason: _Optional[str] = ...) -> None: ...

class GetAttributesQueryRequest(_message.Message):
    __slots__ = ("keys", "all_keys")
    KEYS_FIELD_NUMBER: _ClassVar[int]
    ALL_KEYS_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.RepeatedScalarFieldContainer[str]
    all_keys: bool
    def __init__(self, keys: _Optional[_Iterable[str]] = ..., all_keys: _Optional[bool] = ...) -> None: ...

class GetAttributesQueryResponse(_message.Message):
    __slots__ = ("attributes",)
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    attributes: _containers.RepeatedCompositeFieldContainer[KV]
    def __init__(self, attributes: _Optional[_Iterable[_Union[KV, _Mapping]]] = ...) -> None: ...

class PrepareRpcQueryRequest(_message.Message):
    __slots__ = ("load_attribute_map_instances", "load_channel_names", "load_channel_map_instances")
    LOAD_ATTRIBUTE_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    LOAD_CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    LOAD_CHANNEL_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    load_attribute_map_instances: _containers.RepeatedScalarFieldContainer[str]
    load_channel_names: _containers.RepeatedScalarFieldContainer[str]
    load_channel_map_instances: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, load_attribute_map_instances: _Optional[_Iterable[str]] = ..., load_channel_names: _Optional[_Iterable[str]] = ..., load_channel_map_instances: _Optional[_Iterable[str]] = ...) -> None: ...

class PrepareRpcQueryResponse(_message.Message):
    __slots__ = ("attributes", "run_id", "flow_started_timestamp", "flow_type", "worker_target", "channel_infos", "loaded_channel_messages", "loaded_attribute_map_instances", "loaded_channel_names", "loaded_channel_map_instances")
    class ChannelInfosEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ChannelInfo
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ChannelInfo, _Mapping]] = ...) -> None: ...
    class LoadedChannelMessagesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ChannelValues
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ChannelValues, _Mapping]] = ...) -> None: ...
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    FLOW_STARTED_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    FLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    WORKER_TARGET_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_INFOS_FIELD_NUMBER: _ClassVar[int]
    LOADED_CHANNEL_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    LOADED_ATTRIBUTE_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    LOADED_CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    LOADED_CHANNEL_MAP_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    attributes: _containers.RepeatedCompositeFieldContainer[KV]
    run_id: str
    flow_started_timestamp: int
    flow_type: str
    worker_target: WorkerTarget
    channel_infos: _containers.MessageMap[str, ChannelInfo]
    loaded_channel_messages: _containers.MessageMap[str, ChannelValues]
    loaded_attribute_map_instances: _containers.RepeatedScalarFieldContainer[str]
    loaded_channel_names: _containers.RepeatedScalarFieldContainer[str]
    loaded_channel_map_instances: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, attributes: _Optional[_Iterable[_Union[KV, _Mapping]]] = ..., run_id: _Optional[str] = ..., flow_started_timestamp: _Optional[int] = ..., flow_type: _Optional[str] = ..., worker_target: _Optional[_Union[WorkerTarget, _Mapping]] = ..., channel_infos: _Optional[_Mapping[str, ChannelInfo]] = ..., loaded_channel_messages: _Optional[_Mapping[str, ChannelValues]] = ..., loaded_attribute_map_instances: _Optional[_Iterable[str]] = ..., loaded_channel_names: _Optional[_Iterable[str]] = ..., loaded_channel_map_instances: _Optional[_Iterable[str]] = ...) -> None: ...

class TimerInfo(_message.Message):
    __slots__ = ("condition_id", "firing_unix_timestamp_seconds", "status")
    CONDITION_ID_FIELD_NUMBER: _ClassVar[int]
    FIRING_UNIX_TIMESTAMP_SECONDS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    condition_id: str
    firing_unix_timestamp_seconds: int
    status: InternalTimerStatus
    def __init__(self, condition_id: _Optional[str] = ..., firing_unix_timestamp_seconds: _Optional[int] = ..., status: _Optional[_Union[InternalTimerStatus, str]] = ...) -> None: ...

class TimerInfoList(_message.Message):
    __slots__ = ("timers",)
    TIMERS_FIELD_NUMBER: _ClassVar[int]
    timers: _containers.RepeatedCompositeFieldContainer[TimerInfo]
    def __init__(self, timers: _Optional[_Iterable[_Union[TimerInfo, _Mapping]]] = ...) -> None: ...

class GetCurrentTimerInfosQueryResponse(_message.Message):
    __slots__ = ("step_execution_current_timer_infos",)
    class StepExecutionCurrentTimerInfosEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: TimerInfoList
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[TimerInfoList, _Mapping]] = ...) -> None: ...
    STEP_EXECUTION_CURRENT_TIMER_INFOS_FIELD_NUMBER: _ClassVar[int]
    step_execution_current_timer_infos: _containers.MessageMap[str, TimerInfoList]
    def __init__(self, step_execution_current_timer_infos: _Optional[_Mapping[str, TimerInfoList]] = ...) -> None: ...

class GetScheduledGreedyTimerTimesQueryResponse(_message.Message):
    __slots__ = ("pending_scheduled",)
    PENDING_SCHEDULED_FIELD_NUMBER: _ClassVar[int]
    pending_scheduled: _containers.RepeatedCompositeFieldContainer[TimerInfo]
    def __init__(self, pending_scheduled: _Optional[_Iterable[_Union[TimerInfo, _Mapping]]] = ...) -> None: ...

class DebugDumpResponse(_message.Message):
    __slots__ = ("config", "snapshot", "firing_timers_unix_timestamps", "active_step_executions")
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    FIRING_TIMERS_UNIX_TIMESTAMPS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_STEP_EXECUTIONS_FIELD_NUMBER: _ClassVar[int]
    config: FlowConfig
    snapshot: ContinueAsNewDump
    firing_timers_unix_timestamps: _containers.RepeatedScalarFieldContainer[int]
    active_step_executions: _containers.RepeatedCompositeFieldContainer[ActiveStepExecutionState]
    def __init__(self, config: _Optional[_Union[FlowConfig, _Mapping]] = ..., snapshot: _Optional[_Union[ContinueAsNewDump, _Mapping]] = ..., firing_timers_unix_timestamps: _Optional[_Iterable[int]] = ..., active_step_executions: _Optional[_Iterable[_Union[ActiveStepExecutionState, _Mapping]]] = ...) -> None: ...

class InvokeRpcUpdateResult(_message.Message):
    __slots__ = ("response",)
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    response: InvokeRPCResponse
    def __init__(self, response: _Optional[_Union[InvokeRPCResponse, _Mapping]] = ...) -> None: ...

class StepExecutionNumbers(_message.Message):
    __slots__ = ("numbers",)
    NUMBERS_FIELD_NUMBER: _ClassVar[int]
    numbers: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, numbers: _Optional[_Iterable[int]] = ...) -> None: ...
