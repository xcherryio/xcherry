# Legacy Materials in this file remain under their original licenses.
# See LEGACY_NOTICES.md.

# Modifications Copyright (c) 2026 Super Durable, Inc.
#
# Modifications after the Legacy Cutoff are licensed under the
# Sustainable Use License 1.0.
# Legacy Materials remain under their original licenses.
# See LICENSE and LEGACY_NOTICES.md.

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, TypeVar, cast
from urllib.parse import quote

from dex._utils import require_map_instance, require_persistence_definition_name
from dex.context import Context
from dex.dexpb import dex_pb2 as pb
from dex.runtime_errors import ValueMappingError

ValueT = TypeVar("ValueT")


@dataclass(frozen=True)
class AttributeMapLoad:
    """Load one AttributeMap instance for an RPC snapshot.

    Create a load with :meth:`AttributeMap.load`. Loading provides a point-in-time
    value snapshot; it does not lock the map against concurrent writers.

    Attributes:
        attribute_map: The exact AttributeMap definition registered with the Flow.
        instance: One logical instance key.
    """

    attribute_map: AttributeMap[Any]
    instance: str

    @property
    def physical_name(self) -> str:
        """Return the physical instance name for this typed load.

        Returns:
            The encoded physical instance name.
        """
        instance = require_map_instance(self.instance)
        return f"{self.attribute_map.name}/{quote(instance, safe='')}"


class IndexType(Enum):
    """Select how Dex indexes an Attribute for Flow search.

    Attributes:
        KEYWORD: Index an exact string value.
        FULL_TEXT: Index a string for tokenized full-text matching.
        KEYWORD_ARRAY: Index every string in a sequence as a keyword.
        INT: Index a signed 64-bit integer.
        DOUBLE: Index a finite floating-point number.
        BOOL: Index a Boolean value.
        DATETIME: Index a :class:`datetime.datetime` value.
    """

    KEYWORD = "keyword"
    FULL_TEXT = "full_text"
    KEYWORD_ARRAY = "keyword_array"
    INT = "int"
    DOUBLE = "double"
    BOOL = "bool"
    DATETIME = "datetime"


@dataclass(frozen=True)
class AttributeIndex:
    """Configure the search index for an Attribute or AttributeMap.

    Attributes:
        type: The value representation accepted by the search index.
        index_key: The physical search key. An empty value uses the Attribute name
            for a singleton Attribute; AttributeMap definitions require an explicit
            key so all instances share one index.
    """

    type: IndexType
    index_key: str = ""


@dataclass(frozen=True)
class Attribute(Generic[ValueT]):
    """Define a typed, durable singleton value owned by a Flow.

    Declare Attributes once in a Flow's ``PersistenceSchema`` and reuse the same
    definition from Steps, RPCs, and Client calls. Values are read and written
    through a handler :class:`Context`; Client methods provide external access.

    Set ``sync_to_attribute_store`` to project every write asynchronously through
    the Flow's configured Attribute Store. Projection is latest-state only, deletion
    projects SQL NULL, and failures do not roll back the Flow Attribute.

    Set ``hard_delete_on_null=True`` for short-lived secrets, such as API keys,
    that must be removable. Dex stores every value of a hard-delete Attribute only
    in the Dex blob store and never in Flow history. Deleting the Attribute, or
    setting it to ``None``, permanently deletes the stored value. A deleted value
    reads as absent, so ``get`` returns ``None``.

    A hard-delete Attribute accepts only string and object values, including
    ``bytes`` and dataclasses. ``int``, ``bool``, and ``float`` value types are
    rejected when the Attribute is defined. A custom codec that produces a
    numeric or Boolean value is rejected when its Flow is registered, and any
    numeric or Boolean value is rejected when it is written. The option cannot
    be combined with ``index`` or ``sync_to_attribute_store``.
    ``Client.wait_for_attribute_match`` and ``AsyncClient.wait_for_attribute_match``
    reject a hard-delete Attribute before contacting Dex. ``start_flow`` and
    SubFlow starts reject an initial value whose definition sets
    ``hard_delete_on_null`` differently from the target Flow's registered
    definition of that name, so a secret is never sent inline by mistake.

    Hard-delete writes require a Dex Server with an enabled blob store and Server
    protocol version 3 or later. A ``Worker`` or ``AsyncWorker`` still starts
    against an older Server, even when its Registry contains hard-delete
    Attributes. Against such a Server, a Step, RPC, or timeout handler result
    that writes or deletes a hard-delete Attribute fails that handler attempt
    with ``RuntimeError`` instead of being sent. The same applies to a ``wait_for``
    result whose SubFlow start initializes one. A Client checks the Server
    protocol once, before its first ``start_flow`` that initializes a hard-delete
    Attribute, and raises instead of sending the value to an older Server. A
    Server without an enabled blob store rejects the write.
    Step and RPC handlers receive the current value inline. The SDK never stores
    the value in the local blob cache.

    Hard deletion has these limitations:

    * Each Flow keeps one stored copy per Attribute key. Every write overwrites
      that copy in place.
    * A Step or RPC attempt writes or deletes the stored value before its result
      is committed. A retried attempt, other Steps, and RPC handlers can
      therefore observe a write or deletion from an attempt that later failed.
    * When concurrent Steps or RPCs write the same key, the final stored value
      may not be the last committed write.
    * A failed ``start_flow`` or SubFlow start for a Flow ID that is already
      running still overwrites the running Flow's value. A ``None`` initial value
      deletes it.
    * Executions that reuse a Flow ID share the stored value.
    * ``Client.time_travel`` does not restore a deleted value. The value stays
      absent.
    * A Flow retry or a restarted SubFlow starts from the original initial
      value. If an earlier run deleted that value, the new run reads it as
      absent.
    * Removing ``hard_delete_on_null`` from a definition leaves the stored value
      in place until Dex removes the Flow. Delete the value with the
      hard-delete definition first.
    * Values remain until deleted, or until the Flow is removed by retention and
      blob cleanup.
    * Blob store backups or bucket versioning can retain copies outside Dex.

    Attributes:
        name: The non-empty logical Attribute name without ``/``, unique within its Flow.
        value_type: The Python type used to encode and decode values.
        index: Optional search-index configuration; ``None`` disables indexing.
        sync_to_attribute_store: Whether writes are projected to the selected
            Attribute Store. Defaults to ``False``.
        hard_delete_on_null: Whether values are stored only in the Dex blob store
            and permanently deleted when the Attribute is deleted or set to
            ``None``. Defaults to ``False``. Requires a string or object
            ``value_type`` and no ``index`` or ``sync_to_attribute_store``.

    Raises:
        ValueError: If ``name`` is invalid, or if ``hard_delete_on_null`` is
            combined with ``index`` or ``sync_to_attribute_store``.
        TypeError: If ``hard_delete_on_null`` is set for an ``int``, ``bool``,
            or ``float`` value type.

    Examples:
        >>> status = Attribute("status", str, sync_to_attribute_store=True)
        >>> status.set(context, "paid")
        >>> status.get(context)
        'paid'

        A Step uses a short-lived API key and then permanently deletes it:

        >>> class CallProviderStep(Step[None]):
        ...     def __init__(self, api_key: Attribute[str]) -> None:
        ...         self.api_key = api_key
        ...
        ...     def execute(self, context: Context, input: None) -> StepDecision:
        ...         call_provider(self.api_key.get(context))
        ...         self.api_key.delete(context)
        ...         return graceful_complete("called")
        >>> class ProviderFlow(Flow[None]):
        ...     def __init__(self) -> None:
        ...         self.api_key = Attribute("api-key", str, hard_delete_on_null=True)
        ...         self.call = CallProviderStep(self.api_key)
        ...
        ...     def get_steps(self) -> StepList[None]:
        ...         return StepList.start_step(self.call)
        ...
        ...     def get_persistence_schema(self) -> PersistenceSchema:
        ...         return PersistenceSchema.of(self.api_key)
        >>> flow = ProviderFlow()
        >>> with Client(Registry([flow]), cache) as client:
        ...     client.start_flow(
        ...         flow,
        ...         "provider-42",
        ...         None,
        ...         StartFlowOptions().with_attribute(flow.api_key, "sk-live-123"),
        ...     )
    """

    name: str
    value_type: type[ValueT]
    index: AttributeIndex | None = None
    sync_to_attribute_store: bool = False
    hard_delete_on_null: bool = False

    def __post_init__(self) -> None:
        require_persistence_definition_name(self.name)
        _validate_hard_delete_on_null(self)

    def get(self, context: Context) -> ValueT:
        """Return the current value from a Step or RPC Context.

        Args:
            context: The current handler Context.

        Returns:
            The decoded value.

        Raises:
            KeyError: If the Attribute has no value.
            TypeError: If the stored value cannot be decoded as ``value_type``.
        """
        return context._get_attribute(self, None)

    def set(self, context: Context, value: ValueT) -> None:
        """Stage a value to persist with the current handler decision.

        Args:
            context: The current handler Context.
            value: A value compatible with ``value_type``.

        Raises:
            TypeError: If ``value`` cannot be encoded as ``value_type``.
        """
        context._set_attribute(self, None, value)

    def delete(self, context: Context) -> None:
        """Stage deletion of this Attribute in the current handler decision.

        Args:
            context: The current handler Context.
        """
        context._delete_attribute(cast(Attribute[object], self), None)

    def lock(self) -> AttributeLock:
        """Return a lock request for this Attribute.

        Use the result in Step or RPC lock options to serialize handlers that
        access the same value.

        Returns:
            A lock for this Attribute.
        """
        return AttributeLock(self)


@dataclass(frozen=True)
class AttributeMap(Generic[ValueT]):
    """Define a typed family of durable values keyed by map instance.

    AttributeMap instances share one schema definition while keeping independent
    values and locks. Declare the map in ``PersistenceSchema`` before using it.
    Instance keys must be non-empty and must not contain ``/``.
    Synced instances use their physical Attribute names as target columns. Projection
    is asynchronous and latest-state only, deletion projects SQL NULL, and failures
    do not roll back Flow Attributes.

    Set ``hard_delete_on_null=True`` for short-lived secrets keyed by instance,
    such as one API key per provider. Every instance follows the hard-delete rules
    documented on :class:`Attribute`. Values are stored only in the Dex blob store
    and never in Flow history. Deleting an instance, or setting it to ``None``,
    permanently deletes its stored value. Only string and object value types,
    including ``bytes``, are supported. The option cannot be combined with
    ``index`` or ``sync_to_attribute_store``, and ``wait_for_attribute_match``
    rejects it. Writes require a Server with an enabled blob store and Server
    protocol version 3 or later. Handlers receive values inline, and the SDK
    never stores them in the local blob cache. Each Flow keeps one stored copy
    per instance key. The retry, concurrency, failed-start, Flow ID reuse, time
    travel, retention, and backup limitations on :class:`Attribute` apply to
    each instance.

    Attributes:
        name: The non-empty logical Attribute name without ``/``, unique within its Flow.
        value_type: The Python type used for every map instance.
        index: Optional shared search-index configuration.
        sync_to_attribute_store: Whether every instance is projected to the selected
            Attribute Store. Defaults to ``False``.
        hard_delete_on_null: Whether every instance value is stored only in the
            Dex blob store and permanently deleted when the instance is deleted or
            set to ``None``. Defaults to ``False``. Requires a string or object
            ``value_type`` and no ``index`` or ``sync_to_attribute_store``.

    Raises:
        ValueError: If ``name`` is invalid, or if ``hard_delete_on_null`` is
            combined with ``index`` or ``sync_to_attribute_store``.
        TypeError: If ``hard_delete_on_null`` is set for an ``int``, ``bool``,
            or ``float`` value type.

    Examples:
        >>> balances = AttributeMap("balance", int, sync_to_attribute_store=True)
        >>> balances.set(context, "merchant-7", 1200)
        >>> balances.get(context, "merchant-7")
        1200

        RPCs rotate and revoke one provider's API key:

        >>> class WaitForRevocationStep(Step[None]):
        ...     def __init__(self, revoked: Channel[None]) -> None:
        ...         self.revoked = revoked
        ...
        ...     def wait_for(self, context: Context, input: None) -> Wait:
        ...         return Wait.until(self.revoked.for_one())
        ...
        ...     def execute(self, context: Context, input: None) -> StepDecision:
        ...         return graceful_complete("revoked")
        >>> class CredentialFlow(Flow[None]):
        ...     def __init__(self) -> None:
        ...         self.api_keys = AttributeMap(
        ...             "provider-api-key", str, hard_delete_on_null=True
        ...         )
        ...         self.revoked = Channel("revoked", type(None))
        ...         self.wait = WaitForRevocationStep(self.revoked)
        ...
        ...     def get_steps(self) -> StepList[None]:
        ...         return StepList.start_step(self.wait)
        ...
        ...     def get_persistence_schema(self) -> PersistenceSchema:
        ...         return PersistenceSchema.of(self.api_keys, self.revoked)
        ...
        ...     @rpc
        ...     def rotate_api_key(self, context: Context, new_key: str) -> None:
        ...         self.api_keys.set(context, "payments", new_key)
        ...
        ...     @rpc
        ...     def revoke_api_key(self, context: Context) -> None:
        ...         self.api_keys.delete(context, "payments")
        ...         self.revoked.publish(context, None)
        >>> flow = CredentialFlow()
        >>> with Client(Registry([flow]), cache) as client:
        ...     client.start_flow(flow, "credentials-7", None)
        ...     client.invoke_rpc(flow.rotate_api_key, "credentials-7", "sk-live-456")
        ...     client.invoke_rpc(flow.revoke_api_key, "credentials-7")
    """

    name: str
    value_type: type[ValueT]
    index: AttributeIndex | None = None
    sync_to_attribute_store: bool = False
    hard_delete_on_null: bool = False

    def __post_init__(self) -> None:
        require_persistence_definition_name(self.name)
        _validate_hard_delete_on_null(self)

    def load(self, instance: str) -> AttributeMapLoad:
        """Select one instance for an RPC snapshot.

        Pass the result through ``@rpc(load_attribute_map_instances=(...))``.

        Args:
            instance: The non-empty, slash-free logical map key to load.

        Returns:
            A load for this one instance.

        Raises:
            ValueError: If ``instance`` is empty or contains ``/``.
        """
        return AttributeMapLoad(self, require_map_instance(instance))

    def get(self, context: Context, instance: str) -> ValueT:
        """Return one map instance from a Step or RPC Context.

        Args:
            context: The current handler Context.
            instance: The map instance. Slash is prohibited because it is a reserved character.

        Returns:
            The decoded instance value.

        Raises:
            KeyError: If the instance has no value.
            ValueError: If ``instance`` is empty or contains ``/``.
        """
        return context._get_attribute(self, instance)

    def set(self, context: Context, instance: str, value: ValueT) -> None:
        """Stage one map-instance value for the current handler decision.

        Args:
            context: The current handler Context.
            instance: The map instance. Slash is prohibited because it is a reserved character.
            value: A value compatible with ``value_type``.
        """
        context._set_attribute(self, instance, value)

    def delete(self, context: Context, instance: str) -> None:
        """Stage deletion of one map instance.

        Args:
            context: The current handler Context.
            instance: The map instance. Slash is prohibited because it is a reserved character.
        """
        context._delete_attribute(cast(AttributeMap[object], self), instance)

    def get_map_size(self, context: Context) -> int:
        """Return the number of existing instances, including buffered writes.

        Args:
            context: The current Step or RPC Context.

        Returns:
            The number of keys visible after decision-local writes and deletions.
        """
        return len(self.get_all_instance_keys(context))

    def get_all_instance_keys(self, context: Context) -> tuple[str, ...]:
        """Return decoded existing instance keys in ascending order.

        Args:
            context: The current Step or RPC Context.

        Returns:
            An immutable tuple reflecting decision-local writes and deletions.
        """
        return context._attribute_map_keys(cast(AttributeMap[object], self))

    def lock(self, instance: str) -> AttributeLock:
        """Return a lock request for one map instance.

        Args:
            instance: The map instance. Slash is prohibited because it is a reserved character.

        Returns:
            A lock for ``instance``.

        Raises:
            ValueError: If ``instance`` is empty or contains ``/``.
        """
        require_map_instance(instance)
        return AttributeLock(self, instance)


@dataclass(frozen=True)
class AttributeLock:
    """Describe an Attribute lock acquired for a Step or RPC handler.

    Attributes:
        attribute: The singleton Attribute or AttributeMap definition to lock.
        instance: The AttributeMap instance, or ``None`` for a singleton Attribute.
    """

    attribute: Attribute[Any] | AttributeMap[Any]
    instance: str | None = None

    def __post_init__(self) -> None:
        if self.instance is not None:
            require_map_instance(self.instance)


def _apply_attribute_store_sync(
    write: pb.AttributeWrite,
    definition: Attribute[Any] | AttributeMap[Any],
) -> None:
    if definition.sync_to_attribute_store:
        write.sync_config.enabled = True


_HARD_DELETE_REJECTED_VALUE_TYPES: tuple[type[Any], ...] = (bool, int, float)
_HARD_DELETE_REJECTED_VALUE_KINDS = frozenset(
    ("bool_value", "int_value", "double_value")
)


def _validate_hard_delete_on_null(
    definition: Attribute[Any] | AttributeMap[Any],
) -> None:
    if not definition.hard_delete_on_null:
        return
    if definition.index is not None:
        raise ValueError(f"hard-delete Attribute {definition.name} cannot use an index")
    if definition.sync_to_attribute_store:
        raise ValueError(
            f"hard-delete Attribute {definition.name} cannot use "
            "sync_to_attribute_store"
        )
    if any(
        definition.value_type is rejected
        for rejected in _HARD_DELETE_REJECTED_VALUE_TYPES
    ):
        raise TypeError(
            f"hard-delete Attribute {definition.name} supports only string and "
            f"object values, not {definition.value_type.__name__}"
        )


def _apply_hard_delete_on_null(
    write: pb.AttributeWrite,
    definition: Attribute[Any] | AttributeMap[Any],
) -> None:
    if not definition.hard_delete_on_null:
        return
    value_kind = write.value.WhichOneof("kind")
    if value_kind in _HARD_DELETE_REJECTED_VALUE_KINDS:
        raise ValueMappingError(
            "encode",
            f"hard-delete Attribute {definition.name} supports only string and "
            f"object values, not {value_kind}",
        )
    write.is_hard_delete_on_null = True


def _require_matching_hard_delete_on_null(
    flow_name: str,
    registered: object,
    definition: Attribute[Any] | AttributeMap[Any],
) -> None:
    if not isinstance(registered, (Attribute, AttributeMap)):
        return
    if registered.hard_delete_on_null == definition.hard_delete_on_null:
        return
    raise ValueError(
        f"initial Attribute {definition.name} uses hard_delete_on_null="
        f"{definition.hard_delete_on_null}, but Flow {flow_name} registers it with "
        f"hard_delete_on_null={registered.hard_delete_on_null}"
    )


def _require_attribute_wait_supported(
    definition: Attribute[Any] | AttributeMap[Any],
) -> None:
    if definition.hard_delete_on_null:
        raise ValueError(
            f"wait_for_attribute_match does not support hard-delete Attribute "
            f"{definition.name}"
        )
