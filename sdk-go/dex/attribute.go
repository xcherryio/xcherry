// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package dex

import (
	"reflect"

	"github.com/superdurable/dex/sdk-go/gen/dexpb"
)

// Attribute defines a typed persisted value.
//
// Declare an Attribute once, include it in the Flow's PersistenceSchema, and reuse the same value
// from Step and RPC code. Values use the SDK's primitive and JSON encoding rules.
// String values require valid UTF-8; use []byte for arbitrary bytes.
//
// Example:
//
//	var orderStatus = dex.DefineAttribute[string](
//		"order-status",
//		dex.Indexed(dex.AttributeIndex{Type: dex.IndexKeyword}),
//	)
type Attribute[T any] struct {
	name                 string
	index                *AttributeIndex
	syncToAttributeStore bool
	isHardDeleteOnNull   bool
}

// DefineAttribute creates a typed Attribute with a stable name and definition options.
// "/" is reserved as the AttributeMap separator and is prohibited in Attribute names.
// The definition does not perform I/O; register it through PersistenceSchema before use.
func DefineAttribute[T any](key string, options ...AttributeOption) Attribute[T] {
	config := applyAttributeOptions(options)
	return Attribute[T]{
		name:                 key,
		index:                config.index,
		syncToAttributeStore: config.syncToAttributeStore,
		isHardDeleteOnNull:   config.isHardDeleteOnNull,
	}
}

// AttributeDef is the interface of Attribute, without Go's generic
// So that internal sdk can use it to workaround Go's generic limitations
//
// AttributeDef is internal to the SDK. Applications create values with DefineAttribute or
// DefineAttributeMap, then pass them to PersistenceSchema and Client methods.
type AttributeDef interface {
	attributeName() string
	attributeValueType() reflect.Type
	attributeIndex() *AttributeIndex
	attributeIsMap() bool
	attributeSyncToAttributeStore() bool
	attributeIsHardDeleteOnNull() bool
}

// Get returns the current value. Missing values return AttributeNotFoundError.
// It returns an error when ctx is not an SDK invocation or the value cannot be decoded.
func (a Attribute[T]) Get(ctx Context) (value T, err error) {
	invocation, ok := ctx.(attributeInvocation)
	if !ok {
		return value, errInvalidInvocationContext
	}
	found, err := invocation.getAttribute(a.name, &value)
	if err != nil {
		return value, err
	}
	if !found {
		return value, &AttributeNotFoundError{AttributeName: a.name}
	}
	return value, nil
}

// Set stages value for durable persistence when the current Step or RPC invocation succeeds.
// It returns an error when ctx is not an SDK invocation or value cannot be encoded or indexed.
func (a Attribute[T]) Set(ctx Context, value T) error {
	invocation, ok := ctx.(attributeInvocation)
	if !ok {
		return errInvalidInvocationContext
	}
	return invocation.setAttribute(a.name, value, a.index)
}

// Delete stages removal of this Attribute. Deleting a missing value is valid.
// It returns an error when ctx is not an SDK invocation.
func (a Attribute[T]) Delete(ctx Context) error {
	invocation, ok := ctx.(attributeInvocation)
	if !ok {
		return errInvalidInvocationContext
	}
	return invocation.deleteAttribute(a.name, a.index)
}

// AttributeName returns the stable name sent to Dex and used in PersistenceSchema.
func (a Attribute[T]) AttributeName() string {
	return a.name
}

func (a Attribute[T]) attributeName() string {
	return a.name
}

func (Attribute[T]) attributeValueType() reflect.Type {
	return reflect.TypeFor[T]()
}

func (a Attribute[T]) attributeIndex() *AttributeIndex {
	return a.index
}

func (Attribute[T]) attributeIsMap() bool {
	return false
}

func (a Attribute[T]) attributeSyncToAttributeStore() bool {
	return a.syncToAttributeStore
}

func (a Attribute[T]) attributeIsHardDeleteOnNull() bool {
	return a.isHardDeleteOnNull
}

// AttributeMap defines keyed typed persisted values.
//
// Register the map once in PersistenceSchema, then supply an instance string for every access and lock.
// Slash is prohibited in instance keys because it is a reserved character.
// String values require valid UTF-8; use []byte for arbitrary bytes.
type AttributeMap[T any] struct {
	name                 string
	index                *AttributeIndex
	syncToAttributeStore bool
	isHardDeleteOnNull   bool
}

// AttributeMapLoad selects one AttributeMap instance for a handler invocation.
// Create values with AttributeMap.Load and place them in the matching invocation options.
type AttributeMapLoad struct {
	name     string
	instance string
}

// Load selects one slash-free logical instance for a handler snapshot.
// Loading does not lock the instance or prevent concurrent changes.
func (a AttributeMap[T]) Load(instance string) AttributeMapLoad {
	return AttributeMapLoad{name: a.name, instance: instance}
}

// DefineAttributeMap creates a typed Attribute map with a stable name and definition options.
// "/" is reserved as the AttributeMap separator and is prohibited in AttributeMap names.
// The definition performs no I/O; register it through PersistenceSchema before use.
func DefineAttributeMap[T any](name string, options ...AttributeOption) AttributeMap[T] {
	config := applyAttributeOptions(options)
	return AttributeMap[T]{
		name:                 name,
		index:                config.index,
		syncToAttributeStore: config.syncToAttributeStore,
		isHardDeleteOnNull:   config.isHardDeleteOnNull,
	}
}

// Get returns the current map value. Missing instances return AttributeNotFoundError.
// It returns an error when ctx is invalid or the value cannot be decoded.
func (a AttributeMap[T]) Get(
	ctx Context,
	instance string,
) (value T, err error) {
	invocation, ok := ctx.(attributeInvocation)
	if !ok {
		return value, errInvalidInvocationContext
	}
	found, err := invocation.getAttributeMap(a.name, instance, &value)
	if err != nil {
		return value, err
	}
	if !found {
		return value, &AttributeNotFoundError{
			AttributeName: a.name,
			Instance:      instance,
		}
	}
	return value, nil
}

// Set stages value for one map instance when the invocation succeeds.
// It returns an error for an invalid context or a value that cannot be encoded or indexed.
func (a AttributeMap[T]) Set(ctx Context, instance string, value T) error {
	invocation, ok := ctx.(attributeInvocation)
	if !ok {
		return errInvalidInvocationContext
	}
	return invocation.setAttributeMap(a.name, instance, value, a.index)
}

// Delete stages removal of one map instance. Deleting a missing instance is valid.
// It returns an error when ctx is not an SDK invocation.
func (a AttributeMap[T]) Delete(ctx Context, instance string) error {
	invocation, ok := ctx.(attributeInvocation)
	if !ok {
		return errInvalidInvocationContext
	}
	return invocation.deleteAttributeMap(a.name, instance, a.index)
}

// MapSize returns the number of existing instances, including writes buffered by this invocation.
func (a AttributeMap[T]) MapSize(ctx Context) int {
	return len(a.AllInstanceKeys(ctx))
}

// AllInstanceKeys returns existing instance keys in ascending order, including buffered writes.
func (a AttributeMap[T]) AllInstanceKeys(ctx Context) []string {
	invocation, ok := ctx.(attributeInvocation)
	if !ok {
		panic(errInvalidInvocationContext)
	}
	return invocation.attributeMapKeys(a.name)
}

// AttributeName returns the stable shared name of this Attribute map.
func (a AttributeMap[T]) AttributeName() string {
	return a.name
}

func (a AttributeMap[T]) attributeName() string {
	return a.name
}

func (AttributeMap[T]) attributeValueType() reflect.Type {
	return reflect.TypeFor[T]()
}

func (a AttributeMap[T]) attributeIndex() *AttributeIndex {
	return a.index
}

func (AttributeMap[T]) attributeIsMap() bool {
	return true
}

func (a AttributeMap[T]) attributeSyncToAttributeStore() bool {
	return a.syncToAttributeStore
}

func (a AttributeMap[T]) attributeIsHardDeleteOnNull() bool {
	return a.isHardDeleteOnNull
}

// AttributeOption configures an Attribute or AttributeMap definition.
// Use Indexed, SyncToAttributeStore, and HardDeleteOnNull to create values; custom implementations are not supported.
type AttributeOption interface {
	applyAttribute(*attributeConfig)
}

// IndexType identifies the search representation required for an indexed Attribute value.
type IndexType uint8

const (
	// IndexKeyword indexes one exact UTF-8 string for equality and aggregation queries.
	IndexKeyword IndexType = iota + 1
	// IndexFullText indexes analyzed UTF-8 text for full-text queries.
	IndexFullText
	// IndexKeywordArray indexes a slice or array of exact UTF-8 strings.
	IndexKeywordArray
	// IndexInt indexes a signed or unsigned integer that fits in int64.
	IndexInt
	// IndexDouble indexes a finite floating-point value.
	IndexDouble
	// IndexBool indexes a Boolean value.
	IndexBool
	// IndexDatetime indexes time.Time or an RFC3339Nano string.
	IndexDatetime
)

// AttributeIndex configures visibility indexing; datetime values use time.Time or RFC3339Nano strings.
// An empty IndexKey uses the Attribute name
// for single values and a derived physical name for map instances.
type AttributeIndex struct {
	// Type selects the required value representation.
	Type IndexType
	// IndexKey overrides the physical search field; empty uses the Attribute-derived key.
	IndexKey string
}

// Indexed enables search indexing with index on an Attribute or AttributeMap definition.
func Indexed(index AttributeIndex) AttributeOption {
	return indexedAttributeOption{index: index}
}

// SyncToAttributeStore projects every write through the Flow's configured Attribute Store.
// Projection is asynchronous and latest-state only. Deletes write SQL NULL, and failures do not roll back Flow Attributes.
func SyncToAttributeStore() AttributeOption {
	return syncToAttributeStoreOption{}
}

// HardDeleteOnNull keeps an Attribute's values out of Flow history and permanently deletes them on Delete.
//
// Use it for short-lived secrets, such as an API key that a Flow uses a few times and then
// discards. Dex stores every value of the Attribute only in its blob store, whatever the value
// size, and Flow history keeps only a reference. Delete, or Set with a nil value, permanently
// deletes the stored value, so neither Dex APIs nor Flow history can recover it afterward.
// Step and RPC handlers receive the value inline, and the SDK never writes it to the local blob
// cache.
//
// Only string and object values are supported, including []byte. Registration rejects integer,
// unsigned integer, floating-point, and Boolean value types. An interface value type is checked
// when a value is encoded. The option cannot be combined with Indexed or SyncToAttributeStore,
// and the Attribute cannot be used by WaitForAttributeMatch, WaitForAttributeMapInstanceMatch,
// or an Action condition. The Dex Server must enable its blob store and support Server protocol
// version 3 or later. Otherwise StartFlow returns an error, and a Step or RPC that writes the
// Attribute fails.
//
// Each Flow keeps one stored value per Attribute key and overwrites it in place. This has
// user-visible consequences:
//   - A Step or RPC attempt writes or deletes the stored value before the attempt's result is
//     committed. A retried attempt, other Steps and RPCs, and Attribute reads through the Dex API
//     can therefore observe a write or deletion made by an attempt that later failed. Make
//     handlers safe to repeat instead of relying on a read-modify-write of this Attribute.
//   - When concurrent Steps or RPCs write the same key, the final value may not be the value of
//     the last committed write.
//   - A StartFlow or SubFlow start that fails because the Flow ID is already running still
//     overwrites the running Flow's value, or deletes it when the initial value is nil.
//   - Flows that reuse a Flow ID share the stored value.
//   - ResetFlow does not restore a deleted value. A deleted value reads as absent.
//
// A value remains stored until it is deleted, or until Dex removes the Flow after its retention
// period. Blob store backups or bucket versioning outside Dex can keep copies.
//
// Example:
//
//	var ModelAPIKey = dex.DefineAttribute[string]("model-api-key", dex.HardDeleteOnNull())
//
//	type CallModelStep struct {
//		dex.StepDefaultsNoWaitFor[CallModelInput]
//	}
//
//	func (CallModelStep) Execute(ctx dex.Context, input CallModelInput) (*dex.StepDecision, error) {
//		apiKey, err := ModelAPIKey.Get(ctx)
//		if err != nil {
//			return nil, err
//		}
//		if err := callModel(ctx, apiKey, input.Prompt); err != nil {
//			return nil, err
//		}
//		if err := ModelAPIKey.Delete(ctx); err != nil {
//			return nil, err
//		}
//		return dex.GracefulComplete(nil), nil
//	}
//
// Pass the secret when the Flow starts with InitialAttribute(ModelAPIKey, apiKey) in
// StartFlowOptions.Attributes, and include ModelAPIKey in the Flow's PersistenceSchema.
func HardDeleteOnNull() AttributeOption {
	return hardDeleteOnNullOption{}
}

// AttributeLock identifies one Attribute or Attribute-map instance lock.
// Create locks with LockAttribute or LockAttributeMap, then place them in StepOptions or RPCOptions.
type AttributeLock interface {
	attributeLock()
}

// LockAttribute creates a lock for the single value represented by attribute.
func LockAttribute[T any](attribute Attribute[T]) AttributeLock {
	return attributeLock{name: attribute.name}
}

// LockAttributeMap creates a lock scoped to one Attribute-map instance.
func LockAttributeMap[T any](
	attribute AttributeMap[T],
	instance string,
) AttributeLock {
	return attributeLock{name: attribute.name, instance: instance, isMap: true}
}

// InitialAttribute encodes a value for StartFlowOptions.Attributes.
// It returns ValueMappingError when the value is unsupported or incompatible with its index.
func InitialAttribute[T any](
	attribute Attribute[T],
	value T,
) (InitialAttributeDef, error) {
	encoded, indexConfig, err := encodeAttributeValue(value, attribute.index)
	if err != nil {
		return nil, err
	}
	return initialAttribute{
		name:                 attribute.name,
		value:                value,
		index:                attribute.index,
		syncToAttributeStore: attribute.syncToAttributeStore,
		isHardDeleteOnNull:   attribute.isHardDeleteOnNull,
		encoded:              encoded,
		indexConfig:          indexConfig,
	}, nil
}

// InitialAttributeDef is an encoded Attribute initialization accepted by StartFlowOptions.
// Values are created by InitialAttribute and InitialAttributeMapValue.
type InitialAttributeDef interface {
	initialAttribute()
}

// InitialAttributeMapValue encodes one Attribute-map instance for StartFlowOptions.Attributes.
// The instance identifies the map entry. Slash is prohibited in instance keys because it is a reserved character.
// It returns ValueMappingError when the value is unsupported or incompatible with its index.
func InitialAttributeMapValue[T any](
	attribute AttributeMap[T],
	instance string,
	value T,
) (InitialAttributeDef, error) {
	if err := validateMapInstance(instance); err != nil {
		return nil, err
	}
	encoded, indexConfig, err := encodeAttributeValue(value, attribute.index)
	if err != nil {
		return nil, err
	}
	return initialAttribute{
		name:                 attribute.name,
		instance:             instance,
		value:                value,
		index:                attribute.index,
		isMap:                true,
		syncToAttributeStore: attribute.syncToAttributeStore,
		isHardDeleteOnNull:   attribute.isHardDeleteOnNull,
		encoded:              encoded,
		indexConfig:          indexConfig,
	}, nil
}

type attributeInvocation interface {
	getAttribute(name string, valuePtr any) (bool, error)
	setAttribute(name string, value any, index *AttributeIndex) error
	deleteAttribute(name string, index *AttributeIndex) error
	getAttributeMap(name string, instance string, valuePtr any) (bool, error)
	setAttributeMap(name string, instance string, value any, index *AttributeIndex) error
	deleteAttributeMap(name string, instance string, index *AttributeIndex) error
	attributeMapKeys(name string) []string
}

type attributeConfig struct {
	index                *AttributeIndex
	syncToAttributeStore bool
	isHardDeleteOnNull   bool
}

func applyAttributeOptions(options []AttributeOption) attributeConfig {
	var config attributeConfig
	for _, option := range options {
		option.applyAttribute(&config)
	}
	return config
}

type indexedAttributeOption struct {
	index AttributeIndex
}

func (option indexedAttributeOption) applyAttribute(config *attributeConfig) {
	index := option.index
	config.index = &index
}

type syncToAttributeStoreOption struct{}

func (syncToAttributeStoreOption) applyAttribute(config *attributeConfig) {
	config.syncToAttributeStore = true
}

type hardDeleteOnNullOption struct{}

func (hardDeleteOnNullOption) applyAttribute(config *attributeConfig) {
	config.isHardDeleteOnNull = true
}

type attributeLock struct {
	name     string
	instance string
	isMap    bool
}

func (attributeLock) attributeLock() {}

type initialAttribute struct {
	name                 string
	instance             string
	value                any
	index                *AttributeIndex
	isMap                bool
	syncToAttributeStore bool
	isHardDeleteOnNull   bool
	encoded              *dexpb.Value
	indexConfig          *dexpb.IndexConfig
}

func (initialAttribute) initialAttribute() {}
