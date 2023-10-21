// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tag

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

const LoggingCallAtKey = "logging-call-at"

// Tag is the interface for logging system
type Tag struct {
	// keep this field private
	field zap.Field
}

// Field returns a zap field
func (t *Tag) Field() zap.Field {
	return t.field
}

func newStringTag(key string, value string) Tag {
	return Tag{
		field: zap.String(key, value),
	}
}

func newInt64(key string, value int64) Tag {
	return Tag{
		field: zap.Int64(key, value),
	}
}

func newInt(key string, value int) Tag {
	return Tag{
		field: zap.Int(key, value),
	}
}

func newBoolTag(key string, value bool) Tag {
	return Tag{
		field: zap.Bool(key, value),
	}
}

func newTimeTag(key string, value time.Time) Tag {
	return Tag{
		field: zap.Time(key, value),
	}
}

func newObjectTag(key string, value interface{}) Tag {
	return Tag{
		field: zap.String(key, fmt.Sprintf("%v", value)),
	}
}

func newErrorTag(key string, value error) Tag {
	//NOTE zap already chosen "error" as key
	return Tag{
		field: zap.Error(value),
	}
}

// TAGS

func Error(err error) Tag {
	return newErrorTag("error", err)
}

func Service(sv string) Tag {
	return newStringTag("service", sv)
}

func Message(msg string) Tag {
	return newStringTag("message", msg)
}

func ProcessId(id string) Tag {
	return newStringTag("processId", id)
}

func ProcessType(pt string) Tag {
	return newStringTag("processType", pt)
}

func Namespace(ns string) Tag {
	return newStringTag("namespace", ns)
}

func ProcessExecutionId(id string) Tag {
	return newStringTag("processExecutionId", id)
}

func StateExecutionId(id string) Tag {
	return newStringTag("stateExecutionId", id)
}

func Shard(shardId int32) Tag {
	return newInt64("shard", int64(shardId))
}

func StatusCode(status int) Tag {
	return newInt("status", int(status))
}

func AnyToStr(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

func Value(v interface{}) Tag {
	return newObjectTag("value", v)
}

func UnixTimestamp(v int64) Tag {
	return newTimeTag("UnixTimestamp", time.Unix(v, 0))
}

func ID(v string) Tag {
	return newStringTag("ID", v)
}

func Key(v string) Tag {
	return newStringTag("Key", v)
}

func DefaultValue(v interface{}) Tag {
	return newObjectTag("default-value", v)
}

func ImmediateTaskType(v string) Tag {
	return newStringTag("ImmediateTaskType", v)
}
