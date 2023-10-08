// Apache License 2.0

// Copyright (c) XDBLab organization

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.    

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

func ID(v string) Tag {
	return newStringTag("ID", v)
}

func Key(v string) Tag {
	return newStringTag("Key", v)
}

func DefaultValue(v interface{}) Tag {
	return newObjectTag("default-value", v)
}
