package tag

import (
	"fmt"
	"go.uber.org/zap"
	"time"
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

func Value(v interface{}) Tag {
	return newObjectTag("value", v)
}

func DefaultValue(v interface{}) Tag {
	return newObjectTag("default-value", v)
}
