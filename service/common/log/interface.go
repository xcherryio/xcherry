package log

import "github.com/xdblab/xdb/service/common/log/tag"

// Logger is our abstraction for logging
// Usage examples:
//
//	 import "github.com/uber/cadence/common/log/tag"
//	 1) logger = logger.WithTags(
//	         tag.WorkflowNextEventID( 123),
//	         tag.WorkflowActionWorkflowStarted,
//	         tag.WorkflowDomainID("test-domain-id"))
//	    logger.Info("hello world")
//	 2) logger.Info("hello world",
//	         tag.WorkflowNextEventID( 123),
//	         tag.WorkflowActionWorkflowStarted,
//	         tag.WorkflowDomainID("test-domain-id"))
//		   )
//	 Note: msg should be static, it is not recommended to use fmt.Sprintf() for msg.
//	       Anything dynamic should be tagged.
type Logger interface {
	Debug(msg string, tags ...tag.Tag)
	Info(msg string, tags ...tag.Tag)
	Warn(msg string, tags ...tag.Tag)
	Error(msg string, tags ...tag.Tag)
	Fatal(msg string, tags ...tag.Tag)
	WithTags(tags ...tag.Tag) Logger
}
