package api

import (
	"github.com/gin-gonic/gin"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/persistence"
)

const PathStartProcessExecution = "/api/v1/xdb/service/process-execution/start"
const PathDescribeProcessExecution = "/api/v1/xdb/service/process-execution/describe"

func NewAPIServiceGinController(cfg config.Config, processOrm persistence.ProcessORM, logger log.Logger) *gin.Engine {
	router := gin.Default()

	handler := newHandler(cfg, processOrm, logger)

	router.POST(PathStartProcessExecution, handler.StartProcess)
	router.POST(PathDescribeProcessExecution, handler.DescribeProcess)

	return router
}
