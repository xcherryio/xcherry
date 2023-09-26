package api

import (
	"github.com/gin-gonic/gin"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/config"
)

const ProcessStartApiPath = "/api/v1/process/start"

func NewService(config config.Config, logger log.Logger) *gin.Engine {
	router := gin.Default()

	handler := newHandler(config, logger)

	router.POST(ProcessStartApiPath, handler.ApiV1ProcessStartPost)

	return router
}
