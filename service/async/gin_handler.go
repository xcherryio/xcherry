package async

import (
	"github.com/gin-gonic/gin"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/config"
	"net/http"
	"strconv"
)

type ginHandler struct {
	config config.Config
	logger log.Logger
	svc    Service
}

func newGinHandler(cfg config.Config, svc Service, logger log.Logger) *ginHandler {
	return &ginHandler{
		config: cfg,
		logger: logger,
		svc:    svc,
	}
}

func (h *ginHandler) NotifyWorkerTask(c *gin.Context) {
	shardIdStr := c.Query("shardId")
	shardId, err := strconv.ParseInt(shardIdStr, 10, 32)
	if err != nil {
		c.JSON(400, map[string]string{
			"message": "invalid shardId",
		})
		return
	}
	err = h.svc.NotifyPollingWorkerTask(int32(shardId))
	if err != nil {
		c.JSON(400, map[string]string{
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, map[string]string{
		"message": "success",
	})
}
