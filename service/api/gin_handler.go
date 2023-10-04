package api

import (
	"encoding/json"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/engine"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
)

type ginHandler struct {
	config config.Config
	logger log.Logger
	svc    Service
}

func newGinHandler(cfg config.Config, apiEngine engine.APIEngine, logger log.Logger) *ginHandler {
	svc := NewServiceImpl(cfg, apiEngine, logger)
	return &ginHandler{
		config: cfg,
		logger: logger,
		svc:    svc,
	}
}

func (h *ginHandler) StartProcess(c *gin.Context) {
	var req xdbapi.ProcessExecutionStartRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}
	h.logger.Debug("received StartProcess API request", tag.Value(h.toJson(req)))

	resp, errResp := h.svc.StartProcess(c.Request.Context(), req)

	if errResp != nil {
		c.JSON(errResp.StatusCode, errResp.Error)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (h *ginHandler) DescribeProcess(c *gin.Context) {
	var req xdbapi.ProcessExecutionDescribeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}
	h.logger.Debug("received DescribeProcess API request", tag.Value(h.toJson(req)))

	resp, errResp := h.svc.DescribeLatestProcess(c.Request.Context(), req)

	if errResp != nil {
		c.JSON(errResp.StatusCode, errResp.Error)
		return
	}

	c.JSON(http.StatusOK, resp)
	return
}

func (h *ginHandler) toJson(req any) string {
	str, err := json.Marshal(req)
	if err != nil {
		h.logger.Error("error when serializing request", tag.Error(err), tag.DefaultValue(req))
		return ""
	}
	return string(str)
}

func invalidRequestSchema(c *gin.Context) {
	c.JSON(http.StatusBadRequest, xdbapi.ApiErrorResponse{
		Detail: xdbapi.PtrString("invalid request schema"),
	})
}
