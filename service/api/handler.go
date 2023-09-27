package api

import (
	"encoding/json"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/persistence"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
)

type handler struct {
	config config.Config
	logger log.Logger
	svc    Service
}

func newHandler(cfg config.Config, processOrm persistence.ProcessORM, logger log.Logger) *handler {
	svc := NewServiceImpl(cfg, processOrm, logger)
	return &handler{
		config: cfg,
		logger: logger,
		svc:    svc,
	}
}

func (h *handler) StartProcess(c *gin.Context) {
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
	return
}

func (h *handler) DescribeProcess(c *gin.Context) {
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

func (h *handler) toJson(req any) string {
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
