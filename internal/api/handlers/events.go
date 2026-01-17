package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pjhul/intent/internal/domain/event"
)

// EventHandler handles event-related HTTP requests
type EventHandler struct {
	service *event.Service
}

// NewEventHandler creates a new event handler
func NewEventHandler(service *event.Service) *EventHandler {
	return &EventHandler{service: service}
}

// Ingest ingests a single event
// POST /events
func (h *EventHandler) Ingest(c *gin.Context) {
	var req event.IngestEventRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.service.Ingest(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, resp)
}

// IngestBatch ingests multiple events
// POST /events/batch
func (h *EventHandler) IngestBatch(c *gin.Context) {
	var req event.IngestBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(req.Events) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "events array cannot be empty"})
		return
	}

	if len(req.Events) > 1000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "batch size cannot exceed 1000 events"})
		return
	}

	resp, err := h.service.IngestBatch(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, resp)
}
