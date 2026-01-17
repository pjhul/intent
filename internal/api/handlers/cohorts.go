package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pjhul/intent/internal/domain/cohort"
)

// CohortHandler handles cohort-related HTTP requests
type CohortHandler struct {
	service *cohort.Service
}

// NewCohortHandler creates a new cohort handler
func NewCohortHandler(service *cohort.Service) *CohortHandler {
	return &CohortHandler{service: service}
}

// List returns all cohorts with pagination
// GET /cohorts
func (h *CohortHandler) List(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))

	if limit > 100 {
		limit = 100
	}

	cohorts, err := h.service.List(c.Request.Context(), limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"cohorts": cohorts,
		"limit":   limit,
		"offset":  offset,
	})
}

// Get retrieves a specific cohort by ID
// GET /cohorts/:id
func (h *CohortHandler) Get(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cohort ID"})
		return
	}

	coh, err := h.service.GetByID(c.Request.Context(), id)
	if err != nil {
		if err == cohort.ErrCohortNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "cohort not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, coh)
}

// Create creates a new cohort
// POST /cohorts
func (h *CohortHandler) Create(c *gin.Context) {
	var req cohort.CreateCohortRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	coh, err := h.service.Create(c.Request.Context(), req)
	if err != nil {
		if err == cohort.ErrInvalidRules {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cohort rules"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, coh)
}

// Update updates an existing cohort
// PUT /cohorts/:id
func (h *CohortHandler) Update(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cohort ID"})
		return
	}

	var req cohort.UpdateCohortRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	coh, err := h.service.Update(c.Request.Context(), id, req)
	if err != nil {
		if err == cohort.ErrCohortNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "cohort not found"})
			return
		}
		if err == cohort.ErrInvalidRules {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cohort rules"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, coh)
}

// Delete deletes a cohort
// DELETE /cohorts/:id
func (h *CohortHandler) Delete(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cohort ID"})
		return
	}

	if err := h.service.Delete(c.Request.Context(), id); err != nil {
		if err == cohort.ErrCohortNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "cohort not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
}

// Activate activates a cohort
// POST /cohorts/:id/activate
func (h *CohortHandler) Activate(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cohort ID"})
		return
	}

	coh, err := h.service.Activate(c.Request.Context(), id)
	if err != nil {
		if err == cohort.ErrCohortNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "cohort not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, coh)
}

// Deactivate deactivates a cohort
// POST /cohorts/:id/deactivate
func (h *CohortHandler) Deactivate(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cohort ID"})
		return
	}

	coh, err := h.service.Deactivate(c.Request.Context(), id)
	if err != nil {
		if err == cohort.ErrCohortNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "cohort not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, coh)
}
