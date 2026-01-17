package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pjhul/intent/internal/domain/membership"
)

// MembershipHandler handles membership-related HTTP requests
type MembershipHandler struct {
	service *membership.Service
}

// NewMembershipHandler creates a new membership handler
func NewMembershipHandler(service *membership.Service) *MembershipHandler {
	return &MembershipHandler{service: service}
}

// CheckMembership checks if a user is a member of a cohort
// POST /cohorts/:id/check
func (h *MembershipHandler) CheckMembership(c *gin.Context) {
	cohortID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cohort ID"})
		return
	}

	var req struct {
		UserID string `json:"user_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.service.CheckMembership(c.Request.Context(), cohortID, req.UserID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// GetCohortMembers returns members of a cohort
// GET /cohorts/:id/members
func (h *MembershipHandler) GetCohortMembers(c *gin.Context) {
	cohortID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cohort ID"})
		return
	}

	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))

	if limit > 1000 {
		limit = 1000
	}

	resp, err := h.service.GetCohortMembers(c.Request.Context(), cohortID, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// GetCohortStats returns statistics for a cohort
// GET /cohorts/:id/stats
func (h *MembershipHandler) GetCohortStats(c *gin.Context) {
	cohortID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cohort ID"})
		return
	}

	stats, err := h.service.GetCohortStats(c.Request.Context(), cohortID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, stats)
}

// GetUserCohorts returns all cohorts a user belongs to
// GET /users/:id/cohorts
func (h *MembershipHandler) GetUserCohorts(c *gin.Context) {
	userID := c.Param("id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user ID is required"})
		return
	}

	resp, err := h.service.GetUserCohorts(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}
