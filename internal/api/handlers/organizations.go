package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pjhul/intent/internal/domain/organization"
)

// OrganizationHandler handles organization-related HTTP requests
type OrganizationHandler struct {
	service *organization.Service
}

// NewOrganizationHandler creates a new organization handler
func NewOrganizationHandler(service *organization.Service) *OrganizationHandler {
	return &OrganizationHandler{service: service}
}

// List returns all organizations with pagination
// GET /organizations
func (h *OrganizationHandler) List(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))

	if limit > 100 {
		limit = 100
	}

	orgs, err := h.service.List(c.Request.Context(), limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"organizations": orgs,
		"limit":         limit,
		"offset":        offset,
	})
}

// Get retrieves a specific organization by slug
// GET /organizations/:orgSlug
func (h *OrganizationHandler) Get(c *gin.Context) {
	slug := c.Param("orgSlug")
	if slug == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "organization slug required"})
		return
	}

	org, err := h.service.GetBySlug(c.Request.Context(), slug)
	if err != nil {
		if err == organization.ErrOrganizationNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "organization not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, org)
}

// GetByID retrieves a specific organization by ID
// GET /organizations/by-id/:id
func (h *OrganizationHandler) GetByID(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid organization ID"})
		return
	}

	org, err := h.service.GetByID(c.Request.Context(), id)
	if err != nil {
		if err == organization.ErrOrganizationNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "organization not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, org)
}

// Create creates a new organization
// POST /organizations
func (h *OrganizationHandler) Create(c *gin.Context) {
	var req organization.CreateOrganizationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	org, err := h.service.Create(c.Request.Context(), req)
	if err != nil {
		if err == organization.ErrSlugAlreadyExists {
			c.JSON(http.StatusConflict, gin.H{"error": "organization slug already exists"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, org)
}

// Update updates an existing organization
// PUT /organizations/:orgSlug
func (h *OrganizationHandler) Update(c *gin.Context) {
	slug := c.Param("orgSlug")
	if slug == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "organization slug required"})
		return
	}

	// First get the org by slug to get its ID
	existingOrg, err := h.service.GetBySlug(c.Request.Context(), slug)
	if err != nil {
		if err == organization.ErrOrganizationNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "organization not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var req organization.UpdateOrganizationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	org, err := h.service.Update(c.Request.Context(), existingOrg.ID, req)
	if err != nil {
		if err == organization.ErrOrganizationNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "organization not found"})
			return
		}
		if err == organization.ErrSlugAlreadyExists {
			c.JSON(http.StatusConflict, gin.H{"error": "organization slug already exists"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, org)
}

// Delete deletes an organization
// DELETE /organizations/:orgSlug
func (h *OrganizationHandler) Delete(c *gin.Context) {
	slug := c.Param("orgSlug")
	if slug == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "organization slug required"})
		return
	}

	// First get the org by slug to get its ID
	existingOrg, err := h.service.GetBySlug(c.Request.Context(), slug)
	if err != nil {
		if err == organization.ErrOrganizationNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "organization not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if err := h.service.Delete(c.Request.Context(), existingOrg.ID); err != nil {
		if err == organization.ErrOrganizationNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "organization not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
}
