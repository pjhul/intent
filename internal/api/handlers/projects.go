package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pjhul/intent/internal/domain/organization"
	"github.com/pjhul/intent/internal/domain/project"
)

// ProjectHandler handles project-related HTTP requests
type ProjectHandler struct {
	service     *project.Service
	orgService  *organization.Service
}

// NewProjectHandler creates a new project handler
func NewProjectHandler(service *project.Service, orgService *organization.Service) *ProjectHandler {
	return &ProjectHandler{
		service:    service,
		orgService: orgService,
	}
}

// List returns all projects for an organization with pagination
// GET /organizations/:orgSlug/projects
func (h *ProjectHandler) List(c *gin.Context) {
	orgSlug := c.Param("orgSlug")
	if orgSlug == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "organization slug required"})
		return
	}

	org, err := h.orgService.GetBySlug(c.Request.Context(), orgSlug)
	if err != nil {
		if err == organization.ErrOrganizationNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "organization not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))

	if limit > 100 {
		limit = 100
	}

	projects, err := h.service.List(c.Request.Context(), org.ID, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"projects": projects,
		"limit":    limit,
		"offset":   offset,
	})
}

// Get retrieves a specific project by slug
// GET /organizations/:orgSlug/projects/:projectSlug
func (h *ProjectHandler) Get(c *gin.Context) {
	orgSlug := c.Param("orgSlug")
	projectSlug := c.Param("projectSlug")

	if orgSlug == "" || projectSlug == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "organization and project slugs required"})
		return
	}

	org, err := h.orgService.GetBySlug(c.Request.Context(), orgSlug)
	if err != nil {
		if err == organization.ErrOrganizationNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "organization not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	proj, err := h.service.GetBySlug(c.Request.Context(), org.ID, projectSlug)
	if err != nil {
		if err == project.ErrProjectNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "project not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, proj)
}

// GetByID retrieves a specific project by ID
// GET /projects/by-id/:id
func (h *ProjectHandler) GetByID(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid project ID"})
		return
	}

	proj, err := h.service.GetByID(c.Request.Context(), id)
	if err != nil {
		if err == project.ErrProjectNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "project not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, proj)
}

// Create creates a new project
// POST /organizations/:orgSlug/projects
func (h *ProjectHandler) Create(c *gin.Context) {
	orgSlug := c.Param("orgSlug")
	if orgSlug == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "organization slug required"})
		return
	}

	org, err := h.orgService.GetBySlug(c.Request.Context(), orgSlug)
	if err != nil {
		if err == organization.ErrOrganizationNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "organization not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var req project.CreateProjectRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	proj, err := h.service.Create(c.Request.Context(), org.ID, req)
	if err != nil {
		if err == project.ErrSlugAlreadyExists {
			c.JSON(http.StatusConflict, gin.H{"error": "project slug already exists in this organization"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, proj)
}

// Update updates an existing project
// PUT /organizations/:orgSlug/projects/:projectSlug
func (h *ProjectHandler) Update(c *gin.Context) {
	orgSlug := c.Param("orgSlug")
	projectSlug := c.Param("projectSlug")

	if orgSlug == "" || projectSlug == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "organization and project slugs required"})
		return
	}

	org, err := h.orgService.GetBySlug(c.Request.Context(), orgSlug)
	if err != nil {
		if err == organization.ErrOrganizationNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "organization not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	existingProj, err := h.service.GetBySlug(c.Request.Context(), org.ID, projectSlug)
	if err != nil {
		if err == project.ErrProjectNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "project not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var req project.UpdateProjectRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	proj, err := h.service.Update(c.Request.Context(), existingProj.ID, req)
	if err != nil {
		if err == project.ErrProjectNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "project not found"})
			return
		}
		if err == project.ErrSlugAlreadyExists {
			c.JSON(http.StatusConflict, gin.H{"error": "project slug already exists in this organization"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, proj)
}

// Delete deletes a project
// DELETE /organizations/:orgSlug/projects/:projectSlug
func (h *ProjectHandler) Delete(c *gin.Context) {
	orgSlug := c.Param("orgSlug")
	projectSlug := c.Param("projectSlug")

	if orgSlug == "" || projectSlug == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "organization and project slugs required"})
		return
	}

	org, err := h.orgService.GetBySlug(c.Request.Context(), orgSlug)
	if err != nil {
		if err == organization.ErrOrganizationNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "organization not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	existingProj, err := h.service.GetBySlug(c.Request.Context(), org.ID, projectSlug)
	if err != nil {
		if err == project.ErrProjectNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "project not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if err := h.service.Delete(c.Request.Context(), existingProj.ID); err != nil {
		if err == project.ErrProjectNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "project not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
}
