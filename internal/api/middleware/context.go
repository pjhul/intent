package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pjhul/intent/internal/domain/organization"
	"github.com/pjhul/intent/internal/domain/project"
)

// Context keys for storing org and project in the request context
const (
	OrganizationKey = "organization"
	ProjectKey      = "project"
)

// ContextMiddleware resolves organization and project from URL slugs
type ContextMiddleware struct {
	orgService     *organization.Service
	projectService *project.Service
}

// NewContextMiddleware creates a new context middleware
func NewContextMiddleware(orgService *organization.Service, projectService *project.Service) *ContextMiddleware {
	return &ContextMiddleware{
		orgService:     orgService,
		projectService: projectService,
	}
}

// ResolveOrganization middleware resolves the organization from the URL and adds it to the context
func (m *ContextMiddleware) ResolveOrganization() gin.HandlerFunc {
	return func(c *gin.Context) {
		orgSlug := c.Param("orgSlug")
		if orgSlug == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "organization slug required"})
			c.Abort()
			return
		}

		org, err := m.orgService.GetBySlug(c.Request.Context(), orgSlug)
		if err != nil {
			if err == organization.ErrOrganizationNotFound {
				c.JSON(http.StatusNotFound, gin.H{"error": "organization not found"})
				c.Abort()
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			c.Abort()
			return
		}

		c.Set(OrganizationKey, org)
		c.Next()
	}
}

// ResolveProject middleware resolves the project from the URL and adds it to the context
// Requires organization to be resolved first
func (m *ContextMiddleware) ResolveProject() gin.HandlerFunc {
	return func(c *gin.Context) {
		projectSlug := c.Param("projectSlug")
		if projectSlug == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "project slug required"})
			c.Abort()
			return
		}

		// Get organization from context
		orgVal, exists := c.Get(OrganizationKey)
		if !exists {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "organization not resolved"})
			c.Abort()
			return
		}
		org := orgVal.(*organization.Organization)

		proj, err := m.projectService.GetBySlug(c.Request.Context(), org.ID, projectSlug)
		if err != nil {
			if err == project.ErrProjectNotFound {
				c.JSON(http.StatusNotFound, gin.H{"error": "project not found"})
				c.Abort()
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			c.Abort()
			return
		}

		c.Set(ProjectKey, proj)
		c.Next()
	}
}

// GetOrganization retrieves the organization from the gin context
func GetOrganization(c *gin.Context) (*organization.Organization, bool) {
	val, exists := c.Get(OrganizationKey)
	if !exists {
		return nil, false
	}
	org, ok := val.(*organization.Organization)
	return org, ok
}

// GetProject retrieves the project from the gin context
func GetProject(c *gin.Context) (*project.Project, bool) {
	val, exists := c.Get(ProjectKey)
	if !exists {
		return nil, false
	}
	proj, ok := val.(*project.Project)
	return proj, ok
}

// GetProjectID retrieves the project ID from the gin context
func GetProjectID(c *gin.Context) (uuid.UUID, bool) {
	proj, ok := GetProject(c)
	if !ok {
		return uuid.Nil, false
	}
	return proj.ID, true
}

// GetOrganizationID retrieves the organization ID from the gin context
func GetOrganizationID(c *gin.Context) (uuid.UUID, bool) {
	org, ok := GetOrganization(c)
	if !ok {
		return uuid.Nil, false
	}
	return org.ID, true
}
