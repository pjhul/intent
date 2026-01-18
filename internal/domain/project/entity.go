package project

import (
	"time"

	"github.com/google/uuid"
)

// Project represents a project entity within an organization
type Project struct {
	ID             uuid.UUID `json:"id"`
	OrganizationID uuid.UUID `json:"organization_id"`
	Name           string    `json:"name"`
	Slug           string    `json:"slug"`
	Description    string    `json:"description,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// NewProject creates a new project with the given organization ID, name, and slug
func NewProject(organizationID uuid.UUID, name, slug, description string) *Project {
	now := time.Now().UTC()
	return &Project{
		ID:             uuid.New(),
		OrganizationID: organizationID,
		Name:           name,
		Slug:           slug,
		Description:    description,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// Update updates the project's mutable fields
func (p *Project) Update(name, slug, description string) {
	p.Name = name
	p.Slug = slug
	p.Description = description
	p.UpdatedAt = time.Now().UTC()
}

// CreateProjectRequest represents the request to create a new project
type CreateProjectRequest struct {
	Name        string `json:"name" binding:"required"`
	Slug        string `json:"slug" binding:"required"`
	Description string `json:"description"`
}

// UpdateProjectRequest represents the request to update a project
type UpdateProjectRequest struct {
	Name        string `json:"name"`
	Slug        string `json:"slug"`
	Description string `json:"description"`
}
