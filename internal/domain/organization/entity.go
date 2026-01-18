package organization

import (
	"time"

	"github.com/google/uuid"
)

// Organization represents an organization entity
type Organization struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	Slug        string    `json:"slug"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// NewOrganization creates a new organization with the given name and slug
func NewOrganization(name, slug, description string) *Organization {
	now := time.Now().UTC()
	return &Organization{
		ID:          uuid.New(),
		Name:        name,
		Slug:        slug,
		Description: description,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// Update updates the organization's mutable fields
func (o *Organization) Update(name, slug, description string) {
	o.Name = name
	o.Slug = slug
	o.Description = description
	o.UpdatedAt = time.Now().UTC()
}

// CreateOrganizationRequest represents the request to create a new organization
type CreateOrganizationRequest struct {
	Name        string `json:"name" binding:"required"`
	Slug        string `json:"slug" binding:"required"`
	Description string `json:"description"`
}

// UpdateOrganizationRequest represents the request to update an organization
type UpdateOrganizationRequest struct {
	Name        string `json:"name"`
	Slug        string `json:"slug"`
	Description string `json:"description"`
}
