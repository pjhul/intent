package project

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pjhul/intent/internal/db"
)

var (
	ErrProjectNotFound   = errors.New("project not found")
	ErrSlugAlreadyExists = errors.New("project slug already exists in this organization")
)

// Service handles project business logic
type Service struct {
	queries db.Querier
}

// NewService creates a new project service
func NewService(queries db.Querier) *Service {
	return &Service{
		queries: queries,
	}
}

// Create creates a new project within an organization
func (s *Service) Create(ctx context.Context, organizationID uuid.UUID, req CreateProjectRequest) (*Project, error) {
	pgOrgID := pgtype.UUID{Bytes: organizationID, Valid: true}
	dbProject, err := s.queries.CreateProject(ctx, db.CreateProjectParams{
		OrganizationID: pgOrgID,
		Name:           req.Name,
		Slug:           req.Slug,
		Description:    pgtype.Text{String: req.Description, Valid: req.Description != ""},
	})
	if err != nil {
		return nil, err
	}

	return dbProjectToDomain(dbProject), nil
}

// GetByID retrieves a project by ID
func (s *Service) GetByID(ctx context.Context, id uuid.UUID) (*Project, error) {
	pgID := pgtype.UUID{Bytes: id, Valid: true}
	dbProject, err := s.queries.GetProject(ctx, pgID)
	if err != nil {
		return nil, ErrProjectNotFound
	}

	return dbProjectToDomain(dbProject), nil
}

// GetBySlug retrieves a project by organization ID and slug
func (s *Service) GetBySlug(ctx context.Context, organizationID uuid.UUID, slug string) (*Project, error) {
	pgOrgID := pgtype.UUID{Bytes: organizationID, Valid: true}
	dbProject, err := s.queries.GetProjectBySlug(ctx, db.GetProjectBySlugParams{
		OrganizationID: pgOrgID,
		Slug:           slug,
	})
	if err != nil {
		return nil, ErrProjectNotFound
	}

	return dbProjectToDomain(dbProject), nil
}

// List retrieves projects for an organization with pagination
func (s *Service) List(ctx context.Context, organizationID uuid.UUID, limit, offset int) ([]*Project, error) {
	pgOrgID := pgtype.UUID{Bytes: organizationID, Valid: true}
	dbProjects, err := s.queries.ListProjects(ctx, db.ListProjectsParams{
		OrganizationID: pgOrgID,
		Limit:          int32(limit),
		Offset:         int32(offset),
	})
	if err != nil {
		return nil, err
	}

	projects := make([]*Project, len(dbProjects))
	for i, p := range dbProjects {
		projects[i] = dbProjectToDomain(p)
	}

	return projects, nil
}

// Update updates a project
func (s *Service) Update(ctx context.Context, id uuid.UUID, req UpdateProjectRequest) (*Project, error) {
	existing, err := s.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	name := existing.Name
	if req.Name != "" {
		name = req.Name
	}

	slug := existing.Slug
	if req.Slug != "" {
		slug = req.Slug
	}

	description := existing.Description
	if req.Description != "" {
		description = req.Description
	}

	pgID := pgtype.UUID{Bytes: id, Valid: true}
	dbProject, err := s.queries.UpdateProject(ctx, db.UpdateProjectParams{
		ID:          pgID,
		Name:        name,
		Slug:        slug,
		Description: pgtype.Text{String: description, Valid: description != ""},
	})
	if err != nil {
		return nil, err
	}

	return dbProjectToDomain(dbProject), nil
}

// Delete deletes a project
func (s *Service) Delete(ctx context.Context, id uuid.UUID) error {
	pgID := pgtype.UUID{Bytes: id, Valid: true}
	if err := s.queries.DeleteProject(ctx, pgID); err != nil {
		return ErrProjectNotFound
	}

	return nil
}

// Count returns the total number of projects in an organization
func (s *Service) Count(ctx context.Context, organizationID uuid.UUID) (int64, error) {
	pgOrgID := pgtype.UUID{Bytes: organizationID, Valid: true}
	return s.queries.CountProjects(ctx, pgOrgID)
}

func dbProjectToDomain(p db.Project) *Project {
	return &Project{
		ID:             uuid.UUID(p.ID.Bytes),
		OrganizationID: uuid.UUID(p.OrganizationID.Bytes),
		Name:           p.Name,
		Slug:           p.Slug,
		Description:    p.Description.String,
		CreatedAt:      p.CreatedAt.Time,
		UpdatedAt:      p.UpdatedAt.Time,
	}
}
