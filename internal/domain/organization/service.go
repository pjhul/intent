package organization

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pjhul/intent/internal/db"
)

var (
	ErrOrganizationNotFound = errors.New("organization not found")
	ErrSlugAlreadyExists    = errors.New("organization slug already exists")
)

// Service handles organization business logic
type Service struct {
	queries db.Querier
}

// NewService creates a new organization service
func NewService(queries db.Querier) *Service {
	return &Service{
		queries: queries,
	}
}

// Create creates a new organization
func (s *Service) Create(ctx context.Context, req CreateOrganizationRequest) (*Organization, error) {
	dbOrg, err := s.queries.CreateOrganization(ctx, db.CreateOrganizationParams{
		Name:        req.Name,
		Slug:        req.Slug,
		Description: pgtype.Text{String: req.Description, Valid: req.Description != ""},
	})
	if err != nil {
		return nil, err
	}

	return dbOrganizationToDomain(dbOrg), nil
}

// GetByID retrieves an organization by ID
func (s *Service) GetByID(ctx context.Context, id uuid.UUID) (*Organization, error) {
	pgID := pgtype.UUID{Bytes: id, Valid: true}
	dbOrg, err := s.queries.GetOrganization(ctx, pgID)
	if err != nil {
		return nil, ErrOrganizationNotFound
	}

	return dbOrganizationToDomain(dbOrg), nil
}

// GetBySlug retrieves an organization by slug
func (s *Service) GetBySlug(ctx context.Context, slug string) (*Organization, error) {
	dbOrg, err := s.queries.GetOrganizationBySlug(ctx, slug)
	if err != nil {
		return nil, ErrOrganizationNotFound
	}

	return dbOrganizationToDomain(dbOrg), nil
}

// List retrieves organizations with pagination
func (s *Service) List(ctx context.Context, limit, offset int) ([]*Organization, error) {
	dbOrgs, err := s.queries.ListOrganizations(ctx, db.ListOrganizationsParams{
		Limit:  int32(limit),
		Offset: int32(offset),
	})
	if err != nil {
		return nil, err
	}

	orgs := make([]*Organization, len(dbOrgs))
	for i, o := range dbOrgs {
		orgs[i] = dbOrganizationToDomain(o)
	}

	return orgs, nil
}

// Update updates an organization
func (s *Service) Update(ctx context.Context, id uuid.UUID, req UpdateOrganizationRequest) (*Organization, error) {
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
	dbOrg, err := s.queries.UpdateOrganization(ctx, db.UpdateOrganizationParams{
		ID:          pgID,
		Name:        name,
		Slug:        slug,
		Description: pgtype.Text{String: description, Valid: description != ""},
	})
	if err != nil {
		return nil, err
	}

	return dbOrganizationToDomain(dbOrg), nil
}

// Delete deletes an organization
func (s *Service) Delete(ctx context.Context, id uuid.UUID) error {
	pgID := pgtype.UUID{Bytes: id, Valid: true}
	if err := s.queries.DeleteOrganization(ctx, pgID); err != nil {
		return ErrOrganizationNotFound
	}

	return nil
}

// Count returns the total number of organizations
func (s *Service) Count(ctx context.Context) (int64, error) {
	return s.queries.CountOrganizations(ctx)
}

func dbOrganizationToDomain(o db.Organization) *Organization {
	return &Organization{
		ID:          uuid.UUID(o.ID.Bytes),
		Name:        o.Name,
		Slug:        o.Slug,
		Description: o.Description.String,
		CreatedAt:   o.CreatedAt.Time,
		UpdatedAt:   o.UpdatedAt.Time,
	}
}
