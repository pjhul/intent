-- name: GetOrganization :one
SELECT id, name, slug, description, created_at, updated_at
FROM organizations
WHERE id = $1;

-- name: GetOrganizationBySlug :one
SELECT id, name, slug, description, created_at, updated_at
FROM organizations
WHERE slug = $1;

-- name: ListOrganizations :many
SELECT id, name, slug, description, created_at, updated_at
FROM organizations
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;

-- name: CreateOrganization :one
INSERT INTO organizations (name, slug, description)
VALUES ($1, $2, $3)
RETURNING id, name, slug, description, created_at, updated_at;

-- name: UpdateOrganization :one
UPDATE organizations
SET name = $2, slug = $3, description = $4
WHERE id = $1
RETURNING id, name, slug, description, created_at, updated_at;

-- name: DeleteOrganization :exec
DELETE FROM organizations
WHERE id = $1;

-- name: CountOrganizations :one
SELECT COUNT(*) FROM organizations;
