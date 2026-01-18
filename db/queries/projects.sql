-- name: GetProject :one
SELECT id, organization_id, name, slug, description, created_at, updated_at
FROM projects
WHERE id = $1;

-- name: GetProjectBySlug :one
SELECT id, organization_id, name, slug, description, created_at, updated_at
FROM projects
WHERE organization_id = $1 AND slug = $2;

-- name: ListProjects :many
SELECT id, organization_id, name, slug, description, created_at, updated_at
FROM projects
WHERE organization_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: ListAllProjects :many
SELECT id, organization_id, name, slug, description, created_at, updated_at
FROM projects
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;

-- name: CreateProject :one
INSERT INTO projects (organization_id, name, slug, description)
VALUES ($1, $2, $3, $4)
RETURNING id, organization_id, name, slug, description, created_at, updated_at;

-- name: UpdateProject :one
UPDATE projects
SET name = $2, slug = $3, description = $4
WHERE id = $1
RETURNING id, organization_id, name, slug, description, created_at, updated_at;

-- name: DeleteProject :exec
DELETE FROM projects
WHERE id = $1;

-- name: CountProjects :one
SELECT COUNT(*) FROM projects WHERE organization_id = $1;

-- name: CountAllProjects :one
SELECT COUNT(*) FROM projects;
