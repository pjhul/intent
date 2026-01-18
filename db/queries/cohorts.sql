-- name: GetCohort :one
SELECT id, project_id, name, description, rules, status, version, created_at, updated_at
FROM cohorts
WHERE id = $1;

-- name: GetCohortByName :one
SELECT id, project_id, name, description, rules, status, version, created_at, updated_at
FROM cohorts
WHERE project_id = $1 AND name = $2;

-- name: ListCohorts :many
SELECT id, project_id, name, description, rules, status, version, created_at, updated_at
FROM cohorts
WHERE project_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: ListCohortsByStatus :many
SELECT id, project_id, name, description, rules, status, version, created_at, updated_at
FROM cohorts
WHERE project_id = $1 AND status = $2
ORDER BY created_at DESC
LIMIT $3 OFFSET $4;

-- name: ListActiveCohorts :many
SELECT id, project_id, name, description, rules, status, version, created_at, updated_at
FROM cohorts
WHERE project_id = $1 AND status = 'active'
ORDER BY created_at DESC;

-- name: ListAllActiveCohorts :many
SELECT id, project_id, name, description, rules, status, version, created_at, updated_at
FROM cohorts
WHERE status = 'active'
ORDER BY created_at DESC;

-- name: CreateCohort :one
INSERT INTO cohorts (project_id, name, description, rules, status, version)
VALUES ($1, $2, $3, $4, $5, 1)
RETURNING id, project_id, name, description, rules, status, version, created_at, updated_at;

-- name: UpdateCohort :one
UPDATE cohorts
SET name = $2, description = $3, rules = $4, version = version + 1
WHERE id = $1
RETURNING id, project_id, name, description, rules, status, version, created_at, updated_at;

-- name: UpdateCohortStatus :one
UPDATE cohorts
SET status = $2
WHERE id = $1
RETURNING id, project_id, name, description, rules, status, version, created_at, updated_at;

-- name: DeleteCohort :exec
DELETE FROM cohorts
WHERE id = $1;

-- name: CountCohorts :one
SELECT COUNT(*) FROM cohorts WHERE project_id = $1;

-- name: CountCohortsByStatus :one
SELECT COUNT(*) FROM cohorts WHERE project_id = $1 AND status = $2;

-- name: GetCohortsUpdatedAfter :many
SELECT id, project_id, name, description, rules, status, version, created_at, updated_at
FROM cohorts
WHERE updated_at > $1
ORDER BY updated_at ASC;
