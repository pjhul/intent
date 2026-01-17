-- name: GetCohort :one
SELECT id, name, description, rules, status, version, created_at, updated_at
FROM cohorts
WHERE id = $1;

-- name: GetCohortByName :one
SELECT id, name, description, rules, status, version, created_at, updated_at
FROM cohorts
WHERE name = $1;

-- name: ListCohorts :many
SELECT id, name, description, rules, status, version, created_at, updated_at
FROM cohorts
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;

-- name: ListCohortsByStatus :many
SELECT id, name, description, rules, status, version, created_at, updated_at
FROM cohorts
WHERE status = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: ListActiveCohorts :many
SELECT id, name, description, rules, status, version, created_at, updated_at
FROM cohorts
WHERE status = 'active'
ORDER BY created_at DESC;

-- name: CreateCohort :one
INSERT INTO cohorts (name, description, rules, status, version)
VALUES ($1, $2, $3, $4, 1)
RETURNING id, name, description, rules, status, version, created_at, updated_at;

-- name: UpdateCohort :one
UPDATE cohorts
SET name = $2, description = $3, rules = $4, version = version + 1
WHERE id = $1
RETURNING id, name, description, rules, status, version, created_at, updated_at;

-- name: UpdateCohortStatus :one
UPDATE cohorts
SET status = $2
WHERE id = $1
RETURNING id, name, description, rules, status, version, created_at, updated_at;

-- name: DeleteCohort :exec
DELETE FROM cohorts
WHERE id = $1;

-- name: CountCohorts :one
SELECT COUNT(*) FROM cohorts;

-- name: CountCohortsByStatus :one
SELECT COUNT(*) FROM cohorts WHERE status = $1;

-- name: GetCohortsUpdatedAfter :many
SELECT id, name, description, rules, status, version, created_at, updated_at
FROM cohorts
WHERE updated_at > $1
ORDER BY updated_at ASC;
