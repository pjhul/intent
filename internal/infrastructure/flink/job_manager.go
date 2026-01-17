package flink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pjhul/intent/internal/config"
)

// JobManager handles communication with Flink REST API
type JobManager struct {
	baseURL    string
	httpClient *http.Client
}

// NewJobManager creates a new Flink job manager client
func NewJobManager(cfg config.FlinkConfig) *JobManager {
	return &JobManager{
		baseURL: cfg.URL(),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// JobStatus represents a Flink job status
type JobStatus string

const (
	JobStatusCreated     JobStatus = "CREATED"
	JobStatusRunning     JobStatus = "RUNNING"
	JobStatusFailing     JobStatus = "FAILING"
	JobStatusFailed      JobStatus = "FAILED"
	JobStatusCancelling  JobStatus = "CANCELLING"
	JobStatusCanceled    JobStatus = "CANCELED"
	JobStatusFinished    JobStatus = "FINISHED"
	JobStatusRestarting  JobStatus = "RESTARTING"
	JobStatusSuspended   JobStatus = "SUSPENDED"
	JobStatusReconciling JobStatus = "RECONCILING"
)

// Job represents a Flink job
type Job struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	State     JobStatus `json:"state"`
	StartTime int64     `json:"start-time"`
	EndTime   int64     `json:"end-time"`
	Duration  int64     `json:"duration"`
}

// JobsResponse represents the response from /jobs endpoint
type JobsResponse struct {
	Jobs []Job `json:"jobs"`
}

// JobDetailsResponse represents detailed job information
type JobDetailsResponse struct {
	JID        string    `json:"jid"`
	Name       string    `json:"name"`
	State      JobStatus `json:"state"`
	StartTime  int64     `json:"start-time"`
	EndTime    int64     `json:"end-time"`
	Duration   int64     `json:"duration"`
	Vertices   []Vertex  `json:"vertices"`
}

// Vertex represents a job vertex (operator)
type Vertex struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Parallelism int    `json:"parallelism"`
	Status      string `json:"status"`
	StartTime   int64  `json:"start-time"`
	EndTime     int64  `json:"end-time"`
	Duration    int64  `json:"duration"`
}

// SavepointResponse represents a savepoint trigger response
type SavepointResponse struct {
	RequestID string `json:"request-id"`
}

// SavepointStatusResponse represents the status of a savepoint operation
type SavepointStatusResponse struct {
	Status struct {
		ID string `json:"id"`
	} `json:"status"`
	Operation struct {
		Location      string `json:"location,omitempty"`
		FailureCause  string `json:"failure-cause,omitempty"`
	} `json:"operation,omitempty"`
}

// ClusterOverview represents Flink cluster status
type ClusterOverview struct {
	TaskManagers      int `json:"taskmanagers"`
	SlotsTotal        int `json:"slots-total"`
	SlotsAvailable    int `json:"slots-available"`
	JobsRunning       int `json:"jobs-running"`
	JobsFinished      int `json:"jobs-finished"`
	JobsCancelled     int `json:"jobs-cancelled"`
	JobsFailed        int `json:"jobs-failed"`
	FlinkVersion      string `json:"flink-version"`
	FlinkCommit       string `json:"flink-commit"`
}

// GetClusterOverview retrieves cluster status
func (m *JobManager) GetClusterOverview(ctx context.Context) (*ClusterOverview, error) {
	resp, err := m.doRequest(ctx, "GET", "/overview", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var overview ClusterOverview
	if err := json.NewDecoder(resp.Body).Decode(&overview); err != nil {
		return nil, err
	}

	return &overview, nil
}

// ListJobs returns all jobs
func (m *JobManager) ListJobs(ctx context.Context) ([]Job, error) {
	resp, err := m.doRequest(ctx, "GET", "/jobs", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var jobsResp JobsResponse
	if err := json.NewDecoder(resp.Body).Decode(&jobsResp); err != nil {
		return nil, err
	}

	return jobsResp.Jobs, nil
}

// GetJobDetails returns detailed information about a job
func (m *JobManager) GetJobDetails(ctx context.Context, jobID string) (*JobDetailsResponse, error) {
	resp, err := m.doRequest(ctx, "GET", fmt.Sprintf("/jobs/%s", jobID), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var details JobDetailsResponse
	if err := json.NewDecoder(resp.Body).Decode(&details); err != nil {
		return nil, err
	}

	return &details, nil
}

// CancelJob cancels a running job
func (m *JobManager) CancelJob(ctx context.Context, jobID string) error {
	resp, err := m.doRequest(ctx, "PATCH", fmt.Sprintf("/jobs/%s", jobID), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to cancel job: %s", string(body))
	}

	return nil
}

// TriggerSavepoint triggers a savepoint for a job
func (m *JobManager) TriggerSavepoint(ctx context.Context, jobID string, savepointDir string, cancelJob bool) (string, error) {
	payload := map[string]interface{}{
		"target-directory": savepointDir,
		"cancel-job":       cancelJob,
	}

	resp, err := m.doRequest(ctx, "POST", fmt.Sprintf("/jobs/%s/savepoints", jobID), payload)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var savepointResp SavepointResponse
	if err := json.NewDecoder(resp.Body).Decode(&savepointResp); err != nil {
		return "", err
	}

	return savepointResp.RequestID, nil
}

// GetSavepointStatus gets the status of a savepoint operation
func (m *JobManager) GetSavepointStatus(ctx context.Context, jobID, requestID string) (*SavepointStatusResponse, error) {
	resp, err := m.doRequest(ctx, "GET", fmt.Sprintf("/jobs/%s/savepoints/%s", jobID, requestID), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var status SavepointStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}

	return &status, nil
}

// SubmitJob submits a new job from a JAR
func (m *JobManager) SubmitJob(ctx context.Context, jarID string, entryClass string, programArgs []string, parallelism int, savepointPath string) (string, error) {
	payload := map[string]interface{}{
		"entryClass":  entryClass,
		"parallelism": parallelism,
	}

	if len(programArgs) > 0 {
		payload["programArgs"] = programArgs
	}

	if savepointPath != "" {
		payload["savepointPath"] = savepointPath
		payload["allowNonRestoredState"] = true
	}

	resp, err := m.doRequest(ctx, "POST", fmt.Sprintf("/jars/%s/run", jarID), payload)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		JobID string `json:"jobid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	return result.JobID, nil
}

// ListJars lists all uploaded JARs
func (m *JobManager) ListJars(ctx context.Context) ([]JarInfo, error) {
	resp, err := m.doRequest(ctx, "GET", "/jars", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Files []JarInfo `json:"files"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Files, nil
}

// JarInfo represents information about an uploaded JAR
type JarInfo struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Uploaded int64  `json:"uploaded"`
}

func (m *JobManager) doRequest(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, m.baseURL+path, reqBody)
	if err != nil {
		return nil, err
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return resp, nil
}
