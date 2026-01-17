package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pjhul/intent/internal/infrastructure/flink"
)

// FlinkHandler handles Flink job management HTTP requests
type FlinkHandler struct {
	jobManager *flink.JobManager
}

// NewFlinkHandler creates a new Flink handler
func NewFlinkHandler(jobManager *flink.JobManager) *FlinkHandler {
	return &FlinkHandler{jobManager: jobManager}
}

// GetClusterOverview returns Flink cluster status
// GET /flink/overview
func (h *FlinkHandler) GetClusterOverview(c *gin.Context) {
	overview, err := h.jobManager.GetClusterOverview(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "failed to connect to Flink: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, overview)
}

// ListJobs returns all Flink jobs
// GET /flink/jobs
func (h *FlinkHandler) ListJobs(c *gin.Context) {
	jobs, err := h.jobManager.ListJobs(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "failed to list jobs: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"jobs": jobs})
}

// GetJobDetails returns details of a specific job
// GET /flink/jobs/:id
func (h *FlinkHandler) GetJobDetails(c *gin.Context) {
	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job ID is required"})
		return
	}

	details, err := h.jobManager.GetJobDetails(c.Request.Context(), jobID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, details)
}

// CancelJob cancels a running job
// DELETE /flink/jobs/:id
func (h *FlinkHandler) CancelJob(c *gin.Context) {
	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job ID is required"})
		return
	}

	if err := h.jobManager.CancelJob(c.Request.Context(), jobID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to cancel job: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "cancelling"})
}

// TriggerSavepoint triggers a savepoint for a job
// POST /flink/jobs/:id/savepoints
func (h *FlinkHandler) TriggerSavepoint(c *gin.Context) {
	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job ID is required"})
		return
	}

	var req struct {
		TargetDirectory string `json:"target_directory" binding:"required"`
		CancelJob       bool   `json:"cancel_job"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	requestID, err := h.jobManager.TriggerSavepoint(c.Request.Context(), jobID, req.TargetDirectory, req.CancelJob)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to trigger savepoint: " + err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"request_id": requestID})
}

// GetSavepointStatus gets the status of a savepoint operation
// GET /flink/jobs/:id/savepoints/:requestId
func (h *FlinkHandler) GetSavepointStatus(c *gin.Context) {
	jobID := c.Param("id")
	requestID := c.Param("requestId")

	if jobID == "" || requestID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job ID and request ID are required"})
		return
	}

	status, err := h.jobManager.GetSavepointStatus(c.Request.Context(), jobID, requestID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get savepoint status: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, status)
}

// ListJars lists all uploaded JARs
// GET /flink/jars
func (h *FlinkHandler) ListJars(c *gin.Context) {
	jars, err := h.jobManager.ListJars(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "failed to list JARs: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"jars": jars})
}

// SubmitJob submits a new job from a JAR
// POST /flink/jars/:id/run
func (h *FlinkHandler) SubmitJob(c *gin.Context) {
	jarID := c.Param("id")
	if jarID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "JAR ID is required"})
		return
	}

	var req struct {
		EntryClass    string   `json:"entry_class" binding:"required"`
		ProgramArgs   []string `json:"program_args"`
		Parallelism   int      `json:"parallelism"`
		SavepointPath string   `json:"savepoint_path"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Parallelism <= 0 {
		req.Parallelism = 1
	}

	jobID, err := h.jobManager.SubmitJob(c.Request.Context(), jarID, req.EntryClass, req.ProgramArgs, req.Parallelism, req.SavepointPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to submit job: " + err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"job_id": jobID})
}
