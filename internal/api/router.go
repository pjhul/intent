package api

import (
	"github.com/gin-gonic/gin"
	"github.com/pjhul/intent/internal/api/handlers"
)

// Router holds all the HTTP handlers
type Router struct {
	cohortHandler     *handlers.CohortHandler
	eventHandler      *handlers.EventHandler
	membershipHandler *handlers.MembershipHandler
	wsHandler         *handlers.WebSocketHandler
	sseHandler        *handlers.SSEHandler
	flinkHandler      *handlers.FlinkHandler
}

// NewRouter creates a new router with all handlers
func NewRouter(
	cohortHandler *handlers.CohortHandler,
	eventHandler *handlers.EventHandler,
	membershipHandler *handlers.MembershipHandler,
	wsHandler *handlers.WebSocketHandler,
	sseHandler *handlers.SSEHandler,
	flinkHandler *handlers.FlinkHandler,
) *Router {
	return &Router{
		cohortHandler:     cohortHandler,
		eventHandler:      eventHandler,
		membershipHandler: membershipHandler,
		wsHandler:         wsHandler,
		sseHandler:        sseHandler,
		flinkHandler:      flinkHandler,
	}
}

// SetupRoutes configures all API routes
func (r *Router) SetupRoutes(engine *gin.Engine) {
	// Health check
	engine.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// API v1 routes
	v1 := engine.Group("/api/v1")
	{
		// Event endpoints
		events := v1.Group("/events")
		{
			events.POST("", r.eventHandler.Ingest)
			events.POST("/batch", r.eventHandler.IngestBatch)
		}

		// Cohort endpoints
		cohorts := v1.Group("/cohorts")
		{
			cohorts.GET("", r.cohortHandler.List)
			cohorts.POST("", r.cohortHandler.Create)
			cohorts.GET("/:id", r.cohortHandler.Get)
			cohorts.PUT("/:id", r.cohortHandler.Update)
			cohorts.DELETE("/:id", r.cohortHandler.Delete)
			cohorts.POST("/:id/activate", r.cohortHandler.Activate)
			cohorts.POST("/:id/deactivate", r.cohortHandler.Deactivate)
			cohorts.POST("/:id/check", r.membershipHandler.CheckMembership)
			cohorts.GET("/:id/members", r.membershipHandler.GetCohortMembers)
			cohorts.GET("/:id/stats", r.membershipHandler.GetCohortStats)
		}

		// User endpoints
		users := v1.Group("/users")
		{
			users.GET("/:id/cohorts", r.membershipHandler.GetUserCohorts)
		}

		// Flink management endpoints
		flink := v1.Group("/flink")
		{
			flink.GET("/overview", r.flinkHandler.GetClusterOverview)
			flink.GET("/jobs", r.flinkHandler.ListJobs)
			flink.GET("/jobs/:id", r.flinkHandler.GetJobDetails)
			flink.DELETE("/jobs/:id", r.flinkHandler.CancelJob)
			flink.POST("/jobs/:id/savepoints", r.flinkHandler.TriggerSavepoint)
			flink.GET("/jobs/:id/savepoints/:requestId", r.flinkHandler.GetSavepointStatus)
			flink.GET("/jars", r.flinkHandler.ListJars)
			flink.POST("/jars/:id/run", r.flinkHandler.SubmitJob)
		}

		// Real-time streaming endpoints
		stream := v1.Group("/stream")
		{
			stream.GET("/cohort-changes", r.sseHandler.HandleSSE)
		}
	}

	// WebSocket endpoint (outside /api/v1 for cleaner URL)
	engine.GET("/ws/cohort-changes", r.wsHandler.HandleWebSocket)
}
