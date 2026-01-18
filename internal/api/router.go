package api

import (
	"github.com/gin-gonic/gin"
	"github.com/pjhul/intent/internal/api/handlers"
	"github.com/pjhul/intent/internal/api/middleware"
)

// Router holds all the HTTP handlers
type Router struct {
	cohortHandler       *handlers.CohortHandler
	eventHandler        *handlers.EventHandler
	membershipHandler   *handlers.MembershipHandler
	wsHandler           *handlers.WebSocketHandler
	sseHandler          *handlers.SSEHandler
	flinkHandler        *handlers.FlinkHandler
	organizationHandler *handlers.OrganizationHandler
	projectHandler      *handlers.ProjectHandler
	contextMiddleware   *middleware.ContextMiddleware
}

// NewRouter creates a new router with all handlers
func NewRouter(
	cohortHandler *handlers.CohortHandler,
	eventHandler *handlers.EventHandler,
	membershipHandler *handlers.MembershipHandler,
	wsHandler *handlers.WebSocketHandler,
	sseHandler *handlers.SSEHandler,
	flinkHandler *handlers.FlinkHandler,
	organizationHandler *handlers.OrganizationHandler,
	projectHandler *handlers.ProjectHandler,
	contextMiddleware *middleware.ContextMiddleware,
) *Router {
	return &Router{
		cohortHandler:       cohortHandler,
		eventHandler:        eventHandler,
		membershipHandler:   membershipHandler,
		wsHandler:           wsHandler,
		sseHandler:          sseHandler,
		flinkHandler:        flinkHandler,
		organizationHandler: organizationHandler,
		projectHandler:      projectHandler,
		contextMiddleware:   contextMiddleware,
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
		// Organization endpoints
		orgs := v1.Group("/organizations")
		{
			orgs.GET("", r.organizationHandler.List)
			orgs.POST("", r.organizationHandler.Create)
			orgs.GET("/:orgSlug", r.organizationHandler.Get)
			orgs.PUT("/:orgSlug", r.organizationHandler.Update)
			orgs.DELETE("/:orgSlug", r.organizationHandler.Delete)

			// Project endpoints under organization
			projects := orgs.Group("/:orgSlug/projects", r.contextMiddleware.ResolveOrganization())
			{
				projects.GET("", r.projectHandler.List)
				projects.POST("", r.projectHandler.Create)
				projects.GET("/:projectSlug", r.projectHandler.Get)
				projects.PUT("/:projectSlug", r.projectHandler.Update)
				projects.DELETE("/:projectSlug", r.projectHandler.Delete)

				// Project-scoped resources (require both org and project context)
				projectScoped := projects.Group("/:projectSlug", r.contextMiddleware.ResolveProject())
				{
					// Cohort endpoints
					cohorts := projectScoped.Group("/cohorts")
					{
						cohorts.GET("", r.cohortHandler.List)
						cohorts.POST("", r.cohortHandler.Create)
						cohorts.GET("/:id", r.cohortHandler.Get)
						cohorts.PUT("/:id", r.cohortHandler.Update)
						cohorts.DELETE("/:id", r.cohortHandler.Delete)
						cohorts.POST("/:id/activate", r.cohortHandler.Activate)
						cohorts.POST("/:id/deactivate", r.cohortHandler.Deactivate)
						cohorts.POST("/:id/recompute", r.cohortHandler.Recompute)
						cohorts.GET("/:id/recompute/:jobId", r.cohortHandler.GetRecomputeStatus)
						cohorts.POST("/:id/check", r.membershipHandler.CheckMembership)
						cohorts.GET("/:id/members", r.membershipHandler.GetCohortMembers)
						cohorts.GET("/:id/stats", r.membershipHandler.GetCohortStats)
					}

					// Event endpoints under project
					events := projectScoped.Group("/events")
					{
						events.POST("", r.eventHandler.Ingest)
						events.POST("/batch", r.eventHandler.IngestBatch)
					}

					// User endpoints under project
					users := projectScoped.Group("/users")
					{
						users.GET("/:id/cohorts", r.membershipHandler.GetUserCohorts)
					}

					// Real-time streaming endpoints under project
					stream := projectScoped.Group("/stream")
					{
						stream.GET("/cohort-changes", r.sseHandler.HandleSSE)
					}
				}
			}
		}

		// Flink management endpoints (global, not project-scoped)
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
	}

	// WebSocket endpoint (outside /api/v1 for cleaner URL)
	engine.GET("/ws/cohort-changes", r.wsHandler.HandleWebSocket)
}
