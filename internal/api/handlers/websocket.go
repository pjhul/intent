package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pjhul/intent/internal/domain/membership"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for development
	},
}

// Broadcaster interface for receiving membership changes
type Broadcaster interface {
	Subscribe(id string, sub *membership.StreamSubscription) chan *membership.MembershipChange
	Unsubscribe(id string)
}

// WebSocketHandler handles WebSocket connections for real-time updates
type WebSocketHandler struct {
	broadcaster Broadcaster
}

// NewWebSocketHandler creates a new WebSocket handler
func NewWebSocketHandler(broadcaster Broadcaster) *WebSocketHandler {
	return &WebSocketHandler{broadcaster: broadcaster}
}

// subscribeRequest represents a subscription request from the client
type subscribeRequest struct {
	CohortIDs []string `json:"cohort_ids,omitempty"`
	UserIDs   []string `json:"user_ids,omitempty"`
}

// HandleWebSocket handles WebSocket connections
// WS /ws/cohort-changes
func (h *WebSocketHandler) HandleWebSocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("failed to upgrade WebSocket: %v", err)
		return
	}
	defer conn.Close()

	// Generate subscription ID
	subscriptionID := uuid.New().String()

	// Default subscription (all changes)
	subscription := &membership.StreamSubscription{
		ID:        subscriptionID,
		CreatedAt: time.Now(),
	}

	// Subscribe to changes
	changeChan := h.broadcaster.Subscribe(subscriptionID, subscription)
	defer h.broadcaster.Unsubscribe(subscriptionID)

	// Handle incoming messages (subscription updates)
	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("websocket error: %v", err)
				}
				return
			}

			var req subscribeRequest
			if err := json.Unmarshal(message, &req); err != nil {
				continue
			}

			// Update subscription filters
			var cohortIDs []uuid.UUID
			for _, id := range req.CohortIDs {
				if parsed, err := uuid.Parse(id); err == nil {
					cohortIDs = append(cohortIDs, parsed)
				}
			}
			subscription.CohortIDs = cohortIDs
			subscription.UserIDs = req.UserIDs
		}
	}()

	// Send changes to client
	for change := range changeChan {
		// Check if change matches subscription filters
		if !subscription.MatchesChange(change) {
			continue
		}

		data, err := json.Marshal(change)
		if err != nil {
			continue
		}

		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Printf("failed to write WebSocket message: %v", err)
			return
		}
	}
}

// SSEHandler handles Server-Sent Events for real-time updates
type SSEHandler struct {
	broadcaster Broadcaster
}

// NewSSEHandler creates a new SSE handler
func NewSSEHandler(broadcaster Broadcaster) *SSEHandler {
	return &SSEHandler{broadcaster: broadcaster}
}

// HandleSSE handles SSE connections
// GET /stream/cohort-changes
func (h *SSEHandler) HandleSSE(c *gin.Context) {
	// Parse query params for filtering
	cohortIDsParam := c.QueryArray("cohort_id")
	userIDsParam := c.QueryArray("user_id")

	var cohortIDs []uuid.UUID
	for _, id := range cohortIDsParam {
		if parsed, err := uuid.Parse(id); err == nil {
			cohortIDs = append(cohortIDs, parsed)
		}
	}

	subscriptionID := uuid.New().String()
	subscription := &membership.StreamSubscription{
		ID:        subscriptionID,
		CohortIDs: cohortIDs,
		UserIDs:   userIDsParam,
		CreatedAt: time.Now(),
	}

	changeChan := h.broadcaster.Subscribe(subscriptionID, subscription)
	defer h.broadcaster.Unsubscribe(subscriptionID)

	// Set SSE headers
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("Transfer-Encoding", "chunked")

	// Send initial connection event
	c.SSEvent("connected", gin.H{"subscription_id": subscriptionID})
	c.Writer.Flush()

	// Create a ticker for keepalive
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	clientGone := c.Request.Context().Done()

	for {
		select {
		case <-clientGone:
			return
		case <-ticker.C:
			c.SSEvent("keepalive", gin.H{"timestamp": time.Now().Unix()})
			c.Writer.Flush()
		case change, ok := <-changeChan:
			if !ok {
				return
			}

			// Check if change matches subscription filters
			if !subscription.MatchesChange(change) {
				continue
			}

			c.SSEvent("membership_change", change)
			c.Writer.Flush()
		}
	}
}
