package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID         string                 `json:"id"`
	UserID     string                 `json:"user_id"`
	EventName  string                 `json:"event_name"`
	Properties map[string]interface{} `json:"properties"`
	Timestamp  string                 `json:"timestamp"`
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Generate 100 users
	numUsers := 100
	users := make([]string, numUsers)
	for i := 0; i < numUsers; i++ {
		users[i] = fmt.Sprintf("user_%03d", i+1)
	}

	// Event types
	eventTypes := []string{
		"page_view",
		"purchase_completed",
		"add_to_cart",
		"signup",
		"login",
	}

	// Products for purchase events
	products := []string{"laptop", "phone", "tablet", "headphones", "watch"}

	// Generate events over the last 30 days
	endTime := time.Now()
	startTime := endTime.AddDate(0, 0, -30)

	file, err := os.Create("testdata/events.jsonl")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	totalEvents := 0
	purchasesByUser := make(map[string]float64)

	// Generate events
	for _, userID := range users {
		// Each user has between 5-50 events
		numEvents := rand.Intn(46) + 5

		for i := 0; i < numEvents; i++ {
			eventTime := startTime.Add(time.Duration(rand.Int63n(int64(endTime.Sub(startTime)))))
			eventName := eventTypes[rand.Intn(len(eventTypes))]

			event := Event{
				ID:        uuid.New().String(),
				UserID:    userID,
				EventName: eventName,
				Timestamp: eventTime.UTC().Format(time.RFC3339),
			}

			// Add properties based on event type
			switch eventName {
			case "purchase_completed":
				amount := float64(rand.Intn(500) + 10) // $10 - $510
				product := products[rand.Intn(len(products))]
				event.Properties = map[string]interface{}{
					"amount":   amount,
					"product":  product,
					"currency": "USD",
				}
				purchasesByUser[userID] += amount
			case "page_view":
				pages := []string{"/home", "/products", "/cart", "/checkout", "/about"}
				event.Properties = map[string]interface{}{
					"page": pages[rand.Intn(len(pages))],
				}
			case "add_to_cart":
				event.Properties = map[string]interface{}{
					"product":  products[rand.Intn(len(products))],
					"quantity": rand.Intn(3) + 1,
				}
			case "signup":
				event.Properties = map[string]interface{}{
					"source": []string{"organic", "referral", "paid"}[rand.Intn(3)],
				}
			case "login":
				event.Properties = map[string]interface{}{
					"method": []string{"email", "google", "github"}[rand.Intn(3)],
				}
			}

			jsonBytes, _ := json.Marshal(event)
			file.WriteString(string(jsonBytes) + "\n")
			totalEvents++
		}
	}

	// Print summary
	fmt.Printf("Generated %d events for %d users\n", totalEvents, numUsers)
	fmt.Println("\nHigh-value users (purchases > $500 in last 30 days):")
	highValueCount := 0
	for userID, total := range purchasesByUser {
		if total > 500 {
			fmt.Printf("  %s: $%.2f\n", userID, total)
			highValueCount++
		}
	}
	fmt.Printf("\nTotal high-value users: %d\n", highValueCount)
}
