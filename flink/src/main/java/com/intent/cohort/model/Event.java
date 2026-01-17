package com.intent.cohort.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a user event from the events stream.
 */
public class Event implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("id")
    private UUID id;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("event_name")
    private String eventName;

    @JsonProperty("properties")
    private Map<String, Object> properties;

    @JsonProperty("timestamp")
    private Instant timestamp;

    @JsonProperty("received_at")
    private Instant receivedAt;

    public Event() {}

    public Event(UUID id, String userId, String eventName, Map<String, Object> properties, Instant timestamp) {
        this.id = id;
        this.userId = userId;
        this.eventName = eventName;
        this.properties = properties;
        this.timestamp = timestamp;
        this.receivedAt = Instant.now();
    }

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getEventName() { return eventName; }
    public void setEventName(String eventName) { this.eventName = eventName; }

    public Map<String, Object> getProperties() { return properties; }
    public void setProperties(Map<String, Object> properties) { this.properties = properties; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public Instant getReceivedAt() { return receivedAt; }
    public void setReceivedAt(Instant receivedAt) { this.receivedAt = receivedAt; }

    /**
     * Get a property value as a double, useful for aggregations.
     */
    public Double getPropertyAsDouble(String key) {
        if (properties == null || !properties.containsKey(key)) {
            return null;
        }
        Object value = properties.get(key);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return null;
    }

    /**
     * Get a property value as a string.
     */
    public String getPropertyAsString(String key) {
        if (properties == null || !properties.containsKey(key)) {
            return null;
        }
        Object value = properties.get(key);
        return value != null ? value.toString() : null;
    }

    @Override
    public String toString() {
        return "Event{" +
                "id=" + id +
                ", userId='" + userId + '\'' +
                ", eventName='" + eventName + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
