package com.intent.cohort.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Represents a cohort definition with rules for membership.
 */
public class CohortDefinition implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("id")
    private UUID id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("description")
    private String description;

    @JsonProperty("rules")
    private Rules rules;

    @JsonProperty("status")
    private String status;

    @JsonProperty("version")
    private long version;

    @JsonProperty("created_at")
    private Instant createdAt;

    @JsonProperty("updated_at")
    private Instant updatedAt;

    public CohortDefinition() {}

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public Rules getRules() { return rules; }
    public void setRules(Rules rules) { this.rules = rules; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }

    public boolean isActive() {
        return "active".equals(status);
    }

    /**
     * Rules container with operator and conditions.
     */
    public static class Rules implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("operator")
        private String operator; // AND, OR

        @JsonProperty("conditions")
        private List<Condition> conditions;

        public String getOperator() { return operator; }
        public void setOperator(String operator) { this.operator = operator; }

        public List<Condition> getConditions() { return conditions; }
        public void setConditions(List<Condition> conditions) { this.conditions = conditions; }
    }

    /**
     * A single condition for cohort membership.
     */
    public static class Condition implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("type")
        private String type; // event, property, aggregate

        @JsonProperty("event_name")
        private String eventName;

        @JsonProperty("property_name")
        private String propertyName;

        @JsonProperty("aggregation")
        private String aggregation; // count, sum, avg, min, max, distinct_count

        @JsonProperty("aggregation_field")
        private String aggregationField;

        @JsonProperty("time_window")
        private TimeWindow timeWindow;

        @JsonProperty("operator")
        private String operator; // eq, ne, gt, gte, lt, lte, in, nin

        @JsonProperty("value")
        private Object value;

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public String getEventName() { return eventName; }
        public void setEventName(String eventName) { this.eventName = eventName; }

        public String getPropertyName() { return propertyName; }
        public void setPropertyName(String propertyName) { this.propertyName = propertyName; }

        public String getAggregation() { return aggregation; }
        public void setAggregation(String aggregation) { this.aggregation = aggregation; }

        public String getAggregationField() { return aggregationField; }
        public void setAggregationField(String aggregationField) { this.aggregationField = aggregationField; }

        public TimeWindow getTimeWindow() { return timeWindow; }
        public void setTimeWindow(TimeWindow timeWindow) { this.timeWindow = timeWindow; }

        public String getOperator() { return operator; }
        public void setOperator(String operator) { this.operator = operator; }

        public Object getValue() { return value; }
        public void setValue(Object value) { this.value = value; }

        public double getValueAsDouble() {
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            return 0.0;
        }
    }

    /**
     * Time window specification.
     */
    public static class TimeWindow implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("type")
        private String type; // sliding, absolute

        @JsonProperty("duration")
        private String duration; // e.g., "30d", "7d", "24h"

        @JsonProperty("start")
        private Instant start;

        @JsonProperty("end")
        private Instant end;

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public String getDuration() { return duration; }
        public void setDuration(String duration) { this.duration = duration; }

        public Instant getStart() { return start; }
        public void setStart(Instant start) { this.start = start; }

        public Instant getEnd() { return end; }
        public void setEnd(Instant end) { this.end = end; }

        /**
         * Parse duration string and return milliseconds.
         */
        public long getDurationMillis() {
            if (duration == null || duration.isEmpty()) {
                return 0;
            }

            String numPart = duration.substring(0, duration.length() - 1);
            char unit = duration.charAt(duration.length() - 1);
            long num = Long.parseLong(numPart);

            switch (unit) {
                case 's': return num * 1000;
                case 'm': return num * 60 * 1000;
                case 'h': return num * 60 * 60 * 1000;
                case 'd': return num * 24 * 60 * 60 * 1000;
                default: return 0;
            }
        }
    }

    @Override
    public String toString() {
        return "CohortDefinition{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", status='" + status + '\'' +
                ", version=" + version +
                '}';
    }
}
