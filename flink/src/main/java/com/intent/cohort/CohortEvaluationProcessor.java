package com.intent.cohort;

import com.intent.cohort.model.CohortDefinition;
import com.intent.cohort.model.CohortDefinition.Condition;
import com.intent.cohort.model.Event;
import com.intent.cohort.model.MembershipChange;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

/**
 * Processes events and evaluates cohort membership using broadcast state for cohort definitions
 * and keyed state per user for event history and aggregates.
 *
 * State:
 * - Broadcast state: All active cohort definitions
 * - Keyed state (per user):
 *   - Event history (time-bucketed for efficient sliding window queries)
 *   - Current membership status per cohort
 *   - Aggregates per event type (count, sum, etc.)
 */
public class CohortEvaluationProcessor
        extends KeyedBroadcastProcessFunction<String, Event, CohortDefinition, MembershipChange> {

    private static final Logger LOG = LoggerFactory.getLogger(CohortEvaluationProcessor.class);

    // Time bucket size: 1 minute (for sliding window aggregates)
    private static final long BUCKET_SIZE_MILLIS = 60 * 1000;

    // Maximum event history to keep per user (90 days worth of buckets)
    private static final int MAX_BUCKETS = 90 * 24 * 60;

    // Keyed state for user's current membership status per cohort
    private transient MapState<UUID, Boolean> membershipState;

    // Keyed state for event counts per (event_name, time_bucket)
    private transient MapState<String, Long> eventCountState;

    // Keyed state for event sums per (event_name, field, time_bucket)
    private transient MapState<String, Double> eventSumState;

    // Keyed state for tracking which events user has performed (for simple event conditions)
    private transient MapState<String, Long> lastEventTimestamp;

    @Override
    public void open(Configuration parameters) {
        // Initialize keyed state
        membershipState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("membership-status",
                        TypeInformation.of(UUID.class),
                        TypeInformation.of(Boolean.class)));

        eventCountState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("event-counts",
                        TypeInformation.of(String.class),
                        TypeInformation.of(Long.class)));

        eventSumState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("event-sums",
                        TypeInformation.of(String.class),
                        TypeInformation.of(Double.class)));

        lastEventTimestamp = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("last-event-timestamp",
                        TypeInformation.of(String.class),
                        TypeInformation.of(Long.class)));
    }

    @Override
    public void processElement(Event event, ReadOnlyContext ctx, Collector<MembershipChange> out) throws Exception {
        String userId = event.getUserId();
        long eventTime = event.getTimestamp().toEpochMilli();
        String eventName = event.getEventName();

        // Update event state
        updateEventState(event, eventTime);

        // Get all active cohort definitions from broadcast state
        Iterable<Map.Entry<UUID, CohortDefinition>> cohorts =
                ctx.getBroadcastState(CohortProcessorJob.COHORT_STATE_DESCRIPTOR).immutableEntries();

        // Evaluate each cohort
        for (Map.Entry<UUID, CohortDefinition> entry : cohorts) {
            CohortDefinition cohort = entry.getValue();

            if (!cohort.isActive()) {
                continue;
            }

            // Check if this event is relevant to the cohort
            if (!isEventRelevant(event, cohort)) {
                continue;
            }

            // Evaluate membership
            boolean isMember = evaluateMembership(cohort, eventTime);
            Boolean wasMember = membershipState.get(cohort.getId());

            if (wasMember == null) {
                wasMember = false;
            }

            // Check for membership change
            if (isMember && !wasMember) {
                // User entered cohort
                membershipState.put(cohort.getId(), true);
                out.collect(MembershipChange.entered(
                        cohort.getId(),
                        cohort.getName(),
                        userId,
                        event.getId()
                ));
                LOG.debug("User {} entered cohort {}", userId, cohort.getName());
            } else if (!isMember && wasMember) {
                // User exited cohort
                membershipState.put(cohort.getId(), false);
                out.collect(MembershipChange.exited(
                        cohort.getId(),
                        cohort.getName(),
                        userId,
                        event.getId()
                ));
                LOG.debug("User {} exited cohort {}", userId, cohort.getName());
            }
        }

        // Clean up old state (buckets older than 90 days)
        cleanupOldState(eventTime);
    }

    @Override
    public void processBroadcastElement(CohortDefinition cohort, Context ctx, Collector<MembershipChange> out) throws Exception {
        // Update broadcast state with new cohort definition
        if (cohort == null || cohort.getId() == null) {
            return;
        }

        LOG.info("Received cohort definition update: {} (version {})", cohort.getName(), cohort.getVersion());

        ctx.getBroadcastState(CohortProcessorJob.COHORT_STATE_DESCRIPTOR)
                .put(cohort.getId(), cohort);
    }

    private void updateEventState(Event event, long eventTime) throws Exception {
        String eventName = event.getEventName();
        long bucket = eventTime / BUCKET_SIZE_MILLIS;

        // Update event count
        String countKey = eventName + ":" + bucket;
        Long currentCount = eventCountState.get(countKey);
        eventCountState.put(countKey, (currentCount == null ? 0 : currentCount) + 1);

        // Update event sums for numeric properties
        if (event.getProperties() != null) {
            for (Map.Entry<String, Object> prop : event.getProperties().entrySet()) {
                if (prop.getValue() instanceof Number) {
                    String sumKey = eventName + ":" + prop.getKey() + ":" + bucket;
                    Double currentSum = eventSumState.get(sumKey);
                    double value = ((Number) prop.getValue()).doubleValue();
                    eventSumState.put(sumKey, (currentSum == null ? 0 : currentSum) + value);
                }
            }
        }

        // Update last event timestamp
        lastEventTimestamp.put(eventName, eventTime);
    }

    private boolean isEventRelevant(Event event, CohortDefinition cohort) {
        if (cohort.getRules() == null || cohort.getRules().getConditions() == null) {
            return false;
        }

        String eventName = event.getEventName();
        for (Condition condition : cohort.getRules().getConditions()) {
            if (eventName.equals(condition.getEventName())) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluateMembership(CohortDefinition cohort, long currentTime) throws Exception {
        if (cohort.getRules() == null || cohort.getRules().getConditions() == null) {
            return false;
        }

        List<Condition> conditions = cohort.getRules().getConditions();
        String operator = cohort.getRules().getOperator();
        boolean isAnd = "AND".equalsIgnoreCase(operator);

        for (Condition condition : conditions) {
            boolean result = evaluateCondition(condition, currentTime);

            if (isAnd && !result) {
                return false; // AND: any false makes it false
            }
            if (!isAnd && result) {
                return true; // OR: any true makes it true
            }
        }

        return isAnd; // AND: all true -> true; OR: all false -> false
    }

    private boolean evaluateCondition(Condition condition, long currentTime) throws Exception {
        String type = condition.getType();

        switch (type) {
            case "event":
                return evaluateEventCondition(condition, currentTime);
            case "aggregate":
                return evaluateAggregateCondition(condition, currentTime);
            case "property":
                // Property conditions would require user profile state
                return false;
            default:
                return false;
        }
    }

    private boolean evaluateEventCondition(Condition condition, long currentTime) throws Exception {
        String eventName = condition.getEventName();
        Long lastTimestamp = lastEventTimestamp.get(eventName);

        if (lastTimestamp == null) {
            return false; // User never performed this event
        }

        // Check time window if specified
        if (condition.getTimeWindow() != null) {
            long windowMillis = condition.getTimeWindow().getDurationMillis();
            if (windowMillis > 0) {
                long windowStart = currentTime - windowMillis;
                return lastTimestamp >= windowStart;
            }
        }

        return true; // User has performed the event
    }

    private boolean evaluateAggregateCondition(Condition condition, long currentTime) throws Exception {
        String eventName = condition.getEventName();
        String aggregation = condition.getAggregation();
        String field = condition.getAggregationField();
        String op = condition.getOperator();
        double targetValue = condition.getValueAsDouble();

        // Calculate time window
        long windowStart = 0;
        if (condition.getTimeWindow() != null) {
            long windowMillis = condition.getTimeWindow().getDurationMillis();
            if (windowMillis > 0) {
                windowStart = currentTime - windowMillis;
            }
        }

        long startBucket = windowStart / BUCKET_SIZE_MILLIS;
        long endBucket = currentTime / BUCKET_SIZE_MILLIS;

        double aggregateValue = 0;

        switch (aggregation) {
            case "count":
                for (long bucket = startBucket; bucket <= endBucket; bucket++) {
                    String key = eventName + ":" + bucket;
                    Long count = eventCountState.get(key);
                    if (count != null) {
                        aggregateValue += count;
                    }
                }
                break;

            case "sum":
                if (field == null) return false;
                // Extract field name from path like "properties.amount"
                String fieldName = field.contains(".") ? field.substring(field.lastIndexOf('.') + 1) : field;
                for (long bucket = startBucket; bucket <= endBucket; bucket++) {
                    String key = eventName + ":" + fieldName + ":" + bucket;
                    Double sum = eventSumState.get(key);
                    if (sum != null) {
                        aggregateValue += sum;
                    }
                }
                break;

            default:
                return false;
        }

        // Compare aggregate value with target
        return compareValues(aggregateValue, op, targetValue);
    }

    private boolean compareValues(double actual, String operator, double target) {
        switch (operator) {
            case "eq": return actual == target;
            case "ne": return actual != target;
            case "gt": return actual > target;
            case "gte": return actual >= target;
            case "lt": return actual < target;
            case "lte": return actual <= target;
            default: return false;
        }
    }

    private void cleanupOldState(long currentTime) throws Exception {
        long oldestValidBucket = (currentTime - (90L * 24 * 60 * 60 * 1000)) / BUCKET_SIZE_MILLIS;

        // Clean up event counts
        List<String> keysToRemove = new ArrayList<>();
        for (String key : eventCountState.keys()) {
            String[] parts = key.split(":");
            if (parts.length >= 2) {
                try {
                    long bucket = Long.parseLong(parts[parts.length - 1]);
                    if (bucket < oldestValidBucket) {
                        keysToRemove.add(key);
                    }
                } catch (NumberFormatException ignored) {}
            }
        }
        for (String key : keysToRemove) {
            eventCountState.remove(key);
        }

        // Clean up event sums
        keysToRemove.clear();
        for (String key : eventSumState.keys()) {
            String[] parts = key.split(":");
            if (parts.length >= 3) {
                try {
                    long bucket = Long.parseLong(parts[parts.length - 1]);
                    if (bucket < oldestValidBucket) {
                        keysToRemove.add(key);
                    }
                } catch (NumberFormatException ignored) {}
            }
        }
        for (String key : keysToRemove) {
            eventSumState.remove(key);
        }
    }
}
