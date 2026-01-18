package com.intent.cohort.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.intent.cohort.model.MembershipChange;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;

/**
 * Serializes membership changes to JSON format for the Go inserter service.
 *
 * Output format:
 * {
 *   "cohort_id": "uuid-string",
 *   "cohort_name": "name",
 *   "user_id": "user-id",
 *   "prev_status": -1 or 1,
 *   "new_status": -1 or 1,
 *   "changed_at": "2024-01-01T00:00:00.000Z",
 *   "trigger_event": "uuid-string" (optional)
 * }
 */
public class MembershipChangeSerializer implements SerializationSchema<MembershipChange> {
    private static final Logger LOG = LoggerFactory.getLogger(MembershipChangeSerializer.class);
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public byte[] serialize(MembershipChange change) {
        try {
            if (objectMapper == null) {
                open(null);
            }

            ObjectNode node = objectMapper.createObjectNode();
            node.put("cohort_id", change.getCohortId().toString());
            node.put("cohort_name", change.getCohortName());
            node.put("user_id", change.getUserId());
            node.put("prev_status", change.getPrevStatus());
            node.put("new_status", change.getNewStatus());
            node.put("changed_at", ISO_FORMATTER.format(change.getChangedAt()));
            if (change.getTriggerEvent() != null) {
                node.put("trigger_event", change.getTriggerEvent().toString());
            }

            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            LOG.error("Failed to serialize membership change: {}", change, e);
            return new byte[0];
        }
    }
}
