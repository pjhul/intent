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
 * Serializes membership changes to JSON format compatible with ClickHouse Kafka table engine.
 *
 * Output format:
 * {
 *   "cohort_id": "uuid-string",
 *   "user_id": "user-id",
 *   "is_member": 0 or 1,
 *   "timestamp": "2024-01-01T00:00:00.000Z"
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

            // Create ClickHouse-compatible format
            ObjectNode node = objectMapper.createObjectNode();
            node.put("cohort_id", change.getCohortId().toString());
            node.put("user_id", change.getUserId());
            // Convert status (-1 = out, 1 = in) to is_member (0 or 1)
            node.put("is_member", change.getNewStatus() == MembershipChange.STATUS_IN ? 1 : 0);
            node.put("timestamp", ISO_FORMATTER.format(change.getChangedAt()));

            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            LOG.error("Failed to serialize membership change: {}", change, e);
            return new byte[0];
        }
    }
}
