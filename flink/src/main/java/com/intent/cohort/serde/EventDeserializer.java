package com.intent.cohort.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.intent.cohort.model.Event;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializes events from JSON.
 */
public class EventDeserializer implements DeserializationSchema<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(EventDeserializer.class);

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public Event deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        try {
            if (objectMapper == null) {
                open(null);
            }
            return objectMapper.readValue(message, Event.class);
        } catch (Exception e) {
            LOG.warn("Failed to deserialize event: {}", new String(message), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
