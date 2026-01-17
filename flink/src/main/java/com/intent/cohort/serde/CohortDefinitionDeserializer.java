package com.intent.cohort.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.intent.cohort.model.CohortDefinition;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializes cohort definitions from JSON.
 */
public class CohortDefinitionDeserializer implements DeserializationSchema<CohortDefinition> {
    private static final Logger LOG = LoggerFactory.getLogger(CohortDefinitionDeserializer.class);

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public CohortDefinition deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null; // Tombstone message (deletion)
        }
        try {
            if (objectMapper == null) {
                open(null);
            }
            return objectMapper.readValue(message, CohortDefinition.class);
        } catch (Exception e) {
            LOG.warn("Failed to deserialize cohort definition: {}", new String(message), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(CohortDefinition cohort) {
        return false;
    }

    @Override
    public TypeInformation<CohortDefinition> getProducedType() {
        return TypeInformation.of(CohortDefinition.class);
    }
}
