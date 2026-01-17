package com.intent.cohort;

import com.intent.cohort.model.CohortDefinition;
import com.intent.cohort.model.Event;
import com.intent.cohort.model.MembershipChange;
import com.intent.cohort.serde.CohortDefinitionDeserializer;
import com.intent.cohort.serde.EventDeserializer;
import com.intent.cohort.serde.MembershipChangeSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

/**
 * Main Flink job for processing events and evaluating cohort membership.
 *
 * Architecture:
 * 1. Events stream from Kafka (events.raw topic) - keyed by user_id
 * 2. Cohort definitions from Kafka (cohort.definitions topic) - broadcast to all operators
 * 3. CohortEvaluationProcessor evaluates each event against all active cohorts
 * 4. Membership changes emitted to Kafka (cohort.changes topic) and ClickHouse
 */
public class CohortProcessorJob {
    private static final Logger LOG = LoggerFactory.getLogger(CohortProcessorJob.class);

    // State descriptor for broadcast cohort definitions
    public static final MapStateDescriptor<UUID, CohortDefinition> COHORT_STATE_DESCRIPTOR =
            new MapStateDescriptor<>(
                    "cohort-definitions",
                    TypeInformation.of(UUID.class),
                    TypeInformation.of(CohortDefinition.class)
            );

    public static void main(String[] args) throws Exception {
        // Configuration from environment or args
        String kafkaBrokers = getConfig(args, "kafka.brokers", "localhost:9092");
        String eventsTopic = getConfig(args, "kafka.events.topic", "events.raw");
        String cohortsTopic = getConfig(args, "kafka.cohorts.topic", "cohort.definitions");
        String membershipTopic = getConfig(args, "kafka.membership.topic", "cohort.membership");
        String consumerGroup = getConfig(args, "kafka.consumer.group", "cohort-processor");

        LOG.info("Starting Cohort Processor Job");
        LOG.info("Kafka brokers: {}", kafkaBrokers);
        LOG.info("Events topic: {}", eventsTopic);
        LOG.info("Cohorts topic: {}", cohortsTopic);
        LOG.info("Membership topic: {}", membershipTopic);

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(60000); // 1 minute
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);

        // Create Kafka source for events
        KafkaSource<Event> eventsSource = KafkaSource.<Event>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics(eventsTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new EventDeserializer())
                .build();

        // Create Kafka source for cohort definitions (compacted topic)
        KafkaSource<CohortDefinition> cohortsSource = KafkaSource.<CohortDefinition>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics(cohortsTopic)
                .setGroupId(consumerGroup + "-cohorts")
                .setStartingOffsets(OffsetsInitializer.earliest()) // Read all from compacted topic
                .setValueOnlyDeserializer(new CohortDefinitionDeserializer())
                .build();

        // Events stream with watermarks
        DataStream<Event> eventsStream = env.fromSource(
                eventsSource,
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) ->
                                event.getTimestamp() != null ? event.getTimestamp().toEpochMilli() : timestamp),
                "events-source"
        );

        // Cohort definitions stream (broadcast)
        DataStream<CohortDefinition> cohortsStream = env.fromSource(
                cohortsSource,
                WatermarkStrategy.noWatermarks(),
                "cohorts-source"
        );

        // Broadcast cohort definitions to all parallel instances
        BroadcastStream<CohortDefinition> broadcastCohorts = cohortsStream.broadcast(COHORT_STATE_DESCRIPTOR);

        // Process events with broadcast cohort definitions
        SingleOutputStreamOperator<MembershipChange> membershipChanges = eventsStream
                .keyBy(Event::getUserId)
                .connect(broadcastCohorts)
                .process(new CohortEvaluationProcessor())
                .name("cohort-evaluation");

        // Create Kafka sink for membership changes (ClickHouse consumes via Kafka table engine)
        KafkaSink<MembershipChange> kafkaSink = KafkaSink.<MembershipChange>builder()
                .setBootstrapServers(kafkaBrokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(membershipTopic)
                        .setKeySerializationSchema((MembershipChange change) -> change.getUserId().getBytes())
                        .setValueSerializationSchema(new MembershipChangeSerializer())
                        .build())
                .build();

        // Output to Kafka (ClickHouse will consume via Kafka table engine)
        membershipChanges.sinkTo(kafkaSink).name("kafka-membership-sink");

        // Execute the job
        env.execute("Cohort Processor");
    }

    private static String getConfig(String[] args, String key, String defaultValue) {
        // First check system properties
        String value = System.getProperty(key);
        if (value != null) {
            return value;
        }

        // Then check environment variables (convert dots to underscores and uppercase)
        String envKey = key.replace(".", "_").toUpperCase();
        value = System.getenv(envKey);
        if (value != null) {
            return value;
        }

        // Check args
        for (int i = 0; i < args.length - 1; i++) {
            if (("--" + key).equals(args[i])) {
                return args[i + 1];
            }
        }

        return defaultValue;
    }
}
