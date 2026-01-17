FROM flink:1.18-java11

# Create usrlib directory for user code isolation
RUN mkdir -p /opt/flink/usrlib

# Copy the cohort processor JAR
COPY flink/target/cohort-processor-1.0.0.jar /opt/flink/usrlib/
