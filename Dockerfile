FROM eclipse-temurin:17-jdk AS builder

RUN apt-get update && apt-get install -y gnupg2 curl scala
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | apt-key add -

RUN apt-get update && apt-get install -y sbt=1.10.0

RUN mkdir /app

FROM builder AS declare


COPY src /app/src
COPY project /app/project
COPY build.sbt /app/build.sbt

WORKDIR /app
RUN sbt clean assembly
RUN mv target/scala-2.12/DeclareMiningIncrementally-assembly-0.1.0-SNAPSHOT.jar declare.jar

FROM eclipse-temurin:17-jdk AS execution
RUN apt-get update && apt-get install -y gnupg2 curl procps

RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz &&\
tar xvf spark-3.5.6-bin-hadoop3.tgz && mv spark-3.5.6-bin-hadoop3/ /opt/spark && rm spark-3.5.6-bin-hadoop3.tgz

RUN mkdir /app
WORKDIR /app
RUN mkdir -p /tmp/spark-events /app/logs

# Copy application files
COPY --from=declare /app/declare.jar /app/declare.jar
COPY docker-entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Set default environment variables
ENV s3accessKeyAws=minioadmin \
    s3secretKeyAws=minioadmin \
    s3endPointLoc=http://minio:9000 \
    s3ConnectionTimeout=600000 \
    SPARK_MASTER=local[*] \
    SPARK_DRIVER_MEMORY=10g \
    SPARK_EXECUTOR_MEMORY=4g \
    SUPPORT=0.0 \
    BRANCHING_TYPE=TARGET \
    BRANCHING_BOUND=3 \
    OUTPUT_PATH=/app/output \
    HARD_MODE=false \
    FILTER_UNDERBOUND=false

# Create default output and logs directories
RUN mkdir -p /app/output /app/logs

# Default volumes for output and logs
VOLUME ["/app/output", "/app/logs"]

ENTRYPOINT ["/app/entrypoint.sh"]
CMD []

