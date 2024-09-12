FROM openjdk:11 AS builder

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

FROM builder AS preprocess
RUN apt-get install git

WORKDIR /app
RUN git clone -b last_checked_position https://github.com/mavroudo/SequenceDetectionPreprocess.git
WORKDIR /app/SequenceDetectionPreprocess
RUN sbt clean assembly
RUN ls target/scala-2.12/
RUN mv target/scala-2.12/sequencedetectionpreprocess-assembly-0.1.jar preprocess.jar

FROM openjdk:11 AS execution
RUN apt-get update && apt-get install -y gnupg2 curl procps

RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz &&\
tar xvf spark-3.5.1-bin-hadoop3.tgz && mv spark-3.5.1-bin-hadoop3/ /opt/spark && rm spark-3.5.1-bin-hadoop3.tgz

RUN mkdir /app
WORKDIR /app
RUN mkdir /app/output
COPY --from=declare /app/declare.jar /app/declare.jar
COPY --from=preprocess /app/SequenceDetectionPreprocess/preprocess.jar /app/preprocess.jar
COPY evaluation/incremental_evaluation/script.sh /app/script.sh

CMD ["tail","-f","/dev/null"]
