#!/bin/bash

echo "In script"
DIRECTORY=/app/output
RESULT=/app/output/results.txt
LOG=$1
SUP=$2
POLICY=$3
BOUND=$4
SPARK_SUBMIT_COMMAND="/opt/spark/bin/spark-submit --master local[*] --driver-memory 50g --conf spark.eventLog.enabled=true --conf
spark.eventLog.dir=/tmp/spark-events"
EXTRACTION_JAR="/app/declare.jar"

export s3accessKeyAws=minioadmin
export s3ConnectionTimeout=600000
export s3endPointLoc=http://anaconda.csd.auth.gr:9000
export s3secretKeyAws=minioadmin

# Check if the directory exists
if [ -d "$DIRECTORY" ]; then

  echo "Extracting declare constraints" >> $RESULT
  output=$($SPARK_SUBMIT_COMMAND "$EXTRACTION_JAR" "$LOG" "$SUP" "$POLICY" "$BOUND" 2>&1)
  echo "$output"
  echo "$output" | grep -oP 'Time taken: \d+ ms' >> $RESULT


  # Check if the Spark job failed
  if [ $? -ne 0 ]; then
    echo "Error mining $FILENAME" >> $RESULT
  else
    echo "Mined successfully" >> $RESULT
  fi

else
  echo "Directory not found!"
fi