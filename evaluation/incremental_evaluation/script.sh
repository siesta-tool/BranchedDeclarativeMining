#!/bin/bash

DIRECTORY=/app/output
PREFIX=$1
SPARK_SUBMIT_COMMAND="/opt/spark/bin/spark-submit --master local[*]"
PREPROCESS_JAR="/app/preprocess.jar"
EXTRACTION_JAR="/app/declare.jar"

export s3accessKeyAws=minioadmin
export s3ConnectionTimeout=600000
export s3endPointLoc=http://localhost:9000
export s3secretKeyAws=minioadmin

# Check if the directory exists
if [ -d "$DIRECTORY" ]; then
  # Find files that match the prefix and sort them by the numeric part
  find "$DIRECTORY" -type f -name "${PREFIX}*" | sort -t '_' -k 4,4n | while read file; do
    # Print the filename
    FILENAME=$(basename "$file")
    echo "Processing $FILENAME"

     # Run Spark job using spark-submit and capture the output
      output=$($SPARK_SUBMIT_COMMAND "$PREPROCESS_JAR" -f "$file" --logname "$PREFIX" 2>&1)

      # Check if the Spark job failed
      if [ $? -ne 0 ]; then
        echo "Error processing $FILENAME"
      else
        echo "$FILENAME processed successfully"
      fi

      # Extract the "Time taken" from the output and print it
      echo "$output" | grep -oP 'Time taken: \d+ ms'
      echo "Extracting declare constraints"

      #Run again spark-submit to extract declare constraints
      output=$($SPARK_SUBMIT_COMMAND "$EXTRACTION_JAR" "$PREFIX" 0 2>&1)
      echo "$output" | grep -oP 'Time taken: \d+ ms'


      # Check if the Spark job failed
      if [ $? -ne 0 ]; then
        echo "Error mining $FILENAME"
      else
        echo "Mined successfully"
      fi

  done
else
  echo "Directory not found!"
fi