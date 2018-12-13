#!/bin/bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source $DIR/env.sh

# Check analytics jobs jar
if [ ! -f $ANALYTICS_JOBS_JAR ]; then
    echo "Error: Couldn't locate analytics jobs jar (should have been ${ANALYTICS_JOBS_JAR})"
    exit 1
fi

# Spark environment configs
# Check spark master url
if [ -z "$SPARK_MASTER_URL" ]; then
    echo "Error: SPARK_MASTER_URL not set in env.sh"
    exit 1
fi

# Check spark home path
if [ ! -d "$SPARK_HOME" ]; then
    echo "Error: SPARK_HOME not set in env.sh"
    exit 1
fi

# Check spark logging properties file
if [ ! -f $LOG_CONFIG ]; then
    echo "Error: Couldn't locate log configuration file (should have been ${LOG_CONFIG})"
    exit 1
fi

# Check spark metrics.properties path
# Will be pass under --files parameter in spark.submit to get the metrics from executors
if [ ! -f $METRICS_CONFIG ]; then
    echo "Error: Couldn't locate metrics.properties file (should have been ${METRICS_CONFIG})"
    echo "Please ensure that spark2 is properly installed and that ${DIR}/env.sh has the correct ${SPARK_HOME}."
    exit 1
fi

# Adding queue support
# default to "default" if not set!
QUEUE=

if [[ -z "$QUEUE" ]]
then
  QUEUE=default
fi

# To get the spark driver metrics
SPARK_METRIC_CONF="--conf spark.metrics.conf=metrics.properties"

# Find available Spark jobs in JAR file matching "com.interset*Job.class"
ALL_JOBS=$(unzip -l $ANALYTICS_JOBS_JAR | grep "com.interset" | grep "Job\.class" | grep -v "SparkPhoenixJob" | awk '{print $4}' | sed -e 's/\//\./g' | sed -e 's/\.class//g')
TRAINING_JOBS=$(unzip -l $ANALYTICS_JOBS_JAR | grep "com.interset" | grep "Job\.class" | grep -v "SparkPhoenixJob" | awk '{print $4}' | sed -e 's/\//\./g' | sed -e 's/\.class//g' | grep "training")
SCORING_JOBS=$(unzip -l $ANALYTICS_JOBS_JAR | grep "com.interset" | grep "Job\.class" | grep -v "SparkPhoenixJob" | awk '{print $4}' | sed -e 's/\//\./g' | sed -e 's/\.class//g' | grep "scoring" | grep -v "EntityVulnerabilityJob")

# We need entity vulnerability to run last
SCORING_JOBS=$(echo $SCORING_JOBS com.interset.analytics.scoring.EntityVulnerabilityJob)

# Specifically for indexing jobs while saving to elasticsearch
if [ "$taskMaxFailures" != "" ]; then
    SPARK_CONF="--conf spark.task.maxFailures=${taskMaxFailures}"
fi

# Extract custom job name if provided and remove it from the params (needs to be provided before jar)
PARAMS=${*:2}
EXTRACTED_NAME=$1
TENANT_ID=$(echo $PARAMS | grep -oP "(?<=\-\-tenantID ).*?(?=--|$)")

# Default value set for the job in interset.conf file
BATCH=$batchProcessing

# Check the parameter for batchProcessing override value
# If present then overrides the value in interset.conf file
BATCH=$(echo $PARAMS | grep -o -- "\(--batchProcessing \)".* | cut -d " " -f 2)

# Resolve windowed or batch to append to the job name
if [ $BATCH == "false" ]; then
    MODE="windowed"
else
    MODE="batch"
fi

# Check the parameters for numExecutors, executorCores override values
NUMEXECUTORS=$(echo $PARAMS | grep -o -- "\(--customNumExecutors \)".* | cut -d " " -f 2)
EXECUTORCORES=$(echo $PARAMS | grep -o -- "\(--customExecutorCores \)".* | cut -d " " -f 2)
# If present then overrides the values in interset.conf file
if [ "$NUMEXECUTORS" != "" ]; then numExecutors=$NUMEXECUTORS; fi
if [ "$EXECUTORCORES" != "" ]; then executorCores=$EXECUTORCORES; fi

if [ "$EXTRACTED_NAME" != "" ]; then
    # Set job name with tenant and analytics run mode type details for yarn resource UI
    JOB_NAME=${EXTRACTED_NAME//[[:blank:]]/}-${MODE//[[:blank:]]/}-${TENANT_ID//[[:blank:]]/}
    PARAMS=$(echo $PARAMS | sed -e "s/--name $EXTRACTED_NAME//g")
fi

# Print usage
if [ -z $1 ]; then
    echo "Usage: $0 <job class> --tenantID <tid> --dbServer <serverUrl> --num-executors <number> --executor-cores <number>"
    echo "Or"
    echo "Usage: $0 console"
    echo ""
    echo "Available jobs: "
    for i in $ALL_JOBS; do
        echo $i
    done
    echo
    echo "Spark Cluster Settings:"
    echo "Master: $SPARK_MASTER_URL"
    exit 1
fi

# Spark shell
if [ "$1" == "console" ]; then
    echo "Entering spark shell"
    RUN_CMD="$SPARK_HOME/bin/spark-shell --master yarn-client --name $JOB_NAME --jars $ANALYTICS_JOBS_JAR ${*:2}"
else
    # Default analytics job submit
    # Abort script on error
    set -e

    if [ "$JOB_NAME" == "" ]; then
        $JOB_NAME=$1
    fi

    echo "Submitting job: $JOB_NAME"
    RUN_CMD="$SPARK_HOME/bin/spark-submit --class $1 --master yarn --deploy-mode cluster --files $HBASE_HOME/conf/hbase-site.xml,$LOG_CONFIG#log4j.properties,$METRICS_CONFIG  --queue $QUEUE --name $JOB_NAME --num-executors ${numExecutors} --executor-cores ${executorCores} --executor-memory ${executorMem} --driver-memory ${driverMem} $SPARK_CONF $SPARK_METRIC_CONF $ANALYTICS_JOBS_JAR $PARAMS"
fi

eval $RUN_CMD
