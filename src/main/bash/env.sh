#!/bin/bash

# Pass the bash source path
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Cluster platform configuration
# By default Hortonworks `hdp`
# For Cloudera set values as `cdh`
PLATFORM=hdp

if [ -z "$PLATFORM" ]; then
    echo "Error: PLATFORM not set!"
    echo "Usage: Set 'hdp' if it's Hortonworks or 'cdh' for Cloudera in env.sh file"
    exit 1
fi

export JAVA_HOME="${JAVA_HOME:-/usr/java/latest}"
export SPARK_MASTER_URL=yarn-client
export ANALYTICS_JOBS_JAR=$(echo $DIR/../jars/jobs*.jar)
export LOG_CONFIG=$(echo $DIR/../conf/resources/log4j-spark.properties)
export YARN_QUEUE=interset1

if [ "$PLATFORM" == "hdp" ]; then
    export HADOOP_HOME=/usr/hdp/current/hadoop-client
    export HBASE_HOME=/usr/hdp/current/hbase-client
    export HBASE_REGIONSERVERS=$HBASE_HOME/conf/regionservers
    export HBASE_MANAGES_ZK=true
    export SPARK_HOME=/usr/hdp/current/spark2-client
    export METRICS_CONFIG=$(echo $SPARK_HOME/conf/metrics.properties)
    export SPARK_DEFAULTS_FILE=$(echo $SPARK_HOME/conf/spark-defaults.conf)
    export PHOENIX_CLIENT_PATH=$(echo /usr/hdp/current/phoenix-client/phoenix-client.jar)
elif [ "$PLATFORM" == "cdh" ]; then
    export HADOOP_HOME=/opt/cloudera/parcels/CDH/lib/hadoop
    export HBASE_HOME=/opt/cloudera/parcels/CDH/lib/hbase
    export SPARK_HOME=/opt/cloudera/parcels/SPARK2/lib/spark2
    export METRICS_CONFIG=$(echo $DIR/../conf/resources/metrics.properties)
    export PHOENIX_CLIENT_PATH=$(echo /opt/cloudera/parcels/APACHE_PHOENIX/lib/phoenix/phoenix-4.13.1-HBase-1.2-CDH5.13.1-client.jar)
else
    echo "Error: Unable to set cluster environment"
    exit 1
fi

export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HBASE_HOME/bin
