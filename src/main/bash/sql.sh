#!/bin/bash

# Exit on task failure
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Cluster distribution environment settings
source $DIR/env.sh

# Check Apache phoenix client jar path
if [ ! -f $PHOENIX_CLIENT_PATH ]; then
    echo "Error: PHOENIX_CLIENT_PATH not set in env.sh"
    exit 1
fi

# Set analytics resources path
RESOURCE_PATH=$DIR/../conf/resources

# Set hadoop and hbase configs path
HADOOP_CONF_PATH=/etc/hadoop/conf
HBASE_CONF_PATH=/etc/hbase/conf

# Set analytics-fat jar path
ANALYTICS_JAR_PATH=$(ls $DIR/../jars/analytics-fat.jar)

# Pass all the above path variables to the classpath
CLASSPATH=$RESOURCE_PATH:$HADOOP_CONF_PATH:$HBASE_CONF_PATH:$PHOENIX_CLIENT_PATH:$ANALYTICS_JAR_PATH

# If no args, force printing usage
if [[ -z $1 ]]; then
	echo "Usage: "
	echo "--dbServer    <server>"
	echo "--tenantID    <1 to 3 character tenant ID>"
	echo "--ds          <1 to 3 character Data Source DS>"
	echo "--did         <tiny int value to specify DID for a given DS>"
	echo "--limit       <maximum number of rows to delete>"
	echo "--action      {aggregate, aggregatecontribution, analytics[1-4], clean, cleanscores, console, purge, recreate, cleanbots[1-3], baseline, migrate, migrate_aggregates, checkconfig, checkwdcconfig, checkyarnconfig}"
	exit 1
fi

# Redirect --action console to phoenix-sqlline, it's just better
if [ $(echo "$@" | grep -e "--action console") ]; then
    # We'll drop the dbServer value here, since it's auto-detected
    # In the unlikely event there's multiple zookeeper URLs present, a user can invoke phoenix-sqlline <url> directly
    phoenix-sqlline
else
    java -Xmx8192m -classpath $CLASSPATH com.interset.analytics.sqlrunner.Runner ${*}
    eval $RUN_CMD
fi
