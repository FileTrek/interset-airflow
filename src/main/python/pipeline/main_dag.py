import json
import logging
import os
import sys

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta

from pipeline.aggregate_dag import aggregate_dag
from pipeline.sanity_dag import sanity_dag
from pipeline.scoring_dag import scoring_dag
from pipeline.sync_dag import sync_dag
from pipeline.training_dag import training_dag

VERSION = "0.0.1"
DAG_NAME = "pipeline_development_v%s" % VERSION
CONF_FILE = "%s/dags/%s/conf/interset.conf" % (os.environ["AIRFLOW_HOME"], DAG_NAME)
config = json.load(open(CONF_FILE, 'r'))
config["sparkScript"] = "%s/dags/%s/bin/spark.sh" % (os.environ["AIRFLOW_HOME"], DAG_NAME)
config["sqlScript"] = "%s/dags/%s/bin/sql.sh" % (os.environ["AIRFLOW_HOME"], DAG_NAME)

LOGGER = logging.getLogger(__name__)

START_DATE = datetime(2017, 8, 9)
SCHEDULE_INTERVAL = "@daily"
EMAIL = Variable.get("email_on_failure", "").split(',')

# Default arguments for each DAG/operator
DEFAULT_ARGS = {
    'owner': "interset",
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': EMAIL,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Declare the main DAG that we use
main_dag = DAG(DAG_NAME, default_args=DEFAULT_ARGS, schedule_interval=SCHEDULE_INTERVAL)

expected_vars = { "tenantID", "batchProcessing", "esIndexName", "parallelism", "numExecutors", "executorMem", "executorCores", "driverMem", "storiesFolder", "entityScoresFolder" }

def generate_xcoms_func(dag_run, ti, **kwargs):
    for key, value in config.iteritems():
        ti.xcom_push(key=key, value=value)
        LOGGER.info("%s: %s" % (key, value))

generate_xcoms = PythonOperator(
    task_id="generate_xcoms",
    provide_context=True,
    python_callable=generate_xcoms_func,
    dag=main_dag)

sane_dag = SubDagOperator(task_id="sanity", default_args=DEFAULT_ARGS, subdag=sanity_dag(DAG_NAME, "sanity", default_args=DEFAULT_ARGS, schedule_interval=SCHEDULE_INTERVAL), dag=main_dag)
agg_dag = SubDagOperator(task_id="aggregate", default_args=DEFAULT_ARGS, subdag=aggregate_dag(DAG_NAME, "aggregate", default_args=DEFAULT_ARGS, schedule_interval=SCHEDULE_INTERVAL), dag=main_dag)
train_dag = SubDagOperator(task_id="training", default_args=DEFAULT_ARGS, subdag=training_dag(DAG_NAME, "training", default_args=DEFAULT_ARGS, schedule_interval=SCHEDULE_INTERVAL), dag=main_dag)
score_dag = SubDagOperator(task_id="scoring", default_args=DEFAULT_ARGS, subdag=scoring_dag(DAG_NAME, "scoring", default_args=DEFAULT_ARGS, schedule_interval=SCHEDULE_INTERVAL), dag=main_dag)
sd = SubDagOperator(task_id="sync", default_args=DEFAULT_ARGS, subdag=sync_dag(DAG_NAME, "sync", default_args=DEFAULT_ARGS, schedule_interval=SCHEDULE_INTERVAL), dag=main_dag)

generate_xcoms >> sane_dag >> agg_dag >> train_dag >> score_dag >> sd
