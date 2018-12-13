import logging
import json
import requests
import subprocess

from StringIO import StringIO

from airflow import DAG
from airflow.models import Variable
from utils import get_interset_env, get_xcom

LOGGER = logging.getLogger(__name__)

ES_CLUSTER_NAME = Variable.get("esClusterName")
ES_HOST = Variable.get("esHost")
ES_PROTOCOL = Variable.get("esProtocol", "http")

interset_env = get_interset_env()
sqlScript = get_xcom("sqlScript")


def _check_hdfs_space(ti, **kwargs):    
    LOGGER.info("Checking HDFS disk space...")
    used = int(StringIO(subprocess.check_output(['hdfs', 'dfs', '-df'])).readlines()[1].split()[4][:-1])
    LOGGER.info("HDFS is at %s%% utilization" % used)
    if used > 80:
        LOGGER.warning("HDFS is above 80% uttilization")


class ValidationError(Exception):
    def __init__(self, message):
        super(ValidationError, self).__init__(message)


def _verify_elasticsearch_settings(dag, ti, **kwargs):
    tenantID = ti.xcom_pull(dag_id=dag.parent_dag.dag_id, task_ids=["generate_xcoms"], key="tenantID")[0]
    # sparkScript = ti.xcom_pull(dag_id=dag.parent_dag.dag_id, task_ids=["generate_xcoms"], key="sparkScript")[0]

    LOGGER.info("Checking elasticsearch...")
    tokens = ES_HOST.split(":")
    host, port = (tokens[0], tokens[1]) if len(tokens) == 2 else (tokens[0], 9200)

    response = requests.get("%s://%s:%s/_cluster/health?pretty" % (ES_PROTOCOL, host, port))
    if response.status_code != 200:
        raise ValidationError("Non-200 return code from ElasticSearch host at %s://%s:%s" % (ES_PROTOCOL, host, port))

    LOGGER.info("ElasticSearch Disk Space:")
    response = requests.get("%s://%s:%s/_cat/nodes?h=h,diskAvail" % (ES_PROTOCOL, host, port))
    if response.status_code != 200:
        raise ValidationError("Non-200 return code from ElasticSearch host at %s://%s:%s" % (ES_PROTOCOL, host, port))

    # TODO: Add disk space warning check

    LOGGER.info("ElasticSearch Indices:")
    for ds in [ "wdc", "repo", "ad", "auditd", "printer", "webproxy" ]:
        response = requests.get("%s://%s:%s/interset_%s_rawdata_%s/_count" % (ES_PROTOCOL, host, port, ds, tenantID))
        json_data = json.loads(response.text)
        if "error" in json_data:
            LOGGER.warning("Index: interset_%s_rawdata_%s is missing" % (ds, tenantID))
        else:
            LOGGER.info("Index: interset_%s_rawdata_%s" % (ds, tenantID))
            LOGGER.info("Documents count: %s" % json_data["count"])

    # check violations index
    response = requests.get("%s://%s:%s/interset_violations_%s/_count" % (ES_PROTOCOL, host, port, tenantID))
    json_data = json.loads(response.text)
    if "error" in json_data:
        LOGGER.warning("Index: interset_violations_%s is missing" % tenantID)
    else:
        LOGGER.info("Index: interset_violations_%s" % tenantID)
        LOGGER.info("Documents count: %s" % json_data["count"])

    # TODO Run sample SparkPi job
    #LOGGER.info("Submitting sample Spark job")
    #subprocess.check_call( [sparkScript, "com.interset.analytics.validation.SparkPi" ])
    #LOGGER.info("Sanity check complete")

def sanity_dag(parent_dag_name, child_dag_name, default_args, schedule_interval):
    from airflow.operators.bash_operator import BashOperator
    from airflow.operators.python_operator import PythonOperator
    dag = DAG("%s.%s" % (parent_dag_name, child_dag_name), default_args=default_args, schedule_interval=schedule_interval)
    t1 = BashOperator(task_id="verify_phoenix",
                      bash_command="""%s \
                                      --action checkconfig \
                                      --tenantID %s \
                                      --dbServer {{ var.value.zkPhoenix }}""" % (get_xcom("sqlScript"), get_xcom("tenantID")),
                      env=interset_env,
                      dag=dag)
    
    t2 = PythonOperator(task_id="check_hdfs_space",
                        python_callable=_check_hdfs_space,
                        provide_context=True,
                        dag=dag)
    
    t3 = PythonOperator(task_id="verify_elasticsearch_settings",
                        python_callable=_verify_elasticsearch_settings,
                        provide_context=True,
                        dag=dag)
                        
    t1 >> t2 >> t3
    return dag
