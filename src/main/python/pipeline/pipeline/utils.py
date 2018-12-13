import os
import re

from airflow.operators.bash_operator import BashOperator


class ESBashOperator(BashOperator):
    def __init__(self, *args, **kwargs):
        super(ESBashOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        # Hacky way to remove the arguments that are 'None'
        self.bash_command = re.sub(r" --\w+ None", "", re.sub(r"\s+", " ", self.bash_command))
        super(ESBashOperator, self).execute(context)


def get_xcom(name):
    return '{{ ti.xcom_pull(dag_id=dag.parent_dag.dag_id, task_ids=["generate_xcoms"], key="%s")[0] }}' % name


def get_es_params():
    es_params = "--esClusterName {{ var.value.esClusterName }} --esHost {{ var.value.esHost }}"
    xcom_params = [ "esIndexName", "sslEnabled", "selfSignedAllowed", "esXPackUser", "keystorePath", "keystorePassword", "truststorePath", "truststorePassword" ]

    for param in xcom_params:
        es_params = es_params + " --%s %s" % (param, get_xcom(param))

    return es_params


def get_interset_env():
    interset_env = os.environ.copy()
    interset_env["numExecutors"] = get_xcom("numExecutors")
    interset_env["executorMem"] = get_xcom("executorMem")
    interset_env["executorCores"] = get_xcom("executorCores")
    interset_env["driverMem"] = get_xcom("driverMem")
    return interset_env
