from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from utils import get_interset_env, get_xcom


def aggregate_dag(parent_dag_name, child_dag_name, default_args, schedule_interval):
    dag = DAG("%s.%s" % (parent_dag_name, child_dag_name), default_args=default_args,
              schedule_interval=schedule_interval)

    interset_env = get_interset_env()
    sqlScript = get_xcom("sqlScript")
    sparkScript = get_xcom("sparkScript")
    tenantID = get_xcom("tenantID")
    batchProcessing = get_xcom("batchProcessing")
    parallelism = get_xcom("parallelism")

    # Very start of analytics, mark down metadata
    # $DIR/sql.sh --action meta --label "ANALYTICS_START" --tenantID ${tenantID} --dbServer $zkPhoenix
    mark_start = BashOperator(task_id="mark_start",
                              bash_command="""%s \
                                              --action meta \
                                              --label \"ANALYTICS_START\" \
                                              --tenantID %s \
                                              --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, tenantID),
                              env=interset_env,
                              dag=dag)

    # Mark aggregate start in metadata
    # $DIR/sql.sh --action meta --label "AGGREGATE_START" --batchProcessing ${batchProcessing} --tenantID ${tenantID} --dbServer $zkPhoenix
    mark_aggregate_start = BashOperator(task_id="mark_aggregate_start",
                                        bash_command="""%s \
                                                        --action meta \
                                                        --label \"AGGREGATE_START\" \
                                                        --batchProcessing %s \
                                                        --tenantID %s \
                                                        --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, batchProcessing, tenantID),
                                        env=interset_env,
                                        dag=dag)

    # Initialize weights / params (if needed)
    # $DIR/sql.sh --action initialize --tenantID ${tenantID} --dbServer $zkPhoenix
    initialize_weights = BashOperator(task_id="initialize_weights",
                                      bash_command="""%s \
                                                      --action initialize \
                                                      --tenantID %s \
                                                      --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, tenantID),
                                      env=interset_env,
                                      dag=dag)

    # Kickoff entity remapping
    # $DIR/spark.sh com.interset.analytics.aggregation.EntityMappingJob --batchProcessing ${batchProcessing} --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism
    entity_remapping = BashOperator(task_id="entity_remapping",
                                    bash_command="""%s com.interset.analytics.aggregation.EntityMappingJob \
                                                    --batchProcessing %s \
                                                    --tenantID %s \
                                                    --dbServer {{ var.value.zkPhoenix }} \
                                                    --parallelism %s""" % (sparkScript, batchProcessing, tenantID, parallelism),
                                    env=interset_env,
                                    dag=dag)

    # Bot classifier
    # $DIR/spark.sh com.interset.analytics.scoring.BotClassifierJob --batchProcessing ${batchProcessing} --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism
    bot_classifier = BashOperator(task_id="bot_classifier",
                                  bash_command="""%s com.interset.analytics.scoring.BotClassifierJob \
                                                 --batchProcessing %s \
                                                 --tenantID %s \
                                                 --dbServer {{ var.value.zkPhoenix }} \
                                                 --parallelism %s""" % (sparkScript, batchProcessing, tenantID, parallelism),
                                  env=interset_env,
                                  dag=dag)

    # Purge bots
    # $DIR/sql.sh --action cleanbots --tenantID ${tenantID} --dbServer $zkPhoenix
    purge_bots = BashOperator(task_id="purge_bots",
                              bash_command="""%s \
                                              --action cleanbots \
                                              --tenantID %s \
                                              --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, tenantID),
                              dag=dag)

    # Create hourly counts and aggregates
    # $DIR/spark.sh com.interset.analytics.aggregation.AggregateJob --batchProcessing ${batchProcessing} --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism
    aggregate_job = BashOperator(task_id="aggregate_job",
                                 bash_command="""%s com.interset.analytics.aggregation.AggregateJob \
                                                 --batchProcessing %s \
                                                 --tenantID %s \
                                                 --dbServer {{ var.value.zkPhoenix }} \
                                                 --parallelism %s""" % (sparkScript, batchProcessing, tenantID, parallelism),
                                 env=interset_env,
                                 dag=dag)

    # Mark down timestamps for iterative aggregate
    # $DIR/sql.sh --action markaggregatewindow --tenantID ${tenantID} --dbServer $zkPhoenix
    mark_aggregate_window = BashOperator(task_id="mark_aggregate_window",
                                         bash_command="""%s \
                                                        --action markaggregatewindow \
                                                        --tenantID %s \
                                                        --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, tenantID),
                                         env=interset_env,
                                         dag=dag)

    # Mark end of aggregate
    # $DIR/sql.sh --action meta --label "AGGREGATE_END" --batchProcessing ${batchProcessing} --tenantID ${tenantID} --dbServer $zkPhoenix
    mark_end = BashOperator(task_id="mark_end",
                            bash_command="""%s \
                                            --action meta \
                                            --label \"AGGREGATE_END\" \
                                            --batchProcessing %s \
                                            --tenantID %s \
                                            --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, batchProcessing, tenantID),
                            env=interset_env,
                            dag=dag)

    mark_start >> mark_aggregate_start >> initialize_weights >> entity_remapping >> bot_classifier >> purge_bots >> aggregate_job >> mark_aggregate_window >> mark_end
    return dag
