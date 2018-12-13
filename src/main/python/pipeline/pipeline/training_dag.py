from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from utils import get_interset_env, get_xcom

def training_dag(parent_dag_name, child_dag_name, default_args, schedule_interval):
    dag = DAG("%s.%s" % (parent_dag_name, child_dag_name), default_args=default_args, schedule_interval=schedule_interval)

    interset_env = get_interset_env()
    sqlScript = get_xcom("sqlScript")
    sparkScript = get_xcom("sparkScript")
    tenantID = get_xcom("tenantID")
    batchProcessing = get_xcom("batchProcessing")
    parallelism = get_xcom("parallelism")
    
    # Mark training start in metadata
    # $DIR/sql.sh \ --action meta --label "TRAINING_START" --batchProcessing ${batchProcessing} --tenantID ${tenantID} --dbServer $zkPhoenix
    mark_start = BashOperator(task_id="mark_start",
                              bash_command="""%s \
                                              --action meta \
                                              --label \"TRAINING_START\" \
                                              --batchProcessing %s \
                                              --tenantID %s \
                                              --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, batchProcessing, tenantID),
                              env=interset_env,
                              dag=dag)

    # First spark training jobs
    # $DIR/sql.sh \ --action aggregate1 --tenantID ${tenantID} --dbServer $zkPhoenix
    first_spark_training_jobs = BashOperator(task_id="first_spark_training_jobs",
                                             bash_command="""%s \
                                                             --action aggregate1 \
                                                             --tenantID %s \
                                                             --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, tenantID),
                                             env=interset_env,
                                             dag=dag)

    # Hourly stats job
    # $DIR/spark.sh com.interset.analytics.training.EntityRelationStatsJob --batchProcessing ${batchProcessing} --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism
    hourly_stats_job = BashOperator(task_id="hourly_stats_job",
                                    bash_command="""%s com.interset.analytics.training.EntityRelationStatsJob \
                                                    --batchProcessing %s \
                                                    --tenantID %s \
                                                    --dbServer {{ var.value.zkPhoenix }} \
                                                    --parallelism %s""" % (sparkScript, batchProcessing, tenantID, parallelism),
                                    env=interset_env,
                                    dag=dag)

    # Clustering
    # $DIR/spark.sh com.interset.analytics.training.ClusteringJob --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism
    clustering = BashOperator(task_id="clustering",
                              bash_command="""%s com.interset.analytics.training.ClusteringJob \
                                              --tenantID %s \
                                              --dbServer {{ var.value.zkPhoenix }} \
                                              --parallelism %s""" % (sparkScript, tenantID, parallelism),
                              env=interset_env,
                              dag=dag)

    # Run aggregation of stats
    # $DIR/spark.sh com.interset.analytics.aggregation.AggregateStatsJob --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism
    aggregate_stats = BashOperator(task_id="aggregate_stats",
                                   bash_command="""%s com.interset.analytics.aggregation.AggregateStatsJob \
                                                  --tenantID %s \
                                                  --dbServer {{ var.value.zkPhoenix }} \
                                                  --parallelism %s""" % (sparkScript, tenantID, parallelism),
                                   env=interset_env,
                                   dag=dag)

    # Cross data-source working hours
    # $DIR/spark.sh com.interset.analytics.training.WorkingDaysJob --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism
    cross_working_days = BashOperator(task_id="cross_working_days",
                                      bash_command="""%s com.interset.analytics.training.WorkingDaysJob \
                                                     --tenantID %s \
                                                     --dbServer {{ var.value.zkPhoenix }} \
                                                     --parallelism %s""" % (sparkScript, tenantID, parallelism),
                                      env=interset_env,
                                      dag=dag)

    # Cross data-source working days 
    # $DIR/spark.sh com.interset.analytics.training.WorkingHoursJob --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism
    cross_working_hours = BashOperator(task_id="cross_working_hours",
                                       bash_command="""%s com.interset.analytics.training.WorkingHoursJob \
                                                       --tenantID %s \
                                                       --dbServer {{ var.value.zkPhoenix }} \
                                                       --parallelism %s""" % (sparkScript, tenantID, parallelism),
                                       env=interset_env,
                                       dag=dag)

    # Netflow Human-Machine Classifier
    # $DIR/spark.sh com.interset.analytics.training.HumanMachineClassifier --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism
    human_machine_classifier = BashOperator(task_id="human_machine_classifier",
                                            bash_command="""%s com.interset.analytics.training.HumanMachineClassifier \
                                                            --tenantID %s \
                                                            --dbServer {{ var.value.zkPhoenix }} \
                                                            --parallelism %s""" % (sparkScript, tenantID, parallelism),
                                            env=interset_env,
                                            dag=dag)

    # Mooched projects
    # $DIR/spark.sh com.interset.analytics.training.UpdateTrainingPeriodJob --batchProcessing ${batchProcessing} --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism
    update_training_period = BashOperator(task_id="update_training_period",
                                          bash_command="""%s com.interset.analytics.training.UpdateTrainingPeriodJob \
                                                          --batchProcessing %s \
                                                          --tenantID %s \
                                                          --dbServer {{ var.value.zkPhoenix }} \
                                                          --parallelism %s""" % (sparkScript, batchProcessing, tenantID, parallelism),
                                          env=interset_env,
                                          dag=dag)

    # Mark end of training
    # $DIR/{{ var.value.sqlScript }} \ --action meta --label "TRAINING_END" --batchProcessing ${batchProcessing} --tenantID ${tenantID} --dbServer $zkPhoenix
    mark_end = BashOperator(task_id="mark_end",
                            bash_command="""%s \
                                            --action meta \
                                            --label \"TRAINING_END\" \
                                            --batchProcessing %s \
                                            --tenantID %s \
                                            --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, batchProcessing, tenantID),
                            env=interset_env,
                            dag=dag)
                        
    mark_start >> first_spark_training_jobs >> hourly_stats_job >> clustering >> aggregate_stats >> cross_working_days >> cross_working_hours >> human_machine_classifier >> update_training_period >> mark_end
    return dag
