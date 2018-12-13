from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from utils import get_es_params, get_interset_env, get_xcom, ESBashOperator

def scoring_dag(parent_dag_name, child_dag_name, default_args, schedule_interval):
    dag = DAG("%s.%s" % (parent_dag_name, child_dag_name), default_args=default_args, schedule_interval=schedule_interval)

    interset_env = get_interset_env()
    sqlScript = get_xcom("sqlScript")
    sparkScript = get_xcom("sparkScript")
    tenantID = get_xcom("tenantID")
    batchProcessing = get_xcom("batchProcessing")
    parallelism = get_xcom("parallelism")
    esIndexName = get_xcom("esIndexName")
    es_params = get_es_params()

    # Start scoring, mark metadata
    # $DIR/sql.sh --action meta --label "SCORING_START" --batchProcessing ${batchProcessing} --tenantID ${tenantID} --dbServer $zkPhoenix
    mark_start = BashOperator(task_id="mark_start",
                              bash_command="""%s \
                                              --action meta \
                                              --label \"SCORING_START\" \
                                              --batchProcessing %s \
                                              --tenantID %s \
                                              --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, batchProcessing, tenantID),
                              dag=dag)

    # $DIR/spark.sh com.interset.analytics.scoring.DirectAnomaliesJob --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism --batchProcessing $batchProcessing
    direct_anamolies = BashOperator(task_id="direct_anamolies",
                                    bash_command="""%s com.interset.analytics.scoring.DirectAnomaliesJob \
                                                    --tenantID %s \
                                                    --dbServer {{ var.value.zkPhoenix }} \
                                                    --parallelism %s \
                                                    --batchProcessing %s""" % (sparkScript, tenantID, parallelism, batchProcessing),
                                    env=interset_env,
                                    dag=dag)

    # First analytics SQL tasks
    # $DIR/sql.sh --action analytics1 --tenantID ${tenantID} --dbServer $zkPhoenix
    first_analytics_sql_tasks = BashOperator(task_id="first_analytics_sql_tasks",
                                             bash_command="""%s \
                                                             --action analytics1 \
                                                             --tenantID %s \
                                                             --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, tenantID),
                                             env=interset_env,
                                             dag=dag)

    # updating anomaly_stats table
    # $DIR/spark.sh com.interset.analytics.scoring.AnomalyStatsJob --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism
    anamoly_stats = BashOperator(task_id="anamoly_stats",
                                 bash_command="""%s com.interset.analytics.scoring.AnomalyStatsJob \
                                                 --tenantID %s \
                                                 --dbServer {{ var.value.zkPhoenix }} \
                                                 --parallelism %s""" % (sparkScript, tenantID, parallelism),
                                 env=interset_env,
                                 dag=dag)

    # Spark scoring
    # $DIR/spark.sh com.interset.analytics.scoring.WorkingDaysJob --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism --batchProcessing $batchProcessing
    cross_working_days = BashOperator(task_id="cross_working_days",
                                      bash_command="""%s com.interset.analytics.scoring.WorkingDaysJob \
                                                      --tenantID %s \
                                                      --dbServer {{ var.value.zkPhoenix }} \
                                                      --parallelism %s \
                                                      --batchProcessing %s""" % (sparkScript, tenantID, parallelism, batchProcessing),
                                      env=interset_env,
                                      dag=dag)

    # $DIR/spark.sh com.interset.analytics.scoring.WorkingHoursJob --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism --batchProcessing $batchProcessing
    cross_working_hours = BashOperator(task_id="cross_working_hours",
                                       bash_command="""%s com.interset.analytics.scoring.WorkingHoursJob \
                                                       --tenantID %s \
                                                       --dbServer {{ var.value.zkPhoenix }} \
                                                       --parallelism %s \
                                                       --batchProcessing %s""" % (sparkScript, tenantID, parallelism, batchProcessing),
                                       env=interset_env,
                                       dag=dag)

    # $DIR/spark.sh com.interset.analytics.scoring.GenerateAnomaliesJob --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism --batchProcessing $batchProcessing
    generate_anomalies = BashOperator(task_id="generate_anomalies",
                                      bash_command="""%s com.interset.analytics.scoring.GenerateAnomaliesJob \
                                                      --tenantID %s \
                                                      --dbServer {{ var.value.zkPhoenix }} \
                                                      --parallelism %s \
                                                      --batchProcessing %s""" % (sparkScript, tenantID, parallelism, batchProcessing),
                                      env=interset_env,
                                      dag=dag)

    # Netflow Human-Machine Classifier scoring
    # $DIR/spark.sh com.interset.analytics.scoring.HumanMachineClassifier --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism --batchProcessing $batchProcessing
    human_machine_classifier = BashOperator(task_id="human_machine_classifier",
                                            bash_command="""%s com.interset.analytics.scoring.HumanMachineClassifier \
                                                            --tenantID %s \
                                                            --dbServer {{ var.value.zkPhoenix }} \
                                                            --parallelism %s \
                                                            --batchProcessing %s""" % (sparkScript, tenantID, parallelism, batchProcessing),
                                            env=interset_env,
                                            dag=dag)

    # Followup anomaly aggregation
    # $DIR/sql.sh --action analytics2 --tenantID ${tenantID} --dbServer $zkPhoenix 
    anamoly_aggregation = BashOperator(task_id="anamoly_aggregation",
                                       bash_command="""%s \
                                                       --action analytics2 \
                                                       --tenantID %s \
                                                       --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, tenantID),
                                       env=interset_env,
                                       dag=dag)

    # Calculate and normalize risks
    # $DIR/sql.sh --action normalizerisk --tenantID ${tenantID} --dbServer $zkPhoenix
    #normalize_risk = BashOperator(task_id="normalize_risk",
                                  #bash_command="""%s \
                                                  #--action normalizerisk \
                                                  #--tenantID %s \
                                                  #--dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, tenantID),
                                  #env=interset_env,
                                  #dag=dag)

    # Calculate and normalize risks
    # $DIR/spark.sh com.interset.analytics.scoring.RiskNormalizerJob --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism --batchProcessing $batchProcessing --namespace $namespace
    risk_normalizer_job = BashOperator(task_id="risk_normalizer_job",
                                            bash_command="""%s com.interset.analytics.scoring.RiskNormalizerJob \
                                                            --tenantID %s \
                                                            --dbServer {{ var.value.zkPhoenix }} \
                                                            --parallelism %s \
                                                            --batchProcessing %s""" % (sparkScript, tenantID, parallelism, batchProcessing),
                                            env=interset_env,
                                            dag=dag)


    # Augment endpoint anomalies with entities from ElasticSearch
    # $DIR/spark.sh com.interset.analytics.scoring.AnomalousEntitiesJob --tenantID ${tenantID} --dbServer ${zkPhoenix} --parallelism $parallelism --batchProcessing $batchProcessing $ES_PARAMS
    anamolous_entities = ESBashOperator(task_id="anamolous_entities",
                                      bash_command="""%s com.interset.analytics.scoring.AnomalousEntitiesJob \
                                                      --tenantID %s \
                                                      --dbServer {{ var.value.zkPhoenix }} \
                                                      --parallelism %s \
                                                      --batchProcessing %s \
                                                      %s""" % (sparkScript, tenantID, parallelism, batchProcessing, es_params),
                                      env=interset_env,
                                      dag=dag)

    # Generate stories / persistent anomalies
    # $DIR/spark.sh com.interset.analytics.scoring.AggregateStoryJob --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism --batchProcessing $batchProcessing
    aggregate_story = BashOperator(task_id="aggregate_story",
                                   bash_command="""%s com.interset.analytics.scoring.AggregateStoryJob \
                                                   --tenantID %s \
                                                   --dbServer {{ var.value.zkPhoenix }} \
                                                   --parallelism %s \
                                                   --batchProcessing %s""" % (sparkScript, tenantID, parallelism, batchProcessing),
                                   env=interset_env,
                                   dag=dag)


    # Tune Story parameters
    # $DIR/spark.sh com.interset.analytics.tuning.StoryTuningJob --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism --batchProcessing $batchProcessing --namespace $namespace
    story_tuning = BashOperator(task_id="story_tuning",
                                   bash_command="""%s com.interset.analytics.tuning.StoryTuningJob \
                                                   --tenantID %s \
                                                   --dbServer {{ var.value.zkPhoenix }} \
                                                   --parallelism %s \
                                                   --batchProcessing %s""" % (sparkScript, tenantID, parallelism, batchProcessing),
                                   env=interset_env,
                                   dag=dag)


    # Calculate entity risk scores
    # $DIR/spark.sh com.interset.analytics.scoring.EntityVulnerabilityJob --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism --batchProcessing $batchProcessing --esClusterName ${esClusterName} --esHost ${esHost} - -esIndexName ${esIndexName}
    entity_vulnerability = BashOperator(task_id="entity_vulnerability",
                                   bash_command="""%s com.interset.analytics.scoring.EntityVulnerabilityJob \
                                                   --tenantID %s \
                                                   --dbServer {{ var.value.zkPhoenix }} \
                                                   --parallelism %s \
                                                   --batchProcessing %s \
                                                   --esClusterName {{ var.value.esClusterName }} \
                                                   --esHost {{ var.value.esHost }} \
                                                   --esIndexName %s""" % (sparkScript, tenantID, parallelism, batchProcessing, esIndexName),
                                   env=interset_env,
                                   dag=dag)

    # Tune PARETO SCALE parameter
    # $DIR/spark.sh com.interset.analytics.tuning.EntityTuningJob --tenantID ${tenantID} --dbServer $zkPhoenix --parallelism $parallelism --batchProcessing $batchProcessing --namespace $namespace
    entity_tuning = BashOperator(task_id="entity_tuning",
                                   bash_command="""%s com.interset.analytics.tuning.EntityTuningJob \
                                                   --tenantID %s \
                                                   --dbServer {{ var.value.zkPhoenix }} \
                                                   --parallelism %s \
                                                   --batchProcessing %s""" % (sparkScript, tenantID, parallelism, batchProcessing),
                                   env=interset_env,
                                   dag=dag)


    # Mark down timestamps for iterative scoring
    # $DIR/sql.sh --action markscoringwindow --tenantID ${tenantID} --dbServer $zkPhoenix
    mark_scoring_window = BashOperator(task_id="mark_scoring_window",
                                       bash_command="""%s \
                                                       --action markscoringwindow \
                                                       --tenantID %s \
                                                       --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, tenantID),
                                       env=interset_env,
                                       dag=dag)

    # End scoring, mark metadata
    # $DIR/sql.sh --action meta --label "SCORING_END" --batchProcessing ${batchProcessing} --tenantID ${tenantID} --dbServer $zkPhoenix
    mark_end = BashOperator(task_id="mark_end",
                            bash_command="""%s \
                                            --action meta \
                                            --label \"SCORING_END\" \
                                            --batchProcessing %s \
                                            --tenantID %s \
                                            --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, batchProcessing, tenantID),
                            env=interset_env,
                            dag=dag)

    mark_start >> direct_anamolies >> first_analytics_sql_tasks >> anamoly_stats >> \
    cross_working_days >> cross_working_hours >> generate_anomalies >> \
    human_machine_classifier >> anamoly_aggregation >> risk_normalizer_job >> anamolous_entities >> \
    aggregate_story >> story_tuning >> entity_vulnerability >> entity_tuning >> mark_scoring_window >> mark_end
    return dag
