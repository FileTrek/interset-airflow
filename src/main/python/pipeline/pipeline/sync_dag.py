from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from utils import get_es_params, get_interset_env, get_xcom, ESBashOperator

def sync_dag(parent_dag_name, child_dag_name, default_args, schedule_interval):
    dag = DAG("%s.%s" % (parent_dag_name, child_dag_name), default_args=default_args, schedule_interval=schedule_interval)

    interset_env = get_interset_env()
    sqlScript = get_xcom("sqlScript")
    sparkScript = get_xcom("sparkScript")
    tenantID = get_xcom("tenantID")
    batchModeSync = get_xcom("batchModeSync")
    parallelism = get_xcom("parallelism")
    es_params = get_es_params()

    # Start of sync, mark down metadata
    # $DIR/sql.sh --action meta --label "SYNC_START" --tenantID ${tenantID} --dbServer $zkPhoenix --batchProcessing $batchModeSync
    mark_start = ESBashOperator(task_id="mark_start",
                              bash_command="""%s \
                                              --action meta \
                                              --label \"SYNC_START\" \
                                              --tenantID %s \
                                              --dbServer {{ var.value.zkPhoenix }} \
                                              --batchProcessing %s""" % (sqlScript, tenantID, batchModeSync),
                              env=interset_env,
                              dag=dag)

    # $DIR/spark.sh com.interset.analytics.indexing.EntityRelationStatsIndexerJob $ES_PARAMS
    entity_relation_stats = ESBashOperator(task_id="entity_relation_stats",
                                         bash_command="""%s com.interset.analytics.indexing.EntityRelationStatsIndexerJob \
                                              --tenantID %s \
                                              --dbServer {{ var.value.zkPhoenix }} \
                                              --parallelism %s \
                                              %s""" % (sparkScript, tenantID, parallelism, es_params),
                                         env=interset_env,
                                         dag=dag)

    # $DIR/spark.sh com.interset.analytics.indexing.EntityStatsIndexerJob $ES_PARAMS
    entity_stats = ESBashOperator(task_id="entity_stats",
                                bash_command="""%s  com.interset.analytics.indexing.EntityStatsIndexerJob \
                                              --tenantID %s \
                                              --dbServer {{ var.value.zkPhoenix }} \
                                              --parallelism %s \
                                              %s""" % (sparkScript, tenantID, parallelism, es_params),
                                env=interset_env,
                                dag=dag)

    # $DIR/spark.sh com.interset.analytics.indexing.AnomaliesIndexerJob $ES_PARAMS --batchProcessing $batchModeSync
    anamolies = ESBashOperator(task_id="anamolies",
                             bash_command="""%s  com.interset.analytics.indexing.AnomaliesIndexerJob \
                                             --tenantID %s \
                                             --dbServer {{ var.value.zkPhoenix }} \
                                             --parallelism %s \                                              
                                             %s \
                                             --batchProcessing %s""" % (sparkScript, tenantID, parallelism, es_params, batchModeSync),
                             env=interset_env,
                             dag=dag)

    # $DIR/spark.sh com.interset.analytics.indexing.WorkingHoursIndexerJob $ES_PARAMS
    working_hours = ESBashOperator(task_id="working_hours",
                                 bash_command="""%s com.interset.analytics.indexing.WorkingHoursIndexerJob \
                                              --tenantID %s \
                                              --dbServer {{ var.value.zkPhoenix }} \
                                              --parallelism %s \
                                              %s""" % (sparkScript, tenantID, parallelism, es_params),
                                 env=interset_env,
                                 dag=dag)

    # $DIR/spark.sh com.interset.analytics.indexing.RiskyEntitiesIndexerJob $ES_PARAMS --batchProcessing $batchModeSync
    risky_entities = ESBashOperator(task_id="risky_entities",
                                  bash_command="""%s com.interset.analytics.indexing.RiskyEntitiesIndexerJob \ \
                                                  --tenantID %s \
                                                  --dbServer {{ var.value.zkPhoenix }} \
                                                  --parallelism %s \
                                                  %s \
                                                  --batchProcessing %s""" % (sparkScript, tenantID, parallelism, es_params, batchModeSync),
                                  env=interset_env,
                                  dag=dag)

    # $DIR/spark.sh com.interset.analytics.indexing.ReportingTagsIndexerJob $ES_PARAMS --batchProcessing $batchProcessing --esQueryTimeout $esQueryTimeout
    reporting_tags = ESBashOperator(task_id="reporting_tags",
                                  bash_command="""%s com.interset.analytics.indexing.ReportingTagsIndexerJob \ \
                                                  --tenantID %s \
                                                  --dbServer {{ var.value.zkPhoenix }} \
                                                  --parallelism %s \
                                                  %s \
                                                  --batchProcessing %s""" % (sparkScript, tenantID, parallelism, es_params, batchModeSync),
                                  env=interset_env,
                                  dag=dag)


    # $DIR/spark.sh com.interset.analytics.indexing.RiskScoresIndexerJob $ES_PARAMS --batchProcessing $batchModeSync
    risk_scores = ESBashOperator(task_id="risk_scores",
                               bash_command="""%s com.interset.analytics.indexing.RiskScoresIndexerJob \ \
                                              --tenantID %s \
                                              --dbServer {{ var.value.zkPhoenix }} \
                                              --parallelism %s \
                                               %s \
                                               --batchProcessing %s""" % (sparkScript, tenantID, parallelism, es_params, batchModeSync),
                               env=interset_env,
                               dag=dag)

    # $DIR/spark.sh com.interset.analytics.indexing.EntityRelationCountsIndexerJob $ES_PARAMS --batchProcessing $batchModeSync
    entity_relation_counts = ESBashOperator(task_id="entity_relation_counts",
                                          bash_command="""%s com.interset.analytics.indexing.EntityRelationCountsIndexerJob \ \
                                                          --tenantID %s \
                                                          --dbServer {{ var.value.zkPhoenix }} \
                                                          --parallelism %s \
                                                          %s \
                                                          --batchProcessing %s""" % (sparkScript, tenantID, parallelism, es_params, batchModeSync),
                                          env=interset_env,
                                          dag=dag)

    # $DIR/spark.sh com.interset.analytics.indexing.RelationTagsIndexerJob $ES_PARAMS
    relation_tags = ESBashOperator(task_id="relation_tags",
                                 bash_command="""%s com.interset.analytics.indexing.RelationTagsIndexerJob  \
                                                 --tenantID %s \
                                                 --dbServer {{ var.value.zkPhoenix }} \
                                                 --parallelism %s \
                                                 %s""" % (sparkScript, tenantID, parallelism, es_params),
                                 env=interset_env,
                                 dag=dag)

    # $DIR/spark.sh com.interset.analytics.indexing.IndexingAliasAndCleanupJob $ES_PARAMS --batchProcessing $batchModeSync
    indexing_alias_and_cleanup = ESBashOperator(task_id="indexing_alias_and_cleanup",
                                              bash_command="""%s com.interset.analytics.indexing.IndexingAliasAndCleanupJob \ \
                                                              --tenantID %s \
                                                              --dbServer {{ var.value.zkPhoenix }} \
                                                              --parallelism %s \                                              
                                                              %s \
                                                              --batchProcessing %s""" % (sparkScript, tenantID, parallelism, es_params, batchModeSync),
                                              env=interset_env,
                                              dag=dag)

    # Mark down timestamps for iterative sync
    # $DIR/sql.sh --action marksyncwindow --tenantID ${tenantID} --dbServer $zkPhoenix
    mark_sync_window = BashOperator(task_id="mark_sync_window",
                                    bash_command="""%s \
                                                    --action marksyncwindow \
                                                    --tenantID %s \
                                                   --dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, tenantID),
                                    env=interset_env,
                                    dag=dag)

    # End of sync, mark down metadata
    # $DIR/sql.sh --action meta --label "SYNC_END" --tenantID ${tenantID} --dbServer $zkPhoenix --batchProcessing $batchModeSync
    mark_end = ESBashOperator(task_id="mark_end",
                            bash_command="""%s \
                                            --action meta \
                                            --label \"SYNC_END\" \
                                            --tenantID %s \
                                            --dbServer {{ var.value.zkPhoenix }} \
                                            --batchProcessing %s""" % (sqlScript, tenantID, batchModeSync),
                            env=interset_env,
                            dag=dag)

    # Very end of analytics, mark down metadata
    # $DIR/sql.sh --action meta --label "ANALYTICS_END" --tenantID ${tenantID} --dbServer $zkPhoenix
    #mark_analytics_end = BashOperator(task_id="mark_analytics_end",
                                      #bash_command="""%s \
                                                      #--action meta \
                                                      #--label \"ANALYTICS_END\" \
                                                      #--tenantID %s \
                                                      #--dbServer {{ var.value.zkPhoenix }}""" % (sqlScript, tenantID),
                                      #env=interset_env,
                                      #dag=dag)

    # $DIR/spark.sh com.interset.analytics.indexing.AnalyticsMetadataIndexerJob $ES_PARAMS --batchProcessing $batchProcessing --esQueryTimeout $esQueryTimeout
    analyticsa_metadata = ESBashOperator(task_id="analyticsa_metadata",
                                              bash_command="""%s com.interset.analytics.indexing.AnalyticsMetadataIndexerJob \ \
                                                              --tenantID %s \
                                                              --dbServer {{ var.value.zkPhoenix }} \
                                                              --parallelism %s \                                              
                                                              %s \
                                                              --batchProcessing %s""" % (sparkScript, tenantID, parallelism, es_params, batchModeSync),
                                              env=interset_env,
                                              dag=dag)

    # mark_start >> entity_relation_stats >> entity_stats >> anamolies >> working_hours >> risky_entities >> reporting_tags >> risk_scores >> entity_relation_counts >> relation_tags >> indexing_alias_and_cleanup >> mark_sync_window >> mark_end >> mark_analytics_end
    mark_start >> entity_relation_stats >> entity_stats >> anamolies >> working_hours >> risky_entities >> reporting_tags >> risk_scores >> entity_relation_counts >> relation_tags >> indexing_alias_and_cleanup >> mark_sync_window >> mark_end >> analyticsa_metadata
    return dag
