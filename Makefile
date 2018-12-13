VERSION=`cat VERSION`

ifndef TENANT
$(error TENANT is not set)
endif

deploy: package
	mkdir -p $(AIRFLOW_HOME)/dags/pipeline_$(TENANT)_v$(VERSION)/bin
	mkdir -p $(AIRFLOW_HOME)/dags/pipeline_$(TENANT)_v$(VERSION)/jars
	mkdir -p $(AIRFLOW_HOME)/dags/pipeline_$(TENANT)_v$(VERSION)/conf
	cp pipeline_$(TENANT)_v$(VERSION).zip $(AIRFLOW_HOME)/dags/pipeline_$(TENANT)_v$(VERSION)
	cp src/main/bash/*.sh $(AIRFLOW_HOME)/dags/pipeline_$(TENANT)_v$(VERSION)/bin
	cp /opt/interset/analytics/jars/* $(AIRFLOW_HOME)/dags/pipeline_$(TENANT)_v$(VERSION)/jars
	cp -r conf/* $(AIRFLOW_HOME)/dags/pipeline_$(TENANT)_v$(VERSION)/conf
	sed -i s/"\"tenantID\":.*"/"\"tenantID\": \"$(TENANT)\","/ $(AIRFLOW_HOME)/dags/pipeline_$(TENANT)_v$(VERSION)/conf/interset.conf
ifdef yarnQueue
	sed -i s/"^QUEUE=.*"/"QUEUE=$(yarnQueue)"/ $(AIRFLOW_HOME)/dags/pipeline_$(TENANT)_v$(VERSION)/bin/spark.sh
endif


package: version
ifndef airflowSchedule
	read -p "Enter a schedule (i.e. @daily, or 0 0 * * *): " airflowSchedule
endif
	mkdir pipeline_$(TENANT)_v$(VERSION)
	cd pipeline_$(TENANT)_v$(VERSION)
	rsync -a --exclude='*.pyc' src/main/python/pipeline/* pipeline_$(TENANT)_v$(VERSION)
	sed -i s/"DAG_NAME =.*"/"DAG_NAME = \"pipeline_$(TENANT)_v$(VERSION)\""/ pipeline_$(TENANT)_v$(VERSION)/main_dag.py
	sed -i s/"^SCHEDULE_INTERVAL =.*"/"SCHEDULE_INTERVAL = \"$(airflowSchedule)\""/ pipeline_$(TENANT)_v$(VERSION)/main_dag.py
	rm -f pipeline_$(TENANT)_v$(VERSION).zip
	cd pipeline_$(TENANT)_v$(VERSION) && zip -rq ../pipeline_$(TENANT)_v`cat ../VERSION`.zip *
	rm -rf pipeline_$(TENANT)_v$(VERSION)


version:
	sed -i  s/"^VERSION.*$$"/"VERSION = \"`head -n1 VERSION`\""/ src/main/python/pipeline/main_dag.py
