# Apache Airflow Analytic Pipeline Installation and Deployment Guide
## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Analytic Pipeline Deployment](#analytic-pipeline-deployment)
- [Quick User Guide](#quick-user-guide)

## Requirements
* CentOS 7.3
* Python 2.7 w/ `virtualenv`
* `gcc`
* `python-devel`
* `mysql-server`
* `mysql-devel`
* `git`


### Installation
The following steps will fire up Airflow with a basic login.  You'll want to install Airflow on the Interset node that typically runs the analytics.

1. Create an airflow user, switch to the new user, then create a virtual environment that we will use for Airflow.  We'll use the `spark` user here since this is the user that runs the Interset analytics pipeline.  The virtualenv will need to be in a location accessible to all users.  We will use the future `AIRFLOW_HOME/venv` directory for the virtualenv.

```bash
sudo mkdir -p /usr/lib/airflow /var/log/airflow /usr/lib/airflow/dags /usr/lib/airflow/plugins /run/airflow
sudo chown spark:hadoop /usr/lib/airflow /var/log/airflow /usr/lib/airflow/dags /usr/lib/airflow/plugins /run/airflow
sudo su - spark
virtualenv /usr/lib/airflow/venv
```

2. As a sudoer, create the various systemd files.  These scripts were taken from [Airflow's GitHub respository](https://github.com/apache/incubator-airflow/tree/master/scripts/systemd) and modified to work in this environment.

**/etc/sysconfig/airflow**
```bash
AIRFLOW_HOME=/usr/lib/airflow

# required setting, 0 sets it to unlimited. Scheduler will get restart after every X runs
SCHEDULER_RUNS=5
```

**/usr/lib/systemd/system/airflow-webserver.service**
```bash
[Unit]
Description=Airflow webserver daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
EnvironmentFile=/etc/sysconfig/airflow
User=spark
Group=hadoop
Type=simple
ExecStart=/bin/bash -c 'AIRFLOW_HOME=${AIRFLOW_HOME} PATH=/usr/lib/airflow/venv/bin:$PATH airflow webserver --pid /run/airflow/webserver.pid'
Restart=on-failure
RestartSec=5s
PrivateTmp=true
[Install]
WantedBy=multi-user.target
```

**/usr/lib/systemd/system/airflow-scheduler.service**
```bash
[Unit]
Description=Airflow scheduler daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
EnvironmentFile=/etc/sysconfig/airflow
User=spark
Group=hadoop
Type=simple
ExecStart=/bin/bash -c 'AIRFLOW_HOME=${AIRFLOW_HOME} PATH=/usr/lib/airflow/venv/bin:$PATH airflow scheduler -n ${SCHEDULER_RUNS}'
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

**/usr/lib/systemd/system/airflow-worker.service**
```bash
[Unit]
Description=Airflow celery worker daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
EnvironmentFile=/etc/sysconfig/airflow
User=spark
Group=hadoop
Type=simple
ExecStart=/bin/bash -c 'AIRFLOW_HOME=${AIRFLOW_HOME} PATH=/usr/lib/airflow/venv/bin:$PATH airflow worker'
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
```

3. Change to the `spark` user and add the following to the user's `.bashrc` file to setup the environment.

```bash
# .bashrc
export AIRFLOW_HOME=/usr/lib/airflow
export PYTHON_HOME=$AIRFLOW_HOME/venv
export PATH=$PYTHON_HOME/bin:$PATH
```

4. Source the file and make sure python is using the correct python executable 
```bash
$ source ~/.bashrc
$ which python
/usr/lib/airflow/venv/bin/python
```

5. Install Apache Airflow as the `spark` user.  Note that this installs the base Airflow package and a few subpackages we will be using.  Airflow contains many other subpackages depending on the types of features/operators you plan to use.  You can find the full list at http://airflow.apache.org/installation.html.  We are going to install the latest release at the time of this writing, 1.8.1, with a commit on top to fix a bug related to the packaged DAGs feature.

```bash
# Check out this repository in airflow's home directory
git clone https://<github.username>@github.com/MinerKasch/interset-airflow.git

# We will be pulling source code straight from Github instead of PyPi due to some unreleased bug fixes and features we need
git clone https://github.com/apache/incubator-airflow.git
cd incubator-airflow/
git checkout tags/1.8.1 -b 1.8.1
git cherry-pick 3b863f182d42dc1ba76c2623b9983698a682d5f7
pip install mysql-python flask_bcrypt . .[celery] .[crypto] .[password]
```

6. Install MySQL (or PostgreSQL) and create a database and user for Airflow.  This task is of scope for this document, but we will assume the database, user, and password are all `airflow`.  Note that you'll need to install, for example, the `mysql-python` module and any dependencies (such as `mysql-community-devel`) in order to build the module.

7. Create and update various configurations in `airflow.cfg` to use the new paths and SQL database.  You can find a sample `airflow.cfg` in this repository.
```bash
# Run 'airflow' as the spark user to generate the airflow.cfg file
$ sudo su - spark
$ airflow
$ vi /usr/lib/airflow/airflow.cfg
```

**/usr/lib/airflow/airflow.cfg**
```bash
[core]
base_log_folder = /var/log/airflow
executor = CeleryExecutor
sql_alchemy_conn = mysql://airflow:<YOURPASSWORDHERE>@localhost/airflow
dags_are_paused_at_creation = False
load_examples = False

[cli]
endpoint_url = http://localhost:44444 # Change to desired port, here we use 44444

[webserver]
base_url = http://localhost:44444 # Change port to 44444
web_server_port = 44444 # Change port to 44444
secret_key = <mashyourkeyboard>
authenticate = True # Change from False
auth_backend = airflow.contrib.auth.backends.password_auth # This is a new property

[celery]
# You'll likely need to update the broker_url and celery_result_backend URLs to fit your database
broker_url = sqla+mysql://airflow:airflow@localhost:3306/airflow
celery_result_backend = db+mysql://airflow:airflow@localhost:3306/airflow

[scheduler]
child_process_log_directory = /var/log/airflow/scheduler
```


8. Initialize and configure Airflow as the `spark` user.
```bash
cd $HOME
airflow initdb
```

9. Create the initial user.  See https://airflow.incubator.apache.org/security.html#web-authentication for details.

```bash
# Create and execute the below file
vi /usr/lib/airflow/create_initial_user.py
python /usr/lib/airflow/create_initial_user.py
```
```python
#!/usr/bin/env python'
# usage: python create_initial_user.py
import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
user = PasswordUser(models.User())
user.username = 'intadmin'
user.password = 'vindiesel'
session = settings.Session()
session.add(user)
session.commit()
session.close()
```

10. Initialize the database, re-run the initial user script, and start the webserver and scheduler
```bash
# As sudoer:
sudo systemctl start airflow-webserver
sudo systemctl start airflow-scheduler
sudo systemctl start airflow-worker
```

11. Point your browser to the host at port 44444 and you should see the UI.  If not, you can use `journalctl -u airflow-webserver -f` (same for `airflow-scheduler` and `airflow-worker`) to view the logs and troubleshoot.

### Analytic Pipeline Deployment

The `Makefile` in this repository contains a handful of targets for packaging and deploying the Airflow job for each tenant.  Note that this is a guideline of the steps and not necessarily something that should be used in production (based on my usage of the Interset installer, I don't see this being used directly).

1. Create the Airflow variables for Elasticsearch and ZK Phoenix.  This can be done via the `airflow` CLI program or through the Airflow UI.
```bash
airflow variables -s esHost centos7.home
airflow variables -s zkPhoenix centos7.home:2181:/hbase-unsecure
airflow variables -s esClusterName interset
```

2. Update the `conf/interset.conf` file to contain the configuration for your tenant.

3. Let's take a look at the `Makefile`.  It contains three targets, `version`, `package`, and `deploy`.

**version**

This target will modify the version of the pipeline in the `main_dag.py` file.  Versioning Airflow DAGs is important, as overwriting a DAG within Airflow may have unintended consequences.
```bash
make version TENANT=int
```

**package**

This target (which runs `version`) creates a temporary directory and copies the DAG files into a [package dag](http://airflow.apache.org/concepts.html#packaged-dags).  The `DAG_NAME` and `tenantID` values within the `main_dag.py` is changed to be named after the tenant (as defined by the `TENANT` variable that is passed into the Makefile upon execution) and current version.  It will then zip the DAG files into the packaged zip.
```bash
make package TENANT=int
```

**deploy**

This target (which runs `package` (which runs `version`)) will create the directories for the analytic in the `${AIRFLOW_HOME}/dags` directory.  It will copy the packaged DAG into this directory as well as the `conf`, `bin`, and `jars` dependencies.  The `conf` and `bin` dependencies are already checked into this repository, however the `jars` are copied from `/opt/interset/analytics/jars`.

To deploy a new tenant, execute `make deploy TENANT=int`, substituting `int` with the desired tenant ID.
```bash
make deploy TENANT=int
```
```text
# You can verify the deployment by listing the directory
$ tree /usr/lib/airflow/dags/
/usr/lib/airflow/dags/
└── pipeline_int_v0.0.1
    ├── bin
    │   ├── env.sh
    │   ├── spark.sh
    │   └── sql.sh
    ├── conf
    │   ├── interset.conf
    │   ├── logback.xml
    │   └── resources
    │       ├── log4j.properties
    │       └── log4j-spark.properties
    ├── jars
    │   ├── analytics-fat.jar
    │   └── jobs-5-jar-with-dependencies.jar
    └── pipeline_int_v0.0.1.zip
```

You've now deployed a tenant!  Check out the Airflow UI and it should be listed and begin executing the first DAG run shortly.  Again, you can use `journalctl` to check if there are any errors in the running processes, particularly the scheduler and worker.

## Quick User Guide
### Viewing DAGs
The main page in Airflow lists all of the DAGs.  We see there are three DAGs, one for each tenant (`int`, `te1`, and `te2`).  The "Recent Tasks" shows the status for each task of the latest DAG run.  The "Dag Runs" shows the entire history of the successful, running, and failed DAGs.

![All DAGs](docs/imgs/dags.png)

### Tree and Graph View
The Tree View is a great way to view the status of all historic tasks, while the Graph View is great to see a graphical representation of the latest DAG run (by default, you can change it in the drop down).

![Tree View](docs/imgs/tree_view.png)
![Graph View](docs/imgs/graph_view.png)

### Zoom into Sub Dag
Selecting on one of the squares in the Tree View or one of the nodes in the Graph View will open a window to view the logs, modify the state, or zoom into the Sub DAG (if the oeprator is a SubDagOperator, which applies to all operators but `generate_xcoms`).  Zooming into the Sub DAG will show all of the operators for that piece of the workflow.  Here, we see that the `bot_classifier` task is up for retry, meaning it recently failed and is going to be tried one more time before giving up entirely.

![Zoom into SubDAG](docs/imgs/zoom_into_subdag.png)
![Up for Retry](docs/imgs/up_for_retry.png)

### Viewing Logs
Selecting on `bot_classifier` opens up a window where we can select View Log.  Here we will see the output of the bash operator, which is the Spark client code.  We see that the application is running until the error occured, which in this case was caused by the application being killed by a user.

![View Log](docs/imgs/view_log.png)
![Job Killed](docs/imgs/job_killed.png)

For more information on Apache Airflow, see http://airflow.apache.org/.
