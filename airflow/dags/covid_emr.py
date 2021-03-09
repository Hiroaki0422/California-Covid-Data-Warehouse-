from datetime import datetime, timedelta
import os
import operator
import boto3
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from operators.launch_emr import EMRLaunchClusterOperator
from operators.emr_add_jobs import EMRAddJobsOperator
from operators.terminate_emr import EMRTerminateOperator

# Argument of this dag
default_args = {
    'owner': 'hiroo',
    'start_date': datetime(2021,1,3),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'catchup': False,
    'email':['kifa0422@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False
}

# dag definition
dag = DAG('emr_spark_job',
          default_args=default_args,
          description='submit a spark script on AWS EMR cluster, launch run, terminate automatically',
          schedule_interval= '@daily'
        )

# parameters for EMR Cluster. Hadoop, Spark and Hive must be installed to run the script
EMR_LUNCH = {
    'Name':'boto3 test cluster',
    'LogUri':'s3://hiro-covid-datalake/spark/log/',
    'ReleaseLabel':"emr-5.29.0",
    'Instances':{'MasterInstanceType': 'm4.xlarge',
        'SlaveInstanceType': 'm4.xlarge',
        'InstanceCount': 4,
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False},
    'Applications':[{"Name": "Hadoop"}, {"Name": "Spark"}, {"Name" : "Hive"}],
    'VisibleToAllUsers':True,
    'JobFlowRole':'EMR_EC2_DefaultRole',
    'ServiceRole':'EMR_DefaultRole',
    'Steps' : [{
        'Name': 'Setup Debugging',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['state-pusher-script']
        }
    }]
}

# command you need to run once the cluster launches 
EMR_STEPS = [{'Name': 'Run Spark', 
              'ActionOnFailure': 'TERMINATE_CLUSTER', 
              'HadoopJarStep': {'Jar': 'command-runner.jar', 
                                'Args': ['spark-submit', '--deploy-mode', 'client', 's3://hiro-covid-datalake/spark/spark_test_script.py']}
            }]

# helper function to upload the script to S3 bucket
def upload_to_S3(filename, key, bucket_name):
	hook = S3Hook('my_S3_conn')
	hook.load_file(filename, key, bucket_name, replace=True)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# upload the spark script to S3
upload_spark_script_tos3 = PythonOperator(
		task_id='upload_spark_script_tos3',
		dag=dag,
		python_callable=upload_to_S3,
		op_kwargs = {
			'filename':'/Users/hiroakioshima/Desktop/airflow_covid/airflow/dags/spark_scripts/partition_by_50states.py',
			'bucket_name':'hiro-covid-datalake',
			'key':'spark/partition_50states.py'
		}
	)

# upload the data to S3 temporary bucket
nationwide_tos3 = PythonOperator(
		task_id='Upload_nationwide_tos3',
		dag=dag,
		python_callable=upload_to_S3,
		op_kwargs={
			'filename':'/Users/hiroakioshima/Desktop/Hiro_Covid_Project/tables' + '/nationwide_cases.csv',
			'key':'tables/nationwide_cases.csv',
			'bucket_name':'hiro-covid-datalake'
		}
	)

# Launch a EMR cluster with given parameters, return a cluster ID
emr_launch = EMRLaunchClusterOperator(
		task_id='Launch_EMR_cluster',
		dag=dag,
		emr_credentials='emr_user_credentials',
		JOBFLOW_OVERWRITE=EMR_LUNCH
	)

# Add jobs to the cluster laundhed above
emr_add_jobs = EMRAddJobsOperator(
		task_id='Add_jobs_to_cluster',
		dag=dag,
		provide_context=True,
		emr_credentials='emr_user_credentials',
		JOBSTEPS=EMR_STEPS,
		emr_id="{{ ti.xcom_pull('Launch_EMR_cluster') }}"
	)

# Terminate the cluster once the job is done
emr_terminate = EMRTerminateOperator(
		task_id='Terminate_cluster_when_finished',
		dag=dag,
		provide_context=True,
		emr_credentials='emr_user_credentials',
		emr_id="{{ ti.xcom_pull('Launch_EMR_cluster') }}"
	)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)


# dependency of this dag
start_operator >> [upload_spark_script_tos3, nationwide_tos3]

[upload_spark_script_tos3, nationwide_tos3] >> emr_launch

emr_launch >> emr_add_jobs >> emr_terminate >> end_operator







