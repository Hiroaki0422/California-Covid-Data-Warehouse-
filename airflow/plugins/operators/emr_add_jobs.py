from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import time
import boto3


class EMRAddJobsOperator(BaseOperator):
    template_fields = ['emr_id']

    @apply_defaults
    def __init__(self, emr_credential_id='', JOBSTEPS={}, *args, emr_id, **kwargs):
        super(EMRAddJobsOperator, self).__init__(*args, **kwargs)
        self.emr_credential_id = emr_credential_id
        self.JOBSTEPS = JOBSTEPS
        self.emr_id = emr_id

    def execute(self, context):
        # Get AWS Credential
        aws_hook = AwsHook(self.emr_credential_id)
        credentials = aws_hook.get_credentials()

        self.log.info(context['ti'].xcom_pull(task_ids='Launch_EMR_cluster'))
        self.log.info(self.emr_id)

        # Get EMR Client Instance
        emr = boto3.client('emr', 'us-west-2', aws_access_key_id=credentials.access_key, aws_secret_access_key=credentials.secret_key)

        # Adding jobs to EMR Cluster 
        emr.add_job_flow_steps(JobFlowId=self.emr_id, Steps=self.JOBSTEPS)


        