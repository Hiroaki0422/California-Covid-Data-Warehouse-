from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import time
import boto3


class EMRTerminateOperator(BaseOperator):
    """
    This operator terminate the EMR cluster launched earlier.
    It termnates only after the cluster completed its job/failed
    Params:
        emr_credential_id: the credentials required to access the cluster
        emr_id: the ID of the running cluster
        
    """
    template_fields = ['emr_id']

    @apply_defaults
    def __init__(self, emr_credential_id='', emr_id='', *args, **kwargs):
        super(EMRTerminateOperator, self).__init__(*args, **kwargs)
        self.emr_credential_id = emr_credential_id
        self.emr_id = emr_id

    def execute(self, context):
        # Get AWS Credential
        aws_hook = AwsHook(self.emr_credential_id)
        credentials = aws_hook.get_credentials()

        # self.log.info(context['ti'].xcom_pull(task_ids='Launch_EMR_cluster'))
        self.log.info(self.emr_id)

        # Get EMR Client Instance
        emr = boto3.client('emr', 'us-west-2', aws_access_key_id=credentials.access_key, aws_secret_access_key=credentials.secret_key)
        res = emr.describe_cluster(ClusterId=self.emr_id)

        running_state = res['Cluster']['Status']['State']

        self.log.info(f'Cluster State: {running_state}')
        self.log.info(running_state is 'RUNNING')

        # Wait until the running is complete 
        while running_state == 'RUNNING' or running_state == 'STARTING':
            time.sleep(15)
            res = emr.describe_cluster(ClusterId=self.emr_id)
            running_state = res['Cluster']['Status']['State']
            self.log.info(f'Cluster State: {running_state}')

        # Adding jobs to EMR Cluster 
        emr.terminate_job_flows(JobFlowIds=[self.emr_id])


        