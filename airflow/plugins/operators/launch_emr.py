from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import time
import boto3


class EMRLaunchClusterOperator(BaseOperator):
    """
    This operator launches a AWS based on given parameters dict ElasticMapReduceCluster. 
    Returns a cluster ID upon successful launch.
    Params:
        emr_credential_id: credentials used for using AWS EMR service
        JOBFLOW_OVERWRITE: configure cluster, overwrite the degault values
    """

    @apply_defaults
    def __init__(self, emr_credential_id='', JOBFLOW_OVERWRITE={}, *args, **kwargs):
        super(EMRLaunchClusterOperator, self).__init__(*args, **kwargs)
        self.emr_credential_id = emr_credential_id
        self.JOBFLOW_OVERWRITE = JOBFLOW_OVERWRITE

    def execute(self, context):
        # Get AWS Credential
        aws_hook = AwsHook(self.emr_credential_id)
        credentials = aws_hook.get_credentials()

        # Get EMR Client Instance
        emr = boto3.client('emr', 'us-west-2', aws_access_key_id=credentials.access_key,
                           aws_secret_access_key=credentials.secret_key)

        # Launching EMR Cluster
        try:
            response = emr.run_job_flow(**self.JOBFLOW_OVERWRITE)
        except Exception as e:
            print(e)
            raise ValueError('EMR LAUNCHING FAILED')

        jobFlowId = response['JobFlowId']
        state = emr.describe_cluster(ClusterId=jobFlowId)['Cluster']['Status']['State']

        self.log.info(f'Cluster State: {state}')

        # wait for emr to launch
        while state is 'STARTING':
            time.sleep(15)
            state = emr.describe_cluster(ClusterId=jobFlowId)['Cluster']['Status']['State']

            self.log.info(f'Cluster State:{state}')

        self.log.info(f'EMR STATE:{state}')
        context['ti'].xcom_push(key='EMR_ID', value=jobFlowId) 

        return jobFlowId
