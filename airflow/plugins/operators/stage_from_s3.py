from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageFromS3Operator(BaseOperator):
    """
    This custom operator stage tables in S3 temporary bucket to redshift server.
    Params:
        redshift_conn_id: connection ID for AWS redshift
        table: target staging table on redshift
        aws_credentials_id: a credentials used to access AWS services
        S3_BUCKET: S3 bucket name from where you load the data to redshift
        s3_key: key of the table within the bucket
    """
    ui_color = '#ff0000'
    ui_fgcolor = '#000000'
    
    sql_copy = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                REGION 'us-west-2'
                DELIMITER ','
                IGNOREHEADER 1
                CSV;
               """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 aws_credentials_id="",
                 S3_BUCKET="",
                 s3_key="",
                 *args, **kwargs):

        super(StageFromS3Operator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.S3_BUCKET = S3_BUCKET
        self.s3_key = s3_key

    def execute(self, context):
        self.log.info('Staging to AWS redshift')
        
        # get credentials of aws
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        # Establish connection with redshifrt
        redshift = PostgresHook(self.redshift_conn_id)
        
        # get path to s3 bucket
        # rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.S3_BUCKET, self.s3_key)
        
        # format the sql statement
        delete_sql = 'DELETE FROM {}'.format(self.table)
        stage_sql = StageFromS3Operator.sql_copy.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key
            )
        
        # execute sql statements on redshift
        self.log.info('Staging.....')

        redshift.run(delete_sql)
        redshift.run(stage_sql)
        
        self.log.info('Staging Complete')
        
        
        
        
        
        
        
        





