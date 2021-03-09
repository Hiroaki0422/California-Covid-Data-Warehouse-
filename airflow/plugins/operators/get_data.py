from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class GetDataOperator(BaseOperator):
    """
    This custom operator sends http request to specific data sources and make copy in both
    local postgres server and filesystem.
    Params:
        postgres_conn_id: the connection id of local postgres server
        table: name of staging table in local postgres server 
        data_url: the data source where you send http request
        output_path: the path of local filesystem where you dump copy of the data
    """
    ui_color = '#358140'
    
    sql_copy = """
                DELETE FROM {};

                COPY {}
                FROM PROGRAM 'curl "{}"'
                DELIMITER ','
                CSV HEADER;

                COPY {} TO '{}/{}.csv' WITH DELIMITER ',' csv header NULL AS '\\N';

               """
    
    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 table="",
                 data_url="",
                 output_path="",
                 *args, **kwargs):

        super(GetDataOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.data_url = data_url
        self.output_path = output_path


    def execute(self, context):
        self.log.info(f'Staging {self.table} to Postgres Server')
        
        # Establish connection with postgres server
        redshift = PostgresHook(self.postgres_conn_id)
        
        # format the sql statement
        stage_sql = GetDataOperator.sql_copy.format(
            self.table,
            self.table,
            self.data_url,
            self.table,
            self.output_path,
            self.table
            )
        
        # execute sql statements on redshift
        self.log.info('Staging.....')
        
        redshift.run(stage_sql)
        
        self.log.info('Staging Complete')
        
        
        
        
        
        
        
        





