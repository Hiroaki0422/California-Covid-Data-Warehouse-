from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This custom operators transform staging tables and construct dimension tables.
    The queries of transformation is in plugin/helpers/load_dimension_query.py
    Params:
        redshift_conn_id: connection ID for redshift
        table: the target dimension table you want to load
        sql: sql query used for transformation
    """
    ui_color = 'orange'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 source_table="",
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.source_table = source_table
        self.sql = sql

    def execute(self, context):
        self.log.info('LoadFactOperator Running....')

        # Establish the connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # pull the latest data from redshift
        latest_time = redshift.get_records(f'SELECT MAX(date) FROM {self.table}')[0][0]
        self.log.info(f'LATEST TIME: {latest_time}')

        if latest_time is None:
        	latest_time = '2020-01-01'

        # Run SQL Query
        sql_query = self.sql.format(self.table, self.source_table, latest_time)
        redshift.run(sql_query)

        self.log.info('table {self.table} is loaded')
