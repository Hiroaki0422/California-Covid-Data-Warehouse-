from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import operator

class DataValidationOperator(BaseOperator):
	"""
	This custom operator perform the data validation.
	Perform a given test and see if the result matches expected
	Params:
		redshift_conn_id = connection ID for redshift
		dq_checks = list of tests, a test contains three parameters: query, expected result and comparator
	"""

	ui_color = 'lightblue'

	@apply_defaults
	def __init__(self, redshift_conn_id="", dq_checks=[], *args, **kwargs):
		super(DataValidationOperator, self).__init__(*args, **kwargs)
		self.redshift_conn_id = redshift_conn_id
		self.dq_checks = dq_checks

	def execute(self, context):
		self.log.info('Starting Data Validation')

		# Establish connection
		redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

		# perform data quality test, raise error if the result didn't match given expected value
		for test in self.dq_checks:
			self.log.info(f'Running {test["testsql"]}')
			result = redshift.get_records(test['testsql'])[0][0]

			if not test['op'](result, test['expected']):
				raise ValueError(f'Data Quality Check failed query:{test["testsql"]}, expected:{test["expected"]}, got:{result}')

		self.log.info('Data Validation Complete, Congraduation')



