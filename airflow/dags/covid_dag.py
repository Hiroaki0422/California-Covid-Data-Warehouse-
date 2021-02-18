from datetime import datetime, timedelta
import os
import operator
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from operators.stage_from_s3 import StageFromS3Operator
from operators.get_data import GetDataOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataValidationOperator
from airflow.hooks.S3_hook import S3Hook
from helpers import LoadFactQueries, LoadDimensionQueries

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

dag = DAG('covid_warehouse_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval= '@daily'
        )

# A Helper function to upload file to AWS S3

OUTPUT_PATH = '/Users/hiroakioshima/Desktop/Hiro_Covid_Project/tables'

def upload_to_S3(filename, key, bucket_name):
	hook = S3Hook('my_S3_conn')
	hook.load_file(filename, key, bucket_name, replace=True)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


get_county_case = GetDataOperator(
		task_id='Get_county_data',
		dag=dag,
		postgres_conn_id='local_postgres',
		table='staging_county_case',
		data_url='https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/latimes-county-totals.csv',
		output_path= OUTPUT_PATH
	)

get_hospitalized_case = GetDataOperator(
		task_id='Get_hospitalized_data',
		dag=dag,
		postgres_conn_id='local_postgres',
		table='staging_hospitalized',
		data_url='https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-hospital-patient-county-totals.csv',
		output_path=OUTPUT_PATH
	)

get_nursing_home = GetDataOperator(
		task_id='Get_nursing_home_data',
		dag=dag,
		postgres_conn_id='local_postgres',
		table='staging_nursing_home',
		data_url='https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-skilled-nursing-facilities.csv',
		output_path=OUTPUT_PATH
	)

get_senior_home = GetDataOperator(
		task_id='Get_senior_home_data',
		dag=dag,
		postgres_conn_id='local_postgres',
		table='staging_senior_facilities',
		data_url='https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-adult-and-senior-care-facilities.csv',
		output_path=OUTPUT_PATH
	)

get_place_data = GetDataOperator(
		task_id='Get_place_data',
		dag=dag,
		postgres_conn_id='local_postgres',
		table='staging_place_totals',
		data_url='https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-adult-and-senior-care-facilities.csv',
		output_path=OUTPUT_PATH
	)

get_prison_case_data = GetDataOperator(
		task_id='Get_prison_case_data',
		dag=dag,
		postgres_conn_id='local_postgres',
		table='staging_prison_cases',
		data_url='https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdcr-prison-totals.csv',
		output_path=OUTPUT_PATH
	)

get_reopening_metrics = GetDataOperator(
		task_id='Get_reopening_metrics_data',
		dag=dag,
		postgres_conn_id='local_postgres',
		table='staging_reopening_metrics',
		data_url='https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-reopening-metrics.csv',
		output_path=OUTPUT_PATH
	)

get_nationwide_case = GetDataOperator(
		task_id='Get_nationwide_data',
		dag=dag,
		postgres_conn_id='local_postgres',
		table='nationwide_cases',
		data_url='https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv',
		output_path=OUTPUT_PATH
	)

get_prison_info = GetDataOperator(
		task_id='Get_prison_info',
		dag=dag,
		postgres_conn_id='local_postgres',
		table='staging_prisons',
		data_url='https://raw.githubusercontent.com/nrjones8/cdcr-population-data/master/data/monthly_cdcr_population.csv',
		output_path=OUTPUT_PATH
	)

get_reopening_tier = GetDataOperator(
		task_id='Get_reopening_tier',
		dag=dag,
		postgres_conn_id='local_postgres',
		table='staging_reopening_tier',
		data_url='https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-reopening-tiers.csv',
		output_path=OUTPUT_PATH
	)


county_case_toS3 = PythonOperator(
		task_id='Upload_county_case_s3',
		dag=dag,
		python_callable=upload_to_S3,
		op_kwargs={
			'filename':'/Users/hiroakioshima/Desktop/Hiro_Covid_Project/tables/staging_county_case.csv',
			'key':'tables/staging_county_case.csv',
			'bucket_name':'hiro-covid-datalake'
		}
	)

hospitalized_case_toS3 = PythonOperator(
		task_id='Upload_hospitalized_case_s3',
		dag=dag,
		python_callable=upload_to_S3,
		op_kwargs={
			'filename':'/Users/hiroakioshima/Desktop/Hiro_Covid_Project/tables/staging_hospitalized.csv',
			'key':'tables/staging_hospitalized.csv',
			'bucket_name':'hiro-covid-datalake'
		}
	)

nursing_case_toS3 = PythonOperator(
		task_id='Upload_nursinghome_case_s3',
		dag=dag,
		python_callable=upload_to_S3,
		op_kwargs={
			'filename':OUTPUT_PATH + '/staging_nursing_home.csv',
			'key':'tables/staging_nursing_home.csv',
			'bucket_name':'hiro-covid-datalake'
		}
	)

senior_case_toS3 = PythonOperator(
		task_id='Upload_senior_case_s3',
		dag=dag,
		python_callable=upload_to_S3,
		op_kwargs={
			'filename':OUTPUT_PATH + '/staging_senior_facilities.csv',
			'key':'tables/staging_senior_facilities.csv',
			'bucket_name':'hiro-covid-datalake'
		}
	)

place_totals_tos3 = PythonOperator(
		task_id='Upload_places_s3',
		dag=dag,
		python_callable=upload_to_S3,
		op_kwargs={
			'filename':OUTPUT_PATH + '/staging_place_totals.csv',
			'key':'tables/staging_place_totals.csv',
			'bucket_name':'hiro-covid-datalake'
		}
	)

prison_case_tos3 = PythonOperator(
		task_id='Upload_prison_case_s3',
		dag=dag,
		python_callable=upload_to_S3,
		op_kwargs={
			'filename':OUTPUT_PATH + '/staging_prison_cases.csv',
			'key':'tables/staging_prison_cases.csv',
			'bucket_name':'hiro-covid-datalake'
		}
	)

reopening_metrics_tos3 = PythonOperator(
		task_id='Upload_reopening_metrics_tos3',
		dag=dag,
		python_callable=upload_to_S3,
		op_kwargs={
			'filename':OUTPUT_PATH + '/staging_reopening_metrics.csv',
			'key':'tables/staging_reopening_metrics.csv',
			'bucket_name':'hiro-covid-datalake'
		}
	)

nationwide_tos3 = PythonOperator(
		task_id='Upload_nationwide_tos3',
		dag=dag,
		python_callable=upload_to_S3,
		op_kwargs={
			'filename':OUTPUT_PATH + '/nationwide_cases.csv',
			'key':'tables/nationwide_cases.csv',
			'bucket_name':'hiro-covid-datalake'
		}
	)

prison_info_tos3 = PythonOperator(
		task_id='Upload_prisoninfo_tos3',
		dag=dag,
		python_callable=upload_to_S3,
		op_kwargs={
			'filename':OUTPUT_PATH + '/staging_prisons.csv',
			'key':'tables/staging_prisons.csv',
			'bucket_name':'hiro-covid-datalake'
		}
	)

reopening_tier_tos3 = PythonOperator(
		task_id='Upload_reopening_tier_tos3',
		dag=dag,
		python_callable=upload_to_S3,
		op_kwargs={
			'filename':OUTPUT_PATH + '/staging_reopening_tier.csv',
			'key':'tables/staging_reopening_tier.csv',
			'bucket_name':'hiro-covid-datalake'
		}
	)


load_hospitalized_redshift = StageFromS3Operator(
		task_id='Load_hospitalized_to_redshift',
	    dag=dag,
	    redshift_conn_id='redshift',
	    table='staging_hospitalized',
	    aws_credentials_id='aws_credentials',
	    S3_BUCKET='hiro-covid-datalake',
	    s3_key='tables/staging_hospitalized.csv'
	)

load_county_redshift = StageFromS3Operator(
		task_id='Load_county_to_redshift',
	    dag=dag,
	    redshift_conn_id='redshift',
	    table='staging_county_case',
	    aws_credentials_id='aws_credentials',
	    S3_BUCKET='hiro-covid-datalake',
	    s3_key='tables/staging_county_case.csv'
	)

load_nursing_redshift = StageFromS3Operator(
		task_id='Load_nursing_to_redshift',
	    dag=dag,
	    redshift_conn_id='redshift',
	    table='staging_nursing_home',
	    aws_credentials_id='aws_credentials',
	    S3_BUCKET='hiro-covid-datalake',
	    s3_key='tables/staging_nursing_home.csv'
	)

load_seniorhome_redshift = StageFromS3Operator(
		task_id='Load_senior_to_redshift',
	    dag=dag,
	    redshift_conn_id='redshift',
	    table='staging_senior_facilities',
	    aws_credentials_id='aws_credentials',
	    S3_BUCKET='hiro-covid-datalake',
	    s3_key='tables/staging_senior_facilities.csv'
	)

load_places_redshift = StageFromS3Operator(
		task_id='Load_places_redshift',
	    dag=dag,
	    redshift_conn_id='redshift',
	    table='staging_place_totals',
	    aws_credentials_id='aws_credentials',
	    S3_BUCKET='hiro-covid-datalake',
	    s3_key='tables/staging_place_totals.csv'
	)

load_prisoncase_redshift = StageFromS3Operator(
		task_id='Load_prisoncase_redshift',
	    dag=dag,
	    redshift_conn_id='redshift',
	    table='staging_prison_cases',
	    aws_credentials_id='aws_credentials',
	    S3_BUCKET='hiro-covid-datalake',
	    s3_key='tables/staging_prison_cases.csv'
	)

load_openmetrics_redshift = StageFromS3Operator(
		task_id='Load_openmetrics_redshift',
	    dag=dag,
	    redshift_conn_id='redshift',
	    table='staging_reopening_metrics',
	    aws_credentials_id='aws_credentials',
	    S3_BUCKET='hiro-covid-datalake',
	    s3_key='tables/staging_reopening_metrics.csv'
	)

load_nationwide_redshift = StageFromS3Operator(
		task_id='Load_nationwide_redshift',
	    dag=dag,
	    redshift_conn_id='redshift',
	    table='nationwide_cases',
	    aws_credentials_id='aws_credentials',
	    S3_BUCKET='hiro-covid-datalake',
	    s3_key='tables/nationwide_cases.csv'
	)

load_prisoninfo_redshift = StageFromS3Operator(
		task_id='Load_prisoninfo_redshift',
	    dag=dag,
	    redshift_conn_id='redshift',
	    table='staging_prisons',
	    aws_credentials_id='aws_credentials',
	    S3_BUCKET='hiro-covid-datalake',
	    s3_key='tables/staging_prisons.csv'
	)

load_opentier_redshift = StageFromS3Operator(
		task_id='Load_opentier_redshift',
	    dag=dag,
	    redshift_conn_id='redshift',
	    table='staging_reopening_tier',
	    aws_credentials_id='aws_credentials',
	    S3_BUCKET='hiro-covid-datalake',
	    s3_key='tables/staging_reopening_tier.csv'
	)

load_healthcarefacs_redshift = StageFromS3Operator(
		task_id='Load_healthcarefacs_redshift',
	    dag=dag,
	    redshift_conn_id='redshift',
	    table='staging_healthcare_listing',
	    aws_credentials_id='aws_credentials',
	    S3_BUCKET='hiro-covid-datalake',
	    s3_key='static/healthcare_facility_locations.csv'
	)

load_census_redshift = StageFromS3Operator(
		task_id='Load_census_redshift',
	    dag=dag,
	    redshift_conn_id='redshift',
	    table='census_data2019',
	    aws_credentials_id='aws_credentials',
	    S3_BUCKET='hiro-covid-datalake',
	    s3_key='static/census_2019.csv'
	)

loadfact_county = LoadFactOperator(
		task_id='Loadfact_county',
		dag=dag,
		redshift_conn_id='redshift',
		table = 'county_case',
		source_table = 'staging_county_case',
		sql = LoadFactQueries.load_county_case
	)

loadfact_hospitalized = LoadFactOperator(
		task_id='Loadfact_hospitalized',
		dag=dag,
		redshift_conn_id='redshift',
		table = 'hospitalized_case',
		source_table = 'staging_hospitalized',
		sql = LoadFactQueries.load_hospitalized_case
	)


loadfact_nursing = LoadFactOperator(
		task_id='Loadfact_nursing',
		dag=dag,
		redshift_conn_id='redshift',
		table = 'nursing_home_case',
		source_table = 'staging_nursing_home',
		sql = LoadFactQueries.load_nursing_home
	)

loadfact_senior = LoadFactOperator(
		task_id='Loadfact_senior',
		dag=dag,
		redshift_conn_id='redshift',
		table = 'senior_facility_case',
		source_table = 'staging_senior_facilities',
		sql = LoadFactQueries.load_senior_facs
	)

loadfact_place = LoadFactOperator(
		task_id='Loadfact_place',
		dag=dag,
		redshift_conn_id='redshift',
		table = 'place_total',
		source_table = 'staging_place_totals',
		sql = LoadFactQueries.load_places
	)

loadfact_prison = LoadFactOperator(
		task_id='Loadfact_prison',
		dag=dag,
		redshift_conn_id='redshift',
		table = 'prison_case',
		source_table = 'staging_prison_cases',
		sql = LoadFactQueries.load_prison
	)

loadfact_open_metrics = LoadFactOperator(
		task_id='Loadfact_open_metrics',
		dag=dag,
		redshift_conn_id='redshift',
		table = 'reopening_metrics',
		source_table = 'staging_reopening_metrics',
		sql = LoadFactQueries.load_open_metrics
	)

loadfact_open_tiers = LoadFactOperator(
		task_id='Loadfact_open_tiers',
		dag=dag,
		redshift_conn_id='redshift',
		table = 'reopening_tier',
		source_table = 'staging_reopening_tier',
		sql = LoadFactQueries.load_open_tiers
	)

loadfact_nationwide = LoadFactOperator(
		task_id='Loadfact_nationwide',
		dag=dag,
		redshift_conn_id='redshift',
		table = 'other_states_cases',
		source_table = 'nationwide_cases',
		sql = LoadFactQueries.load_nationwide
	)

loaddim_healthcare = PostgresOperator(
	task_id='Loaddim_healthcare',
    dag=dag,
    sql=LoadDimensionQueries.load_healthcare_facs,
    postgres_conn_id='redshift'
	)

loaddim_county = PostgresOperator(
	task_id='Loaddim_county',
    dag=dag,
    sql=LoadDimensionQueries.load_county,
    postgres_conn_id='redshift'
	)

loaddim_prison = PostgresOperator(
	task_id='Loaddim_prison',
    dag=dag,
    sql=LoadDimensionQueries.load_prison,
    postgres_conn_id='redshift'
	)

loaddim_nursing = PostgresOperator(
	task_id='Loaddim_nursing',
    dag=dag,
    sql=LoadDimensionQueries.load_nursing,
    postgres_conn_id='redshift'
	)

loaddim_senior = PostgresOperator(
	task_id='Loaddim_senior',
    dag=dag,
    sql=LoadDimensionQueries.load_senior,
    postgres_conn_id='redshift'
	)

dataval_fact = DataValidationOperator(
	task_id='data_validation_facttables',
	dag=dag,
	redshift_conn_id='redshift',
	dq_checks= [{'testsql':'SELECT MAX(date) FROM county_case', 'expected':datetime(2021,1,20), 'op':operator.eq},
			   {'testsql':'SELECT MAX(date) FROM hospitalized_case', 'expected':datetime(2021,1,20), 'op':operator.eq},
			   {'testsql':'SELECT MAX(date) FROM reopening_metrics', 'expected':datetime(2021,1,22), 'op':operator.eq},
			   {'testsql':'SELECT MAX(date) FROM nursing_home_case', 'expected':datetime(2021,1,21), 'op':operator.eq},
			   {'testsql':'SELECT MAX(date) FROM senior_facility_case', 'expected':datetime(2021,1,21), 'op':operator.eq},
			   {'testsql':'SELECT MAX(date) FROM place_total', 'expected':datetime(2021,1,21), 'op':operator.eq},
			   {'testsql':'SELECT MAX(date) FROM prison_case', 'expected':datetime(2021,1,22), 'op':operator.eq},
			   {'testsql':'SELECT MAX(date) FROM reopening_tier', 'expected':datetime(2021,1,22), 'op':operator.eq},
			   {'testsql':'SELECT MAX(date) FROM other_states_cases', 'expected':datetime(2021,1,21), 'op':operator.eq}]
	)

dataval_dim = DataValidationOperator(
	task_id='data_validation_dimtables',
	dag=dag,
	redshift_conn_id='redshift',
	dq_checks= [{'testsql':'SELECT COUNT(*) FROM county', 'expected':50, 'op':operator.ge},
			   {'testsql':'SELECT COUNT(*) FROM healthcare_facility', 'expected':1000, 'op':operator.ge},
			   {'testsql':'SELECT COUNT(*) FROM prison', 'expected':200, 'op':operator.le},
			   {'testsql':'SELECT COUNT(*) FROM nursing_facility', 'expected':200, 'op':operator.ge},
			   {'testsql':'SELECT COUNT(*) FROM adult_senior_care', 'expected':200, 'op':operator.ge}]
	)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [get_county_case, get_hospitalized_case, get_nursing_home, get_senior_home, get_place_data, \
					get_prison_case_data, get_reopening_metrics, get_nationwide_case, get_prison_info, get_reopening_tier]

start_operator >> [load_healthcarefacs_redshift, load_census_redshift]

get_county_case >> county_case_toS3 >> load_county_redshift >> loadfact_county 
get_hospitalized_case >> hospitalized_case_toS3 >> load_hospitalized_redshift >> loadfact_hospitalized
get_nursing_home >> nursing_case_toS3 >> load_nursing_redshift >> loadfact_nursing
get_senior_home >> senior_case_toS3 >> load_seniorhome_redshift >> loadfact_senior
get_place_data >> place_totals_tos3 >> load_places_redshift >> loadfact_place
get_prison_case_data >> prison_case_tos3 >> load_prisoncase_redshift >> loadfact_prison
get_reopening_metrics >> reopening_metrics_tos3 >> load_openmetrics_redshift >> loadfact_open_metrics
get_nationwide_case >> nationwide_tos3 >> load_nationwide_redshift >> loadfact_nationwide
get_prison_info >> prison_info_tos3 >> load_prisoninfo_redshift
get_reopening_tier >> reopening_tier_tos3 >> load_opentier_redshift >> loadfact_open_tiers

[loadfact_county, loadfact_hospitalized, loadfact_nursing, loadfact_senior, loadfact_place, loadfact_prison, \
 loadfact_open_metrics, loadfact_nationwide, loadfact_open_tiers] >> dataval_fact

[loaddim_healthcare, loaddim_county, loaddim_prison, loaddim_nursing, loaddim_senior] >> dataval_dim

[dataval_fact, dataval_dim] >> end_operator

[load_census_redshift, loadfact_open_tiers, load_healthcarefacs_redshift] >> loaddim_county
load_healthcarefacs_redshift >> [loaddim_healthcare, loaddim_nursing, loaddim_senior]
[load_prisoninfo_redshift, load_prisoncase_redshift] >> loaddim_prison






