# **About this project**

In this project, I developed two data pipelines (DAG) with Apache Airflow. The first DAG construct a new data warehouse from Postgres server and S3 data lake or add new data to already-existing data warehouse on AWS Redshift. The dag also ingests new data daily to keep the data warehouse up to date. The second data pipeline launches AWS ElasticMapReduce cluster and runs a Spark script to process big data on the cluster. All the processes are scheduled and automated by Apache Airflow. In addition, data validation is performed to ensure the data quality is good and executions of dags were done properly.

All the codes in this repository are written, developed and owned by me.

I did following in this project
- Data Collection
- Modeled the data and schema for data warehouse
- Developed Python and SQL codes for data transformation
- Wrote codes for AWS Cloud Services including Redshift, S3, ElasticMapReduce(EMR), and IAM
- Wrote Spark script for big data processing
- Developed workflows (data pipeline) with Apache Airflow
- Developed custom operators for Airflow
- Wrote tests for data validation/ data quality check

#### Data Sources
Most data come from California Department of Public Health. Other data sources are data.ca.gov, United States Census Bureau, LA Times Data Desk, and New York Times github

# Dag 1: California Coronavirus Data Warehouse
### Data Warehouse
The purpose of this data warehouse is to construct a central data repository about california coronavirus data for healthcare workers to conduct analysis about California coronavirus situation. So that they can draw data insights and make conclusions about their resource allocation. Following is some of examples of what analytics you can do from this data warehouse
-  Find or predict trend or surge in cases in each county
-  Find number of healthcare facilities, their locations and availablity in each county
-  Find number of high risk individuals in each county
-  Identify outbreaks in some institutions
- Identity counties or institution who are in critical need of supplies
- Find nearby healthcare institutions where ICU beds are still available
- Identify how far or close a county is from re-opening
- Analyze statewide data in the context of all 50 states
- Visualization of data

The data is stored in a clean format which allows users to flexibly query them while trying to maximize the space availability and minimize the query time. Data is stored in star-schema with the fact and dimension tables. Fact tables include daily coronavirus cases in each county, number of still-available ICU beds in each county, cases reported in institutions such as nursing homes, adult-senior care facilities and prisons where outbreaks happen often and others. Dimension tables include demography of each county, information about healthcare institutions, nursing homes, prisons and others. Please refer to the data model section below for more detail of data models and schema.

**Data Flow**
![warehouse data flow](https://i.postimg.cc/R0K64cRy/Coronavirus-Data-Warehouse.png)
**Airflow Dag Image**
![warehouse_airflow_dag](https://i.postimg.cc/JzNXgD1x/Screen-Shot-2021-02-12-at-7-09-58-PM.png)
### Steps of the Dag:
1.  My custom operator **GetDataOperator** first makes http requests to 11 data sources who publish new data every day. The copies of new data will be stored in local postgreSQL database who are used for other daily operations
2. The new data will be then forwarded to the certain location in S3 data lake in certain format. Other data in the local database will be forwarded to temporary S3 staging bucket so that they can be loaded to the Redshift server
3. Data in two S3 buckets that are needed to construct the data warehouse in S3 will be copied to temporary staging tables in the redshift server. If the data warehouse already exists in redshift, it only copies the new portions of data with the custom operator **StageFromS3Operator** .
4.  **LoadFactOperator** and **LoadDimensionOperator** will join and transform the data in staging tables in Redshift, It transforms the data into atomic format to be stored in the data warehouse. Fact tables are normalized to improve the space availability, while some dimension tables are denormalized to avoid repeated joining and reduce the query cost.
5. Upon finishing loading, **DataValidationOperator** will run tests on all tables in Redshift. Airflow operator to validate the values in columns are in expected range and also there are no duplicates of rows.
6.  Above processes are automated to run sequentially and scheduled to run every day by Apache Airflow

### Data Models
The purpose of this project is to run analytics, thus many joins (foreign keys) or aggregation operations are expected. Therefore I chose **Relational** data model. In addition, fact data will be huge and not all secondary indexes needed to be joined when running some analytics, so I decided to model the data in star schema so that user can selectively choose which tables they want to join which saves the space.
#### Fact Tables
**Table: county_case**
a fact table to record newly confirmed cases and deaths in each county every day
|Fields                                      |                        |
|----|---------------------------------|
|date| date of when the case was recorded|
|county_id| unique identifiers given to each county|
|new_confirmed_case| the new case reported today|
|total_onfirmed_cases| all confirmed case in this county to this date|
|new_deaths| the number of deaths reported in this county today|
|total_deaths| total number of deaths so far in this county|


**Table: county_hospitalized**
a table to record the number of hospitalized patients and bed availablities for each county
|Fields                                      |                        |
|----|---------------------------------|
|date| date of when the case was recorded|
|county_id| unique identifiers given to each county|
|positive_patients| number of patients that are currently confirmed positive on this date|
|suspected_patients| number of patients who are waiting for test results but suspected covid positive|
|icu_positive_patients| number of positive patients currently using ICU beds|
|icu_suspected_patients| number of suspected patients currently using ICU beds|


**Table: nursing_home_case**
cases recorded in nursing homes which often report outbreak


|Fields                                      |                        |
|----|---------------------------------|
|date| date of when the case was recorded|
|nursing_home_id| unique identifiers given to each nursing home facility that this case was reported|
|county_id| unique identifiers given to each county that this facility is in|
|staff_confirmed_cases|number of staff who contract covid in this facility|
|patients_confirmed_cases|number of patients from this facility who contracted covid|
|staff_deaths|number of staff who died of covid in this facility|
|patients_deaths| number of patients who died of covid in this facility

**Table: prison_case**
cases recorded in prisons
|Fields                                      |                        |
|----|---------------------------------|
|date| date of when the case was recorded|
|prison_id| unique identifier given to each california prison|
|county_id| unique identifiers given to each county|
|new_confirmed_case| the new case reported today|
|total_onfirmed_cases| all confirmed case in this county to this date|
|new_deaths| the number of deaths reported in this county today|
|total_deaths| total number of deaths so far in this county|

\+ 5 more fact tables. The models definitions and transfomration queries are stored in airflow/plugins/helper/sql_queries

#### Dimesion Tables
**Table: county**
county’s information that is related to coronavirus
|Fields                                      |                        |
|----|---------------------------------|
|id| unique identifier given to each county|
|name| name of the county|
|fips| 3-digits county identifier given by the state of California|
|minority_population| number of covid high risk minority populaiton|
|minority_ratio|the ratio of minority population in this county|
|over65_population| number of people who are over 65 who are considered to be high risk|
|over65_ratio|the ratio of over 65 population in this county|
|healthcare_facility_number| number of healthcare facility in this county|
|max_number_beds| number of all ICU beds in this county|
|reopening_tier|the tier of how open this county is close to reopening given by California Department of Public Health|
|reopening_tier_date|the date when the tier was given|

**Table: healthcare_facility**
information about California healthcare facilities
|Fields                                      |                        |
|----|---------------------------------|
|id|unique identifier given to this healthcare facility|
|isopen| indicate if this facility is in business|
|name| the name of the facility|
|type| type of this healthcare facility|
|capacity| patients capacity of this healthcare facility|
|zipcode| zipcode of this healthcare facility|
|county| the county this healthcare facility is in|
|city| the name of the city this healthcare facility is in|
|address| the address of this healthcare facility|
|admin_name| the name of the administrator|
|contact_email| the public contact email of this facility|
|contact_phone| the public contact phone number of this facility|
|latitude| latitude of this facility|
|longitude| longitude of this facility|


**Table: prison**
information about prison which outbreak is often reported
|Fields                                      |                        |
|----|---------------------------------|
|id| unique identifier given to each prison|
|code| official prison code given by California|
|name| name of the prisons|
|county| the county this prison is in|
|fips| 3 digits identifier of the county given by state, useful for join|
|zipcode| zipcode of this prison|
|felon_population| number of felon population in this prison|
|total_population| number of all population in this prison|
|capacity| the max capacity of this population|
|percent_occupied| ratio of felon population compared to the all population|
|longitude| longitude of this prison|
|latitude| latitude of this prison|
|year| year of when the population was recorded|
|month| month of when the population was recorded|

\+ 2 more dimension tables. The models definitions and transfomration queries are stored in airflow/plugins/helper/sql_queries

# Dag 2: Automating EMR job for Big Data Processing

### EMR & Spark Solution for Big Data Processing
One of the data sources has millions of rows and grows bigger every day. Not only it takes a very long time to process and load but also it requires a scalable solution. I decided to use AWS EMR and Apache Spark to run in-memory processing over multiple machines (cluster) on AWS, making the processing much faster and easily scalable. The dag will immediately terminate the cluster upon completing the job therefore the cost is cheap too.

The data being processed here is the case data in counties across all 50 states. Because the raw data takes a very long time to load, I use Spark to store them as a parquet file, a column-based format that allows faster read from Redshift. In addition, the parquet file will be partitioned by states so that the user can selectively load some states into Redshift if they wish to conduct state-to-state analysis. The processed data will be stored in a S3 process bucket.

**Data Flow:**
![emr_dataflow](https://i.postimg.cc/SKrFFc54/EMR-Job.png)
**Dag Image:**
![emr_dag](https://i.postimg.cc/7YMpT3H2/Screen-Shot-2021-02-14-at-6-05-59-PM.png)

### Steps of the Dag:
1.  First, the dag will copy (upload) the data and the pyspark script to S3 bucket, so that the EMR cluster will be able to access them.
2.  My custom operator **EMRLaunchClusterOperator** will launch an emr cluster based on parameters given to launch. We will need to configure to pre-install Hadoop, Spark and Hive and to run the spark script. The operator will return a cluster id if successfully launched.
3. Custom operator **EMRAddJobsOperator** will add jobs to the running cluster
4.  Next, custom operator **EMRTerminateOperator** Next, the airflow operator will poll the cluster state every 15 seconds. The operator does nothing while the cluster is in “RUNNING” state. Once the state has changed upon completing jobs (or failing if something happens), the airflow will programmatically terminate the cluster so you will not be charged any further.

## Other Things I did to Make Computation Faster

- **Taking advantage of Redshift's distributed computation:** Most fact tables have "county_id" column which will be joined by county table. Since county table is small, I made the distribution type ALL so that every cluster on the redshift has a copy of county table which make the join operation faster.  Other things I did was make a column "nursing_home_id" distribution key, so that the tables who will be joined by nursing home id will be spread evenly and each cluster share same joining keys which again made the computation faster. 
- **Accessing Files with s3a instead of s3:** s3a is an "Accessor to S3 Native." Using s3a address instead of native s3 address made the reading significantly faster. 

## Other Considerations for Potential Data or User Number Grow 

**What if the data was increased by 100x?**
Thankfully, AWS Redshift is a scalable data warehouse. As the data size grows, aggregating or joining millions of rows can take great amount of time. In such case, we can increase the number of CPUs and as well as the size of memories. Increased number of partitions/workers will make the computation faster. However, data warehouse is not the best for time sensitive write-heavy workload. Data warehouse is columner storage platform, therefore if we have to insert hundreds of rows per seconds it will significantly presssure the CPU usage and slow down the computation. Data warehouse is speciallized for analytical usage so we want to use another data storage solution such as Apache Cassandra for that purpose. But we have to keep in my mind that NoSQL database such as Cassandra does not provide ACID properties, join or aggregation and should not be best for analytical purpose. However, once we are able to store the write-heavy workload in cassandra, we can batch process them and convert them to columner format such as parquet file and then we can load the data into data warehouse to run analytics. Data warehouse can ingest columner data alot more efficient than ingesting a row record one by one  <br>

**What if the pipelines would be run on a daily basis by 7 am every day? What if they failed?**
Reliablity and maintainability are very important factors to consider when it comes to data engineering. The dag is currently scheduled to run daily. But this does not guarantee that they will not fail. The dag currently send http request to 11 data publishers every day. As the dag image show, some of the workload does not interfere with others. If downloading one of the 11 data sources failed, the other 10 will be still downloaded and go through the routine procedures if it is independent of the failed process. Upon a failure, the airflow will retry the failed operation three times and send me an email if it still fails. And the airflow UI will tell which part of the pipeline has failed, making it easy to fix.

**What if the database needed to be accessed by 100+ people**
As I mentioned above, Redshift data warehouse is scalable and we can always increase the number of CPU and the size of storage. Increasing the number of CPUs or partitions can accomodate the growing number of users. In addition, we can replicate some data across the partitons by setting "distribution_style=ALL," which makes the data warehouse highly available.  
