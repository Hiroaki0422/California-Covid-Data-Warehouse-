DROP TABLE IF EXISTS staging_hospitalized;
DROP TABLE IF EXISTS staging_county_case;
DROP TABLE IF EXISTS staging_hospitalized;
DROP TABLE IF EXISTS staging_nursing_home;
DROP TABLE IF EXISTS staging_healthcare_listing;
DROP TABLE IF EXISTS staging_facility_bedcounts;
DROP TABLE IF EXISTS census_data2019;
DROP TABLE IF EXISTS staging_senior_facilities;
DROP TABLE IF EXISTS staging_place_totals;
DROP TABLE IF EXISTS staging_prison_cases;
DROP TABLE IF EXISTS staging_reopening_metrics;
DROP TABLE IF EXISTS nationwide_cases;
DROP TABLE IF EXISTS staging_prisons;
DROP TABLE IF EXISTS staging_reopening_tier;

CREATE TABLE staging_hospitalized (
	"date" TIMESTAMP,
	county VARCHAR(15),
	fips VARCHAR(3),
	positive_patients INT,
	suspected_patients INT,
	icu_positive_patients INT,
	icu_suspected_patients INT,
	icu_available_beds INT
);

CREATE TABLE staging_county_case(
	"date" TIMESTAMP,
	county VARCHAR(15),
	fips VARCHAR(3),
	confirmed_cases INT,
	deaths INT,
	new_confirmed_cases INT,
	new_deaths INT
);

CREATE TABLE staging_nursing_home(
	"date" TIMESTAMP,
	"id" BIGINT,
	slug TEXT,
	"name" TEXT,
	county TEXT,
	staff_confirmed_cases INT,
	patients_confirmed_cases INT,
	staff_deaths INT,
	patients_deaths INT,
	fips VARCHAR(3),
	staff_confirmed_cases_note TEXT,
	patients_confirmed_cases_note TEXT,
	staff_deaths_note TEXT,
	patients_deaths_note TEXT
);

CREATE TABLE staging_healthcare_listing (
	license_certified TEXT,
	"flag" TEXT,
	T18_19 TEXT,
	facid BIGINT,
	fac_status_type TEXT,
	aspen_facid TEXT,
	ccn VARCHAR(20),
	terminat_sw VARCHAR(20),
	participation_date TIMESTAMP,
	approval_date TIMESTAMP,
	npi TEXT,
	can_be_deemed_factype VARCHAR(20),
	can_be_certified_factype VARCHAR(20),
	deemed NUMERIC,
	deemed_by_id NUMERIC,
	ao_cd NUMERIC,
	dmg_efctv_dt TEXT,
	ao_trmntn_dt TEXT,
	ao_name TEXT,
	fac_name TEXT,
	fac_type_code VARCHAR(20),
	fac_fdr TEXT,
	ltc VARCHAR(10),
	capacity INT,
	address TEXT,
	city TEXT,
	zip INT,
	zip9 INT,
	fac_admin TEXT,
	contact_email TEXT,
	contact_fax TEXT,
	contact_phone TEXT,
	county_code INT,
	county_name VARCHAR(25),
	district_number SMALLINT,
	district_name VARCHAR(25),
	is_fac_main VARCHAR(20),
	parent_fac_id BIGINT,
	fac_relationship_type TEXT,
	start_date TIMESTAMP,
	license_number BIGINT,
	business_name TEXT,
	license_status TEXT,
	initial_license_date TEXT,
	license_effective_date TIMESTAMP,
	licence_expiration_date TIMESTAMP,
	entity_type_desc TEXT,
	latitude NUMERIC,
	longitude NUMERIC,
	"location" TEXT,
	oshpd_id BIGINT,
	cclho_code BIGINT,
	cclho_name TEXT,
	fips_county_code INT,
	birthing_facility_flag TEXT,
	trauma_ped_ctr TEXT,
	trauma_ctr TEXT,
	type_of_care TEXT,
	critical_access_hospital TEXT,
	"date" TIMESTAMP
);

CREATE TABLE staging_facility_bedcounts(
	fac_id BIGINT,
	fac_name text,
	fac_fdr text,
	bed_capacity_type TEXT,
	bed_capacity SMALLINT,
	conty_name VARCHAR(20)
);

CREATE TABLE census_data2019(
	sumlev SMALLINT,
	"state" SMALLINT,
	county SMALLINT,
	stname VARCHAR(15),
	ctyname TEXT,
	"year" SMALLINT,
	agegrp SMALLINT,
	tot_pop INT,
	tot_male INT,
	tot_female INT,
	wa_male INT,
	wa_female INT,
	ba_male INT,
	ba_female INT,
	ia_male INT,
	ia_female INT,
	aa_male INT,
	aa_femle INT,
	na_male INT,
	na_female INT,
	tom_male INT,
	tom_female INT,
	wac_male INT,
	wac_female INT,
	bac_male INT,
	bac_female INT,
	iac_male INT,
	iac_female INT,
	aac_male INT,
	aac_female INT,
	nac_male INT,
	nac_female INT,
	nh_male INT,
	nh_female INT,
	nhwa_male INT,
	nhwa_female INT,
	nhba_male INT,
	nhba_female INT,
	nhia_male INT,
	nhia_female INT,
	nhaa_male INT,
	nhaa_female INT,
	nhna_male INT,
	nhna_female INT,
	nhtom_male INT,
	nhtom_female INT,
	nhwac_male INT,
	nhwac_female INT,
	nhbac_male INT,
	nhbac_female INT,
	nhiac_male INT,
	nhiac_female INT,
	nhaac_male INT,
	nhaac_female INT,
	nhnac_male INT,
	nhnac_female INT,
	h_male INT,
	h_female INT,
	hwa_male INT,
	hwa_female INT,
	hba_male INT,
	hba_female INT,
	hia_male INT,
	hia_female INT,
	haa_male INT,
	haa_female INT,
	hna_male INT,
	hna_female INT,
	htom_male INT,
	htom_female INT,
	hwac_male INT,
	hwac_female INT,
	hbac_male INT,
	hbac_female INT,
	hiac_male INT,
	hiac_female INT,
	haac_male INT,
	haac_female INT,
	hnac_male INT,
	hnac_female INT
);

CREATE TABLE staging_senior_facilities(
	date TIMESTAMP,
	slug TEXT,
	"name" TEXT,
	county VARCHAR(25),
	staff_confirmed_cases INT,
	patients_confirmed_cases INT,
	staff_deaths INT,
	patients_deaths INT,
	fips VARCHAR(3),
	staff_confirmed_cases_note TEXT,
	patients_confirmed_cases_note TEXT,
	staff_deaths_note TEXT,
	patients_deaths_note TEXT
);

CREATE TABLE staging_place_totals (
	"date" TIMESTAMP,
	slug TEXT,
	"name" TEXT,
	county TEXT,
	staff_confirmed_cases INT,
	patients_confirmed_cases INT,
	staff_deaths INT,
	patients_deaths INT,
	fips VARCHAR(5),
	staff_confirmed_cases_note TEXT,
	patients_confirmed_cases_note TEXT,
	staff_deaths_note TEXT,
	patients_deaths_note TEXT
);

CREATE TABLE staging_prison_cases(
	"date" TIMESTAMP,
	code VARCHAR(5),
	"name" TEXT,
	city TEXT,
	county TEXT,
	fips VARCHAR(3),
	zipcode VARCHAR(5),
	x NUMERIC,
	y NUMERIC,
	confirmed_cases INT,
	new_confirmed_cases INT,
	active_cases INT,
	released_cases INT,
	resolved_cases INT,
	deaths INT,
	new_deaths INT
);

CREATE TABLE staging_reopening_metrics(
	"date" TIMESTAMP,
	county VARCHAR(25),
	fips VARCHAR(25),
	percapita_case_rate NUMERIC,
	adjusted_case_rate NUMERIC,
	positivity_rate NUMERIC,
	equity_index NUMERIC
);

CREATE TABLE nationwide_cases(
	"date" TIMESTAMP,
	county TEXT,
	"state" TEXT,
	state_fips VARCHAR(5),
	cases INT,
	deaths INT
);

CREATE TABLE staging_prisons(
	"year" SMALLINT,
	"month" SMALLINT,
	institution_name TEXT,
	population_felons INT,
	civil_addict INT,
	total_population INT,
	designed_capacity INT,
	percent_occupied NUMERIC,
	staffed_capacity INT,
	source_pdf_name TEXT
);

CREATE TABLE staging_reopening_tier(
	"date" TIMESTAMP,
	county VARCHAR(15),
	fips VARCHAR(3),
	tier NUMERIC
);


COPY staging_healthcare_listing
FROM '/Users/hiroakioshima/Desktop/Hiro_Vaccine_Priority/healthcare_facility_listing/healthcare_facility_locations.csv'
DELIMITER ','
CSV HEADER;

COPY staging_county_case
FROM PROGRAM 'curl "https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/latimes-county-totals.csv"'
DELIMITER ','
CSV HEADER;

COPY staging_hospitalized
FROM PROGRAM 'curl "https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-hospital-patient-county-totals.csv"'
DELIMITER ','
CSV HEADER;

COPY staging_nursing_home
FROM PROGRAM  'curl "https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-skilled-nursing-facilities.csv"'
DELIMITER ','
CSV HEADER;

COPY staging_facility_bedcounts
FROM '/Users/hiroakioshima/Desktop/Hiro_Vaccine_Priority/healthcare_facility_bedcounts/healthcare_facility_beds.csv'
DELIMITER ','
CSV HEADER;

COPY census_data2019
FROM '/Users/hiroakioshima/Desktop/Hiro_Vaccine_Priority/census_demography/data.csv'
DELIMITER ','
CSV HEADER;

COPY staging_senior_facilities
FROM PROGRAM 'curl "https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-adult-and-senior-care-facilities.csv"'
DELIMITER ','
CSV HEADER;

COPY staging_place_totals
FROM PROGRAM 'curl "https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-adult-and-senior-care-facilities.csv"'
DELIMITER ','
CSV HEADER;

COPY staging_prison_cases
FROM PROGRAM 'curl "https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdcr-prison-totals.csv"'
DELIMITER ','
CSV HEADER;

COPY staging_reopening_metrics
FROM PROGRAM 'curl "https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-reopening-metrics.csv"'
DELIMITER ','
CSV HEADER;

COPY nationwide_cases
FROM PROGRAM 'curl "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"'
DELIMITER ','
CSV HEADER;

COPY staging_prisons
FROM PROGRAM 'curl "https://raw.githubusercontent.com/nrjones8/cdcr-population-data/master/data/monthly_cdcr_population.csv"'
DELIMITER ','
CSV HEADER;

COPY staging_reopening_tier
FROM PROGRAM 'curl https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-reopening-tiers.csv'
DELIMITER ','
CSV HEADER;

