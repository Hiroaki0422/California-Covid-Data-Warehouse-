DROP TABLE IF EXISTS county;
DROP TABLE IF EXISTS prison;
DROP TABLE IF EXISTS healthcare_facility;
DROP TABLE IF EXISTS nursing_facility;
DROP TABLE IF EXISTS adult_senior_care;

CREATE TABLE county(
	"id" SERIAL NOT NULL,
	name VARCHAR(15),
	fips VARCHAR(15),
	total_population INT,
	minority_population INT,
	minority_weight NUMERIC,
	over65_population INT,
	over65_weight NUMERIC,
	healthcare_facilities_number INT, 
	max_number_beds INT,
	reopening_tier NUMERIC,
	reopening_tier_date TIMESTAMP
);

CREATE TABLE prison(
	id SERIAL NOT NULL,
	code VARCHAR(4),
	name text,
	county VARCHAR(15),
	fips VARCHAR(3),
	zipcode VARCHAR(5),
	felon_population INT,
	civil_addict_population INT,
	total_population INT,
	capacity INT,
	percent_occupied NUMERIC,
	longitude NUMERIC,
	latitude NUMERIC,
	year SMALLINT,
	month SMALLINT
);

CREATE TABLE healthcare_facility(
	id BIGINT NOT NULL,
	isopen TEXT,
	name TEXT,
	type TEXT,
	capacity INT,
	zipcode VARCHAR(5),
	zip9 VARCHAR(4),
	county VARCHAR(15),
	city TEXT,
	address TEXT,
	admin_name TEXT,
	contact_email TEXT,
	contact_phone TEXT,
	latitude NUMERIC,
	longitude NUMERIC
);

CREATE TABLE nursing_facility(
	id BIGINT NOT NULL,
	isopen TEXT,
	name TEXT,
	capacity INT,
	zipcode VARCHAR(5),
	zip9 VARCHAR(4),
	county VARCHAR(15),
	city TEXT,
	address TEXT,
	admin_name TEXT,
	contact_email TEXT,
	contact_phone TEXT,
	latitude NUMERIC,
	longitude NUMERIC,
	date TIMESTAMP
);

CREATE TABLE adult_senior_care(
	id SERIAL NOT NULL,
	health_fac_id BIGINT,
	isopen TEXT,
	name TEXT,
	slug TEXT,
	county TEXT,
	fips VARCHAR(3),
	type TEXT,
	capacity INT
);



