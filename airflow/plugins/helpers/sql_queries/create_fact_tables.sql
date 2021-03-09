-- Create Tables for Fact Tables
DROP TABLE IF EXISTS county_case;

CREATE TABLE county_case(
	"date" TIMESTAMP NOT NULL,
	county VARCHAR(15) NOT NULL,
	fips VARCHAR(3) NOT NULL,
	confirmed_cases INT,
	deaths INT,
	new_confirmed_cases INT,
	new_deaths INT
);

DROP TABLE IF EXISTS hospitalized_case;

CREATE TABLE hospitalized_case (
	"date" TIMESTAMP NOT NULL,
	county VARCHAR(15) NOT NULL,
	fips VARCHAR(3) NOT NULL,
	positive_patients INT,
	suspected_patients INT,
	icu_positive_patients INT,
	icu_suspected_patients INT,
	icu_available_beds INT
);

DROP TABLE IF EXISTS nursing_home_case;

CREATE TABLE nursing_home_case(
	"date" TIMESTAMP NOT NULL,
	"id" BIGINT NOT NULL,
	slug TEXT,
	"name" TEXT,
	county TEXT,
	staff_confirmed_cases INT,
	patients_confirmed_cases INT,
	staff_deaths INT,
	patients_deaths INT,
	fips VARCHAR(3)
);

DROP TABLE IF EXISTS senior_facility_case;

CREATE TABLE senior_facility_case(
	"date" TIMESTAMP NOT NULL,
	slug TEXT NOT NULL,
	"name" TEXT,
	county TEXT,
	staff_confirmed_cases INT,
	patients_confirmed_cases INT,
	staff_deaths INT,
	patients_deaths INT,
	fips VARCHAR(5)
)

DROP TABLE IF EXISTS place_total;

CREATE TABLE place_total(
	date TIMESTAMP,
	slug TEXT,
	"name" TEXT,
	county VARCHAR(25),
	staff_confirmed_cases INT,
	patients_confirmed_cases INT,
	staff_deaths INT,
	patients_deaths INT,
	fips VARCHAR(3)
)

DROP TABLE IF EXISTS prison_case;

CREATE TABLE prison_case(
	"date" TIMESTAMP NOT NULL,
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
)

DROP TABLE IF EXISTS reopening_metrics;

CREATE TABLE reopening_metrics(
	"date" TIMESTAMP,
	county VARCHAR(25),
	fips VARCHAR(25),
	percapita_case_rate NUMERIC,
	adjusted_case_rate NUMERIC,
	positivity_rate NUMERIC,
	equity_index NUMERIC
);

DROP TABLE IF EXISTS reopening_tier;

CREATE TABLE reopening_tier(
	"date" TIMESTAMP,
	county VARCHAR(15),
	fips VARCHAR(3),
	tier NUMERIC
);

DROP TABLE IF EXISTS other_states_cases;

CREATE TABLE other_states_cases(
	"date" TIMESTAMP,
	county TEXT,
	"state" TEXT,
	state_fips VARCHAR(5),
	cases INT,
	deaths INT
)

