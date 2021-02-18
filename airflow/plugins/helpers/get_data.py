# class GetData:
# 	get_county = (""" 
# 			DELETE FROM staging_county_case;

# 			COPY staging_county_case
# 			FROM PROGRAM 'curl \"https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/latimes-county-totals.csv\"'
# 			DELIMITER ','
# 			CSV HEADER;

# 			COPY staging_county_case TO '/Users/hiroakioshima/Desktop/Hiro_Covid_Project/tables/staging_county_case.csv' WITH DELIMITER ',' csv header NULL AS '\N';
# 		""")

# 	get_hospitalized = ("""
# 			DELETE FROM staging_hospitalized;

# 			COPY staging_hospitalized
# 			FROM PROGRAM 'curl "https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-hospital-patient-county-totals.csv"'
# 			DELIMITER ','
# 			CSV HEADER;

# 			COPY staging_hospitalized TO '/Users/hiroakioshima/Desktop/Hiro_Covid_Project/tables/staging_hospitalized.csv' WITH DELIMITER ',' csv header NULL AS '\N';
# 		""")

# 	get_nursing_home = ("""
# 			DELETE FROM staging_nursing_home;

# 			COPY staging_nursing_home
# 			FROM PROGRAM  'curl "https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-skilled-nursing-facilities.csv"'
# 			DELIMITER ','
# 			CSV HEADER;

# 			COPY staging_nursing_home TO '/Users/hiroakioshima/Desktop/Hiro_Covid_Project/tables/staging_nursing_home.csv' WITH DELIMITER ',' csv header NULL AS '\N';
# 		""")

# 	get_senior_facilities = ("""
# 			DELETE FROM staging_senior_facilities;

# 			COPY staging_senior_facilities
# 			FROM PROGRAM 'curl "https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-adult-and-senior-care-facilities.csv"'
# 			DELIMITER ','
# 			CSV HEADER;

# 			COPY staging_senior_facilities TO '/Users/hiroakioshima/Desktop/Hiro_Covid_Project/tables/staging_senior_facilities.csv' WITH DELIMITER ',' csv header NULL AS '\N';
# 		""")

# 	get_place_totals = ("""
# 			DELETE FROM staging_place_totals;

# 			COPY staging_place_totals
# 			FROM PROGRAM 'curl "https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-adult-and-senior-care-facilities.csv"'
# 			DELIMITER ','
# 			CSV HEADER;

# 			COPY staging_place_totals TO '/Users/hiroakioshima/Desktop/Hiro_Covid_Project/tables/staging_place_totals.csv' WITH DELIMITER ',' csv header NULL AS '\N';
# 		""")

# 	get_prison_cases = ("""
# 			DELETE FROM staging_prison_cases;

# 			COPY staging_prison_cases
# 			FROM PROGRAM 'curl "https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdcr-prison-totals.csv"'
# 			DELIMITER ','
# 			CSV HEADER;

# 			COPY staging_prison_cases TO '/Users/hiroakioshima/Desktop/Hiro_Covid_Project/tables/staging_prison_cases.csv' WITH DELIMITER ',' csv header NULL AS '\N';
# 		""")

# 	get_reopening_metrics = ("""
# 			DELETE FROM staging_reopening_metrics;

# 			COPY staging_reopening_metrics
# 			FROM PROGRAM 'curl "https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-reopening-metrics.csv"'
# 			DELIMITER ','
# 			CSV HEADER;

# 			COPY staging_reopening_metrics TO '/Users/hiroakioshima/Desktop/Hiro_Covid_Project/tables/staging_reopening_metrics.csv' WITH DELIMITER ',' csv header NULL AS '\N';
# 		""")

# 	get_nationwide_cases = ("""
# 			DELETE FROM nationwide_cases;

# 			COPY nationwide_cases
# 			FROM PROGRAM 'curl "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"'
# 			DELIMITER ','
# 			CSV HEADER;

# 			COPY nationwide_cases TO '/Users/hiroakioshima/Desktop/Hiro_Covid_Project/tables/nationwide_cases.csv' WITH DELIMITER ',' csv header NULL AS '\N';
# 		""")

# 	get_prisons = ("""
# 			DELETE FROM staging_prisons;

# 			COPY staging_prisons
# 			FROM PROGRAM 'curl "https://raw.githubusercontent.com/nrjones8/cdcr-population-data/master/data/monthly_cdcr_population.csv"'
# 			DELIMITER ','
# 			CSV HEADER;

# 			COPY staging_prisons TO '/Users/hiroakioshima/Desktop/Hiro_Covid_Project/tables/staging_prisons.csv' WITH DELIMITER ',' csv header NULL AS '\N';
# 		""")

# 	get_reopening_tier = ("""
# 			DELETE FROM staging_reopening_tier;

# 			COPY staging_reopening_tier
# 			FROM PROGRAM 'curl https://raw.githubusercontent.com/datadesk/california-coronavirus-data/master/cdph-reopening-tiers.csv'
# 			DELIMITER ','
# 			CSV HEADER;

# 			COPY staging_reopening_tier TO '/Users/hiroakioshima/Desktop/Hiro_Covid_Project/tables/staging_reopening_tier.csv' WITH DELIMITER ',' csv header NULL AS '\N';
# 		""")
