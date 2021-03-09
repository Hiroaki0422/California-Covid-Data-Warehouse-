-- Load State Dimension Table

DROP TABLE IF EXISTS elder_population;

CREATE TEMP TABLE elder_population  AS 
SELECT ctyname, SUM(tot_pop) as over65_population
FROM census_data2019
WHERE year = 12
	AND agegrp >= 14
GROUP BY ctyname;

DROP TABLE IF EXISTS minority_population;

CREATE TEMP TABLE minority_population AS
SELECT county, ctyname, tot_pop, (tot_pop - wa_male - wa_female - aa_male - aa_femle) as minority_population
FROM census_data2019 
WHERE year = 12 and agegrp = 0
;

INSERT INTO county(name, fips, total_population, minority_population, minority_weight, over65_population, over65_weight, healthcare_facilities_number, max_number_beds, reopening_tier, reopening_tier_date)
SELECT UPPER(r.county), fips, tot_pop, minority_population, ROUND((CAST(minority_population AS numeric) / CAST(tot_pop AS numeric)), 3), over65_population, ROUND((CAST(over65_population AS numeric) / CAST(tot_pop AS numeric)), 3), 
		COALESCE(healthcare_facilities_number, 0), COALESCE(b.beds, 0), r.tier, r.date
FROM reopening_tier r 
INNER JOIN minority_population m ON m.ctyname LIKE CONCAT('%',r.county, '%') AND r.date = (SELECT MAX(date) FROM staging_reopening_tier)
INNER JOIN elder_population e ON m.ctyname = e.ctyname
LEFT JOIN (SELECT county_name, COUNT(*) AS healthcare_facilities_number 
	FROM staging_healthcare_listing GROUP BY county_name) AS h
	ON UPPER(m.ctyname) LIKE CONCAT('%', h.county_name, '%')
LEFT JOIN (SELECT conty_name, SUM(bed_capacity) as beds FROM staging_facility_bedcounts GROUP BY conty_name) as b
	ON UPPER(m.ctyname) LIKE CONCAT('%', b.conty_name, '%')
ORDER BY r.county;


-- Load Healthcare Facility Dimension Table

INSERT INTO healthcare_facility(id, isopen, name, type, capacity, zipcode, zip9, county, city, address, admin_name, contact_email, contact_phone, latitude, longitude)
SELECT facid as id, fac_status_type as isopen, fac_name as name, fac_fdr as type, capacity, zip, zip9, county_name as county, city, address, fac_admin as admin_name, contact_email, contact_phone, latitude, longitude, date as date_logged
	FROM staging_healthcare_listing;


-- Load Prison Dimension Table

INSERT INTO prison(code, name, county, fips, zipcode, felon_population, civil_addict_population, total_population, capacity, percent_occupied, longitude, latitude, year, month)
SELECT DISTINCT ps.code, institution_name as name, UPPER(county) as county, fips, zipcode, population_felons, civil_addict, total_population, designed_capacity as capacity, percent_occupied,
	x as longitude, y as latitude, year, month
	FROM staging_prisons p LEFT JOIN prison_case ps
	ON p.institution_name LIKE CONCAT(ps.code, '%')
	WHERE year = 2020 and month = 10;


-- Load Nursing Facility Dimension Table

INSERT INTO nursing_facility(id, isopen, name, capacity, zipcode, zip9, county, city, address, admin_name, contact_email, contact_phone, latitude, longitude, date)
SELECT  facid as id, fac_status_type as isopen, fac_name as name, capacity, zip, zip9, county_name as county, city, address, fac_admin as admin_name, contact_email, contact_phone, latitude, longitude, date as date_logged 
	FROM staging_healthcare_listing
	WHERE fac_fdr LIKE '%SKILLED NURSING%';


-- Load Adult Senior Care Dimension Table
-- This is still in development will import new data too

INSERT INTO adult_senior_care(health_fac_id, isopen, name, slug, county, fips, type, capacity)
SELECT DISTINCT h.id, h.isopen, s.name, s.slug, UPPER(s.county) as county, s.fips, h.type, h.capacity
	FROM staging_senior_facilities s LEFT JOIN healthcare_facility h
	ON s.name = h.name AND UPPER(s.county) = h.county;




