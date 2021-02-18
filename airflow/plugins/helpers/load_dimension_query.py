class LoadDimensionQueries:
	load_county = """DROP TABLE IF EXISTS elder_population;

			CREATE TEMP TABLE elder_population  AS 
			SELECT ctyname, SUM(tot_pop) as over65_population
			FROM census_data2019
			WHERE year = 12
				AND agegrp >= 14
			GROUP BY ctyname;

			DROP TABLE IF EXISTS minority_population;

			CREATE TEMP TABLE minority_population AS
			SELECT c.county, c.ctyname, tot_pop, (tot_pop - wa_male - wa_female - aa_male - aa_femle) as minority_population, (tot_pop - wa_male - wa_female - aa_male - aa_femle)*1.0/tot_pop as minority_ratio, e.over65_population, e.over65_population*1.0/tot_pop as elder_ratio
			FROM census_data2019 c, elder_population e
			WHERE c.year = 12 and c.agegrp = 0 AND c.ctyname = e.ctyname;

			DROP TABLE IF EXISTS county;

			CREATE TABLE county AS
			SELECT m.county as id, UPPER(r.county) as county, fips, tot_pop as total_population, minority_population, minority_ratio, over65_population, elder_ratio, COALESCE(healthcare_facilities_number, 0) as health_facs_num, COALESCE(b.beds, 0) as max_bed_num, r.tier, r.date
			FROM reopening_tier r 
			INNER JOIN minority_population m ON m.ctyname LIKE CONCAT(CONCAT('%',r.county), '%') AND r.date = (SELECT MAX(date) FROM staging_reopening_tier)
			LEFT JOIN (SELECT county_name, COUNT(*) AS healthcare_facilities_number 
			    FROM staging_healthcare_listing GROUP BY county_name) AS h
			    ON UPPER(m.ctyname) LIKE CONCAT(CONCAT('%', h.county_name), '%')
			LEFT JOIN (SELECT conty_name, SUM(bed_capacity) as beds FROM staging_facility_bedcounts GROUP BY conty_name) as b
			    ON UPPER(m.ctyname) LIKE CONCAT(CONCAT('%', b.conty_name), '%')
			ORDER BY r.county;
			"""

	load_healthcare_facs = """INSERT INTO healthcare_facility(id, isopen, name, type, capacity, zipcode, zip9, county, city, address, admin_name, contact_email, contact_phone, latitude, longitude)
			SELECT facid as id, fac_status_type as isopen, fac_name as name, fac_fdr as type, capacity, zip, zip9, county_name as county, city, address, fac_admin as admin_name, contact_email, contact_phone, latitude, longitude
			FROM staging_healthcare_listing;"""

	load_prison = """INSERT INTO prison(code, name, county, fips, zipcode, felon_population, civil_addict_population, total_population, capacity, percent_occupied, longitude, latitude, year, month)
			SELECT DISTINCT ps.code, institution_name as name, UPPER(county) as county, fips, zipcode, population_felons, civil_addict, total_population, designed_capacity as capacity, percent_occupied,
			x as longitude, y as latitude, year, month
			FROM staging_prisons p LEFT JOIN prison_case ps
			ON p.institution_name LIKE CONCAT(ps.code, '%')
			WHERE year = 2020 and month = 10;"""

	load_nursing = """INSERT INTO nursing_facility(id, isopen, name, capacity, zipcode, zip9, county, city, address, admin_name, contact_email, contact_phone, latitude, longitude)
			SELECT  facid as id, fac_status_type as isopen, fac_name as name, capacity, zip, zip9, county_name as county, city, address, fac_admin as admin_name, contact_email, contact_phone, latitude, longitude
			FROM staging_healthcare_listing
			WHERE fac_fdr LIKE '%SKILLED NURSING%'"""

	load_senior = """INSERT INTO adult_senior_care(health_fac_id, isopen, name, slug, county, fips, type, capacity)
			SELECT DISTINCT h.id, h.isopen, s.name, s.slug, UPPER(s.county) as county, s.fips, h.type, h.capacity
			FROM staging_senior_facilities s LEFT JOIN healthcare_facility h
			ON s.name = h.name AND UPPER(s.county) = h.county;"""