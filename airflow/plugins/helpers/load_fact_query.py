class LoadFactQueries:
	load_county_case = """INSERT INTO {} SELECT * FROM {} WHERE date > '{}';"""

	load_hospitalized_case = """ INSERT INTO {} SELECT * FROM {} WHERE date > '{}'"""

	load_nursing_home = """ INSERT INTO {}
							SELECT date, id, slug, name, county, staff_confirmed_cases, patients_confirmed_cases, staff_deaths, patients_deaths, fips
							FROM {}
							WHERE date > '{}'; """

	load_senior_facs = """  INSERT INTO {}
							SELECT date, slug, name, county, staff_confirmed_cases, patients_confirmed_cases, staff_deaths, patients_deaths, fips
							FROM {}
							WHERE date > '{}';"""

	load_places = """   INSERT INTO {}
						SELECT date, slug, name, county, staff_confirmed_cases, patients_confirmed_cases, staff_deaths, patients_deaths, fips
						FROM {}
						WHERE date > '{}'; """

	load_prison = """INSERT INTO {} SELECT * FROM {} WHERE date > '{}';"""

	load_open_metrics = """INSERT INTO {} SELECT * FROM {} WHERE date > '{}';"""

	load_open_tiers = """INSERT INTO {} SELECT * FROM {} WHERE date > '{}';"""

	load_nationwide = """INSERT INTO {} SELECT * FROM {} WHERE date > '{}';"""


