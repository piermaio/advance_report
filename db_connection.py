#
# Connection to the postgres geodatabase
#

import psycopg2
import sqlalchemy
import pandas as pd
import geopandas as gpd


def db_connection(table_name, use='r', statement=''):
	try:
		connection = psycopg2.connect(user = "pieralberto",
									  password = "4q2WK4MZMs",
									  host = "db1.wild-fire.eu",
									  port = "5432",
									  database = "e1gwis")

		cursor = connection.cursor()
		# Print PostgreSQL Connection properties
		print(connection.get_dsn_parameters(), "\n")

		# Print PostgreSQL version
		cursor.execute("SELECT version();")
		record = cursor.fetchone()
		print("You are connected to - ", record, "\n")
		cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
		# print(cursor.fetchall())
		# temp = cursor.fetchall()

		engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
			user = "pieralberto",
			password = "4q2WK4MZMs",
			host = "db1.wild-fire.eu",
			port = "5432",
			database = "e1gwis",
		)
		# print('creating sqlalchemy engine')
		engine = sqlalchemy.create_engine(engine_string)
		# read a table from database into pandas dataframe, replace "tablename" with your table name
		# global df_sql
		# df_sql = pd.read_sql_query('select * from gw_burntarea_effis.ba_oracle_export_year', con = engine)
		# print("dataframe df_sql from ba_oracle_export_year successfully downloaded")
		global gdf_sql
		print('Downloading the geodataframe from {} \n'.format(table_name))
		gdf_sql = gpd.read_postgis('select * from {}'.format(table_name), con=engine)
		print("geodataframe gdf_sql from ba_oracle_export_year successfully downloaded")
		# global gdf_ba
		# gdf_ba = gpd.read_postgis('select * from gw_burntarea_effis.ba_year', con=engine)
		# print("geodataframe gdf_ba from ba_year successfully downloaded")
		if use =='r':
			print('Downloading the dataframe from {} \n'.format(table_name))
			global df_sql # table format excluding geoinformation
			df_sql = pd.read_sql_query('select * from {}'.format(table_name), con=engine)
			print("Dataframe successfully downloaded\n")
			# print('Downloading the geodataframe\n')
			# global gdf_sql # includes geoinformation
			# gdf_sql = gpd.read_postgis('select * from {}'.format(table_name), con=engine)
			# print("geodataframe successfully downloaded\n")
		if use =='w':
			count_statement = 'SELECT COUNT(*) FROM gw_burntarea_effis.rob_ba_evo_test'
			print(statement)
			print(cursor.execute(count_statement))
			cursor.execute(statement)
			print(cursor.execute(count_statement))
			connection.commit()
			# print('Table updated correctly on db\n')
		global tab_nations  # Table associating the
		tab_nations = pd.read_sql_query('select * from "burnt_areas"."Tab_Elenco_Nazioni"', con=engine)
		print("tab_nations successfully downloaded")
		print('Generating nat2k year stats\n')
		nat2k_year = pd.read_sql_query('select sum(clc.natura2k*ba.area_ha) as '
									   'tot_nat2k_ha from gw_burntarea_effis.ba_final as ba, '
									   'gw_burntarea_effis.ba_stats_clc as clc where ba.id=clc.ba_id and '
									   'ba.initialdate>=\'2020-01-01\' and ba.initialdate<\'2021-01-01\' and clc.natura2k is not null;', con=engine)
		print('Generating nat2k week stats\n')
		nat2kweek = pd.read_sql_query('select sum(clc.natura2k*ba.area_ha) as tot_nat2k_ha from '
									  'gw_burntarea_effis.ba_final as ba, gw_burntarea_effis.ba_stats_clc '
									  'as clc where ba.id=clc.ba_id and ba.initialdate>=(now()-interval \'7 days\')::date '
									  'and clc.natura2k is not null;', con=engine)
		print('Generating nat2k by country stats\n')
		nat2k_by_country = pd.read_sql_query('SELECT t."COUNTRY", sum(t."AREA_HA"*t."PERCNA2K"), count(t."PERCNA2K")' 
											 'FROM gw_burntarea_effis.ba_oracle_export_year as t '
											 'where t."AREA_HA">=30 and t."PERCNA2K"!=0 '
											 'group by t."COUNTRY" order by t."COUNTRY" ', con=engine)
		nat2k_areas = pd.read_sql_query('select t."ms", sum(t."area_ha") from rst.nat2000_end2010_mena as t '
										'group by t."ms" '
										'order by t."ms"', con=engine)
		df_nat2k_countries = pd.read_sql_query('SELECT t."COUNTRY", sum(t."AREA_HA"*t."PERCNA2K")/100 as area, count(t."PERCNA2K") '
												   'FROM gw_burntarea_effis.ba_oracle_compat_year as t '
												   'where t."AREA_HA">=30 and t."PERCNA2K"!=0 '
												   'group by t."COUNTRY" order by t."COUNTRY"',
					  con=engine)
		df_nat2k_sort_countries = pd.read_sql_query('SELECT ba."COUNTRY" as country, t."sitecode", '
													't."sitename", t."sitetype", (ba."AREA_HA"*ba."PERCNA2K")/100 as area_ha '
													'FROM rst.nat2000_end2010_mena as t '
													'JOIN gw_burntarea_effis.ba_oracle_compat_year as ba '
													'ON ST_Contains(t."geom", ba."geom") '
													'where ba."AREA_HA">=30 and ba."PERCNA2K"!=0 order by country ASC',
													con=engine)
		df_nat2k_sort_area = pd.read_sql_query('SELECT ba."COUNTRY" as country, t."sitecode", t."sitename", '
											   't."sitetype", (ba."AREA_HA"*ba."PERCNA2K")/100 as area_ha '
											   'FROM rst.nat2000_end2010_mena as t '
											   'JOIN gw_burntarea_effis.ba_oracle_compat_year as ba '
											   'ON ST_Contains(t."geom", ba."geom") '
											   'where ba."AREA_HA">=30 and ba."PERCNA2K"!=0 order by area_ha DESC',
											   con=engine)

	except (Exception, psycopg2.Error) as error:
		print("Error while connecting to PostgreSQL", error)
	finally:
		# closing database connection.
		if (connection):
			cursor.close()
			connection.close()
			print("PostgreSQL connection now closed")
	if use == 'r':
		return df_sql, gdf_sql, nat2k_year, nat2kweek, tab_nations, nat2k_by_country, nat2k_areas, df_nat2k_countries\
			, df_nat2k_sort_countries, df_nat2k_sort_area
	else:
		print('Ahia!')
	# return gdf_evo
