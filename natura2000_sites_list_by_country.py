import os
import psycopg2
import sqlalchemy
import pandas as pd


root = 'C:/Users/piermaio/Documents/gisdata/jrc/advance_report/'

def db_connection(table_name='', use='r', statement=''):
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
		engine = sqlalchemy.create_engine(engine_string)

		if use =='r':
			global df_nat2k_sort_countries # table format excluding geoinformation
			global df_nat2k_sort_area
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

			print('Tables dowloaded succesfully')
		if use =='w':
			count_statement = 'SELECT COUNT(*) FROM gw_burntarea_effis.rob_ba_evo_test'
			print(statement)
			print(cursor.execute(count_statement))
			cursor.execute(statement)
			print(cursor.execute(count_statement))
			connection.commit()
			print('Table updated correctly on db\n')
	except (Exception, psycopg2.Error) as error:
		print("Error while connecting to PostgreSQL", error)
	finally:
		# closing database connection.
		if (connection):
			cursor.close()
			connection.close()
			print("PostgreSQL connection now closed")
	if use == 'r':
		return df_nat2k_sort_countries, df_nat2k_sort_area
	else:
		print('Ahia!')


def main(root):
	os.chdir(root)
	df_nat2k_sort_countries, df_nat2k_sort_area = db_connection()
	df_nat2k_sort_countries.to_csv('10a_NATURA_2000_sites_list_by_country.csv')
	df_nat2k_sort_area.to_csv('10b_NATURA_2000_sites_list_by_desc_area.csv')

if __name__ == '__main__':
	main(root)