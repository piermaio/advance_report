import os
import pandas as pd
import psycopg2
import datetime as dt
import numpy as np
import sqlalchemy
from rasterstats import zonal_stats
import geopandas as gpd
import csv

def db_connection(use='r', statement=''):
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
        # global gdf_sql
        # print('Downloading the geodataframe from {} \n'.format(table_name))
        # gdf_sql = gpd.read_postgis('select * {}'.format(table_name), con=engine)
        # print("geodataframe gdf_sql from ba_oracle_export_year successfully downloaded")
        # global gdf_ba
        # gdf_ba = gpd.read_postgis('select * from gw_burntarea_effis.ba_year', con=engine)
        # print("geodataframe gdf_ba from ba_year successfully downloaded")
        if use =='r':
            # print('Downloading the dataframe from {} \n'.format(table_name))
            global df_nat2k_countries # table format excluding geoinformation
            df_nat2k_countries = pd.read_sql_query('SELECT *'
                                                   'FROM gw_burntarea_effis.ba_oracle_compat_year as t '
                                                   'where t."AREA_HA">=30 and t."PERCNA2K"!=0 ',
                      con=engine)
            print('Table dowloaded succesfully')
        if use =='w':
            count_statement = 'SELECT COUNT(*) FROM gw_burntarea_effis.rob_ba_evo_test'
            print(statement)
            print(cursor.execute(count_statement))
            cursor.execute(statement)
            print(cursor.execute(count_statement))
            connection.commit()
            print('Table updated correctly on db\n')
        # global tab_nations  # Table associating the
        # tab_nations = pd.read_sql_query('select * from "burnt_areas"."Tab_Elenco_Nazioni"', con=engine)
        # print("tab_nations successfully downloaded")
        # print('Generating nat2k year stats\n')
        # nat2k_year = pd.read_sql_query('select sum(clc.natura2k*ba.area_ha) as '
        # 							   'tot_nat2k_ha from gw_burntarea_effis.ba_final as ba, '
        # 							   'gw_burntarea_effis.ba_stats_clc as clc where ba.id=clc.ba_id and '
        # 							   'ba.initialdate>=\'2020-01-01\' and ba.initialdate<\'2021-01-01\' and clc.natura2k is not null;', con=engine)
        # print('Generating nat2k week stats\n')
        # nat2kweek = pd.read_sql_query('select sum(clc.natura2k*ba.area_ha) as tot_nat2k_ha from '
        # 							  'gw_burntarea_effis.ba_final as ba, gw_burntarea_effis.ba_stats_clc '
        # 							  'as clc where ba.id=clc.ba_id and ba.initialdate>=(now()-interval \'7 days\')::date '
        # 							  'and clc.natura2k is not null;', con=engine)
        # print('Generating nat2k by country stats\n')
        # nat2k_by_country = pd.read_sql_query('SELECT t."COUNTRY", sum(t."AREA_HA"*t."PERCNA2K"), count(t."PERCNA2K")'
        # 									 'FROM gw_burntarea_effis.ba_oracle_compat_year as t'
        # 									 'where t."AREA_HA">=30 and t."PERCNA2K"!=0'
        # 									 'group by t."COUNTRY" order by t."COUNTRY" ')
        # nat2k_areas = pd.read_sql_query('select t."ms", sum(t."area_ha") from rst.nat2000_end2010_mena as t'
        # 								'group by t."ms"'
        # 								'order by t."ms"')
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection now closed")
    if use == 'r':
        return df_nat2k_countries
    else:
        print('Ahia!')
    # return gdf_evo

def corine_stats(root, burnt_areas, corine_path):
    os.chdir(root)
    ba = burnt_areas
    list_columns = ['id', 'COUNTRY', 'AREA_HA', 'BROADLEA', 'CONIFER', 'MIXED', 'SCLEROPH', 'TRANSIT', 'OTHERNATLC', 'AGRIAREAS', 'ARTIFSURF', 'OTHERLC', 'PERCNA2K']
    ba = ba[list_columns]
    








    f = csv.reader(open('corine_key.csv'))
    corine_dict = {}
    for row in f:
        corine_dict[row[0]] = row[1]
    corine_dict['1'] = corine_dict.pop('ï»¿1')
    corine_dict = {int(k): v for k, v in corine_dict.items()}
    ba_raw = zonal_stats(ba, corine_path, categorical=True)
    df_ba_raw = pd.DataFrame.from_dict(ba_raw)
    df_ba_raw = df_ba_raw.rename(columns=corine_dict)
    df_ba_raw = df_ba_raw.T
    df_ba_raw.to_csv('corine_ba_raw.csv')
    df_mapped = df_ba_raw.groupby(df_ba_raw.index).sum()
    df_mapped = df_mapped.T
    df_mapped.to_csv('corine_ba_raw2.csv')
    df = df_mapped
    print('--PM')
    print(ba.columns)
    l = ba['id'].to_list()
    a = ba['AREA_HA'].to_list()
    df = df.assign(id=l, area=a)
    # df.to_csv('__df_test.csv')
    df_merge = df.merge(ba[['id', 'COUNTRY'
                                  '']], how='inner', on='id')
    df_group = df_merge.groupby([
        'COUNTRY']).sum()
    df_group.to_csv('__test_group.csv')
    print('\n----- df group columns\n')
    print(df_group.columns)
    print('---end')
    pass

def main():
    root = 'C:/Users/piermaio/Documents/gisdata/jrc/advance_report/'
    corine_path = 'C:\\Users\\piermaio\\Documents\\gisdata\\jrc\\BAmapping\\Corine\\raster\\Corine_globcover_MA_TU_ukraine.tif'  # PM local path to the corine geotif
    gdf = db_connection()
    corine_stats(root, gdf, corine_path)



if __name__ == '__main__':
    main()