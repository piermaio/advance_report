import db_connection
import os
import pandas as pd

table_name = 'gw_burntarea_effis.ba_oracle_export_year'

def month_sum_count(path):
    df_sql, nat2k_year, nat2kweek, df_nations = db_connection.db_connection(table_name)
    os.chdir(path)
    df = df_sql
    df['AREA_HA'] = df['AREA_HA'].astype(int)
    # Conversions from string to date format
    df['FIREDATE'] = pd.to_datetime(df['FIREDATE'])
    df['lastfiredate'] = pd.to_datetime(df['lastfiredate'])
    df['LASTUPDATE'] = pd.to_datetime(df['LASTUPDATE'])
    df['month'] = df['FIREDATE'].apply(lambda x: x.month)
    df = df.merge(df_nations, how='inner', left_on='COUNTRY', right_on='NUTS0_CODE')
    df.to_csv('temp')
    df2 = df.groupby(['NUTS_NAME', 'month']).agg({'AREA_HA': ['sum', 'count']})
    df2.to_csv('2_monthly_ba_and_number_fires.csv')
    # print(type(df['FIREDATE'][0]))
    print(df2)