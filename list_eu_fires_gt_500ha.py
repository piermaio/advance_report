import db_connection
import os
import pandas as pd

table_name = 'gw_burntarea_effis.ba_oracle_export_year'

def sum_count(path):
    df_sql, nat2k_year, nat2kweek, df_nations = db_connection.db_connection(table_name)
    os.chdir(path)
    df = df_sql
    df['AREA_HA'] = df['AREA_HA'].astype(int)
    # Conversions from string to date format
    df['FIREDATE'] = pd.to_datetime(df['FIREDATE'])
    df['lastfiredate'] = pd.to_datetime(df['lastfiredate'])
    df['LASTUPDATE'] = pd.to_datetime(df['LASTUPDATE'])
    list_eunon = ['EU', 'EU_non', 'ME_AF']
    print('The dataset will be divided in three macroregions: {}'.format(list_eunon))
    # dataframes definition
    df = df.merge(df_nations, how='inner', left_on='COUNTRY', right_on='NUTS0_CODE')
    print('\n --- Count of burnt areas per EU, nonEU, ME_NA --- \n')
    print(df.columns)
    df_eu = df.loc[df['EU_nonEU'] == 'EU']
    selection = df_eu.loc[(df['AREA_HA']>=500)]
    selection.shape
    selection = selection.rename(columns={'FIREDATE': 'Starting date', 'place_name': 'Municipality'})
    selection = selection.sort_values(by=['NUTS_NAME', 'AREA_HA'])
    selection[['NUTS_NAME', 'Starting date', 'AREA_HA', 'PROVINCE', 'Municipality']].to_csv('5_list_eu_fires_gt_500ha.csv')
    print(selection[['NUTS_NAME', 'Starting date', 'AREA_HA', 'PROVINCE', 'Municipality']])