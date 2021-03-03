import db_connection
import os
import pandas as pd

table_name = 'gw_burntarea_effis.ba_oracle_export_year'

def sum_count(path, df_sql, df_nations):
    # df_sql, gdf_sql, nat2k_year, nat2kweek, df_nations = db_connection.db_connection(table_name)
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
    df_eunon = df.loc[df['EU_nonEU'] == 'EU_non']
    df_meaf = df.loc[df['EU_nonEU'] == 'ME_AF']
    # selecting the burnt areas in eu over 500ha
    selection_eu = df_eu.loc[(df['AREA_HA']>=500)]
    selection_eu = selection_eu.rename(columns={'FIREDATE': 'Starting date', 'place_name': 'Municipality'})
    selection_eu = selection_eu.sort_values(by=['NUTS_NAME', 'AREA_HA'])
    selection_eu[['NUTS_NAME', 'Starting date', 'AREA_HA', 'PROVINCE', 'Municipality']].to_csv(
        '5a_list_eu_fires_gt_500ha.csv')
    print(selection_eu[['NUTS_NAME', 'Starting date', 'AREA_HA', 'PROVINCE', 'Municipality']])
    # selecting the burnt areas in non eu over 500ha
    selection_noneu = df_eunon.loc[(df['AREA_HA'] >= 500)]
    selection_noneu = selection_noneu.rename(columns={'FIREDATE': 'Starting date', 'place_name': 'Municipality'})
    selection_noneu = selection_noneu.sort_values(by=['NUTS_NAME', 'AREA_HA'])
    selection_noneu[['NUTS_NAME', 'Starting date', 'AREA_HA', 'PROVINCE', 'Municipality']].to_csv(
        '5b_list_eunon_fires_gt_500ha.csv')
    print(selection_noneu[['NUTS_NAME', 'Starting date', 'AREA_HA', 'PROVINCE', 'Municipality']])
    # selecting the burnt areas in non eu over 500ha
    selection_mena = df_meaf.loc[(df['AREA_HA'] >= 500)]
    selection_mena = selection_mena.rename(columns={'FIREDATE': 'Starting date', 'place_name': 'Municipality'})
    selection_mena = selection_mena.sort_values(by=['NUTS_NAME', 'AREA_HA'])
    selection_mena[['NUTS_NAME', 'Starting date', 'AREA_HA', 'PROVINCE', 'Municipality']].to_csv(
        '5c_list_mena_fires_gt_500ha.csv')
    print(selection_mena[['NUTS_NAME', 'Starting date', 'AREA_HA', 'PROVINCE', 'Municipality']])